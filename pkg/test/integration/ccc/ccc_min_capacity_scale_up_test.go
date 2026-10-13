/*
Copyright 2026 Google LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ccc_test

import (
	"context"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"k8s.io/client-go/tools/record"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status/history"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/recorder"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func drainRecordedEvents(rec *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case e := <-rec.Events:
			events = append(events, e)
		default:
			return events
		}
	}
}

func TestCCCScaleUpToMinCapacity(t *testing.T) {
	testCases := []struct {
		name                      string
		experimentEnabled         bool
		clusterScaleToZeroEnabled bool
		expectedNodeCount         int
	}{
		{
			name:              "scales up to min capacity when experiment is enabled",
			experimentEnabled: true,
			expectedNodeCount: 2,
		},
		{
			name:                      "scales up from zero to min capacity when cluster scale-to-zero is enabled",
			experimentEnabled:         true,
			clusterScaleToZeroEnabled: true,
			expectedNodeCount:         2,
		},
		{
			name:              "does not scale up when experiment is disabled",
			experimentEnabled: false,
			expectedNodeCount: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cc := ccc.NewComputeClassBuilder("my-ccc").
				WithNapEnabled().
				WithTargetNodeCount(ptr.To(2)).
				WithWhenUnsatisfiable("ScaleUpAnyway").
				WithPriorities(v1.Priority{MachineFamily: ptr.To("n1")}).
				Build()

			testConfig := integration.NewTestConfig().
				WithOverrides(
					integration.WithAutoProvisioningEnabled(),
					integration.WithComputeClassMinCapacityEnabled(),
					integration.WithEnhancedCrdStatusReportingEnabled(),
					integration.WithAutoscalerVisibility(true),
					integration.WithEmitNoScaleUpCAVizEvents(true),
				).
				WithClusterOverrides(
					integration.WithClusterAutoProvisioningEnabled(),
				).
				WithCccCrds(cc)

			if tc.clusterScaleToZeroEnabled {
				testConfig.WithOverrides(
					integration.WithClusterScaleToZeroEnabled(0),
				)
			}

			if !tc.experimentEnabled {
				testConfig.WithExperimentOverrides(
					map[string]bool{"ComputeClassMinCapacity::Enabled": false},
					map[string]string{},
				)
			}

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				fakeRecorder := recorder.SetupFakeRecorder(autoscaler)
				autoscaler.AutoscalingContext.Recorder = fakeRecorder
				defer integration_synctest.TearDown(cancel)

				// Run the autoscaler loop once. If enabled, NAP should scale up to satisfy target minimum capacity.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

				// Verify the node count.
				nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedNodeCount, len(nodes.Items))

				for _, node := range nodes.Items {
					assert.Equal(t, "my-ccc", node.Labels["cloud.google.com/compute-class"])
				}

				if tc.experimentEnabled {
					// Let the background status aggregator flush the queued condition.
					time.Sleep(ccc.ConditionsFlushInterval)

					updatedCCC, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, "my-ccc", metav1.GetOptions{})
					assert.NoError(t, err)

					// The scale-up was triggered by min capacity fake pods, not by real pending pods.
					ccc.AssertPriorityCondition(t, updatedCCC, 0, history.ConditionTypeNodeProvisioningInProgress, history.ConditionReasonMinimumCapacity)

					// Verify no ScaleUp decision event was emitted to visibility logs for fake pods.
					for _, ev := range infra.Fakes.EventLogger.Events() {
						assert.Nil(t, ev.GetDecision().GetScaleUp(), "expected no ScaleUp visibility decision event for min capacity fake pods")
					}

					// Verify no K8s Event was recorded for min-nodes-fake- pods.
					for _, ev := range drainRecordedEvents(fakeRecorder) {
						assert.NotContains(t, ev, "min-nodes-fake-",
							"unexpected K8s Event emitted for min capacity fake pod: %s", ev)
					}
				}
			})
		})
	}
}

func TestCCCMinCapacityUnschedulableFakePodsDoNotEmitVisibilityOrK8sEvents(t *testing.T) {
	cc := ccc.NewComputeClassBuilder("my-ccc").
		WithTargetNodeCount(ptr.To(2)).
		WithPriorities(v1.Priority{MachineFamily: ptr.To("n1")}).
		Build()

	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithComputeClassMinCapacityEnabled(),
			integration.WithEnhancedCrdStatusReportingEnabled(),
			integration.WithAutoscalerVisibility(true),
			integration.WithEmitNoScaleUpCAVizEvents(true),
		).
		WithCccCrds(cc)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)
		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		fakeRecorder := recorder.SetupFakeRecorder(autoscaler)
		autoscaler.AutoscalingContext.Recorder = fakeRecorder
		defer integration_synctest.TearDown(cancel)

		// Run the autoscaler loop once. NAP is disabled and no matching node pool exists,
		// so the min capacity fake pods remain unschedulable.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Empty(t, nodes.Items)

		// Verify no NoScaleUp visibility event was emitted for the unschedulable fake pods.
		assert.Empty(t, infra.Fakes.EventLogger.NoScaleUpEvents(),
			"expected no NoScaleUp visibility events for unschedulable min capacity fake pods")

		// Verify no K8s Event was recorded for min-nodes-fake- pods.
		assert.Empty(t, drainRecordedEvents(fakeRecorder),
			"expected no K8s Events for unschedulable min capacity fake pods")
	})
}

func TestCCCMinCapacityPreservesEventsForRealPods(t *testing.T) {
	ccScaleUp := ccc.NewComputeClassBuilder("ccc-scale-up").
		WithNapEnabled().
		WithTargetNodeCount(ptr.To(2)).
		WithWhenUnsatisfiable("ScaleUpAnyway").
		WithPriorities(v1.Priority{MachineFamily: ptr.To("n1")}).
		Build()

	ccUnschedulable := ccc.NewComputeClassBuilder("ccc-unschedulable").
		WithNapEnabled().
		WithTargetNodeCount(ptr.To(1)).
		WithPriorities(v1.Priority{
			MachineType: ptr.To("n1-standard-2"),
			Reservations: &v1.Reservations{
				Affinity: v1.AnyThenFail,
			},
		}).
		Build()

	realScaleUpPod := tu.BuildTestPod("real-scale-up-pod", 1000, 1000,
		tu.MarkUnschedulable(),
		pod.WithCCC("ccc-scale-up"),
	)
	realUnschedulablePod := tu.BuildTestPod("real-unschedulable-pod", 1000, 1000,
		tu.MarkUnschedulable(),
		pod.WithCCC("ccc-unschedulable"),
	)

	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithAutoProvisioningEnabled(),
			integration.WithComputeClassMinCapacityEnabled(),
			integration.WithEnhancedCrdStatusReportingEnabled(),
			integration.WithAutoscalerVisibility(true),
			integration.WithEmitNoScaleUpCAVizEvents(true),
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
		).
		WithCccCrds(ccScaleUp, ccUnschedulable)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)
		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		fakeRecorder := recorder.SetupFakeRecorder(autoscaler)
		autoscaler.AutoscalingContext.Recorder = fakeRecorder
		defer integration_synctest.TearDown(cancel)

		infra.Fakes.K8s.AddPod(realScaleUpPod)
		infra.Fakes.K8s.AddPod(realUnschedulablePod)

		// Run 3 loops so pod sharding processes both real-pod ComputeClass shards and the min-capacity fake pods.
		for range 3 {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		}

		// Verify ScaleUp visibility decision event was emitted for realScaleUpPod and contains no fake pods.
		var scaleUpTriggeringPods []string
		for _, ev := range infra.Fakes.EventLogger.Events() {
			if su := ev.GetDecision().GetScaleUp(); su != nil {
				for _, p := range su.GetTriggeringPods() {
					scaleUpTriggeringPods = append(scaleUpTriggeringPods, p.GetName())
				}
			}
		}
		assert.Contains(t, scaleUpTriggeringPods, "real-scale-up-pod")
		for _, name := range scaleUpTriggeringPods {
			assert.False(t, strings.HasPrefix(name, "min-nodes-fake-"),
				"unexpected fake pod in ScaleUp visibility event: %s", name)
		}

		// Verify NoScaleUp visibility event was emitted for realUnschedulablePod and contains no fake pods.
		var noScaleUpPods []string
		for _, ev := range infra.Fakes.EventLogger.NoScaleUpEvents() {
			for _, group := range ev.GetUnhandledPodGroups() {
				noScaleUpPods = append(noScaleUpPods, group.GetPodGroup().GetSamplePod().GetName())
			}
		}
		assert.Contains(t, noScaleUpPods, "real-unschedulable-pod")
		for _, name := range noScaleUpPods {
			assert.False(t, strings.HasPrefix(name, "min-nodes-fake-"),
				"unexpected fake pod in NoScaleUp visibility event: %s", name)
		}

		// Verify K8s Events are emitted for both real pods and never for fake pods.
		recordedEvents := drainRecordedEvents(fakeRecorder)
		var hasTriggeredScaleUp, hasNotTriggerScaleUp bool
		for _, ev := range recordedEvents {
			assert.NotContains(t, ev, "min-nodes-fake-",
				"unexpected K8s Event emitted for min capacity fake pod: %s", ev)
			if strings.Contains(ev, "TriggeredScaleUp") {
				hasTriggeredScaleUp = true
			}
			if strings.Contains(ev, "NotTriggerScaleUp") {
				hasNotTriggerScaleUp = true
			}
		}
		assert.True(t, hasTriggeredScaleUp, "expected TriggeredScaleUp K8s event for realScaleUpPod, got: %v", recordedEvents)
		assert.True(t, hasNotTriggerScaleUp, "expected NotTriggerScaleUp K8s event for realUnschedulablePod, got: %v", recordedEvents)
	})
}
