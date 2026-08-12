// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package flexadvisor

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	experimentfake "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	vispb "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/proto"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const (
	capacityConstraintsMessageId          = "no.scale.up.nap.capacity.constraints"
	migSkippedMessageId                   = "no.scale.up.mig.skipped"
	reasonSkippedDueToCapacityConstraints = "skipped due to capacity constraints"
)

func TestFlexAdvisorVisibilityLogTracking_CapacityConstraintEmitsNoScaleUpEvent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		cccCrd := ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1").Build()
		nodePools := []*gke_api_beta.NodePool{
			integration.EmptyNodePool("pool-1").WithMachineType("n1-standard-4").WithCCCLabel("test-ccc").Build(),
		}

		testConfig := integration.NewTestConfig().
			WithNodePools(nodePools...).
			WithCccCrds(cccCrd).
			WithClusterOverrides(
				integration.WithAutoprovisioningLocations("us-central1-b"),
				integration.WithClusterAutoProvisioningEnabled(),
			).
			WithOverrides(
				integration.WithMaxMemoryTotal(140*1024*1024*1024),
				integration.WithFlexAdvisorEnabled(),
				integration.WithAutoProvisioningEnabled(),
				integration.WithAutoscalerVisibility(true),
				integration.WithEmitNoScaleUpCAVizEvents(true),
			)

		// Flex Advisor returns zero capacity for the machine type in pool-1
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance("n1-standard-4").WithCapacity(0),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)

		unschedulablePod := tu.BuildTestPod("unsched-pod", 3000, 8000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(unschedulablePod)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 15*time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Verify pod is not scheduled
		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "unsched-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Empty(t, updatedPod.Spec.NodeName, "Expected unsched-pod to remain unschedulable")

		// Verify NoScaleUp visibility event was logged with capacity constraint reason
		noScaleUpEvents := infra.Fakes.EventLogger.NoScaleUpEvents()
		assert.NotEmpty(t, noScaleUpEvents, "Expected at least one NoScaleUp visibility event")

		lastEvent := noScaleUpEvents[len(noScaleUpEvents)-1]
		assert.NotNil(t, lastEvent.NapFailureReason, "Expected NapFailureReason field in NoScaleUpData")
		assert.Equal(t, capacityConstraintsMessageId, lastEvent.NapFailureReason.MessageId)
		assert.Contains(t, lastEvent.NapFailureReason.Parameters, "test-ccc")

		// Verify pool-1 is listed in SkippedMigs with capacity constraints reason
		assert.NotEmpty(t, lastEvent.SkippedMigs, "Expected at least one SkippedMigs entry")
		var foundSkippedMig bool
		for _, sm := range lastEvent.SkippedMigs {
			if sm.Mig != nil && sm.Mig.Nodepool == "pool-1" {
				foundSkippedMig = true
				assert.NotNil(t, sm.Reason)
				assert.Equal(t, migSkippedMessageId, sm.Reason.MessageId)
				assert.Contains(t, sm.Reason.Parameters, reasonSkippedDueToCapacityConstraints)
			}
		}
		assert.True(t, foundSkippedMig, "Expected pool-1 in SkippedMigs")
	})
}

func TestFlexAdvisorVisibilityLogTracking_DisabledScenarios(t *testing.T) {
	testCases := []struct {
		name                string
		flexAdvisorEnabled  bool
		experimentEvaluator experiments.Evaluator
	}{
		{
			name:               "tracker disabled via experiment flag",
			flexAdvisorEnabled: true,
			experimentEvaluator: experimentfake.NewEvaluator(map[string]bool{
				experiments.FlexAdvisorScaleUpLimiterTrackerEnabledFlag: false,
			}, nil),
		},
		{
			name:               "main FlexAdvisor processing disabled via experiment flag",
			flexAdvisorEnabled: true,
			experimentEvaluator: experimentfake.NewEvaluator(map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag: false,
			}, nil),
		},
		{
			name:               "tracker disabled via minimum CA version flag",
			flexAdvisorEnabled: true,
			experimentEvaluator: experimentfake.NewEvaluator(nil, map[string]string{
				experiments.FlexAdvisorScaleUpLimiterTrackerMinCAVersionFlag: "999.0.0",
			}),
		},
		{
			name:               "GCEFlexAdvisorEnabled CLI flag disabled",
			flexAdvisorEnabled: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cccCrd := ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1").Build()
			nodePools := []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-1").WithMachineType("n1-standard-4").WithCCCLabel("test-ccc").Build(),
			}

			overrides := []integration.Option[*options.AutoscalingOptions]{
				integration.WithMaxMemoryTotal(140 * 1024 * 1024 * 1024),
				integration.WithAutoProvisioningEnabled(),
				integration.WithAutoscalerVisibility(true),
				integration.WithEmitNoScaleUpCAVizEvents(true),
			}
			if tc.flexAdvisorEnabled {
				overrides = append(overrides, integration.WithFlexAdvisorEnabled())
			}

			testConfig := integration.NewTestConfig().
				WithNodePools(nodePools...).
				WithCccCrds(cccCrd).
				WithClusterOverrides(
					integration.WithAutoprovisioningLocations("us-central1-b"),
					integration.WithClusterAutoProvisioningEnabled(),
				).
				WithOverrides(overrides...)

			if tc.experimentEvaluator != nil {
				testConfig.ExperimentEvaluator = tc.experimentEvaluator
			}

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
					fake.NewGuidance("n1-standard-4").WithCapacity(0),
				)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				unschedulablePod := tu.BuildTestPod("unsched-pod", 3000, 8000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
				infra.Fakes.K8s.AddPod(unschedulablePod)

				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 15*time.Second)
				infra.Fakes.RunScheduler(ctx, t)

				noScaleUpEvents := infra.Fakes.EventLogger.NoScaleUpEvents()
				for _, event := range noScaleUpEvents {
					assertNoCapacityConstraintsReasons(t, event)
				}
			})
		})
	}
}

func TestFlexAdvisorVisibilityLogTracking_StateResetBetweenLoops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		cccCrd := ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1").Build()
		nodePools := []*gke_api_beta.NodePool{
			integration.EmptyNodePool("pool-1").WithMachineType("n1-standard-4").WithCCCLabel("test-ccc").Build(),
		}

		testConfig := integration.NewTestConfig().
			WithNodePools(nodePools...).
			WithCccCrds(cccCrd).
			WithClusterOverrides(
				integration.WithAutoprovisioningLocations("us-central1-b"),
				integration.WithClusterAutoProvisioningEnabled(),
			).
			WithOverrides(
				integration.WithMaxMemoryTotal(140*1024*1024*1024),
				integration.WithFlexAdvisorEnabled(),
				integration.WithAutoProvisioningEnabled(),
				integration.WithAutoscalerVisibility(true),
				integration.WithEmitNoScaleUpCAVizEvents(true),
			)

		// 1. Initial loop with zero capacity -> option removed event
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance("n1-standard-4").WithCapacity(0),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)

		pod1 := tu.BuildTestPod("pod-1", 3000, 8000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(pod1)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 15*time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		events1 := infra.Fakes.EventLogger.NoScaleUpEvents()
		assert.NotEmpty(t, events1)
		lastEvent1 := events1[len(events1)-1]
		assert.NotNil(t, lastEvent1.NapFailureReason)
		assert.Equal(t, capacityConstraintsMessageId, lastEvent1.NapFailureReason.MessageId)
		assert.Contains(t, lastEvent1.NapFailureReason.Parameters, "test-ccc")

		// 2. Clear guidance and clear logged events for second loop
		infra.Fakes.FlexAdvisorClient.ClearCapacityGuidances()
		infra.Fakes.EventLogger.Clear()

		pod2 := tu.BuildTestPod("pod-2", 3000, 8000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(pod2)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 15*time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Pod 2 should schedule on pool-1
		updatedPod2, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-2", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod2.Spec.NodeName, "Expected pod-2 to be scheduled after capacity recovery")

		// Verify no capacity constraints event was logged in second loop
		events2 := infra.Fakes.EventLogger.NoScaleUpEvents()
		for _, event := range events2 {
			assertNoCapacityConstraintsReasons(t, event)
		}
	})
}

func TestFlexAdvisorVisibilityLogTracking_NotEmittedWhenScaleUpSucceedsOnAlternativeOption(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		cccCrd := ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1", "pool-2").Build()
		nodePools := []*gke_api_beta.NodePool{
			integration.EmptyNodePool("pool-1").WithMachineType("n1-standard-4").WithCCCLabel("test-ccc").Build(),
			integration.EmptyNodePool("pool-2").WithMachineType("n1-standard-8").WithCCCLabel("test-ccc").Build(),
		}

		testConfig := integration.NewTestConfig().
			WithNodePools(nodePools...).
			WithCccCrds(cccCrd).
			WithClusterOverrides(
				integration.WithAutoprovisioningLocations("us-central1-b"),
				integration.WithClusterAutoProvisioningEnabled(),
			).
			WithOverrides(
				integration.WithMaxMemoryTotal(140*1024*1024*1024),
				integration.WithFlexAdvisorEnabled(),
				integration.WithAutoProvisioningEnabled(),
				integration.WithAutoscalerVisibility(true),
				integration.WithEmitNoScaleUpCAVizEvents(true),
			)

		// pool-1 has 0 capacity (removed), pool-2 has 10 capacity (available)
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance("n1-standard-4").WithCapacity(0),
			fake.NewGuidance("n1-standard-8").WithCapacity(10),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)

		unschedulablePod := tu.BuildTestPod("standard-pod", 3000, 8000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(unschedulablePod)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 15*time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Verify pod is scheduled on pool-2
		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "standard-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected pod to be scheduled on alternative node pool")
		assert.Contains(t, updatedPod.Spec.NodeName, "pool-2")

		// Verify no capacity constraints event was emitted when scale-up succeeded on alternative option
		noScaleUpEvents := infra.Fakes.EventLogger.NoScaleUpEvents()
		for _, event := range noScaleUpEvents {
			assertNoCapacityConstraintsReasons(t, event)
		}
	})
}

func TestFlexAdvisorVisibilityLogTracking_SimulationForSkippedNodeGroups(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		cccCrd := ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1").Build()
		nodePools := []*gke_api_beta.NodePool{
			integration.EmptyNodePool("pool-1").WithMachineType("n1-standard-4").WithCCCLabel("test-ccc").Build(),
		}

		testConfig := integration.NewTestConfig().
			WithNodePools(nodePools...).
			WithCccCrds(cccCrd).
			WithClusterOverrides(
				integration.WithAutoprovisioningLocations("us-central1-b"),
				integration.WithClusterAutoProvisioningEnabled(),
			).
			WithOverrides(
				integration.WithMaxMemoryTotal(140*1024*1024*1024),
				integration.WithFlexAdvisorEnabled(),
				integration.WithAutoProvisioningEnabled(),
				integration.WithAutoscalerVisibility(true),
				integration.WithEmitNoScaleUpCAVizEvents(true),
				integration.WithScaleUpSimulationForSkippedNodeGroups(true),
			)

		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance("n1-standard-4").WithCapacity(0),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)

		unschedulablePod := tu.BuildTestPod("unsched-pod", 3000, 8000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(unschedulablePod)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 15*time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		noScaleUpEvents := infra.Fakes.EventLogger.NoScaleUpEvents()
		assert.NotEmpty(t, noScaleUpEvents, "Expected at least one NoScaleUp visibility event")

		lastEvent := noScaleUpEvents[len(noScaleUpEvents)-1]
		assert.NotNil(t, lastEvent.NapFailureReason, "Expected NapFailureReason field in NoScaleUpData")
		assert.Equal(t, capacityConstraintsMessageId, lastEvent.NapFailureReason.MessageId)
		assert.Contains(t, lastEvent.NapFailureReason.Parameters, "test-ccc")

		// When all options are removed before binpacking and there are no pod-level NoScaleUpInfos,
		// the visibility processor falls back to reporting removed node groups in global SkippedMigs.
		assert.NotEmpty(t, lastEvent.SkippedMigs, "Expected fallback to global SkippedMigs when no pod simulation infos exist")

		var foundSkippedMig bool
		for _, sm := range lastEvent.SkippedMigs {
			if sm.Mig != nil && sm.Mig.Nodepool == "pool-1" {
				foundSkippedMig = true
				assert.NotNil(t, sm.Reason)
				assert.Equal(t, migSkippedMessageId, sm.Reason.MessageId)
				assert.Contains(t, sm.Reason.Parameters, reasonSkippedDueToCapacityConstraints)
			}
		}
		assert.True(t, foundSkippedMig, "Expected pool-1 in SkippedMigs")
	})
}

func assertNoCapacityConstraintsReasons(t *testing.T, event *vispb.NoScaleUpData) {
	t.Helper()
	if event.NapFailureReason != nil {
		assert.NotEqual(t, capacityConstraintsMessageId, event.NapFailureReason.MessageId,
			"Did not expect capacity constraint NapFailureReason")
	}
	if event.Reason != nil {
		assert.NotEqual(t, capacityConstraintsMessageId, event.Reason.MessageId,
			"Did not expect capacity constraint Reason")
	}
	for _, sm := range event.SkippedMigs {
		if sm.Reason != nil {
			for _, param := range sm.Reason.Parameters {
				assert.NotEqual(t, reasonSkippedDueToCapacityConstraints, param,
					"Did not expect capacity constraint parameter in global SkippedMigs")
			}
		}
	}
	for _, pg := range event.UnhandledPodGroups {
		for _, sm := range pg.SkippedMigs {
			if sm.Reason != nil {
				for _, param := range sm.Reason.Parameters {
					assert.NotEqual(t, reasonSkippedDueToCapacityConstraints, param,
						"Did not expect capacity constraint parameter in pod group SkippedMigs")
				}
			}
		}
	}
}
