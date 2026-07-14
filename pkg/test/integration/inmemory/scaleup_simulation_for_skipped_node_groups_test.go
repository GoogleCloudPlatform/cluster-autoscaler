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

package inmemory

import (
	"context"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	osconfig "k8s.io/gke-autoscaling/cluster-autoscaler/config"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	pod "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	visibilityfake "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/fake"
	vispb "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/proto"
	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
)

// TestScaleUpSimulationForSkippedNodeGroups verifies the "Scale Up Simulations for Skipped Node Groups" feature.
//
// Test Setup:
// 1. Initialize node pool 'backoff-test-np' with labels "simulation-test-label: true" to control pod scheduling.
//
// 2. Mock a GCE STOCKOUT failure on 'backoff-test-np'.
//
// 3. Force the target pool into a backed-off state:
//   - Run Loop 1: Deploy a trigger pod targeting the pool to trigger a scale-up request.
//   - Cleanup Trigger Pod: Delete the trigger pod to prevent residual unschedulable events.
//   - Run Loop 2: GkeManager queries GKE client, detects instance creation failure, and caches the error.
//   - Run Loop 3: ClusterStateRegistry processes the cached error in Refresh() and marks the pool as backed off.
//   - Clear event logger list so we start with a clean state.
//
// 4. Deploy test workloads:
//   - 'rejected-pod': Targets the node pool but fails scheduling predicates (selector mismatch).
//   - 'skipped-pod': Targets the node pool and passes scheduling predicates (selector match).
//
// 5. Run Loop 4 and verify the emitted visibility events match expected outcomes:
//   - If flag is disabled: MIG is skipped globally; both pod groups have no reference to the MIG.
//   - If flag is enabled: MIG is NOT skipped globally; on POD GROUP level skipped-pod group reports it as skipped; rejected-pod group reports it as rejected.
func TestScaleUpSimulationForSkippedNodeGroups(t *testing.T) {
	testCases := []struct {
		name                              string
		flagValue                         bool
		expectGlobalSkipped               bool
		expectSkippedPodPodLevelSkipped   bool
		expectSkippedPodPodLevelRejected  bool
		expectRejectedPodPodLevelSkipped  bool
		expectRejectedPodPodLevelRejected bool
	}{
		{
			name:                              "FlagDisabled_ExpectGlobalSkippedMigs_NoPodLevelMigs",
			flagValue:                         false,
			expectGlobalSkipped:               true,
			expectSkippedPodPodLevelSkipped:   false,
			expectSkippedPodPodLevelRejected:  false,
			expectRejectedPodPodLevelSkipped:  false,
			expectRejectedPodPodLevelRejected: false,
		},
		{
			name:                              "FlagEnabled_ExpectNoGlobalSkippedMigs_PodLevelMigs",
			flagValue:                         true,
			expectGlobalSkipped:               false,
			expectSkippedPodPodLevelSkipped:   true,
			expectSkippedPodPodLevelRejected:  false,
			expectRejectedPodPodLevelSkipped:  false,
			expectRejectedPodPodLevelRejected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)

				// 1. Setup config with experiments and visibility enabled
				testConfig := integration.NewTestConfig().WithOverrides(
					integration.WithAutoscalerVisibility(true),
					integration.WithEmitNoScaleUpCAVizEvents(true),
					integration.WithInitialNodeGroupBackoffDuration(time.Hour),
				)
				if tc.flagValue {
					testConfig = testConfig.WithExperiments(
						experiments.ScaleUpSimulationForSkippedNodeGroupsEnabledFlag,
						experiments.ScaleUpSimulationForSkippedNodeGroupsMinCAVersionFlag,
					)
				}

				// Configure GKE node pools
				defaultNodePool := integration.DefaultNodePool(
					integration.WithNodePoolName("default-pool"),
					integration.WithNodePoolSize(1),
					integration.WithNodePoolLocations("us-central1-a"),
				)
				testNodePool := integration.DefaultNodePool(
					integration.WithNodePoolName("backoff-test-np"),
					integration.WithNodePoolSize(0),
					integration.WithNodePoolMin(0),
					integration.WithNodePoolMax(3),
					integration.WithNodePoolLocations("us-central1-a"),
					integration.WithNodePoolLabels(map[string]string{
						gkelabels.GkeNodePoolLabel: "backoff-test-np",
						"simulation-test-label":    "true",
					}),
				)
				testConfig.WithNodePools(defaultNodePool, testNodePool)

				infra := integration.SetupInfrastructure(ctx, t)

				// 2. Mock GCE client to return STOCKOUT error on scale up for this MIG to trigger backoff
				errInfo := cloudprovider.InstanceErrorInfo{
					ErrorClass:   cloudprovider.OutOfResourcesErrorClass,
					ErrorCode:    "STOCKOUT",
					ErrorMessage: "out of resources",
				}
				infra.Fakes.GceService.SetCreateInstanceForMigError(testNodePool.Name, errInfo)

				// Get the builder
				builder, err := integration.DefaultAutoscalingBuilder(ctx, t, testConfig, infra)
				assert.NoError(t, err)

				// Create the fake event logger
				fakeLogger := visibilityfake.NewEventLogger()
				builder = builder.WithEventLogger(fakeLogger)

				// Build the autoscaler
				autoscaler, _, err := builder.Build(ctx, infra.Snapshotter, osconfig.OsReservedContent)
				assert.NoError(t, err)

				// 3. Force the MIG to enter backoff
				// Deploy a trigger pod that targets backoff-test-np and tolerates the taint.
				triggerPod := tu.BuildTestPod("trigger-pod", 500, 500, tu.MarkUnschedulable(),
					pod.WithNodeSelectorEntry(gkelabels.GkeNodePoolLabel, testNodePool.Name),
					pod.WithNodeSelectorEntry("simulation-test-label", "true"),
				)
				infra.Fakes.K8s.AddPod(triggerPod)

				// Run autoscaler loop to trigger scale-up.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

				// Remove the trigger pod immediately so it doesn't cause NoScaleUp event in Loop 2.
				infra.Fakes.K8s.DeletePod(triggerPod.Namespace, triggerPod.Name)

				// Run again to invalidate GkeManager cache and fetch instance in UpdateNodes
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

				// Run again to process the instance in CSR.Refresh and register backoff
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

				// Verify the MIG is backed off
				var testMig cloudprovider.NodeGroup
				for _, ng := range autoscaler.CloudProvider.NodeGroups() {
					if strings.Contains(ng.Id(), testNodePool.Name) {
						testMig = ng
						break
					}
				}
				assert.NotNil(t, testMig, "Failed to find node group %s", testNodePool.Name)
				assert.True(t, autoscaler.ClusterStateRegistry.BackoffStatusForNodeGroup(testMig, time.Now()).IsBackedOff, "MIG should be backed off")

				// Clear the visibility events recorded so far during setup
				fakeLogger.Clear()

				// 4. Deploy Test Workloads
				// Pod A: 'rejected-pod' targets the node pool but does NOT tolerate the taint.
				rejectedPod := tu.BuildTestPod("rejected-pod", 500, 500, tu.MarkUnschedulable(),
					pod.WithNodeSelectorEntry(gkelabels.GkeNodePoolLabel, testNodePool.Name),
					pod.WithNodeSelectorEntry("simulation-test-label", "false"),
				)
				infra.Fakes.K8s.AddPod(rejectedPod)

				// Pod B: 'skipped-pod' targets the node pool and DOES tolerate the taint.
				skippedPod := tu.BuildTestPod("skipped-pod", 500, 500, tu.MarkUnschedulable(),
					pod.WithNodeSelectorEntry(gkelabels.GkeNodePoolLabel, testNodePool.Name),
					pod.WithNodeSelectorEntry("simulation-test-label", "true"),
				)
				infra.Fakes.K8s.AddPod(skippedPod)

				// Run autoscaler loop to evaluate the new workloads
				integration_synctest.MustRunOnceAfter(t.Context(), t, autoscaler, time.Second)

				// 5. Verify Visibility Events
				noScaleUpEvents := fakeLogger.NoScaleUpEvents()
				assert.NotEmpty(t, noScaleUpEvents, "Expected to find at least one NoScaleUp visibility event")

				// Verify global skipped status
				assert.Equal(t, tc.expectGlobalSkipped, hasGlobalSkippedMig(noScaleUpEvents, testNodePool.Name), "Unexpected global skipped status")

				// Verify skipped-pod group
				verifyPodGroupStatus(t, noScaleUpEvents, "skipped-pod", testNodePool.Name, tc.expectSkippedPodPodLevelSkipped, tc.expectSkippedPodPodLevelRejected)

				// Verify rejected-pod group
				verifyPodGroupStatus(t, noScaleUpEvents, "rejected-pod", testNodePool.Name, tc.expectRejectedPodPodLevelSkipped, tc.expectRejectedPodPodLevelRejected)
			})
		})
	}
}

func verifyPodGroupStatus(t *testing.T, noScaleUpEvents []*vispb.NoScaleUpData, podName, migName string, expectSkipped, expectRejected bool) {
	t.Helper()
	pgExp := findPodGroup(noScaleUpEvents, podName)
	if !assert.NotNil(t, pgExp, "Failed to find pod group for %q", podName) {
		return
	}

	skippedMig := findMigExplanation(pgExp.GetSkippedMigs(), migName)
	if assert.Equal(t, expectSkipped, skippedMig != nil, "%s: expected skipped status to be %t", podName, expectSkipped) && skippedMig != nil {
		assert.Equal(t, vistypes.MessageIdToStringMap[vistypes.NoScaleUpMigSkipped], skippedMig.GetReason().GetMessageId())
	}

	rejectedMig := findMigExplanation(pgExp.GetRejectedMigs(), migName)
	if assert.Equal(t, expectRejected, rejectedMig != nil, "%s: expected rejected status to be %t", podName, expectRejected) && rejectedMig != nil {
		assert.Equal(t, vistypes.MessageIdToStringMap[vistypes.NoScaleUpMigFailingPredicate], rejectedMig.GetReason().GetMessageId())
	}
}

func findPodGroup(noScaleUpEvents []*vispb.NoScaleUpData, podName string) *vispb.PodGroupExplanation {
	for _, noScaleUp := range noScaleUpEvents {
		for _, pgExp := range noScaleUp.GetUnhandledPodGroups() {
			pg := pgExp.GetPodGroup()
			if pg != nil && pg.GetSamplePod() != nil && strings.Contains(pg.GetSamplePod().GetName(), podName) {
				return pgExp
			}
		}
	}
	return nil
}

func findMigExplanation(explanations []*vispb.MigExplanation, migName string) *vispb.MigExplanation {
	for _, exp := range explanations {
		if exp.GetMig() != nil && exp.GetMig().GetName() == migName {
			return exp
		}
	}
	return nil
}

func hasGlobalSkippedMig(noScaleUpEvents []*vispb.NoScaleUpData, migName string) bool {
	for _, noScaleUp := range noScaleUpEvents {
		if findMigExplanation(noScaleUp.GetSkippedMigs(), migName) != nil {
			return true
		}
	}
	return false
}
