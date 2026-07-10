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

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

func TestStockOutWithoutFlexAdvisor(t *testing.T) {
	nodePools := []*gke_api_beta.NodePool{
		createEmptyNodePool("pool-p1", ZeroCapacityRecommendationMachineType),
		createEmptyNodePool("pool-p2", AvailableMachineType),
	}
	ccc := createCCCWithNodePoolsRules([]string{"pool-p1", "pool-p2"})
	nodePools = annotateNodePoolWithCCCLabel(ccc.Name, nodePools)

	testConfig := integration.NewTestConfig().
		WithNodePools(nodePools...).
		WithCccCrds(ccc).
		WithOverrides(
			integration.WithMaxMemoryTotal(30 * 1024 * 1024 * 1024),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		// Mock GCE stockout error on instance creation (happens after successful resize)
		// nodePool's name and MIG's name are the same in the test
		infra.Fakes.GceService.SetCreateInstanceForMigError(nodePools[0].Name, stockOutError())

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		// Now ask for 3000m. It will trigger scale up.
		pod := tu.BuildTestPod("standard-pod", 3000, 12000, tu.MarkUnschedulable(), withTestCCC)
		infra.Fakes.K8s.AddPod(pod)

		// Loop 1: Should try pool-p1 and fail
		integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Verify standard-pod is NOT scheduled and no nodes are created
		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "standard-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Empty(t, updatedPod.Spec.NodeName, "Expected standard-pod to remain unschedulable after first loop")
		assert.Equal(t, 0, len(infra.Fakes.K8s.Nodes().Items), "Expected 0 nodes after first loop")

		// Loop 2: Should try pool-p2 and succeed
		integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Verify standard-pod IS scheduled on pool-p2
		updatedPod, err = infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "standard-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected standard-pod to be scheduled after second loop")
		assert.Contains(t, updatedPod.Spec.NodeName, nodePools[1].Name, "Expected pod to be scheduled on 2nd node pool, but got %s", updatedPod.Spec.NodeName)
		assert.Equal(t, 1, len(infra.Fakes.K8s.Nodes().Items), "Expected 1 node after second loop")
	})
}

func TestStockOutWithFlexAdvisorOneStep(t *testing.T) {
	const NoSchedule = ""
	for name, tt := range map[string]struct {
		ccc                      *v1.ComputeClass
		nodePools                []*gke_api_beta.NodePool
		nodePoolNewInstanceError map[string]cloudprovider.InstanceErrorInfo
		expectedToScheduleOn     string
	}{
		"unable_to_schedule": {
			ccc: createCCCWithNodePoolsRules([]string{"pool-1"}),
			nodePools: []*gke_api_beta.NodePool{
				createEmptyNodePool("pool-1", ZeroCapacityRecommendationMachineType),
			},
			expectedToScheduleOn: NoSchedule,
		},
		"first_priority_available": {
			ccc: createCCCWithNodePoolsRules([]string{"pool-1", "pool-2"}),
			nodePools: []*gke_api_beta.NodePool{
				createEmptyNodePool("pool-1", AvailableMachineType),
				createEmptyNodePool("pool-2", ZeroCapacityRecommendationMachineType),
			},
			expectedToScheduleOn: "pool-1",
		},
		"fallback_to_available": {
			ccc: createCCCWithNodePoolsRules([]string{"pool-1", "pool-2"}),
			nodePools: []*gke_api_beta.NodePool{
				createEmptyNodePool("pool-1", ZeroCapacityRecommendationMachineType),
				createEmptyNodePool("pool-2", AvailableMachineType),
			},
			expectedToScheduleOn: "pool-2",
		},
		"fallback_to_available_3rd_nodepool": {
			ccc: createCCCWithNodePoolsRules([]string{"pool-1", "pool-2", "pool-3"}),
			nodePools: []*gke_api_beta.NodePool{
				createEmptyNodePool("pool-1", ZeroCapacityRecommendationMachineType),
				createEmptyNodePool("pool-2", ZeroCapacityRecommendationMachineType),
				createEmptyNodePool("pool-3", AvailableMachineType),
			},
			expectedToScheduleOn: "pool-3",
		},
		"flexadvisor_recommends_unavailable_machine": {
			ccc: createCCCWithNodePoolsRules([]string{"pool-1", "pool-2"}),
			nodePools: []*gke_api_beta.NodePool{
				createEmptyNodePool("pool-1", AvailableMachineType),
				createEmptyNodePool("pool-2", AvailableMachineType),
			},
			nodePoolNewInstanceError: map[string]cloudprovider.InstanceErrorInfo{
				"pool-1": stockOutError(),
			},
			expectedToScheduleOn: NoSchedule, // we're executing single step
		},
		"fallback_to_unknown_machine": {
			ccc: createCCCWithNodePoolsRules([]string{"pool-1", "pool-2", "pool-3"}),
			nodePools: []*gke_api_beta.NodePool{
				createEmptyNodePool("pool-1", ZeroCapacityRecommendationMachineType),
				createEmptyNodePool("pool-2", UnknownAvailabilityMachineType),
				createEmptyNodePool("pool-3", AvailableMachineType),
			},
			expectedToScheduleOn: "pool-2",
		},
	} {
		t.Run(name, func(t *testing.T) {
			nodePools := annotateNodePoolWithCCCLabel(tt.ccc.Name, tt.nodePools)
			testConfig := integration.NewTestConfig().
				WithNodePools(nodePools...).
				WithCccCrds(tt.ccc).
				WithOverrides(
					integration.WithMaxMemoryTotal(140*1024*1024*1024),
					integration.WithFlexAdvisorEnabled(),
				)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(testCapacityGuidance()...)

				// Mock GCE errors
				for name, err := range tt.nodePoolNewInstanceError {
					// nodePool's name and MIG's name are the same
					infra.Fakes.GceService.SetCreateInstanceForMigError(name, err)
				}

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				// Now ask for 3000m. It will trigger scale up.
				pod := tu.BuildTestPod("standard-pod", 3000, 12000, tu.MarkUnschedulable(), withTestCCC)
				infra.Fakes.K8s.AddPod(pod)

				integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)
				infra.Fakes.RunScheduler(ctx, t)

				updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "standard-pod", metav1.GetOptions{})
				assert.NoError(t, err)
				if tt.expectedToScheduleOn != NoSchedule {
					assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected standard-pod to be scheduled in the first loop")
					assert.Contains(t, updatedPod.Spec.NodeName, tt.expectedToScheduleOn, "Expected pod to be scheduled on %s node directly, but got %s", tt.expectedToScheduleOn, updatedPod.Spec.NodeName)
					assert.Equal(t, 1, len(infra.Fakes.K8s.Nodes().Items), "Expected 1 node (%v) after first loop", tt.expectedToScheduleOn)
				} else {
					assert.Empty(t, updatedPod.Spec.NodeName, "Expected to be not scheduled")
					assert.Equal(t, 0, len(infra.Fakes.K8s.Nodes().Items), "Expected no nodes created")
				}

				assert.Greater(t, infra.Fakes.FlexAdvisorClient.GetFetchCapacityCalls(), 0, "Expected Flex Advisor to be queried")
			})
		})
	}
}

func TestFlexAdvisorRightsizeTheResizeRequestToAvailableCapacity(t *testing.T) {
	const (
		enabled  = true
		disabled = false
	)
	for name, tt := range map[string]struct {
		ccc                   *v1.ComputeClass
		flexAdvisor           bool
		loopRuns              int
		nodePools             []*gke_api_beta.NodePool
		pods                  []*apiv1.Pod
		expectedPodScheduleOn map[string]string
		expectedNumberOfNodes int
	}{
		"no_flexadvisor_schedule_on_1st": {
			flexAdvisor: disabled,
			loopRuns:    1, // enough to create two nodes on a single node-pool
			ccc:         createCCCWithNodePoolsRules([]string{"testpool-1", "testpool-2"}),
			nodePools: []*gke_api_beta.NodePool{
				createEmptyNodePool("testpool-1", OneInstanceAvailableMachineType),
				createEmptyNodePool("testpool-2", AvailableMachineType),
			},
			pods: []*apiv1.Pod{ // one node fits only one pod
				tu.BuildTestPod("pod-1", 3000, 12000, tu.MarkUnschedulable(), withTestCCC),
				tu.BuildTestPod("pod-2", 3000, 12000, tu.MarkUnschedulable(), withTestCCC),
			},
			expectedPodScheduleOn: map[string]string{
				"pod-1": "pool-1",
				"pod-2": "pool-1",
			},
			expectedNumberOfNodes: 2,
		},
		"flexadvisor_schedule_on_2_nodepools": {
			flexAdvisor: enabled,
			loopRuns:    2, // we expect to scale two node-pools, one node each
			ccc:         createCCCWithNodePoolsRules([]string{"pool-1", "pool-2"}),
			nodePools: []*gke_api_beta.NodePool{
				createEmptyNodePool("pool-1", OneInstanceAvailableMachineType),
				createEmptyNodePool("pool-2", AvailableMachineType),
			},
			pods: []*apiv1.Pod{ // one node fits only one pod
				tu.BuildTestPod("pod-1", 3000, 12000, tu.MarkUnschedulable(), withTestCCC),
				tu.BuildTestPod("pod-2", 3000, 12000, tu.MarkUnschedulable(), withTestCCC),
			},
			expectedPodScheduleOn: map[string]string{
				"pod-1": "pool-1",
				"pod-2": "pool-2",
			},
			expectedNumberOfNodes: 2,
		},
	} {
		t.Run(name, func(t *testing.T) {
			nodePools := annotateNodePoolWithCCCLabel(tt.ccc.Name, tt.nodePools)

			overrides := []integration.Option[*options.AutoscalingOptions]{
				integration.WithMaxMemoryTotal(140 * 1024 * 1024 * 1024), //140 gb
			}
			if tt.flexAdvisor {
				overrides = append(overrides,
					integration.WithFlexAdvisorEnabled())
			}
			testConfig := integration.NewTestConfig().
				WithNodePools(nodePools...).
				WithCccCrds(tt.ccc).
				WithOverrides(overrides...)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(testCapacityGuidance()...)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)
				for _, pod := range tt.pods {
					infra.Fakes.K8s.AddPod(pod)
				}

				// run autoscaler loop the number of nodepools we have
				// to ensure we can scale all nodepools in test
				for i := 0; i < tt.loopRuns; i++ {
					integration_synctest.MustRunOnceAfter(t, autoscaler, 2*time.Second)
					infra.Fakes.RunScheduler(ctx, t)
				}

				assert.Len(t, infra.Fakes.K8s.Nodes().Items, tt.expectedNumberOfNodes)
				for podName, nodePoolName := range tt.expectedPodScheduleOn {
					updatedPod1, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, podName, metav1.GetOptions{})
					assert.NoError(t, err)
					if assert.NotEmpty(t, updatedPod1.Spec.NodeName, "Expected %v to be scheduled", podName) {
						assert.Contains(t, updatedPod1.Spec.NodeName, nodePoolName, "Expected to pod %v be scheduled on %v", podName, nodePoolName)
					}
				}
			})
		})
	}
}

// TestFlexAdvisorCapacityShortageRecovery verifies that the autoscaler correctly handles
// situations where a node pool has no availability according to Flex Advisor. It checks that pods
// are scheduled to an alternative available pool, and later, if the initially unavailable pool
// regains capacity, it can be used for new pods.
func TestFlexAdvisorCapacityShortageRecovery(t *testing.T) {
	nodePools := []*gke_api_beta.NodePool{
		createEmptyNodePool("pool-p1", ZeroCapacityRecommendationMachineType),
		createEmptyNodePool("pool-p2", AvailableMachineType),
	}
	ccc := createCCCWithNodePoolsRules([]string{"pool-p1", "pool-p2"})
	nodePools = annotateNodePoolWithCCCLabel(ccc.Name, nodePools)

	testConfig := integration.NewTestConfig().
		WithNodePools(nodePools...).
		WithCccCrds(ccc).
		WithOverrides(
			integration.WithMaxMemoryTotal(140*1024*1024*1024),
			integration.WithFlexAdvisorEnabled(),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		// Initial guidance: pool-p1 (ZeroCapacityRecommendationMachineType) has 0 capacity, pool-p2 (AvailableMachineType) has 10
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewFakeCapacityGuidanceForMachineType(ZeroCapacityRecommendationMachineType, 0, 0.5),
			fake.NewFakeCapacityGuidanceForMachineType(AvailableMachineType, 10, 0.5),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		// 1. Request scale up for pod-1. It should go to pool-p2 because there's no availability according to FA in pool-p1.
		pod1 := tu.BuildTestPod("pod-1", 3000, 12000, tu.MarkUnschedulable(), withTestCCC)
		infra.Fakes.K8s.AddPod(pod1)

		integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod1, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-1", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod1.Spec.NodeName, "Expected pod-1 to be scheduled")
		assert.Contains(t, updatedPod1.Spec.NodeName, "pool-p2", "Expected pod-1 to be scheduled on pool-p2, but got %s", updatedPod1.Spec.NodeName)

		// 2. Now pool-p1 becomes available. Update Flex Advisor guidance.
		infra.Fakes.FlexAdvisorClient.ClearCapacityGuidances()
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewFakeCapacityGuidanceForMachineType(ZeroCapacityRecommendationMachineType, 10, 0.5), // now available
			fake.NewFakeCapacityGuidanceForMachineType(AvailableMachineType, 10, 0.5),
		)

		// Request scale up for pod-2. It should go to pool-p1 because it is now available and has higher priority.
		pod2 := tu.BuildTestPod("pod-2", 3000, 12000, tu.MarkUnschedulable(), withTestCCC)
		infra.Fakes.K8s.AddPod(pod2)

		integration_synctest.MustRunOnceAfter(t, autoscaler, 15*time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod2, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-2", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod2.Spec.NodeName, "Expected pod-2 to be scheduled")
		assert.Contains(t, updatedPod2.Spec.NodeName, "pool-p1", "Expected pod-2 to be scheduled on pool-p1, but got %s", updatedPod2.Spec.NodeName)
	})
}
