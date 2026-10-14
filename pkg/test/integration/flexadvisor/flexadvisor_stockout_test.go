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
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestStockOutWithoutFlexAdvisor(t *testing.T) {
	ccc := ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-p1", "pool-p2").Build()
	nodePools := []*gke_api_beta.NodePool{
		integration.EmptyNodePool("pool-p1").WithMachineType(StockOutMachineType).WithCCCLabel(ccc.Name).Build(),
		integration.EmptyNodePool("pool-p2").WithMachineType(AvailableMachineType).WithCCCLabel(ccc.Name).Build(),
	}

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
		pod := tu.BuildTestPod("standard-pod", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(pod)

		// Loop 1: Should try pool-p1 and fail
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Verify standard-pod is NOT scheduled and no nodes are created
		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "standard-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Empty(t, updatedPod.Spec.NodeName, "Expected standard-pod to remain unschedulable after first loop")
		assert.Equal(t, 0, len(infra.Fakes.K8s.Nodes().Items), "Expected 0 nodes after first loop")

		// Loop 2: Should try pool-p2 and succeed
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
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
			ccc: ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1").Build(),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-1").WithMachineType(StockOutMachineType).WithCCCLabel("test-ccc").Build(),
			},
			expectedToScheduleOn: NoSchedule,
		},
		"first_priority_available": {
			ccc: ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1", "pool-2").Build(),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-1").WithMachineType(AvailableMachineType).WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-2").WithMachineType(StockOutMachineType).WithCCCLabel("test-ccc").Build(),
			},
			expectedToScheduleOn: "pool-1",
		},
		"fallback_to_available": {
			ccc: ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1", "pool-2").Build(),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-1").WithMachineType(StockOutMachineType).WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-2").WithMachineType(AvailableMachineType).WithCCCLabel("test-ccc").Build(),
			},
			expectedToScheduleOn: "pool-2",
		},
		"fallback_to_available_3rd_nodepool": {
			ccc: ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1", "pool-2", "pool-3").Build(),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-1").WithMachineType(StockOutMachineType).WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-2").WithMachineType(StockOutMachineType).WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-3").WithMachineType(AvailableMachineType).WithCCCLabel("test-ccc").Build(),
			},
			expectedToScheduleOn: "pool-3",
		},
		"flexadvisor_recommends_unavailable_machine": {
			ccc: ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1", "pool-2").Build(),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-1").WithMachineType(AvailableMachineType).WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-2").WithMachineType(AvailableMachineType).WithCCCLabel("test-ccc").Build(),
			},
			nodePoolNewInstanceError: map[string]cloudprovider.InstanceErrorInfo{
				"pool-1": stockOutError(),
			},
			expectedToScheduleOn: NoSchedule, // we're executing single step
		},
		"fallback_to_unknown_machine": {
			ccc: ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1", "pool-2", "pool-3").Build(),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-1").WithMachineType(StockOutMachineType).WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-2").WithMachineType(UnknownAvailabilityMachineType).WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-3").WithMachineType(AvailableMachineType).WithCCCLabel("test-ccc").Build(),
			},
			expectedToScheduleOn: "pool-2",
		},
	} {
		t.Run(name, func(t *testing.T) {
			testConfig := integration.NewTestConfig().
				WithNodePools(tt.nodePools...).
				WithCccCrds(tt.ccc).
				WithOverrides(
					integration.WithMaxMemoryTotal(140*1024*1024*1024),
					integration.WithFlexAdvisorEnabled(),
				)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(fake.NewGuidance(StockOutMachineType).WithCapacity(0))

				// Mock GCE errors
				for name, err := range tt.nodePoolNewInstanceError {
					// nodePool's name and MIG's name are the same
					infra.Fakes.GceService.SetCreateInstanceForMigError(name, err)
				}

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				// Now ask for 3000m. It will trigger scale up.
				pod := tu.BuildTestPod("standard-pod", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
				infra.Fakes.K8s.AddPod(pod)

				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
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
			ccc:         ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("testpool-1", "testpool-2").Build(),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("testpool-1").WithMachineType(OneInstanceAvailableMachineType).WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("testpool-2").WithMachineType(AvailableMachineType).WithCCCLabel("test-ccc").Build(),
			},
			pods: []*apiv1.Pod{ // one node fits only one pod
				tu.BuildTestPod("pod-1", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc")),
				tu.BuildTestPod("pod-2", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc")),
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
			ccc:         ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1", "pool-2").Build(),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-1").WithMachineType(OneInstanceAvailableMachineType).WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-2").WithMachineType(AvailableMachineType).WithCCCLabel("test-ccc").Build(),
			},
			pods: []*apiv1.Pod{ // one node fits only one pod
				tu.BuildTestPod("pod-1", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc")),
				tu.BuildTestPod("pod-2", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc")),
			},
			expectedPodScheduleOn: map[string]string{
				"pod-1": "pool-1",
				"pod-2": "pool-2",
			},
			expectedNumberOfNodes: 2,
		},
	} {
		t.Run(name, func(t *testing.T) {
			overrides := []integration.Option[*options.AutoscalingOptions]{
				integration.WithMaxMemoryTotal(140 * 1024 * 1024 * 1024), //140 gb
			}
			if tt.flexAdvisor {
				overrides = append(overrides,
					integration.WithFlexAdvisorEnabled())
			}
			testConfig := integration.NewTestConfig().
				WithNodePools(tt.nodePools...).
				WithCccCrds(tt.ccc).
				WithOverrides(overrides...)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(fake.NewGuidance(OneInstanceAvailableMachineType).WithCapacity(1))

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)
				for _, pod := range tt.pods {
					infra.Fakes.K8s.AddPod(pod)
				}

				// run autoscaler loop the number of nodepools we have
				// to ensure we can scale all nodepools in test
				for i := 0; i < tt.loopRuns; i++ {
					integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*time.Second)
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
	ccc := ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-p1", "pool-p2").Build()
	nodePools := []*gke_api_beta.NodePool{
		integration.EmptyNodePool("pool-p1").WithMachineType(StockOutMachineType).WithCCCLabel(ccc.Name).Build(),
		integration.EmptyNodePool("pool-p2").WithMachineType(AvailableMachineType).WithCCCLabel(ccc.Name).Build(),
	}

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

		// Initial guidance: pool-p1 (StockOutMachineType) has 0 capacity
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance(StockOutMachineType).WithCapacity(0),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		// 1. Request scale up for pod-1. It should go to pool-p2 because there's no availability according to FA in pool-p1.
		pod1 := tu.BuildTestPod("pod-1", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(pod1)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod1, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-1", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod1.Spec.NodeName, "Expected pod-1 to be scheduled")
		assert.Contains(t, updatedPod1.Spec.NodeName, "pool-p2", "Expected pod-1 to be scheduled on pool-p2, but got %s", updatedPod1.Spec.NodeName)

		// 2. Now pool-p1 becomes available. Clear Flex Advisor guidance so everything defaults to available.
		infra.Fakes.FlexAdvisorClient.ClearCapacityGuidances()

		// Request scale up for pod-2. It should go to pool-p1 because it is now available and has higher priority.
		pod2 := tu.BuildTestPod("pod-2", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(pod2)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 15*time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod2, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-2", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod2.Spec.NodeName, "Expected pod-2 to be scheduled")
		assert.Contains(t, updatedPod2.Spec.NodeName, "pool-p1", "Expected pod-2 to be scheduled on pool-p1, but got %s", updatedPod2.Spec.NodeName)
	})
}

// TestUncreatedNAPPriorityWithPartialZoneStockoutDoesNotInvertPriority verifies that when an uncreated
// NAP priority has partial zone stockouts (e.g. ZoneA=0, ZoneB=0, ZoneC=5), the autoscaler evaluates
// available capacity across all planned zones and scales up Priority 1 in ZoneC rather than falsely
// eliminating it and inverting priority to a lower priority (e2-standard-4).
func TestUncreatedNAPPriorityWithPartialZoneStockoutDoesNotInvertPriority(t *testing.T) {
	cccObj := ccc.NewComputeClassBuilder("test-ccc").
		WithNodePoolAutoCreation(true).
		WithPriorities(
			v1.Priority{
				MachineType: ptr.To("n2-standard-4"),
			},
			v1.Priority{
				MachineType: ptr.To("e2-standard-4"),
			},
		).
		Build()

	testConfig := integration.NewTestConfig().
		WithCccCrds(cccObj).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithAutoprovisioningLocations(ZoneA, ZoneB, ZoneC),
		).
		WithOverrides(
			integration.WithMaxMemoryTotal(140*1024*1024*1024),
			integration.WithAutoProvisioningEnabled(),
			integration.WithFlexAdvisorEnabled(),
			integration.WithBalanceSimilarNodeGroups(),
		).
		WithExperiments(experiments.FlexAdvisorNapZoneSetExpansionMinCAVersionFlag)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance("n2-standard-4").WithZone(ZoneA).WithCapacity(0).WithScore(0.5),
			fake.NewGuidance("n2-standard-4").WithZone(ZoneB).WithCapacity(0).WithScore(0.5),
			fake.NewGuidance("n2-standard-4").WithZone(ZoneC).WithCapacity(5).WithScore(0.5),
			fake.NewGuidance("e2-standard-4").WithZone(ZoneA).WithCapacity(10).WithScore(0.5),
			fake.NewGuidance("e2-standard-4").WithZone(ZoneB).WithCapacity(10).WithScore(0.5),
			fake.NewGuidance("e2-standard-4").WithZone(ZoneC).WithCapacity(10).WithScore(0.5),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		pod := tu.BuildTestPod("standard-pod", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(pod)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		assert.Equal(t, 1, len(infra.Fakes.K8s.Nodes().Items), "Expected 1 node to be created")
		node := infra.Fakes.K8s.Nodes().Items[0]
		assert.Equal(t, "n2-standard-4", node.Labels[apiv1.LabelInstanceTypeStable], "Expected node to be n2-standard-4 from Priority 1")
		assert.Equal(t, ZoneC, node.Labels[apiv1.LabelTopologyZone], "Expected node to be in ZoneC where capacity is available")

		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "standard-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected pod to be scheduled")
	})
}

// TestMultiNodeScaleUpSumsFACapacityAcrossPlannedZonesForUncreatedNAPNodePool verifies that binpacking
// estimation sums Flex Advisor capacity across all planned zones for an uncreated regional NAP candidate.
// When the experiment is enabled, 3 pods requiring 1 node each can all schedule onto n2-standard-4
// (capacity=1 in each of ZoneA, ZoneB, ZoneC). When disabled, the candidate is capped at 1 node (single zone),
// leaving the remaining pods pending after a single scale-up cycle.
func TestMultiNodeScaleUpSumsFACapacityAcrossPlannedZonesForUncreatedNAPNodePool(t *testing.T) {
	for name, tc := range map[string]struct {
		experimentEnabled     bool
		expectedN2Nodes       int
		expectedE2Nodes       int
		expectedScheduledPods int
	}{
		"experiment_enabled": {
			experimentEnabled:     true,
			expectedN2Nodes:       3,
			expectedE2Nodes:       0,
			expectedScheduledPods: 3,
		},
		"experiment_disabled": {
			experimentEnabled:     false,
			expectedN2Nodes:       1,
			expectedE2Nodes:       0,
			expectedScheduledPods: 1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			cccObj := ccc.NewComputeClassBuilder("test-ccc").
				WithNodePoolAutoCreation(true).
				WithPriorities(
					v1.Priority{
						MachineType: ptr.To("n2-standard-4"),
					},
					v1.Priority{
						MachineType: ptr.To("e2-standard-4"),
					},
				).
				Build()

			testConfig := integration.NewTestConfig().
				WithCccCrds(cccObj).
				WithClusterOverrides(
					integration.WithClusterAutoProvisioningEnabled(),
					integration.WithAutoprovisioningLocations(ZoneA, ZoneB, ZoneC),
				).
				WithOverrides(
					integration.WithMaxMemoryTotal(140*1024*1024*1024),
					integration.WithAutoProvisioningEnabled(),
					integration.WithFlexAdvisorEnabled(),
					integration.WithBalanceSimilarNodeGroups(),
				)

			if tc.experimentEnabled {
				testConfig = testConfig.WithExperiments(experiments.FlexAdvisorNapZoneSetExpansionMinCAVersionFlag)
			} else {
				testConfig = testConfig.WithExperimentOverrides(map[string]bool{
					experiments.FlexAdvisorNapZoneSetExpansionEnabledFlag: false,
				}, nil)
			}

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
					fake.NewGuidance("n2-standard-4").WithZone(ZoneA).WithCapacity(1).WithScore(0.5),
					fake.NewGuidance("n2-standard-4").WithZone(ZoneB).WithCapacity(1).WithScore(0.5),
					fake.NewGuidance("n2-standard-4").WithZone(ZoneC).WithCapacity(1).WithScore(0.5),
					fake.NewGuidance("e2-standard-4").WithZone(ZoneA).WithCapacity(10).WithScore(0.5),
					fake.NewGuidance("e2-standard-4").WithZone(ZoneB).WithCapacity(10).WithScore(0.5),
					fake.NewGuidance("e2-standard-4").WithZone(ZoneC).WithCapacity(10).WithScore(0.5),
				)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				for i := 1; i <= 3; i++ {
					pod := tu.BuildTestPod(fmt.Sprintf("pod-%d", i), 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
					infra.Fakes.K8s.AddPod(pod)
				}

				// Single scale-up cycle:
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
				infra.Fakes.RunScheduler(ctx, t)

				n2Count := 0
				e2Count := 0
				for _, node := range infra.Fakes.K8s.Nodes().Items {
					switch node.Labels[apiv1.LabelInstanceTypeStable] {
					case "n2-standard-4":
						n2Count++
					case "e2-standard-4":
						e2Count++
					}
				}
				assert.Equal(t, tc.expectedN2Nodes, n2Count, "Mismatch in n2-standard-4 node count")
				assert.Equal(t, tc.expectedE2Nodes, e2Count, "Mismatch in e2-standard-4 node count")

				scheduledPods := 0
				for i := 1; i <= 3; i++ {
					p, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, fmt.Sprintf("pod-%d", i), metav1.GetOptions{})
					assert.NoError(t, err)
					if p.Spec.NodeName != "" {
						scheduledPods++
					}
				}
				assert.Equal(t, tc.expectedScheduledPods, scheduledPods, "Mismatch in scheduled pods count in single cycle")
			})
		})
	}
}

// TestUncreatedNAPWithCccZonalPreferencesSubsetOfAutoprovisioningLocations verifies that an uncreated NAP
// candidate restricted by CCC Location.Zones to a subset of the autoprovisioning locations is evaluated only
// across the specified zones (injection always picks the representative zone from the specified zones), is
// created with a non-empty node pool name spanning only those zones, and is scaled up in one of them.
func TestUncreatedNAPWithCccZonalPreferencesSubsetOfAutoprovisioningLocations(t *testing.T) {
	cccObj := ccc.NewComputeClassBuilder("test-ccc").
		WithNodePoolAutoCreation(true).
		WithPriorities(
			v1.Priority{
				MachineType: ptr.To("n2-standard-4"),
				Location:    &v1.Location{Zones: []string{ZoneB, ZoneC}},
			},
		).
		Build()

	testConfig := integration.NewTestConfig().
		WithCccCrds(cccObj).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithAutoprovisioningLocations(ZoneA, ZoneB, ZoneC),
		).
		WithOverrides(
			integration.WithMaxMemoryTotal(140*1024*1024*1024),
			integration.WithAutoProvisioningEnabled(),
			integration.WithFlexAdvisorEnabled(),
			integration.WithBalanceSimilarNodeGroups(),
		).
		WithExperiments(experiments.FlexAdvisorNapZoneSetExpansionMinCAVersionFlag)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance("n2-standard-4").WithZone(ZoneA).WithCapacity(0).WithScore(0.5),
			fake.NewGuidance("n2-standard-4").WithZone(ZoneB).WithCapacity(5).WithScore(0.5),
			fake.NewGuidance("n2-standard-4").WithZone(ZoneC).WithCapacity(5).WithScore(0.5),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		infra.Fakes.K8s.AddPod(tu.BuildTestPod("zonal-pref-pod", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc")))

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		napNodePools := infra.Fakes.GkeService.GetAutoprovisionedNodePools()
		if assert.Len(t, napNodePools, 1, "Expected one NAP node pool to be created") {
			assert.NotEmpty(t, napNodePools[0].Name, "Expected NAP node pool to have a non-empty name")
			assert.Subset(t, []string{ZoneB, ZoneC}, napNodePools[0].Locations, "Expected NAP node pool to span only the CCC specified zones")
		}

		if assert.Len(t, infra.Fakes.K8s.Nodes().Items, 1, "Expected 1 node to be created") {
			node := infra.Fakes.K8s.Nodes().Items[0]
			assert.Contains(t, []string{ZoneB, ZoneC}, node.Labels[apiv1.LabelTopologyZone], "Expected node in one of the CCC specified zones")
		}

		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "zonal-pref-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected pod to be scheduled")
	})
}
