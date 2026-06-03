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
	compute "google.golang.org/api/compute/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	ccccrd "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// Test clusters with reservation, where FA returns no capacity
// for some nodepools.
func TestReservationAndFAReturnNoCapacitySinglePod(t *testing.T) {
	const NoSchedule = ""
	for name, tt := range map[string]struct {
		ccc                  ccccrd.CRD
		nodePools            []*gke_api_beta.NodePool
		reservations         []*compute.Reservation
		expectedToScheduleOn string
	}{
		"no_capacity_scale_up_through_reservation": {
			ccc: createCCCWithNodePoolsRules([]string{"pool-1"}),
			nodePools: []*gke_api_beta.NodePool{
				createEmptyNodePool("pool-1", StockOutMachineType),
			},
			reservations: []*compute.Reservation{
				reservations.BuildMultipleMachineReservation(StockOutMachineType, "us-central1-b", 0, 1),
			},
			expectedToScheduleOn: "pool-1",
		},
		"reservation_on_unknown_by_fa_machine_type": {
			ccc: createCCCWithNodePoolsRules([]string{"pool-1"}),
			nodePools: []*gke_api_beta.NodePool{
				createEmptyNodePool("pool-1", UnknownAvailabilityMachineType),
				createEmptyNodePool("pool-2", AvailableMachineType),
			},
			reservations: []*compute.Reservation{
				reservations.BuildMultipleMachineReservation(UnknownAvailabilityMachineType, "us-central1-b", 0, 1),
			},
			expectedToScheduleOn: "pool-1",
		},
		"reservation_on_available_machine_does_not_impact_scale_up": {
			ccc: createCCCWithNodePoolsRules([]string{"pool-1"}),
			nodePools: []*gke_api_beta.NodePool{
				createEmptyNodePool("pool-1", AvailableMachineType),
			},
			reservations: []*compute.Reservation{
				reservations.BuildMultipleMachineReservation(AvailableMachineType, "us-central1-b", 0, 1),
			},
			expectedToScheduleOn: "pool-1",
		},
		"reservation_in_wrong_zone": {
			ccc: createCCCWithNodePoolsRules([]string{"pool-1", "pool-2"}),
			nodePools: []*gke_api_beta.NodePool{
				createEmptyNodePool("pool-1", StockOutMachineType),
				createEmptyNodePool("pool-2", AvailableMachineType),
			},
			reservations: []*compute.Reservation{
				reservations.BuildMultipleMachineReservation(StockOutMachineType, "us-central1-a", 0, 1),
			},
			expectedToScheduleOn: "pool-2",
		},
		"reservation_in_wrong_region": {
			ccc: createCCCWithNodePoolsRules([]string{"pool-1", "pool-2"}),
			nodePools: []*gke_api_beta.NodePool{
				createEmptyNodePool("pool-1", StockOutMachineType),
				createEmptyNodePool("pool-2", AvailableMachineType),
			},
			reservations: []*compute.Reservation{
				reservations.BuildMultipleMachineReservation(StockOutMachineType, "us-east1-a", 0, 1),
			},
			expectedToScheduleOn: "pool-2",
		},
	} {
		t.Run(name, func(t *testing.T) {
			nodePools := annotateNodePoolWithCCCLabel(tt.ccc.Name(), tt.nodePools)
			testConfig := integration.NewTestConfig().
				WithNodePools(nodePools...).
				WithNpcCrds(tt.ccc).
				WithReservationsForDefaultProject(tt.reservations).
				WithOverrides(
					integration.WithMaxMemoryTotal(140*1024*1024*1024),
					integration.WithFlexAdvisorEnabled(),
				)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(testCapacityGuidance()...)

				stopCh := make(chan struct{})

				autoscaler, err := integration.SetupAutoscaler(t, ctx, testConfig, infra, stopCh)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel, stopCh)

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

func TestReservationAndFAReturnNoCapacityMultiplePods(t *testing.T) {
	const NoSchedule = ""
	for name, tt := range map[string]struct {
		ccc                   ccccrd.CRD
		reservations          []*compute.Reservation
		nodePools             []*gke_api_beta.NodePool
		pods                  []*apiv1.Pod
		expectedPodScheduleOn map[string]string
		expectedNumberOfNodes int
	}{
		"used_reservation_should_be_ignored": {
			ccc: createCCCWithNodePoolsRules([]string{"pool-1"}),
			nodePools: []*gke_api_beta.NodePool{
				createEmptyNodePool("pool-1", OneInstanceAvailableMachineType),
			},
			reservations: []*compute.Reservation{
				reservations.BuildMultipleMachineReservation(OneInstanceAvailableMachineType, "us-central1-b", 10, 10),
			},
			pods: []*apiv1.Pod{ // one node fits only one pod
				tu.BuildTestPod("pod-1", 3000, 12000, tu.MarkUnschedulable(), withTestCCC),
				tu.BuildTestPod("pod-2", 3000, 12000, tu.MarkUnschedulable(), withTestCCC),
			},
			expectedPodScheduleOn: map[string]string{
				"pod-1": "pool-1",   // one is taken from availability
				"pod-2": NoSchedule, // reservation is already consumed
			},
			expectedNumberOfNodes: 1,
		},
		"reservation_and_fa_capacity_lower_than_pods": {
			ccc: createCCCWithNodePoolsRules([]string{"pool-1"}),
			nodePools: []*gke_api_beta.NodePool{
				createEmptyNodePool("pool-1", StockOutMachineType),
			},
			reservations: []*compute.Reservation{
				reservations.BuildMultipleMachineReservation(StockOutMachineType, "us-central1-b", 0, 1),
			},
			pods: []*apiv1.Pod{ // one node fits only one pod
				tu.BuildTestPod("pod-1", 3000, 12000, tu.MarkUnschedulable(), withTestCCC),
				tu.BuildTestPod("pod-2", 3000, 12000, tu.MarkUnschedulable(), withTestCCC),
			},
			expectedPodScheduleOn: map[string]string{
				"pod-1": "pool-1",   // one is taken from reservation
				"pod-2": NoSchedule, // no available nodes
			},
			expectedNumberOfNodes: 1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			nodePools := annotateNodePoolWithCCCLabel(tt.ccc.Name(), tt.nodePools)

			testConfig := integration.NewTestConfig().
				WithNodePools(nodePools...).
				WithNpcCrds(tt.ccc).
				WithReservationsForDefaultProject(tt.reservations).
				WithOverrides(
					integration.WithMaxMemoryTotal(140*1024*1024*1024), //140 gb
					integration.WithFlexAdvisorEnabled(),
				)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(testCapacityGuidance()...)

				stopCh := make(chan struct{})

				autoscaler, err := integration.SetupAutoscaler(t, ctx, testConfig, infra, stopCh)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel, stopCh)
				for _, pod := range tt.pods {
					infra.Fakes.K8s.AddPod(pod)
				}

				// run autoscaler loop the number of nodepools we have
				// to ensure we can scale all nodepools in test
				integration_synctest.MustRunOnceAfter(t, autoscaler, 2*time.Second)
				infra.Fakes.RunScheduler(ctx, t)

				assert.Len(t, infra.Fakes.K8s.Nodes().Items, tt.expectedNumberOfNodes)
				for podName, nodePoolName := range tt.expectedPodScheduleOn {
					updatedPod1, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, podName, metav1.GetOptions{})
					assert.NoError(t, err)
					if nodePoolName != NoSchedule {
						if assert.NotEmpty(t, updatedPod1.Spec.NodeName, "Expected %v to be scheduled", podName) {
							assert.Contains(t, updatedPod1.Spec.NodeName, nodePoolName, "Expected to pod %v be scheduled on %v", podName, nodePoolName)
						}
					} else {
						assert.Empty(t, updatedPod1.Spec.NodeName, "Expected %v to not be scheduled, but it was on %v", podName, updatedPod1.Spec.NodeName)
					}
				}
			})
		})
	}
}
