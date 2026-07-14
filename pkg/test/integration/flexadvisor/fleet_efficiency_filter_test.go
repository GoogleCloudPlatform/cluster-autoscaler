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
	compute "google.golang.org/api/compute/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"k8s.io/utils/ptr"
)

func TestFleetEfficiency(t *testing.T) {
	lowestCost := v1.AllocationStrategyLowestCost
	fleetEfficiency := v1.AllocationStrategyFleetEfficiency

	testCases := map[string]struct {
		priorityStrategy *v1.AllocationStrategy
		strategyDefaults *v1.AllocationStrategyDefaults
		nodePools        []*gke_api_beta.NodePool
		fakeGuidances    []fake.CapacityGuidance
		reservations     []*compute.Reservation
		experimentFlags  map[string]bool
		provisioningMode instanceavailability.ProvisioningMode
		expectedNodePool string
	}{
		"no strategy - assuming lowest cost": {
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-low-preference").WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference").WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
			},
			fakeGuidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(0.2),
				fake.NewGuidance("e2-standard-8").WithScore(0.9),
			},
			expectedNodePool: "pool-low-preference",
		},
		"fleet efficiency strategy in defaults - use fleet efficiency": {
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-low-preference").WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference").WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
			},
			fakeGuidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(0.2),
				fake.NewGuidance("e2-standard-8").WithScore(0.9),
			},
			strategyDefaults: &v1.AllocationStrategyDefaults{
				OnDemand: &fleetEfficiency,
			},
			expectedNodePool: "pool-high-preference",
		},
		"fleet efficiency strategy in defaults, experiment disabled - fall back to lowest cost": {
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-low-preference").WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference").WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
			},
			fakeGuidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(0.2),
				fake.NewGuidance("e2-standard-8").WithScore(0.9),
			},
			strategyDefaults: &v1.AllocationStrategyDefaults{
				OnDemand:  &fleetEfficiency,
				FlexStart: &fleetEfficiency,
				Spot:      &fleetEfficiency,
			},
			experimentFlags: map[string]bool{
				experiments.FleetEfficiencyStrategyEnabledFlag: false,
			},
			expectedNodePool: "pool-low-preference",
		},
		"fleet efficiency strategy in priority - use fleet efficiency": {
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-low-preference").WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference").WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
			},
			fakeGuidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(0.2),
				fake.NewGuidance("e2-standard-8").WithScore(0.9),
			},
			priorityStrategy: &fleetEfficiency,
			expectedNodePool: "pool-high-preference",
		},
		"override fleet efficiency strategy with lowest cost - use lowest cost": {
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-low-preference").WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference").WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
			},
			fakeGuidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(0.2),
				fake.NewGuidance("e2-standard-8").WithScore(0.9),
			},
			strategyDefaults: &v1.AllocationStrategyDefaults{
				OnDemand: &fleetEfficiency,
			},
			priorityStrategy: &lowestCost,
			expectedNodePool: "pool-low-preference",
		},
		"override lowest cost strategy with fleet efficiency - use fleet efficiency": {
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-low-preference").WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference").WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
			},
			fakeGuidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(0.2),
				fake.NewGuidance("e2-standard-8").WithScore(0.9),
			},
			strategyDefaults: &v1.AllocationStrategyDefaults{
				OnDemand: &lowestCost,
			},
			priorityStrategy: &fleetEfficiency,
			expectedNodePool: "pool-high-preference",
		},
		"Spot VMs, fleet efficiency - use fleet efficiency": {
			strategyDefaults: &v1.AllocationStrategyDefaults{
				OnDemand:  &lowestCost,
				Spot:      &fleetEfficiency,
				FlexStart: &lowestCost,
			},
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-spot-low-preference").WithMachineType("e2-standard-4").WithSpot().WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-spot-high-preference").WithMachineType("e2-standard-8").WithSpot().WithCCCLabel("test-ccc").Build(),
			},
			fakeGuidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(0.2),
				fake.NewGuidance("e2-standard-8").WithScore(0.9),
			},
			provisioningMode: instanceavailability.Spot,
			expectedNodePool: "pool-spot-high-preference",
		},
		"Spot VMs, lowest cost - use lowest cost": {
			strategyDefaults: &v1.AllocationStrategyDefaults{
				OnDemand:  &lowestCost,
				Spot:      &lowestCost,
				FlexStart: &lowestCost,
			},
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-spot-low-preference").WithMachineType("e2-standard-4").WithSpot().WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-spot-high-preference").WithMachineType("e2-standard-8").WithSpot().WithCCCLabel("test-ccc").Build(),
			},
			fakeGuidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(0.2),
				fake.NewGuidance("e2-standard-8").WithScore(0.9),
			},
			provisioningMode: instanceavailability.Spot,
			expectedNodePool: "pool-spot-low-preference",
		},
		"FlexStart workloads, fleet efficiency - use fleet efficiency": {
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-low-preference").WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference").WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
			},
			fakeGuidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(0.2),
				fake.NewGuidance("e2-standard-8").WithScore(0.9),
			},
			strategyDefaults: &v1.AllocationStrategyDefaults{
				OnDemand:  &lowestCost,
				Spot:      &lowestCost,
				FlexStart: &fleetEfficiency,
			},
			provisioningMode: instanceavailability.FlexStart,
			expectedNodePool: "pool-high-preference",
		},
		"found a matching reservation - fall back to lowest cost": {
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-low-preference").WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference").WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
			},
			fakeGuidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(0.2),
				fake.NewGuidance("e2-standard-8").WithScore(0.9),
			},
			strategyDefaults: &v1.AllocationStrategyDefaults{
				OnDemand:  &fleetEfficiency,
				FlexStart: &fleetEfficiency,
				Spot:      &fleetEfficiency,
			},
			reservations: []*compute.Reservation{
				reservations.BuildMultipleMachineReservation("e2-standard-4", ZoneB, 0, 1),
			},
			provisioningMode: instanceavailability.FlexStart,
			expectedNodePool: "pool-low-preference",
		},
		"multiple options with same fleet efficiency - tie break with lowest cost": {
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-low-preference").WithMachineType("n2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-mid-preference").WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference-low-cost").WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference-high-cost").WithMachineType("n1-standard-4").WithCCCLabel("test-ccc").Build(),
			},
			fakeGuidances: []fake.CapacityGuidance{
				fake.NewGuidance("n2-standard-4").WithScore(0.2),
				fake.NewGuidance("e2-standard-8").WithScore(0.5),
				fake.NewGuidance("e2-standard-4").WithScore(0.9),
				fake.NewGuidance("n1-standard-4").WithScore(0.9),
			},
			priorityStrategy: &fleetEfficiency,
			expectedNodePool: "pool-high-preference-low-cost",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var cccPriorityFlexStart *v1.FlexStart
			if tc.provisioningMode == instanceavailability.FlexStart {
				cccPriorityFlexStart = &v1.FlexStart{Enabled: true}
			}
			nodepoolNames := make([]string, 0, len(tc.nodePools))
			for _, np := range tc.nodePools {
				nodepoolNames = append(nodepoolNames, np.Name)
			}
			cccCrd := ccc.NewComputeClassBuilder("test-ccc").
				WithAllocationStrategyDefaults(tc.strategyDefaults).
				WithPriorities(
					v1.Priority{
						Nodepools:          nodepoolNames,
						PriorityScore:      ptr.To(100),
						AllocationStrategy: tc.priorityStrategy,
						FlexStart:          cccPriorityFlexStart,
						Spot:               ptr.To(tc.provisioningMode == instanceavailability.Spot),
					},
				).
				Build()

			testConfig := integration.NewTestConfig().
				WithNodePools(tc.nodePools...).
				WithCccCrds(cccCrd).
				WithExperimentOverrides(tc.experimentFlags, map[string]string{}).
				WithReservationsForDefaultProject(tc.reservations).
				WithOverrides(
					integration.WithMaxMemoryTotal(140*1024*1024*1024),
					integration.WithFlexAdvisorEnabled(),
				)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(tc.fakeGuidances...)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				infra.Fakes.K8s.AddPod(tu.BuildTestPod("fe-pod", 3000, 12000, pod.WithCCC("test-ccc"), tu.MarkUnschedulable(), pod.WithProvisioningMode(tc.provisioningMode)))

				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
				assert.Greater(t, infra.Fakes.FlexAdvisorClient.GetFetchCapacityCalls(), 0, "Expected FlexAdvisor to be queried")
				infra.Fakes.RunScheduler(ctx, t)

				updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "fe-pod", metav1.GetOptions{})
				assert.NoError(t, err)
				assert.Contains(t, updatedPod.Spec.NodeName, tc.expectedNodePool, "Expected pod to be scheduled on %s", tc.expectedNodePool)
			})
		})
	}
}
