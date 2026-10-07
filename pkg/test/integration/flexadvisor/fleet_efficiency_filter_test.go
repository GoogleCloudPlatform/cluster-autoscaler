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
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"k8s.io/utils/ptr"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"

	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
)

func TestFleetEfficiency(t *testing.T) {
	lowestCost := v1.AllocationStrategyLowestCost
	fleetEfficiency := v1.AllocationStrategyFleetEfficiency

	testCases := map[string]struct {
		priorityStrategy       *v1.AllocationStrategy
		strategyDefaults       *v1.AllocationStrategyDefaults
		nodePools              []*gke_api_beta.NodePool
		fakeGuidances          []fake.CapacityGuidance
		reservations           []*compute.Reservation
		experimentFlags        map[string]bool
		stringExperimentFlags  map[string]string
		clusterDefaultStrategy internalopts.ClusterDefaultAllocationStrategy
		provisioningMode       instanceavailability.ProvisioningMode
		expectedNodePool       string
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
		"cluster default via flag is fleet efficiency - use fleet efficiency": {
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-low-preference").WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference").WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
			},
			fakeGuidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(0.2),
				fake.NewGuidance("e2-standard-8").WithScore(0.9),
			},
			clusterDefaultStrategy: internalopts.ClusterDefaultAllocationStrategyFleetEfficiency,
			expectedNodePool:       "pool-high-preference",
		},
		"cluster default via experiment is fleet efficiency - use fleet efficiency": {
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-low-preference").WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference").WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
			},
			fakeGuidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(0.2),
				fake.NewGuidance("e2-standard-8").WithScore(0.9),
			},
			stringExperimentFlags: map[string]string{experiments.ClusterDefaultAllocationStrategyFlag: "fleet-efficiency"},
			expectedNodePool:      "pool-high-preference",
		},
		"cluster default via flag is fleet efficiency, CCC overrides with lowest cost - use lowest cost": {
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-low-preference").WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference").WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
			},
			fakeGuidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(0.2),
				fake.NewGuidance("e2-standard-8").WithScore(0.9),
			},
			clusterDefaultStrategy: internalopts.ClusterDefaultAllocationStrategyFleetEfficiency,
			priorityStrategy:       &lowestCost,
			expectedNodePool:       "pool-low-preference",
		},
		"cluster default via experiment is fleet efficiency, CCC overrides with lowest cost - use lowest cost": {
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-low-preference").WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference").WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
			},
			fakeGuidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(0.2),
				fake.NewGuidance("e2-standard-8").WithScore(0.9),
			},
			stringExperimentFlags: map[string]string{experiments.ClusterDefaultAllocationStrategyFlag: "fleet-efficiency"},
			priorityStrategy:      &lowestCost,
			expectedNodePool:      "pool-low-preference",
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
				integration.EmptyNodePool("pool-low-preference").WithOptions(integration.WithFlexStartNodePool).WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference").WithOptions(integration.WithFlexStartNodePool).WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
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
			experimentFlags: map[string]bool{
				experiments.FlexAdvisorDWSEnabledFlag: true,
			},
			stringExperimentFlags: map[string]string{
				experiments.FlexStartNonQueuedEnabledFlag:  "0.0.0",
				experiments.FlexAdvisorDWSMinCAVersionFlag: "0.0.0",
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
		"ANY_RESERVATION affinity without matching reservations - use fleet efficiency": {
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-low-preference").WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").WithOptions(withReservationAffinity(gkeclient.ReservationAffinityAny)).Build(),
				integration.EmptyNodePool("pool-high-preference").WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").WithOptions(withReservationAffinity(gkeclient.ReservationAffinityAny)).Build(),
			},
			fakeGuidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(0.2),
				fake.NewGuidance("e2-standard-8").WithScore(0.9),
			},
			priorityStrategy: &fleetEfficiency,
			expectedNodePool: "pool-high-preference",
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

			overrides := []integration.Option[*internalopts.AutoscalingOptions]{
				integration.WithMaxMemoryTotal(140 * 1024 * 1024 * 1024),
				integration.WithFlexAdvisorEnabled(),
				integration.WithBalanceSimilarNodeGroups(),
			}
			if tc.clusterDefaultStrategy != "" {
				overrides = append(overrides, func(o *internalopts.AutoscalingOptions) *internalopts.AutoscalingOptions {
					o.ClusterDefaultAllocationStrategy = tc.clusterDefaultStrategy
					return o
				})
			}

			testConfig := integration.NewTestConfig().
				WithNodePools(tc.nodePools...).
				WithCccCrds(cccCrd).
				WithExperimentOverrides(tc.experimentFlags, tc.stringExperimentFlags).
				WithReservationsForDefaultProject(tc.reservations).
				WithOverrides(overrides...)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(tc.fakeGuidances...)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "test-ccc")

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

// TestFleetEfficiency_ExistingVsNapTieBreak tests that an uncreated NAP candidate does not unfairly
// beat an existing regional node pool when zonal scores differ.
// In a multi-zone cluster, the existing regional pool is scored as the average across its zones.
// An uncreated NAP candidate (which GKE will create as a regional pool across its target locations)
// is also scored as the average across those locations, rather than cherry-picking the single zone
// of its candidate virtual MIG. Because both receive equal fleet efficiency scores, they tie, and
// tie-breaking defers to gkePrice, where the existing node pool is chosen (since uncreated node groups
// incur the notExistCoefficient cost penalty).
//
// Pods are zone-agnostic, so NAP injects a single candidate in a randomly selected zone. The result
// must not depend on which zone was picked.
func TestFleetEfficiency_ExistingVsNapTieBreak(t *testing.T) {
	fleetEfficiency := v1.AllocationStrategyFleetEfficiency

	existingPool := integration.EmptyNodePool("pool-existing").
		WithMachineType("n2-standard-4").
		WithLocations(ZoneA, ZoneB, ZoneC).
		WithCCCLabel("test-ccc").
		Build()

	cccCrd := ccc.NewComputeClassBuilder("test-ccc").
		WithNodePoolAutoCreation(true).
		WithPriorities(
			v1.Priority{
				Nodepools:          []string{"pool-existing"},
				PriorityScore:      ptr.To(100),
				AllocationStrategy: &fleetEfficiency,
			},
			v1.Priority{
				MachineType:        ptr.To("n2-standard-4"),
				PriorityScore:      ptr.To(100),
				AllocationStrategy: &fleetEfficiency,
			},
		).
		Build()

	testConfig := integration.NewTestConfig().
		WithNodePools(existingPool).
		WithCccCrds(cccCrd).
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

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		// Set zonal preference scores such that ZoneA has a very high score (0.9), but ZoneB and ZoneC have low scores (0.3).
		// Regional average across ZoneA, ZoneB, ZoneC is (0.9 + 0.3 + 0.3)/3 = 0.5.
		//
		// Every uncreated NAP candidate should be scored as the average across its target locations (0.5),
		// matching the regional score of pool-existing. They tie in fleet efficiency and fall back to gkePrice,
		// where pool-existing wins.
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance("n2-standard-4").WithZone(ZoneA).WithScore(0.9),
			fake.NewGuidance("n2-standard-4").WithZone(ZoneB).WithScore(0.3),
			fake.NewGuidance("n2-standard-4").WithZone(ZoneC).WithScore(0.3),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "test-ccc")

		pod := tu.BuildTestPod("fe-pod", 3000, 12000, pod.WithCCC("test-ccc"), tu.MarkUnschedulable())
		infra.Fakes.K8s.AddPod(pod)

		// Run autoscaler cycle.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		assert.Greater(t, infra.Fakes.FlexAdvisorClient.GetFetchCapacityCalls(), 0, "Expected FlexAdvisor to be queried")
		infra.Fakes.RunScheduler(ctx, t)

		// Verify the pod was scheduled on the existing node pool rather than a newly created NAP node pool.
		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "fe-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected fe-pod to be scheduled")
		assert.Contains(t, updatedPod.Spec.NodeName, "pool-existing", "Expected pod to be scheduled on existing node pool 'pool-existing', but got %s", updatedPod.Spec.NodeName)

		// Verify that no new node pool was created by NAP.
		cluster, err := infra.Fakes.GkeService.GetCluster("projects/test-project/locations/us-central1/clusters/test-cluster")
		assert.NoError(t, err)
		assert.Equal(t, 1, len(cluster.NodePools), "Expected no new NAP node pool to be created; only pool-existing should exist")
		assert.Equal(t, "pool-existing", cluster.NodePools[0].Name)
	})
}

// TestFleetEfficiency_NapInjectedNodePoolReusedOnSubsequentCycle tests that a newly created
// NAP node pool from a first scale-up cycle is reused in subsequent cycles when the same ComputeClass
// priority (with a single machine type) triggers scale-up again.
//
// In a multi-zone cluster with varying zonal preference scores (e.g. ZoneA=0.9, ZoneB=0.3, ZoneC=0.3),
// the first pod causes NAP to inject and create a regional node pool across ZoneA, ZoneB, ZoneC.
// When a second pod arrives in a later cycle, the newly existing regional pool has an average score of 0.5.
// Any new NAP candidate also targets ZoneA, ZoneB, ZoneC and thus receives the same average score of 0.5.
// Because they tie in fleet efficiency, gkePrice tie-breaking prefers the existing node pool,
// proving that NAP does not repeatedly create new redundant node pools on each cycle.
//
// Pods are zone-agnostic, so NAP injects a single candidate in a randomly selected zone. The result
// must not depend on which zone was picked.
func TestFleetEfficiency_NapInjectedNodePoolReusedOnSubsequentCycle(t *testing.T) {
	fleetEfficiency := v1.AllocationStrategyFleetEfficiency

	cccCrd := ccc.NewComputeClassBuilder("test-ccc").
		WithNodePoolAutoCreation(true).
		WithPriorities(
			v1.Priority{
				MachineType:        ptr.To("n2-standard-4"),
				PriorityScore:      ptr.To(100),
				AllocationStrategy: &fleetEfficiency,
			},
		).
		Build()

	testConfig := integration.NewTestConfig().
		WithCccCrds(cccCrd).
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

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance("n2-standard-4").WithZone(ZoneA).WithScore(0.9),
			fake.NewGuidance("n2-standard-4").WithZone(ZoneB).WithScore(0.3),
			fake.NewGuidance("n2-standard-4").WithZone(ZoneC).WithScore(0.3),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "test-ccc")

		// --- Pass 1: First pod arrives ---
		pod1 := tu.BuildTestPod("fe-pod-1", 3000, 12000, pod.WithCCC("test-ccc"), tu.MarkUnschedulable())
		infra.Fakes.K8s.AddPod(pod1)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Verify first pod was scheduled and exactly 1 node pool was created by NAP.
		updatedPod1, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "fe-pod-1", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod1.Spec.NodeName, "Expected fe-pod-1 to be scheduled after pass 1")

		cluster, err := infra.Fakes.GkeService.GetCluster("projects/test-project/locations/us-central1/clusters/test-cluster")
		assert.NoError(t, err)
		assert.Equal(t, 1, len(cluster.NodePools), "Expected exactly 1 NAP node pool to be created after pass 1")
		createdNodePoolName := cluster.NodePools[0].Name
		assert.Contains(t, updatedPod1.Spec.NodeName, createdNodePoolName)

		// --- Pass 2: Second pod arrives ---
		pod2 := tu.BuildTestPod("fe-pod-2", 3000, 12000, pod.WithCCC("test-ccc"), tu.MarkUnschedulable())
		infra.Fakes.K8s.AddPod(pod2)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Verify second pod was scheduled on the same node pool that was created in pass 1.
		updatedPod2, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "fe-pod-2", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod2.Spec.NodeName, "Expected fe-pod-2 to be scheduled after pass 2")
		assert.Contains(t, updatedPod2.Spec.NodeName, createdNodePoolName, "Expected fe-pod-2 to be scheduled on existing node pool %s, but got %s", createdNodePoolName, updatedPod2.Spec.NodeName)

		// Verify that no second node pool was created; the existing node pool was reused.
		cluster, err = infra.Fakes.GkeService.GetCluster("projects/test-project/locations/us-central1/clusters/test-cluster")
		assert.NoError(t, err)
		assert.Equal(t, 1, len(cluster.NodePools), "Expected still exactly 1 node pool in cluster; NAP should not create a new pool when existing pool can be scaled up")
		assert.Equal(t, createdNodePoolName, cluster.NodePools[0].Name)

		// Verify that total nodes in the cluster is now 2 (1 from each pass, both in the same node pool).
		nodes, err := infra.Fakes.K8s.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(nodes.Items), "Expected 2 nodes in total across the two scale-up passes")
	})
}

// TestFleetEfficiency_ZonalPodReusesExistingRegionalNodePool tests that
// when a pending pod is constrained to a single zone (ZoneB, where score=0.3 is below the regional
// average of 0.5), fleet-efficiency falls back to lowest-cost and reuses the existing regional
// node pool in ZoneB rather than allowing an uncreated NAP candidate of the same machine type to
// inject a redundant new node pool.
func TestFleetEfficiency_ZonalPodReusesExistingRegionalNodePool(t *testing.T) {
	fleetEfficiency := v1.AllocationStrategyFleetEfficiency

	existingPool := integration.EmptyNodePool("pool-existing").
		WithMachineType("n2-standard-4").
		WithLocations(ZoneA, ZoneB, ZoneC).
		WithCCCLabel("test-ccc").
		Build()

	cccCrd := ccc.NewComputeClassBuilder("test-ccc").
		WithNodePoolAutoCreation(true).
		WithPriorities(
			v1.Priority{
				Nodepools:          []string{"pool-existing"},
				PriorityScore:      ptr.To(100),
				AllocationStrategy: &fleetEfficiency,
			},
			v1.Priority{
				MachineType:        ptr.To("n2-standard-4"),
				PriorityScore:      ptr.To(100),
				AllocationStrategy: &fleetEfficiency,
			},
		).
		Build()

	testConfig := integration.NewTestConfig().
		WithNodePools(existingPool).
		WithCccCrds(cccCrd).
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

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance("n2-standard-4").WithZone(ZoneA).WithScore(0.9),
			fake.NewGuidance("n2-standard-4").WithZone(ZoneB).WithScore(0.3),
			fake.NewGuidance("n2-standard-4").WithZone(ZoneC).WithScore(0.3),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "test-ccc")

		// Pod is constrained to ZoneB (e.g. via nodeSelector or zonal PVC).
		zonalPod := tu.BuildTestPod(
			"fe-zonal-pod", 3000, 12000,
			pod.WithCCC("test-ccc"),
			pod.WithNodeSelectorEntry("topology.kubernetes.io/zone", ZoneB),
			tu.MarkUnschedulable(),
		)
		infra.Fakes.K8s.AddPod(zonalPod)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Verify the pod was scheduled on the existing regional node pool's ZoneB MIG rather than a newly created NAP node pool.
		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "fe-zonal-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected fe-zonal-pod to be scheduled")
		assert.Contains(t, updatedPod.Spec.NodeName, "pool-existing", "Expected fe-zonal-pod to be scheduled on existing node pool 'pool-existing', but got %s", updatedPod.Spec.NodeName)

		cluster, err := infra.Fakes.GkeService.GetCluster("projects/test-project/locations/us-central1/clusters/test-cluster")
		assert.NoError(t, err)
		assert.Equal(t, 1, len(cluster.NodePools), "Expected no new NAP node pool to be created when pool-existing already covers ZoneB")
		assert.Equal(t, "pool-existing", cluster.NodePools[0].Name)
	})
}

// TestFleetEfficiency_NapCandidateWithExplicitCccLocations tests that
// when a ComputeClass rule specifies explicit Location.Zones ([ZoneA, ZoneB]) that are a subset of
// the cluster's AutoprovisioningLocations ([ZoneA, ZoneB, ZoneC]), the uncreated NAP candidate is
// scored only across the specified zones ([ZoneA, ZoneB]) rather than all AutoprovisioningLocations.
func TestFleetEfficiency_NapCandidateWithExplicitCccLocations(t *testing.T) {
	fleetEfficiency := v1.AllocationStrategyFleetEfficiency

	existingPool := integration.EmptyNodePool("pool-existing").
		WithMachineType("e2-standard-4").
		WithLocations(ZoneA).
		WithCCCLabel("test-ccc").
		Build()

	cccCrd := ccc.NewComputeClassBuilder("test-ccc").
		WithNodePoolAutoCreation(true).
		WithPriorities(
			v1.Priority{
				Nodepools:          []string{"pool-existing"},
				PriorityScore:      ptr.To(100),
				AllocationStrategy: &fleetEfficiency,
			},
			v1.Priority{
				MachineType:        ptr.To("n2-standard-4"),
				PriorityScore:      ptr.To(100),
				AllocationStrategy: &fleetEfficiency,
				Location:           &v1.Location{Zones: []string{ZoneA, ZoneB}},
			},
		).
		Build()

	testConfig := integration.NewTestConfig().
		WithNodePools(existingPool).
		WithCccCrds(cccCrd).
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

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		// n2-standard-4 across specified [ZoneA, ZoneB] has average (0.8 + 0.6)/2 = 0.7
		// (whereas across all 3 autoprovisioning locations [ZoneA, ZoneB, ZoneC] it would be (0.8+0.6+0.1)/3 = 0.5).
		// pool-existing (e2-standard-4 in ZoneA) has score 0.65.
		// Because n2-standard-4 is scored only across [ZoneA, ZoneB] (0.7 > 0.65), n2-standard-4 wins.
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance("n2-standard-4").WithZone(ZoneA).WithScore(0.8),
			fake.NewGuidance("n2-standard-4").WithZone(ZoneB).WithScore(0.6),
			fake.NewGuidance("n2-standard-4").WithZone(ZoneC).WithScore(0.1),
			fake.NewGuidance("e2-standard-4").WithZone(ZoneA).WithScore(0.65),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "test-ccc")

		pod1 := tu.BuildTestPod("fe-pod", 3000, 12000, pod.WithCCC("test-ccc"), tu.MarkUnschedulable())
		infra.Fakes.K8s.AddPod(pod1)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "fe-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected fe-pod to be scheduled")
		assert.Contains(t, updatedPod.Spec.NodeName, "n2-standard-4", "Expected NAP candidate n2-standard-4 (avg 0.7 across [ZoneA, ZoneB]) to beat pool-existing (0.65), but got %s", updatedPod.Spec.NodeName)
	})
}

// withReservationAffinity sets the node pool's reservation affinity consume type.
func withReservationAffinity(consumeReservationType string) integration.Option[*gke_api_beta.NodePool] {
	return func(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
		if np.Config == nil {
			np.Config = &gke_api_beta.NodeConfig{}
		}
		np.Config.ReservationAffinity = &gke_api_beta.ReservationAffinity{ConsumeReservationType: consumeReservationType}
		return np
	}
}

func withNodeAffinityNotIn(key string, values ...string) func(p *apiv1.Pod) {
	return func(p *apiv1.Pod) {
		if p.Spec.Affinity == nil {
			p.Spec.Affinity = &apiv1.Affinity{}
		}
		if p.Spec.Affinity.NodeAffinity == nil {
			p.Spec.Affinity.NodeAffinity = &apiv1.NodeAffinity{}
		}
		p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &apiv1.NodeSelector{
			NodeSelectorTerms: []apiv1.NodeSelectorTerm{
				{
					MatchExpressions: []apiv1.NodeSelectorRequirement{
						{
							Key:      key,
							Operator: apiv1.NodeSelectorOpNotIn,
							Values:   values,
						},
					},
				},
			},
		}
	}
}

func withNodeAffinityIn(key string, values ...string) func(p *apiv1.Pod) {
	return func(p *apiv1.Pod) {
		if p.Spec.Affinity == nil {
			p.Spec.Affinity = &apiv1.Affinity{}
		}
		if p.Spec.Affinity.NodeAffinity == nil {
			p.Spec.Affinity.NodeAffinity = &apiv1.NodeAffinity{}
		}
		p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &apiv1.NodeSelector{
			NodeSelectorTerms: []apiv1.NodeSelectorTerm{
				{
					MatchExpressions: []apiv1.NodeSelectorRequirement{
						{
							Key:      key,
							Operator: apiv1.NodeSelectorOpIn,
							Values:   values,
						},
					},
				},
			},
		}
	}
}

// TestFleetEfficiency_FallbackUnsupported tests that when any unsupported constraint
// is present on a pod or option (e.g. zonal nodeSelector, beta failure domain label, zonal nodeAffinity,
// zonal podAffinity, zonal podAntiAffinity, zonal topologySpread, stateful pod, or compact placement),
// fleet-efficiency safely falls back to lowest-cost and records the "unsupported" metric reason.
// Because of the lowest-cost fallback, the cheaper machine type (e2-standard-4) is selected
// over the more expensive one (n2-standard-4), despite n2-standard-4 having a much higher availability score.
func TestFleetEfficiency_FallbackUnsupported(t *testing.T) {
	fleetEfficiency := v1.AllocationStrategyFleetEfficiency

	testCases := map[string]struct {
		podCustomizer   func(*apiv1.Pod)
		setupExtraObjs  func(ctx context.Context, infra *integration.TestInfrastructure)
		customNodePools []*gke_api_beta.NodePool
	}{
		"zonal NodeSelector (topology.kubernetes.io/zone)": {
			podCustomizer: func(p *apiv1.Pod) {
				pod.WithNodeSelectorEntry(apiv1.LabelTopologyZone, ZoneB)(p)
			},
		},
		"legacy beta zonal NodeSelector (failure-domain.beta.kubernetes.io/zone)": {
			podCustomizer: func(p *apiv1.Pod) {
				pod.WithNodeSelectorEntry(apiv1.LabelFailureDomainBetaZone, ZoneB)(p)
			},
		},
		"zonal NodeAffinity In": {
			podCustomizer: func(p *apiv1.Pod) {
				withNodeAffinityIn(apiv1.LabelTopologyZone, ZoneB)(p)
			},
		},
		"zonal NodeAffinity NotIn": {
			podCustomizer: func(p *apiv1.Pod) {
				withNodeAffinityNotIn(apiv1.LabelTopologyZone, ZoneA, ZoneC)(p)
			},
		},
		"zonal PodAntiAffinity": {
			podCustomizer: func(p *apiv1.Pod) {
				p.Spec.Affinity = &apiv1.Affinity{
					PodAntiAffinity: &apiv1.PodAntiAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: []apiv1.PodAffinityTerm{
							{
								LabelSelector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "non-existent-app"},
								},
								TopologyKey: apiv1.LabelTopologyZone,
							},
						},
					},
				}
			},
		},
		"zonal TopologySpreadConstraints": {
			podCustomizer: func(p *apiv1.Pod) {
				p.Spec.TopologySpreadConstraints = []apiv1.TopologySpreadConstraint{
					{
						MaxSkew:           1,
						TopologyKey:       apiv1.LabelTopologyZone,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": "spread-app"},
						},
					},
				}
			},
		},
		"compact placement existing node pool": {
			customNodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-compact-low-pref").
					WithMachineType("e2-standard-4").
					WithCCCLabel("test-ccc").
					WithOptions(func(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
						np.PlacementPolicy = &gke_api_beta.PlacementPolicy{
							Type:       "COMPACT",
							PolicyName: "projects/test-project/regions/us-central1/resourcePolicies/compact-policy",
						}
						return np
					}).
					Build(),
				integration.EmptyNodePool("pool-compact-high-pref").
					WithMachineType("n2-standard-4").
					WithCCCLabel("test-ccc").
					WithOptions(func(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
						np.PlacementPolicy = &gke_api_beta.PlacementPolicy{
							Type:       "COMPACT",
							PolicyName: "projects/test-project/regions/us-central1/resourcePolicies/compact-policy",
						}
						return np
					}).
					Build(),
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			cccCrd := ccc.NewComputeClassBuilder("test-ccc").
				WithNodePoolAutoCreation(true).
				WithPriorities(
					v1.Priority{
						MachineType:        ptr.To("n2-standard-4"),
						PriorityScore:      ptr.To(100),
						AllocationStrategy: &fleetEfficiency,
					},
					v1.Priority{
						MachineType:        ptr.To("e2-standard-4"),
						PriorityScore:      ptr.To(100),
						AllocationStrategy: &fleetEfficiency,
					},
				).
				Build()

			testConfig := integration.NewTestConfig().
				WithNodePools(tc.customNodePools...).
				WithCccCrds(cccCrd).
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

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				// n2-standard-4 has very high availability across all zones (0.9), while e2-standard-4 has very low availability (0.1).
				// If fleet-efficiency scored these, n2-standard-4 would win easily.
				// Under fallback to lowest-cost, cheaper e2-standard-4 wins.
				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
					fake.NewGuidance("n2-standard-4").WithZone(ZoneA).WithScore(0.9),
					fake.NewGuidance("n2-standard-4").WithZone(ZoneB).WithScore(0.9),
					fake.NewGuidance("n2-standard-4").WithZone(ZoneC).WithScore(0.9),
					fake.NewGuidance("e2-standard-4").WithZone(ZoneA).WithScore(0.1),
					fake.NewGuidance("e2-standard-4").WithZone(ZoneB).WithScore(0.1),
					fake.NewGuidance("e2-standard-4").WithZone(ZoneC).WithScore(0.1),
				)

				if tc.setupExtraObjs != nil {
					tc.setupExtraObjs(ctx, infra)
				}

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "test-ccc")

				testPod := tu.BuildTestPod("fe-constrained-pod", 3000, 12000,
					pod.WithCCC("test-ccc"),
					tu.MarkUnschedulable(),
				)
				if tc.podCustomizer != nil {
					tc.podCustomizer(testPod)
				}
				infra.Fakes.K8s.AddPod(testPod)

				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
				infra.Fakes.RunScheduler(ctx, t)

				// Verify that fallback chose cheaper e2-standard-4 over high-availability n2-standard-4
				if len(tc.customNodePools) > 0 {
					updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "fe-constrained-pod", metav1.GetOptions{})
					assert.NoError(t, err)
					assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected pod to be scheduled")
					assert.Contains(t, updatedPod.Spec.NodeName, "-low-pref", "Expected pod scheduled on cheaper e2 node pool")
				} else {
					cluster, err := infra.Fakes.GkeService.GetCluster("projects/test-project/locations/us-central1/clusters/test-cluster")
					assert.NoError(t, err)
					assert.Equal(t, 1, len(cluster.NodePools), "Expected exactly 1 node pool to be created")
					assert.Contains(t, cluster.NodePools[0].Name, "e2-standard-4", "Expected lowest-cost fallback to choose cheaper e2-standard-4 over n2-standard-4, but got %s", cluster.NodePools[0].Name)
				}

				// Verify that unsupported metric reason was emitted
				count, err := metrics.GetNodesWithAllocationStrategyCountForTest("fleet-efficiency", "unsupported", "e2-standard-4")
				assert.NoError(t, err)
				assert.Equal(t, float64(1), count, "Expected unsupported fallback metric count to be 1")
			})
		})
	}
}

// TestFleetEfficiency_PartialMigBackoffDoesNotArtificiallyBeatHealthyMachineType tests that
// when pool-n2 has its ZoneA MIG in stockout backoff, pool-n2 is scored only across its scalable
// MIGs in ZoneB and ZoneC (availability score 0.20), rather than including the backed-off ZoneA (0.9).
// Competing pool-e2 is healthy across all zones with availability 0.35. Because backed-off zones
// do not inflate pool-n2's score, healthy pool-e2 wins.
func TestFleetEfficiency_PartialMigBackoffDoesNotArtificiallyBeatHealthyMachineType(t *testing.T) {
	fleetEfficiency := v1.AllocationStrategyFleetEfficiency

	existingPoolN2 := integration.EmptyNodePool("pool-n2").
		WithMachineType("n2-standard-4").
		WithLocations(ZoneA, ZoneB, ZoneC).
		WithCCCLabel("test-ccc").
		Build()

	existingPoolE2 := integration.EmptyNodePool("pool-e2").
		WithMachineType("e2-standard-4").
		WithLocations(ZoneA, ZoneB, ZoneC).
		WithCCCLabel("test-ccc").
		Build()

	cccCrd := ccc.NewComputeClassBuilder("test-ccc").
		WithNodePoolAutoCreation(true).
		WithPriorities(
			v1.Priority{
				Nodepools:          []string{"pool-n2"},
				PriorityScore:      ptr.To(100),
				AllocationStrategy: &fleetEfficiency,
			},
			v1.Priority{
				Nodepools:          []string{"pool-e2"},
				PriorityScore:      ptr.To(100),
				AllocationStrategy: &fleetEfficiency,
			},
		).
		Build()

	testConfig := integration.NewTestConfig().
		WithNodePools(existingPoolN2, existingPoolE2).
		WithCccCrds(cccCrd).
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

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance("n2-standard-4").WithZone(ZoneA).WithScore(0.9),
			fake.NewGuidance("n2-standard-4").WithZone(ZoneB).WithScore(0.2),
			fake.NewGuidance("n2-standard-4").WithZone(ZoneC).WithScore(0.2),
			fake.NewGuidance("e2-standard-4").WithZone(ZoneA).WithScore(0.35),
			fake.NewGuidance("e2-standard-4").WithZone(ZoneB).WithScore(0.35),
			fake.NewGuidance("e2-standard-4").WithZone(ZoneC).WithScore(0.35),
		)

		// Simulate stockout on pool-n2 in ZoneA
		infra.Fakes.GceService.SetCreateInstanceForMigError("pool-n2", stockOutError())

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "test-ccc")

		// Pass 1: Pod scales pool-n2 and hits stockout on pool-n2 in ZoneA.
		pod1 := tu.BuildTestPod("fe-pod-1", 3000, 12000, pod.WithCCC("test-ccc"), tu.MarkUnschedulable())
		infra.Fakes.K8s.AddPod(pod1)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Pass 2: With pool-n2 backed off in ZoneA, pool-n2 scalable score is 0.20 (ZoneB/C).
		// pool-e2 has healthy capacity 0.35 across all zones and should be chosen.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod1, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "fe-pod-1", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod1.Spec.NodeName, "Expected fe-pod-1 to be scheduled after pass 2")
		assert.Contains(t, updatedPod1.Spec.NodeName, "pool-e2", "Expected healthy pool-e2 (score 0.35) to beat backed-off pool-n2 (scalable score 0.20), but got %s", updatedPod1.Spec.NodeName)
	})
}

// TestFleetEfficiency_ZonalPod_FallbackReusesExistingNodePoolOverUncreatedNapCandidate tests that
// when an existing regional node pool (pool-existing, n2-standard-4) is present, and a pod arrives
// with a zonal constraint, fleet-efficiency falls back to lowest-cost (unsupported),
// which reuses the existing node pool rather than injecting a new NAP node pool of a competing machine type.
func TestFleetEfficiency_ZonalPod_FallbackReusesExistingNodePoolOverUncreatedNapCandidate(t *testing.T) {
	fleetEfficiency := v1.AllocationStrategyFleetEfficiency

	existingPool := integration.EmptyNodePool("pool-existing").
		WithMachineType("n2-standard-4").
		WithLocations(ZoneA, ZoneB, ZoneC).
		WithCCCLabel("test-ccc").
		Build()

	cccCrd := ccc.NewComputeClassBuilder("test-ccc").
		WithNodePoolAutoCreation(true).
		WithPriorities(
			v1.Priority{
				Nodepools:          []string{"pool-existing"},
				PriorityScore:      ptr.To(100),
				AllocationStrategy: &fleetEfficiency,
			},
			v1.Priority{
				MachineType:        ptr.To("e2-standard-8"),
				PriorityScore:      ptr.To(100),
				AllocationStrategy: &fleetEfficiency,
			},
		).
		Build()

	testConfig := integration.NewTestConfig().
		WithNodePools(existingPool).
		WithCccCrds(cccCrd).
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

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		// pool-existing (n2-standard-4): ZoneA=0.9, ZoneB=0.2, ZoneC=0.9 (regional avg = 0.667).
		// In ZoneB where the pod will run, n2-standard-4 has score 0.2.
		// e2-standard-4: ZoneA=0.1, ZoneB=0.8, ZoneC=0.1 (regional avg = 0.333).
		// In ZoneB where the pod will run, e2-standard-4 has score 0.8.
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance("n2-standard-4").WithZone(ZoneA).WithScore(0.9),
			fake.NewGuidance("n2-standard-4").WithZone(ZoneB).WithScore(0.2),
			fake.NewGuidance("n2-standard-4").WithZone(ZoneC).WithScore(0.9),
			fake.NewGuidance("e2-standard-8").WithZone(ZoneA).WithScore(0.1),
			fake.NewGuidance("e2-standard-8").WithZone(ZoneB).WithScore(0.8),
			fake.NewGuidance("e2-standard-8").WithZone(ZoneC).WithScore(0.1),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "test-ccc")

		// Pod is constrained to ZoneB via NotIn [ZoneA, ZoneC]
		zonalPod := tu.BuildTestPod(
			"fe-zonal-pod-vs-better", 3000, 12000,
			pod.WithCCC("test-ccc"),
			withNodeAffinityNotIn("topology.kubernetes.io/zone", ZoneA, ZoneC),
			tu.MarkUnschedulable(),
		)
		infra.Fakes.K8s.AddPod(zonalPod)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "fe-zonal-pod-vs-better", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected fe-zonal-pod-vs-better to be scheduled")
		assert.Contains(t, updatedPod.Spec.NodeName, "pool-existing", "Expected fallback to lowest-cost to reuse existing node pool 'pool-existing', but got %s", updatedPod.Spec.NodeName)
	})
}

// TestFleetEfficiency_AsyncUpcomingNodePoolReusedOnSubsequentCycle tests that
// in production when WithHighThroughputNAPEnabled is active and NAP creates node pools asynchronously:
// In Pass 1, a pod triggers async creation of a regional node pool.
// In Pass 2, while the pool is still in the upcoming state (Exist() == false, IsUpcoming() == true),
// a second pod arrives.
// Because flexadvisor.NewNodeGroupSet excludes upcoming node groups from uncreated candidate expansion, the upcoming
// pool is recognized as an existing regional pool and correctly reused, preventing NAP
// from creating redundant duplicate node pools.
func TestFleetEfficiency_AsyncUpcomingNodePoolReusedOnSubsequentCycle(t *testing.T) {
	fleetEfficiency := v1.AllocationStrategyFleetEfficiency

	cccCrd := ccc.NewComputeClassBuilder("test-ccc").
		WithNodePoolAutoCreation(true).
		WithPriorities(
			v1.Priority{
				MachineType:        ptr.To("n2-standard-4"),
				PriorityScore:      ptr.To(100),
				AllocationStrategy: &fleetEfficiency,
			},
		).
		Build()

	testConfig := integration.NewTestConfig().
		WithCccCrds(cccCrd).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithAutoprovisioningLocations(ZoneA, ZoneB, ZoneC),
		).
		WithOverrides(
			integration.WithMaxMemoryTotal(140*1024*1024*1024),
			integration.WithAutoProvisioningEnabled(),
			integration.WithHighThroughputNAPEnabled(10, 100),
			integration.WithFlexAdvisorEnabled(),
			integration.WithBalanceSimilarNodeGroups(),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance("n2-standard-4").WithZone(ZoneA).WithScore(0.9),
			fake.NewGuidance("n2-standard-4").WithZone(ZoneB).WithScore(0.3),
			fake.NewGuidance("n2-standard-4").WithZone(ZoneC).WithScore(0.3),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "test-ccc")

		// --- Pass 1: First pod arrives and triggers async NAP node pool creation ---
		pod1 := tu.BuildTestPod("fe-async-pod-1", 3000, 12000, pod.WithCCC("test-ccc"), tu.MarkUnschedulable())
		infra.Fakes.K8s.AddPod(pod1)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// --- Pass 2: Second pod arrives (non-zonal) while first pool is upcoming ---
		pod2 := tu.BuildTestPod("fe-async-pod-2", 3000, 12000,
			pod.WithCCC("test-ccc"),
			tu.MarkUnschedulable(),
		)
		infra.Fakes.K8s.AddPod(pod2)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Verify that exactly 1 node pool was created by NAP across both passes.
		cluster, err := infra.Fakes.GkeService.GetCluster("projects/test-project/locations/us-central1/clusters/test-cluster")
		assert.NoError(t, err)
		assert.Equal(t, 1, len(cluster.NodePools), "Expected exactly 1 node pool in cluster; NAP should reuse the upcoming node pool rather than creating a duplicate")
	})
}

// TestFleetEfficiency_ZonalByDesignPoolDoesNotClampNapCandidate tests that a healthy existing node pool
// of the same hardware which is zonal by design (pool-zonal, n2-standard-4 in ZoneB only) does not
// restrict the target zones of an uncreated NAP candidate of the same machine type.
// The NAP candidate is scored across all autoprovisioning locations (avg (0.9+0.3+0.3)/3 = 0.5) and
// beats pool-zonal (0.3), instead of being clamped to ZoneB and tying with pool-zonal.
func TestFleetEfficiency_ZonalByDesignPoolDoesNotClampNapCandidate(t *testing.T) {
	fleetEfficiency := v1.AllocationStrategyFleetEfficiency

	zonalPool := integration.EmptyNodePool("pool-zonal").
		WithMachineType("n2-standard-4").
		WithLocations(ZoneB).
		WithCCCLabel("test-ccc").
		Build()

	cccCrd := ccc.NewComputeClassBuilder("test-ccc").
		WithNodePoolAutoCreation(true).
		WithPriorities(
			v1.Priority{
				Nodepools:          []string{"pool-zonal"},
				PriorityScore:      ptr.To(100),
				AllocationStrategy: &fleetEfficiency,
			},
			v1.Priority{
				MachineType:        ptr.To("n2-standard-4"),
				PriorityScore:      ptr.To(100),
				AllocationStrategy: &fleetEfficiency,
			},
		).
		Build()

	testConfig := integration.NewTestConfig().
		WithNodePools(zonalPool).
		WithCccCrds(cccCrd).
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

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance("n2-standard-4").WithZone(ZoneA).WithScore(0.9),
			fake.NewGuidance("n2-standard-4").WithZone(ZoneB).WithScore(0.3),
			fake.NewGuidance("n2-standard-4").WithZone(ZoneC).WithScore(0.3),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "test-ccc")

		infra.Fakes.K8s.AddPod(tu.BuildTestPod("fe-pod", 3000, 12000, pod.WithCCC("test-ccc"), tu.MarkUnschedulable()))

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "fe-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected fe-pod to be scheduled")
		assert.NotContains(t, updatedPod.Spec.NodeName, "pool-zonal", "Expected the NAP candidate (avg 0.5 across all zones) to beat zonal pool-zonal (0.3), but got %s", updatedPod.Spec.NodeName)

		cluster, err := infra.Fakes.GkeService.GetCluster("projects/test-project/locations/us-central1/clusters/test-cluster")
		assert.NoError(t, err)
		assert.Equal(t, 2, len(cluster.NodePools), "Expected a new NAP node pool to be created next to pool-zonal")
	})
}

// TestFleetEfficiency_NapStockoutExcludesZoneFromLargerSameFamilyNapCandidate tests that zones in which NAP
// recorded an uncreated candidate as backed off are excluded from its fleet efficiency score.
//
// The n2-standard-4 rule is restricted to ZoneA, where every scale-up stocks out. After the autoprovisioned
// n2-standard-4 node pool stocks out, resource-based backoff also covers the larger n2-standard-8 in ZoneA.
// n2-standard-8 is different hardware than the failed node pool, so it can only learn about the backoff from
// NAP injection. It must be scored across [ZoneB, ZoneC] (0.3) rather than all zones (0.5), so e2-standard-8
// (0.4) is chosen.
func TestFleetEfficiency_NapStockoutExcludesZoneFromLargerSameFamilyNapCandidate(t *testing.T) {
	fleetEfficiency := v1.AllocationStrategyFleetEfficiency

	cccCrd := ccc.NewComputeClassBuilder("test-ccc").
		WithNodePoolAutoCreation(true).
		WithPriorities(
			v1.Priority{
				MachineType:        ptr.To("n2-standard-4"),
				PriorityScore:      ptr.To(100),
				AllocationStrategy: &fleetEfficiency,
				Location:           &v1.Location{Zones: []string{ZoneA}},
			},
			v1.Priority{
				MachineType:        ptr.To("n2-standard-8"),
				PriorityScore:      ptr.To(100),
				AllocationStrategy: &fleetEfficiency,
			},
			v1.Priority{
				MachineType:        ptr.To("e2-standard-8"),
				PriorityScore:      ptr.To(100),
				AllocationStrategy: &fleetEfficiency,
			},
		).
		Build()

	testConfig := integration.NewTestConfig().
		WithCccCrds(cccCrd).
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

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance("n2-standard-4").WithZone(ZoneA).WithScore(0.9),
			fake.NewGuidance("n2-standard-8").WithZone(ZoneA).WithScore(0.9),
			fake.NewGuidance("n2-standard-8").WithZone(ZoneB).WithScore(0.3),
			fake.NewGuidance("n2-standard-8").WithZone(ZoneC).WithScore(0.3),
			fake.NewGuidance("e2-standard-8").WithZone(ZoneA).WithScore(0.4),
			fake.NewGuidance("e2-standard-8").WithZone(ZoneB).WithScore(0.4),
			fake.NewGuidance("e2-standard-8").WithZone(ZoneC).WithScore(0.4),
		)

		infra.Fakes.GceService.SetCreateInstanceForZoneError(ZoneA, stockOutError())

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "test-ccc")

		infra.Fakes.K8s.AddPod(tu.BuildTestPod("fe-pod", 3000, 12000, pod.WithCCC("test-ccc"), tu.MarkUnschedulable()))

		// Pass 1: n2-standard-4 in ZoneA (0.9) beats n2-standard-8 (0.5) and e2-standard-8 (0.4). NAP creates
		// the n2-standard-4 node pool, whose scale-up in ZoneA stocks out.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		cluster, err := infra.Fakes.GkeService.GetCluster("projects/test-project/locations/us-central1/clusters/test-cluster")
		assert.NoError(t, err)
		if assert.Equal(t, 1, len(cluster.NodePools), "Expected a single NAP node pool to be created in pass 1") {
			assert.Equal(t, "n2-standard-4", cluster.NodePools[0].Config.MachineType)
		}

		// Pass 2: the stockout is detected and n2-standard-8 is backed off in ZoneA. NAP injects it in a
		// healthy zone and records ZoneA, so it's scored 0.3 and e2-standard-8 (0.4) wins.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		cluster, err = infra.Fakes.GkeService.GetCluster("projects/test-project/locations/us-central1/clusters/test-cluster")
		assert.NoError(t, err)
		var machineTypesOfNewPools []string
		for _, np := range cluster.NodePools {
			if np.Config.MachineType != "n2-standard-4" {
				machineTypesOfNewPools = append(machineTypesOfNewPools, np.Config.MachineType)
			}
		}
		assert.Equal(t, []string{"e2-standard-8"}, machineTypesOfNewPools, "Expected e2-standard-8 to beat n2-standard-8, which is backed off in ZoneA")
	})
}
