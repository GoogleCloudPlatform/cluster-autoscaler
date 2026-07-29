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
	compute "google.golang.org/api/compute/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// TestFleetEfficiency_NodesWithAllocationStrategyMetric verifies that the nodes_with_allocation_strategy
// metric is correctly recorded with appropriate labels (requested strategy, fallback reason, machine type,
// and node count) across various allocation strategy and fallback scenarios in an end-to-end CA loop.
func TestFleetEfficiency_NodesWithAllocationStrategyMetric(t *testing.T) {

	testCases := map[string]struct {
		strategy                  *v1.AllocationStrategy
		isFlexStart               bool
		podCount                  int
		podCpuRequest             int64
		nodePools                 []*gke_api_beta.NodePool
		guidances                 []fake.CapacityGuidance
		reservations              []*compute.Reservation
		expectedRequestedStrategy string
		expectedFallbackReason    metrics.AllocationStrategyFallbackReason
		expectedMachineType       string
		expectedCount             float64
	}{
		"No fallback: Fleet Efficiency": {
			strategy: new(v1.AllocationStrategyFleetEfficiency),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-low-preference").WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference").WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(0.2),
				fake.NewGuidance("e2-standard-8").WithScore(0.9),
			},
			expectedRequestedStrategy: "fleet-efficiency",
			expectedFallbackReason:    "",
			expectedMachineType:       "e2-standard-8",
			expectedCount:             1,
		},
		"No fallback: Fleet Efficiency - Multi Node Scale Up": {
			strategy: new(v1.AllocationStrategyFleetEfficiency),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-low-preference").WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference").WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(0.9),
				fake.NewGuidance("e2-standard-8").WithScore(0.2),
			},
			podCount:                  2,
			podCpuRequest:             3000,
			expectedRequestedStrategy: "fleet-efficiency",
			expectedFallbackReason:    "",
			expectedMachineType:       "e2-standard-4",
			expectedCount:             2,
		},
		"No fallback: Lowest Cost strategy": {
			strategy: new(v1.AllocationStrategyLowestCost),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-low-preference").WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference").WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(0.2),
				fake.NewGuidance("e2-standard-8").WithScore(0.9),
			},
			expectedRequestedStrategy: "lowest-cost",
			expectedFallbackReason:    "",
			expectedMachineType:       "e2-standard-4",
			expectedCount:             1,
		},
		"Fallback: Missing Score": {
			strategy: new(v1.AllocationStrategyFleetEfficiency),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-low-preference").WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference").WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithOmit(true),
				fake.NewGuidance("e2-standard-8").WithOmit(true),
			},
			expectedRequestedStrategy: "fleet-efficiency",
			expectedFallbackReason:    "missing_score",
			expectedMachineType:       "e2-standard-4",
			expectedCount:             1,
		},
		"Fallback: Tie Break": {
			strategy: new(v1.AllocationStrategyFleetEfficiency),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-low-preference").WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference").WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(0.5),
				fake.NewGuidance("e2-standard-8").WithScore(0.5),
			},
			expectedRequestedStrategy: "fleet-efficiency",
			expectedFallbackReason:    "tie_break",
			expectedMachineType:       "e2-standard-4",
			expectedCount:             1,
		},
		"Fallback: Reservation Present": {
			strategy: new(v1.AllocationStrategyFleetEfficiency),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-low-preference").WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference").WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(0.2),
				fake.NewGuidance("e2-standard-8").WithScore(0.9),
			},
			reservations: []*compute.Reservation{
				reservations.BuildMultipleMachineReservation("e2-standard-4", ZoneB, 0, 1),
			},
			expectedRequestedStrategy: "fleet-efficiency",
			expectedFallbackReason:    "reservation_present",
			expectedMachineType:       "e2-standard-4",
			expectedCount:             1,
		},
		"Fallback: FA Error": {
			strategy: new(v1.AllocationStrategyFleetEfficiency),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-low-preference").WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference").WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(2.0),
				fake.NewGuidance("e2-standard-8").WithScore(2.0),
			},
			expectedRequestedStrategy: "fleet-efficiency",
			expectedFallbackReason:    "error",
			expectedMachineType:       "e2-standard-4",
			expectedCount:             1,
		},
		"Fallback: FA Not Supported": {
			strategy:    new(v1.AllocationStrategyFleetEfficiency),
			isFlexStart: true,
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-low-preference").WithOptions(integration.WithFlexStartNodePool).WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
				integration.EmptyNodePool("pool-high-preference").WithOptions(integration.WithFlexStartNodePool).WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
			},
			expectedRequestedStrategy: "fleet-efficiency",
			expectedFallbackReason:    "flex_advisor_not_supported",
			expectedMachineType:       "e2-standard-4",
			expectedCount:             1,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			metrics.ResetAllForTest()

			nodepoolNames := make([]string, 0, len(tc.nodePools))
			for _, np := range tc.nodePools {
				nodepoolNames = append(nodepoolNames, np.Name)
			}

			cccCrd := ccc.NewComputeClassBuilder("test-ccc").
				WithAllocationStrategyDefaults(&v1.AllocationStrategyDefaults{
					OnDemand:  tc.strategy,
					FlexStart: tc.strategy,
					Spot:      tc.strategy,
				}).
				WithPriorities(
					v1.Priority{
						Nodepools:          nodepoolNames,
						PriorityScore:      new(100),
						AllocationStrategy: tc.strategy,
					},
				).
				Build()

			overrides := []integration.Option[*internalopts.AutoscalingOptions]{
				integration.WithMaxMemoryTotal(140 * 1024 * 1024 * 1024),
				integration.WithFlexAdvisorEnabled(),
			}

			// Explicitly set experiment overrides: FlexStartNonQueued enabled, FlexAdvisorTPU disabled
			boolFlags := map[string]bool{
				experiments.FlexAdvisorTPUEnabledFlag: false,
			}
			stringFlags := map[string]string{
				experiments.FlexStartNonQueuedEnabledFlag: "0.0.0",
			}

			testConfig := integration.NewTestConfig().
				WithNodePools(tc.nodePools...).
				WithCccCrds(cccCrd).
				WithReservationsForDefaultProject(tc.reservations).
				WithOverrides(overrides...).
				WithExperimentOverrides(boolFlags, stringFlags)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				if len(tc.guidances) > 0 {
					infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(tc.guidances...)
				}

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				podCount := tc.podCount
				if podCount == 0 {
					podCount = 1
				}
				cpuReq := int64(3000)
				if tc.podCpuRequest > 0 {
					cpuReq = tc.podCpuRequest
				}

				for i := 0; i < podCount; i++ {
					podName := fmt.Sprintf("fe-metric-pod-%d", i)
					testPod := tu.BuildTestPod(podName, cpuReq, 12000, pod.WithCCC("test-ccc"), tu.MarkUnschedulable())
					if tc.isFlexStart {
						pod.WithFlexStart()(testPod)
					}
					infra.Fakes.K8s.AddPod(testPod)
				}

				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
				infra.Fakes.RunScheduler(ctx, t)

				for i := 0; i < podCount; i++ {
					podName := fmt.Sprintf("fe-metric-pod-%d", i)
					updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, podName, metav1.GetOptions{})
					assert.NoError(t, err)
					assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected pod %s to be scheduled on a node", podName)
				}

				count, err := metrics.GetNodesWithAllocationStrategyCountForTest(tc.expectedRequestedStrategy, tc.expectedFallbackReason, tc.expectedMachineType)
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedCount, count, "Expected nodes_with_allocation_strategy count to match for strategy=%s, reason=%s, machineType=%s", tc.expectedRequestedStrategy, tc.expectedFallbackReason, tc.expectedMachineType)
			})
		})
	}
}

// TestFleetEfficiency_NodesWithAllocationStrategyMetric_ExperimentDisabled verifies that no nodes_with_allocation_strategy
// metric is recorded when the fleet efficiency filter is disabled via experiment flag.
func TestFleetEfficiency_NodesWithAllocationStrategyMetric_ExperimentDisabled(t *testing.T) {
	metrics.ResetAllForTest()

	cccCrd := ccc.NewComputeClassBuilder("test-ccc").
		WithAllocationStrategyDefaults(&v1.AllocationStrategyDefaults{
			OnDemand:  new(v1.AllocationStrategyFleetEfficiency),
			FlexStart: new(v1.AllocationStrategyFleetEfficiency),
			Spot:      new(v1.AllocationStrategyFleetEfficiency),
		}).
		WithPriorities(
			v1.Priority{
				Nodepools:          []string{"pool-low-preference", "pool-high-preference"},
				PriorityScore:      new(100),
				AllocationStrategy: new(v1.AllocationStrategyFleetEfficiency),
			},
		).
		Build()

	nodePools := []*gke_api_beta.NodePool{
		integration.EmptyNodePool("pool-low-preference").WithMachineType("e2-standard-4").WithCCCLabel("test-ccc").Build(),
		integration.EmptyNodePool("pool-high-preference").WithMachineType("e2-standard-8").WithCCCLabel("test-ccc").Build(),
	}

	overrides := []integration.Option[*internalopts.AutoscalingOptions]{
		integration.WithMaxMemoryTotal(140 * 1024 * 1024 * 1024),
		integration.WithFlexAdvisorEnabled(),
	}

	// Explicitly disable FleetEfficiencyStrategy::Enabled flag
	boolFlags := map[string]bool{
		experiments.FleetEfficiencyStrategyEnabledFlag: false,
	}
	stringFlags := map[string]string{}

	testConfig := integration.NewTestConfig().
		WithNodePools(nodePools...).
		WithCccCrds(cccCrd).
		WithOverrides(overrides...).
		WithExperimentOverrides(boolFlags, stringFlags)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		guidances := []fake.CapacityGuidance{
			fake.NewGuidance("e2-standard-4").WithScore(0.2),
			fake.NewGuidance("e2-standard-8").WithScore(0.9),
		}
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(guidances...)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		testPod := tu.BuildTestPod("fe-metric-pod-disabled", 3000, 12000, pod.WithCCC("test-ccc"), tu.MarkUnschedulable())
		infra.Fakes.K8s.AddPod(testPod)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "fe-metric-pod-disabled", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected pod to be scheduled on a node")

		reasons := []metrics.AllocationStrategyFallbackReason{
			"",
			"missing_score",
			"tie_break",
			"reservation_present",
			"error",
			"flex_advisor_not_supported",
		}
		for _, reason := range reasons {
			count, err := metrics.GetNodesWithAllocationStrategyCountForTest("fleet-efficiency", reason, "e2-standard-4")
			assert.NoError(t, err)
			assert.Equal(t, float64(0), count, "Metric should not be recorded when FleetEfficiencyStrategy experiment is disabled (reason=%s)", reason)
			count8, err := metrics.GetNodesWithAllocationStrategyCountForTest("fleet-efficiency", reason, "e2-standard-8")
			assert.NoError(t, err)
			assert.Equal(t, float64(0), count8, "Metric should not be recorded when FleetEfficiencyStrategy experiment is disabled (reason=%s)", reason)
		}
	})
}
