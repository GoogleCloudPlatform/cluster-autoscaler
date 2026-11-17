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
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	prv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/provreq"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// TestFlexAdvisor_BestEffortAtomicProvisioningRequestWithCCC verifies how a
// best-effort-atomic ProvisioningRequest targeting a Custom Compute Class runs
// under default shard-based scheduling:
//   - ShardProcessor and WrapperOrchestrator skip ProvisioningClassName != ""
//     shards (neither pre-registering nor calling Activate/DeactivateFastPolling).
//   - Inside bestEffortAtomicProvClass.Provision, EstimationLimiterWrapper
//     awaits the CCC scope's initial fetch via BinpackerExpanderAwaitBudget
//     and ScaleUpBalancer calls BumpScopeTtl without promoting the scope
//     to fast polling.
//   - Consequently, provisioning is steered away from stocked-out zones while
//     the scope remains in slow polling (20s) until the 10m active TTL expires.
func TestFlexAdvisor_BestEffortAtomicProvisioningRequestWithCCC(t *testing.T) {
	testCases := []struct {
		name               string
		flexAdvisorEnabled bool
		wantNodesCreated   int
		wantMigATargetSize int64
		wantMigBTargetSize int64
		wantSlowPolling    bool
	}{
		{
			name:               "FlexAdvisor enabled (default shard-based mode) - provisions all capacity in ZoneA and keeps CCC scope in 20s slow polling",
			flexAdvisorEnabled: true,
			wantNodesCreated:   2,
			wantMigATargetSize: 2,
			wantMigBTargetSize: 0,
			wantSlowPolling:    true,
		},
		{
			name:               "FlexAdvisor disabled - splits resize into stocked-out ZoneB so only 1 of 2 nodes starts",
			flexAdvisorEnabled: false,
			wantNodesCreated:   1,
			wantMigATargetSize: 1,
			wantMigBTargetSize: 1,
			wantSlowPolling:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				computeClass := ccc.NewComputeClassBuilder("ccc-pr").WithNodePoolsRules("pool-pr-balanced").Build()
				nodePool := integration.DefaultNodePool(
					integration.WithNodePoolName("pool-pr-balanced"),
					integration.WithNodePoolMachineType(AvailableMachineType),
					integration.WithNodePoolSize(0),
					integration.WithNodePoolLocations(ZoneA, ZoneB),
					integration.WithNodePoolLocationPolicy("BALANCED"),
					integration.WithNodePoolCCCLabel(computeClass.Name),
				)

				prPod := tu.BuildTestPod("pr-pod", 3000, 12000, pod.WithCCC(computeClass.Name))
				prWrapper := provreq.BuildTestProvisioningRequest(
					"default",
					"best-effort-pr",
					nil,
					prPod.Spec,
					provreq.WithPodCount(2),
					func(pr *prv1.ProvisioningRequest) {
						pr.CreationTimestamp = metav1.NewTime(time.Now().Add(-1 * time.Minute))
						pr.Spec.ProvisioningClassName = prv1.ProvisioningClassBestEffortAtomicScaleUp
					},
				)

				overrides := []integration.Option[*options.AutoscalingOptions]{
					integration.WithMaxMemoryTotal(140 * 1024 * 1024 * 1024),
					integration.WithBalanceSimilarNodeGroups(),
				}
				if tc.flexAdvisorEnabled {
					overrides = append(overrides, integration.WithFlexAdvisorEnabled())
				}

				testConfig := integration.NewTestConfig().
					WithNodePools(nodePool).
					WithCccCrds(computeClass).
					WithProvisioningRequests(prWrapper).
					WithOverrides(overrides...)

				infra.Fakes.GceService.SetCreateInstanceForZoneError(ZoneB, stockOutError())
				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
					fake.NewGuidance(AvailableMachineType).WithZone(ZoneA).WithCapacity(10).WithScore(0.8),
					fake.NewGuidance(AvailableMachineType).WithZone(ZoneB).WithCapacity(0).WithScore(0.8),
				)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				require.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

				updatedPR, err := infra.Fakes.PRClientset.AutoscalingV1().ProvisioningRequests("default").Get(ctx, "best-effort-pr", metav1.GetOptions{})
				require.NoError(t, err)
				provisionedCond := meta.FindStatusCondition(updatedPR.Status.Conditions, prv1.Provisioned)
				require.NotNil(t, provisionedCond)
				assert.Equal(t, metav1.ConditionTrue, provisionedCond.Status)
				assert.Equal(t, tc.wantNodesCreated, len(infra.Fakes.K8s.Nodes().Items))

				migA, err := infra.Fakes.GceService.FetchMig(ctx, gceRefForTest("pool-pr-balanced", ZoneA))
				require.NoError(t, err)
				assert.Equal(t, tc.wantMigATargetSize, migA.TargetSize)

				migB, err := infra.Fakes.GceService.FetchMig(ctx, gceRefForTest("pool-pr-balanced", ZoneB))
				require.NoError(t, err)
				assert.Equal(t, tc.wantMigBTargetSize, migB.TargetSize)

				if tc.wantSlowPolling {
					// Advance to t+6m and measure calls over a 60s window: because ScaleUpBalancer
					// uses BumpScopeTtl rather than ActivateFastPolling, the scope stays in 20s
					// slow polling (~3 calls/min, never 6 calls/min from 10s fast polling) until its
					// 10m active TTL expires.
					time.Sleep(6 * time.Minute)
					synctest.Wait()
					callsAt6m := infra.Fakes.FlexAdvisorClient.GetFetchCapacityCalls()

					time.Sleep(1 * time.Minute)
					synctest.Wait()
					callsIn60s := infra.Fakes.FlexAdvisorClient.GetFetchCapacityCalls() - callsAt6m
					assert.GreaterOrEqual(t, callsIn60s, 2, "Expected CCC scope to remain registered and polling before 10m expiry")
					assert.LessOrEqual(t, callsIn60s, 3, "Expected CCC scope to stay in 20s slow polling rather than 10s fast polling")

					// Advance past the 10m activeScopeTtl + periodic cleanup sweep so the scope expires.
					time.Sleep(5 * time.Minute)
					synctest.Wait()
					callsAfterExpiry := infra.Fakes.FlexAdvisorClient.GetFetchCapacityCalls()

					time.Sleep(1 * time.Minute)
					synctest.Wait()
					assert.Equal(t, callsAfterExpiry, infra.Fakes.FlexAdvisorClient.GetFetchCapacityCalls(), "Expected CCC scope background polling to stop after 10m active TTL expires")
				}
			})
		})
	}
}
