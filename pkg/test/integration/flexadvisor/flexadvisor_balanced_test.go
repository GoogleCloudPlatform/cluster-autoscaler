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
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// TestBalancedLocationPolicy verifies the behavior of the BALANCED location policy
// with and without Flex Advisor.
func TestBalancedLocationPolicyZoneBUnavailable(t *testing.T) {
	for name, tc := range map[string]struct {
		flexAdvisorEnabled    bool
		expectedPodsScheduled int
		expectedNodes         int
		wantMigATargetSize    int64
		wantMigBTargetSize    int64
	}{
		"With Flex Advisor Everything Scheduled on Zone A": {
			flexAdvisorEnabled:    true,
			expectedPodsScheduled: 2,
			expectedNodes:         2,
			wantMigATargetSize:    2,
			wantMigBTargetSize:    0,
		},
		"Without Flex Advisor Only One Pod Scheduled": {
			flexAdvisorEnabled:    false,
			expectedPodsScheduled: 1,
			expectedNodes:         1,
			wantMigATargetSize:    1,
			wantMigBTargetSize:    1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			nodePool := integration.DefaultNodePool(
				integration.WithNodePoolName("pool-balanced"),
				integration.WithNodePoolMachineType(AvailableMachineType),
				integration.WithNodePoolSize(0),
				integration.WithNodePoolLocations(ZoneA, ZoneB),
				integration.WithNodePoolLocationPolicy("BALANCED"),
			)

			ccc := createCCCWithNodePoolsRules([]string{"pool-balanced"})
			nodePools := annotateNodePoolWithCCCLabel(ccc.Name, []*gke_api_beta.NodePool{nodePool})

			overrides := []integration.Option[*options.AutoscalingOptions]{
				integration.WithMaxMemoryTotal(140 * 1024 * 1024 * 1024),
				integration.WithBalanceSimilarNodeGroups(),
			}
			if tc.flexAdvisorEnabled {
				overrides = append(overrides, integration.WithFlexAdvisorEnabled())
			}

			testConfig := integration.NewTestConfig().
				WithNodePools(nodePools...).
				WithCccCrds(ccc).
				WithOverrides(overrides...)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				// Zone B is unavailable
				infra.Fakes.GceService.SetCreateInstanceForZoneError(ZoneB, stockOutError())
				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
					fake.NewFakeCapacityGuidanceForMachineTypeAndZone(AvailableMachineType, ZoneA, 10, 0.8),
					fake.NewFakeCapacityGuidanceForMachineTypeAndZone(AvailableMachineType, ZoneB, 0, 0.8),
				)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				// Add 2 pods demanding 2 nodes in total
				pod1 := tu.BuildTestPod("pod-1", 3000, 12000, tu.MarkUnschedulable(), withTestCCC)
				pod2 := tu.BuildTestPod("pod-2", 3000, 12000, tu.MarkUnschedulable(), withTestCCC)
				infra.Fakes.K8s.AddPod(pod1)
				infra.Fakes.K8s.AddPod(pod2)

				// Run autoscaler loop
				integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)
				infra.Fakes.RunScheduler(ctx, t)

				// Assertions: Pods scheduling
				updatedPod1, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-1", metav1.GetOptions{})
				assert.NoError(t, err)
				updatedPod2, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-2", metav1.GetOptions{})
				assert.NoError(t, err)

				scheduledPods := 0
				if updatedPod1.Spec.NodeName != "" {
					scheduledPods++
				}
				if updatedPod2.Spec.NodeName != "" {
					scheduledPods++
				}
				assert.Equal(t, tc.expectedPodsScheduled, scheduledPods, "Expected %d pods to be scheduled", tc.expectedPodsScheduled)
				assert.Equal(t, tc.expectedNodes, len(infra.Fakes.K8s.Nodes().Items), "Expected %d nodes in the cluster", tc.expectedNodes)

				// Assertions: MIGs target sizes
				migA, err := infra.Fakes.GceService.FetchMig(gceRefForTest("pool-balanced", ZoneA))
				assert.NoError(t, err)
				assert.Equal(t, tc.wantMigATargetSize, migA.TargetSize, "Expected ZoneA MIG to scale up to %d", tc.wantMigATargetSize)

				migB, err := infra.Fakes.GceService.FetchMig(gceRefForTest("pool-balanced", ZoneB))
				assert.NoError(t, err)
				assert.Equal(t, tc.wantMigBTargetSize, migB.TargetSize, "Expected ZoneB MIG to scale up to %d", tc.wantMigBTargetSize)
			})
		})
	}
}

func gceRefForTest(name, zone string) gce.GceRef {
	return gce.GceRef{
		Project: integration.DefaultProject(),
		Zone:    zone,
		Name:    name,
	}
}
