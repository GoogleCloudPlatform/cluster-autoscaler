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

package ccc_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// TestCCCScaleDownStatusHistory verifies that when nodes belonging to a Custom ComputeClass (CCC)
// are scaled down (due to consolidation/being unneeded), the CCC status is updated correctly
// with the scale-down status history (specifically, ConsolidatedNodesCount) when
// EnhancedCrdStatusReporting is enabled. It also verifies that the history is not updated
// when EnhancedCrdStatusReporting is disabled.
func TestCCCScaleDownStatusHistory(t *testing.T) {
	testCases := []struct {
		name              string
		enhancedReporting bool
	}{
		{
			name:              "Enhanced reporting enabled",
			enhancedReporting: true,
		},
		{
			name:              "Enhanced reporting disabled",
			enhancedReporting: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cccObj := ccc.NewComputeClassBuilder("my-ccc").
				WithPriorities(v1.Priority{
					Nodepools: []string{"default-pool"},
				}).
				Build()

			nodePool := integration.EmptyNodePool("default-pool").
				WithMachineType("n1-standard-1").
				WithSize(3).
				WithMin(1).
				WithLocations("us-central1-b").
				WithCCCLabel("my-ccc").
				Build()

			testConfig := integration.NewTestConfig().
				WithNodePools(nodePool).
				WithCccCrds(cccObj).
				WithOverrides(
					integration.WithScaleDownUnneededTime(time.Second),
					integration.WithEnhancedCrdStatusReporting(tc.enhancedReporting),
				)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				// Verify existing nodes are created.
				nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
				assert.NoError(t, err)
				assert.Equal(t, 3, len(nodes.Items))

				// Run CA once to identify nodes as unneeded.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 0)

				// Advance virtual time to make nodes eligible for scale-down.
				time.Sleep(5 * time.Second)

				// Run CA again to trigger scale-down.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 0)

				// Wait for scale-down to complete (nodes deleted from fake client).
				// We expect 2 nodes to be deleted (3 -> 1).
				var remainingNodes *apiv1.NodeList
				for i := 0; i < 10; i++ {
					time.Sleep(5 * time.Second)
					remainingNodes, err = infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
					assert.NoError(t, err)
					if len(remainingNodes.Items) == 1 {
						break
					}
				}
				assert.Equal(t, 1, len(remainingNodes.Items), "Expected node pool to scale down to min size 1")

				// Run CA one more time to process the deletion results.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 0)

				// Wait for the Aggregator to flush status updates.
				// BatchFlushInterval is 30 seconds.
				time.Sleep(35 * time.Second)

				// Verify ComputeClass status has been updated.
				updatedCCC, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, "my-ccc", metav1.GetOptions{})
				assert.NoError(t, err)

				if tc.enhancedReporting {
					// We expect 2 nodes to have been consolidated/scaled down.
					// Since we have only one priority rule, it should be index 0.
					assert.NotEmpty(t, updatedCCC.Status.PriorityStatuses, "PriorityStatuses should not be empty")
					if len(updatedCCC.Status.PriorityStatuses) > 0 {
						history := updatedCCC.Status.PriorityStatuses[0].ScalingEventsHistory
						assert.NotNil(t, history, "ScalingEventsHistory should not be nil")
						if history != nil {
							assert.NotNil(t, history.ConsolidatedNodesCount, "ConsolidatedNodesCount should not be nil")
							if history.ConsolidatedNodesCount != nil {
								assert.Equal(t, 2, *history.ConsolidatedNodesCount, "ConsolidatedNodesCount mismatch")
							}
						}
					}
				} else {
					// If enhanced status reporting is disabled, ScalingEventsHistory or ConsolidatedNodesCount should be nil.
					if len(updatedCCC.Status.PriorityStatuses) > 0 {
						history := updatedCCC.Status.PriorityStatuses[0].ScalingEventsHistory
						if history != nil {
							assert.Nil(t, history.ConsolidatedNodesCount, "ConsolidatedNodesCount should be nil when enhanced reporting is disabled")
						}
					}
				}
			})
		})
	}
}
