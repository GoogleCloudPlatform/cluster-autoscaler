/*
Copyright 2026 Google LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ccc_test

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	gke_labels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
)

// TestSliceAwareScaleDownKeepsSliceBoundCube exercises BlockingLabelsFilteringProcessor
// end-to-end: an atomic (multi-host TPU) cube whose nodes carry the
// cloud.google.com/gke-tpu-slice label (i.e. it is bound to a TPU dynamic-slicing
// Slice) must be kept out of scale-down, while an equivalent unbound idle cube is
// scaled down normally.
//
// Removing a single labeled node from the atomic candidate set is enough: the
// downstream AtomicResizeFilteringProcessor then keeps the rest of the cube
// together (draining a partial cube would orphan the Slice). See
// go/ca-dynamic-slicing-early-scoping.
func TestSliceAwareScaleDownKeepsSliceBoundCube(t *testing.T) {
	testCases := []struct {
		name              string
		sliceAwareEnabled bool
		nodePools         int
		nodesPerPool      int
		// wantNodeCountAfterCALoop maps a CA loop number to the total node count
		// expected once that many loops have run. Scale-down is deterministic under
		// synctest: loop 1 marks the idle nodes unneeded (they must stay unneeded for
		// ScaleDownUnneededTime=1s), then one whole atomic cube can be removed per
		// subsequent loop. Listing more loops than strictly needed also asserts that
		// the result is stable (i.e. we don't keep scaling down).
		wantNodeCountAfterCALoop map[int]int
	}{
		{
			name:              "slice-bound cube is preserved while the unbound cube scales down",
			sliceAwareEnabled: true,
			nodePools:         2,
			nodesPerPool:      16,
			// Start: 2 cubes x 16 = 32 nodes. Loop 2 removes the unbound cube (32->16).
			// The slice-bound cube is protected, so loop 3 leaves it untouched.
			wantNodeCountAfterCALoop: map[int]int{2: 16, 3: 16},
		},
		{
			name:              "without slice-aware scale-down both idle cubes are removed",
			sliceAwareEnabled: false,
			nodePools:         2,
			nodesPerPool:      16,
			// Start: 2 cubes x 16 = 32 nodes. Loop 2 removes the first cube (32->16),
			// loop 3 the second (16->0). Nothing protects the cubes.
			wantNodeCountAfterCALoop: map[int]int{2: 16, 3: 0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var pools []*gke_api_beta.NodePool
			for i := 0; i < tc.nodePools; i++ {
				pools = append(pools, tpuSlicePool(fmt.Sprintf("tpu-slice-%d", i+1), "tpu7x-standard-4t", "4x4x4", tc.nodesPerPool))
			}

			testConfig := integration.NewTestConfig().
				WithNodePools(pools...).
				WithOverrides(
					integration.WithScaleDownUnneededTime(time.Second),
				)

			if tc.sliceAwareEnabled {
				testConfig.WithOverrides(
					integration.WithScaleDownBlockingNodeLabels(gke_labels.TPUSliceLabel),
				)
			}

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				// Starting condition: nodePools*nodesPerPool nodes, with one cube bound.
				assert.Equal(t, tc.nodePools*tc.nodesPerPool, countNodes(ctx, t, infra), "unexpected initial node count")
				boundNodes := bindNodePoolToSlice(ctx, t, infra, "tpu-slice-1", "my-tpu-slice")

				maxLoop := 0
				for loop := range tc.wantNodeCountAfterCALoop {
					if loop > maxLoop {
						maxLoop = loop
					}
				}
				for loop := 1; loop <= maxLoop; loop++ {
					integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
					if want, ok := tc.wantNodeCountAfterCALoop[loop]; ok {
						assert.Equalf(t, want, countNodes(ctx, t, infra), "unexpected node count after %d CA loops", loop)
					}
				}

				if tc.sliceAwareEnabled {
					// Every surviving node must belong to the slice-bound cube; the
					// unbound idle cube must have been scaled down.
					remaining, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
					assert.NoError(t, err)
					for _, n := range remaining.Items {
						assert.Truef(t, boundNodes[n.Name], "expected surviving node %s to belong to the slice-bound cube", n.Name)
					}
				}
			})
		})
	}
}

// TestSliceAwareScaleDownEmitsBoundToSliceVisibilityReason verifies the
// observability half of the feature: when BlockingLabelsFilteringProcessor keeps a
// slice-bound node out of scale-down, the NoScaleDown visibility log carries the
// slice-specific reason (no.scale.down.node.bound.to.tpu.slice) parameterized with
// the slice name, rather than a generic "scale-down disabled" reason.
//
// This exercises the full pipeline: the processor reports the node as unremovable
// with ScaleDownDisabledAnnotation, and the GKE visibility mapper
// (pkg/visibility/noscaledown) maps it to the slice reason via the gke-tpu-slice label.
func TestSliceAwareScaleDownEmitsBoundToSliceVisibilityReason(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithNodePools(
			tpuSlicePool("tpu-slice-1", "tpu7x-standard-4t", "4x4x4", 16),
			tpuSlicePool("tpu-slice-2", "tpu7x-standard-4t", "4x4x4", 16),
		).
		WithOverrides(
			integration.WithScaleDownUnneededTime(time.Second),
			integration.WithScaleDownBlockingNodeLabels(gke_labels.TPUSliceLabel),
			// Wire up the CA visibility pipeline so NoScaleDown reasons are emitted
			// to the (fake) event logger asserted below.
			integration.WithAutoscalerVisibility(true),
			integration.WithEmitNoScaleDownCAVizEvents(true),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		boundNodes := bindNodePoolToSlice(ctx, t, infra, "tpu-slice-1", "my-tpu-slice")

		// First loop marks the idle nodes unneeded; the second attempts scale-down,
		// at which point the slice-bound cube is filtered out and its reason emitted.
		for i := 0; i < 2; i++ {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
		}

		expectReason(t, infra, vistypes.NoScaleDownNodeBoundToTPUSlice, "my-tpu-slice", boundNodes)
	})
}

func tpuSlicePool(name, machineType, tpuTopology string, nodeCount int) *gke_api_beta.NodePool {
	return &gke_api_beta.NodePool{
		Name: name,
		Config: &gke_api_beta.NodeConfig{
			MachineType: machineType,
			ImageType:   "cos_containerd",
		},
		InitialNodeCount: int64(nodeCount),
		Autoscaling: &gke_api_beta.NodePoolAutoscaling{
			Enabled:      true,
			MinNodeCount: 0,
			MaxNodeCount: int64(nodeCount),
		},
		Locations: []string{"us-central1-b"},
		PlacementPolicy: &gke_api_beta.PlacementPolicy{
			TpuTopology: tpuTopology,
		},
	}
}

// countNodes returns the number of nodes currently registered in the cluster.
func countNodes(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure) int {
	t.Helper()
	nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	assert.NoError(t, err)
	return len(nodes.Items)
}

// bindNodePoolToSlice binds a cube to a TPU Slice by adding the gke-tpu-slice label
// to every node in the named node pool, and returns the set of labeled node names.
func bindNodePoolToSlice(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, poolName, sliceName string) map[string]bool {
	t.Helper()
	nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	assert.NoError(t, err)

	boundNodes := map[string]bool{}
	for i := range nodes.Items {
		node := nodes.Items[i]
		if node.Labels[gke_labels.GkeNodePoolLabel] != poolName {
			continue
		}
		node.Labels[gke_labels.TPUSliceLabel] = sliceName
		infra.Fakes.K8s.UpdateNode(&node)
		boundNodes[node.Name] = true
	}
	return boundNodes
}

// expectReason asserts that a NoScaleDown visibility event was emitted with
// wantReason, that its parameters include wantParameter, and that it is only ever
// attributed to nodes in wantNodes.
func expectReason(t *testing.T, infra *integration.TestInfrastructure, wantReason vistypes.MessageId, wantParameter string, wantNodes map[string]bool) {
	t.Helper()
	wantMsgID := wantReason.String()

	var found bool
	for _, event := range infra.Fakes.EventLogger.NoScaleDownEvents() {
		for _, nodeExplanation := range event.GetNodes() {
			reason := nodeExplanation.GetReason()
			if reason.GetMessageId() != wantMsgID {
				continue
			}
			found = true
			nodeName := nodeExplanation.GetNode().GetName()
			assert.Containsf(t, reason.GetParameters(), wantParameter,
				"reason for node %q should be parameterized with %q", nodeName, wantParameter)
			assert.Truef(t, wantNodes[nodeName],
				"reason %q should only be attributed to expected nodes, got %q", wantMsgID, nodeName)
		}
	}
	assert.Truef(t, found, "expected a NoScaleDown visibility reason %q", wantMsgID)
}
