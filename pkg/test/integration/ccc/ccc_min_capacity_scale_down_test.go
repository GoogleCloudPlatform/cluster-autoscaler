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
	"flag"
	"fmt"
	"sort"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	gke_labels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func init() {
	klog.InitFlags(nil)
	_ = flag.Set("v", "5")
}

func TestCCCScaleDown(t *testing.T) {
	testCases := []struct {
		name                       string
		experimentEnabled          bool
		nodesTaintedForDeletion    int
		nodesWithDeletionTimestamp int
		expectedNodeCount          int
		failureMessage             string
	}{
		{
			name:              "prevents scale down below targetNodeCount",
			experimentEnabled: true,
			expectedNodeCount: 5,
			failureMessage:    "Expected exactly 5 nodes to remain in the cluster due to TargetNodeCountQuota minimum limit",
		},
		{
			name:                       "scale down limits account for nodes in deletion",
			experimentEnabled:          true,
			nodesTaintedForDeletion:    2,
			nodesWithDeletionTimestamp: 1,
			expectedNodeCount:          8,
			failureMessage:             "Expected exactly 8 nodes to remain (3 in deletion + 5 alive) because capacity should subtract nodes in deletion",
		},
		{
			name:              "scale down not prevented when disabled by experiment",
			experimentEnabled: false,
			expectedNodeCount: 1,
			failureMessage:    "Expected node count to drop to 1 because the minimum capacity feature is disabled by the experiment flag",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cc := ccc.NewComputeClassBuilder("my-ccc").
				WithTargetNodeCount(ptr.To(5)).
				Build()

			nodePool := integration.DefaultNodePool(
				integration.WithNodePoolName("default-pool"),
				integration.WithNodePoolMachineType("n1-standard-1"),
				integration.WithNodePoolSize(10),
				integration.WithNodePoolLocations("us-central1-b"),
			)
			nodePool.Autoscaling.MinNodeCount = 1

			testConfig := integration.NewTestConfig().
				WithNodePools(nodePool).
				WithCccCrds(cc).
				WithOverrides(
					integration.WithScaleDownUnneededTime(time.Second),
				)

			if !tc.experimentEnabled {
				testConfig.WithExperimentOverrides(
					map[string]bool{"ComputeClassMinCapacity::Enabled": false},
					map[string]string{},
				)
			}

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				// Label all auto-generated nodes with our ComputeClass label so the TargetNodeCountQuota applies to them.
				nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
				assert.NoError(t, err)
				assert.Equal(t, 10, len(nodes.Items))

				for _, node := range nodes.Items {
					if node.Labels == nil {
						node.Labels = make(map[string]string)
					}
					node.Labels["cloud.google.com/compute-class"] = "my-ccc"
					infra.Fakes.K8s.UpdateNode(&node)
				}

				// Run the autoscaler loop once to identify unneeded nodes.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 0)

				// Sleep inside the synctest bubble to advance virtual clock past the ScaleDownUnneededTime.
				time.Sleep(5 * time.Second)

				if tc.nodesTaintedForDeletion > 0 || tc.nodesWithDeletionTimestamp > 0 {
					freshNodes, listErr := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
					assert.NoError(t, listErr)

					for i := 0; i < tc.nodesTaintedForDeletion; i++ {
						infra.Fakes.K8s.UpdateNode(withDeletionTaint(freshNodes.Items[i]))
					}
					for i := 0; i < tc.nodesWithDeletionTimestamp; i++ {
						infra.Fakes.K8s.UpdateNode(withDeletionTimestamp(freshNodes.Items[tc.nodesTaintedForDeletion+i]))
					}
				}

				// Run the autoscaler loop again to trigger scale down.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 0)

				// Wait for the node count to drop to the expected level.
				var remainingNodes *apiv1.NodeList
				for i := 0; i < 20; i++ {
					time.Sleep(5 * time.Second)
					remainingNodes, err = infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
					assert.NoError(t, err)
					if len(remainingNodes.Items) == tc.expectedNodeCount {
						break
					}
				}

				assert.Equal(t, tc.expectedNodeCount, len(remainingNodes.Items), tc.failureMessage)
			})
		})
	}
}

// withDeletionTaint returns a copy of the node as a pointer, with ToBeDeletedTaint added.
func withDeletionTaint(node apiv1.Node) *apiv1.Node {
	node.Spec.Taints = append(node.Spec.Taints, apiv1.Taint{
		Key:    taints.ToBeDeletedTaint,
		Value:  "true",
		Effect: apiv1.TaintEffectNoSchedule,
	})
	return &node
}

// withDeletionTimestamp returns a copy of the node as a pointer, with DeletionTimestamp set.
func withDeletionTimestamp(node apiv1.Node) *apiv1.Node {
	now := metav1.Now()
	node.DeletionTimestamp = &now
	return &node
}

// TestCCCScaleDownAtomicGroups reproduces b/551773344: reducing a ComputeClass
// minimumCapacity.targetNodeCount for atomic (multi-host TPU) node pools must
// scale the CC down to the target by removing whole slices rather than getting
// stuck. The bug was that the per-node TargetNodeCountQuota split the allowance
// non-deterministically across the slices, so AtomicResizeFilteringProcessor
// rejected every slice and nothing scaled down.
//
// Setup: two 16-node multi-host TPU slices (32 nodes) under one CC with
// targetNodeCount=16. All nodes are idle. With the fix, exactly one whole slice
// is removed (16 nodes remain). With the experiment disabled there is no
// minimum-capacity floor, so both idle slices are removed.
func TestCCCScaleDownAtomicGroups(t *testing.T) {
	testCases := []struct {
		name              string
		experimentEnabled bool
		expectedNodeCount int
		// convergenceCycles is the exact number of autoscaler cycles needed to
		// reach expectedNodeCount. Scale-down is deterministic under synctest:
		// the first cycle marks the idle nodes unneeded (they must stay unneeded
		// for ScaleDownUnneededTime=1s), then one whole atomic slice is removed
		// per subsequent cycle.
		convergenceCycles int
		failureMessage    string
	}{
		{
			name:              "removes one whole slice down to targetNodeCount",
			experimentEnabled: true,
			expectedNodeCount: 16,
			// cycle 1: mark unneeded; cycle 2: remove one slice (32->16). The
			// min-capacity floor then blocks removing the last slice.
			convergenceCycles: 2,
			failureMessage:    "Expected exactly 16 nodes (one 16-node slice) to remain due to the atomic minimum-capacity floor",
		},
		{
			name:              "scale down not prevented when disabled by experiment",
			experimentEnabled: false,
			expectedNodeCount: 0,
			// cycle 1: mark unneeded; cycle 2: remove first slice (32->16);
			// cycle 3: remove second slice (16->0). No floor when disabled.
			convergenceCycles: 3,
			failureMessage:    "Expected all idle atomic nodes to be removed when the minimum-capacity feature is disabled",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cc := ccc.NewComputeClassBuilder("my-ccc").
				WithTargetNodeCount(ptr.To(16)).
				Build()

			testConfig := integration.NewTestConfig().
				WithNodePools(
					atomicTPUNodePool("tpu-slice-1", "my-ccc"),
					atomicTPUNodePool("tpu-slice-2", "my-ccc"),
				).
				WithCccCrds(cc).
				WithOverrides(
					integration.WithScaleDownUnneededTime(time.Second),
					integration.WithComputeClassMinCapacityEnabled(),
				)

			if !tc.experimentEnabled {
				testConfig.WithExperimentOverrides(
					map[string]bool{"ComputeClassMinCapacity::Enabled": false},
					map[string]string{},
				)
			}

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				// Two 16-node slices should be present up-front.
				nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
				assert.NoError(t, err)
				assert.Equal(t, 32, len(nodes.Items), "expected 32 pre-existing atomic TPU nodes")

				// Ensure every node carries the ComputeClass label so the
				// TargetNodeCountQuota / min-capacity processor apply to them.
				for _, node := range nodes.Items {
					if node.Labels == nil {
						node.Labels = make(map[string]string)
					}
					node.Labels["cloud.google.com/compute-class"] = "my-ccc"
					infra.Fakes.K8s.UpdateNode(&node)
				}

				for i := 0; i < tc.convergenceCycles; i++ {
					integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
				}

				remaining, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedNodeCount, len(remaining.Items), tc.failureMessage)

				// One further cycle must not change the result.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
				remaining, err = infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedNodeCount, len(remaining.Items), tc.failureMessage)
			})
		})
	}
}

// TestCCCScaleDownAtomicGroupsKeepsSliceWithRunningPods verifies that when a
// ComputeClass spec-level targetNodeCount is enforced across two atomic slices
// and one slice already runs real pods, scale-down removes the *idle* slice and
// keeps the slice hosting real workload.
func TestCCCScaleDownAtomicGroupsKeepsSliceWithRunningPods(t *testing.T) {
	const cccName = "my-ccc"

	cc := ccc.NewComputeClassBuilder(cccName).
		WithTargetNodeCount(ptr.To(16)).
		Build()

	testConfig := integration.NewTestConfig().
		WithNodePools(
			atomicTPUNodePool("tpu-slice-1", cccName),
			atomicTPUNodePool("tpu-slice-2", cccName),
		).
		WithCccCrds(cc).
		WithOverrides(
			integration.WithScaleDownUnneededTime(time.Second),
			integration.WithComputeClassMinCapacityEnabled(),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Equal(t, 32, len(nodes.Items), "expected 32 pre-existing atomic TPU nodes")

		// Group nodes by their node pool (each atomic slice is a distinct pool),
		// and label every node with the ComputeClass so the min-capacity
		// processor / quota apply to them.
		nodesByPool := map[string][]string{}
		for _, node := range nodes.Items {
			if node.Labels == nil {
				node.Labels = make(map[string]string)
			}
			node.Labels[gke_labels.ComputeClassLabel] = cccName
			infra.Fakes.K8s.UpdateNode(&node)
			pool := node.Labels[gke_labels.GkeNodePoolLabel]
			nodesByPool[pool] = append(nodesByPool[pool], node.Name)
		}
		assert.Equal(t, 2, len(nodesByPool), "expected two atomic slices (node pools)")

		// Deterministically pick one slice to host real pods.
		pools := make([]string, 0, len(nodesByPool))
		for pool := range nodesByPool {
			pools = append(pools, pool)
		}
		sort.Strings(pools)
		occupiedPool := pools[0]
		occupiedNodes := nodesByPool[occupiedPool]
		assert.Equal(t, 16, len(occupiedNodes), "expected 16 nodes in the occupied slice")

		occupiedSet := map[string]bool{}
		// Pin a real running pod onto every node of the occupied slice. The pods
		// carry a node-pool selector so they cannot relocate to the idle slice,
		// which keeps the occupied slice needed (and thus non-removable).
		for i, nodeName := range occupiedNodes {
			occupiedSet[nodeName] = true
			p := tu.BuildTestPod(fmt.Sprintf("real-pod-%d", i), 100, 100*1024*1024,
				tu.WithNodeName(nodeName),
				pod.WithCCC(cccName),
				pod.WithNodeSelectorEntry(gke_labels.GkeNodePoolLabel, occupiedPool),
			)
			infra.Fakes.K8s.AddPod(p)
		}

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second) // mark idle slice unneeded
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second) // remove idle slice

		remaining, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Equal(t, 16, len(remaining.Items), "expected exactly the 16-node occupied slice to remain")

		// A further cycle must not scale down below the minimum: the surviving
		// slice is protected both by its running pods and the min-capacity floor.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
		remaining, err = infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Equal(t, 16, len(remaining.Items), "min-capacity floor must keep the last slice")
		// Every surviving node must belong to the occupied slice; the idle slice
		// (with no real pods) is the one that must be scaled down.
		for _, n := range remaining.Items {
			assert.True(t, occupiedSet[n.Name],
				"expected node %s from the occupied slice %q to be kept; the idle slice should have been removed", n.Name, occupiedPool)
		}
	})
}

// atomicTPUNodePool builds a pre-existing multi-host TPU node pool (one 16-node
// 4x4x4 tpu7x slice) that the GKE client converts into an atomic
// (ZeroOrMaxNodeScaling) MIG. The 4x4x4 topology with 4 chips per node yields
// 64/4 = 16 nodes per slice.
func atomicTPUNodePool(name, cccName string) *gke_api_beta.NodePool {
	return &gke_api_beta.NodePool{
		Name: name,
		Config: &gke_api_beta.NodeConfig{
			MachineType: "tpu7x-standard-4t",
			ImageType:   "cos_containerd",
			Labels: map[string]string{
				"cloud.google.com/compute-class": cccName,
			},
		},
		InitialNodeCount: 16,
		Autoscaling: &gke_api_beta.NodePoolAutoscaling{
			Enabled:      true,
			MinNodeCount: 0,
			MaxNodeCount: 16,
		},
		Locations: []string{"us-central1-b"},
		PlacementPolicy: &gke_api_beta.PlacementPolicy{
			TpuTopology: "4x4x4",
		},
	}
}
