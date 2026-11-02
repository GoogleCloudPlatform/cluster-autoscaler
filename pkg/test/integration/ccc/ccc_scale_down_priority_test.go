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
	"maps"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/core"

	gke_labels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

const scaleDownPriorityTaintKey = "cloud.google.com/gke-scale-down-priority"

// TestScaleDownPriority verifies that, when a ComputeClass sits one node above
// its min capacity, scale-down releases the idle node with the highest
// scale-down priority and keeps the others for the min capacity.
func TestScaleDownPriority(t *testing.T) {
	const (
		cccName   = "my-ccc"
		nodeCount = 4
	)
	testCases := []struct {
		name string
		// priorityByNodeIndex maps indexes into the sorted node names to the
		// priority taint value set on that node.
		priorityByNodeIndex map[int]string
		wantRemoved         int
	}{
		{name: "the tainted node goes", priorityByNodeIndex: map[int]string{1: "1"}, wantRemoved: 1},
		{name: "the higher priority goes first", priorityByNodeIndex: map[int]string{0: "1", 2: "5"}, wantRemoved: 2},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cc := ccc.NewComputeClassBuilder(cccName).
				WithTargetNodeCount(ptr.To(nodeCount - 1)).
				AddPriority(v1.Priority{MachineType: ptr.To("n1-standard-2")}).
				Build()
			testConfig := integration.NewTestConfig().
				WithNodePools(integration.DefaultNodePool(
					integration.WithNodePoolName("pool"),
					integration.WithNodePoolMachineType("n1-standard-2"),
					integration.WithNodePoolSize(nodeCount),
					integration.WithNodePoolMin(0),
					integration.WithNodePoolMax(nodeCount),
					integration.WithNodePoolLocations("us-central1-b"),
					integration.WithNodePoolLabels(map[string]string{gke_labels.ComputeClassLabel: cccName}),
					integration.WithNodePoolAutoscalingEnabled(true),
				)).
				WithCccCrds(cc).
				WithOverrides(
					integration.WithScaleDownUnneededTime(time.Second),
					integration.WithComputeClassMinCapacityEnabled(),
					integration.WithScaleDownPriorityTaintKey(scaleDownPriorityTaintKey),
				)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				if err != nil {
					t.Fatalf("SetupAutoscaler() failed: %v", err)
				}
				defer integration_synctest.TearDown(cancel)

				names := sortedNodeNames(ctx, t, infra)
				if len(names) != nodeCount {
					t.Fatalf("got %d initial nodes, want %d", len(names), nodeCount)
				}
				priorityByNode := map[string]string{}
				for idx, value := range tc.priorityByNodeIndex {
					priorityByNode[names[idx]] = value
				}
				addPriorityTaints(ctx, t, infra, priorityByNode)

				// The first loop marks the idle nodes unneeded, the second
				// removes the surplus.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)

				remaining := sortedNodeNames(ctx, t, infra)
				assert.Len(t, remaining, nodeCount-1, "expected one node above the min capacity to be removed")
				assert.NotContains(t, remaining, names[tc.wantRemoved], "expected the node with the highest priority to be removed")

				// A further loop must not go below the min capacity, priority
				// or not.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
				assert.Equal(t, remaining, sortedNodeNames(ctx, t, infra), "the min capacity must keep the remaining nodes")
			})
		})
	}
}

// TestScaleDownPriorityAtomic verifies that scale-down releases the atomic
// node group with the highest scale-down priority first, even when only one of
// its nodes carries the priority taint.
func TestScaleDownPriorityAtomic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		autoscaler, infra, nodesByPool := setupThreeTPUSlices(ctx, t, true /* enableExperiment */)
		addPriorityTaints(ctx, t, infra, map[string]string{
			nodesByPool["tpu-slice-2"][0]: "5",
			nodesByPool["tpu-slice-3"][0]: "1",
		})

		// The first loop marks the idle nodes unneeded, the second removes
		// one whole slice.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)

		got := remainingNodesByPool(ctx, t, infra, nodesByPool)
		want := map[string]int{"tpu-slice-1": nodesPerCube, "tpu-slice-2": 0, "tpu-slice-3": nodesPerCube}
		assert.Equal(t, want, got, "remaining nodes per slice")
	})
}

// TestScaleDownPriorityAtomic_ExperimentDisabled verifies that with the
// ScaleDownPriority::Enabled kill switch off scale-down still removes exactly
// one whole slice. Which one is left to CA's own candidate order: the slices
// tie once the priority taint is ignored. That the taint is then ignored is
// covered by the unit tests of the taint parser.
func TestScaleDownPriorityAtomic_ExperimentDisabled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		autoscaler, infra, nodesByPool := setupThreeTPUSlices(ctx, t, false /* enableExperiment */)
		addPriorityTaints(ctx, t, infra, map[string]string{
			nodesByPool["tpu-slice-2"][0]: "5",
			nodesByPool["tpu-slice-3"][0]: "1",
		})

		// The first loop marks the idle nodes unneeded, the second removes
		// one whole slice.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)

		got := slices.Sorted(maps.Values(remainingNodesByPool(ctx, t, infra, nodesByPool)))
		assert.Equal(t, []int{0, nodesPerCube, nodesPerCube}, got, "remaining nodes per slice, sorted")
	})
}

// setupThreeTPUSlices starts Cluster Autoscaler on three idle 16-node atomic
// TPU slices (tpu-slice-1 to tpu-slice-3) of a ComputeClass with a min
// capacity of two slices, with the ScaleDownPriority::Enabled
// experiment set to enableExperiment. It returns the sorted node names of each
// slice. Call it inside a synctest bubble.
func setupThreeTPUSlices(ctx context.Context, t *testing.T, enableExperiment bool) (*core.StaticAutoscaler, *integration.TestInfrastructure, map[string][]string) {
	t.Helper()
	const cccName = "my-ccc"
	cc := ccc.NewComputeClassBuilder(cccName).WithTargetNodeCount(ptr.To(2 * nodesPerCube)).Build()
	testConfig := integration.NewTestConfig().
		WithNodePools(
			atomicTPUNodePool("tpu-slice-1", cccName),
			atomicTPUNodePool("tpu-slice-2", cccName),
			atomicTPUNodePool("tpu-slice-3", cccName),
		).
		WithCccCrds(cc).
		WithOverrides(
			integration.WithScaleDownUnneededTime(time.Second),
			integration.WithComputeClassMinCapacityEnabled(),
			integration.WithScaleDownPriorityTaintKey(scaleDownPriorityTaintKey),
		).
		WithExperimentOverrides(map[string]bool{experiments.ScaleDownPriorityEnabledFlag: enableExperiment}, map[string]string{})

	infra := integration.SetupInfrastructure(ctx, t)
	autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
	if err != nil {
		t.Fatalf("SetupAutoscaler() failed: %v", err)
	}
	if got := countNodes(ctx, t, infra); got != 3*nodesPerCube {
		t.Fatalf("got %d initial nodes, want %d", got, 3*nodesPerCube)
	}
	nodesByPool := map[string][]string{}
	for _, node := range listNodes(ctx, t, infra) {
		pool := node.Labels[gke_labels.GkeNodePoolLabel]
		nodesByPool[pool] = append(nodesByPool[pool], node.Name)
	}
	for pool, names := range nodesByPool {
		if len(names) != nodesPerCube {
			t.Fatalf("got %d %s nodes, want %d", len(names), pool, nodesPerCube)
		}
		slices.Sort(names)
	}
	return autoscaler, infra, nodesByPool
}

// remainingNodesByPool returns how many of the nodes in nodesByPool are still
// registered, per node pool.
func remainingNodesByPool(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, nodesByPool map[string][]string) map[string]int {
	t.Helper()
	remaining := sortedNodeNames(ctx, t, infra)
	remainingByPool := map[string]int{}
	for pool, names := range nodesByPool {
		remainingByPool[pool] = 0
		for _, name := range names {
			if slices.Contains(remaining, name) {
				remainingByPool[pool]++
			}
		}
	}
	return remainingByPool
}

// addPriorityTaints adds the scale-down priority taint, with the given value,
// to each node in priorityByNode.
func addPriorityTaints(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, priorityByNode map[string]string) {
	t.Helper()
	for _, node := range listNodes(ctx, t, infra) {
		value, ok := priorityByNode[node.Name]
		if !ok {
			continue
		}
		node.Spec.Taints = append(node.Spec.Taints, apiv1.Taint{
			Key:    scaleDownPriorityTaintKey,
			Value:  value,
			Effect: apiv1.TaintEffectNoSchedule,
		})
		infra.Fakes.K8s.UpdateNode(node)
	}
}

// sortedNodeNames returns the sorted names of all nodes registered in the
// cluster.
func sortedNodeNames(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure) []string {
	t.Helper()
	nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Nodes().List() failed: %v", err)
	}
	names := make([]string, 0, len(nodes.Items))
	for _, n := range nodes.Items {
		names = append(names, n.Name)
	}
	slices.Sort(names)
	return names
}
