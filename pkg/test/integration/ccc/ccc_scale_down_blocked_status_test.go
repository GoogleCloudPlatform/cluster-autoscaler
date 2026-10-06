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

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	gke_labels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const (
	// consolidationFlushInterval is how long to wait for the status Aggregator to push the
	// consolidation updates to the API server. Slightly higher than its flush interval so the
	// test doesn't race with the ticker.
	consolidationFlushInterval = status.BatchFlushInterval + 10*time.Second

	// The reason strings are spelled out rather than imported: they are part of the CCC API
	// contract (a closed enum in the CRD), so a rename upstream must break these tests.
	reasonUsedByFormedSlice  = "UsedByFormedSlice"
	reasonBlockingPods       = "BlockingPods"
	reasonMinCapacityReached = "MinCapacityReached"

	// firstPriority is the identifier the CCC status uses for the first priority of a CCC.
	firstPriority = "0"

	// nodesPerCube is the size of one atomic TPU cube, and also the node pool size both
	// scenarios below start from.
	nodesPerCube = 16

	// caLoops is how many autoscaler loops each scenario runs. Loop 1 marks the idle nodes
	// unneeded (ScaleDownUnneededTime is 1s), loop 2 acts on them, and loop 3 re-observes the
	// outcome, which asserts the reported count is stable rather than a one-off.
	caLoops = 3
)

// TestCCCScaleDownBlockedStatusReportsSliceBoundCube asserts that a slice-bound
// atomic cube kept out of scale-down is reported under usedByFormedSlice, while the
// unbound cube that was actually removed is not reported as blocked. Both pools
// belong to a single CCC priority, so the counts aggregate under that priority's
// Consolidation block.
func TestCCCScaleDownBlockedStatusReportsSliceBoundCube(t *testing.T) {
	const (
		cccName     = "tpu-slice-ccc"
		sliceName   = "my-tpu-slice"
		boundPool   = "tpu-slice-1"
		unboundPool = "tpu-slice-2"
	)

	cccObj := ccc.NewComputeClassBuilder(cccName).
		WithPriorities(v1.Priority{
			Nodepools: []string{boundPool, unboundPool},
		}).
		Build()

	testConfig := integration.NewTestConfig().
		WithNodePools(
			atomicTPUNodePool(boundPool, cccName),
			atomicTPUNodePool(unboundPool, cccName),
		).
		WithCccCrds(cccObj).
		WithOverrides(
			integration.WithScaleDownUnneededTime(time.Second),
			integration.WithEnhancedCrdStatusReporting(true),
			integration.WithComputeClassScaleDownStatusEnabled(),
			integration.WithScaleDownBlockingNodeLabels(gke_labels.TPUSliceLabel),
		)

	synctest.Test(t, func(t *testing.T) {
		// Given: two idle atomic TPU cubes under one CCC priority, one of them bound to a Slice.
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		assert.Equal(t, 2*nodesPerCube, countNodes(ctx, t, infra), "unexpected initial node count")
		bindNodePoolToSlice(ctx, t, infra, boundPool, sliceName)

		// When: the autoscaler consolidates the idle capacity and the status is flushed.
		for range caLoops {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
		}
		time.Sleep(consolidationFlushInterval)

		// Then: the unbound cube is gone and only the slice-bound cube remains.
		assert.Equal(t, nodesPerCube, countNodes(ctx, t, infra), "expected only the slice-bound cube to remain")

		// Then: the whole slice-bound cube is reported as blocked on the CCC status, and no
		// node is left unaccounted for.
		updatedCCC, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, cccName, metav1.GetOptions{})
		assert.NoError(t, err)

		consolidationStatus := consolidationForPriority(t, updatedCCC, firstPriority)
		if !assert.NotNil(t, consolidationStatus, "expected a Consolidation status block on priority %s", firstPriority) {
			return
		}
		assert.Equal(t, []v1.ConsolidationBlockedNodesInfo{
			{Reason: reasonUsedByFormedSlice, NodeCount: nodesPerCube},
		}, consolidationStatus.BlockedNodes, "expected the whole slice-bound cube to be reported blocked")
		assert.Equal(t, 0, ptr.Deref(consolidationStatus.NotProcessed, 0), "expected no unprocessed nodes")
		assert.Equal(t, 0, ptr.Deref(consolidationStatus.ActuationInProgress, 0), "expected no deletion in flight")
	})
}

// TestCCCScaleDownBlockedStatusReportsStandardNodePool asserts that the consolidation status
// is reported for ordinary node pools too, not only for atomic TPU ones.
func TestCCCScaleDownBlockedStatusReportsStandardNodePool(t *testing.T) {
	const (
		standardCCC  = "standard-ccc"
		standardPool = "standard-pool"
	)

	testCases := []struct {
		name          string
		poolSize      int
		poolMin       int
		pinnedNodes   int
		wantRemaining int
		wantBlocked   []v1.ConsolidationBlockedNodesInfo
	}{
		{
			// The pool stays above its minimum, so CA keeps considering the pinned nodes. With
			// the recheck timeout set, loops 2 and 3 only report them as RecentlyUnremovable
			// without re-running the drain simulation; the status must keep attributing them
			// to their pods rather than to a phantom failed removal.
			name:          "nodes pinned by pods that cannot be evicted",
			poolSize:      3,
			poolMin:       1,
			pinnedNodes:   2,
			wantRemaining: 2,
			wantBlocked: []v1.ConsolidationBlockedNodesInfo{
				{Reason: reasonBlockingPods, NodeCount: 2},
			},
		},
		{
			// Once the pool sits at its minimum, CA drops its nodes before evaluating them at
			// all, pinned or not, so the minimum is what keeps every one of them.
			name:          "node pool at its minimum size",
			poolSize:      3,
			poolMin:       2,
			pinnedNodes:   1,
			wantRemaining: 2,
			wantBlocked: []v1.ConsolidationBlockedNodesInfo{
				{Reason: reasonMinCapacityReached, NodeCount: 2},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cccObj := ccc.NewComputeClassBuilder(standardCCC).
				WithPriorities(v1.Priority{
					Nodepools: []string{standardPool},
				}).
				Build()

			testConfig := integration.NewTestConfig().
				WithNodePools(
					integration.EmptyNodePool(standardPool).
						WithSize(int64(tc.poolSize)).
						WithMin(int64(tc.poolMin)).
						WithLocations("us-central1-b").
						WithCCCLabel(standardCCC).
						Build(),
				).
				WithCccCrds(cccObj).
				WithOverrides(
					integration.WithScaleDownUnneededTime(time.Second),
					integration.WithUnremovableNodeRecheckTimeout(5*time.Minute),
					integration.WithEnhancedCrdStatusReporting(true),
					integration.WithComputeClassScaleDownStatusEnabled(),
				)

			synctest.Test(t, func(t *testing.T) {
				// Given: an idle standard node pool with pods that cannot be evicted pinning
				// some of its nodes.
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
				assert.NoError(t, err)
				assert.Len(t, nodes.Items, tc.poolSize, "unexpected initial node count")

				for i := range tc.pinnedNodes {
					// Small enough to leave the node underutilized, so that the drain
					// simulation runs and the pod, not the utilization, is what keeps the node.
					pinningPod := tu.BuildTestPod(fmt.Sprintf("pinning-pod-%d", i), 100, 100,
						pod.WithAnnotation("cluster-autoscaler.kubernetes.io/safe-to-evict", "false"))
					pinningPod.Spec.NodeName = nodes.Items[i].Name
					_, err = infra.Fakes.KubeClient.CoreV1().Pods(pinningPod.Namespace).Create(ctx, pinningPod, metav1.CreateOptions{})
					assert.NoError(t, err)
				}

				// When: the autoscaler consolidates the idle capacity and the status is flushed.
				for range caLoops {
					integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
				}
				time.Sleep(consolidationFlushInterval)

				// Then: the free nodes are gone, down to whatever the pins and the minimum keep.
				assert.Equal(t, tc.wantRemaining, countNodes(ctx, t, infra), "unexpected node count after consolidation")

				// Then: every remaining node is attributed to what keeps it.
				updatedCCC, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, standardCCC, metav1.GetOptions{})
				assert.NoError(t, err)

				consolidationStatus := consolidationForPriority(t, updatedCCC, firstPriority)
				if !assert.NotNil(t, consolidationStatus, "expected a Consolidation status block on priority %s", firstPriority) {
					return
				}
				assert.Equal(t, tc.wantBlocked, consolidationStatus.BlockedNodes)
				assert.Equal(t, 0, ptr.Deref(consolidationStatus.NotProcessed, 0), "expected no unprocessed nodes")
				assert.Equal(t, 0, ptr.Deref(consolidationStatus.ActuationInProgress, 0), "expected no deletion in flight")
			})
		})
	}
}

// TestCCCScaleDownBlockedStatusReportsMinCapacityFloor asserts that nodes an
// otherwise idle ComputeClass keeps alive purely to satisfy its own minimumCapacity
// are reported under minCapacityReached, rather than under noPlaceToMovePods.
//
// A unit test cannot prove this: the attribution depends on the fake pods holding
// the floor really reaching the shared cluster snapshot, where the drain simulation
// sees them and only their annotation tells them apart from a scheduling dead end.
func TestCCCScaleDownBlockedStatusReportsMinCapacityFloor(t *testing.T) {
	const (
		cccName  = "min-capacity-ccc"
		nodePool = "tpu-slice-1"
	)

	cccObj := ccc.NewComputeClassBuilder(cccName).
		WithTargetNodeCount(ptr.To(nodesPerCube)).
		Build()

	testConfig := integration.NewTestConfig().
		WithNodePools(atomicTPUNodePool(nodePool, cccName)).
		WithCccCrds(cccObj).
		WithOverrides(
			integration.WithScaleDownUnneededTime(time.Second),
			integration.WithComputeClassMinCapacityEnabled(),
			integration.WithEnhancedCrdStatusReporting(true),
			integration.WithComputeClassScaleDownStatusEnabled(),
		)

	synctest.Test(t, func(t *testing.T) {
		// Given: one idle node pool whose ComputeClass declares a minimumCapacity floor
		// covering every node in it.
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		assert.Equal(t, nodesPerCube, countNodes(ctx, t, infra), "unexpected initial node count")

		// When: the autoscaler tries to consolidate the idle capacity and the status is flushed.
		for range caLoops {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
		}
		time.Sleep(consolidationFlushInterval)

		// Then: the floor holds every node.
		assert.Equal(t, nodesPerCube, countNodes(ctx, t, infra), "the minimumCapacity floor must keep every node")

		// Then: the pinned nodes are attributed to the floor, not to a phantom scheduling
		// failure. Comparing the whole slice also asserts that nothing is reported under
		// noPlaceToMovePods.
		updatedCCC, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, cccName, metav1.GetOptions{})
		assert.NoError(t, err)

		consolidation := consolidationForPriority(t, updatedCCC, firstPriority)
		if !assert.NotNil(t, consolidation, "expected a Consolidation status block on priority %s", firstPriority) {
			return
		}
		assert.Equal(t, []v1.ConsolidationBlockedNodesInfo{
			{Reason: reasonMinCapacityReached, NodeCount: nodesPerCube},
		}, consolidation.BlockedNodes, "expected the whole node pool to be reported blocked by the floor")
		assert.Equal(t, 0, ptr.Deref(consolidation.NotProcessed, 0), "expected no unprocessed nodes")
		assert.Equal(t, 0, ptr.Deref(consolidation.ActuationInProgress, 0), "expected no deletion in flight")
	})
}

// consolidationForPriority returns the Consolidation block reported for the given CCC priority,
// or nil if the priority has none.
func consolidationForPriority(t *testing.T, cc *v1.ComputeClass, priority string) *v1.ConsolidationStatus {
	t.Helper()
	for _, ps := range cc.Status.PriorityStatuses {
		if ps.Identifier == priority {
			return ps.Consolidation
		}
	}
	return nil
}
