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
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

const (
	// consolidationFlushInterval is how long to wait for the status Aggregator to push the
	// consolidation updates to the API server. Slightly higher than its flush interval so the
	// test doesn't race with the ticker.
	consolidationFlushInterval = status.BatchFlushInterval + 10*time.Second

	// The reason strings are spelled out rather than imported: they are part of the CCC API
	// contract (a closed enum in the CRD), so a rename upstream must break these tests.
	reasonUsedByFormedSlice = "UsedByFormedSlice"

	// firstPriority is the identifier the CCC status uses for the first priority of a CCC.
	firstPriority = "0"

	cccName      = "tpu-slice-ccc"
	sliceName    = "my-tpu-slice"
	boundPool    = "tpu-slice-1"
	unboundPool  = "tpu-slice-2"
	nodesPerCube = 16
	// Loop 1 marks the idle nodes unneeded (ScaleDownUnneededTime is 1s); loop 2 removes
	// the unbound cube while the slice-bound cube is filtered out and reported as
	// unremovable; loop 3 re-observes the slice-bound cube as blocked, which asserts the
	// reported count is stable rather than a one-off.
	caLoops = 3
)

// TestCCCScaleDownBlockedStatusReportsSliceBoundCube asserts that a slice-bound
// atomic cube kept out of scale-down is reported under usedByFormedSlice, while the
// unbound cube that was actually removed is not reported as blocked. Both pools
// belong to a single CCC priority, so the counts aggregate under that priority's
// Consolidation block.
func TestCCCScaleDownBlockedStatusReportsSliceBoundCube(t *testing.T) {
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

		consolidation := consolidationForPriority(t, updatedCCC, firstPriority)
		if !assert.NotNil(t, consolidation, "expected a Consolidation status block on priority %s", firstPriority) {
			return
		}
		assert.Equal(t, []v1.ConsolidationBlockedNodesInfo{
			{Reason: reasonUsedByFormedSlice, Count: nodesPerCube},
		}, consolidation.BlockedNodes, "expected the whole slice-bound cube to be reported blocked")
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
