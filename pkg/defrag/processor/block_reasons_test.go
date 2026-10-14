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

package processor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/observability"
	scaledown_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaledown"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/drain"
)

func TestReasonForExcludedScaleDownCandidate(t *testing.T) {
	stages := &scaledown_processors.ScaleDownCandidateStages{
		ProcessorNames: []string{
			"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodes.PreFilteringScaleDownNodeProcessor",
			"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaleblocking.Processor",
			"example.com/some/pkg.UnmappedProcessor",
		},
		ExclusionReasons: []observability.BlockReason{
			// Vendored upstream, so it declares nothing and is resolved
			// through the lookup table instead.
			"",
			observability.NodePoolOperationInProgress,
			"",
		},
		DroppedBy: map[string]int{
			"at-min-size":        0,
			"blocked-mig":        1,
			"unmapped-stage":     2,
			"index-out-of-range": 99,
		},
	}

	testCases := []struct {
		name     string
		stages   *scaledown_processors.ScaleDownCandidateStages
		nodeName string
		want     observability.BlockReason
	}{
		{
			name:     "vendored processor resolves through the lookup table",
			stages:   stages,
			nodeName: "at-min-size",
			want:     observability.MinCapacityReached,
		},
		{
			name:     "processor declaring its own reason is taken at its word",
			stages:   stages,
			nodeName: "blocked-mig",
			want:     observability.NodePoolOperationInProgress,
		},
		{
			name:     "processor that declares nothing falls back",
			stages:   stages,
			nodeName: "unmapped-stage",
			want:     observability.MigrationBlocked,
		},
		{
			name:     "corrupt index falls back rather than panicking",
			stages:   stages,
			nodeName: "index-out-of-range",
			want:     observability.MigrationBlocked,
		},
		{
			name:     "node that survived the chain falls back",
			stages:   stages,
			nodeName: "survivor",
			want:     observability.MigrationBlocked,
		},
		{
			name: "nil stages falls back",
			// Attribution is optional: a processor chain that cannot report
			// stages must still produce an explainable result.
			stages:   nil,
			nodeName: "any",
			want:     observability.MigrationBlocked,
		},
		{
			name: "declared reason wins over the lookup table",
			// Should the two ever disagree, the processor is the source of
			// truth, because the table is only a stand-in for processors that
			// cannot declare anything.
			stages: &scaledown_processors.ScaleDownCandidateStages{
				ProcessorNames:   []string{"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodes.PreFilteringScaleDownNodeProcessor"},
				ExclusionReasons: []observability.BlockReason{observability.BlockingPods},
				DroppedBy:        map[string]int{"node": 0},
			},
			nodeName: "node",
			want:     observability.BlockingPods,
		},
		{
			name: "missing reasons slice still resolves",
			// Stages built by an older or hand-rolled caller may omit the
			// slice entirely; indexing it must not panic.
			stages: &scaledown_processors.ScaleDownCandidateStages{
				ProcessorNames: []string{"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodes.PreFilteringScaleDownNodeProcessor"},
				DroppedBy:      map[string]int{"node": 0},
			},
			nodeName: "node",
			want:     observability.MinCapacityReached,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, reasonForExcludedScaleDownCandidate(tc.stages, tc.nodeName))
		})
	}
}

// TestBlockReasonForRemovedCandidate pins the boundary the ComputeClass API
// documents between migratingNodes and the ReplacementUnavailable reason.
func TestBlockReasonForRemovedCandidate(t *testing.T) {
	testCases := []struct {
		name   string
		reason removeCandidateReason
		want   observability.BlockReason
	}{
		{
			name:   "no scale up option means the replacement cannot be obtained",
			reason: noScaleUpOptions,
			want:   observability.ReplacementUnavailable,
		},
		{
			name:   "scale up timeout means the replacement never arrived",
			reason: scaleUpTimeoutExceeded,
			want:   observability.ReplacementUnavailable,
		},
		{
			name: "scale down timeout is a failed attempt, not a capacity problem",
			// Capacity was obtained here; the drain or deletion is what stalled.
			reason: scaleDownTimeoutExceeded,
			want:   observability.RecentMigrationFailure,
		},
		{
			name:   "no valid nodes keeps the generic failure",
			reason: noValidNodes,
			want:   observability.RecentMigrationFailure,
		},
		{
			name: "unknown reason still maps to a known value",
			// A reason added later must not leak an empty value, which would
			// drop the node out of the reported totals entirely.
			reason: removeCandidateReason("something_new"),
			want:   observability.RecentMigrationFailure,
		},
	}

	known := make(map[observability.BlockReason]bool)
	for _, reason := range observability.AllReasons() {
		known[reason] = true
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := blockReasonForRemovedCandidate(tc.reason)
			assert.Equal(t, tc.want, got)
			assert.True(t, known[got], "reason %q is not one of observability.AllReasons()", got)
		})
	}
}

// TestReplacementUnavailableOutranksRecentMigrationFailure guards the
// interaction that makes the boundary observable at all.
//
// A candidate abandoned for lack of capacity is backed off in the same pass, so
// both reasons are in play for those nodes. The capacity problem is the one the
// user can act on and must win.
func TestReplacementUnavailableOutranksRecentMigrationFailure(t *testing.T) {
	assert.True(t, observability.Outranks(observability.ReplacementUnavailable, observability.RecentMigrationFailure))
}

func TestReasonForBlockingPod(t *testing.T) {
	testCases := []struct {
		name   string
		reason drain.BlockingPodReason
		want   observability.BlockReason
	}{
		{
			name:   "pdb exhaustion is reported separately",
			reason: drain.NotEnoughPdb,
			want:   observability.PodDisruptionBudget,
		},
		{
			name:   "unreplicated pod",
			reason: drain.NotReplicated,
			want:   observability.BlockingPods,
		},
		{
			name:   "local storage",
			reason: drain.LocalStorageRequested,
			want:   observability.BlockingPods,
		},
		{
			name:   "kube-system pod",
			reason: drain.UnmovableKubeSystemPod,
			want:   observability.BlockingPods,
		},
		{
			name: "unset reason still maps to a known value",
			// A zero value must not leak an empty reason, which would drop the
			// node out of the reported totals entirely.
			reason: drain.NoReason,
			want:   observability.BlockingPods,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := reasonForBlockingPod(tc.reason)
			assert.Equal(t, tc.want, got)
			assert.NotEmpty(t, got, "every blocker must map to a non-empty reason")
		})
	}
}

// TestVendoredScaleDownProcessorReasonsAreKnown guards against a typo in the
// mapping table silently producing a reason the API does not document.
func TestVendoredScaleDownProcessorReasonsAreKnown(t *testing.T) {
	known := make(map[observability.BlockReason]bool)
	for _, reason := range observability.AllReasons() {
		known[reason] = true
	}
	for processor, reason := range vendoredScaleDownProcessorReasons {
		assert.True(t, known[reason], "processor %s maps to unknown reason %q", processor, reason)
	}
}
