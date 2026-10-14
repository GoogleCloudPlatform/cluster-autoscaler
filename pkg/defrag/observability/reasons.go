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

// Package observability records why individual nodes could not be defragmented.
//
// The defrag pipeline drops nodes at roughly a dozen different points, and at
// most of them a structured reason is computed and then discarded. This package
// provides a small sink that those filters write into during a single defrag
// pass, so that the reason can later be aggregated and reported (for example
// into the ComputeClass status).
package observability

// BlockReason identifies why a node that is otherwise eligible for migration
// cannot currently be migrated.
type BlockReason string

const (
	// NodeNotReady means the node is not ready or not schedulable, so the
	// autoscaler will not act on it.
	NodeNotReady BlockReason = "NodeNotReady"
	// Cordoned means the node was marked unschedulable outside of the
	// autoscaler, and no autoscaler-driven deletion is in flight for it.
	Cordoned BlockReason = "Cordoned"
	// NodeConsolidationDisabled means the node carries the
	// cluster-autoscaler.kubernetes.io/scale-down-disabled annotation.
	NodeConsolidationDisabled BlockReason = "NodeConsolidationDisabled"
	// NodePoolOperationInProgress means an operation on the node pool (such as
	// an upgrade or a repair) currently blocks scale-down of its nodes.
	NodePoolOperationInProgress BlockReason = "NodePoolOperationInProgress"
	// MinCapacityReached means removing the node would breach the node group's
	// minimum size or the ComputeClass's minimum or target node count.
	MinCapacityReached BlockReason = "MinCapacityReached"
	// DisruptionBudgetReached means a disruption budget configured in
	// spec.activeMigration.reconciliationPolicy.disruptionBudgets limits how
	// many nodes may be migrated concurrently, and that limit has been reached.
	DisruptionBudgetReached BlockReason = "DisruptionBudgetReached"
	// BlockingPods means the node hosts pods that cannot be safely evicted, for
	// example pods using local storage or pods without a controller.
	BlockingPods BlockReason = "BlockingPods"
	// PodDisruptionBudget means evicting the node's pods would violate a pod
	// disruption budget.
	PodDisruptionBudget BlockReason = "PodDisruptionBudget"
	// ReplacementUnavailable means no replacement node can be provisioned, for
	// example because of a stockout, exhausted quota, or because no rule of the
	// ComputeClass yields a valid scale-up option.
	ReplacementUnavailable BlockReason = "ReplacementUnavailable"
	// RecentMigrationFailure means a recent migration attempt for the node
	// failed and the node is currently backed off.
	RecentMigrationFailure BlockReason = "RecentMigrationFailure"
	// AtomicGroupBlocked means the node itself is not blocked, but it belongs to
	// an atomic group that cannot be migrated as a whole. It should always be
	// read together with the other reasons reported for the same ComputeClass,
	// which explain why the group is stuck.
	AtomicGroupBlocked BlockReason = "AtomicGroupBlocked"
	// MigrationBlocked means the autoscaler's node consolidation eligibility
	// pipeline rejected the node and no more specific reason could be
	// attributed. It is the catch-all that keeps the reported buckets summing
	// to the total number of nodes.
	MigrationBlocked BlockReason = "MigrationBlocked"
)

// reasonPrecedence orders reasons from most to least significant.
//
// A node is frequently blocked by several things at once. Reporting whichever
// blocker the pipeline happened to notice first would make the reported reason
// depend on filter ordering, and would make counts flap as unrelated parts of
// the pipeline change. Instead a fixed precedence is applied, chosen so that
// the reason a user can act on wins, and so that a blocker that outlasts the
// others is not hidden behind one that may clear by itself:
//
//  1. platform operations on the node pool: NodePoolOperationInProgress. An
//     upgrade or a repair cordons and recreates the nodes it works on, so
//     for as long as it runs it is the explanation for the node's own state;
//  2. the node's own state and explicit opt-outs: NodeNotReady, Cordoned,
//     NodeConsolidationDisabled;
//  3. configured minimums, which hold the node back regardless of what runs
//     on it: MinCapacityReached;
//  4. workload constraints: BlockingPods, PodDisruptionBudget;
//  5. replacement capacity and recent failures: ReplacementUnavailable,
//     RecentMigrationFailure;
//  6. the migration's own pacing: DisruptionBudgetReached. It clears by
//     itself as the nodes ahead in the queue finish migrating, so it ranks
//     below every blocker that will still be there afterwards;
//  7. group coupling and the catch-all: AtomicGroupBlocked, MigrationBlocked.
//
// MinCapacityReached is a configured minimum (node group minimum size,
// ComputeClass minimum or target node count), not a capacity shortage, which
// is why it ranks above the workload constraints: a node at its minimum stays
// blocked after every pod on it becomes evictable, so reporting BlockingPods
// for it would send the user to fix the wrong thing. Capacity that cannot be
// obtained is ReplacementUnavailable.
var reasonPrecedence = []BlockReason{
	NodePoolOperationInProgress,
	NodeNotReady,
	Cordoned,
	NodeConsolidationDisabled,
	MinCapacityReached,
	BlockingPods,
	PodDisruptionBudget,
	ReplacementUnavailable,
	RecentMigrationFailure,
	DisruptionBudgetReached,
	AtomicGroupBlocked,
	MigrationBlocked,
}

var reasonRank = func() map[BlockReason]int {
	ranks := make(map[BlockReason]int, len(reasonPrecedence))
	for i, reason := range reasonPrecedence {
		ranks[reason] = i
	}
	return ranks
}()

// AllReasons returns every known reason, ordered by descending precedence.
//
// The returned slice is a copy and may be modified by the caller. It is
// primarily useful for pre-initializing metrics and for producing deterministic
// output ordering.
func AllReasons() []BlockReason {
	reasons := make([]BlockReason, len(reasonPrecedence))
	copy(reasons, reasonPrecedence)
	return reasons
}

// rank returns the precedence of a reason. Unknown reasons rank below every
// known reason, so that an unrecognized value can never mask a known one.
func rank(reason BlockReason) int {
	if r, ok := reasonRank[reason]; ok {
		return r
	}
	return len(reasonPrecedence)
}

// Outranks reports whether reason a is more significant than reason b.
func Outranks(a, b BlockReason) bool {
	return rank(a) < rank(b)
}

// MoreSignificant returns whichever of the two reasons should be reported for
// a node that both apply to. An empty reason stands for "nothing observed" and
// never wins over a non-empty one.
//
// Callers that discover a node's blockers one check at a time fold them
// through this instead of returning the first one found, so that the reported
// reason follows the precedence rather than the order their checks happen to
// run in.
func MoreSignificant(a, b BlockReason) BlockReason {
	if a == "" {
		return b
	}
	if b == "" {
		return a
	}
	if Outranks(b, a) {
		return b
	}
	return a
}

// Registry collects per-node block reasons observed during a single defrag
// pass.
//
// It is populated by the defrag filters as they reject nodes and read by the
// status reporter later in the same autoscaler loop. It is not safe for
// concurrent use.
type Registry struct {
	reasons map[string]BlockReason
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{reasons: make(map[string]BlockReason)}
}

// Record associates a block reason with a node.
//
// If the node already has a reason recorded, the more significant of the two is
// kept, so callers may record reasons in any order.
func (r *Registry) Record(nodeName string, reason BlockReason) {
	if r == nil || nodeName == "" || reason == "" {
		return
	}
	if existing, ok := r.reasons[nodeName]; ok && !Outranks(reason, existing) {
		return
	}
	r.reasons[nodeName] = reason
}

// Reason returns the reason recorded for a node, if any.
func (r *Registry) Reason(nodeName string) (BlockReason, bool) {
	if r == nil {
		return "", false
	}
	reason, ok := r.reasons[nodeName]
	return reason, ok
}

// Each calls fn for every node with a recorded reason.
//
// Iteration order is unspecified, so callers that need deterministic output
// must sort the results themselves.
func (r *Registry) Each(fn func(nodeName string, reason BlockReason)) {
	if r == nil || fn == nil {
		return
	}
	for nodeName, reason := range r.reasons {
		fn(nodeName, reason)
	}
}
