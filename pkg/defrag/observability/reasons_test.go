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

package observability

import (
	"testing"
)

func TestAllReasonsIsOrderedAndComplete(t *testing.T) {
	reasons := AllReasons()
	if len(reasons) != len(reasonPrecedence) {
		t.Fatalf("AllReasons() returned %d reasons, want %d", len(reasons), len(reasonPrecedence))
	}
	for i := 1; i < len(reasons); i++ {
		if !Outranks(reasons[i-1], reasons[i]) {
			t.Errorf("AllReasons() not ordered by descending precedence: %q does not outrank %q", reasons[i-1], reasons[i])
		}
	}
	// Mutating the result must not affect subsequent calls.
	reasons[0] = "mutated"
	if AllReasons()[0] == "mutated" {
		t.Error("AllReasons() returned a slice aliasing internal state")
	}
}

func TestOutranks(t *testing.T) {
	testCases := []struct {
		name string
		a    BlockReason
		b    BlockReason
		want bool
	}{
		{name: "user intent beats capacity", a: NodeConsolidationDisabled, b: ReplacementUnavailable, want: true},
		{name: "capacity does not beat user intent", a: ReplacementUnavailable, b: NodeConsolidationDisabled, want: false},
		{name: "catch-all loses to everything", a: MigrationBlocked, b: AtomicGroupBlocked, want: false},
		{name: "everything beats catch-all", a: AtomicGroupBlocked, b: MigrationBlocked, want: true},
		{name: "reason does not outrank itself", a: BlockingPods, b: BlockingPods, want: false},
		{name: "known beats unknown", a: MigrationBlocked, b: BlockReason("SomethingElse"), want: true},
		{name: "unknown loses to known", a: BlockReason("SomethingElse"), b: MigrationBlocked, want: false},
		// A node pool operation cordons and recreates the nodes it works on,
		// so it explains their state rather than being hidden by it.
		{name: "node pool operation beats cordon", a: NodePoolOperationInProgress, b: Cordoned, want: true},
		{name: "node pool operation beats not ready", a: NodePoolOperationInProgress, b: NodeNotReady, want: true},
		// The disruption budget clears by itself as the migration proceeds;
		// the blockers below must stay visible while it does.
		{name: "blocking pods beat disruption budget", a: BlockingPods, b: DisruptionBudgetReached, want: true},
		{name: "pod disruption budget beats disruption budget", a: PodDisruptionBudget, b: DisruptionBudgetReached, want: true},
		{name: "replacement unavailable beats disruption budget", a: ReplacementUnavailable, b: DisruptionBudgetReached, want: true},
		{name: "disruption budget does not beat blocking pods", a: DisruptionBudgetReached, b: BlockingPods, want: false},
		{name: "configured minimum beats blocking pods", a: MinCapacityReached, b: BlockingPods, want: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Outranks(tc.a, tc.b); got != tc.want {
				t.Errorf("Outranks(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestMoreSignificant(t *testing.T) {
	testCases := []struct {
		name string
		a    BlockReason
		b    BlockReason
		want BlockReason
	}{
		{name: "both empty", a: "", b: "", want: ""},
		{name: "empty never wins over a reason", a: "", b: BlockingPods, want: BlockingPods},
		{name: "reason never loses to empty", a: BlockingPods, b: "", want: BlockingPods},
		{name: "higher precedence second", a: ReplacementUnavailable, b: Cordoned, want: Cordoned},
		{name: "higher precedence first", a: Cordoned, b: ReplacementUnavailable, want: Cordoned},
		{name: "same reason", a: BlockingPods, b: BlockingPods, want: BlockingPods},
		{name: "unknown loses to known", a: BlockReason("SomethingElse"), b: MigrationBlocked, want: MigrationBlocked},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := MoreSignificant(tc.a, tc.b); got != tc.want {
				t.Errorf("MoreSignificant(%q, %q) = %q, want %q", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestRegistryRecordKeepsHighestPrecedence(t *testing.T) {
	testCases := []struct {
		name     string
		recorded []BlockReason
		want     BlockReason
	}{
		{
			name:     "single reason",
			recorded: []BlockReason{BlockingPods},
			want:     BlockingPods,
		},
		{
			name:     "higher precedence recorded second wins",
			recorded: []BlockReason{ReplacementUnavailable, NodeConsolidationDisabled},
			want:     NodeConsolidationDisabled,
		},
		{
			name:     "higher precedence recorded first is not overwritten",
			recorded: []BlockReason{NodeConsolidationDisabled, ReplacementUnavailable},
			want:     NodeConsolidationDisabled,
		},
		{
			name:     "catch-all never masks a specific reason",
			recorded: []BlockReason{MigrationBlocked, DisruptionBudgetReached, MigrationBlocked},
			want:     DisruptionBudgetReached,
		},
		{
			name:     "empty reason is ignored",
			recorded: []BlockReason{BlockingPods, ""},
			want:     BlockingPods,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRegistry()
			for _, reason := range tc.recorded {
				r.Record("node-1", reason)
			}
			got, ok := r.Reason("node-1")
			if !ok {
				t.Fatalf("Reason(%q) reported no reason, want %q", "node-1", tc.want)
			}
			if got != tc.want {
				t.Errorf("Reason(%q) = %q, want %q", "node-1", got, tc.want)
			}
		})
	}
}

func TestRegistryTracksNodesIndependently(t *testing.T) {
	r := NewRegistry()
	r.Record("node-1", NodeConsolidationDisabled)
	r.Record("node-2", BlockingPods)

	if got, _ := r.Reason("node-1"); got != NodeConsolidationDisabled {
		t.Errorf("Reason(node-1) = %q, want %q", got, NodeConsolidationDisabled)
	}
	if got, _ := r.Reason("node-2"); got != BlockingPods {
		t.Errorf("Reason(node-2) = %q, want %q", got, BlockingPods)
	}
}

func TestRegistryUnrecordedNode(t *testing.T) {
	r := NewRegistry()
	r.Record("node-1", NodeConsolidationDisabled)

	if _, ok := r.Reason("node-2"); ok {
		t.Error("Reason(node-2) reported a reason for a node that was never recorded")
	}
}

func TestRegistryEmptyNodeNameIsIgnored(t *testing.T) {
	r := NewRegistry()
	r.Record("", NodeConsolidationDisabled)

	if _, ok := r.Reason(""); ok {
		t.Error("Reason(\"\") reported a reason after recording an empty node name")
	}
}

// A nil Registry is tolerated so that call sites do not need to branch when
// observability is disabled.
func TestNilRegistryIsSafe(t *testing.T) {
	var r *Registry

	r.Record("node-1", NodeConsolidationDisabled)

	if _, ok := r.Reason("node-1"); ok {
		t.Error("Reason() on a nil Registry reported a reason")
	}
}
