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

package status

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"time"

	"github.com/google/go-cmp/cmp"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/drift"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/observability"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const (
	testCrdLabelKey = gkelabels.ComputeClassLabel
	driftedCrdName  = "drifting-cc"
	otherCrdName    = "other-cc"
)

var (
	driftedCrdId = CRDId{CRDLabel: testCrdLabelKey, CRDName: driftedCrdName}
	otherCrdId   = CRDId{CRDLabel: testCrdLabelKey, CRDName: otherCrdName}
)

// nodeOption mutates a node built by newTestNode.
type nodeOption func(*apiv1.Node)

// newTestNode returns a ready, schedulable node not being touched by anything.
func newTestNode(name string, opts ...nodeOption) *apiv1.Node {
	node := test.BuildTestNode(name, 1000, 1000)
	node.Annotations = map[string]string{}
	test.SetNodeReadyState(node, true, time.Now())
	for _, opt := range opts {
		opt(node)
	}
	return node
}

func notReady() nodeOption {
	return func(node *apiv1.Node) { test.SetNodeReadyState(node, false, time.Now()) }
}

func cordoned() nodeOption {
	return func(node *apiv1.Node) { node.Spec.Unschedulable = true }
}

func scaleDownDisabled() nodeOption {
	return func(node *apiv1.Node) {
		node.Annotations["cluster-autoscaler.kubernetes.io/scale-down-disabled"] = "true"
	}
}

func upcoming() nodeOption {
	return func(node *apiv1.Node) { node.Annotations[annotations.NodeUpcomingAnnotation] = "true" }
}

func generatedFromTemplate() nodeOption {
	return func(node *apiv1.Node) { node.Annotations[gkelabels.NodeGeneratedFromTemplateAnnotation] = "true" }
}

func defragCandidate() nodeOption {
	return func(node *apiv1.Node) {
		node.Spec.Taints = append(node.Spec.Taints, apiv1.Taint{
			Key:    defrag.HardTaint,
			Value:  "1700000000",
			Effect: apiv1.TaintEffectNoSchedule,
		})
	}
}

func toBeDeleted() nodeOption {
	return func(node *apiv1.Node) {
		node.Spec.Taints = append(node.Spec.Taints, apiv1.Taint{
			Key:    taints.ToBeDeletedTaint,
			Value:  fmt.Sprint(time.Now().Unix()),
			Effect: apiv1.TaintEffectNoSchedule,
		})
	}
}

func deleted() nodeOption {
	return func(node *apiv1.Node) {
		now := metav1.Now()
		node.DeletionTimestamp = &now
	}
}

// driftSpec is the drift evaluation a test wants a node to see.
type driftSpec struct {
	crdName string
	// noCrd makes the node belong to no ComputeClass at all.
	noCrd bool
	// migrationDisabled makes the node's ComputeClass one that did not opt into
	// config drift migration.
	migrationDisabled bool
	drifted           bool
	// budget is the disruption budget of the node's ComputeClass, or 0 for
	// none. The first node seen for a ComputeClass decides it.
	budget int32
}

// newDriftResolverFor returns a resolver answering from a per-node table.
// Nodes missing from the table belong to no ComputeClass.
func newDriftResolverFor(specs map[string]driftSpec) DriftResolver {
	crds := map[string]crd.CRD{}
	return func(node *apiv1.Node) drift.Result {
		spec, ok := specs[node.Name]
		if !ok || spec.noCrd {
			return drift.Result{}
		}
		if _, ok := crds[spec.crdName]; !ok {
			opts := []crd.TestCrdOption{
				crd.WithLabel(testCrdLabelKey),
				crd.WithName(spec.crdName),
				crd.WithConfigDrift(!spec.migrationDisabled),
			}
			if spec.budget > 0 {
				opts = append(opts, crd.WithMaxNodeDisruption(&spec.budget))
			}
			crds[spec.crdName] = crd.NewTestCrd(opts...)
		}
		return drift.Result{
			CRD:              crds[spec.crdName],
			CRDName:          spec.crdName,
			MigrationEnabled: !spec.migrationDisabled,
			Drifted:          spec.drifted,
		}
	}
}

func newRegistry(reasons map[string]observability.BlockReason) *observability.Registry {
	registry := observability.NewRegistry()
	for nodeName, reason := range reasons {
		registry.Record(nodeName, reason)
	}
	return registry
}

func nodeList(nodes ...*apiv1.Node) []*apiv1.Node {
	return nodes
}

// newImmediateClassifier returns a classifier that reports a block reason as
// soon as it is observed, so that bucketing can be tested without spreading
// every case over several loops. Debouncing itself is covered separately.
func newImmediateClassifier() *ConfigDriftClassifier {
	return &ConfigDriftClassifier{debouncer: newBlockReasonDebouncer(1)}
}

func TestConfigDriftClassifier_Buckets(t *testing.T) {
	tests := map[string]struct {
		nodes               []*apiv1.Node
		specs               map[string]driftSpec
		reasons             map[string]observability.BlockReason
		deletionsInProgress sets.Set[string]
		want                map[CRDId]ConfigDriftCounts
	}{
		"node matching its ComputeClass is current": {
			nodes: []*apiv1.Node{newTestNode("n1")},
			specs: map[string]driftSpec{"n1": {crdName: driftedCrdName}},
			want:  map[CRDId]ConfigDriftCounts{driftedCrdId: {CurrentNodes: 1}},
		},
		"drifted node nothing is holding back is only drifted": {
			nodes: []*apiv1.Node{newTestNode("n1")},
			specs: map[string]driftSpec{"n1": {crdName: driftedCrdName, drifted: true}},
			want:  map[CRDId]ConfigDriftCounts{driftedCrdId: {DriftedNodes: 1}},
		},
		"defrag candidate is migrating": {
			nodes: []*apiv1.Node{newTestNode("n1", defragCandidate())},
			specs: map[string]driftSpec{"n1": {crdName: driftedCrdName, drifted: true}},
			want:  map[CRDId]ConfigDriftCounts{driftedCrdId: {MigratingNodes: 1}},
		},
		"node being drained is migrating": {
			nodes: []*apiv1.Node{newTestNode("n1", toBeDeleted())},
			specs: map[string]driftSpec{"n1": {crdName: driftedCrdName, drifted: true}},
			want:  map[CRDId]ConfigDriftCounts{driftedCrdId: {MigratingNodes: 1}},
		},
		"node with a deletion timestamp is migrating": {
			nodes: []*apiv1.Node{newTestNode("n1", deleted())},
			specs: map[string]driftSpec{"n1": {crdName: driftedCrdName, drifted: true}},
			want:  map[CRDId]ConfigDriftCounts{driftedCrdId: {MigratingNodes: 1}},
		},
		"node the actuator is deleting is migrating": {
			nodes:               []*apiv1.Node{newTestNode("n1")},
			specs:               map[string]driftSpec{"n1": {crdName: driftedCrdName, drifted: true}},
			deletionsInProgress: sets.New("n1"),
			want:                map[CRDId]ConfigDriftCounts{driftedCrdId: {MigratingNodes: 1}},
		},
		"migration outranks a recorded block reason": {
			// The pipeline may have rejected the node before it was picked up
			// by the candidate it now belongs to.
			nodes:   []*apiv1.Node{newTestNode("n1", defragCandidate())},
			specs:   map[string]driftSpec{"n1": {crdName: driftedCrdName, drifted: true}},
			reasons: map[string]observability.BlockReason{"n1": observability.MinCapacityReached},
			want:    map[CRDId]ConfigDriftCounts{driftedCrdId: {MigratingNodes: 1}},
		},
		"reason recorded by the pipeline is reported": {
			nodes:   []*apiv1.Node{newTestNode("n1")},
			specs:   map[string]driftSpec{"n1": {crdName: driftedCrdName, drifted: true}},
			reasons: map[string]observability.BlockReason{"n1": observability.DisruptionBudgetReached},
			want: map[CRDId]ConfigDriftCounts{driftedCrdId: {BlockedNodes: map[observability.BlockReason]int{
				observability.DisruptionBudgetReached: 1,
			}}},
		},
		"cordoned node is reported even though the pipeline never saw it": {
			nodes: []*apiv1.Node{newTestNode("n1", cordoned())},
			specs: map[string]driftSpec{"n1": {crdName: driftedCrdName, drifted: true}},
			want: map[CRDId]ConfigDriftCounts{driftedCrdId: {BlockedNodes: map[observability.BlockReason]int{
				observability.Cordoned: 1,
			}}},
		},
		"unready node is reported as not ready": {
			nodes: []*apiv1.Node{newTestNode("n1", notReady())},
			specs: map[string]driftSpec{"n1": {crdName: driftedCrdName, drifted: true}},
			want: map[CRDId]ConfigDriftCounts{driftedCrdId: {BlockedNodes: map[observability.BlockReason]int{
				observability.NodeNotReady: 1,
			}}},
		},
		"upcoming node is reported as not ready": {
			nodes: []*apiv1.Node{newTestNode("n1", upcoming())},
			specs: map[string]driftSpec{"n1": {crdName: driftedCrdName, drifted: true}},
			want: map[CRDId]ConfigDriftCounts{driftedCrdId: {BlockedNodes: map[observability.BlockReason]int{
				observability.NodeNotReady: 1,
			}}},
		},
		"node excluded from consolidation is reported": {
			nodes: []*apiv1.Node{newTestNode("n1", scaleDownDisabled())},
			specs: map[string]driftSpec{"n1": {crdName: driftedCrdName, drifted: true}},
			want: map[CRDId]ConfigDriftCounts{driftedCrdId: {BlockedNodes: map[observability.BlockReason]int{
				observability.NodeConsolidationDisabled: 1,
			}}},
		},
		"node state outranks the reason the pipeline recorded": {
			nodes:   []*apiv1.Node{newTestNode("n1", cordoned())},
			specs:   map[string]driftSpec{"n1": {crdName: driftedCrdName, drifted: true}},
			reasons: map[string]observability.BlockReason{"n1": observability.BlockingPods},
			want: map[CRDId]ConfigDriftCounts{driftedCrdId: {BlockedNodes: map[observability.BlockReason]int{
				observability.Cordoned: 1,
			}}},
		},
		"nodes blocked by the same reason share a bucket": {
			nodes: []*apiv1.Node{newTestNode("n1"), newTestNode("n2"), newTestNode("n3")},
			specs: map[string]driftSpec{
				"n1": {crdName: driftedCrdName, drifted: true},
				"n2": {crdName: driftedCrdName, drifted: true},
				"n3": {crdName: driftedCrdName, drifted: true},
			},
			reasons: map[string]observability.BlockReason{
				"n1": observability.BlockingPods,
				"n2": observability.BlockingPods,
				"n3": observability.PodDisruptionBudget,
			},
			want: map[CRDId]ConfigDriftCounts{driftedCrdId: {BlockedNodes: map[observability.BlockReason]int{
				observability.BlockingPods:        2,
				observability.PodDisruptionBudget: 1,
			}}},
		},
		"nodes are counted per ComputeClass": {
			nodes: []*apiv1.Node{newTestNode("n1"), newTestNode("n2"), newTestNode("n3")},
			specs: map[string]driftSpec{
				"n1": {crdName: driftedCrdName, drifted: true},
				"n2": {crdName: driftedCrdName},
				"n3": {crdName: otherCrdName, drifted: true},
			},
			reasons: map[string]observability.BlockReason{"n3": observability.ReplacementUnavailable},
			want: map[CRDId]ConfigDriftCounts{
				driftedCrdId: {CurrentNodes: 1, DriftedNodes: 1},
				otherCrdId: {BlockedNodes: map[observability.BlockReason]int{
					observability.ReplacementUnavailable: 1,
				}},
			},
		},
		"ComputeClass that did not enable migration is not reported": {
			nodes: []*apiv1.Node{newTestNode("n1")},
			specs: map[string]driftSpec{"n1": {crdName: driftedCrdName, migrationDisabled: true}},
			want:  map[CRDId]ConfigDriftCounts{},
		},
		"node outside any ComputeClass is not reported": {
			nodes: []*apiv1.Node{newTestNode("n1")},
			specs: map[string]driftSpec{"n1": {noCrd: true}},
			want:  map[CRDId]ConfigDriftCounts{},
		},
		"every node waiting for an exhausted disruption budget is blocked by it": {
			// The pipeline only recorded the reason for the one node it
			// happened to try.
			nodes: []*apiv1.Node{newTestNode("n1", defragCandidate()), newTestNode("n2"), newTestNode("n3")},
			specs: map[string]driftSpec{
				"n1": {crdName: driftedCrdName, drifted: true, budget: 1},
				"n2": {crdName: driftedCrdName, drifted: true, budget: 1},
				"n3": {crdName: driftedCrdName, drifted: true, budget: 1},
			},
			reasons: map[string]observability.BlockReason{"n2": observability.DisruptionBudgetReached},
			want: map[CRDId]ConfigDriftCounts{driftedCrdId: {
				MigratingNodes: 1,
				BlockedNodes:   map[observability.BlockReason]int{observability.DisruptionBudgetReached: 2},
			}},
		},
		"exhausted disruption budget does not hide a node's own blocker": {
			// The budget clears by itself as n1 finishes migrating; n2's bare
			// pod and n3's PDB do not, and the user has to deal with them.
			nodes: []*apiv1.Node{newTestNode("n1", defragCandidate()), newTestNode("n2"), newTestNode("n3"), newTestNode("n4")},
			specs: map[string]driftSpec{
				"n1": {crdName: driftedCrdName, drifted: true, budget: 1},
				"n2": {crdName: driftedCrdName, drifted: true, budget: 1},
				"n3": {crdName: driftedCrdName, drifted: true, budget: 1},
				"n4": {crdName: driftedCrdName, drifted: true, budget: 1},
			},
			reasons: map[string]observability.BlockReason{
				"n2": observability.BlockingPods,
				"n3": observability.PodDisruptionBudget,
			},
			want: map[CRDId]ConfigDriftCounts{driftedCrdId: {
				MigratingNodes: 1,
				BlockedNodes: map[observability.BlockReason]int{
					observability.BlockingPods:            1,
					observability.PodDisruptionBudget:     1,
					observability.DisruptionBudgetReached: 1,
				},
			}},
		},
		"disruption budget is consumed by nodes that have not drifted": {
			nodes: []*apiv1.Node{newTestNode("n1", toBeDeleted()), newTestNode("n2")},
			specs: map[string]driftSpec{
				"n1": {crdName: driftedCrdName, budget: 1},
				"n2": {crdName: driftedCrdName, drifted: true, budget: 1},
			},
			want: map[CRDId]ConfigDriftCounts{driftedCrdId: {
				CurrentNodes: 1,
				BlockedNodes: map[observability.BlockReason]int{observability.DisruptionBudgetReached: 1},
			}},
		},
		"disruption budget with room left leaves waiting nodes drifted": {
			nodes: []*apiv1.Node{newTestNode("n1", defragCandidate()), newTestNode("n2")},
			specs: map[string]driftSpec{
				"n1": {crdName: driftedCrdName, drifted: true, budget: 2},
				"n2": {crdName: driftedCrdName, drifted: true, budget: 2},
			},
			want: map[CRDId]ConfigDriftCounts{driftedCrdId: {MigratingNodes: 1, DriftedNodes: 1}},
		},
		"disruption budget of one ComputeClass does not block another": {
			nodes: []*apiv1.Node{newTestNode("n1", defragCandidate()), newTestNode("n2")},
			specs: map[string]driftSpec{
				"n1": {crdName: driftedCrdName, drifted: true, budget: 1},
				"n2": {crdName: otherCrdName, drifted: true},
			},
			want: map[CRDId]ConfigDriftCounts{
				driftedCrdId: {MigratingNodes: 1},
				otherCrdId:   {DriftedNodes: 1},
			},
		},
		"node state outranks an exhausted disruption budget": {
			nodes: []*apiv1.Node{newTestNode("n1", defragCandidate()), newTestNode("n2", cordoned())},
			specs: map[string]driftSpec{
				"n1": {crdName: driftedCrdName, drifted: true, budget: 1},
				"n2": {crdName: driftedCrdName, drifted: true, budget: 1},
			},
			want: map[CRDId]ConfigDriftCounts{driftedCrdId: {
				MigratingNodes: 1,
				BlockedNodes:   map[observability.BlockReason]int{observability.Cordoned: 1},
			}},
		},
		"node generated from a template is not a real node": {
			nodes: []*apiv1.Node{newTestNode("n1", generatedFromTemplate())},
			specs: map[string]driftSpec{"n1": {crdName: driftedCrdName, drifted: true}},
			want:  map[CRDId]ConfigDriftCounts{},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, evaluated := newImmediateClassifier().Classify(ConfigDriftSnapshot{
				Nodes:               nodeList(tc.nodes...),
				Drift:               newDriftResolverFor(tc.specs),
				BlockReasons:        newRegistry(tc.reasons),
				DeletionsInProgress: tc.deletionsInProgress,
			})
			if !evaluated {
				t.Fatalf("Classify() reported the loop as inconclusive, want conclusive")
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("Classify() counts mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestConfigDriftClassifier_SumInvariant asserts the property users depend on:
// the buckets partition the nodes of a ComputeClass, so the reported numbers add
// up to the size of the ComputeClass and can be read as progress.
func TestConfigDriftClassifier_SumInvariant(t *testing.T) {
	nodeStates := []struct {
		name string
		opts []nodeOption
	}{
		{name: "healthy"},
		{name: "cordoned", opts: []nodeOption{cordoned()}},
		{name: "unready", opts: []nodeOption{notReady()}},
		{name: "unready-and-cordoned", opts: []nodeOption{notReady(), cordoned()}},
		{name: "consolidation-disabled", opts: []nodeOption{scaleDownDisabled()}},
		{name: "upcoming", opts: []nodeOption{upcoming()}},
		{name: "candidate", opts: []nodeOption{defragCandidate()}},
		{name: "draining", opts: []nodeOption{toBeDeleted()}},
		{name: "deleting", opts: []nodeOption{deleted()}},
		{name: "candidate-and-cordoned", opts: []nodeOption{defragCandidate(), cordoned()}},
	}
	// Every reason, plus no reason at all, plus one the classifier has never
	// heard of, which must still be counted somewhere.
	recorded := append(observability.AllReasons(), "", observability.BlockReason("SomethingNew"))

	var nodes []*apiv1.Node
	specs := map[string]driftSpec{}
	reasons := map[string]observability.BlockReason{}
	deletionsInProgress := sets.New[string]()
	expectedPerCrd := map[string]int{}

	i := 0
	for _, state := range nodeStates {
		for _, reason := range recorded {
			for _, drifted := range []bool{true, false} {
				i++
				name := fmt.Sprintf("node-%d-%s", i, state.name)
				nodes = append(nodes, newTestNode(name, state.opts...))
				crdName := driftedCrdName
				var budget int32
				if i%3 == 0 {
					// Exhausted by the disrupted nodes among its own, so the
					// budget-derived reason is covered too.
					crdName, budget = otherCrdName, 1
				}
				specs[name] = driftSpec{crdName: crdName, drifted: drifted, budget: budget}
				expectedPerCrd[crdName]++
				if reason != "" {
					reasons[name] = reason
				}
				if i%7 == 0 {
					deletionsInProgress.Insert(name)
				}
			}
		}
	}
	// Nodes that must not appear in any count.
	nodes = append(nodes,
		newTestNode("ignored-no-crd"),
		newTestNode("ignored-migration-disabled"),
		newTestNode("ignored-template", generatedFromTemplate()),
	)
	specs["ignored-no-crd"] = driftSpec{noCrd: true}
	specs["ignored-migration-disabled"] = driftSpec{crdName: driftedCrdName, migrationDisabled: true, drifted: true}
	specs["ignored-template"] = driftSpec{crdName: driftedCrdName, drifted: true}

	classifier := NewConfigDriftClassifier()
	// Several loops, so that the invariant is asserted both while reasons are
	// still being debounced and after they have been confirmed.
	for loop := 1; loop <= defaultBlockReasonDebounceLoops+1; loop++ {
		got, evaluated := classifier.Classify(ConfigDriftSnapshot{
			Nodes:               nodeList(nodes...),
			Drift:               newDriftResolverFor(specs),
			BlockReasons:        newRegistry(reasons),
			DeletionsInProgress: deletionsInProgress,
		})
		if !evaluated {
			t.Fatalf("loop %d: Classify() reported the loop as inconclusive, want conclusive", loop)
		}
		if len(got) != len(expectedPerCrd) {
			t.Fatalf("loop %d: Classify() reported %d ComputeClasses, want %d", loop, len(got), len(expectedPerCrd))
		}
		for crdId, counts := range got {
			if want := expectedPerCrd[crdId.CRDName]; counts.TotalNodes() != want {
				t.Errorf("loop %d: ComputeClass %q counts add up to %d, want %d: %+v",
					loop, crdId.CRDName, counts.TotalNodes(), want, counts)
			}
		}
	}
}

func TestConfigDriftClassifier_Debounce(t *testing.T) {
	node := newTestNode("n1")
	specs := map[string]driftSpec{"n1": {crdName: driftedCrdName, drifted: true}}

	classify := func(t *testing.T, classifier *ConfigDriftClassifier, reason observability.BlockReason) ConfigDriftCounts {
		t.Helper()
		reasons := map[string]observability.BlockReason{}
		if reason != "" {
			reasons["n1"] = reason
		}
		got, evaluated := classifier.Classify(ConfigDriftSnapshot{
			Nodes:        nodeList(node),
			Drift:        newDriftResolverFor(specs),
			BlockReasons: newRegistry(reasons),
		})
		if !evaluated {
			t.Fatalf("Classify() reported the loop as inconclusive, want conclusive")
		}
		return got[driftedCrdId]
	}
	blockedOn := func(reason observability.BlockReason) ConfigDriftCounts {
		return ConfigDriftCounts{BlockedNodes: map[observability.BlockReason]int{reason: 1}}
	}
	waiting := ConfigDriftCounts{DriftedNodes: 1}

	t.Run("reason is reported only once it persists", func(t *testing.T) {
		classifier := NewConfigDriftClassifier()
		for loop := 1; loop < defaultBlockReasonDebounceLoops; loop++ {
			if got := classify(t, classifier, observability.BlockingPods); !cmp.Equal(waiting, got) {
				t.Errorf("loop %d: got %+v, want the node to still be counted as waiting", loop, got)
			}
		}
		got := classify(t, classifier, observability.BlockingPods)
		if diff := cmp.Diff(blockedOn(observability.BlockingPods), got); diff != "" {
			t.Errorf("loop %d counts mismatch (-want +got):\n%s", defaultBlockReasonDebounceLoops, diff)
		}
		// And it keeps being reported for as long as it holds.
		got = classify(t, classifier, observability.BlockingPods)
		if diff := cmp.Diff(blockedOn(observability.BlockingPods), got); diff != "" {
			t.Errorf("counts mismatch after the reason was confirmed (-want +got):\n%s", diff)
		}
	})

	t.Run("a changed reason starts over", func(t *testing.T) {
		classifier := NewConfigDriftClassifier()
		for loop := 1; loop < defaultBlockReasonDebounceLoops; loop++ {
			classify(t, classifier, observability.BlockingPods)
		}
		if got := classify(t, classifier, observability.MinCapacityReached); !cmp.Equal(waiting, got) {
			t.Errorf("got %+v, want a reason observed for the first time not to be reported", got)
		}
	})

	t.Run("an unconfirmed reason interrupted by an unblocked loop starts over", func(t *testing.T) {
		classifier := NewConfigDriftClassifier()
		for loop := 1; loop < defaultBlockReasonDebounceLoops; loop++ {
			classify(t, classifier, observability.BlockingPods)
		}
		if got := classify(t, classifier, ""); !cmp.Equal(waiting, got) {
			t.Fatalf("got %+v, want the node to be counted as waiting once nothing blocks it", got)
		}
		if got := classify(t, classifier, observability.BlockingPods); !cmp.Equal(waiting, got) {
			t.Errorf("got %+v, want the node to have to earn its way back into the blocked bucket", got)
		}
	})

	t.Run("a confirmed reason is reported until a new one is confirmed", func(t *testing.T) {
		classifier := NewConfigDriftClassifier()
		for loop := 1; loop <= defaultBlockReasonDebounceLoops; loop++ {
			classify(t, classifier, observability.BlockingPods)
		}
		for loop := 1; loop < defaultBlockReasonDebounceLoops; loop++ {
			got := classify(t, classifier, observability.MinCapacityReached)
			if diff := cmp.Diff(blockedOn(observability.BlockingPods), got); diff != "" {
				t.Errorf("loop %d after the change: want the confirmed reason to still be reported (-want +got):\n%s", loop, diff)
			}
		}
		got := classify(t, classifier, observability.MinCapacityReached)
		if diff := cmp.Diff(blockedOn(observability.MinCapacityReached), got); diff != "" {
			t.Errorf("want the new reason once it persists (-want +got):\n%s", diff)
		}
	})

	t.Run("a confirmed reason is reported until the node is confirmed unblocked", func(t *testing.T) {
		classifier := NewConfigDriftClassifier()
		for loop := 1; loop <= defaultBlockReasonDebounceLoops; loop++ {
			classify(t, classifier, observability.DisruptionBudgetReached)
		}
		for loop := 1; loop < defaultBlockReasonDebounceLoops; loop++ {
			got := classify(t, classifier, "")
			if diff := cmp.Diff(blockedOn(observability.DisruptionBudgetReached), got); diff != "" {
				t.Errorf("loop %d without a reason: want the confirmed reason to still be reported (-want +got):\n%s", loop, diff)
			}
		}
		if got := classify(t, classifier, ""); !cmp.Equal(waiting, got) {
			t.Errorf("got %+v, want the node to be counted as waiting once it stays unblocked", got)
		}
	})

	t.Run("a node that stops waiting is forgotten", func(t *testing.T) {
		classifier := NewConfigDriftClassifier()
		for loop := 1; loop <= defaultBlockReasonDebounceLoops; loop++ {
			classify(t, classifier, observability.BlockingPods)
		}
		got, _ := classifier.Classify(ConfigDriftSnapshot{
			Nodes:        nodeList(newTestNode("n1", defragCandidate())),
			Drift:        newDriftResolverFor(specs),
			BlockReasons: newRegistry(nil),
		})
		if want := (ConfigDriftCounts{MigratingNodes: 1}); !cmp.Equal(want, got[driftedCrdId]) {
			t.Fatalf("got %+v, want migration to be reported immediately", got[driftedCrdId])
		}
		if got := classify(t, classifier, observability.BlockingPods); !cmp.Equal(waiting, got) {
			t.Errorf("got %+v, want the node to have to earn its way back into the blocked bucket", got)
		}
	})

	t.Run("an inconclusive loop does not break a streak", func(t *testing.T) {
		classifier := NewConfigDriftClassifier()
		for loop := 1; loop < defaultBlockReasonDebounceLoops; loop++ {
			classify(t, classifier, observability.BlockingPods)
		}
		if _, evaluated := classifier.Classify(ConfigDriftSnapshot{
			Nodes: nodeList(node),
			Drift: newDriftResolverFor(specs),
		}); evaluated {
			t.Fatalf("Classify() reported a loop without recorded reasons as conclusive")
		}
		got := classify(t, classifier, observability.BlockingPods)
		if diff := cmp.Diff(blockedOn(observability.BlockingPods), got); diff != "" {
			t.Errorf("counts mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestConfigDriftClassifier_InconclusiveLoopReportsNothing(t *testing.T) {
	got, evaluated := NewConfigDriftClassifier().Classify(ConfigDriftSnapshot{
		Nodes: nodeList(newTestNode("n1")),
		Drift: newDriftResolverFor(map[string]driftSpec{"n1": {crdName: driftedCrdName}}),
	})
	if evaluated {
		t.Errorf("Classify() = _, true, want false when defrag did not evaluate nodes")
	}
	if got != nil {
		t.Errorf("Classify() = %+v, want no counts when defrag did not evaluate nodes", got)
	}
}

// TestConfigDriftClassifier_NodeLeavingItsNodeGroup covers the end of a node's
// deletion, when its VM has already left the node group but the Node object is
// still registered, so the node can no longer be resolved to a ComputeClass.
func TestConfigDriftClassifier_NodeLeavingItsNodeGroup(t *testing.T) {
	gone := map[string]driftSpec{"n1": {noCrd: true}}
	classify := func(t *testing.T, classifier *ConfigDriftClassifier, node *apiv1.Node, specs map[string]driftSpec) map[CRDId]ConfigDriftCounts {
		t.Helper()
		got, evaluated := classifier.Classify(ConfigDriftSnapshot{
			Nodes:        nodeList(node),
			Drift:        newDriftResolverFor(specs),
			BlockReasons: newRegistry(nil),
		})
		if !evaluated {
			t.Fatalf("Classify() reported the loop as inconclusive, want conclusive")
		}
		return got
	}

	t.Run("drifted node being deleted is still migrating", func(t *testing.T) {
		classifier := newImmediateClassifier()
		node := newTestNode("n1", toBeDeleted())
		classify(t, classifier, node, map[string]driftSpec{"n1": {crdName: driftedCrdName, drifted: true}})
		// Twice, so that the fallback is shown to carry over from a loop that
		// itself relied on it.
		for loop := 1; loop <= 2; loop++ {
			got := classify(t, classifier, node, gone)
			if diff := cmp.Diff(map[CRDId]ConfigDriftCounts{driftedCrdId: {MigratingNodes: 1}}, got); diff != "" {
				t.Errorf("loop %d after the node group disappeared (-want +got):\n%s", loop, diff)
			}
		}
	})

	t.Run("node being scaled down is still current", func(t *testing.T) {
		classifier := newImmediateClassifier()
		node := newTestNode("n1", deleted())
		classify(t, classifier, node, map[string]driftSpec{"n1": {crdName: driftedCrdName}})
		got := classify(t, classifier, node, gone)
		if diff := cmp.Diff(map[CRDId]ConfigDriftCounts{driftedCrdId: {CurrentNodes: 1}}, got); diff != "" {
			t.Errorf("counts mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("node not being deleted is dropped", func(t *testing.T) {
		classifier := newImmediateClassifier()
		node := newTestNode("n1")
		classify(t, classifier, node, map[string]driftSpec{"n1": {crdName: driftedCrdName, drifted: true}})
		if got := classify(t, classifier, node, gone); len(got) != 0 {
			t.Errorf("Classify() = %+v, want a node that is not being deleted to count only while it resolves", got)
		}
	})

	t.Run("node never resolved is dropped", func(t *testing.T) {
		if got := classify(t, newImmediateClassifier(), newTestNode("n1", toBeDeleted()), gone); len(got) != 0 {
			t.Errorf("Classify() = %+v, want no counts for a node whose ComputeClass was never known", got)
		}
	})

	t.Run("evaluation is forgotten once the node is gone", func(t *testing.T) {
		classifier := newImmediateClassifier()
		node := newTestNode("n1", toBeDeleted())
		classify(t, classifier, node, map[string]driftSpec{"n1": {crdName: driftedCrdName, drifted: true}})
		if _, evaluated := classifier.Classify(ConfigDriftSnapshot{
			Drift:        newDriftResolverFor(gone),
			BlockReasons: newRegistry(nil),
		}); !evaluated {
			t.Fatalf("Classify() reported the loop as inconclusive, want conclusive")
		}
		if got := classify(t, classifier, node, gone); len(got) != 0 {
			t.Errorf("Classify() = %+v, want a node name reused later not to inherit a stale evaluation", got)
		}
	})
}

func TestConfigDriftCounts_SortedBlockedNodes(t *testing.T) {
	counts := ConfigDriftCounts{BlockedNodes: map[observability.BlockReason]int{
		observability.MigrationBlocked: 1,
		observability.NodeNotReady:     2,
		observability.BlockingPods:     3,
		"Zebra":                        4,
		"Aardvark":                     5,
	}}
	want := []BlockedNodesCount{
		{Reason: observability.NodeNotReady, Count: 2},
		{Reason: observability.BlockingPods, Count: 3},
		{Reason: observability.MigrationBlocked, Count: 1},
		// Reasons outside the precedence list rank equally and are ordered by
		// name, so the output stays stable.
		{Reason: "Aardvark", Count: 5},
		{Reason: "Zebra", Count: 4},
	}
	if diff := cmp.Diff(want, counts.SortedBlockedNodes()); diff != "" {
		t.Errorf("SortedBlockedNodes() mismatch (-want +got):\n%s", diff)
	}
	empty := ConfigDriftCounts{}
	if got := empty.SortedBlockedNodes(); got != nil {
		t.Errorf("SortedBlockedNodes() = %+v, want nil when nothing is blocked", got)
	}
}

// fakeNodeGroup embeds the interface so that any method other than Id() panics,
// asserting that node groups are only ever identified by ID.
type fakeNodeGroup struct {
	cloudprovider.NodeGroup
	id string
}

func (f *fakeNodeGroup) Id() string { return f.id }

// fakeNodeGroupProvider resolves nodes to node groups by node name.
type fakeNodeGroupProvider struct {
	nodeGroups map[string]cloudprovider.NodeGroup
	err        error
}

func (p *fakeNodeGroupProvider) NodeGroupForNode(_ context.Context, node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	if p.err != nil {
		return nil, p.err
	}
	return p.nodeGroups[node.Name], nil
}

// fakeCRDLister resolves every node group to the same ComputeClass, counting
// lookups so that caching can be asserted.
type fakeCRDLister struct {
	c     crd.CRD
	name  string
	calls int
}

func (l *fakeCRDLister) NodeGroupCrd(cloudprovider.NodeGroup) (crd.CRD, string, error) {
	l.calls++
	return l.c, l.name, nil
}

type fakeMatcher struct{ matches bool }

func (m *fakeMatcher) MatchesCrdLabel(cloudprovider.NodeGroup, crd.CRD) bool  { return true }
func (m *fakeMatcher) MatchesCrdConfig(cloudprovider.NodeGroup, crd.CRD) bool { return m.matches }
func (m *fakeMatcher) FirstMatchedRule(cloudprovider.NodeGroup, crd.CRD) (bool, int, rules.Rule) {
	return m.matches, 0, nil
}

func TestNewDriftResolver(t *testing.T) {
	testCrd := crd.NewTestCrd(
		crd.WithLabel(testCrdLabelKey),
		crd.WithName(driftedCrdName),
		crd.WithConfigDrift(true),
	)
	nodeGroup := &fakeNodeGroup{id: "ng-1"}

	t.Run("resolves and evaluates the node's node group", func(t *testing.T) {
		lister := &fakeCRDLister{c: testCrd, name: driftedCrdName}
		resolver := NewDriftResolver(
			t.Context(),
			&fakeNodeGroupProvider{nodeGroups: map[string]cloudprovider.NodeGroup{
				"n1": nodeGroup,
				"n2": nodeGroup,
			}},
			drift.NewCache(drift.NewEvaluator(lister, &fakeMatcher{matches: false})),
		)
		// Compared by value rather than with cmp: a ComputeClass is an opaque
		// object, and the result only ever carries the one built above.
		want := drift.Result{CRD: testCrd, CRDName: driftedCrdName, MigrationEnabled: true, Drifted: true}
		for _, nodeName := range []string{"n1", "n2"} {
			if got := resolver(newTestNode(nodeName)); got != want {
				t.Errorf("resolver(%q) = %+v, want %+v", nodeName, got, want)
			}
		}
		if lister.calls != 1 {
			t.Errorf("ComputeClass looked up %d times, want 1: nodes sharing a node group must share the cached evaluation", lister.calls)
		}
	})

	t.Run("nodes outside any node group belong to no ComputeClass", func(t *testing.T) {
		resolver := NewDriftResolver(
			t.Context(),
			&fakeNodeGroupProvider{},
			drift.NewCache(drift.NewEvaluator(&fakeCRDLister{c: testCrd, name: driftedCrdName}, &fakeMatcher{})),
		)
		if got := resolver(newTestNode("n1")); got != (drift.Result{}) {
			t.Errorf("resolver() = %+v, want an empty result", got)
		}
	})

	t.Run("a lookup failure is not reported as drift", func(t *testing.T) {
		resolver := NewDriftResolver(
			t.Context(),
			&fakeNodeGroupProvider{err: errors.New("cloud provider is down")},
			drift.NewCache(drift.NewEvaluator(&fakeCRDLister{c: testCrd, name: driftedCrdName}, &fakeMatcher{})),
		)
		if got := resolver(newTestNode("n1")); got != (drift.Result{}) {
			t.Errorf("resolver() = %+v, want an empty result", got)
		}
	})
}
