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

package scaledown

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodes"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"

	gke_labels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	cc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

const testCC = "my-ccc"

func ptrInt(v int) *int { return &v }

func buildCCNode(name, cc string) *apiv1.Node {
	node := test.BuildTestNode(name, 1000, 1000)
	if node.Labels == nil {
		node.Labels = map[string]string{}
	}
	if cc != "" {
		node.Labels[gke_labels.ComputeClassLabel] = cc
	}
	return node
}

// buildUpcomingCCNode builds a ComputeClass node annotated as an "upcoming" node,
// mirroring the placeholder nodes the static autoscaler injects into the
// ClusterSnapshot for capacity that has not yet materialized.
func buildUpcomingCCNode(name, cc string) *apiv1.Node {
	node := buildCCNode(name, cc)
	if node.Annotations == nil {
		node.Annotations = map[string]string{}
	}
	node.Annotations[annotations.NodeUpcomingAnnotation] = "true"
	return node
}

func buildDefragCandidateNode(name string) *apiv1.Node {
	node := buildCCNode(name, testCC)
	node.Spec.Taints = append(node.Spec.Taints, apiv1.Taint{Key: defrag.HardTaint, Effect: apiv1.TaintEffectNoSchedule})
	return node
}

type testNodeGroup struct {
	id     string
	atomic bool
	nodes  []*apiv1.Node
}

func setupMinCapacityCtx(t *testing.T, groups []testNodeGroup) *ca_context.AutoscalingContext {
	t.Helper()
	provider := testprovider.NewTestCloudProviderBuilder().Build()
	snapshot := testsnapshot.NewTestSnapshotOrDie(t)
	for _, g := range groups {
		var opts *config.NodeGroupAutoscalingOptions
		if g.atomic {
			opts = &config.NodeGroupAutoscalingOptions{ZeroOrMaxNodeScaling: true}
		}
		provider.AddNodeGroupWithCustomOptions(g.id, 0, 100, len(g.nodes), opts)
		for _, n := range g.nodes {
			provider.AddNode(g.id, n)
			assert.NoError(t, snapshot.AddNodeInfo(framework.NewNodeInfo(n, nil)))
		}
	}
	return &ca_context.AutoscalingContext{
		CloudProvider:   provider,
		ClusterSnapshot: snapshot,
	}
}

func candidatesFromGroups(groups []testNodeGroup) []simulator.NodeToBeRemoved {
	var candidates []simulator.NodeToBeRemoved
	for _, g := range groups {
		for _, n := range g.nodes {
			candidates = append(candidates, simulator.NodeToBeRemoved{Node: n})
		}
	}
	return candidates
}

func unremovableNames(unremovable []simulator.UnremovableNode) []string {
	names := make([]string, 0, len(unremovable))
	for _, u := range unremovable {
		names = append(names, u.Node.Name)
	}
	return names
}

func TestComputeClassMinCapacityProcessor_FilterUnremovableNodes(t *testing.T) {
	// Two atomic node groups of two nodes each, both belonging to the same CC.
	twoAtomicGroups := func() []testNodeGroup {
		return []testNodeGroup{
			{id: "ng1", atomic: true, nodes: []*apiv1.Node{buildCCNode("ng1-a", testCC), buildCCNode("ng1-b", testCC)}},
			{id: "ng2", atomic: true, nodes: []*apiv1.Node{buildCCNode("ng2-a", testCC), buildCCNode("ng2-b", testCC)}},
		}
	}

	testCases := []struct {
		name            string
		groups          []testNodeGroup
		crds            []crd.CRD
		experimentsMgr  experiments.Manager
		wantRemovable   []string
		wantUnremovable []string
	}{
		{
			name:            "removes one whole atomic group to respect target",
			groups:          twoAtomicGroups(),
			crds:            []crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(2)))},
			experimentsMgr:  experiments.NewMockManager(),
			wantRemovable:   []string{"ng1-a", "ng1-b"},
			wantUnremovable: []string{"ng2-a", "ng2-b"},
		},
		{
			name:            "target zero removes all atomic groups",
			groups:          twoAtomicGroups(),
			crds:            []crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(0)))},
			experimentsMgr:  experiments.NewMockManager(),
			wantRemovable:   []string{"ng1-a", "ng1-b", "ng2-a", "ng2-b"},
			wantUnremovable: nil,
		},
		{
			name: "non-atomic nodes are kept while the ComputeClass is below its min capacity",
			groups: []testNodeGroup{
				{id: "ng1", atomic: false, nodes: []*apiv1.Node{buildCCNode("ng1-a", testCC), buildCCNode("ng1-b", testCC)}},
			},
			crds:            []crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(5)))},
			experimentsMgr:  experiments.NewMockManager(),
			wantRemovable:   nil,
			wantUnremovable: []string{"ng1-a", "ng1-b"},
		},
		{
			name: "non-atomic nodes without ComputeClass label pass through",
			groups: []testNodeGroup{
				{id: "ng1", atomic: false, nodes: []*apiv1.Node{buildCCNode("ng1-a", ""), buildCCNode("ng1-b", "")}},
			},
			crds:            []crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(5)))},
			experimentsMgr:  experiments.NewMockManager(),
			wantRemovable:   []string{"ng1-a", "ng1-b"},
			wantUnremovable: nil,
		},
		{
			name: "atomic nodes without ComputeClass label pass through",
			groups: []testNodeGroup{
				{id: "ng1", atomic: true, nodes: []*apiv1.Node{buildCCNode("ng1-a", ""), buildCCNode("ng1-b", "")}},
			},
			crds:            []crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(5)))},
			experimentsMgr:  experiments.NewMockManager(),
			wantRemovable:   []string{"ng1-a", "ng1-b"},
			wantUnremovable: nil,
		},
		{
			name: "rule-level min capacity is enforced for non-atomic nodes",
			groups: []testNodeGroup{
				{id: "ng1", atomic: false, nodes: []*apiv1.Node{withRuleIndex(buildCCNode("ng1-a", testCC), 0), withRuleIndex(buildCCNode("ng1-b", testCC), 0)}},
				{id: "ng2", atomic: false, nodes: []*apiv1.Node{withRuleIndex(buildCCNode("ng2-a", testCC), 1), withRuleIndex(buildCCNode("ng2-b", testCC), 1)}},
			},
			crds: []crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithRules([]rules.Rule{
				rules.NewRule(rules.WithTargetNodeCountRule(ptrInt(2))),
				rules.NewRule(),
			}))},
			experimentsMgr:  experiments.NewMockManager(),
			wantRemovable:   []string{"ng2-a", "ng2-b"},
			wantUnremovable: []string{"ng1-a", "ng1-b"},
		},
		{
			name: "spec-level and rule-level min capacities each keep the nodes they need",
			groups: []testNodeGroup{
				{id: "ng1", atomic: false, nodes: []*apiv1.Node{withRuleIndex(buildCCNode("ng1-a", testCC), 0), withRuleIndex(buildCCNode("ng1-b", testCC), 0)}},
				{id: "ng2", atomic: false, nodes: []*apiv1.Node{withRuleIndex(buildCCNode("ng2-a", testCC), 1), withRuleIndex(buildCCNode("ng2-b", testCC), 1)}},
			},
			crds: []crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(3)), crd.WithRules([]rules.Rule{
				rules.NewRule(rules.WithTargetNodeCountRule(ptrInt(2))),
				rules.NewRule(),
			}))},
			experimentsMgr:  experiments.NewMockManager(),
			wantRemovable:   []string{"ng2-a"},
			wantUnremovable: []string{"ng1-a", "ng1-b", "ng2-b"},
		},
		{
			name:           "experiment disabled passes all through",
			groups:         twoAtomicGroups(),
			crds:           []crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(2)))},
			experimentsMgr: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{experiments.ComputeClassMinCapacityEnabledFlag: false}, map[string]string{}),
			wantRemovable:  []string{"ng1-a", "ng1-b", "ng2-a", "ng2-b"},
		},
		{
			name:           "no target passes all through",
			groups:         twoAtomicGroups(),
			crds:           []crd.CRD{crd.NewTestCrd(crd.WithName(testCC))},
			experimentsMgr: experiments.NewMockManager(),
			wantRemovable:  []string{"ng1-a", "ng1-b", "ng2-a", "ng2-b"},
		},
		{
			name: "defrag candidate group is released instead of a group the floor needs",
			groups: append(twoAtomicGroups(),
				testNodeGroup{id: "ng3", atomic: true, nodes: []*apiv1.Node{buildDefragCandidateNode("ng3-a"), buildDefragCandidateNode("ng3-b")}}),
			crds:            []crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(4)))},
			experimentsMgr:  experiments.NewMockManager(),
			wantRemovable:   []string{"ng3-a", "ng3-b"},
			wantUnremovable: []string{"ng1-a", "ng1-b", "ng2-a", "ng2-b"},
		},
		{
			name: "defrag candidate group is kept while the floor needs it",
			groups: []testNodeGroup{
				{id: "ng1", atomic: true, nodes: []*apiv1.Node{buildCCNode("ng1-a", testCC), buildCCNode("ng1-b", testCC)}},
				{id: "ng2", atomic: true, nodes: []*apiv1.Node{buildDefragCandidateNode("ng2-a"), buildDefragCandidateNode("ng2-b")}},
			},
			crds:            []crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(4)))},
			experimentsMgr:  experiments.NewMockManager(),
			wantUnremovable: []string{"ng1-a", "ng1-b", "ng2-a", "ng2-b"},
		},
		{
			name: "non-atomic defrag candidate is released once the floor is met without it",
			groups: []testNodeGroup{
				{id: "ng1", atomic: false, nodes: []*apiv1.Node{buildCCNode("ng1-a", testCC), buildCCNode("ng1-b", testCC), buildDefragCandidateNode("ng1-c")}},
			},
			crds:            []crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(2)))},
			experimentsMgr:  experiments.NewMockManager(),
			wantRemovable:   []string{"ng1-c"},
			wantUnremovable: []string{"ng1-a", "ng1-b"},
		},
		{
			name: "non-atomic defrag candidate is kept while the floor needs it",
			groups: []testNodeGroup{
				{id: "ng1", atomic: false, nodes: []*apiv1.Node{buildCCNode("ng1-a", testCC), buildCCNode("ng1-b", testCC), buildDefragCandidateNode("ng1-c")}},
			},
			crds:            []crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(3)))},
			experimentsMgr:  experiments.NewMockManager(),
			wantRemovable:   nil,
			wantUnremovable: []string{"ng1-a", "ng1-b", "ng1-c"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := setupMinCapacityCtx(t, tc.groups)
			ccLister := cc_lister.NewMockCrdLister(tc.crds)
			processor := NewComputeClassMinCapacityProcessor(ccLister, tc.experimentsMgr, nil)

			removable, unremovable := processor.FilterUnremovableNodes(context.Background(), ctx, nodes.NewDefaultScaleDownContext(), candidatesFromGroups(tc.groups))

			assert.ElementsMatch(t, tc.wantRemovable, nodeNames(removable))
			assert.ElementsMatch(t, tc.wantUnremovable, unremovableNames(unremovable))
		})
	}
}

func withRuleIndex(node *apiv1.Node, idx int) *apiv1.Node {
	if node.Annotations == nil {
		node.Annotations = map[string]string{}
	}
	node.Annotations[gke_labels.CCCPriorityIndexAnnotationKey] = strconv.Itoa(idx)
	return node
}

// TestComputeClassMinCapacityProcessor_DecidesInCandidateOrder checks that
// groups that tie, here of the same size and with no pods to reschedule, are
// decided on in the order of the candidates, not by node name or node group ID,
// so that the scale-down candidate order chooses the nodes released above the
// min capacity.
func TestComputeClassMinCapacityProcessor_DecidesInCandidateOrder(t *testing.T) {
	groups := []testNodeGroup{
		{id: "ng-b", atomic: true, nodes: []*apiv1.Node{buildCCNode("ng-b-a", testCC), buildCCNode("ng-b-b", testCC)}},
		{id: "ng-plain", atomic: false, nodes: []*apiv1.Node{buildCCNode("node-z", testCC), buildCCNode("node-y", testCC)}},
		{id: "ng-a", atomic: true, nodes: []*apiv1.Node{buildCCNode("ng-a-a", testCC), buildCCNode("ng-a-b", testCC)}},
	}
	autoscalingCtx := setupMinCapacityCtx(t, groups)
	// 6 nodes and a min capacity of 3: the first atomic group given, ng-b, and
	// the first single node given, node-z, go.
	ccLister := cc_lister.NewMockCrdLister([]crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(3)))})
	processor := NewComputeClassMinCapacityProcessor(ccLister, experiments.NewMockManager(), nil)

	removable, unremovable := processor.FilterUnremovableNodes(t.Context(), autoscalingCtx, nodes.NewDefaultScaleDownContext(), candidatesFromGroups(groups))

	assert.Equal(t, []string{"ng-b-a", "ng-b-b", "node-z"}, nodeNames(removable), "FilterUnremovableNodes() removable nodes")
	assert.Equal(t, []string{"ng-a-a", "ng-a-b", "node-y"}, unremovableNames(unremovable), "FilterUnremovableNodes() unremovable nodes")
}

// TestComputeClassMinCapacityProcessor_DecidesOnLargerGroupsFirst checks that
// single nodes, which AtomicResizeFilteringProcessor passes before atomic
// groups, don't use up the surplus that a whole atomic group could free.
func TestComputeClassMinCapacityProcessor_DecidesOnLargerGroupsFirst(t *testing.T) {
	singles := buildCCNodes("single", 4)
	sliceA := buildCCNodes("slice-a", 16)
	sliceB := buildCCNodes("slice-b", 16)
	groups := []testNodeGroup{
		{id: "ng-singles", atomic: false, nodes: singles},
		{id: "ng-slice-a", atomic: true, nodes: sliceA},
		{id: "ng-slice-b", atomic: true, nodes: sliceB},
	}
	autoscalingCtx := setupMinCapacityCtx(t, groups)
	// 36 nodes and a min capacity of 20: the surplus of 16 is one whole slice.
	ccLister := cc_lister.NewMockCrdLister([]crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(20)))})
	processor := NewComputeClassMinCapacityProcessor(ccLister, experiments.NewMockManager(), nil)

	removable, unremovable := processor.FilterUnremovableNodes(t.Context(), autoscalingCtx, nodes.NewDefaultScaleDownContext(), candidatesFromGroups(groups))

	assert.Equal(t, candidateNodeNames(sliceA), nodeNames(removable), "FilterUnremovableNodes() removable nodes")
	assert.ElementsMatch(t, candidateNodeNames(slices.Concat(singles, sliceB)), unremovableNames(unremovable), "FilterUnremovableNodes() unremovable nodes")
}

// TestComputeClassMinCapacityProcessor_DecidesOnEmptyGroupsFirst checks that
// the surplus goes to a group with no pods to reschedule before a larger group
// that has some.
func TestComputeClassMinCapacityProcessor_DecidesOnEmptyGroupsFirst(t *testing.T) {
	busySlice := buildCCNodes("busy", 3)
	emptySlice := buildCCNodes("empty", 2)
	groups := []testNodeGroup{
		{id: "ng-busy", atomic: true, nodes: busySlice},
		{id: "ng-empty", atomic: true, nodes: emptySlice},
	}
	autoscalingCtx := setupMinCapacityCtx(t, groups)
	candidates := candidatesFromGroups(groups)
	candidates[0].PodsToReschedule = []*apiv1.Pod{test.BuildTestPod("workload", 100, 100)}
	// 5 nodes and a min capacity of 2: the surplus of 3 fits either slice, but
	// not both.
	ccLister := cc_lister.NewMockCrdLister([]crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(2)))})
	processor := NewComputeClassMinCapacityProcessor(ccLister, experiments.NewMockManager(), nil)

	removable, unremovable := processor.FilterUnremovableNodes(t.Context(), autoscalingCtx, nodes.NewDefaultScaleDownContext(), candidates)

	assert.Equal(t, candidateNodeNames(emptySlice), nodeNames(removable), "FilterUnremovableNodes() removable nodes")
	assert.Equal(t, candidateNodeNames(busySlice), unremovableNames(unremovable), "FilterUnremovableNodes() unremovable nodes")
}

func buildCCNodes(prefix string, count int) []*apiv1.Node {
	ccNodes := make([]*apiv1.Node, 0, count)
	for i := range count {
		ccNodes = append(ccNodes, buildCCNode(fmt.Sprintf("%s-%d", prefix, i), testCC))
	}
	return ccNodes
}

// TestComputeClassMinCapacityProcessor_GroupsOnlyComputeClassNodes checks that nodes
// without a ComputeClass label, atomic or not, and nodes of a predefined GKE
// compute class are not grouped, so they are not decided on or logged as
// released.
func TestComputeClassMinCapacityProcessor_GroupsOnlyComputeClassNodes(t *testing.T) {
	groups := []testNodeGroup{
		{id: "ng-cc", atomic: true, nodes: []*apiv1.Node{buildCCNode("cc-a", testCC), buildCCNode("cc-b", testCC)}},
		{id: "ng-non-cc", atomic: true, nodes: []*apiv1.Node{buildCCNode("non-cc-a", ""), buildCCNode("non-cc-b", "")}},
		{id: "ng-regular-non-cc", atomic: false, nodes: []*apiv1.Node{buildCCNode("regular-non-cc", "")}},
		{id: "ng-predefined", atomic: false, nodes: []*apiv1.Node{buildCCNode("balanced", machinetypes.BalancedClass.Name())}},
	}
	autoscalingCtx := setupMinCapacityCtx(t, groups)
	processor := NewComputeClassMinCapacityProcessor(cc_lister.NewMockCrdLister(nil), experiments.NewMockManager(), nil)

	gotGroups := processor.groupCandidates(t.Context(), autoscalingCtx, candidatesFromGroups(groups))

	var gotGroupNames [][]string
	for _, group := range gotGroups {
		gotGroupNames = append(gotGroupNames, nodeNames(group))
	}
	assert.Equal(t, [][]string{{"cc-a", "cc-b"}}, gotGroupNames, "groupCandidates() groups")
}

// TestComputeClassMinCapacityProcessor_KeepsCandidateOrder checks that the removable
// candidates keep their input order, ComputeClass nodes or not, so the
// scale-down budget sees the order the candidates were sorted in.
func TestComputeClassMinCapacityProcessor_KeepsCandidateOrder(t *testing.T) {
	groups := []testNodeGroup{
		{id: "ng1", nodes: []*apiv1.Node{buildCCNode("cc-1", testCC)}},
		{id: "ng-plain", nodes: []*apiv1.Node{buildCCNode("plain", "")}},
		{id: "ng2", nodes: []*apiv1.Node{buildCCNode("cc-2", testCC), buildCCNode("cc-3", testCC)}},
	}
	autoscalingCtx := setupMinCapacityCtx(t, groups)
	// 3 ComputeClass nodes and a min capacity of 1: cc-1 and cc-2 go, cc-3 stays.
	ccLister := cc_lister.NewMockCrdLister([]crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(1)))})
	processor := NewComputeClassMinCapacityProcessor(ccLister, experiments.NewMockManager(), nil)

	removable, unremovable := processor.FilterUnremovableNodes(t.Context(), autoscalingCtx, nodes.NewDefaultScaleDownContext(), candidatesFromGroups(groups))

	assert.Equal(t, []string{"cc-1", "plain", "cc-2"}, nodeNames(removable), "FilterUnremovableNodes() removable nodes")
	assert.Equal(t, []string{"cc-3"}, unremovableNames(unremovable), "FilterUnremovableNodes() unremovable nodes")
}

func TestComputeClassMinCapacityProcessor_TrimScaleDownCandidates(t *testing.T) {
	const otherCC = "other-ccc"
	blockedNode := buildCCNode("ng2-a", otherCC)
	blockedNode.Spec.Taints = append(blockedNode.Spec.Taints, apiv1.Taint{Key: defrag.HardTaint, Effect: apiv1.TaintEffectNoSchedule})
	groups := []testNodeGroup{
		// testCC keeps its min capacity of 2 without the defrag candidate ng1-c.
		{id: "ng1", nodes: []*apiv1.Node{buildCCNode("ng1-a", testCC), buildCCNode("ng1-b", testCC), buildDefragCandidateNode("ng1-c")}},
		// otherCC has no other node to keep its min capacity of 1.
		{id: "ng2", nodes: []*apiv1.Node{blockedNode}},
		{id: "ng3", nodes: []*apiv1.Node{buildCCNode("ng3-a", "")}},
	}
	ctx := setupMinCapacityCtx(t, groups)
	ccLister := cc_lister.NewMockCrdLister([]crd.CRD{
		crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(2))),
		crd.NewTestCrd(crd.WithName(otherCC), crd.WithTargetNodeCount(ptrInt(1))),
	})
	processor := NewComputeClassMinCapacityProcessor(ccLister, experiments.NewMockManager(), nil)

	got := processor.TrimScaleDownCandidates(context.Background(), ctx, []*apiv1.Node{groups[0].nodes[2], blockedNode, groups[2].nodes[0]})

	assert.Equal(t, []*apiv1.Node{groups[0].nodes[2], groups[2].nodes[0]}, got)
}

// TestComputeClassMinCapacityProcessor_AccountsForNodesInDeletion verifies that nodes
// whose deletion is already in flight are not double-counted toward the current
// node count. Without this accounting, a slice that is already leaving would be
// counted as present, letting the processor also release a second slice and
// briefly drop the ComputeClass below its targetNodeCount.
func TestComputeClassMinCapacityProcessor_AccountsForNodesInDeletion(t *testing.T) {
	// Two atomic groups of two nodes each under one CC, target 2.
	groups := []testNodeGroup{
		{id: "ng1", atomic: true, nodes: []*apiv1.Node{buildCCNode("ng1-a", testCC), buildCCNode("ng1-b", testCC)}},
		{id: "ng2", atomic: true, nodes: []*apiv1.Node{buildCCNode("ng2-a", testCC), buildCCNode("ng2-b", testCC)}},
	}
	ctx := setupMinCapacityCtx(t, groups)
	ccLister := cc_lister.NewMockCrdLister([]crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(2)))})
	processor := NewComputeClassMinCapacityProcessor(ccLister, experiments.NewMockManager(), nil)

	// ng1 (2 nodes) is already being deleted; only ng2 is offered as a candidate.
	scaleDownCtx := nodes.NewDefaultScaleDownContext()
	scaleDownCtx.ActuationStatus = &fakeActuationStatus{drainedNodesList: []string{"ng1-a", "ng1-b"}}

	ng2Candidates := []simulator.NodeToBeRemoved{
		{Node: groups[1].nodes[0]},
		{Node: groups[1].nodes[1]},
	}

	removable, unremovable := processor.FilterUnremovableNodes(context.Background(), ctx, scaleDownCtx, ng2Candidates)

	// current = 4 snapshot nodes - 2 in deletion = 2, equal to target. Removing
	// ng2 would drop to 0, so ng2 must be kept.
	assert.Empty(t, nodeNames(removable))
	assert.ElementsMatch(t, []string{"ng2-a", "ng2-b"}, unremovableNames(unremovable))
}

// TestComputeClassMinCapacityProcessor_TrimScaleDownCandidatesAccountsForNodesInDeletion
// checks that defrag's check, which has no scale-down context, still skips
// nodes whose deletion the scale-down actuator has started.
func TestComputeClassMinCapacityProcessor_TrimScaleDownCandidatesAccountsForNodesInDeletion(t *testing.T) {
	groups := []testNodeGroup{
		{id: "ng1", atomic: true, nodes: []*apiv1.Node{buildCCNode("ng1-a", testCC), buildCCNode("ng1-b", testCC)}},
		{id: "ng2", atomic: true, nodes: []*apiv1.Node{buildCCNode("ng2-a", testCC), buildCCNode("ng2-b", testCC)}},
	}
	autoscalingCtx := setupMinCapacityCtx(t, groups)
	autoscalingCtx.ScaleDownActuator = fakeActuator{status: &fakeActuationStatus{drainedNodesList: []string{"ng1-a", "ng1-b"}}}
	ccLister := cc_lister.NewMockCrdLister([]crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(2)))})
	processor := NewComputeClassMinCapacityProcessor(ccLister, experiments.NewMockManager(), nil)

	got := processor.TrimScaleDownCandidates(t.Context(), autoscalingCtx, groups[1].nodes)

	// ng1 is being deleted, so ng2 alone holds the min capacity of 2.
	assert.Empty(t, got, "TrimScaleDownCandidates(ng2) with ng1 in deletion")
}

// fakeActuator is a scaledown.Actuator that only reports the given status.
type fakeActuator struct {
	scaledown.Actuator
	status scaledown.ActuationStatus
}

func (a fakeActuator) CheckStatus() scaledown.ActuationStatus {
	return a.status
}

// TestComputeClassMinCapacityProcessor_DefragCandidatesDoNotCountTowardCurrent pins
// that a node carrying the defrag HardTaint is not counted as current capacity
// even when it is not being deleted yet (see
// computeclass.CountsTowardMinCapacityScaleDown), so it can hold back an
// unrelated atomic group of the same ComputeClass.
func TestComputeClassMinCapacityProcessor_DefragCandidatesDoNotCountTowardCurrent(t *testing.T) {
	for _, tc := range []struct {
		name            string
		defragCandidate bool
		wantRemovable   []string
		wantUnremovable []string
	}{
		{
			// current = 4 >= target 2 after removing ng1: ng1 can go.
			name:          "no defrag candidate",
			wantRemovable: []string{"ng1-a", "ng1-b"},
		},
		{
			// current = 4 - 1 defrag candidate = 3; removing ng1 would leave 1 < 2.
			name:            "defrag candidate in the same ComputeClass",
			defragCandidate: true,
			wantUnremovable: []string{"ng1-a", "ng1-b"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ng2a := buildCCNode("ng2-a", testCC)
			if tc.defragCandidate {
				ng2a = buildDefragCandidateNode("ng2-a")
			}
			groups := []testNodeGroup{
				{id: "ng1", atomic: true, nodes: []*apiv1.Node{buildCCNode("ng1-a", testCC), buildCCNode("ng1-b", testCC)}},
				{id: "ng2", atomic: false, nodes: []*apiv1.Node{ng2a, buildCCNode("ng2-b", testCC)}},
			}
			ctx := setupMinCapacityCtx(t, groups)
			ccLister := cc_lister.NewMockCrdLister([]crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(2)))})
			processor := NewComputeClassMinCapacityProcessor(ccLister, experiments.NewMockManager(), nil)

			// Only the atomic group ng1 is offered; ng2-a is a defrag candidate but not being deleted.
			ng1Candidates := []simulator.NodeToBeRemoved{{Node: groups[0].nodes[0]}, {Node: groups[0].nodes[1]}}
			removable, unremovable := processor.FilterUnremovableNodes(context.Background(), ctx, nodes.NewDefaultScaleDownContext(), ng1Candidates)

			assert.ElementsMatch(t, tc.wantRemovable, nodeNames(removable))
			assert.ElementsMatch(t, tc.wantUnremovable, unremovableNames(unremovable))
		})
	}
}

// TestComputeClassMinCapacityProcessor_UpcomingNodesShouldNotCountTowardCurrent is a
// characterization test for the min-capacity accounting edge affecting
// PROVISION_ONLY / dynamic-slicing multi-host TPU slices.
//
// During RunOnce the static autoscaler injects "upcoming" placeholder nodes into
// the ClusterSnapshot (annotated cluster-autoscaler.k8s.io/upcoming-node=true)
// before scale-down runs, while scale-down *candidates* are taken from the real
// registered nodes only (allNodes, which excludes upcoming). buildConstraints
// seeds `current` from ClusterSnapshot.ListNodeInfos(), so without filtering it
// would count those upcoming placeholders as if they were committed, ready
// capacity.
//
// Scenario: one CC with targetNodeCount=2. Slice ng1 has 2 real ready nodes and
// is offered as a scale-down candidate. Slice ng2 has 2 *upcoming* nodes (not yet
// materialized, e.g. a provision-only slice still coming up under stockout) that
// are present in the snapshot but are NOT scale-down candidates.
//
// Safe behavior: ng1 must be kept, because the only committed-ready capacity for
// the CC is ng1's 2 nodes; removing them drops ready capacity to 0 < target 2
// while relying on ng2, which may never materialize. This mirrors the OSS
// AtomicResizeFilteringProcessor, which also excludes upcoming nodes.
func TestComputeClassMinCapacityProcessor_UpcomingNodesShouldNotCountTowardCurrent(t *testing.T) {
	groups := []testNodeGroup{
		{id: "ng1", atomic: true, nodes: []*apiv1.Node{buildCCNode("ng1-a", testCC), buildCCNode("ng1-b", testCC)}},
		{id: "ng2", atomic: true, nodes: []*apiv1.Node{buildUpcomingCCNode("ng2-a", testCC), buildUpcomingCCNode("ng2-b", testCC)}},
	}
	ctx := setupMinCapacityCtx(t, groups)
	ccLister := cc_lister.NewMockCrdLister([]crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(2)))})
	processor := NewComputeClassMinCapacityProcessor(ccLister, experiments.NewMockManager(), nil)

	// Only ng1 (real, ready) is a scale-down candidate. ng2 is upcoming and thus
	// not offered as a candidate, matching the real RunOnce flow.
	ng1Candidates := []simulator.NodeToBeRemoved{
		{Node: groups[0].nodes[0]},
		{Node: groups[0].nodes[1]},
	}

	removable, unremovable := processor.FilterUnremovableNodes(context.Background(), ctx, nodes.NewDefaultScaleDownContext(), ng1Candidates)

	// Upcoming ng2 nodes must not count toward current, so current = 2 (ng1 only);
	// removing ng1 would drop to 0 < target 2, so ng1 must be kept.
	assert.Empty(t, nodeNames(removable), "ng1 must be kept; upcoming slice ng2 must not count as committed capacity")
	assert.ElementsMatch(t, []string{"ng1-a", "ng1-b"}, unremovableNames(unremovable))
}

// nodeNameFilter is a resourcequotas.NodeFilter that excludes the nodes with
// the given names.
type nodeNameFilter map[string]bool

func (f nodeNameFilter) ExcludeFromTracking(node *apiv1.Node) bool {
	return f[node.Name]
}

func TestComputeClassMinCapacityProcessor_SkipsNodesExcludedByNodeFilter(t *testing.T) {
	// old-a and old-b are being upgraded; surge-c is the surge node that the
	// upgrade added and that the node filter excludes, like the min quotas
	// tracker does.
	testCases := []struct {
		name string
		// clusterNodes are the names of the ComputeClass nodes, all in one
		// non-atomic node group.
		clusterNodes    []string
		minCapacity     int
		nodeFilter      nodeNameFilter
		candidates      []string
		wantRemovable   []string
		wantUnremovable []string
	}{
		{
			name:          "without a node filter the surge node counts",
			clusterNodes:  []string{"old-a", "old-b", "surge-c"},
			minCapacity:   2,
			candidates:    []string{"old-a"},
			wantRemovable: []string{"old-a"},
		},
		{
			name:            "an excluded surge node does not count",
			clusterNodes:    []string{"old-a", "old-b", "surge-c"},
			minCapacity:     2,
			nodeFilter:      nodeNameFilter{"surge-c": true},
			candidates:      []string{"old-a"},
			wantUnremovable: []string{"old-a"},
		},
		{
			name:          "an excluded surge node can go at the min capacity",
			clusterNodes:  []string{"old-a", "old-b", "surge-c"},
			minCapacity:   2,
			nodeFilter:    nodeNameFilter{"surge-c": true},
			candidates:    []string{"surge-c"},
			wantRemovable: []string{"surge-c"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var clusterNodes []*apiv1.Node
			for _, name := range tc.clusterNodes {
				clusterNodes = append(clusterNodes, buildCCNode(name, testCC))
			}
			groups := []testNodeGroup{{id: "ng1", nodes: clusterNodes}}
			autoscalingCtx := setupMinCapacityCtx(t, groups)
			ccLister := cc_lister.NewMockCrdLister([]crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(tc.minCapacity)))})
			processor := NewComputeClassMinCapacityProcessor(ccLister, experiments.NewMockManager(), tc.nodeFilter)
			var candidates []simulator.NodeToBeRemoved
			for _, node := range clusterNodes {
				if slices.Contains(tc.candidates, node.Name) {
					candidates = append(candidates, simulator.NodeToBeRemoved{Node: node})
				}
			}

			removable, unremovable := processor.FilterUnremovableNodes(t.Context(), autoscalingCtx, nodes.NewDefaultScaleDownContext(), candidates)

			assert.ElementsMatch(t, tc.wantRemovable, nodeNames(removable), "FilterUnremovableNodes() removable nodes")
			assert.ElementsMatch(t, tc.wantUnremovable, unremovableNames(unremovable), "FilterUnremovableNodes() unremovable nodes")
		})
	}
}

// fakeActuationStatus is a minimal ActuationStatus stub for tests.
type fakeActuationStatus struct {
	emptyNodesList         []string
	drainedNodesList       []string
	deletionsCountsByGroup map[string]int
	evictedPods            []*apiv1.Pod
}

func (f *fakeActuationStatus) DeletionsInProgress() (empty, drained []string) {
	return f.emptyNodesList, f.drainedNodesList
}

func (f *fakeActuationStatus) DeletionsCount(nodeGroupId string) int {
	return f.deletionsCountsByGroup[nodeGroupId]
}

func (f *fakeActuationStatus) RecentEvictions() (pods []*apiv1.Pod) {
	return f.evictedPods
}

type erroringCrdLister struct {
	*cc_lister.MockCrdLister
}

func (e *erroringCrdLister) ListCrds() ([]crd.CRD, error) {
	return nil, errors.New("simulated CRD lister error")
}

func TestComputeClassMinCapacityProcessor_FailsClosedOnListerError(t *testing.T) {
	groups := []testNodeGroup{
		{id: "ng-atomic-cc", atomic: true, nodes: []*apiv1.Node{buildCCNode("atomic-cc-1", testCC), buildCCNode("atomic-cc-2", testCC)}},
		{id: "ng-regular-cc", atomic: false, nodes: []*apiv1.Node{buildCCNode("regular-cc-1", testCC)}},
		{id: "ng-non-cc", atomic: true, nodes: []*apiv1.Node{buildCCNode("non-cc-1", "")}},
		{id: "ng-regular-non-cc", atomic: false, nodes: []*apiv1.Node{buildCCNode("regular-non-cc-1", "")}},
	}
	ctx := setupMinCapacityCtx(t, groups)
	processor := NewComputeClassMinCapacityProcessor(&erroringCrdLister{cc_lister.NewMockCrdLister(nil)}, experiments.NewMockManager(), nil)

	removable, unremovable := processor.FilterUnremovableNodes(context.Background(), ctx, nodes.NewDefaultScaleDownContext(), candidatesFromGroups(groups))

	assert.ElementsMatch(t, []string{"non-cc-1", "regular-non-cc-1"}, nodeNames(removable))
	assert.ElementsMatch(t, []string{"atomic-cc-1", "atomic-cc-2", "regular-cc-1"}, unremovableNames(unremovable))
	for _, u := range unremovable {
		assert.Equal(t, simulator.UnexpectedError, u.Reason)
	}
}

// erroringNodeGroupProvider fails to return the node group of any node.
type erroringNodeGroupProvider struct {
	cloudprovider.CloudProvider
}

func (erroringNodeGroupProvider) NodeGroupForNode(context.Context, *apiv1.Node) (cloudprovider.NodeGroup, error) {
	return nil, errors.New("simulated NodeGroupForNode error")
}

func TestComputeClassMinCapacityProcessor_ChecksNodesOnNodeGroupForNodeError(t *testing.T) {
	groups := []testNodeGroup{
		{id: "ng1", atomic: true, nodes: []*apiv1.Node{buildCCNode("ng1-a", testCC), buildCCNode("ng1-b", testCC), buildCCNode("plain", "")}},
	}
	autoscalingCtx := setupMinCapacityCtx(t, groups)
	autoscalingCtx.CloudProvider = erroringNodeGroupProvider{autoscalingCtx.CloudProvider}
	ccLister := cc_lister.NewMockCrdLister([]crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(2)))})
	processor := NewComputeClassMinCapacityProcessor(ccLister, experiments.NewMockManager(), nil)

	removable, unremovable := processor.FilterUnremovableNodes(t.Context(), autoscalingCtx, nodes.NewDefaultScaleDownContext(), candidatesFromGroups(groups))

	// The ComputeClass nodes are checked one by one and the ComputeClass needs
	// both for its min capacity. The node without a ComputeClass passes through.
	assert.ElementsMatch(t, []string{"plain"}, nodeNames(removable), "FilterUnremovableNodes() removable nodes")
	assert.ElementsMatch(t, []string{"ng1-a", "ng1-b"}, unremovableNames(unremovable), "FilterUnremovableNodes() unremovable nodes")
}

// erroringOptionsProvider returns node groups whose GetOptions fails.
type erroringOptionsProvider struct {
	cloudprovider.CloudProvider
}

func (p erroringOptionsProvider) NodeGroupForNode(ctx context.Context, node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	nodeGroup, err := p.CloudProvider.NodeGroupForNode(ctx, node)
	if err != nil || nodeGroup == nil {
		return nodeGroup, err
	}
	return erroringOptionsNodeGroup{nodeGroup}, nil
}

type erroringOptionsNodeGroup struct {
	cloudprovider.NodeGroup
}

func (erroringOptionsNodeGroup) GetOptions(context.Context, config.NodeGroupAutoscalingOptions) (*config.NodeGroupAutoscalingOptions, error) {
	return nil, errors.New("simulated GetOptions error")
}

func TestComputeClassMinCapacityProcessor_ChecksNodesOnGetOptionsError(t *testing.T) {
	groups := []testNodeGroup{
		{id: "ng1", nodes: []*apiv1.Node{buildCCNode("ng1-a", testCC), buildDefragCandidateNode("ng1-c")}},
	}
	ctx := setupMinCapacityCtx(t, groups)
	ctx.CloudProvider = erroringOptionsProvider{ctx.CloudProvider}
	ccLister := cc_lister.NewMockCrdLister([]crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(2)))})
	processor := NewComputeClassMinCapacityProcessor(ccLister, experiments.NewMockManager(), nil)

	removable, unremovable := processor.FilterUnremovableNodes(context.Background(), ctx, nodes.NewDefaultScaleDownContext(), candidatesFromGroups(groups))

	// Both nodes are treated as non-atomic ComputeClass nodes and checked: the
	// ComputeClass needs ng1-a and the defrag candidate ng1-c for its min
	// capacity.
	assert.Empty(t, nodeNames(removable))
	assert.ElementsMatch(t, []string{"ng1-a", "ng1-c"}, unremovableNames(unremovable))
}
