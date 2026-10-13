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
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodes"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"

	gke_labels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	cc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
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

func setupAtomicMinCapacityCtx(t *testing.T, groups []testNodeGroup) *ca_context.AutoscalingContext {
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

func TestAtomicMinCapacityProcessor_FilterUnremovableNodes(t *testing.T) {
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
			name: "non-atomic nodes pass through untouched",
			groups: []testNodeGroup{
				{id: "ng1", atomic: false, nodes: []*apiv1.Node{buildCCNode("ng1-a", testCC), buildCCNode("ng1-b", testCC)}},
			},
			crds:            []crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(5)))},
			experimentsMgr:  experiments.NewMockManager(),
			wantRemovable:   []string{"ng1-a", "ng1-b"},
			wantUnremovable: nil,
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
			wantRemovable:   []string{"ng1-a", "ng1-b", "ng1-c"},
			wantUnremovable: nil,
		},
		{
			name: "non-atomic defrag candidate is kept while the floor needs it",
			groups: []testNodeGroup{
				{id: "ng1", atomic: false, nodes: []*apiv1.Node{buildCCNode("ng1-a", testCC), buildCCNode("ng1-b", testCC), buildDefragCandidateNode("ng1-c")}},
			},
			crds:            []crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(3)))},
			experimentsMgr:  experiments.NewMockManager(),
			wantRemovable:   []string{"ng1-a", "ng1-b"},
			wantUnremovable: []string{"ng1-c"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := setupAtomicMinCapacityCtx(t, tc.groups)
			ccLister := cc_lister.NewMockCrdLister(tc.crds)
			processor := NewAtomicMinCapacityProcessor(ccLister, tc.experimentsMgr)

			removable, unremovable := processor.FilterUnremovableNodes(context.Background(), ctx, nodes.NewDefaultScaleDownContext(), candidatesFromGroups(tc.groups))

			assert.ElementsMatch(t, tc.wantRemovable, nodeNames(removable))
			assert.ElementsMatch(t, tc.wantUnremovable, unremovableNames(unremovable))
		})
	}
}

func TestAtomicMinCapacityProcessor_TrimScaleDownCandidates(t *testing.T) {
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
	ctx := setupAtomicMinCapacityCtx(t, groups)
	ccLister := cc_lister.NewMockCrdLister([]crd.CRD{
		crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(2))),
		crd.NewTestCrd(crd.WithName(otherCC), crd.WithTargetNodeCount(ptrInt(1))),
	})
	processor := NewAtomicMinCapacityProcessor(ccLister, experiments.NewMockManager())

	got := processor.TrimScaleDownCandidates(context.Background(), ctx, []*apiv1.Node{groups[0].nodes[2], blockedNode, groups[2].nodes[0]})

	assert.Equal(t, []*apiv1.Node{groups[0].nodes[2], groups[2].nodes[0]}, got)
}

// TestAtomicMinCapacityProcessor_AccountsForNodesInDeletion verifies that nodes
// whose deletion is already in flight are not double-counted toward the current
// node count. Without this accounting, a slice that is already leaving would be
// counted as present, letting the processor also release a second slice and
// briefly drop the ComputeClass below its targetNodeCount.
func TestAtomicMinCapacityProcessor_AccountsForNodesInDeletion(t *testing.T) {
	// Two atomic groups of two nodes each under one CC, target 2.
	groups := []testNodeGroup{
		{id: "ng1", atomic: true, nodes: []*apiv1.Node{buildCCNode("ng1-a", testCC), buildCCNode("ng1-b", testCC)}},
		{id: "ng2", atomic: true, nodes: []*apiv1.Node{buildCCNode("ng2-a", testCC), buildCCNode("ng2-b", testCC)}},
	}
	ctx := setupAtomicMinCapacityCtx(t, groups)
	ccLister := cc_lister.NewMockCrdLister([]crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(2)))})
	processor := NewAtomicMinCapacityProcessor(ccLister, experiments.NewMockManager())

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

// TestAtomicMinCapacityProcessor_DefragCandidatesDoNotCountTowardCurrent pins
// that a node carrying the defrag HardTaint is not counted as current capacity
// even when it is not being deleted yet (see beingDeleted), so it can hold
// back an unrelated atomic group of the same ComputeClass.
func TestAtomicMinCapacityProcessor_DefragCandidatesDoNotCountTowardCurrent(t *testing.T) {
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
			ctx := setupAtomicMinCapacityCtx(t, groups)
			ccLister := cc_lister.NewMockCrdLister([]crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(2)))})
			processor := NewAtomicMinCapacityProcessor(ccLister, experiments.NewMockManager())

			// Only the atomic group ng1 is offered; ng2-a is a defrag candidate but not being deleted.
			ng1Candidates := []simulator.NodeToBeRemoved{{Node: groups[0].nodes[0]}, {Node: groups[0].nodes[1]}}
			removable, unremovable := processor.FilterUnremovableNodes(context.Background(), ctx, nodes.NewDefaultScaleDownContext(), ng1Candidates)

			assert.ElementsMatch(t, tc.wantRemovable, nodeNames(removable))
			assert.ElementsMatch(t, tc.wantUnremovable, unremovableNames(unremovable))
		})
	}
}

// TestAtomicMinCapacityProcessor_UpcomingNodesShouldNotCountTowardCurrent is a
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
func TestAtomicMinCapacityProcessor_UpcomingNodesShouldNotCountTowardCurrent(t *testing.T) {
	groups := []testNodeGroup{
		{id: "ng1", atomic: true, nodes: []*apiv1.Node{buildCCNode("ng1-a", testCC), buildCCNode("ng1-b", testCC)}},
		{id: "ng2", atomic: true, nodes: []*apiv1.Node{buildUpcomingCCNode("ng2-a", testCC), buildUpcomingCCNode("ng2-b", testCC)}},
	}
	ctx := setupAtomicMinCapacityCtx(t, groups)
	ccLister := cc_lister.NewMockCrdLister([]crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(2)))})
	processor := NewAtomicMinCapacityProcessor(ccLister, experiments.NewMockManager())

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

func TestAtomicMinCapacityProcessor_FailsClosedOnListerError(t *testing.T) {
	groups := []testNodeGroup{
		{id: "ng-atomic-cc", atomic: true, nodes: []*apiv1.Node{buildCCNode("atomic-cc-1", testCC), buildCCNode("atomic-cc-2", testCC)}},
		{id: "ng-regular-cc", atomic: false, nodes: []*apiv1.Node{buildCCNode("regular-cc-1", testCC)}},
		{id: "ng-non-cc", atomic: true, nodes: []*apiv1.Node{buildCCNode("non-cc-1", "")}},
	}
	ctx := setupAtomicMinCapacityCtx(t, groups)
	processor := NewAtomicMinCapacityProcessor(&erroringCrdLister{cc_lister.NewMockCrdLister(nil)}, experiments.NewMockManager())

	removable, unremovable := processor.FilterUnremovableNodes(context.Background(), ctx, nodes.NewDefaultScaleDownContext(), candidatesFromGroups(groups))

	assert.ElementsMatch(t, []string{"regular-cc-1", "non-cc-1"}, nodeNames(removable))
	assert.ElementsMatch(t, []string{"atomic-cc-1", "atomic-cc-2"}, unremovableNames(unremovable))
	for _, u := range unremovable {
		assert.Equal(t, simulator.UnexpectedError, u.Reason)
	}
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

func TestAtomicMinCapacityProcessor_ChecksNodesBeingDeletedOnGetOptionsError(t *testing.T) {
	groups := []testNodeGroup{
		{id: "ng1", nodes: []*apiv1.Node{buildCCNode("ng1-a", testCC), buildDefragCandidateNode("ng1-c")}},
	}
	ctx := setupAtomicMinCapacityCtx(t, groups)
	ctx.CloudProvider = erroringOptionsProvider{ctx.CloudProvider}
	ccLister := cc_lister.NewMockCrdLister([]crd.CRD{crd.NewTestCrd(crd.WithName(testCC), crd.WithTargetNodeCount(ptrInt(2)))})
	processor := NewAtomicMinCapacityProcessor(ccLister, experiments.NewMockManager())

	removable, unremovable := processor.FilterUnremovableNodes(context.Background(), ctx, nodes.NewDefaultScaleDownContext(), candidatesFromGroups(groups))

	// ng1-a is left to the per-node quota; ng1-c, already being deleted, is
	// still checked: the ComputeClass needs it for its min capacity.
	assert.ElementsMatch(t, []string{"ng1-a"}, nodeNames(removable))
	assert.ElementsMatch(t, []string{"ng1-c"}, unremovableNames(unremovable))
}
