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
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
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

func nodeNames(candidates []simulator.NodeToBeRemoved) []string {
	names := make([]string, 0, len(candidates))
	for _, c := range candidates {
		names = append(names, c.Node.Name)
	}
	return names
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
