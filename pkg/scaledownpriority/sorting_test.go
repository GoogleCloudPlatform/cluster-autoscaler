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

package scaledownpriority

import (
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/scaledowncandidates"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

var _ scaledowncandidates.CandidatesComparer = &Sorting{}

func TestSorting(t *testing.T) {
	sorting := NewSorting(NewTaintParser(testTaintKey, nil), nil, nil)
	plain := taintedNode("plain")
	prio1 := taintedNode("prio1", priorityTaint("1"))
	prio5 := taintedNode("prio5", priorityTaint("5"))
	otherPrio5 := taintedNode("prio5b", priorityTaint("5"))
	invalid := taintedNode("invalid", priorityTaint("high"))

	testCases := []struct {
		name         string
		node1, node2 *apiv1.Node
		want         bool
	}{
		{name: "prioritized precedes unprioritized", node1: prio1, node2: plain, want: true},
		{name: "unprioritized does not precede prioritized", node1: plain, node2: prio1, want: false},
		{name: "higher priority precedes lower priority", node1: prio5, node2: prio1, want: true},
		{name: "lower priority does not precede higher priority", node1: prio1, node2: prio5, want: false},
		{name: "equal priority is a tie", node1: prio5, node2: otherPrio5, want: false},
		{name: "equal priority is a tie, reversed", node1: otherPrio5, node2: prio5, want: false},
		{name: "two unprioritized nodes are a tie", node1: plain, node2: taintedNode("plain2"), want: false},
		{name: "invalid value counts as unprioritized", node1: invalid, node2: plain, want: false},
		{name: "prioritized precedes invalid value", node1: prio1, node2: invalid, want: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, sorting.ScaleDownEarlierThan(tc.node1, tc.node2))
		})
	}
}

func TestSortingAtomicNodeGroups(t *testing.T) {
	cube1Prio5 := taintedNode("cube1-a", priorityTaint("5"))
	cube1Plain := taintedNode("cube1-b")
	cube2Prio1 := taintedNode("cube2-a", priorityTaint("1"))
	cube2Plain := taintedNode("cube2-b")
	nonAtomicPrio5 := taintedNode("cpu-a", priorityTaint("5"))
	nonAtomicPlain := taintedNode("cpu-b")

	provider := testprovider.NewTestCloudProviderBuilder().Build()
	atomicOpts := &config.NodeGroupAutoscalingOptions{ZeroOrMaxNodeScaling: true}
	nonAtomicOpts := &config.NodeGroupAutoscalingOptions{ZeroOrMaxNodeScaling: false}
	provider.AddNodeGroupWithCustomOptions("cube1", 0, 2, 2, atomicOpts)
	provider.AddNodeGroupWithCustomOptions("cube2", 0, 2, 2, atomicOpts)
	provider.AddNodeGroupWithCustomOptions("cpu", 0, 2, 2, nonAtomicOpts)
	for _, n := range []*apiv1.Node{cube1Prio5, cube1Plain} {
		provider.AddNode("cube1", n)
	}
	for _, n := range []*apiv1.Node{cube2Prio1, cube2Plain} {
		provider.AddNode("cube2", n)
	}
	for _, n := range []*apiv1.Node{nonAtomicPrio5, nonAtomicPlain} {
		provider.AddNode("cpu", n)
	}

	snapshot := testsnapshot.NewTestSnapshotOrDie(t)
	for _, n := range []*apiv1.Node{cube1Prio5, cube1Plain, cube2Prio1, cube2Plain, nonAtomicPrio5, nonAtomicPlain} {
		assert.NoError(t, snapshot.AddNodeInfo(framework.NewTestNodeInfo(n)))
	}

	sorting := NewSorting(NewTaintParser(testTaintKey, nil), provider, snapshot)
	sorting.ResetState()

	// Untainted node in cube1 inherits cube1's max priority (5) and precedes
	// tainted node in cube2 (priority 1).
	assert.True(t, sorting.ScaleDownEarlierThan(cube1Plain, cube2Prio1))
	assert.False(t, sorting.ScaleDownEarlierThan(cube2Prio1, cube1Plain))

	// Nodes within the same atomic group tie on priority (both have effective
	// priority 5).
	assert.False(t, sorting.ScaleDownEarlierThan(cube1Prio5, cube1Plain))
	assert.False(t, sorting.ScaleDownEarlierThan(cube1Plain, cube1Prio5))

	// Non-atomic untainted node does not inherit priority from its peer.
	assert.False(t, sorting.ScaleDownEarlierThan(nonAtomicPlain, cube2Prio1))
	assert.True(t, sorting.ScaleDownEarlierThan(cube2Prio1, nonAtomicPlain))
}

// TestSortingResetState checks that ResetState drops the atomic node group
// priorities cached by the previous sort, so that the next sort sees the
// current taints in the snapshot.
func TestSortingResetState(t *testing.T) {
	cube1Prio5 := taintedNode("cube1-a", priorityTaint("5"))
	cube1Plain := taintedNode("cube1-b")
	cube2Prio1 := taintedNode("cube2-a", priorityTaint("1"))
	cube2Plain := taintedNode("cube2-b")

	provider := testprovider.NewTestCloudProviderBuilder().Build()
	atomicOpts := &config.NodeGroupAutoscalingOptions{ZeroOrMaxNodeScaling: true}
	provider.AddNodeGroupWithCustomOptions("cube1", 0, 2, 2, atomicOpts)
	provider.AddNodeGroupWithCustomOptions("cube2", 0, 2, 2, atomicOpts)
	provider.AddNode("cube1", cube1Prio5)
	provider.AddNode("cube1", cube1Plain)
	provider.AddNode("cube2", cube2Prio1)
	provider.AddNode("cube2", cube2Plain)

	snapshot := testsnapshot.NewTestSnapshotOrDie(t)
	for _, n := range []*apiv1.Node{cube1Prio5, cube1Plain, cube2Prio1, cube2Plain} {
		if err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(n)); err != nil {
			t.Fatalf("AddNodeInfo(%s) failed: %v", n.Name, err)
		}
	}
	sorting := NewSorting(NewTaintParser(testTaintKey, nil), provider, snapshot)
	sorting.ResetState()
	// The first sort caches cube1's priority (5) and cube2's priority (1).
	if !sorting.ScaleDownEarlierThan(cube1Plain, cube2Plain) {
		t.Fatal("ScaleDownEarlierThan(cube1-b, cube2-b) = false before the taint is removed, want true")
	}
	// The user removes the priority taint from cube1 before the next sort.
	if err := snapshot.RemoveNodeInfo(t.Context(), cube1Prio5.Name); err != nil {
		t.Fatalf("RemoveNodeInfo(%s) failed: %v", cube1Prio5.Name, err)
	}
	if err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(taintedNode(cube1Prio5.Name))); err != nil {
		t.Fatalf("AddNodeInfo(%s) failed: %v", cube1Prio5.Name, err)
	}

	sorting.ResetState()

	assert.True(t, sorting.ScaleDownEarlierThan(cube2Plain, cube1Plain), "ScaleDownEarlierThan(cube2-b, cube1-b) after ResetState()")
}

func TestSortingDisabled(t *testing.T) {
	prio5 := taintedNode("prio5", priorityTaint("5"))
	plain := taintedNode("plain")
	for name, sorting := range map[string]*Sorting{
		"nil parser": NewSorting(nil, nil, nil),
		"empty key":  NewSorting(NewTaintParser("", nil), nil, nil),
	} {
		t.Run(name, func(t *testing.T) {
			assert.False(t, sorting.ScaleDownEarlierThan(prio5, plain))
			assert.False(t, sorting.ScaleDownEarlierThan(plain, prio5))
		})
	}
}
