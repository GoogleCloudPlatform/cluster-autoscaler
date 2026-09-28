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

package processors

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

// TestBucketedNodeOrderMapping checks the order in which the mapping visits nodes,
// before and after pods get scheduled on some of them. The scheduler picks the
// first node in this order that fits the pod.
func TestBucketedNodeOrderMapping(t *testing.T) {
	// Three priority buckets, like buffer consumption uses for normal pods:
	// 0 = already consumed, 1 = chilling, 2 = suspended. Bucket 0 is visited first.
	// Nodes without a priority (here: non-CSN nodes) go to an extra last bucket,
	// which is visited last.
	const numBuckets = 3
	nodePriorities := map[string]int{
		"consumed-1":  0,
		"consumed-2":  0,
		"chilling-1":  1,
		"chilling-2":  1,
		"suspended-1": 2,
		"suspended-2": 2,
		"suspended-3": 2,
	}
	// Nodes are mixed in the snapshot on purpose, to show that the mapping groups them by bucket.
	snapshotOrder := []string{"non-csn-1", "suspended-1", "chilling-1", "consumed-1", "suspended-2", "chilling-2", "non-csn-2", "consumed-2", "suspended-3"}
	// Bucket by bucket, in snapshot order inside each bucket.
	// Written one bucket per line: 0, 1, 2, then nodes without a priority.
	wantOrder := []string{
		"consumed-1", "consumed-2",
		"chilling-1", "chilling-2",
		"suspended-1", "suspended-2", "suspended-3",
		"non-csn-1", "non-csn-2",
	}

	testCases := []struct {
		description string
		// matches are the nodes that pods got scheduled on, in this order. For each of them
		// the test calls MarkMatch, like the scheduler does after it places a pod on a node.
		matches []string
	}{
		{
			description: "Without matches, nodes are grouped by bucket and keep snapshot order",
		},
		{
			// Every pod sees the same order, like when the nodes were sorted for every pod.
			description: "Matches don't change the order",
			matches:     []string{"suspended-3", "chilling-2", "consumed-1", "suspended-3"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			nodeInfos := buildTestNodeInfos(snapshotOrder...)
			mapping := newBucketedNodeOrderMapping(nodePriorities, numBuckets)
			mapping.Reset(nodeInfos)

			for _, name := range tc.matches {
				mapping.MarkMatch(testNodeIndex(t, nodeInfos, name))
			}

			assert.Equal(t, wantOrder, scanOrder(t, mapping, nodeInfos))
		})
	}
}

// TestBucketedNodeOrderMappingReset checks the order after Reset is called again with a new list of nodes.
func TestBucketedNodeOrderMappingReset(t *testing.T) {
	const numBuckets = 2 // 0 = chilling, 1 = suspended.
	// chilling-2 isn't in the first snapshot, one of the cases adds it.
	nodePriorities := map[string]int{"chilling-1": 0, "chilling-2": 0, "suspended-1": 1, "suspended-2": 1}
	snapshotOrder := []string{"suspended-1", "chilling-1", "suspended-2"}

	testCases := []struct {
		description      string
		newSnapshotOrder []string
		wantOrder        []string
	}{
		{
			description:      "Same node order keeps the order",
			newSnapshotOrder: []string{"suspended-1", "chilling-1", "suspended-2"},
			wantOrder:        []string{"chilling-1", "suspended-1", "suspended-2"},
		},
		{
			description:      "Different node order rebuilds the order",
			newSnapshotOrder: []string{"chilling-1", "suspended-2", "suspended-1"},
			wantOrder:        []string{"chilling-1", "suspended-2", "suspended-1"},
		},
		{
			description:      "Added node rebuilds the order",
			newSnapshotOrder: []string{"suspended-1", "chilling-1", "suspended-2", "non-csn-1"},
			wantOrder:        []string{"chilling-1", "suspended-1", "suspended-2", "non-csn-1"},
		},
		{
			// chilling-2 is in bucket 0 like chilling-1, and comes before it in the snapshot.
			description:      "Added chilling node is tried first",
			newSnapshotOrder: []string{"chilling-2", "suspended-1", "chilling-1", "suspended-2"},
			wantOrder:        []string{"chilling-2", "chilling-1", "suspended-1", "suspended-2"},
		},
		{
			description:      "Removed node rebuilds the order",
			newSnapshotOrder: []string{"suspended-1", "suspended-2"},
			wantOrder:        []string{"suspended-1", "suspended-2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			mapping := newBucketedNodeOrderMapping(nodePriorities, numBuckets)
			mapping.Reset(buildTestNodeInfos(snapshotOrder...))

			newNodeInfos := buildTestNodeInfos(tc.newSnapshotOrder...)
			mapping.Reset(newNodeInfos)

			assert.Equal(t, tc.wantOrder, scanOrder(t, mapping, newNodeInfos))
		})
	}
}

func TestBucketedNodeOrderMappingIsNodeAcceptable(t *testing.T) {
	mapping := newBucketedNodeOrderMapping(map[string]int{"chilling-1": 0, "suspended-1": 1}, 2)

	// isNodeAcceptable only looks at nodePriorities, so it works before the first Reset.
	for _, ni := range buildTestNodeInfos("chilling-1", "suspended-1") {
		assert.True(t, mapping.isNodeAcceptable(ni), ni.Node().Name)
	}
	for _, ni := range buildTestNodeInfos("consumed-1", "non-csn-1") {
		assert.False(t, mapping.isNodeAcceptable(ni), ni.Node().Name)
	}
}

// buildTestNodeInfos returns NodeInfos of bare nodes with the given names, in the given order.
func buildTestNodeInfos(names ...string) []*framework.NodeInfo {
	nodeInfos := make([]*framework.NodeInfo, 0, len(names))
	for _, name := range names {
		nodeInfos = append(nodeInfos, framework.NewNodeInfo(&apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}, nil))
	}
	return nodeInfos
}

// testNodeIndex returns the index of the named node in nodeInfos.
func testNodeIndex(t *testing.T, nodeInfos []*framework.NodeInfo, name string) int {
	t.Helper()
	idx := slices.IndexFunc(nodeInfos, func(ni *framework.NodeInfo) bool { return ni.Node().Name == name })
	if idx < 0 {
		t.Fatalf("node %q not found", name)
	}
	return idx
}

// scanOrder returns the names of the nodes in the order the mapping visits them.
func scanOrder(t *testing.T, m *bucketedNodeOrderMapping, nodeInfos []*framework.NodeInfo) []string {
	t.Helper()
	order := make([]string, 0, len(nodeInfos))
	for i := range nodeInfos {
		idx := m.At(i)
		if idx < 0 || idx >= len(nodeInfos) {
			t.Fatalf("At(%d) = %d, want an index in [0, %d)", i, idx, len(nodeInfos))
		}
		order = append(order, nodeInfos[idx].Node().Name)
	}
	assert.Equal(t, -1, m.At(-1), "At(-1)")
	assert.Equal(t, -1, m.At(len(nodeInfos)), "At(len(nodeInfos))")
	return order
}
