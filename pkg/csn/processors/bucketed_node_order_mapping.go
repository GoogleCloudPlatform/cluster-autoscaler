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

	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

// bucketedNodeOrderMapping is a clustersnapshot.NodeOrderMapping that makes the scheduler try nodes
// in priority order: nodes of priority 0 first, then priority 1, and so on, and nodes without a
// priority last. Nodes with the same priority keep their order from the snapshot.
//
// This is the order that sorting the nodes by priority for every pod would give, but cheaper: node
// priorities are computed once per pod group, Reset groups the nodes by priority (one bucket per
// priority) in O(n), and the result is reused for the next pods as long as the snapshot lists the
// same nodes in the same order.
type bucketedNodeOrderMapping struct {
	// nodePriorities maps node names to their priority, i.e. the index of the first priority filter
	// that matched the original node. Nodes that aren't in the map have no priority.
	nodePriorities map[string]int
	// numBuckets is the number of priorities. Nodes without a priority go to an extra last bucket.
	numBuckets int
	// order holds indices into the collection passed to Reset, in the order the nodes should be tried.
	order []int
	// names holds the node name at each index of the collection that order was built for.
	names []string
}

func newBucketedNodeOrderMapping(nodePriorities map[string]int, numBuckets int) *bucketedNodeOrderMapping {
	return &bucketedNodeOrderMapping{
		nodePriorities: nodePriorities,
		numBuckets:     numBuckets,
	}
}

// isNodeAcceptable reports whether the node has a priority. The scheduler skips other nodes
// without running the Filter plugins on them.
func (m *bucketedNodeOrderMapping) isNodeAcceptable(ni *framework.NodeInfo) bool {
	_, ok := m.nodePriorities[ni.Node().Name]
	return ok
}

// sameNodeOrder reports whether collection has exactly the same nodes, in the same order, as when order was built.
func (m *bucketedNodeOrderMapping) sameNodeOrder(collection []*framework.NodeInfo) bool {
	if len(collection) != len(m.names) {
		return false
	}
	for i, ni := range collection {
		if ni.Node().Name != m.names[i] {
			return false
		}
	}
	return true
}

// Reset is called by the scheduler with all nodes before it looks for a node for a pod.
// It rebuilds the order only if the nodes in collection changed since the last rebuild.
func (m *bucketedNodeOrderMapping) Reset(collection []*framework.NodeInfo) {
	if m.sameNodeOrder(collection) {
		return
	}

	m.names = make([]string, len(collection))
	buckets := make([][]int, m.numBuckets+1)
	for i, ni := range collection {
		m.names[i] = ni.Node().Name
		b, ok := m.nodePriorities[m.names[i]]
		if !ok || b < 0 || b >= m.numBuckets {
			// Nodes without a priority go last.
			b = m.numBuckets
		}
		buckets[b] = append(buckets[b], i)
	}
	m.order = slices.Concat(buckets...)
}

// At returns the index in collection of the i-th node to try, or -1 if i is out of range.
func (m *bucketedNodeOrderMapping) At(i int) int {
	if i < 0 || i >= len(m.order) {
		return -1
	}
	return m.order[i]
}

// MarkMatch is called by the scheduler after it puts a pod on the node at index idx of collection.
func (m *bucketedNodeOrderMapping) MarkMatch(idx int) {}
