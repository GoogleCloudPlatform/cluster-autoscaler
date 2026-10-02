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
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

// bucketedNodeOrderMapping is a clustersnapshot.NodeOrderMapping that makes the scheduler try nodes
// in priority order: nodes of priority 0 first, then priority 1, and so on, and nodes without a
// priority last.
//
// Inside a bucket, nodes are tried round robin: the scan of a bucket starts at the node that got the
// last pod from that bucket, then wraps around to the nodes before it. So the next pod tries the
// previous pod's node first, and the nodes that said no before are tried last, but still before the
// next bucket.
//
// Node priorities are computed once per pod group, Reset groups the nodes by priority (one bucket
// per priority) in O(n), and the buckets are reused for the next pods as long as the snapshot lists
// the same nodes in the same order.
type bucketedNodeOrderMapping struct {
	// nodePriorities maps node names to their priority, i.e. the index of the first priority filter
	// that matched the original node. Nodes that aren't in the map have no priority.
	nodePriorities map[string]int
	// numPriorities is the number of priorities. There is one bucket per priority, plus an extra
	// last bucket for nodes without a priority, so there are numPriorities+1 buckets.
	numPriorities int
	// buckets[b] holds the indices into the collection passed to Reset of the nodes with priority b,
	// in snapshot order. The last bucket holds the nodes without a priority.
	buckets [][]int
	// start[b] is the position in buckets[b] where the scan of bucket b starts.
	start []int
	// pos[i] is the bucket of the node at index i of the collection, and its position in that bucket.
	pos []bucketPos
	// names holds the node name at each index of the collection that buckets were built for.
	names []string
}

// bucketPos is where a node is in the buckets: the index of its bucket, and its position in that bucket.
type bucketPos struct {
	bucket, pos int
}

func newBucketedNodeOrderMapping(nodePriorities map[string]int, numPriorities int) *bucketedNodeOrderMapping {
	return &bucketedNodeOrderMapping{
		nodePriorities: nodePriorities,
		numPriorities:  numPriorities,
	}
}

// isNodeAcceptable reports whether the node has a priority. The scheduler skips other nodes
// without running the Filter plugins on them.
func (m *bucketedNodeOrderMapping) isNodeAcceptable(ni *framework.NodeInfo) bool {
	_, ok := m.nodePriorities[ni.Node().Name]
	return ok
}

// sameNodeOrder reports whether collection has exactly the same nodes, in the same order, as when buckets were built.
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
// It rebuilds the buckets only if the nodes in collection changed since the last rebuild.
// After a rebuild, the scan of every bucket starts at its first node again.
func (m *bucketedNodeOrderMapping) Reset(collection []*framework.NodeInfo) {
	if m.sameNodeOrder(collection) {
		return
	}

	m.names = make([]string, len(collection))
	m.buckets = make([][]int, m.numPriorities+1)
	m.start = make([]int, m.numPriorities+1)
	m.pos = make([]bucketPos, len(collection))
	for i, ni := range collection {
		m.names[i] = ni.Node().Name
		b, ok := m.nodePriorities[m.names[i]]
		if !ok || b < 0 || b >= m.numPriorities {
			// Nodes without a priority go last.
			b = m.numPriorities
		}
		m.pos[i] = bucketPos{bucket: b, pos: len(m.buckets[b])}
		m.buckets[b] = append(m.buckets[b], i)
	}
}

// At returns the index in collection of the i-th node to try, or -1 if i is out of range.
func (m *bucketedNodeOrderMapping) At(i int) int {
	if i < 0 {
		return -1
	}
	for b, bucket := range m.buckets {
		if i < len(bucket) {
			j := (m.start[b] + i) % len(bucket)
			return bucket[j]
		}
		i -= len(bucket)
	}
	return -1
}

// MarkMatch is called by the scheduler after it puts a pod on the node at index idx of collection.
// The next scan of the node's bucket starts at this node.
func (m *bucketedNodeOrderMapping) MarkMatch(idx int) {
	if idx < 0 || idx >= len(m.pos) {
		return
	}
	p := m.pos[idx]
	m.start[p.bucket] = p.pos
}
