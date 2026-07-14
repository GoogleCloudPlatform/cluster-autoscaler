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

package taints

import (
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state"
	"k8s.io/klog/v2"
	"k8s.io/utils/set"
)

const logPrefix = "CSN Taint Tracker:"

// Queue is an interface for enqueueing operations.
type Queue interface {
	Enqueue(op ops.Operation) error
}

// taintInfo stores the relevant taint information for a node.
type taintInfo struct {
	pool       string
	taintCount int
}

// Tracker is responsible for tracking soft taints on CSN nodes.
// It maintains a state of taint distributions per node pool.
type Tracker struct {
	mu sync.RWMutex
	// node pool name -> distribution
	distributions map[string]*distribution
	// nodeName -> info
	nodes map[string]taintInfo
	queue Queue
}

// NewTracker returns a new instance of Tracker.
func NewTracker(queue Queue) *Tracker {
	return &Tracker{
		distributions: make(map[string]*distribution),
		nodes:         make(map[string]taintInfo),
		queue:         queue,
	}
}

// HandleNodeEvent updates the internal state based on node events.
func (t *Tracker) HandleNodeEvent(e state.NodeEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()

	node, ok := state.GetNodeFromEvent(e)
	if !ok {
		return
	}
	if node == nil {
		klog.Errorf("%s received event %T with nil Node", logPrefix, e)
		return
	}

	switch e.(type) {
	case state.NodeAdded, state.NodeUpdated:
		if !csn.IsCSNNode(node) {
			// the node might have been previously tracked.
			// let's remove it if it's no longer a CSN Node.
			t.removeNode(node.Name)
			return
		}
		t.updateNode(node)
	case state.NodeDeleted, state.NodeUntracked:
		t.removeNode(node.Name)
	}
}

// GetTaintCountToAssign returns the target taint count for the node.
// Returns (count, true) if the node is tracked.
// Returns (0, false) if the node is not tracked.
func (t *Tracker) GetTaintCountToAssign(nodeName string) (int, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if info, ok := t.nodes[nodeName]; ok {
		return info.taintCount, true
	}
	return 0, false
}

func (t *Tracker) updateNode(node *v1.Node) {
	pool := node.Labels[labels.GkeNodePoolLabel]
	if pool == "" {
		t.removeNode(node.Name)
		return
	}

	if count, changed := t.computeTargetCount(node, pool); changed {
		// If the node's actual state doesn't match our target, enqueue correction.
		t.enqueueAssignment(node.Name, count)
	}
}

func (t *Tracker) removeNode(nodeName string) {
	info, exists := t.nodes[nodeName]
	if !exists {
		return
	}
	delete(t.nodes, nodeName)
	dist := t.getDistribution(info.pool)
	dist.Unregister(info.taintCount)
	if dist.IsEmpty() {
		delete(t.distributions, info.pool)
	}
}

func (t *Tracker) computeTargetCount(n *v1.Node, poolName string) (int, bool) {
	// If the taints are already tracked, just return them.
	// No need to adjust distributions, they have been taken care of during the
	// first try.
	if oldInfo, exists := t.nodes[n.Name]; exists {
		return oldInfo.taintCount, false
	}
	// Respect previously set values on CA restarts.
	count, countChanged := csn.GetSoftTaintCount(n), false
	dist := t.getDistribution(poolName)
	if dist.IsFull(count) {
		// The previously assigned bucket might be full already.
		// We need to set a new value to balance the counts.
		count = dist.NextAvailable()
		countChanged = true
	}
	dist.Register(count)
	t.nodes[n.Name] = taintInfo{
		pool:       poolName,
		taintCount: count,
	}
	return count, countChanged
}

func (t *Tracker) enqueueAssignment(nodeName string, count int) {
	op := ops.Operation{
		// Not setting a MIG. We don't need to parallelize over MIGs,
		// this op only requires the k8s API.
		// Possible future improvements: allow for operation types with custom
		// batching.
		Type:      ops.AssignSoftTaintOp,
		NodeNames: set.New(nodeName),
	}
	if err := t.queue.Enqueue(op); err != nil {
		klog.Errorf("%s failed to enqueue soft taint assignment for %q (target: %d): %v", logPrefix, nodeName, count, err)
		t.removeNode(nodeName)
	}
}

func (t *Tracker) getDistribution(pool string) *distribution {
	if t.distributions[pool] == nil {
		t.distributions[pool] = newDist()
	}
	return t.distributions[pool]
}
