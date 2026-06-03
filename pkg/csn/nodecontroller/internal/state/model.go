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

package state

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
)

// TrackedNode represents a node tracked by the node state manager.
type TrackedNode struct {
	// Treat this as a read-only pointer.
	Node *v1.Node
	// Buffer can be nil if the node is not assigned to one yet.
	// This should be treated as a read-only pointer.
	Buffer            *v1beta1.CapacityBuffer
	State             csn.NodeState
	DesiredState      csn.NodeState
	PendingOperations ops.OperationType
}

// NodeFilter is an enum which allows for excluding certain nodes
// from the output of `List`.
type NodeFilter string

const (
	WithoutPendingOperationsFilter NodeFilter = "WITHOUT_PENDING_OPERATIONS"
)

// PendingOperationOpt can be added to modify SetPendingOperation calls.
type PendingOperationOpt string

const (
	// ExclusiveOp makes the SetPendingOperation calls succeed only if there
	// are no other operations pending.
	// This only applies if it is attempted to set a pending operation to `true`.
	ExclusiveOp PendingOperationOpt = "EXCLUSIVE_OP"
)

// NodeEvent can be emitted by the NodeStateManager to inform
// interested parties of the observed events.
// Example usage: emitting metrics.
// This can be interpreted as an optional output of the NodeStateManager.
type NodeEvent interface {
	isNodeEvent()
}

type NodeAdded struct {
	Node  *v1.Node
	State csn.NodeState
}

func (NodeAdded) isNodeEvent() {}

type NodeUpdated struct {
	Node     *v1.Node
	OldState csn.NodeState
	NewState csn.NodeState
}

func (NodeUpdated) isNodeEvent() {}

type NodeDeleted struct {
	Node  *v1.Node
	State csn.NodeState
}

func (NodeDeleted) isNodeEvent() {}

type NodeUntracked struct {
	Node *v1.Node
}

func (NodeUntracked) isNodeEvent() {}

// NodeCounts is emitted periodically to describe the count of nodes
// tracked by the NodeStateManager in each state.
type NodeCounts struct {
	Counts map[csn.NodeState]int
}

func (NodeCounts) isNodeEvent() {}

// EventHandler will be called for every event emitted by the NodeStateManager.
// It may be called by several goroutines, it's important to verify that
// the implementation is thread-safe.
// This is effectively an optional output of the NodeStateManager.
type EventHandler func(NodeEvent)

// NodeHandler defines a set of functions which should be called
// for applicable node events.
// Example 1: when a new node is added, onAdd should be called
// Example 2: when a node is updated, onUpdate should be called with the
// updated node.
type NodeHandler struct {
	OnAdd    func(*v1.Node)
	OnUpdate func(*v1.Node)
	OnDelete func(*v1.Node)
}

// RegisterNodeHandler allows for the registration of node handlers.
// Once a node handler is registered, its functions should be called
// when node addition/deletion/update is detected.
// This effectively serves as input to the NodeStateManager.
// Example implementation: kubernetes informer.
type RegisterNodeHandler func(NodeHandler) error

// GetNodeFromEvent returns the node for a NodeEvent
// if the event is related to one specific node.
// The second return value is set to true if such node is found.
// The second return value is set to false if the event doesn't have information
// about just one node.
func GetNodeFromEvent(e NodeEvent) (*v1.Node, bool) {
	switch typed := e.(type) {
	case NodeAdded:
		return typed.Node, true
	case NodeUpdated:
		return typed.Node, true
	case NodeDeleted:
		return typed.Node, true
	case NodeUntracked:
		return typed.Node, true
	}
	return nil, false
}
