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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/test"
	clock "k8s.io/utils/clock/testing"
	"k8s.io/utils/set"
)

const stopTrackingDelay = 10 * time.Minute

func TestNodeStateManager_Run(t *testing.T) {
	tests := []struct {
		name        string
		register    RegisterNodeHandler
		expectError bool
	}{
		{
			name: "successfully runs",
			register: func(_ NodeHandler) error {
				return nil
			},
			expectError: false,
		},
		{
			name: "fails to run",
			register: func(_ NodeHandler) error {
				return fmt.Errorf("error")
			},
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := NewNodeStateManager(tc.register)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			err := m.Run(ctx)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGet(t *testing.T) {
	suspendedNode := test.CreateNode("suspended-node", test.StateOpt(csn.NodeStateSuspended))
	chillingNode := test.CreateNode("chilling-node", test.StateOpt(csn.NodeStateChilling))

	testCases := []struct {
		name      string
		nodeToAdd *v1.Node
		run       bool

		nodeNameToGet string
		expectFound   bool
	}{
		{
			name:          "suspended_node_can_be_retrieved",
			nodeToAdd:     suspendedNode.DeepCopy(),
			run:           true,
			nodeNameToGet: suspendedNode.Name,
			expectFound:   true,
		},
		{
			name:          "chilling_node_can_be_retrieved",
			nodeToAdd:     chillingNode.DeepCopy(),
			run:           true,
			nodeNameToGet: chillingNode.Name,
			expectFound:   true,
		},
		{
			name:          "non_csn_node_is_not_found",
			nodeToAdd:     test.CreateNode("non-csn-node"),
			run:           true,
			nodeNameToGet: "non-csn-node",
		},
		{
			name:          "node_not_found_when_name_is_different",
			nodeToAdd:     chillingNode.DeepCopy(),
			run:           true,
			nodeNameToGet: chillingNode.Name + "xyz",
		},
		{
			name:          "node_not_found_when_manager_not_running",
			nodeToAdd:     chillingNode.DeepCopy(),
			nodeNameToGet: chillingNode.Name,
		},
		{
			name:          "node_not_found_if_not_added",
			nodeNameToGet: chillingNode.Name,
			run:           true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fs := NewFakeNodeSource()
			m := NewNodeStateManager(fs.RegisterNodeHandler)

			if tc.run {
				mustRunManager(t, m)
			}

			if tc.nodeToAdd != nil {
				fs.AddNodes(tc.nodeToAdd)
			}

			tn, ok := m.Get(tc.nodeNameToGet)
			assert.Equal(t, tc.expectFound, ok)

			if !ok {
				assert.Equal(t, TrackedNode{}, tn)
				return
			}

			assert.Equal(t, tc.nodeToAdd, tn.Node)
			assert.Equal(t, csn.ClassifyNode(tc.nodeToAdd), tn.State)
		})
	}
}

func TestSequentialGet(t *testing.T) {
	fs := NewFakeNodeSource()
	fakeClock := clock.NewFakeClock(time.Now())
	finalEventChan := make(chan bool, 1)
	m := NewNodeStateManager(
		fs.RegisterNodeHandler,
		WithClock(fakeClock),
		// untracking a node is delayed, this allows us to wait for it
		// to happen deterministically.
		WithEventHandler(func(n NodeEvent) {
			if _, ok := n.(NodeUntracked); ok {
				finalEventChan <- true
			}
		}),
		WithStopTrackingDelay(stopTrackingDelay),
	)

	mustRunManager(t, m)

	node := test.CreateNode("test-node")

	// node shouldn't exist, it hasn't been added
	got, ok := m.Get(node.Name)
	assert.False(t, ok)
	assert.Equal(t, TrackedNode{}, got)

	fs.AddNodes(node.DeepCopy())
	// Get should not return the added node, it's not a CSN node.
	got, ok = m.Get(node.Name)
	assert.False(t, ok)
	assert.Equal(t, TrackedNode{}, got)

	chillingNode, err := csn.SetNodeAs(node.DeepCopy(), csn.NodeStateChilling)
	assert.NoError(t, err)

	fs.UpdateNodes(chillingNode.DeepCopy())
	// Get should return the node, it became a CSN node.
	got, ok = m.Get(node.Name)
	assert.True(t, ok)
	assert.Equal(t, chillingNode, got.Node)
	assert.Equal(t, csn.NodeStateChilling, got.State)

	// Modifying the output state should not modify internal state
	got.DesiredState = csn.NodeStateSuspended
	got, ok = m.Get(node.Name)
	assert.True(t, ok)
	assert.Equal(t, csn.NodeStateChilling, got.State)

	suspendedNode, err := csn.SetNodeAs(node.DeepCopy(), csn.NodeStateSuspended)
	assert.NoError(t, err)

	fs.UpdateNodes(suspendedNode.DeepCopy())
	// Get should return the updated node
	got, ok = m.Get(node.Name)
	assert.True(t, ok)
	assert.Equal(t, suspendedNode, got.Node)
	assert.Equal(t, csn.NodeStateSuspended, got.State)

	consumedNode, err := csn.SetNodeAs(suspendedNode.DeepCopy(), csn.NodeStateConsumed)
	assert.NoError(t, err)

	fs.UpdateNodes(consumedNode.DeepCopy())
	// Get should return a consumed node at first
	got, ok = m.Get(node.Name)
	assert.True(t, ok)
	assert.Equal(t, consumedNode, got.Node)
	assert.Equal(t, csn.NodeStateConsumed, got.State)

	// Updates for consumed nodes should be ignored
	fs.UpdateNodes(suspendedNode.DeepCopy())
	got, ok = m.Get(node.Name)
	assert.True(t, ok)
	assert.Equal(t, consumedNode, got.Node)
	assert.Equal(t, csn.NodeStateConsumed, got.State)

	// Consumed node should disappear after a delay.
	fakeClock.Step(stopTrackingDelay * 2)

	<-finalEventChan
	got, ok = m.Get(node.Name)
	assert.False(t, ok)
	assert.Equal(t, TrackedNode{}, got)
}

func TestPeriodicJanitor(t *testing.T) {
	fs := NewFakeNodeSource()
	fakeClock := clock.NewFakeClock(time.Now())
	eventChan := make(chan NodeEvent, 10)
	m := NewNodeStateManager(
		fs.RegisterNodeHandler,
		WithClock(fakeClock),
		WithEventHandler(func(n NodeEvent) {
			if _, ok := n.(NodeUntracked); ok {
				eventChan <- n
			}
		}),
		WithStopTrackingDelay(stopTrackingDelay),
	)

	mustRunManager(t, m)

	// Add and then "consume" node1
	node1 := test.CreateNode("node1", test.StateOpt(csn.NodeStateSuspended))
	fs.AddNodes(node1)
	consumed1, _ := csn.SetNodeAs(node1.DeepCopy(), csn.NodeStateConsumed)
	fs.UpdateNodes(consumed1)

	// Advance clock to trigger first janitor run
	fakeClock.Step(stopTrackingDelay + time.Second)
	assert.IsType(t, NodeUntracked{}, <-eventChan)

	// Add and then "consume" node2 AFTER the first janitor run
	node2 := test.CreateNode("node2", test.StateOpt(csn.NodeStateSuspended))
	fs.AddNodes(node2)
	consumed2, _ := csn.SetNodeAs(node2.DeepCopy(), csn.NodeStateConsumed)
	fs.UpdateNodes(consumed2)

	// Advance clock again. It makes sure that the janitor ticker fires periodically.
	fakeClock.Step(stopTrackingDelay + time.Second)
	assert.IsType(t, NodeUntracked{}, <-eventChan)
}

func TestList(t *testing.T) {
	suspendedNode := test.CreateNode("suspended-node", test.StateOpt(csn.NodeStateSuspended))
	chillingNode := test.CreateNode("chilling-node", test.StateOpt(csn.NodeStateChilling))

	chillingToSuspended, err := csn.SetNodeAs(chillingNode.DeepCopy(), csn.NodeStateSuspended)
	assert.NoError(t, err)

	testCases := []struct {
		name          string
		setup         func(*FakeNodeSource)
		expectedNodes []TrackedNode
	}{
		{
			name:  "empty_list",
			setup: func(fs *FakeNodeSource) {},
		},
		{
			name: "single_node",
			setup: func(fs *FakeNodeSource) {
				fs.AddNodes(suspendedNode)
			},
			expectedNodes: []TrackedNode{
				{
					Node:  suspendedNode,
					State: csn.NodeStateSuspended,
				},
			},
		},
		{
			name: "multiple_nodes",
			setup: func(fs *FakeNodeSource) {
				fs.AddNodes(suspendedNode, chillingNode)
			},
			expectedNodes: []TrackedNode{
				{
					Node:  suspendedNode,
					State: csn.NodeStateSuspended,
				},
				{
					Node:  chillingNode,
					State: csn.NodeStateChilling,
				},
			},
		},
		{
			name: "non_csn_nodes_ignored",
			setup: func(fs *FakeNodeSource) {
				fs.AddNodes(suspendedNode, test.CreateNode("non-csn"))
			},
			expectedNodes: []TrackedNode{
				{
					Node:  suspendedNode,
					State: csn.NodeStateSuspended,
				},
			},
		},
		{
			name: "duplicate_add_updates_existing",
			setup: func(fs *FakeNodeSource) {
				fs.AddNodes(suspendedNode)
				fs.AddNodes(suspendedNode)
			},
			expectedNodes: []TrackedNode{
				{
					Node:  suspendedNode,
					State: csn.NodeStateSuspended,
				},
			},
		},
		{
			name: "update_node",
			setup: func(fs *FakeNodeSource) {
				fs.AddNodes(chillingNode)
				fs.UpdateNodes(chillingToSuspended)
			},
			expectedNodes: []TrackedNode{
				{
					Node:  chillingToSuspended,
					State: csn.NodeStateSuspended,
				},
			},
		},
		{
			name: "delete_node",
			setup: func(fs *FakeNodeSource) {
				fs.AddNodes(suspendedNode, chillingNode)
				fs.DeleteNodes(suspendedNode)
			},
			expectedNodes: []TrackedNode{
				{
					Node:  chillingNode,
					State: csn.NodeStateChilling,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fs := NewFakeNodeSource()
			m := NewNodeStateManager(fs.RegisterNodeHandler)
			mustRunManager(t, m)

			tc.setup(fs)

			got := m.List()
			assert.ElementsMatch(t, tc.expectedNodes, got)
		})
	}
}

func TestList_Copy(t *testing.T) {
	fs := NewFakeNodeSource()
	m := NewNodeStateManager(fs.RegisterNodeHandler)
	mustRunManager(t, m)

	node := test.CreateNode("test-node", test.StateOpt(csn.NodeStateSuspended))
	fs.AddNodes(node)

	got := m.List()
	if len(got) != 1 {
		t.Fatalf("Expected at list one TrackedNode in List output")
	}
	// Modify the returned node
	got[0].State = "modified-state"

	// List again
	got2 := m.List()
	assert.Len(t, got2, 1)

	// Verify original is untouched
	assert.Equal(t, node, got2[0].Node)
	assert.Equal(t, csn.NodeStateSuspended, got2[0].State)
}

func TestList_WithFilters(t *testing.T) {
	suspendedNode := test.CreateNode("suspended-node", test.StateOpt(csn.NodeStateSuspended))
	chillingNode := test.CreateNode("chilling-node", test.StateOpt(csn.NodeStateChilling))
	consumedNode := test.CreateNode("node-to-be-consumed", test.StateOpt(csn.NodeStateChilling))
	bufferAssignNode := test.CreateNode("node-with-buffer-to-assign", test.StateOpt(csn.NodeStateChilling))

	fs := NewFakeNodeSource()
	m := NewNodeStateManager(fs.RegisterNodeHandler)
	mustRunManager(t, m)

	fs.AddNodes(suspendedNode, chillingNode, consumedNode, bufferAssignNode)

	// Mark some nodes as having a pending operation
	assert.Empty(t, m.SetPendingOperation(ops.SuspendOp, true, set.New(suspendedNode.Name)))
	assert.Empty(t, m.SetPendingOperation(ops.ConsumeOp, true, set.New(consumedNode.Name)))
	assert.Empty(t, m.SetPendingOperation(ops.AssignBufferOp, true, set.New(bufferAssignNode.Name)))

	testCases := []struct {
		name          string
		filters       []NodeFilter
		expectedNodes []TrackedNode
	}{
		{
			name:    "no_filters",
			filters: nil,
			expectedNodes: []TrackedNode{
				{
					Node:              suspendedNode,
					State:             csn.NodeStateSuspended,
					PendingOperations: ops.SuspendOp,
					DesiredState:      csn.NodeStateSuspended,
				},
				{
					Node:              chillingNode,
					State:             csn.NodeStateChilling,
					PendingOperations: ops.NoOp,
				},
				{
					Node:              consumedNode,
					State:             csn.NodeStateChilling,
					PendingOperations: ops.ConsumeOp,
					DesiredState:      csn.NodeStateConsumed,
				},
				{
					Node:              bufferAssignNode,
					State:             csn.NodeStateChilling,
					PendingOperations: ops.AssignBufferOp,
				},
			},
		},
		{
			name:    "without_pending_operations",
			filters: []NodeFilter{WithoutPendingOperationsFilter},
			expectedNodes: []TrackedNode{
				{
					Node:              chillingNode,
					State:             csn.NodeStateChilling,
					PendingOperations: ops.NoOp,
				},
				{
					Node:              bufferAssignNode,
					State:             csn.NodeStateChilling,
					PendingOperations: ops.AssignBufferOp,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := m.List(tc.filters...)
			assert.ElementsMatch(t, tc.expectedNodes, got)
		})
	}
}

func TestSetPendingOperation(t *testing.T) {
	node := test.CreateNode("test-node", test.StateOpt(csn.NodeStateChilling))

	testCases := []struct {
		name                 string
		addNode              bool
		initialPending       ops.OperationType
		nodesToUpdate        []string
		pendingToSet         bool
		opToSet              ops.OperationType
		opts                 []PendingOperationOpt
		expectErrorForNodes  []string
		expectedPending      ops.OperationType
		expectedDesiredState csn.NodeState
	}{
		{
			name:                 "set_pending_suspend_true_for_existing_node",
			addNode:              true,
			nodesToUpdate:        []string{node.Name},
			pendingToSet:         true,
			opToSet:              ops.SuspendOp,
			expectedPending:      ops.SuspendOp,
			expectedDesiredState: csn.NodeStateSuspended,
		},
		{
			name:                 "set_pending_consume_true_for_existing_node",
			addNode:              true,
			nodesToUpdate:        []string{node.Name},
			pendingToSet:         true,
			opToSet:              ops.ConsumeOp,
			expectedPending:      ops.ConsumeOp,
			expectedDesiredState: csn.NodeStateConsumed,
		},
		{
			name:                 "set_pending_false_after_true",
			addNode:              true,
			initialPending:       ops.SuspendOp,
			nodesToUpdate:        []string{node.Name},
			pendingToSet:         false,
			opToSet:              ops.SuspendOp,
			expectedPending:      ops.NoOp,
			expectedDesiredState: csn.NodeStateSuspended,
		},
		{
			name:                "set_pending_for_non_existent_node",
			addNode:             false,
			nodesToUpdate:       []string{"non-existent"},
			pendingToSet:        true,
			opToSet:             ops.SuspendOp,
			expectErrorForNodes: []string{"non-existent"},
		},
		{
			name:                 "mixed_existing_and_non_existent",
			addNode:              true,
			nodesToUpdate:        []string{node.Name, "non-existent-1", "non-existent-2"},
			pendingToSet:         true,
			opToSet:              ops.SuspendOp,
			expectErrorForNodes:  []string{"non-existent-1", "non-existent-2"},
			expectedPending:      ops.SuspendOp,
			expectedDesiredState: csn.NodeStateSuspended,
		},
		{
			name:                 "exclusive_pending_fails",
			addNode:              true,
			initialPending:       ops.SuspendOp,
			nodesToUpdate:        []string{node.Name},
			pendingToSet:         true,
			opToSet:              ops.ConsumeOp,
			expectedPending:      ops.SuspendOp,
			expectedDesiredState: csn.NodeStateSuspended,
			opts:                 []PendingOperationOpt{ExclusiveOp},
			expectErrorForNodes:  []string{node.Name},
		},
		{
			name:                 "exclusive_pending_succeeds",
			addNode:              true,
			nodesToUpdate:        []string{node.Name},
			pendingToSet:         true,
			opToSet:              ops.SuspendOp,
			expectedPending:      ops.SuspendOp,
			expectedDesiredState: csn.NodeStateSuspended,
			opts:                 []PendingOperationOpt{ExclusiveOp},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fs := NewFakeNodeSource()
			m := NewNodeStateManager(fs.RegisterNodeHandler)
			mustRunManager(t, m)

			if tc.addNode {
				fs.AddNodes(node)
			}

			if tc.addNode && tc.initialPending != ops.NoOp {
				assert.Empty(t, m.SetPendingOperation(tc.initialPending, true, set.New(node.Name)))
			}

			err := m.SetPendingOperation(tc.opToSet, tc.pendingToSet, set.New(tc.nodesToUpdate...), tc.opts...)
			assert.Len(t, err, len(tc.expectErrorForNodes))
			for _, errNode := range tc.expectErrorForNodes {
				assert.NotNil(t, err[errNode])
			}

			if tc.addNode {
				tn, ok := m.Get(node.Name)
				assert.True(t, ok)
				assert.Equal(t, tc.expectedPending, tn.PendingOperations)
				assert.Equal(t, tc.expectedDesiredState, tn.DesiredState)
			}
		})
	}
}

func TestAssignBuffers(t *testing.T) {
	node := test.CreateNode("node-1", test.StateOpt(csn.NodeStateSuspended))
	node2 := test.CreateNode("node-2", test.StateOpt(csn.NodeStateChilling))
	fs := NewFakeNodeSource()
	m := NewNodeStateManager(fs.RegisterNodeHandler)
	mustRunManager(t, m)

	fs.AddNodes(node, node2)

	buffer := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name: "buffer-1",
		},
	}

	buffer2 := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name: "buffer-2",
		},
	}

	m.AssignBuffers(map[string]*v1beta1.CapacityBuffer{
		node.Name:      buffer,
		node2.Name:     buffer2,
		"missing-node": {ObjectMeta: metav1.ObjectMeta{Name: "buffer-3"}},
	})

	assert.Equal(t, map[string]*v1beta1.CapacityBuffer{
		node.Name:  buffer,
		node2.Name: buffer2,
	}, m.GetAssignedBuffers(node.Name, node2.Name, "missing-node", "extra-missing-node"))
}

func TestPeriodicMetricsSync(t *testing.T) {
	fs := NewFakeNodeSource()
	fakeClock := clock.NewFakeClock(time.Now())
	eventChan := make(chan NodeEvent, 10)
	metricInterval := 10 * time.Second
	m := NewNodeStateManager(
		fs.RegisterNodeHandler,
		WithClock(fakeClock),
		WithMetricsSyncInterval(metricInterval),
		WithEventHandler(func(n NodeEvent) {
			if _, ok := n.(NodeCounts); ok {
				eventChan <- n
			}
		}),
	)

	mustRunManager(t, m)

	node1 := test.CreateNode("node1", test.StateOpt(csn.NodeStateSuspended))
	node2 := test.CreateNode("node2", test.StateOpt(csn.NodeStateChilling))
	fs.AddNodes(node1, node2)

	fakeClock.Step(metricInterval + time.Second)

	event := <-eventChan
	countsEvent, ok := event.(NodeCounts)
	if !ok {
		t.Fatalf("Unexpected event type: %T", countsEvent)
	}
	assert.Equal(t, 1, countsEvent.Counts[csn.NodeStateSuspended])
	assert.Equal(t, 1, countsEvent.Counts[csn.NodeStateChilling])

	fs.AddNodes(
		test.CreateNode("node3", test.StateOpt(csn.NodeStateChilling)),
	)
	fs.DeleteNodes(node1)

	fakeClock.Step(metricInterval + time.Second)

	event = <-eventChan
	countsEvent, ok = event.(NodeCounts)
	if !ok {
		t.Fatalf("Unexpected event type: %T", countsEvent)
	}
	assert.Equal(t, 2, countsEvent.Counts[csn.NodeStateChilling])
	assert.Equal(t, 0, countsEvent.Counts[csn.NodeStateSuspended])
}

type FakeNodeSource struct {
	nodeHandler NodeHandler
}

func NewFakeNodeSource() *FakeNodeSource {
	noOp := func(_ *v1.Node) {}
	return &FakeNodeSource{
		nodeHandler: NodeHandler{
			OnAdd:    noOp,
			OnUpdate: noOp,
			OnDelete: noOp,
		},
	}
}

func (f *FakeNodeSource) RegisterNodeHandler(nh NodeHandler) error {
	f.nodeHandler = nh
	return nil
}

func (f *FakeNodeSource) AddNodes(nodes ...*v1.Node) {
	for _, n := range nodes {
		f.nodeHandler.OnAdd(n)
	}
}

func (f *FakeNodeSource) UpdateNodes(nodes ...*v1.Node) {
	for _, n := range nodes {
		f.nodeHandler.OnUpdate(n)
	}
}

func (f *FakeNodeSource) DeleteNodes(nodes ...*v1.Node) {
	for _, n := range nodes {
		f.nodeHandler.OnDelete(n)
	}
}

func mustRunManager(t *testing.T, m *NodeStateManager) {
	ctx, cancel := context.WithCancel(context.Background())
	if err := m.Run(ctx); err != nil {
		t.Fatalf("failed to run node state manager")
	}
	t.Cleanup(func() {
		cancel()
	})
}
