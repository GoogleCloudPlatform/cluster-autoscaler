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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/test"
)

// Basic cases that have similar behavior for all node event types
func TestNodeEventHandler(t *testing.T) {
	suspendedNode := test.CreateNode("suspended-node", test.StateOpt(csn.NodeStateSuspended))
	chillingNode := test.CreateNode("chilling-node", test.StateOpt(csn.NodeStateChilling))
	consumedNode := test.CreateNode("consumed-node", test.StateOpt(csn.NodeStateConsumed))

	testCases := []struct {
		name          string
		sourceFunc    func(*FakeNodeSource)
		expectedEvent NodeEvent
	}{
		{
			name: "add_event_emitted_for_suspended_node",
			sourceFunc: func(s *FakeNodeSource) {
				s.AddNodes(suspendedNode)
			},
			expectedEvent: NodeAdded{
				State: csn.NodeStateSuspended,
				Node:  suspendedNode,
			},
		},
		{
			name: "update_event_emitted_for_suspended_node",
			sourceFunc: func(s *FakeNodeSource) {
				s.UpdateNodes(suspendedNode)
			},
			expectedEvent: NodeUpdated{
				OldState: csn.NodeStateConsumed, // Fallback for first time seen
				NewState: csn.NodeStateSuspended,
				Node:     suspendedNode,
			},
		},
		{
			name: "delete_event_emitted_for_suspended_node",
			sourceFunc: func(s *FakeNodeSource) {
				s.DeleteNodes(suspendedNode)
			},
			expectedEvent: NodeDeleted{
				Node:  suspendedNode,
				State: csn.NodeStateSuspended,
			},
		},
		{
			name: "add_event_emitted_for_chilling_node",
			sourceFunc: func(s *FakeNodeSource) {
				s.AddNodes(chillingNode)
			},
			expectedEvent: NodeAdded{
				State: csn.NodeStateChilling,
				Node:  chillingNode,
			},
		},
		{
			name: "update_event_emitted_for_chilling_node",
			sourceFunc: func(s *FakeNodeSource) {
				s.UpdateNodes(chillingNode)
			},
			expectedEvent: NodeUpdated{
				OldState: csn.NodeStateConsumed, // Fallback for first time seen
				NewState: csn.NodeStateChilling,
				Node:     chillingNode,
			},
		},
		{
			name: "delete_event_emitted_for_chilling_node",
			sourceFunc: func(s *FakeNodeSource) {
				s.DeleteNodes(chillingNode)
			},
			expectedEvent: NodeDeleted{
				Node:  chillingNode,
				State: csn.NodeStateChilling,
			},
		},
		{
			name: "add_event_emitted_for_consumed_node",
			sourceFunc: func(s *FakeNodeSource) {
				s.AddNodes(consumedNode)
			},
			expectedEvent: NodeAdded{
				State: csn.NodeStateConsumed,
				Node:  consumedNode,
			},
		},
		{
			name: "update_event_emitted_for_consumed_node",
			sourceFunc: func(s *FakeNodeSource) {
				s.UpdateNodes(consumedNode)
			},
			expectedEvent: NodeUpdated{
				OldState: csn.NodeStateConsumed,
				NewState: csn.NodeStateConsumed,
				Node:     consumedNode,
			},
		},
		{
			name: "delete_event_emitted_for_consumed_node",
			sourceFunc: func(s *FakeNodeSource) {
				s.DeleteNodes(consumedNode)
			},
			expectedEvent: NodeDeleted{
				Node:  consumedNode,
				State: csn.NodeStateConsumed,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fs := NewFakeNodeSource()
			eventChan := make(chan NodeEvent, 10)
			eh := func(e NodeEvent) {
				if _, ok := GetNodeFromEvent(e); !ok {
					return
				}
				eventChan <- e
			}
			m := NewNodeStateManager(fs.RegisterNodeHandler, WithEventHandler(eh))
			mustRunManager(t, m)

			if tc.sourceFunc != nil {
				tc.sourceFunc(fs)
			}

			assert.Equal(t, tc.expectedEvent, mustGetSingleEvent(t, eventChan))
		})
	}
}

func TestSequentialNodeUpdateEvents(t *testing.T) {
	fs := NewFakeNodeSource()
	eventChan := make(chan NodeEvent, 10)
	eh := func(e NodeEvent) {
		if _, ok := GetNodeFromEvent(e); !ok {
			return
		}
		eventChan <- e
	}
	m := NewNodeStateManager(fs.RegisterNodeHandler, WithEventHandler(eh))
	mustRunManager(t, m)

	node := test.CreateNode("test-node")

	// The node is not a CSN node, so it will just be observed.
	fs.UpdateNodes(node.DeepCopy())
	assert.Equal(t, NodeUpdated{
		Node:     node,
		OldState: csn.NodeStateConsumed,
		NewState: csn.NodeStateConsumed,
	}, mustGetSingleEvent(t, eventChan))

	chillingNode, err := csn.SetNodeAs(node.DeepCopy(), csn.NodeStateChilling)
	assert.NoError(t, err)

	fs.UpdateNodes(chillingNode.DeepCopy())
	assert.Equal(t, NodeUpdated{
		Node:     chillingNode,
		OldState: csn.NodeStateConsumed,
		NewState: csn.NodeStateChilling,
	}, mustGetSingleEvent(t, eventChan))

	suspendedNode, err := csn.SetNodeAs(node.DeepCopy(), csn.NodeStateSuspended)
	assert.NoError(t, err)

	// Node update event should be emitted with a new state
	fs.UpdateNodes(suspendedNode.DeepCopy())
	assert.Equal(t, NodeUpdated{
		Node:     suspendedNode,
		OldState: csn.NodeStateChilling,
		NewState: csn.NodeStateSuspended,
	}, mustGetSingleEvent(t, eventChan))

	consumedNode, err := csn.SetNodeAs(suspendedNode.DeepCopy(), csn.NodeStateConsumed)
	assert.NoError(t, err)

	// Node update event should be emitted when node is consumed
	fs.UpdateNodes(consumedNode.DeepCopy())
	assert.Equal(t, NodeUpdated{
		Node:     consumedNode,
		OldState: csn.NodeStateSuspended,
		NewState: csn.NodeStateConsumed,
	}, mustGetSingleEvent(t, eventChan))
}

func mustGetSingleEvent(t *testing.T, ch chan NodeEvent) NodeEvent {
	t.Helper()
	if !assert.Len(t, ch, 1) {
		t.Fatal()
	}
	return <-ch
}
