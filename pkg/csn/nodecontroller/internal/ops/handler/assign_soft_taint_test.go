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

package handler

import (
	"errors"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state"
	statetest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/test"
	"k8s.io/utils/set"
)

type mockTaintTracker struct {
	Counts  map[string]int
	Tracked map[string]bool
}

func (m *mockTaintTracker) GetTaintCountToAssign(nodeName string) (int, bool) {
	if m.Counts == nil {
		m.Counts = make(map[string]int)
	}
	if m.Tracked == nil {
		m.Tracked = make(map[string]bool)
	}

	if !m.Tracked[nodeName] {
		return 0, false
	}
	return m.Counts[nodeName], true
}

func TestAssignSoftTaintHandler_Handle(t *testing.T) {
	node1 := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}
	node2 := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-2"}}

	tests := []struct {
		name                    string
		op                      ops.Operation
		nodes                   map[string]state.TrackedNode
		stateManager            *statetest.MockStateManager
		tracker                 *mockTaintTracker
		k8sClientErr            error
		expectError             bool
		expectedSuccessfulNodes set.Set[string]
		expectedFailedNodes     set.Set[string]
		expectedPatches         []test.SoftTaintPatchCall
	}{
		{
			name: "success_single_node",
			op: ops.Operation{
				Type:      ops.AssignSoftTaintOp,
				NodeNames: set.New("node-1"),
			},
			nodes: map[string]state.TrackedNode{
				"node-1": {Node: node1},
			},
			tracker: &mockTaintTracker{
				Counts:  map[string]int{"node-1": 2},
				Tracked: map[string]bool{"node-1": true},
			},
			expectError:             false,
			expectedSuccessfulNodes: set.New("node-1"),
			expectedPatches: []test.SoftTaintPatchCall{
				{Node: node1, TaintCount: 2},
			},
		},
		{
			name: "multiple_nodes_mixed_tracking",
			op: ops.Operation{
				Type:      ops.AssignSoftTaintOp,
				NodeNames: set.New("node-1", "node-2"),
			},
			nodes: map[string]state.TrackedNode{
				"node-1": {Node: node1},
				"node-2": {Node: node2},
			},
			tracker: &mockTaintTracker{
				Counts:  map[string]int{"node-1": 1, "node-2": 3},
				Tracked: map[string]bool{"node-1": true, "node-2": false},
			},
			expectError:             false,
			expectedSuccessfulNodes: set.New("node-1", "node-2"),
			expectedPatches: []test.SoftTaintPatchCall{
				{Node: node1, TaintCount: 1},
				// node-2 is not tracked, so no patch
			},
		},
		{
			name: "node_not_in_state_manager",
			op: ops.Operation{
				Type:      ops.AssignSoftTaintOp,
				NodeNames: set.New("node-1"),
			},
			nodes: map[string]state.TrackedNode{},
			tracker: &mockTaintTracker{
				Counts:  map[string]int{"node-1": 1},
				Tracked: map[string]bool{"node-1": true},
			},
			expectedSuccessfulNodes: set.New("node-1"),
			expectError:             false,
			expectedPatches:         nil,
		},
		{
			name: "patch_error",
			op: ops.Operation{
				Type:      ops.AssignSoftTaintOp,
				NodeNames: set.New("node-1"),
			},
			nodes: map[string]state.TrackedNode{
				"node-1": {Node: node1},
			},
			tracker: &mockTaintTracker{
				Counts:  map[string]int{"node-1": 1},
				Tracked: map[string]bool{"node-1": true},
			},
			k8sClientErr:        errors.New("patch error"),
			expectError:         false,
			expectedFailedNodes: set.New("node-1"),
			expectedPatches: []test.SoftTaintPatchCall{
				{Node: node1, TaintCount: 1},
			},
		},
		{
			name: "wrong_op_type",
			op: ops.Operation{
				Type: ops.SuspendOp,
			},
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			k8sClient := &test.MockK8sClient{SoftTaintPatchErr: tc.k8sClientErr}
			sm := &statetest.MockStateManager{Nodes: tc.nodes}
			tracker := tc.tracker
			if tracker == nil {
				tracker = &mockTaintTracker{}
			}

			h := NewAssignSoftTaintHandler(sm, k8sClient, tracker)
			res, err := h.Handle(t.Context(), tc.op)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.ElementsMatch(t, tc.expectedSuccessfulNodes.UnsortedList(), res.Success.UnsortedList())
			assert.ElementsMatch(t, tc.expectedFailedNodes.UnsortedList(), slices.Collect(maps.Keys(res.Errs)))
			assert.ElementsMatch(t, tc.expectedPatches, k8sClient.GetSoftTaintPatchCalls())
		})
	}
}
