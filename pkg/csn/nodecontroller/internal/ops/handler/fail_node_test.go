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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state"
	statetest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/test"
	"k8s.io/utils/set"
)

func TestFailNodeHandler_Handle(t *testing.T) {
	node1 := test.CreateNode("node-1", test.StateOpt(csn.NodeStateChilling))
	node2 := test.CreateNode("node-2", test.StateOpt(csn.NodeStateChilling))

	tests := []struct {
		name                    string
		op                      ops.Operation
		stateManager            *statetest.MockStateManager
		k8sClient               *test.MockK8sClient
		expectError             bool
		expectedSuccessfulNodes set.Set[string]
		expectedFailedNodes     set.Set[string]
		expectedPatched         []test.PatchCall
	}{
		{
			name: "wrong_operation_type",
			op: ops.Operation{
				Type:      ops.SuspendOp,
				NodeNames: set.New(node1.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					node1.Name: {Node: node1, State: csn.NodeStateChilling},
				},
			},
			k8sClient:   &test.MockK8sClient{},
			expectError: true,
		},
		{
			name: "node_not_found_in_state_manager_is_noop",
			op: ops.Operation{
				Type:      ops.FailNodeOp,
				NodeNames: set.New("unknown-node"),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{},
			},
			k8sClient:               &test.MockK8sClient{},
			expectError:             false,
			expectedSuccessfulNodes: set.New("unknown-node"),
		},
		{
			name: "standard_path_success_not_blocked",
			op: ops.Operation{
				Type:      ops.FailNodeOp,
				NodeNames: set.New(node1.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					node1.Name: {Node: node1, State: csn.NodeStateChilling},
				},
			},
			k8sClient: &test.MockK8sClient{
				SuspensionBlocked: map[string]bool{node1.Name: false},
			},
			expectError:             false,
			expectedSuccessfulNodes: set.New(node1.Name),
			expectedPatched: []test.PatchCall{
				{Node: node1, State: csn.NodeStateFailed},
			},
		},
		{
			name: "fallback_path_success_blocked",
			op: ops.Operation{
				Type:      ops.FailNodeOp,
				NodeNames: set.New(node1.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					node1.Name: {Node: node1, State: csn.NodeStateChilling},
				},
			},
			k8sClient: &test.MockK8sClient{
				SuspensionBlocked: map[string]bool{node1.Name: true},
			},
			expectError:             false,
			expectedSuccessfulNodes: set.New(node1.Name),
			expectedPatched: []test.PatchCall{
				{Node: node1, State: csn.NodeStateConsumed},
			},
		},
		{
			name: "is_suspension_blocked_error",
			op: ops.Operation{
				Type:      ops.FailNodeOp,
				NodeNames: set.New(node1.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					node1.Name: {Node: node1, State: csn.NodeStateChilling},
				},
			},
			k8sClient: &test.MockK8sClient{
				SuspensionBlockedErr: errors.New("blocked error"),
			},
			expectError:         false,
			expectedFailedNodes: set.New(node1.Name),
		},
		{
			name: "patch_error_standard_path",
			op: ops.Operation{
				Type:      ops.FailNodeOp,
				NodeNames: set.New(node1.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					node1.Name: {Node: node1, State: csn.NodeStateChilling},
				},
			},
			k8sClient: &test.MockK8sClient{
				SuspensionBlocked: map[string]bool{node1.Name: false},
				PatchErr:          errors.New("patch error"),
			},
			expectError:         false,
			expectedFailedNodes: set.New(node1.Name),
			expectedPatched: []test.PatchCall{
				{Node: node1, State: csn.NodeStateFailed},
			},
		},
		{
			name: "patch_error_fallback_path",
			op: ops.Operation{
				Type:      ops.FailNodeOp,
				NodeNames: set.New(node1.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					node1.Name: {Node: node1, State: csn.NodeStateChilling},
				},
			},
			k8sClient: &test.MockK8sClient{
				SuspensionBlocked: map[string]bool{node1.Name: true},
				PatchErr:          errors.New("patch error"),
			},
			expectError:         false,
			expectedFailedNodes: set.New(node1.Name),
			expectedPatched: []test.PatchCall{
				{Node: node1, State: csn.NodeStateConsumed},
			},
		},
		{
			name: "multiple_nodes_mixed",
			op: ops.Operation{
				Type:      ops.FailNodeOp,
				NodeNames: set.New(node1.Name, node2.Name, "unknown-node"),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					node1.Name: {Node: node1, State: csn.NodeStateChilling},
					node2.Name: {Node: node2, State: csn.NodeStateChilling},
				},
			},
			k8sClient: &test.MockK8sClient{
				SuspensionBlocked: map[string]bool{
					node1.Name: false,
					node2.Name: true,
				},
			},
			expectError:             false,
			expectedSuccessfulNodes: set.New(node1.Name, node2.Name, "unknown-node"),
			expectedPatched: []test.PatchCall{
				{Node: node1, State: csn.NodeStateFailed},
				{Node: node2, State: csn.NodeStateConsumed},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := NewFailNodeHandler(tc.stateManager, tc.k8sClient)
			res, err := h.Handle(t.Context(), tc.op)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.ElementsMatch(t, tc.expectedSuccessfulNodes.UnsortedList(), res.Success.UnsortedList())
			expectedFailedList := []string{}
			if tc.expectedFailedNodes != nil {
				expectedFailedList = tc.expectedFailedNodes.UnsortedList()
			}
			actualFailedList := []string{}
			for k := range res.Errs {
				actualFailedList = append(actualFailedList, k)
			}
			assert.ElementsMatch(t, expectedFailedList, actualFailedList)
			assert.ElementsMatch(t, tc.k8sClient.GetPatchCalls(), tc.expectedPatched)
		})
	}
}
