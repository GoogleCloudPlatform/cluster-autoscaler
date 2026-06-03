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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state"
	statetest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/test"
	"k8s.io/utils/set"
)

func TestAssignBufferHandler_Handle(t *testing.T) {
	mig := gce.GceRef{Project: "project", Zone: "zone", Name: "mig"}
	node1 := test.CreateNode("node-1", test.StateOpt(csn.NodeStateSuspended))
	node2 := test.CreateNode("node-2", test.StateOpt(csn.NodeStateChilling))
	buffer1 := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "buffer-1",
			Namespace: "default",
		},
	}

	tests := []struct {
		name                    string
		op                      ops.Operation
		stateManager            *statetest.MockStateManager
		k8sClientErr            error
		expectError             bool
		expectedSuccessfulNodes set.Set[string]
		expectedFailedNodes     set.Set[string]
		expectedPatched         []test.BufferAssignmentPatchCall
	}{
		{
			name: "success_single_node",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.AssignBufferOp,
				NodeNames: set.New(node1.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					node1.Name: {Node: node1},
				},
				NodeNameToBuffer: map[string]*v1beta1.CapacityBuffer{
					node1.Name: buffer1,
				},
			},
			expectError:             false,
			expectedSuccessfulNodes: set.New(node1.Name),
			expectedPatched: []test.BufferAssignmentPatchCall{
				{Node: node1, Buffer: buffer1},
			},
		},
		{
			name: "wrong_operation_type",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.ConsumeOp,
				NodeNames: set.New(node1.Name),
			},
			expectError: true,
		},
		{
			name: "node_not_found_in_state_manager",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.AssignBufferOp,
				NodeNames: set.New(node1.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{}, // Empty nodes
				NodeNameToBuffer: map[string]*v1beta1.CapacityBuffer{
					node1.Name: buffer1,
				},
			},
			expectError:             false,
			expectedSuccessfulNodes: set.New(node1.Name),
		},
		{
			name: "buffer_not_found",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.AssignBufferOp,
				NodeNames: set.New(node1.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					node1.Name: {Node: node1},
				},
				NodeNameToBuffer: map[string]*v1beta1.CapacityBuffer{}, // No buffer
			},
			expectedSuccessfulNodes: set.New(node1.Name),
			expectError:             false,
		},
		{
			name: "patch_error",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.AssignBufferOp,
				NodeNames: set.New(node1.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					node1.Name: {Node: node1},
				},
				NodeNameToBuffer: map[string]*v1beta1.CapacityBuffer{
					node1.Name: buffer1,
				},
			},
			k8sClientErr:        errors.New("patch error"),
			expectError:         false,
			expectedFailedNodes: set.New(node1.Name),
			expectedPatched: []test.BufferAssignmentPatchCall{
				{Node: node1, Buffer: buffer1},
			},
		},
		{
			name: "multiple_nodes",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.AssignBufferOp,
				NodeNames: set.New(node1.Name, node2.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					node1.Name: {Node: node1},
					node2.Name: {Node: node2},
				},
				NodeNameToBuffer: map[string]*v1beta1.CapacityBuffer{
					node1.Name: buffer1,
					node2.Name: buffer1,
				},
			},
			expectError:             false,
			expectedSuccessfulNodes: set.New(node1.Name, node2.Name),
			expectedPatched: []test.BufferAssignmentPatchCall{
				{Node: node1, Buffer: buffer1},
				{Node: node2, Buffer: buffer1},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			k8sClient := &test.MockK8sClient{BufferAssignmentPatchErr: tc.k8sClientErr}
			stateManager := &statetest.MockStateManager{}
			if tc.stateManager != nil {
				stateManager = tc.stateManager
			}
			h := NewAssignBufferHandler(stateManager, k8sClient)
			res, err := h.Handle(t.Context(), tc.op)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.ElementsMatch(t, tc.expectedSuccessfulNodes.UnsortedList(), res.Success.UnsortedList())
			assert.ElementsMatch(t, tc.expectedFailedNodes.UnsortedList(), slices.Collect(maps.Keys(res.Errs)))
			assert.ElementsMatch(t, tc.expectedPatched, k8sClient.GetBufferAssignmentPatchCalls())
		})
	}
}
