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
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state"
	statetest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/test"
	"k8s.io/utils/set"
)

// TestSuspendHandler_Handle covers what the handler adds on top of the GCE
// choreography it shares with the consume handler: the patch to Suspended that
// states the intent up front, and the pods check that can turn the operation
// into a consumption instead. The shared part is covered by transition_test.go.
func TestSuspendHandler_Handle(t *testing.T) {
	chillingNode := test.CreateNode("node-1", test.StateOpt(csn.NodeStateChilling))
	chillingNodeRef := mustGetRef(t, chillingNode)
	suspendedNode := test.CreateNode("node-2", test.StateOpt(csn.NodeStateSuspended))
	suspendedNodeRef := mustGetRef(t, suspendedNode)
	consumedNode := test.CreateNode("node-3", test.StateOpt(csn.NodeStateConsumed))
	consumedNodeRef := mustGetRef(t, consumedNode)
	suspendingNode := test.CreateNode("node-4", test.StateOpt(csn.NodeStateChilling))
	suspendingNodeRef := mustGetRef(t, suspendingNode)
	terminatedNode := test.CreateNode("node-5", test.StateOpt(csn.NodeStateChilling))
	terminatedNodeRef := mustGetRef(t, terminatedNode)

	defaultManagedInstances := map[gce.GceRef]*gceclient.ManagedInstance{
		chillingNodeRef:   {Name: chillingNode.Name, InstanceStatus: "RUNNING", TargetStatus: "RUNNING", CurrentAction: "NONE"},
		suspendedNodeRef:  {Name: suspendedNode.Name, InstanceStatus: "SUSPENDED", TargetStatus: "SUSPENDED", CurrentAction: "NONE"},
		consumedNodeRef:   {Name: consumedNode.Name, InstanceStatus: "RUNNING", TargetStatus: "RUNNING", CurrentAction: "NONE"},
		suspendingNodeRef: {Name: suspendingNode.Name, InstanceStatus: "RUNNING", TargetStatus: "SUSPENDED", CurrentAction: "SUSPENDING"},
		terminatedNodeRef: {Name: terminatedNode.Name, InstanceStatus: "TERMINATED", TargetStatus: "STOPPED", CurrentAction: "NONE"},
	}

	tests := []struct {
		name                    string
		op                      ops.Operation
		stateManager            *statetest.MockStateManager
		cloudProvider           *test.MockCloudProvider
		k8sClient               *test.MockK8sClient
		enqueueErr              error
		expectError             bool
		expectedSuccessfulNodes set.Set[string]
		expectedFailedNodes     set.Set[string]
		expectedSuspended       []test.SuspendCall
		expectedPolled          []test.PollUntilCall
		expectedPatched         []test.PatchCall
		expectedEnqueued        []ops.Operation
	}{
		{
			// A safe node and a node that grew pods while it was chilling, so
			// suspending it would disrupt them. Both are patched to Suspended up
			// front; only the safe one is handed to GCE, and the blocked one is
			// consumed back instead.
			name: "mixed_nodes_one_safe_one_unsafe",
			op: ops.Operation{
				MIG:       testMIG,
				Type:      ops.SuspendOp,
				NodeNames: set.New(chillingNode.Name, consumedNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					chillingNode.Name: {Node: chillingNode, State: csn.NodeStateChilling},
					consumedNode.Name: {Node: consumedNode, State: csn.NodeStateConsumed},
				},
			},
			k8sClient: &test.MockK8sClient{
				SuspensionBlocked: map[string]bool{
					chillingNode.Name: false,
					consumedNode.Name: true,
				},
			},
			expectedSuccessfulNodes: set.New(chillingNode.Name, consumedNode.Name),
			expectedSuspended: []test.SuspendCall{{
				MIG:       testMIG,
				Instances: []gce.GceRef{chillingNodeRef},
				Force:     false,
			}},
			expectedPolled: []test.PollUntilCall{{Action: gceclient.ActionSuspending, MIG: testMIG, Instances: []gce.GceRef{chillingNodeRef}}},
			expectedPatched: []test.PatchCall{
				{Node: chillingNode, State: csn.NodeStateSuspended},
				{Node: consumedNode, State: csn.NodeStateSuspended},
			},
			expectedEnqueued: []ops.Operation{{
				MIG:       testMIG,
				Type:      ops.ConsumeOp,
				NodeNames: set.New(consumedNode.Name),
			}},
		},
		{
			// The intent has to be recorded before anything else happens, so a failed
			// patch stops the operation for that node.
			name: "patch_error",
			op: ops.Operation{
				MIG:       testMIG,
				Type:      ops.SuspendOp,
				NodeNames: set.New(chillingNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					chillingNode.Name: {Node: chillingNode, State: csn.NodeStateChilling},
				},
			},
			k8sClient: &test.MockK8sClient{
				PatchErr: errors.New("patch error"),
			},
			expectedFailedNodes: set.New(chillingNode.Name),
			expectedPatched:     []test.PatchCall{{Node: chillingNode, State: csn.NodeStateSuspended}},
		},
		{
			// Without knowing whether pods are running, suspending is not safe.
			name: "check_pods_error",
			op: ops.Operation{
				MIG:       testMIG,
				Type:      ops.SuspendOp,
				NodeNames: set.New(chillingNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					chillingNode.Name: {Node: chillingNode, State: csn.NodeStateChilling},
				},
			},
			k8sClient: &test.MockK8sClient{
				SuspensionBlockedErr: errors.New("check pods error"),
			},
			expectedFailedNodes: set.New(chillingNode.Name),
			expectedPatched:     []test.PatchCall{{Node: chillingNode, State: csn.NodeStateSuspended}},
		},
		{
			// The node keeps the Suspended patch even though GCE refused, so the next
			// reconciliation can pick the suspension back up.
			name: "suspend_instances_error",
			op: ops.Operation{
				MIG:       testMIG,
				Type:      ops.SuspendOp,
				NodeNames: set.New(chillingNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					chillingNode.Name: {Node: chillingNode, State: csn.NodeStateChilling},
				},
			},
			cloudProvider: &test.MockCloudProvider{
				ManagedInstances: map[gce.GceRef][]*gceclient.ManagedInstance{
					testMIG: {defaultManagedInstances[chillingNodeRef]},
				},
				SuspendErr: errors.New("suspend error"),
			},
			k8sClient: &test.MockK8sClient{
				SuspensionBlocked: map[string]bool{chillingNode.Name: false},
			},
			expectedFailedNodes: set.New(chillingNode.Name),
			expectedPatched:     []test.PatchCall{{Node: chillingNode, State: csn.NodeStateSuspended}},
			expectedSuspended:   []test.SuspendCall{{MIG: testMIG, Instances: []gce.GceRef{chillingNodeRef}, Force: false}},
		},
		{
			// The node was reverted to consumption but the queue refused it, so it
			// would be left patched as Suspended without anything acting on it.
			name: "enqueue_error",
			op: ops.Operation{
				MIG:       testMIG,
				Type:      ops.SuspendOp,
				NodeNames: set.New(chillingNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					chillingNode.Name: {Node: chillingNode, State: csn.NodeStateChilling},
				},
			},
			k8sClient: &test.MockK8sClient{
				SuspensionBlocked: map[string]bool{chillingNode.Name: true},
			},
			enqueueErr:          errors.New("enqueue error"),
			expectedFailedNodes: set.New(chillingNode.Name),
			expectedPatched:     []test.PatchCall{{Node: chillingNode, State: csn.NodeStateSuspended}},
			expectedEnqueued: []ops.Operation{{
				MIG:       testMIG,
				Type:      ops.ConsumeOp,
				NodeNames: set.New(chillingNode.Name),
			}},
		},
		{
			name: "node_not_found_should_be_noop",
			op: ops.Operation{
				MIG:       testMIG,
				Type:      ops.SuspendOp,
				NodeNames: set.New("unknown-node"),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{},
			},
			k8sClient:               &test.MockK8sClient{},
			expectedSuccessfulNodes: set.New("unknown-node"),
		},
		{
			name: "error_when_op_type_incorrect",
			op: ops.Operation{
				MIG:       testMIG,
				Type:      ops.ConsumeOp,
				NodeNames: set.New(chillingNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					chillingNode.Name: {Node: chillingNode, State: csn.NodeStateChilling},
				},
			},
			k8sClient:   &test.MockK8sClient{},
			expectError: true,
		},
		{
			// One node of each category, to check that all of them are patched to
			// Suspended up front and that only the ones GCE can still bring down are
			// reported as successes.
			name: "mixed_batch_all_four_groups",
			op: ops.Operation{
				MIG:       testMIG,
				Type:      ops.SuspendOp,
				NodeNames: set.New(chillingNode.Name, suspendingNode.Name, suspendedNode.Name, terminatedNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					chillingNode.Name:   {Node: chillingNode, State: csn.NodeStateChilling},
					suspendingNode.Name: {Node: suspendingNode, State: csn.NodeStateChilling},
					suspendedNode.Name:  {Node: suspendedNode, State: csn.NodeStateSuspended},
					terminatedNode.Name: {Node: terminatedNode, State: csn.NodeStateChilling},
				},
			},
			k8sClient:               &test.MockK8sClient{},
			expectedSuccessfulNodes: set.New(chillingNode.Name, suspendingNode.Name, suspendedNode.Name),
			expectedFailedNodes:     set.New(terminatedNode.Name),
			expectedSuspended:       []test.SuspendCall{{MIG: testMIG, Instances: []gce.GceRef{chillingNodeRef}, Force: false}},
			expectedPolled:          []test.PollUntilCall{{Action: gceclient.ActionSuspending, MIG: testMIG, Instances: []gce.GceRef{chillingNodeRef, suspendingNodeRef}}},
			expectedPatched: []test.PatchCall{
				{Node: chillingNode, State: csn.NodeStateSuspended},
				{Node: suspendingNode, State: csn.NodeStateSuspended},
				{Node: suspendedNode, State: csn.NodeStateSuspended},
				{Node: terminatedNode, State: csn.NodeStateSuspended},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var enqueued []ops.Operation
			enqueue := func(op ops.Operation) error {
				enqueued = append(enqueued, op)
				return tc.enqueueErr
			}
			cloudProvider := tc.cloudProvider
			if cloudProvider == nil {
				cloudProvider = &test.MockCloudProvider{ManagedInstances: map[gce.GceRef][]*gceclient.ManagedInstance{
					testMIG: slices.Collect(maps.Values(defaultManagedInstances)),
				}}
			}

			h := NewSuspendHandler(tc.stateManager, cloudProvider, tc.k8sClient, enqueue, time.Duration(0))

			res, err := h.Handle(t.Context(), tc.op)

			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.ElementsMatch(t, tc.expectedSuccessfulNodes.UnsortedList(), res.Success.UnsortedList())
			assert.ElementsMatch(t, tc.expectedFailedNodes.UnsortedList(), keysOf(res.Errs))
			assert.ElementsMatch(t, tc.expectedSuspended, cloudProvider.GetSuspendCalls())
			assert.ElementsMatch(t, tc.expectedPolled, cloudProvider.GetPollUntilCalls())
			assert.ElementsMatch(t, tc.expectedPatched, tc.k8sClient.GetPatchCalls())
			assert.ElementsMatch(t, tc.expectedEnqueued, enqueued)
		})
	}
}
