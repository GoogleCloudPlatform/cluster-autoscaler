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
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state"
	statetest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/test"
	"k8s.io/utils/set"
)

func TestSuspendHandler_Handle(t *testing.T) {
	mig := gce.GceRef{Project: "project", Zone: "zone", Name: "mig"}
	chillingNode := test.CreateNode("node-1", test.StateOpt(csn.NodeStateChilling))
	chillingNodeRef := mustGetRef(t, chillingNode)
	suspendedNode := test.CreateNode("node-2", test.StateOpt(csn.NodeStateSuspended))
	suspendedNodeRef := mustGetRef(t, suspendedNode)
	consumedNode := test.CreateNode("node-2", test.StateOpt(csn.NodeStateConsumed))
	consumedNodeRef := mustGetRef(t, consumedNode)

	defaultStatusMapping := map[gce.GceRef]*gce.GceInstance{
		suspendedNodeRef: {GCEStatus: "SUSPENDED"},
		consumedNodeRef:  {GCEStatus: "RUNNING"},
		chillingNodeRef:  {GCEStatus: "RUNNING"},
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
		expectedPatched         []test.PatchCall
		expectedEnqueued        []ops.Operation
		expectedDeltas          []*test.MetricDelta
	}{
		{
			name: "success_single_node_safe_to_suspend",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.SuspendOp,
				NodeNames: set.New(chillingNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					chillingNode.Name: {Node: chillingNode, State: csn.NodeStateChilling},
				},
			},
			k8sClient: &test.MockK8sClient{
				SuspensionBlocked: map[string]bool{chillingNode.Name: false},
			},
			expectError:             false,
			expectedSuccessfulNodes: set.New(chillingNode.Name),
			expectedSuspended:       []test.SuspendCall{{MIG: mig, Instances: []gce.GceRef{chillingNodeRef}, Force: false}},
			expectedPatched:         []test.PatchCall{{Node: chillingNode, State: csn.NodeStateSuspended}},
			expectedDeltas: []*test.MetricDelta{
				test.NewMetricDelta(test.ExpectedValue(1), opGceBatchSize, []string{suspendCall, gceSuccess}),
			},
		},
		{
			name: "single_node_unsafe_to_suspend_enqueues_consume",
			op: ops.Operation{
				MIG:       mig,
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
			expectError:             false,
			expectedSuccessfulNodes: set.New(chillingNode.Name),
			expectedSuspended:       nil,
			expectedPatched:         []test.PatchCall{{Node: chillingNode, State: csn.NodeStateSuspended}},
			expectedEnqueued: []ops.Operation{{
				MIG:       mig,
				Type:      ops.ConsumeOp,
				NodeNames: set.New(chillingNode.Name),
			}},
		},
		{
			name: "mixed_nodes_one_safe_one_unsafe",
			op: ops.Operation{
				MIG:       mig,
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
			expectError:             false,
			expectedSuccessfulNodes: set.New(chillingNode.Name, consumedNode.Name),
			expectedSuspended: []test.SuspendCall{{
				MIG:       mig,
				Instances: []gce.GceRef{chillingNodeRef},
				Force:     false,
			}},
			expectedPatched: []test.PatchCall{
				{Node: chillingNode, State: csn.NodeStateSuspended},
				{Node: consumedNode, State: csn.NodeStateSuspended},
			},
			expectedEnqueued: []ops.Operation{{
				MIG:       mig,
				Type:      ops.ConsumeOp,
				NodeNames: set.New(consumedNode.Name),
			}},
		},
		{
			name: "patch_error",
			op: ops.Operation{
				MIG:       mig,
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
			expectError:         false,
			expectedFailedNodes: set.New(chillingNode.Name),
			expectedPatched:     []test.PatchCall{{Node: chillingNode, State: csn.NodeStateSuspended}},
		},
		{
			name: "check_pods_error",
			op: ops.Operation{
				MIG:       mig,
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
			expectError:         false,
			expectedFailedNodes: set.New(chillingNode.Name),
			expectedPatched:     []test.PatchCall{{Node: chillingNode, State: csn.NodeStateSuspended}},
		},
		{
			name: "suspend_instances_error",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.SuspendOp,
				NodeNames: set.New(chillingNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					chillingNode.Name: {Node: chillingNode, State: csn.NodeStateChilling},
				},
			},
			cloudProvider: &test.MockCloudProvider{
				Instances: func(ref gce.GceRef) *gce.GceInstance {
					return defaultStatusMapping[ref]
				},
				SuspendErr: errors.New("suspend error"),
			},
			k8sClient: &test.MockK8sClient{
				SuspensionBlocked: map[string]bool{chillingNode.Name: false},
			},
			expectError:         false,
			expectedFailedNodes: set.New(chillingNode.Name),
			expectedPatched:     []test.PatchCall{{Node: chillingNode, State: csn.NodeStateSuspended}},
			expectedSuspended:   []test.SuspendCall{{MIG: mig, Instances: []gce.GceRef{chillingNodeRef}, Force: false}},
			expectedDeltas: []*test.MetricDelta{
				test.NewMetricDelta(test.ExpectedValue(1), opGceBatchSize, []string{suspendCall, gceFailure}),
			},
		},
		{
			name: "enqueue_error",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.SuspendOp,
				NodeNames: set.New(chillingNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					chillingNode.Name: {Node: chillingNode, State: csn.NodeStateChilling},
				},
			},
			k8sClient: &test.MockK8sClient{
				SuspensionBlocked: map[string]bool{chillingNode.Name: true}, // Unsafe
			},
			enqueueErr:          errors.New("enqueue error"),
			expectError:         false,
			expectedFailedNodes: set.New(chillingNode.Name),
			expectedPatched:     []test.PatchCall{{Node: chillingNode, State: csn.NodeStateSuspended}},
			expectedEnqueued: []ops.Operation{{
				MIG:       mig,
				Type:      ops.ConsumeOp,
				NodeNames: set.New(chillingNode.Name),
			}},
		},
		{
			name: "node_not_found_should_be_noop",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.SuspendOp,
				NodeNames: set.New("unknown-node"),
			},
			expectedSuccessfulNodes: set.New("unknown-node"),
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{},
			},
			k8sClient: &test.MockK8sClient{},
		},
		{
			name: "error_when_op_type_incorrect",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.ConsumeOp,
				NodeNames: set.New(chillingNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					chillingNode.Name: {Node: chillingNode, State: csn.NodeStateChilling},
				},
			},
			k8sClient: &test.MockK8sClient{
				SuspensionBlocked: map[string]bool{chillingNode.Name: false},
			},
			expectError: true,
		},
		{
			name: "instance_status_not_found",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.SuspendOp,
				NodeNames: set.New(chillingNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					chillingNode.Name: {Node: chillingNode, State: csn.NodeStateChilling},
				},
			},
			cloudProvider:       &test.MockCloudProvider{},
			k8sClient:           &test.MockK8sClient{},
			expectError:         false,
			expectedFailedNodes: set.New(chillingNode.Name),
			expectedPatched:     []test.PatchCall{{Node: chillingNode, State: csn.NodeStateSuspended}},
		},
		{
			name: "only_patch_for_suspended_instance",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.SuspendOp,
				NodeNames: set.New(chillingNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					chillingNode.Name: {Node: chillingNode, State: csn.NodeStateChilling},
				},
			},
			cloudProvider: &test.MockCloudProvider{
				Instances: func(_ gce.GceRef) *gce.GceInstance {
					return &gce.GceInstance{GCEStatus: "SUSPENDED"}
				},
			},
			k8sClient:               &test.MockK8sClient{},
			expectError:             false,
			expectedSuccessfulNodes: set.New(chillingNode.Name),
			expectedPatched:         []test.PatchCall{{Node: chillingNode, State: csn.NodeStateSuspended}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var enqueued []ops.Operation
			enqueue := func(op ops.Operation) error {
				enqueued = append(enqueued, op)
				return tc.enqueueErr
			}
			if tc.cloudProvider == nil {
				tc.cloudProvider = &test.MockCloudProvider{Instances: func(ref gce.GceRef) *gce.GceInstance {
					return defaultStatusMapping[ref]
				}}
			}

			h := NewSuspendHandler(tc.stateManager, tc.cloudProvider, tc.k8sClient, enqueue, time.Duration(0))
			for _, ed := range tc.expectedDeltas {
				ed.Init(t)
			}
			res, err := h.Handle(t.Context(), tc.op)
			for _, ed := range tc.expectedDeltas {
				assert.NoError(t, ed.Verify(t))
			}
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.ElementsMatch(t, tc.expectedSuccessfulNodes.UnsortedList(), res.Success.UnsortedList())
			assert.ElementsMatch(t, tc.expectedFailedNodes.UnsortedList(), slices.Collect(maps.Keys(res.Errs)))
			assert.ElementsMatch(t, tc.expectedSuspended, tc.cloudProvider.GetSuspendCalls())
			assert.ElementsMatch(t, tc.k8sClient.GetPatchCalls(), tc.expectedPatched)
			assert.ElementsMatch(t, tc.expectedEnqueued, enqueued)
		})
	}
}

func TestSuspendHandler_HandleBatching(t *testing.T) {
	mig := gce.GceRef{Project: "project", Zone: "zone", Name: "mig"}
	nodeCount := maxBatchSize*2 + 1
	var batchingNodes []string
	batchingTrackedNodes := map[string]state.TrackedNode{}
	var expectedRefs []gce.GceRef

	for i := 0; i < nodeCount; i++ {
		name := fmt.Sprintf("batching-node-%d", i)
		batchingNodes = append(batchingNodes, name)
		n := test.CreateNode(name, test.StateOpt(csn.NodeStateChilling))
		batchingTrackedNodes[name] = state.TrackedNode{Node: n, State: csn.NodeStateChilling}
		expectedRefs = append(expectedRefs, mustGetRef(t, n))
	}

	stateManager := &statetest.MockStateManager{Nodes: batchingTrackedNodes}
	cloudProvider := &test.MockCloudProvider{
		Instances: func(_ gce.GceRef) *gce.GceInstance {
			return &gce.GceInstance{GCEStatus: "RUNNING"}
		},
	}
	enqueue := func(op ops.Operation) error { return nil }

	h := NewSuspendHandler(stateManager, cloudProvider, &test.MockK8sClient{}, enqueue, time.Duration(0))

	op := ops.Operation{
		MIG:       mig,
		Type:      ops.SuspendOp,
		NodeNames: set.New(batchingNodes...),
	}

	res, err := h.Handle(t.Context(), op)
	assert.NoError(t, err)
	assert.Equal(t, nodeCount, res.Success.Len())
	assert.Equal(t, 0, len(res.Errs))

	suspendCalls := cloudProvider.GetSuspendCalls()
	assert.Equal(t, (nodeCount+maxBatchSize-1)/maxBatchSize, len(suspendCalls)) // 1000, 1000, 1

	var totalSize int
	var actualRefs []gce.GceRef
	for _, call := range suspendCalls {
		assert.Equal(t, mig, call.MIG)
		assert.True(t, len(call.Instances) <= maxBatchSize)
		assert.False(t, call.Force)
		totalSize += len(call.Instances)
		actualRefs = append(actualRefs, call.Instances...)
	}
	assert.Equal(t, nodeCount, totalSize)
	assert.ElementsMatch(t, expectedRefs, actualRefs)
}
