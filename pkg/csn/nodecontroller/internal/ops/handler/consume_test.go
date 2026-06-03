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

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state"
	statetest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/test"
	"k8s.io/utils/set"
)

func TestConsumeHandler_Handle(t *testing.T) {
	mig := gce.GceRef{Project: "project", Zone: "zone", Name: "mig"}
	suspendedNode := test.CreateNode("suspended-node", test.StateOpt(csn.NodeStateSuspended))
	suspendedNodeRef := mustGetRef(t, suspendedNode)
	consumedNode := test.CreateNode("consumed-node", test.StateOpt(csn.NodeStateConsumed))
	consumedNodeRef := mustGetRef(t, consumedNode)
	chillingNode := test.CreateNode("chilling-node", test.StateOpt(csn.NodeStateChilling))
	chillingNodeRef := mustGetRef(t, chillingNode)
	invalidNode := test.CreateNode("invalid-node", func(n *v1.Node) {
		n.Spec.ProviderID = ""
	})
	if _, err := gce.GceRefFromProviderId(invalidNode.Spec.ProviderID); err == nil {
		t.Fatalf("Expected err when calculating GceRef from invalid node")
	}

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
		k8sClientErr            error
		expectError             bool
		expectedSuccessfulNodes set.Set[string]
		expectedFailedNodes     set.Set[string]
		expectedResumed         []test.ResumeCall
		expectedPatched         []test.PatchCall
		expectedDeltas          []*test.MetricDelta
	}{
		{
			name: "success_single_suspended_node",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.ConsumeOp,
				NodeNames: set.New(suspendedNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					suspendedNode.Name: {Node: suspendedNode, State: csn.NodeStateSuspended},
				},
			},
			expectError:             false,
			expectedSuccessfulNodes: set.New(suspendedNode.Name),
			expectedResumed:         []test.ResumeCall{{MIG: mig, Instances: []gce.GceRef{mustGetRef(t, suspendedNode)}}},
			expectedPatched:         []test.PatchCall{{Node: suspendedNode, State: csn.NodeStateConsumed}},
			expectedDeltas: []*test.MetricDelta{
				test.NewMetricDelta(test.ExpectedValue(1), opGceBatchSize, []string{resumeCall, gceSuccess}),
			},
		},
		{
			name: "wrong_operation_type",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.SuspendOp,
				NodeNames: set.New(suspendedNode.Name),
			},
			expectError: true,
		},
		{
			name: "node_not_found_in_state_manager",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.ConsumeOp,
				NodeNames: set.New("unknown-node"),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{},
			},
			expectError:             false,
			expectedSuccessfulNodes: set.New("unknown-node"),
			expectedResumed:         nil,
			expectedPatched:         nil,
		},
		{
			name: "node_not_suspended",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.ConsumeOp,
				NodeNames: set.New(consumedNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					consumedNode.Name: {Node: consumedNode, State: csn.NodeStateConsumed},
				},
			},
			expectError:             false,
			expectedSuccessfulNodes: set.New(consumedNode.Name),
			expectedResumed:         nil,
			expectedPatched:         []test.PatchCall{{Node: consumedNode, State: csn.NodeStateConsumed}},
		},
		{
			name: "success_multiple_mixed_nodes",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.ConsumeOp,
				NodeNames: set.New(suspendedNode.Name, chillingNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					suspendedNode.Name: {Node: suspendedNode, State: csn.NodeStateSuspended},
					chillingNode.Name:  {Node: chillingNode, State: csn.NodeStateChilling},
				},
			},
			expectError:             false,
			expectedSuccessfulNodes: set.New(suspendedNode.Name, chillingNode.Name),
			expectedResumed:         []test.ResumeCall{{MIG: mig, Instances: []gce.GceRef{suspendedNodeRef}}},
			expectedPatched: []test.PatchCall{
				{Node: suspendedNode, State: csn.NodeStateConsumed},
				{Node: chillingNode, State: csn.NodeStateConsumed},
			},
			expectedDeltas: []*test.MetricDelta{
				test.NewMetricDelta(test.ExpectedValue(1), opGceBatchSize, []string{resumeCall, gceSuccess}),
			},
		},
		{
			name: "resume_instances_error",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.ConsumeOp,
				NodeNames: set.New(suspendedNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					suspendedNode.Name: {Node: suspendedNode, State: csn.NodeStateSuspended},
				},
			},
			cloudProvider: &test.MockCloudProvider{
				Instances: func(ref gce.GceRef) *gce.GceInstance {
					return defaultStatusMapping[ref]
				},
				ResumeErr: errors.New("resume error"),
			},
			expectError:         false,
			expectedFailedNodes: set.New(suspendedNode.Name),
			expectedResumed:     []test.ResumeCall{{MIG: mig, Instances: []gce.GceRef{suspendedNodeRef}}},
			expectedDeltas: []*test.MetricDelta{
				test.NewMetricDelta(test.ExpectedValue(1), opGceBatchSize, []string{resumeCall, gceFailure}),
			},
		},
		{
			name: "patch_node_error",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.ConsumeOp,
				NodeNames: set.New(suspendedNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					suspendedNode.Name: {Node: suspendedNode, State: csn.NodeStateSuspended},
				},
			},
			k8sClientErr:        errors.New("patch error"),
			expectError:         false,
			expectedFailedNodes: set.New(suspendedNode.Name),
			expectedResumed:     []test.ResumeCall{{MIG: mig, Instances: []gce.GceRef{suspendedNodeRef}}},
			expectedPatched:     []test.PatchCall{{Node: suspendedNode, State: csn.NodeStateConsumed}},
			expectedDeltas: []*test.MetricDelta{
				test.NewMetricDelta(test.ExpectedValue(1), opGceBatchSize, []string{resumeCall, gceSuccess}),
			},
		},
		{
			name: "gce_ref_error",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.ConsumeOp,
				NodeNames: set.New(invalidNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					invalidNode.Name: {Node: invalidNode, State: csn.NodeStateSuspended},
				},
			},
			expectError:         false,
			expectedFailedNodes: set.New(invalidNode.Name),
		},
		{
			name: "instance_status_not_found",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.ConsumeOp,
				NodeNames: set.New(suspendedNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					suspendedNode.Name: {Node: suspendedNode, State: csn.NodeStateSuspended},
				},
			},
			cloudProvider:       &test.MockCloudProvider{},
			expectError:         false,
			expectedFailedNodes: set.New(suspendedNode.Name),
		},
		{
			name: "only_patch_for_running_instance",
			op: ops.Operation{
				MIG:       mig,
				Type:      ops.ConsumeOp,
				NodeNames: set.New(suspendedNode.Name),
			},
			stateManager: &statetest.MockStateManager{
				Nodes: map[string]state.TrackedNode{
					suspendedNode.Name: {Node: suspendedNode, State: csn.NodeStateSuspended},
				},
			},
			cloudProvider: &test.MockCloudProvider{
				Instances: func(_ gce.GceRef) *gce.GceInstance {
					return &gce.GceInstance{GCEStatus: "RUNNING"}
				},
			},
			expectError:             false,
			expectedSuccessfulNodes: set.New(suspendedNode.Name),
			expectedPatched:         []test.PatchCall{{Node: suspendedNode, State: csn.NodeStateConsumed}},
			expectedDeltas: []*test.MetricDelta{
				test.NewMetricDelta(test.ExpectedValue(0), opGceBatchSize, []string{resumeCall, gceSuccess}),
				test.NewMetricDelta(test.ExpectedValue(0), opGceBatchSize, []string{resumeCall, gceFailure}),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			k8sClient := &test.MockK8sClient{PatchErr: tc.k8sClientErr}
			cloudProvider := &test.MockCloudProvider{Instances: func(ref gce.GceRef) *gce.GceInstance {
				return defaultStatusMapping[ref]
			}}
			if tc.cloudProvider != nil {
				cloudProvider = tc.cloudProvider
			}
			stateManager := &statetest.MockStateManager{}
			if tc.stateManager != nil {
				stateManager = tc.stateManager
			}
			h := NewConsumeHandler(stateManager, cloudProvider, k8sClient)

			for _, ed := range tc.expectedDeltas {
				ed.Init(t)
			}

			res, err := h.Handle(t.Context(), tc.op)

			// Verify after
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
			assert.ElementsMatch(t, tc.expectedResumed, cloudProvider.GetResumeCalls())
			assert.ElementsMatch(t, tc.expectedPatched, k8sClient.GetPatchCalls())
		})
	}
}

func mustGetRef(t *testing.T, n *v1.Node) gce.GceRef {
	t.Helper()
	ref, err := gce.GceRefFromProviderId(n.Spec.ProviderID)
	if err != nil {
		t.Fatalf("Failed to extract GceRef from node %q", n.Name)
	}
	return ref
}

func TestConsumeHandler_HandleBatching(t *testing.T) {
	mig := gce.GceRef{Project: "project", Zone: "zone", Name: "mig"}
	nodeCount := maxBatchSize*2 + 1
	var batchingNodes []string
	batchingTrackedNodes := map[string]state.TrackedNode{}
	var expectedRefs []gce.GceRef
	for i := 0; i < nodeCount; i++ {
		name := fmt.Sprintf("batching-node-%d", i)
		batchingNodes = append(batchingNodes, name)
		n := test.CreateNode(name, test.StateOpt(csn.NodeStateSuspended))
		batchingTrackedNodes[name] = state.TrackedNode{Node: n, State: csn.NodeStateSuspended}
		expectedRefs = append(expectedRefs, mustGetRef(t, n))
	}

	stateManager := &statetest.MockStateManager{Nodes: batchingTrackedNodes}
	cloudProvider := &test.MockCloudProvider{
		Instances: func(_ gce.GceRef) *gce.GceInstance {
			return &gce.GceInstance{GCEStatus: "SUSPENDED"}
		},
	}
	k8sClient := &test.MockK8sClient{}
	h := NewConsumeHandler(stateManager, cloudProvider, k8sClient)

	op := ops.Operation{
		MIG:       mig,
		Type:      ops.ConsumeOp,
		NodeNames: set.New(batchingNodes...),
	}

	res, err := h.Handle(t.Context(), op)
	assert.NoError(t, err)
	assert.Equal(t, nodeCount, res.Success.Len())
	assert.Equal(t, 0, len(res.Errs))

	resumeCalls := cloudProvider.GetResumeCalls()
	assert.Equal(t, (nodeCount+maxBatchSize-1)/maxBatchSize, len(resumeCalls)) // 1000, 1000, 1

	var totalSize int
	var actualRefs []gce.GceRef
	for _, call := range resumeCalls {
		assert.Equal(t, mig, call.MIG)
		assert.True(t, len(call.Instances) <= maxBatchSize)
		totalSize += len(call.Instances)
		for _, ref := range call.Instances {
			actualRefs = append(actualRefs, ref)
		}
	}
	assert.Equal(t, nodeCount, totalSize)
	assert.ElementsMatch(t, expectedRefs, actualRefs)
}
