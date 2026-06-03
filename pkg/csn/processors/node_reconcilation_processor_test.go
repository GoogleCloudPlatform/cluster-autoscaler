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

package processors

import (
	"errors"
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/store"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/annotations"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller"
	nodecontrollertesting "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/testing"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

const (
	GiB = 1024 * 1024 * 1024
)

func TestNodeReconciliationProcess(t *testing.T) {
	upcomingMutator := withAnnotationMutator(map[string]string{
		annotations.NodeUpcomingAnnotation: "true",
	})
	csnMig := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{Labels: map[string]string{
		csn.SoftWorkloadSeparationKey: csn.SoftWorkloadSeparationValue,
	}}).Build()
	testCases := []struct {
		name                         string
		initialNodes                 []*apiv1.Node
		csnNodes                     []nodecontroller.CSNNode
		mig                          *gke.GkeMig
		migErr                       error
		listErr                      error
		templateNodeInfoFlagDisabled bool
		expectErr                    bool
		expectedNodeStates           map[string]csn.NodeState
		expectedNodeBuffers          map[string]string
		additionalNodeAssertions     func(*testing.T, *apiv1.Node)
	}{
		{
			name: "Successful reconciliation to Chilling",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "test-node", csn.NodeStateSuspended),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "test-node", DesiredState: csn.NodeStateChilling},
			},
			expectErr: false,
			expectedNodeStates: map[string]csn.NodeState{
				"test-node": csn.NodeStateChilling,
			},
		},
		{
			name: "Successful reconciliation to Suspended",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "test-node", csn.NodeStateChilling),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "test-node", DesiredState: csn.NodeStateSuspended, Buffer: &nodecontroller.BufferInfo{Namespace: "ns", Name: "buffer"}},
			},
			expectErr: false,
			expectedNodeStates: map[string]csn.NodeState{
				"test-node": csn.NodeStateSuspended,
			},
			expectedNodeBuffers: map[string]string{
				"test-node": "ns/buffer",
			},
		},
		{
			name: "Successful reconciliation to Consumed",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "test-node", csn.NodeStateChilling),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "test-node", DesiredState: csn.NodeStateConsumed},
			},
			expectErr: false,
			expectedNodeStates: map[string]csn.NodeState{
				"test-node": csn.NodeStateConsumed,
			},
		},
		{
			name: "Reconciliation to Unknown is skipped",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "test-node", csn.NodeStateChilling),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "test-node", DesiredState: csn.NodeStateUnknown},
			},
			expectErr: false,
			expectedNodeStates: map[string]csn.NodeState{
				"test-node": csn.NodeStateChilling,
			},
			expectedNodeBuffers: map[string]string{
				"test-node": bufferAssignmentUnknown,
			},
		},
		{
			name: "Consumed nodes returned from node controller have the unreachable taint removed",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "test-node", csn.NodeStateConsumed, withTaintsMutator(apiv1.Taint{
					Key:    apiv1.TaintNodeUnreachable,
					Effect: apiv1.TaintEffectNoSchedule,
				})),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "test-node", DesiredState: csn.NodeStateConsumed},
			},
			expectErr: false,
			expectedNodeStates: map[string]csn.NodeState{
				"test-node": csn.NodeStateConsumed,
			},
			additionalNodeAssertions: func(t *testing.T, node *apiv1.Node) {
				assert.False(t, taints.HasTaint(node, apiv1.TaintNodeUnreachable), "Unexpected unreachable taint")
			},
		},
		{
			name: "Consumed nodes not returned from node controller have the unreachable taint stayed",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "test-node", csn.NodeStateConsumed, withTaintsMutator(apiv1.Taint{
					Key:    apiv1.TaintNodeUnreachable,
					Effect: apiv1.TaintEffectNoSchedule,
				})),
			},
			csnNodes:  []nodecontroller.CSNNode{},
			expectErr: false,
			expectedNodeStates: map[string]csn.NodeState{
				"test-node": csn.NodeStateConsumed,
			},
			additionalNodeAssertions: func(t *testing.T, node *apiv1.Node) {
				assert.True(t, taints.HasTaint(node, apiv1.TaintNodeUnreachable), "Expected unreachable taint to be not removed")
			},
		},
		{
			name: "Error from nodeController.List",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "test-node", csn.NodeStateChilling),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "test-node", DesiredState: csn.NodeStateSuspended},
			},
			listErr:   errors.New("controller error"),
			expectErr: true,
			expectedNodeStates: map[string]csn.NodeState{ // Unchanged
				"test-node": csn.NodeStateChilling,
			},
		},
		{
			name: "Node not found in ClusterSnapshot",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "test-node", csn.NodeStateChilling),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "not-found-node", DesiredState: csn.NodeStateSuspended},
			},
			expectErr: false,
			expectedNodeStates: map[string]csn.NodeState{ // Unchanged
				"test-node": csn.NodeStateChilling,
			},
			expectedNodeBuffers: map[string]string{
				"test-node": bufferAssignmentUnknown,
			},
		},
		{
			name: "Empty list of CSN nodes",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "test-node", csn.NodeStateChilling),
			},
			csnNodes:  []nodecontroller.CSNNode{},
			expectErr: false,
			expectedNodeStates: map[string]csn.NodeState{ // Unchanged
				"test-node": csn.NodeStateChilling,
			},
			expectedNodeBuffers: map[string]string{
				"test-node": bufferAssignmentUnknown,
			},
		},
		{
			name: "Multiple nodes, one reconciled",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling),
				create8CPUTestNode(t, "node-2", csn.NodeStateConsumed),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateSuspended, Buffer: &nodecontroller.BufferInfo{Namespace: "ns", Name: "buffer"}},
			},
			expectErr: false,
			expectedNodeStates: map[string]csn.NodeState{
				"node-1": csn.NodeStateSuspended,
				"node-2": csn.NodeStateConsumed,
			},
			expectedNodeBuffers: map[string]string{
				"node-1": "ns/buffer",
			},
		},
		{
			name: "Multiple nodes, multiple reconciled",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling),
				create8CPUTestNode(t, "node-2", csn.NodeStateConsumed),
				create8CPUTestNode(t, "node-3", csn.NodeStateSuspended),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateConsumed},
				{Name: "node-2", DesiredState: csn.NodeStateChilling},
			},
			expectErr: false,
			expectedNodeStates: map[string]csn.NodeState{
				"node-1": csn.NodeStateConsumed,
				"node-2": csn.NodeStateChilling,
				"node-3": csn.NodeStateSuspended,
			},
		},
		{
			name: "Buffer assignment is reconciled from node controller",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateChilling, Buffer: &nodecontroller.BufferInfo{Namespace: "ns", Name: "buffer"}},
			},
			expectErr: false,
			expectedNodeStates: map[string]csn.NodeState{
				"node-1": csn.NodeStateChilling,
			},
			expectedNodeBuffers: map[string]string{
				"node-1": "ns/buffer",
			},
		},
		{
			name: "Buffer assignment is updated from node controller (source of truth)",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling, withBufferAssignmentMutator("ns/old-buffer")),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateChilling, Buffer: &nodecontroller.BufferInfo{Namespace: "ns", Name: "new-buffer"}},
			},
			expectErr: false,
			expectedNodeStates: map[string]csn.NodeState{
				"node-1": csn.NodeStateChilling,
			},
			expectedNodeBuffers: map[string]string{
				"node-1": "ns/new-buffer",
			},
		},
		{
			name: "Buffer assignment stays if the node controller assignment is empty", // I observed this during startup.
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling, withBufferAssignmentMutator("ns/old-buffer")),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateChilling, Buffer: nil},
			},
			expectErr: false,
			expectedNodeStates: map[string]csn.NodeState{
				"node-1": csn.NodeStateChilling,
			},
			expectedNodeBuffers: map[string]string{
				"node-1": "ns/old-buffer",
			},
		},
		{
			name: "Buffer assignment stays if node not in node controller",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling, withBufferAssignmentMutator("ns/buffer")),
			},
			csnNodes:  []nodecontroller.CSNNode{},
			expectErr: false,
			expectedNodeStates: map[string]csn.NodeState{
				"node-1": csn.NodeStateChilling,
			},
			expectedNodeBuffers: map[string]string{
				"node-1": "ns/buffer",
			},
		},
		{
			name: "Buffer assignment is removed on consumed state",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling, withBufferAssignmentMutator("ns/buffer")),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateConsumed, Buffer: &nodecontroller.BufferInfo{Namespace: "ns", Name: "buffer"}},
			},
			expectErr: false,
			expectedNodeStates: map[string]csn.NodeState{
				"node-1": csn.NodeStateConsumed,
			},
			expectedNodeBuffers: map[string]string{
				"node-1": "",
			},
		},
		{
			name: "Unknown buffer assignment is added when it is unknown",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateChilling},
			},
			expectErr: false,
			expectedNodeStates: map[string]csn.NodeState{
				"node-1": csn.NodeStateChilling,
			},
			expectedNodeBuffers: map[string]string{
				"node-1": bufferAssignmentUnknown,
			},
		},
		{
			name: "Upcoming nodes in CSN migs are marked as chilling if templateNodeInfo experiment flag is disabled",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "test-node", csn.NodeStateConsumed, upcomingMutator),
			},
			mig:                          csnMig,
			templateNodeInfoFlagDisabled: true,
			expectedNodeStates: map[string]csn.NodeState{
				"test-node": csn.NodeStateChilling,
			},
			expectedNodeBuffers: map[string]string{
				"test-node": bufferAssignmentUnknown,
			},
		},
		{
			// Reason: It is fixed in nodeInfoProvider and this case will never happen if that flag is enabled.
			name: "Upcoming nodes in CSN migs are NOT marked as chilling if templateNodeInfo experiment flag is enabled",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "test-node", csn.NodeStateConsumed, upcomingMutator),
			},
			mig: csnMig,
			expectedNodeStates: map[string]csn.NodeState{
				"test-node": csn.NodeStateConsumed,
			},
		},
		{
			name: "Consumed node not changed if nil annotations",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "test-node", csn.NodeStateConsumed, withAnnotationMutator(nil)),
			},
			mig: csnMig,
			expectedNodeStates: map[string]csn.NodeState{
				"test-node": csn.NodeStateConsumed,
			},
		},
		{
			name: "Consumed node not changed if empty annotations",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "test-node", csn.NodeStateConsumed, withAnnotationMutator(make(map[string]string))),
			},
			mig: csnMig,
			expectedNodeStates: map[string]csn.NodeState{
				"test-node": csn.NodeStateConsumed,
			},
		},
		{
			name: "Consumed node not changed if incorrect annotation",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "test-node", csn.NodeStateConsumed, withAnnotationMutator(map[string]string{
					annotations.NodeUpcomingAnnotation: "weird-value",
				})),
			},
			mig: csnMig,
			expectedNodeStates: map[string]csn.NodeState{
				"test-node": csn.NodeStateConsumed,
			},
		},
		{
			name: "Consumed node not changed if upcoming set to false",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "test-node", csn.NodeStateConsumed, withAnnotationMutator(map[string]string{
					annotations.NodeUpcomingAnnotation: "false",
				})),
			},
			mig: csnMig,
			expectedNodeStates: map[string]csn.NodeState{
				"test-node": csn.NodeStateConsumed,
			},
		},
		{
			name: "Consumed node not changed if mig error",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "test-node", csn.NodeStateConsumed, upcomingMutator),
			},
			migErr: errors.New("some mig error"),
			expectedNodeStates: map[string]csn.NodeState{
				"test-node": csn.NodeStateConsumed,
			},
		},
		{
			name: "Consumed node not changed if mig without spec",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "test-node", csn.NodeStateConsumed, upcomingMutator),
			},
			mig: gke.NewTestGkeMigBuilder().SetSpec(nil).Build(),
			expectedNodeStates: map[string]csn.NodeState{
				"test-node": csn.NodeStateConsumed,
			},
		},
		{
			name: "Consumed node not changed if mig with nil Labels",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "test-node", csn.NodeStateConsumed, upcomingMutator),
			},
			mig: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{Labels: nil}).Build(),
			expectedNodeStates: map[string]csn.NodeState{
				"test-node": csn.NodeStateConsumed,
			},
		},
		{
			name: "Consumed node not changed if mig with empty Labels",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "test-node", csn.NodeStateConsumed, upcomingMutator),
			},
			mig: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{Labels: make(map[string]string)}).Build(),
			expectedNodeStates: map[string]csn.NodeState{
				"test-node": csn.NodeStateConsumed,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			clusterSnapshot := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
			for _, node := range tc.initialNodes {
				nodeInfo := framework.NewNodeInfo(node, nil)
				err := clusterSnapshot.AddNodeInfo(nodeInfo)
				assert.NoError(t, err)
			}
			autoscalingContext := &context.AutoscalingContext{
				ClusterSnapshot:      clusterSnapshot,
				ClusterStateRegistry: clusterstate.NewClusterStateRegistry(nil, nil, nil, nil, nil),
			}
			mockController := nodecontrollertesting.NewMockCSNNodeController(tc.csnNodes)
			mockController.SetListError(tc.listErr)

			mockCloudProvider := &gke.GkeCloudProviderMock{}
			mockCloudProvider.On("GkeMigForNode", mock.Anything).Return(tc.mig, tc.migErr)

			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.ColdStandbyNodesProcessTemplateNodeInfosFlag: !tc.templateNodeInfoFlagDisabled,
			}, nil)

			processor := NewNodeReconciliationProcessor(mockController, mockCloudProvider, experimentsManager)

			err := processor.Preprocess(autoscalingContext)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			allNodes, err := autoscalingContext.ClusterSnapshot.ListNodeInfos()
			assert.NoError(t, err)
			assert.Len(t, allNodes, len(tc.expectedNodeStates), "Unexpected number of nodes in snapshot")

			if tc.expectedNodeBuffers == nil {
				tc.expectedNodeBuffers = make(map[string]string)
			}

			actualNodeStates := make(map[string]csn.NodeState)
			for _, nodeInfo := range allNodes {
				node := nodeInfo.Node()
				actualNodeStates[node.Name] = csn.ClassifyNode(node)
				assert.Equal(t, tc.expectedNodeBuffers[node.Name], csn.GetBufferIdFromNode(node), "Unexpected buffer for node %q", node.Name)
				if tc.additionalNodeAssertions != nil {
					tc.additionalNodeAssertions(t, node)
				}
			}
			assert.Equal(t, tc.expectedNodeStates, actualNodeStates, "Unexpected node states")
			mockController.WaitForReconcileCall()
			assert.Equal(t, 1, mockController.GetReconcileCalls())
		})
	}
}

type nodeMutator func(*apiv1.Node) *apiv1.Node

func create8CPUTestNode(t *testing.T, name string, state csn.NodeState, mutators ...nodeMutator) *apiv1.Node {
	node := test.BuildTestNode(name, 8000, 24*GiB)
	node, err := csn.SetNodeAs(node, state)
	assert.NoError(t, err)
	for _, mutator := range mutators {
		node = mutator(node)
	}
	return node
}

func withTaintsMutator(taints ...apiv1.Taint) nodeMutator {
	return func(node *apiv1.Node) *apiv1.Node {
		node.Spec.Taints = append(node.Spec.Taints, taints...)
		return node
	}
}

func withLabelsMutator(labels map[string]string) nodeMutator {
	return func(node *apiv1.Node) *apiv1.Node {
		if node.Labels == nil {
			node.Labels = make(map[string]string)
		}
		maps.Copy(node.Labels, labels)
		return node
	}
}

func withBufferAssignmentMutator(bufferId string) nodeMutator {
	return func(node *apiv1.Node) *apiv1.Node {
		node, _ = assignNodeToBufferForProcessors(node, bufferId)
		return node
	}
}

func withAnnotationMutator(annotations map[string]string) nodeMutator {
	return func(node *apiv1.Node) *apiv1.Node {
		node.Annotations = annotations
		return node
	}
}
