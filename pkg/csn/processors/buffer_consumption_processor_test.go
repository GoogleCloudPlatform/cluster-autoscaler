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
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	autoscalingcontext "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/store"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller"
	nodecontrollertesting "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/testing"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	internalmetrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/annotator"
)

type mockCSNMetrics struct {
	invalidConditions []internalmetrics.CSNInvalidCondition
}

func (m *mockCSNMetrics) SetCSNInvalidCondition(condition internalmetrics.CSNInvalidCondition) {
	m.invalidConditions = append(m.invalidConditions, condition)
}

func withUnhelpableAnnotation() func(*apiv1.Pod) {
	return func(pod *apiv1.Pod) {
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		pod.Annotations[annotator.UnhelpableUntilAnnotation] = annotator.UnhelpableForever
	}
}

func TestBufferConsumptionProcess(t *testing.T) {
	testCases := []struct {
		name                       string
		initialNodes               []*apiv1.Node
		alreadyScheduledPods       map[string][]*apiv1.Pod // Pods already scheduled in clustersnapshot. Maps from node name to list of Pods.
		podsCreatedOutsideCA       []*apiv1.Pod            // Pods created outside of CA that are not captured in the clustersnapshot.
		csnNodes                   []nodecontroller.CSNNode
		nonConsumableNodes         []string
		nodesWithPendingOperations []string
		unschedulablePods          []*apiv1.Pod
		experimentsManager         experiments.Manager
		listErr                    error
		expectErr                  bool
		expectedUnschedulablePods  []string
		expectedAllConsumedNodes   []string
		expectedMetrics            []internalmetrics.CSNInvalidCondition
	}{
		{
			name: "Successful processing with some pods scheduled",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateChilling},
			},
			unschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1000, 1*GiB),
				test.BuildTestPod("p2", 9000, 1*GiB),
			},
			expectErr: false,
			expectedUnschedulablePods: []string{
				"p2",
			},
			expectedAllConsumedNodes: []string{"node-1"},
		},
		{
			name: "Unhelpable pods are filtered out and their relative order is preserved",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateChilling},
			},
			unschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p4", 1000, 1*GiB, withUnhelpableAnnotation()), // Unhelpable
				test.BuildTestPod("p3", 1000, 1*GiB),                             // Helpable and scheduled
				test.BuildTestPod("p2", 1000, 1*GiB, withUnhelpableAnnotation()), // Unhelpable
				test.BuildTestPod("p1", 9000, 1*GiB),                             // Helpable but cannot be scheduled (too large)
			},
			expectErr: false,
			expectedUnschedulablePods: []string{
				"p4", "p2", "p1",
			},
			expectedAllConsumedNodes: []string{"node-1"},
		},
		{
			name: "Mark nodes as consumed if they have scheduled pod blocker for suspension",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling),
				create8CPUTestNode(t, "node-2", csn.NodeStateChilling),
				create8CPUTestNode(t, "node-3", csn.NodeStateChilling),
				create8CPUTestNode(t, "node-4", csn.NodeStateChilling),
			},
			alreadyScheduledPods: map[string][]*apiv1.Pod{
				"node-1": {
					test.BuildTestPod("p1", 1000, 1*GiB),
				},
				"node-2": {
					test.BuildTestPod("p2", 1000, 1*GiB, test.WithDSController()), // DS is not blocker for suspension.
				},
			},
			podsCreatedOutsideCA: []*apiv1.Pod{
				test.BuildTestPod("p3", 1000, 1*GiB, test.WithNodeName("node-3")),
				test.BuildTestPod("p4", 1000, 1*GiB, test.WithDSController(), test.WithNodeName("node-4")), // DS is not blocker for suspension.
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateChilling},
				{Name: "node-2", DesiredState: csn.NodeStateChilling},
				{Name: "node-3", DesiredState: csn.NodeStateChilling},
				{Name: "node-4", DesiredState: csn.NodeStateChilling},
			},
			unschedulablePods:         []*apiv1.Pod{},
			expectErr:                 false,
			expectedUnschedulablePods: []string{},
			expectedAllConsumedNodes:  []string{"node-1", "node-3"},
		},
		{
			name: "Schedule pods on already consumed nodes first",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling),
				create8CPUTestNode(t, "node-2", csn.NodeStateChilling),
				create8CPUTestNode(t, "node-3", csn.NodeStateChilling),
				create8CPUTestNode(t, "node-4", csn.NodeStateChilling),
			},
			alreadyScheduledPods: map[string][]*apiv1.Pod{
				"node-2": {
					test.BuildTestPod("p1", 1000, 1*GiB),
				},
			},
			podsCreatedOutsideCA: []*apiv1.Pod{
				test.BuildTestPod("p2", 1000, 1*GiB, test.WithNodeName("node-4")),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateChilling},
				{Name: "node-2", DesiredState: csn.NodeStateChilling},
				{Name: "node-3", DesiredState: csn.NodeStateChilling},
				{Name: "node-4", DesiredState: csn.NodeStateChilling},
			},
			unschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p3", 6000, 1*GiB),
				test.BuildTestPod("p4", 6000, 1*GiB),
			},
			expectErr:                 false,
			expectedUnschedulablePods: []string{},
			expectedAllConsumedNodes:  []string{"node-2", "node-4"},
		},
		{
			name: "Nodes are not available in CSN nodes",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended),
			},
			csnNodes: []nodecontroller.CSNNode{},
			unschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1000, 1*GiB),
				test.BuildTestPod("p2", 9000, 1*GiB),
			},
			expectErr: false,
			expectedUnschedulablePods: []string{
				"p1", "p2",
			},
			expectedAllConsumedNodes: []string{},
		},
		{
			name:         "Nodes is not available in the cluster snapshot",
			initialNodes: []*apiv1.Node{},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateChilling},
			},
			unschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1000, 1*GiB),
				test.BuildTestPod("p2", 9000, 1*GiB),
			},
			expectErr: false,
			expectedUnschedulablePods: []string{
				"p1", "p2",
			},
			expectedAllConsumedNodes: []string{},
		},
		{
			name: "Node filters are used correctly",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended),
				create8CPUTestNode(t, "node-2", csn.NodeStateSuspended),
				create8CPUTestNode(t, "node-3", csn.NodeStateSuspended),
				create8CPUTestNode(t, "node-4", csn.NodeStateSuspended),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateSuspended},
				{Name: "node-2", DesiredState: csn.NodeStateSuspended},
				{Name: "node-3", DesiredState: csn.NodeStateSuspended},
				{Name: "node-4", DesiredState: csn.NodeStateSuspended},
			},
			nodesWithPendingOperations: []string{"node-2", "node-3"},
			unschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p1", 6000, 1*GiB),
				test.BuildTestPod("p2", 6000, 1*GiB),
			},
			expectErr:                 false,
			expectedUnschedulablePods: []string{},
			expectedAllConsumedNodes:  []string{"node-1", "node-4"},
		},
		{
			name: "Successful prioritizing chilling nodes",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended),
				create8CPUTestNode(t, "node-2", csn.NodeStateSuspended),
				create8CPUTestNode(t, "node-3", csn.NodeStateChilling),
				create8CPUTestNode(t, "node-4", csn.NodeStateSuspended),
				create8CPUTestNode(t, "node-5", csn.NodeStateSuspended),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateSuspended},
				{Name: "node-2", DesiredState: csn.NodeStateSuspended},
				{Name: "node-3", DesiredState: csn.NodeStateChilling},
				{Name: "node-4", DesiredState: csn.NodeStateSuspended},
				{Name: "node-5", DesiredState: csn.NodeStateSuspended},
			},
			unschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1000, 1*GiB),
				test.BuildTestPod("p2", 1000, 1*GiB),
			},
			expectErr:                 false,
			expectedUnschedulablePods: []string{},
			expectedAllConsumedNodes:  []string{"node-3"},
		},
		{
			name: "Schedule on Suspended nodes after chilling nodes",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateConsumed),
				create8CPUTestNode(t, "node-2", csn.NodeStateSuspended),
				create8CPUTestNode(t, "node-3", csn.NodeStateChilling),
				create8CPUTestNode(t, "node-4", csn.NodeStateConsumed),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateConsumed},
				{Name: "node-2", DesiredState: csn.NodeStateSuspended},
				{Name: "node-3", DesiredState: csn.NodeStateChilling},
				{Name: "node-4", DesiredState: csn.NodeStateConsumed},
			},
			unschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p1", 6000, 1*GiB),
				test.BuildTestPod("p2", 6000, 1*GiB),
			},
			expectErr:                 false,
			expectedUnschedulablePods: []string{},
			expectedAllConsumedNodes:  []string{"node-1", "node-2", "node-3", "node-4"},
		},
		{
			name: "Schedule pods on node with assigned buffer",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling,
					withBufferAssignmentMutator("ns/buffer")),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateChilling},
			},
			unschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1000, 1*GiB),
			},
			expectErr:                 false,
			expectedUnschedulablePods: []string{},
			expectedAllConsumedNodes:  []string{"node-1"},
		},
		{
			name: "Error from nodeController.List",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling),
			},
			unschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p1", 6000, 1*GiB),
			},
			listErr:                   errors.New("can't list nodes"),
			expectErr:                 false,
			expectedUnschedulablePods: []string{"p1"},
			expectedAllConsumedNodes:  nil,
			expectedMetrics: []internalmetrics.CSNInvalidCondition{
				internalmetrics.CSNBufferConsumptionProcessorError,
			},
		},
		{
			name: "Some chilling nodes are not consumable",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling),
				create8CPUTestNode(t, "node-2", csn.NodeStateChilling),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateChilling},
				{Name: "node-2", DesiredState: csn.NodeStateChilling},
			},
			nonConsumableNodes: []string{"node-1"},
			unschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p1", 6000, 1*GiB, test.WithNodeName("node-1")),
				test.BuildTestPod("p2", 6000, 1*GiB, test.WithNodeName("node-2")),
			},
			expectErr:                 false,
			expectedUnschedulablePods: []string{}, // Pod p1 not evicted as the node wasn't initially in suspended state.
			expectedAllConsumedNodes:  []string{"node-2"},
		},
		{
			name: "Some suspended nodes are not consumable",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended),
				create8CPUTestNode(t, "node-2", csn.NodeStateSuspended),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateSuspended},
				{Name: "node-2", DesiredState: csn.NodeStateSuspended},
			},
			nonConsumableNodes: []string{"node-1"},
			unschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p1", 6000, 1*GiB, test.WithNodeName("node-1")),
				test.BuildTestPod("p2", 6000, 1*GiB, test.WithNodeName("node-2")),
			},
			expectErr:                 false,
			expectedUnschedulablePods: []string{"p1"}, // Pod p1 evicted as it couldn't be scheduled on the non-consumable node and it was initially in suspended state.
			expectedAllConsumedNodes:  []string{"node-2"},
		},
		{
			name: "Mark suspended nodes as consumed if they have pods and flag is enabled (default)",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling),
				create8CPUTestNode(t, "node-2", csn.NodeStateSuspended),
			},
			podsCreatedOutsideCA: []*apiv1.Pod{
				test.BuildTestPod("p1", 1000, 1*GiB, test.WithNodeName("node-2")),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateChilling},
				{Name: "node-2", DesiredState: csn.NodeStateSuspended},
			},
			experimentsManager:        experiments.NewMockManager(experiments.ColdStandbyNodesCheckPodsOnSuspendedNodes),
			expectErr:                 false,
			expectedUnschedulablePods: []string{},
			expectedAllConsumedNodes:  []string{"node-2"},
			expectedMetrics:           []internalmetrics.CSNInvalidCondition{internalmetrics.SuspendedNodeWithBlockingPods},
		},
		{
			name: "Don't mark suspended nodes as consumed if they have pods and flag is disabled",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling),
				create8CPUTestNode(t, "node-2", csn.NodeStateSuspended),
			},
			podsCreatedOutsideCA: []*apiv1.Pod{
				test.BuildTestPod("p1", 1000, 1*GiB, test.WithNodeName("node-2")),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateChilling},
				{Name: "node-2", DesiredState: csn.NodeStateSuspended},
			},
			experimentsManager:        experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{experiments.ColdStandbyNodesCheckPodsOnSuspendedNodes: false}, map[string]string{}),
			expectErr:                 false,
			expectedUnschedulablePods: []string{},
			expectedAllConsumedNodes:  []string{},
		},
		{
			name: "Already consumed node not in node controller is available for scheduling",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling),
			},
			alreadyScheduledPods: map[string][]*apiv1.Pod{
				"node-1": {
					test.BuildTestPod("p-existing", 1000, 1*GiB),
				},
			},
			csnNodes: []nodecontroller.CSNNode{},
			unschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p-new", 1000, 1*GiB),
			},
			expectErr:                 false,
			expectedUnschedulablePods: []string{}, // Pods are scheduled successfully.
			expectedAllConsumedNodes:  []string{}, // No node consumed because no node is in CSN node controller.
		},
		{
			name: "Chilling node not part of node controller is available for scheduling",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling),
			},
			csnNodes: []nodecontroller.CSNNode{},
			unschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1000, 1*GiB),
			},
			expectErr:                 false,
			expectedUnschedulablePods: []string{}, // Pods are scheduled successfully.
			expectedAllConsumedNodes:  []string{}, // No node consumed because no node is in CSN node controller.
		},
		{
			name: "Suspended node not part of node controller is NOT available for scheduling",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended),
			},
			csnNodes: []nodecontroller.CSNNode{},
			unschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1000, 1*GiB),
			},
			expectErr: false,
			expectedUnschedulablePods: []string{
				"p1", // Pods couldn't get scheduled due to the nodeController filter.
			},
			expectedAllConsumedNodes: []string{},
		},
		{
			name: "Metric reported for suspended node with blocking pods",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended),
			},
			podsCreatedOutsideCA: []*apiv1.Pod{
				test.BuildTestPod("p1", 1000, 1*GiB, test.WithNodeName("node-1")),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateSuspended},
			},
			experimentsManager:        experiments.NewMockManager(experiments.ColdStandbyNodesCheckPodsOnSuspendedNodes),
			expectErr:                 false,
			expectedUnschedulablePods: []string{},
			expectedAllConsumedNodes:  []string{"node-1"},
			expectedMetrics:           []internalmetrics.CSNInvalidCondition{internalmetrics.SuspendedNodeWithBlockingPods},
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
			for nodeName, pods := range tc.alreadyScheduledPods {
				for _, pod := range pods {
					err := clusterSnapshot.ForceAddPod(pod, nodeName)
					assert.NoError(t, err)
				}
			}
			podLister := kubernetes.NewTestPodLister(tc.podsCreatedOutsideCA)
			nodeLister := kubernetes.NewTestNodeLister(tc.initialNodes)
			autoscalingCtx := &autoscalingcontext.AutoscalingContext{
				ClusterSnapshot:      clusterSnapshot,
				ClusterStateRegistry: clusterstate.NewClusterStateRegistry(nil, nil, nil, nil, nil),
				AutoscalingKubeClients: autoscalingcontext.AutoscalingKubeClients{
					ListerRegistry: kubernetes.NewListerRegistry(nodeLister, nil, podLister, nil, nil, nil, nil, nil, nil),
				},
			}

			mockController := nodecontrollertesting.NewMockCSNNodeController(tc.csnNodes)
			mockController.MarkAsHasPendingOperations(tc.nodesWithPendingOperations)
			mockController.SetNonConsumableNodes(tc.nonConsumableNodes)
			mockController.SetListError(tc.listErr)

			experimentsManager := tc.experimentsManager
			if experimentsManager == nil {
				experimentsManager = experiments.NewMockManager()
			}
			mockMetrics := &mockCSNMetrics{}
			processor := NewBufferConsumptionProcessor(mockController, experimentsManager)
			processor.metrics = mockMetrics

			remainingPods, err := processor.Process(autoscalingCtx, tc.unschedulablePods)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.ElementsMatch(t, tc.expectedAllConsumedNodes, mockController.NodesWithState(csn.NodeStateConsumed), "Unexpected consumed nodes")
			assert.ElementsMatch(t, tc.expectedMetrics, mockMetrics.invalidConditions, "Unexpected metrics")

			actualUnschedulablePodNames := make([]string, 0, len(remainingPods))
			for _, pod := range remainingPods {
				actualUnschedulablePodNames = append(actualUnschedulablePodNames, pod.Name)
			}
			assert.Equal(t, tc.expectedUnschedulablePods, actualUnschedulablePodNames, "Unexpected unschedulable pods") // Order matters here.

			for _, expectedNode := range tc.expectedAllConsumedNodes {
				ni, err := autoscalingCtx.ClusterSnapshot.GetNodeInfo(expectedNode)
				assert.NoError(t, err)
				node := ni.Node()
				assert.Equal(t, csn.NodeStateConsumed, csn.ClassifyNode(node))
			}
		})
	}
}
