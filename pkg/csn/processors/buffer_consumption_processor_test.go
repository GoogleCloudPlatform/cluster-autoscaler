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
	"context"
	"errors"
	"math"
	"testing"

	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller"
	nodecontrollertesting "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/testing"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	internalmetrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/annotator"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate"
	autoscalingcontext "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/store"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

type mockCSNMetrics struct {
	invalidConditions []internalmetrics.CSNInvalidCondition
	consideredPods    map[internalmetrics.CSNConsideredPodsKey]int
}

func (m *mockCSNMetrics) SetCSNInvalidCondition(condition internalmetrics.CSNInvalidCondition) {
	m.invalidConditions = append(m.invalidConditions, condition)
}

func (m *mockCSNMetrics) UpdateCSNConsideredPods(counts map[internalmetrics.CSNConsideredPodsKey]int) {
	m.consideredPods = counts
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
		backedOffNodes             []string
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
			name: "Backed-off suspended nodes are filtered out by WithoutBackedOffSuspendedFilter and not consumed",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended),
				create8CPUTestNode(t, "node-2", csn.NodeStateChilling),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateSuspended},
				{Name: "node-2", DesiredState: csn.NodeStateChilling},
			},
			backedOffNodes: []string{"node-1"},
			unschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1000, 1*GiB),
			},
			expectErr:                 false,
			expectedUnschedulablePods: []string{},
			expectedAllConsumedNodes:  []string{"node-2"},
		},
		{
			name: "Backed-off chilling nodes are NOT filtered out by WithoutBackedOffSuspendedFilter",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateChilling},
			},
			backedOffNodes: []string{"node-1"},
			unschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1000, 1*GiB),
			},
			expectErr:                 false,
			expectedUnschedulablePods: []string{},
			expectedAllConsumedNodes:  []string{"node-1"},
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
		{
			name: "Pod age fallback disabled: old pod is scheduled on suspended node",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateSuspended},
			},
			unschedulablePods: []*apiv1.Pod{
				buildOldTestPod("p1", 1000, 1*GiB),
			},
			experimentsManager:        podAgeFallbackExperimentManager(false),
			expectErr:                 false,
			expectedUnschedulablePods: []string{},
			expectedAllConsumedNodes:  []string{"node-1"},
		},
		{
			name: "Pod age fallback enabled: old pod is excluded from suspended node and falls back to NAP",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateSuspended},
			},
			unschedulablePods: []*apiv1.Pod{
				buildOldTestPod("p1", 1000, 1*GiB),
			},
			experimentsManager:        podAgeFallbackExperimentManager(true),
			expectErr:                 false,
			expectedUnschedulablePods: []string{"p1"},
			expectedAllConsumedNodes:  []string{},
		},
		{
			name: "Pod age fallback enabled: old pod is scheduled on chilling node instead of suspended node",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended),
				create8CPUTestNode(t, "node-2", csn.NodeStateChilling),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateSuspended},
				{Name: "node-2", DesiredState: csn.NodeStateChilling},
			},
			unschedulablePods: []*apiv1.Pod{
				buildOldTestPod("p1", 1000, 1*GiB),
			},
			experimentsManager:        podAgeFallbackExperimentManager(true),
			expectErr:                 false,
			expectedUnschedulablePods: []string{},
			expectedAllConsumedNodes:  []string{"node-2"},
		},
		{
			name: "Pod age fallback enabled: normal pod goes to suspended node, old pod goes to chilling node",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended),
				create8CPUTestNode(t, "node-2", csn.NodeStateChilling),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateSuspended},
				{Name: "node-2", DesiredState: csn.NodeStateChilling},
			},
			unschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("normal-pod", 8000, 1*GiB),
				buildOldTestPod("old-pod", 8000, 1*GiB),
			},
			experimentsManager:        podAgeFallbackExperimentManager(true),
			expectErr:                 false,
			expectedUnschedulablePods: []string{},
			expectedAllConsumedNodes:  []string{"node-1", "node-2"},
		},
		{
			name: "Pod age fallback enabled: old pod is scheduled on already consumed suspended node",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended),
			},
			podsCreatedOutsideCA: []*apiv1.Pod{
				test.BuildTestPod("existing-pod", 100, 100, test.WithNodeName("node-1")),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateSuspended},
			},
			unschedulablePods: []*apiv1.Pod{
				buildOldTestPod("old-pod", 1000, 1*GiB),
			},
			experimentsManager:        podAgeFallbackExperimentManager(true),
			expectErr:                 false,
			expectedUnschedulablePods: []string{},
			expectedAllConsumedNodes:  []string{"node-1"},
			expectedMetrics:           []internalmetrics.CSNInvalidCondition{internalmetrics.SuspendedNodeWithBlockingPods},
		},
		{
			name: "Pod age fallback enabled with custom giraffe threshold (5m): pod older than 5m is excluded from suspended node",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateSuspended},
			},
			unschedulablePods: []*apiv1.Pod{
				func() *apiv1.Pod {
					pod := test.BuildTestPod("p1", 1000, 1*GiB)
					pod.CreationTimestamp = metav1.NewTime(time.Now().Add(-6 * time.Minute))
					return pod
				}(),
			},
			experimentsManager:        podAgeFallbackExperimentManager(true, "300"),
			expectErr:                 false,
			expectedUnschedulablePods: []string{"p1"},
			expectedAllConsumedNodes:  []string{},
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
			mockController.MarkAsBackedOff(tc.backedOffNodes)
			for _, n := range tc.initialNodes {
				mockController.SetCurrentState(n.Name, csn.ClassifyNode(n))
			}
			mockController.SetNonConsumableNodes(tc.nonConsumableNodes)
			mockController.SetListError(tc.listErr)

			experimentsManager := tc.experimentsManager
			if experimentsManager == nil {
				experimentsManager = experiments.NewMockManager()
			}
			mockMetrics := &mockCSNMetrics{}
			processor := NewBufferConsumptionProcessor(mockController, experimentsManager)
			processor.metrics = mockMetrics

			remainingPods, err := processor.Process(context.TODO(), autoscalingCtx, tc.unschedulablePods)

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

func TestBufferConsumptionProcess_ConsideredPodsMetric(t *testing.T) {
	testCases := []struct {
		name                   string
		initialNodes           []*apiv1.Node
		csnNodes               []nodecontroller.CSNNode
		unschedulablePods      []*apiv1.Pod
		experimentsManager     experiments.Manager
		expectedConsideredPods map[internalmetrics.CSNConsideredPodsKey]int
	}{
		{
			name: "CSN considered pods metric recorded correctly",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateChilling},
			},
			unschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p-unhelpable", 100, 100, withUnhelpableAnnotation()),
				test.BuildTestPod("p-scheduled", 1000, 1*GiB),
				test.BuildTestPod("p-unschedulable", 10000, 10*GiB),
				buildOldTestPod("p-old-unschedulable", 10000, 10*GiB),
			},
			experimentsManager: podAgeFallbackExperimentManager(true),
			expectedConsideredPods: map[internalmetrics.CSNConsideredPodsKey]int{
				{Status: internalmetrics.CSNPodUnhelpable, IsOld: false}:    1,
				{Status: internalmetrics.CSNPodScheduled, IsOld: false}:     1,
				{Status: internalmetrics.CSNPodUnschedulable, IsOld: false}: 1,
				{Status: internalmetrics.CSNPodUnschedulable, IsOld: true}:  1,
			},
		},
		{
			name: "CSN considered pods metric not recorded when pod age fallback disabled",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling),
			},
			csnNodes: []nodecontroller.CSNNode{
				{Name: "node-1", DesiredState: csn.NodeStateChilling},
			},
			unschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p-unhelpable", 100, 100, withUnhelpableAnnotation()),
				test.BuildTestPod("p-scheduled", 1000, 1*GiB),
				test.BuildTestPod("p-unschedulable", 10000, 10*GiB),
				buildOldTestPod("p-old-unschedulable", 10000, 10*GiB),
			},
			experimentsManager:     podAgeFallbackExperimentManager(false),
			expectedConsideredPods: nil,
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
			podLister := kubernetes.NewTestPodLister(nil)
			nodeLister := kubernetes.NewTestNodeLister(tc.initialNodes)
			autoscalingCtx := &autoscalingcontext.AutoscalingContext{
				ClusterSnapshot:      clusterSnapshot,
				ClusterStateRegistry: clusterstate.NewClusterStateRegistry(nil, nil, nil, nil, nil),
				AutoscalingKubeClients: autoscalingcontext.AutoscalingKubeClients{
					ListerRegistry: kubernetes.NewListerRegistry(nodeLister, nil, podLister, nil, nil, nil, nil, nil, nil),
				},
			}

			mockController := nodecontrollertesting.NewMockCSNNodeController(tc.csnNodes)
			for _, n := range tc.initialNodes {
				mockController.SetCurrentState(n.Name, csn.ClassifyNode(n))
			}

			experimentsManager := tc.experimentsManager
			if experimentsManager == nil {
				experimentsManager = experiments.NewMockManager()
			}
			mockMetrics := &mockCSNMetrics{}
			processor := NewBufferConsumptionProcessor(mockController, experimentsManager)
			processor.metrics = mockMetrics

			_, err := processor.Process(context.TODO(), autoscalingCtx, tc.unschedulablePods)
			assert.NoError(t, err)

			if len(tc.expectedConsideredPods) == 0 {
				assert.Empty(t, mockMetrics.consideredPods, "Expected considered pods to be empty")
			} else {
				assert.Equal(t, tc.expectedConsideredPods, mockMetrics.consideredPods, "Unexpected considered pods")
			}
		})
	}
}

func buildOldTestPod(name string, cpu int64, memory int64) *apiv1.Pod {
	pod := test.BuildTestPod(name, cpu, memory)
	pod.CreationTimestamp = metav1.NewTime(time.Now().Add(-15 * time.Minute))
	return pod
}

func podAgeFallbackExperimentManager(enabled bool, thresholdSeconds ...string) experiments.Manager {
	stringFlags := map[string]string{}
	if len(thresholdSeconds) > 0 && thresholdSeconds[0] != "" {
		stringFlags[experiments.ColdStandbyNodesPodAgeFallbackThresholdSecondsFlag] = thresholdSeconds[0]
	}
	return experiments.NewMockManagerWithOptions(
		version.Version{},
		map[string]bool{experiments.ColdStandbyNodesBackoffMinCAVersionFlag: enabled},
		stringFlags,
	)
}

func TestIsPodTooOld(t *testing.T) {
	now := time.Now()
	testCases := []struct {
		name     string
		pod      *apiv1.Pod
		expected bool
	}{
		{
			name:     "Pod without creation timestamp is not too old",
			pod:      test.BuildTestPod("p1", 100, 100),
			expected: false,
		},
		{
			name: "Pod created exactly 10 minutes ago is not too old",
			pod: func() *apiv1.Pod {
				p := test.BuildTestPod("p2", 100, 100)
				p.CreationTimestamp = metav1.NewTime(now.Add(-10 * time.Minute))
				return p
			}(),
			expected: false,
		},
		{
			name: "Pod created more than 10 minutes ago is too old",
			pod: func() *apiv1.Pod {
				p := test.BuildTestPod("p3", 100, 100)
				p.CreationTimestamp = metav1.NewTime(now.Add(-10*time.Minute - 1*time.Second))
				return p
			}(),
			expected: true,
		},
		{
			name: "Pod created less than 10 minutes ago is not too old",
			pod: func() *apiv1.Pod {
				p := test.BuildTestPod("p4", 100, 100)
				p.CreationTimestamp = metav1.NewTime(now.Add(-9 * time.Minute))
				return p
			}(),
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, isPodTooOld(tc.pod, now, 10*time.Minute))
		})
	}
}

func TestPodAgeFallbackThreshold(t *testing.T) {
	testCases := []struct {
		name     string
		em       experiments.Manager
		expected time.Duration
	}{
		{
			name:     "Nil experiments manager returns MaxInt64",
			em:       nil,
			expected: math.MaxInt64,
		},
		{
			name:     "Disabled experiment returns MaxInt64",
			em:       podAgeFallbackExperimentManager(false),
			expected: math.MaxInt64,
		},
		{
			name:     "Unconfigured flag returns default threshold",
			em:       podAgeFallbackExperimentManager(true),
			expected: 10 * time.Minute,
		},
		{
			name:     "Valid configured flag returns configured duration",
			em:       podAgeFallbackExperimentManager(true, "300"),
			expected: 5 * time.Minute,
		},
		{
			name:     "Zero flag falls back to default threshold",
			em:       podAgeFallbackExperimentManager(true, "0"),
			expected: 10 * time.Minute,
		},
		{
			name:     "Negative flag falls back to default threshold",
			em:       podAgeFallbackExperimentManager(true, "-60"),
			expected: 10 * time.Minute,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := NewBufferConsumptionProcessor(nil, tc.em)
			assert.Equal(t, tc.expected, p.podAgeFallbackThreshold())
		})
	}
}
