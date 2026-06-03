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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/store"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/scheduling"
	"k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
)

func TestBasicPriorityFilter(t *testing.T) {
	testCases := []struct {
		description string
		filter      priorityFilter
		node        *apiv1.Node
		expected    bool
	}{
		{
			description: "isChillingFilter should return true for chilling nodes",
			filter:      isChillingFilter,
			node:        create8CPUTestNode(t, "test-node", csn.NodeStateChilling),
			expected:    true,
		},
		{
			description: "isChillingFilter should return false for suspended nodes",
			filter:      isChillingFilter,
			node:        create8CPUTestNode(t, "test-node", csn.NodeStateSuspended),
			expected:    false,
		},
		{
			description: "isChillingFilter should return false for consumed nodes",
			filter:      isChillingFilter,
			node:        create8CPUTestNode(t, "test-node", csn.NodeStateConsumed),
			expected:    false,
		},
		{
			description: "isSuspendedFilter should return true for suspended nodes",
			filter:      isSuspendedFilter,
			node:        create8CPUTestNode(t, "test-node", csn.NodeStateSuspended),
			expected:    true,
		},
		{
			description: "isSuspendedFilter should return false for chilling nodes",
			filter:      isSuspendedFilter,
			node:        create8CPUTestNode(t, "test-node", csn.NodeStateChilling),
			expected:    false,
		},
		{
			description: "isSuspendedFilter should return false for consumed nodes",
			filter:      isSuspendedFilter,
			node:        create8CPUTestNode(t, "test-node", csn.NodeStateConsumed),
			expected:    false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			got := tc.filter(framework.NewNodeInfo(tc.node, nil))
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestSchedulePodsOnCSNNodes(t *testing.T) {
	testCases := []struct {
		description        string
		pods               []*apiv1.Pod
		nodes              []*apiv1.Node
		options            schedulePodsOnCSNNodesOptions
		priorities         []priorityFilter
		expectedScheduling map[string]string
		expectedErr        bool
	}{
		{
			description: "Schedule pod correctly",
			pods: []*apiv1.Pod{
				test.BuildTestPod("pod-1", 1000, 1*GiB),
				test.BuildTestPod("pod-2", 1000, 1*GiB),
			},
			nodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended),
			},
			priorities: []priorityFilter{
				isSuspendedFilter,
			},
			expectedScheduling: map[string]string{
				"pod-1": "node-1",
				"pod-2": "node-1",
			},
		},
		{
			description: "Schedule pod on suspended node only",
			pods: []*apiv1.Pod{
				test.BuildTestPod("pod-1", 1000, 1*GiB),
				test.BuildTestPod("pod-2", 1000, 1*GiB),
			},
			nodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling),
				create8CPUTestNode(t, "node-2", csn.NodeStateChilling),
				create8CPUTestNode(t, "node-3", csn.NodeStateChilling),
				create8CPUTestNode(t, "node-4", csn.NodeStateSuspended),
				create8CPUTestNode(t, "node-5", csn.NodeStateChilling),
				create8CPUTestNode(t, "node-6", csn.NodeStateChilling),
				create8CPUTestNode(t, "node-7", csn.NodeStateChilling),
			},
			priorities: []priorityFilter{
				isSuspendedFilter,
			},
			expectedScheduling: map[string]string{
				"pod-1": "node-4",
				"pod-2": "node-4",
			},
		},
		{
			description: "Schedule pod on chilling node only",
			pods: []*apiv1.Pod{
				test.BuildTestPod("pod-1", 1000, 1*GiB),
				test.BuildTestPod("pod-2", 1000, 1*GiB),
			},
			nodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling),
				create8CPUTestNode(t, "node-2", csn.NodeStateSuspended),
				create8CPUTestNode(t, "node-3", csn.NodeStateSuspended),
				create8CPUTestNode(t, "node-4", csn.NodeStateSuspended),
				create8CPUTestNode(t, "node-5", csn.NodeStateSuspended),
			},
			priorities: []priorityFilter{
				isChillingFilter,
			},
			expectedScheduling: map[string]string{
				"pod-1": "node-1",
				"pod-2": "node-1",
			},
		},
		{
			description: "Schedule pod on suspended first and then chilling nodes",
			pods: []*apiv1.Pod{
				test.BuildTestPod("pod-1", 1000, 1*GiB),
				test.BuildTestPod("pod-2", 1000, 1*GiB),
			},
			nodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling),
				create8CPUTestNode(t, "node-2", csn.NodeStateSuspended),
			},
			priorities: []priorityFilter{
				isSuspendedFilter,
				isChillingFilter,
			},
			expectedScheduling: map[string]string{
				"pod-1": "node-2",
				"pod-2": "node-2",
			},
		},
		{
			description: "Some pods cannot be scheduled",
			pods: []*apiv1.Pod{
				test.BuildTestPod("pod-1", 1000, 1000*GiB),
				test.BuildTestPod("pod-2", 1000, 1*GiB),
			},
			nodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling),
				create8CPUTestNode(t, "node-2", csn.NodeStateSuspended),
			},
			priorities: []priorityFilter{
				isSuspendedFilter,
				isChillingFilter,
			},
			expectedScheduling: map[string]string{
				"pod-2": "node-2",
			},
		},
		{
			description: "No nodes available",
			pods: []*apiv1.Pod{
				test.BuildTestPod("pod-1", 1000, 1*GiB),
			},
			nodes:              []*apiv1.Node{},
			priorities:         []priorityFilter{},
			expectedScheduling: map[string]string{},
		},
		{
			description:        "No pods to schedule",
			pods:               []*apiv1.Pod{},
			nodes:              []*apiv1.Node{},
			priorities:         []priorityFilter{},
			expectedScheduling: map[string]string{},
		},
		{
			description: "Schedule normal pod on node with buffer assignment if ignoreBufferAssignment is true",
			pods: []*apiv1.Pod{
				test.BuildTestPod("pod-1", 1000, 1*GiB),
			},
			nodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling, withBufferAssignmentMutator("ns/buffer")),
			},
			options: schedulePodsOnCSNNodesOptions{
				ignoreBufferAssignment: true,
			},
			priorities: []priorityFilter{
				isChillingFilter,
			},
			expectedScheduling: map[string]string{
				"pod-1": "node-1",
			},
		},
		{
			description: "Do not schedule normal pod on node with buffer assignment if ignoreBufferAssignment is false",
			pods: []*apiv1.Pod{
				test.BuildTestPod("pod-1", 1000, 1*GiB),
			},
			nodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling, withBufferAssignmentMutator("ns/buffer")),
			},
			options: schedulePodsOnCSNNodesOptions{
				ignoreBufferAssignment: false,
			},
			priorities: []priorityFilter{
				isChillingFilter,
			},
			expectedScheduling: map[string]string{},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			sn := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
			for _, node := range tc.nodes {
				nodeInfo := framework.NewNodeInfo(node.DeepCopy(), nil)
				err := sn.AddNodeInfo(nodeInfo)
				assert.NoError(t, err)
			}
			simulator := scheduling.NewHintingSimulator()
			scheduledPods, err := schedulePodsOnCSNNodes(sn, simulator, tc.pods, tc.options, tc.priorities...)
			gotScheduling := map[string]string{}
			for pod, node := range scheduledPods {
				gotScheduling[pod.Name] = node
			}
			if tc.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedScheduling, gotScheduling)
			}

			// Assert that the nodes didn't change. This check exists due to unintuitive Revert behavior in Delta ClusterSnapshot.
			nodeInfos, err := sn.NodeInfos().List()
			assert.NoError(t, err)
			nodes := []*apiv1.Node{}
			for _, ni := range nodeInfos {
				nodes = append(nodes, ni.Node())
			}
			assert.ElementsMatch(t, tc.nodes, nodes)
		})
	}
}

func TestSchedulingDecisionsCaching(t *testing.T) {
	testCases := []struct {
		description                  string
		pods                         []*apiv1.Pod
		nodes                        []*apiv1.Node
		firstLoopFilter              priorityFilter
		secondLoopFilter             priorityFilter
		expectedFirstLoopScheduling  map[string]string
		expectedSecondLoopScheduling map[string]string
	}{
		{
			description: "Scheduling decisions are cached",
			pods: []*apiv1.Pod{
				test.BuildTestPod("pod-1", 1000, 1*GiB),
				test.BuildTestPod("pod-2", 1000, 1*GiB),
			},
			nodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended),
				create8CPUTestNode(t, "node-2", csn.NodeStateSuspended),
				create8CPUTestNode(t, "node-3", csn.NodeStateSuspended),
			},
			firstLoopFilter: func(ni *framework.NodeInfo) bool {
				return ni.Node().Name == "node-2"
			},
			secondLoopFilter: func(_ *framework.NodeInfo) bool {
				return true
			},
			expectedFirstLoopScheduling: map[string]string{
				"pod-1": "node-2",
				"pod-2": "node-2",
			},
			expectedSecondLoopScheduling: map[string]string{
				"pod-1": "node-2",
				"pod-2": "node-2",
			},
		},
		{
			description: "The cached results changes if the node become unschedulable",
			pods: []*apiv1.Pod{
				test.BuildTestPod("pod-1", 1000, 1*GiB),
				test.BuildTestPod("pod-2", 1000, 1*GiB),
			},
			nodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended),
				create8CPUTestNode(t, "node-2", csn.NodeStateSuspended),
				create8CPUTestNode(t, "node-3", csn.NodeStateSuspended),
			},
			firstLoopFilter: func(ni *framework.NodeInfo) bool {
				return ni.Node().Name == "node-2"
			},
			secondLoopFilter: func(ni *framework.NodeInfo) bool {
				return ni.Node().Name == "node-1"
			},
			expectedFirstLoopScheduling: map[string]string{
				"pod-1": "node-2",
				"pod-2": "node-2",
			},
			expectedSecondLoopScheduling: map[string]string{
				"pod-1": "node-1",
				"pod-2": "node-1",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			sn := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
			for _, node := range tc.nodes {
				nodeInfo := framework.NewNodeInfo(node.DeepCopy(), nil)
				err := sn.AddNodeInfo(nodeInfo)
				assert.NoError(t, err)
			}
			simulator := scheduling.NewHintingSimulator()

			// First loop.
			scheduledPods, err := schedulePodsOnCSNNodes(sn, simulator, tc.pods, schedulePodsOnCSNNodesOptions{}, tc.firstLoopFilter)
			assert.NoError(t, err)
			gotScheduling := map[string]string{}
			for pod, node := range scheduledPods {
				gotScheduling[pod.Name] = node
			}
			assert.Equal(t, tc.expectedFirstLoopScheduling, gotScheduling)

			// Second loop.
			scheduledPods, err = schedulePodsOnCSNNodes(sn, simulator, tc.pods, schedulePodsOnCSNNodesOptions{}, tc.secondLoopFilter)
			assert.NoError(t, err)
			gotScheduling = map[string]string{}
			for pod, node := range scheduledPods {
				gotScheduling[pod.Name] = node
			}
			assert.Equal(t, tc.expectedSecondLoopScheduling, gotScheduling)
		})
	}
}

func TestPodsAreNotSpreadDuringScheduling(t *testing.T) {
	nodes := []*apiv1.Node{
		create8CPUTestNode(t, "node-1", csn.NodeStateSuspended),
		create8CPUTestNode(t, "node-2", csn.NodeStateSuspended),
		create8CPUTestNode(t, "node-3", csn.NodeStateSuspended),
	}
	pods := []*apiv1.Pod{
		test.BuildTestPod("pod-1", 1000, 1*GiB),
		test.BuildTestPod("pod-2", 1000, 1*GiB),
		test.BuildTestPod("pod-3", 1000, 1*GiB),
	}
	sn := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
	for _, node := range nodes {
		nodeInfo := framework.NewNodeInfo(node.DeepCopy(), nil)
		err := sn.AddNodeInfo(nodeInfo)
		assert.NoError(t, err)
	}
	simulator := scheduling.NewHintingSimulator()

	scheduledPods, err := schedulePodsOnCSNNodes(sn, simulator, pods, schedulePodsOnCSNNodesOptions{}, isSuspendedFilter)
	assert.NoError(t, err)
	assert.True(t, len(scheduledPods) == len(pods), "All %d pods are expected to be scheduled, actual scheduling: %v", len(pods), scheduledPods)

	scheduledNodes := map[string]bool{}
	gotScheduling := map[string]string{}
	for pod, node := range scheduledPods {
		gotScheduling[pod.Name] = node
		scheduledNodes[node] = true
	}
	assert.True(t, len(scheduledNodes) == 1, "All pods are expected to be scheduled on the same node, but they are scheduled on %d nodes: %v", len(gotScheduling), gotScheduling)
}

func TestMakeCSNNodesSchedulable(t *testing.T) {
	testCases := []struct {
		description   string
		nodes         []*apiv1.Node
		options       schedulePodsOnCSNNodesOptions
		expectedErr   bool
		expectedNodes []*apiv1.Node
	}{
		{
			description: "Suspended CSN node becomes schedulable",
			nodes: []*apiv1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "csn-node-1",
						Labels: map[string]string{
							csn.SoftWorkloadSeparationKey: csn.SoftWorkloadSeparationValue,
						},
					},
					Spec: apiv1.NodeSpec{
						Unschedulable: true,
						Taints: []apiv1.Taint{
							csn.SuspendedTaint,
							{
								Key:    apiv1.TaintNodeUnreachable,
								Effect: apiv1.TaintEffectNoSchedule,
							},
							{
								Key:    apiv1.TaintNodeUnreachable,
								Effect: apiv1.TaintEffectNoExecute,
							},
							{
								Key:    apiv1.TaintNodeUnschedulable,
								Effect: apiv1.TaintEffectNoExecute,
							},
						},
					},
				},
			},
			expectedErr: false,
			expectedNodes: []*apiv1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "csn-node-1",
						Labels: map[string]string{
							csn.SoftWorkloadSeparationKey: csn.SoftWorkloadSeparationValue,
						},
					},
					Spec: apiv1.NodeSpec{
						Unschedulable: false,
						Taints:        []apiv1.Taint{},
					},
				},
			},
		},
		{
			description: "Not suspended but cordoned CSN node still becomes cordoned",
			nodes: []*apiv1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "csn-node-1",
						Labels: map[string]string{
							csn.SoftWorkloadSeparationKey: csn.SoftWorkloadSeparationValue,
						},
					},
					Spec: apiv1.NodeSpec{
						Unschedulable: true,
					},
				},
			},
			expectedErr: false,
			expectedNodes: []*apiv1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "csn-node-1",
						Labels: map[string]string{
							csn.SoftWorkloadSeparationKey: csn.SoftWorkloadSeparationValue,
						},
					},
					Spec: apiv1.NodeSpec{
						Unschedulable: true,
					},
				},
			},
		},
		{
			description: "Not suspended CSN node but tainted with unreachable and unschedulable remains with the taints",
			nodes: []*apiv1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "csn-node-1",
						Labels: map[string]string{
							csn.SoftWorkloadSeparationKey: csn.SoftWorkloadSeparationValue,
						},
					},
					Spec: apiv1.NodeSpec{
						Taints: []apiv1.Taint{
							{
								Key:    apiv1.TaintNodeUnreachable,
								Effect: apiv1.TaintEffectNoSchedule,
							},
							{
								Key:    apiv1.TaintNodeUnreachable,
								Effect: apiv1.TaintEffectNoExecute,
							},
							{
								Key:    apiv1.TaintNodeUnschedulable,
								Effect: apiv1.TaintEffectNoExecute,
							},
						},
					},
				},
			},
			expectedErr: false,
			expectedNodes: []*apiv1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "csn-node-1",
						Labels: map[string]string{
							csn.SoftWorkloadSeparationKey: csn.SoftWorkloadSeparationValue,
						},
					},
					Spec: apiv1.NodeSpec{
						Taints: []apiv1.Taint{
							{
								Key:    apiv1.TaintNodeUnreachable,
								Effect: apiv1.TaintEffectNoSchedule,
							},
							{
								Key:    apiv1.TaintNodeUnreachable,
								Effect: apiv1.TaintEffectNoExecute,
							},
							{
								Key:    apiv1.TaintNodeUnschedulable,
								Effect: apiv1.TaintEffectNoExecute,
							},
						},
					},
				},
			},
		},
		{
			description: "No CSN nodes, no changes",
			nodes: []*apiv1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "regular-node-1",
					},
					Spec: apiv1.NodeSpec{
						Unschedulable: true,
						Taints:        []apiv1.Taint{csn.SuspendedTaint},
					},
				},
			},
			expectedErr: false,
			expectedNodes: []*apiv1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "regular-node-1",
					},
					Spec: apiv1.NodeSpec{
						Unschedulable: true,
						Taints:        []apiv1.Taint{csn.SuspendedTaint},
					},
				},
			},
		},
		{
			description: "Mixed nodes, only CSN node changes",
			nodes: []*apiv1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "csn-node-2",
						Labels: map[string]string{
							csn.SoftWorkloadSeparationKey: csn.SoftWorkloadSeparationValue,
						},
					},
					Spec: apiv1.NodeSpec{
						Unschedulable: true,
						Taints:        []apiv1.Taint{csn.SuspendedTaint},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "regular-node-2",
					},
					Spec: apiv1.NodeSpec{
						Unschedulable: true,
						Taints:        []apiv1.Taint{csn.SuspendedTaint},
					},
				},
			},
			expectedErr: false,
			expectedNodes: []*apiv1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "csn-node-2",
						Labels: map[string]string{
							csn.SoftWorkloadSeparationKey: csn.SoftWorkloadSeparationValue,
						},
					},
					Spec: apiv1.NodeSpec{
						Unschedulable: false,
						Taints:        []apiv1.Taint{},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "regular-node-2",
					},
					Spec: apiv1.NodeSpec{
						Unschedulable: true,
						Taints:        []apiv1.Taint{csn.SuspendedTaint},
					},
				},
			},
		},
		{
			description: "CSN node with buffer assignment and ignoreBufferAssignment is true",
			nodes: []*apiv1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "csn-node-1",
						Labels: map[string]string{
							csn.SoftWorkloadSeparationKey: csn.SoftWorkloadSeparationValue,
							csn.BufferAssignmentKey:       "ns/buffer",
						},
					},
					Spec: apiv1.NodeSpec{
						Unschedulable: true,
						Taints: []apiv1.Taint{
							csn.SuspendedTaint,
							{
								Key:    csn.BufferAssignmentKey,
								Value:  "ns/buffer",
								Effect: apiv1.TaintEffectNoSchedule,
							},
						},
					},
				},
			},
			options: schedulePodsOnCSNNodesOptions{
				ignoreBufferAssignment: true,
			},
			expectedErr: false,
			expectedNodes: []*apiv1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "csn-node-1",
						Labels: map[string]string{
							csn.SoftWorkloadSeparationKey: csn.SoftWorkloadSeparationValue,
						},
					},
					Spec: apiv1.NodeSpec{
						Unschedulable: false,
						Taints:        []apiv1.Taint{},
					},
				},
			},
		},
		{
			description: "CSN node with buffer assignment and ignoreBufferAssignment is false",
			nodes: []*apiv1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "csn-node-1",
						Labels: map[string]string{
							csn.SoftWorkloadSeparationKey: csn.SoftWorkloadSeparationValue,
							csn.BufferAssignmentKey:       "ns/buffer",
						},
					},
					Spec: apiv1.NodeSpec{
						Unschedulable: true,
						Taints: []apiv1.Taint{
							csn.SuspendedTaint,
							{
								Key:    csn.BufferAssignmentKey,
								Value:  "ns/buffer",
								Effect: apiv1.TaintEffectNoSchedule,
							},
						},
					},
				},
			},
			options: schedulePodsOnCSNNodesOptions{
				ignoreBufferAssignment: false,
			},
			expectedErr: false,
			expectedNodes: []*apiv1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "csn-node-1",
						Labels: map[string]string{
							csn.SoftWorkloadSeparationKey: csn.SoftWorkloadSeparationValue,
							csn.BufferAssignmentKey:       "ns/buffer",
						},
					},
					Spec: apiv1.NodeSpec{
						Unschedulable: false,
						Taints: []apiv1.Taint{
							{
								Key:    csn.BufferAssignmentKey,
								Value:  "ns/buffer",
								Effect: apiv1.TaintEffectNoSchedule,
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			nodeInfos := []*framework.NodeInfo{}
			for _, node := range tc.nodes {
				// Deep copy the node to avoid modifications between test cases if the node is reused
				nodeInfos = append(nodeInfos, framework.NewNodeInfo(node.DeepCopy(), nil))
			}

			err := makeCSNNodesSchedulable(nodeInfos, tc.options)

			if tc.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				resultNodes := []*apiv1.Node{}
				for _, ni := range nodeInfos {
					resultNodes = append(resultNodes, ni.Node())
				}
				assert.Equal(t, tc.expectedNodes, resultNodes)
			}
		})
	}
}

func TestSetNodeAsForProcessors(t *testing.T) {
	csnPod := test.BuildTestPod("csn-pod", 1000, 1*GiB)
	csn.MakePodCSN(csnPod, "ns/buffer")

	testCases := []struct {
		description                   string
		node                          *apiv1.Node
		desiredState                  csn.NodeState
		expectedCSNPodSchdulable      bool
		expectedBufferAssignmentExist bool
	}{
		{
			description: "CSN suspended node becomes schedulable for CSN pods",
			node: create8CPUTestNode(t, "node-1", csn.NodeStateSuspended, withBufferAssignmentMutator("ns/buffer"), withTaintsMutator(
				apiv1.Taint{
					Key:    apiv1.TaintNodeUnreachable,
					Effect: apiv1.TaintEffectNoSchedule,
				},
				apiv1.Taint{
					Key:    apiv1.TaintNodeUnreachable,
					Effect: apiv1.TaintEffectNoExecute,
				},
				apiv1.Taint{
					Key:    apiv1.TaintNodeUnschedulable,
					Effect: apiv1.TaintEffectNoSchedule,
				},
			)),
			desiredState:                  csn.NodeStateSuspended,
			expectedCSNPodSchdulable:      true,
			expectedBufferAssignmentExist: true,
		},
		{
			description:                   "Chilling to Suspended",
			node:                          create8CPUTestNode(t, "node-1", csn.NodeStateChilling, withBufferAssignmentMutator("ns/buffer")),
			desiredState:                  csn.NodeStateSuspended,
			expectedCSNPodSchdulable:      true,
			expectedBufferAssignmentExist: true,
		},
		{
			description:                   "Consumed to Suspended",
			node:                          create8CPUTestNode(t, "node-1", csn.NodeStateConsumed, withBufferAssignmentMutator("ns/buffer")),
			desiredState:                  csn.NodeStateSuspended,
			expectedCSNPodSchdulable:      true,
			expectedBufferAssignmentExist: true,
		},
		{
			description:                   "Suspended to Chilling",
			node:                          create8CPUTestNode(t, "node-1", csn.NodeStateSuspended, withBufferAssignmentMutator("ns/buffer")),
			desiredState:                  csn.NodeStateChilling,
			expectedCSNPodSchdulable:      true,
			expectedBufferAssignmentExist: true,
		},
		{
			description:                   "Chilling to Chilling",
			node:                          create8CPUTestNode(t, "node-1", csn.NodeStateChilling, withBufferAssignmentMutator("ns/buffer")),
			desiredState:                  csn.NodeStateChilling,
			expectedCSNPodSchdulable:      true,
			expectedBufferAssignmentExist: true,
		},
		{
			description:                   "Consumed to Chilling",
			node:                          create8CPUTestNode(t, "node-1", csn.NodeStateConsumed, withBufferAssignmentMutator("ns/buffer")),
			desiredState:                  csn.NodeStateChilling,
			expectedCSNPodSchdulable:      true,
			expectedBufferAssignmentExist: true,
		},
		{
			description:                   "Suspended to Consumed",
			node:                          create8CPUTestNode(t, "node-1", csn.NodeStateSuspended, withBufferAssignmentMutator("ns/buffer")),
			desiredState:                  csn.NodeStateConsumed,
			expectedCSNPodSchdulable:      false,
			expectedBufferAssignmentExist: false,
		},
		{
			description:                   "Chilling to Consumed",
			node:                          create8CPUTestNode(t, "node-1", csn.NodeStateChilling, withBufferAssignmentMutator("ns/buffer")),
			desiredState:                  csn.NodeStateConsumed,
			expectedCSNPodSchdulable:      false,
			expectedBufferAssignmentExist: false,
		},
		{
			description:                   "Consumed to Consumed",
			node:                          create8CPUTestNode(t, "node-1", csn.NodeStateConsumed, withBufferAssignmentMutator("ns/buffer")),
			desiredState:                  csn.NodeStateConsumed,
			expectedCSNPodSchdulable:      false,
			expectedBufferAssignmentExist: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			node, err := setNodeAsForProcessors(tc.node, tc.desiredState)
			assert.NoError(t, err)
			assert.Equal(t, tc.desiredState, csn.ClassifyNode(node))

			hasBufferAssignmentTaint := taints.HasTaint(node, csn.BufferAssignmentKey)
			if tc.expectedBufferAssignmentExist {
				assert.Contains(t, node.Labels, csn.BufferAssignmentKey)
				assert.True(t, hasBufferAssignmentTaint, "Expected buffer assignment taint to exist")
			} else {
				assert.NotContains(t, node.Labels, csn.BufferAssignmentKey)
				assert.False(t, hasBufferAssignmentTaint, "Expected buffer assignment taint to be removed")
			}

			sn := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
			nodeInfo := framework.NewNodeInfo(node.DeepCopy(), nil)
			err = sn.AddNodeInfo(nodeInfo)
			assert.NoError(t, err)
			schedErr := sn.CheckPredicates(csnPod, node.Name)
			fmt.Println(schedErr)
			actualCSNPodSchedulable := schedErr == nil
			assert.Equal(t, tc.expectedCSNPodSchdulable, actualCSNPodSchedulable, "mismatch in expectation of CSN pods being schedulable, got: %v, expected: %v, scheduling error: %v", actualCSNPodSchedulable, tc.expectedCSNPodSchdulable, schedErr)
		})
	}
}

func TestSuspendedNodesBecomesReadySuccessfully(t *testing.T) {
	suspendedNode := create8CPUTestNode(t, "suspended-node", csn.NodeStateSuspended)
	suspendedNode.Spec.Unschedulable = true
	suspendedNode.Spec.Taints = append(suspendedNode.Spec.Taints, []apiv1.Taint{
		{Key: apiv1.TaintNodeUnreachable, Effect: apiv1.TaintEffectNoSchedule},
		{Key: apiv1.TaintNodeUnreachable, Effect: apiv1.TaintEffectNoExecute},
		{Key: apiv1.TaintNodeUnschedulable, Effect: apiv1.TaintEffectNoSchedule},
	}...)

	suspendedNode.Status.Conditions = []apiv1.NodeCondition{
		{Type: apiv1.NodeReady, Status: apiv1.ConditionUnknown},
		{Type: apiv1.NodeNetworkUnavailable, Status: apiv1.ConditionUnknown},
		{Type: apiv1.NodeMemoryPressure, Status: apiv1.ConditionUnknown},
		{Type: apiv1.NodeDiskPressure, Status: apiv1.ConditionUnknown},
		{Type: apiv1.NodePIDPressure, Status: apiv1.ConditionUnknown},
	}

	makeSuspendedNodeReady(suspendedNode)

	readiness, err := kubernetes.GetNodeReadiness(suspendedNode)
	assert.NoError(t, err)
	assert.True(t, readiness.Ready, "Node should be ready")
}

func nodeStates(nodes []*apiv1.Node) map[string]csn.NodeState {
	nodeStates := map[string]csn.NodeState{}
	for _, node := range nodes {
		nodeStates[node.Name] = csn.ClassifyNode(node)
	}
	return nodeStates
}

func TestAssignNodeToBufferForProcessors(t *testing.T) {
	testCases := []struct {
		description         string
		node                *apiv1.Node
		bufferId            string
		expectedLabels      map[string]string
		expectedAnnotations map[string]string
		expectedTaints      []apiv1.Taint
	}{
		{
			description: "Assign to a clean node",
			node:        create8CPUTestNode(t, "node-1", csn.NodeStateConsumed),
			bufferId:    "ns1/buffer1",
			expectedLabels: map[string]string{
				csn.BufferAssignmentKey: "ns1_buffer1",
			},
			expectedAnnotations: map[string]string{
				csn.BufferAssignmentKey: "ns1/buffer1",
			},
			expectedTaints: []apiv1.Taint{
				{
					Key:    csn.BufferAssignmentKey,
					Value:  "ns1_buffer1",
					Effect: apiv1.TaintEffectNoSchedule,
				},
			},
		},
		{
			description: "Assign to a node with existing labels and taints",
			node: create8CPUTestNode(t, "node-1", csn.NodeStateConsumed,
				withLabelsMutator(map[string]string{"other-label": "other-value"}),
				withTaintsMutator(apiv1.Taint{Key: "other-taint", Value: "val", Effect: apiv1.TaintEffectNoSchedule}),
			),
			bufferId: "ns2/buffer2",
			expectedLabels: map[string]string{
				"other-label":           "other-value",
				csn.BufferAssignmentKey: "ns2_buffer2",
			},
			expectedAnnotations: map[string]string{
				csn.BufferAssignmentKey: "ns2/buffer2",
			},
			expectedTaints: []apiv1.Taint{
				{Key: "other-taint", Value: "val", Effect: apiv1.TaintEffectNoSchedule},
				{
					Key:    csn.BufferAssignmentKey,
					Value:  "ns2_buffer2",
					Effect: apiv1.TaintEffectNoSchedule,
				},
			},
		},
		{
			description: "Re-assign node to a different buffer",
			node: create8CPUTestNode(t, "node-1", csn.NodeStateConsumed,
				withBufferAssignmentMutator("ns1/buffer1"),
			),
			bufferId: "ns2/buffer2",
			expectedLabels: map[string]string{
				csn.BufferAssignmentKey: "ns2_buffer2",
			},
			expectedAnnotations: map[string]string{
				csn.BufferAssignmentKey: "ns2/buffer2",
			},
			expectedTaints: []apiv1.Taint{
				{
					Key:    csn.BufferAssignmentKey,
					Value:  "ns2_buffer2",
					Effect: apiv1.TaintEffectNoSchedule,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			gotNode, err := assignNodeToBufferForProcessors(tc.node, tc.bufferId)
			assert.NoError(t, err)

			assert.Equal(t, tc.expectedLabels, gotNode.Labels, "Labels mismatch")
			assert.Equal(t, tc.expectedAnnotations, gotNode.Annotations, "Annotations mismatch")
			assert.ElementsMatch(t, tc.expectedTaints, gotNode.Spec.Taints, "Taints mismatch")
			assert.Equal(t, "1", gotNode.ResourceVersion)
		})
	}
}

func TestRemoveBufferAssignmentForProcessors(t *testing.T) {
	testCases := []struct {
		description         string
		node                *apiv1.Node
		expectedLabels      map[string]string
		expectedAnnotations map[string]string
		expectedTaints      []apiv1.Taint
	}{
		{
			description: "Remove from assigned node",
			node: create8CPUTestNode(t, "node-1", csn.NodeStateConsumed,
				withBufferAssignmentMutator("ns/buffer"),
			),
			expectedLabels:      map[string]string{},
			expectedAnnotations: map[string]string{},
			expectedTaints:      []apiv1.Taint{},
		},
		{
			description: "Remove from node with other labels and taints",
			node: create8CPUTestNode(t, "node-1", csn.NodeStateConsumed,
				withLabelsMutator(map[string]string{"other-label": "other-value"}),
				withTaintsMutator(apiv1.Taint{Key: "other-taint", Value: "val", Effect: apiv1.TaintEffectNoSchedule}),
				withBufferAssignmentMutator("ns/buffer"),
			),
			expectedLabels: map[string]string{
				"other-label": "other-value",
			},
			expectedAnnotations: map[string]string{},
			expectedTaints: []apiv1.Taint{
				{Key: "other-taint", Value: "val", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
		{
			description: "Remove when not assigned",
			node: create8CPUTestNode(t, "node-1", csn.NodeStateConsumed,
				withLabelsMutator(map[string]string{"other-label": "other-value"}),
			),
			expectedLabels: map[string]string{
				"other-label": "other-value",
			},
			expectedAnnotations: nil,
			expectedTaints:      []apiv1.Taint{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			node := tc.node.DeepCopy()
			removeBufferAssignmentForProcessors(node)

			assert.Equal(t, tc.expectedLabels, node.Labels, "Labels mismatch")
			assert.Equal(t, tc.expectedAnnotations, node.Annotations, "Annotations mismatch")
			assert.ElementsMatch(t, tc.expectedTaints, node.Spec.Taints, "Taints mismatch")
		})
	}
}
