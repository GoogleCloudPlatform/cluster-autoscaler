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
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/metadata"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/store"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/scheduling"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
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

func buildCCCStandbyPod(name string) *apiv1.Pod {
	pod := test.BuildTestPod(name, 1000, 1*GiB)
	pod.Namespace = "gke-managed-ccc"
	pod.Labels = map[string]string{gkelabels.ComputeClassLabel: "my-ccc"}
	pod.Spec.Tolerations = []apiv1.Toleration{
		{Key: metadata.SoftWorkloadSeparationKey, Operator: apiv1.TolerationOpExists},
	}
	pod.Spec.Affinity = &apiv1.Affinity{
		NodeAffinity: &apiv1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
				NodeSelectorTerms: []apiv1.NodeSelectorTerm{
					{
						MatchExpressions: []apiv1.NodeSelectorRequirement{
							{
								Key:      metadata.SoftWorkloadSeparationKey,
								Operator: apiv1.NodeSelectorOpNotIn,
								Values:   []string{"true"},
							},
						},
					},
				},
			},
		},
	}
	return pod
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
		{
			description: "Do not schedule CCC standby pod on suspended node if ignoreManagedByCCCAntiAffinityForCSN is false",
			pods: []*apiv1.Pod{
				buildCCCStandbyPod("ccc-standby-pod"),
			},
			nodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended),
			},
			options: schedulePodsOnCSNNodesOptions{
				ignoreManagedByCCCAntiAffinityForCSN: false,
			},
			priorities: []priorityFilter{
				isSuspendedFilter,
			},
			expectedScheduling: map[string]string{},
		},
		{
			description: "Schedule CCC standby pod on suspended node if ignoreManagedByCCCAntiAffinityForCSN is true",
			pods: []*apiv1.Pod{
				buildCCCStandbyPod("ccc-standby-pod"),
			},
			nodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended),
			},
			options: schedulePodsOnCSNNodesOptions{
				ignoreManagedByCCCAntiAffinityForCSN: true,
			},
			priorities: []priorityFilter{
				isSuspendedFilter,
			},
			expectedScheduling: map[string]string{
				"ccc-standby-pod": "node-1",
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
							metadata.SoftWorkloadSeparationKey: metadata.SoftWorkloadSeparationValue,
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
					Status: apiv1.NodeStatus{
						Conditions: []apiv1.NodeCondition{{Type: csn.NodeConditionSuspended, Status: apiv1.ConditionTrue}},
					},
				},
			},
			expectedErr: false,
			expectedNodes: []*apiv1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "csn-node-1",
						Labels: map[string]string{
							metadata.SoftWorkloadSeparationKey: metadata.SoftWorkloadSeparationValue,
						},
					},
					Spec: apiv1.NodeSpec{
						Unschedulable: false,
						Taints:        []apiv1.Taint{},
					},
					Status: apiv1.NodeStatus{
						Conditions: []apiv1.NodeCondition{{
							Type:    csn.NodeConditionSuspended,
							Status:  apiv1.ConditionFalse,
							Message: csn.NodeResumedMessage,
							Reason:  csn.NodeConditionReason,
						}},
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
							metadata.SoftWorkloadSeparationKey: metadata.SoftWorkloadSeparationValue,
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
							metadata.SoftWorkloadSeparationKey: metadata.SoftWorkloadSeparationValue,
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
							metadata.SoftWorkloadSeparationKey: metadata.SoftWorkloadSeparationValue,
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
							metadata.SoftWorkloadSeparationKey: metadata.SoftWorkloadSeparationValue,
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
							metadata.SoftWorkloadSeparationKey: metadata.SoftWorkloadSeparationValue,
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
							metadata.SoftWorkloadSeparationKey: metadata.SoftWorkloadSeparationValue,
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
							metadata.SoftWorkloadSeparationKey: metadata.SoftWorkloadSeparationValue,
							metadata.BufferAssignmentKey:       "ns/buffer",
						},
					},
					Spec: apiv1.NodeSpec{
						Unschedulable: true,
						Taints: []apiv1.Taint{
							csn.SuspendedTaint,
							{
								Key:    metadata.BufferAssignmentKey,
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
							metadata.SoftWorkloadSeparationKey: metadata.SoftWorkloadSeparationValue,
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
							metadata.SoftWorkloadSeparationKey: metadata.SoftWorkloadSeparationValue,
							metadata.BufferAssignmentKey:       "ns/buffer",
						},
					},
					Spec: apiv1.NodeSpec{
						Unschedulable: true,
						Taints: []apiv1.Taint{
							csn.SuspendedTaint,
							{
								Key:    metadata.BufferAssignmentKey,
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
							metadata.SoftWorkloadSeparationKey: metadata.SoftWorkloadSeparationValue,
							metadata.BufferAssignmentKey:       "ns/buffer",
						},
					},
					Spec: apiv1.NodeSpec{
						Unschedulable: false,
						Taints: []apiv1.Taint{
							{
								Key:    metadata.BufferAssignmentKey,
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
					node := ni.Node()
					// SetNodeAs sets the condition timestamps to the current time.
					for i := range node.Status.Conditions {
						node.Status.Conditions[i].LastTransitionTime = metav1.Time{}
						node.Status.Conditions[i].LastHeartbeatTime = metav1.Time{}
					}
					resultNodes = append(resultNodes, node)
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
		{
			description:                   "Chilling to Failed",
			node:                          create8CPUTestNode(t, "node-1", csn.NodeStateChilling, withBufferAssignmentMutator("ns/buffer")),
			desiredState:                  csn.NodeStateFailed,
			expectedCSNPodSchdulable:      false,
			expectedBufferAssignmentExist: true,
		},
		{
			description:                   "Suspended to Failed",
			node:                          create8CPUTestNode(t, "node-1", csn.NodeStateSuspended, withBufferAssignmentMutator("ns/buffer")),
			desiredState:                  csn.NodeStateFailed,
			expectedCSNPodSchdulable:      false,
			expectedBufferAssignmentExist: true,
		},
		{
			description:                   "Failed to Failed",
			node:                          create8CPUTestNode(t, "node-1", csn.NodeStateFailed, withBufferAssignmentMutator("ns/buffer")),
			desiredState:                  csn.NodeStateFailed,
			expectedCSNPodSchdulable:      false,
			expectedBufferAssignmentExist: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			node, err := setNodeAsForProcessors(tc.node, tc.desiredState)
			assert.NoError(t, err)
			assert.Equal(t, tc.desiredState, csn.ClassifyNode(node))
			assert.False(t, node.Spec.Unschedulable, "Expected node to be schedulable in snapshot")

			hasBufferAssignmentTaint := taints.HasTaint(node, metadata.BufferAssignmentKey)
			if tc.expectedBufferAssignmentExist {
				assert.Contains(t, node.Labels, metadata.BufferAssignmentKey)
				assert.True(t, hasBufferAssignmentTaint, "Expected buffer assignment taint to exist")
			} else {
				assert.NotContains(t, node.Labels, metadata.BufferAssignmentKey)
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

	makeNodeReadyInSnapshot(suspendedNode)

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
				metadata.BufferAssignmentKey: "ns1_buffer1",
			},
			expectedAnnotations: map[string]string{
				metadata.BufferAssignmentKey: "ns1/buffer1",
			},
			expectedTaints: []apiv1.Taint{
				{
					Key:    metadata.BufferAssignmentKey,
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
				"other-label":                "other-value",
				metadata.BufferAssignmentKey: "ns2_buffer2",
			},
			expectedAnnotations: map[string]string{
				metadata.BufferAssignmentKey: "ns2/buffer2",
			},
			expectedTaints: []apiv1.Taint{
				{Key: "other-taint", Value: "val", Effect: apiv1.TaintEffectNoSchedule},
				{
					Key:    metadata.BufferAssignmentKey,
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
				metadata.BufferAssignmentKey: "ns2_buffer2",
			},
			expectedAnnotations: map[string]string{
				metadata.BufferAssignmentKey: "ns2/buffer2",
			},
			expectedTaints: []apiv1.Taint{
				{
					Key:    metadata.BufferAssignmentKey,
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

func TestIsManagedByCCC(t *testing.T) {
	testCases := []struct {
		description string
		pod         *apiv1.Pod
		expected    bool
	}{
		{
			description: "nil pod",
			pod:         nil,
			expected:    false,
		},
		{
			description: "pod in other namespace with CCC label",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Labels: map[string]string{
						gkelabels.ComputeClassLabel: "my-ccc",
					},
				},
			},
			expected: false,
		},
		{
			description: "pod in gke-managed-ccc without labels",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "gke-managed-ccc",
				},
			},
			expected: false,
		},
		{
			description: "pod in gke-managed-ccc with other labels",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "gke-managed-ccc",
					Labels: map[string]string{
						"app": "nginx",
					},
				},
			},
			expected: false,
		},
		{
			description: "pod in gke-managed-ccc with CCC label",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "gke-managed-ccc",
					Labels: map[string]string{
						gkelabels.ComputeClassLabel: "my-ccc",
					},
				},
			},
			expected: true,
		},
		{
			description: "pod in gke-managed-ccc with empty CCC label value",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "gke-managed-ccc",
					Labels: map[string]string{
						gkelabels.ComputeClassLabel: "",
					},
				},
			},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			assert.Equal(t, tc.expected, isManagedByCCC(tc.pod))
		})
	}
}

func TestIsCSNAntiAffinityRequirement(t *testing.T) {
	testCases := []struct {
		description string
		req         apiv1.NodeSelectorRequirement
		expected    bool
	}{
		{
			description: "matching CSN anti-affinity requirement",
			req: apiv1.NodeSelectorRequirement{
				Key:      metadata.SoftWorkloadSeparationKey,
				Operator: apiv1.NodeSelectorOpNotIn,
				Values:   []string{"true"},
			},
			expected: true,
		},
		{
			description: "different key",
			req: apiv1.NodeSelectorRequirement{
				Key:      "other-key",
				Operator: apiv1.NodeSelectorOpNotIn,
				Values:   []string{"true"},
			},
			expected: false,
		},
		{
			description: "different operator",
			req: apiv1.NodeSelectorRequirement{
				Key:      metadata.SoftWorkloadSeparationKey,
				Operator: apiv1.NodeSelectorOpIn,
				Values:   []string{"true"},
			},
			expected: false,
		},
		{
			description: "different value",
			req: apiv1.NodeSelectorRequirement{
				Key:      metadata.SoftWorkloadSeparationKey,
				Operator: apiv1.NodeSelectorOpNotIn,
				Values:   []string{"false"},
			},
			expected: false,
		},
		{
			description: "multiple values",
			req: apiv1.NodeSelectorRequirement{
				Key:      metadata.SoftWorkloadSeparationKey,
				Operator: apiv1.NodeSelectorOpNotIn,
				Values:   []string{"true", "false"},
			},
			expected: false,
		},
		{
			description: "empty values slice",
			req: apiv1.NodeSelectorRequirement{
				Key:      metadata.SoftWorkloadSeparationKey,
				Operator: apiv1.NodeSelectorOpNotIn,
				Values:   []string{},
			},
			expected: false,
		},
		{
			description: "nil values",
			req: apiv1.NodeSelectorRequirement{
				Key:      metadata.SoftWorkloadSeparationKey,
				Operator: apiv1.NodeSelectorOpNotIn,
				Values:   nil,
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			assert.Equal(t, tc.expected, isCSNAntiAffinityRequirement(tc.req))
		})
	}
}

func TestHasCSNAntiAffinity(t *testing.T) {
	testCases := []struct {
		description string
		pod         *apiv1.Pod
		expected    bool
	}{
		{
			description: "nil pod",
			pod:         nil,
			expected:    false,
		},
		{
			description: "pod with nil affinity",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Affinity: nil,
				},
			},
			expected: false,
		},
		{
			description: "pod with nil node affinity",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{
						NodeAffinity: nil,
					},
				},
			},
			expected: false,
		},
		{
			description: "pod with nil RequiredDuringSchedulingIgnoredDuringExecution",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: nil,
						},
					},
				},
			},
			expected: false,
		},
		{
			description: "pod with unrelated node affinity",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{
										MatchExpressions: []apiv1.NodeSelectorRequirement{
											{
												Key:      "kubernetes.io/os",
												Operator: apiv1.NodeSelectorOpIn,
												Values:   []string{"linux"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			description: "pod with CSN anti-affinity in single term",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{
										MatchExpressions: []apiv1.NodeSelectorRequirement{
											{
												Key:      metadata.SoftWorkloadSeparationKey,
												Operator: apiv1.NodeSelectorOpNotIn,
												Values:   []string{"true"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			description: "pod with multiple terms, second term containing CSN anti-affinity",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{
										MatchExpressions: []apiv1.NodeSelectorRequirement{
											{
												Key:      "kubernetes.io/os",
												Operator: apiv1.NodeSelectorOpIn,
												Values:   []string{"linux"},
											},
										},
									},
									{
										MatchExpressions: []apiv1.NodeSelectorRequirement{
											{
												Key:      metadata.SoftWorkloadSeparationKey,
												Operator: apiv1.NodeSelectorOpNotIn,
												Values:   []string{"true"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			assert.Equal(t, tc.expected, hasCSNAntiAffinity(tc.pod))
		})
	}
}

func TestStripCSNAntiAffinity(t *testing.T) {
	otherReq := apiv1.NodeSelectorRequirement{
		Key:      "kubernetes.io/arch",
		Operator: apiv1.NodeSelectorOpIn,
		Values:   []string{"amd64"},
	}
	csnReq := apiv1.NodeSelectorRequirement{
		Key:      metadata.SoftWorkloadSeparationKey,
		Operator: apiv1.NodeSelectorOpNotIn,
		Values:   []string{"true"},
	}
	matchFieldReq := apiv1.NodeSelectorRequirement{
		Key:      "metadata.name",
		Operator: apiv1.NodeSelectorOpIn,
		Values:   []string{"node-1"},
	}

	testCases := []struct {
		description         string
		pod                 *apiv1.Pod
		expectedTerms       []apiv1.NodeSelectorTerm
		expectedRequiredNil bool
	}{
		{
			description: "nil pod is safe no-op",
			pod:         nil,
		},
		{
			description: "pod with nil affinity is safe no-op",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{Affinity: nil},
			},
		},
		{
			description: "pod with nil node affinity is safe no-op",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{NodeAffinity: nil},
				},
			},
		},
		{
			description: "pod with only CSN anti-affinity in single term cleans up Required to nil",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{MatchExpressions: []apiv1.NodeSelectorRequirement{csnReq}},
								},
							},
						},
					},
				},
			},
			expectedRequiredNil: true,
		},
		{
			description: "pod with mixed requirements in single term removes only CSN requirement",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{MatchExpressions: []apiv1.NodeSelectorRequirement{otherReq, csnReq}},
								},
							},
						},
					},
				},
			},
			expectedTerms: []apiv1.NodeSelectorTerm{
				{MatchExpressions: []apiv1.NodeSelectorRequirement{otherReq}},
			},
		},
		{
			description: "pod with multiple terms drops term that becomes empty and preserves non-empty term",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{MatchExpressions: []apiv1.NodeSelectorRequirement{otherReq}},
									{MatchExpressions: []apiv1.NodeSelectorRequirement{csnReq}},
								},
							},
						},
					},
				},
			},
			expectedTerms: []apiv1.NodeSelectorTerm{
				{MatchExpressions: []apiv1.NodeSelectorRequirement{otherReq}},
			},
		},
		{
			description: "pod with MatchFields preserves term even when MatchExpressions becomes empty",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{
										MatchExpressions: []apiv1.NodeSelectorRequirement{csnReq},
										MatchFields:      []apiv1.NodeSelectorRequirement{matchFieldReq},
									},
								},
							},
						},
					},
				},
			},
			expectedTerms: []apiv1.NodeSelectorTerm{
				{
					MatchExpressions: []apiv1.NodeSelectorRequirement{},
					MatchFields:      []apiv1.NodeSelectorRequirement{matchFieldReq},
				},
			},
		},
		{
			description: "pod without CSN anti-affinity is untouched",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{MatchExpressions: []apiv1.NodeSelectorRequirement{otherReq}},
								},
							},
						},
					},
				},
			},
			expectedTerms: []apiv1.NodeSelectorTerm{
				{MatchExpressions: []apiv1.NodeSelectorRequirement{otherReq}},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			stripCSNAntiAffinity(tc.pod)
			if tc.pod == nil || tc.pod.Spec.Affinity == nil || tc.pod.Spec.Affinity.NodeAffinity == nil {
				return
			}
			req := tc.pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
			if tc.expectedRequiredNil {
				assert.Nil(t, req)
			} else {
				assert.NotNil(t, req)
				assert.Equal(t, tc.expectedTerms, req.NodeSelectorTerms)
			}
			assert.False(t, hasCSNAntiAffinity(tc.pod))
		})
	}
}

func TestClonePodsWithoutCSNAntiAffinity(t *testing.T) {
	t.Run("empty and nil slice", func(t *testing.T) {
		clonedNil, mapNil := clonePodsWithoutCSNAntiAffinity(nil)
		assert.Empty(t, clonedNil)
		assert.Empty(t, mapNil)

		clonedEmpty, mapEmpty := clonePodsWithoutCSNAntiAffinity([]*apiv1.Pod{})
		assert.Empty(t, clonedEmpty)
		assert.Empty(t, mapEmpty)
	})

	t.Run("cloning and stripping behaviour for mixed pods", func(t *testing.T) {
		csnAffinity := &apiv1.Affinity{
			NodeAffinity: &apiv1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
					NodeSelectorTerms: []apiv1.NodeSelectorTerm{
						{
							MatchExpressions: []apiv1.NodeSelectorRequirement{
								{
									Key:      metadata.SoftWorkloadSeparationKey,
									Operator: apiv1.NodeSelectorOpNotIn,
									Values:   []string{"true"},
								},
							},
						},
					},
				},
			},
		}

		// 1. CCC pod with CSN anti-affinity (should be cloned and stripped)
		cccPodWithAffinity := &apiv1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "ccc-pod-with-affinity",
				Namespace: "gke-managed-ccc",
				Labels: map[string]string{
					gkelabels.ComputeClassLabel: "my-ccc",
				},
			},
			Spec: apiv1.PodSpec{
				Affinity: csnAffinity.DeepCopy(),
			},
		}

		// 2. CCC pod without CSN anti-affinity (should not be cloned)
		cccPodWithoutAffinity := &apiv1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "ccc-pod-without-affinity",
				Namespace: "gke-managed-ccc",
				Labels: map[string]string{
					gkelabels.ComputeClassLabel: "my-ccc",
				},
			},
			Spec: apiv1.PodSpec{},
		}

		// 3. Non-CCC pod with CSN anti-affinity (should not be cloned)
		nonCCCPodWithAffinity := &apiv1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "non-ccc-pod-with-affinity",
				Namespace: "default",
			},
			Spec: apiv1.PodSpec{
				Affinity: csnAffinity.DeepCopy(),
			},
		}

		// 4. Non-CCC pod without affinity (should not be cloned)
		nonCCCPodWithoutAffinity := &apiv1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "non-ccc-pod-without-affinity",
				Namespace: "default",
			},
			Spec: apiv1.PodSpec{},
		}

		inputPods := []*apiv1.Pod{cccPodWithAffinity, cccPodWithoutAffinity, nonCCCPodWithAffinity, nonCCCPodWithoutAffinity}
		clonedPods, podMap := clonePodsWithoutCSNAntiAffinity(inputPods)

		assert.Len(t, clonedPods, 4)
		assert.Len(t, podMap, 4)

		// Check 1: cccPodWithAffinity was cloned and stripped, original was unmodified
		assert.NotSame(t, cccPodWithAffinity, clonedPods[0])
		assert.False(t, hasCSNAntiAffinity(clonedPods[0]))
		assert.True(t, hasCSNAntiAffinity(cccPodWithAffinity))
		assert.Same(t, cccPodWithAffinity, podMap[clonedPods[0]])

		// Check 2: cccPodWithoutAffinity was not cloned
		assert.Same(t, cccPodWithoutAffinity, clonedPods[1])
		assert.Same(t, cccPodWithoutAffinity, podMap[clonedPods[1]])

		// Check 3: nonCCCPodWithAffinity was not cloned
		assert.Same(t, nonCCCPodWithAffinity, clonedPods[2])
		assert.True(t, hasCSNAntiAffinity(clonedPods[2]))
		assert.Same(t, nonCCCPodWithAffinity, podMap[clonedPods[2]])

		// Check 4: nonCCCPodWithoutAffinity was not cloned
		assert.Same(t, nonCCCPodWithoutAffinity, clonedPods[3])
		assert.Same(t, nonCCCPodWithoutAffinity, podMap[clonedPods[3]])
	})
}
