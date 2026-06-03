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
	"strings"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/scheduling"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/kubernetes/pkg/util/taints"
)

type priorityFilter func(ni *framework.NodeInfo) bool

var (
	isChillingFilter = func(ni *framework.NodeInfo) bool {
		return csn.ClassifyNode(ni.Node()) == csn.NodeStateChilling
	}
	isSuspendedFilter = func(ni *framework.NodeInfo) bool {
		return csn.ClassifyNode(ni.Node()) == csn.NodeStateSuspended
	}
)

// allOfPriorityFilters returns a single priorityFilter that returns true iff all the given filters return true.
func allOfPriorityFilters(priorities ...priorityFilter) priorityFilter {
	return func(ni *framework.NodeInfo) bool {
		for _, priority := range priorities {
			if !priority(ni) {
				return false
			}
		}
		return true
	}
}

// anyOfPriorityFilters returns a single priorityFilter that returns true if any of the given filters return true.
func anyOfPriorityFilters(priorities ...priorityFilter) priorityFilter {
	return func(ni *framework.NodeInfo) bool {
		for _, priority := range priorities {
			if priority(ni) {
				return true
			}
		}
		return false
	}
}

type schedulePodsOnCSNNodesOptions struct {
	ignoreBufferAssignment bool
}

// schedulePodsOnCSNNodes schedules pods on CSN nodes -even if they are suspended- and returns the node names of the scheduled pods.
// Internally, it does this by temporarily adjusting them (e.g. removing CSN hard taint) and then schedule the pods on them, however the priorityFilter still runs on the original node (not the modified one) for correctness.
func schedulePodsOnCSNNodes(sn clustersnapshot.ClusterSnapshot, simulator *scheduling.HintingSimulator, pods []*apiv1.Pod, opts schedulePodsOnCSNNodesOptions, priorities ...priorityFilter) (map[*apiv1.Pod]string, error) {
	if len(pods) == 0 {
		return map[*apiv1.Pod]string{}, nil
	}

	// We need to store the original nodeInfos before the fork to avoid having to do deep copy here.
	nodeInfos, err := sn.ListNodeInfos()
	if err != nil {
		return nil, fmt.Errorf("failed to list node infos: %v", err)
	}
	// TODO(b/479842232): Temporary optimization to only copy CSN nodes.
	// A more comprehensive fix will address the deep copy performance overhead.
	originalNodeInfo := map[string]*framework.NodeInfo{}
	for _, ni := range nodeInfos {
		if csn.IsCSNNode(ni.Node()) {
			originalNodeInfo[ni.Node().Name] = ni.DeepCopy()
		}
	}

	sn.Fork()
	nodeInfos, err = sn.ListNodeInfos()
	if err != nil {
		sn.Revert()
		return nil, fmt.Errorf("failed to list node infos: %v", err)
	}

	err = makeCSNNodesSchedulable(nodeInfos, opts)
	if err != nil {
		sn.Revert()
		return nil, fmt.Errorf("failed to make CSN nodes schedulable: %v", err)
	}

	// We need to adjust priorities to run against the original nodes, not the modified nodes.
	newPriorities := []priorityFilter{}
	for _, priority := range priorities {
		newPriorities = append(newPriorities, func(ni *framework.NodeInfo) bool {
			node := ni.Node()
			originalNI, ok := originalNodeInfo[node.Name]
			// TODO(b/479842232): This is fine as we will not have non-CSN nodes in the originalNodeInfo.
			// Their priority will thus be lowest possible,
			// but we don't want to schedule non-CSN nodes in this function, so its ok.
			if !ok {
				return false
			}
			return priority(originalNI)
		})
	}
	priorities = newPriorities

	nodesOfScheduledPods, err := schedulePods(sn, simulator, pods, priorities...)
	if err != nil {
		sn.Revert()
		return nil, fmt.Errorf("failed to schedule pods: %v", err)
	}

	// We revert the changes since we adjusted nodes to make them schedulable, we should revert them back as we already got the scheduling info.
	sn.Revert()

	// We need this since revert in Delta snapshot doesn't actually revert nodes if there were no calls to add/remove pod methods.
	nodeInfos, err = sn.ListNodeInfos()
	if err != nil {
		return nil, fmt.Errorf("failed to list node infos: %v", err)
	}
	for _, ni := range nodeInfos {
		nodeName := ni.Node().Name
		if v, ok := originalNodeInfo[nodeName]; ok {
			ni.SetNode(v.Node())
		}
	}

	// Apply the scheduling decisions made earlier that got reverted.
	for pod, nodeName := range nodesOfScheduledPods {
		err := sn.ForceAddPod(pod, nodeName)
		if err != nil {
			return nil, fmt.Errorf("failed to force add pod %s/%s to node %s: %v", pod.Namespace, pod.Name, nodeName, err)
		}
	}

	return nodesOfScheduledPods, nil
}

func comparator(priorityFilters ...priorityFilter) clustersnapshot.NodeOrderMapping {
	priorityIdxFn := func(ni *framework.NodeInfo) int {
		for i := range priorityFilters {
			if priorityFilters[i](ni) {
				return i
			}
		}
		return len(priorityFilters)
	}

	return clustersnapshot.NewPriorityNodeOrderMapping(
		func(a, b *framework.NodeInfo) bool {
			return priorityIdxFn(a) < priorityIdxFn(b)
		},
	)
}

func schedulePods(sn clustersnapshot.ClusterSnapshot, simulator *scheduling.HintingSimulator, pods []*apiv1.Pod, priorities ...priorityFilter) (map[*apiv1.Pod]string, error) {
	scheduledPods := map[*apiv1.Pod]string{}

	scheduled, _, err := simulator.TrySchedulePods(sn, pods, false, clustersnapshot.SchedulingOptions{
		IsNodeAcceptable: anyOfPriorityFilters(priorities...),
		NodeOrdering:     comparator(priorities...),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to schedule pods: %v", err)
	}

	for _, s := range scheduled {
		scheduledPods[s.Pod] = s.NodeName
	}

	return scheduledPods, nil
}

func makeCSNNodesSchedulable(nis []*framework.NodeInfo, opts schedulePodsOnCSNNodesOptions) error {
	for _, ni := range nis {
		node := ni.Node()
		if !csn.IsCSNNode(node) {
			continue
		}
		node, err := setNodeAsForProcessors(node, csn.NodeStateChilling)
		if err != nil {
			return fmt.Errorf("failed to set node %q as schedulable (Chilling): %v", node.Name, err)
		}
		if opts.ignoreBufferAssignment {
			removeBufferAssignmentForProcessors(node)
		}
		ni.SetNode(node)
	}
	return nil
}

// setNodeAsForProcessors marks a node as a CSN node with the given state,
// with extra handling in pod list processors to make them allow to schedule CSN pods on them.
// Otherwise, scale-down won't be able to remove underutilized nodes.
func setNodeAsForProcessors(node *apiv1.Node, desiredState csn.NodeState) (*apiv1.Node, error) {
	currentState := csn.ClassifyNode(node)
	node, err := csn.SetNodeAs(node, desiredState)
	if err != nil {
		return nil, err
	}
	if currentState == csn.NodeStateSuspended {
		makeSuspendedNodeReady(node)
	}
	if desiredState == csn.NodeStateSuspended {
		node.Spec.Unschedulable = false
	}
	if desiredState == csn.NodeStateConsumed {
		removeBufferAssignmentForProcessors(node)
	}
	return node, nil
}

func makeSuspendedNodeReady(node *apiv1.Node) {
	node.Spec.Taints, _ = taints.DeleteTaintsByKey(node.Spec.Taints, apiv1.TaintNodeUnreachable)
	node.Spec.Taints, _ = taints.DeleteTaintsByKey(node.Spec.Taints, apiv1.TaintNodeUnschedulable)
	node.Spec.Unschedulable = false
	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type == apiv1.NodeReady {
			node.Status.Conditions[i].Status = apiv1.ConditionTrue
			break
		}
	}
}

func assignNodeToBufferForProcessors(node *apiv1.Node, bufferId string) (*apiv1.Node, error) {
	node, err := csn.AssignNodeToBufferId(node, bufferId)
	if err != nil {
		return nil, err
	}

	// TODO(b/484466017): Find a better fix.
	// We replace the "/" because "_" is illegal character in taints/label.
	bufferId = strings.ReplaceAll(bufferId, "/", "_")

	// TODO(b/484466017): Find a better fix (hack).
	// To make sure any update request refresh the node first (instead of leaking the workload separation).
	node.ResourceVersion = "1"

	workloadSeparationTaint := &apiv1.Taint{
		Key:    csn.BufferAssignmentKey,
		Value:  bufferId,
		Effect: apiv1.TaintEffectNoSchedule,
	}

	node, _, err = taints.AddOrUpdateTaint(node, workloadSeparationTaint)
	if err != nil {
		return nil, fmt.Errorf("error on adding buffer assignment taints: %w", err)
	}
	if node.Labels == nil {
		node.Labels = make(map[string]string)
	}
	node.Labels[csn.BufferAssignmentKey] = bufferId
	return node, nil
}

func removeBufferAssignmentForProcessors(node *apiv1.Node) {
	csn.RemoveBufferAssignment(node)

	node.Spec.Taints, _ = taints.DeleteTaintsByKey(node.Spec.Taints, csn.BufferAssignmentKey)
	delete(node.Labels, csn.BufferAssignmentKey)
}
