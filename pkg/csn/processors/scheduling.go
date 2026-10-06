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
	"fmt"
	"maps"
	"slices"
	"strings"

	apiv1 "k8s.io/api/core/v1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/controller/capacitybuffers"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/metadata"
	"k8s.io/kubernetes/pkg/util/taints"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/scheduling"
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

type schedulePodsOnCSNNodesOptions struct {
	ignoreBufferAssignment               bool
	startFromLastMatch                   bool
	ignoreManagedByCCCAntiAffinityForCSN bool
}

type podGroup struct {
	pods       []*apiv1.Pod
	priorities []priorityFilter
}

// schedulePodsOnCSNNodes schedules pods on CSN nodes -even if they are suspended- and returns the node names of the scheduled pods.
// Internally, it does this by temporarily adjusting them (e.g. removing CSN hard taint) and then schedule the pods on them, however the priorityFilter still runs on the original node (not the modified one) for correctness.
func schedulePodsOnCSNNodes(sn clustersnapshot.ClusterSnapshot, simulator *scheduling.HintingSimulator, pods []*apiv1.Pod, opts schedulePodsOnCSNNodesOptions, priorities ...priorityFilter) (map[*apiv1.Pod]string, error) {
	return schedulePodGroupsOnCSNNodes(sn, simulator, opts, podGroup{
		pods:       pods,
		priorities: priorities,
	})
}

// schedulePodGroupsOnCSNNodes schedules multiple groups of pods sequentially on CSN nodes with their corresponding priority filters.
// It clones CSN nodes, forks the snapshot, and makes CSN nodes schedulable only ONCE across all groups.
func schedulePodGroupsOnCSNNodes(sn clustersnapshot.ClusterSnapshot, simulator *scheduling.HintingSimulator, opts schedulePodsOnCSNNodesOptions, groups ...podGroup) (map[*apiv1.Pod]string, error) {
	totalPods := 0
	for _, g := range groups {
		totalPods += len(g.pods)
	}
	if totalPods == 0 {
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

	nodesOfScheduledPods := map[*apiv1.Pod]string{}
	for _, g := range groups {
		if len(g.pods) == 0 {
			continue
		}

		// Pre-categorize CSN nodes into priority buckets once per podGroup against the original nodes.
		// TODO(b/479842232): This is fine as we will not have non-CSN nodes in the originalNodeInfo.
		// Their priority will thus be lowest possible,
		// but we don't want to schedule non-CSN nodes in this function, so its ok.
		nodePriorities := make(map[string]int, len(originalNodeInfo))
		for name, origNI := range originalNodeInfo {
			for i, priority := range g.priorities {
				if priority(origNI) {
					nodePriorities[name] = i
					break
				}
			}
		}

		podsToSchedule := g.pods
		var podMap map[*apiv1.Pod]*apiv1.Pod
		if opts.ignoreManagedByCCCAntiAffinityForCSN {
			// TODO(b/564793765): check if this can be achieved more cleanly
			// we do this in order to allow CCC-CB integration to have wildcard toleration
			// this forces us to prevent initial scheduling of ASN pods on CSN which causes
			// consumption to break and requires this fix
			podsToSchedule, podMap = clonePodsWithoutCSNAntiAffinity(g.pods)
		}

		scheduled, err := schedulePodsWithBuckets(sn, simulator, podsToSchedule, nodePriorities, len(g.priorities), opts.startFromLastMatch)
		if err != nil {
			sn.Revert()
			return nil, fmt.Errorf("failed to schedule pods: %v", err)
		}
		if podMap != nil {
			for cp, nodeName := range scheduled {
				if origPod, ok := podMap[cp]; ok {
					nodesOfScheduledPods[origPod] = nodeName
				}
			}
		} else {
			maps.Copy(nodesOfScheduledPods, scheduled)
		}
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

func schedulePodsWithBuckets(sn clustersnapshot.ClusterSnapshot, simulator *scheduling.HintingSimulator, pods []*apiv1.Pod, nodePriorities map[string]int, numPriorities int, startFromLastMatch bool) (map[*apiv1.Pod]string, error) {
	scheduledPods := map[*apiv1.Pod]string{}
	ordering := newBucketedNodeOrderMapping(nodePriorities, numPriorities, startFromLastMatch)

	res, err := simulator.TrySchedulePods(context.Background(), sn, pods, false, clustersnapshot.SchedulingOptions{
		IsNodeAcceptable: ordering.isNodeAcceptable,
		NodeOrdering:     ordering,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to schedule pods: %v", err)
	}

	for _, s := range res.Statuses {
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
	node, err := csn.SetNodeAs(node.DeepCopy(), desiredState)
	if err != nil {
		return nil, err
	}
	if desiredState == csn.NodeStateSuspended {
		node.Spec.Unschedulable = false
	}
	if currentState == csn.NodeStateSuspended || desiredState == csn.NodeStateFailed {
		makeNodeReadyInSnapshot(node)
	}
	if desiredState == csn.NodeStateConsumed {
		removeBufferAssignmentForProcessors(node)
	}
	return node, nil
}

// makeNodeReadyInSnapshot fakes a node's status in the cluster snapshot to make it appear ready and schedulable.
//
// This helper unifies two snapshot preparation routines:
// 1. For Suspended nodes - allows pod list processors to simulate pod placement.
// 2. For Failed nodes - allows the Defrag processor to select failed nodes as candidates for scale-down and replacement.
//
// Note: While this enables Defrag processing, in edge cases standard scale-down can target an empty node immediately
// before Defrag's CreateBeforeDelete pacing completes.
// TODO(b/532162884): Find a clean design to allow Defrag to process failed nodes without faking snapshot states.
func makeNodeReadyInSnapshot(node *apiv1.Node) {
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
		Key:    metadata.BufferAssignmentKey,
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
	node.Labels[metadata.BufferAssignmentKey] = bufferId
	return node, nil
}

func removeBufferAssignmentForProcessors(node *apiv1.Node) {
	csn.RemoveBufferAssignment(node)

	node.Spec.Taints, _ = taints.DeleteTaintsByKey(node.Spec.Taints, metadata.BufferAssignmentKey)
	delete(node.Labels, metadata.BufferAssignmentKey)
}

func isManagedByCCC(pod *apiv1.Pod) bool {
	if pod == nil || pod.Namespace != capacitybuffers.NamespaceGkeManagedCCC {
		return false
	}
	if _, exists := pod.Labels[gkelabels.ComputeClassLabel]; exists {
		return true
	}
	return false
}

func isCSNAntiAffinityRequirement(req apiv1.NodeSelectorRequirement) bool {
	return req.Key == metadata.SoftWorkloadSeparationKey &&
		req.Operator == apiv1.NodeSelectorOpNotIn &&
		len(req.Values) == 1 &&
		req.Values[0] == "true"
}

func getNodeSelectorTerms(pod *apiv1.Pod) []apiv1.NodeSelectorTerm {
	if pod == nil || pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil || pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return nil
	}
	return pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
}

func hasCSNAntiAffinity(pod *apiv1.Pod) bool {
	for _, term := range getNodeSelectorTerms(pod) {
		if slices.ContainsFunc(term.MatchExpressions, isCSNAntiAffinityRequirement) {
			return true
		}
	}
	return false
}

func stripCSNAntiAffinity(pod *apiv1.Pod) {
	terms := getNodeSelectorTerms(pod)
	if terms == nil {
		return
	}

	var newTerms []apiv1.NodeSelectorTerm
	for _, term := range terms {
		if !slices.ContainsFunc(term.MatchExpressions, isCSNAntiAffinityRequirement) {
			newTerms = append(newTerms, term)
			continue
		}

		term.MatchExpressions = slices.DeleteFunc(term.MatchExpressions, isCSNAntiAffinityRequirement)
		if len(term.MatchExpressions) > 0 || len(term.MatchFields) > 0 {
			newTerms = append(newTerms, term)
		}
	}

	if len(newTerms) > 0 {
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = newTerms
	} else {
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = nil
	}
}

func clonePodsWithoutCSNAntiAffinity(pods []*apiv1.Pod) ([]*apiv1.Pod, map[*apiv1.Pod]*apiv1.Pod) {
	hasMatchingPods := false
	for _, p := range pods {
		if isManagedByCCC(p) && hasCSNAntiAffinity(p) {
			hasMatchingPods = true
			break
		}
	}
	if !hasMatchingPods {
		return pods, nil
	}

	result := make([]*apiv1.Pod, 0, len(pods))
	podMap := make(map[*apiv1.Pod]*apiv1.Pod, len(pods))
	for _, p := range pods {
		var targetPod *apiv1.Pod
		if isManagedByCCC(p) && hasCSNAntiAffinity(p) {
			targetPod = p.DeepCopy()
			stripCSNAntiAffinity(targetPod)
		} else {
			targetPod = p
		}
		result = append(result, targetPod)
		podMap[targetPod] = p
	}

	return result, podMap
}
