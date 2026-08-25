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
	"sort"
	"strconv"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/scheduling"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	crd "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	cc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/fairness"
)

const MinCapacityPodListProcessorName = "cc-min-capacity-pod-list-processor"

// MinCapacityPodListProcessor injects fake pods to enforce minimum capacity defined in ComputeClass CRDs.
type MinCapacityPodListProcessor struct {
	ccLister           cc_lister.Lister
	simulator          *scheduling.HintingSimulator
	fairnessEnforcer   fairness.FairnessEnforcer
	experimentsManager experiments.Manager
}

// NewMinCapacityPodListProcessor creates a MinCapacityPodListProcessor.
func NewMinCapacityPodListProcessor(ccLister cc_lister.Lister, fairnessEnforcer fairness.FairnessEnforcer, experimentsManager experiments.Manager) *MinCapacityPodListProcessor {
	return &MinCapacityPodListProcessor{
		ccLister:           ccLister,
		simulator:          scheduling.NewHintingSimulator(),
		fairnessEnforcer:   fairnessEnforcer,
		experimentsManager: experimentsManager,
	}
}

// Process evaluates deficits based on the `targetNodeCount` in ComputeClass CRDs
// and injects synthetic pods to meet the minimum capacity.
func (p *MinCapacityPodListProcessor) Process(
	autoscalingCtx *ca_context.AutoscalingContext,
	unschedulablePods []*apiv1.Pod) ([]*apiv1.Pod, error) {

	if !computeclass.IsComputeClassMinCapacityEnabled(p.experimentsManager) {
		return unschedulablePods, nil
	}

	canBeAdmitted := true
	if p.fairnessEnforcer != nil {
		canBeAdmitted = p.fairnessEnforcer.Admit(unschedulablePods)
	}

	crds, err := p.ccLister.ListCrds()
	if err != nil {
		klog.Errorf("Failed to list CRDs for MinCapacity processor: %v", err)
		return unschedulablePods, nil
	}
	if len(crds) == 0 {
		return unschedulablePods, nil
	}

	nodeInfos, err := autoscalingCtx.ClusterSnapshot.ListNodeInfos()
	if err != nil {
		klog.Errorf("Failed to list nodes for MinCapacity processor: %v", err)
		return unschedulablePods, nil
	}

	existingByCCAndRule := p.countExistingNodes(autoscalingCtx, nodeInfos, crds)
	_, saturatedByCC := computeSaturatedNodeCounts(nodeInfos)

	// In GKE Cluster Autoscaler, minimum capacity can be specified at two levels:
	//
	// 1. Priority-level minimum capacity (`rules[].targetNodeCount`):
	//    Each priority rule specifies a target minimum capacity for nodes matching that rule.
	//    The shortfall is calculated directly: rule.targetNodeCount - existing nodes matching this rule index.
	//    We generate priority-level fake pods to cover this shortfall only when the existing node count
	//    is unsatisfied (below the target). These fake pods are always injected directly (bypassing
	//    the scheduling simulation) to force GKE scale-up of the target priority rule.
	//
	// 2. Spec-level minimum capacity (`spec.targetNodeCount`):
	//    This represents the overall target minimum node count for the entire ComputeClass.
	//    The shortfall is calculated as: spec.targetNodeCount - (priority-level fake pods generated + existing saturated nodes).
	//    For this shortfall, we generate spec-level fake pods. Since some of these spec-level fake pods
	//    can potentially be scheduled on existing under-utilized nodes (that are not yet saturated), we
	//    run a scheduling simulation to filter out any schedulable fake pods. During this simulation,
	//    these spec-level fake pods are expected to be present (added) in the cluster snapshot. Only the
	//    truly unschedulable spec-level fake pods are injected, preventing unnecessary scale-up.
	var priorityFakePods []*apiv1.Pod
	var specFakePods []*apiv1.Pod

	for _, c := range crds {
		crdName := c.Name()
		crdPriorityFakePodsCount := 0

		// 1. Evaluate priority-level fake pods.
		for ruleIdx, r := range c.Rules() {
			if r.TargetNodeCount() == nil {
				continue
			}
			target := *r.TargetNodeCount()
			existingNodes := existingByCCAndRule[crdName][strconv.Itoa(ruleIdx)]
			if rulePods := buildPriorityFakePods(crdName, ruleIdx, target, existingNodes); len(rulePods) > 0 {
				priorityFakePods = append(priorityFakePods, rulePods...)
				crdPriorityFakePodsCount += len(rulePods)
			}
		}

		// 2. Evaluate spec-level fake pods.
		if c.TargetNodeCount() != nil {
			target := *c.TargetNodeCount()
			saturatedCCCNodeCount := saturatedByCC[crdName]
			if specPods := buildSpecFakePods(crdName, target, crdPriorityFakePodsCount, saturatedCCCNodeCount); len(specPods) > 0 {
				specFakePods = append(specFakePods, specPods...)
			}
		}
	}

	// 3. Filter spec-level fake pods.
	trulyUnschedulableSpec := p.filterOutSchedulableFakePods(autoscalingCtx, specFakePods)

	// 4. Combine them.
	var finalFakePods []*apiv1.Pod
	if len(trulyUnschedulableSpec) > 0 {
		finalFakePods = append(finalFakePods, trulyUnschedulableSpec...)
	}
	if len(priorityFakePods) > 0 {
		finalFakePods = append(finalFakePods, priorityFakePods...)
	}

	if len(finalFakePods) > 0 && canBeAdmitted {
		unschedulablePods = append(unschedulablePods, finalFakePods...)
	}

	return unschedulablePods, nil
}

// buildPriorityFakePods returns the list of fake pods needed to satisfy the priority rules of the ComputeClass.
func buildPriorityFakePods(ccName string, ruleIdx int, target int, existingNodes int) []*apiv1.Pod {
	shortfall := target - existingNodes
	if shortfall <= 0 {
		return nil
	}
	var pods []*apiv1.Pod
	idx := ruleIdx
	for i := 0; i < shortfall; i++ {
		pods = append(pods, buildFakePod(ccName, &idx, i))
	}
	return pods
}

// buildSpecFakePods returns the list of fake pods needed to satisfy the spec-level minimum capacity target.
func buildSpecFakePods(ccName string, target int, priorityFakePodsCount int, saturatedNodesCount int) []*apiv1.Pod {
	shortfall := target - priorityFakePodsCount - saturatedNodesCount
	if shortfall <= 0 {
		return nil
	}
	var pods []*apiv1.Pod
	for i := 0; i < shortfall; i++ {
		pods = append(pods, buildFakePod(ccName, nil, i))
	}
	return pods
}

// atomicGroupInfo summarizes an atomic node group for packing.
type atomicGroupInfo struct {
	id         string
	totalNodes int
	freeNodes  int             // nodes in this group that currently host no pod
	nodeNames  map[string]bool // node names belonging to this group
	ccLabels   map[string]bool // ComputeClass labels present on this group's nodes
}

// filterOutSchedulableFakePods runs spec-level fake pods through the scheduling
// simulator and returns the subset that could not be placed on existing nodes.
// The returned pods will be injected as truly-unschedulable to drive scale-up.
//
// Remaining pods fall back to non-atomic nodes; leftovers are returned as truly
// unschedulable. Atomicity is resolved once per node group (not per node).
func (p *MinCapacityPodListProcessor) filterOutSchedulableFakePods(autoscalingCtx *ca_context.AutoscalingContext, fakePods []*apiv1.Pod) []*apiv1.Pod {
	if len(fakePods) == 0 {
		return nil
	}

	nodeInfos, err := autoscalingCtx.ClusterSnapshot.ListNodeInfos()
	if err != nil {
		klog.Errorf("MinCapacityPodListProcessor: failed to list node infos: %v", err)
		return fakePods
	}

	atomicGroups, nonAtomicNodes := p.splitIntoAtomicAndNonAtomicGroups(autoscalingCtx, nodeInfos)
	remaining := fakePods
	if len(atomicGroups) > 0 {
		remaining, err = p.packIntoGroups(autoscalingCtx, remaining, sortAtomicGroupsForPacking(atomicGroups))
		if err != nil {
			klog.Errorf("MinCapacityPodListProcessor: error while packing into atomic groups: %v", err)
			// Fall through: still try non-atomic for whatever is left.
		}
	}

	remaining, err = p.packIntoNonAtomic(autoscalingCtx, remaining, nonAtomicNodes)
	if err != nil {
		klog.Errorf("MinCapacityPodListProcessor: error while packing into non-atomic nodes: %v", err)
		return remaining
	}

	klog.V(2).Infof("MinCapacityPodListProcessor: packed %d fake pods, %d truly unschedulable for minimum capacity", len(fakePods)-len(remaining), len(remaining))
	return remaining
}

// splitIntoAtomicAndNonAtomicGroups classifies every node in the snapshot into
// either an atomic node group (ZeroOrMaxNodeScaling) or the set of non-atomic
// node names. Atomicity is resolved once per node group (not per node), and
// nodes whose group cannot be resolved are treated as non-atomic.
func (p *MinCapacityPodListProcessor) splitIntoAtomicAndNonAtomicGroups(autoscalingCtx *ca_context.AutoscalingContext, nodeInfos []*framework.NodeInfo) (atomicGroups map[string]*atomicGroupInfo, nonAtomicNodes map[string]bool) {
	atomicGroupIDs := p.atomicNodeGroupIDs(autoscalingCtx)
	atomicGroups = map[string]*atomicGroupInfo{}
	nonAtomicNodes = map[string]bool{}
	for _, ni := range nodeInfos {
		node := ni.Node()
		if node == nil {
			continue
		}
		// Fast path: with no atomic node groups every node is non-atomic, so
		// skip the per-node group lookup entirely.
		if len(atomicGroupIDs) == 0 {
			nonAtomicNodes[node.Name] = true
			continue
		}
		id, isAtomic := p.classifyNode(autoscalingCtx, node, atomicGroupIDs)
		if !isAtomic {
			nonAtomicNodes[node.Name] = true
			continue
		}
		g, ok := atomicGroups[id]
		if !ok {
			g = &atomicGroupInfo{id: id, nodeNames: map[string]bool{}, ccLabels: map[string]bool{}}
			atomicGroups[id] = g
		}
		g.nodeNames[node.Name] = true
		g.totalNodes++
		if cc := p.nodeCCC(node); cc != "" {
			g.ccLabels[cc] = true
		}
		if len(ni.Pods()) == 0 {
			g.freeNodes++
		}
	}
	return atomicGroups, nonAtomicNodes
}

// sortAtomicGroupsForPacking orders atomic groups deterministically for packing:
//
//	(1) most existing fake pods first (pack into groups already used),
//	(2) then most free nodes (fill biggest group first),
//	(3) then group ID for tie-break determinism.
func sortAtomicGroupsForPacking(atomicGroups map[string]*atomicGroupInfo) []*atomicGroupInfo {
	sortedGroups := make([]*atomicGroupInfo, 0, len(atomicGroups))
	for _, g := range atomicGroups {
		sortedGroups = append(sortedGroups, g)
	}
	sort.Slice(sortedGroups, func(i, j int) bool {
		a, b := sortedGroups[i], sortedGroups[j]
		apods, bpods := a.totalNodes-a.freeNodes, b.totalNodes-b.freeNodes
		if apods != bpods {
			return apods > bpods
		}
		if a.freeNodes != b.freeNodes {
			return a.freeNodes > b.freeNodes
		}
		return a.id < b.id
	})
	return sortedGroups
}

// atomicNodeGroupIDs returns the set of node group IDs that scale atomically
// (ZeroOrMaxNodeScaling). It resolves options once per node group rather than
// once per node.
func (p *MinCapacityPodListProcessor) atomicNodeGroupIDs(autoscalingCtx *ca_context.AutoscalingContext) map[string]bool {
	ids := map[string]bool{}
	for _, ng := range autoscalingCtx.CloudProvider.NodeGroups(context.TODO()) {
		if p.isAtomicNodeGroup(autoscalingCtx, ng) {
			ids[ng.Id()] = true
		}
	}
	return ids
}

// isAtomicNodeGroup reports whether ng scales atomically (ZeroOrMaxNodeScaling).
// A failure to resolve the node group's options is treated as non-atomic.
func (p *MinCapacityPodListProcessor) isAtomicNodeGroup(autoscalingCtx *ca_context.AutoscalingContext, ng cloudprovider.NodeGroup) bool {
	opts, err := ng.GetOptions(context.TODO(), autoscalingCtx.NodeGroupDefaults)
	if err != nil || opts == nil {
		return false
	}
	return opts.ZeroOrMaxNodeScaling
}

// packIntoGroups tries to place remaining pods into atomic groups in order,
// letting each group absorb pods (persisting placements in the snapshot).
// Groups whose ComputeClass labels don't match any remaining pod are skipped.
//
// TODO(b/555154270): the deterministic concentration order (already-used, then
// emptiest groups) is what frees whole atomic slices for scale-down, so it is
// kept here. A cheaper follow-up is to replace the per-group scheduling
// simulation with a count-based greedy assignment.
func (p *MinCapacityPodListProcessor) packIntoGroups(autoscalingCtx *ca_context.AutoscalingContext, pods []*apiv1.Pod, groups []*atomicGroupInfo) ([]*apiv1.Pod, error) {
	remaining := pods
	for _, g := range groups {
		if len(remaining) == 0 {
			break
		}
		if !p.groupTargetsAnyRemainingCCC(g, remaining) {
			continue
		}
		next, err := p.schedulePersisting(autoscalingCtx, remaining, func(ni *framework.NodeInfo) bool {
			n := ni.Node()
			if n == nil {
				return false
			}
			return g.nodeNames[n.Name]
		})
		if err != nil {
			return remaining, err
		}
		klog.V(2).Infof("MinCapacityPodListProcessor: packed %d pods into atomic group %s", len(remaining)-len(next), g.id)
		remaining = next
	}
	return remaining, nil
}

// packIntoNonAtomic places remaining pods onto non-atomic nodes.
func (p *MinCapacityPodListProcessor) packIntoNonAtomic(autoscalingCtx *ca_context.AutoscalingContext, pods []*apiv1.Pod, nonAtomicNodes map[string]bool) ([]*apiv1.Pod, error) {
	if len(pods) == 0 {
		return pods, nil
	}
	remaining, err := p.schedulePersisting(autoscalingCtx, pods, func(ni *framework.NodeInfo) bool {
		n := ni.Node()
		if n == nil {
			return false
		}
		return nonAtomicNodes[n.Name]
	})
	if err != nil {
		return pods, err
	}
	klog.V(4).Infof("MinCapacityPodListProcessor: packed %d pods into non-atomic nodes, %d remain unschedulable", len(pods)-len(remaining), len(remaining))
	return remaining, nil
}

// schedulePersisting runs the given pods through the scheduling simulator against
// the shared ClusterSnapshot, letting successful placements persist. It returns
// the pods that could not be placed. On simulator error the input pods are
// returned unchanged.
func (p *MinCapacityPodListProcessor) schedulePersisting(autoscalingCtx *ca_context.AutoscalingContext, pods []*apiv1.Pod, isNodeAcceptable func(*framework.NodeInfo) bool) ([]*apiv1.Pod, error) {
	res, err := p.simulator.TrySchedulePods(context.Background(), autoscalingCtx.ClusterSnapshot, pods, false, clustersnapshot.SchedulingOptions{
		IsNodeAcceptable: isNodeAcceptable,
	})
	if err != nil {
		return pods, err
	}
	placed := make(map[types.UID]bool, len(res.Statuses))
	for _, s := range res.Statuses {
		placed[s.Pod.UID] = true
	}
	var remaining []*apiv1.Pod
	for _, pod := range pods {
		if !placed[pod.UID] {
			remaining = append(remaining, pod)
		}
	}
	return remaining, nil
}

// cccOfPod returns the ComputeClass a fake pod targets. It resolves the name
// through the lister (rather than reading the raw node selector) so that special
// cases such as the default ComputeClass are handled consistently.
func (p *MinCapacityPodListProcessor) cccOfPod(pod *apiv1.Pod) string {
	_, name, err := p.ccLister.PodCrd(pod)
	if err != nil {
		klog.Warningf("MinCapacityPodListProcessor: failed to resolve ComputeClass for pod %s: %v", pod.Name, err)
		return ""
	}
	return name
}

// nodeCCC returns the ComputeClass assigned to a node. It resolves the name
// through the lister (rather than reading the raw node label) so that special
// cases such as the default ComputeClass are handled consistently.
func (p *MinCapacityPodListProcessor) nodeCCC(node *apiv1.Node) string {
	_, name, err := p.ccLister.NodeCrd(node)
	if err != nil {
		klog.Warningf("MinCapacityPodListProcessor: failed to resolve ComputeClass for node %s: %v", node.Name, err)
		return ""
	}
	return name
}

// groupTargetsAnyRemainingCCC reports whether the group has at least one node
// whose ComputeClass matches one of the remaining fake pods. This is a cheap,
// label-only gate: it never skips a group that could actually accept a pod, it
// only avoids simulating groups belonging to unrelated ComputeClasses.
func (p *MinCapacityPodListProcessor) groupTargetsAnyRemainingCCC(g *atomicGroupInfo, remaining []*apiv1.Pod) bool {
	for _, pod := range remaining {
		if g.ccLabels[p.cccOfPod(pod)] {
			return true
		}
	}
	return false
}

// classifyNode resolves a node to its group ID and whether the group is atomic.
// A node whose group cannot be resolved is treated as non-atomic.
func (p *MinCapacityPodListProcessor) classifyNode(autoscalingCtx *ca_context.AutoscalingContext, node *apiv1.Node, atomicGroupIDs map[string]bool) (groupID string, isAtomic bool) {
	ng, err := autoscalingCtx.CloudProvider.NodeGroupForNode(context.TODO(), node)
	if err != nil || ng == nil {
		return "", false
	}
	id := ng.Id()
	return id, atomicGroupIDs[id]
}

// CleanUp cleans up internal status.
func (p *MinCapacityPodListProcessor) CleanUp() {}

// countExistingNodes counts the active nodes for each ComputeClass and priority rule using NodeGroup TargetSizes.
func (p *MinCapacityPodListProcessor) countExistingNodes(
	autoscalingCtx *ca_context.AutoscalingContext,
	nodeInfos []*framework.NodeInfo,
	crds []crd.CRD) map[string]map[string]int {

	existingByCCAndRule := make(map[string]map[string]int) // ccName -> ruleIdxStr -> count

	cp, ok := autoscalingCtx.CloudProvider.(computeclass.MatcherCloudProvider)
	if !ok || cp == nil {
		klog.Errorf("MinCapacityPodListProcessor: CloudProvider does not implement MatcherCloudProvider")
		return existingByCCAndRule
	}
	matcher := computeclass.NewMatcher(p.ccLister, cp)

	for _, ng := range autoscalingCtx.CloudProvider.NodeGroups(context.TODO()) {
		targetSize, err := ng.TargetSize(context.TODO())
		if err != nil {
			klog.Warningf("MinCapacityPodListProcessor: Failed to get target size for node group %s: %v", ng.Id(), err)
			continue
		}
		if targetSize <= 0 {
			continue
		}

		for _, cc := range crds {
			ccName := cc.Name()
			matched, ruleIdx, _ := matcher.FirstMatchedRule(ng, cc)
			if matched {
				ruleIdxStr := strconv.Itoa(ruleIdx)
				if _, ok := existingByCCAndRule[ccName]; !ok {
					existingByCCAndRule[ccName] = make(map[string]int)
				}
				existingByCCAndRule[ccName][ruleIdxStr] += targetSize
				break
			} else if matcher.MatchesCrdConfig(ng, cc) {
				ruleIdxStr := "-1"
				if _, ok := existingByCCAndRule[ccName]; !ok {
					existingByCCAndRule[ccName] = make(map[string]int)
				}
				existingByCCAndRule[ccName][ruleIdxStr] += targetSize
				break
			}
		}
	}
	return existingByCCAndRule
}

// computeSaturatedNodeCounts computes the number of saturated nodes for each ComputeClass and priority rule.
func computeSaturatedNodeCounts(nodeInfos []*framework.NodeInfo) (map[string]map[string]int, map[string]int) {
	saturatedByCCAndRule := make(map[string]map[string]int) // ccName -> ruleIdxStr -> count
	saturatedByCC := make(map[string]int)                   // ccName -> count

	for _, ni := range nodeInfos {
		node := ni.Node()
		if node == nil {
			continue
		}
		ccName := node.Labels[labels.ComputeClassLabel]
		if ccName == "" {
			continue
		}
		if !isNodeSaturated(ni) {
			continue
		}

		saturatedByCC[ccName]++

		ruleIdxStr := node.Labels[labels.ComputeClassPriorityIdxLabel]
		if ruleIdxStr != "" && ruleIdxStr != "-1" {
			if _, ok := saturatedByCCAndRule[ccName]; !ok {
				saturatedByCCAndRule[ccName] = make(map[string]int)
			}
			saturatedByCCAndRule[ccName][ruleIdxStr]++
		}
	}
	return saturatedByCCAndRule, saturatedByCC
}

// isNodeSaturated checks if a node is saturated based on the number of pods it hosts.
// Note: fake pods request 0 compute resources, so they only consume node capacity via HostPort and pod count.
func isNodeSaturated(nodeInfo *framework.NodeInfo) bool {
	node := nodeInfo.Node()
	if node == nil {
		return false
	}
	allocatablePods, ok := node.Status.Allocatable[apiv1.ResourcePods]
	if !ok {
		return false
	}
	for _, podInfo := range nodeInfo.Pods() {
		if podInfo.Pod == nil {
			continue
		}
		for _, container := range podInfo.Pod.Spec.Containers {
			for _, port := range container.Ports {
				if port.HostPort == FakePodAntiAffinityHostPort {
					return true
				}
			}
		}
	}

	limit := allocatablePods.Value()
	numPods := int64(len(nodeInfo.Pods()))
	return numPods >= limit
}
