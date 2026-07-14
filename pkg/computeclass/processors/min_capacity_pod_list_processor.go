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
	"strconv"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ca_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/scheduling"
	"k8s.io/klog/v2"

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

func (p *MinCapacityPodListProcessor) filterOutSchedulableFakePods(autoscalingCtx *ca_context.AutoscalingContext, fakePods []*apiv1.Pod) []*apiv1.Pod {
	if len(fakePods) == 0 {
		return nil
	}

	statuses, _, err := p.simulator.TrySchedulePods(autoscalingCtx.ClusterSnapshot, fakePods, false, clustersnapshot.SchedulingOptions{
		IsNodeAcceptable: func(nodeInfo *framework.NodeInfo) bool {
			return true // Accept any node currently in snapshot
		},
	})
	if err != nil {
		klog.Errorf("Failed to run FilterOutSchedulable on MinCapacity fake pods: %v", err)
		return nil
	}

	scheduledMap := make(map[types.UID]bool)
	for _, status := range statuses {
		scheduledMap[status.Pod.UID] = true
	}

	var trulyUnschedulable []*apiv1.Pod
	for _, pod := range fakePods {
		if !scheduledMap[pod.UID] {
			trulyUnschedulable = append(trulyUnschedulable, pod)
		}
	}

	klog.V(4).Infof("MinCapacityPodListProcessor: Filtered out %d schedulable fake pods", len(fakePods)-len(trulyUnschedulable))
	return trulyUnschedulable
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

	for _, ng := range autoscalingCtx.CloudProvider.NodeGroups() {
		targetSize, err := ng.TargetSize()
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
