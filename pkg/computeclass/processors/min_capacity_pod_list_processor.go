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

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	ca_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/scheduling"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	cc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/fairness"
	"k8s.io/klog/v2"
)

const MinCapacityPodListProcessorName = "cc-min-capacity-pod-list-processor"

// MinCapacityPodListProcessor injects fake pods to enforce minimum capacity defined in ComputeClass CRDs.
type MinCapacityPodListProcessor struct {
	ccLister         cc_lister.Lister
	simulator        *scheduling.HintingSimulator
	fairnessEnforcer fairness.FairnessEnforcer
}

// NewMinCapacityPodListProcessor creates a MinCapacityPodListProcessor.
func NewMinCapacityPodListProcessor(ccLister cc_lister.Lister, fairnessEnforcer fairness.FairnessEnforcer) *MinCapacityPodListProcessor {
	return &MinCapacityPodListProcessor{
		ccLister:         ccLister,
		simulator:        scheduling.NewHintingSimulator(),
		fairnessEnforcer: fairnessEnforcer,
	}
}

// Process evaluates deficits based on the `targetNodeCount` in ComputeClass CRDs
// and injects synthetic pods to meet the minimum capacity.
func (p *MinCapacityPodListProcessor) Process(
	autoscalingCtx *ca_context.AutoscalingContext,
	unschedulablePods []*apiv1.Pod) ([]*apiv1.Pod, error) {

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
		klog.V(5).Infof("MinCapacityPodListProcessor: No CRDs found, skipping")
		return unschedulablePods, nil
	}
	klog.V(5).Infof("MinCapacityPodListProcessor: Running for %d CRDs", len(crds))

	nodeInfos, err := autoscalingCtx.ClusterSnapshot.ListNodeInfos()
	if err != nil {
		klog.Errorf("Failed to list nodes for MinCapacity processor: %v", err)
		return unschedulablePods, nil
	}

	var fakePods []*apiv1.Pod

	saturatedByCCAndRule, saturatedByCC := computeSaturatedNodeCounts(nodeInfos)

	for _, c := range crds {
		crdName := c.Name()
		var totalPriorityFakePods int

		// Evaluate fake pods for priority rules.
		for ruleIdx, r := range c.Rules() {
			if r.TargetNodeCount() == nil {
				continue
			}
			target := *r.TargetNodeCount()

			saturatedNodes := 0
			if rulesMap, ok := saturatedByCCAndRule[crdName]; ok {
				saturatedNodes = rulesMap[fmt.Sprintf("%d", ruleIdx)]
			}

			shortfall := target - saturatedNodes
			if shortfall > 0 {
				idx := ruleIdx
				for i := range shortfall {
					fakePods = append(fakePods, buildFakePod(crdName, &idx, i))
				}
				klog.V(4).Infof("MinCapacityPodListProcessor: Generated %d fake pods for rule %d of CRD %s (target %d, saturated %d)", shortfall, ruleIdx, crdName, target, saturatedNodes)
				totalPriorityFakePods += shortfall
			}
		}

		// Evaluate fake pods for top-level ComputeClass.
		if c.TargetNodeCount() != nil {
			target := *c.TargetNodeCount()

			saturatedCCCNodeCount := saturatedByCC[crdName]

			if shortfall := target - totalPriorityFakePods - saturatedCCCNodeCount; shortfall > 0 {
				for i := range shortfall {
					fakePods = append(fakePods, buildFakePod(crdName, nil, i))
				}
				klog.V(4).Infof("MinCapacityPodListProcessor: Generated %d fake pods for top-level ComputeClass of CRD %s (target %d, priority pods %d, saturated CCC nodes %d)", shortfall, crdName, target, totalPriorityFakePods, saturatedCCCNodeCount)
			}
		}
	}

	if len(fakePods) == 0 {
		return unschedulablePods, nil
	}

	fakePods = p.filterOutSchedulableFakePods(autoscalingCtx, fakePods)

	if len(fakePods) > 0 && canBeAdmitted {
		unschedulablePods = append(unschedulablePods, fakePods...)
	}

	return unschedulablePods, nil
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
		// If we fail to filter out schedulable pods, we might end up with more pods than needed.
		// This is a safe fallback.
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
