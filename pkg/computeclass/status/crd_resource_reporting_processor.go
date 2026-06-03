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

package status

import (
	"fmt"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	podutils "k8s.io/autoscaler/cluster-autoscaler/utils/pod"
	computeclass "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/klog/v2"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
)

// CrdResourceReportingProcessor is responsible for updating CRD status with the amounts of resources allocated to each CRD rule.
type CrdResourceReportingProcessor struct {
	npcCrdLister npc_lister.Lister
	updatesCh    chan UpdateMessage
	matcher      computeclass.Matcher
}

// NewNodeGroupResourceReportingProcessor returns a new instance of NodeGroupResourceReportingProcessor.
// npcCrdLister is used to list CRDs and find a CRD matching a given node group.
// updatesCh is a channel where resources (requested, available, used by pods) will be reported by this processor.
// matcher is used to find a rule matching a given node group and CRD.
func NewCrdResourceReportingProcessor(npcCrdLister npc_lister.Lister, updatesCh chan UpdateMessage, matcher computeclass.Matcher) *CrdResourceReportingProcessor {
	return &CrdResourceReportingProcessor{
		npcCrdLister: npcCrdLister,
		updatesCh:    updatesCh,
		matcher:      matcher,
	}
}

func isNodeInfoReal(nodeInfo *framework.NodeInfo) bool {
	_, found := nodeInfo.Node().Annotations[labels.NodeGeneratedFromTemplateAnnotation]
	return !found
}

const BYTES_IN_GB int64 = (1024 * 1024 * 1024)

func roundUpBytesToGiB(bytes int64) int64 {
	result := bytes / BYTES_IN_GB
	if bytes%BYTES_IN_GB != 0 {
		result++
	}
	return result
}

func ReportingUnitsForResource(resourceName apiv1.ResourceName) crd.ResourceUnit {
	switch resourceName {
	case apiv1.ResourceCPU:
		return "Cores"
	case apiv1.ResourceMemory:
		return "GiB"
	case tpu.ResourceGoogleTPU:
		return "Chips"
	default:
		return "Devices"
	}
}

// Process calculates resources pertinent to each CRD rule and sends these data through the updatesCh channel.
func (m *CrdResourceReportingProcessor) Process(ctx *context.AutoscalingContext, csr *clusterstate.ClusterStateRegistry, _ time.Time) error {
	crds, err := m.npcCrdLister.ListCrds()
	if err != nil {
		return fmt.Errorf("failed to list CRDs: %w", err)
	}

	allCrdIds := make(map[CRDId]bool)
	for _, c := range crds {
		allCrdIds[CRDId{CRDLabel: c.Label(), CRDName: c.Name()}] = true
	}

	allNodes, err := ctx.ClusterSnapshot.ListNodeInfos()
	if err != nil {
		return err
	}

	type resourceKey struct {
		crdLabel     string
		crdName      string
		ruleIdx      string
		resourceName apiv1.ResourceName
	}

	type nodeGroupResources struct {
		currentCount  resource.Quantity
		targetCount   resource.Quantity
		podsRequested resource.Quantity
	}

	resourceMap := make(map[resourceKey]nodeGroupResources)

	for _, node := range allNodes {
		if node.Node() == nil {
			continue
		}
		nodeGroup, err := ctx.CloudProvider.NodeGroupForNode(node.Node())
		if err != nil {
			klog.Warningf("Failed to get nodeGroup for node %q: %v", node.Node().Name, err)
			continue
		}
		if nodeGroup == nil {
			continue
		}
		nodeGroupId := nodeGroup.Id()

		crd, cccName, err := m.npcCrdLister.NodeGroupCrd(nodeGroup)
		if err != nil {
			klog.Errorf("Failed to get CRD for nodeGroup %q: %v", nodeGroupId, err)
			continue
		}
		if crd == nil {
			continue
		}

		found, priority, _ := m.matcher.FirstMatchedRule(nodeGroup, crd)
		var ruleIdx string
		if found {
			ruleIdx = fmt.Sprintf("%d", priority)
		} else {
			ruleIdx = "ScaleUpAnyway"
		}

		resourceNames := []apiv1.ResourceName{apiv1.ResourceCPU, apiv1.ResourceMemory}
		if gpuConfig := ctx.CloudProvider.GetNodeGpuConfig(node.Node()); gpuConfig != nil {
			resourceNames = append(resourceNames, gpuConfig.ExtendedResourceName)
		}
		if tpu.NodeHasTpu(node.Node()) {
			resourceNames = append(resourceNames, tpu.ResourceGoogleTPU)
		}
		for _, resourceName := range resourceNames {
			nodeResourceAvailable, err := CalculateNodeAllocatableResource(node, resourceName)
			if err != nil {
				klog.Errorf("Failed to calculate node %s available: %v", resourceName, err)
				continue
			}
			requestedByPods, err := CalculatePodsRequestedResources(node, resourceName)
			ruleResourceKey := resourceKey{
				crdLabel:     crd.Label(),
				crdName:      cccName,
				ruleIdx:      ruleIdx,
				resourceName: resourceName,
			}
			ruleResourceInfo := resourceMap[ruleResourceKey]
			ruleResourceInfo.targetCount.Add(nodeResourceAvailable)
			ruleResourceInfo.podsRequested.Add(requestedByPods)
			if isNodeInfoReal(node) {
				// Only count resources from real nodes, not from nodes generated by the autoscaler for simulation purposes.
				ruleResourceInfo.currentCount.Add(nodeResourceAvailable)
			}
			resourceMap[ruleResourceKey] = ruleResourceInfo
		}
	}

	type crdRuleUpdate struct {
		ruleIdx      string
		resourceInfo crd.ResourceInfo
	}
	measuredAt := metav1.Now()

	// Group resource updates by CRD to apply all rule updates for a single CRD at once.
	crdUpdates := make(map[CRDId][]crdRuleUpdate)
	for key, resources := range resourceMap {
		utilizationPercentage := 0
		if resources.currentCount.MilliValue() > 0 {
			utilizationPercentage = int(resources.podsRequested.MilliValue() * 100 / resources.currentCount.MilliValue())
		}
		if key.resourceName == "memory" {
			// Convert memory from bytes to GiB for reporting.
			resources.currentCount.SetMilli(roundUpBytesToGiB(resources.currentCount.Value()) * 1000)
			resources.targetCount.SetMilli(roundUpBytesToGiB(resources.targetCount.Value()) * 1000)
			resources.podsRequested.SetMilli(roundUpBytesToGiB(resources.podsRequested.Value()) * 1000)
		}
		crdId := CRDId{CRDLabel: key.crdLabel, CRDName: key.crdName}
		info := crd.ResourceInfo{
			Name:                         crd.ResourceName(key.resourceName),
			Unit:                         ReportingUnitsForResource(key.resourceName),
			CurrentCount:                 int(resources.currentCount.Value()),
			TargetCount:                  int(resources.targetCount.Value()),
			CurrentUtilizationPercentage: utilizationPercentage,
			MeasuredAt:                   measuredAt,
		}
		crdUpdates[crdId] = append(crdUpdates[crdId], crdRuleUpdate{ruleIdx: key.ruleIdx, resourceInfo: info})
	}

	for crdId, crdRuleUpdates := range crdUpdates {
		m.updatesCh <- UpdateMessage{
			Id: crdId,
			Mutate: func(status crd.CRDStatus) {
				status.ResetAllResourceInfo()
				for _, ruleUpdate := range crdRuleUpdates {
					status.UpdateRuleResourceInfo(ruleUpdate.ruleIdx, ruleUpdate.resourceInfo)
					klog.V(4).Infof(
						"Reporting for CRD %s, rule %s, resource %s: current %d, target %d, requested by pods %d", crdId.CRDName, ruleUpdate.ruleIdx, ruleUpdate.resourceInfo.Name, ruleUpdate.resourceInfo.CurrentCount, ruleUpdate.resourceInfo.TargetCount, ruleUpdate.resourceInfo.CurrentUtilizationPercentage)
				}
			},
		}
		delete(allCrdIds, crdId)
	}

	for crdId := range allCrdIds {
		m.updatesCh <- UpdateMessage{
			Id: crdId,
			Mutate: func(status crd.CRDStatus) {
				status.ResetAllResourceInfo()
			},
		}
	}

	return nil
}

// CalculateNodeAllocatableResource returns the amount of a particular resource available on the node.
// The value is returned in Millicores for CPU and in bytes for memory.
func CalculateNodeAllocatableResource(nodeInfo *framework.NodeInfo, resourceName apiv1.ResourceName) (resource.Quantity, error) {
	nodeAllocatable, found := nodeInfo.Node().Status.Allocatable[resourceName]
	if !found {
		return resource.Quantity{}, fmt.Errorf("failed to get %v from %s", resourceName, nodeInfo.Node().Name)
	}
	if nodeAllocatable.MilliValue() == 0 {
		return resource.Quantity{}, fmt.Errorf("%v is 0 at %s", resourceName, nodeInfo.Node().Name)
	}
	return nodeAllocatable, nil
}

// CalculatePodsRequestedResources calculates the total requested amount of a given resource by all pods on the node.
func CalculatePodsRequestedResources(nodeInfo *framework.NodeInfo, resourceName apiv1.ResourceName) (resource.Quantity, error) {
	podsRequest := resource.MustParse("0")
	for _, podInfo := range nodeInfo.Pods() {
		podRequests := podutils.PodRequests(podInfo.Pod)
		resourceValue := podRequests[resourceName]
		podsRequest.Add(resourceValue)
	}
	return podsRequest, nil
}
