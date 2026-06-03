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
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaleup/equivalence"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/klog/v2"
)

type nodeGroupListProcessor struct {
	nodeGroupListProcessor nodegroups.NodeGroupListProcessor
}

// NewProcessor creates an instance of processor.
func NewNodeGroupListProcessor(NodeGroupListProcessor nodegroups.NodeGroupListProcessor) *nodeGroupListProcessor {
	return &nodeGroupListProcessor{
		nodeGroupListProcessor: NodeGroupListProcessor,
	}
}

// Process processes the nodegroups and filters out slice-of-hardware node groups
// which are not the smallest machine size which can fit the slice-of-hardware pod.
// Reservations are respected. If a slice-of-hardware pod consumes reservation then
// the pod can only fit on node group with same machine type as reservation.
// This is handled by the scheduling simulation.
func (p *nodeGroupListProcessor) Process(ctx *context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup, nodeInfos map[string]*framework.NodeInfo, unschedulablePods []*apiv1.Pod) ([]cloudprovider.NodeGroup, map[string]*framework.NodeInfo, error) {
	nodeGroups, nodeInfos, err := p.nodeGroupListProcessor.Process(ctx, nodeGroups, nodeInfos, unschedulablePods)
	if err != nil {
		klog.Errorf("Cannot process nodegroups from NAP, error: %v", err)
		return nodeGroups, nodeInfos, err
	}

	// Split node groups between non slice-of-hardware and slice-of-hardware.
	// Non slice-of-hardware groups are not filtered so all of them are returned.
	// Only the smallest machine slice-of-hardware node groups which can fit
	// unscheduled pods are returned.
	// This prevents a CA from using larger machine type than necessary to fit
	// slice-of-hardware pod + daemonsets.
	sohwNodeGroups, otherNodeGroups := splitSoHWNodeGroups(nodeGroups, nodeInfos)
	// Group equivalent pods and only check scheduling for one pod from each
	// equivalence group.
	unschedulablePodGroups := equivalence.BuildPodGroups(unschedulablePods)

	// Find the smallest node group which can schedule each pod.
	smallestSohwNodeGroupsForPod := make(map[int]map[int]bool, len(unschedulablePodGroups))
	for pi := range unschedulablePodGroups {
		smallestSohwNodeGroupsForPod[pi] = map[int]bool{}
	}
	smallestMachineTypeInfo := map[int]machinetypes.MachineType{}
	// Iterate over each nodeGroup once and only fork cluster snapshot once per
	// nodeGroup. Doing this is much less expensive than forking cluster snapshot
	// for each nodeGroup and unschedulable pod group.
	for ni, nodeGroup := range sohwNodeGroups {
		nodeInfo, found := nodeInfos[nodeGroup.Id()]
		if !found {
			klog.Warningf("Failed to get template node info for a node group")
			continue
		}
		ctx.ClusterSnapshot.Fork()
		defer ctx.ClusterSnapshot.Revert()
		// Add node to cluster snapshot
		if err := ctx.ClusterSnapshot.AddNodeInfo(nodeInfo); err != nil {
			klog.Errorf("Couldn't add node to cluster snapshot: %v", err)
			continue
		}
		// Check whether each unschedulablePodGroup can be scheduled on this node group.
		for pi, unschedulablePodGroup := range unschedulablePodGroups {
			// unschedulablePod is sample pod from equivalence group.
			unschedulablePod := unschedulablePodGroup.Pods[0]
			if unschedulablePod == nil {
				continue
			}
			// Check if slice of hardware pod.
			podRequirements := podrequirements.GetRequirements(unschedulablePod)
			if podRequirements.PodCapacity != "1" {
				continue
			}
			// Check if pod schedulable on nodegroup.
			if predicateErr := ctx.ClusterSnapshot.CheckPredicates(unschedulablePod, nodeInfo.Node().Name); predicateErr != nil {
				// Not schedulable
				continue
			}
			// This node can fit pod.
			mig, ok := nodeGroup.(*gke.GkeMig)
			if !ok {
				klog.Errorf("Couldn't cast to GkeMig: %v", nodeGroup)
				continue
			}
			machineTypeName := mig.Spec().MachineType
			currentMachineTypeInfo, err := mig.MachineConfigProvider().ToMachineType(machineTypeName)
			if err != nil {
				klog.Errorf("Error getting machine type: %s", err)
				continue
			}

			// Keep track of all the smallest machine sized node groups.
			if (machinetypes.MachineType{}) == smallestMachineTypeInfo[pi] || smallestMachineTypeInfo[pi].Name == currentMachineTypeInfo.Name {
				smallestMachineTypeInfo[pi] = currentMachineTypeInfo
				smallestSohwNodeGroupsForPod[pi][ni] = true
			} else if machinetypes.IsLargerThan(smallestMachineTypeInfo[pi], currentMachineTypeInfo) {
				smallestMachineTypeInfo[pi] = currentMachineTypeInfo
				// New smallest fitting node group found.
				smallestSohwNodeGroupsForPod[pi] = map[int]bool{}
				smallestSohwNodeGroupsForPod[pi][ni] = true
			}
		}
	}
	useSohwNodeGroups := map[int]bool{}
	for pi, unschedulablePodGroup := range unschedulablePodGroups {
		unschedulablePod := unschedulablePodGroup.Pods[0]
		// Must use the smallest sohwNodeGroups that can schedule this pod.
		for k, v := range smallestSohwNodeGroupsForPod[pi] {
			useSohwNodeGroups[k] = v
		}
		// Debug info.
		if machineInfo, found := smallestMachineTypeInfo[pi]; found {
			klog.V(5).Infof("SoHW Processor: Smallest machine-type for pod %s: %s", unschedulablePod.Name, machineInfo.Name)
		}
	}

	// Unique node groups should be returned.
	// Node groups should not be duplicated.
	smallestSohwNodeGroups := make([]cloudprovider.NodeGroup, 0, len(useSohwNodeGroups))
	for i := range useSohwNodeGroups {
		smallestSohwNodeGroups = append(smallestSohwNodeGroups, sohwNodeGroups[i])
	}

	mergedNodeGroups := append(otherNodeGroups, smallestSohwNodeGroups...)
	return mergedNodeGroups, nodeInfos, nil
}

func splitSoHWNodeGroups(nodeGroups []cloudprovider.NodeGroup, nodeInfos map[string]*framework.NodeInfo) ([]cloudprovider.NodeGroup, []cloudprovider.NodeGroup) {
	sohwNodeGroups := []cloudprovider.NodeGroup{}
	otherNodeGroups := []cloudprovider.NodeGroup{}
	for _, nodeGroup := range nodeGroups {
		nodeInfo, found := nodeInfos[nodeGroup.Id()]
		if !found {
			klog.Warningf("Failed to get template node info for a node group")
			otherNodeGroups = append(otherNodeGroups, nodeGroup)
			continue
		}
		node := nodeInfo.Node()
		if node == nil {
			otherNodeGroups = append(otherNodeGroups, nodeGroup)
			continue
		}
		if _, found := node.Labels[gkelabels.PodCapacityLabel]; found {
			sohwNodeGroups = append(sohwNodeGroups, nodeGroup)
			continue
		}
		otherNodeGroups = append(otherNodeGroups, nodeGroup)
	}
	return sohwNodeGroups, otherNodeGroups
}

// CleanUp cleans up the processor's internal structures. Just here to satisfy the NodeGroupListProcessor interface.
func (p *nodeGroupListProcessor) CleanUp() {
	p.nodeGroupListProcessor.CleanUp()
}
