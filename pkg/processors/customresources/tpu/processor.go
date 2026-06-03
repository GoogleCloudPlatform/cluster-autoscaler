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

package tpu

import (
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/customresources"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/dynamicresources/snapshot"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/dynamicresources"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/klog/v2"
)

type TpuCustomResourcesProcessor struct {
	provider cloudProvider
}

type cloudProvider interface {
	MachineConfigProvider() *machinetypes.MachineConfigProvider
}

func (p *TpuCustomResourcesProcessor) SetCloudProvider(provider cloudProvider) {
	p.provider = provider
}

func (p *TpuCustomResourcesProcessor) FilterOutNodesWithUnreadyResources(context *context.AutoscalingContext, allNodes, readyNodes []*apiv1.Node, _ *snapshot.Snapshot) ([]*apiv1.Node, []*apiv1.Node) {
	newAllNodes := make([]*apiv1.Node, 0)
	newReadyNodes := make([]*apiv1.Node, 0)
	nodesWithUnreadyTpu := make(map[string]*apiv1.Node)
	for _, node := range readyNodes {
		// TPU devices attached through DRA are not using node allocatable
		// to confirm their attachment, assume that node is ready
		// and will be checked in the separate processor
		if dynamicresources.TpuDraDriverEnabled(node) {
			newReadyNodes = append(newReadyNodes, node)
			continue
		}

		_, hasTpuLabel := node.Labels[gkelabels.TPULabel]
		tpuAllocatable, hasTpuAllocatable := node.Status.Allocatable[tpu.ResourceGoogleTPU]
		// We expect node to have TPU based on label, but it doesn't show up
		// on node object. Assume the node is still not fully started (installing
		// TPU drivers).
		if hasTpuLabel && (!hasTpuAllocatable || tpuAllocatable.IsZero()) {
			klog.V(3).Infof("Overriding status of node %v, which seems to have unready TPU", node.Name)
			nodesWithUnreadyTpu[node.Name] = kubernetes.GetUnreadyNodeCopy(node, kubernetes.ResourceUnready)
		} else {
			newReadyNodes = append(newReadyNodes, node)
		}
	}
	// Override any node with unready TPU with its "unready" copy
	for _, node := range allNodes {
		if newNode, found := nodesWithUnreadyTpu[node.Name]; found {
			newAllNodes = append(newAllNodes, newNode)
		} else {
			newAllNodes = append(newAllNodes, node)
		}
	}
	return newAllNodes, newReadyNodes
}

func (p *TpuCustomResourcesProcessor) GetNodeResourceTargets(context *context.AutoscalingContext, node *apiv1.Node, nodeGroup cloudprovider.NodeGroup) ([]customresources.CustomResourceTarget, errors.AutoscalerError) {
	// TPU devices attached through DRA are not using node allocatable
	// to confirm their attachment, assume that will be checked in the
	// separate processor
	if dynamicresources.TpuDraDriverEnabled(node) {
		return nil, nil
	}

	tpuTarget, err := p.GetNodeTpuTarget(gkelabels.TPULabel, node, nodeGroup)

	// We don't want to return a list with empty resource targets.
	if tpuTarget.ResourceCount == 0 {
		return nil, nil
	}

	return []customresources.CustomResourceTarget{tpuTarget}, err
}

func (p *TpuCustomResourcesProcessor) GetNodeTpuTarget(TPULabel string, node *apiv1.Node, nodeGroup cloudprovider.NodeGroup) (customresources.CustomResourceTarget, errors.AutoscalerError) {
	tpuLabel, found := node.Labels[TPULabel]
	if !found {
		return customresources.CustomResourceTarget{}, nil
	}

	tpuAllocatable, found := node.Status.Allocatable[tpu.ResourceGoogleTPU]
	if found && tpuAllocatable.Value() > 0 {
		crt := customresources.CustomResourceTarget{ResourceType: tpuLabel, ResourceCount: tpuAllocatable.Value()}
		return crt, nil
	}

	// If no allocatable TPUs found, get the tpu count from the machine type.
	// For tpus machine type also tells us how many tpus are expected.
	if machineType, found := node.Labels[apiv1.LabelInstanceTypeStable]; found {
		tpuCount, err := p.provider.MachineConfigProvider().GetTpuCountForMachineType(machineType)
		if err != nil {
			klog.Warningf("Node has TPU label but couldn't get tpu count for node: %v with machine type: %v", node.Name, machineType)
		} else if tpuCount > 0 {
			return customresources.CustomResourceTarget{ResourceType: tpuLabel, ResourceCount: tpuCount}, nil
		}
	} else {
		klog.Warningf("Failed to get machine type from labels for node: %v", node.Name)
	}

	// For non-autoscaled node groups we don't have access to the node template. So skip any such node.
	if nodeGroup == nil {
		return customresources.CustomResourceTarget{}, errors.NewAutoscalerError(errors.InternalError, "node without with tpu label, without capacity not belonging to autoscaled node group")
	}

	tr, found, err := getTpuFromTemplate(nodeGroup)
	if err != nil {
		return customresources.CustomResourceTarget{}, err
	}
	if !found {
		// if template does not define tpus we assume node will not have any even if it has tpu label
		klog.Warningf("Template does not define TPUs even though node from its node group does; node=%v", node.Name)
		return customresources.CustomResourceTarget{}, nil
	}
	crt := customresources.CustomResourceTarget{ResourceType: tpuLabel, ResourceCount: tr.Count}
	return crt, nil

}

// CleanUp cleans up processor's internal structures.
func (p *TpuCustomResourcesProcessor) CleanUp() {
}
