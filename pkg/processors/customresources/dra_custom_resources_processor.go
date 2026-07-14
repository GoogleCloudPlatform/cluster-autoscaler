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

package customresources

import (
	"fmt"
	"time"

	gke_api_beta "google.golang.org/api/container/v1beta1"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/customresources"
	csisnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/csi/snapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/dynamicresources/snapshot"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/dynamicresources"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/klog/v2"
)

const draNodepoolReadinessTimeout = 5 * time.Minute

// DraCrpInternalOverride is a thin wrapper around OSS DraCustomResourcesProcessor
// that implements the CustomResourcesProcessor interface, while changing the behaviour
// of GetNodeResourceTargets to return the resources that are available on the node.
//
// This should be only treated as a stop-gap measure until the OSS CustomResourcesProcessor
// is modified to be capable of fetching the resources available on the node when those
// are exposed via DRA.
type DraCrpInternalOverride struct {
	wrap     *customresources.DraCustomResourcesProcessor
	provider cloudProvider
}

var _ customresources.CustomResourcesProcessor = &DraCrpInternalOverride{}

func NewDraCrpInternalOverride() DraCrpInternalOverride {
	return DraCrpInternalOverride{
		wrap: customresources.NewDraCustomResourcesProcessor(),
	}
}

func (p *DraCrpInternalOverride) SetCloudProvider(provider cloudProvider) {
	p.provider = provider
}

func (p *DraCrpInternalOverride) CleanUp() {
	p.wrap.CleanUp()
}

func (p *DraCrpInternalOverride) FilterOutNodesWithUnreadyResources(autoscalingCtx *context.AutoscalingContext, allNodes, readyNodes []*apiv1.Node, draSnapshot *snapshot.Snapshot, csiSnapshot *csisnapshot.Snapshot) ([]*apiv1.Node, []*apiv1.Node) {
	newReadyNodes := make([]*apiv1.Node, 0, len(readyNodes))
	unreadyNodes := make(map[string]*apiv1.Node)

	for _, node := range readyNodes {
		if !gkelabels.IsGkeDraNode(node.Labels) {
			newReadyNodes = append(newReadyNodes, node)
			continue
		}

		nodeAge := time.Since(node.CreationTimestamp.Time)
		if nodeAge > draNodepoolReadinessTimeout {
			newReadyNodes = append(newReadyNodes, node)
			continue
		}

		_, err := p.resolveNodePoolSpec(node, nil)
		if err != nil {
			// If we can't find the node pool spec, it means CA doesn't have the context for this node yet.
			// Since it's a DRA node, we should treat it as unready until we can verify its resource slices.
			// This ensures the node is treated as an upcoming node and a placeholder is injected.
			klog.V(2).Infof("DRA node %s is set to unready because node pool spec is not available: %v", node.Name, err)
			unreadyNodes[node.Name] = kubernetes.GetUnreadyNodeCopy(node, kubernetes.ResourceUnready)
			continue
		}
		newReadyNodes = append(newReadyNodes, node)
	}

	modifiedAllNodes := make([]*apiv1.Node, 0, len(allNodes))
	for _, node := range allNodes {
		if unreadyNode, found := unreadyNodes[node.Name]; found {
			modifiedAllNodes = append(modifiedAllNodes, unreadyNode)
		} else {
			modifiedAllNodes = append(modifiedAllNodes, node)
		}
	}

	return p.wrap.FilterOutNodesWithUnreadyResources(autoscalingCtx, modifiedAllNodes, newReadyNodes, draSnapshot, csiSnapshot)
}

// GetNodeResourceTargets returns the resources that are available on the node.
func (p *DraCrpInternalOverride) GetNodeResourceTargets(ctx *context.AutoscalingContext, node *apiv1.Node, nodeGroup cloudprovider.NodeGroup) ([]customresources.CustomResourceTarget, errors.AutoscalerError) {
	if !gkelabels.IsGkeDraNode(node.Labels) {
		return nil, nil
	}

	nodePoolSpec, err := p.resolveNodePoolSpec(node, nodeGroup)
	if err != nil {
		klog.Warningf("Unable to calculate resource targets for DRA node: %v", err)
		return nil, nil
	}

	if dynamicresources.GpuDraDriverEnabled(node) {
		gpuResources, err := getGpuResources(node, nodePoolSpec.Accelerators)
		if err != nil {
			klog.Warningf("Unable to calculate GPU resources for node %s: %v", node.Name, err)
			return nil, nil
		}
		return gpuResources, nil
	}

	if dynamicresources.TpuDraDriverEnabled(node) {
		tpuResources, err := p.getTpuResources(node, nodePoolSpec.MachineType)
		if err != nil {
			klog.Warningf("Unable to calculate TPU resources for node %s: %v", node.Name, err)
			return nil, nil
		}
		return tpuResources, nil
	}

	return nil, nil
}

func (p *DraCrpInternalOverride) resolveNodePoolSpec(node *apiv1.Node, nodeGroup cloudprovider.NodeGroup) (*gkeclient.NodePoolSpec, error) {
	if nodeGroup != nil {
		mig, ok := nodeGroup.(*gke.GkeMig)
		if !ok {
			return nil, fmt.Errorf("unable to get node pool spec for node %s: node's node group is not a GkeMig, type: %T", node.Name, nodeGroup)
		}

		return mig.Spec(), nil
	}

	if p.provider == nil {
		return nil, fmt.Errorf("unable to get node pool spec for node %s: cloud provider is not set", node.Name)
	}

	nodePoolSpec, err := p.provider.NodePoolSpecForNode(node)
	if err != nil {
		return nil, fmt.Errorf("unable to get node pool spec for node %s: %v", node.Name, err)
	}

	return nodePoolSpec, nil
}

func (p *DraCrpInternalOverride) getTpuResources(node *apiv1.Node, machineType string) ([]customresources.CustomResourceTarget, error) {
	tpuCount, err := p.provider.MachineConfigProvider().GetTpuCountForMachineType(machineType)
	if err != nil {
		return nil, fmt.Errorf("unable to get TPU count for machine type %s, err: %v", machineType, err)
	}

	tpuType, exists := node.Labels[gkelabels.TPULabel]
	if !exists {
		return nil, fmt.Errorf("Node %s has no TPU type label %s, while requesting TPU resources via DRA", node.Name, gkelabels.TPULabel)
	}

	return []customresources.CustomResourceTarget{
		{
			ResourceType:  tpuType,
			ResourceCount: tpuCount,
		},
	}, nil
}

func getGpuResources(node *apiv1.Node, accelerators []*gke_api_beta.AcceleratorConfig) ([]customresources.CustomResourceTarget, error) {
	if len(accelerators) == 0 {
		return nil, nil
	}

	if len(accelerators) > 1 {
		return nil, fmt.Errorf("Node %s has multiple accelerator types, unable to process DRA resources", node.Name)
	}

	acceleratorCount := accelerators[0].AcceleratorCount
	acceleratorType, found := node.Labels[gkelabels.GPULabel]
	if !found {
		return nil, fmt.Errorf("Node %s has no GPU type label %s, while requesting GPU resources via DRA", node.Name, gkelabels.GPULabel)
	}

	return []customresources.CustomResourceTarget{
		{
			ResourceType:  acceleratorType,
			ResourceCount: acceleratorCount,
		},
	}, nil
}
