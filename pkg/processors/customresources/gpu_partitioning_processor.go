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
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/customresources"
	csisnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/csi/snapshot"
	drasnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/dynamicresources/snapshot"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/dynamicresources"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
)

// GpuPartitioningCustomResourcesProcessor uses the default implementation supporting
// GPU resources, but it additionally implements support for GPU Partitioning.
type GpuPartitioningCustomResourcesProcessor struct {
	gpuProcessor *customresources.GpuCustomResourcesProcessor
	context      *context.AutoscalingContext
	provider     cloudProvider
}

// NewGpuPartitioningCustomResourcesProcessor returns a new GpuPartitioningCustomResourcesProcessor.
func NewGpuPartitioningCustomResourcesProcessor() *GpuPartitioningCustomResourcesProcessor {
	return &GpuPartitioningCustomResourcesProcessor{
		gpuProcessor: &customresources.GpuCustomResourcesProcessor{},
	}
}

// SetContext sets context for the processor to use when the provided one is nil
func (p *GpuPartitioningCustomResourcesProcessor) SetContext(context *context.AutoscalingContext) {
	p.context = context
}

// SetCloudProvider sets provider allowing access to machine config.
func (p *GpuPartitioningCustomResourcesProcessor) SetCloudProvider(provider cloudProvider) {
	p.provider = provider
}

// FilterOutNodesWithUnreadyResources removes nodes that should have GPU, but don't have
// it in allocatable from ready nodes list and updates their status to unready on all nodes list.
func (p *GpuPartitioningCustomResourcesProcessor) FilterOutNodesWithUnreadyResources(context *context.AutoscalingContext, allNodes, readyNodes []*apiv1.Node, draSnapshot *drasnapshot.Snapshot, csiSnapshot *csisnapshot.Snapshot) ([]*apiv1.Node, []*apiv1.Node) {
	if context == nil {
		context = p.context
	}

	return p.gpuProcessor.FilterOutNodesWithUnreadyResources(context, allNodes, readyNodes, draSnapshot, csiSnapshot)
}

// GetNodeResourceTargets returns mapping of resource names to their targets.
// This includes resources which are not yet ready to use and visible in kubernetes.
func (p *GpuPartitioningCustomResourcesProcessor) GetNodeResourceTargets(context *context.AutoscalingContext, node *apiv1.Node, nodeGroup cloudprovider.NodeGroup) ([]customresources.CustomResourceTarget, errors.AutoscalerError) {
	if context == nil {
		context = p.context
	}

	gpuTarget, err := p.GetNodeGpuTarget(context, node, nodeGroup)

	// We don't want to return a list with empty resource targets.
	if gpuTarget.ResourceCount == 0 {
		return nil, nil
	}

	return []customresources.CustomResourceTarget{gpuTarget}, err
}

// GetNodeGpuTarget returns the gpu target of a given node. This includes gpus
// that are not ready to use and visible in kubernetes. This method supports
// GPU Partitioning.
func (p *GpuPartitioningCustomResourcesProcessor) GetNodeGpuTarget(context *context.AutoscalingContext, node *apiv1.Node, nodeGroup cloudprovider.NodeGroup) (customresources.CustomResourceTarget, errors.AutoscalerError) {
	// GPU devices attached through DRA are not using node allocatable
	// to confirm their attachment, assume that will be checked in the
	// separate processor
	if dynamicresources.GpuDraDriverEnabled(node) {
		return customresources.CustomResourceTarget{}, nil
	}

	gpuPartitionSize := node.Labels[labels.GPUPartitionSizeLabel]
	gpuResource, err := p.gpuProcessor.GetNodeGpuTarget(context, node, nodeGroup)
	gpuMaxSharedClients := node.Labels[labels.GPUMaxSharedClientsLabel]
	if err != nil || (gpuPartitionSize == "" && gpuMaxSharedClients == "") {
		return gpuResource, err
	}
	gpuCount, aerr := p.provider.MachineConfigProvider().ToPhysicalGPUCount(gpuResource.ResourceType, gpuPartitionSize, gpuMaxSharedClients, machinetypes.AllocatableGpuCount(gpuResource.ResourceCount))
	if aerr != nil {
		return customresources.CustomResourceTarget{}, aerr
	}
	return customresources.CustomResourceTarget{ResourceType: gpuResource.ResourceType, ResourceCount: int64(gpuCount)}, nil
}

// CleanUp cleans up processor's internal structures.
func (p *GpuPartitioningCustomResourcesProcessor) CleanUp() {
	p.gpuProcessor.CleanUp()
}
