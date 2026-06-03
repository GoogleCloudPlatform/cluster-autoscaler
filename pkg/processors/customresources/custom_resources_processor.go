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

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/customresources"
	csisnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/csi/snapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/dynamicresources/snapshot"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	gke_dra "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/dynamicresources"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/customresources/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

var resources = []string{"cpu", string(nodetemplate.MemoryResource), string(nodetemplate.EphemeralStorageResource)}

type waKey struct {
	resource       string
	osDistribution string
	machineType    string
}

// Processor is a struct of gpuPartitioningProcessor and nodeTemplateCache.
//
// TODO: add unit tests that verify the order of sibling processor invocation
type Processor struct {
	nodeTemplateCache               *nodetemplate.Cache
	gpuPartitioningProcessor        *GpuPartitioningCustomResourcesProcessor
	tpuProcessor                    *tpu.TpuCustomResourcesProcessor
	labelsProcessor                 *LabelsProcessor
	draResourcePredictor            *gke_dra.ResourcePredictor // Intentionally only hooked into FilterOutNodesWithUnreadyResources().
	draCustomResourcesProcessor     DraCrpInternalOverride
	worstAllocatableOverestimation  map[waKey]float64
	worstAllocatableUnderestimation map[waKey]float64

	clock clock.PassiveClock

	// processedNodes represents a map of node name to processing time which already
	// contributed to metric being updated and should not be considered
	// any more. If the node with same node name would ever be created and
	// its creation timestamp would be AFTER a processing time stored in a map
	// this means that it's not the node time and metric should be updated again.
	processedNodes map[string]time.Time
}

type cloudProvider interface {
	MachineConfigProvider() *machinetypes.MachineConfigProvider
	NodePoolSpecForNode(node *apiv1.Node) (*gkeclient.NodePoolSpec, error)
}

// NewProcessor returns an gke internal implementation of CustomResourcesProcessor.
func NewProcessor(cache *nodetemplate.Cache) *Processor {
	return &Processor{
		nodeTemplateCache:               cache,
		gpuPartitioningProcessor:        NewGpuPartitioningCustomResourcesProcessor(),
		tpuProcessor:                    &tpu.TpuCustomResourcesProcessor{},
		labelsProcessor:                 &LabelsProcessor{},
		draResourcePredictor:            gke_dra.NewResourcePredictor(),
		draCustomResourcesProcessor:     NewDraCrpInternalOverride(),
		worstAllocatableOverestimation:  make(map[waKey]float64),
		worstAllocatableUnderestimation: make(map[waKey]float64),
		processedNodes:                  make(map[string]time.Time),
		clock:                           clock.RealClock{},
	}
}

// GetDraResourcePredictor returns the embedded DRA ResourcePredictor.
func (p *Processor) GetDraResourcePredictor() *gke_dra.ResourcePredictor {
	return p.draResourcePredictor
}

// SetContext sets context for the processor to use when the provided one is nil
func (p *Processor) SetContext(context *context.AutoscalingContext) {
	p.gpuPartitioningProcessor.SetContext(context)
}

// SetCloudProvider sets machine config provider for the processors to use.
func (p *Processor) SetCloudProvider(provider cloudProvider) {
	p.draResourcePredictor.SetCloudProvider(provider)
	p.gpuPartitioningProcessor.SetCloudProvider(provider)
	p.tpuProcessor.SetCloudProvider(provider)
	p.draCustomResourcesProcessor.SetCloudProvider(provider)
}

func (p *Processor) compareNodesWithNodeTemplates(nodes []*apiv1.Node) error {
	finalErr := utils.NewMultiErr(7)
	for _, node := range nodes {
		processedAt, processed := p.processedNodes[node.Name]
		// Do not process nodes which were already processed if the node is created before processing time
		if processed && (node.CreationTimestamp.IsZero() || processedAt.After(node.CreationTimestamp.Time)) {
			continue
		}

		nodeGroup, err := p.gpuPartitioningProcessor.context.CloudProvider.NodeGroupForNode(node)
		if err != nil {
			finalErr.Append(fmt.Errorf("Couldn't find NodeGroup for node: %s, error: %v;", node.Name, err))
			continue
		} else if nodeGroup == nil {
			// Node Group has disabled autoscaling, skipping.
			continue
		}
		mig, ok := nodeGroup.(*gke.GkeMig)
		if !ok {
			finalErr.Append(fmt.Errorf("NodeGroup for node: %s, is not a MIG;", node.Name))
			continue
		}
		osDistribution := string(util.ExtractOsDistributionFromImageType(mig.Spec().ImageType))
		templateId, err := mig.InstanceTemplateId()
		if err != nil {
			finalErr.Append(fmt.Errorf("couldn't find instance template for MIG: %v", err))
			continue
		}

		key := nodetemplate.BuildKeyForCA(templateId)
		err = p.compareAndUpdateTheMetric(node, key, mig.MachineType(), osDistribution)
		if err != nil {
			finalErr.Append(err)
		}
		if mig.Autoprovisioned() {
			key := nodetemplate.BuildKeyForNAP(mig.Spec(), osDistribution, node.Status.NodeInfo.KubeletVersion[1:], mig.GceRef().Zone)
			err = p.compareAndUpdateTheMetric(node, key, mig.MachineType(), osDistribution)
			if err != nil {
				finalErr.Append(err)
			}
		}

		p.processedNodes[node.Name] = p.clock.Now()
	}
	return finalErr.ErrorOrNil()
}

func (p *Processor) compareAndUpdateTheMetric(node *apiv1.Node, key, machineType, osDistribution string) error {
	result, err := p.nodeTemplateCache.Compare(key, node)
	if err != nil {
		return fmt.Errorf("Get an error during comparison between node %s and template, err: %v,", node.Name, err)
	}
	if result.ResourceDiff != nil {
		p.updateWorstAllocatableEstimation(result.ResourceDiff, node.Name, machineType, osDistribution)
	}
	for label := range result.MissingSystemLabels {
		metrics.Metrics.MarkMissingLabel(label)
	}
	return nil
}

// updateWorstAllocatableEstimation updates worstAllocatableOverestimation and worstAllocatableUnderestimation metrics.
func (p *Processor) updateWorstAllocatableEstimation(diff map[string]float64, nodeName, machineType, osDistribution string) {
	for _, resource := range resources {
		val, ok := diff[resource]
		k := waKey{
			resource:       resource,
			osDistribution: osDistribution,
			machineType:    machineType,
		}
		if !ok {
			klog.Warningf("Resource %s is not present on the node template for node %s ", resource, nodeName)
			continue
		}
		if val >= 0 && p.worstAllocatableOverestimation[k] <= val {
			metrics.Metrics.UpdateWorstAllocatableOverestimation(resource, machineType, osDistribution, val)
			p.worstAllocatableOverestimation[k] = val
		}
		if val <= 0 && p.worstAllocatableUnderestimation[k] <= -val {
			metrics.Metrics.UpdateWorstAllocatableUnderestimation(resource, machineType, osDistribution, -val)
			p.worstAllocatableUnderestimation[k] = -val
		}
	}
}

// FilterOutNodesWithUnreadyResources filters out nodes that are unready based on custom resources (GPU, TPU) or GKE Labels being unready.
func (p *Processor) FilterOutNodesWithUnreadyResources(context *context.AutoscalingContext, allNodes, readyNodes []*apiv1.Node, snapshot *snapshot.Snapshot, csiSnapshot *csisnapshot.Snapshot) ([]*apiv1.Node, []*apiv1.Node) {
	newAllNodes, newReadyNodes := allNodes, readyNodes

	if context.DynamicResourceAllocationEnabled {
		// draResourcePredictor.FilterOutNodesWithUnreadyResources() doesn't modify the Node lists, it just precomputes internal state based on the DRA snapshot.
		// This internal state is then used in GkeMig.TemplateNodeInfo(), so this processor should be called as early as possible in the chain.
		newAllNodes, newReadyNodes = p.draResourcePredictor.FilterOutNodesWithUnreadyResources(context, newAllNodes, newReadyNodes, snapshot)
	}

	newAllNodes, newReadyNodes = p.gpuPartitioningProcessor.FilterOutNodesWithUnreadyResources(context, newAllNodes, newReadyNodes, snapshot, csiSnapshot)
	newAllNodes, newReadyNodes = p.tpuProcessor.FilterOutNodesWithUnreadyResources(context, newAllNodes, newReadyNodes, snapshot)
	newAllNodes, newReadyNodes = p.labelsProcessor.FilterOutNodesWithMissingLabels(newAllNodes, newReadyNodes)

	if context.DynamicResourceAllocationEnabled {
		newAllNodes, newReadyNodes = p.draCustomResourcesProcessor.FilterOutNodesWithUnreadyResources(context, newAllNodes, newReadyNodes, snapshot, csiSnapshot)
	}

	p.cleanProcessedNodesIfNodeRemoved(allNodes)
	err := p.compareNodesWithNodeTemplates(newReadyNodes)
	if err != nil {
		klog.Errorf("Node template comparison err %v", err)
	}
	return newAllNodes, newReadyNodes
}

// cleanProcessedNodesIfNodeRemoved cleans up processed nodes map when node is no
// longer available
func (p *Processor) cleanProcessedNodesIfNodeRemoved(allNodes []*apiv1.Node) {
	existingNodeNames := map[string]struct{}{}
	for _, node := range allNodes {
		existingNodeNames[node.Name] = struct{}{}
	}

	for nodeName := range p.processedNodes {
		if _, exists := existingNodeNames[nodeName]; !exists {
			delete(p.processedNodes, nodeName)
		}
	}
}

// GetNodeResourceTargets returns mapping of resource names to their targets.
// This includes resources which are not yet ready to use and visible in kubernetes.
func (p *Processor) GetNodeResourceTargets(context *context.AutoscalingContext, node *apiv1.Node, nodeGroup cloudprovider.NodeGroup) ([]customresources.CustomResourceTarget, errors.AutoscalerError) {
	var resourceTargets []customresources.CustomResourceTarget
	gpuResourceTargets, err := p.gpuPartitioningProcessor.GetNodeResourceTargets(context, node, nodeGroup)
	if err != nil {
		return gpuResourceTargets, err
	}
	resourceTargets = append(resourceTargets, gpuResourceTargets...)

	tpuResourceTargets, err := p.tpuProcessor.GetNodeResourceTargets(context, node, nodeGroup)
	if err != nil {
		return tpuResourceTargets, err
	}
	resourceTargets = append(resourceTargets, tpuResourceTargets...)

	draResourceTargets, err := p.draCustomResourcesProcessor.GetNodeResourceTargets(context, node, nodeGroup)
	if err != nil {
		return draResourceTargets, err
	}
	resourceTargets = append(resourceTargets, draResourceTargets...)

	return resourceTargets, nil
}

// CleanUp cleans up processor's internal structures.
func (p *Processor) CleanUp() {
	p.gpuPartitioningProcessor.CleanUp()
	p.tpuProcessor.CleanUp()
	p.draCustomResourcesProcessor.CleanUp()
}
