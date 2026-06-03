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
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodes"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/klog/v2"
)

// CloudProvider is the subset of GkeCloudProvider needed for
// processors.TotalMinSizeProcessor.
type CloudProvider interface {
	GkeMigForNode(node *apiv1.Node) (*gke.GkeMig, error)
}

// TotalMinSizeProcessor enforces total min size limit for the node pool.
// This processor works in tandem with change of semantics for the node group
// MinSize function.
// For more details refer to: go/improve-nodepool-size-control-dd
type TotalMinSizeProcessor struct {
	cloudProvider CloudProvider
}

type nodepoolMetadata struct {
	totalSizeLimitEnabled      bool
	sizeAfterCanditatesRemoval int
	totalMinSize               int
}

// NewMinSizeProcessor creates a new ScaleDownSetProcessor.
func NewMinSizeProcessor(cloudProvider CloudProvider) *TotalMinSizeProcessor {
	return &TotalMinSizeProcessor{
		cloudProvider: cloudProvider,
	}
}

// FilterUnremovableNodes filters candidates that their removal will cause node group size going below min size.
func (p *TotalMinSizeProcessor) FilterUnremovableNodes(ctx *context.AutoscalingContext, scaleDownCtx *nodes.ScaleDownContext, candidates []simulator.NodeToBeRemoved) ([]simulator.NodeToBeRemoved, []simulator.UnremovableNode) {
	nodepoolMetadataMap := map[string]nodepoolMetadata{}
	nodesToBeRemoved := []simulator.NodeToBeRemoved{}
	unremovableNodes := []simulator.UnremovableNode{}
	for _, c := range candidates {
		mig, err := p.cloudProvider.GkeMigForNode(c.Node)
		if err != nil {
			klog.Errorf("Skipping removal of node: %s, due to error while fetching mig: %v", c.Node.Name, err)
			unremovableNodes = append(unremovableNodes, simulator.UnremovableNode{Node: c.Node, Reason: simulator.UnexpectedError})
			continue
		}
		if mig == nil {
			klog.Warningf("Skipping removal of node: %s, corresponding MIG not found", c.Node.Name)
			unremovableNodes = append(unremovableNodes, simulator.UnremovableNode{Node: c.Node, Reason: simulator.UnexpectedError})
			continue
		}
		nm, found := nodepoolMetadataMap[getNodePoolKey(mig)]
		if !found {
			totalMinSize := mig.TotalMinSize()
			nodePoolSize, err := mig.NodePoolTargetSize()
			if err != nil {
				klog.Errorf("Couldn't get size of the nodepool (%s), received error: %v, overwriting nodePoolSize to totalMinSize (%d)", getNodePoolKey(mig), err, totalMinSize)
				nodePoolSize = totalMinSize
			}
			numberOfNodesBeingDeleted := getNodesBeingDeletedInNodeGroup(ctx, scaleDownCtx, c.Node)

			actualNodePoolSize := nodePoolSize - numberOfNodesBeingDeleted

			nm = nodepoolMetadata{
				totalSizeLimitEnabled:      mig.TotalSizeLimitEnabled(),
				sizeAfterCanditatesRemoval: actualNodePoolSize,
				totalMinSize:               totalMinSize,
			}
		}

		if !nm.totalSizeLimitEnabled {
			nodesToBeRemoved = append(nodesToBeRemoved, c)
		} else if nm.sizeAfterCanditatesRemoval > nm.totalMinSize {
			nm.sizeAfterCanditatesRemoval--
			nodesToBeRemoved = append(nodesToBeRemoved, c)
		} else {
			klog.V(2).Infof("Not removing node %s, which is from nodepool with a total min, as after removal current size would drop below the total min size: %d", c.Node.Name, nm.totalMinSize)
			unremovableNodes = append(unremovableNodes, simulator.UnremovableNode{Node: c.Node, Reason: simulator.NodeGroupMinSizeReached})
		}
		nodepoolMetadataMap[getNodePoolKey(mig)] = nm
	}

	return nodesToBeRemoved, unremovableNodes
}

// CleanUp is called at CA termination
func (p *TotalMinSizeProcessor) CleanUp() {}

func getNodesBeingDeletedInNodeGroup(ctx *context.AutoscalingContext, scaleDownCtx *nodes.ScaleDownContext, node *apiv1.Node) int {
	numberOfNodesBeingDeletedDefaultValue := 0
	nodeGroup, err := ctx.CloudProvider.NodeGroupForNode(node)

	if err != nil {
		return numberOfNodesBeingDeletedDefaultValue
	}
	if nodeGroup == nil {
		klog.Errorf("Node group for node %s not found", node.Name)
		return numberOfNodesBeingDeletedDefaultValue
	}
	if scaleDownCtx.ActuationStatus == nil {
		return numberOfNodesBeingDeletedDefaultValue
	}

	return scaleDownCtx.ActuationStatus.DeletionsCount(nodeGroup.Id())
}

func getNodePoolKey(mig *gke.GkeMig) string {
	if mig.BlueGreenInfo() == nil {
		return mig.NodePoolName()
	}
	return fmt.Sprintf("%s:%s", mig.NodePoolName(), mig.BlueGreenInfo().Color)
}
