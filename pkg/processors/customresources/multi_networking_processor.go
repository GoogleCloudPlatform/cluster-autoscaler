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
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	netutil "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/networking/util"
	"k8s.io/klog/v2"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
)

// MultiNetworkingProcessor filters out nodes belonging to multi-networking pools
// whose multi-networking resources have not been populated in allocatable yet (e.g., netd initializing).
type MultiNetworkingProcessor struct {
	context *ca_context.AutoscalingContext
}

// SetContext sets the AutoscalingContext for MultiNetworkingProcessor.
func (p *MultiNetworkingProcessor) SetContext(context *ca_context.AutoscalingContext) {
	p.context = context
}

// FilterOutNodesWithUnreadyResources removes nodes that belong to multi-networking node pools
// but don't have multi-networking resources in allocatable yet from ready nodes list
// and updates their status to unready on all nodes list.
func (p *MultiNetworkingProcessor) FilterOutNodesWithUnreadyResources(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, allNodes, readyNodes []*v1.Node) ([]*v1.Node, []*v1.Node) {
	if autoscalingCtx == nil {
		autoscalingCtx = p.context
	}

	newReadyNodes := make([]*v1.Node, 0, len(readyNodes))
	unreadyNodes := make(map[string]bool)

	for _, node := range readyNodes {
		if p.isNodeReady(ctx, autoscalingCtx, node) {
			newReadyNodes = append(newReadyNodes, node)
		} else {
			unreadyNodes[node.Name] = true
		}
	}

	newAllNodes := make([]*v1.Node, 0, len(allNodes))
	for _, node := range allNodes {
		if unreadyNodes[node.Name] {
			newAllNodes = append(newAllNodes, kubernetes.GetUnreadyNodeCopy(node, kubernetes.ResourceUnready))
		} else {
			newAllNodes = append(newAllNodes, node)
		}
	}

	return newAllNodes, newReadyNodes
}

func (p *MultiNetworkingProcessor) isNodeReady(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, node *v1.Node) bool {
	mig := p.getGkeMig(ctx, autoscalingCtx, node)
	if mig == nil || mig.Spec() == nil || len(mig.Spec().NetworkConfigs) == 0 {
		return true // Not a multi-networking node pool (or unmanaged node)
	}

	templateInfo, err := mig.TemplateNodeInfo(ctx)
	if err != nil || templateInfo == nil || templateInfo.Node() == nil {
		klog.Warningf("Failed to get template node info for node pool %q: %v. Keeping node %q in ready list.", mig.Id(), err, node.Name)
		return true
	}

	// Verify that every multi-networking resource expected by the template is present on the live node
	for resName, templateQty := range templateInfo.Node().Status.Allocatable {
		if netutil.IsNetworkResource(resName.String()) && templateQty.Value() > 0 {
			nodeQty, ok := node.Status.Allocatable[resName]
			if !ok || nodeQty.Value() == 0 {
				return false // Missing expected network resource (netd or device plugin still initializing)
			}
		}
	}

	return true
}

func (p *MultiNetworkingProcessor) getGkeMig(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, node *v1.Node) *gke.GkeMig {
	if autoscalingCtx == nil || autoscalingCtx.CloudProvider == nil {
		return nil
	}
	nodeGroup, err := autoscalingCtx.CloudProvider.NodeGroupForNode(ctx, node)
	if err != nil {
		klog.Warningf("Failed to get node group for node %q: %v. Skipping multi-network readiness check.", node.Name, err)
		return nil
	}
	if nodeGroup == nil {
		return nil
	}
	gkeMig, ok := nodeGroup.(*gke.GkeMig)
	if !ok {
		return nil
	}
	return gkeMig
}
