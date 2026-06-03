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

package nodeinfosprovider

import (
	"reflect"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodeinfosprovider"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/klog/v2"
)

// AugmentingNodeInfoProvider augments nodeInfos returned by provided TemplateNodeInfoProvider
type AugmentingNodeInfoProvider struct {
	provider                nodeinfosprovider.TemplateNodeInfoProvider
	nodePoolUpdatesEnabled  bool
	coreDistributionMetrics *coreDistributionMetrics
}

// NewAugmentingNodeInfoProvider returns a new instance of AugmentingNodeInfoProvider
func NewAugmentingNodeInfoProvider(provider nodeinfosprovider.TemplateNodeInfoProvider, nodePoolUpdatesEnabled bool) *AugmentingNodeInfoProvider {
	return &AugmentingNodeInfoProvider{
		provider:                provider,
		nodePoolUpdatesEnabled:  nodePoolUpdatesEnabled,
		coreDistributionMetrics: newCoreDistributionMetrics(),
	}
}

// Process returns augmented nodeInfos returned by basic provider
func (p *AugmentingNodeInfoProvider) Process(ctx *context.AutoscalingContext, nodes []*apiv1.Node, daemonsets []*appsv1.DaemonSet, taintConfig taints.TaintConfig, now time.Time) (map[string]*framework.NodeInfo, errors.AutoscalerError) {
	nodeInfos, err := p.provider.Process(ctx, nodes, daemonsets, taintConfig, now)
	if err != nil {
		return nodeInfos, err
	}

	if p.nodePoolUpdatesEnabled {
		nodeInfos = HandleNodePoolUpdates(ctx, nodeInfos, taintConfig)
	}

	nodeInfos, err = UpdateNodeInfosWithinNodePools(ctx, nodeInfos)
	if err == nil {
		p.coreDistributionMetrics.UpdateMetrics(ctx, nodeInfos)
	}
	logLeakedNodesFromUnInitializedUpcomingNodeGroups(ctx, nodes)
	return nodeInfos, err
}

// CleanUp cleans up internal structures recursively
func (p *AugmentingNodeInfoProvider) CleanUp() {
	p.provider.CleanUp()
}

// logLeakedNodesFromUnInitializedUpcomingNodeGroups it's important to exclude nodes that belong
// to not fully initialized node-groups from scale-up simultaions so there is no duplicatted accounting.
// To reach this state node must be created during scale-up request or right after (<1ms).
// Theorethically this situation may happen but in practice it should not be possible.
// TODO(b/342321627): Remove this function when proven it's not a problem.
func logLeakedNodesFromUnInitializedUpcomingNodeGroups(ctx *context.AutoscalingContext, nodes []*apiv1.Node) {
	if !ctx.AsyncNodeGroupsEnabled {
		return
	}
	for _, node := range nodes {
		nodeGroup, err := ctx.CloudProvider.NodeGroupForNode(node)
		if err != nil || nodeGroup == nil || reflect.ValueOf(nodeGroup).IsNil() {
			continue
		}
		mig := nodeGroup.(*gke.GkeMig)
		if mig.IsUpcoming() {
			klog.Warningf("Detected a node %s belonging to not fully initialized upcoming node-pool: %s (mig: %s)", node.Name, mig.NodePoolName(), mig.Id())
		}
	}
}
