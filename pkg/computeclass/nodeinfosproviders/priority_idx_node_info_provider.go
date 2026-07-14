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

package nodeinfosproviders

import (
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodeinfosprovider"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/klogx"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

// PriorityIdxNodeInfoProvider augments nodeInfos by injecting Compute Class priority index label.
// It is chaining provider that wraps another TemplateNodeInfoProvider and calls its Process function first and then injects priority index label into nodeInfos.
type PriorityIdxNodeInfoProvider struct {
	nodeInfoProvider   nodeinfosprovider.TemplateNodeInfoProvider
	matcher            computeclass.Matcher
	lister             lister.Lister
	nodeGroupQuota     *klogx.Quota
	nodeCrdQuota       *klogx.Quota
	experimentsManager experiments.Manager
}

// NewPriorityIdxNodeInfoProvider returns a new instance of PriorityIdxNodeInfoProvider.
func NewPriorityIdxNodeInfoProvider(nodeInfoProvider nodeinfosprovider.TemplateNodeInfoProvider, matcher computeclass.Matcher, lister lister.Lister, experimentsManager experiments.Manager) nodeinfosprovider.TemplateNodeInfoProvider {
	return &PriorityIdxNodeInfoProvider{
		nodeInfoProvider:   nodeInfoProvider,
		matcher:            matcher,
		lister:             lister,
		nodeGroupQuota:     klogx.NewLoggingQuota(5),
		nodeCrdQuota:       klogx.NewLoggingQuota(5),
		experimentsManager: experimentsManager,
	}
}

// Process returns augmented nodeInfos with priority index label.
func (p *PriorityIdxNodeInfoProvider) Process(ctx *context.AutoscalingContext, nodes []*apiv1.Node, daemonsets []*appsv1.DaemonSet, taintConfig taints.TaintConfig, now time.Time) (map[string]*framework.NodeInfo, errors.AutoscalerError) {
	nodeInfos, err := p.nodeInfoProvider.Process(ctx, nodes, daemonsets, taintConfig, now)
	if err != nil {
		return nodeInfos, err
	}

	if !computeclass.IsComputeClassMinCapacityEnabled(p.experimentsManager) {
		return nodeInfos, nil
	}

	for _, nodeInfo := range nodeInfos {
		if nodeInfo == nil || nodeInfo.Node() == nil {
			continue
		}
		templateNode := nodeInfo.Node()

		// Get node group for node
		nodeGroup, err := ctx.CloudProvider.NodeGroupForNode(templateNode)
		if err != nil {
			klogx.V(4).UpTo(p.nodeGroupQuota).Infof("PriorityIdxNodeInfoProvider: Failed to get node group for template node %s: %v", templateNode.Name, err)
			continue
		} else if nodeGroup == nil {
			continue
		}

		// Get CCC for node group
		ccc, _, err := p.lister.NodeGroupCrd(nodeGroup)
		if err != nil {
			klogx.V(4).UpTo(p.nodeCrdQuota).Infof("PriorityIdxNodeInfoProvider: Failed to get NodeGroupCrd for node group %s: %v", nodeGroup.Id(), err)
			continue
		} else if ccc == nil {
			continue
		}

		// Find which CCC rule matches the node group
		matched, groupIndex, _ := p.matcher.FirstMatchedRule(nodeGroup, ccc)
		if !matched {
			groupIndex = -1
		}

		// Inject priority index label
		if templateNode.Labels == nil {
			templateNode.Labels = make(map[string]string)
		}
		templateNode.Labels[labels.ComputeClassPriorityIdxLabel] = strconv.Itoa(groupIndex)
	}

	klogx.V(4).Over(p.nodeGroupQuota).Infof("PriorityIdxNodeInfoProvider: Suppressed %d node group lookup failures", -p.nodeGroupQuota.Left())
	klogx.V(4).Over(p.nodeCrdQuota).Infof("PriorityIdxNodeInfoProvider: Suppressed %d NodeGroupCrd lookup failures", -p.nodeCrdQuota.Left())
	p.nodeGroupQuota.Reset()
	p.nodeCrdQuota.Reset()

	return nodeInfos, nil
}

// CleanUp cleans up internal structures.
func (p *PriorityIdxNodeInfoProvider) CleanUp() {
	p.nodeInfoProvider.CleanUp()
}
