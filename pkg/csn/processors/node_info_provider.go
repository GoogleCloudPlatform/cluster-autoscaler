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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodeinfosprovider"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

const (
	nodeInfoProviderLogPrefix = "CSN Node Info Provider:"
)

// NodeInfoProvider adjusts nodeInfos returned by provided TemplateNodeInfoProvider
type NodeInfoProvider struct {
	provider           nodeinfosprovider.TemplateNodeInfoProvider
	cp                 cloudProvider
	experimentsManager experiments.Manager
}

func NewNodeInfoProvider(provider nodeinfosprovider.TemplateNodeInfoProvider, cp cloudProvider, experimentsManager experiments.Manager) *NodeInfoProvider {
	return &NodeInfoProvider{
		provider:           provider,
		cp:                 cp,
		experimentsManager: experimentsManager,
	}
}

func (p *NodeInfoProvider) Process(ctx *context.AutoscalingContext, nodes []*apiv1.Node, daemonsets []*appsv1.DaemonSet, taintConfig taints.TaintConfig, now time.Time) (map[string]*framework.NodeInfo, errors.AutoscalerError) {
	experimentEnabled := p.experimentsManager.DirectLaunchBoolFlag(experiments.ColdStandbyNodesProcessTemplateNodeInfosFlag)
	nodeInfos, err := p.provider.Process(ctx, nodes, daemonsets, taintConfig, now)
	if err != nil || !experimentEnabled {
		return nodeInfos, err
	}

	for _, nodeInfo := range nodeInfos {
		node := nodeInfo.Node()
		if !NodeInCSNNodeGroup(node, p.cp) {
			continue
		}
		node, err := csn.SetNodeAs(node, csn.NodeStateChilling)
		if err != nil {
			klog.Errorf("%s error while adjusting nodeInfo for node %q: %v", nodeInfoProviderLogPrefix, node.Name, err)
			continue
		}
		csn.RemoveBufferAssignment(node)
		nodeInfo.SetNode(node)
	}
	return nodeInfos, nil

}

func (p *NodeInfoProvider) CleanUp() {
	p.provider.CleanUp()
}
