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
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

const (
	nodeGroupListLogPrefix = "CSN Node Group List processor:"
)

type nodeGroupListProcessor struct {
	nodeGroupListProcessor nodegroups.NodeGroupListProcessor
	experimentsManager     experiments.Manager
}

// NewNodeGroupListProcessor create a processor for filtering out CSN node groups when there are no CSN pods to schedule.

func NewNodeGroupListProcessor(NodeGroupListProcessor nodegroups.NodeGroupListProcessor, experimentsManager experiments.Manager) *nodeGroupListProcessor {
	return &nodeGroupListProcessor{
		nodeGroupListProcessor: NodeGroupListProcessor,
		experimentsManager:     experimentsManager,
	}
}

// Process updates node groups to not trigger scale-up for non-CSN pods in CSN nodegroups.
func (p *nodeGroupListProcessor) Process(ctx *context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup, nodeInfos map[string]*framework.NodeInfo, unschedulablePods []*apiv1.Pod) ([]cloudprovider.NodeGroup, map[string]*framework.NodeInfo, error) {
	preventCSNScaleUpForNonCSNPods := p.experimentsManager.DirectLaunchBoolFlag(experiments.ColdStandbyNodesPreventCSNScaleUpForNonCSNPodsFlag)
	nodeGroups, nodeInfos, err := p.nodeGroupListProcessor.Process(ctx, nodeGroups, nodeInfos, unschedulablePods)
	if err != nil || !preventCSNScaleUpForNonCSNPods {
		return nodeGroups, nodeInfos, err
	}

	csnPodExists := false
	for _, pod := range unschedulablePods {
		if csn.IsCSNPod(pod) {
			csnPodExists = true
			break
		}
	}

	if csnPodExists {
		return nodeGroups, nodeInfos, nil
	}

	processedNodeGroups := make([]cloudprovider.NodeGroup, 0, len(nodeGroups))
	for _, ng := range nodeGroups {
		if isCSNNodeGroup(ng, nodeInfos) {
			continue
		}
		processedNodeGroups = append(processedNodeGroups, ng)
	}
	filteredLogs := len(nodeGroups) - len(processedNodeGroups)
	if filteredLogs > 0 {
		klog.Infof("%s filtered out %d CSN node groups as all the pods to be scaled-up are non-CSN pods", nodeGroupListLogPrefix, filteredLogs)
	}
	return processedNodeGroups, nodeInfos, nil
}

func isCSNNodeGroup(ng cloudprovider.NodeGroup, nodeInfos map[string]*framework.NodeInfo) bool {
	nodeInfo, ok := nodeInfos[ng.Id()]
	if !ok {
		klog.Warningf("%s failed to get node info for a node group: %q", nodeGroupListLogPrefix, ng.Id())
		return false
	}
	if !csn.IsCSNNode(nodeInfo.Node()) {
		return false
	}
	return true
}

// CleanUp cleans up the processor's internal structures. Just here to satisfy the NodeGroupListProcessor interface.
func (p *nodeGroupListProcessor) CleanUp() {
	p.nodeGroupListProcessor.CleanUp()
}
