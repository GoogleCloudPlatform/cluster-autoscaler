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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/klog/v2"
)

type shardAwareNodeGroupListProcessor struct {
	nodeGroupListProcessor nodegroups.NodeGroupListProcessor
	lister                 lister.Lister
}

func NewShardAwareNodeGroupListProcessor(nodeGroupListProcessor nodegroups.NodeGroupListProcessor, l lister.Lister) *shardAwareNodeGroupListProcessor {
	return &shardAwareNodeGroupListProcessor{
		nodeGroupListProcessor: nodeGroupListProcessor,
		lister:                 l,
	}
}

func (p *shardAwareNodeGroupListProcessor) Process(autoscalingContext *context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup, nodeInfos map[string]*framework.NodeInfo, unschedulablePods []*apiv1.Pod) ([]cloudprovider.NodeGroup, map[string]*framework.NodeInfo, error) {
	nodeGroups, nodeInfos, err := p.nodeGroupListProcessor.Process(autoscalingContext, nodeGroups, nodeInfos, unschedulablePods)
	if err != nil {
		return nodeGroups, nodeInfos, err
	}
	filteredNodeGroups := p.filterNodeGroupsByCccShardHomogeneity(nodeGroups, unschedulablePods)
	return filteredNodeGroups, nodeInfos, nil
}

func (p *shardAwareNodeGroupListProcessor) CleanUp() {
	p.nodeGroupListProcessor.CleanUp()
}

func (p *shardAwareNodeGroupListProcessor) filterNodeGroupsByCccShardHomogeneity(nodeGroups []cloudprovider.NodeGroup, unschedulablePods []*apiv1.Pod) []cloudprovider.NodeGroup {
	if len(unschedulablePods) == 0 {
		return nodeGroups
	}

	targetCCCName := p.getPodCccName(unschedulablePods[0])
	isHomogeneous := true

	for _, pod := range unschedulablePods[1:] {
		if p.getPodCccName(pod) != targetCCCName {
			isHomogeneous = false
			break
		}
	}

	if !isHomogeneous {
		return nodeGroups
	}

	klog.V(4).Infof("Filtering node groups to match homogeneous shard requirement, selected compute-class: %q", targetCCCName)
	var filteredNodeGroups []cloudprovider.NodeGroup
	for _, ng := range nodeGroups {
		ngCrd, ngCrdName, err := p.lister.NodeGroupCrd(ng)
		if err != nil {
			klog.Warningf("Cannot resolve CRD for node group %s due to error, not pruning: %v", ng.Id(), err)
			filteredNodeGroups = append(filteredNodeGroups, ng)
			continue
		}

		isNodeGroupCCC := ngCrd != nil && ngCrd.CrdType() == ccc.CrdType
		if !isNodeGroupCCC {
			ngCrdName = ""
		}

		if ngCrdName == targetCCCName {
			filteredNodeGroups = append(filteredNodeGroups, ng)
		}
	}
	return filteredNodeGroups
}

func (p *shardAwareNodeGroupListProcessor) getPodCccName(pod *apiv1.Pod) string {
	c, name, err := p.lister.PodCrd(pod)
	if err != nil || c == nil || c.CrdType() != ccc.CrdType {
		return ""
	}
	return name
}
