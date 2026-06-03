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
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"

	"k8s.io/klog/v2"
)

type nodePoolAwareNodeGroupSetProcessor struct {
	nodeGroupSetProcessor nodegroupset.NodeGroupSetProcessor
}

func NewNodePoolAwareNodeGroupSetProcessor(nodeGroupSetProcessor nodegroupset.NodeGroupSetProcessor) nodegroupset.NodeGroupSetProcessor {
	return &nodePoolAwareNodeGroupSetProcessor{
		nodeGroupSetProcessor: nodeGroupSetProcessor,
	}
}

// FindSimilarNodeGroups returns all other nodegroups from the nodepool of the given nodegroup.
func (p *nodePoolAwareNodeGroupSetProcessor) FindSimilarNodeGroups(context *context.AutoscalingContext, nodeGroup cloudprovider.NodeGroup, nodeInfosForGroups map[string]*framework.NodeInfo) ([]cloudprovider.NodeGroup, errors.AutoscalerError) {
	result, found := p.otherNodePoolMigs(nodeGroup)
	if found {
		return result, nil
	}
	return p.nodeGroupSetProcessor.FindSimilarNodeGroups(context, nodeGroup, nodeInfosForGroups)
}

// otherNodePoolMigs checks if the provided node group is a GkeMig and if so,
// returns the pre-computed list of similar node groups. The second return value
// indicates if the list was available.
// A nodegroup might be an existing or a virtual one (injected by NAP).
// NAP-injected nodegroups do not have any similar nodegroups because NAP
// injects a single virtual MIG per nodepool. In such case this function returns [], true.
func (p *nodePoolAwareNodeGroupSetProcessor) otherNodePoolMigs(nodeGroup cloudprovider.NodeGroup) ([]cloudprovider.NodeGroup, bool) {
	mig, ok := nodeGroup.(*gke.GkeMig)
	if !ok {
		klog.Warningf("Node group %v is not a GkeMig", nodeGroup.Id())
		return nil, false
	}
	np := mig.NodePool()
	if np == nil || len(np.Migs()) == 0 {
		return nil, false
	}
	// np.Migs() contains all node groups from the same node pool, including the node group itself.
	// This code filters out the node group itself.
	otherMigs := make([]cloudprovider.NodeGroup, 0, len(np.Migs())-1)
	for _, ng := range np.Migs() {
		if ng.Id() != nodeGroup.Id() {
			otherMigs = append(otherMigs, ng)
		}
	}
	return otherMigs, true
}

func (p *nodePoolAwareNodeGroupSetProcessor) BalanceScaleUpBetweenGroups(context *context.AutoscalingContext, groups []cloudprovider.NodeGroup, newNodes int) ([]nodegroupset.ScaleUpInfo, errors.AutoscalerError) {
	return p.nodeGroupSetProcessor.BalanceScaleUpBetweenGroups(context, groups, newNodes)
}

func (p *nodePoolAwareNodeGroupSetProcessor) CleanUp() {
	p.nodeGroupSetProcessor.CleanUp()
}
