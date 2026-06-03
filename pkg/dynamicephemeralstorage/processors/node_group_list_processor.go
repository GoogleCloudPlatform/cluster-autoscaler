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
	"k8s.io/klog/v2"
)

// subset of NapResourceTrimmer needed to set a reference to nodeInfos
type estimatorReferenceSetter interface {
	SetNodeInfosMap(nodeInfos map[string]*framework.NodeInfo)
}

type nodeGroupListProcessor struct {
	nodeGroupListProcessor nodegroups.NodeGroupListProcessor
	nodeInfosSetter        estimatorReferenceSetter
}

// NewProcessor creates an instance of processor.
func NewNodeGroupListProcessor(NodeGroupListProcessor nodegroups.NodeGroupListProcessor, nodeInfosSetter estimatorReferenceSetter) *nodeGroupListProcessor {
	return &nodeGroupListProcessor{
		nodeGroupListProcessor: NodeGroupListProcessor,
		nodeInfosSetter:        nodeInfosSetter,
	}
}

// Process
func (p *nodeGroupListProcessor) Process(ctx *context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup, nodeInfos map[string]*framework.NodeInfo, unschedulablePods []*apiv1.Pod) ([]cloudprovider.NodeGroup, map[string]*framework.NodeInfo, error) {
	nodeGroups, nodeInfos, err := p.nodeGroupListProcessor.Process(ctx, nodeGroups, nodeInfos, unschedulablePods)
	if err != nil {
		klog.Errorf("Cannot process nodegroups from NAP, error: %v", err)
		return nodeGroups, nodeInfos, err
	}

	// pass pointer to nodeInfos, so it could be modified inside estimator:diskSizeAnalysisFunc
	p.nodeInfosSetter.SetNodeInfosMap(nodeInfos)

	return nodeGroups, nodeInfos, nil
}

// CleanUp cleans up the processor's internal structures. Just here to satisfy the NodeGroupListProcessor interface.
func (p *nodeGroupListProcessor) CleanUp() {
	p.nodeGroupListProcessor.CleanUp()
}
