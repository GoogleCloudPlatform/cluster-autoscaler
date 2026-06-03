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

package nodepooldrain

import (
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/config"
	"k8s.io/klog/v2"
)

const (
	PluginName = "nodepool-drain"

	nodeDrainThreshold = 0
)

type plugin struct {
	latestUnfitNodesCount int
	config                config.PluginsConfig
}

func NewPlugin(config config.PluginsConfig) defrag.Plugin {
	return &plugin{
		config: config,
	}
}

func (*plugin) String() string {
	return PluginName
}

func (p *plugin) NewCandidate(ctx *context.AutoscalingContext, nodeNames []string) *defrag.Candidate {
	var suitableNodes []string
	for _, nodeName := range nodeNames {
		nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil {
			klog.Errorf("Failed to get NodeInfo: %v", err)
			continue
		}

		nodeGroup, err := ctx.CloudProvider.NodeGroupForNode(nodeInfo.Node())
		if err != nil {
			klog.Errorf("Failed to get NodeGroup: %v", err)
			continue
		}
		if nodeGroup == nil {
			continue
		}

		if nodeGroup.MaxSize() == nodeDrainThreshold {
			suitableNodes = append(suitableNodes, nodeName)
		}
	}

	p.latestUnfitNodesCount = len(suitableNodes)

	if len(suitableNodes) == 0 {
		return nil
	}
	return defrag.NewCandidateWithLimit(suitableNodes, defrag.Partial, p.config.MaxCandidateNodeCount)
}

func (p *plugin) ValidCandidateNodes(ctx *context.AutoscalingContext, nodeNames []string) []string {
	var candidateNodes []string
	for _, nodeName := range nodeNames {
		nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil {
			klog.Errorf("Failed to get NodeInfo: %v", err)
			continue
		}

		nodeGroup, err := ctx.CloudProvider.NodeGroupForNode(nodeInfo.Node())
		if err != nil {
			klog.Errorf("Failed to get NodeGroup: %v", err)
			continue
		}
		if nodeGroup == nil {
			continue
		}

		if nodeGroup.MaxSize() != nodeDrainThreshold {
			continue
		}
		candidateNodes = append(candidateNodes, nodeName)
	}
	return candidateNodes
}

func (*plugin) IsExpansionOptionValid(ctx *context.AutoscalingContext, candidate *defrag.Candidate, option expander.Option) bool {
	return true
}

func (*plugin) BackoffDuration(ctx *context.AutoscalingContext, candidate *defrag.Candidate) time.Duration {
	return 5 * time.Minute
}

func (p *plugin) Type() defrag.PluginType {
	return defrag.StandardPluginType
}

func (p *plugin) LatestUnfitNodesCount() int {
	return p.latestUnfitNodesCount
}
