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

package recycling

import (
	"cmp"
	"slices"
	"strconv"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/config"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

const (
	PluginName = "recycling"
)

type plugin struct {
	config                config.PluginsConfig
	clock                 clock.PassiveClock
	latestUnfitNodesCount int
}

func NewPlugin(config config.PluginsConfig) defrag.Plugin {
	return &plugin{
		config: config,
		clock:  clock.RealClock{},
	}
}

func (p *plugin) String() string {
	return PluginName
}

func (p *plugin) NewCandidate(ctx *context.AutoscalingContext, nodeNames []string) *defrag.Candidate {
	nodesToRecycle := []string{}
	nodeToTTL := map[string]int{}

	for _, nodeName := range nodeNames {
		nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil {
			klog.Errorf("Failed to get node info for node %v: %v", nodeName, err)
			continue
		}

		ttlSeconds, leadTimeSec, ok := p.getTTLAndRecycleLeadTime(nodeInfo.Node())

		if ok && ttlSeconds < leadTimeSec {
			klog.V(5).Infof("Defrag %s: recycling node %s: %ds TTL < %ds recycle lead time", PluginName, nodeName, ttlSeconds, leadTimeSec)
			nodesToRecycle = append(nodesToRecycle, nodeName)
			nodeToTTL[nodeName] = ttlSeconds
		}
	}

	p.latestUnfitNodesCount = len(nodesToRecycle)

	if len(nodesToRecycle) == 0 {
		return nil
	}

	// Sort by TTL so the nodes closest to terminaion will get recycled first
	slices.SortFunc(nodesToRecycle, func(node1, node2 string) int {
		return cmp.Compare(nodeToTTL[node1], nodeToTTL[node2])
	})

	return defrag.NewCandidateWithLimit(nodesToRecycle, defrag.Partial, p.config.MaxCandidateNodeCount)
}

func (p *plugin) ValidCandidateNodes(ctx *context.AutoscalingContext, nodeNames []string) []string {
	var candidateNodes []string
	for _, nodeName := range nodeNames {
		nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil {
			klog.Errorf("Failed to get node info for node %v: %v", nodeName, err)
			continue
		}
		ttlSeconds, leadTimeSec, ok := p.getTTLAndRecycleLeadTime(nodeInfo.Node())
		if ok && ttlSeconds < leadTimeSec {
			candidateNodes = append(candidateNodes, nodeName)
		}
	}
	return candidateNodes
}

func (p *plugin) getTTLAndRecycleLeadTime(node *apiv1.Node) (int, int, bool) {
	recycleLeadTimeSecStr, ok := node.Labels[gkelabels.NodeRecycleLeadTimeSecondsLabelKey]
	if !ok {
		klog.V(5).Infof("Defrag %s: node %s doesn't have recycling enabled, skipping", PluginName, node.Name)
		return 0, 0, false
	}
	terminationTimeStamp, ok := node.Annotations[gkelabels.InstanceTerminationAnnotationKey]
	if !ok {
		klog.V(5).Infof("Defrag %s: node %s doesn't have termination time stamp, skipping", PluginName, node.Name)
		return 0, 0, false
	}

	recycleLeadTimeSec, err := strconv.Atoi(recycleLeadTimeSecStr)
	if err != nil {
		klog.Errorf("Failed to parse lead time seconds %v of node %v: %v", recycleLeadTimeSecStr, node.Name, err)
		return 0, 0, false
	}

	terminationTime, err := time.Parse(time.RFC3339, terminationTimeStamp)
	if err != nil {
		klog.Errorf("Failed to parse termination timestamp %v of node %v: %v", terminationTimeStamp, node.Name, err)
		return 0, 0, false
	}

	ttlSeconds := int(terminationTime.Sub(p.clock.Now()).Seconds())
	return ttlSeconds, recycleLeadTimeSec, true
}

// Any new node should be better than a dying node
func (p *plugin) IsExpansionOptionValid(_ *context.AutoscalingContext, _ *defrag.Candidate, _ expander.Option) bool {
	return true
}

func (p *plugin) BackoffDuration(_ *context.AutoscalingContext, _ *defrag.Candidate) time.Duration {
	return 1 * time.Minute
}

func (p *plugin) Type() defrag.PluginType {
	return defrag.StandardPluginType
}

func (p *plugin) LatestUnfitNodesCount() int {
	return p.latestUnfitNodesCount
}
