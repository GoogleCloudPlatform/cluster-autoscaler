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

package failednodes

import (
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/config"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/operationtracker"
	"k8s.io/klog/v2"
)

const (
	PluginName = "failed-nodes"

	backoff = 5 * time.Minute
)

type plugin struct {
	resizableVmManager    operationtracker.Manager
	maxCandidateNodeCount int
	latestUnfitNodesCount int
}

// NewPlugin returns new instance of failed nodes plugin. The plugin replaces failed nodes.
// Currently it only replaces failed EK nodes but can be extended to any node.
func NewPlugin(config config.PluginsConfig) defrag.Plugin {
	return &plugin{
		resizableVmManager:    config.ResizableVmManager,
		maxCandidateNodeCount: config.MaxCandidateNodeCount,
	}
}

func (p *plugin) String() string {
	return PluginName
}

func (p *plugin) NewCandidate(ctx *context.AutoscalingContext, nodeNames []string) *defrag.Candidate {
	failedNodes := p.getFailedNodes()

	candidates := []string{}
	for _, nodeName := range nodeNames {
		if failedNodes[nodeName] {
			candidates = append(candidates, nodeName)
		}
	}

	p.latestUnfitNodesCount = len(candidates)

	if len(candidates) == 0 {
		return nil
	}

	klog.V(4).Infof("Defrag %s: New candidates: %v", PluginName, candidates)
	return defrag.NewCandidateWithLimit(candidates, defrag.Partial, p.maxCandidateNodeCount)
}

func (p *plugin) ValidCandidateNodes(ctx *context.AutoscalingContext, nodeNames []string) []string {
	failedNodes := p.getFailedNodes()

	validNodes := []string{}
	for _, nodeName := range nodeNames {
		if !failedNodes[nodeName] {
			klog.V(3).Infof("Defrag %s: node %q is no longer failed node", PluginName, nodeName)
			continue
		}
		validNodes = append(validNodes, nodeName)
	}

	return validNodes
}

func (p *plugin) getFailedNodes() map[string]bool {
	if p.resizableVmManager == nil {
		return map[string]bool{}
	}

	failedNodes := map[string]bool{}
	for _, nodeName := range p.resizableVmManager.UnhealthyNodesWithStatus(operationtracker.FailedResizeStatus) {
		failedNodes[nodeName] = true
	}

	return failedNodes
}

func (p *plugin) IsExpansionOptionValid(ctx *context.AutoscalingContext, candidate *defrag.Candidate, option expander.Option) bool {
	// We don't care about any particular expansion option. Expander should be able to make the best choice for replacing this node.
	return true
}

func (p *plugin) BackoffDuration(*context.AutoscalingContext, *defrag.Candidate) time.Duration {
	return backoff
}

func (p *plugin) Type() defrag.PluginType {
	return defrag.StandardPluginType
}

func (p *plugin) LatestUnfitNodesCount() int {
	return p.latestUnfitNodesCount
}
