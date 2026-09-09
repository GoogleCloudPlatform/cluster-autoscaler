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

package nodeconfigdrift

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/config"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/expander"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
)

const (
	// PluginName is the name of the nodeconfigdrift defrag plugin.
	PluginName = "nodeconfigdrift"
)

type plugin struct {
	config                config.PluginsConfig
	matcher               computeclass.Matcher
	latestUnfitNodesCount int
	listerValid           bool
}

// NewPlugin returns a new nodeconfigdrift defrag plugin instance.
func NewPlugin(config config.PluginsConfig) defrag.Plugin {
	return &plugin{
		config:      config,
		matcher:     computeclass.NewMatcher(config.NPCLister, config.Provider),
		listerValid: config.NPCLister != nil && !reflect.ValueOf(config.NPCLister).IsNil(),
	}
}

func (p *plugin) String() string {
	return PluginName
}

func (p *plugin) NewCandidate(ctx *ca_context.AutoscalingContext, nodeNames []string) *defrag.Candidate {
	if !p.isListerValid() {
		klog.V(2).Infof("Not creating candidate, npc crd lister is nil. NPCs / CCCs might be disabled")
		return nil
	}

	driftedNodeGroups := make(map[string][]string)
	var driftedNodeGroupNames []string
	isDriftedByNodeGroup := make(map[string]bool)
	driftedNodesCount := 0

	for _, nodeName := range nodeNames {
		nodeGroup, err := p.getNodeGroup(ctx, nodeName)
		if err != nil {
			klog.Errorf("Ignoring node %v: %v", nodeName, err)
			continue
		}
		if nodeGroup == nil {
			continue
		}

		nodeGroupId := nodeGroup.Id()
		isDrifted, evaluated := isDriftedByNodeGroup[nodeGroupId]
		if !evaluated {
			isDrifted = p.evaluateNodeGroupDrift(nodeGroup)
			isDriftedByNodeGroup[nodeGroupId] = isDrifted
			if isDrifted {
				driftedNodeGroupNames = append(driftedNodeGroupNames, nodeGroupId)
			}
		}

		if isDrifted {
			driftedNodeGroups[nodeGroupId] = append(driftedNodeGroups[nodeGroupId], nodeName)
			driftedNodesCount++
		}
	}

	p.latestUnfitNodesCount = driftedNodesCount

	if len(driftedNodeGroupNames) == 0 {
		return nil
	}

	randIdx := rand.Intn(len(driftedNodeGroupNames))
	selectedGroup := driftedNodeGroupNames[randIdx]
	candidateNodes := driftedNodeGroups[selectedGroup]

	return defrag.NewPartialCandidateWithLimit(candidateNodes, defrag.CreateBeforeDelete, p.config.MaxCandidateNodeCount)
}

func (p *plugin) ValidCandidateNodes(ctx *ca_context.AutoscalingContext, nodeNames []string) []string {
	if !p.isListerValid() {
		klog.V(2).Infof("Defrag %s: npc crd lister is nil. NPCs / CCCs might be disabled", p.String())
		return nil
	}

	var validNodes []string
	isDriftedByNodeGroup := make(map[string]bool)
	for _, nodeName := range nodeNames {
		nodeGroup, err := p.getNodeGroup(ctx, nodeName)
		if err != nil {
			klog.Errorf("Failed to check candidate node %v: %v", nodeName, err)
			continue
		}
		if nodeGroup == nil {
			continue
		}

		nodeGroupId := nodeGroup.Id()
		isDrifted, evaluated := isDriftedByNodeGroup[nodeGroupId]
		if !evaluated {
			isDrifted = p.evaluateNodeGroupDrift(nodeGroup)
			isDriftedByNodeGroup[nodeGroupId] = isDrifted
		}

		if isDrifted {
			validNodes = append(validNodes, nodeName)
		}
	}
	return validNodes
}

func (p *plugin) IsExpansionOptionValid(ctx *ca_context.AutoscalingContext, candidate *defrag.Candidate, option expander.Option) bool {
	if !p.isListerValid() {
		klog.V(2).Infof("Rejecting expansion option, npc crd lister is nil. NPCs / CCCs might be disabled")
		return false
	}

	if len(candidate.Nodes) == 0 {
		return false
	}

	var candidateCrd crd.CRD
	for _, nodeName := range candidate.Nodes {
		nodeGroup, err := p.getNodeGroup(ctx, nodeName)
		if err != nil {
			if errors.Is(err, clustersnapshot.ErrNodeNotFound) {
				continue
			}
			klog.Errorf("Rejecting expansion option: %v", err)
			return false
		}
		if nodeGroup == nil {
			continue
		}

		c, cName, err := p.config.NPCLister.NodeGroupCrd(nodeGroup)
		if err != nil {
			klog.Errorf("Rejecting expansion option: %v", err)
			return false
		}
		if c != nil && cName != "" && c.ConfigDrift() {
			candidateCrd = c
			break
		}
	}

	if candidateCrd == nil {
		return false
	}

	return p.isNodeGroupCompliant(option.NodeGroup, candidateCrd)
}

func (p *plugin) BackoffDuration(_ *ca_context.AutoscalingContext, _ *defrag.Candidate) time.Duration {
	return 5 * time.Minute
}

func (p *plugin) Type() defrag.PluginType {
	return defrag.StandardPluginType
}

func (p *plugin) LatestUnfitNodesCount() int {
	return p.latestUnfitNodesCount
}

func (p *plugin) isListerValid() bool {
	return p.listerValid
}

func (p *plugin) isNodeGroupDrifted(nodeGroup cloudprovider.NodeGroup, c crd.CRD) bool {
	if !p.matcher.MatchesCrdLabel(nodeGroup, c) {
		return false
	}
	return !p.isNodeGroupCompliant(nodeGroup, c)
}

func (p *plugin) isNodeGroupCompliant(nodeGroup cloudprovider.NodeGroup, c crd.CRD) bool {
	if len(c.Rules()) > 0 && !c.ScaleUpAnyway() {
		found, _, _ := p.matcher.FirstMatchedRule(nodeGroup, c)
		return found
	}
	return p.matcher.MatchesCrdConfig(nodeGroup, c)
}

func (p *plugin) evaluateNodeGroupDrift(nodeGroup cloudprovider.NodeGroup) bool {
	c, cName, err := p.config.NPCLister.NodeGroupCrd(nodeGroup)
	if err != nil {
		klog.Errorf("failed to get CRD for node group %v: %v", nodeGroup.Id(), err)
		return false
	}
	if c != nil && cName != "" && c.ConfigDrift() {
		return p.isNodeGroupDrifted(nodeGroup, c)
	}
	return false
}

func (p *plugin) getNodeGroup(ctx *ca_context.AutoscalingContext, nodeName string) (cloudprovider.NodeGroup, error) {
	nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
	if err != nil {
		return nil, fmt.Errorf("failed to get node info for node %v: %w", nodeName, err)
	}

	nodeGroup, err := ctx.CloudProvider.NodeGroupForNode(context.TODO(), nodeInfo.Node())
	if err != nil {
		return nil, fmt.Errorf("failed to get node group for node %v: %w", nodeName, err)
	}
	if nodeGroup == nil || reflect.ValueOf(nodeGroup).IsNil() {
		return nil, nil
	}

	return nodeGroup, nil
}
