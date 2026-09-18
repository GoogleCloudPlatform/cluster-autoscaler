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
	"reflect"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"k8s.io/apimachinery/pkg/util/rand"
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

type candidateNodeGroupInfo struct {
	nodes    []string
	mode     defrag.Mode
	isAtomic bool
	limit    int
}

func (p *plugin) NewCandidate(ctx *ca_context.AutoscalingContext, nodeNames []string) *defrag.Candidate {
	if !p.isListerValid() {
		klog.V(2).Infof("Not creating candidate, npc crd lister is nil. NPCs / CCCs might be disabled")
		return nil
	}

	candidateNodeGroups, groupKeys, driftedNodesCount := p.candidateNodeGroups(ctx, nodeNames)
	p.latestUnfitNodesCount = driftedNodesCount

	if len(groupKeys) == 0 {
		return nil
	}

	randIdx := rand.Intn(len(groupKeys))
	selectedGroup := candidateNodeGroups[groupKeys[randIdx]]

	if selectedGroup.isAtomic {
		// For atomic groups, return the full group without applying the node limit
		return defrag.NewAtomicCandidate(selectedGroup.nodes, selectedGroup.mode)
	}

	return defrag.NewCandidateWithLimit(selectedGroup.nodes, selectedGroup.mode, selectedGroup.isAtomic, selectedGroup.limit)
}

func (p *plugin) candidateNodeGroups(ctx *ca_context.AutoscalingContext, nodeNames []string) (map[string]*candidateNodeGroupInfo, []string, int) {
	candidateNodeGroups := make(map[string]*candidateNodeGroupInfo)
	var candidateNodeGroupKeys []string
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
		}

		if isDrifted {
			groupKey, mode, isAtomic := p.getAtomicGroupKey(ctx, nodeName, nodeGroup)
			if groupKey == "" {
				continue
			}
			group, exists := candidateNodeGroups[groupKey]
			if !exists {
				candidateNodeGroupKeys = append(candidateNodeGroupKeys, groupKey)
				group = &candidateNodeGroupInfo{
					mode:     mode,
					isAtomic: isAtomic,
					limit:    p.candidateLimit(nodeGroup),
				}
				candidateNodeGroups[groupKey] = group
			}
			group.nodes = append(group.nodes, nodeName)
			driftedNodesCount++
		}
	}

	return candidateNodeGroups, candidateNodeGroupKeys, driftedNodesCount
}

func (p *plugin) candidateLimit(nodeGroup cloudprovider.NodeGroup) int {
	limit := p.config.MaxCandidateNodeCount
	if crd, _, err := p.config.NPCLister.NodeGroupCrd(nodeGroup); err == nil && crd != nil && crd.MaxNodeDisruption() != nil {
		maxDis := int(*crd.MaxNodeDisruption())
		if maxDis > 0 && maxDis < limit {
			limit = maxDis
		} else if maxDis <= 0 {
			limit = 0 // 0 means unlimited for NewCandidateWithLimit
		}
	}
	return limit
}

func (p *plugin) getAtomicGroupKey(ctx *ca_context.AutoscalingContext, nodeName string, nodeGroup cloudprovider.NodeGroup) (string, defrag.Mode, bool) {
	crd, _, err := p.config.NPCLister.NodeGroupCrd(nodeGroup)
	if err != nil || crd == nil {
		return nodeGroup.Id(), defrag.CreateBeforeDelete, false
	}

	labels := crd.AtomicGroupLabels()
	strategy := crd.MigrationStrategy()

	if len(labels) == 0 {
		if strategy == string(v1.MigrationStrategyDeleteBeforeCreate) {
			return nodeGroup.Id(), defrag.DeleteBeforeCreate, false
		}
		if strategy == string(v1.MigrationStrategyCreateBeforeDelete) {
			return nodeGroup.Id(), defrag.CreateBeforeDelete, false
		}
		return nodeGroup.Id(), defrag.CreateBeforeDelete, false
	}

	mode := defrag.CreateBeforeDelete
	if strategy == string(v1.MigrationStrategyDeleteBeforeCreate) {
		mode = defrag.DeleteBeforeCreate
	}

	nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
	if err != nil || nodeInfo == nil || nodeInfo.Node() == nil {
		return "", mode, false
	}

	key := nodeGroup.Id()
	node := nodeInfo.Node()
	for _, label := range labels {
		val := node.Labels[label]
		key += fmt.Sprintf(";;%s=%s", label, val)
	}
	return key, mode, true
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
