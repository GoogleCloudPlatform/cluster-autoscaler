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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/drift"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/config"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/expander"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"
)

const (
	// PluginName is the name of the nodeconfigdrift defrag plugin.
	PluginName = "nodeconfigdrift"
)

type plugin struct {
	config                config.PluginsConfig
	evaluator             *drift.Evaluator
	latestUnfitNodesCount int
	// latestAtomicGroupBlocked lists the nodes that the latest NewCandidate
	// call held back because their atomic group was incomplete.
	latestAtomicGroupBlocked []string
	listerValid              bool
}

// NewPlugin returns a new nodeconfigdrift defrag plugin instance.
func NewPlugin(config config.PluginsConfig) defrag.Plugin {
	return &plugin{
		config:      config,
		evaluator:   drift.NewEvaluator(config.NPCLister, computeclass.NewMatcher(config.NPCLister, config.Provider)),
		listerValid: config.NPCLister != nil && !reflect.ValueOf(config.NPCLister).IsNil(),
	}
}

func (p *plugin) String() string {
	return PluginName
}

type candidateNodeGroupInfo struct {
	nodeGroupID string
	nodes       []string
	mode        defrag.Mode
	isAtomic    bool
	limit       int
}

func (p *plugin) NewCandidate(ctx *ca_context.AutoscalingContext, nodeNames []string) *defrag.Candidate {
	p.latestAtomicGroupBlocked = nil
	if !p.isListerValid() {
		klog.V(2).Infof("Not creating candidate, npc crd lister is nil. NPCs / CCCs might be disabled")
		return nil
	}

	candidateNodeGroups, groupKeys, driftedNodesCount := p.candidateNodeGroups(ctx, nodeNames)
	p.latestUnfitNodesCount = driftedNodesCount
	groupKeys = p.dropIncompleteAtomicGroups(ctx, candidateNodeGroups, groupKeys)

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
	driftCache := drift.NewCache(p.evaluator)
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

		if driftCache.EvaluateNodeGroup(nodeGroup).Drifted {
			groupKey, mode, isAtomic := p.getAtomicGroupKey(ctx, nodeName, nodeGroup)
			if groupKey == "" {
				continue
			}
			group, exists := candidateNodeGroups[groupKey]
			if !exists {
				candidateNodeGroupKeys = append(candidateNodeGroupKeys, groupKey)
				group = &candidateNodeGroupInfo{
					nodeGroupID: nodeGroup.Id(),
					mode:        mode,
					isAtomic:    isAtomic,
					limit:       p.candidateLimit(nodeGroup),
				}
				candidateNodeGroups[groupKey] = group
			}
			group.nodes = append(group.nodes, nodeName)
			driftedNodesCount++
		}
	}

	return candidateNodeGroups, candidateNodeGroupKeys, driftedNodesCount
}

// dropIncompleteAtomicGroups removes the atomic groups that are missing some of
// their members and returns the remaining group keys.
//
// The processor filters out nodes that can't be migrated right now, for
// example because of blocking pods or a backoff, before it calls NewCandidate.
// The nodes passed in may therefore be only part of an atomic group, and a
// candidate built from them would migrate the group piecemeal. Each atomic
// group is compared with its full membership in the cluster snapshot instead,
// and a group with missing members isn't offered as a candidate. Its available
// members are remembered, so they can be reported as blocked by their group.
func (p *plugin) dropIncompleteAtomicGroups(ctx *ca_context.AutoscalingContext, groups map[string]*candidateNodeGroupInfo, keys []string) []string {
	atomicNodeGroupIDs := make(map[string]bool)
	for _, key := range keys {
		if group := groups[key]; group.isAtomic {
			atomicNodeGroupIDs[group.nodeGroupID] = true
		}
	}
	if len(atomicNodeGroupIDs) == 0 {
		return keys
	}

	groupSizes, err := p.atomicGroupSizes(ctx, atomicNodeGroupIDs)
	if err != nil {
		// No atomic group can be shown to be complete. Skipping them all is
		// safer than risking a partial migration.
		klog.Errorf("Defrag %s: skipping atomic groups, failed to determine their members: %v", p.String(), err)
	}

	var kept []string
	for _, key := range keys {
		group := groups[key]
		if !group.isAtomic {
			kept = append(kept, key)
			continue
		}
		if err != nil {
			// The group is held back all the same, and its members should
			// say so rather than read as free to migrate.
			p.latestAtomicGroupBlocked = append(p.latestAtomicGroupBlocked, group.nodes...)
			continue
		}
		if len(group.nodes) < groupSizes[key] {
			klog.V(4).Infof("Defrag %s: atomic group %q has %d of %d nodes available, not creating a candidate", p.String(), key, len(group.nodes), groupSizes[key])
			p.latestAtomicGroupBlocked = append(p.latestAtomicGroupBlocked, group.nodes...)
			continue
		}
		kept = append(kept, key)
	}
	return kept
}

// atomicGroupSizes counts the nodes in the cluster snapshot that belong to each
// atomic group of the given node groups, keyed by atomic group key.
//
// Drift is evaluated per node group and the atomic group key includes the node
// group, so every node counted here is drifted if its group is.
//
// Only registered nodes count. An upcoming node is a placeholder for capacity
// still being provisioned: the processor never offers it, so counting it would
// hold the group's real members back under a reason nothing else explains. A
// member already gone from the snapshot is not waited for either; its deletion
// is under way, and the rest of the group should follow rather than wait for a
// node that is not coming back.
//
// A node whose node group cannot be determined is a different matter: it may be
// a member of any of the groups, and the same lookup failure keeps it out of the
// nodes offered to NewCandidate, so skipping it here would make its group look
// complete without it. The error is returned instead, and the caller holds every
// atomic group back for this loop.
func (p *plugin) atomicGroupSizes(ctx *ca_context.AutoscalingContext, nodeGroupIDs map[string]bool) (map[string]int, error) {
	nodeInfos, err := ctx.ClusterSnapshot.ListNodeInfos()
	if err != nil {
		return nil, err
	}
	sizes := make(map[string]int)
	for _, nodeInfo := range nodeInfos {
		node := nodeInfo.Node()
		if _, upcoming := node.Annotations[annotations.NodeUpcomingAnnotation]; upcoming {
			continue
		}
		nodeGroup, err := p.getNodeGroup(ctx, node.Name)
		if err != nil {
			return nil, err
		}
		if nodeGroup == nil || !nodeGroupIDs[nodeGroup.Id()] {
			continue
		}
		if key, _, isAtomic := p.getAtomicGroupKey(ctx, node.Name, nodeGroup); isAtomic {
			sizes[key]++
		}
	}
	return sizes, nil
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
	driftCache := drift.NewCache(p.evaluator)
	for _, nodeName := range nodeNames {
		nodeGroup, err := p.getNodeGroup(ctx, nodeName)
		if err != nil {
			klog.Errorf("Failed to check candidate node %v: %v", nodeName, err)
			continue
		}
		if nodeGroup == nil {
			continue
		}

		if driftCache.EvaluateNodeGroup(nodeGroup).Drifted {
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

	return p.evaluator.IsNodeGroupCompliant(option.NodeGroup, candidateCrd)
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

// LatestAtomicGroupBlockedNodes implements defrag.AtomicGroupReporter.
func (p *plugin) LatestAtomicGroupBlockedNodes() []string {
	return p.latestAtomicGroupBlocked
}

func (p *plugin) isListerValid() bool {
	return p.listerValid
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
