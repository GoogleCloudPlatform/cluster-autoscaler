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

package highprioritymigration

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/config"
	"k8s.io/klog/v2"
)

const (
	PluginName = "high-priority-migration"
)

type nonOptimalNodeGroup struct {
	nodes              []string
	priorityGroupIndex int
}

type plugin struct {
	config  config.PluginsConfig
	matcher computeclass.Matcher
	// Adds randomGenerator here to not initialize it everytime we pick a candidate
	// Also main reason is it'll be simpler for mocking and testing
	randomGenerator       *rand.Rand
	latestUnfitNodesCount int
}

func NewPlugin(config config.PluginsConfig) defrag.Plugin {
	return &plugin{
		config:          config,
		matcher:         computeclass.NewMatcher(config.NPCLister, config.Provider),
		randomGenerator: rand.New(rand.NewSource(time.Now().Unix())),
	}
}

func (p *plugin) String() string {
	return PluginName
}

func (p *plugin) NewCandidate(ctx *context.AutoscalingContext, nodeNames []string) *defrag.Candidate {
	if !p.isListerValid() {
		// Not crucial, since it might be expected behaviour
		// But it might be an important log for debugging
		klog.V(2).Infof("Not creating candidate, npc crd lister is nil. NPCs / CCCs might be disabled")
		return nil
	}

	nonOptimalNodeGroups := make(map[string]*nonOptimalNodeGroup, 0)
	nonOptimalNodeGroupNames := make([]string, 0)
	nonOptimalNodesCount := 0

	for _, nodeName := range nodeNames {
		nodeGroup, npcCrd, err := p.getNodeGroupAndMatchingNpcCrd(ctx, nodeName)
		if err != nil {
			klog.Errorf("Ignoring node: %v: %v", nodeName, err)
			continue
		}
		if nodeGroup == nil || npcCrd == nil {
			// Something went wrong but is expected behaviour (e.g. npc crd got deleted)
			// Details should be logged by responsible methods
			continue
		}
		// priority index should remain constant (Best case scenario, see below) between different nodes in the same node group
		// This is because each node group uses a single npc crd and rule
		// In some cases however, we might have a race condition where an npc crd changes during candidate creation or just after that
		// This means that we can have different priority index but it we treat it as it doesn't happen
		// Since it's ok for our use case, otherwise we'd have to calculate the difference each time
		_, exists := nonOptimalNodeGroups[nodeGroup.Id()]
		if exists {
			nonOptimalNodeGroups[nodeGroup.Id()].nodes = append(nonOptimalNodeGroups[nodeGroup.Id()].nodes, nodeName)
			nonOptimalNodesCount++
			continue
		}

		priorityGroupFound, priorityGroupIndex, _ := p.matcher.FirstMatchedRuleGroup(nodeGroup, npcCrd)
		klog.V(5).Infof("Calculated priority group index: %v, node: %v, node group: %v, npc crd: %v:%v", priorityGroupIndex, nodeName, nodeGroup.Id(), npcCrd.Label(), npcCrd.Name())

		if len(npcCrd.GroupedRules()) > 0 && (!priorityGroupFound || priorityGroupIndex > 0) {
			nonOptimalNodeGroupNames = append(nonOptimalNodeGroupNames, nodeGroup.Id())
			nonOptimalNodeGroups[nodeGroup.Id()] = &nonOptimalNodeGroup{
				nodes:              []string{nodeName},
				priorityGroupIndex: priorityGroupIndex,
			}
			nonOptimalNodesCount++
		}
	}

	p.latestUnfitNodesCount = nonOptimalNodesCount

	leastOptimalNodeGroup := p.selectBestNodeGroupToScale(nonOptimalNodeGroups, nonOptimalNodeGroupNames)
	// No non-optimal node group found
	if leastOptimalNodeGroup == nil {
		return nil
	}

	candidateNodes := leastOptimalNodeGroup.nodes
	// Shouldn't really happen but just in case...
	if len(candidateNodes) == 0 {
		return nil
	}
	return defrag.NewCandidateWithLimit(candidateNodes, defrag.Partial, p.config.MaxCandidateNodeCount)
}

func (p *plugin) ValidCandidateNodes(ctx *context.AutoscalingContext, nodeNames []string) []string {
	if !p.isListerValid() {
		klog.V(2).Infof("Defrag %s: npc crd lister is nil. NPCs / CCCs might be disabled", p.String())
		return nil
	}

	for _, nodeName := range nodeNames {
		nodeGroup, npcCrd, err := p.getNodeGroupAndMatchingNpcCrd(ctx, nodeName)
		if err != nil {
			klog.Errorf("Candidate %v is invalid: %v", nodeNames, err)
			return nil
		}
		if nodeGroup == nil || npcCrd == nil {
			// Something went wrong but is expected behaviour (e.g. npc got deleted)
			// Details should be logged by responsible methods
			return nil
		}

		priorityGroupFound, priorityGroupIndex, _ := p.matcher.FirstMatchedRuleGroup(nodeGroup, npcCrd)
		klog.V(5).Infof("Calculated priority group index: %v, npc crd: %v:%v", priorityGroupIndex, npcCrd.Label(), npcCrd.Name())
		if (priorityGroupFound && priorityGroupIndex == 0) || len(npcCrd.GroupedRules()) == 0 {
			return nil
		}
	}
	return nodeNames
}

// IsExpansionOptionValid returns true in case of any errors encountered,
// since there's no way to validate the expansion.
func (p *plugin) IsExpansionOptionValid(ctx *context.AutoscalingContext, candidate *defrag.Candidate, option expander.Option) bool {
	if !p.isListerValid() {
		klog.V(2).Infof("Rejecting expansion option, npc crd lister is nil. NPCs / CCCs might be disabled")
		return false
	}

	// We shouldn't have a case where one node got migrated to
	// Lower priority and another to a higher one since we're working on
	// A single node group, but just in case we've updated this in the future
	anyNodeMigratedToLowerPriorityGroup := false
	anyNodeMigratedToHigherPriorityGroup := false
	for _, nodeName := range candidate.Nodes {
		nodeGroup, npcCrd, err := p.getNodeGroupAndMatchingNpcCrd(ctx, nodeName)
		if err != nil {
			if errors.Is(err, clustersnapshot.ErrNodeNotFound) {
				// node has likely been scaled down, no point in filtering the expansion
				// option out because of that
				continue
			}
			klog.Errorf("Rejecting expansion option: %v", err)
			return false
		}
		if nodeGroup == nil || npcCrd == nil {
			// Something went wrong but is expected behaviour (e.g. npc got deleted)
			// Details should be logged by responsible methods
			return false
		}
		currentPriorityGroupIndex := p.priorityGroupIndex(nodeGroup, npcCrd)
		newPriorityGroupIndex := p.priorityGroupIndex(option.NodeGroup, npcCrd)
		klog.V(5).Infof("Calculated priority group index: %v, for candidate: %v, while expansion option's priority group index is %v", currentPriorityGroupIndex, candidate, newPriorityGroupIndex)
		if newPriorityGroupIndex < currentPriorityGroupIndex {
			anyNodeMigratedToHigherPriorityGroup = true
		}
		if newPriorityGroupIndex > currentPriorityGroupIndex {
			anyNodeMigratedToLowerPriorityGroup = true
		}
	}
	return anyNodeMigratedToHigherPriorityGroup && !anyNodeMigratedToLowerPriorityGroup
}

func (p *plugin) priorityGroupIndex(group cloudprovider.NodeGroup, crd crd.CRD) int {
	if found, idx, _ := p.matcher.FirstMatchedRuleGroup(group, crd); found {
		return idx
	}
	return math.MaxInt
}

func (p *plugin) BackoffDuration(_ *context.AutoscalingContext, _ *defrag.Candidate) time.Duration {
	return 5 * time.Minute
}

func (p *plugin) Type() defrag.PluginType {
	return defrag.StandardPluginType
}

func (p *plugin) isListerValid() bool {
	return !reflect.ValueOf(p.config.NPCLister).IsNil()
}

// Utility function which removes some duplicated logic in HighPriorityMigration plugin methods
// - Gets NodeGroup for given node
// - Gets matching npc crd for node group by calling `getMatchingNpcCrd`
// - Checks `CrdActiveMigration.OptimizeRulePriority`
// - Returns NodeGroup, matching npc crd
// An error is returned instead if we:
// - Failed to get NodeGroup or npc crd
// A (nil, nil, nil) tuple is also sometimes returned for expected behaviours:
// - Found nil npc crd  but didn't encounter an error - e.g. in case of implicit default ccc crd
// - OptimizeRulePriority is set to false
func (p *plugin) getNodeGroupAndMatchingNpcCrd(ctx *context.AutoscalingContext, nodeName string) (cloudprovider.NodeGroup, crd.CRD, error) {
	nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get node info for node %v: %w", nodeName, err)
	}

	nodeGroup, err := ctx.CloudProvider.NodeGroupForNode(nodeInfo.Node())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get node group for node %v: %v", nodeName, err)
	}
	if nodeGroup == nil {
		return nil, nil, nil
	}

	npcCrd, npcCrdName, err := p.config.NPCLister.NodeGroupCrd(nodeGroup)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get npc crd for node group %v: %v", nodeGroup.Id(), err)
	}

	if npcCrd == nil || npcCrdName == "" {
		// No need to log here, details should be logged by responsible function
		return nil, nil, nil
	}

	klog.V(5).Infof("Found matching npc crd %v:%v for node group: %v", npcCrd.Label(), npcCrd.Name(), nodeGroup.Id())

	if !npcCrd.OptimizeRulePriority() {
		klog.V(5).Infof("optimizeRulePriority got disabled for npc crd %v:%v", npcCrd.Label(), npcCrd.Name())
		return nil, nil, nil
	}

	return nodeGroup, npcCrd, nil
}

// Selects the node group which would be best to scale in the current cycle
// This would be optimized and changed in the future
// For now it just selects a node group randomly to account for fairness
func (p *plugin) selectBestNodeGroupToScale(nodeGroups map[string]*nonOptimalNodeGroup, nodeGroupNames []string) *nonOptimalNodeGroup {
	if len(nodeGroupNames) != len(nodeGroups) {
		klog.Errorf("Cannot select a node group to scale, found non matching node groups and names")
		return nil
	}
	if len(nodeGroupNames) == 0 {
		return nil
	}
	randIdx := p.randomGenerator.Intn(len(nodeGroups))
	return nodeGroups[nodeGroupNames[randIdx]]
}

func (p *plugin) LatestUnfitNodesCount() int {
	return p.latestUnfitNodesCount
}
