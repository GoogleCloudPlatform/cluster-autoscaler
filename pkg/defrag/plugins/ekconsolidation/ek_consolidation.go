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

package ekconsolidation

import (
	"reflect"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/scheduling"
	podutils "k8s.io/autoscaler/cluster-autoscaler/utils/pod"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/config"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/operationtracker"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/processor"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

const (
	PluginName = "ek-consolidation"

	backoff = 5 * time.Minute

	cpuConsolidationRatio    = 0.25
	memoryConsolidationRatio = 0.25
)

type plugin struct {
	resizableVmManager    operationtracker.Manager
	experimentsManager    experiments.Manager
	simulator             *scheduling.HintingSimulator
	maxCandidateNodeCount int
	latestUnfitNodesCount int
}

func NewPlugin(config config.PluginsConfig) defrag.Plugin {
	return &plugin{
		resizableVmManager:    config.ResizableVmManager,
		experimentsManager:    config.ExperimentsManager,
		simulator:             scheduling.NewHintingSimulator(),
		maxCandidateNodeCount: config.MaxCandidateNodeCount,
	}
}

func (p *plugin) String() string {
	return PluginName
}

func (p *plugin) NewCandidate(ctx *context.AutoscalingContext, nodeNames []string) *defrag.Candidate {
	// Checks if resizing is enabled
	if !p.isResizingEnabled() {
		return nil
	}

	// Create cluster snapshot
	clusterSnapshot := ctx.ClusterSnapshot
	clusterSnapshot.Fork()
	defer clusterSnapshot.Revert()

	resizableNodesSnapshot := p.resizableVmManager.FilteredNodesSnapshot(false, operationtracker.ResizableOnly)
	potentialCandidates := []string{}
	for _, nodeName := range nodeNames {
		// Checks if node is EK
		ekNode, found := resizableNodesSnapshot[nodeName]
		if !found {
			continue
		}

		// Checks if node is small enough for consolidation
		if !p.isSmallNode(ekNode) {
			continue
		}

		if nodeInfo, err := clusterSnapshot.GetNodeInfo(nodeName); err == nil && processor.HasLookaheadPods(nodeInfo) {
			continue
		}

		potentialCandidates = append(potentialCandidates, nodeName)
	}

	if len(potentialCandidates) == 0 {
		return nil
	}

	// Collect maxsizes of all ek nodes
	maxSizes := map[string]size.Allocatable{}
	for nodeName, ekNode := range resizableNodesSnapshot {
		maxSizes[nodeName] = size.Max(ekNode.DesiredSize, ekNode.UpsizableMaxSize)
	}

	if err := processor.AdjustBalloonPodsSize(clusterSnapshot, maxSizes, nil); err != nil {
		klog.Errorf("Defrag %s: failed to adjust balloon pods size: %v", PluginName, err)
		return nil
	}

	klog.V(4).Infof("Defrag %s: Potential candidates: %v", PluginName, potentialCandidates)
	p.latestUnfitNodesCount = len(potentialCandidates)

	candidates := []string{}
	isCandidate := map[string]bool{}
	for _, nodeName := range potentialCandidates {
		if p.podsFromNodeReschedulable(clusterSnapshot, resizableNodesSnapshot, nodeName, isCandidate) {
			isCandidate[nodeName] = true
			candidates = append(candidates, nodeName)
		}
	}

	if len(candidates) == 0 {
		return nil
	}
	klog.V(4).Infof("Defrag %s: New candidates: %v", PluginName, candidates)
	return defrag.NewCandidateWithLimit(candidates, defrag.Partial, p.maxCandidateNodeCount)
}

func (p *plugin) ValidCandidateNodes(ctx *context.AutoscalingContext, nodeNames []string) []string {
	if !p.isResizingEnabled() {
		klog.V(4).Infof("Defrag %s: EK resizing is disabled", PluginName)
		return nil
	}
	// Collect maxsizes of all ek nodes
	resizableNodesSnapshot := p.resizableVmManager.FilteredNodesSnapshot(false, operationtracker.ResizableOnly)
	maxSizes := map[string]size.Allocatable{}
	for nodeName, ekNode := range resizableNodesSnapshot {
		maxSizes[nodeName] = size.Max(ekNode.DesiredSize, ekNode.UpsizableMaxSize)
	}

	// Create cluster snapshot
	clusterSnapshot := ctx.ClusterSnapshot
	clusterSnapshot.Fork()
	defer clusterSnapshot.Revert()

	if err := processor.AdjustBalloonPodsSize(clusterSnapshot, maxSizes, nil); err != nil {
		klog.Errorf("Defrag %s: failed to adjust balloon pods size: %v", PluginName, err)
		return nil
	}

	isCandidate := map[string]bool{}
	for _, nodeName := range nodeNames {
		isCandidate[nodeName] = true
	}

	var candidateNodes []string
	for _, nodeName := range nodeNames {
		if p.podsFromNodeReschedulable(clusterSnapshot, resizableNodesSnapshot, nodeName, isCandidate) {
			candidateNodes = append(candidateNodes, nodeName)
		} else {
			klog.V(4).Infof("Defrag %s: pods from node %s cannot be rescheduled", PluginName, nodeName)
		}
	}
	return candidateNodes
}

func (p *plugin) IsExpansionOptionValid(ctx *context.AutoscalingContext, candidate *defrag.Candidate, option expander.Option) bool {
	return false
}

func (p *plugin) BackoffDuration(*context.AutoscalingContext, *defrag.Candidate) time.Duration {
	return backoff
}

func (p *plugin) Type() defrag.PluginType {
	return defrag.ResizesOnlyPluginType
}

func (p *plugin) isResizingEnabled() bool {
	return p.resizableVmManager != nil && !reflect.ValueOf(p.resizableVmManager).IsNil() && p.resizableVmManager.IsResizingEnabled(machinetypes.EK.Name())
}

func (p *plugin) isSmallNode(ekNode operationtracker.ResizableNode) bool {
	cpuRatio := float64(ekNode.DesiredSize.MilliCpus) / float64(ekNode.PhysicalMaxSize.MilliCpus)
	memoryRatio := float64(ekNode.DesiredSize.KBytes) / float64(ekNode.PhysicalMaxSize.KBytes)
	return cpuRatio < cpuConsolidationRatio && memoryRatio < memoryConsolidationRatio
}

func (p *plugin) podsFromNodeReschedulable(clusterSnapshot clustersnapshot.ClusterSnapshot, resizableNodesSnapshot operationtracker.ResizableNodesSnapshot, nodeName string, isCandidate map[string]bool) bool {
	// Collecting workload pods from node
	nodeInfo, err := clusterSnapshot.GetNodeInfo(nodeName)
	if err != nil {
		klog.Errorf("Defrag %s: failed to get node info for node %v: %v", PluginName, nodeName, err)
		return false
	}

	if processor.HasLookaheadPods(nodeInfo) {
		klog.V(5).Infof("Defrag %s: node %q is not a valid candidate; it has lookahead pod", PluginName, nodeName)
		return false
	}

	pods := p.selectWorkloadPods(nodeInfo)

	// Prevent simulation on non EK nodes or current candidates
	isNodeAcceptable := func(schedulingCandidateNodeInfo *framework.NodeInfo) bool {
		schedulingCandidateName := schedulingCandidateNodeInfo.Node().Name
		_, found := resizableNodesSnapshot[schedulingCandidateName]
		if !found {
			return false
		}
		if isCandidate[schedulingCandidateName] {
			return false
		}
		if p.experimentsManager.DirectLaunchBoolFlag(experiments.EkPreventScheduleOnLookaheadNodes) && processor.HasLookaheadPods(schedulingCandidateNodeInfo) {
			return false
		}
		return schedulingCandidateName != nodeName
	}

	// Try to schedule pods from current node
	unschedulable, err := p.trySchedulePods(clusterSnapshot, pods, isNodeAcceptable)
	if err != nil {
		klog.Errorf("Defrag %s: failed to schedule pods: %v", PluginName, err)
		return false
	}
	if len(unschedulable) > 0 {
		klog.V(5).Infof("Defrag %s: Expected behaviour: cannot schedule %v pods from node: %v", PluginName, len(unschedulable), nodeName)
		return false
	}

	return true
}

func (p *plugin) selectWorkloadPods(nodeInfo *framework.NodeInfo) []*v1.Pod {
	pods := []*v1.Pod{}
	for _, podInfo := range nodeInfo.Pods() {
		pod := podInfo.Pod
		if !processor.IsUserWorkloadPod(pod) {
			continue
		}
		pods = append(pods, pod)
	}
	return podutils.ClearPodNodeNames(pods)
}

func (p *plugin) trySchedulePods(clusterSnapshot clustersnapshot.ClusterSnapshot, pods []*v1.Pod, isNodeAcceptable func(*framework.NodeInfo) bool) ([]*v1.Pod, error) {
	statuses, _, err := p.simulator.TrySchedulePods(clusterSnapshot, pods, false, clustersnapshot.SchedulingOptions{
		IsNodeAcceptable: isNodeAcceptable,
	})
	if err != nil {
		klog.Errorf("Defrag %s: failed to try schedule pods: %v", PluginName, err)
		return nil, err
	}

	podsWithStatus := make(map[types.UID]bool)
	for _, status := range statuses {
		podsWithStatus[status.Pod.UID] = true
	}

	unschedulable := []*v1.Pod{}
	for _, pod := range pods {
		if !podsWithStatus[pod.UID] {
			unschedulable = append(unschedulable, pod)
		}
	}
	return unschedulable, nil
}

func (p *plugin) LatestUnfitNodesCount() int {
	return p.latestUnfitNodesCount
}
