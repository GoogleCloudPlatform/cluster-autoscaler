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

package processor

import (
	"errors"
	"fmt"
	"reflect"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/actuation"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/eligibility"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/pdb"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodes"
	"k8s.io/autoscaler/cluster-autoscaler/resourcequotas"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability/rules"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/options"
	"k8s.io/autoscaler/cluster-autoscaler/utils"
	"k8s.io/autoscaler/cluster-autoscaler/utils/annotations"
	"k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

// defragNodeFilterFactory is a factory for defragNodeFilter.
// It is created once and used for the entire lifetime of the defrag processor.
// It should not contain any state that is specific to a single Process() call.
type defragNodeFilterFactory struct {
	scaleDownNodeProcessor  nodes.ScaleDownNodeProcessor
	deleteOptions           options.NodeDeleteOptions
	drainabilityRules       rules.Rules
	clock                   clock.PassiveClock
	minQuotasTrackerFactory *resourcequotas.TrackerFactory
}

// newDefragNodeFilterFactory returns a new instance of defragNodeFilterFactory
func newDefragNodeFilterFactory(scaleDownNodeProcessor nodes.ScaleDownNodeProcessor, deleteOptions options.NodeDeleteOptions, drainabilityRules rules.Rules, minQuotasTrackerFactory *resourcequotas.TrackerFactory) *defragNodeFilterFactory {
	return &defragNodeFilterFactory{
		scaleDownNodeProcessor:  scaleDownNodeProcessor,
		deleteOptions:           deleteOptions,
		drainabilityRules:       drainabilityRules,
		clock:                   clock.RealClock{},
		minQuotasTrackerFactory: minQuotasTrackerFactory,
	}
}

// NewDefragNodeFilter creates a new defragNodeFilter with a refreshed cache.
func (f *defragNodeFilterFactory) NewDefragNodeFilter(ctx *context.AutoscalingContext) (*defragNodeFilter, error) {
	cache, err := f.buildScaleDownCandidatesCache(ctx)
	if err != nil {
		return nil, err
	}
	nodeInfos, err := ctx.ClusterSnapshot.ListNodeInfos()
	if err != nil {
		return nil, fmt.Errorf("failed to list node infos for tracker: %w", err)
	}
	var allNodes []*apiv1.Node
	for _, nodeInfo := range nodeInfos {
		allNodes = append(allNodes, nodeInfo.Node())
	}
	tracker, err := f.minQuotasTrackerFactory.NewMinQuotasTracker(ctx, allNodes)
	if err != nil {
		return nil, fmt.Errorf("failed to create min quotas tracker: %w", err)
	}
	return &defragNodeFilter{
		scaleDownNodeProcessor:   f.scaleDownNodeProcessor,
		deleteOptions:            f.deleteOptions,
		drainabilityRules:        f.drainabilityRules,
		clock:                    f.clock,
		scaleDownCandidatesCache: cache,
		nodeGroupSize:            utils.GetNodeGroupSizeMap(ctx.CloudProvider),
		minQuotasTracker:         tracker,
	}, nil
}

func (f *defragNodeFilterFactory) buildScaleDownCandidatesCache(ctx *context.AutoscalingContext) (sets.Set[string], error) {
	nodeInfos, err := ctx.ClusterSnapshot.ListNodeInfos()
	if err != nil {
		return nil, fmt.Errorf("failed to list node infos: %w", err)
	}
	scaleDownCandidates := make([]*apiv1.Node, len(nodeInfos))
	for i, nodeInfo := range nodeInfos {
		scaleDownCandidates[i] = nodeInfo.Node()
	}
	scaleDownCandidates, err = f.scaleDownNodeProcessor.GetScaleDownCandidates(ctx, scaleDownCandidates)
	if err != nil {
		return nil, fmt.Errorf("failed to get scale down candidates: %w", err)
	}
	cache := sets.New[string]()
	for _, node := range scaleDownCandidates {
		cache.Insert(node.Name)
	}
	return cache, nil
}

// defragNodeFilter holds a cache for a single Process() call.
type defragNodeFilter struct {
	scaleDownNodeProcessor nodes.ScaleDownNodeProcessor
	deleteOptions          options.NodeDeleteOptions
	drainabilityRules      rules.Rules
	clock                  clock.PassiveClock
	// nodeGroupSize is fetched and set in NewDefragNodeFilter function.
	// Then it is updated during filterNodesViolatingMinSize function
	// to help calculate the future nodePoolSize and maintian its min size.
	nodeGroupSize map[string]int

	scaleDownCandidatesCache sets.Set[string]
	minQuotasTracker         *resourcequotas.Tracker
}

// newValidCandidateNodes returns nodes that could be considered for defrag candidates.
// allNodes contains all nodes that are valid according to the defrag framework.
// nodesWithoutBlockingPods additionally removes nodes with blocking pods.
func (f *defragNodeFilter) newValidCandidateNodes(ctx *context.AutoscalingContext, pdbTracker pdb.RemainingPdbTracker, allCandidateNodes map[string]bool) ([]string, error) {
	nodeInfos, err := ctx.ClusterSnapshot.ListNodeInfos()
	if err != nil {
		return nil, err
	}

	var nodeNames []string
	for _, nodeInfo := range nodeInfos {
		if allCandidateNodes[nodeInfo.Node().Name] || !f.isCandidateNodeValid(ctx, nodeInfo) {
			continue
		}
		if f.hasBlockingPods(nodeInfo, ctx, pdbTracker) {
			klog.V(4).Infof("Defrag: node %s has blocking pods", nodeInfo.Node().Name)
			continue
		}
		nodeNames = append(nodeNames, nodeInfo.Node().Name)
	}
	return nodeNames, nil
}

// filterInvalidCandidateNodes removes candidate nodes that are no longer valid
func (f *defragNodeFilter) filterInvalidCandidateNodes(ctx *context.AutoscalingContext, pdbTracker pdb.RemainingPdbTracker, candidate *defrag.Candidate) {
	var nodeNames []string
	for _, nodeName := range candidate.Nodes {
		nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil {
			if !errors.Is(err, clustersnapshot.ErrNodeNotFound) {
				klog.Errorf("Defrag: failed to get NodeInfo for node %s: %v", nodeName, err)
			}
			continue
		}
		if !f.isCandidateNodeValid(ctx, nodeInfo) {
			continue
		}
		if f.hasBlockingPods(nodeInfo, ctx, pdbTracker) {
			klog.V(4).Infof("Defrag: node %s has blocking pods", nodeName)
			continue
		}
		nodeNames = append(nodeNames, nodeName)
	}
	candidate.Nodes = candidate.Plugin.ValidCandidateNodes(ctx, nodeNames)
}

// isCandidateNodeValid checks if a node is valid candidate node for defrag
func (f *defragNodeFilter) isCandidateNodeValid(ctx *context.AutoscalingContext, nodeInfo *framework.NodeInfo) bool {
	nodeName := nodeInfo.Node().Name

	if eligibility.HasNoScaleDownAnnotation(nodeInfo.Node()) {
		klog.V(4).Infof("Defrag: node %s has no-scale-down annotation", nodeName)
		return false
	}
	if actuation.IsNodeBeingDeleted(nodeInfo.Node(), f.clock.Now()) {
		klog.V(4).Infof("Defrag: node %s is being deleted", nodeName)
		return false
	}
	if !kubernetes.IsNodeReadyAndSchedulable(nodeInfo.Node()) {
		klog.V(4).Infof("Defrag: node %s is not ready and schedulable", nodeName)
		return false
	}
	if _, found := nodeInfo.Node().Annotations[annotations.NodeUpcomingAnnotation]; found {
		klog.V(4).Infof("Defrag: node %s has upcoming annotation", nodeName)
		return false
	}

	if !f.scaleDownCandidatesCache.Has(nodeName) {
		klog.V(4).Infof("Defrag: node %s is not a scale down candidate", nodeName)
		return false
	}

	klog.V(5).Infof("Defrag: node %s is a valid candidate", nodeName)
	return true
}

func (f *defragNodeFilter) hasBlockingPods(nodeInfo *framework.NodeInfo, ctx *context.AutoscalingContext, pdbTracker pdb.RemainingPdbTracker) bool {
	// nodeInfo is tainted here to distinguish the interaction of defrag from scale down
	// when considering for the BspDrainability rule which drains Blocking System Pods
	taint := apiv1.Taint{
		Key:    defrag.HardTaint,
		Value:  "defrag-check", // to ensure it is not the same as in real taint if one exists
		Effect: apiv1.TaintEffectNoSchedule,
	}
	addTaint(nodeInfo, taint)
	defer removeTaint(nodeInfo, taint)
	podMoveInfo, err := simulator.GetPodsToMove(nodeInfo, f.deleteOptions, f.drainabilityRules, ctx.ListerRegistry, pdbTracker, f.clock.Now())
	if err != nil {
		klog.V(4).Infof("Defrag: blocking pod error: %v", err)
		return true
	}
	if podMoveInfo.BlockingPod != nil {
		klog.V(4).Infof("Defrag: blocking pod: %s, reason: %v", podMoveInfo.BlockingPod.Pod.Name, podMoveInfo.BlockingPod.Reason)
		return true
	}
	if len(podMoveInfo.OnCompletionPods) > 0 {
		klog.V(4).Infof("Defrag: node %s has pods with safe-to-evict=on-completion annotation, not considered for defrag: %v", nodeInfo.Node().Name, podNames(podMoveInfo.OnCompletionPods))
		return true
	}
	return false
}

// filterNodesViolatingMinQuotas filters scale-down candidates that would violate
// resource quotas (including node group min size and ComputeClass target node count).
// It tracks the cumulative effect of removals using the shared minQuotasTracker.
func (f *defragNodeFilter) filterNodesViolatingMinQuotas(ctx *context.AutoscalingContext, nodes []string) ([]string, error) {
	var result []string
	for _, nodeName := range nodes {
		nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil {
			if !errors.Is(err, clustersnapshot.ErrNodeNotFound) {
				klog.Errorf("Defrag: failed to get NodeInfo for node %s: %v", nodeName, err)
			}
			continue
		}
		node := nodeInfo.Node()

		nodeGroup, err := ctx.CloudProvider.NodeGroupForNode(node)
		if err != nil {
			klog.Warningf("Error while checking node group for %s: %v", node.Name, err)
			continue
		}
		if nodeGroup == nil || reflect.ValueOf(nodeGroup).IsNil() {
			klog.V(5).Infof("Node %s should not be processed by cluster autoscaler (no node group config)", node.Name)
			continue
		}

		consumeResult, err := f.minQuotasTracker.ConsumeQuota(ctx, nodeGroup, node, 1)
		if err != nil {
			klog.Errorf("Defrag: failed to consume quota for node %s: %v", node.Name, err)
			continue
		}
		if consumeResult.Exceeded() {
			klog.V(1).Infof("Skipping %s - quota exceeded", node.Name)
			continue
		}

		result = append(result, node.Name)
	}
	return result, nil
}

// filterNodesViolatingMinSize filters scale-down candidates that would violate
// their Node Group's MinSize constraint. It tracks the cumulative effect of
// removals within the list to ensure safety for multiple nodes in the same group.
// filterNodesViolatingMinSize uses/updates the same nodeGroupSize within the same Defrag process.
func (f *defragNodeFilter) filterNodesViolatingMinSize(ctx *context.AutoscalingContext, nodes []string) []string {
	var result []string
	for _, nodeName := range nodes {
		nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil {
			if !errors.Is(err, clustersnapshot.ErrNodeNotFound) {
				klog.Errorf("Defrag: failed to get NodeInfo for node %s: %v", nodeName, err)
			}
			continue
		}
		node := nodeInfo.Node()

		nodeGroup, err := ctx.CloudProvider.NodeGroupForNode(node)
		if err != nil {
			klog.Warningf("Error while checking node group for %s: %v", node.Name, err)
			continue
		}
		if nodeGroup == nil || reflect.ValueOf(nodeGroup).IsNil() {
			klog.V(5).Infof("Node %s should not be processed by cluster autoscaler (no node group config)", node.Name)
			continue
		}

		nodeGroupId := nodeGroup.Id()
		size, found := f.nodeGroupSize[nodeGroupId]
		if !found {
			klog.Errorf("Error while checking node group size for %s: group size not found", nodeGroup.Id())
			continue
		}
		minSize := nodeGroup.MinSize()
		deletionsInProgress := ctx.ScaleDownActuator.CheckStatus().DeletionsCount(nodeGroupId)
		if size-deletionsInProgress <= minSize {
			klog.V(1).Infof("Skipping %s - node group min size reached (current: %d, deletionsInProgress: %d, min: %d), accounting for previous nodes", node.Name, size, deletionsInProgress, minSize)
			continue
		}
		result = append(result, node.Name)
		f.nodeGroupSize[nodeGroupId]--
	}
	return result
}

func addTaint(nodeInfo *framework.NodeInfo, taint apiv1.Taint) {
	nodeInfo.Node().Spec.Taints = append(nodeInfo.Node().Spec.Taints, taint)
}

func removeTaint(nodeInfo *framework.NodeInfo, taint apiv1.Taint) {
	var newTaints []apiv1.Taint
	for _, eachTaint := range nodeInfo.Node().Spec.Taints {
		if eachTaint.Key == taint.Key && eachTaint.Value == taint.Value {
			continue
		}
		newTaints = append(newTaints, eachTaint)
	}
	nodeInfo.Node().Spec.Taints = newTaints
}

func podNames(pods []*apiv1.Pod) []string {
	var names []string
	for _, pod := range pods {
		names = append(names, pod.Name)
	}
	return names
}
