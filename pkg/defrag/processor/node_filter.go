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
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/observability"
	scaledown_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaledown"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/actuation"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/eligibility"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/pdb"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodes"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/drainability/rules"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/options"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"
	ca_errors "sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
)

// stagedScaleDownNodeProcessor is implemented by scale-down node processors
// that can report which stage of the chain excluded each node.
//
// It is an optional capability: when the configured processor does not
// implement it, defrag still works, but exclusions are reported under the
// generic observability.MigrationBlocked reason.
type stagedScaleDownNodeProcessor interface {
	GetScaleDownCandidatesWithStages(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, nodes []*apiv1.Node) ([]*apiv1.Node, *scaledown_processors.ScaleDownCandidateStages, ca_errors.AutoscalerError)
}

// defragNodeFilterFactory is a factory for defragNodeFilter.
// It is created once and used for the entire lifetime of the defrag processor.
// It should not contain any state that is specific to a single Process() call.
type defragNodeFilterFactory struct {
	scaleDownNodeProcessor  nodes.ScaleDownNodeProcessor
	deleteOptions           options.NodeDeleteOptions
	drainabilityRules       rules.Rules
	clock                   clock.PassiveClock
	minQuotasTrackerFactory *resourcequotas.TrackerFactory
	ccLister                lister.Lister
}

// newDefragNodeFilterFactory returns a new instance of defragNodeFilterFactory
func newDefragNodeFilterFactory(scaleDownNodeProcessor nodes.ScaleDownNodeProcessor, deleteOptions options.NodeDeleteOptions, drainabilityRules rules.Rules, minQuotasTrackerFactory *resourcequotas.TrackerFactory, ccLister lister.Lister) *defragNodeFilterFactory {
	return &defragNodeFilterFactory{
		scaleDownNodeProcessor:  scaleDownNodeProcessor,
		deleteOptions:           deleteOptions,
		drainabilityRules:       drainabilityRules,
		clock:                   clock.RealClock{},
		minQuotasTrackerFactory: minQuotasTrackerFactory,
		ccLister:                ccLister,
	}
}

// NewDefragNodeFilter creates a new defragNodeFilter with a refreshed cache.
func (f *defragNodeFilterFactory) NewDefragNodeFilter(ctx *ca_context.AutoscalingContext) (*defragNodeFilter, error) {
	cache, stages, err := f.buildScaleDownCandidatesCache(ctx)
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
	tracker, err := f.minQuotasTrackerFactory.NewMinQuotasTracker(context.TODO(), ctx, allNodes)
	if err != nil {
		return nil, fmt.Errorf("failed to create min quotas tracker: %w", err)
	}
	disruptionTracker := NewMaxNodeDisruptionTracker(ctx, f.ccLister, allNodes)

	return &defragNodeFilter{
		scaleDownNodeProcessor:   f.scaleDownNodeProcessor,
		deleteOptions:            f.deleteOptions,
		drainabilityRules:        f.drainabilityRules,
		clock:                    f.clock,
		scaleDownCandidatesCache: cache,
		scaleDownCandidateStages: stages,
		nodeGroupSize:            utils.GetNodeGroupSizeMap(context.TODO(), ctx.CloudProvider),
		minQuotasTracker:         tracker,
		disruptionTracker:        disruptionTracker,
		reasons:                  observability.NewRegistry(),
	}, nil
}

// buildScaleDownCandidatesCache runs the scale-down candidate pipeline and
// returns the surviving nodes, along with the per-stage attribution of the
// excluded ones when the configured processor can provide it.
func (f *defragNodeFilterFactory) buildScaleDownCandidatesCache(ctx *ca_context.AutoscalingContext) (sets.Set[string], *scaledown_processors.ScaleDownCandidateStages, error) {
	nodeInfos, err := ctx.ClusterSnapshot.ListNodeInfos()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list node infos: %w", err)
	}
	scaleDownCandidates := make([]*apiv1.Node, len(nodeInfos))
	for i, nodeInfo := range nodeInfos {
		scaleDownCandidates[i] = nodeInfo.Node()
	}

	var stages *scaledown_processors.ScaleDownCandidateStages
	if staged, ok := f.scaleDownNodeProcessor.(stagedScaleDownNodeProcessor); ok {
		scaleDownCandidates, stages, err = staged.GetScaleDownCandidatesWithStages(context.TODO(), ctx, scaleDownCandidates)
	} else {
		scaleDownCandidates, err = f.scaleDownNodeProcessor.GetScaleDownCandidates(context.TODO(), ctx, scaleDownCandidates)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get scale down candidates: %w", err)
	}

	cache := sets.New[string]()
	for _, node := range scaleDownCandidates {
		cache.Insert(node.Name)
	}
	return cache, stages, nil
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
	// scaleDownCandidateStages explains which stage of the scale-down pipeline
	// excluded each node missing from scaleDownCandidatesCache. It is nil when
	// the configured processor cannot report it.
	scaleDownCandidateStages *scaledown_processors.ScaleDownCandidateStages
	minQuotasTracker         *resourcequotas.Tracker
	disruptionTracker        *MaxNodeDisruptionTracker

	// reasons collects why each rejected node could not be migrated during this
	// pass. It is nil-safe, so filters constructed directly in tests need not
	// set it.
	reasons *observability.Registry
}

// BlockReasons returns the reasons recorded during this pass.
func (f *defragNodeFilter) BlockReasons() *observability.Registry {
	if f == nil {
		return nil
	}
	return f.reasons
}

// newValidCandidateNodes returns nodes that could be considered for defrag candidates.
// allNodes contains all nodes that are valid according to the defrag framework.
// nodesWithoutBlockingPods additionally removes nodes with blocking pods.
func (f *defragNodeFilter) newValidCandidateNodes(ctx *ca_context.AutoscalingContext, pdbTracker pdb.RemainingPdbTracker, allCandidateNodes map[string]bool) ([]string, error) {
	nodeInfos, err := ctx.ClusterSnapshot.ListNodeInfos()
	if err != nil {
		return nil, err
	}

	var nodeNames []string
	for _, nodeInfo := range nodeInfos {
		nodeName := nodeInfo.Node().Name
		if allCandidateNodes[nodeName] {
			continue
		}
		if valid, reason := f.isCandidateNodeValid(ctx, nodeInfo); !valid {
			f.reasons.Record(nodeName, reason)
			continue
		}
		if blocked, reason := f.hasBlockingPods(nodeInfo, ctx, pdbTracker); blocked {
			klog.V(4).Infof("Defrag: node %s has blocking pods", nodeName)
			f.reasons.Record(nodeName, reason)
			continue
		}
		nodeNames = append(nodeNames, nodeName)
	}
	return nodeNames, nil
}

// isNodeOngoingDeletion returns true if the node is in the process of deletion.
//
// The returned reason is empty when the node is genuinely being removed by the
// autoscaler, because such a node is making progress rather than being blocked.
// It is only set for nodes that merely look undeletable, that is cordoned
// nodes. Whether the cordon is somebody's doing or part of an operation on the
// node pool is for the caller to work out from the scale-down pipeline.
func (f *defragNodeFilter) isNodeOngoingDeletion(ctx *ca_context.AutoscalingContext, node *apiv1.Node) (bool, observability.BlockReason) {
	if node.DeletionTimestamp != nil {
		return true, ""
	}
	if actuation.IsNodeBeingDeleted(node, f.clock.Now()) || taints.HasToBeDeletedTaint(node) {
		return true, ""
	}
	if node.Spec.Unschedulable {
		return true, observability.Cordoned
	}
	if ctx.ScaleDownActuator != nil && !reflect.ValueOf(ctx.ScaleDownActuator).IsNil() {
		status := ctx.ScaleDownActuator.CheckStatus()
		if status != nil && !reflect.ValueOf(status).IsNil() {
			empty, drained := status.DeletionsInProgress()
			if slices.Contains(empty, node.Name) || slices.Contains(drained, node.Name) {
				return true, ""
			}
		}
	}
	return false, ""
}

// filterDeletedCandidateNodes removes candidate nodes that are no longer present in the cluster or are being deleted
func (f *defragNodeFilter) filterDeletedCandidateNodes(ctx *ca_context.AutoscalingContext, candidate *defrag.Candidate) {
	var nodeNames []string
	for _, nodeName := range candidate.Nodes {
		nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil {
			if !errors.Is(err, clustersnapshot.ErrNodeNotFound) {
				klog.Errorf("Defrag: failed to get NodeInfo for node %s: %v", nodeName, err)
			}
			continue
		}
		if ongoing, reason := f.isNodeOngoingDeletion(ctx, nodeInfo.Node()); ongoing {
			if reason != "" && !f.scaleDownCandidatesCache.Has(nodeName) {
				// The node only looks undeletable. Whatever excluded it from
				// scale-down may explain that better than its own state does,
				// as with a node cordoned by an upgrade of its node pool.
				reason = observability.MoreSignificant(reason, reasonForExcludedScaleDownCandidate(f.scaleDownCandidateStages, nodeName))
			}
			f.reasons.Record(nodeName, reason)
			continue
		}
		nodeNames = append(nodeNames, nodeName)
	}
	candidate.Nodes = nodeNames
}

// filterInvalidCandidateNodes removes candidate nodes that are no longer valid
func (f *defragNodeFilter) filterInvalidCandidateNodes(ctx *ca_context.AutoscalingContext, pdbTracker pdb.RemainingPdbTracker, candidate *defrag.Candidate) {
	var nodeNames []string
	for _, nodeName := range candidate.Nodes {
		nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil {
			if !errors.Is(err, clustersnapshot.ErrNodeNotFound) {
				klog.Errorf("Defrag: failed to get NodeInfo for node %s: %v", nodeName, err)
			}
			continue
		}
		if valid, reason := f.isCandidateNodeValid(ctx, nodeInfo); !valid {
			f.reasons.Record(nodeName, reason)
			continue
		}
		if blocked, reason := f.hasBlockingPods(nodeInfo, ctx, pdbTracker); blocked {
			klog.V(4).Infof("Defrag: node %s has blocking pods", nodeName)
			f.reasons.Record(nodeName, reason)
			continue
		}
		nodeNames = append(nodeNames, nodeName)
	}
	candidate.Nodes = candidate.Plugin.ValidCandidateNodes(ctx, nodeNames)
}

// isCandidateNodeValid checks if a node is valid candidate node for defrag.
//
// When the node is not a valid candidate it also returns the reason, which is
// empty if the node is already progressing through migration rather than being
// blocked.
//
// A node is often invalid for more than one reason, so instead of stopping at
// the first one found, every check runs and the most significant reason wins.
// In particular what the scale-down pipeline knows about the node is folded in
// even when the node's own state already rules it out: a node cordoned by an
// upgrade of its node pool is reported under the upgrade, not as a cordon.
func (f *defragNodeFilter) isCandidateNodeValid(ctx *ca_context.AutoscalingContext, nodeInfo *framework.NodeInfo) (bool, observability.BlockReason) {
	node := nodeInfo.Node()
	nodeName := node.Name

	valid := true
	var reason observability.BlockReason
	invalidate := func(r observability.BlockReason) {
		valid = false
		reason = observability.MoreSignificant(reason, r)
	}

	if eligibility.HasNoScaleDownAnnotation(node) {
		klog.V(4).Infof("Defrag: node %s has no-scale-down annotation", nodeName)
		invalidate(observability.NodeConsolidationDisabled)
	}
	if ongoing, r := f.isNodeOngoingDeletion(ctx, node); ongoing {
		if r == "" {
			// The node is already on its way out, so whatever else is true of
			// it, it is progressing rather than blocked.
			klog.V(4).Infof("Defrag: node %s is being deleted", nodeName)
			return false, ""
		}
		klog.V(4).Infof("Defrag: node %s is cordoned", nodeName)
		invalidate(r)
	}
	if readiness, err := kubernetes.GetNodeReadiness(node); err != nil || !readiness.Ready {
		klog.V(4).Infof("Defrag: node %s is not ready", nodeName)
		invalidate(observability.NodeNotReady)
	}
	if _, found := node.Annotations[annotations.NodeUpcomingAnnotation]; found {
		klog.V(4).Infof("Defrag: node %s has upcoming annotation", nodeName)
		// An upcoming node is registered but not yet usable, so it is reported
		// as not ready rather than as a distinct reason.
		invalidate(observability.NodeNotReady)
	}
	if !f.scaleDownCandidatesCache.Has(nodeName) {
		klog.V(4).Infof("Defrag: node %s is not a scale down candidate", nodeName)
		invalidate(reasonForExcludedScaleDownCandidate(f.scaleDownCandidateStages, nodeName))
	}

	if valid {
		klog.V(5).Infof("Defrag: node %s is a valid candidate", nodeName)
	}
	return valid, reason
}

// hasBlockingPods reports whether the node hosts pods that prevent it from
// being drained, along with the reason to report it under.
func (f *defragNodeFilter) hasBlockingPods(nodeInfo *framework.NodeInfo, ctx *ca_context.AutoscalingContext, pdbTracker pdb.RemainingPdbTracker) (bool, observability.BlockReason) {
	// nodeInfo is tainted here to distinguish the interaction of defrag from scale down
	// when considering for the BspDrainability rule which drains Blocking System Pods
	taint := apiv1.Taint{
		Key:    defrag.HardTaint,
		Value:  "defrag-check", // to ensure it is not the same as in real taint if one exists
		Effect: apiv1.TaintEffectNoSchedule,
	}
	addTaint(nodeInfo, taint)
	defer removeTaint(nodeInfo, taint)
	podMoveInfo, err := simulator.GetPodsToMove(context.TODO(), nodeInfo, f.deleteOptions, f.drainabilityRules, ctx.ListerRegistry, pdbTracker, f.clock.Now())
	// A blocked drain reports the offending pod and, for some rules, an error
	// describing the same thing. The pod carries the specific reason, so it is
	// inspected first; an error on its own means the simulation failed and the
	// node cannot be attributed to any particular blocker.
	if podMoveInfo.BlockingPod != nil {
		klog.V(4).Infof("Defrag: blocking pod: %s, reason: %v", podMoveInfo.BlockingPod.Pod.Name, podMoveInfo.BlockingPod.Reason)
		return true, reasonForBlockingPod(podMoveInfo.BlockingPod.Reason)
	}
	if err != nil {
		klog.V(4).Infof("Defrag: blocking pod error: %v", err)
		return true, observability.MigrationBlocked
	}
	if len(podMoveInfo.OnCompletionPods) > 0 {
		klog.V(4).Infof("Defrag: node %s has pods with safe-to-evict=on-completion annotation, not considered for defrag: %v", nodeInfo.Node().Name, podNames(podMoveInfo.OnCompletionPods))
		return true, observability.BlockingPods
	}
	return false, ""
}

// filterNodesViolatingMinQuotas filters scale-down candidates that would violate
// resource quotas (including node group min size and ComputeClass target node count).
// It tracks the cumulative effect of removals using the shared minQuotasTracker.
func (f *defragNodeFilter) filterNodesViolatingMinQuotas(ctx *ca_context.AutoscalingContext, nodes []string) ([]string, error) {
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

		nodeGroup, err := ctx.CloudProvider.NodeGroupForNode(context.TODO(), node)
		if err != nil {
			klog.Warningf("Error while checking node group for %s: %v", node.Name, err)
			continue
		}
		if nodeGroup == nil || reflect.ValueOf(nodeGroup).IsNil() {
			klog.V(5).Infof("Node %s should not be processed by cluster autoscaler (no node group config)", node.Name)
			continue
		}

		consumeResult, err := f.minQuotasTracker.ConsumeQuota(context.TODO(), ctx, nodeGroup, node, 1)
		if err != nil {
			klog.Errorf("Defrag: failed to consume quota for node %s: %v", node.Name, err)
			f.reasons.Record(node.Name, observability.MigrationBlocked)
			continue
		}
		if consumeResult.Exceeded() {
			klog.V(1).Infof("Skipping %s - quota exceeded", node.Name)
			f.reasons.Record(node.Name, observability.MinCapacityReached)
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
func (f *defragNodeFilter) filterNodesViolatingMinSize(ctx *ca_context.AutoscalingContext, nodes []string) []string {
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

		nodeGroup, err := ctx.CloudProvider.NodeGroupForNode(context.TODO(), node)
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
		minSize := nodeGroup.MinSize(context.TODO())
		deletionsInProgress := ctx.ScaleDownActuator.CheckStatus().DeletionsCount(nodeGroupId)
		if size-deletionsInProgress <= minSize {
			klog.V(1).Infof("Skipping %s - node group min size reached (current: %d, deletionsInProgress: %d, min: %d), accounting for previous nodes", node.Name, size, deletionsInProgress, minSize)
			f.reasons.Record(node.Name, observability.MinCapacityReached)
			continue
		}
		result = append(result, node.Name)
		f.nodeGroupSize[nodeGroupId]--
	}
	return result
}

// filterNodesViolatingMaxDisruption filters scale-down candidates that would violate
// their ComputeClass's MaxNodeDisruption limit.
func (f *defragNodeFilter) filterNodesViolatingMaxDisruption(ctx *ca_context.AutoscalingContext, nodes []string) []string {
	if f.disruptionTracker != nil {
		return f.disruptionTracker.FilterNodesViolatingMaxDisruption(ctx, nodes, f.reasons)
	}
	return nodes
}

// reserveMaxDisruptionBudget reserves disruption budget for an existing candidate
// that has not yet scaled down, without modifying candidate.Nodes.
func (f *defragNodeFilter) reserveMaxDisruptionBudget(ctx *ca_context.AutoscalingContext, candidate *defrag.Candidate, isNodeScaleDownStarted func(string) bool) {
	if f.disruptionTracker != nil {
		f.disruptionTracker.ReserveMaxDisruptionBudget(ctx, candidate, isNodeScaleDownStarted)
	}
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
