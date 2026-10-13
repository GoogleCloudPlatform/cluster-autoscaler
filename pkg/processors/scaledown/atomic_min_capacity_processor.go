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

package scaledown

import (
	"context"
	"maps"
	"slices"
	"strconv"
	"strings"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodes"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"

	gke_labels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	cc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

// AtomicMinCapacityProcessor is a scale-down processor (implements
// nodes.ScaleDownSetProcessor and the defrag processor's ScaleDownFilter). It
// enforces ComputeClass targetNodeCount for atomically-scaled
// (ZeroOrMaxNodeScaling) node groups at whole-group granularity, accepting a
// group's removal only if doing so keeps every affected ComputeClass at or
// above its targetNodeCount.
//
// It must run after AtomicResizeFilteringProcessor and only accepts or
// rejects whole groups. Atomic groups are exempt from the per-node
// TargetNodeCountQuota (see TargetNodeCountQuota.AppliesTo); this processor
// restores whole-group enforcement. Defrag consults it too before deleting a
// create-before-delete candidate (see pkg/defrag/processor).
//
// TODO(b/573091676): refactor and rename this processor. It decides on more
// than atomic groups: for defrag it also decides on non-atomic nodes that are
// being deleted.
type AtomicMinCapacityProcessor struct {
	ccLister           cc_lister.Lister
	experimentsManager experiments.Manager
}

// NewAtomicMinCapacityProcessor creates a new AtomicMinCapacityProcessor.
func NewAtomicMinCapacityProcessor(ccLister cc_lister.Lister, experimentsManager experiments.Manager) *AtomicMinCapacityProcessor {
	return &AtomicMinCapacityProcessor{
		ccLister:           ccLister,
		experimentsManager: experimentsManager,
	}
}

// minCapacityConstraint models a single targetNodeCount limit (either the
// top-level CC limit or a priority-rule-level limit) and tracks how many nodes
// currently satisfy it as candidate groups are accepted for removal.
type minCapacityConstraint struct {
	crdName    string
	ruleIdxStr string // "" means the top-level CC limit (all nodes of the CC).
	target     int
	current    int
}

func (c *minCapacityConstraint) matches(node *apiv1.Node) bool {
	if node.Labels[gke_labels.ComputeClassLabel] != c.crdName {
		return false
	}
	if c.ruleIdxStr != "" {
		return node.Annotations[gke_labels.CCCPriorityIndexAnnotationKey] == c.ruleIdxStr
	}
	return true
}

// FilterUnremovableNodes blocks scale-down of atomic node groups that would take
// an affected ComputeClass below its targetNodeCount. Non-atomic candidates are
// passed through unchanged (they are enforced by the per-node quota), except
// nodes already being deleted, e.g. defrag candidates, which are checked here as
// single-node groups: defrag's quota tracker ignores the top-level
// targetNodeCount, so defrag relies on this processor for them.
func (p *AtomicMinCapacityProcessor) FilterUnremovableNodes(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, scaleDownCtx *nodes.ScaleDownContext, candidates []simulator.NodeToBeRemoved) ([]simulator.NodeToBeRemoved, []simulator.UnremovableNode) {
	nodesToBeRemoved, groups := p.groupCandidates(ctx, autoscalingCtx, candidates)
	if len(groups) == 0 {
		return candidates, nil
	}

	constraints, err := p.buildConstraints(autoscalingCtx, scaleDownCtx)
	if err != nil {
		// Fail closed for ComputeClass groups: this processor is their only
		// floor protection. Other groups are not affected by the floor.
		klog.Errorf("AtomicMinCapacityProcessor: failed to build min-capacity constraints, not scaling down ComputeClass atomic node groups and nodes being deleted: %v", err)
		unremovableNodes := []simulator.UnremovableNode{}
		for _, group := range groups {
			if group[0].Node.Labels[gke_labels.ComputeClassLabel] == "" {
				nodesToBeRemoved = append(nodesToBeRemoved, group...)
				continue
			}
			for _, c := range group {
				unremovableNodes = append(unremovableNodes, simulator.UnremovableNode{Node: c.Node, Reason: simulator.UnexpectedError})
			}
		}
		return nodesToBeRemoved, unremovableNodes
	}
	if len(constraints) == 0 {
		return candidates, nil
	}

	unremovableNodes := []simulator.UnremovableNode{}
	for _, group := range groups {
		if p.canRemoveGroup(constraints, group) {
			p.applyRemoval(constraints, group)
			nodesToBeRemoved = append(nodesToBeRemoved, group...)
			klog.Infof("AtomicMinCapacityProcessor: scaling down nodes %v; ComputeClass minimum capacity is satisfied", nodeNames(group))
		} else {
			klog.Infof("AtomicMinCapacityProcessor: not scaling down nodes %v; would drop ComputeClass below targetNodeCount", nodeNames(group))
			for _, c := range group {
				unremovableNodes = append(unremovableNodes, simulator.UnremovableNode{Node: c.Node, Reason: simulator.NodeGroupMinSizeReached})
			}
		}
	}
	return nodesToBeRemoved, unremovableNodes
}

// TrimScaleDownCandidates returns the given nodes, in the given order, that can
// be deleted without taking any ComputeClass below its min capacity.
func (p *AtomicMinCapacityProcessor) TrimScaleDownCandidates(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, candidateNodes []*apiv1.Node) []*apiv1.Node {
	candidates := make([]simulator.NodeToBeRemoved, 0, len(candidateNodes))
	for _, node := range candidateNodes {
		candidates = append(candidates, simulator.NodeToBeRemoved{Node: node})
	}
	removable, _ := p.FilterUnremovableNodes(ctx, autoscalingCtx, nil, candidates)
	removableNames := make(map[string]bool, len(removable))
	for _, c := range removable {
		removableNames[c.Node.Name] = true
	}
	return slices.DeleteFunc(slices.Clone(candidateNodes), func(node *apiv1.Node) bool { return !removableNames[node.Name] })
}

// groupCandidates returns the candidates to be removed without further checks,
// and the groups of candidates this processor decides on, each removed or kept
// as a whole. The groups are, in this order: one per atomic node group, sorted
// by node group ID, then one per non-atomic node already being deleted (e.g. a
// defrag candidate), sorted by node name.
func (p *AtomicMinCapacityProcessor) groupCandidates(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, candidates []simulator.NodeToBeRemoved) (toBeRemoved []simulator.NodeToBeRemoved, groups [][]simulator.NodeToBeRemoved) {
	toBeRemoved = []simulator.NodeToBeRemoved{}
	atomicGroups := map[string][]simulator.NodeToBeRemoved{}
	var deletedNonAtomic []simulator.NodeToBeRemoved
	for _, c := range candidates {
		nodeGroup, err := autoscalingCtx.CloudProvider.NodeGroupForNode(ctx, c.Node)
		if err != nil || nodeGroup == nil {
			toBeRemoved = append(toBeRemoved, c)
			continue
		}
		opts, err := nodeGroup.GetOptions(ctx, autoscalingCtx.NodeGroupDefaults)
		if err != nil && err != cloudprovider.ErrNotImplemented {
			klog.Errorf("AtomicMinCapacityProcessor: failed to get options for node group %s, treating node %s as non-atomic: %v", nodeGroup.Id(), c.Node.Name, err)
			opts = nil
		}
		switch {
		case opts != nil && opts.ZeroOrMaxNodeScaling:
			atomicGroups[nodeGroup.Id()] = append(atomicGroups[nodeGroup.Id()], c)
		case beingDeleted(c.Node):
			deletedNonAtomic = append(deletedNonAtomic, c)
		default:
			toBeRemoved = append(toBeRemoved, c)
		}
	}
	for _, id := range slices.Sorted(maps.Keys(atomicGroups)) {
		groups = append(groups, atomicGroups[id])
	}
	slices.SortFunc(deletedNonAtomic, func(a, b simulator.NodeToBeRemoved) int { return strings.Compare(a.Node.Name, b.Node.Name) })
	for _, c := range deletedNonAtomic {
		groups = append(groups, []simulator.NodeToBeRemoved{c})
	}
	return toBeRemoved, groups
}

// buildConstraints reads targetNodeCount limits from all ComputeClass CRDs and
// seeds their current node counts from the cluster snapshot.
func (p *AtomicMinCapacityProcessor) buildConstraints(autoscalingCtx *ca_context.AutoscalingContext, scaleDownCtx *nodes.ScaleDownContext) ([]*minCapacityConstraint, error) {
	if !computeclass.IsComputeClassMinCapacityEnabled(p.experimentsManager) {
		return nil, nil
	}

	crds, err := p.ccLister.ListCrds()
	if err != nil {
		return nil, err
	}

	var constraints []*minCapacityConstraint
	for _, c := range crds {
		crdName := c.Name()
		if c.TargetNodeCount() != nil {
			if target := *c.TargetNodeCount(); target >= 0 {
				constraints = append(constraints, &minCapacityConstraint{crdName: crdName, ruleIdxStr: "", target: target})
			}
		}
		for ruleIdx, r := range c.Rules() {
			if r.TargetNodeCount() != nil {
				if target := *r.TargetNodeCount(); target >= 0 {
					constraints = append(constraints, &minCapacityConstraint{crdName: crdName, ruleIdxStr: strconv.Itoa(ruleIdx), target: target})
				}
			}
		}
	}
	if len(constraints) == 0 {
		return nil, nil
	}

	nodeInfos, err := autoscalingCtx.ClusterSnapshot.ListNodeInfos()
	if err != nil {
		return nil, err
	}
	nodesByName := make(map[string]*apiv1.Node, len(nodeInfos))
	for _, ni := range nodeInfos {
		node := ni.Node()
		if node == nil {
			continue
		}
		// Upcoming placeholder nodes represent capacity that has not yet
		// materialized (e.g. a provision-only slice still coming up). They must
		// not count toward the committed current node count, otherwise the
		// processor could release a ready slice while relying on capacity.
		if val, ok := node.Annotations[annotations.NodeUpcomingAnnotation]; ok {
			if res, err := strconv.ParseBool(val); err == nil && res {
				continue
			}
		}
		// Likewise for nodes already being deleted, e.g. by defrag.
		if beingDeleted(node) {
			continue
		}
		nodesByName[node.Name] = node
		for _, c := range constraints {
			if c.matches(node) {
				c.current++
			}
		}
	}

	// Subtract nodes already in deletion so current reflects capacity that
	// will actually remain. Without this, a slice whose deletion is already
	// in flight would be counted, letting another slice be removed.
	if scaleDownCtx != nil && scaleDownCtx.ActuationStatus != nil {
		empty, drained := scaleDownCtx.ActuationStatus.DeletionsInProgress()
		inDeletion := append(append([]string{}, empty...), drained...)
		for _, name := range inDeletion {
			node, ok := nodesByName[name]
			if !ok {
				continue
			}
			for _, c := range constraints {
				if c.matches(node) {
					c.current--
				}
			}
		}
	}
	return constraints, nil
}

// canRemoveGroup reports whether removing the whole group keeps every affected
// constraint at or above its target.
func (p *AtomicMinCapacityProcessor) canRemoveGroup(constraints []*minCapacityConstraint, group []simulator.NodeToBeRemoved) bool {
	for _, c := range constraints {
		// Checked even if the group contributes 0, e.g. when it is already being deleted.
		if !slices.ContainsFunc(group, func(n simulator.NodeToBeRemoved) bool { return c.matches(n.Node) }) {
			continue
		}
		if c.current-contributionOf(c, group) < c.target {
			return false
		}
	}
	return true
}

// applyRemoval decrements the current counts to reflect an accepted group removal.
func (p *AtomicMinCapacityProcessor) applyRemoval(constraints []*minCapacityConstraint, group []simulator.NodeToBeRemoved) {
	for _, c := range constraints {
		c.current -= contributionOf(c, group)
	}
}

func contributionOf(c *minCapacityConstraint, group []simulator.NodeToBeRemoved) int {
	contribution := 0
	for _, n := range group {
		if c.matches(n.Node) && !beingDeleted(n.Node) {
			contribution++
		}
	}
	return contribution
}

// beingDeleted reports whether the node is leaving the cluster, or is about to:
// the defrag HardTaint marks defrag candidates, not only nodes being deleted.
// Delete-before-create candidates and create-before-delete candidates still
// waiting for their pods to fit carry it too. Since this processor also runs
// in the main scale-down, such a candidate lowers the current node count of
// its ComputeClass and can hold back unrelated atomic groups in that class
// until it is deleted or abandoned. This is accepted: it errs on the side of
// keeping capacity for a class whose candidate is expected to go away.
func beingDeleted(node *apiv1.Node) bool {
	return node.DeletionTimestamp != nil || taints.HasToBeDeletedTaint(node) || taints.HasTaint(node, defrag.HardTaint)
}

func nodeNames(candidates []simulator.NodeToBeRemoved) []string {
	names := make([]string, 0, len(candidates))
	for _, c := range candidates {
		names = append(names, c.Node.Name)
	}
	return names
}

// CleanUp is called at CA termination.
func (p *AtomicMinCapacityProcessor) CleanUp() {}
