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
	"cmp"
	"context"
	"errors"
	"slices"
	"strconv"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodes"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator"

	gke_labels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	cc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

// ComputeClassMinCapacityProcessor implements nodes.ScaleDownSetProcessor and the
// defrag processor's ScaleDownFilter. It enforces ComputeClass min capacity on
// scale-down, deciding on atomically-scaled (ZeroOrMaxNodeScaling) node groups
// as whole groups and on other ComputeClass nodes one by one.
//
// It must run after AtomicResizeFilteringProcessor. Defrag also calls it before
// deleting a create-before-delete candidate because defrag's quota tracker only
// covers the rule-level min capacity.
type ComputeClassMinCapacityProcessor struct {
	ccLister           cc_lister.Lister
	experimentsManager experiments.Manager
	// nodeFilter excludes nodes from the ComputeClass node counts, the same way
	// it excludes them from the min quotas tracker (e.g. surge upgrade nodes).
	nodeFilter resourcequotas.NodeFilter
}

// NewComputeClassMinCapacityProcessor creates a new
// ComputeClassMinCapacityProcessor. A nil nodeFilter excludes no node.
func NewComputeClassMinCapacityProcessor(ccLister cc_lister.Lister, experimentsManager experiments.Manager, nodeFilter resourcequotas.NodeFilter) *ComputeClassMinCapacityProcessor {
	return &ComputeClassMinCapacityProcessor{
		ccLister:           ccLister,
		experimentsManager: experimentsManager,
		nodeFilter:         nodeFilter,
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

// FilterUnremovableNodes blocks the scale-down of ComputeClass nodes that would
// take an affected ComputeClass below its min capacity, deciding on atomic node
// groups as a whole: groups with no pods to reschedule first, then larger
// groups. Other candidates pass through. The removable candidates keep their
// input order.
func (p *ComputeClassMinCapacityProcessor) FilterUnremovableNodes(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, scaleDownCtx *nodes.ScaleDownContext, candidates []simulator.NodeToBeRemoved) ([]simulator.NodeToBeRemoved, []simulator.UnremovableNode) {
	groups := p.groupCandidates(ctx, autoscalingCtx, candidates)
	if len(groups) == 0 {
		return candidates, nil
	}

	constraints, err := p.buildConstraints(autoscalingCtx, scaleDownCtx)
	if err != nil {
		// Fail closed for ComputeClass nodes: this processor is their only min
		// capacity protection. Other nodes have no min capacity.
		klog.Errorf("ComputeClassMinCapacityProcessor: failed to build min-capacity constraints, not scaling down ComputeClass nodes: %v", err)
		return keepGroups(candidates, groups, simulator.UnexpectedError)
	}
	if len(constraints) == 0 {
		return candidates, nil
	}

	var keptGroups [][]simulator.NodeToBeRemoved
	var released, kept []string
	for _, group := range groups {
		if p.canRemoveGroup(constraints, group) {
			p.applyRemoval(constraints, group)
			released = append(released, nodeNames(group)...)
		} else {
			keptGroups = append(keptGroups, group)
			kept = append(kept, nodeNames(group)...)
		}
	}
	klog.V(4).Infof("ComputeClassMinCapacityProcessor: ComputeClass min capacity allows scaling down nodes %v, keeps nodes %v", released, kept)
	// TODO(b/586927841): NodeGroupMinSizeReached makes the noScaleDown
	// visibility log say the node group is at its min size. Report a reason
	// that says the ComputeClass min capacity keeps the node.
	return keepGroups(candidates, keptGroups, simulator.NodeGroupMinSizeReached)
}

// keepGroups returns the candidates outside the given groups, in candidate
// order, and the nodes of the groups as unremovable for the given reason.
func keepGroups(candidates []simulator.NodeToBeRemoved, groups [][]simulator.NodeToBeRemoved, reason simulator.UnremovableReason) ([]simulator.NodeToBeRemoved, []simulator.UnremovableNode) {
	keptNames := make(map[string]bool)
	unremovableNodes := []simulator.UnremovableNode{}
	for _, group := range groups {
		for _, c := range group {
			keptNames[c.Node.Name] = true
			unremovableNodes = append(unremovableNodes, simulator.UnremovableNode{Node: c.Node, Reason: reason})
		}
	}
	removable := slices.DeleteFunc(slices.Clone(candidates), func(c simulator.NodeToBeRemoved) bool { return keptNames[c.Node.Name] })
	return removable, unremovableNodes
}

// TrimScaleDownCandidates returns the given nodes, in the given order, that can
// be deleted without taking any ComputeClass below its min capacity.
func (p *ComputeClassMinCapacityProcessor) TrimScaleDownCandidates(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, candidateNodes []*apiv1.Node) []*apiv1.Node {
	// The candidates carry no pods to reschedule, so every group counts as
	// empty and the empty-groups-first order doesn't apply to defrag.
	candidates := make([]simulator.NodeToBeRemoved, 0, len(candidateNodes))
	for _, node := range candidateNodes {
		candidates = append(candidates, simulator.NodeToBeRemoved{Node: node})
	}
	// Defrag has no scale-down context. The actuation status still lets the
	// count skip nodes whose deletion started but isn't in the snapshot yet.
	scaleDownCtx := nodes.NewDefaultScaleDownContext()
	if autoscalingCtx.ScaleDownActuator != nil {
		scaleDownCtx.ActuationStatus = autoscalingCtx.ScaleDownActuator.CheckStatus()
	}
	removable, _ := p.FilterUnremovableNodes(ctx, autoscalingCtx, scaleDownCtx, candidates)
	removableNodes := make([]*apiv1.Node, 0, len(removable))
	for _, c := range removable {
		removableNodes = append(removableNodes, c.Node)
	}
	return removableNodes
}

// groupCandidates returns the groups of ComputeClass candidates this processor
// decides on, each removed or kept as a whole: one per atomic node group, and
// one per other ComputeClass node. Candidates without a ComputeClass label or
// with a predefined GKE compute class, and nodes that are not autoscaled, are
// in no group. In the main scale-down, groups with no pods to reschedule come
// first, so that the surplus goes to nodes that can be removed without
// evictions. Then larger groups come first, so that single nodes don't use up
// the surplus a whole atomic group could free. Groups that tie keep the order
// in which their first node appears in the candidates. For non-atomic nodes
// this is the scale-down candidate order, so that order decides which nodes go.
// In the main scale-down, atomic groups that tie come from
// AtomicResizeFilteringProcessor in no particular order.
//
// A ComputeClass node whose node group can't be determined is decided on as
// its own group, so that the min capacity still protects it.
func (p *ComputeClassMinCapacityProcessor) groupCandidates(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, candidates []simulator.NodeToBeRemoved) [][]simulator.NodeToBeRemoved {
	var groups [][]simulator.NodeToBeRemoved
	atomicGroupIndexes := map[string]int{}
	for _, c := range candidates {
		// Predefined compute classes (e.g. Balanced, Scale-Out) have no min
		// capacity.
		if cc := c.Node.Labels[gke_labels.ComputeClassLabel]; cc == "" || machinetypes.IsPredefinedComputeClass(cc) {
			continue
		}
		nodeGroup, err := autoscalingCtx.CloudProvider.NodeGroupForNode(ctx, c.Node)
		if err == nil && nodeGroup == nil {
			continue
		}
		atomic := false
		if err != nil {
			klog.Errorf("ComputeClassMinCapacityProcessor: failed to get the node group of node %s, treating it as non-atomic: %v", c.Node.Name, err)
		} else if opts, err := nodeGroup.GetOptions(ctx, autoscalingCtx.NodeGroupDefaults); err != nil && !errors.Is(err, cloudprovider.ErrNotImplemented) {
			klog.Errorf("ComputeClassMinCapacityProcessor: failed to get options for node group %s, treating node %s as non-atomic: %v", nodeGroup.Id(), c.Node.Name, err)
		} else {
			atomic = opts != nil && opts.ZeroOrMaxNodeScaling
		}
		if !atomic {
			groups = append(groups, []simulator.NodeToBeRemoved{c})
			continue
		}
		if i, found := atomicGroupIndexes[nodeGroup.Id()]; found {
			groups[i] = append(groups[i], c)
		} else {
			atomicGroupIndexes[nodeGroup.Id()] = len(groups)
			groups = append(groups, []simulator.NodeToBeRemoved{c})
		}
	}
	slices.SortStableFunc(groups, func(a, b []simulator.NodeToBeRemoved) int {
		if aEmpty, bEmpty := hasNoPodsToReschedule(a), hasNoPodsToReschedule(b); aEmpty != bEmpty {
			if aEmpty {
				return -1
			}
			return 1
		}
		return cmp.Compare(len(b), len(a))
	})
	return groups
}

func hasNoPodsToReschedule(group []simulator.NodeToBeRemoved) bool {
	return !slices.ContainsFunc(group, func(c simulator.NodeToBeRemoved) bool { return len(c.PodsToReschedule) > 0 })
}

// buildConstraints reads targetNodeCount limits from all ComputeClass CRDs and
// seeds their current node counts from the cluster snapshot.
func (p *ComputeClassMinCapacityProcessor) buildConstraints(autoscalingCtx *ca_context.AutoscalingContext, scaleDownCtx *nodes.ScaleDownContext) ([]*minCapacityConstraint, error) {
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
		if !computeclass.CountsTowardMinCapacityScaleDown(node, p.nodeFilter) {
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
func (p *ComputeClassMinCapacityProcessor) canRemoveGroup(constraints []*minCapacityConstraint, group []simulator.NodeToBeRemoved) bool {
	for _, c := range constraints {
		// Checked even if the group contributes 0, e.g. when it is already being
		// deleted.
		if !slices.ContainsFunc(group, func(n simulator.NodeToBeRemoved) bool { return c.matches(n.Node) }) {
			continue
		}
		if c.current-p.contributionOf(c, group) < c.target {
			return false
		}
	}
	return true
}

// applyRemoval decrements the current counts to reflect an accepted group removal.
func (p *ComputeClassMinCapacityProcessor) applyRemoval(constraints []*minCapacityConstraint, group []simulator.NodeToBeRemoved) {
	for _, c := range constraints {
		c.current -= p.contributionOf(c, group)
	}
}

// contributionOf returns the number of the group's nodes that count toward the
// constraint's min capacity.
func (p *ComputeClassMinCapacityProcessor) contributionOf(c *minCapacityConstraint, group []simulator.NodeToBeRemoved) int {
	contribution := 0
	for _, n := range group {
		if c.matches(n.Node) && computeclass.CountsTowardMinCapacityScaleDown(n.Node, p.nodeFilter) {
			contribution++
		}
	}
	return contribution
}

func nodeNames(candidates []simulator.NodeToBeRemoved) []string {
	names := make([]string, 0, len(candidates))
	for _, c := range candidates {
		names = append(names, c.Node.Name)
	}
	return names
}

// CleanUp is called at CA termination.
func (p *ComputeClassMinCapacityProcessor) CleanUp() {}
