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
	"sort"
	"strconv"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodes"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"

	gke_labels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	cc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

// AtomicMinCapacityProcessor is a scale-down processor (implements
// nodes.ScaleDownSetProcessor). It enforces ComputeClass targetNodeCount for
// atomically-scaled (ZeroOrMaxNodeScaling) node groups at whole-group
// granularity, accepting a group's removal only if doing so keeps every
// affected ComputeClass at or above its targetNodeCount.
//
// It must run after AtomicResizeFilteringProcessor and only accepts or
// rejects whole groups. Atomic groups are exempt from the per-node
// TargetNodeCountQuota (see TargetNodeCountQuota.AppliesTo); this processor
// restores whole-group enforcement. Does not influence the defrag path —
// see pkg/autoscaler/builder.go for how defrag keeps per-node quota.
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
// passed through unchanged (they are enforced by the per-node quota).
func (p *AtomicMinCapacityProcessor) FilterUnremovableNodes(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, scaleDownCtx *nodes.ScaleDownContext, candidates []simulator.NodeToBeRemoved) ([]simulator.NodeToBeRemoved, []simulator.UnremovableNode) {
	constraints, err := p.buildConstraints(autoscalingCtx, scaleDownCtx)
	if err != nil {
		klog.Errorf("AtomicMinCapacityProcessor: failed to build min-capacity constraints, passing all candidates through: %v", err)
		return candidates, nil
	}
	if len(constraints) == 0 {
		return candidates, nil
	}

	nodesToBeRemoved := []simulator.NodeToBeRemoved{}
	unremovableNodes := []simulator.UnremovableNode{}

	// Group atomic candidates by node group. Non-atomic candidates pass through.
	atomicGroups := map[string][]simulator.NodeToBeRemoved{}
	groupOrder := []string{}
	for _, c := range candidates {
		nodeGroup, err := autoscalingCtx.CloudProvider.NodeGroupForNode(ctx, c.Node)
		if err != nil || nodeGroup == nil {
			nodesToBeRemoved = append(nodesToBeRemoved, c)
			continue
		}
		opts, err := nodeGroup.GetOptions(ctx, autoscalingCtx.NodeGroupDefaults)
		if err != nil && err != cloudprovider.ErrNotImplemented {
			klog.Errorf("AtomicMinCapacityProcessor: failed to get options for node group %s, passing node %s through: %v", nodeGroup.Id(), c.Node.Name, err)
			nodesToBeRemoved = append(nodesToBeRemoved, c)
			continue
		}
		if opts == nil || !opts.ZeroOrMaxNodeScaling {
			nodesToBeRemoved = append(nodesToBeRemoved, c)
			continue
		}
		id := nodeGroup.Id()
		if _, ok := atomicGroups[id]; !ok {
			groupOrder = append(groupOrder, id)
		}
		atomicGroups[id] = append(atomicGroups[id], c)
	}

	// Process atomic groups in a deterministic order.
	sort.Strings(groupOrder)
	for _, id := range groupOrder {
		group := atomicGroups[id]
		if p.canRemoveGroup(constraints, group) {
			p.applyRemoval(constraints, group)
			nodesToBeRemoved = append(nodesToBeRemoved, group...)
			klog.Infof("AtomicMinCapacityProcessor: scaling down %d nodes from atomic group %s; ComputeClass minimum capacity is satisfied", len(group), id)
		} else {
			klog.Infof("AtomicMinCapacityProcessor: not scaling down %d nodes from atomic group %s; would drop ComputeClass below targetNodeCount", len(group), id)
			for _, c := range group {
				unremovableNodes = append(unremovableNodes, simulator.UnremovableNode{Node: c.Node, Reason: simulator.NodeGroupMinSizeReached})
			}
		}
	}

	return nodesToBeRemoved, unremovableNodes
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
		contribution := contributionOf(c, group)
		if contribution == 0 {
			continue
		}
		if c.current-contribution < c.target {
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
		if c.matches(n.Node) {
			contribution++
		}
	}
	return contribution
}

// CleanUp is called at CA termination.
func (p *AtomicMinCapacityProcessor) CleanUp() {}
