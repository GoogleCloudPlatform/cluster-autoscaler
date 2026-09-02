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

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/klog/v2"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown"
	clustersnapshot "sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	cataints "sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
)

// MaxNodeDisruptionTracker tracks and enforces MaxNodeDisruption limits for ComputeClasses (CRDs).
type MaxNodeDisruptionTracker struct {
	ccLister                  lister.Lister
	remainingDisruptionBudget map[string]int
	reservedNodes             map[string]bool
}

// NewMaxNodeDisruptionTracker creates a new MaxNodeDisruptionTracker and calculates the initial
// remaining disruption budget for each ComputeClass (CRD).
// It accounts for both:
//  1. Ongoing deletions currently tracked by ScaleDownActuator (e.g. while draining or awaiting cloud provider calls).
//  2. Nodes in the cluster snapshot that are actively terminating or marked with ToBeDeletedTaint, ensuring budget
//     remains consumed until the VM and Node object are completely removed from the cluster.
func NewMaxNodeDisruptionTracker(ctx *ca_context.AutoscalingContext, ccLister lister.Lister, allNodes []*apiv1.Node) *MaxNodeDisruptionTracker {
	tracker := &MaxNodeDisruptionTracker{
		ccLister:                  ccLister,
		remainingDisruptionBudget: make(map[string]int),
		reservedNodes:             make(map[string]bool),
	}
	if ccLister == nil || ctx == nil || ctx.CloudProvider == nil {
		return tracker
	}

	crdMaxDisruption := make(map[string]int)
	actuatorDeletions := make(map[string]int)
	var actuationStatus scaledown.ActuationStatus
	if ctx.ScaleDownActuator != nil {
		actuationStatus = ctx.ScaleDownActuator.CheckStatus()
	}

	// 1. Discover all CRDs with positive MaxNodeDisruption via Lister
	if crds, err := ccLister.ListCrds(); err == nil {
		for _, c := range crds {
			if c == nil || c.MaxNodeDisruption() == nil || *c.MaxNodeDisruption() <= 0 || c.Name() == "" {
				continue
			}
			crdMaxDisruption[c.Name()] = int(*c.MaxNodeDisruption())
		}
	}

	// 2. Discover CRDs and actuator deletions per node group
	for _, nodeGroup := range ctx.CloudProvider.NodeGroups(context.TODO()) {
		_, crdName, _ := ccLister.NodeGroupCrd(nodeGroup)
		if actuationStatus != nil && crdName != "" {
			actuatorDeletions[crdName] += actuationStatus.DeletionsCount(nodeGroup.Id())
		}
	}

	nodeMap := make(map[string]*apiv1.Node, len(allNodes))
	disruptedNodes := make(map[string]sets.Set[string])

	// 3. Scan all nodes in snapshot
	for _, node := range allNodes {
		if node == nil {
			continue
		}
		nodeMap[node.Name] = node

		c, crdName, err := ccLister.NodeCrd(node)
		if err != nil || c == nil || c.MaxNodeDisruption() == nil || *c.MaxNodeDisruption() <= 0 || crdName == "" {
			continue
		}

		if !isNodeBeingDeleted(node) {
			continue
		}
		if disruptedNodes[crdName] == nil {
			disruptedNodes[crdName] = sets.New[string]()
		}
		disruptedNodes[crdName].Insert(node.Name)
	}

	// 4. Scan actuator in-progress deletions
	if actuationStatus != nil {
		empty, drained := actuationStatus.DeletionsInProgress()
		for _, nodeName := range append(empty, drained...) {
			node := nodeMap[nodeName]
			if node == nil {
				continue
			}
			c, crdName, err := ccLister.NodeCrd(node)
			if err != nil || c == nil || c.MaxNodeDisruption() == nil || *c.MaxNodeDisruption() <= 0 || crdName == "" {
				continue
			}
			if disruptedNodes[crdName] == nil {
				disruptedNodes[crdName] = sets.New[string]()
			}
			disruptedNodes[crdName].Insert(nodeName)
		}
	}

	for crdName, maxDisruption := range crdMaxDisruption {
		deletions := len(disruptedNodes[crdName])
		if actuatorDeletions[crdName] > deletions {
			deletions = actuatorDeletions[crdName]
		}
		remaining := maxDisruption - deletions
		if remaining < 0 {
			remaining = 0
		}
		tracker.remainingDisruptionBudget[crdName] = remaining
	}

	return tracker
}

// NewMaxNodeDisruptionTrackerWithBudgets creates a MaxNodeDisruptionTracker with predefined budgets (primarily for testing).
func NewMaxNodeDisruptionTrackerWithBudgets(ccLister lister.Lister, budgets map[string]int) *MaxNodeDisruptionTracker {
	tracker := &MaxNodeDisruptionTracker{
		ccLister:                  ccLister,
		remainingDisruptionBudget: make(map[string]int, len(budgets)),
		reservedNodes:             make(map[string]bool),
	}
	for k, v := range budgets {
		tracker.remainingDisruptionBudget[k] = v
	}
	return tracker
}

// Budget returns the remaining disruption budget for a given CRD name.
func (t *MaxNodeDisruptionTracker) Budget(crdName string) (int, bool) {
	if t == nil || t.remainingDisruptionBudget == nil {
		return 0, false
	}
	budget, exists := t.remainingDisruptionBudget[crdName]
	return budget, exists
}

// FilterNodesViolatingMaxDisruption filters scale-down candidates that would violate
// their ComputeClass's MaxNodeDisruption limit.
func (t *MaxNodeDisruptionTracker) FilterNodesViolatingMaxDisruption(ctx *ca_context.AutoscalingContext, nodes []string) []string {
	if t == nil || t.ccLister == nil || len(t.remainingDisruptionBudget) == 0 || len(nodes) == 0 {
		return nodes
	}

	var result []string
	tempBudget := make(map[string]int, len(t.remainingDisruptionBudget))
	for k, v := range t.remainingDisruptionBudget {
		tempBudget[k] = v
	}
	seenNodes := make(map[string]bool)

	for _, nodeName := range nodes {
		nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil {
			if !errors.Is(err, clustersnapshot.ErrNodeNotFound) {
				klog.Errorf("Defrag: failed to get NodeInfo for node %s: %v", nodeName, err)
			}
			continue
		}
		node := nodeInfo.Node()
		if node == nil {
			continue
		}

		if isNodeBeingDeleted(node) {
			result = append(result, node.Name)
			continue
		}

		// If node was already reserved in a pending candidate, or already seen in this slice, don't decrement budget again
		if t.reservedNodes[node.Name] || seenNodes[node.Name] {
			result = append(result, node.Name)
			seenNodes[node.Name] = true
			continue
		}
		seenNodes[node.Name] = true

		_, crdName, err := t.ccLister.NodeCrd(node)
		if err != nil || crdName == "" {
			result = append(result, node.Name)
			continue
		}

		if budget, exists := tempBudget[crdName]; exists {
			if budget > 0 {
				tempBudget[crdName]--
				result = append(result, node.Name)
			} else {
				klog.V(1).Infof("Skipping %s - compute class %s max node disruption reached", node.Name, crdName)
			}
		} else {
			result = append(result, node.Name)
		}
	}

	return result
}

// ReserveMaxDisruptionBudget reserves disruption budget for an existing candidate
// that has not yet scaled down, without modifying candidate.Nodes.
// This prevents newly evaluated candidates from consuming budget already intended
// for this in-flight candidate.
func (t *MaxNodeDisruptionTracker) ReserveMaxDisruptionBudget(ctx *ca_context.AutoscalingContext, candidate *defrag.Candidate, isNodeScaleDownStarted func(string) bool) {
	if t == nil || t.ccLister == nil || len(t.remainingDisruptionBudget) == 0 || len(candidate.Nodes) == 0 {
		return
	}
	for _, nodeName := range candidate.Nodes {
		if isNodeScaleDownStarted != nil && isNodeScaleDownStarted(nodeName) {
			continue
		}
		if t.reservedNodes[nodeName] {
			continue
		}
		nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil || nodeInfo == nil || nodeInfo.Node() == nil {
			continue
		}
		if isNodeBeingDeleted(nodeInfo.Node()) {
			continue
		}
		_, crdName, err := t.ccLister.NodeCrd(nodeInfo.Node())
		if err != nil || crdName == "" {
			continue
		}
		if _, exists := t.remainingDisruptionBudget[crdName]; exists {
			if t.remainingDisruptionBudget[crdName] > 0 {
				t.remainingDisruptionBudget[crdName]--
				t.reservedNodes[nodeName] = true
			}
		}
	}
}

// isNodeBeingDeleted checks if a node is already terminating or marked for deletion by CA.
func isNodeBeingDeleted(node *apiv1.Node) bool {
	if node == nil {
		return false
	}
	return node.DeletionTimestamp != nil || cataints.HasToBeDeletedTaint(node)
}
