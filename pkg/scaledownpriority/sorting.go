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

package scaledownpriority

import (
	"context"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
)

// NodeGroupProvider is the subset of cloudprovider.CloudProvider needed by
// Sorting to resolve atomic node groups.
type NodeGroupProvider interface {
	NodeGroupForNode(ctx context.Context, node *apiv1.Node) (cloudprovider.NodeGroup, error)
}

// Sorting orders scale-down candidates by descending NodePriority. For nodes in
// an atomic (ZeroOrMaxNodeScaling) node group, it uses the maximum priority
// across all nodes in the group so that tainting a single node prioritizes the
// entire atomic slice without splitting its nodes across priority levels. It
// implements scaledowncandidates.CandidatesComparer. Candidates with equal
// priority defer to later comparers.
type Sorting struct {
	parser          *TaintParser
	cloudProvider   NodeGroupProvider
	clusterSnapshot clustersnapshot.ClusterSnapshot

	atomicGroupCache    map[string]bool
	groupPriorityLoaded bool
	groupPriorityCache  map[string]int
	// nodeGroupErrorLogged holds the nodes whose node group lookup failed in
	// this sort, so that the error is logged once per node and sort.
	nodeGroupErrorLogged map[string]bool
}

// NewSorting creates a Sorting. If cloudProvider and clusterSnapshot are
// non-nil, nodes in atomic (ZeroOrMaxNodeScaling) node groups inherit the
// maximum priority of their node group.
func NewSorting(parser *TaintParser, cloudProvider NodeGroupProvider, clusterSnapshot clustersnapshot.ClusterSnapshot) *Sorting {
	return &Sorting{
		parser:          parser,
		cloudProvider:   cloudProvider,
		clusterSnapshot: clusterSnapshot,
	}
}

// ScaleDownEarlierThan reports whether node1 has a higher scale-down priority
// than node2.
func (s *Sorting) ScaleDownEarlierThan(node1, node2 *apiv1.Node) bool {
	if !s.parser.Enabled() {
		return false
	}
	return s.effectivePriority(node1) > s.effectivePriority(node2)
}

// ResetState clears per-sort caches.
func (s *Sorting) ResetState() {
	s.atomicGroupCache = nil
	s.groupPriorityLoaded = false
	s.groupPriorityCache = nil
	s.nodeGroupErrorLogged = nil
}

func (s *Sorting) effectivePriority(node *apiv1.Node) int {
	nodePrio := s.parser.NodePriority(node)
	if node == nil || s.cloudProvider == nil || s.clusterSnapshot == nil {
		return nodePrio
	}
	ng, err := s.cloudProvider.NodeGroupForNode(context.Background(), node)
	if err != nil {
		s.logNodeGroupError(node, err)
		return nodePrio
	}
	if ng == nil {
		return nodePrio
	}
	if !s.isAtomicNodeGroup(ng) {
		return nodePrio
	}
	s.ensureGroupPrioritiesLoaded()
	return max(nodePrio, s.groupPriorityCache[ng.Id()])
}

func (s *Sorting) isAtomicNodeGroup(ng cloudprovider.NodeGroup) bool {
	if s.atomicGroupCache == nil {
		s.atomicGroupCache = make(map[string]bool)
	}
	if isAtomic, ok := s.atomicGroupCache[ng.Id()]; ok {
		return isAtomic
	}
	opts, err := ng.GetOptions(context.Background(), config.NodeGroupAutoscalingOptions{})
	if err != nil && err != cloudprovider.ErrNotImplemented {
		klog.Warningf("Scale-down priority: failed to get the options of node group %s, treating it as non-atomic: %v", ng.Id(), err)
	}
	isAtomic := err == nil && opts != nil && opts.ZeroOrMaxNodeScaling
	s.atomicGroupCache[ng.Id()] = isAtomic
	return isAtomic
}

func (s *Sorting) ensureGroupPrioritiesLoaded() {
	if s.groupPriorityLoaded {
		return
	}
	s.groupPriorityLoaded = true
	s.groupPriorityCache = make(map[string]int)
	nodeInfos, err := s.clusterSnapshot.ListNodeInfos()
	if err != nil {
		klog.Warningf("Scale-down priority: failed to list nodes, atomic node groups don't inherit the priority of their nodes: %v", err)
		return
	}
	for _, ni := range nodeInfos {
		n := ni.Node()
		p := s.parser.NodePriority(n)
		if p <= 0 {
			continue
		}
		ng, err := s.cloudProvider.NodeGroupForNode(context.Background(), n)
		if err != nil {
			s.logNodeGroupError(n, err)
			continue
		}
		if ng == nil {
			continue
		}
		s.groupPriorityCache[ng.Id()] = max(s.groupPriorityCache[ng.Id()], p)
	}
}

// logNodeGroupError logs that the node group of node can't be determined, once
// per node and sort.
func (s *Sorting) logNodeGroupError(node *apiv1.Node, err error) {
	if s.nodeGroupErrorLogged[node.Name] {
		return
	}
	if s.nodeGroupErrorLogged == nil {
		s.nodeGroupErrorLogged = make(map[string]bool)
	}
	s.nodeGroupErrorLogged[node.Name] = true
	klog.Warningf("Scale-down priority: failed to get the node group of node %s, using its own priority: %v", node.Name, err)
}
