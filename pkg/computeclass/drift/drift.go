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

// Package drift determines whether a node group still matches the configuration
// of the ComputeClass it belongs to.
//
// Drift is a property of a node group, not of an individual node: it compares
// the node group's configuration against the ComputeClass configuration and
// never looks at node-level state. This means drift can be evaluated for every
// node group in the cluster independently of whether its nodes are eligible for
// scale-down, which is what allows drift to be reported for nodes that the
// defrag pipeline filters out before they reach the nodeconfigdrift plugin.
package drift

import (
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
)

// CRDLister is the subset of lister.Lister used by the Evaluator.
type CRDLister interface {
	// NodeGroupCrd returns the CRD that should be assigned to the node group.
	NodeGroupCrd(nodeGroup cloudprovider.NodeGroup) (crd.CRD, string, error)
}

// Matcher is the subset of computeclass.Matcher used by the Evaluator.
type Matcher interface {
	// MatchesCrdLabel reports whether the node group belongs to the CRD.
	MatchesCrdLabel(cloudprovider.NodeGroup, crd.CRD) bool
	// MatchesCrdConfig reports whether the node group matches the CRD-wide config.
	MatchesCrdConfig(cloudprovider.NodeGroup, crd.CRD) bool
	// FirstMatchedRule returns the first CRD rule matching the node group.
	FirstMatchedRule(cloudprovider.NodeGroup, crd.CRD) (bool, int, rules.Rule)
}

// Result describes the outcome of evaluating a single node group against the
// ComputeClass it belongs to.
type Result struct {
	// CRD is the ComputeClass the node group belongs to, or nil if it does not
	// belong to one.
	CRD crd.CRD
	// CRDName is the name of the ComputeClass the node group belongs to, or ""
	// if it does not belong to one.
	CRDName string
	// MigrationEnabled reports whether the ComputeClass has config drift
	// migration turned on (spec.activeMigration.configDrift). Drift is only
	// ever reported as true when this is true.
	MigrationEnabled bool
	// Drifted reports whether the node group no longer matches the current
	// ComputeClass configuration and is therefore eligible for migration.
	Drifted bool
}

// Evaluator evaluates config drift of node groups against their ComputeClass.
//
// It is stateless and safe to reuse. Use Cache to memoize results across the
// many nodes that share a node group within a single pass.
type Evaluator struct {
	lister  CRDLister
	matcher Matcher
}

// NewEvaluator returns an Evaluator using the given lister and matcher.
func NewEvaluator(l CRDLister, m Matcher) *Evaluator {
	return &Evaluator{lister: l, matcher: m}
}

// EvaluateNodeGroup resolves the ComputeClass of the given node group and
// reports whether it has drifted away from that ComputeClass's configuration.
//
// A node group is only ever reported as drifted when its ComputeClass has
// config drift migration enabled. Errors resolving the ComputeClass are logged
// and reported as "not drifted", so that a transient lister failure can never
// cause nodes to be migrated.
func (e *Evaluator) EvaluateNodeGroup(nodeGroup cloudprovider.NodeGroup) Result {
	c, cName, err := e.lister.NodeGroupCrd(nodeGroup)
	if err != nil {
		klog.Errorf("failed to get CRD for node group %v: %v", nodeGroup.Id(), err)
		return Result{}
	}
	if c == nil || cName == "" {
		return Result{}
	}
	res := Result{CRD: c, CRDName: cName, MigrationEnabled: c.ConfigDrift()}
	if !res.MigrationEnabled {
		return res
	}
	res.Drifted = e.IsNodeGroupDrifted(nodeGroup, c)
	return res
}

// IsNodeGroupDrifted reports whether the node group belongs to the given
// ComputeClass but no longer matches its configuration.
func (e *Evaluator) IsNodeGroupDrifted(nodeGroup cloudprovider.NodeGroup, c crd.CRD) bool {
	if !e.matcher.MatchesCrdLabel(nodeGroup, c) {
		return false
	}
	return !e.IsNodeGroupCompliant(nodeGroup, c)
}

// IsNodeGroupCompliant reports whether the node group matches the current
// configuration of the given ComputeClass.
//
// When the ComputeClass declares rules and does not allow scaling up anyway,
// compliance means matching at least one rule. Otherwise it means matching the
// ComputeClass-wide configuration.
func (e *Evaluator) IsNodeGroupCompliant(nodeGroup cloudprovider.NodeGroup, c crd.CRD) bool {
	if len(c.Rules()) > 0 && !c.ScaleUpAnyway() {
		found, _, _ := e.matcher.FirstMatchedRule(nodeGroup, c)
		return found
	}
	return e.matcher.MatchesCrdConfig(nodeGroup, c)
}

// Cache memoizes drift evaluation per node group.
//
// Drift is identical for every node in a node group, so callers iterating over
// nodes should share a Cache to avoid re-resolving the ComputeClass and
// re-running the matcher for each node. A Cache is scoped to a single pass over
// the cluster and must be discarded afterwards; it is not safe for concurrent
// use.
type Cache struct {
	evaluator *Evaluator
	results   map[string]Result
}

// NewCache returns a Cache backed by the given Evaluator.
func NewCache(evaluator *Evaluator) *Cache {
	return &Cache{evaluator: evaluator, results: make(map[string]Result)}
}

// EvaluateNodeGroup returns the drift evaluation for the given node group,
// reusing a previously computed result when available.
func (c *Cache) EvaluateNodeGroup(nodeGroup cloudprovider.NodeGroup) Result {
	id := nodeGroup.Id()
	if res, ok := c.results[id]; ok {
		return res
	}
	res := c.evaluator.EvaluateNodeGroup(nodeGroup)
	c.results[id] = res
	return res
}
