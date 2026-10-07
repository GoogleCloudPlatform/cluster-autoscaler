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

package status

import (
	"context"
	"sort"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/drift"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/observability"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/eligibility"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
)

// defaultBlockReasonDebounceLoops is how many consecutive autoscaler loops a
// waiting node must report the same block reason, or the same absence of one,
// before the reported reason changes.
//
// A block reason is the outcome of a pipeline that re-runs every few seconds
// against transient state, so a single observation is not evidence that a node
// is stuck. Three loops is roughly the interval at which the status is flushed
// to the API server, which means debouncing removes flapping that no user would
// have observed anyway, while a genuinely stuck node is still reported within
// one flush.
const defaultBlockReasonDebounceLoops = 3

// ConfigDriftCounts describes the progress of config drift migration for a
// single ComputeClass.
//
// The buckets partition the nodes of the ComputeClass, so
//
//	CurrentNodes + DriftedNodes + MigratingNodes + Σ BlockedNodes == node count
//
// That invariant is what allows the numbers to be read as progress. Keeping it
// is the reason a catch-all block reason exists: a node whose situation cannot
// be explained is still counted somewhere.
type ConfigDriftCounts struct {
	// CurrentNodes counts nodes whose node group already matches the
	// ComputeClass configuration.
	CurrentNodes int
	// DriftedNodes counts nodes that need to be replaced and are waiting their
	// turn. Nothing prevents their migration; the autoscaler has simply not
	// picked them yet, for example because it works through the drifted nodes
	// one candidate at a time. Nodes held back by an exhausted disruption
	// budget are counted as blocked instead.
	DriftedNodes int
	// MigratingNodes counts nodes that are actively being replaced, including
	// nodes whose replacement capacity has been requested but has not arrived
	// yet.
	MigratingNodes int
	// BlockedNodes counts nodes that need to be replaced but cannot be, grouped
	// by the reason holding them back.
	BlockedNodes map[observability.BlockReason]int
}

// BlockedNodesCount is the number of nodes held back by a single reason.
type BlockedNodesCount struct {
	Reason observability.BlockReason
	Count  int
}

// TotalNodes returns the number of nodes covered by the counts.
func (c ConfigDriftCounts) TotalNodes() int {
	total := c.CurrentNodes + c.DriftedNodes + c.MigratingNodes
	for _, count := range c.BlockedNodes {
		total += count
	}
	return total
}

// SortedBlockedNodes returns the blocked buckets ordered by reason precedence.
//
// The order must be stable across loops: the reported list is merged by the API
// server per reason, and a list whose order churns produces a status update on
// every flush even when nothing changed.
func (c ConfigDriftCounts) SortedBlockedNodes() []BlockedNodesCount {
	if len(c.BlockedNodes) == 0 {
		return nil
	}
	blocked := make([]BlockedNodesCount, 0, len(c.BlockedNodes))
	for reason, count := range c.BlockedNodes {
		blocked = append(blocked, BlockedNodesCount{Reason: reason, Count: count})
	}
	sort.Slice(blocked, func(i, j int) bool {
		if observability.Outranks(blocked[i].Reason, blocked[j].Reason) {
			return true
		}
		if observability.Outranks(blocked[j].Reason, blocked[i].Reason) {
			return false
		}
		// Reasons the precedence list does not know about all rank equally;
		// order them by name so the output stays deterministic.
		return blocked[i].Reason < blocked[j].Reason
	})
	return blocked
}

func (c *ConfigDriftCounts) addBlocked(reason observability.BlockReason) {
	if c.BlockedNodes == nil {
		c.BlockedNodes = make(map[observability.BlockReason]int)
	}
	c.BlockedNodes[reason]++
}

// DriftResolver reports which ComputeClass a node belongs to and whether the
// node's node group still matches that ComputeClass's configuration.
type DriftResolver func(node *apiv1.Node) drift.Result

// NodeGroupProvider resolves the node group a node belongs to.
//
// cloudprovider.CloudProvider implements it.
type NodeGroupProvider interface {
	NodeGroupForNode(context.Context, *apiv1.Node) (cloudprovider.NodeGroup, error)
}

// NewDriftResolver returns a DriftResolver backed by the cloud provider and the
// given drift cache. Node group lookups run under ctx, which should span the
// loop the resolver is used in.
//
// Nodes whose node group cannot be resolved are reported as belonging to no
// ComputeClass, which excludes them from the counts entirely rather than
// letting a lookup failure show up as migration progress.
func NewDriftResolver(ctx context.Context, provider NodeGroupProvider, cache *drift.Cache) DriftResolver {
	return func(node *apiv1.Node) drift.Result {
		nodeGroup, err := provider.NodeGroupForNode(ctx, node)
		if err != nil {
			klog.Warningf("Config drift reporting: failed to get node group for node %q: %v", node.Name, err)
			return drift.Result{}
		}
		if nodeGroup == nil {
			return drift.Result{}
		}
		return cache.EvaluateNodeGroup(nodeGroup)
	}
}

// ConfigDriftSnapshot is everything a single classification pass looks at.
type ConfigDriftSnapshot struct {
	// Nodes are the nodes registered in the cluster.
	//
	// These must come from the node lister rather than the cluster snapshot.
	// The snapshot is a simulation structure that the loop mutates as it runs:
	// scale-down actuation removes a node from it as soon as it starts draining
	// it, which is precisely when that node becomes a migrating node. Reading
	// the snapshot would therefore drop the nodes this reports on, and break the
	// partition invariant by undercounting the ComputeClass.
	//
	// Nodes the autoscaler generated from node group templates are skipped, so
	// the caller can pass the listing unfiltered.
	Nodes []*apiv1.Node
	// Drift resolves the ComputeClass and drift state of a node.
	Drift DriftResolver
	// BlockReasons holds what the defrag pipeline recorded during this loop, or
	// nil if it did not evaluate nodes at all.
	//
	// Nil means "unknown", not "nothing is blocked", and makes the whole pass
	// inconclusive.
	BlockReasons *observability.Registry
	// DeletionsInProgress are the nodes the scale-down actuator is currently
	// deleting.
	DeletionsInProgress sets.Set[string]
}

// ConfigDriftClassifier buckets cluster nodes into config drift migration
// progress counters, per ComputeClass.
//
// It carries the debouncing state between loops, so a single instance must be
// reused across the lifetime of the autoscaler. It is not safe for concurrent
// use.
type ConfigDriftClassifier struct {
	debouncer *blockReasonDebouncer
	// lastKnown is the drift evaluation of every node counted in the previous
	// conclusive loop, used for nodes whose evaluation is no longer available
	// because they are being deleted.
	lastKnown map[string]drift.Result
}

// NewConfigDriftClassifier returns a classifier with the default debouncing.
func NewConfigDriftClassifier() *ConfigDriftClassifier {
	return &ConfigDriftClassifier{debouncer: newBlockReasonDebouncer(defaultBlockReasonDebounceLoops)}
}

// Classify buckets every node of every ComputeClass that has config drift
// migration enabled.
//
// The second return value reports whether the pass was conclusive. It is false
// when the defrag pipeline did not evaluate nodes in this loop, in which case
// no counts are produced: the caller must keep reporting what it reported
// before instead of publishing counts that would claim nothing is blocked.
func (c *ConfigDriftClassifier) Classify(snapshot ConfigDriftSnapshot) (map[CRDId]ConfigDriftCounts, bool) {
	if snapshot.BlockReasons == nil {
		klog.V(4).Info("Config drift reporting: defrag did not evaluate nodes, keeping previously reported counts")
		return nil, false
	}

	nodes := c.resolveNodes(snapshot)
	exhaustedBudgets := exhaustedDisruptionBudgets(nodes, snapshot.DeletionsInProgress)

	counts := make(map[CRDId]*ConfigDriftCounts)
	// Drifted nodes that are not migrating, with the reason they appear to be
	// held back by in this loop, or an empty reason if nothing holds them
	// back. What is reported for them is decided by the debouncer.
	waitingReason := make(map[string]observability.BlockReason)
	waitingCrd := make(map[string]CRDId)

	for _, n := range nodes {
		bucket, ok := counts[n.crdId]
		if !ok {
			bucket = &ConfigDriftCounts{}
			counts[n.crdId] = bucket
		}

		switch {
		case !n.result.Drifted:
			bucket.CurrentNodes++
		case isMigrating(n.node, snapshot.DeletionsInProgress):
			bucket.MigratingNodes++
		default:
			reason := blockReasonForNode(n.node, snapshot.BlockReasons)
			if exhaustedBudgets.Has(n.crdId) {
				reason = observability.MoreSignificant(reason, observability.DisruptionBudgetReached)
			}
			waitingReason[n.node.Name] = reason
			waitingCrd[n.node.Name] = n.crdId
		}
	}

	reported := c.debouncer.confirm(waitingReason)
	for nodeName, crdId := range waitingCrd {
		bucket := counts[crdId]
		if reason, ok := reported[nodeName]; ok {
			bucket.addBlocked(reason)
			continue
		}
		// No reason has held long enough to be trusted. The node is drifted
		// and not migrating, so it is waiting its turn as far as anyone can
		// tell.
		bucket.DriftedNodes++
	}

	report := make(map[CRDId]ConfigDriftCounts, len(counts))
	for crdId, bucket := range counts {
		report[crdId] = *bucket
	}
	return report, true
}

// classifiedNode is a node counted towards a ComputeClass, together with the
// drift evaluation of its node group.
type classifiedNode struct {
	node   *apiv1.Node
	crdId  CRDId
	result drift.Result
}

// resolveNodes returns the real nodes that belong to a ComputeClass with config
// drift migration enabled, together with their drift evaluation, and remembers
// those evaluations for the next loop.
func (c *ConfigDriftClassifier) resolveNodes(snapshot ConfigDriftSnapshot) []classifiedNode {
	known := make(map[string]drift.Result, len(c.lastKnown))
	var resolved []classifiedNode
	for _, node := range snapshot.Nodes {
		if node == nil || !isRealNode(node) {
			continue
		}
		result := snapshot.Drift(node)
		if result.CRD == nil && isMigrating(node, snapshot.DeletionsInProgress) {
			// Once the VM of a node being deleted has left its node group,
			// the node can no longer be resolved to a node group, even though
			// the Node object lingers until the deletion completes. Neither
			// the node's ComputeClass nor its drift can change during that
			// window, so the last evaluation still holds. Dropping the node
			// instead would undercount the ComputeClass exactly while the
			// node is still consuming its disruption budget.
			result = c.lastKnown[node.Name]
		}
		// Drift is only defined, and only reported, for ComputeClasses that
		// asked the autoscaler to act on it.
		if result.CRD == nil || !result.MigrationEnabled {
			continue
		}
		known[node.Name] = result
		resolved = append(resolved, classifiedNode{
			node:   node,
			crdId:  CRDId{CRDLabel: result.CRD.Label(), CRDName: result.CRDName},
			result: result,
		})
	}
	c.lastKnown = known
	return resolved
}

// exhaustedDisruptionBudgets returns the ComputeClasses whose disruption budget
// is fully consumed by nodes that are already being disrupted.
//
// The defrag pipeline records DisruptionBudgetReached only for the nodes of the
// candidate it happened to try in a given loop, so the other nodes waiting for
// the same budget would be reported as drifted or as budget-blocked depending
// on which node the plugin picked. Deriving the reason from the ComputeClass
// instead reports every waiting node the same way.
//
// Like the defrag budget accounting, every disrupted node of the ComputeClass
// consumes the budget, whether or not its node group has drifted.
func exhaustedDisruptionBudgets(nodes []classifiedNode, deletionsInProgress sets.Set[string]) sets.Set[CRDId] {
	budgets := make(map[CRDId]int)
	disrupted := make(map[CRDId]int)
	for _, n := range nodes {
		if limit := n.result.CRD.MaxNodeDisruption(); limit != nil && *limit > 0 {
			budgets[n.crdId] = int(*limit)
		}
		if isMigrating(n.node, deletionsInProgress) {
			disrupted[n.crdId]++
		}
	}
	exhausted := sets.New[CRDId]()
	for crdId, budget := range budgets {
		if disrupted[crdId] >= budget {
			exhausted.Insert(crdId)
		}
	}
	return exhausted
}

// isRealNode reports whether the node exists in the cluster, as opposed to being
// a placeholder the autoscaler generated from a node group template.
//
// The node lister only ever returns registered nodes, so this is a safeguard for
// callers that assemble the listing from somewhere else.
func isRealNode(node *apiv1.Node) bool {
	_, generated := node.Annotations[labels.NodeGeneratedFromTemplateAnnotation]
	return !generated
}

// isMigrating reports whether the autoscaler is actively replacing the node.
//
// Every signal here is node-observable, which is what makes the answer
// independent of how far the node got in the defrag pipeline this loop. The
// defrag taint covers nodes belonging to a live candidate, including those still
// waiting for their replacement to be provisioned, and the remaining signals
// cover the drain and the deletion that follow.
func isMigrating(node *apiv1.Node, deletionsInProgress sets.Set[string]) bool {
	if node.DeletionTimestamp != nil {
		return true
	}
	if taints.HasToBeDeletedTaint(node) {
		return true
	}
	if taints.HasTaint(node, defrag.HardTaint) {
		return true
	}
	return deletionsInProgress.Has(node.Name)
}

// blockReasonForNode returns the most significant known reason why the node
// cannot be migrated, or an empty reason if there is none.
func blockReasonForNode(node *apiv1.Node, reasons *observability.Registry) observability.BlockReason {
	reason := nodeLocalBlockReason(node)
	if recorded, ok := reasons.Reason(node.Name); ok {
		reason = observability.MoreSignificant(reason, recorded)
	}
	return reason
}

// nodeLocalBlockReason returns the most significant blocker that can be read off
// the node itself.
//
// These checks are cheap, so they are re-run here for every node instead of
// being taken from the registry. A node that the pipeline dropped early, or
// never looked at, has no recorded reason at all, and without re-evaluating
// there would be no way to tell that apart from a node that nothing is blocking.
func nodeLocalBlockReason(node *apiv1.Node) observability.BlockReason {
	var reason observability.BlockReason
	if readiness, err := kubernetes.GetNodeReadiness(node); err != nil || !readiness.Ready {
		reason = observability.MoreSignificant(reason, observability.NodeNotReady)
	}
	if _, upcoming := node.Annotations[annotations.NodeUpcomingAnnotation]; upcoming {
		// The node is registered but not yet usable, which the autoscaler
		// treats the same way as a node that is not ready.
		reason = observability.MoreSignificant(reason, observability.NodeNotReady)
	}
	if node.Spec.Unschedulable {
		// A cordon that belongs to an autoscaler-driven deletion is recognized
		// as migration before this is reached. What is left is either a node
		// somebody cordoned or one a node pool operation is working on; the
		// registry carries the operation when the pipeline knows about it,
		// and it outranks the cordon when the two are combined.
		reason = observability.MoreSignificant(reason, observability.Cordoned)
	}
	if eligibility.HasNoScaleDownAnnotation(node) {
		reason = observability.MoreSignificant(reason, observability.NodeConsolidationDisabled)
	}
	return reason
}

// blockReasonDebouncer suppresses changes of block reason that do not persist.
//
// It tracks, per waiting node, how many consecutive loops have observed the
// same reason, where no reason at all counts as a reason of its own, and only
// confirms it once it has held for the configured number of loops. Until then
// the previously confirmed reason keeps being reported, so a node moving from
// one blocker to another, or briefly looking unblocked, does not flap through
// the drifted bucket. A node that stops waiting, because it is migrating, no
// longer drifted, or gone, is forgotten.
type blockReasonDebouncer struct {
	threshold int
	streaks   map[string]blockReasonStreak
}

type blockReasonStreak struct {
	// reason is the reason observed in the most recent loop, or empty if the
	// node did not look blocked.
	reason observability.BlockReason
	// loops is the number of consecutive loops reason has been observed in.
	loops int
	// confirmed is the last reason that held for the full threshold. It is
	// empty if none has, or if the node was last confirmed to be unblocked.
	confirmed observability.BlockReason
}

func newBlockReasonDebouncer(threshold int) *blockReasonDebouncer {
	if threshold < 1 {
		threshold = 1
	}
	return &blockReasonDebouncer{threshold: threshold, streaks: make(map[string]blockReasonStreak)}
}

// confirm advances the debouncer by one loop, given the reason observed for
// every waiting node, empty if a node did not look blocked. It returns the
// reason to report for every node that has one confirmed.
//
// Nodes missing from observed are forgotten, so a node that stops waiting and
// gets blocked again later has to earn its way back rather than being reported
// immediately.
func (d *blockReasonDebouncer) confirm(observed map[string]observability.BlockReason) map[string]observability.BlockReason {
	streaks := make(map[string]blockReasonStreak, len(observed))
	reported := make(map[string]observability.BlockReason)
	for nodeName, reason := range observed {
		streak := blockReasonStreak{reason: reason, loops: 1}
		if previous, ok := d.streaks[nodeName]; ok {
			streak.confirmed = previous.confirmed
			if previous.reason == reason {
				// Capped so that a node blocked indefinitely does not count
				// up without bound.
				streak.loops = min(previous.loops+1, d.threshold)
			}
		}
		if streak.loops >= d.threshold {
			streak.confirmed = reason
		}
		streaks[nodeName] = streak
		if streak.confirmed != "" {
			reported[nodeName] = streak.confirmed
		}
	}
	d.streaks = streaks
	return reported
}
