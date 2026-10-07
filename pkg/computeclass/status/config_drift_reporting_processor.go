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
	"fmt"
	"reflect"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	computeclass "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/drift"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/observability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
)

// BlockReasonSource provides the reasons the defrag pipeline recorded during the
// current autoscaler loop.
//
// This is an interface rather than the defrag processor itself because
// pkg/defrag imports the ComputeClass packages, so depending on it directly here
// would be an import cycle.
type BlockReasonSource interface {
	// BlockReasons returns the reasons recorded during the last defrag pass, or
	// nil if that pass did not evaluate nodes.
	BlockReasons() *observability.Registry
}

// ConfigDriftReportingProcessor reports, in the status of each ComputeClass, how
// far the replacement of nodes that no longer match its configuration has got.
//
// It runs at the end of the autoscaler loop, in the same iteration as defrag and
// against the same cluster snapshot, which is what makes the block reasons
// defrag recorded meaningful: they describe the state the counters are derived
// from.
type ConfigDriftReportingProcessor struct {
	ccLister           drift.CRDLister
	updatesCh          chan UpdateMessage
	matcher            drift.Matcher
	blockReasons       BlockReasonSource
	experimentsManager experiments.Manager
	classifier         *ConfigDriftClassifier

	// ownedCrds are the ComputeClasses this autoscaler reported for in the
	// previous loop.
	//
	// The ComputeClass informer is cluster-wide, so in a sharded deployment
	// every shard sees every ComputeClass, including those whose nodes belong
	// entirely to another shard. Emitting for all of them would make the shards
	// overwrite each other's counters on every flush. Ownership is therefore
	// derived from the node groups this autoscaler actually manages, and
	// remembered across loops so that the shard that owned a ComputeClass is
	// also the one that clears it when its last node group disappears.
	ownedCrds sets.Set[CRDId]
}

// NewConfigDriftReportingProcessor returns a new ConfigDriftReportingProcessor.
func NewConfigDriftReportingProcessor(ccLister drift.CRDLister, updatesCh chan UpdateMessage, matcher drift.Matcher, blockReasons BlockReasonSource, experimentsManager experiments.Manager) *ConfigDriftReportingProcessor {
	return &ConfigDriftReportingProcessor{
		ccLister:           ccLister,
		updatesCh:          updatesCh,
		matcher:            matcher,
		blockReasons:       blockReasons,
		experimentsManager: experimentsManager,
		classifier:         NewConfigDriftClassifier(),
		ownedCrds:          sets.New[CRDId](),
	}
}

// Process classifies the cluster's nodes and sends one status update per owned
// ComputeClass.
//
// The full picture is sent every loop, rather than only what changed. The
// aggregator suppresses flushes that would not alter the object, and a patch
// that fails is never retried, so re-sending is what makes a dropped update
// heal on the next flush.
func (p *ConfigDriftReportingProcessor) Process(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, csr *clusterstate.ClusterStateRegistry, _ time.Time) error {
	if !computeclass.IsComputeClassConfigDriftStatusEnabled(p.experimentsManager) {
		return nil
	}

	// Deliberately the node lister and not the cluster snapshot: the snapshot no
	// longer contains nodes whose deletion this loop already started, which are
	// exactly the ones that have to be reported as migrating.
	nodes, err := autoscalingCtx.ListerRegistry.AllNodeLister().List()
	if err != nil {
		return fmt.Errorf("failed to list nodes: %w", err)
	}

	// Scoped to this loop: it memoizes drift per node group, and the
	// ComputeClass definitions it evaluates against can change between loops.
	driftCache := drift.NewCache(drift.NewEvaluator(p.ccLister, p.matcher))

	counts, evaluated := p.classifier.Classify(ConfigDriftSnapshot{
		Nodes:               nodes,
		Drift:               NewDriftResolver(ctx, autoscalingCtx.CloudProvider, driftCache),
		BlockReasons:        p.blockReasons.BlockReasons(),
		DeletionsInProgress: deletionsInProgress(autoscalingCtx),
	})
	if !evaluated {
		// Defrag did not evaluate nodes this loop, so there is nothing to say
		// about what is blocked. Reporting now would claim every blocked node
		// became free; leaving the status alone keeps the last real answer.
		return nil
	}

	owned := p.ownedCrdIds(ctx, autoscalingCtx, driftCache)
	measuredAt := metav1.Now()
	for crdId, crdCounts := range counts {
		// A ComputeClass with nodes is owned by definition, even if its node
		// group is momentarily missing from the cloud provider's listing.
		owned.Insert(crdId)
		info := toConfigDriftInfo(crdCounts, measuredAt)
		klog.V(4).Infof("Reporting config drift for ComputeClass %s: current %d, drifted %d, migrating %d, blocked %v",
			crdId.CRDName, info.CurrentNodes, info.DriftedNodes, info.MigratingNodes, info.BlockedNodes)
		TrySendUpdate(p.updatesCh, UpdateMessage{
			Id: crdId,
			Mutate: func(status crd.CRDStatus) {
				status.UpdateConfigDriftInfo(info)
			},
		})
	}

	// Clear the ComputeClasses this autoscaler is responsible for but has
	// nothing to report about, either because they never opted into config
	// drift migration, because they turned it off, or because their last node
	// is gone. Previously owned ones are included so that stale counters do not
	// outlive the node groups they described.
	//
	// Like the counters above, the resets are re-sent every loop for as long
	// as the ComputeClass stays owned without counts, which is what
	// CrdResourceReportingProcessor does for its fields too. A reset that the
	// channel drops is therefore retried next loop, and a ComputeClass only
	// stops being owned once its reset went through, or its counters could
	// stay stale until the autoscaler restarts.
	stillOwned := owned.Clone()
	for crdId := range p.ownedCrds.Union(owned) {
		if _, reported := counts[crdId]; reported {
			continue
		}
		sent := TrySendUpdate(p.updatesCh, UpdateMessage{
			Id: crdId,
			Mutate: func(status crd.CRDStatus) {
				status.ResetConfigDriftInfo()
			},
		})
		if !sent {
			stillOwned.Insert(crdId)
		}
	}
	p.ownedCrds = stillOwned

	return nil
}

// CleanUp implements the AutoscalingStatusProcessor interface.
func (p *ConfigDriftReportingProcessor) CleanUp() {}

// ownedCrdIds returns the ComputeClasses this autoscaler manages node groups
// for.
//
// ComputeClasses that did not enable config drift migration are included: the
// counters have to be cleared when the feature is turned off, and that is only
// safe to do from the autoscaler that owns the node groups.
func (p *ConfigDriftReportingProcessor) ownedCrdIds(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, driftCache *drift.Cache) sets.Set[CRDId] {
	owned := sets.New[CRDId]()
	for _, nodeGroup := range autoscalingCtx.CloudProvider.NodeGroups(ctx) {
		result := driftCache.EvaluateNodeGroup(nodeGroup)
		if result.CRD == nil {
			continue
		}
		owned.Insert(CRDId{CRDLabel: result.CRD.Label(), CRDName: result.CRDName})
	}
	return owned
}

// deletionsInProgress returns the nodes the scale-down actuator is deleting.
func deletionsInProgress(autoscalingCtx *ca_context.AutoscalingContext) sets.Set[string] {
	if isNil(autoscalingCtx.ScaleDownActuator) {
		return nil
	}
	status := autoscalingCtx.ScaleDownActuator.CheckStatus()
	if isNil(status) {
		return nil
	}
	empty, drained := status.DeletionsInProgress()
	return sets.New(empty...).Insert(drained...)
}

// isNil reports whether the interface is nil or holds a nil value.
//
// An interface holding a typed nil is not itself nil, which a plain comparison
// would miss. reflect.Value.IsNil answers that, but panics on kinds that cannot
// be nil, so an implementation that happened to be a struct rather than a
// pointer would turn this guard into the crash it exists to prevent.
func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

// toConfigDriftInfo converts the classifier's counts into what the status
// reports.
func toConfigDriftInfo(counts ConfigDriftCounts, measuredAt metav1.Time) crd.ConfigDriftInfo {
	info := crd.ConfigDriftInfo{
		CurrentNodes:   counts.CurrentNodes,
		DriftedNodes:   counts.DriftedNodes,
		MigratingNodes: counts.MigratingNodes,
		MeasuredAt:     measuredAt,
	}
	// Sorted, because the reported list is merged per reason by the API server
	// and an order that churns would rewrite the object on every flush.
	for _, blocked := range counts.SortedBlockedNodes() {
		info.BlockedNodes = append(info.BlockedNodes, crd.BlockedNodesInfo{
			Reason: string(blocked.Reason),
			Count:  blocked.Count,
		})
	}
	return info
}
