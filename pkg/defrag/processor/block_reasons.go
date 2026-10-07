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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/observability"
	scaledown_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaledown"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/drain"
)

// vendoredScaleDownProcessorReasons maps an upstream scale-down node processor
// to the reason its exclusions should be reported under.
//
// Processors owned by this repository declare their own reason by implementing
// scaledown.ExclusionReasonProvider, which keeps the explanation next to the
// logic that produces it. That is not possible for processors vendored from
// upstream cluster-autoscaler, so those few are listed here instead.
//
// Keys are built from the processor type with the same function that
// ScaleDownCandidateStages.ProcessorFor uses, rather than written out, so that
// renaming or moving one of these types upstream stops the build instead of
// quietly downgrading every node it excludes to MigrationBlocked.
var vendoredScaleDownProcessorReasons = map[string]observability.BlockReason{
	// Drops nodes with no node group, or whose node group is already at its
	// minimum size.
	scaledown_processors.ProcessorName(&nodes.PreFilteringScaleDownNodeProcessor{}): observability.MinCapacityReached,
}

// The remaining processors in the chain deliberately declare no reason, so
// their exclusions are reported as observability.MigrationBlocked. They are
// listed here so that the fallback is a reviewed decision rather than an
// oversight:
//
//   - provisioningrequests/processors.ProvisioningRequestScaleDownNodeProcessor
//     protects nodes booked by a ProvisioningRequest. Those are
//     queued-provisioning node pools, which a ComputeClass does not manage, so
//     this should not be reachable from defrag in practice.
//   - ekvms/processor.ScaleDownNodeProcessor classifies EK VMs for resize
//     rather than removal, and an exclusion there can mean any of several
//     unrelated resize states.
//   - ekvms/lookaheadbuffer/processor.ScaleDownNodeProcessor has a no-op
//     GetScaleDownCandidates; it only filters pod destinations.
//   - scaledowncandidates.ScaleDownCandidatesDelayProcessor delays candidates
//     after a recent scale event, which is transient and not user-actionable.
//   - scaledowncandidates.ScaleDownCandidatesSortingProcessor only sorts
//     candidates, it never removes any.

// reasonForExcludedScaleDownCandidate explains why a node is missing from the
// scale-down candidate set.
//
// It prefers the reason the responsible processor declared for itself, falls
// back to the table of vendored processors, and finally to
// observability.MigrationBlocked. The last fallback keeps every excluded node
// accounted for rather than silently dropping it from the reported totals.
func reasonForExcludedScaleDownCandidate(stages *scaledown_processors.ScaleDownCandidateStages, nodeName string) observability.BlockReason {
	if reason, ok := stages.ReasonFor(nodeName); ok {
		return reason
	}
	name, ok := stages.ProcessorFor(nodeName)
	if !ok {
		return observability.MigrationBlocked
	}
	if reason, ok := vendoredScaleDownProcessorReasons[name]; ok {
		return reason
	}
	return observability.MigrationBlocked
}

// blockReasonForRemovedCandidate maps the reason a defrag candidate was
// abandoned to the reason its nodes should be reported under.
//
// This is where the boundary the ComputeClass API documents between
// migratingNodes and ReplacementUnavailable is actually drawn: a node keeps
// counting as migrating while the attempt to obtain replacement capacity is
// still in flight, and only becomes blocked once defrag gives that attempt up.
func blockReasonForRemovedCandidate(reason removeCandidateReason) observability.BlockReason {
	switch reason {
	case noScaleUpOptions:
		// No configuration allowed by the ComputeClass could supply the
		// replacement, so the migration cannot proceed at all.
		return observability.ReplacementUnavailable
	case scaleUpTimeoutExceeded:
		// The replacement was requested and never arrived. From the user's
		// point of view this is the same problem: capacity was asked for and
		// could not be obtained.
		return observability.ReplacementUnavailable
	case scaleDownTimeoutExceeded:
		// Replacement capacity was obtained; it is the drain or deletion that
		// stalled, so this is a failed attempt rather than a capacity problem.
		return observability.RecentMigrationFailure
	default:
		// This includes noValidNodes, which is only returned for a candidate
		// with no nodes left to record anything for: every node was filtered
		// out earlier and already carries the specific reason that removed it.
		return observability.RecentMigrationFailure
	}
}

// reasonForBlockingPod maps a drain blocker to a reported reason.
//
// Only PDB exhaustion is called out separately: it is transient and resolves
// without user action, whereas every other blocker describes a pod that needs
// an owner to intervene.
func reasonForBlockingPod(reason drain.BlockingPodReason) observability.BlockReason {
	if reason == drain.NotEnoughPdb {
		return observability.PodDisruptionBudget
	}
	return observability.BlockingPods
}

// recordAtomicGroupBlocked marks nodes as held back by their atomic group.
//
// It is called with the nodes that survived a filter which removed some of
// their peers. The removed peers keep the specific reason that removed them,
// which is what a user needs to act on; the survivors are only stuck because
// an atomic group must be migrated in full or not at all.
func recordAtomicGroupBlocked(filter *defragNodeFilter, nodeNames []string) {
	for _, nodeName := range nodeNames {
		filter.BlockReasons().Record(nodeName, observability.AtomicGroupBlocked)
	}
}
