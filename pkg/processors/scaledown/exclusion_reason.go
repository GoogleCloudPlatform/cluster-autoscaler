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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/observability"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodes"
)

// ExclusionReasonProvider is an optional interface for scale-down node
// processors that can explain, in user-visible terms, why they exclude nodes
// from the scale-down candidate set.
//
// The chain only returns the nodes that survived it, so a consumer such as
// defrag migration reporting cannot otherwise distinguish a node that was never
// eligible for removal from one that is merely waiting its turn. Implementing
// this interface lets a processor state its reason next to the logic that
// produces it, rather than that knowledge living in a lookup table in an
// unrelated package, where a newly added processor would silently fall back to
// a generic reason.
//
// Implementing it is optional, and a processor should only do so when every
// node it removes is removed for the same single reason that a user can act on.
// Processors whose exclusions have no such explanation are reported as
// observability.MigrationBlocked.
type ExclusionReasonProvider interface {
	// ExclusionReason returns the reason under which nodes removed by this
	// processor are reported. It must return one of observability.AllReasons.
	ExclusionReason() observability.BlockReason
}

// exclusionReason returns the reason declared by a processor, or the empty
// reason if the processor does not declare one.
func exclusionReason(processor nodes.ScaleDownNodeProcessor) observability.BlockReason {
	provider, ok := processor.(ExclusionReasonProvider)
	if !ok {
		return ""
	}
	return provider.ExclusionReason()
}
