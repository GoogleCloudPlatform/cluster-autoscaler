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
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/observability"
	"k8s.io/utils/clock"
	"sigs.k8s.io/cluster-autoscaler/pkg/context"
)

// backoffEntry records when a node's backoff expires and why it was started.
//
// The cause is retained because backoff outlives the pass that created it. A
// candidate abandoned for lack of replacement capacity is backed off
// immediately, so without the cause every later pass would report those nodes
// as a generic recent failure and hide the reason the user actually needs.
type backoffEntry struct {
	until time.Time
	cause observability.BlockReason
}

// defragBackoff is responsible for tracking backoff of defrag candidate nodes
// per each defrag plugin.
type defragBackoff struct {
	// Maps [plugin -> node name -> backoff entry]
	backedOffNodes map[defrag.Plugin]map[string]backoffEntry
	clock          clock.PassiveClock
}

// newDefragBackoff returns a new instance of defragBackoff
func newDefragBackoff() *defragBackoff {
	return &defragBackoff{
		backedOffNodes: make(map[defrag.Plugin]map[string]backoffEntry),
		clock:          clock.RealClock{},
	}
}

// backoff initiates backoff for all candidate nodes for the candidate plugin.
//
// The cause is reported for as long as the backoff lasts.
func (b *defragBackoff) backoff(ctx *context.AutoscalingContext, candidate *defrag.Candidate, cause observability.BlockReason) {
	pluginBackoff := candidate.Plugin.BackoffDuration(ctx, candidate)
	if _, found := b.backedOffNodes[candidate.Plugin]; !found {
		b.backedOffNodes[candidate.Plugin] = make(map[string]backoffEntry)
	}
	entry := backoffEntry{
		until: b.clock.Now().Add(pluginBackoff),
		cause: cause,
	}
	for _, node := range candidate.Nodes {
		b.backedOffNodes[candidate.Plugin][node] = entry
	}
}

// splitNodesBasedOnBackoff splits the nodes associated with the given plugin
// into two slices: the ones that are not backed off and the ones that are
func (b *defragBackoff) splitNodesBasedOnBackoff(plugin defrag.Plugin, nodes []string) (availableNodes []string, backedOffNodes []string) {
	timeNow := b.clock.Now()
	for _, node := range nodes {
		if entry, found := b.backedOffNodes[plugin][node]; !found || timeNow.After(entry.until) {
			availableNodes = append(availableNodes, node)
		} else {
			backedOffNodes = append(backedOffNodes, node)
		}
	}
	return availableNodes, backedOffNodes
}

// cause returns why a node is currently backed off for the given plugin.
//
// It falls back to observability.RecentMigrationFailure, which is true of any
// backed-off node, so that a caller never has to handle an empty reason.
func (b *defragBackoff) cause(plugin defrag.Plugin, node string) observability.BlockReason {
	entry, found := b.backedOffNodes[plugin][node]
	if !found || entry.cause == "" {
		return observability.RecentMigrationFailure
	}
	return entry.cause
}

// cleanBackoffInfo cleans up obsolete backoff info
func (b *defragBackoff) cleanBackoffInfo() {
	timeNow := b.clock.Now()
	backedOffNodes := make(map[defrag.Plugin]map[string]backoffEntry)
	for plugin, nodes := range b.backedOffNodes {
		backedOffNodes[plugin] = make(map[string]backoffEntry)
		for node, entry := range nodes {
			if timeNow.Before(entry.until) {
				backedOffNodes[plugin][node] = entry
			}
		}
	}
	b.backedOffNodes = backedOffNodes
}
