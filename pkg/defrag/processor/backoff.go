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

	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/utils/clock"
)

// defragBackoff is responsible for tracking backoff of defrag candidate nodes
// per each defrag plugin.
type defragBackoff struct {
	// Maps [plugin -> node name -> backoff time]
	backedOffNodes map[defrag.Plugin]map[string]time.Time
	clock          clock.PassiveClock
}

// newDefragBackoff returns a new instance of defragBackoff
func newDefragBackoff() *defragBackoff {
	return &defragBackoff{
		backedOffNodes: make(map[defrag.Plugin]map[string]time.Time),
		clock:          clock.RealClock{},
	}
}

// backoff initiates backoff for all candidate nodes for the candidate plugin
func (b *defragBackoff) backoff(ctx *context.AutoscalingContext, candidate *defrag.Candidate) {
	pluginBackoff := candidate.Plugin.BackoffDuration(ctx, candidate)
	if _, found := b.backedOffNodes[candidate.Plugin]; !found {
		b.backedOffNodes[candidate.Plugin] = make(map[string]time.Time)
	}
	backoffTime := b.clock.Now().Add(pluginBackoff)
	for _, node := range candidate.Nodes {
		b.backedOffNodes[candidate.Plugin][node] = backoffTime
	}
}

// splitNodesBasedOnBackoff splits the nodes associated with the given plugin
// into two slices: the ones that are not backed off and the ones that are
func (b *defragBackoff) splitNodesBasedOnBackoff(plugin defrag.Plugin, nodes []string) (availableNodes []string, backedOffNodes []string) {
	timeNow := b.clock.Now()
	for _, node := range nodes {
		if stamp, found := b.backedOffNodes[plugin][node]; !found || timeNow.After(stamp) {
			availableNodes = append(availableNodes, node)
		} else {
			backedOffNodes = append(backedOffNodes, node)
		}
	}
	return availableNodes, backedOffNodes
}

// cleanBackoffInfo cleans up obsolete backoff info
func (b *defragBackoff) cleanBackoffInfo() {
	timeNow := b.clock.Now()
	backedOffNodes := make(map[defrag.Plugin]map[string]time.Time)
	for plugin, nodes := range b.backedOffNodes {
		backedOffNodes[plugin] = make(map[string]time.Time)
		for node, stamp := range nodes {
			if timeNow.Before(stamp) {
				backedOffNodes[plugin][node] = stamp
			}
		}
	}
	b.backedOffNodes = backedOffNodes
}
