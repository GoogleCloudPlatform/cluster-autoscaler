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

package podsharding

import (
	"cmp"
	"slices"
)

// ShardProcessor is notified by LruPodShardSelector about the order in which shards are going to be processed.
type ShardProcessor interface {
	// ShouldProcess returns whether ProcessIncomingShards should be invoked during shard selection.
	ShouldProcess() bool
	// ProcessIncomingShards is called once per selection with all shards ordered from the least recently selected
	// to the most recently selected one, i.e. shards[0] is the shard selected for the current loop and shards[1:]
	// are the shards that will be selected in the following loops (assuming the set of shards does not change).
	ProcessIncomingShards(shards []*PodShard)
}

// LruPodShardSelector is a PodShardSelector which picks shards in least-recently picked order
type LruPodShardSelector struct {
	lruMap         map[ShardSignature]uint64
	useCounter     uint64
	shardProcessor ShardProcessor
}

// NewLruPodShardSelector creates PodShardSelector which picks shards in least-recently picked order.
// shardProcessor may be nil, in which case no notifications about the shard selection order are sent.
func NewLruPodShardSelector(shardProcessor ShardProcessor) PodShardSelector {
	return &LruPodShardSelector{
		lruMap:         make(map[ShardSignature]uint64),
		shardProcessor: shardProcessor,
	}
}

// SelectPodShard selects shard with least recently used shard signature
func (selector *LruPodShardSelector) SelectPodShard(shards []*PodShard) *PodShard {
	if len(shards) == 0 {
		return nil
	}

	var selectedShard *PodShard

	// fill in entries for unknown shards; those are added to the end of the queue
	for _, shard := range shards {
		signature := shard.Signature()
		if _, found := selector.lruMap[signature]; !found {
			selector.useCounter++
			selector.lruMap[signature] = selector.useCounter
		}
	}

	// select oldest entry
	selector.useCounter++
	selectedShardTime := selector.useCounter
	for _, shard := range shards {
		shardTime := selector.lruMap[shard.Signature()]
		if shardTime < selectedShardTime {
			selectedShard = shard
			selectedShardTime = shardTime
		}
	}
	if selector.shardProcessor != nil && selector.shardProcessor.ShouldProcess() {
		selector.shardProcessor.ProcessIncomingShards(selector.incomingShards(shards))
	}
	selector.lruMap[selectedShard.Signature()] = selector.useCounter
	return selectedShard
}

// incomingShards returns a copy of shards ordered from the least recently selected to the most recently selected one.
// TODO(b/580929011): Avoid repeated uncached Signature() allocations inside the sort comparator.
func (selector *LruPodShardSelector) incomingShards(shards []*PodShard) []*PodShard {
	incoming := slices.Clone(shards)
	slices.SortStableFunc(incoming, func(a, b *PodShard) int {
		return cmp.Compare(selector.lruMap[a.Signature()], selector.lruMap[b.Signature()])
	})
	return incoming
}
