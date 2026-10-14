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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLruShardSelector(t *testing.T) {
	selector := NewLruPodShardSelector(nil)
	podShards := generatePodShards("a", "b", "c")

	var selectedShards1 []*PodShard
	for i := 0; i < len(podShards); i++ {
		selectedShards1 = append(selectedShards1, selector.SelectPodShard(podShards))
	}
	assert.ElementsMatch(t, signatures(podShards...), signatures(selectedShards1...), "not all shards selected")

	// another round of selection should be the same (including order)
	var selectedShards2 []*PodShard
	for i := 0; i < len(podShards); i++ {
		selectedShards2 = append(selectedShards2, selector.SelectPodShard(podShards))
	}
	assert.EqualValues(t, signatures(selectedShards1...), signatures(selectedShards2...), "different order of shard selection in second round")

	// we add new shard. It should be picked last.
	podShards = generatePodShards("a", "b", "c", "d")
	var selectedShards3 []*PodShard
	for i := 0; i < len(podShards)-1; i++ {
		selectedShards3 = append(selectedShards3, selector.SelectPodShard(podShards))
	}
	assert.EqualValues(t, signatures(selectedShards1...), signatures(selectedShards3...), "different order of shard selection for first 3 queries on third round")
	assert.Equal(t, ShardSignature("Labels(key=d)"), selector.SelectPodShard(podShards).Signature(), "newly added shard was not picked last")
}

type recordingShardProcessor struct {
	shouldProcess bool
	calls         [][]ShardSignature
}

func newRecordingShardProcessor() *recordingShardProcessor {
	return &recordingShardProcessor{shouldProcess: true}
}

func (p *recordingShardProcessor) ShouldProcess() bool {
	return p.shouldProcess
}

func (p *recordingShardProcessor) ProcessIncomingShards(shards []*PodShard) {
	p.calls = append(p.calls, signatures(shards...))
}

func TestLruShardSelector_ProcessIncomingShards(t *testing.T) {
	testCases := []struct {
		name string
		// each entry is the set of shard labels offered to SelectPodShard in one loop
		rounds            [][]string
		nilProcessor      bool
		disableProcessing bool
		wantSelected      []ShardSignature
		wantIncoming      [][]ShardSignature
	}{
		{
			name:         "nil processor - selection works and nothing is notified",
			rounds:       [][]string{{"a", "b"}, {"a", "b"}},
			nilProcessor: true,
			wantSelected: []ShardSignature{"Labels(key=a)", "Labels(key=b)"},
		},
		{
			name:              "ShouldProcess returns false - selection works and processor is not called",
			rounds:            [][]string{{"a", "b"}, {"a", "b"}},
			disableProcessing: true,
			wantSelected:      []ShardSignature{"Labels(key=a)", "Labels(key=b)"},
		},
		{
			name:         "single shard - notified with that shard only",
			rounds:       [][]string{{"a"}},
			wantSelected: []ShardSignature{"Labels(key=a)"},
			wantIncoming: [][]ShardSignature{{"Labels(key=a)"}},
		},
		{
			name:         "fresh selector - incoming order follows input order, selected shard first",
			rounds:       [][]string{{"a", "b", "c"}},
			wantSelected: []ShardSignature{"Labels(key=a)"},
			wantIncoming: [][]ShardSignature{{"Labels(key=a)", "Labels(key=b)", "Labels(key=c)"}},
		},
		{
			name:         "consecutive loops - selected shard moves to the end of the incoming queue",
			rounds:       [][]string{{"a", "b", "c"}, {"a", "b", "c"}, {"a", "b", "c"}, {"a", "b", "c"}},
			wantSelected: []ShardSignature{"Labels(key=a)", "Labels(key=b)", "Labels(key=c)", "Labels(key=a)"},
			wantIncoming: [][]ShardSignature{
				{"Labels(key=a)", "Labels(key=b)", "Labels(key=c)"},
				{"Labels(key=b)", "Labels(key=c)", "Labels(key=a)"},
				{"Labels(key=c)", "Labels(key=a)", "Labels(key=b)"},
				{"Labels(key=a)", "Labels(key=b)", "Labels(key=c)"},
			},
		},
		{
			name:         "new shard appears - it is queued last and the least recently selected shard stays first",
			rounds:       [][]string{{"a", "b"}, {"a", "b"}, {"a", "b", "c"}},
			wantSelected: []ShardSignature{"Labels(key=a)", "Labels(key=b)", "Labels(key=a)"},
			wantIncoming: [][]ShardSignature{
				{"Labels(key=a)", "Labels(key=b)"},
				{"Labels(key=b)", "Labels(key=a)"},
				{"Labels(key=a)", "Labels(key=b)", "Labels(key=c)"},
			},
		},
		{
			name:         "shard disappears and reappears - keeps its old LRU position",
			rounds:       [][]string{{"a", "b", "c"}, {"b", "c"}, {"a", "b", "c"}},
			wantSelected: []ShardSignature{"Labels(key=a)", "Labels(key=b)", "Labels(key=c)"},
			wantIncoming: [][]ShardSignature{
				{"Labels(key=a)", "Labels(key=b)", "Labels(key=c)"},
				{"Labels(key=b)", "Labels(key=c)"},
				{"Labels(key=c)", "Labels(key=a)", "Labels(key=b)"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			processor := newRecordingShardProcessor()
			if tc.disableProcessing {
				processor.shouldProcess = false
			}
			var selector PodShardSelector
			if tc.nilProcessor {
				selector = NewLruPodShardSelector(nil)
			} else {
				selector = NewLruPodShardSelector(processor)
			}

			var selected []ShardSignature
			for _, round := range tc.rounds {
				shards := generatePodShards(round...)
				selected = append(selected, selector.SelectPodShard(shards).Signature())
				assert.Equal(t, signatures(generatePodShards(round...)...), signatures(shards...), "input slice must not be reordered")
			}

			assert.Equal(t, tc.wantSelected, selected)
			assert.Equal(t, tc.wantIncoming, processor.calls)
			for i, call := range processor.calls {
				assert.Equal(t, selected[i], call[0], "selected shard must be first in incoming shards of round %d", i)
			}
		})
	}
}

func generatePodShards(lables ...string) []*PodShard {
	var result []*PodShard
	for _, label := range lables {
		result = append(result, &PodShard{
			NodeGroupDescriptor: NodeGroupDescriptor{
				Labels: map[string]string{
					"key": label,
				},
			},
		})
	}
	return result
}

func signatures(podShards ...*PodShard) []ShardSignature {
	var result []ShardSignature
	for _, podShard := range podShards {
		result = append(result, podShard.Signature())
	}
	return result
}
