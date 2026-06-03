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

package nodetracker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	clock "k8s.io/utils/clock/testing"
)

const testNodeName = "test-node"

var testStartTime = time.Date(2024, 1, 1, 1, 1, 1, 1, time.UTC)

func TestAddNode_NodeIsTracked(t *testing.T) {
	fakeClock := clock.NewFakeClock(testStartTime)
	tracker := New(fakeClock)

	tracker.AddNode(testNodeName, fakeClock.Now().Add(time.Millisecond))

	assert.True(t, tracker.IsTracked(testNodeName))
	assert.Len(t, tracker.nodes, 1)
}

func TestAddNode_NodeIsNotTracked(t *testing.T) {
	fakeClock := clock.NewFakeClock(testStartTime)
	tracker := New(fakeClock)

	tracker.AddNode(testNodeName, fakeClock.Now().Add(time.Millisecond))
	fakeClock.Sleep(2 * time.Millisecond)

	assert.False(t, tracker.IsTracked(testNodeName))
	assert.Len(t, tracker.nodes, 0)
}

func TestAddNode_OneNodeAddedTwice(t *testing.T) {
	fakeClock := clock.NewFakeClock(testStartTime)
	tracker := New(fakeClock)

	laterExpiration := fakeClock.Now().Add(10 * time.Minute)
	earlierExpiration := fakeClock.Now().Add(time.Minute)

	tracker.AddNode(testNodeName, laterExpiration)
	tracker.AddNode(testNodeName, earlierExpiration)

	fakeClock.Sleep(2 * time.Minute)

	assert.True(t, tracker.IsTracked(testNodeName))
}

func TestAddNode_NoNodes(t *testing.T) {
	tracker := New(clock.NewFakeClock(testStartTime))
	assert.False(t, tracker.IsTracked(testNodeName))
}

func TestDeleteNode_TrackedNode(t *testing.T) {
	fakeClock := clock.NewFakeClock(testStartTime)
	tracker := New(fakeClock)

	tracker.AddNode(testNodeName, fakeClock.Now().Add(time.Millisecond))
	assert.Len(t, tracker.nodes, 1)

	tracker.DeleteNode(testNodeName)

	assert.Len(t, tracker.nodes, 0)
}

func TestDeleteNode_UntrackedNode(t *testing.T) {
	fakeClock := clock.NewFakeClock(testStartTime)
	tracker := New(fakeClock)

	tracker.AddNode(testNodeName, fakeClock.Now().Add(time.Millisecond))
	fakeClock.Sleep(2 * time.Millisecond)
	assert.Len(t, tracker.nodes, 1)

	tracker.DeleteNode(testNodeName)

	assert.Len(t, tracker.nodes, 0)
}

func TestDeleteNode_UnexistingNode(t *testing.T) {
	fakeClock := clock.NewFakeClock(testStartTime)
	tracker := New(fakeClock)

	assert.Len(t, tracker.nodes, 0)

	tracker.DeleteNode(testNodeName)

	assert.Len(t, tracker.nodes, 0)
}

type nodeCountAndTime struct {
	count   int
	expTime time.Time
}

func TestCount(t *testing.T) {
	fakeClock := clock.NewFakeClock(testStartTime)
	earlyExpiration := fakeClock.Now().Add(3 * time.Minute)
	lateExpiration := fakeClock.Now().Add(10 * time.Minute)
	veryLateExpiration := fakeClock.Now().Add(15 * time.Minute)
	for _, tc := range []struct {
		name                 string
		expiredNodes         []string
		firstBackedOffNodes  []string
		secondBackedOffNodes []string
		wantCount            nodeCountAndTime
	}{
		{
			name:      "no nodes",
			wantCount: nodeCountAndTime{count: 0, expTime: time.Time{}},
		},
		{
			name:         "only expired nodes",
			expiredNodes: []string{"node1", "node2", "node3"},
			wantCount:    nodeCountAndTime{count: 0, expTime: time.Time{}},
		},
		{
			name:                "only backed off nodes",
			firstBackedOffNodes: []string{"node1", "node2", "node3"},
			wantCount:           nodeCountAndTime{count: 3, expTime: lateExpiration},
		},
		{
			name:                "mixed nodes",
			expiredNodes:        []string{"node1", "node2", "node3"},
			firstBackedOffNodes: []string{"node4", "node5", "node6"},
			wantCount:           nodeCountAndTime{count: 3, expTime: lateExpiration},
		},
		{
			name:                 "mixed nodes with very late expiration",
			expiredNodes:         []string{"node1", "node2", "node3"},
			firstBackedOffNodes:  []string{"node4", "node5", "node6"},
			secondBackedOffNodes: []string{"node7", "node8", "node9"},
			wantCount:            nodeCountAndTime{count: 6, expTime: lateExpiration},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeClock := clock.NewFakeClock(testStartTime)
			tracker := New(fakeClock)

			for _, n := range tc.expiredNodes {
				tracker.AddNode(n, earlyExpiration)
			}

			for _, n := range tc.firstBackedOffNodes {
				tracker.AddNode(n, lateExpiration)
			}

			for _, n := range tc.secondBackedOffNodes {
				tracker.AddNode(n, veryLateExpiration)
			}

			// Step so that some nodes are expired, some still in backoff.
			fakeClock.Step(5 * time.Minute)
			nodeCount, earliestExpiration := tracker.Count()
			assert.Equal(t, tc.wantCount.count, nodeCount)
			assert.Equal(t, tc.wantCount.expTime, earliestExpiration)
		})
	}
}
