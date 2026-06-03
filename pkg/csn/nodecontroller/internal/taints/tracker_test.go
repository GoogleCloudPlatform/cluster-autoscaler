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

package taints

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/test"
	"k8s.io/utils/set"
)

type fakeQueue struct {
	counter int
	Ops     []ops.Operation
	// call count to error
	Err map[int]error
}

func (q *fakeQueue) Enqueue(op ops.Operation) error {
	q.counter++
	q.Ops = append(q.Ops, op)
	if err, ok := q.Err[q.counter]; ok {
		return err
	}
	return nil
}

func TestTaintTracker_HandleNodeEvent(t *testing.T) {
	pool1 := "pool-1"
	pool2 := "pool-2"

	tests := []struct {
		name   string
		events []state.NodeEvent
		// event count to error
		queueErr           map[int]error
		expectedTaintCount []int
		expectTracked      []bool
		expectEnqueue      []bool
	}{
		{
			name: "fresh_node_tracked",
			events: []state.NodeEvent{
				state.NodeAdded{Node: test.CreateNode("node-1", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
			},
			expectedTaintCount: []int{0},
			expectTracked:      []bool{true},
			expectEnqueue:      []bool{false},
		},
		{
			name: "filling_bucket_0_triggers_correction",
			events: []state.NodeEvent{
				state.NodeAdded{Node: test.CreateNode("node-1", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
				state.NodeAdded{Node: test.CreateNode("node-2", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
			},
			expectedTaintCount: []int{0, 1},
			expectTracked:      []bool{true, true},
			expectEnqueue:      []bool{false, true},
		},
		{
			name: "filling_bucket_1",
			events: []state.NodeEvent{
				state.NodeAdded{Node: test.CreateNode("node-1", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
				state.NodeAdded{Node: test.CreateNode("node-2", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1), SoftTaintOp(t, 1))},
				state.NodeAdded{Node: test.CreateNode("node-3", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
			},
			expectedTaintCount: []int{0, 1, 1},
			expectTracked:      []bool{true, true, true},
			expectEnqueue:      []bool{false, false, true},
		},
		{
			name: "overflow_to_bucket_2",
			events: []state.NodeEvent{
				state.NodeAdded{Node: test.CreateNode("node-1", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
				state.NodeAdded{Node: test.CreateNode("node-2", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1), SoftTaintOp(t, 1))},
				state.NodeAdded{Node: test.CreateNode("node-3", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1), SoftTaintOp(t, 1))},
				state.NodeAdded{Node: test.CreateNode("node-4", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
			},
			// B0 Full, B1 Full. Next available is 2.
			// Node-4 has 0. Correct to 2.
			expectedTaintCount: []int{0, 1, 1, 2},
			expectTracked:      []bool{true, true, true, true},
			expectEnqueue:      []bool{false, false, false, true},
		},
		{
			name: "respect_existing_taints",
			// Node comes with 5 taints. Bucket 5 empty. Should keep 5.
			events: []state.NodeEvent{
				state.NodeAdded{Node: test.CreateNode("node-1", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1), SoftTaintOp(t, 5))},
			},
			expectedTaintCount: []int{5},
			expectTracked:      []bool{true},
			expectEnqueue:      []bool{false},
		},
		{
			name: "non_csn_node_ignored",
			events: []state.NodeEvent{
				state.NodeAdded{Node: test.CreateNode("node-1", NodePoolOpt(pool1))},
			},
			expectedTaintCount: []int{0},
			expectTracked:      []bool{false},
			expectEnqueue:      []bool{false},
		},
		{
			name: "node_count_event_ignored",
			events: []state.NodeEvent{
				state.NodeCounts{Counts: map[csn.NodeState]int{csn.NodeStateSuspended: 5}},
			},
			expectedTaintCount: []int{0},
			expectTracked:      []bool{false},
			expectEnqueue:      []bool{false},
		},
		{
			name: "update_csn_to_non_csn_stops_tracking",
			events: []state.NodeEvent{
				state.NodeAdded{Node: test.CreateNode("node-1", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
				state.NodeUpdated{Node: test.CreateNode("node-1", NodePoolOpt(pool1))},
			},
			expectedTaintCount: []int{0, 0},
			expectTracked:      []bool{true, false},
			expectEnqueue:      []bool{false, false},
		},
		{
			name: "node_deletion_stops_tracking",
			events: []state.NodeEvent{
				state.NodeAdded{Node: test.CreateNode("node-1", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
				state.NodeDeleted{Node: test.CreateNode("node-1", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
			},
			expectedTaintCount: []int{0, 0},
			expectTracked:      []bool{true, false},
			expectEnqueue:      []bool{false, false},
		},
		{
			name: "reclaim_bucket_after_deletion",
			events: []state.NodeEvent{
				state.NodeAdded{Node: test.CreateNode("node-1", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
				state.NodeDeleted{Node: test.CreateNode("node-1", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
				state.NodeAdded{Node: test.CreateNode("node-1", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
			},
			expectedTaintCount: []int{0, 0, 0},
			expectTracked:      []bool{true, false, true},
			expectEnqueue:      []bool{false, false, false},
		},
		{
			name: "reclaim_larger_bucket_after_deletion",
			events: []state.NodeEvent{
				state.NodeAdded{Node: test.CreateNode("node-1", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
				state.NodeAdded{Node: test.CreateNode("node-2", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
				state.NodeAdded{Node: test.CreateNode("node-3", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
				state.NodeAdded{Node: test.CreateNode("node-4", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
				state.NodeDeleted{Node: test.CreateNode("node-1", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
				state.NodeDeleted{Node: test.CreateNode("node-4", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
				// should get `0` soft taints assigned
				state.NodeAdded{Node: test.CreateNode("node-5", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
				// should get 2, which was left empty after node-4 was deleted
				// soft taint count 1 is still taken up by node-2 and node-3
				state.NodeAdded{Node: test.CreateNode("node-6", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
			},
			expectedTaintCount: []int{0, 1, 1, 2, 0, 0, 0, 2},
			expectTracked:      []bool{true, true, true, true, false, false, true, true},
			expectEnqueue:      []bool{false, true, true, true, false, false, false, true},
		},
		{
			name: "multiple_pools_independent",
			events: []state.NodeEvent{
				state.NodeAdded{Node: test.CreateNode("node-1", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
				state.NodeAdded{Node: test.CreateNode("node-2", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
				state.NodeAdded{Node: test.CreateNode("node-3", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool2))},
			},
			expectedTaintCount: []int{0, 1, 0},
			expectTracked:      []bool{true, true, true},
			expectEnqueue:      []bool{false, true, false},
		},
		{
			name: "idempotency",
			events: []state.NodeEvent{
				state.NodeAdded{Node: test.CreateNode("node-1", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
				state.NodeAdded{Node: test.CreateNode("node-2", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
				state.NodeAdded{Node: test.CreateNode("node-2", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
			},
			// Re-adding same node shouldn't change anything or enqueue again.
			expectedTaintCount: []int{0, 1, 1},
			expectTracked:      []bool{true, true, true},
			expectEnqueue:      []bool{false, true, false},
		},
		{
			name: "queue_err_removes_node",
			events: []state.NodeEvent{
				state.NodeAdded{Node: test.CreateNode("node-1", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
				state.NodeAdded{Node: test.CreateNode("node-2", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
			},
			// error on first call, triggered by node-2.
			queueErr: map[int]error{1: errors.New("some-error")},
			// last one should be 1 without the error
			expectedTaintCount: []int{0, 0},
			expectTracked:      []bool{true, false},
			expectEnqueue:      []bool{false, true},
		},
		{
			name: "nil_node_does_not_impact_other_nodes",
			events: []state.NodeEvent{
				state.NodeAdded{Node: nil},
				state.NodeAdded{Node: test.CreateNode("node-1", test.StateOpt(csn.NodeStateChilling), NodePoolOpt(pool1))},
			},
			expectedTaintCount: []int{0, 0},
			expectTracked:      []bool{false, true},
			expectEnqueue:      []bool{false, false},
		},
		{
			name: "nodes_without_np_are_not_stored",
			events: []state.NodeEvent{
				state.NodeAdded{Node: test.CreateNode("node-1", test.StateOpt(csn.NodeStateChilling))},
				state.NodeAdded{Node: test.CreateNode("node-2", test.StateOpt(csn.NodeStateSuspended))},
			},
			expectedTaintCount: []int{0, 0},
			expectTracked:      []bool{false, false},
			expectEnqueue:      []bool{false, false},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.expectedTaintCount) != len(tc.expectTracked) {
				t.Fatalf("len(expectedTaintCount) != len(expectTracked")
			}

			if len(tc.expectTracked) != len(tc.expectEnqueue) {
				t.Fatalf("len(expectedTracked) != len(expectEnqueue")
			}

			q := &fakeQueue{Err: tc.queueErr}
			tracker := NewTracker(q)

			for idx, e := range tc.events {
				q.Ops = []ops.Operation{}
				tracker.HandleNodeEvent(e)
				name := ""
				if n, _ := state.GetNodeFromEvent(e); n != nil {
					name = n.Name
				}
				count, tracked := tracker.GetTaintCountToAssign(name)
				assert.Equal(t, tc.expectTracked[idx], tracked)
				assert.Equal(t, tc.expectedTaintCount[idx], count)

				if tc.expectEnqueue[idx] {
					assert.Len(t, q.Ops, 1, "expected op to be enqueued")
					assert.Contains(t, q.Ops, ops.Operation{
						Type:      ops.AssignSoftTaintOp,
						NodeNames: set.New(name),
					})
				} else {
					assert.Empty(t, q.Ops, "expected no enqueue")
				}
			}
		})
	}
}

func NodePoolOpt(nodePoolName string) func(*v1.Node) {
	return func(n *v1.Node) {
		n.Labels[labels.GkeNodePoolLabel] = nodePoolName
	}
}

func SoftTaintOp(t *testing.T, count int) func(node *v1.Node) {
	return func(n *v1.Node) {
		err := csn.ApplySoftTaints(n, count)
		assert.NoError(t, err)
	}
}
