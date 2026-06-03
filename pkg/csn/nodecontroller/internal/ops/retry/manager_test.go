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

package retry

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	testingclock "k8s.io/utils/clock/testing"
	"k8s.io/utils/set"
)

func TestBackoffManager_AddFailedNodes(t *testing.T) {
	config := Config{MaxRetries: 2, InitialDelay: time.Second, MaxDelay: time.Second}
	op := ops.Operation{Type: ops.SuspendOp}

	tests := []struct {
		name            string
		nodes           set.Set[string]
		setup           func(*BackoffManager)
		expectBackedOff set.Set[string]
		expectFailed    set.Set[string]
	}{
		{
			name:            "nodes_backed_off_at_first_failure",
			nodes:           set.New("node1", "node2"),
			expectBackedOff: set.New("node1", "node2"),
			expectFailed:    set.New[string](),
		},
		{
			name:  "nodes_failed_permanently_at_max_retries",
			nodes: set.New("node1", "node2"),
			setup: func(m *BackoffManager) {
				// Fail the nodes MaxRetries (2) times beforehand
				m.AddFailedNodes(op, set.New("node1", "node2"))
				m.AddFailedNodes(op, set.New("node1", "node2"))
			},
			expectBackedOff: set.New[string](),
			expectFailed:    set.New("node1", "node2"),
		},
		{
			name:  "partial_failures",
			nodes: set.New("node1", "node2"),
			setup: func(m *BackoffManager) {
				// node1 has failed 2 times already (at max retries)
				// node2 has failed 1 time already (below max retries)
				m.AddFailedNodes(op, set.New("node1", "node2"))
				m.AddFailedNodes(op, set.New("node1"))
			},
			// The 3rd failure for node1 should cause it to fail permanently
			// The 2nd failure for node2 should just back it off again
			expectBackedOff: set.New("node2"),
			expectFailed:    set.New("node1"),
		},
		{
			name:  "different_op_types_kept_separate",
			nodes: set.New("node1", "node2"),
			setup: func(m *BackoffManager) {
				// node1 has failed 2 times already for consume (at max retries)
				m.AddFailedNodes(ops.Operation{Type: ops.ConsumeOp}, set.New("node1"))
				m.AddFailedNodes(ops.Operation{Type: ops.ConsumeOp}, set.New("node1"))
				m.AddFailedNodes(op, set.New("node2"))
				m.AddFailedNodes(op, set.New("node2"))
			},
			// The 3rd failure for node1 should be fine
			// because it's for a different operation type.
			expectBackedOff: set.New("node1"),
			expectFailed:    set.New("node2"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeClock := testingclock.NewFakeClock(time.Now())
			m := NewBackoffManager(fakeClock, config,
				func(ops.Operation) error { return nil },
			)

			if tc.setup != nil {
				tc.setup(m)
			}

			result := m.AddFailedNodes(op, tc.nodes)

			assert.Equal(t, tc.expectBackedOff, result.BackedOffNodes)
			assert.Equal(t, tc.expectFailed, result.FailedNodes)
		})
	}
}

func TestBackoffManager_RetryCount(t *testing.T) {
	config := Config{MaxRetries: 1, InitialDelay: time.Second, MaxDelay: time.Second}
	fakeClock := testingclock.NewFakeClock(time.Now())
	m := NewBackoffManager(fakeClock, config,
		func(ops.Operation) error { return nil },
	)

	op := ops.Operation{Type: ops.SuspendOp}
	nodes := set.New("node1", "node2", "node3")

	// Fail once
	m.AddFailedNodes(op, nodes)
	assert.Equal(t, map[Key]int{
		{OpType: ops.SuspendOp, NodeName: "node1"}: 1,
		{OpType: ops.SuspendOp, NodeName: "node2"}: 1,
		{OpType: ops.SuspendOp, NodeName: "node3"}: 1,
	}, m.RetryCounts())

	// Clear the count for node1 and node 2
	m.ClearRetryCount(op.Type, set.New("node1", "node2"))
	assert.Equal(t, map[Key]int{
		{OpType: ops.SuspendOp, NodeName: "node3"}: 1,
	}, m.RetryCounts())

	// Fail again. Since count was cleared, it should be treated as the 1st failure,
	// not the 2nd (which would exceed MaxRetries=1 and fail permanently).
	result := m.AddFailedNodes(op, nodes)

	assert.Equal(t, set.New("node1", "node2"), result.BackedOffNodes)
	assert.Equal(t, set.New("node3"), result.FailedNodes)
}

func TestBackoffManager_Run(t *testing.T) {
	config := Config{
		MaxRetries:   5,
		InitialDelay: 10 * time.Second,
		MaxDelay:     1 * time.Minute,
	}

	type step struct {
		name           string
		action         func(m *BackoffManager)
		nodesToFail    set.Set[string]
		expectedEvents []Event
		expectEnqueued []string // node names expected to be enqueued in this step
		// time to advance after step completion
		advance time.Duration
	}

	tests := []struct {
		name string
		// NodeName->error
		queueErrs map[string]error
		steps     []step
	}{
		{
			name: "single_node_retry",
			steps: []step{
				{
					name:        "fail_node1",
					nodesToFail: set.New("node1"),
					// Run() execution and first loop iteration triggered
					// by adding a node.
					expectedEvents: []Event{{RetriedOps: 0}, {RetriedOps: 0}},
					advance:        config.InitialDelay,
				},
				{
					name:           "wait_for_retry",
					expectEnqueued: []string{"node1"},
					expectedEvents: []Event{{RetriedOps: 1}},
				},
			},
		},
		{
			name: "single_node_retry_after_some_time",
			steps: []step{
				{
					name:           "wait_for_some_time",
					expectedEvents: []Event{{RetriedOps: 0}},
					advance:        2 * config.MaxDelay,
				},
				{
					name:           "wait_for_timer_loop",
					expectedEvents: []Event{{RetriedOps: 0}},
				},
				{
					name:        "fail_node1",
					nodesToFail: set.New("node1"),
					// loop iteration after node addition
					expectedEvents: []Event{{RetriedOps: 0}},
					advance:        config.InitialDelay,
				},
				{
					name:           "wait_for_retry",
					expectEnqueued: []string{"node1"},
					expectedEvents: []Event{{RetriedOps: 1}},
				},
			},
		},
		{
			name: "timer_refresh_on_earlier_op",
			steps: []step{
				{
					name:           "fail_node1_once",
					nodesToFail:    set.New("node1"), // retry 1: [5s, 10s)
					expectedEvents: []Event{{RetriedOps: 0}, {RetriedOps: 0}},
				},
				{
					name:           "fail_node1_second_time_to_get_longer_delay",
					nodesToFail:    set.New("node1"), // retry 2: [10s, 20s)
					expectedEvents: []Event{{RetriedOps: 0}},
				},
				{
					name: "fail_node2_once_for_shorter_delay",
					// node2 retry 1 will be [5s, 10s).
					// Even if node1's retry 1 pops, node2's retry 1 might be earlier
					// than node1's retry 2.
					nodesToFail:    set.New("node2"),
					expectedEvents: []Event{{RetriedOps: 0}},
					advance:        config.InitialDelay * 3,
				},
				{
					name:           "wait_for_all",
					expectEnqueued: []string{"node1", "node2", "node1"}, // retry 1, retry 1, retry 2 (order depends on jitter but all should pop)
					expectedEvents: []Event{{RetriedOps: 3}},
				},
			},
		},
		{
			name:      "enqueue_error_prevents_retry",
			queueErrs: map[string]error{"node1": errors.New("some-error")},
			steps: []step{
				{
					name:        "fail_nodes",
					nodesToFail: set.New("node1", "node2"),
					// Run() execution and first loop iteration triggered
					// by adding nodes.
					expectedEvents: []Event{{RetriedOps: 0}, {RetriedOps: 0}},
					advance:        config.InitialDelay,
				},
				{
					name:           "wait_for_retry",
					expectEnqueued: []string{"node1", "node2"},
					expectedEvents: []Event{{RetriedOps: 1}},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defaultOp := ops.Operation{
				MIG: gce.GceRef{
					Project: "some-project",
					Zone:    "some-zone",
					Name:    "some-mig",
				},
				Type: ops.SuspendOp,
			}
			fakeClock := testingclock.NewFakeClock(time.Now())
			enqueued := make(chan ops.Operation, 10)
			events := make(chan Event)
			m := NewBackoffManager(
				fakeClock,
				config,
				func(op ops.Operation) error {
					enqueued <- op
					for nodeName := range op.NodeNames {
						if err := tc.queueErrs[nodeName]; err != nil {
							return err
						}
					}
					return nil
				},
				func(event Event) {
					events <- event
				},
			)
			go m.Run(t.Context())

			for _, s := range tc.steps {
				if len(s.nodesToFail) != 0 {
					m.AddFailedNodes(defaultOp, s.nodesToFail.Clone())
				}
				var gotEnqueued []string
				var gotEvents []Event
				// Drain what's currently available in the channel
				for len(s.expectEnqueued) != len(gotEnqueued) || len(s.expectedEvents) != len(gotEvents) {
					select {
					case e := <-events:
						gotEvents = append(gotEvents, e)
					case op := <-enqueued:
						gotEnqueued = append(gotEnqueued, op.NodeNames.UnsortedList()...)
						assert.Equal(t, defaultOp.MIG, op.MIG)
						assert.Equal(t, defaultOp.Type, op.Type)
					}
				}
				assert.ElementsMatch(t, s.expectEnqueued, gotEnqueued, "Step %q: unexpected enqueued nodes", s.name)
				assert.Equal(t, s.expectedEvents, gotEvents, "Step %q: unexpected events", s.name)
				if s.advance > 0 {
					fakeClock.Step(s.advance)
				}
			}
		})
	}
}
