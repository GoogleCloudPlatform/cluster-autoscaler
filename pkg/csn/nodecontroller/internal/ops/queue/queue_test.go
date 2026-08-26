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

package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state"
	statetest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state/test"
	"k8s.io/utils/set"
)

func TestEnqueue(t *testing.T) {
	mig1 := gce.GceRef{
		Project: "p1",
		Zone:    "z1",
		Name:    "m1",
	}

	tests := []struct {
		name                     string
		queueSize                int
		initialOps               []ops.Operation
		opToEnqueue              ops.Operation
		opts                     []state.PendingOperationOpt
		pendingUpdateErr         map[string]error
		expectError              bool
		expectEmptyQueue         bool
		expectPendingUpdateCalls []statetest.SetPendingOperationCall
	}{
		{
			name:      "enqueue_single_item",
			queueSize: 10,
			opToEnqueue: ops.Operation{
				MIG:       mig1,
				Type:      ops.SuspendOp,
				NodeNames: set.New("n1"),
			},
			expectError: false,
			expectPendingUpdateCalls: []statetest.SetPendingOperationCall{
				{Op: ops.SuspendOp, Pending: true, NodeNames: set.New("n1")},
			},
		},
		{
			name:      "enqueue_empty_set_is_noop",
			queueSize: 10,
			opToEnqueue: ops.Operation{
				MIG:       mig1,
				Type:      ops.SuspendOp,
				NodeNames: set.New[string](),
			},
			expectError:      false,
			expectEmptyQueue: true,
		},
		{
			name:      "enqueue_to_full_queue_fails",
			queueSize: 1,
			initialOps: []ops.Operation{
				{
					MIG:       mig1,
					Type:      ops.ConsumeOp,
					NodeNames: set.New("n2"),
				},
			},
			opToEnqueue: ops.Operation{
				MIG:       mig1,
				Type:      ops.SuspendOp,
				NodeNames: set.New("n1"),
			},
			expectError: true, // select default case will trigger for unbuffered chan if no reader
			expectPendingUpdateCalls: []statetest.SetPendingOperationCall{
				{Op: ops.ConsumeOp, Pending: true, NodeNames: set.New("n2")},
				{Op: ops.SuspendOp, Pending: true, NodeNames: set.New("n1")},
				// n1 rolled back since it wasn't successfully added to the queue
				{Op: ops.SuspendOp, Pending: false, NodeNames: set.New("n1")},
			},
		},
		{
			name:      "enqueue_to_full_queue_succeeds_if_op_exists",
			queueSize: 1,
			initialOps: []ops.Operation{
				{
					MIG:       mig1,
					Type:      ops.SuspendOp,
					NodeNames: set.New("n1"),
				},
			},
			opToEnqueue: ops.Operation{
				MIG:       mig1,
				Type:      ops.SuspendOp,
				NodeNames: set.New("n2"),
			},
			expectError: false,
			expectPendingUpdateCalls: []statetest.SetPendingOperationCall{
				{Op: ops.SuspendOp, Pending: true, NodeNames: set.New("n1")},
				{Op: ops.SuspendOp, Pending: true, NodeNames: set.New("n2")},
			},
		},
		{
			name:      "enqueue_does_not_increase_queue_length_when_pending_err",
			queueSize: 10,
			opToEnqueue: ops.Operation{
				MIG:       mig1,
				Type:      ops.SuspendOp,
				NodeNames: set.New("n2"),
			},
			pendingUpdateErr: map[string]error{"n2": errors.New("some error")},
			expectError:      false,
			expectPendingUpdateCalls: []statetest.SetPendingOperationCall{
				{Op: ops.SuspendOp, Pending: true, NodeNames: set.New("n2")},
			},
			expectEmptyQueue: true,
		},
		{
			name:      "exclusive_opt_is_passed",
			queueSize: 10,
			opToEnqueue: ops.Operation{
				MIG:       mig1,
				Type:      ops.SuspendOp,
				NodeNames: set.New("n1"),
			},
			opts:             []state.PendingOperationOpt{state.ExclusiveOp},
			pendingUpdateErr: map[string]error{"n1": errors.New("exclusive op error")},
			expectError:      false,
			expectPendingUpdateCalls: []statetest.SetPendingOperationCall{
				{
					Op:        ops.SuspendOp,
					Pending:   true,
					NodeNames: set.New("n1"),
					Opts:      []state.PendingOperationOpt{state.ExclusiveOp},
				},
			},
			expectEmptyQueue: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sm := &statetest.MockStateManager{SetPendingErr: tc.pendingUpdateErr}
			q := NewWorkQueue(tc.queueSize, sm)

			for _, op := range tc.initialOps {
				err := q.Enqueue(op)
				assert.NoError(t, err)
			}

			var err error
			if len(tc.opts) > 0 {
				err = q.EnqueueWithOpts(tc.opToEnqueue, tc.opts...)
			} else {
				err = q.Enqueue(tc.opToEnqueue)
			}

			if tc.expectError {
				assert.ErrorIs(t, err, ErrQueueFull)
			} else {
				assert.NoError(t, err)
			}
			if tc.expectEmptyQueue {
				assert.Equal(t, 0, q.Length())
			} else {
				assert.Equal(t, 1, q.Length())
			}

			assert.Equal(t, tc.expectPendingUpdateCalls, sm.GetPendingOperationUpdateCalls())
		})
	}
}

func TestDequeue(t *testing.T) {
	mig1 := gce.GceRef{Project: "p1", Zone: "z1", Name: "m1"}
	mig2 := gce.GceRef{Project: "p2", Zone: "z2", Name: "m2"}

	tests := []struct {
		name             string
		ops              []ops.Operation
		expectOps        []ops.Operation
		queueSize        int
		pendingUpdateErr map[string]error
	}{
		{
			name:      "dequeue_single_item",
			queueSize: 10,
			ops: []ops.Operation{
				{
					MIG:       mig1,
					Type:      ops.SuspendOp,
					NodeNames: set.New("n1"),
				},
			},
			expectOps: []ops.Operation{
				{
					MIG:       mig1,
					Type:      ops.SuspendOp,
					NodeNames: set.New("n1"),
				},
			},
		},
		{
			name:      "batching_merges_nodes",
			queueSize: 10,
			ops: []ops.Operation{
				{
					MIG:       mig1,
					Type:      ops.SuspendOp,
					NodeNames: set.New("n1"),
				},
				{
					MIG:       mig1,
					Type:      ops.SuspendOp,
					NodeNames: set.New("n2"),
				},
			},
			expectOps: []ops.Operation{
				{
					MIG:       mig1,
					Type:      ops.SuspendOp,
					NodeNames: set.New("n1", "n2"),
				},
			},
		},
		{
			name:             "batching_prevented_by_set_pending_err",
			queueSize:        10,
			pendingUpdateErr: map[string]error{"n2": errors.New("some error")},
			ops: []ops.Operation{
				{
					MIG:       mig1,
					Type:      ops.SuspendOp,
					NodeNames: set.New("n1"),
				},
				{
					MIG:       mig1,
					Type:      ops.SuspendOp,
					NodeNames: set.New("n2"),
				},
			},
			expectOps: []ops.Operation{
				{
					MIG:       mig1,
					Type:      ops.SuspendOp,
					NodeNames: set.New("n1"),
				},
			},
		},
		{
			name:      "separate_ops",
			queueSize: 10,
			ops: []ops.Operation{
				{
					MIG:       mig1,
					Type:      ops.SuspendOp,
					NodeNames: set.New("n1", "n2"),
				},
				{
					MIG:       mig1,
					Type:      ops.ConsumeOp,
					NodeNames: set.New("n3", "n4"),
				},
				{
					MIG:       mig2,
					Type:      ops.SuspendOp,
					NodeNames: set.New("n5", "n6"),
				},
				{
					MIG:       mig2,
					Type:      ops.ConsumeOp,
					NodeNames: set.New("n7", "n8"),
				},
			},
			expectOps: []ops.Operation{
				{
					MIG:       mig1,
					Type:      ops.SuspendOp,
					NodeNames: set.New("n1", "n2"),
				},
				{
					MIG:       mig1,
					Type:      ops.ConsumeOp,
					NodeNames: set.New("n3", "n4"),
				},
				{
					MIG:       mig2,
					Type:      ops.SuspendOp,
					NodeNames: set.New("n5", "n6"),
				},
				{
					MIG:       mig2,
					Type:      ops.ConsumeOp,
					NodeNames: set.New("n7", "n8"),
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sm := &statetest.MockStateManager{SetPendingErr: tc.pendingUpdateErr}
			q := NewWorkQueue(tc.queueSize, sm)
			for _, op := range tc.ops {
				assert.NoError(t, q.Enqueue(op))
			}
			assert.Len(t, sm.GetPendingOperationUpdateCalls(), len(tc.ops))

			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()

			for _, expected := range tc.expectOps {
				got, ok := q.Dequeue(ctx)
				assert.True(t, ok, "Dequeue should return true")
				assert.Equal(t, expected.MIG, got.MIG)
				assert.Equal(t, expected.Type, got.Type)
				assert.True(t, expected.NodeNames.Equal(got.NodeNames), "NodeNames should match")
			}
		})
	}
}

func TestDequeue_ContextCancel(t *testing.T) {
	sm := &statetest.MockStateManager{}
	q := NewWorkQueue(10, sm)
	ctx, cancel := context.WithCancel(t.Context())

	// Cancel immediately
	cancel()

	op, ok := q.Dequeue(ctx)
	assert.False(t, ok)
	assert.Equal(t, ops.Operation{}, op)
	assert.Empty(t, sm.GetPendingOperationUpdateCalls())
}
