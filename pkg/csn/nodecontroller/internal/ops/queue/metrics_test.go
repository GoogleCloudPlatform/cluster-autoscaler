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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	statetest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/test"
	"k8s.io/utils/set"
)

func TestWorkQueueMetrics(t *testing.T) {
	mig1 := gce.GceRef{Project: "p1", Zone: "z1", Name: "m1"}
	mig2 := gce.GceRef{Project: "p1", Zone: "z2", Name: "m2"}
	sm := &statetest.MockStateManager{}

	suspendLabel := ops.SuspendOp.String()
	consumeLabel := ops.ConsumeOp.String()

	tests := []struct {
		name           string
		size           int
		initialEnqueue []ops.Operation
		action         func(t *testing.T, q *WorkQueue)
		expectedDeltas []*test.MetricDelta
	}{
		{
			name: "enqueue_single_item",
			action: func(t *testing.T, q *WorkQueue) {
				err := q.Enqueue(ops.Operation{MIG: mig1, Type: ops.SuspendOp, NodeNames: set.New("n1")})
				assert.NoError(t, err)
			},
			expectedDeltas: []*test.MetricDelta{
				delta(1, opQueueLength, suspendLabel),
				delta(1, opQueueNodesTotal, suspendLabel),
				delta(1, opQueueAddsTotal, queueAddSuccess, newBatch, suspendLabel),
			},
		},
		{
			name: "enqueue_items_with_same_mig",
			initialEnqueue: []ops.Operation{
				{MIG: mig1, Type: ops.ConsumeOp, NodeNames: set.New("n1")},
			},
			action: func(t *testing.T, q *WorkQueue) {
				err := q.Enqueue(ops.Operation{MIG: mig1, Type: ops.ConsumeOp, NodeNames: set.New("n2")})
				assert.NoError(t, err)
			},
			expectedDeltas: []*test.MetricDelta{
				delta(0, opQueueLength, consumeLabel),
				delta(1, opQueueNodesTotal, consumeLabel),
				delta(1, opQueueAddsTotal, queueAddSuccess, existingBatch, consumeLabel),
			},
		},
		{
			name: "enqueue_items_with_different_mig",
			initialEnqueue: []ops.Operation{
				{MIG: mig1, Type: ops.SuspendOp, NodeNames: set.New("n1")},
			},
			action: func(t *testing.T, q *WorkQueue) {
				err := q.Enqueue(ops.Operation{MIG: mig2, Type: ops.SuspendOp, NodeNames: set.New("n2")})
				assert.NoError(t, err)
			},
			expectedDeltas: []*test.MetricDelta{
				delta(1, opQueueLength, suspendLabel),
				delta(1, opQueueNodesTotal, suspendLabel),
				delta(1, opQueueAddsTotal, queueAddSuccess, newBatch, suspendLabel),
			},
		},
		{
			name: "enqueue_to_full_queue_fails",
			size: 1,
			initialEnqueue: []ops.Operation{
				{MIG: mig1, Type: ops.SuspendOp, NodeNames: set.New("n1")},
			},
			action: func(t *testing.T, q *WorkQueue) {
				err := q.Enqueue(ops.Operation{MIG: mig1, Type: ops.ConsumeOp, NodeNames: set.New("n2")})
				assert.ErrorIs(t, err, ErrQueueFull)
			},
			expectedDeltas: []*test.MetricDelta{
				delta(1, opQueueAddsTotal, queueAddFailure, newBatch, consumeLabel),
			},
		},
		{
			name: "dequeue",
			initialEnqueue: []ops.Operation{
				{MIG: mig1, Type: ops.SuspendOp, NodeNames: set.New("n1")},
			},
			action: func(t *testing.T, q *WorkQueue) {
				_, ok := q.Dequeue(context.Background())
				assert.True(t, ok)
			},
			expectedDeltas: []*test.MetricDelta{
				delta(-1, opQueueLength, suspendLabel),
				delta(-1, opQueueNodesTotal, suspendLabel),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			size := tc.size
			if size == 0 {
				size = 10
			}
			q := NewWorkQueue(size, sm)

			for _, op := range tc.initialEnqueue {
				err := q.Enqueue(op)
				assert.NoError(t, err)
			}

			for _, ed := range tc.expectedDeltas {
				ed.Init(t)
			}

			tc.action(t, q)

			// Verify after
			for _, ed := range tc.expectedDeltas {
				assert.NoError(t, ed.Verify(t))
			}
		})
	}
}

func delta(expected float64, metric k8smetrics.Registerable, labels ...string) *test.MetricDelta {
	return test.NewMetricDelta(test.ExpectedValue(expected), metric, labels)
}
