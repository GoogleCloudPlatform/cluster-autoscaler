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
	"sync"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state"
	"k8s.io/utils/set"
)

var (
	// ErrQueueFull is returned when the queue is at maximum capacity.
	ErrQueueFull = errors.New("queue is full")
)

type StateManager interface {
	SetPendingOperation(op ops.OperationType, pending bool, nodeNames set.Set[string], opts ...state.PendingOperationOpt) map[string]error
}

// WorkQueue is a thread-safe, batched work queue.
type WorkQueue struct {
	lock             sync.Mutex
	opKeyToNodeNames map[opKey]set.Set[string]
	workNotifier     chan opKey
	stateManager     StateManager
}

// NewWorkQueue returns a new instance of WorkQueue.
func NewWorkQueue(maxSize int, stateManager StateManager) *WorkQueue {
	return &WorkQueue{
		opKeyToNodeNames: make(map[opKey]set.Set[string]),
		workNotifier:     make(chan opKey, maxSize),
		stateManager:     stateManager,
	}
}

// Enqueue adds nodes to the queue.
// Returns ErrQueueFull if a new batch cannot be created because the queue
// is full.
func (q *WorkQueue) Enqueue(o ops.Operation) error {
	return q.EnqueueWithOpts(o)
}

// EnqueueWithOpts allows callers to pass some optional parameters related
// to interactions with the NodeStateManager.
func (q *WorkQueue) EnqueueWithOpts(o ops.Operation, opts ...state.PendingOperationOpt) error {
	if len(o.NodeNames) == 0 {
		return nil
	}
	key := opKey{MIG: o.MIG, OpType: o.Type}
	pendingErrs := q.stateManager.SetPendingOperation(o.Type, true, o.NodeNames, opts...)
	for name := range pendingErrs {
		// Skip adding to the queue if it wasn't possible to mark
		// operations as pending.
		// This is because the node is no longer a CSN node.
		o.NodeNames.Delete(name)
	}
	if len(o.NodeNames) == 0 {
		return nil
	}
	if err := q.addBatch(key, o.NodeNames); err != nil {
		// best-effort rollback of pending operation
		q.stateManager.SetPendingOperation(o.Type, false, o.NodeNames)
		return err
	}
	return nil
}

func (q *WorkQueue) addBatch(key opKey, nodeNames set.Set[string]) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	opType := key.OpType.String()
	entry, exists := q.opKeyToNodeNames[key]
	if exists {
		// Existing batch. We don't need to notify (already notified when created).
		for name := range nodeNames {
			if !entry.Has(name) {
				entry.Insert(name)
				opQueueNodesTotal.WithLabelValues(opType).Inc()
			}
		}
		opQueueAddsTotal.WithLabelValues(queueAddSuccess, existingBatch, opType).Inc()
		return nil
	}
	// New batch.
	q.opKeyToNodeNames[key] = nodeNames
	select {
	// Success
	case q.workNotifier <- key:
		opQueueLength.WithLabelValues(opType).Inc()
		opQueueNodesTotal.WithLabelValues(opType).Add(float64(len(nodeNames)))
		opQueueAddsTotal.WithLabelValues(queueAddSuccess, newBatch, opType).Inc()
	default:
		// roll back addition of new nodes
		delete(q.opKeyToNodeNames, key)
		opQueueAddsTotal.WithLabelValues(queueAddFailure, newBatch, opType).Inc()
		return ErrQueueFull
	}
	return nil
}

// Dequeue waits for a new operation to be found in the queue and returns it.
// This is a blocking operation. It can be stopped early by cancelling the
// passed context. The operation can also be stopped if the WorkQueue
// stops running. In both of these cases, the second return value will
// be `false`.
func (q *WorkQueue) Dequeue(ctx context.Context) (ops.Operation, bool) {
	key, ok := q.waitForOp(ctx)
	if !ok {
		return ops.Operation{}, false
	}

	q.lock.Lock()
	defer q.lock.Unlock()

	entry := q.opKeyToNodeNames[key]

	// Take ownership
	delete(q.opKeyToNodeNames, key)

	opType := key.OpType.String()
	opQueueLength.WithLabelValues(opType).Dec()
	opQueueNodesTotal.WithLabelValues(opType).Add(float64(-len(entry)))

	return ops.Operation{
		MIG:       key.MIG,
		Type:      key.OpType,
		NodeNames: entry,
	}, true
}

// Length returns the number of operations which await being processed.
func (q *WorkQueue) Length() int {
	return len(q.workNotifier)
}

func (q *WorkQueue) waitForOp(ctx context.Context) (opKey, bool) {
	select {
	case key := <-q.workNotifier:
		return key, true
	case <-ctx.Done():
		return opKey{}, false
	}
}

// opKey keys batches by MIG and OperationType.
type opKey struct {
	MIG    gce.GceRef
	OpType ops.OperationType
}
