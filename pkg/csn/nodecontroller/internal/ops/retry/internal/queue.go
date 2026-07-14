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

package internal

import (
	"container/heap"
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
)

type DelayedOp struct {
	Op           ops.Operation
	ExecuteAfter time.Time
}

// RetryQueue is a data structure that can be used for storing
// operations that should be run later.
// It allows for easy retrieval of operations ready to be retried
// as determined by the ExecuteAfter field.
type RetryQueue struct {
	heap *heapAdapter[DelayedOp]
}

// NewRetryQueue creates an empty operation retry queue
func NewRetryQueue() *RetryQueue {
	return &RetryQueue{
		heap: &heapAdapter[DelayedOp]{less: func(op1, op2 DelayedOp) bool {
			return op1.ExecuteAfter.Before(op2.ExecuteAfter)
		}},
	}
}

// Push adds a delayed operation to the retry queue.
func (q *RetryQueue) Push(item DelayedOp) {
	heap.Push(q.heap, item)
}

// PopReadyToRun return a slice of operations that can be retried.
func (q *RetryQueue) PopReadyToRun(now time.Time) []DelayedOp {
	var readyOps []DelayedOp
	for {
		op, ok := q.heap.Peek()
		if !ok || !now.After(op.ExecuteAfter) {
			break
		}
		readyOps = append(readyOps, heap.Pop(q.heap).(DelayedOp))
	}
	return readyOps
}

// FirstToRun returns the operation that is the closest to being able
// to be retried if at least one exists in the queue.
func (q *RetryQueue) FirstToRun() (DelayedOp, bool) {
	return q.heap.Peek()
}

// heapAdapter is the internal struct that satisfies the standard
// container/heap interface.
// We keep this private so callers never have to deal with 'any'.
type heapAdapter[T any] struct {
	items []T
	less  func(a, b T) bool
}

// Len satisfies the heap interface.
func (a *heapAdapter[T]) Len() int { return len(a.items) }

// Less satisfies the heap interface.
func (a *heapAdapter[T]) Less(i, j int) bool { return a.less(a.items[i], a.items[j]) }

// Swap satisfies the heap interface.
func (a *heapAdapter[T]) Swap(i, j int) { a.items[i], a.items[j] = a.items[j], a.items[i] }

// Push satisfies the heap interface.
func (a *heapAdapter[T]) Push(x any) {
	a.items = append(a.items, x.(T))
}

// Peek allows callers to easily look at the first
// element in the heap without taking it out.
func (a *heapAdapter[T]) Peek() (T, bool) {
	if len(a.items) == 0 {
		var zero T
		return zero, false
	}
	return a.items[0], true
}

// Pop satisfies the heap interface.
func (a *heapAdapter[T]) Pop() any {
	old := a.items
	n := len(old)
	item := old[n-1]
	a.items = old[0 : n-1]
	return item
}
