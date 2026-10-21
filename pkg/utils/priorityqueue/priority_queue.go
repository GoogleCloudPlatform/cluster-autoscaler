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

// Package priorityqueue provides a generic priority queue backed by
// container/heap.
package priorityqueue

import (
	"container/heap"
)

// PriorityQueue is a generic priority queue backed by container/heap and
// ordered by a caller-supplied comparator. Construct instances with
// NewWithLessComparator.
type PriorityQueue[T any] struct {
	adapter heapAdapter[T]
}

// NewWithLessComparator creates a PriorityQueue ordered by less, where
// less(a, b) reports whether a has higher priority than b.
func NewWithLessComparator[T any](less func(a, b T) bool) *PriorityQueue[T] {
	return &PriorityQueue[T]{
		adapter: heapAdapter[T]{less: less},
	}
}

// Len returns the number of items in the priority queue.
func (q *PriorityQueue[T]) Len() int {
	return q.adapter.Len()
}

// Push adds an item to the priority queue.
func (q *PriorityQueue[T]) Push(item T) {
	heap.Push(&q.adapter, item)
}

// Pop removes and returns the highest-priority item according to less, along
// with true. If the priority queue is empty, Pop returns the zero value of T
// and false.
func (q *PriorityQueue[T]) Pop() (T, bool) {
	if q.Len() == 0 {
		var zero T
		return zero, false
	}
	item, _ := heap.Pop(&q.adapter).(T)
	return item, true
}

// heapAdapter satisfies container/heap.Interface while keeping untyped any
// methods unexported so callers cannot bypass heap invariants.
type heapAdapter[T any] struct {
	items []T
	less  func(a, b T) bool
}

var _ heap.Interface = (*heapAdapter[any])(nil)

func (h *heapAdapter[T]) Len() int {
	return len(h.items)
}

func (h *heapAdapter[T]) Less(i, j int) bool {
	if h.less == nil {
		return false
	}
	return h.less(h.items[i], h.items[j])
}

func (h *heapAdapter[T]) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *heapAdapter[T]) Push(item any) {
	typedItem, _ := item.(T)
	h.items = append(h.items, typedItem)
}

func (h *heapAdapter[T]) Pop() any {
	old := h.items
	lastIndex := len(old) - 1
	item := old[lastIndex]
	// Zero the vacated slot so the backing array does not retain references
	// to popped items.
	var zero T
	old[lastIndex] = zero
	h.items = old[:lastIndex]
	return item
}
