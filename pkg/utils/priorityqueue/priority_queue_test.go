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

package priorityqueue

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

type testQueueItem struct {
	id    string
	size  int
	score float64
}

func TestPriorityQueue_PushPopAndTieBreaking(t *testing.T) {
	pq := NewWithLessComparator(func(a, b *testQueueItem) bool {
		if a.size != b.size {
			return a.size < b.size
		}
		return a.score > b.score
	})

	if got, ok := pq.Pop(); ok || got != nil {
		t.Fatalf("Pop() on empty queue = (%v, %v), want (nil, false)", got, ok)
	}
	if got := pq.Len(); got != 0 {
		t.Fatalf("Len() on empty queue = %d, want 0", got)
	}

	pq.Push(&testQueueItem{id: "b", size: 10, score: 0.5})
	pq.Push(&testQueueItem{id: "a", size: 5, score: 0.2})
	pq.Push(&testQueueItem{id: "c", size: 10, score: 0.9})
	pq.Push(&testQueueItem{id: "d", size: 2, score: 0.1})

	if got := pq.Len(); got != 4 {
		t.Fatalf("Len() after 4 pushes = %d, want 4", got)
	}

	var gotOrder []string
	for pq.Len() > 0 {
		item, ok := pq.Pop()
		if !ok {
			t.Fatalf("Pop() ok = false while Len() > 0")
		}
		gotOrder = append(gotOrder, item.id)
	}
	wantOrder := []string{"d", "a", "c", "b"}
	if diff := cmp.Diff(wantOrder, gotOrder); diff != "" {
		t.Errorf("Pop() order diff (-want +got):\n%s", diff)
	}

	if got, ok := pq.Pop(); ok || got != nil {
		t.Errorf("Pop() after draining = (%v, %v), want (nil, false)", got, ok)
	}

	// Backing array slots past Len() must be zeroed so GC references
	// are not retained.
	backing := pq.adapter.items[:cap(pq.adapter.items)]
	for i, slot := range backing {
		if slot != nil {
			t.Errorf("backing[%d] = %v, want nil", i, slot)
		}
	}
}

func TestPriorityQueue_MutateAndRePush(t *testing.T) {
	pq := NewWithLessComparator(func(a, b *testQueueItem) bool {
		if a.size != b.size {
			return a.size < b.size
		}
		return a.score > b.score
	})

	itemA := &testQueueItem{id: "a", size: 2, score: 0.1}
	itemB := &testQueueItem{id: "b", size: 2, score: 0.9}
	pq.Push(itemA)
	pq.Push(itemB)

	var poppedIDs []string
	for range 3 {
		item, ok := pq.Pop()
		if !ok {
			t.Fatalf("Pop() ok = false, want true")
		}
		poppedIDs = append(poppedIDs, item.id)
		item.size++
		pq.Push(item)
	}

	wantPopped := []string{"b", "a", "b"}
	if diff := cmp.Diff(wantPopped, poppedIDs); diff != "" {
		t.Errorf("Pop()/re-Push sequence diff (-want +got):\n%s", diff)
	}
}

func TestPriorityQueue_NilComparatorDoesNotPanic(t *testing.T) {
	pq := NewWithLessComparator[int](nil)
	pq.Push(2)
	pq.Push(1)

	first, ok := pq.Pop()
	if !ok || first != 2 {
		t.Errorf("pq.Pop() first = (%d, %v), want (2, true)", first, ok)
	}
	second, ok := pq.Pop()
	if !ok || second != 1 {
		t.Errorf("pq.Pop() second = (%d, %v), want (1, true)", second, ok)
	}
}

func TestPriorityQueue_NilInterfaceElement(t *testing.T) {
	anyQueue := NewWithLessComparator(func(a, b any) bool { return a == nil && b != nil })
	anyQueue.Push("non-nil")
	anyQueue.Push(nil)

	first, ok := anyQueue.Pop()
	if !ok || first != nil {
		t.Errorf("anyQueue.Pop() first = (%v, %v), want (nil, true)", first, ok)
	}
	second, ok := anyQueue.Pop()
	if !ok || second != "non-nil" {
		t.Errorf("anyQueue.Pop() second = (%v, %v), want (\"non-nil\", true)", second, ok)
	}
}
