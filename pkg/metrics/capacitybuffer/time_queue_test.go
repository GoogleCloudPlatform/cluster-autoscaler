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

package capacitybuffer

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestTimeQueue_BasicOperations(t *testing.T) {
	tq := &timeQueue{}
	baseTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)

	// Test Empty
	if got, ok := tq.Peek(); ok {
		t.Errorf("Peek() on empty queue returned %v, %v; want false", got, ok)
	}
	if got, ok := tq.Dequeue(); ok {
		t.Errorf("Dequeue() on empty queue returned %v, %v; want false", got, ok)
	}
	if got := tq.Len(); got != 0 {
		t.Errorf("Len() = %d; want 0", got)
	}

	// Test Enqueue
	tq.EnqueueMany(baseTime, 1)
	if got := tq.Len(); got != 1 {
		t.Errorf("After EnqueueMany(1), Len() = %d; want 1", got)
	}

	tq.EnqueueMany(baseTime.Add(time.Minute), 2)
	if got := tq.Len(); got != 3 {
		t.Errorf("After EnqueueMany(2), Len() = %d; want 3", got)
	}

	// Test Peek
	if got, ok := tq.Peek(); !ok || !got.Equal(baseTime) {
		t.Errorf("Peek() = %v, %v; want %v, true", got, ok, baseTime)
	}

	// Test Dequeue
	got, ok := tq.Dequeue()
	if !ok || !got.Equal(baseTime) {
		t.Errorf("Dequeue() = %v, %v; want %v, true", got, ok, baseTime)
	}
	if got := tq.Len(); got != 2 {
		t.Errorf("After Dequeue, Len() = %d; want 2", got)
	}

	// Dequeue remaining
	got2, ok2 := tq.Dequeue()
	got3, ok3 := tq.Dequeue()

	if !ok2 || !got2.Equal(baseTime.Add(time.Minute)) {
		t.Errorf("Second Dequeue() = %v, %v; want %v, true", got2, ok2, baseTime.Add(time.Minute))
	}
	if !ok3 || !got3.Equal(baseTime.Add(time.Minute)) {
		t.Errorf("Third Dequeue() = %v, %v; want %v, true", got3, ok3, baseTime.Add(time.Minute))
	}

	// Verify empty again
	if got := tq.Len(); got != 0 {
		t.Errorf("After all Dequeues, Len() = %d; want 0", got)
	}
}

func TestTimeQueue_WrapAround(t *testing.T) {
	// Initialize with capacity 4 to control wrap around easily
	tq := &timeQueue{
		items:    make([]time.Time, 4),
		startIdx: 0,
		len:      0,
	}
	baseTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)

	// Fill buffer: [t0, t1, t2, t3]
	times := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Minute),
		baseTime.Add(2 * time.Minute),
		baseTime.Add(3 * time.Minute),
	}
	for i := 0; i < 4; i++ {
		tq.EnqueueMany(times[i], 1)
	}

	// Dequeue 2 items: [_, _, t2, t3] (startIdx = 2)
	tq.Dequeue()
	tq.Dequeue()

	if tq.startIdx != 2 {
		t.Errorf("Internal state check: startIdx = %d; want 2", tq.startIdx)
	}

	// Enqueue 2 new items: [t5, t6, t2, t3] (wrapped)
	t4 := baseTime.Add(4 * time.Minute)
	t5 := baseTime.Add(5 * time.Minute)
	tq.EnqueueMany(t4, 1)
	tq.EnqueueMany(t5, 1)

	if tq.Len() != 4 {
		t.Fatalf("Len() = %d; want 4", tq.Len())
	}

	// Verify order matches expectation
	expected := []time.Time{
		times[2],
		times[3],
		t4,
		t5,
	}

	var got []time.Time
	for tq.Len() > 0 {
		item, _ := tq.Dequeue()
		got = append(got, item)
	}

	if diff := cmp.Diff(expected, got); diff != "" {
		t.Errorf("Unexpected queue content mismatch (-want +got):\n%s", diff)
	}
}

func TestTimeQueue_ResizeWithWrapAround(t *testing.T) {
	// Initialize small buffer
	tq := &timeQueue{
		items:    make([]time.Time, 3),
		startIdx: 0,
		len:      0,
	}
	baseTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)

	// Fill: [t0, t1, t2] (startIdx=0, len=3)
	for i := 0; i < 3; i++ {
		tq.EnqueueMany(baseTime.Add(time.Duration(i)*time.Minute), 1)
	}

	// Dequeue 2: [_, _, t2] (startIdx=2, len=1)
	tq.Dequeue()
	tq.Dequeue()

	// Enqueue 2: [t3, t4, t2] (wrapped) (startIdx=2, len=3)
	tq.EnqueueMany(baseTime.Add(3*time.Minute), 1)
	tq.EnqueueMany(baseTime.Add(4*time.Minute), 1)

	// Current state check
	if tq.items[2] != baseTime.Add(2*time.Minute) { // logical head
		t.Errorf("Internal state mismatch before resize")
	}

	// Enqueue more to force resize: Need capacity for 3 (current) + 2 (new) = 5
	// This should trigger ensureCapacity -> resize (likely double or exact fit logic pending impl details, but definitely > 3)
	tq.EnqueueMany(baseTime.Add(5*time.Minute), 2)

	if len(tq.items) < 5 {
		t.Errorf("Capacity not increased; cap = %d, want >= 5", len(tq.items))
	}

	// Verify contents are linearized correctly if ensureCapacity cleans it up
	// Implementation `ensureCapacity`:
	// 	newData := make([]time.Time, size)
	//	for i := 0; i < bs.len; i++ { ... newData[i] = bs.items[oldIndex] }
	//	bs.startIdx = 0
	// This suggests it linearizes the buffer starting at 0.

	if tq.startIdx != 0 {
		t.Errorf("After resize, startIdx = %d; want 0", tq.startIdx)
	}

	expected := []time.Time{
		baseTime.Add(2 * time.Minute), // t2
		baseTime.Add(3 * time.Minute), // t3
		baseTime.Add(4 * time.Minute), // t4
		baseTime.Add(5 * time.Minute), // t5 (1st)
		baseTime.Add(5 * time.Minute), // t5 (2nd)
	}

	var got []time.Time
	for tq.Len() > 0 {
		item, _ := tq.Dequeue()
		got = append(got, item)
	}

	if diff := cmp.Diff(expected, got); diff != "" {
		t.Errorf("Unexpected queue content after resize (-want +got):\n%s", diff)
	}
}

func TestTimeQueue_Interleaved(t *testing.T) {
	tq := &timeQueue{}
	baseTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)

	// Sequence:
	// +1 (t0)
	// +1 (t1)
	// -1 (pop t0)
	// +2 (t2, t2)
	// -1 (pop t1)
	// -1 (pop t2)
	// +1 (t3)
	// Remaining: t2, t3

	tq.EnqueueMany(baseTime, 1)                    // [t0]
	tq.EnqueueMany(baseTime.Add(time.Minute), 1)   // [t0, t1]
	item0, _ := tq.Dequeue()                       // [t1]
	tq.EnqueueMany(baseTime.Add(2*time.Minute), 2) // [t1, t2, t2]
	item1, _ := tq.Dequeue()                       // [t2, t2]
	item2, _ := tq.Dequeue()                       // [t2]
	tq.EnqueueMany(baseTime.Add(3*time.Minute), 1) // [t2, t3]

	if !item0.Equal(baseTime) {
		t.Errorf("First pop = %v; want %v", item0, baseTime)
	}
	if !item1.Equal(baseTime.Add(time.Minute)) {
		t.Errorf("Second pop = %v; want %v", item1, baseTime.Add(time.Minute))
	}
	if !item2.Equal(baseTime.Add(2 * time.Minute)) {
		t.Errorf("Third pop = %v; want %v", item2, baseTime.Add(2*time.Minute))
	}

	if tq.Len() != 2 {
		t.Fatalf("Final Len() = %d; want 2", tq.Len())
	}
}
