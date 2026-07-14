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

import "time"

// timeQueue is a ring buffer of time.Time values.
// The queue reserves only as much memory as needed to store the items. It's meant to be used in cases when max number of items doesn't change often.
type timeQueue struct {
	items    []time.Time
	startIdx int // first element index in the queue
	len      int // number of elements in the queue
}

func (bs *timeQueue) getIndex(position int) int {
	return (bs.startIdx + position) % len(bs.items)
}

func (bs *timeQueue) ensureCapacity(size int) {
	if size <= len(bs.items) {
		return // enough size
	}

	newData := make([]time.Time, size)

	for i := 0; i < bs.len; i++ {
		oldIndex := bs.getIndex(i)
		newData[i] = bs.items[oldIndex]
	}

	bs.items = newData
	bs.startIdx = 0
}

// EnqueueMany enqueues time num times.
func (bs *timeQueue) EnqueueMany(item time.Time, num int) {
	bs.ensureCapacity(bs.len + num)

	for i := 0; i < num; i++ {
		idx := bs.getIndex(bs.len + i)
		bs.items[idx] = item
	}
	bs.len += num
}

// Dequeue dequeues the first item from the queue.
func (bs *timeQueue) Dequeue() (time.Time, bool) {
	if bs.len == 0 {
		return time.Time{}, false
	}

	item := bs.items[bs.getIndex(0)]
	bs.items[bs.getIndex(0)] = time.Time{}
	bs.startIdx = bs.getIndex(1)
	bs.len--
	return item, true
}

// Peek returns the first item from the queue.
func (bs *timeQueue) Peek() (time.Time, bool) {
	if bs.len == 0 {
		return time.Time{}, false
	}

	return bs.items[bs.getIndex(0)], true
}

func (bs *timeQueue) Len() int {
	return bs.len
}
