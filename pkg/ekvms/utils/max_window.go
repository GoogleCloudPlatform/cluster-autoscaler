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

package utils

import (
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	"k8s.io/utils/clock"
)

// MaxWindow keeps track of added size.Allocatable instances, and handles max cpu/memory queries.
type MaxWindow interface {
	MaxWindowReader
	MaxWindowWriter
}

// MaxWindowReader exposes reading functionality of MaxWindow.
type MaxWindowReader interface {
	// MaxMilliCpus returns highest kBytes value over allocatables added after (now-ttl).
	MaxMilliCpus() (int64, error)

	// MaxKBytes returns highest kBytes value over allocatables added after (now-ttl).
	MaxKBytes() (int64, error)
}

// MaxWindowWriter exposes writing functionality of MaxWindow.
type MaxWindowWriter interface {
	// Add adds allocatable.
	Add(allocatable size.Allocatable)

	UpdateTtl(ttl time.Duration)
}

// TtlMaxWindow is a simple MaxWindow implementation that automatically evicts samples older than ttl.
type TtlMaxWindow struct {
	clock clock.PassiveClock
	ttl   time.Duration
	// Monotonic queue tracking samples sorted by CPU within the TTL window. Used for efficient MaxMilliCpus calculations.
	cpuSortedSamples *samplesMonotonicQueue
	// Monotonic queue tracking samples sorted by memory within the TTL window. Used for efficient MaxKBytes calculations.
	memorySortedSamples *samplesMonotonicQueue
}

// NewTtlMaxWindow returns a new TtlMaxWindow.
func NewTtlMaxWindow(clock clock.PassiveClock, ttl time.Duration) MaxWindow {
	return &TtlMaxWindow{
		clock: clock,
		ttl:   ttl,
		cpuSortedSamples: &samplesMonotonicQueue{
			queue: []sample{},
		},
		memorySortedSamples: &samplesMonotonicQueue{
			queue: []sample{},
		},
	}
}

// Adds a new sample to monotonic queues based on a given allocatable and current time.
func (w *TtlMaxWindow) Add(allocatable size.Allocatable) {
	w.cpuSortedSamples.add(sample{value: allocatable.MilliCpus, addTime: w.clock.Now()})
	w.memorySortedSamples.add(sample{value: allocatable.KBytes, addTime: w.clock.Now()})
	w.evictExpired()
}

// MaxMilliCpus returns the highest milliCpu value over samples added after (now-ttl), or error if none exist.
func (w *TtlMaxWindow) MaxMilliCpus() (int64, error) {
	return w.max(w.cpuSortedSamples, v1.ResourceCPU)
}

// MaxKBytes returns the highest kBytes value over samples added after (now-ttl), or error if none exist.
func (w *TtlMaxWindow) MaxKBytes() (int64, error) {
	return w.max(w.memorySortedSamples, v1.ResourceMemory)
}

func (w *TtlMaxWindow) UpdateTtl(ttl time.Duration) {
	w.ttl = ttl
}

// Evicts samples older than ttl.
func (w *TtlMaxWindow) evictExpired() {
	threshold := w.clock.Now().Add(-w.ttl)
	w.cpuSortedSamples.evictExpired(threshold)
	w.memorySortedSamples.evictExpired(threshold)
}

func (w *TtlMaxWindow) max(queue *samplesMonotonicQueue, resource v1.ResourceName) (int64, error) {
	w.evictExpired()
	if queue.isEmpty() {
		return 0, fmt.Errorf("no samples within specified ttl for resource %s", resource)
	}
	return queue.head().value, nil
}

// sample is a size.Allocatable with the time when it was added to MaxWindow.
type sample struct {
	value   int64
	addTime time.Time
}

// Monotonic decreasing queue implementation for samples.
type samplesMonotonicQueue struct {
	queue []sample
}

func (q *samplesMonotonicQueue) add(newSample sample) {
	for !q.isEmpty() && q.tail().value < newSample.value {
		q.removeTail()
	}
	q.queue = append(q.queue, newSample)
}

func (q *samplesMonotonicQueue) head() sample {
	return q.queue[0]
}

func (q *samplesMonotonicQueue) removeHead() {
	q.queue = q.queue[1:]
}

func (q *samplesMonotonicQueue) tail() sample {
	return q.queue[q.len()-1]
}

func (q *samplesMonotonicQueue) removeTail() {
	q.queue = q.queue[:q.len()-1]
}

func (q *samplesMonotonicQueue) len() int {
	return len(q.queue)
}

func (q *samplesMonotonicQueue) isEmpty() bool {
	return q.len() == 0
}

func (q *samplesMonotonicQueue) evictExpired(threshold time.Time) {
	for !q.isEmpty() && q.head().addTime.Before(threshold) {
		q.removeHead()
	}
}
