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

package taints

import "math/bits"

// distribution tracks the number of nodes having a specific number of soft taints.
// It ensures that the number of nodes with k taints is limited to 2^k.
// This distribution strategy keeps the maximum number of taints per node
// logarithmic relative to the total number of nodes (log2(n)).
// WARNING: make sure all values passed to the distribution are >= 0.
// Otherwise, the program will panic.
type distribution struct {
	counts map[int]int
	// availableMask is a bitmask where bit `i` is 1 if bucket `i` is available.
	// A bucket `i` is available if the number of nodes with `i` taints is less
	// than `2^i`.
	availableMask uint
}

// newDist returns a new distribution.
// All buckets are empty at first.
func newDist() *distribution {
	return &distribution{
		counts:        make(map[int]int),
		availableMask: ^uint(0), // All available initially
	}
}

// Register increments the count for a specific taint level.
func (d *distribution) Register(k int) {
	d.counts[k]++
	if d.IsFull(k) {
		// Mark as full: clear bit k
		d.availableMask &^= 1 << k
	}
}

// Unregister decrements the count for a specific taint level.
func (d *distribution) Unregister(k int) {
	if c := d.counts[k]; c == 0 {
		return
	} else if c == 1 {
		delete(d.counts, k)
	} else {
		d.counts[k]--
	}

	if !d.IsFull(k) {
		// Mark as available: set bit k
		d.availableMask |= 1 << k
	}
}

// NextAvailable finds the smallest taint count N such that the number of nodes
// with N taints is strictly less than 2^N.
func (d *distribution) NextAvailable() int {
	return bits.TrailingZeros(d.availableMask)
}

// IsFull returns true if the bucket for taint count k is full.
func (d *distribution) IsFull(k int) bool {
	return d.counts[k] >= (1 << k)
}

// IsEmpty returns true if and only if there are no registered taint counts.
func (d *distribution) IsEmpty() bool {
	return len(d.counts) == 0
}
