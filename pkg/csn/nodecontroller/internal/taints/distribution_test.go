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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDistribution_NextAvailable(t *testing.T) {
	testCases := map[string]struct {
		registers           []int
		wantAfterRegister   []int
		unregisters         []int
		wantAfterUnregister []int
		empty               bool
	}{
		"fill_bucket_1_fully": {
			registers:         []int{0, 1, 1},
			wantAfterRegister: []int{1, 1, 2},
		},
		"register_then_unregister": {
			registers:           []int{0},
			wantAfterRegister:   []int{1},
			unregisters:         []int{0},
			wantAfterUnregister: []int{0},
			empty:               true,
		},
		"skip_buckets": {
			registers:         []int{0, 2, 2, 2, 2},
			wantAfterRegister: []int{1, 1, 1, 1, 1},
		},
		"multiple_buckets_filled": {
			registers:         []int{0, 1, 1, 2, 2, 2, 2},
			wantAfterRegister: []int{1, 1, 2, 2, 2, 2, 3},
		},
		"punch_hole_in_middle": {
			registers:           []int{0, 1, 1, 2, 2, 2, 2},
			wantAfterRegister:   []int{1, 1, 2, 2, 2, 2, 3},
			unregisters:         []int{2, 1},
			wantAfterUnregister: []int{2, 1},
		},
		"unregister_everything": {
			registers:           []int{0, 1, 1, 2, 2, 2, 2},
			wantAfterRegister:   []int{1, 1, 2, 2, 2, 2, 3},
			unregisters:         []int{2, 2, 2, 2, 1, 1, 0},
			wantAfterUnregister: []int{2, 2, 2, 2, 1, 1, 0},
			empty:               true,
		},
		"overfilling_bucket_is_possible": {
			registers:           []int{0, 0, 0, 0, 0, 0, 0},
			wantAfterRegister:   []int{1, 1, 1, 1, 1, 1, 1},
			unregisters:         []int{0, 0, 0, 0, 0, 0, 0},
			wantAfterUnregister: []int{1, 1, 1, 1, 1, 1, 0},
			empty:               true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if len(tc.registers) != len(tc.wantAfterRegister) || len(tc.unregisters) != len(tc.wantAfterUnregister) {
				t.Fatalf("Number of checks has to be the same as the number of actions")
			}
			d := newDist()
			assert.True(t, d.IsEmpty())
			assert.Equal(t, 0, d.NextAvailable())
			for idx, k := range tc.registers {
				d.Register(k)
				assert.Equal(t, tc.wantAfterRegister[idx], d.NextAvailable())
			}
			for idx, k := range tc.unregisters {
				d.Unregister(k)
				assert.Equal(t, tc.wantAfterUnregister[idx], d.NextAvailable())
			}
			assert.Equal(t, tc.empty, d.IsEmpty())
		})
	}
}

func TestDistribution_IsFull(t *testing.T) {
	testCases := map[string]struct {
		registers   []int
		unregisters []int
		checkBucket int
		want        bool
	}{
		"empty_bucket_is_not_full": {
			checkBucket: 0,
			want:        false,
		},
		"bucket_0_full_with_1": {
			registers:   []int{0},
			checkBucket: 0,
			want:        true,
		},
		"bucket_1_not_full_with_1": {
			registers:   []int{1},
			checkBucket: 1,
			want:        false,
		},
		"bucket_1_full_with_2": {
			registers:   []int{1, 1},
			checkBucket: 1,
			want:        true,
		},
		"unregister_makes_not_full": {
			registers:   []int{0},
			unregisters: []int{0},
			checkBucket: 0,
			want:        false,
		},
		"unregister_from_empty_stays_not_full": {
			unregisters: []int{5},
			checkBucket: 5,
			want:        false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			d := newDist()
			for _, k := range tc.registers {
				d.Register(k)
			}
			for _, k := range tc.unregisters {
				d.Unregister(k)
			}
			assert.Equal(t, tc.want, d.IsFull(tc.checkBucket))
		})
	}
}

func TestDistribution_LargeBuckets(t *testing.T) {
	d := newDist()
	// Fill buckets 0 to 12
	for k := 0; k <= 12; k++ {
		assert.Equal(t, k, d.NextAvailable(), "Bucket %d should be next available", k)
		capacity := 1 << k
		for i := 0; i < capacity; i++ {
			assert.False(t, d.IsFull(k), "Bucket %d should not be full at %d/%d", k, i, capacity)
			d.Register(k)
		}
		assert.True(t, d.IsFull(k), "Bucket %d should be full", k)
	}
	assert.Equal(t, 13, d.NextAvailable())

	// Unregister from bucket 8 (capacity 256)
	d.Unregister(8)
	assert.False(t, d.IsFull(8))
	assert.Equal(t, 8, d.NextAvailable())

	// Re-register to bucket 8
	d.Register(8)
	assert.True(t, d.IsFull(8))
	assert.Equal(t, 13, d.NextAvailable())
}
