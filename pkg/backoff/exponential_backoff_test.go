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

package backoff

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
)

func TestExponentialBackoff_Backoff(t *testing.T) {
	minBackoff := 2 * time.Second
	maxBackoff := 5 * time.Second
	t0 := time.Now()

	testCases := []struct {
		name             string
		initialDuration  time.Duration
		initialUntil     time.Time
		expectedUntil    time.Time
		expectedDuration time.Duration
	}{
		{
			name:             "first backoff",
			initialDuration:  0,
			initialUntil:     time.Time{},
			expectedUntil:    t0.Add(minBackoff),
			expectedDuration: minBackoff,
		},
		{
			name:             "backoff doesn't increase before it wears off",
			initialDuration:  minBackoff,
			initialUntil:     t0.Add(minBackoff),
			expectedUntil:    t0.Add(minBackoff),
			expectedDuration: minBackoff,
		},
		{
			name:             "backoff increases exponentially",
			initialDuration:  minBackoff,
			expectedUntil:    t0.Add(2 * minBackoff),
			expectedDuration: 2 * minBackoff,
		},
		{
			name:             "backoff hits max",
			initialDuration:  2 * minBackoff,
			initialUntil:     t0,
			expectedUntil:    t0.Add(maxBackoff),
			expectedDuration: maxBackoff,
		},
		{
			name:             "backoff stays at max",
			initialDuration:  maxBackoff,
			initialUntil:     t0,
			expectedUntil:    t0.Add(maxBackoff),
			expectedDuration: maxBackoff,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b := &exponentialBackoff{
				until:           tc.initialUntil,
				duration:        tc.initialDuration,
				initialDuration: minBackoff,
				maxDuration:     maxBackoff,
			}

			until := b.Backoff(cloudprovider.InstanceErrorInfo{}, t0)
			assert.Equal(t, tc.expectedUntil, until)
			assert.Equal(t, tc.expectedDuration, b.duration)
		})
	}
}

func TestExponentialBackoff_RemoveStaleBackoffData(t *testing.T) {
	minBackoff := 2 * time.Second
	reset := 10 * time.Second
	t0 := time.Now()

	initialDuration := minBackoff
	initialUntil := t0.Add(minBackoff)
	initialReset := t0.Add(reset)

	testCases := []struct {
		name             string
		currentTime      time.Time
		expectedDuration time.Duration
		expectedUntil    time.Time
	}{
		{
			name:             "before reset time",
			currentTime:      t0.Add(reset - time.Second),
			expectedDuration: minBackoff,
			expectedUntil:    initialUntil,
		},
		{
			name:             "at reset time",
			currentTime:      t0.Add(reset),
			expectedDuration: 0,
			expectedUntil:    time.Time{},
		},
		{
			name:             "after reset time",
			currentTime:      t0.Add(reset + time.Second),
			expectedDuration: 0,
			expectedUntil:    time.Time{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b := &exponentialBackoff{
				until:           initialUntil,
				duration:        initialDuration,
				initialDuration: minBackoff,
				resetTime:       reset,
				reset:           initialReset,
			}

			b.RemoveStaleBackoffData(tc.currentTime)
			assert.Equal(t, tc.expectedDuration, b.duration)
			assert.Equal(t, tc.expectedUntil, b.BackoffUntil())
		})
	}
}
