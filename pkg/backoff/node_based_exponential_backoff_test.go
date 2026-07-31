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
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	testprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
)

func TestNodeBasedExponentialBackoff_Backoff(t *testing.T) {
	minBackoff := 2 * time.Second
	maxBackoff := 5 * time.Second
	resetTimeout := 10 * time.Second
	t0 := time.Now()

	nodeGroup := testprovider.NewTestNodeGroup("mig1", 1, 10, 1, true, false, "e2-standard-2", nil, nil)
	nodeInfo := framework.NewTestNodeInfo(&apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}})

	tests := []struct {
		name             string
		initialDuration  time.Duration
		initialUntil     time.Time
		expectedUntil    time.Time
		expectedDuration time.Duration
		useJitter        bool
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
			initialUntil:     t0.Add(-time.Second), // previously wore off
			expectedUntil:    t0.Add(2 * minBackoff),
			expectedDuration: 2 * minBackoff,
		},
		{
			name:             "backoff hits max",
			initialDuration:  2 * minBackoff,
			initialUntil:     t0.Add(-time.Second),
			expectedUntil:    t0.Add(maxBackoff),
			expectedDuration: maxBackoff,
		},
		{
			name:             "backoff stays at max",
			initialDuration:  maxBackoff,
			initialUntil:     t0.Add(-time.Second),
			expectedUntil:    t0.Add(maxBackoff),
			expectedDuration: maxBackoff,
		},
		{
			name:             "jitter capped by maxBackoff",
			initialDuration:  maxBackoff,
			initialUntil:     t0.Add(-time.Second),
			expectedUntil:    t0.Add(maxBackoff),
			expectedDuration: maxBackoff,
			useJitter:        true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := NewNodeBasedExponentialBackoff(minBackoff, maxBackoff, resetTimeout, tc.useJitter)

			if tc.initialDuration > 0 {
				b.backoffInfo["node-1"] = nodeBackoffInfo{
					duration:            tc.initialDuration,
					backoffUntil:        tc.initialUntil,
					lastFailedExecution: t0.Add(-time.Second),
				}
			}

			until := b.Backoff(nodeGroup, nodeInfo, cloudprovider.InstanceErrorInfo{}, t0)
			assert.Equal(t, tc.expectedUntil, until)
			assert.Equal(t, tc.expectedDuration, b.backoffInfo["node-1"].duration)
		})
	}
}

func TestNodeBasedExponentialBackoff_RemoveStaleBackoffData(t *testing.T) {
	minBackoff := 2 * time.Second
	maxBackoff := 5 * time.Second
	resetTimeout := 10 * time.Second
	t0 := time.Now()

	tests := []struct {
		name                 string
		lastFailedExecution  time.Time
		currentTime          time.Time
		expectBackoffRemoved bool
	}{
		{
			name:                 "not stale",
			lastFailedExecution:  t0,
			currentTime:          t0.Add(resetTimeout).Add(-time.Second),
			expectBackoffRemoved: false,
		},
		{
			name:                 "stale",
			lastFailedExecution:  t0,
			currentTime:          t0.Add(resetTimeout).Add(time.Second),
			expectBackoffRemoved: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := NewNodeBasedExponentialBackoff(minBackoff, maxBackoff, resetTimeout, false)

			b.backoffInfo["node-1"] = nodeBackoffInfo{
				duration:            minBackoff,
				backoffUntil:        tc.lastFailedExecution.Add(minBackoff),
				lastFailedExecution: tc.lastFailedExecution,
			}

			b.RemoveStaleBackoffData(tc.currentTime)

			_, found := b.backoffInfo["node-1"]
			assert.Equal(t, !tc.expectBackoffRemoved, found)
		})
	}
}

func TestNodeBasedExponentialBackoff_BackoffStatus(t *testing.T) {
	minBackoff := 2 * time.Second
	maxBackoff := 5 * time.Second
	resetTimeout := 10 * time.Second
	t0 := time.Now()

	nodeGroup := testprovider.NewTestNodeGroup("mig1", 1, 10, 1, true, false, "e2-standard-2", nil, nil)
	nodeInfo := framework.NewTestNodeInfo(&apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}})
	errInfo := cloudprovider.InstanceErrorInfo{ErrorCode: "ERROR"}

	b := NewNodeBasedExponentialBackoff(minBackoff, maxBackoff, resetTimeout, false)

	// Before any backoff
	status := b.BackoffStatus(nodeGroup, nodeInfo, t0)
	assert.False(t, status.IsBackedOff)

	// Trigger backoff
	until := b.Backoff(nodeGroup, nodeInfo, errInfo, t0)

	// Right after backoff
	status = b.BackoffStatus(nodeGroup, nodeInfo, t0)
	assert.True(t, status.IsBackedOff)
	assert.Equal(t, errInfo, status.ErrorInfo)

	// After backoff expires
	status = b.BackoffStatus(nodeGroup, nodeInfo, until.Add(time.Second))
	assert.False(t, status.IsBackedOff)
}

func TestNodeBasedExponentialBackoff_RemoveBackoff(t *testing.T) {
	minBackoff := 2 * time.Second
	maxBackoff := 5 * time.Second
	resetTimeout := 10 * time.Second
	t0 := time.Now()

	nodeGroup := testprovider.NewTestNodeGroup("mig1", 1, 10, 1, true, false, "e2-standard-2", nil, nil)
	nodeInfo := framework.NewTestNodeInfo(&apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}})

	b := NewNodeBasedExponentialBackoff(minBackoff, maxBackoff, resetTimeout, false)

	// Trigger backoff
	b.Backoff(nodeGroup, nodeInfo, cloudprovider.InstanceErrorInfo{}, t0)

	_, found := b.backoffInfo["node-1"]
	assert.True(t, found)

	// Remove backoff
	b.RemoveBackoff(nodeGroup, nodeInfo)

	_, found = b.backoffInfo["node-1"]
	assert.False(t, found)
}

func TestNodeBasedExponentialBackoff_Backoff_WithJitter(t *testing.T) {
	minBackoff := 10 * time.Second
	maxBackoff := 60 * time.Second
	resetTimeout := 100 * time.Second
	t0 := time.Now()

	nodeGroup := testprovider.NewTestNodeGroup("mig1", 1, 10, 1, true, false, "e2-standard-2", nil, nil)
	nodeInfo := framework.NewTestNodeInfo(&apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}})

	// Test that jitter increases backoff duration within [duration, 2*duration)
	b := NewNodeBasedExponentialBackoff(minBackoff, maxBackoff, resetTimeout, true)

	for i := 0; i < 100; i++ {
		b.backoffInfo["node-1"] = nodeBackoffInfo{
			duration:            minBackoff,
			backoffUntil:        t0.Add(-time.Second),
			lastFailedExecution: t0.Add(-time.Second),
		}
		until := b.Backoff(nodeGroup, nodeInfo, cloudprovider.InstanceErrorInfo{}, t0)
		dur := b.backoffInfo["node-1"].duration

		assert.True(t, dur >= minBackoff, "duration %v should be >= minBackoff %v", dur, minBackoff)
		assert.True(t, dur < 2*minBackoff, "duration %v should be < 2*minBackoff %v", dur, 2*minBackoff)
		assert.Equal(t, t0.Add(dur), until)
	}
}
