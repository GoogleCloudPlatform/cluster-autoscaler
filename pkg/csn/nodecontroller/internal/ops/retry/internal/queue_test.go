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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	testingclock "k8s.io/utils/clock/testing"
	"k8s.io/utils/set"
)

func TestRetryQueue_FirstToRun(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name        string
		ops         []DelayedOp
		expectOp    DelayedOp
		expectFound bool
	}{
		{
			name:        "empty_queue",
			ops:         []DelayedOp{},
			expectOp:    DelayedOp{},
			expectFound: false,
		},
		{
			name: "single_op",
			ops: []DelayedOp{
				delayedOp(now.Add(10 * time.Minute)),
			},
			expectOp:    delayedOp(now.Add(10 * time.Minute)),
			expectFound: true,
		},
		{
			name: "returns_earliest_with_multiple_ops",
			ops: []DelayedOp{
				delayedOp(now.Add(10 * time.Minute)),
				delayedOp(now.Add(5 * time.Minute)),
				delayedOp(now.Add(15 * time.Minute)),
			},
			expectOp:    delayedOp(now.Add(5 * time.Minute)),
			expectFound: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q := NewRetryQueue()
			for _, op := range tc.ops {
				q.Push(op)
			}

			gotOp, gotFound := q.FirstToRun()
			assert.Equal(t, tc.expectFound, gotFound)
			assert.Equal(t, tc.expectOp, gotOp)
		})
	}
}

func TestRetryQueue_PopReadyToRun(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name        string
		ops         []DelayedOp
		advanceTime time.Duration
		expectReady []DelayedOp
	}{
		{
			name: "none_ready",
			ops: []DelayedOp{
				delayedOp(now.Add(10 * time.Minute)),
			},
			advanceTime: 5 * time.Minute,
			expectReady: nil,
		},
		{
			name: "one_ready",
			ops: []DelayedOp{
				delayedOp(now.Add(10 * time.Minute)),
				delayedOp(now.Add(20 * time.Minute)),
			},
			advanceTime: 15 * time.Minute,
			expectReady: []DelayedOp{
				delayedOp(now.Add(10 * time.Minute)),
			},
		},
		{
			name: "multiple_ready_in_order",
			ops: []DelayedOp{
				delayedOp(now.Add(20 * time.Minute)),
				delayedOp(now.Add(10 * time.Minute)),
				delayedOp(now.Add(15 * time.Minute)),
			},
			advanceTime: 25 * time.Minute,
			expectReady: []DelayedOp{
				delayedOp(now.Add(10 * time.Minute)),
				delayedOp(now.Add(15 * time.Minute)),
				delayedOp(now.Add(20 * time.Minute)),
			},
		},
		{
			name: "not_ready_exactly_at_deadline",
			ops: []DelayedOp{
				{ExecuteAfter: now.Add(10 * time.Minute)},
			},
			advanceTime: 10 * time.Minute,
			expectReady: nil, // clock.Now().After(op.ExecuteAfter) is used
		},
		{
			name: "ready_just_after_deadline",
			ops: []DelayedOp{
				delayedOp(now.Add(10 * time.Minute)),
			},
			advanceTime: 10*time.Minute + 1*time.Second,
			expectReady: []DelayedOp{
				delayedOp(now.Add(10 * time.Minute)),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeClock := testingclock.NewFakeClock(now)
			q := NewRetryQueue()
			for _, op := range tc.ops {
				q.Push(op)
			}

			fakeClock.Step(tc.advanceTime)
			gotReady := q.PopReadyToRun(fakeClock.Now())

			assert.ElementsMatch(t, tc.expectReady, gotReady)
		})
	}
}

func delayedOp(executeAfter time.Time) DelayedOp {
	return DelayedOp{
		Op: ops.Operation{
			MIG: gce.GceRef{
				Project: "some-project",
				Zone:    "some-zone",
				Name:    "some-mig",
			},
			Type:      ops.SuspendOp,
			NodeNames: set.New[string]("node-1", "node-2"),
		},
		ExecuteAfter: executeAfter,
	}
}
