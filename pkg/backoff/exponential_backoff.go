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
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
)

// exponentialBackoff allows to backoff the NAP to create a new nodes.
// It is meant to be used as a shared implementation of backoff.
type exponentialBackoff struct {
	until           time.Time
	duration        time.Duration
	initialDuration time.Duration
	maxDuration     time.Duration
	resetTime       time.Duration
	reset           time.Time
	errorInfo       cloudprovider.InstanceErrorInfo
}

// NewExponentialBackoff initialises exponentialBackoff.
func NewExponentialBackoff(initialBackoffDuration, maxBackoffDuration, resetTime time.Duration) *exponentialBackoff {
	return &exponentialBackoff{
		duration:        0 * time.Second,
		initialDuration: initialBackoffDuration,
		maxDuration:     maxBackoffDuration,
		resetTime:       resetTime,
	}
}

// Backoff execution. Returns time till execution is backed off.
func (b *exponentialBackoff) Backoff(errorInfo cloudprovider.InstanceErrorInfo, currentTime time.Time) time.Time {
	// don't bump backoff multiple times in a row if there were parallel failed
	// scale up operations.
	if b.until.After(currentTime) {
		return b.until
	}
	if b.duration == 0 {
		b.duration = b.initialDuration
		b.reset = currentTime.Add(b.resetTime)
	} else {
		b.duration = 2 * b.duration
		if b.duration > b.maxDuration {
			b.duration = b.maxDuration
		}
	}
	b.until = currentTime.Add(b.duration)
	b.errorInfo = errorInfo
	return b.until
}

// RemoveStaleBackoffData removes stale backoff data.
func (b *exponentialBackoff) RemoveStaleBackoffData(currentTime time.Time) {
	if currentTime.Before(b.reset) {
		return
	}
	b.duration = 0
	b.until = time.Time{}
}

// BackoffUntil returns the time until which the execution is backed off.
func (b *exponentialBackoff) BackoffUntil() time.Time {
	return b.until
}
