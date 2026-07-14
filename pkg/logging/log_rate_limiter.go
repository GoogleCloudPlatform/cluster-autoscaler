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

package logging

import (
	"sync/atomic"
	"time"

	"k8s.io/client-go/util/flowcontrol"
	klog "k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

// LogRateLimiter implements log limiting using the
// Kubernetes flowcontrol package.
type LogRateLimiter struct {
	limiter flowcontrol.PassiveRateLimiter
	dropped atomic.Uint64
}

// NewLogRateLimiter creates a new limiter.
// 'limit' is the number of logs allowed in the 'window' (this is the 'burst').
// 'window' is the duration to spread that burst over (this defines the 'qps').
func NewLogRateLimiter(limit int, window time.Duration) *LogRateLimiter {
	return newLogRateLimiterWithClock(limit, window, clock.RealClock{})
}
func newLogRateLimiterWithClock(limit int, window time.Duration, c clock.PassiveClock) *LogRateLimiter {
	windowSec := window.Seconds()
	// Allow sub-second windows
	if windowSec < 0.001 {
		windowSec = 0.001
		klog.Warningf("Window duration %v is too small, using 1ms instead", windowSec)
	}

	qps := float32(float64(limit) / windowSec)
	burst := limit
	if burst == 0 {
		burst = 1
		klog.Warningf("Burst %v is too small, using 1 instead", burst)
	}

	l := flowcontrol.NewTokenBucketPassiveRateLimiterWithClock(qps, burst, c)

	return &LogRateLimiter{
		limiter: l,
	}
}

// Logf attempts to log. If the token bucket is empty, it drops the log.
func (t *LogRateLimiter) Logf(level klog.Level, format string, args ...any) {
	if !klog.V(level).Enabled() {
		return
	}

	if t.limiter.TryAccept() {
		klog.V(level).Infof(format, args...)
	} else {
		t.dropped.Add(1)
	}
}

// ReportDrops logs and resets dropped counter
func (t *LogRateLimiter) ReportDrops() uint64 {
	d := t.dropped.Swap(0)
	if d > 0 {
		klog.V(1).Infof("[LogRateLimiter] Dropped %d log entries", d)
	}
	return d
}
