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
	"flag"
	"sync"
	"testing"
	"time"

	klog "k8s.io/klog/v2"
	clock "k8s.io/utils/clock/testing"
)

func init() {
	klog.InitFlags(nil)
	_ = flag.Set("v", "6")
}

func TestLogRateLimiter_BasicLimit(t *testing.T) {
	t.Parallel()
	limit := 5
	window := 100 * time.Millisecond
	// QPS = 5 / 0.1s = 50
	fakeClock := clock.NewFakeClock(time.Now())
	l := newLogRateLimiterWithClock(limit, window, fakeClock)

	// Log 5 times, these should all be allowed
	for i := 0; i < limit; i++ {
		l.Logf(1, "allowed %d", i)
	}

	// Check that no drops have occurred yet
	if l.dropped.Load() != 0 {
		t.Fatalf("expected 0 drops, got %d", l.dropped.Load())
	}

	// Next log should be dropped
	l.Logf(1, "dropped")
	if l.dropped.Load() != 1 {
		t.Fatalf("expected 1 drop, got %d", l.dropped.Load())
	}
}

func TestLogRateLimiter_ResetAfterWindow(t *testing.T) {
	t.Parallel()
	limit := 3
	window := 50 * time.Millisecond
	// QPS = 3 / 0.05s = 60
	fakeClock := clock.NewFakeClock(time.Now())
	l := newLogRateLimiterWithClock(limit, window, fakeClock)

	// Log 3 times, consuming the limit
	for i := 0; i < limit; i++ {
		l.Logf(1, "window1 log %d", i)
	}
	if l.dropped.Load() != 0 {
		t.Fatalf("expected 0 drops in window 1, got %d", l.dropped.Load())
	}

	// Advance the clock 1.1s.
	// The bucket will refill 1.1s * 60/s = 66 tokens.
	// It's capped at the burst size of 3.
	fakeClock.Step(1100 * time.Millisecond)

	// Log 3 more times. These should be allowed by the refilled bucket.
	for i := 0; i < limit; i++ {
		l.Logf(1, "window2 log %d", i)
	}

	// The drop count should still be 0
	if l.dropped.Load() != 0 {
		t.Fatalf("expected 0 total drops, got %d", l.dropped.Load())
	}

	// This one should be dropped
	l.Logf(1, "window2 drop")
	if l.dropped.Load() != 1 {
		t.Fatalf("expected 1 total drop, got %d", l.dropped.Load())
	}
}

func TestLogRateLimiter_ReportDrops(t *testing.T) {
	t.Parallel()
	fakeClock := clock.NewFakeClock(time.Now())
	l := newLogRateLimiterWithClock(1, 100*time.Millisecond, fakeClock)

	l.Logf(1, "ok")
	l.Logf(1, "should be dropped")

	d := l.ReportDrops()
	if d != 1 {
		t.Fatalf("expected dropped=1, got %d", d)
	}

	if l.dropped.Load() != 0 {
		t.Fatalf("expected dropped counter reset to 0, got %d", l.dropped.Load())
	}
}

func TestLogRateLimiter_Concurrent(t *testing.T) {
	t.Parallel()
	limit := 100
	window := 100 * time.Millisecond
	// QPS = 100 / 0.1s = 1000
	fakeClock := clock.NewFakeClock(time.Now())
	l := newLogRateLimiterWithClock(limit, window, fakeClock)

	var wg sync.WaitGroup
	const goroutines = 500
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			l.Logf(1, "concurrent log %d", i)
		}(i)
	}

	wg.Wait()

	// 500 concurrent logs hit a bucket at the same time.
	// The first 100 get the burst. The rest are dropped.
	// No refilling happens, because the clock is frozen.
	expectedDrops := uint64(goroutines - limit)
	if l.dropped.Load() != expectedDrops {
		t.Fatalf("expected %d drops, got %d", expectedDrops, l.dropped.Load())
	}
}

func TestLogRateLimiter_ConcurrentRefill(t *testing.T) {
	t.Parallel()
	limit := 50
	window := 100 * time.Millisecond
	// QPS = 50 / 0.1s = 500
	fakeClock := clock.NewFakeClock(time.Now())
	l := newLogRateLimiterWithClock(limit, window, fakeClock)

	var wg sync.WaitGroup

	for r := 0; r < 3; r++ {
		// Sleep long enough for the bucket to fully refill
		// to its burst capacity of 50.
		fakeClock.Step(1100 * time.Millisecond)

		const goroutines = 500
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				l.Logf(1, "race log")
			}()
		}
		wg.Wait()

		// 500 goroutines hit at the same time.
		// The first 50 take the refilled burst.
		// The other 450 are dropped. The clock doesn't advance
		// during the Wait(), so no extra tokens are added.
		expectedDrops := uint64(goroutines - limit)

		drops := l.dropped.Swap(0)

		if drops != expectedDrops {
			t.Fatalf("round %d: expected %d drops, got %d", r, expectedDrops, drops)
		}
		t.Logf("Round %d: OK (Dropped: %d)", r, drops)
	}
}
