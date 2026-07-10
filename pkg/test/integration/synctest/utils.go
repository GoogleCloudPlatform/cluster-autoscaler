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

package synctest

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/autoscaler/cluster-autoscaler/core"
)

// EnableLargeWatchChannel overrides the default watcher channel size in client-go.
// This is necessary for large scale tests (5k+ nodes) under synctest.
// When running in synctest, background informers are often paused while CA parallel workers
// (like node tainting) execute. If the channel is too small, it panics with "channel full".
// It returns a cleanup function that restores the original value.
func EnableLargeWatchChannel() func() {
	original := watch.DefaultChanSize
	watch.DefaultChanSize = 100000
	return func() {
		watch.DefaultChanSize = original
	}
}

// RunOnceAfter advances the virtual clock by the specified duration and then
// executes a single Cluster Autoscaler cycle.
func RunOnceAfter(t *testing.T, autoscaler core.Autoscaler, d time.Duration) error {
	t.Helper()

	// Ensure any pending work is done before changing the time.
	synctest.Wait()

	time.Sleep(d)
	err := autoscaler.RunOnce(time.Now())

	// Let side-effects of the RunOnce finish.
	synctest.Wait()
	return err
}

// MustRunOnceAfter is a helper that calls RunOnceAfter and
// immediately fails the test if an error occurs.
// Use this for "happy path" simulation steps.
func MustRunOnceAfter(t *testing.T, autoscaler core.Autoscaler, d time.Duration) {
	t.Helper()
	err := RunOnceAfter(t, autoscaler, d)
	assert.NoError(t, err)
}

// TearDown is a helper to tear down the context and drain the synctest bubble.
func TearDown(cancel context.CancelFunc) {
	cancel()
	// Synctest drain: Background goroutines (like MetricAsyncRecorder) often use uninterruptible time.Sleep loops.
	// In a synctest bubble, these are "durable" sleeps. We must advance the virtual clock to allow these goroutines to wake up, observe the
	// closed context channel, and terminate gracefully.
	time.Sleep(1 * time.Minute)
	synctest.Wait()
}

// RealTimeClock allows fetching the real wall-clock time from inside a synctest bubble.
// Synctest artificially fast-forwards time.Now(), so this utility uses a background
// goroutine outside the bubble communicating via channels to provide the true time.
type RealTimeClock struct {
	req  chan struct{}
	resp chan time.Time
}

// NewRealTimeClock creates and starts a RealTimeClock. It MUST be called
// outside the synctest.Test() bubble.
func NewRealTimeClock() *RealTimeClock {
	c := &RealTimeClock{
		req:  make(chan struct{}),
		resp: make(chan time.Time),
	}
	go func() {
		for {
			_, ok := <-c.req
			if !ok {
				return
			}
			c.resp <- time.Now()
		}
	}()
	return c
}

// Stop terminates the RealTimeClock's background goroutine.
func (c *RealTimeClock) Stop() {
	close(c.req)
}

// Now returns the real wall-clock time. It can be called safely from within
// the synctest.Test() bubble.
func (c *RealTimeClock) Now() time.Time {
	c.req <- struct{}{}
	return <-c.resp
}
