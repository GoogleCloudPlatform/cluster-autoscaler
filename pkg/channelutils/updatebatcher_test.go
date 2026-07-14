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

package channelutils

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestBatchChannel aims to test that we successfully dethrottle the channel and the output channel have a rate of ~1/max(windowWidth, inputSpacing)
func TestBatchChannel(t *testing.T) {
	tests := []struct {
		name             string
		windowWidth      time.Duration
		inputSpacing     time.Duration
		signalCount      int
		expectFullLedger []time.Duration
	}{
		{
			"10/s to 1/s - windowWidth >> inputSpacing, output signals are generated ~(windowWidth) sec apart",
			1050 * time.Millisecond,
			100 * time.Millisecond,
			37,
			[]time.Duration{0, 1150 * time.Millisecond, 2250 * time.Millisecond, 3350 * time.Millisecond, 4450 * time.Millisecond},
		},
		{
			"1/s to 10/s - windowWidth << inputSpacing, output signals are generated after the windowWidth sec after the last received signal",
			100 * time.Millisecond,
			time.Second,
			3,
			[]time.Duration{0, 1100 * time.Millisecond, 2100 * time.Millisecond, 3100 * time.Millisecond},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				input := make(chan any)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				output := BatchChannel(ctx, input, tc.windowWidth)

				// Log events output by the batcher.
				var ledger []time.Time
				done := make(chan struct{})

				go func() {
					defer close(done)
					for {
						select {
						case <-output:
							// time.Now() is virtualized and deterministic here.
							ledger = append(ledger, time.Now())
						case <-ctx.Done():
							return
						}
					}
				}()
				// synctest.Wait() to ensure that background goroutine is actively blocked on the select.
				synctest.Wait()
				// Record the start time.
				startTime := time.Now()
				fullLedger := []time.Time{startTime}

				// Generate input signals with specific spacing.
				for i := 0; i < tc.signalCount; i++ {
					// Advance the virtual clock by tc.inputSpacing.
					time.Sleep(tc.inputSpacing)
					// Send the signal.
					input <- struct{}{}
					// synctest.Wait() ensures the batcher goroutine processes the signal.
					synctest.Wait()
				}

				// Wait enough time to ensure the final batch is emitted.
				time.Sleep(2 * tc.windowWidth)
				synctest.Wait()
				cancel()
				<-done

				fullLedger = append(fullLedger, ledger...)

				assert.Equal(t, len(tc.expectFullLedger), len(fullLedger), "Batcher output unexpected number of signals")

				// Verify that each signal matches the expected exact virtual time duration
				for i := 0; i < len(fullLedger) && i < len(tc.expectFullLedger); i++ {
					dur := fullLedger[i].Sub(startTime)
					assert.Equal(t, tc.expectFullLedger[i], dur, "Signal %d occurred at %v, expected %v", i, dur, tc.expectFullLedger[i])
				}
			})
		})
	}
}

// TestBatchChannel_BlockedChannelReader aims to test that batchChannel is effectively blocked if no goroutine is reading from it.
func TestBatchChannel_BlockedChannelReader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		input := make(chan any)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		output := BatchChannel(ctx, input, 1*time.Second)

		// Send multiple inputs to simulate continuous sending - send 10000 signals each 1 ms apart
		for i := 0; i < 10000; i++ {
			// Advance the virtual clock by tc.inputSpacing.
			time.Sleep(time.Millisecond)
			// Send the signal.
			input <- struct{}{}
			// synctest.Wait() ensures the batcher goroutine processes the signal.
			synctest.Wait()
		}
		// Read one signal from the blocked channel.
		batchedSignals := 0

	batchedSignalsCounter:
		for {
			select {
			case <-output:
				batchedSignals += 1
			default:
				break batchedSignalsCounter
			}
		}

		// When channel is blocked because nothing is reading from it batcher should keep ignoring incoming messages.
		if batchedSignals != 1 {
			t.Errorf("BatchChannel output %d signals, wanted 1", batchedSignals)
		}
		// Verify that exactly 1 signal was emitted despite many inputs,
		// as the batcher waits for previous emission to finish or drops inputs.
		assert.Equal(t, 1, batchedSignals, "BatchChannel should have output exactly 1 signal after being blocked")

	})
}
