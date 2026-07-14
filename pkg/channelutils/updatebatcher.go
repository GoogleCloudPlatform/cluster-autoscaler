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
	"sync"
	"time"

	klog "k8s.io/klog/v2"
)

// BatchChannel returns a channel that will receive one message for each batch of messages in the
// input channel. A batch of message in the input channel are messages that were received within
// a batchingWindow.
//
// To achieve this the functions opens the output channel, starts processInput go routine and
// returns the output channel.
//
// When processInput reads a message from the input channel it checks if it should start a new
// batch. If forwardNextItem is false it means the batch already started so processInput ignores the
// message. Otherwise it sets forwardNextItem to false to mark the messages incoming through the
// input channel should be ignored until the batch ends and launches waitAndEmit go routine.
//
// waitAndEmit sleeps for batchingWindow (during which any messages incoming through input channel
// are ignored) then emits a message to output channel and sets forwardNextItem to true (so next
// message received trhough input channel will start a new batch).
func BatchChannel(ctx context.Context, input <-chan any, batchingWindow time.Duration) <-chan any {
	output := make(chan any)
	processInput := func() {
		klog.Info("Goroutine BatchChannel.processInput starts")
		m := &sync.Mutex{} // Guards forwardNextItem
		forwardNextItem := true
		for {
			select {
			case <-input:
				m.Lock()
				if forwardNextItem {
					forwardNextItem = false
					waitAndEmit := func() {
						klog.Info("Goroutine BatchChannel.processInput.waitAndEmit starts")
						time.Sleep(batchingWindow)
						output <- struct{}{}
						m.Lock()
						forwardNextItem = true
						m.Unlock()
					}
					go waitAndEmit()
				}
				m.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}
	go processInput()
	return output
}
