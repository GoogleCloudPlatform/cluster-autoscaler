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

package recorder

import (
	"strings"
	"testing"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/core"
	"k8s.io/client-go/tools/record"
)

const defaultBufferSize = 100

// SetupFakeRecorder creates a new FakeRecorder with a default buffer size
func SetupFakeRecorder(autoscaler *core.StaticAutoscaler) *record.FakeRecorder {
	return record.NewFakeRecorder(defaultBufferSize)
}

// AssertEventContains reads currently available events from the fake recorder
// and asserts that at least one of them contains the expected substring within
// the specified timeout.
// Note: This consumes the events from the recorder's channel.
func AssertEventsContains(t *testing.T, rec record.EventRecorder, expectedSubstring string, timeout time.Duration) {
	t.Helper()
	fakeRecorder, ok := rec.(*record.FakeRecorder)
	if !ok {
		t.Fatalf("recorder must be of type *record.FakeRecorder, got %T", rec)
	}
	if fakeRecorder == nil {
		t.Fatal("recorder must not be nil")
	}

	var receivedEvents []string
	timeoutCh := time.After(timeout)

	for {
		select {
		case e := <-fakeRecorder.Events:
			receivedEvents = append(receivedEvents, e)
			if strings.Contains(e, expectedSubstring) {
				return
			}
		case <-timeoutCh:
			t.Fatalf("Timeout waiting for event containing %q.\nReceived events: %v", expectedSubstring, receivedEvents)
		}
	}
}
