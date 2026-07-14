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

package fake

import (
	"sync"

	vispb "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/proto"
)

// EventLogger is a thread-safe fake implementation of visibility.EventLogger for testing.
type EventLogger struct {
	mu     sync.Mutex
	events []*vispb.AutoscalerEvent
}

// NewEventLogger creates a new fake EventLogger.
func NewEventLogger() *EventLogger {
	return &EventLogger{}
}

// Close is a no-op.
func (l *EventLogger) Close() error {
	return nil
}

// LogEvent appends an event to the internal event slice.
func (l *EventLogger) LogEvent(event *vispb.AutoscalerEvent) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event)
	return nil
}

// LogEventWithDefaults appends an event to the internal event slice.
func (l *EventLogger) LogEventWithDefaults(event *vispb.AutoscalerEvent) error {
	return l.LogEvent(event)
}

// Clear removes all recorded events.
func (l *EventLogger) Clear() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = nil
}

// Events returns a copy of all recorded visibility events.
func (l *EventLogger) Events() []*vispb.AutoscalerEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	eventsCopy := make([]*vispb.AutoscalerEvent, len(l.events))
	copy(eventsCopy, l.events)
	return eventsCopy
}

// NoScaleUpEvents returns all NoScaleUpData payloads from the recorded events.
func (l *EventLogger) NoScaleUpEvents() []*vispb.NoScaleUpData {
	events := l.Events()
	var res []*vispb.NoScaleUpData
	for _, event := range events {
		noDecisionStatus := event.GetNoDecisionStatus()
		if noDecisionStatus == nil {
			continue
		}
		noScaleUp := noDecisionStatus.GetNoScaleUp()
		if noScaleUp == nil {
			continue
		}
		res = append(res, noScaleUp)
	}
	return res
}
