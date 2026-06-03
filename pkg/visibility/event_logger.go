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

package visibility

import (
	"github.com/stretchr/testify/mock"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/proto"
	vispb "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/proto"
)

// EventLogger is capable of logging visibility events.
type EventLogger interface {
	Close() error
	LogEvent(event *proto.AutoscalerEvent) error
	LogEventWithDefaults(event *proto.AutoscalerEvent) error
}

// NoOpEventLogger is a no-op implementation of the EventLogger interface.
type NoOpEventLogger struct {
}

// Close is a no-op.
func (l *NoOpEventLogger) Close() error {
	return nil
}

// LogEvent is a no-op.
func (l *NoOpEventLogger) LogEvent(event *proto.AutoscalerEvent) error {
	return nil
}

// LogEventWithDefaults is a no-op.
func (l *NoOpEventLogger) LogEventWithDefaults(event *proto.AutoscalerEvent) error {
	return nil
}

// NewNoOpEventLogger creates and returns a NoOpEventLogger.
func NewNoOpEventLogger() EventLogger {
	return &NoOpEventLogger{}
}

// MockEventLogger is a mock of the EventLogger interface.
type MockEventLogger struct {
	mock.Mock
}

// Close is a mocked method.
func (l *MockEventLogger) Close() error {
	args := l.Called()
	return args.Error(0)
}

// LogEvent is a mocked method.
func (l *MockEventLogger) LogEvent(event *vispb.AutoscalerEvent) error {
	args := l.Called(event)
	return args.Error(0)
}

// LogEventWithDefaults is a mocked method.
func (l *MockEventLogger) LogEventWithDefaults(event *vispb.AutoscalerEvent) error {
	args := l.Called(event)
	return args.Error(0)
}
