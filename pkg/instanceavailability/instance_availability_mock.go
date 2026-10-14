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

package instanceavailability

import (
	"time"

	"github.com/stretchr/testify/mock"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
)

// MockProvider is a mock implementation of the Provider interface for testing purposes.
type MockProvider struct {
	mock.Mock
}

func (m *MockProvider) GetAvailabilitySnapshot(flexibilityScopeKey, instanceConfigKey string) (*Snapshot, error) {
	return snapshotFromMockArgs(m.Called(flexibilityScopeKey, instanceConfigKey))
}

func (m *MockProvider) RegisterFlexibilityScope(flexibilityScopeKey string) error {
	args := m.Called(flexibilityScopeKey)
	return args.Error(0)
}

func (m *MockProvider) BumpScopeTtl(flexibilityScopeKey string) error {
	args := m.Called(flexibilityScopeKey)
	return args.Error(0)
}

func (m *MockProvider) AwaitAvailabilitySnapshotWithTimeout(flexibilityScopeKey, instanceConfigKey string, timeout time.Duration) (*Snapshot, error) {
	return snapshotFromMockArgs(m.Called(flexibilityScopeKey, instanceConfigKey, timeout))
}

func snapshotFromMockArgs(args mock.Arguments) (*Snapshot, error) {
	if err := args.Error(1); err != nil {
		return nil, err
	}
	snapshot, ok := args.Get(0).(*Snapshot)
	if !ok || snapshot == nil {
		panic("MockProvider: cannot return (nil, nil); either Snapshot or error must be non-nil")
	}
	return snapshot, nil
}

func (m *MockProvider) MarkUsed(flexibilityScopeKey, instanceConfigKey, guidanceId, decisionId string, zonalInstancesToProvision map[string]int) error {
	args := m.Called(flexibilityScopeKey, instanceConfigKey, guidanceId, decisionId, zonalInstancesToProvision)
	return args.Error(0)
}

func (m *MockProvider) IncrementFlexAdvisorCacheQueryCount(result metrics.FACacheQueryResult, flexibilityScopeKey string, instanceConfigKey string) {
	m.Called(result, flexibilityScopeKey, instanceConfigKey)
}
