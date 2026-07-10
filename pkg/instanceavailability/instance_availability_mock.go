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
	"github.com/stretchr/testify/mock"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
)

// MockProvider is a mock implementation of the Provider interface for testing purposes.
type MockProvider struct {
	mock.Mock
}

func (m *MockProvider) GetInstanceAvailability(flexibilityScopeKey, instanceConfigKey string) *Snapshot {
	args := m.Called(flexibilityScopeKey, instanceConfigKey)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*Snapshot)
}

func (m *MockProvider) RegisterFlexibilityScope(flexibilityScopeKey string) error {
	args := m.Called(flexibilityScopeKey)
	return args.Error(0)
}

func (m *MockProvider) AwaitInstanceAvailability(flexibilityScopeKey, instanceConfigKey string) (*Snapshot, error) {
	args := m.Called(flexibilityScopeKey, instanceConfigKey)
	if args.Get(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*Snapshot), nil
}

func (m *MockProvider) MarkUsed(flexibilityScopeKey, instanceConfigKey, guidanceId, decisionId string, zonalInstancesToProvision map[string]int) error {
	args := m.Called(flexibilityScopeKey, instanceConfigKey, guidanceId, decisionId, zonalInstancesToProvision)
	return args.Error(0)
}

func (m *MockProvider) IncrementFlexAdvisorCacheQueryCount(result metrics.FACacheQueryResult, flexibilityScopeKey string, instanceConfigKey string) {
	m.Called(result, flexibilityScopeKey, instanceConfigKey)
}
