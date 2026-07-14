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

package experiments

import (
	"testing"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
)

func TestIsDemandFungibilityImpactTrackingEnabled(t *testing.T) {
	testCases := []struct {
		name     string
		manager  Manager
		expected bool
	}{
		{
			name:     "nil manager defaults to false",
			manager:  nil,
			expected: false,
		},
		{
			name:     "eval bool flag returns false",
			manager:  NewMockManager(),
			expected: false,
		},
		{
			name: "eval bool flag true, min version returns false",
			manager: NewMockManagerWithOptions(
				version.Version{},
				map[string]bool{
					DemandFungibilityImpactTrackingEnabledFlag: true,
				},
				map[string]string{
					DemandFungibilityImpactTrackingMinCAVersionFlag: "1.20.0-gke.100",
				},
			),
			expected: false,
		},
		{
			name: "eval bool flag true, min version true",
			manager: NewMockManagerWithOptions(
				version.Version{1, 21, 0, 100},
				map[string]bool{
					DemandFungibilityImpactTrackingEnabledFlag: true,
				},
				map[string]string{
					DemandFungibilityImpactTrackingMinCAVersionFlag: "1.20.0-gke.100",
				},
			),
			expected: true,
		},
		{
			name: "eval bool flag false, min version true",
			manager: NewMockManagerWithOptions(
				version.Version{1, 21, 0, 100},
				map[string]bool{
					DemandFungibilityImpactTrackingEnabledFlag: false,
				},
				map[string]string{
					DemandFungibilityImpactTrackingMinCAVersionFlag: "1.20.0-gke.100",
				},
			),
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := IsDemandFungibilityImpactTrackingEnabled(tc.manager)
			if result != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}
		})
	}
}
