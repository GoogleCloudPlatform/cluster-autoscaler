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

package fleetefficiency

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestIsFleetEfficiencyEnabled(t *testing.T) {
	tests := []struct {
		name                                    string
		gceFlexAdvisorEnabled                   bool
		flexAdvisorProcessingEnabled            bool
		fleetEfficiencyStrategyEnabledFlag      bool
		fleetEfficiencyStrategyMinCAVersionFlag bool
		expected                                bool
	}{
		{
			name:                                    "all enabled/true",
			gceFlexAdvisorEnabled:                   true,
			flexAdvisorProcessingEnabled:            true,
			fleetEfficiencyStrategyEnabledFlag:      true,
			fleetEfficiencyStrategyMinCAVersionFlag: true,
			expected:                                true,
		},
		{
			name:                                    "gceFlexAdvisorEnabled false",
			gceFlexAdvisorEnabled:                   false,
			flexAdvisorProcessingEnabled:            true,
			fleetEfficiencyStrategyEnabledFlag:      true,
			fleetEfficiencyStrategyMinCAVersionFlag: true,
			expected:                                false,
		},
		{
			name:                                    "flexAdvisorProcessingEnabled false",
			gceFlexAdvisorEnabled:                   true,
			flexAdvisorProcessingEnabled:            false,
			fleetEfficiencyStrategyEnabledFlag:      true,
			fleetEfficiencyStrategyMinCAVersionFlag: true,
			expected:                                false,
		},
		{
			name:                                    "fleetEfficiencyStrategyEnabledFlag false",
			gceFlexAdvisorEnabled:                   true,
			flexAdvisorProcessingEnabled:            true,
			fleetEfficiencyStrategyEnabledFlag:      false,
			fleetEfficiencyStrategyMinCAVersionFlag: true,
			expected:                                false,
		},
		{
			name:                                    "fleetEfficiencyStrategyMinCAVersionFlag false",
			gceFlexAdvisorEnabled:                   true,
			flexAdvisorProcessingEnabled:            true,
			fleetEfficiencyStrategyEnabledFlag:      true,
			fleetEfficiencyStrategyMinCAVersionFlag: false,
			expected:                                false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			boolFlags := map[string]bool{
				experiments.FleetEfficiencyStrategyEnabledFlag:      tc.fleetEfficiencyStrategyEnabledFlag,
				experiments.FleetEfficiencyStrategyMinCAVersionFlag: tc.fleetEfficiencyStrategyMinCAVersionFlag,
				experiments.FlexAdvisorProcessingEnabledFlag:        tc.flexAdvisorProcessingEnabled,
			}
			manager := experiments.NewMockManagerWithOptions(version.Version{}, boolFlags, nil)

			assert.Equal(t, tc.expected, IsFleetEfficiencyEnabled(tc.gceFlexAdvisorEnabled, manager))
		})
	}

	t.Run("nil manager", func(t *testing.T) {
		assert.False(t, IsFleetEfficiencyEnabled(true, nil))
	})

	t.Run("default manager", func(t *testing.T) {
		manager := experiments.NewMockManager()
		assert.True(t, IsFleetEfficiencyEnabled(true, manager))
	})
}
