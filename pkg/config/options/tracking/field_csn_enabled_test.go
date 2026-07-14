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

package tracking

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func getCSNEnabledTestCases() []struct {
	testName         string
	autopilotEnabled bool
	csnCAFlag        internalopts.CSNStatus
	experimentValues map[string]bool
	wantValue        bool
} {
	return []struct {
		testName         string
		autopilotEnabled bool
		csnCAFlag        internalopts.CSNStatus
		experimentValues map[string]bool
		wantValue        bool
	}{
		{
			testName:         "Autopilot enabled overrides all features and forces disable when SoHW flag is false",
			autopilotEnabled: true,
			csnCAFlag:        internalopts.CSNEnabled,
			experimentValues: map[string]bool{
				experiments.ColdStandbyNodesInternalMinCAVersionFlag: true,
				experiments.ColdStandbyNodesMinCAVersionFlag:         true,
				experiments.ColdStandbyNodesAutopilotSoHWFlag:        false,
			},
			wantValue: false,
		},
		{
			testName:         "Autopilot enabled does not override features when SoHW flag is true",
			autopilotEnabled: true,
			csnCAFlag:        internalopts.CSNEnabled,
			experimentValues: map[string]bool{
				experiments.ColdStandbyNodesInternalMinCAVersionFlag: true,
				experiments.ColdStandbyNodesMinCAVersionFlag:         true,
				experiments.ColdStandbyNodesAutopilotSoHWFlag:        true,
			},
			wantValue: true,
		},
		{
			testName:         "Autopilot enabled does not override features when SoHW flag is true, but features are disabled",
			autopilotEnabled: true,
			csnCAFlag:        internalopts.CSNUnspecified,
			experimentValues: map[string]bool{
				experiments.ColdStandbyNodesAutopilotSoHWFlag: true,
			},
			wantValue: false,
		},
		{
			testName:         "Autopilot disabled and no experiments exist, csnStatusCAFlag unspecified",
			autopilotEnabled: false,
			csnCAFlag:        internalopts.CSNUnspecified,
			experimentValues: nil,
			wantValue:        false,
		},
		{
			testName:         "Autopilot disabled and no experiments exist, csnStatusCAFlag enabled",
			autopilotEnabled: false,
			csnCAFlag:        internalopts.CSNEnabled,
			experimentValues: nil,
			wantValue:        true,
		},
		{
			testName:         "Autopilot disabled and both experiments enabled, csnStatusCAFlag unspecified",
			autopilotEnabled: false,
			csnCAFlag:        internalopts.CSNUnspecified,
			experimentValues: map[string]bool{
				experiments.ColdStandbyNodesInternalMinCAVersionFlag: true,
				experiments.ColdStandbyNodesMinCAVersionFlag:         true,
			},
			wantValue: true,
		},
		{
			testName:         "Autopilot disabled and both experiments enabled, csnStatusCAFlag disabled",
			autopilotEnabled: false,
			csnCAFlag:        internalopts.CSNDisabled,
			experimentValues: map[string]bool{
				experiments.ColdStandbyNodesInternalMinCAVersionFlag: true,
				experiments.ColdStandbyNodesMinCAVersionFlag:         true,
			},
			wantValue: false,
		},
		{
			testName:         "Autopilot disabled and only internal experiment enabled, csnStatusCAFlag unspecified",
			autopilotEnabled: false,
			csnCAFlag:        internalopts.CSNUnspecified,
			experimentValues: map[string]bool{
				experiments.ColdStandbyNodesInternalMinCAVersionFlag: true,
			},
			wantValue: true,
		},
		{
			testName:         "Autopilot disabled and only public experiment enabled, csnStatusCAFlag unspecified",
			autopilotEnabled: false,
			csnCAFlag:        internalopts.CSNUnspecified,
			experimentValues: map[string]bool{
				experiments.ColdStandbyNodesMinCAVersionFlag: true,
			},
			wantValue: true,
		},
		{
			testName:         "Autopilot disabled, csnStatusCAFlag enabled, flag guard false",
			autopilotEnabled: false,
			csnCAFlag:        internalopts.CSNEnabled,
			experimentValues: map[string]bool{
				experiments.ColdStandbyNodesMinCAVersionGuardForCAFlag: false,
			},
			wantValue: false,
		},
		{
			testName:         "Autopilot disabled, csnStatusCAFlag enabled, flag guard true",
			autopilotEnabled: false,
			csnCAFlag:        internalopts.CSNEnabled,
			experimentValues: map[string]bool{
				experiments.ColdStandbyNodesMinCAVersionGuardForCAFlag: true,
			},
			wantValue: true,
		},
		{
			testName:         "Autopilot disabled, csnStatusCAFlag enabled, the main experiment enabled, flag guard false",
			autopilotEnabled: false,
			csnCAFlag:        internalopts.CSNEnabled,
			experimentValues: map[string]bool{
				experiments.ColdStandbyNodesMinCAVersionFlag:           true,
				experiments.ColdStandbyNodesMinCAVersionGuardForCAFlag: false,
			},
			wantValue: true,
		},
		{
			testName:         "Autopilot disabled, csnStatusCAFlag disabled, the main experiment enabled, flag guard false",
			autopilotEnabled: false,
			csnCAFlag:        internalopts.CSNDisabled,
			experimentValues: map[string]bool{
				experiments.ColdStandbyNodesMinCAVersionFlag:           true,
				experiments.ColdStandbyNodesMinCAVersionGuardForCAFlag: false,
			},
			wantValue: true,
		},
	}
}

func TestCSNEnabledFieldSetValue(t *testing.T) {
	for _, tc := range getCSNEnabledTestCases() {
		t.Run(tc.testName, func(t *testing.T) {
			optsFromFlags := internalopts.AutoscalingOptions{}
			optsFromFlags.AutopilotEnabled = tc.autopilotEnabled
			optsFromFlags.CSNCAFlag = tc.csnCAFlag
			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, tc.experimentValues, nil)

			optsToModify := internalopts.AutoscalingOptions{}
			csnEnabledField.setValue(optsFromFlags, experimentsManager, &optsToModify)
			gotValue := optsToModify.CSNEnabled

			// Assert that the field was modified as expected in the provided AutoscalingOptions.
			assert.Equal(t, tc.wantValue, gotValue)

			// Assert that getValueStr() works correctly.
			assert.Equal(t, fmt.Sprintf("%v", tc.wantValue), csnEnabledField.getValueStr(optsToModify))
		})
	}
}
