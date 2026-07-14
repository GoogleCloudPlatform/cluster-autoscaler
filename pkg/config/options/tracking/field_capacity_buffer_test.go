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

func getCapacityBuffersTestCases() []struct {
	testName         string
	flagValue        bool
	autopilotEnabled bool
	experimentValues map[string]bool
	wantValue        bool
} {
	return []struct {
		testName         string
		flagValue        bool
		autopilotEnabled bool
		experimentValues map[string]bool
		wantValue        bool
	}{
		// General Scenarios
		{
			testName:         "Autopilot enabled overrides all features and forces disable",
			flagValue:        true,
			autopilotEnabled: true,
			experimentValues: map[string]bool{
				experiments.CapacityBuffersEnabled:                    true,
				experiments.CapacityBuffersPrivatePreviewMinCAVersion: true,
				experiments.CapacityBuffersMinCAVersion:               true,
			},
			wantValue: true,
		},
		{
			testName:         "CLI flag disabled and no experiments exist",
			flagValue:        false,
			experimentValues: nil,
			wantValue:        false,
		},
		{
			testName:         "CLI flag enabled and no experiments exist",
			flagValue:        true,
			experimentValues: nil,
			wantValue:        true,
		},
		// Private Preview Scenarios
		{
			testName:  "Cluster in private preview with CLI flag disabled",
			flagValue: false,
			experimentValues: map[string]bool{
				experiments.CapacityBuffersPrivatePreviewMinCAVersion: true,
			},
			wantValue: true,
		},
		{
			testName:  "Cluster in private preview with experiment disabled and CLI flag disabled",
			flagValue: false,
			experimentValues: map[string]bool{
				experiments.CapacityBuffersPrivatePreviewMinCAVersion: false,
			},
			wantValue: false,
		},
		{
			testName:  "Cluster in private preview with experiment disabled but CLI flag enabled",
			flagValue: true,
			experimentValues: map[string]bool{
				experiments.CapacityBuffersPrivatePreviewMinCAVersion: false,
			},
			wantValue: true,
		},

		// Public Preview Scenarios
		{
			testName:  "Cluster in public preview with CLI flag enabled",
			flagValue: true,
			experimentValues: map[string]bool{
				experiments.CapacityBuffersEnabled:                    true,
				experiments.CapacityBuffersPrivatePreviewMinCAVersion: false,
				experiments.CapacityBuffersMinCAVersion:               true,
			},
			wantValue: true,
		},
		{
			testName:  "Cluster in public preview but experiment toggle disabled",
			flagValue: true,
			experimentValues: map[string]bool{
				experiments.CapacityBuffersEnabled:                    false,
				experiments.CapacityBuffersPrivatePreviewMinCAVersion: false,
				experiments.CapacityBuffersMinCAVersion:               true,
			},
			wantValue: false,
		},
		{
			testName:  "Cluster in public preview with older unsupported version",
			flagValue: true,
			experimentValues: map[string]bool{
				experiments.CapacityBuffersEnabled:                    true,
				experiments.CapacityBuffersPrivatePreviewMinCAVersion: false,
				experiments.CapacityBuffersMinCAVersion:               false,
			},
			wantValue: false,
		},
		// Mixed Private & Public Preview Scenarios
		{
			testName:  "Cluster in both private and public preview with CLI enabled",
			flagValue: true,
			experimentValues: map[string]bool{
				experiments.CapacityBuffersEnabled:                    true,
				experiments.CapacityBuffersPrivatePreviewMinCAVersion: true,
				experiments.CapacityBuffersMinCAVersion:               true,
			},
			wantValue: true,
		},
		{
			testName:  "Cluster in both private and public preview but global experiment toggle is disabled",
			flagValue: true,
			experimentValues: map[string]bool{
				experiments.CapacityBuffersEnabled:                    false,
				experiments.CapacityBuffersPrivatePreviewMinCAVersion: true,
				experiments.CapacityBuffersMinCAVersion:               true,
			},
			wantValue: false,
		},
		{
			testName:  "Cluster in both previews, public version unsupported but private preview carries it",
			flagValue: true,
			experimentValues: map[string]bool{
				experiments.CapacityBuffersEnabled:                    true,
				experiments.CapacityBuffersPrivatePreviewMinCAVersion: true,
				experiments.CapacityBuffersMinCAVersion:               false,
			},
			wantValue: true,
		},
		{
			testName:  "Only the public-preview version experiment defined",
			flagValue: true,
			experimentValues: map[string]bool{
				experiments.CapacityBuffersMinCAVersion: true,
			},
			wantValue: true,
		},
		{
			testName:  "Only the global boolean experiment defined (true)",
			flagValue: true,
			experimentValues: map[string]bool{
				experiments.CapacityBuffersEnabled: true,
			},
			wantValue: true,
		},
		{
			testName:  "Only the global boolean experiment defined (false acting as kill switch)",
			flagValue: true,
			experimentValues: map[string]bool{
				experiments.CapacityBuffersEnabled: false,
			},
			wantValue: false,
		},
		{
			testName:  "Combination: Public-preview version (false) and boolean experiment (true)",
			flagValue: true,
			experimentValues: map[string]bool{
				experiments.CapacityBuffersMinCAVersion: false,
				experiments.CapacityBuffersEnabled:      true,
			},
			wantValue: false,
		},
		{
			testName:  "Combination: Private preview and boolean experiment defined, CLI disabled",
			flagValue: false,
			experimentValues: map[string]bool{
				experiments.CapacityBuffersPrivatePreviewMinCAVersion: true,
				experiments.CapacityBuffersEnabled:                    true,
			},
			wantValue: true,
		},
		{
			testName:  "Combination: Private preview and public-preview version defined, CLI disabled",
			flagValue: false,
			experimentValues: map[string]bool{
				experiments.CapacityBuffersPrivatePreviewMinCAVersion: true,
				experiments.CapacityBuffersMinCAVersion:               true,
			},
			wantValue: true,
		},
	}
}

func TestCapacityBuffersControllerEnabledFieldSetValue(t *testing.T) {
	for _, tc := range getCapacityBuffersTestCases() {
		t.Run(tc.testName, func(t *testing.T) {
			optsFromFlags := internalopts.AutoscalingOptions{}
			optsFromFlags.CapacitybufferControllerEnabled = tc.flagValue
			optsFromFlags.AutopilotEnabled = tc.autopilotEnabled
			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, tc.experimentValues, nil)

			flagsToModify := internalopts.AutoscalingOptions{}
			capacityBuffersControllerEnabledField.setValue(optsFromFlags, experimentsManager, &flagsToModify)
			gotValue := flagsToModify.CapacitybufferControllerEnabled

			// Assert that the field was modified as expected in the provided AutoscalingOptions.
			assert.Equal(t, tc.wantValue, gotValue)

			// Assert that getValueStr() works correctly.
			assert.Equal(t, fmt.Sprintf("%v", tc.wantValue), capacityBuffersControllerEnabledField.getValueStr(flagsToModify))
		})
	}
}

func TestCapacityBuffersPodInjectionEnabledFieldSetValue(t *testing.T) {
	for _, tc := range getCapacityBuffersTestCases() {
		t.Run(tc.testName, func(t *testing.T) {
			optsFromFlags := internalopts.AutoscalingOptions{}
			optsFromFlags.CapacitybufferPodInjectionEnabled = tc.flagValue
			optsFromFlags.AutopilotEnabled = tc.autopilotEnabled
			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, tc.experimentValues, nil)

			flagsToModify := internalopts.AutoscalingOptions{}
			capacityBuffersPodInjectionEnabledField.setValue(optsFromFlags, experimentsManager, &flagsToModify)
			gotValue := flagsToModify.CapacitybufferPodInjectionEnabled

			// Assert that the field was modified as expected in the provided AutoscalingOptions.
			assert.Equal(t, tc.wantValue, gotValue)

			// Assert that getValueStr() works correctly.
			assert.Equal(t, fmt.Sprintf("%v", tc.wantValue), capacityBuffersPodInjectionEnabledField.getValueStr(flagsToModify))
		})
	}
}
