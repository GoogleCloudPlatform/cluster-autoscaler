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

func getDefaultReservedResourcesV2EnabledTestCases() []struct {
	testName         string
	experimentValues map[string]bool
	wantValue        bool
} {
	return []struct {
		testName         string
		experimentValues map[string]bool
		wantValue        bool
	}{
		{
			testName:         "No experiments, expect False",
			experimentValues: nil,
			wantValue:        false,
		},
		{
			testName: "MinCAVersion experiment true, expect True",
			experimentValues: map[string]bool{
				experiments.DefaultReservedResourcesMinCAVersionFlag: true,
			},
			wantValue: true,
		},
		{
			testName: "Both experiments true, expect True",
			experimentValues: map[string]bool{
				experiments.DefaultReservedResourcesEnabledFlag:      true,
				experiments.DefaultReservedResourcesMinCAVersionFlag: true,
			},
			wantValue: true,
		},
		{
			testName: "MinCAVersion true, Enabled experiment false, expect False (mitigation)",
			experimentValues: map[string]bool{
				experiments.DefaultReservedResourcesEnabledFlag:      false,
				experiments.DefaultReservedResourcesMinCAVersionFlag: true,
			},
			wantValue: false,
		},
		{
			testName: "MinCAVersion false, expect False",
			experimentValues: map[string]bool{
				experiments.DefaultReservedResourcesEnabledFlag:      true,
				experiments.DefaultReservedResourcesMinCAVersionFlag: false,
			},
			wantValue: false,
		},
	}
}

func TestDefaultReservedResourcesV2EnabledFieldSetValue(t *testing.T) {
	for _, tc := range getDefaultReservedResourcesV2EnabledTestCases() {
		t.Run(tc.testName, func(t *testing.T) {
			optsFromFlags := internalopts.AutoscalingOptions{}
			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, tc.experimentValues, nil)

			optsToModify := internalopts.AutoscalingOptions{}
			err := defaultReservedResourcesV2EnabledField.setValue(optsFromFlags, experimentsManager, &optsToModify)
			assert.NoError(t, err)
			gotValue := optsToModify.DefaultReservedResourcesV2Enabled

			// Assert that the field was modified as expected in the provided AutoscalingOptions.
			assert.Equal(t, tc.wantValue, gotValue)

			// Assert that getValueStr() works correctly.
			assert.Equal(t, fmt.Sprintf("%v", tc.wantValue), defaultReservedResourcesV2EnabledField.getValueStr(optsToModify))
		})
	}
}
