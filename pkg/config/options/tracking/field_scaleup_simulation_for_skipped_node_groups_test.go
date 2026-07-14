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

func TestScaleUpSimulationForSkippedNodeGroupsEnabledFieldSetValue(t *testing.T) {
	for _, tc := range []struct {
		testName         string
		flagValue        bool
		experimentValues map[string]bool
		wantValue        bool
	}{
		{
			testName:         "flag_true_experiments_undefined",
			flagValue:        true,
			experimentValues: nil,
			wantValue:        false,
		},
		{
			testName:         "flag_true_enabled_false",
			flagValue:        true,
			experimentValues: map[string]bool{experiments.ScaleUpSimulationForSkippedNodeGroupsEnabledFlag: false},
			wantValue:        false,
		},
		{
			testName:         "flag_true_min_version_false",
			flagValue:        true,
			experimentValues: map[string]bool{experiments.ScaleUpSimulationForSkippedNodeGroupsMinCAVersionFlag: false},
			wantValue:        false,
		},
		{
			testName:  "flag_true_both_true",
			flagValue: true,
			experimentValues: map[string]bool{
				experiments.ScaleUpSimulationForSkippedNodeGroupsEnabledFlag:      true,
				experiments.ScaleUpSimulationForSkippedNodeGroupsMinCAVersionFlag: true,
			},
			wantValue: true,
		},
		{
			testName:         "flag_false_experiments_undefined",
			flagValue:        false,
			experimentValues: nil,
			wantValue:        false,
		},
		{
			testName:  "flag_false_enabled_true_min_version_false",
			flagValue: false,
			experimentValues: map[string]bool{
				experiments.ScaleUpSimulationForSkippedNodeGroupsEnabledFlag:      true,
				experiments.ScaleUpSimulationForSkippedNodeGroupsMinCAVersionFlag: false,
			},
			wantValue: false,
		},
		{
			testName:  "flag_false_enabled_false_min_version_true",
			flagValue: false,
			experimentValues: map[string]bool{
				experiments.ScaleUpSimulationForSkippedNodeGroupsEnabledFlag:      false,
				experiments.ScaleUpSimulationForSkippedNodeGroupsMinCAVersionFlag: true,
			},
			wantValue: false,
		},
		{
			testName:  "flag_false_both_true",
			flagValue: false,
			experimentValues: map[string]bool{
				experiments.ScaleUpSimulationForSkippedNodeGroupsEnabledFlag:      true,
				experiments.ScaleUpSimulationForSkippedNodeGroupsMinCAVersionFlag: true,
			},
			wantValue: true,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			optsFromFlags := internalopts.AutoscalingOptions{}
			optsFromFlags.ScaleUpSimulationForSkippedNodeGroupsEnabled = tc.flagValue

			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, tc.experimentValues, nil)

			optsToModify := internalopts.AutoscalingOptions{}
			err := scaleUpSimulationForSkippedNodeGroupsEnabledField.setValue(optsFromFlags, experimentsManager, &optsToModify)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantValue, optsToModify.ScaleUpSimulationForSkippedNodeGroupsEnabled)
			assert.Equal(t, fmt.Sprintf("%v", tc.wantValue), scaleUpSimulationForSkippedNodeGroupsEnabledField.getValueStr(optsToModify))
		})
	}
}
