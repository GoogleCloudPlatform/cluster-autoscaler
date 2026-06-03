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
	"time"

	"github.com/stretchr/testify/assert"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestSalvoScaleUpFieldSetValue(t *testing.T) {
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
			wantValue:        true,
		},
		{
			testName:         "flag_true_enabled_false",
			flagValue:        true,
			experimentValues: map[string]bool{experiments.SalvoScaleUpEnabledFlag: false},
			wantValue:        false,
		},
		{
			testName:         "flag_true_min_version_false",
			flagValue:        true,
			experimentValues: map[string]bool{experiments.SalvoScaleUpMinCAVersionFlag: false},
			wantValue:        false,
		},
		{
			testName:         "flag_true_both_true",
			flagValue:        true,
			experimentValues: map[string]bool{experiments.SalvoScaleUpEnabledFlag: true, experiments.SalvoScaleUpMinCAVersionFlag: true},
			wantValue:        true,
		},
		{
			testName:         "flag_false_both_true",
			flagValue:        false,
			experimentValues: map[string]bool{experiments.SalvoScaleUpEnabledFlag: true, experiments.SalvoScaleUpMinCAVersionFlag: true},
			wantValue:        true,
		},
		{
			testName:         "flag_false_experiments_undefined",
			flagValue:        false,
			experimentValues: nil,
			wantValue:        false,
		},
		{
			testName:         "flag_false_enabled_true_min_version_false",
			flagValue:        false,
			experimentValues: map[string]bool{experiments.SalvoScaleUpEnabledFlag: true, experiments.SalvoScaleUpMinCAVersionFlag: false},
			wantValue:        false,
		},
		{
			testName:         "flag_false_enabled_false_min_version_true",
			flagValue:        false,
			experimentValues: map[string]bool{experiments.SalvoScaleUpEnabledFlag: false, experiments.SalvoScaleUpMinCAVersionFlag: true},
			wantValue:        false,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			optsFromFlags := internalopts.AutoscalingOptions{}
			optsFromFlags.SalvoScaleUp = tc.flagValue

			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, tc.experimentValues, nil)

			optsToModify := internalopts.AutoscalingOptions{}
			err := salvoScaleUpField.setValue(optsFromFlags, experimentsManager, &optsToModify)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantValue, optsToModify.SalvoScaleUp)
			assert.Equal(t, fmt.Sprintf("%v", tc.wantValue), salvoScaleUpField.getValueStr(optsToModify))
		})
	}
}

func TestSalvoScaleUpBudgetSecondsFieldSetValue(t *testing.T) {
	for _, tc := range []struct {
		testName string

		flagValue              time.Duration
		stringExperimentValues map[string]string

		wantValue time.Duration
	}{
		{
			testName:               "flag_value_experiments_undefined",
			flagValue:              5 * time.Minute,
			stringExperimentValues: nil,
			wantValue:              5 * time.Minute,
		},
		{
			testName:               "flag_value_overridden_by_experiment",
			flagValue:              5 * time.Minute,
			stringExperimentValues: map[string]string{experiments.SalvoScaleUpBudgetSecondsFlag: "600"},
			wantValue:              10 * time.Minute,
		},
		{
			testName:               "invalid_experiment_value_falls_back_to_failsafe",
			flagValue:              5 * time.Minute,
			stringExperimentValues: map[string]string{experiments.SalvoScaleUpBudgetSecondsFlag: "invalid"},
			wantValue:              5 * time.Minute,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			optsFromFlags := internalopts.AutoscalingOptions{}
			optsFromFlags.SalvoScaleUpBudget = tc.flagValue

			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, nil, tc.stringExperimentValues)

			optsToModify := internalopts.AutoscalingOptions{}
			err := salvoScaleUpBudgetField.setValue(optsFromFlags, experimentsManager, &optsToModify)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantValue, optsToModify.SalvoScaleUpBudget)
			assert.Equal(t, fmt.Sprintf("%v", tc.wantValue), salvoScaleUpBudgetField.getValueStr(optsToModify))
		})
	}
}
