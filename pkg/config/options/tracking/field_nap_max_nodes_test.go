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

func TestIncreasedNapLimitEnabledFieldSetValue(t *testing.T) {
	for _, tc := range []struct {
		testName string

		flagValue        int
		experimentValues map[string]bool

		wantValue int
	}{
		{
			testName:         "flag_2k_both_experiments_undefined", // Standard rollout path
			flagValue:        2000,
			experimentValues: nil,
			wantValue:        2000,
		},
		{
			testName:         "flag_default_value_both_experiments_undefined", // Manually overridden nap limit to the default value of 1k
			flagValue:        defaultDecreasedNapMaxNodes,
			experimentValues: nil,
			wantValue:        1000,
		},
		{
			testName:         "flag_500_both_experiments_undefined", // Manually overidden nap limit below the default value
			flagValue:        500,
			experimentValues: nil,
			wantValue:        500,
		},
		{
			testName:         "flag_2k_version_experiment_false", // Issues found during standard rollout, version-based mitigation experiment applied
			flagValue:        2000,
			experimentValues: map[string]bool{experiments.IncreasedNapMaxNodesMinCAVersionFlag: false}, // This mocks CA version being lower than the value of MinCAVersion
			wantValue:        defaultDecreasedNapMaxNodes,                                              // 2k by flag, but the override version-based experiment is defined and CA version is lower -> 1k
		},
		{
			testName:         "flag_2k_version_experiment_true", // Issues found during standard rollout, version-based mitigation experiment applied
			flagValue:        2000,
			experimentValues: map[string]bool{experiments.IncreasedNapMaxNodesMinCAVersionFlag: true}, // This mocks CA version being equal or higher than the value of MinCAVersion
			wantValue:        2000,                                                                    // 2k by flag, there is an override version-based experiment defined but CA version is higher -> 2k
		},
		{
			testName:         "flag_2k_bool_experiment_false", // Issues found during standard rollout, bool mitigation experiment applied
			flagValue:        2000,
			experimentValues: map[string]bool{experiments.IncreasedNapMaxNodesEnabledFlag: false},
			wantValue:        defaultDecreasedNapMaxNodes, // 2k by flag, but the override bool experiment is defined as false -> 1k
		},
		{
			testName:         "flag_2k_both_experiments_false", // Issues found during standard rollout, both mitigation experiments applied
			flagValue:        2000,
			experimentValues: map[string]bool{experiments.IncreasedNapMaxNodesEnabledFlag: false, experiments.IncreasedNapMaxNodesMinCAVersionFlag: false},
			wantValue:        defaultDecreasedNapMaxNodes, // 2k by flag, but both experiments override to disabled -> 1k
		},
		{
			testName:         "flag_2k_version_experiment_true_bool_experiment_false", // Issues found during standard rollout, both mitigation experiments applied
			flagValue:        2000,
			experimentValues: map[string]bool{experiments.IncreasedNapMaxNodesMinCAVersionFlag: true, experiments.IncreasedNapMaxNodesEnabledFlag: false},
			wantValue:        defaultDecreasedNapMaxNodes, // Any of the two experiments alone should still override the limit to 1k
		},
		{
			testName:         "flag_2k_bool_experiment_true", // This should never happen, the bool experiment should only have the "false" value. Tested for completeness.
			flagValue:        2000,
			experimentValues: map[string]bool{experiments.IncreasedNapMaxNodesEnabledFlag: true},
			wantValue:        2000, // If the bool experiment were to have the "true" value, it'd be a no-op.
		},
		{
			testName:         "flag_2k_both_experiments_true", // This should never happen, the bool experiment should only have the "false" value. Tested for completeness.
			flagValue:        2000,
			experimentValues: map[string]bool{experiments.IncreasedNapMaxNodesMinCAVersionFlag: true, experiments.IncreasedNapMaxNodesEnabledFlag: true},
			wantValue:        2000, // If the bool experiment were to have the "true" value, it'd be a no-op.
		},
		{
			testName:         "flag_2k_version_experiment_false_bool_experiment_true", // This should never happen, the bool experiment should only have the "false" value. Tested for completeness.
			flagValue:        2000,
			experimentValues: map[string]bool{experiments.IncreasedNapMaxNodesMinCAVersionFlag: true, experiments.IncreasedNapMaxNodesEnabledFlag: false},
			wantValue:        defaultDecreasedNapMaxNodes, // If the bool experiment were to have the "true" value, it'd be a no-op.
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			optsFromFlags := internalopts.AutoscalingOptions{}
			optsFromFlags.NapMaxNodes = tc.flagValue

			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, tc.experimentValues, nil)

			flagsToModify := internalopts.AutoscalingOptions{}
			napMaxNodesField.setValue(optsFromFlags, experimentsManager, &flagsToModify)
			gotValue := flagsToModify.NapMaxNodes

			// Assert that the field was modified as expected in the provided AutoscalingOptions.
			assert.Equal(t, tc.wantValue, gotValue)
			// Assert that getValueStr() works correctly.
			assert.Equal(t, fmt.Sprintf("%v", tc.wantValue), napMaxNodesField.getValueStr(flagsToModify))
		})
	}
}

func TestNapAutoscalingLimitFieldValueEqual(t *testing.T) {
	for _, tc := range []struct {
		testName string

		optsA internalopts.AutoscalingOptions
		optsB internalopts.AutoscalingOptions

		wantEqual bool
	}{
		{
			testName:  "both_2k",
			optsA:     internalopts.AutoscalingOptions{InternalOptions: internalopts.InternalOptions{NapMaxNodes: 2000}},
			optsB:     internalopts.AutoscalingOptions{InternalOptions: internalopts.InternalOptions{NapMaxNodes: 2000}},
			wantEqual: true,
		},
		{
			testName:  "both_undefined",
			optsA:     internalopts.AutoscalingOptions{},
			optsB:     internalopts.AutoscalingOptions{},
			wantEqual: true,
		},
		{
			testName:  "different_values",
			optsA:     internalopts.AutoscalingOptions{InternalOptions: internalopts.InternalOptions{NapMaxNodes: 1000}},
			optsB:     internalopts.AutoscalingOptions{InternalOptions: internalopts.InternalOptions{NapMaxNodes: 2000}},
			wantEqual: false,
		},
		{
			testName:  "different_values_one_missing",
			optsA:     internalopts.AutoscalingOptions{},
			optsB:     internalopts.AutoscalingOptions{InternalOptions: internalopts.InternalOptions{NapMaxNodes: 2000}},
			wantEqual: false,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			gotEqual := napMaxNodesField.valueEqual(tc.optsA, tc.optsB)
			assert.Equal(t, tc.wantEqual, gotEqual)
		})
	}
}
