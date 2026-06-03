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

func TestTrackedFieldSetValue(t *testing.T) {
	const (
		CAVersion    = "35.195.1-gke.0"
		minCAVersion = "minCAVersion"
		enabled      = "enabled"
	)
	var (
		htnapBoolExperimentFlags = map[string]string{
			enabled: experiments.HtnapEnabledFlag,
		}
		zoneTypesBoolExperimentFlags = map[string]string{
			enabled: experiments.ZoneTypesEnabledFlag,
		}
		fastpathBinpackingBoolExperimentFlags = map[string]string{
			enabled: experiments.FastpathBinpackingEnabledFlag,
		}
		htnapStringExperimentFlags = map[string]string{
			minCAVersion: experiments.HtnapMinCAVersionFlag,
		}
		zoneTypesStringExperimentFlags = map[string]string{
			minCAVersion: experiments.ZoneTypesMinCAVersionFlag,
		}
		fastpathBinpackingStringExperimentFlags = map[string]string{
			minCAVersion: experiments.FastpathBinpackingMinCAVersionFlag,
		}
	)

	for _, trackedFieldTc := range []struct {
		name                  string
		field                 trackedField
		boolExperimentFlags   map[string]string
		stringExperimentFlags map[string]string
		setFlag               func(opts *internalopts.AutoscalingOptions, value bool)
		getFlag               func(opts internalopts.AutoscalingOptions) bool
	}{
		{
			name:                  "AsyncNodeGroupsEnabled",
			field:                 asyncNodeGroupsEnabledField,
			boolExperimentFlags:   htnapBoolExperimentFlags,
			stringExperimentFlags: htnapStringExperimentFlags,
			setFlag: func(opts *internalopts.AutoscalingOptions, value bool) {
				opts.AsyncNodeGroupsEnabled = value
			},
			getFlag: func(opts internalopts.AutoscalingOptions) bool {
				return opts.AsyncNodeGroupsEnabled
			},
		},
		{
			name:                  "ZoneTypesEnabled",
			field:                 zoneTypesEnabledField,
			boolExperimentFlags:   zoneTypesBoolExperimentFlags,
			stringExperimentFlags: zoneTypesStringExperimentFlags,
			setFlag: func(opts *internalopts.AutoscalingOptions, value bool) {
				opts.ZoneTypesEnabled = value
			},
			getFlag: func(opts internalopts.AutoscalingOptions) bool {
				return opts.ZoneTypesEnabled
			},
		},
		{
			name:                  "FastpathBinpackingEnabled",
			field:                 fastpathBinpackingEnabledField,
			boolExperimentFlags:   fastpathBinpackingBoolExperimentFlags,
			stringExperimentFlags: fastpathBinpackingStringExperimentFlags,
			setFlag: func(opts *internalopts.AutoscalingOptions, value bool) {
				opts.FastpathBinpackingEnabled = value
			},
			getFlag: func(opts internalopts.AutoscalingOptions) bool {
				return opts.FastpathBinpackingEnabled
			},
		},
	} {
		for _, tc := range []struct {
			testName string

			flagValue              bool
			boolExperimentValues   map[string]bool
			stringExperimentValues map[string]string

			wantValue bool
		}{
			// Test cases where the flag value is true - if any of the mitigation experiments are defined, they override the
			// result value to false. The bool mitigation experiment should never have "true" value in practice, but it's tested for completeness.
			{
				testName:             "flag_true_both_experiments_undefined", // Standard rollout path
				flagValue:            true,
				boolExperimentValues: nil,
				wantValue:            true, // Enabled by flag, no experiment overrides -> enabled
			},
			{
				testName:               "flag_true_version_experiment_false", // Issues found during standard rollout, version-based mitigation experiment applied
				flagValue:              true,
				stringExperimentValues: map[string]string{minCAVersion: "35.195.1-gke.1"},
				wantValue:              false, // Enabled by flag, but the override version-based experiment is defined and CA version is lower -> disabled
			},
			{
				testName:               "flag_true_version_experiment_true", // Issues found during standard rollout, version-based mitigation experiment applied
				flagValue:              true,
				stringExperimentValues: map[string]string{minCAVersion: "34.198.0"},
				wantValue:              true, // Enabled by flag, there is an override version-based experiment defined but CA version is higher -> enabled
			},
			{
				testName:             "flag_true_bool_experiment_false", // Issues found during standard rollout, bool mitigation experiment applied
				flagValue:            true,
				boolExperimentValues: map[string]bool{enabled: false},
				wantValue:            false, // Enabled by flag, but the override bool experiment is defined as false -> disabled
			},
			{
				testName:               "flag_true_both_experiments_false", // Issues found during standard rollout, both mitigation experiments applied
				flagValue:              true,
				boolExperimentValues:   map[string]bool{enabled: false},
				stringExperimentValues: map[string]string{minCAVersion: "35.195.1-gke.1"},
				wantValue:              false, // Enabled by flag, but both experiments override to disabled -> disabled
			},
			{
				testName:               "flag_true_version_experiment_true_bool_experiment_false", // Issues found during standard rollout, both mitigation experiments applied
				flagValue:              true,
				boolExperimentValues:   map[string]bool{enabled: false},
				stringExperimentValues: map[string]string{minCAVersion: "34.198.0"},
				wantValue:              false, // Any of the two experiments alone should still override the flag value to disabled
			},
			{
				testName:             "flag_true_bool_experiment_true", // This should never happen, the bool experiment should only have the "false" value. Tested for completeness.
				flagValue:            true,
				boolExperimentValues: map[string]bool{enabled: true},
				wantValue:            true, // If the bool experiment were to have the "true" value, it'd be a no-op.
			},
			{
				testName:               "flag_true_both_experiments_true", // This should never happen, the bool experiment should only have the "false" value. Tested for completeness.
				flagValue:              true,
				boolExperimentValues:   map[string]bool{enabled: true},
				stringExperimentValues: map[string]string{minCAVersion: "34.198.0"},
				wantValue:              true, // If the bool experiment were to have the "true" value, it'd be a no-op.
			},
			{
				testName:               "flag_true_version_experiment_false_bool_experiment_true", // This should never happen, the bool experiment should only have the "false" value. Tested for completeness.
				flagValue:              true,
				boolExperimentValues:   map[string]bool{enabled: true},
				stringExperimentValues: map[string]string{minCAVersion: "35.195.1-gke.1"},
				wantValue:              false, // If the bool experiment were to have the "true" value, it'd be a no-op.
			},

			// Test cases where the flag value is false, so the result value will always be false. Only the first test case (both experiments undefined) should happen in practice
			// if the flag value is false, the rest are tested for completeness.
			{
				testName:             "flag_false_both_experiments_undefined", // Everything disabled, nothing rolling out yet
				flagValue:            false,
				boolExperimentValues: nil,
				wantValue:            false, // Disabled by flag -> disabled regardless of the mitigation experiments
			},
			{
				testName:               "flag_false_version_experiment_false", // This should never happen, the mitigation experiments should only be defined if the flag was already flipped to true. Tested for completeness.
				flagValue:              false,
				stringExperimentValues: map[string]string{minCAVersion: "35.195.1-gke.1"},
				wantValue:              false, // Disabled by flag -> disabled regardless of the mitigation experiments
			},
			{
				testName:               "flag_false_version_experiment_true", // This should never happen, the mitigation experiments should only be defined if the flag was already flipped to true. Tested for completeness.
				flagValue:              false,
				stringExperimentValues: map[string]string{minCAVersion: "34.198.0"},
				wantValue:              false, // Disabled by flag -> disabled regardless of the mitigation experiments
			},
			{
				testName:             "flag_false_bool_experiment_false", // This should never happen, the mitigation experiments should only be defined if the flag was already flipped to true. Tested for completeness.
				flagValue:            false,
				boolExperimentValues: map[string]bool{enabled: false},
				wantValue:            false, // Disabled by flag -> disabled regardless of the mitigation experiments
			},
			{
				testName:               "flag_false_both_experiments_false", // This should never happen, the mitigation experiments should only be defined if the flag was already flipped to true. Tested for completeness.
				flagValue:              false,
				boolExperimentValues:   map[string]bool{enabled: false},
				stringExperimentValues: map[string]string{minCAVersion: "35.195.1-gke.1"},
				wantValue:              false, // Disabled by flag -> disabled regardless of the mitigation experiments
			},
			{
				testName:               "flag_false_version_experiment_true_bool_experiment_false", // This should never happen, the mitigation experiments should only be defined if the flag was already flipped to true. Tested for completeness.
				flagValue:              false,
				boolExperimentValues:   map[string]bool{enabled: false},
				stringExperimentValues: map[string]string{minCAVersion: "34.198.0"},
				wantValue:              false, // Disabled by flag -> disabled regardless of the mitigation experiments
			},
			{
				testName:             "flag_false_bool_experiment_true", // This should never happen, the mitigation experiments should only be defined if the flag was already flipped to true. Tested for completeness.
				flagValue:            false,
				boolExperimentValues: map[string]bool{enabled: true},
				wantValue:            false, // Disabled by flag -> disabled regardless of the mitigation experiments
			},
			{
				testName:               "flag_false_both_experiments_true", // This should never happen, the mitigation experiments should only be defined if the flag was already flipped to true. Tested for completeness.
				flagValue:              false,
				boolExperimentValues:   map[string]bool{enabled: true},
				stringExperimentValues: map[string]string{minCAVersion: "34.198.0"},
				wantValue:              false, // Disabled by flag -> disabled regardless of the mitigation experiments
			},
			{
				testName:               "flag_false_version_experiment_false_bool_experiment_true", // This should never happen, the mitigation experiments should only be defined if the flag was already flipped to true. Tested for completeness.
				flagValue:              false,
				boolExperimentValues:   map[string]bool{enabled: true},
				stringExperimentValues: map[string]string{minCAVersion: "35.195.1-gke.1"},
				wantValue:              false, // Disabled by flag -> disabled regardless of the mitigation experiments
			},
		} {
			t.Run(fmt.Sprintf("%s/%s", trackedFieldTc.name, tc.testName), func(t *testing.T) {
				boolExperiments := make(map[string]bool)
				for k, v := range tc.boolExperimentValues {
					boolExperiments[trackedFieldTc.boolExperimentFlags[k]] = v
				}
				stringExperiments := make(map[string]string)
				for k, v := range tc.stringExperimentValues {
					stringExperiments[trackedFieldTc.stringExperimentFlags[k]] = v
				}
				v, err := version.FromString(CAVersion)
				assert.NoError(t, err)
				experimentsManager := experiments.NewMockManagerWithOptions(v, boolExperiments, stringExperiments)

				optsFromFlags := internalopts.AutoscalingOptions{}
				trackedFieldTc.setFlag(&optsFromFlags, tc.flagValue)

				flagsToModify := internalopts.AutoscalingOptions{}
				trackedFieldTc.field.setValue(optsFromFlags, experimentsManager, &flagsToModify)
				gotValue := trackedFieldTc.getFlag(flagsToModify)

				// Assert that the field was modified as expected in the provided AutoscalingOptions.
				assert.Equal(t, tc.wantValue, gotValue)
				// Assert that getValueStr() works correctly.
				assert.Equal(t, fmt.Sprintf("%v", tc.wantValue), trackedFieldTc.field.getValueStr(flagsToModify))
			})
		}
	}
}

func TestTrackedFieldValueEqual(t *testing.T) {
	for _, trackedFieldTc := range []struct {
		name    string
		field   trackedField
		setFlag func(opts *internalopts.AutoscalingOptions, value bool)
	}{
		{
			name:  "AsyncNodeGroupsEnabled",
			field: asyncNodeGroupsEnabledField,
			setFlag: func(opts *internalopts.AutoscalingOptions, value bool) {
				opts.AsyncNodeGroupsEnabled = value
			},
		},
		{
			name:  "ZoneTypesEnabled",
			field: zoneTypesEnabledField,
			setFlag: func(opts *internalopts.AutoscalingOptions, value bool) {
				opts.ZoneTypesEnabled = value
			},
		},
		{
			name:  "FastpathBinpackingEnabled",
			field: fastpathBinpackingEnabledField,
			setFlag: func(opts *internalopts.AutoscalingOptions, value bool) {
				opts.FastpathBinpackingEnabled = value
			},
		},
	} {
		for _, tc := range []struct {
			testName string

			valueA bool
			valueB bool

			wantEqual bool
		}{
			{
				testName:  "both_true",
				valueA:    true,
				valueB:    true,
				wantEqual: true,
			},
			{
				testName:  "both_false",
				valueA:    false,
				valueB:    false,
				wantEqual: true,
			},
			{
				testName:  "different_values",
				valueA:    false,
				valueB:    true,
				wantEqual: false,
			},
		} {
			t.Run(fmt.Sprintf("%s/%s", trackedFieldTc.name, tc.testName), func(t *testing.T) {
				optsA := internalopts.AutoscalingOptions{}
				trackedFieldTc.setFlag(&optsA, tc.valueA)
				optsB := internalopts.AutoscalingOptions{}
				trackedFieldTc.setFlag(&optsB, tc.valueB)

				gotEqual := trackedFieldTc.field.valueEqual(optsA, optsB)
				assert.Equal(t, tc.wantEqual, gotEqual)
			})
		}
	}
}
