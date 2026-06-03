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

	"k8s.io/autoscaler/cluster-autoscaler/config"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestDynamicResourceAllocationEnabledFieldSetValue(t *testing.T) {
	for _, tc := range []struct {
		testName string

		flagValue        bool
		experimentValues map[string]bool
		clusterProto     gkeclient.Cluster

		wantValue bool
	}{
		// Test cases where the flag value is true - if any of the mitigation experiments are defined, they override the
		// result value to false. The bool mitigation experiment should never have "true" value in practice, but it's tested for completeness.
		{
			testName:         "flag_true_both_experiments_undefined", // Standard rollout path
			flagValue:        true,
			experimentValues: nil,
			wantValue:        true, // Enabled by flag, no experiment overrides -> enabled
		},
		{
			testName:         "flag_true_both_experiments_undefined_emulated_version_too_low", // Standard rollout path
			flagValue:        true,
			experimentValues: nil,
			clusterProto:     gkeclient.Cluster{EmulatedClusterVersion: "1.33"},
			wantValue:        false, // Enabled by flag, no experiment overrides, but incompatible emulated version -> disabled
		},
		{
			testName:         "flag_true_both_experiments_undefined_emulated_version_ok", // Standard rollout path
			flagValue:        true,
			experimentValues: nil,
			clusterProto:     gkeclient.Cluster{EmulatedClusterVersion: "1.34"},
			wantValue:        true, // Enabled by flag, no experiment overrides, emulated version set but compatible -> enabled
		},
		{
			testName:         "flag_true_version_experiment_false", // Issues found during standard rollout, version-based mitigation experiment applied
			flagValue:        true,
			experimentValues: map[string]bool{experiments.DraMinCAVersionFlag: false}, // This mocks CA version being lower than the value of DRA::MinCAVersion
			wantValue:        false,                                                   // Enabled by flag, but the override version-based experiment is defined and CA version is lower -> disabled
		},
		{
			testName:         "flag_true_version_experiment_true", // Issues found during standard rollout, version-based mitigation experiment applied
			flagValue:        true,
			experimentValues: map[string]bool{experiments.DraMinCAVersionFlag: true}, // This mocks CA version being equal or higher than the value of DRA::MinCAVersion
			wantValue:        true,                                                   // Enabled by flag, there is an override version-based experiment defined but CA version is higher -> enabled
		},
		{
			testName:         "flag_true_version_experiment_true_emulated_version_too_low", // Issues found during standard rollout, version-based mitigation experiment applied
			flagValue:        true,
			experimentValues: map[string]bool{experiments.DraMinCAVersionFlag: true}, // This mocks CA version being equal or higher than the value of DRA::MinCAVersion
			clusterProto:     gkeclient.Cluster{EmulatedClusterVersion: "1.33"},
			wantValue:        false, // Enabled by flag, there is an override version-based experiment defined but CA version is higher, but emulated version is incompatible -> disabled
		},
		{
			testName:         "flag_true_version_experiment_true_emulated_version_ok", // Issues found during standard rollout, version-based mitigation experiment applied
			flagValue:        true,
			experimentValues: map[string]bool{experiments.DraMinCAVersionFlag: true}, // This mocks CA version being equal or higher than the value of DRA::MinCAVersion
			clusterProto:     gkeclient.Cluster{EmulatedClusterVersion: "1.34"},
			wantValue:        true, // Enabled by flag, there is an override version-based experiment defined but CA version is higher, emulated version is defined but compatible -> enabled
		},
		{
			testName:         "flag_true_bool_experiment_false", // Issues found during standard rollout, bool mitigation experiment applied
			flagValue:        true,
			experimentValues: map[string]bool{draBoolMitigationExperiment: false},
			wantValue:        false, // Enabled by flag, but the override bool experiment is defined as false -> disabled
		},
		{
			testName:         "flag_true_both_experiments_false", // Issues found during standard rollout, both mitigation experiments applied
			flagValue:        true,
			experimentValues: map[string]bool{experiments.DraMinCAVersionFlag: false, draBoolMitigationExperiment: false},
			wantValue:        false, // Enabled by flag, but both experiments override to disabled -> disabled
		},
		{
			testName:         "flag_true_version_experiment_true_bool_experiment_false", // Issues found during standard rollout, both mitigation experiments applied
			flagValue:        true,
			experimentValues: map[string]bool{experiments.DraMinCAVersionFlag: true, draBoolMitigationExperiment: false},
			wantValue:        false, // Any of the two experiments alone should still override the flag value to disabled
		},
		{
			testName:         "flag_true_bool_experiment_true", // This should never happen, the bool experiment should only have the "false" value. Tested for completeness.
			flagValue:        true,
			experimentValues: map[string]bool{draBoolMitigationExperiment: true},
			wantValue:        true, // If the bool experiment were to have the "true" value, it'd be a no-op.
		},
		{
			testName:         "flag_true_bool_experiment_true_emulated_version_too_low", // This should never happen, the bool experiment should only have the "false" value. Tested for completeness.
			flagValue:        true,
			experimentValues: map[string]bool{draBoolMitigationExperiment: true},
			clusterProto:     gkeclient.Cluster{EmulatedClusterVersion: "1.33"},
			wantValue:        false, // If the bool experiment were to have the "true" value, it'd be a no-op - but the emulated version is too low -> disabled
		},
		{
			testName:         "flag_true_bool_experiment_true_emulated_version_ok", // This should never happen, the bool experiment should only have the "false" value. Tested for completeness.
			flagValue:        true,
			experimentValues: map[string]bool{draBoolMitigationExperiment: true},
			clusterProto:     gkeclient.Cluster{EmulatedClusterVersion: "1.34"},
			wantValue:        true, // If the bool experiment were to have the "true" value, it'd be a no-op. The emulated version is defined but compatible, so still enabled.
		},
		{
			testName:         "flag_true_both_experiments_true", // This should never happen, the bool experiment should only have the "false" value. Tested for completeness.
			flagValue:        true,
			experimentValues: map[string]bool{experiments.DraMinCAVersionFlag: true, draBoolMitigationExperiment: true},
			wantValue:        true, // If the bool experiment were to have the "true" value, it'd be a no-op.
		},
		{
			testName:         "flag_true_both_experiments_true_emulated_version_too_low", // This should never happen, the bool experiment should only have the "false" value. Tested for completeness.
			flagValue:        true,
			experimentValues: map[string]bool{experiments.DraMinCAVersionFlag: true, draBoolMitigationExperiment: true},
			clusterProto:     gkeclient.Cluster{EmulatedClusterVersion: "1.33"},
			wantValue:        false, // If the bool experiment were to have the "true" value, it'd be a no-op - but the emulated version is too low -> disabled
		},
		{
			testName:         "flag_true_both_experiments_true_emulated_version_ok", // This should never happen, the bool experiment should only have the "false" value. Tested for completeness.
			flagValue:        true,
			experimentValues: map[string]bool{experiments.DraMinCAVersionFlag: true, draBoolMitigationExperiment: true},
			clusterProto:     gkeclient.Cluster{EmulatedClusterVersion: "1.34"},
			wantValue:        true, // If the bool experiment were to have the "true" value, it'd be a no-op. The emulated version is defined but compatible, so still enabled.
		},
		{
			testName:         "flag_true_version_experiment_false_bool_experiment_true", // This should never happen, the bool experiment should only have the "false" value. Tested for completeness.
			flagValue:        true,
			experimentValues: map[string]bool{experiments.DraMinCAVersionFlag: false, draBoolMitigationExperiment: true},
			wantValue:        false, // If the bool experiment were to have the "true" value, it'd be a no-op.
		},

		// Test cases where the flag value is false, so the result value will always be false. Only the first test case (both experiments undefined) should happen in practice
		// if the flag value is false, the rest are tested for completeness.
		{
			testName:         "flag_false_both_experiments_undefined", // Everything disabled, nothing rolling out yet
			flagValue:        false,
			experimentValues: nil,
			wantValue:        false, // Disabled by flag -> disabled regardless of the mitigation experiments
		},
		{
			testName:         "flag_false_version_experiment_false", // This should never happen, the mitigation experiments should only be defined if the flag was already flipped to true. Tested for completeness.
			flagValue:        false,
			experimentValues: map[string]bool{experiments.DraMinCAVersionFlag: false},
			wantValue:        false, // Disabled by flag -> disabled regardless of the mitigation experiments
		},
		{
			testName:         "flag_false_version_experiment_true", // This should never happen, the mitigation experiments should only be defined if the flag was already flipped to true. Tested for completeness.
			flagValue:        false,
			experimentValues: map[string]bool{experiments.DraMinCAVersionFlag: true},
			wantValue:        false, // Disabled by flag -> disabled regardless of the mitigation experiments
		},
		{
			testName:         "flag_false_bool_experiment_false", // This should never happen, the mitigation experiments should only be defined if the flag was already flipped to true. Tested for completeness.
			flagValue:        false,
			experimentValues: map[string]bool{draBoolMitigationExperiment: false},
			wantValue:        false, // Disabled by flag -> disabled regardless of the mitigation experiments
		},
		{
			testName:         "flag_false_both_experiments_false", // This should never happen, the mitigation experiments should only be defined if the flag was already flipped to true. Tested for completeness.
			flagValue:        false,
			experimentValues: map[string]bool{experiments.DraMinCAVersionFlag: false, draBoolMitigationExperiment: false},
			wantValue:        false, // Disabled by flag -> disabled regardless of the mitigation experiments
		},
		{
			testName:         "flag_false_version_experiment_true_bool_experiment_false", // This should never happen, the mitigation experiments should only be defined if the flag was already flipped to true. Tested for completeness.
			flagValue:        false,
			experimentValues: map[string]bool{experiments.DraMinCAVersionFlag: true, draBoolMitigationExperiment: false},
			wantValue:        false, // Disabled by flag -> disabled regardless of the mitigation experiments
		},
		{
			testName:         "flag_false_bool_experiment_true", // This should never happen, the mitigation experiments should only be defined if the flag was already flipped to true. Tested for completeness.
			flagValue:        false,
			experimentValues: map[string]bool{draBoolMitigationExperiment: true},
			wantValue:        false, // Disabled by flag -> disabled regardless of the mitigation experiments
		},
		{
			testName:         "flag_false_both_experiments_true", // This should never happen, the mitigation experiments should only be defined if the flag was already flipped to true. Tested for completeness.
			flagValue:        false,
			experimentValues: map[string]bool{experiments.DraMinCAVersionFlag: true, draBoolMitigationExperiment: true},
			wantValue:        false, // Disabled by flag -> disabled regardless of the mitigation experiments
		},
		{
			testName:         "flag_false_version_experiment_false_bool_experiment_true", // This should never happen, the mitigation experiments should only be defined if the flag was already flipped to true. Tested for completeness.
			flagValue:        false,
			experimentValues: map[string]bool{experiments.DraMinCAVersionFlag: false, draBoolMitigationExperiment: true},
			wantValue:        false, // Disabled by flag -> disabled regardless of the mitigation experiments
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			optsFromFlags := internalopts.AutoscalingOptions{}
			optsFromFlags.DynamicResourceAllocationEnabled = tc.flagValue

			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, tc.experimentValues, nil)

			flagsToModify := internalopts.AutoscalingOptions{}
			err := dynamicResourceAllocationEnabledField.setValueFromClusterProto(optsFromFlags, experimentsManager, tc.clusterProto, &flagsToModify)
			assert.NoError(t, err)
			gotValue := flagsToModify.DynamicResourceAllocationEnabled

			// Assert that the field was modified as expected in the provided AutoscalingOptions.
			assert.Equal(t, tc.wantValue, gotValue)
			// Assert that getValueStr() works correctly.
			assert.Equal(t, fmt.Sprintf("%v", tc.wantValue), dynamicResourceAllocationEnabledField.getValueStr(flagsToModify))
		})
	}
}

func TestDynamicResourceAllocationEnabledFieldValueEqual(t *testing.T) {
	for _, tc := range []struct {
		testName string

		optsA internalopts.AutoscalingOptions
		optsB internalopts.AutoscalingOptions

		wantEqual bool
	}{
		{
			testName:  "both_true",
			optsA:     internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{DynamicResourceAllocationEnabled: true}},
			optsB:     internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{DynamicResourceAllocationEnabled: true}},
			wantEqual: true,
		},
		{
			testName:  "both_false",
			optsA:     internalopts.AutoscalingOptions{},
			optsB:     internalopts.AutoscalingOptions{},
			wantEqual: true,
		},
		{
			testName:  "different_values",
			optsA:     internalopts.AutoscalingOptions{},
			optsB:     internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{DynamicResourceAllocationEnabled: true}},
			wantEqual: false,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			gotEqual := dynamicResourceAllocationEnabledField.valueEqual(tc.optsA, tc.optsB)
			assert.Equal(t, tc.wantEqual, gotEqual)
		})
	}
}
