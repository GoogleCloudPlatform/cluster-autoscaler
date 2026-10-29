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

func TestComputeClassCapacityBuffersFieldsSetValue(t *testing.T) {
	caVersion := version.Version{37, 237, 0, 1}
	for _, tc := range []struct {
		testName          string
		boolExperiments   map[string]bool
		stringExperiments map[string]string
		wantEnabled       bool
		wantCleanup       bool
	}{
		{
			testName:    "no_experiments_defined",
			wantEnabled: false, // The MinCAVersion failsafe is false.
			wantCleanup: false,
		},
		{
			testName:          "min_version_met_enabled_undefined",
			stringExperiments: map[string]string{experiments.ComputeClassCapacityBuffersMinCAVersionFlag: "37.237.0"},
			wantEnabled:       true, // The Enabled failsafe is true.
			wantCleanup:       false,
		},
		{
			testName:          "min_version_met_enabled_true",
			boolExperiments:   map[string]bool{experiments.ComputeClassCapacityBuffersFlag: true},
			stringExperiments: map[string]string{experiments.ComputeClassCapacityBuffersMinCAVersionFlag: "37.237.0"},
			wantEnabled:       true,
			wantCleanup:       false,
		},
		{
			testName:          "min_version_met_enabled_false",
			boolExperiments:   map[string]bool{experiments.ComputeClassCapacityBuffersFlag: false},
			stringExperiments: map[string]string{experiments.ComputeClassCapacityBuffersMinCAVersionFlag: "37.237.0"},
			wantEnabled:       false,
			wantCleanup:       false,
		},
		{
			testName:          "min_version_not_met",
			boolExperiments:   map[string]bool{experiments.ComputeClassCapacityBuffersFlag: true},
			stringExperiments: map[string]string{experiments.ComputeClassCapacityBuffersMinCAVersionFlag: "999.999.999"},
			wantEnabled:       false,
			wantCleanup:       false,
		},
		{
			testName:          "cleanup_true_gate_on",
			boolExperiments:   map[string]bool{experiments.ComputeClassCapacityBuffersCleanupFlag: true},
			stringExperiments: map[string]string{experiments.ComputeClassCapacityBuffersMinCAVersionFlag: "37.237.0"},
			wantEnabled:       true, // Cleanup doesn't change the gate; builder.go calls SetupSweeper instead of SetupReconciler when Cleanup is set.
			wantCleanup:       true,
		},
		{
			testName:        "cleanup_true_gate_off",
			boolExperiments: map[string]bool{experiments.ComputeClassCapacityBuffersCleanupFlag: true},
			wantEnabled:     false,
			wantCleanup:     true,
		},
		{
			testName:          "cleanup_false",
			boolExperiments:   map[string]bool{experiments.ComputeClassCapacityBuffersCleanupFlag: false},
			stringExperiments: map[string]string{experiments.ComputeClassCapacityBuffersMinCAVersionFlag: "37.237.0"},
			wantEnabled:       true,
			wantCleanup:       false,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			experimentsManager := experiments.NewMockManagerWithOptions(caVersion, tc.boolExperiments, tc.stringExperiments)

			optsToModify := internalopts.AutoscalingOptions{}
			assert.NoError(t, computeClassCapacityBuffersEnabledField.setValue(internalopts.AutoscalingOptions{}, experimentsManager, &optsToModify))
			assert.NoError(t, computeClassCapacityBuffersCleanupField.setValue(internalopts.AutoscalingOptions{}, experimentsManager, &optsToModify))

			// Assert that the fields were modified as expected in the provided AutoscalingOptions.
			assert.Equal(t, tc.wantEnabled, optsToModify.ComputeClassCapacityBuffersEnabled)
			assert.Equal(t, tc.wantCleanup, optsToModify.ComputeClassCapacityBuffersCleanup)

			// Assert that getValueStr() works correctly.
			assert.Equal(t, fmt.Sprintf("%v", tc.wantEnabled), computeClassCapacityBuffersEnabledField.getValueStr(optsToModify))
			assert.Equal(t, fmt.Sprintf("%v", tc.wantCleanup), computeClassCapacityBuffersCleanupField.getValueStr(optsToModify))
		})
	}
}

func TestComputeClassCapacityBuffersFieldsRequireRestart(t *testing.T) {
	for _, tc := range []struct {
		name  string
		field trackedField
		set   func(opts *internalopts.AutoscalingOptions, value bool)
	}{
		{
			name:  "ComputeClassCapacityBuffersEnabled",
			field: computeClassCapacityBuffersEnabledField,
			set: func(opts *internalopts.AutoscalingOptions, value bool) {
				opts.ComputeClassCapacityBuffersEnabled = value
			},
		},
		{
			name:  "ComputeClassCapacityBuffersCleanup",
			field: computeClassCapacityBuffersCleanupField,
			set: func(opts *internalopts.AutoscalingOptions, value bool) {
				opts.ComputeClassCapacityBuffersCleanup = value
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The controller is only set up at CA startup, so a change must restart CA.
			assert.True(t, tc.field.caRestartNeededOnValueChange)
			assert.Nil(t, tc.field.propagateChangesToAutoscalingContext)

			optsFalse, optsTrue := internalopts.AutoscalingOptions{}, internalopts.AutoscalingOptions{}
			tc.set(&optsTrue, true)
			assert.True(t, tc.field.valueEqual(optsFalse, optsFalse))
			assert.True(t, tc.field.valueEqual(optsTrue, optsTrue))
			assert.False(t, tc.field.valueEqual(optsFalse, optsTrue))
		})
	}
}
