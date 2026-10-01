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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments/fake"
)

func getReservationOverrideTestCases(enabledFlag, minVersionFlag string) []struct {
	testName          string
	flagValue         bool
	boolExperiments   map[string]bool
	stringExperiments map[string]string
	wantValue         bool
} {
	return []struct {
		testName          string
		flagValue         bool
		boolExperiments   map[string]bool
		stringExperiments map[string]string
		wantValue         bool
	}{
		{
			testName:          "Flag true, no experiments, expect True",
			flagValue:         true,
			boolExperiments:   nil,
			stringExperiments: nil,
			wantValue:         true,
		},
		{
			testName:          "Flag false, no experiments, expect False",
			flagValue:         false,
			boolExperiments:   nil,
			stringExperiments: nil,
			wantValue:         false,
		},
		{
			testName:  "Flag false, experiment enabled true, expect True (experiment overrides false CLI flag)",
			flagValue: false,
			boolExperiments: map[string]bool{
				enabledFlag: true,
			},
			stringExperiments: map[string]string{
				minVersionFlag: "1.0.0",
			},
			wantValue: true,
		},
		{
			testName:  "Flag true, experiment enabled false, expect False (experiment overrides true CLI flag)",
			flagValue: true,
			boolExperiments: map[string]bool{
				enabledFlag: false,
			},
			stringExperiments: map[string]string{
				minVersionFlag: "1.0.0",
			},
			wantValue: false,
		},
		{
			testName:  "Flag false, experiment enabled true but version unsupported, expect False (falls back to CLI flag)",
			flagValue: false,
			boolExperiments: map[string]bool{
				enabledFlag: true,
			},
			stringExperiments: map[string]string{
				minVersionFlag: "999.0.0",
			},
			wantValue: false,
		},
		{
			testName:  "Flag true, experiment enabled true but version unsupported, expect True (falls back to CLI flag)",
			flagValue: true,
			boolExperiments: map[string]bool{
				enabledFlag: true,
			},
			stringExperiments: map[string]string{
				minVersionFlag: "999.0.0",
			},
			wantValue: true,
		},
		{
			testName:  "Flag true, experiment enabled false but version unsupported, expect True (falls back to CLI flag)",
			flagValue: true,
			boolExperiments: map[string]bool{
				enabledFlag: false,
			},
			stringExperiments: map[string]string{
				minVersionFlag: "999.0.0",
			},
			wantValue: true,
		},
	}
}

func TestSpecificTypeReservationMatchEnabledFieldSetValue(t *testing.T) {
	testCases := getReservationOverrideTestCases(
		experiments.SpecificTypeReservationMatchEnabledFlag,
		experiments.SpecificTypeReservationMatchMinCAVersionFlag,
	)
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			optsFromFlags := internalopts.AutoscalingOptions{}
			optsFromFlags.SpecificTypeReservationMatchEnabled = tc.flagValue

			caVersion, _ := version.FromString("1.30.0")
			evaluator := fake.NewEvaluator(tc.boolExperiments, tc.stringExperiments)
			experimentsManager := experiments.NewManager(caVersion, evaluator)

			optsToModify := internalopts.AutoscalingOptions{}
			err := specificTypeReservationMatchEnabledField.setValue(optsFromFlags, experimentsManager, &optsToModify)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantValue, optsToModify.SpecificTypeReservationMatchEnabled)
		})
	}
}

func TestSpecificTypeReservationWithoutMatchEnabledFieldSetValue(t *testing.T) {
	testCases := getReservationOverrideTestCases(
		experiments.SpecificTypeReservationWithoutMatchEnabledFlag,
		experiments.SpecificTypeReservationWithoutMatchMinCAVersionFlag,
	)
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			optsFromFlags := internalopts.AutoscalingOptions{}
			optsFromFlags.SpecificTypeReservationWithoutMatchEnabled = tc.flagValue

			caVersion, _ := version.FromString("1.30.0")
			evaluator := fake.NewEvaluator(tc.boolExperiments, tc.stringExperiments)
			experimentsManager := experiments.NewManager(caVersion, evaluator)

			optsToModify := internalopts.AutoscalingOptions{}
			err := specificTypeReservationWithoutMatchEnabledField.setValue(optsFromFlags, experimentsManager, &optsToModify)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantValue, optsToModify.SpecificTypeReservationWithoutMatchEnabled)
		})
	}
}

func TestReservationsAnyLocationPolicyOverrideFieldSetValue(t *testing.T) {
	testCases := getReservationOverrideTestCases(
		experiments.ReservationsAnyLocationPolicyOverrideEnabledFlag,
		experiments.ReservationsAnyLocationPolicyOverrideMinCAVersionFlag,
	)
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			optsFromFlags := internalopts.AutoscalingOptions{}
			optsFromFlags.ReservationsAnyLocationPolicyOverride = tc.flagValue

			caVersion, _ := version.FromString("1.30.0")
			evaluator := fake.NewEvaluator(tc.boolExperiments, tc.stringExperiments)
			experimentsManager := experiments.NewManager(caVersion, evaluator)

			optsToModify := internalopts.AutoscalingOptions{}
			err := reservationsAnyLocationPolicyOverrideField.setValue(optsFromFlags, experimentsManager, &optsToModify)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantValue, optsToModify.ReservationsAnyLocationPolicyOverride)
		})
	}
}
