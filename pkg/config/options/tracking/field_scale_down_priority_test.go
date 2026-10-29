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
)

func TestScaleDownPriorityTaintKeyFieldSetValue(t *testing.T) {
	const (
		taintKey  = "cloud.google.com/gke-scale-down-priority"
		caVersion = "37.0.0-gke.0"
	)

	for _, tc := range []struct {
		testName               string
		flagValue              string
		experimentValues       map[string]bool
		stringExperimentValues map[string]string
		wantValue              string
	}{
		{
			testName:         "flag_set_experiment_undefined",
			flagValue:        taintKey,
			experimentValues: nil,
			wantValue:        taintKey,
		},
		{
			testName:         "flag_set_experiment_enabled",
			flagValue:        taintKey,
			experimentValues: map[string]bool{experiments.ScaleDownPriorityEnabledFlag: true},
			wantValue:        taintKey,
		},
		{
			testName:         "flag_set_experiment_disabled",
			flagValue:        taintKey,
			experimentValues: map[string]bool{experiments.ScaleDownPriorityEnabledFlag: false},
			wantValue:        "",
		},
		{
			testName:               "flag_set_min_ca_version_below_ca_version",
			flagValue:              taintKey,
			stringExperimentValues: map[string]string{experiments.ScaleDownPriorityMinCAVersionFlag: "36.0.0-gke.0"},
			wantValue:              taintKey,
		},
		{
			testName:               "flag_set_min_ca_version_above_ca_version",
			flagValue:              taintKey,
			stringExperimentValues: map[string]string{experiments.ScaleDownPriorityMinCAVersionFlag: "38.0.0-gke.0"},
			wantValue:              "",
		},
		{
			testName:               "flag_set_experiment_disabled_min_ca_version_below_ca_version",
			flagValue:              taintKey,
			experimentValues:       map[string]bool{experiments.ScaleDownPriorityEnabledFlag: false},
			stringExperimentValues: map[string]string{experiments.ScaleDownPriorityMinCAVersionFlag: "36.0.0-gke.0"},
			wantValue:              "",
		},
		{
			testName:         "flag_empty_experiment_undefined",
			flagValue:        "",
			experimentValues: nil,
			wantValue:        "",
		},
		{
			testName:         "flag_empty_experiment_enabled",
			flagValue:        "",
			experimentValues: map[string]bool{experiments.ScaleDownPriorityEnabledFlag: true},
			wantValue:        "",
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			optsFromFlags := internalopts.AutoscalingOptions{}
			optsFromFlags.ScaleDownPriorityTaintKey = tc.flagValue

			v, err := version.FromString(caVersion)
			if err != nil {
				t.Fatalf("version.FromString(%q) failed: %v", caVersion, err)
			}
			experimentsManager := experiments.NewMockManagerWithOptions(v, tc.experimentValues, tc.stringExperimentValues)

			optsToModify := internalopts.AutoscalingOptions{}
			err = scaleDownPriorityTaintKeyField.setValue(optsFromFlags, experimentsManager, &optsToModify)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantValue, optsToModify.ScaleDownPriorityTaintKey)
			assert.Equal(t, tc.wantValue, scaleDownPriorityTaintKeyField.getValueStr(optsToModify))
		})
	}
}

func TestScaleDownPriorityTaintKeyFieldValueEqual(t *testing.T) {
	const taintKey = "cloud.google.com/gke-scale-down-priority"
	optsWithKey := func(key string) internalopts.AutoscalingOptions {
		var opts internalopts.AutoscalingOptions
		opts.ScaleDownPriorityTaintKey = key
		return opts
	}

	testCases := []struct {
		name string
		a, b internalopts.AutoscalingOptions
		want bool
	}{
		{name: "both empty", a: optsWithKey(""), b: optsWithKey(""), want: true},
		{name: "same key", a: optsWithKey(taintKey), b: optsWithKey(taintKey), want: true},
		{name: "empty and non-empty", a: optsWithKey(""), b: optsWithKey(taintKey), want: false},
		{name: "different keys", a: optsWithKey(taintKey), b: optsWithKey("other-key"), want: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, scaleDownPriorityTaintKeyField.valueEqual(tc.a, tc.b))
		})
	}
}
