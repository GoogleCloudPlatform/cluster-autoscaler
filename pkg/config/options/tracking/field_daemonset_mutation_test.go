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

func getDaemonSetMutationEnabledTestCases() []struct {
	testName         string
	flagValue        bool
	experimentValues map[string]bool
	wantValue        bool
} {
	return []struct {
		testName         string
		flagValue        bool
		experimentValues map[string]bool
		wantValue        bool
	}{
		{
			testName:         "Flag true, no experiments, expect True",
			flagValue:        true,
			experimentValues: nil,
			wantValue:        true,
		},
		{
			testName:         "Flag false, no experiments, expect False",
			flagValue:        false,
			experimentValues: nil,
			wantValue:        false,
		},
		{
			testName:  "Flag true, both experiments true, expect True",
			flagValue: true,
			experimentValues: map[string]bool{
				experiments.DaemonSetMutationEnabledFlag:      true,
				experiments.DaemonSetMutationMinCAVersionFlag: true,
			},
			wantValue: true,
		},
		{
			testName:  "Flag true, enabled experiment false, expect False",
			flagValue: true,
			experimentValues: map[string]bool{
				experiments.DaemonSetMutationEnabledFlag:      false,
				experiments.DaemonSetMutationMinCAVersionFlag: true,
			},
			wantValue: false,
		},
		{
			testName:  "Flag true, version experiment false, expect False",
			flagValue: true,
			experimentValues: map[string]bool{
				experiments.DaemonSetMutationEnabledFlag:      true,
				experiments.DaemonSetMutationMinCAVersionFlag: false,
			},
			wantValue: false,
		},
		{
			testName:  "Flag false, both experiments true, expect False (flag overrides)",
			flagValue: false,
			experimentValues: map[string]bool{
				experiments.DaemonSetMutationEnabledFlag:      true,
				experiments.DaemonSetMutationMinCAVersionFlag: true,
			},
			wantValue: false,
		},
	}
}

func TestDaemonSetMutationEnabledFieldSetValue(t *testing.T) {
	for _, tc := range getDaemonSetMutationEnabledTestCases() {
		t.Run(tc.testName, func(t *testing.T) {
			optsFromFlags := internalopts.AutoscalingOptions{}
			optsFromFlags.DaemonSetMutationEnabled = tc.flagValue
			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, tc.experimentValues, nil)

			optsToModify := internalopts.AutoscalingOptions{}
			err := daemonSetMutationEnabledField.setValue(optsFromFlags, experimentsManager, &optsToModify)
			assert.NoError(t, err)
			gotValue := optsToModify.DaemonSetMutationEnabled

			// Assert that the field was modified as expected in the provided AutoscalingOptions.
			assert.Equal(t, tc.wantValue, gotValue)

			// Assert that getValueStr() works correctly.
			assert.Equal(t, fmt.Sprintf("%v", tc.wantValue), daemonSetMutationEnabledField.getValueStr(optsToModify))
		})
	}
}
