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

func TestAuthoritativeMachinesCacheEnabledFieldSetValue(t *testing.T) {
	testCases := []struct {
		name             string
		experimentValues map[string]bool
		wantValue        bool
	}{
		{
			name:             "no experiments - enabled by default",
			experimentValues: nil,
			wantValue:        true,
		},
		{
			name: "MinCAVersion experiment true - enabled",
			experimentValues: map[string]bool{
				experiments.AuthoritativeMachinesCacheMinCAVersionFlag: true,
			},
			wantValue: true,
		},
		{
			name: "both experiments true - enabled",
			experimentValues: map[string]bool{
				experiments.AuthoritativeMachinesCacheEnabledFlag:      true,
				experiments.AuthoritativeMachinesCacheMinCAVersionFlag: true,
			},
			wantValue: true,
		},
		{
			name: "Enabled experiment false - disabled",
			experimentValues: map[string]bool{
				experiments.AuthoritativeMachinesCacheEnabledFlag:      false,
				experiments.AuthoritativeMachinesCacheMinCAVersionFlag: true,
			},
			wantValue: false,
		},
		{
			name: "MinCAVersion experiment false - disabled",
			experimentValues: map[string]bool{
				experiments.AuthoritativeMachinesCacheEnabledFlag:      true,
				experiments.AuthoritativeMachinesCacheMinCAVersionFlag: false,
			},
			wantValue: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			optsFromFlags := internalopts.AutoscalingOptions{}
			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, tc.experimentValues, nil)

			optsToModify := internalopts.AutoscalingOptions{}
			err := authoritativeMachinesCacheEnabledField.setValue(optsFromFlags, experimentsManager, &optsToModify)
			assert.NoError(t, err)
			gotValue := optsToModify.AuthoritativeMachinesCacheEnabled

			assert.Equal(t, tc.wantValue, gotValue)
			assert.Equal(t, fmt.Sprintf("%v", tc.wantValue), authoritativeMachinesCacheEnabledField.getValueStr(optsToModify))
		})
	}
}
