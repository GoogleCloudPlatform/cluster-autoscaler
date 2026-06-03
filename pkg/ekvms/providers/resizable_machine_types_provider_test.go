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

package providers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestNewAllResizableMachineTypesProvider(t *testing.T) {
	tests := []struct {
		name               string
		experimentsManager config.StringFlagEvaluator
		machineTypeFlags   map[string]string
		experimentFlags    map[string]string
		expected           sets.Set[string]
	}{
		{
			name:             "overrides provided",
			machineTypeFlags: map[string]string{machinetypes.EK.Name(): "ek-standard-8", machinetypes.E4A.Name(): "e4a-standard-8"},
			expected:         sets.New("ek-standard-8", "e4a-standard-8"),
		},
		{
			name:            "experiment provided",
			experimentFlags: map[string]string{machinetypes.EK.Name(): "experiment-ek-flag", machinetypes.E4A.Name(): "experiment-e4a-flag"},
			experimentsManager: experiments.NewMockManagerWithOptions(
				version.Version{},
				map[string]bool{},
				map[string]string{"experiment-ek-flag": "ek-standard-4", "experiment-e4a-flag": "e4a-standard-4"},
			),
			expected: sets.New("ek-standard-4", "e4a-standard-4"),
		},
		{
			name:             "overrides and experiment both provided",
			machineTypeFlags: map[string]string{machinetypes.EK.Name(): "ek-standard-8", machinetypes.E4A.Name(): "e4a-standard-8"},
			experimentFlags:  map[string]string{machinetypes.EK.Name(): "experiment-ek-flag", machinetypes.E4A.Name(): "experiment-e4a-flag"},
			experimentsManager: experiments.NewMockManagerWithOptions(
				version.Version{},
				map[string]bool{},
				map[string]string{"experiment-ek-flag": "ek-standard-4", "experiment-e4a-flag": "e4a-standard-4"},
			),
			expected: sets.New("ek-standard-8", "e4a-standard-8"),
		},
		{
			name:             "defaults",
			machineTypeFlags: map[string]string{},
			expected:         sets.New("ek-standard-16", "ek-standard-32", "e4a-standard-8", "e4a-standard-16", "e4a-standard-32"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider := NewAllResizableMachineTypesProvider(machinetypes.NewMachineConfigProvider(nil), tc.experimentsManager, tc.machineTypeFlags, tc.experimentFlags)
			assert.Equal(t, tc.expected, provider.Provide())
		})
	}
}
