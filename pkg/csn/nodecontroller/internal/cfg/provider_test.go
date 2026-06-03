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

package cfg

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestProvider_GetConfig(t *testing.T) {
	tests := []struct {
		name      string
		flagValue string
		hasFlag   bool
		expected  Controller
	}{
		{
			name:      "valid json string",
			flagValue: ExampleControllerJSON,
			hasFlag:   true,
			expected:  ExampleControllerStruct,
		},
		{
			name:      "empty string",
			flagValue: "",
			hasFlag:   true,
			expected:  defaultConfig,
		},
		{
			name:     "flag not set (fallback to empty string)",
			hasFlag:  false,
			expected: defaultConfig,
		},
		{
			name:      "malformed json",
			flagValue: `{"workQueue": {"maxSize": "invalid"}}`,
			hasFlag:   true,
			expected:  defaultConfig,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stringFlags := map[string]string{}
			if tc.hasFlag {
				stringFlags[experiments.ColdStandbyNodesControllerConfigV1Flag] = tc.flagValue
			}

			mockManager := experiments.NewMockManagerWithOptions(version.Version{}, nil, stringFlags)
			provider := NewProvider(mockManager)

			cfg := provider.GetConfig()
			assert.Equal(t, tc.expected, cfg)
		})
	}
}
