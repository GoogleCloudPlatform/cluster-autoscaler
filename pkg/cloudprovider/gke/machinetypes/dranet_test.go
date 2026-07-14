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

package machinetypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/resource/v1"
)

var (
	// defaultMachineType is the default machine used for testing.
	defaultMachineType = "a3-highgpu-8g"
	// defaultConfig is the default config used for testing, should match to defaultMachineType.
	defaultConfig = a3HighgpunNicAttributes
)

func TestConfigForMachineType(t *testing.T) {
	xConfig := MultiNicConfig{SharedAttributes: map[v1.QualifiedName]v1.DeviceAttribute{"X": {}}}
	provider := NewDranetConfigProviderWithConfigs(map[string]MultiNicConfig{"x": xConfig})
	tests := map[string]struct {
		machineType string
		wantFound   bool
		wantConfig  MultiNicConfig
	}{
		"EmptyString": {
			machineType: "",
			wantFound:   false,
			wantConfig:  MultiNicConfig{},
		},
		"ExistingMachineType": {
			machineType: "x",
			wantFound:   true,
			wantConfig:  xConfig,
		},
		"NonExistingMachineType": {
			machineType: "y",
			wantFound:   false,
			wantConfig:  MultiNicConfig{},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			gotConfig, gotFound := provider.ConfigForMachineType(tc.machineType)
			assert.Equal(t, tc.wantFound, gotFound)
			assert.Equal(t, tc.wantConfig, gotConfig)
		})
	}
}

func TestDefaultDranetConfigProvider(t *testing.T) {
	provider := NewDranetConfigProvider()
	config, found := provider.ConfigForMachineType(defaultMachineType)
	assert.True(t, found)
	assert.Equal(t, defaultConfig, config)
}
