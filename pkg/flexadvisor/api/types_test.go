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

package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/utils/set"
)

func TestInstanceConfig_Signature(t *testing.T) {
	testCases := []struct {
		name          string
		config        *InstanceConfig
		wantSignature string
	}{
		{
			name: "Basic configuration with GPU",
			config: &InstanceConfig{
				machineType:      "n2-standard-4",
				provisioningMode: instanceavailability.Standard,
				gpuType:          "nvidia-tesla-t4",
				gpuCount:         1,
				zones:            set.New[string]("us-central1-a", "us-central1-c"),
			},
			wantSignature: "machineType: n2-standard-4, provisioningMode: STANDARD, gpuType: nvidia-tesla-t4, gpuCount: 1",
		},
		{
			name: "Configuration without GPU",
			config: &InstanceConfig{
				machineType:      "e2-medium",
				provisioningMode: instanceavailability.Spot,
				gpuType:          "",
				gpuCount:         0,
				zones:            set.New[string]("us-central1-b"),
			},
			wantSignature: "machineType: e2-medium, provisioningMode: SPOT",
		},
		{
			name: "Configuration with no zones",
			config: &InstanceConfig{
				machineType:      "n2d-highcpu-8",
				provisioningMode: instanceavailability.Standard,
				gpuType:          "",
				gpuCount:         0,
				zones:            set.New[string](),
			},
			wantSignature: "machineType: n2d-highcpu-8, provisioningMode: STANDARD",
		},
		{
			name: "DWS",
			config: &InstanceConfig{
				machineType:      "n2-standard-4",
				provisioningMode: instanceavailability.FlexStart,
				gpuType:          "nvidia-tesla-t4",
				gpuCount:         1,
				zones:            set.New[string]("us-central1-a", "us-central1-c"),
			},
			wantSignature: "machineType: n2-standard-4, provisioningMode: FLEX_START, gpuType: nvidia-tesla-t4, gpuCount: 1",
		},
		{
			name: "MRD",
			config: &InstanceConfig{
				machineType:             "n2-standard-4",
				provisioningMode:        instanceavailability.FlexStart,
				gpuType:                 "nvidia-tesla-t4",
				gpuCount:                1,
				zones:                   set.New[string]("us-central1-a", "us-central1-c"),
				maxRunDurationInSeconds: "3600",
			},
			wantSignature: "machineType: n2-standard-4, provisioningMode: FLEX_START, gpuType: nvidia-tesla-t4, gpuCount: 1, maxRunDuration: 3600",
		},
		{
			name: "with AcceleratorTopology",
			config: &InstanceConfig{
				machineType:      "ct5p-hightpu-4t",
				provisioningMode: instanceavailability.Standard,
				zones:            set.New[string]("us-central1-a", "us-central1-c"),
				workloadPolicies: WorkloadPolicies{AcceleratorTopology: "2x2x1"},
			},
			wantSignature: "machineType: ct5p-hightpu-4t, provisioningMode: STANDARD, acceleratorTopology: 2x2x1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			signature := tc.config.Signature()
			assert.Equal(t, tc.wantSignature, signature)
		})
	}
}
