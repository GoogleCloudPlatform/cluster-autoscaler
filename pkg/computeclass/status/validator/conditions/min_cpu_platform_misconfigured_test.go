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

package conditions

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/utils/ptr"
)

func TestValidateMinCpuPlatformConfig(t *testing.T) {
	testCases := []struct {
		name        string
		rule        rules.Rule
		wantReason  string
		wantMessage string
	}{
		{
			name:        "min cpu platform is invalid",
			rule:        rules.NewRule(rules.WithMinCpuPlatformRule(ptr.To("Invalid Platform Name"))),
			wantReason:  UnknownMinCpuPlatformReason,
			wantMessage: fmt.Sprintf(UnknownMinCpuPlatformMessage, "Invalid Platform Name"),
		},
		{
			name:        "min cpu platform Intel Skylake is not supported by default E2 machine family",
			rule:        rules.NewRule(rules.WithMinCpuPlatformRule(ptr.To("Intel Skylake"))),
			wantReason:  MinCpuPlatformIncompatibleReason,
			wantMessage: fmt.Sprintf(MinCpuPlatformIncompatibleWithMachineFamilyMessage, "Intel Skylake", machinetypes.E2.Name()),
		},
		{
			name: "min cpu platform Intel Granite Rapids is not supported by N1 machine family",
			rule: rules.NewRule(
				rules.WithMinCpuPlatformRule(ptr.To("Intel Granite Rapids")),
				rules.WithMachineFamilyRule(ptr.To(machinetypes.N1.Name())),
			),
			wantReason:  MinCpuPlatformIncompatibleReason,
			wantMessage: fmt.Sprintf(MinCpuPlatformIncompatibleWithMachineFamilyMessage, "Intel Granite Rapids", machinetypes.N1.Name()),
		},
		{
			name: "min cpu platform Intel Haswell is supported by N1 machine family, n1-highcpu-8 has no overrides",
			rule: rules.NewRule(
				rules.WithMinCpuPlatformRule(ptr.To("Intel Haswell")),
				rules.WithMachineTypeRule(ptr.To("n1-highcpu-8")),
			),
		},
		{
			name: "min cpu platform Intel Sapphire Rapids is not supported by N1 machine family, n1-highcpu-8 has no overrides",
			rule: rules.NewRule(
				rules.WithMinCpuPlatformRule(ptr.To("Intel Sapphire Rapids")),
				rules.WithMachineTypeRule(ptr.To("n1-highcpu-8")),
			),
			wantReason:  MinCpuPlatformIncompatibleReason,
			wantMessage: fmt.Sprintf(MinCpuPlatformIncompatibleWithMachineTypeMessage, "Intel Sapphire Rapids", "n1-highcpu-8"),
		},
		{
			name: "min cpu platform Intel Haswell is supported by N1 machine family, but n1-highcpu-96 override does not support it",
			rule: rules.NewRule(
				rules.WithMinCpuPlatformRule(ptr.To("Intel Haswell")),
				rules.WithMachineTypeRule(ptr.To("n1-highcpu-96")),
			),
			wantReason:  MinCpuPlatformIncompatibleReason,
			wantMessage: fmt.Sprintf(MinCpuPlatformIncompatibleWithMachineTypeMessage, "Intel Haswell", "n1-highcpu-96"),
		},
		{
			name: "min cpu platform Intel Skylake is supported by n1-highcpu-96 override",
			rule: rules.NewRule(
				rules.WithMinCpuPlatformRule(ptr.To("Intel Skylake")),
				rules.WithMachineTypeRule(ptr.To("n1-highcpu-96")),
			),
		},
		{
			name: "min cpu platform Intel Skylake is not compatible with nvidia-tesla-k80 gpu",
			rule: rules.NewRule(
				rules.WithMinCpuPlatformRule(ptr.To("Intel Skylake")),
				rules.WithMachineTypeRule(ptr.To("n1-standard-16")),
				rules.WithGpuRule(
					&machinetypes.GpuRequest{
						Config:           machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaK80.Name()},
						Count:            machinetypes.AllocatableGpuCount(4),
						PhysicalGPUCount: 4,
					}),
			),
			wantReason:  MinCpuPlatformIncompatibleReason,
			wantMessage: fmt.Sprintf(MinCpuPlatformIncompatibleWithGpuMessage, "Intel Skylake", machinetypes.NvidiaTeslaK80.Name()),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := newTestProvider().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()

			checker := &minCpuPlatformConfigChecker{provider: provider}
			condition := checker.checkRule(tc.rule)
			if tc.wantReason != "" {
				assert.NotNil(t, condition)
				assert.Equal(t, RuleMisconfiguredCondition, condition.Type)
				assert.Equal(t, tc.wantReason, condition.Reason)
				assert.Equal(t, tc.wantMessage, condition.Message)
			} else {
				assert.Nil(t, condition)
			}
		})
	}
}
