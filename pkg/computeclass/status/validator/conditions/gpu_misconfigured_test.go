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
)

func TestValidateGpuConfig(t *testing.T) {
	existingMachineFamily := "N2"
	existingMachineType := "n2-standard-4"
	existingGpuType1 := "nvidia-a100-80gb"
	existingGpuType2 := "nvidia-tesla-v100"
	nonExistingGpuType := "invalid-gpu"
	existingGpuCount := machinetypes.PhysicalGpuCount(2)
	nonExistingGpuCount := machinetypes.PhysicalGpuCount(3)
	machineTypeWithGpu := "a2-ultragpu-2g"
	machineFamilyWithGpu := "a2"
	nonExistingCpuRequirementForGpu := 30
	existingCpuRequirementForGpu := 12

	customMachineType1 := "custom-4-32768"
	customMachineType3 := "e2-custom-16-131072"

	testCases := []struct {
		name        string
		rule        rules.Rule
		wantReason  string
		wantMessage string
	}{
		{
			name: "invalid gpu type",
			rule: rules.NewRule(rules.WithGpuRule(
				&machinetypes.GpuRequest{
					Config:           machinetypes.GpuConfig{GpuType: nonExistingGpuType},
					Count:            machinetypes.AllocatableGpuCount(existingGpuCount),
					PhysicalGPUCount: existingGpuCount,
				})),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(InvalidGpuTypeMessage, nonExistingGpuType),
		},
		{
			name: "valid gpu type and invalid count",
			rule: rules.NewRule(rules.WithGpuRule(
				&machinetypes.GpuRequest{
					Config:           machinetypes.GpuConfig{GpuType: existingGpuType1},
					Count:            machinetypes.AllocatableGpuCount(nonExistingGpuCount),
					PhysicalGPUCount: nonExistingGpuCount,
				})),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(InvalidGpuConfigurationMessage, existingGpuType1, nonExistingGpuCount),
		},
		{
			name: "valid gpu type and valid count",
			rule: rules.NewRule(rules.WithGpuRule(
				&machinetypes.GpuRequest{
					Config:           machinetypes.GpuConfig{GpuType: existingGpuType1},
					Count:            machinetypes.AllocatableGpuCount(existingGpuCount),
					PhysicalGPUCount: existingGpuCount,
				}), rules.WithMachineFamilyRule(&machineFamilyWithGpu)),
		},
		{
			name: "valid gpu type without machine family or machine type",
			rule: rules.NewRule(rules.WithGpuRule(
				&machinetypes.GpuRequest{
					Config:           machinetypes.GpuConfig{GpuType: existingGpuType1},
					Count:            machinetypes.AllocatableGpuCount(existingGpuCount),
					PhysicalGPUCount: existingGpuCount,
				})),
		},
		{
			name: "valid machine type with valid GPU",
			rule: rules.NewRule(rules.WithGpuRule(
				&machinetypes.GpuRequest{
					Config:           machinetypes.GpuConfig{GpuType: existingGpuType1},
					Count:            machinetypes.AllocatableGpuCount(existingGpuCount),
					PhysicalGPUCount: existingGpuCount,
				}), rules.WithMachineTypeRule(&machineTypeWithGpu)),
		},
		{
			name: "valid machine type with invalid GPU",
			rule: rules.NewRule(rules.WithGpuRule(
				&machinetypes.GpuRequest{
					Config:           machinetypes.GpuConfig{GpuType: existingGpuType1},
					Count:            machinetypes.AllocatableGpuCount(existingGpuCount),
					PhysicalGPUCount: machinetypes.PhysicalGpuCount(existingGpuCount),
				}), rules.WithMachineTypeRule(&existingMachineType)),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(GpuNotSupportedWithMachineTypeMessage, existingGpuType1, existingGpuCount, existingMachineType),
		},
		{
			name: "valid custom machine type with invalid GPU",
			rule: rules.NewRule(rules.WithGpuRule(
				&machinetypes.GpuRequest{
					Config:           machinetypes.GpuConfig{GpuType: existingGpuType1},
					Count:            machinetypes.AllocatableGpuCount(existingGpuCount),
					PhysicalGPUCount: existingGpuCount,
				}), rules.WithMachineTypeRule(&customMachineType3)),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(GpuNotSupportedWithMachineTypeMessage, existingGpuType1, existingGpuCount, customMachineType3),
		},
		{
			name: "machine family doesn't support GPU",
			rule: rules.NewRule(rules.WithGpuRule(
				&machinetypes.GpuRequest{
					Config:           machinetypes.GpuConfig{GpuType: existingGpuType1},
					Count:            machinetypes.AllocatableGpuCount(existingGpuCount),
					PhysicalGPUCount: existingGpuCount,
				}), rules.WithMachineFamilyRule(&existingMachineFamily)),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(GpuNotSupportedWithMachineFamilyMessage, existingGpuType1, existingGpuCount, existingMachineFamily),
		},
		{
			name: "machine family support GPU",
			rule: rules.NewRule(rules.WithGpuRule(
				&machinetypes.GpuRequest{
					Config:           machinetypes.GpuConfig{GpuType: existingGpuType1},
					Count:            machinetypes.AllocatableGpuCount(existingGpuCount),
					PhysicalGPUCount: existingGpuCount,
				}), rules.WithMachineFamilyRule(&machineFamilyWithGpu)),
		},
		{
			name: "GPU incompatible with CPU requirement",
			rule: rules.NewRule(rules.WithGpuRule(
				&machinetypes.GpuRequest{
					Config:           machinetypes.GpuConfig{GpuType: existingGpuType1},
					Count:            machinetypes.AllocatableGpuCount(existingGpuCount),
					PhysicalGPUCount: existingGpuCount,
				}), rules.WithMachineFamilyRule(&machineFamilyWithGpu), rules.WithMinCoresRule(&nonExistingCpuRequirementForGpu)),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(GpuNotSupportedWithCpuMessage, existingGpuType1, existingGpuCount, nonExistingCpuRequirementForGpu, 24),
		},
		{
			name: "GPU incompatible with CPU requirement with custom machine type",
			rule: rules.NewRule(rules.WithGpuRule(
				&machinetypes.GpuRequest{
					Config:           machinetypes.GpuConfig{GpuType: existingGpuType2},
					Count:            machinetypes.AllocatableGpuCount(existingGpuCount),
					PhysicalGPUCount: existingGpuCount,
				}), rules.WithMachineTypeRule(&customMachineType1), rules.WithMinCoresRule(&nonExistingCpuRequirementForGpu)),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(GpuNotSupportedWithCpuMessage, existingGpuType2, existingGpuCount, nonExistingCpuRequirementForGpu, 24),
		},
		{
			name: "GPU compatible with CPU requirement",
			rule: rules.NewRule(rules.WithGpuRule(
				&machinetypes.GpuRequest{
					Config:           machinetypes.GpuConfig{GpuType: existingGpuType1},
					Count:            machinetypes.AllocatableGpuCount(existingGpuCount),
					PhysicalGPUCount: existingGpuCount,
				}), rules.WithMachineFamilyRule(&machineFamilyWithGpu), rules.WithMinCoresRule(&existingCpuRequirementForGpu)),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := newTestProvider().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			checker := &gpuConfigChecker{provider: provider}
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
