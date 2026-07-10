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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"

	resizable_vm_size "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
)

func TestAllResizableMachineFamilies(t *testing.T) {
	mcp := NewMachineConfigProvider(nil)
	resizableFamilies := mcp.AllResizableMachineFamilies()
	allFamilies := mcp.AllMachineFamilies()

	for _, family := range resizableFamilies {
		assert.True(t, family.IsResizable(), "Family %s returned as resizable but IsResizable() is false", family.Name())
		assert.NotNil(t, family.ResizableConfig(), "Family %s returned as resizable but has no ResizableConfig", family.Name())
	}

	resizableMap := make(map[string]bool)
	for _, f := range resizableFamilies {
		resizableMap[f.Name()] = true
	}

	for _, family := range allFamilies {
		if family.ResizableConfig() != nil {
			assert.True(t, family.IsResizable(), "Family %s has ResizableConfig but IsResizable() is false", family.Name())
			assert.True(t, resizableMap[family.Name()], "Family %s has ResizableConfig but was not returned by AllResizableMachineFamilies", family.Name())
		} else {
			assert.False(t, family.IsResizable(), "Family %s has no ResizableConfig but IsResizable() is true", family.Name())
			assert.False(t, resizableMap[family.Name()], "Family %s has no ResizableConfig but was returned by AllResizableMachineFamilies", family.Name())
		}
	}
}

func TestIsLargerThan(t *testing.T) {
	for tn, tc := range map[string]struct {
		machineType MachineType
		otherType   MachineType
		expected    bool
	}{
		"more cpu": {
			machineType: NewMachineTypeInfo("a", 4, 8),
			otherType:   NewMachineTypeInfo("b", 2, 8),
			expected:    true,
		},
		"less cpu": {
			machineType: NewMachineTypeInfo("a", 2, 8),
			otherType:   NewMachineTypeInfo("b", 4, 8),
			expected:    false,
		},
		"same cpu, more mem": {
			machineType: NewMachineTypeInfo("a", 4, 16),
			otherType:   NewMachineTypeInfo("b", 4, 8),
			expected:    true,
		},
		"same cpu, less mem": {
			machineType: NewMachineTypeInfo("a", 4, 8),
			otherType:   NewMachineTypeInfo("b", 4, 16),
			expected:    false,
		},
		"same cpu and mem": {
			machineType: NewMachineTypeInfo("a", 4, 8),
			otherType:   NewMachineTypeInfo("b", 4, 8),
			expected:    false,
		},
		"same cpu and mem, has localSSD": {
			machineType: NewMachineTypeInfo("a", 4, 8).withAutomaticEphemeralLocalSsdCount(1),
			otherType:   NewMachineTypeInfo("b", 4, 8),
			expected:    true,
		},
		"same cpu and mem, more localSSD": {
			machineType: NewMachineTypeInfo("a", 4, 8).withAutomaticEphemeralLocalSsdCount(2),
			otherType:   NewMachineTypeInfo("b", 4, 8).withAutomaticEphemeralLocalSsdCount(1),
			expected:    true,
		},
		"same cpu and mem, less localSSD": {
			machineType: NewMachineTypeInfo("a", 4, 8).withAutomaticEphemeralLocalSsdCount(1),
			otherType:   NewMachineTypeInfo("b", 4, 8).withAutomaticEphemeralLocalSsdCount(2),
			expected:    false,
		},
		"same cpu and mem, no localSSD": {
			machineType: NewMachineTypeInfo("a", 4, 8),
			otherType:   NewMachineTypeInfo("b", 4, 8).withAutomaticEphemeralLocalSsdCount(1),
			expected:    false,
		},
		"same cpu and mem, same localSSD": {
			machineType: NewMachineTypeInfo("a", 4, 8).withAutomaticEphemeralLocalSsdCount(1),
			otherType:   NewMachineTypeInfo("b", 4, 8).withAutomaticEphemeralLocalSsdCount(1),
			expected:    false,
		},
		"same cpu and less mem, more localSSD": {
			machineType: NewMachineTypeInfo("a", 4, 4).withAutomaticEphemeralLocalSsdCount(2),
			otherType:   NewMachineTypeInfo("b", 4, 8).withAutomaticEphemeralLocalSsdCount(1),
			expected:    false,
		},
		"less cpu and same mem, more localSSD": {
			machineType: NewMachineTypeInfo("a", 2, 8).withAutomaticEphemeralLocalSsdCount(2),
			otherType:   NewMachineTypeInfo("b", 4, 8).withAutomaticEphemeralLocalSsdCount(1),
			expected:    false,
		},
		"more cpu and less mem, less localSSD": {
			machineType: NewMachineTypeInfo("a", 8, 8).withAutomaticEphemeralLocalSsdCount(2),
			otherType:   NewMachineTypeInfo("b", 4, 16).withAutomaticEphemeralLocalSsdCount(1),
			expected:    true,
		},
		"same cpu and more mem, less localSSD": {
			machineType: NewMachineTypeInfo("a", 4, 16).withAutomaticEphemeralLocalSsdCount(2),
			otherType:   NewMachineTypeInfo("b", 4, 8).withAutomaticEphemeralLocalSsdCount(1),
			expected:    true,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			assert.Equal(t, IsLargerThan(tc.machineType, tc.otherType), tc.expected)
		})
	}
}

func TestLargest(t *testing.T) {
	t1 := NewMachineTypeInfo("t1", 2, 8)
	t2 := NewMachineTypeInfo("t2", 4, 16)
	t3 := NewMachineTypeInfo("t3", 8, 32)

	for tn, tc := range map[string]struct {
		machineTypes    map[string]MachineType
		expectedLargest MachineType
	}{
		"no machine types": {
			machineTypes:    nil,
			expectedLargest: UnknownMachineType,
		},
		"single machine type": {
			machineTypes:    onboardMachineType(t1),
			expectedLargest: t1,
		},
		"multiple machine types": {
			machineTypes:    onboardMachineType(t1, t2, t3),
			expectedLargest: t3,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			assert.Equal(t, Largest(tc.machineTypes), tc.expectedLargest)
		})
	}
}

func TestValidateMachineFamilyConfig(t *testing.T) {
	machineFamilies := NewMachineConfigProvider(nil).AllMachineFamilies()
	assert.NotEmpty(t, machineFamilies)

	for _, machineFamily := range machineFamilies {
		t.Run(machineFamily.name, func(t *testing.T) {
			mcp := NewMachineConfigProvider(nil)

			// Check if the family name can be used to convert back to the object.
			_, err := mcp.ToMachineFamily(machineFamily.name)
			assert.NoError(t, err)
			// Non-lowercase family names should still work.
			_, err = mcp.ToMachineFamily(strings.ToUpper(machineFamily.name))
			assert.NoError(t, err)

			// Check if the family has at least one machine type
			assert.NotEmpty(t, machineFamily.AllMachineTypes(NoConstraints))

			// Check if all autoprovisioned machine types are contained in all machine types.
			assert.Subset(t, machineFamily.AllMachineTypes(NoConstraints), machineFamily.AutoprovisionedMachineTypes(NoConstraints))
			assert.Contains(t, machineFamily.AllMachineTypes(NoConstraints), machineFamily.LargestMachineType(NoConstraints).Name)

			// Validate CPU platform requirements.
			validateRequirements(t, machineFamily.supportedCpuPlatforms)
			// Validate CPU platform overrides.
			for _, machineType := range machineFamily.AllMachineTypes(NoConstraints) {
				if req, found := machineType.cpuPlatformRequirementsOverrides(); found {
					validateRequirements(t, req)
				}
				// validate machine name parsing back to machine family
				validateParsingMachineName(t, machineType.Name)
			}
		})
	}
}

func validateRequirements(t *testing.T, requirements CpuPlatformRequirements) {
	if requirements == noPlatformSupported {
		return
	}

	lower, lowerFound := cpuPlatforms.get(requirements.lowerBound)
	upper, upperFound := cpuPlatforms.get(requirements.upperBound)
	assert.True(t, lowerFound)
	assert.True(t, upperFound)
	assert.GreaterOrEqual(t, upper.order, lower.order)
}

func TestInvalidMachineFamily(t *testing.T) {
	_, err := NewMachineConfigProvider(nil).ToMachineFamily("xyz")
	assert.Error(t, err)
}

func validateParsingMachineName(t *testing.T, machineName string) {
	mcp := NewMachineConfigProvider(nil)
	f, err := mcp.GetMachineFamilyFromMachineName(machineName)
	assert.NoError(t, err)
	familyMachines := f.AllMachineTypes(NoConstraints)
	assert.Contains(t, familyMachines, machineName)
}

func TestGetMachineFamilyFromMachineName(t *testing.T) {
	for tn, tc := range map[string]struct {
		machineName   string
		family        MachineFamily
		expectedError bool
		injectConfig  bool
	}{
		"standard machine name": {
			machineName:   machineTypeA2,
			family:        A2,
			expectedError: false,
		},
		"custom machine name": {
			machineName:   "n1-custom-64",
			family:        N1,
			expectedError: false,
		},
		"invalid machine name format": {
			machineName:   "xyz-badmachine",
			expectedError: true,
		},
		"non existent machine family": {
			machineName:   "badfamily-standard-21",
			expectedError: true,
		},
		"n2d-standard-2-sev resolves to n2d-vm-sev when registered": {
			machineName:   "n2d-standard-2-sev",
			family:        MachineFamily{name: "n2d-vm-sev"},
			expectedError: false,
			injectConfig:  true,
		},
		"n2d-standard-2-sev-snp resolves to n2d-vm-sev-snp when registered": {
			machineName:   "n2d-standard-2-sev-snp",
			family:        MachineFamily{name: "n2d-vm-sev-snp"},
			expectedError: false,
			injectConfig:  true,
		},
		"n2d-standard-2 resolves to n2d": {
			machineName:   "n2d-standard-2",
			family:        N2D,
			expectedError: false,
		},
		"n2d-standard-2-sev falls back to n2d when variant is not registered": {
			machineName:   "n2d-standard-2-sev",
			family:        N2D,
			expectedError: false,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			mcp := NewMachineConfigProvider(nil)
			if tc.injectConfig {
				families := make(map[string]MachineFamily)
				for k, v := range mcp.cache.machineFamilies() {
					families[k] = v
				}
				families["n2d-vm-sev"] = MachineFamily{name: "n2d-vm-sev"}
				families["n2d-vm-sev-snp"] = MachineFamily{name: "n2d-vm-sev-snp"}
				mcp.cache.update(families)
			}
			actualFamily, err := mcp.GetMachineFamilyFromMachineName(tc.machineName)
			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.family, actualFamily)
			}
		})
	}
}

func TestIsPlatformSupported(t *testing.T) {
	for tn, tc := range map[string]struct {
		family   MachineFamily
		platform CpuPlatform
		expected bool
	}{
		"no autoprovisioned machine types defined - no platform supported": {
			family: MachineFamily{
				supportedCpuPlatforms: CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
			},
			platform: AnyPlatform,
			expected: false,
		},
		"AnyPlatform always supported if there are autoprovisioned machine types": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
			},
			platform: AnyPlatform,
			expected: true,
		},
		"AnyPlatform always supported, even for noPlatformSupported": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedCpuPlatforms:       noPlatformSupported,
			},
			platform: AnyPlatform,
			expected: true,
		},
		"UnknownPlatform never supported": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
			},
			platform: UnknownPlatform,
			expected: false,
		},
		"UnknownPlatform never supported, even for noPlatformSupported": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedCpuPlatforms:       noPlatformSupported,
			},
			platform: UnknownPlatform,
			expected: false,
		},
		"supported for the whole family": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
			},
			platform: IntelSkylake,
			expected: true,
		},
		"supported for the whole family, but no autoprovisioned types": {
			family: MachineFamily{
				supportedCpuPlatforms: CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
			},
			platform: IntelSkylake,
			expected: false,
		},
		"supported for the whole family, but all autoprovisioned types actually overridden": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0).
					withCpuPlatformRequirements(noPlatformSupported),
					NewMachineTypeInfo("machine-type-2", 0, 0).
						withCpuPlatformRequirements(noPlatformSupported)),
				supportedCpuPlatforms: CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
			},
			platform: IntelSkylake,
			expected: false,
		},
		"not supported for the whole family": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
			},
			platform: IntelHaswell,
			expected: false,
		},
		"supported for one of the overrides": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0).
					withCpuPlatformRequirements(CpuPlatformRequirements{lowerBound: IntelHaswell, upperBound: IntelHaswell}),
					NewMachineTypeInfo("machine-type-2", 0, 0).
						withCpuPlatformRequirements(CpuPlatformRequirements{lowerBound: IntelSandyBridge, upperBound: IntelSandyBridge})),
				supportedCpuPlatforms: CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
			},
			platform: IntelHaswell,
			expected: true,
		},
		"supported for one of the overrides, but it isn't autoprovisioned": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-2", 0, 0).
					withCpuPlatformRequirements(CpuPlatformRequirements{lowerBound: IntelSandyBridge, upperBound: IntelSandyBridge})),
				otherMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0).
					withCpuPlatformRequirements(CpuPlatformRequirements{lowerBound: IntelHaswell, upperBound: IntelHaswell})),
				supportedCpuPlatforms: CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
			},
			platform: IntelHaswell,
			expected: false,
		},
		"not supported in any of the overrides": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0).
					withCpuPlatformRequirements(CpuPlatformRequirements{lowerBound: IntelHaswell, upperBound: IntelHaswell}),
					NewMachineTypeInfo("machine-type-2", 0, 0).
						withCpuPlatformRequirements(CpuPlatformRequirements{lowerBound: IntelSandyBridge, upperBound: IntelSandyBridge})),
				supportedCpuPlatforms: CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
			},
			platform: IntelIvyBridge,
			expected: false,
		},
		"supported for the whole family, but not supported for some overrides": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0).
					withCpuPlatformRequirements(CpuPlatformRequirements{lowerBound: IntelHaswell, upperBound: IntelHaswell}),
					NewMachineTypeInfo("machine-type-2", 0, 0).
						withCpuPlatformRequirements(CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelBroadwell}),
					NewMachineTypeInfo("machine-type-3", 0, 0)),
				supportedCpuPlatforms: CpuPlatformRequirements{lowerBound: IntelSandyBridge, upperBound: IntelIvyBridge},
			},
			platform: IntelIvyBridge,
			expected: true,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			got := tc.family.IsPlatformSupported(tc.platform)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestIsGpuTypeSupported(t *testing.T) {
	gpuType1 := NewTestGpu("gpu-type-1", true, nil, nil)
	gpuType2 := NewTestGpu("gpu-type-2", true, nil, nil)
	gpuType3 := NewTestGpu("gpu-type-3", true, nil, nil)
	for tn, tc := range map[string]struct {
		family   MachineFamily
		gpuType  string
		expected bool
	}{
		"no autoprovisioned types defined - no GPU type supported": {
			family: MachineFamily{
				supportedGpuTypes: onboardSupportedGpus(gpuType1),
			},
			gpuType:  "",
			expected: false,
		},
		"empty GPU type always supported if there are autoprovisioned types defined": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedGpuTypes:           onboardSupportedGpus(gpuType1),
			},
			gpuType:  "",
			expected: true,
		},
		"empty GPU type always supported, even if no GPUs listed as supported": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
			},
			gpuType:  "",
			expected: true,
		},
		"GPU type supported by whole family": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedGpuTypes:           onboardSupportedGpus(gpuType1),
			},
			gpuType:  "gpu-type-1",
			expected: true,
		},
		"GPU type supported by whole family, but no autoprovisioned types defined": {
			family: MachineFamily{
				supportedGpuTypes: onboardSupportedGpus(gpuType1),
			},
			gpuType:  "gpu-type-1",
			expected: false,
		},
		"GPU type supported by whole family, but all autoprovisioned types actually overridden": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0).
					withGpuOverride(gpuType2, 0),
					NewMachineTypeInfo("machine-type-2", 0, 0).
						withGpuOverride(gpuType2, 0)),
				supportedGpuTypes: onboardSupportedGpus(gpuType1),
			},
			gpuType:  "gpu-type-1",
			expected: false,
		},
		"GPU type not supported by whole family": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedGpuTypes:           onboardSupportedGpus(gpuType1),
			},
			gpuType:  "gpu-type-2",
			expected: false,
		},
		"supported for one of the overrides": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0).
					withGpuOverride(gpuType2, 0),
					NewMachineTypeInfo("machine-type-2", 0, 0).
						withGpuOverride(gpuType3, 0)),
				supportedGpuTypes: onboardSupportedGpus(gpuType1),
			},
			gpuType:  "gpu-type-2",
			expected: true,
		},
		"supported for one of the overrides with explicitReqOnly": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(
					NewMachineTypeInfo("machine-type-1", 0, 0).
						withGpuOverride(gpuType2, 0).
						withExplicitReqOnly(),
					NewMachineTypeInfo("machine-type-2", 0, 0).
						withGpuOverride(gpuType3, 0)),
				supportedGpuTypes: onboardSupportedGpus(gpuType1),
			},
			gpuType:  "gpu-type-2",
			expected: true,
		},
		"supported for one of the overrides, but it isn't autoprovisioned": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-2", 0, 0).
					withGpuOverride(gpuType3, 0)),
				otherMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0).
					withGpuOverride(gpuType2, 0)),
				supportedGpuTypes: onboardSupportedGpus(gpuType1),
			},
			gpuType:  "gpu-type-2",
			expected: false,
		},
		"not supported in any of the overrides": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0).
					withGpuOverride(gpuType2, 0),
					NewMachineTypeInfo("machine-type-2", 0, 0).
						withGpuOverride(gpuType3, 0)),
				supportedGpuTypes: onboardSupportedGpus(gpuType1),
			},
			gpuType:  "gpu-type-4",
			expected: false,
		},
		"supported for the whole family, but not supported for some overrides": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0).
					withGpuOverride(gpuType2, 0),
					NewMachineTypeInfo("machine-type-2", 0, 0).
						withGpuOverride(gpuType3, 0),
					NewMachineTypeInfo("machine-type-3", 0, 0)),
				supportedGpuTypes: onboardSupportedGpus(gpuType1),
			},
			gpuType:  "gpu-type-1",
			expected: true,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			got := tc.family.IsGpuTypeSupported(tc.gpuType)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestIsDiskTypeSupported(t *testing.T) {
	for tn, tc := range map[string]struct {
		family   MachineFamily
		diskType string
		expected bool
	}{
		"machine family: C2 - supports pd-standard": {
			family:   C2,
			diskType: DiskTypeStandard,
			expected: true,
		},
		"machine family: H3 - does NOT support pd-standard": {
			family:   H3,
			diskType: DiskTypeStandard,
			expected: false,
		},
		"machine family: M1 - supports pd-balanced": {
			family:   M1,
			diskType: DiskTypeBalanced,
			expected: true,
		},
		"only N4 does NOT supports pd-balanced": {
			family:   N4,
			diskType: DiskTypeBalanced,
			expected: false,
		},
		"machine family: N1 - supports pd-ssd": {
			family:   N1,
			diskType: DiskTypeSSD,
			expected: true,
		},
		"machine family: H3 - does NOT support pd-ssd": {
			family:   H3,
			diskType: DiskTypeSSD,
			expected: false,
		},
		"machine family: N4 - supports hyperdisk-balanced": {
			family:   N4,
			diskType: DiskTypeHyperdiskBalanced,
			expected: true,
		},
		"machine family: E2 - does NOT support hyperdisk-balanced": {
			family:   E2,
			diskType: DiskTypeHyperdiskBalanced,
			expected: false,
		},
		"machine family: A3 - supports hyperdisk-balanced": {
			family:   A3,
			diskType: DiskTypeHyperdiskBalanced,
			expected: true,
		},
	} {
		t.Run(tn, func(t *testing.T) {

			got := tc.family.IsDiskTypeSupported(tc.diskType)
			assert.Equal(t, tc.expected, got, "Machine family: %v", tc.family)

		})
	}
}

func TestIsDiskTypeSupportedForMachineType(t *testing.T) {
	for tn, tc := range map[string]struct {
		family      MachineFamily
		machineType string
		diskType    string
		expected    bool
	}{
		"machine family: C2, machine type: c2-standard-4 - supports pd-standard": {
			family:      C2,
			diskType:    DiskTypeStandard,
			machineType: "c2-standard-4",
			expected:    true,
		},
		"machine family: H3, machine type: h3-standard-88 - does NOT support pd-standard": {
			family:      H3,
			diskType:    DiskTypeStandard,
			machineType: "h3-standard-88",
			expected:    false,
		},
		"machine family: M1, machine type: m1-ultramem-40 - supports pd-balanced": {
			family:      M1,
			diskType:    DiskTypeBalanced,
			machineType: "m1-ultramem-40",
			expected:    true,
		},
		"machine family:  N4, machine type: n4-standard-2 - does NOT support pd-balanced": {
			family:      N4,
			diskType:    DiskTypeBalanced,
			machineType: "n4-standard-2",
			expected:    false,
		},
		"machine family: N1, machine type: n1-standard-1 - supports pd-ssd": {
			family:      N1,
			diskType:    DiskTypeSSD,
			machineType: "n1-standard-1",
			expected:    true,
		},
		"machine family: H3, machine type: h3-standard-88 - does NOT support pd-ssd": {
			family:      H3,
			diskType:    DiskTypeSSD,
			machineType: "h3-standard-88",
			expected:    false,
		},
		"machine family: N4, machine type: n4-standard-2 - supports hyperdisk-balanced": {
			family:      N4,
			diskType:    DiskTypeHyperdiskBalanced,
			machineType: "n4-standard-2",
			expected:    true,
		},
		"machine family: E2, machine type: e2-standard-2 - does NOT support hyperdisk-balanced": {
			family:      E2,
			diskType:    DiskTypeHyperdiskBalanced,
			machineType: "e2-standard-2",
			expected:    false,
		},
		"machine family: A3, machine type: a3-ultragpu-8g - does NOT support pd-ssd": {
			family:      A3,
			diskType:    DiskTypeSSD,
			machineType: "a3-ultragpu-8g",
			expected:    false,
		},
	} {
		t.Run(tn, func(t *testing.T) {

			got := tc.family.IsDiskTypeSupportedForMachineType(tc.diskType, tc.machineType)
			assert.Equal(t, tc.expected, got, "Machine family: %v, machine type: %v", tc.family, tc.machineType)

		})
	}
}

func TestAreConstraintsSupported(t *testing.T) {
	gpuType1 := NewTestGpu("gpu-type-1", true, nil, nil)

	for tn, tc := range map[string]struct {
		family      MachineFamily
		constraints Constraints
		expected    bool
	}{
		"no autoprovisioned types defined - no constraints supported": {
			family:      MachineFamily{},
			constraints: NoConstraints,
			expected:    false,
		},
		"NoConstraints always supported if there are autoprovisioned types defined": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
			},
			constraints: NoConstraints,
			expected:    true,
		},
		"GPU type constraint is taken into account - compatible": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedGpuTypes:           onboardSupportedGpus(gpuType1),
			},
			constraints: Constraints{CpuPlatform: AnyPlatform, GpuType: "gpu-type-1"},
			expected:    true,
		},
		"GPU type constraint is taken into account - not compatible": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedGpuTypes:           onboardSupportedGpus(gpuType1),
			},
			constraints: Constraints{CpuPlatform: AnyPlatform, GpuType: "gpu-type-2"},
			expected:    false,
		},
		"TPU type constraint is taken into account - compatible": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedTpuTypes:           map[string]bool{"tpu-type-1": true},
			},
			constraints: Constraints{CpuPlatform: AnyPlatform, TpuType: "tpu-type-1"},
			expected:    true,
		},
		"TPU type constraint is taken into account - not compatible": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedTpuTypes:           map[string]bool{"tpu-type-1": true},
			},
			constraints: Constraints{CpuPlatform: AnyPlatform, TpuType: "tpu-type-2"},
			expected:    false,
		},
		"CPU platform constraint is taken into account - compatible": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
			},
			constraints: Constraints{CpuPlatform: IntelSkylake, GpuType: ""},
			expected:    true,
		},
		"CPU platform is taken into account - not compatible": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
			},
			constraints: Constraints{CpuPlatform: IntelIceLake, GpuType: ""},
			expected:    false,
		},
		"multiple constraints are taken into account - 2x compatible (CPU platform and GPU)": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
				supportedGpuTypes:           onboardSupportedGpus(gpuType1),
			},
			constraints: Constraints{CpuPlatform: IntelSkylake, GpuType: "gpu-type-1"},
			expected:    true,
		},
		"multiple constraints are taken into account - 2x compatible (CPU platform and TPU)": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
				supportedTpuTypes:           map[string]bool{"tpu-type-1": true},
			},
			constraints: Constraints{CpuPlatform: IntelSkylake, TpuType: "tpu-type-1"},
			expected:    true,
		},
		"multiple constraints are taken into account - 2x not compatible (CPU platform and GPU)": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
				supportedGpuTypes:           onboardSupportedGpus(gpuType1),
			},
			constraints: Constraints{CpuPlatform: IntelIceLake, GpuType: "gpu-type-2"},
			expected:    false,
		},
		"multiple constraints are taken into account - 2x not compatible (CPU platform and TPU)": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
				supportedTpuTypes:           map[string]bool{"tpu-type-1": true},
			},
			constraints: Constraints{CpuPlatform: IntelIceLake, TpuType: "tpu-type-2"},
			expected:    false,
		},
		"multiple constraints are taken into account - GPU type compatible, CPU platform not compatible": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
				supportedGpuTypes:           onboardSupportedGpus(gpuType1),
			},
			constraints: Constraints{CpuPlatform: IntelIceLake, GpuType: "gpu-type-1"},
			expected:    false,
		},
		"multiple constraints are taken into account - GPU type not compatible, CPU platform compatible": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
				supportedGpuTypes:           onboardSupportedGpus(gpuType1),
			},
			constraints: Constraints{CpuPlatform: IntelSkylake, GpuType: "gpu-type-2"},
			expected:    false,
		},
		"multiple constraints are taken into account - TPU type compatible, CPU platform not compatible": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
				supportedTpuTypes:           map[string]bool{"tpu-type-1": true},
			},
			constraints: Constraints{CpuPlatform: IntelIceLake, TpuType: "tpu-type-1"},
			expected:    false,
		},
		"multiple constraints are taken into account - TPU type not compatible, CPU platform compatible": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
				supportedTpuTypes:           map[string]bool{"tpu-type-1": true},
			},
			constraints: Constraints{CpuPlatform: IntelSkylake, TpuType: "tpu-type-2"},
			expected:    false,
		},
		"multiple constraints are taken into account - GPU type compatible, TPU not compatible": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
				supportedGpuTypes:           onboardSupportedGpus(gpuType1),
				supportedTpuTypes:           map[string]bool{"tpu-type-1": true},
			},
			constraints: Constraints{CpuPlatform: AnyPlatform, GpuType: "gpu-type-1", TpuType: "tpu-type-2"},
			expected:    false,
		},
		"multiple constraints are taken into account - GPU type not compatible, TPU compatible": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
				supportedGpuTypes:           onboardSupportedGpus(gpuType1),
				supportedTpuTypes:           map[string]bool{"tpu-type-1": true},
			},
			constraints: Constraints{CpuPlatform: AnyPlatform, GpuType: "gpu-type-2", TpuType: "tpu-type-1"},
			expected:    false,
		},
		"multiple constraints are taken into account - 3x compatible": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
				supportedGpuTypes:           onboardSupportedGpus(gpuType1),
				supportedTpuTypes:           map[string]bool{"tpu-type-1": true},
			},
			constraints: Constraints{CpuPlatform: IntelSkylake, GpuType: "gpu-type-1", TpuType: "tpu-type-1"},
			expected:    true,
		},
		"multiple constraints are taken into account - 3x not compatible": {
			family: MachineFamily{
				autoprovisionedMachineTypes: onboardMachineType(NewMachineTypeInfo("machine-type-1", 0, 0)),
				supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
				supportedGpuTypes:           onboardSupportedGpus(gpuType1),
				supportedTpuTypes:           map[string]bool{"tpu-type-1": true},
			},
			constraints: Constraints{CpuPlatform: IntelIceLake, GpuType: "gpu-type-2", TpuType: "tpu-type-2"},
			expected:    false,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			got := tc.family.AreConstraintsSupported(tc.constraints)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestAllMachineTypes(t *testing.T) {
	gpuType1 := NewTestGpu("gpu-type-1", true, nil, nil)
	gpuType2 := NewTestGpu("gpu-type-2", true, nil, nil)
	gpuType3 := NewTestGpu("gpu-type-3", true, nil, nil)

	// Singleton within family requirements.
	t1 := NewMachineTypeInfo("t1", 1, 1).
		withCpuPlatformRequirements(CpuPlatformRequirements{lowerBound: IntelSkylake, upperBound: IntelSkylake})
	// Subrange completely within family requirements.
	t2 := NewMachineTypeInfo("t2", 2, 2).withCpuPlatformRequirements(CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelSkylake})
	// Subrange overlapping family requirements but going above.
	t3 := NewMachineTypeInfo("t3", 3, 3).withCpuPlatformRequirements(CpuPlatformRequirements{lowerBound: IntelSkylake, upperBound: IntelCascadeLake})
	// Singleton above family requirements.
	t4 := NewMachineTypeInfo("t4", 4, 4).withCpuPlatformRequirements(CpuPlatformRequirements{lowerBound: IntelCascadeLake, upperBound: IntelCascadeLake})
	// Subrange completely above family requirements.
	t5 := NewMachineTypeInfo("t5", 5, 5).withCpuPlatformRequirements(CpuPlatformRequirements{lowerBound: IntelCascadeLake, upperBound: IntelIceLake})
	t6 := NewMachineTypeInfo("t6", 6, 6).withGpuOverride(gpuType3, 0)
	t7 := NewMachineTypeInfo("t7", 7, 7).withGpuOverride(gpuType3, 0)

	testFamily := MachineFamily{
		autoprovisionedMachineTypes: onboardMachineType(t1, t2, t3, t4, t5, t6, t7),
		supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelIvyBridge, upperBound: IntelSkylake},
		supportedGpuTypes:           onboardSupportedGpus(gpuType1, gpuType2),
		supportedTpuTypes:           map[string]bool{"tpu-type-1": true},
	}
	testFamily.precomputeAllMachineTypes()

	for tn, tc := range map[string]struct {
		constraints   Constraints
		expectedTypes map[string]MachineType
	}{
		"No Constraints": {
			constraints: NoConstraints,
			// expectedTypes: map[string]MachineType{},
			expectedTypes: onboardMachineType(t1, t2, t3, t4, t5, t6, t7),
		},
		"UnknownPlatform always returns nothing": {
			constraints:   Constraints{CpuPlatform: UnknownPlatform},
			expectedTypes: map[string]MachineType{},
		},
		"AnyPlatform always returns all": {
			constraints:   Constraints{CpuPlatform: AnyPlatform},
			expectedTypes: onboardMachineType(t1, t2, t3, t4, t5, t6, t7),
		},
		"platform not supported by anything": {
			constraints:   Constraints{CpuPlatform: AmdRome},
			expectedTypes: map[string]MachineType{},
		},
		"platform supported by family requirements, but none of the overrides": {
			constraints:   Constraints{CpuPlatform: IntelIvyBridge},
			expectedTypes: onboardMachineType(t6, t7),
		},
		"platform supported by family requirements, and a subrange override": {
			constraints:   Constraints{CpuPlatform: IntelBroadwell},
			expectedTypes: onboardMachineType(t2, t6, t7),
		},
		"platform supported by family requirements, a singleton override, and 2 subrange overrides": {
			constraints:   Constraints{CpuPlatform: IntelSkylake},
			expectedTypes: onboardMachineType(t1, t2, t3, t6, t7),
		},
		"platform not supported by family requirements, supported by a singleton override, and 2 subrange overrides": {
			constraints:   Constraints{CpuPlatform: IntelCascadeLake},
			expectedTypes: onboardMachineType(t3, t4, t5),
		},
		"platform not supported by family requirements, supported by a subrange override": {

			constraints:   Constraints{CpuPlatform: IntelIceLake},
			expectedTypes: onboardMachineType(t5),
		},
		"GPU type supported by family-wide field": {
			constraints:   Constraints{GpuType: "gpu-type-1", CpuPlatform: AnyPlatform},
			expectedTypes: onboardMachineType(t1, t2, t3, t4, t5),
		},
		"GPU type supported by overrides": {
			constraints:   Constraints{GpuType: "gpu-type-3", CpuPlatform: AnyPlatform},
			expectedTypes: onboardMachineType(t6, t7),
		},
		"GPU type not supported by anything": {
			constraints:   Constraints{GpuType: "gpu-type-4", CpuPlatform: AnyPlatform},
			expectedTypes: map[string]MachineType{},
		},
		"TPU type supported by family-wide field": {
			constraints:   Constraints{TpuType: "tpu-type-1", CpuPlatform: AnyPlatform},
			expectedTypes: onboardMachineType(t1, t2, t3, t4, t5, t6, t7),
		},
		"TPU type not supported by anything": {
			constraints:   Constraints{TpuType: "tpu-type-2", CpuPlatform: AnyPlatform},
			expectedTypes: map[string]MachineType{},
		},
		"both platform and GPU type imposing restrictions, some types left": {
			constraints:   Constraints{GpuType: "gpu-type-1", CpuPlatform: IntelSkylake},
			expectedTypes: onboardMachineType(t1, t2, t3),
		},
		"both platform and TPU type imposing restrictions, some types left": {
			constraints:   Constraints{TpuType: "tpu-type-1", CpuPlatform: IntelSkylake},
			expectedTypes: onboardMachineType(t1, t2, t3, t6, t7),
		},
		"both platform and GPU type imposing restrictions, all types filtered out": {
			constraints:   Constraints{GpuType: "gpu-type-3", CpuPlatform: IntelCascadeLake},
			expectedTypes: map[string]MachineType{},
		},
		"both platform and TPU type imposing restrictions, all types filtered out": {
			constraints:   Constraints{TpuType: "tpu-type-4", CpuPlatform: IntelCascadeLake},
			expectedTypes: map[string]MachineType{},
		},
		"all platform, GPU and TPU type impose restrictions, some types left": {
			constraints:   Constraints{GpuType: "gpu-type-1", CpuPlatform: IntelSkylake, TpuType: "tpu-type-1"},
			expectedTypes: onboardMachineType(t1, t2, t3),
		},
		"all platform, GPU and TPU type impose restrictions (gpu override), some types left": {
			constraints:   Constraints{GpuType: "gpu-type-3", CpuPlatform: IntelSkylake, TpuType: "tpu-type-1"},
			expectedTypes: onboardMachineType(t6, t7),
		},
		"all platform, GPU and TPU type impose restrictions, all types filtered out": {
			constraints:   Constraints{GpuType: "gpu-type-3", CpuPlatform: IntelCascadeLake, TpuType: "tpu-type-1"},
			expectedTypes: map[string]MachineType{},
		},
		"all platform, GPU and TPU type impose restrictions, all types filtered out (unsupported tpu)": {
			constraints:   Constraints{GpuType: "gpu-type-1", CpuPlatform: IntelSkylake, TpuType: "tpu-type-2"},
			expectedTypes: map[string]MachineType{},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			assert.Equal(t, tc.expectedTypes, testFamily.AllMachineTypes(tc.constraints))
		})
	}
}

func TestAutoprovisionedMachineTypes(t *testing.T) {
	gpuType1 := NewTestGpu("gpu-type-1", true, nil, nil)
	gpuType2 := NewTestGpu("gpu-type-2", true, nil, nil)
	gpuType3 := NewTestGpu("gpu-type-3", true, nil, nil)

	// Singleton within family requirements.
	t1 := NewMachineTypeInfo("t1", 1, 1).
		withCpuPlatformRequirements(CpuPlatformRequirements{lowerBound: IntelSkylake, upperBound: IntelSkylake})
	// Subrange completely within family requirements.
	t2 := NewMachineTypeInfo("t2", 2, 2).
		withCpuPlatformRequirements(CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelSkylake})
	// Subrange overlapping family requirements but going above.
	t3 := NewMachineTypeInfo("t3", 3, 3).
		withCpuPlatformRequirements(CpuPlatformRequirements{lowerBound: IntelSkylake, upperBound: IntelCascadeLake})
	// Singleton above family requirements.
	t4 := NewMachineTypeInfo("t4", 4, 4).
		withCpuPlatformRequirements(CpuPlatformRequirements{lowerBound: IntelCascadeLake, upperBound: IntelCascadeLake})
	// Subrange completely above family requirements.
	t5 := NewMachineTypeInfo("t5", 5, 5).
		withCpuPlatformRequirements(CpuPlatformRequirements{lowerBound: IntelCascadeLake, upperBound: IntelIceLake})
	t6 := NewMachineTypeInfo("t6", 6, 6).
		withGpuOverride(gpuType3, 0)
	t7 := NewMachineTypeInfo("t7", 7, 7).
		withGpuOverride(gpuType3, 0)
	t8 := NewMachineTypeInfo("t8", 8, 8).
		withGpuOverride(gpuType3, 0).
		withExplicitReqOnly()
	t9 := NewMachineTypeInfo("t9", 9, 9).
		withExplicitReqOnly()

	testFamily := MachineFamily{
		autoprovisionedMachineTypes: onboardMachineType(t1, t2, t3, t4, t5, t6, t7, t8, t9),
		supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelIvyBridge, upperBound: IntelSkylake},
		supportedGpuTypes:           onboardSupportedGpus(gpuType1, gpuType2),
	}

	t1NoOverride := NewMachineTypeInfo("t1", 1, 1)
	t2NoOverride := NewMachineTypeInfo("t2", 2, 2)
	t3NoOverride := NewMachineTypeInfo("t3", 3, 3)
	noOverridesFamily := MachineFamily{
		autoprovisionedMachineTypes: onboardMachineType(t1NoOverride, t2NoOverride, t3NoOverride),
		supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelIvyBridge, upperBound: IntelHaswell},
	}
	for tn, tc := range map[string]struct {
		family              MachineFamily
		constraints         Constraints
		expectedTypes       map[string]MachineType
		expectedLargestType MachineType
	}{
		"NoConstraints always returns all types": {
			family:              testFamily,
			constraints:         NoConstraints,
			expectedTypes:       onboardMachineType(t1, t2, t3, t4, t5, t6, t7, t8, t9),
			expectedLargestType: t9,
		},
		"UnknownPlatform always returns nothing": {
			family:              testFamily,
			constraints:         Constraints{CpuPlatform: UnknownPlatform},
			expectedTypes:       map[string]MachineType{},
			expectedLargestType: UnknownMachineType,
		},
		"platform not supported by anything": {
			family:              testFamily,
			constraints:         Constraints{CpuPlatform: AmdRome},
			expectedTypes:       map[string]MachineType{},
			expectedLargestType: UnknownMachineType,
		},
		"platform supported by family requirements, but none of the overrides": {
			family:              testFamily,
			constraints:         Constraints{CpuPlatform: IntelIvyBridge},
			expectedTypes:       onboardMachineType(t6, t7),
			expectedLargestType: t7,
		},
		"platform supported by family requirements, and a subrange override": {
			family:              testFamily,
			constraints:         Constraints{CpuPlatform: IntelBroadwell},
			expectedTypes:       onboardMachineType(t2, t6, t7),
			expectedLargestType: t7,
		},
		"platform supported by family requirements, a singleton override, and 2 subrange overrides": {
			family:              testFamily,
			constraints:         Constraints{CpuPlatform: IntelSkylake},
			expectedTypes:       onboardMachineType(t1, t2, t3, t6, t7),
			expectedLargestType: t7,
		},
		"platform not supported by family requirements, supported by a singleton override, and 2 subrange overrides": {
			family:              testFamily,
			constraints:         Constraints{CpuPlatform: IntelCascadeLake},
			expectedTypes:       onboardMachineType(t3, t4, t5),
			expectedLargestType: t5,
		},
		"platform not supported by family requirements, supported by a subrange override": {
			family:              testFamily,
			constraints:         Constraints{CpuPlatform: IntelIceLake},
			expectedTypes:       onboardMachineType(t5),
			expectedLargestType: t5,
		},
		"family with no platform overrides, platform supported": {
			family:              noOverridesFamily,
			constraints:         Constraints{CpuPlatform: IntelIvyBridge},
			expectedTypes:       onboardMachineType(t1NoOverride, t2NoOverride, t3NoOverride),
			expectedLargestType: t3NoOverride,
		},
		"family with no platform overrides, platform not supported": {
			family:              noOverridesFamily,
			constraints:         Constraints{CpuPlatform: IntelIceLake},
			expectedTypes:       map[string]MachineType{},
			expectedLargestType: UnknownMachineType,
		},
		"GPU type not supported by anything": {
			family:              testFamily,
			constraints:         Constraints{GpuType: "gpu-type-4", CpuPlatform: AnyPlatform},
			expectedTypes:       map[string]MachineType{},
			expectedLargestType: UnknownMachineType,
		},
		"GPU type supported by family-wide field": {
			family:              testFamily,
			constraints:         Constraints{GpuType: "gpu-type-1", CpuPlatform: AnyPlatform},
			expectedTypes:       onboardMachineType(t1, t2, t3, t4, t5),
			expectedLargestType: t5,
		},
		"GPU type supported by overrides": {
			family:              testFamily,
			constraints:         Constraints{GpuType: "gpu-type-3", CpuPlatform: AnyPlatform},
			expectedTypes:       onboardMachineType(t6, t7),
			expectedLargestType: t7,
		},
		"both platform and GPU type imposing restrictions, some types left": {
			family:              testFamily,
			constraints:         Constraints{GpuType: "gpu-type-1", CpuPlatform: IntelSkylake},
			expectedTypes:       onboardMachineType(t1, t2, t3),
			expectedLargestType: t3,
		},
		"both platform and GPU type imposing restrictions, all types filtered out": {
			family:              testFamily,
			constraints:         Constraints{GpuType: "gpu-type-3", CpuPlatform: IntelCascadeLake},
			expectedTypes:       map[string]MachineType{},
			expectedLargestType: UnknownMachineType,
		},
		"Single ExplicitMachineType supported": {
			family:              testFamily,
			constraints:         Constraints{CpuPlatform: AnyPlatform, ExplicitMachineTypes: []string{"t8"}},
			expectedTypes:       onboardMachineType(t8),
			expectedLargestType: t8,
		},
		"Multiple ExplicitMachineType supported": {
			family:              testFamily,
			constraints:         Constraints{CpuPlatform: AnyPlatform, ExplicitMachineTypes: []string{"t8", "t9"}},
			expectedTypes:       onboardMachineType(t8, t9),
			expectedLargestType: t9,
		},
		"Multiple ExplicitMachineType with other constraints supported": {
			family:              testFamily,
			constraints:         Constraints{CpuPlatform: AnyPlatform, GpuType: "gpu-type-3", ExplicitMachineTypes: []string{"t8", "t9"}},
			expectedTypes:       onboardMachineType(t8),
			expectedLargestType: t8,
		},
		"Standard machine types can be specified as explicitly required": {
			family:              testFamily,
			constraints:         Constraints{CpuPlatform: AnyPlatform, ExplicitMachineTypes: []string{"t7", "t8"}},
			expectedTypes:       onboardMachineType(t7, t8),
			expectedLargestType: t8,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			assert.Equal(t, tc.expectedTypes, tc.family.AutoprovisionedMachineTypes(tc.constraints))
			assert.Equal(t, tc.expectedLargestType, tc.family.LargestAutoprovisionedMachineType(tc.constraints))
		})
	}
}

func TestExplicitlySetUnknownCustomMachineTypes(t *testing.T) {
	mcp := NewMachineConfigProvider(nil)
	customN1Cpu4Memory64Name := "custom-4-65536"
	customN1Cpu4Memory64, _ := mcp.ToMachineType(customN1Cpu4Memory64Name)
	customN1Cpu20Memory72Name := "n1-custom-20-73728"
	customN1Cpu20Memory72, _ := mcp.ToMachineType(customN1Cpu20Memory72Name)
	customN1Cpu28Memory300Name := "n1-custom-20-307200-ext"
	customN1Cpu28Memory300, _ := mcp.ToMachineType(customN1Cpu28Memory300Name)
	customN1Cpu48Memory180Name := "custom-48-184320" // known type
	customN1Cpu48Memory180, _ := mcp.ToMachineType(customN1Cpu48Memory180Name)
	customN1Cpu80Memory300Name := "custom-80-307200" // known type
	customN1Cpu80Memory300, _ := mcp.ToMachineType(customN1Cpu80Memory300Name)
	customN1Cpu48Memory312Name := "custom-48-319488" // known type
	customN1Cpu48Memory312, _ := mcp.ToMachineType(customN1Cpu48Memory312Name)
	customN1Cpu80Memory512Name := "custom-80-532480" // known type
	customN1Cpu80Memory512, _ := mcp.ToMachineType(customN1Cpu80Memory512Name)
	customN2Cpu20Memory32Name := "n2-custom-20-32768" //unknown custom type of N2 family
	invalidTypeName := "custom-invalid"               // invalid type

	for tn, tc := range map[string]struct {
		family              MachineFamily
		constraints         Constraints
		expectedCustomTypes map[string]MachineType
	}{
		"N1 machine family with explicitly required custom unknown type custom-4-65536": {
			family: N1,
			constraints: Constraints{CpuPlatform: AnyPlatform, ExplicitMachineTypes: []string{
				customN1Cpu4Memory64Name,
			}},
			expectedCustomTypes: onboardMachineType(customN1Cpu4Memory64),
		},
		"N1 machine family with explicitly required custom known type custom-48-184320": {
			family: N1,
			constraints: Constraints{CpuPlatform: AnyPlatform, ExplicitMachineTypes: []string{
				customN1Cpu48Memory180Name,
			}},
			expectedCustomTypes: onboardMachineType(customN1Cpu48Memory180),
		},
		"N1 machine family with explicitly required custom known type custom-80-307200": {
			family: N1,
			constraints: Constraints{CpuPlatform: AnyPlatform, ExplicitMachineTypes: []string{
				customN1Cpu80Memory300Name,
			}},
			expectedCustomTypes: onboardMachineType(customN1Cpu80Memory300),
		},
		"N1 machine family with explicitly required custom known type custom-48-319488": {
			family: N1,
			constraints: Constraints{CpuPlatform: AnyPlatform, ExplicitMachineTypes: []string{
				customN1Cpu48Memory312Name,
			}},
			expectedCustomTypes: onboardMachineType(customN1Cpu48Memory312),
		},
		"N1 machine family with explicitly required custom known type custom-80-532480": {
			family: N1,
			constraints: Constraints{CpuPlatform: AnyPlatform, ExplicitMachineTypes: []string{
				customN1Cpu80Memory512Name,
			}},
			expectedCustomTypes: onboardMachineType(customN1Cpu80Memory512),
		},
		"N1 machine family with explicitly required custom known type n2-custom-20-32768 belonging to N2 family": {
			family: N1,
			constraints: Constraints{CpuPlatform: AnyPlatform, ExplicitMachineTypes: []string{
				customN2Cpu20Memory32Name,
			}},
		},
		"N1 machine family with explicitly required custom invalid type": {
			family: N1,
			constraints: Constraints{CpuPlatform: AnyPlatform, ExplicitMachineTypes: []string{
				invalidTypeName,
			}},
		},
		"N1 machine family with explicitly required custom unknown type n1-custom-20-73728": {
			family: N1,
			constraints: Constraints{CpuPlatform: AnyPlatform, ExplicitMachineTypes: []string{
				customN1Cpu20Memory72Name,
			}},
			expectedCustomTypes: onboardMachineType(customN1Cpu20Memory72),
		},
		"N1 machine family with explicitly required custom unknown type n1-custom-20-307200-ext with extended memory": {
			family: N1,
			constraints: Constraints{CpuPlatform: AnyPlatform, ExplicitMachineTypes: []string{
				customN1Cpu28Memory300Name,
			}},
			expectedCustomTypes: onboardMachineType(customN1Cpu28Memory300),
		},
		"N1 machine family with multiple explicitly required custom machine types": {
			family: N1,
			constraints: Constraints{CpuPlatform: AnyPlatform, ExplicitMachineTypes: []string{
				customN1Cpu4Memory64Name,
				customN1Cpu48Memory180Name, // known type
				customN1Cpu80Memory300Name, // known type
				customN1Cpu48Memory312Name, // known type
				customN1Cpu80Memory512Name, // known type
				customN1Cpu20Memory72Name,
				customN1Cpu28Memory300Name,
				customN2Cpu20Memory32Name, // from N2 family
				invalidTypeName,           // invalid type
			}},
			expectedCustomTypes: onboardMachineType(
				customN1Cpu4Memory64,
				customN1Cpu20Memory72,
				customN1Cpu28Memory300,
				customN1Cpu48Memory180, // known type
				customN1Cpu80Memory300, // known type
				customN1Cpu48Memory312, // known type
				customN1Cpu80Memory512, // known type
			),
		},
		"N1 machine family with multiple explicitly required custom machine types, boot disk type constraint is supported": {
			family: N1,
			constraints: Constraints{
				CpuPlatform: AnyPlatform,
				ExplicitMachineTypes: []string{
					customN1Cpu4Memory64Name,
					customN1Cpu20Memory72Name,
					customN1Cpu28Memory300Name,
				},
				DiskType: DiskTypeBalanced,
			},
			expectedCustomTypes: onboardMachineType(
				customN1Cpu4Memory64,
				customN1Cpu20Memory72,
				customN1Cpu28Memory300,
			),
		},
		"N1 machine family with multiple explicitly required custom machine types, boot disk type constraint is not supported": {
			family: N1,
			constraints: Constraints{
				CpuPlatform: AnyPlatform,
				ExplicitMachineTypes: []string{
					customN1Cpu4Memory64Name,
					customN1Cpu20Memory72Name,
					customN1Cpu28Memory300Name,
				},
				DiskType: DiskTypeHyperdiskBalanced,
			},
		},
		"N1 machine family with multiple explicitly required custom machine types, gpu constraint is supported": {
			family: N1,
			constraints: Constraints{
				CpuPlatform: AnyPlatform,
				ExplicitMachineTypes: []string{
					customN1Cpu4Memory64Name,
					customN1Cpu20Memory72Name,
					customN1Cpu28Memory300Name,
				},
				GpuType: NvidiaTeslaP100.Name(),
			},
			expectedCustomTypes: onboardMachineType(
				customN1Cpu4Memory64,
				customN1Cpu20Memory72,
				customN1Cpu28Memory300,
			),
		},
		"N1 machine family with multiple explicitly required custom machine types, gpu constraint is not supported": {
			family: N1,
			constraints: Constraints{
				CpuPlatform: AnyPlatform,
				ExplicitMachineTypes: []string{
					customN1Cpu4Memory64Name,
					customN1Cpu20Memory72Name,
					customN1Cpu28Memory300Name,
				},
				GpuType: NvidiaL4.Name(),
			},
		},
		"N1 machine family with multiple explicitly required custom machine types, cpu platform constraint is supported": {
			family: N1,
			constraints: Constraints{
				CpuPlatform: IntelIvyBridge,
				ExplicitMachineTypes: []string{
					customN1Cpu4Memory64Name,
					customN1Cpu20Memory72Name,
					customN1Cpu28Memory300Name,
				},
			},
			expectedCustomTypes: onboardMachineType(
				customN1Cpu4Memory64,
				customN1Cpu20Memory72,
				customN1Cpu28Memory300,
			),
		},
		"N1 machine family with multiple explicitly required custom machine types, cpu platform constraint is not supported": {
			family: N1,
			constraints: Constraints{
				CpuPlatform: IntelGraniteRapids,
				ExplicitMachineTypes: []string{
					customN1Cpu4Memory64Name,
					customN1Cpu20Memory72Name,
					customN1Cpu28Memory300Name,
				},
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			if len(tc.expectedCustomTypes) == 0 {
				assert.Empty(t, tc.family.explicitlySetCustomMachineTypes(tc.constraints))
			} else {
				assert.Equal(t, tc.expectedCustomTypes, tc.family.explicitlySetCustomMachineTypes(tc.constraints))
			}
		})
	}
}

func TestCustomAutoprovisionedMachineTypes(t *testing.T) {
	noIceLake := CpuPlatformRequirements{lowerBound: IntelSandyBridge, upperBound: IntelCascadeLake}
	allIntel := CpuPlatformRequirements{lowerBound: IntelSandyBridge, upperBound: IntelIceLake}
	gpuType1 := NewTestGpu("gpu-type-1", true, nil, nil)
	gpuType2 := NewTestGpu("gpu-type-2", true, nil, nil)

	t1 := NewMachineTypeInfo("n1-standard-1", 1, 3.75)
	t2 := NewMachineTypeInfo("n1-custom-4-2048", 4, 2)
	t3 := NewMachineTypeInfo("custom-8-4096", 8, 4)
	t4 := NewMachineTypeInfo("n1-standard-8", 8, 30)
	t1Override := NewMachineTypeInfo("n1-standard-1", 1, 3.75).
		withCpuPlatformRequirements(noIceLake)
	t2Override := NewMachineTypeInfo("n1-custom-4-2048", 4, 2).
		withCpuPlatformRequirements(noIceLake)
	t3Override := NewMachineTypeInfo("custom-8-4096", 8, 4).
		withGpuOverride(gpuType2, 0)
	t4Override := NewMachineTypeInfo("n1-standard-8", 8, 30).
		withGpuOverride(gpuType2, 0)

	onlyPredefined := MachineFamily{autoprovisionedMachineTypes: onboardMachineType(t1, t4), supportedCpuPlatforms: allIntel}
	onlyCustom := MachineFamily{autoprovisionedMachineTypes: onboardMachineType(t2, t3), supportedCpuPlatforms: allIntel}
	both := MachineFamily{autoprovisionedMachineTypes: onboardMachineType(t1, t2, t3, t4), supportedCpuPlatforms: allIntel}
	bothWithOverrides := MachineFamily{
		autoprovisionedMachineTypes: onboardMachineType(t1Override, t2Override, t3Override, t4Override),
		supportedCpuPlatforms:       allIntel,
		supportedGpuTypes:           onboardSupportedGpus(gpuType1),
	}

	for tn, tc := range map[string]struct {
		family        MachineFamily
		constraints   Constraints
		expectedTypes map[string]MachineType
	}{
		"no custom machines in family": {
			family:        onlyPredefined,
			constraints:   Constraints{CpuPlatform: IntelIceLake},
			expectedTypes: map[string]MachineType{},
		},
		"only custom machines in family": {
			family:        onlyCustom,
			constraints:   Constraints{CpuPlatform: IntelIceLake},
			expectedTypes: onboardMachineType(t2, t3),
		},
		"both custom and predefined machines in family": {
			family:        both,
			constraints:   Constraints{CpuPlatform: IntelIceLake},
			expectedTypes: onboardMachineType(t2, t3),
		},
		"both custom and predefined machines in family, incompatible platform": {
			family:        both,
			constraints:   Constraints{CpuPlatform: AmdRome},
			expectedTypes: map[string]MachineType{},
		},
		"both custom and predefined machines in family, incompatible GPU type": {
			family:        both,
			constraints:   Constraints{CpuPlatform: IntelIceLake, GpuType: "gpu-type-3"},
			expectedTypes: map[string]MachineType{},
		},
		"both custom and predefined machines in family, some types incompatible with platform": {
			family:        bothWithOverrides,
			constraints:   Constraints{CpuPlatform: IntelIceLake},
			expectedTypes: onboardMachineType(t3Override),
		},
		"both custom and predefined machines in family, some types incompatible with GPU type": {
			family:        bothWithOverrides,
			constraints:   Constraints{CpuPlatform: IntelCascadeLake, GpuType: "gpu-type-2"},
			expectedTypes: onboardMachineType(t3Override),
		},
		"both custom and predefined machines in family, some types incompatible with platform, some types incompatible with GPU type": {
			family:        bothWithOverrides,
			constraints:   Constraints{CpuPlatform: IntelIceLake, GpuType: "gpu-type-1"},
			expectedTypes: map[string]MachineType{},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			assert.Equal(t, tc.expectedTypes, tc.family.CustomAutoprovisionedMachineTypes(tc.constraints))
		})
	}
}

func TestEqual(t *testing.T) {
	for tn, tc := range map[string]struct {
		family      MachineFamily
		otherFamily MachineFamily
		expected    bool
	}{
		"equal": {
			family:      N2,
			otherFamily: N2,
			expected:    true,
		},
		"not equal": {
			family:      N2,
			otherFamily: A2,
			expected:    false,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.family.Equal(tc.otherFamily))
		})
	}
}

func TestIn(t *testing.T) {
	for tn, tc := range map[string]struct {
		family        MachineFamily
		otherFamilies []MachineFamily
		expected      bool
	}{
		"in": {
			family:        N2,
			otherFamilies: []MachineFamily{N1, N2, A2},
			expected:      true,
		},
		"not in": {
			family:        N2,
			otherFamilies: []MachineFamily{N1, N2D, A2},
			expected:      false,
		},
		"not in empty list": {
			family:        N2,
			otherFamilies: []MachineFamily{},
			expected:      false,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.family.In(tc.otherFamilies...))
		})
	}
}

func TestAllAutoprovisionedCustomTypes(t *testing.T) {
	// It's difficult to test this function because it relies on constant variables, so we can't inject a precise set of
	// custom types we expect. Instead, just verify that at least the types introduced at the time of writing this test are returned.
	knownCustomTypes := map[string]bool{"custom-48-184320": true, "custom-80-307200": true, "custom-48-319488": true, "custom-80-532480": true}
	customTypes := NewMachineConfigProvider(nil).AllAutoprovisionedCustomTypes()
	for machineType := range knownCustomTypes {
		assert.Contains(t, customTypes, machineType)
	}
}

func TestIsCompactPlacementSupported(t *testing.T) {
	for tn, tc := range map[string]struct {
		family   MachineFamily
		expected bool
	}{
		"supported": {
			family:   N2,
			expected: true,
		},
		"unsupported": {
			family:   E2,
			expected: false,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			actual := tc.family.IsCompactPlacementSupported()
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestMaxCompactPlacementNodesExists(t *testing.T) {
	machineFamilies := NewMachineConfigProvider(nil).AllMachineFamilies()
	for _, mf := range machineFamilies {
		if mf.IsCompactPlacementSupported() {
			_, err := mf.MaxCompactPlacementNodes()
			assert.NoError(t, err)
		}
	}
}

func TestMaxCompactPlacementNodes(t *testing.T) {
	for tn, tc := range map[string]struct {
		family      MachineFamily
		expected    int64
		expectedErr bool
	}{
		"supported - A2": {
			family:   A2,
			expected: 150,
		},
		"supported - A3": {
			family:   A3,
			expected: 96,
		},
		"supported - A4": {
			family:   A4,
			expected: 1500,
		},
		"supported - A4X": {
			family:      A4X,
			expectedErr: true,
		},
		"supported - G4": {
			family:   G4,
			expected: 1500,
		},
		"tpu machine family": {
			family:      CT4L,
			expectedErr: true,
		},
		"unsupported": {
			family:      E2,
			expectedErr: true,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			actual, err := tc.family.MaxCompactPlacementNodes()
			if tc.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, actual)
			}
		})
	}
}

func TestUpdateMachineFamiliesOnFlag(t *testing.T) {
	for tn, tc := range map[string]struct {
		passedMachineLimits map[string]int64
		wantMachineLimits   map[string]int64
		wantErr             bool
	}{
		"no limits - use defaults": {
			passedMachineLimits: map[string]int64{},
			wantMachineLimits:   map[string]int64{"a2": 150},
			wantErr:             false,
		},
		"wrong family name": {
			passedMachineLimits: map[string]int64{"nonexist": 123},
			wantMachineLimits:   map[string]int64{},
			wantErr:             true,
		},
		"proper value - should change default": {
			passedMachineLimits: map[string]int64{"a2": 997},
			wantMachineLimits:   map[string]int64{"a2": 997},
			wantErr:             false,
		},
	} {
		tc := tc
		t.Run(tn, func(t *testing.T) {
			prevState := NewMachineConfigProvider(nil).AllMachineFamilies()
			response := ApplyMaxCompactPlacementNodesUpdates(tc.passedMachineLimits)
			assert.Equal(t, tc.wantErr, response != nil)
			if !tc.wantErr {
				actual := NewMachineConfigProvider(nil).AllMachineFamilies()
				a2Family := "a2"
				var actualA2Index int = -1
				for i, mf := range actual {
					if mf.name == a2Family {
						actualA2Index = i
						_, isA2Passed := tc.passedMachineLimits[a2Family]
						assert.Equal(t, mf.supportCompactPlacement, isA2Passed)
						assert.Equal(t, mf.maxCompactPlacementNodes, tc.wantMachineLimits[a2Family])
					}
				}
				assert.NotEqual(t, actualA2Index, -1)
			}
			// Returning to the original state
			for _, mf := range prevState {
				RegisterMachineFamily(mf)
			}
		})
	}
}

func TestMachineType_ThreadsPerCore(t *testing.T) {
	for tn, tc := range map[string]struct {
		family      MachineFamily
		machineType string
		expected    int64
	}{
		"using default value": {
			family:      N2,
			machineType: "n2-standard-2",
			expected:    DefaultThreadPerCore,
		},
		"using non-default value": {
			family:      T2D,
			machineType: "t2d-standard-4",
			expected:    1,
		},
		"using override value": {
			family:      CT6E,
			machineType: "ct6e-standard-8t",
			expected:    1,
		},
		"using non-override value": {
			family:      CT6E,
			machineType: "ct6e-standard-4t",
			expected:    2,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			mt, _ := NewMachineConfigProvider(nil).ToMachineType(tc.machineType)
			actual := mt.GetThreadsPerCore()
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestIsConfidentialNodes(t *testing.T) {
	for tn, tc := range map[string]struct {
		family   MachineFamily
		expected bool
	}{
		"supported C2D": {
			family:   C2D,
			expected: true,
		},
		"unsupported": {
			family:   E2,
			expected: false,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			actual := tc.family.IsConfidentialNodesSupported()
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestFixedGPUTypeAndCountForMachineType(t *testing.T) {
	testCases := []struct {
		name         string
		machineType  string
		wantGPUType  string
		wantGPUCount PhysicalGpuCount
		wantFound    bool
	}{
		{
			name:        "non GPU machine type",
			machineType: machineTypeN1,
			wantFound:   false,
		},
		{
			name:         "g2-standard-12, 1 Nvidia L4 GPU",
			machineType:  "g2-standard-12",
			wantGPUType:  NvidiaL4.Name(),
			wantGPUCount: 1,
			wantFound:    true,
		},
		{
			name:         "a2-highgpu-2g, 2 Nvidia A100 GPUs",
			machineType:  "a2-highgpu-2g",
			wantGPUType:  NvidiaTeslaA100.Name(),
			wantGPUCount: 2,
			wantFound:    true,
		},
		{
			name:         "a2-ultragpu-8g, 8 A100 80GB GPUs",
			machineType:  "a2-ultragpu-8g",
			wantGPUType:  NvidiaA100_80gb.Name(),
			wantGPUCount: 8,
			wantFound:    true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := NewMachineConfigProvider(nil)
			gpuType, count, found := provider.FixedGPUTypeAndCountForMachineType(tc.machineType)
			assert.Equal(t, tc.wantGPUType, gpuType)
			assert.Equal(t, tc.wantGPUCount, count)
			assert.Equal(t, tc.wantFound, found)
		})
	}
}

func TestIsGpuTypeAndCountSupportedByMachineType(t *testing.T) {
	for tn, tc := range map[string]struct {
		machineName string
		gpuType     string
		gpuCount    PhysicalGpuCount
		expectFound bool
		expectErr   bool
	}{
		"empty machine type": {
			expectErr: true,
		},
		"invalid machine family": {
			machineName: "invalid-machine",
			expectErr:   true,
		},
		"empty gpu type": {
			machineName: "a2-highgpu-2g",
			expectFound: false,
		},
		"empty gpu count": {
			machineName: "a2-highgpu-2g",
			gpuType:     NvidiaTeslaA100.Name(),
			expectFound: false,
		},
		"invalid gpu type": {
			machineName: "a2-highgpu-2g",
			gpuType:     "invalid-gpu-type",
			expectFound: false,
		},
		"invalid gpu count 01": {
			machineName: "a2-highgpu-2g",
			gpuType:     NvidiaTeslaA100.Name(),
			gpuCount:    3,
			expectFound: false,
		},
		"gpu not supported with machine type": {
			machineName: "n2-standard-4",
			gpuType:     NvidiaTeslaA100.Name(),
			gpuCount:    2,
			expectFound: false,
		},
		"gpu not supported with custom machine type": {
			machineName: "n2-custom-4-4096",
			gpuType:     NvidiaTeslaA100.Name(),
			gpuCount:    2,
			expectFound: false,
		},
		"invalid gpu count 02": {
			machineName: "n1-highcpu-64",
			gpuType:     NvidiaTeslaP100.Name(),
			gpuCount:    2,
			expectFound: false,
		},
		"invalid gpu count for custom machine type": {
			machineName: "n1-custom-56-32768",
			gpuType:     NvidiaTeslaV100.Name(),
			gpuCount:    4,
			expectFound: false,
		},
		"gpu config supported 01": {
			machineName: "a2-highgpu-2g",
			gpuType:     NvidiaTeslaA100.Name(),
			gpuCount:    2,
			expectFound: true,
		},
		"gpu config supported 02": {
			machineName: "a2-ultragpu-8g",
			gpuType:     NvidiaA100_80gb.Name(),
			gpuCount:    8,
			expectFound: true,
		},
		"gpu config supported 03": {
			machineName: "n1-standard-4",
			gpuType:     NvidiaTeslaK80.Name(),
			gpuCount:    8,
			expectFound: true,
		},
		"gpu config supported 04": {
			machineName: "n1-standard-4",
			gpuType:     NvidiaTeslaV100.Name(),
			gpuCount:    4,
			expectFound: true,
		},
		"gpu config supported 05": {
			machineName: "n1-highcpu-64",
			gpuType:     NvidiaTeslaP100.Name(),
			gpuCount:    4,
			expectFound: true,
		},
		"gpu config supported 06": {
			machineName: "a3-megagpu-8g",
			gpuType:     NvidiaH100Mega_80gb.Name(),
			gpuCount:    8,
			expectFound: true,
		},
		"gpu config supported 07": {
			machineName: "a4-highgpu-8g",
			gpuType:     NvidiaB200.Name(),
			gpuCount:    8,
			expectFound: true,
		},
		"gpu config supported 08": {
			machineName: "a4x-highgpu-4g",
			gpuType:     NvidiaGB200.Name(),
			gpuCount:    4,
			expectFound: true,
		},
		"gpu config supported 09": {
			machineName: "n1-custom-4-2048",
			gpuType:     NvidiaTeslaV100.Name(),
			gpuCount:    4,
			expectFound: true,
		},
		"gpu config supported 10": {
			machineName: "g4-standard-48",
			gpuType:     NvidiaRTXPro6000.Name(),
			gpuCount:    1,
			expectFound: true,
		},
		"gpu_config_supported_11": {
			machineName: "g4-standard-6",
			gpuType:     NvidiaRTXPro6000.Name(),
			gpuCount:    1,
			expectFound: true,
		},
		"gpu_config_supported_12": {
			machineName: "a4x-maxgpu-4g-metal",
			gpuType:     NvidiaGB300.Name(),
			gpuCount:    4,
			expectFound: true,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			mcp := NewMachineConfigProvider(nil)
			found, err := mcp.IsGpuTypeAndCountSupportedByMachineType(tc.machineName, tc.gpuType, tc.gpuCount)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectFound, found)
			}
		})
	}
}

func TestAutomaticEphemeralLocalSsdCountByMachineType(t *testing.T) {
	for tn, tc := range map[string]struct {
		machineName   string
		expectedCount int64
		expectFound   bool
		expectErr     bool
	}{
		"expect error due to invalid machine family": {
			machineName: "invalid-machine",
			expectErr:   true,
		},
		"expect local ssd count not found": {
			machineName: "a2-highgpu-2g",
			expectFound: false,
		},
		"expect local ssd count not found for custom machine type": {
			machineName: "custom-4-32768",
			expectFound: false,
		},
		"expect local ssd count not found for custom machine type 2": {
			machineName: "n2-custom-8-65536",
			expectFound: false,
		},
		"expect 2 local ssds in a2-ultragpu-2g": {
			machineName:   "a2-ultragpu-2g",
			expectFound:   true,
			expectedCount: 2,
		},
		"expect 8 local ssds in a2-ultragpu-8g": {
			machineName:   "a2-ultragpu-8g",
			expectFound:   true,
			expectedCount: 8,
		},
		"expect 32 local ssds in a3-ultragpu-8g": {
			machineName:   "a3-ultragpu-8g",
			expectFound:   true,
			expectedCount: 32,
		},
		"expect 32 local ssds in a4-highgpu-8g": {
			machineName:   "a4-highgpu-8g",
			expectFound:   true,
			expectedCount: 32,
		},
		"expect 4 local ssds in a4x-highgpu-4g": {
			machineName:   "a4x-highgpu-4g",
			expectFound:   true,
			expectedCount: 4,
		},
		"expect_4_local_ssds_in_a4x-maxgpu-4g-metal": {
			machineName:   "a4x-maxgpu-4g-metal",
			expectFound:   true,
			expectedCount: 4,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			p := NewMachineConfigProvider(nil)
			count, found, err := p.AutomaticEphemeralLocalSsdCountByMachineType(tc.machineName)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectFound, found)
				if tc.expectFound {
					assert.Equal(t, tc.expectedCount, count)
				}
			}
		})
	}
}

func TestAllowedEphemeralLocalSsdCountByMachineType(t *testing.T) {
	for tn, tc := range map[string]struct {
		machineName        string
		expectAllowedCount []int
		expectFound        bool
		expectErr          bool
	}{
		"expect error due to invalid machine family": {
			machineName: "invalid-machine",
			expectErr:   true,
		},
		"expect local ssd count not found": {
			machineName: "m1-ultramem-40",
			expectFound: false,
		},
		"expect local ssd count not found for custom machine type 1": {
			machineName: "n2-custom-4-32768",
			expectFound: false,
		},
		"expect local ssd count not found for custom machine type 2": {
			machineName: "e2-custom-16-131072",
			expectFound: false,
		},
		"expect allowed 1 local ssd in g2-standard-8": {
			machineName:        "g2-standard-8",
			expectFound:        true,
			expectAllowedCount: []int{1},
		},
		"a2-highgpu-1g supports	1, 2, 4, or 8 local ssds": {
			machineName:        "a2-highgpu-1g",
			expectFound:        true,
			expectAllowedCount: []int{1, 2, 4, 8},
		},
		"a2-highgpu-2g supports	2, 4, or 8 local ssds": {
			machineName:        "a2-highgpu-2g",
			expectFound:        true,
			expectAllowedCount: []int{2, 4, 8},
		},
		"a2-highgpu-4g supports	4 or 8 local ssds": {
			machineName:        "a2-highgpu-4g",
			expectFound:        true,
			expectAllowedCount: []int{4, 8},
		},
		"a2-highgpu-8g supports 8 local ssds": {
			machineName:        "a2-highgpu-8g",
			expectFound:        true,
			expectAllowedCount: []int{8},
		},
		"a2-megagpu-16g supports 8 local ssds": {
			machineName:        "a2-megagpu-16g",
			expectFound:        true,
			expectAllowedCount: []int{8},
		},
		"a3-ultragpu-8g supports 32 local ssds": {
			machineName:        "a3-ultragpu-8g",
			expectFound:        true,
			expectAllowedCount: []int{32},
		},
		"a4-highgpu-8g supports 32 local ssds": {
			machineName:        "a4-highgpu-8g",
			expectFound:        true,
			expectAllowedCount: []int{32},
		},
		"a4x-highgpu-4g supports 4 local ssds": {
			machineName:        "a4x-highgpu-4g",
			expectFound:        true,
			expectAllowedCount: []int{4},
		},
		"a4x-maxgpu-4g-metal_supports_4_local_ssds": {
			machineName:        "a4x-maxgpu-4g-metal",
			expectFound:        true,
			expectAllowedCount: []int{4},
		},
		"g2-standard-4 supports 1 local ssds": {
			machineName:        "g2-standard-4",
			expectFound:        true,
			expectAllowedCount: []int{1},
		},
		"g2-standard-8 supports 1 local ssds": {
			machineName:        "g2-standard-8",
			expectFound:        true,
			expectAllowedCount: []int{1},
		},
		"g2-standard-12 supports 1 local ssds": {
			machineName:        "g2-standard-12",
			expectFound:        true,
			expectAllowedCount: []int{1},
		},
		"g2-standard-16 supports 1 local ssds": {
			machineName:        "g2-standard-16",
			expectFound:        true,
			expectAllowedCount: []int{1},
		},
		"g2-standard-24 supports 2 local ssds": {
			machineName:        "g2-standard-24",
			expectFound:        true,
			expectAllowedCount: []int{2},
		},
		"g2-standard-32 supports 1 local ssds": {
			machineName:        "g2-standard-32",
			expectFound:        true,
			expectAllowedCount: []int{1},
		},
		"g2-standard-48 supports 4 local ssds": {
			machineName:        "g2-standard-48",
			expectFound:        true,
			expectAllowedCount: []int{4},
		},
		"g2-standard-96 supports 8 local ssds": {
			machineName:        "g2-standard-96",
			expectFound:        true,
			expectAllowedCount: []int{8},
		},
		"m1-megamem-96 supports 1 to 8 local ssds": {
			machineName:        "m1-megamem-96",
			expectFound:        true,
			expectAllowedCount: []int{1, 2, 3, 4, 5, 6, 7, 8},
		},
		"m3-ultramem-32	 supports 4 or 8 local ssds": {
			machineName:        "m3-ultramem-32",
			expectFound:        true,
			expectAllowedCount: []int{4, 8},
		},
		"m3-megamem-64 supports 4 or 8 local ssds": {
			machineName:        "m3-megamem-64",
			expectFound:        true,
			expectAllowedCount: []int{4, 8},
		},
		"m3-ultramem-64	supports 4 or 8 local ssds": {
			machineName:        "m3-ultramem-64",
			expectFound:        true,
			expectAllowedCount: []int{4, 8},
		},
		"m3-megamem-128	supports 8 local ssds": {
			machineName:        "m3-megamem-128",
			expectFound:        true,
			expectAllowedCount: []int{8},
		},
		"m3-ultramem-128 supports 8 local ssds": {
			machineName:        "m3-ultramem-128",
			expectFound:        true,
			expectAllowedCount: []int{8},
		},
		"g4-standard-48	supports 4 local ssds": {
			machineName:        "g4-standard-48",
			expectFound:        true,
			expectAllowedCount: []int{4},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			provider := NewMachineConfigProvider(nil)
			count, found, err := provider.AllowedEphemeralLocalSsdCountByMachineType(tc.machineName)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectFound, found)
				if tc.expectFound {
					assert.Equal(t, tc.expectAllowedCount, count)
				}
			}
		})
	}
}

func TestAllowedEphemeralLocalSsdCountByMachineFamily(t *testing.T) {
	for tn, tc := range map[string]struct {
		machineFamily       string
		minCPU              int
		maxCPU              int
		expectAllowedCounts []int
		expectFound         bool
	}{
		"expect error due to invalid machine family": {
			machineFamily: "invalid-machine-family",
			expectFound:   false,
		},
		"n1 family: supports 1 to 8, 16, or 24 local SSDs": {
			machineFamily:       "n1",
			expectFound:         true,
			minCPU:              0,
			maxCPU:              1000, // every machine in N1 family
			expectAllowedCounts: []int{1, 2, 3, 4, 5, 6, 7, 8, 16, 24},
		},
		"n2 family: machine types with 2 to 10 vCPUs, supports 1, 2, 4, 8, 16, or 24 local SSDs": {
			machineFamily:       "n2",
			expectFound:         true,
			minCPU:              2,
			maxCPU:              10,
			expectAllowedCounts: []int{1, 2, 4, 8, 16, 24},
		},
		"n2 family: machine types with 12 to 20 vCPUs, supports 2, 4, 8, 16, or 24 local SSDs": {
			machineFamily:       "n2",
			expectFound:         true,
			minCPU:              12,
			maxCPU:              20,
			expectAllowedCounts: []int{2, 4, 8, 16, 24},
		},
		"n2 family: machine types with 22 to 40 vCPUs, supports 4, 8, 16, or 24 local SSDs": {
			machineFamily:       "n2",
			expectFound:         true,
			minCPU:              22,
			maxCPU:              40,
			expectAllowedCounts: []int{4, 8, 16, 24},
		},
		"n2 family: machine types with 42 to 80 vCPUs, supports 8, 16, or 24 local SSDs": {
			machineFamily:       "n2",
			expectFound:         true,
			minCPU:              42,
			maxCPU:              80,
			expectAllowedCounts: []int{8, 16, 24},
		},
		"n2 family: machine types with 82 to 128 vCPUs, supports 16 or 24 local SSDs": {
			machineFamily:       "n2",
			expectFound:         true,
			minCPU:              82,
			maxCPU:              128,
			expectAllowedCounts: []int{16, 24},
		},
		"n2d family: machine types with 2 to 16 vCPUs, supports 1, 2, 4, 8, 16, or 24 local SSDs": {
			machineFamily:       "n2d",
			expectFound:         true,
			minCPU:              2,
			maxCPU:              16,
			expectAllowedCounts: []int{1, 2, 4, 8, 16, 24},
		},
		"n2d family: machine types with 32 to 48 vCPUs, supports 2, 4, 8, 16, or 24 local SSDs": {
			machineFamily:       "n2d",
			expectFound:         true,
			minCPU:              32,
			maxCPU:              48,
			expectAllowedCounts: []int{2, 4, 8, 16, 24},
		},
		"n2d family: machine types with 64 to 80 vCPUs, supports 4, 8, 16, or 24 local SSDs": {
			machineFamily:       "n2d",
			expectFound:         true,
			minCPU:              64,
			maxCPU:              80,
			expectAllowedCounts: []int{4, 8, 16, 24},
		},
		"n2d family: machine types with 96 to 224 vCPUs, supports 8, 16, or 24 local SSDs": {
			machineFamily:       "n2d",
			expectFound:         true,
			minCPU:              96,
			maxCPU:              224,
			expectAllowedCounts: []int{8, 16, 24},
		},
		"c2 family: machine types with 4 or 8 vCPUs, supports 1, 2, 4, or 8 local SSDs": {
			machineFamily:       "c2",
			expectFound:         true,
			minCPU:              4,
			maxCPU:              8,
			expectAllowedCounts: []int{1, 2, 4, 8},
		},
		"c2 family: machine types with 16 vCPUs, supports 2, 4, or 8 local SSDs": {
			machineFamily:       "c2",
			expectFound:         true,
			minCPU:              16,
			maxCPU:              16,
			expectAllowedCounts: []int{2, 4, 8},
		},
		"c2 family: machine types with 30 vCPUs, supports 4 or 8 local SSDs": {
			machineFamily:       "c2",
			expectFound:         true,
			minCPU:              30,
			maxCPU:              30,
			expectAllowedCounts: []int{4, 8},
		},
		"c2 family: machine types with 60 vCPUs, supports 8 local SSDs": {
			machineFamily:       "c2",
			expectFound:         true,
			minCPU:              60,
			maxCPU:              60,
			expectAllowedCounts: []int{8},
		},
		"c2d family: machine types with 2 to 16 vCPUs, supports 1, 2, 4, or 8 local SSDs": {
			machineFamily:       "c2d",
			expectFound:         true,
			minCPU:              2,
			maxCPU:              16,
			expectAllowedCounts: []int{1, 2, 4, 8},
		},
		"c2d family: machine types with 32 vCPUs, supports 2, 4, or 8 local SSDs": {
			machineFamily:       "c2d",
			expectFound:         true,
			minCPU:              32,
			maxCPU:              32,
			expectAllowedCounts: []int{2, 4, 8},
		},
		"c2d family: machine types with 56 vCPUs, supports 4 or 8 local SSDs": {
			machineFamily:       "c2",
			expectFound:         true,
			minCPU:              56,
			maxCPU:              56,
			expectAllowedCounts: []int{4, 8},
		},
		"c2d family: machine types with 112 vCPUs, supports 8 local SSDs": {
			machineFamily:       "c2",
			expectFound:         true,
			minCPU:              112,
			maxCPU:              112,
			expectAllowedCounts: []int{8},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			provider := NewMachineConfigProvider(nil)
			family, err := provider.ToMachineFamily(tc.machineFamily)
			assert.Equal(t, tc.expectFound, err == nil)
			allowedEphemeralLocalSsdCountByMachineFamily(t, provider, family, tc.minCPU, tc.maxCPU, tc.expectFound, tc.expectAllowedCounts)
		})
	}
}

func allowedEphemeralLocalSsdCountByMachineFamily(t *testing.T, provider *MachineConfigProvider, machineFamily MachineFamily, minCPU, maxCPU int, expectFound bool, expectAllowedCounts []int) {
	for _, machine := range machineFamily.AllMachineTypes(NoConstraints) {
		if machine.Name == "f1-micro" || machine.Name == "g1-small" {
			continue
		}
		if machine.CPU >= int64(minCPU) && machine.CPU <= int64(maxCPU) {
			if gce.IsCustomMachine(machine.Name) {
				continue
			}
			counts, found, _ := provider.AllowedEphemeralLocalSsdCountByMachineType(machine.Name)
			assert.Equal(t, expectFound, found)
			assert.Equal(t, expectAllowedCounts, counts, "machine: %s fails check", machine.Name)
		}
	}
}

func TestToMachineType(t *testing.T) {
	customMachineType, _ := gce.NewCustomMachineType("n2-custom-6-3072")
	for tn, tc := range map[string]struct {
		machineTypeName         string
		expectedMachineTypeInfo MachineType
		expectedError           error
		expectedFamily          bool
	}{
		"Known predefined type with overrides": {
			machineTypeName: "a2-highgpu-2g",
			expectedMachineTypeInfo: NewMachineTypeInfo("a2-highgpu-2g", 24, 170).
				withAllowedEphemeralLocalSsdCounts(2, 4, 8).
				withGpuOverride(NvidiaTeslaA100, 2).
				withInstancePriceOverride(7.34677),
			expectedError:  nil,
			expectedFamily: true,
		},
		"Known custom type with overrides": {
			machineTypeName: "custom-80-532480",
			expectedMachineTypeInfo: NewMachineTypeInfo("custom-80-532480", 80, 520).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelSkylake, IntelSkylake)),
			expectedError:  nil,
			expectedFamily: true,
		},
		"Unknown valid custom type": {
			machineTypeName:         "n2-custom-6-3072",
			expectedMachineTypeInfo: MachineType{MachineType: customMachineType},
			expectedError:           nil,
			expectedFamily:          true,
		},
		"Unknown predefined type": {
			machineTypeName:         "n2-standard-9999",
			expectedMachineTypeInfo: MachineType{},
			expectedError:           fmt.Errorf("unsupported machine type \"n2-standard-9999\""),
		},
	} {
		t.Run(tn, func(t *testing.T) {
			receivedMachineTypeInfo, err := NewMachineConfigProvider(nil).ToMachineType(tc.machineTypeName)
			assert.Equal(t, tc.expectedFamily, receivedMachineTypeInfo.family != nil)
			// Clear pointer to MachineFamily for a clean diff.
			receivedMachineTypeInfo.family = nil
			assert.Equal(t, tc.expectedMachineTypeInfo, receivedMachineTypeInfo)
			assert.Equal(t, tc.expectedError, err)
		})
	}
}

func TestAllMachineFamiliesDiskTypeConfig(t *testing.T) {
	machineFamilies := NewMachineConfigProvider(nil).AllMachineFamilies()
	for _, family := range machineFamilies {
		t.Run("Test DiskType config for family "+family.Name(), func(t *testing.T) {
			defaultDiskType := family.DefaultAutoprovisionedBootDiskType("")
			assert.NotEmpty(t, defaultDiskType)

			// if there're no autoprovisionedMachineTypes, the family is not considered for NAP
			if len(family.autoprovisionedMachineTypes) > 0 {
				assert.True(t, family.IsDiskTypeSupported(defaultDiskType))
			}
		})
	}
}

func TestOverridenMachineFamiliesDefaultDiskTypes(t *testing.T) {
	for tn, tc := range map[string]struct {
		family          MachineFamily
		machineType     string
		defaultDiskType string
	}{
		"machine type: a3-ultragpu-8g, default: hyperdisk-balanced": {
			family:          A3,
			machineType:     "a3-ultragpu-8g",
			defaultDiskType: "hyperdisk-balanced",
		},
		"machine type: a3-highgpu-8g, default: hyperdisk-balanced": {
			family:          A3,
			machineType:     "a3-highgpu-8g",
			defaultDiskType: "pd-balanced",
		},
	} {
		t.Run(tn, func(t *testing.T) {
			family := tc.family
			got := family.DefaultAutoprovisionedBootDiskType(tc.machineType)
			assert.Equal(t, tc.defaultDiskType, got, "MachineFamily: %v", family)
		})
	}
}

func TestGetMaxResizableVmSizeByMachineType(t *testing.T) {
	for _, tc := range []struct {
		name        string
		machineType string
		expected    resizable_vm_size.VmSize
		expectedErr error
	}{
		{
			name:        "found_type",
			machineType: "ek-standard-32",
			expected:    resizable_vm_size.VmSize{MilliCpus: 32000, KBytes: 128 * 1024 * 1024},
			expectedErr: nil,
		},
		{
			name:        "unknown_type",
			machineType: "ek-standard-xx",
			expected:    resizable_vm_size.VmSize{},
			expectedErr: fmt.Errorf("unknown machine type %q", "ek-standard-xx"),
		},
		{
			name:        "not_resizable_family",
			machineType: "e2-standard-2",
			expected:    resizable_vm_size.VmSize{},
			expectedErr: fmt.Errorf("machine family %q is not resizable", E2.Name()),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := NewMachineConfigProvider(nil)
			got, err := provider.GetMaxResizableVmSizeByMachineType(tc.machineType)
			if tc.expectedErr != nil {
				assert.EqualError(t, err, tc.expectedErr.Error())
			}
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestGetMinResizableVmSizeByMachineType(t *testing.T) {
	for _, tc := range []struct {
		name        string
		machineType string
		expected    resizable_vm_size.VmSize
		expectedErr error
	}{
		{
			name:        "found_type",
			machineType: "ek-standard-8",
			expected:    resizable_vm_size.VmSize{MilliCpus: 250, KBytes: 2 * 1024 * 1024},
			expectedErr: nil,
		},
		{
			name:        "unknown_ek_type",
			machineType: "ek-standard-xx",
			expected:    resizable_vm_size.VmSize{},
			expectedErr: fmt.Errorf("unknown machine type %q", "ek-standard-xx"),
		},
		{
			name:        "not_resizable_family",
			machineType: "e2-standard-2",
			expected:    resizable_vm_size.VmSize{},
			expectedErr: fmt.Errorf("machine family %q is not resizable", E2.Name()),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := NewMachineConfigProvider(nil)
			got, err := provider.GetMinResizableVmSizeByMachineType(tc.machineType)
			if tc.expectedErr != nil {
				assert.EqualError(t, err, tc.expectedErr.Error())
			}
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestIsHugepageSize1gSupported(t *testing.T) {
	for tn, tc := range map[string]struct {
		families []MachineFamily
		expected bool
	}{
		"supported": {
			families: []MachineFamily{A3, A4, A4X, C2D, C3, C3D, C4, CT5L, CT5LP, CT6E, G4, H3, M2, M3, Z3, Z4D, TPU7X}, // C3A, CT5E supports 1G hugepages but not supported by CA;
			expected: true,
		},
		"unsupported": {
			families: []MachineFamily{E4, E2, N2, C2},
			expected: false,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			for _, family := range tc.families {
				actual := family.IsHugepageSize1gSupported()
				assert.Equal(t, tc.expected, actual, "test fails for machine family %s", family.name)
			}
		})
	}
}

func TestSupportsAcceleratorSlice(t *testing.T) {
	testCases := []struct {
		name                         string
		machineFamily                MachineFamily
		wantSupportsAcceleratorSlice bool
	}{
		{
			name: "supportsAcceleratorSlice is true",
			machineFamily: MachineFamily{
				name:                     "test-family-true",
				supportsAcceleratorSlice: true,
			},
			wantSupportsAcceleratorSlice: true,
		},
		{
			name: "supportsAcceleratorSlice is false",
			machineFamily: MachineFamily{
				name:                     "test-family-false",
				supportsAcceleratorSlice: false,
			},
			wantSupportsAcceleratorSlice: false,
		},
		{
			name: "supportsAcceleratorSlice is default (false)",
			machineFamily: MachineFamily{
				name: "test-family-default",
				// supportsAcceleratorSlice is omitted, should default to false
			},
			wantSupportsAcceleratorSlice: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotSupportsAcceleratorSlice := tc.machineFamily.IsAcceleratorSliceSupported()
			assert.Equal(t, tc.wantSupportsAcceleratorSlice, gotSupportsAcceleratorSlice)
		})
	}
}

func TestSupportsGpuAcceleratorSlice(t *testing.T) {
	testCases := []struct {
		name                            string
		supportsAcceleratorSlice        bool
		supportsGpu                     bool
		wantSupportsGpuAcceleratorSlice bool
	}{
		{
			name:                            "supportsAcceleratorSlice_is_false_returns_false",
			supportsAcceleratorSlice:        false,
			supportsGpu:                     true,
			wantSupportsGpuAcceleratorSlice: false,
		},
		{
			name:                            "supportsAcceleratorSlice_is_true_supports_GPU",
			supportsAcceleratorSlice:        true,
			supportsGpu:                     true,
			wantSupportsGpuAcceleratorSlice: true,
		},
		{
			name:                            "supportsAcceleratorSlice_is_true_not_supports_GPU",
			supportsAcceleratorSlice:        true,
			wantSupportsGpuAcceleratorSlice: false,
		},
		{
			name:                            "supportsAcceleratorSlice_is_false_supports_GPU",
			supportsAcceleratorSlice:        false,
			wantSupportsGpuAcceleratorSlice: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			machineFamily := MachineFamily{
				name:                     "test-family-true",
				supportsAcceleratorSlice: tc.supportsAcceleratorSlice,
			}
			if tc.supportsGpu {
				machineFamily.supportedGpuTypes = onboardSupportedGpus(
					NvidiaH100_80gb,
				)
			}
			gotSupportsGpuAcceleratorSlice := machineFamily.IsGpuAcceleratorSliceSupported()
			assert.Equal(t, tc.wantSupportsGpuAcceleratorSlice, gotSupportsGpuAcceleratorSlice)
		})
	}
}

func TestSupportedConfidentialNodeTypesConsistency(t *testing.T) {
	for _, machineFamily := range NewMachineConfigProvider(nil).AllMachineFamilies() {
		for confidentialNodeType := range machineFamily.supportConfidentialNodeTypes {
			atLeast1TypeSupports := false
			for _, machineType := range machineFamily.AllMachineTypes(NoConstraints) {
				if machineType.confidentialNodeCfg == nil || machineType.confidentialNodeCfg.supportConfidentialNodeTypes[confidentialNodeType] {
					atLeast1TypeSupports = true
					break
				}
			}
			assert.Truef(t, atLeast1TypeSupports, "Machine family %s configured to support confidential node type %s, but none of its machine types actually support it", machineFamily.Name(), confidentialNodeType)
		}
	}
}

func TestDwsDisablementForMachineFamily(t *testing.T) {
	for tn, tc := range map[string]struct {
		families []MachineFamily
		expected bool
	}{
		"unsupported": {
			families: []MachineFamily{CT3, CT3P, CT4L, CT4P, CT5L},
			expected: true,
		},
		"supported": {
			families: []MachineFamily{A3, A4, A4X, C2, C2D, C3, C3D, C4, CT5P, CT5LP, CT6E, E4, E2, H3, M2, M3, N2, Z3, Z4D, TPU7X}, // All other machine families than in "unsupported" above
			expected: false,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			for _, family := range tc.families {
				actual := family.IsDwsDisabled()
				assert.Equal(t, tc.expected, actual, "test fails for machine family %s", family.name)
			}
		})
	}
}

// For each machine family, not all machine types should have withDwsDisabled set, but use dwsDisabled defined for whole family
func TestNoIndividualDwsDisablementForWholeFamily(t *testing.T) {
	t.Run("for no family, all machine types can have DWS disabled individually", func(t *testing.T) {
		for _, mf := range NewMachineConfigProvider(nil).AllMachineFamilies() {
			var familyHasMachinesSupportingDws = false
			for _, mt := range mf.AllMachineTypes(NoConstraints) {
				if !mt.notInDWS {
					familyHasMachinesSupportingDws = true
				}
			}
			assert.Equal(t, familyHasMachinesSupportingDws, true, "In family %s all machines have DWS disabled individually", mf.Name())
		}
	})
}

func TestIsBareMetal(t *testing.T) {
	for _, tc := range []struct {
		machineType string
		expected    bool
	}{
		{
			machineType: "c3-highcpu-192-metal",
			expected:    true,
		},
		{
			machineType: "c3-highcpu-192",
			expected:    false,
		},
	} {
		t.Run(tc.machineType, func(t *testing.T) {
			actual := IsBareMetal(tc.machineType)
			assert.Equal(t, tc.expected, actual, "test fails for machine type %s that should be %v", tc.machineType, tc.expected)
		})
	}
}

func TestListSupportedDisks(t *testing.T) {
	// Prepare a MachineFamily with a mix of all 3 ConfidentialMode values
	mf := MachineFamily{
		name: "test-family",
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeStandard: ConfidentialOnlyMode,
			DiskTypeBalanced: NonConfidentialOnlyMode,
			DiskTypeSSD:      UnspecifiedMode,
		},
	}

	tests := []struct {
		name               string
		isConfidentialNode bool
		expectedDiskTypes  []string
	}{
		{
			name:               "confidential node",
			isConfidentialNode: true,
			expectedDiskTypes:  []string{DiskTypeStandard, DiskTypeSSD},
		},
		{
			name:               "non-confidential node",
			isConfidentialNode: false,
			expectedDiskTypes:  []string{DiskTypeBalanced, DiskTypeSSD},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := mf.ListSupportedDisks(tc.isConfidentialNode)
			assert.ElementsMatch(t, tc.expectedDiskTypes, got)
		})
	}
}

func TestGetMachineGeneration(t *testing.T) {
	testCases := []struct {
		machineType string
		generation  int
	}{
		{machineType: "c4-standard-2", generation: 4},
		{machineType: "c4-standard-288-metal", generation: 4},
		{machineType: "c4a-standard-64", generation: 4},
		{machineType: "c4d-standard-384-metal", generation: 4},
		{machineType: "e2-standard-16", generation: 2},
		{machineType: "e2-small", generation: 2},
		{machineType: "t2a-standard-1", generation: 2},
		{machineType: "n1-standard-16", generation: 1},
		{machineType: "f1-micro", generation: 1},
		{machineType: "a4x-highgpu-4g", generation: 4},
		{machineType: "a4-highgpu-8g", generation: 4},
		{machineType: "g2-standard-16", generation: 2},
		{machineType: "a3-ultragpu-8g", generation: 3},
		{machineType: "a12-gigagpu-4099g", generation: 12},     //made-up machine
		{machineType: "a12x-gigagpu-8111g", generation: 12},    //made-up machine
		{machineType: "a12xx-gigagpu-16111g", generation: 12},  //made-up machine
		{machineType: "a317a-gigagpu-16111g", generation: 317}, //made-up machine
	}
	for _, tc := range testCases {
		t.Run(tc.machineType, func(t *testing.T) {
			tc := tc
			t.Parallel()

			received := GetMachineGeneration(tc.machineType)

			assert.Equal(t, tc.generation, received)
		})
	}
}

func TestIsConfidentialNodeTypeSupported(t *testing.T) {
	mcp := NewMachineConfigProvider(nil)
	c2dStandard2, err := mcp.ToMachineType("c2d-standard-2")
	assert.NoError(t, err)
	c3Standard4, err := mcp.ToMachineType("c3-standard-4")
	assert.NoError(t, err)
	c3Standard4Lssd, err := mcp.ToMachineType("c3-standard-4-lssd")
	assert.NoError(t, err)
	c3Highcpu4, err := mcp.ToMachineType("c3-highcpu-4")
	assert.NoError(t, err)
	e2Standard2, err := mcp.ToMachineType("e2-standard-2")
	assert.NoError(t, err)
	c4Standard4, err := mcp.ToMachineType("c4-standard-4")
	assert.NoError(t, err)

	legacyCvmFamily := MachineFamily{
		name:                     "legacy-cvm",
		supportConfidentialNodes: true,
	}
	legacyCvmType := MachineType{
		MachineType: gce.MachineType{Name: "legacy-cvm-standard-2"},
		family:      &legacyCvmFamily,
	}

	testCases := []struct {
		name                 string
		machineType          MachineType
		confidentialNodeType string
		expected             bool
	}{
		{name: "c2d-standard-2 supports sev",
			machineType:          c2dStandard2,
			confidentialNodeType: labels.SEVConfidentialNodeTypeValue,
			expected:             true,
		},
		{name: "c2d-standard-2 does not support tdx",
			machineType:          c2dStandard2,
			confidentialNodeType: labels.TDXConfidentialNodeTypeValue,
			expected:             false,
		},
		{name: "c3-standard-4 supports tdx",
			machineType:          c3Standard4,
			confidentialNodeType: labels.TDXConfidentialNodeTypeValue,
			expected:             true,
		},
		{name: "c3-standard-4 does not support sev",
			machineType:          c3Standard4,
			confidentialNodeType: labels.SEVConfidentialNodeTypeValue,
			expected:             false,
		},
		{name: "c3-standard-4-lssd supports tdx",
			machineType:          c3Standard4Lssd,
			confidentialNodeType: labels.TDXConfidentialNodeTypeValue,
			expected:             true,
		},
		{name: "c3-standard-4-lssd does not support sev",
			machineType:          c3Standard4Lssd,
			confidentialNodeType: labels.SEVConfidentialNodeTypeValue,
			expected:             false,
		},
		{name: "c3-highcpu-4 does not support tdx",
			machineType:          c3Highcpu4,
			confidentialNodeType: labels.TDXConfidentialNodeTypeValue,
			expected:             false,
		},
		{name: "c4-standard-4 support tdx",
			machineType:          c4Standard4,
			confidentialNodeType: labels.TDXConfidentialNodeTypeValue,
			expected:             true,
		},
		{name: "c4-standard-4 does not support sev",
			machineType:          c4Standard4,
			confidentialNodeType: labels.SEVConfidentialNodeTypeValue,
			expected:             false,
		},
		{name: "e2-standard-2 does not support any confidential type",
			machineType:          e2Standard2,
			confidentialNodeType: labels.SEVConfidentialNodeTypeValue,
			expected:             false,
		},
		{name: "legacy cvm family supports sev",
			machineType:          legacyCvmType,
			confidentialNodeType: labels.SEVConfidentialNodeTypeValue,
			expected:             true,
		},
		{name: "legacy cvm family does not support tdx",
			machineType:          legacyCvmType,
			confidentialNodeType: labels.TDXConfidentialNodeTypeValue,
			expected:             false,
		},
		{name: "legacy cvm family does not support sev-snp",
			machineType:          legacyCvmType,
			confidentialNodeType: labels.SEVSNPConfidentialNodeTypeValue,
			expected:             false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.machineType.IsConfidentialNodeTypeSupported(tc.confidentialNodeType))
		})
	}
}

// TestKnownMachineFamilies verifies that all GKE machine families registered in the
// provider are accounted for in the test's knownMachineFamilies list. If a new GKE
// machine family is introduced, this test will fail to ensure developers take appropriate
// action:
//   - If it is internal-only, they must update gke-common-webhooks to enforce constraints.
//   - If it is public, they can simply add it to the knownMachineFamilies list.
func TestKnownMachineFamilies(t *testing.T) {
	knownMachineFamilies := map[string]bool{
		"a2":    true,
		"a3":    true,
		"a4":    true,
		"a4x":   true,
		"c2":    true,
		"c2d":   true,
		"c3":    true,
		"c3d":   true,
		"c4":    true,
		"c4a":   true,
		"c4d":   true,
		"c4n":   true,
		"ct3":   true,
		"ct3p":  true,
		"ct4l":  true,
		"ct4p":  true,
		"ct5l":  true,
		"ct5lp": true,
		"ct5p":  true,
		"ct6e":  true,
		"e2":    true,
		"e4":    true,
		"e4a":   true,
		"ek":    true,
		"g2":    true,
		"g4":    true,
		"h3":    true,
		"h4d":   true,
		"m1":    true,
		"m2":    true,
		"m3":    true,
		"m4":    true,
		"n1":    true,
		"n2":    true,
		"n2d":   true,
		"n4":    true,
		"n4a":   true,
		"n4d":   true,
		"t2a":   true,
		"t2d":   true,
		"tpu7":  true,
		"tpu7x": true,
		"z3":    true,
		"z4d":   true,
	}

	mcp := NewMachineConfigProvider(nil)
	for _, family := range mcp.AllMachineFamilies() {
		name := family.Name()
		if !knownMachineFamilies[name] {
			t.Errorf("New GKE machine family %q detected!\n"+
				"- If this family IS intended to be internal-only, you MUST update go/gcw-internal-machine-family-constraint. GCW will enforce the constraint on selecting internal only machine families. Once updated, add %q to the 'knownMachineFamilies' list in this test to bypass.\n"+
				"- If this family is NOT internal-only, it is safe to simply add %q to the 'knownMachineFamilies' list in this test to bypass.", name, name, name)
		}
	}
}

func TestIsConfidentialNodesSupported(t *testing.T) {
	mcp := NewMachineConfigProvider(nil)

	// Families
	c2dFamily, err := mcp.ToMachineFamily("c2d")
	assert.NoError(t, err)
	assert.True(t, c2dFamily.IsConfidentialNodesSupported())

	c3Family, err := mcp.ToMachineFamily("c3")
	assert.NoError(t, err)
	assert.True(t, c3Family.IsConfidentialNodesSupported())

	e2Family, err := mcp.ToMachineFamily("e2")
	assert.NoError(t, err)
	assert.False(t, e2Family.IsConfidentialNodesSupported())

	// Machine types table-driven assertions
	testCases := []struct {
		name        string
		machineType string
		expected    bool
	}{
		{"c3-standard-4 supports CVM", "c3-standard-4", true},
		{"c3-highcpu-4 does not support CVM", "c3-highcpu-4", false},
		{"e2-standard-2 does not support CVM", "e2-standard-2", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mt, err := mcp.ToMachineType(tc.machineType)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, mt.IsConfidentialNodesSupported())
		})
	}
}

func TestConfidentialNodesConstraintsMatching(t *testing.T) {
	mcp := NewMachineConfigProvider(nil)
	c3Family, err := mcp.ToMachineFamily("c3")
	assert.NoError(t, err)

	// When ConfidentialNodesRequired is true, c3-highcpu-4 should be filtered out.
	constraints := Constraints{
		CpuPlatform:               AnyPlatform,
		ConfidentialNodesRequired: true,
		ConfidentialNodeType:      labels.TDXConfidentialNodeTypeValue,
	}

	c3Types := c3Family.AutoprovisionedMachineTypes(constraints)

	// In GKE Autoscaler registries, c3-highcpu-4 explicitly disables CVM support.
	// When pod constraints request CVM, c3-highcpu-4 must be filtered out.
	_, foundHighCpu := c3Types["c3-highcpu-4"]
	assert.False(t, foundHighCpu, "c3-highcpu-4 should be filtered out when ConfidentialNodesRequired is true")

	// Conversely, c3-standard-4 supports TDX CVM and must NOT be filtered out.
	_, foundStandard := c3Types["c3-standard-4"]
	assert.True(t, foundStandard, "c3-standard-4 should remain available when ConfidentialNodesRequired is true")

	// When ConfidentialNodesRequired is false, c3-highcpu-4 should NOT be filtered out.
	constraintsFalse := Constraints{
		CpuPlatform:               AnyPlatform,
		ConfidentialNodesRequired: false,
	}

	c3TypesFalse := c3Family.AutoprovisionedMachineTypes(constraintsFalse)
	_, foundFalse := c3TypesFalse["c3-highcpu-4"]
	assert.True(t, foundFalse, "c3-highcpu-4 should NOT be filtered out when ConfidentialNodesRequired is false")
}

func TestToCustomMachineType_FamilyBackfill(t *testing.T) {
	for _, tc := range []struct {
		name            string
		machineTypeName string
		wantFamilyName  string
		wantCvmSupport  bool
	}{
		{
			name:            "N2D Custom CVM Shape",
			machineTypeName: "n2d-custom-8-32768",
			wantFamilyName:  "n2d",
			wantCvmSupport:  true,
		},
		{
			name:            "C3 Custom CVM Shape",
			machineTypeName: "c3-custom-4-16384",
			wantFamilyName:  "c3",
			wantCvmSupport:  true,
		},
		{
			name:            "E2 Custom Non-CVM Shape",
			machineTypeName: "e2-custom-2-4096",
			wantFamilyName:  "e2",
			wantCvmSupport:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Generate the custom machine type dynamically.
			mt, err := ToCustomMachineType(tc.machineTypeName)
			assert.NoError(t, err)
			assert.NotNil(t, mt)

			// Verify the parent family pointer is backfilled correctly.
			assert.NotNil(t, mt.family, "mt.family should be backfilled and non-nil on custom shapes")
			assert.Equal(t, tc.wantFamilyName, mt.family.Name())

			// Verify IsConfidentialNodesSupported executes safely on custom shapes without nil panics.
			assert.Equal(t, tc.wantCvmSupport, mt.IsConfidentialNodesSupported())
		})
	}
}
