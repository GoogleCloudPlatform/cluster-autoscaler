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
)

func TestMachineTypesGeneric(t *testing.T) {
	iceLake := CpuPlatformRequirements{IntelIceLake, IntelIceLake}
	gpuType1 := NewTestGpu("gpu-type-1", true, nil, nil)
	gpuType2 := NewTestGpu("gpu-type-2", true, nil, nil)

	t1 := NewMachineTypeInfo("t1", 1, 1).
		withGpuOverride(gpuType2, 0)
	t2 := NewMachineTypeInfo("t2", 2, 2)
	t3 := NewMachineTypeInfo("t3", 3, 3).withCpuPlatformRequirements(iceLake)
	t4 := NewMachineTypeInfo("t4", 4, 4)
	t5 := NewMachineTypeInfo("t5", 5, 5).
		withCpuPlatformRequirements(iceLake).
		withGpuOverride(gpuType1, 0)

	testFamily := MachineFamily{
		name:                        "test",
		autoprovisionedMachineTypes: onboardMachineType(t1, t2, t3),
		supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
		supportedGpuTypes:           onboardSupportedGpus(gpuType1),
	}
	otherFamily := MachineFamily{
		name:                        "other",
		autoprovisionedMachineTypes: onboardMachineType(t4, t5),
		supportedCpuPlatforms:       CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
	}
	for tn, tc := range map[string]struct {
		families               []MachineFamily
		platform               CpuPlatform
		gpuType                string
		expectedMachines       []MachineType
		expectedLargestMachine MachineType
	}{
		"one family, all machines are returned if AnyPlatform platform and no GPU type are specified": {
			families:               []MachineFamily{testFamily},
			platform:               AnyPlatform,
			gpuType:                "",
			expectedMachines:       []MachineType{t1, t2, t3},
			expectedLargestMachine: t3,
		},
		"many families, all machines are returned if AnyPlatform platform and no GPU type are specified": {
			families:               []MachineFamily{testFamily, otherFamily},
			platform:               AnyPlatform,
			gpuType:                "",
			expectedMachines:       []MachineType{t1, t2, t3, t4, t5},
			expectedLargestMachine: t5,
		},
		"one family, nothing is returned if incompatible platform is specified (shouldn't happen, just a sanity check)": {
			families:               []MachineFamily{testFamily},
			platform:               AmdRome,
			expectedMachines:       []MachineType{},
			expectedLargestMachine: UnknownMachineType,
		},
		"one family, nothing is returned if incompatible gpu type is specified": {
			families:               []MachineFamily{testFamily},
			platform:               AnyPlatform,
			gpuType:                "gpu-type-3",
			expectedMachines:       []MachineType{},
			expectedLargestMachine: UnknownMachineType,
		},
		"many families, nothing is returned if incompatible platform is specified (shouldn't happen, just a sanity check)": {
			families:               []MachineFamily{testFamily, otherFamily},
			platform:               AmdRome,
			expectedMachines:       []MachineType{},
			expectedLargestMachine: UnknownMachineType,
		},
		"many families, nothing is returned if incompatible gpu type is specified": {
			families:               []MachineFamily{testFamily, otherFamily},
			platform:               AnyPlatform,
			gpuType:                "gpu-type-3",
			expectedMachines:       []MachineType{},
			expectedLargestMachine: UnknownMachineType,
		},
		"one family, machines are cropped correctly for the specified platform": {
			families:               []MachineFamily{testFamily},
			platform:               IntelBroadwell,
			expectedMachines:       []MachineType{t1, t2},
			expectedLargestMachine: t2,
		},
		"one family, machines are cropped correctly for the specified GPU type": {
			families:               []MachineFamily{testFamily},
			platform:               AnyPlatform,
			gpuType:                "gpu-type-1",
			expectedMachines:       []MachineType{t2, t3},
			expectedLargestMachine: t3,
		},
		"many families, machines are cropped correctly for the specified platform": {
			families:               []MachineFamily{testFamily, otherFamily},
			platform:               IntelBroadwell,
			expectedMachines:       []MachineType{t1, t2, t4},
			expectedLargestMachine: t4,
		},
		"many families, machines are cropped correctly for the specified GPU type": {
			families:               []MachineFamily{testFamily, otherFamily},
			platform:               AnyPlatform,
			gpuType:                "gpu-type-1",
			expectedMachines:       []MachineType{t2, t3, t5},
			expectedLargestMachine: t5,
		},
		"one family, machines are cropped correctly for the specified platform, even with overridden requirements": {
			families:               []MachineFamily{testFamily},
			platform:               IntelIceLake,
			expectedMachines:       []MachineType{t3},
			expectedLargestMachine: t3,
		},
		"one family, machines are cropped correctly for the specified GPU type, even with overridden requirements": {
			families:               []MachineFamily{testFamily},
			platform:               AnyPlatform,
			gpuType:                "gpu-type-2",
			expectedMachines:       []MachineType{t1},
			expectedLargestMachine: t1,
		},
		"many families, machines are cropped correctly for the specified platform, even with overridden requirements": {
			families:               []MachineFamily{testFamily, otherFamily},
			platform:               IntelIceLake,
			expectedMachines:       []MachineType{t3, t5},
			expectedLargestMachine: t5,
		},
		"many families, machines are cropped correctly for the specified GPU type, even with overridden requirements": {
			families:               []MachineFamily{testFamily, otherFamily},
			platform:               AnyPlatform,
			gpuType:                "gpu-type-2",
			expectedMachines:       []MachineType{t1},
			expectedLargestMachine: t1,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			spec := MachineSpec{Families: tc.families, MinCpuPlatform: tc.platform, GpuType: tc.gpuType}
			assert.ElementsMatch(t, tc.expectedMachines, spec.AutoprovisionedMachineTypes())
			assert.Equal(t, tc.expectedLargestMachine, spec.LargestAutoprovisionedMachineType())
		})
	}
}

func TestMachineTypes(t *testing.T) {
	mcp := NewMachineConfigProvider(nil)
	predefinedN1Cpu64Name := "n1-standard-64"
	predefinedN1Cpu64, _ := mcp.ToMachineType(predefinedN1Cpu64Name)
	customN1Cpu4Memory64Name := "custom-4-65536"
	customN1Cpu4Memory64, _ := mcp.ToMachineType(customN1Cpu4Memory64Name)
	customN1Cpu20Memory72Name := "n1-custom-20-73728"
	customN1Cpu20Memory72, _ := mcp.ToMachineType(customN1Cpu20Memory72Name)
	customN1Cpu28Memory300Name := "n1-custom-20-307200-ext"
	customN1Cpu28Memory300, _ := mcp.ToMachineType(customN1Cpu28Memory300Name)
	customN1Cpu48Memory180Name := "custom-48-184320" // known custom type
	customN1Cpu48Memory180, _ := mcp.ToMachineType(customN1Cpu48Memory180Name)
	customN1Cpu80Memory300Name := "custom-80-307200" // known custom type
	customN1Cpu80Memory300, _ := mcp.ToMachineType(customN1Cpu80Memory300Name)
	predefinedN2Cpu2Name := "n2-standard-2"
	predefinedN2Cpu2, _ := mcp.ToMachineType(predefinedN2Cpu2Name)
	customN2Cpu20Memory32Name := "n2-custom-20-32768"
	customN2Cpu20Memory32, _ := mcp.ToMachineType(customN2Cpu20Memory32Name)
	invalidTypeName := "custom-invalid" // invalid type

	for tn, tc := range map[string]struct {
		families               []MachineFamily
		platform               CpuPlatform
		gpuType                string
		explicitMachineTypes   []string
		expectedMachines       []MachineType
		expectedLargestMachine MachineType
	}{
		"N1 machine family with explicitly required predefined type n1-standard-64": {
			families: []MachineFamily{N1},
			platform: AnyPlatform,
			explicitMachineTypes: []string{
				predefinedN1Cpu64Name,
			},
			expectedMachines:       []MachineType{predefinedN1Cpu64},
			expectedLargestMachine: predefinedN1Cpu64,
		},
		"N1 machine family with explicitly required custom unknown type custom-4-65536": {
			families: []MachineFamily{N1},
			platform: AnyPlatform,
			explicitMachineTypes: []string{
				customN1Cpu4Memory64Name,
			},
			expectedMachines:       []MachineType{customN1Cpu4Memory64},
			expectedLargestMachine: customN1Cpu4Memory64,
		},
		"N1 machine family with explicitly required custom known type custom-48-184320": {
			families: []MachineFamily{N1},
			platform: AnyPlatform,
			explicitMachineTypes: []string{
				customN1Cpu48Memory180Name,
			},
			expectedMachines:       []MachineType{customN1Cpu48Memory180},
			expectedLargestMachine: customN1Cpu48Memory180,
		},
		"N1 machine family with explicitly required custom known type custom-80-307200": {
			families: []MachineFamily{N1},
			platform: AnyPlatform,
			explicitMachineTypes: []string{
				customN1Cpu80Memory300Name,
			},
			expectedMachines:       []MachineType{customN1Cpu80Memory300},
			expectedLargestMachine: customN1Cpu80Memory300,
		},
		"N1 machine family with explicitly required custom known type n2-custom-20-32768 belonging to N2 family": {
			families: []MachineFamily{N1},
			platform: AnyPlatform,
			explicitMachineTypes: []string{
				customN2Cpu20Memory32Name,
			},
			expectedMachines:       []MachineType{},
			expectedLargestMachine: UnknownMachineType,
		},
		"N1 machine family with explicitly required custom invalid type": {
			families: []MachineFamily{N1},
			platform: AnyPlatform,
			explicitMachineTypes: []string{
				invalidTypeName,
			},
			expectedMachines:       []MachineType{},
			expectedLargestMachine: UnknownMachineType,
		},
		"N1 machine family with explicitly required custom unknown type n1-custom-20-73728": {
			families: []MachineFamily{N1},
			platform: AnyPlatform,
			explicitMachineTypes: []string{
				customN1Cpu20Memory72Name,
			},
			expectedMachines:       []MachineType{customN1Cpu20Memory72},
			expectedLargestMachine: customN1Cpu20Memory72,
		},
		"N1 machine family with explicitly required custom unknown type n1-custom-20-307200-ext with extended memory": {
			families: []MachineFamily{N1},
			platform: AnyPlatform,
			explicitMachineTypes: []string{
				customN1Cpu28Memory300Name,
			},
			expectedMachines:       []MachineType{customN1Cpu28Memory300},
			expectedLargestMachine: customN1Cpu28Memory300,
		},
		"N1 machine family with multiple explicitly required custom machine types": {
			families: []MachineFamily{N1},
			platform: AnyPlatform,
			explicitMachineTypes: []string{
				predefinedN1Cpu64Name,
				customN1Cpu4Memory64Name,
				customN1Cpu48Memory180Name, // known type
				customN1Cpu80Memory300Name, // known type
				customN1Cpu20Memory72Name,
				customN1Cpu28Memory300Name,
				customN2Cpu20Memory32Name, // from N2 family
				invalidTypeName,           // invalid type,
			},
			expectedMachines: []MachineType{
				predefinedN1Cpu64,
				customN1Cpu4Memory64,
				customN1Cpu48Memory180,
				customN1Cpu80Memory300,
				customN1Cpu20Memory72,
				customN1Cpu28Memory300,
			},
			expectedLargestMachine: customN1Cpu80Memory300,
		},
		"N1 and N2 machine families with multiple explicitly required machine types": {
			families: []MachineFamily{N1, N2},
			platform: AnyPlatform,
			explicitMachineTypes: []string{
				predefinedN1Cpu64Name,
				customN1Cpu4Memory64Name,
				customN1Cpu48Memory180Name, // known type
				customN1Cpu80Memory300Name, // known type
				customN1Cpu20Memory72Name,
				customN1Cpu28Memory300Name,

				predefinedN2Cpu2Name,
				customN2Cpu20Memory32Name,

				invalidTypeName, // invalid type,
			},
			expectedMachines: []MachineType{
				predefinedN1Cpu64,
				customN1Cpu4Memory64,
				customN1Cpu48Memory180,
				customN1Cpu80Memory300,
				customN1Cpu20Memory72,
				customN1Cpu28Memory300,
				predefinedN2Cpu2,
				customN2Cpu20Memory32,
			},
			expectedLargestMachine: customN1Cpu80Memory300,
		},
		"N1 and N2 machine families with multiple explicitly required machine types, gpu constraint is supported only by N1 types": {
			families: []MachineFamily{N1, N2},
			platform: AnyPlatform,
			gpuType:  NvidiaTeslaP100.Name(),
			explicitMachineTypes: []string{
				predefinedN1Cpu64Name,
				customN1Cpu4Memory64Name,
				customN1Cpu48Memory180Name, // known type
				customN1Cpu20Memory72Name,
				customN1Cpu28Memory300Name,

				predefinedN2Cpu2Name,
				customN2Cpu20Memory32Name,

				invalidTypeName, // invalid type,
			},
			expectedMachines: []MachineType{
				predefinedN1Cpu64,
				customN1Cpu4Memory64,
				customN1Cpu48Memory180,
				customN1Cpu20Memory72,
				customN1Cpu28Memory300,
			},
			expectedLargestMachine: predefinedN1Cpu64,
		},
		"N1 and N2 machine families with multiple explicitly required machine types, gpu constraint is not supported by any of types": {
			families: []MachineFamily{N1, N2},
			platform: AnyPlatform,
			gpuType:  NvidiaL4.Name(),
			explicitMachineTypes: []string{
				predefinedN1Cpu64Name,
				customN1Cpu4Memory64Name,
				customN1Cpu48Memory180Name, // known type
				customN1Cpu20Memory72Name,
				customN1Cpu28Memory300Name,

				predefinedN2Cpu2Name,
				customN2Cpu20Memory32Name,

				invalidTypeName, // invalid type,
			},
			expectedMachines:       []MachineType{},
			expectedLargestMachine: UnknownMachineType,
		},
		"N1 and N2 machine families with multiple explicitly required custom machine types, cpu platform constraint is supported only by N1 types": {
			families: []MachineFamily{N1, N2},
			platform: IntelIvyBridge,
			explicitMachineTypes: []string{
				predefinedN1Cpu64Name,
				customN1Cpu4Memory64Name,
				customN1Cpu48Memory180Name, // known type
				customN1Cpu20Memory72Name,
				customN1Cpu28Memory300Name,

				predefinedN2Cpu2Name,
				customN2Cpu20Memory32Name,

				invalidTypeName, // invalid type,
			},
			expectedMachines: []MachineType{
				predefinedN1Cpu64,
				customN1Cpu4Memory64,
				customN1Cpu48Memory180,
				customN1Cpu20Memory72,
				customN1Cpu28Memory300,
			},
			expectedLargestMachine: predefinedN1Cpu64,
		},
		"N1 and N2 machine families with multiple explicitly required custom machine types, cpu platform constraint is supported only by N2 types": {
			families: []MachineFamily{N1, N2},
			platform: IntelCascadeLake,
			explicitMachineTypes: []string{
				predefinedN1Cpu64Name,
				customN1Cpu4Memory64Name,
				customN1Cpu48Memory180Name, // known type
				customN1Cpu20Memory72Name,
				customN1Cpu28Memory300Name,

				predefinedN2Cpu2Name,
				customN2Cpu20Memory32Name,

				invalidTypeName, // invalid type,
			},
			expectedMachines: []MachineType{
				predefinedN2Cpu2,
				customN2Cpu20Memory32,
			},
			expectedLargestMachine: customN2Cpu20Memory32,
		},
		"N1 and N2 machine families with multiple explicitly required custom machine types, cpu platform constraint is not supported by any types": {
			families: []MachineFamily{N1, N2},
			platform: IntelGraniteRapids,
			explicitMachineTypes: []string{
				predefinedN1Cpu64Name,
				customN1Cpu4Memory64Name,
				customN1Cpu48Memory180Name, // known type
				customN1Cpu20Memory72Name,
				customN1Cpu28Memory300Name,

				predefinedN2Cpu2Name,
				customN2Cpu20Memory32Name,

				invalidTypeName, // invalid type,
			},
			expectedMachines:       []MachineType{},
			expectedLargestMachine: UnknownMachineType,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			spec := MachineSpec{
				Families:             tc.families,
				MinCpuPlatform:       tc.platform,
				GpuType:              tc.gpuType,
				ExplicitMachineTypes: tc.explicitMachineTypes,
			}

			assert.ElementsMatch(t, tc.expectedMachines, spec.AutoprovisionedMachineTypes())
			assert.Equal(t, tc.expectedLargestMachine, spec.LargestAutoprovisionedMachineType())
		})
	}
}

func TestMachineSpecSignature(t *testing.T) {
	for tn, tc := range map[string]struct {
		families          []MachineFamily
		platform          CpuPlatform
		gpuType           string
		tpuType           string
		bootDiskType      string
		computeClassName  string
		expectedSignature string
	}{
		"single machine family": {
			families:          []MachineFamily{M1},
			platform:          AmdRome,
			expectedSignature: `families="m1" min_cpu_platform="AMD Rome" gpu_type="" tpu_type="" boot_disk_type="" compute_class=""`,
		},
		"many machine families (sorted by name)": {
			families:          []MachineFamily{C2D, T2D},
			platform:          AmdMilan,
			expectedSignature: `families="c2d,t2d" min_cpu_platform="AMD Milan" gpu_type="" tpu_type="" boot_disk_type="" compute_class=""`,
		},
		"many machine families (not sorted by name)": {
			families:          []MachineFamily{T2D, C2D},
			platform:          AmdMilan,
			expectedSignature: `families="c2d,t2d" min_cpu_platform="AMD Milan" gpu_type="" tpu_type="" boot_disk_type="" compute_class=""`,
		},
		"single machine family with boot disk type": {
			families:          []MachineFamily{M1},
			platform:          AmdRome,
			bootDiskType:      "pd-balanced",
			expectedSignature: `families="m1" min_cpu_platform="AMD Rome" gpu_type="" tpu_type="" boot_disk_type="pd-balanced" compute_class=""`,
		},
		"single machine family with compute class": {
			families:          []MachineFamily{M1},
			platform:          AmdRome,
			computeClassName:  "m-class",
			expectedSignature: `families="m1" min_cpu_platform="AMD Rome" gpu_type="" tpu_type="" boot_disk_type="" compute_class="m-class"`,
		},
		"single machine family with GPU type": {
			families:          []MachineFamily{M1},
			platform:          AmdRome,
			gpuType:           "nvidia-tesla-a100",
			expectedSignature: `families="m1" min_cpu_platform="AMD Rome" gpu_type="nvidia-tesla-a100" tpu_type="" boot_disk_type="" compute_class=""`,
		},
		"single machine family with TPU device type": {
			families:          []MachineFamily{CT4L},
			platform:          AmdRome,
			tpuType:           "tpu-v4-lite-device",
			expectedSignature: `families="ct4l" min_cpu_platform="AMD Rome" gpu_type="" tpu_type="tpu-v4-lite-device" boot_disk_type="" compute_class=""`,
		},
		"single machine family with TPU podslice type": {
			families:          []MachineFamily{CT4P},
			platform:          AmdRome,
			tpuType:           "tpu-v4-podslice",
			expectedSignature: `families="ct4p" min_cpu_platform="AMD Rome" gpu_type="" tpu_type="tpu-v4-podslice" boot_disk_type="" compute_class=""`,
		},
		"many machine families with compute class": {
			families:          []MachineFamily{C2D, T2D},
			platform:          AmdMilan,
			computeClassName:  "confidential-class",
			expectedSignature: `families="c2d,t2d" min_cpu_platform="AMD Milan" gpu_type="" tpu_type="" boot_disk_type="" compute_class="confidential-class"`,
		},
		"many machine families with boot disk type": {
			families:          []MachineFamily{C2D, T2D},
			platform:          AmdMilan,
			bootDiskType:      "pd-balanced",
			expectedSignature: `families="c2d,t2d" min_cpu_platform="AMD Milan" gpu_type="" tpu_type="" boot_disk_type="pd-balanced" compute_class=""`,
		},
		"many machine families with GPU type": {
			families:          []MachineFamily{C2D, T2D},
			platform:          AmdMilan,
			gpuType:           "nvidia-tesla-a100",
			expectedSignature: `families="c2d,t2d" min_cpu_platform="AMD Milan" gpu_type="nvidia-tesla-a100" tpu_type="" boot_disk_type="" compute_class=""`,
		},
		"compute class and GPU type together": {
			families:          []MachineFamily{C2D, T2D},
			platform:          AmdMilan,
			computeClassName:  "confidential-class",
			gpuType:           "nvidia-tesla-a100",
			expectedSignature: `families="c2d,t2d" min_cpu_platform="AMD Milan" gpu_type="nvidia-tesla-a100" tpu_type="" boot_disk_type="" compute_class="confidential-class"`,
		},
		"AnyPlatform": {
			families:          []MachineFamily{N1},
			platform:          AnyPlatform,
			expectedSignature: `families="n1" min_cpu_platform="__ANY_PLATFORM" gpu_type="" tpu_type="" boot_disk_type="" compute_class=""`,
		},
		"UnknownPlatform (shouldn't happen, just a sanity check that we'll be able to tell if it does)": {
			families:          []MachineFamily{N2D},
			platform:          UnknownPlatform,
			expectedSignature: `families="n2d" min_cpu_platform="__UNKNOWN_PLATFORM" gpu_type="" tpu_type="" boot_disk_type="" compute_class=""`,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			spec := MachineSpec{
				Families:         tc.families,
				MinCpuPlatform:   tc.platform,
				GpuType:          tc.gpuType,
				TpuType:          tc.tpuType,
				BootDiskType:     tc.bootDiskType,
				ComputeClassName: tc.computeClassName,
			}
			assert.Equal(t, tc.expectedSignature, spec.Signature())
		})
	}
}
