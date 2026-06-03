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

import "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"

// NewTestMachineFamily returns a new MachineFamily object to use in test code.
func NewTestMachineFamily(name string, machineTypes []MachineType, platformReqLower, platformReqUpper CpuPlatform, gpuTypes []Gpu, bootDiskTypes []string) MachineFamily {
	var cpuReq CpuPlatformRequirements
	if platformReqLower != UnknownPlatform && platformReqUpper != UnknownPlatform {
		cpuReq = CpuPlatformRequirements{lowerBound: platformReqLower, upperBound: platformReqUpper}
	}
	mf := MachineFamily{
		name:                        name,
		autoprovisionedMachineTypes: onboardMachineType(machineTypes...),
		supportedCpuPlatforms:       cpuReq,
		supportedGpuTypes:           onboardSupportedGpus(gpuTypes...),
		supportedBootDiskTypes:      sliceToSet(bootDiskTypes),
	}
	mf.precomputeAllMachineTypes()
	return mf
}

// NewTestMachineTypeInfo is a utility used to create a MachineType for testing since members are non-public
// This func needs to change when adding new members
func NewTestMachineTypeInfo(machineType gce.MachineType, spec GpuSpec, requirements *CpuPlatformRequirements, priceInfo *MachinePriceInfo) MachineType {
	return MachineType{
		MachineType:             machineType,
		gpuOverridden:           &spec,
		cpuPlatformRequirements: requirements,
		priceInfo:               priceInfo,
	}
}

// NewTestMachineGpuSpec is a utility used to create GpuSpec for testing
func NewTestMachineGpuSpec(gpu Gpu, fixedGpuCount PhysicalGpuCount) GpuSpec {
	return GpuSpec{
		gpu:           gpu,
		fixedGpuCount: fixedGpuCount,
	}
}

func NewTestGpu(name string, isNap bool, maxCpuCount map[PhysicalGpuCount]int, pSize map[string]int64) Gpu {
	return Gpu{
		name:           name,
		isNapSupported: isNap,
		maxCpuCount:    maxCpuCount,
		partitionSizes: pSize,
	}
}

func sliceToSet[T comparable](slice []T) map[T]bool {
	hashSet := make(map[T]bool)
	for _, v := range slice {
		hashSet[v] = true
	}
	return hashSet
}
