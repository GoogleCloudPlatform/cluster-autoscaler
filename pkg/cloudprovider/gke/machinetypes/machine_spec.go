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
	"sort"
	"strings"
)

// MachineSpec combines information about machine family and min-cpu-platform.
type MachineSpec struct {
	Families                 []MachineFamily
	MinCpuPlatform           CpuPlatform
	GpuType                  string
	TpuType                  string
	BootDiskType             string
	ComputeClassName         string
	ExplicitMachineTypes     []string
	ConfidentialNodesEnabled bool
	ConfidentialNodeType     string
}

// String returns a human-readable description of the spec.
func (s MachineSpec) String() string {
	return s.Signature()
}

// Signature returns a stable string representation of the spec.
func (s MachineSpec) Signature() string {
	var names []string
	for _, family := range s.Families {
		names = append(names, family.Name())
	}
	sort.Strings(names)
	return fmt.Sprintf("families=%q min_cpu_platform=%q gpu_type=%q tpu_type=%q boot_disk_type=%q compute_class=%q", strings.Join(names, ","), CpuPlatformDebugName(s.MinCpuPlatform), s.GpuType, s.TpuType, s.BootDiskType, s.ComputeClassName)
}

// AutoprovisionedMachineTypes returns autoprovisioned machine types specified by the spec.
// Custom unknown but supported and specified in ExplicitMachineTypes machine types are also included.
func (s MachineSpec) AutoprovisionedMachineTypes() []MachineType {
	var result []MachineType
	constraints := Constraints{
		GpuType:                   s.GpuType,
		CpuPlatform:               s.MinCpuPlatform,
		TpuType:                   s.TpuType,
		ExplicitMachineTypes:      s.ExplicitMachineTypes,
		ConfidentialNodesRequired: s.ConfidentialNodesEnabled,
		ConfidentialNodeType:      s.ConfidentialNodeType,
	}
	for _, family := range s.Families {

		for _, machine := range family.AutoprovisionedMachineTypes(constraints) {
			result = append(result, machine)
		}
	}
	return result
}

// LargestAutoprovisionedMachineType returns the largest machine type for this spec.
func (s MachineSpec) LargestAutoprovisionedMachineType() MachineType {
	largestMachineType := UnknownMachineType
	constraints := Constraints{
		GpuType:                   s.GpuType,
		CpuPlatform:               s.MinCpuPlatform,
		TpuType:                   s.TpuType,
		ExplicitMachineTypes:      s.ExplicitMachineTypes,
		ConfidentialNodesRequired: s.ConfidentialNodesEnabled,
		ConfidentialNodeType:      s.ConfidentialNodeType,
	}
	for _, family := range s.Families {

		machineType := family.LargestAutoprovisionedMachineType(constraints)
		if IsLargerThan(machineType, largestMachineType) {
			largestMachineType = machineType
		}
	}
	return largestMachineType
}

// NewMachineSpec returns a new machineSpec
func NewMachineSpec(families []MachineFamily, platform CpuPlatform, gpuType, tpuType string) MachineSpec {
	return MachineSpec{
		Families:       families,
		MinCpuPlatform: platform,
		GpuType:        gpuType,
		TpuType:        tpuType,
	}
}

// NewMachineSpecSingleFamily returns a new machineSpec with a single family
func NewMachineSpecSingleFamily(family MachineFamily, platform CpuPlatform, gpuType, tpuType string) MachineSpec {
	return MachineSpec{
		Families:       []MachineFamily{family},
		MinCpuPlatform: platform,
		GpuType:        gpuType,
		TpuType:        tpuType,
	}
}

// NewExplicitMachineSpec returns a new machine spec with machine types requested explicitly
func NewExplicitMachineSpec(families []MachineFamily, platform CpuPlatform, gpuType, tpuType string, explicitMachineTypes []string) MachineSpec {
	return MachineSpec{
		Families:             families,
		MinCpuPlatform:       platform,
		GpuType:              gpuType,
		TpuType:              tpuType,
		ExplicitMachineTypes: explicitMachineTypes,
	}
}
