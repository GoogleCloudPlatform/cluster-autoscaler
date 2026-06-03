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
	"math"
	"sort"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
)

// GpuSpec defines machine specific gpu properties. This is an overridden
// set of gpus supported over that of the machine family.
// Pointers used since some values can be unset even if the object is set
type GpuSpec struct {
	gpu           Gpu
	fixedGpuCount PhysicalGpuCount
}

// MachinePriceInfo defines machine specific pricing information which differs
// from that of the machine family. Pointers used since some values can be unset
// even if the object is set
type MachinePriceInfo struct {
	instancePrice           *float64
	preemtibleInstancePrice *float64
}

const (
	// MaxEvictionToCapacityRatio is the maximum ratio of eviction memory to machine capacity.
	MaxEvictionToCapacityRatio = 0.5
)

// MachineType defines a new object which should enclose all machine specific information.
// This is extension of gce.MachineType because existing object don't provide the ability to add gke specific config
// Pointer fields may be optional since they may not be relevant to all machines.
type MachineType struct {
	gce.MachineType

	// Default values for machine types are often defined in MachineFamily.
	// Let's not abuse it by accessing other machine types from MachineType methods.
	family *MachineFamily

	// Overrides specific to this machine type.
	gpuOverridden           *GpuSpec
	cpuPlatformRequirements *CpuPlatformRequirements
	priceInfo               *MachinePriceInfo
	ephemeralLocalSsdCfg    *ephemeralLocalSsdConfig
	confidentialNodeCfg     *confidentialNodeConfig
	// Use this machine type for NAP only if explicitly required
	explicitReqOnly bool
	// threadsPerCore zero value indicates no override of threads per core for the machine type,
	// analogous to the SMT setting, see compute.Instance.AdvancedMachineFeatures.ThreadsPerCore
	threadsPerCore int64
	// supportedDisksOverride and defaultDiskOverride should not be used if all machine types
	// within the machine family support the same set of disks / have the same default disk.
	supportedDisksOverride *[]string
	defaultDiskOverride    string
	notInDWS               bool
	resizableConfig        *ResizableMachineTypeConfig
}

type ephemeralLocalSsdConfig struct {
	automaticDiskCount *int64
	allowedDiskCounts  map[int]bool
	diskSize           uint64
}

type confidentialNodeConfig struct {
	// supportConfidentialNodeTypes defines the Confidential Node types supported by the machine type
	// overriding the confidential node types supported at the machine family level
	supportConfidentialNodeTypes map[string]bool
}

// withDefaultDiskOverride returns a new object with the default disk overriden for the machine type
func (t MachineType) withDefaultDiskOverride(defaultDisk string) MachineType {
	t.defaultDiskOverride = defaultDisk
	return t
}

func (t MachineType) withSupportedDisksOverride(supportedDisks []string) MachineType {
	t.supportedDisksOverride = &supportedDisks
	return t
}

// withGpuOverride returns a new object with the label Override gpu properties for the machine type
func (t MachineType) withGpuOverride(gpu Gpu, fixedGpuCount PhysicalGpuCount) MachineType {
	t.gpuOverridden = &GpuSpec{
		gpu:           gpu,
		fixedGpuCount: fixedGpuCount,
	}
	return t
}

// withInstancePriceOverride returns a new object with the regular price override for the machine type
func (t MachineType) withInstancePriceOverride(price float64) MachineType {
	if t.priceInfo == nil {
		t.priceInfo = &MachinePriceInfo{}
	}
	t.priceInfo.instancePrice = &price
	return t
}

// withPreemptibleInstancePriceOverride returns a new object with the preemptible price override for the machine type
func (t MachineType) withPreemptibleInstancePriceOverride(price float64) MachineType {
	if t.priceInfo == nil {
		t.priceInfo = &MachinePriceInfo{}
	}
	t.priceInfo.preemtibleInstancePrice = &price
	return t
}

// withThreadsPerCoreOverride returns a new object with the threads per core override for the machine type
func (t MachineType) withThreadsPerCoreOverride(threadsPerCoreOverride int64) MachineType {
	t.threadsPerCore = threadsPerCoreOverride
	return t
}

// withCpuPlatformRequirements returns a new object with the cpu platform properties for the machine type
func (t MachineType) withCpuPlatformRequirements(properties CpuPlatformRequirements) MachineType {
	t.cpuPlatformRequirements = &properties
	return t
}

// withAutomaticEphemeralLocalSsdCount returns a new object with the automatic local SSD cards set appropriately
func (t MachineType) withAutomaticEphemeralLocalSsdCount(count int64) MachineType {
	t.ephemeralLocalSsdCfg = &ephemeralLocalSsdConfig{
		automaticDiskCount: &count,
		allowedDiskCounts:  map[int]bool{int(count): true},
	}
	return t
}

// withAllowedEphemeralLocalSsdCounts defines allowed Local SSD disk counts for the machine type.
func (t MachineType) withAllowedEphemeralLocalSsdCounts(values ...int) MachineType {
	allowedValues := make(map[int]bool)
	for _, v := range values {
		allowedValues[v] = true
	}
	t.ephemeralLocalSsdCfg = &ephemeralLocalSsdConfig{allowedDiskCounts: allowedValues}
	return t
}

// withExplicitReqOnly returns a new object that will be used for NAP only if requested explicitly
func (t MachineType) withExplicitReqOnly() MachineType {
	t.explicitReqOnly = true
	return t
}

// withDwsDisabled returns a new object that is marked as not supported by DWS.
// Should not be applied to all machine types in a given family, instead disable DWS support for the
// whole family by using dwsDisabled field in MachineFamily struct.
func (t MachineType) withDwsDisabled() MachineType {
	t.notInDWS = true
	return t
}

// withSupportedConfidentialNodeTypes returns a new object that support the given confidential node types
// overriding the confidential node types support defined at the machine family level
func (t MachineType) withSupportedConfidentialNodeTypes(nodeTypes []string) MachineType {
	t.confidentialNodeCfg = &confidentialNodeConfig{make(map[string]bool)}
	for _, nodeType := range nodeTypes {
		t.confidentialNodeCfg.supportConfidentialNodeTypes[nodeType] = true
	}
	return t
}

// cpuPlatformRequirementsOverrides fetches the cpu platform properties for the machine type if it is set
// else it returns nil with found=false
func (t MachineType) cpuPlatformRequirementsOverrides() (cpuPlatformRequirement CpuPlatformRequirements, found bool) {
	if t.cpuPlatformRequirements == nil {
		return CpuPlatformRequirements{}, false
	}
	return *t.cpuPlatformRequirements, true
}

// gpuOverride fetches the Override gpu properties for the
// machine type if it is set else it returns nil with found=false
func (t MachineType) gpuOverride() (gpuOverride GpuSpec, found bool) {
	if t.gpuOverridden == nil {
		return GpuSpec{}, false
	}
	return *t.gpuOverridden, true
}

// HasFixedGPU returns if the machine type has fixed GPU configuration.
func (t MachineType) HasFixedGPU() bool {
	_, found := t.gpuOverride()
	return found
}

// GpuType returns gpu type if the machine type has fixed GPU configuration.
func (t MachineType) GpuType() string {
	if gpuSpec := t.gpuOverridden; gpuSpec != nil {
		return gpuSpec.gpu.Name()
	}
	return ""
}

// FixedGpuCount returns fixed gpu count if the machine type has fixed GPU configuration.
func (t MachineType) FixedGpuCount() PhysicalGpuCount {
	if gpuSpec := t.gpuOverridden; gpuSpec != nil {
		return gpuSpec.fixedGpuCount
	}
	return 0
}

// InstancePriceOverride fetches the regular price for the
// machine type if it is set else it returns nil with found=false
func (t MachineType) InstancePriceOverride() (float64, bool) {
	if t.priceInfo == nil || t.priceInfo.instancePrice == nil {
		return 0, false
	}
	return *t.priceInfo.instancePrice, true
}

// PreemptibleInstancePriceOverride fetches the preemptible price for the
// machine type info if it is set else it returns nil with found=false
func (t MachineType) PreemptibleInstancePriceOverride() (float64, bool) {
	if t.priceInfo == nil || t.priceInfo.preemtibleInstancePrice == nil {
		return 0, false
	}
	return *t.priceInfo.preemtibleInstancePrice, true
}

func (t MachineType) ThreadsPerCore() int64 {
	return t.threadsPerCore
}

// AutomaticEphemeralLocalSsdCount fetches the count of local SSD counts to automatically
// use for ephemeral storage, if set, otherwise return found=false
func (t MachineType) AutomaticEphemeralLocalSsdCount() (count int64, found bool) {
	if cfg := t.ephemeralLocalSsdCfg; cfg != nil && cfg.automaticDiskCount != nil {
		return *cfg.automaticDiskCount, true
	}
	return 0, false
}

// AllowedEphemeralLocalSsdCounts returns all allowed local SSD counts sorted ascendingly.
func (t MachineType) AllowedEphemeralLocalSsdCounts() (counts []int, found bool) {
	if cfg := t.ephemeralLocalSsdCfg; cfg != nil && len(cfg.allowedDiskCounts) != 0 {
		values := mapsKeys(cfg.allowedDiskCounts)
		sort.Ints(values)
		return values, true
	}
	return nil, false
}

// LocalSSDDiskSize returns the size of local SSDs available with this machine type.
func (t MachineType) LocalSSDDiskSize() (size uint64, supported bool) {
	if cfg := t.ephemeralLocalSsdCfg; cfg != nil && cfg.diskSize != 0 {
		return cfg.diskSize, true
	}
	return 0, false
}

// AllocatableHugepageRatioCap returns allocatable hugepage ratio cap depending on machine memory.
// Hugepage will be preallocated on memory and leave free memory limited. To prevent over
// consuming memory makes the node unavailable, we set an arbitrary cap to restrict the memory used for hugepages.
// For machines with less than 30Gi memory, we set 60% cap. E.g. Customer request 1024 of 2M hugepages and 4
// of 1G hugepages, total memory for hugepages are 6Gb, for VM such as c2d-standard-2 where only
// have 8Gb memory, this request exceed the cap of 8Gb * 0.6 = 4.8Gb and will be failed.
// For machines with equal or more than 30Gi memory, we set 80% cap, and ensure it does not affect reserved memory.
func (t MachineType) AllocatableHugepageRatioCap() float64 {
	if t.Memory >= 30*units.GiB {
		return float64(0.8)
	}
	return float64(0.6)
}

// MaximumAllocatableHugepageCapacityInMB returns maximum allocatable hugepage capacity in MB.
func (t MachineType) MaximumAllocatableHugepageCapacityInMB() int64 {
	return int64(math.Floor((float64(t.Memory) / float64(units.MiB)) * t.AllocatableHugepageRatioCap()))
}

// MaximumAllowedEvictionMemory returns maximum allowed eviction memory.
func (t MachineType) MaximumAllowedEvictionMemory() int64 {
	return int64(float64(t.Memory) * MaxEvictionToCapacityRatio)
}

// GetThreadsPerCore returns threads.
func (t MachineType) GetThreadsPerCore() int64 {
	if t.threadsPerCore > 0 {
		return t.threadsPerCore
	}
	if t.family != nil {
		return t.family.threadsPerCore()
	}
	return DefaultThreadPerCore
}

// IsConfidentialNodeTypeSupported returns true if the machine type supports the given confidential node type.
func (t MachineType) IsConfidentialNodeTypeSupported(confidentialNodeType string) bool {
	if t.confidentialNodeCfg != nil {
		return t.confidentialNodeCfg.supportConfidentialNodeTypes[confidentialNodeType]
	}
	return t.family.supportConfidentialNodeTypes[confidentialNodeType]
}

// NewMachineTypeInfo returns information a MachineTypeInfo object based on a static
// configuration of name cpu and memory
func NewMachineTypeInfo(name string, cpu int64, memoryGb float64) MachineType {
	mem := math.Floor(memoryGb * units.GiB)
	return MachineType{
		MachineType: gce.MachineType{
			Name:   name,
			CPU:    cpu,
			Memory: int64(mem),
		},
	}
}

// NewResizableMachineTypeInfo returns information about a MachineType object based on a static
// configuration of name, cpu, memory, and resizable config.
func NewResizableMachineTypeInfo(name string, cpu int64, memoryGb float64, resizableConfig *ResizableMachineTypeConfig) MachineType {
	mem := math.Floor(memoryGb * units.GiB)
	return MachineType{
		MachineType: gce.MachineType{
			Name:   name,
			CPU:    cpu,
			Memory: int64(mem),
		},
		resizableConfig: resizableConfig,
	}
}

// IsLargerThan returns whether the machine type is larger than the other machine type,
// if any of the machine types has a threads per core override different from the default
// value for the family, the CPU value must be recalculated into the common threads per core
// value
func IsLargerThan(a, b MachineType) bool {
	aCPU := a.CPU
	bCPU := b.CPU
	aCPU /= a.GetThreadsPerCore()
	bCPU /= b.GetThreadsPerCore()
	if aCPU != bCPU {
		return aCPU > bCPU
	}
	if a.Memory != b.Memory {
		return a.Memory > b.Memory
	}
	aSSDCount, _ := a.AutomaticEphemeralLocalSsdCount()
	bSSDCount, _ := b.AutomaticEphemeralLocalSsdCount()

	return aSSDCount > bSSDCount
}

// Largest returns the largest machine type out of the provided ones.
func Largest(machineTypes map[string]MachineType) MachineType {
	result := UnknownMachineType
	for _, machineType := range machineTypes {
		if IsLargerThan(machineType, result) {
			result = machineType
		}
	}
	return result
}

// mapsKeys is equivalent helper function of maps.Keys() for Go 1.20.
func mapsKeys(m map[int]bool) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
