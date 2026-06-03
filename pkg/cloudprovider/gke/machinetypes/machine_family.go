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
	"slices"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/klog/v2"
)

const (
	// DefaultThreadPerCore defines the default number of threads per core
	DefaultThreadPerCore int64 = 2
)

// MachineFamilyPricingInfo defines the machine family specific pricing information.
// These values apply to all machines in the family unless the machine has overrides
// which don't follow family pricing
type MachineFamilyPricingInfo struct {
	CpuPricePerHour           float64
	MemoryPricePerHourPerGb   float64
	LocalSsdPricePerHourPerGb float64
	PreemptibleDiscount       float64
}

// Constraints contain parameters that can limit the set of compatible machine types from a given machine family.
type Constraints struct {
	CpuPlatform             CpuPlatform
	GpuType                 string
	TpuType                 string
	DiskType                string
	ExplicitMachineTypes    []string
	AllExplicitTypesAllowed bool
	ConfidentialNodeType    string
}

// String returns a human-readable representation of the constraints.
func (c Constraints) String() string {
	return fmt.Sprintf("CPU platform: %q, GPU type: %q, TPU type: %q", CpuPlatformDebugName(c.CpuPlatform), c.GpuType, c.TpuType)
}

func (c Constraints) isNoConstraints() bool {
	return c.CpuPlatform == AnyPlatform &&
		c.GpuType == "" &&
		c.TpuType == "" &&
		c.DiskType == "" &&
		len(c.ExplicitMachineTypes) == 0 &&
		c.AllExplicitTypesAllowed &&
		c.ConfidentialNodeType == ""
}

// NoConstraints is a constraint object compatible with every machine type. It should be passed to family methods to
// ensure every machine type is returned.
var NoConstraints = Constraints{CpuPlatform: AnyPlatform, GpuType: "", TpuType: "", AllExplicitTypesAllowed: true}

// UnknownMachineType is a placeholder representing unknown machine type
var UnknownMachineType = NewMachineTypeInfo("unknown-machine-type", 0, 0)

// MachineFamily contains information about a machine family.
type MachineFamily struct {
	name string
	// systemArchitecture defines the system architecture associated with the machine family
	systemArchitecture gce.SystemArchitecture
	// autoprovisionedMachineTypes contains the types of machines from this family that can be used for autoprovisioning.
	autoprovisionedMachineTypes map[string]MachineType
	// otherMachineTypes contains the types of machines from this family that cannot be used for autoprovisioning.
	otherMachineTypes map[string]MachineType
	// pricingInfo contains the standard pricing information applicable to the machine family
	pricingInfo MachineFamilyPricingInfo
	// customPricingInfo contains the pricing information for custom machines applicable to machine family
	customPricingInfo *MachineFamilyPricingInfo
	// supportedCpuPlatforms contains CPU platform requirements for every autoprovisioned machine type that is
	// not present in supportedCpuPlatformsOverrides. Typically most of the machine types within a single family have the
	// same requirements, but some families have different requirements for certain machine types. This field caters to
	// the typical use case, while supportedCpuPlatformsOverrides allows handling the other families as well. Technically
	// we could just define the requirements per-machine-type in the first place, but that would lead to a lot of repeated
	// code in the typical case.
	supportedCpuPlatforms CpuPlatformRequirements
	// supportedGpuTypes contains GPU types supported by all autoprovisioned machine types from the family.
	supportedGpuTypes map[string]Gpu
	// supportedTpuTypes contains TPU types supported by all autoprovisioned machine types from the family.
	supportedTpuTypes map[string]bool
	// supportCompactPlacement defines if the machine family supports compact placement
	supportCompactPlacement bool
	// maxCompactPlacementNodes defines the maximum number of nodes in node pool for compact placement
	maxCompactPlacementNodes int64
	// nonDefaultThreadsPerCore defines the number of threads per core where the value
	// defers from the default value for the machine family
	nonDefaultThreadsPerCore *int64
	// supportConfidentialNodes defines if the machine family supports Confidential Nodes
	supportConfidentialNodes bool
	// supportConfidentialNodeTypes defines the Confidential Node types supported by the machine family
	// Information is taken from go/confidential-node-ccc
	// In the future this should be retreived from GCE instead of hard-coded here
	supportConfidentialNodeTypes map[string]bool
	// supportedBootDiskTypes contains disk types supported by all autoprovisioned machine types from the family.
	// Hyperdisk Extreme and Hyperdisk Throughput cannot be boot disks.
	// For more details, see gkecl/1044834/comment/6b7b5ffd_f94b92d9/
	supportedBootDiskTypes map[string]bool
	// supportedAttachDiskTypes contains disk types that can be attached to all machine types from the family.
	// For more details, see gkecl/1463324
	supportedAttachDiskTypes map[string]ConfidentialMode
	// defaultDiskType should be in sync with persistent_disk_config.default_persistent_disk_type
	// ref: http://google3/configs/cloud/cluster/vmfamilies/sot/textproto/common_metadata/vm_family_metadata/
	defaultDiskType string
	// supportHugepageSize1g defines if the machine family supports 1-gigabyte-sized huge pages.
	supportHugepageSize1g bool
	// supportsAcceleratorSlice defines if the machine family supports GCE accelerator slices.
	// Slice is a group of VMs with specified accelerator topology that enables high performant
	// connectivity between accelerators to run jobs distributed among multiple VMs.
	supportsAcceleratorSlice bool
	// dwsDisabled defines if the machine family supports DWS so we can disable the accelerator
	// support for a whole family. The Cluster Autoscaler makes a silent assumption that only one
	// machine family is equipped with each type of a certain accelerator, so a machine family
	//  disablement is equal to disabling DWS support for a certain accelerator.
	// Note that there should not be a case where you disable DWS manually for all machine types in
	// a certain family, instead you should add this variable to the family definition.
	dwsDisabled bool
	// draComputeDomainAutoDetection specifies whether Cluster Autoscaler should try to automatically detect
	// if ComputeDomain DRA Devices should be predicted, based on the existence of specific DeviceClasses.
	// Details: go/dra-nap-computedomains
	draComputeDomainAutoDetection bool
	resizableConfig               *ResizableMachineFamilyConfig
	// allMachineTypes contains all machine types from this family.
	// It is used to avoid memory allocations and computations in AllMachineTypes().
	allMachineTypes map[string]MachineType
}

// ConfidentialMode acts as an enum for confidential disk support.
type ConfidentialMode string

var (
	ConfidentialOnlyMode    = ConfidentialMode("ConfidentialOnly")
	NonConfidentialOnlyMode = ConfidentialMode("NonConfidentialOnly")
	UnspecifiedMode         = ConfidentialMode("Unspecified")
)

// Name returns the name of the machine family.
func (f MachineFamily) Name() string {
	return f.name
}

// Equal checks whether f is the same as otherFamily.
func (f MachineFamily) Equal(otherFamily MachineFamily) bool {
	return f.In(otherFamily)
}

// In checks whether f is one of otherFamilies.
func (f MachineFamily) In(otherFamilies ...MachineFamily) bool {
	for _, otherFamily := range otherFamilies {
		if f.name == otherFamily.name {
			return true
		}
	}
	return false
}

func (d *MachineFamily) ListSupportedDisks(isConfidentialNode bool) []string {
	supportedDisks := []string{}

	for diskType, confidentialMode := range d.supportedAttachDiskTypes {
		// Skip disks whose confidential mode does not match the node's confidentiality.
		// Disks with an unspecified confidential mode are supported by both confidential and non-confidential nodes.
		switch {
		case confidentialMode == NonConfidentialOnlyMode && isConfidentialNode:
			continue
		case confidentialMode == ConfidentialOnlyMode && !isConfidentialNode:
			continue
		default:
			supportedDisks = append(supportedDisks, diskType)
		}
	}
	return supportedDisks
}

// AreConstraintsSupported returns whether the given constraints are supported by any autoprovisioned machine type from this family.
func (f MachineFamily) AreConstraintsSupported(constraints Constraints) bool {
	return len(f.AutoprovisionedMachineTypes(constraints)) > 0
}

// IsPlatformSupported returns whether the given CPU platform is supported by any autoprovisioned machine type from this family.
func (f MachineFamily) IsPlatformSupported(cpuPlatform CpuPlatform) bool {
	return f.AreConstraintsSupported(Constraints{CpuPlatform: cpuPlatform, GpuType: ""})
}

// IsDiskTypeSupported returns whether the given disk type is supported by any machine type in the machine family.
func (f MachineFamily) IsDiskTypeSupported(diskType string) bool {
	return f.AreConstraintsSupported(Constraints{CpuPlatform: AnyPlatform, DiskType: diskType})
}

// AreMachineTypesSupported returns whether any of the provided machine types is supported in the machine family.
func (f MachineFamily) AreMachineTypesSupported(machineTypes []string) bool {
	return f.AreConstraintsSupported(Constraints{CpuPlatform: AnyPlatform, ExplicitMachineTypes: machineTypes, AllExplicitTypesAllowed: true})
}

// IsDiskTypeSupportedForMachineType returns whether the given disk type is supported by the provided machine type, if given.
func (f MachineFamily) IsDiskTypeSupportedForMachineType(diskType string, machineType string) bool {
	return f.AreConstraintsSupported(Constraints{CpuPlatform: AnyPlatform, DiskType: diskType, ExplicitMachineTypes: []string{machineType}})
}

// IsDwsDisabled returns whether the given machine family has DWS supported turned off for the whole family.
func (f MachineFamily) IsDwsDisabled() bool {
	return f.dwsDisabled
}

// IsGpuTypeSupported returns whether the given GPU type is supported by any autoprovisioned machine type from this family.
func (f MachineFamily) IsGpuTypeSupported(gpuType string) bool {
	return f.AreConstraintsSupported(Constraints{CpuPlatform: AnyPlatform, GpuType: gpuType, AllExplicitTypesAllowed: true})
}

// IsTpuTypeSupported returns whether the given TPU type is supported by any autoprovisioned machine type from this family.
func (f MachineFamily) IsTpuTypeSupported(tpuType string) bool {
	return f.AreConstraintsSupported(Constraints{CpuPlatform: AnyPlatform, TpuType: tpuType, AllExplicitTypesAllowed: true})
}

// IsTpuSupported returns whether machine family supports TPU.
func (f MachineFamily) IsTpuSupported() bool {
	return len(f.supportedTpuTypes) > 0
}

// IsCompactPlacementSupported returns whether the given machine family supports compact placement
func (f MachineFamily) IsCompactPlacementSupported() bool {
	return f.supportCompactPlacement
}

// IsHugepageSize1gSupported returns whether the given machine family supports 1-gigabyte-sized huge pages.
func (f MachineFamily) IsHugepageSize1gSupported() bool {
	return f.supportHugepageSize1g
}

// MaxCompactPlacementNodes returns the maximum number of nodes for compact placement if the machine family supports compact placement
// and the value for maxCompactPlacementNodes is set.
func (f MachineFamily) MaxCompactPlacementNodes() (int64, error) {
	if !f.supportCompactPlacement {
		return 0, fmt.Errorf("machine family %q does not support compact placement", f.Name())
	}
	return f.maxCompactPlacementNodes, nil
}

// IsConfidentialNodesSupported returns whether the given machine family supports confidential nodes
func (f MachineFamily) IsConfidentialNodesSupported() bool {
	return f.supportConfidentialNodes
}

func (f MachineFamily) PricingInfo() MachineFamilyPricingInfo {
	return f.pricingInfo
}

func (f MachineFamily) CustomPricingInfo() *MachineFamilyPricingInfo {
	return f.customPricingInfo
}

// precomputeAllMachineTypes precomputes data that is frequently used but doesn't change.
func (f *MachineFamily) precomputeAllMachineTypes() {
	f.allMachineTypes = make(map[string]MachineType, len(f.autoprovisionedMachineTypes)+len(f.otherMachineTypes))
	for k, v := range f.autoprovisionedMachineTypes {
		f.allMachineTypes[k] = v
	}
	for k, v := range f.otherMachineTypes {
		f.allMachineTypes[k] = v
	}
}

// DefaultAutoprovisionedBootDiskType returns the default disk type for NAP
func (f MachineFamily) DefaultAutoprovisionedBootDiskType(machineType string) string {
	if val, ok := f.autoprovisionedMachineTypes[machineType]; ok {
		if val.defaultDiskOverride != "" {
			return val.defaultDiskOverride
		}
	}
	return f.defaultDiskType
}

// AutoprovisionedMachineTypes returns predefined autoprovisioned machine types
// as well as custom unknown ones from this family suitable for a given set of constraints.
func (f MachineFamily) AutoprovisionedMachineTypes(constraints Constraints) map[string]MachineType {
	result := make(map[string]MachineType)
	predefinedAutoprovisionedMachineTypes := f.supportedMachineTypes(f.autoprovisionedMachineTypes, constraints)
	explicitlySetCustomMachineTypes := f.explicitlySetCustomMachineTypes(constraints)

	for machineTypeName, machineTypeInfo := range predefinedAutoprovisionedMachineTypes {
		result[machineTypeName] = machineTypeInfo
	}
	for machineTypeName, machineTypeInfo := range explicitlySetCustomMachineTypes {
		result[machineTypeName] = machineTypeInfo
	}

	return result
}

// explicitlySetCustomMachineTypes returns custom theoretically supported machine types from this family suitable for a given set of constraints.
// Those types are taken from ExplicitMachineTypes values in constraints.
func (f MachineFamily) explicitlySetCustomMachineTypes(constraints Constraints) map[string]MachineType {
	if len(constraints.ExplicitMachineTypes) == 0 {
		return map[string]MachineType{}
	}

	validCustomMachineTypes := make(map[string]MachineType)
	for _, machineTypeName := range constraints.ExplicitMachineTypes {
		// Skip if not custom machine type.
		if !gce.IsCustomMachine(machineTypeName) {
			continue
		}
		machineFamilyName, err := gce.GetMachineFamily(machineTypeName)
		if err != nil {
			klog.Warningf("Failed to get machine family name from machine type %q: %v", machineTypeName, err)
			continue
		}
		// Skip if the machine family does not match the machine type.
		if machineFamilyName != f.name {
			continue

		}
		// Some custom types are predefined.
		machineType, found := f.AllMachineTypes(NoConstraints)[machineTypeName]
		if !found {
			machineType, err = ToCustomMachineType(machineTypeName)
			if err != nil {
				klog.Warningf("Failed to get machine type object from machine type name %q: %v", machineTypeName, err)
				continue
			}
		}
		validCustomMachineTypes[machineType.Name] = machineType
	}

	return f.supportedMachineTypes(validCustomMachineTypes, constraints)
}

// AllMachineTypes returns all predefined machine types from this family suitable for a given CPU set of constraints
func (f MachineFamily) AllMachineTypes(constraints Constraints) map[string]MachineType {
	if f.allMachineTypes == nil {
		if len(f.autoprovisionedMachineTypes) > 0 || len(f.otherMachineTypes) > 0 {
			panic(fmt.Sprintf("MachineFamily %q has nil allMachineTypes but contains %d+%d types. Ensure precomputeAllMachineTypes() is called after initialization.", f.name, len(f.autoprovisionedMachineTypes), len(f.otherMachineTypes)))
		}
	}
	if constraints.isNoConstraints() {
		return f.allMachineTypes
	}

	return f.supportedMachineTypes(f.allMachineTypes, constraints)
}

// SupportedGpuTypes returns gpu types supported by all the machine types in this family
func (f MachineFamily) SupportedGpuTypes() map[string]Gpu {
	return f.supportedGpuTypes
}

func (f MachineFamily) supportedMachineTypes(machineTypes map[string]MachineType, constraints Constraints) map[string]MachineType {
	var result = map[string]MachineType{}
	for _, machineType := range machineTypes {
		if constraints.rejectedByExplicitMachineConstraint(machineType) {
			continue
		}
		supportedPlatforms := f.supportedCpuPlatforms
		if overridePlatforms, found := machineType.cpuPlatformRequirementsOverrides(); found {
			supportedPlatforms = overridePlatforms
		}
		if !supportedPlatforms.validate(constraints.CpuPlatform) {
			continue
		}

		if constraints.GpuType != "" {
			overrideGpuType, found := machineType.gpuOverride()
			if found {
				if overrideGpuType.gpu.Name() != constraints.GpuType {
					continue
				}
			} else if _, found := f.supportedGpuTypes[constraints.GpuType]; !found {
				continue
			}
		}

		if constraints.TpuType != "" && !f.supportedTpuTypes[constraints.TpuType] {
			continue
		}

		if constraints.DiskType != "" {
			if machineType.supportedDisksOverride == nil { // if we didn't override the default for all
				if !f.supportedBootDiskTypes[constraints.DiskType] {
					continue
				}
			} else { // if the defaults are overriden
				if !slices.Contains(*machineType.supportedDisksOverride, constraints.DiskType) {
					continue
				}
			}
		}

		if constraints.ConfidentialNodeType != "" && !machineType.IsConfidentialNodeTypeSupported(constraints.ConfidentialNodeType) {
			continue
		}

		result[machineType.Name] = machineType
	}
	return result
}

func (c Constraints) rejectedByExplicitMachineConstraint(machine MachineType) bool {
	if len(c.ExplicitMachineTypes) == 0 {
		return machine.explicitReqOnly && !c.AllExplicitTypesAllowed
	}
	for _, mt := range c.ExplicitMachineTypes {
		if mt == machine.Name {
			return false
		}
	}
	return true
}

// CustomAutoprovisionedMachineTypes returns custom machine types from this family suitable for autoprovisioning for
// a given set of constraints.
func (f MachineFamily) CustomAutoprovisionedMachineTypes(constraints Constraints) map[string]MachineType {
	var result = map[string]MachineType{}
	allTypes := f.AutoprovisionedMachineTypes(constraints)
	for _, machineType := range allTypes {
		if gce.IsCustomMachine(machineType.Name) {
			result[machineType.Name] = machineType
		}
	}
	return result
}

// LargestAutoprovisionedMachineType returns the largest machine type (for a given set of constraints) suitable for autoprovisioning
// from this machine family.
func (f MachineFamily) LargestAutoprovisionedMachineType(constraints Constraints) MachineType {
	return Largest(f.AutoprovisionedMachineTypes(constraints))
}

// LargestMachineType returns the largest machine type (for a given set of constraints) from this machine family.
func (f MachineFamily) LargestMachineType(constraints Constraints) MachineType {
	return Largest(f.AllMachineTypes(constraints))
}

// SystemArchitecture returns the system architecture of the machine family
func (f MachineFamily) SystemArchitecture() gce.SystemArchitecture {
	return f.systemArchitecture
}

// IsAcceleratorSliceSupported returns whether the machine family supports accelerator slice
func (f MachineFamily) IsAcceleratorSliceSupported() bool {
	return f.supportsAcceleratorSlice
}

// IsAcceleRequiresBYOResourcePolicy returns whether the machine family requires BYO resource (workload) policy
func (f MachineFamily) RequiresBYOResourcePolicy() bool {
	return f.supportsAcceleratorSlice
}

// IsGpuAcceleratorSliceSupported returns whether the machine family supports accelerator slice and supports GPU
func (f MachineFamily) IsGpuAcceleratorSliceSupported() bool {
	return f.supportsAcceleratorSlice && len(f.supportedGpuTypes) > 0
}

// IsConfidentialNodeTypeSupported returns whether the machine family supports a given confidential node type
func (f MachineFamily) IsConfidentialNodeTypeSupported(confidentialNodeType string) bool {
	// Since individual machine types can override the family-level configuration, we need to check them all
	return f.AreConstraintsSupported(Constraints{CpuPlatform: AnyPlatform, ConfidentialNodeType: confidentialNodeType})
}

// DraComputeDomainAutoDetection returns whether Cluster Autoscaler should try to automatically detect
// if ComputeDomain DRA Devices should be predicted, based on the existence of specific DeviceClasses.
// Details: go/dra-nap-computedomains
func (f MachineFamily) DraComputeDomainAutoDetection() bool {
	return f.draComputeDomainAutoDetection
}

// IsResizable returns whether the machine family is resizable.
func (f MachineFamily) IsResizable() bool {
	return f.resizableConfig != nil
}

// ResizableConfig returns the resizable config for the machine family.
func (f MachineFamily) ResizableConfig() *ResizableMachineFamilyConfig {
	return f.resizableConfig
}

// ThreadsPerCore returns the threads per core for the machine family
func (f MachineFamily) threadsPerCore() int64 {
	if f.nonDefaultThreadsPerCore == nil {
		return DefaultThreadPerCore
	}
	return *f.nonDefaultThreadsPerCore
}
