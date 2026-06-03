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
	"context"
	"fmt"
	"strings"
	"sync"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	resizable_vm_size "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	"k8s.io/klog/v2"
)

// MachineConfigProvider provides machine configuration based on source if available,
// or hard-coded configuration if the source is disabled or nil.
//
// TODO(b/448575257): make it use source
type MachineConfigProvider struct {
	source *Source

	cache *machineConfigurationCache

	// Helper caches, not refreshed on configuration update
	// TODO(b/493869128): figure out if they ever need to be invalidated
	maxResizableVmByMachineTypeCache sync.Map
	minResizableVmByMachineTypeCache sync.Map
}

// NewMachineConfigProvider creates a new MachineConfigProvider from source.
func NewMachineConfigProvider(source *Source) *MachineConfigProvider {
	if source != nil {
		go source.Run(context.Background())
	}
	return &MachineConfigProvider{
		source: source,
		cache:  newMachineConfigurationCache(),
	}
}

// Refresh triggers source refresh, if source is available.
func (p *MachineConfigProvider) Refresh() bool {
	if p == nil || p.source == nil {
		// This is fine, many tests don't need machine config and will set it to nil.
		return false
	}
	p.cache.update(p.source.Snapshot())
	// TODO(b/448575257): compare the new and old snapshot and return true if they're different.
	return false
}

// Generic methods for accessing machine configuration.

// AllMachineFamilies returns all machine families.
func (p *MachineConfigProvider) AllMachineFamilies() []MachineFamily {
	cachedFamilies := p.cache.machineFamilies()
	families := make([]MachineFamily, 0, len(cachedFamilies))
	for _, machineFamily := range cachedFamilies {
		families = append(families, machineFamily)
	}
	return families
}

// AllAutoprovisionedCustomTypes returns a set of all predefined custom machine types suitable for autoprovisioning.
func (p *MachineConfigProvider) AllAutoprovisionedCustomTypes() map[string]bool {
	allMachineFamilies := p.cache.machineFamilies()
	result := make(map[string]bool, len(allMachineFamilies))
	for _, family := range allMachineFamilies {
		for _, machineType := range family.CustomAutoprovisionedMachineTypes(NoConstraints) {
			result[machineType.Name] = true
		}
	}
	return result
}

// AllResizableMachineFamilies returns all resizable machine families.
func (p *MachineConfigProvider) AllResizableMachineFamilies() []MachineFamily {
	families := make([]MachineFamily, 0)
	for _, machineFamily := range p.AllMachineFamilies() {
		if machineFamily.IsResizable() {
			families = append(families, machineFamily)
		}
	}
	return families
}

// GetMachineFamilyFromMachineName fetches the MachineFamily object based on the machineName else returns error
func (p *MachineConfigProvider) GetMachineFamilyFromMachineName(machineName string) (MachineFamily, error) {
	machineFamilyName, err := gce.GetMachineFamily(machineName)
	if err != nil {
		return MachineFamily{}, err
	}
	machineFamily, err := p.ToMachineFamily(machineFamilyName)
	if err != nil {
		return MachineFamily{}, err
	}
	return machineFamily, nil
}

// ToMachineFamily converts a machine family string into a MachineFamily object.
func (p *MachineConfigProvider) ToMachineFamily(machineFamilyName string) (MachineFamily, error) {
	machineFamily, found := p.cache.machineFamilies()[strings.ToLower(machineFamilyName)]
	if !found {
		return MachineFamily{}, fmt.Errorf("unsupported machine family %q", machineFamilyName)
	}
	return machineFamily, nil
}

// ToMachineType converts a machine type string into a gce.MachineType object. Works for custom machine types and predefined
// machine types known to Cluster Autoscaler. The returned object should not be used for scheduling purposes, as it doesn't
// consult the API for predefined machine types. Can be useful for reasoning about machine types based on their rough amount
// of resources, or in testing.
func (p *MachineConfigProvider) ToMachineType(machineTypeName string) (MachineType, error) {
	for _, family := range p.cache.machineFamilies() {
		if machineType, found := family.AllMachineTypes(NoConstraints)[machineTypeName]; found {
			return machineType, nil
		}
	}
	return ToCustomMachineType(machineTypeName)
}

// Specialized methods for determining particular properties based on machine configuration.

// AllowedEphemeralLocalSsdCountByMachineType returns the allowed values for the local SSD count.
func (p *MachineConfigProvider) AllowedEphemeralLocalSsdCountByMachineType(machineType string) (allowedCounts []int, found bool, err error) {
	machineFamily, err := p.GetMachineFamilyFromMachineName(machineType)
	if err != nil {
		return nil, false, err
	}
	if machine, found := machineFamily.AllMachineTypes(NoConstraints)[machineType]; found {
		allowedCounts, found = machine.AllowedEphemeralLocalSsdCounts()
		return allowedCounts, found, nil
	}
	return nil, false, nil
}

// AutomaticEphemeralLocalSsdCountByMachineType returns the number local SSD cards that should be automatically used for ephemeral storage.
func (p *MachineConfigProvider) AutomaticEphemeralLocalSsdCountByMachineType(machineType string) (count int64, found bool, err error) {
	machineFamily, err := p.GetMachineFamilyFromMachineName(machineType)
	if err != nil {
		return 0, false, err
	}
	if machine, found := machineFamily.AllMachineTypes(NoConstraints)[machineType]; found {
		count, found = machine.AutomaticEphemeralLocalSsdCount()
		return count, found, nil
	}
	return 0, false, nil
}

// LocalSSDDiskSizes returns a map of machine families *and* machine types with
// overrides for the default local SSD disk size (375 GB).
func (p *MachineConfigProvider) LocalSSDDiskSizes() map[string]uint64 {
	sizes := map[string]uint64{}

	// Data from MachineConfig CRD will be propagated to the MachineType object.
	for _, family := range p.AllMachineFamilies() {
		for mtName, mt := range family.AllMachineTypes(NoConstraints) {
			if size, found := mt.LocalSSDDiskSize(); found {
				sizes[mtName] = size
			}
		}
	}

	// Fallback to hard-coded values.
	for name, size := range LocalSSDDiskSizes {
		if _, found := sizes[name]; !found {
			sizes[name] = size
		}
	}

	return sizes
}

// GetOrDefaultMinCPUPlatform returns the min CPU platform or the default min CPU platform.
// It is used in matching node pool to reservations.
func (p *MachineConfigProvider) GetOrDefaultMinCPUPlatform(machineType string, minCPUPlatformName string) CpuPlatform {
	canonicalMinCPUPlatform, err := ToCpuPlatform(minCPUPlatformName)
	if err != nil {
		canonicalMinCPUPlatform = AnyPlatform
	}

	// Try to deduce min-cpu platform for machine type.
	if canonicalMinCPUPlatform == AnyPlatform {
		// Check min-cpu platform in the Machine Type.
		machineTypeObj, err := p.ToMachineType(machineType)
		if err == nil && machineTypeObj.cpuPlatformRequirements != nil && machineTypeObj.cpuPlatformRequirements.lowerBound != UnknownPlatform {
			return machineTypeObj.cpuPlatformRequirements.lowerBound
		}

		// Check min-cpu platform in the Machine Family.
		machineFamily, err := p.GetMachineFamilyFromMachineName(machineType)
		if err == nil && machineFamily.supportedCpuPlatforms.lowerBound != UnknownPlatform {
			return machineFamily.supportedCpuPlatforms.lowerBound
		}
	}

	return canonicalMinCPUPlatform
}

// GetMaxResizableVmSizeByMachineType returns the maximum resizable VM size for the given machine type.
func (p *MachineConfigProvider) GetMaxResizableVmSizeByMachineType(machineType string) (resizable_vm_size.VmSize, error) {
	if v, ok := p.maxResizableVmByMachineTypeCache.Load(machineType); ok {
		return v.(resizable_vm_size.VmSize), nil
	}
	typeInfo, err := p.getResizableMachineType(machineType)
	if err != nil {
		return resizable_vm_size.VmSize{}, err
	}
	sz := resizable_vm_size.VmSize{
		MilliCpus: typeInfo.CPU * 1000,
		KBytes:    typeInfo.Memory / 1024,
	}
	p.maxResizableVmByMachineTypeCache.Store(machineType, sz)
	return sz, nil
}

// ResizableFamilyNames returns names of all resizable machine families.
func (p *MachineConfigProvider) ResizableFamilyNames() []string {
	resizableFamilies := p.AllResizableMachineFamilies()
	families := make([]string, 0, len(resizableFamilies))
	for _, family := range resizableFamilies {
		families = append(families, family.Name())
	}
	return families
}

// GetMinResizableVmSizeByMachineType returns the minimum resizable VM size for the given machine type.
func (p *MachineConfigProvider) GetMinResizableVmSizeByMachineType(machineType string) (resizable_vm_size.VmSize, error) {
	if v, ok := p.minResizableVmByMachineTypeCache.Load(machineType); ok {
		return v.(resizable_vm_size.VmSize), nil
	}
	typeInfo, err := p.getResizableMachineType(machineType)
	if err != nil {
		return resizable_vm_size.VmSize{}, err
	}
	sz := resizable_vm_size.VmSize{
		MilliCpus: typeInfo.resizableConfig.MinMilliCPU(),
		KBytes:    typeInfo.resizableConfig.MinSizeKb(),
	}
	p.minResizableVmByMachineTypeCache.Store(machineType, sz)
	return sz, nil
}

// IsNotInDWS returns whether the machine type is not supported by DWS
func (p *MachineConfigProvider) IsNotInDWS(machineType string) (bool, error) {
	mt, err := p.ToMachineType(machineType)
	if err != nil {
		return false, err
	}
	return mt.notInDWS, nil
}

// Specialized GPU methods.

// FixedGPUTypeAndCountForMachineType returns the fixed GPU type and count on the selected machine type if it exists.
func (p *MachineConfigProvider) FixedGPUTypeAndCountForMachineType(machineTypeName string) (gpuType string, count PhysicalGpuCount, found bool) {
	machineType, err := p.ToMachineType(machineTypeName)
	if err != nil {
		return "", 0, false
	}
	gpu, found := machineType.gpuOverride()
	if !found {
		return "", 0, false
	}
	return gpu.gpu.Name(), gpu.fixedGpuCount, true
}

// GetAllGpuTypes returns all supported GPU types.
func (p *MachineConfigProvider) GetAllGpuTypes() map[string]Gpu {
	gpus := make(map[string]Gpu, len(allGpusByName))
	for name, gpu := range allGpusByName {
		gpus[name] = gpu
	}
	return gpus
}

// GetAvailableGpuTypes returns all GPU types available for autoprovisioning (with max limit set and NAP support).
func (p *MachineConfigProvider) GetAvailableGpuTypes(limiter *cloudprovider.ResourceLimiter) []string {
	gpusWithLimitSet := make([]string, 0)
	for _, resource := range limiter.GetResources() {
		if cloudprovider.IsCustomResource(resource) && limiter.HasMaxLimitSet(resource) {
			if p.IsGpuNapSupported(resource) {
				gpusWithLimitSet = append(gpusWithLimitSet, resource)
			} else {
				klog.Warningf("NAP: GPU type %q has limit set, but isn't supported by NAP - ignoring", resource)
			}
		}
	}
	return gpusWithLimitSet
}

// GetDraAcceleratorInfo returns the information about the GPU accelerator
// applicable in DRA context
func (p *MachineConfigProvider) GetDraAcceleratorInfo(gpuName string) (DraAcceleratorInfo, bool) {
	info, exists := draAcceleratorInfos[gpuName]
	return info, exists
}

// GetGpuConfigsGreaterOrEqual returns all the available GPU configs equal and above the required number for the specified type of GPU.
// The returned GPU counts are guaranteed to be sorted in ascending order.
func (p *MachineConfigProvider) GetGpuConfigsGreaterOrEqual(partitionSize, maxSharedClients, gpuName string, minNumberOfGpus AllocatableGpuCount) ([]AllocatableGpuCount, int64, int64, error) {
	gpuType, found := p.ToGpuType(gpuName)
	if !found {
		return nil, 1, 1, errors.NewAutoscalerErrorf(errors.InternalError, "missing config for gpu type: %q", gpuName)
	}
	return gpuType.GetConfigsGreaterOrEqual(partitionSize, maxSharedClients, minNumberOfGpus)
}

// GetMaxAllocatableGpuCount returns maximum count of GPUs of given type that can be present in single machine. Returns error if
// queried for unknown GPU type.
func (p *MachineConfigProvider) GetMaxAllocatableGpuCount(gpuName, gpuPartitionSize, gpuMaxSharedClients string) (AllocatableGpuCount, error) {
	gpuType, found := p.ToGpuType(gpuName)
	if !found {
		return 0, errors.NewAutoscalerErrorf(errors.InternalError, "missing config for gpu type: %q", gpuName)
	}
	return gpuType.GetMaxAllocatableGpuCount(gpuPartitionSize, gpuMaxSharedClients)
}

// IsGpuNapSupported returns true if the given GPU is supported for autoprovisioning.
func (p *MachineConfigProvider) IsGpuNapSupported(gpuName string) bool {
	if g, found := allGpusByName[gpuName]; found {
		return g.isNapSupported
	}
	return false
}

// IsGpuTypeAndCountSupportedByMachineType returns whether the provided machine type is compatible with the provided GPU type and count.
func (p *MachineConfigProvider) IsGpuTypeAndCountSupportedByMachineType(machineType, gpuType string, gpuCount PhysicalGpuCount) (bool, error) {
	machineFamily, err := p.GetMachineFamilyFromMachineName(machineType)
	if err != nil {
		return false, err
	}

	var machineCpuCount int64
	if machine, found := machineFamily.AllMachineTypes(NoConstraints)[machineType]; found {
		// Check gpu override for predefined machine type.
		override, found := machine.gpuOverride()
		if found && override.gpu.Name() == gpuType && override.fixedGpuCount == gpuCount {
			return true, nil
		}
		machineCpuCount = machine.CPU
	} else {
		// Unknown custom machine type case.
		customMachineTypeInfo, err := gce.NewCustomMachineType(machineType)
		if err != nil {
			return false, err
		}
		machineCpuCount = customMachineTypeInfo.CPU
	}

	// check machine family.
	if gpu, found := machineFamily.supportedGpuTypes[gpuType]; found && int(machineCpuCount) <= gpu.maxCpuCount[gpuCount] {
		return true, nil
	}
	return false, nil
}

// MachineFamilyForGpuType returns the machine family required for the specified GPU if found.
func (p *MachineConfigProvider) MachineFamilyForGpuType(gpuName string) (MachineFamily, bool) {
	family, ok := machineFamilyForGPU[gpuName]
	return family, ok
}

// PhysicalToAllocatableWithGpuName converts physical GPU count to allocatable GPU count.
// The gpuName is required because the partition sizes are specific to each GPU type.
func (p *MachineConfigProvider) PhysicalToAllocatableWithGpuName(c PhysicalGpuCount, gpuName, partitionSize, maxSharedClients string) (AllocatableGpuCount, error) {
	gpuType, found := p.ToGpuType(gpuName)
	if !found {
		return 0, fmt.Errorf("unable to find a gpu type definition for gpuName=%v", gpuName)
	}
	return gpuType.PhysicalToAllocatable(c, partitionSize, maxSharedClients)
}

// ToGpuType returns GPU by name.
func (p *MachineConfigProvider) ToGpuType(gpuName string) (Gpu, bool) {
	g, found := allGpusByName[gpuName]
	return g, found
}

// ToPhysicalGPUCount returns an actual number of physical GPUs for a given GPU partitioning and GPU sharing.
func (p *MachineConfigProvider) ToPhysicalGPUCount(gpuName, gpuPartitionSize, gpuMaxSharedClients string, gpuCount AllocatableGpuCount) (PhysicalGpuCount, errors.AutoscalerError) {
	gpuType, found := p.ToGpuType(gpuName)
	if !found {
		return 0, errors.NewAutoscalerErrorf(errors.InternalError, "missing config for gpu type: %q", gpuName)
	}
	partitionCount, err := gpuType.GetPartitionCount(gpuPartitionSize)
	if err != nil {
		return 0, err
	}
	maxSharedClients, err := GetMaxGpuSharedClients(gpuMaxSharedClients)
	if err != nil {
		return 0, err
	}
	if int64(gpuCount)%(partitionCount*maxSharedClients) != 0 {
		return 0, errors.NewAutoscalerErrorf(errors.InternalError, "GPU count of %d is not supported for %s with '%s' partition size and %d max shared clients - gpuCount should be a multiple of partitionCount (%v) * maxSharedClients (%v)", gpuCount, gpuName, gpuPartitionSize, maxSharedClients, partitionCount, maxSharedClients)
	}
	return PhysicalGpuCount(int64(gpuCount) / maxSharedClients / partitionCount), nil
}

// ValidateGpuForMachineType checks if the gpu configuration is correct
func (p *MachineConfigProvider) ValidateGpuForMachineType(gpuName, gpuPartitionSize, gpuMaxSharedClients, machineType string, allocatableGpuCount AllocatableGpuCount, cpus, _ int64) error {
	gpuType, found := p.ToGpuType(gpuName)
	if !found {
		return errors.NewAutoscalerErrorf(errors.InternalError, "missing config for gpu type: %q", gpuName)
	}
	mf, err := p.GetMachineFamilyFromMachineName(machineType)
	if err != nil {
		return errors.NewAutoscalerErrorf(errors.InternalError, "couldn't determine machine type and GPU compatibility, this shouldn't happen: %v", err)
	}
	if _, found := mf.supportedGpuTypes[gpuName]; !found {
		return errors.NewAutoscalerErrorf(errors.CloudProviderError, "GPU type %q is not compatible with machine type %q", gpuType.Name(), machineType)
	}
	physicalGpuCount, err := p.ToPhysicalGPUCount(gpuType.Name(), gpuPartitionSize, gpuMaxSharedClients, allocatableGpuCount)
	if err != nil {
		return err
	}
	mtGpuName, mtGpuCount, found := p.FixedGPUTypeAndCountForMachineType(machineType)
	if found && (mtGpuName != gpuName || mtGpuCount != physicalGpuCount) {
		return errors.NewAutoscalerErrorf(errors.CloudProviderError,
			"Machine type %v does not support %v of GPU type %v - it supports %v of GPU type %v", machineType, physicalGpuCount, gpuType.Name(), mtGpuCount, mtGpuName)
	}
	maxCpuInfo := gpuType.MaxCpuCount()
	maxCpus, found := maxCpuInfo[physicalGpuCount]
	if !found || cpus > int64(maxCpus) {
		return errors.NewAutoscalerErrorf(errors.CloudProviderError,
			"Too high CPU count of %d for %d of GPU type %s", cpus, physicalGpuCount, gpuType.Name())
	}
	return nil
}

// NumNodesFromTopology returns the number of nodes that the given topology of machine type results in.
// If the topology string is malformed or the number of chips specified by topology is invalid it
// returns 0 along with an error.
func (p *MachineConfigProvider) NumNodesFromTopology(machineType, topology string) (int64, error) {
	topologyChips, err := p.NumChipsFromTopology(topology)
	if err != nil {
		return 0, err
	}

	chipsPerNode, err := p.ChipsPerNode(machineType)
	if err != nil {
		return 0, err
	}
	if chipsPerNode == 0 {
		return 0, fmt.Errorf("received invalid chipsPerNode==0 for machine type %q", machineType)
	}

	if topologyChips%chipsPerNode != 0 {
		return 0, fmt.Errorf("number of chips specified by topology %q is not divisible by %d", topology, chipsPerNode)
	}

	return topologyChips / chipsPerNode, nil
}

// ChipsPerNode returns the number of chips (TPU or GPU) for a given machine type.
func (p *MachineConfigProvider) ChipsPerNode(machineType string) (int64, error) {
	tpuChipsPerNode, err := p.GetTpuCountForMachineType(machineType)
	if err == nil {
		return tpuChipsPerNode, nil
	}

	_, gpuChipsPerNode, gpuFound := p.FixedGPUTypeAndCountForMachineType(machineType)
	if gpuFound {
		return int64(gpuChipsPerNode), nil
	}

	return 0, fmt.Errorf("chips per node value is not defined for machine type %q", machineType)
}

// NumChipsFromTopology returns the number of chips for the given topology.
// If the topology string is malformed or the number of tpu chips specified by topology is invalid it returns 0 along with an error.
func (p *MachineConfigProvider) NumChipsFromTopology(topology string) (int64, error) {
	dimensions, err := parseTopologyString(topology)
	if err != nil {
		return 0, err
	}
	numChips := 1
	for _, dim := range dimensions {
		numChips = numChips * dim
	}
	return int64(numChips), nil
}

// Specialized TPU methods.

// GetTpuCountForMachineType returns the number of TPU chips attached to the given machine type.
func (p *MachineConfigProvider) GetTpuCountForMachineType(machineType string) (int64, error) {
	count, found := fixedTpuCount[machineType]
	if !found {
		return 0, errors.NewAutoscalerErrorf(errors.CloudProviderError, "Can't get TPU count for machine type %v", machineType)
	}
	return count, nil
}

// TpuTypeForMachineFamily returns the single TPU type supported by the given machine family.
func (p *MachineConfigProvider) TpuTypeForMachineFamily(mf string) (string, error) {
	for tpuType, tpuFamily := range machineFamilyForTpuType {
		if mf == tpuFamily.Name() {
			return tpuType, nil
		}
	}
	return "", errors.NewAutoscalerErrorf(errors.CloudProviderError, "Can't get TPU type for machine family %v", mf)
}

// GetMaxTpuCount returns maximum TPU chips for the given TPU type.
func (p *MachineConfigProvider) GetMaxTpuCount(tpuType string) (int64, error) {
	count, found := maxTpuCount[tpuType]
	if !found {
		return 0, errors.NewAutoscalerErrorf(errors.CloudProviderError, "TPU type %v not supported", tpuType)
	}
	return count, nil
}

// IsMultiHostTpuPodslice checks whether the given machine type in given topology represents multi-host vm.
func (p *MachineConfigProvider) IsMultiHostTpuPodslice(tpuType, topology string, tpuCount int64) (bool, error) {
	if tpuType == "" {
		return false, nil
	}
	numTpuChips, err := p.NumChipsFromTopology(topology)
	if err != nil {
		return false, err
	}
	return numTpuChips != tpuCount, nil
}

// IsTPUCountSupported checks whether the provided tpu count is supported.
func (p *MachineConfigProvider) IsTPUCountSupported(tpuType string, tpuCount int64) bool {
	mf, found := machineFamilyForTpuType[tpuType]
	if !found {
		return false
	}
	for mt := range mf.AllMachineTypes(NoConstraints) {
		if fixedTpuCount[mt] == tpuCount {
			return true
		}
	}
	return false
}

// IsTpuNapSupported returns true if the given TPU is supported for autoprovisioning.
func (p *MachineConfigProvider) IsTpuNapSupported(tpuType string) bool {
	_, supported := napSupportedTpuTypes[tpuType]
	return supported
}

// IsTpuSupported returns true if the given TPU is supported.
func (p *MachineConfigProvider) IsTpuSupported(tpuType string) bool {
	for _, tpu := range supportedTpuTypes {
		if tpu == tpuType {
			return true
		}
	}
	return false
}

// GetAllSupportedTpuTypes returns all supported TPU types.
func (p *MachineConfigProvider) GetAllSupportedTpuTypes() []string {
	return supportedTpuTypes
}

// GetSingleHostTopology returns the single host TPU topology for the given machine type.
func (p *MachineConfigProvider) GetSingleHostTopology(machineType string) (string, bool) {
	topology, found := singleHostTopologyMap[machineType]
	return topology, found
}

// MachineFamilyForTpuType returns the machine family required for the specified TPU if found.
func (p *MachineConfigProvider) MachineFamilyForTpuType(tpuType string) (MachineFamily, bool) {
	family, ok := machineFamilyForTpuType[tpuType]
	return family, ok
}

// Utility methods

func (p *MachineConfigProvider) getResizableMachineType(machineType string) (MachineType, error) {
	family, err := p.GetMachineFamilyFromMachineName(machineType)
	if err != nil {
		return MachineType{}, err
	}
	if family.resizableConfig == nil {
		return MachineType{}, fmt.Errorf("machine family %q is not resizable", family.Name())
	}
	typeInfo, found := family.AllMachineTypes(NoConstraints)[machineType]
	if !found {
		return MachineType{}, fmt.Errorf("unknown machine type %q", machineType)
	}
	if typeInfo.resizableConfig == nil {
		return MachineType{}, fmt.Errorf("machine type %q is not resizable", machineType)
	}
	return typeInfo, nil
}
