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
	"strconv"

	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

const (
	// DefaultGPUSharingStrategy is used as a default GPU sharing strategy in case if
	// Max Shared clients label is specified, but sharing strategy isn't.
	DefaultGPUSharingStrategy = labels.GPUTimeSharingStrategy

	// DefaultGPUMaxSharedClients is used as a default GPU Max Shared Clients value in case
	// GPU sharing strategy is defined, but max shared clients isn't.
	DefaultGPUMaxSharedClients = "2"

	// DeprecatedDefaultGPU was used as most preferable GPU type to be used by NAP if user did not
	// specify exact GPU type in pod requests. Other GPU type may still be used if there
	// is no max limit defined for DeprecatedDefaultGPU.
	DeprecatedDefaultGPU = gpu.DefaultGPUType
)

// PhysicalGpuCount is a number of physical GPUs attached to a single machine, e.g., for a2-ultragpu-4g it's 4.
type PhysicalGpuCount int64

// AllocatableGpuCount is a number of allocatable GPU units on a machine.
// For machines without sharing strategy and partitioning it's equal to PhysicalGpuCount
// In other cases it's multiply of it, e.g.,
// a2-ultragpu-4g (4 GPUs) with partitioning "2g.20gb" (3 partitions) and MPS strategy with maxSharedClients = 5
// it's 4 * 3 * 5 = 60 Allocatable GPU units
type AllocatableGpuCount int64

func PhysicalToAllocatable(c PhysicalGpuCount, partitionSize int64, maxSharedClients int64) AllocatableGpuCount {
	return AllocatableGpuCount(int64(c) * partitionSize * maxSharedClients)
}

// Gpu contains information about gpus
type Gpu struct {
	name string
	// isNapSupported defines if the gpu is supported by NAP
	isNapSupported bool
	// maxCpuCount defines the maximum count of cpus allowed for given number of GPUs
	// on a single machine
	maxCpuCount map[PhysicalGpuCount]int
	// partitionSizes - optional - defines partition sizes for a given gpu type.
	// some gpus offer partial amount of gpus to be used. This encapsulates the
	// number of partitions based on the partition spec
	partitionSizes map[string]int64
}

// DraAcceleratorInfo holds a per GPU type data need for DRA device prediction
// this doesn't neceserrily represent information reusable in other places
type DraAcceleratorInfo struct {
	Brand        string
	Model        string
	CapacityGB   int64
	Architecture string
}

func (g Gpu) Name() string {
	return g.name
}

func (g Gpu) PartitionSizes() (map[string]int64, bool) {
	if g.partitionSizes == nil {
		return nil, false
	}
	return g.partitionSizes, true
}

func (g Gpu) MaxCpuCount() map[PhysicalGpuCount]int {
	return g.maxCpuCount
}

// GetPartitionCount returns number of partitions for the given partition size.
func (g Gpu) GetPartitionCount(partitionSize string) (int64, errors.AutoscalerError) {
	if partitionSize == "" {
		return 1, nil
	}
	p, found := g.PartitionSizes()
	if !found {
		return 1, errors.NewAutoscalerErrorf(errors.CloudProviderError, "GPU %s does not support partitioning", g.Name())
	}
	s, found := p[partitionSize]
	if !found {
		return 1, errors.NewAutoscalerErrorf(errors.CloudProviderError, "GPU %s does not support %s partition size", g.Name(), partitionSize)
	}
	return s, nil
}

// GetMaxAllocatableGpuCount returns maximum count of GPUs that can be present in single machine.
func (g Gpu) GetMaxAllocatableGpuCount(partitionSize, maxSharedClients string) (AllocatableGpuCount, error) {
	maxCpuInfo := g.MaxCpuCount()
	var maxGpuCountForType PhysicalGpuCount
	for gpuCount := range maxCpuInfo {
		if gpuCount > maxGpuCountForType {
			maxGpuCountForType = gpuCount
		}
	}
	maxSharedClientsInt, err := GetMaxGpuSharedClients(maxSharedClients)
	if err != nil {
		return 0, err
	}
	partitionCount, err := g.GetPartitionCount(partitionSize)
	return PhysicalToAllocatable(maxGpuCountForType, partitionCount, maxSharedClientsInt), err
}

// GetNormalizedGpuCount bumps up GPU count to binary round values.
func (g Gpu) GetNormalizedGpuCount(gpuPartitionSize, gpuMaxSharedClients string, initialGpuCount AllocatableGpuCount, cpus, mem int64) (AllocatableGpuCount, error) {
	maxCpuInfo := g.MaxCpuCount()
	var gpuCounts []PhysicalGpuCount
	for k := range maxCpuInfo {
		gpuCounts = append(gpuCounts, k)
	}
	sort.Slice(gpuCounts, func(i, j int) bool { return gpuCounts[i] < gpuCounts[j] })

	partitionCount, err := g.GetPartitionCount(gpuPartitionSize)
	if err != nil {
		return 0, err
	}
	maxSharedClientsInt, err := GetMaxGpuSharedClients(gpuMaxSharedClients)
	if err != nil {
		return 0, err
	}

	for _, physicalGpuCount := range gpuCounts {
		allocatableGpuCount := PhysicalToAllocatable(physicalGpuCount, partitionCount, maxSharedClientsInt)
		if allocatableGpuCount < initialGpuCount {
			continue
		}
		if cpus <= int64(maxCpuInfo[physicalGpuCount]) {
			return allocatableGpuCount, nil
		}
	}
	return 0, errors.NewAutoscalerErrorf(errors.CloudProviderError,
		"invalid combination of number of CPUs: %d and number of allocatable "+
			"GPUs: %d of gpuType: %s. partitionCount=%v, maxSharedClients=%v",
		cpus, initialGpuCount, g.Name(), partitionCount, maxSharedClientsInt)
}

// GetConfigsGreaterOrEqual returns all the available GPU configs equal and above the required number for the specified type of GPU.
// The returned GPU counts are guaranteed to be sorted in ascending order.
func (g Gpu) GetConfigsGreaterOrEqual(partitionSize, maxSharedClients string, minNumberOfGpus AllocatableGpuCount) ([]AllocatableGpuCount, int64, int64, error) {
	partitionCount, err := g.GetPartitionCount(partitionSize)
	if err != nil {
		return nil, 1, 1, err
	}
	maxSharedClientsInt, err := GetMaxGpuSharedClients(maxSharedClients)
	if err != nil {
		return nil, 1, 1, err
	}

	var availableGpuConfigs []AllocatableGpuCount
	maxCpuInfo := g.MaxCpuCount()
	for physicalGpuCount := range maxCpuInfo {
		gpuComputeUnits := PhysicalToAllocatable(physicalGpuCount, partitionCount, maxSharedClientsInt)
		if gpuComputeUnits >= minNumberOfGpus {
			availableGpuConfigs = append(availableGpuConfigs, gpuComputeUnits)
		}
	}
	sort.Slice(availableGpuConfigs, func(i, j int) bool { return availableGpuConfigs[i] < availableGpuConfigs[j] })
	return availableGpuConfigs, partitionCount, maxSharedClientsInt, nil
}

// PhysicalToAllocatable converts physical GPU count to allocatable GPU count.
func (g Gpu) PhysicalToAllocatable(c PhysicalGpuCount, partitionSize, maxSharedClients string) (AllocatableGpuCount, error) {
	partitionCount, err := g.GetPartitionCount(partitionSize)
	if err != nil {
		return 0, err
	}
	maxSharedClientsInt, err := GetMaxGpuSharedClients(maxSharedClients)
	if err != nil {
		return 0, err
	}
	return PhysicalToAllocatable(c, partitionCount, maxSharedClientsInt), nil
}

// TODO(b/245288810): Migrate autoprovisioning.GpuSpec and autoprovisioning.GpuRequest here.

// GetMaxGpuSharedClients parses and returns max gpu shared clients.
func GetMaxGpuSharedClients(gpuMaxSharedClients string) (int64, errors.AutoscalerError) {
	if gpuMaxSharedClients == "" {
		return 1, nil
	}
	gpuMaxSharedClientsInt, err := strconv.Atoi(gpuMaxSharedClients)
	if err != nil {
		return 1, errors.NewAutoscalerError(errors.InternalError, "GPU max shared clients should be integer")
	}
	if gpuMaxSharedClientsInt < 2 || gpuMaxSharedClientsInt > 48 {
		return 1, errors.NewAutoscalerError(errors.InternalError, "GPU max shared clients should be integer between 2 and 48.")
	}
	return int64(gpuMaxSharedClientsInt), nil
}

// ValidateGpuSharingStrategy validates whether provided GPU sharing strategy is correct.
func ValidateGpuSharingStrategy(sharingStrategy string) errors.AutoscalerError {
	if sharingStrategy != "" {
		if sharingStrategy != labels.GPUTimeSharingStrategy && sharingStrategy != labels.GPUMpsStrategy {
			return errors.NewAutoscalerErrorf(errors.CloudProviderError, "Unsupported GPU sharing strategy: %s", sharingStrategy)
		}
	}
	return nil
}

// GpuConfig represents a GPU config.
type GpuConfig struct {
	GpuType          string
	PartitionSize    string
	MaxSharedClients string
	SharingStrategy  string
	DriverVersion    string
}

// GpuRequest represents a request for a number of GPUs with a given config.
type GpuRequest struct {
	Config           GpuConfig
	Count            AllocatableGpuCount
	PhysicalGPUCount PhysicalGpuCount
}

// String returns a human-readable description of the request.
func (r GpuRequest) String() string {
	if r.Empty() {
		return "no request"
	}
	return r.Signature()
}

// Signature returns a stable string representation of the request.
func (r GpuRequest) Signature() string {
	return fmt.Sprintf("type: %q, partition: %q, count: %d, physicalCount: %v, maxSharedClients: %q, sharingStrategy: %q, driverVersion: %q",
		r.Config.GpuType,
		r.Config.PartitionSize,
		r.Count,
		r.PhysicalGPUCount,
		r.Config.MaxSharedClients,
		r.Config.SharingStrategy,
		r.Config.DriverVersion)
}

// Empty returns true if r isn't an actual GPU request, but e.g. a zero-value for GpuRequest.
func (r GpuRequest) Empty() bool {
	return r.Config.GpuType == ""
}

const AnyGPU = "--ANY_GPU--"
