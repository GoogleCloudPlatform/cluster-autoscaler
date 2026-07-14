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

package gke

import (
	"fmt"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
	klog "k8s.io/klog/v2"
)

// There should be no imports as it is used standalone in e2e tests

const (
	// MiB - MebiByte size (2^20)
	MiB = 1024 * 1024
	// GiB - GigiByte size (2^30)
	GiB = 1024 * MiB

	// Duplicating an upstream bug treating GB as 1000*MiB (we need to predict the end result accurately).
	mbPerGB           = 1000
	millicoresPerCore = 1000

	// GCFS requires memory to cache the layers, hence reserving additional memory for GCFS enabled (Riptide) nodes.
	gcfsReservedFactor = 0.04

	// threshold value of max pods per node node-pool setting, above which kubelet s additional CPU.
	maxPodsPerNodeAllocatableThreshold = 110

	// additional kubelet milli CPU reservation for nodes with max pods per node
	// larger than maxPodsPerNodeAllocatableThreshold value.
	additionalKubeletMilliCPUReservationForHighMaxPodsPerNode = 400
)

var (
	e2FractionalMillicores = map[string]int64{
		"e2-micro":  1000,
		"e2-small":  1000,
		"e2-medium": 1000,
	}

	memoryBrackets = []allocatableBracket{
		{
			threshold:            0,
			marginalReservedRate: 0.25,
		},
		{
			threshold:            4 * mbPerGB,
			marginalReservedRate: 0.2,
		},
		{
			threshold:            8 * mbPerGB,
			marginalReservedRate: 0.1,
		},
		{
			threshold:            16 * mbPerGB,
			marginalReservedRate: 0.06,
		},
		{
			threshold:            128 * mbPerGB,
			marginalReservedRate: 0.02,
		},
	}
)

// GkeReserved implement gce.OsReservedCalculator interface.
type GkeReserved struct {
	gceReserved *gce.GceReserved
	osReserved  *nodetemplate.SystemReserved
}

func NewGkeReserved(content []byte) (*GkeReserved, error) {
	osReserved, err := nodetemplate.BuildSystemReservedConfig(content)
	if err != nil {
		return nil, fmt.Errorf("haven't loaded os-reserved file: %v", err)
	}
	return &GkeReserved{osReserved: osReserved, gceReserved: &gce.GceReserved{}}, nil
}

func NewGkeReservedForTesting() *GkeReserved {
	return &GkeReserved{gceReserved: &gce.GceReserved{}}
}

// CalculateKernelReserved computes how much memory Linux kernel will reserve.
func (r *GkeReserved) CalculateKernelReserved(m gce.MigOsInfo, physicalMemory int64) int64 {
	gkeMigOsInfo, ok := m.(*GkeMigOsInfo)
	if ok && r.osReserved != nil {
		val, err := r.osReserved.GetOsReservedValue(nodetemplate.MemoryResource, gkeMigOsInfo.NodeVersion(), gkeMigOsInfo.OsDistribution(), gkeMigOsInfo.Arch(), gkeMigOsInfo.ConfidentialNode(), physicalMemory)
		if err != nil {
			klog.Warningf("Could not get OS reserved memory from config; fallback to kernel reserved calculation. Error: %v", err)
			return r.gceReserved.CalculateKernelReserved(m, physicalMemory)
		}
		return val
	}
	return r.gceReserved.CalculateKernelReserved(m, physicalMemory)
}

// CalculateOSReservedEphemeralStorage estimates how much ephemeral storage OS will reserve and eviction threshold.
func (r *GkeReserved) CalculateOSReservedEphemeralStorage(m gce.MigOsInfo, diskSize int64) int64 {
	gkeMigOsInfo, ok := m.(*GkeMigOsInfo)
	if ok && r.osReserved != nil {
		val, err := r.osReserved.GetOsReservedValue(nodetemplate.EphemeralStorageResource, gkeMigOsInfo.NodeVersion(), gkeMigOsInfo.OsDistribution(), gkeMigOsInfo.Arch(), false, diskSize)
		if err != nil {
			klog.Warningf("Could not get OS reserved ephemeral storage from config; fallback to kernel reserved calculation. Error: %v", err)
			return r.gceReserved.CalculateKernelReserved(m, diskSize)
		}
		return val
	}
	return r.gceReserved.CalculateOSReservedEphemeralStorage(m, diskSize)
}

// CalculateOSPhysicalEphemeralStorageGiB find minimum Physical disk size that accommodate Allocatable
// Base formula: Allocatable + kube-reserved + system-reserved + eviction-threshold <= Physical
// Read more: go/gke-ca-node-prediction
func (r *GkeReserved) CalculatePhysicalEphemeralStorageGiB(m gce.MigOsInfo, allocatableBytes int64) int64 {
	var minPhysicalGiB int64 = machinetypes.MinGceBootDiskSizeGb
	var maxPhysicalGiB int64 = machinetypes.MaxBootDiskSizeNonSharedCoreMachinesGb
	return binarySearchFirstTrue(minPhysicalGiB, maxPhysicalGiB, func(physicalGiB int64) bool {
		return r.storageLeftForAllocatable(m, physicalGiB) >= allocatableBytes
	})
}

func (r *GkeReserved) storageLeftForAllocatable(m gce.MigOsInfo, physicalGiB int64) int64 {
	physical := physicalGiB * GiB
	osReserved := r.CalculateOSReservedEphemeralStorage(m, physical)
	kubeReserved := PredictKubeReservedEphemeralStorage(physicalGiB)
	// TODO(b/325896400): Share the logic with cloudprovider/gce/templates.go:CalculateAllocatable
	// evictionHard is nil because:
	// 1) GKE doesn't specify custom kubelet --eviction-treashold
	// 2) calculator is used for new NAP instances and KUBE_ENV is not defined yet
	capacity := physical - osReserved
	evictionThreshold := int64(gce.GetKubeletEvictionHardForEphemeralStorage(capacity, nil /* evictionHard */))

	// storageLeft = physical - used
	return physical - (osReserved + kubeReserved + evictionThreshold)
}

// binarySearchFirstTrue returns the smallest x where f(x) = true && low <= x <= high.
// Returns "high" if all x in the range evaluate to false.
func binarySearchFirstTrue(low, high int64, f func(int64) bool) int64 {
	result := high
	for low <= high {
		mid := low + (high-low)/2
		if f(mid) {
			result = mid
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	return result
}

// PredictKubeReservedMemory calculates kube-reserved memory based on physical memory.
func PredictKubeReservedMemory(physicalMemory int64, gcfsEnabled bool) int64 {
	return memoryReservedMiB(physicalMemory/MiB, gcfsEnabled) * MiB
}

// PredictKubeReservedCpuMillicores calculates kube-reserved cpu based on physical cpu.
func PredictKubeReservedCpuMillicores(physicalCpuMillicores int64, machineType string, maxPodsPerNode int64) int64 {
	return cpuReservedMillicores(physicalCpuMillicores, machineType, maxPodsPerNode)
}

// PredictKubeReservedEphemeralStorage calculates kube-reserved ephemeral storage based on physical ephemeral storage.
func PredictKubeReservedEphemeralStorage(diskSizeGb int64) int64 {
	halfSize := int64(0.5 * float64(diskSizeGb))
	basicSize := 6 + int64(0.35*float64(diskSizeGb))
	var capSize int64 = 100
	if basicSize > halfSize {
		return halfSize * GiB
	} else if basicSize > capSize {
		return capSize * GiB
	}
	return basicSize * GiB
}

// PredictEphemeralLocalSsdKubeReservedEphemeralStorage calculates kube-reserved ephemeral storage when Ephemeral Local SSD is used.
func PredictEphemeralLocalSsdKubeReservedEphemeralStorage(localSsdCount int64) int64 {
	// See c/k/distro/legacy/kube_env.go:ephemeralStorageReservedGB for the source of this calculation.
	switch localSsdCount {
	case 1:
		return 50 * GiB
	case 2:
		return 75 * GiB
	default:
		return 100 * GiB
	}
}

type allocatableBracket struct {
	threshold            int64
	marginalReservedRate float64
}

func memoryReservedMiB(memoryCapacityMiB int64, gcfsEnabled bool) int64 {
	if memoryCapacityMiB <= mbPerGB {
		if memoryCapacityMiB <= 0 {
			return 0
		}
		// The minimum reservation required for proper node operation is 255 MiB.
		// For any node with less than 1 GB of memory use the minimum. Nodes with
		// more memory will use the existing reservation thresholds.
		return 255
	}
	if gcfsEnabled {
		return calculateReserved(memoryCapacityMiB, memoryBrackets) + memoryReservedForGCFSMiB(memoryCapacityMiB)
	}
	return calculateReserved(memoryCapacityMiB, memoryBrackets)
}

func memoryReservedForGCFSMiB(memoryCapacityMiB int64) int64 {
	var gcfsMemoryBrackets []allocatableBracket

	for _, b := range memoryBrackets {
		gcfsMemoryBrackets = append(gcfsMemoryBrackets, allocatableBracket{
			threshold:            b.threshold,
			marginalReservedRate: b.marginalReservedRate * gcfsReservedFactor,
		})
	}
	return calculateReserved(memoryCapacityMiB, gcfsMemoryBrackets)
}

func cpuReservedMillicores(cpuCapacityMillicores int64, machineType string, maxPodsPerNode int64) int64 {
	// Reserved millicores for fractional CPU machine types.
	rBurstableMillicores := int64(0)
	if e2millicores, ok := e2FractionalMillicores[machineType]; ok {
		// Calculate reserved overhead based on the "actual"(sustained) CPU.
		cpuCapacityMillicores = e2millicores
		// Reduce CPU for e2 fractional CPU VMs.
		// This is the reserved CPU, alloctable will be calculated as 2000 millis minus reserved.
		// To get to for example 250 alloctable for e2-micro, this variable will be 2000-250=1750,
		// so allocatable will be set to 2000 - 1750 = 250 millicores.
		rBurstableMillicores = int64(2000) - cpuCapacityMillicores
	}
	// if maxPodsPerNode of a given node is larger than 110, extra CPU is being reserved by kubelet.
	maxPodsPerNodeOverhead := int64(0)
	if maxPodsPerNode > maxPodsPerNodeAllocatableThreshold {
		maxPodsPerNodeOverhead = additionalKubeletMilliCPUReservationForHighMaxPodsPerNode
	}
	klog.V(5).Infof("machineType = %s, rBurstableMillicores = %d, cpuCapacityMillicores = %d, maxPodsPerNode = %d", machineType,
		rBurstableMillicores, cpuCapacityMillicores, maxPodsPerNode)
	return calculateReserved(cpuCapacityMillicores, []allocatableBracket{
		{
			threshold:            0,
			marginalReservedRate: 0.06,
		},
		{
			threshold:            1 * millicoresPerCore,
			marginalReservedRate: 0.01,
		},
		{
			threshold:            2 * millicoresPerCore,
			marginalReservedRate: 0.005,
		},
		{
			threshold:            4 * millicoresPerCore,
			marginalReservedRate: 0.0025,
		},
	}) + rBurstableMillicores + maxPodsPerNodeOverhead
}

// calculateReserved calculates reserved using capacity and a series of
// brackets as follows:  the marginalReservedRate applies to all capacity
// greater than the bracket, but less than the next bracket.  For example, if
// the first bracket is threshold: 0, rate:0.1, and the second bracket has
// threshold: 100, rate: 0.4, a capacity of 100 results in a reserved of
// 100*0.1 = 10, but a capacity of 200 results in a reserved of
// 10 + (200-100)*.4 = 50.  Using brackets with marginal rates ensures that as
// capacity increases, reserved always increases, and never decreases.
func calculateReserved(capacity int64, brackets []allocatableBracket) int64 {
	var reserved float64
	for i, bracket := range brackets {
		c := capacity
		if i < len(brackets)-1 && brackets[i+1].threshold < capacity {
			c = brackets[i+1].threshold
		}
		additionalReserved := float64(c-bracket.threshold) * bracket.marginalReservedRate
		if additionalReserved > 0 {
			reserved += additionalReserved
		}
	}
	return int64(reserved)
}
