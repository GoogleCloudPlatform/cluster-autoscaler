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

package calculator

import (
	"fmt"
	"math"
	"strconv"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/utils/klogx"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/vmreservation"

	"k8s.io/klog/v2"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
)

const (
	defaultConfidentialNode = false // defaultConfidentialNode is the default value for confidential node when it is not set.
)

var (
	// We add quota not to spam the logs since not having confidential labels is expected when it is not used.
	confidentialNodeLoggingQuota = klogx.NewLoggingQuota(100)
	zeroBinarySIQuantity         = *resource.NewQuantity(0, resource.BinarySI)
)

type cloudProvider interface {
	GetClusterVersion() string
	MachineConfigProvider() *machinetypes.MachineConfigProvider
}

// Calculator is responsible for calculating corresponding allocatable for a given VM size (and the other way round).
type Calculator interface {
	// ToVmSize returns VM size for a given allocatable size assuming kube and os reserved based on provided node.
	ToVmSize(*apiv1.Node, size.Allocatable) (size.VmSize, error)
	// ToAllocatable returns allocatable for a given VM size assuming kube and os reserved based on provided node.
	ToAllocatable(*apiv1.Node, size.VmSize) size.Allocatable
	// MinAllocatable returns allocatable for maximally downsized VM.
	MinAllocatable(*apiv1.Node) (size.Allocatable, error)
	// RoundUp returns the smallest valid allocatable that is greater or equal than given allocatable in all dimensions.
	RoundUp(*apiv1.Node, size.Allocatable) (size.Allocatable, error)
	MakeVmSizeValid(*apiv1.Node, size.VmSize) (size.VmSize, error)
	GetMaxResizableVmSizeByMachineType(string) (size.VmSize, error)
}

type calculator struct {
	reservation        vmreservation.VmReservation
	provider           cloudProvider
	isClusterUsingDPV1 bool
	limitProvider      LimitProvider
}

func New(reservation vmreservation.VmReservation, provider cloudProvider, isClusterUsingDPV1 bool, limitProvider LimitProvider) Calculator {
	return &calculator{
		reservation:        reservation,
		provider:           provider,
		isClusterUsingDPV1: isClusterUsingDPV1,
		limitProvider:      limitProvider,
	}
}

// ToVmSize returns VM size for a given allocatable size assuming kube and os reserved based on provided node.
func (c *calculator) ToVmSize(node *apiv1.Node, s size.Allocatable) (size.VmSize, error) {
	lmts, err := c.limitProvider.GetLimits(node)
	if err != nil {
		return size.VmSize{}, err
	}
	maxVmSize, err := getMaxVmSize(c.provider, node)
	if err != nil {
		return size.VmSize{}, err
	}
	// We have to use increments of MiB in the binary search to maintain its monotonicity (as there is rounding to MiB in the allocatable calculations).
	memMiBLow := size.RoundUpToIncrement(s.KBytes, size.KiB) / size.MiBToKiB
	memMiBHigh := int64(maxVmSize.KBytes / size.MiBToKiB)
	memKiB := binarySearchFirstTrue(memMiBLow, memMiBHigh, func(sizeInMiB int64) bool {
		return c.toAllocatableMemory(node, sizeInMiB*size.MiBToKiB, lmts) >= s.KBytes
	}) * size.MiBToKiB
	cpu := binarySearchFirstTrue(s.MilliCpus, maxVmSize.MilliCpus, func(milliCores int64) bool {
		return c.toAllocatableCpu(node, milliCores, getMaxPodsPerNode(node), lmts) >= s.MilliCpus
	})
	return c.MakeVmSizeValid(node, size.VmSize{KBytes: memKiB, MilliCpus: cpu})
}

// ToAllocatable returns allocatable for a given VM size assuming kube and os reserved based on provided node.
func (c *calculator) ToAllocatable(node *apiv1.Node, s size.VmSize) size.Allocatable {
	lmts, err := c.limitProvider.GetLimits(node)
	if err != nil {
		klog.Warningf("Failed to get limits for node %q: %v", node.Name, err)
	}
	return size.Allocatable{KBytes: c.toAllocatableMemory(node, s.KBytes, lmts), MilliCpus: c.toAllocatableCpu(node, s.MilliCpus, getMaxPodsPerNode(node), lmts)}
}

// MinAllocatable returns allocatable for maximally downsized VM.
func (c *calculator) MinAllocatable(node *apiv1.Node) (size.Allocatable, error) {
	minVmSize, err := c.MakeVmSizeValid(node, size.VmSize{})
	if err != nil {
		return size.Allocatable{}, err
	}
	return c.ToAllocatable(node, minVmSize), nil
}

// RoundUp returns the smallest valid allocatable that is greater or equal than given allocatable in all dimensions.
func (c *calculator) RoundUp(node *apiv1.Node, allocatable size.Allocatable) (size.Allocatable, error) {
	vmSize, err := c.ToVmSize(node, allocatable)
	if err != nil {
		return size.Allocatable{}, err
	}
	return c.ToAllocatable(node, vmSize), nil
}

// GetMaxResizableVmSizeByMachineType returns the maximum size for the given resizable machine type.
func (c *calculator) GetMaxResizableVmSizeByMachineType(machineType string) (size.VmSize, error) {
	return c.provider.MachineConfigProvider().GetMaxResizableVmSizeByMachineType(machineType)
}

func (c *calculator) toAllocatableCpu(node *apiv1.Node, cpuMillicores int64, maxPodsPerNode int64, lmts limits) int64 {
	capacity := cpuMillicores
	kubeReserved := c.calculateKubeReservedForCpuMillicores(node, cpuMillicores, maxPodsPerNode)

	allocatable := (&gce.GceTemplateBuilder{}).CalculateAllocatable(
		apiv1.ResourceList{apiv1.ResourceCPU: *resource.NewMilliQuantity(capacity, resource.BinarySI)},
		apiv1.ResourceList{apiv1.ResourceCPU: *resource.NewMilliQuantity(kubeReserved, resource.BinarySI)},
		nil,
	)[apiv1.ResourceCPU]

	// Apply SafetyBuffer.
	if lmts.safetyBuffer.Cpu() != nil {
		allocatable.Sub(*lmts.safetyBuffer.Cpu())
	}
	if allocatable.Sign() < 0 {
		return 0
	}

	return allocatable.MilliValue()
}

func (c *calculator) toAllocatableMemory(node *apiv1.Node, sizeInKb int64, lmts limits) int64 {
	capacity := sizeInKb*size.KiB - c.calculateKernelReserved(node)
	kubeReserved := c.calculateKubeReservedForMemory(node, sizeInKb)

	allocatable := (&gce.GceTemplateBuilder{}).CalculateAllocatable(
		apiv1.ResourceList{apiv1.ResourceMemory: *resource.NewQuantity(capacity, resource.BinarySI)},
		apiv1.ResourceList{apiv1.ResourceMemory: *resource.NewQuantity(kubeReserved, resource.BinarySI)},
		nil,
	)[apiv1.ResourceMemory]

	// Apply SafetyBuffer.
	if lmts.safetyBuffer.Memory() != nil {
		allocatable.Sub(*lmts.safetyBuffer.Memory())
	}
	allocatable.Sub(c.kubeProxyMemKiBOverhead(node, lmts))

	// For every huge page reservation, we need to remove it from allocatable memory:
	// https://github.com/kubernetes/kubernetes/blob/84cacae7046df93c1f6f8ea97c912d948e1ad06a/pkg/kubelet/nodestatus/setters.go#L323
	for key, value := range node.Status.Capacity {
		if v1helper.IsHugePageResourceName(key) {
			allocatable.Sub(value)
		}
	}

	if allocatable.Sign() < 0 {
		return 0
	}

	return allocatable.Value() / size.KiB
}

func (c *calculator) calculateKernelReserved(node *apiv1.Node) int64 {
	// TODO(b/277200127): Until this bug is fixed, use the max VM size.
	maxVmSize, err := getMaxVmSize(c.provider, node)
	if err != nil {
		// This is a safe fallback. If the max VM size cannot be determined,
		// assume 0 for kernel reserved memory to prevent cascading errors
		// and allow the allocatable calculation to proceed.
		klog.Warningf("Failed to get max VM size for node %q, assuming 0 for kernel reserved. Error: %v", node.Name, err)
		return 0
	}
	maxPhysicalMemoryInKb := maxVmSize.KBytes
	return c.reservation.CalculateKernelReserved(getGkeMigOsInfo(c.provider, node), maxPhysicalMemoryInKb*size.KiB)
}

func (c *calculator) calculateKubeReservedForMemory(_ *apiv1.Node, physicalMemoryInKb int64) int64 {
	// TODO(b/326220707): GCFS is always true for autopilot, however this will reserve slightly more memory if EKVM ship to standard cluster and GCFS is disabled.
	gcfsEnabled := true
	return c.reservation.PredictKubeReservedMemory(physicalMemoryInKb*size.KiB, gcfsEnabled)
}

func (c *calculator) calculateKubeReservedForCpuMillicores(node *apiv1.Node, physicalCpuMillicores, maxPodsPerNode int64) int64 {
	machineType, err := getMachineType(node)
	if err != nil {
		// This is a safe fallback. An unknown machineType causes the reservation
		// calculation to skip special handling for e2 fractional-CPU machines
		// and use the default tiered reservation, which is correct for all other
		// machine families.
		klog.Warningf("Failed to get machine type for node %q, proceeding with an unknown machine type. Error: %v", node.Name, err)
		machineType = "unknown"
	}
	return c.reservation.PredictKubeReservedCpuMillicores(physicalCpuMillicores, machineType, maxPodsPerNode)
}

// MakeVmSizeValid increases the given VM size as necessary to match requirements
// of the resize API of EK VMs except maximum VM size.
func (c *calculator) MakeVmSizeValid(node *apiv1.Node, sz size.VmSize) (size.VmSize, error) {
	lmts, err := c.limitProvider.GetLimits(node)
	if err != nil {
		return size.VmSize{}, err
	}
	// Apply minimums.
	sz.KBytes, err = c.minMemKiBLimit(node, sz, lmts)
	if err != nil {
		return size.VmSize{}, err
	}
	sz.MilliCpus = c.minMilliCpuLimit(sz, lmts)
	// Round up to supported precision.
	sz.KBytes = size.RoundUpToIncrement(sz.KBytes, lmts.incrementStep.KBytes)
	sz.MilliCpus = size.RoundUpToIncrement(sz.MilliCpus, lmts.incrementStep.MilliCpus)
	// If ratio between CPU and memory is skewed too much adjust one of the resources up.
	sz.KBytes = max(sz.KBytes, c.minMemKiBForMilliCpu(sz.MilliCpus, lmts))
	sz.MilliCpus = max(sz.MilliCpus, c.minMilliCpuForMemKiB(sz.KBytes, lmts))
	return sz, nil
}

// minMemKiBLimit makes sure that memory satisfy all minimum requirements.
func (c *calculator) minMemKiBLimit(node *apiv1.Node, size size.VmSize, lmts limits) (int64, error) {
	minVmMemoryKiB, err := getMinVmMemoryKiB(c.provider, node)
	if err != nil {
		return 0, err
	}
	return max(size.KBytes, lmts.minVmSize.KBytes, minVmMemoryKiB), nil
}

// minMilliCpuLimit makes sure that cpu satisfy all minimum requirements.
func (c *calculator) minMilliCpuLimit(size size.VmSize, lmts limits) int64 {
	return max(size.MilliCpus, lmts.minVmSize.MilliCpus)
}

func (c *calculator) minMemKiBForMilliCpu(mCpu int64, lmts limits) int64 {
	unrounded := size.RoundUpToIncrement(mCpu*lmts.resizableConfig.MinKiBPerCPU(), 1000) / 1000
	return size.RoundUpToIncrement(unrounded, lmts.incrementStep.KBytes)
}

func (c *calculator) minMilliCpuForMemKiB(memKiB int64, lmts limits) int64 {
	unrounded := size.RoundUpToIncrement(memKiB*1000, lmts.resizableConfig.MaxKiBPerCPU()) / lmts.resizableConfig.MaxKiBPerCPU()
	return size.RoundUpToIncrement(unrounded, lmts.incrementStep.MilliCpus)
}

func (c *calculator) kubeProxyMemKiBOverhead(node *apiv1.Node, lmts limits) resource.Quantity {
	if !c.isClusterUsingDPV1 {
		return zeroBinarySIQuantity
	}
	maxVmSize, err := getMaxVmSize(c.provider, node)
	if err != nil {
		// This is a safe fallback. If the max VM size cannot be determined,
		// assume 0 for kube proxy memory overhead to prevent cascading errors
		// and allow the allocatable calculation to proceed.
		klog.Warningf("Failed to get max VM size for node %q, using 0 for kube proxy memory overhead. Error: %v", node.Name, err)
		return zeroBinarySIQuantity
	}
	cpuRounded := size.RoundUpToIncrement(maxVmSize.MilliCpus, 1000) / 1000
	resizableConfig := lmts.resizableConfig
	if resizableConfig == nil {
		klog.Warningf("Limits ResizableConfig is nil for node %q, using 0 for kube proxy memory overhead.", node.Name)
		return zeroBinarySIQuantity
	}
	return *resource.NewQuantity(resizableConfig.KubeProxyMemoryBytesOverheadPerCPU.Value()*cpuRounded, resource.BinarySI)
}

// binarySearchFirstTrue returns the smallest x where f(x) = true && low <= x <= high, and returns math.MinInt64 if all x in the range evaluate to false.
// Assumes that there is no x and y pair where low <= x < y <= high && f(x) = true && f(y) = false
func binarySearchFirstTrue(low, high int64, f func(int64) bool) int64 {
	var result int64 = math.MinInt64
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

func getMaxVmSize(provider cloudProvider, node *apiv1.Node) (size.VmSize, error) {
	machineType, err := getMachineType(node)
	if err != nil {
		return size.VmSize{}, fmt.Errorf("failed to get machine type for node %q: %w", node.Name, err)
	}
	vmSize, err := provider.MachineConfigProvider().GetMaxResizableVmSizeByMachineType(machineType)
	if err != nil {
		return size.VmSize{}, fmt.Errorf("failed to find info for machine type %s in node %q: %w", machineType, node.Name, err)
	}
	return vmSize, nil
}

func getMinVmMemoryKiB(provider cloudProvider, node *apiv1.Node) (int64, error) {
	machineType, err := getMachineType(node)
	if err != nil {
		return 0, fmt.Errorf("failed to get machine type for node %q: %w", node.Name, err)
	}
	size, err := provider.MachineConfigProvider().GetMinResizableVmSizeByMachineType(machineType)
	if err != nil {
		return 0, fmt.Errorf("failed to find info for machine type %s in node %q: %w", machineType, node.Name, err)
	}
	return size.KBytes, nil
}

func getMachineType(node *apiv1.Node) (string, error) {
	machineType, err := getValueFromLabel(node.Labels, apiv1.LabelInstanceTypeStable)
	if err != nil {
		return "", fmt.Errorf("failed retrieving machine type in node %q: %w", node.Name, err)
	}
	return machineType, nil
}

func getGkeMigOsInfo(provider cloudProvider, node *apiv1.Node) *gke.GkeMigOsInfo {
	return gke.NewGkeMigOsInfo(
		gce.NewMigOsInfo(getOs(node), getOsDistribution(node), getArch(node)),
		getNodeVersion(provider, node),
		getConfidentialNode(node))
}

func getNodeVersion(provider cloudProvider, node *apiv1.Node) string {
	version := node.Status.NodeInfo.KubeletVersion
	if version == "" {
		version = provider.GetClusterVersion()
	}
	if version == "" {
		klog.Warningf("Failed retrieving node version in node %q", node.Name)
	}
	return version
}

func getArch(node *apiv1.Node) gce.SystemArchitecture {
	arch, err := getValueFromLabel(node.Labels, apiv1.LabelArchStable)
	if err != nil {
		klog.Warningf("Failed retrieving system architecture in node %q. Error value: %q. Assuming default: %q", node.Name, err, gce.DefaultArch)
		return gce.DefaultArch
	}
	return gce.SystemArchitecture(arch)
}

func getConfidentialNode(node *apiv1.Node) bool {
	confNode, found := node.Labels[gkelabels.GkeConfidentialNodes]
	if !found {
		klogx.V(4).UpTo(confidentialNodeLoggingQuota).Infof("Node %q doesn't have %q label (this is expected for clusters without confidential nodes). Using default: %v", node.Name, gkelabels.GkeConfidentialNodes, defaultConfidentialNode)
		return defaultConfidentialNode
	}
	return confNode == "true"
}

func getOs(node *apiv1.Node) gce.OperatingSystem {
	os, err := getValueFromLabel(node.Labels, apiv1.LabelOSStable)
	if err != nil {
		klog.Warningf("Failed retrieving OS in node %q. Error value: %q. Assuming default: %q", node.Name, err, gce.OperatingSystemDefault)
		return gce.OperatingSystemDefault
	}
	return gce.OperatingSystem(os)
}

func getOsDistribution(node *apiv1.Node) gce.OperatingSystemDistribution {
	osDist, err := getValueFromLabel(node.Labels, gkelabels.GkeOsDistributionLabel)
	if err != nil {
		klog.Warningf("Failed retrieving OS distribution in node %q. Error value: %q. Assuming default: %q", node.Name, err, gce.OperatingSystemDistributionDefault)
		return gce.OperatingSystemDistributionDefault
	}
	return gce.OperatingSystemDistribution(osDist)
}

func getMaxPodsPerNode(node *apiv1.Node) int64 {
	mppnStr, err := getValueFromLabel(node.Labels, gkelabels.MaxPodsPerNodeLabel)
	if err != nil {
		klog.Warningf("Failed retrieving max pods per node value of node %v, err: %v. Defaulting to: %v", node.Name, err, gkelabels.DefaultMaxPodsPerNode)
		return gkelabels.DefaultMaxPodsPerNode
	}
	mppn, err := strconv.Atoi(mppnStr)
	if err != nil {
		klog.Warningf("Failed retrieving max pods per node value of node %v, err: %v. Defaulting to: %v", node.Name, err, gkelabels.DefaultMaxPodsPerNode)
		return gkelabels.DefaultMaxPodsPerNode
	}
	return int64(mppn)
}

func getValueFromLabel(labels map[string]string, key string) (string, error) {
	value, found := labels[key]
	if !found {
		return "", fmt.Errorf("failed to find label %s", key)
	}
	return value, nil
}
