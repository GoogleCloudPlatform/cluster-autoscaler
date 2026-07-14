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
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
)

// ResizableMachineFamilyConfig contains information about resizable machine families.
type ResizableMachineFamilyConfig struct {
	// KubeProxyMemoryBytesOverheadPerCPU is the memory overhead used by kubeProxy per CPU in bytes. Source: go/gke-ek-kube-proxy
	KubeProxyMemoryBytesOverheadPerCPU resource.Quantity

	// MinSizeLimit is the minimum size limit for a resizable VM.
	MinSizeLimit apiv1.ResourceList

	// MinVmSizeDefault is the default minimum size for a resizable VM.
	MinVmSizeDefault apiv1.ResourceList

	// MinIncrementLimit is the minimum increment limit for a resizable VM.
	MinIncrementLimit apiv1.ResourceList

	// IncrementStepDefault is the default increment step for a resizable VM.
	IncrementStepDefault apiv1.ResourceList

	// AllocationSafetyDefault is the default allocation safety buffer for a resizable VM.
	AllocationSafetyDefault apiv1.ResourceList

	// MinMemoryPerCPU is the minimum memory per CPU for a resizable VM.
	MinMemoryPerCPU resource.Quantity

	// MaxMemoryPerCPU is the maximum memory per CPU for a resizable VM.
	MaxMemoryPerCPU resource.Quantity

	// DefaultMachineTypes is the list of default machine types for the resizable machine family.
	DefaultMachineTypes []string
}

// MinSizeLimitMilliCPU returns the minimum CPU size limit in milliCPU for a resizable VM.
func (c *ResizableMachineFamilyConfig) MinSizeLimitMilliCPU() int64 {
	return c.MinSizeLimit.Cpu().MilliValue()
}

// MinIncrementLimitMilliCPU returns the minimum CPU increment limit in milliCPU for a resizable VM.
func (c *ResizableMachineFamilyConfig) MinIncrementLimitMilliCPU() int64 {
	return c.MinIncrementLimit.Cpu().MilliValue()
}

// MinKiBPerCPU returns the minimum memory in KiB per CPU for a resizable VM.
func (c *ResizableMachineFamilyConfig) MinKiBPerCPU() int64 {
	return c.MinMemoryPerCPU.Value() / size.KiB
}

// MaxKiBPerCPU returns the maximum memory in KiB per CPU for a resizable VM.
func (c *ResizableMachineFamilyConfig) MaxKiBPerCPU() int64 {
	return c.MaxMemoryPerCPU.Value() / size.KiB
}

// MinVmSizeDefaultMilliCPU returns the default minimum CPU size in milliCPU for a resizable VM.
func (c *ResizableMachineFamilyConfig) MinVmSizeDefaultMilliCPU() int64 {
	return c.MinVmSizeDefault.Cpu().MilliValue()
}

// MinVmSizeDefaultKiB returns the default minimum memory size in KiB for a resizable VM.
func (c *ResizableMachineFamilyConfig) MinVmSizeDefaultKiB() int64 {
	return c.MinVmSizeDefault.Memory().Value() / size.KiB
}

// IncrementStepDefaultMilliCPU returns the default CPU increment step in milliCPU for a resizable VM.
func (c *ResizableMachineFamilyConfig) IncrementStepDefaultMilliCPU() int64 {
	return c.IncrementStepDefault.Cpu().MilliValue()
}

// IncrementStepDefaultKiB returns the default memory increment step in KiB for a resizable VM.
func (c *ResizableMachineFamilyConfig) IncrementStepDefaultKiB() int64 {
	return c.IncrementStepDefault.Memory().Value() / size.KiB
}

// ResizableMachineTypeConfig contains information about resizable machine types.
type ResizableMachineTypeConfig struct {
	// Minimum supported resources.
	MinResources apiv1.ResourceList
}

// MinMilliCPU returns the minimum supported CPU in milliCPU.
func (c *ResizableMachineTypeConfig) MinMilliCPU() int64 {
	return c.MinResources.Cpu().MilliValue()
}

// MinSizeKb returns the minimum supported memory in KB.
func (c *ResizableMachineTypeConfig) MinSizeKb() int64 {
	return c.MinResources.Memory().Value() / size.KiB
}
