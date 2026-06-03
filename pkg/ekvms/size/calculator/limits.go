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

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
)

type limits struct {
	minVmSize       size.VmSize
	incrementStep   size.VmSize
	safetyBuffer    apiv1.ResourceList
	resizableConfig *machinetypes.ResizableMachineFamilyConfig
}

// LimitConfig contains custom limits for resizable machine families.
type LimitConfig struct {
	// MinVmSize is the minimum VM size allowed for the machine family.
	MinVmSize apiv1.ResourceList
	// IncrementStep is the granularity of VM size adjustments for the machine family.
	IncrementStep apiv1.ResourceList
	// SafetyBuffer is the resource buffer added to the required resources when calculating target VM size.
	SafetyBuffer apiv1.ResourceList
}

type LimitProvider interface {
	GetLimits(node *apiv1.Node) (limits, error)
}

type resizeLimitProvider struct {
	configs       map[string]LimitConfig
	cloudProvider cloudProvider
}

func NewResizeLimitProvider(cloudProvider cloudProvider) *resizeLimitProvider {
	return &resizeLimitProvider{
		configs:       make(map[string]LimitConfig),
		cloudProvider: cloudProvider,
	}
}

func (p *resizeLimitProvider) RegisterConfig(machineFamily string, config LimitConfig) {
	p.configs[machineFamily] = config
}

func (p *resizeLimitProvider) GetLimits(node *apiv1.Node) (limits, error) {
	machineType, err := getMachineType(node)
	if err != nil {
		return limits{}, err
	}
	machineFamily, err := p.cloudProvider.MachineConfigProvider().GetMachineFamilyFromMachineName(machineType)
	if err != nil {
		return limits{}, fmt.Errorf("failed to get machine family for machine type %s in node %q: %w", machineType, node.Name, err)
	}
	resizableConfig := machineFamily.ResizableConfig()
	if resizableConfig == nil {
		return limits{}, fmt.Errorf("node %q is not resizable", node.Name)
	}

	limitConfig, found := p.configs[machineFamily.Name()]
	// If no config found, fallback to defaults from ResizableMachineFamilyConfig
	if !found {
		return limits{
			minVmSize:       size.VmSize{MilliCpus: resizableConfig.MinVmSizeDefaultMilliCPU(), KBytes: resizableConfig.MinVmSizeDefaultKiB()},
			incrementStep:   size.VmSize{MilliCpus: resizableConfig.IncrementStepDefaultMilliCPU(), KBytes: resizableConfig.IncrementStepDefaultKiB()},
			safetyBuffer:    resizableConfig.AllocationSafetyDefault,
			resizableConfig: resizableConfig,
		}, nil
	}

	minVmMilliCpu := limitConfig.MinVmSize.Cpu().MilliValue()
	minVmMemory := limitConfig.MinVmSize.Memory().Value()
	incrementStepMilliCpu := limitConfig.IncrementStep.Cpu().MilliValue()
	incrementStepMemory := limitConfig.IncrementStep.Memory().Value()

	if err := validateValue(minVmMilliCpu, resizableConfig.MinSizeLimitMilliCPU(), resizableConfig.MinIncrementLimitMilliCPU()); err != nil {
		return limits{}, fmt.Errorf("Min VmSize MilliCPUs has unsupported value: %v", err)
	}
	if err := validateValue(minVmMemory, resizableConfig.MinSizeLimit.Memory().Value(), resizableConfig.MinIncrementLimit.Memory().Value()); err != nil {
		return limits{}, fmt.Errorf("Min VmSize KBytes has unsupported value: %v", err)
	}
	if err := validateValue(incrementStepMilliCpu, resizableConfig.MinIncrementLimitMilliCPU(), resizableConfig.MinIncrementLimitMilliCPU()); err != nil {
		return limits{}, fmt.Errorf("Increment Step MilliCPUs has unsupported value: %v", err)
	}
	if err := validateValue(incrementStepMemory, resizableConfig.MinIncrementLimit.Memory().Value(), resizableConfig.MinIncrementLimit.Memory().Value()); err != nil {
		return limits{}, fmt.Errorf("Increment Step KBytes has unsupported value: %v", err)
	}

	return limits{
		minVmSize:       size.VmSize{MilliCpus: minVmMilliCpu, KBytes: minVmMemory / size.KiB},
		incrementStep:   size.VmSize{MilliCpus: incrementStepMilliCpu, KBytes: incrementStepMemory / size.KiB},
		safetyBuffer:    limitConfig.SafetyBuffer,
		resizableConfig: resizableConfig,
	}, nil
}

func validateValue(value, minValue, increment int64) error {
	if value%increment != 0 {
		return fmt.Errorf("should be divisible by %d, got %d", increment, value)
	}
	if value < minValue {
		return fmt.Errorf("should be greater than or equal to %d, got %d", minValue, value)
	}
	return nil
}
