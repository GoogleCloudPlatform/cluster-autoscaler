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

package conditions

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

// gpuConfigChecker validated gpu config in node config rule.
type gpuConfigChecker struct {
	provider CloudProvider
}

func (ch *gpuConfigChecker) checkRule(rule rules.Rule) *metav1.Condition {
	if rule.GpuRequest().Empty() {
		return nil
	}

	gpuName := rule.GpuRequest().Config.GpuType
	gpuCount := rule.GpuRequest().PhysicalGPUCount

	// check if GPU type is supported.
	gpu, found := ch.provider.MachineConfigProvider().ToGpuType(gpuName)
	if !found {
		return InvalidGpuTypeCondition(gpuName)
	}
	// check if GPU count is supported.
	if _, found := gpu.MaxCpuCount()[gpuCount]; !found {
		return InvalidGpuConfigurationCondition(gpuName, int(gpuCount))
	}

	// check if gpu is compatible with min CPU requirements.
	if int(rule.MinCores()) > gpu.MaxCpuCount()[gpuCount] {
		return GpuNotSupportedWithCpuCondition(gpuName, int(gpuCount), int(rule.MinCores()), gpu.MaxCpuCount()[gpuCount])
	}

	// check if gpu is compatible with machine type / machine family.
	// Note: only machine type or machine family would exist.
	ruleMachineType := rule.MachineType()
	if ruleMachineType != "" {
		if found, err := ch.provider.MachineConfigProvider().IsGpuTypeAndCountSupportedByMachineType(ruleMachineType, gpuName, gpuCount); !found || err != nil {
			return GpuNotSupportedWithMachineTypeCondition(gpuName, int(gpuCount), ruleMachineType)
		}
	} else {
		machineFamilyName := rule.MachineFamily()
		// if rule doesn't specify machine family, it will be implied from the GPU config of a rule.
		// previous crdChecks are sufficient in such case, so we return early.
		if machineFamilyName == "" {
			return nil
		}
		machineFamily, _ := ch.provider.MachineConfigProvider().ToMachineFamily(machineFamilyName)
		found := false
		for machineType := range machineFamily.AllMachineTypes(machinetypes.NoConstraints) {
			if supported, err := ch.provider.MachineConfigProvider().IsGpuTypeAndCountSupportedByMachineType(machineType, gpuName, gpuCount); supported && err == nil {
				found = true
				break
			}
		}
		if !found {
			return GpuNotSupportedWithMachineFamilyCondition(gpuName, int(gpuCount), machineFamilyName)
		}
	}

	return nil
}

func (ch *gpuConfigChecker) conditionType() string {
	return CrdMisconfiguredCondition
}
