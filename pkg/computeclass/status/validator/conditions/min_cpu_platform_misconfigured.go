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

// minCpuPlatformConfigChecker returns condition if min cpu platform config is not valid.
type minCpuPlatformConfigChecker struct {
	provider CloudProvider
}

func (ch *minCpuPlatformConfigChecker) checkRule(rule rules.Rule) *metav1.Condition {
	minCpuPlatform, _ := rule.MinCpuPlatform()
	minCpuPlatformString := rule.MinCpuPlatformString()
	if minCpuPlatform == machinetypes.UnknownPlatform {
		// In theory this shouldn't happen, as such case should be prevented by kubebuilder validation enum rule.
		return UnknownMinCpuPlatformCondition(minCpuPlatformString)
	}

	machineFamilyName := rule.MachineFamily()
	machineFamily, err := ch.provider.MachineConfigProvider().ToMachineFamily(machineFamilyName)
	if err == nil {
		if !machineFamily.IsPlatformSupported(minCpuPlatform) {
			return MinCpuPlatformIncompatibleWithMachineFamilyCondition(minCpuPlatformString, machineFamilyName)
		}
	}
	machineTypeName := rule.MachineType()
	machineFamily, machineFamilyError := ch.provider.MachineConfigProvider().GetMachineFamilyFromMachineName(machineTypeName)
	if machineFamilyError == nil {
		if !machineFamily.AreConstraintsSupported(machinetypes.Constraints{CpuPlatform: minCpuPlatform, ExplicitMachineTypes: []string{machineTypeName}}) {
			return MinCpuPlatformIncompatibleWithMachineTypeCondition(minCpuPlatformString, machineTypeName)
		}
	}
	if machineFamilyName == "" && machineTypeName == "" {
		defaultFamilyName := ch.provider.GetAutoprovisioningDefaultFamily().Name()
		defaultFamily, err := ch.provider.MachineConfigProvider().ToMachineFamily(defaultFamilyName)
		if err == nil {
			if !defaultFamily.IsPlatformSupported(minCpuPlatform) {
				// In theory user can still specify machine family other than the default one through node selector.
				// Nevertheless we consider CCC to be invalid in this case.
				return MinCpuPlatformIncompatibleWithMachineFamilyCondition(minCpuPlatformString, defaultFamilyName)
			}
		}
	}

	// K80 has some additional restrictions on min cpu platform.
	gpuRequest := rule.GpuRequest()
	if !gpuRequest.Empty() {
		gpuType := gpuRequest.Config.GpuType
		if gpuType == machinetypes.NvidiaTeslaK80.Name() && machinetypes.PlatformIsAtLeast(minCpuPlatform, machinetypes.IntelSkylake) {
			return MinCpuPlatformIncompatibleWithGpuCondition(minCpuPlatformString, gpuType)
		}
	}

	return nil

}

func (ch *minCpuPlatformConfigChecker) conditionType() string {
	return RuleMisconfiguredCondition
}
