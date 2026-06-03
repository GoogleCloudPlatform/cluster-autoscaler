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
	"k8s.io/klog/v2"
)

// storageConfigChecker checks if storage configuration is valid.
type storageConfigChecker struct {
	provider CloudProvider
}

func (ch *storageConfigChecker) checkRule(rule rules.Rule) *metav1.Condition {
	machineFamilyName := rule.MachineFamily()
	if machineFamilyName == "" {
		machineFamilyName = ch.provider.GetAutoprovisioningDefaultFamily().Name()
	}
	machineFamily, _ := ch.provider.MachineConfigProvider().ToMachineFamily(machineFamilyName)
	ruleMachineType := rule.MachineType()

	// check if disk type configuration is valid.
	if rule.BootDiskType() != "" {
		// check if disk type is valid with machine type / machine family.
		if ruleMachineType != "" {
			if mf, err := ch.provider.MachineConfigProvider().GetMachineFamilyFromMachineName(ruleMachineType); err == nil && ch.machineFamilySupportsDiskType(mf.Name(), rule.BootDiskType()) {
				return DiskTypeNotSupportedWithMachineTypeCondition(rule.BootDiskType(), ruleMachineType)
			}
		} else if ch.machineFamilySupportsDiskType(machineFamily.Name(), rule.BootDiskType()) {
			return DiskTypeNotSupportedWithMachineFamilyCondition(rule.BootDiskType(), machineFamilyName)
		}
	}
	// check local ssd count configuration is valid.
	if rule.TotalLSSDCount() != 0 {
		// check if local ssd count is valid with machine type / machine family.
		if ruleMachineType != "" {
			if found, allowedCounts := ch.localSSDSupportedByMachineType(ruleMachineType, int(rule.TotalLSSDCount())); !found {
				return LocalSSDNotSupportedWithMachineTypeCondition(int(rule.TotalLSSDCount()), ruleMachineType, allowedCounts)
			}
		} else {
			machineFamilySupportsLocalSSD := false
			for machineType := range machineFamily.AllMachineTypes(machinetypes.NoConstraints) {
				if found, _ := ch.localSSDSupportedByMachineType(machineType, int(rule.TotalLSSDCount())); found {
					machineFamilySupportsLocalSSD = true
					break
				}
			}
			if !machineFamilySupportsLocalSSD {
				return LocalSSDNotSupportedWithMachineFamilyCondition(int(rule.TotalLSSDCount()), machineFamilyName)
			}
		}

		// check if local ssd count is valid with min CPU.
		if rule.MinCores() > 0 {
			localSSDCompatibleWithMinCPU := false
			for machineName, machine := range machineFamily.AllMachineTypes(machinetypes.NoConstraints) {
				if machine.CPU < rule.MinCores() {
					continue
				}
				if found, _ := ch.localSSDSupportedByMachineType(machineName, int(rule.TotalLSSDCount())); found {
					localSSDCompatibleWithMinCPU = true
					break
				}
			}
			if !localSSDCompatibleWithMinCPU {
				return LocalSSDIncompatibleWithCpuCondition(machineFamilyName, int(rule.TotalLSSDCount()), int(rule.MinCores()))
			}
		}
	}
	return nil
}

func (ch *storageConfigChecker) conditionType() string {
	return RuleMisconfiguredCondition
}

func (ch *storageConfigChecker) localSSDSupportedByMachineType(machineType string, localSSDCount int) (bool, []int) {
	localSSDSupported := false
	supportedCounts := []int{}
	if count, found, err := ch.provider.MachineConfigProvider().AutomaticEphemeralLocalSsdCountByMachineType(machineType); found && err == nil {
		if localSSDCount == int(count) {
			localSSDSupported = true
		}
		supportedCounts = append(supportedCounts, int(count))
	} else if counts, found, err := ch.provider.MachineConfigProvider().AllowedEphemeralLocalSsdCountByMachineType(machineType); found && err == nil {
		for _, c := range counts {
			if int(localSSDCount) == c {
				localSSDSupported = true
				break
			}
		}
		supportedCounts = append(supportedCounts, counts...)
	}
	return localSSDSupported, supportedCounts
}

func (ch *storageConfigChecker) machineFamilySupportsDiskType(machineFamily, diskType string) bool {
	mf, err := ch.provider.MachineConfigProvider().ToMachineFamily(machineFamily)
	if err != nil {
		klog.Errorf("invalid machine family: %v", machineFamily)
		return false
	}

	constraint := machinetypes.Constraints{
		CpuPlatform: machinetypes.AnyPlatform,
		DiskType:    diskType,
	}

	return len(mf.AllMachineTypes(constraint)) == 0
}
