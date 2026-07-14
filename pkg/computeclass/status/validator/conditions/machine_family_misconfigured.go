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
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

// machineFamilyConfigChecker checks if machine family configuration is valid.
type machineFamilyConfigChecker struct {
	provider CloudProvider
}

func (ch *machineFamilyConfigChecker) checkRule(rule rules.Rule) *metav1.Condition {
	machineFamilyName := rule.MachineFamily()
	if machineFamilyName == "" {
		// TODO(b/517095756): this approach doesn't care about specified GPU or TPU type while they affect
		// machine family selection. Machine selector should be used instead in order to
		// determine real machine family that would be considered during autoprovisioning.
		machineFamilyName = ch.provider.GetAutoprovisioningDefaultFamily().Name()
	}
	machineFamily, err := ch.provider.MachineConfigProvider().ToMachineFamily(machineFamilyName)
	if err != nil {
		return MachineFamilyNotFoundCondition(machineFamilyName)
	}
	// check if any machine type exists with given machine family, cpu and memory.
	cpu, memoryGb := rule.MinCores(), rule.MinMemoryGb()
	largestMachineType := machineFamily.LargestMachineType(machinetypes.NoConstraints)
	if largestMachineType.CPU < cpu || largestMachineType.Memory < memoryGb*units.GiB {
		return NoSuitableMachineExists(cpu, memoryGb)
	}
	return nil
}

func (ch *machineFamilyConfigChecker) conditionType() string {
	return RuleMisconfiguredCondition
}

// machineTypeExistenceChecker checks if the machine type exists.
type machineTypeExistenceChecker struct {
	provider CloudProvider
}

func (ch *machineTypeExistenceChecker) checkRule(rule rules.Rule) *metav1.Condition {
	machineType := rule.MachineType()
	// check if machine type exists.
	if machineType != "" {
		_, err := ch.provider.MachineConfigProvider().ToMachineType(machineType)
		if err != nil {
			return MachineTypeNotFoundCondition(machineType)
		}
	}
	return nil
}

func (ch *machineTypeExistenceChecker) conditionType() string {
	return RuleMisconfiguredCondition
}
