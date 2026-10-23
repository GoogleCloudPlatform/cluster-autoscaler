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
	"math"
	"slices"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce/localssdsize"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/klog/v2"
)

// mixedModeLocalSSDMinNodeVersion mirrors GKE's LocalSSD::MixedModeMinVersion
// control-plane flag, which CA cannot read.
const mixedModeLocalSSDMinNodeVersion = "1.37.0-gke.3509000"

// storageConfigChecker checks if storage configuration is valid.
type storageConfigChecker struct {
	provider         CloudProvider
	localSsdProvider localssdsize.LocalSSDSizeProvider
}

func (ch *storageConfigChecker) checkRule(rule rules.Rule) *metav1.Condition {
	return ch.checkRuleWithNodeVersion(rule, "")
}

func (ch *storageConfigChecker) checkRuleWithNodeVersion(rule rules.Rule, nodeVersion string) *metav1.Condition {
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
	// check local ssd configuration is valid.
	hasLocalSSD := rule.TotalLSSDCount() != 0 || rule.EphemeralCapacityGb() != nil || rule.LocalNvmeSsdBlockEnabled() || rule.EphemeralStorageLocalSsdEnabled()
	if hasLocalSSD {
		if rule.EphemeralCapacityGb() != nil && *rule.EphemeralCapacityGb() > 0 && isBelowMixedModeMin(nodeVersion) {
			return MixedModeLocalSSDUnsupportedNodeVersionCondition(mixedModeLocalSSDMinNodeVersion, nodeVersion)
		}
		dedicatedSwap := rule.SwapConfig() != nil && rule.SwapConfig().DedicatedLocalSsdProfile != nil
		if dedicatedSwap {
			if rule.EphemeralCapacityGb() != nil {
				return DedicatedLocalSsdSwapWithMixedModeCondition()
			}
			if rule.LocalNvmeSsdBlockEnabled() {
				return DedicatedLocalSsdSwapWithBlockModeCondition()
			}
		}
		if rule.SwapConfig() != nil && rule.SwapConfig().EphemeralLocalSsdProfile != nil && rule.LocalNvmeSsdBlockEnabled() {
			return EphemeralLocalSsdSwapWithBlockModeCondition()
		}

		swap := rule.SwapDedicatedLSSDCount()
		countOmitted := rule.LocalSSDCountOmitted()

		// check if local ssd count is valid with machine type / machine family.
		if ruleMachineType != "" {
			if countOmitted {
				fixedCount, isFixed, err := ch.provider.MachineConfigProvider().AutomaticEphemeralLocalSsdCountByMachineType(ruleMachineType)
				if err != nil {
					klog.Warningf("Failed to get fixed local SSD count for machine type %s: %v", ruleMachineType, err)
					isFixed = false
				}
				if !isFixed {
					if !ch.anyLocalSSDSupportedByMachineType(ruleMachineType) {
						return LocalSSDNotSupportedByMachineTypeCondition(ruleMachineType)
					}
					return LocalSSDCountRequiredForMachineTypeCondition(ruleMachineType)
				}
				if fixedCount-swap <= 0 {
					return DedicatedLocalSsdSwapConsumesAllLocalSSDsCondition(swap, ruleMachineType, fixedCount)
				}
			} else {
				if found, allowedCounts := ch.localSSDSupportedByMachineType(ruleMachineType, int(rule.TotalLSSDCount())); !found {
					return LocalSSDNotSupportedWithMachineTypeCondition(int(rule.TotalLSSDCount()), ruleMachineType, allowedCounts)
				}
			}

			res := ch.evaluateMachineFeasibility(ruleMachineType, rule)
			if res.exceedsCapacity {
				return LocalSSDCapacityExceedsMachineTypeLimitCondition(*rule.EphemeralCapacityGb(), res.totalCapacity, ruleMachineType)
			}
		} else {
			familySupportsAnyLocalSSD := false
			familySupportsCount := false
			// Whether a machine with CPU >= minCores supports the count. Explicit counts
			// are checked against minCores separately below.
			eligibleSupportsCount := false
			hasFeasibleMachine := false
			var maxCapacityInFamily int64

			for machineType, machine := range machineFamily.AllMachineTypes(machinetypes.NoConstraints) {
				if ch.anyLocalSSDSupportedByMachineType(machineType) {
					familySupportsAnyLocalSSD = true
				}

				res := ch.evaluateMachineFeasibility(machineType, rule)
				if !res.supportsCount {
					continue
				}
				familySupportsCount = true
				if countOmitted && machine.CPU < rule.MinCores() {
					continue
				}
				eligibleSupportsCount = true
				if res.totalCapacity > maxCapacityInFamily {
					maxCapacityInFamily = res.totalCapacity
				}
				if res.feasible {
					hasFeasibleMachine = true
					break
				}
			}

			if !familySupportsAnyLocalSSD {
				if countOmitted {
					return LocalSSDNotSupportedByMachineFamilyCondition(machineFamilyName)
				}
				return LocalSSDNotSupportedWithMachineFamilyCondition(int(rule.TotalLSSDCount()), machineFamilyName)
			}
			if !familySupportsCount {
				if countOmitted {
					return LocalSSDCountRequiredForMachineFamilyCondition(machineFamilyName)
				}
				return LocalSSDNotSupportedWithMachineFamilyCondition(int(rule.TotalLSSDCount()), machineFamilyName)
			}
			if !eligibleSupportsCount {
				return LocalSSDCountOmittedIncompatibleWithCpuCondition(machineFamilyName, int(rule.MinCores()))
			}
			if !hasFeasibleMachine {
				return LocalSSDCapacityExceedsMachineFamilyLimitCondition(*rule.EphemeralCapacityGb(), maxCapacityInFamily, machineFamilyName)
			}
		}

		// check if local ssd count is valid with min CPU.
		if rule.MinCores() > 0 && !countOmitted {
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

type machineFeasibilityResult struct {
	supportsCount   bool
	effectiveCount  int
	totalCapacity   int64
	exceedsCapacity bool
	feasible        bool
}

func (ch *storageConfigChecker) evaluateMachineFeasibility(machineType string, rule rules.Rule) machineFeasibilityResult {
	var res machineFeasibilityResult
	swap := rule.SwapDedicatedLSSDCount()
	countOmitted := rule.LocalSSDCountOmitted()

	if countOmitted {
		fixedCount, isFixed, err := ch.provider.MachineConfigProvider().AutomaticEphemeralLocalSsdCountByMachineType(machineType)
		if err != nil {
			klog.Warningf("Failed to get fixed local SSD count for machine type %s: %v", machineType, err)
			isFixed = false
		}
		if !isFixed {
			return res
		}
		modeCount := int(fixedCount - swap)
		if modeCount <= 0 {
			return res
		}
		res.supportsCount = true
		res.effectiveCount = modeCount
	} else {
		supported, _ := ch.localSSDSupportedByMachineType(machineType, int(rule.TotalLSSDCount()))
		if !supported {
			return res
		}
		modeCount := int(rule.TotalLSSDCount() - swap)
		res.supportsCount = true
		res.effectiveCount = modeCount
	}

	if rule.EphemeralCapacityGb() != nil {
		partitionSize := int64(ch.localSsdProvider.SSDSizeInGiB(machineType))
		res.totalCapacity = partitionSize * int64(res.effectiveCount)
		res.exceedsCapacity = *rule.EphemeralCapacityGb() >= res.totalCapacity
		if res.exceedsCapacity {
			return res
		}
	}

	res.feasible = true
	return res
}

func (ch *storageConfigChecker) supportedLocalSSDCounts(machineType string) []int {
	autoCount, autoFound, err := ch.provider.MachineConfigProvider().AutomaticEphemeralLocalSsdCountByMachineType(machineType)
	if err != nil {
		klog.Warningf("Failed to get automatic ephemeral local SSD count for machine type %s: %v", machineType, err)
	} else if autoFound && autoCount > 0 {
		return []int{int(autoCount)}
	}

	allowedCounts, allowedFound, err := ch.provider.MachineConfigProvider().AllowedEphemeralLocalSsdCountByMachineType(machineType)
	if err != nil {
		klog.Warningf("Failed to get allowed ephemeral local SSD counts for machine type %s: %v", machineType, err)
	} else if allowedFound {
		return allowedCounts
	}
	return nil
}

func (ch *storageConfigChecker) localSSDSupportedByMachineType(machineType string, localSSDCount int) (bool, []int) {
	counts := ch.supportedLocalSSDCounts(machineType)
	return slices.Contains(counts, localSSDCount), counts
}

func (ch *storageConfigChecker) anyLocalSSDSupportedByMachineType(machineType string) bool {
	return len(ch.supportedLocalSSDCounts(machineType)) > 0
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

// isBelowMixedModeMin reports whether a pinned nodeVersion is below
// mixedModeLocalSSDMinNodeVersion. Aliases ("1.36", "1.36.5") resolve to the
// newest matching build, so they're flagged only if that build is still below
// the minimum. Empty or unparseable values (e.g. "latest") aren't flagged.
func isBelowMixedModeMin(nodeVersion string) bool {
	minVer, err := version.FromString(mixedModeLocalSSDMinNodeVersion)
	if err != nil || nodeVersion == "" {
		return false
	}
	if strings.Contains(nodeVersion, "-gke") {
		v, err := version.FromString(nodeVersion)
		return err == nil && v.LessThan(minVer)
	}
	if v, err := version.FromString(nodeVersion); err == nil {
		return version.Version{v[0], v[1], v[2], math.MaxInt}.LessThan(minVer)
	}
	if mv, err := version.MinorVersionFromString(nodeVersion); err == nil {
		return mv.LessThan(minVer.MinorVersion())
	}
	return false
}
