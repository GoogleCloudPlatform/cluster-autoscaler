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
	"fmt"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Conditions for crd status.
const (
	HealthCondition                = "Health"
	NodepoolMisconfiguredCondition = "NodepoolMisconfigured"
	CrdMisconfiguredCondition      = "CrdMisconfigured"
	RuleMisconfiguredCondition     = "RuleMisconfigured"
)

// Reasons for crd conditions.
const (
	NapCannotBeEnabledReason                    = "NapCannotBeEnabled"
	NodePoolNotExistReason                      = "NodePoolNotExist"
	LocationNotEnabledForAutoprovisioningReason = "LocationNotEnabledForAutoprovisioning"
	UnavailableMachineTypeReason                = "UnavailableMachineType"
	NoRuleMatchingReason                        = "NoRuleMatching"

	NapDisabledAndNoMatchingNodegroupsReason            = "NapDisabledAndNoMatchingNodegroups"
	CrdLabelNotMatchingReason                           = "CrdLabelNotMatching"
	MultipleCrdTaintsReason                             = "MultipleCrdTaints"
	TaintMissingReason                                  = "TaintMissing"
	TaintValueNotMatchingReason                         = "TaintValueNotMatching"
	MachineFamilyNotFoundReason                         = "MachineFamilyNotFound"
	MachineTypeNotFoundReason                           = "MachineTypeNotFound"
	NoSuitableMachineExistsReason                       = "NoSuitableMachineExists"
	NodepoolWillNeverScaleUpReason                      = "NodepoolWillNeverScaleUp"
	ReservationNotFoundReason                           = "ReservationNotFound"
	ReservationUnusableReason                           = "ReservationUnusable"
	ReservationBlockUnusableReason                      = "ReservationBlockUnusable"
	UnsupportedNodeSystemConfigFormatReason             = "UnsupportedNodeSystemConfig"
	UnknownMinCpuPlatformReason                         = "UnknownMinCpuPlatform"
	MinCpuPlatformIncompatibleReason                    = "MinCpuPlatformIncompatible"
	RmTagValidationReason                               = "ResourceManagerTagValidationFailed"
	EvictionSoftMemoryInvalidQuantityReason             = "EvictionSoftMemoryInvalidQuantity"
	EvictionSoftMemoryTooLowReason                      = "EvictionSoftMemoryTooLow"
	EvictionSoftMemoryTooHighReason                     = "EvictionSoftMemoryTooHigh"
	EvictionSoftInvalidPercentageFormatReason           = "EvictionSoftInvalidPercentageFormat"
	EvictionSoftPercentageOutOfRangeReason              = "EvictionSoftPercentageOutOfRange"
	EvictionSoftMissingGracePeriodReason                = "EvictionSoftMissingGracePeriod"
	EvictionSoftGracePeriodInvalidDurationReason        = "EvictionSoftGracePeriodInvalidDuration"
	EvictionSoftGracePeriodOutOfRangeReason             = "EvictionSoftGracePeriodOutOfRange"
	EvictionMinimumReclaimInvalidPercentageReason       = "EvictionMinimumReclaimInvalidPercentage"
	EvictionMinimumReclaimInvalidPercentageFormatReason = "EvictionMinimumReclaimInvalidPercentageFormat"
	EvictionMinimumReclaimPercentageOutOfRangeReason    = "EvictionMinimumReclaimPercentageOutOfRange"
)

// Messages for Crd conditions.
const (
	CrdHealthyMessage                                    = "Crd is healthy."
	NapCannotBeEnabledMessage                            = "NAP is not enabled for the cluster but Crd is configured with NAP enabled."
	CrdNotHealthyMessage                                 = "Crd is not healthy."
	NodePoolNotExistMessage                              = "Nodepool %v specified by name has autoscaling disabled or doesn't exist in the cluster."
	LocationNotEnabledForAutoprovisioningMessage         = "Location %v specified in zonal preferences is not eligible for autoprovisioning. Available locations: %v"
	UnavailableMachineTypeMessage                        = "Machine type %s is not available in any of auto provisioned zones, last zone check error: %v"
	NoRuleMatchingMessage                                = "Nodepool %v has an Crd label but doesn't match any priority rule."
	NapDisabledAndNoMatchingNodegroupsMessage            = "Crd with NAP disabled should have at least one matching nodepool with autoscaling enabled."
	CrdLabelNotMatchingMessage                           = "Crd label doesn't match Crd name for the nodepool %v."
	MultipleCrdTaintsMessage                             = "Nodepool %v has more than one crd taints."
	TaintMissingMessage                                  = "Crd taint is missing for the nodepool %v."
	TaintValueNotMatchingMessage                         = "Crd taint value doesn't match Crd name for the nodepool %v."
	MachineFamilyNotFoundMessage                         = "Machine family %v is not a known family."
	MachineTypeNotFoundMessage                           = "Machine type %v is not a known type."
	NoSuitableMachineExistsMessage                       = "Machine type in the given machine family with at least %v cpu and %v memory doesn't exist."
	NodepoolWillNeverScaleUpMessage                      = "Nodepool %v doesn't match any priority rule and Crd is configured to not scale up in that case"
	InvalidGpuTypeMessage                                = "GPU type %v is not supported."
	InvalidGpuConfigurationMessage                       = "GPU type %v with count %v is not supported."
	GpuNotSupportedWithMachineTypeMessage                = "GPU type %v with count %v is not supported with machine type %v."
	GpuNotSupportedWithMachineFamilyMessage              = "GPU type %v with count %v is not supported with machine family %v."
	GpuNotSupportedWithCpuMessage                        = "GPU type %v with count %v is not supported with min CPU requirements %v. Max CPUs supported in a single machine with the given GPU count are %v."
	DiskTypeNotSupportedWithMachineTypeMessage           = "Disk type %s is not supported with machine type %s."
	DiskTypeNotSupportedWithMachineFamilyMessage         = "Disk type %s is not supported with machine family %s."
	LocalSSDNotSupportedWithMachineTypeMessage           = "Local SSD count %d is not supported with machine type %s."
	LocalSSDNotSupportedWithMachineFamilyMessage         = "Local SSD count %d is not supported with machine family %s."
	LocalSSDIncompatibleWithCpuMessage                   = "No machine exists in machine family %s with Local SSD count %d and min CPU cores of %d."
	LocalSSDAllowedCountsMessage                         = "Allowed counts are - [%v]."
	ReservationNotFoundMessage                           = "Reservation %q either does not exist or not accessible."
	ReservationUnusableMessageWithReason                 = "Reservation %q is incompatible with priority rule configuration (%v)."
	ReservationBlockUnusableMessage                      = "Reservation block %s in reservation %q is incompatible with priority rule configuration."
	UnsupportedEvictionConfigFormatMessage               = "Unsupported eviction config format: %s."
	UnsupportedSysctlsFormatMessage                      = "Sysctl setting %q with value '%s' is not formatted correctly. Required format is %s."
	UnsupportedSysctlsWithMachineMessage                 = "Sysctl setting %q with value '%s' is not supported on low memory machine."
	UnsupportedCpuCFSQuotaPeriodMessage                  = "CPU CFS quota period %s is not in range."
	UnsupportedImageMinimumGcAgeMessage                  = "ImageMinimumGcAge %q must be a positive duration and less than or equal to '2m'."
	UnsupportedImageMaximumGcAgeMessage                  = "ImageMaximumGcAge %q must be a positive duration and greater than imageMinimumGcAge %q."
	UnsupportedAllowedUnsafeSysctlsMessage               = "AllowedUnsafeSysctls contains invalid sysctls: %s. Supported sysctl groups include: kernel.shm*, kernel.msg*, kernel.sem, fs.mqueue.*, net.*"
	UnsupportedMachineFamilyForHugepageSize1g            = "Machine family %s doesn't support 1-gigabyte-sized huge pages."
	UnsupportedDefaultMachineFamilyForHugepageSize1g     = "Default autoprovisioning machine family %s doesn't support 1-gigabyte-sized huge pages."
	UnsupportedMachineTypeForHugepageSize1g              = "Machine type %s doesn't support 1-gigabyte-sized huge pages."
	TotalHugepagesExceedMemoryLimit                      = "Total hugepages exceeds %.1f of total machine memory. Total hugepages requested: %dMB, maximum allocatable hugepages memory in machine type %s is %dMB."
	AllMachinesInMachineFamilyExceedHugepageMemoryLimit  = "Total hugepages exceeds %.1f of total machine memory in all machine types in machine family %s."
	AllMachinesInDefaultFamilyExceedHugepageMemoryLimit  = "Total hugepages exceeds %.1f of total machine memory in all machine types in default autoprovisioning machine family %s."
	InvalidContainerLogSizeMessage                       = "Kubelet config containerLogMaxSize %s is invalid, must be a positive integer with unit Ki, Mi or Gi between 10Mi and 500Mi."
	InvalidContainerTotalLogSizeMessage                  = "Kubelet config total container log size (containerLogMaxSize*containerLogMaxFiles) %.2fGi cannot exceed 1%% of the total storage %dGi. The default boot disk size is %dGi if not specified."
	UnknownMinCpuPlatformMessage                         = "MinCpuPlatform %s is not known."
	MinCpuPlatformIncompatibleWithMachineFamilyMessage   = "MinCpuPlatform %s is not compatible with machine family %s."
	MinCpuPlatformIncompatibleWithMachineTypeMessage     = "MinCpuPlatform %s is not compatible with machine type %s."
	MinCpuPlatformIncompatibleWithGpuMessage             = "MinCpuPlatform %s is not compatible with GPU type %s."
	RmTagValidationMessage                               = "Resource Manager Tag validation failed: %v"
	EvictionSoftMemoryInvalidQuantityMessage             = "KubeletConfig.EvictionSoft: memoryAvailable value %q is not a valid quantity: %v."
	EvictionSoftMemoryTooLowMessage                      = "KubeletConfig.EvictionSoft: memoryAvailable value %q must be greater than or equal to %s."
	EvictionSoftMemoryTooHighMessage                     = "KubeletConfig.EvictionSoft: memoryAvailable value %q must be less than 50%% of the machine's memory capacity (%s for %s)."
	EvictionSoftInvalidPercentageFormatMessage           = "KubeletConfig.%s: %s value %q is not a valid percentage. Expected format: NN%%."
	EvictionSoftPercentageOutOfRangeMessage              = "KubeletConfig.%s: %s value %q is outside the allowed range [%d%%, %d%%]."
	EvictionSoftMissingGracePeriodMessage                = "KubeletConfig.EvictionSoft: %s is set to %q but no corresponding grace period is set in KubeletConfig.EvictionSoftGracePeriod."
	EvictionSoftGracePeriodInvalidDurationMessage        = "KubeletConfig.EvictionSoftGracePeriod: %s value %q is not a valid duration: %v."
	EvictionSoftGracePeriodOutOfRangeMessage             = "KubeletConfig.EvictionSoftGracePeriod: %s value %q must be positive and less than %s."
	EvictionMinimumReclaimInvalidPercentageMessage       = "KubeletConfig.EvictionMinimumReclaim: %s value %q is invalid: %v."
	EvictionMinimumReclaimInvalidPercentageFormatMessage = "KubeletConfig.EvictionMinimumReclaim: %s value %q is not a valid percentage. Expected format: NN%%"
	EvictionMinimumReclaimPercentageOutOfRangeMessage    = "KubeletConfig.EvictionMinimumReclaim: %s value %q is outside the allowed range [0%%, 10%%]."
)

func CrdHealthyCondition() *metav1.Condition {
	return &metav1.Condition{
		Type:               HealthCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             HealthCondition,
		Message:            CrdHealthyMessage,
	}
}

func CrdNotHealthyCondition() *metav1.Condition {
	return &metav1.Condition{
		Type:               HealthCondition,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             HealthCondition,
		Message:            CrdNotHealthyMessage,
	}
}

func NapCannotBeEnabledCondition() *metav1.Condition {
	return &metav1.Condition{
		Type:               CrdMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             NapCannotBeEnabledReason,
		Message:            NapCannotBeEnabledMessage,
	}
}

func NodePoolNotExistCondition(nodepoolName string) *metav1.Condition {
	return &metav1.Condition{
		Type:               CrdMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             NodePoolNotExistReason,
		Message:            fmt.Sprintf(NodePoolNotExistMessage, nodepoolName),
	}
}

func LocationNotEnabledForAutoprovisioningCondition(zone string, availableZones []string) *metav1.Condition {
	return &metav1.Condition{
		Type:               CrdMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             LocationNotEnabledForAutoprovisioningReason,
		Message:            fmt.Sprintf(LocationNotEnabledForAutoprovisioningMessage, zone, availableZones),
	}
}

func UnavailableMachineTypeCondition(machineType string, err error) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             UnavailableMachineTypeReason,
		Message:            fmt.Sprintf(UnavailableMachineTypeMessage, machineType, err),
	}
}

func NoRuleMatchingCondition(nodepoolName string) *metav1.Condition {
	return &metav1.Condition{
		Type:               NodepoolMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             NoRuleMatchingReason,
		Message:            fmt.Sprintf(NoRuleMatchingMessage, nodepoolName),
	}
}

func NapDisabledAndNoMatchingNodegroupsCondition() *metav1.Condition {
	return &metav1.Condition{
		Type:               CrdMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             NapDisabledAndNoMatchingNodegroupsReason,
		Message:            NapDisabledAndNoMatchingNodegroupsMessage,
	}
}

func CrdLabelNotMatchingCondition(nodepoolName string) *metav1.Condition {
	return &metav1.Condition{
		Type:               NodepoolMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             CrdLabelNotMatchingReason,
		Message:            fmt.Sprintf(CrdLabelNotMatchingMessage, nodepoolName),
	}
}

func MultipleCrdTaintsCondition(nodepoolName string) *metav1.Condition {
	return &metav1.Condition{
		Type:               NodepoolMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             MultipleCrdTaintsReason,
		Message:            fmt.Sprintf(MultipleCrdTaintsMessage, nodepoolName),
	}
}

func TaintMissingCondition(nodepoolName string) *metav1.Condition {
	return &metav1.Condition{
		Type:               NodepoolMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             TaintMissingReason,
		Message:            fmt.Sprintf(TaintMissingMessage, nodepoolName),
	}
}

func TaintValueNotMatchingCondition(nodepoolName string) *metav1.Condition {
	return &metav1.Condition{
		Type:               NodepoolMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             TaintValueNotMatchingReason,
		Message:            fmt.Sprintf(TaintValueNotMatchingMessage, nodepoolName),
	}
}

func MachineFamilyNotFoundCondition(machineFamily string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             MachineFamilyNotFoundReason,
		Message:            fmt.Sprintf(MachineFamilyNotFoundMessage, machineFamily),
	}
}

func MachineTypeNotFoundCondition(machineType string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             MachineTypeNotFoundReason,
		Message:            fmt.Sprintf(MachineTypeNotFoundMessage, machineType),
	}
}

func NoSuitableMachineExists(cpu int64, memory int64) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             NoSuitableMachineExistsReason,
		Message:            fmt.Sprintf(NoSuitableMachineExistsMessage, cpu, memory),
	}
}

func NodepoolWillNeverScaleUpCondition(nodepoolName string) *metav1.Condition {
	return &metav1.Condition{
		Type:               NodepoolMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             NodepoolWillNeverScaleUpReason,
		Message:            fmt.Sprintf(NodepoolWillNeverScaleUpMessage, nodepoolName),
	}
}

func InvalidGpuTypeCondition(gpuType string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             NoSuitableMachineExistsReason,
		Message:            fmt.Sprintf(InvalidGpuTypeMessage, gpuType),
	}
}

func InvalidGpuConfigurationCondition(gpuType string, gpuCount int) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             NoSuitableMachineExistsReason,
		Message:            fmt.Sprintf(InvalidGpuConfigurationMessage, gpuType, gpuCount),
	}
}

func GpuNotSupportedWithMachineTypeCondition(gpuType string, gpuCount int, machineType string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             NoSuitableMachineExistsReason,
		Message:            fmt.Sprintf(GpuNotSupportedWithMachineTypeMessage, gpuType, gpuCount, machineType),
	}
}

func GpuNotSupportedWithMachineFamilyCondition(gpuType string, gpuCount int, machineFamily string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             NoSuitableMachineExistsReason,
		Message:            fmt.Sprintf(GpuNotSupportedWithMachineFamilyMessage, gpuType, gpuCount, machineFamily),
	}
}

func GpuNotSupportedWithCpuCondition(gpuType string, gpuCount, minCpu, maxCpuSupported int) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             NoSuitableMachineExistsReason,
		Message:            fmt.Sprintf(GpuNotSupportedWithCpuMessage, gpuType, gpuCount, minCpu, maxCpuSupported),
	}
}

func DiskTypeNotSupportedWithMachineTypeCondition(diskType, machineType string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             NoSuitableMachineExistsReason,
		Message:            fmt.Sprintf(DiskTypeNotSupportedWithMachineTypeMessage, diskType, machineType),
	}
}

func DiskTypeNotSupportedWithMachineFamilyCondition(diskType, machineFamily string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             NoSuitableMachineExistsReason,
		Message:            fmt.Sprintf(DiskTypeNotSupportedWithMachineFamilyMessage, diskType, machineFamily),
	}
}

func LocalSSDNotSupportedWithMachineTypeCondition(localSSDCount int, machineType string, allowedCounts []int) *metav1.Condition {
	message := fmt.Sprintf(LocalSSDNotSupportedWithMachineTypeMessage, localSSDCount, machineType)
	if len(allowedCounts) > 0 {
		message += " " + fmt.Sprintf(LocalSSDAllowedCountsMessage, intSliceToString(allowedCounts))
	}
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             NoSuitableMachineExistsReason,
		Message:            message,
	}
}

func LocalSSDNotSupportedWithMachineFamilyCondition(localSSDCount int, machineFamily string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             NoSuitableMachineExistsReason,
		Message:            fmt.Sprintf(LocalSSDNotSupportedWithMachineFamilyMessage, localSSDCount, machineFamily),
	}
}

func LocalSSDIncompatibleWithCpuCondition(machineFamily string, localSSDCount int, minCpu int) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             NoSuitableMachineExistsReason,
		Message:            fmt.Sprintf(LocalSSDIncompatibleWithCpuMessage, machineFamily, localSSDCount, minCpu),
	}
}

func ReservationNotFoundCondition(name, project string) *metav1.Condition {
	reservationName := formatReservationName(name, project)
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             ReservationNotFoundReason,
		Message:            fmt.Sprintf(ReservationNotFoundMessage, reservationName),
	}
}

func ReservationUnusableWithReasonCondition(name, project string, reason string) *metav1.Condition {
	reservationName := formatReservationName(name, project)
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             ReservationUnusableReason,
		Message:            fmt.Sprintf(ReservationUnusableMessageWithReason, reservationName, reason),
	}
}

func ReservationBlockUnusableCondition(name, project, blockName string) *metav1.Condition {
	reservationName := formatReservationName(name, project)
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             ReservationBlockUnusableReason,
		Message:            fmt.Sprintf(ReservationBlockUnusableMessage, blockName, reservationName),
	}
}

func SysctlsBadFormatCondition(sysctl string, value string, requiredFormat string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             UnsupportedNodeSystemConfigFormatReason,
		Message:            fmt.Sprintf(UnsupportedSysctlsFormatMessage, sysctl, value, requiredFormat),
	}
}

func SysctlsNotSupportedWithMachineCondition(sysctl string, value string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             UnsupportedNodeSystemConfigFormatReason,
		Message:            fmt.Sprintf(UnsupportedSysctlsWithMachineMessage, sysctl, value),
	}
}

func EvictionConfigBadFormatCondition(message string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             UnsupportedNodeSystemConfigFormatReason,
		Message:            message,
	}
}

func CpuCfsQuotaPeriodBadFormatCondition(value string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             UnsupportedNodeSystemConfigFormatReason,
		Message:            fmt.Sprintf(UnsupportedCpuCFSQuotaPeriodMessage, value),
	}
}

func ImageMinimumGcAgeBadFormatCondition(minAge string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             UnsupportedNodeSystemConfigFormatReason,
		Message:            fmt.Sprintf(UnsupportedImageMinimumGcAgeMessage, minAge),
	}
}

func ImageMaximumGcAgeBadFormatCondition(minAge, maxAge string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             UnsupportedNodeSystemConfigFormatReason,
		Message:            fmt.Sprintf(UnsupportedImageMaximumGcAgeMessage, maxAge, minAge),
	}
}

func InvalidContainerLogSizeCondition(value string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             UnsupportedNodeSystemConfigFormatReason,
		Message:            fmt.Sprintf(InvalidContainerLogSizeMessage, value),
	}
}

func InvalidContainerTotalLogSizeCondition(logSize float64, totalStorage, defaultBootDiskSize int64) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             UnsupportedNodeSystemConfigFormatReason,
		Message:            fmt.Sprintf(InvalidContainerTotalLogSizeMessage, logSize, totalStorage, defaultBootDiskSize),
	}
}

func UnsupportedAllowedUnsafeSysctls(value string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             UnsupportedNodeSystemConfigFormatReason,
		Message:            fmt.Sprintf(UnsupportedAllowedUnsafeSysctlsMessage, value),
	}
}

func UnsupportedMachineFamilyForHugepageSize1gCondition(machineFamily string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             UnsupportedNodeSystemConfigFormatReason,
		Message:            fmt.Sprintf(UnsupportedMachineFamilyForHugepageSize1g, machineFamily),
	}
}

func UnsupportedMachineTypeForHugepageSize1gCondition(machineType string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             UnsupportedNodeSystemConfigFormatReason,
		Message:            fmt.Sprintf(UnsupportedMachineTypeForHugepageSize1g, machineType),
	}
}

func UnsupportedDefaultMachineFamilyForHugepageSize1gCondition(machineFamily string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             UnsupportedNodeSystemConfigFormatReason,
		Message:            fmt.Sprintf(UnsupportedDefaultMachineFamilyForHugepageSize1g, machineFamily),
	}
}

func TotalHugepagesExceedMemoryLimitCondition(requested int64, machineType string, allocatable int64, hugepageCap float64) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             UnsupportedNodeSystemConfigFormatReason,
		Message:            fmt.Sprintf(TotalHugepagesExceedMemoryLimit, hugepageCap, requested, machineType, allocatable),
	}
}

func AllMachinesInMachineFamilyExceedHugepageMemoryLimitCondition(machineFamily string, hugepageCap float64) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             UnsupportedNodeSystemConfigFormatReason,
		Message:            fmt.Sprintf(AllMachinesInMachineFamilyExceedHugepageMemoryLimit, hugepageCap, machineFamily),
	}
}

func AllMachinesInDefaultFamilyExceedHugepageMemoryLimitCondition(machineFamily string, hugepageCap float64) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             UnsupportedNodeSystemConfigFormatReason,
		Message:            fmt.Sprintf(AllMachinesInDefaultFamilyExceedHugepageMemoryLimit, hugepageCap, machineFamily),
	}
}

func UnknownMinCpuPlatformCondition(minCpuPlatformName string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             UnknownMinCpuPlatformReason,
		Message:            fmt.Sprintf(UnknownMinCpuPlatformMessage, minCpuPlatformName),
	}
}

func MinCpuPlatformIncompatibleWithMachineFamilyCondition(minCpuPlatformName string, machineFamily string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             MinCpuPlatformIncompatibleReason,
		Message:            fmt.Sprintf(MinCpuPlatformIncompatibleWithMachineFamilyMessage, minCpuPlatformName, machineFamily),
	}
}

func MinCpuPlatformIncompatibleWithMachineTypeCondition(minCpuPlatformName string, machineType string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             MinCpuPlatformIncompatibleReason,
		Message:            fmt.Sprintf(MinCpuPlatformIncompatibleWithMachineTypeMessage, minCpuPlatformName, machineType),
	}
}

func MinCpuPlatformIncompatibleWithGpuCondition(minCpuPlatformName string, gpuType string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             MinCpuPlatformIncompatibleReason,
		Message:            fmt.Sprintf(MinCpuPlatformIncompatibleWithGpuMessage, minCpuPlatformName, gpuType),
	}
}

func ResourceManagerValidationCondition(msg string) *metav1.Condition {
	return &metav1.Condition{
		Type:               CrdMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             RmTagValidationReason,
		Message:            fmt.Sprintf(RmTagValidationMessage, msg),
	}
}

func formatReservationName(name, project string) string {
	if project == "" {
		return name
	}

	return fmt.Sprintf("%s/%s", project, name)
}

func intSliceToString(nums []int) string {
	strCounts := []string{}
	for _, c := range nums {
		strCounts = append(strCounts, strconv.Itoa(c))
	}
	return strings.Join(strCounts, ", ")
}

func EvictionSoftMemoryInvalidQuantityCondition(value string, err error) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             EvictionSoftMemoryInvalidQuantityReason,
		Message:            fmt.Sprintf(EvictionSoftMemoryInvalidQuantityMessage, value, err),
	}
}

func EvictionSoftMemoryTooLowCondition(value string, minVal string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             EvictionSoftMemoryTooLowReason,
		Message:            fmt.Sprintf(EvictionSoftMemoryTooLowMessage, value, minVal),
	}
}

func EvictionSoftMemoryTooHighCondition(value string, halfMemory string, machineType string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             EvictionSoftMemoryTooHighReason,
		Message:            fmt.Sprintf(EvictionSoftMemoryTooHighMessage, value, halfMemory, machineType),
	}
}

func EvictionSoftInvalidPercentageFormatCondition(configType, field, value string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             EvictionSoftInvalidPercentageFormatReason,
		Message:            fmt.Sprintf(EvictionSoftInvalidPercentageFormatMessage, configType, field, value),
	}
}

func EvictionSoftPercentageOutOfRangeCondition(configType, field, value string, min, max int) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             EvictionSoftPercentageOutOfRangeReason,
		Message:            fmt.Sprintf(EvictionSoftPercentageOutOfRangeMessage, configType, field, value, min, max),
	}
}

func EvictionSoftMissingGracePeriodCondition(field, value string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             EvictionSoftMissingGracePeriodReason,
		Message:            fmt.Sprintf(EvictionSoftMissingGracePeriodMessage, field, value),
	}
}

func EvictionSoftGracePeriodInvalidDurationCondition(field, value string, err error) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             EvictionSoftGracePeriodInvalidDurationReason,
		Message:            fmt.Sprintf(EvictionSoftGracePeriodInvalidDurationMessage, field, value, err),
	}
}

func EvictionSoftGracePeriodOutOfRangeCondition(field, value string, maxDur string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             EvictionSoftGracePeriodOutOfRangeReason,
		Message:            fmt.Sprintf(EvictionSoftGracePeriodOutOfRangeMessage, field, value, maxDur),
	}
}

func EvictionMinimumReclaimInvalidPercentageCondition(field, value string, err error) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             EvictionMinimumReclaimInvalidPercentageReason,
		Message:            fmt.Sprintf(EvictionMinimumReclaimInvalidPercentageMessage, field, value, err),
	}
}

func EvictionMinimumReclaimInvalidPercentageFormatCondition(field, value string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             EvictionMinimumReclaimInvalidPercentageFormatReason,
		Message:            fmt.Sprintf(EvictionMinimumReclaimInvalidPercentageFormatMessage, field, value),
	}
}

func EvictionMinimumReclaimPercentageOutOfRangeCondition(field, value string) *metav1.Condition {
	return &metav1.Condition{
		Type:               RuleMisconfiguredCondition,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             EvictionMinimumReclaimPercentageOutOfRangeReason,
		Message:            fmt.Sprintf(EvictionMinimumReclaimPercentageOutOfRangeMessage, field, value),
	}
}
