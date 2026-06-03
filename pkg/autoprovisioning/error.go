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

package autoprovisioning

import (
	"fmt"
	"sort"

	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"

	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

const (
	// GpuTypeNotSupportedError - an unsupported/unknown GPU type is requested.
	GpuTypeNotSupportedError errors.AutoscalerErrorType = "gpuTypeNotSupported"
	// GpuTypeNoLimitDefinedError - a GPU type without a limit defined is requested.
	GpuTypeNoLimitDefinedError errors.AutoscalerErrorType = "gpuTypeNoLimitDefined"
	// GpuRequestInvalidError - a pod's GPU request is invalid by itself (or in combination with the CPU request) in some way.
	GpuRequestInvalidError errors.AutoscalerErrorType = "gpuRequestInvalid"
	// GpuFailingPredicatesError - a pod doesn't pass scheduler predicates for any of the GPU node groups that NAP would inject - it's probably misconfigured in some way.
	GpuFailingPredicatesError errors.AutoscalerErrorType = "gpuRequestFailingPredicatesError"
	// TpuTypeNotSupportedError - an unsupported/unknown TPU type is requested.
	TpuTypeNotSupportedError errors.AutoscalerErrorType = "tpuTypeNotSupported"
	// TpuTypeNoLimitDefinedError - a TPU type without a limit defined is requested.
	TpuTypeNoLimitDefinedError errors.AutoscalerErrorType = "tpuTypeNoLimitDefined"
	// TpuTypeInvalidAcceleratorCount - an unsupported TPU accelerator count is requested.
	TpuTypeInvalidAcceleratorCount errors.AutoscalerErrorType = "tpuAcceleratorCountInvalid"
	// InvalidExtendedDurationPodCPUReqError means that a pod has an invalid extended duration config.
	InvalidExtendedDurationPodCPUReqError errors.AutoscalerErrorType = "invalidExtendedDurationPodCPUReqError"
	// ExtendedDurationPodNonAutopilotError means that an extended duration pod was created in non Autopilot cluster.
	ExtendedDurationPodNonAutopilotError errors.AutoscalerErrorType = "extendedDurationPodNonAutopilotError"

	// ComputeClassNotFoundError means that pod requested non-existing ComputeClass.
	ComputeClassNotFoundError errors.AutoscalerErrorType = "ComputeClassNotFoundError"

	// ComputeClassFetchingError means that NAP failed to fetch a ComputeClass.
	ComputeClassFetchingError errors.AutoscalerErrorType = "ComputeClassFetchingError"

	// ComputeClassAutoprovisioningDisabled means that NAP was disabled for given ComputeClass
	ComputeClassAutoprovisioningDisabled errors.AutoscalerErrorType = "ComputeClassAutoprovisioningDisabled"

	// ComputeClassPodIncompatibleError means that pod is incompatible with ComputeClass it requested.
	ComputeClassPodIncompatibleError errors.AutoscalerErrorType = "ComputeClassPodIncompatibleError"

	// ComputeClassPodMultipleDefinitionsError means that pod has multiple ComputeClasses configured.
	ComputeClassPodMultipleDefinitionsError errors.AutoscalerErrorType = "ComputeClassPodMultipleDefinitionsError"

	// InvalidIsolatedPodCPUReqError means that a pod has an invalid isolated cpu request config.
	InvalidIsolatedPodCPUReqError errors.AutoscalerErrorType = "invalidIsolatedPodCPUReqError"
	// IsolatedPodNonAutopilotError means that an isolated pod was created in non Autopilot cluster.
	IsolatedPodNonAutopilotError errors.AutoscalerErrorType = "isolatedPodNonAutopilotError"
	// IsolatedPodCapacityError means that an isolated pod was created with invalid pod capacity requested.
	IsolatedPodCapacityError errors.AutoscalerErrorType = "isolatedPodCapacityError"

	// LocalSSDNotSupportedForMachineTypeError means that a pod is requesting Local SSD ephemeral storage for machine type that doesn't support it.
	LocalSSDNotSupportedForMachineTypeError errors.AutoscalerErrorType = "localSSDNotSupportedForMachineTypeError"

	// InvalidLocalSSDCountForMachineTypeError means the local ssd count for machine type is invalid.
	InvalidLocalSSDCountForMachineTypeError errors.AutoscalerErrorType = "invalidLocalSSDCountError"

	// InvalidLocalSSDCountForMachineTypeError means the local ssd count for the reservation is invalid.
	InvalidLocalSSDCountForReservationError errors.AutoscalerErrorType = "invalidLocalSSDCountForReservationError"

	// FlexStartMisconfiguredError means there were incompatible/missing node selectors present in pod requesting Flex Start provisioning model.
	FlexStartMisconfiguredError errors.AutoscalerErrorType = "FlexStartMisconfiguredError"

	// InvalidConfidentialNodeTypeError means the confidential node type is invalid
	InvalidConfidentialNodeTypeError errors.AutoscalerErrorType = "InvalidConfidentialNodeTypeError"

	// InvalidMachineFamilyForConfidentialNodeTypeError means the machine family is invalid for the confidential node type
	InvalidMachineFamilyForConfidentialNodeTypeError errors.AutoscalerErrorType = "InvalidMachineFamilyForConfidentialNodeTypeError"

	// MachineFamiliesDoNotSupportDwsError means the machine families associated with requested accelerator are disabled for DWS
	MachineFamiliesDoNotSupportDwsError errors.AutoscalerErrorType = "MachineFamiliesDoNotSupportDwsError"
)

// Error represents an error encountered in the autoprovisioning package.
type Error struct {
	ErrType                                           errors.AutoscalerErrorType
	Prefix                                            string
	GpuType                                           string
	TpuType                                           string
	GpuRequestInvalidReason                           string
	GpuPredicateFailureReasons                        []string
	ExtendedDurationCPUReq                            string
	ComputeClassName                                  string
	ComputeClassType                                  string
	ComputeClassNotFoundReason                        string
	IsolatedPodCPUReq                                 string
	IsolatedPodCapacity                               string
	MachineType                                       string
	MachineFamilies                                   []string
	LocalSSDCount                                     int
	AllowedLocalSSDCounts                             []int
	AcceleratorCount                                  int
	FlexStartMisconfiguredReason                      string
	ConfidentialNodeType                              string
	InvalidMachineFamilyForConfidentialNodeTypeReason string
	ReservationName                                   string
	ReservationLocalSSDCount                          int
}

// Type returns the type of the error.
func (e *Error) Type() errors.AutoscalerErrorType {
	return e.ErrType
}

// AddPrefix adds a prefix to the error message, without changing the error type.
func (e *Error) AddPrefix(msg string, args ...any) errors.AutoscalerError {
	e.Prefix = fmt.Sprintf(msg, args...) + e.Prefix
	return e
}

// Error returns an error message applicable for the error type.
func (e *Error) Error() string {
	msg := ""
	switch e.ErrType {
	case GpuTypeNotSupportedError:
		msg = fmt.Sprintf("GPU type %q not supported", e.GpuType)
	case GpuTypeNoLimitDefinedError:
		msg = fmt.Sprintf("GPU type %q doesn't have a limit defined", e.GpuType)
	case GpuRequestInvalidError:
		msg = fmt.Sprintf("GPU request is invalid: %s", e.GpuRequestInvalidReason)
	case GpuFailingPredicatesError:
		msg = fmt.Sprintf("pod requesting GPU doesn't pass scheduler predicates on any GPU node group considered by NAP, reasons: %v", e.GpuPredicateFailureReasons)
	case TpuTypeNotSupportedError:
		msg = fmt.Sprintf("TPU type %q not supported", e.TpuType)
	case TpuTypeNoLimitDefinedError:
		msg = fmt.Sprintf("TPU type %q doesn't have a limit defined", e.TpuType)
	case TpuTypeInvalidAcceleratorCount:
		msg = fmt.Sprintf("Accelerator count %v is not supported for TPU type %s", e.AcceleratorCount, e.TpuType)
	case InvalidExtendedDurationPodCPUReqError:
		msg = fmt.Sprintf("incorrect extended duration pod cpu request value - should be kubernetes quantity, got: %s", e.ExtendedDurationCPUReq)
	case ExtendedDurationPodNonAutopilotError:
		msg = "extended duration pods are only available in autopilot clusters"
	case ComputeClassNotFoundError:
		msg = fmt.Sprintf("ComputeClass %s of type %s not found", e.ComputeClassName, e.ComputeClassType)
	case ComputeClassFetchingError:
		msg = fmt.Sprintf("Failed to fetch ComputeClass %s of type %s", e.ComputeClassName, e.ComputeClassType)
	case ComputeClassAutoprovisioningDisabled:
		msg = fmt.Sprintf("NAP disabled for ComputeClass %s of type %s", e.ComputeClassName, e.ComputeClassType)
	case ComputeClassPodIncompatibleError:
		msg = fmt.Sprintf("Pod incompatible with requested ComputeClass %s of type %s", e.ComputeClassName, e.ComputeClassType)
	case ComputeClassPodMultipleDefinitionsError:
		msg = "Pod has multiple ComputeClasses defined"
	case InvalidIsolatedPodCPUReqError:
		msg = fmt.Sprintf("incorrect %s pod cpu request value - should be kubernetes quantity, got: %s", gkelabels.PodPerVMSizeLabel, e.IsolatedPodCPUReq)
	case IsolatedPodNonAutopilotError:
		msg = "Pod per VM pods are only available in autopilot clusters"
	case IsolatedPodCapacityError:
		msg = fmt.Sprintf("%s must have integer value requested, got: %s", gkelabels.PodCapacityLabel, e.IsolatedPodCapacity)
	case LocalSSDNotSupportedForMachineTypeError:
		msg = fmt.Sprintf("LocalSSD is unsupported for given machineType: %s", e.MachineType)
	case InvalidLocalSSDCountForMachineTypeError:
		msg = fmt.Sprintf("Invalid localSSD count: %d for given machineType: %s, possible local ssd counts: %v", e.LocalSSDCount, e.MachineType, e.AllowedLocalSSDCounts)
	case InvalidLocalSSDCountForReservationError:
		msg = fmt.Sprintf("Reservation %s does not have enough local SSDs, reservation has %d, need %d", e.ReservationName, e.ReservationLocalSSDCount, e.LocalSSDCount)
	case FlexStartMisconfiguredError:
		msg = fmt.Sprintf("pod has misconfigured Flex Start selectors: %s", e.FlexStartMisconfiguredReason)
	case InvalidConfidentialNodeTypeError:
		msg = fmt.Sprintf("Invalid confidential node type: %s", e.ConfidentialNodeType)
	case MachineFamiliesDoNotSupportDwsError:
		msg = fmt.Sprintf("Invalid DWS config, pod requires one of machine families: '%s', but they do not support DWS", e.MachineFamilies)
	case InvalidMachineFamilyForConfidentialNodeTypeError:
		msg = fmt.Sprintf("pod has misconfigured confidential node type selectors: %s", e.InvalidMachineFamilyForConfidentialNodeTypeReason)
	default:
		return fmt.Sprintf("autoprovisioning.Error type %q unknown - this shouldn't happen", e.ErrType)
	}
	return e.Prefix + msg
}

// NewGpuTypeNotSupportedError creates a specific error type.
func NewGpuTypeNotSupportedError(gpuType string) errors.AutoscalerError {
	return &Error{ErrType: GpuTypeNotSupportedError, GpuType: gpuType}
}

// NewGpuTypeNoLimitDefinedError creates a specific error type.
func NewGpuTypeNoLimitDefinedError(gpuType string) errors.AutoscalerError {
	return &Error{ErrType: GpuTypeNoLimitDefinedError, GpuType: gpuType}
}

// NewGpuRequestInvalidError creates a specific error type.
func NewGpuRequestInvalidError(reason string) errors.AutoscalerError {
	return &Error{ErrType: GpuRequestInvalidError, GpuRequestInvalidReason: reason}
}

// NewGpuRequestFailingPredicatesError creates a specific error type.
func NewGpuRequestFailingPredicatesError(predicateFailureReasons []string) errors.AutoscalerError {
	// Remove duplicate reasons and sort for readable and repeatable error messages.
	reasonsSet := map[string]bool{}
	for _, reason := range predicateFailureReasons {
		reasonsSet[reason] = true
	}
	var dedupedReasons []string
	for reason := range reasonsSet {
		dedupedReasons = append(dedupedReasons, reason)
	}
	sort.Strings(dedupedReasons)
	return &Error{ErrType: GpuFailingPredicatesError, GpuPredicateFailureReasons: dedupedReasons}
}

// NewTpuTypeNotSupportedError creates a specific error type.
func NewTpuTypeNotSupportedError(tpuType string) errors.AutoscalerError {
	return &Error{ErrType: TpuTypeNotSupportedError, TpuType: tpuType}
}

// NewTpuTypeNoLimitDefinedError creates a specific error type.
func NewTpuTypeNoLimitDefinedError(tpuType string) errors.AutoscalerError {
	return &Error{ErrType: TpuTypeNoLimitDefinedError, TpuType: tpuType}
}

// NewTpuTypeInvalidAcceleratorCount creates a specific error type.
func NewTpuTypeInvalidAcceleratorCount(tpuType string, acceleratorCount int) errors.AutoscalerError {
	return &Error{ErrType: TpuTypeInvalidAcceleratorCount, TpuType: tpuType, AcceleratorCount: acceleratorCount}
}

// NewInvalidExtendedDurationPodCPUReq is an instance of AutoscalerError with InvalidExtendedDurationPodCPUReqError
// caused by incorrect extended duration label value - it should be parsable to kubernetes quantity,
func NewInvalidExtendedDurationPodCPUReq(cpuReq string) errors.AutoscalerError {
	return &Error{ErrType: InvalidExtendedDurationPodCPUReqError, ExtendedDurationCPUReq: cpuReq}
}

// NewExtendedDurationPodNonAutopilotError is an instance of AutoscalerError with ExtendedDurationPodNonAutopilotError
// caused by extended duration pod being created in non autopilot cluster.
func NewExtendedDurationPodNonAutopilotError() errors.AutoscalerError {
	return &Error{ErrType: ExtendedDurationPodNonAutopilotError}
}

// NewComputeClassNotFoundError is an instance of AutoscalerError with ComputeClassNotFoundError
// caused by pod requesting non-existing ComputeClass.
func NewComputeClassNotFoundError(ccName, ccType string, reason error) errors.AutoscalerError {
	if reason != nil {
		return &Error{ErrType: ComputeClassNotFoundError, ComputeClassName: ccName, ComputeClassType: ccType, ComputeClassNotFoundReason: reason.Error()}
	}
	return &Error{ErrType: ComputeClassNotFoundError, ComputeClassName: ccName, ComputeClassType: ccType}
}

// NewComputeClassFetchingError is an instance of AutoscalerError with ComputeClassFetchingError
// caused by CA failing to fetch ComputeClass.
func NewComputeClassFetchingError(ccName, ccType string) errors.AutoscalerError {
	return &Error{ErrType: ComputeClassFetchingError, ComputeClassName: ccName, ComputeClassType: ccType}
}

// NewComputeClassAutoprovisioningDisabled is an instance of AutoscalerError with
// ComputeClassAutoprovisioningDisabled disabled NAP.
func NewComputeClassAutoprovisioningDisabled(ccName, ccType string) errors.AutoscalerError {
	return &Error{ErrType: ComputeClassAutoprovisioningDisabled, ComputeClassName: ccName, ComputeClassType: ccType}
}

// NewComputeClassPodIncompatibleError is an instance of AutoscalerError with ComputeClassPodIncompatibleError
// caused by pod requesting ComputeClass that it is incompatible with.
func NewComputeClassPodIncompatibleError(ccName, ccType string) errors.AutoscalerError {
	return &Error{ErrType: ComputeClassPodIncompatibleError, ComputeClassName: ccName, ComputeClassType: ccType}
}

// NewComputeClassPodMultipleDefinitionsError is an instance of AutoscalerError with ComputeClassPodMultipleDefinitionsError
// caused by pod having multiple ComputeClasses configured.
func NewComputeClassPodMultipleDefinitionsError() errors.AutoscalerError {
	return &Error{ErrType: ComputeClassPodMultipleDefinitionsError}
}

// NewInvalidIsolatedPodCPUReq is an instance of AutoscalerError with InvalidIsolatedPodCPUReqError
// caused by incorrect pod isolation label value - it should be parsable to kubernetes quantity,
func NewInvalidIsolatedPodCPUReq(cpuReq string) errors.AutoscalerError {
	return &Error{ErrType: InvalidIsolatedPodCPUReqError, IsolatedPodCPUReq: cpuReq}
}

// NewIsolatedPodNonAutopilotError is an instance of AutoscalerError with IsolatedPodNonAutopilotError
// caused by an isolated pod being created in non autopilot cluster.
func NewIsolatedPodNonAutopilotError() errors.AutoscalerError {
	return &Error{ErrType: IsolatedPodNonAutopilotError}
}

// NewIsolatedPodCapacityError is an instance of AutoscalerError with IsolatedPodCapacityError
// caused by an isolated pod being created with an invalid count.
func NewIsolatedPodCapacityError(capacity string) errors.AutoscalerError {
	return &Error{ErrType: IsolatedPodCapacityError, IsolatedPodCapacity: capacity}
}

// NewLocalSSDNotSupportedForMachineTypeError is an instance of AutoscalerError with LocalSSDNotSupportedForMachineTypeError
// caused by pod requesting LocalSSD ephemeral storage for a machine type that doesn't support it.
func NewLocalSSDNotSupportedForMachineTypeError(machineType string) errors.AutoscalerError {
	return &Error{ErrType: LocalSSDNotSupportedForMachineTypeError, MachineType: machineType}
}

// NewInvalidLocalSSDCountForMachineTypeError is an instance of AutoscalerError with InvalidLocalSSDCountForMachineTypeError
// caused by pod requesting LocalSSD ephemeral storage with an invalid count for a machine type.
func NewInvalidLocalSSDCountForMachineTypeError(machineType string, localSSDCount int, allowedLocalSSDCounts []int) errors.AutoscalerError {
	return &Error{ErrType: InvalidLocalSSDCountForMachineTypeError, MachineType: machineType, LocalSSDCount: localSSDCount, AllowedLocalSSDCounts: allowedLocalSSDCounts}
}

// NewInvalidLocalSSDCountForReservationError is an instance of AutoscalerError with InvalidLocalSSDCountForReservationError
// caused by pod requesting local SSDs and reservation, but the reservation does not have sufficient number of local SSDs.
func NewInvalidLocalSSDCountForReservationError(reservationName string, reservationLocalSSDCount int, localSSDCount int) errors.AutoscalerError {
	return &Error{ErrType: InvalidLocalSSDCountForReservationError, ReservationName: reservationName, ReservationLocalSSDCount: reservationLocalSSDCount, LocalSSDCount: localSSDCount}
}

// NewFlexStartMisconfiguredError is an instance of AutoscalerError with FlexStartMisconfiguredError
// caused by misconfigured pod Flex Start node selectors.
func NewFlexStartMisconfiguredError(reason string) errors.AutoscalerError {
	return &Error{ErrType: FlexStartMisconfiguredError, FlexStartMisconfiguredReason: reason}
}

func NewInvalidConfidentialNodeTypeError(confidentialNodeType string) errors.AutoscalerError {
	return &Error{ErrType: InvalidConfidentialNodeTypeError, ConfidentialNodeType: confidentialNodeType}
}

func NewInvalidMachineFamilyForConfidentialNodeTypeError(reason string) errors.AutoscalerError {
	return &Error{ErrType: InvalidMachineFamilyForConfidentialNodeTypeError, InvalidMachineFamilyForConfidentialNodeTypeReason: reason}
}

// NewInvalidDwsMachineFamilyError is an instance of AutoscalerError with MachineFamiliesDoNotSupportDwsError
// caused by asking for an accelerator associated with a machine family that has DWS disabled.
func NewInvalidDwsMachineFamilyError(machineFamilies []string) errors.AutoscalerError {
	return &Error{ErrType: MachineFamiliesDoNotSupportDwsError, MachineFamilies: machineFamilies}
}
