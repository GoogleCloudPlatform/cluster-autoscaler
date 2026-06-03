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

package machineselection

import (
	"fmt"

	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
)

const (
	// MachineFamilyUnknownError - an unknown machine family is specified.
	MachineFamilyUnknownError errors.AutoscalerErrorType = "machineFamilyUnknown"
	// MachineFamilyNotSupportedError - a machine family is not supported in NAP.
	MachineFamilyNotSupportedError errors.AutoscalerErrorType = "machineFamilyNotSupported"
	// PodFamilyUnknownError - an unknown pod family is specified.
	PodFamilyUnknownError errors.AutoscalerErrorType = "podFamilyUnknown"
	// ComputeClassNonAutopilotError - a compute class is specified in non-autopilot cluster.
	ComputeClassNonAutopilotError errors.AutoscalerErrorType = "computeClassNonAutopilot"
	// ComputeClassWithMachineFamilyError - a compute class is specified with machine family.
	ComputeClassWithMachineFamilyError errors.AutoscalerErrorType = "computeClassWithMachineFamily"
	// ComputeClassWithInvalidMachineFamilyError - a compute class is specified with unsupported machine family.
	ComputeClassWithInvalidMachineFamilyError errors.AutoscalerErrorType = "computeClassWithInvalidMachineFamily"
	// ComputeClassWithoutMachineFamilyError - a compute class is specified without family.
	ComputeClassWithoutMachineFamilyError errors.AutoscalerErrorType = "computeClassWithoutMachineFamily"
	// ComputeClassWithoutAcceleratorError - a compute class is specified without accelerator (gpu,tpu) specified.
	ComputeClassWithoutAcceleratorError errors.AutoscalerErrorType = "computeClassWithoutAccelerator"
	// ConfidentialNodesIncompatibleError - Confidential Nodes are enabled, but the machines can't be used with it.
	ConfidentialNodesIncompatibleError errors.AutoscalerErrorType = "confidentialNodesIncompatibleError"
	// GpuIncompatibleError - a GPU is requested, but the machines don't support it.
	GpuIncompatibleError errors.AutoscalerErrorType = "gpuIncompatibleError"
	// GpuMinCpuPlatformIncompatibleError - a GPU is requested, but the min cpu platform doesn't support it.
	GpuMinCpuPlatformIncompatibleError errors.AutoscalerErrorType = "gpuMinCpuPlatformIncompatibleError"
	// TpuIncompatibleError - a TPU is requested, but the machines don't support it.
	TpuIncompatibleError errors.AutoscalerErrorType = "tpuIncompatibleError"
	// SystemArchitectureIncompatibleError - a system architecture requested is not compatible
	SystemArchitectureIncompatibleError errors.AutoscalerErrorType = "systemArchitectureIncompatibleError"
	// MinCpuPlatformInvalidError - a min_cpu_platform is specified, but it's not valid for the machines.
	MinCpuPlatformInvalidError errors.AutoscalerErrorType = "minCpuPlatformInvalidError"
	// MinCpuPlatformUnknownError - an unknown min_cpu_platform is specified.
	MinCpuPlatformUnknownError errors.AutoscalerErrorType = "minCpuPlatformUnknownError"
	// MultipleMinCpuPlatformsError - pod requests multiple min_cpu_platform values.
	MultipleMinCpuPlatformsError errors.AutoscalerErrorType = "multipleMinCpuPlatform"
	// SystemArchitectureUnknownError - an unknown system architecture is specified
	SystemArchitectureUnknownError errors.AutoscalerErrorType = "systemArchitectureUnknown"
	// AutopilotArchNoComputeClassError - arch specified without required compute class in Autopilot.
	AutopilotArchNoComputeClassError errors.AutoscalerErrorType = "AutopilotArchNoComputeClass"
	// MachineConfigInvalidError - the requested machine config is invalid in some way not detected explicitly.
	MachineConfigInvalidError errors.AutoscalerErrorType = "machineTypeConfigNotSupported"
	// MachineTypeNotSupportedError - a machine type is not supported in NAP.
	MachineTypeNotSupportedError errors.AutoscalerErrorType = "machineTypeNotSupported"
	// BootDiskTypeIncompatibleError - a boot disk type is requested, but the machine spec doesn't support it.
	BootDiskTypeIncompatibleError errors.AutoscalerErrorType = "bootDiskTypeIncompatibleError"

	// MachineTypesIncompatibleError - explicit machine types are requested, but are incompatible with machine family.
	MachineTypesIncompatibleError errors.AutoscalerErrorType = "machineTypesIncompatibleError"
)

// Error represents an error encountered while selecting machines.
type Error struct {
	ErrType            errors.AutoscalerErrorType
	Prefix             string
	ComputeClassName   string
	MachineGroupName   string
	MachineTypeName    string
	PodFamilyName      string
	MinCpuPlatformName string
	GpuName            string
	TpuName            string
	SystemArch         string
	MachineConfigDesc  string
	MachineConfigErr   string
	BootDiskType       string
	MachineTypeNames   []string
}

// Error returns an error message applicable for the error type.
func (e *Error) Error() string {
	msg := ""
	switch e.ErrType {
	case MachineFamilyUnknownError:
		msg = fmt.Sprintf("machine family %q unknown", e.MachineGroupName)
	case MachineFamilyNotSupportedError:
		msg = fmt.Sprintf("machine family %q is not supported in NAP", e.MachineGroupName)
	case PodFamilyUnknownError:
		msg = fmt.Sprintf("pod family %q unknown", e.PodFamilyName)
	case ComputeClassNonAutopilotError:
		msg = fmt.Sprintf("compute class %q can be selected only in Autopilot clusters", e.ComputeClassName)
	case ComputeClassWithMachineFamilyError:
		msg = fmt.Sprintf("compute class %q cannot be specified with machine family", e.ComputeClassName)
	case ComputeClassWithInvalidMachineFamilyError:
		msg = fmt.Sprintf("compute class %q specified unsupported machine family %s", e.ComputeClassName, e.MachineGroupName)
	case ComputeClassWithoutMachineFamilyError:
		msg = fmt.Sprintf("compute class %q cannot be specified without machine family", e.ComputeClassName)
	case ComputeClassWithoutAcceleratorError:
		msg = fmt.Sprintf("compute class %q cannot be specified without an accelerator being specified", e.ComputeClassName)
	case ConfidentialNodesIncompatibleError:
		msg = fmt.Sprintf("%s can't be used with Confidential Nodes", e.MachineGroupName)
	case GpuIncompatibleError:
		msg = fmt.Sprintf("GPU %q is not compatible with %s", e.GpuName, e.MachineGroupName)
	case GpuMinCpuPlatformIncompatibleError:
		msg = fmt.Sprintf("GPU %q is not compatible with min_cpu_platform %q", e.GpuName, e.MinCpuPlatformName)
	case TpuIncompatibleError:
		msg = fmt.Sprintf("TPU %q is not compatible with %s", e.TpuName, e.MachineGroupName)
	case SystemArchitectureIncompatibleError:
		msg = fmt.Sprintf("system architecture %q is not compatible with %q", e.SystemArch, e.MachineGroupName)
	case MinCpuPlatformInvalidError:
		msg = fmt.Sprintf("%s doesn't allow setting min_cpu_platform to %q", e.MachineGroupName, e.MinCpuPlatformName)
	case MinCpuPlatformUnknownError:
		msg = fmt.Sprintf("min_cpu_platform %q unknown", e.MinCpuPlatformName)
	case SystemArchitectureUnknownError:
		msg = fmt.Sprintf("system architecture %q unknown", e.SystemArch)
	case AutopilotArchNoComputeClassError:
		msg = fmt.Sprintf("system architecture %q requires compute class to be specified in Autopilot", e.SystemArch)
	case MultipleMinCpuPlatformsError:
		msg = "multiple min_cpu_platform values are requested (only 1 is allowed)"
	case MachineConfigInvalidError:
		msg = fmt.Sprintf("machine config <%s> is invalid: %s", e.MachineConfigDesc, e.MachineConfigErr)
	case MachineTypeNotSupportedError:
		msg = fmt.Sprintf("machine type %s is not supported", e.MachineTypeName)
	case BootDiskTypeIncompatibleError:
		msg = fmt.Sprintf("Boot disk type %q is not compatible with %s", e.BootDiskType, e.MachineGroupName)
	case MachineTypesIncompatibleError:
		msg = fmt.Sprintf("Machine types %v are not compatible with %s", e.MachineTypeNames, e.MachineGroupName)
	default:
		return fmt.Sprintf("machineselection.Error type %q unknown - this shouldn't happen", e.ErrType)
	}
	return e.Prefix + msg
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

// NewMachineFamilyUnknownError creates a specific error type.
func NewMachineFamilyUnknownError(familyName string) errors.AutoscalerError {
	return &Error{ErrType: MachineFamilyUnknownError, MachineGroupName: familyName}
}

// NewMachineFamilyNotSupportedError creates a specific error type.
func NewMachineFamilyNotSupportedError(familyName string) errors.AutoscalerError {
	return &Error{ErrType: MachineFamilyNotSupportedError, MachineGroupName: familyName}
}

// NewPodFamilyUnknownError creates a specific error type.
func NewPodFamilyUnknownError(podFamilyName string) errors.AutoscalerError {
	return &Error{ErrType: PodFamilyUnknownError, PodFamilyName: podFamilyName}
}

// NewSystemArchitectureUnknownError creates a specific error type.
func NewSystemArchitectureUnknownError(arch string) errors.AutoscalerError {
	return &Error{ErrType: SystemArchitectureUnknownError, SystemArch: arch}
}

// NewComputeClassNonAutopilotError creates a specific error type.
func NewComputeClassNonAutopilotError(className string) errors.AutoscalerError {
	return &Error{ErrType: ComputeClassNonAutopilotError, ComputeClassName: className}
}

// NewComputeClassWithMachineFamilyError creates a specific error type.
func NewComputeClassWithMachineFamilyError(className string) errors.AutoscalerError {
	return &Error{ErrType: ComputeClassWithMachineFamilyError, ComputeClassName: className}
}

// NewComputeClassWithInvalidMachineFamilyError creates a specific error type.
func NewComputeClassWithInvalidMachineFamilyError(className, machineFamily string) errors.AutoscalerError {
	return &Error{ErrType: ComputeClassWithInvalidMachineFamilyError, ComputeClassName: className, MachineGroupName: machineFamily}
}

// NewComputeClassWithoutMachineFamilyError creates a specific error type.
func NewComputeClassWithoutMachineFamilyError(className string) errors.AutoscalerError {
	return &Error{ErrType: ComputeClassWithoutMachineFamilyError, ComputeClassName: className}
}

// NewComputeClassWithoutAcceleratorError creates a specific error type.
func NewComputeClassWithoutAcceleratorError(className string) errors.AutoscalerError {
	return &Error{ErrType: ComputeClassWithoutAcceleratorError, ComputeClassName: className}
}

// NewConfidentialNodesIncompatibleError creates a specific error type.
func NewConfidentialNodesIncompatibleError(machineGroupName string) errors.AutoscalerError {
	return &Error{ErrType: ConfidentialNodesIncompatibleError, MachineGroupName: machineGroupName}
}

// NewGpuIncompatibleError creates a specific error type.
func NewGpuIncompatibleError(machineGroupName, gpuName string) errors.AutoscalerError {
	return &Error{ErrType: GpuIncompatibleError, MachineGroupName: machineGroupName, GpuName: gpuName}
}

// NewGpuMinCpuPlatformIncompatibleError creates a specific error type.
func NewGpuMinCpuPlatformIncompatibleError(minCpuPlatformName, gpuName string) errors.AutoscalerError {
	return &Error{ErrType: GpuMinCpuPlatformIncompatibleError, MinCpuPlatformName: minCpuPlatformName, GpuName: gpuName}
}

// NewTpuIncompatibleError creates a specific error type.
func NewTpuIncompatibleError(machineGroupName, tpuName string) errors.AutoscalerError {
	return &Error{ErrType: TpuIncompatibleError, MachineGroupName: machineGroupName, TpuName: tpuName}
}

// NewSystemArchitectureIncompatibleError creates a specific error type.
func NewSystemArchitectureIncompatibleError(machineGroupName, arch string) errors.AutoscalerError {
	return &Error{ErrType: SystemArchitectureIncompatibleError, MachineGroupName: machineGroupName, SystemArch: arch}
}

// NewMinCpuPlatformInvalidError creates a specific error type.
func NewMinCpuPlatformInvalidError(machineGroupName, minCpuPlatformName string) errors.AutoscalerError {
	return &Error{ErrType: MinCpuPlatformInvalidError, MachineGroupName: machineGroupName, MinCpuPlatformName: minCpuPlatformName}
}

// NewMinCpuPlatformUnknownError creates a specific error type.
func NewMinCpuPlatformUnknownError(minCpuPlatformName string) errors.AutoscalerError {
	return &Error{ErrType: MinCpuPlatformUnknownError, MinCpuPlatformName: minCpuPlatformName}
}

// NewMultipleMinCpuPlatformsError creates a specific error type.
func NewMultipleMinCpuPlatformsError() errors.AutoscalerError {
	return &Error{ErrType: MultipleMinCpuPlatformsError}
}

// NewAutopilotArchNoComputeClassError creates a specific error type.
func NewAutopilotArchNoComputeClassError(arch string) errors.AutoscalerError {
	return &Error{ErrType: AutopilotArchNoComputeClassError, SystemArch: arch}
}

// NewMachineConfigInvalidError creates a specific error type.
func NewMachineConfigInvalidError(configDesc, errMsg string) errors.AutoscalerError {
	return &Error{ErrType: MachineConfigInvalidError, MachineConfigDesc: configDesc, MachineConfigErr: errMsg}
}

// NewMachineTypeNotSupportedError creates a specific error type.
func NewMachineTypeNotSupportedError(machineType string) errors.AutoscalerError {
	return &Error{ErrType: MachineTypeNotSupportedError, MachineTypeName: machineType}
}

// NewBootDiskTypeIncompatibleError creates a specific error type.
func NewBootDiskTypeIncompatibleError(machineGroupName, bootDiskType string) errors.AutoscalerError {
	return &Error{ErrType: BootDiskTypeIncompatibleError, MachineGroupName: machineGroupName, BootDiskType: bootDiskType}
}

// NewMachineTypesUnsupportedByFamilyError creates a specific error type.
func NewMachineTypesUnsupportedByFamilyError(machineTypes []string, machineGroupName string) errors.AutoscalerError {
	return &Error{ErrType: MachineTypesIncompatibleError, MachineGroupName: machineGroupName, MachineTypeNames: machineTypes}
}
