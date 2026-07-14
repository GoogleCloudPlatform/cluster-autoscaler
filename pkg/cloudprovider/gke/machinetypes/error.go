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
	"fmt"

	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
)

const InvalidMachineSpecError errors.AutoscalerErrorType = "invalidMachineSpec"

// ErrInvalidMachineSpec represents an error caused by pod configuration incompatible
// with compact placement.
type ErrInvalidMachineSpec struct {
	Prefix      string
	MachineSpec string
	Msg         string
}

// Error returns an error message applicable for the error type.
func (e *ErrInvalidMachineSpec) Error() string {
	return e.Prefix + fmt.Sprintf("Invalid configuration for machine spec %q: %s", e.MachineSpec, e.Msg)
}

// Type returns the type of the error.
func (e *ErrInvalidMachineSpec) Type() errors.AutoscalerErrorType {
	return InvalidMachineSpecError
}

// AddPrefix adds a prefix to the error message, without changing the error type.
func (e *ErrInvalidMachineSpec) AddPrefix(msg string, args ...any) errors.AutoscalerError {
	e.Prefix = fmt.Sprintf(msg, args...) + e.Prefix
	return e
}

// NewInvalidMachineFamilyError creates a specific error type.
func NewInvalidMachineSpecError(machineSpec, msg string) errors.AutoscalerError {
	return &ErrInvalidMachineSpec{MachineSpec: machineSpec, Msg: msg}
}
