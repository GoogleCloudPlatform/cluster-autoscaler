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

package podrequirements

import (
	"fmt"

	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
)

// InvalidLabelValueErrorType - an invalid label value is specified
const InvalidLabelValueErrorType errors.AutoscalerErrorType = "invalidLabelValue"

// InvalidWorkloadSeparationError means that a pod has an invalid workload separation config (e.g. a node selector for a
// non-system label, but no toleration for the corresponding taint).
const InvalidWorkloadSeparationError errors.AutoscalerErrorType = "InvalidWorkloadSeparationError"

// ErrInvalidLabelValue represents an error caused by invalid label value
type ErrInvalidLabelValue struct {
	Prefix string
	Label  string
	Value  string
}

// Error returns an error message applicable for the error type.
func (e *ErrInvalidLabelValue) Error() string {
	return e.Prefix + fmt.Sprintf("pod requests an invalid value %q for label %q", e.Value, e.Label)
}

// Type returns the type of the error.
func (e *ErrInvalidLabelValue) Type() errors.AutoscalerErrorType {
	return InvalidLabelValueErrorType
}

// AddPrefix adds a prefix to the error message, without changing the error type.
func (e *ErrInvalidLabelValue) AddPrefix(msg string, args ...any) errors.AutoscalerError {
	e.Prefix = fmt.Sprintf(msg, args...) + e.Prefix
	return e
}

// NewInvalidLabelValueError creates a specific error type.
func NewInvalidLabelValueError(label, value string) errors.AutoscalerError {
	return &ErrInvalidLabelValue{Label: label, Value: value}
}

// ErrInvalidWorkloadSeparation is an instance of AutoscalerError with InvalidWorkloadSeparationError type and an appropriate
// error message.
type ErrInvalidWorkloadSeparation struct {
	Prefix string
	Label  string
}

func (e *ErrInvalidWorkloadSeparation) Error() string {
	return fmt.Sprintf("pod requests an unknown label without a matching toleration: %s", e.Label)
}

func (e *ErrInvalidWorkloadSeparation) Type() errors.AutoscalerErrorType {
	return InvalidWorkloadSeparationError
}

func (e *ErrInvalidWorkloadSeparation) AddPrefix(msg string, args ...interface{}) errors.AutoscalerError {
	e.Prefix = fmt.Sprintf(msg, args...) + e.Prefix
	return e
}

// NewInvalidWorkloadSeparationError creates an invalid workload separation error.
func NewInvalidWorkloadSeparationError(label string) *ErrInvalidWorkloadSeparation {
	return &ErrInvalidWorkloadSeparation{Label: label}
}
