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

package placement

import (
	"fmt"
	"strings"

	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
)

// InvalidPlacementGroupNameError - an invalid label value is specified.
const InvalidPlacementGroupNameError errors.AutoscalerErrorType = "invalidPlacementGroupName"

// InvalidPlacementPolicyError - an invalid placement policy is specified.
const InvalidPlacementPolicyError errors.AutoscalerErrorType = "invalidPlacementPolicy"

// UnsupportedCompactPlacementConfigError - compact placement was requested with unsupported set of options.
const UnsupportedCompactPlacementConfigError errors.AutoscalerErrorType = "unsupportedCompactPlacementConfig"

// NodeGroupAlreadyExistsError - node group requested explicitly already exists.
const NodeGroupAlreadyExistsError errors.AutoscalerErrorType = "nodeGroupAlreadyExists"

// InvalidMachineFamilyError - node group machine family does not support any type of placement.
const InvalidMachineFamilyError errors.AutoscalerErrorType = "invalidMachineFamily"

// ErrInvalidPlacementGroupName represents an error caused by invalid placement group name.
type ErrInvalidPlacementGroupName struct {
	Prefix         string
	PlacementGroup string
	Msg            string
}

// Error returns an error message applicable for the error type.
func (e *ErrInvalidPlacementGroupName) Error() string {
	return e.Prefix + fmt.Sprintf("placement group name %q is invalid, %s", e.PlacementGroup, e.Msg)
}

// Type returns the type of the error.
func (e *ErrInvalidPlacementGroupName) Type() errors.AutoscalerErrorType {
	return InvalidPlacementGroupNameError
}

// AddPrefix adds a prefix to the error message, without changing the error type.
func (e *ErrInvalidPlacementGroupName) AddPrefix(msg string, args ...any) errors.AutoscalerError {
	e.Prefix = fmt.Sprintf(msg, args...) + e.Prefix
	return e
}

// NewInvalidPlacementGroupNameError creates a specific error type.
func NewInvalidPlacementGroupNameError(placementGroup, msg string) errors.AutoscalerError {
	return &ErrInvalidPlacementGroupName{PlacementGroup: placementGroup, Msg: msg}
}

// ErrInvalidPlacementPolicy represents an error caused by invalid placement policy
type ErrInvalidPlacementPolicy struct {
	Prefix          string
	PlacementPolicy string
	Msg             string
}

// Error returns an error message applicable for the error type.
func (e *ErrInvalidPlacementPolicy) Error() string {
	return e.Prefix + fmt.Sprintf("invalid placement policy %s, %s", e.PlacementPolicy, e.Msg)
}

// Type returns the type of the error.
func (e *ErrInvalidPlacementPolicy) Type() errors.AutoscalerErrorType {
	return InvalidPlacementPolicyError
}

// AddPrefix adds a prefix to the error message, without changing the error type.
func (e *ErrInvalidPlacementPolicy) AddPrefix(msg string, args ...any) errors.AutoscalerError {
	e.Prefix = fmt.Sprintf(msg, args...) + e.Prefix
	return e
}

// NewInvalidPlacementPolicyFromErrors creates an error for invalid placement policy.
func NewInvalidPlacementPolicy(placementPolicy, msg string) errors.AutoscalerError {
	return &ErrInvalidPlacementPolicy{PlacementPolicy: placementPolicy, Msg: msg}
}

func newInvalidPlacementPolicyFromErrors(errs []*ErrInvalidPlacementPolicy) errors.AutoscalerError {
	if len(errs) == 0 {
		return nil
	}
	if len(errs) == 1 {
		return errs[0]
	}
	var errors []string
	for _, err := range errs {
		errors = append(errors, err.Error())
	}
	return NewInvalidPlacementPolicy(errs[0].PlacementPolicy, fmt.Sprintf("multiple validation failures: %s", strings.Join(errors, ",")))
}

// ErrUnsupportedCompactPlacementConfig represents an error caused by pod configuration incompatible
// with compact placement.
type ErrUnsupportedCompactPlacementConfig struct {
	Prefix         string
	PlacementGroup string
	Msg            string
}

// Error returns an error message applicable for the error type.
func (e *ErrUnsupportedCompactPlacementConfig) Error() string {
	return e.Prefix + fmt.Sprintf("configuration specified for compact placement group %q is invalid, %s", e.PlacementGroup, e.Msg)
}

// Type returns the type of the error.
func (e *ErrUnsupportedCompactPlacementConfig) Type() errors.AutoscalerErrorType {
	return InvalidPlacementGroupNameError
}

// AddPrefix adds a prefix to the error message, without changing the error type.
func (e *ErrUnsupportedCompactPlacementConfig) AddPrefix(msg string, args ...any) errors.AutoscalerError {
	e.Prefix = fmt.Sprintf(msg, args...) + e.Prefix
	return e
}

// NewUnsupportedCompactPlacementConfigError creates a specific error type.
func NewUnsupportedCompactPlacementConfigError(placementGroup, msg string) errors.AutoscalerError {
	return &ErrUnsupportedCompactPlacementConfig{PlacementGroup: placementGroup, Msg: msg}
}

// ErrNodeGroupAlreadyExists represents an error caused by already existing node group with
// the same name, but without compatible configuration.
type ErrNodeGroupAlreadyExists struct {
	Prefix    string
	NodeGroup string
	Msg       string
}

// Error returns an error message applicable for the error type.
func (e *ErrNodeGroupAlreadyExists) Error() string {
	return e.Prefix + e.Msg
}

// Type returns the type of the error.
func (e *ErrNodeGroupAlreadyExists) Type() errors.AutoscalerErrorType {
	return NodeGroupAlreadyExistsError
}

// AddPrefix adds a prefix to the error message, without changing the error type.
func (e *ErrNodeGroupAlreadyExists) AddPrefix(msg string, args ...any) errors.AutoscalerError {
	e.Prefix = fmt.Sprintf(msg, args...) + e.Prefix
	return e
}

// NewNodeGroupAlreadyExistsError creates a specific error type.
func NewNodeGroupAlreadyExistsError(nodeGroup string) errors.AutoscalerError {
	return &ErrNodeGroupAlreadyExists{
		NodeGroup: nodeGroup,
		Msg:       fmt.Sprintf("node group %q already exists", nodeGroup),
	}
}

// ErrInvalidMachineFamily represents an error caused by pod configuration incompatible
// with compact placement.
type ErrInvalidMachineFamily struct {
	Prefix         string
	PlacementGroup string
	Msg            string
}

// Error returns an error message applicable for the error type.
func (e *ErrInvalidMachineFamily) Error() string {
	return e.Prefix + fmt.Sprintf("Invalid configuration for placement group %q: %s", e.PlacementGroup, e.Msg)
}

// Type returns the type of the error.
func (e *ErrInvalidMachineFamily) Type() errors.AutoscalerErrorType {
	return InvalidMachineFamilyError
}

// AddPrefix adds a prefix to the error message, without changing the error type.
func (e *ErrInvalidMachineFamily) AddPrefix(msg string, args ...any) errors.AutoscalerError {
	e.Prefix = fmt.Sprintf(msg, args...) + e.Prefix
	return e
}

// NewInvalidMachineFamilyError creates a specific error type.
func NewInvalidMachineFamilyError(placementGroup, msg string) errors.AutoscalerError {
	return &ErrInvalidMachineFamily{PlacementGroup: placementGroup, Msg: msg}
}
