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

package errors

import (
	"errors"
	"fmt"
	"strings"
)

type BackoffType string

const (
	// NodeLevel backoff is used for errors contained to a single node.
	NodeLevel BackoffType = "nodeLevel"

	// ClusterLevel backoff is used for errors affecting multiple nodes or unknown states.
	ClusterLevel BackoffType = "clusterLevel"

	// NoBackoff is used when no backoff is desired but we want to propagate the resize error for observability purposes.
	NoBackoff BackoffType = "noBackoff"
)

type VmState int

const (
	// DesiredState indicates that VM is in the desired state despite some errors.
	DesiredState VmState = iota
	// StartingState indicates that VM is in the state before the resize.
	StartingState
	// UnknownState indicates that VM in in an unknown state.
	UnknownState
)

type ResizeErrorType string

const (
	// NotEnoughResourceOnHostError - upsize failed due to lack of available resources on the host.
	NotEnoughResourceOnHostError ResizeErrorType = "notEnoughResourceOnHostError"

	// GuestAgentResizeWarning indicates uknown VM state. Borg limits for the VM were updated and no rollback was initiated.
	// The resize might go through at any time. We should retry the same resize and hope that guest agent and GCE resync.
	// This issue might persist, in which case we assume that the VM is no longer resizable.
	GuestAgentResizeWarning ResizeErrorType = "guestAgentResizeWarning"

	// GuestAgentResizeTimeout is handled identically to GuestAgentResizeWarning.
	GuestAgentResizeTimeout ResizeErrorType = "guestAgentResizeTimeout"

	// GuestAgentFailedToResizeError - downsize failed due to GuestAgent failure.
	GuestAgentFailedToResizeError ResizeErrorType = "guestAgentFailedToResizeError"

	// InstanceIsBusyError - resize operation failed due to the target VM having another long running operation running.
	// This makes the VM non-resizable until the operation finishes.
	InstanceIsBusyError ResizeErrorType = "instanceIsBusyError"

	// Http5xxError - resize failed due to an internal GCE error.
	Http5xxError ResizeErrorType = "http5xxError"

	// ClientError - error or timeout happened while waiting for GCE resize operation.
	ClientError ResizeErrorType = "clientError"

	// ResourceNotReadyError - error happened while trying to schedule a resize operation
	// during another ongoing operation.
	ResourceNotReadyError ResizeErrorType = "resourceNotReadyError"

	// RateLimitExceededError - resize failed due to hitting an API rate limit during GceClient ResizeVm method call.
	RateLimitExceededError ResizeErrorType = "rateLimitExceededError"

	// TimeoutError - timeout during GceClient ResizeVm method call.
	TimeoutError ResizeErrorType = "getInstancesTimeoutError"

	// QuotaExceededError - quota exceeded during GCE resize operation.
	QuotaExceededError ResizeErrorType = "quotaExceededError"

	// BalloonPodResizeError - error preventing a balloon pod from resizing correctly.
	BalloonPodResizeError ResizeErrorType = "balloonPodResizeError"

	// BalloonPodResizeTaintError - error happened during apply or remove resize taint.
	BalloonPodResizeTaintError ResizeErrorType = "balloonPodResizeTaintError"

	// ExceededPodRequestWarning - node state changed while waiting for a downsize.
	// At the moment of processing a downsize operation pod's accumulated requests
	// exceed operation desired size allocatable.
	ExceededPodRequestWarning ResizeErrorType = "exceededPodRequestWarning"

	// GenericError - catchall error for unclassified resizing issues. For instance, failure to update CA caches or to update node taints.
	GenericError ResizeErrorType = "genericError"

	// UntypedError - wrapper for errors that are not already wrapped in a ResizeError. Error handling logic requires all errors to be wrapped in a ResizeError.
	// If they aren't we wrap them as UntypedError to keep the error handling logic consistent.
	UntypedError ResizeErrorType = "untypedError"
)

type GceClientResizeErrorSource string

const (
	GetInstance   GceClientResizeErrorSource = "GceClientGetInstance"
	SetScheduling GceClientResizeErrorSource = "GceClientSetScheduling"
	WaitForBetaOp GceClientResizeErrorSource = "GceClientWaitForBetaOp"
	Empty         GceClientResizeErrorSource = ""
)

// ResizeErr is used for comparing errors using errors.As when we only care about the type.
var ResizeErr = &ResizeError{}

type ResizeError struct {
	ErrType       ResizeErrorType
	MachineFamily string
	Backoff       BackoffType
	VmState       VmState
	OriginalError error
	ErrSource     GceClientResizeErrorSource
}

func (e *ResizeError) Error() string {
	family := strings.ToUpper(e.MachineFamily)
	if family == "" {
		family = "Unknown"
	}
	if e.ErrSource == "" {
		return fmt.Sprintf("%s VM Resize Error - %s: %s", family, e.ErrType, e.OriginalError.Error())
	}
	return fmt.Sprintf("%s VM Resize Error - %s during %s: %s", family, e.ErrType, e.ErrSource, e.OriginalError.Error())
}

func (e *ResizeError) Unwrap() error {
	return e.OriginalError
}
func NewGuestAgentFailedToResizeError(machineFamily string, err error) error {
	return &ResizeError{
		MachineFamily: machineFamily,
		ErrType:       GuestAgentFailedToResizeError,
		Backoff:       NodeLevel,
		VmState:       UnknownState,
		OriginalError: err,
	}
}

func NewInstanceIsBusyError(machineFamily string, err error) error {
	return &ResizeError{
		MachineFamily: machineFamily,
		ErrType:       InstanceIsBusyError,
		Backoff:       NodeLevel,
		VmState:       StartingState,
		OriginalError: err,
	}
}

func NewHttp5xxError(machineFamily string, err error) error {
	return &ResizeError{
		MachineFamily: machineFamily,
		ErrType:       Http5xxError,
		Backoff:       ClusterLevel,
		VmState:       UnknownState,
		OriginalError: err,
	}
}

func NewNotEnoughResourceOnHostError(machineFamily string, err error) error {
	return &ResizeError{
		MachineFamily: machineFamily,
		ErrType:       NotEnoughResourceOnHostError,
		Backoff:       NodeLevel,
		VmState:       StartingState,
		OriginalError: err,
	}
}

func NewTimeoutError(machineFamily string, gceClientResizeErrorSource GceClientResizeErrorSource, err error) error {
	vmState := StartingState
	if gceClientResizeErrorSource == WaitForBetaOp || gceClientResizeErrorSource == SetScheduling {
		vmState = UnknownState
	}
	return &ResizeError{
		MachineFamily: machineFamily,
		ErrType:       TimeoutError,
		Backoff:       NodeLevel,
		VmState:       vmState,
		OriginalError: err,
		ErrSource:     gceClientResizeErrorSource,
	}
}

func NewRateLimitExceededError(machineFamily string, gceClientResizeErrorSource GceClientResizeErrorSource, err error) error {
	vmState := StartingState
	if gceClientResizeErrorSource == WaitForBetaOp {
		vmState = UnknownState
	}
	return &ResizeError{
		MachineFamily: machineFamily,
		ErrType:       RateLimitExceededError,
		Backoff:       NodeLevel,
		VmState:       vmState,
		OriginalError: err,
		ErrSource:     gceClientResizeErrorSource,
	}
}

func NewResourceNotReadyError(machineFamily string, gceClientResizeErrorSource GceClientResizeErrorSource, err error) error {
	vmState := StartingState
	if gceClientResizeErrorSource == WaitForBetaOp {
		vmState = UnknownState
	}
	return &ResizeError{
		MachineFamily: machineFamily,
		ErrType:       ResourceNotReadyError,
		Backoff:       NodeLevel,
		VmState:       vmState,
		OriginalError: err,
		ErrSource:     gceClientResizeErrorSource,
	}
}

func NewQuotaExceededError(machineFamily string, gceClientResizeErrorSource GceClientResizeErrorSource, err error) error {
	vmState := StartingState
	if gceClientResizeErrorSource == WaitForBetaOp || gceClientResizeErrorSource == Empty {
		vmState = UnknownState
	}
	return &ResizeError{
		MachineFamily: machineFamily,
		ErrType:       QuotaExceededError,
		Backoff:       NodeLevel,
		VmState:       vmState,
		OriginalError: err,
		ErrSource:     gceClientResizeErrorSource,
	}
}

func NewGuestAgentResizeWarning(machineFamily string, err error) error {
	return &ResizeError{
		MachineFamily: machineFamily,
		ErrType:       GuestAgentResizeWarning,
		Backoff:       NodeLevel,
		VmState:       UnknownState,
		OriginalError: err,
	}
}

func NewGuestAgentResizeTimeout(machineFamily string, err error) error {
	return &ResizeError{
		MachineFamily: machineFamily,
		ErrType:       GuestAgentResizeTimeout,
		Backoff:       NodeLevel,
		VmState:       UnknownState,
		OriginalError: err,
	}
}

func NewClientError(machineFamily string, err error) error {
	return &ResizeError{
		MachineFamily: machineFamily,
		ErrType:       ClientError,
		Backoff:       ClusterLevel,
		VmState:       UnknownState,
		OriginalError: err,
	}
}

func NewBalloonPodResizeError(machineFamily string, err error, vmState VmState) error {
	return &ResizeError{
		MachineFamily: machineFamily,
		ErrType:       BalloonPodResizeError,
		Backoff:       NoBackoff,
		VmState:       vmState,
		OriginalError: err,
	}
}

func NewBalloonPodResizeTaintError(machineFamily string, err error, vmState VmState) error {
	return &ResizeError{
		MachineFamily: machineFamily,
		ErrType:       BalloonPodResizeTaintError,
		Backoff:       NoBackoff,
		VmState:       vmState,
		OriginalError: err,
	}
}

func NewExceededPodRequestsWarning(machineFamily string, err error, vmState VmState) error {
	return &ResizeError{
		MachineFamily: machineFamily,
		ErrType:       ExceededPodRequestWarning,
		Backoff:       NoBackoff,
		VmState:       vmState,
		OriginalError: err,
	}
}

// NewGenericError creates a catch-all error.
func NewGenericError(machineFamily string, err error, vmState VmState) error {
	return &ResizeError{
		MachineFamily: machineFamily,
		ErrType:       GenericError,
		Backoff:       NoBackoff,
		VmState:       vmState,
		OriginalError: err,
	}
}

func NewUntypedError(machineFamily string, gceClientResizeErrorSource GceClientResizeErrorSource, err error) *ResizeError {
	vmState := UnknownState
	if gceClientResizeErrorSource == GetInstance {
		vmState = StartingState
	}
	return &ResizeError{
		MachineFamily: machineFamily,
		ErrSource:     gceClientResizeErrorSource,
		ErrType:       UntypedError,
		Backoff:       NoBackoff,
		OriginalError: err,
		VmState:       vmState,
	}
}

// ToResizeError is a convenience wrapper around errors.As to check if an error is a ResizeError.
func ToResizeError(err error) (*ResizeError, bool) {
	var resizeErr *ResizeError
	ok := errors.As(err, &resizeErr)
	return resizeErr, ok
}

// AreTwoResizeErrorsEqual returns true if two errors' ErrType, Backoff, VmState, ErrSource are equal
func AreTwoResizeErrorsEqual(firstError, secondError ResizeError) bool {
	return firstError.ErrType == secondError.ErrType && firstError.Backoff == secondError.Backoff && firstError.VmState == secondError.VmState && firstError.ErrSource == secondError.ErrSource && firstError.MachineFamily == secondError.MachineFamily
}
