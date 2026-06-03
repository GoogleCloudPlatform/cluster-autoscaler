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

package reservations

import (
	"fmt"
	"strings"

	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
)

// UnusableReservationError - provided reservation is not usable.
const UnusableReservationError errors.AutoscalerErrorType = "unusableReservation"

// UnsupportedReservationAffinityError - provided affinity config is not supported.
const UnsupportedReservationAffinityError errors.AutoscalerErrorType = "unsupportedReservationAffinity"

// UnsupportedReservationProjectError - provided project config is not supported.
const UnsupportedReservationProjectError errors.AutoscalerErrorType = "unsupportedReservationProject"

// ErrUnusableReservation represents an error caused by an unusable reservation.
type ErrUnusableReservation struct {
	Prefix         string
	ReservationRef gceclient.ReservationRef
	Msg            string
}

// Error returns an error message applicable for the error type.
func (e *ErrUnusableReservation) Error() string {
	var sb strings.Builder
	if e.Prefix != "" {
		sb.WriteString(e.Prefix)
		sb.WriteString(" ")
	}
	sb.WriteString(fmt.Sprintf("reservation '%s' in project '%s'", e.ReservationRef.Name, e.ReservationRef.Project))
	if e.ReservationRef.BlockName != "" {
		sb.WriteString(fmt.Sprintf(" with block '%s'", e.ReservationRef.BlockName))
	}
	if e.ReservationRef.SubBlockName != "" {
		sb.WriteString(fmt.Sprintf(" with sub-block '%s'", e.ReservationRef.SubBlockName))
	}
	sb.WriteString(fmt.Sprintf(" is unusable, %s", e.Msg))
	return sb.String()
}

// Type returns the type of the error.
func (e *ErrUnusableReservation) Type() errors.AutoscalerErrorType {
	return UnusableReservationError
}

// AddPrefix adds a prefix to the error message, without changing the error type.
func (e *ErrUnusableReservation) AddPrefix(msg string, args ...any) errors.AutoscalerError {
	e.Prefix = fmt.Sprintf(msg, args...) + e.Prefix
	return e
}

// NewErrUnusableReservation creates a specific error type.
func NewErrUnusableReservation(reservationPath gceclient.ReservationRef, msg string) errors.AutoscalerError {
	return &ErrUnusableReservation{
		ReservationRef: reservationPath,
		Msg:            msg,
	}
}

// ErrUnsupportedReservationAffinity represents an error caused by unsupported reservation affinity.
type ErrUnsupportedReservationAffinity struct {
	Prefix   string
	Affinity string
	Msg      string
}

// Error returns an error message applicable for the error type.
func (e *ErrUnsupportedReservationAffinity) Error() string {
	return e.Prefix + fmt.Sprintf("reservation affinity '%q' is invalid, %s", e.Affinity, e.Msg)
}

// Type returns the type of the error.
func (e *ErrUnsupportedReservationAffinity) Type() errors.AutoscalerErrorType {
	return UnsupportedReservationAffinityError
}

// AddPrefix adds a prefix to the error message, without changing the error type.
func (e *ErrUnsupportedReservationAffinity) AddPrefix(msg string, args ...any) errors.AutoscalerError {
	e.Prefix = fmt.Sprintf(msg, args...) + e.Prefix
	return e
}

// NewUnsupportedReservationAffinityError creates a specific error type.
func NewUnsupportedReservationAffinityError(affinity, msg string) errors.AutoscalerError {
	return &ErrUnsupportedReservationAffinity{Affinity: affinity, Msg: msg}
}

// ErrUnsupportedReservationProject represents an error caused by unsupported reservation project.
type ErrUnsupportedReservationProject struct {
	Prefix  string
	Project string
	Msg     string
}

// Error returns an error message applicable for the error type.
func (e *ErrUnsupportedReservationProject) Error() string {
	return e.Prefix + fmt.Sprintf("reservation project %q is invalid, %s", e.Project, e.Msg)
}

// Type returns the type of the error.
func (e *ErrUnsupportedReservationProject) Type() errors.AutoscalerErrorType {
	return UnsupportedReservationProjectError
}

// AddPrefix adds a prefix to the error message, without changing the error type.
func (e *ErrUnsupportedReservationProject) AddPrefix(msg string, args ...any) errors.AutoscalerError {
	e.Prefix = fmt.Sprintf(msg, args...) + e.Prefix
	return e
}

// NewUnsupportedReservationProjectError creates a specific error type.
func NewUnsupportedReservationProjectError(project, msg string) errors.AutoscalerError {
	return &ErrUnsupportedReservationProject{Project: project, Msg: msg}
}
