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

package resizerequestclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/api/googleapi"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	klog "k8s.io/klog/v2"
)

// ResizeRequestState represents state the Resize Request is in.
type ResizeRequestState string

const (
	ResizeRequestStateAccepted     ResizeRequestState = "Accepted"
	ResizeRequestStateCreating     ResizeRequestState = "Creating"
	ResizeRequestStateDeleting     ResizeRequestState = "Deleting"
	ResizeRequestStateCancelled    ResizeRequestState = "Cancelled"
	ResizeRequestStateFailed       ResizeRequestState = "Failed"
	ResizeRequestStateProvisioning ResizeRequestState = "Provisioning"
	ResizeRequestStateSucceeded    ResizeRequestState = "Succeeded"
)

const (
	// The MIG makes call to the QRM that pre-validates some of the provisioning
	// requirements, for example. machine-type, image, reservation affinities,
	// VM scheduling properties, quota, etc.
	// This may take more than minute, hence 2min timeout.
	defaultOperationWaitTimeout  = 120 * time.Second
	defaultOperationPollInterval = 1 * time.Second
)

// ResizeRequestMode represents the mode of the resize request.
type ResizeRequestMode string

const (
	ResizeRequestModeQueued ResizeRequestMode = "queued"
	ResizeRequestModeAtomic ResizeRequestMode = "atomic"
	// TODO(b/381046606): Temporary service until migration to CreateInstance API
	ResizeRequestModeFlex ResizeRequestMode = "flex"
)

// ResizeRequestClient is used for communicating with GCE MIG API
// providing additional methods over OSS AutoscalingGCEClient.
type ResizeRequestClient interface {
	ResizeRequests(ctx context.Context, migRef gce.GceRef) ([]ResizeRequestStatus, error)
	ResizeRequest(ctx context.Context, migRef gce.GceRef, resizeRequestName string) (ResizeRequestStatus, error)
	AdvanceResizeRequestCleanUp(ctx context.Context, resizeRequest ResizeRequestStatus) error
	CreateResizeRequest(ctx context.Context, migRef gce.GceRef, createRequest ResizeRequestCreateRequest) error
	// TODO(b/381046606): remove ReportState, SetReportState, RegisterFailedResizeRequestsCreation, ResetFailedResizeRequestsCreation after migrating FSNQ to CreateInstances API
	ReportState(resizeRequest ResizeRequestStatus) ResizeRequestReportState
	SetReportState(resizeRequest ResizeRequestStatus, state ResizeRequestReportState)
	RegisterFailedResizeRequestsCreation(migRef gce.GceRef, err error, count int)
	ResetFailedResizeRequestsCreation(migRef gce.GceRef) map[error]int
}

// ResizeRequestCreateRequest represents a Resize Request we want to create.
type ResizeRequestCreateRequest struct {
	// Name: The name of this resize request. The name must comply with RFC1035.
	Name string
	// ResizeBy: The number of instances to create as part of this resize request.
	ResizeBy int64
	// RequestedRunDuration, duration for which we want the VMs to run.
	RequestedRunDuration *time.Duration
}

// ResizeRequestStatus represents a status of the ResizeRequest.
type ResizeRequestStatus struct {
	// ID: A unique identifier for this resource type. The server generates
	// this identifier.
	ID uint64
	// Name: The name of this resize request. The name must comply with RFC1035.
	Name string
	// CreationTime: The creation timestamp for this resize request.
	CreationTime time.Time
	// ResizeBy: The number of instances to create as part of this resize request.
	ResizeBy int64
	// State: Current state of the request.
	State ResizeRequestState
	// ProjectID: The project the MIG belongs to.
	ProjectID string
	// Mig: The name of this resize request's MIG.
	MigName string
	// Zone: The name of this resize request's zone.
	Zone string
	// RequestedRunDuration, duration for which we want the VMs to run.
	RequestedRunDuration *time.Duration
	// Errors: list of errors encountered during the queueing or provisioning phases
	// of the ResizeRequest
	Errors []DwsStatusError
	// LastAttemptErrors: Information about the last attempt to fulfill the request.
	LastAttemptErrors []DwsStatusError
}

// Ref returns an identifying string of format "zone/mig/name"
func (rr *ResizeRequestStatus) Ref() string {
	return fmt.Sprintf("%s/%s/%s", rr.Zone, rr.MigName, rr.Name)
}

// SortResizeRequestsByCreationTimestampAsc sorts the given Resize Requests by CreationTime in ascending order (oldest ones first).
func SortResizeRequestsByCreationTimestampAsc(rrs []ResizeRequestStatus) {
	compareCreationTime := func(a, b ResizeRequestStatus) int {
		return a.CreationTime.Compare(b.CreationTime)
	}
	slices.SortFunc(rrs, compareCreationTime)
}

// DwsStatusError represents a detailed error for the status
type DwsStatusError struct {
	Code     string
	Location string
	Message  string
}

// ResizeRequestOperationMultiError represents a collection wrapper error of detailed creation errors
type ResizeRequestOperationMultiError struct {
	Errors []ResizeRequestOperationError
}

// NewResizeRequestOperationMultiError creates a new instance of ResizeRequestOperationMultiError
func NewResizeRequestOperationMultiError(capacity int) *ResizeRequestOperationMultiError {
	return &ResizeRequestOperationMultiError{Errors: make([]ResizeRequestOperationError, 0, capacity)}
}

// AppendCreationError appends an individual error to the internal error list
func (err *ResizeRequestOperationMultiError) AppendCreationError(rrce ResizeRequestOperationError) {
	err.Errors = append(err.Errors, rrce)
}

func (err *ResizeRequestOperationMultiError) Error() string {
	var errs []string
	for _, rrErr := range err.Errors {
		errs = append(errs, rrErr.Error())
	}
	return fmt.Sprintf("while creating Resize Request got %d error(s): %s", len(errs), strings.Join(errs, "; "))
}

// ResizeRequestOperationError represents a detailed creation error
type ResizeRequestOperationError struct {
	Code, Message string
}

func (err *ResizeRequestOperationError) Error() string {
	return fmt.Sprintf("[%s] %q", err.Code, err.Message)
}

func logResponseError(err error) {
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		if gErr, ok := err.(*googleapi.Error); ok {
			klog.Errorf("While consulting Resize Request API received error: %q", gErr.Error())
		} else {
			klog.Errorf("While consulting Resize Request API encountered internal error: %v", err)
		}
	}
}

func stateFromGCE(s string) (ResizeRequestState, error) {
	switch strings.ToUpper(s) {
	case "ACCEPTED":
		return ResizeRequestStateAccepted, nil
	case "CREATING":
		return ResizeRequestStateCreating, nil
	case "DELETING":
		return ResizeRequestStateDeleting, nil
	case "CANCELLED":
		return ResizeRequestStateCancelled, nil
	case "FAILED":
		return ResizeRequestStateFailed, nil
	case "PROVISIONING":
		return ResizeRequestStateProvisioning, nil
	case "SUCCEEDED":
		return ResizeRequestStateSucceeded, nil
	default:
		return ResizeRequestState(""), fmt.Errorf("unknown resize request state: %q", s)
	}
}

const (
	namespaceLengthCap int = 20
	nameLengthCap      int = 20
	hashLengthCap      int = 16

	// provisioningRequestPrefix is used when creating a Resize Request for a Provisioning Request.
	// This should match http://cs/piper///depot/google3/cloud/kubernetes/engine/server/resources/node/mig.go;l=451-456;rcl=716166911
	provisioningRequestPrefix = "gke-"
	// flexStartNonQueuedPrefix is used when creating a Resize Request for DWS Flex Start Non-Queued (FSNQ) scale up.
	flexStartNonQueuedPrefix = "flex-"
	// atomicPrefix is used when creating a Resize Request for atomic TPU scale up.
	atomicPrefix = "rr-"
)

// ResizeRequestName returns name to be used when creating a Resize Request for a Provisioning Request.
// Returned value will be compliant with RFC1035 only if the input values are
// also compliant with RFC1035. For explanation see:
// https://docs.google.com/document/d/1_oAWZU9hNHm2PP4qDeyqRLqW1DcK97dh2M9hKU2_xNc/edit#bookmark=id.59nka0o7htsl
func ResizeRequestName(namespace, provReqName string) string {
	hashBuilder := sha256.New()
	hashBuilder.Write([]byte(namespace + "/" + provReqName))
	hashCapped := stringCapLength(hex.EncodeToString(hashBuilder.Sum(nil)), hashLengthCap)
	namespaceCapped := stringCapLength(namespace, namespaceLengthCap)
	provReqNameCapped := stringCapLength(provReqName, nameLengthCap)
	return fmt.Sprintf("%s%s-%s-%s", provisioningRequestPrefix, namespaceCapped, provReqNameCapped, hashCapped)
}

// IsProvisioningRequestManagedResizeRequest returns true when the given name of a Resize Request instance has the "gke-" prefix, i.e. is managed by GKE Provisioning Request.
func IsProvisioningRequestManagedResizeRequest(name string) bool {
	return strings.HasPrefix(name, provisioningRequestPrefix)
}

// AtomicResizeRequestName generates a new atomic resize request name at random, used for TPU provisioning
func AtomicResizeRequestName() string {
	// Using the same naming scheme with GKE to make it consistent.
	// See go/gke-autoscaler-resize-request-naming
	return fmt.Sprintf("%s%s", atomicPrefix, uuid.NewString())
}

// IsAtomicResizeRequest returns true when the given name of a Resize Request instance has the "rr-" prefix, used for TPU provisioning.
func IsAtomicResizeRequest(name string) bool {
	return strings.HasPrefix(name, atomicPrefix)
}

// TODO(b/381046606): remove DwsFlexScaleUp related code after migrating FSNQ to CreateInstances API
// NewFlexStartNonQueuedScaleUpId generates a new ID prefix to be used by a group of Resize Request created as a part of the same DWS Flex Start Non-Queued (FSNQ) scale up
func NewFlexStartNonQueuedScaleUpId() string {
	return fmt.Sprintf("%s%s", flexStartNonQueuedPrefix, uuid.NewString())
}

// FlexStartNonQueuedResizeRequestName returns a DWS Flex Start Non-Queued (FSNQ) Resize Request name
func FlexStartNonQueuedResizeRequestName(scaleUpId string, index int) string {
	return fmt.Sprintf("%s-%d", scaleUpId, index)
}

// FlexScaleUpId returns the prefix identifying a group of Resize Requests belonging to the same scale up
func FlexScaleUpId(rrName string) string {
	// Cut off the last `-%d` suffix denoting the index
	lastInd := strings.LastIndex(rrName, "-")
	return rrName[:lastInd]
}

// IsFlexStartNonQueuedScaleUpResizeRequest returns true when the given name of a Resize Request instance has the "flex-" prefix, i.e. is managed by DWS Flex Start Non-Queued (FSNQ) scale up.
func IsFlexStartNonQueuedScaleUpResizeRequest(name string) bool {
	return strings.HasPrefix(name, flexStartNonQueuedPrefix)
}

func stringCapLength(s string, c int) string {
	if len(s) <= c {
		return s
	}
	return s[:c]
}
