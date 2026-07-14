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
	"errors"
	"fmt"
	"net/http"
	"time"

	gce_api_v1 "google.golang.org/api/compute/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	gke_metrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils"
	klog "k8s.io/klog/v2"
	"k8s.io/utils/lru"
)

type resizeRequestClientV1 struct {
	gceV1Service *gce_api_v1.Service
	mode         ResizeRequestMode
	resourceName string
	// The cache is NOT THREAD SAFE (some methods do both acccess and write)
	cache *resizeRequestOperationV1Cache

	// failedCreationRequestsPerMIG are registered during scale up and used to correct the number of VMs expected on scale up
	failedCreationRequestsPerMIG *failedRequestTracker

	// These can be overridden, e.g. for testing.
	operationWaitTimeout  time.Duration
	operationPollInterval time.Duration
}

// NewResizeRequestClientV1 creates a new client to communicate with
// MIG Resize Request API. If gceEndpoint is not empty the base path is overridden.
func NewResizeRequestClientV1(client *http.Client, projectID, userAgent, gceEndpoint string, resizeRequestMode ResizeRequestMode) (*resizeRequestClientV1, error) {
	gceV1Service, err := gce_api_v1.New(client)
	if err != nil {
		return nil, err
	}
	gceV1Service.UserAgent = userAgent
	if len(gceEndpoint) > 0 {
		gceV1Service.BasePath = gceEndpoint
	}
	rrResourceName := "resize_request_" + string(resizeRequestMode)

	return &resizeRequestClientV1{
		gceV1Service:                 gceV1Service,
		mode:                         resizeRequestMode,
		resourceName:                 rrResourceName,
		operationWaitTimeout:         defaultOperationWaitTimeout,
		operationPollInterval:        defaultOperationPollInterval,
		cache:                        newResizeRequestOperationV1Cache(deleteOperationsCacheSize),
		failedCreationRequestsPerMIG: newFailedRequestTracker(),
	}, nil
}

// RegisterFailedResizeRequestsCreation stores the error-reason and number of failed Resize Request creations, which are registered during DWS Flex Start scale up and used later to correct the number of VMs expected in scale up
func (client *resizeRequestClientV1) RegisterFailedResizeRequestsCreation(migRef gce.GceRef, err error, count int) {
	client.failedCreationRequestsPerMIG.record(migRef, err, count)
}

// ResetFailedResizeRequestsCreation returns the currently saved failed creation requests (errors and number of VMs from that scale up) for the MIG and clears its map
func (client *resizeRequestClientV1) ResetFailedResizeRequestsCreation(migRef gce.GceRef) map[error]int {
	return client.failedCreationRequestsPerMIG.reset(migRef)
}

// ResizeRequests lists all Resize Requests in the MIG.
func (client *resizeRequestClientV1) ResizeRequests(ctx context.Context, migRef gce.GceRef) ([]ResizeRequestStatus, error) {
	items := []*gce_api_v1.InstanceGroupManagerResizeRequest{}
	start := time.Now()
	lastRequestStart := start
	err := client.gceV1Service.InstanceGroupManagerResizeRequests.List(migRef.Project, migRef.Zone, migRef.Name).Pages(
		ctx, func(gceResp *gce_api_v1.InstanceGroupManagerResizeRequestsListResponse) error {
			// Register successful execution for the call
			gke_metrics.EmitGceLatency(client.resourceName, "list_page", gceResp, nil, lastRequestStart)
			// Add the items from the page to the slice
			items = append(items, gceResp.Items...)
			// Reset the start time for the next request
			lastRequestStart = time.Now()
			return nil
		},
	)
	gke_metrics.EmitGceLatency(client.resourceName, "list", nil, err, start)
	if err != nil {
		logResponseError(err)
		gke_metrics.EmitGceLatency(client.resourceName, "list_page", nil, err, lastRequestStart)
		return nil, fmt.Errorf("while ResizeRequests.List got error: %w", err)
	}
	finalError := utils.NewMultiErr(10) // Catch first ten errors.
	result := make([]ResizeRequestStatus, 0, len(items))
	for _, item := range items {
		rr, err := resizeRequestFromGCEV1Response(item, migRef)
		if err != nil {
			finalError.Append(err)
			continue
		}
		result = append(result, rr)
	}
	return result, finalError.ErrorOrNil()
}

// ResizeRequest gets the status of the specific Resize Request within a MIG.
func (client *resizeRequestClientV1) ResizeRequest(ctx context.Context, migRef gce.GceRef, resizeRequestName string) (ResizeRequestStatus, error) {
	start := time.Now()
	gceResp, err := client.gceV1Service.InstanceGroupManagerResizeRequests.Get(migRef.Project, migRef.Zone, migRef.Name, resizeRequestName).Context(ctx).Do()
	gke_metrics.EmitGceLatency(client.resourceName, "get", gceResp, err, start)
	if err != nil {
		logResponseError(err)
		return ResizeRequestStatus{}, fmt.Errorf("while ResizeRequests.Get got error: %w", err)
	}
	return resizeRequestFromGCEV1Response(gceResp, migRef)
}

// AdvanceResizeRequestCleanUp triggers cancel or delete operation for the Resize Request specified by ResizeRequestStatus based on its State.
// Previously invoked Cancel/Delete operations are stored in cache to avoid triggering multiple operations simultaneously for the same Resize Request.
// If operation already exists, only its status is checked and in case of failure the operation is retriggered.
func (client *resizeRequestClientV1) AdvanceResizeRequestCleanUp(ctx context.Context, rr ResizeRequestStatus) error {
	// nextDeletionAction decides the next appropriate GCE Resize Request API call (Cancel/Delete) based on existing operation and state of the Resize Request.
	nextAction, actionErr := client.nextDeletionAction(rr)
	switch nextAction {
	case noAction:
		return nil

	case cancelAction:
		if actionErr != nil {
			klog.Warningf("Retrying Cancel operation of Resize Request %s for mig %s/%s in zone %s, because the previous attempt failed with error: %v", rr.Name, rr.ProjectID, rr.MigName, rr.Zone, actionErr)
		} else {
			klog.Infof("Triggering Cancel operation on Resize Request %s in mig %s/%s in zone %s", rr.Name, rr.ProjectID, rr.MigName, rr.Zone)
		}
		start := time.Now()
		operation, err := client.gceV1Service.InstanceGroupManagerResizeRequests.Cancel(rr.ProjectID, rr.Zone, rr.MigName, rr.Name).Context(ctx).Do()
		gke_metrics.EmitGceLatency(client.resourceName, "cancel", operation, err, start)
		if err != nil {
			logResponseError(err)
			// TODO(b/486109144): we want to ignore this error for FSNQ, but for FSQ OBTAINABILITY strategy we'll want to get it to count it as overprovisioning. It'd require updating function signature to accept a parameter
			if IsConditionNotMetErr(err) {
				klog.Warningf("Triggered Cancel operation on Resize Request %s in mig %s/%s in zone %s in state other than %q, will retry with Delete operation later", rr.Name, rr.ProjectID, rr.MigName, rr.Zone, ResizeRequestStateAccepted)
				return nil
			}
			return fmt.Errorf("while calling ResizeRequests.Cancel got error: %w", err)
		}
		client.cache.setOperation(rr.ID, operation)

	case deleteAction:
		if actionErr != nil {
			klog.Warningf("Retrying Delete operation of Resize Request %s for mig %s/%s in zone %s, because the previous attempt failed with error: %v", rr.Name, rr.ProjectID, rr.MigName, rr.Zone, actionErr)
		} else {
			klog.Infof("Triggering Delete operation on Resize Request %s in mig %s/%s in zone %s", rr.Name, rr.ProjectID, rr.MigName, rr.Zone)
		}
		start := time.Now()
		operation, err := client.gceV1Service.InstanceGroupManagerResizeRequests.Delete(rr.ProjectID, rr.Zone, rr.MigName, rr.Name).Context(ctx).Do()
		gke_metrics.EmitGceLatency(client.resourceName, "delete", operation, err, start)
		if err != nil {
			logResponseError(err)
			return fmt.Errorf("while calling ResizeRequests.Delete got error: %w", err)
		}
		client.cache.setOperation(rr.ID, operation)

	case invalidAction:
		klog.Warningf("Couldn't decide on the next deletion action for Resize Request %s in mig %s/%s zone %s, got error: %v", rr.Name, rr.ProjectID, rr.MigName, rr.Zone, actionErr)
		return nil
	}

	return nil
}

// ReportState returns the report state of the particular Resize Request
func (client *resizeRequestClientV1) ReportState(rr ResizeRequestStatus) ResizeRequestReportState {
	return client.cache.reportState(rr.ID)
}

// SetReportState sets the report state of the particular Resize Request
func (client *resizeRequestClientV1) SetReportState(rr ResizeRequestStatus, state ResizeRequestReportState) {
	client.cache.setReportState(rr.ID, state)
}

// CreateResizeRequest creates a new Resize Request.
func (client *resizeRequestClientV1) CreateResizeRequest(ctx context.Context, migRef gce.GceRef, createRequest ResizeRequestCreateRequest) error {
	gceResizeRequest := &gce_api_v1.InstanceGroupManagerResizeRequest{
		Name:     createRequest.Name,
		ResizeBy: createRequest.ResizeBy,
	}
	if createRequest.RequestedRunDuration != nil {
		protoDuration := durationpb.New(*createRequest.RequestedRunDuration)
		gceResizeRequest.RequestedRunDuration = &gce_api_v1.Duration{
			Seconds: protoDuration.Seconds,
			Nanos:   int64(protoDuration.Nanos),
		}
	}
	start := time.Now()
	operation, err := client.gceV1Service.InstanceGroupManagerResizeRequests.Insert(migRef.Project, migRef.Zone, migRef.Name, gceResizeRequest).Context(ctx).Do()
	gke_metrics.EmitGceLatency(client.resourceName, "put", operation, err, start)
	if err != nil {
		logResponseError(err)
		return fmt.Errorf("while ResizeRequests.Insert got error: %w", err)
	}
	return client.waitForV1Op(operation, migRef.Project, migRef.Zone, gceResizeRequest.Name)
}

// nextDeletionAction decides the next appropriate GCE Resize Request API call (Cancel/Delete) based on existing operation and state of the Resize Request.
// Resize Request API allows only invoking DELETE on terminal (Succeeded/Failed/Cancelled) Resize Requests, so if we want to delete active (Accepted) Resize Request, we have to call CANCEL first, then after that operation has finished, we will be able to delete the Cancelled terminal request.
func (client *resizeRequestClientV1) nextDeletionAction(rr ResizeRequestStatus) (nextAction, error) {
	op := client.cache.operation(rr.ID)

	// If there's no running operation for the given Resize Request, trigger Cancel/Delete appropriately
	if op == nil {
		switch rr.State {
		// Terminal requests can be deleted
		case ResizeRequestStateSucceeded, ResizeRequestStateFailed, ResizeRequestStateCancelled:
			return deleteAction, nil
		// Active requests have to be cancelled first, cannot be deleted directly
		case ResizeRequestStateAccepted:
			return cancelAction, nil
		// Creating state - should be brief and move to other state soon
		default:
			return invalidAction, fmt.Errorf("cannot trigger Cancel nor Delete operation on Resize Request %s in mig %s in zone %s, because it's in state %q", rr.Name, rr.MigName, rr.Zone, rr.State)
		}
	}

	// There's already an existing related operation, check status and decide the next action
	err := client.fetchV1Op(op, rr.ProjectID, rr.Zone, rr.Name)
	switch {
	// Cancel operation is finished, trigger Delete and set ToBeReported report state
	case err == nil && op.OperationType == cancelOpType:
		klog.Infof("Cancel operation on Resize Request %s in mig %s in zone %s finished successfully, will proceed with Delete operation", rr.Name, rr.MigName, rr.Zone)
		client.cache.removeOperation(rr.ID)
		client.SetReportState(rr, ToBeReportedState)
		return deleteAction, nil
	// Delete operation is finished, we're done
	case err == nil && op.OperationType == deleteOpType:
		client.cache.remove(rr.ID)
		return noAction, nil
	// Operation is still running/couldn't be retrieved, we'll try next time
	case errors.Is(err, errOperationStillRunning) || errors.Is(err, errZoneOperationsAPI):
		return noAction, nil
	// Cancel failed due to RR already being provisioned, skip, this request will be evaluated based on newer state next time
	case op.OperationType == cancelOpType && errors.Is(err, errCancelConditionNotMet):
		klog.Infof("Cancel operation on Resize Request %s in mig %s in zone %s was triggered too late, finished with: %v. Further handling will continue in next loop.", rr.Name, rr.MigName, rr.Zone, err)
		client.cache.removeOperation(rr.ID)
		return noAction, nil
	// There were unknown errors, we have to retry Cancel
	case op.OperationType == cancelOpType:
		client.cache.removeOperation(rr.ID)
		return cancelAction, err
	// There were unknown errors, we have to retry Delete
	case op.OperationType == deleteOpType:
		client.cache.removeOperation(rr.ID)
		return deleteAction, err
	// Operation type was unrecognized, return error
	default:
		return invalidAction, fmt.Errorf("cannot sync operation of unsupported type %q on Resize Request %s in mig %s in zone %s", op.OperationType, rr.Name, rr.MigName, rr.Zone)
	}
}

func (client *resizeRequestClientV1) waitForV1Op(operation *gce_api_v1.Operation, projectID, zone, resizeRequestName string) error {
	start := time.Now()
	time.Sleep(client.operationPollInterval) // Delay the first iteration.
	for time.Since(start) < client.operationWaitTimeout {
		klog.V(4).Infof("Waiting for ResizeRequest creation operation %s %s %s", projectID, zone, operation.Name)
		err := client.fetchV1Op(operation, projectID, zone, resizeRequestName)
		if !errors.Is(err, errOperationStillRunning) && !errors.Is(err, errZoneOperationsAPI) {
			gke_metrics.EmitGceLatency("zone_operations_resize", "get_polling", nil, err, start)
			return err
		}
		time.Sleep(client.operationPollInterval)
	}
	gke_metrics.EmitGceLatency("zone_operations_resize", "get_polling", nil, context.DeadlineExceeded, start)
	return fmt.Errorf("timeout while waiting for ResizeRequest %q creation operation %s on %s to complete", resizeRequestName, operation.Name, operation.TargetLink)
}

// fetchV1Op fetches the current state of the operation and depending on its status returns:
// * nil - if the operation has finished successfully (without errors),
// * errZoneOperationsAPI error - when the current state of the operation couldn't be retrieved,
// * errOperationStillRunning error - when the operation is not finished yet,
// * otherwise propagates the errors retrieved from finished unsuccessful operation.
func (client *resizeRequestClientV1) fetchV1Op(operation *gce_api_v1.Operation, projectID, zone, resizeRequestName string) error {
	start := time.Now()
	op, err := client.gceV1Service.ZoneOperations.Get(projectID, zone, operation.Name).Do()
	gke_metrics.EmitGceLatency("zone_operations_resize", "get", op, err, start)
	if err != nil {
		logResponseError(err)
		klog.Warningf("Error while getting ResizeRequest %s/%q %s operation %s on %s: %v", projectID, resizeRequestName, operation.OperationType, operation.Name, operation.TargetLink, err)
		return errZoneOperationsAPI
	}

	klog.V(4).Infof("ResizeRequest %q %s operation %s %s %s status: %s", resizeRequestName, operation.OperationType, projectID, zone, operation.Name, op.Status)
	if op.Status == "DONE" {
		if op.Error != nil && op.Error.Errors != nil {
			// Check for race condition case of invalid Cancel operation triggered on provisioning/terminal Resize Request
			if op.OperationType == cancelOpType && hasConditionNotMetV1ErrorCode(op.Error.Errors) {
				return errCancelConditionNotMet
			}
			multiErr := NewResizeRequestOperationMultiError(len(op.Error.Errors))
			for _, err := range op.Error.Errors {
				multiErr.AppendCreationError(ResizeRequestOperationError{err.Code, err.Message})
			}
			return fmt.Errorf("while %s ResizeRequest.Operation.Get %s/%s for Resize Request %q on %s got error: %w", operation.OperationType, projectID, operation.Name, resizeRequestName, operation.TargetLink, multiErr)
		}
		return nil
	}
	return errOperationStillRunning
}

func resizeRequestFromGCEV1Response(rr *gce_api_v1.InstanceGroupManagerResizeRequest, migRef gce.GceRef) (ResizeRequestStatus, error) {
	creationTime, err := time.Parse(time.RFC3339, rr.CreationTimestamp)
	if err != nil {
		return ResizeRequestStatus{}, fmt.Errorf("while time.Parse(CreationTimestamp) got error: %w", err)
	}
	state, err := stateFromGCE(rr.State)
	if err != nil {
		return ResizeRequestStatus{}, fmt.Errorf("while parsing resize state got error: %w", err)
	}
	var requestedRunDuration *time.Duration
	if rr.RequestedRunDuration != nil {
		duration := durationpb.Duration{
			Nanos:   int32(rr.RequestedRunDuration.Nanos),
			Seconds: rr.RequestedRunDuration.Seconds,
		}
		requestedRunDuration = protoDuration(duration.AsDuration())
	}
	return ResizeRequestStatus{
		ProjectID:            migRef.Project,
		ID:                   rr.Id,
		Name:                 rr.Name,
		CreationTime:         creationTime,
		ResizeBy:             rr.ResizeBy,
		State:                state,
		MigName:              migRef.Name,
		Zone:                 migRef.Zone,
		RequestedRunDuration: requestedRunDuration,
		Errors:               errorsFromGCEV1(rr.Status.Error),
		LastAttemptErrors:    lastAttemptErrorsFromGCEV1(rr.Status.LastAttempt),
	}, nil
}

func lastAttemptErrorsFromGCEV1(gceLastAttempt *gce_api_v1.InstanceGroupManagerResizeRequestStatusLastAttempt) []DwsStatusError {
	if gceLastAttempt == nil || gceLastAttempt.Error == nil || len(gceLastAttempt.Error.Errors) == 0 {
		return nil
	}
	result := make([]DwsStatusError, 0, len(gceLastAttempt.Error.Errors))
	for _, e := range gceLastAttempt.Error.Errors {
		err := DwsStatusError{
			Code:    e.Code,
			Message: e.Message,
		}
		if IsResourcePoolExhaustedErrorCode(e.Code) && len(e.ErrorDetails) > 0 {
			errorInfo := e.ErrorDetails[0].ErrorInfo
			if errorInfo != nil && errorInfo.Metadatas != nil && len(errorInfo.Reason) > 0 {
				details := fmt.Sprintf("Reason: %q, VMType: %q, Attachment: %q.", errorInfo.Reason, errorInfo.Metadatas["vmType"], errorInfo.Metadatas["attachment"])
				err.Message = err.Message + " " + details
			}
		}
		result = append(result, err)
	}
	return result
}

func errorsFromGCEV1(gceError *gce_api_v1.InstanceGroupManagerResizeRequestStatusError) []DwsStatusError {
	if gceError == nil || len(gceError.Errors) == 0 {
		return nil
	}

	result := make([]DwsStatusError, 0, len(gceError.Errors))
	for _, e := range gceError.Errors {
		result = append(result, DwsStatusError{
			Code:     e.Code,
			Location: e.Location,
			Message:  e.Message,
		})
	}
	return result
}

// hasConditionNotMetBetaErrorCode matches ConditionNotMet errors returned with finished operation
func hasConditionNotMetV1ErrorCode(errors []*gce_api_v1.OperationErrorErrors) bool {
	if errors == nil {
		return false
	}
	for _, err := range errors {
		if err.Code == "CONDITION_NOT_MET" {
			return true
		}
	}
	return false
}

// The cache is NOT THREAD SAFE (some methods do both acccess and write)
type resizeRequestOperationV1Cache struct {
	cache *lru.Cache
}

type rrV1CacheOpStatus struct {
	op          *gce_api_v1.Operation
	reportState ResizeRequestReportState
}

func newResizeRequestOperationV1Cache(maxSize int) *resizeRequestOperationV1Cache {
	return &resizeRequestOperationV1Cache{
		cache: lru.New(maxSize),
	}
}

func (c *resizeRequestOperationV1Cache) operation(rrID uint64) *gce_api_v1.Operation {
	value, found := c.cache.Get(rrID)
	if !found {
		return nil
	}
	return value.(*rrV1CacheOpStatus).op
}

func (c *resizeRequestOperationV1Cache) reportState(rrID uint64) ResizeRequestReportState {
	value, found := c.cache.Get(rrID)
	if !found {
		return UnspecifiedReportState
	}
	return value.(*rrV1CacheOpStatus).reportState
}

func (c *resizeRequestOperationV1Cache) setOperation(rrID uint64, op *gce_api_v1.Operation) {
	c.cache.Add(rrID, &rrV1CacheOpStatus{op, c.reportState(rrID)})
}

func (c *resizeRequestOperationV1Cache) setReportState(rrID uint64, state ResizeRequestReportState) {
	c.cache.Add(rrID, &rrV1CacheOpStatus{c.operation(rrID), state})
}

func (c *resizeRequestOperationV1Cache) removeOperation(rrID uint64) {
	value, found := c.cache.Get(rrID)
	if found {
		state := value.(*rrV1CacheOpStatus).reportState
		c.cache.Add(rrID, &rrV1CacheOpStatus{nil, state})
	}
}

func (c *resizeRequestOperationV1Cache) remove(rrID uint64) {
	c.cache.Remove(rrID)
}
