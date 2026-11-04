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
	"strings"
	"sync"
	"time"

	gce_api_beta "google.golang.org/api/compute/v0.beta"
	gce_api_v1 "google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/types/known/durationpb"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce"
	gke_metrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils"
	klog "k8s.io/klog/v2"
	"k8s.io/utils/lru"
)

const (
	deleteOperationsCacheSize = 2000

	cancelOpType = "compute.instanceGroupManagerResizeRequests.cancel"
	deleteOpType = "compute.instanceGroupManagerResizeRequests.delete"

	// Known error transmitted from within cluster autoscaler
	// TODO(b/381046606): remove after migration to CreateInstances API
	FragmentedRRWarningCode          = "FRAGMENTED_FLEX_START_SCALE_UP"
	FragmentedRRWarningReason        = "FragmentedFlexStartScaleUp"
	FragmentedRRWarningMessageFormat = "Scale-up Limitation: Only %d of %d expected processed due to flex start non-queued preview scalability. Overflow will be handled in a later scale-up."
)

var (
	errOperationStillRunning = errors.New("operation is still running")
	errZoneOperationsAPI     = errors.New("error while getting ResizeRequest operation")
	errCancelConditionNotMet = errors.New("ResizeRequest was cancelled too late, it already started provisioning or reached terminal state")
)

type ResizeRequestReportState int

const (
	UnspecifiedReportState ResizeRequestReportState = iota
	ToBeReportedState
	AlreadyReportedState
	CleanUpOnlyState
)

type failedRequestTracker struct {
	mu           sync.Mutex
	errorsPerMIG map[gce.GceRef]map[error]int
}

func newFailedRequestTracker() *failedRequestTracker {
	return &failedRequestTracker{
		errorsPerMIG: make(map[gce.GceRef]map[error]int),
	}
}

func (t *failedRequestTracker) record(migRef gce.GceRef, err error, count int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.errorsPerMIG[migRef] == nil {
		t.errorsPerMIG[migRef] = map[error]int{}
	}
	t.errorsPerMIG[migRef][err] += count
}

func (t *failedRequestTracker) reset(migRef gce.GceRef) map[error]int {
	t.mu.Lock()
	defer t.mu.Unlock()

	err := t.errorsPerMIG[migRef]
	t.errorsPerMIG[migRef] = map[error]int{}
	return err
}

type nextAction int

const (
	noAction nextAction = iota
	deleteAction
	cancelAction
	invalidAction
)

// IsConditionNotMetErr matches ConditionNotMet errors returned on invoking an operation
func IsConditionNotMetErr(err error) bool {
	gErr, ok := err.(*googleapi.Error)
	return ok && gErr.Code == http.StatusPreconditionFailed
}

func protoDuration(d time.Duration) *time.Duration {
	return &d
}

func toSecondsAndNanos(d time.Duration) (int64, int64) {
	protoDur := durationpb.New(d)
	return protoDur.Seconds, int64(protoDur.Nanos)
}

func IsResourcePoolExhaustedErrorCode(errorCode string) bool {
	return strings.Contains(errorCode, "RESOURCE_POOL_EXHAUSTED")
}

type opInfo struct {
	name          string
	operationType string
	targetLink    string
	status        string
	errors        []ResizeRequestOperationError
}

type rawResizeRequest struct {
	id                 uint64
	name               string
	creationTimestamp  string
	resizeBy           int64
	state              string
	hasRunDuration     bool
	runDurationSeconds int64
	runDurationNanos   int64
	errors             []DwsStatusError
	lastAttemptErrors  []DwsStatusError
}

// gceOperation restricts generic Operation type parameters to the v1 or beta GCE SDK structs.
type gceOperation interface {
	gce_api_v1.Operation | gce_api_beta.Operation
}

// gceResizeRequest restricts generic ResizeRequest type parameters to the v1 or beta GCE SDK structs.
type gceResizeRequest interface {
	gce_api_v1.InstanceGroupManagerResizeRequest | gce_api_beta.InstanceGroupManagerResizeRequest
}

// gceResizeRequestAPI abstracts the generated v1 and beta GCE ResizeRequest and
// ZoneOperations services behind compile-time-checked methods.
type gceResizeRequestAPI[Operation gceOperation, ResizeRequest gceResizeRequest] interface {
	listPages(ctx context.Context, migRef gce.GceRef, onPage func(resp any, items []*ResizeRequest)) error
	get(ctx context.Context, migRef gce.GceRef, name string) (*ResizeRequest, error)
	insert(ctx context.Context, migRef gce.GceRef, req ResizeRequestCreateRequest) (*Operation, error)
	cancel(ctx context.Context, rr ResizeRequestStatus) (*Operation, error)
	delete(ctx context.Context, rr ResizeRequestStatus) (*Operation, error)
	getZoneOp(projectID, zone, opName string) (*Operation, error)
	parseResizeRequest(rr *ResizeRequest, migRef gce.GceRef) (ResizeRequestStatus, error)
	extractOpInfo(op *Operation) opInfo
}

type resizeRequestClientCommon[Operation gceOperation, ResizeRequest gceResizeRequest] struct {
	api          gceResizeRequestAPI[Operation, ResizeRequest]
	mode         ResizeRequestMode
	resourceName string
	// The cache is NOT THREAD SAFE (some methods do both access and write).
	cache *resizeRequestOperationCache[Operation]

	// failedCreationRequestsPerMIG are registered during scale up and used to correct the number of VMs expected on scale up.
	failedCreationRequestsPerMIG *failedRequestTracker

	// These can be overridden, e.g. for testing.
	operationWaitTimeout  time.Duration
	operationPollInterval time.Duration
}

func newResizeRequestClientCommon[Operation gceOperation, ResizeRequest gceResizeRequest](api gceResizeRequestAPI[Operation, ResizeRequest], resizeRequestMode ResizeRequestMode) resizeRequestClientCommon[Operation, ResizeRequest] {
	return resizeRequestClientCommon[Operation, ResizeRequest]{
		api:                          api,
		mode:                         resizeRequestMode,
		resourceName:                 "resize_request_" + string(resizeRequestMode),
		operationWaitTimeout:         defaultOperationWaitTimeout,
		operationPollInterval:        defaultOperationPollInterval,
		cache:                        newResizeRequestOperationCache[Operation](deleteOperationsCacheSize),
		failedCreationRequestsPerMIG: newFailedRequestTracker(),
	}
}

// RegisterFailedResizeRequestsCreation stores the error-reason and number of failed Resize Request creations, which are registered during DWS Flex Start scale up and used later to correct the number of VMs expected in scale up.
func (client *resizeRequestClientCommon[Operation, ResizeRequest]) RegisterFailedResizeRequestsCreation(migRef gce.GceRef, err error, count int) {
	client.failedCreationRequestsPerMIG.record(migRef, err, count)
}

// ResetFailedResizeRequestsCreation returns the currently saved failed creation requests (errors and number of VMs from that scale up) for the MIG and clears its map.
func (client *resizeRequestClientCommon[Operation, ResizeRequest]) ResetFailedResizeRequestsCreation(migRef gce.GceRef) map[error]int {
	return client.failedCreationRequestsPerMIG.reset(migRef)
}

// ResizeRequests lists all Resize Requests in the MIG.
func (client *resizeRequestClientCommon[Operation, ResizeRequest]) ResizeRequests(ctx context.Context, migRef gce.GceRef) ([]ResizeRequestStatus, error) {
	items := []*ResizeRequest{}
	start := time.Now()
	lastRequestStart := start
	err := client.api.listPages(ctx, migRef, func(gceResp any, pageItems []*ResizeRequest) {
		gke_metrics.EmitGceLatency(client.resourceName, "list_page", gceResp, nil, lastRequestStart)
		items = append(items, pageItems...)
		lastRequestStart = time.Now()
	})
	gke_metrics.EmitGceLatency(client.resourceName, "list", nil, err, start)
	if err != nil {
		logResponseError(err)
		gke_metrics.EmitGceLatency(client.resourceName, "list_page", nil, err, lastRequestStart)
		return nil, fmt.Errorf("while ResizeRequests.List got error: %w", err)
	}

	finalError := utils.NewMultiErr(10) // Catch first ten errors.
	result := make([]ResizeRequestStatus, 0, len(items))
	for _, item := range items {
		rr, err := client.api.parseResizeRequest(item, migRef)
		if err != nil {
			finalError.Append(err)
			continue
		}
		result = append(result, rr)
	}
	return result, finalError.ErrorOrNil()
}

// ResizeRequest gets the status of the specific Resize Request within a MIG.
func (client *resizeRequestClientCommon[Operation, ResizeRequest]) ResizeRequest(ctx context.Context, migRef gce.GceRef, resizeRequestName string) (ResizeRequestStatus, error) {
	start := time.Now()
	gceResp, err := client.api.get(ctx, migRef, resizeRequestName)
	gke_metrics.EmitGceLatency(client.resourceName, "get", gceResp, err, start)
	if err != nil {
		logResponseError(err)
		return ResizeRequestStatus{}, fmt.Errorf("while ResizeRequests.Get got error: %w", err)
	}
	return client.api.parseResizeRequest(gceResp, migRef)
}

// ReportState returns the report state of the particular Resize Request.
func (client *resizeRequestClientCommon[Operation, ResizeRequest]) ReportState(rr ResizeRequestStatus) ResizeRequestReportState {
	return client.cache.reportState(rr.ID)
}

// SetReportState sets the report state of the particular Resize Request.
func (client *resizeRequestClientCommon[Operation, ResizeRequest]) SetReportState(rr ResizeRequestStatus, state ResizeRequestReportState) {
	client.cache.setReportState(rr.ID, state)
}

// AdvanceResizeRequestCleanUp triggers cancel or delete operation for the Resize Request specified by ResizeRequestStatus based on its State.
// Previously invoked Cancel/Delete operations are stored in cache to avoid triggering multiple operations simultaneously for the same Resize Request.
// If operation already exists, only its status is checked and in case of failure the operation is retriggered.
func (client *resizeRequestClientCommon[Operation, ResizeRequest]) AdvanceResizeRequestCleanUp(ctx context.Context, rr ResizeRequestStatus) error {
	// nextDeletionAction decides the next appropriate GCE Resize Request API call (Cancel/Delete) based on existing operation and state of the Resize Request.
	action, actionErr := client.nextDeletionAction(rr)
	switch action {
	case noAction:
		return nil

	case cancelAction:
		err := client.triggerCleanUpOp(ctx, rr, "Cancel", "cancel", actionErr, client.api.cancel)
		if err != nil {
			// TODO(b/486109144): we want to ignore this error for FSNQ, but for FSQ OBTAINABILITY strategy we'll want to get it to count it as overprovisioning. It'd require updating function signature to accept a parameter
			if IsConditionNotMetErr(err) {
				klog.Warningf("Triggered Cancel operation on Resize Request %s in mig %s/%s in zone %s in state other than %q, will retry with Delete operation later", rr.Name, rr.ProjectID, rr.MigName, rr.Zone, ResizeRequestStateAccepted)
				return nil
			}
			return fmt.Errorf("while calling ResizeRequests.Cancel got error: %w", err)
		}

	case deleteAction:
		if err := client.triggerCleanUpOp(ctx, rr, "Delete", "delete", actionErr, client.api.delete); err != nil {
			return fmt.Errorf("while calling ResizeRequests.Delete got error: %w", err)
		}

	case invalidAction:
		klog.Warningf("Couldn't decide on the next deletion action for Resize Request %s in mig %s/%s zone %s, got error: %v", rr.Name, rr.ProjectID, rr.MigName, rr.Zone, actionErr)
		return nil
	}

	return nil
}

func (client *resizeRequestClientCommon[Operation, ResizeRequest]) triggerCleanUpOp(ctx context.Context, rr ResizeRequestStatus, actionName, metricVerb string, actionErr error, call func(context.Context, ResizeRequestStatus) (*Operation, error)) error {
	if actionErr != nil {
		klog.Warningf("Retrying %s operation of Resize Request %s for mig %s/%s in zone %s, because the previous attempt failed with error: %v", actionName, rr.Name, rr.ProjectID, rr.MigName, rr.Zone, actionErr)
	} else {
		klog.Infof("Triggering %s operation on Resize Request %s in mig %s/%s in zone %s", actionName, rr.Name, rr.ProjectID, rr.MigName, rr.Zone)
	}
	start := time.Now()
	operation, err := call(ctx, rr)
	gke_metrics.EmitGceLatency(client.resourceName, metricVerb, operation, err, start)
	if err != nil {
		logResponseError(err)
		return err
	}
	client.cache.setOperation(rr.ID, operation)
	return nil
}

// CreateResizeRequest creates a new Resize Request.
func (client *resizeRequestClientCommon[Operation, ResizeRequest]) CreateResizeRequest(ctx context.Context, migRef gce.GceRef, createRequest ResizeRequestCreateRequest) error {
	start := time.Now()
	operation, err := client.api.insert(ctx, migRef, createRequest)
	gke_metrics.EmitGceLatency(client.resourceName, "put", operation, err, start)
	if err != nil {
		logResponseError(err)
		return fmt.Errorf("while ResizeRequests.Insert got error: %w", err)
	}
	return client.waitForOp(operation, migRef.Project, migRef.Zone, createRequest.Name)
}

// nextDeletionAction decides the next appropriate GCE Resize Request API call (Cancel/Delete) based on existing operation and state of the Resize Request.
// Resize Request API allows only invoking DELETE on terminal (Succeeded/Failed/Cancelled) Resize Requests, so if we want to delete active (Accepted) Resize Request, we have to call CANCEL first, then after that operation has finished, we will be able to delete the Cancelled terminal request.
func (client *resizeRequestClientCommon[Operation, ResizeRequest]) nextDeletionAction(rr ResizeRequestStatus) (nextAction, error) {
	op := client.cache.operation(rr.ID)

	// If there's no running operation for the given Resize Request, trigger Cancel/Delete appropriately.
	if op == nil {
		switch rr.State {
		// Terminal requests can be deleted.
		case ResizeRequestStateSucceeded, ResizeRequestStateFailed, ResizeRequestStateCancelled:
			return deleteAction, nil
		// Active requests have to be cancelled first, cannot be deleted directly.
		case ResizeRequestStateAccepted:
			return cancelAction, nil
		// Creating state should be brief and move to another state soon.
		default:
			return invalidAction, fmt.Errorf("cannot trigger Cancel nor Delete operation on Resize Request %s in mig %s in zone %s, because it's in state %q", rr.Name, rr.MigName, rr.Zone, rr.State)
		}
	}

	// There's already an existing related operation, check status and decide the next action.
	err := client.fetchOp(op, rr.ProjectID, rr.Zone, rr.Name)
	opType := client.api.extractOpInfo(op).operationType
	switch {
	// Cancel operation is finished, trigger Delete and set ToBeReported report state.
	case err == nil && opType == cancelOpType:
		klog.Infof("Cancel operation on Resize Request %s in mig %s in zone %s finished successfully, will proceed with Delete operation", rr.Name, rr.MigName, rr.Zone)
		client.cache.removeOperation(rr.ID)
		client.SetReportState(rr, ToBeReportedState)
		return deleteAction, nil
	// Delete operation is finished, we're done.
	case err == nil && opType == deleteOpType:
		client.cache.remove(rr.ID)
		return noAction, nil
	// Operation is still running/couldn't be retrieved, we'll try next time.
	case errors.Is(err, errOperationStillRunning) || errors.Is(err, errZoneOperationsAPI):
		return noAction, nil
	// Cancel failed due to RR already being provisioned, skip; this request will be evaluated based on newer state next time.
	case opType == cancelOpType && errors.Is(err, errCancelConditionNotMet):
		klog.Infof("Cancel operation on Resize Request %s in mig %s in zone %s was triggered too late, finished with: %v. Further handling will continue in next loop.", rr.Name, rr.MigName, rr.Zone, err)
		client.cache.removeOperation(rr.ID)
		return noAction, nil
	// There were unknown errors, we have to retry Cancel.
	case opType == cancelOpType:
		client.cache.removeOperation(rr.ID)
		return cancelAction, err
	// There were unknown errors, we have to retry Delete.
	case opType == deleteOpType:
		client.cache.removeOperation(rr.ID)
		return deleteAction, err
	// Operation type was unrecognized, return error.
	default:
		return invalidAction, fmt.Errorf("cannot sync operation of unsupported type %q on Resize Request %s in mig %s in zone %s", opType, rr.Name, rr.MigName, rr.Zone)
	}
}

func (client *resizeRequestClientCommon[Operation, ResizeRequest]) waitForOp(operation *Operation, projectID, zone, resizeRequestName string) error {
	reqInfo := client.api.extractOpInfo(operation)
	start := time.Now()
	time.Sleep(client.operationPollInterval) // Delay the first iteration.
	for time.Since(start) < client.operationWaitTimeout {
		klog.V(4).Infof("Waiting for ResizeRequest creation operation %s %s %s", projectID, zone, reqInfo.name)
		err := client.fetchOp(operation, projectID, zone, resizeRequestName)
		if !errors.Is(err, errOperationStillRunning) && !errors.Is(err, errZoneOperationsAPI) {
			gke_metrics.EmitGceLatency("zone_operations_resize", "get_polling", nil, err, start)
			return err
		}
		time.Sleep(client.operationPollInterval)
	}
	gke_metrics.EmitGceLatency("zone_operations_resize", "get_polling", nil, context.DeadlineExceeded, start)
	return fmt.Errorf("timeout while waiting for ResizeRequest %q creation operation %s on %s to complete", resizeRequestName, reqInfo.name, reqInfo.targetLink)
}

// fetchOp fetches the current state of the operation and depending on its status returns:
// * nil - if the operation has finished successfully (without errors),
// * errZoneOperationsAPI error - when the current state of the operation couldn't be retrieved,
// * errOperationStillRunning error - when the operation is not finished yet,
// * otherwise propagates the errors retrieved from finished unsuccessful operation.
func (client *resizeRequestClientCommon[Operation, ResizeRequest]) fetchOp(operation *Operation, projectID, zone, resizeRequestName string) error {
	reqInfo := client.api.extractOpInfo(operation)
	start := time.Now()
	op, err := client.api.getZoneOp(projectID, zone, reqInfo.name)
	gke_metrics.EmitGceLatency("zone_operations_resize", "get", op, err, start)
	if err != nil {
		logResponseError(err)
		klog.Warningf("Error while getting ResizeRequest %s/%q %s operation %s on %s: %v", projectID, resizeRequestName, reqInfo.operationType, reqInfo.name, reqInfo.targetLink, err)
		return errZoneOperationsAPI
	}
	fetchedInfo := client.api.extractOpInfo(op)
	klog.V(4).Infof("ResizeRequest %q %s operation %s %s %s status: %s", resizeRequestName, reqInfo.operationType, projectID, zone, reqInfo.name, fetchedInfo.status)
	if fetchedInfo.status == "DONE" {
		if fetchedInfo.errors != nil {
			// Check for race condition case of invalid Cancel operation triggered on provisioning/terminal Resize Request.
			if fetchedInfo.operationType == cancelOpType && hasConditionNotMetErrorCode(fetchedInfo.errors) {
				return errCancelConditionNotMet
			}
			multiErr := NewResizeRequestOperationMultiError(len(fetchedInfo.errors))
			for _, opErr := range fetchedInfo.errors {
				multiErr.AppendCreationError(opErr)
			}
			return fmt.Errorf("while %s ResizeRequest.Operation.Get %s/%s for Resize Request %q on %s got error: %w", reqInfo.operationType, projectID, reqInfo.name, resizeRequestName, reqInfo.targetLink, multiErr)
		}
		return nil
	}
	return errOperationStillRunning
}

// hasConditionNotMetErrorCode matches ConditionNotMet errors returned with a finished operation.
func hasConditionNotMetErrorCode(errs []ResizeRequestOperationError) bool {
	for _, err := range errs {
		if err.Code == "CONDITION_NOT_MET" {
			return true
		}
	}
	return false
}

func parseResizeRequestStatus(migRef gce.GceRef, raw rawResizeRequest) (ResizeRequestStatus, error) {
	creationTime, err := time.Parse(time.RFC3339, raw.creationTimestamp)
	if err != nil {
		return ResizeRequestStatus{}, fmt.Errorf("while time.Parse(CreationTimestamp) got error: %w", err)
	}
	state, err := stateFromGCE(raw.state)
	if err != nil {
		return ResizeRequestStatus{}, fmt.Errorf("while parsing resize state got error: %w", err)
	}
	var requestedRunDuration *time.Duration
	if raw.hasRunDuration {
		duration := durationpb.Duration{
			Nanos:   int32(raw.runDurationNanos),
			Seconds: raw.runDurationSeconds,
		}
		requestedRunDuration = protoDuration(duration.AsDuration())
	}
	return ResizeRequestStatus{
		ProjectID:            migRef.Project,
		ID:                   raw.id,
		Name:                 raw.name,
		CreationTime:         creationTime,
		ResizeBy:             raw.resizeBy,
		State:                state,
		MigName:              migRef.Name,
		Zone:                 migRef.Zone,
		RequestedRunDuration: requestedRunDuration,
		Errors:               raw.errors,
		LastAttemptErrors:    raw.lastAttemptErrors,
	}, nil
}

func formatLastAttemptError(code, message, reason string, metadatas map[string]string) DwsStatusError {
	err := DwsStatusError{
		Code:    code,
		Message: message,
	}
	if IsResourcePoolExhaustedErrorCode(code) && metadatas != nil && len(reason) > 0 {
		details := fmt.Sprintf("Reason: %q, VMType: %q, Attachment: %q.", reason, metadatas["vmType"], metadatas["attachment"])
		err.Message = err.Message + " " + details
	}
	return err
}

// resizeRequestOperationCache is NOT THREAD SAFE (some methods do both access and write).
type resizeRequestOperationCache[Operation gceOperation] struct {
	cache *lru.Cache
}

type rrCacheOpStatus[Operation gceOperation] struct {
	op          *Operation
	reportState ResizeRequestReportState
}

func newResizeRequestOperationCache[Operation gceOperation](maxSize int) *resizeRequestOperationCache[Operation] {
	return &resizeRequestOperationCache[Operation]{
		cache: lru.New(maxSize),
	}
}

func (c *resizeRequestOperationCache[Operation]) operation(rrID uint64) *Operation {
	value, found := c.cache.Get(rrID)
	if !found {
		return nil
	}
	return value.(*rrCacheOpStatus[Operation]).op
}

func (c *resizeRequestOperationCache[Operation]) reportState(rrID uint64) ResizeRequestReportState {
	value, found := c.cache.Get(rrID)
	if !found {
		return UnspecifiedReportState
	}
	return value.(*rrCacheOpStatus[Operation]).reportState
}

func (c *resizeRequestOperationCache[Operation]) setOperation(rrID uint64, op *Operation) {
	c.cache.Add(rrID, &rrCacheOpStatus[Operation]{op, c.reportState(rrID)})
}

func (c *resizeRequestOperationCache[Operation]) setReportState(rrID uint64, state ResizeRequestReportState) {
	c.cache.Add(rrID, &rrCacheOpStatus[Operation]{c.operation(rrID), state})
}

func (c *resizeRequestOperationCache[Operation]) removeOperation(rrID uint64) {
	value, found := c.cache.Get(rrID)
	if found {
		state := value.(*rrCacheOpStatus[Operation]).reportState
		c.cache.Add(rrID, &rrCacheOpStatus[Operation]{nil, state})
	}
}

func (c *resizeRequestOperationCache[Operation]) remove(rrID uint64) {
	c.cache.Remove(rrID)
}
