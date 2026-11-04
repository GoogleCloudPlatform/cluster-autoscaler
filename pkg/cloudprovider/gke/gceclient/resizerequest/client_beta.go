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
	"net/http"

	gce_api_beta "google.golang.org/api/compute/v0.beta"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce"
)

type resizeRequestClientBeta struct {
	resizeRequestClientCommon[gce_api_beta.Operation, gce_api_beta.InstanceGroupManagerResizeRequest]
}

var _ ResizeRequestClient = (*resizeRequestClientBeta)(nil)

// NewResizeRequestClientBeta creates a new client to communicate with
// MIG Resize Request API. If gceEndpoint is not empty the base path is overridden.
func NewResizeRequestClientBeta(client *http.Client, projectID, userAgent, gceEndpoint string, resizeRequestMode ResizeRequestMode) (*resizeRequestClientBeta, error) {
	gceBetaService, err := gce_api_beta.New(client)
	if err != nil {
		return nil, err
	}
	gceBetaService.UserAgent = userAgent
	if len(gceEndpoint) > 0 {
		gceBetaService.BasePath = gceEndpoint
	}
	return &resizeRequestClientBeta{
		resizeRequestClientCommon: newResizeRequestClientCommon[gce_api_beta.Operation, gce_api_beta.InstanceGroupManagerResizeRequest](
			betaResizeRequestAPI{service: gceBetaService},
			resizeRequestMode,
		),
	}, nil
}

type betaResizeRequestAPI struct {
	service *gce_api_beta.Service
}

func (a betaResizeRequestAPI) listPages(ctx context.Context, migRef gce.GceRef, onPage func(any, []*gce_api_beta.InstanceGroupManagerResizeRequest)) error {
	return a.service.InstanceGroupManagerResizeRequests.List(migRef.Project, migRef.Zone, migRef.Name).Pages(
		ctx, func(gceResp *gce_api_beta.InstanceGroupManagerResizeRequestsListResponse) error {
			onPage(gceResp, gceResp.Items)
			return nil
		},
	)
}

func (a betaResizeRequestAPI) get(ctx context.Context, migRef gce.GceRef, name string) (*gce_api_beta.InstanceGroupManagerResizeRequest, error) {
	return a.service.InstanceGroupManagerResizeRequests.Get(migRef.Project, migRef.Zone, migRef.Name, name).Context(ctx).Do()
}

func (a betaResizeRequestAPI) insert(ctx context.Context, migRef gce.GceRef, createRequest ResizeRequestCreateRequest) (*gce_api_beta.Operation, error) {
	gceResizeRequest := &gce_api_beta.InstanceGroupManagerResizeRequest{
		Name:     createRequest.Name,
		ResizeBy: createRequest.ResizeBy,
	}
	if createRequest.RequestedRunDuration != nil {
		seconds, nanos := toSecondsAndNanos(*createRequest.RequestedRunDuration)
		gceResizeRequest.RequestedRunDuration = &gce_api_beta.Duration{
			Seconds: seconds,
			Nanos:   nanos,
		}
	}
	return a.service.InstanceGroupManagerResizeRequests.Insert(migRef.Project, migRef.Zone, migRef.Name, gceResizeRequest).Context(ctx).Do()
}

func (a betaResizeRequestAPI) cancel(ctx context.Context, rr ResizeRequestStatus) (*gce_api_beta.Operation, error) {
	return a.service.InstanceGroupManagerResizeRequests.Cancel(rr.ProjectID, rr.Zone, rr.MigName, rr.Name).Context(ctx).Do()
}

func (a betaResizeRequestAPI) delete(ctx context.Context, rr ResizeRequestStatus) (*gce_api_beta.Operation, error) {
	return a.service.InstanceGroupManagerResizeRequests.Delete(rr.ProjectID, rr.Zone, rr.MigName, rr.Name).Context(ctx).Do()
}

func (a betaResizeRequestAPI) getZoneOp(projectID, zone, opName string) (*gce_api_beta.Operation, error) {
	return a.service.ZoneOperations.Get(projectID, zone, opName).Do()
}

func (betaResizeRequestAPI) extractOpInfo(op *gce_api_beta.Operation) opInfo {
	info := opInfo{
		name:          op.Name,
		operationType: op.OperationType,
		targetLink:    op.TargetLink,
		status:        op.Status,
	}
	if op.Error != nil && op.Error.Errors != nil {
		info.errors = make([]ResizeRequestOperationError, 0, len(op.Error.Errors))
		for _, e := range op.Error.Errors {
			info.errors = append(info.errors, ResizeRequestOperationError{Code: e.Code, Message: e.Message})
		}
	}
	return info
}

func (betaResizeRequestAPI) parseResizeRequest(rr *gce_api_beta.InstanceGroupManagerResizeRequest, migRef gce.GceRef) (ResizeRequestStatus, error) {
	raw := rawResizeRequest{
		id:                rr.Id,
		name:              rr.Name,
		creationTimestamp: rr.CreationTimestamp,
		resizeBy:          rr.ResizeBy,
		state:             rr.State,
	}
	if rr.RequestedRunDuration != nil {
		raw.hasRunDuration = true
		raw.runDurationSeconds = rr.RequestedRunDuration.Seconds
		raw.runDurationNanos = rr.RequestedRunDuration.Nanos
	}
	if rr.Status != nil {
		raw.errors = errorsFromGCEBeta(rr.Status.Error)
		raw.lastAttemptErrors = lastAttemptErrorsFromGCEBeta(rr.Status.LastAttempt)
	}
	return parseResizeRequestStatus(migRef, raw)
}

func errorsFromGCEBeta(gceError *gce_api_beta.InstanceGroupManagerResizeRequestStatusError) []DwsStatusError {
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

func lastAttemptErrorsFromGCEBeta(gceLastAttempt *gce_api_beta.InstanceGroupManagerResizeRequestStatusLastAttempt) []DwsStatusError {
	if gceLastAttempt == nil || gceLastAttempt.Error == nil || len(gceLastAttempt.Error.Errors) == 0 {
		return nil
	}
	result := make([]DwsStatusError, 0, len(gceLastAttempt.Error.Errors))
	for _, e := range gceLastAttempt.Error.Errors {
		var reason string
		var metadatas map[string]string
		if len(e.ErrorDetails) > 0 && e.ErrorDetails[0] != nil && e.ErrorDetails[0].ErrorInfo != nil {
			reason = e.ErrorDetails[0].ErrorInfo.Reason
			metadatas = e.ErrorDetails[0].ErrorInfo.Metadatas
		}
		result = append(result, formatLastAttemptError(e.Code, e.Message, reason, metadatas))
	}
	return result
}
