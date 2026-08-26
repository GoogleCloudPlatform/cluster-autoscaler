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
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/stretchr/testify/mock"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
)

type resizeRequestClientFake struct {
	rrs               map[string]map[string]ResizeRequestStatus
	creationTimestamp time.Time
	reportStates      map[string]ResizeRequestReportState
}

func NewResizeRequestClientFake(resizeRequests map[string][]ResizeRequestStatus, creationTimestamp time.Time) ResizeRequestClient {
	res := map[string]map[string]ResizeRequestStatus{}
	for migName, rrs := range resizeRequests {
		res[migName] = map[string]ResizeRequestStatus{}
		for _, rr := range rrs {
			res[migName][rr.Name] = rr
		}
	}
	return &resizeRequestClientFake{
		rrs:               res,
		creationTimestamp: creationTimestamp,
		reportStates:      map[string]ResizeRequestReportState{},
	}
}

func (f *resizeRequestClientFake) ResizeRequests(_ context.Context, migRef gce.GceRef) ([]ResizeRequestStatus, error) {
	if f.rrs[migRef.Name] == nil {
		return nil, fmt.Errorf("Couldn't retrieve Resize Requests, mig %s not found.", migRef)
	}
	return slices.Collect(maps.Values(f.rrs[migRef.Name])), nil
}

func (f *resizeRequestClientFake) ResizeRequest(_ context.Context, migRef gce.GceRef, resizeRequestName string) (ResizeRequestStatus, error) {
	if f.rrs[migRef.Name] == nil {
		return ResizeRequestStatus{}, fmt.Errorf("Couldn't retrieve Resize Request %q, mig %s not found.", resizeRequestName, migRef)
	}
	rr, found := f.rrs[migRef.Name][resizeRequestName]
	if !found {
		return ResizeRequestStatus{}, fmt.Errorf("Resize Request %q in mig %s not found.", resizeRequestName, migRef)
	}
	return rr, nil
}

func (f *resizeRequestClientFake) AdvanceResizeRequestCleanUp(_ context.Context, resizeRequest ResizeRequestStatus) error {
	delete(f.rrs[resizeRequest.MigName], resizeRequest.Name)
	return nil
}

func (f *resizeRequestClientFake) CreateResizeRequest(_ context.Context, migRef gce.GceRef, createRequest ResizeRequestCreateRequest) error {
	if f.rrs[migRef.Name] == nil {
		f.rrs[migRef.Name] = map[string]ResizeRequestStatus{}
	}
	_, found := f.rrs[migRef.Name][createRequest.Name]
	if found {
		return fmt.Errorf("Resize Request %q in mig %s already exists", createRequest.Name, migRef)
	}
	f.rrs[migRef.Name][createRequest.Name] = ResizeRequestStatus{
		ID:                   uint64(12717127),
		Name:                 createRequest.Name,
		CreationTime:         f.creationTimestamp,
		ResizeBy:             createRequest.ResizeBy,
		State:                ResizeRequestStateAccepted,
		ProjectID:            migRef.Project,
		MigName:              migRef.Name,
		Zone:                 migRef.Zone,
		RequestedRunDuration: createRequest.RequestedRunDuration,
		Errors:               nil,
		LastAttemptErrors:    nil,
	}
	return nil
}

func (f *resizeRequestClientFake) ReportState(resizeRequest ResizeRequestStatus) ResizeRequestReportState {
	return f.reportStates[resizeRequest.Name]
}

func (f *resizeRequestClientFake) SetReportState(resizeRequest ResizeRequestStatus, state ResizeRequestReportState) {
	f.reportStates[resizeRequest.Name] = state
}

func (f *resizeRequestClientFake) RegisterFailedResizeRequestsCreation(_ gce.GceRef, _ error, _ int) {
	// not necessary for current unit tests
	panic("not implemented")
}

func (f *resizeRequestClientFake) ResetFailedResizeRequestsCreation(_ gce.GceRef) map[error]int {
	// not necessary for current unit tests
	panic("not implemented")
}

// TO BE DEPRECATED, please use ResizeRequestClientFake instead.
type ResizeRequestClientMock struct {
	mock.Mock
}

func NewResizeRequestClientMock() *ResizeRequestClientMock {
	return &ResizeRequestClientMock{}
}

// ResizeRequests is a mocked method.
func (m *ResizeRequestClientMock) ResizeRequests(ctx context.Context, migRef gce.GceRef) ([]ResizeRequestStatus, error) {
	args := m.Called(ctx, migRef)
	return args.Get(0).([]ResizeRequestStatus), args.Error(1)
}

// ResizeRequest is a mocked method.
func (m *ResizeRequestClientMock) ResizeRequest(ctx context.Context, migRef gce.GceRef, resizeRequestName string) (ResizeRequestStatus, error) {
	args := m.Called(ctx, migRef, resizeRequestName)
	return args.Get(0).(ResizeRequestStatus), args.Error(1)
}

// CreateResizeRequest is a mocked method.
func (m *ResizeRequestClientMock) CreateResizeRequest(ctx context.Context, migRef gce.GceRef, createRequest ResizeRequestCreateRequest) error {
	args := m.Called(ctx, migRef, createRequest)
	return args.Error(0)
}

// AdvanceResizeRequestCleanUp is a mocked method.
func (m *ResizeRequestClientMock) AdvanceResizeRequestCleanUp(ctx context.Context, resizeRequest ResizeRequestStatus) error {
	args := m.Called(ctx, resizeRequest)
	return args.Error(0)
}

// ReportState is a mocked method.
func (m *ResizeRequestClientMock) ReportState(resizeRequest ResizeRequestStatus) ResizeRequestReportState {
	args := m.Called(resizeRequest)
	return args.Get(0).(ResizeRequestReportState)
}

// RegisterFailedResizeRequestsCreation is a mocked method.
func (m *ResizeRequestClientMock) SetReportState(resizeRequest ResizeRequestStatus, state ResizeRequestReportState) {
	m.Called(resizeRequest, state)
}

// RegisterFailedResizeRequestsCreation is a mocked method.
func (m *ResizeRequestClientMock) RegisterFailedResizeRequestsCreation(migRef gce.GceRef, err error, count int) {
	m.Called(migRef, err, count)
}

// ResetFailedResizeRequestsCreation is a mocked method.
func (m *ResizeRequestClientMock) ResetFailedResizeRequestsCreation(migRef gce.GceRef) map[error]int {
	args := m.Called(migRef)
	return args.Get(0).(map[error]int)
}
