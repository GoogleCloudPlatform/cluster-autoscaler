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

package gceclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	gce_api_beta "google.golang.org/api/compute/v0.beta"
	gce_api "google.golang.org/api/compute/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	test_util "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/utils/ptr"
)

func newTestAutoscalingInternalGceClientWithTimeout(t *testing.T, projectId, url string, provider MigInfoProvider, timeout time.Duration, opts ...Option) *autoscalingInternalGceClient {
	return newTestAutoscalingInternalGceClientWithCustomTransport(t, projectId, url, provider, timeout, nil, opts...)
}

func newTestAutoscalingInternalGceClientWithCustomTransport(t *testing.T, projectId, url string, provider MigInfoProvider, timeout time.Duration, transport http.RoundTripper, opts ...Option) *autoscalingInternalGceClient {
	client := &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}
	gceClient, err := NewCustomAutoscalingInternalGceClient(client, provider, projectId, "", url, "", 120*time.Second, time.Second, experiments.NewMockManager(), opts...)
	if !assert.NoError(t, err) {
		t.Fatalf("fatal error: %v", err)
	}
	return gceClient
}

type transportWithWaitGroup struct {
	handler http.Handler
	wg      *sync.WaitGroup
}

func (t *transportWithWaitGroup) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.handler == nil {
		return nil, errors.New("directTransport: no handler provided")
	}

	ctx := req.Context()
	resCh := make(chan *http.Response, 1)

	// Run handler and the HTTP query in a separate goroutine and increment the WaitGroup counter, so we can observe context cancellation.
	// synctest will track this goroutine completion as part of the test group.
	if t.wg != nil {
		t.wg.Add(1)
	}
	go func() {
		if t.wg != nil {
			defer t.wg.Done()
		}
		recorder := httptest.NewRecorder()
		t.handler.ServeHTTP(recorder, req)
		// Send the result. If the request was already canceled, this might block
		// if we didn't use a buffered channel, but resCh is buffered (1).
		resCh <- recorder.Result()
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-resCh:
		return res, nil
	}
}

func newTestAutoscalingInternalGceClient(t *testing.T, projectId, url string, queuedProvisioning, tpu bool, opts ...Option) *autoscalingInternalGceClient {
	provider := &fakeSingleMigInfoProvider{
		queuedProvisioning: queuedProvisioning,
		tpu:                tpu,
	}
	return newTestAutoscalingInternalGceClientWithTimeout(t, projectId, url, provider, time.Duration(0), opts...)
}

const acceleratorTypesResponse = `
{
 "kind": "compute#acceleratorTypeList",
 "items": [
   {
    "kind": "compute#acceleratorType",
    "id": "1",
    "creationTimestamp": "1969-12-31T16:00:00.000-08:00",
    "name": "nvidia-tesla-k80",
    "description": "NVIDIA Tesla K80",
    "zone": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-a",
    "selfLink": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-a/acceleratorTypes/nvidia-tesla-k80",
    "maximumCardsPerInstance": 8
   }
 ]
}`

func TestFetchAcceleratorTypes(t *testing.T) {
	server := test_util.NewHttpServerMock()
	defer server.Close()
	gceInternalService := newTestAutoscalingInternalGceClient(t, "project1", server.URL, false, false)

	server.On("handle", "/projects/project1/zones/us-central1-a/acceleratorTypes").Return(acceleratorTypesResponse).Times(1)

	acceleratorTypes, err := gceInternalService.FetchAcceleratorTypes("us-central1-a")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(acceleratorTypes.Items))
	assert.Equal(t, "nvidia-tesla-k80", acceleratorTypes.Items[0].Name)
	assert.Equal(t, int64(8), acceleratorTypes.Items[0].MaximumCardsPerInstance)
}

func TestIgnoreInstanceCreationStockoutErrors(t *testing.T) {
	now := time.Now()
	ref := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "ref1"}
	ccwt := 3600 * time.Second

	testCases := []struct {
		name                            string
		enabledFlags                    []string
		provider                        fakeSingleMigInfoProvider
		wantIgnoreStockouts             bool
		wantCapacityCheckTimeoutExpired bool
	}{
		{
			name: "DisabledFlag",
			provider: fakeSingleMigInfoProvider{
				queuedProvisioning:           false,
				flexStart:                    true,
				capacityCheckWaitTimeSeconds: &ccwt,
				scaleUpTime:                  &now,
			},
			wantIgnoreStockouts:             false,
			wantCapacityCheckTimeoutExpired: true,
		},
		{
			name:         "FlexStartNonQueued_false",
			enabledFlags: []string{experiments.FlexStartNonQueuedIgnoreStockoutErrorsEnabledFlag},
			provider: fakeSingleMigInfoProvider{
				queuedProvisioning:           false,
				flexStart:                    false,
				capacityCheckWaitTimeSeconds: &ccwt,
				scaleUpTime:                  &now,
			},
			wantIgnoreStockouts:             false,
			wantCapacityCheckTimeoutExpired: true,
		},
		{
			name:         "CapacityCheckWaitTimeSeconds_error",
			enabledFlags: []string{experiments.FlexStartNonQueuedIgnoreStockoutErrorsEnabledFlag},
			provider: fakeSingleMigInfoProvider{
				queuedProvisioning:           false,
				flexStart:                    true,
				capacityCheckWaitTimeSeconds: nil,
				scaleUpTime:                  &now,
			},
			wantIgnoreStockouts:             true,
			wantCapacityCheckTimeoutExpired: true,
		},
		{
			name:         "ScaleUpTime_error",
			enabledFlags: []string{experiments.FlexStartNonQueuedIgnoreStockoutErrorsEnabledFlag},
			provider: fakeSingleMigInfoProvider{
				queuedProvisioning:           false,
				flexStart:                    true,
				capacityCheckWaitTimeSeconds: &ccwt,
				scaleUpTime:                  nil,
			},
			wantIgnoreStockouts:             true,
			wantCapacityCheckTimeoutExpired: true,
		},
		{
			name:         "nowAfterScaleUpTimePlusCCWT",
			enabledFlags: []string{experiments.FlexStartNonQueuedIgnoreStockoutErrorsEnabledFlag},
			provider: fakeSingleMigInfoProvider{
				queuedProvisioning:           false,
				flexStart:                    true,
				capacityCheckWaitTimeSeconds: &ccwt,
				scaleUpTime:                  ptr.To(now.Add(-ccwt).Add(-time.Minute)),
			},
			wantIgnoreStockouts:             true,
			wantCapacityCheckTimeoutExpired: true,
		},
		{
			name:         "nowBeforeScaleUpTimePlusCCWT",
			enabledFlags: []string{experiments.FlexStartNonQueuedIgnoreStockoutErrorsEnabledFlag},
			provider: fakeSingleMigInfoProvider{
				queuedProvisioning:           false,
				flexStart:                    true,
				capacityCheckWaitTimeSeconds: &ccwt,
				scaleUpTime:                  &now,
			},
			wantIgnoreStockouts:             true,
			wantCapacityCheckTimeoutExpired: false,
		},
	}

	for _, tc := range testCases {
		server := test_util.NewHttpServerMock()
		defer server.Close()
		gceInternalService := newTestAutoscalingInternalGceClientWithTimeout(t, ref.Project, server.URL, &tc.provider, time.Duration(0))
		gceInternalService.experimentsManager = experiments.NewMockManager(tc.enabledFlags...)
		gotIgnoreStockouts, gotCapacityCheckTimeoutExpired := gceInternalService.ignoreInstanceCreationStockoutErrors(ref)
		assert.Equal(t, tc.wantIgnoreStockouts, gotIgnoreStockouts)
		assert.Equal(t, tc.wantCapacityCheckTimeoutExpired, gotCapacityCheckTimeoutExpired)
	}
}

func TestFetchMigInstances(t *testing.T) {
	now := time.Now()
	ref := gce.GceRef{Project: "myprojid", Zone: "myzone"}
	ccwt := 3600 * time.Second

	tests := []struct {
		name             string
		enabledFlags     []string
		provider         fakeSingleMigInfoProvider
		lmiResponse      gce_api.InstanceGroupManagersListManagedInstancesResponse
		lmiPageResponses map[string]gce_api.InstanceGroupManagersListManagedInstancesResponse
		want             []gce.GceInstance
		wantErr          bool
	}{
		{
			name: "all instances good beta call",
			provider: fakeSingleMigInfoProvider{
				queuedProvisioning: true,
			},
			lmiResponse: gce_api.InstanceGroupManagersListManagedInstancesResponse{
				ManagedInstances: []*gce_api.ManagedInstance{
					{
						Id:            2,
						Name:          "myinst_2",
						CurrentAction: "CREATING",
						LastAttempt: &gce_api.ManagedInstanceLastAttempt{
							Errors: &gce_api.ManagedInstanceLastAttemptErrors{},
						},
						InstanceStatus: "RUNNING",
					},
					{
						Id:            42,
						Name:          "myinst_42",
						CurrentAction: "CREATING",
						LastAttempt: &gce_api.ManagedInstanceLastAttempt{
							Errors: &gce_api.ManagedInstanceLastAttemptErrors{},
						},
						InstanceStatus: "SUSPENDED",
					},
				},
			},
			want: []gce.GceInstance{
				{
					Instance: cloudprovider.Instance{
						Id:     "gce://myprojid/myzone/myinst_2",
						Status: &cloudprovider.InstanceStatus{State: cloudprovider.InstanceCreating},
					},
					NumericId: 2,
					GCEStatus: "RUNNING",
				},
				{
					Instance: cloudprovider.Instance{
						Id:     "gce://myprojid/myzone/myinst_42",
						Status: &cloudprovider.InstanceStatus{State: cloudprovider.InstanceCreating},
					},
					NumericId: 42,
					GCEStatus: "SUSPENDED",
				},
			},
		},
		{
			name: "paginated response",
			lmiResponse: gce_api.InstanceGroupManagersListManagedInstancesResponse{
				ManagedInstances: []*gce_api.ManagedInstance{
					{
						Id:            2,
						Name:          "myinst_2",
						CurrentAction: "CREATING",
						LastAttempt: &gce_api.ManagedInstanceLastAttempt{
							Errors: &gce_api.ManagedInstanceLastAttemptErrors{},
						},
					},
					{
						Id:            42,
						Name:          "myinst_42",
						CurrentAction: "CREATING",
						LastAttempt: &gce_api.ManagedInstanceLastAttempt{
							Errors: &gce_api.ManagedInstanceLastAttemptErrors{},
						},
					},
				},
				NextPageToken: "foo",
			},
			lmiPageResponses: map[string]gce_api.InstanceGroupManagersListManagedInstancesResponse{
				"foo": {
					ManagedInstances: []*gce_api.ManagedInstance{
						{
							Id:            123,
							Name:          "myinst_123",
							CurrentAction: "CREATING",
							LastAttempt: &gce_api.ManagedInstanceLastAttempt{
								Errors: &gce_api.ManagedInstanceLastAttemptErrors{},
							},
							InstanceStatus: "RUNNING",
						},
						{
							Id:            456,
							Name:          "myinst_456",
							CurrentAction: "CREATING",
							LastAttempt: &gce_api.ManagedInstanceLastAttempt{
								Errors: &gce_api.ManagedInstanceLastAttemptErrors{},
							},
						},
					},
				},
			},
			want: []gce.GceInstance{
				{
					Instance: cloudprovider.Instance{
						Id:     "gce://myprojid/myzone/myinst_2",
						Status: &cloudprovider.InstanceStatus{State: cloudprovider.InstanceCreating},
					},
					NumericId: 2,
				},
				{
					Instance: cloudprovider.Instance{
						Id:     "gce://myprojid/myzone/myinst_42",
						Status: &cloudprovider.InstanceStatus{State: cloudprovider.InstanceCreating},
					},
					NumericId: 42,
				},
				{
					Instance: cloudprovider.Instance{
						Id:     "gce://myprojid/myzone/myinst_123",
						Status: &cloudprovider.InstanceStatus{State: cloudprovider.InstanceCreating},
					},
					GCEStatus: "RUNNING",
					NumericId: 123,
				},
				{
					Instance: cloudprovider.Instance{
						Id:     "gce://myprojid/myzone/myinst_456",
						Status: &cloudprovider.InstanceStatus{State: cloudprovider.InstanceCreating},
					},
					NumericId: 456,
				},
			},
		},
		{
			name: "paginated response, more pages",
			lmiResponse: gce_api.InstanceGroupManagersListManagedInstancesResponse{
				ManagedInstances: []*gce_api.ManagedInstance{
					{
						Id:            2,
						Name:          "myinst_2",
						CurrentAction: "CREATING",
						LastAttempt: &gce_api.ManagedInstanceLastAttempt{
							Errors: &gce_api.ManagedInstanceLastAttemptErrors{},
						},
					},
					{
						Id:            42,
						Name:          "myinst_42",
						CurrentAction: "CREATING",
						LastAttempt: &gce_api.ManagedInstanceLastAttempt{
							Errors: &gce_api.ManagedInstanceLastAttemptErrors{},
						},
					},
				},
				NextPageToken: "foo",
			},
			lmiPageResponses: map[string]gce_api.InstanceGroupManagersListManagedInstancesResponse{
				"foo": {
					ManagedInstances: []*gce_api.ManagedInstance{
						{
							Id:            123,
							Name:          "myinst_123",
							CurrentAction: "CREATING",
							LastAttempt: &gce_api.ManagedInstanceLastAttempt{
								Errors: &gce_api.ManagedInstanceLastAttemptErrors{},
							},
						},
						{
							Id:            456,
							Name:          "myinst_456",
							CurrentAction: "CREATING",
							LastAttempt: &gce_api.ManagedInstanceLastAttempt{
								Errors: &gce_api.ManagedInstanceLastAttemptErrors{},
							},
						},
					},
					NextPageToken: "bar",
				},
				"bar": {
					ManagedInstances: []*gce_api.ManagedInstance{
						{
							Id:            789,
							Name:          "myinst_789",
							CurrentAction: "CREATING",
							LastAttempt: &gce_api.ManagedInstanceLastAttempt{
								Errors: &gce_api.ManagedInstanceLastAttemptErrors{},
							},
						},
						{
							Id:            666,
							Name:          "myinst_666",
							CurrentAction: "CREATING",
							LastAttempt: &gce_api.ManagedInstanceLastAttempt{
								Errors: &gce_api.ManagedInstanceLastAttemptErrors{},
							},
						},
					},
				},
			},
			want: []gce.GceInstance{
				{
					Instance: cloudprovider.Instance{
						Id:     "gce://myprojid/myzone/myinst_2",
						Status: &cloudprovider.InstanceStatus{State: cloudprovider.InstanceCreating},
					},
					NumericId: 2,
				},
				{
					Instance: cloudprovider.Instance{
						Id:     "gce://myprojid/myzone/myinst_42",
						Status: &cloudprovider.InstanceStatus{State: cloudprovider.InstanceCreating},
					},
					NumericId: 42,
				},
				{
					Instance: cloudprovider.Instance{
						Id:     "gce://myprojid/myzone/myinst_123",
						Status: &cloudprovider.InstanceStatus{State: cloudprovider.InstanceCreating},
					},
					NumericId: 123,
				},
				{
					Instance: cloudprovider.Instance{
						Id:     "gce://myprojid/myzone/myinst_456",
						Status: &cloudprovider.InstanceStatus{State: cloudprovider.InstanceCreating},
					},
					NumericId: 456,
				},
				{
					Instance: cloudprovider.Instance{
						Id:     "gce://myprojid/myzone/myinst_789",
						Status: &cloudprovider.InstanceStatus{State: cloudprovider.InstanceCreating},
					},
					NumericId: 789,
				},
				{
					Instance: cloudprovider.Instance{
						Id:     "gce://myprojid/myzone/myinst_666",
						Status: &cloudprovider.InstanceStatus{State: cloudprovider.InstanceCreating},
					},
					NumericId: 666,
				},
			},
		},
		{
			name: "instances queued and deleting beta call",
			provider: fakeSingleMigInfoProvider{
				queuedProvisioning: true,
			},
			lmiResponse: gce_api.InstanceGroupManagersListManagedInstancesResponse{
				ManagedInstances: []*gce_api.ManagedInstance{
					{
						Id:            2,
						Name:          "myinst_2",
						CurrentAction: "QUEUING",
						LastAttempt: &gce_api.ManagedInstanceLastAttempt{
							Errors: &gce_api.ManagedInstanceLastAttemptErrors{},
						},
						InstanceStatus: "STAGING",
					},
					{
						Id:            42,
						Name:          "myinst_42",
						CurrentAction: "DELETING",
						LastAttempt: &gce_api.ManagedInstanceLastAttempt{
							Errors: &gce_api.ManagedInstanceLastAttemptErrors{},
						},
					},
				},
			},
			want: []gce.GceInstance{
				{
					Instance: cloudprovider.Instance{
						Id:     "gce://myprojid/myzone/myinst_2",
						Status: &cloudprovider.InstanceStatus{State: cloudprovider.InstanceCreating},
					},
					GCEStatus: "STAGING",
					NumericId: 2,
				},
				{
					Instance: cloudprovider.Instance{
						Id:     "gce://myprojid/myzone/myinst_42",
						Status: &cloudprovider.InstanceStatus{State: cloudprovider.InstanceDeleting},
					},
					NumericId: 42,
				},
			},
		},
		{
			name: "queuedProvisioning instances with errors beta call - ignore the errors",
			provider: fakeSingleMigInfoProvider{
				queuedProvisioning: true,
			},
			lmiResponse: gce_api.InstanceGroupManagersListManagedInstancesResponse{
				ManagedInstances: []*gce_api.ManagedInstance{
					{
						Id:            2,
						Name:          "myinst_2",
						CurrentAction: "CREATING",
						LastAttempt: &gce_api.ManagedInstanceLastAttempt{
							Errors: &gce_api.ManagedInstanceLastAttemptErrors{
								Errors: []*gce_api.ManagedInstanceLastAttemptErrorsErrors{
									{
										Code: "IP_SPACE_EXHAUSTED",
									},
								},
							},
						},
					},
					{
						Id:            42,
						Name:          "myinst_42",
						CurrentAction: "CREATING",
						LastAttempt: &gce_api.ManagedInstanceLastAttempt{
							Errors: &gce_api.ManagedInstanceLastAttemptErrors{
								Errors: []*gce_api.ManagedInstanceLastAttemptErrorsErrors{
									{
										Code: zoneResourcePoolExhaustedWithDetails,
									},
								},
							},
						},
					},
					{
						Id:            101,
						Name:          "myinst_101",
						CurrentAction: "CREATING",
						LastAttempt: &gce_api.ManagedInstanceLastAttempt{
							Errors: &gce_api.ManagedInstanceLastAttemptErrors{
								Errors: []*gce_api.ManagedInstanceLastAttemptErrorsErrors{
									{
										Code:    "CONDITION_NOT_MET",
										Message: "Instance 'myinst_101' creation failed: Constraint constraints/compute.vmExternalIpAccess violated for project 1234567890.",
									},
								},
							},
						},
					},
				},
			},
			want: []gce.GceInstance{
				{
					Instance: cloudprovider.Instance{
						Id: "gce://myprojid/myzone/myinst_2",
						Status: &cloudprovider.InstanceStatus{
							State: cloudprovider.InstanceCreating,
						},
					},
					NumericId: 2,
				},
				{
					Instance: cloudprovider.Instance{
						Id: "gce://myprojid/myzone/myinst_42",
						Status: &cloudprovider.InstanceStatus{
							State: cloudprovider.InstanceCreating,
						},
					},
					NumericId: 42,
				},
				{
					Instance: cloudprovider.Instance{
						Id: "gce://myprojid/myzone/myinst_101",
						Status: &cloudprovider.InstanceStatus{
							State: cloudprovider.InstanceCreating,
						},
					},
					NumericId: 101,
				},
			},
		},
		{
			name: "tpu instances with errors beta call - propagate the errors",
			provider: fakeSingleMigInfoProvider{
				tpu: true,
			},
			lmiResponse: gce_api.InstanceGroupManagersListManagedInstancesResponse{
				ManagedInstances: []*gce_api.ManagedInstance{
					{
						Id:            2,
						Name:          "myinst_2",
						CurrentAction: "CREATING",
						LastAttempt: &gce_api.ManagedInstanceLastAttempt{
							Errors: &gce_api.ManagedInstanceLastAttemptErrors{
								Errors: []*gce_api.ManagedInstanceLastAttemptErrorsErrors{
									{
										Code: "IP_SPACE_EXHAUSTED",
									},
								},
							},
						},
					},
					{
						Id:            42,
						Name:          "myinst_42",
						CurrentAction: "CREATING",
						LastAttempt: &gce_api.ManagedInstanceLastAttempt{
							Errors: &gce_api.ManagedInstanceLastAttemptErrors{
								Errors: []*gce_api.ManagedInstanceLastAttemptErrorsErrors{
									{
										Code: zoneResourcePoolExhaustedWithDetails,
									},
								},
							},
						},
					},
					{
						Id:            101,
						Name:          "myinst_101",
						CurrentAction: "CREATING",
						LastAttempt: &gce_api.ManagedInstanceLastAttempt{
							Errors: &gce_api.ManagedInstanceLastAttemptErrors{
								Errors: []*gce_api.ManagedInstanceLastAttemptErrorsErrors{
									{
										Code:    "CONDITION_NOT_MET",
										Message: "Instance 'myinst_101' creation failed: Constraint constraints/compute.vmExternalIpAccess violated for project 1234567890.",
									},
								},
							},
						},
					},
				},
			},
			want: []gce.GceInstance{
				{
					Instance: cloudprovider.Instance{
						Id: "gce://myprojid/myzone/myinst_2",
						Status: &cloudprovider.InstanceStatus{
							State: cloudprovider.InstanceCreating,
							ErrorInfo: &cloudprovider.InstanceErrorInfo{
								ErrorCode:  "IP_SPACE_EXHAUSTED",
								ErrorClass: cloudprovider.OtherErrorClass,
							},
						},
					},
					NumericId: 2,
				},
				{
					Instance: cloudprovider.Instance{
						Id: "gce://myprojid/myzone/myinst_42",
						Status: &cloudprovider.InstanceStatus{
							State: cloudprovider.InstanceCreating,
							ErrorInfo: &cloudprovider.InstanceErrorInfo{
								ErrorCode:  "RESOURCE_POOL_EXHAUSTED",
								ErrorClass: cloudprovider.OutOfResourcesErrorClass,
							},
						},
					},
					NumericId: 42,
				},
				{
					Instance: cloudprovider.Instance{
						Id: "gce://myprojid/myzone/myinst_101",
						Status: &cloudprovider.InstanceStatus{
							State: cloudprovider.InstanceCreating,
							ErrorInfo: &cloudprovider.InstanceErrorInfo{
								ErrorCode:    "VM_EXTERNAL_IP_ACCESS_POLICY_CONSTRAINT",
								ErrorClass:   cloudprovider.OtherErrorClass,
								ErrorMessage: "Instance 'myinst_101' creation failed: Constraint constraints/compute.vmExternalIpAccess violated for project 1234567890.",
							},
						},
					},
					NumericId: 101,
				},
			},
		},
		{
			name: "ignoreStockoutErrors_false_returnWithStockoutErrors",
			lmiResponse: gce_api.InstanceGroupManagersListManagedInstancesResponse{
				ManagedInstances: []*gce_api.ManagedInstance{
					{
						Id:            1,
						Name:          "inst1",
						CurrentAction: "CREATING",
						LastAttempt: &gce_api.ManagedInstanceLastAttempt{
							Errors: &gce_api.ManagedInstanceLastAttemptErrors{
								Errors: []*gce_api.ManagedInstanceLastAttemptErrorsErrors{
									{
										Code: "QUOTA_EXCEEDED", // ErrorClass == cloudprovider.OutOfResourcesErrorClass; see GetErrorInfo()
									},
								},
							},
						},
					},
					{
						Id:            2,
						Name:          "inst2",
						CurrentAction: "CREATING",
						LastAttempt: &gce_api.ManagedInstanceLastAttempt{
							Errors: &gce_api.ManagedInstanceLastAttemptErrors{
								Errors: []*gce_api.ManagedInstanceLastAttemptErrorsErrors{
									{
										Code: "IP_SPACE_EXHAUSTED", // ErrorClass == cloudprovider.OtherErrorClass; see GetErrorInfo()
									},
								},
							},
						},
					},
				},
			},
			want: []gce.GceInstance{
				{
					Instance: cloudprovider.Instance{
						Id: "gce://myprojid/myzone/inst1",
						Status: &cloudprovider.InstanceStatus{
							State: cloudprovider.InstanceCreating,
							ErrorInfo: &cloudprovider.InstanceErrorInfo{
								ErrorCode:  "QUOTA_EXCEEDED",
								ErrorClass: cloudprovider.OutOfResourcesErrorClass,
							},
						},
					},
					NumericId: 1,
				},
				{
					Instance: cloudprovider.Instance{
						Id: "gce://myprojid/myzone/inst2",
						Status: &cloudprovider.InstanceStatus{
							State: cloudprovider.InstanceCreating,
							ErrorInfo: &cloudprovider.InstanceErrorInfo{
								ErrorCode:  "IP_SPACE_EXHAUSTED",
								ErrorClass: cloudprovider.OtherErrorClass,
							},
						},
					},
					NumericId: 2,
				},
			},
		},
		{
			name:         "ignoreStockouts_true_returnWithoutStockoutErrors",
			enabledFlags: []string{experiments.FlexStartNonQueuedIgnoreStockoutErrorsEnabledFlag},
			provider: fakeSingleMigInfoProvider{
				queuedProvisioning:           false,
				flexStart:                    true,
				capacityCheckWaitTimeSeconds: &ccwt,
				scaleUpTime:                  &now,
			},
			lmiResponse: gce_api.InstanceGroupManagersListManagedInstancesResponse{
				ManagedInstances: []*gce_api.ManagedInstance{
					{
						Id:            1,
						Name:          "inst1",
						CurrentAction: "CREATING",
						LastAttempt: &gce_api.ManagedInstanceLastAttempt{
							Errors: &gce_api.ManagedInstanceLastAttemptErrors{
								Errors: []*gce_api.ManagedInstanceLastAttemptErrorsErrors{
									{
										Code: "QUOTA_EXCEEDED", // ErrorClass == cloudprovider.OutOfResourcesErrorClass; see GetErrorInfo()
									},
								},
							},
						},
					},
					{
						Id:            2,
						Name:          "inst2",
						CurrentAction: "CREATING",
						LastAttempt: &gce_api.ManagedInstanceLastAttempt{
							Errors: &gce_api.ManagedInstanceLastAttemptErrors{
								Errors: []*gce_api.ManagedInstanceLastAttemptErrorsErrors{
									{
										Code: "IP_SPACE_EXHAUSTED", // ErrorClass == cloudprovider.OtherErrorClass; see GetErrorInfo()
									},
								},
							},
						},
					},
				},
			},
			want: []gce.GceInstance{
				{
					Instance: cloudprovider.Instance{
						Id: "gce://myprojid/myzone/inst1",
						Status: &cloudprovider.InstanceStatus{
							State: cloudprovider.InstanceCreating,
						},
					},
					NumericId: 1,
				},
				{
					Instance: cloudprovider.Instance{
						Id: "gce://myprojid/myzone/inst2",
						Status: &cloudprovider.InstanceStatus{
							State: cloudprovider.InstanceCreating,
							ErrorInfo: &cloudprovider.InstanceErrorInfo{
								ErrorCode:  "IP_SPACE_EXHAUSTED",
								ErrorClass: cloudprovider.OtherErrorClass,
							},
						},
					},
					NumericId: 2,
				},
			},
		},
		{
			name:         "ignoreStockouts_false_capacityCheckTimeoutExpired_false_returnWithoutStockoutErrorsInStagingAndRunningInstances",
			enabledFlags: []string{experiments.FlexStartNonQueuedIgnoreStockoutErrorsEnabledFlag},
			provider: fakeSingleMigInfoProvider{
				queuedProvisioning:           false,
				flexStart:                    true,
				capacityCheckWaitTimeSeconds: &ccwt,
				scaleUpTime:                  ptr.To(now.Add(-ccwt).Add(-time.Minute)),
			},
			lmiResponse: gce_api.InstanceGroupManagersListManagedInstancesResponse{
				ManagedInstances: []*gce_api.ManagedInstance{
					{
						Id:             1,
						Name:           "inst1",
						CurrentAction:  "CREATING",
						InstanceStatus: "RUNNING",
						LastAttempt: &gce_api.ManagedInstanceLastAttempt{
							Errors: &gce_api.ManagedInstanceLastAttemptErrors{
								Errors: []*gce_api.ManagedInstanceLastAttemptErrorsErrors{
									{
										Code: "QUOTA_EXCEEDED", // ErrorClass == cloudprovider.OutOfResourcesErrorClass; see GetErrorInfo()
									},
								},
							},
						},
					},
					{
						Id:             2,
						Name:           "inst2",
						CurrentAction:  "CREATING",
						InstanceStatus: "STAGING",
						LastAttempt: &gce_api.ManagedInstanceLastAttempt{
							Errors: &gce_api.ManagedInstanceLastAttemptErrors{
								Errors: []*gce_api.ManagedInstanceLastAttemptErrorsErrors{
									{
										Code: "QUOTA_EXCEEDED", // ErrorClass == cloudprovider.OutOfResourcesErrorClass; see GetErrorInfo()
									},
								},
							},
						},
					},
					{
						Id:            3,
						Name:          "inst3",
						CurrentAction: "CREATING",
						LastAttempt: &gce_api.ManagedInstanceLastAttempt{
							Errors: &gce_api.ManagedInstanceLastAttemptErrors{
								Errors: []*gce_api.ManagedInstanceLastAttemptErrorsErrors{
									{
										Code: "QUOTA_EXCEEDED", // ErrorClass == cloudprovider.OutOfResourcesErrorClass; see GetErrorInfo()
									},
								},
							},
						},
					},
					{
						Id:            4,
						Name:          "inst4",
						CurrentAction: "CREATING",
						LastAttempt: &gce_api.ManagedInstanceLastAttempt{
							Errors: &gce_api.ManagedInstanceLastAttemptErrors{
								Errors: []*gce_api.ManagedInstanceLastAttemptErrorsErrors{
									{
										Code: "IP_SPACE_EXHAUSTED", // ErrorClass == cloudprovider.OtherErrorClass; see GetErrorInfo()
									},
								},
							},
						},
					},
				},
			},
			want: []gce.GceInstance{
				{
					Instance: cloudprovider.Instance{
						Id: "gce://myprojid/myzone/inst1",
						Status: &cloudprovider.InstanceStatus{
							State: cloudprovider.InstanceCreating,
						},
					},
					GCEStatus: "RUNNING",
					NumericId: 1,
				},
				{
					Instance: cloudprovider.Instance{
						Id: "gce://myprojid/myzone/inst2",
						Status: &cloudprovider.InstanceStatus{
							State: cloudprovider.InstanceCreating,
						},
					},
					GCEStatus: "STAGING",
					NumericId: 2,
				},
				{
					Instance: cloudprovider.Instance{
						Id: "gce://myprojid/myzone/inst3",
						Status: &cloudprovider.InstanceStatus{
							State: cloudprovider.InstanceCreating,
							ErrorInfo: &cloudprovider.InstanceErrorInfo{
								ErrorCode:  "QUOTA_EXCEEDED",
								ErrorClass: cloudprovider.OutOfResourcesErrorClass,
							},
						},
					},
					NumericId: 3,
				},
				{
					Instance: cloudprovider.Instance{
						Id: "gce://myprojid/myzone/inst4",
						Status: &cloudprovider.InstanceStatus{
							State: cloudprovider.InstanceCreating,
							ErrorInfo: &cloudprovider.InstanceErrorInfo{
								ErrorCode:  "IP_SPACE_EXHAUSTED",
								ErrorClass: cloudprovider.OtherErrorClass,
							},
						},
					},
					NumericId: 4,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()

			gceInternalService := newTestAutoscalingInternalGceClientWithTimeout(t, ref.Project, server.URL, &tt.provider, time.Duration(0))
			gceInternalService.experimentsManager = experiments.NewMockManager(tt.enabledFlags...)

			b, err := json.Marshal(tt.lmiResponse)
			assert.NoError(t, err)
			server.On("handle", "/projects/myprojid/zones/myzone/instanceGroupManagers/listManagedInstances").Return(string(b)).Times(1)
			for token, response := range tt.lmiPageResponses {
				b, err := json.Marshal(response)
				assert.NoError(t, err)
				server.On("handle", "/projects/myprojid/zones/myzone/instanceGroupManagers/listManagedInstances", token).Return(string(b)).Times(1)
			}

			got, err := gceInternalService.FetchMigInstances(ref)
			if (err != nil) != tt.wantErr {
				t.Errorf("autoscalingInternalGceClient.FetchMigInstances() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("autoscalingInternalGceClient.FetchMigInstances() diff (-want +got): %s", diff)
			}
		})
	}
}

// NOTE: pagination operations can't be tested with context timeouts as it's not possible
// to control per call timeouts as context is global per operation
func TestAutoscalingClientTimeouts(t *testing.T) {
	// non zero timeout to indicate that timeout should be respected for http client
	zeroDuration := 1 * time.Nanosecond
	tests := map[string]struct {
		clientFunc              func(*autoscalingInternalGceClient) error
		httpTimeout             time.Duration
		operationPerCallTimeout *time.Duration
	}{
		"FetchAcceleratorTypes_ContextTimeout": {
			clientFunc: func(client *autoscalingInternalGceClient) error {
				_, err := client.FetchAcceleratorTypes("")
				return err
			},
			operationPerCallTimeout: &zeroDuration,
		},
		"FetchAcceleratorTypes_HttpTimeout": {
			clientFunc: func(client *autoscalingInternalGceClient) error {
				_, err := client.FetchAcceleratorTypes("")
				return err
			},
			httpTimeout: zeroDuration,
		},
		"FetchMigInstances_HttpTimeout": {
			clientFunc: func(client *autoscalingInternalGceClient) error {
				_, err := client.FetchMigInstances(gce.GceRef{})
				return err
			},
			httpTimeout: zeroDuration,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					time.Sleep(50 * time.Millisecond)
					w.Header().Set("Content-Type", "application/json")
					w.Write([]byte(`{"status": "unreachable"}`))
				})

				// Since we are simulating the server side timeouts here it is better to use the synctest bubbles and custom transport
				var wg sync.WaitGroup
				client := newTestAutoscalingInternalGceClientWithCustomTransport(t, "project", "", &fakeSingleMigInfoProvider{}, tc.httpTimeout, &transportWithWaitGroup{handler: handler, wg: &wg})
				if tc.operationPerCallTimeout != nil {
					client.operationPerCallTimeout = *tc.operationPerCallTimeout
				}
				err := tc.clientFunc(client)
				wg.Wait()
				// NOTE: unable to test with ErrorIs as http errors are not wrapping an err, but overwriting it
				assert.ErrorContains(t, err, context.DeadlineExceeded.Error())
			})
		})
	}
}

type fakeSingleMigInfoProvider struct {
	capacityCheckWaitTimeSeconds *time.Duration
	scaleUpTime                  *time.Time
	flexStart                    bool
	queuedProvisioning           bool
	tpu                          bool
}

func (f *fakeSingleMigInfoProvider) CapacityCheckWaitTimeSeconds(_ gce.GceRef) (time.Duration, error) {
	if f.capacityCheckWaitTimeSeconds == nil {
		return 0, fmt.Errorf("failed to find CapacityCheckWaitTimeSeconds")
	}
	return *f.capacityCheckWaitTimeSeconds, nil
}

func (f *fakeSingleMigInfoProvider) ScaleUpTime(_ gce.GceRef) (time.Time, error) {
	if f.scaleUpTime == nil {
		return time.Time{}, fmt.Errorf("failed to find ScaleUpTime")
	}
	return *f.scaleUpTime, nil
}

func (f *fakeSingleMigInfoProvider) FlexStartNonQueued(_ gce.GceRef) bool {
	return f.flexStart && !f.queuedProvisioning
}

func (f *fakeSingleMigInfoProvider) QueuedProvisioning(_ gce.GceRef) bool {
	return f.queuedProvisioning
}

func (f *fakeSingleMigInfoProvider) IsTpuMig(_ gce.GceRef) bool {
	return f.tpu
}

// TestFetchReservationBlocksInReservation tests the reservation blocks api call from GCE.
func TestFetchReservationBlocksInReservation(t *testing.T) {
	tests := []struct {
		name                  string
		projectID             string
		zone                  string
		reservationName       string
		response              *gce_api_beta.ReservationBlocksListResponse
		pages                 map[string]*gce_api_beta.ReservationBlocksListResponse
		wantReservationBlocks []*GceReservationBlock
		warning               *gce_api_beta.ReservationBlocksListResponseWarning
		wantErr               bool
	}{
		{
			name:            "Success",
			projectID:       "test-project",
			zone:            "test-zone",
			reservationName: "test-reservation",
			response: &gce_api_beta.ReservationBlocksListResponse{
				Items: []*gce_api_beta.ReservationBlock{
					{
						Name:  "test-block-1",
						Count: 1,
					},
					{
						Name:  "test-block-2",
						Count: 2,
					},
				},
			},
			wantReservationBlocks: []*GceReservationBlock{
				{
					Name:  "test-block-1",
					Count: 1,
				},
				{
					Name:  "test-block-2",
					Count: 2,
				},
			},
			wantErr: false,
		},
		{
			name:            "Empty response",
			projectID:       "test-project",
			zone:            "test-zone",
			reservationName: "test-reservation",
			response: &gce_api_beta.ReservationBlocksListResponse{
				Items: []*gce_api_beta.ReservationBlock{},
			},
			wantReservationBlocks: []*GceReservationBlock{},
			wantErr:               false,
		},
		{
			name:            "Paginated response",
			projectID:       "test-project",
			zone:            "test-zone",
			reservationName: "test-reservation",
			response: &gce_api_beta.ReservationBlocksListResponse{
				Items: []*gce_api_beta.ReservationBlock{
					{
						Name:  "test-block-1",
						Count: 1,
					},
				},
				NextPageToken: "page2",
			},
			pages: map[string]*gce_api_beta.ReservationBlocksListResponse{
				"page2": {
					Items: []*gce_api_beta.ReservationBlock{
						{
							Name:  "test-block-2",
							Count: 2,
						},
					},
				},
			},
			wantReservationBlocks: []*GceReservationBlock{
				{
					Name:  "test-block-1",
					Count: 1,
				},
				{
					Name:  "test-block-2",
					Count: 2,
				},
			},
			wantErr: false,
		},
		{
			name:            "Warning message",
			projectID:       "test-project",
			zone:            "test-zone",
			reservationName: "test-reservation",
			response: &gce_api_beta.ReservationBlocksListResponse{
				Items: []*gce_api_beta.ReservationBlock{
					{
						Name:  "test-block-1",
						Count: 1,
					},
				},
				Warning: &gce_api_beta.ReservationBlocksListResponseWarning{
					Code:    "NO_RESULTS_ON_PAGE",
					Message: "No results on this page",
				},
			},
			wantReservationBlocks: []*GceReservationBlock{
				{
					Name:  "test-block-1",
					Count: 1,
				},
			},
			warning: &gce_api_beta.ReservationBlocksListResponseWarning{
				Code:    "NO_RESULTS_ON_PAGE",
				Message: "No results on this page",
			},
			wantErr: false,
		},
		{
			name:            "Error, missing project",
			projectID:       "",
			zone:            "test-zone",
			reservationName: "test-reservation",
			wantErr:         true,
		},
		{
			name:            "Error, missing reservation",
			projectID:       "test-project",
			zone:            "test-zone",
			reservationName: "",
			wantErr:         true,
		},
		{
			name:            "Error, missing zone",
			projectID:       "test-project",
			zone:            "",
			reservationName: "test-reservation",
			wantErr:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			gceInternalService := newTestAutoscalingInternalGceClient(t, "test-project", server.URL, false, false)

			b, err := json.Marshal(tt.response)
			assert.NoError(t, err)

			path := reservationTestPathBuilder(tt.projectID, tt.zone, tt.reservationName, "", false)
			if tt.wantErr {
				server.On("handle", path).Return("Not Found", http.StatusNotFound).Times(1)
			} else {
				server.On("handle", path).Return(string(b)).Times(1)
			}
			for token, response := range tt.pages {
				b, err := json.Marshal(response)
				assert.NoError(t, err)
				server.On("handle", path, token).Return(string(b)).Times(1)
			}

			got, err := gceInternalService.FetchReservationBlocksInReservation(ReservationRef{
				Project: tt.projectID,
				Zone:    tt.zone,
				Name:    tt.reservationName})
			if (err != nil) != tt.wantErr {
				t.Errorf("autoscalingInternalGceClient.FetchReservationBlocksInReservation() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(tt.wantReservationBlocks, got); diff != "" {
				t.Errorf("autoscalingInternalGceClient.FetchReservationBlocksInReservation() diff (-want +got): %s", diff)
			}
		})
	}
}

// TestFetchReservationSubBlocksInReservationBlock tests the reservation sub-blocks api call from GCE.
func TestFetchReservationSubBlocksInReservationBlock(t *testing.T) {
	tests := []struct {
		name                     string
		projectID                string
		zone                     string
		reservationName          string
		blockName                string
		response                 *gce_api_beta.ReservationSubBlocksListResponse
		pages                    map[string]*gce_api_beta.ReservationSubBlocksListResponse
		wantReservationSubBlocks []*GceReservationSubBlock
		wantErr                  bool
	}{
		{
			name:            "Success",
			projectID:       "test-project",
			zone:            "test-zone",
			reservationName: "test-reservation",
			blockName:       "test-block",
			response: &gce_api_beta.ReservationSubBlocksListResponse{
				Items: []*gce_api_beta.ReservationSubBlock{
					{
						Name:  "test-sub-block-1",
						Count: 1,
					},
					{
						Name:  "test-sub-block-2",
						Count: 2,
					},
				},
			},
			wantReservationSubBlocks: []*GceReservationSubBlock{
				{
					Name:  "test-sub-block-1",
					Count: 1,
				},
				{
					Name:  "test-sub-block-2",
					Count: 2,
				},
			},
			wantErr: false,
		},
		{
			name:            "Empty response",
			projectID:       "test-project",
			zone:            "test-zone",
			reservationName: "test-reservation",
			blockName:       "test-block",
			response: &gce_api_beta.ReservationSubBlocksListResponse{
				Items: []*gce_api_beta.ReservationSubBlock{},
			},
			wantReservationSubBlocks: []*GceReservationSubBlock{},
			wantErr:                  false,
		},
		{
			name:            "Paginated response",
			projectID:       "test-project",
			zone:            "test-zone",
			reservationName: "test-reservation",
			blockName:       "test-block",
			response: &gce_api_beta.ReservationSubBlocksListResponse{
				Items: []*gce_api_beta.ReservationSubBlock{
					{
						Name:  "test-sub-block-1",
						Count: 1,
					},
				},
				NextPageToken: "page2",
			},
			pages: map[string]*gce_api_beta.ReservationSubBlocksListResponse{
				"page2": {
					Items: []*gce_api_beta.ReservationSubBlock{
						{
							Name:  "test-sub-block-2",
							Count: 2,
						},
					},
				},
			},
			wantReservationSubBlocks: []*GceReservationSubBlock{
				{
					Name:  "test-sub-block-1",
					Count: 1,
				},
				{
					Name:  "test-sub-block-2",
					Count: 2,
				},
			},
			wantErr: false,
		},
		{
			name:            "Error, missing project",
			projectID:       "",
			zone:            "test-zone",
			reservationName: "test-reservation",
			blockName:       "test-block",
			wantErr:         true,
		},
		{
			name:            "Error, missing zone",
			projectID:       "test-project",
			zone:            "",
			reservationName: "test-reservation",
			blockName:       "test-block",
			wantErr:         true,
		},
		{
			name:            "Error, missing reservation name",
			projectID:       "test-project",
			zone:            "test-zone",
			reservationName: "",
			blockName:       "test-block",
			wantErr:         true,
		},
		{
			name:            "Error, missing block name",
			projectID:       "test-project",
			zone:            "test-zone",
			reservationName: "test-reservation",
			blockName:       "",
			wantErr:         true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			gceInternalService := newTestAutoscalingInternalGceClient(t, "test-project", server.URL, false, false)

			b, err := json.Marshal(tt.response)
			assert.NoError(t, err)

			path := reservationTestPathBuilder(tt.projectID, tt.zone, tt.reservationName, tt.blockName, true)

			if tt.wantErr {
				server.On("handle", path).Return("Not Found", http.StatusNotFound).Times(1)
			} else {
				server.On("handle", path).Return(string(b)).Times(1)
			}
			for token, response := range tt.pages {
				b, err := json.Marshal(response)
				assert.NoError(t, err)
				server.On("handle", path, token).Return(string(b)).Times(1)
			}

			got, err := gceInternalService.FetchReservationSubBlocksInReservationBlock(ReservationRef{
				Project:   tt.projectID,
				Zone:      tt.zone,
				Name:      tt.reservationName,
				BlockName: tt.blockName})
			if (err != nil) != tt.wantErr {
				t.Errorf("autoscalingInternalGceClient.FetchReservationSubBlocksInReservationBlock() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if diff := cmp.Diff(tt.wantReservationSubBlocks, got); diff != "" {
					t.Errorf("autoscalingInternalGceClient.FetchReservationSubBlocksInReservationBlock() diff (-want +got): %s", diff)
				}
			}
		})
	}
}

// TestFetchResourcePolicies tests the resource policy api call from GCE.
func TestFetchResourcePolicies(t *testing.T) {
	tests := []struct {
		name               string
		projectID          string
		region             string
		response           *gce_api_beta.ResourcePolicyList
		pages              map[string]*gce_api_beta.ResourcePolicyList
		wantResourcePolicy []*GceResourcePolicy
		warning            *gce_api_beta.ResourcePolicyListWarning
		wantErr            bool
	}{
		{
			name:      "Placement Policy",
			projectID: "test-project",
			region:    "test-region",
			response: &gce_api_beta.ResourcePolicyList{
				Items: []*gce_api_beta.ResourcePolicy{
					{
						Name: "test-rp-1",
						GroupPlacementPolicy: &gce_api_beta.ResourcePolicyGroupPlacementPolicy{
							MaxDistance: 1,
							TpuTopology: "2x2",
						},
					},
				},
			},
			wantResourcePolicy: []*GceResourcePolicy{
				{
					Name: "test-rp-1",
					PlacementPolicy: PlacementPolicy{
						TpuTopology: "2x2",
						MaxDistance: 1,
					},
				},
			},
		},
		{
			name:      "Workload Policy",
			projectID: "test-project",
			region:    "test-region",
			response: &gce_api_beta.ResourcePolicyList{
				Items: []*gce_api_beta.ResourcePolicy{
					{
						Name: "test-rp-1",
						WorkloadPolicy: &gce_api_beta.ResourcePolicyWorkloadPolicy{
							Type: "HIGH_AVAILABILITY",
						},
					},
				},
			},
			wantResourcePolicy: []*GceResourcePolicy{
				{
					Name: "test-rp-1",
					WorkloadPolicy: WorkloadPolicy{
						Type: "HIGH_AVAILABILITY",
					},
				},
			},
		},
		{
			name:      "Empty response",
			projectID: "test-project",
			region:    "test-region",
			response: &gce_api_beta.ResourcePolicyList{
				Items: []*gce_api_beta.ResourcePolicy{},
			},
			wantResourcePolicy: []*GceResourcePolicy{},
		},
		{
			name:      "Paginated response",
			projectID: "test-project",
			region:    "test-region",
			response: &gce_api_beta.ResourcePolicyList{
				Items: []*gce_api_beta.ResourcePolicy{
					{
						Name: "test-rp-1",
						GroupPlacementPolicy: &gce_api_beta.ResourcePolicyGroupPlacementPolicy{
							MaxDistance: 1,
							TpuTopology: "2x2",
						},
					},
				},
				NextPageToken: "page2",
			},
			pages: map[string]*gce_api_beta.ResourcePolicyList{
				"page2": {
					Items: []*gce_api_beta.ResourcePolicy{
						{
							Name: "test-rp-2",
							WorkloadPolicy: &gce_api_beta.ResourcePolicyWorkloadPolicy{
								Type: "HIGH_AVAILABILITY",
							},
						},
					},
				},
			},
			wantResourcePolicy: []*GceResourcePolicy{
				{
					Name: "test-rp-1",
					PlacementPolicy: PlacementPolicy{
						TpuTopology: "2x2",
						MaxDistance: 1,
					},
				},
				{
					Name: "test-rp-2",
					WorkloadPolicy: WorkloadPolicy{
						Type: "HIGH_AVAILABILITY",
					},
				},
			},
		},
		{
			name:      "Warning message",
			projectID: "test-project",
			region:    "test-region",
			response: &gce_api_beta.ResourcePolicyList{
				Items: []*gce_api_beta.ResourcePolicy{
					{
						Name: "test-rp-1",
					},
				},
				Warning: &gce_api_beta.ResourcePolicyListWarning{
					Code:    "NO_RESULTS_ON_PAGE",
					Message: "No results on this page",
				},
			},
			wantResourcePolicy: []*GceResourcePolicy{
				{
					Name: "test-rp-1",
				},
			},
			warning: &gce_api_beta.ResourcePolicyListWarning{
				Code:    "NO_RESULTS_ON_PAGE",
				Message: "No results on this page",
			},
		},
		{
			name:      "Error, missing project",
			projectID: "",
			region:    "test-region",
			wantErr:   true,
		},
		{
			name:      "Error, missing region",
			projectID: "test-project",
			region:    "",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			gceInternalService := newTestAutoscalingInternalGceClient(t, "test-project", server.URL, false, false)

			rp, err := json.Marshal(tt.response)
			assert.NoError(t, err)

			rpPath := path.Join("/projects", tt.projectID, "regions", tt.region, "resourcePolicies")

			if tt.wantErr {
				server.On("handle", rpPath).Return("Not Found", http.StatusNotFound).Times(1)
			} else {
				server.On("handle", rpPath).Return(string(rp)).Times(1)
			}
			for token, response := range tt.pages {
				b, err := json.Marshal(response)
				assert.NoError(t, err)
				server.On("handle", rpPath, token).Return(string(b)).Times(1)
			}

			got, err := gceInternalService.FetchResourcePolicies(tt.projectID, tt.region)
			if (err != nil) != tt.wantErr {
				t.Errorf("autoscalingInternalGceClient.FetchResourcePolicies() error = %v, wantErr %v", err, tt.wantErr)
				t.Fatalf("error while fetching resource policies")
			}
			if diff := cmp.Diff(tt.wantResourcePolicy, got); diff != "" {
				t.Errorf("autoscalingInternalGceClient.FetchResourcePolicies() diff (-want +got): %s", diff)
			}
		})
	}
}

func TestFetchNetwork(t *testing.T) {
	tests := []struct {
		name        string
		projectID   string
		networkName string
		wantNetwork *gce_api.Network
		wantErr     bool
	}{
		{
			name:        "Successful network fetch",
			projectID:   "test-project",
			networkName: "default",
			wantNetwork: &gce_api.Network{
				Name:           "default",
				SelfLink:       "https://www.googleapis.com/compute/v1/projects/test-project/global/networks/default",
				SelfLinkWithId: "https://www.googleapis.com/compute/v1/projects/test-project/global/networks/12345",
			},
		},
		{
			name:        "Error, missing projectID",
			projectID:   "",
			networkName: "default",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			networkResponseJSON, err := json.Marshal(tt.wantNetwork)
			assert.NoError(t, err)
			gceInternalService := newTestAutoscalingInternalGceClient(t, tt.projectID, server.URL, false, false)

			path := path.Join("/projects", tt.projectID, "global", "networks", tt.networkName)

			if tt.wantErr {
				server.On("handle", path).Return("Not Found", http.StatusNotFound).Once()
			} else {
				server.On("handle", path).Return(string(networkResponseJSON)).Once()
			}

			got, err := gceInternalService.FetchNetwork(tt.projectID, tt.networkName)
			if (err != nil) != tt.wantErr {
				t.Errorf("autoscalingInternalGceClient.FetchNetwork() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(tt.wantNetwork, got, cmpopts.IgnoreFields(gce_api.Network{}, "ServerResponse")); diff != "" {
				t.Errorf("autoscalingInternalGceClient.FetchNetwork() diff (-want +got): %s", diff)
			}
		})
	}
}

func TestResumeInstances(t *testing.T) {
	tests := []struct {
		name      string
		migRef    gce.GceRef
		instances []gce.GceRef
		initialOp *gce_api.Operation
		finalOp   *gce_api.Operation
		wantErr   *string
	}{
		{
			name:   "resumeInstances_returnError",
			migRef: gce.GceRef{Project: "project1", Zone: "zoneA", Name: "mig1"},
			instances: []gce.GceRef{
				{Project: "project1", Zone: "zoneA", Name: "inst1"},
				{Project: "project1", Zone: "zoneA", Name: "inst2"},
				{Project: "project1", Zone: "zoneA", Name: "inst3"},
			},
			wantErr: ptr.To("failed to call ResumeInstances for mig"),
		},
		{
			name:   "operationError_returnError",
			migRef: gce.GceRef{Project: "project1", Zone: "zoneA", Name: "mig1"},
			instances: []gce.GceRef{
				{Project: "project1", Zone: "zoneA", Name: "inst1"},
				{Project: "project1", Zone: "zoneA", Name: "inst2"},
				{Project: "project1", Zone: "zoneA", Name: "inst3"},
			},
			initialOp: &gce_api.Operation{Name: "op123", Status: "RUNNING"},
			finalOp:   &gce_api.Operation{Name: "op123", Status: "DONE", Error: &gce_api.OperationError{Errors: []*gce_api.OperationErrorErrors{{Message: "error123"}}}},
			wantErr:   ptr.To("failed to wait for ResumeInstances operation"),
		},
		{
			name:   "operationDone_returnNil",
			migRef: gce.GceRef{Project: "project1", Zone: "zoneA", Name: "mig1"},
			instances: []gce.GceRef{
				{Project: "project1", Zone: "zoneA", Name: "inst1"},
				{Project: "project1", Zone: "zoneA", Name: "inst2"},
				{Project: "project1", Zone: "zoneA", Name: "inst3"},
			},
			initialOp: &gce_api.Operation{Name: "op123", Status: "RUNNING"},
			finalOp:   &gce_api.Operation{Name: "op123", Status: "DONE"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()

			path := fmt.Sprintf("/projects/%s/zones/%s/instanceGroupManagers/%s/resumeInstances", tt.migRef.Project, tt.migRef.Zone, tt.migRef.Name)
			if tt.initialOp == nil {
				server.On("handle", path).Panic("")
			} else {
				initialOpJson, err := json.Marshal(tt.initialOp)
				assert.NoError(t, err)
				server.On("handle", path).Return(string(initialOpJson)).Once()

				path = fmt.Sprintf("/projects/%s/zones/%s/operations/%s/wait", tt.migRef.Project, tt.migRef.Zone, tt.initialOp.Name)
				finalOpJSON, err := json.Marshal(tt.finalOp)
				assert.NoError(t, err)
				server.On("handle", path).Return(string(finalOpJSON)).Once()
			}

			gceInternalService := newTestAutoscalingInternalGceClient(
				t,
				tt.migRef.Project,
				server.URL,
				false,
				false,
				WithInstanceActionPollingFrequency(1*time.Millisecond),
				WithInstanceActionTimeout(5*time.Second),
			)

			if tt.wantErr == nil {
				makeServerReturnInstancesWithStatus(t, server, "RUNNING", tt.migRef, tt.instances)
			}

			err := gceInternalService.ResumeInstances(tt.migRef, tt.instances)

			if tt.wantErr == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, *tt.wantErr, fmt.Sprintf("got %v", err.Error()))
			}
			assert.True(t, server.AssertExpectations(t), "Not all expected calls were made to server")
		})
	}
}

func TestSuspendInstances(t *testing.T) {
	tests := []struct {
		name      string
		migRef    gce.GceRef
		instances []gce.GceRef
		initialOp *gce_api.Operation
		finalOp   *gce_api.Operation
		wantErr   *string
	}{
		{
			name:   "suspendInstances_returnError",
			migRef: gce.GceRef{Project: "project1", Zone: "zoneA", Name: "mig1"},
			instances: []gce.GceRef{
				{Project: "project1", Zone: "zoneA", Name: "inst1"},
				{Project: "project1", Zone: "zoneA", Name: "inst2"},
				{Project: "project1", Zone: "zoneA", Name: "inst3"},
			},
			wantErr: ptr.To("failed to call SuspendInstances for mig"),
		},
		{
			name:   "operationError_returnError",
			migRef: gce.GceRef{Project: "project1", Zone: "zoneA", Name: "mig1"},
			instances: []gce.GceRef{
				{Project: "project1", Zone: "zoneA", Name: "inst1"},
				{Project: "project1", Zone: "zoneA", Name: "inst2"},
				{Project: "project1", Zone: "zoneA", Name: "inst3"},
			},
			initialOp: &gce_api.Operation{Name: "op123", Status: "RUNNING"},
			finalOp:   &gce_api.Operation{Name: "op123", Status: "DONE", Error: &gce_api.OperationError{Errors: []*gce_api.OperationErrorErrors{{Message: "error123"}}}},
			wantErr:   ptr.To("failed to wait for SuspendInstances operation"),
		},
		{
			name:   "operationDone_returnNil",
			migRef: gce.GceRef{Project: "project1", Zone: "zoneA", Name: "mig1"},
			instances: []gce.GceRef{
				{Project: "project1", Zone: "zoneA", Name: "inst1"},
				{Project: "project1", Zone: "zoneA", Name: "inst2"},
				{Project: "project1", Zone: "zoneA", Name: "inst3"},
			},
			initialOp: &gce_api.Operation{Name: "op123", Status: "RUNNING"},
			finalOp:   &gce_api.Operation{Name: "op123", Status: "DONE"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()

			path := fmt.Sprintf("/projects/%s/zones/%s/instanceGroupManagers/%s/suspendInstances", tt.migRef.Project, tt.migRef.Zone, tt.migRef.Name)
			if tt.initialOp == nil {
				server.On("handle", path).Panic("")
			} else {
				initialOpJson, err := json.Marshal(tt.initialOp)
				assert.NoError(t, err)
				server.On("handle", path).Return(string(initialOpJson)).Once()

				path = fmt.Sprintf("/projects/%s/zones/%s/operations/%s/wait", tt.migRef.Project, tt.migRef.Zone, tt.initialOp.Name)
				finalOpJSON, err := json.Marshal(tt.finalOp)
				assert.NoError(t, err)
				server.On("handle", path).Return(string(finalOpJSON)).Once()
			}

			gceInternalService := newTestAutoscalingInternalGceClient(
				t,
				tt.migRef.Project,
				server.URL,
				false,
				false,
				WithInstanceActionPollingFrequency(1*time.Millisecond),
			)

			if tt.wantErr == nil {
				makeServerReturnInstancesWithStatus(t, server, "SUSPENDED", tt.migRef, tt.instances)
			}

			err := gceInternalService.SuspendInstances(tt.migRef, tt.instances, false)

			if tt.wantErr == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, *tt.wantErr, fmt.Sprintf("got %v", err.Error()))
			}
			assert.True(t, server.AssertExpectations(t), "Not all expected calls were made to server")
		})
	}
}

func makeServerReturnInstancesWithStatus(t *testing.T, server *test_util.HttpServerMock, status string, migRef gce.GceRef, instances []gce.GceRef) {
	var managedInstances []*gce_api.ManagedInstance
	for _, inst := range instances {
		managedInstances = append(managedInstances, &gce_api.ManagedInstance{
			Name:           inst.Name,
			InstanceStatus: status,
		})
	}
	lmiResponse := &gce_api.InstanceGroupManagersListManagedInstancesResponse{
		ManagedInstances: managedInstances,
	}
	b, err := json.Marshal(lmiResponse)
	assert.NoError(t, err)
	listPath := fmt.Sprintf("/projects/%s/zones/%s/instanceGroupManagers/%s/listManagedInstances", migRef.Project, migRef.Zone, migRef.Name)
	server.On("handle", listPath).Return(string(b)).Once()
}

func TestGetGkeErrorCode(t *testing.T) {
	testCases := []struct {
		description   string
		errorInfo     *cloudprovider.InstanceErrorInfo
		wantErrorCode string
	}{
		{
			description: "constraints/compute.requireShieldedVm violation error - return GkePersistentOperationError code",
			errorInfo: &cloudprovider.InstanceErrorInfo{
				ErrorCode:    gce.ErrorCodeOther,
				ErrorMessage: "Instance 'myinst_101' creation failed: Constraint constraints/compute.requireShieldedVm violated for project 1234567890.",
			},
			wantErrorCode: GkePersistentOperationError,
		},
		{
			description: "other error - propagate error code",
			errorInfo: &cloudprovider.InstanceErrorInfo{
				ErrorCode:    gce.ErrorCodeVmExternalIpAccessPolicyConstraint,
				ErrorMessage: "Instance 'myinst_101' creation failed: Constraint constraints/compute.vmExternalIpAccess violated for project 1234567890.",
			},
			wantErrorCode: gce.ErrorCodeVmExternalIpAccessPolicyConstraint,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			gotErrorCode := getGkeErrorCode(tc.errorInfo)
			assert.Equal(t, tc.wantErrorCode, gotErrorCode)
		})
	}
}

func reservationTestPathBuilder(projectID, zone, reservationName, blockName string, subBlockTest bool) string {
	segments := []string{
		"/projects", projectID,
		"zones", zone,
		"reservations", reservationName,
		"reservationBlocks", blockName,
	}

	if subBlockTest {
		segments = append(segments, "reservationSubBlocks")
	}

	return path.Join(segments...)
}

// TestFetchAIZones tests the AI zones api call from GCE.
func TestFetchAIZones(t *testing.T) {
	tests := []struct {
		name        string
		projectID   string
		region      string
		response    *gce_api.ZoneList
		pages       map[string]*gce_api.ZoneList
		wantAIZones []string
		wantErr     bool
	}{
		{
			name:      "success",
			projectID: "test-project",
			region:    "test-region",
			response: &gce_api.ZoneList{
				Items: []*gce_api.Zone{
					{
						Name: "ai-zone-1",
					},
					{
						Name: "ai-zone-2",
					},
				},
			},
			wantAIZones: []string{
				"ai-zone-1",
				"ai-zone-2",
			},
			wantErr: false,
		},
		{
			name:      "empty_response",
			projectID: "test-project",
			region:    "test-region",
			response: &gce_api.ZoneList{
				Items: []*gce_api.Zone{},
			},
			wantAIZones: []string{},
			wantErr:     false,
		},
		{
			name:      "paginated_response",
			projectID: "test-project",
			region:    "test-region",
			response: &gce_api.ZoneList{
				Items: []*gce_api.Zone{
					{
						Name: "ai-zone-1",
					},
					{
						Name: "ai-zone-2",
					},
				},
				NextPageToken: "page2",
			},
			pages: map[string]*gce_api.ZoneList{
				"page2": {
					Items: []*gce_api.Zone{
						{
							Name: "ai-zone-3",
						},
						{
							Name: "ai-zone-4",
						},
					},
					NextPageToken: "page3",
				},
				"page3": {
					Items: []*gce_api.Zone{
						{
							Name: "ai-zone-5",
						},
					},
				},
			},
			wantAIZones: []string{
				"ai-zone-1",
				"ai-zone-2",
				"ai-zone-3",
				"ai-zone-4",
				"ai-zone-5",
			},
			wantErr: false,
		},
		{
			name:      "api_returns_error",
			projectID: "test-project",
			region:    "test-region",
			response: &gce_api.ZoneList{
				Items: []*gce_api.Zone{},
			},
			wantAIZones: nil,
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			gceInternalService := newTestAutoscalingInternalGceClient(t, tt.projectID, server.URL, false, false)

			b, err := json.Marshal(tt.response)
			assert.NoError(t, err)

			path := "/projects/test-project/zones"

			if tt.wantErr {
				server.On("handle", path).Return("Error", http.StatusNotFound).Once()
			} else {
				server.On("handle", path).Return(string(b)).Once()
			}
			for token, response := range tt.pages {
				b, err := json.Marshal(response)
				assert.NoError(t, err)
				server.On("handle", path, token).Return(string(b)).Once()
			}

			got, err := gceInternalService.FetchAIZones(tt.region)

			if (err != nil) != tt.wantErr {
				t.Errorf("autoscalingInternalGceClient.FetchAIZones() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			if diff := cmp.Diff(tt.wantAIZones, got); diff != "" {
				t.Errorf("autoscalingInternalGceClient.FetchAIZones() diff (-want +got): %s", diff)
			}
		})
	}
}
