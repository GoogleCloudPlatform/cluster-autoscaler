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
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	gce_api_beta "google.golang.org/api/compute/v0.beta"
	gce_api "google.golang.org/api/compute/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/component-base/metrics/legacyregistry"
	gke_metrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	test_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
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

			got, err := gceInternalService.FetchMigInstances(context.TODO(), ref)
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
				_, err := client.FetchMigInstances(context.TODO(), gce.GceRef{})
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
			)

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

// instanceError pairs a terminal error with the instance it was yielded for.
type instanceError struct {
	Ref gce.GceRef
	Err error
}

// nonBlockingError pairs a recoverable failure with the instance it was reported for.
type nonBlockingError struct {
	Ref gce.GceRef
	Err NonBlockingInstanceError
}

// pollReport is everything a PollUntilActionStops iterator reported, split by the outcome of the
// yielded update so tests can assert on each part independently. Each part is in report order.
type pollReport struct {
	// completed holds the instances reported PollCompleted, in the order they were reported.
	completed []gce.GceRef
	// nonBlocking holds the non-blocking failures reported while the instances kept retrying.
	nonBlocking []nonBlockingError
	// aborted holds the instances reported PollAborted, alongside the errors explaining why.
	aborted []instanceError
	// unexpected holds updates with an outcome none of the above buckets covers.
	unexpected []gce.GceRef
}

// collectPoll drains a PollUntilActionStops iterator into a pollReport.
func collectPoll(seq ActionPollSeq) pollReport {
	var out pollReport
	for ref, update := range seq {
		switch u := update.(type) {
		case PollCompleted:
			out.completed = append(out.completed, ref)
		case PollRetrying:
			out.nonBlocking = append(out.nonBlocking, nonBlockingError{Ref: ref, Err: *u.Err})
		case PollAborted:
			out.aborted = append(out.aborted, instanceError{Ref: ref, Err: u.Err})
		default:
			out.unexpected = append(out.unexpected, ref)
		}
	}
	return out
}

// managedInstance builds the ManagedInstance GCE would report for ref while it is running
// currentAction with status instanceStatus. Any lastAttemptErrors are attached as the errors of the
// MIG's most recent attempt on the instance.
func managedInstance(ref gce.GceRef, currentAction InstanceAction, instanceStatus string, lastAttemptErrors ...*gce_api.ManagedInstanceLastAttemptErrorsErrors) *gce_api.ManagedInstance {
	inst := &gce_api.ManagedInstance{
		Name:           ref.Name,
		Instance:       fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s/instances/%s", ref.Project, ref.Zone, ref.Name),
		CurrentAction:  string(currentAction),
		InstanceStatus: instanceStatus,
	}
	if len(lastAttemptErrors) > 0 {
		inst.LastAttempt = &gce_api.ManagedInstanceLastAttempt{
			Errors: &gce_api.ManagedInstanceLastAttemptErrors{Errors: lastAttemptErrors},
		}
	}
	return inst
}

func listManagedInstancesPath(migRef gce.GceRef) string {
	return fmt.Sprintf("/projects/%s/zones/%s/instanceGroupManagers/%s/listManagedInstances", migRef.Project, migRef.Zone, migRef.Name)
}

// pollTestResponder answers the listManagedInstances calls of a single MIG, serving one queued
// response per poll and repeating the last one once the queue runs out, which is what the cases
// that can only end on a timeout rely on.
type pollTestResponder struct {
	path   string
	bodies [][]byte

	mu    sync.Mutex
	calls int
}

// newPollTestResponder marshals one listManagedInstances response per entry of polls.
func newPollTestResponder(t *testing.T, migRef gce.GceRef, polls ...[]*gce_api.ManagedInstance) *pollTestResponder {
	t.Helper()
	responder := &pollTestResponder{path: listManagedInstancesPath(migRef)}
	for _, instances := range polls {
		b, err := json.Marshal(&gce_api.InstanceGroupManagersListManagedInstancesResponse{ManagedInstances: instances})
		if err != nil {
			t.Fatalf("Failed to marshal listManagedInstances response: %v", err)
		}
		responder.bodies = append(responder.bodies, b)
	}
	return responder
}

func (r *pollTestResponder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != r.path {
		http.Error(w, fmt.Sprintf("unexpected request to %q", req.URL.Path), http.StatusNotFound)
		return
	}
	r.mu.Lock()
	body := r.bodies[min(r.calls, len(r.bodies)-1)]
	r.calls++
	r.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.Write(body)
}

// polls returns how many listManagedInstances calls have been served so far.
func (r *pollTestResponder) polls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// pollTestActions are the actions every PollUntilActionStops test is run against. The poll loop is
// action agnostic, so resuming and suspending must behave identically.
var pollTestActions = []InstanceAction{ActionResuming, ActionSuspending}

// newPollTestClient builds a client whose GCE calls responder answers in process, and which gives
// up on action after actionTimeout. Only action's own Giraffe flag is set, so a poll reading the
// flag of another action would wait for that action's default timeout instead. boolFlags overrides
// the Giraffe bool flags of the client, which are otherwise all enabled.
//
// The poll tests run inside a synctest bubble, which lets them wait out the polling interval and
// the action timeout for free. A bubble only moves its clock once every goroutine inside it is
// blocked, and a socket keeps one parked in the netpoller indefinitely, so the calls must not leave
// the process: transportWithWaitGroup hands each request straight to responder on a goroutine of
// the bubble's own.
func newPollTestClient(t *testing.T, action InstanceAction, responder http.Handler, actionTimeout time.Duration, boolFlags map[string]bool) *autoscalingInternalGceClient {
	t.Helper()
	var timeoutFlag string
	switch action {
	case ActionResuming:
		timeoutFlag = experiments.ColdStandbyNodesResumeTimeoutSecondsFlag
	case ActionSuspending:
		timeoutFlag = experiments.ColdStandbyNodesSuspendTimeoutSecondsFlag
	default:
		t.Fatalf("no timeout flag is known for action %q", action)
	}
	timeouts := map[string]string{
		timeoutFlag: strconv.Itoa(int(actionTimeout / time.Second)),
	}
	return newTestAutoscalingInternalGceClientWithCustomTransport(
		t, "project1", "", &fakeSingleMigInfoProvider{}, time.Duration(0),
		&transportWithWaitGroup{handler: responder},
		WithExperimentsManager(experiments.NewMockManagerWithOptions(version.Version{}, boolFlags, timeouts)))
}

func TestPollUntilActionStops(t *testing.T) {
	for _, action := range pollTestActions {
		t.Run(string(action), func(t *testing.T) {
			testPollUntilActionStops(t, action)
		})
	}
}

// testPollUntilActionStops runs the scenario table for a single action. It is a function of its own
// so that the table is not indented behind the per-action subtest.
func testPollUntilActionStops(t *testing.T, actionToTest InstanceAction) {
	migRef := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "mig1"}
	inst1Ref := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "inst1"}
	inst2Ref := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "inst2"}
	stockout := &gce_api.ManagedInstanceLastAttemptErrorsErrors{Code: "STOCKOUT", Message: "Zone stockout"}
	quotaExceeded := &gce_api.ManagedInstanceLastAttemptErrorsErrors{Code: "QUOTA_EXCEEDED", Message: "Quota exceeded"}

	// The instance status GCE reports while the action is under way, and once it is done. The poll
	// loop only passes the former through to the non-blocking errors it reports, but the two actions
	// are mirror images of each other, so each gets its own pair.
	inProgressStatus, doneStatus := "SUSPENDED", "RUNNING"
	if actionToTest == ActionSuspending {
		inProgressStatus, doneStatus = "RUNNING", "SUSPENDED"
	}

	// actionTimeout outlasts the longest run of polls the table queues up, so that only the case
	// meaning to time out does. It deliberately is not a whole multiple of
	// instanceActionPollingFrequency: on a multiple the poll timer and the timeout timer come due
	// on the same select, which picks between them at random.
	const actionTimeout = 5*time.Minute + 2*time.Second

	// neverDone keeps both instances running the action forever, so a row that expects the wait to
	// be skipped fails instead of quietly polling its way to the same result.
	neverDone := [][]*gce_api.ManagedInstance{{
		managedInstance(inst1Ref, actionToTest, ""),
		managedInstance(inst2Ref, actionToTest, ""),
	}}

	tests := []struct {
		name string
		// boolFlags overrides the Giraffe bool flags of the client, which are otherwise all enabled.
		boolFlags map[string]bool
		// noExperimentsManager leaves the client without an experiments manager.
		noExperimentsManager bool
		// polls holds one listManagedInstances response per poll, answered in order. The last entry
		// is replayed if the loop polls more often than there are entries.
		polls     [][]*gce_api.ManagedInstance
		wantPolls int
		instances []gce.GceRef
		// wantCompleted is in report order. No row may have two instances finishing on the same poll:
		// PollUntilActionStops reports them while ranging over a map, so their relative order would
		// be random. Staggering the completions keeps this a meaningful ordering assertion.
		wantCompleted   []gce.GceRef
		wantAborted     []instanceError
		wantNonBlocking []nonBlockingError
	}{
		{
			name: "early signaling for progressive instance readiness",
			polls: [][]*gce_api.ManagedInstance{
				// inst1 is done, inst2 is still running the action.
				{
					managedInstance(inst1Ref, ActionNone, ""),
					managedInstance(inst2Ref, actionToTest, ""),
				},
				// inst2 is done too.
				{
					managedInstance(inst1Ref, ActionNone, ""),
					managedInstance(inst2Ref, ActionNone, ""),
				},
			},
			instances: []gce.GceRef{inst1Ref, inst2Ref},
			wantPolls: 2,
			// inst1 is reported on the first poll, without waiting for inst2.
			wantCompleted: []gce.GceRef{inst1Ref, inst2Ref},
		},
		{
			name: "non-blocking errors yielded during poll",
			polls: [][]*gce_api.ManagedInstance{
				{managedInstance(inst1Ref, actionToTest, inProgressStatus, stockout)},
				// The action finished, but LastAttempt still carries a now-stale error, which must
				// not be reported.
				{managedInstance(inst1Ref, ActionNone, doneStatus,
					&gce_api.ManagedInstanceLastAttemptErrorsErrors{Code: "INTERNAL_ERROR", Message: "Stale failure"})},
			},
			instances:       []gce.GceRef{inst1Ref},
			wantPolls:       2,
			wantCompleted:   []gce.GceRef{inst1Ref},
			wantNonBlocking: []nonBlockingError{{Ref: inst1Ref, Err: NonBlockingInstanceError{Code: "STOCKOUT", Message: "Zone stockout", InstanceStatus: inProgressStatus}}},
		},
		{
			name: "no errors reported for an instance that is already done",
			// The instance finished the action but its last attempt still carries an error.
			polls:         [][]*gce_api.ManagedInstance{{managedInstance(inst1Ref, ActionNone, doneStatus, stockout)}},
			instances:     []gce.GceRef{inst1Ref},
			wantPolls:     1,
			wantCompleted: []gce.GceRef{inst1Ref},
		},
		{
			// This row is also what pins the deduplication of non-blocking errors: GCE retries
			// the action indefinitely, so without it every poll would re-report the same error.
			// The four polls span 20 seconds of the bubble's clock, well inside the 5-minute
			// deduplication window, and wantNonBlocking is compared as a multiset, so a repeat
			// that slipped through would fail the case.
			name: "no errors reported for an instance that became done on an earlier poll",
			polls: [][]*gce_api.ManagedInstance{
				// Both instances are still running the action and both hit a stockout.
				{
					managedInstance(inst1Ref, actionToTest, inProgressStatus, stockout),
					managedInstance(inst2Ref, actionToTest, inProgressStatus, stockout),
				},
				// inst1 finished. inst2 repeats the same stockout, which the deduper swallows.
				{
					managedInstance(inst1Ref, ActionNone, doneStatus),
					managedInstance(inst2Ref, actionToTest, inProgressStatus, stockout),
				},
				// A brand new error shows up for both instances. Only inst2's is reported: inst1
				// already made it, even though GCE still lists errors for it.
				{
					managedInstance(inst1Ref, actionToTest, inProgressStatus, quotaExceeded),
					managedInstance(inst2Ref, actionToTest, inProgressStatus, quotaExceeded),
				},
				// inst2 finishes too, ending the poll.
				{
					managedInstance(inst2Ref, ActionNone, doneStatus),
				},
			},
			instances:     []gce.GceRef{inst1Ref, inst2Ref},
			wantPolls:     4,
			wantCompleted: []gce.GceRef{inst1Ref, inst2Ref},
			wantNonBlocking: []nonBlockingError{
				{Ref: inst1Ref, Err: NonBlockingInstanceError{Code: "STOCKOUT", Message: "Zone stockout", InstanceStatus: inProgressStatus}},
				{Ref: inst2Ref, Err: NonBlockingInstanceError{Code: "STOCKOUT", Message: "Zone stockout", InstanceStatus: inProgressStatus}},
				{Ref: inst2Ref, Err: NonBlockingInstanceError{Code: "QUOTA_EXCEEDED", Message: "Quota exceeded", InstanceStatus: inProgressStatus}},
			},
		},
		{
			name: "timeout reports only the instances that never became done",
			// inst1 finishes on the first poll, inst2 stays stuck, so this single response is
			// replayed until the wait runs out.
			polls: [][]*gce_api.ManagedInstance{{
				managedInstance(inst1Ref, ActionNone, ""),
				managedInstance(inst2Ref, actionToTest, ""),
			}},
			instances: []gce.GceRef{inst1Ref, inst2Ref},
			// The loop keeps polling right up to the deadline instead of giving up early.
			wantPolls:     int(actionTimeout / instanceActionPollingFrequency),
			wantCompleted: []gce.GceRef{inst1Ref},
			wantAborted:   []instanceError{{Ref: inst2Ref, Err: context.DeadlineExceeded}},
		},
		{
			name:      "no instances to wait for",
			polls:     neverDone,
			instances: nil,
			wantPolls: 0,
		},
		{
			name:      "waiting disabled by the Giraffe flag",
			boolFlags: map[string]bool{experiments.ColdStandbyNodesWaitForInstanceStatus: false},
			polls:     neverDone,
			instances: []gce.GceRef{inst1Ref, inst2Ref},
			wantPolls: 0,
			// Without a poll the instances are reported in the order they were passed in.
			wantCompleted: []gce.GceRef{inst1Ref, inst2Ref},
		},
		{
			name:                 "no experiments manager",
			noExperimentsManager: true,
			polls:                neverDone,
			instances:            []gce.GceRef{inst1Ref, inst2Ref},
			wantPolls:            0,
			wantCompleted:        []gce.GceRef{inst1Ref, inst2Ref},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				responder := newPollTestResponder(t, migRef, tt.polls...)
				client := newPollTestClient(t, actionToTest, responder, actionTimeout, tt.boolFlags)
				if tt.noExperimentsManager {
					client.experimentsManager = nil
				}

				got := collectPoll(client.PollUntilActionStops(t.Context(), actionToTest, migRef, tt.instances))

				assert.Equal(t, tt.wantCompleted, got.completed)
				// Compared as sets: both are yielded while ranging over the pending instances, so
				// their relative order is not part of the contract.
				assert.ElementsMatch(t, tt.wantNonBlocking, got.nonBlocking)
				if diff := cmp.Diff(tt.wantAborted, got.aborted, cmpopts.EquateErrors(), cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("aborted diff (-want +got):\n%s", diff)
				}
				for _, aborted := range got.aborted {
					assert.ErrorContains(t, aborted.Err, fmt.Sprintf("stopped waiting for instance %v", aborted.Ref))
				}
				assert.Equal(t, tt.wantPolls, responder.polls())
				assert.Empty(t, got.unexpected, "poll reported instances with an unrecognized outcome")
			})
		})
	}
}

// TestPollUntilActionStopsReportsInstancesFinishingTogether is kept out of the table above because
// both instances leave the pending set on the same poll, so their report order is unordered.
func TestPollUntilActionStopsReportsInstancesFinishingTogether(t *testing.T) {
	for _, actionToTest := range pollTestActions {
		t.Run(string(actionToTest), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				migRef := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "mig1"}
				inst1Ref := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "inst1"}
				inst2Ref := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "inst2"}

				// A single poll settles both instances, so the iterator must not poll again.
				responder := newPollTestResponder(t, migRef, []*gce_api.ManagedInstance{
					managedInstance(inst1Ref, ActionNone, ""),
					managedInstance(inst2Ref, ActionNone, ""),
				})
				client := newPollTestClient(t, actionToTest, responder, time.Minute, nil)

				got := collectPoll(client.PollUntilActionStops(t.Context(), actionToTest, migRef, []gce.GceRef{inst1Ref, inst2Ref}))
				assert.ElementsMatch(t, []gce.GceRef{inst1Ref, inst2Ref}, got.completed)
				assert.Empty(t, got.aborted)
				assert.Empty(t, got.nonBlocking)
				assert.Equal(t, 1, responder.polls())
			})
		})
	}
}

func TestPollUntilActionStops_FetchError(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		calls++
		if calls == 1 {
			res.WriteHeader(http.StatusRequestTimeout)
			return
		}
		lmiResponse := &gce_api_beta.InstanceGroupManagersListManagedInstancesResponse{
			ManagedInstances: []*gce_api_beta.ManagedInstance{
				{Name: "inst1", CurrentAction: "NONE"},
			},
		}
		b, err := json.Marshal(lmiResponse)
		assert.NoError(t, err)
		res.Write(b)
	}))
	defer server.Close()

	client := newTestAutoscalingInternalGceClientWithTimeout(t, "project1", server.URL, nil, time.Duration(0), WithInstanceActionPollingFrequency(time.Millisecond))
	migRef := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "mig1"}
	inst1Ref := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "inst1"}

	got := collectPoll(client.PollUntilActionStops(t.Context(), ActionResuming, migRef, []gce.GceRef{inst1Ref}))
	assert.Equal(t, []gce.GceRef{inst1Ref}, got.completed)
	assert.Empty(t, got.aborted)
	assert.Equal(t, 2, calls)
}

// TestPollUntilActionStopsStopsEarly covers the two ways of ending a wait before every instance is
// done. It is kept out of the table above because each case drives the iterator differently.
func TestPollUntilActionStopsStopsEarly(t *testing.T) {
	for _, action := range pollTestActions {
		t.Run(string(action), func(t *testing.T) {
			testPollUntilActionStopsStopsEarly(t, action)
		})
	}
}

// testPollUntilActionStopsStopsEarly runs both early-stop cases for a single action. It is a
// function of its own so that the cases are not indented behind the per-action subtest.
func testPollUntilActionStopsStopsEarly(t *testing.T, action InstanceAction) {
	migRef := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "mig1"}
	inst1Ref := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "inst1"}
	inst2Ref := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "inst2"}

	// startPoll waits on an inst1 that is done right away and an inst2 that never finishes, so
	// without an early stop the poll would run for a whole minute.
	startPoll := func(t *testing.T, ctx context.Context) (ActionPollSeq, *pollTestResponder) {
		t.Helper()
		responder := newPollTestResponder(t, migRef, []*gce_api.ManagedInstance{
			managedInstance(inst1Ref, ActionNone, ""),
			managedInstance(inst2Ref, action, ""),
		})
		client := newPollTestClient(t, action, responder, time.Minute, nil)
		return client.PollUntilActionStops(ctx, action, migRef, []gce.GceRef{inst1Ref, inst2Ref}), responder
	}

	t.Run("consumer breaks", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			poll, responder := startPoll(t, t.Context())

			var done []gce.GceRef
			for ref := range poll {
				done = append(done, ref)
				break
			}

			assert.Equal(t, []gce.GceRef{inst1Ref}, done)
			// The break has to stop the polling too, not just the reporting.
			assert.Equal(t, 1, responder.polls())
		})
	})

	// Cancelling the context covers what a break cannot: between two updates there is no loop body
	// to break out of, so cancelling is the only way for the caller to stop the wait then.
	t.Run("context is cancelled", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			poll, responder := startPoll(t, ctx)

			// Cancelling between the first and the second poll catches the iterator while it is
			// waiting, which is precisely when the consumer has no say in the matter.
			go func() {
				time.Sleep(instanceActionPollingFrequency + time.Second)
				cancel()
			}()

			got := collectPoll(poll)

			assert.Equal(t, []gce.GceRef{inst1Ref}, got.completed)
			if assert.Len(t, got.aborted, 1) {
				assert.Equal(t, inst2Ref, got.aborted[0].Ref)
				assert.ErrorIs(t, got.aborted[0].Err, context.Canceled)
			}
			// The wait ended on the cancellation rather than on the timeout, so the iterator
			// never got to poll a second time.
			assert.Equal(t, 1, responder.polls())
		})
	})
}

// requestLatenciesMetricName is the fully qualified name of the histogram EmitGceLatency feeds.
const requestLatenciesMetricName = "cluster_autoscaler_request_latencies"

// registerGkeMetrics makes the GKE metrics observable: component-base metrics silently drop
// observations until they are registered, and the default registry rejects a second registration.
var registerGkeMetrics = sync.OnceFunc(gke_metrics.RegisterMetrics)

// actionPollingLatencyCounts returns how many observations the polling latency metric of action
// holds so far, keyed by the status label of the outcome: "200" for instances that stopped running
// the action, "timeout" for the ones the poll gave up on.
func actionPollingLatencyCounts(t *testing.T, action InstanceAction) map[string]uint64 {
	t.Helper()
	registerGkeMetrics()

	families, err := legacyregistry.DefaultGatherer.Gather()
	assert.NoError(t, err)

	counts := make(map[string]uint64)
	for _, family := range families {
		if family.GetName() != requestLatenciesMetricName {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := make(map[string]string, len(metric.GetLabel()))
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["service"] != "gce" || labels["resource"] != "instance_group_managers" || labels["verb"] != actionPollingVerb(action) {
				continue
			}
			counts[labels["status"]] = metric.GetHistogram().GetSampleCount()
		}
	}
	return counts
}

func TestPollUntilActionStopsEmitsLatencyPerInstance(t *testing.T) {
	for _, action := range pollTestActions {
		t.Run(string(action), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				migRef := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "mig1"}
				inst1Ref := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "inst1"}
				inst2Ref := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "inst2"}

				// inst1 is done on the first poll, inst2 stays stuck until the poll times out, so
				// the two instances of a single poll end up with opposite outcomes.
				responder := newPollTestResponder(t, migRef, []*gce_api.ManagedInstance{
					managedInstance(inst1Ref, ActionNone, ""),
					managedInstance(inst2Ref, action, ""),
				})
				client := newPollTestClient(t, action, responder, time.Minute, nil)

				// Each action reports its latency on a metric of its own, so the counts are read
				// back for the action under test.
				before := actionPollingLatencyCounts(t, action)
				collectPoll(client.PollUntilActionStops(t.Context(), action, migRef, []gce.GceRef{inst1Ref, inst2Ref}))
				after := actionPollingLatencyCounts(t, action)

				// One observation per instance, each carrying that instance's own outcome, rather
				// than a single one for the whole poll.
				assert.Equal(t, uint64(1), after["200"]-before["200"], "observations for the instance that finished the action")
				assert.Equal(t, uint64(1), after["timeout"]-before["timeout"], "observations for the instance that timed out")
			})
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
			)

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

func TestAutoscalingGceClient_CreateInstancesWithRecommendation(t *testing.T) {
	var capturedPayload []byte
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/projects/project1/zones/zoneA/instanceGroupManagers/mig1/createInstances" {
			capturedPayload, _ = io.ReadAll(req.Body)

			operation := gce_api.Operation{
				Name:   "op123",
				Status: "DONE",
			}
			b, _ := json.Marshal(operation)
			res.WriteHeader(http.StatusOK)
			res.Write(b)
			return
		}
		if req.URL.Path == "/projects/project1/zones/zoneA/operations/op123/wait" {
			operation := gce_api.Operation{
				Name:   "op123",
				Status: "DONE",
			}
			b, _ := json.Marshal(operation)
			res.WriteHeader(http.StatusOK)
			res.Write(b)
			return
		}
		res.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := newTestAutoscalingInternalGceClientWithTimeout(t, "project1", server.URL, nil, time.Duration(0))
	migRef := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "mig1"}

	ids, err := client.CreateInstancesWithRecommendation(migRef, "base-name", 2, []string{"existing1"}, "my-test-recommendation")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(ids))

	var createReqMap map[string]interface{}
	err = json.Unmarshal(capturedPayload, &createReqMap)
	assert.NoError(t, err)

	instances, ok := createReqMap["instances"].([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 2, len(instances))
	if rec, ok := createReqMap["recommendation"]; ok {
		assert.Equal(t, "my-test-recommendation", rec)
	}
}

func TestFetchManagedInstances_Filter(t *testing.T) {
	tests := []struct {
		name   string
		filter string
	}{
		{
			name:   "no filter",
			filter: "",
		},
		{
			name:   "with currentAction filter",
			filter: "currentAction = RESUMING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedFilter string
			var hasFilter bool
			server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
				if req.URL.Path == "/projects/project1/zones/zoneA/instanceGroupManagers/mig1/listManagedInstances" {
					capturedFilter = req.URL.Query().Get("filter")
					hasFilter = req.URL.Query().Has("filter")
					lmiResponse := &gce_api_beta.InstanceGroupManagersListManagedInstancesResponse{
						ManagedInstances: []*gce_api_beta.ManagedInstance{
							{Name: "inst1", CurrentAction: "NONE"},
							{Name: "inst2", CurrentAction: "RESUMING"},
						},
					}
					b, err := json.Marshal(lmiResponse)
					assert.NoError(t, err)
					res.WriteHeader(http.StatusOK)
					res.Write(b)
					return
				}
				res.WriteHeader(http.StatusNotFound)
			}))
			defer server.Close()

			client := newTestAutoscalingInternalGceClientWithTimeout(t, "project1", server.URL, nil, time.Duration(0))
			migRef := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "mig1"}

			instances, err := client.FetchManagedInstances(migRef, tt.filter)
			assert.NoError(t, err)
			assert.Equal(t, tt.filter, capturedFilter)
			assert.Equal(t, tt.filter != "", hasFilter)
			if assert.Len(t, instances, 2) {
				assert.Equal(t, "inst1", instances[0].Name)
				assert.Equal(t, "NONE", instances[0].CurrentAction)
				assert.Equal(t, "inst2", instances[1].Name)
				assert.Equal(t, "RESUMING", instances[1].CurrentAction)
			}
		})
	}
}

func TestAutoscalingGceClient_InstanceActionTimeout(t *testing.T) {
	tests := []struct {
		name               string
		action             InstanceAction
		experimentsManager experiments.Manager
		want               time.Duration
	}{
		{
			name:   "resume default timeout",
			action: ActionResuming,
			want:   DefaultResumeInstanceActionTimeout,
		},
		{
			name:   "suspend default timeout",
			action: ActionSuspending,
			want:   DefaultSuspendInstanceActionTimeout,
		},
		{
			name:   "resume Giraffe flag configured",
			action: ActionResuming,
			experimentsManager: experiments.NewMockManagerWithOptions(
				version.Version{},
				nil,
				map[string]string{
					experiments.ColdStandbyNodesResumeTimeoutSecondsFlag: "180",
				},
			),
			want: 180 * time.Second,
		},
		{
			name:   "suspend Giraffe flag configured",
			action: ActionSuspending,
			experimentsManager: experiments.NewMockManagerWithOptions(
				version.Version{},
				nil,
				map[string]string{
					experiments.ColdStandbyNodesSuspendTimeoutSecondsFlag: "900",
				},
			),
			want: 900 * time.Second,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &autoscalingInternalGceClient{
				experimentsManager: tc.experimentsManager,
			}
			assert.Equal(t, tc.want, client.instanceActionTimeout(tc.action))
		})
	}
}

func TestReapFinishedInstances_Filter(t *testing.T) {
	tests := []struct {
		name           string
		action         InstanceAction
		expectedFilter string
	}{
		{
			name:           "empty action",
			action:         "",
			expectedFilter: "",
		},
		{
			name:           "resuming action",
			action:         ActionResuming,
			expectedFilter: "currentAction = RESUMING",
		},
		{
			name:           "suspending action",
			action:         ActionSuspending,
			expectedFilter: "currentAction = SUSPENDING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedFilter string
			var hasFilter bool
			server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
				if req.URL.Path == "/projects/project1/zones/zoneA/instanceGroupManagers/mig1/listManagedInstances" {
					capturedFilter = req.URL.Query().Get("filter")
					hasFilter = req.URL.Query().Has("filter")
					lmiResponse := &gce_api_beta.InstanceGroupManagersListManagedInstancesResponse{}
					b, err := json.Marshal(lmiResponse)
					assert.NoError(t, err)
					res.WriteHeader(http.StatusOK)
					res.Write(b)
					return
				}
				res.WriteHeader(http.StatusNotFound)
			}))
			defer server.Close()

			client := newTestAutoscalingInternalGceClientWithTimeout(t, "project1", server.URL, nil, time.Duration(0))
			migRef := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "mig1"}
			inst1Ref := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "inst1"}

			neverDedupe := func(gce.GceRef, string, string) bool { return true }
			discard := func(gce.GceRef, PollUpdate) bool { return true }
			_, _ = client.reapFinishedInstances(t.Context(), tt.action, migRef, map[string]gce.GceRef{inst1Ref.Name: inst1Ref}, neverDedupe, time.Now(), discard)
			assert.Equal(t, tt.expectedFilter, capturedFilter)
			assert.Equal(t, tt.expectedFilter != "", hasFilter)
		})
	}
}

func TestReapFinishedInstances_FetchError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(http.StatusRequestTimeout)
	}))
	defer server.Close()

	client := newTestAutoscalingInternalGceClientWithTimeout(t, "project1", server.URL, nil, time.Duration(0))
	migRef := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "mig1"}
	inst1Ref := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "inst1"}

	neverDedupe := func(gce.GceRef, string, string) bool { return true }
	yieldCalled := false
	yield := func(gce.GceRef, PollUpdate) bool {
		yieldCalled = true
		return true
	}
	pending := map[string]gce.GceRef{inst1Ref.Name: inst1Ref}

	stopped, err := client.reapFinishedInstances(t.Context(), ActionResuming, migRef, pending, neverDedupe, time.Now(), yield)
	assert.False(t, stopped)
	assert.ErrorContains(t, err, strconv.Itoa(http.StatusRequestTimeout))
	assert.Len(t, pending, 1)
	assert.False(t, yieldCalled)
}

func TestReapFinishedInstances_ContextCancelled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := newTestAutoscalingInternalGceClientWithTimeout(t, "project1", server.URL, nil, time.Duration(0))
	migRef := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "mig1"}
	inst1Ref := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "inst1"}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	neverDedupe := func(gce.GceRef, string, string) bool { return true }
	discard := func(gce.GceRef, PollUpdate) bool { return true }
	pending := map[string]gce.GceRef{inst1Ref.Name: inst1Ref}

	stopped, err := client.reapFinishedInstances(ctx, ActionResuming, migRef, pending, neverDedupe, time.Now(), discard)
	assert.False(t, stopped)
	assert.ErrorIs(t, err, context.Canceled)
}
