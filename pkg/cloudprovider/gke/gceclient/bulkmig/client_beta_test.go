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

package bulkmig

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	gce_api_beta "google.golang.org/api/compute/v0.beta"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	test_util "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
)

func TestBulkMigStatusBeta(t *testing.T) {
	exampleTime := time.Date(2015, 5, 26, 7, 17, 0, 0, time.UTC)
	migRef := gce.GceRef{Name: "test-mig", Zone: "us-central1-a", Project: "myprojid"}

	tests := []struct {
		name                         string
		operationWaitTimeoutOverride time.Duration
		igmResponse                  gce_api_beta.InstanceGroupManager
		want                         Status
		wantError                    error
	}{
		{
			name: "no_Status_want_error",
			igmResponse: gce_api_beta.InstanceGroupManager{
				Zone:       migRef.Zone,
				Name:       migRef.Name,
				TargetSize: 4,
			},
			wantError: fmt.Errorf("cannot resolve BulkMigStatus for mig %s, got nil BulkInstanceOperation", migRef),
		},
		{
			name: "no_BulkInstanceOperation_want_error",
			igmResponse: gce_api_beta.InstanceGroupManager{
				Zone:       migRef.Zone,
				Name:       migRef.Name,
				TargetSize: 4,
				Status:     &gce_api_beta.InstanceGroupManagerStatus{},
			},
			wantError: fmt.Errorf("cannot resolve BulkMigStatus for mig %s, got nil BulkInstanceOperation", migRef),
		},
		{
			name: "no_LastProgressCheck_ok",
			igmResponse: gce_api_beta.InstanceGroupManager{
				Id:         1234567,
				Zone:       migRef.Zone,
				Name:       migRef.Name,
				TargetSize: 4,
				Status: &gce_api_beta.InstanceGroupManagerStatus{
					BulkInstanceOperation: &gce_api_beta.InstanceGroupManagerStatusBulkInstanceOperation{},
				},
			},
			want: Status{
				ID:                         1234567,
				Ref:                        migRef,
				InProgress:                 false,
				LastProgressCheckErrors:    nil,
				LastProgressCheckTimestamp: time.Time{},
				TargetSize:                 4,
			},
		},
		{
			name: "LastProgressCheck_no_errors_ok",
			igmResponse: gce_api_beta.InstanceGroupManager{
				Id:         1234567,
				Zone:       migRef.Zone,
				Name:       migRef.Name,
				TargetSize: 4,
				Status: &gce_api_beta.InstanceGroupManagerStatus{
					BulkInstanceOperation: &gce_api_beta.InstanceGroupManagerStatusBulkInstanceOperation{
						LastProgressCheck: &gce_api_beta.InstanceGroupManagerStatusBulkInstanceOperationLastProgressCheck{
							Timestamp: exampleTime.Format(time.RFC3339),
						},
					},
				},
			},
			want: Status{
				ID:                         1234567,
				Ref:                        migRef,
				InProgress:                 false,
				LastProgressCheckErrors:    nil,
				LastProgressCheckTimestamp: exampleTime,
				TargetSize:                 4,
			},
		},
		{
			name: "LastProgressCheck_with_errors_ok",
			igmResponse: gce_api_beta.InstanceGroupManager{
				Id:         1234567,
				Zone:       migRef.Zone,
				Name:       migRef.Name,
				TargetSize: 4,
				Status: &gce_api_beta.InstanceGroupManagerStatus{
					BulkInstanceOperation: &gce_api_beta.InstanceGroupManagerStatusBulkInstanceOperation{
						LastProgressCheck: &gce_api_beta.InstanceGroupManagerStatusBulkInstanceOperationLastProgressCheck{
							Timestamp: exampleTime.Format(time.RFC3339),
							Error: &gce_api_beta.InstanceGroupManagerStatusBulkInstanceOperationLastProgressCheckError{
								Errors: []*gce_api_beta.InstanceGroupManagerStatusBulkInstanceOperationLastProgressCheckErrorErrors{
									{
										Code:    "SOME_CODE",
										Message: "Some error message",
									},
									{
										Code:    "RESOURCE_POOL_EXHAUSTED",
										Message: "Resource pool exhausted message",
										ErrorDetails: []*gce_api_beta.InstanceGroupManagerStatusBulkInstanceOperationLastProgressCheckErrorErrorsErrorDetails{
											{
												ErrorInfo: &gce_api_beta.ErrorInfo{
													Reason: "Some stockout reason",
													Metadatas: map[string]string{
														"vmType":     "some-vm-type",
														"attachment": "some-attachment",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			want: Status{
				ID:         1234567,
				Ref:        migRef,
				InProgress: false,
				LastProgressCheckErrors: []resizerequestclient.DwsStatusError{
					{
						Code:    "SOME_CODE",
						Message: "Some error message",
					},
					{
						Code:    "RESOURCE_POOL_EXHAUSTED",
						Message: `Resource pool exhausted message Reason: "Some stockout reason", VMType: "some-vm-type", Attachment: "some-attachment".`,
					},
				},
				LastProgressCheckTimestamp: exampleTime,
				TargetSize:                 4,
			},
		},
		{
			name: "TargetSuspendedSize_non_zero_ok",
			igmResponse: gce_api_beta.InstanceGroupManager{
				Id:                  1234567,
				Zone:                migRef.Zone,
				Name:                migRef.Name,
				TargetSize:          4,
				TargetSuspendedSize: 2,
				Status: &gce_api_beta.InstanceGroupManagerStatus{
					BulkInstanceOperation: &gce_api_beta.InstanceGroupManagerStatusBulkInstanceOperation{
						LastProgressCheck: &gce_api_beta.InstanceGroupManagerStatusBulkInstanceOperationLastProgressCheck{
							Timestamp: exampleTime.Format(time.RFC3339),
						},
					},
				},
			},
			want: Status{
				ID:                         1234567,
				Ref:                        migRef,
				InProgress:                 false,
				LastProgressCheckErrors:    nil,
				LastProgressCheckTimestamp: exampleTime,
				TargetSize:                 6,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			client := &http.Client{}
			mockGceAutoscalingClient := gceclient.BuildAutoscalingInternalGceClientMock()
			bulkMigBetaClient, err := NewBulkMigClientBeta(client, migRef.Project, "", server.URL, mockGceAutoscalingClient, &mockGceMigCache{})
			assert.NoError(t, err, tt)

			b, err := json.Marshal(tt.igmResponse)
			assert.NoError(t, err)
			server.On("handle", fmt.Sprintf("/projects/%s/zones/%s/instanceGroupManagers/%s", migRef.Project, migRef.Zone, migRef.Name)).Return(string(b)).Times(1)

			got, err := bulkMigBetaClient.BulkMigStatus(migRef)
			if tt.wantError != nil {
				assert.ErrorContains(t, err, tt.wantError.Error())
			} else {
				assert.NoError(t, err, tt)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("bulkMigBetaClient.BulkMigStatus() diff (-want +got): %s", diff)
			}
		})
	}
}

func TestBulkMigStatusBetaTimeouts(t *testing.T) {
	migRef := gce.GceRef{Name: "test-mig", Zone: "us-central1-a", Project: "myprojid"}

	// non zero timeout to indicate that timeout should be respected for http client
	zeroDuration := 1 * time.Nanosecond
	tests := []struct {
		name                 string
		httpTimeout          time.Duration
		operationWaitTimeout *time.Duration
	}{
		{
			name:                 "ContextTimeout",
			operationWaitTimeout: &zeroDuration,
		},
		{
			name:        "HttpTimeout",
			httpTimeout: zeroDuration,
		},
	}

	server := test_util.NewHttpServerMock()
	defer server.Close()
	server.On("handle", mock.Anything).Return(`{"status": "unreachable"}`).After(50 * time.Millisecond)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &http.Client{}
			client.Timeout = tt.httpTimeout
			mockGceAutoscalingClient := gceclient.BuildAutoscalingInternalGceClientMock()
			bulkMigBetaClient, err := NewBulkMigClientBeta(client, migRef.Project, "", server.URL, mockGceAutoscalingClient, &mockGceMigCache{})
			assert.NoError(t, err, tt)

			if tt.operationWaitTimeout != nil {
				bulkMigBetaClient.operationWaitTimeout = *tt.operationWaitTimeout
			}
			_, err = bulkMigBetaClient.BulkMigStatus(migRef)
			// NOTE: unable to test with ErrorIs as http errors are not wrapping an err, but overwriting it
			assert.ErrorContains(t, err, context.DeadlineExceeded.Error())
		})
	}
}

func TestBulkMigStatusBetaGceErrorResponses(t *testing.T) {
	migRef := gce.GceRef{Name: "test-mig", Zone: "us-central1-a", Project: "myprojid"}

	tests := []struct {
		name    string
		code    int
		gceErr  string
		wantErr error
	}{
		{
			name:    "mig_not_found",
			code:    404,
			gceErr:  migDoesNotExistsError,
			wantErr: errors.NewAutoscalerError(errors.NodeGroupDoesNotExistError, `googleapi: Error 404: The resource 'projects/myprojid/us-central1-a/instanceGroups/test-mig' was not found`),
		},
		{
			name:    "server_error",
			code:    500,
			gceErr:  migServerError,
			wantErr: fmt.Errorf("Some server error"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock(test_util.MockFieldStatusCode, test_util.MockFieldResponse)
			defer server.Close()
			server.On("handle", mock.Anything).Return(tt.code, tt.gceErr).Times(1)
			client := &http.Client{}
			mockGceAutoscalingClient := gceclient.BuildAutoscalingInternalGceClientMock()
			bulkMigBetaClient, err := NewBulkMigClientBeta(client, migRef.Project, "", server.URL, mockGceAutoscalingClient, &mockGceMigCache{})
			assert.NoError(t, err, tt)

			_, err = bulkMigBetaClient.BulkMigStatus(migRef)
			assert.ErrorContains(t, err, tt.wantErr.Error())
		})
	}
}

const migDoesNotExistsError = `{
  "error": {
    "code": 404,
    "message": "The resource 'projects/myprojid/us-central1-a/instanceGroups/test-mig' was not found",
    "errors": [
      {
        "message": "The resource 'projects/myprojid/zones/us-central1-a/instanceGroups/test-mig' was not found",
        "domain": "global",
        "reason": "notFound"
      }
    ]
  }
}`

const migServerError = `{
  "error": {
    "code": 500,
    "message": "Some server error",
  }
}`

type mockGceMigCache struct {
}

func (g *mockGceMigCache) InvalidateMigTargetSize(ref gce.GceRef) {
}

func (g *mockGceMigCache) SetMigTargetSize(ref gce.GceRef, i int64) {
}
