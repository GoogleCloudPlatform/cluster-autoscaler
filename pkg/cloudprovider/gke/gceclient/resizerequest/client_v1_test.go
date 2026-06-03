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
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	gce_api_v1 "google.golang.org/api/compute/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	test_util "k8s.io/autoscaler/cluster-autoscaler/utils/test"
)

const (
	resizeRequestCreationV1Response = `{
	"kind": "compute#operation",
  "id": "6684860318710205276",
  "name": "operation-1669031976964-5edf9ca1b11e5-b4d28da7-c9b8624c",
  "zone": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a",
  "operationType": "compute.instanceGroupManagerResizeRequests.insert",
  "targetLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345qwert12345qwert12345asdfg09876asdfg09876asdfg09876lkj",
  "targetId": "8075376278805646172",
  "status": "RUNNING",
  "user": "test@google.com",
  "progress": 0,
  "insertTime": "2022-11-23T07:01:07.788-08:00",
  "startTime": "2022-11-23T07:01:07.793-08:00",
  "selfLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/operations/operation-1669031976964-5edf9ca1b11e5-b4d28da7-c9b8624c",
  "selfLinkWithId": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/operations/6684860318710205276"
}`
	operationRunningV1Response = `{
  "name": "operation-1669031976964-5edf9ca1b11e5-b4d28da7-c9b8624c",
  "zone": "us-west4-a",
  "operationType": "compute.instanceGroupManagerResizeRequests.insert",
  "status": "RUNNING",
  "selfLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/operations/operation-1669031976964-5edf9ca1b11e5-b4d28da7-c9b8624c",
  "targetLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q",
  "startTime": "2022-11-23T09:54:26.148507311Z",
  "endTime": "2022-11-23T09:54:35.124878859Z"
}`
	operationDoneV1Response = `{
  "name": "operation-1669031976964-5edf9ca1b11e5-b4d28da7-c9b8624c",
  "zone": "us-west4-a",
  "operationType": "compute.instanceGroupManagerResizeRequests.insert",
  "status": "DONE",
  "selfLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/operations/operation-1669031976964-5edf9ca1b11e5-b4d28da7-c9b8624c",
  "targetLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q",
  "startTime": "2022-11-23T09:54:26.148507311Z",
  "endTime": "2022-11-23T09:54:35.124878859Z"
}`
	deleteResizeRequestV1Response = `{
	"kind": "compute#operation",
	"id": "845011320898608083",
	"name": "operation-1677059387561-5f546d11017de-c1697fc0-cac83e49",
	"zone": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a",
	"operationType": "compute.instanceGroupManagerResizeRequests.delete",
	"targetLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q",
	"targetId": "3078633175129807406",
	"status": "RUNNING",
	"user": "test@google.com",
	"progress": 0,
	"insertTime": "2022-11-23T07:01:07.788-08:00",
	"startTime": "2022-11-23T07:01:07.793-08:00",
	"selfLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/operations/operation-1677059387561-5f546d11017de-c1697fc0-cac83e49",
	"selfLinkWithId": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/operations/845011320898608083"
  }`
	deleteOperationRunningV1Response = `{
	"name": "operation-1677059387561-5f546d11017de-c1697fc0-cac83e49",
	"zone": "us-west4-a",
	"operationType": "compute.instanceGroupManagerResizeRequests.delete",
	"status": "RUNNING",
	"selfLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/operations/operation-1677059387561-5f546d11017de-c1697fc0-cac83e49",
	"targetLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q",
	"startTime": "2022-11-23T09:54:26.148507311Z",
	"endTime": "2022-11-23T09:54:35.124878859Z"
  }`
	deleteOperationDoneV1Response = `{
	"name": "operation-1677059387561-5f546d11017de-c1697fc0-cac83e49",
	"zone": "us-west4-a",
	"operationType": "compute.instanceGroupManagerResizeRequests.delete",
	"status": "DONE",
	"selfLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/operations/operation-1677059387561-5f546d11017de-c1697fc0-cac83e49",
	"targetLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q",
	"startTime": "2022-11-23T09:54:36.148507311Z",
	"endTime": "2022-11-23T09:54:45.124878859Z"
  }`
	deleteOperationDoneWithErrorsV1Response = `{
	"name": "operation-1677059387561-5f546d11017de-c1697fc0-cac83e49",
	"zone": "us-west4-a",
	"operationType": "compute.instanceGroupManagerResizeRequests.delete",
	"status": "DONE",
	"error": {
		"errors": [
		  {
			"code": "ERROR_CODE",
			"message": "Got some error."
		  }
		]
	},
	"selfLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/operations/operation-1677059387561-5f546d11017de-c1697fc0-cac83e49",
	"targetLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q",
	"startTime": "2022-11-23T09:54:36.148507311Z",
	"endTime": "2022-11-23T09:54:45.124878859Z"
  }`
	retryDeleteResizeRequestV1Response = `{
	"kind": "compute#operation",
	"id": "678346782376843267",
	"name": "operation-1697108668456-60782e7541cb2-6686cfad-8f925b38",
	"zone": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a",
	"operationType": "compute.instanceGroupManagerResizeRequests.delete",
	"targetLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q",
	"targetId": "3078633175129807406",
	"status": "RUNNING",
	"user": "test@google.com",
	"progress": 0,
	"insertTime": "2022-11-23T07:01:07.788-08:00",
	"startTime": "2022-11-23T07:01:07.793-08:00",
	"selfLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/operations/operation-1697108668456-60782e7541cb2-6686cfad-8f925b38",
	"selfLinkWithId": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/operations/678346782376843267"
  }`
	retryDeleteOperationDoneV1Response = `{
	"name": "operation-1697108668456-60782e7541cb2-6686cfad-8f925b38",
	"zone": "us-west4-a",
	"operationType": "compute.instanceGroupManagerResizeRequests.delete",
	"status": "DONE",
	"selfLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/operations/operation-1697108668456-60782e7541cb2-6686cfad-8f925b38",
	"targetLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q",
	"startTime": "2022-11-23T09:54:36.148507311Z",
	"endTime": "2022-11-23T09:54:45.124878859Z"
  }`

	cancelResizeRequestV1Response = `{
	"kind": "compute#operation",
	"id": "6167131165371271367",
	"name": "operation-1704965670300-60ea84121ea94-fd8d8370-461c8976",
	"zone": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a",
	"operationType": "compute.instanceGroupManagerResizeRequests.cancel",
	"targetLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q",
	"targetId": "3078633175129807406",
	"status": "RUNNING",
	"user": "test@google.com",
	"progress": 0,
	"insertTime": "2022-11-23T07:00:07.788-08:00",
	"startTime": "2022-11-23T07:00:07.793-08:00",
	"selfLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/operations/operation-1704965670300-60ea84121ea94-fd8d8370-461c8976",
	"selfLinkWithId": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/operations/6167131165371271367"
  }`
	cancelOperationRunningV1Response = `{
	"name": "operation-1704965670300-60ea84121ea94-fd8d8370-461c8976",
	"zone": "us-west4-a",
	"operationType": "compute.instanceGroupManagerResizeRequests.cancel",
	"status": "RUNNING",
	"selfLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/operations/operation-1704965670300-60ea84121ea94-fd8d8370-461c8976",
	"targetLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q",
	"startTime": "2022-11-23T07:00:09.793-08:00",
	"endTime": "2022-11-23T07:00:09.799-08:00"
  }`
	cancelOperationDoneV1Response = `{
	"name": "operation-1704965670300-60ea84121ea94-fd8d8370-461c8976",
	"zone": "us-west4-a",
	"operationType": "compute.instanceGroupManagerResizeRequests.cancel",
	"status": "DONE",
	"selfLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/operations/operation-operation-1704965670300-60ea84121ea94-fd8d8370-461c8976",
	"targetLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q",
	"startTime": "2022-11-23T07:00:11.793-08:00",
	"endTime": "2022-11-23T07:00:11.799-08:00"
  }`
	cancelOperationDoneWithConditionNotMetErrorV1Response = `{
	"name": "operation-1704965670300-60ea84121ea94-fd8d8370-461c8976",
	"zone": "us-west4-a",
	"operationType": "compute.instanceGroupManagerResizeRequests.cancel",
	"status": "DONE",
	"error": {
		"errors": [
		  {
			"code": "CONDITION_NOT_MET",
			"message": "Cancelling resize request that has reached a final state is not possible."
		  }
		]
	},
	"selfLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/operations/operation-operation-1704965670300-60ea84121ea94-fd8d8370-461c8976",
	"targetLink": "https://www.googleapis.com/compute/v1/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q",
	"startTime": "2022-11-23T07:00:11.793-08:00",
	"endTime": "2022-11-23T07:00:11.799-08:00"
  }`
)

func TestCreateAtomicResizeRequestV1(t *testing.T) {
	t.Parallel()

	projectID := "project1"
	server := test_util.NewHttpServerMock()
	defer server.Close()
	client := &http.Client{}
	g, err := NewResizeRequestClientV1(client, projectID, "user agent", server.URL, ResizeRequestModeAtomic)
	if err != nil {
		t.Fatalf("Received error when creating the client: %v", err)
	}

	g.operationPollInterval = 1 * time.Millisecond
	g.operationWaitTimeout = 500 * time.Millisecond

	server.On("handle", "/projects/project1/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests").Return(resizeRequestCreationV1Response).Once()
	server.On("handle", "/projects/project1/zones/us-west4-a/operations/operation-1669031976964-5edf9ca1b11e5-b4d28da7-c9b8624c").Return(operationRunningV1Response).Once()
	server.On("handle", "/projects/project1/zones/us-west4-a/operations/operation-1669031976964-5edf9ca1b11e5-b4d28da7-c9b8624c").Return(operationDoneV1Response).Once()

	err = g.CreateResizeRequest(context.Background(), gce.GceRef{Project: projectID, Zone: "us-west4-a", Name: "mig-name"}, ResizeRequestCreateRequest{})
	assert.NoError(t, err)
	mock.AssertExpectationsForObjects(t, server)
}

func TestAdvanceResizeRequestCleanUpV1(t *testing.T) {
	t.Parallel()

	projectID := "test-gke-dev"
	server := test_util.NewHttpServerMock()
	defer server.Close()
	client := &http.Client{}
	g, err := NewResizeRequestClientV1(client, "test-gke-dev", "user agent", server.URL, ResizeRequestModeAtomic)
	if err != nil {
		t.Fatalf("Received error when creating the client: %v", err)
	}

	g.operationPollInterval = 1 * time.Millisecond
	g.operationWaitTimeout = 500 * time.Millisecond

	server.On("handle", "/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q").Return(deleteResizeRequestV1Response).Once()

	err = g.AdvanceResizeRequestCleanUp(context.Background(), ResizeRequestStatus{ProjectID: projectID, MigName: "mig-name", Zone: "us-west4-a", Name: "qwert12345q", State: ResizeRequestStateSucceeded, ID: 123456789})
	assert.NoError(t, err)
	mock.AssertExpectationsForObjects(t, server)
}

func TestDeleteTerminalResizeRequestV1LongRunningOperation(t *testing.T) {
	t.Parallel()

	projectID := "test-gke-dev"
	server := test_util.NewHttpServerMock()
	defer server.Close()
	client := &http.Client{}
	g, err := NewResizeRequestClientV1(client, projectID, "user agent", server.URL, ResizeRequestModeQueued)
	if err != nil {
		t.Fatalf("Received error when creating the client: %v", err)
	}

	g.operationPollInterval = 1 * time.Millisecond
	g.operationWaitTimeout = 500 * time.Millisecond

	resizeRequestTerminal := ResizeRequestStatus{ProjectID: projectID, MigName: "mig-name", Zone: "us-west4-a", Name: "qwert12345q", State: ResizeRequestStateSucceeded, ID: 123456789}

	// Trigger delete
	server.On("handle", "/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q").Return(deleteResizeRequestV1Response).Once()
	err = g.AdvanceResizeRequestCleanUp(context.Background(), resizeRequestTerminal)
	assert.NoError(t, err)

	// The Resize Request is still present, check the operation status
	server.On("handle", "/projects/test-gke-dev/zones/us-west4-a/operations/operation-1677059387561-5f546d11017de-c1697fc0-cac83e49").Return(deleteOperationRunningV1Response).Once()
	err = g.AdvanceResizeRequestCleanUp(context.Background(), resizeRequestTerminal)
	assert.NoError(t, err)

	// Delete operation finished with errors, we retry with new delete operation
	server.On("handle", "/projects/test-gke-dev/zones/us-west4-a/operations/operation-1677059387561-5f546d11017de-c1697fc0-cac83e49").Return(deleteOperationDoneWithErrorsV1Response).Once()
	server.On("handle", "/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q").Return(retryDeleteResizeRequestV1Response).Once()
	err = g.AdvanceResizeRequestCleanUp(context.Background(), resizeRequestTerminal)
	assert.NoError(t, err)

	// The Resize Request is still present, check the new operation status
	server.On("handle", "/projects/test-gke-dev/zones/us-west4-a/operations/operation-1697108668456-60782e7541cb2-6686cfad-8f925b38").Return(retryDeleteOperationDoneV1Response).Once()
	err = g.AdvanceResizeRequestCleanUp(context.Background(), resizeRequestTerminal)
	assert.NoError(t, err)

	mock.AssertExpectationsForObjects(t, server)
}

func TestDeleteWithCancelActiveResizeRequestV1(t *testing.T) {
	t.Parallel()

	projectID := "test-gke-dev"
	server := test_util.NewHttpServerMock()
	defer server.Close()
	client := &http.Client{}
	g, err := NewResizeRequestClientV1(client, projectID, "user agent", server.URL, ResizeRequestModeQueued)
	if err != nil {
		t.Fatalf("Received error when creating the client: %v", err)
	}

	g.operationPollInterval = 1 * time.Millisecond
	g.operationWaitTimeout = 500 * time.Millisecond

	// Resize Request is Accepted, but e.g. user deleted the corresponding Provisioning Request, so we want to clean up the Resize Request
	resizeRequest := ResizeRequestStatus{ProjectID: projectID, MigName: "mig-name", Zone: "us-west4-a", Name: "qwert12345q", State: ResizeRequestStateAccepted, ID: 123456789}

	// Delete triggers cancel since Resize Request is active
	server.On("handle", "/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q/cancel").Return(cancelResizeRequestV1Response).Once()
	err = g.AdvanceResizeRequestCleanUp(context.Background(), resizeRequest)
	assert.NoError(t, err)

	// The Resize Request is still present (it was just cancelled), check the cancel operation status - it's finished, so we make a Delete call no matter the current Resize Request status
	server.On("handle", "/projects/test-gke-dev/zones/us-west4-a/operations/operation-1704965670300-60ea84121ea94-fd8d8370-461c8976").Return(cancelOperationDoneV1Response).Once()
	server.On("handle", "/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q").Return(deleteResizeRequestV1Response).Once()
	err = g.AdvanceResizeRequestCleanUp(context.Background(), resizeRequest)
	assert.NoError(t, err)

	// In the next loop the Resize Request will have status Cancelled, since Cancel operation has finished successfully
	resizeRequest.State = ResizeRequestStateCancelled

	// The Resize Request is still present, check the new operation status - the operation is done, we successfully deleted the Resize Request
	server.On("handle", "/projects/test-gke-dev/zones/us-west4-a/operations/operation-1677059387561-5f546d11017de-c1697fc0-cac83e49").Return(deleteOperationDoneV1Response).Once()
	err = g.AdvanceResizeRequestCleanUp(context.Background(), resizeRequest)
	assert.NoError(t, err)

	mock.AssertExpectationsForObjects(t, server)
}

func TestAdvanceResizeRequestCleanUpV1_WithCancelTriggerFail(t *testing.T) {
	t.Parallel()

	projectID := "test-gke-dev"
	server := test_util.NewHttpServerMock(test_util.MockFieldStatusCode, test_util.MockFieldResponse)

	defer server.Close()
	client := &http.Client{}
	g, err := NewResizeRequestClientV1(client, projectID, "user agent", server.URL, ResizeRequestModeQueued)
	if err != nil {
		t.Fatalf("Received error when creating the client: %v", err)
	}

	g.operationPollInterval = 1 * time.Millisecond
	g.operationWaitTimeout = 500 * time.Millisecond

	// We want to delete a Resize Request that we fetched in Accepted state, so we'll attempt to Cancel it first. Since there's a race condition,
	// in the meantime the request changed state to a terminal one, so we'll attempt a Cancel call on a terminal Resize Request, which is forbidden.
	// We'll trigger the Cancel operation, but it'll be rejected at creation with `conditionNotMet` error
	resizeRequest := ResizeRequestStatus{ProjectID: projectID, MigName: "mig-name", Zone: "us-west4-a", Name: "qwert12345q", State: ResizeRequestStateAccepted, ID: 123456789}
	server.On("handle", "/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q/cancel").Return(412, cancelTerminalResizeRequestErrorResponse).Once()
	err = g.AdvanceResizeRequestCleanUp(context.Background(), resizeRequest)
	assert.NoError(t, err)

	// In the next loop the Resize Request will have the terminal status, so we'll just trigger deletion
	resizeRequest.State = ResizeRequestStateFailed
	server.On("handle", "/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q").Return(200, deleteResizeRequestV1Response).Once()
	err = g.AdvanceResizeRequestCleanUp(context.Background(), resizeRequest)
	assert.NoError(t, err)

	// The Resize Request is still present, check the new operation status - the operation is done, we successfully deleted the Resize Request
	server.On("handle", "/projects/test-gke-dev/zones/us-west4-a/operations/operation-1677059387561-5f546d11017de-c1697fc0-cac83e49").Return(200, deleteOperationDoneV1Response).Once()
	err = g.AdvanceResizeRequestCleanUp(context.Background(), resizeRequest)
	assert.NoError(t, err)

	mock.AssertExpectationsForObjects(t, server)
}

func TestAdvanceResizeRequestCleanUpV1_WithCancelFinishFail(t *testing.T) {
	t.Parallel()

	projectID := "test-gke-dev"
	server := test_util.NewHttpServerMock()
	defer server.Close()
	client := &http.Client{}
	g, err := NewResizeRequestClientV1(client, projectID, "user agent", server.URL, ResizeRequestModeQueued)
	if err != nil {
		t.Fatalf("Received error when creating the client: %v", err)
	}

	g.operationPollInterval = 1 * time.Millisecond
	g.operationWaitTimeout = 500 * time.Millisecond

	// Resize Request is Accepted, but e.g. user deleted the corresponding Provisioning Request or we're cancelling a FlexStart scale up, so we want to clean up the Resize Request
	resizeRequest := ResizeRequestStatus{ProjectID: projectID, MigName: "mig-name", Zone: "us-west4-a", Name: "qwert12345q", State: ResizeRequestStateAccepted, ID: 123456789}

	// Delete triggers Cancel since Resize Request is active
	server.On("handle", "/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q/cancel").Return(cancelResizeRequestV1Response).Once()
	err = g.AdvanceResizeRequestCleanUp(context.Background(), resizeRequest)
	assert.NoError(t, err)

	// The Resize Request is still present (it was just cancelled), check the Cancel operation status - it failed with ConditionNotMet error, so we return and don't make new calls
	server.On("handle", "/projects/test-gke-dev/zones/us-west4-a/operations/operation-1704965670300-60ea84121ea94-fd8d8370-461c8976").Return(cancelOperationDoneWithConditionNotMetErrorV1Response).Once()
	err = g.AdvanceResizeRequestCleanUp(context.Background(), resizeRequest)
	assert.NoError(t, err)

	// Since Cancel failed with ConditionNotMet error, in the next loop the Resize Request will have a new terminal state, e.g. Succeeded (could also be Cancelled/Failed, doesn't matter)
	resizeRequest.State = ResizeRequestStateSucceeded

	// The Resize Request has a terminal state, so we just trigger deletion
	server.On("handle", "/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q").Return(deleteResizeRequestV1Response).Once()
	err = g.AdvanceResizeRequestCleanUp(context.Background(), resizeRequest)
	assert.NoError(t, err)

	// The Resize Request is still present, check the new operation status - the operation is done, we successfully deleted the Resize Request
	server.On("handle", "/projects/test-gke-dev/zones/us-west4-a/operations/operation-1677059387561-5f546d11017de-c1697fc0-cac83e49").Return(deleteOperationDoneV1Response).Once()
	err = g.AdvanceResizeRequestCleanUp(context.Background(), resizeRequest)
	assert.NoError(t, err)

	mock.AssertExpectationsForObjects(t, server)
}

func TestFailedCancelAndRetryV1(t *testing.T) {
	t.Parallel()

	projectID := "test-gke-dev"
	server := test_util.NewHttpServerMock()
	defer server.Close()
	client := &http.Client{}
	g, err := NewResizeRequestClientV1(client, "test-gke-dev", "user agent", server.URL, ResizeRequestModeQueued)
	if err != nil {
		t.Fatalf("Received error when creating the client: %v", err)
	}

	g.operationPollInterval = 1 * time.Millisecond
	g.operationWaitTimeout = 500 * time.Millisecond

	// Resize Request is Accepted, but e.g. user deleted the corresponding Provisioning Request, so we want to clean up the Resize Request
	resizeRequest := ResizeRequestStatus{ProjectID: projectID, MigName: "mig-name", Zone: "us-west4-a", Name: "qwert12345q", State: ResizeRequestStateAccepted, ID: 123456789}

	// Cancel has errors
	server.On("handle", "/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q/cancel").Return(400, "Failed").Once()
	err = g.AdvanceResizeRequestCleanUp(context.Background(), resizeRequest)
	assert.Error(t, err)

	assert.Equal(t, UnspecifiedReportState, g.ReportState(resizeRequest))

	// Cancle retried and succeeds
	server.On("handle", "/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q/cancel").Return(cancelResizeRequestV1Response).Once()
	err = g.AdvanceResizeRequestCleanUp(context.Background(), resizeRequest)
	assert.NoError(t, err)
	assert.Equal(t, UnspecifiedReportState, g.ReportState(resizeRequest))

	// In the next loop the Resize Request might have status Cancelled, since Cancel operation has finished successfully
	resizeRequest.State = ResizeRequestStateCancelled

	// The Resize Request is still present, check the new operation status - the operation is done, we successfully cancelled the Resize Request
	// Next comes the delete operation
	server.On("handle", "/projects/test-gke-dev/zones/us-west4-a/operations/operation-1704965670300-60ea84121ea94-fd8d8370-461c8976").Return(cancelOperationDoneV1Response).Once()
	server.On("handle", "/projects/test-gke-dev/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/qwert12345q").Return(deleteResizeRequestV1Response).Once()
	err = g.AdvanceResizeRequestCleanUp(context.Background(), resizeRequest)
	assert.NoError(t, err)

	// Report state will be set to ToBeReportedState on successful Cancel operation
	assert.Equal(t, ToBeReportedState, g.ReportState(resizeRequest))

	// The Resize Request is still present, check the new operation status - the operation is done, we successfully deleted the Resize Request
	server.On("handle", "/projects/test-gke-dev/zones/us-west4-a/operations/operation-1677059387561-5f546d11017de-c1697fc0-cac83e49").Return(deleteOperationDoneV1Response).Once()
	err = g.AdvanceResizeRequestCleanUp(context.Background(), resizeRequest)
	assert.NoError(t, err)

	mock.AssertExpectationsForObjects(t, server)
}

func TestResizeRequestNameV1(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc        string
		namespace   string
		provReqName string
		want        string
	}{
		{
			"simple",
			"short-namespace",
			"short-name",
			"gke-short-namespace-short-name-3d1e61d88309874b",
		},
		{
			"namespace too long",
			"long-namespace-that-will-not-fit-in-the-cap",
			"short-name",
			"gke-long-namespace-that--short-name-85245499fd480865",
		},
		{
			"name too long",
			"short-namespace",
			"long-name-that-will-not-fit-in-the-cap",
			"gke-short-namespace-long-name-that-will--f494c6e6242f10af",
		},
		{
			"namespace and name too long",
			"long-namespace-that-will-not-fit-in-the-cap",
			"long-name-that-will-not-fit-in-the-cap",
			"gke-long-namespace-that--long-name-that-will--35167d31b679a0d7",
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			t.Parallel()

			got := ResizeRequestName(tt.namespace, tt.provReqName)
			if got != tt.want {
				t.Errorf("ResizeRequestName(%s, %s) = %v, want %v", tt.namespace, tt.provReqName, got, tt.want)
			}
			if len(got) > 63 {
				t.Errorf("Got name: %q that does not fulfill the RFC1035, len(ResizeRequestName(%v, %v)) = %d, want <= 63", got, tt.namespace, tt.provReqName, len(got))
			}
		})
	}
}

func TestResizeRequestsV1(t *testing.T) {
	t.Parallel()

	migName := "test-mig-name"
	projectID := "project1"

	tests := []struct {
		name           string
		zone           string
		resizeRequests [][]resizeRequestResponseV1Mock
		want           []ResizeRequestStatus
		wantErr        bool
	}{
		{
			name: "simple test",
			zone: "us-central1-a",
			resizeRequests: [][]resizeRequestResponseV1Mock{
				{
					{
						id:                12312412,
						name:              "test-name",
						resizeBy:          42,
						creationTimestamp: "2022-11-12T13:14:15Z",
						state:             "ACCEPTED",
						requestedRunDuration: &gce_api_v1.Duration{
							Nanos:   1232,
							Seconds: 36000,
						},
					},
				},
			},
			want: []ResizeRequestStatus{
				{
					ProjectID:            projectID,
					ID:                   uint64(12312412),
					Name:                 "test-name",
					CreationTime:         time.Date(2022, 11, 12, 13, 14, 15, 0, time.UTC),
					ResizeBy:             42,
					State:                ResizeRequestStateAccepted,
					MigName:              migName,
					Zone:                 "us-central1-a",
					RequestedRunDuration: protoDuration(1232*time.Nanosecond + 36000*time.Second),
				},
			},
		},
		{
			name: "simple paginated test",
			zone: "us-central1-a",
			resizeRequests: [][]resizeRequestResponseV1Mock{
				{
					{
						id:                12312412,
						name:              "test-name",
						resizeBy:          42,
						creationTimestamp: "2022-11-12T13:14:15Z",
						state:             "ACCEPTED",
					},
				},
				{
					{
						id:                2345,
						name:              "test-name1",
						resizeBy:          13,
						creationTimestamp: "2022-11-12T13:14:15Z",
						state:             "ACCEPTED",
					},
				},
			},
			want: []ResizeRequestStatus{
				{
					ProjectID:    projectID,
					ID:           uint64(12312412),
					Name:         "test-name",
					CreationTime: time.Date(2022, 11, 12, 13, 14, 15, 0, time.UTC),
					ResizeBy:     42,
					State:        ResizeRequestStateAccepted,
					MigName:      migName,
					Zone:         "us-central1-a",
				},
				{
					ProjectID:    projectID,
					ID:           uint64(2345),
					Name:         "test-name1",
					CreationTime: time.Date(2022, 11, 12, 13, 14, 15, 0, time.UTC),
					ResizeBy:     13,
					State:        ResizeRequestStateAccepted,
					MigName:      migName,
					Zone:         "us-central1-a",
				},
			},
		},
		{
			name: "resize request with error",
			zone: "us-central1-h",
			resizeRequests: [][]resizeRequestResponseV1Mock{
				{
					{
						id:                12312412,
						name:              "test-name",
						resizeBy:          42,
						creationTimestamp: "2022-11-12T13:14:15Z",
						state:             "FAILED",
						errors: []errorMock{
							{
								code:     "403",
								location: "us-central1-h",
								message:  "us-central1-h does not exist",
							},
						},
					},
				},
			},
			want: []ResizeRequestStatus{
				{
					ProjectID:    projectID,
					ID:           uint64(12312412),
					Name:         "test-name",
					CreationTime: time.Date(2022, 11, 12, 13, 14, 15, 0, time.UTC),
					ResizeBy:     42,
					State:        ResizeRequestStateFailed,
					MigName:      migName,
					Zone:         "us-central1-h",
					Errors: []DwsStatusError{
						{
							Code:     "403",
							Location: "us-central1-h",
							Message:  "us-central1-h does not exist",
						},
					},
				},
			},
		},
		{
			name: "multiple resize requests ",
			zone: "us-central1-c",
			resizeRequests: [][]resizeRequestResponseV1Mock{
				{
					{
						id:                12312412,
						name:              "test-name",
						resizeBy:          42,
						creationTimestamp: "2022-11-12T13:14:15Z",
						state:             "FAILED",
						errors: []errorMock{
							{
								code:     "500",
								location: "us-central1-c",
								message:  "QRM internal error",
							},
							{
								code:     "503",
								location: "us-central1-c",
								message:  "another QRM internal error",
							},
						},
					},
					{
						id:                35242354,
						name:              "test-name-4",
						resizeBy:          13,
						creationTimestamp: "2022-11-12T14:13:12Z",
						state:             "CREATING",
					},
					{
						id:                2355345,
						name:              "test-name-11",
						resizeBy:          5,
						creationTimestamp: "2022-11-15T14:13:12Z",
						state:             "PROVISIONING",
					},
					{
						id:                9876533,
						name:              "test-name-12",
						resizeBy:          78,
						creationTimestamp: "2022-11-15T14:13:12Z",
						state:             "DELETING",
					},
				},
			},
			want: []ResizeRequestStatus{
				{
					ProjectID:    projectID,
					ID:           uint64(12312412),
					Name:         "test-name",
					CreationTime: time.Date(2022, 11, 12, 13, 14, 15, 0, time.UTC),
					ResizeBy:     42,
					State:        ResizeRequestStateFailed,
					MigName:      migName,
					Zone:         "us-central1-c",
					Errors: []DwsStatusError{
						{
							Code:     "500",
							Location: "us-central1-c",
							Message:  "QRM internal error",
						},
						{
							Code:     "503",
							Location: "us-central1-c",
							Message:  "another QRM internal error",
						},
					},
				},
				{
					ProjectID:    projectID,
					ID:           uint64(35242354),
					Name:         "test-name-4",
					CreationTime: time.Date(2022, 11, 12, 14, 13, 12, 0, time.UTC),
					ResizeBy:     13,
					State:        ResizeRequestStateCreating,
					MigName:      migName,
					Zone:         "us-central1-c",
				},
				{
					ProjectID:    projectID,
					ID:           uint64(2355345),
					Name:         "test-name-11",
					CreationTime: time.Date(2022, 11, 15, 14, 13, 12, 0, time.UTC),
					ResizeBy:     5,
					State:        ResizeRequestStateProvisioning,
					MigName:      migName,
					Zone:         "us-central1-c",
				},
				{
					ProjectID:    projectID,
					ID:           uint64(9876533),
					Name:         "test-name-12",
					CreationTime: time.Date(2022, 11, 15, 14, 13, 12, 0, time.UTC),
					ResizeBy:     78,
					State:        ResizeRequestStateDeleting,
					MigName:      migName,
					Zone:         "us-central1-c",
				},
			},
		},
		{
			name: "resize request with QUOTA_EXCEEDED last attempt error",
			zone: "us-central1-h",
			resizeRequests: [][]resizeRequestResponseV1Mock{
				{
					{
						id:                12312412,
						name:              "test-name",
						resizeBy:          42,
						creationTimestamp: "2022-11-12T13:14:15Z",
						state:             "ACCEPTED",
						lastAttemptErrors: []lastAttemptErrorMock{
							{
								code:    "QUOTA_EXCEEDED",
								message: "Quota 'SSD_TOTAL_GB' exceeded.  Limit: 125.0 in region us-central1.",
							},
						},
					},
				},
			},
			want: []ResizeRequestStatus{
				{
					ProjectID:    projectID,
					ID:           uint64(12312412),
					Name:         "test-name",
					CreationTime: time.Date(2022, 11, 12, 13, 14, 15, 0, time.UTC),
					ResizeBy:     42,
					State:        ResizeRequestStateAccepted,
					MigName:      migName,
					Zone:         "us-central1-h",
					LastAttemptErrors: []DwsStatusError{
						{
							Code:    "QUOTA_EXCEEDED",
							Message: "Quota 'SSD_TOTAL_GB' exceeded.  Limit: 125.0 in region us-central1.",
						},
					},
				},
			},
		},
		{
			name: "resize request with ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS last attempt error",
			zone: "us-central1-h",
			resizeRequests: [][]resizeRequestResponseV1Mock{
				{
					{
						id:                12312412,
						name:              "test-name",
						resizeBy:          42,
						creationTimestamp: "2022-11-12T13:14:15Z",
						state:             "ACCEPTED",
						lastAttemptErrors: []lastAttemptErrorMock{
							{
								code:    "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS",
								message: "There are currently not enough resources available to fulfill the request.",
								errorDetails: &errorDetailsMock{
									reason:    "resource_availability",
									metadatas: map[string]string{"vmType": "a2-ultragpu-1g", "attachment": "local-ssd=1,nvidia-a100-80gb=1"},
								},
							},
						},
					},
				},
			},
			want: []ResizeRequestStatus{
				{
					ProjectID:    projectID,
					ID:           uint64(12312412),
					Name:         "test-name",
					CreationTime: time.Date(2022, 11, 12, 13, 14, 15, 0, time.UTC),
					ResizeBy:     42,
					State:        ResizeRequestStateAccepted,
					MigName:      migName,
					Zone:         "us-central1-h",
					LastAttemptErrors: []DwsStatusError{
						{
							Code:    "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS",
							Message: "There are currently not enough resources available to fulfill the request. Reason: \"resource_availability\", VMType: \"a2-ultragpu-1g\", Attachment: \"local-ssd=1,nvidia-a100-80gb=1\".",
						},
					},
				},
			},
		},
		{
			name: "resize request with RESOURCE_POOL_EXHAUSTED_WITH_DETAILS last attempt error",
			zone: "us-central1-h",
			resizeRequests: [][]resizeRequestResponseV1Mock{
				{
					{
						id:                12312412,
						name:              "test-name",
						resizeBy:          42,
						creationTimestamp: "2022-11-12T13:14:15Z",
						state:             "ACCEPTED",
						lastAttemptErrors: []lastAttemptErrorMock{
							{
								code:    "RESOURCE_POOL_EXHAUSTED_WITH_DETAILS",
								message: "There are currently not enough resources available to fulfill the request.",
								errorDetails: &errorDetailsMock{
									reason:    "resource_availability",
									metadatas: map[string]string{"vmType": "a2-ultragpu-1g", "attachment": "local-ssd=1,nvidia-a100-80gb=1"},
								},
							},
						},
					},
				},
			},
			want: []ResizeRequestStatus{
				{
					ProjectID:    projectID,
					ID:           uint64(12312412),
					Name:         "test-name",
					CreationTime: time.Date(2022, 11, 12, 13, 14, 15, 0, time.UTC),
					ResizeBy:     42,
					State:        ResizeRequestStateAccepted,
					MigName:      migName,
					Zone:         "us-central1-h",
					LastAttemptErrors: []DwsStatusError{
						{
							Code:    "RESOURCE_POOL_EXHAUSTED_WITH_DETAILS",
							Message: "There are currently not enough resources available to fulfill the request. Reason: \"resource_availability\", VMType: \"a2-ultragpu-1g\", Attachment: \"local-ssd=1,nvidia-a100-80gb=1\".",
						},
					},
				},
			},
		},
		{
			name: "resize request with ZONE_RESOURCE_POOL_EXHAUSTED last attempt error",
			zone: "us-central1-h",
			resizeRequests: [][]resizeRequestResponseV1Mock{
				{
					{
						id:                12312412,
						name:              "test-name",
						resizeBy:          42,
						creationTimestamp: "2022-11-12T13:14:15Z",
						state:             "ACCEPTED",
						lastAttemptErrors: []lastAttemptErrorMock{
							{
								code:    "ZONE_RESOURCE_POOL_EXHAUSTED",
								message: "There are currently not enough resources available to fulfill the request.",
								errorDetails: &errorDetailsMock{
									reason:    "resource_availability",
									metadatas: map[string]string{"vmType": "a2-ultragpu-1g", "attachment": "local-ssd=1,nvidia-a100-80gb=1"},
								},
							},
						},
					},
				},
			},
			want: []ResizeRequestStatus{
				{
					ProjectID:    projectID,
					ID:           uint64(12312412),
					Name:         "test-name",
					CreationTime: time.Date(2022, 11, 12, 13, 14, 15, 0, time.UTC),
					ResizeBy:     42,
					State:        ResizeRequestStateAccepted,
					MigName:      migName,
					Zone:         "us-central1-h",
					LastAttemptErrors: []DwsStatusError{
						{
							Code:    "ZONE_RESOURCE_POOL_EXHAUSTED",
							Message: "There are currently not enough resources available to fulfill the request. Reason: \"resource_availability\", VMType: \"a2-ultragpu-1g\", Attachment: \"local-ssd=1,nvidia-a100-80gb=1\".",
						},
					},
				},
			},
		},
		{
			name: "resize request with ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS last attempt error without error details",
			zone: "us-central1-h",
			resizeRequests: [][]resizeRequestResponseV1Mock{
				{
					{
						id:                12312412,
						name:              "test-name",
						resizeBy:          42,
						creationTimestamp: "2022-11-12T13:14:15Z",
						state:             "ACCEPTED",
						lastAttemptErrors: []lastAttemptErrorMock{
							{
								code:    "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS",
								message: "There are currently not enough resources available to fulfill the request.",
							},
						},
					},
				},
			},
			want: []ResizeRequestStatus{
				{
					ProjectID:    projectID,
					ID:           uint64(12312412),
					Name:         "test-name",
					CreationTime: time.Date(2022, 11, 12, 13, 14, 15, 0, time.UTC),
					ResizeBy:     42,
					State:        ResizeRequestStateAccepted,
					MigName:      migName,
					Zone:         "us-central1-h",
					LastAttemptErrors: []DwsStatusError{
						{
							Code:    "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS",
							Message: "There are currently not enough resources available to fulfill the request.",
						},
					},
				},
			},
		},
		{
			name: "resize request with ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS last attempt error with ETA",
			zone: "us-central1-h",
			resizeRequests: [][]resizeRequestResponseV1Mock{
				{
					{
						id:                12312412,
						name:              "test-name",
						resizeBy:          42,
						creationTimestamp: "2022-11-12T13:14:15Z",
						state:             "ACCEPTED",
						lastAttemptErrors: []lastAttemptErrorMock{
							{
								code:    "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS",
								message: "Waiting for resources. Currently there are not enough resources available to fulfill the request. Expected time is indefinite.",
								errorDetails: &errorDetailsMock{
									metadatas: map[string]string{"estimatedAvailabilityTime": "9999-12-31T23:59:59.997Z"},
								},
							},
						},
					},
				},
			},
			want: []ResizeRequestStatus{
				{
					ProjectID:    projectID,
					ID:           uint64(12312412),
					Name:         "test-name",
					CreationTime: time.Date(2022, 11, 12, 13, 14, 15, 0, time.UTC),
					ResizeBy:     42,
					State:        ResizeRequestStateAccepted,
					MigName:      migName,
					Zone:         "us-central1-h",
					LastAttemptErrors: []DwsStatusError{
						{
							Code:    "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS",
							Message: "Waiting for resources. Currently there are not enough resources available to fulfill the request. Expected time is indefinite.",
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := test_util.NewHttpServerMock()
			defer server.Close()
			client := newTestResizeRequestClientV1(t, projectID, server.URL)

			url := fmt.Sprintf("/projects/%s/zones/%s/instanceGroupManagers/%s/resizeRequests", projectID, tt.zone, migName)
			resizeRequestListResponses := fillAtomicResizeRequestListResponseV1Template(tt.resizeRequests)
			for i, response := range resizeRequestListResponses {
				if i > 0 {
					server.On("handle", url, fmt.Sprintf("%d", i-1)).Return(response).Once()
				} else {
					server.On("handle", url).Return(response).Once()
				}
			}

			got, err := client.ResizeRequests(context.Background(), gce.GceRef{Project: projectID, Zone: tt.zone, Name: migName})
			if (err != nil) != tt.wantErr {
				t.Errorf("resizeRequestClientV1.ResizeRequests() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("resizeRequestClientV1.ResizeRequests() = \n%+v\nwant: \n%+v", got, tt.want)
			}
			mock.AssertExpectationsForObjects(t, server)
		})
	}
}

func TestRegisterFailedResizeRequestsV1Creation(t *testing.T) {
	t.Parallel()

	server := test_util.NewHttpServerMock()
	defer server.Close()
	client := newTestResizeRequestClientV1(t, projectID, server.URL)

	wantFailures := map[gce.GceRef]map[error]int{
		migRef1: {
			err1: 17 + 110,
			err2: 3,
		},
		migRef2: {
			err2: 8,
		},
	}

	client.RegisterFailedResizeRequestsCreation(migRef1, err1, 17)
	client.RegisterFailedResizeRequestsCreation(migRef1, err1, 110)
	client.RegisterFailedResizeRequestsCreation(migRef1, err2, 3)
	client.RegisterFailedResizeRequestsCreation(migRef2, err2, 8)

	assertMapsEqual(t, wantFailures, client.failedCreationRequestsPerMIG.errorsPerMIG)
}

func TestResetFailedResizeRequestsV1Creation(t *testing.T) {
	t.Parallel()

	projectID := "project1"
	server := test_util.NewHttpServerMock()
	defer server.Close()
	client := newTestResizeRequestClientV1(t, projectID, server.URL)

	currentFailures := map[gce.GceRef]map[error]int{
		migRef1: {
			err1: 127,
			err2: 13,
		},
		migRef2: {
			err2: 4,
		},
	}
	client.failedCreationRequestsPerMIG.errorsPerMIG = currentFailures

	mig1Failures := client.ResetFailedResizeRequestsCreation(migRef1)
	assertMapsEqual(t,
		map[error]int{
			err1: 127,
			err2: 13,
		},
		mig1Failures)
	assert.Equal(t, map[error]int{}, client.failedCreationRequestsPerMIG.errorsPerMIG[migRef1])

	mig2Failures := client.ResetFailedResizeRequestsCreation(migRef2)
	assertMapsEqual(t,
		map[error]int{
			err2: 4,
		},
		mig2Failures)
	assert.Equal(t, map[error]int{}, client.failedCreationRequestsPerMIG.errorsPerMIG[migRef2])
}

type resizeRequestResponseV1Mock struct {
	id                   int
	name                 string
	resizeBy             int
	creationTimestamp    string
	state                string
	requestedRunDuration *gce_api_v1.Duration
	errors               []errorMock
	lastAttemptErrors    []lastAttemptErrorMock
}

func fillAtomicResizeRequestListResponseV1Template(resizeRequestsPages [][]resizeRequestResponseV1Mock) []string {
	const resizeRequestListResponseTemplate = `
	{
		"id": "113123123",
		"nextPageToken": %q,
		"items": [%s]
	}`

	responses := make([]string, 0, len(resizeRequestsPages))
	for i, resizeRequests := range resizeRequestsPages {
		items := make([]string, 0, len(resizeRequests))
		for _, rr := range resizeRequests {
			items = append(items, fillResizeRequestResponseV1Template(rr))
		}
		nextToken := fmt.Sprintf("%d", i)
		if i == len(resizeRequestsPages)-1 {
			nextToken = ""
		}
		responses = append(responses, fmt.Sprintf(resizeRequestListResponseTemplate, nextToken, strings.Join(items, ",")))
	}

	return responses
}

func fillResizeRequestResponseV1Template(rr resizeRequestResponseV1Mock) string {
	const (
		resizeRequestResponseTemplate = `
{
	"id": "%d",
	"name": "%s",
	"resizeBy": %d,
	"creationTimestamp": "%s",
	"state": "%s",
	%s
	"status": {
		"error": {
			"errors": [%s]
		},
		"lastAttempt": {
			"error": {
				"errors": [%s]
			}
		}
	}
}`
		errorTemplate = `
{
	"code": "%s",
	"location": "%s",
	"message": "%s"
}`

		requestedRunTemplate = `
	"requestedRunDuration": {
		"nanos": %d,
		"seconds": "%d"
	},
`
		lastAttemptErrorTemplate = `
	{
		"code": "%s",
		"message": "%s",
		"errorDetails": [{%s}]
	}
	`

		lastAttemptErrorDetailsTemplate = `
	"errorInfo": {
		"reason": "%s",
		%s
	}`

		lastAttemptErrorDetailsNoReasonTemplate = `
	"errorInfo": {
		%s
	}`

		lastAttemptErrorDetailsMetadataTemplate = `
	"metadatas": {
		"vmType": "%s",
		"attachment": "%s"
	}`
	)

	errors := make([]string, 0, len(rr.errors))
	for _, e := range rr.errors {
		errors = append(errors, fmt.Sprintf(errorTemplate, e.code, e.location, e.message))
	}
	requestedRunDuration := ""
	if rr.requestedRunDuration != nil {
		requestedRunDuration = fmt.Sprintf(requestedRunTemplate, rr.requestedRunDuration.Nanos, rr.requestedRunDuration.Seconds)
	}
	lastAttemptErrors := make([]string, 0, len(rr.lastAttemptErrors))
	for _, e := range rr.lastAttemptErrors {
		errorDetails := ""
		if e.errorDetails != nil {
			var metadata string
			if e.errorDetails.metadatas != nil {
				metadata = fmt.Sprintf(lastAttemptErrorDetailsMetadataTemplate, e.errorDetails.metadatas["vmType"], e.errorDetails.metadatas["attachment"])
			}
			if len(e.errorDetails.reason) > 0 {
				errorDetails = fmt.Sprintf(lastAttemptErrorDetailsTemplate, e.errorDetails.reason, metadata)
			} else {
				errorDetails = fmt.Sprintf(lastAttemptErrorDetailsNoReasonTemplate, metadata)
			}
		}
		lastAttemptErrors = append(lastAttemptErrors, fmt.Sprintf(lastAttemptErrorTemplate, e.code, e.message, errorDetails))
	}
	return fmt.Sprintf(resizeRequestResponseTemplate, rr.id, rr.name, rr.resizeBy, rr.creationTimestamp, rr.state, requestedRunDuration, strings.Join(errors, ","), strings.Join(lastAttemptErrors, ","))
}

func newTestResizeRequestClientV1(t *testing.T, projectID, url string) *resizeRequestClientV1 {
	httpClient := &http.Client{}
	client, err := NewResizeRequestClientV1(httpClient, projectID, "", url, ResizeRequestModeAtomic)
	if err != nil {
		t.Fatalf("fatal error: %v", err)
	}
	return client
}
