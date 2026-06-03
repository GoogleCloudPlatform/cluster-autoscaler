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

package manager

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	oss_test "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
)

const createResizeRequestResponse = `{
  "kind": "compute#operation",
  "id": "2890052495600280364",
  "name": "operation-1624366531120-5c55a4e128c15-fc5daa90-42ef6c32",
  "zone": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b",
  "operationType": "compute.instanceGroupManagers.createResizeRequest",
  "targetLink": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool-e25725dc-grp",
  "targetId": "7836594831806456968",
  "status": "DONE",
  "user": "user@example.com",
  "progress": 100,
  "insertTime": "2021-06-22T05:55:31.903-07:00",
  "startTime": "2021-06-22T05:55:31.907-07:00",
  "selfLink": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b/operations/operation-1624366531120-5c55a4e128c15-fc5daa90-e1ef6c32"
}`

const createResizeRequestOperationResponse = `{
  "kind": "compute#operation",
  "id": "2890052495600280364",
  "name": "operation-1624366531120-5c55a4e128c15-fc5daa90-e1ef6c32",
  "zone": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b",
  "operationType": "compute.instanceGroupManagers.createResizeRequest",
  "targetLink": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b/instanceGroupManagers/gke-cluster-1-default-pool-e25725dc-grp",
  "targetId": "7836594831806456968",
  "status": "DONE",
  "user": "user@example.com",
  "progress": 100,
  "insertTime": "2021-06-22T05:55:31.903-07:00",
  "startTime": "2021-06-22T05:55:31.907-07:00",
  "selfLink": "https://www.googleapis.com/compute/v1/projects/project1/zones/us-central1-b/operations/operation-1624366531120-5c55a4e128c15-fc5daa90-e1ef6c32"
}`

const (
	projectId = "project1"
	migName   = "test-mig-name"
	version   = "1.25.1-gke1000"
	zoneA     = "us-central1-a"
)

func TestCreateQueuedBulkInstances(t *testing.T) {
	initTime := time.Date(2022, 11, 12, 13, 14, 15, 0, time.UTC)

	initLabels := map[string]string{gkelabels.ProvisioningRequestLabelKey: "init", "key": "val"}
	finalLabels := map[string]string{gkelabels.ProvisioningRequestLabelKey: resizerequestclient.ResizeRequestName("default", "pr-0"), "key": "val"}

	tests := []struct {
		name                string
		simulateNPUpdateErr bool
		simulateResizeErr   bool
		wantState           provreqstate.ProvisioningRequestState
		wantLabels          map[string]string
		wantSize            int
	}{
		{
			name:                "updateNodePoolLabels_error_markAsFailed",
			simulateNPUpdateErr: true,
			wantState:           provreqstate.FailedState,
			wantLabels:          initLabels,
			wantSize:            0,
		},
		{
			name:              "increaseSize_error_markAsFailed",
			simulateResizeErr: true,
			wantState:         provreqstate.FailedState,
			wantLabels:        finalLabels,
			wantSize:          0,
		},
		{
			name:       "success_markAsAccepted",
			wantState:  provreqstate.AcceptedState,
			wantLabels: finalLabels,
			wantSize:   1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			provReq := provreqstate.ProvisioningRequestInStateForTests("default", "pr-0", "", "", provreqstate.PendingState, initTime, time.Minute)

			fakeClient := provreqclient.NewFakeProvisioningRequestClient(ctx, t, provReq)
			m := &provReqManager{
				prClient: fakeClient,
			}

			mig := &testMig{
				NodeGroup: oss_test.NewTestNodeGroup(migName, 1, 1, 11, true, false, "n1-standard-4", nil, nil),
				gceRef: gce.GceRef{
					Project: projectId,
					Zone:    zoneA,
					Name:    migName,
				},
				labels:              initLabels,
				simulateNPUpdateErr: tc.simulateNPUpdateErr,
				simulateResizeErr:   tc.simulateResizeErr,
			}

			spec := &ProvisioningRequestDetailsSpec{
				ProjectID:          projectId,
				ProvReqNamespace:   provReq.Namespace,
				ProvReqName:        provReq.Name,
				Zone:               mig.GceRef().Zone,
				Delta:              1,
				MigName:            mig.GceRef().Name,
				NodePoolName:       fmt.Sprintf("np-%s", mig.GceRef().Name),
				AcceleratorType:    "nvidia-tesla-t4",
				MigAutoProvisioned: mig.Autoprovisioned(),
				ProvisioningMode:   queuedwrapper.ProvisioningModeBulkMig,
			}

			err := m.CreateQueuedBulkInstances(mig, spec)
			assert.NoError(t, err)
			assertProvReqIsUpdated(t, ctx, tc.wantState, fakeClient, spec, provReq.PodTemplates, true)
			assert.Equal(t, tc.wantLabels, mig.labels)
			assert.Equal(t, tc.wantSize, mig.size)
		})
	}
}

func TestCreateResizeRequest(t *testing.T) {
	exampleInitTime := time.Date(2022, 11, 12, 13, 14, 15, 0, time.UTC)
	exampleTimeInc := time.Minute

	tests := []struct {
		name                       string
		provReq                    *provreqwrapper.ProvisioningRequest
		shouldUpdateProvReqDetails ShouldUpdateProvReqDetails
		expectedState              provreqstate.ProvisioningRequestState
		expectedCreationCall       bool
	}{
		{
			name:                       "simple test",
			provReq:                    provreqstate.ProvisioningRequestInStateForTests("test-namespace", "test-name", "", "", provreqstate.PendingState, exampleInitTime, exampleTimeInc),
			expectedState:              provreqstate.AcceptedState,
			expectedCreationCall:       true,
			shouldUpdateProvReqDetails: UpdateProvReqDetails,
		},
		{
			name:                       "provreq with 10min maxRunDuration",
			provReq:                    provreqstate.ProvisioningRequestInStateForTests("test-namespace", "test-name", "", "", provreqstate.PendingState, exampleInitTime, exampleTimeInc, provreqstate.WithMaxRunDuration("6000")),
			expectedState:              provreqstate.AcceptedState,
			expectedCreationCall:       true,
			shouldUpdateProvReqDetails: UpdateProvReqDetails,
		},
		{
			name:                       "provreq with wrong maxRunDuration",
			provReq:                    provreqstate.ProvisioningRequestInStateForTests("test-namespace", "test-name", "", "", provreqstate.PendingState, exampleInitTime, exampleTimeInc, provreqstate.WithMaxRunDuration("tests")),
			expectedState:              provreqstate.FailedState,
			shouldUpdateProvReqDetails: UpdateProvReqDetails,
		},
		{
			name:                       "skip_update",
			provReq:                    provreqstate.ProvisioningRequestInStateForTests("test-namespace", "test-name", "", "", provreqstate.PendingState, exampleInitTime, exampleTimeInc),
			expectedCreationCall:       true,
			expectedState:              provreqstate.PendingState,
			shouldUpdateProvReqDetails: DoNotUpdateProvReqDetails,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fakeClient := provreqclient.NewFakeProvisioningRequestClient(ctx, t, tc.provReq)
			server := test.NewHttpServerMock()
			defer server.Close()
			m := &provReqManager{
				queuedResizeRequestService: newTestResizeRequestClient(t, projectId, server.URL),
				prClient:                   fakeClient,
			}

			mig := setupTestZonalNodePool()
			if tc.expectedCreationCall {
				server.On("handle", fmt.Sprintf("/projects/project1/zones/us-central1-a/instanceGroupManagers/%v/resizeRequests", mig.GceRef().Name)).Return(createResizeRequestResponse).Once()
				server.On("handle", "/projects/project1/zones/us-central1-a/operations/operation-1624366531120-5c55a4e128c15-fc5daa90-42ef6c32").Return(createResizeRequestOperationResponse).Once()
			}
			spec := &ProvisioningRequestDetailsSpec{
				ProjectID:          projectId,
				ProvReqNamespace:   tc.provReq.Namespace,
				ProvReqName:        tc.provReq.Name,
				Zone:               mig.GceRef().Zone,
				Delta:              1,
				MigName:            mig.GceRef().Name,
				NodePoolName:       fmt.Sprintf("np-%s", mig.GceRef().Name),
				AcceleratorType:    "nvidia-tesla-t4",
				MigAutoProvisioned: mig.Autoprovisioned(),
				ProvisioningMode:   queuedwrapper.ProvisioningModeResizeRequest,
			}

			err := m.CreateResizeRequest(spec, tc.shouldUpdateProvReqDetails)
			assert.NoError(t, err)
			mock.AssertExpectationsForObjects(t, server)
			assertProvReqIsUpdated(t, ctx, tc.expectedState, fakeClient, spec, tc.provReq.PodTemplates, false)
		})
	}
}

type fakeProvReqClient interface {
	UpdateProvisioningRequest(*v1.ProvisioningRequest) (*v1.ProvisioningRequest, error)
	DeleteProvisioningRequest(*v1.ProvisioningRequest) error
	ProvisioningRequests() ([]*provreqwrapper.ProvisioningRequest, error)
	ProvisioningRequest(string, string) (*provreqwrapper.ProvisioningRequest, error)
	ProvisioningRequestNoCache(string, string) (*provreqwrapper.ProvisioningRequest, error)
}

func assertProvReqIsUpdated(t *testing.T, ctx context.Context, expectedState provreqstate.ProvisioningRequestState, client fakeProvReqClient, spec *ProvisioningRequestDetailsSpec, podTemplates []*apiv1.PodTemplate, bulkProvReq bool) {
	t.Helper()

	var resizeRequestName string
	if !bulkProvReq {
		resizeRequestName = resizerequestclient.ResizeRequestName(spec.ProvReqNamespace, spec.ProvReqName)
	}

	prFound, err := client.ProvisioningRequestNoCache(spec.ProvReqNamespace, spec.ProvReqName)
	if err != nil {
		t.Errorf("Got error while fetching the updated Provisioning Request: %v", err)
	}
	assert.Equal(t, expectedState, provreqstate.StateOfProvisioningRequest(prFound))

	qpr := queuedwrapper.ToQueuedProvisioningRequest(*prFound)
	switch expectedState {
	case provreqstate.AcceptedState:
		if !bulkProvReq {
			assert.Equal(t, *qpr.ResizeRequestName(), resizeRequestName)
		} else {
			assert.Nil(t, qpr.ResizeRequestName())
		}
		assert.Equal(t, *qpr.NodeGroupName(), spec.MigName)
		assert.Equal(t, *qpr.NodePoolName(), spec.NodePoolName)
		assert.Equal(t, *qpr.AcceleratorType(), spec.AcceleratorType)
		assert.Equal(t, *qpr.SelectedZone(), spec.Zone)
		assert.Equal(t, *qpr.NodePoolAutoProvisioned(), strconv.FormatBool(spec.MigAutoProvisioned))
		assert.Equal(t, *qpr.PodTemplateName(), PodTemplateNames(podTemplates))
		assert.Equal(t, *qpr.ProvisioningMode(), spec.ProvisioningMode)
	case provreqstate.PendingState:
		assert.Empty(t, qpr.Status.ProvisioningClassDetails)
	}
}

func newTestResizeRequestClient(t *testing.T, projectId, url string) resizerequestclient.ResizeRequestClient {
	t.Helper()
	client := &http.Client{}
	resReqClient, err := resizerequestclient.NewResizeRequestClientV1(client, projectId, "cluster-autoscaler", url, resizerequestclient.ResizeRequestModeQueued)
	if !assert.NoError(t, err) {
		t.Fatalf("fatal error: %v", err)
	}
	return resReqClient
}

func setupTestZonalNodePool() gce.Mig {
	return &testMig{
		NodeGroup: oss_test.NewTestNodeGroup(migName, 1, 1, 11, true, false, "n1-standard-4", nil, nil),
		gceRef: gce.GceRef{
			Project: projectId,
			Zone:    zoneA,
			Name:    migName,
		},
		version: version,
	}
}

// testMig is needed to mock gce.Mig, as we cannot import the gke package.
type testMig struct {
	cloudprovider.NodeGroup
	npName  string
	gceRef  gce.GceRef
	version string
	labels  map[string]string
	size    int

	simulateNPUpdateErr bool
	simulateResizeErr   bool
}

func (m *testMig) GceRef() gce.GceRef {
	return m.gceRef
}

func (m *testMig) Version() string {
	return m.version
}

func (m *testMig) IncreaseSize(delta int) error {
	if m.simulateResizeErr {
		return fmt.Errorf("resize error")
	}
	m.size += delta
	return nil
}

func (m *testMig) Spec() *gkeclient.NodePoolSpec {
	return &gkeclient.NodePoolSpec{Labels: m.labels}
}

func (m *testMig) NodePoolName() string {
	return m.npName
}

func (m *testMig) UpdateNodePoolLabels(labels map[string]string) error {
	if m.simulateNPUpdateErr {
		return fmt.Errorf("update error")
	}
	m.labels = labels
	return nil
}

func (m *testMig) IsStable() (bool, error) {
	return true, nil
}
