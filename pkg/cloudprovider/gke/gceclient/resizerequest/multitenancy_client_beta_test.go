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
	"testing"

	"github.com/stretchr/testify/assert"
	gce_api_beta "google.golang.org/api/compute/v0.beta"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	test_util "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/multitenancy"
)

type fakeResizeRequestClient struct {
	ResizeRequestClient
	projectID string
}

func TestAddProviderConfig(t *testing.T) {
	t.Parallel()

	clusterProjectID := "vertex-clusterproject"
	tests := []struct {
		name            string
		providerConfigs []*multitenancy.ProviderConfig
		migRef          gce.GceRef
		wantErr         bool
	}{
		{
			name: "matching_project",
			providerConfigs: []*multitenancy.ProviderConfig{
				{
					Name:          "t1234-foo",
					ProjectID:     "vertex-tp-1",
					ProjectNumber: 1234,
				},
			},
			migRef: gce.GceRef{
				Project: "vertex-tp-1",
				Zone:    "us-central1-a",
				Name:    "foo",
			},
			wantErr: false,
		},
		{
			name: "non_matching_project",
			providerConfigs: []*multitenancy.ProviderConfig{
				{
					Name:          "t1234-foo",
					ProjectID:     "vertex-tp-1",
					ProjectNumber: 1234,
				},
			},
			migRef: gce.GceRef{
				Project: "vertex-tp-555555",
				Zone:    "us-central1-a",
				Name:    "foo",
			},
			wantErr: true,
		},
		{
			name: "multiple_matching_providerconfigs",
			providerConfigs: []*multitenancy.ProviderConfig{
				{
					Name:          "t1234-foo",
					ProjectID:     "vertex-tp-1",
					ProjectNumber: 1234,
				},
				{
					Name:          "t1234-banana",
					ProjectID:     "vertex-tp-1",
					ProjectNumber: 1234,
				},
			},
			migRef: gce.GceRef{
				Project: "vertex-tp-1",
				Zone:    "us-central1-a",
				Name:    "foo",
			},
			wantErr: false,
		},
		{
			name: "different_projects",
			providerConfigs: []*multitenancy.ProviderConfig{
				{
					Name:          "t1234-foo",
					ProjectID:     "vertex-tp-1",
					ProjectNumber: 1234,
				},
				{
					Name:          "t5678-banana",
					ProjectID:     "vertex-tp-2",
					ProjectNumber: 5678,
				},
			},
			migRef: gce.GceRef{
				Project: "vertex-tp-1",
				Zone:    "us-central1-a",
				Name:    "foo",
			},
			wantErr: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			server := test_util.NewHttpServerMock()
			defer server.Close()
			server.On("handle", "/projects/vertex-tp-1/zones/us-central1-a/instanceGroupManagers/foo/resizeRequests").Return(resizeRequestCreationBetaResponse).Once()
			server.On("handle", "/projects/vertex-tp-1/zones/us-central1-a/operations/operation-1669031976964-5edf9ca1b11e5-b4d28da7-c9b8624c").Return(operationRunningBetaResponse).Once()
			server.On("handle", "/projects/vertex-tp-1/zones/us-central1-a/operations/operation-1669031976964-5edf9ca1b11e5-b4d28da7-c9b8624c").Return(operationDoneBetaResponse).Once()

			client := &http.Client{}
			resizeRequestClient, err := NewMultitenancyResizeRequestClientBeta(client, clusterProjectID, 12345, "user-agent", server.URL, ResizeRequestModeAtomic, nil)
			if err != nil {
				t.Errorf("expect nil error, got: %v", err)
			}
			for _, providerConfig := range tc.providerConfigs {
				err := resizeRequestClient.AddProviderConfig(providerConfig)
				if err != nil {
					t.Errorf("got: %v, want: nil", err)
					return
				}
			}
			err = resizeRequestClient.CreateResizeRequest(context.Background(), tc.migRef, ResizeRequestCreateRequest{})
			if (err != nil) != tc.wantErr {
				t.Errorf("got: %v, want: %v", err, tc.wantErr)
				return
			}
		})
	}
}

func TestDeleteProviderConfig(t *testing.T) {
	t.Parallel()

	clusterProjectID := "vertex-clusterproject"
	tests := []struct {
		name                        string
		providerConfigsToAdd        []*multitenancy.ProviderConfig
		providerConfigsToDelete     []*multitenancy.ProviderConfig
		migRef                      gce.GceRef
		wantGCEErr                  bool
		wantDeleteProviderConfigErr bool
	}{
		{
			name: "providerconfig_deleted_for_gceref",
			providerConfigsToAdd: []*multitenancy.ProviderConfig{
				{
					Name:          "t1234-foo",
					ProjectID:     "vertex-tp-1",
					ProjectNumber: 1234,
				},
			},
			providerConfigsToDelete: []*multitenancy.ProviderConfig{
				{
					Name:          "t1234-foo",
					ProjectID:     "vertex-tp-1",
					ProjectNumber: 1234,
				},
			},
			migRef: gce.GceRef{
				Project: "vertex-tp-1",
				Zone:    "us-central1-a",
				Name:    "foo",
			},
			wantGCEErr: true,
		},
		{
			name: "non_existing_providerconfig",
			providerConfigsToDelete: []*multitenancy.ProviderConfig{
				{
					Name:          "t1234-foo",
					ProjectID:     "vertex-tp-1",
					ProjectNumber: 1234,
				},
			},
			migRef: gce.GceRef{
				Project: "vertex-tp-1",
				Zone:    "us-central1-a",
				Name:    "foo",
			},
			wantGCEErr:                  true,
			wantDeleteProviderConfigErr: true,
		},
		{
			name: "many_providerconfigs_for_project",
			providerConfigsToAdd: []*multitenancy.ProviderConfig{
				{
					Name:          "t1234-foo",
					ProjectID:     "vertex-tp-1",
					ProjectNumber: 1234,
				},
				{
					Name:          "t1234-banana",
					ProjectID:     "vertex-tp-1",
					ProjectNumber: 1234,
				},
			},
			providerConfigsToDelete: []*multitenancy.ProviderConfig{
				{
					Name:          "t1234-foo",
					ProjectID:     "vertex-tp-1",
					ProjectNumber: 1234,
				},
			},
			migRef: gce.GceRef{
				Project: "vertex-tp-1",
				Zone:    "us-central1-a",
				Name:    "foo",
			},
		},
		{
			name: "unrelated_providerconfig_deleted",
			providerConfigsToAdd: []*multitenancy.ProviderConfig{
				{
					Name:          "t1234-foo",
					ProjectID:     "vertex-tp-1",
					ProjectNumber: 1234,
				},
				{
					Name:          "t5678-banana",
					ProjectID:     "vertex-tp-2",
					ProjectNumber: 5678,
				},
			},
			providerConfigsToDelete: []*multitenancy.ProviderConfig{
				{
					Name:          "t5678-banana",
					ProjectID:     "vertex-tp-2",
					ProjectNumber: 5678,
				},
			},
			migRef: gce.GceRef{
				Project: "vertex-tp-1",
				Zone:    "us-central1-a",
				Name:    "foo",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			server := test_util.NewHttpServerMock()
			defer server.Close()
			server.On("handle", "/projects/vertex-tp-1/zones/us-central1-a/instanceGroupManagers/foo/resizeRequests").Return(resizeRequestCreationBetaResponse).Once()
			server.On("handle", "/projects/vertex-tp-1/zones/us-central1-a/operations/operation-1669031976964-5edf9ca1b11e5-b4d28da7-c9b8624c").Return(operationRunningBetaResponse).Once()
			server.On("handle", "/projects/vertex-tp-1/zones/us-central1-a/operations/operation-1669031976964-5edf9ca1b11e5-b4d28da7-c9b8624c").Return(operationDoneBetaResponse).Once()

			client := &http.Client{}
			resizeRequestClient, err := NewMultitenancyResizeRequestClientBeta(client, clusterProjectID, 12345, "user-agent", server.URL, ResizeRequestModeAtomic, nil)
			if err != nil {
				t.Errorf("expect nil error, got: %v", err)
			}
			for _, providerConfig := range tc.providerConfigsToAdd {
				err := resizeRequestClient.AddProviderConfig(providerConfig)
				if err != nil {
					t.Errorf("got: %v, want: nil", err)
					return
				}
			}
			for _, providerConfig := range tc.providerConfigsToDelete {
				err := resizeRequestClient.DeleteProviderConfig(providerConfig)
				if (err != nil) != tc.wantDeleteProviderConfigErr {
					t.Errorf("got: %v, want: %v", err, tc.wantDeleteProviderConfigErr)
					return
				}
			}
			err = resizeRequestClient.CreateResizeRequest(context.Background(), tc.migRef, ResizeRequestCreateRequest{})
			if (err != nil) != tc.wantGCEErr {
				t.Errorf("got: %v, want: %v", err, tc.wantGCEErr)
				return
			}
		})
	}
}

func TestCreateResizeRequest(t *testing.T) {
	t.Parallel()

	server := test_util.NewHttpServerMock()
	defer server.Close()
	server.On("handle", "/projects/vertex-tp-1/zones/us-central1-a/instanceGroupManagers/foo/resizeRequests").Return(resizeRequestCreationBetaResponse).Once()
	server.On("handle", "/projects/vertex-tp-1/zones/us-central1-a/operations/operation-1669031976964-5edf9ca1b11e5-b4d28da7-c9b8624c").Return(operationRunningBetaResponse).Once()
	server.On("handle", "/projects/vertex-tp-1/zones/us-central1-a/operations/operation-1669031976964-5edf9ca1b11e5-b4d28da7-c9b8624c").Return(operationDoneBetaResponse).Once()
	server.On("handle", "/projects/vertex-clusterproject/zones/us-central1-a/instanceGroupManagers/foo/resizeRequests").Return(resizeRequestCreationBetaResponse).Once()
	server.On("handle", "/projects/vertex-clusterproject/zones/us-central1-a/operations/operation-1669031976964-5edf9ca1b11e5-b4d28da7-c9b8624c").Return(operationRunningBetaResponse).Once()
	server.On("handle", "/projects/vertex-clusterproject/zones/us-central1-a/operations/operation-1669031976964-5edf9ca1b11e5-b4d28da7-c9b8624c").Return(operationDoneBetaResponse).Once()

	client := &http.Client{}
	resizeRequestClient, err := NewMultitenancyResizeRequestClientBeta(client, "vertex-clusterproject", 12345, "user-agent", server.URL, ResizeRequestModeAtomic, nil)
	if err != nil {
		t.Errorf("expect nil error, got: %v", err)
	}
	err = resizeRequestClient.AddProviderConfig(&multitenancy.ProviderConfig{
		Name:      "t1234-foo",
		ProjectID: "vertex-tp-1",
	})
	if err != nil {
		t.Errorf("expect nil error, got: %v", err)
	}
	err = resizeRequestClient.CreateResizeRequest(context.Background(), gce.GceRef{Project: "vertex-tp-1", Zone: "us-central1-a", Name: "foo"}, ResizeRequestCreateRequest{})
	if err != nil {
		t.Errorf("expect nil error, got: %v", err)
	}
	err = resizeRequestClient.CreateResizeRequest(context.Background(), gce.GceRef{Project: "vertex-clusterproject", Zone: "us-central1-a", Name: "foo"}, ResizeRequestCreateRequest{})
	if err != nil {
		t.Errorf("expect nil error, got: %v", err)
	}
}

func TestAdvanceResizeRequestCleanUp(t *testing.T) {
	t.Parallel()

	server := test_util.NewHttpServerMock()
	defer server.Close()
	server.On("handle", "/projects/vertex-clusterproject/zones/us-central1-a/instanceGroupManagers/foo/resizeRequests/qwert12345q").Return(deleteResizeRequestBetaResponse).Once()
	server.On("handle", "/projects/vertex-tp-1/zones/us-central1-a/instanceGroupManagers/foo/resizeRequests/qwert12345q").Return(deleteResizeRequestBetaResponse).Once()

	client := &http.Client{}
	resizeRequestClient, err := NewMultitenancyResizeRequestClientBeta(client, "vertex-clusterproject", 12345, "user-agent", server.URL, ResizeRequestModeAtomic, nil)
	if err != nil {
		t.Errorf("expect nil error, got: %v", err)
	}
	err = resizeRequestClient.AddProviderConfig(&multitenancy.ProviderConfig{
		Name:      "t1234-foo",
		ProjectID: "vertex-tp-1",
	})
	if err != nil {
		t.Errorf("expect nil error, got: %v", err)
	}
	err = resizeRequestClient.AdvanceResizeRequestCleanUp(context.Background(), ResizeRequestStatus{ProjectID: "vertex-tp-1", MigName: "foo", Zone: "us-central1-a", Name: "qwert12345q", State: ResizeRequestStateSucceeded, ID: 123456789})
	if err != nil {
		t.Errorf("expect nil error, got: %v", err)
	}
	err = resizeRequestClient.AdvanceResizeRequestCleanUp(context.Background(), ResizeRequestStatus{ProjectID: "vertex-clusterproject", MigName: "foo", Zone: "us-central1-a", Name: "qwert12345q", State: ResizeRequestStateSucceeded, ID: 123456789})
	if err != nil {
		t.Errorf("expect nil error, got: %v", err)
	}
}

func TestResizeRequest(t *testing.T) {
	t.Parallel()

	server := test_util.NewHttpServerMock()
	defer server.Close()
	resp, err := gce_api_beta.InstanceGroupManagerResizeRequest{
		Name:              "foo",
		Kind:              "compute#instanceGroupManagerResizeRequest",
		CreationTimestamp: "2006-01-02T15:04:05Z",
		State:             "ACCEPTED",
		Status:            &gce_api_beta.InstanceGroupManagerResizeRequestStatus{},
	}.MarshalJSON()
	if err != nil {
		t.Errorf("expect nil error, got: %v", err)
	}
	server.On("handle", "/projects/vertex-tp-1/zones/us-central1-a/instanceGroupManagers/foo/resizeRequests/some-name").Return(string(resp)).Once()
	server.On("handle", "/projects/vertex-clusterproject/zones/us-central1-a/instanceGroupManagers/foo/resizeRequests/some-name").Return(string(resp)).Once()

	client := &http.Client{}
	resizeRequestClient, err := NewMultitenancyResizeRequestClientBeta(client, "vertex-clusterproject", 12345, "user-agent", server.URL, ResizeRequestModeAtomic, nil)
	if err != nil {
		t.Errorf("expect nil error, got: %v", err)
	}
	err = resizeRequestClient.AddProviderConfig(&multitenancy.ProviderConfig{
		Name:      "t1234-foo",
		ProjectID: "vertex-tp-1",
	})
	if err != nil {
		t.Errorf("expect nil error, got: %v", err)
	}
	_, err = resizeRequestClient.ResizeRequest(context.Background(), gce.GceRef{Project: "vertex-tp-1", Zone: "us-central1-a", Name: "foo"}, "some-name")
	if err != nil {
		t.Errorf("expect nil error, got: %v", err)
	}
	_, err = resizeRequestClient.ResizeRequest(context.Background(), gce.GceRef{Project: "vertex-clusterproject", Zone: "us-central1-a", Name: "foo"}, "some-name")
	if err != nil {
		t.Errorf("expect nil error, got: %v", err)
	}
}

func TestResizeRequests(t *testing.T) {
	t.Parallel()

	server := test_util.NewHttpServerMock()
	defer server.Close()
	resp, err := gce_api_beta.InstanceGroupManagerResizeRequestsListResponse{
		Kind: "compute#instanceGroupManagerResizeRequestList",
	}.MarshalJSON()
	if err != nil {
		t.Errorf("expect nil error, got: %v", err)
	}
	server.On("handle", "/projects/vertex-tp-1/zones/us-central1-a/instanceGroupManagers/foo/resizeRequests").Return(string(resp)).Once()
	server.On("handle", "/projects/vertex-clusterproject/zones/us-central1-a/instanceGroupManagers/foo/resizeRequests").Return(string(resp)).Once()

	client := &http.Client{}
	resizeRequestClient, err := NewMultitenancyResizeRequestClientBeta(client, "vertex-clusterproject", 12345, "user-agent", server.URL, ResizeRequestModeAtomic, nil)
	if err != nil {
		t.Errorf("expect nil error, got: %v", err)
	}
	err = resizeRequestClient.AddProviderConfig(&multitenancy.ProviderConfig{
		Name:      "t1234-foo",
		ProjectID: "vertex-tp-1",
	})
	if err != nil {
		t.Errorf("expect nil error, got: %v", err)
	}
	_, err = resizeRequestClient.ResizeRequests(context.Background(), gce.GceRef{Project: "vertex-tp-1", Zone: "us-central1-a", Name: "foo"})
	if err != nil {
		t.Errorf("expect nil error, got: %v", err)
	}
	_, err = resizeRequestClient.ResizeRequests(context.Background(), gce.GceRef{Project: "vertex-clusterproject", Zone: "us-central1-a", Name: "foo"})
	if err != nil {
		t.Errorf("expect nil error, got: %v", err)
	}
}

func (f *fakeResizeRequestClient) CreateResizeRequest(ctx context.Context, migRef gce.GceRef, createRequest ResizeRequestCreateRequest) error {
	return nil
}

func TestMultitenancyResizeRequestClient_AddDeleteProviderConfig(t *testing.T) {
	clientCount := 0
	mtClient := &multitenancyResizeRequestClientBeta{
		resizeRequestClients:    map[string]ResizeRequestClient{},
		projectToProviderConfig: map[string]sets.Set[string]{},
		createClientFunc: func(projectID string, pc *multitenancy.ProviderConfig) (ResizeRequestClient, error) {
			clientCount++
			return &fakeResizeRequestClient{projectID: projectID}, nil
		},
	}

	pc1 := &multitenancy.ProviderConfig{
		Name:      "tenant-1",
		ProjectID: "project-1",
	}
	pc2 := &multitenancy.ProviderConfig{
		Name:      "tenant-2",
		ProjectID: "project-1", // Shared project
	}

	// 1. Add first tenant
	err := mtClient.AddProviderConfig(pc1)
	assert.NoError(t, err)
	assert.Equal(t, 1, clientCount)
	assert.Contains(t, mtClient.resizeRequestClients, "project-1")

	// 2. Add second tenant (shared project)
	err = mtClient.AddProviderConfig(pc2)
	assert.NoError(t, err)
	assert.Equal(t, 1, clientCount, "Should reuse existing client for shared project")

	// 3. Delete first tenant
	err = mtClient.DeleteProviderConfig(pc1)
	assert.NoError(t, err)
	assert.Contains(t, mtClient.resizeRequestClients, "project-1", "Client should persist while tenant-2 exists")

	// 4. Delete second tenant
	err = mtClient.DeleteProviderConfig(pc2)
	assert.NoError(t, err)
	assert.NotContains(t, mtClient.resizeRequestClients, "project-1", "Client should be deleted with last tenant")
}

func TestMultitenancyResizeRequestClient_UpdateAuthConfig(t *testing.T) {
	clientCount := 0
	mtClient := &multitenancyResizeRequestClientBeta{
		resizeRequestClients:    map[string]ResizeRequestClient{},
		projectToProviderConfig: map[string]sets.Set[string]{},
		createClientFunc: func(projectID string, pc *multitenancy.ProviderConfig) (ResizeRequestClient, error) {
			clientCount++
			return &fakeResizeRequestClient{projectID: projectID}, nil
		},
	}

	pc := &multitenancy.ProviderConfig{
		Name:      "tenant-1",
		ProjectID: "project-1",
		AuthConfig: &multitenancy.AuthConfig{
			TokenURL: "url-1",
		},
	}

	// 1. Initial add
	err := mtClient.AddProviderConfig(pc)
	assert.NoError(t, err)
	assert.Equal(t, 1, clientCount)

	// 2. Update with SAME config
	err = mtClient.AddProviderConfig(pc)
	assert.NoError(t, err)
	assert.Equal(t, 1, clientCount, "Should NOT recreate client for same config")

	// 3. Update with DIFFERENT AuthConfig
	pcUpdated := &multitenancy.ProviderConfig{
		Name:      "tenant-1",
		ProjectID: "project-1",
		AuthConfig: &multitenancy.AuthConfig{
			TokenURL: "url-2",
		},
	}
	err = mtClient.AddProviderConfig(pcUpdated)
	assert.NoError(t, err)
	assert.Equal(t, 1, clientCount, "Should NOT recreate client when AuthConfig changes after simplification")
}
