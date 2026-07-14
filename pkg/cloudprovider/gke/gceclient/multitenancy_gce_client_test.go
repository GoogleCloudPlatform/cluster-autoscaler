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
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"golang.org/x/oauth2"
	gce_api_beta "google.golang.org/api/compute/v0.beta"
	gce_api "google.golang.org/api/compute/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/multitenancy"
	"k8s.io/utils/ptr"
)

func TestAddProviderConfig(t *testing.T) {
	tests := []struct {
		name            string
		providerConfigs []*multitenancy.ProviderConfig
		gceRef          gce.GceRef
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
			gceRef: gce.GceRef{
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
			gceRef: gce.GceRef{
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
			gceRef: gce.GceRef{
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
			gceRef: gce.GceRef{
				Project: "vertex-tp-1",
				Zone:    "us-central1-a",
				Name:    "foo",
			},
			wantErr: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
				operation := gce_api.Operation{
					Status: "DONE",
				}
				b, _ := json.Marshal(operation)
				res.WriteHeader(http.StatusOK)
				res.Write(b)
			}))
			defer server.Close()
			mtGCEClient := createDefaultMTGCEClient(t, server.URL)
			for _, providerConfig := range tc.providerConfigs {
				err := mtGCEClient.AddProviderConfig(providerConfig)
				if err != nil {
					t.Errorf("got: %v, want: nil", err)
					return
				}
			}
			_, err := mtGCEClient.CreateInstances(tc.gceRef, "", 0, nil)
			if (err != nil) != tc.wantErr {
				t.Errorf("got: %v, want: %v", err, tc.wantErr)
				return
			}
		})
	}
}

func TestDeleteProviderConfig(t *testing.T) {
	tests := []struct {
		name                        string
		providerConfigsToAdd        []*multitenancy.ProviderConfig
		providerConfigsToDelete     []*multitenancy.ProviderConfig
		gceRef                      gce.GceRef
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
			gceRef: gce.GceRef{
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
			gceRef: gce.GceRef{
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
			gceRef: gce.GceRef{
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
			gceRef: gce.GceRef{
				Project: "vertex-tp-1",
				Zone:    "us-central1-a",
				Name:    "foo",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
				operation := gce_api.Operation{
					Status: "DONE",
				}
				b, _ := json.Marshal(operation)
				res.WriteHeader(http.StatusOK)
				res.Write(b)
			}))
			defer server.Close()
			mtGCEClient := createDefaultMTGCEClient(t, server.URL)
			for _, providerConfig := range tc.providerConfigsToAdd {
				err := mtGCEClient.AddProviderConfig(providerConfig)
				if err != nil {
					t.Errorf("got: %v, want: nil", err)
					return
				}
			}
			for _, providerConfig := range tc.providerConfigsToDelete {
				err := mtGCEClient.DeleteProviderConfig(providerConfig)
				if (err != nil) != tc.wantDeleteProviderConfigErr {
					t.Errorf("got: %v, want: %v", err, tc.wantDeleteProviderConfigErr)
					return
				}
			}
			_, err := mtGCEClient.CreateInstances(tc.gceRef, "", 0, nil)
			if (err != nil) != tc.wantGCEErr {
				t.Errorf("got: %v, want: %v", err, tc.wantGCEErr)
				return
			}
		})
	}
}

var (
	fooProviderConfig = &multitenancy.ProviderConfig{
		Name:          "t1234-foo",
		ProjectID:     "vertex-tp-1",
		ProjectNumber: 1234,
	}
	bananaProviderConfig = &multitenancy.ProviderConfig{
		Name:          "t5678-banana",
		ProjectID:     "vertex-tp-2",
		ProjectNumber: 5678,
	}
)

func TestMTCreateInstances(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		operation := gce_api.Operation{
			Status: "DONE",
		}
		b, _ := json.Marshal(operation)
		res.WriteHeader(http.StatusOK)
		res.Write(b)
	}))
	defer server.Close()
	mtGCEClient := createDefaultMTGCEClient(t, server.URL)
	addDefaultProviderConfigs(t, mtGCEClient)
	_, err := mtGCEClient.CreateInstances(gce.GceRef{Project: "bad-project"}, "", 0, nil)
	if err == nil {
		t.Error("got: nil, want: error")
		return
	}
	_, err = mtGCEClient.CreateInstances(gce.GceRef{Project: fooProviderConfig.ProjectID}, "", 0, nil)
	if err != nil {
		t.Errorf("got: %v, want: nil", err)
		return
	}
}

func TestMTResumeInstances(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		operation := gce_api.Operation{
			Status: "DONE",
		}
		b, _ := json.Marshal(operation)
		res.WriteHeader(http.StatusOK)
		res.Write(b)
	}))
	defer server.Close()
	mtGCEClient := createDefaultMTGCEClient(t, server.URL)
	addDefaultProviderConfigs(t, mtGCEClient)
	err := mtGCEClient.ResumeInstances(gce.GceRef{Project: "bad-project"}, nil)
	if err == nil {
		t.Error("got: nil, want: error")
		return
	}
	err = mtGCEClient.ResumeInstances(gce.GceRef{Project: fooProviderConfig.ProjectID}, nil)
	if err != nil {
		t.Errorf("got: %v, want: nil", err)
		return
	}
}

func TestMTSuspendInstances(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		operation := gce_api.Operation{
			Status: "DONE",
		}
		b, _ := json.Marshal(operation)
		res.WriteHeader(http.StatusOK)
		res.Write(b)
	}))
	defer server.Close()
	mtGCEClient := createDefaultMTGCEClient(t, server.URL)
	addDefaultProviderConfigs(t, mtGCEClient)
	err := mtGCEClient.SuspendInstances(gce.GceRef{Project: "bad-project"}, nil, false)
	if err == nil {
		t.Error("got: nil, want: error")
		return
	}
	err = mtGCEClient.SuspendInstances(gce.GceRef{Project: fooProviderConfig.ProjectID}, nil, false)
	if err != nil {
		t.Errorf("got: %v, want: nil", err)
		return
	}
}

func TestMTDeleteInstances(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		operation := gce_api.Operation{
			Status: "DONE",
		}
		b, _ := json.Marshal(operation)
		res.WriteHeader(http.StatusOK)
		res.Write(b)
	}))
	defer server.Close()
	mtGCEClient := createDefaultMTGCEClient(t, server.URL)
	addDefaultProviderConfigs(t, mtGCEClient)
	err := mtGCEClient.DeleteInstances(gce.GceRef{Project: "bad-project"}, nil)
	if err == nil {
		t.Error("got: nil, want: error")
		return
	}
	err = mtGCEClient.DeleteInstances(gce.GceRef{Project: fooProviderConfig.ProjectID}, nil)
	if err != nil {
		t.Errorf("got: %v, want: nil", err)
		return
	}
}

func TestMTFetchAcceleratorTypes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		accs := gce_api.AcceleratorTypeList{
			Items: []*gce_api.AcceleratorType{
				{
					Name: "foo",
				},
			},
		}
		b, err := json.Marshal(accs)
		if err != nil {
			t.Log(err)
		}
		res.WriteHeader(http.StatusOK)
		res.Write(b)
	}))
	defer server.Close()
	mtGCEClient := createDefaultMTGCEClientWithClusterProject(t, "bad-project", server.URL)
	addDefaultProviderConfigs(t, mtGCEClient)
	_, err := mtGCEClient.FetchAcceleratorTypes("us-central1-a")
	if err == nil {
		t.Error("got: nil, want: error")
		return
	}
	// override the cluster project ID to a registered ProviderConfig.
	mtGCEClient.clusterProjectID = fooProviderConfig.ProjectID
	accs, err := mtGCEClient.FetchAcceleratorTypes("us-central1-a")
	if err != nil {
		t.Errorf("got: %v, want: nil", err)
		return
	}
	for _, acc := range accs.Items {
		if acc.Name != "foo" {
			t.Errorf("got: %s, want: foo", acc.Name)
			return
		}
	}
}

func TestMTFetchAllInstances(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		instances := gce_api.InstanceList{
			Items: []*gce_api.Instance{
				{
					Name:     "foo",
					SelfLink: fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/us-central1-a/instances/foo", fooProviderConfig.ProjectID),
					Metadata: &gce_api.Metadata{
						Items: []*gce_api.MetadataItems{
							{
								Key:   "created-by",
								Value: ptr.To(fmt.Sprintf("projects/%s/zones/us-central1-a/instanceGroupManagers/foo", fooProviderConfig.ProjectID)),
							},
						},
					},
				},
			},
		}
		b, _ := json.Marshal(instances)
		res.WriteHeader(http.StatusOK)
		res.Write(b)
	}))
	defer server.Close()
	mtGCEClient := createDefaultMTGCEClient(t, server.URL)
	addDefaultProviderConfigs(t, mtGCEClient)
	_, err := mtGCEClient.FetchAllInstances("bad-project", "", "")
	if err == nil {
		t.Error("got: nil, want: error")
		return
	}
	instances, err := mtGCEClient.FetchAllInstances(fooProviderConfig.ProjectID, "", "")
	if err != nil {
		t.Errorf("got: %v, want: nil", err)
		return
	}
	for _, instance := range instances {
		if instance.Igm.Name != "foo" {
			t.Errorf("got: %s, want: foo", instance.Igm.Name)
			return
		}
	}
}

func TestMTFetchAllMigs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		instances := gce_api.InstanceGroupManagerList{
			Items: []*gce_api.InstanceGroupManager{
				{
					Name: "foo",
				},
			},
		}
		b, _ := json.Marshal(instances)
		res.WriteHeader(http.StatusOK)
		res.Write(b)
	}))
	defer server.Close()
	mtGCEClient := createDefaultMTGCEClient(t, server.URL)
	addDefaultProviderConfigs(t, mtGCEClient)
	igms, err := mtGCEClient.FetchAllMigs("us-central1-a")
	if err != nil {
		t.Errorf("got: %v, want: nil", err)
		return
	}
	// Expects calls to 2 clients.
	if len(igms) != 2 {
		t.Errorf("got: %d, want: 2", len(igms))
		return
	}
}

func TestMTFetchAvailableCpuPlatforms(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		zones := gce_api.ZoneList{
			Items: []*gce_api.Zone{
				{
					Name: "foo",
				},
			},
		}
		b, err := json.Marshal(zones)
		if err != nil {
			t.Log(err)
		}
		res.WriteHeader(http.StatusOK)
		res.Write(b)
	}))
	defer server.Close()
	mtGCEClient := createDefaultMTGCEClientWithClusterProject(t, "bad-project", server.URL)
	addDefaultProviderConfigs(t, mtGCEClient)
	_, err := mtGCEClient.FetchAvailableCpuPlatforms()
	if err == nil {
		t.Error("got: nil, want: error")
		return
	}
	// override the cluster project ID to a registered ProviderConfig.
	mtGCEClient.clusterProjectID = fooProviderConfig.ProjectID
	platforms, err := mtGCEClient.FetchAvailableCpuPlatforms()
	if err != nil {
		t.Errorf("got: %v, want: nil", err)
		return
	}
	for platform := range platforms {
		if platform != "foo" {
			t.Errorf("got: %s, want: foo", platform)
			return
		}
	}
}

func TestMTFetchAvailableDiskTypes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		diskTypes := gce_api.DiskTypeList{
			Items: []*gce_api.DiskType{
				{
					Name: "foo",
					Zone: "us-central1-a",
				},
			},
		}
		b, _ := json.Marshal(diskTypes)
		res.WriteHeader(http.StatusOK)
		res.Write(b)
	}))
	defer server.Close()
	mtGCEClient := createDefaultMTGCEClientWithClusterProject(t, "bad-project", server.URL)
	addDefaultProviderConfigs(t, mtGCEClient)
	_, err := mtGCEClient.FetchAvailableDiskTypes("us-central1-a")
	if err == nil {
		t.Error("got: nil, want: error")
		return
	}
	// override the cluster project ID to a registered ProviderConfig.
	mtGCEClient.clusterProjectID = fooProviderConfig.ProjectID
	diskTypes, err := mtGCEClient.FetchAvailableDiskTypes("us-central1-a")
	if err != nil {
		t.Errorf("got: %v, want: nil", err)
		return
	}
	if len(diskTypes) != 1 {
		t.Errorf("got: %d, want: 1", len(diskTypes))
		return
	}
}

func TestMTFetchListManagedInstancesResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		igm := gce_api.InstanceGroupManager{
			Name:                        "foo",
			ListManagedInstancesResults: "PAGINATED",
		}
		b, _ := json.Marshal(igm)
		res.WriteHeader(http.StatusOK)
		res.Write(b)
	}))
	defer server.Close()
	mtGCEClient := createDefaultMTGCEClient(t, server.URL)
	addDefaultProviderConfigs(t, mtGCEClient)
	_, err := mtGCEClient.FetchListManagedInstancesResults(gce.GceRef{Project: "bad-project"})
	if err == nil {
		t.Error("got: nil, want: error")
		return
	}
	res, err := mtGCEClient.FetchListManagedInstancesResults(gce.GceRef{Project: fooProviderConfig.ProjectID})
	if err != nil {
		t.Errorf("got: %v, want: nil", err)
		return
	}
	if res != "PAGINATED" {
		t.Errorf("got: %s, want: PAGINATED", res)
		return
	}
}

func TestMTFetchMachineType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		machineType := gce_api.MachineType{
			Name: "foo",
		}
		b, _ := json.Marshal(machineType)
		res.WriteHeader(http.StatusOK)
		res.Write(b)
	}))
	defer server.Close()
	mtGCEClient := createDefaultMTGCEClientWithClusterProject(t, "bad-project", server.URL)
	addDefaultProviderConfigs(t, mtGCEClient)
	_, err := mtGCEClient.FetchMachineType("us-central1-a", "fast-machine")
	if err == nil {
		t.Error("got: nil, want: error")
		return
	}
	// override the cluster project ID to a registered ProviderConfig.
	mtGCEClient.clusterProjectID = fooProviderConfig.ProjectID
	machineType, err := mtGCEClient.FetchMachineType("us-central1-a", "fast-machine")
	if err != nil {
		t.Errorf("got: %v, want: nil", err)
		return
	}
	if machineType.Name != "foo" {
		t.Errorf("got: %s, want: foo", machineType.Name)
		return
	}
}

func TestMTFetchMachineTypes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		machineTypes := gce_api.MachineTypeList{
			Items: []*gce_api.MachineType{
				{
					Name: "foo",
				},
			},
		}
		b, _ := json.Marshal(machineTypes)
		res.WriteHeader(http.StatusOK)
		res.Write(b)
	}))
	defer server.Close()
	mtGCEClient := createDefaultMTGCEClientWithClusterProject(t, "bad-project", server.URL)
	addDefaultProviderConfigs(t, mtGCEClient)
	_, err := mtGCEClient.FetchMachineTypes("us-central1-a")
	if err == nil {
		t.Error("got: nil, want: error")
		return
	}
	// override the cluster project ID to a registered ProviderConfig.
	mtGCEClient.clusterProjectID = fooProviderConfig.ProjectID
	machineTypes, err := mtGCEClient.FetchMachineTypes("us-central1-a")
	if err != nil {
		t.Errorf("got: %v, want: nil", err)
		return
	}
	if len(machineTypes) != 1 {
		t.Errorf("got: %d, want: 1", len(machineTypes))
		return
	}
	if machineTypes[0].Name != "foo" {
		t.Errorf("got: %s, want: foo", machineTypes[0].Name)
		return
	}
}

func TestMTFetchMigBasename(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		igm := gce_api.InstanceGroupManager{
			BaseInstanceName: fooProviderConfig.ProjectID,
		}
		b, _ := json.Marshal(igm)
		res.WriteHeader(http.StatusOK)
		res.Write(b)
	}))
	defer server.Close()
	mtGCEClient := createDefaultMTGCEClient(t, server.URL)
	addDefaultProviderConfigs(t, mtGCEClient)
	_, err := mtGCEClient.FetchMigBasename(gce.GceRef{Project: "bad-project"})
	if err == nil {
		t.Error("got: nil, want: error")
		return
	}
	res, err := mtGCEClient.FetchMigBasename(gce.GceRef{Project: fooProviderConfig.ProjectID})
	if err != nil {
		t.Errorf("got: %v, want: nil", err)
		return
	}
	if res != fooProviderConfig.ProjectID {
		t.Errorf("got: %s, want: %s", res, fooProviderConfig.ProjectID)
		return
	}
}

func TestMTFetchMigInstances(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		data := gce_api.InstanceGroupManagersListManagedInstancesResponse{
			ManagedInstances: []*gce_api.ManagedInstance{
				{
					Name: "foo",
				},
			},
		}
		b, _ := json.Marshal(data)
		res.WriteHeader(http.StatusOK)
		res.Write(b)
	}))
	defer server.Close()
	mtGCEClient := createDefaultMTGCEClient(t, server.URL)
	addDefaultProviderConfigs(t, mtGCEClient)
	_, err := mtGCEClient.FetchMigInstances(gce.GceRef{Project: "bad-project"})
	if err == nil {
		t.Error("got: nil, want: error")
		return
	}
	_, err = mtGCEClient.FetchMigInstances(gce.GceRef{Project: fooProviderConfig.ProjectID})
	if err != nil {
		t.Errorf("got: %v, want: nil", err)
		return
	}
}

func TestMTFetchMigTargetSize(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		igm := gce_api.InstanceGroupManager{
			TargetSize: 40,
		}
		b, _ := json.Marshal(igm)
		res.WriteHeader(http.StatusOK)
		res.Write(b)
	}))
	defer server.Close()
	mtGCEClient := createDefaultMTGCEClient(t, server.URL)
	addDefaultProviderConfigs(t, mtGCEClient)
	_, err := mtGCEClient.FetchMigTargetSize(gce.GceRef{Project: "bad-project"})
	if err == nil {
		t.Error("got: nil, want: error")
		return
	}
	res, err := mtGCEClient.FetchMigTargetSize(gce.GceRef{Project: fooProviderConfig.ProjectID})
	if err != nil {
		t.Errorf("got: %v, want: nil", err)
		return
	}
	if res != 40 {
		t.Errorf("got: %d, want: 100", res)
		return
	}
}

func TestMTFetchMigTemplate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		igm := gce_api.InstanceTemplate{
			Name: "foo",
		}
		b, _ := json.Marshal(igm)
		res.WriteHeader(http.StatusOK)
		res.Write(b)
	}))
	defer server.Close()
	mtGCEClient := createDefaultMTGCEClient(t, server.URL)
	addDefaultProviderConfigs(t, mtGCEClient)
	_, err := mtGCEClient.FetchMigTemplate(gce.GceRef{Project: "bad-project"}, "", false)
	if err == nil {
		t.Error("got: nil, want: error")
		return
	}
	res, err := mtGCEClient.FetchMigTemplate(gce.GceRef{Project: fooProviderConfig.ProjectID}, "", false)
	if err != nil {
		t.Errorf("got: %v, want: nil", err)
		return
	}
	if res.Name != "foo" {
		t.Errorf("got: %s, want: %s", res.Name, "foo")
		return
	}
}

func TestMTFetchNetwork(t *testing.T) {
	const networkName = "foo-network"
	wantNetwork := &gce_api.Network{
		Name:           networkName,
		SelfLink:       fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/global/networks/%s", fooProviderConfig.ProjectID, networkName),
		SelfLinkWithId: fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/global/networks/12345", fooProviderConfig.ProjectID),
	}
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		b, _ := json.Marshal(wantNetwork)
		res.WriteHeader(http.StatusOK)
		_, err := res.Write(b)
		if err != nil {
			t.Errorf("failed to write response: %v", err)
		}
	}))
	defer server.Close()
	mtGCEClient := createDefaultMTGCEClient(t, server.URL)
	addDefaultProviderConfigs(t, mtGCEClient)
	_, err := mtGCEClient.FetchNetwork("bad-project", "some-network")
	if err == nil {
		t.Error("got: nil, want: error")
		return
	}
	res, err := mtGCEClient.FetchNetwork(fooProviderConfig.ProjectID, networkName)
	if err != nil {
		t.Errorf("got: %v, want: nil", err)
		return
	}
	if diff := cmp.Diff(wantNetwork, res, cmpopts.IgnoreFields(gce_api.Network{}, "ServerResponse")); diff != "" {
		t.Errorf("multitenancyGCEClient.FetchNetwork() diff (-want +got):\n%s", diff)
	}
}

func TestMTOperations(t *testing.T) {
	testCases := []struct {
		name       string
		data       any
		setupFunc  func(client *multitenancyGCEClient)
		methodCall func(client *multitenancyGCEClient) (any, error)
		want       any
		wantErr    bool
	}{
		{
			name: "FetchMigTemplateName failure",
			data: gce_api.InstanceGroupManager{},
			methodCall: func(client *multitenancyGCEClient) (any, error) {
				return client.FetchMigTemplateName(gce.GceRef{Project: "bad-project", Zone: "us-central1-a", Name: "test-mig"})
			},
			wantErr: true,
		},
		{
			name: "FetchMigTemplateName success",
			data: gce_api.InstanceGroupManager{InstanceTemplate: "projects/test-project/us-central1-a/instanceTemplates/test-template"},
			methodCall: func(client *multitenancyGCEClient) (any, error) {
				return client.FetchMigTemplateName(gce.GceRef{Project: fooProviderConfig.ProjectID, Zone: "us-central1-a", Name: "test-mig"})
			},
			want: gce.InstanceTemplateName{
				Name:     "test-template",
				Regional: false,
			},
		},
		{
			name: "FetchReservations failure",
			data: gce_api.ReservationAggregatedList{Items: map[string]gce_api.ReservationsScopedList{
				"foo": {
					Reservations: []*gce_api.Reservation{{Name: "some-reservation"}},
				},
			}},
			setupFunc: func(client *multitenancyGCEClient) { client.clusterProjectID = "bad-project" },
			methodCall: func(client *multitenancyGCEClient) (any, error) {
				return client.FetchReservations()
			},
			wantErr: true,
		},
		{
			name: "FetchReservations success",
			data: gce_api.ReservationAggregatedList{Items: map[string]gce_api.ReservationsScopedList{
				"foo": {
					Reservations: []*gce_api.Reservation{{Name: "some-reservation"}},
				},
			}},
			setupFunc: func(client *multitenancyGCEClient) { client.clusterProjectID = fooProviderConfig.ProjectID },
			methodCall: func(client *multitenancyGCEClient) (any, error) {
				return client.FetchReservations()
			},
			want: []*gce_api.Reservation{{Name: "some-reservation"}},
		},
		{
			name: "FetchReservationsInProject success with unmapped project",
			data: gce_api.ReservationAggregatedList{Items: map[string]gce_api.ReservationsScopedList{
				"foo": {
					Reservations: []*gce_api.Reservation{{Name: fooProviderConfig.ProjectID}},
				},
			}},
			methodCall: func(client *multitenancyGCEClient) (any, error) {
				return client.FetchReservationsInProject("bad-project")
			},
			want:    []*gce_api.Reservation{{Name: fooProviderConfig.ProjectID}},
			wantErr: false,
		},
		{
			name: "FetchReservationsInProject success",
			data: gce_api.ReservationAggregatedList{Items: map[string]gce_api.ReservationsScopedList{
				"foo": {
					Reservations: []*gce_api.Reservation{{Name: fooProviderConfig.ProjectID}},
				},
			}},
			methodCall: func(client *multitenancyGCEClient) (any, error) {
				return client.FetchReservationsInProject(fooProviderConfig.ProjectID)
			},
			want: []*gce_api.Reservation{{Name: fooProviderConfig.ProjectID}},
		},
		{
			name: "FetchReservationBlocksInReservation success with unmapped project",
			data: gce_api.ReservationBlocksListResponse{Items: []*gce_api.ReservationBlock{{Name: "blockName"}}},
			methodCall: func(client *multitenancyGCEClient) (any, error) {
				return client.FetchReservationBlocksInReservation(ReservationRef{Project: "bad-project", Zone: "zone", Name: "rsv"})
			},
			want:    []*GceReservationBlock{{Name: "blockName"}},
			wantErr: false,
		},
		{
			name: "FetchReservationBlocksInReservation success",
			data: gce_api.ReservationBlocksListResponse{Items: []*gce_api.ReservationBlock{{Name: "blockName"}}},
			methodCall: func(client *multitenancyGCEClient) (any, error) {
				return client.FetchReservationBlocksInReservation(ReservationRef{Project: fooProviderConfig.ProjectID, Zone: "zone", Name: "rsv"})
			},
			want: []*GceReservationBlock{{Name: "blockName"}},
		},
		{
			name: "FetchReservationSubBlocksInReservation success with unmapped project",
			data: gce_api.ReservationSubBlocksListResponse{Items: []*gce_api.ReservationSubBlock{{Name: "subBlockName"}}},
			methodCall: func(client *multitenancyGCEClient) (any, error) {
				return client.FetchReservationSubBlocksInReservationBlock(ReservationRef{Project: "bad-project", Zone: "zone", Name: "rsv", BlockName: "blockName"})
			},
			want:    []*GceReservationSubBlock{{Name: "subBlockName"}},
			wantErr: false,
		},
		{
			name: "FetchReservationSubBlocksInReservation success",
			data: gce_api.ReservationSubBlocksListResponse{Items: []*gce_api.ReservationSubBlock{{Name: "subBlockName"}}},
			methodCall: func(client *multitenancyGCEClient) (any, error) {
				return client.FetchReservationSubBlocksInReservationBlock(ReservationRef{Project: fooProviderConfig.ProjectID, Zone: "zone", Name: "rsv", BlockName: "blockName"})
			},
			want: []*GceReservationSubBlock{{Name: "subBlockName"}},
		},
		{
			name: "FetchResourcePolicies failure",
			data: gce_api_beta.ResourcePolicyList{
				Items: []*gce_api_beta.ResourcePolicy{{Name: "test-rp-1"}},
			},
			methodCall: func(client *multitenancyGCEClient) (any, error) {
				return client.FetchResourcePolicies("bad-project", "some-region")
			},
			wantErr: true,
		},
		{
			name: "FetchResourcePolicies success",
			data: gce_api_beta.ResourcePolicyList{Items: []*gce_api_beta.ResourcePolicy{{Name: "test-rp-1"}}},
			methodCall: func(client *multitenancyGCEClient) (any, error) {
				return client.FetchResourcePolicies(fooProviderConfig.ProjectID, "some-region")
			},
			want: []*GceResourcePolicy{{Name: "test-rp-1"}},
		},
		{
			name: "FetchZones failure",
			data: gce_api.Region{Zones: []string{"foo"}},
			setupFunc: func(client *multitenancyGCEClient) {
				client.clusterProjectID = "bad-project"
			},
			methodCall: func(client *multitenancyGCEClient) (any, error) {
				return client.FetchZones("us-central1")
			},
			wantErr: true,
		},
		{
			name: "FetchZones success",
			data: gce_api.Region{Zones: []string{"foo"}},
			setupFunc: func(client *multitenancyGCEClient) {
				client.clusterProjectID = fooProviderConfig.ProjectID
			},
			methodCall: func(client *multitenancyGCEClient) (any, error) {
				return client.FetchZones("us-central1")
			},
			want: []string{"foo"},
		},
		{
			name: "ResizeMig failure",
			data: gce_api.Operation{Status: "DONE"},
			methodCall: func(client *multitenancyGCEClient) (any, error) {
				return nil, client.ResizeMig(gce.GceRef{Project: "bad-project"}, 0)
			},
			wantErr: true,
		},
		{
			name: "ResizeMig success",
			data: gce_api.Operation{Status: "DONE"},
			methodCall: func(client *multitenancyGCEClient) (any, error) {
				return nil, client.ResizeMig(gce.GceRef{Project: fooProviderConfig.ProjectID}, 0)
			},
		},
		{
			name: "WaitForOperation failure",
			data: gce_api.Operation{Status: "DONE"},
			methodCall: func(client *multitenancyGCEClient) (any, error) {
				return nil, client.WaitForOperation("", "", "bad-project", "")
			},
			wantErr: true,
		},
		{
			name: "WaitForOperation success",
			data: gce_api.Operation{Status: "DONE"},
			methodCall: func(client *multitenancyGCEClient) (any, error) {
				return nil, client.WaitForOperation("", "", fooProviderConfig.ProjectID, "")
			},
		},
		{
			name: "FetchAIZones failure",
			data: gce_api.ZoneList{Items: []*gce_api.Zone{{Name: "foo"}}},
			setupFunc: func(client *multitenancyGCEClient) {
				client.clusterProjectID = "bad-project"
			},
			methodCall: func(client *multitenancyGCEClient) (any, error) {
				return client.FetchAIZones("us-central1")
			},
			wantErr: true,
		},
		{
			name: "FetchAIZones success",
			data: gce_api.ZoneList{Items: []*gce_api.Zone{{Name: "foo"}}},
			setupFunc: func(client *multitenancyGCEClient) {
				client.clusterProjectID = fooProviderConfig.ProjectID
			},
			methodCall: func(client *multitenancyGCEClient) (any, error) {
				return client.FetchAIZones("us-central1")
			},
			want: []string{"foo"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := createTestServer(t, tc.data)
			defer server.Close()
			mtGCEClient := createDefaultMTGCEClient(t, server.URL)
			addDefaultProviderConfigs(t, mtGCEClient)
			if tc.setupFunc != nil {
				tc.setupFunc(mtGCEClient)
			}

			got, err := tc.methodCall(mtGCEClient)
			if tc.wantErr && err == nil {
				t.Errorf("got: nil, want: error")
				return
			}
			if !tc.wantErr && err != nil {
				t.Errorf("got: %v, want: nil", err)
				return
			}
			if tc.want != nil && !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got: %#+v, want: %#+v", got, tc.want)
			}
		})
	}
}

func TestMTFetchMigsWithName(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		projectID := strings.Split(req.URL.Path, "/")[2]
		igl := gce_api.InstanceGroupList{
			Items: []*gce_api.InstanceGroup{
				{
					SelfLink: projectID,
				},
			},
		}
		b, _ := json.Marshal(igl)
		res.WriteHeader(http.StatusOK)
		res.Write(b)
	}))
	defer server.Close()
	mtGCEClient := createDefaultMTGCEClient(t, server.URL)
	addDefaultProviderConfigs(t, mtGCEClient)
	res, err := mtGCEClient.FetchMigsWithName("us-central1-a", nil)
	if err != nil {
		t.Errorf("got: %v, want: nil", err)
		return
	}
	for _, name := range res {
		if name != fooProviderConfig.ProjectID && name != bananaProviderConfig.ProjectID {
			t.Errorf("got: %s, want: %s or %s", name, fooProviderConfig.ProjectID, bananaProviderConfig.ProjectID)
			return
		}
	}
}

func createTestServer(t *testing.T, input any) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		data := input
		b, err := json.Marshal(data)
		if err != nil {
			t.Log(err)
		}
		res.WriteHeader(http.StatusOK)
		if _, err := res.Write(b); err != nil {
			t.Errorf("got: %v, want: nil", err)
		}
	}))
}

func createDefaultMTGCEClient(t *testing.T, url string) *multitenancyGCEClient {
	t.Helper()
	mtGCEClient := &multitenancyGCEClient{
		gceClients:              map[string]AutoscalingInternalGceClient{},
		projectToProviderConfig: map[string]sets.Set[string]{},
		experimentsManager:      experiments.NewMockManager(experiments.MultitenancyEnableLazyReservationGCEClientFlag),
		createClientFunc: func(projectID string, pc *multitenancy.ProviderConfig) (AutoscalingInternalGceClient, error) {
			tenantClient := &http.Client{}
			if pc != nil && pc.AuthConfig != nil {
				ts := &testTenantTokenSource{pc.AuthConfig}
				tenantClient = oauth2.NewClient(context.Background(), ts)
			}
			c, err := NewCustomAutoscalingInternalGceClient(tenantClient, &fakeSingleMigInfoProvider{}, projectID, "cluster", url, "user-agent", time.Minute, time.Second, experiments.NewMockManager(), WithInstanceActionPollingFrequency(1*time.Millisecond))
			if err != nil {
				return nil, err
			}
			return c, nil
		},
	}
	return mtGCEClient
}

type testTenantTokenSource struct {
	authConfig *multitenancy.AuthConfig
}

func (ts *testTenantTokenSource) Token() (*oauth2.Token, error) {
	req, err := http.NewRequest("POST", ts.authConfig.TokenURL, strings.NewReader(ts.authConfig.TokenBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var tr struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		TokenType   string `json:"token_type"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return nil, err
	}
	return &oauth2.Token{
		AccessToken: tr.AccessToken,
		TokenType:   tr.TokenType,
		Expiry:      time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second),
	}, nil
}

func createDefaultMTGCEClientWithClusterProject(t *testing.T, projectID string, url string) *multitenancyGCEClient {
	t.Helper()
	client := createDefaultMTGCEClient(t, url)
	client.clusterProjectID = projectID
	return client
}

func TestAddProviderConfigUpdate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		operation := gce_api.Operation{
			Status: "DONE",
		}
		b, _ := json.Marshal(operation)
		res.WriteHeader(http.StatusOK)
		res.Write(b)
	}))
	defer server.Close()

	clientCount := 0
	mtGCEClient := &multitenancyGCEClient{
		gceClients:              map[string]AutoscalingInternalGceClient{},
		projectToProviderConfig: map[string]sets.Set[string]{},
		createClientFunc: func(projectID string, pc *multitenancy.ProviderConfig) (AutoscalingInternalGceClient, error) {
			clientCount++
			return newTestAutoscalingInternalGceClientWithTimeout(t, projectID, server.URL, &fakeSingleMigInfoProvider{}, time.Duration(0)), nil
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
	err := mtGCEClient.AddProviderConfig(pc)
	assert.NoError(t, err)
	assert.Equal(t, 1, clientCount)

	// 2. Add again with same config - should NOT recreate client
	err = mtGCEClient.AddProviderConfig(pc)
	assert.NoError(t, err)
	assert.Equal(t, 1, clientCount)

	// 3. Update with different AuthConfig - should NOT recreate client anymore after simplification
	pcUpdated := &multitenancy.ProviderConfig{
		Name:      "tenant-1",
		ProjectID: "project-1",
		AuthConfig: &multitenancy.AuthConfig{
			TokenURL: "url-2",
		},
	}
	err = mtGCEClient.AddProviderConfig(pcUpdated)
	assert.NoError(t, err)
	assert.Equal(t, 1, clientCount)
}

func addDefaultProviderConfigs(t *testing.T, mtGCEClient *multitenancyGCEClient) {
	t.Helper()
	providerConfigsToAdd := []*multitenancy.ProviderConfig{
		fooProviderConfig,
		bananaProviderConfig,
	}
	for _, providerConfig := range providerConfigsToAdd {
		err := mtGCEClient.AddProviderConfig(providerConfig)
		if err != nil {
			t.Errorf("got: %v, want: nil", err)
			return
		}
	}
}

func TestMTPerTenantAuth(t *testing.T) {
	const tenantToken = "secret-tenant-token"

	// 1. Mock TokenURL
	tokenServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		if req.Method != "POST" {
			t.Errorf("expected POST request, got %s", req.Method)
		}
		res.WriteHeader(http.StatusOK)
		fmt.Fprintf(res, `{"access_token": "%s", "expires_in": 3600, "token_type": "Bearer"}`, tenantToken)
	}))
	defer tokenServer.Close()

	// 2. Mock GCE API
	gceServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		authHeader := req.Header.Get("Authorization")
		expectedAuth := "Bearer " + tenantToken
		if authHeader != expectedAuth {
			t.Errorf("expected Authorization header %q, got %q", expectedAuth, authHeader)
			res.WriteHeader(http.StatusUnauthorized)
			return
		}

		operation := gce_api.Operation{
			Status: "DONE",
		}
		b, _ := json.Marshal(operation)
		res.WriteHeader(http.StatusOK)
		res.Write(b)
	}))
	defer gceServer.Close()

	// 3. Setup MultitenancyGCEClient with per-tenant creation func
	mtGCEClient := &multitenancyGCEClient{
		gceClients:              map[string]AutoscalingInternalGceClient{},
		projectToProviderConfig: map[string]sets.Set[string]{},
		createClientFunc: func(projectID string, pc *multitenancy.ProviderConfig) (AutoscalingInternalGceClient, error) {
			tenantClient := &http.Client{}
			if pc != nil && pc.AuthConfig != nil {
				ts := &testTenantTokenSource{pc.AuthConfig}
				tenantClient = oauth2.NewClient(context.Background(), ts)
			}
			return NewCustomAutoscalingInternalGceClient(tenantClient, &fakeSingleMigInfoProvider{}, projectID, "cluster", gceServer.URL, "user-agent", time.Minute, time.Second, nil)
		},
	}

	providerConfig := &multitenancy.ProviderConfig{
		Name:          "tenant-1",
		ProjectID:     "tenant-project",
		ProjectNumber: 1234,
		AuthConfig: &multitenancy.AuthConfig{
			TokenURL:  tokenServer.URL,
			TokenBody: `{"grant_type": "client_credentials"}`,
		},
	}

	err := mtGCEClient.AddProviderConfig(providerConfig)
	assert.NoError(t, err)

	// 4. Trigger client creation and API call
	_, err = mtGCEClient.CreateInstances(gce.GceRef{Project: "tenant-project", Zone: "us-central1-a", Name: "mig"}, "", 0, nil)
	assert.NoError(t, err)
}

func TestMTPerTenantAuthFallback(t *testing.T) {
	// 1. Mock GCE API
	gceServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		authHeader := req.Header.Get("Authorization")
		if authHeader != "" {
			t.Errorf("expected no Authorization header (using default auth), got %q", authHeader)
		}

		operation := gce_api.Operation{
			Status: "DONE",
		}
		b, _ := json.Marshal(operation)
		res.WriteHeader(http.StatusOK)
		res.Write(b)
	}))
	defer gceServer.Close()

	// 2. Setup MultitenancyGCEClient with per-tenant creation func
	mtGCEClient := &multitenancyGCEClient{
		gceClients:              map[string]AutoscalingInternalGceClient{},
		projectToProviderConfig: map[string]sets.Set[string]{},
		createClientFunc: func(projectID string, pc *multitenancy.ProviderConfig) (AutoscalingInternalGceClient, error) {
			tenantClient := &http.Client{}
			if pc != nil && pc.AuthConfig != nil {
				ts := &testTenantTokenSource{pc.AuthConfig}
				tenantClient = oauth2.NewClient(context.Background(), ts)
			}
			return NewCustomAutoscalingInternalGceClient(tenantClient, &fakeSingleMigInfoProvider{}, projectID, "cluster", gceServer.URL, "user-agent", time.Minute, time.Second, nil)
		},
	}

	providerConfig := &multitenancy.ProviderConfig{
		Name:          "tenant-1",
		ProjectID:     "tenant-project",
		ProjectNumber: 1234,
		AuthConfig:    nil, // No AuthConfig
	}

	err := mtGCEClient.AddProviderConfig(providerConfig)
	assert.NoError(t, err)

	// 3. Trigger client creation and API call
	_, err = mtGCEClient.CreateInstances(gce.GceRef{Project: "tenant-project", Zone: "us-central1-a", Name: "mig"}, "", 0, nil)
	assert.NoError(t, err)
}
