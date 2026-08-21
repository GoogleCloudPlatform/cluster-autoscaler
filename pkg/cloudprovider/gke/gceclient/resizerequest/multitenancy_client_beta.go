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
	"sync"

	"golang.org/x/oauth2"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/multitenancy"
	klog "k8s.io/klog/v2"
)

type multitenancyResizeRequestClientBeta struct {
	// resizeRequestClients contains the mapping from projectID to resizeRequestClient client.
	// MT tenants are 1:1 with ProviderConfig but many ProviderConfigs
	// can map to the same project. For the purposes of the resizeRequestClient
	// we only care about the project.
	resizeRequestClients map[string]ResizeRequestClient
	// projectToProviderConfig holds all the ProviderConfigs for a project.
	// We track this to handle proper addition/deletion as we only want to
	// create a client for the first ProviderConfig in a project, and delete
	// for the last ProviderConfig.
	projectToProviderConfig map[string]sets.Set[string]
	// clusterProjectID is the projectID where the cluster resource lives. The supervisor workloads
	// also run in this project.
	clusterProjectID     string
	clusterProjectNumber int64
	// createClientFunc creates a ResizeRequestClient for a given project.
	createClientFunc func(projectID string, pc *multitenancy.ProviderConfig) (ResizeRequestClient, error)
	// mutex is used for any mutations to gceClients or projectToProviderConfig.
	mutex sync.Mutex
}

type MultitenancyResizeRequestClient interface {
	ResizeRequestClient
	multitenancy.ProviderConfigEventHandler
}

var _ MultitenancyResizeRequestClient = &multitenancyResizeRequestClientBeta{}

// NewMultitenancyResizeRequestClientBeta wraps multiple ResizeRequestClient in GKE MT clusters
// and handles dispatch for ResizeRequest GCE API calls across multiple projects.
func NewMultitenancyResizeRequestClientBeta(client *http.Client, clusterProjectID string, clusterProjectNumber int64, userAgent, gceEndpoint string, resizeRequestMode ResizeRequestMode, experimentsManager experiments.Manager) (MultitenancyResizeRequestClient, error) {
	clusterProjectClient, err := NewResizeRequestClientBeta(client, clusterProjectID, userAgent, gceEndpoint, resizeRequestMode)
	if err != nil {
		return nil, err
	}
	return &multitenancyResizeRequestClientBeta{
		resizeRequestClients: map[string]ResizeRequestClient{
			clusterProjectID: clusterProjectClient,
		},
		projectToProviderConfig: map[string]sets.Set[string]{},
		clusterProjectID:        clusterProjectID,
		clusterProjectNumber:    clusterProjectNumber,
		createClientFunc: func(tenantProjectID string, pc *multitenancy.ProviderConfig) (ResizeRequestClient, error) {
			if experimentsManager != nil && experimentsManager.DirectLaunchBoolFlag(experiments.MultitenancyEnablePerTenantP4SAFlag) {
				tenantClient := client
				if pc != nil && pc.AuthConfig != nil {
					ts := multitenancy.NewTenantTokenSource(pc.AuthConfig, clusterProjectNumber, pc.ProjectNumber)
					tenantClient = oauth2.NewClient(context.Background(), ts)
					tenantClient.Timeout = client.Timeout
				}
				return NewResizeRequestClientBeta(tenantClient, tenantProjectID, userAgent, gceEndpoint, resizeRequestMode)
			}
			return NewResizeRequestClientBeta(client, tenantProjectID, userAgent, gceEndpoint, resizeRequestMode)
		},
		mutex: sync.Mutex{},
	}, nil
}

func (m *multitenancyResizeRequestClientBeta) AddProviderConfig(providerConfig *multitenancy.ProviderConfig) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	tenantProjectID := providerConfig.ProjectID

	if _, exists := m.projectToProviderConfig[tenantProjectID]; !exists {
		m.projectToProviderConfig[tenantProjectID] = sets.Set[string]{}
	}
	m.projectToProviderConfig[tenantProjectID].Insert(providerConfig.Name)

	if _, clientExists := m.resizeRequestClients[tenantProjectID]; clientExists {
		klog.Infof("Reusing existing ResizeRequest client for project %s (tenant %s)", tenantProjectID, providerConfig.Name)
		return nil
	}

	klog.Infof("Creating new ResizeRequest client for project %s (tenant %s)", tenantProjectID, providerConfig.Name)
	resizeRequestClient, err := m.createClientFunc(tenantProjectID, providerConfig)
	if err != nil {
		return err
	}
	m.resizeRequestClients[tenantProjectID] = resizeRequestClient
	return nil
}

// DeleteProviderConfig unregisters a ProviderConfig against the MT ResizeRequest client.
func (m *multitenancyResizeRequestClientBeta) DeleteProviderConfig(providerConfig *multitenancy.ProviderConfig) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	tenantProjectID := providerConfig.ProjectID
	if !m.projectToProviderConfig[tenantProjectID].Has(providerConfig.Name) {
		return fmt.Errorf("unable to delete ProviderConfig %s as its not registered", providerConfig.Name)
	}
	m.projectToProviderConfig[tenantProjectID].Delete(providerConfig.Name)
	if m.projectToProviderConfig[tenantProjectID].Len() == 0 {
		delete(m.resizeRequestClients, tenantProjectID)
	}
	return nil
}

func (m *multitenancyResizeRequestClientBeta) CreateResizeRequest(ctx context.Context, migRef gce.GceRef, createRequest ResizeRequestCreateRequest) error {
	client, err := m.resizeRequestClient(migRef.Project)
	if err != nil {
		return err
	}
	return client.CreateResizeRequest(ctx, migRef, createRequest)
}

func (m *multitenancyResizeRequestClientBeta) AdvanceResizeRequestCleanUp(ctx context.Context, resizeRequest ResizeRequestStatus) error {
	client, err := m.resizeRequestClient(resizeRequest.ProjectID)
	if err != nil {
		return err
	}
	return client.AdvanceResizeRequestCleanUp(ctx, resizeRequest)
}

func (m *multitenancyResizeRequestClientBeta) ResizeRequest(ctx context.Context, migRef gce.GceRef, resizeRequestName string) (ResizeRequestStatus, error) {
	client, err := m.resizeRequestClient(migRef.Project)
	if err != nil {
		return ResizeRequestStatus{}, err
	}
	return client.ResizeRequest(ctx, migRef, resizeRequestName)
}

func (m *multitenancyResizeRequestClientBeta) ResizeRequests(ctx context.Context, migRef gce.GceRef) ([]ResizeRequestStatus, error) {
	client, err := m.resizeRequestClient(migRef.Project)
	if err != nil {
		return []ResizeRequestStatus{}, err
	}
	return client.ResizeRequests(ctx, migRef)
}

func (m *multitenancyResizeRequestClientBeta) ReportState(rr ResizeRequestStatus) ResizeRequestReportState {
	client, err := m.resizeRequestClient(rr.ProjectID)
	if err != nil {
		klog.Warningf("Couldn't get client for project %q: %v", rr.ProjectID, err)
		return UnspecifiedReportState
	}
	return client.ReportState(rr)
}

func (m *multitenancyResizeRequestClientBeta) SetReportState(rr ResizeRequestStatus, state ResizeRequestReportState) {
	client, err := m.resizeRequestClient(rr.ProjectID)
	if err != nil {
		klog.Warningf("Couldn't get client for project %q: %v", rr.ProjectID, err)
		return
	}
	client.SetReportState(rr, state)
}

func (m *multitenancyResizeRequestClientBeta) RegisterFailedResizeRequestsCreation(migRef gce.GceRef, err error, count int) {
	client, err := m.resizeRequestClient(migRef.Project)
	if err != nil {
		klog.Warningf("Couldn't get client for MIG %+v: %v", migRef, err)
		return
	}
	client.RegisterFailedResizeRequestsCreation(migRef, err, count)
}

func (m *multitenancyResizeRequestClientBeta) ResetFailedResizeRequestsCreation(migRef gce.GceRef) map[error]int {
	client, err := m.resizeRequestClient(migRef.Project)
	if err != nil {
		klog.Warningf("Couldn't get client for MIG %+v: %v", migRef, err)
		return nil
	}
	return client.ResetFailedResizeRequestsCreation(migRef)
}

func (m *multitenancyResizeRequestClientBeta) resizeRequestClient(projectID string) (ResizeRequestClient, error) {
	client, exists := m.resizeRequestClients[projectID]
	if !exists {
		return nil, fmt.Errorf("resize request client for projectID %s does not exist", projectID)
	}
	return client, nil
}
