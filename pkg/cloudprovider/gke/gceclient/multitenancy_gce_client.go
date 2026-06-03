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
	"fmt"
	"net/http"
	"regexp"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"k8s.io/klog/v2"

	gce_api "google.golang.org/api/compute/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/multitenancy"
)

type MultitenancyGCEClient interface {
	AutoscalingInternalGceClient
	multitenancy.ProviderConfigEventHandler
}

type multitenancyGCEClient struct {
	// gceClients contains the mapping from projectID to the GCE client.
	// MT tenants are 1:1 with ProviderConfig but many ProviderConfigs
	// can map to the same project. For the purposes of the GCE client
	// we only care about the project.
	gceClients map[string]AutoscalingInternalGceClient
	// projectToProviderConfig holds all the ProviderConfigs for a project.
	// We track this to handle proper addition/deletion as we only want to
	// create a client for the first ProviderConfig in a project, and delete
	// for the last ProviderConfig.
	projectToProviderConfig map[string]sets.Set[string]
	// clusterProjectID is the projectID where the cluster resource lives. The supervisor workloads
	// also run in this project.
	clusterProjectID     string
	clusterProjectNumber int64
	experimentsManager   experiments.Manager
	// createClientFunc creates a AutoscalingInternalGceClient for a given project.
	createClientFunc func(projectID string, pc *multitenancy.ProviderConfig) (AutoscalingInternalGceClient, error)
	// mutex is used for any mutations to gceClients or projectToProviderConfig.
	mutex sync.Mutex
}

var _ MultitenancyGCEClient = &multitenancyGCEClient{}

// NewMultitenancyGCEClient wraps multiple AutoscalingInternalGceClient in GKE MT clusters
// and handles dispatch for GCE API calls across multiple projects.
//
// NOTE: The MT client uses the cluster project tokenSource for all GCE API calls
// which means the cluster GKE P4SA needs to have permissions on all tenant projects.
// This will change when MT specific Auth server changes have been made.
func NewMultitenancyGCEClient(client *http.Client, migInfoProvider MigInfoProvider, clusterProjectID string, clusterProjectNumber int64, clusterName string, userAgent string, waitTimeout, pollInterval time.Duration, experimentsManager experiments.Manager) (*multitenancyGCEClient, error) {
	// Create cluster project client as the default client otherwise during cluster startup
	// nodes won't be created since ProviderConfig for cluster project does not exist.
	clusterProjectClient, err := NewAutoscalingInternalGceClient(client, migInfoProvider, clusterProjectID, clusterName, userAgent, waitTimeout, pollInterval, experimentsManager)
	if err != nil {
		return nil, fmt.Errorf("unable to create cluster project client: %v", err)
	}
	gceClientsMap := map[string]AutoscalingInternalGceClient{
		clusterProjectID: clusterProjectClient,
	}
	return &multitenancyGCEClient{
		clusterProjectID:        clusterProjectID,
		clusterProjectNumber:    clusterProjectNumber,
		gceClients:              gceClientsMap,
		experimentsManager:      experimentsManager,
		projectToProviderConfig: map[string]sets.Set[string]{},
		mutex:                   sync.Mutex{},
		createClientFunc: func(tenantProjectID string, pc *multitenancy.ProviderConfig) (AutoscalingInternalGceClient, error) {
			if experimentsManager != nil && experimentsManager.DirectLaunchBoolFlag(experiments.MultitenancyEnablePerTenantP4SAFlag) {
				return NewPerTenantAutoscalingInternalGceClient(client, migInfoProvider, tenantProjectID, clusterProjectNumber, clusterName, userAgent, waitTimeout, pollInterval, experimentsManager, pc)
			}
			return NewAutoscalingInternalGceClient(client, migInfoProvider, tenantProjectID, clusterName, userAgent, waitTimeout, pollInterval, experimentsManager)
		},
	}, nil
}

func NewPerTenantAutoscalingInternalGceClient(client *http.Client, migInfoProvider MigInfoProvider, projectID string, clusterProjectNumber int64, clusterName string, userAgent string, waitTimeout, pollInterval time.Duration, experimentsManager experiments.Manager, providerConfig *multitenancy.ProviderConfig) (AutoscalingInternalGceClient, error) {
	timeout := client.Timeout
	if providerConfig != nil {
		klog.Infof("NewPerTenantAutoscalingInternalGceClient called for providerConfig: %s", providerConfig.Name)
		if providerConfig.AuthConfig != nil {
			klog.Infof("NewPerTenantAutoscalingInternalGceClient using AuthConfig TokenURL: %s", providerConfig.AuthConfig.TokenURL)
			ts := multitenancy.NewTenantTokenSource(providerConfig.AuthConfig, clusterProjectNumber, providerConfig.ProjectNumber)
			client = oauth2.NewClient(context.Background(), ts)
			client.Timeout = timeout
		} else {
			klog.Warningf("NewPerTenantAutoscalingInternalGceClient: AuthConfig is nil for %s, falling back to default client", providerConfig.Name)
		}
	} else {
		klog.Warningf("NewPerTenantAutoscalingInternalGceClient: providerConfig is nil")
	}

	return NewAutoscalingInternalGceClient(client, migInfoProvider, projectID, clusterName, userAgent, waitTimeout, pollInterval, experimentsManager)
}

// AddProviderConfig should be called when a new ProviderConfig is created.
func (m *multitenancyGCEClient) AddProviderConfig(providerConfig *multitenancy.ProviderConfig) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	tenantProjectID := providerConfig.ProjectID

	if _, exists := m.projectToProviderConfig[tenantProjectID]; !exists {
		m.projectToProviderConfig[tenantProjectID] = sets.Set[string]{}
	}
	m.projectToProviderConfig[tenantProjectID].Insert(providerConfig.Name)

	if _, clientExists := m.gceClients[tenantProjectID]; clientExists {
		klog.Infof("Reusing existing GCE client for project %s (tenant %s)", tenantProjectID, providerConfig.Name)
		return nil
	}

	klog.Infof("Creating new GCE client for project %s (tenant %s)", tenantProjectID, providerConfig.Name)
	gceService, err := m.createClientFunc(tenantProjectID, providerConfig)
	if err != nil {
		return err
	}
	m.gceClients[tenantProjectID] = gceService
	return nil
}

// DeleteProviderConfig removes any ProviderConfig state for the GCE client.
// This **MUST** only be called when all cleanup operations are complete for the ProviderConfig.
func (m *multitenancyGCEClient) DeleteProviderConfig(providerConfig *multitenancy.ProviderConfig) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	tenantProjectID := providerConfig.ProjectID
	if !m.projectToProviderConfig[tenantProjectID].Has(providerConfig.Name) {
		return fmt.Errorf("unable to delete ProviderConfig %s as its not registered", providerConfig.Name)
	}
	m.projectToProviderConfig[tenantProjectID].Delete(providerConfig.Name)
	if m.projectToProviderConfig[tenantProjectID].Len() == 0 {
		delete(m.gceClients, tenantProjectID)
	}
	return nil
}

// TODO(b/385776259): Evaluate aggregating information across all tenant projects.
func (m *multitenancyGCEClient) FetchAcceleratorTypes(zone string) (*gce_api.AcceleratorTypeList, error) {
	clusterProjectGceService, err := m.clusterProjectGCEService()
	if err != nil {
		return nil, err
	}
	return clusterProjectGceService.FetchAcceleratorTypes(zone)
}

func (m *multitenancyGCEClient) FetchMachineType(zone, machineType string) (*gce_api.MachineType, error) {
	clusterProjectGceService, err := m.clusterProjectGCEService()
	if err != nil {
		return nil, err
	}
	return clusterProjectGceService.FetchMachineType(zone, machineType)
}

func (m *multitenancyGCEClient) FetchMachineTypes(zone string) ([]*gce_api.MachineType, error) {
	clusterProjectGceService, err := m.clusterProjectGCEService()
	if err != nil {
		return nil, err
	}
	return clusterProjectGceService.FetchMachineTypes(zone)
}

// FetchAllMigs returns all MIGs across all known projects in a MT cluster.
// This function fails-fast if any of the GCE API calls fail, this is to
// protect against incomplete state issues. We may relax this behaviour in
// the future as needed.
//
// TODO(b/385776258): Parallelize the GCE API calls across projects.
func (m *multitenancyGCEClient) FetchAllMigs(zone string) ([]*gce_api.InstanceGroupManager, error) {
	var allMigs []*gce_api.InstanceGroupManager
	for projectID, gceService := range m.gceClients {
		migs, err := gceService.FetchAllMigs(zone)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch all MIGs for project %s: %v", projectID, err)
		}
		allMigs = append(allMigs, migs...)
	}
	return allMigs, nil
}

// TODO(b/385784728): Update call stack to fetch for all projects or create new function in client for it.
func (m *multitenancyGCEClient) FetchAllInstances(project, zone string, filter string) ([]gce.GceInstance, error) {
	gceService, err := m.gceService(project)
	if err != nil {
		return nil, err
	}
	return gceService.FetchAllInstances(project, zone, filter)
}

func (m *multitenancyGCEClient) FetchMigTargetSize(ref gce.GceRef) (int64, error) {
	gceService, err := m.gceService(ref.Project)
	if err != nil {
		return 0, err
	}
	return gceService.FetchMigTargetSize(ref)
}

func (m *multitenancyGCEClient) FetchMigBasename(ref gce.GceRef) (string, error) {
	gceService, err := m.gceService(ref.Project)
	if err != nil {
		return "", err
	}
	return gceService.FetchMigBasename(ref)
}

func (m *multitenancyGCEClient) FetchMigInstances(ref gce.GceRef) ([]gce.GceInstance, error) {
	gceService, err := m.gceService(ref.Project)
	if err != nil {
		return nil, err
	}
	return gceService.FetchMigInstances(ref)
}

func (m *multitenancyGCEClient) FetchMig(ref gce.GceRef) (*gce_api.InstanceGroupManager, error) {
	gceService, err := m.gceService(ref.Project)
	if err != nil {
		return nil, err
	}
	return gceService.FetchMig(ref)
}

func (m *multitenancyGCEClient) FetchMigTemplateName(ref gce.GceRef) (gce.InstanceTemplateName, error) {
	gceService, err := m.gceService(ref.Project)
	if err != nil {
		return gce.InstanceTemplateName{}, err
	}
	return gceService.FetchMigTemplateName(ref)
}

func (m *multitenancyGCEClient) FetchMigTemplate(ref gce.GceRef, templateName string, regional bool) (*gce_api.InstanceTemplate, error) {
	gceService, err := m.gceService(ref.Project)
	if err != nil {
		return nil, err
	}
	return gceService.FetchMigTemplate(ref, templateName, regional)
}

// TODO(b/385776258): Parallelize the GCE API calls across projects.
func (m *multitenancyGCEClient) FetchMigsWithName(zone string, filter *regexp.Regexp) ([]string, error) {
	var foundMigs []string
	for projectID, gceService := range m.gceClients {
		migs, err := gceService.FetchMigsWithName(zone, filter)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch all MIGs with filter for project %s: %v", projectID, err)
		}
		foundMigs = append(foundMigs, migs...)
	}
	return foundMigs, nil
}

func (m *multitenancyGCEClient) FetchZones(region string) ([]string, error) {
	clusterProjectGceService, err := m.clusterProjectGCEService()
	if err != nil {
		return nil, err
	}
	return clusterProjectGceService.FetchZones(region)
}

// TODO(b/385776259): Evaluate aggregating information across all tenant projects.
func (m *multitenancyGCEClient) FetchAvailableCpuPlatforms() (map[string][]string, error) {
	clusterProjectGceService, err := m.clusterProjectGCEService()
	if err != nil {
		return nil, err
	}
	return clusterProjectGceService.FetchAvailableCpuPlatforms()
}

// TODO(b/385776259): Evaluate aggregating information across all tenant projects.
func (m *multitenancyGCEClient) FetchAvailableDiskTypes(zone string) ([]string, error) {
	clusterProjectGceService, err := m.clusterProjectGCEService()
	if err != nil {
		return nil, err
	}
	return clusterProjectGceService.FetchAvailableDiskTypes(zone)
}

func (m *multitenancyGCEClient) FetchReservations() ([]*gce_api.Reservation, error) {
	clusterProjectGceService, err := m.clusterProjectGCEService()
	if err != nil {
		return nil, err
	}
	return clusterProjectGceService.FetchReservations()
}

func (m *multitenancyGCEClient) FetchReservationsInProject(projectId string) ([]*gce_api.Reservation, error) {
	gceService, err := m.reservationGceService(projectId)
	if err != nil {
		return nil, err
	}
	return gceService.FetchReservationsInProject(projectId)
}

func (m *multitenancyGCEClient) FetchReservationBlocksInReservation(reservationRef ReservationRef) ([]*GceReservationBlock, error) {
	gceService, err := m.reservationGceService(reservationRef.Project)
	if err != nil {
		return nil, err
	}
	return gceService.FetchReservationBlocksInReservation(reservationRef)
}

func (m *multitenancyGCEClient) FetchReservationSubBlocksInReservationBlock(reservationRef ReservationRef) ([]*GceReservationSubBlock, error) {
	gceService, err := m.reservationGceService(reservationRef.Project)
	if err != nil {
		return nil, err
	}
	return gceService.FetchReservationSubBlocksInReservationBlock(reservationRef)
}

func (m *multitenancyGCEClient) FetchFutureReservationsInProject(projectID string) ([]*GceFutureReservation, error) {
	gceService, err := m.reservationGceService(projectID)
	if err != nil {
		return nil, err
	}
	return gceService.FetchFutureReservationsInProject(projectID)
}

func (m *multitenancyGCEClient) FetchResourcePolicies(projectId, region string) ([]*GceResourcePolicy, error) {
	gceService, err := m.gceService(projectId)
	if err != nil {
		return nil, err
	}
	return gceService.FetchResourcePolicies(projectId, region)
}

func (m *multitenancyGCEClient) FetchListManagedInstancesResults(ref gce.GceRef) (string, error) {
	gceService, err := m.gceService(ref.Project)
	if err != nil {
		return "", err
	}
	return gceService.FetchListManagedInstancesResults(ref)
}

func (m *multitenancyGCEClient) ResizeMig(ref gce.GceRef, size int64) error {
	gceService, err := m.gceService(ref.Project)
	if err != nil {
		return err
	}
	return gceService.ResizeMig(ref, size)
}

func (m *multitenancyGCEClient) DeleteInstances(ref gce.GceRef, instances []gce.GceRef) error {
	gceService, err := m.gceService(ref.Project)
	if err != nil {
		return err
	}
	return gceService.DeleteInstances(ref, instances)
}

func (m *multitenancyGCEClient) CreateInstances(ref gce.GceRef, basename string, delta int64, instanceNames []string) ([]string, error) {
	gceService, err := m.gceService(ref.Project)
	if err != nil {
		return nil, err
	}
	return gceService.CreateInstances(ref, basename, delta, instanceNames)
}

func (m *multitenancyGCEClient) ResumeInstances(migRef gce.GceRef, instances []gce.GceRef) error {
	gceClient, err := m.gceService(migRef.Project)
	if err != nil {
		return err
	}
	return gceClient.ResumeInstances(migRef, instances)
}

func (m *multitenancyGCEClient) SuspendInstances(migRef gce.GceRef, instances []gce.GceRef, forceSuspend bool) error {
	gceClient, err := m.gceService(migRef.Project)
	if err != nil {
		return err
	}
	return gceClient.SuspendInstances(migRef, instances, forceSuspend)
}

func (m *multitenancyGCEClient) WaitForOperation(operationName, operationType, project, zone string) error {
	gceService, err := m.gceService(project)
	if err != nil {
		return err
	}
	return gceService.WaitForOperation(operationName, operationType, project, zone)
}

func (m *multitenancyGCEClient) FetchNetwork(projectId, name string) (*gce_api.Network, error) {
	gceClient, err := m.gceService(projectId)
	if err != nil {
		return nil, err
	}
	return gceClient.FetchNetwork(projectId, name)
}

func (m *multitenancyGCEClient) gceService(projectID string) (AutoscalingInternalGceClient, error) {
	gceService, exists := m.gceClients[projectID]
	if !exists {
		return nil, fmt.Errorf("Unable to find GCE client for projectID: %s", projectID)
	}
	return gceService, nil
}

func (m *multitenancyGCEClient) reservationGceService(projectID string) (AutoscalingInternalGceClient, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	gceService, exists := m.gceClients[projectID]
	if !exists {
		if m.experimentsManager != nil && m.experimentsManager.DirectLaunchBoolFlag(experiments.MultitenancyEnableLazyReservationGCEClientFlag) {
			klog.Infof("Lazily creating GCE client for reservation operations in project %s", projectID)
			service, err := m.createClientFunc(projectID, nil)
			if err != nil {
				return nil, fmt.Errorf("Unable to lazily create GCE client for reservation projectID %s: %v", projectID, err)
			}
			m.gceClients[projectID] = service
			return service, nil
		}
		return nil, fmt.Errorf("Unable to find GCE client for projectID: %s", projectID)
	}
	return gceService, nil
}

// clusterProjectGCEService returns the GCE service for the GKE cluster project.
// This is useful for making certain types of calls where only a single project suffices and dispatch isn't required.
func (m *multitenancyGCEClient) clusterProjectGCEService() (AutoscalingInternalGceClient, error) {
	gceService, exists := m.gceClients[m.clusterProjectID]
	if !exists {
		return nil, fmt.Errorf("Unable to find GCE client for m.clusterProjectID: %s", m.clusterProjectID)
	}
	return gceService, nil
}

func (m *multitenancyGCEClient) GetHttpTimeout() time.Duration {
	return m.gceClients[m.clusterProjectID].GetHttpTimeout()
}

func (m *multitenancyGCEClient) FetchStandardZones(region string) ([]string, error) {
	gceService, err := m.clusterProjectGCEService()
	if err != nil {
		return nil, err
	}
	return gceService.FetchStandardZones(region)
}

func (m *multitenancyGCEClient) FetchAIZones(region string) ([]string, error) {
	gceService, err := m.clusterProjectGCEService()
	if err != nil {
		return nil, err
	}
	return gceService.FetchAIZones(region)
}
