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

package api

import (
	"net/http"

	gkeapibeta "google.golang.org/api/container/v1beta1"
)

// Client is an interface that abstracts the raw GKE API calls.
type Client interface {
	GetCluster(clusterPath string) (*gkeapibeta.Cluster, error)
	CreateNodePool(clusterPath string, req *gkeapibeta.CreateNodePoolRequest) (*gkeapibeta.Operation, error)
	DeleteNodePool(nodePoolPath string) (*gkeapibeta.Operation, error)
	UpdateNodePoolLabels(nodePoolPath string, req *gkeapibeta.UpdateNodePoolRequest) (*gkeapibeta.Operation, error)
	GetOperation(operationPath string) (*gkeapibeta.Operation, error)
}

type clientImpl struct {
	betaService *gkeapibeta.Service
}

// NewClient creates the wrapper around the Google client.
func NewClient(client *http.Client, userAgent, endpoint string) (Client, error) {
	betaService, err := gkeapibeta.New(client)
	if err != nil {
		return nil, err
	}
	betaService.UserAgent = userAgent
	if endpoint != "" {
		betaService.BasePath = endpoint
	}
	return &clientImpl{betaService: betaService}, nil
}

// GetCluster implements the Client interface.
func (a *clientImpl) GetCluster(clusterPath string) (*gkeapibeta.Cluster, error) {
	return a.betaService.Projects.Locations.Clusters.Get(clusterPath).Do()
}

// CreateNodePool implements the Client interface.
func (a *clientImpl) CreateNodePool(clusterPath string, req *gkeapibeta.CreateNodePoolRequest) (*gkeapibeta.Operation, error) {
	return a.betaService.Projects.Locations.Clusters.NodePools.Create(clusterPath, req).Do()
}

// DeleteNodePool implements the Client interface.
func (a *clientImpl) DeleteNodePool(nodePoolPath string) (*gkeapibeta.Operation, error) {
	return a.betaService.Projects.Locations.Clusters.NodePools.Delete(nodePoolPath).Do()
}

// UpdateNodePoolLabels implements the Client interface.
func (a *clientImpl) UpdateNodePoolLabels(nodePoolPath string, req *gkeapibeta.UpdateNodePoolRequest) (*gkeapibeta.Operation, error) {
	return a.betaService.Projects.Locations.Clusters.NodePools.Update(nodePoolPath, req).Do()
}

// GetOperation implements the Client interface.
func (a *clientImpl) GetOperation(operationPath string) (*gkeapibeta.Operation, error) {
	return a.betaService.Projects.Locations.Operations.Get(operationPath).Do()
}
