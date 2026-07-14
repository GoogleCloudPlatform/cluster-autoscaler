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

package processors

import (
	"context"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

// ProcessorsCloudProvider is cloud provider interface extended for GKE processors use cases.
type ProcessorsCloudProvider interface {
	cloudprovider.CloudProvider

	GetClusterCreateTime() time.Time
	GetClusterVersion() string
	ClusterStarted() (bool, error)
	GetAllZones() ([]string, error)
	RecommendLocations(context.Context, gceclient.RecommendLocationsRequest) (*gceclient.RecommendLocationsResponse, error)
	GetFutureReservationsInProject(string) ([]*gceclient.GceFutureReservation, error)
	GetReservationBlocksInReservation(reservationRef gceclient.ReservationRef) ([]*gceclient.GceReservationBlock, error)
	GetResourcePolicies(projectId string) ([]*gceclient.GceResourcePolicy, error)
	GetMigInstanceTemplateLabels(*gke.GkeMig) (map[string]string, error)
	GetMigInstanceTemplateTaints(*gke.GkeMig) ([]apiv1.Taint, error)
	GetMigInstanceTemplateSelfLink(*gke.GkeMig) (string, error)
	GetExperimentsManager() experiments.Manager
}
