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

package lister

import (
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

type listerCloudProvider interface {
	GetClusterInfo() (projectId, location, clusterName string)
	IsAutopilotEnabled() bool
	IsDefaultCCCEnabled() bool
	MachineConfigProvider() *machinetypes.MachineConfigProvider
	GetAIZones() ([]string, error)
	GetStandardZones() ([]string, error)
	GetAutoprovisioningLocations() []string
	TrimLocationsForMachineConfig(locations []string, machineType string, acceleratorConfig *gke_api_beta.AcceleratorConfig, minCpuPlatform string, diskType string) []string
}

type Lister interface {
	// GetCrd returns the CRD with the given name.
	GetCrd(name string) (crd.CRD, error)
	// ListCrds return all CRDs in the cluster.
	ListCrds() ([]crd.CRD, error)
	// Crd returns the CRD associated with the given label and name.
	Crd(crdLabel string, crdName string) (crd.CRD, error)
	// PodReqCrd returns CRD that should be assigned to Requirements. It returns:
	// - (nil, "", nil) if the CRD should not be assigned
	// - (nil, name, err) if the CRD should be assigned, but it cannot be fetched
	// - (crd, name, nil) if the CRD should be assigned, and it is fetched
	PodReqCrd(req *podrequirements.Requirements) (crd.CRD, string, error)
	// PodReqCrdType returns type of CRD that should be assigned to Requirements. It returns:
	// - ("", nil) if the CRD should not be assigned
	// - ("", err) if the CRD should be assigned, but it is misconfigured
	// - (type, nil) if the CRD should be assigned, and it is configured correctly
	PodReqCrdType(req *podrequirements.Requirements) (string, error)
	// PodCrd returns CRD that should be assigned to Pod. It returns:
	// - (nil, "", nil) if the CRD should not be assigned
	// - (nil, name, err) if the CRD should be assigned, but it cannot be fetched
	// - (crd, name, nil) if the CRD should be assigned, and it is fetched
	PodCrd(pod *apiv1.Pod) (crd.CRD, string, error)
	// NodeGroupCrd returns CRD that should be assigned to NodeGroup. It returns:
	// - (nil, "", nil) if the CRD should not be assigned
	// - (nil, name, err) if the CRD should be assigned, but it cannot be fetched
	// - (crd, name, nil) if the CRD should be assigned, and it is fetched
	NodeGroupCrd(nodeGroup cloudprovider.NodeGroup) (crd.CRD, string, error)
	// NodeCrd returns CRD that should be assigned to Node. It returns:
	// - (nil, "", nil) if the CRD should not be assigned
	// - (nil, name, err) if the CRD should be assigned, but it cannot be fetched
	// - (crd, name, nil) if the CRD should be assigned, and it is fetched
	NodeCrd(node *apiv1.Node) (crd.CRD, string, error)
	// Labels returns the CRD labels supported by the lister.
	Labels() []string
	// Default returns name, label and found value for the default CRD.
	Default() (name string, label string, found bool)
	// SetCloudProvider sets cloud provider so that lister is aware of cluster metadata
	SetCloudProvider(provider listerCloudProvider)
}
