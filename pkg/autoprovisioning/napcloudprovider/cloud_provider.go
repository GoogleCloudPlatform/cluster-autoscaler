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

package napcloudprovider

import (
	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

// AutoprovisioningCloudProvider is cloud provider interface extended for autoprovisioning use cases. It needs to be
// in a separate subpackage so that other autoprovisioning subpackages can reference it without import cycles.
type AutoprovisioningCloudProvider interface {
	cloudprovider.CloudProvider

	IsNodeAutoprovisioningEnabled() bool
	UseAutoprovisioningFeaturesForPodRequirements(*podrequirements.Requirements) bool
	UseAutoprovisioningFeaturesForNodeGroup(cloudprovider.NodeGroup) bool
	NodeGroupsBlockedByNotFoundError() []cloudprovider.NodeGroup
	NodeGroupsBlockedByServerError() []cloudprovider.NodeGroup
	AreConfidentialNodesEnabled() bool
	GetConfidentialInstanceType() string
	GetDefaultNodePoolDiskType() string
	GetDefaultNodePoolMinCpuPlatform() string
	GetAutoprovisioningLocations() []string
	GetAutoprovisioningDefaultFamily() machinetypes.MachineFamily
	IsResizableVmEnabledInAutopilot(machineFamily string) bool
	IsResizableVmWithinPodFamilyEnabled(machineFamily string) bool
	// IsClusterUsingPSCInfrastructure checks if cluster is using PSC infrastructure. If so, cluster supports public and private nodes.
	IsClusterUsingPSCInfrastructure() bool
	IsAutopilotEnabled() bool
	IsDefaultCCCEnabled() bool
	IsCompactPlacementEnabled() bool
	// GetDefaultEnablePrivateNodes return default value for enablePrivateNodes.
	GetDefaultEnablePrivateNodes() bool
	GetAllNodePoolNames() sets.Set[string]
	GetMachineType(machineType string, zone string) (gce.MachineType, error)
	ValidateGpuConfig(gpuType, gpuPartitionSize, gpuMaxSharedClients, gpuSharingStrategy, machineType string, gpuCount int64, zone string, cpus, mem int64) error
	RegisterNodePoolSpecBuilders(builders []NodePoolSpecBuilder)
	GetClusterInfo() (projectId, location, clusterName string)
	ValidateLocationForDiskType(location string, requestedDiskType string) (ok bool, reason string, err error)
	GetAllZones() ([]string, error)
	GetStandardZones() ([]string, error)
	GetAIZones() ([]string, error)
	IsEkSpotEnabled() bool
	IsEkEdpEnabled() bool
	TrimLocationsForMachineConfig(locations []string, machineType string, acceleratorConfig *gke_api_beta.AcceleratorConfig, minCpuPlatform string, diskType string) []string
	MachineConfigProvider() *machinetypes.MachineConfigProvider
	IsE2lessRegion() bool
}

type NodePoolSpecBuilder interface {
	UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, extraResources map[string]resource.Quantity) error
}
