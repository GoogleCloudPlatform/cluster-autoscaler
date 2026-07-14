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

package noscaleup

import (
	"context"
	"fmt"
	"time"

	"github.com/stretchr/testify/mock"
	gce_api "google.golang.org/api/compute/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
)

// NoScaleUpMock is a mock of the NoScaleUp interface.
type NoScaleUpMock struct {
	mock.Mock
}

// GetNewReasons is a mocked method.
func (nsu *NoScaleUpMock) GetNewReasons(scaleUpStatus *vistypes.ScaleUpStatus, napStatus *vistypes.NapStatus, now time.Time) *Reasons {
	args := nsu.Called(scaleUpStatus, napStatus, now)
	return args.Get(0).(*Reasons)
}

// MarkReasonsReported is a mocked method.
func (nsu *NoScaleUpMock) MarkReasonsReported(reasons *Reasons, reportTime time.Time) {
	nsu.Called(reasons, reportTime)
}

// NewNoScaleUpMock returns a new mock of the NoScaleUp interface.
func NewNoScaleUpMock() *NoScaleUpMock {
	return &NoScaleUpMock{}
}

type vizNapTestCloudProvider struct {
	*gke.TestAutoprovisioningCloudProvider
	manager                       vizNapTestGkeManager
	autoprovisioningLocations     []string
	unavailableMachineTypesByZone map[string]map[string]bool
}

// NewNodeGroup is a mocked method.
func (cp vizNapTestCloudProvider) NewNodeGroup(machineType string, labels map[string]string, systemLabels map[string]string, taints []apiv1.Taint, extraResources map[string]resource.Quantity) (cloudprovider.NodeGroup, error) {
	zone, found := systemLabels[apiv1.LabelZoneFailureDomain]
	if !found {
		return nil, fmt.Errorf("zone not found in systemLabels")
	}

	// This is a mechanism to simulate errors on GKE side - e.g. when a machine type is not available in a given zone.
	if unavailableMachineTypes, found := cp.unavailableMachineTypesByZone[zone]; found && unavailableMachineTypes[machineType] {
		return nil, fmt.Errorf("SIMULATED ERROR: machine type %q not available in zone %q", machineType, zone)
	}

	arch := gce.DefaultArch
	nodePoolName := "nap-" + machineType
	spec := &gkeclient.NodePoolSpec{
		MachineType:        machineType,
		Labels:             systemLabels,
		Taints:             taints,
		SystemArchitecture: &arch,
	}

	return gke.NewTestGkeMigBuilder().
		SetGceRef(gce.GceRef{Project: "test-project", Zone: zone, Name: nodePoolName + "-mig"}).
		SetGkeManager(cp.manager).
		SetMaxSize(1000).
		SetAutoprovisioned(true).
		SetNodePoolName(nodePoolName).
		SetSpec(spec).
		SetExtraResources(extraResources).
		Build(), nil
}

// GetResourceLimiter is a mocked method.
func (cp vizNapTestCloudProvider) GetResourceLimiter() (*cloudprovider.ResourceLimiter, error) {
	return cloudprovider.NewResourceLimiter(map[string]int64{}, map[string]int64{
		cloudprovider.ResourceNameCores:  10,
		cloudprovider.ResourceNameMemory: 10000000000,
	}), nil
}

// GetAutoprovisioningLocations returns a list of locations where NAP can create new nodepools
func (cp vizNapTestCloudProvider) GetAutoprovisioningLocations() []string {
	return cp.autoprovisioningLocations
}

func (cp vizNapTestCloudProvider) GetAllNodePoolNames() sets.Set[string] {
	return sets.New[string]()
}

func newVizNapTestCloudProvider(autoprovisioningLocations []string, unavailableMachineTypesByZone map[string]map[string]bool, machineTypesWithInternalErrors map[string]bool) vizNapTestCloudProvider {
	allZones := autoprovisioningLocations
	allZones = append(allZones, "new-zone1", "new-zone2")
	machineTypesPerZone := map[string][]string{}
	for _, zone := range allZones {
		machineTypesPerZone[zone] = machineTypes
	}
	return vizNapTestCloudProvider{
		TestAutoprovisioningCloudProvider: gke.NewTestAutoprovisioningCloudProviderBuilder().
			WithAutoprovisioningEnabled(true).
			WithMachineTypes(machineTypes...).
			WithAllZones(allZones...).
			WithMachineTypesPerZone(machineTypesPerZone).
			WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
			Build(),
		manager: vizNapTestGkeManager{
			GkeManagerMock:                 &gke.GkeManagerMock{},
			machineTypesWithInternalErrors: machineTypesWithInternalErrors,
		},
		autoprovisioningLocations:     autoprovisioningLocations,
		unavailableMachineTypesByZone: unavailableMachineTypesByZone,
	}
}

var machineTypes = []string{
	"n2-standard-2",
	"n2-standard-4",
	"n2-standard-8",
	"n2-standard-16",
}

var machineTypesCpu = map[string]int64{
	"n2-standard-2":  2,
	"n2-standard-4":  4,
	"n2-standard-8":  8,
	"n2-standard-16": 16,
}

var machineTypesMemory = map[string]int64{
	"n2-standard-2":  2000000000,
	"n2-standard-4":  4000000000,
	"n2-standard-8":  8000000000,
	"n2-standard-16": 16000000000,
}

type vizNapTestGkeManager struct {
	*gke.GkeManagerMock
	machineTypesWithInternalErrors map[string]bool
}

func (m vizNapTestGkeManager) BulkFetchCurrentEkVmSizes() (map[gce.GceRef]size.VmSize, error) {
	return nil, nil
}

func (m vizNapTestGkeManager) GetClusterVersion() string {
	return ""
}

// RecommendLocations returns recommendation made by recommendLocations API.
func (m vizNapTestGkeManager) RecommendLocations(context context.Context, region string, request gceclient.RecommendLocationsRequest) (*gceclient.RecommendLocationsResponse, error) {
	return nil, nil
}

func (m vizNapTestGkeManager) FetchCapacityGuidance(ctx context.Context, flexibilityScopeKey string, instanceConfigs map[string]*api.InstanceConfig) (map[string]*api.InstanceAvailability, error) {
	return nil, nil
}

func (m vizNapTestGkeManager) SendCapacityDecision(ctx context.Context, decision api.ProvisioningDecisionNotification) error {
	return nil
}

// GetZonesInRegion returns all zones within a given region.
func (m vizNapTestGkeManager) GetZonesInRegion(region string) ([]string, error) {
	return nil, nil
}

// GetMigInstanceTemplate is a mocked method.
func (m vizNapTestGkeManager) GetMigInstanceTemplate(mig *gke.GkeMig) (*gce_api.InstanceTemplate, error) {
	return nil, nil
}

// GetMigKubeEnv is a mocked method.
func (m vizNapTestGkeManager) GetMigKubeEnv(mig *gke.GkeMig) (gce.KubeEnv, error) {
	return gce.KubeEnv{}, nil
}

// GetMigTemplateNodeInfo is a mocked method.
func (m vizNapTestGkeManager) GetMigTemplateNodeInfo(mig *gke.GkeMig) (*framework.NodeInfo, error) {
	// This is a mechanism to simulate "shouldn't happen" errors on GKE side - this function is usually expected to not fail.
	if m.machineTypesWithInternalErrors[mig.Spec().MachineType] {
		return nil, fmt.Errorf("SIMULATED ERROR: couldn't generate template node for machine type %q", mig.Spec().MachineType)
	}
	templateBuilder := gke.GkeTemplateBuilder{}
	machineType := mig.Spec().MachineType
	gkeMigOsInfo := gke.NewGkeMigOsInfo(gce.NewMigOsInfo(gce.OperatingSystemLinux, "cos", mig.GetSystemArchitecture()), mig.Version(), false)
	ssdDiskSizeProvider := localssdsize.NewSimpleLocalSSDProvider()
	node, err := templateBuilder.BuildNodeFromMigSpec(mig, gkeMigOsInfo, machineTypesCpu[machineType], machineTypesMemory[machineType], nil, &gke.DaemonSetConditions{}, false, &gke.GkeReserved{}, ssdDiskSizeProvider, gkelabels.DefaultMaxPodsPerNode)
	if err != nil {
		return nil, fmt.Errorf("unexpected error: %v", err)
	}
	return framework.NewTestNodeInfo(node), nil
}

// GetNumberOfSurgeNodesInMig is a mocked method.
func (m vizNapTestGkeManager) GetNumberOfSurgeNodesInMig(mig *gke.GkeMig) int {
	return 0
}

// AreConfidentialNodesEnabled is a mocked method.
func (m vizNapTestGkeManager) AreConfidentialNodesEnabled() bool {
	return false
}

func (m vizNapTestGkeManager) GetAllNodePoolNames() sets.Set[string] {
	return sets.New[string]()
}

func (m vizNapTestGkeManager) IsDataplaneV2Enabled() bool {
	return false
}

func (m vizNapTestGkeManager) ScaleDownUnreadyTimeOverride(mig *gke.GkeMig) (time.Duration, bool) {
	return time.Duration(0), false
}

func (m vizNapTestGkeManager) GetAutoprovisioningDefaultFamily() machinetypes.MachineFamily {
	return machinetypes.E2
}

type mockFailureReasons struct {
	reasons []string
}

func (r *mockFailureReasons) Reasons() []string {
	return r.reasons
}
