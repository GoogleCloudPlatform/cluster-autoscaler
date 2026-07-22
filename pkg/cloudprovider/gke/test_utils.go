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

package gke

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/stretchr/testify/mock"
	gce_api "google.golang.org/api/compute/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	testprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	autoscaling_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/customresources"

	csisnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/csi/snapshot"
	drasnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/dynamicresources/snapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/interfaces"
	nap_interfaces "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/interfaces"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/napcloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	ekvmsize "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	ekvmtypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/manager"
	prpods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/reconciler"
)

// TestGkeMigBuilder contains fields needed to create GkeMig for the test purposes.
type TestGkeMigBuilder struct {
	gceRef                      gce.GceRef
	gkeManager                  GkeManager
	id                          string
	minSize                     int
	maxSize                     int
	totalMinSize                int
	totalMaxSize                int
	locationPolicy              LocationPolicyEnum
	autoprovisioned             bool
	exist                       bool
	nodePoolName                string
	napNodePoolHash             string
	spec                        *gkeclient.NodePoolSpec
	blueGreenInfo               *MigBlueGreenInfo
	extraResources              map[string]resource.Quantity
	nodeConfig                  *NodeConfig
	queuedProvisioning          bool
	shortLivedUpgradeInProgress bool
	deploymentType              DeploymentTypeEnum
	domainUrl                   string
}

// NewTestGkeMigBuilder returns new instance of the TestGkeMigBuilder.
func NewTestGkeMigBuilder() *TestGkeMigBuilder {
	return &TestGkeMigBuilder{
		extraResources: map[string]resource.Quantity{},
		nodeConfig:     &NodeConfig{},
		gkeManager: &FakeGkeManager{
			machineConfigProvider: machinetypes.NewMachineConfigProvider(nil),
		},
	}
}

// SetGceRef sets the gceRef field.
func (b *TestGkeMigBuilder) SetGceRef(gceRef gce.GceRef) *TestGkeMigBuilder {
	b.gceRef = gceRef
	return b
}

// SetGceRefProject sets the project of the gceRef field.
func (b *TestGkeMigBuilder) SetGceRefProject(project string) *TestGkeMigBuilder {
	b.gceRef.Project = project
	return b
}

// SetId sets the id of mig.
func (b *TestGkeMigBuilder) SetId(id string) *TestGkeMigBuilder {
	b.id = id
	return b
}

// SetGceRefZone sets the zone of the gceRef field.
func (b *TestGkeMigBuilder) SetGceRefZone(zone string) *TestGkeMigBuilder {
	b.gceRef.Zone = zone
	return b
}

// SetGceRefName sets the name of the gceRef field.
func (b *TestGkeMigBuilder) SetGceRefName(name string) *TestGkeMigBuilder {
	b.gceRef.Name = name
	return b
}

// SetGkeManager sets the gkeManager field.
func (b *TestGkeMigBuilder) SetGkeManager(gkeManager GkeManager) *TestGkeMigBuilder {
	b.gkeManager = gkeManager
	return b
}

// SetExtendedFallbacksEnabled sets the extendedFallbacksEnabled field on FakeGkeManager.
func (b *TestGkeMigBuilder) SetExtendedFallbacksEnabled(enabled bool) *TestGkeMigBuilder {
	if fake, ok := b.gkeManager.(*FakeGkeManager); ok {
		fake.extendedFallbacksEnabled = enabled
	}
	return b
}

// SetMinSize sets the minSize field.
func (b *TestGkeMigBuilder) SetMinSize(minSize int) *TestGkeMigBuilder {
	b.minSize = minSize
	return b
}

// SetMaxSize sets the maxSize field.
func (b *TestGkeMigBuilder) SetMaxSize(maxSize int) *TestGkeMigBuilder {
	b.maxSize = maxSize
	return b
}

// SetTotalMinSize sets the totalMinSize field.
func (b *TestGkeMigBuilder) SetTotalMinSize(totalMinSize int) *TestGkeMigBuilder {
	b.totalMinSize = totalMinSize
	return b
}

// SetTotalMaxSize sets the totalMaxSize field.
func (b *TestGkeMigBuilder) SetTotalMaxSize(totalMaxSize int) *TestGkeMigBuilder {
	b.totalMaxSize = totalMaxSize
	return b
}

// SetLocationPolicy sets the locationPolicy field.
func (b *TestGkeMigBuilder) SetLocationPolicy(locationPolicy LocationPolicyEnum) *TestGkeMigBuilder {
	b.locationPolicy = locationPolicy
	return b
}

// SetAutoprovisioned sets the autoprovisioned field.
func (b *TestGkeMigBuilder) SetAutoprovisioned(autoprovisioned bool) *TestGkeMigBuilder {
	b.autoprovisioned = autoprovisioned
	return b
}

// SetExist sets the exist field.
func (b *TestGkeMigBuilder) SetExist(exist bool) *TestGkeMigBuilder {
	b.exist = exist
	return b
}

func (b *TestGkeMigBuilder) SetUpcoming() *TestGkeMigBuilder {
	upcomingGkeManager := &GkeManagerMock{MockIsUpcoming: true}
	upcomingGkeManager.On("IsUpcoming", mock.Anything).Return(true)
	return NewTestGkeMigBuilder().
		SetGkeManager(upcomingGkeManager).
		SetExist(false)
}

// SetNodePoolName sets the nodePoolName field.
func (b *TestGkeMigBuilder) SetNodePoolName(nodePoolName string) *TestGkeMigBuilder {
	b.nodePoolName = nodePoolName
	return b
}

// SetNapNodePoolHash sets the napNodePoolHash field.
func (b *TestGkeMigBuilder) SetNapNodePoolHash(napNodePoolHash string) *TestGkeMigBuilder {
	b.napNodePoolHash = napNodePoolHash
	return b
}

// SetSpec sets the spec field.
func (b *TestGkeMigBuilder) SetSpec(spec *gkeclient.NodePoolSpec) *TestGkeMigBuilder {
	b.spec = spec
	return b
}

// SetNodeConfig sets the spec field.
func (b *TestGkeMigBuilder) SetNodeConfig(nodeConfig *NodeConfig) *TestGkeMigBuilder {
	b.nodeConfig = nodeConfig
	return b
}

// SetBlueGreenInfo sets the blueGreenInfo fields.
func (b *TestGkeMigBuilder) SetBlueGreenInfo(bgi *MigBlueGreenInfo) *TestGkeMigBuilder {
	b.blueGreenInfo = bgi
	return b
}

// SetQueuedProvisioning sets the queuedProvisioning field.
func (b *TestGkeMigBuilder) SetQueuedProvisioning(queuedProvisioning bool) *TestGkeMigBuilder {
	b.queuedProvisioning = queuedProvisioning
	return b
}

// SetShortLivedUpgradeInProgress sets the shortLivedUpgradeInProgress field.
func (b *TestGkeMigBuilder) SetShortLivedUpgradeInProgress(shortLivedUpgradeInProgress bool) *TestGkeMigBuilder {
	b.shortLivedUpgradeInProgress = shortLivedUpgradeInProgress
	return b
}

// SetExtraResources sets the extra resources field.
func (b *TestGkeMigBuilder) SetExtraResources(extraResources map[string]resource.Quantity) *TestGkeMigBuilder {
	b.extraResources = extraResources
	return b
}

func (b *TestGkeMigBuilder) SetDeploymentType(deploymentType DeploymentTypeEnum) *TestGkeMigBuilder {
	b.deploymentType = deploymentType
	return b
}

// SetDomainUrl sets the domainUrl field.
func (b *TestGkeMigBuilder) SetDomainUrl(domainUrl string) *TestGkeMigBuilder {
	b.domainUrl = domainUrl
	return b
}

// Build creates an instance of GkeMig to be used in tests.
// We do not want to expose private GkeMig fields to other modules most of the time but it is
// sometimes convenient to create GkeMig instance in tests of pieces of code which assume they get
// NodeGroup of GkeMig type.
func (b *TestGkeMigBuilder) Build() *GkeMig {
	if len(b.locationPolicy) == 0 {
		b.locationPolicy = LocationPolicyUnspecified
	}

	mig := NewGkeMig(b.gceRef, b.domainUrl, b.gkeManager)
	mig.id = b.id
	mig.minSize = b.minSize
	mig.maxSize = b.maxSize
	mig.totalMinSize = b.totalMinSize
	mig.totalMaxSize = b.totalMaxSize
	mig.locationPolicy = b.locationPolicy
	mig.autoprovisioned = b.autoprovisioned
	mig.exist = b.exist
	mig.spec = b.spec
	mig.blueGreenInfo = b.blueGreenInfo
	mig.extraResources = b.extraResources
	mig.nodeConfig = b.nodeConfig
	mig.queuedProvisioning = b.queuedProvisioning
	mig.shortLivedUpgradeInProgress = b.shortLivedUpgradeInProgress
	mig.deploymentType = b.deploymentType

	AddMigsToNodePool(b.nodePoolName, mig)
	return mig
}

// TestMigSpecBuilder contains fields needed to create Mig Spec for the test purposes.
type TestMigSpecBuilder struct {
	Labels              map[string]string
	ReservationAffinity *gke_api_beta.ReservationAffinity
	MachineType         string
	Locations           []string
	PlacementGroup      placement.Spec
	TpuType             string
	TpuTopology         string
	TpuMultiHost        bool
	FlexStart           bool
}

// NewTestMigSpecBuilder returns new instance of the TestGkeMigBuilder.
func NewTestMigSpecBuilder() *TestMigSpecBuilder {
	return &TestMigSpecBuilder{
		Labels: map[string]string{},
	}
}

// SetReservationAffinity sets the reservation affinity spec field.
func (s *TestMigSpecBuilder) SetReservationAffinity(rsvType, rsvPath string) *TestMigSpecBuilder {
	s.ReservationAffinity = &gke_api_beta.ReservationAffinity{}
	s.ReservationAffinity.ConsumeReservationType = rsvType
	if rsvPath != "" {
		s.ReservationAffinity.Key = gkeclient.ReservationNameKey
		s.ReservationAffinity.Values = []string{rsvPath}
	}
	return s
}

// SetLabels sets the Labels spec field.
func (s *TestMigSpecBuilder) SetLabels(labels map[string]string) *TestMigSpecBuilder {
	s.Labels = labels
	return s
}

// SetMachineType sets the machine type spec field.
func (s *TestMigSpecBuilder) SetMachineType(machineType string) *TestMigSpecBuilder {
	s.MachineType = machineType
	return s
}

// SetLocations sets the locations spec field.
func (s *TestMigSpecBuilder) SetLocations(locations []string) *TestMigSpecBuilder {
	s.Locations = locations
	return s
}

// SetPlacementGroup sets the placement group spec field.
func (s *TestMigSpecBuilder) SetPlacementGroup(groupId, policy string) *TestMigSpecBuilder {
	s.PlacementGroup = placement.Spec{}
	s.PlacementGroup.GroupId = groupId
	s.PlacementGroup.Policy = policy
	return s
}

// SetTpuType sets the tpu type spec fields.
func (s *TestMigSpecBuilder) SetTpuType(tpuType string) *TestMigSpecBuilder {
	s.TpuType = tpuType
	return s
}

// SetTpuTopology sets the tpu topology spec fields.
func (s *TestMigSpecBuilder) SetTpuTopology(topology string) *TestMigSpecBuilder {
	s.TpuTopology = topology
	return s
}

// SetTpuMultiHost sets the tpu multi host spec fields.
func (s *TestMigSpecBuilder) SetTpuMultiHost(multiHost bool) *TestMigSpecBuilder {
	s.TpuMultiHost = multiHost
	return s
}

func (s *TestMigSpecBuilder) SetFlexStart(flexStart bool) *TestMigSpecBuilder {
	s.FlexStart = flexStart
	return s
}

// SpecBuild creates an instance of NodePoolSpec to be used in tests.
func (s *TestMigSpecBuilder) SpecBuild() *gkeclient.NodePoolSpec {
	return &gkeclient.NodePoolSpec{
		Labels:              s.Labels,
		ReservationAffinity: s.ReservationAffinity,
		MachineType:         s.MachineType,
		Locations:           s.Locations,
		PlacementGroup:      s.PlacementGroup,
		TpuType:             s.TpuType,
		TpuTopology:         s.TpuTopology,
		TpuMultiHost:        s.TpuMultiHost,
		FlexStart:           s.FlexStart,
	}
}

// TestAutoprovisioningCloudProvider extends TestCloudProvider with node locations.
type TestAutoprovisioningCloudProvider struct {
	*testprovider.TestCloudProvider
	network                           *gce_api.Network
	nodeLocations                     []string
	autoprovisioningLocations         []string
	allZones                          []string
	standardZones                     []string
	aiZones                           []string
	trimmedLocations                  []string
	confidentialNodesEnabled          bool
	confidentialInstanceType          string
	isClusterUsingPSCInfrastructure   bool
	resizableVmInAutopilotEnabled     map[string]bool
	resizableVmWithinPodFamilyEnabled map[string]bool
	isAutopilotEnabled                bool
	isDefaultCCCEnabled               bool
	isCompactPlacementEnabled         bool
	defaultNodePoolDiskType           string
	defaultNodePoolMinCpuPlatform     string
	defaultEnablePrivateNodes         bool
	validateGpuConfigErrorPerGpuType  map[string]error
	gkeMigs                           []*GkeMig
	injectedNodeGroups                []cloudprovider.NodeGroup
	nodeGroupsBlockedByServerError    []cloudprovider.NodeGroup
	nodeGroupsBlockedByNotFoundError  []cloudprovider.NodeGroup
	autoprovisioningDefaultFamily     *machinetypes.MachineFamily
	autoprovisioningSecondaryFamily   *machinetypes.MachineFamily
	autoprovisioningEligibility       AutoprovisioningEligibility
	validMachineTypes                 map[gce.MachineTypeKey]bool
	isEkSpotEnabled                   bool
	isEkEdpEnabled                    bool
	extendedFallbacksEnabled          bool
	isArmMachineFallbacksEnabled      bool
	machineTypesPerZone               map[string][]string
	machineConfigProvider             *machinetypes.MachineConfigProvider
	nodePoolSpec                      *gkeclient.NodePoolSpec
}

// TestAutoprovisioningCloudProviderBuilder is used to create test GKE CloudProvider
type TestAutoprovisioningCloudProviderBuilder struct {
	ossBuilder *testprovider.TestCloudProviderBuilder
	builders   []func(p *TestAutoprovisioningCloudProvider)
}

func NewTestAutoprovisioningCloudProviderBuilder() *TestAutoprovisioningCloudProviderBuilder {
	return &TestAutoprovisioningCloudProviderBuilder{
		ossBuilder: testprovider.NewTestCloudProviderBuilder(),
	}
}

// WithOnScaleUp adds scale-up handle function to provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithOnScaleUp(onScaleUp testprovider.OnScaleUpFunc) *TestAutoprovisioningCloudProviderBuilder {
	b.ossBuilder = b.ossBuilder.WithOnScaleUp(onScaleUp)
	return b
}

// WithOnScaleDown adds scale-down handle function to provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithOnScaleDown(onScaleDown testprovider.OnScaleDownFunc) *TestAutoprovisioningCloudProviderBuilder {
	b.ossBuilder = b.ossBuilder.WithOnScaleDown(onScaleDown)
	return b
}

// WithOnNodeGroupCreate adds node group creation handle function to provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithOnNodeGroupCreate(onNodeGroupCreate testprovider.OnNodeGroupCreateFunc) *TestAutoprovisioningCloudProviderBuilder {
	b.ossBuilder = b.ossBuilder.WithOnNodeGroupCreate(onNodeGroupCreate)
	return b
}

// WithOnNodeGroupDelete adds node group deletion handle function to provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithOnNodeGroupDelete(onNodeGroupDelete testprovider.OnNodeGroupDeleteFunc) *TestAutoprovisioningCloudProviderBuilder {
	b.ossBuilder = b.ossBuilder.WithOnNodeGroupDelete(onNodeGroupDelete)
	return b
}

// WithMachineTypes adds machine types to provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithMachineTypes(machineTypes ...string) *TestAutoprovisioningCloudProviderBuilder {
	b.ossBuilder = b.ossBuilder.WithMachineTypes(machineTypes)
	return b
}

// WithMachineTypesPerZone adds machine types per zone to provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithMachineTypesPerZone(machineTypesPerZone map[string][]string) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.machineTypesPerZone = machineTypesPerZone
	})
	return b
}

// WithMachineTemplates adds machine templates for provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithMachineTemplates(machineTemplates map[string]*framework.NodeInfo) *TestAutoprovisioningCloudProviderBuilder {
	b.ossBuilder = b.ossBuilder.WithMachineTemplates(machineTemplates)
	return b
}

// WithHasInstance adds has instance handler to provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithHasInstance(hasInstance testprovider.HasInstance) *TestAutoprovisioningCloudProviderBuilder {
	b.ossBuilder = b.ossBuilder.WithHasInstance(hasInstance)
	return b
}

// WithPricingModel adds pricing model to provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithPricingModel(pricingModel cloudprovider.PricingModel) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.SetPricingModel(pricingModel)
	})
	return b
}

// WithResourceLimiter adds resource limiter to provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithResourceLimiter(resourceLimiter *cloudprovider.ResourceLimiter) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.SetResourceLimiter(resourceLimiter)
	})
	return b
}

func (b *TestAutoprovisioningCloudProviderBuilder) WithAutoprovisioningEnabled(enabled bool) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		eligibility := &MockAutoprovisioningEligibility{}
		eligibility.On("IsNodeAutoprovisioningEnabled").Return(enabled)
		eligibility.On("AreClusterLimitsEnabled").Return(enabled)
		eligibility.On("UseAutoprovisioningFeaturesForPodRequirements", mock.Anything).Return(enabled)
		eligibility.On("UseAutoprovisioningFeaturesForNodeGroup", mock.Anything).Return(enabled)
		p.autoprovisioningEligibility = eligibility
	})
	return b
}

func (b *TestAutoprovisioningCloudProviderBuilder) WithAutoprovisioningEligibility(eligibility AutoprovisioningEligibility) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.autoprovisioningEligibility = eligibility
	})
	return b
}

// WithNodeLocations adds node locations to provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithNodeLocations(locations ...string) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.nodeLocations = locations
	})
	return b
}

// WithConfidentialNodesEnabled enables confidential nodes in provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithConfidentialNodesEnabled(enabled bool) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.confidentialNodesEnabled = enabled
	})
	return b
}

// WithConfidentialInstanceType sets confidential instance type in provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithConfidentialInstanceType(confidentialInstanceType string) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.confidentialInstanceType = confidentialInstanceType
	})
	return b
}

// WithUsingPSCInfrastructure enables PSC infrastructure in provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithUsingPSCInfrastructure(enabled bool) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.isClusterUsingPSCInfrastructure = enabled
	})
	return b
}

// WithAutopilotEnabled enables autopilot in provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithAutopilotEnabled(enabled bool) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.isAutopilotEnabled = enabled
	})
	return b
}

// WithEkEdpEnabled enables new ek edp logic in provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithEkEdpEnabled(enabled bool) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.isEkEdpEnabled = enabled
	})
	return b
}

// WithDefaultNodePoolDiskType sets default node pool disk type in provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithDefaultNodePoolDiskType(diskType string) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.defaultNodePoolDiskType = diskType
	})
	return b
}

// WithDefaultNodePoolMinCpuPlatform sets default min cpu platform in provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithDefaultNodePoolMinCpuPlatform(minCpuPlatform string) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.defaultNodePoolMinCpuPlatform = minCpuPlatform
	})
	return b
}

// WithAutoprovisioningLocations sets autoprovisioning locations in provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithAutoprovisioningLocations(locations ...string) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.autoprovisioningLocations = locations
	})
	return b
}

// WithAllZones sets all zones within a region that cluster is running in.
func (b *TestAutoprovisioningCloudProviderBuilder) WithAllZones(zones ...string) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.allZones = zones
	})
	return b
}

// WithResizableVmInAutopilotEnabled enables resizable VM in autopilot in provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithResizableVmInAutopilotEnabled(machineFamily string, enabled bool) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		if p.resizableVmInAutopilotEnabled == nil {
			p.resizableVmInAutopilotEnabled = make(map[string]bool)
		}
		p.resizableVmInAutopilotEnabled[machineFamily] = enabled
	})
	return b
}

// WithResizableVmWithinPodFamilyEnabled enables resizable VM within pod family in provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithResizableVmWithinPodFamilyEnabled(machineFamily string, enabled bool) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		if p.resizableVmWithinPodFamilyEnabled == nil {
			p.resizableVmWithinPodFamilyEnabled = make(map[string]bool)
		}
		p.resizableVmWithinPodFamilyEnabled[machineFamily] = enabled
	})
	return b
}

// WithExtendedFallbacksEnabled enables extended fallbacks in provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithExtendedFallbacksEnabled(enabled bool) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.extendedFallbacksEnabled = enabled
	})
	return b
}

// WithCompactPlacementEnabled enables compact placement in provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithCompactPlacementEnabled(enabled bool) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.isCompactPlacementEnabled = enabled
	})
	return b
}

// WithAutoprovisioningDefaultFamily set default autoprovisioning family in provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithAutoprovisioningDefaultFamily(family machinetypes.MachineFamily) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.autoprovisioningDefaultFamily = &family
	})
	return b
}

// WithDefaultEnablePrivateNodes enables default enablement of private nodes in provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithDefaultEnablePrivateNodes(enabled bool) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.defaultEnablePrivateNodes = enabled
	})
	return b
}

// WithValidateGpuConfigErrors sets validate gpu errors in provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithValidateGpuConfigErrors(errs map[string]error) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.validateGpuConfigErrorPerGpuType = errs
	})
	return b
}

// WithGkeMigs sets gke migs in provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithGkeMigs(migs []*GkeMig) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.gkeMigs = migs
	})
	return b
}

// WithValidMachineTypes sets valid machine types in provider
func (b *TestAutoprovisioningCloudProviderBuilder) WithValidMachineTypes(validMachineTypes []gce.MachineTypeKey) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.validMachineTypes = make(map[gce.MachineTypeKey]bool)
		for _, validMachineType := range validMachineTypes {
			p.validMachineTypes[validMachineType] = true
		}
	})
	return b
}

func (b *TestAutoprovisioningCloudProviderBuilder) WithEkSpotEnabled(enabled bool) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.isEkSpotEnabled = enabled
	})
	return b
}

func (b *TestAutoprovisioningCloudProviderBuilder) WithArmMachineFallbacksEnabled(enabled bool) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.isArmMachineFallbacksEnabled = enabled
	})
	return b
}
func (b *TestAutoprovisioningCloudProviderBuilder) WithNetwork(network *gce_api.Network) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.network = network
	})
	return b
}

func (b *TestAutoprovisioningCloudProviderBuilder) WithMachineConfigProvider(machineConfigProvider *machinetypes.MachineConfigProvider) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.machineConfigProvider = machineConfigProvider
	})
	return b
}

func (b *TestAutoprovisioningCloudProviderBuilder) WithNodePoolSpec(nps *gkeclient.NodePoolSpec) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.nodePoolSpec = nps
	})
	return b
}

func (b *TestAutoprovisioningCloudProviderBuilder) WithStandardZones(zones []string) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.standardZones = zones
	})
	return b
}

func (b *TestAutoprovisioningCloudProviderBuilder) WithAiZones(zones []string) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.aiZones = zones
	})
	return b
}

func (b *TestAutoprovisioningCloudProviderBuilder) WithTrimmedLocations(zones []string) *TestAutoprovisioningCloudProviderBuilder {
	b.builders = append(b.builders, func(p *TestAutoprovisioningCloudProvider) {
		p.trimmedLocations = zones
	})
	return b
}

// Build returns a built test cloud provider
func (b *TestAutoprovisioningCloudProviderBuilder) Build() *TestAutoprovisioningCloudProvider {
	p := &TestAutoprovisioningCloudProvider{
		TestCloudProvider: b.ossBuilder.Build(),
	}

	for _, builder := range b.builders {
		builder(p)
	}
	return p
}

func (cp *TestAutoprovisioningCloudProvider) setNodeGroupsBlockedByServerError(nodeGroups []cloudprovider.NodeGroup) {
	cp.nodeGroupsBlockedByServerError = nodeGroups
}

func (cp *TestAutoprovisioningCloudProvider) NodeGroupsBlockedByServerError() []cloudprovider.NodeGroup {
	return cp.nodeGroupsBlockedByServerError
}

func (cp *TestAutoprovisioningCloudProvider) setNodeGroupsBlockedByNotFoundError(nodeGroups []cloudprovider.NodeGroup) {
	cp.nodeGroupsBlockedByNotFoundError = nodeGroups
}

func (cp *TestAutoprovisioningCloudProvider) NodeGroupsBlockedByNotFoundError() []cloudprovider.NodeGroup {
	return cp.nodeGroupsBlockedByNotFoundError
}

func (cp *TestAutoprovisioningCloudProvider) GetClusterInfo() (projectId, location, clusterName string) {
	return "12345", "us-central1", "test"
}

func (cp *TestAutoprovisioningCloudProvider) GetClusterNetwork() (*gce_api.Network, error) {
	return cp.network, nil
}

func (cp *TestAutoprovisioningCloudProvider) RegisterNodePoolSpecBuilders(builders []napcloudprovider.NodePoolSpecBuilder) {
}

// IsNodeAutoprovisioningEnabled returns true if NAP is enabled.
func (cp *TestAutoprovisioningCloudProvider) IsNodeAutoprovisioningEnabled() bool {
	if cp.autoprovisioningEligibility == nil {
		return false
	}
	return cp.autoprovisioningEligibility.IsNodeAutoprovisioningEnabled()
}

// UseAutoprovisioningFeaturesForPodRequirements checks if pod should trigger autoprovisioning features.
func (cp *TestAutoprovisioningCloudProvider) UseAutoprovisioningFeaturesForPodRequirements(req *podrequirements.Requirements) bool {
	if cp.autoprovisioningEligibility == nil {
		return false
	}
	return cp.autoprovisioningEligibility.UseAutoprovisioningFeaturesForPodRequirements(req)
}

// UseAutoprovisioningFeaturesForNodeGroup check if node group should trigger autoprovisioning features.
func (cp *TestAutoprovisioningCloudProvider) UseAutoprovisioningFeaturesForNodeGroup(group cloudprovider.NodeGroup) bool {
	if cp.autoprovisioningEligibility == nil {
		return false
	}
	return cp.autoprovisioningEligibility.UseAutoprovisioningFeaturesForNodeGroup(group)
}

func (cp *TestAutoprovisioningCloudProvider) IsResizableVmEnabledInAutopilot(machineFamily string) bool {
	return cp.resizableVmInAutopilotEnabled[machineFamily]
}

func (cp *TestAutoprovisioningCloudProvider) IsEkEdpEnabled() bool {
	return cp.isEkEdpEnabled
}

func (cp *TestAutoprovisioningCloudProvider) IsResizableVmWithinPodFamilyEnabled(machineFamily string) bool {
	return cp.resizableVmWithinPodFamilyEnabled[machineFamily]
}

func (cp *TestAutoprovisioningCloudProvider) IsExtendedFallbacksEnabled() bool {
	return cp.extendedFallbacksEnabled
}

func (cp *TestAutoprovisioningCloudProvider) IsE2lessRegion() bool {
	return cp.GetAutoprovisioningDefaultFamily().Name() == machinetypes.E4.Name()
}

func (cp *TestAutoprovisioningCloudProvider) IsArmMachineFallbacksEnabled() bool {
	return cp.isArmMachineFallbacksEnabled
}

// AreConfidentialNodesEnabled checks if ConfidentialNodes are enabled in cluster.
func (cp *TestAutoprovisioningCloudProvider) AreConfidentialNodesEnabled() bool {
	if cp.confidentialNodesEnabled {
		return true
	}
	confidentialInstanceType := cp.GetConfidentialInstanceType()
	return confidentialInstanceType != "" && confidentialInstanceType != gkelabels.UnspecifiedConfidentialNodeTypeValue
}

// GetConfidentialInstanceType returns the confidential instance type of the cluster.
func (cp *TestAutoprovisioningCloudProvider) GetConfidentialInstanceType() string {
	return cp.confidentialInstanceType
}

// IsClusterUsingPSCInfrastructure checks if cluster is using PSC infrastructure.
func (cp *TestAutoprovisioningCloudProvider) IsClusterUsingPSCInfrastructure() bool {
	return cp.isClusterUsingPSCInfrastructure
}

// IsAutopilotEnabled checks if Autopilot is enabled.
func (cp *TestAutoprovisioningCloudProvider) IsAutopilotEnabled() bool {
	return cp.isAutopilotEnabled
}

// IsDefaultCCCEnabled checks if CCC Default is enabled.
func (cp *TestAutoprovisioningCloudProvider) IsDefaultCCCEnabled() bool {
	return cp.isDefaultCCCEnabled
}

// IsCompactPlacementEnabled checks if Autopilot is enabled.
func (cp *TestAutoprovisioningCloudProvider) IsCompactPlacementEnabled() bool {
	return cp.isCompactPlacementEnabled
}

// GetDefaultEnablePrivateNodes return default value for enablePrivateNodes.
func (cp *TestAutoprovisioningCloudProvider) GetDefaultEnablePrivateNodes() bool {
	return cp.defaultEnablePrivateNodes
}

// GPULabel returns the label added to nodes with GPU resource.
func (cp *TestAutoprovisioningCloudProvider) GPULabel() string {
	return gce.GPULabel
}

// GetExistingNodeGroupLocations returns a list of locations for created node groups.
func (cp *TestAutoprovisioningCloudProvider) GetExistingNodeGroupLocations() []string {
	return cp.nodeLocations
}

// GetDefaultNodePoolDiskType returns a default node pool disk type
func (cp *TestAutoprovisioningCloudProvider) GetDefaultNodePoolDiskType() string {
	return cp.defaultNodePoolDiskType
}

// GetDefaultNodePoolMinCpuPlatform returns a default node pool min cpu platform
func (cp *TestAutoprovisioningCloudProvider) GetDefaultNodePoolMinCpuPlatform() string {
	return cp.defaultNodePoolMinCpuPlatform
}

// GetAutoprovisioningLocations returns a list of locations where NAP can create new nodepools
func (cp *TestAutoprovisioningCloudProvider) GetAutoprovisioningLocations() []string {
	return cp.autoprovisioningLocations
}

// GetAllZones returns all zones within a region that cluster is running in.
func (cp *TestAutoprovisioningCloudProvider) GetAllZones() ([]string, error) {
	return cp.allZones, nil
}

func (cp *TestAutoprovisioningCloudProvider) GetMachineType(machineType string, zone string) (gce.MachineType, error) {
	if machineTypes, zoneFound := cp.machineTypesPerZone[zone]; zoneFound {
		for _, mt := range machineTypes {
			if mt == machineType {
				return gce.MachineType{}, nil
			}
		}
	}
	return gce.MachineType{}, fmt.Errorf("machine type %s not found in zone %s", machineType, zone)
}

func (cp *TestAutoprovisioningCloudProvider) ValidateGpuConfig(gpuType, gpuPartitionSize, gpuMaxSharedClients, gpuSharingStrategy, machineType string, gpuCount int64, zone string, cpus, mem int64) error {
	return cp.validateGpuConfigErrorPerGpuType[gpuType]
}

// GetGkeMigs returns a list of registered migs in the current snapshot or an error on failure
func (cp *TestAutoprovisioningCloudProvider) GetGkeMigs() []*GkeMig {
	return cp.gkeMigs
}

// GetAllNodePoolNames returns a list of all node pool names
func (cp *TestAutoprovisioningCloudProvider) GetAllNodePoolNames() sets.Set[string] {
	return sets.New[string]()
}

func (cp *TestAutoprovisioningCloudProvider) ValidateLocationForDiskType(location string, requestedDiskType string) (ok bool, reason string, err error) {
	return true, "", nil
}

func (cp *TestAutoprovisioningCloudProvider) TrimLocationsForMachineConfig(locations []string, machineType string, acceleratorConfig *gke_api_beta.AcceleratorConfig, minCpuPlatform string, diskType string) []string {
	return cp.trimmedLocations
}

// GetAutoprovisioningDefaultFamily returns an ad-hoc machine family based on machine types specified in the
// embedded *testprovider.TestCloudProvider.
func (cp *TestAutoprovisioningCloudProvider) GetAutoprovisioningDefaultFamily() machinetypes.MachineFamily {
	if cp.autoprovisioningDefaultFamily != nil {
		return *cp.autoprovisioningDefaultFamily
	}
	machineTypeNames, _ := cp.GetAvailableMachineTypes()
	var machineTypes []machinetypes.MachineType
	for i, machineTypeName := range machineTypeNames {
		machineTypes = append(machineTypes, machinetypes.NewMachineTypeInfo(machineTypeName, int64(i), float64(i)))
	}
	return machinetypes.NewTestMachineFamily("testfamily", machineTypes, machinetypes.IntelSandyBridge, machinetypes.IntelIceLake, nil, nil)
}

// ValidateMachineTypeConfig checks if machine type is valid
func (cp *TestAutoprovisioningCloudProvider) ValidateMachineTypeConfig(machineType, zone string) error {
	key := gce.MachineTypeKey{
		MachineTypeName: machineType,
		Zone:            zone,
	}
	if cp.validMachineTypes[key] {
		return nil
	}
	return fmt.Errorf("Machine type %s is not available in zone %s.", machineType, zone)
}

func (cp *TestAutoprovisioningCloudProvider) IsEkSpotEnabled() bool {
	return cp.isEkSpotEnabled
}

// AddAutoprovisionedGkeNodeGroup creates and registers GkeNodeGroup
func (cp *TestAutoprovisioningCloudProvider) AddAutoprovisionedGkeNodeGroup(nodePoolName, id string, size int, exists, autoprovisioned bool, machineType string, isAutopilot bool, isStable bool) {
	cp.InsertNodeGroup(cp.NewTestGkeNodeGroup(nodePoolName, id, size, exists, autoprovisioned, machineType, isAutopilot, false, isStable))
}

func (cp *TestAutoprovisioningCloudProvider) AddBlockedNodeGroupBlockedByServerError(nodePoolName, id string, size int, exists, autoprovisioned bool, machineType string, isAutopilot bool, isStable bool) {
	cp.setNodeGroupsBlockedByServerError(append(cp.NodeGroupsBlockedByServerError(), cp.NewTestGkeNodeGroup(nodePoolName, id, size, exists, autoprovisioned, machineType, isAutopilot, false, isStable)))
}

func (cp *TestAutoprovisioningCloudProvider) AddNodeGroupBlockedByNotFoundError(nodePoolName, id string, size int, exists, autoprovisioned bool, machineType string, isAutopilot bool, isStable bool) {
	cp.setNodeGroupsBlockedByNotFoundError(append(cp.NodeGroupsBlockedByNotFoundError(), cp.NewTestGkeNodeGroup(nodePoolName, id, size, exists, autoprovisioned, machineType, isAutopilot, false, isStable)))
}

// NewTestGkeNodeGroup creates TestGkeNodeGroup
func (cp *TestAutoprovisioningCloudProvider) NewTestGkeNodeGroup(nodePoolName, id string, size int, exists, autoprovisioned bool, machineType string, isAutopilot bool, queuedProvisioning bool, isStable bool) *TestGkeNodeGroup {
	return &TestGkeNodeGroup{TestNodeGroup: cp.BuildNodeGroup(id, 0, 10, size, exists, autoprovisioned, machineType, nil), machineConfigProvider: cp.machineConfigProvider, nodePoolName: nodePoolName, isAutopilot: isAutopilot, queuedProvisioning: queuedProvisioning, isStable: isStable}
}

// NewNodeGroup creates regular TestNodeGroup
func (cp *TestAutoprovisioningCloudProvider) NewNodeGroup(machineType string, labels map[string]string, systemLabels map[string]string, taints []apiv1.Taint, extraResources map[string]resource.Quantity) (cloudprovider.NodeGroup, error) {
	nodeGroup, err := cp.TestCloudProvider.NewNodeGroup(machineType, labels, systemLabels, taints, extraResources)
	if err != nil {
		return nil, err
	}
	gkeNodeGroup := wrapAsTestGkeNodeGroupIfNeeded(nodeGroup)
	gkeNodeGroup.isAutopilot = cp.isAutopilotEnabled
	gkeNodeGroup.machineConfigProvider = machinetypes.NewMachineConfigProvider(nil)

	if zone, found := systemLabels[apiv1.LabelZoneFailureDomain]; found {
		gkeNodeGroup.zone = zone
	}

	if edpLabel, found := systemLabels[gkelabels.ExtendedDurationPodsLabel]; found {
		gkeNodeGroup.extendDurationPods = edpLabel
	}
	if ppvmCPULabel, found := systemLabels[gkelabels.PodPerVMSizeLabel]; found {
		gkeNodeGroup.ppvmCPU = ppvmCPULabel
	}
	if podCapacityLabel, found := systemLabels[gkelabels.PodCapacityLabel]; found {
		gkeNodeGroup.podCapacity = podCapacityLabel
	}
	if reservationNameLabel, found := systemLabels[gkelabels.ReservationNameLabel]; found {
		gkeNodeGroup.reservationName = reservationNameLabel
	}
	if reservationProjectLabel, found := systemLabels[gkelabels.ReservationProjectLabel]; found {
		gkeNodeGroup.reservationProject = reservationProjectLabel
	}
	if reservationAffinityLabel, found := systemLabels[gkelabels.ReservationAffinityLabel]; found {
		gkeNodeGroup.reservationAffinity = reservationAffinityLabel
	}
	if computeClassLabel, found := systemLabels[gkelabels.ComputeClassLabel]; found {
		gkeNodeGroup.computeClass = computeClassLabel
	}
	if csnLabel, found := systemLabels[csn.SoftWorkloadSeparationKey]; found {
		gkeNodeGroup.csnLabel = csnLabel
	}
	if cccPriorityIdxLabel, found := systemLabels[gkelabels.ComputeClassPriorityIdxLabel]; found {
		gkeNodeGroup.cccPriorityIdx = cccPriorityIdxLabel
	}

	for _, t := range taints {
		if t.Key == csn.SoftWorkloadSeparationKey {
			gkeNodeGroup.csnSoftTaint = t
			break
		}
	}

	if gpuCount, found := extraResources[gpu.ResourceNvidiaGPU]; found {
		gpuName, found := systemLabels[gkelabels.GPULabel]
		if !found {
			return nil, fmt.Errorf("GPU supplied in extraResources, but no type specified in systemLabels")
		}
		gpuDriverVersion := systemLabels[gkelabels.GPUDriverVersionLabel]

		gpuType, ok := cp.machineConfigProvider.ToGpuType(gpuName)
		if !ok {
			return nil, fmt.Errorf("GPU name %s is not recognized", gpuName)
		}
		gpuPartitionSize := systemLabels[gkelabels.GPUPartitionSizeLabel]
		gpuPartitionCount, err := gpuType.GetPartitionCount(gpuPartitionSize)
		if err != nil {
			return nil, err
		}
		gpuMaxSharedClientsStr := systemLabels[gkelabels.GPUMaxSharedClientsLabel]
		gpuMaxSharedClients, err := machinetypes.GetMaxGpuSharedClients(gpuMaxSharedClientsStr)
		if err != nil {
			return nil, err
		}

		gkeNodeGroup.gpuType = gpuName
		gkeNodeGroup.gpuPartitionSize = gpuPartitionSize
		gkeNodeGroup.gpuMaxSharedClients = gpuMaxSharedClientsStr
		gkeNodeGroup.gpuSharingStrategy = systemLabels[gkelabels.GPUSharingStrategyLabel]
		gkeNodeGroup.gpuCount = gpuCount.Value() * gpuPartitionCount * gpuMaxSharedClients
		gkeNodeGroup.gpuDriverVersion = gpuDriverVersion
	}

	return gkeNodeGroup, nil
}

// MachineConfigProvider returns machine config provider.
func (cp *TestAutoprovisioningCloudProvider) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return cp.machineConfigProvider
}

// NodePoolSpecForNode returns node pool specification for the given node.
// Current implementation only allows stubbing a single node pool spec to be
// used for all nodes.
func (cp *TestAutoprovisioningCloudProvider) NodePoolSpecForNode(_ *apiv1.Node) (*gkeclient.NodePoolSpec, error) {
	if cp.nodePoolSpec == nil {
		return nil, fmt.Errorf("nodePoolSpec not set in TestAutoprovisioningCloudProvider")
	}
	return cp.nodePoolSpec, nil
}

func wrapAsTestGkeNodeGroupIfNeeded(nodeGroup cloudprovider.NodeGroup) *TestGkeNodeGroup {
	if testGkeNodeGroup, ok := nodeGroup.(*TestGkeNodeGroup); ok {
		return testGkeNodeGroup
	}
	return &TestGkeNodeGroup{TestNodeGroup: nodeGroup.(*testprovider.TestNodeGroup)}
}

func (cp *TestAutoprovisioningCloudProvider) GetStandardZones() ([]string, error) {
	return cp.standardZones, nil
}

func (cp *TestAutoprovisioningCloudProvider) GetAIZones() ([]string, error) {
	return cp.aiZones, nil
}

// TestGkeNodeGroup extends TestNodeGroup with node pool name.
type TestGkeNodeGroup struct {
	*testprovider.TestNodeGroup
	machineConfigProvider *machinetypes.MachineConfigProvider
	nodePoolName          string
	zone                  string
	gpuType               string
	gpuDriverVersion      string
	gpuPartitionSize      string
	gpuMaxSharedClients   string
	gpuSharingStrategy    string
	gpuCount              int64
	extendDurationPods    string
	ppvmCPU               string
	podCapacity           string
	reservationName       string
	reservationProject    string
	reservationAffinity   string
	isAutopilot           bool
	queuedProvisioning    bool
	computeClass          string
	csnLabel              string
	csnSoftTaint          apiv1.Taint
	cccPriorityIdx        string
	isStable              bool
}

func (mig *TestGkeNodeGroup) IsAutopilot() bool {
	return mig.isAutopilot
}

func (mig *TestGkeNodeGroup) QueuedProvisioning() bool {
	return mig.queuedProvisioning
}

// NodePoolName returns the name of the GKE node pool this Mig belongs to.
func (mig *TestGkeNodeGroup) NodePoolName() string {
	return mig.nodePoolName
}

func (mig *TestGkeNodeGroup) Zone() string {
	return mig.zone
}

func (mig *TestGkeNodeGroup) IsStable() (bool, error) {
	return mig.isStable, nil
}

// AutoprovisionedCreate creates the node group on the cloud provider side.
func (mig *TestGkeNodeGroup) AutoprovisionedCreate() (nap_interfaces.CreateNodePoolResult, error) {
	createdNodeGroup, err := mig.Create()
	if err != nil {
		return nap_interfaces.CreateNodePoolResult{}, err
	}
	testGkeNodeGroup := wrapAsTestGkeNodeGroupIfNeeded(createdNodeGroup)
	return nap_interfaces.CreateNodePoolResult{MainCreatedNodeGroup: testGkeNodeGroup}, nil
}

// CreateAsync immediately creates the node group on the cloud provider side and invokes the initializer with the result.
func (mig *TestGkeNodeGroup) CreateAsync(updater interfaces.AsyncNodeGroupUpdater, initializer interfaces.AsyncNodeGroupInitializer) (interfaces.CreateNodePoolResult, error) {
	result, err := mig.AutoprovisionedCreate()
	asyncResult := interfaces.AsyncCreateNodePoolResult{
		InjectedMigs:   []nap_interfaces.AutoprovisionedNodeGroup{mig},
		CreationResult: result,
		Error:          err,
	}
	initializer.InitializeNodeGroup(asyncResult)
	return result, err
}

// DeleteAsync immediately deleted the node group on the cloud provider side and invoked the finalizer.
func (mig *TestGkeNodeGroup) DeleteAsync(finalizer interfaces.AsyncNodeGroupFinalizer) error {
	err := mig.Delete()
	result := interfaces.AsyncDeleteNodePoolResult{
		Migs:  []interfaces.AutoprovisionedNodeGroup{mig},
		Error: err,
	}
	finalizer.FinalizeNodeGroup(result)
	return nil
}

// IsUpcoming checks if node group is upcoming. Always returns false.
func (mig *TestGkeNodeGroup) IsUpcoming() bool {
	return false
}

func (mig *TestGkeNodeGroup) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return mig.machineConfigProvider
}

// TemplateNodeInfo creates a template node info for the node group. Uses predefined templates if they are defined,
// and auto-generates based on the machine type and other TestGkeNodeGroup info if they aren't.
func (mig *TestGkeNodeGroup) TemplateNodeInfo() (*framework.NodeInfo, error) {
	nodeInfo, err := mig.TestNodeGroup.TemplateNodeInfo()
	// ErrNotImplemented is returned if TestNodeGroup doesn't have any explicit node info templates defined.
	if err != cloudprovider.ErrNotImplemented {
		return nodeInfo, err
	}

	// TestNodeGroup.TemplateNodeInfo() has a static mapping of predefined per-machine-type templates for autoprovisioned
	// node groups. Auto-creating node infos from scratch based on the machine type and GPU-related info is implemented here
	// instead, in order to be able to test different GPUs.
	machineTypeInfo, err := mig.MachineConfigProvider().ToMachineType(mig.MachineType())
	machineType := machineTypeInfo.MachineType
	if err != nil {
		return nil, err
	}
	node := test.BuildTestNode(machineType.Name, machineType.CPU*1000, machineType.Memory)

	node.Labels, err = gce.BuildGenericLabels(gce.GceRef{Name: node.Name, Zone: mig.zone}, mig.MachineType(), node.Name, gce.OperatingSystemLinux, gce.Amd64)
	if err != nil {
		return nil, err
	}

	machineFamily, err := gce.GetMachineFamily(machineType.Name)
	if err != nil {
		return nil, err
	}
	node.Labels[gkelabels.MachineFamilyLabel] = machineFamily

	if mig.gpuType != "" {
		node.Labels[gkelabels.GPULabel] = mig.gpuType
		if mig.gpuPartitionSize != "" {
			node.Labels[gkelabels.GPUPartitionSizeLabel] = mig.gpuPartitionSize
		}
		if mig.gpuMaxSharedClients != "" {
			node.Labels[gkelabels.GPUMaxSharedClientsLabel] = mig.gpuMaxSharedClients
		}
		if mig.gpuSharingStrategy != "" {
			node.Labels[gkelabels.GPUSharingStrategyLabel] = mig.gpuSharingStrategy
		}
		if mig.gpuDriverVersion != "" {
			node.Labels[gkelabels.GPUDriverVersionLabel] = mig.gpuDriverVersion
		}
		node.Spec.Taints = append(
			node.Spec.Taints,
			apiv1.Taint{
				Key:    gpu.ResourceNvidiaGPU,
				Value:  "present",
				Effect: "NoSchedule",
			})
		node.Status.Capacity[gpu.ResourceNvidiaGPU] = *resource.NewQuantity(mig.gpuCount, resource.DecimalSI)
		node.Status.Allocatable[gpu.ResourceNvidiaGPU] = *resource.NewQuantity(mig.gpuCount, resource.DecimalSI)
	}

	if mig.extendDurationPods != "" {
		node.Labels[gkelabels.ExtendedDurationPodsLabel] = mig.extendDurationPods
	}
	if mig.ppvmCPU != "" {
		node.Labels[gkelabels.PodPerVMSizeLabel] = mig.ppvmCPU
	}
	if mig.podCapacity != "" {
		node.Labels[gkelabels.PodCapacityLabel] = mig.podCapacity
	}
	if mig.reservationName != "" {
		node.Labels[gkelabels.ReservationNameLabel] = mig.reservationName
	}
	if mig.reservationProject != "" {
		node.Labels[gkelabels.ReservationProjectLabel] = mig.reservationProject
	}
	if mig.reservationAffinity != "" {
		node.Labels[gkelabels.ReservationAffinityLabel] = mig.reservationAffinity
	}
	if mig.computeClass != "" {
		node.Labels[gkelabels.ComputeClassLabel] = mig.computeClass
	}
	if mig.csnLabel != "" {
		node.Labels[csn.SoftWorkloadSeparationKey] = mig.csnLabel
	}
	if mig.cccPriorityIdx != "" {
		node.Labels[gkelabels.ComputeClassPriorityIdxLabel] = mig.cccPriorityIdx
	}

	if mig.csnSoftTaint.Key != "" {
		node.Spec.Taints = append(node.Spec.Taints, mig.csnSoftTaint)
	}

	emptyNodeInfo := framework.NewTestNodeInfo(node)
	return emptyNodeInfo, nil
}

// GkeCloudProviderMock is a mock of the GkeCloudProvider interface.
type GkeCloudProviderMock struct {
	mock.Mock
}

func (m *GkeCloudProviderMock) SetRecommendation(migId string, rec ScaleUpRecommendation) {
	m.Called(migId, rec)
}

func (m *GkeCloudProviderMock) PopRecommendation(migId string) (rec ScaleUpRecommendation, ok bool) {
	args := m.Called(migId)
	return args.Get(0).(ScaleUpRecommendation), args.Bool(1)
}

func (m *GkeCloudProviderMock) ClearRecommendations() {
	m.Called()
}

// Name returns name of the cloud provider.
func (m *GkeCloudProviderMock) Name() string {
	args := m.Called()
	return args.Get(0).(string)
}

// NodeGroups returns all node groups configured for this cloud provider.
func (m *GkeCloudProviderMock) NodeGroups() []cloudprovider.NodeGroup {
	args := m.Called()
	return args.Get(0).([]cloudprovider.NodeGroup)
}

// InjectedNodeGroups returns all node groups injected by NAP in this loop.
func (m *GkeCloudProviderMock) InjectedNodeGroups() []cloudprovider.NodeGroup {
	args := m.Called()
	return args.Get(0).([]cloudprovider.NodeGroup)
}

// NodeGroupForNode returns the node group for the given node.
func (m *GkeCloudProviderMock) NodeGroupForNode(node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	args := m.Called(node)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(cloudprovider.NodeGroup), args.Error(1)
}

// Pricing returns pricing model for this cloud provider or error if not available.
func (m *GkeCloudProviderMock) Pricing() (cloudprovider.PricingModel, errors.AutoscalerError) {
	args := m.Called()
	return args.Get(0).(cloudprovider.PricingModel), args.Get(1).(errors.AutoscalerError)
}

// HasInstance returns whether a given node has a corresponding instance in this cloud provider
func (m *GkeCloudProviderMock) HasInstance(node *apiv1.Node) (bool, error) {
	args := m.Called(node)
	return args.Get(0).(bool), args.Error(1)
}

// GetAvailableMachineTypes get all machine types that can be requested from the cloud provider.
func (m *GkeCloudProviderMock) GetAvailableMachineTypes() ([]string, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

// NewNodeGroup builds a theoretical node group based on the node definition provided.
func (m *GkeCloudProviderMock) NewNodeGroup(machineType string, labels map[string]string, systemLabels map[string]string, taints []apiv1.Taint, extraResources map[string]resource.Quantity) (cloudprovider.NodeGroup, error) {
	args := m.Called(machineType, labels, systemLabels, taints, extraResources)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(cloudprovider.NodeGroup), args.Error(1)
}

// GetResourceLimiter returns struct containing limits (max, min) for resources (cores, memory etc.).
func (m *GkeCloudProviderMock) GetResourceLimiter() (*cloudprovider.ResourceLimiter, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*cloudprovider.ResourceLimiter), args.Error(1)
}

// GPULabel returns the label added to nodes with GPU resource.
func (m *GkeCloudProviderMock) GPULabel() string {
	args := m.Called()
	return args.Get(0).(string)
}

// GetAvailableGPUTypes return all available GPU types cloud provider supports
func (m *GkeCloudProviderMock) GetAvailableGPUTypes() map[string]struct{} {
	args := m.Called()
	return args.Get(0).(map[string]struct{})
}

func (m *GkeCloudProviderMock) GetExperimentsManager() experiments.Manager {
	args := m.Called()
	return args.Get(0).(experiments.Manager)
}

func (m *GkeCloudProviderMock) GetFutureReservationsInProject(projectID string) ([]*gceclient.GceFutureReservation, error) {
	args := m.Called()
	return args.Get(0).([]*gceclient.GceFutureReservation), args.Error(1)
}

// GetReservationBlocksInReservation returns the reservation blocks in the provided project and reservation.
func (m *GkeCloudProviderMock) GetReservationBlocksInReservation(reservationRef gceclient.ReservationRef) ([]*gceclient.GceReservationBlock, error) {
	args := m.Called()
	return args.Get(0).([]*gceclient.GceReservationBlock), args.Error(1)
}

// GetResourcePolicies returns the resource policies in the provided project.
func (m *GkeCloudProviderMock) GetResourcePolicies(projectId string) ([]*gceclient.GceResourcePolicy, error) {
	args := m.Called()
	return args.Get(0).([]*gceclient.GceResourcePolicy), args.Error(1)
}

func (m *GkeCloudProviderMock) GetNodeGpuConfig(node *apiv1.Node) *cloudprovider.GpuConfig {
	args := m.Called(node)
	return args.Get(0).(*cloudprovider.GpuConfig)
}

// Cleanup cleans up all resources before the cloud provider is removed
func (m *GkeCloudProviderMock) Cleanup() error {
	args := m.Called()
	return args.Error(0)
}

// Refresh is called before every main loop and can be used to dynamically update cloud provider state.
func (m *GkeCloudProviderMock) Refresh() error {
	args := m.Called()
	return args.Error(0)
}

// RefreshLocalSSDSizes is called before the main loop if the machine configuration has changed.
func (m *GkeCloudProviderMock) RefreshLocalSSDSizes() {
	m.Called()
}

// Client returns the authenticated GKE http client.
func (m *GkeCloudProviderMock) Client() *http.Client {
	args := m.Called()
	return args.Get(0).(*http.Client)
}

// IsNodeAutoprovisioningEnabled returns true if NAP is enabled.
func (m *GkeCloudProviderMock) IsNodeAutoprovisioningEnabled() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

// GetClusterCreateTime gets the time of cluster creation.
func (m *GkeCloudProviderMock) GetClusterCreateTime() time.Time {
	args := m.Called()
	return args.Get(0).(time.Time)
}

// GetClusterVersion gets the gke / master version of the cluster
func (m *GkeCloudProviderMock) GetClusterVersion() string {
	args := m.Called()
	return args.Get(0).(string)
}

// GetAllZones returns all zones within a region that cluster is running in.
func (m *GkeCloudProviderMock) GetAllZones() ([]string, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

// GetClusterInfo returns the project id, location and cluster name.
func (m *GkeCloudProviderMock) GetClusterInfo() (projectId, location, clusterName string) {
	args := m.Called()
	return args.Get(0).(string), args.Get(1).(string), args.Get(2).(string)
}

// AreConfidentialNodesEnabled checks if ConfidentialNodes are enabled in cluster.
func (m *GkeCloudProviderMock) AreConfidentialNodesEnabled() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

// GetConfidentialInstanceType returns confidential instance type.
func (m *GkeCloudProviderMock) GetConfidentialInstanceType() string {
	args := m.Called()
	return args.Get(0).(string)
}

// GetDefaultNodePoolDiskType returns a default node pool disk type
func (m *GkeCloudProviderMock) GetDefaultNodePoolDiskType() string {
	args := m.Called()
	return args.Get(0).(string)
}

// GetDefaultNodePoolMinCpuPlatform returns a default node pool min cpu platform
func (m *GkeCloudProviderMock) GetDefaultNodePoolMinCpuPlatform() string {
	args := m.Called()
	return args.Get(0).(string)
}

// GetExistingNodeGroupLocations returns a list of locations for created node groups.
func (m *GkeCloudProviderMock) GetExistingNodeGroupLocations() []string {
	args := m.Called()
	return args.Get(0).([]string)
}

// GetAutoprovisioningLocations returns a list of locations where NAP can create new nodepools.
func (m *GkeCloudProviderMock) GetAutoprovisioningLocations() []string {
	args := m.Called()
	return args.Get(0).([]string)
}

// RecommendLocations returns recommendation made by recommendLocations API.
func (m *GkeCloudProviderMock) RecommendLocations(ctx context.Context, request gceclient.RecommendLocationsRequest) (*gceclient.RecommendLocationsResponse, error) {
	args := m.Called(ctx, request)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*gceclient.RecommendLocationsResponse), args.Error(1)
}

// GetMigInstanceTemplateLabels returns instance template labels for MIG.
func (m *GkeCloudProviderMock) GetMigInstanceTemplateLabels(mig *GkeMig) (map[string]string, error) {
	args := m.Called(mig)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]string), args.Error(1)
}

// GetMigInstanceTemplateTaints returns instance template taints for MIG.
func (m *GkeCloudProviderMock) GetMigInstanceTemplateTaints(mig *GkeMig) ([]apiv1.Taint, error) {
	args := m.Called(mig)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]apiv1.Taint), args.Error(1)
}

// GetMigInstanceTemplateSelfLink returns an instance template link for MIG.
func (m *GkeCloudProviderMock) GetMigInstanceTemplateSelfLink(mig *GkeMig) (string, error) {
	args := m.Called(mig)
	return args.Get(0).(string), args.Error(1)
}

// GetGkeMigs returns a list of registered migs in the current snapshot or an error on failure
func (m *GkeCloudProviderMock) GetGkeMigs() []*GkeMig {
	args := m.Called()
	return args.Get(0).([]*GkeMig)
}

// ClusterStarted is a mocked method
func (m *GkeCloudProviderMock) ClusterStarted() (bool, error) {
	args := m.Called()
	return args.Get(0).(bool), args.Error(1)
}

// GkeMigForNode returns a mig in which a given node is running.
func (m *GkeCloudProviderMock) GkeMigForNode(node *apiv1.Node) (*GkeMig, error) {
	args := m.Called(node)
	return args.Get(0).(*GkeMig), args.Error(1)
}

// QueuedProvisioningNodeHasScaleDownImmunity is a mocked method.
func (m *GkeCloudProviderMock) QueuedProvisioningNodeHasScaleDownImmunity(node *apiv1.Node, migSpec *reconciler.QueuedProvisioningMigSpec, now time.Time) bool {
	args := m.Called(node, migSpec, now)
	return args.Get(0).(bool)
}

// IsAutopilotEnabled checks if autopilot is enabled.
func (m *GkeCloudProviderMock) IsAutopilotEnabled() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

func (m *GkeCloudProviderMock) NodeGroupsInNodePool(nodePoolName string) []cloudprovider.NodeGroup {
	args := m.Called(nodePoolName)
	return args.Get(0).([]cloudprovider.NodeGroup)
}

func (m *GkeCloudProviderMock) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return machinetypes.NewMachineConfigProvider(nil)
}

// used in FakeGkeManager to call real methods where possible
var (
	testGkeManager = gkeManagerImpl{}
)

// SuspensionStatus contains the information about the state of GCE instances.
// This state can be modified by `SuspendInstances` and `ResumeInstances` calls.
type SuspensionStatus struct {
	// Suspended is true when a `suspend` call was made for an instance
	Suspended bool
	// ForceUsed is true when an instance was suspended forcefully
	// Suspended==false and ForceUsed==true should not happen.
	ForceUsed bool
}

// suspension key contains the information about GCE instances which is
// necessary to perform suspend/resume operations.
type suspensionKey struct {
	InstanceRef gce.GceRef
	MigRef      gce.GceRef
}

type FakeGkeManager struct {
	zones                             []string
	dataplaneV2Enabled                bool
	migTemplateNode                   *framework.NodeInfo
	injectedMigs                      map[string]*GkeMig
	isUpcoming                        bool
	instances                         []gce.GceInstance
	resizableVmInAutopilotEnabled     map[string]bool
	resizableVmWithinPodFamilyEnabled map[string]bool
	isArmMachineFallbacksEnabled      bool
	extendedFallbacksEnabled          bool

	suspensionStatuses    map[suspensionKey]SuspensionStatus
	machineConfigProvider *machinetypes.MachineConfigProvider
}

func NewFakeGkeManager(zones []string) *FakeGkeManager {
	return &FakeGkeManager{
		zones: zones,
	}
}

func (fake *FakeGkeManager) IsNodeAutoprovisioningEnabled() bool {
	panic("not implemented")
}

func (fake *FakeGkeManager) UseAutoprovisioningFeaturesForPodRequirements(req *podrequirements.Requirements) bool {
	panic("not implemented")
}

func (fake *FakeGkeManager) UseAutoprovisioningFeaturesForNodeGroup(nodeGroup cloudprovider.NodeGroup) bool {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetAutoprovisioningDefaultFamily() machinetypes.MachineFamily {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetMigSize(mig gce.Mig) (int64, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) SetMigSize(mig gce.Mig, size int64) error {
	panic("not implemented")
}

func (fake *FakeGkeManager) IsMigStable(mig gce.Mig) (bool, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) DeleteInstances(instances []gce.GceRef) error {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetMigForInstance(instance gce.GceRef) (gce.Mig, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetMigNodes(mig gce.Mig) ([]gce.GceInstance, error) {
	return fake.instances, nil
}

func (fake *FakeGkeManager) Refresh() error {
	panic("not implemented")
}

func (fake *FakeGkeManager) RefreshLocalSSDSizes() {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetResourceLimiter(n NodeGroupFromNode) (*cloudprovider.ResourceLimiter, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) Cleanup() error {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetGkeMigs() []*GkeMig {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetGkeMigsBlockedByServerError() []*GkeMig {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetGkeMigsBlockedByNotFoundError() []*GkeMig {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetAllNodePoolNames() sets.Set[string] {
	panic("not implemented")
}

func (fake *FakeGkeManager) CreateNodePool(mig *GkeMig) (MigCreateNodePoolResult, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) CreateNodePoolAsync(mig *GkeMig, updater interfaces.AsyncNodeGroupUpdater, initializer interfaces.AsyncNodeGroupInitializer) (MigCreateNodePoolResult, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) IsUpcoming(mig *GkeMig) bool {
	return fake.isUpcoming
}

func (fake *FakeGkeManager) DeleteNodePool(toBeRemoved *GkeMig) error {
	panic("not implemented")
}

func (fake *FakeGkeManager) DeleteNodePoolAsync(mig *GkeMig, finalizer interfaces.AsyncNodeGroupFinalizer) error {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetMigsTargetSize(migs []gce.GceRef) (int64, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetLocation() string {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetProjectId() string {
	return "project-id"
}

func (fake *FakeGkeManager) GetReleaseChannel() string {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetClusterName() string {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetClusterVersion() string {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetClusterNetwork() (*gce_api.Network, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) RecommendLocations(ctx context.Context, region string, request gceclient.RecommendLocationsRequest) (*gceclient.RecommendLocationsResponse, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) FetchCapacityGuidance(ctx context.Context, flexibilityScopeKey string, instanceConfigs map[string]*api.InstanceConfig) (map[string]*api.InstanceAvailability, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) SendCapacityDecision(ctx context.Context, decision api.ProvisioningDecisionNotification) error {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetReservationsInProject(projectID string) ([]*gce_api.Reservation, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetFutureReservationsInProject(projectID string) ([]*gceclient.GceFutureReservation, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetReservationBlocksInReservation(reservationRef gceclient.ReservationRef) ([]*gceclient.GceReservationBlock, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetReservationSubBlocksInReservationBlock(reservationRef gceclient.ReservationRef) ([]*gceclient.GceReservationSubBlock, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetResourcePolicies(projectId, region string) ([]*gceclient.GceResourcePolicy, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetZonesInRegion(region string) ([]string, error) {
	panic("not implemented")
}

func (m *FakeGkeManager) GetStandardZonesInRegion(region string) ([]string, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetAIZonesInRegion(region string) ([]string, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetMigInstanceTemplate(mig *GkeMig) (*gce_api.InstanceTemplate, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetMigKubeEnv(mig *GkeMig) (gce.KubeEnv, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetMigTemplateNodeInfo(mig *GkeMig) (*framework.NodeInfo, error) {
	if fake.migTemplateNode != nil {
		return fake.migTemplateNode, nil
	}
	return framework.NewTestNodeInfo(&apiv1.Node{}), nil
}

func (fake *FakeGkeManager) GetExistingNodeGroupLocations() []string {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetAutoprovisioningLocations() []string {
	return fake.zones
}

func (fake *FakeGkeManager) Client() *http.Client {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetMachineType(machineType string, zone string) (gce.MachineType, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetNumberOfSurgeNodesInMig(mig *GkeMig) int {
	return 0
}

func (fake *FakeGkeManager) AreConfidentialNodesEnabled() bool {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetConfidentialInstanceType() string {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetDefaultNodePoolDiskType() string {
	return "pd-balanced"
}

func (fake *FakeGkeManager) GetDefaultNodePoolMinCpuPlatform() string {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetDefaultNodePoolDiskSizeGB() int64 {
	return 100
}

func (fake *FakeGkeManager) GetImageTypeForNap(mig *GkeMig) string {
	return "cos_containerd"
}

func (fake *FakeGkeManager) GetOsDistributionForNap(mig *GkeMig) gce.OperatingSystemDistribution {
	return gce.OperatingSystemDistributionUbuntu
}

func (fake *FakeGkeManager) GetNewNodePoolDaemonSetConditions() *DaemonSetConditions {
	panic("not implemented")
}

func (fake *FakeGkeManager) CreateInstances(mig gce.Mig, delta int64) error {
	panic("not implemented")
}

func (fake *FakeGkeManager) CreateFlexResizeRequests(mig gce.Mig, delta int64) error {
	panic("not implemented")
}

func (fake *FakeGkeManager) CreateResizeRequest(mig gce.Mig, delta int64) error {
	panic("not implemented")
}

func (fake *FakeGkeManager) AdvanceResizeRequestCleanUp(resizeRequest resizerequestclient.ResizeRequestStatus) error {
	panic("not implemented")
}

func (fake *FakeGkeManager) ReportState(resizeRequest resizerequestclient.ResizeRequestStatus) resizerequestclient.ResizeRequestReportState {
	panic("not implemented")
}

func (fake *FakeGkeManager) SetReportState(resizeRequest resizerequestclient.ResizeRequestStatus, state resizerequestclient.ResizeRequestReportState) {
	panic("not implemented")
}

func (fake *FakeGkeManager) ResetFailedResizeRequestsCreation(migRef gce.GceRef) map[error]int {
	panic("not implemented")
}

func (fake *FakeGkeManager) IsResizeRequestErrorHandlingEnabled() bool {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetClusterCreateTime() time.Time {
	panic("not implemented")
}

func (fake *FakeGkeManager) ClusterStarted() (bool, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) IsClusterUsingPSCInfrastructure() bool {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetDefaultEnablePrivateNodes() bool {
	panic("not implemented")
}

func (fake *FakeGkeManager) IsDataplaneV2Enabled() bool {
	return fake.dataplaneV2Enabled
}

func (fake *FakeGkeManager) RegisterInitializationFunc(f InitializationFunc) {
	panic("not implemented")
}

func (fake *FakeGkeManager) ResizeRequests(mig *GkeMig) ([]resizerequestclient.ResizeRequestStatus, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) ResizeVm(context.Context, gce.GceRef, ekvmsize.VmSize) error {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetCurrentResizableVmState(_ *machinetypes.MachineConfigProvider, instance gce.GceRef) (ekvmtypes.ResizableVmState, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) BulkFetchCurrentResizableVmStates(_ *machinetypes.MachineConfigProvider) (map[gce.GceRef]ekvmtypes.ResizableVmState, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) QueuedProvisioningNodeHasScaleDownImmunity(node *apiv1.Node, migSpec *reconciler.QueuedProvisioningMigSpec, now time.Time) bool {
	panic("not implemented")
}

func (fake *FakeGkeManager) InstanceByRef(ref gce.GceRef) *gce.GceInstance {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetListManagedInstancesResults(migRef gce.GceRef) (string, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) IsDefaultCCCEnabled() bool {
	panic("not implemented")
}

func (fake *FakeGkeManager) CalculatePhysicalEphemeralStorageGiB(mig *GkeMig, allocatableBytes int64) int64 {
	panic("not implemented")
}

func (fake *FakeGkeManager) ScaleDownUnreadyTimeOverride(mig *GkeMig) (time.Duration, bool) {
	panic("not implemented")
}

func (fake *FakeGkeManager) ScaleDownUnneededTimeOverride(cloudprovider.NodeGroup) (time.Duration, bool, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) ScaleDownUtilizationThresholdOverride(cloudprovider.NodeGroup) (float64, bool, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) ScaleDownGpuUtilizationThresholdOverride(cloudprovider.NodeGroup) (float64, bool, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) ValidateLocationForDiskType(location string, requestedDiskType string) (ok bool, reason string, err error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) ResizingEnabled(machineFamily string) bool {
	panic("not implemented")
}

func (fake *FakeGkeManager) IsResizableVmEnabledInAutopilot(machineFamily string) bool {
	return fake.resizableVmInAutopilotEnabled[machineFamily]
}

func (fake *FakeGkeManager) IsEkEdpEnabled() bool {
	panic("not implemented")
}

func (fake *FakeGkeManager) IsResizableVmWithinPodFamilyEnabled(machineFamily string) bool {
	return fake.resizableVmWithinPodFamilyEnabled[machineFamily]
}

func (fake *FakeGkeManager) IsExtendedFallbacksEnabled() bool {
	return fake.extendedFallbacksEnabled
}

// SetExtendedFallbacksEnabled sets the extendedFallbacksEnabled field.
func (fake *FakeGkeManager) SetExtendedFallbacksEnabled(enabled bool) {
	fake.extendedFallbacksEnabled = enabled
}

func (fake *FakeGkeManager) IsEkSpotEnabled() bool {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetNodesScaleDownAllowedFromCache([]string) map[string]bool {
	panic("not implemented")
}

func (fake *FakeGkeManager) UpdateNodesScaleDownAllowedCache(map[string]bool) {
	panic("not implemented")
}

func (fake *FakeGkeManager) InvalidateNodesScaleDownAllowedCache() {
	panic("not implemented")
}

func (fake *FakeGkeManager) IsArmMachineFallbacksEnabled() bool {
	return fake.isArmMachineFallbacksEnabled
}
func (fake *FakeGkeManager) GetInjectedMig(mig *GkeMig) *GkeMig {
	if fake.injectedMigs == nil {
		return nil
	}
	return fake.injectedMigs[mig.Id()]
}

func (fake *FakeGkeManager) SetInjectedMig(real, injected *GkeMig) {
	if fake.injectedMigs == nil {
		fake.injectedMigs = map[string]*GkeMig{}
	}
	fake.injectedMigs[real.Id()] = injected
}

func (fake *FakeGkeManager) EvaluateCapacityCheckWaitTimeSeconds(mig *GkeMig) (time.Duration, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) CapacityCheckWaitTimeSeconds(mig *GkeMig) (time.Duration, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetMaxNodeProvisioningTimeOverride(mig *GkeMig) (time.Duration, bool) {
	panic("not implemented")
}

func (fake *FakeGkeManager) SetScaleUpTimeProvider(provider ScaleUpTimeProvider) {
	panic("not implemented")
}

func (fake *FakeGkeManager) SetReservationsPuller(*gceclient.ReservationsPuller) {
	panic("not implemented")
}

func (fake *FakeGkeManager) ValidateMachineTypeConfig(machineType, zone string) error {
	panic("not implemented")
}

func (fake *FakeGkeManager) CreateQueuedInstances(pr prpods.ProvReqID, mig *GkeMig, delta int64, shouldUpdateProvReqDetails manager.ShouldUpdateProvReqDetails) error {
	panic("not implemented")
}

func (fake *FakeGkeManager) UpdateNodePoolLabels(nodePoolName string, labels map[string]string) error {
	panic("not implemented")
}

func (fake *FakeGkeManager) ValidateGpuConfig(gpuType, gpuPartitionSize, gpuMaxSharedClients, gpuSharingStrategy, machineType string, gpuCount int64, zone string, cpus, mem int64) error {
	panic("not implemented")
}

func (fake *FakeGkeManager) ExistingMigsInNodePool(nodePoolName string) []*GkeMig {
	panic("not implemented")
}

func (fake *FakeGkeManager) GetDeploymentType(gceRef gce.GceRef, spec *gkeclient.NodePoolSpec) DeploymentTypeEnum {
	return testGkeManager.GetDeploymentType(gceRef, spec)
}

func (fake *FakeGkeManager) GetBasenameForMig(mig *GkeMig) (string, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) NodePoolSpecForNode(node *apiv1.Node) (*gkeclient.NodePoolSpec, error) {
	panic("not implemented")
}

func (fake *FakeGkeManager) ResumeInstances(mig gce.GceRef, instances []gce.GceRef) error {
	if fake.suspensionStatuses == nil {
		return nil
	}
	for _, instRef := range instances {
		key := suspensionKey{InstanceRef: instRef, MigRef: mig}
		if _, ok := fake.suspensionStatuses[key]; !ok {
			continue
		}
		fake.suspensionStatuses[key] = SuspensionStatus{}
	}
	return nil
}

func (fake *FakeGkeManager) SuspendInstances(mig gce.GceRef, instances []gce.GceRef, forceSuspend bool) error {
	if fake.suspensionStatuses == nil {
		fake.suspensionStatuses = make(map[suspensionKey]SuspensionStatus)
	}

	for _, instRef := range instances {
		fake.suspensionStatuses[suspensionKey{MigRef: mig, InstanceRef: instRef}] = SuspensionStatus{Suspended: true, ForceUsed: forceSuspend}
	}
	return nil
}

func (fake *FakeGkeManager) GetSuspensionStatus(mig gce.GceRef, instance gce.GceRef) SuspensionStatus {
	if fake.suspensionStatuses == nil {
		return SuspensionStatus{}
	}
	instStatus, ok := fake.suspensionStatuses[suspensionKey{MigRef: mig, InstanceRef: instance}]
	if !ok {
		return SuspensionStatus{}
	}
	return instStatus
}

func (fake *FakeGkeManager) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return fake.machineConfigProvider
}

func (fake *FakeGkeManager) ExperimentsManager() experiments.Manager {
	return experiments.NewMockManager()
}

func (fake *FakeGkeManager) TrimLocationsForMachineConfig(locations []string, machineType string, acceleratorConfig *gke_api_beta.AcceleratorConfig, minCpuPlatform string, diskType string) []string {
	panic("not implemented")
}

// FakeGkeManagerBuilder is a builder for FakeGkeManager.
type FakeGkeManagerBuilder struct {
	zones                             []string
	dataplaneV2Enabled                bool
	migTemplateNode                   *framework.NodeInfo
	injectedMigs                      map[string]*GkeMig
	isUpcoming                        bool
	instances                         []gce.GceInstance
	machineConfigProvider             *machinetypes.MachineConfigProvider
	resizableVmInAutopilotEnabled     map[string]bool
	resizableVmWithinPodFamilyEnabled map[string]bool
}

// NewFakeGkeManagerBuilder returns a new FakeGkeManagerBuilder.
func NewFakeGkeManagerBuilder() *FakeGkeManagerBuilder {
	return &FakeGkeManagerBuilder{
		injectedMigs:                      map[string]*GkeMig{},
		machineConfigProvider:             machinetypes.NewMachineConfigProvider(nil),
		resizableVmInAutopilotEnabled:     make(map[string]bool),
		resizableVmWithinPodFamilyEnabled: make(map[string]bool),
	}
}

// WithZones sets the zones for the FakeGkeManager.
func (b *FakeGkeManagerBuilder) WithZones(zones []string) *FakeGkeManagerBuilder {
	b.zones = zones
	return b
}

// WithInstances sets the instances for the FakeGkeManager.
func (b *FakeGkeManagerBuilder) WithInstances(instances []gce.GceInstance) *FakeGkeManagerBuilder {
	b.instances = instances
	return b
}

// WithDataplaneV2Enabled sets whether dataplane v2 is enabled.
func (b *FakeGkeManagerBuilder) WithDataplaneV2Enabled(enabled bool) *FakeGkeManagerBuilder {
	b.dataplaneV2Enabled = enabled
	return b
}

// WithMigTemplateNode sets the template node for the FakeGkeManager.
func (b *FakeGkeManagerBuilder) WithMigTemplateNode(node *apiv1.Node) *FakeGkeManagerBuilder {
	b.migTemplateNode = framework.NewTestNodeInfo(node)
	return b
}

// WithInjectedMigs sets the injected migs for the FakeGkeManager.
func (b *FakeGkeManagerBuilder) WithInjectedMigs(injectedMigs map[string]*GkeMig) *FakeGkeManagerBuilder {
	b.injectedMigs = injectedMigs
	return b
}

// WithIsUpcoming sets the isUpcoming flag for the FakeGkeManager.
func (b *FakeGkeManagerBuilder) WithIsUpcoming(isUpcoming bool) *FakeGkeManagerBuilder {
	b.isUpcoming = isUpcoming
	return b
}

func (b *FakeGkeManagerBuilder) WithResizableVmInAutopilotEnabled(machineFamily string, enabled bool) *FakeGkeManagerBuilder {
	b.resizableVmInAutopilotEnabled[machineFamily] = enabled
	return b
}

func (b *FakeGkeManagerBuilder) WithResizableVmWithinPodFamilyEnabled(machineFamily string, enabled bool) *FakeGkeManagerBuilder {
	b.resizableVmWithinPodFamilyEnabled[machineFamily] = enabled
	return b
}

// Build creates a new FakeGkeManager.
func (b *FakeGkeManagerBuilder) Build() *FakeGkeManager {
	return &FakeGkeManager{
		zones:                             b.zones,
		dataplaneV2Enabled:                b.dataplaneV2Enabled,
		migTemplateNode:                   b.migTemplateNode,
		isUpcoming:                        b.isUpcoming,
		injectedMigs:                      b.injectedMigs,
		machineConfigProvider:             b.machineConfigProvider,
		resizableVmInAutopilotEnabled:     b.resizableVmInAutopilotEnabled,
		resizableVmWithinPodFamilyEnabled: b.resizableVmWithinPodFamilyEnabled,
	}
}

// DEPRECATED. please use FakeGkeManager instead and extend it as needed.
// GkeManagerMock is a mock of the GkeManager interface.
type GkeManagerMock struct {
	mock.Mock

	haveAutoscalingOptsOverrides bool
	MockIsUpcoming               bool
	InjectedMigs                 map[string]*GkeMig
	ResReqReportState            map[string]resizerequestclient.ResizeRequestReportState
	m                            sync.Mutex
}

// ExperimentsManager is a mocked method.
func (m *GkeManagerMock) ExperimentsManager() experiments.Manager {
	for _, call := range m.ExpectedCalls {
		if call.Method == "ExperimentsManager" {
			args := m.Called()
			return args.Get(0).(experiments.Manager)
		}
	}
	return experiments.NewMockManager()
}

// IsResizableVmEnabledInAutopilot is a mocked method.
func (m *GkeManagerMock) IsResizableVmEnabledInAutopilot(machineFamily string) bool {
	args := m.Called(machineFamily)
	return args.Get(0).(bool)
}

// IsResizableVmWithinPodFamilyEnabled is a mocked method.
func (m *GkeManagerMock) IsResizableVmWithinPodFamilyEnabled(machineFamily string) bool {
	args := m.Called(machineFamily)
	return args.Get(0).(bool)
}

// IsExtendedFallbacksEnabled is a mocked method.
func (m *GkeManagerMock) IsExtendedFallbacksEnabled() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

// IsMigStable returns whether the MIG is stable.
func (m *GkeManagerMock) IsMigStable(mig gce.Mig) (bool, error) {
	args := m.Called(mig)
	return args.Bool(0), args.Error(1)
}

// IsArmMachineFallbacksEnabled is a mocked method.
func (m *GkeManagerMock) IsArmMachineFallbacksEnabled() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

// SetScaleUpTimeProvider is a mocked method
func (m *GkeManagerMock) SetScaleUpTimeProvider(provider ScaleUpTimeProvider) {
	// no-op
}

// SetReservationsPuller is a mocked method
func (m *GkeManagerMock) SetReservationsPuller(reservationsPuller *gceclient.ReservationsPuller) {
	// no-op
}

// GetCluster is a mocked method
func (m *GkeManagerMock) GetCluster() (gkeclient.Cluster, error) {
	args := m.Called()
	return args.Get(0).(gkeclient.Cluster), args.Error(1)
}

// NewNodePoolSpec is a mocked method
func (m *GkeManagerMock) NewNodePoolSpec(mig *GkeMig) (*gkeclient.NodePoolSpec, error) {
	args := m.Called(mig)
	return args.Get(0).(*gkeclient.NodePoolSpec), args.Error(1)
}

// CreateNodePoolNoRefresh is a mocked method
func (m *GkeManagerMock) CreateNodePoolNoRefresh(nodePoolName string, nodePoolSpec *gkeclient.NodePoolSpec) error {
	args := m.Called(nodePoolName, nodePoolSpec)
	return args.Error(0)
}

// DeleteNodePoolNoRefresh is a mocked method
func (m *GkeManagerMock) DeleteNodePoolNoRefresh(mig *GkeMig) error {
	args := m.Called(mig)
	return args.Error(0)
}

// CleanUpBrokenNodePool is a mocked method
func (m *GkeManagerMock) CleanUpBrokenNodePool(name string) {
	m.Called(name)
}

// ClusterStarted is a mocked method
func (m *GkeManagerMock) ClusterStarted() (bool, error) {
	args := m.Called()
	return args.Get(0).(bool), args.Error(1)
}

// GetClusterCreateTime is a mocked method.
func (m *GkeManagerMock) GetClusterCreateTime() time.Time {
	args := m.Called()
	return args.Get(0).(time.Time)
}

// GetMigSize is a mocked method.
func (m *GkeManagerMock) GetMigSize(mig gce.Mig) (int64, error) {
	args := m.Called(mig)
	return args.Get(0).(int64), args.Error(1)
}

// SetMigSize is a mocked method.
func (m *GkeManagerMock) SetMigSize(mig gce.Mig, size int64) error {
	args := m.Called(mig, size)
	return args.Error(0)
}

// DeleteInstances is a mocked method.
func (m *GkeManagerMock) DeleteInstances(instances []gce.GceRef) error {
	args := m.Called(instances)
	return args.Error(0)
}

// GetMigForInstance is a mocked method.
func (m *GkeManagerMock) GetMigForInstance(instance gce.GceRef) (migRet gce.Mig, errRet error) {
	args := m.Called(instance)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	// based on https://github.com/stretchr/testify/issues/350#issuecomment-1173347793
	switch v := args.Get(0).(type) {
	case nil:
		migRet = nil
	case *GkeMig:
		migRet = v
	case func(gce.GceRef) *GkeMig:
		migRet = v(instance)
	default:
		panic(fmt.Sprintf("unexpected type %T", v))
	}

	// based on https://github.com/stretchr/testify/issues/350#issuecomment-1173347793
	switch v := args.Get(1).(type) {
	case nil:
		errRet = nil
	case error:
		errRet = v
	case func(gce.GceRef) error:
		errRet = v(instance)
	default:
		panic(fmt.Sprintf("unexpected type %T", v))
	}

	return
}

// GetMigNodes is a mocked method.
func (m *GkeManagerMock) GetMigNodes(mig gce.Mig) ([]gce.GceInstance, error) {
	args := m.Called(mig)
	return args.Get(0).([]gce.GceInstance), args.Error(1)
}

// GetAllNodePoolNames is a mocked method
func (m *GkeManagerMock) GetAllNodePoolNames() sets.Set[string] {
	_ = m.Called()
	return sets.New[string]()
}

// Refresh is a mocked method.
func (m *GkeManagerMock) Refresh() error {
	args := m.Called()
	return args.Error(0)
}

// RefreshForce refreshes cluster resources.
func (m *GkeManagerMock) RefreshForce() error {
	args := m.Called()
	return args.Error(0)
}

// RefreshLocalSSDSizes refreshes local SSD sizes.
func (m *GkeManagerMock) RefreshLocalSSDSizes() {
	m.Called()
}

// RegisterInitializationFunc is a mocked method.
func (m *GkeManagerMock) RegisterInitializationFunc(f InitializationFunc) {
	m.Called()
}

// ResizeRequests is a mocked method.
func (m *GkeManagerMock) ResizeRequests(mig *GkeMig) ([]resizerequestclient.ResizeRequestStatus, error) {
	args := m.Called(mig)
	return args.Get(0).([]resizerequestclient.ResizeRequestStatus), args.Error(1)
}

// Cleanup is a mocked method.
func (m *GkeManagerMock) Cleanup() error {
	args := m.Called()
	return args.Error(0)
}

// GetGkeMigs is a mocked method.
func (m *GkeManagerMock) GetGkeMigs() []*GkeMig {
	args := m.Called()
	return args.Get(0).([]*GkeMig)
}

// GetGkeMigsBlockedByServerError is a mocked method.
func (m *GkeManagerMock) GetGkeMigsBlockedByServerError() []*GkeMig {
	args := m.Called()
	return args.Get(0).([]*GkeMig)
}

// GetGkeMigsBlockedByNotFoundError is a mocked method.
func (m *GkeManagerMock) GetGkeMigsBlockedByNotFoundError() []*GkeMig {
	args := m.Called()
	return args.Get(0).([]*GkeMig)
}

// CreateNodePool is a mocked method.
func (m *GkeManagerMock) CreateNodePool(mig *GkeMig) (MigCreateNodePoolResult, error) {
	args := m.Called(mig)
	result := MigCreateNodePoolResult{
		MainCreatedMig:   mig,
		ExtraCreatedMigs: []*GkeMig{},
	}
	if len(args) > 1 {
		if migs, ok := args.Get(1).([]*GkeMig); ok {
			result.ExtraCreatedMigs = migs
		}
	}
	return result, args.Error(0)
}

// CreateNodePoolAsync is a mocked method.
func (m *GkeManagerMock) CreateNodePoolAsync(mig *GkeMig, updater nap_interfaces.AsyncNodeGroupUpdater, intializer nap_interfaces.AsyncNodeGroupInitializer) (MigCreateNodePoolResult, error) {
	args := m.Called(mig)
	result := MigCreateNodePoolResult{
		MainCreatedMig:   mig,
		ExtraCreatedMigs: []*GkeMig{},
	}
	return result, args.Error(0)
}

// IsUpcoming is a mocked method.
func (m *GkeManagerMock) IsUpcoming(mig *GkeMig) bool {
	if m.MockIsUpcoming {
		args := m.Called(mig)
		return args.Get(0).(bool)
	}
	return false
}

// DeleteNodePool is a mocked method.
func (m *GkeManagerMock) DeleteNodePool(toBeRemoved *GkeMig) error {
	args := m.Called(toBeRemoved)
	return args.Error(0)
}

// DeleteNodePoolAsync is a mocked method.
func (m *GkeManagerMock) DeleteNodePoolAsync(toBeRemoved *GkeMig, finalizer nap_interfaces.AsyncNodeGroupFinalizer) error {
	args := m.Called(toBeRemoved)
	return args.Error(0)
}

// GetMigsTargetSize is a mocked method.
func (m *GkeManagerMock) GetMigsTargetSize(migs []gce.GceRef) (int64, error) {
	args := m.Called(migs)
	return args.Get(0).(int64), args.Error(1)
}

// GetLocation is a mocked method.
func (m *GkeManagerMock) GetLocation() string {
	args := m.Called()
	return args.String(0)
}

// GetProjectId is a mocked method.
func (m *GkeManagerMock) GetProjectId() string {
	args := m.Called()
	return args.String(0)
}

// GetClusterName is a mocked method.
func (m *GkeManagerMock) GetClusterName() string {
	args := m.Called()
	return args.String(0)
}

// GetClusterVersion is a mocked method.
func (m *GkeManagerMock) GetClusterVersion() string {
	args := m.Called()
	return args.String(0)
}

// GetClusterNetwork is a mocked method.
func (m *GkeManagerMock) GetClusterNetwork() (*gce_api.Network, error) {
	args := m.Called()
	return args.Get(0).(*gce_api.Network), args.Error(1)
}

// GetReleaseChannel is a mocked method.
func (m *GkeManagerMock) GetReleaseChannel() string {
	args := m.Called()
	return args.String(0)
}

// RecommendLocations is a mocked method.
func (m *GkeManagerMock) RecommendLocations(ctx context.Context, region string, request gceclient.RecommendLocationsRequest) (*gceclient.RecommendLocationsResponse, error) {
	args := m.Called(ctx, region, request)
	return args.Get(0).(*gceclient.RecommendLocationsResponse), args.Error(1)
}

// FetchCapacityGuidance is a mocked method.
func (m *GkeManagerMock) FetchCapacityGuidance(ctx context.Context, flexibilityScopeKey string, instanceConfigs map[string]*api.InstanceConfig) (map[string]*api.InstanceAvailability, error) {
	args := m.Called(ctx, flexibilityScopeKey, instanceConfigs)
	return args.Get(0).(map[string]*api.InstanceAvailability), args.Error(1)
}

// SendCapacityDecision is a mocked method.
func (m *GkeManagerMock) SendCapacityDecision(ctx context.Context, decision api.ProvisioningDecisionNotification) error {
	args := m.Called(ctx, decision)
	return args.Error(0)
}

func (m *GkeManagerMock) GetFutureReservationsInProject(projectID string) ([]*gceclient.GceFutureReservation, error) {
	args := m.Called()
	return args.Get(0).([]*gceclient.GceFutureReservation), args.Error(1)
}

// GetReservationBlocksInReservation is a mocked method.
func (m *GkeManagerMock) GetReservationBlocksInReservation(reservationRef gceclient.ReservationRef) ([]*gceclient.GceReservationBlock, error) {
	args := m.Called()
	return args.Get(0).([]*gceclient.GceReservationBlock), args.Error(1)
}

func (m *GkeManagerMock) GetReservationSubBlocksInReservationBlock(reservationRef gceclient.ReservationRef) ([]*gceclient.GceReservationSubBlock, error) {
	args := m.Called()
	return args.Get(0).([]*gceclient.GceReservationSubBlock), args.Error(1)
}

// GetResourcePolicies returns the resource policies in the provided project and region.
func (m *GkeManagerMock) GetResourcePolicies(projectId, region string) ([]*gceclient.GceResourcePolicy, error) {
	args := m.Called()
	return args.Get(0).([]*gceclient.GceResourcePolicy), args.Error(1)
}

// GetZonesInRegion is a mocked method.
func (m *GkeManagerMock) GetZonesInRegion(region string) ([]string, error) {
	args := m.Called(region)
	return args.Get(0).([]string), args.Error(1)
}

func (m *GkeManagerMock) GetStandardZonesInRegion(region string) ([]string, error) {
	panic("not implemented")
}

// GetAIZonesInRegion is a mocked method.
func (m *GkeManagerMock) GetAIZonesInRegion(region string) ([]string, error) {
	args := m.Called(region)
	return args.Get(0).([]string), args.Error(1)
}

// GetMigInstanceTemplate is a mocked method.
func (m *GkeManagerMock) GetMigInstanceTemplate(mig *GkeMig) (*gce_api.InstanceTemplate, error) {
	args := m.Called(mig)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*gce_api.InstanceTemplate), args.Error(1)
}

// GetMigKubeEnv is a mocked method.
func (m *GkeManagerMock) GetMigKubeEnv(mig *GkeMig) (gce.KubeEnv, error) {
	args := m.Called(mig)
	if args.Get(0) == nil {
		return gce.KubeEnv{}, args.Error(1)
	}
	return args.Get(0).(gce.KubeEnv), args.Error(1)
}

// GetResourceLimiter is a mocked method.
func (m *GkeManagerMock) GetResourceLimiter(ngFromNode NodeGroupFromNode) (*cloudprovider.ResourceLimiter, error) {
	args := m.Called()
	return args.Get(0).(*cloudprovider.ResourceLimiter), args.Error(1)
}

// GetNumberOfSurgeNodesInMig is a mocked method.
func (m *GkeManagerMock) GetNumberOfSurgeNodesInMig(mig *GkeMig) int {
	args := m.Called(mig)
	return args.Get(0).(int)
}

// GetMigTemplateNodeInfo is a mocked method.
func (m *GkeManagerMock) GetMigTemplateNodeInfo(mig *GkeMig) (*framework.NodeInfo, error) {
	args := m.Called(mig)
	return args.Get(0).(*framework.NodeInfo), args.Error(1)
}

// GetMachineType is a mocked method.
func (m *GkeManagerMock) GetMachineType(machineType string, zone string) (machine gce.MachineType, err error) {
	args := m.Called(machineType, zone)
	return args.Get(0).(gce.MachineType), args.Error(1)
}

func (m *GkeManagerMock) NodePoolSpecForNode(node *apiv1.Node) (*gkeclient.NodePoolSpec, error) {
	args := m.Called(node)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*gkeclient.NodePoolSpec), args.Error(1)
}

// IsNodeAutoprovisioningEnabled is a mocked method.
func (m *GkeManagerMock) IsNodeAutoprovisioningEnabled() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

// UseAutoprovisioningFeaturesForPodRequirements is a mocked method.
func (m *GkeManagerMock) UseAutoprovisioningFeaturesForPodRequirements(req *podrequirements.Requirements) bool {
	args := m.Called(req)
	return args.Get(0).(bool)
}

// UseAutoprovisioningFeaturesForNodeGroup is a mocked method.
func (m *GkeManagerMock) UseAutoprovisioningFeaturesForNodeGroup(nodeGroup cloudprovider.NodeGroup) bool {
	args := m.Called(nodeGroup)
	return args.Get(0).(bool)
}

func (m *GkeManagerMock) GetAutoprovisioningDefaultFamily() machinetypes.MachineFamily {
	args := m.Called()
	return args.Get(0).(machinetypes.MachineFamily)
}

// AreConfidentialNodesEnabled is a mocked method
func (m *GkeManagerMock) AreConfidentialNodesEnabled() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

// GetConfidentialInstanceType is a mocked method
func (m *GkeManagerMock) GetConfidentialInstanceType() string {
	args := m.Called()
	return args.Get(0).(string)
}

// IsClusterUsingPSCInfrastructure is a mocked method
func (m *GkeManagerMock) IsClusterUsingPSCInfrastructure() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

// GetDefaultEnablePrivateNodes is a mocked method
func (m *GkeManagerMock) GetDefaultEnablePrivateNodes() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

// GetNewNodePoolDaemonSetConditions returns the flags for gke daemon sets
func (m *GkeManagerMock) GetNewNodePoolDaemonSetConditions() *DaemonSetConditions {
	args := m.Called()
	return args.Get(0).(*DaemonSetConditions)
}

// GetDefaultNodePoolDiskType returns a default node pool disk type
func (m *GkeManagerMock) GetDefaultNodePoolDiskType() string {
	args := m.Called()
	return args.Get(0).(string)
}

// GetDefaultNodePoolDiskSizeGB returns a default node pool disk size GiB
func (m *GkeManagerMock) GetDefaultNodePoolDiskSizeGB() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}

// GetDefaultNodePoolMinCpuPlatform returns a default node pool min cpu platform
func (m *GkeManagerMock) GetDefaultNodePoolMinCpuPlatform() string {
	args := m.Called()
	return args.Get(0).(string)
}

// GetExistingNodeGroupLocations is a mocked method.
func (m *GkeManagerMock) GetExistingNodeGroupLocations() []string {
	args := m.Called()
	return args.Get(0).([]string)
}

// GetAutoprovisioningLocations is a mocked method.
func (m *GkeManagerMock) GetAutoprovisioningLocations() []string {
	args := m.Called()
	return args.Get(0).([]string)
}

// IsDataplaneV2Enabled is a mocked method
func (m *GkeManagerMock) IsDataplaneV2Enabled() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

// IsDefaultCCCEnabled is a mocked method
func (m *GkeManagerMock) IsDefaultCCCEnabled() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

// ValidateGpuConfig is a mocked method.
func (m *GkeManagerMock) ValidateGpuConfig(gpuType, gpuPartitionSize, gpuMaxSharedClients, gpuSharingStrategy, machineType string, gpuCount int64, zone string, cpus, mem int64) error {
	args := m.Called(gpuType, gpuPartitionSize, gpuMaxSharedClients, gpuSharingStrategy, machineType, gpuCount, zone, cpus, mem)
	return args.Error(0)
}

// ValidateMachineTypeConfig is a mocked method.
func (m *GkeManagerMock) ValidateMachineTypeConfig(machineType, zone string) error {
	args := m.Called(machineType, zone)
	return args.Error(0)
}

// Client is a mocked method.
func (m *GkeManagerMock) Client() *http.Client {
	args := m.Called()
	return args.Get(0).(*http.Client)
}

// GetImageTypeForNap is a mocked method.
func (m *GkeManagerMock) GetImageTypeForNap(mig *GkeMig) string {
	return "cos_containerd"
}

// GetOsDistributionForNap is a mocked method.
func (m *GkeManagerMock) GetOsDistributionForNap(mig *GkeMig) gce.OperatingSystemDistribution {
	return gce.OperatingSystemDistributionDefault
}

// QueuedProvisioningNodeHasScaleDownImmunity is a mocked method.
func (m *GkeManagerMock) QueuedProvisioningNodeHasScaleDownImmunity(node *apiv1.Node, migSpec *reconciler.QueuedProvisioningMigSpec, now time.Time) bool {
	args := m.Called(node, migSpec, now)
	return args.Get(0).(bool)
}

// CreateQueuedInstances is a mocked method.
func (m *GkeManagerMock) CreateQueuedInstances(pr prpods.ProvReqID, mig *GkeMig, delta int64, shouldUpdateProvReqDetails manager.ShouldUpdateProvReqDetails) error {
	args := m.Called(pr, mig, delta)
	return args.Error(0)
}

// UpdateNodePoolLabels is a mocked method.
func (m *GkeManagerMock) UpdateNodePoolLabels(nodePoolName string, labels map[string]string) error {
	panic("GkeManagerMock is deprecated and should not have new usages. Use FakeGkeManager instead")
}

// CreateResizeRequest is a mocked method.
func (m *GkeManagerMock) CreateResizeRequest(mig gce.Mig, delta int64) error {
	args := m.Called(mig, delta)
	return args.Error(0)
}

// CreateFlexResizeRequests is a mocked method.
func (m *GkeManagerMock) CreateFlexResizeRequests(mig gce.Mig, delta int64) error {
	args := m.Called(mig, delta)
	return args.Error(0)
}

// AdvanceResizeRequestCleanUp is a mocked method.
func (m *GkeManagerMock) AdvanceResizeRequestCleanUp(resizeRequest resizerequestclient.ResizeRequestStatus) error {
	args := m.Called(resizeRequest)
	return args.Error(0)
}

// ResetFailedResizeRequestsCreation is a mocked method.
func (m *GkeManagerMock) ResetFailedResizeRequestsCreation(migRef gce.GceRef) map[error]int {
	args := m.Called(migRef)
	return args.Get(0).(map[error]int)
}

func (m *GkeManagerMock) ReportState(resizeRequest resizerequestclient.ResizeRequestStatus) resizerequestclient.ResizeRequestReportState {
	if m.ResReqReportState == nil {
		return resizerequestclient.UnspecifiedReportState
	}
	return m.ResReqReportState[resizeRequest.Name]
}

func (m *GkeManagerMock) SetReportState(resizeRequest resizerequestclient.ResizeRequestStatus, state resizerequestclient.ResizeRequestReportState) {
	if m.ResReqReportState == nil {
		m.ResReqReportState = map[string]resizerequestclient.ResizeRequestReportState{}
	}
	m.ResReqReportState[resizeRequest.Name] = state
}

// IsResizeRequestErrorHandlingEnabled is a mocked method.
func (m *GkeManagerMock) IsResizeRequestErrorHandlingEnabled() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

// CreateInstances is a mocked method.
func (m *GkeManagerMock) CreateInstances(mig gce.Mig, delta int64) error {
	args := m.Called(mig, delta)
	return args.Error(0)
}

// ResizeVm is a mocked method.
func (m *GkeManagerMock) ResizeVm(ctx context.Context, instance gce.GceRef, desiredSize ekvmsize.VmSize) error {
	args := m.Called(ctx, instance, desiredSize)
	return args.Error(0)
}

func (m *GkeManagerMock) GetCurrentResizableVmState(_ *machinetypes.MachineConfigProvider, instance gce.GceRef) (ekvmtypes.ResizableVmState, error) {
	args := m.Called(instance)
	return args.Get(0).(ekvmtypes.ResizableVmState), args.Error(1)
}

func (m *GkeManagerMock) BulkFetchCurrentResizableVmStates(_ *machinetypes.MachineConfigProvider) (map[gce.GceRef]ekvmtypes.ResizableVmState, error) {
	args := m.Called()
	return args.Get(0).(map[gce.GceRef]ekvmtypes.ResizableVmState), args.Error(1)
}

func (m *GkeManagerMock) InstanceByRef(ref gce.GceRef) *gce.GceInstance {
	args := m.Called(ref)
	return args.Get(0).(*gce.GceInstance)
}

func (m *GkeManagerMock) GetListManagedInstancesResults(ref gce.GceRef) (string, error) {
	args := m.Called(ref)
	return args.Get(0).(string), args.Error(1)
}

func (m *GkeManagerMock) CalculatePhysicalEphemeralStorageGiB(mig *GkeMig, allocatableBytes int64) int64 {
	args := m.Called(mig, allocatableBytes)
	return args.Get(0).(int64)
}

func (m *GkeManagerMock) ScaleDownUnreadyTimeOverride(mig *GkeMig) (time.Duration, bool) {
	args := m.Called(mig)
	return args.Get(0).(time.Duration), args.Bool(1)
}

func (m *GkeManagerMock) ScaleDownUnneededTimeOverride(nodeGroup cloudprovider.NodeGroup) (time.Duration, bool, error) {
	if !m.haveAutoscalingOptsOverrides {
		return 0, false, nil
	}

	args := m.Called(nodeGroup)
	return args.Get(0).(time.Duration), args.Bool(1), args.Error(2)
}

func (m *GkeManagerMock) ScaleDownUtilizationThresholdOverride(nodeGroup cloudprovider.NodeGroup) (float64, bool, error) {
	if !m.haveAutoscalingOptsOverrides {
		return 0, false, nil
	}

	args := m.Called(nodeGroup)
	return args.Get(0).(float64), args.Bool(1), args.Error(2)
}

func (m *GkeManagerMock) ScaleDownGpuUtilizationThresholdOverride(nodeGroup cloudprovider.NodeGroup) (float64, bool, error) {
	if !m.haveAutoscalingOptsOverrides {
		return 0, false, nil
	}

	args := m.Called(nodeGroup)
	return args.Get(0).(float64), args.Bool(1), args.Error(2)
}

func (m *GkeManagerMock) ValidateLocationForDiskType(location string, requestedDiskType string) (ok bool, reason string, err error) {
	args := m.Called(location, requestedDiskType)
	return args.Get(0).(bool), args.String(1), args.Error(2)
}

func (m *GkeManagerMock) ResizingEnabled(machineFamily string) bool {
	args := m.Called(machineFamily)
	return args.Get(0).(bool)
}

func (m *GkeManagerMock) IsEkEdpEnabled() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

func (m *GkeManagerMock) IsEkSpotEnabled() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

func (m *GkeManagerMock) GetNodesScaleDownAllowedFromCache(nodeNames []string) map[string]bool {
	args := m.Called(nodeNames)
	return args.Get(0).(map[string]bool)
}

func (m *GkeManagerMock) UpdateNodesScaleDownAllowedCache(nodesScaleDownAllowed map[string]bool) {
	m.Called(nodesScaleDownAllowed)
}

func (m *GkeManagerMock) InvalidateNodesScaleDownAllowedCache() {
	m.Called()
}

func (m *GkeManagerMock) GetMaxNodeProvisioningTimeOverride(mig *GkeMig) (time.Duration, bool) {
	if !m.haveAutoscalingOptsOverrides {
		return time.Duration(0), false
	}
	args := m.Called(mig)
	return args.Get(0).(time.Duration), args.Get(1).(bool)
}

func (m *GkeManagerMock) EvaluateCapacityCheckWaitTimeSeconds(mig *GkeMig) (time.Duration, error) {
	args := m.Called(mig)
	return args.Get(0).(time.Duration), args.Error(1)
}

func (m *GkeManagerMock) CapacityCheckWaitTimeSeconds(mig *GkeMig) (time.Duration, error) {
	args := m.Called(mig)
	return args.Get(0).(time.Duration), args.Error(1)
}

func (m *GkeManagerMock) GetInjectedMig(mig *GkeMig) *GkeMig {
	m.m.Lock()
	defer m.m.Unlock()

	return m.InjectedMigs[mig.Id()]
}

func (m *GkeManagerMock) SetInjectedMig(real, injected *GkeMig) {
	m.m.Lock()
	defer m.m.Unlock()

	if m.InjectedMigs == nil {
		m.InjectedMigs = map[string]*GkeMig{}
	}
	m.InjectedMigs[real.Id()] = injected
}

// ExistingMigsInNodePool is a mocked method.
func (m *GkeManagerMock) ExistingMigsInNodePool(nodePoolName string) []*GkeMig {
	args := m.Called(nodePoolName)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).([]*GkeMig)
}

// ResumeInstances is a mocked method.
func (m *GkeManagerMock) ResumeInstances(migRef gce.GceRef, instances []gce.GceRef) error {
	args := m.Called(migRef, instances)
	return args.Error(0)
}

// SuspendInstances is a mocked method.
func (m *GkeManagerMock) SuspendInstances(migRef gce.GceRef, instances []gce.GceRef, forceSuspend bool) error {
	args := m.Called(migRef, instances, forceSuspend)
	return args.Error(0)
}

func (m *GkeManagerMock) GetDeploymentType(_ gce.GceRef, spec *gkeclient.NodePoolSpec) DeploymentTypeEnum {
	if spec == nil || spec.ReservationAffinity == nil {
		return DeploymentTypeUnspecified
	}
	return DeploymentTypeNone
}

func (m *GkeManagerMock) GetBasenameForMig(mig *GkeMig) (string, error) {
	args := m.Called(mig)

	var basename string
	switch v := args.Get(0).(type) {
	case string:
		basename = v
	case func(*GkeMig) string:
		basename = v(mig)
	default:
		panic(fmt.Sprintf("unexpected type %T", v))
	}

	return basename, args.Error(1)
}

func (m *GkeManagerMock) TrimLocationsForMachineConfig(locations []string, machineType string, acceleratorConfig *gke_api_beta.AcceleratorConfig, minCpuPlatform string, diskType string) []string {
	panic("not implemented")
}

func (m *GkeManagerMock) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return machinetypes.NewMachineConfigProvider(nil)
}

// UpdateAutoprovisioningMachineFamilies only exists to satisfy interface.
func (m *GkeManagerMock) UpdateAutoprovisioningMachineFamilies() {
}

// MockGceCache mocks the gce cache functionality so we can do better testing
type MockGceCache struct {
}

// RegenerateInstancesCache triggers instances cache regeneration under lock.
func (g *MockGceCache) RegenerateInstancesCache() error {
	return nil
}

// InvalidateMigInstances clears the mig instances cache
func (g *MockGceCache) InvalidateMigInstances(ref gce.GceRef) {
}

// InvalidateMigTargetSize clears the target size cache
func (g *MockGceCache) InvalidateMigTargetSize(ref gce.GceRef) {
}

// SetMigTargetSize sets targetSize for a GceRef
func (g *MockGceCache) SetMigTargetSize(ref gce.GceRef, i int64) {
}

// SetResourceLimiter sets resource limiter.
func (g *MockGceCache) SetResourceLimiter(limiter *cloudprovider.ResourceLimiter) {
}

// GetResourceLimiter returns resource limiter.
func (g *MockGceCache) GetResourceLimiter() (*cloudprovider.ResourceLimiter, error) {
	return nil, nil
}

// GetMigForInstance returns Mig to which the given instance belongs.
func (g *MockGceCache) GetMigForInstance(ref gce.GceRef) (gce.Mig, error) {
	return nil, nil
}

// SetMachines sets machine cache
func (g *MockGceCache) SetMachines(m map[gce.MachineTypeKey]gce.MachineType) {
}

// AddMachine adds machines to cache
func (g *MockGceCache) AddMachine(machineType gce.MachineType, s string) {
}

// InvalidateAllMachines clears the machines cache
func (g *MockGceCache) InvalidateAllMachines() {
}

// SetMigBasename sets basename for given mig in cache
func (g *MockGceCache) SetMigBasename(ref gce.GceRef, s string) {
}

// GetMigBasename gets basename for given mig from cache.
func (g *MockGceCache) GetMigBasename(gce.GceRef) (string, bool) {
	return "", true
}

// InvalidateAllMigInstances clears the instances cache
func (g *MockGceCache) InvalidateAllMigInstances() {
}

// InvalidateAllMigBasenames clears the basename cache
func (g *MockGceCache) InvalidateAllMigBasenames() {
}

// InvalidateAllMigTargetSizes clears the target size cache
func (g *MockGceCache) InvalidateAllMigTargetSizes() {
}

// InvalidateAllMigInstanceTemplateNames clears the instance template name cache
func (g *MockGceCache) InvalidateAllMigInstanceTemplateNames() {
}

// GetMachine retrieves machine type from cache under lock.
func (g *MockGceCache) GetMachine(s string, s2 string) (gce.MachineType, bool) {
	return gce.MachineType{}, true
}

// RegisterMig will register the Mig
func (g *MockGceCache) RegisterMig(newMig gce.Mig) bool {
	return true
}

// UnregisterMig will un-register the Mig
func (g *MockGceCache) UnregisterMig(toBeRemoved gce.Mig) bool {
	return true
}

// GetMigs returns cached Migs
func (g *MockGceCache) GetMigs() []gce.Mig {
	return nil
}

// SetListManagedInstancesResults sets listManagedInstancesResults for a given mig in cache
func (g *MockGceCache) SetListManagedInstancesResults(migRef gce.GceRef, listManagedInstancesResults string) {
}

// GetListManagedInstancesResults gets listManagedInstancesResults for a given mig from cache.
func (g *MockGceCache) GetListManagedInstancesResults(gce.GceRef) (string, bool) {
	return "", true
}

// InvalidateAllListManagedInstancesResults invalidates all listManagedInstancesResults entries.
func (g *MockGceCache) InvalidateAllListManagedInstancesResults() {
}

// DropInstanceTemplatesForMissingMigs clears the instance template
// cache intended MIGs which are no longer present in the cluster
func (g *MockGceCache) DropInstanceTemplatesForMissingMigs(currentMigs []gce.Mig) {}

// MockCustomResourceProcessor mocks internal custom resource processor.
type MockCustomResourceProcessor struct {
	context *autoscaling_context.AutoscalingContext
}

// NewMockProcessor returns mock of custom resource processor.
func NewMockProcessor() *MockCustomResourceProcessor {
	return &MockCustomResourceProcessor{}
}

// SetContext set context for mock processor.
func (p *MockCustomResourceProcessor) SetContext(context *autoscaling_context.AutoscalingContext) {
	p.context = context
}

// FilterOutNodesWithUnreadyResources removes nodes that should have a custom resource, but don't have
// it in allocatable from ready nodes list and updates their status to unready on all nodes list.
func (p *MockCustomResourceProcessor) FilterOutNodesWithUnreadyResources(context *autoscaling_context.AutoscalingContext, allNodes, readyNodes []*apiv1.Node, _ *drasnapshot.Snapshot, _ *csisnapshot.Snapshot) ([]*apiv1.Node, []*apiv1.Node) {
	return allNodes, readyNodes
}

// GetNodeResourceTargets returns mapping of resource names to their targets.
func (p *MockCustomResourceProcessor) GetNodeResourceTargets(context *autoscaling_context.AutoscalingContext, node *apiv1.Node, nodeGroup cloudprovider.NodeGroup) ([]customresources.CustomResourceTarget, errors.AutoscalerError) {
	return []customresources.CustomResourceTarget{}, nil
}

// CleanUp cleans up processor's internal structures.
func (p *MockCustomResourceProcessor) CleanUp() {
	p.context = nil
}

type MockAutoprovisioningEligibility struct {
	mock.Mock
}

// SetClusterAutoprovisioningEnabled sets the value of cluster NAP flag. Returns true if the flag has changed, false
// otherwise (in case of a no-op). In case of a mock, the return value can just be true.
func (m *MockAutoprovisioningEligibility) SetClusterAutoprovisioningEnabled(flag bool) bool {
	args := m.Called(flag)
	return args.Get(0).(bool)
}

// IsNodeAutoprovisioningEnabled returns true if autoprovisioning is enabled.
func (m *MockAutoprovisioningEligibility) IsNodeAutoprovisioningEnabled() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

// AreClusterLimitsEnabled returns true if NAP cluster limits are enabled.
func (m *MockAutoprovisioningEligibility) AreClusterLimitsEnabled() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

// UseAutoprovisioningFeaturesForPodRequirements checks if pod should trigger autoprovisioning features.
func (m *MockAutoprovisioningEligibility) UseAutoprovisioningFeaturesForPodRequirements(req *podrequirements.Requirements) bool {
	args := m.Called(req)
	return args.Get(0).(bool)
}

// UseAutoprovisioningFeaturesForNodeGroup check if node group should trigger autoprovisioning features.
func (m *MockAutoprovisioningEligibility) UseAutoprovisioningFeaturesForNodeGroup(nodeGroup cloudprovider.NodeGroup) bool {
	args := m.Called(nodeGroup)
	return args.Get(0).(bool)
}

func (fake *FakeGkeManager) SetRecommendation(migId string, rec ScaleUpRecommendation) {}
func (fake *FakeGkeManager) PopRecommendation(migId string) (rec ScaleUpRecommendation, ok bool) {
	return ScaleUpRecommendation{}, false
}
func (fake *FakeGkeManager) ClearRecommendations() {}

func (m *GkeManagerMock) SetRecommendation(migId string, rec ScaleUpRecommendation) {
	m.Called(migId, rec)
}
func (m *GkeManagerMock) PopRecommendation(migId string) (rec ScaleUpRecommendation, ok bool) {
	args := m.Called(migId)
	return args.Get(0).(ScaleUpRecommendation), args.Bool(1)
}
func (m *GkeManagerMock) ClearRecommendations() {
	m.Called()
}
