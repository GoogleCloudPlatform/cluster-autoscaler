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
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	gce_api "google.golang.org/api/compute/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	ca_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/annotations"
	ca_errors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/autoscaler/cluster-autoscaler/utils/klogx"
	"k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	nap_interfaces "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/interfaces"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/napcloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/selfservice"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/dynamicresources"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/consumablereservations"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizablevms"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	gke_metrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/preemption"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	gkeutil "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	ekvms_customthresholds "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/backoff/customthresholds"
	ek_errors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer"
	ekvm_provider_interfaces "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/providers/interfaces"
	ekvmsize "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	ekvmtypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
	ekvms_utils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/utils"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"
	flexadvisorapi "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/gkedebuggingsnapshot"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/multitenancy"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/networking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	provreqcache "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/cache"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/manager"
	prmanager "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/manager"
	prpods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/reconciler"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
)

const (
	// ProviderNameGKE is the name of GKE cloud provider.
	ProviderNameGKE = "gke"
	MigPaginated    = "PAGINATED"
	MigPageless     = "PAGELESS"

	// NodePoolErrorStatus comes from http://google3/google/container/v1beta1/cluster_service.proto;l=2793;rcl=216954548
	NodePoolErrorStatus = "ERROR"
)

const (
	maxMachineTypeLength   = 14
	minAutoprovisionedSize = 0
	// gceLimitOnMigSizes is the maximum number of instances in a Managed Instance Group per GCE limits. It's not possible to fetch this value dynamically and it should be considered a hard limit on the scaling of a single node group.
	gceLimitOnMigSizes = 2000

	// MaxNodeProvisionTime for QueuedProvisioning. It is set as a very large number so that it is effectively infinite.
	// Provisioning requests can be in the queue for a very long time.
	queuedProvisioningMaxNodeProvisionTime = 365 * 24 * time.Hour
	tpuMigMaxNodeProvisionTime             = 10 * time.Hour
	// For Flex Start Non-Queued scale ups, the requests for VMs will be cancelled after the `CapacityCheckWaitTime`.
	// `MaxNodeProvisionTime` is set to be `CapacityCheckWaitTime` + an offset for the VM to actually register.
	// Setting `MaxNodeProvisionTime` for Flex Start Non-Queued node pools is also a fallback, used when e.g. `error_reporter` was disabled by `FlexStartNonQueuedEnabledFlag` experiment.
	// In such case, the VM placeholders will be marked as `longUnregistered` and deleted after the `MaxNodeProvisionTime`,
	// resulting in their corresponding Resize Requests getting cancelled by GCE.
	DefaultFlexStartCapacityCheckWaitTime = 15 * time.Minute
	maxNodeProvisionTimeOffset            = 15 * time.Minute
)

// gkeCloudProviderImpl implements various CloudProvider sub-interfaces.
type gkeCloudProviderImpl struct {
	gkeManager GkeManager
	priceModel cloudprovider.PricingModel
	// This resource limiter is used if resource limits are not defined through cloud API.
	resourceLimiterFromFlags *cloudprovider.ResourceLimiter
	// autopilotEnabled which signifies if autoscaling is run in Auto Pilot managed GKE
	autopilotEnabled bool
	// region in which the cluster is running.
	region string
	// napMaxNodes max nodes per zone in autoprovisioned node pools
	napMaxNodes int
	// gkeDebuggingSnapshotter is the debuggingSnapshot used to capture internal state of CA
	gkeDebuggingSnapshotter *gkedebuggingsnapshot.GkeDebuggingSnapshotter
	// compactPlacementEnabled signifies if compact placement node pools can be provisioned
	compactPlacementEnabled              bool
	resolveInstanceRefUsingNodePoolLabel bool

	nodePoolSpecBuilders []napcloudprovider.NodePoolSpecBuilder

	podLister kubernetes.PodLister
	domainUrl string

	experimentsManager    experiments.Manager
	machineConfigProvider *machinetypes.MachineConfigProvider
	gkePriceInfo          *GkePriceInfo
}

// BuildGkeCloudProvider builds CloudProvider implementation for GKE.
func BuildGkeCloudProvider(
	gkeManager GkeManager,
	priceModel cloudprovider.PricingModel,
	resourceLimiter *cloudprovider.ResourceLimiter,
	autopilotEnabled bool,
	location string,
	gkeDebuggingSnapshotter *gkedebuggingsnapshot.GkeDebuggingSnapshotter,
	compactPlacementEnabled bool,
	resolveInstanceRefUsingNodePoolLabel bool,
	listers kubernetes.PodLister,
	domainUrl string,
	experimentsManager experiments.Manager,
	machineConfigProvider *machinetypes.MachineConfigProvider,
	gkePriceInfo *GkePriceInfo,
	napMaxNodes int,
) (*gkeCloudProviderImpl, error) {
	region, err := gkeutil.GetRegionFromLocation(location)
	if err != nil {
		return nil, err
	}

	return &gkeCloudProviderImpl{
		gkeManager:                           gkeManager,
		priceModel:                           priceModel,
		resourceLimiterFromFlags:             resourceLimiter,
		autopilotEnabled:                     autopilotEnabled,
		region:                               region,
		gkeDebuggingSnapshotter:              gkeDebuggingSnapshotter,
		compactPlacementEnabled:              compactPlacementEnabled,
		resolveInstanceRefUsingNodePoolLabel: resolveInstanceRefUsingNodePoolLabel,
		podLister:                            listers,
		domainUrl:                            domainUrl,
		experimentsManager:                   experimentsManager,
		machineConfigProvider:                machineConfigProvider,
		gkePriceInfo:                         gkePriceInfo,
		napMaxNodes:                          napMaxNodes,
	}, nil
}

// RegisterNodePoolSpecBuilders registers builders for Node spec.
func (p *gkeCloudProviderImpl) RegisterNodePoolSpecBuilders(builders []napcloudprovider.NodePoolSpecBuilder) {
	p.nodePoolSpecBuilders = builders
}

// IsNodeAutoprovisioningEnabled returns true if NAP is enabled.
func (p *gkeCloudProviderImpl) IsNodeAutoprovisioningEnabled() bool {
	return p.gkeManager.IsNodeAutoprovisioningEnabled()
}

// UseAutoprovisioningFeaturesForPodRequirements checks if pod should trigger autoprovisioning features.
func (p *gkeCloudProviderImpl) UseAutoprovisioningFeaturesForPodRequirements(req *podrequirements.Requirements) bool {
	return p.gkeManager.UseAutoprovisioningFeaturesForPodRequirements(req)
}

// UseAutoprovisioningFeaturesForNodeGroup check if node group should trigger autoprovisioning features.
func (p *gkeCloudProviderImpl) UseAutoprovisioningFeaturesForNodeGroup(nodeGroup cloudprovider.NodeGroup) bool {
	return p.gkeManager.UseAutoprovisioningFeaturesForNodeGroup(nodeGroup)
}

// GPULabel returns the label added to nodes with GPU resource.
func (p *gkeCloudProviderImpl) GPULabel() string {
	return gkelabels.GPULabel
}

// IsE2lessRegion implements the AutoprovisioningCloudProvider interface.
func (p *gkeCloudProviderImpl) IsE2lessRegion() bool {
	return p.GetAutoprovisioningDefaultFamily().Name() == machinetypes.E4.Name()
}

// GetNodeGpuConfig returns the label, type and resource name for the accelerator added to node. It checks
// for GPUs and TPUs in that order. Assumes a node won't have both GPU and TPU. Returns nil if node doesn't
// have any accelerator.
//
// If GPU/TPU devices are exposed using DRA - extended resource won't be present in the
// node alloctable or capacity so we overwrite extended resource name as it won't ever be there
func (p *gkeCloudProviderImpl) GetNodeGpuConfig(node *apiv1.Node) *cloudprovider.GpuConfig {
	if gpuConfig := gpu.GetNodeGPUFromCloudProvider(p, node); gpuConfig != nil {
		if dynamicresources.GpuDraDriverEnabled(node) {
			gpuConfig.DraDriverName = dynamicresources.GpuDriver
			gpuConfig.ExtendedResourceName = ""
		}

		return gpuConfig
	}

	if tpuConfig := tpu.GetNodeTpu(node); tpuConfig != nil {
		if dynamicresources.TpuDraDriverEnabled(node) {
			tpuConfig.DraDriverName = dynamicresources.TpuDriver
			tpuConfig.ExtendedResourceName = ""
		}

		return tpuConfig
	}

	return nil
}

// GetAvailableGPUTypes return all available GPU and TPU types cloud provider supports.
// Note: this method also returns TPUs and it is used to correctly retrieve the GPU/TPU labels for scale up metrics.
// This method is a part of the OSS CloudProvider interface, so we can't just
// inline it or return a concrete type.
func (p *gkeCloudProviderImpl) GetAvailableGPUTypes() map[string]struct{} {
	gpus := p.machineConfigProvider.GetAllGpuTypes()
	tpus := p.machineConfigProvider.GetAllSupportedTpuTypes()
	result := make(map[string]struct{}, len(gpus)+len(tpus))
	for name := range gpus {
		result[name] = struct{}{}
	}
	for _, name := range p.machineConfigProvider.GetAllSupportedTpuTypes() {
		result[name] = struct{}{}
	}
	return result
}

func (p *gkeCloudProviderImpl) GetFutureReservationsInProject(projectID string) ([]*gceclient.GceFutureReservation, error) {
	return p.gkeManager.GetFutureReservationsInProject(projectID)
}

// GetReservationBlocksInReservation returns the reservation blocks for a particular reservation, in specfied project and zone.
func (p *gkeCloudProviderImpl) GetReservationBlocksInReservation(reservationRef gceclient.ReservationRef) ([]*gceclient.GceReservationBlock, error) {
	return p.gkeManager.GetReservationBlocksInReservation(reservationRef)
}

// GetReservationSubBlocksInReservationBlock returns the reservation subBlocks for a particular reservation block, in specfied reservation, project and zone.
func (p *gkeCloudProviderImpl) GetReservationSubBlocksInReservationBlock(reservationRef gceclient.ReservationRef) ([]*gceclient.GceReservationSubBlock, error) {
	return p.gkeManager.GetReservationSubBlocksInReservationBlock(reservationRef)
}

// GetResourcePolicies returns the resource policies in the provided project and region.
func (p *gkeCloudProviderImpl) GetResourcePolicies(projectId string) ([]*gceclient.GceResourcePolicy, error) {
	return p.gkeManager.GetResourcePolicies(projectId, p.region)
}

// GetExperimentsManager returns the experiment manager.
func (p *gkeCloudProviderImpl) GetExperimentsManager() experiments.Manager {
	return p.experimentsManager
}

// Cleanup cleans up all resources before the cloud provider is removed
func (p *gkeCloudProviderImpl) Cleanup() error {
	if err := p.gkeManager.Cleanup(); err != nil {
		klog.Errorf("Error during cleanup: %v", err)
		return err
	}
	return nil
}

// Name returns name of the cloud provider.
func (p *gkeCloudProviderImpl) Name() string {
	return ProviderNameGKE
}

// NodeGroups returns all node groups configured for this cloud provider.
func (p *gkeCloudProviderImpl) NodeGroups() []cloudprovider.NodeGroup {
	migs := p.gkeManager.GetGkeMigs()
	return toCloudProviderNodeGroups(migs)
}

// NodeGroupsBlockedByServerError returns a list of irretrievable migs blocked by server error (5xx).
func (p *gkeCloudProviderImpl) NodeGroupsBlockedByServerError() []cloudprovider.NodeGroup {
	migs := p.gkeManager.GetGkeMigsBlockedByServerError()
	return toCloudProviderNodeGroups(migs)
}

// NodeGroupsBlockedByNotFoundError returns a list of irretrievable migs blocked by not found error (404).
func (p *gkeCloudProviderImpl) NodeGroupsBlockedByNotFoundError() []cloudprovider.NodeGroup {
	migs := p.gkeManager.GetGkeMigsBlockedByNotFoundError()
	return toCloudProviderNodeGroups(migs)
}

func toCloudProviderNodeGroups(migs []*GkeMig) []cloudprovider.NodeGroup {
	result := make([]cloudprovider.NodeGroup, 0, len(migs))
	for _, mig := range migs {
		result = append(result, mig)
	}
	return result
}

// RecommendLocations returns recommendation made by recommendLocations API.
func (p *gkeCloudProviderImpl) RecommendLocations(ctx context.Context, request gceclient.RecommendLocationsRequest) (*gceclient.RecommendLocationsResponse, error) {
	return p.gkeManager.RecommendLocations(ctx, p.region, request)
}

// GetAllZones returns all zones within a region that cluster is running in.
func (p *gkeCloudProviderImpl) GetAllZones() ([]string, error) {
	return p.gkeManager.GetZonesInRegion(p.region)
}

// GetStandardZones returns all standard zones within a region that cluster is running in.
func (p *gkeCloudProviderImpl) GetStandardZones() ([]string, error) {
	return p.gkeManager.GetStandardZonesInRegion(p.region)
}

// GetAIZones returns all AI zones within a region that cluster is running in.
func (p *gkeCloudProviderImpl) GetAIZones() ([]string, error) {
	return p.gkeManager.GetAIZonesInRegion(p.region)
}

// GetMigInstanceTemplateLabels returns instance template labels for MIG.
func (p *gkeCloudProviderImpl) GetMigInstanceTemplateLabels(mig *GkeMig) (map[string]string, error) {
	ke, err := p.gkeManager.GetMigKubeEnv(mig)
	if err != nil {
		return nil, err
	}
	return gce.GetLabelsFromKubeEnv(ke)
}

// GetMigInstanceTemplateTaints returns instance template taints for MIG.
func (p *gkeCloudProviderImpl) GetMigInstanceTemplateTaints(mig *GkeMig) ([]apiv1.Taint, error) {
	ke, err := p.gkeManager.GetMigKubeEnv(mig)
	if err != nil {
		return nil, err
	}
	return gce.GetTaintsFromKubeEnv(ke)
}

// GetMigInstanceTemplateSelfLink returns an instance template link for MIG.
func (p *gkeCloudProviderImpl) GetMigInstanceTemplateSelfLink(mig *GkeMig) (string, error) {
	it, err := p.gkeManager.GetMigInstanceTemplate(mig)
	if err != nil {
		return "", err
	}
	return it.SelfLink, nil
}

// ResumeInstances resumes instances
func (p *gkeCloudProviderImpl) ResumeInstances(migRef gce.GceRef, instances []gce.GceRef) error {
	return p.gkeManager.ResumeInstances(migRef, instances)
}

// SuspendInstances suspends instances
func (p *gkeCloudProviderImpl) SuspendInstances(migRef gce.GceRef, instances []gce.GceRef, forceSuspend bool) error {
	return p.gkeManager.SuspendInstances(migRef, instances, forceSuspend)
}

// NodeGroupForNode returns the node group for the given node.
func (p *gkeCloudProviderImpl) NodeGroupForNode(node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	ref, err := p.instanceRefForNode(node)
	if err != nil {
		return nil, err
	}
	mig, err := p.gkeManager.GetMigForInstance(ref)
	return mig, err
}

func (p *gkeCloudProviderImpl) instanceRefForNode(node *apiv1.Node) (gce.GceRef, error) {
	if node.Spec.ProviderID != "" {
		return gce.GceRefFromProviderId(node.Spec.ProviderID)
	}

	// Upcoming nodes belonging to node groups being asynchronously created don't have a ProviderID
	// because they don't exist in GCE yet. Luckily GCE ref can be parsed from their node name.
	if node.Annotations[annotations.NodeUpcomingAnnotation] == "true" {
		return instanceRefFromUpcomingNodeName(node.Name)
	}

	if p.resolveInstanceRefUsingNodePoolLabel {
		// Newly created nodes may not have a ProviderID yet as it's reconciled by cloud-controller-manager.
		// However they should have a node pool label set at registration which can be used to find
		// the GCE ref. See go/gke-ca-nil-providerid-logs for details.
		if nodepool := node.Labels[gkelabels.GkeNodePoolLabel]; nodepool != "" {
			return p.instanceRefFromNodeNameAndNodePool(node.Name, nodepool)
		}
	}

	return gce.GceRef{}, fmt.Errorf("cannot resolve GCE instance for node %s with an empty ProviderID (note: new nodes may have empty ProviderID temporarily right after creation)", node.Name)
}

func instanceRefFromUpcomingNodeName(nodeName string) (gce.GceRef, error) {
	expectedPrefix := "template-node-for-https://www.googleapis.com/compute/"
	if len(nodeName) <= len(expectedPrefix) || !strings.HasPrefix(nodeName, expectedPrefix) || strings.Count(nodeName[len(expectedPrefix):], "/") != 6 {
		return gce.GceRef{}, fmt.Errorf("wrong upcoming node name: expected format %s<version>/projects/<project>/zones/<zone>/instanceGroups/<node-name>, got %v", expectedPrefix, nodeName)
	}
	splitted := strings.Split(nodeName[len(expectedPrefix):], "/")
	return gce.GceRef{
		Project: splitted[2],
		Zone:    splitted[4],
		Name:    splitted[6],
	}, nil
}

func (p *gkeCloudProviderImpl) instanceRefFromNodeNameAndNodePool(nodeName string, nodepool string) (gce.GceRef, error) {
	candidateMigs := p.gkeManager.ExistingMigsInNodePool(nodepool)
	var basenamesTried []string
	var basenameErrs []error
	for _, candidateMig := range candidateMigs {
		basename, err := p.gkeManager.GetBasenameForMig(candidateMig)
		if err != nil {
			basenameErrs = append(basenameErrs, err)
			continue
		}
		if strings.HasPrefix(nodeName, basename) {
			return gce.GceRef{Project: candidateMig.gceRef.Project, Zone: candidateMig.gceRef.Zone, Name: nodeName}, nil
		}
		basenamesTried = append(basenamesTried, basename)
	}

	return gce.GceRef{}, fmt.Errorf("no ProviderID for node %s and no match with MIGs for node pool %s; MIG basenames tried: %q, errors encountered: %q", nodeName, nodepool, basenamesTried, basenameErrs)
}

// GkeMigForNode returns the MIG a given node belongs to. If the MIG is not autoscaled (and so shouldn't be processed by
// Cluster Autoscaler), nil is returned instead.
func (p *gkeCloudProviderImpl) GkeMigForNode(node *apiv1.Node) (*GkeMig, error) {
	nodeGroup, err := p.NodeGroupForNode(node)
	if err != nil {
		return nil, err
	}
	if nodeGroup == nil || reflect.ValueOf(nodeGroup).IsNil() {
		return nil, nil
	}
	mig, ok := nodeGroup.(*GkeMig)
	if !ok {
		return nil, fmt.Errorf(`unexpected NodeGroup type: want "*gke.GkeMig", got %q"`, reflect.TypeOf(nodeGroup))
	}
	return mig, err
}

// Pricing returns pricing model for this cloud provider or error if not available.
func (p *gkeCloudProviderImpl) Pricing() (cloudprovider.PricingModel, ca_errors.AutoscalerError) {
	return p.priceModel, nil
}

// GetAvailableMachineTypes get all machine types that can be requested from the cloud provider.
func (p *gkeCloudProviderImpl) GetAvailableMachineTypes() ([]string, error) {
	machineTypes := p.GetAutoprovisioningDefaultFamily().AutoprovisionedMachineTypes(machinetypes.NoConstraints)
	var typeNames []string
	for _, machineType := range machineTypes {
		typeNames = append(typeNames, machineType.Name)
	}
	return typeNames, nil
}

// GetAutoprovisioningDefaultFamily returns the default machine family used for autoprovisioned node pools.
func (p *gkeCloudProviderImpl) GetAutoprovisioningDefaultFamily() machinetypes.MachineFamily {
	return p.gkeManager.GetAutoprovisioningDefaultFamily()
}

func (p *gkeCloudProviderImpl) ResizingEnabled(machineFamily string) bool {
	return p.gkeManager.ResizingEnabled(machineFamily)
}

// IsResizableVmEnabledInAutopilot returns true if resizable VM for the given family should be used for autoprovisioning in Autopilot.
func (p *gkeCloudProviderImpl) IsResizableVmEnabledInAutopilot(machineFamily string) bool {
	return p.gkeManager.IsResizableVmEnabledInAutopilot(machineFamily)
}

// IsResizableVmWithinPodFamilyEnabled returns true if resizable VMs for the given family can be used within a pod family.
func (p *gkeCloudProviderImpl) IsResizableVmWithinPodFamilyEnabled(machineFamily string) bool {
	return p.gkeManager.IsResizableVmWithinPodFamilyEnabled(machineFamily)
}

// IsEkSpotEnabled returns true if EKs can be used as spot VMs
func (p *gkeCloudProviderImpl) IsEkSpotEnabled() bool {
	return p.gkeManager.IsEkSpotEnabled()
}

// GetMachineType gets gce.MachineType for a given type name and location.
func (p *gkeCloudProviderImpl) GetMachineType(machineType string, zone string) (gce.MachineType, error) {
	return p.gkeManager.GetMachineType(machineType, zone)
}

// ValidateGpuConfig validates gpu config.
func (p *gkeCloudProviderImpl) ValidateGpuConfig(gpuType, gpuPartitionSize, gpuMaxSharedClients, gpuSharingStrategy, machineType string, gpuCount int64, zone string, cpus, mem int64) error {
	return p.gkeManager.ValidateGpuConfig(gpuType, gpuPartitionSize, gpuMaxSharedClients, gpuSharingStrategy, machineType, gpuCount, zone, cpus, mem)
}

func (p *gkeCloudProviderImpl) IsCompactPlacementEnabled() bool {
	return p.compactPlacementEnabled
}

func (p *gkeCloudProviderImpl) ValidateLocationForDiskType(location string, requestedDiskType string) (ok bool, reason string, err error) {
	return p.gkeManager.ValidateLocationForDiskType(location, requestedDiskType)
}

// IsEkEdpEnabled returns true if Edp on EKs with affinity X is enabled
func (p *gkeCloudProviderImpl) IsEkEdpEnabled() bool {
	return p.gkeManager.IsEkEdpEnabled()
}

// GenerateRandomId create random [0-9a-z]{length} identifier
func GenerateRandomId(length int) string {
	var number string
	for len(number) < length {
		number += strconv.FormatInt(rand.Int63(), 36)
	}
	if len(number) > length {
		return number[:length]
	}
	return number
}

// NewNodeGroup builds a theoretical node group based on the node definition provided. The node group is not automatically
// created on the cloud provider side. The node group is not returned by NodeGroups() until it is created.
// TODO(b/216616990): Move NewNodeGroup out of OSS CloudProvider.
func (p *gkeCloudProviderImpl) NewNodeGroup(machineType string, labels map[string]string, systemLabels map[string]string,
	taints []apiv1.Taint, extraResources map[string]resource.Quantity,
) (cloudprovider.NodeGroup, error) {
	zone, found := systemLabels[apiv1.LabelZoneFailureDomain]
	if !found {
		return nil, cloudprovider.ErrIllegalConfiguration
	}

	nodePoolSpec := &gkeclient.NodePoolSpec{
		MachineType: machineType,
		Labels:      make(map[string]string),
		Taints:      taints,
	}

	for key, value := range labels {
		nodePoolSpec.Labels[key] = value
	}

	targetExtraResources := make(map[string]resource.Quantity)
	for key, val := range extraResources {
		targetExtraResources[key] = val
	}

	for _, builder := range p.nodePoolSpecBuilders {
		err := builder.UpdateNodePoolSpec(nodePoolSpec, systemLabels, extraResources)
		if err != nil {
			return nil, err
		}
	}

	var nodePoolName string
	if p.compactPlacementEnabled && nodePoolSpec.PlacementGroup.GroupId != "" {
		nodePoolName = nodePoolSpec.PlacementGroup.GroupId
	} else if p.autopilotEnabled {
		nodePoolName = generateNewNodeGroupNameForAutopilot()
	} else {
		gpuRequest := targetExtraResources[gpu.ResourceNvidiaGPU]
		vmPreemptionType := preemption.NoPreemption
		if nodePoolSpec.Spot {
			vmPreemptionType = preemption.Spot
		} else if nodePoolSpec.Preemptible {
			vmPreemptionType = preemption.LegacyPreemptible
		}
		nodePoolName = generateNewNodeGroupName(nodePoolSpec.MachineType, vmPreemptionType, gpuRequest)
	}
	if nodePoolSpec.Spot {
		nodePoolSpec.LocationPolicy = string(LocationPolicyAny)
	}

	nodePoolSpec.Labels[gkelabels.GkeNodePoolLabel] = nodePoolName

	autopilotManagedNodepool := systemLabels[gkelabels.ManagedNodeLabel] == "true"

	if (p.autopilotEnabled || autopilotManagedNodepool) && nodePoolSpec.ReservationAffinity == nil && nodePoolSpec.TpuType == "" {
		nodePoolSpec.ReservationAffinity = &gke_api_beta.ReservationAffinity{
			ConsumeReservationType: gkeclient.ReservationAffinityNone,
		}
	}

	machineFamily, err := p.machineConfigProvider.GetMachineFamilyFromMachineName(machineType)
	if err != nil {
		return nil, ca_errors.NewAutoscalerErrorf(ca_errors.InternalError, "unknown machine family for machine type %q: %v", machineType, err)
	}

	maxSize := p.napMaxNodes
	if nodePoolSpec.TpuMultiHost {
		tpuNodesFromTopology, err := p.machineConfigProvider.NumNodesFromTopology(nodePoolSpec.MachineType, nodePoolSpec.TpuTopology)
		if err != nil {
			return nil, ca_errors.NewAutoscalerError(ca_errors.ConfigurationError, err.Error())
		}
		maxSize = int(tpuNodesFromTopology)
	} else if policy := nodePoolSpec.PlacementGroup.ResourcePolicy; policy != nil && machineFamily.IsAcceleratorSliceSupported() {
		maxNodesFromResourcePolicy, err := placement.MaxNodes(p.machineConfigProvider, nodePoolSpec.MachineType, policy)
		if err != nil {
			return nil, ca_errors.NewAutoscalerError(ca_errors.ConfigurationError, err.Error())
		}
		maxSize = int(maxNodesFromResourcePolicy)
	}

	bootDiskSize := p.bootDiskSizeForNewNodeGroup(*nodePoolSpec)

	nodePoolSpec.DiskSize = bootDiskSize
	if nodePoolSpec.AutopilotManaged && nodePoolSpec.DiskType == "" {
		// Autopilot nodes only support pd-balanced and hyperdisk-balanced (for gen 4 machines).
		// Autopilot nodes should default to either pd-balanced or hyperdisk-balanced
		// regardless of the cluster level NAP configurations.
		// Otherwise node pool creation will fail with internal error.
		nodePoolSpec.DiskType = p.defaultBootDiskTypeForNewAutopilotNodeGroup(nodePoolSpec.MachineType)
	}
	nodePoolSpec.DiskType = p.bootDiskTypeForNewNodeGroup(nodePoolSpec.MachineType, nodePoolSpec.DiskType)
	gceRef := gce.GceRef{
		Project: p.gkeManager.GetProjectId(),
		Zone:    zone,
		// Randomize temporary mig name to make it unique. Cluster Autoscaler relies on
		// MIG uniqueness by using it as a map key. Node Pool Name is not guaranteed to
		// be unique.
		Name: fmt.Sprintf("%s-temporary-mig-%s", nodePoolName, GenerateRandomId(8)),
	}
	mig := NewGkeMig(gceRef, p.domainUrl, p.gkeManager)
	mig.autoprovisioned = true
	mig.exist = false
	mig.nodeConfig = &NodeConfig{}
	mig.minSize = minAutoprovisionedSize
	mig.maxSize = maxSize
	mig.spec = nodePoolSpec
	mig.extraResources = targetExtraResources
	mig.queuedProvisioning = nodePoolSpec.QueuedProvisioning
	mig.shortLivedUpgradeInProgress = false
	mig.locationPolicy = toLocationPolicyEnum(nodePoolSpec.LocationPolicy)
	mig.deploymentType = p.gkeManager.GetDeploymentType(gceRef, nodePoolSpec)
	AddMigsToNodePool(nodePoolName, mig)

	if nodePoolSpec.QueuedProvisioning {
		// Explicitly set the location policy due to b/320422870.
		mig.locationPolicy = LocationPolicyAny
		nodePoolSpec.LocationPolicy = string(LocationPolicyAny)
	}

	nodePoolSpec.ImageType = p.gkeManager.GetImageTypeForNap(mig)
	nodePoolSpec.Labels[gkelabels.GkeOsDistributionLabel] = string(p.gkeManager.GetOsDistributionForNap(mig))

	// CCC self-service features don't have a chance to set nodePoolSpec until the call to GKE is made.
	// This allows them to apply mutations if needed: b/407954225.
	selfservice.UpdateMig(mig, nodePoolSpec.SelfServiceMetadata)

	// Try to build a NodeInfo from autoprovisioning spec. We don't need one right now,
	// but if it fails later, we'd end up with a node group we can't scale anyway,
	// so there's no point creating it.
	if _, err := p.gkeManager.GetMigTemplateNodeInfo(mig); err != nil {
		return nil, fmt.Errorf("failed to build node from spec: %v", err)
	}

	return mig, nil
}

func (p *gkeCloudProviderImpl) bootDiskSizeForNewNodeGroup(spec gkeclient.NodePoolSpec) int64 {
	if spec.DiskSize != 0 {
		return spec.DiskSize
	}
	defaultSize := p.gkeManager.GetDefaultNodePoolDiskSizeGB()

	if spec.LocalSSDConfig.EphemeralStorageOnLocalSsd(spec.MachineType) {
		// Ephemeral storage is backed by Local SSDs, so increasing the boot disk size doesn't have any impact on
		// the amount of ephemeral storage. Ideally, we'd return the lowest size possible here in this case.
		// TODO(b/317509315): Determine the min required size for the boot disk, use that here instead.
		return defaultSize
	}

	if p.autopilotEnabled || (spec.AutopilotManaged && spec.Labels[gkelabels.NodeGroupDynamicBootDiskSizeEnabledLabelKey] == "true") {
		// If cluster is autopilot or standard but autopilot mode for node group is enabled, DynamicBootDiskSize is enabled and ephemeral storage is based on the boot disk. Set the boot disk size to
		// the maximum value possible, and the trim it in EstimatorAnalysisFunc: go/ap-dynamic-eph-storage.
		// TODO(b/317524532): Determine if we need to propagate the max size from an API.
		return machinetypes.MaxBootDiskSizeNonSharedCoreMachinesGb
	}

	return defaultSize
}

func (p *gkeCloudProviderImpl) bootDiskTypeForNewNodeGroup(machineType string, diskType string) string {
	if diskType == "" {
		diskType = p.gkeManager.GetDefaultNodePoolDiskType()
	}
	mf, err := p.machineConfigProvider.GetMachineFamilyFromMachineName(machineType)
	if err != nil {
		klog.Warningf("could not get machine family for %s: %v", machineType, err)
		return diskType
	}

	// if the disk-type specificed on cluster creation is supported, use it
	if mf.IsDiskTypeSupportedForMachineType(diskType, machineType) {
		return diskType
	}
	klog.Warningf("The specified boot disk type '%s' does not support machine family '%s', use default boot disk type", diskType, machineType)
	return mf.DefaultAutoprovisionedBootDiskType(machineType)
}

func (p *gkeCloudProviderImpl) defaultBootDiskTypeForNewAutopilotNodeGroup(machineType string) string {
	mf, err := p.machineConfigProvider.GetMachineFamilyFromMachineName(machineType)
	if err != nil {
		klog.Warningf("could not get machine family for %s: %v", machineType, err)
		return ""
	}

	// if the pd-balanced disk-type is supported, use it
	if mf.IsDiskTypeSupportedForMachineType(machinetypes.DiskTypeBalanced, machineType) {
		return machinetypes.DiskTypeBalanced
	}
	if mf.IsDiskTypeSupportedForMachineType(machinetypes.DiskTypeHyperdiskBalanced, machineType) {
		return machinetypes.DiskTypeHyperdiskBalanced
	}
	return ""
}

func generateNewNodeGroupName(machineType string, preemptionType preemption.VmPreemptionType, gpuRequest resource.Quantity) string {
	var nameBuilder strings.Builder
	nameBuilder.WriteString(nodeAutoprovisioningPrefix)
	nameBuilder.WriteRune('-')
	nameBuilder.WriteString(cropMachineType(machineType))
	nameBuilder.WriteRune('-')
	if preemptionName := preemptionType.ShortName(); preemptionName != "" {
		nameBuilder.WriteString(fmt.Sprintf("%s-", preemptionName))
	}
	if gpuRequest.Value() > 0 {
		nameBuilder.WriteString(fmt.Sprintf("gpu%d-", gpuRequest.Value()))
	}
	nameBuilder.WriteString(GenerateRandomId(8))
	return nameBuilder.String()
}

func generateNewNodeGroupNameForAutopilot() string {
	var nameBuilder strings.Builder
	nameBuilder.WriteString(nodeAutoprovisioningPrefix)
	nameBuilder.WriteRune('-')
	nameBuilder.WriteString(GenerateRandomId(8))
	return nameBuilder.String()
}

// Returns machine type cropped to the appropriate length.
func cropMachineType(machineType string) string {
	if len(machineType) <= maxMachineTypeLength {
		return machineType
	}

	splitMachineType := strings.Split(machineType, "-")
	excess := len(machineType) - maxMachineTypeLength

	if len(splitMachineType) == 3 && len(splitMachineType[1]) > excess {
		// If the machineType follows standard formatting, crop middle part.
		splitMachineType[1] = splitMachineType[1][:len(splitMachineType[1])-excess]
		return strings.Join(splitMachineType, "-")
	}

	// If the machineType does not follow standard formatting.
	return machineType[excess:]
}

// GetResourceLimiter returns struct containing limits (max, min) for resources (cores, memory etc.).
func (p *gkeCloudProviderImpl) GetResourceLimiter() (*cloudprovider.ResourceLimiter, error) {
	resourceLimiter, err := p.gkeManager.GetResourceLimiter(p.NodeGroupForNode)
	if err != nil {
		return nil, err
	}
	if resourceLimiter != nil {
		return resourceLimiter, nil
	}
	return p.resourceLimiterFromFlags, nil
}

// Refresh is called before every main loop and can be used to dynamically update cloud provider state.
// In particular the list of node groups returned by NodeGroups can change as a result of CloudProvider.Refresh().
func (p *gkeCloudProviderImpl) Refresh() error {
	if changed := p.machineConfigProvider.Refresh(); changed {
		p.gkePriceInfo.RefreshGkePrices()
		p.gkeManager.RefreshLocalSSDSizes()
	}
	err := p.gkeManager.Refresh()
	if err == nil && p.gkeDebuggingSnapshotter != nil {
		p.gkeDebuggingSnapshotter.GenerateUpdateInfo()
	}
	return err
}

// RegisterInitializationFunc registers an initialization func, which would be called once
// after the first successful Refresh of the cluster state.
func (p *gkeCloudProviderImpl) RegisterInitializationFunc(f InitializationFunc) {
	p.gkeManager.RegisterInitializationFunc(f)
}

// SetScaleUpTimeProvider sets the ScaleUpTimeProvider
func (p *gkeCloudProviderImpl) SetScaleUpTimeProvider(provider ScaleUpTimeProvider) {
	p.gkeManager.SetScaleUpTimeProvider(provider)
}

// GetClusterInfo returns the project id, location and cluster name.
func (p *gkeCloudProviderImpl) GetClusterInfo() (projectId, location, clusterName string) {
	return p.gkeManager.GetProjectId(), p.gkeManager.GetLocation(), p.gkeManager.GetClusterName()
}

// GetClusterCreateTime gets the time of cluster creation.
func (p *gkeCloudProviderImpl) GetClusterCreateTime() time.Time {
	return p.gkeManager.GetClusterCreateTime()
}

// GetClusterVersion gets the gke / master version of the cluster
func (p *gkeCloudProviderImpl) GetClusterVersion() string {
	return p.gkeManager.GetClusterVersion()
}

// NodePoolSpecForNode returns the node pool spec for a particular node, regardless whether
// the node pool is autoscaled or not.
func (p *gkeCloudProviderImpl) NodePoolSpecForNode(node *apiv1.Node) (*gkeclient.NodePoolSpec, error) {
	return p.gkeManager.NodePoolSpecForNode(node)
}

// GetClusterNetwork returns the GCE Network resource of the cluster's VPC
func (p *gkeCloudProviderImpl) GetClusterNetwork() (*gce_api.Network, error) {
	return p.gkeManager.GetClusterNetwork()
}

// ClusterStarted tells if the cluster has started or not
func (p *gkeCloudProviderImpl) ClusterStarted() (bool, error) {
	return p.gkeManager.ClusterStarted()
}

// AreConfidentialNodesEnabled checks if ConfidentialNodes are enabled in cluster.
func (p *gkeCloudProviderImpl) AreConfidentialNodesEnabled() bool {
	return p.gkeManager.AreConfidentialNodesEnabled()
}

// GetConfidentialInstanceType returns the confidential instance type of the cluster.
func (p *gkeCloudProviderImpl) GetConfidentialInstanceType() string {
	return p.gkeManager.GetConfidentialInstanceType()
}

// GetDefaultNodePoolDiskType returns a default node pool disk type.
func (p *gkeCloudProviderImpl) GetDefaultNodePoolDiskType() string {
	return p.gkeManager.GetDefaultNodePoolDiskType()
}

// GetDefaultNodePoolMinCpuPlatform returns a default node pool min cpu platform.
func (p *gkeCloudProviderImpl) GetDefaultNodePoolMinCpuPlatform() string {
	return p.gkeManager.GetDefaultNodePoolMinCpuPlatform()
}

// GetExistingNodeGroupLocations returns a list of locations for created node groups.
func (p *gkeCloudProviderImpl) GetExistingNodeGroupLocations() []string {
	return p.gkeManager.GetExistingNodeGroupLocations()
}

// GetAutoprovisioningLocations returns a list of locations where NAP can create new nodepools.
func (p *gkeCloudProviderImpl) GetAutoprovisioningLocations() []string {
	return p.gkeManager.GetAutoprovisioningLocations()
}

// ExistingMigsInNodePool returns a list of registered MIGs that belong to a given node pool.
func (p *gkeCloudProviderImpl) ExistingMigsInNodePool(nodePoolName string) []*GkeMig {
	return p.gkeManager.ExistingMigsInNodePool(nodePoolName)
}

// ValidateMachineTypeConfig validates the machine type config for a given zone.
func (p *gkeCloudProviderImpl) ValidateMachineTypeConfig(machineType, zone string) error {
	return p.gkeManager.ValidateMachineTypeConfig(machineType, zone)
}

// GetGkeMigs returns a list of registered migs in the current snapshot or an error on failure.
func (p *gkeCloudProviderImpl) GetGkeMigs() []*GkeMig {
	return p.gkeManager.GetGkeMigs()
}

// GetAllNodePoolNames returns all node pool names.
func (p *gkeCloudProviderImpl) GetAllNodePoolNames() sets.Set[string] {
	return p.gkeManager.GetAllNodePoolNames()
}

// IsClusterUsingPSCInfrastructure checks if cluster is using PSC infrastructure. If so, cluster support public and private nodes.
func (p *gkeCloudProviderImpl) IsClusterUsingPSCInfrastructure() bool {
	return p.gkeManager.IsClusterUsingPSCInfrastructure()
}

// IsAutopilotEnabled checks autopilot is enabled.
func (p *gkeCloudProviderImpl) IsAutopilotEnabled() bool {
	return p.autopilotEnabled
}

// IsDefaultCCCEnabled checks if the cluster has default CCC enabled.
func (p *gkeCloudProviderImpl) IsDefaultCCCEnabled() bool {
	return p.gkeManager.IsDefaultCCCEnabled()
}

// FetchCapacityGuidance queries the GCE for current capacity availability insights
// for a given set of desired instance configurations.
func (p *gkeCloudProviderImpl) FetchCapacityGuidance(ctx context.Context, flexibilityScopeKey string, instanceConfigs map[string]*api.InstanceConfig) (map[string]*api.InstanceAvailability, error) {
	return p.gkeManager.FetchCapacityGuidance(ctx, flexibilityScopeKey, instanceConfigs)
}

// SendCapacityDecision notifies a final provisioning decision, allowing it to
// propagate the decision back to GCE
func (p *gkeCloudProviderImpl) SendCapacityDecision(ctx context.Context, decision api.ProvisioningDecisionNotification) error {
	return p.gkeManager.SendCapacityDecision(ctx, decision)
}

// HasInstance returns whether a given node has a corresponding instance in this cloud provider.
// The method assumes that we only check existence of underlying when the node is marked for deletion.
// See b/380440556 for additional context.
func (p *gkeCloudProviderImpl) HasInstance(node *apiv1.Node) (bool, error) {
	// This is a quick return to not use gce cache when node is not being deleted.
	// Gce cache refreshes all MIG instances on miss, thus impacting CA performance.
	// For a being-deleted node we want to check underlying instance. If CA makes assumptions only based on taint,
	// a being-deleted node can be assume to be an upcoming node. See b/364972236 for additional context.
	if !taints.HasToBeDeletedTaint(node) {
		return true, nil
	}
	ref, err := gce.GceRefFromProviderId(node.Spec.ProviderID)
	if err != nil {
		return false, err
	}
	return p.InstanceByRef(ref) != nil, nil
}

// ResizeVm resizes a given Node to a given size.
func (p *gkeCloudProviderImpl) ResizeVm(ctx context.Context, node *apiv1.Node, desiredSize ekvmsize.VmSize) error {
	machineFamily, err := ekvms_utils.GetMachineFamilyName(node)
	if err != nil {
		return fmt.Errorf("aborting resize due to missing machine family, providerId: %q, error: %v", node.Spec.ProviderID, err)
	}
	ref, err := gce.GceRefFromProviderId(node.Spec.ProviderID)
	if err != nil {
		return ek_errors.NewGenericError(machineFamily, fmt.Errorf("aborting resize due to invalid GceRef, providerId: %q, error: %v", node.Spec.ProviderID, err), ek_errors.StartingState)
	}

	return p.gkeManager.ResizeVm(ctx, ref, desiredSize)
}

// GetCurrentResizableVmState fetches the current size of given resizable VM.
func (p *gkeCloudProviderImpl) GetCurrentResizableVmState(node *apiv1.Node) (ekvmtypes.ResizableVmState, error) {
	ref, err := gce.GceRefFromProviderId(node.Spec.ProviderID)
	if err != nil {
		return ekvmtypes.ResizableVmState{}, err
	}

	return p.gkeManager.GetCurrentResizableVmState(p.machineConfigProvider, ref)
}

// BulkFetchCurrentResizableVmStates fetches current sizes of resizable VMs from GCE.
func (p *gkeCloudProviderImpl) BulkFetchCurrentResizableVmStates() (map[gce.GceRef]ekvmtypes.ResizableVmState, error) {
	return p.gkeManager.BulkFetchCurrentResizableVmStates(p.machineConfigProvider)
}

// QueuedProvisioningNodeHasScaleDownImmunity returns true if the provided QueuedProvisioning node still shouldn't get scaled down,
// i.e. additionalImmunity hasn't ran out yet.
func (p *gkeCloudProviderImpl) QueuedProvisioningNodeHasScaleDownImmunity(node *apiv1.Node, migSpec *reconciler.QueuedProvisioningMigSpec, now time.Time) bool {
	return p.gkeManager.QueuedProvisioningNodeHasScaleDownImmunity(node, migSpec, now)
}

// InstanceByRef returns GceInstance from cache. It returns nil when the corresponding instance is not cached.
func (p *gkeCloudProviderImpl) InstanceByRef(ref gce.GceRef) *gce.GceInstance {
	return p.gkeManager.InstanceByRef(ref)
}

func (p *gkeCloudProviderImpl) TrimLocationsForMachineConfig(locations []string, machineType string, acceleratorConfig *gke_api_beta.AcceleratorConfig, minCpuPlatform string, diskType string) []string {
	return p.gkeManager.TrimLocationsForMachineConfig(locations, machineType, acceleratorConfig, minCpuPlatform, diskType)
}

// MigUpdateColor is the color of a MIG during a Blue/Green update.
type MigUpdateColor string

const (
	// BlueMig means the MIG is blue.
	BlueMig MigUpdateColor = "BLUE"
	// GreenMig means the MIG is green.
	GreenMig MigUpdateColor = "GREEN"
)

// MigBlueGreenInfo contains information about an ongoing Blue/Green update for a particular MIG.
type MigBlueGreenInfo struct {
	Color MigUpdateColor
	// Phase is actually defined at the node pool level, but we don't have a dedicated node pool object, so
	// it's just repeated in every MIG.
	Phase gkeclient.UpdatePhase
	// True if this is an Autoscaled Blue/Green update.
	IsAutoScaled bool
}

// String returns a string representation of the info.
func (i MigBlueGreenInfo) String() string {
	return fmt.Sprintf("<color: %s, phase: %s>", i.Color, i.Phase)
}

// GetDefaultEnablePrivateNodes return default value for enablePrivateNodes.
func (p *gkeCloudProviderImpl) GetDefaultEnablePrivateNodes() bool {
	return p.gkeManager.GetDefaultEnablePrivateNodes()
}

// CalculateOSPhysicalEphemeralStorageGiB find minimum Physical disk size that accommodate Allocatable
func (p *gkeCloudProviderImpl) CalculatePhysicalEphemeralStorageGiB(mig *GkeMig, allocatableBytes int64) int64 {
	return p.gkeManager.CalculatePhysicalEphemeralStorageGiB(mig, allocatableBytes)
}

// GetNodesScaleDownAllowedFromCache retrieves the scale-down information for nodes from the cache.
func (p *gkeCloudProviderImpl) GetNodesScaleDownAllowedFromCache(nodeNames []string) map[string]bool {
	return p.gkeManager.GetNodesScaleDownAllowedFromCache(nodeNames)
}

// UpdateNodesScaleDownAllowedCache updates the scale-down information for nodes in the cache.
func (p *gkeCloudProviderImpl) UpdateNodesScaleDownAllowedCache(nodesScaleDownAllowed map[string]bool) {
	p.gkeManager.UpdateNodesScaleDownAllowedCache(nodesScaleDownAllowed)
}

// InvalidateNodesScaleDownAllowed invalidates the cache storing information about whether nodes are allowed to be scaled down.
func (p *gkeCloudProviderImpl) InvalidateNodesScaleDownAllowedCache() {
	p.gkeManager.InvalidateNodesScaleDownAllowedCache()
}

// MachineConfigProvider returns the source for obtaining machine configuration.
func (p *gkeCloudProviderImpl) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return p.machineConfigProvider
}

// NodeConfig contantain information about configs of the GKE node pool this Mig belongs to.
type NodeConfig struct {
	ThreadsPerCore     int64
	Version            string
	IsConfidentialNode bool
}

// GkeMig represents the GKE Managed Instance Group implementation of a NodeGroup.
type GkeMig struct {
	gceRef gce.GceRef

	gkeManager         GkeManager
	minSize            int
	maxSize            int
	totalMinSize       int
	totalMaxSize       int
	locationPolicy     LocationPolicyEnum
	autoprovisioned    bool
	exist              bool
	nodeConfig         *NodeConfig
	spec               *gkeclient.NodePoolSpec
	blueGreenInfo      *MigBlueGreenInfo
	extraResources     map[string]resource.Quantity
	queuedProvisioning bool
	// shortLivedUpgradeInProgress is set when there are nodes in the MIG with InstanceTemplate different than the one assigned to MIG
	shortLivedUpgradeInProgress bool
	status                      string
	domainUrl                   string
	deploymentType              DeploymentTypeEnum

	nodePool *GkeNodePool

	id string
}

// NewGkeMig creates a new GkeMig instance and calculates its ID.
func NewGkeMig(gceRef gce.GceRef, domainUrl string, gkeManager GkeManager) *GkeMig {
	return &GkeMig{
		gceRef:     gceRef,
		domainUrl:  domainUrl,
		gkeManager: gkeManager,
		id:         gce.GenerateMigUrl(domainUrl, gceRef),
	}
}

// GkeNodePool represents a GKE node pool. From Cluster Autoscaler perspective,
// this is simply a grouping mechanism to store all MIGs associated with a given node pool.
type GkeNodePool struct {
	name string
	migs []*GkeMig
}

func (g *GkeNodePool) Name() string {
	return g.name
}

func (g *GkeNodePool) Migs() []*GkeMig {
	return g.migs
}

func AddMigsToNodePool(name string, migs ...*GkeMig) *GkeNodePool {
	np := &GkeNodePool{
		name: name,
		migs: migs,
	}
	for _, mig := range migs {
		mig.nodePool = np
	}
	return np
}

// NodeGroup extends cloudprovider.Nodegroup with GkeMig specific methods
type NodeGroup interface {
	cloudprovider.NodeGroup
	LocationPolicy() LocationPolicyEnum
	IsTpuMig() bool
	FlexStartNonQueued() bool
	IsUpcoming() bool
	GceRef() gce.GceRef
	Spec() *gkeclient.NodePoolSpec
	GetSCSILLocalSSDCount() int
	GetNVMELocalSSDCount() int
	NodePool() *GkeNodePool
	FlexStart() bool
	MachineType() string
	GetMig() *GkeMig
}

// DeploymentTypeEnum represents the type of reservation deployment that was used when creating the MIG.
type DeploymentTypeEnum string

const (
	// DeploymentTypeDense represents the DENSE deployment type used
	// for accelerator slices in gSC. The reserved capacity of such reservation
	// is made up of densely deployed reservation blocks.
	DeploymentTypeDense DeploymentTypeEnum = "DENSE"
	// DeploymentTypeUnspecified is the default reservation deployment type.
	DeploymentTypeUnspecified DeploymentTypeEnum = "UNSPECIFIED"
	// DeploymentTypeNone is the zero value of the enum, e.g. reservation was not used.
	DeploymentTypeNone DeploymentTypeEnum = "NONE"
)

// LocationPolicyEnum represents the spreading algorithm used when scaling up the cluster.
type LocationPolicyEnum string

const (
	// LocationPolicyAny represents the ANY location policy, which picks zones
	// that have the highest capacity available.
	LocationPolicyAny LocationPolicyEnum = "ANY"
	// LocationPolicyBalanced represents the BALANCED location policy, which is
	// a best effort policy that aims to balance the sizes of different zones.
	LocationPolicyBalanced LocationPolicyEnum = "BALANCED"
	// LocationPolicyUnspecified represents not set location policy.
	LocationPolicyUnspecified LocationPolicyEnum = "LOCATION_POLICY_UNSPECIFIED"
)

// GceRef returns Mig's GceRef
func (mig *GkeMig) GceRef() gce.GceRef {
	return mig.gceRef
}

// IsStable returns whether the MIG is stable.
func (mig *GkeMig) IsStable() (bool, error) {
	return mig.gkeManager.IsMigStable(mig)
}

// IsAutopilot returns whether a MIG was created in an Autopilot cluster. All MIGs created in AP clusters have the
// "gk3-" prefix in their name. In most scenarios, MIG's "Autopilotness" should match the cluster's Autopilot setting,
// and the cluster setting should be used for AP determination in most cases. One notable exception is during an ongoing
// cluster conversion between Standard/Autopilot.
func (mig *GkeMig) IsAutopilot() bool {
	return strings.HasPrefix(mig.gceRef.Name, "gk3-")
}

// Version returns corresponding node version.
func (mig *GkeMig) Version() string {
	if mig.Autoprovisioned() {
		if mig.spec != nil && mig.spec.NodeVersion != "" {
			return mig.spec.NodeVersion
		}
		return mig.gkeManager.GetClusterVersion()
	}
	if mig.nodeConfig == nil {
		return ""
	}
	return mig.nodeConfig.Version
}

func (mig *GkeMig) IsConfidentialNode() bool {
	if mig.gkeManager != nil && mig.gkeManager.AreConfidentialNodesEnabled() {
		return true
	}
	if mig.nodeConfig == nil {
		return false
	}
	return mig.nodeConfig.IsConfidentialNode
}

// GetSystemArchitecture returns node system architecture if specified or default one.
func (mig *GkeMig) GetSystemArchitecture() gce.SystemArchitecture {
	arch := *mig.Spec().SystemArchitecture
	if arch == gce.UnknownArch {
		klog.Warningf("Mig spec returned unknown system architecture, falling back to %q", gce.DefaultArch)
		arch = gce.DefaultArch
	}
	return arch
}

// NodePoolName returns the name of the GKE node pool this Mig belongs to.
func (mig *GkeMig) NodePoolName() string {
	if mig.nodePool == nil {
		return ""
	}
	return mig.nodePool.name
}

// NodePool returns the GKE node pool this Mig belongs to.
func (mig *GkeMig) NodePool() *GkeNodePool {
	return mig.nodePool
}

// GetNodeConfig returns the configs of the GKE node pool this Mig belongs to.
func (mig *GkeMig) GetNodeConfig() *NodeConfig {
	return mig.nodeConfig
}

// Spec returns specification of the Mig.
func (mig *GkeMig) Spec() *gkeclient.NodePoolSpec {
	return mig.spec
}

// ExtraResources returns extra resources requested by the Mig.
func (mig *GkeMig) ExtraResources() map[string]resource.Quantity {
	return mig.extraResources
}

// MachineType returns the machine type of the Mig.
func (mig *GkeMig) MachineType() string {
	if mig.spec == nil {
		return ""
	}
	return mig.spec.MachineType
}

// NodeTpuCount returns the TPU count of each node in the Mig.
func (mig *GkeMig) NodeTpuCount() (int64, error) {
	return mig.gkeManager.MachineConfigProvider().GetTpuCountForMachineType(mig.MachineType())
}

// MachineType returns the machine type of the Mig.
func (mig *GkeMig) DiskSize() int64 {
	if mig.spec == nil {
		return 0
	}
	return mig.spec.DiskSize
}

// MaxSize returns the maximum number of nodes that this node group can have. It is the minimum of maxSize, totalMaxSize (if set), and the default MIG size of 2000.
func (mig *GkeMig) MaxSize() int {
	if !mig.TotalSizeLimitEnabled() {
		return min(mig.maxSize, gceLimitOnMigSizes)
	}
	// We change the semantics of the MaxSize when the total size is enabled.
	// The total max size is later enforced by the TotalMaxSizeProcessor.
	// For more details refer to: go/improve-nodepool-size-control-dd
	otherMigsSize, err := mig.otherMigsTargetSize()
	if err != nil {
		klog.Errorf("Couldn't get size of other migs, received error: %v", err)
		// Return min size to avoid scaling up.
		return minAutoprovisionedSize
	}
	return min(gceLimitOnMigSizes, max(0, mig.totalMaxSize-otherMigsSize))
}

// SetDiskSize sets the disk size setting.
func (mig *GkeMig) SetDiskSize(diskSize int64) error {
	if mig.exist {
		return ca_errors.NewAutoscalerErrorf(ca_errors.CloudProviderError, "cannot set disk size for existing node pool %s", mig.NodePoolName())
	}
	mig.spec.DiskSize = diskSize
	return nil
}

// SetMaxPodsPerNode sets the max pods per node setting.
func (mig *GkeMig) SetMaxPodsPerNode(mppn int64) error {
	if mig.exist {
		return ca_errors.NewAutoscalerErrorf(ca_errors.CloudProviderError, "cannot set max pods per node for existing node pool %s", mig.NodePoolName())
	}
	mig.spec.MaxPodsPerNode = mppn
	return nil
}

// MinSize returns minimum size of the node group.
func (mig *GkeMig) MinSize() int {
	bgInfo := mig.BlueGreenInfo()
	if bgInfo != nil && bgInfo.IsAutoScaled && bgInfo.Color == BlueMig && bgInfo.Phase == gkeclient.PhaseWaitingToDrainBluePool {
		// This is to allow scaling down the blue pool to zero nodes during the
		// WAITING_TO_DRAIN_BLUE_POOL phase of Autoscaled blue-green upgrade.
		return 0
	}
	if !mig.TotalSizeLimitEnabled() {
		return mig.minSize + mig.gkeManager.GetNumberOfSurgeNodesInMig(mig)
	}

	// We change the semantics of the MinSize when the total size is enabled.
	// The total min size is later enforced by the TotalMinSizeProcessor.
	// For more details refer to: go/improve-nodepool-size-control-dd
	otherMigsSize, err := mig.otherMigsTargetSize()
	if err != nil {
		klog.Errorf("Couldn't get size of other migs, received error: %v", err)
		// Return max size to avoid scaling down.
		return mig.maxSize
	}
	return max(0, mig.totalMinSize-otherMigsSize) + mig.gkeManager.GetNumberOfSurgeNodesInMig(mig)
}

// TotalSizeLimitEnabled returns true if the size of the node group is limited
// on the nodepool level, rather than on the MIG level.
func (mig *GkeMig) TotalSizeLimitEnabled() bool {
	return mig.totalMaxSize > 0
}

// TotalMaxSize returns total maximum size of the node pool.
func (mig *GkeMig) TotalMaxSize() int {
	return mig.totalMaxSize
}

// TotalMinSize returns total minimum size of the node pool.
func (mig *GkeMig) TotalMinSize() int {
	bgInfo := mig.BlueGreenInfo()
	if bgInfo != nil && bgInfo.IsAutoScaled && bgInfo.Color == BlueMig && bgInfo.Phase == gkeclient.PhaseWaitingToDrainBluePool {
		// This is to allow scaling down the blue pool to zero nodes during the
		// WAITING_TO_DRAIN_BLUE_POOL phase of Autoscaled blue-green upgrade.
		return 0
	}
	return mig.totalMinSize
}

// LocationPolicy returns location policy used in a node group.
func (mig *GkeMig) LocationPolicy() LocationPolicyEnum {
	return mig.locationPolicy
}

// TargetSize returns the current TARGET size of the node group. It is possible that the
// number is different from the number of nodes registered in Kubernetes.
func (mig *GkeMig) TargetSize() (int, error) {
	upcoming := mig.IsUpcoming()
	if !mig.exist && !upcoming {
		return 0, nil
	}
	size, err := mig.gkeManager.GetMigSize(mig)
	if err != nil {
		return 0, err
	}
	if upcoming {
		return int(size), nil
	}
	// If TPU mig, check resize requests. In case of a failed resize request add the
	// latest resize requests count to the target size as well because failed resize
	// requests change the target size back when they fail
	if mig.IsMultiHostTpuMig() && mig.gkeManager.IsResizeRequestErrorHandlingEnabled() {
		rrStatus, err := mig.ResizeRequests()
		if err != nil {
			klog.Errorf("Error getting resize requests for TPU mig %q: %v", mig.gceRef.Name, err)
		}
		// Resize requests should be ordered by their creation timestamp and we are only interested
		// in the most recent one.

		// TODO(b/395836275): Sorting by timestamp was removed in gkecl/846107, so there should be no guarantee
		// that the first RR is the most recent one.
		if len(rrStatus) > 0 && rrStatus[0].State == resizerequestclient.ResizeRequestStateFailed {
			size += rrStatus[0].ResizeBy
		}
	}
	if mig.FlexStartNonQueued() && mig.gkeManager.IsResizeRequestErrorHandlingEnabled() {
		// Overwriting TargetSize prevents scale up race condition, i.e.
		// if we don’t manage to handle the Failed/Canceled Resize Requests before the next scale up attempt,
		// the unschedulable pod that was initially supposed to use the node from that Resize Request scale up will trigger a new scale up,
		// while the node pool should’ve been in backoff.
		// Further reading in: go/gke-dws-flex-start-design-ca-changes
		rrs, err := mig.ResizeRequests()
		if err != nil {
			klog.Errorf("Error getting resize requests for FlexStart mig %q: %v", mig.gceRef.Name, err)
		}
		for _, rr := range rrs {
			if mig.gkeManager.ReportState(rr) == resizerequestclient.AlreadyReportedState {
				continue
			}
			// GCE decreases MIG's TargetSize when a RR is in one of the following states (https://cloud.google.com/compute/docs/instance-groups/about-resize-requests-mig)
			if rr.State == resizerequestclient.ResizeRequestStateFailed || rr.State == resizerequestclient.ResizeRequestStateCancelled {
				size += rr.ResizeBy
			}
		}
	}
	return int(size), err
}

// increaseViaCreateInstances creates CreateInstances and handle errors
func increaseViaCreateInstances(mig *GkeMig, delta int64) error {
	err := mig.gkeManager.CreateInstances(mig, int64(delta))
	return convertMigResizeError(err)
}

// increaseViaResizeRequest creates ResizeRequest and handle errors
func increaseViaResizeRequest(mig *GkeMig, delta int64) error {
	if err := mig.CreateResizeRequest(int(delta)); err != nil {
		err = ca_errors.ToAutoscalerError(ca_errors.CloudProviderError, err).
			AddPrefix("failed to increase node group size atomically (Flex Start: %v): ", mig.FlexStartNonQueued())
		return err
	}
	return nil
}

// increaseViaFlexResizeRequests creates FlexResizeRequests and handle errors
func increaseViaFlexResizeRequests(mig *GkeMig, delta int64) error {
	if err := mig.gkeManager.CreateFlexResizeRequests(mig, delta); err != nil {
		err = ca_errors.ToAutoscalerError(ca_errors.CloudProviderError, err).
			AddPrefix("failed to increase node group size using DWS Flex Start Non-Queued Resize Requests: ")
		return err
	}
	return nil
}

// IncreaseSize increases Mig size
func (mig *GkeMig) IncreaseSize(delta int) error {
	if err := mig.validateSizeIncrease(delta); err != nil {
		return err
	}
	if mig.IsMultiHostTpuMig() {
		// MultiHost tpus always use ResizeRequest, no matter the flex
		return increaseViaResizeRequest(mig, int64(delta))
	}
	if mig.UsesBulkProvisioning() {
		// flex and non-flex BulkMIGs use CreateInstances
		return increaseViaCreateInstances(mig, int64(delta))
	} else if mig.FlexStart() && mig.IsSingleHostTpuMig() {
		// Single host flex TPU migs use CreateInstances
		return increaseViaCreateInstances(mig, int64(delta))
	} else if mig.FlexStart() {
		if mig.gkeManager.ExperimentsManager().EvaluateMinimumVersionFlagOrFailsafe(experiments.FlexStartNonQueuedTrickleModeMinCAVersionFlag, false) && mig.gkeManager.ExperimentsManager().EvaluateBoolFlagOrFailsafe(experiments.FlexStartNonQueuedTrickleModeEnabledFlag, true) {
			return increaseViaCreateInstances(mig, int64(delta))
		}
		// Other flex configurations use FlexResizeRequest
		return increaseViaFlexResizeRequests(mig, int64(delta))
	} else {
		// Single host non-flex TPUs and other unspecified configurations use CreateInstances
		return increaseViaCreateInstances(mig, int64(delta))
	}
}

// CreateQueuedInstances queues creation of VMs
func (mig *GkeMig) CreateQueuedInstances(pr prpods.ProvReqID, delta int, shouldUpdateProvReqDetails manager.ShouldUpdateProvReqDetails) error {
	if err := mig.validateSizeIncrease(delta); err != nil {
		return err
	}
	err := mig.gkeManager.CreateQueuedInstances(pr, mig, int64(delta), shouldUpdateProvReqDetails)
	return convertMigResizeError(err)
}

func (mig *GkeMig) UpdateNodePoolLabels(labels map[string]string) error {
	return mig.gkeManager.UpdateNodePoolLabels(mig.NodePoolName(), labels)
}

// CreateResizeRequest creates an atomic Resize Request with the Resize Request API.
func (mig *GkeMig) CreateResizeRequest(delta int) error {
	if err := mig.validateSizeIncrease(delta); err != nil {
		return err
	}
	err := mig.gkeManager.CreateResizeRequest(mig, int64(delta))
	return convertMigResizeError(err)
}

// AdvanceResizeRequestCleanUp (re)triggers a new cancel/delete operation based on the Resize Request state or checks the status of the existing operation.
func (mig *GkeMig) AdvanceResizeRequestCleanUp(resizeRequest resizerequestclient.ResizeRequestStatus) error {
	if !mig.exist {
		return fmt.Errorf("cannot delete resize request on non-existent mig: %q, resize request name: %q", mig.GceRef(), resizeRequest.Name)
	}
	return mig.gkeManager.AdvanceResizeRequestCleanUp(resizeRequest)
}

// ResetFailedResizeRequestsCreation retrieves the failed Resize Request creation error-count entries and clears the map.
func (mig *GkeMig) ResetFailedResizeRequestsCreation() map[error]int {
	return mig.gkeManager.ResetFailedResizeRequestsCreation(mig.gceRef)
}

// ReportState returns the report state of the particular Resize Request
func (mig *GkeMig) ReportState(resizeRequest resizerequestclient.ResizeRequestStatus) resizerequestclient.ResizeRequestReportState {
	return mig.gkeManager.ReportState(resizeRequest)
}

// SetReportState sets the report state of the particular Resize Request
func (mig *GkeMig) SetReportState(resizeRequest resizerequestclient.ResizeRequestStatus, state resizerequestclient.ResizeRequestReportState) {
	mig.gkeManager.SetReportState(resizeRequest, state)
}

func (mig *GkeMig) validateSizeIncrease(delta int) error {
	if delta <= 0 {
		return fmt.Errorf("size increase must be positive")
	}
	size, err := mig.gkeManager.GetMigSize(mig)
	if err != nil {
		return err
	}
	desiredSize := int(size) + delta
	if desiredSize > mig.MaxSize() {
		return fmt.Errorf("size increase too large - desired:%d max:%d", desiredSize, mig.MaxSize())
	}
	return nil
}

func convertMigResizeError(err error) error {
	if IsServiceAccountDeletedError(err) {
		err = ca_errors.NewAutoscalerError(ca_errors.AutoscalerErrorType(gkeclient.ServiceAccountDeleted), err.Error()).AddPrefix("service account deleted: ")
	}
	if IsOutOfQuotaError(err) {
		err = ca_errors.NewAutoscalerError(ca_errors.AutoscalerErrorType(gce.ErrorCodeQuotaExceeded), err.Error()).AddPrefix("insufficient regional quota for project: ")
	}
	if IsInvalidReservationError(err) {
		err = ca_errors.NewAutoscalerError(ca_errors.AutoscalerErrorType(gce.ErrorInvalidReservation), err.Error()).AddPrefix("invalid reservation: ")
	}
	if IsReservationNotReadyError(err) {
		err = ca_errors.NewAutoscalerError(ca_errors.AutoscalerErrorType(gce.ErrorReservationNotReady), err.Error()).AddPrefix("reservation not ready: ")
	}
	return err
}

// ResizeRequests returns all Resize Requests for a given node group.
func (mig *GkeMig) ResizeRequests() ([]resizerequestclient.ResizeRequestStatus, error) {
	if !mig.exist {
		return []resizerequestclient.ResizeRequestStatus{}, nil
	}
	return mig.gkeManager.ResizeRequests(mig)
}

// DecreaseTargetSize decreases the target size of the node group. This function
// doesn't permit to delete any existing node and can be used only to reduce the
// request for new nodes that have not been yet fulfilled. Delta should be negative.
func (mig *GkeMig) DecreaseTargetSize(delta int) error {
	if delta >= 0 {
		return fmt.Errorf("size decrease must be negative")
	}
	size, err := mig.gkeManager.GetMigSize(mig)
	if err != nil {
		return err
	}
	nodes, err := mig.gkeManager.GetMigNodes(mig)
	if err != nil {
		return err
	}
	if int(size)+delta < len(nodes) {
		return fmt.Errorf("attempt to delete existing nodes targetSize:%d delta:%d existingNodes: %d",
			size, delta, len(nodes))
	}
	return mig.gkeManager.SetMigSize(mig, size+int64(delta))
}

// Belongs returns true if the given node belongs to the NodeGroup.
func (mig *GkeMig) Belongs(node *apiv1.Node) (bool, error) {
	ref, err := gce.GceRefFromProviderId(node.Spec.ProviderID)
	if err != nil {
		return false, err
	}
	targetMig, err := mig.gkeManager.GetMigForInstance(ref)
	if err != nil {
		return false, err
	}
	if targetMig == nil {
		return false, fmt.Errorf("%s doesn't belong to a known mig", node.Name)
	}
	if targetMig.Id() != mig.Id() {
		return false, nil
	}
	return true, nil
}

// NodePoolTargetSize returns the sum of current target sizes of all node groups
// within a GKE node pool. This takes into account the Blue/Green upgrades, for
// which we count only migs with the color of the mig for which the function
// was called.
func (mig *GkeMig) NodePoolTargetSize() (int, error) {
	if !mig.exist {
		return 0, nil
	}
	var migGceRefs []gce.GceRef
	var candidateMigs []*GkeMig

	if mig.nodePool != nil {
		candidateMigs = mig.nodePool.migs
	} else {
		klog.Warningf("GkeMig.nodePool is nil for %s, falling back to O(N) lookup. This should not happen in production.", mig.Id())
		for _, m := range mig.gkeManager.GetGkeMigs() {
			if mig.NodePoolName() == m.NodePoolName() {
				candidateMigs = append(candidateMigs, m)
			}
		}
	}

	for _, m := range candidateMigs {
		if mig.BlueGreenInfo() != nil && m.BlueGreenInfo() != nil &&
			mig.BlueGreenInfo().Color != m.BlueGreenInfo().Color {
			continue
		}
		migGceRefs = append(migGceRefs, m.GceRef())
	}

	migsTargetSize, err := mig.gkeManager.GetMigsTargetSize(migGceRefs)
	if err != nil {
		return 0, err
	}
	return int(migsTargetSize), nil
}

// TODO(b/517096955): Implement AtomicIncreaseSize
// ref: https://github.com/kubernetes/autoscaler/commit/9cdced4cfd7a4aef9bdf23d56ee430085d13b57f
func (mig *GkeMig) AtomicIncreaseSize(delta int) error {
	return cloudprovider.ErrNotImplemented
}

// otherMigsTargetSize returns the sum of current TARGET sizes of all other node
// groups within a GKE node pool. This takes into account the Blue/Green upgrades,
// for which we count only migs with the color of the mig for which the function
// was called. Such approach allows us to defer the decision, which color should
// be scaled up or down, to the corresponding source, in this case to the
// `bluegreen.BlockedMigsSource`.
func (mig *GkeMig) otherMigsTargetSize() (int, error) {
	nodePoolTargetSize, err := mig.NodePoolTargetSize()
	if err != nil {
		return 0, fmt.Errorf("could not get target size for nodepool (%s), got error: %w", mig.NodePoolName(), err)
	}
	migTargetSize, err := mig.TargetSize()
	if err != nil {
		return 0, fmt.Errorf("could not get target size for mig (%s), got error: %w", mig.Id(), err)
	}
	return int(nodePoolTargetSize - migTargetSize), nil
}

// MinSizeReachedError represents a failure to delete nodes because the minimum size
// of their MIG had already been reached.
type MinSizeReachedError struct{}

func (err MinSizeReachedError) Error() string {
	return "min size reached, nodes will not be deleted"
}

// InvalidAtomicResizeError represents an internal error where CA attempted to partially resize
// a node pool that should be resized atomically.
type InvalidAtomicResizeError struct{}

func (err InvalidAtomicResizeError) Error() string {
	return "invalid partial resize of a node group that should be resized atomically"
}

// ForceDeleteNodes deletes nodes from this node group, without checking for
// constraints like minimal size validation etc.
func (mig *GkeMig) ForceDeleteNodes(nodes []*apiv1.Node) error {
	refs := make([]gce.GceRef, 0, len(nodes))
	for _, node := range nodes {
		belongs, err := mig.Belongs(node)
		if err != nil {
			return err
		}
		if !belongs {
			return fmt.Errorf("%s belong to a different mig than %s", node.Name, mig.Id())
		}
		gceref, err := gce.GceRefFromProviderId(node.Spec.ProviderID)
		if err != nil {
			return err
		}
		refs = append(refs, gceref)
	}
	return mig.gkeManager.DeleteInstances(refs)
}

// DeleteNodes deletes the nodes from the group.
func (mig *GkeMig) DeleteNodes(nodes []*apiv1.Node) error {
	size, err := mig.gkeManager.GetMigSize(mig)
	if err != nil {
		return err
	}
	if int(size) <= mig.MinSize() {
		return MinSizeReachedError{}
	}
	if mig.ResizeAtomically() && int(size) != len(nodes) {
		klog.Infof("Scaling down partially healthy atomic node group with %d nodes (vs desired size %d)", len(nodes), size)
	}
	return mig.ForceDeleteNodes(nodes)
}

// Id returns mig url.
func (mig *GkeMig) Id() string {
	if mig.id != "" {
		return mig.id
	}
	// This generally shouldn't happen outside of test code.
	return gce.GenerateMigUrl(mig.domainUrl, mig.gceRef)
}

// Debug returns a debug string for the Mig.
func (mig *GkeMig) Debug() string {
	return fmt.Sprintf("%s (%d:%d)", mig.Id(), mig.MinSize(), mig.MaxSize())
}

// Nodes returns a list of all nodes that belong to this node group.
func (mig *GkeMig) Nodes() ([]cloudprovider.Instance, error) {
	if !mig.Exist() {
		// GetMigNodes() also returns an empty list for upcoming and injected nodes.
		// Check here to prevent a case with a race condition during instance cache refresh for upcoming MIGs that were created in the meantime.
		return nil, nil
	}
	gceInstances, err := mig.gkeManager.GetMigNodes(mig)
	if err != nil {
		return nil, err
	}
	instances := make([]cloudprovider.Instance, len(gceInstances))
	for i, inst := range gceInstances {
		instances[i] = inst.Instance
	}
	return instances, nil
}

// Exist checks if the node group really exists on the cloud provider side. Allows to tell the
// theoretical node group from the real one.
func (mig *GkeMig) Exist() bool {
	return mig.exist
}

// Create is not implemented. Use AutoprovisionedCreate instead.
func (mig *GkeMig) Create() (cloudprovider.NodeGroup, error) {
	return nil, fmt.Errorf("Not implemented. Use AutoprovisionedCreate")
}

// AutoprovisionedCreate creates the node group on the cloud provider side.
func (mig *GkeMig) AutoprovisionedCreate() (nap_interfaces.CreateNodePoolResult, error) {
	if err := validateForCreation(mig); err != nil {
		return nap_interfaces.CreateNodePoolResult{}, err
	}
	result, err := mig.gkeManager.CreateNodePool(mig)
	if err != nil {
		return nap_interfaces.CreateNodePoolResult{}, err
	}
	return convertMigCreateNodePoolResult(result), nil
}

// CreateAsync asynchronously creates the node group on the cloud provider side.
func (mig *GkeMig) CreateAsync(updater nap_interfaces.AsyncNodeGroupUpdater, initializer nap_interfaces.AsyncNodeGroupInitializer) (nap_interfaces.CreateNodePoolResult, error) {
	if err := validateForCreation(mig); err != nil {
		return nap_interfaces.CreateNodePoolResult{}, err
	}
	result, err := mig.gkeManager.CreateNodePoolAsync(mig, updater, initializer)
	if err != nil {
		return nap_interfaces.CreateNodePoolResult{}, err
	}
	return convertMigCreateNodePoolResult(result), nil
}

func validateForCreation(mig *GkeMig) error {
	if mig.exist {
		return fmt.Errorf("cannot create already existing node group")
	}
	if !mig.autoprovisioned {
		return fmt.Errorf("cannot create non-autoprovisioned node group")
	}
	if mig.IsUpcoming() {
		return fmt.Errorf("cannot create upcoming node group")
	}
	return nil
}

// IsUpcoming checks if the node group is being created asynchronously.
func (mig *GkeMig) IsUpcoming() bool {
	// Some tests don't setup gkeManager in migs
	if mig.gkeManager == nil {
		return false
	}
	return mig.gkeManager.IsUpcoming(mig)
}

func convertMigCreateNodePoolResult(migResult MigCreateNodePoolResult) nap_interfaces.CreateNodePoolResult {
	gkeResult := nap_interfaces.CreateNodePoolResult{}
	gkeResult.MainCreatedNodeGroup = migResult.MainCreatedMig
	gkeResult.ExtraCreatedNodeGroups = make([]nap_interfaces.AutoprovisionedNodeGroup, 0, len(migResult.ExtraCreatedMigs))
	gkeResult.ExtraCreatedNodeGroups = convertMigsToAutoprovisioning(migResult.ExtraCreatedMigs)
	return gkeResult
}

func convertMigsToAutoprovisioning(migs []*GkeMig) []nap_interfaces.AutoprovisionedNodeGroup {
	result := make([]nap_interfaces.AutoprovisionedNodeGroup, 0, len(migs))
	for _, mig := range migs {
		result = append(result, mig)
	}
	return result
}

// Delete deletes the node group on the cloud provider side.
// This will be executed only for autoprovisioned node groups, once their size drops to 0.
func (mig *GkeMig) Delete() error {
	if err := validateForDeletion(mig); err != nil {
		return err
	}
	return mig.gkeManager.DeleteNodePool(mig)
}

func (mig *GkeMig) DeleteAsync(finalizer nap_interfaces.AsyncNodeGroupFinalizer) error {
	if err := validateForDeletion(mig); err != nil {
		return err
	}
	return mig.gkeManager.DeleteNodePoolAsync(mig, finalizer)
}

func validateForDeletion(mig *GkeMig) error {
	if !mig.exist {
		return fmt.Errorf("cannot delete non-existent node group")
	}
	if !mig.autoprovisioned {
		return fmt.Errorf("cannot delete non-autoprovisioned node group")
	}
	return nil
}

// Autoprovisioned returns true if the node group is autoprovisioned.
func (mig *GkeMig) Autoprovisioned() bool {
	return mig.autoprovisioned
}

// GetInstanceLimit returns the mig's instance limit. See https://cloud.google.com/compute/docs/instance-groups/add-remove-vms-in-mig#increase_the_groups_size_limit.
func (mig *GkeMig) GetInstanceLimit() int {
	if !mig.Exist() {
		// MIGs are paginated by default. Controlled by ListManagedInstanceEnablePaginationExperiment
		return gceclient.PaginatedMigInstanceLimit
	}
	results, err := mig.gkeManager.GetListManagedInstancesResults(mig.GceRef())
	if err != nil {
		results = MigPaginated
		klog.Errorf("Error when fetching the pagination behavior of mig: %s. Defaulting to %s. Error: %v", mig.GceRef().String(), results, err.Error())
	}
	if results == MigPaginated {
		return gceclient.PaginatedMigInstanceLimit
	}
	return gceclient.PagelessMigInstanceLimit
}

func (mig *GkeMig) getMaxNodeProvisionTime(defaultValue time.Duration) time.Duration {
	if overrideValue, hasOverride := mig.gkeManager.GetMaxNodeProvisioningTimeOverride(mig); hasOverride {
		return overrideValue
	}
	if mig.QueuedProvisioning() {
		return queuedProvisioningMaxNodeProvisionTime
	}
	if mig.FlexStartNonQueued() {
		customTime, err := mig.CapacityCheckWaitTimeSeconds()
		if err == nil {
			// `MaxNodeProvisionTime` is set to be `CapacityCheckWaitTimeSeconds` + an offset for the VM to actually register.
			return customTime + maxNodeProvisionTimeOffset
		}
		klogx.V(3).Infof("Unable to fetch CapacityCheckWaitTimeSeconds value (mig=%s): %v", mig.Id(), err)
		return DefaultFlexStartCapacityCheckWaitTime + maxNodeProvisionTimeOffset
	}
	if mig.IsSingleHostTpuMig() {
		return tpuMigMaxNodeProvisionTime
	}
	if mig.IsMultiHostTpuMig() {
		customTime, err := mig.CapacityCheckWaitTimeSeconds()
		if err == nil {
			// `MaxNodeProvisionTime` is set to be `CapacityCheckWaitTimeSeconds` + an offset for the VM to actually register.
			return customTime + maxNodeProvisionTimeOffset
		}
		klogx.V(3).Infof("Unable to fetch CapacityCheckWaitTimeSeconds value (mig=%s): %v", mig.Id(), err)
		return tpuMigMaxNodeProvisionTime
	}

	return defaultValue
}

// GetOptions returns NodeGroupAutoscalingOptions that should be used for this particular
// NodeGroup. Returning a nil will result in using default options.
func (mig *GkeMig) GetOptions(defaults config.NodeGroupAutoscalingOptions) (*config.NodeGroupAutoscalingOptions, error) {
	opts := defaults

	opts.MaxNodeProvisionTime = mig.getMaxNodeProvisionTime(opts.MaxNodeProvisionTime)
	opts.ZeroOrMaxNodeScaling = mig.ResizeAtomically()
	opts.AllowNonAtomicScaleUpToMax = mig.IsGpuAcceleratorSlice()

	warningLogQuota := klogx.NewLoggingQuota(1)
	if unreadyTime, found := mig.gkeManager.ScaleDownUnreadyTimeOverride(mig); found {
		opts.ScaleDownUnreadyTime = unreadyTime
	}

	if unneededTime, found, err := mig.gkeManager.ScaleDownUnneededTimeOverride(mig); found && err == nil {
		opts.ScaleDownUnneededTime = unneededTime
	} else if err != nil {
		klogx.V(3).UpTo(warningLogQuota).Infof("Unable to fetch nodegroup autoscaling options (mig=%s): %v", mig.Id(), err)
	}

	if threshold, found, err := mig.gkeManager.ScaleDownUtilizationThresholdOverride(mig); found && err == nil {
		opts.ScaleDownUtilizationThreshold = threshold
	} else if err != nil {
		klogx.V(3).UpTo(warningLogQuota).Infof("Unable to fetch nodegroup autoscaling options (mig=%s): %v", mig.Id(), err)
	}

	if gpuThreshold, found, err := mig.gkeManager.ScaleDownGpuUtilizationThresholdOverride(mig); found && err == nil {
		opts.ScaleDownGpuUtilizationThreshold = gpuThreshold
	} else if err != nil {
		klogx.V(3).UpTo(warningLogQuota).Infof("Unable to fetch nodegroup autoscaling options (mig=%s): %v", mig.Id(), err)
	}

	return &opts, nil
}

// TemplateNodeLabels returns labels for the template node for this node group.
func (mig *GkeMig) TemplateNodeLabels() (map[string]string, error) {
	nodeInfo, err := mig.gkeManager.GetMigTemplateNodeInfo(mig)
	if err != nil {
		return nil, err
	}
	return nodeInfo.Node().GetLabels(), nil
}

// TemplateNodeInfo returns a node template for this node group.
func (mig *GkeMig) TemplateNodeInfo() (*framework.NodeInfo, error) {
	return mig.gkeManager.GetMigTemplateNodeInfo(mig)
}

// InstanceTemplateId returns a node template id for this node group.
func (mig *GkeMig) InstanceTemplateId() (uint64, error) {
	template, err := mig.gkeManager.GetMigInstanceTemplate(mig)
	if err != nil {
		return 0, err
	}

	return template.Id, err
}

// BlueGreenInfo returns Blue/Green update info when the MIG takes part in an ongoing Blue/Green update, and nil otherwise.
func (mig *GkeMig) BlueGreenInfo() *MigBlueGreenInfo {
	return mig.blueGreenInfo
}

// Status returns the status of the nodepool.
func (mig *GkeMig) Status() string {
	return mig.status
}

func (mig *GkeMig) GetDeploymentType() DeploymentTypeEnum {
	return mig.deploymentType
}

func (mig *GkeMig) UsesReservation() bool {
	return mig.GetDeploymentType() != DeploymentTypeNone
}

// GkeMigOsInfo represents the GKE specific implemetation of MigOsInfo interface, that stores
// data that is needed for os reserved calculator.
type GkeMigOsInfo struct {
	os               gce.OperatingSystem
	osDistribution   gce.OperatingSystemDistribution
	arch             gce.SystemArchitecture
	nodeVersion      string
	confidentialNode bool
}

func (m *GkeMigOsInfo) ConfidentialNode() bool {
	return m.confidentialNode
}

// Os return operating system.
func (m *GkeMigOsInfo) Os() gce.OperatingSystem {
	return m.os
}

// OsDistribution return operating system distribution.
func (m *GkeMigOsInfo) OsDistribution() gce.OperatingSystemDistribution {
	return m.osDistribution
}

// Arch return system architecture
func (m *GkeMigOsInfo) Arch() gce.SystemArchitecture {
	return m.arch
}

// NodeVersion return the gke version of nodes managed by mig.
func (m *GkeMigOsInfo) NodeVersion() string {
	return m.nodeVersion
}

// NewGkeMigOsInfo initialize GkeMigOsInfo.
func NewGkeMigOsInfo(m gce.MigOsInfo, nodeVersion string, confidentialNode bool) *GkeMigOsInfo {
	return &GkeMigOsInfo{
		os:               m.Os(),
		osDistribution:   m.OsDistribution(),
		arch:             m.Arch(),
		nodeVersion:      nodeVersion,
		confidentialNode: confidentialNode,
	}
}

// QueuedProvisioning returns the value of the `QueuedProvisioning` flag.
func (mig *GkeMig) QueuedProvisioning() bool {
	return mig.queuedProvisioning
}

// FlexStart returns true if mig is flex
func (mig *GkeMig) FlexStart() bool {
	if mig.spec == nil {
		return false
	}
	return mig.spec.FlexStart
}

// FlexStartNonQueued returns whether the node pool's provisioning model is DWS Flex Start Non-Queued Provisioning (FSNQ), see go/gke-dws-flex-start-design
func (mig *GkeMig) FlexStartNonQueued() bool {
	if mig.spec == nil {
		return false
	}
	return mig.spec.FlexStart && !mig.queuedProvisioning
}

// CapacityCheckWaitTimeSeconds returns time duration indicating for how long the scale up in given MIG will be waiting for capacity before failing
// and error if the feature is not supported for particular MIG.
// Returns the custom value if CapacityCheckWaitTimeSecondsLabel is present, otherwise either the default value base on experiment is returned
// or error if the feature is not supported by the particular MIG.
func (mig *GkeMig) CapacityCheckWaitTimeSeconds() (time.Duration, error) {
	return mig.gkeManager.CapacityCheckWaitTimeSeconds(mig)
}

// ShortLivedUpgradeInProgress returns whether there's currently a ShortLived upgrade running in the MIG
func (mig *GkeMig) ShortLivedUpgradeInProgress() bool {
	return mig.shortLivedUpgradeInProgress
}

// IsTpuMig returns whether the mig is a TPU mig
func (mig *GkeMig) IsTpuMig() bool {
	if mig.spec == nil {
		return false
	}
	return mig.spec.TpuType != ""
}

// IsSingleHostTpuMig returns whether the mig is a single host TPU mig
func (mig *GkeMig) IsSingleHostTpuMig() bool {
	return mig.IsTpuMig() && !mig.spec.TpuMultiHost
}

// IsMultiHostTpuMig returns whether the mig is a multi host TPU mig
func (mig *GkeMig) IsMultiHostTpuMig() bool {
	return mig.IsTpuMig() && mig.spec.TpuMultiHost
}

// GetNVMELocalSSDCount returns the count of local SSDs with NVME interface.
func (mig *GkeMig) GetNVMELocalSSDCount() int {
	if mig.spec == nil || mig.spec.LocalSSDConfig == nil {
		return 0
	}
	migLocalSSDConfig := mig.spec.LocalSSDConfig
	count := 0
	if migLocalSSDConfig.EphemeralStorageConfig != nil {
		count += int(migLocalSSDConfig.EphemeralStorageConfig.LocalSsdCount)
	}
	if migLocalSSDConfig.EphemeralStorageLocalSsdConfig != nil {
		count += int(migLocalSSDConfig.EphemeralStorageLocalSsdConfig.LocalSsdCount)
	}
	if migLocalSSDConfig.LocalNvmeSsdBlockConfig != nil {
		count += int(migLocalSSDConfig.LocalNvmeSsdBlockConfig.LocalSsdCount)
	}
	if mig.spec.LinuxNodeConfig != nil && mig.spec.LinuxNodeConfig.SwapConfig != nil {
		swapConfig := mig.spec.LinuxNodeConfig.SwapConfig
		if swapConfig.Enabled && swapConfig.DedicatedLocalSsdProfile != nil {
			count += int(swapConfig.DedicatedLocalSsdProfile.DiskCount)
		}
	}
	return count
}

// GetSCSILLocalSSDCount returns the count of local SSDs with SCSI interface.
func (mig *GkeMig) GetSCSILLocalSSDCount() int {
	if mig.spec.LocalSSDConfig == nil {
		return 0
	}
	return int(mig.spec.LocalSSDConfig.LocalSsdCount)
}

func (mig *GkeMig) Accelerators() string {
	if mig.spec == nil {
		return ""
	}
	if mig.spec.Accelerators != nil {
		accTypes := []string{}
		for _, acc := range mig.spec.Accelerators {
			if acc != nil {
				accTypes = append(accTypes, acc.AcceleratorType)
			}
		}
		return strings.Join(accTypes, ",")
	}
	return mig.spec.TpuType
}

func (mig *GkeMig) IsGpuAcceleratorSlice() bool {
	if mig.spec == nil {
		return false
	}

	machineFamily, err := mig.MachineConfigProvider().GetMachineFamilyFromMachineName(mig.spec.MachineType)
	if err != nil {
		klog.Errorf("could not get machine family for %s: %v", mig.spec.MachineType, err)
		return false
	}

	if machineFamily.IsGpuAcceleratorSliceSupported() && mig.spec.PlacementGroup.UsesPlacement() {
		return true
	}

	return false
}

// ResizeAtomically returns whether the mig should be resized atomically.
func (mig *GkeMig) ResizeAtomically() bool {
	if mig.spec == nil {
		return false
	}
	return mig.IsMultiHostTpuMig() || mig.IsGpuAcceleratorSlice() || mig.UsesBulkProvisioning()
}

func (mig *GkeMig) UsesBulkProvisioning() bool {
	if mig.spec == nil {
		return false
	}
	return mig.spec.UsesBulkProvisioning(mig.MachineConfigProvider())
}

// GetHugepageSize1g returns the total bytes specified by 1-gigabyte-sized huge pages.
func (mig *GkeMig) GetHugepageSize1gBytes() int64 {
	if mig.spec == nil || mig.spec.LinuxNodeConfig == nil || mig.spec.LinuxNodeConfig.Hugepages == nil {
		return 0
	}
	return mig.spec.LinuxNodeConfig.Hugepages.HugepageSize1g * units.GiB
}

// GetHugepageSize2m returns the total bytes specified by 2-megabyte-sized huge pages.
func (mig *GkeMig) GetHugepageSize2mBytes() int64 {
	if mig.spec == nil || mig.spec.LinuxNodeConfig == nil || mig.spec.LinuxNodeConfig.Hugepages == nil {
		return 0
	}
	return 2 * mig.spec.LinuxNodeConfig.Hugepages.HugepageSize2m * units.MiB
}

// GetInjectedMig returns the GkeMig as defined by CA in the injection stage.
func (mig *GkeMig) GetInjectedMig() *GkeMig {
	return mig.gkeManager.GetInjectedMig(mig)
}

// ShallowCopyInZone returns a shallow copy of the mig in the provided zone
func (mig *GkeMig) ShallowCopyInZone(z string) *GkeMig {
	copy := *mig
	copy.gceRef.Zone = z
	copy.id = gce.GenerateMigUrl(copy.domainUrl, copy.gceRef)
	return &copy
}

// IsReservationCompatible checks if given reservation is compatible with the created MIG.
func (mig *GkeMig) IsReservationCompatible(rsv *gce_api.Reservation) bool {
	migReservationAffinity := mig.Spec().ReservationAffinity
	migReservationAffinityType := gkeclient.ReservationAffinityAny
	migReservationAffinityName := ""

	if migReservationAffinity != nil {
		migReservationAffinityType = migReservationAffinity.ConsumeReservationType
		if len(migReservationAffinity.Values) > 0 {
			migReservationAffinityName = migReservationAffinity.Values[0]
		}
	}
	// Check if specific reservation is not required.
	if !rsv.SpecificReservationRequired &&
		migReservationAffinityType == gkeclient.ReservationAffinityAny ||
		migReservationAffinityType == gkeclient.ReservationAffinityAnyThenFail {
		return true
	}
	// Check if specific reservation is required.
	if rsv.SpecificReservationRequired && migReservationAffinityType == gkeclient.ReservationAffinitySpecific && rsv.Name == reservationName(migReservationAffinityName) {
		return true
	}
	return false
}

// SetLocationPolicy sets location policy in mig and nodepoolspec
func (mig *GkeMig) SetLocationPolicy(locationPolicy string) {
	locationPolicyEnum := toLocationPolicyEnum(locationPolicy)
	mig.locationPolicy = locationPolicyEnum
	mig.spec.LocationPolicy = string(locationPolicyEnum)
}

func (mig *GkeMig) SetTaint(taint apiv1.Taint) {
	mig.spec.Taints = append(mig.spec.Taints, taint)
}

// Accessing machine config from GkeMig is needed for evaluating some CCC rules.
func (mig *GkeMig) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return mig.gkeManager.MachineConfigProvider()
}

// GetMig returns the *GkeMig object
func (mig *GkeMig) GetMig() *GkeMig {
	return mig
}

type Config struct {
	ProjectId string
	Location  string
	InternalClient
	GceCache                            *gce.GceCache
	Cache                               *GkeCache
	AutoscalingOptionsTracker           *optstracking.OptionsTracker
	SurgeTracker                        *SurgeUpgradeResourceTracker
	TemplateCache                       *nodetemplate.Cache
	GkeDebuggingSnapshotter             *gkedebuggingsnapshot.GkeDebuggingSnapshotter
	Listers                             kubernetes.ListerRegistry
	NetworkMatcher                      networking.Matcher
	GkeReserved                         *GkeReserved
	SystemLabelPatterns                 []string
	ClusterLocationsObserver            ClusterLocationsObserver
	AutoscalingOptsProvider             AutoscalingOptionsProvider
	AutoprovisioningEligibility         AutoprovisioningEligibility
	EkSpotEnabledCache                  ekvm_provider_interfaces.ExperimentFlagCache[bool]
	ResizableVmAutoprovisioningProvider ekvm_provider_interfaces.ResizableVmAutoprovisioningProvider
	LookaheadBufferStrategyProvider     lookaheadbuffer.StrategyProvider
	ProviderConfigObserver              multitenancy.ProviderConfigObserver
	ProvisioningCache                   *provreqcache.QueuedProvisioningCache
	DraResourcePredictor                *dynamicresources.ResourcePredictor
	ReservationsPuller                  *gceclient.ReservationsPuller
	ResizableVmCustomThresholdsProvider ekvms_customthresholds.CustomThresholdsProvider
}

// InternalClient contains all the internal client dependencies required by the GkeManager.
type InternalClient struct {
	GCE                        gceclient.AutoscalingInternalGceClient
	GKE                        gkeclient.AutoscalingGkeClient
	ResizableVmClient          resizablevms.Client
	ConsumableReservations     consumablereservations.Client
	RecommendLocations         gceclient.RecommendLocationsClient
	AtomicResizeRequest        resizerequestclient.ResizeRequestClient
	FlexResizeRequest          resizerequestclient.ResizeRequestClient
	FlexAdvisor                flexadvisorapi.AdviceProvider
	ProvisioningRequestManager prmanager.ProvisioningRequestManager
	MachineConfigProvider      *machinetypes.MachineConfigProvider
}

var registerMetricsOnce sync.Once

// BuildGKE builds a new GKE cloud provider, manager etc.
func BuildGKE(ctx context.Context, config Config) (*gkeCloudProviderImpl, error) {
	opts := config.AutoscalingOptionsTracker.Options()
	rl := ca_context.NewResourceLimiterFromAutoscalingOptions(opts.AutoscalingOptions)
	do := cloudprovider.NodeGroupDiscoveryOptions{
		NodeGroupSpecs:              opts.NodeGroups,
		NodeGroupAutoDiscoverySpecs: opts.NodeGroupAutoDiscovery,
	}

	if do.DiscoverySpecified() {
		return nil, errors.New("GKE gets nodegroup specification via API, command line specs are not allowed")
	}

	gceConnectionConfig := GceConnectionConfig{
		UserAgent:                      opts.UserAgent,
		Endpoint:                       opts.GceEndpoint,
		ConcurrentRefreshes:            opts.GCEOptions.ConcurrentRefreshes,
		MigInstancesMinRefreshWaitTime: opts.GCEOptions.MigInstancesMinRefreshWaitTime,
	}

	allowlistedSystemLabelsMatcher, err := gkelabels.NewMatcher(config.SystemLabelPatterns)
	if err != nil {
		klog.Fatalf("Failed to create system labels matcher: %v", err)
	}

	managerOptions := GkeManagerOptions{
		Regional:                          opts.Regional,
		AutopilotEnabled:                  opts.AutopilotEnabled,
		napDefaultMachineTypeFamily:       opts.NapDefaultMachineTypeFamily,
		AutopilotHigherMaxPodsPerNode:     opts.AutopilotHigherMaxPodsPerNode,
		MultiNetworkSupportEnabled:        opts.MultiNetworkSupportEnabled,
		bootDiskConfigEnabled:             opts.BootDiskSelectorEnabled,
		bulkGceMigInstancesListingEnabled: opts.GCEOptions.BulkMigInstancesListingEnabled,
		allowlistedSystemLabelsMatcher:    allowlistedSystemLabelsMatcher,
		cpMaxParallelOps:                  opts.CpMaxParallelOps,
		cpMaxQueuedOps:                    opts.CpMaxQueuedOps,
		asyncNodePoolsEnabled:             opts.AsyncNodeGroupsEnabled,
		MultitenancyEnabled:               opts.MultitenancyEnabled,
		enableUserAnyZoneSelection:        opts.EnableUserAnyZoneSelection,
		MachineSerenityLabelsEnabled:      opts.MachineSerenityLabelsEnabled,
	}

	manager, err := CreateGkeManager(
		ctx,
		config.ProjectId,
		config.Location,
		config.InternalClient,
		config.GceCache,
		config.Cache,
		gceConnectionConfig,
		managerOptions,
		opts.ClusterName,
		config.SurgeTracker,
		config.GkeReserved,
		config.NetworkMatcher,
		config.ClusterLocationsObserver,
		config.AutoscalingOptionsTracker,
		opts.GCEOptions.LocalSSDDiskSizeProvider,
		config.AutoscalingOptsProvider,
		config.AutoprovisioningEligibility,
		config.EkSpotEnabledCache,
		config.ResizableVmAutoprovisioningProvider,
		config.LookaheadBufferStrategyProvider,
		config.DraResourcePredictor,
		config.ReservationsPuller,
		config.ResizableVmCustomThresholdsProvider,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create GKE Manager: %v", err)
	}
	gkePriceInfo := NewGkePriceInfo(config.InternalClient.MachineConfigProvider, opts.AutopilotEnabled)
	priceModel := gce.NewGcePriceModel(gkePriceInfo, opts.GCEOptions.LocalSSDDiskSizeProvider)
	provider, err := BuildGkeCloudProvider(manager, priceModel, rl, opts.AutopilotEnabled, opts.Location, config.GkeDebuggingSnapshotter, opts.CompactPlacementEnabled, opts.ResolveInstanceRefUsingNodePoolLabel, config.Listers.AllPodLister(), opts.GceEndpoint, config.AutoscalingOptionsTracker.ExperimentsManager(), config.InternalClient.MachineConfigProvider, gkePriceInfo, opts.NapMaxNodes)
	if err != nil {
		return nil, fmt.Errorf("failed to create GKE cloud provider: %v", err)
	}
	// Register GKE & GCE API usage metrics.
	registerMetricsOnce.Do(func() {
		gke_metrics.RegisterMetrics()
		gce.RegisterMetrics()
	})
	return provider, nil
}
