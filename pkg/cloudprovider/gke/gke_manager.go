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
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	caerrors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/autoscaler/cluster-autoscaler/utils/klogx"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/interfaces"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/dynamicresources"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizablevms"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/logging"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/networking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"

	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	gkelocalssdsize "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/localssdsize"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/sandbox"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	gkeutil "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/validators"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	ekvms_customthresholds "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/backoff/customthresholds"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer"
	ekvm_provider_interfaces "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/providers/interfaces"
	ekvmsize "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	ekvmtypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
	gke_metrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	internalmetrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/common"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/manager"
	prpods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/reconciler"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"

	gce_api "google.golang.org/api/compute/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
)

const (
	// ClusterRefreshInterval specifies how often the GKE Cluster state is refreshed.
	ClusterRefreshInterval      = 1 * time.Minute
	machinesRefreshInterval     = 1 * time.Hour
	migCreationWaitTimeout      = 120 * time.Second
	migCreationCheckInterval    = 1 * time.Second
	injectedMigCacheTTL         = 1 * time.Hour
	nodeAutoprovisioningPrefix  = "nap"
	defaultImageType            = "cos_containerd"
	defaultOsDistribution       = gce.OperatingSystemDistributionCOS
	autopilotMaxPodsPerNode     = int64(32)
	maxFlexStartRRBatchSize     = 50
	createInstancesRequestLimit = 1000
)

// GkeMetrics is an interface for recording GKE-specific metrics.
type GkeMetrics interface {
	UpdateCSNEnabled(enabled bool)
	UpdateNapEnabled(enabled bool)
}

// AutoscalingOptionsProvider fetches autoscaling option overrides for a given node group
type AutoscalingOptionsProvider interface {
	ScaleDownUnneededTime(cloudprovider.NodeGroup) (time.Duration, bool, error)
	ScaleDownUtilizationThreshold(cloudprovider.NodeGroup) (float64, bool, error)
	ScaleDownGpuUtilizationThreshold(cloudprovider.NodeGroup) (float64, bool, error)
}

// AutoprovisioningEligibility checks if resource is eligible for autoprovisioning related functionalities.
type AutoprovisioningEligibility interface {
	// SetClusterAutoprovisioningEnabled sets the value of cluster NAP flag. Returns true if the flag has
	// changed, false otherwise (in case of a no-op).
	SetClusterAutoprovisioningEnabled(bool) bool
	// IsNodeAutoprovisioningEnabled returns true if cluster NAP or CCC NAP-less autoprovisioning is enabled.
	IsNodeAutoprovisioningEnabled() bool
	// AreClusterLimitsEnabled returns true if NAP cluster limits are enabled.
	AreClusterLimitsEnabled() bool
	// UseAutoprovisioningFeaturesForPodRequirements checks if pod should trigger autoprovisioning features.
	UseAutoprovisioningFeaturesForPodRequirements(req *podrequirements.Requirements) bool
	// UseAutoprovisioningFeaturesForNodeGroup check if node group should trigger autoprovisioning features.
	UseAutoprovisioningFeaturesForNodeGroup(nodeGroup cloudprovider.NodeGroup) bool
}

// GkeManager handles gce communication and data caching.
type GkeManager interface {
	validators.MachineConfigValidator
	api.AdviceProvider
	// IsNodeAutoprovisioningEnabled returns true if NAP is enabled.
	IsNodeAutoprovisioningEnabled() bool
	// UseAutoprovisioningFeaturesForPodRequirements checks if pod should trigger autoprovisioning features.
	UseAutoprovisioningFeaturesForPodRequirements(req *podrequirements.Requirements) bool
	// UseAutoprovisioningFeaturesForNodeGroup check if node group should trigger autoprovisioning features.
	UseAutoprovisioningFeaturesForNodeGroup(nodeGroup cloudprovider.NodeGroup) bool
	// GetAutoprovisioningDefaultFamily returns the default family used for NAP.
	GetAutoprovisioningDefaultFamily() machinetypes.MachineFamily
	// GetMigSize gets MIG size.
	GetMigSize(mig gce.Mig) (int64, error)
	// SetMigSize sets MIG size.
	SetMigSize(mig gce.Mig, size int64) error
	// IsMigStable returns whether the MIG is stable.
	IsMigStable(mig gce.Mig) (bool, error)
	// DeleteInstances deletes the given instances. All instances must be controlled by the same MIG.
	DeleteInstances(instances []gce.GceRef) error
	// GetMigForInstance returns MigConfig of the given Instance
	GetMigForInstance(instance gce.GceRef) (gce.Mig, error)
	// GetMigNodes returns mig nodes.
	GetMigNodes(mig gce.Mig) ([]gce.GceInstance, error)
	// Refresh updates both GCE and GKE resources.
	Refresh() error
	// RefreshLocalSSDSizes updates local SSD sizes.
	RefreshLocalSSDSizes()
	// GetResourceLimiter returns resource limiter.
	GetResourceLimiter(n NodeGroupFromNode) (*cloudprovider.ResourceLimiter, error)
	// Cleanup cleans up open resources before the cloud provider is destroyed, i.e. go routines etc.
	Cleanup() error
	// GetGkeMigs returns a list of registered MIGs.
	GetGkeMigs() []*GkeMig
	// GetGkeMigsBlockedByServerError returns a list of irretrievable migs blocked by server error (5xx).
	GetGkeMigsBlockedByServerError() []*GkeMig
	// GetGkeMigsBlockedByNotFoundError returns a list of irretrievable migs blocked by not found error (404).
	GetGkeMigsBlockedByNotFoundError() []*GkeMig
	// GetAllNodePoolNames returns all node pool names
	GetAllNodePoolNames() sets.Set[string]
	// CreateNodePool creates a MIG based on blueprint and returns the newly created MIG.
	CreateNodePool(mig *GkeMig) (MigCreateNodePoolResult, error)
	// CreateNodePoolAsync creates node pool asynchronously. Immediately reruns upcoming node groups.
	CreateNodePoolAsync(mig *GkeMig, updater interfaces.AsyncNodeGroupUpdater, initializer interfaces.AsyncNodeGroupInitializer) (MigCreateNodePoolResult, error)
	// IsUpcoming checks if node group is being created asynchronously.
	IsUpcoming(mig *GkeMig) bool
	// DeleteNodePool deletes a MIG from cloud provider.
	DeleteNodePool(toBeRemoved *GkeMig) error
	// UpdateNodePoolLabels updates node pool labels.
	UpdateNodePoolLabels(name string, labels map[string]string) error
	// DeleteNodePoolAsync deletes node pool asynchronously. Returns immediately.
	DeleteNodePoolAsync(mig *GkeMig, finalizer interfaces.AsyncNodeGroupFinalizer) error
	// GetMigsTargetSize gets sum of MIGs target sizes.
	GetMigsTargetSize(migs []gce.GceRef) (int64, error)
	// GetLocation returns cluster's location.
	GetLocation() string
	// GetProjectId returns id of GCE project to which the cluster belongs.
	GetProjectId() string
	// GetReleaseChannel returns the release channel.
	GetReleaseChannel() string
	// GetClusterName returns the name of the GKE cluster.
	GetClusterName() string
	// GetClusterVersion returns the version of the GKE cluster.
	GetClusterVersion() string
	// GetClusterNetwork returns the GCE Network resource of the cluster's VPC
	GetClusterNetwork() (*gce_api.Network, error)
	// RecommendLocations returns recommendation made by recommendLocations API.
	RecommendLocations(ctx context.Context, region string, request gceclient.RecommendLocationsRequest) (*gceclient.RecommendLocationsResponse, error)
	// GetFutureReservationsInProject returns future reservations in a given project.
	GetFutureReservationsInProject(projectID string) ([]*gceclient.GceFutureReservation, error)
	// GetReservationBlocksInReservation returns the reservation blocks for a particular reservation, in specfied project and zone.
	GetReservationBlocksInReservation(reservationRef gceclient.ReservationRef) ([]*gceclient.GceReservationBlock, error)
	// GetReservationSubblocksInReservationBlock returns the reservation subblocks for a particular reservation block, in specfied reservation, project, and zone.
	GetReservationSubBlocksInReservationBlock(reservationRef gceclient.ReservationRef) ([]*gceclient.GceReservationSubBlock, error)
	// GetResourcePolicies returns the resource policies in the provided project and region.
	GetResourcePolicies(projectId, region string) ([]*gceclient.GceResourcePolicy, error)
	// GetZonesInRegion returns all zones within a given region.
	GetZonesInRegion(region string) ([]string, error)
	// GetStandardZones returns all Standard zones within a given region.
	GetStandardZonesInRegion(region string) ([]string, error)
	// GetAIZonesInRegion returns all AI zones within a given region.
	GetAIZonesInRegion(region string) ([]string, error)
	// GetMigInstanceTemplate returns an instance template for MIG.
	GetMigInstanceTemplate(mig *GkeMig) (*gce_api.InstanceTemplate, error)
	// GetMigKubeEnv returns the kube-env for MIG.
	GetMigKubeEnv(mig *GkeMig) (gce.KubeEnv, error)
	// GetMigTemplateNodeInfo returns a template NodeInfo for MIG.
	GetMigTemplateNodeInfo(mig *GkeMig) (*framework.NodeInfo, error)
	// GetExistingNodeGroupLocations returns a list of locations for created node groups
	GetExistingNodeGroupLocations() []string
	// GetAutoprovisioningLocations returns a list of locations where NAP can create new nodepools.
	GetAutoprovisioningLocations() []string
	// Client returns the authenticated GKE http client.
	Client() *http.Client
	// GetMachineType gets gce.MachineType for a given type name and location.
	// Note that this is only meant to be used in NAP context, for any scenario involving
	// a real MIG, you should retrieve this information via MigInfoProvider to get
	// proper error handling.
	GetMachineType(machineType string, zone string) (gce.MachineType, error)
	// GetNumberOfSurgeNodesInMig get the number of surge nodes in a mig.
	GetNumberOfSurgeNodesInMig(mig *GkeMig) int
	// AreConfidentialNodesEnabled checks if ConfidentialNodes are enabled in cluster.
	AreConfidentialNodesEnabled() bool
	// GetConfidentialInstanceType returns the confidential instance type of the cluster.
	GetConfidentialInstanceType() string
	// GetDefaultNodePoolDiskType returns a default node pool disk type
	GetDefaultNodePoolDiskType() string
	// GetDefaultNodePoolMinCpuPlatform returns a default node pool min cpu platform
	GetDefaultNodePoolMinCpuPlatform() string
	// GetDefaultNodePoolDiskSizeGB returns a default node pool disk size GiB
	GetDefaultNodePoolDiskSizeGB() int64
	// GetImageTypeForNap returns the Node Autoprovisioning Image Type for a mig.
	GetImageTypeForNap(mig *GkeMig) string
	// GetOsDistributionForNap returns the Node Autoprovisioning Operating System distribution for a mig.
	GetOsDistributionForNap(mig *GkeMig) gce.OperatingSystemDistribution
	// GetNewNodePoolDaemonSetConditions returns the flags for gke daemon sets
	GetNewNodePoolDaemonSetConditions() *DaemonSetConditions
	// CreateInstances creates delta new instances in a mig.
	CreateInstances(mig gce.Mig, delta int64) error
	// CreateFlexResizeRequests creates delta new single VM Resize Requests in a mig
	// TODO(b/381046789): add async_gke_manager.go CreateFlexResizeRequests implementation for NAP support
	// TODO(b/381046606): remove FSNQ CreateFlexResizeRequests after migration to CreateInstances API
	CreateFlexResizeRequests(mig gce.Mig, delta int64) error
	// CreateQueuedInstances queues creation of VMs
	CreateQueuedInstances(pr prpods.ProvReqID, mig *GkeMig, delta int64, shouldUpdateProvReqDetails manager.ShouldUpdateProvReqDetails) error
	// CreateResizeRequest creates a RR with the Resize Request API using atomic/flex client.
	CreateResizeRequest(mig gce.Mig, delta int64) error
	// AdvanceResizeRequestCleanUp (re)triggers a new cancel/delete operation based on the Resize Request state or checks the status of the existing operation.
	AdvanceResizeRequestCleanUp(resizeRequest resizerequestclient.ResizeRequestStatus) error
	// ReportState returns the report state of the particular Resize Request
	ReportState(resizeRequest resizerequestclient.ResizeRequestStatus) resizerequestclient.ResizeRequestReportState
	// SetReportState sets the report state of the particular Resize Request
	SetReportState(resizeRequest resizerequestclient.ResizeRequestStatus, state resizerequestclient.ResizeRequestReportState)
	// ResetFailedResizeRequestsCreation returns map of failed Resize Creation errors and number of not created Resize Requests and clears the map
	ResetFailedResizeRequestsCreation(migRef gce.GceRef) map[error]int
	// IsResizeRequestErrorHandlingEnabled returns if the resize request errors should be handled.
	IsResizeRequestErrorHandlingEnabled() bool
	// SetScaleUpTimeProvider sets the ScaleUpTimeProvider
	SetScaleUpTimeProvider(provider ScaleUpTimeProvider)
	// GetClusterCreateTime gets the cluster create time
	GetClusterCreateTime() time.Time
	// ClusterStarted lets you know if the cluster started
	ClusterStarted() (bool, error)
	// IsClusterUsingPSCInfrastructure checks if cluster is using PSC infrastructure. If so, cluster support public and private nodes.
	IsClusterUsingPSCInfrastructure() bool
	// GetDefaultEnablePrivateNodes return default value for enablePrivateNodes.
	GetDefaultEnablePrivateNodes() bool
	// IsDataplaneV2Enabled returns if dataplane is enabled in cluster.
	IsDataplaneV2Enabled() bool
	// RegisterInitializationFunc registers an initialization func, which would be called once
	// after the first successful Refresh of the cluster state.
	RegisterInitializationFunc(f InitializationFunc)
	// ResizeRequests returns all Resize Requests for a given node group.
	ResizeRequests(mig *GkeMig) ([]resizerequestclient.ResizeRequestStatus, error)
	// ResizeVm resizes a given VM Instance to a given size.
	ResizeVm(context.Context, gce.GceRef, ekvmsize.VmSize) error
	// GetCurrentResizableVmState fetches the current size of given resizable VM.
	GetCurrentResizableVmState(provider *machinetypes.MachineConfigProvider, instance gce.GceRef) (ekvmtypes.ResizableVmState, error)
	// BulkFetchCurrentResizableVmStates fetches current sizes of resizable VMs from GCE.
	BulkFetchCurrentResizableVmStates(provider *machinetypes.MachineConfigProvider) (map[gce.GceRef]ekvmtypes.ResizableVmState, error)
	// QueuedProvisioningNodeHasScaleDownImmunity returns true if the provided QueuedProvisioning node still shouldn't get scaled down,
	// i.e. additionalImmunity hasn't ran out yet.
	QueuedProvisioningNodeHasScaleDownImmunity(node *apiv1.Node, migSpec *reconciler.QueuedProvisioningMigSpec, now time.Time) bool
	// InstanceByRef returns GceInstance from cache. It returns nil when the corresponding instance is not cached.
	InstanceByRef(ref gce.GceRef) *gce.GceInstance
	// GetListManagedInstancesResults returns the pagination behavior of the listManagedInstances API method for a given MIG ref
	GetListManagedInstancesResults(migRef gce.GceRef) (string, error)
	// ResumeInstances resumes instances
	ResumeInstances(migRef gce.GceRef, instances []gce.GceRef) error
	// SuspendInstances suspends instances
	SuspendInstances(migRef gce.GceRef, instances []gce.GceRef, forceSuspend bool) error
	// IsDefaultCCCEnabled returns if default CCC is enabled in cluster.
	IsDefaultCCCEnabled() bool

	CalculatePhysicalEphemeralStorageGiB(mig *GkeMig, allocatableBytes int64) int64

	// ScaleDownUnreadyTimeOverride fetches an override for scaledown unready time
	ScaleDownUnreadyTimeOverride(mig *GkeMig) (time.Duration, bool)
	// ScaleDownUnneededTimeOverride fetches an override for scaledown unneeded time
	ScaleDownUnneededTimeOverride(cloudprovider.NodeGroup) (time.Duration, bool, error)
	// ScaleDownUtilizationThresholdOverride fetches an override for scaledown utilization threshold
	ScaleDownUtilizationThresholdOverride(cloudprovider.NodeGroup) (float64, bool, error)
	// ScaleDownGpuUtilizationThresholdOverride fetches an override for scaledown gpu utilization threshold
	ScaleDownGpuUtilizationThresholdOverride(cloudprovider.NodeGroup) (float64, bool, error)
	// ValidateLocationForDiskType validate if the disk type is available in the given location.
	ValidateLocationForDiskType(location string, requestedDiskType string) (ok bool, reason string, err error)
	// ResizingEnabled checks if resizing is enabled for the given machine family.
	ResizingEnabled(machineFamily string) bool
	// IsResizableVmEnabledInAutopilot returns true if resizable VM for the given family should be used for autoprovisioning in Autopilot.
	IsResizableVmEnabledInAutopilot(machineFamily string) bool
	// IsResizableVmWithinPodFamilyEnabled returns true if resizable VMs for the given family can be used within a pod family.
	IsResizableVmWithinPodFamilyEnabled(machineFamily string) bool
	// IsEkSpotEnabled returns true if EKs can be used as spot VMs
	IsEkSpotEnabled() bool
	// GetNodesScaleDownAllowedFromCache retrieves the scale-down information for nodes from the cache.
	GetNodesScaleDownAllowedFromCache([]string) map[string]bool
	// UpdateNodesScaleDownAllowedCache updates the scale-down information for nodes in the cache.
	// The cache is updated once per CA loop.
	UpdateNodesScaleDownAllowedCache(map[string]bool)
	// InvalidateNodesScaleDownAllowedCache invalidates the cache storing information about whether nodes are allowed to be scaled down.
	InvalidateNodesScaleDownAllowedCache()
	// GetInjectedMig returns an injected mig for this existing mig.
	GetInjectedMig(mig *GkeMig) *GkeMig
	// SetInjectedMig sets an injected mig for existing mig.
	SetInjectedMig(real, injected *GkeMig)
	// IsEkEdpEnabled returns true if Edp on EKs with affinity X is enabled
	IsEkEdpEnabled() bool
	// IsArmMachineFallbacksEnabled returns true if machine fallbacks to N4A and C4A are enabled.
	IsArmMachineFallbacksEnabled() bool
	// CapacityCheckWaitTimeSeconds returns capacityCheckWaitTimeSeconds for the given mig based on default experiment values and custom capacityCheckWaitTimeSeconds override.
	CapacityCheckWaitTimeSeconds(mig *GkeMig) (time.Duration, error)
	// EvaluateCapacityCheckWaitTimeSeconds returns capacityCheckWaitTimeSeconds for the given mig based on default experiment values and custom capacityCheckWaitTimeSeconds override.
	EvaluateCapacityCheckWaitTimeSeconds(mig *GkeMig) (time.Duration, error)
	GetMaxNodeProvisioningTimeOverride(mig *GkeMig) (time.Duration, bool)
	// GetDeploymentType returns the MIG's deployment type based on the reservation type
	GetDeploymentType(gceRef gce.GceRef, spec *gkeclient.NodePoolSpec) DeploymentTypeEnum
	// ExistingMigsInNodePool returns a list of registered node groups (existing MIGs) that belong to a given node pool.
	ExistingMigsInNodePool(nodePoolName string) []*GkeMig
	// GetBasenameForMig returns basename for this existing MIG
	GetBasenameForMig(mig *GkeMig) (string, error)
	// NodePoolSpecForNode returns the node pool spec for a particular node
	NodePoolSpecForNode(node *apiv1.Node) (*gkeclient.NodePoolSpec, error)
	// TrimLocationsForMachineConfig cross-checks each location for mig specification, and rejects locations where mig cannot be created.
	TrimLocationsForMachineConfig(locations []string, machineType string, acceleratorConfig *gke_api_beta.AcceleratorConfig, minCpuPlatform string, diskType string) []string
	// MachineConfigProvider return the MachineConfigProvider.
	MachineConfigProvider() *machinetypes.MachineConfigProvider
	// ExperimentsManager returns the experiments.Manager.
	ExperimentsManager() experiments.Manager
}

type ScaleUpTimeProvider interface {
	NodeGroupScaleUpTime(nodeGroup cloudprovider.NodeGroup) (time.Time, error)
}

// MigCreateNodePoolResult represent a set of MIGs created with call to GkeManager.CreateNodePool.
type MigCreateNodePoolResult struct {
	MainCreatedMig   *GkeMig
	ExtraCreatedMigs []*GkeMig
}

// AllCreatedMigs returns all created migs, main mig with extra migs.
func (r MigCreateNodePoolResult) AllCreatedMigs() []*GkeMig {
	var result []*GkeMig
	if r.MainCreatedMig != nil && !reflect.ValueOf(r.MainCreatedMig).IsNil() {
		result = append(result, r.MainCreatedMig)
	}
	result = append(result, r.ExtraCreatedMigs...)
	return result
}

// gkeConfigurationCache is used for storing cached cluster configuration.
type gkeConfigurationCache struct {
	sync.Mutex
	autoprovisioningLocations []string
}

// DaemonSetConditions gathers flags of daemon sets enabled on the cluster
type DaemonSetConditions struct {
	NodeLocalDNSEnabled          bool
	MetadataServerEnabled        bool
	HighThroughputLoggingEnabled bool
	NetdEnabled                  bool
	IpMasqAgentEnabled           bool
}

// InitializationFunc type of function to be run ran once after the first successful refresh of cluster state.
type InitializationFunc func() error

func (cache *gkeConfigurationCache) setAutoprovisioningLocations(locations []string) {
	cache.Lock()
	defer cache.Unlock()

	cache.autoprovisioningLocations = make([]string, len(locations))
	copy(cache.autoprovisioningLocations, locations)
}

func (cache *gkeConfigurationCache) getAutoprovisioningLocations() []string {
	cache.Lock()
	defer cache.Unlock()

	locations := make([]string, len(cache.autoprovisioningLocations))
	copy(locations, cache.autoprovisioningLocations)
	return locations
}

type ClusterLocationsObserver interface {
	SetLocations(locations []string)
}

// gkeManagerImpl handles gce communication and data caching.
type gkeManagerImpl struct {
	cache                    *GkeCache
	gkeConfigurationCache    gkeConfigurationCache
	lastRefresh              time.Time
	machinesCacheLastRefresh time.Time
	validators.MachineConfigValidator
	surgeUpgradeResourceTracker     *SurgeUpgradeResourceTracker
	confidentialNodesEnabled        bool
	confidentialInstanceType        string
	isClusterUsingPSCInfrastructure bool
	defaultEnablePrivateNodes       bool
	NewNodePoolDaemonSetConditions  *DaemonSetConditions
	defaultMaxPodsPerNode           atomic.Int64
	dataplaneV2Enabled              bool
	isDefaultCCCEnabled             atomic.Bool

	gkeService                 gkeclient.AutoscalingGkeClient
	gceService                 gceclient.AutoscalingInternalGceClient
	resizableVmService         resizablevms.Client
	recommendLocationsService  gceclient.RecommendLocationsClient
	atomicResizeRequestService resizerequestclient.ResizeRequestClient
	// TODO(b/381046606): Temporary service until migration to CreateInstance API
	flexResizeRequestService   resizerequestclient.ResizeRequestClient
	flexAdvisorService         api.AdviceProvider
	provisioningRequestManager manager.ProvisioningRequestManager
	matcher                    networking.Matcher
	reservationsPuller         *gceclient.ReservationsPuller

	migLister                     *gkeMigLister
	migInfoProvider               gce.MigInfoProvider
	availableCpuPlatformsProvider AvailableCpuPlatformsProvider
	availableDiskTypesProvider    AvailableDiskTypesProvider

	clusterName                      string
	location                         string
	projectId                        string
	releaseChannel                   string
	clusterVersion                   string
	emulatedClusterVersion           string // Only contains the major and minor version, example value: "1.33"
	domainUrl                        string
	autoprovisioningNodePoolDefaults *gke_api_beta.AutoprovisioningNodePoolDefaults
	nodePoolDefaults                 *gke_api_beta.NodePoolDefaults
	napDefaultMachineTypeFamily      machinetypes.MachineFamily
	templates                        *GkeTemplateBuilder
	interrupt                        chan struct{}
	client                           *http.Client
	clusterCreateTime                time.Time
	clusterStarted                   bool
	reserved                         *GkeReserved
	network                          *gce_api.Network
	networkPath                      string
	subnetworkPath                   string
	subnetwork                       string

	managerOptions      GkeManagerOptions
	gceConnectionConfig GceConnectionConfig

	initializationFuncs []InitializationFunc
	gkeMetrics          GkeMetrics
	initializationOnce  sync.Once

	allowlistedSystemLabelsMatcher *gkelabels.Matcher
	clusterLocationsObserver       ClusterLocationsObserver

	localSSDDiskSizeProvider *gkelocalssdsize.DynamicLocalSSDDiskSizeProvider

	autoscalingOptsProvider             AutoscalingOptionsProvider
	autoprovisioningEligibility         AutoprovisioningEligibility
	ekSpotEnabledCache                  ekvm_provider_interfaces.ExperimentFlagCache[bool]
	resizableVmAutoprovisioningProvider ekvm_provider_interfaces.ResizableVmAutoprovisioningProvider
	lookaheadBufferStrategyProvider     lookaheadbuffer.StrategyProvider
	optsTracker                         *optstracking.OptionsTracker
	// injectedMig is set for all existing node pools created by this instance of Cluster Autoscaler
	// and represents an injected specification of this MIG (before part of the spec was filled
	// on the GKE API side).
	// injectedMig is intended to use as a read-only copy
	migIdToInjectedNg map[gce.GceRef]*GkeMig
	// migIdToInjectedNgMutex guards migIdToInjectedNg as it's modified from a separate goroutine
	// after asynchronous node group creation in asyncGkeManager.
	migIdToInjectedNgMutex sync.Mutex
	draResourcePredictor   *dynamicresources.ResourcePredictor
	machineConfigProvider  *machinetypes.MachineConfigProvider

	ekEdpEnabledCache                   bool
	resizableVmCustomThresholdsProvider ekvms_customthresholds.CustomThresholdsProvider
}

// GceConnectionConfig is a config for GCE connection.
type GceConnectionConfig struct {
	UserAgent                      string
	Endpoint                       string
	ConcurrentRefreshes            int
	MigInstancesMinRefreshWaitTime time.Duration
}

// GkeManagerOptions contains option flags for GKE Manager.
type GkeManagerOptions struct {
	Regional                          bool
	AutopilotEnabled                  bool
	AutopilotHigherMaxPodsPerNode     bool
	ResizeRequestErrorHandlingEnabled bool
	MultiNetworkSupportEnabled        bool
	MultitenancyEnabled               bool
	bootDiskConfigEnabled             bool
	napDefaultMachineTypeFamily       string
	bulkGceMigInstancesListingEnabled bool
	allowlistedSystemLabelsMatcher    *gkelabels.Matcher
	cpMaxParallelOps                  int
	cpMaxQueuedOps                    int
	asyncNodePoolsEnabled             bool
	enableUserAnyZoneSelection        bool
	MachineSerenityLabelsEnabled      bool
}

// CreateGkeManager constructs GkeManager object.
func CreateGkeManager(
	ctx context.Context,
	projectId string,
	location string,
	client InternalClient,
	gceCache *gce.GceCache,
	cache *GkeCache,
	gceConnectionConfig GceConnectionConfig,
	managerOptions GkeManagerOptions,
	clusterName string,
	tracker *SurgeUpgradeResourceTracker,
	gkeReserved *GkeReserved,
	matcher networking.Matcher,
	clusterLocationsObserver ClusterLocationsObserver,
	optsTracker *optstracking.OptionsTracker,
	localSSDDiskSizeProvider localssdsize.LocalSSDSizeProvider,
	autoscalingOptsProvider AutoscalingOptionsProvider,
	autoprovisioningEligibility AutoprovisioningEligibility,
	ekSpotEnabledCache ekvm_provider_interfaces.ExperimentFlagCache[bool],
	resizableVmAutoprovisioningProvider ekvm_provider_interfaces.ResizableVmAutoprovisioningProvider,
	lookaheadBufferStrategyProvider lookaheadbuffer.StrategyProvider,
	draResourcePredictor *dynamicresources.ResourcePredictor,
	reservationsPuller *gceclient.ReservationsPuller,
	resizableVmCustomThresholdsProvider ekvms_customthresholds.CustomThresholdsProvider,
) (GkeManager, error) {
	// Create cache and mig lister
	migLister := NewGkeMigLister(cache, blockListRefreshIntervalTime, markedIrretrievableRefreshIntervalTime, maxIrretrievableErrsBeforeBlocked)

	domainUrl := strings.TrimSuffix(gceConnectionConfig.Endpoint, "/")
	gceService := client.GCE
	napDefaultMachineTypeFamily, err := client.MachineConfigProvider.ToMachineFamily(managerOptions.napDefaultMachineTypeFamily)
	if err != nil {
		return nil, fmt.Errorf("error finding machine family %s", managerOptions.napDefaultMachineTypeFamily)
	}

	var dynamicLocalSSDDiskSizeProvider *gkelocalssdsize.DynamicLocalSSDDiskSizeProvider
	var ok bool
	if dynamicLocalSSDDiskSizeProvider, ok = localSSDDiskSizeProvider.(*gkelocalssdsize.DynamicLocalSSDDiskSizeProvider); !ok {
		return nil, fmt.Errorf("expected DynamicLocalSSDSizeProvider, got %v", reflect.TypeOf(localSSDDiskSizeProvider))
	}

	manager := &gkeManagerImpl{
		cache:                               cache,
		gceService:                          gceService,
		gkeService:                          client.GKE,
		resizableVmService:                  client.ResizableVmClient,
		recommendLocationsService:           client.RecommendLocations,
		flexAdvisorService:                  client.FlexAdvisor,
		atomicResizeRequestService:          client.AtomicResizeRequest,
		flexResizeRequestService:            client.FlexResizeRequest,
		provisioningRequestManager:          client.ProvisioningRequestManager,
		migLister:                           migLister,
		migInfoProvider:                     gce.NewCachingMigInfoProvider(gceCache, migLister, gceService, projectId, gceConnectionConfig.ConcurrentRefreshes, gceConnectionConfig.MigInstancesMinRefreshWaitTime, managerOptions.bulkGceMigInstancesListingEnabled, managerOptions.MultitenancyEnabled),
		availableCpuPlatformsProvider:       NewCachingAvailableCpuPlatformsProvider(cache, gceService),
		availableDiskTypesProvider:          NewCachingAvailableDiskTypesProvider(cache, gceService),
		MachineConfigValidator:              validators.NewCachedMachineConfigValidator(gceService, gceService, client.MachineConfigProvider),
		location:                            location,
		projectId:                           projectId,
		clusterName:                         clusterName,
		templates:                           &GkeTemplateBuilder{machineSerenityLabelsEnabled: managerOptions.MachineSerenityLabelsEnabled},
		interrupt:                           make(chan struct{}),
		surgeUpgradeResourceTracker:         tracker,
		clusterCreateTime:                   time.Time{},
		reserved:                            gkeReserved,
		gceConnectionConfig:                 gceConnectionConfig,
		managerOptions:                      managerOptions,
		initializationFuncs:                 []InitializationFunc{},
		matcher:                             matcher,
		domainUrl:                           domainUrl,
		allowlistedSystemLabelsMatcher:      managerOptions.allowlistedSystemLabelsMatcher,
		clusterLocationsObserver:            clusterLocationsObserver,
		localSSDDiskSizeProvider:            dynamicLocalSSDDiskSizeProvider,
		autoscalingOptsProvider:             autoscalingOptsProvider,
		ekSpotEnabledCache:                  ekSpotEnabledCache,
		autoprovisioningEligibility:         autoprovisioningEligibility,
		resizableVmAutoprovisioningProvider: resizableVmAutoprovisioningProvider,
		lookaheadBufferStrategyProvider:     lookaheadBufferStrategyProvider,
		optsTracker:                         optsTracker,
		migIdToInjectedNg:                   map[gce.GceRef]*GkeMig{},
		draResourcePredictor:                draResourcePredictor,
		reservationsPuller:                  reservationsPuller,
		machineConfigProvider:               client.MachineConfigProvider,
		napDefaultMachineTypeFamily:         napDefaultMachineTypeFamily,
		resizableVmCustomThresholdsProvider: resizableVmCustomThresholdsProvider,
		gkeMetrics:                          internalmetrics.Metrics,
	}

	if err := manager.forceRefreshResources(); err != nil {
		return nil, err
	}

	go wait.Until(func() {
		if err := manager.migInfoProvider.RegenerateMigInstancesCache(); err != nil {
			klog.Errorf("Error while regenerating Mig cache: %v", err)
		}
	}, time.Hour, ctx.Done())

	if managerOptions.asyncNodePoolsEnabled {
		var extGkeManager extendedGkeManager = manager
		return newAsyncGkeManager(ctx, extGkeManager, cache, managerOptions.cpMaxParallelOps, managerOptions.cpMaxQueuedOps, domainUrl), nil
	}

	return manager, nil
}

// Cleanup closes the channel to signal the go routine to stop that is handling the cache
func (m *gkeManagerImpl) Cleanup() error {
	close(m.interrupt)
	return nil
}

// Client returns the authenticated GKE http client.
func (m *gkeManagerImpl) Client() *http.Client {
	return m.client
}

// GetCluster returns GKE cluster representation
func (m *gkeManagerImpl) GetCluster() (gkeclient.Cluster, error) {
	return m.gkeService.GetCluster()
}

// SetScaleUpTimeProvider sets the ScaleUpTimeProvider
func (m *gkeManagerImpl) SetScaleUpTimeProvider(provider ScaleUpTimeProvider) {
	if m.cache != nil {
		m.cache.scaleUpTimeProvider = provider
	}
}

func (m *gkeManagerImpl) validateNAPEnabled() error {
	if !m.IsNodeAutoprovisioningEnabled() {
		return caerrors.NewAutoscalerError(caerrors.InternalError, "This should be called only when Autoprovisioning is enabled")
	}
	return nil
}

func (m *gkeManagerImpl) refreshNodePools(nodePools []gkeclient.NodePool, allNodePoolNames sets.Set[string]) {
	existingMigs := map[gce.GceRef]struct{}{}
	newOrModifiedMigs := map[*GkeMig]bool{}
	nodePoolSpecs := make(map[string]*gkeclient.NodePoolSpec, len(nodePools))
	// Invalidating the cache getting previously blocked migs
	prevBlockedMigs := m.migLister.InvalidateIrretrievableMigsCacheIfExpired()
	for _, nodePool := range nodePools {
		nodePoolMigs, err := nodePoolMIGs(m, m.domainUrl, nodePool)
		unblockedMigs := make([]*GkeMig, 0, len(nodePoolMigs))

		if err != nil {
			// Errors in one node pool shouldn't affect others (or the other node pools should also get errors), so it's
			// probably better to skip a node pool in case of errors than to short-circuit the whole refresh.
			klog.Errorf("Failed to refresh node pool %q, skipping (it won't be autoscaled until the next refresh): %v", nodePool.Name, err)
			continue
		}
		for _, mig := range nodePoolMigs {
			migRef := mig.GceRef()
			// Check if the MIG was previously blocked
			if _, wasBlocked := prevBlockedMigs[migRef]; wasBlocked {
				klog.V(4).Infof("Revalidating previously blocked mig: %s", migRef.Name)
				m.validateMigTemplateNode(mig)
			}
			// Register MIG if not blocked
			if blocked, reason := m.cache.BlockReason(migRef); !blocked {
				unblockedMigs = append(unblockedMigs, mig)
			} else {
				klog.Warningf("Skipping registering blocked irretrievable mig %s to cache, reason: %v", migRef.Name, reason)
			}
		}
		// AddMigsToNodePool must be called with all MIGs belonging to the same node pool
		// before RegisterMig is called. This is because RegisterMig uses DeepEqual, which
		// includes comparing the nodePool field. Since `unblockedMigs` contains
		// all unblocked MIGs from the current `nodePool`, this assignment ensures
		// `nodePool` is correctly set for each MIG in this node pool.

		if nodePool.Autoscaled {
			AddMigsToNodePool(nodePool.Name, unblockedMigs...)
			for _, mig := range unblockedMigs {
				existingMigs[mig.GceRef()] = struct{}{}
				if m.cache.RegisterMig(mig) {
					newOrModifiedMigs[mig] = true
				}
			}
		}

		// We add all node pools specs to the map, whether they are autoscaled or not
		nodePoolSpecs[nodePool.Name] = nodePool.Spec
	}
	m.cache.InvalidateNodePoolSpecCache()
	m.cache.RegisterNodePoolSpecs(nodePoolSpecs)
	for mig := range newOrModifiedMigs {
		m.validateMigTemplateNode(mig)
	}
	for _, mig := range m.migLister.GetGkeMigs() {
		if _, found := existingMigs[mig.GceRef()]; !found {
			m.cache.UnregisterMig(mig)
		}
	}
	m.cleanUpOutdatedMigIdToInjectedNg()
	m.cache.SetAllNodePoolNames(allNodePoolNames)
}

func (m *gkeManagerImpl) cleanUpOutdatedMigIdToInjectedNg() {
	m.migIdToInjectedNgMutex.Lock()
	defer m.migIdToInjectedNgMutex.Unlock()
	for migRef := range m.migIdToInjectedNg {
		if time.Now().After(m.cache.LastMigRegistration(migRef).Add(injectedMigCacheTTL)) {
			delete(m.migIdToInjectedNg, migRef)
		}
	}
}

// validateMigTemplateNode validates the MIG and if an error of type CloudProviderError
// is found the mig is blocked to not cause CA breaking the loop
func (m *gkeManagerImpl) validateMigTemplateNode(mig *GkeMig) {
	_, err := m.GetMigTemplateNodeInfo(mig)
	if err != nil && isCloudProviderError(err) {
		klog.Errorf("An error occurred when processing mig %s: %v", mig.GceRef().Name, err)
		m.cache.MarkIrretrievableMig(mig.GceRef(), 1, IrretrievableMigReasonCloudProviderError)
	}
}

func nodePoolMIGs(gkeManager GkeManager, domainUrl string, nodePool gkeclient.NodePool) ([]*GkeMig, error) {
	var migs []*GkeMig
	for _, igurl := range nodePool.InstanceGroupUrls {

		project, zone, name, err := gce.ParseIgmUrl(igurl)
		if err != nil {
			return nil, err
		}
		blueGreenInfo, err := getMigBlueGreenInfo(nodePool.BlueGreenInfo, igurl)
		if err != nil {
			// This probably means that there's an ongoing B/G update, but there's something wrong with how we consume
			// the B/G API. In that case, it seems better to return an error here and skip scaling this node pool until
			// the update finishes. This means that CA won't be able to scale it beyond the original capacity if more pods
			// appear. An alternative could be treating the node pool as if the update wasn't happening (i.e. set
			// blueGreenInfo to nil without returning an error), but then we risk CA scaling down blue MIGs.
			return nil, fmt.Errorf("couldn't parse B/G info for node pool %q: %w", nodePool.Name, err)
		}
		gceRef := gce.GceRef{
			Name:    name,
			Zone:    zone,
			Project: project,
		}
		mig := NewGkeMig(gceRef, domainUrl, gkeManager)
		mig.exist = true
		mig.autoprovisioned = nodePool.Autoprovisioned
		mig.nodeConfig = &NodeConfig{
			ThreadsPerCore:     nodePool.ThreadsPerCore,
			Version:            nodePool.Version,
			IsConfidentialNode: nodePool.ConfidentialNodesEnabled,
		}
		mig.minSize = int(nodePool.MinNodeCount)
		mig.maxSize = int(nodePool.MaxNodeCount)
		mig.totalMinSize = int(nodePool.TotalMinNodeCount)
		mig.totalMaxSize = int(nodePool.TotalMaxNodeCount)
		mig.locationPolicy = toLocationPolicyEnum(nodePool.LocationPolicy)
		mig.spec = nodePool.Spec
		mig.blueGreenInfo = blueGreenInfo
		mig.queuedProvisioning = nodePool.QueuedProvisioning
		// TODO(b/420880764): verify whether shortLivedUpgradeInProgress is correctly propagated despite missing here
		mig.status = nodePool.Status
		mig.deploymentType = gkeManager.GetDeploymentType(gceRef, nodePool.Spec)
		migs = append(migs, mig)
	}

	return migs, nil
}

func getMigBlueGreenInfo(blueGreenInfo *gkeclient.BlueGreenInfo, migUrl string) (*MigBlueGreenInfo, error) {
	if blueGreenInfo == nil {
		// No B/G update in progress in this node pool.
		return nil, nil
	}
	for _, greenMigUrl := range blueGreenInfo.GreenMigUrls {
		if migUrl == greenMigUrl {
			if blueGreenInfo.Autoscaled {
				return &MigBlueGreenInfo{Color: GreenMig, Phase: blueGreenInfo.Phase, IsAutoScaled: true}, nil
			}
			return &MigBlueGreenInfo{Color: GreenMig, Phase: blueGreenInfo.Phase}, nil
		}
	}
	for _, blueMigUrl := range blueGreenInfo.BlueMigUrls {
		if migUrl == blueMigUrl {
			if blueGreenInfo.Autoscaled {
				return &MigBlueGreenInfo{Color: BlueMig, Phase: blueGreenInfo.Phase, IsAutoScaled: true}, nil
			}
			return &MigBlueGreenInfo{Color: BlueMig, Phase: blueGreenInfo.Phase}, nil
		}
	}
	return nil, fmt.Errorf("MIG %q is not marked as blue or green", migUrl)
}

// GetNumberOfSurgeNodesInMig get the number of surge nodes in a mig.
func (m *gkeManagerImpl) GetNumberOfSurgeNodesInMig(mig *GkeMig) int {
	if m.surgeUpgradeResourceTracker == nil {
		return 0
	}
	if !mig.Exist() {
		return 0
	}

	surgeNodes, err := m.surgeUpgradeResourceTracker.SurgeNodesInMIG(mig)
	if err != nil {
		klog.Errorf("could not get surge nodes in group; %v", err)
		return 0
	}
	return surgeNodes
}

// AreConfidentialNodesEnabled checks if ConfidentialNodes are enabled in cluster.
// AreConfidentialNodesEnabled returns true if confidential nodes are enabled or a specific confidential instance type is requested.
func (m *gkeManagerImpl) AreConfidentialNodesEnabled() bool {
	if m.confidentialNodesEnabled {
		return true
	}
	confidentialInstanceType := m.GetConfidentialInstanceType()
	return confidentialInstanceType != "" && confidentialInstanceType != gkelabels.UnspecifiedConfidentialNodeTypeValue
}

// GetConfidentialInstanceType returns the confidential instance type of the cluster.
func (m *gkeManagerImpl) GetConfidentialInstanceType() string {
	return m.confidentialInstanceType
}

// IsClusterUsingPSCInfrastructure checks if cluster is using PSC infrastructure. If so, cluster support public and private nodes.
func (m *gkeManagerImpl) IsClusterUsingPSCInfrastructure() bool {
	return m.isClusterUsingPSCInfrastructure
}

// GetDefaultEnablePrivateNodes returns cluster default private nodes setting.
func (m *gkeManagerImpl) GetDefaultEnablePrivateNodes() bool {
	return m.defaultEnablePrivateNodes
}

// GetDefaultNodePoolDiskType returns a default node pool disk type
// Configured on cluster creation:
// ref: https://cloud.google.com/kubernetes-engine/docs/how-to/custom-boot-disks#specify
func (m *gkeManagerImpl) GetDefaultNodePoolDiskType() string {
	if m.autoprovisioningNodePoolDefaults != nil {
		return m.autoprovisioningNodePoolDefaults.DiskType
	}
	return gce.DefaultBootDiskType
}

// GetDefaultNodePoolMinCpuPlatform returns a default node pool min cpu platform
func (m *gkeManagerImpl) GetDefaultNodePoolMinCpuPlatform() string {
	if m.autoprovisioningNodePoolDefaults != nil {
		return m.autoprovisioningNodePoolDefaults.MinCpuPlatform
	}
	return ""
}

// GetDefaultNodePoolDiskSizeGB returns a default node pool disk size GiB
func (m *gkeManagerImpl) GetDefaultNodePoolDiskSizeGB() int64 {
	if m.autoprovisioningNodePoolDefaults != nil && m.autoprovisioningNodePoolDefaults.DiskSizeGb != 0 {
		return m.autoprovisioningNodePoolDefaults.DiskSizeGb
	}
	if m.managerOptions.AutopilotEnabled {
		return machinetypes.DefaultDiskSizeGBForAutopilot
	}
	return machinetypes.DefaultDiskSizeGBForStandard
}

// GetNewNodePoolDaemonSetConditions returns the flags for gke daemon sets
func (m *gkeManagerImpl) GetNewNodePoolDaemonSetConditions() *DaemonSetConditions {
	if m.NewNodePoolDaemonSetConditions == nil {
		return &DaemonSetConditions{}
	}
	return m.NewNodePoolDaemonSetConditions
}

// GetExistingNodeGroupLocations returns a list of locations for created node groups
func (m *gkeManagerImpl) GetExistingNodeGroupLocations() []string {
	return m.migLister.GetGkeMigsLocations()
}

// GetAutoprovisioningLocations returns a list of locations where NAP can create new nodepools.
func (m *gkeManagerImpl) GetAutoprovisioningLocations() []string {
	if err := m.validateNAPEnabled(); err != nil {
		return []string{}
	}
	return m.gkeConfigurationCache.getAutoprovisioningLocations()
}

// DeleteNodePool deletes a node pool corresponding to the given MIG.
func (m *gkeManagerImpl) DeleteNodePool(toBeRemoved *GkeMig) error {
	err := m.DeleteNodePoolNoRefresh(toBeRemoved)
	if err != nil {
		return err
	}
	return m.refreshGkeResources()
}

// DeleteNodePoolNoRefresh deletes a node pool corresponding to the given MIG but does not refresh cluster resources.
func (m *gkeManagerImpl) DeleteNodePoolNoRefresh(toBeRemoved *GkeMig) error {
	if err := m.validateNAPEnabled(); err != nil {
		return err
	}

	if !toBeRemoved.Autoprovisioned() {
		return fmt.Errorf("only autoprovisioned node pools can be deleted")
	}
	// TODO: handle multi-zonal node pools.
	err := m.gkeService.DeleteNodePool(toBeRemoved.NodePoolName())
	if err == nil {
		// Remove nodepool from the cache so NAP cleanup does not analyze already removed migs previously marked as missing.
		m.cache.UnregisterNodePool(toBeRemoved.NodePoolName())
	}
	return err
}

// DeleteNodePoolAsync deletes node pool asynchronously. Returns immediately.
func (m *gkeManagerImpl) DeleteNodePoolAsync(toBeRemoved *GkeMig, finalizer interfaces.AsyncNodeGroupFinalizer) error {
	return fmt.Errorf("async node pool removal is not supported by gkeManager")
}

// GetMigsTargetSize gets sum of MIGs target sizes.
func (m *gkeManagerImpl) GetMigsTargetSize(migRefs []gce.GceRef) (int64, error) {
	var migsTargetSize int64
	for _, migRef := range migRefs {
		migSize, err := m.migInfoProvider.GetMigTargetSize(migRef)
		if err != nil {
			return 0, fmt.Errorf("Could not get mig size for mig (%s), got error: %v", migRef.String(), err)
		}
		migsTargetSize += migSize
	}
	return migsTargetSize, nil
}

func (m *gkeManagerImpl) validateLocationForMachineConfig(location string, machineType string, acceleratorConfig *gke_api_beta.AcceleratorConfig) (ok bool, reason string, err error) {
	err = m.ValidateMachineTypeConfig(machineType, location)
	if err != nil {
		return false,
			fmt.Sprintf("Machine type %s is not supported in zone %v", machineType, location),
			nil
	}
	if acceleratorConfig == nil {
		return true, "", nil
	}
	machine, err := m.GetMachineType(machineType, location)
	if err != nil {
		return false, "",
			fmt.Errorf("couldn't get cpus and mem for machineType %s in zone %s; %v", machineType, location, err)
	}
	// We provide an empty gpuPartitionSize and gpuMaxSharedClients because the accelerator count was already recalculated according to partitioning.
	err = m.ValidateGpuConfig(acceleratorConfig.AcceleratorType, "", "", "", machineType, acceleratorConfig.AcceleratorCount, location, machine.CPU, machine.Memory)
	if err != nil {
		return false,
			fmt.Sprintf("GPU configuration (gpuType %s, gpuCount %d, machineType %s) is not supported in zone %v",
				acceleratorConfig.AcceleratorType, acceleratorConfig.AcceleratorCount, machineType, location),
			nil
	}
	return true, "", nil
}

func (m *gkeManagerImpl) validateLocationForMinCpuPlatform(location string, requestedMinCpuPlatform string) (ok bool, reason string, err error) {
	if requestedMinCpuPlatform == "" {
		return true, "", nil
	}
	availableCpuPlatforms, err := m.availableCpuPlatformsProvider.GetAvailableCpuPlatforms(location)
	if err != nil {
		return false, "", err
	}
	for _, cpuPlatform := range availableCpuPlatforms {
		if cpuPlatform == requestedMinCpuPlatform {
			return true, "", nil
		}
	}
	return false, fmt.Sprintf("CPU platform %v is not supported in zone %v; supported CPU platforms %#v", requestedMinCpuPlatform, location, availableCpuPlatforms), nil
}

func (m *gkeManagerImpl) ValidateLocationForDiskType(location string, requestedDiskType string) (ok bool, reason string, err error) {
	if requestedDiskType == "" {
		return true, "", nil
	}

	availableDiskTypes, err := m.availableDiskTypesProvider.GetAvailableDiskTypes(location)
	if err != nil {
		return false, "", err
	}

	for _, diskType := range availableDiskTypes {
		if diskType == requestedDiskType {
			return true, "", nil
		}
	}

	return false, fmt.Sprintf("Disk type %s is not supported in zone %v; supported disk types: %#v", requestedDiskType, location, availableDiskTypes), nil
}

func (m *gkeManagerImpl) limitNodePoolLocations(
	mainZone string,
	specifiedLocations []string,
	machineType string,
	diskType string,
	acceleratorConfig *gke_api_beta.AcceleratorConfig,
	requestedMinCpuPlatform string,
	compactPlacement bool,
	reservationZoneSpecified bool) ([]string, error) {
	reasons := make(map[string]string)
	testFunctions := []func(location string) (ok bool, reason string, err error){
		func(location string) (ok bool, reason string, err error) {
			return m.validateLocationForMachineConfig(location, machineType, acceleratorConfig)
		},
		func(location string) (ok bool, reason string, err error) {
			return m.validateLocationForMinCpuPlatform(location, requestedMinCpuPlatform)
		},
		func(location string) (ok bool, reason string, err error) {
			return m.ValidateLocationForDiskType(location, diskType)
		},
	}

	var allZones []string
	if m.managerOptions.enableUserAnyZoneSelection {
		clusterRegion, err := gkeutil.GetRegionFromLocation(m.GetLocation())
		if err != nil {
			return nil, err
		}
		allZones, err = m.GetZonesInRegion(clusterRegion)
		if err != nil {
			return nil, err
		}
	} else {
		allZones = m.GetAutoprovisioningLocations()
	}

	for _, location := range allZones {
		for _, testFunction := range testFunctions {
			ok, reason, err := testFunction(location)
			if err != nil {
				return nil, err
			}
			if !ok {
				oldReason := reasons[location]
				if oldReason != "" {
					reason = fmt.Sprintf("%s; %v", oldReason, reason)
				}
				reasons[location] = reason
			}
		}
	}

	klog.V(6).Infof("Filtering out some locations; %v", reasons)

	if _, masterLocationFailed := reasons[mainZone]; masterLocationFailed {
		return nil, fmt.Errorf("Cannot create node pool for master location %v; %v", mainZone, reasons[mainZone])
	}

	if compactPlacement && len(specifiedLocations) > 1 {
		return nil, fmt.Errorf("Cannot create node pool for multiple specified locations %v when using compact placement", specifiedLocations)
	}

	// Compact Placement node pools and/or node pools which request reservation have to be limited to a single zone,
	// so we're choosing pre-selected zone
	if compactPlacement || reservationZoneSpecified {
		return []string{mainZone}, nil
	}

	// Prioritize specified locations if available.
	if len(specifiedLocations) > 0 {
		allZonesMap := make(map[string]bool)
		for _, loc := range allZones {
			allZonesMap[loc] = true
		}

		// TODO(b/517095739): Reject node pool options with bad specified zones - reject all requirements for bad specified zones in injection.go.
		// We should not create an option with specified zones outside of
		// AP locations or all zones. Such a option should be discarded early.
		failedReasons := make([]string, 0)
		failed := false
		for _, location := range specifiedLocations {
			if _, found := allZonesMap[location]; !found {
				newReason := fmt.Sprintf("location %v not configured for autoprovisioning", location)
				failedReasons = append(failedReasons, newReason)
				failed = true
			}
			if _, locationFailed := reasons[location]; locationFailed {
				// TODO(b/517095162): report this to CRD Validator
				failedReasons = append(failedReasons, reasons[location])
				failed = true
			}
		}
		if failed {
			errMsg := strings.Join(failedReasons, "; ")
			return nil, fmt.Errorf("Cannot create node pool for specified locations: %v; %v", specifiedLocations, errMsg)
		}

		return specifiedLocations, nil
	}

	// Fallback to Autoprovisioning Locations if not specified.
	var validLocations []string
	for _, location := range m.GetAutoprovisioningLocations() {
		if _, failedLocation := reasons[location]; !failedLocation {
			validLocations = append(validLocations, location)
		}
	}

	return validLocations, nil
}

func usesPlacement(mig *GkeMig) bool {
	return mig.spec.PlacementGroup.UsesPlacement() || mig.spec.TpuMultiHost
}

// reservationZoneSpecified validates the reservation zone, if present.
// if affinity is specific and reservation zone is present,
// then the node pool locations should be limited to reservation zone.
func reservationZoneSpecified(mig *GkeMig) bool {
	if len(mig.spec.Labels) == 0 {
		return false
	}
	zone := mig.spec.Labels[gkelabels.ReservationZoneLabel]
	return mig.spec.ReservationAffinity != nil && mig.spec.ReservationAffinity.ConsumeReservationType == gkeclient.ReservationAffinitySpecific && zone == mig.gceRef.Zone
}

// CreateNodePool creates a node pool based on provided spec and returns newly created MIG.
func (m *gkeManagerImpl) CreateNodePool(mig *GkeMig) (MigCreateNodePoolResult, error) {
	nodePoolSpec, err := m.NewNodePoolSpec(mig)
	if err != nil {
		return MigCreateNodePoolResult{}, err
	}
	err = m.CreateNodePoolNoRefresh(mig.NodePoolName(), nodePoolSpec)
	if err != nil {
		if caerrors.ToAutoscalerError(caerrors.InternalError, err).Type() == gkeclient.GkePersistentOperationError {
			klog.V(2).Infof("Node pool creation has failed with err: %v, delete broken node pool: %s", err, mig.NodePoolName())
			m.CleanUpBrokenNodePool(mig.NodePoolName())
		} else {
			klog.V(2).Infof("Node pool create request has failed, err: %v", err)
		}
		return MigCreateNodePoolResult{}, err
	}
	for start := time.Now(); time.Since(start) < migCreationWaitTimeout; time.Sleep(migCreationCheckInterval) {
		err := m.refreshGkeResources()
		if err != nil {
			return MigCreateNodePoolResult{}, err
		}
		result := m.getNodePoolCreationResult(mig)
		if result != nil && result.MainCreatedMig != nil {
			if result.MainCreatedMig.Status() == NodePoolErrorStatus {
				klog.Warningf("Node pool creation has failed: main MIG has error status, delete broken node pool: %s", mig.NodePoolName())
				m.CleanUpBrokenNodePool(mig.NodePoolName())
				return MigCreateNodePoolResult{}, fmt.Errorf("main MIG for node pool %s has error status", mig.NodePoolName())
			}
			for _, createdMig := range result.AllCreatedMigs() {
				m.SetInjectedMig(createdMig, mig)
			}
			extraMigsRefs := make([]gce.GceRef, 0, len(result.ExtraCreatedMigs))
			for _, extraMig := range result.ExtraCreatedMigs {
				extraMigsRefs = append(extraMigsRefs, extraMig.GceRef())
			}
			klog.Infof("created main MIG %+v and following extra MIGs %+v", result.MainCreatedMig, extraMigsRefs)
			return *result, nil
		}
	}
	klog.V(2).Infof("Node pool creation has failed: main MIG wasn't found, delete broken node pool: %s", mig.NodePoolName())
	m.CleanUpBrokenNodePool(mig.NodePoolName())
	return MigCreateNodePoolResult{}, fmt.Errorf("could not find main MIG for node pool %s", mig.NodePoolName())
}

// NewNodePoolSpec creates node-pool spec based on mig definition.
func (m *gkeManagerImpl) NewNodePoolSpec(mig *GkeMig) (*gkeclient.NodePoolSpec, error) {
	if err := m.validateNAPEnabled(); err != nil {
		return nil, err
	}

	if mig.spec == nil {
		return nil, fmt.Errorf("could not find mig spec for mig %s", mig.NodePoolName())
	}

	if len(mig.spec.Accelerators) > 1 {
		return nil, fmt.Errorf("autoprovisioning for MIG with multiple accelerators is not supported (%s)", mig.NodePoolName())
	}

	// Make copy-by-value.
	nodePoolSpec := *mig.spec

	var acceleratorConfig *gke_api_beta.AcceleratorConfig
	if len(mig.spec.Accelerators) > 0 {
		acceleratorConfig = mig.spec.Accelerators[0]
	}

	// non-empty specifiedLocations can currently only be a result of CCC zonal preferences
	specifiedLocations := mig.spec.Locations
	locations, err := m.limitNodePoolLocations(mig.GceRef().Zone, specifiedLocations, nodePoolSpec.MachineType, nodePoolSpec.DiskType, acceleratorConfig, nodePoolSpec.MinCpuPlatform,
		usesPlacement(mig), reservationZoneSpecified(mig))
	if err != nil {
		return nil, err
	}
	nodePoolSpec.Locations = locations

	// Remove internal Labels and Taints.
	nodePoolSpec.Labels = filterOutExternalSystemLabels(nodePoolSpec.Labels, m.allowlistedSystemLabelsMatcher, m.managerOptions)
	nodePoolSpec.Taints = filterOutSystemTaints(nodePoolSpec.Taints)

	if m.autoprovisioningNodePoolDefaults != nil {
		nodePoolSpec.Defaults = m.autoprovisioningNodePoolDefaults
	}
	nodePoolSpec.ClusterNetworkPath = m.networkPath
	nodePoolSpec.ClusterSubnetworkPath = m.subnetworkPath
	nodePoolSpec.ClusterSubnetwork = m.subnetwork
	return &nodePoolSpec, nil
}

// CreateNodePoolNoRefresh initiates node pool creation, but does not wait for node pools to be created and registered in GKE.
func (m *gkeManagerImpl) CreateNodePoolNoRefresh(nodePoolName string, nodePoolSpec *gkeclient.NodePoolSpec) error {
	if err := m.validateNAPEnabled(); err != nil {
		return err
	}
	return m.gkeService.CreateNodePool(nodePoolName, nodePoolSpec)
}

// UpdateNodePoolLabels updates node pool labels.
func (m *gkeManagerImpl) UpdateNodePoolLabels(nodePoolName string, labels map[string]string) error {
	return m.gkeService.UpdateNodePoolLabels(nodePoolName, labels)
}

// getNodePoolCreationResult returns node pool creation result based on registered migs.
func (m *gkeManagerImpl) getNodePoolCreationResult(mig *GkeMig) *MigCreateNodePoolResult {
	result := MigCreateNodePoolResult{}
	for _, gkeMig := range m.migLister.GetGkeMigs() {
		if gkeMig.NodePoolName() == mig.NodePoolName() {
			// Compact Placement node pools are always in a single zone, but the zone is picked at random,
			// so it may be different than the zone originally assigned to the mig.
			if gkeMig.gceRef.Zone == mig.gceRef.Zone || usesPlacement(mig) {
				result.MainCreatedMig = gkeMig
			} else {
				result.ExtraCreatedMigs = append(result.ExtraCreatedMigs, gkeMig)
			}
		}
	}
	if result.MainCreatedMig != nil {
		return &result
	}
	return nil
}

// CreateNodePoolAsync implements GkeManager. Returns error as only AsyncGkeManager handles async node pool creation.
func (m *gkeManagerImpl) CreateNodePoolAsync(mig *GkeMig, _ interfaces.AsyncNodeGroupUpdater, _ interfaces.AsyncNodeGroupInitializer) (MigCreateNodePoolResult, error) {
	return MigCreateNodePoolResult{}, fmt.Errorf("async node pool creation is not supported by sync gkeManager")
}

// IsUpcoming implements GkeManager. Always returns false as only AsyncGkeManager handles upcoming node groups.
func (m *gkeManagerImpl) IsUpcoming(mig *GkeMig) bool {
	return false
}

// GetImageTypeForNap returns the default Node Autoprovisioning Image Type for a mig.
func (m *gkeManagerImpl) GetImageTypeForNap(mig *GkeMig) string {
	if mig.spec != nil {
		itype := strings.ToLower(mig.spec.ImageType)
		if itype == string(gce.OperatingSystemImageCOSContainerd) || itype == string(gce.OperatingSystemImageUbuntuContainerd) {
			return itype
		} else if itype != "" {
			klog.Warningf("Invalid ImageType for a mig: %v, using default image type.", itype)
		}
	}
	if m.autoprovisioningNodePoolDefaults != nil && m.autoprovisioningNodePoolDefaults.ImageType != "" {
		return m.autoprovisioningNodePoolDefaults.ImageType
	}
	return strings.ToLower(defaultImageType)
}

// GetOsDistributionForNap returns the Node Autoprovisioning Operating System distribution for a mig.
func (m *gkeManagerImpl) GetOsDistributionForNap(mig *GkeMig) gce.OperatingSystemDistribution {
	imageType := m.GetImageTypeForNap(mig)
	switch imageType {
	case string(gce.OperatingSystemImageCOS):
		return gce.OperatingSystemDistributionCOS
	case string(gce.OperatingSystemImageUbuntu):
		return gce.OperatingSystemDistributionUbuntu
	case string(gce.OperatingSystemImageCOSContainerd):
		return gce.OperatingSystemDistributionCOS
	case string(gce.OperatingSystemImageUbuntuContainerd):
		return gce.OperatingSystemDistributionUbuntu
	default:
		klog.Errorf("Unknown OperatingSystemDistribution for %s, using default OS Distribution", imageType)
		return defaultOsDistribution
	}
}

func (m *gkeManagerImpl) refreshMachinesCache() error {
	if m.machinesCacheLastRefresh.Add(machinesRefreshInterval).After(time.Now()) {
		return nil
	}
	// Machine types cache is only updated directly after refreshing cluster resources, so value from cache should be good enough.
	migLocations := m.migLister.GetGkeMigsLocations()
	locations := make(map[string]struct{}, len(migLocations))
	for _, l := range migLocations {
		locations[l] = struct{}{}
	}
	if m.IsNodeAutoprovisioningEnabled() {
		for _, l := range m.GetAutoprovisioningLocations() {
			locations[l] = struct{}{}
		}
	}
	machinesCache := make(map[gce.MachineTypeKey]gce.MachineType)
	for location := range locations {
		machineTypes, err := m.gceService.FetchMachineTypes(location)
		if err != nil {
			return err
		}
		for _, machineType := range machineTypes {
			gceMt, err := gce.NewMachineTypeFromAPI(machineType.Name, machineType)
			if err != nil {
				klog.Errorf("Failed to parse machine type information for %v/%v", location, machineType.Name)
				continue
			}
			machinesCache[gce.MachineTypeKey{Zone: location, MachineTypeName: machineType.Name}] = gceMt
		}

	}
	m.cache.SetMachines(machinesCache)
	nextRefresh := time.Now()
	m.machinesCacheLastRefresh = nextRefresh
	klog.V(2).Infof("Refreshed machine types, next refresh after %v", nextRefresh)
	return nil
}

// IsNodeAutoprovisioningEnabled returns true if NAP is enabled.
func (m *gkeManagerImpl) IsNodeAutoprovisioningEnabled() bool {
	return m.autoprovisioningEligibility.IsNodeAutoprovisioningEnabled()
}

// AreClusterLimitsEnabled returns true if NAP cluster limits are enabled.
func (m *gkeManagerImpl) AreClusterLimitsEnabled() bool {
	return m.autoprovisioningEligibility.AreClusterLimitsEnabled()
}

// UseAutoprovisioningFeaturesForPodRequirements checks if pod should trigger autoprovisioning features.
func (m *gkeManagerImpl) UseAutoprovisioningFeaturesForPodRequirements(req *podrequirements.Requirements) bool {
	return m.autoprovisioningEligibility.UseAutoprovisioningFeaturesForPodRequirements(req)
}

// UseAutoprovisioningFeaturesForNodeGroup check if node group should trigger autoprovisioning features.
func (m *gkeManagerImpl) UseAutoprovisioningFeaturesForNodeGroup(nodeGroup cloudprovider.NodeGroup) bool {
	return m.autoprovisioningEligibility.UseAutoprovisioningFeaturesForNodeGroup(nodeGroup)
}

// GetMigSize gets MIG size.
func (m *gkeManagerImpl) GetMigSize(mig gce.Mig) (int64, error) {
	if !mig.Exist() {
		return 0, nil
	}
	return m.migInfoProvider.GetMigTargetSize(mig.GceRef())
}

// SetMigSize sets MIG size.
func (m *gkeManagerImpl) SetMigSize(mig gce.Mig, size int64) error {
	klog.V(0).Infof("Setting mig with id=%s size to %d", mig.Id(), size)
	m.cache.InvalidateMigTargetSize(mig.GceRef())
	err := m.gceService.ResizeMig(mig.GceRef(), size)
	if err != nil {
		return err
	}
	m.cache.SetMigTargetSize(mig.GceRef(), size)
	return nil
}

// IsMigStable returns whether the MIG is stable.
func (m *gkeManagerImpl) IsMigStable(mig gce.Mig) (bool, error) {
	return m.migInfoProvider.GetMigIsStable(mig.GceRef())
}

func (m *gkeManagerImpl) CreateInstances(mig gce.Mig, delta int64) error {
	klog.V(0).Infof("Adding %d new instances to mig %v.", delta, mig.Id())
	instances, err := m.GetMigNodes(mig)
	if err != nil {
		return fmt.Errorf("could not get mig instances: %v", err)
	}
	existingIds := make([]string, 0, len(instances))
	for _, ins := range instances {
		existingIds = append(existingIds, ins.Id)
	}
	basename, err := m.migInfoProvider.GetMigBasename(mig.GceRef())
	if err != nil {
		return fmt.Errorf("can't create instances in %s: failed to fetch basename: %v", mig.GceRef(), err)
	}
	m.cache.InvalidateMigTargetSize(mig.GceRef())
	totalReqs := int((delta + createInstancesRequestLimit - 1) / createInstancesRequestLimit)
	remaining := delta
	for i := 0; i < totalReqs; i++ {
		increment := min(remaining, createInstancesRequestLimit)
		if totalReqs > 1 {
			klog.Infof("Sending chunked GCE createInstances request. Request: %d/%d RequestSize: %v/%v MIG: %v", i+1, totalReqs, increment, delta, mig.Id())
		}
		newIds, err := m.gceService.CreateInstances(mig.GceRef(), basename, increment, existingIds)
		if err != nil {
			return err
		}
		remaining -= increment
		existingIds = append(existingIds, newIds...)
	}
	return nil
}

// CreateFlexResizeRequests handles DWS Flex scale up by creating Resize Requests requesting single VMs
func (m *gkeManagerImpl) CreateFlexResizeRequests(mig gce.Mig, delta int64) error {
	ctx := context.Background()
	m.cache.InvalidateMigTargetSize(mig.GceRef())

	gkeMig, ok := mig.(*GkeMig)
	if !ok {
		return fmt.Errorf(`unexpected NodeGroup type: want "*gke.GkeMig", got %q`, reflect.TypeOf(mig))
	}

	// Looping over a very big delta could take too long and invalidate the timestamp for liveness probes, so we split it into batches
	batchSize := min(delta, maxFlexStartRRBatchSize)
	if delta > maxFlexStartRRBatchSize {
		klog.V(2).Infof("Creating only %d out of %d Flex Resize Requests in this CA loop due to scalability limitations", batchSize, delta)
		// Register the remainder as failed-to-create so error_reporter.go will correct it by: RegisterScaleUp(-remainder)
		m.flexResizeRequestService.RegisterFailedResizeRequestsCreation(mig.GceRef(), fragmentedResizeRequestWarning(delta, batchSize), int(delta-maxFlexStartRRBatchSize))
	}

	rrNamePrefix := resizerequestclient.NewFlexStartNonQueuedScaleUpId()
	klog.V(0).Infof("Adding %d new instances via Resize Requests to mig %v, DWS flex scale up ID: %q", batchSize, mig.Id(), rrNamePrefix)

	for instanceIndex := range batchSize {
		resizeRequestName := resizerequestclient.FlexStartNonQueuedResizeRequestName(rrNamePrefix, int(instanceIndex))
		klog.V(2).Infof("Creating Flex Resize Request %q for %v-th new instance in mig %v.", resizeRequestName, instanceIndex, mig.Id())

		duration, err := queuedwrapper.MaxRunDurationFromStringOrDefaultWithWarning(gkeMig.spec.MaxRunDurationInSeconds)
		if err != nil {
			return fmt.Errorf("got error while parsing MaxRunDuration GkeMig %+v field, value: %q, error: %v", mig.GceRef(), gkeMig.spec.MaxRunDurationInSeconds, err)
		}
		createRequest := resizerequestclient.ResizeRequestCreateRequest{
			Name:                 resizeRequestName,
			ResizeBy:             1,
			RequestedRunDuration: duration,
		}

		err = m.flexResizeRequestService.CreateResizeRequest(ctx, mig.GceRef(), createRequest)
		if err != nil {
			klog.Errorf("Create Flex Resize Request failed: %v", err)

			// We didn't create any ResizeRequests successfuly yet, so fail the scale up
			if instanceIndex == 0 {
				return err
			}

			// Some of the Resize Requests already got created successfully, so
			// we will return without error as if the scale up succeeded fully, but we'll save:
			// * how many RRs (VMs) didn't get created
			// * what was the error
			// so that on the next MIG Refresh in error_reporter.go we will correct the number by: RegisterScaleUp(-failedRRs)
			klog.Warningf("Failed creating the %d-th Resize Request %q with error: %v; Scale up will proceed as successful and the number of requested nodes (%d) will be corrected by %d on a failed scale up registration later on", instanceIndex, resizeRequestName, err, delta, delta-instanceIndex)
			m.flexResizeRequestService.RegisterFailedResizeRequestsCreation(mig.GceRef(), err, int(batchSize-instanceIndex))
			break
		}
	}
	return nil
}

// QueuedProvisioningNodeHasScaleDownImmunity returns true if the provided QueuedProvisioning node still shouldn't get scaled down,
// i.e. additionalImmunity hasn't ran out yet.
func (m *gkeManagerImpl) QueuedProvisioningNodeHasScaleDownImmunity(node *apiv1.Node, migSpec *reconciler.QueuedProvisioningMigSpec, now time.Time) bool {
	return m.provisioningRequestManager.QueuedProvisioningNodeHasScaleDownImmunity(node, migSpec, now)
}

// CreateQueuedInstances queues creation of VMs
func (m *gkeManagerImpl) CreateQueuedInstances(pr prpods.ProvReqID, mig *GkeMig, delta int64, shouldUpdateProvReqDetails manager.ShouldUpdateProvReqDetails) error {
	spec := &manager.ProvisioningRequestDetailsSpec{
		ProjectID:          mig.GceRef().Project,
		ProvReqNamespace:   pr.Namespace,
		ProvReqName:        pr.Name,
		Zone:               mig.GceRef().Zone,
		Delta:              delta,
		MigName:            mig.GceRef().Name,
		MigAutoProvisioned: mig.Autoprovisioned(),
		NodePoolName:       mig.NodePoolName(),
		AcceleratorType:    mig.Accelerators(),

		ProvisioningMode: queuedwrapper.ProvisioningModeResizeRequest,
	}

	bulkProvisioning := mig.UsesBulkProvisioning()
	if bulkProvisioning {
		spec.ProvisioningMode = queuedwrapper.ProvisioningModeBulkMig
	}

	klog.V(0).Infof("Queueing for %d new instances in mig %v nodepool %q with accelerator type %q. Bulk provisioning: %v", delta, mig.Id(), spec.NodePoolName, spec.AcceleratorType, bulkProvisioning)

	if bulkProvisioning {
		err := m.provisioningRequestManager.CreateQueuedBulkInstances(mig, spec)
		if err == nil {
			m.cache.InvalidateMigTargetSize(mig.GceRef())
		}
		return err
	}
	err := m.provisioningRequestManager.CreateResizeRequest(spec, shouldUpdateProvReqDetails)
	if err == nil {
		m.cache.InvalidateMigTargetSize(mig.GceRef())
	}
	return err
}

// CreateResizeRequest creates a RR with the Resize Request API using client specified by mode.
func (m *gkeManagerImpl) CreateResizeRequest(mig gce.Mig, delta int64) error {
	gkeMig, ok := mig.(*GkeMig)
	if !ok {
		return fmt.Errorf(`unexpected NodeGroup type: want "*gke.GkeMig", got %q`, reflect.TypeOf(mig))
	}

	mode := resizerequestclient.ResizeRequestModeAtomic
	service := m.atomicResizeRequestService
	if gkeMig.FlexStartNonQueued() {
		mode = resizerequestclient.ResizeRequestModeFlex
		service = m.flexResizeRequestService
	}

	klog.V(0).Infof("Creating %v Resize Request for %d new instances in mig %v.", mode, delta, mig.GceRef())
	ctx := context.Background()

	rrCreateReq, err := createRequest(gkeMig, mode, delta)
	if err != nil {
		return err
	}

	err = service.CreateResizeRequest(ctx, mig.GceRef(), rrCreateReq)
	if err != nil {
		klog.Errorf("Create %v Resize Request failed: %v", mode, err)
		return err
	}
	m.cache.InvalidateMigTargetSize(mig.GceRef())
	return nil
}

func createRequest(mig *GkeMig, mode resizerequestclient.ResizeRequestMode, delta int64) (resizerequestclient.ResizeRequestCreateRequest, error) {
	switch mode {
	case resizerequestclient.ResizeRequestModeAtomic:
		return resizerequestclient.ResizeRequestCreateRequest{
			Name:     resizerequestclient.AtomicResizeRequestName(),
			ResizeBy: delta,
		}, nil
	case resizerequestclient.ResizeRequestModeFlex:
		duration, err := queuedwrapper.MaxRunDurationFromStringOrDefaultWithWarning(mig.spec.MaxRunDurationInSeconds)
		if err != nil {
			return resizerequestclient.ResizeRequestCreateRequest{}, fmt.Errorf("got error while parsing MaxRunDuration GkeMig %v field, value: %q, error: %v", mig.GceRef(), mig.spec.MaxRunDurationInSeconds, err)
		}
		return resizerequestclient.ResizeRequestCreateRequest{
			Name:                 resizerequestclient.FlexStartNonQueuedResizeRequestName(resizerequestclient.NewFlexStartNonQueuedScaleUpId(), 0),
			ResizeBy:             delta,
			RequestedRunDuration: duration,
		}, nil
	default:
		return resizerequestclient.ResizeRequestCreateRequest{}, fmt.Errorf("Create Resize Request: Unsupported Resize Request mode %q invoked for MIG %v", mode, mig.GceRef())
	}
}

// AdvanceResizeRequestCleanUp deletes the specified resize request.
func (m *gkeManagerImpl) AdvanceResizeRequestCleanUp(resizeRequest resizerequestclient.ResizeRequestStatus) error {
	if m.cache.FlexStartNonQueued(gce.GceRef{Zone: resizeRequest.Zone, Name: resizeRequest.MigName, Project: resizeRequest.ProjectID}) {
		return m.flexResizeRequestService.AdvanceResizeRequestCleanUp(context.Background(), resizeRequest)
	}
	return m.atomicResizeRequestService.AdvanceResizeRequestCleanUp(context.Background(), resizeRequest)
}

// ResetFailedResizeRequestsCreation returns map of failed Resize Creation errors and number of not created Resize Requests and clears the map
func (m *gkeManagerImpl) ResetFailedResizeRequestsCreation(migRef gce.GceRef) map[error]int {
	return m.flexResizeRequestService.ResetFailedResizeRequestsCreation(migRef)
}

// ReportState returns the report state of the particular Resize Request
func (m *gkeManagerImpl) ReportState(resizeRequest resizerequestclient.ResizeRequestStatus) resizerequestclient.ResizeRequestReportState {
	return m.flexResizeRequestService.ReportState(resizeRequest)
}

// SetReportState sets the report state of the particular Resize Request
func (m *gkeManagerImpl) SetReportState(resizeRequest resizerequestclient.ResizeRequestStatus, state resizerequestclient.ResizeRequestReportState) {
	m.flexResizeRequestService.SetReportState(resizeRequest, state)
}

// IsResizeRequestErrorHandlingEnabled returns if the resize request errors should be handled.
func (m *gkeManagerImpl) IsResizeRequestErrorHandlingEnabled() bool {
	return m.managerOptions.ResizeRequestErrorHandlingEnabled
}

// DeleteInstances deletes the given instances. All instances must be controlled by the same MIG.
func (m *gkeManagerImpl) DeleteInstances(instances []gce.GceRef) error {
	if len(instances) == 0 {
		return nil
	}
	commonMig, err := m.GetMigForInstance(instances[0])
	if err != nil {
		return err
	}
	for _, instance := range instances {
		mig, err := m.GetMigForInstance(instance)
		if err != nil {
			return err
		}
		if mig != commonMig {
			return fmt.Errorf("cannot delete instances which don't belong to the same MIG.")
		}
	}
	if gkeMig, ok := commonMig.(*GkeMig); ok && gkeMig.ResizeAtomically() {
		// If we got here, it means the MIG is going to atomically
		// scale down to 0 nodes. DeleteInstances call will not work in
		// this scenario, but since it is an atomic removal of all VMs
		// in the MIG, we can just set the target size to 0 instead.
		return m.SetMigSize(commonMig, 0)
	}
	m.cache.InvalidateMigTargetSize(commonMig.GceRef())
	return m.gceService.DeleteInstances(commonMig.GceRef(), instances)
}

func (m *gkeManagerImpl) GetGkeMigs() []*GkeMig {
	return m.migLister.GetGkeMigs()
}

func (m *gkeManagerImpl) GetGkeMigsBlockedByNotFoundError() []*GkeMig {
	return m.migLister.GetBlockedGkeMigs(IrretrievableMigReasonNotFound)
}

func (m *gkeManagerImpl) GetGkeMigsBlockedByServerError() []*GkeMig {
	return m.migLister.GetBlockedGkeMigs(IrretrievableMigReasonServerError)
}

func (m *gkeManagerImpl) GetAllNodePoolNames() sets.Set[string] {
	return m.cache.GetAllNodePoolNames()
}

// GetMigForInstance returns MIG to which the given instance belongs.
func (m *gkeManagerImpl) GetMigForInstance(instance gce.GceRef) (gce.Mig, error) {
	return m.migInfoProvider.GetMigForInstance(instance)
}

// GetMigNodes returns instances that belong to a MIG.
func (m *gkeManagerImpl) GetMigNodes(mig gce.Mig) ([]gce.GceInstance, error) {
	return m.migInfoProvider.GetMigInstances(mig.GceRef())
}

// GetLocation returns cluster's location.
func (m *gkeManagerImpl) GetLocation() string {
	return m.location
}

// GetProjectId returns id of GCE project to which the cluster belongs.
func (m *gkeManagerImpl) GetProjectId() string {
	return m.projectId
}

// GetReleaseChannel returns release channel of the cluster.
func (m *gkeManagerImpl) GetReleaseChannel() string {
	return m.releaseChannel
}

// GetClusterName returns the name of GKE cluster.
func (m *gkeManagerImpl) GetClusterName() string {
	return m.clusterName
}

// GetClusterVersion returns the version of GKE cluster.
func (m *gkeManagerImpl) GetClusterVersion() string {
	return m.clusterVersion
}

// GetClusterNetwork returns the GCE Network resource of the cluster's VPC
// This function will call Network GCE API once per gkeManager instance lifecycle,
// then retrieve the cached result on each following call.
func (m *gkeManagerImpl) GetClusterNetwork() (*gce_api.Network, error) {
	// Make sure networkPath is defined to avoid a race condition when GetClusterNetwork
	// is called before refreshGkeResources() defines cluster resources.
	if m.networkPath == "" {
		return nil, fmt.Errorf("networkPath is not defined")
	}
	projectId, networkName := parseNetworkPath(m.networkPath)
	if projectId == "" || networkName == "" {
		return nil, fmt.Errorf("invalid network string: must have format projects/{PROJECT_ID}/global/networks/{NETWORK_NAME} but got %s", m.networkPath)
	}

	var err error
	// Only fetch network resource if currently undefined.
	// This avoids redundant GCE API calls.
	if m.network == nil {
		m.network, err = m.gceService.FetchNetwork(projectId, networkName)
	}
	return m.network, err
}

func parseNetworkPath(networkPath string) (string, string) {
	// networkPath must be in the following format: projects/{PROJECT_ID}/global/networks/{NETWORK_NAME}
	parts := strings.Split(networkPath, "/")
	if len(parts) != 5 {
		return "", ""
	}
	projectId := parts[1]
	networkName := parts[4]
	return projectId, networkName
}

// RecommendLocations returns recommendation made by recommendLocations API.
func (m *gkeManagerImpl) RecommendLocations(ctx context.Context, region string, request gceclient.RecommendLocationsRequest) (*gceclient.RecommendLocationsResponse, error) {
	return m.recommendLocationsService.RecommendLocations(ctx, region, request)
}

func (m *gkeManagerImpl) FetchCapacityGuidance(ctx context.Context, flexibilityScopeKey string, instanceConfigs map[string]*api.InstanceConfig) (map[string]*api.InstanceAvailability, error) {
	return m.flexAdvisorService.FetchCapacityGuidance(ctx, flexibilityScopeKey, instanceConfigs)
}

func (m *gkeManagerImpl) SendCapacityDecision(ctx context.Context, decision api.ProvisioningDecisionNotification) error {
	return m.flexAdvisorService.SendCapacityDecision(ctx, decision)
}

// GetZonesInRegion returns all zones within a given region.
func (m *gkeManagerImpl) GetZonesInRegion(region string) ([]string, error) {
	zones, found := m.cache.GetZonesInRegion(region)
	if found {
		return zones, nil
	}

	zones, err := m.gceService.FetchZones(region)
	if err != nil {
		return nil, err
	}

	m.cache.SetZonesInRegion(region, zones)
	return zones, nil
}

// GetStandardZonesInRegion returns all standard zones within a given region.
func (m *gkeManagerImpl) GetStandardZonesInRegion(region string) ([]string, error) {
	zones, found := m.cache.GetStandardZonesInRegion(region)
	if found {
		return zones, nil
	}

	zones, err := m.gceService.FetchStandardZones(region)
	if err != nil {
		return nil, err
	}

	m.cache.SetStandardZonesInRegion(region, zones)
	return zones, nil
}

// GetAIZonesInRegion returns all AI zones within a given region.
func (m *gkeManagerImpl) GetAIZonesInRegion(region string) ([]string, error) {
	aiZones, found := m.cache.GetAIZonesInRegion(region)
	if found {
		return aiZones, nil
	}

	aiZones, err := m.gceService.FetchAIZones(region)
	if err != nil {
		return nil, err
	}

	m.cache.SetAIZonesInRegion(region, aiZones)
	return aiZones, nil
}

func (m *gkeManagerImpl) GetFutureReservationsInProject(projectID string) ([]*gceclient.GceFutureReservation, error) {
	return m.gceService.FetchFutureReservationsInProject(projectID)
}

// GetReservationBlocksInReservation returns the reservation blocks for a particular reservation, in specfied project and zone.
func (m *gkeManagerImpl) GetReservationBlocksInReservation(reservationRef gceclient.ReservationRef) ([]*gceclient.GceReservationBlock, error) {
	return m.gceService.FetchReservationBlocksInReservation(reservationRef)
}

func (m *gkeManagerImpl) GetReservationSubBlocksInReservationBlock(reservationRef gceclient.ReservationRef) ([]*gceclient.GceReservationSubBlock, error) {
	return m.gceService.FetchReservationSubBlocksInReservationBlock(reservationRef)
}

// GetResourcePolicies returns the resource policies in the provided project and region.
func (m *gkeManagerImpl) GetResourcePolicies(projectId, region string) ([]*gceclient.GceResourcePolicy, error) {
	return m.gceService.FetchResourcePolicies(projectId, region)
}

// GetMigInstanceTemplate returns instance template for a given mig.
func (m *gkeManagerImpl) GetMigInstanceTemplate(mig *GkeMig) (*gce_api.InstanceTemplate, error) {
	return m.migInfoProvider.GetMigInstanceTemplate(mig.GceRef())
}

// GetMigKubeEnv returns kube-env for a given mig.
func (m *gkeManagerImpl) GetMigKubeEnv(mig *GkeMig) (gce.KubeEnv, error) {
	return m.migInfoProvider.GetMigKubeEnv(mig.GceRef())
}

// RefreshForce triggers complete refresh of cluster cached resources.
func (m *gkeManagerImpl) RefreshForce() error {
	return m.refresh(true)
}

// Refresh triggers refresh of cluster cached resources if they are stale.
func (m *gkeManagerImpl) Refresh() error {
	return m.refresh(false)
}

// refresh invalidates cache and refreshes GKE and GCE resources.
func (m *gkeManagerImpl) refresh(force bool) error {
	m.cache.InvalidateAllMigInstances()
	m.refreshShortLivedUpgradeInProgress()
	m.cache.InvalidateAllMigTargetSizes()
	m.cache.InvalidateAllMigBasenames()
	m.cache.InvalidateAllListManagedInstancesResults()
	m.cache.InvalidateAllMigInstanceTemplateNames()
	m.InvalidateNodesScaleDownAllowedCache()
	m.cache.InvalidateCapacityCheckWaitTimes()

	// TODO(b/449919936): Cleanup after EK spot enabled suppoerted experiment is over.
	m.ekSpotEnabledCache.RefreshValue()

	m.refreshEkEdpEnabled()

	// TODO(b/348360895): Cleanup after EK experiment is over.
	m.resizableVmAutoprovisioningProvider.Refresh()

	// TODO(b/377482817): Cleanup after LA experiment is over.
	m.lookaheadBufferStrategyProvider.RefreshStrategy()

	m.lookaheadBufferStrategyProvider.SetEkResizingEnabled(m.ResizingEnabled(machinetypes.EK.Name()))

	// TODO(b/486148603): Cleanup experiment flag for custom thresholds when the experiment is over
	m.resizableVmCustomThresholdsProvider.RefreshCustomThresholds()

	if m.surgeUpgradeResourceTracker != nil {
		err := m.surgeUpgradeResourceTracker.Refresh()
		if err != nil {
			return fmt.Errorf("could not refresh surgeUpgradeResourceTracker; %v", err)
		}
	}
	if force || m.lastRefresh.Add(ClusterRefreshInterval).Before(time.Now()) {
		if err := m.forceRefreshResources(); err != nil {
			return err
		}

		// Once GCE MIGs list got updated drop instance templates for no longer tracked instance groups
		migs := m.migLister.GetMigs()
		m.cache.DropInstanceTemplatesForMissingMigs(migs)
	}
	// ProvisioningRequest Manager depends on MIG status it should be refreshed after the main loop
	if m.provisioningRequestManager != nil {
		rrMigs, bulkMigs := m.queuedProvisioningMigGceRefs()
		if err := m.provisioningRequestManager.Refresh(rrMigs, bulkMigs, time.Now()); err != nil {
			return fmt.Errorf("could not refresh provisioningRequestManager: %w", err)
		}
	}
	return m.initializeOnce()
}

func (m *gkeManagerImpl) queuedProvisioningMigGceRefs() (map[gce.GceRef]common.GkeMigWrapper, map[gce.GceRef]common.GkeMigWrapper) {
	gkeMigs := m.GetGkeMigs()
	rrMigs := make(map[gce.GceRef]common.GkeMigWrapper)
	bulkMigs := make(map[gce.GceRef]common.GkeMigWrapper)

	for _, mig := range gkeMigs {
		if !mig.queuedProvisioning {
			continue
		}
		if mig.UsesBulkProvisioning() {
			bulkMigs[mig.gceRef] = mig
		} else {
			rrMigs[mig.gceRef] = mig
		}
	}
	return rrMigs, bulkMigs
}

// forceRefreshResources forcefully refreshes GKE and GCE resources
func (m *gkeManagerImpl) forceRefreshResources() error {
	if err := m.refreshGkeResources(); err != nil {
		klog.Errorf("Failed to refresh GKE cluster resources: %v", err)
		return err
	}
	if err := m.refreshMachinesCache(); err != nil {
		klog.Errorf("Failed to fetch machine types: %v", err)
		return err
	}
	m.lastRefresh = time.Now()
	klog.V(2).Infof("Refreshed GCE resources, next refresh after %v", m.lastRefresh.Add(ClusterRefreshInterval))
	return nil
}

// refreshGkeResources refreshes GKE resources only
func (m *gkeManagerImpl) refreshGkeResources() error {
	cluster, err := m.gkeService.GetCluster()
	if err != nil {
		return err
	}
	maxPodsPerNode := gkelabels.DefaultMaxPodsPerNode
	if cluster.DefaultMaxPodsConstraint != nil && !m.managerOptions.AutopilotEnabled {
		maxPodsPerNode = cluster.DefaultMaxPodsConstraint.MaxPodsPerNode
	}
	m.releaseChannel = cluster.ReleaseChannel
	if changed := m.autoprovisioningEligibility.SetClusterAutoprovisioningEnabled(cluster.NodeAutoprovisioningEnabled); changed {
		klog.Infof("Cluster NAP enabled flag was updated to %v", cluster.NodeAutoprovisioningEnabled)
	}
	m.clusterCreateTime = cluster.CreateTime
	m.clusterVersion = cluster.ClusterVersion
	m.emulatedClusterVersion = cluster.EmulatedClusterVersion
	m.autoprovisioningNodePoolDefaults = cluster.AutoprovisioningNodePoolDefaults
	m.nodePoolDefaults = cluster.NodePoolDefaults
	m.confidentialNodesEnabled = cluster.ConfidentialNodesEnabled
	m.confidentialInstanceType = cluster.ConfidentialInstanceType
	m.isClusterUsingPSCInfrastructure = cluster.IsClusterUsingPSCInfrastructure
	m.defaultEnablePrivateNodes = cluster.EnablePrivateNodes
	m.dataplaneV2Enabled = cluster.DataplaneV2Enabled
	m.networkPath = cluster.NetworkPath
	m.subnetworkPath = cluster.SubnetworkPath
	m.subnetwork = cluster.Subnetwork
	m.NewNodePoolDaemonSetConditions = &DaemonSetConditions{
		NetdEnabled:                  cluster.NetdEnabled(),
		NodeLocalDNSEnabled:          cluster.NodeLocalDNSEnabled,
		MetadataServerEnabled:        cluster.MetadataServerEnabled(),
		HighThroughputLoggingEnabled: cluster.HighThroughputLoggingEnabled,
		IpMasqAgentEnabled:           m.managerOptions.AutopilotEnabled && cluster.DataplaneV2Enabled,
	}
	m.defaultMaxPodsPerNode.Store(maxPodsPerNode)
	m.isDefaultCCCEnabled.Store(cluster.DefaultCCCEnabled)

	m.refreshNodePools(cluster.NodePools, cluster.AllNodePoolNames)
	m.refreshResourceLimiter(cluster.ResourceLimiter)
	if m.IsNodeAutoprovisioningEnabled() {
		m.refreshAutoprovisioningLocations(&cluster)
	}

	// emit a metric specifying if AutoprovisioningNodePoolDefaults.MinCpuPlatform is in use
	napDefaultsMinCpuPlatform := m.GetDefaultNodePoolMinCpuPlatform()
	gke_metrics.Metrics.UpdateNapDefaultsMinCpuPlatformEnabled(napDefaultsMinCpuPlatform)
	m.gkeMetrics.UpdateNapEnabled(cluster.NodeAutoprovisioningEnabled)

	if m.optsTracker != nil {
		m.optsTracker.ExperimentsManager().UpdateReleaseChannel(m.releaseChannel)
		m.optsTracker.RecomputeOptions(cluster)
		m.gkeMetrics.UpdateCSNEnabled(m.optsTracker.Options().CSNEnabled)
	}

	return nil
}

func (m *gkeManagerImpl) refreshResourceLimiter(resourceLimiter *cloudprovider.ResourceLimiter) {
	if m.AreClusterLimitsEnabled() {
		if resourceLimiter != nil {
			klog.V(2).Infof("Refreshed resource limits: %s", resourceLimiter.String())
			m.cache.SetResourceLimiter(resourceLimiter)
		} else if oldLimits, _ := m.cache.GetResourceLimiter(); oldLimits != nil {
			klog.Errorf("Resource limits should always be defined in NAP mode, but they appear to be empty. Using possibly outdated limits: %v", oldLimits.String())
		} else {
			klog.Errorf("Resource limits should always be defined in NAP mode, but they appear to be empty, and there aren't any previous limits.")
		}
	} else {
		m.cache.SetResourceLimiter(nil)
	}
}

func (m *gkeManagerImpl) refreshAutoprovisioningLocations(cluster *gkeclient.Cluster) {
	var autoprovisioningLocations []string
	if cluster.AutoprovisioningLocations != nil {
		klog.V(2).Infof("Refreshed autoprovisioning locations %v using autoprovisioning locations field", cluster.AutoprovisioningLocations)
		autoprovisioningLocations = cluster.AutoprovisioningLocations
	} else {
		klog.V(2).Infof("Refreshed autoprovisioning locations %v", cluster.Locations)
		autoprovisioningLocations = cluster.Locations
	}
	m.gkeConfigurationCache.setAutoprovisioningLocations(autoprovisioningLocations)
	if m.clusterLocationsObserver != nil {
		if m.resizableVmAutoprovisioningProvider.HasActiveResizableNodes() {
			// Updates locations, closes old streams, and connects new ones.
			m.clusterLocationsObserver.SetLocations(autoprovisioningLocations)
		} else {
			// Closes all UAS recommendation streams.
			m.clusterLocationsObserver.SetLocations(nil)
		}
	}
}

func (m *gkeManagerImpl) refreshShortLivedUpgradeInProgress() {
	loggingQuota := logging.NodeGroupLoggingQuota()
	for _, mig := range m.migLister.GetGkeMigs() {
		if !mig.QueuedProvisioning() && !mig.FlexStartNonQueued() {
			continue
		}
		mig.shortLivedUpgradeInProgress = false

		migRef := mig.GceRef()
		migInstanceTemplate, err := m.migInfoProvider.GetMigInstanceTemplateName(mig.GceRef())
		if err != nil {
			klog.Errorf("[refreshShortLivedUpgradeInProgress] GetMigInstanceTemplate failed for %v with err=%v", mig.GceRef(), err)
			continue
		}

		instances, err := m.migInfoProvider.GetMigInstances(migRef)
		if err != nil {
			klog.Errorf("[refreshShortLivedUpgradeInProgress] GetMigInstances failed for %v with err=%v", migRef, err)
			continue
		}
		for _, instance := range instances {
			if instance.InstanceTemplateName != "" && instance.InstanceTemplateName != migInstanceTemplate.Name {
				klogx.V(1).UpTo(loggingQuota).Infof("MIG %v has ShortLived upgrade in progress - MIG has instance template %q, but there's instance with ID %s with different instance template %q", migRef, migInstanceTemplate.Name, instance.Id, instance.InstanceTemplateName)
				mig.shortLivedUpgradeInProgress = true
				break
			}
		}
	}
	klogx.V(1).Over(loggingQuota).Infof("There are also %d other MIGs having ShortLived upgrade in progress", -loggingQuota.Left())
}

func (m *gkeManagerImpl) refreshEkEdpEnabled() {
	var minGkeVersion = m.optsTracker.ExperimentsManager().EvaluateStringFlagOrFailsafe(experiments.EnableEkEdpMinGKEVersionFlag, "999.999.999-gke.0")

	v, err := version.FromString(m.clusterVersion)
	if err != nil {
		m.ekEdpEnabledCache = false
		klog.Errorf("Failed to parse cluster version %q: %v", m.clusterVersion, err)
		return
	}
	vMin, err := version.FromString(minGkeVersion)
	if err != nil {
		m.ekEdpEnabledCache = false
		klog.Errorf("Failed to parse min GKE version %q: %v", minGkeVersion, err)
		return
	}
	m.ekEdpEnabledCache = !v.LessThan(vMin)
}

func (m *gkeManagerImpl) RefreshLocalSSDSizes() {
	m.localSSDDiskSizeProvider.UpdateDiskSizes(m.machineConfigProvider.LocalSSDDiskSizes())
}

func (m *gkeManagerImpl) initializeOnce() error {
	multiError := utils.NewMultiErr(len(m.initializationFuncs))
	m.initializationOnce.Do(func() {
		for _, initFunc := range m.initializationFuncs {
			if err := initFunc(); err != nil {
				multiError.Append(err)
			}
		}
	})
	return multiError.ErrorOrNil()
}

// RegisterInitializationFunc registers an initialization func, which would be called once
// after the first successful Refresh of the cluster state.
func (m *gkeManagerImpl) RegisterInitializationFunc(f InitializationFunc) {
	m.initializationFuncs = append(m.initializationFuncs, f)
}

func (m *gkeManagerImpl) GetClusterCreateTime() time.Time {
	if m.clusterCreateTime.IsZero() {
		klog.Error("ClusterCreateTime is not set and should be. This could lead to un-intended scale up at cluster creation")
	}
	return m.clusterCreateTime
}

// ClusterStarted lets you know if the cluster started
func (m *gkeManagerImpl) ClusterStarted() (bool, error) {
	if m.clusterStarted {
		return m.clusterStarted, nil
	}
	cluster, err := m.gkeService.GetCluster()
	if err != nil {
		return false, err
	}
	if cluster.Status != "PROVISIONING" && cluster.Status != "STATUS_UNSPECIFIED" {
		m.clusterStarted = true
	}
	return m.clusterStarted, nil
}

// GetResourceLimiter returns resource limiter from cache.
func (m *gkeManagerImpl) GetResourceLimiter(n NodeGroupFromNode) (*cloudprovider.ResourceLimiter, error) {
	resourceLimiter, err := m.cache.GetResourceLimiter()
	if err != nil {
		klog.Errorf("error getting resourceLimiter; %v", err)
		return nil, err
	}
	if m.surgeUpgradeResourceTracker == nil {
		return resourceLimiter, nil
	}
	resourcesConsumedInSurgeUpgrade, err := m.surgeUpgradeResourceTracker.GetSurgeResources(n)
	if err != nil {
		klog.Errorf("error calculating resources consumed during surge upgrade; %v", err)
		return nil, err
	}
	return gkeutil.RightShiftTransformResourceLimiter(resourceLimiter, resourcesConsumedInSurgeUpgrade), nil
}

// GetAutoprovisioningDefaultFamily returns the default machine family used for autoprovisioned node pools.
func (m *gkeManagerImpl) GetAutoprovisioningDefaultFamily() machinetypes.MachineFamily {
	return m.napDefaultMachineTypeFamily
}

// Return information about EK launch phase and source and if it is disabled by Cluster Proto

// ResizeRequests returns all Resize Requests for a given node group.
func (m *gkeManagerImpl) ResizeRequests(mig *GkeMig) ([]resizerequestclient.ResizeRequestStatus, error) {
	switch {
	case mig.QueuedProvisioning():
		return m.provisioningRequestManager.ResizeRequests(mig.GceRef())
	case mig.FlexStartNonQueued():
		return m.flexResizeRequestService.ResizeRequests(context.Background(), mig.GceRef())
	default:
		return m.atomicResizeRequestService.ResizeRequests(context.Background(), mig.GceRef())
	}
}

func (m *gkeManagerImpl) IsDefaultCCCEnabled() bool {
	return m.isDefaultCCCEnabled.Load()
}

func cacheTTLForMig(mig *GkeMig) time.Duration {
	if multinetworkingConfiguredForMig(mig) {
		// Cache node templates for Migs with multi-networking configured
		// for a short period of time only, as such node templates have dynamic properties.
		return nodetemplate.ShortTTL
	}
	return nodetemplate.LongTTL
}

func (m *gkeManagerImpl) prepareNodeBeforeAddingToCache(node *apiv1.Node) {
	if node.Labels == nil {
		node.Labels = make(map[string]string)
	}

	gkelabels.UpdateDeprecatedLabels(node.Labels)
}

func (m *gkeManagerImpl) fetchExistingMigTemplateNode(mig *GkeMig) (*apiv1.Node, error) {
	template, err := m.GetMigInstanceTemplate(mig)
	if err != nil {
		return nil, err
	}

	cacheKey := nodetemplate.BuildKeyForCA(template.Id)
	if node, exists := m.cache.nodeTemplateCache.Get(cacheKey); exists {
		klog.V(5).Infof("Node template cache hit for MIG: %s, template: %v", mig.Id(), template.Id)
		return node, nil
	}

	node, err := m.nodeTemplateFromInstanceTemplate(mig, template)
	if err != nil {
		return nil, err
	}

	m.prepareNodeBeforeAddingToCache(node)

	m.cache.nodeTemplateCache.Add(cacheKey, node, cacheTTLForMig(mig))

	return node, nil
}

func (m *gkeManagerImpl) fetchAutoprovisionedMigTemplateNode(mig *GkeMig) (*apiv1.Node, error) {
	cacheKey := ""

	if m.clusterVersion != "" {
		cacheKey = nodetemplate.BuildKeyForNAP(mig.Spec(), string(m.GetOsDistributionForNap(mig)), m.clusterVersion, mig.gceRef.Zone)
		if node, exists := m.cache.nodeTemplateCache.Get(cacheKey); exists {
			klog.V(5).Infof("Node template cache hit for autoprovisioned MIG: %s", mig.Id())
			return node, nil
		}
	} else {
		klog.Warning("Unable to determine cluster version, instance templates are not cached")
	}

	node, err := m.nodeTemplateFromMigSpec(mig)
	if err != nil {
		return nil, err
	}

	m.prepareNodeBeforeAddingToCache(node)

	if cacheKey != "" {
		m.cache.nodeTemplateCache.Add(cacheKey, node, cacheTTLForMig(mig))
	}

	return node, nil
}

// GetMigTemplateNodeInfo constructs a NodeInfo with static Pods (DS Pods are not included):
// - from GCE instance template of the given MIG, if the MIG already exists,
// - from MIG spec, if it doesn't exist, but may be autoprovisioned.
// WARNING: Returned NodeInfo can be shared among multiple goroutines and shouldn't be modified.
// Concurrent modification can lead to data races and panics.
func (m *gkeManagerImpl) GetMigTemplateNodeInfo(mig *GkeMig) (*framework.NodeInfo, error) {
	var node *apiv1.Node
	var err error
	if mig.Exist() {
		node, err = m.fetchExistingMigTemplateNode(mig)
	} else if mig.Autoprovisioned() {
		node, err = m.fetchAutoprovisionedMigTemplateNode(mig)
	} else {
		return nil, fmt.Errorf("unable to get node info for %s", mig.GceRef().String())
	}
	if err != nil {
		return nil, err
	}

	resourceSlices, err := m.draResourcePredictor.ResourceSlicesForNode(mig.spec, node)
	if err != nil {
		return nil, err
	}

	nodeInfo := framework.NewNodeInfo(node, resourceSlices)
	if !m.IsDataplaneV2Enabled() {
		nodeInfo.AddPod(framework.NewPodInfo(cloudprovider.BuildKubeProxy(mig.Id()), nil))
	}

	return nodeInfo, nil
}

func (m *gkeManagerImpl) nodeTemplateFromInstanceTemplate(mig *GkeMig, template *gce_api.InstanceTemplate) (*apiv1.Node, error) {
	kubeEnv, err := m.GetMigKubeEnv(mig)
	if err != nil {
		return nil, err
	}

	gceMachineType, err := m.migInfoProvider.GetMigMachineType(mig.GceRef())
	if err != nil {
		return nil, err
	}
	gkeMachineType, err := m.machineConfigProvider.ToMachineType(template.Properties.MachineType)
	if err != nil {
		klog.Errorf("couldn't convert template machine type %s to GKE machine type, falling back to gceMachineType.Name %s", template.Properties.MachineType, gceMachineType.Name)
		gkeMachineType, err = m.machineConfigProvider.ToMachineType(gceMachineType.Name)
		if err != nil {
			klog.Errorf("couldn't convert GCE machine type %s to GKE machine type, falling back to UnknownMachineType placeholder", gceMachineType.Name)
			gkeMachineType = machinetypes.UnknownMachineType
		}
	}
	threadsPerCore := gkeMachineType.GetThreadsPerCore()
	if template.Properties.AdvancedMachineFeatures != nil && template.Properties.AdvancedMachineFeatures.ThreadsPerCore > 0 {
		threadsPerCore = template.Properties.AdvancedMachineFeatures.ThreadsPerCore
	}
	cpu, err := getMachineTypeCpu(gkeMachineType, gceMachineType, threadsPerCore)
	if err != nil {
		return nil, err
	}
	pods := getMaxPodsForNodeForTemplate(mig, m.managerOptions.AutopilotEnabled, m.managerOptions.AutopilotHigherMaxPodsPerNode, gceMachineType.CPU)
	gceMigOsInfo, err := m.templates.MigOsInfo(mig.Id(), kubeEnv)
	if err != nil {
		return nil, err
	}
	gkeMigOsInfo := NewGkeMigOsInfo(gceMigOsInfo, mig.Version(), mig.IsConfidentialNode())
	node, err := m.templates.BuildNodeFromTemplate(mig, gkeMigOsInfo, template, kubeEnv, cpu, gceMachineType.Memory, pods, m.reserved, m.localSSDDiskSizeProvider)
	if err != nil {
		return node, err
	}

	if !dynamicresources.GpuDraDriverEnabled(node) {
		node, err = m.addGpuPartitioningCapacity(node)
		if err != nil {
			return node, err
		}
	} else {
		// GPU capacity and allocatable are injected by BuildNodeFromTemplate
		// while in case of DRA driver enablement - those values are supposed
		// to be missing. Per driver enablement of DRA is GKE-specific
		// mechanism which can't be easily open-sourced at the moment.
		//
		// Context: go/dra-enablement-on-gke
		clearGpuCapacityInplace(node)
	}

	if !dynamicresources.TpuDraDriverEnabled(node) {
		node, err = m.addTpuCapacity(node, gceMachineType.Name)
		if err != nil {
			return node, err
		}
	}

	node, err = addPodBucketingCapacity(node)
	if err != nil {
		return node, err
	}

	node = addHugepagesCapacity(node, mig)

	if err := m.addSupportedDiskTypeLabelsToNode(node, mig); err != nil {
		return node, err
	}

	// Add an annotation to easily distinguish nodeInfos generated from real nodes from the ones generated from templates.
	addAnnotation(node, gkelabels.NodeGeneratedFromTemplateAnnotation, "true")

	if m.managerOptions.MultiNetworkSupportEnabled && m.matcher != nil {
		node, err = addMultiNetworkCapacity(node, mig, m.matcher)
		if err != nil {
			return node, err
		}
	}

	return node, nil
}

func (m *gkeManagerImpl) nodeTemplateFromMigSpec(mig *GkeMig) (*apiv1.Node, error) {
	machineTypeStr := mig.Spec().MachineType
	gceMachineType, err := m.GetMachineType(machineTypeStr, mig.GceRef().Zone)
	if err != nil {
		return nil, err
	}
	gkeMachineType, err := m.machineConfigProvider.ToMachineType(gceMachineType.Name)
	if err != nil {
		return nil, err
	}
	cpu, err := getMachineTypeCpu(gkeMachineType, gceMachineType, 0)
	if err != nil {
		return nil, err
	}
	pods := getMaxPodsForNodeForTemplate(mig, m.managerOptions.AutopilotEnabled, m.managerOptions.AutopilotHigherMaxPodsPerNode, gceMachineType.CPU)
	// TODO: Operating system will need to changed if we ever want NAP for windows.
	gkeMigOsInfo := NewGkeMigOsInfo(gce.NewMigOsInfo(gce.OperatingSystemLinux, m.GetOsDistributionForNap(mig), mig.GetSystemArchitecture()), mig.Version(), mig.IsConfidentialNode())
	defaultMaxPodsPerNode := m.defaultMaxPodsPerNode.Load()
	node, err := m.templates.BuildNodeFromMigSpec(mig, gkeMigOsInfo, cpu, gceMachineType.Memory, pods, m.GetNewNodePoolDaemonSetConditions(), m.isGCFSEnabled(), m.reserved, m.localSSDDiskSizeProvider, defaultMaxPodsPerNode)
	if err != nil {
		return nil, err
	}

	// Add an annotation to easily distinguish nodeInfos generated from real nodes from the ones generated from templates.
	addAnnotation(node, gkelabels.NodeGeneratedFromTemplateAnnotation, "true")

	return node, nil
}

// addSupportedDiskTypeLabelsToNode adds supported disk type labels to a node based on its machine family.
func (m *gkeManagerImpl) addSupportedDiskTypeLabelsToNode(node *apiv1.Node, mig *GkeMig) error {
	machineFamily, err := mig.MachineConfigProvider().GetMachineFamilyFromMachineName(mig.Spec().MachineType)
	if err != nil {
		klog.Warningf("Failed to get disk type labels for mig_name=[%s], machine_type=[%s], error=[%v]", mig.Id(), mig.Spec().MachineType, err)
		return nil
	}

	for _, diskType := range machineFamily.ListSupportedDisks(mig.IsConfidentialNode()) {
		node.Labels[gkelabels.SupportedDiskTypeKey(diskType)] = "true"
	}
	return nil
}

func (m *gkeManagerImpl) isGCFSEnabled() bool {
	defaults := m.nodePoolDefaults
	return defaults != nil &&
		defaults.NodeConfigDefaults != nil &&
		defaults.NodeConfigDefaults.GcfsConfig != nil &&
		defaults.NodeConfigDefaults.GcfsConfig.Enabled
}

func (m *gkeManagerImpl) addGpuPartitioningCapacity(node *apiv1.Node) (*apiv1.Node, error) {
	gpuName, gpuTypeFound := node.Labels[gkelabels.GPULabel]
	gpuPartitionSize, gpuPartitionSizeFound := node.Labels[gkelabels.GPUPartitionSizeLabel]
	gpuMaxSharedClients, gpuMaxSharedClientsFound := node.Labels[gkelabels.GPUMaxSharedClientsLabel]
	if !gpuTypeFound || !(gpuPartitionSizeFound || gpuMaxSharedClientsFound) {
		return node, nil
	}
	gpuCapacity := node.Status.Capacity[gpu.ResourceNvidiaGPU]
	gpuAllocatable := node.Status.Allocatable[gpu.ResourceNvidiaGPU]
	gpuType, ok := m.machineConfigProvider.ToGpuType(gpuName)
	if !ok {
		return nil, caerrors.NewAutoscalerErrorf(caerrors.InternalError, "provided GPU is not supported: %s", gpuName)
	}
	partitionCount, err := gpuType.GetPartitionCount(gpuPartitionSize)
	if err != nil {
		return nil, err
	}
	maxSharedClients, err := machinetypes.GetMaxGpuSharedClients(gpuMaxSharedClients)
	if err != nil {
		return nil, err
	}
	additionalGpu := resource.NewQuantity((partitionCount*maxSharedClients-1)*gpuCapacity.Value(), resource.DecimalSI)
	gpuCapacity.Add(*additionalGpu)
	gpuAllocatable.Add(*additionalGpu)

	node.Status.Capacity[gpu.ResourceNvidiaGPU] = gpuCapacity
	node.Status.Allocatable[gpu.ResourceNvidiaGPU] = gpuAllocatable
	return node, nil
}

func clearGpuCapacityInplace(node *apiv1.Node) {
	delete(node.Status.Capacity, gpu.ResourceNvidiaGPU)
	delete(node.Status.Allocatable, gpu.ResourceNvidiaGPU)
}

func (m *gkeManagerImpl) addTpuCapacity(node *apiv1.Node, machineType string) (*apiv1.Node, error) {
	_, tpuTypeFound := node.Labels[gkelabels.TPULabel]
	if !tpuTypeFound {
		return node, nil
	}
	tpuCount, err := m.machineConfigProvider.GetTpuCountForMachineType(machineType)
	if err != nil {
		return nil, err
	}
	tpuQuantity := resource.NewQuantity(tpuCount, resource.DecimalSI)
	node.Status.Capacity[tpu.ResourceGoogleTPU] = *tpuQuantity
	node.Status.Allocatable[tpu.ResourceGoogleTPU] = *tpuQuantity
	return node, nil
}

func multinetworkingConfiguredForMig(mig *GkeMig) bool {
	return mig.Spec() != nil && len(mig.Spec().NetworkConfigs) >= 1
}

func addMultiNetworkCapacity(node *apiv1.Node, mig *GkeMig, matcher networking.Matcher) (*apiv1.Node, error) {
	if !multinetworkingConfiguredForMig(mig) {
		return node, nil
	}
	resources, err := matcher.GetNetworkingResourcesFromNetworkConfig(mig.spec.NetworkConfigs)
	if err != nil {
		return nil, err
	}
	for resourceName, resourceVal := range resources {
		node.Status.Capacity[apiv1.ResourceName(resourceName)] = resourceVal
		node.Status.Allocatable[apiv1.ResourceName(resourceName)] = resourceVal
	}
	return node, nil
}

func addPodBucketingCapacity(node *apiv1.Node) (*apiv1.Node, error) {
	podCapacity := node.Labels[gkelabels.PodCapacityLabel]
	if podCapacity == "" {
		return node, nil
	}
	capacity, err := resource.ParseQuantity(podCapacity)
	if err != nil {
		return node, fmt.Errorf("failed to parse quanity %q: %w", podCapacity, err)
	}
	node.Status.Capacity[gkelabels.PodCapacityLabel] = capacity
	node.Status.Allocatable[gkelabels.PodCapacityLabel] = capacity
	return node, nil
}

func addHugepagesCapacity(node *apiv1.Node, mig *GkeMig) *apiv1.Node {
	// For every huge page reservation, we need to remove it from allocatable memory:
	// https://github.com/kubernetes/kubernetes/blob/84cacae7046df93c1f6f8ea97c912d948e1ad06a/pkg/kubelet/nodestatus/setters.go#L323
	if hugepage1g := mig.GetHugepageSize1gBytes(); hugepage1g > 0 {
		node.Status.Capacity[HugepageSize1gResourceName] = *resource.NewQuantity(hugepage1g, resource.DecimalSI)
		node.Status.Allocatable[HugepageSize1gResourceName] = *resource.NewQuantity(hugepage1g, resource.DecimalSI)
		node.Status.Allocatable[apiv1.ResourceMemory] = subtract(node.Status.Allocatable[apiv1.ResourceMemory], node.Status.Capacity[HugepageSize1gResourceName])
	}
	if hugepage2m := mig.GetHugepageSize2mBytes(); hugepage2m > 0 {
		node.Status.Capacity[HugepageSize2mResourceName] = *resource.NewQuantity(hugepage2m, resource.DecimalSI)
		node.Status.Allocatable[HugepageSize2mResourceName] = *resource.NewQuantity(hugepage2m, resource.DecimalSI)
		node.Status.Allocatable[apiv1.ResourceMemory] = subtract(node.Status.Allocatable[apiv1.ResourceMemory], node.Status.Capacity[HugepageSize2mResourceName])
	}
	return node
}

func subtract(a, b resource.Quantity) resource.Quantity {
	value := a.DeepCopy()
	pValue := &value
	pValue.Sub(b)
	if pValue.Sign() < 0 {
		// Negative Allocatable resources don't make sense.
		pValue.Set(0)
	}
	return *pValue
}

func (m *gkeManagerImpl) GetMachineType(machineName string, zone string) (gce.MachineType, error) {
	if gce.IsCustomMachine(machineName) {
		return gce.NewCustomMachineType(machineName)
	}

	// Check the error cache first to avoid GCE API call spam if the machine type is known to be malformed, non-existent, or unavailable in the zone.
	if err, found := m.cache.GetMachineTypeError(machineName, zone); found {
		klog.V(5).Infof("GetMachineType: negative cache hit for machine type %q in zone %q; returning cached error: %v", machineName, zone, err)
		return gce.MachineType{}, err
	}

	// Cache hit: return the machine type immediately.
	if machine, found := m.cache.GetMachine(machineName, zone); found {
		return machine, nil
	}

	// Cache miss: fetch from GCE API.
	rawMachine, err := m.gceService.FetchMachineType(zone, machineName)
	if err != nil {
		// Cache the error (if cacheable) to prevent GCE API spam on subsequent calls.
		if ttl, cached := m.cache.AddMachineTypeError(machineName, zone, err); cached {
			klog.V(2).Infof("GetMachineType: GCE API returned error for machine type %q in zone %q: %v. Caching this error for %v to prevent API spam.", machineName, zone, err, ttl)
		}
		return gce.MachineType{}, err
	}

	machine, err := gce.NewMachineTypeFromAPI(machineName, rawMachine)
	if err != nil {
		return gce.MachineType{}, err
	}

	m.cache.AddMachine(machine, zone)
	return machine, nil
}

func (m *gkeManagerImpl) IsDataplaneV2Enabled() bool {
	return m.dataplaneV2Enabled
}

func toLocationPolicyEnum(s string) LocationPolicyEnum {
	switch s {
	case "ANY":
		return LocationPolicyAny
	case "BALANCED":
		return LocationPolicyBalanced
	default:
		// The default value is set to BALANCED to trigger the GKE Fungibility logic
		// in case the control plane defaulting fails to set the location policy.
		return LocationPolicyBalanced
	}
}

func getMachineTypeCpu(gkeMachineType machinetypes.MachineType, gceMachineType gce.MachineType, templateThreadsPerCore int64) (int64, error) {
	baseCpu := gceMachineType.CPU
	machineThreadsPerCore := gkeMachineType.GetThreadsPerCore()
	if machineThreadsPerCore == 1 {
		// Some machine types like ct6e-standard-8t have a non-default SMT=1 configuration in GKE.
		// GCE vCPU count assumes SMT=2, so it can't be used for those machines.
		if gkeMachineType.CPU == 0 {
			return 0, fmt.Errorf("SHOULD NEVER HAPPEN: machine type %s with %d cpus reported non-default threads per core %d", gkeMachineType.Name, gkeMachineType.CPU, machineThreadsPerCore)
		}
		baseCpu = gkeMachineType.CPU
	}
	if templateThreadsPerCore == 0 || templateThreadsPerCore == machineThreadsPerCore {
		return baseCpu, nil
	}
	threads := baseCpu * templateThreadsPerCore
	if threads%machineThreadsPerCore != 0 {
		if threads > machineThreadsPerCore {
			return 0, fmt.Errorf("Machine type %s has inconsistent number of guestCpu and threadsPerCore: (%d, %d))", gceMachineType.Name, baseCpu, templateThreadsPerCore)
		}
		// Less threads than VM type would suggest. Don't scale them
		// down: this can happen with 1-core VMs using gVisor.
		return baseCpu, nil
	}
	return threads / machineThreadsPerCore, nil
}

// getMaxPodsForNodeForTemplate is used to set the max pods per node when it's
// not a gke standard default value. However, these values are only used for simulation
// and the properties of the nodepool is being set in the control plane.
// Changing this will not affect the max pods per node in a newly created nodepool.
func getMaxPodsForNodeForTemplate(mig *GkeMig, autopilotEnabled bool, autopilotHigherMaxPodsPerNode bool, cpuCount int64) *int64 {
	var pods int64 = 0
	if mig.spec != nil && mig.spec.MaxPodsPerNode != 0 {
		return &mig.spec.MaxPodsPerNode
	}
	if autopilotEnabled {
		if autopilotHigherMaxPodsPerNode {
			pods = getAutopilotHigherMaxPodsPerNodePerCpuCount(cpuCount)
		} else {
			pods = autopilotMaxPodsPerNode
		}
		return &pods
	}
	return nil
}

func getAutopilotHigherMaxPodsPerNodePerCpuCount(cpuCount int64) int64 {
	if cpuCount <= 4 {
		return 32
	} else if cpuCount <= 8 {
		return 64
	} else {
		return 128
	}
}

func filterOutSystemTaints(taints []apiv1.Taint) []apiv1.Taint {
	var nodeTaints []apiv1.Taint
	for _, taint := range taints {
		if taint.Key == gpu.ResourceNvidiaGPU || taint.Key == sandbox.RuntimeTaintKey || taint.Key == tpu.ResourceGoogleTPU {
			continue
		}
		nodeTaints = append(nodeTaints, taint)
	}
	return nodeTaints
}

// filterOutExternalSystemLabels filters out system labels, apart from those which
// are added by Cluster Autoscaler or allow listed to be passed to GKE Control Plane.
func filterOutExternalSystemLabels(labels map[string]string, matcher *gkelabels.Matcher, options GkeManagerOptions) map[string]string {
	result := make(map[string]string)
	for k, v := range labels {
		if !gkelabels.IsSystemLabel(k) ||
			gkelabels.IsAddedByClusterAutoscaler(k, options.bootDiskConfigEnabled) ||
			matcher.Match(k) {
			result[k] = v
		}
	}
	return result
}

func (m *gkeManagerImpl) CleanUpBrokenNodePool(name string) {
	deleteErr := m.gkeService.DeleteNodePool(name)
	if deleteErr != nil {
		klog.Errorf("Error during node pool deletion, node pool: %s, err: %v", name, deleteErr)
	}
}

func (m *gkeManagerImpl) ResizeVm(ctx context.Context, instance gce.GceRef, desiredSize ekvmsize.VmSize) error {
	return m.resizableVmService.ResizeVm(ctx, instance, desiredSize.MilliCpus, desiredSize.KBytes)
}

func (m *gkeManagerImpl) GetCurrentResizableVmState(machineConfigProvider *machinetypes.MachineConfigProvider, instance gce.GceRef) (ekvmtypes.ResizableVmState, error) {
	return m.resizableVmService.GetCurrentResizableVmState(machineConfigProvider, instance)
}

func (m *gkeManagerImpl) BulkFetchCurrentResizableVmStates(machineConfigProvider *machinetypes.MachineConfigProvider) (map[gce.GceRef]ekvmtypes.ResizableVmState, error) {
	return m.resizableVmService.BulkFetchCurrentResizableVmStates(machineConfigProvider, m.projectId, m.clusterName)
}

func (m *gkeManagerImpl) GetMaxNodeProvisioningTimeOverride(mig *GkeMig) (time.Duration, bool) {
	if !mig.IsSingleHostTpuMig() || mig.QueuedProvisioning() {
		return time.Duration(0), false
	}
	fallbackValue := tpuMigMaxNodeProvisionTime
	if mig.FlexStartNonQueued() {
		fallbackValue = DefaultFlexStartCapacityCheckWaitTime + maxNodeProvisionTimeOffset
	}
	return m.optsTracker.ExperimentsManager().EvaluateDurationSecondsFlagOrFailsafe(experiments.NodeProvisionTimeSingleHostTPU, fallbackValue), true
}

// CapacityCheckWaitTimeSeconds returns capacityCheckWaitTimeSeconds based on default experiment values and custom capacityCheckWaitTimeSeconds override
// or returns error in case the feature is not supported.
func (m *gkeManagerImpl) CapacityCheckWaitTimeSeconds(mig *GkeMig) (time.Duration, error) {
	return m.cache.CapacityCheckWaitTimeSeconds(mig.gceRef)
}

// EvaluateCapacityCheckWaitTimeSeconds returns capacityCheckWaitTimeSeconds based on default experiment values and custom capacityCheckWaitTimeSeconds override
// or returns error in case the feature is not supported.
func (m *gkeManagerImpl) EvaluateCapacityCheckWaitTimeSeconds(mig *GkeMig) (time.Duration, error) {
	if mig.IsSingleHostTpuMig() {
		return time.Duration(0), fmt.Errorf("CapacityCheckWaitTimeSeconds not supported for single host TPU mig %v", mig.gceRef)
	}
	ccwtTpuSupportEnabled := m.optsTracker.ExperimentsManager().EvaluateMinimumVersionFlagOrFailsafe(experiments.CapacityCheckWaitTimeSecondsMultiHostTpuEnabledFlag, false)
	if !mig.FlexStartNonQueued() && !(mig.IsMultiHostTpuMig() && ccwtTpuSupportEnabled) {
		return time.Duration(0), fmt.Errorf("CapacityCheckWaitTimeSeconds not supported for non Flex Start mig %v", mig.gceRef)
	}

	defaultTime, err := m.fetchDefaultCheckCapacityWaitTime(mig)
	if err != nil {
		return time.Duration(0), err
	}

	capacityCheckWaitTimeSecondsEnabled := m.customCapacityCheckWaitTimeSecondsEnabled(mig)
	if !capacityCheckWaitTimeSecondsEnabled || mig.Spec() == nil || mig.Spec().Labels == nil {
		return defaultTime, nil
	}
	customTime, ok := capacityCheckWaitTimeSecondsLabelValue(mig.Spec().Labels, defaultTime)
	if !ok {
		return defaultTime, nil
	}
	return customTime, nil
}

func (m *gkeManagerImpl) fetchDefaultCheckCapacityWaitTime(mig *GkeMig) (time.Duration, error) {
	switch {
	case mig.FlexStartNonQueued() && !mig.IsTpuMig():
		return m.optsTracker.ExperimentsManager().EvaluateDurationSecondsFlagOrFailsafe(experiments.CapacityCheckWaitTimeSecondsDefaultValueGpuFlag, DefaultFlexStartCapacityCheckWaitTime), nil
	case mig.FlexStartNonQueued() && mig.IsMultiHostTpuMig():
		return m.optsTracker.ExperimentsManager().EvaluateDurationSecondsFlagOrFailsafe(experiments.CapacityCheckWaitTimeSecondsFlexValueMultiHostTpuFlag, DefaultFlexStartCapacityCheckWaitTime), nil
	case mig.IsMultiHostTpuMig():
		return m.optsTracker.ExperimentsManager().EvaluateDurationSecondsFlagOrFailsafe(experiments.CapacityCheckWaitTimeSecondsNonFlexValueMultiHostTpuFlag, tpuMigMaxNodeProvisionTime-maxNodeProvisionTimeOffset), nil
	default:
		return time.Duration(0), fmt.Errorf("CapacityCheckWaitTimeSeconds not supported for non Flex Start mig %v", mig.gceRef)
	}
}

func (m *gkeManagerImpl) customCapacityCheckWaitTimeSecondsEnabled(mig *GkeMig) bool {
	switch {
	case mig.FlexStartNonQueued() && !mig.IsSingleHostTpuMig():
		return m.optsTracker.ExperimentsManager().EvaluateMinimumVersionFlagOrFailsafe(experiments.CapacityCheckWaitTimeSecondsFlexStartEnabledFlag, false)
	case mig.IsMultiHostTpuMig():
		return m.optsTracker.ExperimentsManager().EvaluateMinimumVersionFlagOrFailsafe(experiments.CapacityCheckWaitTimeSecondsMultiHostTpuEnabledFlag, false)
	default:
		return false
	}
}

func capacityCheckWaitTimeSecondsLabelValue(labels map[string]string, minDefaultTime time.Duration) (time.Duration, bool) {
	val, found := labels[gkelabels.CapacityCheckWaitTimeSecondsLabel]
	if !found {
		return time.Duration(0), false
	}

	ccwt, err := strconv.ParseInt(val, 10, 64)
	if err != nil || ccwt < 1 {
		klog.Warningf("CapacityCheckWaitTimeSeconds label %q value %q is not valid", gkelabels.CapacityCheckWaitTimeSecondsLabel, val)
		return time.Duration(0), false
	}

	capacityCheckWaitTimeSeconds := time.Duration(ccwt) * time.Second
	if capacityCheckWaitTimeSeconds < minDefaultTime {
		klog.Infof("CapacityCheckWaitTimeSeconds label %q value %q is lower than the default value %v; ignoring the label value and defaulting", gkelabels.CapacityCheckWaitTimeSecondsLabel, val, minDefaultTime)
		return minDefaultTime, true
	}
	return capacityCheckWaitTimeSeconds, true
}

// InstanceByRef returns GceInstance from cache. It returns nil when the corresponding instance is not cached.
func (m *gkeManagerImpl) InstanceByRef(ref gce.GceRef) *gce.GceInstance {
	mig, err := m.GetMigForInstance(ref)
	if err != nil {
		klog.Errorf("[InstanceByRef] GetMigForInstance failed for %v with err: %v", ref, err)
		return nil
	}
	if mig == nil {
		klog.V(4).Infof("[InstanceByRef] Mig for %v not found", ref)
		return nil
	}

	instances, err := m.migInfoProvider.GetMigInstances(mig.GceRef())
	if err != nil {
		klog.Errorf("[InstanceByRef] GetMigInstances failed for %v with err: %v", mig.GceRef(), err)
		return nil
	}

	for _, instance := range instances {
		instanceRef, err := gce.GceRefFromProviderId(instance.Id)
		if err != nil {
			klog.Errorf("[InstanceByRef] gce.GceRefFromProviderId failed for %v with err: %v", instance.Id, err)
			return nil
		}

		if instanceRef == ref {
			return &instance
		}
	}

	return nil
}

func (m *gkeManagerImpl) GetListManagedInstancesResults(migRef gce.GceRef) (string, error) {
	return m.migInfoProvider.GetListManagedInstancesResults(migRef)
}

func (m *gkeManagerImpl) ResumeInstances(migRef gce.GceRef, instances []gce.GceRef) error {
	return m.gceService.ResumeInstances(migRef, instances)
}

func (m *gkeManagerImpl) SuspendInstances(migRef gce.GceRef, instances []gce.GceRef, forceSuspend bool) error {
	return m.gceService.SuspendInstances(migRef, instances, forceSuspend)
}

// CalculateOSPhysicalEphemeralStorageGiB find minimum Physical disk size that accommodate Allocatable
func (m *gkeManagerImpl) CalculatePhysicalEphemeralStorageGiB(mig *GkeMig, allocatableBytes int64) int64 {
	// TODO: Operating system will need to changed if we ever want NAP for windows.
	gkeMigOsInfo := NewGkeMigOsInfo(gce.NewMigOsInfo(gce.OperatingSystemLinux, m.GetOsDistributionForNap(mig), mig.GetSystemArchitecture()), mig.Version(), mig.IsConfidentialNode())
	return m.reserved.CalculatePhysicalEphemeralStorageGiB(gkeMigOsInfo, allocatableBytes)
}

// haveAutoscalingOptionsProvider defines whether GKE has some override for node group autoscaling options
func (m *gkeManagerImpl) haveAutoscalingOptionsProvider() bool {
	if m.autoscalingOptsProvider == nil {
		return false
	}

	val := reflect.ValueOf(m.autoscalingOptsProvider)
	if val.Kind() == reflect.Pointer && val.IsNil() {
		return false
	}

	return true
}

// ScaleDownUnreadyTimeOverride fetches an override for scaledown unneeded time
func (m *gkeManagerImpl) ScaleDownUnreadyTimeOverride(mig *GkeMig) (time.Duration, bool) {
	if mig == nil || !mig.QueuedProvisioning() || m.optsTracker == nil {
		return 0, false
	}
	unreadyTime := m.optsTracker.ExperimentsManager().EvaluateDurationSecondsFlagOrFailsafe(experiments.ProvisioningRequestsScaleDownUnreadyFlag, -1)
	if unreadyTime == -1 {
		return 0, false
	}
	return unreadyTime, true
}

// ScaleDownUnneededTimeOverride fetches an override for scaledown unneeded time
func (m *gkeManagerImpl) ScaleDownUnneededTimeOverride(nodeGroup cloudprovider.NodeGroup) (time.Duration, bool, error) {
	if !m.haveAutoscalingOptionsProvider() {
		return 0, false, nil
	}

	return m.autoscalingOptsProvider.ScaleDownUnneededTime(nodeGroup)
}

// ScaleDownUtilizationThresholdOverride fetches an override for scaledown utilization threshold
func (m *gkeManagerImpl) ScaleDownUtilizationThresholdOverride(nodeGroup cloudprovider.NodeGroup) (float64, bool, error) {
	if !m.haveAutoscalingOptionsProvider() {
		return 0, false, nil
	}

	return m.autoscalingOptsProvider.ScaleDownUtilizationThreshold(nodeGroup)
}

// ScaleDownGpuUtilizationThresholdOverride fetches an override for scaledown gpu utilization threshold
func (m *gkeManagerImpl) ScaleDownGpuUtilizationThresholdOverride(nodeGroup cloudprovider.NodeGroup) (float64, bool, error) {
	if !m.haveAutoscalingOptionsProvider() {
		return 0, false, nil
	}

	return m.autoscalingOptsProvider.ScaleDownGpuUtilizationThreshold(nodeGroup)
}

// IsResizableVmEnabledInAutopilot returns true if resizable VM for the given family is enabled for Autopilot.
func (m *gkeManagerImpl) IsResizableVmEnabledInAutopilot(machineFamily string) bool {
	return m.resizableVmAutoprovisioningProvider.IsResizableVmEnabledInAutopilot(machineFamily)
}

// IsResizableVmWithinPodFamilyEnabled returns true if resizable VMs for the given family can be used within a pod family.
func (m *gkeManagerImpl) IsResizableVmWithinPodFamilyEnabled(machineFamily string) bool {
	return m.resizableVmAutoprovisioningProvider.IsResizableVmWithinPodFamilyEnabled(machineFamily)
}

// IsArmMachineFallbacksEnabled returns true if machine fallbacks to N4A and C4A are enabled.
func (m *gkeManagerImpl) IsArmMachineFallbacksEnabled() bool {
	if m == nil || m.optsTracker == nil || m.optsTracker.ExperimentsManager() == nil {
		return false
	}
	return m.optsTracker.ExperimentsManager().EvaluateMinimumVersionFlagOrFailsafe(experiments.AutopilotArmMachineFallbacksMinCAVersionFlag, false) &&
		m.optsTracker.ExperimentsManager().EvaluateBoolFlagOrFailsafe(experiments.AutopilotArmMachineFallbacksEnabledFlag, true)
}

// IsEkEdpEnabled returns true if Edp on EKs with affinity X is enabled
func (m *gkeManagerImpl) IsEkEdpEnabled() bool {
	return m.ekEdpEnabledCache
}

// ResizingEnabled checks if resizing is enabled for the given machine family.
func (m *gkeManagerImpl) ResizingEnabled(machineFamily string) bool {
	return m.resizableVmAutoprovisioningProvider.ResizingEnabled(machineFamily)
}

// IsEkSpotEnabled returns true if EKs can be used as spot VMs
func (m *gkeManagerImpl) IsEkSpotEnabled() bool {
	return m.ekSpotEnabledCache.Get()
}

// GetNodesScaleDownAllowedFromCache retrieves the scale-down information for nodes from the cache.
func (m *gkeManagerImpl) GetNodesScaleDownAllowedFromCache(nodeNames []string) map[string]bool {
	return m.cache.getNodesScaleDownAllowed(nodeNames)
}

// UpdateNodesScaleDownAllowedCache updates the scale-down information for nodes in the cache.
func (m *gkeManagerImpl) UpdateNodesScaleDownAllowedCache(nodesScaleDownAllowed map[string]bool) {
	m.cache.updateNodesScaleDownAllowed(nodesScaleDownAllowed)
}

// InvalidateNodesScaleDownAllowed invalidates the cache storing information about whether nodes are allowed to be scaled down.
func (m *gkeManagerImpl) InvalidateNodesScaleDownAllowedCache() {
	m.cache.invalidateNodesScaleDownAllowed()
}

// fragmentedResizeRequestWarning creates an appropriate error object to be handled by error_reporter.go for updating the scale up size of a fragmented flex start scale up,
// The scale up is originally of size delta, and was fragmented into a smaller scale up of size batchSize.
func fragmentedResizeRequestWarning(delta, batchSize int64) *resizerequestclient.ResizeRequestOperationMultiError {
	// Any error that is not of type `NewResizeRequestOperationMultiError` will cause a backoff, so we must use it
	err := resizerequestclient.NewResizeRequestOperationMultiError(1)
	message := fmt.Sprintf(resizerequestclient.FragmentedRRWarningMessageFormat, batchSize, delta)
	err.AppendCreationError(resizerequestclient.ResizeRequestOperationError{Code: resizerequestclient.FragmentedRRWarningCode, Message: message})
	return err
}

// GetInjectedMig returns injected mig for a given mig.
func (m *gkeManagerImpl) GetInjectedMig(mig *GkeMig) *GkeMig {
	m.migIdToInjectedNgMutex.Lock()
	defer m.migIdToInjectedNgMutex.Unlock()
	return m.migIdToInjectedNg[mig.GceRef()]
}

// SetInjectedMig sets injected mig for a given mig.
func (m *gkeManagerImpl) SetInjectedMig(real, injected *GkeMig) {
	m.migIdToInjectedNgMutex.Lock()
	defer m.migIdToInjectedNgMutex.Unlock()
	if m.migIdToInjectedNg == nil {
		m.migIdToInjectedNg = map[gce.GceRef]*GkeMig{}
	}
	m.migIdToInjectedNg[real.GceRef()] = injected
}

// GetDeploymentType returns the MIG's deployment type based on the reservation type
func (m *gkeManagerImpl) GetDeploymentType(gceRef gce.GceRef, spec *gkeclient.NodePoolSpec) DeploymentTypeEnum {
	if m.reservationsPuller == nil {
		klog.V(5).Infof("Reservations puller not set, can't get reservation deployment type for mig %s", gceRef)
		return DeploymentTypeNone
	}
	if spec == nil || spec.ReservationAffinity == nil || spec.ReservationAffinity.ConsumeReservationType == gkeclient.ReservationAffinityNone || len(spec.ReservationAffinity.Values) == 0 {
		return DeploymentTypeNone
	}
	if len(spec.ReservationAffinity.Values) != 1 {
		klog.Error("this should not happen, currently only one reservation per mig is supported")
		return DeploymentTypeUnspecified
	}
	rsvName := reservationName(spec.ReservationAffinity.Values[0])

	reservations := m.reservationsPuller.GetReservations()
	for _, r := range reservations {
		if r.Name == rsvName {
			return DeploymentTypeEnum(r.DeploymentType)
		}
	}
	klog.Warningf("can't get reservation deployment type, reservation %s not found for mig %s", spec.ReservationAffinity.Values[0], gceRef)
	return DeploymentTypeUnspecified
}

// ExistingMigsInNodePool returns a list of node groups (MIGs) that belong to a given node pool. It returns a nil if the node pool is not found.
func (m *gkeManagerImpl) ExistingMigsInNodePool(nodePoolName string) []*GkeMig {
	return m.cache.ExistingMigsInNodePool(nodePoolName)
}

// NodePoolSpecForNode returns the node pool spec for a particular node, regardless whether
// the node pool is autoscaled or not.
func (m *gkeManagerImpl) NodePoolSpecForNode(node *apiv1.Node) (*gkeclient.NodePoolSpec, error) {
	nodePoolName, ok := node.Labels[gkelabels.GkeNodePoolLabel]
	if !ok {
		return nil, fmt.Errorf("node %s does not have %s label", node.Name, gkelabels.GkeNodePoolLabel)
	}
	if spec, ok := m.cache.GetNodePoolSpec(nodePoolName); ok {
		return spec, nil
	}
	return nil, fmt.Errorf("node pool %s not found", nodePoolName)
}

// GetBasenameForMig returns basename for this existing MIG
func (m *gkeManagerImpl) GetBasenameForMig(mig *GkeMig) (string, error) {
	return m.migInfoProvider.GetMigBasename(mig.gceRef)
}

// MachineConfigProvider returns the MachineConfigProvider.
func (m *gkeManagerImpl) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return m.machineConfigProvider
}

// ExperimentsManager returns the experiments.Manager.
func (m *gkeManagerImpl) ExperimentsManager() experiments.Manager {
	return m.optsTracker.ExperimentsManager()
}

func (m *gkeManagerImpl) TrimLocationsForMachineConfig(locations []string, machineType string, acceleratorConfig *gke_api_beta.AcceleratorConfig, minCpuPlatform string, diskType string) []string {
	testFunctions := []func(location string) (ok bool, reason string, err error){
		func(location string) (ok bool, reason string, err error) {
			return m.validateLocationForMachineConfig(location, machineType, acceleratorConfig)
		},
		func(location string) (ok bool, reason string, err error) {
			return m.validateLocationForMinCpuPlatform(location, minCpuPlatform)
		},
		func(location string) (ok bool, reason string, err error) {
			return m.ValidateLocationForDiskType(location, diskType)
		},
	}

	trimmedLocations := []string{}
	for _, location := range locations {
		isValid := true
		for _, testFunc := range testFunctions {
			ok, _, err := testFunc(location)
			if err != nil || !ok {
				isValid = false
				break
			}
		}
		if isValid {
			trimmedLocations = append(trimmedLocations, location)
		}
	}

	return trimmedLocations
}
