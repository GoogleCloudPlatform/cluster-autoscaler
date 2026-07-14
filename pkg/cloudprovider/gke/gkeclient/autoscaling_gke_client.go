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

package gkeclient

import (
	"flag"
	"fmt"
	"strconv"
	"time"

	gke_api_beta "google.golang.org/api/container/v1beta1"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/sandbox"
)

var supportedBasicResources = map[string]bool{}

func init() {
	supportedBasicResources[cloudprovider.ResourceNameCores] = true
	supportedBasicResources[cloudprovider.ResourceNameMemory] = true
}

const (
	defaultOperationWaitTimeout  = 120 * time.Second
	defaultOperationPollInterval = 1 * time.Second

	// ServiceAccountDeleted represents a deleted service account error.
	ServiceAccountDeleted = "serviceAccountDeleted"
)

const (
	clusterPathPrefix   = "projects/%s/locations/%s/clusters/%s"
	nodePoolPathPrefix  = "projects/%s/locations/%s/clusters/%s/nodePools/%%s"
	operationPathPrefix = "projects/%s/locations/%s/operations/%%s"
)

// ReservationAffinityNone means that nodepool will not consume any reservations
const ReservationAffinityNone string = "NO_RESERVATION"

// ReservationAffinitySpecific means that nodepool will only consume specific reservations.
const ReservationAffinitySpecific string = "SPECIFIC_RESERVATION"

// ReservationAffinityAny means that nodepool will only consume any matching reservations.
const ReservationAffinityAny string = "ANY_RESERVATION"

// ReservationAffinityAnyThenFail means that nodepool will consume any matching reservation and if there is none will fail, not consuming on-demand capacity.
const ReservationAffinityAnyThenFail string = "ANY_RESERVATION_THEN_FAIL"

const ReservationNameKey string = "compute.googleapis.com/reservation-name"

// GPUSharingStrategy specifies the type of GPU sharing strategy to enable on the
// GPU node.
type GPUSharingStrategy string

// TimeSharing represents GPU sharing strategy in which GPUs are time-shared between containers.
const TimeSharing GPUSharingStrategy = "TIME_SHARING"

// Mps represents GPU sharing strategy in which GPUs are shred between containers with Nvidia mps.
const Mps GPUSharingStrategy = "MPS"

// DefaultArchTaintBehavior represents the default architecture taint behavior for a node pool.
const DefaultArchTaintBehavior string = "ARM"

var (
	// GkeAPIEndpoint overrides default GKE API endpoint for testing.
	// This flag is outside main as it's only useful for test/development.
	GkeAPIEndpoint = flag.String("gke-api-endpoint", "", "GKE API endpoint address. This flag is used by developers only. Users shouldn't change this flag.")
)

// AutoscalingGkeClient is used for communicating with GKE API.
type AutoscalingGkeClient interface {
	// reading cluster state
	GetCluster() (Cluster, error)

	// modifying cluster state
	DeleteNodePool(pool string) error
	CreateNodePool(name string, spec *NodePoolSpec) error
	UpdateNodePoolLabels(name string, labels map[string]string) error
}

// LocalSSDConfig holds values relevant for Local SSD configuration.
// Please refer to google.golang.org/api/container/v1beta1/container-gen.go for fields meaning.
type LocalSSDConfig struct {
	LocalSsdCount                  int64
	EphemeralStorageConfig         *gke_api_beta.EphemeralStorageConfig
	EphemeralStorageLocalSsdConfig *gke_api_beta.EphemeralStorageLocalSsdConfig
	LocalNvmeSsdBlockConfig        *gke_api_beta.LocalNvmeSsdBlockConfig
}

// EphemeralStorageOnLocalSsd returns whether this Local SSD config means that Ephemeral storage will be based on
// Local SSDs.
func (c *LocalSSDConfig) EphemeralStorageOnLocalSsd(machineType string) bool {
	if c == nil {
		return false
	}
	if c.EphemeralStorageConfig != nil && c.EphemeralStorageConfig.LocalSsdCount > 0 {
		return true
	}
	if c.EphemeralStorageLocalSsdConfig != nil && c.EphemeralStorageLocalSsdConfig.LocalSsdCount > 0 {
		return true
	}
	// TODO(b/317518331): Handle gen3 machines with 0 count properly, based on the machine type:
	// go/gke-autoscaler-gen3-machine-type
	return false
}

// AdditionalNetworkConfig represents additional network config for a given node pool.
type AdditionalNetworkConfig struct {
	VPCNetName        string
	VPCSubnetName     string
	SubRange          string
	MaxPodsPerNode    int64
	NetworkAttachment string
}

// MaxPodsConstraint contains constraints applied to pods.
type MaxPodsConstraint struct {
	MaxPodsPerNode int64
}

// Cluster contains cluster's fields we want to use.
type Cluster struct {
	Status         string
	ClusterVersion string
	// EmulatedClusterVersion is set on the Cluster proto (actual field name is CurrentEmulatedVersion) when the cluster has an ongoing "safer" upgrade
	// where the API server is emulated to behave like this minor version. Has to be taken into account when CA depends on K8s APIs being enabled by default
	// from some minor version. More details: go/gke-component-emulated-version.
	EmulatedClusterVersion string // Only contains the major and minor version, example value: "1.33"
	ReleaseChannel         string
	// Cluster network path in the format "projects/{PROJECT_ID}/global/networks/{NETWORK_NAME}""
	NetworkPath    string
	SubnetworkPath string
	Subnetwork     string
	Locations      []string
	// NodePools keeps track of NodePools with cluster autoscaling enabled
	NodePools []NodePool
	// AllNodePoolNames keeps track of all node pools in the cluster
	AllNodePoolNames                 sets.Set[string]
	ResourceLimiter                  *cloudprovider.ResourceLimiter
	AutoprovisioningLocations        []string
	NodeAutoprovisioningEnabled      bool
	AutoprovisioningNodePoolDefaults *gke_api_beta.AutoprovisioningNodePoolDefaults
	NodePoolDefaults                 *gke_api_beta.NodePoolDefaults
	DefaultMaxPodsConstraint         *MaxPodsConstraint
	ConfidentialNodesEnabled         bool
	ConfidentialInstanceType         string
	NodeLocalDNSEnabled              bool
	WorkloadIdentityEnabled          bool
	HighThroughputLoggingEnabled     bool
	CreateTime                       time.Time
	IsClusterUsingPSCInfrastructure  bool
	EnablePrivateNodes               bool
	DataplaneV2Enabled               bool
	DefaultCCCEnabled                bool
}

func (c Cluster) MetadataServerEnabled() bool {
	return c.WorkloadIdentityEnabled
}

func (c Cluster) NetdEnabled() bool {
	return c.DataplaneV2Enabled || c.WorkloadIdentityEnabled
}

// NodePoolSpec contains the information needed to create a new node pool
type NodePoolSpec struct {
	Accelerators         []*gke_api_beta.AcceleratorConfig
	LocalSSDConfig       *LocalSSDConfig
	MachineType          string
	ComputeClass         string
	Labels               map[string]string
	Metadata             map[string]string
	Taints               []apiv1.Taint
	Locations            []string
	DiskSize             int64
	DiskType             string
	DiskEncryptionKey    string
	Preemptible          bool
	Spot                 bool
	ImageType            string
	MinCpuPlatform       string
	SystemArchitecture   *gce.SystemArchitecture
	Defaults             *gke_api_beta.AutoprovisioningNodePoolDefaults
	ReservationAffinity  *gke_api_beta.ReservationAffinity
	ExtendedDurationPods string
	PlacementGroup       placement.Spec
	SandboxType          sandbox.Type
	TpuType              string
	TpuTopology          string
	TpuMultiHost         bool
	// MaxPodsPerNode sets the Max Pods Constraint value for given node pool.
	// If it's 0, it means it's unset, and we rely on control plane defaulting
	// to set appropriate value.
	MaxPodsPerNode           int64
	NetworkConfigs           []AdditionalNetworkConfig
	QueuedProvisioning       bool
	LocationPolicy           string
	ResourceLabels           map[string]string
	PodIpv4CidrBlock         string
	PodRange                 string
	Network                  string
	Subnetwork               string
	ClusterSubnetwork        string
	ClusterNetworkPath       string
	ClusterSubnetworkPath    string
	MaxRunDurationInSeconds  string // String has to be a parsable integer representing number of seconds, no unit
	FlexStart                bool
	SecondaryBootDisks       []*gke_api_beta.SecondaryBootDisk
	AutopilotManaged         bool
	ServiceAccount           string
	LinuxNodeConfig          *LinuxNodeConfig
	KubeletConfig            *gke_api_beta.NodeKubeletConfig
	ReservationBlockCount    int64
	ReservationSubBlockCount int64
	// SelfServiceMetadata represents metadata of self-service. It is
	// translated from/to gke_api_beta.NodePool using self-service generators.
	SelfServiceMetadata         map[string]string
	UpgradeSettings             *gke_api_beta.UpgradeSettings
	ConfidentialNodeType        string
	ConsolidationDelayInSeconds string
	NodeVersion                 string
	ArchTaintBehavior           string
}

// TODO(b/433732918): For existing migs, get the actual target size policy value once targetSizePolicy API hits v1 (since it needs to be through OSS)
// TODO(b/412855977): For upcoming migs, get the resource policy via resource policy puller, and check the gpu topology field
// TODO(b/480838423): Add unit test
// This temporary approximation might be invalidated in the future and needs to be maintained in the meantime.
// This helper needs to be kept in sync with cluster-autoscaler/pkg/provisioningrequests/pods/pods.go's UsesBulkProvisioning
func (spec *NodePoolSpec) UsesBulkProvisioning(mcp *machinetypes.MachineConfigProvider) bool {
	// TODO(b/485169961): return false if ProvisioningRequestBulkMigsFlag not enabled
	mf, err := mcp.GetMachineFamilyFromMachineName(spec.MachineType)
	if err != nil {
		klog.Errorf("could not get machine family for %s: %v", spec.MachineType, err)
		return false
	}
	return mf.IsGpuAcceleratorSliceSupported() && spec.FlexStart && spec.PlacementGroup.UsesPlacement()
}

func (spec *NodePoolSpec) ConsolidationDelay() (*time.Duration, error) {
	if spec == nil || spec.ConsolidationDelayInSeconds == "" {
		return nil, nil
	}
	seconds, err := strconv.ParseInt(spec.ConsolidationDelayInSeconds, 10, 0)
	if err != nil {
		return nil, fmt.Errorf("while parsing 'consolidationDelayInSeconds' got error: %w", err)
	}
	duration := time.Duration(seconds) * time.Second
	return &duration, nil
}

// NodePool contains node pool's fields we want to use.
type NodePool struct {
	Name                     string
	InstanceGroupUrls        []string
	Autoscaled               bool
	MinNodeCount             int64
	MaxNodeCount             int64
	TotalMinNodeCount        int64
	TotalMaxNodeCount        int64
	LocationPolicy           string
	Autoprovisioned          bool
	ThreadsPerCore           int64
	Version                  string
	BlueGreenInfo            *BlueGreenInfo
	QueuedProvisioning       bool
	ConfidentialNodesEnabled bool
	ConfidentialInstanceType string
	Spec                     *NodePoolSpec
	Spot                     bool
	Status                   string
	AutopilotManaged         bool
}

// BlueGreenInfo contains v1beta1.BlueGreenInfo fields we want to use.
type BlueGreenInfo struct {
	Autoscaled   bool
	BlueMigUrls  []string
	GreenMigUrls []string
	Phase        UpdatePhase
}

// UpdatePhase is the phase of a Blue/Green update. For some reason the generated GKE client holds enums in simple
// string fields, with options just listed in a comment. So these have to be defined here.
type UpdatePhase string

const (
	// PhaseUnspecified is a sentry value that shouldn't be encountered in the wild, unless new fields are added to
	// the enum and our client lags behind.
	PhaseUnspecified UpdatePhase = "PHASE_UNSPECIFIED"
	// PhaseUpdateStarted - Blue/green update has been initiated.
	PhaseUpdateStarted UpdatePhase = "UPDATE_STARTED"
	// PhaseCreatingGreenPool - start creating green pool nodes.
	PhaseCreatingGreenPool UpdatePhase = "CREATING_GREEN_POOL"
	// PhaseCordoningBluePool - start cordoning blue pool nodes.
	PhaseCordoningBluePool UpdatePhase = "CORDONING_BLUE_POOL"
	// PhaseDrainingBluePool - start draining blue pool nodes.
	PhaseDrainingBluePool UpdatePhase = "DRAINING_BLUE_POOL"
	// PhaseNodePoolSoaking - start soaking time after draining entire blue pool.
	PhaseNodePoolSoaking UpdatePhase = "NODE_POOL_SOAKING"
	// PhaseDeletingBluePool - start deleting blue nodes.
	PhaseDeletingBluePool UpdatePhase = "DELETING_BLUE_POOL"
	// PhaseRollbackStarted - rollback has been initiated.
	PhaseRollbackStarted UpdatePhase = "ROLLBACK_STARTED"
	// PhaseWaitingToDrainBluePool - autoscale bg intermediate phase
	PhaseWaitingToDrainBluePool UpdatePhase = "WAITING_TO_DRAIN_BLUE_POOL"
)

// ValidUpdatePhases lists valid values for a Blue/Green update phase. It contains all UpdatePhase values apart from
// PhaseUnspecified, which isn't a valid value.
var ValidUpdatePhases = map[UpdatePhase]bool{
	PhaseUpdateStarted:          true,
	PhaseCreatingGreenPool:      true,
	PhaseCordoningBluePool:      true,
	PhaseDrainingBluePool:       true,
	PhaseNodePoolSoaking:        true,
	PhaseDeletingBluePool:       true,
	PhaseRollbackStarted:        true,
	PhaseWaitingToDrainBluePool: true,
}
