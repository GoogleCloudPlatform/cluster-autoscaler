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

package integration

import (
	"fmt"
	"math"
	"time"

	cccv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"google.golang.org/api/compute/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	ossconfig "k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/localssdsize"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	config "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments/fake"
)

const (
	defaultCluster  = "test-cluster"
	defaultProject  = "test-project"
	defaultLocation = "us-central1"

	// DefaultMaxCoresResourceLimit is 64k CPU cores
	DefaultMaxCoresResourceLimit = 64_000
	// DefaultMaxMemoryResourceLimit is 256k GiB memory
	DefaultMaxMemoryResourceLimit = 256_000
)

var DefaultZones = []string{"us-central1-a", "us-central1-b", "us-central1-c"}

// DefaultInternalOptions provides GKE-specific autoscaler backend defaults.
var DefaultInternalOptions = config.InternalOptions{
	Location:                    defaultLocation,
	ClusterHash:                 "12345678",
	EkAutoprovisioning:          "EK_AUTOPROVISIONING_UNSPECIFIED",
	EkvmsMinVmSize:              apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse("250m"), apiv1.ResourceMemory: resource.MustParse("2Gi")},
	EkvmsIncrementStep:          apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse("50m"), apiv1.ResourceMemory: resource.MustParse("1Mi")},
	TpuAutoprovisioningEnabled:  true,
	NapDefaultMachineTypeFamily: "e2",
	DefragCandidateLimit:        10,
	DefragCandidateNodeLimit:    1,
	DefragMaxDelay:              5 * time.Minute,
	DefragScaleUpTimeout:        10 * time.Minute,
	DefragScaleDownTimeout:      5 * time.Minute,
}

// DefaultAutoscalingOptions provides the baseline configuration for all tests.
var DefaultAutoscalingOptions = ossconfig.AutoscalingOptions{
	NodeGroupDefaults: ossconfig.NodeGroupAutoscalingOptions{
		ScaleDownUnneededTime:         time.Second,
		ScaleDownUnreadyTime:          time.Minute,
		ScaleDownUtilizationThreshold: 0.5,
		MaxNodeProvisionTime:          10 * time.Second,
	},
	EstimatorName:              estimator.BinpackingEstimatorName,
	EnforceNodeGroupMinSize:    true,
	ScaleDownSimulationTimeout: 24 * time.Hour,
	ScaleDownDelayAfterAdd:     0,
	ScaleDownDelayAfterDelete:  0,
	ScaleDownDelayAfterFailure: 0,
	ScaleDownDelayTypeLocal:    true,
	MaxScaleDownParallelism:    10,
	MaxNodesTotal:              100,
	MaxCoresTotal:              1000,
	MaxMemoryTotal:             1000 * 1024 * 1024 * 1024, // 1 TB
	// TOOD (b/516735931): Add priority expander to the list to match CA manifest.
	ExpanderNames:        "edp-filter,snowflake,mppn-filter,fleet-efficiency,gke-price",
	ScaleUpFromZero:      true,
	FrequentLoopsEnabled: true,
	ClusterName:          defaultCluster,
	MaxBinpackingTime:    10 * time.Second,
	OkTotalUnreadyCount:  10,
	PredicateParallelism: 1,
	GCEOptions: ossconfig.GCEOptions{
		LocalSSDDiskSizeProvider: localssdsize.NewDynamicLocalSSDDiskSizeProvider(machinetypes.LocalSSDDiskSizes),
	},
	ScaleDownEnabled: true,
}

// DefaultCluster provides a standard skeleton GKE regional cluster.
var DefaultCluster = gke_api_beta.Cluster{
	Name:                 defaultCluster,
	CreateTime:           time.Now().Format(time.RFC3339),
	CurrentMasterVersion: "1.35.1-gke.1510000",
	Autoscaling: &gke_api_beta.ClusterAutoscaling{
		ResourceLimits: []*gke_api_beta.ResourceLimit{
			{ResourceType: "cpu", Maximum: DefaultMaxCoresResourceLimit},
			{ResourceType: "memory", Maximum: DefaultMaxMemoryResourceLimit},
		},
	},
	Status:    "RUNNING",
	Location:  defaultLocation,
	Locations: DefaultZones,
	NodePools: []*gke_api_beta.NodePool{},
}

var DefaultDeviceClasses = []string{"gpu.nvidia.com", "tpu.google.com", "dra.net"}

// TestConfig is the "blueprint" for a test. It defines the entire
// initial state of the world before the test runs.
type TestConfig struct {
	// BaseOptions can be set to DefaultAutoscalingOptions + DefaultInternalOptions or a custom base.
	BaseOptions *config.AutoscalingOptions
	// OptionsOverrides allows adding options overrides.
	OptionsOverrides []Option[*config.AutoscalingOptions]

	CaVersion string
	ProjectID string
	Location  string

	Cluster          *gke_api_beta.Cluster
	ClusterOverrides []Option[*gke_api_beta.Cluster]

	ExternalHardwareSoT bool

	RegionToZones   map[string][]string
	RegionToAiZones map[string][]string
	// Reservations are stored per project ID
	Reservations      map[string][]*compute.Reservation
	InstanceTemplates []*compute.InstanceTemplate
	// CccCrds are populated into the ComputeClass fake clientset during test initialization.
	CccCrds []*cccv1.ComputeClass
	// Names of the Device Classes that will be deployed in the cluster.
	DeviceClasses []string
	// ExperimentEvaluator allows overriding the default experiment evaluator.
	ExperimentEvaluator experiments.Evaluator
	// ProvisioningRequests to inject into the fake client.
	ProvisioningRequests []*provreqwrapper.ProvisioningRequest
}

// ConfigBuilder uses the Builder Pattern to create a test Config.
type ConfigBuilder struct {
	autoscalingOptions  []Option[*config.AutoscalingOptions]
	clusterOptions      []Option[*gke_api_beta.Cluster]
	externalHardwareSoT bool

	regionToZoneOptions     []Option[*map[string][]string]
	reservationOptions      []Option[*map[string][]*compute.Reservation]
	instanceTemplateOptions []Option[*[]*compute.InstanceTemplate]
}

// deepCopyClusterAutoscaling returns an independent deep copy of a
// ClusterAutoscaling value. This prevents cross-test contamination when
// overrides (e.g. WithAutoprovisioningLocations) mutate pointer fields
// that would otherwise be shared with the global DefaultCluster.
func deepCopyClusterAutoscaling(src *gke_api_beta.ClusterAutoscaling) *gke_api_beta.ClusterAutoscaling {
	if src == nil {
		return nil
	}
	dst := *src
	dst.ResourceLimits = make([]*gke_api_beta.ResourceLimit, len(src.ResourceLimits))
	for i, rl := range src.ResourceLimits {
		rlCopy := *rl
		dst.ResourceLimits[i] = &rlCopy
	}
	dst.AutoprovisioningLocations = append([]string(nil), src.AutoprovisioningLocations...)
	if src.AutoprovisioningNodePoolDefaults != nil {
		npDefaultsCopy := *src.AutoprovisioningNodePoolDefaults
		dst.AutoprovisioningNodePoolDefaults = &npDefaultsCopy
	}
	if src.DefaultComputeClassConfig != nil {
		cccCopy := *src.DefaultComputeClassConfig
		dst.DefaultComputeClassConfig = &cccCopy
	}
	return &dst
}

// NewTestConfig creates a test config pre-populated with Default settings.
func NewTestConfig() *TestConfig {
	defaultOpts := config.AutoscalingOptions{
		InternalOptions:    DefaultInternalOptions,
		AutoscalingOptions: DefaultAutoscalingOptions,
	}
	defaultCluster := DefaultCluster
	defaultCluster.NodePools = make([]*gke_api_beta.NodePool, 0)
	defaultCluster.Locations = make([]string, len(DefaultCluster.Locations))
	copy(defaultCluster.Locations, DefaultCluster.Locations)
	defaultCluster.Autoscaling = deepCopyClusterAutoscaling(DefaultCluster.Autoscaling)

	return &TestConfig{
		BaseOptions:         &defaultOpts,
		OptionsOverrides:    []Option[*config.AutoscalingOptions]{},
		CaVersion:           "35.140.0",
		Cluster:             &defaultCluster,
		ClusterOverrides:    []Option[*gke_api_beta.Cluster]{},
		ProjectID:           defaultProject,
		Location:            defaultLocation,
		RegionToZones:       map[string][]string{defaultLocation: DefaultZones},
		Reservations:        make(map[string][]*compute.Reservation),
		InstanceTemplates:   make([]*compute.InstanceTemplate, 0),
		CccCrds:             make([]*cccv1.ComputeClass, 0),
		DeviceClasses:       DefaultDeviceClasses,
		ExperimentEvaluator: experiments.NewNoopEvaluator(),
	}
}

// DefaultNodePool provides a standard skeleton GKE node pool with options.
// The default machine type is n1-standard-2.
func DefaultNodePool(opts ...Option[*gke_api_beta.NodePool]) *gke_api_beta.NodePool {
	np := &gke_api_beta.NodePool{
		Name: "default-pool",
		Config: &gke_api_beta.NodeConfig{
			MachineType: "n1-standard-2",
			ImageType:   "cos_containerd",
		},
		InitialNodeCount: 1,
		Autoscaling: &gke_api_beta.NodePoolAutoscaling{
			Enabled:      true,
			MinNodeCount: 1,
			MaxNodeCount: 1000,
		},
		Locations: DefaultZones,
	}
	return Apply(np, opts...)
}

// --- Fluent Builder Methods on TestConfig ---

// WithCaVersion sets the Cluster Autoscaler version for the test environment.
func (c *TestConfig) WithCaVersion(version string) *TestConfig {
	c.CaVersion = version
	return c
}

// WithProjectID sets the cluster project ID.
func (c *TestConfig) WithProjectID(id string) *TestConfig {
	c.ProjectID = id
	return c
}

// WithLocation sets the cluster location.
func (c *TestConfig) WithLocation(loc string) *TestConfig {
	c.Location = loc
	return c
}

// WithClusterWideLimits sets aggregate global limits for the whole cluster.
func (c *TestConfig) WithClusterWideLimits(maxNodes int, maxCores int64, maxMemory int64) *TestConfig {
	c.WithOverrides(
		WithMaxNodesTotal(maxNodes),
		WithMaxCoresTotal(maxCores),
		WithMaxMemoryTotal(maxMemory),
	)
	return c
}

// WithProvisioningRequests sets the ProvisioningRequests for the test.
func (c *TestConfig) WithProvisioningRequests(prs ...*provreqwrapper.ProvisioningRequest) *TestConfig {
	c.ProvisioningRequests = prs
	return c
}

// WithOverrides allows adding options overrides to the config.
func (c *TestConfig) WithOverrides(overrides ...Option[*config.AutoscalingOptions]) *TestConfig {
	c.OptionsOverrides = append(c.OptionsOverrides, overrides...)
	return c
}

// WithClusterOverrides allows adding cluster configuration overrides.
func (c *TestConfig) WithClusterOverrides(overrides ...Option[*gke_api_beta.Cluster]) *TestConfig {
	c.ClusterOverrides = append(c.ClusterOverrides, overrides...)
	return c
}

// WithExperiments allows enabling specific experiment flags.
func (c *TestConfig) WithExperiments(flags ...string) *TestConfig {
	boolFlags := make(map[string]bool)
	stringFlags := make(map[string]string)
	for _, f := range flags {
		boolFlags[f] = true
		stringFlags[f] = "0.0.0" // By setting version to 0.0.0, the minimum version check will always pass
	}
	c.ExperimentEvaluator = fake.NewEvaluator(boolFlags, stringFlags)
	return c
}

// WithExperimentOverrides allows setting explicit boolean and string values for experiment flags.
func (c *TestConfig) WithExperimentOverrides(boolFlags map[string]bool, stringFlags map[string]string) *TestConfig {
	c.ExperimentEvaluator = fake.NewEvaluator(boolFlags, stringFlags)
	return c
}

// WithExternalHardwareSoT configures the test to use the Kubernetes MachineConfig client as source of truth.
func (c *TestConfig) WithExternalHardwareSoT(externalHardwareSoT bool) *TestConfig {
	c.ExternalHardwareSoT = externalHardwareSoT
	return c
}

// WithMachineConfigEnabled enables the MachineConfig feature in the autoscaler options.
func (c *TestConfig) WithMachineConfigEnabled() *TestConfig {
	c.WithOverrides(WithMachineConfigEnabled())
	return c
}

// WithReservations replaces the entire map of reservations.
func (c *TestConfig) WithReservations(reservations map[string][]*compute.Reservation) *TestConfig {
	c.Reservations = reservations
	return c
}

// WithReservationsForDefaultProject replaces the entire map of reservations
// with map of default project to reservations.
func (c *TestConfig) WithReservationsForDefaultProject(reservations []*compute.Reservation) *TestConfig {
	return c.WithReservations(map[string][]*compute.Reservation{
		defaultProject: reservations,
	})
}

// AddReservation adds a single reservation to a specific project.
func (c *TestConfig) AddReservation(projectID string, reservation *compute.Reservation) *TestConfig {
	if c.Reservations == nil {
		c.Reservations = make(map[string][]*compute.Reservation)
	}
	if c.Reservations[projectID] == nil {
		c.Reservations[projectID] = make([]*compute.Reservation, 0)
	}
	c.Reservations[projectID] = append(c.Reservations[projectID], reservation)
	return c
}

// WithInstanceTemplates replaces the entire list of instance templates.
func (c *TestConfig) WithInstanceTemplates(templates ...*compute.InstanceTemplate) *TestConfig {
	c.InstanceTemplates = templates
	return c
}

// AddInstanceTemplate appends a single instance template to the list.
func (c *TestConfig) AddInstanceTemplate(template *compute.InstanceTemplate) *TestConfig {
	c.InstanceTemplates = append(c.InstanceTemplates, template)
	return c
}

// AddCccCrd adds a ComputeClass to the list.
func (c *TestConfig) AddCccCrd(crd *cccv1.ComputeClass) *TestConfig {
	c.CccCrds = append(c.CccCrds, crd)
	return c
}

// WithCccCrds replaces the entire list of CccCrds.
func (c *TestConfig) WithCccCrds(crds ...*cccv1.ComputeClass) *TestConfig {
	c.CccCrds = crds
	return c
}

// WithOkTotalUnreadyCount sets the OkTotalUnreadyCount in BaseOptions.
func (c *TestConfig) WithOkTotalUnreadyCount(count int) *TestConfig {
	c.BaseOptions.OkTotalUnreadyCount = count
	return c
}

// WithRegionToZones sets the RegionToZones map.
func (c *TestConfig) WithRegionToZones(regionToZones map[string][]string) *TestConfig {
	c.RegionToZones = regionToZones
	return c
}

// WithRegionToAiZones sets the RegionToAiZones map.
func (c *TestConfig) WithRegionToAiZones(regionToAiZones map[string][]string) *TestConfig {
	c.RegionToAiZones = regionToAiZones
	return c
}

// WithNodePools appends node pools to the cluster.
func (c *TestConfig) WithNodePools(nodePools ...*gke_api_beta.NodePool) *TestConfig {
	if c.Cluster == nil {
		c.Cluster = &gke_api_beta.Cluster{}
	}
	for _, np := range nodePools {
		if np == nil || np.Config == nil || np.Config.MachineType == "" {
			panic(fmt.Sprintf("cannot provision NodePool without a configured MachineType"))
		}
		c.Cluster.NodePools = append(c.Cluster.NodePools, np)
	}
	return c
}

// ResolveOptions merges the base options with all registered overrides.
func (c *TestConfig) ResolveOptions() config.AutoscalingOptions {
	var opts config.AutoscalingOptions
	if c.BaseOptions != nil {
		opts = *c.BaseOptions
	}

	Apply(&opts, c.OptionsOverrides...)
	return opts
}

// ResolveCluster merges the base cluster with all registered overrides.
func (c *TestConfig) ResolveCluster() gke_api_beta.Cluster {
	var cluster gke_api_beta.Cluster
	if c.Cluster != nil {
		cluster = *c.Cluster
	}

	Apply(&cluster, c.ClusterOverrides...)
	return cluster
}

// --- Autoscaling Option Overrides ---

// WithMachineConfigEnabled enables the MachineConfig feature in the autoscaler options.
func WithMachineConfigEnabled() Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.MachineConfigEnabled = true
		return o
	}
}

// WithLocation overrides the Location setting in InternalOptions.
func WithLocation(location string) Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.InternalOptions.Location = location
		return o
	}
}

// WithMaxNodesTotal overrides the MaxNodesTotal setting.
func WithMaxNodesTotal(max int) Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.MaxNodesTotal = max
		return o
	}
}

// WithMaxNodesPerScaleUp overrides the MaxNodesPerScaleUp setting.
func WithMaxNodesPerScaleUp(max int) Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.MaxNodesPerScaleUp = max
		return o
	}
}

// WithMaxScaleDownParallelism overrides the MaxScaleDownParallelism setting.
func WithMaxScaleDownParallelism(max int) Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.MaxScaleDownParallelism = max
		return o
	}
}

// WithMaxCoresTotal overrides the MaxCoresTotal setting.
func WithMaxCoresTotal(max int64) Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.MaxCoresTotal = max
		return o
	}
}

// WithMaxMemoryTotal overrides the MaxMemoryTotal setting.
func WithMaxMemoryTotal(max int64) Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.MaxMemoryTotal = max
		return o
	}
}

// WithFlexAdvisorEnabled enables FlexAdvisor in CA.
func WithFlexAdvisorEnabled() Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.GCEFlexAdvisorEnabled = true
		return o
	}
}

// WithComputeClassMinCapacityEnabled enables ComputeClass minimum capacity support in CA.
func WithComputeClassMinCapacityEnabled() Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.EnableComputeClassMinCapacity = true
		return o
	}
}

// WithScaleDownDelayAfterAdd overrides the ScaleDownDelayAfterAdd setting.
func WithScaleDownDelayAfterAdd(delay time.Duration) Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.ScaleDownDelayAfterAdd = delay
		return o
	}
}

// WithScaleDownUnneededTime overrides the ScaleDownUnneededTime in NodeGroupDefaults.
func WithScaleDownUnneededTime(d time.Duration) Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.NodeGroupDefaults.ScaleDownUnneededTime = d
		return o
	}
}

// WithScaleDownUtilizationThreshold overrides the ScaleDownUtilizationThreshold in NodeGroupDefaults.
func WithScaleDownUtilizationThreshold(t float64) Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.NodeGroupDefaults.ScaleDownUtilizationThreshold = t
		return o
	}
}

// WithScaleDownSimulationTimeout overrides the ScaleDownSimulationTimeout setting.
func WithScaleDownSimulationTimeout(d time.Duration) Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.ScaleDownSimulationTimeout = d
		return o
	}
}

// WithExpanderNames overrides the ExpanderNames setting.
func WithExpanderNames(names string) Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.ExpanderNames = names
		return o
	}
}

// WithClusterName overrides the ClusterName setting.
func WithClusterName(name string) Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.ClusterName = name
		return o
	}
}

// WithMaxBinpackingTime overrides the MaxBinpackingTime setting.
func WithMaxBinpackingTime(d time.Duration) Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.MaxBinpackingTime = d
		return o
	}
}

// WithBalanceSimilarNodeGroups overrides the BalanceSimilarNodeGroups setting.
func WithBalanceSimilarNodeGroups() Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.BalanceSimilarNodeGroups = true
		return o
	}
}

// WithAutoProvisioningEnabled enables Node Auto-Provisioning (NAP) and sets a default machine type family.
func WithAutoProvisioningEnabled() Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.NapMaxNodes = math.MaxInt32
		o.MaxAutoprovisionedNodeGroupCount = 999
		o.NapDefaultMachineTypeFamily = "n1"
		return o
	}
}

// WithHighThroughputNAPEnabled enables async node groups and sets control plane operation limits.
func WithHighThroughputNAPEnabled(maxParallelOps, maxQueuedOps int) Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.AsyncNodeGroupsEnabled = true
		o.InternalOptions.CpMaxParallelOps = maxParallelOps
		o.InternalOptions.CpMaxQueuedOps = maxQueuedOps
		return o
	}
}

// WithDraEnabled enables DRA support.
func WithDraEnabled() Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.DynamicResourceAllocationEnabled = true
		return o
	}
}

// --- Cluster Option Overrides ---

// WithClusterZones sets the cluster's locations.
func WithClusterZones(zones ...string) Option[*gke_api_beta.Cluster] {
	return func(c *gke_api_beta.Cluster) *gke_api_beta.Cluster {
		c.Locations = zones
		return c
	}
}

// WithAutoprovisioningLocations sets the AutoprovisioningLocations.
func WithAutoprovisioningLocations(locations ...string) Option[*gke_api_beta.Cluster] {
	return func(c *gke_api_beta.Cluster) *gke_api_beta.Cluster {
		if c.Autoscaling == nil {
			c.Autoscaling = &gke_api_beta.ClusterAutoscaling{}
		}
		c.Autoscaling.AutoprovisioningLocations = locations
		return c
	}
}

// WithClusterAutoProvisioningEnabled enables Node Auto-Provisioning (NAP) on the internal cluster configuration.
func WithClusterAutoProvisioningEnabled() Option[*gke_api_beta.Cluster] {
	return func(c *gke_api_beta.Cluster) *gke_api_beta.Cluster {
		if c.Autoscaling == nil {
			c.Autoscaling = &gke_api_beta.ClusterAutoscaling{}
		}
		c.Autoscaling.EnableNodeAutoprovisioning = true
		return c
	}
}

// WithClusterResourceLimits sets the ResourceLimits.
func WithClusterResourceLimits(limits []*gke_api_beta.ResourceLimit) Option[*gke_api_beta.Cluster] {
	return func(c *gke_api_beta.Cluster) *gke_api_beta.Cluster {
		if c.Autoscaling == nil {
			c.Autoscaling = &gke_api_beta.ClusterAutoscaling{}
		}
		c.Autoscaling.ResourceLimits = limits
		return c
	}
}

// --- Node Pool Option Overrides ---

// WithNodePoolName sets the name for the pool.
func WithNodePoolName(name string) Option[*gke_api_beta.NodePool] {
	return func(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
		np.Name = name
		return np
	}
}

// WithNodePoolSize sets the initial and minimum node count for the pool.
func WithNodePoolSize(size int64) Option[*gke_api_beta.NodePool] {
	return func(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
		np.InitialNodeCount = size
		if np.Autoscaling != nil {
			np.Autoscaling.MinNodeCount = size
		}
		return np
	}
}

// WithNodePoolMachineType sets the machine type for the pool.
func WithNodePoolMachineType(machineType string) Option[*gke_api_beta.NodePool] {
	return func(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
		if np.Config == nil {
			np.Config = &gke_api_beta.NodeConfig{}
		}
		np.Config.MachineType = machineType
		return np
	}
}

// WithNodePoolLocations sets the locations (zones) for the pool.
func WithNodePoolLocations(locations ...string) Option[*gke_api_beta.NodePool] {
	return func(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
		np.Locations = locations
		return np
	}
}

// WithNodePoolSpot configures the node pool to use Spot VMs.
func WithNodePoolSpot() Option[*gke_api_beta.NodePool] {
	return func(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
		if np.Config == nil {
			np.Config = &gke_api_beta.NodeConfig{}
		}
		np.Config.Spot = true
		if np.Config.Labels == nil {
			np.Config.Labels = make(map[string]string)
		}
		np.Config.Labels[gkelabels.ProvisioningLabel] = gkelabels.SpotProvisioningValue
		np.Config.Labels[gkelabels.SpotLabel] = gkelabels.PreemptionValue
		return np
	}
}

// WithNodePoolLocationPolicy sets the location policy for the pool.
func WithNodePoolLocationPolicy(policy string) Option[*gke_api_beta.NodePool] {
	return func(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
		if np.Autoscaling == nil {
			np.Autoscaling = &gke_api_beta.NodePoolAutoscaling{}
		}
		np.Autoscaling.LocationPolicy = policy
		return np
	}
}

// WithFlexStartNodePool configures the node pool for Flex Start workloads.
func WithFlexStartNodePool(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
	if np.Config == nil {
		np.Config = &gke_api_beta.NodeConfig{}
	}
	np.Config.FlexStart = true
	if np.Config.Labels == nil {
		np.Config.Labels = make(map[string]string)
	}
	np.Config.Labels[labels.FlexStartLabel] = labels.FlexStartValue
	np.Config.Labels[labels.ProvisioningLabel] = labels.FlexStartProvisioningValue

	return np
}

// WithTPUConfig configures the node pool for TPU workloads.
func WithTPUConfig(machineType string, accelerator string, topology string, acceleratorCount string, maxNodeCount int64) Option[*gke_api_beta.NodePool] {
	return func(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
		if np.Config == nil {
			np.Config = &gke_api_beta.NodeConfig{}
		}
		np.Config.MachineType = machineType
		if np.Config.Labels == nil {
			np.Config.Labels = make(map[string]string)
		}
		np.Config.Labels[labels.TPULabel] = accelerator
		np.Config.Labels[labels.TPUTopologyLabel] = topology
		np.Config.Labels[labels.AcceleratorCountLabel] = acceleratorCount

		np.Config.Taints = append(np.Config.Taints, &gke_api_beta.NodeTaint{
			Key:    tpu.ResourceGoogleTPU,
			Value:  "present",
			Effect: "NO_SCHEDULE",
		})

		if np.Autoscaling == nil {
			np.Autoscaling = &gke_api_beta.NodePoolAutoscaling{}
		}
		np.Autoscaling.MaxNodeCount = maxNodeCount

		return np
	}
}

// WithNodePoolMax sets the maximum autoscaling node count for the pool.
func WithNodePoolMax(max int64) Option[*gke_api_beta.NodePool] {
	return func(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
		if np.Autoscaling != nil {
			np.Autoscaling.MaxNodeCount = max
		}
		return np
	}
}

// WithNodePoolLabels sets the labels for the node pool.
func WithNodePoolLabels(labels map[string]string) Option[*gke_api_beta.NodePool] {
	return func(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
		if np.Config == nil {
			np.Config = &gke_api_beta.NodeConfig{}
		}
		if np.Config.Labels == nil {
			np.Config.Labels = make(map[string]string)
		}
		for k, v := range labels {
			np.Config.Labels[k] = v
		}
		return np
	}
}

// WithNodePoolAutoscalingEnabled configures the autoscaling enabled state for the pool.
func WithNodePoolAutoscalingEnabled(enabled bool) Option[*gke_api_beta.NodePool] {
	return func(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
		if np.Autoscaling == nil {
			np.Autoscaling = &gke_api_beta.NodePoolAutoscaling{}
		}
		np.Autoscaling.Enabled = enabled
		return np
	}
}

// WithNodePoolMin sets the minimum autoscaling node count for the pool.
func WithNodePoolMin(min int64) Option[*gke_api_beta.NodePool] {
	return func(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
		if np.Autoscaling == nil {
			np.Autoscaling = &gke_api_beta.NodePoolAutoscaling{}
		}
		np.Autoscaling.MinNodeCount = min
		return np
	}
}

// NodePoolBuilder is a wrapper around GKE NodePool.
type NodePoolBuilder struct {
	*gke_api_beta.NodePool
}

// EmptyNodePool creates a NodePoolBuilder with size 0 and min size 0.
func EmptyNodePool(name string) NodePoolBuilder {
	return NodePoolBuilder{
		NodePool: &gke_api_beta.NodePool{
			Name: name,
			Config: &gke_api_beta.NodeConfig{
				MachineType: "n1-standard-2",
				ImageType:   "cos_containerd",
			},
			InitialNodeCount: 0,
			Autoscaling: &gke_api_beta.NodePoolAutoscaling{
				Enabled:      true,
				MinNodeCount: 0,
				MaxNodeCount: 1000,
			},
			Locations: []string{"us-central1-b"},
		},
	}
}

// DefaultNodePoolBuilder creates a NodePoolBuilder with size 1 and min size 1.
func DefaultNodePoolBuilder(name string) NodePoolBuilder {
	return EmptyNodePool(name).WithSize(1)
}

// WithName sets the name of the node pool.
func (b NodePoolBuilder) WithName(name string) NodePoolBuilder {
	b.NodePool.Name = name
	return b
}

// WithSize sets the initial and minimum size of the node pool.
func (b NodePoolBuilder) WithSize(size int64) NodePoolBuilder {
	b.NodePool.InitialNodeCount = size
	if b.NodePool.Autoscaling != nil {
		b.NodePool.Autoscaling.MinNodeCount = size
	}
	return b
}

// WithMax sets the maximum size of the node pool.
func (b NodePoolBuilder) WithMax(max int64) NodePoolBuilder {
	if b.NodePool.Autoscaling != nil {
		b.NodePool.Autoscaling.MaxNodeCount = max
	}
	return b
}

// WithMachineType sets the machine type of the node pool.
func (b NodePoolBuilder) WithMachineType(machineType string) NodePoolBuilder {
	if b.NodePool.Config == nil {
		b.NodePool.Config = &gke_api_beta.NodeConfig{}
	}
	b.NodePool.Config.MachineType = machineType
	return b
}

// WithLocations sets the locations of the node pool.
func (b NodePoolBuilder) WithLocations(locations ...string) NodePoolBuilder {
	b.NodePool.Locations = locations
	return b
}

// Build returns the underlying *gke_api_beta.NodePool.
func (b NodePoolBuilder) Build() *gke_api_beta.NodePool {
	return b.NodePool
}

// DefaultProject returns default project for the BUT config.
func DefaultProject() string {
	return defaultProject
}

// WithDefragEnabled enables defrag with specified plugins.
func WithDefragEnabled(plugins string) Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.InternalOptions.DefragEnabled = true
		o.InternalOptions.DefragPlugins = plugins
		return o
	}
}

// WithDefragScaleDownDelay sets the DefragScaleDownDelay option.
func WithDefragScaleDownDelay(d time.Duration) Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.InternalOptions.DefragScaleDownDelay = d
		return o
	}
}

// WithDefragCandidateLimit sets the DefragCandidateLimit option.
func WithDefragCandidateLimit(limit int) Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.DefragCandidateLimit = limit
		return o
	}
}

// WithMaxDrainParallelism sets the MaxDrainParallelism option.
func WithMaxDrainParallelism(limit int) Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.MaxDrainParallelism = limit
		return o
	}
}

func WithNodePoolInitialNodeCount(count int) Option[*gke_api_beta.NodePool] {
	return func(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
		np.InitialNodeCount = int64(count)
		return np
	}
}

func WithNodePoolMinNodeCount(count int) Option[*gke_api_beta.NodePool] {
	return func(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
		if np.Autoscaling == nil {
			np.Autoscaling = &gke_api_beta.NodePoolAutoscaling{}
		}
		np.Autoscaling.MinNodeCount = int64(count)
		return np
	}
}

func WithNodePoolMaxNodeCount(count int) Option[*gke_api_beta.NodePool] {
	return func(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
		if np.Autoscaling == nil {
			np.Autoscaling = &gke_api_beta.NodePoolAutoscaling{}
		}
		np.Autoscaling.MaxNodeCount = int64(count)
		return np
	}
}

func WithNodePoolTaints(taints ...*gke_api_beta.NodeTaint) Option[*gke_api_beta.NodePool] {
	return func(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
		if np.Config == nil {
			np.Config = &gke_api_beta.NodeConfig{}
		}
		np.Config.Taints = append(np.Config.Taints, taints...)
		return np
	}
}

func WithNodePoolLocation(location string) Option[*gke_api_beta.NodePool] {
	return func(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
		np.Locations = append(np.Locations, location)
		return np
	}
}

func WithNodePoolQueuedProvisioning(enabled bool) Option[*gke_api_beta.NodePool] {
	return func(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
		np.QueuedProvisioning = &gke_api_beta.QueuedProvisioning{
			Enabled: enabled,
		}
		return np
	}
}

func WithNodePoolAccelerators(accelerators ...*gke_api_beta.AcceleratorConfig) Option[*gke_api_beta.NodePool] {
	return func(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
		if np.Config == nil {
			np.Config = &gke_api_beta.NodeConfig{}
		}
		np.Config.Accelerators = append(np.Config.Accelerators, accelerators...)
		return np
	}
}
