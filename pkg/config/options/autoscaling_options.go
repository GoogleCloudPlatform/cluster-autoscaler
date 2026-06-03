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

package options

import (
	"time"

	apiv1 "k8s.io/api/core/v1"

	"k8s.io/autoscaler/cluster-autoscaler/config"
)

type AutoscalingOptions struct {
	config.AutoscalingOptions // OSS options
	InternalOptions
}

type InternalOptions struct {
	ProjectNumber                                int
	Location                                     string
	CccNodeAutoprovisioningEnabled               bool
	MaxAutoprovisionedNodeGroupCount             int
	UseCapacityRequests                          bool
	UseAutoscalerVisibility                      bool
	EmitNoScaleUpCAVizEvents                     bool
	EmitNoScaleDownCAVizEvents                   bool
	IncludePerMigStatusesInCAViz                 bool
	EmitNapInfoInCAViz                           bool
	Profile                                      string
	PvmUnfitnessPenaltyEnabled                   bool
	AutopilotEnabled                             bool
	NodePoolUpdatesEnabled                       bool
	NapDefaultMachineTypeFamily                  string
	EkMachineTypes                               string
	E4aMachineTypes                              string
	EkDownsizeConfig                             string
	E4aDownsizeConfig                            string
	IsClusterUsingDPV1                           bool
	GceEndpoint                                  string
	ClusterScaleToZeroEnabled                    bool
	ClusterScaleToZeroDelay                      time.Duration
	CompactPlacementEnabled                      bool
	MaxCompactPlacementNodes                     map[string]int64
	MaxProvReqBinpackingDuration                 time.Duration
	ProvisioningLabelEnabled                     bool
	AutopilotHigherMaxPodsPerNode                bool
	DefragEnabled                                bool
	DefragPlugins                                string
	DefragMaxDelay                               time.Duration
	DefragCandidateLimit                         int
	DefragCandidateNodeLimit                     int
	DefragScaleUpTimeout                         time.Duration
	DefragScaleDownTimeout                       time.Duration
	DefragScaleDownDelay                         time.Duration
	MaxLoopsBeforeAdmission                      int
	TpuAutoprovisioningEnabled                   bool
	EnableUserAnyZoneSelection                   bool
	SpecificTypeReservationMatchEnabled          bool
	SpecificTypeReservationWithoutMatchEnabled   bool
	ReservationsAnyLocationPolicyOverride        bool
	ExtendedDurationPodsUpgradeNodesTaintPerLoop int
	ReservationBlocksEnabled                     bool
	GCEFlexAdvisorEnabled                        bool
	CSNEnabled                                   bool
	CSNCAFlag                                    CSNStatus
	CSNDefaultRefreshFrequency                   time.Duration
	EkvmsFixerEnabled                            bool
	EkvmsFixerInterval                           time.Duration
	EkvmsConcurrentResizeWorkers                 int
	EkAutoprovisioning                           string
	E4aAutoprovisioning                          string
	EkOnManagedNodesEnabled                      bool
	E4aOnManagedNodesEnabled                     bool
	EkLookaheadMaxWorkloadSeparations            int
	EkLookaheadPodStrategy                       string
	MachineConfigEnabled                         bool
	CvmMachineConfigEnabled                      bool
	MachineConfigRefreshInterval                 time.Duration
	MultiNetworkSupportEnabled                   bool
	AllowlistedSystemLabels                      string
	AllowlistedSystemLabelPatterns               string
	BootDiskSelectorEnabled                      bool
	CpMaxParallelOps                             int
	CpMaxQueuedOps                               int
	MultitenancyEnabled                          bool
	SystemNamespaces                             []string
	FutureReservationsBackoffEnabled             bool
	EnableConsumablePuller                       bool
	ParentProduct                                string
	ClusterHash                                  string
	EkvmsMinVmSize                               apiv1.ResourceList
	EkvmsIncrementStep                           apiv1.ResourceList
	EkvmsAllocationSafetyBuffer                  apiv1.ResourceList
	MachineSerenityLabelsEnabled                 bool
	PendingPodsMetricEnabled                     bool
	ResolveInstanceRefUsingNodePoolLabel         bool
	MetricsPerCccEnabled                         bool
	PendingPodsPerCccMetricEnabled               bool
	ScaleUpPerCccMetricsEnabled                  bool
	EnhancedCrdStatusReporting                   bool
	ZoneTypesEnabled                             bool
	EnableComputeClassMinCapacity                bool
	NapMaxNodes                                  int
	NodeWatchLabelSelector                       string
	NodeWatchFieldSelector                       string
	PodWatchLabelSelector                        string
	PodWatchFieldSelector                        string
}

// CSNStatus represents the status of Cold Standby Nodes feature in CA.
type CSNStatus string

const (
	// CSN is forcibly enabled (regardless of the main experiment value).
	CSNEnabled CSNStatus = "ENABLED"
	// CSN is forcibly disabled (regardless of the main experiment value).
	CSNDisabled CSNStatus = "DISABLED"
	// Default: Uses experiment values.
	CSNUnspecified CSNStatus = "UNSPECIFIED"
)

// Valid names for AutoscalingOptions.ExpanderNames:
var (
	// PriceBasedImprovedExpanderName selects a node group that is the most cost-effective and consistent with
	// the preferred node size for the cluster
	PriceBasedImprovedExpanderName = "gke-price"
	// ScalabilityTestExpanderName selects a node group that has a preferred instance type.
	ScalabilityTestExpanderName = "scalability-test"
	// DefragExpanderName is a defrag expander added automatically if defrag is enabled
	DefragExpanderName = "defrag"
	// SnowflakeExpanderName selects snowflaked node groups, if available.
	SnowflakeExpanderName = "snowflake"
	// EdpExpanderName selects options with edp and lgtpp pods with the smallest
	// machine types for each unique ExtendedDurationPodsLabel. Otherwise, returns all the options without edp pods
	EdpExpanderName = "edp-filter"
	// MaxPodsPerNodeExpanderName filters options with too large max pods per node
	// value taking into account pods that can schedule on the node and current
	// number of nodes in a cluster.
	MaxPodsPerNodeExpanderName = "mppn-filter"
)
