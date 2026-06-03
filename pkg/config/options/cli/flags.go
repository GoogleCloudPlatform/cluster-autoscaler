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

package cli

import (
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	kube_flag "k8s.io/component-base/cli/flag"
	"k8s.io/klog/v2"

	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/config/flags"
	"k8s.io/autoscaler/cluster-autoscaler/utils/scheduler"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/localssdsize"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/parsing"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	resizable_types "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
	internalmetrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
)

func MapStringStringFlag(name string, defaultValue *map[string]string, usage string) *map[string]string {
	flag.Var(kube_flag.NewMapStringString(defaultValue), name, usage)
	return defaultValue
}

// K8s type flags; requirest flagset
// ref: k8s.io/component-base/cli/flag
// Default values for custom typed flags
var (
	ekvmsMinVmSizeDefault        = map[string]string{"cpu": "2", "memory": "4Gi"}
	ekvmsIncrementStepDefault    = map[string]string{"cpu": "2", "memory": "1Mi"}
	ekvmsAllocationSafetyDefault = map[string]string{"cpu": "0", "memory": "500Mi"}
)

var (
	projectNumber                                = flag.Int("project-number", 0, "Project number")
	location                                     = flag.String("location", "", "Cluster location")
	cccNodeAutoprovisioningEnabled               = flag.Bool("ccc-node-autoprovisioning-enabled", false, "Should CA enable autoprovisioning for CCC node groups")
	maxAutoprovisionedNodeGroupCount             = flag.Int("max-autoprovisioned-node-group-count", 15, "The maximum number of autoprovisioned groups in the cluster.")
	useCapacityRequests                          = flag.Bool("use-capacity-requests", false, "Should CA use Capacity Requests information when running simulations for the cluster.")
	useAutoscalerVisibility                      = flag.Bool("use-ca-viz", false, "Should CA emit visibility events.")
	emitNoScaleUpCAVizEvents                     = flag.Bool("ca-viz-no-scale-up", false, "Should CA visibility emit events representing a decision NOT to scale up (NoScaleUp events).")
	emitNoScaleDownCAVizEvents                   = flag.Bool("ca-viz-no-scale-down", false, "Should CA visibility emit events representing a decision NOT to scale down (NoScaleDown events).")
	includePerMigStatusesInCAViz                 = flag.Bool("ca-viz-per-mig-statuses", false, "Should CA visibility status events include per-MIG statuses.")
	emitNapInfoInCAViz                           = flag.Bool("ca-viz-nap-info", false, "Should CA visibility emit information related to NAP.")
	profile                                      = flag.String("profile", "default", "Name of autoscaling profile to expose as metric")
	pvmUnfitnessPenaltyEnabled                   = flag.Bool("pvm-unfitness-penalty-enabled", false, "Should CA add a penalty towards creation of unfit pvms")
	autopilotEnabled                             = flag.Bool("enable-autopilot", false, "Is Autoscaling running in GKE Autopilot mode")
	nodePoolUpdatesEnabled                       = flag.Bool("enable-node-pool-updates", false, "Are node-pool updates enabled")
	napDefaultMachineTypeFamily                  = flag.String("nap-default-machine-type-family", "n1", "The machine type family that is used by autoprovisioning by default")
	ekMachineTypes                               = flag.String("ek-machine-types", "", "A comma-separated list of EK machine types to override EK machine types supported by NAP. Empty (default value) means to use the default (no override).")
	e4aMachineTypes                              = flag.String("e4a-machine-types", "", "A comma-separated list of E4A machine types to override E4A machine types supported by NAP. Empty (default value) means to use the default (no override).")
	ekDownsizeConfig                             = flag.String("ek-downsize-config", "{}", "An override for DownsizeConfig in json format for EK.")
	e4aDownsizeConfig                            = flag.String("e4a-downsize-config", "{}", "An override for DownsizeConfig in json format for E4A.")
	isClusterUsingDPV1                           = flag.Bool("is-cluster-using-dpv1", false, "Is the cluster using dataplane v1 (i.e. kube-proxy)")
	gceEndpoint                                  = flag.String("gce-endpoint", "", "GCE endpoint address. If not set default GCE API endpoint is used.")
	clusterScaleToZeroEnabled                    = flag.Bool("enable-cluster-scale-to-zero", false, "Whether the cluster can be scaled to 0 nodes if all pods are in kube-system namespace")
	clusterScaleToZeroDelay                      = flag.Duration("cluster-scale-to-zero-delay", 3*time.Hour, "Additional delay before cluster is scaled to 0 nodes. Only used if cluster scale to 0 is enabled.")
	compactPlacementEnabled                      = flag.Bool("enable-compact-placement", false, "Whether node pools with compact placement can be provisioned")
	maxCompactPlacementNodes                     = flag.String("max-compact-placement-nodes", "", "Maximum number of nodes for a node pool on each machine family when compact placement is enabled, in a format xx:aa,yy:bb where xx, yy are machine family names, and aa, bb are node maximas")
	maxProvReqBinpackingDuration                 = flag.Duration("max-provreq-binpacking-duration", 4*time.Minute, "Maximum time that will be spent in binpacking simulation for each Provisioning Request. If the threshold is exceeded binpacking will be cut short and a Provisioning Request will fail.")
	provisioningLabelEnabled                     = flag.Bool("enable-provisioning-label", false, "Enables handling of the gke-provisioning system label.")
	autopilotHigherMaxPodsPerNode                = flag.Bool("autopilot-higher-max-pods-per-node", false, "If true NAP sets a higher max pods per node step function based on VM size")
	defragEnabled                                = flag.Bool("enable-defrag", false, "Whether Defrag should be enabled.")
	defragPlugins                                = flag.String("defrag-plugins", "", "List of Defrag plugins used, separated by comma.")
	defragCandidateLimit                         = flag.Int("defrag-candidate-limit", 10, "Defrag limit for the number of candidates tracked simultaneously.")
	defragCandidateNodeLimit                     = flag.Int("defrag-candidate-node-limit", 1, "Defrag limit for the number of nodes in a candidate.")
	defragMaxDelay                               = flag.Duration("defrag-max-delay", 5*time.Minute, "Maximum delay between each defrag run.")
	defragScaleUpTimeout                         = flag.Duration("defrag-scale-up-timeout", 10*time.Minute, "Defrag timeout for scale-up of candidate nodes.")
	defragScaleDownTimeout                       = flag.Duration("defrag-scale-down-timeout", 5*time.Minute, "Defrag timeout for scale-down of candidate nodes.")
	defragScaleDownDelay                         = flag.Duration("defrag-scale-down-delay", 0, "Defrag delay after which candidate nodes can be scaled down.")
	maxLoopsBeforeAdmission                      = flag.Int("max-loops-before-admission", 5, "The number of loops between runs of any processor sharing the same fairness enforcer")
	tpuAutoprovisioningEnabled                   = flag.Bool("enable-tpu-autoprovisioning", false, "Whether NAP wil try to autoprovision TPU nodepools.")
	enableUserAnyZoneSelection                   = flag.Bool("enable-user-any-zone-selection", false, "Whether user can specify any zone (eg. via CCC, zone node selector) for their workload. NAP should respect this zone even if it is not part of Autoprovisioning Locations.")
	specificTypeReservationMatchEnabled          = flag.Bool("enable-reservation-match", false, "Whether NAP will look up user configured reservations and match specific type reserved machines.")
	specificTypeReservationWithoutMatchEnabled   = flag.Bool("enable-specific-type-reservation-without-match", false, "Whether NAP will allow specific type reservations without matching in enable-reservation-match.")
	reservationsAnyLocationPolicyOverride        = flag.Bool("enable-reservations-any-location-policy-override", false, "Whether NAP will use ANY location policy in injected node groups using reservations. It is used to query Recommend Locations API before every scale-up, as it has the knowledge about all (even cross org) reservations.")
	extendedDurationPodsUpgradeNodesTaintPerLoop = flag.Int("extended-duration-pods-max-taint-per-loop", 10, "Number of EDP nodes that will be tainted during upgrade per loop")
	reservationBlocksEnabled                     = flag.Bool("enable-reservation-blocks", false, "Fetch reservation blocks from GCE reservations to be used at NAP validation phase.")
	gceFlexAdvisorEnabled                        = flag.Bool("enable-gce-flexadvisor", false, "Whether GCE Flex Advisor should be enabled.")
	csnStatus                                    = flag.String("csn-status", string(options.CSNUnspecified), "Whether CSN is force enabled, force disabled or use experiment values (go/gke-csn-launch).")
	csnDefaultRefreshFrequency                   = flag.Duration("csn-default-refresh-frequency", 24*time.Hour, `The default frequency for refreshing CSN nodes if not specified in the buffer annotation or an error occured during parsing.`)
	ekvmsFixerEnabled                            = flag.Bool("enable-ekvms-fixer", false, "Whether EK VM fixer is enabled (go/gke-ek-vm-balloon-pod-error-handling).")
	ekvmsFixerInterval                           = flag.Duration("ekvms-fixer-interval", 10*time.Second, "How often EK VM fixer loop is running (go/gke-ek-vm-balloon-pod-error-handling).")
	ekvmsConcurrentResizeWorkers                 = flag.Int("ekvms-concurrent-resize-workers", 10, "Number of concurrent workers that will execute EK VMs resize operations.")
	ekAutoprovisioning                           = flag.String("ek-autoprovisioning", string(resizable_types.EkAutoprovisioningUnspecified), "Specifies if EKs are enabled and if so, which resizing mode is used. If unspecified, can be overridden by experiment.")
	e4aAutoprovisioning                          = flag.String("e4a-autoprovisioning", string(resizable_types.E4aAutoprovisioningUnspecified), "Specifies if E4As are enabled and if so, which resizing mode is used. If unspecified, can be overridden by experiment.")
	ekLookaheadMaxWorkloadSeparations            = flag.Int("ek-lookahead-max-workload-separations", 0, "Maximum number of workload separations that will be supported by EK lookahead buffers.")
	ekLookaheadPodStrategy                       = flag.String("ek-lookahead-pod-strategy", "{}", "Single-quote wrapped JSON string representing EK lookahead pod strategy.")
	ekOnManagedNodesEnabled                      = flag.Bool("enable-ek-on-managed-nodes", false, "Specifies if EKs are enabled on managed nodes.")
	e4aOnManagedNodesEnabled                     = flag.Bool("enable-e4a-on-managed-nodes", false, "Specifies if E4As are enabled on managed nodes.")
	multiNetworkSupportEnabled                   = flag.Bool("multi-network-support-enabled", false, "Enables support for multi network pods.")
	allowlistedSystemLabels                      = flag.String("allowlisted-system-labels", "", "[DEPRECATED] Use --allowlisted-system-label-patterns instead.")
	allowlistedSystemLabelPatterns               = flag.String("allowlisted-system-label-patterns", "", "Comma separated list of allow listed system regex label patterns to be passed by NAP.")
	bootDiskSelectorEnabled                      = flag.Bool("boot-disk-nap-enabled", false, "Enables NAP support for boot disk config.")
	cpMaxParallelOps                             = flag.Int("cp-max-parallel-ops", 9, "The max number of GKE CP parallel operations. Operations excceeding the limit are queued by CA in memory. It's used for async node group creations and deletions. See: go/ht-nap")
	cpMaxQueuedOps                               = flag.Int("cp-max-queued-ops", 40, "The max number of queued GKE CP operations. It's used for async node group creations and deletions. See: go/ht-nap")
	multitenancyEnabled                          = flag.Bool("multitenancy-enabled", false, "Enables support for GKE Multitenancy")
	systemNamespaces                             = flag.String("system-namespaces", "", "Comma separated list of system namepsaces.")
	futureReservationsBackoffEnabled             = flag.Bool("enable-future-reservations-backoff", false,
		"Enable backoff to future reservation start time when a specific named reservation used for node-pool refers to an existing future reservation")
	enableConsumablePuller               = flag.Bool("enable-consumable-reservations-puller", false, "Whether to pull reservations from the Beta ListConsumableReservations API.")
	machineSerenityLabelsEnabled         = flag.Bool("machine-serenity-labels-enabled", false, "Enables the inclusion of disk support labels on simulated nodes based on machine source-of-truth information.")
	pendingPodsMetricEnabled             = flag.Bool("enable-pending-pods-metric", false, "Enables registration of pendingPodsCollector and pending_pod metric in an effect")
	resolveInstanceRefUsingNodePoolLabel = flag.Bool("resolve-instanceref-using-nodepool-label", true, "If true, will attempt to find nodegroup for node by matching node's cloud.google.com/gke-nodepool label if ProviderID is empty. See go/gke-ca-nil-providerid-logs for details.")
	zoneTypesEnabled                     = flag.Bool("enable-zone-types", false, "Enables automatic AI zones selection via CCC zoneTypes field, see go/gke-auto-ai-zones for details.")
	enableComputeClassMinCapacity        = flag.Bool("enable-compute-class-min-capacity", false, "Enables Compute Class minimum capacity support.")
	napMaxNodes                          = flag.Int("nap-max-nodes", 1000, "The max number of nodes per zone in autoprovisioned node pools.")

	// See go/gke-ca-perf-node-pod-watch-selector, read Risks sections before using
	nodeWatchLabelSelector = flag.String("node-watch-label-selector", "", "Label selector to filter nodes in informer watch, defaults to all nodes.")
	nodeWatchFieldSelector = flag.String("node-watch-field-selector", "", "Field selector to filter nodes in informer watch, defaults to all nodes.")
	podWatchLabelSelector  = flag.String("pod-watch-label-selector", "", "Label selector to filter pods in informer watch, defaults to all pods.")
	podWatchFieldSelector  = flag.String("pod-watch-field-selector", "", "Field selector to filter pods in informer watch, defaults to all pods.")

	ekvmsMinVmSize              = MapStringStringFlag("ekvms-min-vm-size", &ekvmsMinVmSizeDefault, fmt.Sprintf("A set of ResourceName=ResourceQuantity (e.g. cpu=200m,memory=500Mi) pairs that describe resources used by fully downsized VM. Currently only cpu and memory are supported. [default=%v]", parsing.ResourceListString(ekvmsMinVmSizeDefault)))
	ekvmsIncrementStep          = MapStringStringFlag("ekvms-increment-step", &ekvmsIncrementStepDefault, fmt.Sprintf("A set of ResourceName=ResourceQuantity (e.g. cpu=200m,memory=500Mi) pairs that describe single resize step resources. Currently only cpu and memory are supported. [default=%v]", parsing.ResourceListString(ekvmsIncrementStepDefault)))
	ekvmsAllocationSafetyBuffer = MapStringStringFlag("ekvms-allocation-safety-buffer", &ekvmsAllocationSafetyDefault, fmt.Sprintf("A set of ResourceName=ResourceQuantity (e.g. cpu=200m,memory=500Mi) pairs that describe resources to be used as a safety buffer in EK VMs node Allocatable calculator. Currently only cpu and memory are supported. See https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/ for more detail. [default=%v]", parsing.ResourceListString(ekvmsAllocationSafetyDefault)))

	machineConfigEnabled         = flag.Bool("machine-config-enabled", false, "Whether to enable MachineConfig CRD puller")
	cvmMachineConfigEnabled      = flag.Bool("cvm-machine-config-enabled", false, "Whether to enable parsing of CVM capabilities from MachineConfig CRD. Requires --machine-config-enabled to be true.")
	machineConfigRefreshInterval = flag.Duration("machine-config-refresh-interval", 5*time.Minute, "Interval of refreshing machine config")

	metricsPerCccEnabled           = flag.Bool("metrics-per-ccc-enabled", false, "Enables the emitting metrics per ccc")
	pendingPodsPerCccMetricEnabled = flag.Bool("enable-pending-pods-per-ccc-metric", false, "Whether emmiting metrics pending pods per ccc metric should be enabled.")
	scaleUpPerCccMetricsEnabled    = flag.Bool("scaleup-per-ccc-metrics-enabled", false, "Whether emitting the scale up metrics per ccc should be enabled.")

	enhancedCrdStatusReporting = flag.Bool("enhanced-crd-status-reporting", false, "Whether to enable enhanced CRD status reporting.")

	parentProduct = flag.String("parent-product", "", "Parent product as a division property for experiments.")
	clusterHash   = flag.String("cluster-hash", "", "The cluster's hash")
)

// ComponentVersion returns cluster autoscaler component version.
func ComponentVersion() version.Version {
	vs, found := os.LookupEnv(internalmetrics.ComponentVersionEnvVar)
	if !found {
		klog.Fatalf("Couldn't find component version in %s", internalmetrics.ComponentVersionEnvVar)
	}
	v, err := version.FromString(vs)
	if err != nil {
		klog.Fatalf("Component version %s is not a correct version: %v", vs, err)
	}
	return v
}

func OssOptionsFromFlags() config.AutoscalingOptions {
	genericOptions := flags.AutoscalingOptions()

	// Enable bypassing schedulers only when proactive-scaleups is enabled
	if !genericOptions.ProactiveScaleupEnabled {
		genericOptions.BypassedSchedulers = scheduler.SchedulersMap([]string{})
	} else {
		klog.Infof("Proactive scaleup is enabled, allowing scheduler bypassing")
		// BypassedSchedulers is already set in the AutoscalingOptions() call above
	}

	// Add the defrag taints to the list of status taints
	allStatusTaints := genericOptions.StatusTaints
	allStatusTaints = append(allStatusTaints, defrag.HardTaint, defrag.SoftTaint)
	// Ignore taints we apply during Balloon Pod resizes.
	allStatusTaints = append(allStatusTaints, resizable_types.BPResizeTaint.Key)
	// Ignore taints we apply for CSN nodes.
	allStatusTaints = append(allStatusTaints, csn.BufferAssignmentKey)
	allStatusTaints = append(allStatusTaints, csn.SuspendedTaintKey)
	genericOptions.StatusTaints = allStatusTaints

	// Update ExpendablePodsPriorityCutoff if autopilot is enabled
	if *autopilotEnabled {
		t := -math.MaxInt32
		if strconv.IntSize == 64 {
			t = -math.MaxInt64
		}
		genericOptions.ExpendablePodsPriorityCutoff = t
	}

	if genericOptions.AsyncNodeGroupsEnabled && *cpMaxParallelOps < 1 {
		klog.Fatalf("Async node pools require max GKE CP concurrent ops > 0, got: %d", *cpMaxParallelOps)
	}

	if err := parsing.ValidateMppnFlags(genericOptions.ExpanderNames); err != nil {
		klog.Fatalf("max pods per node flags invalid: %v", err)
	}

	genericOptions.GCEOptions.LocalSSDDiskSizeProvider = localssdsize.NewDynamicLocalSSDDiskSizeProvider(machinetypes.LocalSSDDiskSizes)
	if !genericOptions.FrequentLoopsEnabled {
		klog.Fatalf("Running without FrequentLoopsEnabled option is no longer supported.")
	}

	// GKE clusterautoscaler always uses ProvReqs, forcing the flag on
	genericOptions.ProvisioningRequestEnabled = true

	return genericOptions
}

func InternalOptsFromFlags() internalopts.InternalOptions {
	// ref: go/ek-node-allocatable
	ekMinVmSize, err := parsing.ParseResourceList(*ekvmsMinVmSize)
	if err != nil {
		klog.Fatalf("--ekvms-min-vm-size value failed to parse: %v", err)
	}
	ekIncrementStep, err := parsing.ParseResourceList(*ekvmsIncrementStep)
	if err != nil {
		klog.Fatalf("--ekvms-increment-step value failed to parse: %v", err)
	}
	safetyBuffer, err := parsing.ParseResourceList(*ekvmsAllocationSafetyBuffer)
	if err != nil {
		klog.Fatalf("--ekvms-allocation-safety-buffer value failed to parse: %v", err)
	}

	switch options.CSNStatus(*csnStatus) {
	case options.CSNUnspecified, options.CSNEnabled, options.CSNDisabled:
	default:
		klog.Fatalf("Invalid value for --csn-status: %s", *csnStatus)

	}

	parsedMaxCompactPlacementNodes, err := parsing.ParseMultipleNodeLimits(*maxCompactPlacementNodes)
	if err != nil {
		klog.Fatalf("Failed to parse compact placement flag: %v", err)
	}

	return internalopts.InternalOptions{
		ProjectNumber:                                *projectNumber,
		Location:                                     *location,
		CccNodeAutoprovisioningEnabled:               *cccNodeAutoprovisioningEnabled,
		MaxAutoprovisionedNodeGroupCount:             *maxAutoprovisionedNodeGroupCount,
		UseCapacityRequests:                          *useCapacityRequests,
		UseAutoscalerVisibility:                      *useAutoscalerVisibility,
		EmitNoScaleUpCAVizEvents:                     *emitNoScaleUpCAVizEvents,
		EmitNoScaleDownCAVizEvents:                   *emitNoScaleDownCAVizEvents,
		IncludePerMigStatusesInCAViz:                 *includePerMigStatusesInCAViz,
		EmitNapInfoInCAViz:                           *emitNapInfoInCAViz,
		Profile:                                      *profile,
		PvmUnfitnessPenaltyEnabled:                   *pvmUnfitnessPenaltyEnabled,
		AutopilotEnabled:                             *autopilotEnabled,
		NodePoolUpdatesEnabled:                       *nodePoolUpdatesEnabled,
		NapDefaultMachineTypeFamily:                  *napDefaultMachineTypeFamily,
		EkMachineTypes:                               *ekMachineTypes,
		E4aMachineTypes:                              *e4aMachineTypes,
		EkDownsizeConfig:                             *ekDownsizeConfig,
		E4aDownsizeConfig:                            *e4aDownsizeConfig,
		IsClusterUsingDPV1:                           *isClusterUsingDPV1,
		GceEndpoint:                                  *gceEndpoint,
		ClusterScaleToZeroEnabled:                    *clusterScaleToZeroEnabled,
		ClusterScaleToZeroDelay:                      *clusterScaleToZeroDelay,
		CompactPlacementEnabled:                      *compactPlacementEnabled,
		MaxCompactPlacementNodes:                     parsedMaxCompactPlacementNodes,
		MaxProvReqBinpackingDuration:                 *maxProvReqBinpackingDuration,
		ProvisioningLabelEnabled:                     *provisioningLabelEnabled,
		AutopilotHigherMaxPodsPerNode:                *autopilotHigherMaxPodsPerNode,
		DefragEnabled:                                *defragEnabled,
		DefragPlugins:                                *defragPlugins,
		DefragCandidateLimit:                         *defragCandidateLimit,
		DefragCandidateNodeLimit:                     *defragCandidateNodeLimit,
		DefragMaxDelay:                               *defragMaxDelay,
		DefragScaleUpTimeout:                         *defragScaleUpTimeout,
		DefragScaleDownTimeout:                       *defragScaleDownTimeout,
		DefragScaleDownDelay:                         *defragScaleDownDelay,
		MaxLoopsBeforeAdmission:                      *maxLoopsBeforeAdmission,
		TpuAutoprovisioningEnabled:                   *tpuAutoprovisioningEnabled,
		EnableUserAnyZoneSelection:                   *enableUserAnyZoneSelection,
		SpecificTypeReservationMatchEnabled:          *specificTypeReservationMatchEnabled,
		SpecificTypeReservationWithoutMatchEnabled:   *specificTypeReservationWithoutMatchEnabled,
		ReservationsAnyLocationPolicyOverride:        *reservationsAnyLocationPolicyOverride,
		ExtendedDurationPodsUpgradeNodesTaintPerLoop: *extendedDurationPodsUpgradeNodesTaintPerLoop,
		ReservationBlocksEnabled:                     *reservationBlocksEnabled,
		GCEFlexAdvisorEnabled:                        *gceFlexAdvisorEnabled,
		CSNCAFlag:                                    options.CSNStatus(*csnStatus),
		CSNDefaultRefreshFrequency:                   *csnDefaultRefreshFrequency,
		EkvmsFixerEnabled:                            *ekvmsFixerEnabled,
		EkvmsFixerInterval:                           *ekvmsFixerInterval,
		EkvmsConcurrentResizeWorkers:                 *ekvmsConcurrentResizeWorkers,
		EkAutoprovisioning:                           *ekAutoprovisioning,
		E4aAutoprovisioning:                          *e4aAutoprovisioning,
		EkOnManagedNodesEnabled:                      *ekOnManagedNodesEnabled,
		E4aOnManagedNodesEnabled:                     *e4aOnManagedNodesEnabled,
		EkLookaheadMaxWorkloadSeparations:            *ekLookaheadMaxWorkloadSeparations,
		EkLookaheadPodStrategy:                       *ekLookaheadPodStrategy,
		MachineConfigEnabled:                         *machineConfigEnabled,
		CvmMachineConfigEnabled:                      *cvmMachineConfigEnabled,
		MachineConfigRefreshInterval:                 *machineConfigRefreshInterval,
		MultiNetworkSupportEnabled:                   *multiNetworkSupportEnabled,
		AllowlistedSystemLabels:                      *allowlistedSystemLabels,
		AllowlistedSystemLabelPatterns:               *allowlistedSystemLabelPatterns,
		BootDiskSelectorEnabled:                      *bootDiskSelectorEnabled,
		CpMaxParallelOps:                             *cpMaxParallelOps,
		CpMaxQueuedOps:                               *cpMaxQueuedOps,
		MultitenancyEnabled:                          *multitenancyEnabled,
		SystemNamespaces:                             parsing.GetSystemNamespaces(*systemNamespaces),
		FutureReservationsBackoffEnabled:             *futureReservationsBackoffEnabled,
		ClusterHash:                                  *clusterHash,
		ParentProduct:                                *parentProduct,
		EkvmsMinVmSize:                               ekMinVmSize,
		EkvmsIncrementStep:                           ekIncrementStep,
		EkvmsAllocationSafetyBuffer:                  safetyBuffer,
		EnableConsumablePuller:                       *enableConsumablePuller,
		MachineSerenityLabelsEnabled:                 *machineSerenityLabelsEnabled,
		PendingPodsMetricEnabled:                     *pendingPodsMetricEnabled,
		ResolveInstanceRefUsingNodePoolLabel:         *resolveInstanceRefUsingNodePoolLabel,
		MetricsPerCccEnabled:                         *metricsPerCccEnabled,
		PendingPodsPerCccMetricEnabled:               *pendingPodsPerCccMetricEnabled,
		EnhancedCrdStatusReporting:                   *enhancedCrdStatusReporting,
		ScaleUpPerCccMetricsEnabled:                  *scaleUpPerCccMetricsEnabled,
		ZoneTypesEnabled:                             *zoneTypesEnabled,
		EnableComputeClassMinCapacity:                *enableComputeClassMinCapacity,
		NapMaxNodes:                                  *napMaxNodes,
		NodeWatchLabelSelector:                       *nodeWatchLabelSelector,
		NodeWatchFieldSelector:                       *nodeWatchFieldSelector,
		PodWatchLabelSelector:                        *podWatchLabelSelector,
		PodWatchFieldSelector:                        *podWatchFieldSelector,
	}
}
