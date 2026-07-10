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

package metrics

import (
	"math"
	"os"
	"strconv"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/metrics"
	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	_ "k8s.io/component-base/metrics/prometheus/workqueue"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	vispb "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/proto"
	"k8s.io/klog/v2"
)

// TaskQueuePhase describes the task queue phase.
type TaskQueuePhase string

// TaskResult describes the result of a task execution.
type TaskResult string

// FACacheQueryResult describes Flex Advisor cache query result.
type FACacheQueryResult string

// KeyGenerationState describes the state of the key generation for Flex Advisor.
type KeyGenerationState string

// CccState describes the state of the key generation for Flex Advisor.
type CccState string

const (
	// KeyGenerationStateNotGenerated represents a key that was not generated.
	KeyGenerationStateNotGenerated KeyGenerationState = "not_generated"
	// KeyGenerationStateGeneratedButCapped represents a key that was generated but capped due to limit.
	KeyGenerationStateGeneratedButCapped KeyGenerationState = "generated_but_capped"
	// KeyGenerationStateGeneratedAndSent represents a key that was generated and sent to the backend.
	KeyGenerationStateGeneratedAndSent KeyGenerationState = "generated_and_sent"
)

const (
	// CccStateEmpty represents a key that was not generated.
	CccStateEmpty CccState = ""
	// CccStateStale represents a key that was not generated.
	CccStateStale CccState = "stale"
)

// FADecisionFeedbackCategory describes Flex Advisor decision feedback category.
type FADecisionFeedbackCategory string

// FAGenerationErrorReason describes the reason for a Flex Advisor generation error.
type FAGenerationErrorReason string

const (
	// ZeroConfigsGeneratedForRule represents generating zero instance configurations for a single CCC rule.
	ZeroConfigsGeneratedForRule FAGenerationErrorReason = "zero_configs_generated_for_rule"
)

// FAResponseErrorReason describes the reason for a Flex Advisor response error.
type FAResponseErrorReason string

const (
	// ResponseMissingInstanceConfig represents a missing instance configuration in backend response.
	ResponseMissingInstanceConfig FAResponseErrorReason = "response_missing_instance_config"
	// ResponseMissingZone represents a missing zone in backend response.
	ResponseMissingZone FAResponseErrorReason = "response_missing_zone"
	// InvalidInstanceCount represents a negative instance count in backend response.
	InvalidInstanceCount FAResponseErrorReason = "invalid_instance_count"
	// InvalidPreferenceScore represents an out-of-bounds preference score in backend response.
	InvalidPreferenceScore FAResponseErrorReason = "invalid_preference_score"
)

// OperationStatus says whether an operation succeeded or failed.
type OperationStatus string

// ReactionType defines the type of reaction CA has for a pod.
type ReactionType uint8

const (
	NoReaction ReactionType = iota
	ScaleUp
	EkUpsize
	Deleted
	Unhelpable
	Scheduled
	NoActionNeeded
	Timeout // Timeout indicates that CA didn't react to the pod within a predefined time threshold (MaxReactionTime).
)

var reactionTypeToString = map[ReactionType]string{
	ScaleUp:        "scale_up",
	EkUpsize:       "ek_upsize",
	Deleted:        "deleted",
	Unhelpable:     "unhelpable",
	Scheduled:      "scheduled",
	NoActionNeeded: "no_action_needed",
	Timeout:        "timeout",
}

// DurationBuckets1sTo24h provides granular buckets for short durations
// and wider gaps for long-running tasks up to 24 hours.
var DurationBuckets1sTo24h = []float64{
	1, 3, 5, 10, 15, 20, 30, 45, 60, 75, 90, 120, 150, 180,
	210, 240, 270, 300, 360, 420, 480, 540, 600, 750, 900,
	1800, 3600, 7200, 14400, 28800, 86400,
}

// String returns the string representation of a ReactionType for logging and metrics.
func (rt ReactionType) String() string {
	if s, ok := reactionTypeToString[rt]; ok {
		return s
	}
	klog.Warningf("ReactionType.String: Unknown ReactionType: %d", rt)
	return "unknown"
}

// PodType describes the type of a pod.
type PodType uint8

const (
	// RealPodType represents a real pod.
	RealPodType PodType = iota
	// CapacityBufferPodType represents a fake pod injected by capacity buffer.
	CapacityBufferPodType
)

var podTypeToString = map[PodType]string{
	RealPodType:           "",
	CapacityBufferPodType: "capacity_buffer",
}

// String returns the string representation of a PodType.
func (pt PodType) String() string {
	return podTypeToString[pt]
}

const (
	caNamespace       = "cluster_autoscaler"
	gkeStandardLabel  = "gke_standard"
	gkeAutopilotLabel = "gke_autopilot"

	// CaVizStatus represents the CA Viz autoscaling status processor execution.
	CaVizStatus metrics.FunctionLabel = "caViz:status"
	// CaVizScaleUp represents the CA Viz scale-up status processor execution.
	CaVizScaleUp metrics.FunctionLabel = "caViz:scaleUp"
	// CaVizScaleDown represents the CA Viz scale-down status processor execution.
	CaVizScaleDown metrics.FunctionLabel = "caViz:scaleDown"
	// QueuePhase represents waiting phase in task queue before execution.
	QueuePhase TaskQueuePhase = "queue"
	// ExecutionPhase represents execution phase in task queue.
	ExecutionPhase TaskQueuePhase = "execution"
	// TaskSucceeded represents the state of tasks executed successfully from the task queue.
	TaskSucceeded TaskResult = "succeeded"
	// TaskFailed represents the state of tasks failed during execution from the task queue.
	TaskFailed TaskResult = "failed"
	// FACacheMissNoZone represents the case where zone is missing in the cached snapshot of the instance config key.
	FACacheMissNoZone FACacheQueryResult = "cache_miss_no_zone"
	// FACacheMissNoInstanceConfigKey represents the case where instance config key is not in the cache.
	FACacheMissNoInstanceConfigKey FACacheQueryResult = "cache_miss_no_instance_config_key"
	// FACacheMissFetchFailed represents the case where instance config key is not in the cache and the first cache fetch failed.
	FACacheMissFetchFailed FACacheQueryResult = "cache_miss_fetch_failed"
	// FACacheHit represents the case where Flex Advisor cache hit.
	FACacheHit FACacheQueryResult = "cache_hit"
	// FADecisionFeedbackSent represents the number of FA feedback decisions sent to GCE.
	FADecisionFeedbackSent FADecisionFeedbackCategory = "sent"
	// FADecisionFeedbackDropped represents the number of FA feedback decisions dropped without sending to GCE.
	FADecisionFeedbackDropped FADecisionFeedbackCategory = "dropped"
	// FADecisionFeedbackError represents the number of FA feedback decisions ended in error when attempting to send to GCE.
	FADecisionFeedbackError FADecisionFeedbackCategory = "error"
	// OperationSucceeded represents a status of a successful operation.
	OperationSucceeded OperationStatus = "succeeded"
	// OperationFailed represents a status of a failed operation.
	OperationFailed OperationStatus = "failed"
	// ComponentVersionEnvVar is the name of an environment variable holding the version of the clusterautoscaler component.
	// The variable is set in the Cluster Autoscaler manifest.
	ComponentVersionEnvVar = "CLUSTERAUTOSCALER_VERSION"

	// Threshold after which we report timeout for a pod for which CA didn't react.
	MaxReactionTime = 60 * time.Minute
)

type LAPodNodeShape struct {
	NodeSizeAllocatable   size.Allocatable
	UserWorkloadPodsCount int
}

var (
	profile = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "profile",
			Help:      "Whether or not a given autoscaling profile is enabled. 1 if it is, 0 otherwise.",
		},
		[]string{"name"},
	)

	clusterType = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "cluster_type",
			Help:      "Whether or not Autopilot is enabled. 1 if it is, 0 otherwise.",
		},
		[]string{"cluster_type"},
	)

	podShardCount = k8smetrics.NewGauge(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "pod_shard_count",
			Help:      "Number of shards for currently unschedulable pods.",
		},
	)

	unschedulablePodDuration = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Namespace: caNamespace,
			Name:      "unschedulable_pod_duration_seconds",
			Help:      "How long it takes for an unschedulable pod to become schedulable. Note that this measures time until pod is possible to schedule, not when it's actually scheduled.",
			Buckets:   DurationBuckets1sTo24h,
		},
		[]string{"gpu", "tpu", "out_of_resource", "placement_type", "consuming_provisioning_request", "device_allocation_mode"},
	)

	podSchedulingDuration = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Namespace: caNamespace,
			Name:      "pod_scheduling_duration_seconds",
			Help:      "How long it takes for an initially unschedulable pod to actually be scheduled.",
			Buckets:   DurationBuckets1sTo24h,
		},
		[]string{"out_of_resource", "device_allocation_mode"},
	)

	reactionTimeMilliseconds = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Namespace: caNamespace,
			Name:      "reaction_time_milliseconds",
			Help:      "How long it takes for CA to react for a new pod.",
			Buckets:   k8smetrics.ExponentialBucketsRange(1, float64(MaxReactionTime.Milliseconds()), 50),
		},
		[]string{"system_pod", "has_pvc", "has_csi", "reaction_type", "fake_pod_type", "capacity_buffer_provisioning_strategy", "device_allocation_mode"},
	)

	longUnschedulablePodsCount = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "long_unschedulable_pods_count",
			Help:      "Number of pods that remained pending for over 1 hour, despite scale-up being possible",
		},
		[]string{"gpu", "tpu", "out_of_resource", "placement_type", "consuming_provisioning_request", "device_allocation_mode"},
	)

	podSchedulableResets = k8smetrics.NewCounter(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "pod_schedulable_resets_total",
			Help:      "How many times pods switched from being considered schedulable to unschedulable.",
		},
	)

	caVizLoggedBytes = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Namespace: caNamespace,
			Name:      "ca_viz_logged_bytes",
			Help:      "How many bytes of Cluster Autoscaler Visibility logs have been logged.",
			Buckets:   []float64{100, 250, 750, 1000, 1500, 2000, 3000, 5000, 7500, 10000, 15000, 20000, 25000, 30000, 50000, 100000, 1000000},
		},
		[]string{"event_type"},
	)

	caVizDroppedEventTotal = k8smetrics.NewCounter(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "ca_viz_dropped_event_total",
			Help:      "How many CA Viz events were dropped because the event buffer was full.",
		},
	)

	caVizChemistClientRequestDurationSeconds = k8smetrics.NewHistogram(
		&k8smetrics.HistogramOpts{
			Namespace: caNamespace,
			Name:      "ca_viz_chemist_client_request_duration_seconds",
			Help:      "What is the latency of Chemist calls made for CA Viz.",
			Buckets:   k8smetrics.ExponentialBuckets(0.001, 2, 10),
		},
	)

	caVizChemistClientRequestsTotal = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "ca_viz_chemist_client_requests_total",
			Help:      "How many chemist calls with each response status code were made by CA Viz.",
		},
		[]string{"status_code"},
	)

	napDefaultsMinCpuPlatformEnabled = k8smetrics.NewGauge(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "nap_defaults_min_cpu_platform_enabled",
			Help:      "Whether or not minCpuPlatform is specified and non-empty in Node Autoprovisioning node pool defaults. 1 if it is, 0 otherwise.",
		},
	)

	// TODO(b/149229801): Delete component_version metric and export it using Confluence once master pods are supported by Kubestore.
	componentVersion = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "component_version",
			Help:      "Version of the clusterautoscaler component.",
		},
		[]string{"component_version"},
	)

	worstAllocatableOverestimation = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "worst_allocatable_overestimation",
			Help:      "The worst allocatable overestimation of node template resources compared to actual node (fraction).",
		},
		[]string{"resource", "machine_type", "os_distribution"},
	)

	worstAllocatableUnderestimation = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "worst_allocatable_underestimation",
			Help:      "The worst allocatable underestimation of node template resources compared to actual node (fraction).",
		},
		[]string{"resource", "machine_type", "os_distribution"},
	)

	// Fungibility metrics
	coreZonalDistribution = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "core_zonal_distribution",
			Help:      "Distribution of cores for each machine type in zones.",
		},
		[]string{"zone", "machine_type", "location_policy"},
	)

	// GCE Reservations metrics
	reservationsAvailable = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "reservations_available",
			Help:      "Aggregated sum of all consumable reservations, stored in cache",
		},
		[]string{"zone", "machine_type", "gpu_type"},
	)
	reservationsUsed = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "reservations_used_with_scale_up",
			Help:      "Amount of reservations used with scale-up",
		},
		[]string{"zone", "machine_type", "gpu_type"},
	)
	reservationsUseConsumablePuller = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "reservations_use_consumable_puller",
			Help:      "Indicates 1 if reservations are using ListConsumableReservations API, 0 if using legacy API",
		},
		[]string{},
	)
	// Provisioning Request metrics
	provisioningRequestCount = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "provisioning_request_count",
			Help:      "Number of Provisioning Requests having a particular status and reason.",
		},
		[]string{"status", "reason"},
	)
	provisioningRequestProcessingLatencySeconds = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Namespace: caNamespace,
			Name:      "provisioning_request_processing_latency_seconds",
			Help:      "Latency of handling Provisioning Requests by Cluster Autoscaler.",
			Buckets:   []float64{30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 360, 420, 480, 540, 600, 900, 1200, 1500, 1800, 2400, 3000, 3600, 7200, 14400, 28800, 86400},
		},
		[]string{"status", "reason", "nap_used"},
	)
	provisioningRequestQueueWaitDurationSeconds = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Namespace: caNamespace,
			Name:      "provisioning_request_queue_wait_duration_seconds",
			Help:      "The time it took for a Provisioning Requests to become successfully provisioned.",
			Buckets:   []float64{60, 300, 900, 1800, 3600, 7200, 10800, 14400, 18000, 21600, 25200, 28800, 32400, 36000, 39600, 43200, 54000, 64800, 75600, 86400, 129600, 172800, 216000, 259200, 302400, 345600, 388800, 432000, 475200, 518400, 561600, 604800},
		},
		[]string{"nap_used"},
	)
	longUnprocessedProvisioningRequestCount = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "long_unprocessed_provisioning_request_count",
			Help:      "Number of Provisioning Requests that are in uninitialized or pending state for over 1 hour.",
		},
		[]string{"custom_resources"},
	)
	longAcceptedProvisioningRequestCount = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "long_accepted_provisioning_request_count",
			Help:      "Number of Provisioning Requests that are in accepted state for over 1 day.",
		},
		[]string{"accelerator_type", "zone", "nap_used"},
	)
	overwrittenShortLivedNodeInfos = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "short_lived_node_info_status",
			Help:      "Whether the nodepool had the nodeInfo overwritten / didn't need overwriting / overwriting failed.",
		},
		[]string{"node_pool_name", "status"},
	)

	// defrag metrics
	defragScaledDownNodesTotal = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "defrag_scaled_down_nodes_total",
			Help:      "Number of nodes removed by autoscaler defrag framework.",
		},
		[]string{"plugin"},
	)

	defragFailedScaledDownNodesTotal = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "defrag_failed_scaled_down_nodes_total",
			Help:      "Number of nodes which failed to be removed by autoscaler defrag framework.",
		},
		[]string{"plugin"},
	)

	defragUnfitNodes = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "defrag_unfit_nodes",
			Help:      "Number of nodes that are considered unfit by one of autoscaler defrag plugins.",
		},
		[]string{"plugin"})

	defragEvictedPodsTotal = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "defrag_evicted_pods_total",
			Help:      "Number of pods evicted by autoscaler defrag framework.",
		},
		[]string{"plugin"},
	)
	defragNodeRemovalDurationSeconds = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Namespace: caNamespace,
			Name:      "defrag_node_removal_duration",
			Help:      "How long it takes for node to get removed by defrag framework from the moment it was selected as a candidate.",
			Buckets:   k8smetrics.LinearBuckets(60, 60, 30),
		},
		[]string{"plugin"},
	)
	defragInvalidatedCandidatesTotal = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "defrag_invalidated_candidates",
		},
		[]string{"reason", "plugin"},
	)
	defragStaleness = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "defrag_staleness",
			Help:      "Last time the defrag plugin was used since unix epoch in seconds",
		},
		[]string{"plugin"},
	)

	nodeUtilization = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "utilization_node_count",
			Help:      "The number of nodes in each resource utilization bucket by the type of node, and their machine_type.",
		},
		[]string{"node_type", "machine_type", "resource", "utilization_bucket"},
	)

	unexpectedPods = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "unexpected_pods",
			Help:      "Number of pods with a node selector that should have been rejected by GKE Webhook.",
		}, []string{"reason"},
	)

	// NodeProvisioningConfig Metrics
	npcCount = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "npc_count",
			Help:      "Number of NPCs in the cluster split by their NPC configuration.",
		},
		[]string{"nap_enabled", "defrag_enabled", "when_unsatisfiable", "crd_type"},
	)

	npcRuleCount = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "npc_rule_count",
			Help:      "Number of NPC rules in the cluster split by their rule type.",
		},
		[]string{"rule_index", "rule_type", "crd_type"},
	)

	npcHealth = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "npc_health",
			Help:      "Number of NPCs in the cluster split by their health status as set by the NPC Validator.",
		},
		[]string{"status", "crd_type"},
	)

	crdUnhealthinessConditions = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "crd_unhealthiness_conditions",
			Help:      "Number of unhealthiness conditions for NPCs and CCCs in the cluster.",
		},
		[]string{"condition", "reason", "crd_type"},
	)

	scaledUpNodesPerRule = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "scaled_up_nodes_per_npc_rule",
			Help:      "Number of scaled up nodes per NPC rule ordinal number",
		},
		[]string{"rule_index", "crd_type"},
	)

	scaledDownNodesPerRule = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "scaled_down_nodes_per_npc_rule",
			Help:      "Number of scaled down nodes per NPC rule ordinal number",
		},
		[]string{"rule_index", "crd_type"},
	)

	invalidNpcScaleUpOrder = k8smetrics.NewCounter(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "invalid_npc_scale_up_order",
			Help:      "Indicated the NPC scale-up option order is different than expected",
		},
	)

	binpackingNodeGroupsTotal = k8smetrics.NewHistogram(
		&k8smetrics.HistogramOpts{
			Namespace: caNamespace,
			Name:      "binpacking_node_groups_total",
			Help:      "Distribution of node groups passed to binpacking algorithm.",
			Buckets:   []float64{10, 20, 30, 40, 60, 80, 100, 120, 160, 200, 300, 400, 600, 800, 1000},
		},
	)

	binpackingNodeGroupsProcessed = k8smetrics.NewHistogram(
		&k8smetrics.HistogramOpts{
			Namespace: caNamespace,
			Name:      "binpacking_node_groups_processed",
			Help:      "Distribution of node groups processed by binpacking algorithm.",
			Buckets:   []float64{10, 20, 30, 40, 60, 80, 100, 120, 160, 200, 300, 400, 600, 800, 1000},
		},
	)

	binpackingNodeGroupsSkipped = k8smetrics.NewHistogram(
		&k8smetrics.HistogramOpts{
			Namespace: caNamespace,
			Name:      "binpacking_node_groups_skipped",
			Help:      "Distribution of node groups skipped by binpacking algorithm.",
			Buckets:   []float64{10, 20, 30, 40, 60, 80, 100, 120, 160, 200, 300, 400, 600, 800, 1000},
		},
	)

	missingLabels = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "missing_node_template_labels",
			Help:      "System labels missing from template node, but present in the real nodes",
		},
		[]string{"missing_label"},
	)

	uasMaxSizeRecommendationAge = k8smetrics.NewHistogram(
		&k8smetrics.HistogramOpts{
			Namespace: caNamespace,
			Name:      "uas_max_size_recommendations_age_seconds",
			Help:      "seconds elapsed since UAS max size recommendation was generated",
			Buckets:   []float64{10, 20, 40, 80, 160, 320, 640, 1280},
		})

	ekGceResizeRequestDuration = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Namespace: caNamespace,
			Name:      "ek_gce_resize_request_duration_seconds",
			Help:      "How long it takes for a GCE resize request to complete.",
			Buckets:   []float64{1, 2, 4, 6, 8, 10, 12, 15, 18, 21, 25, 30, 45, 60, 120, 300, 600},
		},
		[]string{"direction", "status"},
	)

	ekResizeOperation = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "ek_resize_operations",
			Help:      "How many times an EK resize failed / succeeded.",
		},
		[]string{"direction", "reason", "status"},
	)

	vmGceResizeRequestDuration = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Namespace: caNamespace,
			Name:      "vm_gce_resize_request_duration_seconds",
			Help:      "How long it takes for a GCE resize request to complete.",
			Buckets:   []float64{1, 2, 4, 6, 8, 10, 12, 15, 18, 21, 25, 30, 45, 60, 120, 300, 600},
		},
		[]string{"machine_family", "direction", "status"},
	)

	vmResizeOperation = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "vm_resize_operations",
			Help:      "How many times a VM resize failed / succeeded.",
		},
		[]string{"machine_family", "direction", "reason", "status"},
	)

	// TODO(b/470880235): delete after migrating to general resize metrics
	ekBackoffStatus = k8smetrics.NewGauge(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "ek_backoff_status",
			Help:      "Describes if cluster is in resize backoff. Ongoing backoff if 1, no backoff otherwise.",
		},
	)

	resizeBackoffStatus = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "resize_backoff_status",
			Help:      "Describes if cluster is in resize backoff. Ongoing backoff if 1, no backoff otherwise.",
		}, []string{"machine_family"},
	)

	taskQueueSize = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "task_queue_size",
			Help:      "The number of queued GKE operation tasks",
		}, []string{"task_type", "phase"},
	)

	taskQueueDuration = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Namespace: caNamespace,
			Name:      "task_queue_duration_seconds",
			Help:      "The time spent by a task in a particular phase in GKE operation task queue",
			Buckets:   k8smetrics.ExponentialBuckets(0.01, 1.5, 30), // 0.01, 0.015, 0.0225, ..., 852.2269299239293, 1278.3403948858938
		},
		[]string{"task_type", "phase", "task_result"},
	)

	taskQueueCompletedCount = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "task_queue_completed_count",
			Help:      "Number of tasks completed in the GKE operation task queue",
		},
		[]string{"task_type", "task_result"},
	)

	// TODO(b/470880235): delete after migrating to general resize metrics
	ekLaunchStatus = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "ek_launch_status",
			Help:      "Information about EK launch status.",
		},
		[]string{"launch_phase", "launched_from"},
	)

	resizableVmLaunchStatus = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "resizable_vm_launch_status",
			Help:      "Information about resizable vm launch status.",
		},
		[]string{"machine_family", "launch_phase", "launched_from"},
	)

	// TODO(b/470880235): delete after migrating to general resize metrics
	ekAutopilotComputeClassStatus = k8smetrics.NewGauge(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "ek_autopilot_compute_class_status",
			Help:      "Describes if EKs are supported on Autopilot Compute Class. Enabled if 1, disabled otherwise.",
		},
	)

	resizableVmAutopilotComputeClassStatus = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "resizable_vm_autopilot_compute_class_status",
			Help:      "Describes if a machine family is supported on Autopilot Compute Class. Enabled if 1, disabled otherwise.",
		},
		[]string{"machine_family"},
	)

	podsSchedulableOnEkUpsizes = k8smetrics.NewCounter(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "pods_scheduled_on_ek_upsizes",
			Help:      "Number of pods removed from scale up loop by scheduling them on attempted EK resizes.",
		},
	)

	resizableVmPodsSchedulableOnUpsizes = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "resizable_vm_pods_schedulable_on_upsizes",
			Help:      "Number of pods removed from scale up loop by scheduling them on attempted resizable VM resizes.",
		},
		[]string{"machine_family"},
	)

	fixerEvents = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "ek_fixer_events_total",
			Help:      "How many node fix events were performed by the ekvm fixer.",
		},
		[]string{"fix_type", "status", "source"},
	)

	resizableVmFixerEvents = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "resizable_vm_fixer_events",
			Help:      "How many node fix events were performed by the resizable VM fixer.",
		},
		[]string{"machine_family", "fix_type", "status", "source"},
	)

	reconcileNodeStateEvents = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "ek_reconcile_node_state_events_total",
			Help:      "How many node reconcile node state events were performed by the ekvm fixer.",
		},
		[]string{"attempts_num", "status", "should_retry"},
	)

	resizableVmReconcileNodeStateEvents = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "resizable_vm_reconcile_node_state_events",
			Help:      "How many node reconcile node state events were performed by the resizable VM fixer.",
		},
		[]string{"machine_family", "attempts_num", "status", "should_retry"},
	)

	lookaheadLaunchStatus = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "lookahead_launch_status",
			Help:      "Information about lookahead buffer launch status.",
		},
		[]string{"launch_phase", "launched_from", "strategy"},
	)

	unscheduleableLookaheadPodsCount = k8smetrics.NewGauge(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "unschedulable_lookahead_pods_count",
			Help:      "Count of unschedulable lookahead pods on existing or upcoming nodes in the cluster.",
		},
	)

	resizableVmUnschedulableLookaheadPodsCount = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "resizable_vm_unschedulable_lookahead_pods_count",
			Help:      "Count of unschedulable lookahead pods on existing or upcoming nodes in the cluster.",
		},
		[]string{"machine_family"},
	)

	lookaheadPodsCount = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "lookahead_pods_count",
			Help:      "Count of injected lookahead pods in a CA loop.",
		},
		[]string{"milliCpu", "memoryKiB"},
	)

	totalEkNodesLookaheadSpaceCPU = k8smetrics.NewGauge(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "total_ek_nodes_lookahead_space_cpu",
			Help:      "Total lookahead space (max upsizable size - pods requests) CPU size of EK nodes in the cluster",
		},
	)

	totalEkNodesLookaheadSpaceMemory = k8smetrics.NewGauge(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "total_ek_nodes_lookahead_space_memory",
			Help:      "Total lookahead space (max upsizable size - pods requests) memory size of EK nodes in the cluster",
		},
	)

	resizableVmTotalNodesLookaheadSpaceCPU = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "resizable_vm_total_nodes_lookahead_space_cpu",
			Help:      "Total lookahead space (max upsizable size - pods requests) CPU size of resizable VM nodes in the cluster",
		},
		[]string{"machine_family"},
	)

	resizableVmTotalNodesLookaheadSpaceMemory = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "resizable_vm_total_nodes_lookahead_space_memory",
			Help:      "Total lookahead space (max upsizable size - pods requests) memory size of resizable VM nodes in the cluster",
		},
		[]string{"machine_family"},
	)

	nodesWithLookaheadPodsShape = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "nodes_with_lookahead_pods_shape",
			Help:      "Shape of nodes that have scheduled lookahead pods",
		},
		[]string{"nodeCpu", "nodeMemoryGiB", "sqrtUserPodsCount"},
	)

	// TODO(b/494558643): Move it to OSS.
	capacityBuffersPodsMetric = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "capacity_buffer_pods",
			Help:      "Number of capacity buffer pods in the cluster",
		},
		[]string{"provisioning_strategy", "state"},
	)

	// TODO(b/494558643): Move it to OSS.
	capacityBuffersNumberMetric = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "capacity_buffers_total",
			Help:      "Number of capacity buffers in the cluster",
		},
		[]string{"provisioning_strategy"},
	)

	csnEnabled = k8smetrics.NewGauge(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "csn_enabled",
			Help:      "Whether or not CSN (Cold Standby Nodes) is enabled. 1 if it is, 0 otherwise.",
		},
	)

	csnInvalidCondition = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "csn_invalid_conditions",
			Help:      "Invalid CSN conditions found in the cluster",
		},
		[]string{"condition"},
	)

	napEnabled = k8smetrics.NewGauge(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "nap_enabled",
			Help:      "Whether or not Node Autoprovisioning is enabled. 1 if it is, 0 otherwise.",
		},
	)

	flexAdvisorCacheQueryCount = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "flexadvisor_cache_query_count",
			Help:      "Number of Flex Advisor cache queries of different result.",
		},
		[]string{"result", "ccc_state", "is_ccc_scale_up_anyway", "key_generation_state"},
	)

	flexAdvisorActiveScopes = k8smetrics.NewGauge(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "flexadvisor_active_scopes",
			Help:      "Number of active Flex Advisor scopes.",
		},
	)

	flexAdvisorRejectedScopes = k8smetrics.NewGauge(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "flexadvisor_rejected_scopes",
			Help:      "Number of Flex Advisor scopes rejected in the current loop due to throttling.",
		},
	)

	flexAdvisorDecisionFeedbackCount = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "flexadvisor_decision_feedback_count",
			Help:      "Number of provisioning decision feedbacks from Flex Advisor.",
		},
		[]string{"category"},
	)

	flexAdvisorGeneratedInstanceConfigurationCount = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "flexadvisor_instance_config_count",
			Help:      "Number of instance configurations generated by Flex Advisor for a given flexibility scope key (CCC)",
		},
		[]string{"flexibility_scope"},
	)

	ccMinTargetNodesReactionLatency = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Namespace: caNamespace,
			Name:      "compute_class_min_target_nodes_reaction_latencies",
			Help:      "Distribution of latencies measured from a detected shortfall in TargetNodeCount defined in Compute Classes to the initiation of a scale-up cycle by Cluster Autoscaler.",
			Buckets:   DurationBuckets1sTo24h,
		},
		[]string{"defined_in_priority"},
	)

	ccMinTargetNodesProvisioningLatency = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Namespace: caNamespace,
			Name:      "compute_class_min_target_nodes_provisioning_latencies",
			Help:      "Distribution of end-to-end latencies required to fulfill the TargetNodeCount defined in Compute Classes.",
			Buckets:   DurationBuckets1sTo24h,
		},
		[]string{"provisioning_error_encountered", "unhelpable", "defined_in_priority"},
	)

	ccLongUnprovisionedMinTargetNodesCount = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "compute_class_long_unprovisioned_min_target_nodes",
			Help:      "Current number of Compute Classes where static capacity has remained unsatisfied for more than 30 minutes.",
		},
		[]string{"provisioning_error_encountered", "unhelpable", "defined_in_priority"},
	)

	flexAdvisorGenerationErrors = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "flexadvisor_generation_errors_total",
			Help:      "Number of errors or anomalies encountered while generating instance configurations for Flex Advisor.",
		},
		[]string{"reason"},
	)

	flexAdvisorResponseErrors = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "flexadvisor_response_errors_total",
			Help:      "Number of errors or anomalies encountered by Flex Advisor.",
		},
		[]string{"reason"},
	)

	machineConfigSourceInfo = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "machine_config_source_info",
			Help:      "A real-time metric gauge identifying the configuration source of a machine family, labeling it as either hardcoded within the legacy codebase, or dynamically provisioned via the automated CRD pipeline.",
		},
		[]string{"machine_family", "config_source"},
	)
)

// allMetrics is the single source of truth for all metrics to register.
// When adding a new metric, add it here. Both RegisterAll and ResetAllForTest
// (in metrics_test_accessor.go) use this slice, so new metrics are automatically
// handled by both registration and test reset.
var allMetrics = []k8smetrics.Registerable{
	profile,
	clusterType,
	podShardCount,
	unschedulablePodDuration,
	podSchedulingDuration,
	reactionTimeMilliseconds,
	longUnschedulablePodsCount,
	podSchedulableResets,
	caVizLoggedBytes,
	caVizDroppedEventTotal,
	caVizChemistClientRequestDurationSeconds,
	caVizChemistClientRequestsTotal,
	napDefaultsMinCpuPlatformEnabled,
	componentVersion,
	worstAllocatableOverestimation,
	worstAllocatableUnderestimation,
	coreZonalDistribution,
	provisioningRequestCount,
	provisioningRequestProcessingLatencySeconds,
	provisioningRequestQueueWaitDurationSeconds,
	longUnprocessedProvisioningRequestCount,
	longAcceptedProvisioningRequestCount,
	overwrittenShortLivedNodeInfos,
	defragScaledDownNodesTotal,
	defragFailedScaledDownNodesTotal,
	defragUnfitNodes,
	defragEvictedPodsTotal,
	defragNodeRemovalDurationSeconds,
	defragInvalidatedCandidatesTotal,
	defragStaleness,
	reservationsAvailable,
	reservationsUsed,
	reservationsUseConsumablePuller,
	nodeUtilization,
	unexpectedPods,
	npcCount,
	npcRuleCount,
	npcHealth,
	crdUnhealthinessConditions,
	scaledUpNodesPerRule,
	scaledDownNodesPerRule,
	binpackingNodeGroupsTotal,
	binpackingNodeGroupsProcessed,
	binpackingNodeGroupsSkipped,
	missingLabels,
	uasMaxSizeRecommendationAge,
	ekGceResizeRequestDuration,
	ekResizeOperation,
	vmGceResizeRequestDuration,
	vmResizeOperation,
	ekBackoffStatus,
	resizeBackoffStatus,
	taskQueueSize,
	taskQueueDuration,
	taskQueueCompletedCount,
	ekLaunchStatus,
	resizableVmLaunchStatus,
	ekAutopilotComputeClassStatus,
	resizableVmAutopilotComputeClassStatus,
	podsSchedulableOnEkUpsizes,
	resizableVmPodsSchedulableOnUpsizes,
	fixerEvents,
	resizableVmFixerEvents,
	reconcileNodeStateEvents,
	resizableVmReconcileNodeStateEvents,
	lookaheadLaunchStatus,
	unscheduleableLookaheadPodsCount,
	resizableVmUnschedulableLookaheadPodsCount,
	lookaheadPodsCount,
	totalEkNodesLookaheadSpaceCPU,
	totalEkNodesLookaheadSpaceMemory,
	resizableVmTotalNodesLookaheadSpaceCPU,
	resizableVmTotalNodesLookaheadSpaceMemory,
	nodesWithLookaheadPodsShape,
	capacityBuffersPodsMetric,
	capacityBuffersNumberMetric,
	csnEnabled,
	csnInvalidCondition,
	napEnabled,
	flexAdvisorCacheQueryCount,
	flexAdvisorActiveScopes,
	flexAdvisorRejectedScopes,
	flexAdvisorDecisionFeedbackCount,
	flexAdvisorGeneratedInstanceConfigurationCount,
	featureEnabled,
	ccMinTargetNodesReactionLatency,
	ccMinTargetNodesProvisioningLatency,
	ccLongUnprovisionedMinTargetNodesCount,
	flexAdvisorGenerationErrors,
	flexAdvisorResponseErrors,
	machineConfigSourceInfo,
}

// RegisterAll registers all metrics.
func RegisterAll() {
	for _, m := range allMetrics {
		legacyregistry.MustRegister(m)
	}
	recordComponentVersion()
}

// recordComponentVersion extracts the clusterautoscaler component version from an env variable and records it as a metric label.
func recordComponentVersion() {
	version, found := os.LookupEnv(ComponentVersionEnvVar)
	if !found {
		klog.Warningf("Couldn't find component version in %s - it won't be exported as a metric. This is expected for GKE <1.21.0.", ComponentVersionEnvVar)
		return
	}
	klog.Infof("clusterautoscaler version: %s", version)
	componentVersion.WithLabelValues(version).Set(1.0)
}

// prometheusMetrics implements the interface of prometheus metrics
type prometheusMetrics struct{}

// Metrics surfaces the default prod metrics implementation
var Metrics = &prometheusMetrics{}

// ObserveFirstReactionTime reports reaction time metric for real pods.
func (m *prometheusMetrics) ObserveFirstReactionTime(duration time.Duration, systemPod, hasPVC, hasCSI bool, reactionType ReactionType, allocationMode string) {
	reactionTimeMilliseconds.With(map[string]string{
		"system_pod":                            strconv.FormatBool(systemPod),
		"has_pvc":                               strconv.FormatBool(hasPVC),
		"has_csi":                               strconv.FormatBool(hasCSI),
		"reaction_type":                         reactionType.String(),
		"fake_pod_type":                         RealPodType.String(),
		"capacity_buffer_provisioning_strategy": "",
		"device_allocation_mode":                allocationMode,
	}).Observe(float64(duration.Milliseconds()))
}

// ObserveCapacityBufferFakePodReactionTime reports reaction time metric for capacity buffer pods.
func (m *prometheusMetrics) ObserveCapacityBufferFakePodReactionTime(duration time.Duration, systemPod, hasPVC, hasCSI bool, reactionType ReactionType, provisioningStrategy, allocationMode string) {
	reactionTimeMilliseconds.With(map[string]string{
		"system_pod":                            strconv.FormatBool(systemPod),
		"has_pvc":                               strconv.FormatBool(hasPVC),
		"has_csi":                               strconv.FormatBool(hasCSI),
		"reaction_type":                         reactionType.String(),
		"fake_pod_type":                         CapacityBufferPodType.String(),
		"capacity_buffer_provisioning_strategy": provisioningStrategy,
		"device_allocation_mode":                allocationMode,
	}).Observe(float64(duration.Milliseconds()))
}

// UpdateProfile records autoscaling profile used in the cluster.
func (*prometheusMetrics) UpdateProfile(name string) {
	profile.WithLabelValues(name).Set(1)
}

func (*prometheusMetrics) RecordClusterType(autopilotEnabled bool) {
	if autopilotEnabled {
		clusterType.WithLabelValues(gkeAutopilotLabel).Set(1.0)
	} else {
		clusterType.WithLabelValues(gkeStandardLabel).Set(1.0)
	}
}

// UpdateMachineConfigSourceInfo records whether a machine family's config is from legacy code (hardcoded) or CRD pipeline (dynamic).
func (*prometheusMetrics) UpdateMachineConfigSourceInfo(machineFamily string, configSource machinetypes.ConfigSource, value float64) {
	machineConfigSourceInfo.WithLabelValues(machineFamily, string(configSource)).Set(value)
}

// UpdatePodShardCount updates number of shards for currently unschedulable pods.
func (*prometheusMetrics) UpdatePodShardCount(count int) {
	podShardCount.Set(float64(count))
}

// RegisterCAVizLoggedBytes registers logged byte count for the given event type.
func (*prometheusMetrics) RegisterCAVizLoggedBytes(byteCount int, eventType vispb.EventType) {
	caVizLoggedBytes.WithLabelValues(string(eventType)).Observe(float64(byteCount))
}

// RegisterCAVizDroppedEvent increments the CA Viz dropped event total.
func (*prometheusMetrics) RegisterCAVizDroppedEvent() {
	caVizDroppedEventTotal.Inc()
}

// RegisterCAVizChemistCall registers information about a chemist call made by CA Viz.
func (*prometheusMetrics) RegisterCAVizChemistCall(statusCode string, latency time.Duration) {
	caVizChemistClientRequestDurationSeconds.Observe(latency.Seconds())
	caVizChemistClientRequestsTotal.WithLabelValues(statusCode).Inc()
}

// UpdateNapDefaultsMinCpuPlatformEnabled records if cluster uses minCpuPlatform in autoprovisioning
// node pool defaults.
func (*prometheusMetrics) UpdateNapDefaultsMinCpuPlatformEnabled(napDefaultsMinCpuPlatform string) {
	if napDefaultsMinCpuPlatform == "" {
		napDefaultsMinCpuPlatformEnabled.Set(0)
	} else {
		napDefaultsMinCpuPlatformEnabled.Set(1)
	}
}

// UpdateWorstAllocatableOverestimation records the worst allocatable overestimation.
func (*prometheusMetrics) UpdateWorstAllocatableOverestimation(resource, machineType, osDistribution string, value float64) {
	worstAllocatableOverestimation.WithLabelValues(resource, machineType, osDistribution).Set(value)
}

// UpdateWorstAllocatableUnderestimation records the worst allocatable underestimation.
func (*prometheusMetrics) UpdateWorstAllocatableUnderestimation(resource, machineType, osDistribution string, value float64) {
	worstAllocatableUnderestimation.WithLabelValues(resource, machineType, osDistribution).Set(value)
}

// UpdateCoreZonalDistribution updates the node zonal distribution metrics
func (*prometheusMetrics) UpdateCoreZonalDistribution(labels map[string]string, count int64) {
	coreZonalDistribution.With(labels).Set(float64(count))
}

// UnregisterCoreZonalDistribution unregisters the node zonal distribution metrics
func (*prometheusMetrics) UnregisterCoreZonalDistribution(labels map[string]string) {
	coreZonalDistribution.Delete(labels)
}

// UpdateProvisioningRequestCount records number of Provisioning Requests having a particular state and reason.
func (*prometheusMetrics) UpdateProvisioningRequestCount(status, reason string, count int) {
	provisioningRequestCount.WithLabelValues(status, reason).Set(float64(count))
}

// ObserveProvisioningRequestProcessingLatencySeconds registers latency of processing a Provisioning Request by Cluster Autoscaler.
func (*prometheusMetrics) ObserveProvisioningRequestProcessingLatencySeconds(state, reason, napUsed string, duration time.Duration) {
	provisioningRequestProcessingLatencySeconds.WithLabelValues(state, reason, napUsed).Observe(duration.Seconds())
}

// ObserveProvisioningRequestQueueWaitDurationSeconds registers the time it took for a Provisioning Requests to become successfully provisioned.
func (*prometheusMetrics) ObserveProvisioningRequestQueueWaitDurationSeconds(napUsed string, duration time.Duration) {
	provisioningRequestQueueWaitDurationSeconds.WithLabelValues(napUsed).Observe(duration.Seconds())
}

// UpdateLongUnprocessedProvisioningRequestCount records number of Provisioning Requests that are in uninitialized or pending state for over 1 hour.
func (*prometheusMetrics) UpdateLongUnprocessedProvisioningRequestCount(customResources string, count int) {
	longUnprocessedProvisioningRequestCount.WithLabelValues(customResources).Set(float64(count))
}

// UpdateLongAcceptedProvisioningRequestCount records number of Provisioning Requests that are in accepted statue for over 1 day.
func (*prometheusMetrics) UpdateLongAcceptedProvisioningRequestCount(labels map[string]string, count int) {
	longAcceptedProvisioningRequestCount.With(labels).Set(float64(count))
}

// RecordOverwrittenShortLivedNodeInfos records whether the nodepool had the nodeInfo overwritten / didn't need overwriting / overwriting failed.
func (*prometheusMetrics) RecordOverwrittenShortLivedNodeInfos(nodepool_name, status string) {
	overwrittenShortLivedNodeInfos.WithLabelValues(nodepool_name, status).Set(float64(1))
}

// IncreaseDefragScaleDownNodesTotal increases number of nodes removed by a given defrag plugin.
func (*prometheusMetrics) IncreaseDefragScaleDownNodesTotal(plugin string, removedNodes int) {
	defragScaledDownNodesTotal.WithLabelValues(plugin).Add(float64(removedNodes))
}

// IncreaseDefragFailedScaleDownNodesTotal increases number of nodes which failed to be removed by a given defrag plugin.
func (*prometheusMetrics) IncreaseDefragFailedScaleDownNodesTotal(plugin string, unremovedNodes int) {
	defragFailedScaledDownNodesTotal.WithLabelValues(plugin).Add(float64(unremovedNodes))
}

// SetDefragUnfitNodes sets current number of nodes considered unfit by a given defrag plugin.
func (*prometheusMetrics) SetDefragUnfitNodes(plugin string, count int) {
	defragUnfitNodes.WithLabelValues(plugin).Set(float64(count))
}

// IncreaseDefragEvictedPodsTotal increases number of pods evicted by a given defrag plugin.
func (*prometheusMetrics) IncreaseDefragEvictedPodsTotal(plugin string, count int) {
	defragEvictedPodsTotal.WithLabelValues(plugin).Add(float64(count))
}

// ObserveDefragNodeRemovalDuration registers the time it took for a given node
// to get removed since the moment it was selected by a given defrag plugin as a candidate.
func (*prometheusMetrics) ObserveDefragNodeRemovalDuration(plugin string, duration time.Duration) {
	defragNodeRemovalDurationSeconds.WithLabelValues(plugin).Observe(duration.Seconds())
}

// IncrementDefragInvalidatedCandidatesTotal increments a number of invalidated
// candidates by a given defrag plugin due to provided reason.
func (*prometheusMetrics) IncrementDefragInvalidatedCandidatesTotal(reason, plugin string) {
	defragInvalidatedCandidatesTotal.WithLabelValues(reason, plugin).Inc()
}

// ObserveDefragStaleness records current time for given defrag plugin.
func (*prometheusMetrics) ObserveDefragStaleness(plugin string) {
	defragStaleness.WithLabelValues(plugin).SetToCurrentTime()
}

// SetReservationsAvailable sets aggregated available reservations
func (*prometheusMetrics) SetReservationsAvailable(labels map[string]string, count int) {
	reservationsAvailable.With(labels).Set(float64(count))
}

// IncreaseReservationsUsed adds reservations used during scale-up
func (*prometheusMetrics) IncreaseReservationsUsed(labels map[string]string, count int) {
	reservationsUsed.With(labels).Add(float64(count))
}

// SetReservationsUseConsumablePuller sets metric indicating which puller is used.
func (*prometheusMetrics) SetReservationsUseConsumablePuller(value bool) {
	var useConsumablePuller float64
	if value {
		useConsumablePuller = 1.0
	}
	reservationsUseConsumablePuller.WithLabelValues().Set(useConsumablePuller)
}

// UpdateNodeUtilization sets the node utilization of a resourceName for a nodeType
func (*prometheusMetrics) UpdateNodeUtilization(nodeType string, machine string, resourceName string, utilizationBucket string, value int) {
	nodeUtilization.WithLabelValues(nodeType, machine, resourceName, utilizationBucket).Set(float64(value))
}

// ResetNodeUtilization resets/clears the node utilization metrics
func (*prometheusMetrics) ResetNodeUtilization() {
	nodeUtilization.Reset()
}

func (*prometheusMetrics) RegisterUnexpectedPod(reason string) {
	unexpectedPods.WithLabelValues(reason).Inc()
}

// MarkMissingLabel marks the given label as a missing label from node template.
func (*prometheusMetrics) MarkMissingLabel(label string) {
	missingLabels.WithLabelValues(label).Set(float64(1))
}

// ObserveMaxSizeRecommendationAge observes UAS max size recommendation age.
func (*prometheusMetrics) ObserveMaxSizeRecommendationAge(age time.Duration) {
	uasMaxSizeRecommendationAge.Observe(age.Seconds())
}

type CapacityBufferPodState string

const (
	// CapacityBufferPodStateReady represents a capacity buffer pod that is successfully scheduled,
	// indicating that the required buffer capacity is available in the cluster.
	CapacityBufferPodStateReady CapacityBufferPodState = "Ready"
	// CapacityBufferPodStateProvisioning represents a capacity buffer pod that is currently unschedulable
	// but has triggered a scale-up, indicating that the required capacity is being provisioned.
	CapacityBufferPodStateProvisioning CapacityBufferPodState = "Provisioning"
	// CapacityBufferPodStateNotReady represents a capacity buffer pod that is unschedulable and
	// has not yet triggered capacity provisioning.
	CapacityBufferPodStateNotReady CapacityBufferPodState = "Not Ready"
)

type CapacityBufferPodsKey struct {
	ProvisioningStrategy string
	State                CapacityBufferPodState
}

// UpdateCapacityBufferPods records the number of capacity buffer pods in each state.
func (*prometheusMetrics) UpdateCapacityBufferPods(counts map[CapacityBufferPodsKey]int) {
	capacityBuffersPodsMetric.Reset()

	for key, count := range counts {
		capacityBuffersPodsMetric.WithLabelValues(key.ProvisioningStrategy, string(key.State)).Set(float64(count))
	}
}

// UpdateCapacityBuffersNumber records the number of capacity buffers in the cluster.
func (*prometheusMetrics) UpdateCapacityBuffersNumber(countsByType map[string]int) {
	capacityBuffersNumberMetric.Reset()

	for strategy, count := range countsByType {
		capacityBuffersNumberMetric.WithLabelValues(strategy).Set(float64(count))
	}
}

// UpdateCSNEnabled records if CSN is enabled
func (*prometheusMetrics) UpdateCSNEnabled(enabled bool) {
	if enabled {
		csnEnabled.Set(1)
	} else {
		csnEnabled.Set(0)
	}
}

type CSNInvalidCondition string

const (
	// SuspendedNodeWithBlockingPods represents a CSN condition where a node is suspended but has pods that block its suspension.
	// This should be investigated if emitted.
	// It can be false positive if the node controller suspend handler correctly didn't suspend the node, which should be visible in the logs. However such case should be extremely rare.
	SuspendedNodeWithBlockingPods CSNInvalidCondition = "SuspendedNodeWithBlockingPods"
)

// SetCSNInvalidCondition records invalid CSN conditions in the cluster.
func (*prometheusMetrics) SetCSNInvalidCondition(condition CSNInvalidCondition) {
	csnInvalidCondition.WithLabelValues(string(condition)).Set(1)
}

// UpdateNapEnabled records if NodeAutoprovisioning is enabled
func (*prometheusMetrics) UpdateNapEnabled(enabled bool) {
	if enabled {
		napEnabled.Set(1)
	} else {
		napEnabled.Set(0)
	}
}

type NpcCountSample struct {
	NapEnabled        bool
	DefragEnabled     bool
	WhenUnsatisfiable string
	CrdType           string
}

func boolToString(b bool) string {
	if b {
		return "true"
	}

	return "false"
}

func (*prometheusMetrics) ObserveNpcCount(samples []NpcCountSample) {
	npcCount.Reset()

	for _, s := range samples {
		npcCount.WithLabelValues(boolToString(s.NapEnabled), boolToString(s.DefragEnabled), s.WhenUnsatisfiable, s.CrdType).Inc()
	}
}

type NpcRuleCountSample struct {
	RuleIndex int
	RuleType  string
	CrdType   string
}

func (*prometheusMetrics) ObserveNpcRuleCount(samples []NpcRuleCountSample) {
	npcRuleCount.Reset()

	for _, s := range samples {
		npcRuleCount.WithLabelValues(strconv.Itoa(s.RuleIndex), s.RuleType, s.CrdType).Inc()
	}
}

func (*prometheusMetrics) ObserveNpcHealth(crdType string, healthy, unhealthy int) {
	npcHealth.WithLabelValues("healthy", crdType).Set(float64(healthy))
	npcHealth.WithLabelValues("unhealthy", crdType).Set(float64(unhealthy))
}

type CrdUnhealthinessConditionSample struct {
	Condition string
	Reason    string
	CrdType   string
}

func (*prometheusMetrics) ObserveCrdUnhealthinessConditions(samples []CrdUnhealthinessConditionSample) {
	crdUnhealthinessConditions.Reset()

	for _, sample := range samples {
		crdUnhealthinessConditions.WithLabelValues(sample.Condition, sample.Reason, sample.CrdType).Inc()
	}
}

func (*prometheusMetrics) ObserveInvalidNpcScaleUpOrder() {
	invalidNpcScaleUpOrder.Inc()
}

// IncreaseScaledUpNodesPerRule increases count of scaled up node per NPC rule.
func (*prometheusMetrics) IncreaseScaledUpNodesPerRule(crdType string, ruleIndex int, count int) {
	scaledUpNodesPerRule.WithLabelValues(strconv.Itoa(ruleIndex), crdType).Add(float64(count))
}

// IncreaseScaledDownNodesPerRule increases count of scaled down node per NPC rule.
func (*prometheusMetrics) IncreaseScaledDownNodesPerRule(crdType string, ruleIndex int) {
	scaledDownNodesPerRule.WithLabelValues(strconv.Itoa(ruleIndex), crdType).Inc()
}

func (*prometheusMetrics) ObserveBinpackingNodeGroupTotal(count int) {
	binpackingNodeGroupsTotal.Observe(float64(count))
}

func (*prometheusMetrics) ObserveBinpackingNodeGroupProcessed(count int) {
	binpackingNodeGroupsProcessed.Observe(float64(count))
}

func (*prometheusMetrics) ObserveBinpackingNodeGroupSkipped(count int) {
	binpackingNodeGroupsSkipped.Observe(float64(count))
}

// ObserveEkGceResizeRequestDuration records how long GCE resize request takes.
func (*prometheusMetrics) ObserveEkGceResizeRequestDuration(direction, status string, duration time.Duration) {
	ekGceResizeRequestDuration.WithLabelValues(direction, status).Observe(duration.Seconds())
}

// ObserveVmGceResizeRequestDuration records how long GCE resize request takes.
func (pm *prometheusMetrics) ObserveVmGceResizeRequestDuration(machineFamily, direction, status string, duration time.Duration) {
	vmGceResizeRequestDuration.WithLabelValues(machineFamily, direction, status).Observe(duration.Seconds())

	// TODO(b/470880235): delete after migrating to general resize metrics
	if machineFamily == machinetypes.EK.Name() {
		pm.ObserveEkGceResizeRequestDuration(direction, status, duration)
	}
}

// RegisterEkResizeOperation increments the count of EK resize operations for the given reason and operation status.
func (*prometheusMetrics) RegisterEkResizeOperation(direction string, reason string, status OperationStatus) {
	ekResizeOperation.WithLabelValues(direction, reason, string(status)).Inc()
}

// RegisterVmResizeOperation increments the count of VM resize operations for the given reason and operation status.
func (pm *prometheusMetrics) RegisterVmResizeOperation(machineFamily, direction, reason string, status OperationStatus) {
	vmResizeOperation.WithLabelValues(machineFamily, direction, reason, string(status)).Inc()

	// TODO(b/470880235): delete after migrating to general resize metrics
	if machineFamily == machinetypes.EK.Name() {
		pm.RegisterEkResizeOperation(direction, reason, status)
	}
}

// UpdateEkBackoffStatus updates the EK backoff status metric.
func (*prometheusMetrics) UpdateEkBackoffStatus(isBackedOff bool) {
	if isBackedOff {
		ekBackoffStatus.Set(1)
	} else {
		ekBackoffStatus.Set(0)
	}
}

// UpdateResizeBackoffStatus updates the resize backoff status metric.
func (pm *prometheusMetrics) UpdateResizeBackoffStatus(machineFamily string, isBackedOff bool) {
	if isBackedOff {
		resizeBackoffStatus.WithLabelValues(machineFamily).Set(1)
	} else {
		resizeBackoffStatus.WithLabelValues(machineFamily).Set(0)
	}

	// TODO(b/470880235): delete after migrating to general resize metrics
	if machineFamily == machinetypes.EK.Name() {
		pm.UpdateEkBackoffStatus(isBackedOff)
	}
}

// UpdateTaskQueueSize updates size of the task queue.
func (*prometheusMetrics) UpdateTaskQueueSize(taskType string, phase TaskQueuePhase, size int) {
	taskQueueSize.WithLabelValues(taskType, string(phase)).Set(float64(size))
}

// ObserveTaskQueueDurationSeconds registers queue time for different task types along with phase and result.
func (*prometheusMetrics) ObserveTaskQueueDurationSeconds(taskType string, phase TaskQueuePhase, result TaskResult, duration time.Duration) {
	taskQueueDuration.WithLabelValues(taskType, string(phase), string(result)).Observe(duration.Seconds())
}

// IncrementTaskCompletedCount increments the task completed count of the task queue.
func (*prometheusMetrics) IncrementTaskCompletedCount(taskType string, result TaskResult) {
	taskQueueCompletedCount.WithLabelValues(taskType, string(result)).Inc()
}

// IncrementFlexAdvisorCacheQueryCount increments the cache query count of the Flex Advisor cache.
func (*prometheusMetrics) IncrementFlexAdvisorCacheQueryCount(result FACacheQueryResult, cccState CccState, isScaleUpAnyway *bool, keyGenerationState KeyGenerationState) {
	var formattedIsScaleUpAnyway string

	if isScaleUpAnyway != nil {
		formattedIsScaleUpAnyway = strconv.FormatBool(*isScaleUpAnyway)
	}
	flexAdvisorCacheQueryCount.WithLabelValues(string(result), string(cccState), formattedIsScaleUpAnyway, string(keyGenerationState)).Inc()
}

// UpdateFlexAdvisorActiveScopes updates the number of active Flex Advisor scopes.
func (*prometheusMetrics) UpdateFlexAdvisorActiveScopes(count int) {
	flexAdvisorActiveScopes.Set(float64(count))
}

// UpdateFlexAdvisorRejectedScopes updates the number of rejected Flex Advisor scopes.
func (*prometheusMetrics) UpdateFlexAdvisorRejectedScopes(rejected int) {
	flexAdvisorRejectedScopes.Set(float64(rejected))
}

// IncrementFlexAdvisorFeedbackDecisionCount increments the number of Flex Advisor feedback decision for each FADecisionFeedbackCategory.
func (*prometheusMetrics) IncrementFlexAdvisorFeedbackDecisionCount(category FADecisionFeedbackCategory) {
	flexAdvisorDecisionFeedbackCount.WithLabelValues(string(category)).Inc()
}

// UpdateFlexAdvisorGeneratedInstanceConfigCount updates the number of instance configurations generated by Flex Advisor for a given flexibility scope key (CCC).
func (*prometheusMetrics) UpdateFlexAdvisorGeneratedInstanceConfigCount(flexibilityScopeKey string, count int) {
	flexAdvisorGeneratedInstanceConfigurationCount.WithLabelValues(flexibilityScopeKey).Set(float64(count))
}

// TODO(b/470880235): delete after migrating to general resize metrics
func (pm *prometheusMetrics) UpdateEkLaunchStatus(launchPhase, launchedFrom string) {
	// Cluster launch status changes so we don't want to track it multiple times
	ekLaunchStatus.Reset()

	ekLaunchStatus.WithLabelValues(launchPhase, launchedFrom).Set(1)
}

func (pm *prometheusMetrics) UpdateResizableVmLaunchStatus(machineFamily, launchPhase, launchedFrom string) {
	if resizableVmLaunchStatus.IsCreated() {
		resizableVmLaunchStatus.DeletePartialMatch(map[string]string{"machine_family": machineFamily})
	}
	resizableVmLaunchStatus.WithLabelValues(machineFamily, launchPhase, launchedFrom).Set(1)

	// TODO(b/470880235): delete after migrating to general resize metrics
	if machineFamily == machinetypes.EK.Name() {
		pm.UpdateEkLaunchStatus(launchPhase, launchedFrom)
	}
}

// TODO(b/470880235): delete after migrating to general resize metrics
func (pm *prometheusMetrics) UpdateEkAutopilotComputeClassStatus(enabled bool) {
	if enabled {
		ekAutopilotComputeClassStatus.Set(1)
	} else {
		ekAutopilotComputeClassStatus.Set(0)
	}
}

func (pm *prometheusMetrics) UpdateResizableVmAutopilotComputeClassStatus(machineFamily string, enabled bool) {
	if enabled {
		resizableVmAutopilotComputeClassStatus.WithLabelValues(machineFamily).Set(1)
	} else {
		resizableVmAutopilotComputeClassStatus.WithLabelValues(machineFamily).Set(0)
	}

	// TODO(b/470880235): delete after migrating to general resize metrics
	if machineFamily == machinetypes.EK.Name() {
		pm.UpdateEkAutopilotComputeClassStatus(enabled)
	}
}

func (pm *prometheusMetrics) RegisterResizableVmPodsSchedulableOnUpsizes(machineFamily string, pods int) {
	resizableVmPodsSchedulableOnUpsizes.WithLabelValues(machineFamily).Add(float64(pods))

	// TODO(b/470880235): delete after migrating to general resize metrics
	if machineFamily == machinetypes.EK.Name() {
		podsSchedulableOnEkUpsizes.Add(float64(pods))
	}
}

// RegisterFixerEvent increments node fixer events total.
func (pm *prometheusMetrics) RegisterResizableVmFixerEvents(machineFamily, fixType, status, source string) {
	resizableVmFixerEvents.WithLabelValues(machineFamily, fixType, status, source).Inc()

	// TODO(b/470880235): delete after migrating to general resize metrics
	if machineFamily == machinetypes.EK.Name() {
		fixerEvents.WithLabelValues(fixType, status, source).Inc()
	}
}

// RegisterReconcileNodeStateEvent increments node reconcile node state events total.
func (pm *prometheusMetrics) RegisterResizableVmReconcileNodeStateEvents(machineFamily string, attemptsNum int, status string, shouldRetry bool) {
	resizableVmReconcileNodeStateEvents.WithLabelValues(machineFamily, strconv.Itoa(attemptsNum), status, strconv.FormatBool(shouldRetry)).Inc()

	// TODO(b/470880235): delete after migrating to general resize metrics
	if machineFamily == machinetypes.EK.Name() {
		reconcileNodeStateEvents.WithLabelValues(strconv.Itoa(attemptsNum), status, strconv.FormatBool(shouldRetry)).Inc()
	}
}

func (*prometheusMetrics) UpdateLookaheadLaunchStatus(launchPhase, launchedFrom, strategy string) {
	// Cluster launch status changes so we don't want to track it multiple times
	lookaheadLaunchStatus.Reset()

	lookaheadLaunchStatus.WithLabelValues(launchPhase, launchedFrom, strategy).Set(1)
}

func (pm *prometheusMetrics) UpdateResizableVmUnschedulableLookaheadPodsCount(machineFamily string, count int) {
	resizableVmUnschedulableLookaheadPodsCount.WithLabelValues(machineFamily).Set(float64(count))

	// TODO(b/470880235): delete after migrating to general resize metrics
	if machineFamily == machinetypes.EK.Name() {
		unscheduleableLookaheadPodsCount.Set(float64(count))
	}
}

func (m *prometheusMetrics) UpdateLookaheadPodsCount(laPodsCount map[size.Allocatable]int) {
	// We reset it since it is a gauge metric (we want only one value per cluster).
	lookaheadPodsCount.Reset()

	for allocatable, count := range laPodsCount {
		milliCpu := allocatable.MilliCpus
		memoryKiB := allocatable.KBytes
		lookaheadPodsCount.WithLabelValues(strconv.Itoa(int(milliCpu)), strconv.Itoa(int(memoryKiB))).Set(float64(count))
	}
}

func (pm *prometheusMetrics) UpdateResizableVmTotalNodesLookaheadSpace(machineFamily string, val size.Allocatable) {
	resizableVmTotalNodesLookaheadSpaceCPU.WithLabelValues(machineFamily).Set(float64(val.MilliCpus))
	resizableVmTotalNodesLookaheadSpaceMemory.WithLabelValues(machineFamily).Set(float64(val.KBytes))

	// TODO(b/470880235): delete after migrating to general resize metrics
	if machineFamily == machinetypes.EK.Name() {
		totalEkNodesLookaheadSpaceCPU.Set(float64(val.MilliCpus))
		totalEkNodesLookaheadSpaceMemory.Set(float64(val.KBytes))
	}
}

func (m *prometheusMetrics) ObservePodUnschedulableDuration(duration time.Duration, labels map[string]string) {
	unschedulablePodDuration.With(labels).Observe(duration.Seconds())
}

func (m *prometheusMetrics) ObservePodSchedulingDuration(duration time.Duration, stockout string, deviceAllocationMode string) {
	podSchedulingDuration.WithLabelValues(stockout, deviceAllocationMode).Observe(duration.Seconds())
}

func (m *prometheusMetrics) SetLongUnschedulablePodCount(count float64, labels map[string]string) {
	longUnschedulablePodsCount.With(labels).Set(count)
}

func int64ToString(x int64) string {
	return strconv.FormatInt(x, 10)
}

func (m *prometheusMetrics) UpdateNodesWithLookaheadPodsShape(laPodsNodeShape []LAPodNodeShape) {
	// We reset it since it is a gauge metric (we want only one value per cluster).
	nodesWithLookaheadPodsShape.Reset()

	for nodeShape, count := range getRoundedLANodesShapeMap(laPodsNodeShape) {
		nodesWithLookaheadPodsShape.WithLabelValues(int64ToString(nodeShape.cpu), int64ToString(nodeShape.memoryGiB), int64ToString(nodeShape.sqrtUserPodsCount)).Set(float64(count))
	}
}

// roundedLAPodNodeShape contains similar information as LAPodNodeShape, but rounded to avoid cardinality issues.
type roundedLAPodNodeShape struct {
	cpu               int64
	memoryGiB         int64
	sqrtUserPodsCount int64
}

// getRoundedLANodesShapeMap aggregates the nodes based on their rounded CPU, memory, and user pod counts.
// This helps to reduce the cardinality of the nodes_with_lookahead_pods_shape metric.
func getRoundedLANodesShapeMap(laPodsNodeShape []LAPodNodeShape) map[roundedLAPodNodeShape]int64 {
	sizeCount := make(map[roundedLAPodNodeShape]int64)
	for _, laNodeShape := range laPodsNodeShape {
		sqrtUserPodsCount := int64(math.Sqrt(float64(laNodeShape.UserWorkloadPodsCount)))

		// We round up to nearest 2 CPU and 8 GiB to avoid cardinality issues.
		nodeCpu := size.RoundUpToIncrement(laNodeShape.NodeSizeAllocatable.MilliCpus, 2000) / 1000
		nodeMemoryGiB := size.RoundUpToIncrement(laNodeShape.NodeSizeAllocatable.KBytes, 8*size.GiBToKiB) / size.GiBToKiB
		sizeCount[roundedLAPodNodeShape{
			cpu:               nodeCpu,
			memoryGiB:         nodeMemoryGiB,
			sqrtUserPodsCount: sqrtUserPodsCount,
		}]++
	}
	return sizeCount
}

// ObserveCcMinTargetNodesReactionLatency records reaction latency for CC min target nodes.
func (m *prometheusMetrics) ObserveCcMinTargetNodesReactionLatency(definedInPriority bool, duration time.Duration) {
	ccMinTargetNodesReactionLatency.WithLabelValues(strconv.FormatBool(definedInPriority)).Observe(duration.Seconds())
}

// ObserveCcMinTargetNodesProvisioningLatency records provisioning latency for CC min target nodes.
func (m *prometheusMetrics) ObserveCcMinTargetNodesProvisioningLatency(provisioningErrorEncountered string, unhelpable, definedInPriority bool, duration time.Duration) {
	ccMinTargetNodesProvisioningLatency.WithLabelValues(provisioningErrorEncountered, strconv.FormatBool(unhelpable), strconv.FormatBool(definedInPriority)).Observe(duration.Seconds())
}

type CcLongUnprovisionedSample struct {
	ProvisioningErrorEncountered string
	Unhelpable                   bool
	DefinedInPriority            bool
}

// ObserveCcLongUnprovisionedMinTargetNodesCount observes count of long unprovisioned CC min target nodes across all CCCs.
func (m *prometheusMetrics) ObserveCcLongUnprovisionedMinTargetNodesCount(samples []CcLongUnprovisionedSample) {
	ccLongUnprovisionedMinTargetNodesCount.Reset()
	for _, s := range samples {
		ccLongUnprovisionedMinTargetNodesCount.WithLabelValues(s.ProvisioningErrorEncountered, strconv.FormatBool(s.Unhelpable), strconv.FormatBool(s.DefinedInPriority)).Inc()
	}
}

// RegisterFlexAdvisorGenerationError records a Flex Advisor generation error.
func (*prometheusMetrics) RegisterFlexAdvisorGenerationError(reason FAGenerationErrorReason) {
	flexAdvisorGenerationErrors.WithLabelValues(string(reason)).Inc()
}

// RegisterFlexAdvisorResponseError records a Flex Advisor response error.
func (*prometheusMetrics) RegisterFlexAdvisorResponseError(reason FAResponseErrorReason) {
	flexAdvisorResponseErrors.WithLabelValues(string(reason)).Inc()
}
