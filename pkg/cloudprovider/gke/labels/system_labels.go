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

package labels

import (
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
)

const (
	// AcceleratorNetworkProfile denotes label used by GKE AcceleratorNetworkProfile feature
	AcceleratorNetworkProfileLabel = "gke.networks.io/accelerator-network-profile"

	// GkeNodePoolLabel denotes label used by GKE to specify node pool name
	GkeNodePoolLabel = "cloud.google.com/gke-nodepool"

	// GkeOsDistributionLabel denotes label used by GKE to specify os distribution
	GkeOsDistributionLabel = "cloud.google.com/gke-os-distribution"

	// NodeGeneratedFromTemplateAnnotation denotes an annotation we add to nodes in nodeInfos, which were generated from MIG templates.
	NodeGeneratedFromTemplateAnnotation = "cloud.google.com/cluster-autoscaler-node-generated-from-template"

	// MachineFamilyLabel provides the machine family of the machine type.
	MachineFamilyLabel = "cloud.google.com/machine-family"

	// ComputeClassLabel specifies the compute class.
	ComputeClassLabel = "cloud.google.com/compute-class"

	// CCCPriorityIndexAnnotationKey is the annotation key used to store the index of the
	// first matching rule.
	CCCPriorityIndexAnnotationKey = "ccc_priority_index"

	// GkeConfidentialNodes specifis if the node is confidential node.
	GkeConfidentialNodes = "cloud.google.com/gke-confidential-nodes"

	// GkeConfidentialNodeType specifies the type of the confidential node
	GkeConfidentialNodeType = "cloud.google.com/gke-confidential-nodes-instance-type"

	// UnspecifiedConfidentialNodeTypeValue represents unspecified confidential node type
	UnspecifiedConfidentialNodeTypeValue = "CONFIDENTIAL_INSTANCE_TYPE_UNSPECIFIED"
	// SEVConfidentialNodeTypeValue represents the SEV confidential node type
	SEVConfidentialNodeTypeValue = "SEV"
	// SEVSNPConfidentialNodeTypeValue represents the SEV-SNP confidential node type
	SEVSNPConfidentialNodeTypeValue = "SEV_SNP"
	// TDXConfidentialNodeTypeValue represents the TDX confidential node type
	TDXConfidentialNodeTypeValue = "TDX"

	// RequestedMinCpuPlatformLabel specifies the min_cpu_platform setting that an autoprovisioned node pool was created with.
	// Can be used to request min_cpu_platform from NAP, but it's deprecated in favor of SupportedCpuPlatformKeyPrefix below.
	RequestedMinCpuPlatformLabel = "cloud.google.com/requested-min-cpu-platform"
	// SupportedCpuPlatformKeyPrefix is used to prefix a CPU platform in a label key (instead of the value, so that multiple platforms
	// can be specified). Such labels are placed on node pools created by NAP with min_cpu_platform set for that platform, and all lower
	// platforms. Pods can request min_cpu_platform from NAP by having node selector/affinity for such a label. If there is an existing
	// suitable node from a node pool created with min_cpu_platform equal to or higher than what the pod requests, the pod will just
	// get scheduled there, preserving the "minimum" semantics. If not, NAP will create a new node pool for the pod. More details:
	// go/ap-min-cpu-platform.
	SupportedCpuPlatformKeyPrefix = "supported-cpu-platform.cloud.google.com/"
	// SupportedCpuPlatformValue is the value used for labels prefixed with SupportedCpuPlatformKeyPrefix.
	SupportedCpuPlatformValue = "true"

	// PreemptibleLabel is a label/taint key used for preemptible VM nodes on GKE.
	PreemptibleLabel = "cloud.google.com/gke-preemptible"
	// SpotLabel is a label/taint key used for Spot VM nodes on GKE.
	SpotLabel = "cloud.google.com/gke-spot"
	// PlacementGroupLabel is a label/taint key used for Compact Placement nodes on GKE.
	PlacementGroupLabel = "cloud.google.com/gke-placement-group"
	// Policy label is a label describing resource policy directly specified by a customer by its name.
	PolicyLabel = "cloud.google.com/placement-policy-name"
	// PreemptionValue is a taint/label value used for preemptible/Spot node pools on GKE.
	PreemptionValue = "true"
	// FlexStartLabel is a label / (optional) taint key used for nodes using Flex Start provisioning model.
	FlexStartLabel = "cloud.google.com/gke-flex-start"
	// FlexStartValue is a taint/label value used for Flex Start node pools on GKE.
	FlexStartValue = "true"

	// NotTargetGkeVersionLabel is a taint label used for marking nodes as unusable
	// since they are not on the expected gke version
	NotTargetGkeVersionLabel = "cloud.google.com/not-target-gke-version"
	// NotTargetGkeVersionValue is a taint value used for marking nodes with unexpected gke version
	NotTargetGkeVersionValue = "true"

	// ProvisioningLabel is a label/taint key used for provisioning types of VM nodes on GKE.
	ProvisioningLabel = "cloud.google.com/gke-provisioning"
	// SpotProvisioningValue describes spot provisioning
	SpotProvisioningValue = "spot"
	// FlexStartProvisioningValue describes Flex Start provisioning
	FlexStartProvisioningValue = "flex-start"
	// PreemptibleProvisioningValue describes preemptible provisioning
	PreemptibleProvisioningValue = "preemptible"
	// StandardProvisioningValue describes standard provisioning
	StandardProvisioningValue = "standard"

	// PrivateNodeLabel is a label/taint used for private nodes on GKE.
	PrivateNodeLabel = "cloud.google.com/private-node"

	// AcceleratorCountLabel indicates how many GPUs a node has in Autopilot clusters.
	AcceleratorCountLabel = "cloud.google.com/gke-accelerator-count"

	// GPULabel is the label added to nodes with GPU resource on GKE.
	GPULabel = gce.GPULabel
	// GPUPartitionSizeLabel is the label added to nodes with GPU resource with GPU partitioning.
	GPUPartitionSizeLabel = "cloud.google.com/gke-gpu-partition-size"
	// GPUMaxSharedClientsLabel is the label added to nodes with GPU sharing.
	GPUMaxSharedClientsLabel = "cloud.google.com/gke-max-shared-clients-per-gpu"
	// GPUSharingStrategyLabel indicates which GPU sharing strategy is used.
	GPUSharingStrategyLabel = "cloud.google.com/gke-gpu-sharing-strategy"
	// GPUDriverVersionLabel indicates which GPU driver to use.
	GPUDriverVersionLabel = "cloud.google.com/gke-gpu-driver-version"
	// DefaultGPUDriverVersionValue is set by NAP if not overriden by a node selector.
	DefaultGPUDriverVersionValue = "default"
	// DisabledGPUDriverVersionValue is used to disable GPU driver auto installation.
	DisabledGPUDriverVersionValue = "autoinstall-disabled"
	// GPUTimeSharingStrategy represents time-sharing GPU Sharing strategy.
	GPUTimeSharingStrategy = "time-sharing"
	// GPUMpsStrategy represents GPU Sharing strategy with Nvidia mps.
	GPUMpsStrategy = "mps"
	// NvidiaTeslaK80 represents the nvidia-tesla-k80 GPU type
	NvidiaTeslaK80 = "nvidia-tesla-k80"
	// NvidiaTeslaP100 represents the nvidia-tesla-p100 GPU type
	NvidiaTeslaP100 = "nvidia-tesla-p100"
	// NvidiaTeslaV100 represents the nvidia-tesla-v100 GPU type
	NvidiaTeslaV100 = "nvidia-tesla-v100"
	// NvidiaTeslaP4 represents the nvidia-tesla-p4 GPU type
	NvidiaTeslaP4 = "nvidia-tesla-p4"
	// NvidiaTeslaT4 represents the nvidia-tesla-t4 GPU type
	NvidiaTeslaT4 = "nvidia-tesla-t4"
	// NvidiaTeslaA100 represents the nvidia-tesla-a100 GPU type
	NvidiaTeslaA100 = "nvidia-tesla-a100"
	// NvidiaA100_80gb represents the nvidia-a100-80gb GPU type
	NvidiaA100_80gb = "nvidia-a100-80gb"
	// NvidiaL4 represents the nvidia-l4 GPU type
	NvidiaL4 = "nvidia-l4"
	// NvidiaH100_80gb represents the nvidia-h100-80gb GPU type
	NvidiaH100_80gb = "nvidia-h100-80gb"
	// NvidiaH100Mega_80gb represents the nvidia-h100-mega-80gb GPU type
	NvidiaH100Mega_80gb = "nvidia-h100-mega-80gb"
	// NvidiaH200Ultra_141gb represents the nvidia-h200-141gb GPU type
	NvidiaH200Ultra_141gb = "nvidia-h200-141gb"
	// NvidiaB200 represents the nvidia-b200 GPU type
	NvidiaB200 = "nvidia-b200"
	// NvidiaGB200 represents the nvidia-gb200 GPU type
	NvidiaGB200 = "nvidia-gb200"
	// NvidiaGB300 represents the nvidia-gb300 GPU type
	NvidiaGB300 = "nvidia-gb300"
	// NvidiaRTXPro6000 represents the nvidia-rtx-pro-6000 GPU type
	NvidiaRTXPro6000 = "nvidia-rtx-pro-6000"
	// NetdLabel is label required to schedule netd daemonset on node.
	NetdLabel = "cloud.google.com/gke-netd-ready"
	// NetdValue is label value used to schedule netd daemonset on node.
	NetdValue = "true"
	// IpMasqAgentLabel is label required to schedule ip-masq-agent daemonset on node.
	IpMasqAgentLabel = "node.kubernetes.io/masq-agent-ds-ready"
	// IpMasqAgentValue is value for label required to schedule ip-masq-agent daemonset on node.
	IpMasqAgentValue = "true"

	// MaxPodsPerNodeLabel is a label whose value is the max pods per node.
	MaxPodsPerNodeLabel = "cloud.google.com/gke-max-pods-per-node"

	// TPULabel is the label added to nodes with TPU resource on GKE.
	TPULabel = "cloud.google.com/gke-tpu-accelerator"
	// TPUTopologyLabel is the label specifying the topology of multi-host tpu podslice.
	TPUTopologyLabel = "cloud.google.com/gke-tpu-topology"
	// TpuV3DeviceValue represents the 'tpu-v3-device' TPU type
	TpuV3DeviceValue = "tpu-v3-device"
	// TpuV3SliceValue represents the 'tpu-v3-podslice' TPU type.
	TpuV3SliceValue = "tpu-v3-slice"
	// TpuV4LiteDevice represents the 'tpu-v4-lite-device' TPU type.
	TpuV4LiteDeviceValue = "tpu-v4-lite-device"
	// TpuV4PodsliceValue represents the 'tpu-v4-podslice' TPU type.
	TpuV4PodsliceValue = "tpu-v4-podslice"

	// TpuV5LitePodsliceValue represents the 'tpu-v5-lite-podslice' TPU type.
	TpuV5LitePodsliceValue = "tpu-v5-lite-podslice"
	// TpuV5LiteDeviceValue represents the 'tpu-v5-lite-device' TPU type.
	TpuV5LiteDeviceValue = "tpu-v5-lite-device"
	// TpuV5PSliceValue represents the 'tpu-v5p-slice' TPU Type.
	// This is similar to the podslice type, however, the naming convention
	// has been changed
	TpuV5PSliceValue = "tpu-v5p-slice"
	// TpuV6ESliceValue represents the 'tpu-v6e-slice' TPU Type.
	TpuV6ESliceValue = "tpu-v6e-slice"
	// Tpu7xValue represents the 'tpu7x' TPU Type.
	Tpu7xValue = "tpu7x"
	// Tpu7Value represents the 'tpu7' TPU Type.
	Tpu7Value = "tpu7"

	// ReservationNameLabel is the name of a specific reservation that should be used.
	ReservationNameLabel = "cloud.google.com/reservation-name"
	// ReservationProjectLabel is a project where the specific reservation lives.
	ReservationProjectLabel = "cloud.google.com/reservation-project"
	// ReservationZoneLabel is the zone in which the reservation exists.
	ReservationZoneLabel = "cloud.google.com/reservation-zone"
	// ReservationAffinityLabel is the type of affinity a reservation uses.
	ReservationAffinityLabel = "cloud.google.com/reservation-affinity"
	// ReservationBlocksLabel is the name of a block a reservation uses.
	ReservationBlocksLabel = "cloud.google.com/reservation-blocks"
	// ReservationBlocksCountLabel is the label to store block count.
	ReservationBlocksCountLabel = "cloud.google.com/reservation-blocks-count"
	// ReservationSubBlocks is the name of a subblock a reservation uses.
	ReservationSubBlocksLabel = "cloud.google.com/reservation-subblocks"
	// ReservationSubBlocksCountLabel is the label to store subblock count.
	ReservationSubBlocksCountLabel = "cloud.google.com/reservation-subblocks-count"
	// ReservationResourcePoliciesPolicyKey is the key under which the placement policy name is stored, when created via CLI
	ReservationResourcePoliciesPolicyKey = "policy"
	// ReservationResourcePoliciesPlacementKey is the key under which the placement group name is stored, when created via Pantheon
	ReservationResourcePoliciesPlacementKey = "placement"

	// EphemeralLocalSsdLabel is the label added to nodes when ephemeral local SSD is enabled.
	EphemeralLocalSsdLabel = "cloud.google.com/gke-ephemeral-storage-local-ssd"
	// EphemeralLocalSsdEnabledValue is the value for an enabled ephemeral local SSD feature.
	EphemeralLocalSsdEnabledValue = "true"

	// HighPerformanceNetworkLabel is the label added to nodes when high performance
	// network is attached for multi-network pods.
	HighPerformanceNetworkLabel = "cloud.google.com/run-high-perf-daemons"
	// HighPerformanceNetworkValue is the value for an enabled high performance network feature.
	HighPerformanceNetworkValue = "true"

	// ExtendedDurationPodsLabel is the label required to schedule extended duration pods.
	// Details in go/extended-duration-pod-design.
	ExtendedDurationPodsLabel = "cloud.google.com/extended-duration-pods"
	// ExtendedDurationPackedPodsValue is the value indicating pods should be exempt from being
	// forced onto smallest possible machine type. See details in go/agones-extended-duration-pods.
	ExtendedDurationPackedPodsValue = "0"

	// NodeProvisioningConfigLabel is the label required by nodegroups to be managed by NPC.
	// Details in go/nap-npc.
	NodeProvisioningConfigLabel = "autoscaling.gke.io/node-provisioning-config"

	// DefaultNPCName is the name of default NPC
	DefaultNPCName = "default"

	// DefaultCCCName is the name of default CCC
	DefaultCCCName = "default"

	// PodPerVMSizeLabel specifies CPU requested by slice of hardware pods and
	// prevents slice of hardware pods from being scheduled on oversized nodes.
	// Details in go/gke-ap-sohw.
	PodPerVMSizeLabel = "cloud.google.com/pod-isolation"
	// PodCapacityLabel is the label of pod "resource" that is requested by
	// slice of hardware pods to force one pod per node. This controls the
	// number of user-workloads (excluding daemonsets) on the node.
	// Details in go/gke-ap-sohw.
	PodCapacityLabel = "cloud.google.com/pod-slots"

	// CpuScalingLevelLabel is used to allow system pods to have different DaemonSets based on the number of VM vCPUs.
	// See go/gke-metrics-agent-vertical-scaling-vm-size.
	CpuScalingLevelLabel = "cloud.google.com/gke-cpu-scaling-level"

	// MemoryScalingLevelLabel is used to allow system pods to have different DaemonSets based on the memory size (GB) of VMs.
	// See  go/dpv2-deployment-ekvm
	MemoryScalingLevelLabel = "cloud.google.com/gke-memory-gb-scaling-level"

	// ResourceLabelPrefix is a label prefix which is captured by NAP to control injected node group resource labels
	// Its value is contained not inside the label itself, but rather in a referenced annotation
	// to mitigate character count limits.
	// Details at go/ap-vertex-billing-internal
	ResourceLabelPrefix = "cloud.google.com/resourcelabel_"

	// BootDiskTypeLabelKey is the key for node boot disk type.
	BootDiskTypeLabelKey = "cloud.google.com/gke-boot-disk"

	// BootDiskSizeLabelKey is the key for node boot disk size.
	BootDiskSizeLabelKey = "cloud.google.com/gke-boot-disk-size"

	// BootDiskEncryptionLabelKey is the label key for disk encryption key.
	BootDiskEncryptionLabelKey = "cloud.google.com/gke-boot-disk-encryption-key"

	// BootDiskEncryptionAnnotationKey is the system label key which holds annotation key initially stored
	// in the BootDiskEncryptionLabelKey value. It is only used as part of node selector and annotation specification.
	BootDiskEncryptionAnnotationKey = "cloud.google.com/gke-boot-disk-encryption-key-annotation"

	// PodsPerNodeKey is the key for specifying pod capacity on a node for Autopilot clusters.
	PodsPerNodeKey = "cloud.google.com/pods-per-node"
	// BinpackedSliceOfHardwareValue is the value of PodsPerNodeKey used for binpacked SoHW nodes.
	BinpackedSliceOfHardwareValue = "any"

	// MaxRunDurationLabelKey is the key for max run duration.
	MaxRunDurationLabelKey = "cloud.google.com/gke-max-run-duration-seconds"

	// InstanceTerminationAnnotationKey is the key for getting the time of the termination configured for a node
	InstanceTerminationAnnotationKey = "node.gke.io/machine-termination-datetime"

	// NodeRecycleLeadTimeSecondsLabelKey is the label key used to define how many seconds before the termination of a node, should the node be recycled.
	NodeRecycleLeadTimeSecondsLabelKey = "cloud.google.com/gke-node-recycle-lead-time-seconds"

	// SecondaryBootDisksLabelKey is for secondary boot disks
	// CA internal use only
	SecondaryBootDisksLabelKey = "secondary-boot-disks-label-key"

	// NodeGroupDynamicBootDiskSizeEnabledLabelKey is for node group dynamic boot disk size
	// CA internal use only
	NodeGroupDynamicBootDiskSizeEnabledLabelKey = "cloud.google.com/node-group-dynamic-boot-disk-enabled-label-key"

	// NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey is for node group max pods per node
	// CA internal use only
	NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey = "cloud.google.com/node-group-dynamic-max-pods-per-node-enabled-label-key"

	// PTSDomainKeyAnnotation is the key for getting the PTS (Pod Topology Spread) domain key for a node, used to whitelist this nodeSelector in workload separation checks.
	// CA internal use only
	// We added illegal character to make sure user can't use it.
	PTSDomainKeyAnnotation = "cloud.google.com/pts-domain-key-annotation$"

	// ManagedNodeLabel specifies that a node is Autopilot managed.
	ManagedNodeLabel = "cloud.google.com/autopilot-managed-node"

	// GeneralPurposePodFamilyLabel indicates general purpose pod family for pod-based billing.
	GeneralPurposePodFamilyLabel = "cloud.google.com/general-purpose-pod-family"

	// NodeVersionLabelKey is for node version
	// CA internal use only
	NodeVersionLabelKey = "ccc-node-version"

	// ServiceAccountLabelKey is for service account
	// CA internal use only
	ServiceAccountLabelKey = "ccc-service-account"

	// ImageTypeLabelKey is for image type
	// CA internal use only
	ImageTypeLabelKey = "ccc-image-type"

	// WorkloadTypeLabel specifies type of the workload for Collections Scheduling
	WorkloadTypeLabel = "cloud.google.com/gke-workload-type"

	// NodePoolGroupNameLabel specifies the name of the node pool group, it
	// triggers a creation of Multi-Mig by NAP feature
	NodePoolGroupNameLabel = "cloud.google.com/gke-nodepool-group-name"

	// SpecifiedZonesLabelKey is for CCC zonal preferences
	// CA internal use only
	SpecifiedZonesLabelKey = "ccc-specified-zones"

	// PickedZoneLabelKey is for CCC zone type preferences with TPU multi-host workloads
	// CA internal use only
	PickedZoneLabelKey = "ccc-picked-zone"

	// ZoneTypesLabelKey is for CCC zone type preferences with TPU multi-host workloads
	// CA internal use only
	ZoneTypesLabelKey = "ccc-zone-type"

	// SelfServiceLabelKey is for self-service.
	// CA internal use only
	SelfServiceLabelKey = "self-service"

	// AutoRepairLabelKey is for CCC node auto repair setting.
	// CA internal use only
	AutoRepairLabelKey = "auto-repair"

	// AutoUpgradeLabelKey is for CCC node auto upgrade setting.
	// CA internal use only
	AutoUpgradeLabelKey = "auto-upgrade"

	// MaintenanceExclusionLabelKey is for CCC node pool maintenance exclusion setting.
	// CA internal use only
	MaintenanceExclusionLabelKey = "maintenance-exclusion"

	// DefaultMaxPodsPerNode defines the default max pods per node value used by GKE.
	DefaultMaxPodsPerNode = int64(110)

	// CapacityCheckWaitTimeSecondsLabel is the label key used to define for how many seconds will the given CCC priority be attempted to scale up before falling back to the next priority.
	CapacityCheckWaitTimeSecondsLabel = "cloud.google.com/gke-capacity-check-wait-time-seconds"

	// ImageStreamingLabelKey is for CCC container image streaming settings.
	// CA internal use only
	ImageStreamingLabelKey = "image-streaming"

	// WorkloadMetadataLabelKey is for CCC workload metadata setting.
	// CA internal use only
	WorkloadMetadataLabelKey = "workload-metadata"

	// GvnicLabelKey is for CCC Google Virtual NIC setting.
	GvnicLabelKey = "gvnic"

	// SandboxLabelKey is for sandbox configuration.
	SandboxLabelKey = "sandbox"

	// TagsLabelKey is for specifying resource manager tags to bind to the node pool.
	// CA internal use only
	TagsLabelKey = "resource-manager-tags"

	// SupportedDiskTypeLabelPrefix is the prefix for labels that indicate a node pool supports for a specific disk type.
	SupportedDiskTypeLabelPrefix = "disk-type.gke.io/"

	// DraGpuNodeLabel is the node label indicates that DRA GPU driver is enabled for that node
	DraGpuNodeLabel = "cloud.google.com/gke-nvidia-gpu-dra-driver"

	// DraTpuNodeLabel is the node label indicates that DRA TPU driver is enabled for that node
	DraTpuNodeLabel = "cloud.google.com/gke-tpu-dra-driver"

	// DraNetNodeLabel is the node label indicates that DRANET driver is enabled for that node
	DraNetNodeLabel = "cloud.google.com/gke-networking-dra-driver"

	// Self-service metadata key for nodepools' LocationPolicy setting.
	// CA internal use only
	LocationPolicyLabelKey = "location-policy"

	// ConsolidationDelayLabelKey is the key for consolidation delay label
	ConsolidationDelayLabelKey = "cloud.google.com/gke-consolidation-delay-seconds"

	// Self-service metadata key for nodepool's logging variant config.
	LoggingConfigVariant = "cloud.google.com/gke-logging-variant"

	// ProvisioningRequestLabelKey - QueuedProvisioning node label key used to identify the related Resize Request,
	// and thus the corresponding Provisioning Request.
	// For more see: go/ca-pr-dd
	ProvisioningRequestLabelKey = "autoscaling.gke.io/provisioning-request"

	// GpuDirectLabel corresponds to the gpudirect strategy
	GpuDirectLabel = "cloud.google.com/gke-gpudirect"

	// ComputeClassPriorityIdxLabel marks which priority index the node satisfies.
	// If a CCC node doesn't satisfy any priority index, the label will contain "-1" value.
	// CA internal use only
	ComputeClassPriorityIdxLabel = "cloud.google.com/compute-class-priority-idx"

	// Warning! Consider adding your label to `systemLabelsAddedByClusterAutoscaler` below
)

var (
	ValidConfidentialNodeTypes = map[string]bool{
		SEVConfidentialNodeTypeValue:    true,
		SEVSNPConfidentialNodeTypeValue: true,
		TDXConfidentialNodeTypeValue:    true,
	}

	// gke_manager removes system labels and taints added throughout the pipeline before actually creating a node pool
	// In order for them to be preserved they need to be added here
	systemLabelsAddedByClusterAutoscaler = sets.NewString(
		RequestedMinCpuPlatformLabel,
		ComputeClassLabel,
		AcceleratorCountLabel,
		ExtendedDurationPodsLabel,
		NodeProvisioningConfigLabel,
		PodPerVMSizeLabel,
		PodCapacityLabel,
		ReservationAffinityLabel,
		MaxRunDurationLabelKey,
		ConsolidationDelayLabelKey,
		GPUDriverVersionLabel,
		ManagedNodeLabel,
		GeneralPurposePodFamilyLabel,
		NodeRecycleLeadTimeSecondsLabelKey,
		CapacityCheckWaitTimeSecondsLabel,
		ProvisioningRequestLabelKey,
		LoggingConfigVariant,
		DraNetNodeLabel,
		DraTpuNodeLabel,
		csn.SoftWorkloadSeparationKey,
		AcceleratorNetworkProfileLabel,
		GpuDirectLabel,
		MaintenanceExclusionLabelKey,
	)
	bootDiskLabels = sets.NewString(
		BootDiskSizeLabelKey,
		BootDiskTypeLabelKey,
		BootDiskEncryptionLabelKey,
	)
	systemLabelPrefixesAddedByClusterAutoscaler = sets.NewString(
		SupportedCpuPlatformKeyPrefix,
		ResourceLabelPrefix,
	)

	systemLabelPatterns = []string{
		".*\\bcloud\\.google\\.com\\/.*",
		".*\\bkubernetes\\.io\\/.*",
		".*\\bgke\\.io\\/.*",
		".*\\bk8s\\.io\\/.*",
		".*\\bautoscaling\\.gke\\.io\\/.*",
		".*\\bgke\\.networks\\.io\\/.*",
	}
	systemLabelPattern = regexp.MustCompile(strings.Join(systemLabelPatterns, "|"))
)

// IsSystemLabel tells if a given label key belongs to a system label
func IsSystemLabel(key string) bool {
	return systemLabelPattern.MatchString(key)
}

// IsResourceLabel tells if a given label key belongs to a resource label
func IsResourceLabel(key string) bool {
	return strings.HasPrefix(key, ResourceLabelPrefix)
}

// IsResourceLabel extracts annotation key where the actual value of a resource label is stored
func ExtractResourceLabelAnnotationKey(key string) string {
	return strings.TrimPrefix(key, ResourceLabelPrefix)
}

// IsAddedByClusterAutoscaler tells if a given system label is added by Cluster Autoscaler in a CreateNodePool request,
// instead of being added e.g. by the control plane.
func IsAddedByClusterAutoscaler(key string, bootDiskConfigEnabled bool) bool {
	for prefix := range systemLabelPrefixesAddedByClusterAutoscaler {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}

	if bootDiskConfigEnabled {
		return systemLabelsAddedByClusterAutoscaler.Has(key) || bootDiskLabels.Has(key)
	}

	return systemLabelsAddedByClusterAutoscaler.Has(key)
}

// SupportedCpuPlatformKey creates a supported CPU platform label key based on the provided platform.
func SupportedCpuPlatformKey(platform string) string {
	return SupportedCpuPlatformKeyPrefix + platform
}

// ExtractSupportedCpuPlatformFromKey extracts the CPU platform from a supported CPU platform label key.
func ExtractSupportedCpuPlatformFromKey(key string) string {
	return strings.TrimPrefix(key, SupportedCpuPlatformKeyPrefix)
}

// SystemLabelsForGPU returns set of node system labels when we want node to have GPUs of given type.
func SystemLabelsForGPU(gpuType, gpuPartitionSize, gpuMaxSharedClients, gpuSharingStrategy, gpuDriverVersion string, nodeAutoprovisioningEnabled bool) map[string]string {
	labels := map[string]string{
		GPULabel:                 gpuType,
		GPUPartitionSizeLabel:    gpuPartitionSize,
		GPUMaxSharedClientsLabel: gpuMaxSharedClients,
		GPUSharingStrategyLabel:  gpuSharingStrategy,
		GPUDriverVersionLabel:    gpuDriverVersion,
	}
	// Autopilot auto-installs default GPU drivers for autoprovisioned nodepools
	// when unspecified through a node-selector.
	if nodeAutoprovisioningEnabled && gpuType != "" && gpuDriverVersion == "" {
		labels[GPUDriverVersionLabel] = DefaultGPUDriverVersionValue
	}
	return labels
}

func SupportedDiskTypeKey(diskType string) string {
	return SupportedDiskTypeLabelPrefix + diskType
}

func ConvertGpuSharingStrategyToLabelEnum(s string) string {
	switch s {
	case "TIME_SHARING":
		return GPUTimeSharingStrategy
	case "MPS":
		return GPUMpsStrategy
	default:
		return s
	}
}
