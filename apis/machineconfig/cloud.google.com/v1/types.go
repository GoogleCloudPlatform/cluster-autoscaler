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

package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:storageversion
// +kubebuilder:metadata:labels="addonmanager.kubernetes.io/mode=Reconcile"
// +kubebuilder:metadata:annotations="components.gke.io/layer=addon"
// +kubebuilder:resource:scope=Cluster,shortName=mc;mcs
// +kubebuilder:subresource:status

// MachineConfig defines a machine configuration available in the cluster.
// It is consumed by Cluster Autoscaler and GKE Common Webhooks and impacts the decisions on
// workload admission and cluster autoscaling.
type MachineConfig struct {
	metav1.TypeMeta `json:",inline"`
	// Standard object metadata. More info: https://git.k8s.io/community/contributors/devel/api-conventions.md#metadata
	//
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Specification of the MachineConfig object.
	// More info: https://git.k8s.io/community/contributors/devel/api-conventions.md#spec-and-status.
	// +required
	Spec MachineConfigSpec `json:"spec" protobuf:"bytes,2,name=spec"`
	// Status of the MachineConfig object.
	//
	// +optional
	Status MachineConfigStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MachineConfigList is a list of MachineConfig objects.
type MachineConfigList struct {
	metav1.TypeMeta `json:",inline"`
	// Standard list metadata.
	//
	// +optional
	metav1.ListMeta `json:"metadata" protobuf:"bytes,1,opt,name=metadata"`
	// Items, list of MachineConfig returned from API.
	//
	// +optional
	Items []MachineConfig `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// MachineConfigSpec specifies a machine configuration available in the cluster.
type MachineConfigSpec struct {
	// Machine family specified by this machine configuration.
	MachineFamily MachineFamily `json:"machineFamily"`

	// Version of the configuration stored in this object.
	Version string `json:"version"`
}

// MachineConfigStatus defines the status of the MachineConfig object.
type MachineConfigStatus struct {
	// Conditions represent the observations of a MachineConfig's current state.
	//
	// +optional
	Conditions []metav1.Condition `json:"conditions" protobuf:"bytes,1,rep,name=conditions"`
}

// MachineFamily defines a single machine family.
type MachineFamily struct {
	// Name of this machine family.
	Name string `json:"name"`

	// Default properties of this machine family.
	DefaultProperties MachineProperties `json:"defaultProperties"`

	// Weights contains information about the preference for choosing this machine configuration.
	// Lower weights are preferred for scaling up the cluster.
	//
	// +optional
	Weights *MachineFamilyWeights `json:"weights"`

	// Machine types available for this machine family.
	MachineTypes []MachineType `json:"machineTypes"`

	// Accelerators available for this machine family.
	//
	// +optional
	Accelerators []Accelerator `json:"accelerators"`

	// ComputeClasses supporting this machine family.
	//
	// +optional
	ComputeClasses []string `json:"computeClasses"`
}

// MachineProperties defines properties of a machine family/type.
type MachineProperties struct {
	// System architecture available for this machine configuration.
	//
	// +optional
	SystemArchitecture *string `json:"systemArchitecture"`

	// CPU platforms available for this machine configuration.
	//
	// +optional
	CPUPlatforms []CPUPlatform `json:"cpuPlatforms"`

	// Compact placement options available for this machine configuration.
	//
	// +optional
	CompactPlacementConfig *CompactPlacementConfig `json:"compactPlacementConfig"`

	// ThreadsPerCore for the machine configuration.
	//
	// +optional
	ThreadsPerCore *int64 `json:"threadsPerCore"`

	// BootDiskConfig specifies the boot disk configuration.
	//
	// +optional
	BootDiskConfig *BootDiskConfig `json:"bootDiskConfig"`

	// NAPDisabled specifies if the machine configuration is excluded from autoprovisioning support.
	//
	// +optional
	NAPDisabled *bool `json:"napDisabled"`

	// SupportsConfidentialNodes specifies whether confidential nodes are supported for this machine
	// configuration.
	// TODO(b/505564007): remove this field before launch in favor of ConfidentialNodeConfig.Supported
	//
	// +optional
	SupportsConfidentialNodes *bool `json:"supportsConfidentialNodes"`

	// ConfidentialNodeConfig specifies the confidential node configuration supported by the machine configuration.
	//
	// +optional
	ConfidentialNodeConfig *ConfidentialNodeConfig `json:"confidentialNodeConfig,omitempty"`

	// Whether the machine family is excluded from NUMA alignment support.
	//
	// +optional
	NumaAlignmentUnsupported bool `json:"numaAlignmentUnsupported"`
}

// MachineType defines a machine type.
type MachineType struct {
	// Name of the machine type.
	Name string `json:"name"`

	// Resources specifies the fixed resources of this machine type.
	Resources MachineResources `json:"resources"`

	// Weights contains information about specific Machine Type weight overrides.
	//
	// +optional
	Weights *MachineTypeWeights `json:"weights"`

	// Properties specific to this machine type.
	// If set, these properties override the default properties defined in the MachineFamily.
	// Any property not explicitly set here inherits the value from MachineFamily.DefaultProperties.
	//
	// +optional
	Properties *MachineProperties `json:"properties"`
}

// MachineResources defines resources of a machine type.
type MachineResources struct {
	// CPU count of the machine type.
	CPUs int64 `json:"cpus"`

	// Memory in MiB of the machine type.
	Memory int64 `json:"memory"`

	// LocalSSDConfig contains information about the local SSD configurations
	// available for the machine type.
	//
	// +optional
	LocalSSDConfig *LocalSSDConfig `json:"LocalSSDConfig"`

	// Accelerator provisioned with this machine type.
	//
	// +optional
	Accelerator *AcceleratorSpec `json:"accelerator"`
}

// MachineTypeWeights contains information about the overrides for the preference for choosing the machine configuration.
type MachineTypeWeights struct {
	// InstanceWeight provides an override for the weight of a single non-preemptible instance of this machine type.
	//
	// +optional
	InstanceWeight *string `json:"instanceWeight"`

	// PreemptibleInstanceWeight provides an override for the weight of a single preemptible instance of this machine type.
	//
	// +optional
	PreemptibleInstanceWeight *string `json:"preemptibleInstanceWeight"`
}

// Accelerator defines an accelerator type.
type Accelerator struct {
	// Name of the accelerator.
	Name string `json:"name"`

	// Accelerator configurations available for this accelerator type.
	Configurations []AcceleratorConfig `json:"configurations"`

	// PartitionSizes contains the partition sizes supported for the accelerator.
	// Some gpus offer partial amount of gpus to be used. This encapsulates the number of partitions
	// based on the partition spec.
	//
	// +optional
	PartitionSizes map[string]int64 `json:"partitionSizes"`

	// Weight contains the weight for the accelerator.
	Weight string `json:"weight"`

	// PreemptibleWeight contains the weight for the preemptible accelerator.
	//
	// +optional
	PreemptibleWeight *string `json:"preemptibleWeight"`

	// ProviderType is the provider type of the accelerator.
	//
	// +kubebuilder:validation:Enum=nvidia;tpu;unspecified
	// +optional
	ProviderType *string `json:"providerType"`

	// Topologies contains the topologies supported by the accelerator.
	//
	// +optional
	Topologies []string `json:"topologies"`

	// MultiInstanceSharingSupported indicates if multi instance sharing is supported by the accelerator.
	//
	// +optional
	MultiInstanceSharingSupported *bool `json:"multiInstanceSharingSupported"`

	// TimeSharingSupported indicates if time sharing is supported by the accelerator.
	//
	// +optional
	TimeSharingSupported *bool `json:"timeSharingSupported"`
}

// AcceleratorConfig defines an available accelerator configuration.
type AcceleratorConfig struct {
	// Count specifies the number of accelerators in this configuration.
	Count int64 `json:"count"`

	// Max CPUs for a machine with this configuration.
	MaxCPUs int64 `json:"maxCPUs"`

	// Max memory for a machine with this configuration.
	MaxMemory int64 `json:"maxMemory"`
}

// AcceleratorSpec defines a fixed accelerator specification for the machine.
type AcceleratorSpec struct {
	// Name of the accelerator.
	Name string `json:"name"`

	// FixedCount indicates the number of accelerators of this type always attached to the machine.
	FixedCount int64 `json:"fixedCount"`

	// TpuConfig supported by the machine type.
	//
	// +optional
	TpuConfig *TpuConfig `json:"tpuConfig"`
}

// CPUPlatform defines a CPU platform.
type CPUPlatform struct {
	// Name of the CPU platform.
	Name string `json:"name"`

	// Aliases contains possible alternative names for this CPU platform.
	//
	// +optional
	Aliases []string `json:"aliases"`

	// Vendor contains the CPU vendor this platform belongs to.
	//
	// +optional
	Vendor *string `json:"vendor"`

	// VendorOrder contains the relative order of the CPU platform for that vendor, from oldest to newest.
	//
	// +optional
	VendorOrder *int64 `json:"vendorOrder"`
}

// CompactPlacementConfig contains information about the compact placement options available for
// the machine configuration.
type CompactPlacementConfig struct {
	// Supported indicates whether the compact placement config is supported.
	Supported bool `json:"supported"`

	// MaxCount indicates the maximum number of machines that can be compactly placed.
	//
	// +optional
	MaxCount *int64 `json:"maxCount"`
}

// LocalSSDConfig contains information about the local SSDs.
type LocalSSDConfig struct {
	// DefaultCount contains the default number of local SSDs.
	DefaultCount int64 `json:"localSsdCount"`

	// AvailableCounts contains the possible numbers of local SSDs.
	//
	// +optional
	AvailableCounts []int64 `json:"availableCounts"`

	// DiskSize contains the size of a single local SSD in GB.
	DiskSize int64 `json:"diskSize"`

	// MaxTotalStorage contains the maximum total storage of local SSDs.
	MaxTotalStorage int64 `json:"maxTotalStorage"`
}

// MachineFamilyWeights contains information about the preference for choosing the machine configuration.
type MachineFamilyWeights struct {
	// Predefined contains weights for the predefined types.
	Predefined ResourceWeights `json:"predefined"`

	// Custom contains weights for the custom types.
	//
	// +optional
	Custom *ResourceWeights `json:"custom"`

	// Preemptible contains weights for the preemptible types.
	//
	// +optional
	Preemptible *ResourceWeights `json:"preemptible"`
}

// ResourceWeights contains information about the preference for choosing the machine configuration
// based on resources.
type ResourceWeights struct {
	// CPU contains weight for the CPU resource.
	CPU string `json:"cpu"`

	// Memory contains weight for the memory resource.
	Memory string `json:"memory"`

	// LocalSSD contains weight for the local SSD resource.
	LocalSSD string `json:"localSSD"`
}

// BootDiskConfig contains information about the boot disk configuration.
type BootDiskConfig struct {
	// Types specifies the boot disk types available for this machine configuration.
	//
	// +optional
	Types []string `json:"types"`

	// DefaultType specifies the default boot disk type for this machine configuration.
	//
	// +optional
	DefaultType string `json:"defaultType"`
}

// TpuConfig defines the TPU configuration.
type TpuConfig struct {
	// SingleHostTopology supported by the machine type.
	//
	// +optional
	SingleHostTopology *string `json:"singleHostTopology"`
}

// ConfidentialNodeConfig defines the confidential node configuration.
type ConfidentialNodeConfig struct {
	// Supported specifies if confidential nodes are supported.
	//
	// +optional
	Supported *bool `json:"supported,omitempty"`

	// Types specifies the confidential instance types supported.
	//
	// +optional
	Types []ConfidentialNodeType `json:"types,omitempty"`
}

// ConfidentialNodeType defines the confidential instance type.
type ConfidentialNodeType struct {
	// Type specifies the confidential instance type (e.g. SEV, SEV_SNP, TDX).
	//
	// +optional
	Type string `json:"type,omitempty"`
}
