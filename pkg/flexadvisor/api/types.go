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

package api

import (
	"context"
	"strconv"
	"strings"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/utils/set"
)

// AdviceProvider defines the contract for fetching capacity guidance and reporting provisioning decisions.
type AdviceProvider interface {
	// FetchCapacityGuidance queries the GCE for current capacity availability insights
	// for a given set of desired instance configurations.
	FetchCapacityGuidance(ctx context.Context, flexibilityScopeKey string, instanceConfigs map[string]*InstanceConfig) (map[string]*InstanceAvailability, error)
	// SendCapacityDecision notifies a final provisioning decision, allowing it to
	// propagate the decision back to GCE
	SendCapacityDecision(ctx context.Context, decision ProvisioningDecisionNotification) error
}

const EmptyMaxRunDuration = ""

// WorkloadPolicies represents policies applied to the workload.
type WorkloadPolicies struct {
	AcceleratorTopology string
}

// InstanceOption is a function type for modifying an InstanceConfig.
type InstanceOption func(instanceConfig *InstanceConfig)

// WithWorkloadPolicies adds workload policies to the instance configuration.
func WithWorkloadPolicies(workloadPolicies WorkloadPolicies) InstanceOption {
	return func(instanceConfig *InstanceConfig) {
		instanceConfig.workloadPolicies = WorkloadPolicies{
			AcceleratorTopology: workloadPolicies.AcceleratorTopology,
		}
	}
}

// InstanceConfig represents a specific desired machine configuration, including
// machine type, GPU, provisioning mode, and applicable zones among other fields
type InstanceConfig struct {
	machineType             string
	provisioningMode        instanceavailability.ProvisioningMode
	gpuType                 string
	gpuCount                int
	rank                    int
	zones                   set.Set[string]
	maxRunDurationInSeconds string
	workloadPolicies        WorkloadPolicies
}

// NewInstanceConfig creates a new InstanceConfig with an empty set of zones.
// TODO(b/491088027): move non-required fields(gpuType, gpuCount, MRD) to options builder architecture
func NewInstanceConfig(machineType, gpuType string, gpuCount, rank int, mode instanceavailability.ProvisioningMode, maxRunDurationInSeconds string) *InstanceConfig {
	return NewInstanceConfigWithZones(machineType, gpuType, gpuCount, rank, mode, maxRunDurationInSeconds, set.New[string]())
}

// NewInstanceConfigWithZones creates a new InstanceConfig with a pre-populated set of zones.
func NewInstanceConfigWithZones(machineType, gpuType string, gpuCount, rank int, mode instanceavailability.ProvisioningMode, maxRunDurationInSeconds string, zones set.Set[string], opts ...InstanceOption) *InstanceConfig {
	instanceConfig := &InstanceConfig{
		machineType:             machineType,
		provisioningMode:        mode,
		gpuType:                 gpuType,
		gpuCount:                gpuCount,
		rank:                    rank,
		maxRunDurationInSeconds: maxRunDurationInSeconds,
		zones:                   zones,
	}
	for _, opt := range opts {
		opt(instanceConfig)
	}
	return instanceConfig
}

// DeepCopyInstanceConfig creates a new, deep copy of the given InstanceConfig.
func DeepCopyInstanceConfig(config *InstanceConfig) *InstanceConfig {
	return NewInstanceConfigWithZones(
		config.machineType,
		config.gpuType,
		config.gpuCount,
		config.rank,
		config.provisioningMode,
		config.maxRunDurationInSeconds,
		config.zones.Clone(),
		WithWorkloadPolicies(config.workloadPolicies),
	)
}

// MachineType returns the machine type.
func (i *InstanceConfig) MachineType() string {
	return i.machineType
}

// ProvisioningMode returns the provisioning mode.
func (i *InstanceConfig) ProvisioningMode() instanceavailability.ProvisioningMode {
	return i.provisioningMode
}

// GpuType returns the GPU accelerator type.
func (i *InstanceConfig) GpuType() string {
	return i.gpuType
}

// GpuCount returns the number of GPUs.
func (i *InstanceConfig) GpuCount() int {
	return i.gpuCount
}

// Zones returns the set of zones where this configuration is applicable.
func (i *InstanceConfig) Zones() set.Set[string] {
	return i.zones
}

// Rank returns the rank.
func (i *InstanceConfig) Rank() int {
	return i.rank
}

// MaxRunDurationInSeconds returns the maximum run duration in seconds
func (i *InstanceConfig) MaxRunDurationInSeconds() string {
	return i.maxRunDurationInSeconds
}

// WorkloadPolicies returns the workload policies.
func (i *InstanceConfig) WorkloadPolicies() WorkloadPolicies {
	return i.workloadPolicies
}

// SetWorkloadPolicies sets the workload policies.
func (i *InstanceConfig) SetWorkloadPolicies(policies WorkloadPolicies) {
	i.workloadPolicies = policies
}

// SetGpuType sets the GPU type for the configuration.
func (i *InstanceConfig) SetGpuType(gpuType string) {
	i.gpuType = gpuType
}

// SetGpuCount sets the GPU count for the configuration.
func (i *InstanceConfig) SetGpuCount(count int) {
	i.gpuCount = count
}

// SetProvisioningMode sets the provisioning mode for the configuration.
func (i *InstanceConfig) SetProvisioningMode(mode instanceavailability.ProvisioningMode) {
	i.provisioningMode = mode
}

// SetMaxRunDurationInSeconds sets the maximum run duration in seconds.
func (i *InstanceConfig) SetMaxRunDurationInSeconds(maxRunDurationInSeconds string) {
	i.maxRunDurationInSeconds = maxRunDurationInSeconds
}

// InsertZone adds a zone to the configuration's set of zones.
func (i *InstanceConfig) InsertZone(zone string) {
	i.zones.Insert(zone)
}

// MergeZones adds all zones from the given set to the configuration's set.
func (i *InstanceConfig) MergeZones(zones set.Set[string]) {
	i.zones = i.zones.Union(zones)
}

// Signature returns string representation of InstanceConfig.
// CCC priority priorityScore is not considered since the same configuration is not expected to be in different CCC priorities.
// Zones are not included in the signature. Including zones makes it impossible search InstanceConfig from information gathered from a mig.
func (i *InstanceConfig) Signature() string {
	builder := strings.Builder{}
	builder.WriteString("machineType: ")
	builder.WriteString(i.machineType)
	builder.WriteString(", provisioningMode: ")
	builder.WriteString(string(i.provisioningMode))
	if i.gpuType != "" {
		builder.WriteString(", gpuType: ")
		builder.WriteString(i.gpuType)
		builder.WriteString(", gpuCount: ")
		builder.WriteString(strconv.Itoa(i.gpuCount))
	}
	if i.maxRunDurationInSeconds != EmptyMaxRunDuration {
		builder.WriteString(", maxRunDuration: ")
		builder.WriteString(i.maxRunDurationInSeconds)
	}
	if i.workloadPolicies.AcceleratorTopology != "" {
		builder.WriteString(", acceleratorTopology: ")
		builder.WriteString(i.workloadPolicies.AcceleratorTopology)
	}
	return builder.String()
}

type ProvisioningDecisionNotification struct {
	flexibilityScopeKey       string
	instanceConfigKey         string
	guidanceId                string
	decisionId                string
	zonalInstancesToProvision map[string]int
}

// NewProvisioningDecisionNotification return a new ProvisioningDecisionNotification
func NewProvisioningDecisionNotification(flexibilityScopeKey, instanceConfigKey, guidanceId, decisionId string, zonalInstancesToProvision map[string]int) ProvisioningDecisionNotification {
	return ProvisioningDecisionNotification{
		flexibilityScopeKey:       flexibilityScopeKey,
		instanceConfigKey:         instanceConfigKey,
		guidanceId:                guidanceId,
		decisionId:                decisionId,
		zonalInstancesToProvision: zonalInstancesToProvision,
	}
}

// FlexibilityScopeKey returns the flexibility scope key.
func (n *ProvisioningDecisionNotification) FlexibilityScopeKey() string {
	return n.flexibilityScopeKey
}

// InstanceConfigKey returns the instance configuration key.
func (n *ProvisioningDecisionNotification) InstanceConfigKey() string {
	return n.instanceConfigKey
}

// GuidanceId returns the ID of the guidance that informed this decision.
func (n *ProvisioningDecisionNotification) GuidanceId() string {
	return n.guidanceId
}

// DecisionId returns the unique ID for this provisioning decision.
func (n *ProvisioningDecisionNotification) DecisionId() string {
	return n.decisionId
}

// ZonalInstancesToProvision returns a map of zones to the number of instances that are to be provisioned.
func (n *ProvisioningDecisionNotification) ZonalInstancesToProvision() map[string]int {
	return n.zonalInstancesToProvision
}
