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

package fake

import (
	flexadvisorapi "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
)

// CapacityGuidance is used to provide mocked capacity guidance data for testing.
// It contains filters that determine which InstanceConfig this guidance applies to
// and the response to return for the given filters.
type CapacityGuidance struct {
	// Request matchers (nil value matches all):
	MachineType             *string
	ProvisioningMode        *instanceavailability.ProvisioningMode
	GpuType                 *string
	GpuCount                *int
	Rank                    *int
	Zone                    *string
	MaxRunDurationInSeconds *string
	WorkloadPolicies        *flexadvisorapi.WorkloadPolicies

	// Response:
	InstanceCount      int
	GcePreferenceScore float64
	Error              error
	Omit               bool
}

func (g CapacityGuidance) matches(realConfig *flexadvisorapi.InstanceConfig, zone string) bool {
	if g.MachineType != nil && *g.MachineType != realConfig.MachineType() {
		return false
	}
	if g.ProvisioningMode != nil && *g.ProvisioningMode != realConfig.ProvisioningMode() {
		return false
	}
	if g.GpuType != nil && *g.GpuType != realConfig.GpuType() {
		return false
	}
	if g.GpuCount != nil && *g.GpuCount != realConfig.GpuCount() {
		return false
	}
	if g.Rank != nil && *g.Rank != realConfig.Rank() {
		return false
	}
	if g.MaxRunDurationInSeconds != nil && *g.MaxRunDurationInSeconds != realConfig.MaxRunDurationInSeconds() {
		return false
	}
	if g.WorkloadPolicies != nil && g.WorkloadPolicies.AcceleratorTopology != realConfig.WorkloadPolicies().AcceleratorTopology {
		return false
	}
	if g.Zone != nil && *g.Zone != zone {
		return false
	}
	return true
}

// NewGuidance creates a new CapacityGuidance matching the specified machineType,
// defaulted to DefaultZonalCapacity (1000) and DefaultZonalScore (0.5).
func NewGuidance(machineType string) CapacityGuidance {
	return CapacityGuidance{
		MachineType:        &machineType,
		InstanceCount:      DefaultZonalCapacity,
		GcePreferenceScore: DefaultZonalScore,
	}
}

// WithCapacity sets the available instance count.
func (g CapacityGuidance) WithCapacity(count int) CapacityGuidance {
	g.InstanceCount = count
	return g
}

// WithScore sets the GCE preference score.
func (g CapacityGuidance) WithScore(score float64) CapacityGuidance {
	g.GcePreferenceScore = score
	return g
}

// WithZone sets the target zone filter.
func (g CapacityGuidance) WithZone(zone string) CapacityGuidance {
	g.Zone = &zone
	return g
}

// WithProvisioningMode sets the target provisioning mode filter.
func (g CapacityGuidance) WithProvisioningMode(mode instanceavailability.ProvisioningMode) CapacityGuidance {
	g.ProvisioningMode = &mode
	return g
}

// WithRank sets the target rank filter.
func (g CapacityGuidance) WithRank(rank int) CapacityGuidance {
	g.Rank = &rank
	return g
}

// WithGpuType sets the target GPU type filter.
func (g CapacityGuidance) WithGpuType(gpuType string) CapacityGuidance {
	g.GpuType = &gpuType
	return g
}

// WithGpuCount sets the target GPU count filter.
func (g CapacityGuidance) WithGpuCount(count int) CapacityGuidance {
	g.GpuCount = &count
	return g
}

// WithMaxRunDurationInSeconds sets the target max run duration filter.
func (g CapacityGuidance) WithMaxRunDurationInSeconds(duration string) CapacityGuidance {
	g.MaxRunDurationInSeconds = &duration
	return g
}

// WithWorkloadPolicies sets the target workload policies filter.
func (g CapacityGuidance) WithWorkloadPolicies(policies *flexadvisorapi.WorkloadPolicies) CapacityGuidance {
	g.WorkloadPolicies = policies
	return g
}

// WithError sets an error to be returned when this guidance matches.
func (g CapacityGuidance) WithError(err error) CapacityGuidance {
	g.Error = err
	return g
}

// WithOmit sets whether this guidance should be omitted.
func (g CapacityGuidance) WithOmit(omit bool) CapacityGuidance {
	g.Omit = omit
	return g
}
