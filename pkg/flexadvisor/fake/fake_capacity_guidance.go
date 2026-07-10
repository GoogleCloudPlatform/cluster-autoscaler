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

// FakeCapacityGuidance is used to provide mocked capacity guidance data for testing.
// It contains filters that determine which InstanceConfig this guidance applies to
// and the response to return for the given filters.
type FakeCapacityGuidance struct {
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

func (g FakeCapacityGuidance) matches(realConfig *flexadvisorapi.InstanceConfig, zone string) bool {
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
