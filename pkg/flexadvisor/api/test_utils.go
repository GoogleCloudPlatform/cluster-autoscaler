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

import "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"

// TestInstanceAvailabilityBuilder is a test utility for creating mock InstanceAvailability objects.
type TestInstanceAvailabilityBuilder struct {
	zonalInstanceCount              map[string]int
	zonalProvisionsSinceLastRefresh map[string]int
	zonalGcePreferenceScore         map[string]float64
	guidanceId                      string
	instanceConfigKey               string
	flexibilityScopeKey             string
	decisionChan                    chan ProvisioningDecisionNotification
	provider                        instanceavailability.Provider
}

// NewTestInstanceAvailabilityBuilder creates a new TestInstanceAvailabilityBuilder
func NewTestInstanceAvailabilityBuilder(flexibilityScopeKey, instanceConfigKey string) *TestInstanceAvailabilityBuilder {
	return &TestInstanceAvailabilityBuilder{
		flexibilityScopeKey:             flexibilityScopeKey,
		instanceConfigKey:               instanceConfigKey,
		zonalInstanceCount:              make(map[string]int),
		zonalProvisionsSinceLastRefresh: make(map[string]int),
		zonalGcePreferenceScore:         make(map[string]float64),
	}
}

// Build constructs and returns a new InstanceAvailability object based on the builder's current configuration.
func (b *TestInstanceAvailabilityBuilder) Build() *InstanceAvailability {
	return &InstanceAvailability{
		zonalInstanceCount:              b.zonalInstanceCount,
		zonalProvisionsSinceLastRefresh: b.zonalProvisionsSinceLastRefresh,
		zonalGcePreferenceScore:         b.zonalGcePreferenceScore,
		guidanceId:                      b.guidanceId,
		instanceConfigKey:               b.instanceConfigKey,
		flexibilityScopeKey:             b.flexibilityScopeKey,
		decisionChan:                    b.decisionChan,
		provider:                        b.provider,
	}
}

// WithZonalInstanceCount sets the zonalInstanceCount map for the object to be built.
func (b *TestInstanceAvailabilityBuilder) WithZonalInstanceCount(zonalInstanceCount map[string]int) *TestInstanceAvailabilityBuilder {
	b.zonalInstanceCount = zonalInstanceCount
	return b
}

// WithZonalProvisionsSinceLastRefresh sets the zonalProvisionsSinceLastRefresh map for the object to be built.
func (b *TestInstanceAvailabilityBuilder) WithZonalProvisionsSinceLastRefresh(zonalProvisionsSinceLastRefresh map[string]int) *TestInstanceAvailabilityBuilder {
	b.zonalProvisionsSinceLastRefresh = zonalProvisionsSinceLastRefresh
	return b
}

// WithZonalGcePreferenceScore sets the zonalGcePreferenceScore map for the object to be built.
func (b *TestInstanceAvailabilityBuilder) WithZonalGcePreferenceScore(zonalGcePreferenceScore map[string]float64) *TestInstanceAvailabilityBuilder {
	b.zonalGcePreferenceScore = zonalGcePreferenceScore
	return b
}
