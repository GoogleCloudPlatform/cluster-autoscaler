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
	"context"
	"sync"
	"sync/atomic"

	flexadvisorapi "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"
)

const (
	// DefaultZonalCapacity is the default available instance count when no capacity guidance matches.
	DefaultZonalCapacity = 1000
	// DefaultZonalScore is the default GCE preference score when no capacity guidance matches.
	DefaultZonalScore = 0.5
)

type FakeFlexAdvisorClient struct {
	mu                               sync.RWMutex
	fetchCapacityCalls               int32
	capacityGuidances                []FakeCapacityGuidance
	capacityGuidanceResponseModifier func(results map[string]*flexadvisorapi.InstanceAvailability, err error) (map[string]*flexadvisorapi.InstanceAvailability, error)
}

func (c *FakeFlexAdvisorClient) FetchCapacityGuidance(ctx context.Context, flexibilityScopeKey string, instanceConfigs map[string]*flexadvisorapi.InstanceConfig) (map[string]*flexadvisorapi.InstanceAvailability, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	atomic.AddInt32(&c.fetchCapacityCalls, 1)

	results, err := c.fetchCapacityGuidance(flexibilityScopeKey, instanceConfigs)
	if c.capacityGuidanceResponseModifier != nil {
		return c.capacityGuidanceResponseModifier(results, err)
	}
	return results, err
}

func (c *FakeFlexAdvisorClient) fetchCapacityGuidance(flexibilityScopeKey string, instanceConfigs map[string]*flexadvisorapi.InstanceConfig) (map[string]*flexadvisorapi.InstanceAvailability, error) {
	results := make(map[string]*flexadvisorapi.InstanceAvailability)
	for key, config := range instanceConfigs {
		if config == nil {
			continue
		}

		zonalCapacity := make(map[string]int)
		zonalScore := make(map[string]float64)

		var zones []string
		if config.Zones() != nil {
			zones = config.Zones().UnsortedList()
		}

		for _, zone := range zones {
			var matched *FakeCapacityGuidance
			for _, guidance := range c.capacityGuidances {
				if guidance.matches(config, zone) {
					matched = &guidance
					break
				}
			}

			if matched != nil {
				if matched.Error != nil {
					return nil, matched.Error
				}
				zonalCapacity[zone] = matched.InstanceCount
				zonalScore[zone] = matched.GcePreferenceScore
			} else {
				zonalCapacity[zone] = DefaultZonalCapacity
				zonalScore[zone] = DefaultZonalScore
			}
		}

		availability := flexadvisorapi.NewTestInstanceAvailabilityBuilder(flexibilityScopeKey, key).
			WithZonalInstanceCount(zonalCapacity).
			WithZonalGcePreferenceScore(zonalScore).
			Build()
		results[key] = availability
	}
	return results, nil
}

func (c *FakeFlexAdvisorClient) SendCapacityDecision(ctx context.Context, decision flexadvisorapi.ProvisioningDecisionNotification) error {
	return nil
}

func (c *FakeFlexAdvisorClient) GetFetchCapacityCalls() int {
	return int(atomic.LoadInt32(&c.fetchCapacityCalls))
}

// AddCapacityGuidances adds multiple capacity guidances that will be used to generate the response.
// The first matched rule wins, so start with the most specific rules and finish with most generic.
func (c *FakeFlexAdvisorClient) AddCapacityGuidances(guidances ...FakeCapacityGuidance) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.capacityGuidances = append(c.capacityGuidances, guidances...)
}

// ClearCapacityGuidances clears all fake capacity guidances.
func (c *FakeFlexAdvisorClient) ClearCapacityGuidances() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.capacityGuidances = nil
}

// SetCapacityGuidanceResponseModifier allows modifying capacity guidance response.
func (c *FakeFlexAdvisorClient) SetCapacityGuidanceResponseModifier(modifier func(results map[string]*flexadvisorapi.InstanceAvailability, err error) (map[string]*flexadvisorapi.InstanceAvailability, error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.capacityGuidanceResponseModifier = modifier
}

// NewFakeCapacityGuidanceForMachineType creates a FakeCapacityGuidance that matches the specified machineType and returns the given instanceCount and score.
func NewFakeCapacityGuidanceForMachineType(machineType string, instanceCount int, gcePreferenceScore float64) FakeCapacityGuidance {
	return FakeCapacityGuidance{
		MachineType:        &machineType,
		InstanceCount:      instanceCount,
		GcePreferenceScore: gcePreferenceScore,
	}
}
