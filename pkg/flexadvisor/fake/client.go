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
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	flexadvisorapi "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
)

const (
	// DefaultZonalCapacity is the default available instance count when no capacity guidance matches.
	DefaultZonalCapacity = 1000
	// DefaultZonalScore is the default GCE preference score when no capacity guidance matches.
	DefaultZonalScore = 0.5
)

type FakeFlexAdvisorClient struct {
	mu                 sync.RWMutex
	fetchCapacityCalls int32
	capacityGuidances  []CapacityGuidance
	delay              time.Duration
	experimentsManager experiments.Manager
}

func (c *FakeFlexAdvisorClient) FetchCapacityGuidance(ctx context.Context, flexibilityScopeKey string, instanceConfigs map[string]*flexadvisorapi.InstanceConfig) (map[string]*flexadvisorapi.InstanceAvailability, error) {
	c.mu.RLock()
	delay := c.delay
	c.mu.RUnlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
			return nil, context.DeadlineExceeded
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	atomic.AddInt32(&c.fetchCapacityCalls, 1)

	return c.fetchCapacityGuidance(flexibilityScopeKey, instanceConfigs)
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

		var guidanceId string
		var specKey string

		for _, zone := range zones {
			var matched *CapacityGuidance
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
				if matched.Omit {
					continue
				}
				zonalCapacity[zone] = matched.InstanceCount
				zonalScore[zone] = matched.GcePreferenceScore
				if matched.GuidanceId != "" {
					guidanceId = matched.GuidanceId
				}
				if matched.SpecKey != "" {
					specKey = matched.SpecKey
				}
			} else {
				zonalCapacity[zone] = DefaultZonalCapacity
				zonalScore[zone] = DefaultZonalScore
			}
		}

		if len(zonalCapacity) == 0 {
			continue
		}
		builder := flexadvisorapi.NewTestInstanceAvailabilityBuilder(flexibilityScopeKey, key).
			WithZonalInstanceCount(zonalCapacity).
			WithZonalGcePreferenceScore(zonalScore)
		if guidanceId != "" {
			builder = builder.WithGuidanceId(guidanceId)
			if c.experimentsManager != nil && experiments.IsDemandFungibilityImpactTrackingEnabled(c.experimentsManager) {
				metrics.Metrics.RegisterDemandFungibilityExtracted(metrics.FA)
			}
		} else if c.experimentsManager != nil && experiments.IsDemandFungibilityImpactTrackingEnabled(c.experimentsManager) {
			metrics.Metrics.RegisterDemandFungibilityMissingId(metrics.FA, "MissingGuidanceId")
		}
		if specKey != "" {
			builder = builder.WithSpecKey(specKey)
		}
		availability := builder.Build()
		results[key] = availability
	}
	return results, nil
}

// WithExperimentsManager sets the experiments manager for the fake client.
func (c *FakeFlexAdvisorClient) WithExperimentsManager(em experiments.Manager) *FakeFlexAdvisorClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.experimentsManager = em
	return c
}

func (c *FakeFlexAdvisorClient) SendCapacityDecision(ctx context.Context, decision flexadvisorapi.ProvisioningDecisionNotification) error {
	return nil
}

func (c *FakeFlexAdvisorClient) GetFetchCapacityCalls() int {
	return int(atomic.LoadInt32(&c.fetchCapacityCalls))
}

// AddCapacityGuidances adds multiple capacity guidances that will be used to generate the response.
// The first matched rule wins, so start with the most specific rules and finish with most generic.
func (c *FakeFlexAdvisorClient) AddCapacityGuidances(guidances ...CapacityGuidance) {
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

// SetDelay sets a delay for FetchCapacityGuidance to simulate timeout.
func (c *FakeFlexAdvisorClient) SetDelay(delay time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.delay = delay
}
