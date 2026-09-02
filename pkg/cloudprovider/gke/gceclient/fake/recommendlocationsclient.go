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
	"fmt"
	"slices"
	"sort"
	"sync"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
)

// RecommendLocationsClient is a fake implementation of gceclient.RecommendLocationsClient for integration testing.
type RecommendLocationsClient struct {
	mu                 sync.RWMutex
	recommendationID   string
	specKey            string
	err                error
	customResponse     *gceclient.RecommendLocationsResponse
	customHandler      func(ctx context.Context, region string, request gceclient.RecommendLocationsRequest) (*gceclient.RecommendLocationsResponse, error)
	calls              []gceclient.RecommendLocationsRequest
	experimentsManager experiments.Manager
}

// NewRecommendLocationsClient creates a new fake RecommendLocationsClient with sensible defaults.
func NewRecommendLocationsClient() *RecommendLocationsClient {
	return &RecommendLocationsClient{
		recommendationID: "fake-rla-recommendation-id",
		specKey:          "recommend-locations-nodes",
	}
}

// WithRecommendationID sets the recommendation ID.
func (c *RecommendLocationsClient) WithRecommendationID(id string) *RecommendLocationsClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recommendationID = id
	return c
}

// WithSpecKey sets the specification key.
func (c *RecommendLocationsClient) WithSpecKey(key string) *RecommendLocationsClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.specKey = key
	return c
}

// WithError sets an error to be returned on RecommendLocations calls.
func (c *RecommendLocationsClient) WithError(err error) *RecommendLocationsClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err = err
	return c
}

// WithCustomResponse sets a custom response to be returned.
func (c *RecommendLocationsClient) WithCustomResponse(resp *gceclient.RecommendLocationsResponse) *RecommendLocationsClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.customResponse = resp
	return c
}

// WithExperimentsManager sets the experiments manager for the fake client.
func (c *RecommendLocationsClient) WithExperimentsManager(em experiments.Manager) *RecommendLocationsClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.experimentsManager = em
	return c
}

// SetRecommendationID sets the recommendation ID.
func (c *RecommendLocationsClient) SetRecommendationID(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recommendationID = id
}

// SetSpecKey sets the specification key.
func (c *RecommendLocationsClient) SetSpecKey(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.specKey = key
}

// SetError sets an error to be returned.
func (c *RecommendLocationsClient) SetError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err = err
}

// SetCustomResponse sets a custom response to be returned.
func (c *RecommendLocationsClient) SetCustomResponse(resp *gceclient.RecommendLocationsResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.customResponse = resp
}

// GetCalls returns a slice of all RecommendLocations requests received.
func (c *RecommendLocationsClient) GetCalls() []gceclient.RecommendLocationsRequest {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return slices.Clone(c.calls)
}

func (c *RecommendLocationsClient) RecommendLocations(ctx context.Context, region string, request gceclient.RecommendLocationsRequest) (*gceclient.RecommendLocationsResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, request)

	if c.err != nil {
		return nil, c.err
	}
	if c.customResponse != nil {
		c.registerMetrics(c.customResponse.RecommendationID)
		return c.customResponse, nil
	}
	if c.customHandler != nil {
		resp, err := c.customHandler(ctx, region, request)
		if err == nil && resp != nil {
			c.registerMetrics(resp.RecommendationID)
		}
		return resp, err
	}

	var allowedZones []string
	for zone, setting := range request.LocationSettings {
		if setting.ZonePreference == gceclient.ZonePreferenceAllow && setting.MaxScaleUpSize > 0 {
			allowedZones = append(allowedZones, zone)
		}
	}
	sort.Strings(allowedZones)

	if len(allowedZones) == 0 {
		return nil, fmt.Errorf("no allowed zones with available capacity in region %s", region)
	}

	remaining := request.Count
	recommendation := make(map[string]int)
	for _, zone := range allowedZones {
		if remaining <= 0 {
			break
		}
		maxSize := int(request.LocationSettings[zone].MaxScaleUpSize)
		allocated := min(remaining, maxSize)
		if allocated > 0 {
			recommendation[zone] = allocated
			remaining -= allocated
		}
	}

	if remaining > 0 {
		return nil, fmt.Errorf("insufficient capacity across allowed zones in region %s: requested %d, remaining %d", region, request.Count, remaining)
	}

	recID := c.recommendationID
	specKey := c.specKey
	if specKey == "" {
		specKey = "recommend-locations-nodes"
	}

	c.registerMetrics(recID)

	return &gceclient.RecommendLocationsResponse{
		Recommendation:   recommendation,
		RecommendationID: recID,
		SpecKey:          specKey,
	}, nil
}

func (c *RecommendLocationsClient) registerMetrics(recID string) {
	if c.experimentsManager != nil && experiments.IsDemandFungibilityImpactTrackingEnabled(c.experimentsManager) {
		if recID != "" {
			metrics.Metrics.RegisterDemandFungibilityExtracted(metrics.RLA)
		} else {
			metrics.Metrics.RegisterDemandFungibilityMissingId(metrics.RLA, "MissingRecommendationId")
		}
	}
}
