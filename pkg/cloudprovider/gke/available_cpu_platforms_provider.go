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

package gke

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
)

const (
	baseCacheRefreshInterval = 24 * time.Hour
)

// AvailableCpuPlatformsProvider allows obtaining what cpu platforms are available in which zones
type AvailableCpuPlatformsProvider interface {
	// GetAvailableCpuPlatforms returns what cpu platforms are available in given zone
	GetAvailableCpuPlatforms(zone string) ([]string, error)
}

// CachingAvailableCpuPlatformsProvider is caching implementation of AvailableCpuPlatformsProvider
type CachingAvailableCpuPlatformsProvider struct {
	mutex                        sync.Mutex
	cache                        *GkeCache
	lastRefresh                  time.Time
	jitteredCacheRefreshInterval time.Duration
	gceClient                    gce.AutoscalingGceClient
}

// NewCachingAvailableCpuPlatformsProvider creates an instance of caching AvailableCpuPlatformsProvider
func NewCachingAvailableCpuPlatformsProvider(cache *GkeCache, gceClient gce.AutoscalingGceClient) *CachingAvailableCpuPlatformsProvider {
	return &CachingAvailableCpuPlatformsProvider{
		cache:                        cache,
		gceClient:                    gceClient,
		jitteredCacheRefreshInterval: time.Duration(0.9*float64(baseCacheRefreshInterval) + rand.Float64()*0.2*float64(baseCacheRefreshInterval)),
	}
}

// GetAvailableCpuPlatforms returns list of cpu platforms available for given zone
func (p *CachingAvailableCpuPlatformsProvider) GetAvailableCpuPlatforms(zone string) ([]string, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.lastRefresh.Add(p.jitteredCacheRefreshInterval).Before(time.Now()) {
		p.cache.InvalidateAvailableCpuPlatforms()
	}

	availableCpuPlatformsMap, mapFound := p.cache.GetAvailableCpuPlatforms()
	if !mapFound {
		var err error
		availableCpuPlatformsMap, err = p.gceClient.FetchAvailableCpuPlatforms()
		if err != nil {
			return nil, err
		}
		p.cache.SetAvailableCpuPlatforms(availableCpuPlatformsMap)
		p.lastRefresh = time.Now()
	}

	availableCpuPlatforms := availableCpuPlatformsMap[zone]
	if len(availableCpuPlatforms) == 0 {
		return nil, fmt.Errorf("no available CPU platforms found for zone %v", zone)
	}
	return availableCpuPlatforms, nil
}

// StaticAvailableCpuPlatformsProvider is implementation of AvailableCpuPlatformsProvider based on static map
type StaticAvailableCpuPlatformsProvider struct {
	availableCpuPlatforms map[string][]string
}

// NewStaticAvailableCpuPlatformsProvider creates an instance of AvailableCpuPlatformsProvider based on static map
func NewStaticAvailableCpuPlatformsProvider(availableCpuPlatforms map[string][]string) *StaticAvailableCpuPlatformsProvider {
	return &StaticAvailableCpuPlatformsProvider{
		availableCpuPlatforms: availableCpuPlatforms,
	}
}

// GetAvailableCpuPlatforms returns list of cpu platforms available for given zone
func (p *StaticAvailableCpuPlatformsProvider) GetAvailableCpuPlatforms(zone string) ([]string, error) {
	availableCpuPlatforms := p.availableCpuPlatforms[zone]
	if len(availableCpuPlatforms) == 0 {
		return nil, fmt.Errorf("no available CPU platforms found for zone %v", zone)
	}
	return availableCpuPlatforms, nil
}
