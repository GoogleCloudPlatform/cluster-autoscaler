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

// AvailableDiskTypesProvider allows obtaining what disk types are available in which zones
type AvailableDiskTypesProvider interface {
	// GetAvailableDiskTypes returns what disk types are available in given zone
	GetAvailableDiskTypes(zone string) ([]string, error)
}

// CachingAvailableDiskTypesProvider is caching implementation of AvailableDiskTypesProvider
type CachingAvailableDiskTypesProvider struct {
	mutex                        sync.Mutex
	cache                        *GkeCache
	lastRefresh                  map[string]time.Time
	jitteredCacheRefreshInterval time.Duration
	gceClient                    gce.AutoscalingGceClient
}

// NewCachingAvailableDiskTypesProvider creates an instance of caching AvailableDiskTypesProvider
func NewCachingAvailableDiskTypesProvider(cache *GkeCache, gceClient gce.AutoscalingGceClient) *CachingAvailableDiskTypesProvider {
	return &CachingAvailableDiskTypesProvider{
		cache:                        cache,
		lastRefresh:                  make(map[string]time.Time),
		gceClient:                    gceClient,
		jitteredCacheRefreshInterval: time.Duration(0.9*float64(baseCacheRefreshInterval) + rand.Float64()*0.2*float64(baseCacheRefreshInterval)),
	}
}

// GetAvailableDiskTypes returns list of disk types available for given zone
func (p *CachingAvailableDiskTypesProvider) GetAvailableDiskTypes(zone string) ([]string, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.lastRefresh[zone].Add(p.jitteredCacheRefreshInterval).Before(time.Now()) {
		p.cache.InvalidateAvailableDiskTypes()
	}

	availableDiskTypes, found := p.cache.GetAvailableDiskTypes(zone)
	if !found {
		var err error
		availableDiskTypes, err = p.gceClient.FetchAvailableDiskTypes(zone)
		if err != nil {
			return nil, err
		}
		p.cache.SetAvailableDiskTypes(zone, availableDiskTypes)
		p.lastRefresh[zone] = time.Now()
	}

	if len(availableDiskTypes) == 0 {
		return nil, fmt.Errorf("no available disk types found for zone %v", zone)
	}

	return availableDiskTypes, nil
}

// StaticAvailableDiskTypesProvider is implementation of AvailableDiskTypesProvider based on static map
type StaticAvailableDiskTypesProvider struct {
	availableDiskTypes map[string][]string
}

// NewStaticAvailableDiskTypesProvider creates an instance of AvailableDiskTypesProvider based on static map
func NewStaticAvailableDiskTypesProvider(availableDiskTypes map[string][]string) *StaticAvailableDiskTypesProvider {
	return &StaticAvailableDiskTypesProvider{
		availableDiskTypes: availableDiskTypes,
	}
}

// GetAvailableDiskTypes returns list of cpu platforms available for given zone
func (p *StaticAvailableDiskTypesProvider) GetAvailableDiskTypes(zone string) ([]string, error) {
	availableDiskTypes := p.availableDiskTypes[zone]
	if len(availableDiskTypes) == 0 {
		return nil, fmt.Errorf("no available disk types found for zone %v", zone)
	}
	return availableDiskTypes, nil
}
