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
	"context"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	gkeutil "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util"
)

// RegionZonesProvider provides the zones of the cluster's region.
//
// It reads the zones through GkeCache, so it shares them with GkeManager and fetches them from
// GCE only once the cached zones expire. It needs only the cache and the GCE client, so that it
// can be created before the cloud provider, e.g. for gceclient.ReservationsPuller that the cloud
// provider depends on.
type RegionZonesProvider struct {
	cache     *GkeCache
	gceClient gceclient.AutoscalingInternalGceClient
	region    string
}

// NewRegionZonesProvider builds a RegionZonesProvider for the region of the given location,
// which can be either a region or a zone.
func NewRegionZonesProvider(cache *GkeCache, gceClient gceclient.AutoscalingInternalGceClient, location string) (*RegionZonesProvider, error) {
	region, err := gkeutil.GetRegionFromLocation(location)
	if err != nil {
		return nil, err
	}
	return &RegionZonesProvider{
		cache:     cache,
		gceClient: gceClient,
		region:    region,
	}, nil
}

// GetAllZones returns all zones within the cluster's region.
func (p *RegionZonesProvider) GetAllZones() ([]string, error) {
	return zonesInRegion(p.cache, p.gceClient, p.region)
}

// zonesInRegion returns all zones within a given region. The zones are fetched from GCE if they
// are not in the cache or the cached zones expired.
func zonesInRegion(cache *GkeCache, gceClient gceclient.AutoscalingInternalGceClient, region string) ([]string, error) {
	zones, found := cache.GetZonesInRegion(region)
	if found {
		return zones, nil
	}

	zones, err := gceClient.FetchZones(context.TODO(), region)
	if err != nil {
		return nil, err
	}

	cache.SetZonesInRegion(region, zones)
	return zones, nil
}
