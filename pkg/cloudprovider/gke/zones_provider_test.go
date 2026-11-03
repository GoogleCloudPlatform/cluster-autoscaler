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
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
)

func TestNewRegionZonesProvider(t *testing.T) {
	testCases := []struct {
		name       string
		location   string
		wantRegion string
		wantErr    bool
	}{
		{
			name:       "regional location",
			location:   "us-central1",
			wantRegion: "us-central1",
		},
		{
			name:       "zonal location",
			location:   "us-central1-a",
			wantRegion: "us-central1",
		},
		{
			name:     "invalid location",
			location: "invalid",
			wantErr:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var fetchedRegions []string
			gceClient := gceclient.BuildAutoscalingInternalGceClientMock().
				WithFetchZones(func(region string) ([]string, error) {
					fetchedRegions = append(fetchedRegions, region)
					return nil, nil
				})

			provider, err := NewRegionZonesProvider(NewGkeCache(&MockGceCache{}, nodetemplate.NewCache()), gceClient, tc.location)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("NewRegionZonesProvider(%q) returned error %v, want error: %v", tc.location, err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}

			if _, err := provider.GetAllZones(); err != nil {
				t.Fatalf("GetAllZones() returned unexpected error: %v", err)
			}
			if diff := cmp.Diff([]string{tc.wantRegion}, fetchedRegions); diff != "" {
				t.Errorf("GetAllZones() fetched zones of unexpected regions, diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRegionZonesProviderGetAllZones(t *testing.T) {
	const region = "us-central1"
	standardZones := []string{"us-central1-a", "us-central1-b"}
	allZones := []string{"us-central1-a", "us-central1-b", "us-central1-ai1a"}
	errFetch := errors.New("transient GCE outage")

	// call is a single call getting the zones, together with the state it is made in.
	type call struct {
		// zonesInGCE and fetchErr are returned by GCE if the call fetches the zones.
		zonesInGCE []string
		fetchErr   error
		// expireCache makes the cached zones expire before the call.
		expireCache bool
		// viaManager makes the call through GkeManager sharing the cache with the provider,
		// instead of through the provider.
		viaManager bool

		wantZones []string
		wantErr   error
	}
	testCases := []struct {
		name        string
		calls       []call
		wantFetches int
	}{
		{
			name: "fetched zones are cached",
			calls: []call{
				{zonesInGCE: standardZones, wantZones: standardZones},
				{zonesInGCE: allZones, wantZones: standardZones},
			},
			wantFetches: 1,
		},
		{
			name: "zones are fetched again once the cached zones expire",
			calls: []call{
				{zonesInGCE: standardZones, wantZones: standardZones},
				{zonesInGCE: allZones, expireCache: true, wantZones: allZones},
			},
			wantFetches: 2,
		},
		{
			name: "zones fetched by GkeManager are reused",
			calls: []call{
				{zonesInGCE: allZones, viaManager: true, wantZones: allZones},
				{zonesInGCE: standardZones, wantZones: allZones},
			},
			wantFetches: 1,
		},
		{
			name: "fetched zones are reused by GkeManager",
			calls: []call{
				{zonesInGCE: allZones, wantZones: allZones},
				{zonesInGCE: standardZones, viaManager: true, wantZones: allZones},
			},
			wantFetches: 1,
		},
		{
			name: "fetch error is returned and not cached",
			calls: []call{
				{fetchErr: errFetch, wantErr: errFetch},
				{zonesInGCE: standardZones, wantZones: standardZones},
			},
			wantFetches: 2,
		},
		{
			name: "cached zones are returned when GCE is failing",
			calls: []call{
				{zonesInGCE: standardZones, wantZones: standardZones},
				{fetchErr: errFetch, wantZones: standardZones},
			},
			wantFetches: 1,
		},
		{
			name: "fetch error is returned once the cached zones expire",
			calls: []call{
				{zonesInGCE: standardZones, wantZones: standardZones},
				{fetchErr: errFetch, expireCache: true, wantErr: errFetch},
			},
			wantFetches: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var current call
			fetches := 0
			gceClient := gceclient.BuildAutoscalingInternalGceClientMock().
				WithFetchZones(func(string) ([]string, error) {
					fetches++
					return current.zonesInGCE, current.fetchErr
				})
			cache := NewGkeCache(&MockGceCache{}, nodetemplate.NewCache())
			manager := &gkeManagerImpl{cache: cache, gceService: gceClient}
			provider, err := NewRegionZonesProvider(cache, gceClient, region)
			if err != nil {
				t.Fatalf("NewRegionZonesProvider(%q) returned unexpected error: %v", region, err)
			}

			for i, c := range tc.calls {
				current = c
				if c.expireCache {
					cache.zonesInRegionCache.Set(region, []string(nil), expiredCacheValidity)
				}
				getZones := provider.GetAllZones
				if c.viaManager {
					getZones = func() ([]string, error) { return manager.GetZonesInRegion(region) }
				}

				got, err := getZones()

				if !errors.Is(err, c.wantErr) {
					t.Errorf("call %d returned error %v, want %v", i, err, c.wantErr)
				}
				if diff := cmp.Diff(c.wantZones, got); diff != "" {
					t.Errorf("call %d returned unexpected zones, diff (-want +got):\n%s", i, diff)
				}
			}
			if fetches != tc.wantFetches {
				t.Errorf("zones were fetched from GCE %d times, want %d", fetches, tc.wantFetches)
			}
		})
	}
}
