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
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// machinesCacheRefreshState returns the machines cache refresh state under lock.
func machinesCacheRefreshState(g *gkeManagerImpl) (nextRefresh time.Time, failuresCount int) {
	g.machinesCacheRefresher.mutex.Lock()
	defer g.machinesCacheRefresher.mutex.Unlock()
	return g.machinesCacheRefresher.nextRefreshAt, g.machinesCacheRefresher.refreshFailuresCount
}

func TestRefreshMachinesCache(t *testing.T) {
	testCases := []struct {
		name                        string
		disableUserAnyZoneSelection bool
		autoprovisioningLocations   []string
		migZones                    []string
		nextRefreshOffset           time.Duration // Zero means the cache was never refreshed.
		initialFetchedZones         []string
		previousFailures            int
		refreshIntervalExperiment   string
		invalidLocation             bool
		wantFetch                   bool
		fetchFails                  bool
		wantAsync                   bool
		wantErr                     bool
		wantRefreshed               bool
		wantFailures                int
		// wantNextRefreshIn is the expected time until the next refresh is allowed.
		wantNextRefreshIn        time.Duration
		wantNextRefreshUnchanged bool
	}{
		{
			name:              "never refreshed - refreshes synchronously",
			wantFetch:         true,
			wantRefreshed:     true,
			wantNextRefreshIn: defaultMachinesRefreshIntervalMinutes * time.Minute,
		},
		{
			name:                        "enableUserAnyZoneSelection disabled - refreshes only autoprovisioning and MIG locations",
			disableUserAnyZoneSelection: true,
			autoprovisioningLocations:   []string{zoneB},
			migZones:                    []string{zoneC},
			wantFetch:                   true,
			wantRefreshed:               true,
			wantNextRefreshIn:           defaultMachinesRefreshIntervalMinutes * time.Minute,
		},
		{
			name:                      "never refreshed with custom refresh interval experiment - uses configured interval",
			refreshIntervalExperiment: "30",
			wantFetch:                 true,
			wantRefreshed:             true,
			wantNextRefreshIn:         30 * time.Minute,
		},
		{
			name:                     "never refreshed, fetch fails - returns error",
			wantFetch:                true,
			fetchFails:               true,
			wantErr:                  true,
			wantFailures:             1,
			wantNextRefreshUnchanged: true,
		},
		{
			name:                     "refreshed recently, zones unchanged - skips refresh",
			nextRefreshOffset:        time.Hour,
			initialFetchedZones:      []string{zoneB, zoneC},
			wantNextRefreshUnchanged: true,
		},
		{
			name:                     "refreshed recently, zone check fails - skips refresh without returning error",
			nextRefreshOffset:        time.Hour,
			initialFetchedZones:      []string{zoneB, zoneC},
			invalidLocation:          true,
			wantNextRefreshUnchanged: true,
		},
		{
			name:              "fetchedZones nil before nextRefreshAt - refreshes asynchronously",
			nextRefreshOffset: time.Hour,
			wantFetch:         true,
			wantAsync:         true,
			wantRefreshed:     true,
			wantNextRefreshIn: defaultMachinesRefreshIntervalMinutes * time.Minute,
		},
		{
			name:                "refreshed recently, zone added - refreshes asynchronously",
			nextRefreshOffset:   time.Hour,
			initialFetchedZones: []string{zoneB},
			wantFetch:           true,
			wantAsync:           true,
			wantRefreshed:       true,
			wantNextRefreshIn:   defaultMachinesRefreshIntervalMinutes * time.Minute,
		},
		{
			name:                     "refreshed recently, zone removed - skips refresh",
			nextRefreshOffset:        time.Hour,
			initialFetchedZones:      []string{zoneA, zoneB, zoneC},
			wantNextRefreshUnchanged: true,
		},
		{
			name:                     "in error backoff, zone added - respects backoff and skips refresh",
			nextRefreshOffset:        10 * time.Minute,
			initialFetchedZones:      []string{zoneB},
			previousFailures:         1,
			wantFailures:             1,
			wantNextRefreshUnchanged: true,
		},
		{
			name:              "stale - refreshes asynchronously",
			nextRefreshOffset: -time.Hour,
			wantFetch:         true,
			wantAsync:         true,
			wantRefreshed:     true,
			wantNextRefreshIn: defaultMachinesRefreshIntervalMinutes * time.Minute,
		},
		{
			name:              "stale after previous failures - refreshes asynchronously and resets failures",
			nextRefreshOffset: -time.Hour,
			previousFailures:  2,
			wantFetch:         true,
			wantAsync:         true,
			wantRefreshed:     true,
			wantNextRefreshIn: defaultMachinesRefreshIntervalMinutes * time.Minute,
		},
		{
			name:              "stale, fetch fails - keeps serving previous machine types and retries after backoff",
			nextRefreshOffset: -time.Hour,
			wantFetch:         true,
			fetchFails:        true,
			wantAsync:         true,
			wantFailures:      1,
			wantNextRefreshIn: 4 * defaultMachinesCacheRefreshHttpTimeout,
		},
		{
			name:              "stale after 1 previous failure, fetch fails - retries after doubled backoff",
			nextRefreshOffset: -time.Hour,
			previousFailures:  1,
			wantFetch:         true,
			fetchFails:        true,
			wantAsync:         true,
			wantFailures:      2,
			wantNextRefreshIn: 8 * defaultMachinesCacheRefreshHttpTimeout,
		},
		{
			name:              "stale after 2 previous failures, fetch fails - reaches max async failures without backoff",
			nextRefreshOffset: -time.Hour,
			previousFailures:  2,
			wantFetch:         true,
			fetchFails:        true,
			wantAsync:         true,
			wantFailures:      3,
			wantNextRefreshIn: 0,
		},
		{
			name:              "too many previous failures - refreshes synchronously",
			nextRefreshOffset: -time.Hour,
			previousFailures:  3,
			wantFetch:         true,
			wantRefreshed:     true,
			wantNextRefreshIn: defaultMachinesRefreshIntervalMinutes * time.Minute,
		},
		{
			name:                     "too many previous failures, fetch fails - returns error",
			nextRefreshOffset:        -time.Hour,
			previousFailures:         3,
			wantFetch:                true,
			fetchFails:               true,
			wantErr:                  true,
			wantFailures:             4,
			wantNextRefreshUnchanged: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := test.NewHttpServerMock(test.MockFieldStatusCode, test.MockFieldResponse)
			defer server.Close()

			g := newTestGkeManager(t, server.URL, napEnabled, false, false, nil, false, nil)
			g.cache.SetZonesInRegion(region, []string{zoneB, zoneC})
			if tc.invalidLocation {
				g.location = "invalid"
			}
			if tc.disableUserAnyZoneSelection {
				g.managerOptions.enableUserAnyZoneSelection = false
				// Set an extra zoneA in zonesInRegion to verify it is not fetched when enableUserAnyZoneSelection is disabled.
				g.cache.SetZonesInRegion(region, []string{zoneA, zoneB, zoneC})
			}
			if tc.autoprovisioningLocations != nil {
				g.gkeConfigurationCache.setAutoprovisioningLocations(tc.autoprovisioningLocations)
			}
			if len(tc.migZones) > 0 {
				for i, z := range tc.migZones {
					g.cache.RegisterMig(&GkeMig{
						gceRef: gce.GceRef{Project: projectId, Zone: z, Name: fmt.Sprintf("mig-%d", i)},
					})
				}
			}
			if tc.refreshIntervalExperiment != "" {
				g.optsTracker = optstracking.TestOptionsTracker(internalopts.AutoscalingOptions{}, gkeclient.Cluster{}, experiments.NewMockManagerWithOptions(version.Version{}, nil, map[string]string{
					experiments.AuthoritativeMachinesCacheRefreshIntervalMinutesFlag: tc.refreshIntervalExperiment,
				}))
			}
			var initialNextRefresh time.Time
			if tc.nextRefreshOffset != 0 {
				initialNextRefresh = time.Now().Add(tc.nextRefreshOffset)
			}
			g.machinesCacheRefresher.nextRefreshAt = initialNextRefresh
			if tc.initialFetchedZones != nil {
				initialZones := sets.New(tc.initialFetchedZones...)
				g.machinesCacheRefresher.fetchedZones.Store(&initialZones)
			}
			g.machinesCacheRefresher.refreshFailuresCount = tc.previousFailures

			// Blocks the last zone fetch, so that the state during an asynchronous refresh can be verified.
			fetchInProgress := make(chan struct{})
			releaseFetch := make(chan time.Time)
			if tc.wantFetch {
				zoneCStatus, zoneCResponse := http.StatusOK, listMachineTypesResponse
				if tc.fetchFails {
					zoneCStatus, zoneCResponse = http.StatusInternalServerError, "Internal Server Error"
				}
				server.On("handle", "/projects/project1/zones/us-central1-b/machineTypes").Return(http.StatusOK, listMachineTypesResponse).Run(func(mock.Arguments) {
					close(fetchInProgress)
				}).Once()
				server.On("handle", "/projects/project1/zones/us-central1-c/machineTypes").Return(zoneCStatus, zoneCResponse).WaitUntil(releaseFetch).Once()
			}
			if !tc.wantAsync {
				close(releaseFetch)
			}

			err := g.refreshMachinesCacheIfStale()

			if tc.wantAsync {
				<-fetchInProgress
				// Further refreshes are skipped while the async refresh lock is held.
				_, acquired := g.machinesCacheRefresher.tryLockAsyncRefreshWithTimeout(time.Second)
				assert.False(t, acquired)
				assert.NoError(t, g.refreshMachinesCacheIfStale())
				// Previously fetched machine types are served while the refresh is in progress.
				_, found := g.cache.GetMachine(machineTypeA, zoneB)
				assert.True(t, found)
				_, found = g.cache.GetMachine("f1-micro", zoneC)
				assert.False(t, found)

				close(releaseFetch)
				assert.Eventually(t, func() bool {
					nextRefresh, _ := machinesCacheRefreshState(g)
					return !nextRefresh.Equal(initialNextRefresh)
				}, 5*time.Second, 10*time.Millisecond)
			}

			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			_, oldMachineFound := g.cache.GetMachine(machineTypeA, zoneB)
			_, newMachineFound := g.cache.GetMachine("f1-micro", zoneC)
			assert.Equal(t, !tc.wantRefreshed, oldMachineFound)
			assert.Equal(t, tc.wantRefreshed, newMachineFound)
			nextRefresh, failuresCount := machinesCacheRefreshState(g)
			assert.Equal(t, tc.wantFailures, failuresCount)
			if tc.wantRefreshed {
				fetchedZones := g.machinesCacheRefresher.fetchedZones.Load()
				if assert.NotNil(t, fetchedZones) {
					assert.Equal(t, sets.New(zoneB, zoneC), *fetchedZones)
				}
			} else if tc.initialFetchedZones != nil {
				fetchedZones := g.machinesCacheRefresher.fetchedZones.Load()
				if assert.NotNil(t, fetchedZones) {
					assert.Equal(t, sets.New(tc.initialFetchedZones...), *fetchedZones)
				}
			} else {
				assert.Nil(t, g.machinesCacheRefresher.fetchedZones.Load())
			}
			if tc.wantNextRefreshUnchanged {
				assert.Equal(t, initialNextRefresh, nextRefresh)
			} else {
				assert.WithinDuration(t, time.Now().Add(tc.wantNextRefreshIn), nextRefresh, 10*time.Second)
			}
			mock.AssertExpectationsForObjects(t, server)
			if !tc.wantFetch {
				server.AssertNotCalled(t, "handle", mock.Anything)
			}
		})
	}
}

func TestTryLockAsyncRefresh(t *testing.T) {
	testCases := []struct {
		name               string
		firstTimeout       time.Duration
		waitBeforeSecond   time.Duration
		unlockFirstBefore  bool
		wantSecondAcquired bool
	}{
		{
			name:               "first lock unlocked before second attempt - second acquires lock",
			firstTimeout:       time.Minute,
			unlockFirstBefore:  true,
			wantSecondAcquired: true,
		},
		{
			name:               "first lock still held - second attempt fails immediately",
			firstTimeout:       time.Minute,
			wantSecondAcquired: false,
		},
		{
			name:               "first lock timed out - second attempt acquires lock and late unlock is safe",
			firstTimeout:       20 * time.Millisecond,
			waitBeforeSecond:   50 * time.Millisecond,
			wantSecondAcquired: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := newMachinesCacheRefresher(&gkeManagerImpl{})
			unlock1, acquired1 := r.tryLockAsyncRefreshWithTimeout(tc.firstTimeout)
			assert.True(t, acquired1)
			defer unlock1()

			if tc.unlockFirstBefore {
				unlock1()
			}
			if tc.waitBeforeSecond > 0 {
				time.Sleep(tc.waitBeforeSecond)
			}

			unlock2, acquired2 := r.tryLockAsyncRefreshWithTimeout(time.Minute)
			assert.Equal(t, tc.wantSecondAcquired, acquired2)
			if acquired2 {
				// Calling unlock1 after timeout/re-acquisition must not unlock the second holder's lock.
				unlock1()
				_, acquired3 := r.tryLockAsyncRefreshWithTimeout(time.Minute)
				assert.False(t, acquired3)
				unlock2()
			}
		})
	}
}

func TestMachinesCacheRefreshBackoff(t *testing.T) {
	testCases := []struct {
		name          string
		httpTimeout   time.Duration
		failuresCount int
		want          time.Duration
	}{
		{
			name:          "default 3m timeout, 1 failure - 4x timeout",
			httpTimeout:   3 * time.Minute,
			failuresCount: 1,
			want:          12 * time.Minute,
		},
		{
			name:          "default 3m timeout, 2 failures - doubled",
			httpTimeout:   3 * time.Minute,
			failuresCount: 2,
			want:          24 * time.Minute,
		},
		{
			name:          "default 3m timeout, 3 failures - capped at max backoff",
			httpTimeout:   3 * time.Minute,
			failuresCount: 3,
			want:          maxMachinesCacheRefreshBackoff,
		},
		{
			name:          "30s timeout, 1 failure - 4x timeout",
			httpTimeout:   30 * time.Second,
			failuresCount: 1,
			want:          2 * time.Minute,
		},
		{
			name:          "30s timeout, 4 failures - doubled 3 times",
			httpTimeout:   30 * time.Second,
			failuresCount: 4,
			want:          16 * time.Minute,
		},
		{
			name:          "30s timeout, 5 failures - capped at max backoff",
			httpTimeout:   30 * time.Second,
			failuresCount: 5,
			want:          maxMachinesCacheRefreshBackoff,
		},
		{
			name:          "no failures - treated as 1 failure",
			httpTimeout:   30 * time.Second,
			failuresCount: 0,
			want:          2 * time.Minute,
		},
		{
			name:          "many failures - capped at max backoff without overflow",
			httpTimeout:   30 * time.Second,
			failuresCount: 100,
			want:          maxMachinesCacheRefreshBackoff,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := &machinesCacheRefresher{refreshFailuresCount: tc.failuresCount}
			assert.Equal(t, tc.want, r.getRefreshBackoff(tc.httpTimeout))
		})
	}
}

func TestMachinesCacheRefreshHttpTimeout(t *testing.T) {
	testCases := []struct {
		name              string
		customEndpoint    bool
		clientHttpTimeout time.Duration
		want              time.Duration
	}{
		{
			name:              "client timeout set - uses client timeout",
			clientHttpTimeout: 30 * time.Second,
			want:              30 * time.Second,
		},
		{
			name:              "no client timeout - uses default timeout",
			clientHttpTimeout: 0,
			want:              defaultMachinesCacheRefreshHttpTimeout,
		},
		{
			name:              "custom endpoint client, which doesn't expose its timeout - uses default timeout",
			customEndpoint:    true,
			clientHttpTimeout: 30 * time.Second,
			want:              defaultMachinesCacheRefreshHttpTimeout,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			httpClient := &http.Client{Timeout: tc.clientHttpTimeout}
			var gceService gceclient.AutoscalingInternalGceClient
			var err error
			if tc.customEndpoint {
				gceService, err = gceclient.NewCustomAutoscalingInternalGceClient(httpClient, &migInfoProviderStub{}, projectId, "", "http://localhost", "cluster-autoscaler", time.Second, time.Millisecond, experiments.NewMockManager())
			} else {
				gceService, err = gceclient.NewAutoscalingInternalGceClient(httpClient, &migInfoProviderStub{}, projectId, "", "cluster-autoscaler", time.Second, time.Millisecond, experiments.NewMockManager())
			}
			assert.NoError(t, err)
			r := newMachinesCacheRefresher(&gkeManagerImpl{gceService: gceService})
			assert.Equal(t, tc.want, r.refreshHttpTimeout())
		})
	}
}

func TestMachinesCacheRefreshInterval(t *testing.T) {
	testCases := []struct {
		name               string
		experimentsManager experiments.Manager
		want               time.Duration
	}{
		{
			name:               "nil experiments manager - uses default interval",
			experimentsManager: nil,
			want:               defaultMachinesRefreshIntervalMinutes * time.Minute,
		},
		{
			name:               "experiment not set - uses default interval",
			experimentsManager: experiments.NewMockManager(),
			want:               defaultMachinesRefreshIntervalMinutes * time.Minute,
		},
		{
			name: "custom positive interval - uses configured interval",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, nil, map[string]string{
				experiments.AuthoritativeMachinesCacheRefreshIntervalMinutesFlag: "90",
			}),
			want: 90 * time.Minute,
		},
		{
			name: "zero interval - falls back to default interval",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, nil, map[string]string{
				experiments.AuthoritativeMachinesCacheRefreshIntervalMinutesFlag: "0",
			}),
			want: defaultMachinesRefreshIntervalMinutes * time.Minute,
		},
		{
			name: "negative interval - falls back to default interval",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, nil, map[string]string{
				experiments.AuthoritativeMachinesCacheRefreshIntervalMinutesFlag: "-15",
			}),
			want: defaultMachinesRefreshIntervalMinutes * time.Minute,
		},
		{
			name: "invalid non-integer interval - falls back to default interval",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, nil, map[string]string{
				experiments.AuthoritativeMachinesCacheRefreshIntervalMinutesFlag: "invalid",
			}),
			want: defaultMachinesRefreshIntervalMinutes * time.Minute,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var optsTracker *optstracking.OptionsTracker
			if tc.experimentsManager != nil {
				optsTracker = optstracking.TestOptionsTracker(internalopts.AutoscalingOptions{}, gkeclient.Cluster{}, tc.experimentsManager)
			}
			r := newMachinesCacheRefresher(&gkeManagerImpl{optsTracker: optsTracker})
			assert.Equal(t, tc.want, r.refreshInterval())
		})
	}
}

func TestMachinesCacheHasUnfetchedZones(t *testing.T) {
	testCases := []struct {
		name                        string
		disableUserAnyZoneSelection bool
		autoprovisioningLocations   []string
		migZones                    []string
		fetchedZones                []string
		cachedZonesInRegion         []string
		location                    string
		wantUnfetchedZones          bool
		wantErr                     bool
	}{
		{
			name:                "zones unchanged - returns false",
			fetchedZones:        []string{zoneA, zoneB},
			cachedZonesInRegion: []string{zoneA, zoneB},
			location:            region,
			wantUnfetchedZones:  false,
		},
		{
			name:                "new zone added in region - returns true",
			fetchedZones:        []string{zoneA, zoneB},
			cachedZonesInRegion: []string{zoneA, zoneB, zoneAIA},
			location:            region,
			wantUnfetchedZones:  true,
		},
		{
			name:                "zone removed in region - returns false",
			fetchedZones:        []string{zoneA, zoneB, zoneC},
			cachedZonesInRegion: []string{zoneA, zoneB},
			location:            region,
			wantUnfetchedZones:  false,
		},
		{
			name:                "zone added and another zone removed in region - returns true",
			fetchedZones:        []string{zoneA, zoneB},
			cachedZonesInRegion: []string{zoneA, zoneAIA},
			location:            region,
			wantUnfetchedZones:  true,
		},
		{
			name:                        "enableUserAnyZoneSelection disabled, autoprovisioning and MIG zones unchanged - returns false",
			disableUserAnyZoneSelection: true,
			autoprovisioningLocations:   []string{zoneA},
			migZones:                    []string{zoneB},
			fetchedZones:                []string{zoneA, zoneB},
			cachedZonesInRegion:         []string{zoneA, zoneB, zoneC},
			location:                    region,
			wantUnfetchedZones:          false,
		},
		{
			name:                        "enableUserAnyZoneSelection disabled, new MIG zone added - returns true",
			disableUserAnyZoneSelection: true,
			autoprovisioningLocations:   []string{zoneA},
			migZones:                    []string{zoneB, zoneC},
			fetchedZones:                []string{zoneA, zoneB},
			location:                    region,
			wantUnfetchedZones:          true,
		},
		{
			name:                        "enableUserAnyZoneSelection disabled, MIG zone removed - returns false",
			disableUserAnyZoneSelection: true,
			autoprovisioningLocations:   []string{zoneA},
			migZones:                    []string{zoneB},
			fetchedZones:                []string{zoneA, zoneB, zoneC},
			location:                    region,
			wantUnfetchedZones:          false,
		},
		{
			name:                        "enableUserAnyZoneSelection disabled, new autoprovisioning location added - returns true",
			disableUserAnyZoneSelection: true,
			autoprovisioningLocations:   []string{zoneA, zoneB},
			migZones:                    []string{zoneB},
			fetchedZones:                []string{zoneB},
			location:                    region,
			wantUnfetchedZones:          true,
		},
		{
			name:                        "enableUserAnyZoneSelection disabled, autoprovisioning location removed - returns false",
			disableUserAnyZoneSelection: true,
			autoprovisioningLocations:   []string{zoneA},
			migZones:                    []string{zoneB},
			fetchedZones:                []string{zoneA, zoneB, zoneC},
			location:                    region,
			wantUnfetchedZones:          false,
		},
		{
			name:                "fetchedZones is nil with target zones - returns true",
			fetchedZones:        nil,
			cachedZonesInRegion: []string{zoneA, zoneB},
			location:            region,
			wantUnfetchedZones:  true,
		},
		{
			name:                        "fetchedZones is nil with no target zones - returns false",
			disableUserAnyZoneSelection: true,
			fetchedZones:                nil,
			location:                    region,
			wantUnfetchedZones:          false,
		},
		{
			name:                "invalid location - returns error",
			fetchedZones:        []string{zoneA},
			cachedZonesInRegion: []string{zoneA, zoneB},
			location:            "invalid",
			wantErr:             true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gceCache := gce.NewGceCache()
			cache := NewGkeCache(gceCache, nodetemplate.NewCache())
			migLister := NewGkeMigLister(cache, irretrievableMigRefreshTime, irretrievableMigRefreshTime, 2)
			if tc.cachedZonesInRegion != nil {
				cache.SetZonesInRegion(region, tc.cachedZonesInRegion)
			}
			if len(tc.migZones) > 0 {
				for i, z := range tc.migZones {
					cache.RegisterMig(&GkeMig{
						gceRef: gce.GceRef{Project: projectId, Zone: z, Name: fmt.Sprintf("mig-%d", i)},
					})
				}
			}
			autoprovisioningEligibility := &MockAutoprovisioningEligibility{}
			autoprovisioningEligibility.On("IsNodeAutoprovisioningEnabled").Maybe().Return(true)
			r := newMachinesCacheRefresher(&gkeManagerImpl{
				cache:                       cache,
				migLister:                   migLister,
				autoprovisioningEligibility: autoprovisioningEligibility,
				gkeConfigurationCache:       gkeConfigurationCache{autoprovisioningLocations: tc.autoprovisioningLocations},
				location:                    tc.location,
				managerOptions: GkeManagerOptions{
					enableUserAnyZoneSelection: !tc.disableUserAnyZoneSelection,
				},
			})
			if tc.fetchedZones != nil {
				zones := sets.New(tc.fetchedZones...)
				r.fetchedZones.Store(&zones)
			}

			got, err := r.hasUnfetchedZones()

			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantUnfetchedZones, got)
			}
		})
	}
}
