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
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce"
	gkeutil "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

const (
	defaultMachinesRefreshIntervalMinutes  = 4 * 60
	maxMachinesCacheRefreshBackoff         = 30 * time.Minute
	maxMachinesCacheAsyncRefreshFailures   = 3
	defaultMachinesCacheRefreshHttpTimeout = 3 * time.Minute
)

type machinesCacheRefresher struct {
	gkeManager            *gkeManagerImpl
	refreshFailuresCount  int
	nextRefreshAt         time.Time
	mutex                 sync.Mutex
	asyncRefreshMutex     sync.Mutex
	missingZoneLogLimiter *rate.Limiter
	fetchedZones          atomic.Pointer[sets.Set[string]]
}

func newMachinesCacheRefresher(gkeManager *gkeManagerImpl) *machinesCacheRefresher {
	return &machinesCacheRefresher{
		gkeManager:            gkeManager,
		missingZoneLogLimiter: rate.NewLimiter(rate.Every(time.Minute), 5),
	}
}

func (r *machinesCacheRefresher) refreshIfStale() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	hasUnfetched, err := r.hasUnfetchedZones()
	if err != nil {
		// unfetched zones calculations at this step should not stop whole refresh, just log an error
		klog.Warningf("Failed to get zones when checking zones for machines cache: %v", err)
	}

	// nextRefreshAt can be overridden if we have unfetched zones and failures count is 0
	// meaning, backoffs installed due to failures in previous runs are effective, even if we have zones unfetched
	if r.nextRefreshAt.After(time.Now()) && (!hasUnfetched || r.refreshFailuresCount > 0) {
		return nil
	}

	// Run blocking refresh on initial poll or if async refresh failed too many times(default: 3)
	// Note: backoffs are not applied on synchronous path on purpose - without this data or with stale one
	// CA cannot work properly and backoff would cause CA to be blocked longer amount of time. We allow some async failures,
	// but at some points refresh is blocking
	if r.nextRefreshAt.IsZero() || r.refreshFailuresCount >= maxMachinesCacheAsyncRefreshFailures {
		if err := r.fetchMachines(true); err != nil {
			r.refreshFailuresCount++
			return err
		}
		return nil
	}

	// Async refresh on next ones
	httpTimeout := r.refreshHttpTimeout()
	unlock, acquired := r.tryLockAsyncRefreshWithTimeout(20 * httpTimeout)
	if !acquired {
		return nil
	}
	go func() {
		defer unlock()

		if err := r.fetchMachines(false); err != nil {
			r.mutex.Lock()
			defer r.mutex.Unlock()
			r.refreshFailuresCount++
			var backoff time.Duration
			// Apply backoff only if we still allow async retries
			if r.refreshFailuresCount < maxMachinesCacheAsyncRefreshFailures {
				backoff = r.getRefreshBackoff(httpTimeout)
			}
			r.nextRefreshAt = time.Now().Add(backoff)
		}
	}()
	return nil
}

// tryLockAsyncRefreshWithTimeout attempts to acquire asyncRefreshMutex without blocking. If acquired, the lock is
// held until the returned unlock function is called or timeout elapses, whichever comes first.
func (r *machinesCacheRefresher) tryLockAsyncRefreshWithTimeout(timeout time.Duration) (func(), bool) {
	if !r.asyncRefreshMutex.TryLock() {
		return nil, false
	}
	var once sync.Once
	timer := time.AfterFunc(timeout, func() {
		once.Do(r.asyncRefreshMutex.Unlock)
	})
	return func() {
		timer.Stop()
		once.Do(r.asyncRefreshMutex.Unlock)
	}, true
}

func (r *machinesCacheRefresher) refreshInterval() time.Duration {
	experimentsManager := r.gkeManager.ExperimentsManager()
	if experimentsManager == nil {
		return defaultMachinesRefreshIntervalMinutes * time.Minute
	}
	minutes := experimentsManager.EvaluateIntFlagOrFailsafe(experiments.AuthoritativeMachinesCacheRefreshIntervalMinutesFlag, defaultMachinesRefreshIntervalMinutes)
	if minutes <= 0 {
		return defaultMachinesRefreshIntervalMinutes * time.Minute
	}
	return time.Duration(minutes) * time.Minute
}

// refreshHttpTimeout returns the GCE HTTP client timeout used to size the machines cache refresh
// windows. Zero timeout means no timeout in http.Client, so defaultMachinesCacheRefreshHttpTimeout is used instead.
func (r *machinesCacheRefresher) refreshHttpTimeout() time.Duration {
	if httpTimeout := r.gkeManager.gceService.GetHttpTimeout(); httpTimeout > 0 {
		return httpTimeout
	}
	return defaultMachinesCacheRefreshHttpTimeout
}

// getRefreshBackoff returns the backoff before retrying an asynchronous machines cache refresh after
// r.refreshFailuresCount consecutive failures, capped at maxMachinesCacheRefreshBackoff.
// With the default 3m GCE HTTP timeout the backoffs are 12m, 24m, 30m.
func (r *machinesCacheRefresher) getRefreshBackoff(httpTimeout time.Duration) time.Duration {
	shift := min(max(r.refreshFailuresCount-1, 0), 8) // keep the shift safe
	return min(4*httpTimeout*time.Duration(1<<shift), maxMachinesCacheRefreshBackoff)
}

func (r *machinesCacheRefresher) getZonesToFetch() ([]string, error) {
	if r.gkeManager.managerOptions.enableUserAnyZoneSelection {
		clusterRegion, err := gkeutil.GetRegionFromLocation(r.gkeManager.location)
		if err != nil {
			return nil, err
		}
		return r.gkeManager.GetZonesInRegion(clusterRegion)
	}
	zones := sets.New(r.gkeManager.GetAutoprovisioningLocations()...)
	// GetGkeMigsLocations's cache is refreshed on:
	// - new pool created by CA
	// - 1 minute interval
	// In both cases our cache will refresh asynchronously. Because of it being >asynchronous< refresh,
	// it's possible on following loop our cache will not represent accurately all zones we can actually provision at.
	// This is not critical and should auto correct on next loop.
	// Note: this should only happen in cases where MIGs were added >manually and outside of configured cluster's AP locations<. Other thing to consider is,
	// what happens if customer will be conducting it frequently on edge zone where set of provisionable zones will change on each loop - in this case refresh
	// will be throttled but ultimately soon as previously triggered refresh finishes we should auto correct
	if r.gkeManager.migLister != nil {
		zones.Insert(r.gkeManager.migLister.GetGkeMigsLocations()...)
	}
	return sets.List(zones), nil
}

// hasUnfetchedZones reports whether getZonesToFetch currently returns any zone that is not yet in
// fetchedZones. Zone removals do not trigger an immediate refresh.
func (r *machinesCacheRefresher) hasUnfetchedZones() (bool, error) {
	currentZones, err := r.getZonesToFetch()
	if err != nil {
		return false, err
	}
	var fetchedZones sets.Set[string]
	if loaded := r.fetchedZones.Load(); loaded != nil {
		fetchedZones = *loaded
	}
	return !fetchedZones.HasAll(currentZones...), nil
}

func (r *machinesCacheRefresher) fetchMachines(lockAcquired bool) error {
	allZones, err := r.getZonesToFetch()
	if err != nil {
		return err
	}
	machinesCache := make(map[gce.MachineTypeKey]gce.MachineType)
	for _, location := range allZones {
		machineTypes, err := r.gkeManager.gceService.FetchMachineTypes(location)
		if err != nil {
			return err
		}
		for _, machineType := range machineTypes {
			// At the time of writing, NewMachineTypeFromAPI returns error only if machineType==nil. With current implementation
			// FetchMachineTypes will never include nil machineType in the results so below err != nil check
			// will never enter. We must keep it though to conform to the API
			gceMt, err := gce.NewMachineTypeFromAPI(machineType.Name, machineType)
			if err != nil {
				klog.Errorf("Failed to parse machine type information for zone=%v, machineType=%v", location, machineType.Name)
				return err
			}
			machinesCache[gce.MachineTypeKey{Zone: location, MachineTypeName: machineType.Name}] = gceMt
		}

	}

	if !lockAcquired {
		r.mutex.Lock()
		defer r.mutex.Unlock()
	}
	r.gkeManager.cache.SetMachines(machinesCache)
	fetchedZones := sets.New(allZones...)
	r.fetchedZones.Store(&fetchedZones)
	r.nextRefreshAt = time.Now().Add(r.refreshInterval())
	r.refreshFailuresCount = 0

	klog.V(2).Infof("Refreshed machine types for zones=%v, nextRefreshAt=%v", allZones, r.nextRefreshAt)

	return nil
}
