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

package validators

import (
	"math/rand"
	"net/http"
	"sync"
	"time"

	gceapi "google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/klog/v2"
)

const maxCacheValidity = 24 * time.Hour

// MachineConfigValidator validates machine configuration
// TODO(b/448575257): consider refreshing cache on MachineConfig CRD update.
type MachineConfigValidator interface {
	ValidateMachineTypeConfig(machineType, zone string) error
	ValidateGpuConfig(gpuType, gpuPartitionSize, gpuMaxSharedClients, gpuSharingStrategy, machineType string, gpuCount int64, zone string, cpus, mem int64) error
}

type machineTypeFetcher interface {
	FetchMachineType(zone, machineType string) (*gceapi.MachineType, error)
	FetchMachineTypes(zone string) ([]*gceapi.MachineType, error)
}

type cachedMachineConfigValidator struct {
	// cached machine configs
	// TODO(b/239697645): Use GkeCache instead.
	configCaches map[string]machineConfigCache
	// cache mutex
	cacheMutex sync.Mutex
	// authenticated http client
	gceInternalService gceclient.AutoscalingInternalGceClient
	machineFetcher     machineTypeFetcher
	// Stores user defined custom machine types we checked validity for.
	customMachineTypes map[string]bool
	// client http timeout
	httpTimeout           time.Duration
	machineConfigProvider *machinetypes.MachineConfigProvider
}

type machineConfigCache struct {
	expires      time.Time
	errorCount   uint64
	machineTypes map[string]bool
	gpuCounts    map[string]int64
}

// NewCachedMachineConfigValidator creates a new MachineConfigValidator based on caching.
func NewCachedMachineConfigValidator(gceInternalService gceclient.AutoscalingInternalGceClient, machineFetcher machineTypeFetcher, machineConfigProvider *machinetypes.MachineConfigProvider) *cachedMachineConfigValidator {
	return &cachedMachineConfigValidator{
		configCaches:          make(map[string]machineConfigCache),
		machineFetcher:        machineFetcher,
		gceInternalService:    gceInternalService,
		customMachineTypes:    make(map[string]bool),
		httpTimeout:           gceInternalService.GetHttpTimeout(),
		machineConfigProvider: machineConfigProvider,
	}
}

// ValidateMachineTypeConfig validates machine type config.
func (mcv *cachedMachineConfigValidator) ValidateMachineTypeConfig(machineType, zone string) error {
	timeNow := time.Now()
	// User-defined, not yet known custom machine types case.
	if !mcv.isKnownCustomMachineType(machineType) && gce.IsCustomMachine(machineType) {
		// We gather information about the custom machine type availability in all of zones and save the type as known
		// into customMachineTypes map.
		var zones []string
		zones = append(zones, zone)
		for cachedZone := range mcv.configCaches {
			if cachedZone == zone {
				continue
			}
			zones = append(zones, cachedZone)
		}
		err := mcv.processUnknownCustomMachineType(machineType, zones)
		if err != nil {
			return err
		}
	}

	// Predefined and known custom machine types case
	machineTypes, err := mcv.getMachineTypes(zone, timeNow)
	if err != nil {
		return err
	}
	if _, found := machineTypes[machineType]; !found {
		return errors.NewAutoscalerErrorf(errors.CloudProviderError,
			"Machine type %s not available in %s zone", machineType, zone)
	}
	return nil
}

func (mcv *cachedMachineConfigValidator) isKnownCustomMachineType(machineType string) bool {
	if _, found := mcv.customMachineTypes[machineType]; found {
		return true
	}
	predefinedCustomTypes := mcv.machineConfigProvider.AllAutoprovisionedCustomTypes()
	if _, found := predefinedCustomTypes[machineType]; found {
		return true
	}
	return false
}

// processUnknownCustomMachineType checks if the custom machine type is valid for any of the zones.
// In case of successful gce api call, it saves the new custom machine type into customMachineTypes map of known custom types.
// Updates caches for all zones where the custom machine type is valid.
// In case of fetching error, it resets the cache update time for the zone and returns the error.
func (mcv *cachedMachineConfigValidator) processUnknownCustomMachineType(machineType string, zones []string) error {
	// Check validity for all other zones.
	customMachineTypeValidZones := make(map[string]bool)
	for _, zone := range zones {
		isValid, fetchingError := mcv.validateCustomMachineType(machineType, zone)
		if fetchingError != nil {
			mcv.cacheMutex.Lock()
			// Fetching error occurred. Reset cache update time and return immediately.
			if configCache, found := mcv.configCaches[zone]; found {
				configCache.expires = time.Time{}
				mcv.configCaches[zone] = configCache
			}
			mcv.cacheMutex.Unlock()
			return fetchingError
		}
		if isValid {
			customMachineTypeValidZones[zone] = true
		}
	}
	mcv.cacheMutex.Lock()
	defer mcv.cacheMutex.Unlock()
	// Place custom machine type in the customMachineTypes map to mark it as known.
	// Update cache for all zones where custom machine type is valid.
	mcv.customMachineTypes[machineType] = true
	for validZone := range customMachineTypeValidZones {
		// We do not add cache for the zone in case it is not present in configCaches.
		// We want validator to create it from scratch in getMachineTypes method call.
		if _, found := mcv.configCaches[validZone]; found && mcv.configCaches[validZone].machineTypes != nil {
			mcv.configCaches[validZone].machineTypes[machineType] = true
		}
	}
	return nil // No fetching error.
}

// ValidateGpuConfig validates gpu config
func (mcv *cachedMachineConfigValidator) ValidateGpuConfig(gpuType, gpuPartitionSize, gpuMaxSharedClients, gpuSharingStrategy, machineType string, allocatableGpuCount int64, zone string, cpus, mem int64) error {
	gpuCount := machinetypes.AllocatableGpuCount(allocatableGpuCount)
	if err := mcv.machineConfigProvider.ValidateGpuForMachineType(gpuType, gpuPartitionSize, gpuMaxSharedClients, machineType, gpuCount, cpus, mem); err != nil {
		return err
	}

	zoneGpuCounts, err := mcv.getGpuCount(zone, time.Now())
	if err != nil {
		return err
	}
	err = machinetypes.ValidateGpuSharingStrategy(gpuSharingStrategy)
	if err != nil {
		return err
	}

	physicalGpuCount, err := mcv.machineConfigProvider.ToPhysicalGPUCount(gpuType, gpuPartitionSize, gpuMaxSharedClients, gpuCount)
	if err != nil {
		return err
	}
	maxGpu, found := zoneGpuCounts[gpuType]
	if !found {
		return errors.NewAutoscalerErrorf(errors.CloudProviderError,
			"GPU %s is unavailable in zone %s", gpuType, zone)
	}
	if physicalGpuCount > machinetypes.PhysicalGpuCount(maxGpu) {
		return errors.NewAutoscalerErrorf(errors.CloudProviderError,
			"GPU count of %d not allowed for %s in %s zone", gpuCount, gpuType, zone)
	}
	return nil
}

func (mcv *cachedMachineConfigValidator) getMachineTypes(zone string, now time.Time) (map[string]bool, error) {
	machineTypes, _, err := mcv.getZone(zone, now)
	return machineTypes, err
}

func (mcv *cachedMachineConfigValidator) getGpuCount(zone string, now time.Time) (map[string]int64, error) {
	_, gpuCount, err := mcv.getZone(zone, now)
	return gpuCount, err
}

func (mcv *cachedMachineConfigValidator) getZone(zone string, now time.Time) (map[string]bool, map[string]int64, error) {
	mcv.cacheMutex.Lock()
	configCache := mcv.configCaches[zone]

	if now.Before(configCache.expires) {
		mcv.cacheMutex.Unlock()
	} else {
		// Update expires to prevent concurrent fetches
		// errorCount would be reset after successful fetch
		backoff := computeBackoff(configCache.errorCount, mcv.httpTimeout)
		mcv.configCaches[zone] = machineConfigCache{
			expires:      now.Add(backoff),
			errorCount:   configCache.errorCount + 1,
			machineTypes: configCache.machineTypes,
			gpuCounts:    configCache.gpuCounts,
		}

		if configCache.gpuCounts != nil && configCache.machineTypes != nil {
			// Unlock and run update in the background
			mcv.cacheMutex.Unlock()
			go func() {
				_, _, err := mcv.updateZone(zone, now, false)
				if err != nil {
					klog.Warningf("Failed to refreshed available machine config cache in %q: %v", zone, err)
				}
			}()
		} else {
			var err error
			// Keep locked and update synchronously
			configCache.machineTypes, configCache.gpuCounts, err = mcv.updateZone(zone, now, true)
			mcv.cacheMutex.Unlock()
			if err != nil {
				return nil, nil, err
			}
		}
	}

	if configCache.machineTypes == nil || configCache.gpuCounts == nil {
		return nil, nil, errors.NewAutoscalerErrorf(errors.CloudProviderError,
			"zone %s not fetched", zone)
	}
	return configCache.machineTypes, configCache.gpuCounts, nil
}

func (mcv *cachedMachineConfigValidator) updateZone(zone string, now time.Time, lockAcquired bool) (map[string]bool, map[string]int64, error) {
	predefinedMachineTypes, err := mcv.fetchPredefinedMachineTypes(zone)
	if err != nil {
		return nil, nil, err
	}
	predefinedCustomMachineTypes, err := mcv.fetchPredefinedCustomMachineTypes(zone)
	if err != nil {
		return nil, nil, err
	}
	customMachineTypes, err := mcv.fetchUserDefinedCustomMachineTypes(zone)
	if err != nil {
		return nil, nil, err
	}
	gpuCounts, err := mcv.fetchGpuCounts(zone)
	if err != nil {
		return nil, nil, err
	}

	machineTypes := make(map[string]bool, len(predefinedMachineTypes)+len(predefinedCustomMachineTypes)+len(customMachineTypes))
	for machineType := range predefinedMachineTypes {
		machineTypes[machineType] = true
	}
	for machineType := range predefinedCustomMachineTypes {
		machineTypes[machineType] = true
	}
	for machineType := range customMachineTypes {
		machineTypes[machineType] = true
	}

	// Lock only if run asynchronously
	if !lockAcquired {
		mcv.cacheMutex.Lock()
		defer mcv.cacheMutex.Unlock()
	}

	// Randomized to distribute updates of different zones
	validity := computeValidity()
	nextRefresh := now.Add(validity)
	mcv.configCaches[zone] = machineConfigCache{
		expires:      nextRefresh,
		errorCount:   0,
		machineTypes: machineTypes,
		gpuCounts:    gpuCounts,
	}
	klog.V(2).Infof("Refreshed available machine config cache in %s, next refresh after %v", zone, nextRefresh)
	return machineTypes, gpuCounts, nil
}

func (mcv *cachedMachineConfigValidator) fetchPredefinedMachineTypes(zone string) (map[string]bool, error) {
	klog.V(2).Infof("Fetching predefined machine types for %s", zone)
	machines, err := mcv.machineFetcher.FetchMachineTypes(zone)
	if err != nil {
		klog.Errorf("Couldn't fetch %s zone machine types: %v", zone, err)
		return nil, errors.NewAutoscalerErrorf(errors.CloudProviderError,
			"zone %s could not be fetched: %v", zone, err)
	}

	machineTypes := make(map[string]bool)
	for _, machineType := range machines {
		if machineType.Deprecated != nil {
			state := machineType.Deprecated.State
			klog.V(4).Infof("Machine type %s deprecated status is %s", machineType.Name, state)
			if state == "DELETED" || state == "OBSOLETE" {
				continue
			}
		}
		machineTypes[machineType.Name] = true
	}
	return machineTypes, nil
}

func (mcv *cachedMachineConfigValidator) fetchPredefinedCustomMachineTypes(zone string) (map[string]bool, error) {
	klog.V(2).Infof("Fetching predefined custom machine types for %s", zone)
	return mcv.fetchCustomMachineTypes(mcv.machineConfigProvider.AllAutoprovisionedCustomTypes(), zone)
}
func (mcv *cachedMachineConfigValidator) fetchUserDefinedCustomMachineTypes(zone string) (map[string]bool, error) {
	klog.V(2).Infof("Fetching user-defined custom machine types for %s", zone)
	return mcv.fetchCustomMachineTypes(mcv.customMachineTypes, zone)
}

func (mcv *cachedMachineConfigValidator) fetchCustomMachineTypes(customTypes map[string]bool, zone string) (map[string]bool, error) {
	availableTypes := make(map[string]bool)
	for typeName := range customTypes {
		isValid, fetchingError := mcv.validateCustomMachineType(typeName, zone)
		// If fetching error occurred, we return it immediately.
		if fetchingError != nil {
			return nil, fetchingError
		}
		if !isValid {
			continue
		}
		availableTypes[typeName] = true
	}
	return availableTypes, nil
}

// validateCustomMachineType checks custom machine type for the zone.
// If the machine type is valid, it returns true. False otherwise.
// If fetching error occurrs, it returns an error.
func (mcv *cachedMachineConfigValidator) validateCustomMachineType(machineType, zone string) (bool, error) {
	// Sending request to GCE API to check if custom machine type is valid.
	klog.V(2).Infof("Synchronously updating cache for custom machine type %s in zone %s", machineType, zone)
	_, err := mcv.machineFetcher.FetchMachineType(zone, machineType)

	// Custom machine type is valid for the zone.
	if err == nil {
		klog.V(2).Infof("Custom machine type %s is valid in the zone %s", machineType, zone)
		return true, nil
	}

	// Custom machine type is not valid for the zone.
	if gErr, ok := err.(*googleapi.Error); ok && (gErr.Code == http.StatusBadRequest || gErr.Code == http.StatusNotFound) {
		err = errors.NewAutoscalerErrorf(errors.CloudProviderError, "Invalid custom machine type %q in zone %q: %v", machineType, zone, err)
		klog.V(2).Infof("Custom machine type %s is invalid in the zone %s: %v", machineType, zone, err)

		return false, nil
	}

	// Fetching error occurred.
	err = errors.NewAutoscalerErrorf(errors.CloudProviderError, "Unexpected error while fetching custom machine type %q in zone %q: %v", machineType, zone, err)
	klog.Errorf("Custom machine type %s fetching in zone %s resulted in error: %v", machineType, zone, err)

	return false, err
}

func (mcv *cachedMachineConfigValidator) fetchGpuCounts(zone string) (map[string]int64, error) {
	klog.V(2).Infof("Fetching acceleratorTypes for %s", zone)
	accelerators, err := mcv.gceInternalService.FetchAcceleratorTypes(zone)
	if err != nil {
		klog.Errorf("Couldn't fetch %s zone acceleratorTypes: %v", zone, err)
		return nil, errors.NewAutoscalerErrorf(errors.CloudProviderError,
			"zone %s could not be fetched: %v", zone, err)
	}

	gpuCounts := make(map[string]int64)
	for _, acceleratorType := range accelerators.Items {
		if acceleratorType.Deprecated != nil {
			state := acceleratorType.Deprecated.State
			klog.V(4).Infof("GPU %s deprecated status is %s", acceleratorType.Name, state)
			if state == "DELETED" || state == "OBSOLETE" {
				continue
			}
		}
		klog.V(4).Infof("Maximum %d of %s cards per instance",
			acceleratorType.MaximumCardsPerInstance, acceleratorType.Name)
		gpuCounts[acceleratorType.Name] = acceleratorType.MaximumCardsPerInstance
	}
	return gpuCounts, nil
}

func computeValidity() time.Duration {
	// Randomized to distribute updates of different zones
	return time.Duration((0.8 + 0.2*rand.Float64()) * float64(maxCacheValidity))
}

func computeBackoff(errorCount uint64, httpTimeout time.Duration) time.Duration {
	if errorCount > 20 {
		errorCount = 20
	}
	// Initial backoff longer than HTTP client timeout
	// Randomized to distribute updates of different zones
	// Exponential so delay is twice as long on each error
	backoff := (1.25 + 0.5*rand.Float64()) * float64(httpTimeout) * float64(uint64(1)<<errorCount)
	if backoff > float64(maxCacheValidity) {
		return computeValidity()
	}
	return time.Duration(backoff)
}
