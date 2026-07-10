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

package machinetypes

import (
	"sync"
)

// machineConfigurationCache holds the machine configuration currently served by
// the MachineConfigProvider.
type machineConfigurationCache struct {
	lock   sync.RWMutex
	config machineConfiguration
}

type machineConfiguration struct {
	families map[string]MachineFamily
}

func (mc machineConfiguration) copyConfig() machineConfiguration {
	families := make(map[string]MachineFamily, len(mc.families))
	for name, mf := range mc.families {
		families[name] = mf
	}
	return machineConfiguration{
		families: families,
	}
}

func newMachineConfigurationCache() *machineConfigurationCache {
	return &machineConfigurationCache{
		config: defaultMachineConfiguration(),
	}
}

func (cache *machineConfigurationCache) update(newConfig machineConfiguration) {
	cache.lock.Lock()
	defer cache.lock.Unlock()

	cache.config = newConfig
}

func (cache *machineConfigurationCache) machineFamilies() map[string]MachineFamily {
	cache.lock.RLock()
	defer cache.lock.RUnlock()

	return cache.config.families
}

func defaultMachineConfiguration() machineConfiguration {
	return machineConfiguration{
		families: machineFamiliesByName,
	}
}

// mixedMachineConfigurationCache allows using mixed hard-coded and dynamic
// configuration. It uses machineConfigurationCache underneath.
// This middle layer exists to separate both MachineConfigProvider and the
// actual caching logic from deciding which configuration to use.
type mixedMachineConfigurationCache struct {
	coreCache             *machineConfigurationCache
	pricingCache          *machineConfigurationCache
	fallbackConfiguration machineConfiguration
	metrics               Metrics
	previousSources       map[string]ConfigSource
}

func (mixedCache *mixedMachineConfigurationCache) update(snapshot map[string]MachineFamily) {
	coreConfig := mixedCache.fallbackConfiguration.copyConfig()
	pricingConfig := mixedCache.fallbackConfiguration.copyConfig()

	newSources := make(map[string]ConfigSource)
	for name := range coreConfig.families {
		newSources[name] = ConfigSourceHardcoded
	}

	for name, mf := range snapshot {
		hasProperties := mf.usagePolicy == nil || mf.usagePolicy.MachineProperties
		hasWeights := mf.usagePolicy == nil || mf.usagePolicy.Weights

		if hasProperties {
			coreConfig.families[name] = mf
			newSources[name] = ConfigSourceDynamic
		}

		if hasWeights {
			if hasProperties {
				pricingConfig.families[name] = mf
			} else {
				// Weights-only update, clone the fallback family before merging weights
				pricingFamily := pricingConfig.families[name].Clone()
				pricingFamily.ApplyPricing(mf)
				pricingConfig.families[name] = pricingFamily
			}
		}
	}
	mixedCache.coreCache.update(coreConfig)
	mixedCache.pricingCache.update(pricingConfig)

	mixedCache.emitMetrics(newSources)
	mixedCache.previousSources = newSources
}

func (mixedCache *mixedMachineConfigurationCache) emitMetrics(newSources map[string]ConfigSource) {
	if mixedCache.metrics != nil {
		for name, source := range newSources {
			if source == ConfigSourceDynamic {
				mixedCache.metrics.UpdateMachineConfigSourceInfo(name, ConfigSourceDynamic, 1.0)
				mixedCache.metrics.UpdateMachineConfigSourceInfo(name, ConfigSourceHardcoded, 0.0)
			} else if source == ConfigSourceHardcoded {
				mixedCache.metrics.UpdateMachineConfigSourceInfo(name, ConfigSourceHardcoded, 1.0)
				mixedCache.metrics.UpdateMachineConfigSourceInfo(name, ConfigSourceDynamic, 0.0)
			}
		}
		for name := range mixedCache.previousSources {
			if _, exists := newSources[name]; !exists {
				mixedCache.metrics.UpdateMachineConfigSourceInfo(name, ConfigSourceDynamic, 0.0)
				mixedCache.metrics.UpdateMachineConfigSourceInfo(name, ConfigSourceHardcoded, 0.0)
			}
		}
	}
}

func (mixedCache *mixedMachineConfigurationCache) machineFamilies() map[string]MachineFamily {
	return mixedCache.coreCache.machineFamilies()
}

func (mixedCache *mixedMachineConfigurationCache) pricingFamilies() map[string]MachineFamily {
	return mixedCache.pricingCache.machineFamilies()
}

func (mixedCache *mixedMachineConfigurationCache) setMetrics(m Metrics) {
	mixedCache.metrics = m
	// Emit metrics for the currently tracked sources as soon as metrics are available.
	if mixedCache.previousSources != nil {
		mixedCache.emitMetrics(mixedCache.previousSources)
	}
}

func newMixedMachineConfigurationCache() *mixedMachineConfigurationCache {
	initialSources := make(map[string]ConfigSource)
	fallback := defaultMachineConfiguration()
	for name := range fallback.families {
		initialSources[name] = ConfigSourceHardcoded
	}
	return &mixedMachineConfigurationCache{
		coreCache:             newMachineConfigurationCache(),
		pricingCache:          newMachineConfigurationCache(),
		fallbackConfiguration: fallback,
		previousSources:       initialSources,
	}
}
