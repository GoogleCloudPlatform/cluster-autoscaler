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

func newMachineConfigurationCache() *machineConfigurationCache {
	return &machineConfigurationCache{
		config: defaultMachineConfiguration(),
	}
}

func (cache *machineConfigurationCache) update(snapshot map[string]MachineFamily) {
	cache.lock.Lock()
	defer cache.lock.Unlock()

	cache.config = machineConfiguration{families: snapshot}
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
