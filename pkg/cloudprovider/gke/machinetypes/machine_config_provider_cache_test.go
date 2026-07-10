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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
)

func TestMachineConfigurationCacheUpdate(t *testing.T) {
	cache := newMachineConfigurationCache()
	newFamilies := map[string]MachineFamily{
		"A2":         A2,
		"new family": {name: "new family"},
	}

	assert.True(t, len(cache.machineFamilies()) > 2)
	cache.update(machineConfiguration{families: newFamilies})

	newCachedFamilies := cache.machineFamilies()
	assert.True(t, len(newCachedFamilies) == 2)

	a2, found := newCachedFamilies["A2"]
	assert.True(t, found)
	assert.Equal(t, A2, a2)

	newFamily, found := newCachedFamilies["new family"]
	assert.True(t, found)
	assert.Equal(t, MachineFamily{name: "new family"}, newFamily)
}

func TestMixedMachineConfigurationCacheUpdate(t *testing.T) {
	cache := newMixedMachineConfigurationCache()
	size := len(cache.coreCache.config.families)

	newC4N := MachineFamily{name: "c4n"}
	pricingOnlyA2 := MachineFamily{
		name: "a2",
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour: 123.45,
		},
		usagePolicy: &UsagePolicy{
			MachineProperties: false,
			Weights:           true,
		},
	}
	propertiesOnlyC3 := MachineFamily{
		name:               "c3",
		systemArchitecture: gce.Arm64,
		usagePolicy: &UsagePolicy{
			MachineProperties: true,
			Weights:           false,
		},
	}
	newFamilies := map[string]MachineFamily{
		"c4n":        newC4N,
		"new family": {name: "new family"},
		"a2":         pricingOnlyA2,
		"c3":         propertiesOnlyC3,
	}

	assert.True(t, size > 10) // Make sure the cache is filled. The exact number of families doesn't matter and can change over time.

	cache.update(newFamilies)

	newCachedFamilies := cache.machineFamilies()
	pricingCachedFamilies := cache.pricingFamilies()

	assert.Equal(t, size+1, len(newCachedFamilies)) // +1 for new family, c4n, a2 and c3 already existed
	assert.Equal(t, size+1, len(pricingCachedFamilies))

	// c4n was fully replaced
	c4n, found := newCachedFamilies["c4n"]
	assert.True(t, found)
	assert.NotEqual(t, C4N, c4n)

	// new family was added
	newFamily, found := newCachedFamilies["new family"]
	assert.True(t, found)
	assert.Equal(t, MachineFamily{name: "new family"}, newFamily)

	// In the core cache, a2 should still be the original fallback family
	a2Core, found := newCachedFamilies["a2"]
	assert.True(t, found)
	assert.Equal(t, A2, a2Core)

	// In the pricing cache, a2 should have the new pricing configuration merged in
	a2Pricing, found := pricingCachedFamilies["a2"]
	assert.True(t, found)
	assert.Equal(t, 123.45, a2Pricing.pricingInfo.CpuPricePerHour)

	// In coreCache, c3 should have the new properties (Arm64) and 0 prices
	c3Core, found := newCachedFamilies["c3"]
	assert.True(t, found)
	assert.Equal(t, gce.Arm64, c3Core.systemArchitecture)
	assert.Equal(t, 0.0, c3Core.pricingInfo.CpuPricePerHour)

	// In pricingCache, c3 should be the fallback family (meaning it has the hardcoded prices)
	c3Pricing, found := pricingCachedFamilies["c3"]
	assert.True(t, found)
	assert.Equal(t, C3.pricingInfo.CpuPricePerHour, c3Pricing.pricingInfo.CpuPricePerHour)
}

// fakeMetrics provides a fake implementation of the machinetypes.Metrics interface for testing.
type fakeMetrics struct {
	Updates map[string]map[string]float64
}

func newFakeMetrics() *fakeMetrics {
	return &fakeMetrics{
		Updates: make(map[string]map[string]float64),
	}
}

func (m *fakeMetrics) UpdateMachineConfigSourceInfo(machineFamily string, configSource ConfigSource, value float64) {
	if m.Updates[machineFamily] == nil {
		m.Updates[machineFamily] = make(map[string]float64)
	}
	m.Updates[machineFamily][string(configSource)] = value
}

func TestMixedMachineConfigurationCacheMetrics(t *testing.T) {
	testCases := []struct {
		name     string
		updates  []map[string]MachineFamily
		expected map[string]map[ConfigSource]float64
	}{
		{
			name:    "initial state all hardcoded",
			updates: nil,
			expected: map[string]map[ConfigSource]float64{
				"c4n": {
					ConfigSourceHardcoded: 1.0,
					ConfigSourceDynamic:   0.0,
				},
			},
		},
		{
			name: "update with dynamic family",
			updates: []map[string]MachineFamily{
				{
					"c4n": {name: "c4n", usagePolicy: &UsagePolicy{MachineProperties: true}},
				},
			},
			expected: map[string]map[ConfigSource]float64{
				"c4n": {
					ConfigSourceHardcoded: 0.0,
					ConfigSourceDynamic:   1.0,
				},
			},
		},
		{
			name: "update with dynamic family and then remove it",
			updates: []map[string]MachineFamily{
				{
					"c4n": {name: "c4n", usagePolicy: &UsagePolicy{MachineProperties: true}},
				},
				nil,
			},
			expected: map[string]map[ConfigSource]float64{
				"c4n": {
					ConfigSourceHardcoded: 1.0,
					ConfigSourceDynamic:   0.0,
				},
			},
		},
		{
			name: "update with dynamic family and then flip to weights-only (hardcoded properties)",
			updates: []map[string]MachineFamily{
				{
					"c4n": {name: "c4n", usagePolicy: &UsagePolicy{MachineProperties: true}},
				},
				{
					"c4n": {name: "c4n", usagePolicy: &UsagePolicy{MachineProperties: false, Weights: true}},
				},
			},
			expected: map[string]map[ConfigSource]float64{
				"c4n": {
					ConfigSourceHardcoded: 1.0,
					ConfigSourceDynamic:   0.0,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache := newMixedMachineConfigurationCache()
			metrics := newFakeMetrics()
			cache.setMetrics(metrics)

			for _, update := range tc.updates {
				metrics.Updates = make(map[string]map[string]float64)
				cache.update(update)
			}

			// Validate expected state for specific families
			for familyName, sources := range tc.expected {
				assert.NotNil(t, metrics.Updates[familyName])
				for source, expectedValue := range sources {
					assert.Equal(t, expectedValue, metrics.Updates[familyName][string(source)])
				}
			}
		})
	}
}
