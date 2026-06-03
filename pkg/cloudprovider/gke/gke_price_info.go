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

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
)

// GkePriceInfo is the GKE specific implementation of the PricingInfo
type GkePriceInfo struct {
	provider         *machinetypes.MachineConfigProvider
	autopilotEnabled bool

	gcePriceInfo *gce.GcePriceInfo

	// lock guards access to gkePrices. Allows safe replacement
	// when prices are fetched live.
	lock      sync.RWMutex
	gkePrices gkePrices
}

// NewGkePriceInfo returns a new instance of the GkePriceInfo
func NewGkePriceInfo(provider *machinetypes.MachineConfigProvider, autopilotEnabled bool) *GkePriceInfo {
	priceInfo := &GkePriceInfo{
		provider:         provider,
		autopilotEnabled: autopilotEnabled,
		gcePriceInfo:     gce.NewGcePriceInfo(),
	}
	priceInfo.RefreshGkePrices()
	return priceInfo
}

func (g *GkePriceInfo) RefreshGkePrices() {
	newPrices := g.buildGkePrices()

	g.lock.Lock()
	defer g.lock.Unlock()
	g.gkePrices = newPrices
}

func (g *GkePriceInfo) buildGkePrices() gkePrices {
	gkePrices := newGkePrices()
	for _, family := range g.provider.AllMachineFamilies() {
		familyPriceInfo := family.PricingInfo()
		familyCustomPriceInfo := family.CustomPricingInfo()

		// In Autopilot, for certain compute classes we want to balance scale-ups
		// between all families in a compute class. GkePrice expander would normally
		// always pick the cheapest option, so we have to equalize the families' prices in that case.
		isComputeClassBalancingEnabled := false
		if g.autopilotEnabled {
			if computeClass, found := machinetypes.ComputeClassForMachineFamily(family); found {
				if computeClass.IsFamilyBalancingEnabled() {
					familyPriceInfo = computeClass.CanonicalFamily().PricingInfo()
					familyCustomPriceInfo = computeClass.CanonicalFamily().CustomPricingInfo()
					isComputeClassBalancingEnabled = true
				}
			}
		}

		gkePrices.predefinedCpuPricesPerHour[family.Name()] = familyPriceInfo.CpuPricePerHour
		gkePrices.predefinedMemoryPricePerHourPerGb[family.Name()] = familyPriceInfo.MemoryPricePerHourPerGb
		gkePrices.predefinedPreemptibleDiscount[family.Name()] = familyPriceInfo.PreemptibleDiscount

		if familyCustomPriceInfo != nil {
			gkePrices.customCpuPricesPerHour[family.Name()] = familyCustomPriceInfo.CpuPricePerHour
			gkePrices.customMemoryPricePerHourPerGb[family.Name()] = familyCustomPriceInfo.MemoryPricePerHourPerGb
			gkePrices.customPreemptibleDiscount[family.Name()] = familyCustomPriceInfo.PreemptibleDiscount
		}
		for _, machineType := range family.AllMachineTypes(machinetypes.NoConstraints) {
			mPrice := machineTypePrice(machineType, familyPriceInfo.CpuPricePerHour, familyPriceInfo.MemoryPricePerHourPerGb)
			gkePrices.instancePrices[machineType.Name] = mPrice
			gkePrices.preemptibleInstancePrices[machineType.Name] = mPrice * familyPriceInfo.PreemptibleDiscount

			// if the equivalent flag is not found, add machine type overrides.
			// we don't want to add machine type overrides for eqFlag=true as they represent
			// compute classes which have an intent of balancing between machine families
			// and the machine type overrides don't translate between families
			if !isComputeClassBalancingEnabled {
				if p, found := machineType.InstancePriceOverride(); found {
					gkePrices.instancePrices[machineType.Name] = p
					gkePrices.preemptibleInstancePrices[machineType.Name] = p * familyPriceInfo.PreemptibleDiscount
				}
				if p, found := machineType.PreemptibleInstancePriceOverride(); found {
					gkePrices.preemptibleInstancePrices[machineType.Name] = p
				}
			}
		}
	}
	return gkePrices
}

// machineTypePrice returns the approx. price per hour for this machine type given the cpu and mem pricing.
func machineTypePrice(machineType machinetypes.MachineType, cpuPricePerHour, memPricePerHourPerGb float64) float64 {
	cpuPrice := float64(machineType.CPU) * cpuPricePerHour
	memPrice := float64(machineType.Memory) * memPricePerHourPerGb / float64(units.GiB)
	return cpuPrice + memPrice
}

// BaseCpuPricePerHour gets the base cpu price per hour
func (g *GkePriceInfo) BaseCpuPricePerHour() float64 {
	g.lock.RLock()
	defer g.lock.RUnlock()

	return g.gkePrices.customCpuPricesPerHour[machinetypes.N1.Name()]
}

// BaseMemoryPricePerHourPerGb gets the base memory price per hour per Gb
func (g *GkePriceInfo) BaseMemoryPricePerHourPerGb() float64 {
	g.lock.RLock()
	defer g.lock.RUnlock()

	return g.gkePrices.customMemoryPricePerHourPerGb[machinetypes.N1.Name()]
}

// BasePreemptibleDiscount gets the base preemptible discount applicable
func (g *GkePriceInfo) BasePreemptibleDiscount() float64 {
	g.lock.RLock()
	defer g.lock.RUnlock()

	return g.gkePrices.customPreemptibleDiscount[machinetypes.N1.Name()]
}

// BaseGpuPricePerHour gets the base gpu price per hour
func (g *GkePriceInfo) BaseGpuPricePerHour() float64 {
	return g.gcePriceInfo.BaseGpuPricePerHour()
}

// PredefinedCpuPricePerHour gets the predefined cpu price per hour for machine family
func (g *GkePriceInfo) PredefinedCpuPricePerHour() map[string]float64 {
	g.lock.RLock()
	defer g.lock.RUnlock()

	return g.gkePrices.predefinedCpuPricesPerHour
}

// PredefinedMemoryPricePerHourPerGb gets the predefined memory price per hour per Gb for machine family
func (g *GkePriceInfo) PredefinedMemoryPricePerHourPerGb() map[string]float64 {
	g.lock.RLock()
	defer g.lock.RUnlock()

	return g.gkePrices.predefinedMemoryPricePerHourPerGb
}

// PredefinedPreemptibleDiscount gets the predefined preemptible discount for machine family
func (g *GkePriceInfo) PredefinedPreemptibleDiscount() map[string]float64 {
	g.lock.RLock()
	defer g.lock.RUnlock()

	return g.gkePrices.predefinedPreemptibleDiscount
}

// CustomCpuPricePerHour gets the cpu price per hour for custom machine of a machine family
func (g *GkePriceInfo) CustomCpuPricePerHour() map[string]float64 {
	g.lock.RLock()
	defer g.lock.RUnlock()

	return g.gkePrices.customCpuPricesPerHour
}

// CustomMemoryPricePerHourPerGb gets the memory price per hour per Gb for custom machine of a machine family
func (g *GkePriceInfo) CustomMemoryPricePerHourPerGb() map[string]float64 {
	g.lock.RLock()
	defer g.lock.RUnlock()

	return g.gkePrices.customMemoryPricePerHourPerGb
}

// CustomPreemptibleDiscount gets the preemptible discount of a machine family
func (g *GkePriceInfo) CustomPreemptibleDiscount() map[string]float64 {
	g.lock.RLock()
	defer g.lock.RUnlock()

	return g.gkePrices.customPreemptibleDiscount
}

// InstancePrices gets the prices for standard machine types
func (g *GkePriceInfo) InstancePrices() map[string]float64 {
	g.lock.RLock()
	defer g.lock.RUnlock()

	return g.gkePrices.instancePrices
}

// PreemptibleInstancePrices gets the preemptible prices for standard machine types
func (g *GkePriceInfo) PreemptibleInstancePrices() map[string]float64 {
	g.lock.RLock()
	defer g.lock.RUnlock()

	return g.gkePrices.preemptibleInstancePrices
}

// GpuPrices gets the price of GPUs
func (g *GkePriceInfo) GpuPrices() map[string]float64 {
	prices := g.gcePriceInfo.GpuPrices()
	// TODO(b/272498792): remove when L4 prices is added in open-source
	prices[machinetypes.NvidiaL4.Name()] = 0
	// TODO(b/273300443): remove when H100 prices is updated
	prices[machinetypes.NvidiaH100_80gb.Name()] = 0
	// TODO(b/324866517): remove when H100 Mega prices is updated.
	prices[machinetypes.NvidiaH100Mega_80gb.Name()] = 0
	return prices
}

// PreemptibleGpuPrices gets the price of preemptible GPUs
func (g *GkePriceInfo) PreemptibleGpuPrices() map[string]float64 {
	prices := g.gcePriceInfo.PreemptibleGpuPrices()
	// TODO(b/272498792): remove when L4 prices is added in open-source
	prices[machinetypes.NvidiaL4.Name()] = 0
	// TODO(b/273300443): remove when H100 prices is updated
	prices[machinetypes.NvidiaH100_80gb.Name()] = 0
	// TODO(b/324866517): remove when H100 Mega prices is updated.
	prices[machinetypes.NvidiaH100Mega_80gb.Name()] = 0
	return prices
}

// BootDiskPricePerHour gets the price of boot disk.
func (g *GkePriceInfo) BootDiskPricePerHour() map[string]float64 {
	return g.gcePriceInfo.BootDiskPricePerHour()
}

// LocalSsdPricePerHour gets the price of boot disk.
func (g *GkePriceInfo) LocalSsdPricePerHour() float64 {
	return g.gcePriceInfo.LocalSsdPricePerHour()
}

// SpotLocalSsdPricePerHour gets the price of boot disk.
func (g *GkePriceInfo) SpotLocalSsdPricePerHour() float64 {
	return g.gcePriceInfo.SpotLocalSsdPricePerHour()
}

type gkePrices struct {
	predefinedCpuPricesPerHour        map[string]float64
	predefinedMemoryPricePerHourPerGb map[string]float64
	predefinedPreemptibleDiscount     map[string]float64

	customCpuPricesPerHour        map[string]float64
	customMemoryPricePerHourPerGb map[string]float64
	customPreemptibleDiscount     map[string]float64

	instancePrices            map[string]float64
	preemptibleInstancePrices map[string]float64
}

func newGkePrices() gkePrices {
	g := gkePrices{}
	g.predefinedCpuPricesPerHour = make(map[string]float64)
	g.predefinedMemoryPricePerHourPerGb = make(map[string]float64)
	g.predefinedPreemptibleDiscount = make(map[string]float64)

	g.customCpuPricesPerHour = make(map[string]float64)
	g.customMemoryPricePerHourPerGb = make(map[string]float64)
	g.customPreemptibleDiscount = make(map[string]float64)

	g.instancePrices = make(map[string]float64)
	g.preemptibleInstancePrices = make(map[string]float64)
	return g
}
