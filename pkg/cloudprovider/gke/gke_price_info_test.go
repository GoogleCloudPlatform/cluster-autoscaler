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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
)

func TestPriceConstantsCompatabilityWithGCE(t *testing.T) {
	gcePriceInfo := gce.NewGcePriceInfo()
	gkePriceInfo := NewGkePriceInfo(machinetypes.NewMachineConfigProvider(nil), false)
	epsilon := 0.02

	testFunctions := map[string]func(info gce.PriceInfo) float64{
		"BaseCpuPricePerHour": func(info gce.PriceInfo) float64 {
			return info.BaseCpuPricePerHour()
		},
		"BaseMemoryPricePerHourPerGb": func(info gce.PriceInfo) float64 {
			return info.BaseMemoryPricePerHourPerGb()
		},
		"BasePreemptibleDiscount": func(info gce.PriceInfo) float64 {
			return info.BasePreemptibleDiscount()
		},
		"BaseGpuPricePerHour": func(info gce.PriceInfo) float64 {
			return info.BaseGpuPricePerHour()
		},
		"LocalSsdPricePerHour": func(info gce.PriceInfo) float64 {
			return info.LocalSsdPricePerHour()
		},
		"SpotLocalSsdPricePerHour": func(info gce.PriceInfo) float64 {
			return info.SpotLocalSsdPricePerHour()
		},
	}

	for fn, f := range testFunctions {
		t.Run(fn, func(t *testing.T) {
			assert.InEpsilon(t, f(gcePriceInfo), f(gkePriceInfo), epsilon)
		})
	}
}

func TestPriceMappingsCompatabilityWithGCE(t *testing.T) {
	gcePriceInfo := gce.NewGcePriceInfo()
	gkePriceInfo := NewGkePriceInfo(machinetypes.NewMachineConfigProvider(nil), false)
	epsilon := 0.02

	testFunctions := map[string]func(info gce.PriceInfo) map[string]float64{
		"PredefinedCpuPricePerHour": func(info gce.PriceInfo) map[string]float64 {
			return info.PredefinedCpuPricePerHour()
		},
		"PredefinedMemoryPricePerHourPerGb": func(info gce.PriceInfo) map[string]float64 {
			return info.PredefinedMemoryPricePerHourPerGb()
		},
		"PredefinedPreemptibleDiscount": func(info gce.PriceInfo) map[string]float64 {
			return info.PredefinedPreemptibleDiscount()
		},
		"CustomCpuPricePerHour": func(info gce.PriceInfo) map[string]float64 {
			return info.CustomCpuPricePerHour()
		},
		"CustomMemoryPricePerHourPerGb": func(info gce.PriceInfo) map[string]float64 {
			return info.CustomMemoryPricePerHourPerGb()
		},
		"CustomPreemptibleDiscount": func(info gce.PriceInfo) map[string]float64 {
			return info.CustomPreemptibleDiscount()
		},
		"InstancePrices": func(info gce.PriceInfo) map[string]float64 {
			return info.InstancePrices()
		},
		"PreemptibleInstancePrices": func(info gce.PriceInfo) map[string]float64 {
			return info.PreemptibleInstancePrices()
		},
		"GpuPrices": func(info gce.PriceInfo) map[string]float64 {
			return info.GpuPrices()
		},
		"PreemptibleGpuPrices": func(info gce.PriceInfo) map[string]float64 {
			return info.PreemptibleGpuPrices()
		},
		"BootDiskPricePerHour": func(info gce.PriceInfo) map[string]float64 {
			return info.BootDiskPricePerHour()
		},
	}

	for fn, f := range testFunctions {
		t.Run(fn, func(t *testing.T) {
			gcePrices := f(gcePriceInfo)
			gkePrices := f(gkePriceInfo)
			for name, price := range gcePrices {
				// TODO(b/409956456): match OSS z3 machine family prices with internal
				if strings.HasPrefix(name, "z3") {
					continue
				}
				// TODO(b/530563236): fix GCE & GKE price divergence
				if strings.HasPrefix(name, "n4a") || strings.HasPrefix(name, "n4d") || strings.HasPrefix(name, "z4") {
					continue
				}

				assert.Containsf(t, gkePrices, name, "Price not defined for %q", name)
				if price != gkePrices[name] {
					assert.InEpsilonf(t, price, gkePrices[name], epsilon, "Price diverge for %q", name)
				}
			}
		})
	}
}

func TestAutopilotMachinePricing(t *testing.T) {
	gkeStandardPriceInfo := NewGkePriceInfo(machinetypes.NewMachineConfigProvider(nil), false)
	gkeAutopilotPriceInfo := NewGkePriceInfo(machinetypes.NewMachineConfigProvider(nil), true)

	testFunctions := map[string]func(info gce.PriceInfo) map[string]float64{
		"PredefinedCpuPricePerHour": func(info gce.PriceInfo) map[string]float64 {
			return info.PredefinedCpuPricePerHour()
		},
		"PredefinedMemoryPricePerHourPerGb": func(info gce.PriceInfo) map[string]float64 {
			return info.PredefinedMemoryPricePerHourPerGb()
		},
		"PredefinedPreemptibleDiscount": func(info gce.PriceInfo) map[string]float64 {
			return info.PredefinedPreemptibleDiscount()
		},
	}

	machineFamilies := append(
		machinetypes.ScaleOutClass.MachineFamilies(),
		machinetypes.E2,
		machinetypes.C2,
		machinetypes.C2D,
	)

	for fn, f := range testFunctions {
		t.Run(fn, func(t *testing.T) {
			gkeStandardPrices := f(gkeStandardPriceInfo)
			gkeAutopilotPrices := f(gkeAutopilotPriceInfo)

			for _, family := range machineFamilies {
				assert.Containsf(t, gkeStandardPrices, family.Name(), "GKE Standard price not defined for %q", family.Name())
				assert.Containsf(t, gkeAutopilotPrices, family.Name(), "GKE Autopilot price not defined for %q", family.Name())
				assert.Equalf(t, gkeStandardPrices[family.Name()], gkeAutopilotPrices[family.Name()], "Price diverge for %q", family.Name())
			}
		})
	}
}

func TestAutopilotInstancePricing(t *testing.T) {
	gkeStandardPriceInfo := NewGkePriceInfo(machinetypes.NewMachineConfigProvider(nil), false)
	gkeAutopilotPriceInfo := NewGkePriceInfo(machinetypes.NewMachineConfigProvider(nil), true)

	testFunctions := map[string]func(info gce.PriceInfo) map[string]float64{
		"InstancePrices": func(info gce.PriceInfo) map[string]float64 {
			return info.InstancePrices()
		},
		"PreemptibleInstancePrices": func(info gce.PriceInfo) map[string]float64 {
			return info.PreemptibleInstancePrices()
		},
	}

	machineFamilies := append(
		machinetypes.ScaleOutClass.MachineFamilies(),
		machinetypes.E2,
	)

	for fn, f := range testFunctions {
		t.Run(fn, func(t *testing.T) {
			gkeStandardPrices := f(gkeStandardPriceInfo)
			gkeAutopilotPrices := f(gkeAutopilotPriceInfo)

			for _, family := range machineFamilies {
				for _, machineType := range family.AllMachineTypes(machinetypes.NoConstraints) {
					assert.Containsf(t, gkeStandardPrices, machineType.Name, "GKE Standard price not defined for %q", machineType.Name)
					assert.Containsf(t, gkeAutopilotPrices, machineType.Name, "GKE Autopilot price not defined for %q", machineType.Name)
					assert.Equalf(t, gkeStandardPrices[machineType.Name], gkeAutopilotPrices[machineType.Name], "Price diverge for %q", machineType.Name)
				}
			}
		})
	}
}

func TestCustomComputeClassPricing(t *testing.T) {
	gkePriceInfo := NewGkePriceInfo(machinetypes.NewMachineConfigProvider(nil), true)

	testFunctions := map[string]func(info gce.PriceInfo) map[string]float64{
		"PredefinedCpuPricePerHour": func(info gce.PriceInfo) map[string]float64 {
			return info.PredefinedCpuPricePerHour()
		},
		"PredefinedMemoryPricePerHourPerGb": func(info gce.PriceInfo) map[string]float64 {
			return info.PredefinedMemoryPricePerHourPerGb()
		},
		"PredefinedPreemptibleDiscount": func(info gce.PriceInfo) map[string]float64 {
			return info.PredefinedPreemptibleDiscount()
		},
		"CustomCpuPricePerHour": func(info gce.PriceInfo) map[string]float64 {
			return info.CustomCpuPricePerHour()
		},
		"CustomMemoryPricePerHourPerGb": func(info gce.PriceInfo) map[string]float64 {
			return info.CustomMemoryPricePerHourPerGb()
		},
		"CustomPreemptibleDiscount": func(info gce.PriceInfo) map[string]float64 {
			return info.CustomPreemptibleDiscount()
		},
	}

	for fn, f := range testFunctions {
		t.Run(fn, func(t *testing.T) {
			prices := f(gkePriceInfo)
			price := -1.0
			for _, family := range machinetypes.BalancedClass.MachineFamilies() {
				if price < 0 {
					price = prices[family.Name()]
				}
				assert.Containsf(t, prices, family.Name(), "Price not defined for %q", family.Name())
				assert.Equalf(t, price, prices[family.Name()], "Price diverge for %q", family.Name())
			}
		})
	}
}

func TestCustomComputeClassInstancePricing(t *testing.T) {
	gkePriceInfo := NewGkePriceInfo(machinetypes.NewMachineConfigProvider(nil), true)

	testFunctions := map[string]func(info gce.PriceInfo) map[string]float64{
		"InstancePrices": func(info gce.PriceInfo) map[string]float64 {
			return info.InstancePrices()
		},
		"PreemptibleInstancePrices": func(info gce.PriceInfo) map[string]float64 {
			return info.PreemptibleInstancePrices()
		},
	}

	for fn, f := range testFunctions {
		t.Run(fn, func(t *testing.T) {
			prices := f(gkePriceInfo)
			for _, family := range machinetypes.BalancedClass.MachineFamilies() {
				for _, otherFamily := range machinetypes.BalancedClass.MachineFamilies() {
					if family.Name() != otherFamily.Name() {
						checkEqualInstancePricing(t, prices, family, otherFamily)
					}
				}
			}
		})
	}
}

func checkEqualInstancePricing(t *testing.T, prices map[string]float64, family, otherFamily machinetypes.MachineFamily) {
	for _, machineType := range family.AllMachineTypes(machinetypes.NoConstraints) {
		for _, otherMachineType := range otherFamily.AllMachineTypes(machinetypes.NoConstraints) {
			if machineType.CPU == otherMachineType.CPU && machineType.Memory == otherMachineType.Memory {
				assert.Containsf(t, prices, machineType.Name, "Price not defined for %q", machineType.Name)
				assert.Containsf(t, prices, otherMachineType.Name, "Price not defined for %q", otherMachineType.Name)
				assert.Equalf(t, prices[machineType.Name], prices[otherMachineType.Name], "Price diverge between %q and %q", machineType.Name, otherMachineType.Name)
			}
		}
	}
}
