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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

var (
	NvidiaTeslaK80 = RegisterGpu(Gpu{
		name:           labels.NvidiaTeslaK80,
		isNapSupported: true,
		maxCpuCount: map[PhysicalGpuCount]int{
			1: 8,
			2: 16,
			4: 32,
			8: 64,
		},
	})

	NvidiaTeslaP100 = RegisterGpu(Gpu{
		name:           labels.NvidiaTeslaP100,
		isNapSupported: true,
		maxCpuCount: map[PhysicalGpuCount]int{
			1: 16,
			2: 32,
			4: 64,
		},
	})

	NvidiaTeslaV100 = RegisterGpu(Gpu{
		name:           labels.NvidiaTeslaV100,
		isNapSupported: true,
		maxCpuCount: map[PhysicalGpuCount]int{
			1: 12,
			2: 24,
			4: 48,
			8: 96,
		},
	})

	NvidiaTeslaP4 = RegisterGpu(Gpu{
		name:           labels.NvidiaTeslaP4,
		isNapSupported: true,
		maxCpuCount: map[PhysicalGpuCount]int{
			1: 24,
			2: 48,
			4: 96,
		},
	})

	NvidiaTeslaT4 = RegisterGpu(Gpu{
		name:           labels.NvidiaTeslaT4,
		isNapSupported: true,
		maxCpuCount: map[PhysicalGpuCount]int{
			1: 48,
			2: 48,
			4: 96,
		},
	})

	NvidiaTeslaA100 = RegisterGpu(Gpu{
		name:           labels.NvidiaTeslaA100,
		isNapSupported: true,
		maxCpuCount: map[PhysicalGpuCount]int{
			1:  12,
			2:  24,
			4:  48,
			8:  96,
			16: 96,
		},
		partitionSizes: map[string]int64{
			"1g.5gb":  7,
			"2g.10gb": 3,
			"3g.20gb": 2,
			"7g.40gb": 1,
		},
	})

	NvidiaA100_80gb = RegisterGpu(Gpu{
		name:           labels.NvidiaA100_80gb,
		isNapSupported: true,
		maxCpuCount: map[PhysicalGpuCount]int{
			1: 12,
			2: 24,
			4: 48,
			8: 96,
		},
		partitionSizes: map[string]int64{
			"1g.10gb": 7,
			"2g.20gb": 3,
			"3g.40gb": 2,
			"7g.80gb": 1,
		},
	})

	NvidiaL4 = RegisterGpu(Gpu{
		name:           labels.NvidiaL4,
		isNapSupported: true,
		maxCpuCount: map[PhysicalGpuCount]int{
			1: 32,
			2: 24,
			4: 48,
			8: 96,
		},
	})

	NvidiaH100_80gb = RegisterGpu(Gpu{
		name:           labels.NvidiaH100_80gb,
		isNapSupported: true,
		maxCpuCount: map[PhysicalGpuCount]int{
			1: 26,
			2: 52,
			4: 104,
			8: 208,
		},
		partitionSizes: map[string]int64{
			"1g.10gb": 7,
			"1g.20gb": 4,
			"2g.20gb": 3,
			"3g.40gb": 2,
			"7g.80gb": 1,
		},
	})

	NvidiaH100Mega_80gb = RegisterGpu(Gpu{
		name:           labels.NvidiaH100Mega_80gb,
		isNapSupported: true,
		maxCpuCount: map[PhysicalGpuCount]int{
			8: 208,
		},
		partitionSizes: map[string]int64{
			"1g.10gb": 7,
			"1g.20gb": 4,
			"2g.20gb": 3,
			"3g.40gb": 2,
			"7g.80gb": 1,
		},
	})

	NvidiaH200Ultra_141gb = RegisterGpu(Gpu{
		name:           labels.NvidiaH200Ultra_141gb,
		isNapSupported: true,
		maxCpuCount: map[PhysicalGpuCount]int{
			8: 224,
		},
		partitionSizes: map[string]int64{
			"1g.18gb":  7,
			"1g.35gb":  4,
			"2g.35gb":  3,
			"3g.71gb":  2,
			"7g.141gb": 1,
		},
	})

	NvidiaB200 = RegisterGpu(Gpu{
		name:           labels.NvidiaB200,
		isNapSupported: true,
		maxCpuCount: map[PhysicalGpuCount]int{
			8: 224,
		},
		partitionSizes: map[string]int64{
			"1g.23gb":  7,
			"1g.45gb":  4,
			"2g.45gb":  3,
			"3g.90gb":  2,
			"7g.180gb": 1,
		},
	})

	NvidiaGB200 = RegisterGpu(Gpu{
		name:           labels.NvidiaGB200,
		isNapSupported: true,
		maxCpuCount: map[PhysicalGpuCount]int{
			4: 140,
		},
		partitionSizes: map[string]int64{
			"1g.23gb":  7,
			"1g.47gb":  4,
			"2g.47gb":  3,
			"3g.93gb":  2,
			"7g.186gb": 1,
		},
	})

	NvidiaGB300 = RegisterGpu(Gpu{
		name:           labels.NvidiaGB300,
		isNapSupported: true,
		maxCpuCount: map[PhysicalGpuCount]int{
			4: 144,
		},
	})

	NvidiaRTXPro6000 = RegisterGpu(Gpu{
		name:           labels.NvidiaRTXPro6000,
		isNapSupported: true,
		maxCpuCount: map[PhysicalGpuCount]int{
			1: 48,
			2: 96,
			4: 192,
			8: 384,
		},
		partitionSizes: map[string]int64{
			"1g.24gb":        4,
			"1g.24gb.me":     1,
			"1g.24gb.gfx":    4,
			"1g.24gb.me.all": 1,
			"1g.24gb-me":     4,
			"2g.48gb":        2,
			"2g.48gb.gfx":    2,
			"2g.48gb.me.all": 1,
			"2g.48gb-me":     2,
			"4g.96gb":        1,
			"4g.96gb.gfx":    1,
		},
	})

	// machineFamilyForGPU specifies the machine family to use for a given GPU type
	machineFamilyForGPU = map[string]MachineFamily{
		NvidiaTeslaK80.name:        N1,
		NvidiaTeslaP100.name:       N1,
		NvidiaTeslaV100.name:       N1,
		NvidiaTeslaP4.name:         N1,
		NvidiaTeslaT4.name:         N1,
		NvidiaTeslaA100.name:       A2,
		NvidiaA100_80gb.name:       A2,
		NvidiaL4.name:              G2,
		NvidiaH100_80gb.name:       A3,
		NvidiaH100Mega_80gb.name:   A3,
		NvidiaH200Ultra_141gb.name: A3,
		NvidiaB200.name:            A4,
		NvidiaGB200.name:           A4X,
		NvidiaGB300.name:           A4X,
		NvidiaRTXPro6000.name:      G4,
	}

	draAcceleratorInfos = map[string]DraAcceleratorInfo{
		NvidiaTeslaK80.name: {
			Brand:        "Nvidia",
			Model:        "Tesla K80",
			Architecture: "Kepler",
			CapacityGB:   11,
		},
		NvidiaTeslaP100.name: {
			Brand:        "Nvidia",
			Model:        "Tesla P100",
			Architecture: "Pascal",
			CapacityGB:   15,
		},
		NvidiaTeslaP4.name: {
			Brand:        "Nvidia",
			Model:        "Tesla P4",
			Architecture: "Pascal",
			CapacityGB:   8,
		},
		NvidiaTeslaV100.name: {
			Brand:        "Nvidia",
			Model:        "Tesla V100",
			Architecture: "Volta",
			CapacityGB:   15,
		},
		NvidiaTeslaT4.name: {
			Brand:        "Nvidia",
			Model:        "Tesla T4",
			Architecture: "Turing",
			CapacityGB:   15,
		},

		NvidiaTeslaA100.name: {
			Brand:        "Nvidia",
			Model:        "A100 40GB",
			Architecture: "Ampere",
			CapacityGB:   38,
		},
		NvidiaA100_80gb.name: {
			Brand:        "Nvidia",
			Model:        "A100 80GB",
			Architecture: "Ampere",
			CapacityGB:   75,
		},
		NvidiaL4.name: {
			Brand:        "Nvidia",
			Model:        "L4",
			Architecture: "Ada Lovelace",
			CapacityGB:   23,
		},
		NvidiaH100_80gb.name: {
			Brand:        "Nvidia",
			Model:        "H100 80GB",
			Architecture: "Hopper",
			CapacityGB:   75,
		},
	}
)
