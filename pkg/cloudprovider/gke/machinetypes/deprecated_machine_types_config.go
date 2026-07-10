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
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

// DEPRECATION NOTICE: This configuration will become obsolete after the
// migration to the MachineConfig CRD is complete.
// Any configuration change here must be reflected in the underlying APIs.
// Check go/gke-extend-machine-config for details.
var (

	// A2 represents a2 machine family
	A2 = RegisterMachineFamily(MachineFamily{
		name:               "a2",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.031611,
			MemoryPricePerHourPerGb: 0.004237,
			PreemptibleDiscount:     0.009483 / 0.031611,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			// Initial A2 machines - compatible with A100-40gb.
			NewMachineTypeInfo("a2-highgpu-1g", 12, 85).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8).
				withGpuOverride(NvidiaTeslaA100, 1).
				withInstancePriceOverride(3.673385), // price additionally contains the cost of 1 A100 GPU,

			NewMachineTypeInfo("a2-highgpu-2g", 24, 170).
				withAllowedEphemeralLocalSsdCounts(2, 4, 8).
				withGpuOverride(NvidiaTeslaA100, 2).
				withInstancePriceOverride(7.34677), // price additionally contains the cost of 2 A100 GPU

			NewMachineTypeInfo("a2-highgpu-4g", 48, 340).
				withAllowedEphemeralLocalSsdCounts(4, 8).
				withGpuOverride(NvidiaTeslaA100, 4).
				withInstancePriceOverride(14.69354), // price additionally contains the cost of 4 A100 GPU

			NewMachineTypeInfo("a2-highgpu-8g", 96, 680).
				withAllowedEphemeralLocalSsdCounts(8).
				withGpuOverride(NvidiaTeslaA100, 8).
				withInstancePriceOverride(29.38708), // price additionally contains the cost of 8 A100 GPU

			NewMachineTypeInfo("a2-megagpu-16g", 96, 1360).
				withAllowedEphemeralLocalSsdCounts(8).
				withGpuOverride(NvidiaTeslaA100, 16).
				withInstancePriceOverride(55.739504), // price additionally contains the cost of 16 A100 GPU

			// A2+ machines - compatible with A100-80gb.
			// A2+ machines have a slightly different on-demand/preemptible ratio.
			NewMachineTypeInfo("a2-ultragpu-1g", 12, 170).
				withGpuOverride(NvidiaA100_80gb, 1).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(5.0688).
				withPreemptibleInstancePriceOverride(1.6), // price additionally contains the cost of 1 A100-80gb GPU

			NewMachineTypeInfo("a2-ultragpu-2g", 24, 340).
				withGpuOverride(NvidiaA100_80gb, 2).
				withAutomaticEphemeralLocalSsdCount(2).
				withInstancePriceOverride(10.1376).
				withPreemptibleInstancePriceOverride(3.2), // price additionally contains the cost of 2 A100-80gb GPU

			NewMachineTypeInfo("a2-ultragpu-4g", 48, 680).
				withGpuOverride(NvidiaA100_80gb, 4).
				withAutomaticEphemeralLocalSsdCount(4).
				withInstancePriceOverride(20.2752).
				withPreemptibleInstancePriceOverride(6.4), // price additionally contains the cost of 4 A100-80gb GPU

			NewMachineTypeInfo("a2-ultragpu-8g", 96, 1360).
				withGpuOverride(NvidiaA100_80gb, 8).
				withAutomaticEphemeralLocalSsdCount(8).
				withInstancePriceOverride(40.5504).
				withPreemptibleInstancePriceOverride(12.8), // price additionally contains the cost of 8 A100-80gb GPU

			// A2 shapes without local SSD
			NewMachineTypeInfo("a2-ultragpu-1g-nolssd", 12, 170). // - 375 * 0.08 / 3730
										withGpuOverride(NvidiaA100_80gb, 1).
										withInstancePriceOverride(5.0277). // price reduced by lssd cost: [375 GB] * [0.08 $ / GB / month] / 730 [h / month]
										withExplicitReqOnly(),

			NewMachineTypeInfo("a2-ultragpu-2g-nolssd", 24, 340).
				withGpuOverride(NvidiaA100_80gb, 2).
				withInstancePriceOverride(10.0554). // price reduced by lssd cost: [750 GB] * [0.08 $ / GB / month] / 730 [h / month]
				withExplicitReqOnly(),

			NewMachineTypeInfo("a2-ultragpu-4g-nolssd", 48, 680).
				withGpuOverride(NvidiaA100_80gb, 4).
				withInstancePriceOverride(20.1108). // price reduced by lssd cost: [1500 GB] * [0.08 $ / GB / month] / 730 [h / month]
				withExplicitReqOnly(),

			NewMachineTypeInfo("a2-ultragpu-8g-nolssd", 96, 1360).
				withGpuOverride(NvidiaA100_80gb, 8).
				withInstancePriceOverride(40.2216). // price reduced by lssd cost: [3000 GB] * [0.08 $ / GB / month] / 730 [h / month]
				withExplicitReqOnly(),
		),
		supportedGpuTypes: onboardSupportedGpus(
			NvidiaA100_80gb,
			NvidiaTeslaA100,
		),
		supportedCpuPlatforms:    CpuPlatformRequirements{lowerBound: IntelCascadeLake, upperBound: IntelCascadeLake},
		supportCompactPlacement:  true,
		maxCompactPlacementNodes: int64(150),
		supportedBootDiskTypes: map[string]bool{
			DiskTypeStandard: true,
			DiskTypeBalanced: true,
			DiskTypeSSD:      true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced: ConfidentialOnlyMode,
			DiskTypeHyperdiskMl:       NonConfidentialOnlyMode,
			DiskTypeBalanced:          UnspecifiedMode,
			DiskTypePDExtreme:         UnspecifiedMode,
			DiskTypeSSD:               UnspecifiedMode,
			DiskTypeStandard:          UnspecifiedMode,
		},
		defaultDiskType: DiskTypeStandard,
	})
	// A3 represents a3 machine family.
	A3 = RegisterMachineFamily(MachineFamily{
		name:               "a3",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.02466,
			MemoryPricePerHourPerGb: 0.002147,
			PreemptibleDiscount:     0.01036 / 0.02466,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			// A3 machines for nvidia-h100-80gb.
			// For A3 only a full VM shape with 8 GPUs supports local SSD opt-out.
			NewMachineTypeInfo("a3-highgpu-1g", 26, 234).
				withGpuOverride(NvidiaH100_80gb, 1).
				withAutomaticEphemeralLocalSsdCount(2).
				withInstancePriceOverride(10.62).
				withPreemptibleInstancePriceOverride(4.46). // price additionally contains the cost of 1 H100 GPU
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}),
			NewMachineTypeInfo("a3-highgpu-2g", 52, 468).
				withGpuOverride(NvidiaH100_80gb, 2).
				withAutomaticEphemeralLocalSsdCount(4).
				withInstancePriceOverride(21.24).
				withPreemptibleInstancePriceOverride(8.92), // price additionally contains the cost of 2 H100 GPU
			NewMachineTypeInfo("a3-highgpu-4g", 104, 936).
				withGpuOverride(NvidiaH100_80gb, 4).
				withAutomaticEphemeralLocalSsdCount(8).
				withInstancePriceOverride(42.47).
				withPreemptibleInstancePriceOverride(17.84), // price additionally contains the cost of 4 H100 GPU
			NewMachineTypeInfo("a3-highgpu-8g", 208, 1872).
				withGpuOverride(NvidiaH100_80gb, 8).
				withAutomaticEphemeralLocalSsdCount(16).
				withInstancePriceOverride(84.94).
				withPreemptibleInstancePriceOverride(35.67), // price additionally contains the cost of 8 H100 GPU
			NewMachineTypeInfo("a3-edgegpu-8g", 208, 1872).
				withGpuOverride(NvidiaH100_80gb, 8).
				withAutomaticEphemeralLocalSsdCount(16).
				withInstancePriceOverride(84.94).
				withPreemptibleInstancePriceOverride(35.67). // price additionally contains the cost of 8 H100 GPU
				withDwsDisabled(),
			NewMachineTypeInfo("a3-megagpu-8g", 208, 1872).
				withGpuOverride(NvidiaH100Mega_80gb, 8).
				withAutomaticEphemeralLocalSsdCount(16).
				withInstancePriceOverride(93.40).
				withPreemptibleInstancePriceOverride(65.39), // price additionally contains the cost of 8 H100 Mega GPU
			NewMachineTypeInfo("a3-highgpu-8g-nolssd", 208, 1872).
				withGpuOverride(NvidiaH100_80gb, 8).
				withInstancePriceOverride(84.2667). // price reduced by lssd cost: [6 TiB] * [0.08 $ / GB / month] / 730 [h / month]
				withExplicitReqOnly(),
			NewMachineTypeInfo("a3-edgegpu-8g-nolssd", 208, 1872).
				withGpuOverride(NvidiaH100_80gb, 8).
				withInstancePriceOverride(84.2667). // price reduced by lssd cost: [6 TiB] * [0.08 $ / GB / month] / 730 [h / month]
				withExplicitReqOnly().
				withDwsDisabled(),
			NewMachineTypeInfo("a3-megagpu-8g-nolssd", 208, 1872).
				withGpuOverride(NvidiaH100Mega_80gb, 8).
				withInstancePriceOverride(92.7267). // price reduced by lssd cost: [6 TiB] * [0.08 $ / GB / month] / 730 [h / month]
				withExplicitReqOnly(),
			NewMachineTypeInfo("a3-highgpu-1g-nolssd", 26, 234).
				withGpuOverride(NvidiaH100_80gb, 1).
				withInstancePriceOverride(10.5358). // price reduced by lssd cost: [0.75 TiB] * [0.08 $ / GB / month] / 730 [h / month]
				withExplicitReqOnly().
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}),
			NewMachineTypeInfo("a3-highgpu-2g-nolssd", 52, 468).
				withGpuOverride(NvidiaH100_80gb, 2).
				withInstancePriceOverride(21.0717). // price reduced by lssd cost: [1.5 TiB] * [0.08 $ / GB / month] / 730 [h / month]
				withExplicitReqOnly(),
			NewMachineTypeInfo("a3-highgpu-4g-nolssd", 104, 936).
				withGpuOverride(NvidiaH100_80gb, 4).
				withInstancePriceOverride(42.1333). // price reduced by lssd cost: [3 TiB] * [0.08 $ / GB / month] / 730 [h / month]
				withExplicitReqOnly(),
			NewMachineTypeInfo("a3-ultragpu-8g", 224, 2952).
				withSupportedDisksOverride([]string{DiskTypeHyperdiskBalanced}).
				withDefaultDiskOverride(DiskTypeHyperdiskBalanced).
				withGpuOverride(NvidiaH200Ultra_141gb, 8).
				withAutomaticEphemeralLocalSsdCount(32).
				withInstancePriceOverride(140.1).
				withPreemptibleInstancePriceOverride(98.085), // price additionally contains the cost of 8 H200 Mega GPU
			NewMachineTypeInfo("a3-ultragpu-8g-nolssd", 224, 2952).
				withSupportedDisksOverride([]string{DiskTypeHyperdiskBalanced}).
				withDefaultDiskOverride(DiskTypeHyperdiskBalanced).
				withGpuOverride(NvidiaH200Ultra_141gb, 8).
				withInstancePriceOverride(139.4267). // price reduced by lssd cost: [6 TiB] * [0.08 $ / GB / month] / 730 [h / month]
				withExplicitReqOnly(),
		),
		supportedCpuPlatforms:    CpuPlatformRequirements{lowerBound: IntelCascadeLake, upperBound: IntelCascadeLake},
		supportCompactPlacement:  true,
		maxCompactPlacementNodes: int64(96),
		supportedGpuTypes: onboardSupportedGpus(
			NvidiaH100_80gb,
			NvidiaH100Mega_80gb,
			NvidiaH200Ultra_141gb,
		),
		supportedBootDiskTypes: map[string]bool{
			DiskTypeBalanced:          true,
			DiskTypeSSD:               true,
			DiskTypeHyperdiskBalanced: true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced:                 NonConfidentialOnlyMode,
			DiskTypeHyperdiskBalancedHighAvailability: NonConfidentialOnlyMode,
			DiskTypeHyperdiskExtreme:                  NonConfidentialOnlyMode,
			DiskTypeHyperdiskThroughput:               NonConfidentialOnlyMode,
			DiskTypeHyperdiskMl:                       NonConfidentialOnlyMode,
			DiskTypeBalanced:                          UnspecifiedMode,
			DiskTypeSSD:                               UnspecifiedMode,
		},
		defaultDiskType:       DiskTypeBalanced,
		supportHugepageSize1g: true,
	})
	// A4 represents a4 machine family.
	A4 = RegisterMachineFamily(MachineFamily{
		name:               "a4",
		systemArchitecture: gce.Amd64,
		// TODO: b/390949819 update prices
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.03699,
			MemoryPricePerHourPerGb: 0.003221,
			PreemptibleDiscount:     0.01036 / 0.03699,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			// A4 machines for nvidia-b200.
			// TODO: b/390949821 smaller VM shapes to be added post-GA
			NewMachineTypeInfo("a4-highgpu-8g", 224, 3968).
				withGpuOverride(NvidiaB200, 8).
				withAutomaticEphemeralLocalSsdCount(32).
				withInstancePriceOverride(140.1).
				withPreemptibleInstancePriceOverride(98.085),
			NewMachineTypeInfo("a4-highgpu-8g-nolssd", 224, 3968).
				withGpuOverride(NvidiaB200, 8).
				withInstancePriceOverride(139.4267).
				withExplicitReqOnly(),
		),
		supportedCpuPlatforms:    CpuPlatformRequirements{lowerBound: IntelEmeraldRapids, upperBound: IntelEmeraldRapids},
		supportCompactPlacement:  true,
		maxCompactPlacementNodes: int64(1500),
		supportedGpuTypes: onboardSupportedGpus(
			NvidiaB200,
		),
		supportedBootDiskTypes: map[string]bool{
			DiskTypeHyperdiskBalanced: true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced: NonConfidentialOnlyMode,
			DiskTypeHyperdiskExtreme:  NonConfidentialOnlyMode,
			DiskTypeHyperdiskMl:       UnspecifiedMode,
		},
		defaultDiskType:       DiskTypeHyperdiskBalanced,
		supportHugepageSize1g: true,
	})
	A4X = RegisterMachineFamily(MachineFamily{
		name:               "a4x",
		systemArchitecture: gce.Arm64,
		// TODO(b/405041492): update pricing info when available
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.03699,
			MemoryPricePerHourPerGb: 0.003221,
			PreemptibleDiscount:     0.01036 / 0.03699,
		},
		// TODO(b/405045353): add smaller VM shapes post-GA
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("a4x-highgpu-4g", 140, 884).
				withGpuOverride(NvidiaGB200, 4).
				withAutomaticEphemeralLocalSsdCount(4).
				// TODO(b/405041492): update pricing info when available
				withInstancePriceOverride(140.1).
				withPreemptibleInstancePriceOverride(98.085),
			NewMachineTypeInfo("a4x-highgpu-4g-nolssd", 140, 884).
				withGpuOverride(NvidiaGB200, 4).
				// TODO(b/405041492): update pricing info when available
				withInstancePriceOverride(140.1).
				withExplicitReqOnly(),
			NewMachineTypeInfo("a4x-maxgpu-4g-metal", 144, 960).
				withGpuOverride(NvidiaGB300, 4).
				withAutomaticEphemeralLocalSsdCount(4).
				withExplicitReqOnly(),
		),
		supportedCpuPlatforms: CpuPlatformRequirements{lowerBound: NvidiaGrace, upperBound: NvidiaGrace},
		supportedGpuTypes:     onboardSupportedGpus(NvidiaGB200, NvidiaGB300),
		supportedBootDiskTypes: map[string]bool{
			DiskTypeHyperdiskBalanced: true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced: NonConfidentialOnlyMode,
			DiskTypeHyperdiskExtreme:  NonConfidentialOnlyMode,
			DiskTypeHyperdiskMl:       UnspecifiedMode,
		},
		defaultDiskType:               DiskTypeHyperdiskBalanced,
		supportHugepageSize1g:         true,
		supportsAcceleratorSlice:      true,
		draComputeDomainAutoDetection: true,
	})
	// C2 represents c2 machine family
	C2 = RegisterMachineFamily(MachineFamily{
		name:               "c2",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.03398,
			MemoryPricePerHourPerGb: 0.00455,
			PreemptibleDiscount:     0.00822 / 0.03398,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("c2-standard-4", 4, 16).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8),
			NewMachineTypeInfo("c2-standard-8", 8, 32).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8),
			NewMachineTypeInfo("c2-standard-16", 16, 64).
				withAllowedEphemeralLocalSsdCounts(2, 4, 8),
			NewMachineTypeInfo("c2-standard-30", 30, 120).
				withAllowedEphemeralLocalSsdCounts(4, 8),
			NewMachineTypeInfo("c2-standard-60", 60, 240).
				withAllowedEphemeralLocalSsdCounts(8),
		),
		supportedCpuPlatforms:    CpuPlatformRequirements{lowerBound: IntelCascadeLake, upperBound: IntelCascadeLake},
		supportCompactPlacement:  true,
		maxCompactPlacementNodes: int64(150),
		supportedBootDiskTypes: map[string]bool{
			DiskTypeStandard: true,
			DiskTypeBalanced: true,
			DiskTypeSSD:      true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeBalanced:  UnspecifiedMode,
			DiskTypePDExtreme: UnspecifiedMode,
			DiskTypeSSD:       UnspecifiedMode,
			DiskTypeStandard:  UnspecifiedMode,
		},
		defaultDiskType: DiskTypeStandard,
	})
	// C2D represents c2d machine family
	C2D = RegisterMachineFamily(MachineFamily{
		name:               "c2d",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.029563,
			MemoryPricePerHourPerGb: 0.003959,
			PreemptibleDiscount:     0.007154 / 0.029563,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("c2d-standard-2", 2, 8).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8),
			NewMachineTypeInfo("c2d-standard-4", 4, 16).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8),
			NewMachineTypeInfo("c2d-standard-8", 8, 32).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8),
			NewMachineTypeInfo("c2d-standard-16", 16, 64).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8),
			NewMachineTypeInfo("c2d-standard-32", 32, 128).
				withAllowedEphemeralLocalSsdCounts(2, 4, 8),
			NewMachineTypeInfo("c2d-standard-56", 56, 224).
				withAllowedEphemeralLocalSsdCounts(4, 8),
			NewMachineTypeInfo("c2d-standard-112", 112, 448).
				withAllowedEphemeralLocalSsdCounts(8),
			NewMachineTypeInfo("c2d-highcpu-2", 2, 4).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8),
			NewMachineTypeInfo("c2d-highcpu-4", 4, 8).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8),
			NewMachineTypeInfo("c2d-highcpu-8", 8, 16).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8),
			NewMachineTypeInfo("c2d-highcpu-16", 16, 32).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8),
			NewMachineTypeInfo("c2d-highcpu-32", 32, 64).
				withAllowedEphemeralLocalSsdCounts(2, 4, 8),
			NewMachineTypeInfo("c2d-highcpu-56", 56, 112).
				withAllowedEphemeralLocalSsdCounts(4, 8),
			NewMachineTypeInfo("c2d-highcpu-112", 112, 224).
				withAllowedEphemeralLocalSsdCounts(8),
			NewMachineTypeInfo("c2d-highmem-2", 2, 16).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8),
			NewMachineTypeInfo("c2d-highmem-4", 4, 32).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8),
			NewMachineTypeInfo("c2d-highmem-8", 8, 64).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8),
			NewMachineTypeInfo("c2d-highmem-16", 16, 128).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8),
			NewMachineTypeInfo("c2d-highmem-32", 32, 256).
				withAllowedEphemeralLocalSsdCounts(2, 4, 8),
			NewMachineTypeInfo("c2d-highmem-56", 56, 448).
				withAllowedEphemeralLocalSsdCounts(4, 8),
			NewMachineTypeInfo("c2d-highmem-112", 112, 896).
				withAllowedEphemeralLocalSsdCounts(8),
		),
		supportedCpuPlatforms:        CpuPlatformRequirements{lowerBound: AmdMilan, upperBound: AmdMilan},
		supportCompactPlacement:      true,
		maxCompactPlacementNodes:     int64(150),
		supportConfidentialNodes:     true,
		supportConfidentialNodeTypes: map[string]bool{labels.SEVConfidentialNodeTypeValue: true},
		supportedBootDiskTypes: map[string]bool{
			DiskTypeStandard: true,
			DiskTypeBalanced: true,
			DiskTypeSSD:      true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeBalanced:  UnspecifiedMode,
			DiskTypePDExtreme: UnspecifiedMode,
			DiskTypeSSD:       UnspecifiedMode,
			DiskTypeStandard:  UnspecifiedMode,
		},
		defaultDiskType:       DiskTypeStandard,
		supportHugepageSize1g: true,
	})
	// C3 represents c3 machine family
	C3 = RegisterMachineFamily(MachineFamily{
		name:               "c3",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.03398,
			MemoryPricePerHourPerGb: 0.00456,
			PreemptibleDiscount:     0.003086 / 0.03398,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("c3-standard-4", 4, 16).
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}),
			NewMachineTypeInfo("c3-standard-8", 8, 32).
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}),
			NewMachineTypeInfo("c3-standard-22", 22, 88).
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}),
			NewMachineTypeInfo("c3-standard-44", 44, 176).
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}),
			NewMachineTypeInfo("c3-standard-88", 88, 352).
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}),
			NewMachineTypeInfo("c3-standard-176", 176, 704).
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}),
			NewMachineTypeInfo("c3-highcpu-4", 4, 8),
			NewMachineTypeInfo("c3-highcpu-8", 8, 16),
			NewMachineTypeInfo("c3-highcpu-22", 22, 44),
			NewMachineTypeInfo("c3-highcpu-44", 44, 88),
			NewMachineTypeInfo("c3-highcpu-88", 88, 176),
			NewMachineTypeInfo("c3-highcpu-176", 176, 352),
			NewMachineTypeInfo("c3-highmem-4", 4, 32),
			NewMachineTypeInfo("c3-highmem-8", 8, 64),
			NewMachineTypeInfo("c3-highmem-22", 22, 176),
			NewMachineTypeInfo("c3-highmem-44", 44, 352),
			NewMachineTypeInfo("c3-highmem-88", 88, 704),
			NewMachineTypeInfo("c3-highmem-176", 176, 1408),
			// Local SSD bundled c3 machine types
			NewMachineTypeInfo("c3-standard-4-lssd", 4, 16).
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(0.25055).
				withPreemptibleInstancePriceOverride(0.12),
			NewMachineTypeInfo("c3-standard-8-lssd", 8, 32).
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}).
				withAutomaticEphemeralLocalSsdCount(2).
				withInstancePriceOverride(0.50109).
				withPreemptibleInstancePriceOverride(0.23),
			NewMachineTypeInfo("c3-standard-22-lssd", 22, 88).
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}).
				withAutomaticEphemeralLocalSsdCount(4).
				withInstancePriceOverride(1.31551).
				withPreemptibleInstancePriceOverride(0.59),
			NewMachineTypeInfo("c3-standard-44-lssd", 44, 176).
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}).
				withAutomaticEphemeralLocalSsdCount(8).
				withInstancePriceOverride(2.63101).
				withPreemptibleInstancePriceOverride(1.18),
			NewMachineTypeInfo("c3-standard-88-lssd", 88, 352).
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}).
				withAutomaticEphemeralLocalSsdCount(16).
				withInstancePriceOverride(5.26203).
				withPreemptibleInstancePriceOverride(2.36),
			NewMachineTypeInfo("c3-standard-176-lssd", 176, 704).
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}).
				withAutomaticEphemeralLocalSsdCount(32).
				withInstancePriceOverride(10.52405).
				withPreemptibleInstancePriceOverride(4.72),
			// C3 bare metal machine types
			NewMachineTypeInfo("c3-highcpu-192-metal", 192, 512).
				withExplicitReqOnly(),
			NewMachineTypeInfo("c3-standard-192-metal", 192, 768).
				withExplicitReqOnly(),
			NewMachineTypeInfo("c3-highmem-192-metal", 192, 1536).
				withExplicitReqOnly(),
		),
		supportedCpuPlatforms:    CpuPlatformRequirements{lowerBound: IntelSapphireRapids, upperBound: IntelSapphireRapids},
		supportCompactPlacement:  true,
		maxCompactPlacementNodes: int64(150),
		supportedBootDiskTypes: map[string]bool{
			DiskTypeBalanced:          true,
			DiskTypeSSD:               true,
			DiskTypeHyperdiskBalanced: true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced:                 NonConfidentialOnlyMode,
			DiskTypeHyperdiskExtreme:                  NonConfidentialOnlyMode,
			DiskTypeHyperdiskThroughput:               NonConfidentialOnlyMode,
			DiskTypeHyperdiskMl:                       NonConfidentialOnlyMode,
			DiskTypeHyperdiskBalancedHighAvailability: NonConfidentialOnlyMode,
			DiskTypeBalanced:                          UnspecifiedMode,
			DiskTypeSSD:                               UnspecifiedMode,
			DiskTypePDExtreme:                         UnspecifiedMode,
		},
		defaultDiskType:       DiskTypeBalanced,
		supportHugepageSize1g: true,
	})
	// C3D represents c3d machine family
	C3D = RegisterMachineFamily(MachineFamily{
		name:               "c3d",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.02956,
			MemoryPricePerHourPerGb: 0.003956,
			PreemptibleDiscount:     0.011825 / 0.02956,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("c3d-standard-4", 4, 16),
			NewMachineTypeInfo("c3d-standard-8", 8, 32),
			NewMachineTypeInfo("c3d-standard-16", 16, 64),
			NewMachineTypeInfo("c3d-standard-30", 30, 120),
			NewMachineTypeInfo("c3d-standard-60", 60, 240),
			NewMachineTypeInfo("c3d-standard-90", 90, 360),
			NewMachineTypeInfo("c3d-standard-180", 180, 720),
			NewMachineTypeInfo("c3d-standard-360", 360, 1440),
			NewMachineTypeInfo("c3d-highcpu-4", 4, 8),
			NewMachineTypeInfo("c3d-highcpu-8", 8, 16),
			NewMachineTypeInfo("c3d-highcpu-16", 16, 32),
			NewMachineTypeInfo("c3d-highcpu-30", 30, 59),
			NewMachineTypeInfo("c3d-highcpu-60", 60, 118),
			NewMachineTypeInfo("c3d-highcpu-90", 90, 177),
			NewMachineTypeInfo("c3d-highcpu-180", 180, 354),
			NewMachineTypeInfo("c3d-highcpu-360", 360, 708),
			NewMachineTypeInfo("c3d-highmem-4", 4, 32),
			NewMachineTypeInfo("c3d-highmem-8", 8, 64),
			NewMachineTypeInfo("c3d-highmem-16", 16, 128),
			NewMachineTypeInfo("c3d-highmem-30", 30, 240),
			NewMachineTypeInfo("c3d-highmem-60", 60, 480),
			NewMachineTypeInfo("c3d-highmem-90", 90, 720),
			NewMachineTypeInfo("c3d-highmem-180", 180, 1440),
			NewMachineTypeInfo("c3d-highmem-360", 360, 2880),
			NewMachineTypeInfo("c3d-standard-8-lssd", 8, 32).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(0.403515).
				withPreemptibleInstancePriceOverride(0.080652),
			NewMachineTypeInfo("c3d-standard-16-lssd", 16, 64).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(0.766707).
				withPreemptibleInstancePriceOverride(0.149308),
			NewMachineTypeInfo("c3d-standard-30-lssd", 30, 120).
				withAutomaticEphemeralLocalSsdCount(2).
				withInstancePriceOverride(1.442615).
				withPreemptibleInstancePriceOverride(0.281452),
			NewMachineTypeInfo("c3d-standard-60-lssd", 60, 240).
				withAutomaticEphemeralLocalSsdCount(4).
				withInstancePriceOverride(2.885230).
				withPreemptibleInstancePriceOverride(0.562904),
			NewMachineTypeInfo("c3d-standard-90-lssd", 90, 360).
				withAutomaticEphemeralLocalSsdCount(8).
				withInstancePriceOverride(4.408491).
				withPreemptibleInstancePriceOverride(0.868348),
			NewMachineTypeInfo("c3d-standard-90-12TB", 90, 720).
				withAutomaticEphemeralLocalSsdCount(32).
				withInstancePriceOverride(6.801473).
				withPreemptibleInstancePriceOverride(1.425531),
			NewMachineTypeInfo("c3d-standard-60-6TB", 60, 240).
				withAutomaticEphemeralLocalSsdCount(16).
				withInstancePriceOverride(3.369101).
				withPreemptibleInstancePriceOverride(0.706855),
			NewMachineTypeInfo("c3d-standard-180-lssd", 180, 720).
				withAutomaticEphemeralLocalSsdCount(16).
				withInstancePriceOverride(8.816981).
				withPreemptibleInstancePriceOverride(1.736695),
			NewMachineTypeInfo("c3d-standard-360-lssd", 360, 1440).
				withAutomaticEphemeralLocalSsdCount(32).
				withInstancePriceOverride(17.633963).
				withPreemptibleInstancePriceOverride(3.473391),
			NewMachineTypeInfo("c3d-highmem-8-lssd", 8, 64).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(0.530203).
				withPreemptibleInstancePriceOverride(0.104588),
			NewMachineTypeInfo("c3d-highmem-16-lssd", 16, 128).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(1.020083).
				withPreemptibleInstancePriceOverride(0.197180),
			NewMachineTypeInfo("c3d-highmem-30-lssd", 30, 240).
				withAutomaticEphemeralLocalSsdCount(2).
				withInstancePriceOverride(1.917695).
				withPreemptibleInstancePriceOverride(0.371212),
			NewMachineTypeInfo("c3d-highmem-60-lssd", 60, 480).
				withAutomaticEphemeralLocalSsdCount(4).
				withInstancePriceOverride(3.835390).
				withPreemptibleInstancePriceOverride(0.742424),
			NewMachineTypeInfo("c3d-highmem-90-lssd", 90, 720).
				withAutomaticEphemeralLocalSsdCount(8).
				withInstancePriceOverride(5.833731).
				withPreemptibleInstancePriceOverride(1.137628),
			NewMachineTypeInfo("c3d-highmem-180-lssd", 180, 1440).
				withAutomaticEphemeralLocalSsdCount(16).
				withInstancePriceOverride(11.667461).
				withPreemptibleInstancePriceOverride(2.275255),
			NewMachineTypeInfo("c3d-highmem-360-lssd", 360, 2880).
				withAutomaticEphemeralLocalSsdCount(32).
				withInstancePriceOverride(23.334923).
				withPreemptibleInstancePriceOverride(4.550511),
		),
		supportedCpuPlatforms:    CpuPlatformRequirements{lowerBound: AmdGenoa, upperBound: AmdGenoa},
		supportCompactPlacement:  true,
		maxCompactPlacementNodes: int64(150),
		supportedBootDiskTypes: map[string]bool{
			DiskTypeBalanced:          true,
			DiskTypeSSD:               true,
			DiskTypeHyperdiskBalanced: true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced:                 NonConfidentialOnlyMode,
			DiskTypeHyperdiskExtreme:                  NonConfidentialOnlyMode,
			DiskTypeHyperdiskMl:                       NonConfidentialOnlyMode,
			DiskTypeHyperdiskThroughput:               NonConfidentialOnlyMode,
			DiskTypeBalanced:                          UnspecifiedMode,
			DiskTypeSSD:                               UnspecifiedMode,
			DiskTypeHyperdiskBalancedHighAvailability: NonConfidentialOnlyMode,
		},
		supportConfidentialNodes:     true,
		supportConfidentialNodeTypes: map[string]bool{labels.SEVConfidentialNodeTypeValue: true},
		defaultDiskType:              DiskTypeBalanced,
		supportHugepageSize1g:        true,
	})

	// C4 represents c4 machine family
	C4 = RegisterMachineFamily(MachineFamily{
		name:               "c4",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.03465,
			MemoryPricePerHourPerGb: 0.003938,
			PreemptibleDiscount:     0.01152 / 0.03465,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("c4-highcpu-2", 2, 4),
			NewMachineTypeInfo("c4-highcpu-4", 4, 8),
			NewMachineTypeInfo("c4-highcpu-8", 8, 16),
			NewMachineTypeInfo("c4-highcpu-16", 16, 32),
			NewMachineTypeInfo("c4-highcpu-32", 32, 64),
			NewMachineTypeInfo("c4-highcpu-48", 48, 96),
			NewMachineTypeInfo("c4-highcpu-64", 64, 128),
			NewMachineTypeInfo("c4-highcpu-96", 96, 192),
			NewMachineTypeInfo("c4-highcpu-192", 192, 384),
			NewMachineTypeInfo("c4-highmem-2", 2, 15),
			NewMachineTypeInfo("c4-highmem-4", 4, 31),
			NewMachineTypeInfo("c4-highmem-8", 8, 62),
			NewMachineTypeInfo("c4-highmem-16", 16, 124),
			NewMachineTypeInfo("c4-highmem-32", 32, 248),
			NewMachineTypeInfo("c4-highmem-48", 48, 372),
			NewMachineTypeInfo("c4-highmem-64", 64, 496),
			NewMachineTypeInfo("c4-highmem-96", 96, 744),
			NewMachineTypeInfo("c4-highmem-192", 192, 1488),
			NewMachineTypeInfo("c4-standard-2", 2, 7).
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}),
			NewMachineTypeInfo("c4-standard-4", 4, 15).
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}),
			NewMachineTypeInfo("c4-standard-8", 8, 30).
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}),
			NewMachineTypeInfo("c4-standard-16", 16, 60).
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}),
			NewMachineTypeInfo("c4-standard-32", 32, 120).
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}),
			NewMachineTypeInfo("c4-standard-48", 48, 180).
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}),
			NewMachineTypeInfo("c4-standard-64", 64, 240).
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}),
			NewMachineTypeInfo("c4-standard-96", 96, 360).
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}),
			NewMachineTypeInfo("c4-standard-192", 192, 720).
				withSupportedConfidentialNodeTypes([]string{labels.TDXConfidentialNodeTypeValue}),
			NewMachineTypeInfo("c4-highmem-4-lssd-v1", 4, 31).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(0.301001).
				withPreemptibleInstancePriceOverride(0.098302),
			NewMachineTypeInfo("c4-highmem-8-lssd-v1", 8, 62).
				withAutomaticEphemeralLocalSsdCount(2).
				withInstancePriceOverride(0.602001).
				withPreemptibleInstancePriceOverride(0.196604),
			NewMachineTypeInfo("c4-highmem-16-lssd-v1", 16, 124).
				withAutomaticEphemeralLocalSsdCount(4).
				withInstancePriceOverride(1.204002).
				withPreemptibleInstancePriceOverride(0.393209),
			NewMachineTypeInfo("c4-highmem-32-lssd-v1", 32, 248).
				withAutomaticEphemeralLocalSsdCount(6).
				withInstancePriceOverride(2.327359).
				withPreemptibleInstancePriceOverride(0.763131),
			NewMachineTypeInfo("c4-highmem-48-lssd-v1", 48, 372).
				withAutomaticEphemeralLocalSsdCount(8).
				withInstancePriceOverride(3.450717).
				withPreemptibleInstancePriceOverride(1.133053),
			NewMachineTypeInfo("c4-highmem-64-lssd-v1", 64, 496).
				withAutomaticEphemeralLocalSsdCount(8).
				withInstancePriceOverride(4.493429).
				withPreemptibleInstancePriceOverride(1.479689),
			NewMachineTypeInfo("c4-highmem-96-lssd-v1", 96, 744).
				withAutomaticEphemeralLocalSsdCount(16).
				withInstancePriceOverride(6.901433).
				withPreemptibleInstancePriceOverride(2.266106),
			NewMachineTypeInfo("c4-highmem-192-lssd-v1", 192, 1488).
				withAutomaticEphemeralLocalSsdCount(32).
				withInstancePriceOverride(13.802867).
				withPreemptibleInstancePriceOverride(4.532213),
			NewMachineTypeInfo("c4-standard-4-lssd-v1", 4, 15).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(0.237993).
				withPreemptibleInstancePriceOverride(0.077358),
			NewMachineTypeInfo("c4-standard-8-lssd-v1", 8, 30).
				withAutomaticEphemeralLocalSsdCount(2).
				withInstancePriceOverride(0.475985).
				withPreemptibleInstancePriceOverride(0.154716),
			NewMachineTypeInfo("c4-standard-16-lssd-v1", 16, 60).
				withAutomaticEphemeralLocalSsdCount(4).
				withInstancePriceOverride(0.951970).
				withPreemptibleInstancePriceOverride(0.309433),
			NewMachineTypeInfo("c4-standard-32-lssd-v1", 32, 120).
				withAutomaticEphemeralLocalSsdCount(6).
				withInstancePriceOverride(1.823295).
				withPreemptibleInstancePriceOverride(0.595579),
			NewMachineTypeInfo("c4-standard-48-lssd-v1", 48, 180).
				withAutomaticEphemeralLocalSsdCount(8).
				withInstancePriceOverride(2.694621).
				withPreemptibleInstancePriceOverride(0.881725),
			NewMachineTypeInfo("c4-standard-64-lssd-v1", 64, 240).
				withAutomaticEphemeralLocalSsdCount(8).
				withInstancePriceOverride(3.485301).
				withPreemptibleInstancePriceOverride(1.144585),
			NewMachineTypeInfo("c4-standard-96-lssd-v1", 96, 360).
				withAutomaticEphemeralLocalSsdCount(16).
				withInstancePriceOverride(5.389241).
				withPreemptibleInstancePriceOverride(1.763450),
			NewMachineTypeInfo("c4-standard-192-lssd-v1", 192, 720).
				withAutomaticEphemeralLocalSsdCount(32).
				withInstancePriceOverride(10.778483).
				withPreemptibleInstancePriceOverride(3.526901),
			NewMachineTypeInfo("c4-highcpu-24", 24, 48),
			NewMachineTypeInfo("c4-highcpu-144", 144, 288).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)),
			NewMachineTypeInfo("c4-highcpu-288", 288, 576).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)),
			NewMachineTypeInfo("c4-standard-24", 24, 90),
			NewMachineTypeInfo("c4-standard-144", 144, 540).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)),
			NewMachineTypeInfo("c4-standard-288", 288, 1080).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)),
			NewMachineTypeInfo("c4-highmem-24", 24, 186),
			NewMachineTypeInfo("c4-highmem-144", 144, 1116).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)),
			NewMachineTypeInfo("c4-highmem-288", 288, 2232).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)),
			NewMachineTypeInfo("c4-standard-4-lssd", 4, 15).
				withAutomaticEphemeralLocalSsdCount(1).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(0.237993).
				withPreemptibleInstancePriceOverride(0.077358),
			NewMachineTypeInfo("c4-standard-8-lssd", 8, 30).
				withAutomaticEphemeralLocalSsdCount(1).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(0.435663).
				withPreemptibleInstancePriceOverride(0.143073),
			NewMachineTypeInfo("c4-standard-16-lssd", 16, 60).
				withAutomaticEphemeralLocalSsdCount(2).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(0.871325).
				withPreemptibleInstancePriceOverride(0.286146),
			NewMachineTypeInfo("c4-standard-24-lssd", 24, 90).
				withAutomaticEphemeralLocalSsdCount(4).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(1.347310).
				withPreemptibleInstancePriceOverride(0.440863),
			NewMachineTypeInfo("c4-standard-32-lssd", 32, 120).
				withAutomaticEphemeralLocalSsdCount(5).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(1.782973).
				withPreemptibleInstancePriceOverride(0.583936),
			NewMachineTypeInfo("c4-standard-48-lssd", 48, 180).
				withAutomaticEphemeralLocalSsdCount(8).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(2.694621).
				withPreemptibleInstancePriceOverride(0.881725),
			NewMachineTypeInfo("c4-standard-64-lssd", 64, 240).
				withAutomaticEphemeralLocalSsdCount(10).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(3.565946).
				withPreemptibleInstancePriceOverride(1.167871),
			NewMachineTypeInfo("c4-standard-96-lssd", 96, 360).
				withAutomaticEphemeralLocalSsdCount(16).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(5.389241).
				withPreemptibleInstancePriceOverride(1.763450),
			NewMachineTypeInfo("c4-standard-144-lssd", 144, 540).
				withAutomaticEphemeralLocalSsdCount(24).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(8.083862).
				withPreemptibleInstancePriceOverride(2.645175),
			NewMachineTypeInfo("c4-standard-192-lssd", 192, 720).
				withAutomaticEphemeralLocalSsdCount(32).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(10.778483).
				withPreemptibleInstancePriceOverride(3.526901),
			NewMachineTypeInfo("c4-standard-288-lssd", 288, 1080).
				withAutomaticEphemeralLocalSsdCount(48).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(16.167724).
				withPreemptibleInstancePriceOverride(5.290351),
			NewMachineTypeInfo("c4-highmem-4-lssd", 4, 31).
				withAutomaticEphemeralLocalSsdCount(1).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(0.301001).
				withPreemptibleInstancePriceOverride(0.098302),
			NewMachineTypeInfo("c4-highmem-8-lssd", 8, 62).
				withAutomaticEphemeralLocalSsdCount(1).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(0.561679).
				withPreemptibleInstancePriceOverride(0.184961),
			NewMachineTypeInfo("c4-highmem-16-lssd", 16, 124).
				withAutomaticEphemeralLocalSsdCount(2).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(1.123357).
				withPreemptibleInstancePriceOverride(0.369922),
			NewMachineTypeInfo("c4-highmem-24-lssd", 24, 186).
				withAutomaticEphemeralLocalSsdCount(4).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(1.725358).
				withPreemptibleInstancePriceOverride(0.566527),
			NewMachineTypeInfo("c4-highmem-32-lssd", 32, 248).
				withAutomaticEphemeralLocalSsdCount(5).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(2.287037).
				withPreemptibleInstancePriceOverride(0.751488),
			NewMachineTypeInfo("c4-highmem-48-lssd", 48, 372).
				withAutomaticEphemeralLocalSsdCount(8).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(3.450717).
				withPreemptibleInstancePriceOverride(1.133053),
			NewMachineTypeInfo("c4-highmem-64-lssd", 64, 496).
				withAutomaticEphemeralLocalSsdCount(10).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(4.574074).
				withPreemptibleInstancePriceOverride(1.502975),
			NewMachineTypeInfo("c4-highmem-96-lssd", 96, 744).
				withAutomaticEphemeralLocalSsdCount(16).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(6.901433).
				withPreemptibleInstancePriceOverride(2.266106),
			NewMachineTypeInfo("c4-highmem-144-lssd", 144, 1116).
				withAutomaticEphemeralLocalSsdCount(24).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(10.352150).
				withPreemptibleInstancePriceOverride(3.399159),
			NewMachineTypeInfo("c4-highmem-192-lssd", 192, 1488).
				withAutomaticEphemeralLocalSsdCount(32).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(13.802867).
				withPreemptibleInstancePriceOverride(4.532213),
			NewMachineTypeInfo("c4-highmem-288-lssd", 288, 2232).
				withAutomaticEphemeralLocalSsdCount(48).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(20.704300).
				withPreemptibleInstancePriceOverride(6.798319),
			// C4 bare metal machine types
			NewMachineTypeInfo("c4-highcpu-288-metal", 288, 576).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withExplicitReqOnly(),
			NewMachineTypeInfo("c4-standard-288-metal", 288, 1080).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withExplicitReqOnly(),
			NewMachineTypeInfo("c4-highmem-288-metal", 288, 2232).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withExplicitReqOnly(),
			NewMachineTypeInfo("c4-standard-288-lssd-metal", 288, 1080).
				withAutomaticEphemeralLocalSsdCount(6).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(18.103208).
				withPreemptibleInstancePriceOverride(9.046665).
				withExplicitReqOnly(),
			NewMachineTypeInfo("c4-highmem-288-lssd-metal", 288, 2232).
				withAutomaticEphemeralLocalSsdCount(6).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelGraniteRapids, IntelGraniteRapids)).
				withInstancePriceOverride(22.639784).
				withPreemptibleInstancePriceOverride(11.313801).
				withExplicitReqOnly(),
		),
		supportedCpuPlatforms:    CpuPlatformRequirements{lowerBound: IntelEmeraldRapids, upperBound: IntelGraniteRapids},
		supportCompactPlacement:  true,
		maxCompactPlacementNodes: int64(150),
		supportedBootDiskTypes: map[string]bool{
			DiskTypeHyperdiskBalanced: true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced:                 NonConfidentialOnlyMode,
			DiskTypeHyperdiskBalancedHighAvailability: NonConfidentialOnlyMode,
			DiskTypeHyperdiskExtreme:                  NonConfidentialOnlyMode,
			DiskTypeHyperdiskThroughput:               NonConfidentialOnlyMode,
			DiskTypeHyperdiskMl:                       UnspecifiedMode,
		},
		supportHugepageSize1g: true,
		defaultDiskType:       DiskTypeHyperdiskBalanced,
	})
	// C4A represents c4a machine family
	C4A = RegisterMachineFamily(MachineFamily{
		name:               "c4a",
		systemArchitecture: gce.Arm64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.03086,
			MemoryPricePerHourPerGb: 0.00351,
			PreemptibleDiscount:     0.01234 / 0.03086,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("c4a-standard-1", 1, 4),
			NewMachineTypeInfo("c4a-standard-2", 2, 8),
			NewMachineTypeInfo("c4a-standard-4", 4, 16),
			NewMachineTypeInfo("c4a-standard-8", 8, 32),
			NewMachineTypeInfo("c4a-standard-16", 16, 64),
			NewMachineTypeInfo("c4a-standard-32", 32, 128),
			NewMachineTypeInfo("c4a-standard-36", 36, 144).
				withExplicitReqOnly(),
			NewMachineTypeInfo("c4a-standard-48", 48, 192),
			NewMachineTypeInfo("c4a-standard-64", 64, 256),
			NewMachineTypeInfo("c4a-standard-72", 72, 288),
			NewMachineTypeInfo("c4a-highcpu-1", 1, 2),
			NewMachineTypeInfo("c4a-highcpu-2", 2, 4),
			NewMachineTypeInfo("c4a-highcpu-4", 4, 8),
			NewMachineTypeInfo("c4a-highcpu-8", 8, 16),
			NewMachineTypeInfo("c4a-highcpu-16", 16, 32),
			NewMachineTypeInfo("c4a-highcpu-32", 32, 64),
			NewMachineTypeInfo("c4a-highcpu-36", 36, 72).
				withExplicitReqOnly(),
			NewMachineTypeInfo("c4a-highcpu-48", 48, 96),
			NewMachineTypeInfo("c4a-highcpu-64", 64, 128),
			NewMachineTypeInfo("c4a-highcpu-72", 72, 144),
			NewMachineTypeInfo("c4a-highmem-1", 1, 8),
			NewMachineTypeInfo("c4a-highmem-2", 2, 16),
			NewMachineTypeInfo("c4a-highmem-4", 4, 32),
			NewMachineTypeInfo("c4a-highmem-8", 8, 64),
			NewMachineTypeInfo("c4a-highmem-16", 16, 128),
			NewMachineTypeInfo("c4a-highmem-32", 32, 256),
			NewMachineTypeInfo("c4a-highmem-36", 36, 288).
				withExplicitReqOnly(),
			NewMachineTypeInfo("c4a-highmem-48", 48, 384),
			NewMachineTypeInfo("c4a-highmem-64", 64, 512),
			NewMachineTypeInfo("c4a-highmem-72", 72, 576),
			NewMachineTypeInfo("c4a-standard-4-lssd", 4, 16).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(0.2421).
				withPreemptibleInstancePriceOverride(0.09676),
			NewMachineTypeInfo("c4a-standard-8-lssd", 8, 32).
				withAutomaticEphemeralLocalSsdCount(2).
				withInstancePriceOverride(0.4842).
				withPreemptibleInstancePriceOverride(0.19352),
			NewMachineTypeInfo("c4a-standard-16-lssd", 16, 64).
				withAutomaticEphemeralLocalSsdCount(4).
				withInstancePriceOverride(0.9684).
				withPreemptibleInstancePriceOverride(0.38704),
			NewMachineTypeInfo("c4a-standard-32-lssd", 32, 128).
				withAutomaticEphemeralLocalSsdCount(6).
				withInstancePriceOverride(1.8118).
				withPreemptibleInstancePriceOverride(0.72408),
			NewMachineTypeInfo("c4a-standard-48-lssd", 48, 192).
				withAutomaticEphemeralLocalSsdCount(10).
				withInstancePriceOverride(2.7802).
				withPreemptibleInstancePriceOverride(1.11112),
			NewMachineTypeInfo("c4a-standard-64-lssd", 64, 256).
				withAutomaticEphemeralLocalSsdCount(14).
				withInstancePriceOverride(3.7486).
				withPreemptibleInstancePriceOverride(1.49816),
			NewMachineTypeInfo("c4a-standard-72-lssd", 72, 288).
				withAutomaticEphemeralLocalSsdCount(16).
				withInstancePriceOverride(4.2328).
				withPreemptibleInstancePriceOverride(1.69168),
			NewMachineTypeInfo("c4a-highcpu-4-lssd", 4, 8).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(0.21402).
				withPreemptibleInstancePriceOverride(0.08556),
			NewMachineTypeInfo("c4a-highcpu-8-lssd", 8, 16).
				withAutomaticEphemeralLocalSsdCount(2).
				withInstancePriceOverride(0.42804).
				withPreemptibleInstancePriceOverride(0.17112),
			NewMachineTypeInfo("c4a-highcpu-16-lssd", 16, 32).
				withAutomaticEphemeralLocalSsdCount(4).
				withInstancePriceOverride(0.85608).
				withPreemptibleInstancePriceOverride(0.34224),
			NewMachineTypeInfo("c4a-highcpu-32-lssd", 32, 64).
				withAutomaticEphemeralLocalSsdCount(6).
				withInstancePriceOverride(1.58716).
				withPreemptibleInstancePriceOverride(0.63448),
			NewMachineTypeInfo("c4a-highcpu-48-lssd", 48, 96).
				withAutomaticEphemeralLocalSsdCount(10).
				withInstancePriceOverride(2.44324).
				withPreemptibleInstancePriceOverride(0.97672),
			NewMachineTypeInfo("c4a-highcpu-64-lssd", 64, 128).
				withAutomaticEphemeralLocalSsdCount(14).
				withInstancePriceOverride(3.29932).
				withPreemptibleInstancePriceOverride(1.31896),
			NewMachineTypeInfo("c4a-highcpu-72-lssd", 72, 144).
				withAutomaticEphemeralLocalSsdCount(16).
				withInstancePriceOverride(3.72736).
				withPreemptibleInstancePriceOverride(1.49008),
			NewMachineTypeInfo("c4a-highmem-4-lssd", 4, 32).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(0.25).
				withPreemptibleInstancePriceOverride(0.11916),
			NewMachineTypeInfo("c4a-highmem-8-lssd", 8, 64).
				withAutomaticEphemeralLocalSsdCount(2).
				withInstancePriceOverride(0.59652).
				withPreemptibleInstancePriceOverride(0.23832),
			NewMachineTypeInfo("c4a-highmem-16-lssd", 16, 128).
				withAutomaticEphemeralLocalSsdCount(4).
				withInstancePriceOverride(1.19304).
				withPreemptibleInstancePriceOverride(0.47664),
			NewMachineTypeInfo("c4a-highmem-32-lssd", 32, 256).
				withAutomaticEphemeralLocalSsdCount(6).
				withInstancePriceOverride(2.26108).
				withPreemptibleInstancePriceOverride(0.90328),
			NewMachineTypeInfo("c4a-highmem-48-lssd", 48, 384).
				withAutomaticEphemeralLocalSsdCount(10).
				withInstancePriceOverride(3.45412).
				withPreemptibleInstancePriceOverride(1.37992),
			NewMachineTypeInfo("c4a-highmem-64-lssd", 64, 512).
				withAutomaticEphemeralLocalSsdCount(14).
				withInstancePriceOverride(4.64716).
				withPreemptibleInstancePriceOverride(1.85656),
			NewMachineTypeInfo("c4a-highmem-72-lssd", 72, 576).
				withAutomaticEphemeralLocalSsdCount(16).
				withInstancePriceOverride(5.24368).
				withPreemptibleInstancePriceOverride(2.09488),
			// Users must explicitly request metal shape to use it.
			NewMachineTypeInfo("c4a-standard-96-metal", 96, 384).
				withExplicitReqOnly(),
			NewMachineTypeInfo("c4a-highmem-96-metal", 96, 768).
				withExplicitReqOnly(),
		),
		supportedCpuPlatforms: CpuPlatformRequirements{lowerBound: GoogleAxion, upperBound: GoogleAxion},
		// C4A doesn't support compact placement yet: b/331835782
		supportCompactPlacement:  false,
		nonDefaultThreadsPerCore: pInt64(1),
		supportedBootDiskTypes: map[string]bool{
			DiskTypeHyperdiskBalanced: true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced:                 NonConfidentialOnlyMode,
			DiskTypeHyperdiskExtreme:                  NonConfidentialOnlyMode,
			DiskTypeHyperdiskBalancedHighAvailability: NonConfidentialOnlyMode,
			DiskTypeHyperdiskThroughput:               NonConfidentialOnlyMode,
			DiskTypeHyperdiskMl:                       UnspecifiedMode,
		},
		defaultDiskType: DiskTypeHyperdiskBalanced,
	})
	// C4D represents c4d machine family
	C4D = RegisterMachineFamily(MachineFamily{
		name:               "c4d",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.0327035,
			MemoryPricePerHourPerGb: 0.0037187,
			PreemptibleDiscount:     0.0130312 / 0.0327035,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("c4d-highcpu-2", 2, 3),
			NewMachineTypeInfo("c4d-highcpu-4", 4, 7),
			NewMachineTypeInfo("c4d-highcpu-8", 8, 15),
			NewMachineTypeInfo("c4d-highcpu-16", 16, 30),
			NewMachineTypeInfo("c4d-highcpu-32", 32, 60),
			NewMachineTypeInfo("c4d-highcpu-48", 48, 90),
			NewMachineTypeInfo("c4d-highcpu-64", 64, 120),
			NewMachineTypeInfo("c4d-highcpu-96", 96, 180),
			NewMachineTypeInfo("c4d-highcpu-192", 192, 360),
			NewMachineTypeInfo("c4d-highcpu-384", 384, 720),
			NewMachineTypeInfo("c4d-standard-2", 2, 7),
			NewMachineTypeInfo("c4d-standard-4", 4, 15),
			NewMachineTypeInfo("c4d-standard-8", 8, 31),
			NewMachineTypeInfo("c4d-standard-16", 16, 62),
			NewMachineTypeInfo("c4d-standard-32", 32, 124),
			NewMachineTypeInfo("c4d-standard-48", 48, 186),
			NewMachineTypeInfo("c4d-standard-64", 64, 248),
			NewMachineTypeInfo("c4d-standard-96", 96, 372),
			NewMachineTypeInfo("c4d-standard-192", 192, 744),
			NewMachineTypeInfo("c4d-standard-384", 384, 1488),
			NewMachineTypeInfo("c4d-standard-8-lssd", 8, 31).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(0.377654).
				withPreemptibleInstancePriceOverride(0.150901),
			NewMachineTypeInfo("c4d-standard-16-lssd", 16, 62).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(0.754562).
				withPreemptibleInstancePriceOverride(0.301055),
			NewMachineTypeInfo("c4d-standard-32-lssd", 32, 124).
				withAutomaticEphemeralLocalSsdCount(2).
				withInstancePriceOverride(1.509124).
				withPreemptibleInstancePriceOverride(0.602110),
			NewMachineTypeInfo("c4d-standard-48-lssd", 48, 186).
				withAutomaticEphemeralLocalSsdCount(4).
				withInstancePriceOverride(2.264432).
				withPreemptibleInstancePriceOverride(0.903912),
			NewMachineTypeInfo("c4d-standard-64-lssd", 64, 248).
				withAutomaticEphemeralLocalSsdCount(6).
				withInstancePriceOverride(3.019740).
				withPreemptibleInstancePriceOverride(1.205713),
			NewMachineTypeInfo("c4d-standard-96-lssd", 96, 372).
				withAutomaticEphemeralLocalSsdCount(8).
				withInstancePriceOverride(4.528863).
				withPreemptibleInstancePriceOverride(1.807824),
			NewMachineTypeInfo("c4d-standard-192-lssd", 192, 744).
				withAutomaticEphemeralLocalSsdCount(16).
				withInstancePriceOverride(9.057727).
				withPreemptibleInstancePriceOverride(3.615648),
			NewMachineTypeInfo("c4d-standard-384-lssd", 384, 1488).
				withAutomaticEphemeralLocalSsdCount(32).
				withInstancePriceOverride(18.115453).
				withPreemptibleInstancePriceOverride(7.231295),
			NewMachineTypeInfo("c4d-highmem-2", 2, 15),
			NewMachineTypeInfo("c4d-highmem-4", 4, 31),
			NewMachineTypeInfo("c4d-highmem-8", 8, 63),
			NewMachineTypeInfo("c4d-highmem-16", 16, 126),
			NewMachineTypeInfo("c4d-highmem-32", 32, 252),
			NewMachineTypeInfo("c4d-highmem-48", 48, 378),
			NewMachineTypeInfo("c4d-highmem-64", 64, 504),
			NewMachineTypeInfo("c4d-highmem-96", 96, 756),
			NewMachineTypeInfo("c4d-highmem-192", 192, 1512),
			NewMachineTypeInfo("c4d-highmem-384", 384, 3024),
			NewMachineTypeInfo("c4d-highmem-8-lssd", 8, 63).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(0.496652).
				withPreemptibleInstancePriceOverride(0.198286),
			NewMachineTypeInfo("c4d-highmem-16-lssd", 16, 126).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(0.992559).
				withPreemptibleInstancePriceOverride(0.395826),
			NewMachineTypeInfo("c4d-highmem-32-lssd", 32, 252).
				withAutomaticEphemeralLocalSsdCount(2).
				withInstancePriceOverride(1.985117).
				withPreemptibleInstancePriceOverride(0.791653),
			NewMachineTypeInfo("c4d-highmem-48-lssd", 48, 378).
				withAutomaticEphemeralLocalSsdCount(4).
				withInstancePriceOverride(2.978422).
				withPreemptibleInstancePriceOverride(1.188225),
			NewMachineTypeInfo("c4d-highmem-64-lssd", 64, 504).
				withAutomaticEphemeralLocalSsdCount(6).
				withInstancePriceOverride(3.971727).
				withPreemptibleInstancePriceOverride(1.584798),
			NewMachineTypeInfo("c4d-highmem-96-lssd", 96, 756).
				withAutomaticEphemeralLocalSsdCount(8).
				withInstancePriceOverride(5.956844).
				withPreemptibleInstancePriceOverride(2.376451),
			NewMachineTypeInfo("c4d-highmem-192-lssd", 192, 1512).
				withAutomaticEphemeralLocalSsdCount(16).
				withInstancePriceOverride(11.913688).
				withPreemptibleInstancePriceOverride(4.752902),
			NewMachineTypeInfo("c4d-highmem-384-lssd", 384, 3024).
				withAutomaticEphemeralLocalSsdCount(32).
				withInstancePriceOverride(23.827377).
				withPreemptibleInstancePriceOverride(9.505804),
		),
		supportedCpuPlatforms:        CpuPlatformRequirements{lowerBound: AmdTurin, upperBound: AmdTurin},
		supportCompactPlacement:      true,
		maxCompactPlacementNodes:     int64(150),
		supportConfidentialNodes:     true,
		supportConfidentialNodeTypes: map[string]bool{labels.SEVConfidentialNodeTypeValue: true},
		supportedBootDiskTypes: map[string]bool{
			DiskTypeHyperdiskBalanced: true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced:                 NonConfidentialOnlyMode,
			DiskTypeHyperdiskExtreme:                  NonConfidentialOnlyMode,
			DiskTypeHyperdiskBalancedHighAvailability: NonConfidentialOnlyMode,
			DiskTypeHyperdiskMl:                       UnspecifiedMode,
		},
		defaultDiskType:       DiskTypeHyperdiskBalanced,
		supportHugepageSize1g: true,
	})
	// C4N represents c4n machine family
	C4N = RegisterMachineFamily(MachineFamily{
		name:               "c4n",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.051975,
			MemoryPricePerHourPerGb: 0.005907,
			PreemptibleDiscount:     0.02079 / 0.051975,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("c4n-highcpu-2", 2, 4),
			NewMachineTypeInfo("c4n-highcpu-4", 4, 8),
			NewMachineTypeInfo("c4n-highcpu-8", 8, 16),
			NewMachineTypeInfo("c4n-highcpu-16", 16, 32),
			NewMachineTypeInfo("c4n-highcpu-24", 24, 48),
			NewMachineTypeInfo("c4n-highcpu-32", 32, 64),
			NewMachineTypeInfo("c4n-highcpu-48", 48, 96),
			NewMachineTypeInfo("c4n-highcpu-96", 96, 192),
			NewMachineTypeInfo("c4n-highcpu-192", 192, 384),
			NewMachineTypeInfo("c4n-standard-2", 2, 7),
			NewMachineTypeInfo("c4n-standard-4", 4, 15),
			NewMachineTypeInfo("c4n-standard-8", 8, 30),
			NewMachineTypeInfo("c4n-standard-16", 16, 60),
			NewMachineTypeInfo("c4n-standard-24", 24, 90),
			NewMachineTypeInfo("c4n-standard-32", 32, 120),
			NewMachineTypeInfo("c4n-standard-48", 48, 180),
			NewMachineTypeInfo("c4n-standard-96", 96, 360),
			NewMachineTypeInfo("c4n-standard-192", 192, 720),
			NewMachineTypeInfo("c4n-highmem-2", 2, 15),
			NewMachineTypeInfo("c4n-highmem-4", 4, 31),
			NewMachineTypeInfo("c4n-highmem-8", 8, 62),
			NewMachineTypeInfo("c4n-highmem-16", 16, 124),
			NewMachineTypeInfo("c4n-highmem-24", 24, 186),
			NewMachineTypeInfo("c4n-highmem-32", 32, 248),
			NewMachineTypeInfo("c4n-highmem-48", 48, 372),
			NewMachineTypeInfo("c4n-highmem-96", 96, 744),
			NewMachineTypeInfo("c4n-highmem-192", 192, 1488),
			NewMachineTypeInfo("c4n-standard-4-lssd", 4, 15).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(0.417473).
				withPreemptibleInstancePriceOverride(0.166989),
			NewMachineTypeInfo("c4n-standard-8-lssd", 8, 30).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(0.713978).
				withPreemptibleInstancePriceOverride(0.285591),
			NewMachineTypeInfo("c4n-standard-16-lssd", 16, 60).
				withAutomaticEphemeralLocalSsdCount(2).
				withInstancePriceOverride(1.427955).
				withPreemptibleInstancePriceOverride(0.571182),
			NewMachineTypeInfo("c4n-standard-24-lssd", 24, 90).
				withAutomaticEphemeralLocalSsdCount(4).
				withInstancePriceOverride(2.262901).
				withPreemptibleInstancePriceOverride(0.905160),
			NewMachineTypeInfo("c4n-standard-32-lssd", 32, 120).
				withAutomaticEphemeralLocalSsdCount(5).
				withInstancePriceOverride(2.976879).
				withPreemptibleInstancePriceOverride(1.190751),
			NewMachineTypeInfo("c4n-standard-48-lssd", 48, 180).
				withAutomaticEphemeralLocalSsdCount(8).
				withInstancePriceOverride(4.525802).
				withPreemptibleInstancePriceOverride(1.810321),
			NewMachineTypeInfo("c4n-standard-96-lssd", 96, 360).
				withAutomaticEphemeralLocalSsdCount(16).
				withInstancePriceOverride(9.051604).
				withPreemptibleInstancePriceOverride(3.620642),
			NewMachineTypeInfo("c4n-standard-192-lssd", 192, 720).
				withAutomaticEphemeralLocalSsdCount(32).
				withInstancePriceOverride(18.103208).
				withPreemptibleInstancePriceOverride(7.241283),
			NewMachineTypeInfo("c4n-highmem-4-lssd", 4, 31).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(0.511985).
				withPreemptibleInstancePriceOverride(0.204794),
			NewMachineTypeInfo("c4n-highmem-8-lssd", 8, 62).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(0.903002).
				withPreemptibleInstancePriceOverride(0.361201),
			NewMachineTypeInfo("c4n-highmem-16-lssd", 16, 124).
				withAutomaticEphemeralLocalSsdCount(2).
				withInstancePriceOverride(1.806003).
				withPreemptibleInstancePriceOverride(0.722401),
			NewMachineTypeInfo("c4n-highmem-24-lssd", 24, 186).
				withAutomaticEphemeralLocalSsdCount(4).
				withInstancePriceOverride(2.829973).
				withPreemptibleInstancePriceOverride(1.131989),
			NewMachineTypeInfo("c4n-highmem-32-lssd", 32, 248).
				withAutomaticEphemeralLocalSsdCount(5).
				withInstancePriceOverride(3.732975).
				withPreemptibleInstancePriceOverride(1.493190),
			NewMachineTypeInfo("c4n-highmem-48-lssd", 48, 372).
				withAutomaticEphemeralLocalSsdCount(8).
				withInstancePriceOverride(5.659946).
				withPreemptibleInstancePriceOverride(2.263978),
			NewMachineTypeInfo("c4n-highmem-96-lssd", 96, 744).
				withAutomaticEphemeralLocalSsdCount(16).
				withInstancePriceOverride(11.319892).
				withPreemptibleInstancePriceOverride(4.527957),
			NewMachineTypeInfo("c4n-highmem-192-lssd", 192, 1488).
				withAutomaticEphemeralLocalSsdCount(32).
				withInstancePriceOverride(22.639784).
				withPreemptibleInstancePriceOverride(9.055913),
		),
		supportedCpuPlatforms:    CpuPlatformRequirements{lowerBound: IntelEmeraldRapids, upperBound: IntelEmeraldRapids},
		supportCompactPlacement:  true,
		maxCompactPlacementNodes: int64(150),
		supportConfidentialNodes: false,
		supportedBootDiskTypes: map[string]bool{
			DiskTypeHyperdiskBalanced:                 true,
			DiskTypeHyperdiskBalancedHighAvailability: true,
			DiskTypeHyperdiskThroughput:               true,
			DiskTypeHyperdiskMl:                       true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced:                 NonConfidentialOnlyMode,
			DiskTypeHyperdiskBalancedHighAvailability: NonConfidentialOnlyMode,
			DiskTypeHyperdiskExtreme:                  NonConfidentialOnlyMode,
			DiskTypeHyperdiskMl:                       NonConfidentialOnlyMode,
			DiskTypeHyperdiskThroughput:               NonConfidentialOnlyMode,
		},
		supportHugepageSize1g: true,
		defaultDiskType:       DiskTypeHyperdiskBalanced,
	})
	// TODO: add C3A, C3 metal
	// those machine families supports only disk-type: hyperdisk

	CT3 = RegisterMachineFamily(MachineFamily{
		name:               "ct3",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.05,
			MemoryPricePerHourPerGb: 0,
			PreemptibleDiscount:     0.6 / 1.2,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("ct3-hightpu-4t", 96, 340).withInstancePriceOverride(4.8),
		),
		supportedTpuTypes: map[string]bool{
			labels.TpuV3DeviceValue: true,
		},
		supportedCpuPlatforms: noPlatformSupported,
		supportedBootDiskTypes: map[string]bool{
			DiskTypeStandard: true,
			DiskTypeBalanced: true,
			DiskTypeSSD:      true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeBalanced: UnspecifiedMode,
			DiskTypeSSD:      UnspecifiedMode,
			DiskTypeStandard: UnspecifiedMode,
		},
		defaultDiskType: DiskTypeStandard,
		dwsDisabled:     true,
	})
	CT3P = RegisterMachineFamily(MachineFamily{
		name:               "ct3p",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.05,
			MemoryPricePerHourPerGb: 0,
			PreemptibleDiscount:     0.6 / 1.2,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("ct3p-hightpu-4t", 48, 340).withInstancePriceOverride(4.8),
		),
		supportedTpuTypes: map[string]bool{
			labels.TpuV3SliceValue: true,
		},
		supportedCpuPlatforms: noPlatformSupported,
		supportedBootDiskTypes: map[string]bool{
			DiskTypeStandard: true,
			DiskTypeBalanced: true,
			DiskTypeSSD:      true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeBalanced: UnspecifiedMode,
			DiskTypeSSD:      UnspecifiedMode,
			DiskTypeStandard: UnspecifiedMode,
		},
		defaultDiskType: DiskTypeStandard,
		dwsDisabled:     true,
	})

	// CT4L represents ct4l machine family (TPU Device)
	// TODO(b/517095663): If adding new TPU VMs, consider whether CA can always
	// unambiguously pick appropriate machine type for a pod.
	// If different machine types are available, consider waiting 30s
	// for more pods to appear after the first TPU pod is created
	// in order to make more informed scaling decision, similarly to GPU logic:
	// https://github.com/kubernetes/autoscaler/blob/0ae555cbee3abf423c7d56fa906477cae0575dbe/cluster-autoscaler/core/static_autoscaler.go#L67
	CT4L = RegisterMachineFamily(MachineFamily{
		name:               "ct4l",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.002841,
			MemoryPricePerHourPerGb: 0,
			PreemptibleDiscount:     3.88 / 12.88,
		},
		// TPU VMs are priced on per-TPU-chip basis: cloud.google.com/tpu/pricing
		// cpuPricePerHour is calculated as (Price of TPUs/Num. CPUs). In this case 12.88 / 48.
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("ct4l-hightpu-4t", 48, 340).
				withInstancePriceOverride(12.88),
		),
		supportedTpuTypes: map[string]bool{
			labels.TpuV4LiteDeviceValue: true,
		},
		supportedCpuPlatforms: noPlatformSupported,
		supportedBootDiskTypes: map[string]bool{
			DiskTypeStandard:          true,
			DiskTypeBalanced:          true,
			DiskTypeSSD:               true,
			DiskTypeHyperdiskBalanced: true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced: ConfidentialOnlyMode,
			DiskTypeBalanced:          UnspecifiedMode,
			DiskTypePDExtreme:         UnspecifiedMode,
			DiskTypeSSD:               UnspecifiedMode,
			DiskTypeStandard:          UnspecifiedMode,
		},
		defaultDiskType: DiskTypeStandard,
		dwsDisabled:     true,
	})
	// CT4P represents ct4p machine family (TPU PodSlice)
	CT4P = RegisterMachineFamily(MachineFamily{
		name:               "ct4p",
		systemArchitecture: gce.Amd64,
		// For TPU, cpu and memory are not billed, the price is only based on TPU chips.
		// CpuPricePerHour is calculated by dividing total VM price by the cpu count.
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.053666,
			MemoryPricePerHourPerGb: 0,
			PreemptibleDiscount:     3.88 / 12.88,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("ct4p-hightpu-4t", 240, 407).
				withInstancePriceOverride(12.88),
		),
		supportedTpuTypes: map[string]bool{
			labels.TpuV4PodsliceValue: true,
		},
		supportedCpuPlatforms: noPlatformSupported,
		supportedBootDiskTypes: map[string]bool{
			DiskTypeStandard:          true,
			DiskTypeBalanced:          true,
			DiskTypeSSD:               true,
			DiskTypeHyperdiskBalanced: true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced: ConfidentialOnlyMode,
			DiskTypeBalanced:          UnspecifiedMode,
			DiskTypePDExtreme:         UnspecifiedMode,
			DiskTypeSSD:               UnspecifiedMode,
			DiskTypeStandard:          UnspecifiedMode,
		},
		defaultDiskType: DiskTypeStandard,
		dwsDisabled:     true,
	})
	CT5L = RegisterMachineFamily(MachineFamily{
		name:               "ct5l",
		systemArchitecture: gce.Amd64,
		// For TPU, cpu and memory are not billed, the price is only based on TPU chips.
		// CpuPricePerHour is calculated by dividing total VM price by the cpu count
		// for ct5l-hightpu-1t, which is the most expensive one on per-cpu-basis.
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.05,
			MemoryPricePerHourPerGb: 0,
			PreemptibleDiscount:     0.6 / 1.2,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("ct5l-hightpu-8t", 224, 384).withInstancePriceOverride(9.6),
			NewMachineTypeInfo("ct5l-hightpu-4t", 112, 192).withInstancePriceOverride(4.8),
			NewMachineTypeInfo("ct5l-hightpu-1t", 24, 48).withInstancePriceOverride(1.2),
		),
		supportedTpuTypes: map[string]bool{
			labels.TpuV5LiteDeviceValue: true,
		},
		supportedCpuPlatforms: noPlatformSupported,
		supportedBootDiskTypes: map[string]bool{
			DiskTypeBalanced: true,
			DiskTypeSSD:      true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced: ConfidentialOnlyMode,
			DiskTypeHyperdiskMl:       UnspecifiedMode,
			DiskTypeBalanced:          UnspecifiedMode,
			DiskTypeSSD:               UnspecifiedMode,
		},
		defaultDiskType:       DiskTypeBalanced,
		supportHugepageSize1g: true,
		dwsDisabled:           true,
	})

	CT5LP = RegisterMachineFamily(MachineFamily{
		name:               "ct5lp",
		systemArchitecture: gce.Amd64,
		// For TPU, cpu and memory are not billed, the price is only based on TPU chips.
		// CpuPricePerHour is calculated by dividing total VM price by the cpu count
		// for ct5lp-hightpu-1t, which is the most expensive one on per-cpu-basis.
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.05,
			MemoryPricePerHourPerGb: 0,
			PreemptibleDiscount:     0.6 / 1.2,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("ct5lp-hightpu-8t", 224, 384).withInstancePriceOverride(9.6),
			NewMachineTypeInfo("ct5lp-hightpu-4t", 112, 192).withInstancePriceOverride(4.8),
			NewMachineTypeInfo("ct5lp-hightpu-1t", 24, 48).withInstancePriceOverride(1.2),
		),
		supportedTpuTypes: map[string]bool{
			labels.TpuV5LitePodsliceValue: true,
		},
		supportedCpuPlatforms: noPlatformSupported,
		supportedBootDiskTypes: map[string]bool{
			DiskTypeBalanced: true,
			DiskTypeSSD:      true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced: ConfidentialOnlyMode,
			DiskTypeHyperdiskMl:       NonConfidentialOnlyMode,
			DiskTypeBalanced:          UnspecifiedMode,
			DiskTypeSSD:               UnspecifiedMode,
		},
		defaultDiskType:       DiskTypeBalanced,
		supportHugepageSize1g: true,
	})

	// CT5P represents ct5p machine family (TPU PodSlice)
	CT5P = RegisterMachineFamily(MachineFamily{
		name:               "ct5p",
		systemArchitecture: gce.Amd64,
		// For TPU, cpu and memory are not billed, the price is only based on TPU chips.
		// CpuPricePerHour is calculated by dividing total VM price by the cpu count.
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.080769231769231,
			MemoryPricePerHourPerGb: 0,
			PreemptibleDiscount:     0.6 / 1.2,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("ct5p-hightpu-4t", 208, 448).
				withInstancePriceOverride(16.8),
		),
		supportedTpuTypes: map[string]bool{
			labels.TpuV5PSliceValue: true,
		},
		supportedCpuPlatforms: CpuPlatformRequirements{
			lowerBound: IntelSapphireRapids,
			upperBound: IntelSapphireRapids,
		},
		supportedBootDiskTypes: map[string]bool{
			DiskTypeBalanced: true,
			DiskTypeSSD:      true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced: ConfidentialOnlyMode,
			DiskTypeHyperdiskMl:       NonConfidentialOnlyMode,
			DiskTypeBalanced:          UnspecifiedMode,
			DiskTypeSSD:               UnspecifiedMode,
		},
		defaultDiskType: DiskTypeBalanced,
	})
	// CT6E represents ct6e machine family (TPU PodSlice)
	// For TPU it's a minor issue, as no decision regarding TPUs is based on pricing.
	CT6E = RegisterMachineFamily(MachineFamily{
		name:               "ct6e",
		systemArchitecture: gce.Amd64,
		// For TPU, cpu and memory are not billed, the price is only based on TPU chips.
		// CpuPricePerHour is calculated by dividing total VM price by the cpu count.
		// value is provided for the most expensive machine type, the ct6e-standard-8t
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.12,
			MemoryPricePerHourPerGb: 0,
			PreemptibleDiscount:     0.6 / 1.2,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("ct6e-standard-1t", 44, 176).
				withInstancePriceOverride(2.7),
			NewMachineTypeInfo("ct6e-standard-4t", 180, 720).
				withInstancePriceOverride(10.8),
			NewMachineTypeInfo("ct6e-standard-8t", 180, 1440).
				withInstancePriceOverride(21.6).
				withThreadsPerCoreOverride(1),
		),
		supportedTpuTypes: map[string]bool{
			labels.TpuV6ESliceValue: true,
		},
		supportedCpuPlatforms: CpuPlatformRequirements{
			lowerBound: IntelSapphireRapids,
			upperBound: IntelSapphireRapids,
		},
		supportedBootDiskTypes: map[string]bool{
			DiskTypeHyperdiskBalanced: true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced: NonConfidentialOnlyMode,
			DiskTypeHyperdiskMl:       NonConfidentialOnlyMode,
		},
		defaultDiskType:          DiskTypeHyperdiskBalanced,
		supportsAcceleratorSlice: true,
		supportHugepageSize1g:    true,
	})
	// E2 represents e2 machine family
	E2 = RegisterMachineFamily(MachineFamily{
		name:               "e2",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.021811,
			MemoryPricePerHourPerGb: 0.002923,
			PreemptibleDiscount:     0.006543 / 0.021811,
		},
		customPricingInfo: &MachineFamilyPricingInfo{
			CpuPricePerHour:         0.022890,
			MemoryPricePerHourPerGb: 0.003067,
			PreemptibleDiscount:     0.006867 / 0.022890,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			// E2 shared core machines show 2 cores in the machine-types API, but their allocatable and price are computed
			// based off of 1 core: https://cloud.google.com/kubernetes-engine/docs/release-notes#June_02_2020. Setting 1 core
			// for them in this package as it closer reflects how they compare to other types in the family.
			NewMachineTypeInfo("e2-medium", 1, 4).
				withInstancePriceOverride(0.03351),

			NewMachineTypeInfo("e2-standard-2", 2, 8),
			NewMachineTypeInfo("e2-standard-4", 4, 16),
			NewMachineTypeInfo("e2-standard-8", 8, 32),
			NewMachineTypeInfo("e2-standard-16", 16, 64),
			NewMachineTypeInfo("e2-standard-32", 32, 128),
			NewMachineTypeInfo("e2-highcpu-2", 2, 2),
			NewMachineTypeInfo("e2-highcpu-4", 4, 4),
			NewMachineTypeInfo("e2-highcpu-8", 8, 8),
			NewMachineTypeInfo("e2-highcpu-16", 16, 16),
			NewMachineTypeInfo("e2-highcpu-32", 32, 32),
			NewMachineTypeInfo("e2-highmem-2", 2, 16),
			NewMachineTypeInfo("e2-highmem-4", 4, 32),
			NewMachineTypeInfo("e2-highmem-8", 8, 64),
			NewMachineTypeInfo("e2-highmem-16", 16, 128),
		),
		otherMachineTypes: onboardMachineType(
			// See comment on e2-medium.
			NewMachineTypeInfo("e2-micro", 1, 1).
				withInstancePriceOverride(0.03353), // Should be 0.00838. Set to be > e2-medium.

			// See comment on e2-medium.
			NewMachineTypeInfo("e2-small", 1, 2).
				withInstancePriceOverride(0.03352), // Should be 0.01675. Set to be > e2-medium.
		),
		supportedCpuPlatforms: noPlatformSupported,
		supportedBootDiskTypes: map[string]bool{
			DiskTypeStandard: true,
			DiskTypeBalanced: true,
			DiskTypeSSD:      true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeBalanced:  UnspecifiedMode,
			DiskTypePDExtreme: UnspecifiedMode,
			DiskTypeSSD:       UnspecifiedMode,
			DiskTypeStandard:  UnspecifiedMode,
			DiskTypeHyperdiskBalancedHighAvailability: UnspecifiedMode,
			DiskTypeHyperdiskThroughput:               UnspecifiedMode,
		},
		defaultDiskType:          DiskTypeStandard,
		numaAlignmentUnsupported: true,
	})
	// E4A represents e4a machine family
	E4A = RegisterMachineFamily(MachineFamily{
		name:               "e4a",
		systemArchitecture: gce.Arm64, // E4A is an Arm-based VM family
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.001746,
			MemoryPricePerHourPerGb: 0.000298,
			PreemptibleDiscount:     0.5,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewResizableMachineTypeInfo("e4a-standard-2", 2, 8, &ResizableMachineTypeConfig{
				MinResources: apiv1.ResourceList{
					apiv1.ResourceCPU:    resource.MustParse("250m"),
					apiv1.ResourceMemory: resource.MustParse("2Gi"),
				},
			}),
			NewResizableMachineTypeInfo("e4a-standard-4", 4, 16, &ResizableMachineTypeConfig{
				MinResources: apiv1.ResourceList{
					apiv1.ResourceCPU:    resource.MustParse("250m"),
					apiv1.ResourceMemory: resource.MustParse("2Gi"),
				},
			}),
			NewResizableMachineTypeInfo("e4a-standard-8", 8, 32, &ResizableMachineTypeConfig{
				MinResources: apiv1.ResourceList{
					apiv1.ResourceCPU:    resource.MustParse("250m"),
					apiv1.ResourceMemory: resource.MustParse("2Gi"),
				},
			}),
			NewResizableMachineTypeInfo("e4a-standard-16", 16, 64, &ResizableMachineTypeConfig{
				MinResources: apiv1.ResourceList{
					apiv1.ResourceCPU:    resource.MustParse("250m"),
					apiv1.ResourceMemory: resource.MustParse("3Gi"),
				},
			}),
			NewResizableMachineTypeInfo("e4a-standard-32", 32, 128, &ResizableMachineTypeConfig{
				MinResources: apiv1.ResourceList{
					apiv1.ResourceCPU:    resource.MustParse("250m"),
					apiv1.ResourceMemory: resource.MustParse("3Gi"),
				},
			}),
		),
		supportedCpuPlatforms: CpuPlatformRequirements{lowerBound: GoogleAxionTamar, upperBound: GoogleAxionTamar}, // Used value from N4A
		supportedBootDiskTypes: map[string]bool{
			DiskTypeHyperdiskBalanced: true, // Explicitly required in create commands
		},
		// TODO: b/467906342 add DiskTypeHyperdiskMl
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced:   UnspecifiedMode, // E4A supports Hyperdisk
			DiskTypeHyperdiskThroughput: UnspecifiedMode,
			DiskTypeHyperdiskMl:         UnspecifiedMode,
		},
		defaultDiskType: DiskTypeHyperdiskBalanced, // Based on a requirement for instance creation.
		resizableConfig: &ResizableMachineFamilyConfig{
			DefaultMachineTypes:                []string{"e4a-standard-8", "e4a-standard-16", "e4a-standard-32"},
			KubeProxyMemoryBytesOverheadPerCPU: *resource.NewQuantity(8192000, resource.BinarySI),
			MinSizeLimit: apiv1.ResourceList{
				apiv1.ResourceCPU:    resource.MustParse("250m"),
				apiv1.ResourceMemory: resource.MustParse("2Gi"),
			},
			MinIncrementLimit: apiv1.ResourceList{
				apiv1.ResourceCPU:    resource.MustParse("50m"),
				apiv1.ResourceMemory: resource.MustParse("1Mi"),
			},
			MinMemoryPerCPU: resource.MustParse("512Mi"),
			MaxMemoryPerCPU: resource.MustParse("8Gi"),
			MinVmSizeDefault: apiv1.ResourceList{
				apiv1.ResourceCPU:    resource.MustParse("2"),
				apiv1.ResourceMemory: resource.MustParse("4Gi"),
			},
			IncrementStepDefault: apiv1.ResourceList{
				apiv1.ResourceCPU:    resource.MustParse("2"),
				apiv1.ResourceMemory: resource.MustParse("1Mi"),
			},
			AllocationSafetyDefault: apiv1.ResourceList{
				apiv1.ResourceCPU:    resource.MustParse("0"),
				apiv1.ResourceMemory: resource.MustParse("500Mi"),
			},
		},
	})
	// E4 represents e4 machine family
	E4 = RegisterMachineFamily(MachineFamily{
		name:               "e4",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.02181159,
			MemoryPricePerHourPerGb: 0.00292353,
			PreemptibleDiscount:     0.0130312 / 0.02181159,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("e4-standard-2", 2, 8),
			NewMachineTypeInfo("e4-medium", 2, 4),
			NewMachineTypeInfo("e4-standard-4", 4, 16),
			NewMachineTypeInfo("e4-standard-8", 8, 32),
			NewMachineTypeInfo("e4-standard-16", 16, 64),
			NewMachineTypeInfo("e4-standard-32", 32, 128),
			NewMachineTypeInfo("e4-highcpu-2", 2, 2),
			NewMachineTypeInfo("e4-highcpu-4", 4, 4),
			NewMachineTypeInfo("e4-highcpu-8", 8, 8),
			NewMachineTypeInfo("e4-highcpu-16", 16, 16),
			NewMachineTypeInfo("e4-highcpu-32", 32, 32),
			NewMachineTypeInfo("e4-highmem-2", 2, 16),
			NewMachineTypeInfo("e4-highmem-4", 4, 32),
			NewMachineTypeInfo("e4-highmem-8", 8, 64),
			NewMachineTypeInfo("e4-highmem-16", 16, 128),
		),
		supportedCpuPlatforms:    CpuPlatformRequirements{lowerBound: IntelSapphireRapids, upperBound: IntelSapphireRapids},
		supportCompactPlacement:  false,
		supportConfidentialNodes: false,
		supportedBootDiskTypes: map[string]bool{
			"hyperdisk-balanced":                   true,
			"hyperdisk-throughput":                 true,
			"hyperdisk-balanced-high-availability": true,
			"hyperdisk-ml":                         true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced:                 UnspecifiedMode,
			DiskTypeHyperdiskThroughput:               UnspecifiedMode,
			DiskTypeHyperdiskMl:                       UnspecifiedMode,
			DiskTypeHyperdiskBalancedHighAvailability: UnspecifiedMode,
		},
		supportHugepageSize1g: false,
		defaultDiskType:       "hyperdisk-balanced",
	})
	// EK represents ek machine family
	EK = RegisterMachineFamily(MachineFamily{
		name:               "ek",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.001999,
			MemoryPricePerHourPerGb: 0.00024,
			PreemptibleDiscount:     0.5,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewResizableMachineTypeInfo("ek-standard-2", 2, 8, &ResizableMachineTypeConfig{
				MinResources: apiv1.ResourceList{
					apiv1.ResourceCPU:    resource.MustParse("250m"),
					apiv1.ResourceMemory: resource.MustParse("2Gi"),
				},
			}),
			NewResizableMachineTypeInfo("ek-standard-4", 4, 16, &ResizableMachineTypeConfig{
				MinResources: apiv1.ResourceList{
					apiv1.ResourceCPU:    resource.MustParse("250m"),
					apiv1.ResourceMemory: resource.MustParse("2Gi"),
				},
			}),
			NewResizableMachineTypeInfo("ek-standard-8", 8, 32, &ResizableMachineTypeConfig{
				MinResources: apiv1.ResourceList{
					apiv1.ResourceCPU:    resource.MustParse("250m"),
					apiv1.ResourceMemory: resource.MustParse("2Gi"),
				},
			}),
			NewResizableMachineTypeInfo("ek-standard-16", 16, 64, &ResizableMachineTypeConfig{
				MinResources: apiv1.ResourceList{
					apiv1.ResourceCPU:    resource.MustParse("250m"),
					apiv1.ResourceMemory: resource.MustParse("3Gi"),
				},
			}),
			NewResizableMachineTypeInfo("ek-standard-32", 32, 128, &ResizableMachineTypeConfig{
				MinResources: apiv1.ResourceList{
					apiv1.ResourceCPU:    resource.MustParse("250m"),
					apiv1.ResourceMemory: resource.MustParse("3Gi"),
				},
			}),
		),
		supportedCpuPlatforms: noPlatformSupported,
		supportedBootDiskTypes: map[string]bool{
			DiskTypeStandard: true,
			DiskTypeBalanced: true,
			DiskTypeSSD:      true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeBalanced:  UnspecifiedMode,
			DiskTypeSSD:       UnspecifiedMode,
			DiskTypeStandard:  UnspecifiedMode,
			DiskTypePDExtreme: UnspecifiedMode,
		},
		defaultDiskType: DiskTypeBalanced,
		resizableConfig: &ResizableMachineFamilyConfig{
			DefaultMachineTypes:                []string{"ek-standard-16", "ek-standard-32"},
			KubeProxyMemoryBytesOverheadPerCPU: *resource.NewQuantity(8192000, resource.BinarySI),
			MinSizeLimit: apiv1.ResourceList{
				apiv1.ResourceCPU:    resource.MustParse("250m"),
				apiv1.ResourceMemory: resource.MustParse("2Gi"),
			},
			MinIncrementLimit: apiv1.ResourceList{
				apiv1.ResourceCPU:    resource.MustParse("50m"),
				apiv1.ResourceMemory: resource.MustParse("1Mi"),
			},
			MinMemoryPerCPU: resource.MustParse("512Mi"),
			MaxMemoryPerCPU: resource.MustParse("8Gi"),
			MinVmSizeDefault: apiv1.ResourceList{
				apiv1.ResourceCPU:    resource.MustParse("2"),
				apiv1.ResourceMemory: resource.MustParse("4Gi"),
			},
			IncrementStepDefault: apiv1.ResourceList{
				apiv1.ResourceCPU:    resource.MustParse("2"),
				apiv1.ResourceMemory: resource.MustParse("1Mi"),
			},
			AllocationSafetyDefault: apiv1.ResourceList{
				apiv1.ResourceCPU:    resource.MustParse("0"),
				apiv1.ResourceMemory: resource.MustParse("500Mi"),
			},
		},
		numaAlignmentUnsupported: true,
	})
	// G2 represents g2 machine family
	G2 = RegisterMachineFamily(MachineFamily{
		name:               "g2",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.024988,
			MemoryPricePerHourPerGb: 0.002927,
			PreemptibleDiscount:     0.007496 / 0.024988,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			// Initial G2 machines - compatible with L4 gpus.
			NewMachineTypeInfo("g2-standard-4", 4, 16).
				withGpuOverride(NvidiaL4, 1).
				withInstancePriceOverride(0.76).
				withPreemptibleInstancePriceOverride(0.23).
				withAllowedEphemeralLocalSsdCounts(1),

			NewMachineTypeInfo("g2-standard-8", 8, 32).
				withGpuOverride(NvidiaL4, 1).
				withInstancePriceOverride(0.91).
				withPreemptibleInstancePriceOverride(0.27).
				withAllowedEphemeralLocalSsdCounts(1),

			NewMachineTypeInfo("g2-standard-12", 12, 48).
				withGpuOverride(NvidiaL4, 1).
				withInstancePriceOverride(1.06).
				withPreemptibleInstancePriceOverride(0.32).
				withAllowedEphemeralLocalSsdCounts(1),

			NewMachineTypeInfo("g2-standard-16", 16, 64).
				withGpuOverride(NvidiaL4, 1).
				withInstancePriceOverride(1.20).
				withPreemptibleInstancePriceOverride(0.36).
				withAllowedEphemeralLocalSsdCounts(1),

			NewMachineTypeInfo("g2-standard-24", 23, 96).
				withGpuOverride(NvidiaL4, 2).
				withInstancePriceOverride(2.11).
				withPreemptibleInstancePriceOverride(0.63).
				withAllowedEphemeralLocalSsdCounts(2),

			NewMachineTypeInfo("g2-standard-32", 32, 128).
				withGpuOverride(NvidiaL4, 1).
				withInstancePriceOverride(1.79).
				withPreemptibleInstancePriceOverride(0.54).
				withAllowedEphemeralLocalSsdCounts(1),

			NewMachineTypeInfo("g2-standard-48", 48, 192).
				withGpuOverride(NvidiaL4, 4).
				withInstancePriceOverride(4.23).
				withPreemptibleInstancePriceOverride(1.27).
				withAllowedEphemeralLocalSsdCounts(4),

			NewMachineTypeInfo("g2-standard-96", 96, 384).
				withGpuOverride(NvidiaL4, 8).
				withInstancePriceOverride(8.46).
				withPreemptibleInstancePriceOverride(2.54).
				withAllowedEphemeralLocalSsdCounts(8),
		),
		supportedCpuPlatforms: CpuPlatformRequirements{lowerBound: IntelCascadeLake, upperBound: IntelCascadeLake},
		supportedGpuTypes: onboardSupportedGpus(
			NvidiaL4,
		),
		supportCompactPlacement:  true,
		maxCompactPlacementNodes: int64(150),
		supportedBootDiskTypes: map[string]bool{
			DiskTypeBalanced: true,
			DiskTypeSSD:      true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskThroughput: UnspecifiedMode,
			DiskTypeHyperdiskMl:         NonConfidentialOnlyMode,
			DiskTypeBalanced:            UnspecifiedMode,
			DiskTypeSSD:                 UnspecifiedMode,
			DiskTypeStandard:            UnspecifiedMode,
		},
		defaultDiskType: DiskTypeBalanced,
	})
	// G4 represents g4 machine family
	G4 = RegisterMachineFamily(MachineFamily{
		name:               "g4",
		systemArchitecture: gce.Amd64,
		// Copied and multiplied by 2 from G2
		// TODO(b/432461254): fix pricing when real prices are available
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.049976,
			MemoryPricePerHourPerGb: 0.005854,
			PreemptibleDiscount:     0.014992 / 0.024988,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			// (b/481243051) Fractional vGPU shapes have the same accelerator name & count as the non-frac ones,
			// so they all need to be set to explicit request only for now.
			NewMachineTypeInfo("g4-standard-6", 6, 22).
				withGpuOverride(NvidiaRTXPro6000, 1).
				withExplicitReqOnly(),
			NewMachineTypeInfo("g4-standard-12", 12, 45).
				withGpuOverride(NvidiaRTXPro6000, 1).
				withAllowedEphemeralLocalSsdCounts(1).
				withExplicitReqOnly(),
			NewMachineTypeInfo("g4-standard-24", 24, 90).
				withGpuOverride(NvidiaRTXPro6000, 1).
				withAllowedEphemeralLocalSsdCounts(2).
				withExplicitReqOnly(),
			NewMachineTypeInfo("g4-standard-48", 48, 180).
				withGpuOverride(NvidiaRTXPro6000, 1).
				withAllowedEphemeralLocalSsdCounts(4).
				withSupportedConfidentialNodeTypes([]string{labels.SEVConfidentialNodeTypeValue}),
			NewMachineTypeInfo("g4-standard-96", 96, 360).
				withGpuOverride(NvidiaRTXPro6000, 2).
				withAllowedEphemeralLocalSsdCounts(8),
			NewMachineTypeInfo("g4-standard-192", 192, 720).
				withGpuOverride(NvidiaRTXPro6000, 4).
				withAllowedEphemeralLocalSsdCounts(16),
			NewMachineTypeInfo("g4-standard-384", 384, 1440).
				withGpuOverride(NvidiaRTXPro6000, 8).
				withAllowedEphemeralLocalSsdCounts(32),
		),
		supportedCpuPlatforms: CpuPlatformRequirements{lowerBound: AmdTurin, upperBound: AmdTurin},
		supportedGpuTypes: onboardSupportedGpus(
			NvidiaRTXPro6000,
		),
		supportCompactPlacement:  true,
		maxCompactPlacementNodes: int64(1500),
		supportedBootDiskTypes: map[string]bool{
			DiskTypeHyperdiskBalanced: true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced:                 UnspecifiedMode,
			DiskTypeHyperdiskBalancedHighAvailability: UnspecifiedMode,
			DiskTypeHyperdiskExtreme:                  UnspecifiedMode,
			DiskTypeHyperdiskThroughput:               UnspecifiedMode,
		},
		defaultDiskType:       DiskTypeHyperdiskBalanced,
		supportHugepageSize1g: true,
	})
	// H3 represents h3 machine family
	H3 = RegisterMachineFamily(MachineFamily{
		name:               "h3",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.04411,
			MemoryPricePerHourPerGb: 0.00296,
			PreemptibleDiscount:     1, // H3 machines are not available as preemptible
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("h3-standard-88", 88, 352),
		),
		supportedCpuPlatforms:    CpuPlatformRequirements{lowerBound: IntelSapphireRapids, upperBound: IntelSapphireRapids},
		supportCompactPlacement:  true,
		maxCompactPlacementNodes: int64(150),
		supportedBootDiskTypes: map[string]bool{
			DiskTypeBalanced:          true,
			DiskTypeHyperdiskBalanced: true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced:   NonConfidentialOnlyMode,
			DiskTypeHyperdiskThroughput: UnspecifiedMode,
			DiskTypeBalanced:            UnspecifiedMode,
		},
		defaultDiskType:       DiskTypeBalanced,
		supportHugepageSize1g: true,
	})
	// H4D represents h4d machine family
	H4D = RegisterMachineFamily(MachineFamily{
		name:               "h4d",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.03224,
			MemoryPricePerHourPerGb: 0.00231,
			PreemptibleDiscount:     0.019343999999999997 / 0.03224,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("h4d-standard-192", 192, 720),
			NewMachineTypeInfo("h4d-highmem-192", 192, 1488),
			NewMachineTypeInfo("h4d-highmem-192-lssd", 192, 1488).
				withAutomaticEphemeralLocalSsdCount(10).
				withInstancePriceOverride(10.030586).
				withPreemptibleInstancePriceOverride(5.776885),
		),
		supportedCpuPlatforms:    CpuPlatformRequirements{lowerBound: AmdTurin, upperBound: AmdTurin},
		supportCompactPlacement:  true,
		maxCompactPlacementNodes: int64(150),
		supportConfidentialNodes: false,
		supportedBootDiskTypes: map[string]bool{
			"hyperdisk-balanced": true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced:                 NonConfidentialOnlyMode,
			DiskTypeHyperdiskBalancedHighAvailability: NonConfidentialOnlyMode,
		},
		supportHugepageSize1g: true,
		defaultDiskType:       DiskTypeHyperdiskBalanced,
	})
	// M1 represents m1 machine family
	M1 = RegisterMachineFamily(MachineFamily{
		name:               "m1",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.0348,
			MemoryPricePerHourPerGb: 0.0051,
			PreemptibleDiscount:     0.00733 / 0.0348,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("m1-ultramem-40", 40, 961),
			NewMachineTypeInfo("m1-ultramem-80", 80, 1922),
			NewMachineTypeInfo("m1-ultramem-160", 160, 3844),

			NewMachineTypeInfo("m1-megamem-96", 96, 1433.6).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelSkylake, IntelSkylake)),
		),
		supportedCpuPlatforms: CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelBroadwell},
		supportedBootDiskTypes: map[string]bool{
			DiskTypeStandard:          true,
			DiskTypeBalanced:          true,
			DiskTypeSSD:               true,
			DiskTypeHyperdiskBalanced: true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced: NonConfidentialOnlyMode,
			DiskTypeHyperdiskExtreme:  NonConfidentialOnlyMode,
			DiskTypeBalanced:          UnspecifiedMode,
			DiskTypePDExtreme:         UnspecifiedMode,
			DiskTypeSSD:               UnspecifiedMode,
			DiskTypeStandard:          UnspecifiedMode,
		},
		defaultDiskType: DiskTypeStandard,
	})
	M2 = RegisterMachineFamily(MachineFamily{
		name:               "m2",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.0393,
			MemoryPricePerHourPerGb: 0.0057,
			PreemptibleDiscount:     1, // M2 machines are not available as preemptible
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("m2-ultramem-208", 208, 5888),
			NewMachineTypeInfo("m2-ultramem-416", 416, 11776),
			NewMachineTypeInfo("m2-megamem-416", 416, 5888),
			NewMachineTypeInfo("m2-hypermem-416", 416, 8832),
		),
		supportedCpuPlatforms: noPlatformSupported,
		supportedBootDiskTypes: map[string]bool{
			DiskTypeStandard:          true,
			DiskTypeBalanced:          true,
			DiskTypeSSD:               true,
			DiskTypeHyperdiskBalanced: true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced: NonConfidentialOnlyMode,
			DiskTypeHyperdiskExtreme:  NonConfidentialOnlyMode,
			DiskTypeBalanced:          UnspecifiedMode,
			DiskTypePDExtreme:         UnspecifiedMode,
			DiskTypeSSD:               UnspecifiedMode,
			DiskTypeStandard:          UnspecifiedMode,
		},
		defaultDiskType:       DiskTypeStandard,
		supportHugepageSize1g: true,
	})
	M3 = RegisterMachineFamily(MachineFamily{
		name:               "m3",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.0348,
			MemoryPricePerHourPerGb: 0.0051,
			PreemptibleDiscount:     0.0136 / 0.0348,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("m3-ultramem-32", 32, 976).
				withAllowedEphemeralLocalSsdCounts(4, 8),
			NewMachineTypeInfo("m3-ultramem-64", 64, 1952).
				withAllowedEphemeralLocalSsdCounts(4, 8),
			NewMachineTypeInfo("m3-ultramem-128", 128, 3904).
				withAllowedEphemeralLocalSsdCounts(8),
			NewMachineTypeInfo("m3-megamem-64", 64, 976).
				withAllowedEphemeralLocalSsdCounts(4, 8),
			NewMachineTypeInfo("m3-megamem-128", 128, 1952).
				withAllowedEphemeralLocalSsdCounts(8)),
		supportedCpuPlatforms: noPlatformSupported,
		supportedBootDiskTypes: map[string]bool{
			DiskTypeStandard:          true,
			DiskTypeBalanced:          true,
			DiskTypeSSD:               true,
			DiskTypeHyperdiskBalanced: true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced:                 NonConfidentialOnlyMode,
			DiskTypeHyperdiskExtreme:                  NonConfidentialOnlyMode,
			DiskTypeHyperdiskThroughput:               NonConfidentialOnlyMode,
			DiskTypeHyperdiskBalancedHighAvailability: NonConfidentialOnlyMode,
			DiskTypeBalanced:                          UnspecifiedMode,
			DiskTypePDExtreme:                         UnspecifiedMode,
			DiskTypeSSD:                               UnspecifiedMode,
			DiskTypeStandard:                          UnspecifiedMode,
		},
		defaultDiskType:       DiskTypeStandard,
		supportHugepageSize1g: true,
	})

	// M4 represents m4 machine family
	M4 = RegisterMachineFamily(MachineFamily{
		name:               "m4",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.0182784,
			MemoryPricePerHourPerGb: 0.00457,
			PreemptibleDiscount:     0.0073114 / 0.0182784,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("m4-ultramem-56", 56, 1488),
			NewMachineTypeInfo("m4-ultramem-112", 112, 2976),
			NewMachineTypeInfo("m4-ultramem-224", 224, 5952),
			NewMachineTypeInfo("m4-megamem-28", 28, 372),
			NewMachineTypeInfo("m4-megamem-56", 56, 744),
			NewMachineTypeInfo("m4-megamem-112", 112, 1488),
			NewMachineTypeInfo("m4-megamem-224", 224, 2976),
		),
		supportedCpuPlatforms:    CpuPlatformRequirements{lowerBound: IntelEmeraldRapids, upperBound: IntelEmeraldRapids},
		supportCompactPlacement:  true,
		maxCompactPlacementNodes: int64(150),
		supportConfidentialNodes: false,
		// M4 as a GCE machine family also supports hyperdisk-extreme
		// as mentioned in the docs (https://cloud.google.com/compute/docs/memory-optimized-machines#m4_disks),
		// however it is not supported as a boot disk,
		// according to SoT: http://google3/configs/cloud/cluster/vmfamilies/sot/textproto/common_metadata/vm_family_metadata/m4_vm.textproto;l=216
		supportedBootDiskTypes: map[string]bool{
			DiskTypeHyperdiskBalanced: true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced: NonConfidentialOnlyMode,
			DiskTypeHyperdiskExtreme:  NonConfidentialOnlyMode,
		},
		supportHugepageSize1g: true,
		defaultDiskType:       DiskTypeHyperdiskBalanced,
	})

	// N1 represents n1 machine family
	N1 = RegisterMachineFamily(MachineFamily{
		name:               "n1",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.031611,
			MemoryPricePerHourPerGb: 0.004237,
			PreemptibleDiscount:     0.006655 / 0.031611,
		},
		customPricingInfo: &MachineFamilyPricingInfo{
			CpuPricePerHour:         0.033174,
			MemoryPricePerHourPerGb: 0.004446,
			PreemptibleDiscount:     0.00698 / 0.033174,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			// The custom shapes are needed for supporting the whole CPU range of a T4 GPU (the 48-core ones), and for decreasing
			// defragmentation in Autopilot clusters with GPU pods (the 80-core ones). The shapes use normal n1 standard and highmem
			// ratios, and there are equivalent n2 shapes (though the ratios are different there). More details: go/autopilot-gpu-design.
			NewMachineTypeInfo("n1-standard-1", 1, 3.75).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8, 16, 24),
			NewMachineTypeInfo("n1-standard-2", 2, 7.5).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8, 16, 24),
			NewMachineTypeInfo("n1-standard-4", 4, 15).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8, 16, 24),
			NewMachineTypeInfo("n1-standard-8", 8, 30).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8, 16, 24),
			NewMachineTypeInfo("n1-standard-16", 16, 60).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8, 16, 24),
			NewMachineTypeInfo("n1-standard-32", 32, 120).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8, 16, 24),
			NewMachineTypeInfo("custom-48-184320", 48, 180), // Ratio equivalent to n1-standard (stand-in for missing n1-standard-48).
			NewMachineTypeInfo("n1-standard-64", 64, 240).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8, 16, 24),

			NewMachineTypeInfo("custom-80-307200", 80, 300).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelSkylake, IntelSkylake)), // Ratio equivalent to n1-standard (stand-in for missing n1-standard-80).

			NewMachineTypeInfo("n1-standard-96", 96, 360).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelSkylake, IntelSkylake)).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8, 16, 24),

			NewMachineTypeInfo("n1-highcpu-2", 2, 1.8).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8, 16, 24),
			NewMachineTypeInfo("n1-highcpu-4", 4, 3.6).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8, 16, 24),
			NewMachineTypeInfo("n1-highcpu-8", 8, 7.2).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8, 16, 24),
			NewMachineTypeInfo("n1-highcpu-16", 16, 14.4).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8, 16, 24),
			NewMachineTypeInfo("n1-highcpu-32", 32, 28.8).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8, 16, 24),
			NewMachineTypeInfo("n1-highcpu-64", 64, 57.6).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8, 16, 24),

			NewMachineTypeInfo("n1-highcpu-96", 96, 86.4).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelSkylake, IntelSkylake)).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8, 16, 24),

			NewMachineTypeInfo("n1-highmem-2", 2, 13).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8, 16, 24),
			NewMachineTypeInfo("n1-highmem-4", 4, 26).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8, 16, 24),
			NewMachineTypeInfo("n1-highmem-8", 8, 52).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8, 16, 24),
			NewMachineTypeInfo("n1-highmem-16", 16, 104).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8, 16, 24),
			NewMachineTypeInfo("n1-highmem-32", 32, 208).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8, 16, 24),
			NewMachineTypeInfo("custom-48-319488", 48, 312), // Ratio equivalent to n1-highmem (stand-in for missing n1-highmem-48).
			NewMachineTypeInfo("n1-highmem-64", 64, 416).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8, 16, 24),

			NewMachineTypeInfo("custom-80-532480", 80, 520).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelSkylake, IntelSkylake)), // Ratio equivalent to n1-highmem (stand-in for missing n1-highmem-80).

			NewMachineTypeInfo("n1-highmem-96", 96, 624).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelSkylake, IntelSkylake)).
				withAllowedEphemeralLocalSsdCounts(1, 2, 3, 4, 5, 6, 7, 8, 16, 24),
		),
		otherMachineTypes: onboardMachineType(
			NewMachineTypeInfo("f1-micro", 1, 0.6).
				withInstancePriceOverride(0.0076).
				withPreemptibleInstancePriceOverride(0.0035),

			NewMachineTypeInfo("g1-small", 1, 1.7).
				withInstancePriceOverride(0.0257).
				withPreemptibleInstancePriceOverride(0.007),
		),
		supportedCpuPlatforms: CpuPlatformRequirements{lowerBound: IntelSandyBridge, upperBound: IntelSkylake},
		supportedGpuTypes: onboardSupportedGpus(
			NvidiaTeslaK80,
			NvidiaTeslaP100,
			NvidiaTeslaV100,
			NvidiaTeslaP4,
			NvidiaTeslaT4,
		),
		supportedBootDiskTypes: map[string]bool{
			DiskTypeStandard: true,
			DiskTypeBalanced: true,
			DiskTypeSSD:      true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeBalanced:            UnspecifiedMode,
			DiskTypePDExtreme:           UnspecifiedMode,
			DiskTypeSSD:                 UnspecifiedMode,
			DiskTypeStandard:            UnspecifiedMode,
			DiskTypeHyperdiskThroughput: UnspecifiedMode,
			DiskTypeHyperdiskBalanced:   UnspecifiedMode,
		},
		defaultDiskType:          DiskTypeStandard,
		numaAlignmentUnsupported: true,
	})
	// N2 represents n2 machine family
	N2 = RegisterMachineFamily(MachineFamily{
		name:               "n2",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.031611,
			MemoryPricePerHourPerGb: 0.004237,
			PreemptibleDiscount:     0.007650 / 0.031611,
		},
		customPricingInfo: &MachineFamilyPricingInfo{
			CpuPricePerHour:         0.033174,
			MemoryPricePerHourPerGb: 0.004446,
			PreemptibleDiscount:     0.00802 / 0.033174,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("n2-standard-2", 2, 8).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2-standard-4", 4, 16).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2-standard-8", 8, 32).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2-standard-16", 16, 64).
				withAllowedEphemeralLocalSsdCounts(2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2-standard-32", 32, 128).
				withAllowedEphemeralLocalSsdCounts(4, 8, 16, 24),
			NewMachineTypeInfo("n2-standard-48", 48, 192).
				withAllowedEphemeralLocalSsdCounts(8, 16, 24),
			NewMachineTypeInfo("n2-standard-64", 64, 256).
				withAllowedEphemeralLocalSsdCounts(8, 16, 24),
			NewMachineTypeInfo("n2-standard-80", 80, 320).
				withAllowedEphemeralLocalSsdCounts(8, 16, 24),

			NewMachineTypeInfo("n2-standard-96", 96, 384).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelIceLake, IntelIceLake)).
				withAllowedEphemeralLocalSsdCounts(16, 24),

			NewMachineTypeInfo("n2-standard-128", 128, 512).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelIceLake, IntelIceLake)).
				withAllowedEphemeralLocalSsdCounts(16, 24),

			NewMachineTypeInfo("n2-highcpu-2", 2, 2).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2-highcpu-4", 4, 4).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2-highcpu-8", 8, 8).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2-highcpu-16", 16, 16).
				withAllowedEphemeralLocalSsdCounts(2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2-highcpu-32", 32, 32).
				withAllowedEphemeralLocalSsdCounts(4, 8, 16, 24),
			NewMachineTypeInfo("n2-highcpu-48", 48, 48).
				withAllowedEphemeralLocalSsdCounts(8, 16, 24),
			NewMachineTypeInfo("n2-highcpu-64", 64, 64).
				withAllowedEphemeralLocalSsdCounts(8, 16, 24),
			NewMachineTypeInfo("n2-highcpu-80", 80, 80).
				withAllowedEphemeralLocalSsdCounts(8, 16, 24),

			NewMachineTypeInfo("n2-highcpu-96", 96, 96).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelIceLake, IntelIceLake)).
				withAllowedEphemeralLocalSsdCounts(16, 24),

			NewMachineTypeInfo("n2-highcpu-128", 128, 128).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelIceLake, IntelIceLake)).
				withAllowedEphemeralLocalSsdCounts(16, 24),

			NewMachineTypeInfo("n2-highmem-2", 2, 16).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2-highmem-4", 4, 32).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2-highmem-8", 8, 64).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2-highmem-16", 16, 128).
				withAllowedEphemeralLocalSsdCounts(2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2-highmem-32", 32, 256).
				withAllowedEphemeralLocalSsdCounts(4, 8, 16, 24),
			NewMachineTypeInfo("n2-highmem-48", 48, 384).
				withAllowedEphemeralLocalSsdCounts(8, 16, 24),
			NewMachineTypeInfo("n2-highmem-64", 64, 512).
				withAllowedEphemeralLocalSsdCounts(8, 16, 24),
			NewMachineTypeInfo("n2-highmem-80", 80, 640).
				withAllowedEphemeralLocalSsdCounts(8, 16, 24),

			NewMachineTypeInfo("n2-highmem-96", 96, 768).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelIceLake, IntelIceLake)).
				withAllowedEphemeralLocalSsdCounts(16, 24),

			NewMachineTypeInfo("n2-highmem-128", 128, 864).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(IntelIceLake, IntelIceLake)).
				withAllowedEphemeralLocalSsdCounts(16, 24),
		),
		supportedCpuPlatforms:    CpuPlatformRequirements{lowerBound: IntelCascadeLake, upperBound: IntelIceLake},
		supportCompactPlacement:  true,
		maxCompactPlacementNodes: int64(150),
		supportedBootDiskTypes: map[string]bool{
			DiskTypeStandard: true,
			DiskTypeBalanced: true,
			DiskTypeSSD:      true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced:                 ConfidentialOnlyMode,
			DiskTypeHyperdiskBalancedHighAvailability: UnspecifiedMode,
			DiskTypeHyperdiskExtreme:                  NonConfidentialOnlyMode,
			DiskTypeHyperdiskMl:                       UnspecifiedMode,
			DiskTypeHyperdiskThroughput:               NonConfidentialOnlyMode,
			DiskTypeBalanced:                          UnspecifiedMode,
			DiskTypePDExtreme:                         UnspecifiedMode,
			DiskTypeSSD:                               UnspecifiedMode,
			DiskTypeStandard:                          UnspecifiedMode,
		},
		defaultDiskType:          DiskTypeStandard,
		numaAlignmentUnsupported: true,
	})
	// N2D represents n2d machine family
	N2D = RegisterMachineFamily(MachineFamily{
		name:               "n2d",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.027502,
			MemoryPricePerHourPerGb: 0.003686,
			PreemptibleDiscount:     0.002773 / 0.027502,
		},
		customPricingInfo: &MachineFamilyPricingInfo{
			CpuPricePerHour:         0.028877,
			MemoryPricePerHourPerGb: 0.003870,
			PreemptibleDiscount:     0.002908 / 0.028877,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("n2d-standard-2", 2, 8).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2d-standard-4", 4, 16).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2d-standard-8", 8, 32).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2d-standard-16", 16, 64).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2d-standard-32", 32, 128).
				withAllowedEphemeralLocalSsdCounts(2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2d-standard-48", 48, 192).
				withAllowedEphemeralLocalSsdCounts(2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2d-standard-64", 64, 256).
				withAllowedEphemeralLocalSsdCounts(4, 8, 16, 24),
			NewMachineTypeInfo("n2d-standard-80", 80, 320).
				withAllowedEphemeralLocalSsdCounts(4, 8, 16, 24),
			NewMachineTypeInfo("n2d-standard-96", 96, 384).
				withAllowedEphemeralLocalSsdCounts(8, 16, 24),
			NewMachineTypeInfo("n2d-standard-128", 128, 512).
				withAllowedEphemeralLocalSsdCounts(8, 16, 24),
			NewMachineTypeInfo("n2d-standard-224", 224, 896).
				withAllowedEphemeralLocalSsdCounts(8, 16, 24),
			NewMachineTypeInfo("n2d-highcpu-2", 2, 2).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2d-highcpu-4", 4, 4).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2d-highcpu-8", 8, 8).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2d-highcpu-16", 16, 16).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2d-highcpu-32", 32, 32).
				withAllowedEphemeralLocalSsdCounts(2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2d-highcpu-48", 48, 48).
				withAllowedEphemeralLocalSsdCounts(2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2d-highcpu-64", 64, 64).
				withAllowedEphemeralLocalSsdCounts(4, 8, 16, 24),
			NewMachineTypeInfo("n2d-highcpu-80", 80, 80).
				withAllowedEphemeralLocalSsdCounts(4, 8, 16, 24),
			NewMachineTypeInfo("n2d-highcpu-96", 96, 96).
				withAllowedEphemeralLocalSsdCounts(8, 16, 24),
			NewMachineTypeInfo("n2d-highcpu-128", 128, 128).
				withAllowedEphemeralLocalSsdCounts(8, 16, 24),
			NewMachineTypeInfo("n2d-highcpu-224", 224, 224).
				withAllowedEphemeralLocalSsdCounts(8, 16, 24),
			NewMachineTypeInfo("n2d-highmem-2", 2, 16).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2d-highmem-4", 4, 32).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2d-highmem-8", 8, 64).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2d-highmem-16", 16, 128).
				withAllowedEphemeralLocalSsdCounts(1, 2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2d-highmem-32", 32, 256).
				withAllowedEphemeralLocalSsdCounts(2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2d-highmem-48", 48, 384).
				withAllowedEphemeralLocalSsdCounts(2, 4, 8, 16, 24),
			NewMachineTypeInfo("n2d-highmem-64", 64, 512).
				withAllowedEphemeralLocalSsdCounts(4, 8, 16, 24),
			NewMachineTypeInfo("n2d-highmem-80", 80, 640).
				withAllowedEphemeralLocalSsdCounts(4, 8, 16, 24),
			NewMachineTypeInfo("n2d-highmem-96", 96, 786).
				withAllowedEphemeralLocalSsdCounts(8, 16, 24),
		),
		supportedCpuPlatforms:    CpuPlatformRequirements{lowerBound: AmdRome, upperBound: AmdMilan},
		supportCompactPlacement:  true,
		maxCompactPlacementNodes: int64(150),
		supportConfidentialNodes: true,
		supportConfidentialNodeTypes: map[string]bool{
			labels.SEVConfidentialNodeTypeValue:    true,
			labels.SEVSNPConfidentialNodeTypeValue: true,
		},
		supportedBootDiskTypes: map[string]bool{
			DiskTypeStandard: true,
			DiskTypeBalanced: true,
			DiskTypeSSD:      true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced:   ConfidentialOnlyMode,
			DiskTypeHyperdiskThroughput: NonConfidentialOnlyMode,
			DiskTypeBalanced:            UnspecifiedMode,
			DiskTypePDExtreme:           UnspecifiedMode,
			DiskTypeSSD:                 UnspecifiedMode,
			DiskTypeStandard:            UnspecifiedMode,
		},
		defaultDiskType:          DiskTypeStandard,
		numaAlignmentUnsupported: true,
	})
	// N4 represents n4 machine family
	N4 = RegisterMachineFamily(MachineFamily{
		name:               "n4",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.03082073,
			MemoryPricePerHourPerGb: 0.004131075,
			PreemptibleDiscount:     0.0079755 / 0.03082073,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("n4-standard-2", 2, 8),
			NewMachineTypeInfo("n4-standard-4", 4, 16),
			NewMachineTypeInfo("n4-standard-8", 8, 32),
			NewMachineTypeInfo("n4-standard-16", 16, 64),
			NewMachineTypeInfo("n4-standard-32", 32, 128),
			NewMachineTypeInfo("n4-standard-48", 48, 192),
			NewMachineTypeInfo("n4-standard-64", 64, 256),
			NewMachineTypeInfo("n4-standard-80", 80, 320),
			NewMachineTypeInfo("n4-highcpu-2", 2, 4),
			NewMachineTypeInfo("n4-highcpu-4", 4, 8),
			NewMachineTypeInfo("n4-highcpu-8", 8, 16),
			NewMachineTypeInfo("n4-highcpu-16", 16, 32),
			NewMachineTypeInfo("n4-highcpu-32", 32, 64),
			NewMachineTypeInfo("n4-highcpu-48", 48, 96),
			NewMachineTypeInfo("n4-highcpu-64", 64, 128),
			NewMachineTypeInfo("n4-highcpu-80", 80, 160),
			NewMachineTypeInfo("n4-highmem-2", 2, 16),
			NewMachineTypeInfo("n4-highmem-4", 4, 32),
			NewMachineTypeInfo("n4-highmem-8", 8, 64),
			NewMachineTypeInfo("n4-highmem-16", 16, 128),
			NewMachineTypeInfo("n4-highmem-32", 32, 256),
			NewMachineTypeInfo("n4-highmem-48", 48, 384),
			NewMachineTypeInfo("n4-highmem-64", 64, 512),
			NewMachineTypeInfo("n4-highmem-80", 80, 640),
		),
		supportedCpuPlatforms: CpuPlatformRequirements{lowerBound: IntelEmeraldRapids, upperBound: IntelEmeraldRapids},
		supportedBootDiskTypes: map[string]bool{
			DiskTypeHyperdiskBalanced: true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced:                 NonConfidentialOnlyMode,
			DiskTypeHyperdiskThroughput:               NonConfidentialOnlyMode,
			DiskTypeHyperdiskBalancedHighAvailability: NonConfidentialOnlyMode,
			DiskTypeHyperdiskMl:                       UnspecifiedMode,
		},
		defaultDiskType: DiskTypeHyperdiskBalanced,
	})
	// N4A represents n4a machine family
	N4A = RegisterMachineFamily(MachineFamily{
		name:               "n4a",
		systemArchitecture: gce.Arm64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.032578,
			MemoryPricePerHourPerGb: 0.3,
			PreemptibleDiscount:     0.0130312 / 0.032578,
		},
		customPricingInfo: &MachineFamilyPricingInfo{
			CpuPricePerHour:         0.0342069,
			MemoryPricePerHourPerGb: 0.0038871,
			PreemptibleDiscount:     0.0136828 / 0.0342069,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("n4a-standard-1", 1, 4),
			NewMachineTypeInfo("n4a-standard-2", 2, 8),
			NewMachineTypeInfo("n4a-standard-4", 4, 16),
			NewMachineTypeInfo("n4a-standard-8", 8, 32),
			NewMachineTypeInfo("n4a-standard-16", 16, 64),
			NewMachineTypeInfo("n4a-standard-32", 32, 128),
			NewMachineTypeInfo("n4a-standard-48", 48, 192),
			NewMachineTypeInfo("n4a-standard-64", 64, 256),
			NewMachineTypeInfo("n4a-highcpu-1", 1, 2),
			NewMachineTypeInfo("n4a-highcpu-2", 2, 4),
			NewMachineTypeInfo("n4a-highcpu-4", 4, 8),
			NewMachineTypeInfo("n4a-highcpu-8", 8, 16),
			NewMachineTypeInfo("n4a-highcpu-16", 16, 32),
			NewMachineTypeInfo("n4a-highcpu-32", 32, 64),
			NewMachineTypeInfo("n4a-highcpu-48", 48, 96),
			NewMachineTypeInfo("n4a-highcpu-64", 64, 128),
			NewMachineTypeInfo("n4a-highmem-1", 1, 8),
			NewMachineTypeInfo("n4a-highmem-2", 2, 16),
			NewMachineTypeInfo("n4a-highmem-4", 4, 32),
			NewMachineTypeInfo("n4a-highmem-8", 8, 64),
			NewMachineTypeInfo("n4a-highmem-16", 16, 128),
			NewMachineTypeInfo("n4a-highmem-32", 32, 256),
			NewMachineTypeInfo("n4a-highmem-48", 48, 384),
			NewMachineTypeInfo("n4a-highmem-64", 64, 512),
		),
		supportedCpuPlatforms:    CpuPlatformRequirements{lowerBound: GoogleAxionTamar, upperBound: GoogleAxionTamar},
		supportCompactPlacement:  false,
		supportConfidentialNodes: false,
		nonDefaultThreadsPerCore: pInt64(1),
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced:                 NonConfidentialOnlyMode,
			DiskTypeHyperdiskBalancedHighAvailability: NonConfidentialOnlyMode,
			DiskTypeHyperdiskThroughput:               NonConfidentialOnlyMode,
			DiskTypeHyperdiskMl:                       NonConfidentialOnlyMode,
		},
		supportedBootDiskTypes: map[string]bool{
			DiskTypeHyperdiskBalanced: true,
		},
		supportHugepageSize1g: false,
		defaultDiskType:       DiskTypeHyperdiskBalanced,
	})
	// N4D represents n4d machine family
	N4D = RegisterMachineFamily(MachineFamily{
		name:               "n4d",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.032578,
			MemoryPricePerHourPerGb: 0.3,
			PreemptibleDiscount:     0.0130312 / 0.032578,
		},
		customPricingInfo: &MachineFamilyPricingInfo{
			CpuPricePerHour:         0.0342069,
			MemoryPricePerHourPerGb: 0.0038871,
			PreemptibleDiscount:     0.0130312 / 0.0342069,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("n4d-standard-2", 2, 8),
			NewMachineTypeInfo("n4d-standard-4", 4, 16),
			NewMachineTypeInfo("n4d-standard-8", 8, 32),
			NewMachineTypeInfo("n4d-standard-16", 16, 64),
			NewMachineTypeInfo("n4d-standard-32", 32, 128),
			NewMachineTypeInfo("n4d-standard-48", 48, 192),
			NewMachineTypeInfo("n4d-standard-64", 64, 256),
			NewMachineTypeInfo("n4d-standard-80", 80, 320),
			NewMachineTypeInfo("n4d-standard-96", 96, 384),
			NewMachineTypeInfo("n4d-highmem-2", 2, 16),
			NewMachineTypeInfo("n4d-highmem-4", 4, 32),
			NewMachineTypeInfo("n4d-highmem-8", 8, 64),
			NewMachineTypeInfo("n4d-highmem-16", 16, 128),
			NewMachineTypeInfo("n4d-highmem-32", 32, 256),
			NewMachineTypeInfo("n4d-highmem-48", 48, 384),
			NewMachineTypeInfo("n4d-highmem-64", 64, 512),
			NewMachineTypeInfo("n4d-highmem-80", 80, 640),
			NewMachineTypeInfo("n4d-highmem-96", 96, 768),
			NewMachineTypeInfo("n4d-highcpu-2", 2, 4),
			NewMachineTypeInfo("n4d-highcpu-4", 4, 8),
			NewMachineTypeInfo("n4d-highcpu-8", 8, 16),
			NewMachineTypeInfo("n4d-highcpu-16", 16, 32),
			NewMachineTypeInfo("n4d-highcpu-32", 32, 64),
			NewMachineTypeInfo("n4d-highcpu-48", 48, 96),
			NewMachineTypeInfo("n4d-highcpu-64", 64, 128),
			NewMachineTypeInfo("n4d-highcpu-80", 80, 160),
			NewMachineTypeInfo("n4d-highcpu-96", 96, 192),
		),
		supportedCpuPlatforms:    CpuPlatformRequirements{lowerBound: AmdTurin, upperBound: AmdTurin},
		supportCompactPlacement:  false,
		supportConfidentialNodes: false,
		supportedBootDiskTypes: map[string]bool{
			"hyperdisk-balanced": true,
		},
		supportHugepageSize1g: false,
		defaultDiskType:       "hyperdisk-balanced",
	})
	// T2A represents t2a (arm) machine family
	T2A = RegisterMachineFamily(MachineFamily{
		name:               "t2a",
		systemArchitecture: gce.Arm64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.02490,
			MemoryPricePerHourPerGb: 0.00340,
			PreemptibleDiscount:     0.04620 / 0.15400,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("t2a-standard-1", 1, 4),
			NewMachineTypeInfo("t2a-standard-2", 2, 8),
			NewMachineTypeInfo("t2a-standard-4", 4, 16),
			NewMachineTypeInfo("t2a-standard-8", 8, 32),
			NewMachineTypeInfo("t2a-standard-16", 16, 64),
			NewMachineTypeInfo("t2a-standard-32", 32, 128),
			NewMachineTypeInfo("t2a-standard-48", 48, 192),
		),
		supportedCpuPlatforms:    CpuPlatformRequirements{lowerBound: AmpereAltra, upperBound: AmpereAltra},
		nonDefaultThreadsPerCore: pInt64(1),
		supportedBootDiskTypes: map[string]bool{
			DiskTypeStandard: true,
			DiskTypeBalanced: true,
			DiskTypeSSD:      true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeBalanced:  UnspecifiedMode,
			DiskTypeSSD:       UnspecifiedMode,
			DiskTypeStandard:  UnspecifiedMode,
			DiskTypePDExtreme: UnspecifiedMode,
		},
		defaultDiskType: DiskTypeStandard,
	})
	// T2D represents t2d machine family
	T2D = RegisterMachineFamily(MachineFamily{
		name:               "t2d",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.027502,
			MemoryPricePerHourPerGb: 0.003686,
			PreemptibleDiscount:     0.006655 / 0.027502,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("t2d-standard-1", 1, 4),
			NewMachineTypeInfo("t2d-standard-2", 2, 8),
			NewMachineTypeInfo("t2d-standard-4", 4, 16),
			NewMachineTypeInfo("t2d-standard-8", 8, 32),
			NewMachineTypeInfo("t2d-standard-16", 16, 64),
			NewMachineTypeInfo("t2d-standard-32", 32, 128),
			NewMachineTypeInfo("t2d-standard-48", 48, 192),
			NewMachineTypeInfo("t2d-standard-60", 60, 240),
		),
		supportedCpuPlatforms:    CpuPlatformRequirements{lowerBound: AmdMilan, upperBound: AmdMilan},
		nonDefaultThreadsPerCore: pInt64(1),
		supportedBootDiskTypes: map[string]bool{
			DiskTypeStandard: true,
			DiskTypeBalanced: true,
			DiskTypeSSD:      true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskThroughput: NonConfidentialOnlyMode,
			DiskTypeBalanced:            UnspecifiedMode,
			DiskTypeSSD:                 UnspecifiedMode,
			DiskTypeStandard:            UnspecifiedMode,
		},
		defaultDiskType:          DiskTypeStandard,
		numaAlignmentUnsupported: true,
	})
	// TPU7X represents tpu7x machine family
	TPU7X = RegisterMachineFamily(MachineFamily{
		name:               "tpu7x",
		systemArchitecture: gce.Amd64,
		// For TPU, cpu and memory are not billed, the price is only based on TPU chips.
		// CpuPricePerHour is calculated by dividing total VM price by the cpu count.
		// TODO(b/432451286): update the prices once finalized.
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.12,
			MemoryPricePerHourPerGb: 0,
			PreemptibleDiscount:     0.6 / 1.2,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("tpu7x-standard-1t", 56, 248).
				withInstancePriceOverride(2.7),
			NewMachineTypeInfo("tpu7x-standard-4t", 224, 992).
				withInstancePriceOverride(10.8),
			NewMachineTypeInfo("tpu7x-ultranet-4t", 224, 992).
				withInstancePriceOverride(11.8).
				withExplicitReqOnly(),
		),
		supportedTpuTypes: map[string]bool{
			labels.Tpu7xValue: true,
		},
		supportedCpuPlatforms: CpuPlatformRequirements{
			lowerBound: IntelEmeraldRapids,
			upperBound: IntelEmeraldRapids,
		},
		supportedBootDiskTypes: map[string]bool{
			DiskTypeHyperdiskBalanced: true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced:   NonConfidentialOnlyMode,
			DiskTypeHyperdiskExtreme:    NonConfidentialOnlyMode,
			DiskTypeHyperdiskThroughput: UnspecifiedMode,
			DiskTypeHyperdiskMl:         UnspecifiedMode,
		},
		defaultDiskType:          DiskTypeHyperdiskBalanced,
		supportHugepageSize1g:    true,
		supportsAcceleratorSlice: true,
	})
	// TPU7 represents tpu7 machine family
	TPU7 = RegisterMachineFamily(MachineFamily{
		name:               "tpu7",
		systemArchitecture: gce.Amd64,
		// For TPU, cpu and memory are not billed, the price is only based on TPU chips.
		// CpuPricePerHour is calculated by dividing total VM price by the cpu count.
		// TODO(b/432451286): update the prices once finalized.
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.12,
			MemoryPricePerHourPerGb: 0,
			PreemptibleDiscount:     0.6 / 1.2,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("tpu7-standard-1t", 56, 240).
				withInstancePriceOverride(2.7),
			NewMachineTypeInfo("tpu7-standard-4t", 224, 960).
				withInstancePriceOverride(10.8),
		),
		supportedTpuTypes: map[string]bool{
			labels.Tpu7Value: true,
		},
		supportedCpuPlatforms: CpuPlatformRequirements{
			lowerBound: IntelEmeraldRapids,
			upperBound: IntelEmeraldRapids,
		},
		supportedBootDiskTypes: map[string]bool{
			DiskTypeHyperdiskBalanced: true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced: NonConfidentialOnlyMode,
			DiskTypeHyperdiskExtreme:  NonConfidentialOnlyMode,
			DiskTypeBalanced:          NonConfidentialOnlyMode,
		},
		defaultDiskType:          DiskTypeHyperdiskBalanced,
		supportHugepageSize1g:    true,
		supportsAcceleratorSlice: true,
	})
	// Z3 represents z3 machine family
	Z3 = RegisterMachineFamily(MachineFamily{
		name:               "z3",
		systemArchitecture: gce.Amd64,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.0496531,
			MemoryPricePerHourPerGb: 0.0066553,
			PreemptibleDiscount:     0.01192 / 0.0496531,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("z3-highmem-8", 8, 64).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(1.145745).
				withPreemptibleInstancePriceOverride(0.290841),
			NewMachineTypeInfo("z3-highmem-14", 14, 112).
				withAutomaticEphemeralLocalSsdCount(2).
				withInstancePriceOverride(2.085698).
				withPreemptibleInstancePriceOverride(0.532258),
			NewMachineTypeInfo("z3-highmem-22", 22, 176).
				withAutomaticEphemeralLocalSsdCount(3).
				withInstancePriceOverride(3.231443).
				withPreemptibleInstancePriceOverride(0.823099),
			NewMachineTypeInfo("z3-highmem-30", 30, 240).
				withAutomaticEphemeralLocalSsdCount(4).
				withInstancePriceOverride(4.377188).
				withPreemptibleInstancePriceOverride(1.113941),
			NewMachineTypeInfo("z3-highmem-36", 36, 288).
				withAutomaticEphemeralLocalSsdCount(5).
				withInstancePriceOverride(5.317141).
				withPreemptibleInstancePriceOverride(1.355358),
			NewMachineTypeInfo("z3-highmem-44", 44, 352).
				withAutomaticEphemeralLocalSsdCount(6).
				withInstancePriceOverride(6.462886).
				withPreemptibleInstancePriceOverride(1.646199),
			NewMachineTypeInfo("z3-highmem-88", 88, 704).
				withAutomaticEphemeralLocalSsdCount(12).
				withInstancePriceOverride(12.925772).
				withPreemptibleInstancePriceOverride(3.292398),
			NewMachineTypeInfo("z3-highmem-176", 176, 1408).
				withAutomaticEphemeralLocalSsdCount(12).
				withInstancePriceOverride(21.980576).
				withPreemptibleInstancePriceOverride(5.467054),
			NewMachineTypeInfo("z3-highmem-8-highlssd", 8, 64).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(1.145745).
				withPreemptibleInstancePriceOverride(0.290841),
			NewMachineTypeInfo("z3-highmem-16-highlssd", 16, 128).
				withAutomaticEphemeralLocalSsdCount(2).
				withInstancePriceOverride(2.291489).
				withPreemptibleInstancePriceOverride(0.581682),
			NewMachineTypeInfo("z3-highmem-22-highlssd", 22, 176).
				withAutomaticEphemeralLocalSsdCount(3).
				withInstancePriceOverride(3.231443).
				withPreemptibleInstancePriceOverride(0.823099),
			NewMachineTypeInfo("z3-highmem-32-highlssd", 32, 256).
				withAutomaticEphemeralLocalSsdCount(4).
				withInstancePriceOverride(4.582979).
				withPreemptibleInstancePriceOverride(1.163365),
			NewMachineTypeInfo("z3-highmem-44-highlssd", 44, 352).
				withAutomaticEphemeralLocalSsdCount(6).
				withInstancePriceOverride(6.462886).
				withPreemptibleInstancePriceOverride(1.646199),
			NewMachineTypeInfo("z3-highmem-88-highlssd", 88, 704).
				withAutomaticEphemeralLocalSsdCount(12).
				withInstancePriceOverride(12.925772).
				withPreemptibleInstancePriceOverride(3.292398),
			NewMachineTypeInfo("z3-highmem-14-standardlssd", 14, 112).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(1.763118).
				withPreemptibleInstancePriceOverride(0.439113),
			NewMachineTypeInfo("z3-highmem-22-standardlssd", 22, 176).
				withAutomaticEphemeralLocalSsdCount(2).
				withInstancePriceOverride(2.908862).
				withPreemptibleInstancePriceOverride(0.729954),
			NewMachineTypeInfo("z3-highmem-44-standardlssd", 44, 352).
				withAutomaticEphemeralLocalSsdCount(3).
				withInstancePriceOverride(5.495144).
				withPreemptibleInstancePriceOverride(1.366763),
			NewMachineTypeInfo("z3-highmem-88-standardlssd", 88, 704).
				withAutomaticEphemeralLocalSsdCount(6).
				withInstancePriceOverride(10.990288).
				withPreemptibleInstancePriceOverride(2.733527),
			NewMachineTypeInfo("z3-highmem-176-standardlssd", 176, 1408).
				withAutomaticEphemeralLocalSsdCount(12).
				withInstancePriceOverride(21.980576).
				withPreemptibleInstancePriceOverride(5.467054),
		),
		supportedCpuPlatforms:    CpuPlatformRequirements{lowerBound: IntelSapphireRapids, upperBound: IntelSapphireRapids},
		supportCompactPlacement:  false,
		supportConfidentialNodes: false,
		supportedBootDiskTypes: map[string]bool{
			DiskTypeHyperdiskBalanced: true,
			DiskTypeBalanced:          true,
			DiskTypeSSD:               true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskExtreme:                  NonConfidentialOnlyMode,
			DiskTypeHyperdiskThroughput:               UnspecifiedMode,
			DiskTypeHyperdiskBalanced:                 NonConfidentialOnlyMode,
			DiskTypeHyperdiskBalancedHighAvailability: NonConfidentialOnlyMode,
			DiskTypeBalanced:                          UnspecifiedMode,
			DiskTypeSSD:                               UnspecifiedMode,
		},
		supportHugepageSize1g: true,
		defaultDiskType:       DiskTypeBalanced,
	})

	// Z4D represents z4d machine family
	Z4D = RegisterMachineFamily(MachineFamily{
		name:               "z4d",
		systemArchitecture: gce.Amd64,
		// TODO(b/521945608): Remove these hardcoded placeholder prices once Z4D SKUs in Ohara are finalized.
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:         0.00875,
			MemoryPricePerHourPerGb: 0.01,
			PreemptibleDiscount:     0.0035 / 0.00875,
		},
		autoprovisionedMachineTypes: onboardMachineType(
			NewMachineTypeInfo("z4d-highmem-8-standardlssd", 8, 63).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(1.086344).
				withPreemptibleInstancePriceOverride(0.421366),
			NewMachineTypeInfo("z4d-highmem-16-standardlssd", 16, 126).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(1.796344).
				withPreemptibleInstancePriceOverride(0.705366),
			NewMachineTypeInfo("z4d-highmem-32-standardlssd", 32, 252).
				withAutomaticEphemeralLocalSsdCount(2).
				withInstancePriceOverride(3.592688).
				withPreemptibleInstancePriceOverride(1.410731),
			NewMachineTypeInfo("z4d-highmem-48-standardlssd", 48, 378).
				withAutomaticEphemeralLocalSsdCount(3).
				withInstancePriceOverride(5.389032).
				withPreemptibleInstancePriceOverride(2.116097),
			NewMachineTypeInfo("z4d-highmem-64-standardlssd", 64, 504).
				withAutomaticEphemeralLocalSsdCount(4).
				withInstancePriceOverride(7.185376).
				withPreemptibleInstancePriceOverride(2.821462),
			NewMachineTypeInfo("z4d-highmem-96-standardlssd", 96, 756).
				withAutomaticEphemeralLocalSsdCount(6).
				withInstancePriceOverride(10.778065).
				withPreemptibleInstancePriceOverride(4.232194),
			NewMachineTypeInfo("z4d-highmem-192-standardlssd", 192, 1512).
				withAutomaticEphemeralLocalSsdCount(12).
				withInstancePriceOverride(21.556129).
				withPreemptibleInstancePriceOverride(8.464387),
			NewMachineTypeInfo("z4d-highmem-384-standardlssd", 384, 3024).
				withAutomaticEphemeralLocalSsdCount(24).
				withInstancePriceOverride(43.112258).
				withPreemptibleInstancePriceOverride(16.928774),
			NewMachineTypeInfo("z4d-highmem-8-highlssd", 8, 63).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(1.086344).
				withPreemptibleInstancePriceOverride(0.421366),
			NewMachineTypeInfo("z4d-highmem-16-highlssd", 16, 126).
				withAutomaticEphemeralLocalSsdCount(2).
				withInstancePriceOverride(2.172688).
				withPreemptibleInstancePriceOverride(0.842731),
			NewMachineTypeInfo("z4d-highmem-32-highlssd", 32, 252).
				withAutomaticEphemeralLocalSsdCount(4).
				withInstancePriceOverride(4.345376).
				withPreemptibleInstancePriceOverride(1.685462),
			NewMachineTypeInfo("z4d-highmem-48-highlssd", 48, 378).
				withAutomaticEphemeralLocalSsdCount(6).
				withInstancePriceOverride(6.518065).
				withPreemptibleInstancePriceOverride(2.528194),
			NewMachineTypeInfo("z4d-highmem-64-highlssd", 64, 504).
				withAutomaticEphemeralLocalSsdCount(8).
				withInstancePriceOverride(8.690753).
				withPreemptibleInstancePriceOverride(3.370925),
			NewMachineTypeInfo("z4d-highmem-96-highlssd", 96, 756).
				withAutomaticEphemeralLocalSsdCount(12).
				withInstancePriceOverride(13.036129).
				withPreemptibleInstancePriceOverride(5.056387),
			NewMachineTypeInfo("z4d-highmem-192-highlssd", 192, 1512).
				withAutomaticEphemeralLocalSsdCount(24).
				withInstancePriceOverride(26.072258).
				withPreemptibleInstancePriceOverride(10.112774),
			NewMachineTypeInfo("z4d-8t-standard-16-standardlssd", 16, 62).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(1.156344).
				withPreemptibleInstancePriceOverride(0.449366),
			NewMachineTypeInfo("z4d-8t-standard-32-standardlssd", 32, 124).
				withAutomaticEphemeralLocalSsdCount(2).
				withInstancePriceOverride(2.312688).
				withPreemptibleInstancePriceOverride(0.898731),
			NewMachineTypeInfo("z4d-8t-standard-48-standardlssd", 48, 186).
				withAutomaticEphemeralLocalSsdCount(3).
				withInstancePriceOverride(3.469032).
				withPreemptibleInstancePriceOverride(1.348097),
			NewMachineTypeInfo("z4d-8t-standard-64-standardlssd", 64, 248).
				withAutomaticEphemeralLocalSsdCount(4).
				withInstancePriceOverride(4.625376).
				withPreemptibleInstancePriceOverride(1.797462),
			NewMachineTypeInfo("z4d-8t-standard-96-standardlssd", 96, 372).
				withAutomaticEphemeralLocalSsdCount(6).
				withInstancePriceOverride(6.938065).
				withPreemptibleInstancePriceOverride(2.696194),
			NewMachineTypeInfo("z4d-8t-standard-192-standardlssd", 192, 744).
				withAutomaticEphemeralLocalSsdCount(12).
				withInstancePriceOverride(13.876129).
				withPreemptibleInstancePriceOverride(5.392387),
			NewMachineTypeInfo("z4d-4t-standard-16-standardlssd", 16, 62).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(1.156344).
				withPreemptibleInstancePriceOverride(0.449366),
			NewMachineTypeInfo("z4d-4t-standard-32-standardlssd", 32, 124).
				withAutomaticEphemeralLocalSsdCount(2).
				withInstancePriceOverride(2.312688).
				withPreemptibleInstancePriceOverride(0.898731),
			NewMachineTypeInfo("z4d-4t-standard-48-standardlssd", 48, 186).
				withAutomaticEphemeralLocalSsdCount(3).
				withInstancePriceOverride(3.469032).
				withPreemptibleInstancePriceOverride(1.348097),
			NewMachineTypeInfo("z4d-4t-standard-64-standardlssd", 64, 248).
				withAutomaticEphemeralLocalSsdCount(4).
				withInstancePriceOverride(4.625376).
				withPreemptibleInstancePriceOverride(1.797462),
			NewMachineTypeInfo("z4d-4t-standard-96-standardlssd", 96, 372).
				withAutomaticEphemeralLocalSsdCount(6).
				withInstancePriceOverride(6.938065).
				withPreemptibleInstancePriceOverride(2.696194),
			NewMachineTypeInfo("z4d-4t-standard-192-standardlssd", 192, 744).
				withAutomaticEphemeralLocalSsdCount(12).
				withInstancePriceOverride(13.876129).
				withPreemptibleInstancePriceOverride(5.392387),
			NewMachineTypeInfo("z4d-standard-16-standardlssd", 16, 62).
				withAutomaticEphemeralLocalSsdCount(1).
				withInstancePriceOverride(1.156344).
				withPreemptibleInstancePriceOverride(0.449366),
			NewMachineTypeInfo("z4d-standard-32-standardlssd", 32, 124).
				withAutomaticEphemeralLocalSsdCount(2).
				withInstancePriceOverride(2.312688).
				withPreemptibleInstancePriceOverride(0.898731),
			NewMachineTypeInfo("z4d-standard-48-standardlssd", 48, 186).
				withAutomaticEphemeralLocalSsdCount(3).
				withInstancePriceOverride(3.469032).
				withPreemptibleInstancePriceOverride(1.348097),
			NewMachineTypeInfo("z4d-standard-64-standardlssd", 64, 248).
				withAutomaticEphemeralLocalSsdCount(4).
				withInstancePriceOverride(4.625376).
				withPreemptibleInstancePriceOverride(1.797462),
			NewMachineTypeInfo("z4d-standard-96-standardlssd", 96, 372).
				withAutomaticEphemeralLocalSsdCount(6).
				withInstancePriceOverride(6.938065).
				withPreemptibleInstancePriceOverride(2.696194),
			NewMachineTypeInfo("z4d-standard-192-standardlssd", 192, 744).
				withAutomaticEphemeralLocalSsdCount(12).
				withInstancePriceOverride(13.876129).
				withPreemptibleInstancePriceOverride(5.392387),
		),
		supportedCpuPlatforms:    CpuPlatformRequirements{lowerBound: AmdTurin, upperBound: AmdTurin},
		supportCompactPlacement:  true,
		maxCompactPlacementNodes: 150,
		supportConfidentialNodes: false,
		supportedBootDiskTypes: map[string]bool{
			DiskTypeHyperdiskBalanced:                 true,
			DiskTypeHyperdiskBalancedHighAvailability: true,
			DiskTypeHyperdiskThroughput:               true,
			DiskTypeHyperdiskMl:                       true,
		},
		supportedAttachDiskTypes: map[string]ConfidentialMode{
			DiskTypeHyperdiskBalanced:                 UnspecifiedMode,
			DiskTypeHyperdiskBalancedHighAvailability: UnspecifiedMode,
			DiskTypeHyperdiskExtreme:                  UnspecifiedMode,
			DiskTypeHyperdiskMl:                       NonConfidentialOnlyMode,
			DiskTypeHyperdiskThroughput:               UnspecifiedMode,
		},
		supportHugepageSize1g: true,
		defaultDiskType:       DiskTypeHyperdiskBalanced,
	})
)

// LocalSSDDiskSizes are mappings between machine type/family to the local disk sizes in GiB
var LocalSSDDiskSizes = map[string]uint64{
	Z3.Name():                    3000,
	Z4D.Name():                   3500,
	A4X.Name():                   3000,
	"c4-standard-288-lssd-metal": 3000,
	"c4-highmem-288-lssd-metal":  3000,
}

func pInt64(i int64) *int64 {
	t1 := i
	return &t1
}

func onboardSupportedGpus(gpus ...Gpu) map[string]Gpu {
	m := make(map[string]Gpu, len(gpus))
	for _, g := range gpus {
		m[g.Name()] = g
	}
	return m
}

func onboardMachineType(mT ...MachineType) map[string]MachineType {
	mp := make(map[string]MachineType, len(mT))
	for _, m := range mT {
		mp[m.Name] = m
	}
	return mp
}
