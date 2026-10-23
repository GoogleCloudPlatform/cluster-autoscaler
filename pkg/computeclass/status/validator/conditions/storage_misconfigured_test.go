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

package conditions

import (
	"fmt"
	"testing"

	ccc_api "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce"
	gkelocalssdsize "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/localssdsize"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/utils/ptr"
)

func TestValidateStorageConfig(t *testing.T) {
	existingMachineFamily := "N2"
	existingMachineType := "n2-standard-4"
	nonExistingCpuRequirementForGpu := 30
	existingCpuCores := 4
	invalidBootDiskTypeN2 := "hyperdisk-balanced"
	validBootDiskTypeN2 := "pd-ssd"
	validLocalSSDCountN2 := 24
	invalidLocalSSDCountN2 := 48
	n4Family := "n4"
	z3HighMem88 := "z3-highmem-88"
	z3Family := "z3"
	c4Standard96 := "c4-standard-96"
	two := 2
	fourDisks := 4
	a4xHighGpu4g := "a4x-highgpu-4g"
	a4xFamily := "a4x"
	g2Standard4 := "g2-standard-4"
	g2Family := "g2"
	a3HighGpu8g := "a3-highgpu-8g"
	c3Standard8Lssd := "c3-standard-8-lssd"
	c3Family := "c3"

	customMachineType2 := "n2d-custom-8-65536"
	customMachineType5 := "n2-custom-8-65536"

	zoneA := "zone-a"
	zoneB := "zone-b"
	zoneC := "zone-c"

	validMachineTypes := []gce.MachineTypeKey{
		{
			MachineTypeName: customMachineType2,
			Zone:            zoneB,
		},
		{
			MachineTypeName: customMachineType5,
			Zone:            zoneB,
		},
		{
			MachineTypeName: existingMachineType,
			Zone:            zoneA,
		},
		{
			MachineTypeName: z3HighMem88,
			Zone:            zoneA,
		},
		{
			MachineTypeName: c4Standard96,
			Zone:            zoneA,
		},
		{
			MachineTypeName: a4xHighGpu4g,
			Zone:            zoneA,
		},
		{
			MachineTypeName: g2Standard4,
			Zone:            zoneA,
		},
		{
			MachineTypeName: a3HighGpu8g,
			Zone:            zoneA,
		},
		{
			MachineTypeName: c3Standard8Lssd,
			Zone:            zoneA,
		},
	}

	testCases := []struct {
		name        string
		rule        rules.Rule
		wantReason  string
		wantMessage string
	}{
		{
			name: "disk type incompatible with machine type",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType), rules.WithStorageRule(&invalidBootDiskTypeN2, nil, nil, nil)),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(DiskTypeNotSupportedWithMachineTypeMessage, invalidBootDiskTypeN2, existingMachineType),
		},
		{
			name: "disk type compatible with machine type",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType), rules.WithStorageRule(&validBootDiskTypeN2, nil, nil, nil)),
		},
		{
			name: "disk type incompatible with custom machine type",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&customMachineType5), rules.WithStorageRule(&invalidBootDiskTypeN2, nil, nil, nil)),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(DiskTypeNotSupportedWithMachineTypeMessage, invalidBootDiskTypeN2, customMachineType5),
		},
		{
			name: "disk type compatible with custom machine type",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&customMachineType5), rules.WithStorageRule(&validBootDiskTypeN2, nil, nil, nil)),
		},
		{
			name: "disk type incompatible with machine family",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&existingMachineFamily), rules.WithStorageRule(&invalidBootDiskTypeN2, nil, nil, nil)),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(DiskTypeNotSupportedWithMachineFamilyMessage, invalidBootDiskTypeN2, existingMachineFamily),
		},
		{
			name: "disk type compatible with machine family",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&existingMachineFamily), rules.WithStorageRule(&validBootDiskTypeN2, nil, nil, nil)),
		},
		{
			name: "N2 supports atleast one machine with 24 local SSD",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&existingMachineFamily), rules.WithStorageRule(&validBootDiskTypeN2, nil, nil, &validLocalSSDCountN2)),
		},
		{
			name: "no machines in N2 with 48 local SSD",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&existingMachineFamily), rules.WithStorageRule(&validBootDiskTypeN2, nil, nil, &invalidLocalSSDCountN2)),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDNotSupportedWithMachineFamilyMessage, invalidLocalSSDCountN2, existingMachineFamily),
		},
		{
			name: "n4 machine family doesn't support any local SSD",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&n4Family), rules.WithStorageRule(nil, nil, nil, &validLocalSSDCountN2)),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDNotSupportedWithMachineFamilyMessage, validLocalSSDCountN2, n4Family),
		},
		{
			name: "n2-standard-4 supports 24 local SSD",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType), rules.WithStorageRule(&validBootDiskTypeN2, nil, nil, &validLocalSSDCountN2)),
		},
		{
			name: "n2-standard-4 doesn't support 48 local SSD",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType), rules.WithStorageRule(&validBootDiskTypeN2, nil, nil, &invalidLocalSSDCountN2)),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDNotSupportedWithMachineTypeMessage, invalidLocalSSDCountN2, existingMachineType) + " " + fmt.Sprintf(LocalSSDAllowedCountsMessage, "1, 2, 4, 8, 16, 24"),
		},
		{
			name: "z3-highmem-88 only support 12 ssd, required 13",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&z3HighMem88), rules.WithStorageRule(&validBootDiskTypeN2, nil, nil, &invalidLocalSSDCountN2)),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDNotSupportedWithMachineTypeMessage, invalidLocalSSDCountN2, z3HighMem88) + " " + fmt.Sprintf(LocalSSDAllowedCountsMessage, "12"),
		},
		{
			name: "c4-standard-96 doesn't support any local SSD",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&c4Standard96), rules.WithStorageRule(nil, nil, nil, &invalidLocalSSDCountN2)),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDNotSupportedWithMachineTypeMessage, invalidLocalSSDCountN2, c4Standard96),
		},
		{
			name: "n2-custom-8-65536 doesn't support any local SSD",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&customMachineType5), rules.WithStorageRule(nil, nil, nil, &validLocalSSDCountN2)),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDNotSupportedWithMachineTypeMessage, validLocalSSDCountN2, customMachineType5),
		},
		{
			name: "e2d-custom-8-65536 doesn't support any local SSD",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&customMachineType2), rules.WithStorageRule(nil, nil, nil, &invalidLocalSSDCountN2)),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDNotSupportedWithMachineTypeMessage, invalidLocalSSDCountN2, customMachineType2),
		},
		{
			name: "N2 supports atleast one machine with 2 local SSD count with 4 cores requirement",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&existingMachineFamily),
				rules.WithStorageRule(&validBootDiskTypeN2, nil, nil, &two),
				rules.WithMinCoresRule(&existingCpuCores)),
		},
		{
			name: "N2 doesn't support any machine with 2 local SSD count and 30 cores",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&existingMachineFamily),
				rules.WithStorageRule(&validBootDiskTypeN2, nil, nil, &two),
				rules.WithMinCoresRule(&nonExistingCpuRequirementForGpu)),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDIncompatibleWithCpuMessage, existingMachineFamily, two, nonExistingCpuRequirementForGpu),
		},
		{
			name: "pure block mode on n4 machine family which has no local SSD support",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&n4Family),
				rules.WithLocalNvmeSsdBlockConfigRule(nil),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDNotSupportedByMachineFamilyMessage, n4Family),
		},
		{
			name: "family rule with block mode and no capacity reports no condition",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&existingMachineFamily),
				rules.WithLocalNvmeSsdBlockConfigRule(ptr.To(4)),
			),
		},
		{
			name: "pure block mode with omitted count on n2 family requires local SSD count",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&existingMachineFamily),
				rules.WithLocalNvmeSsdBlockConfigRule(nil),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDCountRequiredForMachineFamilyMessage, existingMachineFamily),
		},
		{
			name: "mixed mode with omitted count on n2 family requires local SSD count",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&existingMachineFamily),
				rules.WithEphemeralStorageLocalSsdConfigRule(nil, ptr.To[int64](100)),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDCountRequiredForMachineFamilyMessage, existingMachineFamily),
		},
		{
			name: "mixed mode with capacity equal to max allowed SSD capacity in N2",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&existingMachineFamily),
				rules.WithEphemeralStorageLocalSsdConfigRule(ptr.To(24), ptr.To[int64](24*375)),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDCapacityExceedsMachineFamilyLimitMessage, int64(24*375), int64(24*375), existingMachineFamily),
		},
		{
			name: "pure block mode with omitted count on z3-highmem-88 is valid",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&z3HighMem88),
				rules.WithLocalNvmeSsdBlockConfigRule(nil),
			),
		},
		{
			name: "mixed mode with valid capacity on z3-highmem-88 is valid",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&z3HighMem88),
				rules.WithEphemeralStorageLocalSsdConfigRule(nil, ptr.To[int64](500)),
			),
		},
		{
			name: "mixed mode with capacity exceeding limit on z3-highmem-88",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&z3HighMem88),
				rules.WithEphemeralStorageLocalSsdConfigRule(nil, ptr.To[int64](12*3000)),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDCapacityExceedsMachineTypeLimitMessage, int64(12*3000), int64(12*3000), z3HighMem88),
		},
		{
			name: "ephemeralStorageLocalSsdConfig Enabled-only on z3-highmem-88 is valid",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&z3HighMem88),
				rules.WithEphemeralStorageLocalSsdConfigRule(nil, nil),
			),
		},
		{
			name: "pure block mode Enabled-only on z3-highmem-88 is valid",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&z3HighMem88),
				rules.WithLocalNvmeSsdBlockConfigRule(nil),
			),
		},
		{
			name: "ephemeralStorageLocalSsdConfig Enabled-only on z3 family is valid",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&z3Family),
				rules.WithEphemeralStorageLocalSsdConfigRule(nil, nil),
			),
		},
		{
			name: "pure block mode Enabled-only on z3 family is valid",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&z3Family),
				rules.WithLocalNvmeSsdBlockConfigRule(nil),
			),
		},
		{
			// c3-standard-176-lssd is the largest c3 type with Local SSD.
			name: "omitted count on c3 family with minCores 176 is valid",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&c3Family),
				rules.WithLocalNvmeSsdBlockConfigRule(nil),
				rules.WithMinCoresRule(ptr.To(176)),
			),
		},
		{
			// Only non-LSSD c3 types (e.g. c3-standard-192-metal) have >= 180 cores.
			name: "pure block mode with omitted count on c3 family with minCores 180",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&c3Family),
				rules.WithLocalNvmeSsdBlockConfigRule(nil),
				rules.WithMinCoresRule(ptr.To(180)),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDCountOmittedIncompatibleWithCpuMessage, c3Family, 180),
		},
		{
			name: "mixed mode with omitted count on c3 family with minCores 180",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&c3Family),
				rules.WithEphemeralStorageLocalSsdConfigRule(nil, ptr.To[int64](100)),
				rules.WithMinCoresRule(ptr.To(180)),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDCountOmittedIncompatibleWithCpuMessage, c3Family, 180),
		},
		{
			name: "ephemeralStorageLocalSsdConfig Enabled-only on n2 family requires local SSD count",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&existingMachineFamily),
				rules.WithEphemeralStorageLocalSsdConfigRule(nil, nil),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDCountRequiredForMachineFamilyMessage, existingMachineFamily),
		},
		{
			name: "pure block mode Enabled-only on n2 family requires local SSD count",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&existingMachineFamily),
				rules.WithLocalNvmeSsdBlockConfigRule(nil),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDCountRequiredForMachineFamilyMessage, existingMachineFamily),
		},
		{
			name: "ephemeralStorageLocalSsdConfig Enabled-only on n2-standard-4 requires local SSD count",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType),
				rules.WithEphemeralStorageLocalSsdConfigRule(nil, nil),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDCountRequiredForMachineTypeMessage, existingMachineType),
		},
		{
			name: "pure block mode Enabled-only on n2-standard-4 requires local SSD count",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType),
				rules.WithLocalNvmeSsdBlockConfigRule(nil),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDCountRequiredForMachineTypeMessage, existingMachineType),
		},
		{
			name: "ephemeralStorageLocalSsdConfig Enabled with valid count on n2-standard-4 is valid",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType),
				rules.WithEphemeralStorageLocalSsdConfigRule(ptr.To(4), nil),
			),
		},
		{
			name: "pure block mode Enabled with valid count on n2-standard-4 is valid",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType),
				rules.WithLocalNvmeSsdBlockConfigRule(ptr.To(4)),
			),
		},
		// Mixed mode with 4 disks: GKE sizes kube-reserved from the
		// partition, so small capacities are valid.
		{
			name: "mixed mode on machine type with small capacity is valid",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType),
				rules.WithEphemeralStorageLocalSsdConfigRule(&fourDisks, ptr.To[int64](50)),
			),
		},
		{
			name: "mixed mode on machine family with small capacity is valid",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&existingMachineFamily),
				rules.WithEphemeralStorageLocalSsdConfigRule(&fourDisks, ptr.To[int64](50)),
			),
		},
		// A4X partition-size tests (proves 375 is not used:
		// a4x partition is 3000, 4*3000 = 12000).
		{
			name: "mixed mode with valid capacity (2000GiB) on a4x-highgpu-4g machine type",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&a4xHighGpu4g),
				rules.WithEphemeralStorageLocalSsdConfigRule(nil, ptr.To[int64](2000)),
			),
		},
		{
			name: "mixed mode with capacity exceeding limit (12000GiB) on a4x-highgpu-4g machine type",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&a4xHighGpu4g),
				rules.WithEphemeralStorageLocalSsdConfigRule(nil, ptr.To[int64](12000)),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDCapacityExceedsMachineTypeLimitMessage, 12000, 12000, a4xHighGpu4g),
		},
		{
			name: "mixed mode with valid capacity (2000GiB) on a4x machine family",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&a4xFamily),
				rules.WithEphemeralStorageLocalSsdConfigRule(nil, ptr.To[int64](2000)),
			),
		},
		{
			name: "mixed mode with capacity exceeding limit (12000GiB) on a4x machine family",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&a4xFamily),
				rules.WithEphemeralStorageLocalSsdConfigRule(nil, ptr.To[int64](12000)),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDCapacityExceedsMachineFamilyLimitMessage, 12000, 12000, a4xFamily),
		},
		// Automatic LSSD machine type (c3-standard-8-lssd) accepts omitted count
		{
			name: "mixed mode with omitted count on c3-standard-8-lssd machine type is valid",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&c3Standard8Lssd),
				rules.WithEphemeralStorageLocalSsdConfigRule(nil, ptr.To[int64](200)),
			),
		},
		{
			name: "pure block mode with omitted count on c3-standard-8-lssd machine type is valid",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&c3Standard8Lssd),
				rules.WithLocalNvmeSsdBlockConfigRule(nil),
			),
		},
		// Single-allowed configurable machine type (g2-standard-4 / g2)
		// requires explicit count.
		{
			name: "ephemeralStorageLocalSsdConfig Enabled-only on g2-standard-4 machine type requires local SSD count",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&g2Standard4),
				rules.WithEphemeralStorageLocalSsdConfigRule(nil, nil),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDCountRequiredForMachineTypeMessage, g2Standard4),
		},
		{
			name: "mixed mode with omitted count on g2-standard-4 machine type requires local SSD count",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&g2Standard4),
				rules.WithEphemeralStorageLocalSsdConfigRule(nil, ptr.To[int64](200)),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDCountRequiredForMachineTypeMessage, g2Standard4),
		},
		{
			name: "pure block mode with omitted count on g2-standard-4 machine type requires local SSD count",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&g2Standard4),
				rules.WithLocalNvmeSsdBlockConfigRule(nil),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDCountRequiredForMachineTypeMessage, g2Standard4),
		},
		{
			name: "ephemeralStorageLocalSsdConfig Enabled-only on g2 machine family requires local SSD count",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&g2Family),
				rules.WithEphemeralStorageLocalSsdConfigRule(nil, nil),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDCountRequiredForMachineFamilyMessage, g2Family),
		},
		{
			name: "mixed mode with omitted count on g2 machine family requires local SSD count",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&g2Family),
				rules.WithEphemeralStorageLocalSsdConfigRule(nil, ptr.To[int64](200)),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDCountRequiredForMachineFamilyMessage, g2Family),
		},
		{
			name: "pure block mode with omitted count on g2 machine family requires local SSD count",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&g2Family),
				rules.WithLocalNvmeSsdBlockConfigRule(nil),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDCountRequiredForMachineFamilyMessage, g2Family),
		},
		{
			name: "ephemeralStorageLocalSsdConfig with explicit count 1 on g2-standard-4 machine type is valid",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&g2Standard4),
				rules.WithEphemeralStorageLocalSsdConfigRule(ptr.To(1), nil),
			),
		},
		{
			name: "mixed mode with explicit count 1 on g2-standard-4 machine type is valid",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&g2Standard4),
				rules.WithEphemeralStorageLocalSsdConfigRule(ptr.To(1), ptr.To[int64](200)),
			),
		},
		{
			name: "pure block mode with explicit count 1 on g2-standard-4 machine type is valid",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&g2Standard4),
				rules.WithLocalNvmeSsdBlockConfigRule(ptr.To(1)),
			),
		},
		// Mixed mode + Swap profile tests
		{
			name: "mixed mode with dedicated local SSD swap profile errors",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType),
				rules.WithEphemeralStorageLocalSsdConfigRule(&fourDisks, ptr.To[int64](500)),
				rules.WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					DedicatedLocalSsdProfile: &ccc_api.SwapConfigDedicatedLocalSsdProfile{
						DiskCount: 1,
					},
				}),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: DedicatedLocalSsdSwapWithMixedModeMessage,
		},
		{
			name: "mixed mode with boot-disk swap profile is valid",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType),
				rules.WithEphemeralStorageLocalSsdConfigRule(&fourDisks, ptr.To[int64](500)),
				rules.WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					BootDiskProfile: &ccc_api.SwapConfigBootDiskProfile{
						SwapSizeGib: ptr.To(int64(10)),
					},
				}),
			),
		},
		{
			name: "mixed mode with ephemeral local SSD swap profile is valid",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType),
				rules.WithEphemeralStorageLocalSsdConfigRule(&fourDisks, ptr.To[int64](500)),
				rules.WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					EphemeralLocalSsdProfile: &ccc_api.SwapConfigEphemeralLocalSsdProfile{
						SwapSizeGib: ptr.To(int64(10)),
					},
				}),
			),
		},
		// Ephemeral mode + Swap profile tests
		{
			name: "ephemeral storage config with omitted count and dedicated swap 2 on a3-highgpu-8g is valid",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&a3HighGpu8g),
				rules.WithEphemeralStorageLocalSsdConfigRule(nil, nil),
				rules.WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					DedicatedLocalSsdProfile: &ccc_api.SwapConfigDedicatedLocalSsdProfile{
						DiskCount: 2,
					},
				}),
			),
		},
		{
			name: "ephemeral storage config with explicit 14 and dedicated swap 2 on a3-highgpu-8g is valid",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&a3HighGpu8g),
				rules.WithEphemeralStorageLocalSsdConfigRule(ptr.To(14), nil),
				rules.WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					DedicatedLocalSsdProfile: &ccc_api.SwapConfigDedicatedLocalSsdProfile{
						DiskCount: 2,
					},
				}),
			),
		},
		{
			name: "ephemeral storage config with explicit 16 and dedicated swap 2 on a3-highgpu-8g is not supported",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&a3HighGpu8g),
				rules.WithEphemeralStorageLocalSsdConfigRule(ptr.To(16), nil),
				rules.WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					DedicatedLocalSsdProfile: &ccc_api.SwapConfigDedicatedLocalSsdProfile{
						DiskCount: 2,
					},
				}),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDNotSupportedWithMachineTypeMessage, 18, a3HighGpu8g) + " " + fmt.Sprintf(LocalSSDAllowedCountsMessage, "16"),
		},
		{
			name: "ephemeral storage config with explicit 2 and dedicated swap 2 on configurable machine is valid",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType),
				rules.WithEphemeralStorageLocalSsdConfigRule(ptr.To(2), nil),
				rules.WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					DedicatedLocalSsdProfile: &ccc_api.SwapConfigDedicatedLocalSsdProfile{
						DiskCount: 2,
					},
				}),
			),
		},
		{
			name: "ephemeral storage config with omitted count and dedicated swap on configurable machine requires count",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType),
				rules.WithEphemeralStorageLocalSsdConfigRule(nil, nil),
				rules.WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					DedicatedLocalSsdProfile: &ccc_api.SwapConfigDedicatedLocalSsdProfile{
						DiskCount: 2,
					},
				}),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(LocalSSDCountRequiredForMachineTypeMessage, existingMachineType),
		},
		{
			name: "block mode with dedicated local SSD swap profile errors",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType),
				rules.WithLocalNvmeSsdBlockConfigRule(ptr.To(4)),
				rules.WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					DedicatedLocalSsdProfile: &ccc_api.SwapConfigDedicatedLocalSsdProfile{
						DiskCount: 1,
					},
				}),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: DedicatedLocalSsdSwapWithBlockModeMessage,
		},
		{
			name: "block mode with ephemeral local SSD swap profile errors",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType),
				rules.WithLocalNvmeSsdBlockConfigRule(ptr.To(4)),
				rules.WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					EphemeralLocalSsdProfile: &ccc_api.SwapConfigEphemeralLocalSsdProfile{
						SwapSizeGib: ptr.To(int64(10)),
					},
				}),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: EphemeralLocalSsdSwapWithBlockModeMessage,
		},
		{
			name: "block mode with boot-disk swap profile is valid",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType),
				rules.WithLocalNvmeSsdBlockConfigRule(ptr.To(4)),
				rules.WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					BootDiskProfile: &ccc_api.SwapConfigBootDiskProfile{
						SwapSizeGib: ptr.To(int64(10)),
					},
				}),
			),
		},
		{
			name: "ephemeral storage config with omitted count on fixed machine where swap equals fixed count is not supported",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&c3Standard8Lssd),
				rules.WithEphemeralStorageLocalSsdConfigRule(nil, nil),
				rules.WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					DedicatedLocalSsdProfile: &ccc_api.SwapConfigDedicatedLocalSsdProfile{
						DiskCount: 2,
					},
				}),
			),
			wantReason:  NoSuitableMachineExistsReason,
			wantMessage: fmt.Sprintf(DedicatedLocalSsdSwapConsumesAllLocalSSDsMessage, 2, c3Standard8Lssd, 2),
		},
		{
			name: "machine family with dedicated local SSD swap profile only is valid",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&existingMachineFamily),
				rules.WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					DedicatedLocalSsdProfile: &ccc_api.SwapConfigDedicatedLocalSsdProfile{
						DiskCount: 2,
					},
				}),
			),
		},
		{
			name: "machine type with dedicated local SSD swap profile only is valid",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType),
				rules.WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					DedicatedLocalSsdProfile: &ccc_api.SwapConfigDedicatedLocalSsdProfile{
						DiskCount: 2,
					},
				}),
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := newTestProvider().
				WithAutoprovisioningLocations(zoneA, zoneB, zoneC).
				WithValidMachineTypes(validMachineTypes).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			checker := &storageConfigChecker{provider: provider, localSsdProvider: gkelocalssdsize.NewDynamicLocalSSDDiskSizeProvider(machinetypes.LocalSSDDiskSizes)}
			condition := checker.checkRule(tc.rule)
			if tc.wantReason != "" {
				assert.NotNil(t, condition)
				assert.Equal(t, RuleMisconfiguredCondition, condition.Type)
				assert.Equal(t, tc.wantReason, condition.Reason)
				assert.Equal(t, tc.wantMessage, condition.Message)
			} else {
				assert.Nil(t, condition)
			}
		})
	}
}

func TestValidateStorageConfig_MixedModeNodeVersion(t *testing.T) {
	c3Standard8Lssd := "c3-standard-8-lssd"
	n2Standard4 := "n2-standard-4"
	fourDisks := 4
	zoneA := "zone-a"

	validMachineTypes := []gce.MachineTypeKey{
		{MachineTypeName: c3Standard8Lssd, Zone: zoneA},
		{MachineTypeName: n2Standard4, Zone: zoneA},
	}

	mixedModeRule := rules.NewRule(
		rules.WithMachineTypeRule(&c3Standard8Lssd),
		rules.WithEphemeralStorageLocalSsdConfigRule(nil, ptr.To[int64](200)),
	)
	blockModeRule := rules.NewRule(
		rules.WithMachineTypeRule(&c3Standard8Lssd),
		rules.WithLocalNvmeSsdBlockConfigRule(nil),
	)
	plainEphemeralRule := rules.NewRule(
		rules.WithMachineTypeRule(&c3Standard8Lssd),
		rules.WithEphemeralStorageLocalSsdConfigRule(nil, nil),
	)
	legacyLocalSSDRule := rules.NewRule(
		rules.WithMachineTypeRule(&n2Standard4),
		rules.WithStorageRule(nil, nil, nil, &fourDisks),
	)

	testCases := []struct {
		name        string
		rule        rules.Rule
		nodeVersion string
		wantFlagged bool
	}{
		{
			name:        "mixed mode with 1.36.5-gke.100 is flagged",
			rule:        mixedModeRule,
			nodeVersion: "1.36.5-gke.100",
			wantFlagged: true,
		},
		{
			name:        "mixed mode with 1.37.0-gke.3508000 is flagged",
			rule:        mixedModeRule,
			nodeVersion: "1.37.0-gke.3508000",
			wantFlagged: true,
		},
		{
			name:        "mixed mode with 1.37.0-gke.3509000 is valid",
			rule:        mixedModeRule,
			nodeVersion: "1.37.0-gke.3509000",
		},
		{
			name:        "mixed mode with minor alias 1.36 is flagged",
			rule:        mixedModeRule,
			nodeVersion: "1.36",
			wantFlagged: true,
		},
		{
			name:        "mixed mode with minor alias 1.37 is valid",
			rule:        mixedModeRule,
			nodeVersion: "1.37",
		},
		{
			name:        "mixed mode with patch alias 1.36.5 is flagged",
			rule:        mixedModeRule,
			nodeVersion: "1.36.5",
			wantFlagged: true,
		},
		{
			name:        "mixed mode with patch alias 1.37.0 is valid",
			rule:        mixedModeRule,
			nodeVersion: "1.37.0",
		},
		{
			name:        "mixed mode with latest alias is valid",
			rule:        mixedModeRule,
			nodeVersion: "latest",
		},
		{
			name:        "mixed mode with unparseable nodeVersion is not flagged",
			rule:        mixedModeRule,
			nodeVersion: "not-a-version",
		},
		{
			name: "mixed mode with no nodeVersion is valid",
			rule: mixedModeRule,
		},
		{
			name:        "block mode with old nodeVersion is valid",
			rule:        blockModeRule,
			nodeVersion: "1.36.5-gke.100",
		},
		{
			name:        "plain ephemeral mode with old nodeVersion is valid",
			rule:        plainEphemeralRule,
			nodeVersion: "1.36.5-gke.100",
		},
		{
			name:        "legacy localSSDCount with old nodeVersion is valid",
			rule:        legacyLocalSSDRule,
			nodeVersion: "1.36.5-gke.100",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := newTestProvider().
				WithAutoprovisioningLocations(zoneA).
				WithValidMachineTypes(validMachineTypes).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			checker := &storageConfigChecker{
				provider:         provider,
				localSsdProvider: gkelocalssdsize.NewDynamicLocalSSDDiskSizeProvider(machinetypes.LocalSSDDiskSizes),
			}
			condition := checker.checkRuleWithNodeVersion(tc.rule, tc.nodeVersion)
			if tc.wantFlagged {
				assert.NotNil(t, condition)
				assert.Equal(t, RuleMisconfiguredCondition, condition.Type)
				assert.Equal(t, NoSuitableMachineExistsReason, condition.Reason)
				assert.Equal(t, fmt.Sprintf(MixedModeLocalSSDUnsupportedNodeVersionMessage, mixedModeLocalSSDMinNodeVersion, tc.nodeVersion), condition.Message)
			} else {
				assert.Nil(t, condition)
			}
		})
	}
}
