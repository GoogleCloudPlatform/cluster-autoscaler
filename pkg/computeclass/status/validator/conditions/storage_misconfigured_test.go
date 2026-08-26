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

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
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
	c4Standard96 := "c4-standard-96"
	two := 2

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
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := newTestProvider().
				WithAutoprovisioningLocations(zoneA, zoneB, zoneC).
				WithValidMachineTypes(validMachineTypes).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			checker := &storageConfigChecker{provider: provider}
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
