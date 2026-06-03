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
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

func TestValidateNodeSystemConfig(t *testing.T) {
	existingMachineType := "n2-standard-4"
	n4Family := "n4"
	z3HighMem88 := "z3-highmem-88"
	c3Standard4 := "c3-standard-4"
	n1HighCpu8 := "n1-highcpu-8"
	n1Standard16 := "n1-standard-16"
	c3dFamily := "c3d"
	one := 1
	validBootDiskTypeN2 := "pd-ssd"
	validBootDiskSize := 50
	validLocalSSDCountN2 := 24
	supportedCpuCFSQuotaPeriod := "100ms"
	unsupportedCpuCFSQuotaPeriod := "2s"
	ssdProvider := localssdsize.NewSimpleLocalSSDProvider()

	customMachineType1 := "custom-4-32768"
	customMachineType2 := "n2d-custom-8-65536"
	customMachineType3 := "e2-custom-16-131072"
	customMachineType5 := "n2-custom-8-65536"
	customMachineType6 := "n4-custom-4-16384"

	zoneA := "zone-a"
	zoneB := "zone-b"
	zoneC := "zone-c"

	validMachineTypes := []gce.MachineTypeKey{
		{
			MachineTypeName: customMachineType1,
			Zone:            zoneA,
		},
		{
			MachineTypeName: customMachineType2,
			Zone:            zoneB,
		},
		{
			MachineTypeName: customMachineType3,
			Zone:            zoneB,
		},
		{
			MachineTypeName: customMachineType3,
			Zone:            zoneC,
		},
		{
			MachineTypeName: customMachineType5,
			Zone:            zoneB,
		},
		{
			MachineTypeName: customMachineType6,
			Zone:            zoneA,
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
			MachineTypeName: c3Standard4,
			Zone:            zoneA,
		},
		{
			MachineTypeName: n1HighCpu8,
			Zone:            zoneA,
		},
		{
			MachineTypeName: n1Standard16,
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
			name: "invalid sysctl config - tcp_rmem - wrong format",
			rule: rules.NewRule(
				rules.WithSysctlsRule(map[string]string{
					"net.ipv4.tcp_rmem": "123 456 abc",
				})),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(UnsupportedSysctlsFormatMessage, "net.ipv4.tcp_rmem", "123 456 abc", "3 numbers - 'min default max', with each being > 0 and min <= default <= max"),
		},
		{
			name: "invalid sysctl config - net.ipv4.tcp_wmem- wrong format",
			rule: rules.NewRule(
				rules.WithSysctlsRule(map[string]string{
					"net.ipv4.tcp_wmem": "1 1 0",
				})),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(UnsupportedSysctlsFormatMessage, "net.ipv4.tcp_wmem", "1 1 0", "3 numbers - 'min default max', with each being > 0 and min <= default <= max"),
		},
		{
			name: "invalid sysctl config - not following min <= default <= max",
			rule: rules.NewRule(
				rules.WithSysctlsRule(map[string]string{
					"net.ipv4.tcp_wmem": "3 1 2",
				})),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(UnsupportedSysctlsFormatMessage, "net.ipv4.tcp_wmem", "3 1 2", "3 numbers - 'min default max', with each being > 0 and min <= default <= max"),
		},
		{
			name: "invalid sysctl config kernel.shmmax",
			rule: rules.NewRule(
				rules.WithSysctlsRule(map[string]string{
					"kernel.shmmax": "18446744073692774400",
				})),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(UnsupportedSysctlsFormatMessage, "kernel.shmmax", "18446744073692774400", "an integer between 0 and 18446744073692774399"),
		},
		{
			name: "invalid sysctl config kernel.shmall",
			rule: rules.NewRule(
				rules.WithSysctlsRule(map[string]string{
					"kernel.shmall": "-1",
				})),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(UnsupportedSysctlsFormatMessage, "kernel.shmall", "-1", "an integer between 0 and 18446744073692774399"),
		},
		{
			name: "invalid machine type for sysctl vm.overcommit_memory",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&n1HighCpu8),
				rules.WithSysctlsRule(map[string]string{
					"vm.overcommit_memory": "2",
				})),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(UnsupportedSysctlsWithMachineMessage, "vm.overcommit_memory", "2"),
		},
		{
			name: "valid sysctl config for both",
			rule: rules.NewRule(
				rules.WithSysctlsRule(map[string]string{
					"net.ipv4.tcp_rmem": "4096 87380 16777216",
					"net.ipv4.tcp_wmem": "4096 65536 16777216",
				})),
		},
		{
			name: "invalid cpu cfs quota period",
			rule: rules.NewRule(
				rules.WithCpuCfsQuotaPeriodRule(unsupportedCpuCFSQuotaPeriod),
			),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(UnsupportedCpuCFSQuotaPeriodMessage, unsupportedCpuCFSQuotaPeriod),
		},
		{
			name: "invalid image minimum GC age",
			rule: rules.NewRule(
				rules.WithImageMinimumGcAgeRule("10m20s"),
			),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(UnsupportedImageMinimumGcAgeMessage, "10m20s"),
		},
		{
			name: "invalid image maximum GC age",
			rule: rules.NewRule(
				rules.WithImageMaximumGcAgeRule("10s"),
			),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(UnsupportedImageMaximumGcAgeMessage, "10s", "2m0s"),
		},
		{
			name: "valid image GC threshold and age",
			rule: rules.NewRule(
				rules.WithImageGcLowThresholdPercentRule(20),
				rules.WithImageGcHighThresholdPercentRule(85),
				rules.WithImageMinimumGcAgeRule("1m"),
				rules.WithImageMaximumGcAgeRule("1h100m"),
			),
		},
		{
			name: "valid cpu cfs quota period",
			rule: rules.NewRule(
				rules.WithCpuCfsQuotaPeriodRule(supportedCpuCFSQuotaPeriod),
			),
		},
		{
			name: "cpu cfs quota period not set - valid",
			rule: rules.NewRule(
				rules.WithCpuCfsQuotaRule(true),
			),
		},
		{
			name: "invalid container log max size",
			rule: rules.NewRule(
				rules.WithContainerLogMaxSizeRule("1Gi20Mi"),
			),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(InvalidContainerLogSizeMessage, "1Gi20Mi"),
		},
		{
			name: "invalid total container log size",
			rule: rules.NewRule(
				rules.WithContainerLogMaxSizeRule("400Mi"),
				rules.WithContainerLogMaxFilesRule(10),
			),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(InvalidContainerTotalLogSizeMessage, 3.91, 100, 100),
		},
		{
			name: "invalid total container log size with lssd count only",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType),
				rules.WithContainerLogMaxSizeRule("500Mi"),
				rules.WithContainerLogMaxFilesRule(10),
				rules.WithStorageRule(&validBootDiskTypeN2, nil, nil, &one),
			),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(InvalidContainerTotalLogSizeMessage, 4.88, 475, 100),
		},
		{
			name: "invalid total container log size with boot disk size only",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType),
				rules.WithContainerLogMaxSizeRule("500Mi"),
				rules.WithContainerLogMaxFilesRule(10),
				rules.WithStorageRule(&validBootDiskTypeN2, &validBootDiskSize, nil, nil),
			),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(InvalidContainerTotalLogSizeMessage, 4.88, 50, 100),
		},
		{
			name: "invalid total container log size with both boot disk size and lssd count",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType),
				rules.WithContainerLogMaxSizeRule("500Mi"),
				rules.WithContainerLogMaxFilesRule(10),
				rules.WithStorageRule(&validBootDiskTypeN2, &validBootDiskSize, nil, &one),
			),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(InvalidContainerTotalLogSizeMessage, 4.88, 425, 100),
		},
		{
			name: "valid total container log size with both boot disk size and lssd count",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType),
				rules.WithContainerLogMaxSizeRule("500Mi"),
				rules.WithContainerLogMaxFilesRule(10),
				rules.WithStorageRule(&validBootDiskTypeN2, &validBootDiskSize, nil, &validLocalSSDCountN2),
			),
		},
		{
			name: "invalid allowed unsafe sysctls",
			rule: rules.NewRule(
				rules.WithAllowedUnsafeSysctlsRule([]string{"netfilter", "net.netfilter", "vm.max_map_count"}),
			),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(UnsupportedAllowedUnsafeSysctlsMessage, "netfilter, vm.max_map_count"),
		},
		{
			name: "valid allowed unsafe sysctls",
			rule: rules.NewRule(
				rules.WithAllowedUnsafeSysctlsRule([]string{"kernel.shm*", "kernel.msg*", "kernel.sem", "fs.mqueue.*", "net.*"}),
			),
		},
		{
			name: "invalid machine family for 1-gigabyte-sized huge pages",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&n4Family),
				rules.WithHugepageSize1gRule(3),
			),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(UnsupportedMachineFamilyForHugepageSize1g, n4Family),
		},
		{
			name: "invalid default machine family for 1-gigabyte-sized huge pages",
			rule: rules.NewRule(
				rules.WithHugepageSize1gRule(3),
			),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(UnsupportedDefaultMachineFamilyForHugepageSize1g, machinetypes.E2.Name()),
		},
		{
			name: "invalid machine type for 1-gigabyte-sized huge pages",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&existingMachineType),
				rules.WithHugepageSize1gRule(3),
			),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(UnsupportedMachineTypeForHugepageSize1g, existingMachineType),
		},
		{
			name: "invalid custom machine type for 1-gigabyte-sized huge pages",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&customMachineType5),
				rules.WithHugepageSize1gRule(3),
			),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(UnsupportedMachineTypeForHugepageSize1g, customMachineType5),
		},
		{
			name: "valid machine family for 1-gigabyte-sized huge pages",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&c3dFamily),
				rules.WithHugepageSize1gRule(3),
			),
		},
		{
			name: "valid machine type for 1-gigabyte-sized huge pages",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&z3HighMem88),
				rules.WithHugepageSize1gRule(3),
			),
		},
		{
			name: "total hugepage size exceeds 60 percent capacity of machine memory in machine type",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&c3Standard4),
				rules.WithHugepageSize1gRule(6),
				rules.WithHugepageSize2mRule(2048),
			),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(TotalHugepagesExceedMemoryLimit, 0.6, 10240, c3Standard4, 9830),
		},
		{
			name: "total hugepage size exceeds 60 percent capacity of machine memory in custom machine type",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&customMachineType6),
				rules.WithHugepageSize2mRule(6144),
			),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(TotalHugepagesExceedMemoryLimit, 0.6, 12288, customMachineType6, 9830),
		},
		{
			name: "total hugepage size exceeds 80 percent capacity of machine memory on large machine types",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&z3HighMem88),
				rules.WithHugepageSize1gRule(600),
				rules.WithHugepageSize2mRule(10240),
			),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(TotalHugepagesExceedMemoryLimit, 0.8, 634880, z3HighMem88, 576716),
		},
		{
			name: "total hugepage size exceeds 80 percent capacity of machine memory in custom machine type",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&customMachineType2),
				rules.WithHugepageSize2mRule(26624),
			),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(TotalHugepagesExceedMemoryLimit, 0.8, 53248, customMachineType2, 52428),
		},
		{
			name: "total hugepage size exceeds 60 but not 80 percent capacity of machine memory on large machine type",
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&z3HighMem88),
				rules.WithHugepageSize1gRule(500),
				rules.WithHugepageSize2mRule(10240),
			),
		},
		{
			name: "total hugepage size exceeds 80 percent capacity of machine memory in all machine types in machine family",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&c3dFamily),
				rules.WithHugepageSize1gRule(6000),
				rules.WithHugepageSize2mRule(10240),
			),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(AllMachinesInMachineFamilyExceedHugepageMemoryLimit, 0.8, c3dFamily),
		},
		{
			name: "total hugepage size doesn't exceed 80 percent capacity of machine memory in all machine types in machine family",
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&c3dFamily),
				rules.WithHugepageSize1gRule(600),
				rules.WithHugepageSize2mRule(10240),
			),
		},
		{
			name: "total hugepage size exceeds 80 percent capacity of machine memory in all machine types in default family",
			rule: rules.NewRule(
				rules.WithHugepageSize2mRule(102400),
			),
			wantReason:  UnsupportedNodeSystemConfigFormatReason,
			wantMessage: fmt.Sprintf(AllMachinesInDefaultFamilyExceedHugepageMemoryLimit, 0.8, machinetypes.E2.Name()),
		},
		{
			name: "total hugepage size doesn't exceed 80 percent capacity of machine memory in all machine types in default family",
			rule: rules.NewRule(
				rules.WithHugepageSize2mRule(10240),
			),
		},
		{
			name: "invalid eviction soft memory.available too high for machine family",
			rule: rules.NewRule(
				rules.WithEvictionSoftMemoryAvailableRule("300Gi"),
				rules.WithEvictionSoftGracePeriodMemoryAvailableRule("30s"),
			),
			wantReason:  EvictionSoftMemoryTooHighReason,
			wantMessage: fmt.Sprintf(EvictionSoftMemoryTooHighMessage, "300Gi", "64Gi", "e2-standard-32"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := newTestProvider().
				WithAutoprovisioningLocations(zoneA, zoneB, zoneC).
				WithValidMachineTypes(validMachineTypes).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			checker := &nodeSystemConfigChecker{provider: provider, localSsdProvider: ssdProvider}
			condition := checker.checkRule(tc.rule)
			if tc.wantReason != "" {
				if assert.NotNil(t, condition) {
					assert.Equal(t, RuleMisconfiguredCondition, condition.Type)
					assert.Equal(t, tc.wantReason, condition.Reason)
					assert.Equal(t, tc.wantMessage, condition.Message)
				}
			} else {
				assert.Nil(t, condition)
			}
		})
	}
}
