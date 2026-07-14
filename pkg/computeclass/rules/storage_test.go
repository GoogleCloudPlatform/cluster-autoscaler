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

package rules

import (
	"fmt"
	"testing"

	ccc_api "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
)

func TestStorageRuleMatchesNodeGroup(t *testing.T) {
	defaultMachineFamily := machinetypes.E2
	defaultMachineFamilyName := defaultMachineFamily.Name()

	nonDefaultMachineFamilyName := machinetypes.N2.Name()
	nonDefaultMachineType := fmt.Sprintf("%s-standard-8", nonDefaultMachineFamilyName)

	defaultBootDiskType := "default-boot-disk"
	nonDefaultBootDiskType := "non-default-boot-disk"
	defaultBootDiskSize := 10
	nonDefaultBootDiskSize := 20
	defaultLocalSSDcount := 1
	nonDefaultLocalSSDcount := 2
	defaultBootDiskKmsKey := "default-boot-disk-kms"
	nonDefaultBootDiskKmsKey := "non-default-boot-disk-kms"

	project1 := "project1"
	mode1 := "CONTAINER_IMAGE_CACHE"
	diskImage1 := "disk1"
	project2 := "project2"
	mode2 := "MODE_UNSPECIFIED"
	diskImage2 := "disk2"
	gkeApiSecondaryBootDisk1 := &gke_api_beta.SecondaryBootDisk{
		DiskImage: fmt.Sprintf("projects/%s/global/images/%s", project1, "disk1"),
		Mode:      mode1,
	}
	gkeApiSecondaryBootDisk2 := &gke_api_beta.SecondaryBootDisk{
		DiskImage: fmt.Sprintf("projects/%s/global/images/%s", project2, "disk2"),
		Mode:      mode2,
	}
	gkeApiSecondaryBootDisk3 := &gke_api_beta.SecondaryBootDisk{
		DiskImage: fmt.Sprintf("projects/%s/global/images/%s", project2, "disk2"),
		Mode:      mode1,
	}

	testCases := []struct {
		name      string
		nodegroup cloudprovider.NodeGroup
		rule      StorageRule
		expected  bool
	}{
		{
			name:      "rule with boot disk type, node group without boot disk type - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithStorageRule(&defaultBootDiskType, nil, nil, nil),
			),
			expected: false,
		},
		{
			name:      "rule with boot disk type, node group with different boot disk type - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, DiskType: nonDefaultBootDiskType}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithStorageRule(&defaultBootDiskType, nil, nil, nil),
			),
			expected: false,
		},
		{
			name:      "rule without boot disk type, node group with boot disk type - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, DiskType: defaultBootDiskType}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
			),
			expected: true,
		},
		{
			name:      "rule and node group with same boot disk type - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, DiskType: defaultBootDiskType}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithStorageRule(&defaultBootDiskType, nil, nil, nil),
			),
			expected: true,
		},
		{
			name:      "rule with boot disk encryption key, node group without boot disk encryption key - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithStorageRule(nil, nil, &defaultBootDiskKmsKey, nil),
			),
			expected: false,
		},
		{
			name:      "rule with boot disk encryption key, node group with different boot disk encryption key - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, DiskEncryptionKey: nonDefaultBootDiskKmsKey}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithStorageRule(nil, nil, &defaultBootDiskKmsKey, nil),
			),
			expected: false,
		},
		{
			name:      "rule without boot disk encryption key, node group with boot disk encryption key - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, DiskEncryptionKey: defaultBootDiskKmsKey}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
			),
			expected: true,
		},
		{
			name:      "rule and node group with same boot disk encryption key - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, DiskEncryptionKey: defaultBootDiskKmsKey}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithStorageRule(nil, nil, &defaultBootDiskKmsKey, nil),
			),
			expected: true,
		},
		{
			name:      "rule with boot disk size, node group without boot disk size - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithStorageRule(nil, &defaultBootDiskSize, nil, nil),
			),
			expected: false,
		},
		{
			name:      "rule with boot disk size, node group with different boot disk size - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, DiskSize: int64(nonDefaultBootDiskSize)}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithStorageRule(nil, &defaultBootDiskSize, nil, nil),
			),
			expected: false,
		},
		{
			name:      "rule without boot disk size, node group with boot disk size - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, DiskSize: int64(defaultBootDiskSize)}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
			),
			expected: true,
		},
		{
			name:      "rule and node group with same boot disk size - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, DiskSize: int64(defaultBootDiskSize)}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithStorageRule(nil, &defaultBootDiskSize, nil, nil),
			),
			expected: true,
		},
		{
			name:      "rule with local ssd count, node group without local ssd count - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithStorageRule(nil, nil, nil, &defaultLocalSSDcount),
			),
			expected: false,
		},
		{
			name: "rule with local ssd count, node group with empty LocalSSDConfig - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType,
				LocalSSDConfig: &gkeclient.LocalSSDConfig{}}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithStorageRule(nil, nil, nil, &defaultLocalSSDcount),
			),
			expected: false,
		},
		{
			name: "rule with local ssd count, node group with empty EphemeralStorageConfig- no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType,
				LocalSSDConfig: &gkeclient.LocalSSDConfig{
					EphemeralStorageConfig: &gke_api_beta.EphemeralStorageConfig{},
				}}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithStorageRule(nil, nil, nil, &defaultLocalSSDcount),
			),
			expected: false,
		},
		{
			name: "rule with local ssd count, node group with different local ssd count - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType,
				LocalSSDConfig: &gkeclient.LocalSSDConfig{
					EphemeralStorageConfig: &gke_api_beta.EphemeralStorageConfig{
						LocalSsdCount: int64(nonDefaultLocalSSDcount),
					},
				}}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithStorageRule(nil, nil, nil, &defaultLocalSSDcount),
			),
			expected: false,
		},
		{
			name: "rule without local ssd count, node group with local ssd count - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType,
				LocalSSDConfig: &gkeclient.LocalSSDConfig{
					EphemeralStorageConfig: &gke_api_beta.EphemeralStorageConfig{
						LocalSsdCount: int64(nonDefaultLocalSSDcount),
					},
				}}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
			),
			expected: true,
		},
		{
			name: "rule and node group with same local ssd count - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType,
				LocalSSDConfig: &gkeclient.LocalSSDConfig{
					EphemeralStorageConfig: &gke_api_beta.EphemeralStorageConfig{
						LocalSsdCount: int64(defaultLocalSSDcount),
					},
				}}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithStorageRule(nil, nil, nil, &defaultLocalSSDcount),
			),
			expected: true,
		},
		{
			name: "rule and node group with default storage options - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: fmt.Sprintf("%s-standard-8", defaultMachineFamilyName),
				DiskSize:          int64(defaultBootDiskSize),
				DiskType:          defaultBootDiskType,
				DiskEncryptionKey: defaultBootDiskKmsKey,
				LocalSSDConfig: &gkeclient.LocalSSDConfig{
					EphemeralStorageConfig: &gke_api_beta.EphemeralStorageConfig{
						LocalSsdCount: int64(defaultLocalSSDcount),
					},
				}}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&defaultMachineFamilyName),
				WithStorageRule(&defaultBootDiskType, &defaultBootDiskSize, &defaultBootDiskKmsKey, &defaultLocalSSDcount),
			),
			expected: true,
		},
		{
			name:      "rule with secondary boot disks, node group without secondary boot disks - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSecondaryBootDiskRule(diskImage1, project1, mode1), //disk1
				WithSecondaryBootDiskRule(diskImage2, project2, mode2), //disk2
			),
			expected: false,
		},
		{
			name: "rule with secondary boot disks, node group with secondary boot disks - no matching, the slices are completely different",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				SecondaryBootDisks: []*gke_api_beta.SecondaryBootDisk{
					gkeApiSecondaryBootDisk2,
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSecondaryBootDiskRule(diskImage1, project1, mode1), // disk1
			),
			expected: false,
		},
		{
			name: "rule with secondary boot disks, node group with secondary boot disks - no matching, rule has one additional boot disk that node group does not",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				SecondaryBootDisks: []*gke_api_beta.SecondaryBootDisk{
					gkeApiSecondaryBootDisk2,
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSecondaryBootDiskRule(diskImage1, project1, mode1), // disk1
				WithSecondaryBootDiskRule(diskImage2, project2, mode2), // disk2
			),
			expected: false,
		},
		{
			name: "rule with secondary boot disks, node group with secondary boot disks - no matching, node group has one additional boot disk that rule does not",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				SecondaryBootDisks: []*gke_api_beta.SecondaryBootDisk{
					gkeApiSecondaryBootDisk1,
					gkeApiSecondaryBootDisk2,
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSecondaryBootDiskRule(diskImage1, project1, mode1), // disk1
			),
			expected: false,
		},
		{
			name: "rule with secondary boot disks, node group with secondary boot disks - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				SecondaryBootDisks: []*gke_api_beta.SecondaryBootDisk{
					gkeApiSecondaryBootDisk1,
					gkeApiSecondaryBootDisk2,
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSecondaryBootDiskRule(diskImage1, project1, mode1), // disk1
				WithSecondaryBootDiskRule(diskImage2, project2, mode2), // disk2
			),
			expected: true,
		},
		{
			name: "rule with secondary boot disks, node group with secondary boot disks - matching, the order should not matter, 2 elements slices",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				SecondaryBootDisks: []*gke_api_beta.SecondaryBootDisk{
					gkeApiSecondaryBootDisk2,
					gkeApiSecondaryBootDisk1,
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSecondaryBootDiskRule(diskImage1, project1, mode1), // disk1
				WithSecondaryBootDiskRule(diskImage2, project2, mode2), // disk2
			),
			expected: true,
		},
		{
			name: "rule with secondary boot disks, node group with secondary boot disks - matching, the order should not matter, 3 elements slices",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				SecondaryBootDisks: []*gke_api_beta.SecondaryBootDisk{
					gkeApiSecondaryBootDisk2,
					gkeApiSecondaryBootDisk1,
					gkeApiSecondaryBootDisk3,
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSecondaryBootDiskRule(diskImage2, project2, mode1), // disk3
				WithSecondaryBootDiskRule(diskImage1, project1, mode1), // disk1
				WithSecondaryBootDiskRule(diskImage2, project2, mode2), // disk2
			),
			expected: true,
		},
		{
			name: "rule with secondary boot disk and storage, the order of rule configuration should not matter",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:       nonDefaultMachineType,
				DiskEncryptionKey: defaultBootDiskKmsKey,
				SecondaryBootDisks: []*gke_api_beta.SecondaryBootDisk{
					gkeApiSecondaryBootDisk1,
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithSecondaryBootDiskRule(diskImage1, project1, mode1), // disk1
				WithStorageRule(nil, nil, &defaultBootDiskKmsKey, nil),
			),
			expected: true,
		},
		{
			name: "rule with storage and secondary boot disk, the order of rule configuration should not matter",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:       nonDefaultMachineType,
				DiskEncryptionKey: defaultBootDiskKmsKey,
				SecondaryBootDisks: []*gke_api_beta.SecondaryBootDisk{
					gkeApiSecondaryBootDisk1,
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithStorageRule(nil, nil, &defaultBootDiskKmsKey, nil),
				WithSecondaryBootDiskRule(diskImage1, project1, mode1), // disk1
			),
			expected: true,
		},
		{
			name: "rule with storage local ssd, node group with ephemeral storage and swap lssd - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LocalSSDConfig: &gkeclient.LocalSSDConfig{
					EphemeralStorageLocalSsdConfig: &gke_api_beta.EphemeralStorageLocalSsdConfig{
						LocalSsdCount: 1,
					},
				},
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						DedicatedLocalSsdProfile: &gkeclient.DedicatedLocalSsdProfile{
							DiskCount: 1,
						},
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithStorageRule(nil, nil, nil, &defaultLocalSSDcount),
				WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					DedicatedLocalSsdProfile: &ccc_api.SwapConfigDedicatedLocalSsdProfile{
						DiskCount: 1,
					},
				}),
			),
			expected: true,
		},
		{
			name: "rule with storage local ssd, node group with ephemeral storage and swap lssd - not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
				LocalSSDConfig: &gkeclient.LocalSSDConfig{
					EphemeralStorageLocalSsdConfig: &gke_api_beta.EphemeralStorageLocalSsdConfig{
						LocalSsdCount: 2,
					},
				},
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						DedicatedLocalSsdProfile: &gkeclient.DedicatedLocalSsdProfile{
							DiskCount: 1,
						},
					},
				},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithStorageRule(nil, nil, nil, &defaultLocalSSDcount),
				WithSwapConfigRule(ccc_api.SwapConfig{
					Enabled: true,
					DedicatedLocalSsdProfile: &ccc_api.SwapConfigDedicatedLocalSsdProfile{
						DiskCount: 1,
					},
				}),
			),
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.rule.Matches(tc.nodegroup)
			if actual != tc.expected {
				t.Errorf("Test: \"%v\" failed, expected matching: %v got: %v", tc.name, tc.expected, actual)
			}
		})
	}
}
