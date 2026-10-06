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

	"github.com/google/go-cmp/cmp"
	ccc_api "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
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

	storagePool1 := "projects/test-project/zones/us-central1-a/storagePools/pool-1"
	storagePool2 := "projects/test-project/zones/us-central1-a/storagePools/pool-2"

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
			name: "rule with boot disk storage pools, node group with matching storage pools - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:  nonDefaultMachineType,
				StoragePools: []string{storagePool1, storagePool2},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithBootDiskStoragePoolsRule([]string{storagePool1, storagePool2}),
			),
			expected: true,
		},
		{
			name: "rule with boot disk storage pools in different order - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:  nonDefaultMachineType,
				StoragePools: []string{storagePool2, storagePool1},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithBootDiskStoragePoolsRule([]string{storagePool1, storagePool2}),
			),
			expected: true,
		},
		{
			name: "rule with boot disk storage pools, node group with different storage pools - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:  nonDefaultMachineType,
				StoragePools: []string{storagePool1},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithBootDiskStoragePoolsRule([]string{storagePool1, storagePool2}),
			),
			expected: false,
		},
		{
			name: "rule with boot disk storage pools, node group without storage pools - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: nonDefaultMachineType,
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithBootDiskStoragePoolsRule([]string{storagePool1}),
			),
			expected: false,
		},
		{
			name: "rule without boot disk storage pools, node group with storage pools - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType:  nonDefaultMachineType,
				StoragePools: []string{storagePool1},
			}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
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
		{
			name:      "rule and nodegroup matching boot disk provisioned iops and throughput",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, BootDiskProvisionedIops: 3000, BootDiskProvisionedThroughput: 140}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithBootDiskProvisionedIopsRule(3000),
				WithBootDiskProvisionedThroughputRule(140),
			),
			expected: true,
		},
		{
			name:      "rule and nodegroup mismatching boot disk provisioned iops",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, BootDiskProvisionedIops: 4000, BootDiskProvisionedThroughput: 140}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithBootDiskProvisionedIopsRule(3000),
				WithBootDiskProvisionedThroughputRule(140),
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

func TestStorageRuleGetters(t *testing.T) {
	pools := []string{"projects/p/zones/us-central1-a/storagePools/sp1"}
	r := NewRule(WithBootDiskStoragePoolsRule(pools))
	if diff := cmp.Diff(pools, r.BootDiskStoragePools()); diff != "" {
		t.Errorf("BootDiskStoragePools() mismatch (-want +got):\n%s", diff)
	}
}

func TestMatchSecondaryBootDisks(t *testing.T) {
	d1 := &gke_api_beta.SecondaryBootDisk{DiskImage: "image1", Mode: "MODE_A"}
	d1Copy := &gke_api_beta.SecondaryBootDisk{DiskImage: "image1", Mode: "MODE_A"}
	d2 := &gke_api_beta.SecondaryBootDisk{DiskImage: "image2", Mode: "MODE_B"}
	d3 := &gke_api_beta.SecondaryBootDisk{DiskImage: "image1", Mode: "MODE_B"} // same image, diff mode

	tests := []struct {
		name     string
		s1       []*gke_api_beta.SecondaryBootDisk
		s2       []*gke_api_beta.SecondaryBootDisk
		expected bool
	}{
		{
			name:     "both nil",
			s1:       nil,
			s2:       nil,
			expected: true,
		},
		{
			name:     "one nil one empty",
			s1:       nil,
			s2:       []*gke_api_beta.SecondaryBootDisk{},
			expected: true,
		},
		{
			name:     "different lengths",
			s1:       []*gke_api_beta.SecondaryBootDisk{d1},
			s2:       []*gke_api_beta.SecondaryBootDisk{d1, d2},
			expected: false,
		},
		{
			name:     "single element matching",
			s1:       []*gke_api_beta.SecondaryBootDisk{d1},
			s2:       []*gke_api_beta.SecondaryBootDisk{d1Copy},
			expected: true,
		},
		{
			name:     "single element mismatch image",
			s1:       []*gke_api_beta.SecondaryBootDisk{d1},
			s2:       []*gke_api_beta.SecondaryBootDisk{d2},
			expected: false,
		},
		{
			name:     "single element mismatch mode",
			s1:       []*gke_api_beta.SecondaryBootDisk{d1},
			s2:       []*gke_api_beta.SecondaryBootDisk{d3},
			expected: false,
		},
		{
			name:     "single element with nil in one",
			s1:       []*gke_api_beta.SecondaryBootDisk{d1},
			s2:       []*gke_api_beta.SecondaryBootDisk{nil},
			expected: false,
		},
		{
			name:     "single element both nil",
			s1:       []*gke_api_beta.SecondaryBootDisk{nil},
			s2:       []*gke_api_beta.SecondaryBootDisk{nil},
			expected: true,
		},
		{
			name:     "two elements matching in order",
			s1:       []*gke_api_beta.SecondaryBootDisk{d1, d2},
			s2:       []*gke_api_beta.SecondaryBootDisk{d1Copy, d2},
			expected: true,
		},
		{
			name:     "two elements permuted",
			s1:       []*gke_api_beta.SecondaryBootDisk{d1, d2},
			s2:       []*gke_api_beta.SecondaryBootDisk{d2, d1Copy},
			expected: true,
		},
		{
			name:     "two elements with duplicate mismatching",
			s1:       []*gke_api_beta.SecondaryBootDisk{d1, d1},
			s2:       []*gke_api_beta.SecondaryBootDisk{d1, d2},
			expected: false,
		},
		{
			name:     "two elements with nil matching",
			s1:       []*gke_api_beta.SecondaryBootDisk{d1, nil},
			s2:       []*gke_api_beta.SecondaryBootDisk{nil, d1Copy},
			expected: true,
		},
		{
			name:     "two elements with nil mismatching",
			s1:       []*gke_api_beta.SecondaryBootDisk{d1, nil},
			s2:       []*gke_api_beta.SecondaryBootDisk{d1, d2},
			expected: false,
		},
		{
			name:     "multiple elements permuted",
			s1:       []*gke_api_beta.SecondaryBootDisk{d1, d2, d3},
			s2:       []*gke_api_beta.SecondaryBootDisk{d3, d1, d2},
			expected: true,
		},
		{
			name:     "multiple elements with duplicate matching",
			s1:       []*gke_api_beta.SecondaryBootDisk{d1, d1Copy, d2},
			s2:       []*gke_api_beta.SecondaryBootDisk{d2, d1, d1},
			expected: true,
		},
		{
			name:     "multiple elements with duplicate mismatching",
			s1:       []*gke_api_beta.SecondaryBootDisk{d1, d1, d2},
			s2:       []*gke_api_beta.SecondaryBootDisk{d1, d2, d2},
			expected: false,
		},
		{
			name:     "multiple elements with nil matching",
			s1:       []*gke_api_beta.SecondaryBootDisk{d1, d2, nil},
			s2:       []*gke_api_beta.SecondaryBootDisk{nil, d2, d1Copy},
			expected: true,
		},
		{
			name:     "multiple elements with nil mismatching",
			s1:       []*gke_api_beta.SecondaryBootDisk{d1, d2, nil},
			s2:       []*gke_api_beta.SecondaryBootDisk{d1, d2, d3},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if actual := matchSecondaryBootDisks(tc.s1, tc.s2); actual != tc.expected {
				t.Errorf("matchSecondaryBootDisks(%v, %v) = %v; want %v", tc.s1, tc.s2, actual, tc.expected)
			}
		})
	}
}

func TestMatchStoragePools(t *testing.T) {
	tests := []struct {
		name     string
		s1       []string
		s2       []string
		expected bool
	}{
		{
			name:     "both nil",
			s1:       nil,
			s2:       nil,
			expected: true,
		},
		{
			name:     "nil and empty",
			s1:       nil,
			s2:       []string{},
			expected: true,
		},
		{
			name:     "different lengths",
			s1:       []string{"p1"},
			s2:       []string{"p1", "p2"},
			expected: false,
		},
		{
			name:     "single match",
			s1:       []string{"p1"},
			s2:       []string{"p1"},
			expected: true,
		},
		{
			name:     "single mismatch",
			s1:       []string{"p1"},
			s2:       []string{"p2"},
			expected: false,
		},
		{
			name:     "multiple permuted match",
			s1:       []string{"p1", "p2", "p3"},
			s2:       []string{"p3", "p1", "p2"},
			expected: true,
		},
		{
			name:     "duplicates mismatch",
			s1:       []string{"p1", "p1", "p2"},
			s2:       []string{"p1", "p2", "p2"},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if actual := matchStoragePools(tc.s1, tc.s2); actual != tc.expected {
				t.Errorf("matchStoragePools(%v, %v) = %v; want %v", tc.s1, tc.s2, actual, tc.expected)
			}
		})
	}
}
