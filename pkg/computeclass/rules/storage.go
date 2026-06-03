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

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/klog/v2"
)

const (
	diskImageTemplate = "projects/%s/global/images/%s"

	// Defined in http://google3/cloud/kubernetes/engine/common/constants.go;l=103;rcl=737468291
	DefaultBootDiskSizeGb = 100
)

// StorageRule is an interface for rules with storage.
type StorageRule interface {
	BaseRule
	BootDiskType() string
	BootDiskSize() int64
	BootDiskKMSKey() string
	EphemerslStorageLSSDCount() int
	TotalLSSDCount() int64
	SecondaryBootDisks() []*gke_api_beta.SecondaryBootDisk
}

type storageRule struct {
	bootDiskType              *string
	bootDiskSize              *int
	bootDiskKMSKey            *string
	ephemeralStorageLSSDCount *int
	secondaryBootDisks        []*gke_api_beta.SecondaryBootDisk
}

// Matches returns true if the nodegroup is within one of the nodepools.
func (r *storageRule) Matches(nodeGroup cloudprovider.NodeGroup) bool {
	mig, ok := nodeGroup.(gkeNodeGroup)
	if !ok {
		klog.Errorf("expected GkeMig; got %v", nodeGroup)
		return false
	}

	if r.bootDiskType == nil &&
		r.bootDiskSize == nil &&
		r.bootDiskKMSKey == nil &&
		r.ephemeralStorageLSSDCount == nil &&
		len(r.secondaryBootDisks) == 0 {
		return true
	}

	if mig.Spec() == nil {
		return false
	}

	// Check for storage.
	if r.bootDiskType != nil && mig.Spec().DiskType != *r.bootDiskType {
		return false
	}
	if r.bootDiskSize != nil && mig.Spec().DiskSize != int64(*r.bootDiskSize) {
		return false
	}
	if r.bootDiskKMSKey != nil && mig.Spec().DiskEncryptionKey != *r.bootDiskKMSKey {
		return false
	}
	if r.ephemeralStorageLSSDCount != nil && migEphemeralStorageLSSDCount(mig) != int64(*r.ephemeralStorageLSSDCount) {
		return false
	}

	// Check for secondary boot disk.
	if len(r.secondaryBootDisks) > 0 {
		if !cmp.Equal(mig.Spec().SecondaryBootDisks, r.secondaryBootDisks,
			cmpopts.SortSlices(func(disk1, disk2 *gke_api_beta.SecondaryBootDisk) bool {
				if disk1.DiskImage != disk2.DiskImage {
					return disk1.DiskImage < disk2.DiskImage
				}
				return disk1.Mode < disk2.Mode
			})) {
			return false
		}
	}
	return true
}

func migEphemeralStorageLSSDCount(nodeGroup gkeNodeGroup) int64 {
	var count int64 = 0
	if nodeGroup.Spec().LocalSSDConfig != nil {
		if nodeGroup.Spec().LocalSSDConfig.EphemeralStorageLocalSsdConfig != nil {
			count += nodeGroup.Spec().LocalSSDConfig.EphemeralStorageLocalSsdConfig.LocalSsdCount
		}
		if nodeGroup.Spec().LocalSSDConfig.EphemeralStorageConfig != nil {
			count += nodeGroup.Spec().LocalSSDConfig.EphemeralStorageConfig.LocalSsdCount
		}
	}
	return count
}

// BootDiskType returns the type of boot disk of rule.
func (r *storageRule) BootDiskType() string {
	if r.bootDiskType == nil {
		return ""
	}
	return *r.bootDiskType
}

// BootDiskSize returns the size of boot disk of rule.
func (r *storageRule) BootDiskSize() int64 {
	if r.bootDiskSize == nil {
		return 0
	}
	return int64(*r.bootDiskSize)
}

// BootDiskKMSKey returns boot disk encryption key of rule.
func (r *storageRule) BootDiskKMSKey() string {
	if r.bootDiskKMSKey == nil {
		return ""
	}
	return *r.bootDiskKMSKey
}

func (r *storageRule) EphemerslStorageLSSDCount() int {
	if r.ephemeralStorageLSSDCount == nil {
		return 0
	}
	return *r.ephemeralStorageLSSDCount
}

func (r *rule) TotalLSSDCount() int64 {
	var count int64
	if r.storageRule.ephemeralStorageLSSDCount != nil {
		count += int64(*r.storageRule.ephemeralStorageLSSDCount)
	}
	count += r.nodeSystemConfigRule.SwapDedicatedLSSDCount()
	return count
}

// SecondaryBootDisks returns secondary disks slice collection
func (r *storageRule) SecondaryBootDisks() []*gke_api_beta.SecondaryBootDisk {
	return r.secondaryBootDisks
}

// WithStorageRule returns RuleOption setting basic StorageRule.
func WithStorageRule(bootDiskType *string, bootDiskSize *int, bootDiskKMSKey *string, ephemeralStorageLSSDCount *int) RuleOption {
	return func(r *rule) {
		r.storageRule.bootDiskType = bootDiskType
		r.storageRule.bootDiskSize = bootDiskSize
		r.storageRule.bootDiskKMSKey = bootDiskKMSKey
		r.storageRule.ephemeralStorageLSSDCount = ephemeralStorageLSSDCount
	}
}

// WithSecondaryBootDiskRule returns RuleOption adding secondary boot disk to StorageRule.
func WithSecondaryBootDiskRule(diskImageName string, project string, mode string) RuleOption {
	return func(r *rule) {
		gkeApiSecondaryBootDisk := GenerateGkeApiSecondaryBootDisk(diskImageName, project, mode)
		r.storageRule.secondaryBootDisks = append(r.storageRule.secondaryBootDisks, gkeApiSecondaryBootDisk)
	}
}

// GenerateGkeApiSecondaryBootDisk returns gke_api_beta.SecondaryBootDisk pointer
// whose DiskImage field is equal to "projects/{project}/global/images/{diskImageName}" and
// Mode field is equal to mode
func GenerateGkeApiSecondaryBootDisk(diskImageName string, project string, mode string) *gke_api_beta.SecondaryBootDisk {
	diskImage := fmt.Sprintf(diskImageTemplate, project, diskImageName)
	gkeApisecondaryBootDisk := &gke_api_beta.SecondaryBootDisk{
		DiskImage: diskImage,
		Mode:      mode,
	}

	return gkeApisecondaryBootDisk
}
