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
	"sort"

	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
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
	BootDiskStoragePools() []string
	BootDiskProvisionedIops() int64
	BootDiskProvisionedThroughput() int64
}

type storageRule struct {
	bootDiskType                  *string
	bootDiskSize                  *int
	bootDiskKMSKey                *string
	ephemeralStorageLSSDCount     *int
	secondaryBootDisks            []*gke_api_beta.SecondaryBootDisk
	bootDiskStoragePools          []string
	bootDiskProvisionedIops       *int64
	bootDiskProvisionedThroughput *int64
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
		len(r.secondaryBootDisks) == 0 &&
		len(r.bootDiskStoragePools) == 0 &&
		r.bootDiskProvisionedIops == nil &&
		r.bootDiskProvisionedThroughput == nil {
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
	if r.bootDiskProvisionedIops != nil && mig.Spec().BootDiskProvisionedIops != *r.bootDiskProvisionedIops {
		return false
	}
	if r.bootDiskProvisionedThroughput != nil && mig.Spec().BootDiskProvisionedThroughput != *r.bootDiskProvisionedThroughput {
		return false
	}

	// Check for secondary boot disk.
	if len(r.secondaryBootDisks) > 0 {
		if !matchSecondaryBootDisks(mig.Spec().SecondaryBootDisks, r.secondaryBootDisks) {
			return false
		}
	}

	// Check for boot disk storage pools.
	if len(r.bootDiskStoragePools) > 0 {
		if !matchStoragePools(mig.Spec().StoragePools, r.bootDiskStoragePools) {
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

// BootDiskStoragePools returns boot disk storage pools slice collection
func (r *storageRule) BootDiskStoragePools() []string {
	return r.bootDiskStoragePools
}

// BootDiskProvisionedIops returns provisioned IOPS of rule.
func (r *storageRule) BootDiskProvisionedIops() int64 {
	if r.bootDiskProvisionedIops == nil {
		return 0
	}
	return *r.bootDiskProvisionedIops
}

// BootDiskProvisionedThroughput returns provisioned throughput of rule.
func (r *storageRule) BootDiskProvisionedThroughput() int64 {
	if r.bootDiskProvisionedThroughput == nil {
		return 0
	}
	return *r.bootDiskProvisionedThroughput
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

// WithBootDiskProvisionedIopsRule returns RuleOption setting BootDiskProvisionedIops.
func WithBootDiskProvisionedIopsRule(iops int64) RuleOption {
	return func(r *rule) {
		r.storageRule.bootDiskProvisionedIops = &iops
	}
}

// WithBootDiskProvisionedThroughputRule returns RuleOption setting BootDiskProvisionedThroughput.
func WithBootDiskProvisionedThroughputRule(throughput int64) RuleOption {
	return func(r *rule) {
		r.storageRule.bootDiskProvisionedThroughput = &throughput
	}
}

// WithSecondaryBootDiskRule returns RuleOption adding secondary boot disk to StorageRule.
func WithSecondaryBootDiskRule(diskImageName string, project string, mode string) RuleOption {
	return func(r *rule) {
		gkeApiSecondaryBootDisk := GenerateGkeApiSecondaryBootDisk(diskImageName, project, mode)
		r.storageRule.secondaryBootDisks = append(r.storageRule.secondaryBootDisks, gkeApiSecondaryBootDisk)
	}
}

// WithBootDiskStoragePoolsRule returns RuleOption setting bootDiskStoragePools to StorageRule.
func WithBootDiskStoragePoolsRule(storagePools []string) RuleOption {
	return func(r *rule) {
		r.storageRule.bootDiskStoragePools = storagePools
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

func equalSecondaryBootDisk(d1, d2 *gke_api_beta.SecondaryBootDisk) bool {
	if d1 == nil || d2 == nil {
		return d1 == d2
	}
	return d1.DiskImage == d2.DiskImage && d1.Mode == d2.Mode
}

// matchSecondaryBootDisks performs an order-independent comparison of two SecondaryBootDisk slices.
// We use a custom implementation instead of cmp.Equal with cmpopts.SortSlices because storageRule.Matches
// is called on the rule-matching hot path, and go-cmp relies on reflection and heap allocations that
// cause significant CPU and memory overhead.
// This implementation avoids allocations entirely for 0, 1, or 2 elements (covering ~90% of cases) and
// copies before sorting for >2 elements to avoid mutating the input slices.
func matchSecondaryBootDisks(disks1, disks2 []*gke_api_beta.SecondaryBootDisk) bool {
	if len(disks1) != len(disks2) {
		return false
	}
	switch len(disks1) {
	case 0:
		return true
	case 1:
		return equalSecondaryBootDisk(disks1[0], disks2[0])
	case 2:
		return (equalSecondaryBootDisk(disks1[0], disks2[0]) && equalSecondaryBootDisk(disks1[1], disks2[1])) ||
			(equalSecondaryBootDisk(disks1[0], disks2[1]) && equalSecondaryBootDisk(disks1[1], disks2[0]))
	}

	s1 := make([]*gke_api_beta.SecondaryBootDisk, len(disks1))
	s2 := make([]*gke_api_beta.SecondaryBootDisk, len(disks2))
	copy(s1, disks1)
	copy(s2, disks2)

	sortSecondaryBootDisks := func(slice []*gke_api_beta.SecondaryBootDisk) {
		sort.Slice(slice, func(i, j int) bool {
			if slice[i] == nil || slice[j] == nil {
				return slice[i] != nil
			}
			if slice[i].DiskImage != slice[j].DiskImage {
				return slice[i].DiskImage < slice[j].DiskImage
			}
			return slice[i].Mode < slice[j].Mode
		})
	}
	sortSecondaryBootDisks(s1)
	sortSecondaryBootDisks(s2)

	for i := range s1 {
		if !equalSecondaryBootDisk(s1[i], s2[i]) {
			return false
		}
	}
	return true
}

// matchStoragePools performs an order-independent comparison of two storage pool slices without
// the reflection and allocation overhead of cmp.Equal/cmpopts.SortSlices (see matchSecondaryBootDisks).
func matchStoragePools(pools1, pools2 []string) bool {
	if len(pools1) != len(pools2) {
		return false
	}
	if len(pools1) == 0 {
		return true
	}
	if len(pools1) == 1 {
		return pools1[0] == pools2[0]
	}
	s1 := make([]string, len(pools1))
	s2 := make([]string, len(pools2))
	copy(s1, pools1)
	copy(s2, pools2)
	sort.Strings(s1)
	sort.Strings(s2)
	for i := range s1 {
		if s1[i] != s2[i] {
			return false
		}
	}
	return true
}
