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
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/klog/v2"
)

const (
	imageMinimumGcAgeDefault                = 2 * time.Minute
	containerLogMaxSizeDefault              = "10Mi"
	containerLogMaxSizeUpperBound           = "500Mi"
	containerLogMaxFilesDefault             = 5
	overcommitSysctlMachineMemoryLowerBound = int64(15 * 1024 * 1024 * 1024) // 15 GiB
)

var (
	ErrInvalidPercentageFormat = errors.New("invalid percentage format")
	ErrPercentageOutOfRange    = errors.New("percentage out of range")
)

// nodeSystemConfigChecker checks if node system configuration is valid.
type nodeSystemConfigChecker struct {
	provider         CloudProvider
	localSsdProvider localssdsize.LocalSSDSizeProvider
}

func (ch *nodeSystemConfigChecker) checkRule(rule rules.Rule) *metav1.Condition {
	if rule.Sysctls() != nil {
		validFormat := "3 numbers - 'min default max', with each being > 0 and min <= default <= max"
		// Check for net.ipv4.tcp_rmem.
		if v, exists := rule.Sysctls()["net.ipv4.tcp_rmem"]; exists {
			if !correctFormatforMinDefaultMaxSysctl(v) {
				return SysctlsBadFormatCondition("net.ipv4.tcp_rmem", v, validFormat)
			}
		}
		// Check for net.ipv4.tcp_wmem.
		if v, exists := rule.Sysctls()["net.ipv4.tcp_wmem"]; exists {
			if !correctFormatforMinDefaultMaxSysctl(v) {
				return SysctlsBadFormatCondition("net.ipv4.tcp_wmem", v, validFormat)
			}
		}
		if v, exists := rule.Sysctls()["kernel.shmall"]; exists {
			if !correctMinMaxLimitSysctl(v, 0, 18446744073692774399) {
				return SysctlsBadFormatCondition("kernel.shmall", v, "an integer between 0 and 18446744073692774399")
			}
		}
		if v, exists := rule.Sysctls()["kernel.shmmax"]; exists {
			if !correctMinMaxLimitSysctl(v, 0, 18446744073692774399) {
				return SysctlsBadFormatCondition("kernel.shmmax", v, "an integer between 0 and 18446744073692774399")
			}
		}
		// vm.overcommit_memory cannot be set to 2 on machines with less than 15GB memory.
		if val, exists := rule.Sysctls()["vm.overcommit_memory"]; exists && val == "2" {
			if rule.MachineFamily() != "" {
				if mf, err := ch.provider.MachineConfigProvider().ToMachineFamily(rule.MachineFamily()); err == nil {
					largestMachineType := mf.LargestMachineType(machinetypes.NoConstraints)
					if largestMachineType.Memory < overcommitSysctlMachineMemoryLowerBound {
						return SysctlsNotSupportedWithMachineCondition("vm.overcommit_memory", val)
					}
				}
			} else if rule.MachineType() != "" {
				if mt, err := ch.provider.MachineConfigProvider().ToMachineType(rule.MachineType()); err == nil && mt.Memory < overcommitSysctlMachineMemoryLowerBound {
					return SysctlsNotSupportedWithMachineCondition("vm.overcommit_memory", val)
				}
			} else {
				defaultFamily := ch.provider.GetAutoprovisioningDefaultFamily().Name()
				if mf, err := ch.provider.MachineConfigProvider().ToMachineFamily(defaultFamily); err == nil {
					largestMachineType := mf.LargestMachineType(machinetypes.NoConstraints)
					if largestMachineType.Memory < overcommitSysctlMachineMemoryLowerBound {
						return SysctlsNotSupportedWithMachineCondition("vm.overcommit_memory", val)
					}
				}
			}
		}
	}
	if rule.CpuCfsQuotaPeriod() != nil {
		if !correctCpuCfsQuotaPeriod(*rule.CpuCfsQuotaPeriod()) {
			return CpuCfsQuotaPeriodBadFormatCondition(*rule.CpuCfsQuotaPeriod())
		}
	}
	if rule.ImageMinimumGcAge() != nil || rule.ImageMaximumGcAge() != nil {
		minAge := imageMinimumGcAgeDefault
		if rule.ImageMinimumGcAge() != nil {
			minAge, _ = time.ParseDuration(*rule.ImageMinimumGcAge())
			if minAge <= 0 || minAge > 2*time.Minute {
				return ImageMinimumGcAgeBadFormatCondition(*rule.ImageMinimumGcAge())
			}
		}
		if rule.ImageMaximumGcAge() != nil {
			maxAge, _ := time.ParseDuration(*rule.ImageMaximumGcAge())
			if maxAge < 0 || (maxAge != 0 && maxAge <= minAge) {
				return ImageMaximumGcAgeBadFormatCondition(minAge.String(), *rule.ImageMaximumGcAge())
			}
		}

	}
	if rule.ContainerLogMaxSize() != nil || rule.ContainerLogMaxFiles() != nil {
		maxSize, _ := resource.ParseQuantity(containerLogMaxSizeDefault)
		if rule.ContainerLogMaxSize() != nil {
			maxSize, _ = resource.ParseQuantity(*rule.ContainerLogMaxSize())
			// Default value in OSS kubelet is 10Mi, and we only allow users to increase the value.
			lowerBound, _ := resource.ParseQuantity(containerLogMaxSizeDefault)
			// Upper bound is 500Mi, as too large value adds load on kubectl logs.
			upperBound, _ := resource.ParseQuantity(containerLogMaxSizeUpperBound)
			if maxSize.Cmp(lowerBound) == -1 || maxSize.Cmp(upperBound) == 1 {
				return InvalidContainerLogSizeCondition(*rule.ContainerLogMaxSize())
			}
		}
		var maxFiles int64 = containerLogMaxFilesDefault
		if rule.ContainerLogMaxFiles() != nil {
			maxFiles = *rule.ContainerLogMaxFiles()
		}
		totalStorageGb := getTotalStorageGb(rule.BootDiskSize(), rule.TotalLSSDCount(), ch.localSsdProvider)
		if totalLogSize := float64(maxFiles) * maxSize.AsApproximateFloat64() / units.GiB; totalLogSize > float64(totalStorageGb)/100 {
			return InvalidContainerTotalLogSizeCondition(totalLogSize, totalStorageGb, rules.DefaultBootDiskSizeGb)
		}
	}
	if rule.AllowedUnsafeSysctls() != nil {
		if s := invalidAllowedUnsafeSysctls(rule.AllowedUnsafeSysctls()); s != "" {
			return UnsupportedAllowedUnsafeSysctls(s)
		}
	}

	var hugepage1g, hugepage2m int64
	if rule.HugepageSize1g() != nil {
		hugepage1g = *rule.HugepageSize1g()
	}
	if rule.HugepageSize2m() != nil {
		hugepage2m = *rule.HugepageSize2m()
	}
	totalHugepageSizeInMB := hugepage2m*2 + hugepage1g*1024

	var numaAlignmentNeeded bool
	if rule.MemoryManagerPolicy() != nil || rule.TopologyManagerPolicy() != nil || rule.TopologyManagerScope() != nil {
		numaAlignmentNeeded = true
	}

	if rule.MachineFamily() != "" {
		if mf, err := ch.provider.MachineConfigProvider().ToMachineFamily(rule.MachineFamily()); err == nil {
			if hugepage1g > 0 && !mf.IsHugepageSize1gSupported() {
				return UnsupportedMachineFamilyForHugepageSize1gCondition(rule.MachineFamily())
			}
			largestMachineType := mf.LargestMachineType(machinetypes.NoConstraints)
			if totalHugepageSizeInMB > largestMachineType.MaximumAllocatableHugepageCapacityInMB() {
				return AllMachinesInMachineFamilyExceedHugepageMemoryLimitCondition(rule.MachineFamily(), largestMachineType.AllocatableHugepageRatioCap())
			}
			if numaAlignmentNeeded && !mf.IsNumaAlignmentSupported() {
				return UnsupportedMachineFamilyForNumaAlignmentCondition(rule.MachineFamily())
			}
		}
	} else if rule.MachineType() != "" {
		if mf, err := ch.provider.MachineConfigProvider().GetMachineFamilyFromMachineName(rule.MachineType()); err == nil {
			if hugepage1g > 0 && !mf.IsHugepageSize1gSupported() {
				return UnsupportedMachineTypeForHugepageSize1gCondition(rule.MachineType())
			}
			if numaAlignmentNeeded && !mf.IsNumaAlignmentSupported() {
				return UnsupportedMachineTypeForNumaAlignmentCondition(rule.MachineType())
			}
		}
		if mt, err := ch.provider.MachineConfigProvider().ToMachineType(rule.MachineType()); err == nil && totalHugepageSizeInMB > mt.MaximumAllocatableHugepageCapacityInMB() {
			return TotalHugepagesExceedMemoryLimitCondition(totalHugepageSizeInMB, rule.MachineType(), mt.MaximumAllocatableHugepageCapacityInMB(), mt.AllocatableHugepageRatioCap())
		}
	} else {
		defaultFamily := ch.provider.GetAutoprovisioningDefaultFamily().Name()
		if mf, err := ch.provider.MachineConfigProvider().ToMachineFamily(defaultFamily); err == nil {
			if hugepage1g > 0 && !mf.IsHugepageSize1gSupported() {
				return UnsupportedDefaultMachineFamilyForHugepageSize1gCondition(defaultFamily)
			}
			largestMachineType := mf.LargestMachineType(machinetypes.NoConstraints)
			if totalHugepageSizeInMB > largestMachineType.MaximumAllocatableHugepageCapacityInMB() {
				return AllMachinesInDefaultFamilyExceedHugepageMemoryLimitCondition(defaultFamily, largestMachineType.AllocatableHugepageRatioCap())
			}
			if numaAlignmentNeeded && !mf.IsNumaAlignmentSupported() {
				return UnsupportedDefaultMachineFamilyForNumaAlignmentCondition(defaultFamily)
			}
		}
	}

	if condition := ch.validateEvictionConfig(rule); condition != nil {
		return condition
	}
	return nil
}

func (ch *nodeSystemConfigChecker) conditionType() string {
	return RuleMisconfiguredCondition
}

func correctFormatforMinDefaultMaxSysctl(str string) bool {
	parts := strings.Split(str, " ")
	if len(parts) != 3 {
		return false
	}

	minValue, err := strconv.Atoi(parts[0])
	if err != nil || minValue <= 0 {
		return false
	}

	defaultValue, err := strconv.Atoi(parts[1])
	if err != nil || defaultValue <= 0 || defaultValue < minValue {
		return false
	}

	maxValue, err := strconv.Atoi(parts[2])
	if err != nil || maxValue <= 0 || maxValue < defaultValue {
		return false
	}

	return true
}

func correctMinMaxLimitSysctl(val string, minVal, maxVal uint64) bool {
	uintVal, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return false
	}
	if uintVal < minVal || uintVal > maxVal {
		return false
	}
	return true
}

func correctCpuCfsQuotaPeriod(period string) bool {
	duration, err := time.ParseDuration(period)
	if err != nil {
		klog.Errorf("error parsing CPU CFS Quota: %s from CCC CRD, err: %s", period, err.Error())
		return false
	}
	if duration < 1*time.Millisecond || duration > 1*time.Second {
		return false
	}
	return true
}

// The maps define allowed unsafe sysctl groups: kernel.shm*, kernel.msg*, kernel.sem, fs.mqueue.*, net.*.
// It should keep the same as OSS kubelet in go/gke-autoscaler-ccc-unsafe-sysctl-groups
var (
	kubeletAllowedUnsafeSysctlNames = map[string]bool{
		// kernel semaphore parameters: SEMMSL, SEMMNS, SEMOPM, and SEMMNI.
		"kernel.sem": true,
		// kernel shared memory limits include shmall, shmmax, shmmni, and shm_rmid_forced.
		"kernel.shmall":          true,
		"kernel.shmmax":          true,
		"kernel.shmmni":          true,
		"kernel.shm_rmid_forced": true,
		"kernel.shm":             true,
		// kernel messages include msgmni, msgmax and msgmnb.
		"kernel.msgmax": true,
		"kernel.msgmnb": true,
		"kernel.msgmni": true,
		"kernel.msg":    true,
	}

	kubeletAllowedUnsafeSysctlPrefixes = map[string]bool{
		"net": true,
		// mqueue filesystem provides the necessary kernel features to enable the creation
		// of a user space library that implements the POSIX message queues API.
		"fs.mqueue": true,
	}
)

func invalidAllowedUnsafeSysctls(sysctls []string) string {
	invalidSysctls := []string{}
	for _, s := range sysctls {
		if !isValidSysctl(s) {
			invalidSysctls = append(invalidSysctls, s)
		}
	}
	if len(invalidSysctls) > 0 {
		return strings.Join(invalidSysctls, ", ")
	}
	return ""
}

func isValidSysctl(s string) bool {
	normalizedSys := normalizeSysctlName(s)
	firstStarIndex := strings.IndexAny(normalizedSys, "*")
	if firstStarIndex != -1 {
		normalizedSys = normalizedSys[:firstStarIndex]
	}

	// Check if the sysctl name is in the allowed maps.
	if kubeletAllowedUnsafeSysctlNames[normalizedSys] {
		return true
	}

	for prefix := range kubeletAllowedUnsafeSysctlPrefixes {
		if strings.HasPrefix(normalizedSys, prefix+".") {
			return true
		}
	}
	return false
}

// NormalizeName can return sysctl variables in dots separator format.
// The '/' separator is also accepted in place of a '.'. Convert the sysctl variables to dots separator format for validation.
// Same check as in go/gke-autoscaler-ccc-sysctl-normalization
func normalizeSysctlName(s string) string {
	if s == "" {
		return s
	}
	firstSepIndex := strings.IndexAny(s, "./")
	// if the first found is `.` like `net.ipv4.conf.eno2/100.rp_filter`
	if firstSepIndex == -1 || s[firstSepIndex] == '.' {
		return s
	}

	// for `net/ipv4/conf/eno2.100/rp_filter`, swap the use of `.` and `/` to `net.ipv4.conf.eno2/100.rp_filter`
	f := func(r rune) rune {
		switch r {
		case '.':
			return '/'
		case '/':
			return '.'
		}
		return r
	}
	return strings.Map(f, s)
}

// validateEvictionConfig crdChecks the eviction-related settings in a rule.
func (ch *nodeSystemConfigChecker) validateEvictionConfig(rule rules.Rule) *metav1.Condition {
	// 1. Validate soft eviction thresholds.
	if rule.EvictionSoftMemoryAvailable() != nil {
		// The memory available threshold must be at least 100Mi.
		minVal := resource.MustParse("100Mi")
		evictionVal, err := resource.ParseQuantity(*rule.EvictionSoftMemoryAvailable())
		if err != nil {
			return EvictionSoftMemoryInvalidQuantityCondition(*rule.EvictionSoftMemoryAvailable(), err)
		}
		if evictionVal.Cmp(minVal) < 0 {
			return EvictionSoftMemoryTooLowCondition(*rule.EvictionSoftMemoryAvailable(), minVal.String())
		}

		// We check that the threshold shouldn't be more than
		// half of the memory of the largest machine type in the node pool.
		var machineType machinetypes.MachineType
		if rule.MachineFamily() != "" {
			fam, err := ch.provider.MachineConfigProvider().ToMachineFamily(rule.MachineFamily())
			if err != nil {
				return EvictionConfigBadFormatCondition(fmt.Sprintf("KubeletConfig.EvictionSoft: invalid machine family %q for memory check: %c", rule.MachineFamily(), err))
			}
			machineType = fam.LargestMachineType(machinetypes.NoConstraints)
		} else if rule.MachineType() != "" {
			mt, err := ch.provider.MachineConfigProvider().ToMachineType(rule.MachineType())
			if err != nil {
				return EvictionConfigBadFormatCondition(fmt.Sprintf("KubeletConfig.EvictionSoft: invalid machine type %q for memory check: %c", rule.MachineType(), err))
			}
			machineType = mt
		} else {
			// If no machine type or family is specified, we use the default for autoprovisioning.
			mf := ch.provider.GetAutoprovisioningDefaultFamily()
			machineType = mf.LargestMachineType(machinetypes.NoConstraints)
		}

		if machineType.Name == "" {
			return EvictionConfigBadFormatCondition(fmt.Sprintf("KubeletConfig.EvictionSoft: no machine types found in family %q for memory check", rule.MachineFamily()))
		}

		if machineType.Memory > 0 {
			halfMemory := resource.NewQuantity(machineType.Memory/2, resource.BinarySI)
			if evictionVal.Cmp(*halfMemory) > 0 {
				return EvictionSoftMemoryTooHighCondition(*rule.EvictionSoftMemoryAvailable(), halfMemory.String(), machineType.Name)
			}
		}
		// If a soft eviction threshold is set, a grace period must also be specified.
		if rule.EvictionSoftGracePeriodMemoryAvailable() == nil {
			return EvictionSoftMissingGracePeriodCondition("memoryAvailable", *rule.EvictionSoftMemoryAvailable())
		}
	}

	// Next, validate the other soft eviction thresholds.
	if condition := validatePercentageWithConditions(rule.EvictionSoftNodefsAvailable(), "nodefsAvailable", "EvictionSoft", 10, 50); condition != nil {
		return condition
	}
	if rule.EvictionSoftNodefsAvailable() != nil && rule.EvictionSoftGracePeriodNodefsAvailable() == nil {
		return EvictionSoftMissingGracePeriodCondition("nodefsAvailable", *rule.EvictionSoftNodefsAvailable())
	}

	if condition := validatePercentageWithConditions(rule.EvictionSoftImagefsAvailable(), "imagefsAvailable", "EvictionSoft", 15, 50); condition != nil {
		return condition
	}
	if rule.EvictionSoftImagefsAvailable() != nil && rule.EvictionSoftGracePeriodImagefsAvailable() == nil {
		return EvictionSoftMissingGracePeriodCondition("imagefsAvailable", *rule.EvictionSoftImagefsAvailable())
	}

	if condition := validatePercentageWithConditions(rule.EvictionSoftImagefsInodesFree(), "imagefsInodesFree", "EvictionSoft", 5, 50); condition != nil {
		return condition
	}
	if rule.EvictionSoftImagefsInodesFree() != nil && rule.EvictionSoftGracePeriodImagefsInodesFree() == nil {
		return EvictionSoftMissingGracePeriodCondition("imagefsInodesFree", *rule.EvictionSoftImagefsInodesFree())
	}

	if condition := validatePercentageWithConditions(rule.EvictionSoftNodefsInodesFree(), "nodefsInodesFree", "EvictionSoft", 5, 50); condition != nil {
		return condition
	}
	if rule.EvictionSoftNodefsInodesFree() != nil && rule.EvictionSoftGracePeriodNodefsInodesFree() == nil {
		return EvictionSoftMissingGracePeriodCondition("nodefsInodesFree", *rule.EvictionSoftNodefsInodesFree())
	}

	if condition := validatePercentageWithConditions(rule.EvictionSoftPidAvailable(), "pidAvailable", "EvictionSoft", 10, 50); condition != nil {
		return condition
	}
	if rule.EvictionSoftPidAvailable() != nil && rule.EvictionSoftGracePeriodPidAvailable() == nil {
		return EvictionSoftMissingGracePeriodCondition("pidAvailable", *rule.EvictionSoftPidAvailable())
	}

	// 2. Validate the grace periods for soft evictions.
	maxDur := 5 * time.Minute

	// We create a temporary struct to hold all the grace period values. This allows us to use reflection
	// to validate them all with a single function call.
	gracePeriods := struct {
		MemoryAvailable   *string
		NodefsAvailable   *string
		ImagefsAvailable  *string
		ImagefsInodesFree *string
		NodefsInodesFree  *string
		PidAvailable      *string
	}{
		MemoryAvailable:   rule.EvictionSoftGracePeriodMemoryAvailable(),
		NodefsAvailable:   rule.EvictionSoftGracePeriodNodefsAvailable(),
		ImagefsAvailable:  rule.EvictionSoftGracePeriodImagefsAvailable(),
		ImagefsInodesFree: rule.EvictionSoftGracePeriodImagefsInodesFree(),
		NodefsInodesFree:  rule.EvictionSoftGracePeriodNodefsInodesFree(),
		PidAvailable:      rule.EvictionSoftGracePeriodPidAvailable(),
	}
	if condition := validateDurationsWithReflection(&gracePeriods, maxDur); condition != nil {
		return condition
	}

	// 3. Validate the minimum reclaim percentages.
	validateReclaim := func(val *string, name string) *metav1.Condition {
		if val == nil {
			return nil
		}
		if err := validatePercentage(*val, 0, 10); err != nil {
			// Distinguish between a format error and a range error to provide a more specific message.
			if errors.Is(err, ErrInvalidPercentageFormat) {
				return EvictionMinimumReclaimInvalidPercentageFormatCondition(name, *val)
			}
			return EvictionMinimumReclaimPercentageOutOfRangeCondition(name, *val)
		}
		return nil
	}
	if condition := validateReclaim(rule.EvictionMinimumReclaimMemoryAvailable(), "memoryAvailable"); condition != nil {
		return condition
	}
	if condition := validateReclaim(rule.EvictionMinimumReclaimNodefsAvailable(), "nodefsAvailable"); condition != nil {
		return condition
	}
	if condition := validateReclaim(rule.EvictionMinimumReclaimImagefsAvailable(), "imagefsAvailable"); condition != nil {
		return condition
	}
	if condition := validateReclaim(rule.EvictionMinimumReclaimImagefsInodesFree(), "imagefsInodesFree"); condition != nil {
		return condition
	}
	if condition := validateReclaim(rule.EvictionMinimumReclaimNodefsInodesFree(), "nodefsInodesFree"); condition != nil {
		return condition
	}
	if condition := validateReclaim(rule.EvictionMinimumReclaimPidAvailable(), "pidAvailable"); condition != nil {
		return condition
	}
	return nil
}

// validatePercentage crdChecks if a string is a valid percentage within a given range.
func validatePercentage(percentageStr string, minVal float64, maxVal float64) error {
	if !strings.HasSuffix(percentageStr, "%") {
		return fmt.Errorf("%w: %s", ErrInvalidPercentageFormat, percentageStr)
	}
	val, err := strconv.ParseFloat(strings.TrimSuffix(percentageStr, "%"), 64)
	if err != nil {
		return fmt.Errorf("%w: failed to parse: %v", ErrInvalidPercentageFormat, err)
	}
	if val < minVal || val > maxVal {
		return fmt.Errorf("%w: %f is not within the allowed range [%f, %f]", ErrPercentageOutOfRange, val, minVal, maxVal)
	}
	return nil
}

// validatePercentageWithConditions crdChecks a percentage value and returns a specific condition on failure.
// - val is the string value to be validated.
// - fieldName is the name of the field being validated (ex "nodefsAvailable").
// - configType is the parent configuration section (ex "EvictionSoft").
func validatePercentageWithConditions(val *string, fieldName, configType string, min, max int) *metav1.Condition {
	if val == nil {
		return nil
	}
	if err := validatePercentage(*val, float64(min), float64(max)); err != nil {
		if errors.Is(err, ErrInvalidPercentageFormat) {
			return EvictionSoftInvalidPercentageFormatCondition(configType, fieldName, *val)
		}
		if errors.Is(err, ErrPercentageOutOfRange) {
			return EvictionSoftPercentageOutOfRangeCondition(configType, fieldName, *val, min, max)
		}

		return EvictionSoftInvalidPercentageFormatCondition(configType, fieldName, *val)
	}
	return nil
}

// validateDuration crdChecks if a string is a valid duration and less than a max value.
func validateDuration(durationStr string, maxVal time.Duration) error {
	val, err := time.ParseDuration(durationStr)
	if err != nil {
		return fmt.Errorf("failed to parse duration: %v", err)
	}
	if val <= 0 {
		return fmt.Errorf("duration must be positive")
	}
	if val >= maxVal {
		return fmt.Errorf("duration %s is greater than or equal to the maximum allowed value %s", val.String(), maxVal.String())
	}
	return nil
}

// validateDurationsWithReflection uses reflection to validate all duration fields in a struct.
// This is a bit of a fancy way to avoid writing the same validation logic for each field.
func validateDurationsWithReflection(st interface{}, maxVal time.Duration) *metav1.Condition {
	val := reflect.ValueOf(st).Elem()
	typ := val.Type()
	for i := 0; i < val.NumField(); i++ {
		fieldTyp := typ.Field(i)
		fieldVal := val.Field(i)
		if fieldVal.Kind() == reflect.Ptr && !fieldVal.IsNil() {
			durationStr := fieldVal.Elem().String()
			if err := validateDuration(durationStr, maxVal); err != nil {
				if strings.Contains(err.Error(), "failed to parse duration") {
					return EvictionSoftGracePeriodInvalidDurationCondition(fieldTyp.Name, durationStr, err)
				}
				return EvictionSoftGracePeriodOutOfRangeCondition(fieldTyp.Name, durationStr, maxVal.String())
			}
		}
	}
	return nil
}

// Get the total requested storage containing boot disk and local SSD.
func getTotalStorageGb(bootDiskSize, localSSDCount int64, ssdProvider localssdsize.LocalSSDSizeProvider) int64 {
	var totalStorage int64 = rules.DefaultBootDiskSizeGb
	if bootDiskSize != 0 {
		totalStorage = bootDiskSize
	}
	if localSSDCount != 0 {
		totalStorage += int64(ssdProvider.SSDSizeInGiB("NVME")) * localSSDCount
	}
	return totalStorage
}
