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
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	mcv1 "k8s.io/gke-autoscaling/cluster-autoscaler/apis/machineconfig/cloud.google.com/v1"
	v1 "k8s.io/gke-autoscaling/cluster-autoscaler/apis/machineconfig/cloud.google.com/v1"
	"k8s.io/utils/ptr"
)

const bytesPerMiB = 1024 * 1024

func ToCpuPlatformInfoObject(cp mcv1.CPUPlatform) (cpuPlatformInfo, error) {
	var as []CpuPlatform
	for _, alias := range cp.Aliases {
		as = append(as, CpuPlatform(alias))
	}

	o, err := safeConvertInt64(ptr.Deref(cp.VendorOrder, 0))
	if err != nil {
		return cpuPlatformInfo{}, err
	}

	return cpuPlatformInfo{
		name:    CpuPlatform(cp.Name),
		aliases: as,
		vendor:  ptr.Deref(cp.Vendor, ""),
		order:   o,
	}, nil
}

func safeConvertInt64(n int64) (int, error) {
	res := int(n)
	if int64(res) != n {
		return 0, fmt.Errorf("integer overflow: %d does not fit in an int", n)
	}
	return res, nil
}

func CollectCPUPlatforms(mf *mcv1.MachineFamily) map[string]mcv1.CPUPlatform {
	if mf == nil {
		return nil
	}
	allPlatforms := make(map[string]mcv1.CPUPlatform)
	for _, p := range mf.DefaultProperties.CPUPlatforms {
		allPlatforms[p.Name] = p
	}
	for _, mt := range mf.MachineTypes {
		if mt.Properties == nil {
			continue
		}
		for _, p := range mt.Properties.CPUPlatforms {
			allPlatforms[p.Name] = p
		}
	}
	return allPlatforms
}

// GradedError holds the warning and error details encountered during the processing of a MachineFamily.
type GradedError struct {
	Warning error
	Err     error
}

// ToMachineFamilyObject converts a MachineFamily answer from MachineConfig SoT to the MachineFamily object from machinetypes package.
// TODO(b/517095165): make it fill all the fields in MachineFamily object.
func ToMachineFamilyObject(mf *mcv1.MachineFamily, cpSource *cpuPlatformsSource, enableCvmSot bool) (MachineFamily, GradedError) {
	// TODO(b/498193239): add conversion of resizable configs for EK and E4A machines
	if mf == nil {
		return MachineFamily{}, GradedError{}
	}

	usagePolicy := extractUsagePolicy(mf.UsagePolicy)

	if err := validateMachineFamily(mf, usagePolicy); err != nil {
		return MachineFamily{}, GradedError{
			Err: fmt.Errorf("failed to validate machine family %q: %v", mf.Name, err),
		}
	}

	result := MachineFamily{
		name:                  mf.Name,
		usagePolicy:           usagePolicy,
		supportedCpuPlatforms: noPlatformSupported,
	}

	var warnings []error

	if usagePolicy == nil || usagePolicy.MachineProperties {
		warns, err := extractCore(&result, mf, cpSource, enableCvmSot)
		if warns != nil {
			warnings = append(warnings, warns...)
		}
		if err != nil {
			return MachineFamily{}, GradedError{Warning: errors.Join(warnings...), Err: err}
		}
	}

	if usagePolicy == nil || usagePolicy.Weights {
		if err := extractWeights(&result, mf); err != nil {
			return MachineFamily{}, GradedError{Warning: errors.Join(warnings...), Err: err}
		}
	}

	backfillAndPrecomputeIfRequired(&result, usagePolicy)

	return result, GradedError{
		Warning: errors.Join(warnings...),
	}
}

func extractCore(result *MachineFamily, mf *mcv1.MachineFamily, cpSource *cpuPlatformsSource, enableCvmSot bool) ([]error, error) {
	var warnings []error

	cpuReqs, cpuErr := extractCPUPlatforms(mf.DefaultProperties.CPUPlatforms, cpSource)
	if cpuErr.Warning != nil {
		warnings = append(warnings, cpuErr.Warning)
	}
	if cpuErr.Err != nil {
		return warnings, fmt.Errorf("failed to extract CPU platforms for family %q: %w", mf.Name, cpuErr.Err)
	}

	var supportConfidentialNodes bool
	var supportConfidentialNodeTypes map[string]bool

	if enableCvmSot && mf.DefaultProperties.ConfidentialNodeConfig != nil {
		cvmConfig := mf.DefaultProperties.ConfidentialNodeConfig
		supportConfidentialNodeTypes = extractConfidentialNodeTypes(cvmConfig)
		supportConfidentialNodes = ptr.Deref(cvmConfig.Supported, false)
	} else if ptr.Deref(mf.DefaultProperties.SupportsConfidentialNodes, false) {
		supportConfidentialNodes = true
		supportConfidentialNodeTypes = getHardcodedFamilyConfidentialTypes(mf.Name)
	}

	autoprovisionedMachineTypes, otherMachineTypes, err := extractMachineTypes(mf, cpSource, cpuReqs, enableCvmSot)
	if err != nil {
		return warnings, fmt.Errorf("failed to extract machine types for family %q: %v", mf.Name, err)
	}

	result.systemArchitecture = gce.ToSystemArchitecture(ptr.Deref(mf.DefaultProperties.SystemArchitecture, ""))
	result.nonDefaultThreadsPerCore = mf.DefaultProperties.ThreadsPerCore
	result.supportedBootDiskTypes = extractBootDiskTypes(mf.DefaultProperties.BootDiskConfig)
	result.defaultDiskType = extractDefaultBootDisk(mf.DefaultProperties.BootDiskConfig)
	result.supportCompactPlacement = extractCompactPlacementEnabled(mf.DefaultProperties.CompactPlacementConfig)
	result.maxCompactPlacementNodes = extractCompactPlacementMaxNodes(mf.DefaultProperties.CompactPlacementConfig)
	result.supportedCpuPlatforms = cpuReqs
	result.autoprovisionedMachineTypes = autoprovisionedMachineTypes
	result.otherMachineTypes = otherMachineTypes
	result.supportConfidentialNodes = supportConfidentialNodes
	result.supportConfidentialNodeTypes = supportConfidentialNodeTypes
	result.supportedAttachDiskTypes = extractAttachDiskTypes(mf.DefaultProperties.PersistentDiskTypeConfigs)
	result.supportHugepageSize1g = extractSupportHugepageSize1g(mf.PageType)
	result.numaAlignmentUnsupported = mf.DefaultProperties.NumaAlignmentUnsupported

	return warnings, nil
}

func extractWeights(result *MachineFamily, mf *mcv1.MachineFamily) error {
	result.pricingInfo = extractFamilyPricingInfo(mf.Weights)
	result.customPricingInfo = extractCustomFamilyPricingInfo(mf.Weights)

	hasCore := result.usagePolicy == nil || result.usagePolicy.MachineProperties
	if !hasCore && result.otherMachineTypes == nil {
		result.otherMachineTypes = make(map[string]MachineType)
	}

	for _, mt := range mf.MachineTypes {
		pInfo := extractMachineTypePriceInfo(mt.Weights)

		if !hasCore {
			result.otherMachineTypes[mt.Name] = MachineType{
				MachineType: gce.MachineType{Name: mt.Name},
				priceInfo:   pInfo,
			}
			continue
		}

		if existing, ok := result.autoprovisionedMachineTypes[mt.Name]; ok {
			existing.priceInfo = pInfo
			result.autoprovisionedMachineTypes[mt.Name] = existing
			continue
		}

		if existing, ok := result.otherMachineTypes[mt.Name]; ok {
			existing.priceInfo = pInfo
			result.otherMachineTypes[mt.Name] = existing
		}
	}
	return nil
}

func validateMachineFamily(mf *mcv1.MachineFamily, policy *UsagePolicy) error {
	var errs []error

	if policy == nil || policy.MachineProperties {
		if err := validateCore(mf); err != nil {
			errs = append(errs, err)
		}
	}

	if policy == nil || policy.Weights {
		if err := validateWeights(mf); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func validateCore(mf *mcv1.MachineFamily) error {
	if err := validateMachineTypes(mf.MachineTypes); err != nil {
		return err
	}
	if err := validateBootDiskConfig(mf.DefaultProperties.BootDiskConfig); err != nil {
		return err
	}
	if err := validateCompactPlacementConfig(mf.DefaultProperties.CompactPlacementConfig); err != nil {
		return err
	}
	if err := validatePersistentDiskTypeConfigs(mf.DefaultProperties.PersistentDiskTypeConfigs); err != nil {
		return err
	}
	if err := validatePageType(mf.PageType); err != nil {
		return err
	}
	// TODO(b/489334073): validate CPU platform settings
	return nil
}

func validateWeights(mf *mcv1.MachineFamily) error {
	var errs []error
	if err := validateMachineFamilyWeights(mf.Weights); err != nil {
		errs = append(errs, err)
	}
	for _, mt := range mf.MachineTypes {
		if mt.Name == "" {
			errs = append(errs, fmt.Errorf("machine type name is required"))
			continue
		}
		if err := validateMachineTypeWeights(mt.Weights); err != nil {
			errs = append(errs, fmt.Errorf("failed to validate weights for machine type %q: %w", mt.Name, err))
		}
	}
	return errors.Join(errs...)
}

func extractCPUPlatforms(cpuPlatforms []mcv1.CPUPlatform, cpSource *cpuPlatformsSource) (CpuPlatformRequirements, GradedError) {
	if len(cpuPlatforms) == 0 {
		return noPlatformSupported, GradedError{}
	}

	var parseErrs []error
	var platforms []cpuPlatformInfo
	for _, p := range cpuPlatforms {
		cp, found := cpSource.get(CpuPlatform(p.Name))
		if !found {
			parseErrs = append(parseErrs, fmt.Errorf("CPU platform %s not found in machineconfig source", p.Name))
		} else {
			platforms = append(platforms, cp)
		}
	}

	lowerBound, upperBound, boundErrs := BoundsOrFail(platforms)
	if len(boundErrs) > 0 {
		return noPlatformSupported, GradedError{
			Warning: errors.Join(errors.Join(parseErrs...), errors.Join(boundErrs...)),
		}
	}

	return NewCpuPlatformRequirements(lowerBound, upperBound), GradedError{
		Warning: errors.Join(errors.Join(parseErrs...), errors.Join(boundErrs...)),
	}
}

func extractBootDiskTypes(bootDiskConfig *mcv1.BootDiskConfig) map[string]bool {
	if bootDiskConfig == nil {
		return nil
	}

	result := make(map[string]bool)
	for _, diskType := range bootDiskConfig.Types {
		result[diskType] = true
	}
	return result
}

func extractDefaultBootDisk(bootDiskConfig *mcv1.BootDiskConfig) string {
	if bootDiskConfig == nil {
		return ""
	}
	return bootDiskConfig.DefaultType
}

func validateBootDiskConfig(bootDiskConfig *mcv1.BootDiskConfig) error {
	if bootDiskConfig == nil {
		return nil
	}
	// TODO(b/517095166): change CRD field of DefaultType to pointer and check for nil, not ""
	if bootDiskConfig.DefaultType == "" {
		return nil
	}
	if !slices.Contains(bootDiskConfig.Types, bootDiskConfig.DefaultType) {
		return fmt.Errorf(
			"Default boot disk '%s' not found in supported boot disk list: %v",
			bootDiskConfig.DefaultType,
			bootDiskConfig.Types,
		)
	}
	return nil
}

func validateCompactPlacementConfig(compactPlacementConfig *mcv1.CompactPlacementConfig) error {
	if compactPlacementConfig == nil {
		return nil
	}
	if !compactPlacementConfig.Supported {
		return nil
	}
	if ptr.Deref(compactPlacementConfig.MaxCount, 0) <= 0 {
		return fmt.Errorf("Max compact placement count must be a positive number - got MaxCount=%d", *compactPlacementConfig.MaxCount)
	}
	return nil
}

func extractCompactPlacementEnabled(compactPlacementConfig *mcv1.CompactPlacementConfig) bool {
	if compactPlacementConfig == nil {
		return false
	}
	return compactPlacementConfig.Supported
}

func extractCompactPlacementMaxNodes(compactPlacementConfig *mcv1.CompactPlacementConfig) int64 {
	if compactPlacementConfig == nil {
		return 0
	}
	return ptr.Deref(compactPlacementConfig.MaxCount, 0)
}

func validateMachineFamilyWeights(weights *mcv1.MachineFamilyWeights) error {
	if weights == nil {
		return nil
	}
	// Predefined is a value type, always validate it
	if err := validateResourceWeights(&weights.Predefined); err != nil {
		return fmt.Errorf("error in parsing Predefined prices: %w", err)
	}
	// Custom and Preemptible are optional pointers; only validate if present
	if weights.Custom != nil {
		if err := validateResourceWeights(weights.Custom); err != nil {
			return fmt.Errorf("error in parsing Custom prices: %w", err)
		}
	}
	if weights.Preemptible != nil {
		if err := validateResourceWeights(weights.Preemptible); err != nil {
			return fmt.Errorf("error in parsing Preemptible prices: %w", err)
		}
	}
	return nil
}

func validateResourceWeights(rw *mcv1.ResourceWeights) error {
	if _, err := parsePrice(rw.CPU); err != nil {
		return fmt.Errorf("failed to parse CPU price: %w", err)
	}
	if _, err := parsePrice(rw.Memory); err != nil {
		return fmt.Errorf("failed to parse Memory price: %w", err)
	}
	if _, err := parsePrice(rw.LocalSSD); err != nil {
		return fmt.Errorf("failed to parse LocalSSD price: %w", err)
	}
	return nil
}

// Simplifications made in the current implementation:
// (Both of these are minor and across all machine families the resulting values are almost always identical)
//   - PreemptibleDiscount is calculated based on the ratio of CPU prices (spot vs. on-demand), disregarding Memory and LocalSSD prices.
//     This is due to OSS CA not currently supporting per-resource preemptible discounts.
//   - Custom PreemptibleDiscount is equal to PreemptibleDiscount.
//     This is due to not pulling Custom Preemptible prices from the API.
//
// TODO(b/503662299): Close these gaps.
func extractFamilyPricingInfo(weights *mcv1.MachineFamilyWeights) MachineFamilyPricingInfo {
	if weights == nil {
		return MachineFamilyPricingInfo{}
	}
	return MachineFamilyPricingInfo{
		CpuPricePerHour:           extractCpuPricePerHour(&weights.Predefined),
		MemoryPricePerHourPerGb:   extractMemoryPricePerHourPerGb(&weights.Predefined),
		LocalSsdPricePerHourPerGb: extractLocalSsdPricePerHourPerGb(&weights.Predefined),
		PreemptibleDiscount:       extractPreemptibleDiscount(weights),
	}
}

func extractCustomFamilyPricingInfo(weights *mcv1.MachineFamilyWeights) *MachineFamilyPricingInfo {
	if weights == nil || weights.Custom == nil {
		return nil
	}
	return &MachineFamilyPricingInfo{
		CpuPricePerHour:           extractCpuPricePerHour(weights.Custom),
		MemoryPricePerHourPerGb:   extractMemoryPricePerHourPerGb(weights.Custom),
		LocalSsdPricePerHourPerGb: extractLocalSsdPricePerHourPerGb(weights.Custom),
		PreemptibleDiscount:       extractPreemptibleDiscount(weights),
	}
}

func extractCpuPricePerHour(weights *mcv1.ResourceWeights) float64 {
	if weights == nil {
		return 0
	}
	val, _ := parsePrice(weights.CPU)
	return val
}

func extractMemoryPricePerHourPerGb(weights *mcv1.ResourceWeights) float64 {
	if weights == nil {
		return 0
	}
	val, _ := parsePrice(weights.Memory)
	return val
}

func extractLocalSsdPricePerHourPerGb(weights *mcv1.ResourceWeights) float64 {
	if weights == nil {
		return 0
	}
	val, _ := parsePrice(weights.LocalSSD)
	return val
}

func extractPreemptibleDiscount(weights *mcv1.MachineFamilyWeights) float64 {
	if weights == nil || weights.Preemptible == nil {
		return 0
	}
	preemptibleCpu, _ := parsePrice(weights.Preemptible.CPU)
	predefinedCpu, _ := parsePrice(weights.Predefined.CPU)
	if predefinedCpu == 0 {
		return 0
	}
	return preemptibleCpu / predefinedCpu
}

// ToMachineTypeObject converts a MachineType answer from MachineConfig SoT to the MachineType object from machinetypes package.
func ToMachineTypeObject(
	mf *mcv1.MachineFamily,
	mt *mcv1.MachineType,
	cpSource *cpuPlatformsSource,
	defaultCPUReqs CpuPlatformRequirements,
	enableCvmSot bool,
) (MachineType, error) {
	// TODO(b/498113704): Add conversion of pricing
	// TODO(b/498193239): add conversion of resizable configs for EK and E4A machines
	if mt == nil {
		return MachineType{}, nil
	}

	var confidentialNodeCfg *confidentialNodeConfig
	if mt.Properties != nil {
		if enableCvmSot {
			if mt.Properties.ConfidentialNodeConfig != nil {
				confidentialNodeCfg = &confidentialNodeConfig{
					supportConfidentialNodeTypes: extractConfidentialNodeTypes(mt.Properties.ConfidentialNodeConfig),
				}
			}
		} else if ptr.Deref(mt.Properties.SupportsConfidentialNodes, false) {
			confidentialNodeCfg = getHardcodedTypeConfidentialCfg(mf.Name, mt.Name)
		}
	}

	newMt := MachineType{
		MachineType: gce.MachineType{
			Name:   mt.Name,
			CPU:    mt.Resources.CPUs,
			Memory: mt.Resources.Memory * bytesPerMiB,
		},
		confidentialNodeCfg: confidentialNodeCfg,
	}

	if mt.Properties == nil {
		return newMt, nil
	}

	if len(mt.Properties.CPUPlatforms) > 0 {
		cr, _ := extractCPUPlatforms(mt.Properties.CPUPlatforms, cpSource)
		if cr != defaultCPUReqs {
			newMt = newMt.withCpuPlatformRequirements(cr)
		}
	}
	if mt.Properties.ThreadsPerCore != nil {
		newMt = newMt.withThreadsPerCoreOverride(*mt.Properties.ThreadsPerCore)
	}
	if mt.Properties.NAPDisabled != nil && *mt.Properties.NAPDisabled {
		newMt = newMt.withExplicitReqOnly()
	}

	if mt.Properties.BootDiskConfig != nil {
		newMt.supportedDisksOverride = extractBootDiskTypesOrNil(mt.Properties.BootDiskConfig)
		newMt.defaultDiskOverride = ptr.Deref(mt.Properties.BootDiskConfig, mcv1.BootDiskConfig{}).DefaultType
	}

	if mt.Resources.LocalSSDConfig != nil {
		newMt.ephemeralLocalSsdCfg = extractSsdConfig(mt.Resources.LocalSSDConfig)
	}

	return newMt, nil
}

func extractSsdConfig(ssdConfig *v1.LocalSSDConfig) *ephemeralLocalSsdConfig {
	if ssdConfig == nil {
		return nil
	}

	var ac map[int]bool
	if len(ssdConfig.AvailableCounts) > 0 {
		ac = make(map[int]bool)
		for _, c := range ssdConfig.AvailableCounts {
			ac[int(c)] = true
		}
	}

	return &ephemeralLocalSsdConfig{
		allowedDiskCounts:  ac,
		automaticDiskCount: ptr.To(ssdConfig.DefaultCount),
		diskSize:           uint64(ssdConfig.DiskSize),
	}
}

func extractBootDiskTypesOrNil(bootDiskConfig *mcv1.BootDiskConfig) *[]string {
	if bootDiskConfig == nil || len(bootDiskConfig.Types) == 0 {
		return nil
	}
	var diskTypes []string
	for _, dt := range bootDiskConfig.Types {
		diskTypes = append(diskTypes, dt)
	}
	return &diskTypes
}

func extractMachineTypes(
	mf *mcv1.MachineFamily,
	cpSource *cpuPlatformsSource,
	cpReqs CpuPlatformRequirements,
	enableCvmSot bool,
) (auto map[string]MachineType, other map[string]MachineType, err error) {
	familyNAPDisabled := ptr.Deref(mf.DefaultProperties.NAPDisabled, false)

	for _, mt := range mf.MachineTypes {
		napDisabled := familyNAPDisabled
		if mt.Properties != nil && mt.Properties.NAPDisabled != nil {
			napDisabled = *mt.Properties.NAPDisabled
		}

		mtObj, err := ToMachineTypeObject(mf, &mt, cpSource, cpReqs, enableCvmSot)
		if err != nil {
			return nil, nil, err
		}

		if napDisabled {
			if other == nil {
				other = make(map[string]MachineType)
			}
			other[mt.Name] = mtObj
		} else {
			if auto == nil {
				auto = make(map[string]MachineType)
			}
			auto[mt.Name] = mtObj
		}
	}
	return auto, other, nil
}

func validateMachineTypes(machineTypes []mcv1.MachineType) error {
	for _, mt := range machineTypes {
		if err := validateMachineType(&mt); err != nil {
			return err
		}
	}
	return nil
}

func validateMachineType(mt *mcv1.MachineType) error {
	if mt == nil {
		return nil
	}
	if mt.Name == "" {
		return fmt.Errorf("machine type name is required")
	}
	if err := validateMachineResources(mt.Resources, mt.Name); err != nil {
		return err
	}
	if err := validateMachineTypeProperties(mt.Properties); err != nil {
		return err
	}
	return nil
}

func validateMachineResources(res mcv1.MachineResources, mtName string) error {
	if res.CPUs <= 0 {
		return fmt.Errorf("machine type %q requires cpu count > 0, got %d", mtName, res.CPUs)
	}
	if res.Memory <= 0 {
		return fmt.Errorf("machine type %q requires memory amount > 0, got %d", mtName, res.Memory)
	}
	return nil
}

func validateMachineTypeWeights(weights *mcv1.MachineTypeWeights) error {
	if weights == nil {
		return nil
	}
	if weights.InstanceWeight != nil {
		if _, err := parsePrice(*weights.InstanceWeight); err != nil {
			return fmt.Errorf("invalid InstanceWeight: %w", err)
		}
	}
	if weights.PreemptibleInstanceWeight != nil {
		if _, err := parsePrice(*weights.PreemptibleInstanceWeight); err != nil {
			return fmt.Errorf("invalid PreemptibleInstanceWeight: %w", err)
		}
	}
	return nil
}

func validateMachineTypeProperties(props *mcv1.MachineProperties) error {
	if props == nil {
		return nil
	}
	if err := validateBootDiskConfig(props.BootDiskConfig); err != nil {
		return err
	}
	if err := validateCompactPlacementConfig(props.CompactPlacementConfig); err != nil {
		return err
	}
	if err := validatePersistentDiskTypeConfigs(props.PersistentDiskTypeConfigs); err != nil {
		return err
	}
	return nil
}

func extractMachineTypePriceInfo(weights *mcv1.MachineTypeWeights) *MachinePriceInfo {
	instancePrice := extractInstancePrice(weights)
	preemtibleInstancePrice := extractPreemptibleInstancePrice(weights)
	if instancePrice == nil && preemtibleInstancePrice == nil {
		return nil
	}

	return &MachinePriceInfo{
		instancePrice:           instancePrice,
		preemtibleInstancePrice: preemtibleInstancePrice,
	}
}

func extractInstancePrice(weights *mcv1.MachineTypeWeights) *float64 {
	if weights == nil || weights.InstanceWeight == nil {
		return nil
	}
	val, _ := parsePrice(*weights.InstanceWeight)
	return ptr.To(val)
}

func extractPreemptibleInstancePrice(weights *mcv1.MachineTypeWeights) *float64 {
	if weights == nil || weights.PreemptibleInstanceWeight == nil {
		return nil
	}
	val, _ := parsePrice(*weights.PreemptibleInstanceWeight)
	return ptr.To(val)
}

func parsePrice(price string) (float64, error) {
	if price == "" {
		return 0, nil
	}
	value, err := strconv.ParseFloat(price, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse float: %v\n", err)
	}
	return value, nil
}

func extractConfidentialNodeTypes(config *mcv1.ConfidentialNodeConfig) map[string]bool {
	if config == nil || !ptr.Deref(config.Supported, false) || len(config.Types) == 0 {
		return nil
	}
	supportedTypes := make(map[string]bool)
	for _, cvmType := range config.Types {
		if cvmType.Type != "" && cvmType.Type != "CONFIDENTIAL_INSTANCE_TYPE_UNSPECIFIED" {
			supportedTypes[cvmType.Type] = true
		}
	}
	if len(supportedTypes) == 0 {
		return nil
	}
	return supportedTypes
}

func extractUsagePolicy(usagePolicy *mcv1.UsagePolicy) *UsagePolicy {
	if usagePolicy == nil || usagePolicy.Mode == nil {
		return nil
	}

	switch *usagePolicy.Mode {
	case mcv1.UsagePolicyModeWeightsOnly:
		return &UsagePolicy{
			MachineProperties: false,
			Weights:           true,
		}
	case mcv1.UsagePolicyModePropertiesOnly:
		return &UsagePolicy{
			MachineProperties: true,
			Weights:           false,
		}
	case mcv1.UsagePolicyModeLegacy:
		return &UsagePolicy{
			MachineProperties: false,
			Weights:           false,
		}
	default:
		return nil
	}
}

func getHardcodedFamilyConfidentialTypes(familyName string) map[string]bool {
	if hardcodedFamily, found := machineFamiliesByName[strings.ToLower(familyName)]; found {
		return hardcodedFamily.supportConfidentialNodeTypes
	}
	return nil
}

func getHardcodedTypeConfidentialCfg(familyName, typeName string) *confidentialNodeConfig {
	if hardcodedFamily, found := machineFamiliesByName[strings.ToLower(familyName)]; found {
		if hardcodedType, found := hardcodedFamily.AllMachineTypes(NoConstraints)[strings.ToLower(typeName)]; found {
			return hardcodedType.confidentialNodeCfg
		}
	}
	return nil
}

func extractAttachDiskTypes(configs []mcv1.PersistentDiskTypeConfig) map[string]ConfidentialMode {
	if len(configs) == 0 {
		return nil
	}
	result := make(map[string]ConfidentialMode)
	for _, config := range configs {
		mode := UnspecifiedMode
		if config.ConfidentialMode != nil {
			switch *config.ConfidentialMode {
			case "CONFIDENTIAL_ONLY":
				mode = ConfidentialOnlyMode
			case "NON_CONFIDENTIAL_ONLY":
				mode = NonConfidentialOnlyMode
			default:
				mode = UnspecifiedMode
			}
		}
		result[config.Name] = mode
	}
	return result
}

func validatePersistentDiskTypeConfigs(configs []mcv1.PersistentDiskTypeConfig) error {
	for _, config := range configs {
		if config.Name == "" {
			return fmt.Errorf("persistent disk type config name is required")
		}
		if config.ConfidentialMode != nil {
			switch *config.ConfidentialMode {
			case "CONFIDENTIAL_MODE_UNSPECIFIED", "UNSPECIFIED", "CONFIDENTIAL_ONLY", "NON_CONFIDENTIAL_ONLY", "":
			default:
				return fmt.Errorf("invalid confidential mode value: %q", *config.ConfidentialMode)
			}
		}
	}
	return nil
}

func validatePageType(pageType *string) error {
	if pageType == nil {
		return nil
	}
	switch *pageType {
	case "PAGE_TYPE_UNSPECIFIED", "HUGETMPFS_SIZE2M", "HUGETLBFS_SIZE2M", "HUGETLBFS_SIZE1G":
		return nil
	default:
		return fmt.Errorf("invalid page type value: %s", *pageType)
	}
}

func extractSupportHugepageSize1g(pageType *string) bool {
	if pageType == nil {
		return false
	}
	return *pageType == "HUGETLBFS_SIZE1G"
}

// backfillAndPrecomputeIfRequired populates the parent family pointer in all extracted machine types
// and caches the aggregated map of all machine types. This is skipped in weights-only mode
// because the extracted struct is a partial update; the pointers and maps will come from
// the fully populated base MachineFamily struct that this update is merged into.
func backfillAndPrecomputeIfRequired(family *MachineFamily, usagePolicy *UsagePolicy) {
	if usagePolicy == nil || usagePolicy.MachineProperties {
		backfillMachineFamilyInMachineTypes(family)
		family.precomputeAllMachineTypes()
	}
}
