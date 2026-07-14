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

package machineselection

import (
	"fmt"
	"sort"
	"strings"

	"slices"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	klog "k8s.io/klog/v2"

	napprovider "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/napcloudprovider"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

// Selector can be used to select machine types to base injected NAP node groups on.
type Selector struct {
	CloudProvider      napprovider.AutoprovisioningCloudProvider
	ExperimentsManager experiments.Manager
}

// Select selects machine spec to base injected NAP node groups on for given pod label requirements,
// specified GPU, TPU and boot disk type.
func (s Selector) Select(
	labelReq podrequirements.LabelRequirements,
	specifiedGpu, specifiedTpu, specifiedBootDiskType string,
	rule rules.Rule,
	wantsSpot bool,
	reservationMachineType string,
	autopilotEnabled, autopilotManaged, isStateless bool,
) (machinetypes.MachineSpec, machinetypes.SelectionType, errors.AutoscalerError) {
	architectures, err := podArchitectures(labelReq)
	if err != nil {
		return machinetypes.MachineSpec{}, machinetypes.SelectionTypeNone, err
	}
	spec, selectionType, err := s.selectMachineSpec(labelReq, specifiedGpu,
		specifiedTpu, specifiedBootDiskType, architectures, rule, wantsSpot, reservationMachineType, autopilotEnabled, autopilotManaged, isStateless)
	if err != nil {
		return machinetypes.MachineSpec{}, machinetypes.SelectionTypeNone, err
	}
	architectures, err = s.limitArchitectures(spec, architectures)
	if err != nil {
		return machinetypes.MachineSpec{}, machinetypes.SelectionTypeNone, err
	}
	spec, err = s.limitMachineSpec(spec, architectures)
	if err != nil {
		return machinetypes.MachineSpec{}, machinetypes.SelectionTypeNone, err
	}
	return spec, selectionType, nil
}

// selectMachineSpec selects machine spec to be used for NAP node groups.
// Note: The returned machine spec is not validated here.
func (s Selector) selectMachineSpec(
	labelReq podrequirements.LabelRequirements,
	specifiedGpu, specifiedTpu, specifiedBootDiskType string,
	architectures map[gce.SystemArchitecture]bool,
	rule rules.Rule,
	wantsSpot bool,
	reservationMachineType string,
	autopilotEnabled, autopilotManaged, isStateless bool,
) (machinetypes.MachineSpec, machinetypes.SelectionType, errors.AutoscalerError) {
	families, computeClassName, selectionType, err := s.selectMachineGroup(labelReq, specifiedGpu, specifiedTpu, architectures, rule, wantsSpot, reservationMachineType, autopilotEnabled, autopilotManaged, isStateless)
	if err != nil {
		return machinetypes.MachineSpec{}, machinetypes.SelectionTypeNone, err
	}
	minCpuPlatform, err := s.selectMinCpuPlatform(families, labelReq, rule)
	if err != nil {
		return machinetypes.MachineSpec{}, machinetypes.SelectionTypeNone, err
	}
	var explicitMachineTypes []string
	if reservationMachineType != "" {
		explicitMachineTypes = append(explicitMachineTypes, reservationMachineType)
	}
	return machinetypes.MachineSpec{
		Families:                 families,
		ComputeClassName:         computeClassName,
		MinCpuPlatform:           minCpuPlatform,
		GpuType:                  specifiedGpu,
		TpuType:                  specifiedTpu,
		BootDiskType:             specifiedBootDiskType,
		ExplicitMachineTypes:     explicitMachineTypes,
		ConfidentialNodesEnabled: s.CloudProvider.AreConfidentialNodesEnabled(),
		ConfidentialNodeType:     s.CloudProvider.GetConfidentialInstanceType(),
	}, selectionType, nil
}

// selectMachineGroup selects machine families and compute class name to be used for NAP node groups.
// Note: The returned machine families and compute class is not necessarily validated here.
func (s Selector) selectMachineGroup(labelReq podrequirements.LabelRequirements,
	specifiedGpu, specifiedTpu string, architectures map[gce.SystemArchitecture]bool,
	rule rules.Rule, wantsSpot bool, reservationMachineType string,
	autopilotEnabled, autopilotManaged, isStateless bool) ([]machinetypes.MachineFamily, string, machinetypes.SelectionType, errors.AutoscalerError) {
	podCrdFamilies, podCrdFamiliesSpecified, err := crdMachineFamilies(s.CloudProvider, rule)
	if err != nil {
		return nil, "", machinetypes.SelectionTypeNone, err
	}

	// Evaluate E4 and E4A eligibility
	isE4Enabled := s.isE4Enabled(autopilotEnabled, autopilotManaged, isStateless)
	isE4aEnabled := (autopilotEnabled && s.CloudProvider.IsResizableVmEnabledInAutopilot(machinetypes.E4A.Name())) || (autopilotManaged && s.CloudProvider.IsResizableVmWithinPodFamilyEnabled(machinetypes.E4A.Name()))
	isExtendedFallbacksEnabled := s.isExtendedFallbacksEnabled(autopilotEnabled, autopilotManaged, isStateless)

	// If crd rule specifies families, always try to use it.
	if podCrdFamiliesSpecified {
		if !s.CloudProvider.IsEkSpotEnabled() {
			podCrdFamilies = filterEkMachineFamilyIfNotEnabled(podCrdFamilies, wantsSpot, s.CloudProvider.IsResizableVmWithinPodFamilyEnabled(machinetypes.EK.Name()))
		}
		podCrdFamilies = filterE4aMachineFamilyIfNotEnabled(podCrdFamilies, isE4aEnabled)
		if rule.PodFamilyName() == rules.GeneralPurposePodFamily {
			podCrdFamilies = s.filterE4MachineFamilyIfNotEnabled(podCrdFamilies, isE4Enabled)
			if isExtendedFallbacksEnabled {
				podCrdFamilies = slices.Clone(podCrdFamilies)
				podCrdFamilies = append(podCrdFamilies, rules.ExtendedFallbacks...)
			}
		}
		// Only filter ARM machine fallbacks if the rule is specifically for GeneralPurposeArmPodFamily.
		if rule.PodFamilyName() == rules.GeneralPurposeArmPodFamily {
			podCrdFamilies = filterArmMachineFallbacksIfNotEnabled(podCrdFamilies, s.CloudProvider.IsArmMachineFallbacksEnabled())
			if len(podCrdFamilies) == 0 {
				return []machinetypes.MachineFamily{}, "", machinetypes.SelectionTypeSpecified, NewPodFamilyUnknownError(rule.PodFamilyName())
			}
		}
		if len(podCrdFamilies) == 0 {
			return nil, "", machinetypes.SelectionTypeNone, NewPodFamilyUnknownError(rule.PodFamilyName() + " after filtering out disabled machine families")
		}
		return podCrdFamilies, "", machinetypes.SelectionTypeSpecified, nil
	}

	podClass, predefinedClassSpecified, customClassSpecified, err := podComputeClass(labelReq)
	if err != nil {
		return nil, "", machinetypes.SelectionTypeNone, err
	}
	machineFamily, machineFamilySpecified, err := podMachineFamily(s.CloudProvider, labelReq)
	if err != nil {
		return nil, "", machinetypes.SelectionTypeNone, err
	}

	allowE4A := isE4aEnabled && !predefinedClassSpecified

	if predefinedClassSpecified {
		// Compute classes are supported only in Autopilot.
		if !s.CloudProvider.IsAutopilotEnabled() {
			return nil, "", machinetypes.SelectionTypeNone, NewComputeClassNonAutopilotError(podClass.Name())
		}
		// Accelerator compute class
		if podClass.IsAcceleratorClass() {
			// Accelerator compute class should not specify machine family.
			if machineFamilySpecified {
				return nil, "", machinetypes.SelectionTypeNone, NewComputeClassWithMachineFamilyError(podClass.Name())
			}
			// Accelerator compute class must specify GPU or TPU.
			if specifiedGpu == "" && specifiedTpu == "" {
				return nil, "", machinetypes.SelectionTypeNone, NewComputeClassWithoutAcceleratorError(podClass.Name())
			}
			// Accelerator compute class should use machineSelection based on GPU and TPU.
			mf, selectionType := s.defaultFamilyForPod(architectures, specifiedGpu, specifiedTpu, allowE4A)
			return []machinetypes.MachineFamily{mf}, podClass.Name(), selectionType, nil
		}
		// Compute classes should only be specified with machine families that
		// use slice of hardware.
		machineFamilies := podClass.MachineFamilies()
		if machineFamilySpecified && !podClass.IsSliceOfHardware() {
			return nil, "", machinetypes.SelectionTypeNone, NewComputeClassWithMachineFamilyError(podClass.Name())
		}
		if !machineFamilySpecified && podClass.IsSliceOfHardware() {
			// Slice of hardware compute classes must specify machine family
			// except when Arm arch is specified, and would default to use C4A.
			if len(architectures) == 1 && architectures[gce.Arm64] {
				mf, selectionType := s.DefaultMachineFamilyForArm(allowE4A)
				return []machinetypes.MachineFamily{mf}, podClass.Name(), selectionType, nil
			}
			return nil, "", machinetypes.SelectionTypeNone, NewComputeClassWithoutMachineFamilyError(podClass.Name())
		}
		if !machineFamilySpecified && !podClass.IsSliceOfHardware() {
			// If pod specifies a compute class, always try to use it.
			// Non slice of hardware compute classes should return all
			// machine families supported by the compute class.

			if podClass.Name() == "autopilot" {
				machineFamilies = s.filterE4MachineFamilyIfNotEnabled(machineFamilies, isE4Enabled)
			}

			return machineFamilies, podClass.Name(), machinetypes.SelectionTypeSpecified, nil
		}
		// Slice of hardware compute class must specify valid machine family.
		for _, f := range machineFamilies {
			if f.Name() == machineFamily.Name() {
				return []machinetypes.MachineFamily{machineFamily}, podClass.Name(), machinetypes.SelectionTypeSpecified, nil
			}
		}
		// No valid machine-family for slice-of-hardware compute class found.
		return nil, "", machinetypes.SelectionTypeNone, NewComputeClassWithInvalidMachineFamilyError(podClass.Name(), machineFamily.Name())
	}
	if machineFamilySpecified {
		// If pod specifies a family, always try to use it.
		return []machinetypes.MachineFamily{machineFamily}, "", machinetypes.SelectionTypeSpecified, nil
	}

	if reservationMachineType != "" {
		reservationMachineFamily, err := s.CloudProvider.MachineConfigProvider().GetMachineFamilyFromMachineName(reservationMachineType)
		if err != nil {
			return nil, "", machinetypes.SelectionTypeNone, NewMachineTypeNotSupportedError(reservationMachineType)
		}
		return []machinetypes.MachineFamily{reservationMachineFamily}, "", machinetypes.SelectionTypeImplied, nil
	}

	defaultFamily, selectionType := s.defaultFamilyForPod(architectures, specifiedGpu, specifiedTpu, allowE4A)
	// Confidential nodes should never use EK machine family as default.
	if s.CloudProvider.IsResizableVmEnabledInAutopilot(machinetypes.EK.Name()) && !s.CloudProvider.AreConfidentialNodesEnabled() {
		if s.CloudProvider.IsEkSpotEnabled() || (!customClassSpecified && !wantsSpot) {
			// Default family is used as fallback when EKs are in backoff.
			return []machinetypes.MachineFamily{machinetypes.EK, defaultFamily}, "", selectionType, nil
		}
	}
	return []machinetypes.MachineFamily{defaultFamily}, "", selectionType, nil
}

// selectMinCpuPlatform selects a min_cpu_platform to be used for NAP node groups.
// Note: The returned platform is not necessarily validated here (even against the provided family).
func (s Selector) selectMinCpuPlatform(families []machinetypes.MachineFamily, labelReq podrequirements.LabelRequirements, rule rules.Rule) (machinetypes.CpuPlatform, errors.AutoscalerError) {
	// If the pods have a corresponding CRD rule, always try to use it. If it's not supported on the chosen
	// family, we'll fail during validation. In case of any errors here we want to error out to provide
	// an explicit error for this pod.
	ruleMinCpuPlatform, ruleSpecified, err := crdMinCpuPlatform(rule)
	if err != nil {
		return machinetypes.UnknownPlatform, err
	}
	if ruleSpecified {
		return ruleMinCpuPlatform, nil
	}

	// If pod specifies min_cpu_platform, always try to use it. If it's not supported on the chosen
	// family, we'll fail during validation. In case of any errors here we want to error out to provide
	// an explicit error for this pod.
	podPlatform, podSpecified, err := podMinCpuPlatform(labelReq)
	if err != nil {
		return machinetypes.UnknownPlatform, err
	}
	if podSpecified {
		return podPlatform, nil
	}

	// Cluster-wide platform is only used if it's compatible with any one of the chosen families. If that's not the case (or if there
	// are any related errors), we don't want to block NAP completely, so we fall back to AnyPlatform instead of erroring out.
	clusterWidePlatform, clusterWideSpecified, err := s.clusterWideMinCpuPlatform()
	if err != nil {
		// Fall back to AnyPlatform, only log the error here.
		klog.Warningf("error while retrieving default min_cpu_platform for NAP: %v", err)
		return machinetypes.AnyPlatform, nil
	}
	if clusterWideSpecified {
		var names []string
		for _, family := range families {
			names = append(names, family.Name())
			if family.IsPlatformSupported(clusterWidePlatform) {
				return clusterWidePlatform, nil
			}
		}
		// Fall back to AnyPlatform, only log the error here.
		klog.Warningf("autoprovisioning_node_pool_defaults.min_cpu_platform %q not supported for machine families %q - ignoring",
			machinetypes.CpuPlatformDebugName(clusterWidePlatform), strings.Join(names, ","))
		return machinetypes.AnyPlatform, nil
	}

	// No platform specified anywhere - use AnyPlatform.
	return machinetypes.AnyPlatform, nil
}

// limitArchitectures limits the used architectures based on machine spec and cluster settings
func (s Selector) limitArchitectures(spec machinetypes.MachineSpec, architectures map[gce.SystemArchitecture]bool) (map[gce.SystemArchitecture]bool, errors.AutoscalerError) {
	// Use the default architecture for Scale-Out compute class workloads if both min cpu platform and architecture are not specified.
	// Currently, it is not possible to specify the compute class and min cpu platform for a pod. If this ever changes then this logic needs to be revisited.
	if spec.ComputeClassName == machinetypes.ScaleOutClass.Name() && spec.MinCpuPlatform == machinetypes.AnyPlatform && len(architectures) == 0 {
		return map[gce.SystemArchitecture]bool{gce.DefaultArch: true}, nil
	}
	return architectures, nil
}

// limitMachineSpec limits the number of machine families in the spec, checking their
// comparability with cluster settings, pod requirements, and spec parameters. If all
// machine families are filtered then the function returns an error.
func (s Selector) limitMachineSpec(spec machinetypes.MachineSpec, architectures map[gce.SystemArchitecture]bool) (machinetypes.MachineSpec, errors.AutoscalerError) {
	var errs []errors.AutoscalerError
	var families []machinetypes.MachineFamily
	for _, family := range spec.Families {
		err := s.validateMachineFamily(family, spec, architectures)
		if err != nil {
			errs = append(errs, err)
		} else {
			families = append(families, family)
		}
	}
	if len(families) > 0 {
		spec.Families = families
		return spec, nil
	}

	err := combineMachineFamilyErrors(spec, errs)
	return machinetypes.MachineSpec{}, err
}

func filterEkMachineFamilyIfNotEnabled(podFamilyMachineFamilies []machinetypes.MachineFamily, wantsSpot, isEkWithinPodFamilyEnabled bool) []machinetypes.MachineFamily {
	if isEkWithinPodFamilyEnabled && !wantsSpot {
		return podFamilyMachineFamilies
	}
	return filterMachineFamilyByName(podFamilyMachineFamilies, machinetypes.EK.Name())
}

func filterE4aMachineFamilyIfNotEnabled(podFamilyMachineFamilies []machinetypes.MachineFamily, isE4aEnabled bool) []machinetypes.MachineFamily {
	if isE4aEnabled {
		return podFamilyMachineFamilies
	}
	return filterMachineFamilyByName(podFamilyMachineFamilies, machinetypes.E4A.Name())
}

func (s Selector) isE4Enabled(autopilotEnabled, autopilotManaged, isStateless bool) bool {
	isE4EnabledInAutopilot := s.CloudProvider.IsResizableVmEnabledInAutopilot(machinetypes.E4.Name())
	isE4EnabledOnManagedNodes := s.CloudProvider.IsResizableVmWithinPodFamilyEnabled(machinetypes.E4.Name())

	if !(autopilotEnabled && isE4EnabledInAutopilot) && !(autopilotManaged && isE4EnabledOnManagedNodes) {
		return false
	}

	if s.CloudProvider.IsE2lessRegion() {
		return true // E4 is mandatory in E2-less regions
	}

	// In all other regions, only enable E4 for stateless workloads
	return isStateless
}

func (s Selector) isExtendedFallbacksEnabled(autopilotEnabled, autopilotManaged, isStateless bool) bool {
	if !(autopilotEnabled || autopilotManaged) {
		return false
	}
	return s.CloudProvider.IsExtendedFallbacksEnabled() && isStateless
}

func (s Selector) filterE4MachineFamilyIfNotEnabled(families []machinetypes.MachineFamily, isE4Enabled bool) []machinetypes.MachineFamily {
	if !isE4Enabled {
		// Remove E4 to prevent it from being used in mixed regions during Phase 1/2 for stateful workloads
		return filterMachineFamilyByName(families, machinetypes.E4.Name())
	}

	if s.CloudProvider.IsE2lessRegion() {
		// E2-less: Fast-path to E4 by stripping E2/EK
		filtered := filterMachineFamilyByName(families, machinetypes.E2.Name())
		filtered = filterMachineFamilyByName(filtered, machinetypes.EK.Name())
		return filtered
	}

	// Stateless in other regions: Keep E4 alongside E2/EK
	return families
}

func filterArmMachineFallbacksIfNotEnabled(podFamilyMachineFamilies []machinetypes.MachineFamily, isEnabled bool) []machinetypes.MachineFamily {
	if isEnabled {
		return podFamilyMachineFamilies
	}
	filtered := []machinetypes.MachineFamily{}
	for _, f := range podFamilyMachineFamilies {
		if f.Name() != machinetypes.N4A.Name() && f.Name() != machinetypes.C4A.Name() {
			filtered = append(filtered, f)
		}
	}
	return filtered
}

func filterMachineFamilyByName(families []machinetypes.MachineFamily, name string) []machinetypes.MachineFamily {
	filtered := []machinetypes.MachineFamily{}
	for _, f := range families {
		if f.Name() != name {
			filtered = append(filtered, f)
		}
	}
	return filtered
}

func (s Selector) ValidateSpec(spec machinetypes.MachineSpec) errors.AutoscalerError {
	var errs []errors.AutoscalerError
	for _, family := range spec.Families {
		err := s.validateMachineFamily(family, spec, nil)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	err := combineMachineFamilyErrors(spec, errs)
	return err
}

func (s Selector) validateMachineFamily(family machinetypes.MachineFamily, spec machinetypes.MachineSpec,
	architectures map[gce.SystemArchitecture]bool) errors.AutoscalerError {
	machineGroupName := fmt.Sprintf("machine family %q", family.Name())
	if spec.ComputeClassName != "" {
		// If compute class is specified than we want all the errors to specify this
		// class name as the machine group name - this hides the machine families used
		// by compute class from the user.
		machineGroupName = fmt.Sprintf("compute class %q", spec.ComputeClassName)
	}
	// Some machine families are not supported in NAP.
	if len(family.AutoprovisionedMachineTypes(machinetypes.NoConstraints)) == 0 {
		return NewMachineFamilyNotSupportedError(family.Name())
	}
	if !family.IsPlatformSupported(spec.MinCpuPlatform) {
		return NewMinCpuPlatformInvalidError(machineGroupName, machinetypes.CpuPlatformDebugName(spec.MinCpuPlatform))
	}
	if !family.IsGpuTypeSupported(spec.GpuType) {
		return NewGpuIncompatibleError(machineGroupName, spec.GpuType)
	}
	if !family.IsTpuTypeSupported(spec.TpuType) {
		return NewTpuIncompatibleError(machineGroupName, spec.TpuType)
	}
	if !family.IsDiskTypeSupported(spec.BootDiskType) {
		return NewBootDiskTypeIncompatibleError(machineGroupName, spec.BootDiskType)
	}
	// K80 has some additional restrictions on min_cpu_platform.
	if spec.GpuType == machinetypes.NvidiaTeslaK80.Name() && machinetypes.PlatformIsAtLeast(spec.MinCpuPlatform, machinetypes.IntelSkylake) {
		return NewGpuMinCpuPlatformIncompatibleError(machinetypes.CpuPlatformDebugName(spec.MinCpuPlatform), spec.GpuType)
	}
	if s.CloudProvider.AreConfidentialNodesEnabled() {
		confidentialNodeType := s.CloudProvider.GetConfidentialInstanceType()
		if (confidentialNodeType == "" && !supportsDefaultConfidentialNodes(family)) || (confidentialNodeType != "" && !family.IsConfidentialNodeTypeSupported(confidentialNodeType)) {
			return NewConfidentialNodesIncompatibleError(machineGroupName)
		}
	}
	// The pod should be compatible with all architectures it specifies, so it's enough if a family is compatible with
	// any of them.
	if len(architectures) > 0 && !architectures[family.SystemArchitecture()] {
		var archNames []string
		for arch := range architectures {
			archNames = append(archNames, arch.Name())
		}
		sort.Strings(archNames)
		return NewSystemArchitectureIncompatibleError(machineGroupName, strings.Join(archNames, ","))
	}
	if len(spec.ExplicitMachineTypes) > 0 && !family.AreMachineTypesSupported(spec.ExplicitMachineTypes) {
		return NewMachineTypesUnsupportedByFamilyError(spec.ExplicitMachineTypes, machineGroupName)
	}
	constraints := machinetypes.Constraints{
		CpuPlatform:               spec.MinCpuPlatform,
		GpuType:                   spec.GpuType,
		ExplicitMachineTypes:      spec.ExplicitMachineTypes,
		ConfidentialNodesRequired: spec.ConfidentialNodesEnabled,
		ConfidentialNodeType:      spec.ConfidentialNodeType,
	}
	if !family.AreConstraintsSupported(constraints) {
		return NewMachineConfigInvalidError(constraints.String(), "no machine types supporting all parts of the config found")
	}
	return nil
}

// combineMachineFamilyErrors combines NAP errors returned for machine families.
// The function assumes that at least one error is provided as an argument.
func combineMachineFamilyErrors(_ machinetypes.MachineSpec, errs []errors.AutoscalerError) errors.AutoscalerError {
	// return the first error because the NAP error handling does not support multiple errors per pod yet.
	// TODO(b/233887511): support multiple NAP errors per pod
	return errs[0]
}

// DefaultMachineFamily returns the default machine family based on cluster spec.
func (s Selector) DefaultMachineFamily() (machinetypes.MachineFamily, machinetypes.SelectionType) {
	if s.CloudProvider.AreConfidentialNodesEnabled() {
		// Current supported confidential machine types: https://docs.cloud.google.com/confidential-computing/confidential-vm/docs/supported-configurations#machine-type-cpu-zone.
		family := s.DefaultMachineFamilyForConfidentialNodes(s.CloudProvider.GetConfidentialInstanceType())
		return family, machinetypes.SelectionTypeImplied
	}
	return s.CloudProvider.GetAutoprovisioningDefaultFamily(), machinetypes.SelectionTypeDefault
}

// ARM machines would default to C4A, except for those allowlisted for E4A that are using autopilot mode. See go/autopilot-arm-container-optimized-pods.
// We are not handling x86 arch since we assume the AutoprovisioningDefaultFamily is going to be x86
// This logic would need to change we allow setting ARM machines as default
func (s Selector) DefaultMachineFamilyForArm(allowE4A bool) (machinetypes.MachineFamily, machinetypes.SelectionType) {
	// If allowlisted, ARM will default to E4A for autopilot-arm and custom compute classes.
	if allowE4A {
		return machinetypes.E4A, machinetypes.SelectionTypeImplied
	}
	return machinetypes.C4A, machinetypes.SelectionTypeImplied
}

// DefaultMachineFamilyForConfidentialNodes returns the default machine family for confidential nodes.
func (s Selector) DefaultMachineFamilyForConfidentialNodes(confidentialNodeType string) machinetypes.MachineFamily {
	var family machinetypes.MachineFamily
	switch confidentialNodeType {
	case gkelabels.SEVConfidentialNodeTypeValue:
		family = machinetypes.N2D
	case gkelabels.SEVSNPConfidentialNodeTypeValue:
		family = machinetypes.N2D
	case gkelabels.TDXConfidentialNodeTypeValue:
		family = machinetypes.C3
	default:
		// Fall back to N2D for backward compatibility. Invalid instance types are rejected by control plane validation.
		// go/gke-autoscaler-confidential-nodes-validation
		klog.V(4).Infof("confidential nodes instance type %v is not supported yet, default to N2D machine family", confidentialNodeType)
		family = machinetypes.N2D
	}
	klog.V(4).Infof("Returning family: %v for confidential instance type: %v", family.Name(), confidentialNodeType)
	return family
}

// defaultFamilyForPod returns the default machine family to use when the pod doesn't specify any.
func (s Selector) defaultFamilyForPod(architectures map[gce.SystemArchitecture]bool, specifiedGpu, specifiedTpu string, allowE4A bool) (machinetypes.MachineFamily, machinetypes.SelectionType) {
	if machineFamily, found := s.CloudProvider.MachineConfigProvider().MachineFamilyForGpuType(specifiedGpu); found {
		return machineFamily, machinetypes.SelectionTypeImplied
	}
	if machineFamily, found := s.CloudProvider.MachineConfigProvider().MachineFamilyForTpuType(specifiedTpu); found {
		return machineFamily, machinetypes.SelectionTypeImplied
	}
	// ARM machines would default to C4A, except for those allowlisted for E4A that are using autopilot mode. See go/autopilot-arm-container-optimized-pods.
	// We are not handling x86 arch since we assume the AutoprovisioningDefaultFamily is going to be x86
	// This logic would need to change we allow setting ARM machines as default
	if len(architectures) == 1 && architectures[gce.Arm64] {
		return s.DefaultMachineFamilyForArm(allowE4A)
	}

	return s.DefaultMachineFamily()
}

// clusterWideMinCpuPlatform extracts the cluster-wide min_cpu_platform specified in AutoprovisioningNodePoolDefaults.
//
//	The second return value signifies whether a platform was specified.
func (s Selector) clusterWideMinCpuPlatform() (machinetypes.CpuPlatform, bool, errors.AutoscalerError) {
	platformName := s.CloudProvider.GetDefaultNodePoolMinCpuPlatform()
	if platformName == "" {
		return machinetypes.UnknownPlatform, false, nil
	}
	platform, err := machinetypes.ToCpuPlatform(platformName)
	if err != nil {
		return machinetypes.UnknownPlatform, true, NewMinCpuPlatformUnknownError(platformName)
	}
	return platform, true, nil
}

// supportsDefaultConfidentialNodes returns whether the machine family supports
// the default confidential node types (SEV).
func supportsDefaultConfidentialNodes(family machinetypes.MachineFamily) bool {
	return family.IsConfidentialNodeTypeSupported(gkelabels.SEVConfidentialNodeTypeValue)
}
