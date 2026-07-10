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
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	napprovider "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/napcloudprovider"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

// podMinCpuPlatform extracts the min_cpu_platform specified in pod label requirements. The second return value signifies
// whether a platform was specified.
func podMinCpuPlatform(labelReq podrequirements.LabelRequirements) (machinetypes.CpuPlatform, bool, errors.AutoscalerError) {
	platformNameFromRequested, requestedSpecified := labelReq.GetSingleValue(gkelabels.RequestedMinCpuPlatformLabel)

	var platformNamesFromSupported []string
	for platformNameKey := range labelReq.KeysWithPrefix(gkelabels.SupportedCpuPlatformKeyPrefix) {
		if val, _ := labelReq.GetSingleValue(platformNameKey); val == gkelabels.SupportedCpuPlatformValue {
			platformNamesFromSupported = append(platformNamesFromSupported, gkelabels.ExtractSupportedCpuPlatformFromKey(platformNameKey))
		}
	}

	platformsSpecified := len(platformNamesFromSupported)
	if requestedSpecified {
		platformsSpecified += 1
	}
	if platformsSpecified < 1 {
		return machinetypes.UnknownPlatform, false, nil
	}
	if platformsSpecified > 1 {
		return machinetypes.UnknownPlatform, true, NewMultipleMinCpuPlatformsError()
	}

	var platformName string
	if len(platformNamesFromSupported) > 0 {
		platformName = platformNamesFromSupported[0]
	} else {
		platformName = platformNameFromRequested
	}

	platform, err := machinetypes.ToCpuPlatform(platformName)
	if err != nil {
		return machinetypes.UnknownPlatform, true, NewMinCpuPlatformUnknownError(platformName)
	}
	return platform, true, nil
}

// podMachineFamily extracts the machine family specified in pod label requirements. The second return value signifies
// whether a machine family was specified.
func podMachineFamily(provider napprovider.AutoprovisioningCloudProvider, labelReq podrequirements.LabelRequirements) (machinetypes.MachineFamily, bool, errors.AutoscalerError) {
	familyName, isSpecified := labelReq.GetSingleValue(gkelabels.MachineFamilyLabel)
	if !isSpecified {
		return machinetypes.MachineFamily{}, false, nil
	}
	family, err := provider.MachineConfigProvider().ToMachineFamily(familyName)
	if err != nil {
		return machinetypes.MachineFamily{}, true, NewMachineFamilyUnknownError(familyName)
	}
	return family, true, nil
}

// podComputeClass extracts the compute class specified in pod label requirements.
func podComputeClass(labelReq podrequirements.LabelRequirements) (machinetypes.PredefinedComputeClass, bool, bool, errors.AutoscalerError) {
	className, isSpecified := labelReq.GetSingleValue(gkelabels.ComputeClassLabel)
	if !isSpecified {
		return machinetypes.PredefinedComputeClass{}, false, false, nil
	}

	class, err := machinetypes.ToPredefinedComputeClass(className)
	if err != nil {
		// Note: The assumption that unknown compute class should raise an error
		// is no longer true because of dynamically defined custom compute classes
		return machinetypes.PredefinedComputeClass{}, false, true, nil
	}
	return class, true, false, nil
}

// crdMachineFamilies extracts the machine families specified in pod CRD. The second return value signifies
// whether a machine families were specified.
func crdMachineFamilies(provider napprovider.AutoprovisioningCloudProvider, rule rules.Rule) ([]machinetypes.MachineFamily, bool, errors.AutoscalerError) {
	if rule == nil {
		return nil, false, nil
	}
	if rule.PodFamilyName() != "" {
		podFamilyMachineFamilies, err := rule.PodFamilyMachineFamilies()
		if err != nil {
			return nil, true, NewPodFamilyUnknownError(rule.PodFamilyName())
		}

		if rule.MachineFamily() != "" {
			familyName := rule.MachineFamily()
			family, err := provider.MachineConfigProvider().ToMachineFamily(familyName)
			if err != nil {
				return nil, true, NewMachineFamilyUnknownError(familyName)
			}
			for _, f := range podFamilyMachineFamilies {
				if f.Equal(family) {
					return []machinetypes.MachineFamily{family}, true, nil
				}
			}
			return nil, true, NewMachineFamilyUnknownError(familyName + " (not in pod family " + rule.PodFamilyName() + ")")
		}

		return podFamilyMachineFamilies, true, nil
	}
	if rule.MachineType() != "" {
		family, err := provider.MachineConfigProvider().GetMachineFamilyFromMachineName(rule.MachineType())
		if err != nil {
			return nil, false, NewMachineTypeNotSupportedError(rule.MachineType())
		}
		return []machinetypes.MachineFamily{family}, true, nil
	}
	if !rule.GpuRequest().Empty() {
		return nil, false, nil
	}

	if rule.MachineFamily() != "" {
		familyName := rule.MachineFamily()
		family, err := provider.MachineConfigProvider().ToMachineFamily(familyName)
		if err != nil {
			return nil, true, NewMachineFamilyUnknownError(familyName)
		}

		return []machinetypes.MachineFamily{family}, true, nil
	}

	return nil, false, nil
}

// podArchitectures extracts the architectures specified in pod label requirements.
func podArchitectures(labelReq podrequirements.LabelRequirements) (map[gce.SystemArchitecture]bool, errors.AutoscalerError) {
	archNames, found := labelReq.GetValues(apiv1.LabelArchStable)
	if !found {
		return nil, nil
	}
	result := map[gce.SystemArchitecture]bool{}
	for archName := range archNames.Get() {
		arch := gce.ToSystemArchitecture(archName)
		if arch == gce.UnknownArch {
			return nil, NewSystemArchitectureUnknownError(archName)
		}
		result[arch] = true
	}
	return result, nil
}

// crdMinCpuPlatform extracts the min cpu platform specified in pod CRD. The second return value signifies
// whether the rule was specified in the first place.
// CRD rule default min cpu platform value is set to AnyPlaform if not specified.
func crdMinCpuPlatform(rule rules.Rule) (machinetypes.CpuPlatform, bool, errors.AutoscalerError) {
	if rule == nil {
		return machinetypes.UnknownPlatform, false, nil
	}

	cpuPlatform, err := rule.MinCpuPlatform()
	if err != nil {
		return cpuPlatform, true, NewMinCpuPlatformUnknownError(rule.MinCpuPlatformString())
	}
	return cpuPlatform, true, nil
}
