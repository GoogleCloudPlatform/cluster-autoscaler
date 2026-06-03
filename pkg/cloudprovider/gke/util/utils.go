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

package util

import (
	"fmt"
	"strings"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/klog/v2"
)

// RightShiftTransformResourceLimiter adds an offset equivalent of additionalResources
// to the specified resource limiter
func RightShiftTransformResourceLimiter(resourceLimiter *cloudprovider.ResourceLimiter,
	addtionalResources map[string]int64) *cloudprovider.ResourceLimiter {
	if resourceLimiter == nil {
		return nil
	}
	resources := resourceLimiter.GetResources()
	allResources := make(map[string]bool, len(resources))
	for _, resource := range resources {
		allResources[resource] = true
	}
	rightShiftedResourcesMinResource := make(map[string]int64, len(allResources))
	rightShiftedResourcesMaxResource := make(map[string]int64, len(allResources))
	for resource := range allResources {
		rightShiftedResourcesMinResource[resource] = resourceLimiter.GetMin(resource) + addtionalResources[resource]
		rightShiftedResourcesMaxResource[resource] = resourceLimiter.GetMax(resource) + addtionalResources[resource]
	}
	return cloudprovider.NewResourceLimiter(rightShiftedResourcesMinResource, rightShiftedResourcesMaxResource)
}

// GetNodeNameFromInstance parses instance name to return the GKE node name.
// Returns nil on a nil instance. Returns a non nil error in case of a parsing
// error.
func GetNodeNameFromInstance(instance *cloudprovider.Instance) (string, error) {
	if instance == nil {
		return "", nil
	}
	parts := strings.Split(instance.Id, "/")
	if len(parts) == 1 {
		return "", fmt.Errorf("error parsing instance id '%s'", instance.Id)
	}
	return parts[len(parts)-1], nil
}

// GetRegionFromLocation returns the region for given location, which can be either plain region or zone.
func GetRegionFromLocation(location string) (string, error) {
	const tpcPrefix = "u"
	// Zone expected format is {}-{}-{}, e.g. europe-central2-a.
	// Region expected format is {}-{}, e.g. europe-central2.
	// TPC zone expected format is u-{}-{}-{}, e.g. u-europe-central2-a.
	// TPC regions expected format is u-{}-{}, e.g. u-europe-central2. See go/tpc-region-naming.
	splitLocation := strings.Split(location, "-")
	switch len(splitLocation) {
	case 4:
		if splitLocation[0] == tpcPrefix {
			return strings.Join(splitLocation[0:3], "-"), nil
		}
	case 3:
		if splitLocation[0] == tpcPrefix {
			return location, nil
		}
		return strings.Join(splitLocation[0:2], "-"), nil
	case 2:
		return location, nil
	}
	return "", fmt.Errorf("location in unexpected format, expected: {locale}-{region}-{zone}, {locale}-{region} or u-{locale}-{region}, got: %v", location)
}

// ExtractOsDistributionFromImageType return operating system distribution for given image type.
func ExtractOsDistributionFromImageType(image string) gce.OperatingSystemDistribution {
	switch strings.ToLower(image) {
	case string(gce.OperatingSystemDistributionUbuntu):
		return gce.OperatingSystemDistributionUbuntu
	case string(gce.OperatingSystemDistributionCOS):
		return gce.OperatingSystemDistributionCOS
	case "cos_containerd":
		return gce.OperatingSystemDistributionCOS
	case "ubuntu_containerd":
		return gce.OperatingSystemDistributionUbuntu
	case string(gce.OperatingSystemDistributionWindowsLTSC):
		return gce.OperatingSystemDistributionWindowsLTSC
	case string(gce.OperatingSystemDistributionWindowsSAC):
		return gce.OperatingSystemDistributionWindowsSAC
	default:
		klog.Errorf("Unknown OperatingSystemDistribution for %s", image)
		return gce.OperatingSystemDistributionUnknown
	}
}
