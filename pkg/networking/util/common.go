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

// Package util contains common functions for
// interacting with networking resources. It's in a separate package than the
// rest of networking functionalities to avoid import cycle.
package util

import (
	"strings"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	MultiNetworkResourceNamePrefix = "networking.gke.io.networks/"

	MultiNetworkResourceNameSuffix = ".IP"

	MultiNetworkingResourceNamePattern        = MultiNetworkResourceNamePrefix + "%s" + MultiNetworkResourceNameSuffix
	HighPerformanceNetworkResourceNamePattern = MultiNetworkResourceNamePrefix + "%s"

	// DefaultAdditionalMultiNetworkConfigMaxPodsPerNode specifies the default max
	// pods per node setting for networking resources.
	DefaultAdditionalMultiNetworkConfigMaxPodsPerNode = 4
)

var (
	// HighPerformanceDefaultResourceValue specifies the default resource value for high performance network.
	HighPerformanceDefaultResourceValue = resource.NewQuantity(1, resource.DecimalSI)
)

// GetNetworkResourcesNamesFromResourceList extracts networking resource names
// from given resource list. Such resource names need to match either multi
// network or high performance network naming pattern.
func GetNetworkResourcesNamesFromResourceList(resourceList apiv1.ResourceList) []string {
	var result []string
	for name := range resourceList {
		if IsNetworkResource(name.String()) {
			result = append(result, name.String())
		}
	}
	return result
}

// IsMultiNetworkResource checks whether given resource name represents multi
// network resource by checking if it matches multi network pattern.
func IsMultiNetworkResource(resourceName string) bool {
	return strings.HasPrefix(resourceName, MultiNetworkResourceNamePrefix) && strings.HasSuffix(resourceName, MultiNetworkResourceNameSuffix)
}

// IsHighPerformanceNetworkResource checks whether given resource name represents high
// performance network resource by checking if it matches high performance network pattern.
func IsHighPerformanceNetworkResource(resourceName string) bool {
	return strings.HasPrefix(resourceName, MultiNetworkResourceNamePrefix) && !strings.HasSuffix(resourceName, MultiNetworkResourceNameSuffix)
}

// IsNetworkResource checks whether given resource name represents either
// multi network or high performance network resource.
func IsNetworkResource(resourceName string) bool {
	return strings.HasPrefix(resourceName, MultiNetworkResourceNamePrefix)
}
