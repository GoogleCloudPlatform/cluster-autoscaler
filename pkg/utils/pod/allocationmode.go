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

package pod

import (
	v1 "k8s.io/api/core/v1"
	coreK8s "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/helper"
)

type DeviceAllocationMode int

const (
	// AllocationModeUnknown is the default value for the device allocation mode
	AllocationModeUnknown DeviceAllocationMode = 0
	// AllocationModeNone is the device allocation mode for pods that do not request any extended resources or resource claims
	AllocationModeNone DeviceAllocationMode = 1
	// AllocationModeDra is the device allocation mode for pods that do not request any extended resources but request DRA resource claims
	AllocationModeDra DeviceAllocationMode = 2
	// AllocationModeExtendedResources is the device allocation mode for pods that request extended resources and are not handled by the DRA driver
	AllocationModeExtendedResources DeviceAllocationMode = 3
	// AllocationModeExtendedResourcesDra is the device allocation mode for pods that request extended resources handled by the DRA driver
	AllocationModeExtendedResourcesDra DeviceAllocationMode = 4
	// AllocationModeMixed is the device allocation mode for pods that are using multiple device allocation methods
	AllocationModeMixed DeviceAllocationMode = 5
)

func (m DeviceAllocationMode) String() string {
	switch m {
	case AllocationModeNone:
		return "none"
	case AllocationModeDra:
		return "dra"
	case AllocationModeExtendedResources:
		return "extendedResources"
	case AllocationModeExtendedResourcesDra:
		return "extendedResourcesDra"
	case AllocationModeMixed:
		return "mixed"
	default:
		return "unknown"
	}
}

// filterExtendedResourcesInplace removes non-extended resources from the resource list
// and returns the filtered list
func filterExtendedResourcesInplace(resourceList v1.ResourceList) v1.ResourceList {
	for resourceName := range resourceList {
		// We need to explicitly convert the resource name from
		// k8s.io/api/core/v1 => k8s.io/kubernetes/pkg/apis/core
		// because the helper package explicitly expects the type
		// defined in the kubernetes repo and not in the staging
		// repository
		resource := coreK8s.ResourceName(resourceName)
		if !helper.IsExtendedResourceName(resource) {
			delete(resourceList, resourceName)
		}
	}

	return resourceList
}

// podRequestsResourceClaims checks if the pod requests any resource claims.
// We do not check that the resource claims are valid or referenced by any
// contains - if any claim is defined on the pod level - we assume it's valid
func podRequestsResourceClaims(pod *v1.Pod) bool {
	return len(pod.Spec.ResourceClaims) > 0
}

// extendedResourceAllocationModeForPod determines the extended resource allocation mode for a pod
// based on the extended resources it requests and the extended resource claim status.
//
// Warning: resource list map can be modified in place after this function is called
func extendedResourceAllocationModeForPod(pod *v1.Pod, extendedResources v1.ResourceList) DeviceAllocationMode {
	claimStatus := pod.Status.ExtendedResourceClaimStatus
	if claimStatus == nil || claimStatus.ResourceClaimName == "" || len(claimStatus.RequestMappings) == 0 {
		return AllocationModeExtendedResources
	}

	// We assume that that if the resource name was handled by the DRA driver
	// a single time - it's sufficient to consider that all the resources
	// with the same name are handled by the DRA driver. Otherwise we would
	// need to go through all the contains and individual resource requests
	// to determine if they are handled by the DRA driver or not.
	for _, request := range claimStatus.RequestMappings {
		delete(extendedResources, v1.ResourceName(request.ResourceName))
	}

	// If we determined that all the extended resources are handled by the
	// DRA driver we return AllocationModeExtendedResourcesDra
	if len(extendedResources) == 0 {
		return AllocationModeExtendedResourcesDra
	}

	return AllocationModeMixed
}

// DeviceAllocationModeForPod determines the device allocation mode for a pod
// based on the resources it requests and binding status.
func DeviceAllocationModeForPod(pod *v1.Pod) DeviceAllocationMode {
	podResourceList := PodRequests(pod)
	// PodRequests() always produces a new ResourceList and doesn't keep
	// references to data stored in the pod itself, modifying it inplace
	// is safe and doesn't affect the pod object.
	extendedResources := filterExtendedResourcesInplace(podResourceList)
	hasClaims := podRequestsResourceClaims(pod)
	if len(extendedResources) > 0 && hasClaims {
		return AllocationModeMixed
	}

	if hasClaims {
		return AllocationModeDra
	}

	if len(extendedResources) > 0 {
		// Extended resources can be handled by the DRA driver, we want to determine
		// if they are handled by the DRA driver or not
		return extendedResourceAllocationModeForPod(pod, extendedResources)
	}

	return AllocationModeNone
}
