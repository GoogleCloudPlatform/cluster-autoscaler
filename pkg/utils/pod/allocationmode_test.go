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
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestDeviceAllocationModeForPod(t *testing.T) {
	tests := map[string]struct {
		pod                *v1.Pod
		wantAllocationMode DeviceAllocationMode
	}{
		"PodWithGpuRequest": {
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									"nvidia.com/gpu": resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			wantAllocationMode: AllocationModeExtendedResources,
		},
		"PodWithGpuRequestAndLimit": {
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									"nvidia.com/gpu": resource.MustParse("1"),
								},
								Limits: v1.ResourceList{
									"nvidia.com/gpu": resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			wantAllocationMode: AllocationModeExtendedResources,
		},
		"ResourceClaimNoReference": {
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					ResourceClaims: []v1.PodResourceClaim{
						{
							Name: "test-claim",
						},
					},
					Containers: []v1.Container{
						{},
					},
				},
			},
			wantAllocationMode: AllocationModeDra,
		},
		"ResourceClaimWithContainerReference": {
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					ResourceClaims: []v1.PodResourceClaim{
						{
							Name: "test-claim",
						},
					},
					Containers: []v1.Container{
						{Resources: v1.ResourceRequirements{
							Claims: []v1.ResourceClaim{
								{
									Name: "test-claim",
								},
							},
						}},
					},
				},
			},
			wantAllocationMode: AllocationModeDra,
		},
		"DraExtendedResources": {
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									"example.com/extended": resource.MustParse("2"),
								},
							},
						},
					},
				},
				Status: v1.PodStatus{
					ExtendedResourceClaimStatus: &v1.PodExtendedResourceClaimStatus{
						ResourceClaimName: "test-claim",
						RequestMappings: []v1.ContainerExtendedResourceRequest{
							{
								ContainerName: "container-0",
								ResourceName:  "example.com/extended",
								RequestName:   "req-0",
							},
						},
					},
				},
			},
			wantAllocationMode: AllocationModeExtendedResourcesDra,
		},
		"MixedAllocationExtendedResources": {
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									"example.com/dra":     resource.MustParse("2"),
									"example.com/non-dra": resource.MustParse("1"),
								},
							},
						},
					},
				},
				Status: v1.PodStatus{
					ExtendedResourceClaimStatus: &v1.PodExtendedResourceClaimStatus{
						ResourceClaimName: "test-claim",
						RequestMappings: []v1.ContainerExtendedResourceRequest{
							{
								ContainerName: "container-0",
								ResourceName:  "example.com/dra",
								RequestName:   "req-0",
							},
						},
					},
				},
			},
			wantAllocationMode: AllocationModeMixed,
		},
		"MixedAllocationMultipleContainers": {
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "container-dra",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									"example.com/dra": resource.MustParse("1"),
								},
							},
						},
						{
							Name: "container-non-dra",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									"example.com/non-dra": resource.MustParse("1"),
								},
							},
						},
					},
				},
				Status: v1.PodStatus{
					ExtendedResourceClaimStatus: &v1.PodExtendedResourceClaimStatus{
						ResourceClaimName: "test-claim",
						RequestMappings: []v1.ContainerExtendedResourceRequest{
							{
								ContainerName: "container-dra",
								ResourceName:  "example.com/dra",
								RequestName:   "req-0",
							},
						},
					},
				},
			},
			wantAllocationMode: AllocationModeMixed,
		},
		"AllStandardResources": {
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									"cpu":               resource.MustParse("1"),
									"memory":            resource.MustParse("1Gi"),
									"ephemeral-storage": resource.MustParse("1Gi"),
								},
							},
						},
					},
				},
			},
			wantAllocationMode: AllocationModeNone,
		},
		"SingleStandardResource": {
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									"cpu": resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			wantAllocationMode: AllocationModeNone,
		},
		"ClaimsAndExtendedResources": {
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					ResourceClaims: []v1.PodResourceClaim{
						{
							Name: "test-claim",
						},
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									"example.com/extended": resource.MustParse("2"),
								},
							},
						},
					},
				},
				Status: v1.PodStatus{
					ExtendedResourceClaimStatus: &v1.PodExtendedResourceClaimStatus{
						ResourceClaimName: "test-claim",
						RequestMappings: []v1.ContainerExtendedResourceRequest{
							{
								ContainerName: "container-0",
								ResourceName:  "example.com/extended",
								RequestName:   "req-0",
							},
						},
					},
				},
			},
			wantAllocationMode: AllocationModeMixed,
		},
		"ClaimStatusMissingClaimName": {
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									"example.com/extended": resource.MustParse("2"),
								},
							},
						},
					},
				},
				Status: v1.PodStatus{
					ExtendedResourceClaimStatus: &v1.PodExtendedResourceClaimStatus{
						ResourceClaimName: "", // Missing claim name here causes fallback to AllocationModeExtendedResources
						RequestMappings: []v1.ContainerExtendedResourceRequest{
							{
								ContainerName: "container-0",
								ResourceName:  "example.com/extended",
								RequestName:   "req-0",
							},
						},
					},
				},
			},
			wantAllocationMode: AllocationModeExtendedResources,
		},
		"ClaimStatusEmptyRequestMappings": {
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									"example.com/extended": resource.MustParse("2"),
								},
							},
						},
					},
				},
				Status: v1.PodStatus{
					ExtendedResourceClaimStatus: &v1.PodExtendedResourceClaimStatus{
						ResourceClaimName: "test-claim",
						RequestMappings:   []v1.ContainerExtendedResourceRequest{},
					},
				},
			},
			wantAllocationMode: AllocationModeExtendedResources,
		},
		"MismatchedRequestMappings": {
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									"example.com/extended": resource.MustParse("2"),
								},
							},
						},
					},
				},
				Status: v1.PodStatus{
					ExtendedResourceClaimStatus: &v1.PodExtendedResourceClaimStatus{
						ResourceClaimName: "test-claim",
						RequestMappings: []v1.ContainerExtendedResourceRequest{
							{
								ContainerName: "container-0",
								ResourceName:  "other.com/gpu",
								RequestName:   "req-0",
							},
						},
					},
				},
			},
			wantAllocationMode: AllocationModeMixed,
		},
		"StandardAndFullyMappedExtended": {
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									"cpu":                  resource.MustParse("1"),
									"memory":               resource.MustParse("1Gi"),
									"example.com/extended": resource.MustParse("2"),
								},
							},
						},
					},
				},
				Status: v1.PodStatus{
					ExtendedResourceClaimStatus: &v1.PodExtendedResourceClaimStatus{
						ResourceClaimName: "test-claim",
						RequestMappings: []v1.ContainerExtendedResourceRequest{
							{
								ContainerName: "container-0",
								ResourceName:  "example.com/extended",
								RequestName:   "req-0",
							},
						},
					},
				},
			},
			wantAllocationMode: AllocationModeExtendedResourcesDra,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := DeviceAllocationModeForPod(tc.pod)
			if got != tc.wantAllocationMode {
				t.Errorf("DeviceAllocationModeForPod() = %v, want %v", got, tc.wantAllocationMode)
			}
		})
	}
}
