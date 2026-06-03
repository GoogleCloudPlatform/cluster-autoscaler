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

package testing

import (
	"context"
	"testing"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
)

func NewResourceClaim(name string, exactReq *resourceapi.ExactDeviceRequest) *resourceapi.ResourceClaim {
	return &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{
				Requests: []resourceapi.DeviceRequest{
					{
						Name:    "gpu-request",
						Exactly: exactReq,
					},
				},
			},
		},
	}
}

func GpuDeviceExactReq(cnt int64) *resourceapi.ExactDeviceRequest {
	return &resourceapi.ExactDeviceRequest{
		DeviceClassName: gce.DraGPUDriver,
		AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
		Count:           cnt,
	}
}

func MustAllocateClaim(t *testing.T, ctx context.Context, existingSlices []resourceapi.ResourceSlice, claim *resourceapi.ResourceClaim, gpuCnt int, nodeName string) {
	for _, s := range existingSlices {
		if s.Spec.Driver == gce.DraGPUDriver && *s.Spec.NodeName == nodeName {
			claim.Status = resourceapi.ResourceClaimStatus{
				Allocation: &resourceapi.AllocationResult{
					Devices: resourceapi.DeviceAllocationResult{
						Results: allocationsForSlice(&s, gpuCnt),
					},
				},
			}
			return
		}
	}
	t.Fatalf("No resource slice found for the selected node")
}

func allocationsForSlice(s *resourceapi.ResourceSlice, cnt int) []resourceapi.DeviceRequestAllocationResult {
	allocLen := min(cnt, len(s.Spec.Devices))
	allocs := make([]resourceapi.DeviceRequestAllocationResult, 0, allocLen)
	for i := 0; i < allocLen; i++ {
		allocs = append(allocs, resourceapi.DeviceRequestAllocationResult{
			Request: "gpu-request",
			Driver:  s.Spec.Driver,
			Pool:    s.Spec.Pool.Name,
			Device:  s.Spec.Devices[i].Name,
		})
	}
	return allocs
}
