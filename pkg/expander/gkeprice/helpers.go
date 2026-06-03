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

package gkeprice

import (
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	podutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/pod"
)

const (
	// Minimum requests for a pod.
	// Pods without requests will be consider as having minimum requests.
	podMinMilliCPURequest         = 5
	podMinMemoryRequest           = 5 * units.MiB
	podMinEphemeralStorageRequest = 0 * units.MiB
)

// Resource keeps sum of cpu and memory resources
type Resource struct {
	MilliCPU         int64
	Memory           int64
	EphemeralStorage int64
}

// Add adds values from other Resource
func (r *Resource) Add(o Resource) {
	if r == nil {
		return
	}
	r.MilliCPU += o.MilliCPU
	r.Memory += o.Memory
	r.EphemeralStorage += o.EphemeralStorage
}

// AddResourceList adds ResourceList into Resource.
func (r *Resource) AddResourceList(rl apiv1.ResourceList) {
	if r == nil {
		return
	}
	if resourceValue, found := rl[apiv1.ResourceCPU]; found {
		r.MilliCPU += resourceValue.MilliValue()
	}
	if resourceValue, found := rl[apiv1.ResourceMemory]; found {
		r.Memory += resourceValue.Value()
	}
	if resourceValue, found := rl[apiv1.ResourceEphemeralStorage]; found {
		r.EphemeralStorage += resourceValue.Value()
	}
}

// AddPodsRequests adds all pods containers requests into Resource.
func (r *Resource) AddPodsRequests(pods ...*apiv1.Pod) {
	if r == nil {
		return
	}
	for _, pod := range pods {
		podResources := Resource{}
		podResources.AddResourceList(podutils.PodRequests(pod))

		// Normalize bumping up too low requests
		podResources.MilliCPU = max(podResources.MilliCPU, podMinMilliCPURequest)
		podResources.Memory = max(podResources.Memory, podMinMemoryRequest)
		podResources.EphemeralStorage = max(podResources.EphemeralStorage, podMinEphemeralStorageRequest)
		r.Add(podResources)
	}
}
