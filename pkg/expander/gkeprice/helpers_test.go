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
	"fmt"
	"testing"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"

	"github.com/stretchr/testify/assert"
)

func TestResourceAddResource(t *testing.T) {
	testCases := []struct {
		terms    []Resource
		expected Resource
	}{
		{nil, Resource{0, 0, 0}},
		{[]Resource{{0, 0, 0}}, Resource{0, 0, 0}},
		{[]Resource{{0, 10, 0}}, Resource{0, 10, 0}},
		{[]Resource{{10, 0, 0}}, Resource{10, 0, 0}},
		{[]Resource{{0, 0, 10}}, Resource{0, 0, 10}},
		{[]Resource{{10000, 20 * units.GiB, 0}}, Resource{10000, 20 * units.GiB, 0}},
		{[]Resource{{10, 20, 30}, {20, 30, 40}}, Resource{30, 50, 70}},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("terms %+v", tc.terms), func(t *testing.T) {
			result := Resource{}
			for _, term := range tc.terms {
				result.Add(term)
			}
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestResourceAddResourceList(t *testing.T) {
	testCases := []struct {
		terms    []Resource
		expected Resource
	}{
		{nil, Resource{0, 0, 0}},
		{[]Resource{{0, 0, 0}}, Resource{0, 0, 0}},
		{[]Resource{{0, 10, 0}}, Resource{0, 10, 0}},
		{[]Resource{{10, 0, 0}}, Resource{10, 0, 0}},
		{[]Resource{{0, 0, 10}}, Resource{0, 0, 10}},
		{[]Resource{{10000, 20 * units.GiB, 0}}, Resource{10000, 20 * units.GiB, 0}},
		{[]Resource{{10, 20, 30}, {20, 30, 40}}, Resource{30, 50, 70}},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("terms %+v", tc.terms), func(t *testing.T) {
			result := Resource{}
			for _, term := range tc.terms {
				resourceList := make(apiv1.ResourceList)
				if term.MilliCPU > 0 {
					resourceList[apiv1.ResourceCPU] = *resource.NewMilliQuantity(term.MilliCPU, resource.DecimalSI)
				}
				if term.Memory > 0 {
					resourceList[apiv1.ResourceMemory] = *resource.NewQuantity(term.Memory, resource.BinarySI)
				}
				if term.EphemeralStorage > 0 {
					resourceList[apiv1.ResourceEphemeralStorage] = *resource.NewQuantity(term.EphemeralStorage, resource.BinarySI)
				}
				result.AddResourceList(resourceList)
			}
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TODO(b/234546178): Update test after sync (BuildPodWithEphemeralStorageResource).
func TestResourceAddPods(t *testing.T) {
	testCases := []struct {
		terms    []Resource
		expected Resource
	}{
		{nil, Resource{0, 0, 0}},
		{[]Resource{{0, 0, 0}}, Resource{5, 5 * units.MiB, 0}},
		{[]Resource{{0, 10 * units.MiB, 0}}, Resource{5, 10 * units.MiB, 0}},
		{[]Resource{{0, 0, 0}, {0, 10 * units.MiB, 0}}, Resource{10, 15 * units.MiB, 0}},
		{[]Resource{{10, 0, 0}}, Resource{10, 5 * units.MiB, 0}},
		{[]Resource{{10000, 20 * units.GiB, 0}}, Resource{10000, 20 * units.GiB, 0}},
		{[]Resource{{10, 20 * units.MiB, 0}, {20, 30 * units.MiB, 0}}, Resource{30, 50 * units.MiB, 0}},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("terms %+v", tc.terms), func(t *testing.T) {
			var pods []*apiv1.Pod
			for idx, term := range tc.terms {
				pod := BuildTestPod(fmt.Sprintf("p%d", idx), term.MilliCPU, term.Memory)
				pods = append(pods, pod)
			}
			result := Resource{}
			result.AddPodsRequests(pods...)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TODO(b/234546178): Update test after sync (BuildPodWithEphemeralStorageResource).
func TestResourceAddContainers(t *testing.T) {
	testCases := []struct {
		terms    []Resource
		expected Resource
	}{
		{nil, Resource{0, 0, 0}},
		{[]Resource{{0, 0, 0}}, Resource{5, 5 * units.MiB, 0}},
		{[]Resource{{0, 10 * units.MiB, 0}}, Resource{5, 10 * units.MiB, 0}},
		{[]Resource{{0, 0, 0}, {0, 10 * units.MiB, 0}}, Resource{5, 10 * units.MiB, 0}},
		{[]Resource{{10, 0, 0}}, Resource{10, 5 * units.MiB, 0}},
		{[]Resource{{10000, 20 * units.GiB, 0}}, Resource{10000, 20 * units.GiB, 0}},
		{[]Resource{{10, 20 * units.MiB, 0}, {20, 30 * units.MiB, 0}}, Resource{30, 50 * units.MiB, 0}},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("terms %+v", tc.terms), func(t *testing.T) {
			var pod *apiv1.Pod
			for idx, term := range tc.terms {
				if pod == nil {
					pod = BuildTestPod(fmt.Sprintf("p%d", idx), term.MilliCPU, term.Memory)
				} else {
					tempPod := BuildTestPod(fmt.Sprintf("p%d", idx), term.MilliCPU, term.Memory)
					pod.Spec.Containers = append(pod.Spec.Containers, tempPod.Spec.Containers[0])
				}
			}
			result := Resource{}
			if pod != nil {
				result.AddPodsRequests(pod)
			}
			assert.Equal(t, tc.expected, result)
		})
	}
}
