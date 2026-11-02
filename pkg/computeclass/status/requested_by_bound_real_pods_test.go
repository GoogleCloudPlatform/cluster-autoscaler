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

package status

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/gpu"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestExcludedPodsRequests(t *testing.T) {
	resourceNames := []apiv1.ResourceName{apiv1.ResourceCPU, apiv1.ResourceMemory, gpu.ResourceNvidiaGPU}

	realPod := test.BuildScheduledTestPod("real-pod", 2000, 4*GB, "test-node")
	test.RequestGpuForPod(realPod, 2)

	unboundPod := test.BuildTestPod("unbound-pod", 1000, 1*GB)
	test.RequestGpuForPod(unboundPod, 1)

	minCapacityPod := test.BuildScheduledTestPod("min-capacity-pod", 500, 2*GB, "test-node")
	minCapacityPod.Annotations = map[string]string{crd.MinCapacityFakePodAnnotation: "true"}
	test.RequestGpuForPod(minCapacityPod, 1)

	tests := []struct {
		name string
		pods []*apiv1.Pod
		want map[apiv1.ResourceName]int64
	}{
		{
			name: "no_excluded_pods_returns_nil",
			pods: []*apiv1.Pod{realPod},
			want: nil,
		},
		{
			name: "sums_every_resource_of_excluded_pods",
			pods: []*apiv1.Pod{realPod, unboundPod, minCapacityPod},
			want: map[apiv1.ResourceName]int64{
				apiv1.ResourceCPU:     1500, // millicores
				apiv1.ResourceMemory:  3 * GB,
				gpu.ResourceNvidiaGPU: 2,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nodeInfo := framework.NewTestNodeInfo(test.BuildTestNode("test-node", 32000, 128*GB), tc.pods...)
			got := excludedPodsRequests(nodeInfo, resourceNames)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("excludedPodsRequests() returned unexpected diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResourceValue(t *testing.T) {
	nonScalarResource := apiv1.ResourceName("example-resource")

	pod := test.BuildScheduledTestPod("real-pod", 2000, 4*GB, "test-node")
	test.RequestGpuForPod(pod, 2)
	pod.Spec.Containers[0].Resources.Requests[tpu.ResourceGoogleTPU] = *resource.NewQuantity(4, resource.DecimalSI)
	pod.Spec.Containers[0].Resources.Requests[nonScalarResource] = *resource.NewQuantity(3, resource.DecimalSI)
	nodeInfo := framework.NewTestNodeInfo(test.BuildTestNode("test-node", 32000, 128*GB), pod)

	tests := []struct {
		name         string
		resourceName apiv1.ResourceName
		want         int64
	}{
		{name: "cpu_in_millicores", resourceName: apiv1.ResourceCPU, want: 2000},
		{name: "memory_in_bytes", resourceName: apiv1.ResourceMemory, want: 4 * GB},
		{name: "gpu_is_scalar", resourceName: gpu.ResourceNvidiaGPU, want: 2},
		{name: "tpu_is_scalar", resourceName: tpu.ResourceGoogleTPU, want: 4},
		{name: "non_scalar_resource_reads_zero", resourceName: nonScalarResource, want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := resourceValue(nodeInfo.Requested, tc.resourceName); got != tc.want {
				t.Errorf("resourceValue(nodeInfo.Requested, %q) = %d, want %d", tc.resourceName, got, tc.want)
			}
		})
	}
}
