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

package test

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	gceprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	priceexpander "k8s.io/autoscaler/cluster-autoscaler/expander/price"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	testutils "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	units "k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/impostor"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/stretchr/testify/assert"
)

// PodBuilder helps building pods for tests.
type PodsBuilder interface {
	WithName(name string) PodsBuilder
	WithCPUMilli(cpuMilli int64) PodsBuilder
	WithMemMiB(memMiB int64) PodsBuilder
	WithGPU(gpu int64) PodsBuilder
	WithGpuType(gpuType string) PodsBuilder
	DedicateForWorkload(workload string) PodsBuilder
	Get() []*apiv1.Pod
}

// Pods returns new PodBuilder to build count pods.
func Pods(count int) PodsBuilder {
	return &podsBuilderImpl{
		count: count,
		// If ownerReferences is nil, pods won't be grouped correctly into equivalence groups, so we'll set one by default
		ownerReferences: testutils.GenerateOwnerReferences("some-deployment", "Deployment", "apps/v1", ""),
	}
}

type podsBuilderImpl struct {
	count           int
	name            string
	cpuMilli        int64
	memMiB          int64
	gpu             int64
	gpuType         string
	workload        string
	dedicated       bool
	ownerReferences []metav1.OwnerReference
}

func (pb *podsBuilderImpl) WithName(name string) PodsBuilder {
	r := *pb
	r.name = name
	return &r
}

func (pb *podsBuilderImpl) WithCPUMilli(cpuMilli int64) PodsBuilder {
	r := *pb
	r.cpuMilli = cpuMilli
	return &r
}

func (pb *podsBuilderImpl) WithMemMiB(memMiB int64) PodsBuilder {
	if memMiB > 1000000 {
		panic(memMiB)
	}
	r := *pb
	r.memMiB = memMiB
	return &r
}

func (pb *podsBuilderImpl) WithGPU(gpu int64) PodsBuilder {
	r := *pb
	r.gpu = gpu
	return &r
}

func (pb *podsBuilderImpl) WithGpuType(gpuType string) PodsBuilder {
	r := *pb
	r.gpuType = gpuType
	return &r
}

func (pb *podsBuilderImpl) DedicateForWorkload(workload string) PodsBuilder {
	r := *pb
	r.workload = workload
	r.dedicated = true
	return &r
}

func (pb *podsBuilderImpl) WithOwnerReferences(ownerReferences []metav1.OwnerReference) {
	pb.ownerReferences = ownerReferences
}

func (pb *podsBuilderImpl) Get() []*apiv1.Pod {
	var pods []*apiv1.Pod
	for i := 0; i < pb.count; i++ {
		podName := fmt.Sprintf("%s-%s", pb.name, gke.GenerateRandomId(8))
		pod := testutils.BuildTestPod(podName, pb.cpuMilli, pb.memMiB*units.MiB)
		if pb.gpu > 0 {
			testutils.RequestGpuForPod(pod, pb.gpu)
			AddGpuTolerationToPod(pod)
			if pb.gpuType != "" {
				if pod.Spec.NodeSelector == nil {
					pod.Spec.NodeSelector = make(map[string]string)
				}
				pod.Spec.NodeSelector[gceprovider.GPULabel] = pb.gpuType
			}
		}
		if pb.dedicated {
			DedicatePodForWorkload(pod, pb.workload)
		}
		for _, ref := range pb.ownerReferences {
			pod.OwnerReferences = append(pod.OwnerReferences, ref)
		}
		pods = append(pods, pod)
	}
	return pods
}

// AddGpuTolerationToPod adds GPU taint toleration to given pod.
func AddGpuTolerationToPod(pod *apiv1.Pod) {
	pod.Spec.Tolerations = append(pod.Spec.Tolerations, apiv1.Toleration{
		Key:    gpu.ResourceNvidiaGPU,
		Value:  "present",
		Effect: "NoSchedule",
	})
}

// DedicatePodForWorkload marks pod as dedicated for given workload.
func DedicatePodForWorkload(pod *apiv1.Pod, workload string) {
	pod.Spec.Tolerations = append(
		pod.Spec.Tolerations,
		apiv1.Toleration{
			Key:    "workload",
			Value:  workload,
			Effect: "NoSchedule",
		})
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = make(map[string]string)
	}

	pod.Spec.NodeSelector["workload"] = workload
}

// DedicateNodeForWorkload marks node as dedicated for given workload.
func DedicateNodeForWorkload(node *apiv1.Node, workload string) {
	node.Spec.Taints = append(
		node.Spec.Taints,
		apiv1.Taint{
			Key:    "workload",
			Value:  workload,
			Effect: "NoSchedule",
		})
	node.Labels["workload"] = workload
}

func getBestOptionGroupName(option *expander.Option) string {
	if option == nil {
		return "-"
	}
	groupName := option.NodeGroup.Id()
	r := regexp.MustCompile("^((nap-)?([a-z][0-9]-[a-z]+(-[0-9]+)?)(-gpu[0-9])?)(-[0-9a-z]{8})?$")
	if !r.MatchString(groupName) {
		return groupName
	}
	return r.FindStringSubmatch(groupName)[1]
}

func parseBestOption(option *expander.Option) (groupName string, nodes int, pods int) {
	if option == nil {
		return "-", 0, 0
	}
	groupName = option.NodeGroup.Id()
	if strings.HasPrefix(groupName, "nap-") {
		groupName = groupName[:len(groupName)-9]
	}
	return groupName, option.NodeCount, len(option.Pods)
}

func getPreferredMachineType(t *testing.T, cluster *impostor.Cluster) string {
	node, err := priceexpander.NewSimplePreferredNodeProvider(cluster.NodeLister()).Node()
	assert.NoError(t, err)
	return fmt.Sprintf("n1-standard-%d", node.Status.Capacity.Cpu().Value())
}
