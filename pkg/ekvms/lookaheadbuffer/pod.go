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

package lookaheadbuffer

import (
	"fmt"
	"hash/fnv"
	"strconv"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	quota "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/fake"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	podutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/pod"
	"k8s.io/utils/ptr"
)

const (
	lookaheadPodNamePrefix = "lookahead-virtual-pod"
	lookaheadVUIDPrefix    = "lookahead-vuid"
	lookaheadBufferName    = "lookahead-buffer"
	lookaheadBufferKind    = "LookaheadBuffer"
	lookaheadPodLabel      = "cloud.google.com/ca-lookahead-pod"
)

// GenerateLookaheadPods generates virtual lookahead pods to reserve additional space in the cluster.
// The space they requested will be downsized to save costs.
//
// Doc: go/gke-ek-design-lookahead-buffer-v1-implementation
//
// The generated pods have deterministic names and UIDs based on their index and workload ID hash
// to ensure consistent behavior in the hinting simulator. They are placed in the "kube-system"
// namespace so users will see it in scale-up events as part of system pods, although it is
// worth noting that such pods block scale-down in CA (non-daemonSet non-mirrored kube-system pod without PDB).
// All pods are configured to request the specified CPU and memory resources and target EKVMs by using the "ek"
// machine family label. The lookaheadPodLabel is added for easy and future-proof identification.

func GenerateLookaheadPods(number int, cpu, memory resource.Quantity, workloadID string) []*apiv1.Pod {
	hash := hashWorkloadID(workloadID)
	basename := fmt.Sprintf("%s-%s-", lookaheadPodNamePrefix, hash)

	// Controller logic: CA uses ControllerRef to group pods.
	// We create a virtual "LookaheadBuffer" owner.
	virtualUID := types.UID(fmt.Sprintf("%s-%s", lookaheadVUIDPrefix, hash))

	pod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      basename,
			UID:       types.UID(basename),
			Namespace: metav1.NamespaceSystem,
			Labels: map[string]string{
				lookaheadPodLabel: "true",
			},
			Annotations: map[string]string{
				fake.FakePodAnnotationKey: fake.FakePodAnnotationValue,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       lookaheadBufferKind,
					Name:       fmt.Sprintf("%s-%s", lookaheadBufferName, hash),
					UID:        virtualUID,
					Controller: ptr.To(true),
				},
			},
		},
		Spec: apiv1.PodSpec{
			Containers: []apiv1.Container{
				{
					Resources: apiv1.ResourceRequirements{
						Requests: apiv1.ResourceList{
							apiv1.ResourceCPU:    cpu,
							apiv1.ResourceMemory: memory,
						},
					},
				},
			},
			NodeSelector: map[string]string{
				labels.MachineFamilyLabel: "ek",
			},
		},
	}

	// Add tolerations and node selectors if the pod is expected to run on workload separated nodes.
	tolerations := podrequirements.WorkloadIDToTolerations(workloadID)
	for _, t := range tolerations {
		pod.Spec.Tolerations = append(pod.Spec.Tolerations, t)
		pod.Spec.NodeSelector[t.Key] = t.Value
	}

	laPods := make([]*apiv1.Pod, number)
	for i := 0; i < number; i++ {
		laPods[i] = pod.DeepCopy()
		laPods[i].Name += strconv.Itoa(i)
		laPods[i].UID = types.UID(laPods[i].Name)
	}

	return laPods
}

// IsLookaheadPod checks if a given pod is a lookahead pod.
// Lookahead pods are identified by the presence and value of the lookaheadPodLabel.
func IsLookaheadPod(pod *apiv1.Pod) bool {
	return pod != nil && pod.Labels[lookaheadPodLabel] == "true"
}

func AllLookaheadPodsRequests(nodeInfo *framework.NodeInfo) apiv1.ResourceList {
	laPodsRequests := apiv1.ResourceList{}
	for _, pod := range nodeInfo.GetPods() {
		if IsLookaheadPod(pod.GetPod()) {
			laPodsRequests = quota.Add(laPodsRequests, CpuMemRequests(pod.GetPod()))
		}
	}

	return laPodsRequests
}

func CpuMemRequests(pod *apiv1.Pod) apiv1.ResourceList {
	podRequests := podutils.PodRequests(pod)

	return apiv1.ResourceList{
		apiv1.ResourceCPU:    podRequests[apiv1.ResourceCPU],
		apiv1.ResourceMemory: podRequests[apiv1.ResourceMemory],
	}
}

// LookaheadWorkloadSeparationInfo returns a list of tolerations that have a matching node selector, formatted as strings.
// This function can only be used for Lookahead pods and will not work for any other generic pod.
func LookaheadWorkloadSeparationInfo(pod *apiv1.Pod) []string {
	var workloadInfo []string = make([]string, 0)
	for _, toleration := range pod.Spec.Tolerations {
		nodeSelectorValue, hasSelector := pod.Spec.NodeSelector[toleration.Key]
		if hasSelector && (toleration.Operator == apiv1.TolerationOpExists || toleration.Value == nodeSelectorValue) {
			workloadInfo = append(workloadInfo, fmt.Sprintf("%s=%s", toleration.Key, toleration.Value))
		}
	}
	return workloadInfo
}

func hashWorkloadID(s string) string {
	if s == "" {
		return "default"
	}
	return fnvHash32(s)
}

func fnvHash32(s string) string {
	h := fnv.New32()
	_, _ = h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum32())
}
