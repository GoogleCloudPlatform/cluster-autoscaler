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

package csn

import (
	"strings"

	apiv1 "k8s.io/api/core/v1"
	capacitybufferpodlister "k8s.io/autoscaler/cluster-autoscaler/processors/capacitybuffer"
	"k8s.io/autoscaler/cluster-autoscaler/utils/pod"
)

const (
	// Annotation key and value to identify CSN pods, used internally only since those pods are fake.
	CSNPodAnnotationKey   = "buffer.gke.io/standby-capacity-pod"
	CSNPodAnnotationValue = "true"

	memoryScalingLevelLabel     = "cloud.google.com/gke-memory-gb-scaling-level"
	minUnsupportedMemoryGBValue = "209"
)

func IsCSNPod(pod *apiv1.Pod) bool {
	return pod != nil && pod.Annotations != nil && pod.Annotations[CSNPodAnnotationKey] == CSNPodAnnotationValue
}

func MakePodCSN(pod *apiv1.Pod, bufferId string) {
	applyWorkloadSeparation(pod, SoftWorkloadSeparationKey, SoftWorkloadSeparationValue, apiv1.TaintEffectPreferNoSchedule)

	// TODO(b/484466017): Find a better fix.
	// We replace the "/" because "_" is illegal character in taints/label.
	bufferId = strings.ReplaceAll(bufferId, "/", "_")
	applyWorkloadSeparation(pod, BufferAssignmentKey, bufferId, apiv1.TaintEffectNoSchedule)

	pod.Spec.Tolerations = append(pod.Spec.Tolerations, apiv1.Toleration{
		Key:    SuspendedTaintKey,
		Value:  SuspendedTaintValue,
		Effect: apiv1.TaintEffectNoSchedule,
	})

	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[capacitybufferpodlister.CapacityBufferFakePodAnnotationKey] = capacitybufferpodlister.CapacityBufferFakePodAnnotationValue
	pod.Annotations[CSNPodAnnotationKey] = CSNPodAnnotationValue
	// Annotation is the main identifier for buffer assignment. Buffer assignment workload separation doesn't exist for unschedulable pods.
	pod.Annotations[BufferAssignmentKey] = bufferId

	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &apiv1.Affinity{}
	}
	if pod.Spec.Affinity.NodeAffinity == nil {
		pod.Spec.Affinity.NodeAffinity = &apiv1.NodeAffinity{}
	}
	if pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &apiv1.NodeSelector{}
	}
	nodeAffinityTerms := &pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	*nodeAffinityTerms = append(*nodeAffinityTerms,
		apiv1.NodeSelectorTerm{
			MatchExpressions: []apiv1.NodeSelectorRequirement{
				{
					Key:      memoryScalingLevelLabel,
					Operator: apiv1.NodeSelectorOpLt,
					Values:   []string{minUnsupportedMemoryGBValue},
				},
			},
		},
		apiv1.NodeSelectorTerm{
			MatchExpressions: []apiv1.NodeSelectorRequirement{
				{
					Key:      memoryScalingLevelLabel,
					Operator: apiv1.NodeSelectorOpDoesNotExist,
				},
			},
		})

}

func RemoveBufferAssignmentWorkloadSeparation(pod *apiv1.Pod) {
	if pod == nil {
		return
	}
	delete(pod.Spec.NodeSelector, BufferAssignmentKey)
	pod.Spec.Tolerations = removeTolerationsByKey(pod.Spec.Tolerations, BufferAssignmentKey)
}

func removeTolerationsByKey(tolerations []apiv1.Toleration, key string) []apiv1.Toleration {
	var result []apiv1.Toleration
	for _, t := range tolerations {
		if t.Key != key {
			result = append(result, t)
		}
	}
	return result
}

func applyWorkloadSeparation(pod *apiv1.Pod, k, v string, effect apiv1.TaintEffect) {
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = map[string]string{}
	}
	pod.Spec.NodeSelector[k] = v

	pod.Spec.Tolerations = append(pod.Spec.Tolerations, apiv1.Toleration{
		Key:    k,
		Value:  v,
		Effect: effect,
	})
}

func GetBufferIdFromPod(pod *apiv1.Pod) string {
	if pod == nil || pod.Annotations == nil {
		return ""
	}
	return pod.Annotations[BufferAssignmentKey]
}

// IsPodBlockingSuspension returns true if a pod should block suspension.
func IsPodBlockingSuspension(p *apiv1.Pod) bool {
	if p.Status.Phase == apiv1.PodSucceeded || p.Status.Phase == apiv1.PodFailed {
		return false
	}
	return !pod.IsDaemonSetPod(p) && !pod.IsMirrorPod(p) && !pod.IsStaticPod(p)
}
