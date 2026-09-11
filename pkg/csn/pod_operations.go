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
	"strconv"
	"strings"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/metadata"
	capacitybufferpodlister "sigs.k8s.io/cluster-autoscaler/pkg/processors/capacitybuffer"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/pod"
)

const (
	// Annotation key and value to identify CSN pods, used internally only since those pods are fake.
	CSNPodAnnotationKey   = "buffer.gke.io/standby-capacity-pod"
	CSNPodAnnotationValue = "true"
)

func IsCSNPod(pod *apiv1.Pod) bool {
	return pod != nil && pod.Annotations != nil && pod.Annotations[CSNPodAnnotationKey] == CSNPodAnnotationValue
}

// PodOption adjusts how MakePodCSN builds a standby buffer fake pod.
type PodOption func(*podOptions)

// podOptions holds the adjustable parts of MakePodCSN. The zero value is the default behaviour.
type podOptions struct {
	memoryLimit MemoryLimit
}

// WithMemoryLimit overrides the node memory limit encoded in the pod's node affinity. Without it
// MakePodCSN uses the default limit; production callers should pass NewMemoryLimit's result so
// that the configured limit is honoured.
func WithMemoryLimit(limit MemoryLimit) PodOption {
	return func(options *podOptions) {
		options.memoryLimit = limit
	}
}

// MakePodCSN turns pod into a standby buffer (CSN) fake pod for the given buffer.
func MakePodCSN(pod *apiv1.Pod, bufferId string, opts ...PodOption) {
	options := podOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	applyWorkloadSeparation(pod, metadata.SoftWorkloadSeparationKey, metadata.SoftWorkloadSeparationValue, apiv1.TaintEffectPreferNoSchedule)

	// TODO(b/484466017): Find a better fix.
	// We replace the "/" because "_" is illegal character in taints/label.
	bufferId = strings.ReplaceAll(bufferId, "/", "_")
	applyWorkloadSeparation(pod, metadata.BufferAssignmentKey, bufferId, apiv1.TaintEffectNoSchedule)

	pod.Spec.Tolerations = append(pod.Spec.Tolerations, apiv1.Toleration{
		Key:    metadata.SuspendedTaintKey,
		Value:  metadata.SuspendedTaintValue,
		Effect: apiv1.TaintEffectNoSchedule,
	})

	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[capacitybufferpodlister.CapacityBufferFakePodAnnotationKey] = capacitybufferpodlister.CapacityBufferFakePodAnnotationValue
	pod.Annotations[CSNPodAnnotationKey] = CSNPodAnnotationValue
	// Annotation is the main identifier for buffer assignment. Buffer assignment workload separation doesn't exist for unschedulable pods.
	pod.Annotations[metadata.BufferAssignmentKey] = bufferId

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
	*nodeAffinityTerms = append(*nodeAffinityTerms, memoryLimitNodeSelectorTerms(options.memoryLimit)...)
}

// memoryLimitNodeSelectorTerms returns the node selector terms restricting a standby buffer pod to
// nodes that GCE VM Suspend/Resume supports. The terms are OR-ed by the scheduler: a node is
// acceptable if its memory scaling level is below the limit, or if it does not carry the label at
// all.
func memoryLimitNodeSelectorTerms(limit MemoryLimit) []apiv1.NodeSelectorTerm {
	return []apiv1.NodeSelectorTerm{
		{
			MatchExpressions: []apiv1.NodeSelectorRequirement{
				{
					Key:      labels.MemoryScalingLevelLabel,
					Operator: apiv1.NodeSelectorOpLt,
					Values:   []string{strconv.FormatInt(limit.GB(), 10)},
				},
			},
		},
		{
			MatchExpressions: []apiv1.NodeSelectorRequirement{
				{
					Key:      labels.MemoryScalingLevelLabel,
					Operator: apiv1.NodeSelectorOpDoesNotExist,
				},
			},
		},
	}
}

func RemoveBufferAssignmentWorkloadSeparation(pod *apiv1.Pod) {
	if pod == nil {
		return
	}
	delete(pod.Spec.NodeSelector, metadata.BufferAssignmentKey)
	pod.Spec.Tolerations = removeTolerationsByKey(pod.Spec.Tolerations, metadata.BufferAssignmentKey)
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
	return pod.Annotations[metadata.BufferAssignmentKey]
}

// IsPodBlockingSuspension returns true if a pod should block suspension.
func IsPodBlockingSuspension(p *apiv1.Pod) bool {
	if p.Status.Phase == apiv1.PodSucceeded || p.Status.Phase == apiv1.PodFailed {
		return false
	}
	return !pod.IsDaemonSetPod(p) && !pod.IsMirrorPod(p) && !pod.IsStaticPod(p)
}
