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
	nodeSelector := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	nodeSelector.NodeSelectorTerms = withSuspensionConstraints(nodeSelector.NodeSelectorTerms, options.memoryLimit)
}

// withSuspensionConstraints ANDs the Suspend/Resume requirements into every existing node
// selector term.
//
// Kubernetes ORs NodeSelectorTerms and only ANDs the requirements within a single term, so the
// suspension constraints must not be appended as new terms - that would widen the pod's affinity instead of
// narrowing it, letting a node satisfy the pod by matching the injected requirements alone. The
// result is therefore the cross product:
//
//	(T1 OR T2) AND (A1 OR A2) == (T1 AND A1) OR (T1 AND A2) OR (T2 AND A1) OR (T2 AND A2)
func withSuspensionConstraints(terms []apiv1.NodeSelectorTerm, memoryLimit MemoryLimit) []apiv1.NodeSelectorTerm {
	requirements := []apiv1.NodeSelectorRequirement{
		{
			Key:      labels.MemoryScalingLevelLabel,
			Operator: apiv1.NodeSelectorOpLt,
			Values:   []string{strconv.FormatInt(memoryLimit.GB(), 10)},
		},
		{
			Key:      labels.MemoryScalingLevelLabel,
			Operator: apiv1.NodeSelectorOpDoesNotExist,
		},
	}

	if len(terms) == 0 {
		// Nothing to narrow, the requirements stand on their own.
		terms = []apiv1.NodeSelectorTerm{{}}
	}
	result := make([]apiv1.NodeSelectorTerm, 0, len(terms)*len(requirements))
	for _, term := range terms {
		for _, req := range requirements {
			// The term needs to be copied for two reasons:
			// 1. The terms generated for suspension constraints need
			// to not share the same MatchExpressions slice. Otherwise, the
			// generated terms would share all added constraints. That is:
			// (T1) AND (A1 OR A2) == (T1 AND A1 AND A2) OR (T1 AND A1 AND A2)
			// instead of the expected:
			// (T1) AND (A1 OR A2) == (T1 AND A1) OR (T1 AND A2)
			// 2. All other fields need to be copied as well in case some other
			// place in CA starts mutating node affinities and is unpleasantly
			// surprised that modifying a (sub)field of one term also modifies
			// the same for the neighbouring term.
			copied := *term.DeepCopy()
			copied.MatchExpressions = append(copied.MatchExpressions, req)
			result = append(result, copied)
		}
	}
	return result
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
