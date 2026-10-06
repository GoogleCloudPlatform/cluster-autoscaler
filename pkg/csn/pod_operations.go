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
	"slices"
	"strconv"
	"strings"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
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

var noLocalSSDRequirement = apiv1.NodeSelectorRequirement{
	Key:      labels.EphemeralLocalSsdLabel,
	Operator: apiv1.NodeSelectorOpNotIn,
	Values:   []string{labels.EphemeralLocalSsdEnabledValue},
}

// withSuspensionConstraints ANDs the Suspend/Resume requirements (the disjunctive memory limit
// A1 OR A2 and the conjunctive Local SSD exclusion B) into every existing node selector term.
//
// Kubernetes ORs NodeSelectorTerms and only ANDs the requirements within a single term, so the
// suspension constraints must not be appended as new terms - that would widen the pod's affinity
// instead of narrowing it, letting a node satisfy the pod by matching the injected requirements
// alone. The result is therefore the cross product with the memory requirements, with the Local SSD
// requirement appended to every term:
//
//	(T1 OR T2) AND (A1 OR A2) AND B == (T1 AND A1 AND B) OR (T1 AND A2 AND B) OR (T2 AND A1 AND B) OR (T2 AND A2 AND B)
func withSuspensionConstraints(terms []apiv1.NodeSelectorTerm, memoryLimit MemoryLimit) []apiv1.NodeSelectorTerm {
	memoryRequirements := []apiv1.NodeSelectorRequirement{
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
	result := make([]apiv1.NodeSelectorTerm, 0, len(terms)*len(memoryRequirements))
	for _, term := range terms {
		for _, req := range memoryRequirements {
			// The term needs to be copied for two reasons:
			// 1. The terms generated for suspension constraints need
			// to not share the same MatchExpressions slice. Otherwise, the
			// generated terms would share all added constraints. That is:
			// (T1) AND (A1 OR A2) AND B == (T1 AND A1 AND B AND A2 AND B) OR (T1 AND A1 AND B AND A2 AND B)
			// instead of the expected:
			// (T1) AND (A1 OR A2) AND B == (T1 AND A1 AND B) OR (T1 AND A2 AND B)
			// 2. All other fields need to be copied as well in case some other
			// place in CA starts mutating node affinities and is unpleasantly
			// surprised that modifying a (sub)field of one term also modifies
			// the same for the neighbouring term.
			copied := *term.DeepCopy()
			copied.MatchExpressions = append(copied.MatchExpressions, req, noLocalSSDRequirement)
			result = append(result, copied)
		}
	}
	return result
}

// requiredNodeAffinityWithoutSuspensionConstraints returns the RequiredNodeAffinity for pod with
// the MatchExpressions injected by withSuspensionConstraints removed. Callers use it to check
// whether a node rejected by the pod's full affinity would have matched the user's own constraints
// if not for the CSN suspension constraints.
//
// Because withSuspensionConstraints duplicates each original user term across the two memory
// requirement branches consecutively, stripping the injected expressions produces adjacent
// duplicate terms (e.g. [T1, T2] -> [T1 + memLt + noLSSD, T1 + memDNE + noLSSD,
// T2 + memLt + noLSSD, T2 + memDNE + noLSSD] -> [T1, T1, T2, T2]). Collapsing adjacent duplicates
// in O(T) here is faster than leaving duplicates for nodeaffinity.NewRequiredNodeAffinity to
// compile and Match to evaluate, while avoiding the O(T^2) cost of full slice deduplication.
func requiredNodeAffinityWithoutSuspensionConstraints(pod *apiv1.Pod) nodeaffinity.RequiredNodeAffinity {
	if pod.Spec.Affinity == nil ||
		pod.Spec.Affinity.NodeAffinity == nil ||
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return nodeaffinity.NewRequiredNodeAffinity(pod.Spec.NodeSelector, nil)
	}
	strippedTerms := stripSuspensionConstraints(pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms)
	var affinity *apiv1.Affinity
	if len(strippedTerms) > 0 {
		affinity = &apiv1.Affinity{
			NodeAffinity: &apiv1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
					NodeSelectorTerms: strippedTerms,
				},
			},
		}
	}
	return nodeaffinity.NewRequiredNodeAffinity(pod.Spec.NodeSelector, affinity)
}

func stripSuspensionConstraints(terms []apiv1.NodeSelectorTerm) []apiv1.NodeSelectorTerm {
	var strippedTerms []apiv1.NodeSelectorTerm
	for _, term := range terms {
		var expressions []apiv1.NodeSelectorRequirement
		for _, expr := range term.MatchExpressions {
			if !isSuspensionConstraint(expr) {
				expressions = append(expressions, expr)
			}
		}
		if len(expressions) == 0 && len(term.MatchFields) == 0 {
			continue
		}
		stripped := apiv1.NodeSelectorTerm{
			MatchExpressions: expressions,
			MatchFields:      term.MatchFields,
		}
		if len(strippedTerms) == 0 || !nodeSelectorTermEqual(strippedTerms[len(strippedTerms)-1], stripped) {
			strippedTerms = append(strippedTerms, stripped)
		}
	}
	return strippedTerms
}

func nodeSelectorTermEqual(a, b apiv1.NodeSelectorTerm) bool {
	return slices.EqualFunc(a.MatchExpressions, b.MatchExpressions, nodeSelectorRequirementEqual) &&
		slices.EqualFunc(a.MatchFields, b.MatchFields, nodeSelectorRequirementEqual)
}

func nodeSelectorRequirementEqual(a, b apiv1.NodeSelectorRequirement) bool {
	return a.Key == b.Key && a.Operator == b.Operator && slices.Equal(a.Values, b.Values)
}

// isSuspensionConstraint reports whether req matches the shape of a constraint injected by
// withSuspensionConstraints.
//
// TODO(b/570359780): Avoid stripping user-specified node affinity expressions that share the same
// key and operator as CSN suspension constraints.
func isSuspensionConstraint(req apiv1.NodeSelectorRequirement) bool {
	switch req.Key {
	case labels.MemoryScalingLevelLabel:
		return req.Operator == apiv1.NodeSelectorOpLt || req.Operator == apiv1.NodeSelectorOpDoesNotExist
	case labels.EphemeralLocalSsdLabel:
		return isLocalSSDRequirement(req, apiv1.NodeSelectorOpNotIn)
	default:
		return false
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
