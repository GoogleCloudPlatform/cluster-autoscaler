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
	"fmt"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

// SuspensionConstraints validates whether CSN suspension constraints (memory limit, Local SSD
// exclusion) are what prevent a standby buffer pod from scaling up, and formats the corresponding
// warning event message.
type SuspensionConstraints struct {
	constraints []suspensionConstraint
}

type suspensionConstraint struct {
	violatedByPod  func(*apiv1.Pod) bool
	violatedByNode func(*apiv1.Node) bool
	message        string
}

// NewSuspensionConstraints returns a composite validator for all CSN suspension constraints.
func NewSuspensionConstraints(experimentsManager experiments.Manager) SuspensionConstraints {
	memoryLimit := NewMemoryLimit(experimentsManager)
	return SuspensionConstraints{
		constraints: []suspensionConstraint{
			{
				violatedByPod:  memoryLimit.exceededByPodRequest,
				violatedByNode: memoryLimit.exceededByNode,
				message: fmt.Sprintf(
					"Standby buffers don't support nodes with %d GB of memory or more. Make sure the buffer configuration doesn't prevent the buffer from using nodes with less memory.",
					memoryLimit.GB(),
				),
			},
			{
				violatedByPod:  podRequestsLocalSSD,
				violatedByNode: hasLocalSSD,
				message:        "Standby buffers don't support nodes with Local SSDs. Make sure the buffer configuration doesn't prevent the buffer from using nodes without Local SSDs.",
			},
		},
	}
}

// BlocksPodRequest reports whether pod's own requests/selectors directly violate any CSN
// suspension constraint regardless of available node groups, returning the corresponding event
// message for the first violated constraint.
func (s SuspensionConstraints) BlocksPodRequest(pod *apiv1.Pod) (string, bool) {
	if pod == nil {
		return "", false
	}
	for _, c := range s.constraints {
		if c.violatedByPod(pod) {
			return c.message, true
		}
	}
	return "", false
}

// BlocksPodOnAnyNode reports whether any CSN suspension constraint is what keeps pod off at least
// one of nodes, returning the event message for the first violated constraint.
func (s SuspensionConstraints) BlocksPodOnAnyNode(pod *apiv1.Pod, nodes ...*apiv1.Node) (string, bool) {
	if pod == nil || len(nodes) == 0 {
		return "", false
	}
	required := nodeaffinity.GetRequiredNodeAffinity(pod)
	withoutSuspension := requiredNodeAffinityWithoutSuspensionConstraints(pod)
	for _, c := range s.constraints {
		if blockedByConstraint(nodes, required, withoutSuspension, c.violatedByNode) {
			return c.message, true
		}
	}
	return "", false
}

func blockedByConstraint(nodes []*apiv1.Node, required, withoutSuspension nodeaffinity.RequiredNodeAffinity, violatedByNode func(*apiv1.Node) bool) bool {
	for _, node := range nodes {
		if node == nil || !violatedByNode(node) {
			continue
		}
		if matched, err := required.Match(node); err != nil || matched {
			continue
		}
		if matchedWithout, err := withoutSuspension.Match(node); err == nil && matchedWithout {
			return true
		}
	}
	return false
}
