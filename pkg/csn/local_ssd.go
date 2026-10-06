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
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

// TODO(b/570359780): Extend the Local SSD label set beyond EphemeralLocalSsdLabel.
//
// podRequestsLocalSSD reports whether pod's own scheduling constraints require nodes with
// ephemeral Local SSDs, which makes it unschedulable on any node that standby buffers support.
func podRequestsLocalSSD(pod *apiv1.Pod) bool {
	if pod == nil {
		return false
	}
	if pod.Spec.NodeSelector[labels.EphemeralLocalSsdLabel] == labels.EphemeralLocalSsdEnabledValue {
		return true
	}
	if pod.Spec.Affinity == nil ||
		pod.Spec.Affinity.NodeAffinity == nil ||
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return false
	}
	terms := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) == 0 {
		return false
	}
	for _, term := range terms {
		if !termRequiresLocalSSD(term) {
			return false
		}
	}
	return true
}

func termRequiresLocalSSD(term apiv1.NodeSelectorTerm) bool {
	for _, req := range term.MatchExpressions {
		if isLocalSSDRequirement(req, apiv1.NodeSelectorOpIn) {
			return true
		}
	}
	return false
}

func isLocalSSDRequirement(req apiv1.NodeSelectorRequirement, op apiv1.NodeSelectorOperator) bool {
	return req.Key == labels.EphemeralLocalSsdLabel &&
		req.Operator == op &&
		len(req.Values) == 1 &&
		req.Values[0] == labels.EphemeralLocalSsdEnabledValue
}

func hasLocalSSD(node *apiv1.Node) bool {
	return node.Labels[labels.EphemeralLocalSsdLabel] == labels.EphemeralLocalSsdEnabledValue
}
