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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

// noLocalSSDRequirement is ANDed into every node selector term by withSuspensionConstraints.
var noLocalSSDRequirement = apiv1.NodeSelectorRequirement{
	Key:      labels.EphemeralLocalSsdLabel,
	Operator: apiv1.NodeSelectorOpNotIn,
	Values:   []string{labels.EphemeralLocalSsdEnabledValue},
}

// TODO(b/570359780): Extend the Local SSD label set beyond EphemeralLocalSsdLabel.
//
// podRequestsLocalSSD reports whether pod's own scheduling constraints require nodes with
// ephemeral Local SSDs, which makes it unschedulable on any node that standby buffers support. It
// extracts the pod's label requirements via podrequirements (nodeSelector plus In/Exists affinity
// expressions) so the check matches how NAP interprets the pod when provisioning.
func podRequestsLocalSSD(pod *apiv1.Pod) bool {
	if pod == nil {
		return false
	}
	vals, found := podrequirements.GetRequirements(pod).LabelReq.GetValues(labels.EphemeralLocalSsdLabel)
	// IsAny() covers Exists affinities (where vals.Get() is nil): GKE only sets
	// EphemeralLocalSsdLabel on nodes with Local SSDs, so Exists also requires Local SSDs.
	return found && (vals.IsAny() || vals.Get()[labels.EphemeralLocalSsdEnabledValue])
}

func hasLocalSSD(node *apiv1.Node) bool {
	return node.Labels[labels.EphemeralLocalSsdLabel] == labels.EphemeralLocalSsdEnabledValue
}
