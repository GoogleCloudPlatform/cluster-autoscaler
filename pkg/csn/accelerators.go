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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/accelerators"
	podutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/pod"
)

// acceleratorLabels are set by GKE on nodes with GPUs or TPUs attached. GCE VM Suspend/Resume
// doesn't support such VMs, so MakePodCSN requires every one of them to be absent.
var acceleratorLabels = []string{labels.GPULabel, labels.TPULabel}

// noAcceleratorRequirements are ANDed into every node selector term by withSuspensionConstraints.
var noAcceleratorRequirements = []apiv1.NodeSelectorRequirement{
	{Key: labels.GPULabel, Operator: apiv1.NodeSelectorOpDoesNotExist},
	{Key: labels.TPULabel, Operator: apiv1.NodeSelectorOpDoesNotExist},
}

// podRequestsAccelerator reports whether pod requires a node with a GPU or TPU, which makes it
// unschedulable on any node that standby buffers support. It reads the pod's resource requests and
// its label requirements the way NAP does (podrequirements: nodeSelector plus In/Exists affinity
// expressions merged across all terms), so a pod counts as requiring an accelerator if any required
// term selects an accelerator label, matching how NAP provisions for it.
func podRequestsAccelerator(pod *apiv1.Pod) bool {
	if pod == nil {
		return false
	}
	for name := range podutils.PodRequests(pod) {
		if accelerators.IsAccelerator(name) {
			return true
		}
	}
	labelReq := podrequirements.GetRequirements(pod).LabelReq
	for _, key := range acceleratorLabels {
		if _, found := labelReq.GetValues(key); found {
			return true
		}
	}
	return false
}

// hasAccelerator reports whether node carries any accelerator label, i.e. whether the
// DoesNotExist requirements injected by MakePodCSN reject it.
func hasAccelerator(node *apiv1.Node) bool {
	for _, key := range acceleratorLabels {
		if _, found := node.Labels[key]; found {
			return true
		}
	}
	return false
}
