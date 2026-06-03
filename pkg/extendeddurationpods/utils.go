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

package extendeddurationpods

import (
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/klog/v2"

	ekvmtypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
)

// GetExtendedDurationValueFromReq extracts the extended duration value from pod requirements.
// It prioritizes a numeric value over the placeholder "X".
// If only "X" is found, "X" is returned.
// If the label is not present or has no usable values, it returns an empty string.
func GetExtendedDurationValueFromReq(req *podrequirements.Requirements) string {
	values, found := req.LabelReq.GetValues(labels.ExtendedDurationPodsLabel)
	if !found {
		return ""
	}

	valMap := values.Get()
	if len(valMap) == 0 {
		return ""
	}

	for v := range valMap {
		if v != ekvmtypes.ExtendedDurationLabelX {
			return v
		}
	}
	if _, ok := valMap[ekvmtypes.ExtendedDurationLabelX]; ok {
		klog.Warningf("No specific numeric CPU value found for extended duration pod.")
		return ekvmtypes.ExtendedDurationLabelX
	}
	return ""
}

// EdpSelector returns the string value specified in node selector/ node affinity of the given pod
func EdpSelector(pod *apiv1.Pod) string {
	req := podrequirements.GetRequirements(pod)
	return GetExtendedDurationValueFromReq(req)
}

func EdpOnePodPerNode(node *apiv1.Node) bool {
	edpValue, found := node.Labels[labels.ExtendedDurationPodsLabel]
	if !found {
		return false
	}

	machineFamily := node.Labels[labels.MachineFamilyLabel]

	return machineFamily != machinetypes.EK.Name() && edpValue != "" && edpValue != labels.ExtendedDurationPackedPodsValue
}
