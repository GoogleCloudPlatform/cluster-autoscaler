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

package preemption

import (
	"fmt"

	apiv1 "k8s.io/api/core/v1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

const (
	// GkePreemptionTaintEffect is a taint effect used for preemptible/Spot node pools on GKE.
	GkePreemptionTaintEffect = apiv1.TaintEffectNoSchedule
)

// VmPreemptionType specifies the type of VM preemption.
type VmPreemptionType int

const (
	// NoPreemption means that the VM is not preemptible at all.
	NoPreemption VmPreemptionType = iota
	// LegacyPreemptible denotes a Preemptible VM (PVM), which was later deprecated in favor of Spot VMs.
	LegacyPreemptible
	// Spot denotes a Spot VM, which is a preemptible VM type that replaced Preemptible VMs.
	Spot
)

// LabelKeyValue returns the label key and value used for selecting preemptible VMs for a specified preemption type.
func (t VmPreemptionType) LabelKeyValue() (string, string, error) {
	switch t {
	case Spot:
		return gkelabels.SpotLabel, gkelabels.PreemptionValue, nil
	case LegacyPreemptible:
		return gkelabels.PreemptibleLabel, gkelabels.PreemptionValue, nil
	}
	return "", "", fmt.Errorf("method GkeKey() called on NoPreemption type - no corresponding key")
}

// ProvisioningLabelValue returns the value of provisioning type for a specific preemption type.
func (t VmPreemptionType) ProvisioningLabelValue() string {
	switch t {
	case Spot:
		return gkelabels.SpotProvisioningValue
	case LegacyPreemptible:
		return gkelabels.PreemptibleProvisioningValue
	}
	return gkelabels.StandardProvisioningValue
}

// Taint returns the taint that can be added to preemptible VMs for a specified preemption type.
func (t VmPreemptionType) Taint() (apiv1.Taint, error) {
	var key string
	switch t {
	case Spot:
		key = gkelabels.SpotLabel
	case LegacyPreemptible:
		key = gkelabels.PreemptibleLabel
	default:
		return apiv1.Taint{}, fmt.Errorf("method Taint() called on NoPreemption type - can't generate taint")
	}
	return apiv1.Taint{
		Key:    key,
		Value:  gkelabels.PreemptionValue,
		Effect: GkePreemptionTaintEffect,
	}, nil
}

// ShortName returns a short name for a preemption type. Empty string is returned for NoPreemption.
func (t VmPreemptionType) ShortName() string {
	switch t {
	case Spot:
		return "spot"
	case LegacyPreemptible:
		return "pvm"
	case NoPreemption:
		return ""
	}
	return ""
}

// IsSpot returns true for all preemptible VMs, false otherwise.
func (t VmPreemptionType) IsSpot() bool {
	switch t {
	case Spot, LegacyPreemptible:
		return true
	case NoPreemption:
		return false
	}
	return false
}

// TypeFromToleration returns the VM preemption type the toleration permits. If the toleration is not
// preemption-related, NoPreemption will be returned.
func TypeFromToleration(toleration apiv1.Toleration) VmPreemptionType {
	if toleration.Effect != "" && toleration.Effect != GkePreemptionTaintEffect {
		return NoPreemption
	}

	operatorExists := toleration.Operator == apiv1.TolerationOpExists
	operatorEqual := toleration.Operator == apiv1.TolerationOpEqual || toleration.Operator == ""
	if operatorExists || (operatorEqual && toleration.Value == gkelabels.PreemptionValue) {
		if toleration.Key == gkelabels.SpotLabel {
			return Spot
		}
		if toleration.Key == gkelabels.PreemptibleLabel {
			return LegacyPreemptible
		}
	}

	return NoPreemption
}

// TypeFromLabels returns the VM preemption type the labels indicate. If both types are indicated in the
// labels, Spot is returned.
func TypeFromLabels(labels map[string]string) VmPreemptionType {
	legacyPreemptible := false

	for k, v := range labels {
		if k == gkelabels.SpotLabel && v == gkelabels.PreemptionValue {
			return Spot
		}
		if k == gkelabels.PreemptibleLabel && v == gkelabels.PreemptionValue {
			legacyPreemptible = true
		}
	}

	if legacyPreemptible {
		return LegacyPreemptible
	}
	return NoPreemption
}

// ToleratedVmPreemptionForPod returns the type of VM preemption the pod tolerates. If the pod has tolerations for
// both types, Spot is returned.
func ToleratedVmPreemptionForPod(pod *apiv1.Pod) VmPreemptionType {
	toleratesLegacyPreemptible := false

	for _, toleration := range pod.Spec.Tolerations {
		toleratedType := TypeFromToleration(toleration)
		switch toleratedType {
		case Spot:
			return Spot
		case LegacyPreemptible:
			toleratesLegacyPreemptible = true
		}
	}

	if toleratesLegacyPreemptible {
		return LegacyPreemptible
	}
	return NoPreemption
}

// ToleratedVmPreemptionForAnyPod returns the type of VM preemption any of the pods in the list tolerate. If there are
// pods tolerating both types, Spot is returned.
func ToleratedVmPreemptionForAnyPod(pods []*apiv1.Pod) VmPreemptionType {
	podWithLegacyPreemptibleTolerationExists := false

	for _, pod := range pods {
		toleratedType := ToleratedVmPreemptionForPod(pod)
		switch toleratedType {
		case Spot:
			return Spot
		case LegacyPreemptible:
			podWithLegacyPreemptibleTolerationExists = true
		}
	}

	if podWithLegacyPreemptibleTolerationExists {
		return LegacyPreemptible
	}
	return NoPreemption
}

// PodRequiresPreemption returns whether the pod's node selector or required node affinity indicate that it can only
// run on preemptible nodes (regardless of the type).
func PodRequiresPreemption(pod *apiv1.Pod) bool {
	req := podrequirements.GetRequirements(pod)

	preemptibleValues, preemptibleFound := req.LabelReq.GetValues(gkelabels.PreemptibleLabel)
	preemptibleValue, _ := preemptibleValues.GetSingle()
	preemptibleRequired := preemptibleFound && (preemptibleValues.IsAny() || preemptibleValue == gkelabels.PreemptionValue)

	spotValues, spotFound := req.LabelReq.GetValues(gkelabels.SpotLabel)
	spotValue, _ := spotValues.GetSingle()
	spotRequired := spotFound && (spotValues.IsAny() || spotValue == gkelabels.PreemptionValue)

	return spotRequired || preemptibleRequired
}
