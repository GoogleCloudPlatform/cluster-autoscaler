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

package rules

import (
	"fmt"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/klog/v2"
)

const (
	GeneralPurposePodFamily    = "general-purpose"
	GeneralPurposeArmPodFamily = "general-purpose-arm"
)

// TODO(b/534602062): Make the machine family list configurable (e.g., via configmap or flags) instead of hardcoding in code.
var podFamilyMachineFamilies = map[string][]machinetypes.MachineFamily{
	GeneralPurposePodFamily:    {machinetypes.E2, machinetypes.EK, machinetypes.E4},
	GeneralPurposeArmPodFamily: {machinetypes.E4A, machinetypes.N4A, machinetypes.C4A},
}

// ExtendedFallbacks is a list of machine families used as fallbacks when extended fallbacks are enabled.
// The order of families in this list matters as it defines the priority tiers for fallback.
// Cluster Autoscaler evaluates these sequentially; if valid candidates are found in a higher-priority
// family (earlier in the list), lower-priority families will not be evaluated.
var ExtendedFallbacks = []machinetypes.MachineFamily{
	machinetypes.N4, machinetypes.N4D,
	machinetypes.N2, machinetypes.N2D,
	machinetypes.N1,
	machinetypes.C4, machinetypes.C4D,
}

// PodFamilyRule is an interface for rules with podFamily defined.
type PodFamilyRule interface {
	BaseRule
	PodFamilyName() string
	PodFamilyMachineFamilies() ([]machinetypes.MachineFamily, error)
	IsCustomFamiliesConfigured() bool
}

type podFamilyRule struct {
	podFamily                     *string
	autopilotMode                 bool
	generalPurposeMachineFamilies []machinetypes.MachineFamily
}

// Matches returns true if the nodegroup is matching machine spec.
func (r *podFamilyRule) Matches(nodeGroup cloudprovider.NodeGroup) bool {
	// This check applies only to autopilot managed node groups,
	// either in GKE Standard managed by AP or in GKE Autopilot
	if !r.autopilotMode {
		return true
	}

	mig, ok := nodeGroup.(gkeNodeGroup)
	if !ok {
		klog.Errorf("expected GkeMig; got %v", nodeGroup)
		return false
	}
	// Check for machine type.
	migMachineFamily, err := mig.MachineConfigProvider().GetMachineFamilyFromMachineName(mig.MachineType())
	if err != nil {
		klog.Errorf("Cannot find machine family for the machine type: %v", mig.MachineType())
		return false
	}

	value, found := mig.Spec().Labels[gkelabels.PodsPerNodeKey]
	usesBSoHW := found && value == gkelabels.BinpackedSliceOfHardwareValue

	if r.podFamily == nil {
		// If podFamily is not defined, then BPSoHW needs to be defined
		return usesBSoHW
	} else if usesBSoHW {
		// If podFamily and BPSoHW are both defined, then nodegroup does not match
		return false
	}

	// Custom GP families override (Custom Mode).
	if *r.podFamily == GeneralPurposePodFamily && len(r.generalPurposeMachineFamilies) > 0 {
		return migMachineFamily.In(r.generalPurposeMachineFamilies...)
	}

	if migMachineFamily.In(podFamilyMachineFamilies[*r.podFamily]...) {
		return true
	}
	if *r.podFamily == GeneralPurposePodFamily && mig.IsExtendedFallbacksEnabled() {
		return migMachineFamily.In(ExtendedFallbacks...)
	}
	return false
}

// PodFamilyName returns the pod family name specified by rule.
func (r *podFamilyRule) PodFamilyName() string {
	if r.podFamily == nil {
		return ""
	}
	return *r.podFamily
}

// PodFamilyMachineFamilies returns machine families associated with the rule.
func (r *podFamilyRule) PodFamilyMachineFamilies() ([]machinetypes.MachineFamily, error) {
	if r.podFamily == nil || podFamilyMachineFamilies[*r.podFamily] == nil {
		return nil, fmt.Errorf("unknown pod family")
	}

	// If custom GP families are configured, return them.
	if *r.podFamily == GeneralPurposePodFamily && len(r.generalPurposeMachineFamilies) > 0 {
		return r.generalPurposeMachineFamilies, nil
	}

	return podFamilyMachineFamilies[*r.podFamily], nil
}

// IsCustomFamiliesConfigured returns true if custom GP families are configured.
func (r *podFamilyRule) IsCustomFamiliesConfigured() bool {
	return r.podFamily != nil && *r.podFamily == GeneralPurposePodFamily && len(r.generalPurposeMachineFamilies) > 0
}

// WithAutopilotModeRule return RuleOption setting this rule to be using
// autopilot mode. This is either in GKE Autopilot or in GKE Standard with
// autopilot managed CCC.
func WithAutopilotModeRule() RuleOption {
	return func(r *rule) {
		r.podFamilyRule.autopilotMode = true
	}
}

// WithPodFamilyRule returns RuleOption setting PodFamilyRule.
func WithPodFamilyRule(podFamily *string, generalPurposeMachineFamilies ...machinetypes.MachineFamily) RuleOption {
	return func(r *rule) {
		r.podFamilyRule.podFamily = podFamily
		r.podFamilyRule.generalPurposeMachineFamilies = generalPurposeMachineFamilies
	}
}
