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

var podFamilyMachineFamilies = map[string][]machinetypes.MachineFamily{
	GeneralPurposePodFamily:    {machinetypes.E2, machinetypes.EK, machinetypes.E4},
	GeneralPurposeArmPodFamily: {machinetypes.E4A, machinetypes.N4A, machinetypes.C4A},
}

// PodFamilyRule is an interface for rules with podFamily defined.
type PodFamilyRule interface {
	BaseRule
	PodFamilyName() string
	PodFamilyMachineFamilies() ([]machinetypes.MachineFamily, error)
}

type podFamilyRule struct {
	podFamily     *string
	autopilotMode bool
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

	return migMachineFamily.In(podFamilyMachineFamilies[*r.podFamily]...)
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

	return podFamilyMachineFamilies[*r.podFamily], nil
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
func WithPodFamilyRule(podFamily *string) RuleOption {
	return func(r *rule) {
		r.podFamilyRule.podFamily = podFamily
	}
}
