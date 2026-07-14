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
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/klog/v2"
)

const (
	defaultMinCores    = 0
	defaultMinMemoryGb = 0
	defaultSpot        = false
)

// MachineSpecRule is an interface for rules with machine spec defined.
type MachineSpecRule interface {
	BaseRule
	MachineFamily() string
	MachineType() string
	Spot() bool
	MinCores() int64
	MinMemoryGb() int64
}

type machineSpecRule struct {
	machineFamily *string
	machineType   *string
	spot          *bool
	minCores      *int
	minMemoryGb   *int
}

// Matches returns true if the nodegroup is matching machine spec.
func (r *machineSpecRule) Matches(nodeGroup cloudprovider.NodeGroup) bool {
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

	if r.machineFamily != nil && *r.machineFamily != migMachineFamily.Name() {
		return false
	}
	if r.machineType != nil && *r.machineType != mig.MachineType() {
		return false
	}
	if r.spot != nil && *r.spot != mig.Spec().Spot {
		return false
	}

	// Check for min cores and memory.
	migMachineType, err := mig.MachineConfigProvider().ToMachineType(mig.MachineType())
	if err != nil {
		klog.Errorf("Cannot convert nodeGroup machine type: %v to GCE machine type", mig.MachineType())
		return false
	}

	if r.minCores != nil && migMachineType.CPU < int64(*r.minCores) {
		return false
	}
	if r.minMemoryGb != nil && migMachineType.Memory/units.GiB < int64(*r.minMemoryGb) {
		return false
	}

	return true
}

// MachineFamily returns the machine family specified by rule.
func (r *machineSpecRule) MachineFamily() string {
	if r.machineFamily == nil {
		return ""
	}
	return *r.machineFamily
}

// MachineType returns the machine type specified by rule.
func (r *machineSpecRule) MachineType() string {
	if r.machineType == nil {
		return ""
	}
	return *r.machineType
}

// Spot returns the spot setting specified by rule.
func (r *machineSpecRule) Spot() bool {
	if r.spot == nil {
		return defaultSpot
	}
	return *r.spot
}

// MinCores returns the min cores setting specified by rule.
func (r *machineSpecRule) MinCores() int64 {
	if r.minCores == nil {
		return defaultMinCores
	}
	return int64(*r.minCores)
}

// MinMemoryGb returns the min memory setting specified by rule.
func (r *machineSpecRule) MinMemoryGb() int64 {
	if r.minMemoryGb == nil {
		return defaultMinMemoryGb
	}
	return int64(*r.minMemoryGb)
}

// WithMachineFamilyRule returns RuleOption setting MachineSpecRule specifying machine family.
func WithMachineFamilyRule(machineFamily *string) RuleOption {
	return func(r *rule) {
		r.machineSpecRule.machineFamily = machineFamily
	}
}

// WithMachineTypeRule returns RuleOption setting MachineSpecRule specifying machine type.
func WithMachineTypeRule(machineType *string) RuleOption {
	return func(r *rule) {
		r.machineSpecRule.machineType = machineType
	}
}

// WithSpotRule returns RuleOption setting MachineSpecRule specifying spot.
func WithSpotRule(spot *bool) RuleOption {
	return func(r *rule) {
		r.machineSpecRule.spot = spot
	}
}

// WithMinCoresRule returns RuleOption setting MachineSpecRule specifying min cores.
func WithMinCoresRule(minCores *int) RuleOption {
	return func(r *rule) {
		r.machineSpecRule.minCores = minCores
	}
}

// WithMinMemoryGbRule returns RuleOption setting MachineSpecRule specifying min memory.
func WithMinMemoryGbRule(minMemoryGb *int) RuleOption {
	return func(r *rule) {
		r.machineSpecRule.minMemoryGb = minMemoryGb
	}
}
