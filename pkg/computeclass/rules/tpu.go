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
	"k8s.io/klog/v2"
)

// TpuRule is an interface for rules with TPU.
type TpuRule interface {
	BaseRule
	TpuType() string
	TpuCount() int64
	TpuTopology() string
	HasTpu() bool
}

type tpuRule struct {
	tpuType     *string
	tpuCount    *int64
	tpuTopology *string
}

// Matches returns true if the nodegroup is within one of the nodepools.
func (r *tpuRule) Matches(nodeGroup cloudprovider.NodeGroup) bool {
	mig, ok := nodeGroup.(gkeNodeGroup)
	if !ok {
		klog.Errorf("expected GkeMig; got %v", nodeGroup)
		return false
	}

	if r.tpuType == nil && r.tpuCount == nil && r.tpuTopology == nil {
		return true
	}
	if mig.Spec() == nil {
		return false
	}

	migMachineType, err := mig.MachineConfigProvider().ToMachineType(mig.MachineType())
	if err != nil {
		klog.Errorf("Cannot convert nodeGroup machine type: %v to GCE machine type", mig.MachineType())
		return false
	}

	// Check for TPU.
	if r.tpuTopology != nil && mig.Spec().TpuTopology != *r.tpuTopology {
		return false
	}
	if r.tpuType != nil && mig.Spec().TpuType != *r.tpuType {
		return false
	}

	tpuCount, err := mig.NodeTpuCount()
	if err != nil {
		klog.Errorf("Failed to get tpuCount for machine family %s: %v", migMachineType.Name, err)
		return false
	}
	if r.tpuCount != nil && tpuCount != *r.tpuCount {
		return false
	}

	return true
}

// TpuType return tpu type of rule.
func (r *tpuRule) TpuType() string {
	if r.tpuType == nil {
		return ""
	}
	return *r.tpuType
}

// TpuCount return tpu count of rule.
func (r *tpuRule) TpuCount() int64 {
	if r.tpuCount == nil {
		return 0
	}
	return *r.tpuCount
}

// TpuTopology return tpu topology of rule.
func (r *tpuRule) TpuTopology() string {
	if r.tpuTopology == nil {
		return ""
	}
	return *r.tpuTopology
}

// HasTpu tells whether TPU is configured for this rule.
func (r *tpuRule) HasTpu() bool {
	return r.tpuType != nil
}

// WithTpuRule returns RuleOption adding TpuRule.
func WithTpuRule(tpuType string, tpuCount int64, tpuTopology string) RuleOption {
	return func(r *rule) {
		r.tpuRule.tpuType = &tpuType
		r.tpuRule.tpuCount = &tpuCount
		r.tpuRule.tpuTopology = &tpuTopology
	}
}
