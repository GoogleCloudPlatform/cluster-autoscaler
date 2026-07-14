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

package ccc

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	realccc "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
)

// CrdBuilder is a builder for the CCC CRD wrapper.
type CrdBuilder struct {
	cc               *v1.ComputeClass
	projectID        string
	autopilotEnabled bool
	optionsTracker   *optstracking.OptionsTracker
}

// NewCccCrdBuilder creates a new CccCrdBuilder for the given ComputeClass with default test settings.
func NewCccCrdBuilder(cc *v1.ComputeClass) *CrdBuilder {
	return &CrdBuilder{
		cc: cc,
	}
}

// WithProjectId sets the project ID.
func (b *CrdBuilder) WithProjectId(projectId string) *CrdBuilder {
	b.projectID = projectId
	return b
}

// WithAutopilotEnabled sets whether Autopilot is enabled.
func (b *CrdBuilder) WithAutopilotEnabled(enabled bool) *CrdBuilder {
	b.autopilotEnabled = enabled
	return b
}

// WithOptionsTracker sets the options tracker.
func (b *CrdBuilder) WithOptionsTracker(tracker *optstracking.OptionsTracker) *CrdBuilder {
	b.optionsTracker = tracker
	return b
}

// Build builds and returns the wrapped CRD.
func (b *CrdBuilder) Build() crd.CRD {
	return realccc.NewCccCrd(b.cc, b.projectID, b.autopilotEnabled, crd.TestDefaultDataProvider(), b.optionsTracker)
}

// ComputeClassBuilder is a builder for v1.ComputeClass.
type ComputeClassBuilder struct {
	cc *v1.ComputeClass
}

// NewComputeClassBuilder creates a new ComputeClassBuilder with default values.
func NewComputeClassBuilder(name string) *ComputeClassBuilder {
	return &ComputeClassBuilder{
		cc: &v1.ComputeClass{
			TypeMeta: metav1.TypeMeta{
				Kind:       "ComputeClass",
				APIVersion: "cloud.google.com/v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
			Spec: v1.ComputeClassSpec{},
		},
	}
}

// WithNodePoolAutoCreation sets the NodePoolAutoCreation field.
func (b *ComputeClassBuilder) WithNodePoolAutoCreation(enabled bool) *ComputeClassBuilder {
	b.cc.Spec.NodePoolAutoCreation = &v1.NodePoolAutoCreation{
		Enabled: enabled,
	}
	return b
}

// WithWhenUnsatisfiable sets the WhenUnsatisfiable field.
func (b *ComputeClassBuilder) WithWhenUnsatisfiable(when string) *ComputeClassBuilder {
	b.cc.Spec.WhenUnsatisfiable = when
	return b
}

// WithNapEnabled enables NodePoolAutoCreation.
func (b *ComputeClassBuilder) WithNapEnabled() *ComputeClassBuilder {
	return b.WithNodePoolAutoCreation(true)
}

// WithNapDisabled disables NodePoolAutoCreation.
func (b *ComputeClassBuilder) WithNapDisabled() *ComputeClassBuilder {
	return b.WithNodePoolAutoCreation(false)
}

// WithPriorities sets the priorities.
func (b *ComputeClassBuilder) WithPriorities(priorities ...v1.Priority) *ComputeClassBuilder {
	b.cc.Spec.Priorities = priorities
	return b
}

// WithNodePoolsRules sets the priorities such that there is one priority per node pool.
func (b *ComputeClassBuilder) WithNodePoolsRules(nodePools ...string) *ComputeClassBuilder {
	var priorities []v1.Priority
	for _, name := range nodePools {
		priorities = append(priorities,
			v1.Priority{Nodepools: []string{name}})
	}
	return b.WithPriorities(priorities...)
}

// WithAllocationStrategyDefaults sets the allocation strategy defaults.
func (b *ComputeClassBuilder) WithAllocationStrategyDefaults(defaults *v1.AllocationStrategyDefaults) *ComputeClassBuilder {
	b.cc.Spec.AllocationStrategyDefaults = defaults
	return b
}

// AddPriority appends a priority to the list.
func (b *ComputeClassBuilder) AddPriority(priority v1.Priority) *ComputeClassBuilder {
	b.cc.Spec.Priorities = append(b.cc.Spec.Priorities, priority)
	return b
}

// WithTargetNodeCount sets the TargetNodeCount for the MinimumCapacity.
func (b *ComputeClassBuilder) WithTargetNodeCount(count *int) *ComputeClassBuilder {
	if b.cc.Spec.MinimumCapacity == nil {
		b.cc.Spec.MinimumCapacity = &v1.MinimumCapacity{}
	}
	b.cc.Spec.MinimumCapacity.TargetNodeCount = count
	return b
}

// WithActiveMigration sets the ActiveMigration field.
func (b *ComputeClassBuilder) WithActiveMigration(optimizeRulePriority bool) *ComputeClassBuilder {
	b.cc.Spec.ActiveMigration = &v1.ActiveMigration{
		OptimizeRulePriority: optimizeRulePriority,
	}
	return b
}

// WithLabels sets the Labels field.
func (b *ComputeClassBuilder) WithLabels(labels map[string]string) *ComputeClassBuilder {
	b.cc.Labels = labels
	return b
}

// WithLabel sets a single label.
func (b *ComputeClassBuilder) WithLabel(k, v string) *ComputeClassBuilder {
	if b.cc.Labels == nil {
		b.cc.Labels = make(map[string]string)
	}
	b.cc.Labels[k] = v
	return b
}

// Build returns the constructed ComputeClass.
func (b *ComputeClassBuilder) Build() *v1.ComputeClass {
	return b.cc
}

// CreateNamedCCCWithNodePoolsRules creates a ComputeClass with priorities for the specified nodepools.
func CreateNamedCCCWithNodePoolsRules(name string, nodePools []string) *v1.ComputeClass {
	var priorities []v1.Priority
	for _, npName := range nodePools {
		priorities = append(priorities,
			v1.Priority{Nodepools: []string{npName}})
	}
	return NewComputeClassBuilder(name).WithPriorities(priorities...).Build()
}

// Clone creates a deep copy of the ComputeClassBuilder.
func (b *ComputeClassBuilder) Clone() *ComputeClassBuilder {
	return &ComputeClassBuilder{
		cc: b.cc.DeepCopy(),
	}
}

// AssertComputeClassConditions asserts that the conditions of actual match expected ComputeClassStatus while ignoring fields like LastTransitionTime, ResourceInfo, and ScalingEventsHistory.
func AssertComputeClassConditions(t *testing.T, expected, actual v1.ComputeClassStatus, msg string) {
	t.Helper()
	diff := cmp.Diff(expected, actual,
		cmpopts.IgnoreFields(metav1.Condition{}, "LastTransitionTime"),
		cmpopts.IgnoreFields(v1.PriorityStatus{}, "ResourceInfo", "ScalingEventsHistory"),
		cmpopts.IgnoreFields(v1.ComputeClassStatus{}, "ResourceInfo"),
	)
	assert.Empty(t, diff, "%s (-want +got):\n%s", msg, diff)
}

// ConditionBuilder is a builder for metav1.Condition.
type ConditionBuilder struct {
	cond metav1.Condition
}

// NewConditionBuilder creates a new ConditionBuilder with given type and default status ConditionTrue.
func NewConditionBuilder(condType string) *ConditionBuilder {
	return &ConditionBuilder{
		cond: metav1.Condition{
			Type:   condType,
			Status: metav1.ConditionTrue,
		},
	}
}

// WithStatus sets the status of the condition.
func (b *ConditionBuilder) WithStatus(status metav1.ConditionStatus) *ConditionBuilder {
	b.cond.Status = status
	return b
}

// WithReason sets the reason of the condition.
func (b *ConditionBuilder) WithReason(reason string) *ConditionBuilder {
	b.cond.Reason = reason
	return b
}

// WithMessage sets the message of the condition.
func (b *ConditionBuilder) WithMessage(message string) *ConditionBuilder {
	b.cond.Message = message
	return b
}

// Build returns the constructed Condition.
func (b *ConditionBuilder) Build() metav1.Condition {
	return b.cond
}

// PriorityStatusBuilder is a builder for v1.PriorityStatus.
type PriorityStatusBuilder struct {
	ps v1.PriorityStatus
}

// NewPriorityStatusBuilder creates a new PriorityStatusBuilder for the given priority identifier.
func NewPriorityStatusBuilder(identifier string) *PriorityStatusBuilder {
	return &PriorityStatusBuilder{
		ps: v1.PriorityStatus{
			Identifier:   identifier,
			Conditions:   []metav1.Condition{},
			ResourceInfo: []v1.ResourceInfo{},
		},
	}
}

// WithConditions sets the conditions on the PriorityStatus.
func (b *PriorityStatusBuilder) WithConditions(conditions ...metav1.Condition) *PriorityStatusBuilder {
	b.ps.Conditions = conditions
	return b
}

// AddCondition appends a condition to the PriorityStatus.
func (b *PriorityStatusBuilder) AddCondition(condition metav1.Condition) *PriorityStatusBuilder {
	b.ps.Conditions = append(b.ps.Conditions, condition)
	return b
}

// WithResourceInfo sets the ResourceInfo field.
func (b *PriorityStatusBuilder) WithResourceInfo(resourceInfo ...v1.ResourceInfo) *PriorityStatusBuilder {
	b.ps.ResourceInfo = resourceInfo
	return b
}

// Build returns the constructed PriorityStatus.
func (b *PriorityStatusBuilder) Build() v1.PriorityStatus {
	return b.ps
}

// ComputeClassStatusBuilder is a builder for v1.ComputeClassStatus.
type ComputeClassStatusBuilder struct {
	status v1.ComputeClassStatus
}

// NewComputeClassStatusBuilder creates a new ComputeClassStatusBuilder with a default healthy CRD condition.
func NewComputeClassStatusBuilder() *ComputeClassStatusBuilder {
	return &ComputeClassStatusBuilder{
		status: v1.ComputeClassStatus{
			Conditions: []metav1.Condition{
				NewConditionBuilder("Health").WithReason("Health").WithMessage("Crd is healthy.").Build(),
			},
			PriorityStatuses: []v1.PriorityStatus{},
			ResourceInfo:     []v1.ResourceInfo{},
		},
	}
}

// WithConditions sets top-level conditions on the ComputeClassStatus.
func (b *ComputeClassStatusBuilder) WithConditions(conditions ...metav1.Condition) *ComputeClassStatusBuilder {
	b.status.Conditions = conditions
	return b
}

// WithPriorityStatuses sets the PriorityStatuses field.
func (b *ComputeClassStatusBuilder) WithPriorityStatuses(priorityStatuses ...v1.PriorityStatus) *ComputeClassStatusBuilder {
	b.status.PriorityStatuses = priorityStatuses
	return b
}

// AddPriorityStatus appends a PriorityStatus to the list.
func (b *ComputeClassStatusBuilder) AddPriorityStatus(priorityStatus v1.PriorityStatus) *ComputeClassStatusBuilder {
	b.status.PriorityStatuses = append(b.status.PriorityStatuses, priorityStatus)
	return b
}

// WithResourceInfo sets the ResourceInfo field.
func (b *ComputeClassStatusBuilder) WithResourceInfo(resourceInfo ...v1.ResourceInfo) *ComputeClassStatusBuilder {
	b.status.ResourceInfo = resourceInfo
	return b
}

// Build returns the constructed ComputeClassStatus.
func (b *ComputeClassStatusBuilder) Build() v1.ComputeClassStatus {
	return b.status
}
