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
	ccc_api "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
)

func TestCccCRDStatus(t *testing.T) {
	testCases := []struct {
		name           string
		operations     func(s crd.CRDStatus)
		expectedStatus ccc_api.ComputeClassStatus
	}{
		{
			name: "UpdateConditions",
			operations: func(s crd.CRDStatus) {
				s.UpdateConditions([]metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}})
			},
			expectedStatus: ccc_api.ComputeClassStatus{
				Conditions:       []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
				ResourceInfo:     []ccc_api.ResourceInfo{},
				PriorityStatuses: []ccc_api.PriorityStatus{},
			},
		},
		{
			name: "UpdateResourceInfo",
			operations: func(s crd.CRDStatus) {
				s.UpdateResourceInfo(crd.ResourceInfo{Name: crd.ResourceName("CPU"), TargetCount: 10})
			},
			expectedStatus: ccc_api.ComputeClassStatus{
				Conditions:       []metav1.Condition{},
				ResourceInfo:     []ccc_api.ResourceInfo{expectedResourceInfo("CPU", 10)},
				PriorityStatuses: []ccc_api.PriorityStatus{},
			},
		},
		{
			name: "UpdateRuleConditions",
			operations: func(s crd.CRDStatus) {
				s.UpdateRuleConditions("0", []metav1.Condition{{Type: "Valid", Status: metav1.ConditionTrue}})
			},
			expectedStatus: ccc_api.ComputeClassStatus{
				Conditions:   []metav1.Condition{},
				ResourceInfo: []ccc_api.ResourceInfo{},
				PriorityStatuses: []ccc_api.PriorityStatus{
					{Identifier: "0", Conditions: []metav1.Condition{{Type: "Valid", Status: metav1.ConditionTrue}}, ResourceInfo: []ccc_api.ResourceInfo{}},
				},
			},
		},
		{
			name: "UpdateRuleResourceInfo",
			operations: func(s crd.CRDStatus) {
				s.UpdateRuleResourceInfo("0", crd.ResourceInfo{Name: crd.ResourceName("Memory"), CurrentCount: 5})
			},
			expectedStatus: ccc_api.ComputeClassStatus{
				Conditions:   []metav1.Condition{},
				ResourceInfo: []ccc_api.ResourceInfo{},
				PriorityStatuses: []ccc_api.PriorityStatus{
					{
						Identifier:   "0",
						ResourceInfo: []ccc_api.ResourceInfo{expectedResourceInfoWithCurrent("Memory", 5)},
						Conditions:   []metav1.Condition{},
					},
				},
			},
		},
		{
			name: "UpdateRuleScalingHistory",
			operations: func(s crd.CRDStatus) {
				s.UpdateRuleScalingHistory("0", crd.ScalingEventsHistory{ProvisionedNodesCount: 3})
			},
			expectedStatus: ccc_api.ComputeClassStatus{
				Conditions:   []metav1.Condition{},
				ResourceInfo: []ccc_api.ResourceInfo{},
				PriorityStatuses: []ccc_api.PriorityStatus{
					{
						Identifier:           "0",
						ScalingEventsHistory: expectedScalingHistory(3),
						Conditions:           []metav1.Condition{},
						ResourceInfo:         []ccc_api.ResourceInfo{},
					},
				},
			},
		},
		{
			name: "Multiple operations independent",
			operations: func(s crd.CRDStatus) {
				s.UpdateConditions([]metav1.Condition{{Type: "Ready"}})
				s.UpdateResourceInfo(crd.ResourceInfo{Name: crd.ResourceName("CPU")})
				s.UpdateRuleConditions("0", []metav1.Condition{{Type: "Valid"}})
			},
			expectedStatus: ccc_api.ComputeClassStatus{
				Conditions:   []metav1.Condition{{Type: "Ready"}},
				ResourceInfo: []ccc_api.ResourceInfo{expectedResourceInfo("CPU", 0)},
				PriorityStatuses: []ccc_api.PriorityStatus{
					{Identifier: "0", Conditions: []metav1.Condition{{Type: "Valid"}}, ResourceInfo: []ccc_api.ResourceInfo{}},
				},
			},
		},
		{
			name: "Multiple rule indices sorted",
			operations: func(s crd.CRDStatus) {
				s.UpdateRuleConditions("2", []metav1.Condition{{Type: "Valid2"}})
				s.UpdateRuleConditions("ScaleUpAnyway", []metav1.Condition{{Type: "ValidSUA"}})
				s.UpdateRuleConditions("0", []metav1.Condition{{Type: "Valid0"}})
				s.UpdateRuleConditions("1", []metav1.Condition{{Type: "Valid1"}})
			},
			expectedStatus: ccc_api.ComputeClassStatus{
				Conditions:   []metav1.Condition{},
				ResourceInfo: []ccc_api.ResourceInfo{},
				PriorityStatuses: []ccc_api.PriorityStatus{
					{Identifier: "0", Conditions: []metav1.Condition{{Type: "Valid0"}}, ResourceInfo: []ccc_api.ResourceInfo{}},
					{Identifier: "1", Conditions: []metav1.Condition{{Type: "Valid1"}}, ResourceInfo: []ccc_api.ResourceInfo{}},
					{Identifier: "2", Conditions: []metav1.Condition{{Type: "Valid2"}}, ResourceInfo: []ccc_api.ResourceInfo{}},
					{Identifier: "ScaleUpAnyway", Conditions: []metav1.Condition{{Type: "ValidSUA"}}, ResourceInfo: []ccc_api.ResourceInfo{}},
				},
			},
		},
		{
			name: "Multiple operations on same rule",
			operations: func(s crd.CRDStatus) {
				s.UpdateRuleConditions("0", []metav1.Condition{{Type: "Valid"}})
				s.UpdateRuleResourceInfo("0", crd.ResourceInfo{Name: crd.ResourceName("Memory"), CurrentCount: 5})
			},
			expectedStatus: ccc_api.ComputeClassStatus{
				Conditions:   []metav1.Condition{},
				ResourceInfo: []ccc_api.ResourceInfo{},
				PriorityStatuses: []ccc_api.PriorityStatus{
					{
						Identifier:   "0",
						Conditions:   []metav1.Condition{{Type: "Valid"}},
						ResourceInfo: []ccc_api.ResourceInfo{expectedResourceInfoWithCurrent("Memory", 5)},
					},
				},
			},
		},
		{
			name: "Multiple resource info on same rule",
			operations: func(s crd.CRDStatus) {
				s.UpdateRuleResourceInfo("0", crd.ResourceInfo{Name: crd.ResourceName("CPU"), TargetCount: 10})
				s.UpdateRuleResourceInfo("0", crd.ResourceInfo{Name: crd.ResourceName("Memory"), CurrentCount: 5})
			},
			expectedStatus: ccc_api.ComputeClassStatus{
				Conditions:   []metav1.Condition{},
				ResourceInfo: []ccc_api.ResourceInfo{},
				PriorityStatuses: []ccc_api.PriorityStatus{
					{
						Identifier: "0",
						ResourceInfo: []ccc_api.ResourceInfo{
							expectedResourceInfo("CPU", 10),
							expectedResourceInfoWithCurrent("Memory", 5),
						},
						Conditions: []metav1.Condition{},
					},
				},
			},
		},
		{
			name: "Override existing values",
			operations: func(s crd.CRDStatus) {
				s.UpdateConditions([]metav1.Condition{{Type: "Ready", Status: metav1.ConditionFalse}})
				s.UpdateResourceInfo(crd.ResourceInfo{Name: crd.ResourceName("CPU"), TargetCount: 5})
				s.UpdateRuleConditions("0", []metav1.Condition{{Type: "Valid", Status: metav1.ConditionFalse}})
				s.UpdateRuleResourceInfo("0", crd.ResourceInfo{Name: crd.ResourceName("Memory"), CurrentCount: 5})
				s.UpdateRuleScalingHistory("0", crd.ScalingEventsHistory{ProvisionedNodesCount: 1})

				s.UpdateConditions([]metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}})
				s.UpdateResourceInfo(crd.ResourceInfo{Name: crd.ResourceName("CPU"), TargetCount: 10})
				s.UpdateRuleConditions("0", []metav1.Condition{{Type: "Valid", Status: metav1.ConditionTrue}})
				s.UpdateRuleResourceInfo("0", crd.ResourceInfo{Name: crd.ResourceName("Memory"), CurrentCount: 10})
				s.UpdateRuleScalingHistory("0", crd.ScalingEventsHistory{ProvisionedNodesCount: 3})
			},
			expectedStatus: ccc_api.ComputeClassStatus{
				Conditions:   []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
				ResourceInfo: []ccc_api.ResourceInfo{expectedResourceInfo("CPU", 10)},
				PriorityStatuses: []ccc_api.PriorityStatus{
					{
						Identifier:           "0",
						Conditions:           []metav1.Condition{{Type: "Valid", Status: metav1.ConditionTrue}},
						ResourceInfo:         []ccc_api.ResourceInfo{expectedResourceInfoWithCurrent("Memory", 10)},
						ScalingEventsHistory: expectedScalingHistory(3),
					},
				},
			},
		},
		{
			name: "ResetAllResourceInfo",
			operations: func(s crd.CRDStatus) {
				s.UpdateResourceInfo(crd.ResourceInfo{Name: crd.ResourceName("CPU"), TargetCount: 10})
				s.UpdateRuleResourceInfo("0", crd.ResourceInfo{Name: crd.ResourceName("Memory"), CurrentCount: 5})
				s.UpdateRuleResourceInfo("1", crd.ResourceInfo{Name: crd.ResourceName("CPU"), CurrentCount: 2})
				s.ResetAllResourceInfo()
			},
			expectedStatus: ccc_api.ComputeClassStatus{
				Conditions:   []metav1.Condition{},
				ResourceInfo: []ccc_api.ResourceInfo{expectedResourceInfo("CPU", 10)},
				PriorityStatuses: []ccc_api.PriorityStatus{
					{
						Identifier:   "0",
						ResourceInfo: []ccc_api.ResourceInfo{},
						Conditions:   []metav1.Condition{},
					},
					{
						Identifier:   "1",
						ResourceInfo: []ccc_api.ResourceInfo{},
						Conditions:   []metav1.Condition{},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test internal state
			s := NewCccCRDStatus("test-ccc").(*cccCRDStatus)
			tc.operations(s)

			// Ignore LastTransitionTime as it's set by metav1.Now() internally or default initialization
			opts := []cmp.Option{
				cmpopts.IgnoreFields(metav1.Condition{}, "LastTransitionTime"),
			}

			if diff := cmp.Diff(tc.expectedStatus, s.apiStatus, opts...); diff != "" {
				t.Errorf("internal status mismatch (-want +got):\n%s", diff)
			}

			// Test GetCRDStatusPatch
			s2 := NewCccCRDStatus("test-ccc")
			tc.operations(s2)
			patch := s2.GetCRDStatusPatch().(*ccc_api.ComputeClass)

			if diff := cmp.Diff("test-ccc", patch.Name); diff != "" {
				t.Errorf("patch name mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.expectedStatus, patch.Status, opts...); diff != "" {
				t.Errorf("patch status mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func expectedResourceInfo(name string, target int) ccc_api.ResourceInfo {
	n := ccc_api.ResourceName(name)
	u := ccc_api.ResourceUnit("")
	tc := target
	cc := 0
	cu := 0
	ma := metav1.Time{}
	return ccc_api.ResourceInfo{
		Name:                         &n,
		Unit:                         &u,
		TargetCount:                  &tc,
		CurrentCount:                 &cc,
		CurrentUtilizationPercentage: &cu,
		MeasuredAt:                   &ma,
	}
}

func expectedResourceInfoWithCurrent(name string, current int) ccc_api.ResourceInfo {
	n := ccc_api.ResourceName(name)
	u := ccc_api.ResourceUnit("")
	tc := 0
	cc := current
	cu := 0
	ma := metav1.Time{}
	return ccc_api.ResourceInfo{
		Name:                         &n,
		Unit:                         &u,
		TargetCount:                  &tc,
		CurrentCount:                 &cc,
		CurrentUtilizationPercentage: &cu,
		MeasuredAt:                   &ma,
	}
}

func expectedScalingHistory(provisioned int) *ccc_api.ScalingEventsHistory {
	c := 0
	p := provisioned
	m := 0
	ma := metav1.Time{}
	ms := metav1.Time{}
	return &ccc_api.ScalingEventsHistory{
		ConsolidatedNodesCount: &c,
		ProvisionedNodesCount:  &p,
		MigratedNodesCount:     &m,
		MeasuredAt:             &ma,
		MeasuredSince:          &ms,
	}
}

func TestCccCRDStatus_GetConditions(t *testing.T) {
	testCases := []struct {
		name               string
		operations         func(s crd.CRDStatus)
		expectedConditions []metav1.Condition
	}{
		{
			name: "No conditions",
			operations: func(s crd.CRDStatus) {
			},
			expectedConditions: []metav1.Condition{},
		},
		{
			name: "With conditions",
			operations: func(s crd.CRDStatus) {
				s.UpdateConditions([]metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}})
			},
			expectedConditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewCccCRDStatus("test-ccc")
			tc.operations(s)

			got := s.GetConditions()
			if diff := cmp.Diff(tc.expectedConditions, got, cmpopts.IgnoreFields(metav1.Condition{}, "LastTransitionTime")); diff != "" {
				t.Errorf("GetConditions mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCccCRDStatus_GetRuleConditions(t *testing.T) {
	testCases := []struct {
		name               string
		operations         func(s crd.CRDStatus)
		ruleIdx            string
		expectedConditions []metav1.Condition
	}{
		{
			name: "No matching priority",
			operations: func(s crd.CRDStatus) {
				s.UpdateRuleConditions("0", []metav1.Condition{{Type: "Valid"}})
			},
			ruleIdx:            "1",
			expectedConditions: nil,
		},
		{
			name: "Matching priority, empty conditions",
			operations: func(s crd.CRDStatus) {
				// Creates the entry but no conditions set explicitly
				s.UpdateRuleResourceInfo("0", crd.ResourceInfo{Name: "CPU"})
			},
			ruleIdx:            "0",
			expectedConditions: []metav1.Condition{},
		},
		{
			name: "Matching priority with conditions",
			operations: func(s crd.CRDStatus) {
				s.UpdateRuleConditions("0", []metav1.Condition{{Type: "Valid", Status: metav1.ConditionTrue}})
			},
			ruleIdx:            "0",
			expectedConditions: []metav1.Condition{{Type: "Valid", Status: metav1.ConditionTrue}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewCccCRDStatus("test-ccc")
			tc.operations(s)

			got := s.GetRuleConditions(tc.ruleIdx)
			if diff := cmp.Diff(tc.expectedConditions, got, cmpopts.IgnoreFields(metav1.Condition{}, "LastTransitionTime")); diff != "" {
				t.Errorf("GetRuleConditions mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
