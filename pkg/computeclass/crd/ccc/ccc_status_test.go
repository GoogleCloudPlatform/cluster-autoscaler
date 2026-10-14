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
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	ccc_api "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/observability"
	"k8s.io/utils/ptr"
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
		{
			name: "UpdateConfigDriftInfo",
			operations: func(s crd.CRDStatus) {
				s.UpdateConfigDriftInfo(crd.ConfigDriftInfo{
					CurrentNodes:   3,
					DriftedNodes:   2,
					MigratingNodes: 1,
					BlockedNodes: []crd.BlockedNodesInfo{
						{Reason: "Cordoned", Count: 4},
						{Reason: "BlockingPods", Count: 5},
					},
					MeasuredAt: configDriftMeasuredAt,
				})
			},
			expectedStatus: ccc_api.ComputeClassStatus{
				Conditions:       []metav1.Condition{},
				ResourceInfo:     []ccc_api.ResourceInfo{},
				PriorityStatuses: []ccc_api.PriorityStatus{},
				Migration: &ccc_api.MigrationStatus{
					ConfigDrift: &ccc_api.ConfigDriftStatus{
						CurrentNodes:   intPtr(3),
						DriftedNodes:   intPtr(2),
						MigratingNodes: intPtr(1),
						BlockedNodes: []ccc_api.BlockedNodesInfo{
							{Reason: "Cordoned", Count: 4},
							{Reason: "BlockingPods", Count: 5},
						},
						MeasuredAt: &configDriftMeasuredAt,
					},
				},
			},
		},
		{
			name: "ResetConfigDriftInfo",
			operations: func(s crd.CRDStatus) {
				s.UpdateConfigDriftInfo(crd.ConfigDriftInfo{CurrentNodes: 3, MeasuredAt: configDriftMeasuredAt})
				s.ResetConfigDriftInfo()
			},
			expectedStatus: ccc_api.ComputeClassStatus{
				Conditions:       []metav1.Condition{},
				ResourceInfo:     []ccc_api.ResourceInfo{},
				PriorityStatuses: []ccc_api.PriorityStatus{},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test internal state
			s := NewCccCRDStatus("test-ccc").(*cccCRDStatus)
			tc.operations(s)

			if diff := cmp.Diff(tc.expectedStatus, s.apiStatus); diff != "" {
				t.Errorf("internal status mismatch (-want +got):\n%s", diff)
			}

			// Test GetCRDStatusPatch
			s2 := NewCccCRDStatus("test-ccc")
			tc.operations(s2)
			patch := s2.GetCRDStatusPatch().(*ccc_api.ComputeClass)

			if diff := cmp.Diff("test-ccc", patch.Name); diff != "" {
				t.Errorf("patch name mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.expectedStatus, patch.Status); diff != "" {
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
	now := time.Now()
	tRecent := metav1.NewTime(now)
	tOld := metav1.NewTime(now.Add(-10 * time.Minute))
	tNew := metav1.NewTime(now.Add(1 * time.Minute))

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
		{
			name: "identical condition within maxConditionAge preserves original LastTransitionTime",
			operations: func(s crd.CRDStatus) {
				s.UpdateConditions([]metav1.Condition{{
					Type:               "Ready",
					Status:             metav1.ConditionTrue,
					Reason:             "ReasonA",
					Message:            "MessageA",
					LastTransitionTime: tRecent,
				}})
				s.UpdateConditions([]metav1.Condition{{
					Type:               "Ready",
					Status:             metav1.ConditionTrue,
					Reason:             "ReasonA",
					Message:            "MessageA",
					LastTransitionTime: tNew,
				}})
			},
			expectedConditions: []metav1.Condition{{
				Type:               "Ready",
				Status:             metav1.ConditionTrue,
				Reason:             "ReasonA",
				Message:            "MessageA",
				LastTransitionTime: tRecent,
			}},
		},
		{
			name: "status change within maxConditionAge updates condition and LastTransitionTime",
			operations: func(s crd.CRDStatus) {
				s.UpdateConditions([]metav1.Condition{{
					Type:               "Ready",
					Status:             metav1.ConditionTrue,
					Reason:             "ReasonA",
					Message:            "MessageA",
					LastTransitionTime: tRecent,
				}})
				s.UpdateConditions([]metav1.Condition{{
					Type:               "Ready",
					Status:             metav1.ConditionFalse,
					Reason:             "ReasonB",
					Message:            "MessageB",
					LastTransitionTime: tNew,
				}})
			},
			expectedConditions: []metav1.Condition{{
				Type:               "Ready",
				Status:             metav1.ConditionFalse,
				Reason:             "ReasonB",
				Message:            "MessageB",
				LastTransitionTime: tNew,
			}},
		},
		{
			name: "identical condition older than maxConditionAge updates LastTransitionTime",
			operations: func(s crd.CRDStatus) {
				s.UpdateConditions([]metav1.Condition{{
					Type:               "Ready",
					Status:             metav1.ConditionTrue,
					Reason:             "ReasonA",
					Message:            "MessageA",
					LastTransitionTime: tOld,
				}})
				s.UpdateConditions([]metav1.Condition{{
					Type:               "Ready",
					Status:             metav1.ConditionTrue,
					Reason:             "ReasonA",
					Message:            "MessageA",
					LastTransitionTime: tNew,
				}})
			},
			expectedConditions: []metav1.Condition{{
				Type:               "Ready",
				Status:             metav1.ConditionTrue,
				Reason:             "ReasonA",
				Message:            "MessageA",
				LastTransitionTime: tNew,
			}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewCccCRDStatus("test-ccc")
			tc.operations(s)

			got := s.GetConditions()
			if diff := cmp.Diff(tc.expectedConditions, got); diff != "" {
				t.Errorf("GetConditions mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCccCRDStatus_GetRuleConditions(t *testing.T) {
	now := time.Now()
	tRecent := metav1.NewTime(now)
	tOld := metav1.NewTime(now.Add(-10 * time.Minute))
	tNew := metav1.NewTime(now.Add(1 * time.Minute))

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
		{
			name: "identical condition within maxConditionAge preserves original LastTransitionTime",
			operations: func(s crd.CRDStatus) {
				s.UpdateRuleConditions("0", []metav1.Condition{{
					Type:               "Valid",
					Status:             metav1.ConditionTrue,
					Reason:             "ReasonA",
					Message:            "MessageA",
					LastTransitionTime: tRecent,
				}})
				s.UpdateRuleConditions("0", []metav1.Condition{{
					Type:               "Valid",
					Status:             metav1.ConditionTrue,
					Reason:             "ReasonA",
					Message:            "MessageA",
					LastTransitionTime: tNew,
				}})
			},
			ruleIdx: "0",
			expectedConditions: []metav1.Condition{{
				Type:               "Valid",
				Status:             metav1.ConditionTrue,
				Reason:             "ReasonA",
				Message:            "MessageA",
				LastTransitionTime: tRecent,
			}},
		},
		{
			name: "status change within maxConditionAge updates condition and LastTransitionTime",
			operations: func(s crd.CRDStatus) {
				s.UpdateRuleConditions("0", []metav1.Condition{{
					Type:               "Valid",
					Status:             metav1.ConditionTrue,
					Reason:             "ReasonA",
					Message:            "MessageA",
					LastTransitionTime: tRecent,
				}})
				s.UpdateRuleConditions("0", []metav1.Condition{{
					Type:               "Valid",
					Status:             metav1.ConditionFalse,
					Reason:             "ReasonB",
					Message:            "MessageB",
					LastTransitionTime: tNew,
				}})
			},
			ruleIdx: "0",
			expectedConditions: []metav1.Condition{{
				Type:               "Valid",
				Status:             metav1.ConditionFalse,
				Reason:             "ReasonB",
				Message:            "MessageB",
				LastTransitionTime: tNew,
			}},
		},
		{
			name: "identical condition older than maxConditionAge updates LastTransitionTime",
			operations: func(s crd.CRDStatus) {
				s.UpdateRuleConditions("0", []metav1.Condition{{
					Type:               "Valid",
					Status:             metav1.ConditionTrue,
					Reason:             "ReasonA",
					Message:            "MessageA",
					LastTransitionTime: tOld,
				}})
				s.UpdateRuleConditions("0", []metav1.Condition{{
					Type:               "Valid",
					Status:             metav1.ConditionTrue,
					Reason:             "ReasonA",
					Message:            "MessageA",
					LastTransitionTime: tNew,
				}})
			},
			ruleIdx: "0",
			expectedConditions: []metav1.Condition{{
				Type:               "Valid",
				Status:             metav1.ConditionTrue,
				Reason:             "ReasonA",
				Message:            "MessageA",
				LastTransitionTime: tNew,
			}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewCccCRDStatus("test-ccc")
			tc.operations(s)

			got := s.GetRuleConditions(tc.ruleIdx)
			if diff := cmp.Diff(tc.expectedConditions, got); diff != "" {
				t.Errorf("GetRuleConditions mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// configDriftMeasuredAt is a fixed timestamp, so that expectations can compare
// the reported one by value.
var configDriftMeasuredAt = metav1.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// TestCccCRDStatus_UpdateConfigDriftInfoUnknownReason covers the failure mode
// the closed enum creates: a reason the API does not know about makes the
// apiserver reject the whole status patch, taking every other counter with it.
func TestCccCRDStatus_UpdateConfigDriftInfoUnknownReason(t *testing.T) {
	s := NewCccCRDStatus("test-ccc").(*cccCRDStatus)
	s.UpdateConfigDriftInfo(crd.ConfigDriftInfo{
		CurrentNodes: 1,
		BlockedNodes: []crd.BlockedNodesInfo{
			{Reason: "MigrationBlocked", Count: 2},
			{Reason: "SomethingTheApiHasNeverHeardOf", Count: 3},
			{Reason: "BlockingPods", Count: 0},
		},
		MeasuredAt: configDriftMeasuredAt,
	})

	want := &ccc_api.ConfigDriftStatus{
		CurrentNodes:   intPtr(1),
		DriftedNodes:   intPtr(0),
		MigratingNodes: intPtr(0),
		// The unknown reason is folded into the catch-all and merged with the
		// entry already there, because the list is keyed by reason. The bucket
		// with no nodes is dropped, since the API requires a count of at least
		// one.
		BlockedNodes: []ccc_api.BlockedNodesInfo{{Reason: "MigrationBlocked", Count: 5}},
		MeasuredAt:   &configDriftMeasuredAt,
	}
	if diff := cmp.Diff(want, s.apiStatus.Migration.ConfigDrift); diff != "" {
		t.Errorf("config drift status mismatch (-want +got):\n%s", diff)
	}
}

// apiBlockReasonEnum returns the values the ComputeClass API actually accepts
// for BlockedNodesInfo.Reason, read from the kubebuilder enum marker in the
// vendored API.
//
// The marker is the contract the apiserver enforces. Comparing against a second
// hand-maintained list instead would make the check self-referential: both
// copies can be wrong in the same way, which is exactly how a reason the API
// rejects can reach a cluster and take a whole status patch down with it.
func apiBlockReasonEnum(t *testing.T) map[string]bool {
	t.Helper()
	// The path is resolved from this file's location rather than from the
	// working directory, which is not the package directory under every
	// build. Builds that do not vendor the API (the OSS-style one drops the
	// replace directive) have nothing to compare against and skip the check.
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("Could not determine the location of this test file")
	}
	typesPath := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "vendor", "github.com", "googlecloudplatform", "compute-class-api", "api", "cloud.google.com", "v1", "types.go")
	src, err := os.ReadFile(typesPath)
	if errors.Is(err, fs.ErrNotExist) {
		t.Skipf("The ComputeClass API is not vendored at %s; nothing to compare the reasons against", typesPath)
	}
	if err != nil {
		t.Fatalf("Failed to read the vendored ComputeClass API at %s: %v", typesPath, err)
	}
	const marker = "+kubebuilder:validation:Enum="
	body := string(src)
	structAt := strings.Index(body, "type BlockedNodesInfo struct")
	if structAt < 0 {
		t.Fatalf("Could not find BlockedNodesInfo in %s", typesPath)
	}
	markerAt := strings.Index(body[structAt:], marker)
	if markerAt < 0 {
		t.Fatalf("Could not find %q on BlockedNodesInfo.Reason in %s", marker, typesPath)
	}
	start := structAt + markerAt + len(marker)
	line := body[start:]
	if end := strings.IndexByte(line, '\n'); end >= 0 {
		line = line[:end]
	}
	enum := map[string]bool{}
	for _, value := range strings.Split(strings.TrimSpace(line), ";") {
		if value = strings.TrimSpace(value); value != "" {
			enum[value] = true
		}
	}
	if len(enum) == 0 {
		t.Fatalf("Parsed an empty enum from %s", typesPath)
	}
	return enum
}

func TestBlockReasonsAreAcceptedByTheApi(t *testing.T) {
	apiEnum := apiBlockReasonEnum(t)

	// Every reason the autoscaler can report has to be one the apiserver will
	// accept, or the status patch carrying it is rejected in full.
	for _, reason := range observability.AllReasons() {
		if !apiEnum[string(reason)] {
			t.Errorf("reason %q can be reported but the ComputeClass API enum does not accept it; the apiserver would reject the whole status patch", reason)
		}
	}

	// knownBlockReasons guards the conversion, so it has to mirror the enum
	// exactly: a missing value would be rewritten to the fallback even though
	// the API accepts it, and an extra one would be let through and rejected.
	for reason := range apiEnum {
		if !knownBlockReasons[reason] {
			t.Errorf("the ComputeClass API accepts %q but knownBlockReasons does not list it", reason)
		}
	}
	for reason := range knownBlockReasons {
		if !apiEnum[reason] {
			t.Errorf("knownBlockReasons lists %q but the ComputeClass API enum does not accept it", reason)
		}
	}

	if !apiEnum[unknownBlockReasonFallback] {
		t.Errorf("the fallback reason %q is not accepted by the ComputeClass API enum, so an unknown reason would still be rejected", unknownBlockReasonFallback)
	}
}

func TestCccCRDStatus_UpdateRuleConsolidationStatus(t *testing.T) {
	measuredAt := metav1.NewTime(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))

	testCases := []struct {
		name   string
		status crd.ConsolidationStatus
		want   *ccc_api.ConsolidationStatus
	}{
		{
			name: "known reasons are reported verbatim in the producer's order, topology units only when set",
			status: crd.ConsolidationStatus{
				ActuationInProgress: 2,
				NotProcessed:        1,
				BlockedNodes: []crd.BlockedNodesByReason{
					{Reason: crd.ConsolidationReasonUsedByFormedSlice, NodeCount: 16, TopologyUnitCount: 1},
					{Reason: crd.ConsolidationReasonBlockingPods, NodeCount: 3},
				},
				MeasuredAt: measuredAt,
			},
			want: &ccc_api.ConsolidationStatus{
				ActuationInProgress: ptr.To(2),
				NotProcessed:        ptr.To(1),
				BlockedNodes: []ccc_api.ConsolidationBlockedNodesInfo{
					{Reason: crd.ConsolidationReasonUsedByFormedSlice, NodeCount: 16, TopologyUnitCount: ptr.To(1)},
					{Reason: crd.ConsolidationReasonBlockingPods, NodeCount: 3},
				},
				MeasuredAt: &measuredAt,
			},
		},
		{
			// The CRD enum rejects unknown values, and a single invalid entry takes the whole
			// status patch down with it, so unknown reasons must never reach the API.
			name: "unknown reasons are folded into the catch-all and merged with it",
			status: crd.ConsolidationStatus{
				BlockedNodes: []crd.BlockedNodesByReason{
					{Reason: "SomethingNew", NodeCount: 2, TopologyUnitCount: 1},
					{Reason: crd.ConsolidationReasonConsolidationBlocked, NodeCount: 1},
					{Reason: "SomethingElse", NodeCount: 4, TopologyUnitCount: 2},
				},
				MeasuredAt: measuredAt,
			},
			want: &ccc_api.ConsolidationStatus{
				ActuationInProgress: ptr.To(0),
				NotProcessed:        ptr.To(0),
				BlockedNodes: []ccc_api.ConsolidationBlockedNodesInfo{
					{Reason: crd.ConsolidationReasonConsolidationBlocked, NodeCount: 7, TopologyUnitCount: ptr.To(3)},
				},
				MeasuredAt: &measuredAt,
			},
		},
		{
			name: "entries without nodes are dropped",
			status: crd.ConsolidationStatus{
				BlockedNodes: []crd.BlockedNodesByReason{
					{Reason: crd.ConsolidationReasonBlockingPods, NodeCount: 0},
					{Reason: "SomethingNew", NodeCount: -1},
				},
				MeasuredAt: measuredAt,
			},
			want: &ccc_api.ConsolidationStatus{
				ActuationInProgress: ptr.To(0),
				NotProcessed:        ptr.To(0),
				BlockedNodes:        []ccc_api.ConsolidationBlockedNodesInfo{},
				MeasuredAt:          &measuredAt,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewCccCRDStatus("test-ccc").(*cccCRDStatus)
			s.UpdateRuleConsolidationStatus("0", tc.status)

			statuses := s.apiStatus.PriorityStatuses
			if len(statuses) != 1 {
				t.Fatalf("expected exactly one priority status, got %d", len(statuses))
			}
			if diff := cmp.Diff(tc.want, statuses[0].Consolidation); diff != "" {
				t.Errorf("Consolidation mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
