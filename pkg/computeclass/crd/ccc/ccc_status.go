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
	"sort"
	"strconv"

	ccc_api "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// cccCRDStatus is an implementation of the crd.CRDStatus interface for CCC.
type cccCRDStatus struct {
	cccName   string
	apiStatus ccc_api.ComputeClassStatus
}

// NewCccCRDStatus returns a new cccCRDStatus.
func NewCccCRDStatus(cccName string) crd.CRDStatus {
	return &cccCRDStatus{
		cccName: cccName,
		apiStatus: ccc_api.ComputeClassStatus{
			Conditions:       []metav1.Condition{},
			ResourceInfo:     []ccc_api.ResourceInfo{},
			PriorityStatuses: []ccc_api.PriorityStatus{},
		},
	}
}

// UpdateConditions implements crd.CRDStatus.
func (s *cccCRDStatus) UpdateConditions(conditions []metav1.Condition) {
	if conditions == nil {
		conditions = []metav1.Condition{}
	}
	s.apiStatus.Conditions = conditions
}

// UpdateResourceInfo implements crd.CRDStatus.
func (s *cccCRDStatus) UpdateResourceInfo(info crd.ResourceInfo) {
	cccInfo := toCccResourceInfo(info)
	for i, existing := range s.apiStatus.ResourceInfo {
		if existing.Name != nil && cccInfo.Name != nil && *existing.Name == *cccInfo.Name {
			s.apiStatus.ResourceInfo[i] = cccInfo
			return
		}
	}
	s.apiStatus.ResourceInfo = append(s.apiStatus.ResourceInfo, cccInfo)
}

// UpdateRuleConditions implements crd.CRDStatus.
func (s *cccCRDStatus) UpdateRuleConditions(ruleIdx string, conditions []metav1.Condition) {
	idx := s.getOrCreatePriorityStatusIdx(ruleIdx)
	if conditions == nil {
		conditions = []metav1.Condition{}
	}
	s.apiStatus.PriorityStatuses[idx].Conditions = conditions
}

// UpdateRuleResourceInfo implements crd.CRDStatus.
func (s *cccCRDStatus) UpdateRuleResourceInfo(ruleIdx string, info crd.ResourceInfo) {
	idx := s.getOrCreatePriorityStatusIdx(ruleIdx)
	cccInfo := toCccResourceInfo(info)
	for i, existing := range s.apiStatus.PriorityStatuses[idx].ResourceInfo {
		if existing.Name != nil && cccInfo.Name != nil && *existing.Name == *cccInfo.Name {
			s.apiStatus.PriorityStatuses[idx].ResourceInfo[i] = cccInfo
			return
		}
	}
	s.apiStatus.PriorityStatuses[idx].ResourceInfo = append(s.apiStatus.PriorityStatuses[idx].ResourceInfo, cccInfo)
}

// UpdateRuleScalingHistory implements crd.CRDStatus.
func (s *cccCRDStatus) UpdateRuleScalingHistory(ruleIdx string, history crd.ScalingEventsHistory) {
	idx := s.getOrCreatePriorityStatusIdx(ruleIdx)
	h := toCccScalingEventsHistory(history)
	s.apiStatus.PriorityStatuses[idx].ScalingEventsHistory = h
}

// ResetAllScalingHistories implements crd.CRDStatus.
func (s *cccCRDStatus) ResetAllScalingHistories() {
	for i := range s.apiStatus.PriorityStatuses {
		s.apiStatus.PriorityStatuses[i].ScalingEventsHistory = nil
	}
}

// ResetAllResourceInfo implements crd.CRDStatus.
func (s *cccCRDStatus) ResetAllResourceInfo() {
	for i := range s.apiStatus.PriorityStatuses {
		s.apiStatus.PriorityStatuses[i].ResourceInfo = []ccc_api.ResourceInfo{}
	}
}

// GetConditions implements crd.CRDStatus.
func (s *cccCRDStatus) GetConditions() []metav1.Condition {
	return s.apiStatus.Conditions
}

// GetRuleConditions implements crd.CRDStatus.
func (s *cccCRDStatus) GetRuleConditions(ruleIdx string) []metav1.Condition {
	for _, status := range s.apiStatus.PriorityStatuses {
		if status.Identifier == ruleIdx {
			return status.Conditions
		}
	}
	return nil
}

// GetRuleScalingHistory implements crd.CRDStatus.
func (s *cccCRDStatus) GetRuleScalingHistory(ruleIdx string) *crd.ScalingEventsHistory {
	idx := s.getOrCreatePriorityStatusIdx(ruleIdx)
	if s.apiStatus.PriorityStatuses[idx].ScalingEventsHistory == nil {
		return nil
	}
	h := s.apiStatus.PriorityStatuses[idx].ScalingEventsHistory
	return &crd.ScalingEventsHistory{
		ConsolidatedNodesCount: fromIntPointer(h.ConsolidatedNodesCount),
		ProvisionedNodesCount:  fromIntPointer(h.ProvisionedNodesCount),
		MigratedNodesCount:     fromIntPointer(h.MigratedNodesCount),
		MeasuredAt:             fromTimePointer(h.MeasuredAt),
		MeasuredSince:          fromTimePointer(h.MeasuredSince),
	}
}

// GetCRDStatusPatch implements crd.CRDStatus.
func (s *cccCRDStatus) GetCRDStatusPatch() client.Object {
	return &ccc_api.ComputeClass{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ComputeClass",
			APIVersion: "cloud.google.com/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: s.cccName,
		},
		Status: s.apiStatus,
	}
}

func (s *cccCRDStatus) getOrCreatePriorityStatusIdx(ruleIdx string) int {
	for i, status := range s.apiStatus.PriorityStatuses {
		if status.Identifier == ruleIdx {
			return i
		}
	}

	newStatus := ccc_api.PriorityStatus{
		Identifier:   ruleIdx,
		Conditions:   []metav1.Condition{},
		ResourceInfo: []ccc_api.ResourceInfo{},
	}
	s.apiStatus.PriorityStatuses = append(s.apiStatus.PriorityStatuses, newStatus)

	sort.Slice(s.apiStatus.PriorityStatuses, func(i, j int) bool {
		id1 := s.apiStatus.PriorityStatuses[i].Identifier
		id2 := s.apiStatus.PriorityStatuses[j].Identifier

		if id1 == "ScaleUpAnyway" {
			return false
		}
		if id2 == "ScaleUpAnyway" {
			return true
		}

		n1, err1 := strconv.Atoi(id1)
		n2, err2 := strconv.Atoi(id2)

		if err1 == nil && err2 == nil {
			return n1 < n2
		}
		return id1 < id2
	})

	for i, status := range s.apiStatus.PriorityStatuses {
		if status.Identifier == ruleIdx {
			return i
		}
	}
	return -1
}

func toCccResourceInfo(info crd.ResourceInfo) ccc_api.ResourceInfo {
	name := ccc_api.ResourceName(info.Name)
	unit := ccc_api.ResourceUnit(info.Unit)
	return ccc_api.ResourceInfo{
		Name:                         &name,
		Unit:                         &unit,
		TargetCount:                  &info.TargetCount,
		CurrentCount:                 &info.CurrentCount,
		CurrentUtilizationPercentage: &info.CurrentUtilizationPercentage,
		MeasuredAt:                   &info.MeasuredAt,
	}
}

func toCccScalingEventsHistory(history crd.ScalingEventsHistory) *ccc_api.ScalingEventsHistory {
	return &ccc_api.ScalingEventsHistory{
		ConsolidatedNodesCount: &history.ConsolidatedNodesCount,
		ProvisionedNodesCount:  &history.ProvisionedNodesCount,
		MigratedNodesCount:     &history.MigratedNodesCount,
		MeasuredAt:             &history.MeasuredAt,
		MeasuredSince:          &history.MeasuredSince,
	}
}

func fromIntPointer(i *int) int {
	if i == nil {
		return 0
	}
	return *i
}

func fromTimePointer(t *metav1.Time) metav1.Time {
	if t == nil {
		return metav1.Time{}
	}
	return *t
}
