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
	"slices"
	"sort"
	"strconv"
	"time"

	ccc_api "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/observability"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var maxConditionAge = 5 * time.Minute

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
	if crd.AnyConditionsChanged(s.apiStatus.Conditions, conditions, &maxConditionAge) {
		s.apiStatus.Conditions = conditions
	}
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
	if crd.AnyConditionsChanged(s.apiStatus.PriorityStatuses[idx].Conditions, conditions, &maxConditionAge) {
		s.apiStatus.PriorityStatuses[idx].Conditions = conditions
	}
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

// UpdateRuleConfigHash implements crd.CRDStatus.
func (s *cccCRDStatus) UpdateRuleConfigHash(ruleIdx string, hash string) {
	idx := s.getOrCreatePriorityStatusIdx(ruleIdx)
	s.apiStatus.PriorityStatuses[idx].ConfigHash = hash
}

// UpdateRuleConsolidationStatus implements crd.CRDStatus.
func (s *cccCRDStatus) UpdateRuleConsolidationStatus(ruleIdx string, status crd.ConsolidationStatus) {
	idx := s.getOrCreatePriorityStatusIdx(ruleIdx)
	s.apiStatus.PriorityStatuses[idx].Consolidation = toCccConsolidationStatus(status)
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

// UpdateConfigDriftInfo implements crd.CRDStatus.
func (s *cccCRDStatus) UpdateConfigDriftInfo(info crd.ConfigDriftInfo) {
	// Only the configDrift field belongs to this report; anything else under
	// migration is left as it is, just as ResetConfigDriftInfo leaves it.
	if s.apiStatus.Migration == nil {
		s.apiStatus.Migration = &ccc_api.MigrationStatus{}
	}
	s.apiStatus.Migration.ConfigDrift = toCccConfigDriftStatus(info)
}

// ResetConfigDriftInfo implements crd.CRDStatus.
func (s *cccCRDStatus) ResetConfigDriftInfo() {
	if s.apiStatus.Migration == nil {
		return
	}
	s.apiStatus.Migration.ConfigDrift = nil
	if apiequality.Semantic.DeepEqual(*s.apiStatus.Migration, ccc_api.MigrationStatus{}) {
		// Nothing else is reported under migration, so drop the wrapper too
		// rather than leaving an empty object behind in the status.
		s.apiStatus.Migration = nil
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

func toCccConsolidationStatus(status crd.ConsolidationStatus) *ccc_api.ConsolidationStatus {
	actuationInProgress := status.ActuationInProgress
	notProcessed := status.NotProcessed
	return &ccc_api.ConsolidationStatus{
		ActuationInProgress: &actuationInProgress,
		NotProcessed:        &notProcessed,
		BlockedNodes:        toCccBlockedNodes(status.BlockedNodes),
		MeasuredAt:          &status.MeasuredAt,
	}
}

// toCccBlockedNodes converts the per-reason breakdown to the API type, preserving the order the
// producer chose (the order CA evaluates the reasons in). Reasons outside the API enum are folded
// into the catch-all: the API server rejects the whole status patch on a single invalid value, so
// writing one through would take the rest of the status down with it. Counts are merged per
// reason, because blockedNodes is a map-typed list keyed by reason and a folded entry may collide
// with an existing catch-all entry; the merged entry keeps the position of its first occurrence.
func toCccBlockedNodes(blocked []crd.BlockedNodesByReason) []ccc_api.ConsolidationBlockedNodesInfo {
	blockedNodes := make([]ccc_api.ConsolidationBlockedNodesInfo, 0, len(blocked))
	indexByReason := make(map[string]int, len(blocked))
	for _, b := range blocked {
		if b.Count <= 0 {
			continue
		}
		reason := b.Reason
		if !slices.Contains(crd.ConsolidationReasons, reason) {
			klog.Warningf("Unknown consolidation blocked reason %q (%d nodes), reporting it as %s", reason, b.Count, crd.ConsolidationReasonConsolidationBlocked)
			reason = crd.ConsolidationReasonConsolidationBlocked
		}
		if i, ok := indexByReason[reason]; ok {
			blockedNodes[i].Count += b.Count
			continue
		}
		indexByReason[reason] = len(blockedNodes)
		blockedNodes = append(blockedNodes, ccc_api.ConsolidationBlockedNodesInfo{Reason: reason, Count: b.Count})
	}
	return blockedNodes
}

// knownBlockReasons are the values the ComputeClass API accepts for
// BlockedNodesInfo.Reason.
//
// The API declares the field as a closed enum. Rather than keeping a third copy
// of the list here, the set is derived from the reasons the autoscaler can
// produce, and TestBlockReasonsAreAcceptedByTheApi asserts that those and the
// enum match exactly. The fallback below is therefore purely defensive.
var knownBlockReasons = func() map[string]bool {
	known := make(map[string]bool)
	for _, reason := range observability.AllReasons() {
		known[string(reason)] = true
	}
	return known
}()

// unknownBlockReasonFallback is where reasons the API does not know about are
// counted.
const unknownBlockReasonFallback = "MigrationBlocked"

func toCccConfigDriftStatus(info crd.ConfigDriftInfo) *ccc_api.ConfigDriftStatus {
	status := &ccc_api.ConfigDriftStatus{
		CurrentNodes:   &info.CurrentNodes,
		DriftedNodes:   &info.DriftedNodes,
		MigratingNodes: &info.MigratingNodes,
		MeasuredAt:     &info.MeasuredAt,
	}
	for _, blocked := range info.BlockedNodes {
		if blocked.Count <= 0 {
			// The API requires a count of at least one, and a reason blocking
			// nothing carries no information anyway.
			continue
		}
		reason := blocked.Reason
		if !knownBlockReasons[reason] {
			// The enum is closed, so an unrecognized reason would make the
			// apiserver reject the entire status patch and lose every other
			// counter with it. Counting those nodes under the catch-all keeps
			// the report valid and the sum intact.
			klog.Warningf("Reporting nodes blocked by unknown reason %q as %q", reason, unknownBlockReasonFallback)
			reason = unknownBlockReasonFallback
		}
		if i := indexOfBlockedNodes(status.BlockedNodes, reason); i >= 0 {
			// Folding an unknown reason into the catch-all can collide with an
			// entry that is already there. The list is keyed by reason, so the
			// counts have to be merged rather than appended.
			status.BlockedNodes[i].Count += blocked.Count
			continue
		}
		status.BlockedNodes = append(status.BlockedNodes, ccc_api.BlockedNodesInfo{
			Reason: reason,
			Count:  blocked.Count,
		})
	}
	return status
}

func indexOfBlockedNodes(blockedNodes []ccc_api.BlockedNodesInfo, reason string) int {
	for i, blocked := range blockedNodes {
		if blocked.Reason == reason {
			return i
		}
	}
	return -1
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
