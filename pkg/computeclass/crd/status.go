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

package crd

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CRDStatus is an interface that allows to interact with the status of a CRD.
type CRDStatus interface {
	// UpdateConditions updates the conditions of the CRD.
	UpdateConditions(conditions []metav1.Condition)
	// UpdateResourceInfo updates the resource info of the CRD.
	UpdateResourceInfo(info ResourceInfo)
	// UpdateRuleConditions updates the conditions of a rule.
	UpdateRuleConditions(ruleIdx string, conditions []metav1.Condition)
	// UpdateRuleResourceInfo updates the resource info of a rule.
	UpdateRuleResourceInfo(ruleIdx string, info ResourceInfo)
	// UpdateRuleScalingHistory updates the scaling history of a rule.
	UpdateRuleScalingHistory(ruleIdx string, history ScalingEventsHistory)
	// UpdateRuleConfigHash updates the configuration hash of a rule.
	UpdateRuleConfigHash(ruleIdx string, hash string)
	// UpdateConfigDriftInfo updates the reported progress of migrating nodes
	// that no longer match the configuration of the CRD.
	UpdateConfigDriftInfo(info ConfigDriftInfo)
	// UpdateRuleConsolidationStatus updates the aggregated consolidation (scale-down) status of a rule.
	UpdateRuleConsolidationStatus(ruleIdx string, status ConsolidationStatus)
	// GetConditions returns the current conditions of the CRD.
	GetConditions() []metav1.Condition
	// GetRuleConditions returns the conditions of a rule.
	GetRuleConditions(ruleIdx string) []metav1.Condition
	// GetRuleScalingHistory returns the scaling history of a rule.
	GetRuleScalingHistory(ruleIdx string) *ScalingEventsHistory
	// ResetAllScalingHistories resets scaling histories across all rules.
	ResetAllScalingHistories()
	// ResetAllResourceInfo resets resource info across all rules.
	ResetAllResourceInfo()
	// ResetConfigDriftInfo removes the reported config drift migration
	// progress, so that nothing is reported for CRDs that are not migrating.
	ResetConfigDriftInfo()
	// GetCRDStatusPatch returns the object that can be used to patch the status of the CRD.
	GetCRDStatusPatch() client.Object
}

// ConfigDriftInfo describes how far the migration of nodes that no longer match
// the CRD configuration has got.
//
// The counts partition the nodes of the CRD: every node is counted exactly
// once, so their sum is the number of nodes belonging to the CRD at MeasuredAt.
type ConfigDriftInfo struct {
	// CurrentNodes is the number of nodes matching the current configuration.
	CurrentNodes int

	// DriftedNodes is the number of nodes that need to be replaced and whose
	// replacement has not started yet.
	DriftedNodes int

	// MigratingNodes is the number of nodes that are being replaced, including
	// those waiting for their replacement capacity to be provisioned.
	MigratingNodes int

	// BlockedNodes groups the nodes that need to be replaced but cannot be, by
	// the reason holding them back. Reasons blocking no node are not reported.
	BlockedNodes []BlockedNodesInfo

	// MeasuredAt represents the timestamp at which the data was gathered.
	MeasuredAt metav1.Time
}

// BlockedNodesInfo is the number of nodes that a single reason holds back.
type BlockedNodesInfo struct {
	// Reason identifies what prevents the nodes from being replaced.
	Reason string

	// Count is the number of nodes blocked by Reason. It is never zero.
	Count int
}

// ScalingEventsHistory represents the aggregated information about scaling events.
type ScalingEventsHistory struct {
	// ConsolidatedNodesCount represents how many nodes in this priority were consolidated.
	ConsolidatedNodesCount int

	// ProvisionedNodesCount represents how many nodes in this priority were added.
	ProvisionedNodesCount int

	// MigratedNodesCount represents how many nodes in this priority were removed as part of high priority migration.
	MigratedNodesCount int

	// MeasuredAt represents a timestamp at which the data was gathered.
	MeasuredAt metav1.Time

	// MeasuredSince represents a timestamp at which data started being collected.
	MeasuredSince metav1.Time
}

// ConsolidationStatus represents the aggregated consolidation (scale-down) state within a priority.
type ConsolidationStatus struct {
	// ActuationInProgress represents how many nodes in this priority are currently being removed.
	ActuationInProgress int

	// NotProcessed represents how many nodes in this priority the autoscaler reached no verdict
	// on during the last scale-down pass. It is derived as
	// total - ActuationInProgress - sum(BlockedNodes.Count), clamped at 0.
	NotProcessed int

	// BlockedNodes represents nodes that can't be removed, aggregated by reason.
	BlockedNodes []BlockedNodesByReason

	// MeasuredAt represents the timestamp at which the consolidation state was gathered.
	MeasuredAt metav1.Time
}

// BlockedNodesByReason represents the number of nodes that can't be removed for a given reason.
type BlockedNodesByReason struct {
	// Reason is the reason why the nodes can't be removed. It must be one of the
	// ConsolidationReason* constants below.
	Reason string
	// Count is the number of nodes blocked for this reason.
	Count int
}

// Consolidation blocked reasons reported on the ComputeClass status. Each one must be a member of
// the enum on ConsolidationBlockedNodesInfo.Reason in compute-class-api: the API server rejects the
// entire status patch on a single invalid value, taking the rest of the status down with it.
// TestConsolidationReasonsMatchAPIEnum guards the two lists against drift.
const (
	// ConsolidationReasonNodeNotReady - the node is not ready, and consolidation of unready nodes
	// is disabled or the node has not been unready for long enough.
	ConsolidationReasonNodeNotReady = "NodeNotReady"
	// ConsolidationReasonNodeConsolidationDisabled - the node opted out of removal, for example
	// with the "cluster-autoscaler.kubernetes.io/scale-down-disabled" annotation.
	ConsolidationReasonNodeConsolidationDisabled = "NodeConsolidationDisabled"
	// ConsolidationReasonAboveUtilizationThreshold - the node is too well utilized to be
	// considered unneeded.
	ConsolidationReasonAboveUtilizationThreshold = "AboveUtilizationThreshold"
	// ConsolidationReasonNotUnneededLongEnough - the node is unneeded but has not been unneeded
	// for long enough to be removed yet.
	ConsolidationReasonNotUnneededLongEnough = "NotUnneededLongEnough"
	// ConsolidationReasonMinCapacityReached - removing the node would take the priority below a
	// configured minimum, such as the minimum capacity required by the compute class or the node
	// pool minimum size. Nodes of a node pool at its minimum size are not evaluated any further,
	// so they may be kept by their utilization or their pods as well.
	ConsolidationReasonMinCapacityReached = "MinCapacityReached"
	// ConsolidationReasonBlockingPods - a pod running on the node prevents it from being drained.
	ConsolidationReasonBlockingPods = "BlockingPods"
	// ConsolidationReasonPodDisruptionBudget - draining the node would violate a
	// PodDisruptionBudget.
	ConsolidationReasonPodDisruptionBudget = "PodDisruptionBudget"
	// ConsolidationReasonNoPlaceToMovePods - the pods running on the node are evictable, but
	// there is nowhere else in the cluster to reschedule them.
	ConsolidationReasonNoPlaceToMovePods = "NoPlaceToMovePods"
	// ConsolidationReasonUsedByFormedSlice - the node belongs to a multi-host accelerator slice
	// that is currently formed, so it cannot be removed independently of that slice.
	ConsolidationReasonUsedByFormedSlice = "UsedByFormedSlice"
	// ConsolidationReasonAtomicGroupBlocked - the node belongs to an atomically scaled group in
	// which another node is blocked, so the whole group must be kept.
	ConsolidationReasonAtomicGroupBlocked = "AtomicGroupBlocked"
	// ConsolidationReasonRecentConsolidationFailure - a recent attempt to remove the node failed
	// and it is backed off.
	ConsolidationReasonRecentConsolidationFailure = "RecentConsolidationFailure"
	// ConsolidationReasonConsolidationBlocked - catch-all for nodes that cannot be removed for a
	// reason that is not reported more specifically.
	ConsolidationReasonConsolidationBlocked = "ConsolidationBlocked"
)

// ConsolidationReasons is the closed set of reasons that may be reported in
// BlockedNodesByReason.Reason.
var ConsolidationReasons = []string{
	ConsolidationReasonNodeNotReady,
	ConsolidationReasonNodeConsolidationDisabled,
	ConsolidationReasonAboveUtilizationThreshold,
	ConsolidationReasonNotUnneededLongEnough,
	ConsolidationReasonMinCapacityReached,
	ConsolidationReasonBlockingPods,
	ConsolidationReasonPodDisruptionBudget,
	ConsolidationReasonNoPlaceToMovePods,
	ConsolidationReasonUsedByFormedSlice,
	ConsolidationReasonAtomicGroupBlocked,
	ConsolidationReasonRecentConsolidationFailure,
	ConsolidationReasonConsolidationBlocked,
}

// ResourceName represents the resource a given ResourceInfo applies to. Can be one of CPU, Memory, GPU or TPU.
type ResourceName string

// ResourceUnit specifies the unit used to measure a resource.
type ResourceUnit string

// ResourceInfo describes current usage of resources.
type ResourceInfo struct {
	// Name is the name of a given resource measured in this ResourceInfo.
	Name ResourceName

	// Unit is a unit a given resource was measured in.
	Unit ResourceUnit

	// TargetCount represents the target count of a given resource within a priority. Can be lower than current count if there is ongoing node consolidation or higher, if there is ongoing node provisioning event.
	TargetCount int

	// CurrentCount represents the current count of a given resource.
	CurrentCount int

	// CurrentUtilizationPercentage represents the percentage of utilization for the resource `Name` at the `MeasuredAt` timestamp.
	CurrentUtilizationPercentage int

	// MeasuredAt represents the timestamp at which the resource information was measured.
	MeasuredAt metav1.Time
}
