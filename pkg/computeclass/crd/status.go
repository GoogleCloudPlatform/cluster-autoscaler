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
