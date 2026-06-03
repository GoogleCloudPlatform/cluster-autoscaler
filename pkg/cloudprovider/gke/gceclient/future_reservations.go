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

package gceclient

import (
	"fmt"
	"time"

	gce_api_beta "google.golang.org/api/compute/v0.beta"
)

// GceFutureReservation is a GKE cluster autoscaler domain object abstracting GCE future reserevations.
// Only data used in cluster autoscaler is defined and populated from GCE API.
type GceFutureReservation struct {
	Id                uint64
	Name              string
	PlanningStatus    PlanningStatusEnum
	ProcurementStatus ProcurementStatusEnum
	StartTime         time.Time
}

type PlanningStatusEnum int

const (
	PlanningStatusUnknown PlanningStatusEnum = iota // special value when decoding from GCE failed
	PlanningStatusDraft
	PlanningStatusPlanningStatusUnspecified
	PlanningStatusSubmitted
)

var (
	planningStatusMapping = map[string]PlanningStatusEnum{
		"DRAFT":                       PlanningStatusDraft,
		"PLANNING_STATUS_UNSPECIFIED": PlanningStatusPlanningStatusUnspecified,
		"SUBMITTED":                   PlanningStatusSubmitted,
	}
)

type ProcurementStatusEnum int

const (
	ProcurementStatusUnknown ProcurementStatusEnum = iota // special value when decoding from GCE failed
	ProcurementStatusApproved
	ProcurementStatusCancelled
	ProcurementStatusCommitted
	ProcurementStatusDeclined
	ProcurementStatusDrafting
	ProcurementStatusFailed
	ProcurementStatusFailedPartiallyFulfilled
	ProcurementStatusFulfilled
	ProcurementStatusPendingAmendmentApproval
	ProcurementStatusPendingApproval
	ProcurementStatusProcurementStatusUnspecified
	ProcurementStatusProcuring
	ProcurementStatusProvisioning
)

var (
	procurementStatusMapping = map[string]ProcurementStatusEnum{
		"APPROVED":                       ProcurementStatusApproved,
		"CANCELLED":                      ProcurementStatusCancelled,
		"COMMITTED":                      ProcurementStatusCommitted,
		"DECLINED":                       ProcurementStatusDeclined,
		"DRAFTING":                       ProcurementStatusDrafting,
		"FAILED":                         ProcurementStatusFailed,
		"FAILED_PARTIALLY_FULFILLED":     ProcurementStatusFailedPartiallyFulfilled,
		"FULFILLED":                      ProcurementStatusFulfilled,
		"PENDING_AMENDMENT_APPROVAL":     ProcurementStatusPendingAmendmentApproval,
		"PENDING_APPROVAL":               ProcurementStatusPendingApproval,
		"PROCUREMENT_STATUS_UNSPECIFIED": ProcurementStatusProcurementStatusUnspecified,
		"PROCURING":                      ProcurementStatusProcuring,
		"PROVISIONING":                   ProcurementStatusProvisioning,
	}
)

func toGceFutureReservation(item *gce_api_beta.FutureReservation) (*GceFutureReservation, error) {
	if item == nil {
		return nil, fmt.Errorf("GCE future reservation is nil")
	}
	planningStatus := toEnumWithDefault(planningStatusMapping, item.PlanningStatus, PlanningStatusUnknown)

	if item.Status == nil {
		return nil, fmt.Errorf("GCE future reservation Status is nil")
	}
	procurementStatus :=
		toEnumWithDefault(procurementStatusMapping, item.Status.ProcurementStatus, ProcurementStatusUnknown)

	if item.TimeWindow == nil {
		return nil, fmt.Errorf("GCE Future Reservation TimeWindow is nil")
	}
	startTime, err := time.Parse(time.RFC3339, item.TimeWindow.StartTime)
	if err != nil {
		return nil, fmt.Errorf(
			"TimeWindow.StartTime [%s] not parsable to RFC3339 timestamp: %v", item.TimeWindow.StartTime, err)
	}

	return &GceFutureReservation{
		Id:                item.Id,
		Name:              item.Name,
		PlanningStatus:    planningStatus,
		ProcurementStatus: procurementStatus,
		StartTime:         startTime,
	}, nil
}

func toEnumWithDefault[Enum any](mapping map[string]Enum, str string, def Enum) Enum {
	val, ok := mapping[str]
	if !ok {
		return def
	}
	return val
}
