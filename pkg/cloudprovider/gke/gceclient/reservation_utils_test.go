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
	"testing"

	gce_api "google.golang.org/api/compute/v1"
)

func TestIsReservationUsable(t *testing.T) {
	for _, tc := range []struct {
		name           string
		rsv            *gce_api.Reservation
		allowAggregate bool
		wantUsable     bool
		wantReason     ReservationComment
	}{
		{
			name: "Status not READY",
			rsv: &gce_api.Reservation{
				Status: "CREATING",
			},
			wantUsable: false,
			wantReason: ReservationNotReady,
		},
		{
			name:       "Both specific and aggregate are nil",
			rsv:        &gce_api.Reservation{Status: "READY"},
			wantUsable: false,
			wantReason: ReservationNeitherAggregateNorSpecific,
		},
		{
			name: "Specific reservation is nil, aggregate not allowed",
			rsv: &gce_api.Reservation{
				Status:               "READY",
				AggregateReservation: &gce_api.AllocationAggregateReservation{},
			},
			allowAggregate: false,
			wantUsable:     false,
			wantReason:     AggregateReservationNotAllowed,
		},
		{
			name: "Specific reservation is nil, aggregate allowed",
			rsv: &gce_api.Reservation{
				Status:               "READY",
				AggregateReservation: &gce_api.AllocationAggregateReservation{},
			},
			allowAggregate: true,
			wantUsable:     true,
			wantReason:     "",
		},
		{
			name: "Specific reservation with nil instance properties",
			rsv: &gce_api.Reservation{
				Status:              "READY",
				SpecificReservation: &gce_api.AllocationSpecificSKUReservation{},
			},
			wantUsable: false,
			wantReason: SpecificReservationNoInstanceProperties,
		},
		{
			name: "Specific reservation with non-nil instance properties",
			rsv: &gce_api.Reservation{
				Status: "READY",
				SpecificReservation: &gce_api.AllocationSpecificSKUReservation{
					InstanceProperties: &gce_api.AllocationSpecificSKUAllocationReservedInstanceProperties{},
				},
			},
			wantUsable: true,
			wantReason: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isUsable, reason := IsReservationUsable(tc.rsv, tc.allowAggregate)
			if isUsable != tc.wantUsable {
				t.Errorf("IsReservationUsable() = %v, want %v", isUsable, tc.wantUsable)
			}
			if reason != tc.wantReason {
				t.Errorf("IsReservationUsable() reason = %q, want %q", reason, tc.wantReason)
			}
		})
	}
}
