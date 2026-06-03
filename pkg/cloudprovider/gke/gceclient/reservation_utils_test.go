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
		isUsable       bool
	}{
		{
			name: "Status not READY",
			rsv: &gce_api.Reservation{
				Status: "CREATING",
			},
			isUsable: false,
		},
		{
			name:     "Both specific and aggregate are nil",
			rsv:      &gce_api.Reservation{Status: "READY"},
			isUsable: false,
		},
		{
			name: "Specific reservation is nil, aggregate not allowed",
			rsv: &gce_api.Reservation{
				Status:               "READY",
				AggregateReservation: &gce_api.AllocationAggregateReservation{},
			},
			allowAggregate: false,
			isUsable:       false,
		},
		{
			name: "Specific reservation is nil, aggregate allowed",
			rsv: &gce_api.Reservation{
				Status:               "READY",
				AggregateReservation: &gce_api.AllocationAggregateReservation{},
			},
			allowAggregate: true,
			isUsable:       true,
		},
		{
			name: "Specific reservation with nil instance properties",
			rsv: &gce_api.Reservation{
				Status:              "READY",
				SpecificReservation: &gce_api.AllocationSpecificSKUReservation{},
			},
			isUsable: false,
		},
		{
			name: "Specific reservation with non-nil instance properties",
			rsv: &gce_api.Reservation{
				Status: "READY",
				SpecificReservation: &gce_api.AllocationSpecificSKUReservation{
					InstanceProperties: &gce_api.AllocationSpecificSKUAllocationReservedInstanceProperties{},
				},
			},
			isUsable: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isUsable := IsReservationUsable(tc.rsv, tc.allowAggregate)
			if isUsable != tc.isUsable {
				t.Errorf("IsReservationUsable() = %v, want %v", isUsable, tc.isUsable)
			}
		})
	}
}
