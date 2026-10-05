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
	gce_api "google.golang.org/api/compute/v1"
)

// ReservationComment describes why a reservation cannot be used.
type ReservationComment string

const (
	// ReservationNotReady indicates that the reservation is not in READY status.
	ReservationNotReady ReservationComment = "reservation is not ready"
	// ReservationNeitherAggregateNorSpecific indicates that the reservation has neither aggregate nor specific configuration.
	ReservationNeitherAggregateNorSpecific ReservationComment = "reservation is neither aggregate nor specific"
	// SpecificReservationNoInstanceProperties indicates that the specific reservation has no instance properties.
	SpecificReservationNoInstanceProperties ReservationComment = "specific reservation has no instance properties"
	// AggregateReservationNotAllowed indicates that aggregate reservations are not allowed in this context.
	AggregateReservationNotAllowed ReservationComment = "aggregate reservation is not allowed"
)

// IsReservationUsable checks whether the reservation can be used. If not, it
// also returns a comment describing the reason.
func IsReservationUsable(rsv *gce_api.Reservation, allowAggregate bool) (bool, ReservationComment) {
	if rsv.Status != "READY" {
		return false, ReservationNotReady
	}

	if rsv.AggregateReservation == nil && rsv.SpecificReservation == nil {
		return false, ReservationNeitherAggregateNorSpecific
	}

	if rsv.SpecificReservation != nil && rsv.SpecificReservation.InstanceProperties == nil {
		return false, SpecificReservationNoInstanceProperties
	}

	if rsv.AggregateReservation != nil && !allowAggregate {
		return false, AggregateReservationNotAllowed
	}

	return true, ""
}
