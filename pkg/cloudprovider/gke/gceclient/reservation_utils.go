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

func IsReservationUsable(rsv *gce_api.Reservation, allowAggregate bool) bool {
	if rsv.Status != "READY" {
		return false
	}

	if rsv.AggregateReservation == nil && rsv.SpecificReservation == nil {
		return false
	}

	if rsv.SpecificReservation != nil && rsv.SpecificReservation.InstanceProperties == nil {
		return false
	}

	if rsv.AggregateReservation != nil && !allowAggregate {
		return false
	}

	return true
}
