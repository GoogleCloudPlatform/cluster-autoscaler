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

package reservations

import (
	gce_api "google.golang.org/api/compute/v1"
)

func IsSpecificReservation(rsv *gce_api.Reservation) bool {
	return rsv.SpecificReservation != nil
}

func IsAggregateReservation(rsv *gce_api.Reservation) bool {
	return rsv.AggregateReservation != nil
}

func getTotalLocalSSDSize(ip *gce_api.AllocationSpecificSKUAllocationReservedInstanceProperties, targetInterface string) int64 {
	var totalLocalSSDSize int64
	for _, ssd := range ip.LocalSsds {
		if ssd.Interface == targetInterface {
			totalLocalSSDSize += ssd.DiskSizeGb
		}
	}
	return totalLocalSSDSize
}
