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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/reservation"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
)

func IsSpecificReservation(rsv *reservation.Reservation) bool {
	return rsv.SpecificReservation != nil
}

func IsAggregateReservation(rsv *reservation.Reservation) bool {
	return rsv.AggregateReservation != nil
}

// IsReservationAffinitySpecified reports whether nodeGroup specifies a reservation affinity
// other than ReservationAffinityNone.
func IsReservationAffinitySpecified(nodeGroup gke.NodeGroup) bool {
	if nodeGroup == nil {
		return false
	}
	spec := nodeGroup.Spec()
	return spec != nil &&
		spec.ReservationAffinity != nil &&
		spec.ReservationAffinity.ConsumeReservationType != gkeclient.ReservationAffinityNone
}

func getTotalLocalSSDSize(ip *reservation.InstanceProperties, targetInterface string) int64 {
	var totalLocalSSDSize int64
	for _, ssd := range ip.LocalSSDs {
		if ssd.Interface == targetInterface {
			totalLocalSSDSize += ssd.DiskSizeGB
		}
	}
	return totalLocalSSDSize
}
