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

type MetricLabels struct {
	zone        string
	machineType string
	gpuType     string
}

func ReservationMetricLabels(reservation *gce_api.Reservation) MetricLabels {
	instanceProperties := reservation.SpecificReservation.InstanceProperties
	gpuType := "NONE"
	if len(instanceProperties.GuestAccelerators) > 0 {
		gpuType = instanceProperties.GuestAccelerators[0].AcceleratorType
	}

	return MetricLabels{
		zone:        GetReservationZone(reservation),
		machineType: instanceProperties.MachineType,
		gpuType:     gpuType,
	}
}

func (l MetricLabels) ToMap() map[string]string {
	return map[string]string{
		"zone":         l.zone,
		"machine_type": l.machineType,
		"gpu_type":     l.gpuType,
	}
}
