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

package fake

import (
	"fmt"
	"path"
	"slices"
	"strings"

	gcev1 "google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/reservation"
)

const (
	consumeAnyReservation         = "ANY_RESERVATION"
	consumeAnyReservationThenFail = "ANY_RESERVATION_THEN_FAIL"
	consumeSpecificReservation    = "SPECIFIC_RESERVATION"
)

func cloneReservations(rsvs []*gcev1.Reservation) []*gcev1.Reservation {
	results := make([]*gcev1.Reservation, 0, len(rsvs))
	for _, rsv := range rsvs {
		results = append(results, reservation.CloneV1(rsv))
	}
	return results
}

func instanceKey(zone, migName, instanceName string) string {
	return fmt.Sprintf("%s/%s/%s", zone, migName, instanceName)
}

// capacityAssignment is a reservation slot or, if reservation is nil,
// on-demand capacity consumed by an instance.
type capacityAssignment struct {
	reservation *gcev1.Reservation
	onDemand    CapacityKey
}

func isCapacityTracked(template *gcev1.InstanceTemplate) bool {
	return template != nil && template.Properties != nil && template.Properties.MachineType != ""
}

// allocateCapacityAtomicallyLocked assigns capacity to all new instances of
// template, or to none of them. Matching reservation slots are used first,
// then on-demand capacity, if the reservation affinity allows it. Only the
// zone and machine type are matched.
func (g *GceClient) allocateCapacityAtomicallyLocked(project, zone, migName string, instanceNames []string, template *gcev1.InstanceTemplate) error {
	affinity := template.Properties.ReservationAffinity
	onDemand := CapacityKey{Zone: zone, MachineType: path.Base(template.Properties.MachineType)}
	var assignments []capacityAssignment
	for _, rsv := range g.matchingReservationsLocked(project, onDemand, affinity) {
		for free := freeReservationSlots(rsv); free > 0 && len(assignments) < len(instanceNames); free-- {
			assignments = append(assignments, capacityAssignment{reservation: rsv})
		}
	}
	if missing := int64(len(instanceNames) - len(assignments)); missing > 0 {
		if !allowsOnDemand(affinity) {
			return reservationCapacityExceededError(affinity)
		}
		if err := g.consumeOnDemandCapacityLocked(onDemand, missing); err != nil {
			return err
		}
		for range missing {
			assignments = append(assignments, capacityAssignment{onDemand: onDemand})
		}
	}
	for i, instanceName := range instanceNames {
		if rsv := assignments[i].reservation; rsv != nil {
			rsv.SpecificReservation.InUseCount++
		}
		g.instanceCapacity[instanceKey(zone, migName, instanceName)] = assignments[i]
	}
	return nil
}

func (g *GceClient) releaseCapacityLocked(zone, migName, instanceName string) {
	key := instanceKey(zone, migName, instanceName)
	assignment, found := g.instanceCapacity[key]
	if !found {
		return
	}
	delete(g.instanceCapacity, key)
	if rsv := assignment.reservation; rsv != nil {
		if rsv.SpecificReservation.InUseCount > 0 {
			rsv.SpecificReservation.InUseCount--
		}
		return
	}
	g.releaseOnDemandCapacityLocked(assignment.onDemand, 1)
}

// availableCapacityLocked returns how many new instances of template can get
// capacity, and whether that number is limited.
func (g *GceClient) availableCapacityLocked(project, zone string, template *gcev1.InstanceTemplate) (int64, bool) {
	affinity := template.Properties.ReservationAffinity
	onDemand := CapacityKey{Zone: zone, MachineType: path.Base(template.Properties.MachineType)}
	var available int64
	for _, rsv := range g.matchingReservationsLocked(project, onDemand, affinity) {
		available += freeReservationSlots(rsv)
	}
	if !allowsOnDemand(affinity) {
		return available, true
	}
	onDemandAvailable, limited := g.onDemandCapacity[onDemand]
	return available + onDemandAvailable, limited
}

func (g *GceClient) matchingReservationsLocked(project string, key CapacityKey, affinity *gcev1.ReservationAffinity) []*gcev1.Reservation {
	if affinity == nil {
		return nil
	}
	var result []*gcev1.Reservation
	for _, rsv := range slices.Concat(g.projectToReservations[project], g.unlistedReservations[project]) {
		if specificReservationMatches(rsv, key) && affinityMatches(affinity, rsv) {
			result = append(result, rsv)
		}
	}
	return result
}

func specificReservationMatches(rsv *gcev1.Reservation, key CapacityKey) bool {
	if rsv == nil || rsv.SpecificReservation == nil || rsv.SpecificReservation.InstanceProperties == nil {
		return false
	}
	return path.Base(rsv.Zone) == key.Zone && path.Base(rsv.SpecificReservation.InstanceProperties.MachineType) == key.MachineType
}

func freeReservationSlots(rsv *gcev1.Reservation) int64 {
	return max(0, rsv.SpecificReservation.Count-rsv.SpecificReservation.InUseCount)
}

func allowsOnDemand(affinity *gcev1.ReservationAffinity) bool {
	if affinity == nil {
		return true
	}
	switch affinity.ConsumeReservationType {
	case consumeAnyReservationThenFail, consumeSpecificReservation:
		return false
	}
	return true
}

func reservationCapacityExceededError(affinity *gcev1.ReservationAffinity) error {
	reservation := affinity.ConsumeReservationType
	if len(affinity.Values) > 0 {
		reservation = strings.Join(affinity.Values, ", ")
	}
	return &googleapi.Error{
		Code:    400,
		Message: fmt.Sprintf("Specified reservation %s does not have available resources for the request.", reservation),
	}
}

func affinityMatches(affinity *gcev1.ReservationAffinity, rsv *gcev1.Reservation) bool {
	switch affinity.ConsumeReservationType {
	case consumeAnyReservation, consumeAnyReservationThenFail:
		return !rsv.SpecificReservationRequired
	case consumeSpecificReservation:
		for _, value := range affinity.Values {
			if reservationNameFromAffinityValue(value) == rsv.Name {
				return true
			}
		}
	}
	return false
}

// reservationNameFromAffinityValue extracts the reservation name from an
// affinity value, which is a bare name, a block or sub-block path relative to
// the reservation like "{name}/reservationBlocks/...", or a full path like
// "projects/{project}/reservations/{name}[/reservationBlocks/...]".
// Capacity is tracked per reservation, so blocks and sub-blocks are ignored.
func reservationNameFromAffinityValue(value string) string {
	const marker = "reservations/"
	if idx := strings.Index(value, marker); idx >= 0 {
		value = value[idx+len(marker):]
	}
	name, _, _ := strings.Cut(value, "/")
	return name
}
