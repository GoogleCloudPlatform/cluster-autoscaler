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

package conditions

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	gceapiv1 "google.golang.org/api/compute/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/gce/localssdsize"
)

// reservationConfigChecker checks for misconfigured reservations in a rule.
type reservationConfigChecker struct {
	rsvCache                ReservationProvider
	localSsdProvider        localssdsize.LocalSSDSizeProvider
	reservationBlocksPuller *reservations.BlocksPuller
	cloudProvider           CloudProvider
}

func (ch *reservationConfigChecker) checkRule(rule rules.Rule) *metav1.Condition {
	if len(rule.Reservations()) == 0 {
		return nil
	}

	for _, reservation := range rule.Reservations() {
		// Only specific reservations are supported for validation
		if !reservation.IsSpecificAffinity() {
			continue
		}

		gceReservations := ch.rsvCache.GetReservations(reservation.Name(), reservation.Project())
		if len(gceReservations) == 0 {
			return ReservationNotFoundCondition(reservation.Name(), reservation.Project())
		}

		var errMsgs []string

		if len(reservation.Zones()) > 0 {
			// Reservation zones specified - sharding not allowed: all reservations
			// must be compatible with each other and with the CCC rule.
			errMsgs = ch.validateReservationsInSpecificZones(reservation, gceReservations, rule)
		} else {
			// No reservation zones specified - sharding allowed: reservations don't need to be
			// compatible with each other, but ALL of them must be compatible with the CCC rule.
			for _, gceReservation := range gceReservations {
				err := ch.validateAgainstRule(gceReservation, reservation, rule)

				if err != nil {
					errMsgs = append(errMsgs, fmt.Sprintf("in zone %s: %s", gceclient.GetReservationZone(gceReservation), err.Error()))
				}
			}
		}

		if len(errMsgs) > 0 {
			sort.Strings(errMsgs)
			return ReservationUnusableWithReasonCondition(reservation.Name(), reservation.Project(), strings.Join(errMsgs, "; "))
		}
	}

	return nil
}

func (ch *reservationConfigChecker) validateReservationsInSpecificZones(reservation rules.Reservation, gceReservations []*gceapiv1.Reservation, rule rules.Rule) []string {
	var errMsgs []string
	var firstRsv *gceapiv1.Reservation
	var firstZone string

	for _, reqZone := range reservation.Zones() {
		var gceReservation *gceapiv1.Reservation
		for _, r := range gceReservations {
			if gceclient.GetReservationZone(r) == reqZone {
				gceReservation = r
				break
			}
		}

		if gceReservation == nil {
			errMsgs = append(errMsgs, fmt.Sprintf("in zone %s: reservation is missing", reqZone))
			continue
		}

		err := ch.validateAgainstRule(gceReservation, reservation, rule)

		if err != nil {
			errMsgs = append(errMsgs, fmt.Sprintf("in zone %s: %s", reqZone, err.Error()))
			continue
		}

		// Check compatibility with other reservations in the specified zones
		if firstRsv == nil {
			firstRsv = gceReservation
			firstZone = reqZone
		} else {
			if !areReservationsCompatible(firstRsv, gceReservation) {
				errMsgs = append(errMsgs, fmt.Sprintf("in zone %s: incompatible with reservation in zone %s", reqZone, firstZone))
			}
		}
	}
	return errMsgs
}

func (ch *reservationConfigChecker) validateAgainstRule(gceReservation *gceapiv1.Reservation, reservation rules.Reservation, rule rules.Rule) error {
	var err error

	if !gceReservation.SpecificReservationRequired {
		return errors.New("any affinity reservation cannot be consumed")
	}
	if reservations.IsSpecificReservation(gceReservation) {
		if rule.HasTpu() {
			return errors.New("tpu requested for non aggregate reservation")
		}
		err = matchSpecificReservationOrError(ch.cloudProvider, gceReservation, rule, ch.localSsdProvider)
		if err == nil && reservation.BlockName() != "" && ch.reservationBlocksPuller != nil {
			if cond := ch.validateReservationBlock(reservation, gceReservation); cond != nil {
				return errors.New(cond.Message)
			}
		}

	} else if reservations.IsAggregateReservation(gceReservation) {
		err = matchAggregateReservationOrError(ch.cloudProvider, gceReservation, rule)
	} else {
		err = errors.New("unsupported reservation type")
	}

	return err
}

func (ch *reservationConfigChecker) conditionType() string {
	return RuleMisconfiguredCondition
}

func (ch *reservationConfigChecker) validateReservationBlock(reservation rules.Reservation, gceReservation *gceapiv1.Reservation) *metav1.Condition {
	prj := reservation.Project()
	if prj == "" {
		// The project can be unspecified in CCC reservation config
		prj = gceclient.GetReservationProject(gceReservation)
	}
	blocks := ch.reservationBlocksPuller.GetReservationBlocksInReservation(gceclient.GetReservationRefFromReservation(*gceReservation))
	if !matchReservationBlock(reservation.BlockName(), blocks) {
		return ReservationBlockUnusableCondition(reservation.Name(), prj, reservation.BlockName())
	}
	return nil
}

func areReservationsCompatible(r1, r2 *gceapiv1.Reservation) bool {
	if r1.SpecificReservationRequired != r2.SpecificReservationRequired {
		return false
	}
	if reservations.IsSpecificReservation(r1) != reservations.IsSpecificReservation(r2) {
		return false
	}
	if reservations.IsAggregateReservation(r1) != reservations.IsAggregateReservation(r2) {
		return false
	}

	if len(r1.ResourcePolicies) != len(r2.ResourcePolicies) {
		return false
	}
	for k, v1 := range r1.ResourcePolicies {
		if v2, ok := r2.ResourcePolicies[k]; !ok || v1 != v2 {
			return false
		}
	}

	if reservations.IsSpecificReservation(r1) {
		p1 := r1.SpecificReservation.InstanceProperties
		p2 := r2.SpecificReservation.InstanceProperties
		if p1 == nil || p2 == nil {
			return false
		}
		if p1.MachineType != p2.MachineType {
			return false
		}
		if p1.MinCpuPlatform != p2.MinCpuPlatform {
			return false
		}

		if len(p1.LocalSsds) != len(p2.LocalSsds) {
			return false
		}
		for i := range p1.LocalSsds {
			if p1.LocalSsds[i].Interface != p2.LocalSsds[i].Interface || p1.LocalSsds[i].DiskSizeGb != p2.LocalSsds[i].DiskSizeGb {
				return false
			}
		}

		if len(p1.GuestAccelerators) != len(p2.GuestAccelerators) {
			return false
		}
		for i := range p1.GuestAccelerators {
			if p1.GuestAccelerators[i].AcceleratorType != p2.GuestAccelerators[i].AcceleratorType || p1.GuestAccelerators[i].AcceleratorCount != p2.GuestAccelerators[i].AcceleratorCount {
				return false
			}
		}
	}
	return true
}
