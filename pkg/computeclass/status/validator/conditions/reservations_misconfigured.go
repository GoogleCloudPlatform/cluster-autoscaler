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
	gceapiv1 "google.golang.org/api/compute/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
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

		gceReservation := ch.rsvCache.GetReservation(reservation.Name(), reservation.Project())
		if gceReservation == nil {
			return ReservationNotFoundCondition(reservation.Name(), reservation.Project())
		}

		if !gceReservation.SpecificReservationRequired {
			return ReservationUnusableWithReasonCondition(reservation.Name(), reservation.Project(), "any affinity reservation cannot be consumed")
		}

		if reservations.IsSpecificReservation(gceReservation) {
			if rule.HasTpu() {
				// Unable to use TPU with specific reservations
				return ReservationUnusableWithReasonCondition(reservation.Name(), reservation.Project(), "tpu requested for non aggregate reservation")
			}

			if err := matchSpecificReservationOrError(ch.cloudProvider, gceReservation, rule, ch.localSsdProvider); err != nil {
				return ReservationUnusableWithReasonCondition(reservation.Name(), reservation.Project(), err.Error())
			}
			// Validate block if requested and puller enabled
			if reservation.BlockName() != "" && ch.reservationBlocksPuller != nil {
				return ch.validateReservationBlock(reservation, gceReservation)
			}
		} else if reservations.IsAggregateReservation(gceReservation) {
			if err := matchAggregateReservationOrError(ch.cloudProvider, gceReservation, rule); err != nil {
				return ReservationUnusableWithReasonCondition(reservation.Name(), reservation.Project(), err.Error())
			}
		}
	}

	return nil
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
