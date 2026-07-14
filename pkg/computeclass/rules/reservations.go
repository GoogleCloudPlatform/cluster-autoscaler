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

package rules

import (
	"slices"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"k8s.io/klog/v2"
)

// Reservation defines a single reservation to be used by the provisioned node.
type Reservation struct {
	affinity     string
	project      string
	zones        []string // used to populate ngRequirements.specifiedZones in NAP
	name         string
	blockName    string
	subBlockName string
	path         string
}

func (r *Reservation) Affinity() string {
	return r.affinity
}

func (r *Reservation) IsSpecificAffinity() bool {
	return r.affinity == reservations.SpecificAffinity
}

func (r *Reservation) IsAnyAffinity() bool {
	return r.affinity == reservations.AnyAffinity
}

func (r *Reservation) Project() string {
	return r.project
}

func (r *Reservation) Zones() []string {
	return r.zones
}

func (r *Reservation) Name() string {
	return r.name
}

func (r *Reservation) BlockName() string {
	return r.blockName
}

func (r *Reservation) SubBlockName() string {
	return r.subBlockName
}

func (r *Reservation) Path() string {
	return r.path
}

func NewReservation() *Reservation {
	return &Reservation{}
}

func (r *Reservation) WithReservationName(name string) *Reservation {
	r.name = name
	return r
}

func (r *Reservation) WithReservationAffinity(affinity string) *Reservation {
	r.affinity = affinity
	return r
}

func (r *Reservation) WithReservationProject(project string) *Reservation {
	r.project = project
	return r
}

func (r *Reservation) WithReservationZones(zones []string) *Reservation {
	r.zones = zones
	return r
}

func (r *Reservation) WithReservationPath(path string) *Reservation {
	r.path = path
	return r
}

func (r *Reservation) WithReservationBlock(blockName string) *Reservation {
	r.blockName = blockName
	return r
}

func (r *Reservation) WithReservationSubBlock(subBlockName string) *Reservation {
	r.subBlockName = subBlockName
	return r
}

// ReservationsRule is an interface for rules with reservations.
type ReservationsRule interface {
	BaseRule
	Reservations() []Reservation
}

type reservationsRule struct {
	reservations []Reservation
}

// Matches returns true if the nodegroup is within one of the nodepools.
func (r *reservationsRule) Matches(nodeGroup cloudprovider.NodeGroup) bool {
	mig, ok := nodeGroup.(gkeNodeGroup)
	if !ok {
		klog.Errorf("expected GkeMig; got %v", nodeGroup)
		return false
	}
	if len(r.reservations) == 0 {
		return true
	}

	for _, reservation := range r.reservations {
		if r.matchesReservation(mig, reservation) {
			return true
		}
	}

	return false
}

func (r *reservationsRule) matchesReservation(mig gkeNodeGroup, reservation Reservation) bool {
	if mig.Spec().ReservationAffinity == nil {
		return false
	}

	ruleAffinity, ok := reservations.GkeAffinityFromSelectorValue(reservation.affinity)
	if !ok {
		return false
	}

	affinity, err := reservations.NewNodepoolReservationAffinity(reservation.path, ruleAffinity)
	if err != nil {
		klog.Warningf("Failed to create node pool affinity for reservation %v: %v", reservation, err)
		return false
	}

	migAffinity := mig.Spec().ReservationAffinity

	if migAffinity.ConsumeReservationType != affinity.ConsumeReservationType {
		return false
	}

	if migAffinity.Key != affinity.Key {
		return false
	}

	if !slices.Equal(migAffinity.Values, affinity.Values) {
		return false
	}

	// Check that node pool does not contain zones outside of the rule.
	ruleZones := reservation.Zones()
	if len(ruleZones) != 0 {
		nodePoolLocationsMap := make(map[string]bool)
		for _, loc := range mig.Spec().Locations {
			nodePoolLocationsMap[loc] = true
		}
		ruleLocationsMap := make(map[string]bool)
		for _, loc := range ruleZones {
			ruleLocationsMap[loc] = true
		}

		for k := range nodePoolLocationsMap {
			if _, exists := ruleLocationsMap[k]; !exists {
				return false
			}
		}
	}

	return true
}

// Reservations returns reservations of rule.
func (r *reservationsRule) Reservations() []Reservation {
	return r.reservations
}

// WithReservationsRule returns RuleOption adding a single reservation to ReservationsRule.
func WithReservationsRule(reservation *Reservation) RuleOption {
	return func(r *rule) {
		if reservation != nil {
			r.reservationsRule.reservations = append(r.reservationsRule.reservations, *reservation)
		}
	}
}
