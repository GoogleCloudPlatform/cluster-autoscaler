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
	"fmt"
	"testing"

	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
)

func TestReservationsRuleMatchesNodeGroup(t *testing.T) {
	nonDefaultMachineFamilyName := machinetypes.N2.Name()
	nonDefaultMachineType := fmt.Sprintf("%s-standard-8", nonDefaultMachineFamilyName)
	reservationBlockName := "res-block"
	reservationSubBlockName := "res-sub-block"

	specificLocalAffinityName := "reservation"
	specificLocalAffinityPath := gceclient.ReservationRef{Name: specificLocalAffinityName}.Path()
	specificLocalAffinity := &gke_api_beta.ReservationAffinity{
		ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
		Key:                    gkeclient.ReservationNameKey,
		Values:                 []string{specificLocalAffinityPath},
	}
	// Local affinity with reservation block specified
	specificLocalAffinityPathWithBlock := gceclient.ReservationRef{Name: specificLocalAffinityName, BlockName: reservationBlockName}.Path()
	specificLocalAffinityWithBlock := &gke_api_beta.ReservationAffinity{
		ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
		Key:                    gkeclient.ReservationNameKey,
		Values:                 []string{specificLocalAffinityPathWithBlock},
	}
	// Local affinity with reservation block and sub-block specified
	specificLocalAffinityPathWithSubBlock := gceclient.ReservationRef{Name: specificLocalAffinityName, BlockName: reservationBlockName, SubBlockName: reservationSubBlockName}.Path()
	specificLocalAffinityWithSubBlock := &gke_api_beta.ReservationAffinity{
		ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
		Key:                    gkeclient.ReservationNameKey,
		Values:                 []string{specificLocalAffinityPathWithSubBlock},
	}

	specificSharedAffinityProject := "other-project"
	specificSharedAffinityName := "reservation"
	specificSharedAffinityPath := gceclient.ReservationRef{Name: specificSharedAffinityName, Project: specificSharedAffinityProject}.Path()
	specificSharedAffinity := &gke_api_beta.ReservationAffinity{
		ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
		Key:                    gkeclient.ReservationNameKey,
		Values:                 []string{specificSharedAffinityPath},
	}
	// Shared affinity with reservation block specified
	specificSharedAffinityPathWithBlock := gceclient.ReservationRef{Name: specificSharedAffinityName, Project: specificSharedAffinityProject, BlockName: reservationBlockName}.Path()
	specificSharedAffinityWithBlock := &gke_api_beta.ReservationAffinity{
		ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
		Key:                    gkeclient.ReservationNameKey,
		Values:                 []string{specificSharedAffinityPathWithBlock},
	}
	// Shared affinity with reservation block and sub-block specified
	specificSharedAffinityPathWithSubBlock := gceclient.ReservationRef{Name: specificSharedAffinityName, Project: specificSharedAffinityProject, BlockName: reservationBlockName, SubBlockName: reservationSubBlockName}.Path()
	specificSharedAffinityWithSubBlock := &gke_api_beta.ReservationAffinity{
		ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
		Key:                    gkeclient.ReservationNameKey,
		Values:                 []string{specificSharedAffinityPathWithSubBlock},
	}

	anyAffinity := &gke_api_beta.ReservationAffinity{ConsumeReservationType: gkeclient.ReservationAffinityAny}

	testCases := []struct {
		name      string
		nodegroup cloudprovider.NodeGroup
		rule      ReservationsRule
		expected  bool
	}{
		{
			name:      "rule with specific local reservation, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificLocalAffinity}).Build(),
			rule: NewRule(WithReservationsRule(NewReservation().
				WithReservationName(specificLocalAffinityName).
				WithReservationAffinity(reservations.SpecificAffinity).
				WithReservationPath(specificLocalAffinityPath)),
			),
			expected: true,
		},
		{
			name:      "rule with specific local reservation with block, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificLocalAffinityWithBlock}).Build(),
			rule: NewRule(WithReservationsRule(NewReservation().
				WithReservationName(specificLocalAffinityName).
				WithReservationAffinity(reservations.SpecificAffinity).
				WithReservationPath(specificLocalAffinityPathWithBlock).
				WithReservationBlock(reservationBlockName))),
			expected: true,
		},
		{
			name:      "rule with specific local reservation with block and sub-block, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificLocalAffinityWithSubBlock}).Build(),
			rule: NewRule(WithReservationsRule(NewReservation().
				WithReservationName(specificLocalAffinityName).
				WithReservationAffinity(reservations.SpecificAffinity).
				WithReservationPath(specificLocalAffinityPathWithSubBlock).
				WithReservationBlock(reservationBlockName).
				WithReservationSubBlock(reservationSubBlockName))),
			expected: true,
		},
		{
			name:      "rule with specific shared reservation, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificSharedAffinity}).Build(),
			rule: NewRule(
				WithReservationsRule(NewReservation().
					WithReservationName(specificSharedAffinityName).
					WithReservationAffinity(reservations.SpecificAffinity).
					WithReservationProject(specificSharedAffinityProject).
					WithReservationPath(specificSharedAffinityPath))),
			expected: true,
		},
		{
			name:      "rule with specific shared reservation with block, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificSharedAffinityWithBlock}).Build(),
			rule: NewRule(
				WithReservationsRule(NewReservation().
					WithReservationName(specificSharedAffinityName).
					WithReservationAffinity(reservations.SpecificAffinity).
					WithReservationProject(specificSharedAffinityProject).
					WithReservationPath(specificSharedAffinityPathWithBlock).
					WithReservationBlock(reservationBlockName))),
			expected: true,
		},
		{
			name:      "rule with specific shared reservation with block and sub-block, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificSharedAffinityWithSubBlock}).Build(),
			rule: NewRule(
				WithReservationsRule(NewReservation().
					WithReservationName(specificSharedAffinityName).
					WithReservationAffinity(reservations.SpecificAffinity).
					WithReservationProject(specificSharedAffinityProject).
					WithReservationPath(specificSharedAffinityPathWithSubBlock).
					WithReservationBlock(reservationBlockName).
					WithReservationSubBlock(reservationSubBlockName))),
			expected: true,
		},
		{
			name:      "rule with any reservation, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: anyAffinity}).Build(),
			rule:      NewRule(WithReservationsRule(NewReservation().WithReservationAffinity(reservations.AnyAffinity))),
			expected:  true,
		},
		{
			name:      "rule with specific local reservation, mig with any reservation, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: anyAffinity}).Build(),
			rule: NewRule(
				WithReservationsRule(NewReservation().
					WithReservationName(specificLocalAffinityName).
					WithReservationAffinity(reservations.SpecificAffinity).
					WithReservationPath(specificLocalAffinityPath))),
			expected: false,
		},
		{
			name:      "rule with specific local reservation with block, mig with any reservation, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: anyAffinity}).Build(),
			rule: NewRule(WithReservationsRule(NewReservation().
				WithReservationName(specificLocalAffinityName).
				WithReservationAffinity(reservations.SpecificAffinity).
				WithReservationPath(specificLocalAffinityPathWithBlock).
				WithReservationBlock(reservationBlockName))),
			expected: false,
		},
		{
			name:      "rule with specific shared reservation, mig with any reservation, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: anyAffinity}).Build(),
			rule: NewRule(WithReservationsRule(NewReservation().
				WithReservationName(specificSharedAffinityName).
				WithReservationAffinity(reservations.SpecificAffinity).
				WithReservationProject(specificSharedAffinityProject).
				WithReservationPath(specificSharedAffinityPath))),
			expected: false,
		},
		{
			name:      "rule with specific shared reservation with block, mig with any reservation, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: anyAffinity}).Build(),
			rule: NewRule(WithReservationsRule(NewReservation().
				WithReservationName(specificSharedAffinityName).
				WithReservationAffinity(reservations.SpecificAffinity).
				WithReservationProject(specificSharedAffinityProject).
				WithReservationPath(specificSharedAffinityPathWithBlock).
				WithReservationBlock(reservationBlockName))),
			expected: false,
		},
		{
			name:      "rule with specific local reservation, mig with shared reservation, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificSharedAffinity}).Build(),
			rule: NewRule(WithReservationsRule(NewReservation().
				WithReservationName(specificLocalAffinityName).
				WithReservationAffinity(reservations.SpecificAffinity).
				WithReservationPath(specificLocalAffinityPath))),
			expected: false,
		},
		{
			name:      "rule with specific local reservation with block, mig with shared reservation, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificSharedAffinityWithBlock}).Build(),
			rule: NewRule(WithReservationsRule(NewReservation().
				WithReservationName(specificLocalAffinityName).
				WithReservationAffinity(reservations.SpecificAffinity).
				WithReservationPath(specificLocalAffinityPathWithBlock).
				WithReservationBlock(reservationBlockName))),
			expected: false,
		},
		{
			name:      "rule with specific shared reservation, mig with local reservation, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificLocalAffinity}).Build(),
			rule: NewRule(WithReservationsRule(NewReservation().
				WithReservationName(specificSharedAffinityName).
				WithReservationAffinity(reservations.SpecificAffinity).
				WithReservationProject(specificSharedAffinityProject).
				WithReservationPath(specificSharedAffinityPath))),
			expected: false,
		},
		{
			name:      "rule with specific shared reservation with block, mig with local reservation, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificLocalAffinityWithBlock}).Build(),
			rule: NewRule(WithReservationsRule(NewReservation().
				WithReservationName(specificSharedAffinityName).
				WithReservationAffinity(reservations.SpecificAffinity).
				WithReservationProject(specificSharedAffinityProject).
				WithReservationPath(specificSharedAffinityPathWithBlock).
				WithReservationBlock(reservationBlockName))),
			expected: false,
		},
		{
			name:      "rule with specific local reservation and specified zones, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificLocalAffinity, Locations: []string{"us-central1-a"}}).Build(),
			rule: NewRule(WithReservationsRule(NewReservation().
				WithReservationName(specificLocalAffinityName).
				WithReservationZones([]string{"us-central1-a"}).
				WithReservationAffinity(reservations.SpecificAffinity).
				WithReservationPath(specificLocalAffinityPath)),
			),
			expected: true,
		},
		{
			name:      "rule with specific local reservation and zones in different order, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificLocalAffinity, Locations: []string{"us-central1-a", "us-central1-b"}}).Build(),
			rule: NewRule(WithReservationsRule(NewReservation().
				WithReservationName(specificLocalAffinityName).
				WithReservationZones([]string{"us-central1-b", "us-central1-a"}).
				WithReservationAffinity(reservations.SpecificAffinity).
				WithReservationPath(specificLocalAffinityPath)),
			),
			expected: true,
		},
		{
			name:      "rule with specific local reservation and different zone, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificLocalAffinity, Locations: []string{"us-central1-a"}}).Build(),
			rule: NewRule(WithReservationsRule(NewReservation().
				WithReservationName(specificLocalAffinityName).
				WithReservationZones([]string{"us-central1-b"}).
				WithReservationAffinity(reservations.SpecificAffinity).
				WithReservationPath(specificLocalAffinityPath)),
			),
			expected: false,
		},
		{
			name:      "rule with specific local reservation and superset of nodepool zones, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificLocalAffinity, Locations: []string{"us-central1-a"}}).Build(),
			rule: NewRule(WithReservationsRule(NewReservation().
				WithReservationName(specificLocalAffinityName).
				WithReservationZones([]string{"us-central1-a", "us-central1-b"}).
				WithReservationAffinity(reservations.SpecificAffinity).
				WithReservationPath(specificLocalAffinityPath)),
			),
			expected: true,
		},
		{
			name:      "rule with specific local reservation and subset of nodepool zones, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificLocalAffinity, Locations: []string{"us-central1-a", "us-central1-b"}}).Build(),
			rule: NewRule(WithReservationsRule(NewReservation().
				WithReservationName(specificLocalAffinityName).
				WithReservationZones([]string{"us-central1-a"}).
				WithReservationAffinity(reservations.SpecificAffinity).
				WithReservationPath(specificLocalAffinityPath)),
			),
			expected: false,
		},
		{
			name:      "rule with specific local reservation, no zone preference, but mig has locations, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificLocalAffinity, Locations: []string{"us-central1-a"}}).Build(),
			rule: NewRule(WithReservationsRule(NewReservation().
				WithReservationName(specificLocalAffinityName).
				WithReservationAffinity(reservations.SpecificAffinity).
				WithReservationPath(specificLocalAffinityPath)),
			),
			expected: true,
		},
		{
			name:      "rule with any reservation, mig with local reservation, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificLocalAffinity}).Build(),
			rule:      NewRule(WithReservationsRule(NewReservation().WithReservationAffinity(reservations.AnyAffinity))),
			expected:  false,
		},
		{
			name:      "rule with any reservation, mig with shared reservation, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificSharedAffinity}).Build(),
			rule:      NewRule(WithReservationsRule(NewReservation().WithReservationAffinity(reservations.AnyAffinity))),
			expected:  false,
		},
		{
			name:      "rule without reservation, mig with specific reservation, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificLocalAffinity}).Build(),
			rule:      NewRule(),
			expected:  true,
		},
		{
			name:      "rule without reservation, mig with specific shared reservation, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificSharedAffinity}).Build(),
			rule:      NewRule(),
			expected:  true,
		},
		{
			name:      "rule without reservation, mig with any reservation, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: anyAffinity}).Build(),
			rule:      NewRule(),
			expected:  true,
		},
		{
			name:      "rule with specific local reservation (block + sub-block), mig with (block only), not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificLocalAffinityWithBlock}).Build(),
			rule: NewRule(WithReservationsRule(NewReservation().
				WithReservationName(specificLocalAffinityName).
				WithReservationAffinity(reservations.SpecificAffinity).
				WithReservationPath(specificLocalAffinityPathWithSubBlock).
				WithReservationBlock(reservationBlockName).
				WithReservationSubBlock(reservationSubBlockName))),
			expected: false,
		},
		{
			name:      "rule with specific local reservation (block only), mig with (block + sub-block), not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificLocalAffinityWithSubBlock}).Build(),
			rule: NewRule(WithReservationsRule(NewReservation().
				WithReservationName(specificLocalAffinityName).
				WithReservationAffinity(reservations.SpecificAffinity).
				WithReservationPath(specificLocalAffinityPathWithBlock).
				WithReservationBlock(reservationBlockName))),
			expected: false,
		},
		{
			name:      "rule with specific local reservation (block + sub-block), mig with different sub-block, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, ReservationAffinity: specificLocalAffinityWithSubBlock}).Build(), // rule will have "res-sub-block"
			rule: NewRule(WithReservationsRule(NewReservation().
				WithReservationName(specificLocalAffinityName).
				WithReservationAffinity(reservations.SpecificAffinity).
				WithReservationPath(gceclient.ReservationRef{Name: specificLocalAffinityName, BlockName: reservationBlockName, SubBlockName: "other-sub-block"}.Path()).
				WithReservationBlock(reservationBlockName).
				WithReservationSubBlock(reservationSubBlockName))),
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.rule.Matches(tc.nodegroup)
			if actual != tc.expected {
				t.Errorf("Test: \"%v\" failed, expected matching: %v got: %v", tc.name, tc.expected, actual)
			}
		})
	}
}
