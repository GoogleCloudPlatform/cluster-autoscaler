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
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	gce_api "google.golang.org/api/compute/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
)

func TestBlocksPullerPullReservationBlocks(t *testing.T) {
	projectID := "cluster-project"
	sharedPrjID := "shared-project"

	// Define reservations
	rsv1 := BuildReservationWithLink("zone-A", "fake-machine-type", projectID, "rsv1")
	rsv1.Id = 1
	rsv2 := BuildReservationWithLink("zone-B", "fake-machine-type", projectID, "rsv2")
	rsv2.Id = 2
	rsv3 := BuildReservationWithLink("zone-B", "fake-machine-type", sharedPrjID, "rsv3")
	rsv3.Id = 3
	rsv4 := BuildReservationWithLink("zone-B", "fake-machine-type", sharedPrjID, "rsv4")
	rsv4.Id = 4
	rsv4.ResourceStatus = &gce_api.AllocationResourceStatus{ReservationBlockCount: 0}

	rsv1Ref := gceclient.GetReservationRefFromReservation(*rsv1)
	rsv2Ref := gceclient.GetReservationRefFromReservation(*rsv2)
	rsv3Ref := gceclient.GetReservationRefFromReservation(*rsv3)
	rsv4Ref := gceclient.GetReservationRefFromReservation(*rsv4)

	// Define reservation blocks
	rsvb1 := BuildSingleReservationBlock("rb1", 1, 0, "zone-A")
	rsvb2 := BuildSingleReservationBlock("rb2", 1, 0, "zone-B")
	rsvb3 := BuildSingleReservationBlock("rb3", 1, 0, "zone-B")
	rsvb4 := BuildSingleReservationBlock("rb4", 1, 0, "zone-B")

	testCases := []struct {
		name                  string
		sharedPrjs            []string
		reservations          []*gce_api.Reservation
		reservationBlocks     map[gceclient.ReservationRef][]*gceclient.GceReservationBlock
		wantReservationBlocks map[gceclient.ReservationRef][]*gceclient.GceReservationBlock
		errors                map[gceclient.ReservationRef]error
	}{
		{
			name: "ReservationBlocksInProject",
			reservations: []*gce_api.Reservation{
				rsv1, rsv2,
			},
			reservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				rsv1Ref: {rsvb1},
				rsv2Ref: {rsvb2},
			},
			wantReservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				rsv1Ref: {rsvb1},
				rsv2Ref: {rsvb2},
			},
		},
		{
			name: "NoReservationBlocksAvailable",
			reservations: []*gce_api.Reservation{
				{},
			},
			reservationBlocks:     map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{},
			wantReservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{},
		},
		{
			name: "ReservationBlocksInReservationError",
			reservations: []*gce_api.Reservation{
				rsv1,
			},
			reservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				rsv1Ref: {rsvb1},
			},
			wantReservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				rsv1Ref: nil,
			},
			errors: map[gceclient.ReservationRef]error{
				rsv1Ref: fmt.Errorf("api error"),
			},
		},
		{
			name:       "ReservationBlocksFromCurrentAndSharedProjects",
			sharedPrjs: []string{sharedPrjID},
			reservations: []*gce_api.Reservation{
				rsv1, rsv2, rsv3,
			},
			reservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				rsv1Ref: {rsvb1},
				rsv2Ref: {rsvb2},
				rsv3Ref: {rsvb3},
			},
			wantReservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				rsv1Ref: {rsvb1},
				rsv2Ref: {rsvb2},
				rsv3Ref: {rsvb3},
			},
		},
		{
			name: "FilterNotReadyReservationBlocks",
			reservations: []*gce_api.Reservation{
				rsv1,
			},
			reservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				rsv1Ref: {
					rsvb1,
					{Name: "not-ready-block", Status: "CREATING"},
				},
			},
			wantReservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				rsv1Ref: {rsvb1},
			},
		},
		{
			name: "CloudProviderIsNotCalledIfReservationBlocksCountIsZero",
			reservations: []*gce_api.Reservation{
				rsv4,
			},
			reservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				rsv4Ref: {
					rsvb4,
					{Name: "this-block-does-not-exist", Status: "NULL"},
				},
			},
			wantReservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				rsv4Ref: {},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reservationsPuller := NewTestingReservationsPuller(projectID, tc.sharedPrjs, tc.reservations)
			blocksPuller := NewBlocksPuller(NewFakeBlocksPullerProvider(tc.reservationBlocks, tc.errors), reservationsPuller)
			blocksPuller.Loop()

			result := make(map[gceclient.ReservationRef][]*gceclient.GceReservationBlock)
			for key := range tc.reservationBlocks {
				result[key] = blocksPuller.GetReservationBlocksInReservation(key)
			}

			if diff := cmp.Diff(tc.wantReservationBlocks, result, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("BlocksPuller.GetReservationBlocksInReservation() diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBlocksPullerRemoveStaleReservations(t *testing.T) {
	projectID := "project"

	rsv1 := BuildReservationWithLink("zone-A", "fake-machine-type", projectID, "rsv1")
	rsv1.Id = 1
	rsv2 := BuildReservationWithLink("zone-B", "fake-machine-type", projectID, "rsv2")
	rsv2.Id = 2

	rsv1Key := gceclient.GetReservationRefFromReservation(*rsv1)
	rsv2Key := gceclient.GetReservationRefFromReservation(*rsv2)

	rsvb1 := BuildSingleReservationBlock("rb1", 1, 0, "zone-A")
	rsvb2 := BuildSingleReservationBlock("rb2", 1, 0, "zone-B")

	// Initial reservations (both rsv1 and rsv2)
	initialReservations := []*gce_api.Reservation{rsv1, rsv2}

	// Reservations after rsv2 is removed
	updatedReservations := []*gce_api.Reservation{rsv1}

	rbs := map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
		rsv1Key: {rsvb1},
		rsv2Key: {rsvb2},
	}

	// First loop with both reservations
	reservationsPuller := NewTestingReservationsPuller(projectID, nil, initialReservations)
	blocksPuller := NewBlocksPuller(NewFakeBlocksPullerProvider(rbs, nil), reservationsPuller)
	blocksPuller.Loop()

	if _, ok := blocksPuller.reservationBlocks[rsv1Key]; !ok {
		t.Errorf("Expected reservation key %q to be present", rsv1Key)
	}
	if _, ok := blocksPuller.reservationBlocks[rsv2Key]; !ok {
		t.Errorf("Expected reservation key %q to be present", rsv2Key)
	}

	reservationsPuller = NewTestingReservationsPuller(projectID, nil, updatedReservations)
	blocksPuller.reservationsPuller = reservationsPuller
	blocksPuller.Loop()

	if _, ok := blocksPuller.reservationBlocks[rsv1Key]; !ok {
		t.Errorf("Expected reservation key %q to be present", rsv1Key)
	}
	if _, ok := blocksPuller.reservationBlocks[rsv2Key]; ok {
		t.Errorf("Expected reservation key %q to be removed", rsv2Key)
	}
}

func TestBlocksPullerSubblocksPullerLoop(t *testing.T) {
	rsv1Ref := gceclient.ReservationRef{Project: "p", Zone: "z-a", Name: "rsv1"}
	testCases := []struct {
		name                  string
		reservationBlocks     map[gceclient.ReservationRef][]*gceclient.GceReservationBlock
		subBlocksForProvider  map[gceclient.ReservationRef]map[string][]*gceclient.GceReservationSubBlock
		wantReservationBlocks map[gceclient.ReservationRef][]*gceclient.GceReservationBlock
		errors                map[gceclient.ReservationRef]error
	}{
		{
			name: "SubBlocksAreAdded",
			reservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				rsv1Ref: {BuildSingleReservationBlock("rb1", 1, 0, "zone-A")},
			},
			subBlocksForProvider: map[gceclient.ReservationRef]map[string][]*gceclient.GceReservationSubBlock{
				rsv1Ref: {
					"rb1": {
						{Name: "rsb1", Status: "READY"},
						{Name: "rsb2", Status: "READY"},
					},
				},
			},
			wantReservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				rsv1Ref: {
					{
						Name:          "rb1",
						Count:         1,
						InUseCount:    0,
						Status:        "READY",
						Zone:          "zone-A",
						SubBlocks:     []*gceclient.GceReservationSubBlock{{Name: "rsb1", Status: "READY"}, {Name: "rsb2", Status: "READY"}},
						SubBlockCount: 2,
					},
				},
			},
		},
		{
			name: "SubBlocksAreReplaced",
			reservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				rsv1Ref: {
					{
						Name:          "rb1",
						Status:        "READY",
						SubBlocks:     []*gceclient.GceReservationSubBlock{{Name: "existing-sub-block"}},
						SubBlockCount: 1,
					},
				},
			},
			subBlocksForProvider: map[gceclient.ReservationRef]map[string][]*gceclient.GceReservationSubBlock{
				rsv1Ref: {
					"rb1": {
						{Name: "new-ready-sub-block", Status: "READY"},
					},
				},
			},
			wantReservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				rsv1Ref: {
					{
						Name:          "rb1",
						Status:        "READY",
						SubBlocks:     []*gceclient.GceReservationSubBlock{{Name: "new-ready-sub-block", Status: "READY"}},
						SubBlockCount: 1,
					},
				},
			},
		},
		{
			name: "NotReadySubBlocksAreFiltered",
			reservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				rsv1Ref: {BuildSingleReservationBlock("rb1", 1, 0, "zone-A")},
			},
			subBlocksForProvider: map[gceclient.ReservationRef]map[string][]*gceclient.GceReservationSubBlock{
				rsv1Ref: {
					"rb1": {
						{Name: "rsb1", Status: "READY"},
						{Name: "rsb2", Status: "CREATING"},
					},
				},
			},
			wantReservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				rsv1Ref: {
					{
						Name:          "rb1",
						Count:         1,
						InUseCount:    0,
						Status:        "READY",
						Zone:          "zone-A",
						SubBlocks:     []*gceclient.GceReservationSubBlock{{Name: "rsb1", Status: "READY"}},
						SubBlockCount: 2,
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := &fakeSubBlockProvider{subBlocks: tc.subBlocksForProvider, errors: tc.errors}
			puller := &BlocksPuller{provider: provider}
			result := puller.subblocksPullerLoop(tc.reservationBlocks)

			if diff := cmp.Diff(tc.wantReservationBlocks, result, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("BlocksPuller.subblocksPullerLoop() diff (-want +got):\n%s", diff)
			}
		})
	}
}
