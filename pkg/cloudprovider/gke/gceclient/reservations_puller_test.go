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
	"context"
	"errors"
	"math/rand"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	gce_api "google.golang.org/api/compute/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/consumablereservations"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func less(a, b *gce_api.Reservation) bool {
	return a.Id < b.Id
}

func TestProjectPullerPullReservations(t *testing.T) {
	rsv1 := buildSingleMachineReservation("fake-machine-type", "us-central1-a")
	rsv2 := buildSingleMachineReservation("fake-machine-type", "us-central1-b")

	testCases := []struct {
		name                  string
		projectID             string
		reservations          []*gce_api.Reservation
		reservationsInProject []*gce_api.Reservation
		err                   error
		wantReservations      []*gce_api.Reservation
		wantLoopError         error
	}{
		{
			name: "empty puller cache - return no reservations in default project",
		},
		{
			name:          "on provider reservations fetch error - return no reservations",
			err:           errors.New("GCE GetReservations Fetch error"),
			wantLoopError: errors.New("GCE GetReservations Fetch error"),
		},
		{
			name: "standard reservations lookup in default project",
			reservationsInProject: []*gce_api.Reservation{
				rsv1,
			},
			wantReservations: []*gce_api.Reservation{
				rsv1,
			},
		},
		{
			name:          "on provider reservations in project fetch error - return no reservations",
			projectID:     "someOtherProject",
			err:           errors.New("GCE GetReservationsInProject Fetch error"),
			wantLoopError: errors.New("GCE GetReservationsInProject Fetch error"),
		},
		{
			name:      "standard reservations lookup in specified project",
			projectID: "someOtherProject",
			reservationsInProject: []*gce_api.Reservation{
				rsv2,
			},
			wantReservations: []*gce_api.Reservation{
				rsv2,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mGceClient := BuildAutoscalingInternalGceClientMock().
				WithFetchReservationsInProject(func(project string) ([]*gce_api.Reservation, error) { return tc.reservationsInProject, tc.err })

			puller := NewProjectPuller(mGceClient, tc.projectID, []string{"us-central1-a", "us-central1-b"})
			puller.lastReservationSeen = time.Time{}
			puller.Loop()

			assert.Equal(t, tc.wantLoopError, puller.lastLoopError)
			assert.Equal(t, len(tc.wantReservations) == 0, puller.lastReservationSeen.Equal(time.Time{}))

			result := puller.GetReservations()
			if diff := cmp.Diff(tc.wantReservations, result, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("ProjectPuller.GetReservations() diff (-want +got):\n%s", diff)
			}
		})
	}
}

type consumableReservationsCallResult struct {
	reservations []*gce_api.Reservation
	err          consumablereservations.Error
}

func TestPullConsumableReservations(t *testing.T) {
	rsvA := buildSingleMachineReservation("fake-machine-type", "us-central1-a")
	rsvB := buildSingleMachineReservation("fake-machine-type", "us-central1-b")

	testCases := []struct {
		name             string
		callsPerZone     map[string]consumableReservationsCallResult
		wantReservations []*gce_api.Reservation
		wantLoopFailed   bool
	}{
		{
			name: "empty puller cache - return no reservations",
			callsPerZone: map[string]consumableReservationsCallResult{
				"us-central1-a": {},
				"us-central1-b": {},
			},
		},
		{
			name: "on internal error - return no reservations, fail loop",
			callsPerZone: map[string]consumableReservationsCallResult{
				"us-central1-a": {
					err: consumablereservations.NewError(errors.New("internal error"), consumablereservations.InternalError),
				},
				"us-central1-b": {
					reservations: []*gce_api.Reservation{rsvB},
				},
			},
			wantLoopFailed: true,
		},
		{
			name: "on client error in one zone - return other reservations",
			callsPerZone: map[string]consumableReservationsCallResult{
				"us-central1-a": {
					err: consumablereservations.NewError(errors.New("client error"), consumablereservations.ClientError),
				},
				"us-central1-b": {
					reservations: []*gce_api.Reservation{rsvB},
				},
			},
			wantReservations: []*gce_api.Reservation{rsvB},
		},
		{
			name: "on client error in all zones - return no reservations, fail loop",
			callsPerZone: map[string]consumableReservationsCallResult{
				"us-central1-a": {
					err: consumablereservations.NewError(errors.New("client error"), consumablereservations.ClientError),
				},
				"us-central1-b": {
					err: consumablereservations.NewError(errors.New("client error"), consumablereservations.ClientError),
				},
			},
			wantLoopFailed: true,
		},
		{
			name: "on all calls successful - return combined reservations",
			callsPerZone: map[string]consumableReservationsCallResult{
				"us-central1-a": {
					reservations: []*gce_api.Reservation{rsvA},
				},
				"us-central1-b": {
					reservations: []*gce_api.Reservation{rsvB},
				},
			},
			wantReservations: []*gce_api.Reservation{rsvA, rsvB},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			mGceClient := BuildAutoscalingInternalGceClientMock().
				WithFetchZones(func(region string) ([]string, error) { return []string{"us-central1-a", "us-central1-b"}, nil })
			crClient := consumablereservations.NewClientMock(func(ctx context.Context, projectID, zone string) ([]*gce_api.Reservation, consumablereservations.Error) {
				return tc.callsPerZone[zone].reservations, tc.callsPerZone[zone].err
			})
			gm := experiments.NewMockManager("ClusterAutoscaler::UseConsumableReservationsApi")

			puller, _ := NewReservationsPuller(mGceClient, crClient, gm, "", true, "us-central1")
			puller.updateExperiments()
			puller.lastConsumableReservationSeen = time.Time{}
			puller.consumablePullerLoop(ctx)

			assert.Equal(t, tc.wantLoopFailed, puller.lastConsumableLoopFailed)
			assert.Equal(t, len(tc.wantReservations) == 0, puller.lastConsumableReservationSeen.Equal(time.Time{}))

			result := puller.GetReservations()
			assert.ElementsMatch(t, tc.wantReservations, result)
		})
	}
}

func TestFilterReservations(t *testing.T) {
	zone1 := "us-central1-a"
	zone2 := "us-central1-b"

	specificReservation1 := &gce_api.AllocationSpecificSKUReservation{
		InstanceProperties: &gce_api.AllocationSpecificSKUAllocationReservedInstanceProperties{
			MachineType: "machine-type-1",
		},
	}

	specificReservation2 := &gce_api.AllocationSpecificSKUReservation{
		InstanceProperties: &gce_api.AllocationSpecificSKUAllocationReservedInstanceProperties{
			MachineType: "machine-type-2",
		},
	}

	testCases := []struct {
		name             string
		zones            []string
		zonesErr         error
		reservations     []*gce_api.Reservation
		wantReservations []*gce_api.Reservation
		wantErr          bool
	}{
		{
			name:  "no reservations",
			zones: []string{zone1, zone2},
		},
		{
			name:  "not-READY reservations are filtered out",
			zones: []string{zone1, zone2},
			reservations: []*gce_api.Reservation{
				{Status: "UNREADY", Zone: zone1, SpecificReservation: specificReservation1},
				{Status: "ANYTHING", Zone: zone2, SpecificReservation: specificReservation2},
			},
		},
		{
			name:  "specific reservations required are not filtered out",
			zones: []string{zone1, zone2},
			reservations: []*gce_api.Reservation{
				{Status: "READY", Zone: zone1, SpecificReservation: specificReservation1, SpecificReservationRequired: true},
			},
			wantReservations: []*gce_api.Reservation{
				{Status: "READY", Zone: zone1, SpecificReservation: specificReservation1, SpecificReservationRequired: true},
			},
		},
		{
			name:  "reservations without properties set are filtered out",
			zones: []string{zone1, zone2},
			reservations: []*gce_api.Reservation{
				{Status: "READY", Zone: zone1},
				{Status: "READY", Zone: zone2, SpecificReservation: &gce_api.AllocationSpecificSKUReservation{}},
			},
		},
		{
			name:  "reservations in non-cluster zones are filtered out",
			zones: []string{zone1, zone2},
			reservations: []*gce_api.Reservation{
				{Status: "READY", Zone: "zone-3", SpecificReservation: specificReservation1},
				{Status: "READY", Zone: "zone-4", SpecificReservation: specificReservation2},
			},
		},
		{
			name:  "READY reservations are not filtered",
			zones: []string{zone1, zone2},
			reservations: []*gce_api.Reservation{
				{Status: "READY", Zone: zone1, SpecificReservation: specificReservation1},
				{Status: "READY", Zone: zone2, SpecificReservation: specificReservation2},
			},
			wantReservations: []*gce_api.Reservation{
				{Status: "READY", Zone: zone1, SpecificReservation: specificReservation1},
				{Status: "READY", Zone: zone2, SpecificReservation: specificReservation2},
			},
		},
		{
			name:  "everything together",
			zones: []string{zone1, zone2},
			reservations: []*gce_api.Reservation{
				{Status: "UNREADY", Zone: zone1, SpecificReservation: specificReservation1},
				{Status: "ANYTHING", Zone: zone2, SpecificReservation: specificReservation2},
				{Status: "READY", Zone: zone1, SpecificReservation: specificReservation1, SpecificReservationRequired: true},
				{Status: "READY", Zone: zone1},
				{Status: "READY", Zone: zone2, SpecificReservation: &gce_api.AllocationSpecificSKUReservation{}},
				{Status: "READY", SpecificReservation: specificReservation1},
				{Status: "READY", SpecificReservation: specificReservation2},
				{Status: "READY", Zone: zone1, SpecificReservation: specificReservation1},
				{Status: "READY", Zone: zone2, SpecificReservation: specificReservation2},
			},
			wantReservations: []*gce_api.Reservation{
				{Status: "READY", Zone: zone1, SpecificReservation: specificReservation1},
				{Status: "READY", Zone: zone2, SpecificReservation: specificReservation2},
				{Status: "READY", Zone: zone1, SpecificReservation: specificReservation1, SpecificReservationRequired: true},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reservations, err := filterReservations(tc.reservations, tc.zones)

			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.ElementsMatch(t, tc.wantReservations, reservations)
			}
		})
	}
}

func TestProjectPullerUpdateInterval(t *testing.T) {
	testCases := []struct {
		name                string
		lastLoopFailed      error
		lastReservationSeen time.Time
		wantDuration        time.Duration
	}{
		{
			name:                "last reservation seen over 1 hour ago, last successful loop - update interval about 1 hour",
			lastLoopFailed:      nil,
			lastReservationSeen: time.Now().Add(-time.Hour),
			wantDuration:        time.Hour,
		},
		{
			name:                "last reservation seen within 1 hour ago, last successful loop - update interval about 1 minute",
			lastLoopFailed:      nil,
			lastReservationSeen: time.Now().Add(-30 * time.Minute),
			wantDuration:        time.Minute,
		},
		{
			name:                "empty puller cache, last failed loop - update interval about 1 minute",
			lastLoopFailed:      errors.New("last loop failed"),
			lastReservationSeen: time.Now().Add(-time.Hour),
			wantDuration:        time.Minute,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mGceClient := BuildAutoscalingInternalGceClientMock()
			puller := NewProjectPuller(mGceClient, "", []string{})

			// manually set the puller config
			puller.lastLoopError = tc.lastLoopFailed
			puller.lastReservationSeen = tc.lastReservationSeen

			// the interval is jittered by 20%, so we're expecting a result within a range
			gotDuration := puller.UpdateInterval()

			if gotDuration != tc.wantDuration {
				t.Errorf("ProjectPuller.updateInterval() is out of range; want: %v got: %v", tc.wantDuration, gotDuration)
			}
		})
	}
}

func TestUpdateConsumablePullerInterval(t *testing.T) {
	testCases := []struct {
		name                string
		lastLoopFailed      bool
		lastReservationSeen time.Time
		wantDuration        time.Duration
	}{
		{
			name:                "last reservation seen over 1 hour ago, last successful loop - update interval about 1 hour",
			lastLoopFailed:      false,
			lastReservationSeen: time.Now().Add(-time.Hour),
			wantDuration:        time.Hour,
		},
		{
			name:                "last reservation seen within 1 hour ago, last successful loop - update interval about 1 minute",
			lastLoopFailed:      false,
			lastReservationSeen: time.Now().Add(-30 * time.Minute),
			wantDuration:        time.Minute,
		},
		{
			name:                "empty puller cache, last failed loop - update interval about 1 minute",
			lastLoopFailed:      true,
			lastReservationSeen: time.Now().Add(-time.Hour),
			wantDuration:        time.Minute,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mGceClient := BuildAutoscalingInternalGceClientMock().
				WithFetchZones(func(region string) ([]string, error) { return []string{"us-centra1-b"}, nil })
			gm := experiments.NewMockManager("ClusterAutoscaler::UseConsumableReservationsApi")
			puller, _ := NewReservationsPuller(mGceClient, nil, gm, "", true, "us-central1")

			puller.lastConsumableLoopFailed = tc.lastLoopFailed
			puller.lastConsumableReservationSeen = tc.lastReservationSeen

			gotDuration := puller.updateConsumablePullerInterval()

			if gotDuration != tc.wantDuration {
				t.Errorf("ProjectPuller.updateConsumablePullerInterval() is out of range; want: %v got: %v", tc.wantDuration, gotDuration)
			}
		})
	}
}

func TestPullerPullReservations(t *testing.T) {
	rsv1 := buildSingleMachineReservation("fake-machine-type", "us-central1-a")
	rsv1.Id = 1
	rsv2 := buildSingleMachineReservation("fake-machine-type", "us-central1-b")
	rsv2.Id = 2
	rsv3 := buildSingleMachineReservation("fake-machine-type", "us-central1-b")
	rsv3.Id = 3

	testCases := []struct {
		name                   string
		localProject           string
		reservations           map[string][]*gce_api.Reservation
		consumableReservations []*gce_api.Reservation
		zones                  []string
		experimentFlags        []string
		addProjectBeforeRun    []string
		addProjectAfterRun     []string
		wantReservations       []*gce_api.Reservation
	}{
		{
			name:         "getreservations can pull from across different projects",
			localProject: "local-proj",
			reservations: map[string][]*gce_api.Reservation{
				"local-proj":               {rsv1},
				"somewhere-out-there-proj": {rsv2},
			},
			zones:               []string{"us-central1-a", "us-central1-b"},
			addProjectBeforeRun: []string{"somewhere-out-there-proj"},
			wantReservations:    []*gce_api.Reservation{rsv1, rsv2},
		},
		{
			name:         "getreservations does filtering out of unsupported zones",
			localProject: "local-proj",
			reservations: map[string][]*gce_api.Reservation{
				"local-proj":               {rsv1},
				"somewhere-out-there-proj": {rsv2},
			},
			zones:               []string{"us-central1-b"},
			addProjectBeforeRun: []string{"somewhere-out-there-proj"},
			wantReservations:    []*gce_api.Reservation{rsv2},
		},
		{
			name:         "getreservations does not pull from projects not added",
			localProject: "local-proj",
			reservations: map[string][]*gce_api.Reservation{
				"local-proj":               {rsv1},
				"somewhere-out-there-proj": {rsv2},
			},
			zones:            []string{"us-central1-a", "us-central1-b"},
			wantReservations: []*gce_api.Reservation{rsv1},
		},
		{
			name:         "getreservations adding project after is same as adding project before",
			localProject: "local-proj",
			reservations: map[string][]*gce_api.Reservation{
				"local-proj":               {rsv1},
				"somewhere-out-there-proj": {rsv2},
			},
			zones:              []string{"us-central1-a", "us-central1-b"},
			addProjectAfterRun: []string{"somewhere-out-there-proj"},
			wantReservations:   []*gce_api.Reservation{rsv1, rsv2},
		},
		{
			name:         "getreservations when no reservations return no reservations",
			localProject: "local-proj",
			reservations: map[string][]*gce_api.Reservation{
				"local-proj":               {},
				"somewhere-out-there-proj": {},
			},
			zones:               []string{"us-central1-a", "us-central1-b"},
			addProjectBeforeRun: []string{"somewhere-out-there-proj"},
			wantReservations:    []*gce_api.Reservation{},
		},
		{
			name:         "getreservations from ConsumableReservation puller",
			localProject: "local-proj",
			reservations: map[string][]*gce_api.Reservation{
				"local-proj":               {rsv1},
				"somewhere-out-there-proj": {rsv2},
			},
			consumableReservations: []*gce_api.Reservation{rsv1, rsv2, rsv3},
			zones:                  []string{"us-central1-a", "us-central1-b"},
			experimentFlags:        []string{"ClusterAutoscaler::UseConsumableReservationsApi"},
			addProjectAfterRun:     []string{"somewhere-out-there-proj"},
			wantReservations:       []*gce_api.Reservation{rsv1, rsv2, rsv3},
		},
		{
			name:         "getreservations from ProjectPullers, ConsumableReservation puller deactivated",
			localProject: "local-proj",
			reservations: map[string][]*gce_api.Reservation{
				"local-proj":               {rsv1},
				"somewhere-out-there-proj": {rsv2},
			},
			consumableReservations: []*gce_api.Reservation{rsv1, rsv2, rsv3},
			zones:                  []string{"us-central1-a", "us-central1-b"},
			addProjectAfterRun:     []string{"somewhere-out-there-proj"},
			wantReservations:       []*gce_api.Reservation{rsv1, rsv2},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			mGceClient := BuildAutoscalingInternalGceClientMock().
				WithFetchZones(func(region string) ([]string, error) { return tc.zones, nil }).
				WithFetchReservationsInProject(func(project string) ([]*gce_api.Reservation, error) { return tc.reservations[project], nil })
			gm := experiments.NewMockManager(tc.experimentFlags...)

			puller, _ := NewReservationsPuller(mGceClient, nil, gm, tc.localProject, len(tc.experimentFlags) > 0, "us-central1-b")
			puller.consumableReservations = tc.consumableReservations

			for _, p := range tc.addProjectBeforeRun {
				puller.AddProject(p)
			}
			puller.Run(ctx)
			defer cancel()
			for _, p := range tc.addProjectAfterRun {
				puller.AddProject(p)
			}

			result := puller.GetReservations()
			if diff := cmp.Diff(tc.wantReservations, result, cmpopts.EquateEmpty(), cmpopts.SortSlices(less)); diff != "" {
				t.Errorf("ProjectPuller.GetReservations() diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPullerPullReservationsInProject(t *testing.T) {
	rsv1 := buildSingleMachineReservation("fake-machine-type", "us-central1-a")
	rsv1.SelfLink = "https://www.googleapis.com/compute/v1/projects/local-proj/zones/[Zone]/reservations/[Reservation]"
	rsv1.Id = 1
	rsv2 := buildSingleMachineReservation("fake-machine-type", "us-central1-b")
	rsv2.SelfLink = "https://www.googleapis.com/compute/v1/projects/somewhere-out-there-proj/zones/[Zone]/reservations/[Reservation]"
	rsv2.Id = 2
	rsv3 := buildSingleMachineReservation("fake-machine-type", "us-central1-b")
	rsv3.SelfLink = "https://www.googleapis.com/compute/v1/projects/somewhere-out-there-proj/zones/[Zone]/reservations/[Reservation]"
	rsv3.Id = 3

	testCases := []struct {
		name                   string
		localProject           string
		reservations           map[string][]*gce_api.Reservation
		consumableReservations []*gce_api.Reservation
		zones                  []string
		experimentFlags        []string
		addProjectBeforeRun    []string
		addProjectAfterRun     []string
		inProject              string
		wantReservations       []*gce_api.Reservation
	}{
		{
			name:         "getreservationsinproject can pull from local project",
			localProject: "local-proj",
			reservations: map[string][]*gce_api.Reservation{
				"local-proj":               {rsv1},
				"somewhere-out-there-proj": {rsv2},
			},
			zones:               []string{"us-central1-a", "us-central1-b"},
			addProjectBeforeRun: []string{"somewhere-out-there-proj"},
			inProject:           "local-proj",
			wantReservations:    []*gce_api.Reservation{rsv1},
		},
		{
			name:         "getreservationsinproject can pull from different project",
			localProject: "local-proj",
			reservations: map[string][]*gce_api.Reservation{
				"local-proj":               {rsv1},
				"somewhere-out-there-proj": {rsv2},
			},
			zones:               []string{"us-central1-a", "us-central1-b"},
			addProjectBeforeRun: []string{"somewhere-out-there-proj"},
			inProject:           "somewhere-out-there-proj",
			wantReservations:    []*gce_api.Reservation{rsv2},
		},
		{
			name:         "getreservationsinproject not error for a project not added",
			localProject: "local-proj",
			reservations: map[string][]*gce_api.Reservation{
				"local-proj":               {rsv1},
				"somewhere-out-there-proj": {rsv2},
			},
			zones:            []string{"us-central1-a", "us-central1-b"},
			inProject:        "somewhere-out-there-proj",
			wantReservations: []*gce_api.Reservation{},
		},
		{
			name:         "getreservationsinproject does filtering out of unsupported zones",
			localProject: "local-proj",
			reservations: map[string][]*gce_api.Reservation{
				"local-proj": {rsv1, rsv2},
			},
			zones:            []string{"us-central1-b"},
			inProject:        "local-proj",
			wantReservations: []*gce_api.Reservation{rsv2},
		},
		{
			name:         "getreservationsinproject adding project after is same as adding project before",
			localProject: "local-proj",
			reservations: map[string][]*gce_api.Reservation{
				"local-proj":               {rsv1},
				"somewhere-out-there-proj": {rsv2},
			},
			zones:              []string{"us-central1-a", "us-central1-b"},
			addProjectAfterRun: []string{"somewhere-out-there-proj"},
			inProject:          "somewhere-out-there-proj",
			wantReservations:   []*gce_api.Reservation{rsv2},
		},
		{
			name:         "getreservationsinproject when no reservations return no reservations",
			localProject: "local-proj",
			reservations: map[string][]*gce_api.Reservation{
				"local-proj":               {},
				"somewhere-out-there-proj": {},
			},
			zones:               []string{"us-central1-a", "us-central1-b"},
			addProjectBeforeRun: []string{"somewhere-out-there-proj"},
			inProject:           "local-proj",
			wantReservations:    []*gce_api.Reservation{},
		},
		{
			name:         "getreservationsinproject correctly returns ConsumableReservations",
			localProject: "local-proj",
			reservations: map[string][]*gce_api.Reservation{
				"local-proj":               {rsv1},
				"somewhere-out-there-proj": {rsv2},
			},
			consumableReservations: []*gce_api.Reservation{rsv1, rsv2, rsv3},
			zones:                  []string{"us-central1-a", "us-central1-b"},
			experimentFlags:        []string{"ClusterAutoscaler::UseConsumableReservationsApi"},
			addProjectBeforeRun:    []string{"somewhere-out-there-proj"},
			inProject:              "somewhere-out-there-proj",
			wantReservations:       []*gce_api.Reservation{rsv2, rsv3},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())

			mGceClient := BuildAutoscalingInternalGceClientMock().
				WithFetchZones(func(region string) ([]string, error) { return tc.zones, nil }).
				WithFetchReservationsInProject(func(project string) ([]*gce_api.Reservation, error) { return tc.reservations[project], nil })
			gm := experiments.NewMockManager(tc.experimentFlags...)

			puller, _ := NewReservationsPuller(mGceClient, nil, gm, tc.localProject, len(tc.experimentFlags) > 0, "us-central1-a")
			puller.consumableReservations = tc.consumableReservations

			for _, p := range tc.addProjectBeforeRun {
				puller.AddProject(p)
			}

			puller.Run(ctx)
			defer cancel()
			for _, p := range tc.addProjectAfterRun {
				puller.AddProject(p)
			}

			result := puller.GetReservationsInProject(tc.inProject)
			if diff := cmp.Diff(tc.wantReservations, result, cmpopts.EquateEmpty(), cmpopts.SortSlices(less)); diff != "" {
				t.Errorf("ProjectPuller.GetReservations() diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestReservationsPuller_AddProjectRace(t *testing.T) {
	// test for race condition in b/454905749
	synctest.Test(t, func(t *testing.T) {
		localProject := "local-project"
		sharedProject := "shared-project"
		zone := "us-central1-a"

		ctx, cancel := context.WithCancel(context.Background())

		localRsv := buildSingleMachineReservation("fake-machine-type", zone)
		sharedRsv := buildSingleMachineReservation("fake-machine-type", zone)

		mGceClient := BuildAutoscalingInternalGceClientMock().
			WithFetchZones(func(region string) ([]string, error) { return []string{zone}, nil }).
			WithFetchReservationsInProject(func(project string) ([]*gce_api.Reservation, error) {
				if project == localProject {
					// We no longer need to "widen windows" with real time; synctest.Wait() ensures the bubble is idle.
					return []*gce_api.Reservation{localRsv}, nil
				}
				return []*gce_api.Reservation{sharedRsv}, nil
			})

		puller, _ := NewReservationsPuller(mGceClient, nil, nil, localProject, false, "us-central1-c")
		defer puller.Wait()
		defer cancel()

		go puller.Run(ctx)

		synctest.Wait()

		puller.AddProject(sharedProject)

		synctest.Wait()

		wantLocal := []*gce_api.Reservation{localRsv}
		wantShared := []*gce_api.Reservation{sharedRsv}

		assert.ElementsMatch(t, wantLocal, puller.GetReservationsInProject(localProject))
		assert.ElementsMatch(t, wantShared, puller.GetReservationsInProject(sharedProject))
	})
}

func TestPullerPullLocalReservations(t *testing.T) {
	localProject := "local-project"
	sharedProject := "somewhere-out-there-proj"
	zones := []string{"us-central1-a", "us-central1-b"}
	rsv1 := buildSingleMachineReservation("fake-machine-type", "us-central1-a")
	rsv2 := buildSingleMachineReservation("fake-machine-type", "us-central1-b")

	testCases := []struct {
		name             string
		reservations     map[string][]*gce_api.Reservation
		wantReservations []*gce_api.Reservation
	}{
		{
			name: "ReservationsInBothProjects",
			reservations: map[string][]*gce_api.Reservation{
				localProject:  {rsv1},
				sharedProject: {rsv2},
			},
			wantReservations: []*gce_api.Reservation{rsv1},
		},
		{
			name: "LocalReservationAvailable",
			reservations: map[string][]*gce_api.Reservation{
				localProject:  {rsv1},
				sharedProject: {},
			},
			wantReservations: []*gce_api.Reservation{rsv1},
		},
		{
			name: "SharedProjectReservationAvailable",
			reservations: map[string][]*gce_api.Reservation{
				localProject:  {},
				sharedProject: {rsv2},
			},
			wantReservations: []*gce_api.Reservation{},
		},
		{
			name: "NoReservationAvailable",
			reservations: map[string][]*gce_api.Reservation{
				localProject:  {},
				sharedProject: {},
			},
			wantReservations: []*gce_api.Reservation{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			mGceClient := BuildAutoscalingInternalGceClientMock().
				WithFetchZones(func(region string) ([]string, error) { return zones, nil }).
				WithFetchReservationsInProject(func(project string) ([]*gce_api.Reservation, error) { return tc.reservations[project], nil })

			puller, _ := NewReservationsPuller(mGceClient, nil, nil, localProject, false, "us-central1-c")
			puller.AddProject(sharedProject)

			puller.Run(ctx)

			result := puller.GetLocalReservations()
			if diff := cmp.Diff(tc.wantReservations, result, cmpopts.EquateEmpty(), cmpopts.SortSlices(less)); diff != "" {
				t.Errorf("ProjectPuller.GetLocalReservations() diff (-want +got):\n%s", diff)
			}
		})
	}
}

func buildSingleMachineReservation(machineType, zone string) *gce_api.Reservation {
	return &gce_api.Reservation{
		Id:                          rand.Uint64(),
		Status:                      "READY",
		SpecificReservationRequired: false,
		Zone:                        zone,
		SpecificReservation: &gce_api.AllocationSpecificSKUReservation{
			InstanceProperties: &gce_api.AllocationSpecificSKUAllocationReservedInstanceProperties{
				MachineType: machineType,
			},
			InUseCount: int64(0),
			Count:      int64(1),
		},
		ResourceStatus: &gce_api.AllocationResourceStatus{},
	}
}
