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
	"testing"

	"github.com/stretchr/testify/assert"
	gcev1 "google.golang.org/api/compute/v1"
	gceinternal "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
)

const testReservation = "res-1"

func TestCreateInstancesConsumesReservation(t *testing.T) {
	testCases := []struct {
		name           string
		mig            testMIG
		reservation    *gcev1.Reservation
		wantErr        bool
		wantInUseCount int64
	}{
		{
			name:           "no_affinity",
			mig:            newDefaultMig(2),
			reservation:    newTestReservation(testMachineType, testZone, 3, affinityAny),
			wantInUseCount: 0,
		},
		{
			name:           "no_reservation_affinity",
			mig:            newDefaultMig(2, withReservationAffinity("NO_RESERVATION")),
			reservation:    newTestReservation(testMachineType, testZone, 3, affinityAny),
			wantInUseCount: 0,
		},
		{
			name:           "any_reservation",
			mig:            newDefaultMig(2, withReservationAffinity(consumeAnyReservation)),
			reservation:    newTestReservation(testMachineType, testZone, 3, affinityAny),
			wantInUseCount: 2,
		},
		{
			name:           "any_reservation_then_fail",
			mig:            newDefaultMig(2, withReservationAffinity(consumeAnyReservationThenFail)),
			reservation:    newTestReservation(testMachineType, testZone, 3, affinityAny),
			wantInUseCount: 2,
		},
		{
			name:           "any_reservation_capped_at_reservation_count",
			mig:            newDefaultMig(2, withReservationAffinity(consumeAnyReservation)),
			reservation:    newTestReservation(testMachineType, testZone, 3, affinityAny, withUsed(2)),
			wantInUseCount: 3,
		},
		{
			name:           "any_reservation_skips_specific_reservation_required",
			mig:            newDefaultMig(2, withReservationAffinity(consumeAnyReservation)),
			reservation:    newTestReservation(testMachineType, testZone, 3, affinitySpecific),
			wantInUseCount: 0,
		},
		{
			name:           "any_reservation_skips_other_zone",
			mig:            newDefaultMig(2, withReservationAffinity(consumeAnyReservation)),
			reservation:    newTestReservation(testMachineType, "us-central1-b", 3, affinityAny),
			wantInUseCount: 0,
		},
		{
			name:           "any_reservation_skips_other_machine_type",
			mig:            newDefaultMig(2, withReservationAffinity(consumeAnyReservation)),
			reservation:    newTestReservation("n1-standard-4", testZone, 3, affinityAny),
			wantInUseCount: 0,
		},
		{
			name:           "specific_reservation_by_name",
			mig:            newDefaultMig(2, withReservationAffinity(consumeSpecificReservation, testReservation)),
			reservation:    newTestReservation(testMachineType, testZone, 3, affinitySpecific),
			wantInUseCount: 2,
		},
		{
			name:           "specific_reservation_by_path",
			mig:            newDefaultMig(2, withReservationAffinity(consumeSpecificReservation, "projects/"+testProject+"/reservations/"+testReservation+"/reservationBlocks/block-1")),
			reservation:    newTestReservation(testMachineType, testZone, 3, affinitySpecific),
			wantInUseCount: 2,
		},
		{
			name:           "specific_reservation_by_relative_sub_block_path",
			mig:            newDefaultMig(2, withReservationAffinity(consumeSpecificReservation, testReservation+"/reservationBlocks/block-1/reservationSubBlocks/sub-block-1")),
			reservation:    newTestReservation(testMachineType, testZone, 3, affinitySpecific),
			wantInUseCount: 2,
		},
		{
			name:           "specific_reservation_with_other_name",
			mig:            newDefaultMig(2, withReservationAffinity(consumeSpecificReservation, "other")),
			reservation:    newTestReservation(testMachineType, testZone, 3, affinitySpecific),
			wantErr:        true,
			wantInUseCount: 0,
		},
		{
			name:           "specific_reservation_without_enough_free_slots",
			mig:            newDefaultMig(2, withReservationAffinity(consumeSpecificReservation, testReservation)),
			reservation:    newTestReservation(testMachineType, testZone, 3, affinitySpecific, withUsed(3)),
			wantErr:        true,
			wantInUseCount: 3,
		},
		{
			name:           "any_reservation_then_fail_without_matching_reservation",
			mig:            newDefaultMig(2, withReservationAffinity(consumeAnyReservationThenFail)),
			reservation:    newTestReservation(testMachineType, "us-central1-b", 3, affinityAny),
			wantErr:        true,
			wantInUseCount: 0,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := newDefaultGceClient(t).
				WithTemplates(tc.mig.template).
				WithMigs(tc.mig.mig).
				WithReservations(inTestProject(tc.reservation))

			_, err := client.CreateInstances(t.Context(), testMigRef(), testTemplate, tc.mig.size, nil)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("CreateInstances() error = %v, want error: %t", err, tc.wantErr)
			}

			assert.Equal(t, tc.wantInUseCount, fetchInUseCount(t, client))
		})
	}
}

func TestDeleteInstancesReleasesReservation(t *testing.T) {
	mig := newDefaultMig(2, withReservationAffinity(consumeAnyReservation))
	client := newDefaultGceClient(t).
		WithTemplates(mig.template).
		WithMigs(mig.mig).
		WithReservations(inTestProject(newTestReservation(testMachineType, testZone, 3, affinityAny)))

	names, err := client.CreateInstances(t.Context(), testMigRef(), testTemplate, mig.size, nil)
	if err != nil || len(names) != 2 {
		t.Fatalf("CreateInstances() = %v, %v, want 2 instances", names, err)
	}
	toDelete := []gceinternal.GceRef{{Project: testProject, Zone: testZone, Name: names[0]}}
	if err := client.DeleteInstances(t.Context(), testMigRef(), toDelete); err != nil {
		t.Fatalf("DeleteInstances() failed: %v", err)
	}

	assert.Equal(t, int64(1), fetchInUseCount(t, client))
}

func TestResizeMigConsumesAndReleasesReservation(t *testing.T) {
	mig := newDefaultMig(3, withReservationAffinity(consumeAnyReservation))
	client := newDefaultGceClient(t).
		WithTemplates(mig.template).
		WithMigs(mig.mig).
		WithReservations(inTestProject(newTestReservation(testMachineType, testZone, 3, affinityAny)))

	if err := client.ResizeMig(t.Context(), testMigRef(), mig.size); err != nil {
		t.Fatalf("ResizeMig(%d) failed: %v", mig.size, err)
	}
	assert.Equal(t, int64(3), fetchInUseCount(t, client))

	if err := client.ResizeMig(t.Context(), testMigRef(), 1); err != nil {
		t.Fatalf("ResizeMig(1) failed: %v", err)
	}
	assert.Equal(t, int64(1), fetchInUseCount(t, client))
}

func TestUnlistedReservationsAreConsumedButNotListed(t *testing.T) {
	mig := newDefaultMig(2, withReservationAffinity(consumeSpecificReservation, "unlisted"))
	client := newDefaultGceClient(t).
		WithTemplates(mig.template).
		WithMigs(mig.mig).
		WithReservations(inTestProject(newTestReservation(testMachineType, testZone, 3, affinitySpecific))).
		WithUnlistedReservations(testProject, newTestReservation(testMachineType, testZone, 2, affinitySpecific, withName("unlisted")))

	if _, err := client.CreateInstances(t.Context(), testMigRef(), testTemplate, mig.size, nil); err != nil {
		t.Fatalf("CreateInstances() failed: %v", err)
	}

	assert.Equal(t, int64(2), client.unlistedReservations[testProject][0].SpecificReservation.InUseCount)
	listed, err := client.FetchReservationsInProject(testProject)
	if err != nil {
		t.Fatalf("FetchReservationsInProject() failed: %v", err)
	}
	assert.Len(t, listed, 1)
	if _, err := client.FetchReservation(testProject, "unlisted"); err == nil {
		t.Error("FetchReservation(unlisted) succeeded, want not found")
	}
}

func TestCreateInstancesCombinesReservedAndOnDemandCapacity(t *testing.T) {
	testCases := []struct {
		name                 string
		mig                  testMIG
		reservation          *gcev1.Reservation
		onDemandCapacity     int64
		wantErr              bool
		wantCreated          int
		wantInUseCount       int64
		wantOnDemandCapacity int64
	}{
		{
			name:                 "any_reservation_falls_back_to_on_demand",
			mig:                  newDefaultMig(2, withReservationAffinity(consumeAnyReservation)),
			reservation:          newTestReservation(testMachineType, testZone, 3, affinityAny, withUsed(2)),
			onDemandCapacity:     1,
			wantCreated:          2,
			wantInUseCount:       3,
			wantOnDemandCapacity: 0,
		},
		{
			name:                 "any_reservation_capped_by_on_demand",
			mig:                  newDefaultMig(2, withReservationAffinity(consumeAnyReservation)),
			reservation:          newTestReservation(testMachineType, testZone, 3, affinityAny, withUsed(2)),
			onDemandCapacity:     0,
			wantCreated:          1,
			wantInUseCount:       3,
			wantOnDemandCapacity: 0,
		},
		{
			name:                 "any_reservation_stockout",
			mig:                  newDefaultMig(2, withReservationAffinity(consumeAnyReservation)),
			reservation:          newTestReservation(testMachineType, testZone, 3, affinityAny, withUsed(3)),
			onDemandCapacity:     0,
			wantErr:              true,
			wantInUseCount:       3,
			wantOnDemandCapacity: 0,
		},
		{
			name:                 "no_affinity_uses_only_on_demand",
			mig:                  newDefaultMig(2),
			reservation:          newTestReservation(testMachineType, testZone, 3, affinityAny),
			onDemandCapacity:     1,
			wantCreated:          1,
			wantInUseCount:       0,
			wantOnDemandCapacity: 0,
		},
		{
			name:                 "specific_reservation_does_not_use_on_demand",
			mig:                  newDefaultMig(2, withReservationAffinity(consumeSpecificReservation, testReservation)),
			reservation:          newTestReservation(testMachineType, testZone, 3, affinitySpecific, withUsed(2)),
			onDemandCapacity:     5,
			wantCreated:          1,
			wantInUseCount:       3,
			wantOnDemandCapacity: 5,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := newDefaultGceClient(t).
				WithTemplates(tc.mig.template).
				WithMigs(tc.mig.mig).
				WithReservations(inTestProject(tc.reservation))
			client.SetOnDemandCapacity(testZone, testMachineType, tc.onDemandCapacity)

			names, err := client.CreateInstances(t.Context(), testMigRef(), testTemplate, tc.mig.size, nil)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("CreateInstances() error = %v, want error: %t", err, tc.wantErr)
			}

			assert.Len(t, names, tc.wantCreated)
			assert.Equal(t, tc.wantInUseCount, fetchInUseCount(t, client))
			assert.Equal(t, tc.wantOnDemandCapacity, onDemandCapacity(client))
		})
	}
}

func TestResizeAtomicallyConsumesNothingWithoutEnoughCapacity(t *testing.T) {
	mig := newDefaultMig(2, withReservationAffinity(consumeAnyReservation))
	client := newDefaultGceClient(t).
		WithTemplates(mig.template).
		WithMigs(mig.mig).
		WithReservations(inTestProject(newTestReservation(testMachineType, testZone, 3, affinityAny, withUsed(2))))
	client.SetOnDemandCapacity(testZone, testMachineType, 0)

	if err := client.ResizeAtomically(testMigRef(), testTemplate, []string{"instance-1", "instance-2"}); err == nil {
		t.Fatal("ResizeAtomically() succeeded, want stockout error")
	}

	assert.Equal(t, int64(2), fetchInUseCount(t, client))
	assert.Empty(t, client.instanceCapacity)
}

func TestDeleteInstancesReleasesOnDemandCapacity(t *testing.T) {
	mig := newDefaultMig(2)
	client := newDefaultGceClient(t).
		WithTemplates(mig.template).
		WithMigs(mig.mig)
	client.SetOnDemandCapacity(testZone, testMachineType, 2)

	names, err := client.CreateInstances(t.Context(), testMigRef(), testTemplate, mig.size, nil)
	if err != nil || len(names) != 2 {
		t.Fatalf("CreateInstances() = %v, %v, want 2 instances", names, err)
	}
	assert.Equal(t, int64(0), onDemandCapacity(client))

	toDelete := []gceinternal.GceRef{{Project: testProject, Zone: testZone, Name: names[0]}}
	if err := client.DeleteInstances(t.Context(), testMigRef(), toDelete); err != nil {
		t.Fatalf("DeleteInstances() failed: %v", err)
	}
	assert.Equal(t, int64(1), onDemandCapacity(client))
}

func TestFailedInstancesDoNotConsumeCapacity(t *testing.T) {
	mig := newDefaultMig(2, withReservationAffinity(consumeAnyReservation))
	client := newDefaultGceClient(t).
		WithTemplates(mig.template).
		WithMigs(mig.mig).
		WithReservations(inTestProject(newTestReservation(testMachineType, testZone, 3, affinityAny)))
	client.SetOnDemandCapacity(testZone, testMachineType, 1)
	client.SetCreateInstanceForZoneError(testZone, cloudprovider.InstanceErrorInfo{ErrorClass: cloudprovider.OtherErrorClass})

	if _, err := client.CreateInstances(t.Context(), testMigRef(), testTemplate, mig.size, nil); err != nil {
		t.Fatalf("CreateInstances() failed: %v", err)
	}

	assert.Equal(t, int64(0), fetchInUseCount(t, client))
	assert.Equal(t, int64(1), onDemandCapacity(client))
}

// reservationAffinity defines which instances can consume a test reservation.
type reservationAffinity int

const (
	// affinityAny reservations can be consumed by instances with a matching
	// ANY_RESERVATION, ANY_RESERVATION_THEN_FAIL or SPECIFIC_RESERVATION affinity.
	affinityAny reservationAffinity = iota
	// affinitySpecific reservations can only be consumed by instances that
	// target them with a SPECIFIC_RESERVATION affinity.
	affinitySpecific
)

type reservationOption func(*gcev1.Reservation)

// withUsed sets the number of reserved VMs that are already in use.
func withUsed(inUseCount int64) reservationOption {
	return func(r *gcev1.Reservation) {
		r.SpecificReservation.InUseCount = inUseCount
	}
}

// withName overrides the default testReservation name.
func withName(name string) reservationOption {
	return func(r *gcev1.Reservation) {
		r.Name = name
	}
}

// newTestReservation returns an unused specific reservation named
// testReservation for count machineType VMs in zone.
func newTestReservation(machineType, zone string, count int64, affinity reservationAffinity, opts ...reservationOption) *gcev1.Reservation {
	r := &gcev1.Reservation{
		Name:                        testReservation,
		Zone:                        "https://www.googleapis.com/compute/v1/projects/" + testProject + "/zones/" + zone,
		SpecificReservationRequired: affinity == affinitySpecific,
		SpecificReservation: &gcev1.AllocationSpecificSKUReservation{
			Count: count,
			InstanceProperties: &gcev1.AllocationSpecificSKUAllocationReservedInstanceProperties{
				MachineType: machineType,
			},
		},
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// inTestProject returns the reservations keyed by testProject, as expected by
// WithReservations.
func inTestProject(rsvs ...*gcev1.Reservation) map[string][]*gcev1.Reservation {
	return map[string][]*gcev1.Reservation{testProject: rsvs}
}

func fetchInUseCount(t *testing.T, client *GceClient) int64 {
	t.Helper()
	rsv, err := client.FetchReservation(testProject, testReservation)
	if err != nil {
		t.Fatalf("FetchReservation() failed: %v", err)
	}
	return rsv.SpecificReservation.InUseCount
}

func onDemandCapacity(client *GceClient) int64 {
	client.Lock()
	defer client.Unlock()
	return client.onDemandCapacity[CapacityKey{Zone: testZone, MachineType: testMachineType}]
}
