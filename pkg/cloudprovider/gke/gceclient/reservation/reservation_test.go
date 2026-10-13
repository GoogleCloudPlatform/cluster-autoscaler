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

package reservation

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	gceapi "google.golang.org/api/compute/v1"
)

const (
	ct5lp      = "projects/p/zones/z/acceleratorTypes/ct5lp"
	ct5lpVM    = "VM_FAMILY_CLOUD_TPU_LITE_POD_SLICE_CT5LP"
	testZone   = "https://www.googleapis.com/compute/v1/projects/p/zones/us-central1-a"
	testLink   = "https://www.googleapis.com/compute/v1/projects/p/zones/us-central1-a/reservations/rsv"
	testPolicy = "projects/p/regions/us-central1/resourcePolicies/compact"
)

func v1Resource(acceleratorType string, count int64) *gceapi.AllocationAggregateReservationReservedResourceInfo {
	return &gceapi.AllocationAggregateReservationReservedResourceInfo{
		Accelerator: &gceapi.AllocationAggregateReservationReservedResourceInfoAccelerator{
			AcceleratorType:  acceleratorType,
			AcceleratorCount: count,
		},
	}
}

func TestFromV1Nil(t *testing.T) {
	if got := FromV1(nil); got != nil {
		t.Errorf("FromV1(nil) = %v, want nil", got)
	}
}

func TestFromV1SpecificReservation(t *testing.T) {
	for _, tc := range []struct {
		name          string
		count         int64
		inUseCount    int64
		wantAvailable int64
	}{
		{name: "unused", count: 5, inUseCount: 0, wantAvailable: 5},
		{name: "partially_used", count: 5, inUseCount: 3, wantAvailable: 2},
		{name: "fully_used", count: 5, inUseCount: 5, wantAvailable: 0},
		{name: "overused_is_floored_at_zero", count: 5, inUseCount: 7, wantAvailable: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			props := &gceapi.AllocationSpecificSKUAllocationReservedInstanceProperties{MachineType: "n2-standard-4"}
			wantProps := &InstanceProperties{MachineType: "n2-standard-4"}
			v1 := &gceapi.Reservation{
				Id:                          123,
				Name:                        "rsv",
				SelfLink:                    testLink,
				Zone:                        testZone,
				Status:                      "READY",
				DeploymentType:              "DENSE",
				SpecificReservationRequired: true,
				ResourcePolicies:            map[string]string{"policy": testPolicy},
				ResourceStatus:              &gceapi.AllocationResourceStatus{ReservationBlockCount: 2},
				SpecificReservation: &gceapi.AllocationSpecificSKUReservation{
					Count:              tc.count,
					InUseCount:         tc.inUseCount,
					InstanceProperties: props,
				},
			}
			want := &Reservation{
				ID:                          123,
				Name:                        "rsv",
				SelfLink:                    testLink,
				Zone:                        testZone,
				Status:                      "READY",
				DeploymentType:              "DENSE",
				SpecificReservationRequired: true,
				ResourcePolicies:            map[string]string{"policy": testPolicy},
				ResourceStatus:              &ResourceStatus{ReservationBlockCount: 2},
				SpecificReservation: &SpecificReservation{
					InstanceProperties: wantProps,
					AvailableCount:     tc.wantAvailable,
				},
			}

			if diff := cmp.Diff(want, FromV1(v1)); diff != "" {
				t.Errorf("FromV1() diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestFromV1WithoutResourceStatus(t *testing.T) {
	got := FromV1(&gceapi.Reservation{Name: "rsv"})
	if got.ResourceStatus != nil {
		t.Errorf("FromV1().ResourceStatus = %v, want nil", got.ResourceStatus)
	}
}

func TestFromV1InstanceProperties(t *testing.T) {
	v1 := &gceapi.Reservation{
		SpecificReservation: &gceapi.AllocationSpecificSKUReservation{
			InstanceProperties: &gceapi.AllocationSpecificSKUAllocationReservedInstanceProperties{
				MachineType:    "a3-highgpu-8g",
				MinCpuPlatform: "Intel Sapphire Rapids",
				GuestAccelerators: []*gceapi.AcceleratorConfig{
					nil,
					{AcceleratorType: "nvidia-h100-80gb", AcceleratorCount: 8},
				},
				LocalSsds: []*gceapi.AllocationSpecificSKUAllocationAllocatedInstancePropertiesReservedDisk{
					nil,
					{DiskSizeGb: 375, Interface: "NVME"},
					{DiskSizeGb: 375, Interface: "SCSI"},
				},
			},
		},
	}
	want := &InstanceProperties{
		MachineType:       "a3-highgpu-8g",
		MinCPUPlatform:    "Intel Sapphire Rapids",
		GuestAccelerators: []AcceleratorResource{{AcceleratorType: "nvidia-h100-80gb", AcceleratorCount: 8}},
		LocalSSDs:         []LocalSSD{{DiskSizeGB: 375, Interface: "NVME"}, {DiskSizeGB: 375, Interface: "SCSI"}},
	}

	if diff := cmp.Diff(want, FromV1(v1).SpecificReservation.InstanceProperties); diff != "" {
		t.Errorf("FromV1().SpecificReservation.InstanceProperties diff (-want +got):\n%s", diff)
	}
}

func TestFromV1AggregateReservation(t *testing.T) {
	for _, tc := range []struct {
		name          string
		reserved      []*gceapi.AllocationAggregateReservationReservedResourceInfo
		inUse         []*gceapi.AllocationAggregateReservationReservedResourceInfo
		wantAvailable []AcceleratorResource
	}{
		{
			name:          "no_reserved_resources",
			wantAvailable: nil,
		},
		{
			name:          "unused",
			reserved:      []*gceapi.AllocationAggregateReservationReservedResourceInfo{v1Resource(ct5lp, 16)},
			wantAvailable: []AcceleratorResource{{AcceleratorType: ct5lp, AcceleratorCount: 16}},
		},
		{
			name:          "partially_used",
			reserved:      []*gceapi.AllocationAggregateReservationReservedResourceInfo{v1Resource(ct5lp, 16)},
			inUse:         []*gceapi.AllocationAggregateReservationReservedResourceInfo{v1Resource(ct5lp, 4)},
			wantAvailable: []AcceleratorResource{{AcceleratorType: ct5lp, AcceleratorCount: 12}},
		},
		{
			name:          "fully_used",
			reserved:      []*gceapi.AllocationAggregateReservationReservedResourceInfo{v1Resource(ct5lp, 16)},
			inUse:         []*gceapi.AllocationAggregateReservationReservedResourceInfo{v1Resource(ct5lp, 16)},
			wantAvailable: []AcceleratorResource{{AcceleratorType: ct5lp, AcceleratorCount: 0}},
		},
		{
			name:          "overused_is_floored_at_zero",
			reserved:      []*gceapi.AllocationAggregateReservationReservedResourceInfo{v1Resource(ct5lp, 16)},
			inUse:         []*gceapi.AllocationAggregateReservationReservedResourceInfo{v1Resource(ct5lp, 20)},
			wantAvailable: []AcceleratorResource{{AcceleratorType: ct5lp, AcceleratorCount: 0}},
		},
		{
			// Each entry is a oneof of vCPU, memory or accelerator. Non-accelerator entries have no
			// Accelerator set in the Go SDK.
			name:          "non-accelerator_and_nil_entries_are_skipped",
			reserved:      []*gceapi.AllocationAggregateReservationReservedResourceInfo{nil, {}, v1Resource(ct5lp, 16)},
			inUse:         []*gceapi.AllocationAggregateReservationReservedResourceInfo{nil, {}, v1Resource(ct5lp, 1)},
			wantAvailable: []AcceleratorResource{{AcceleratorType: ct5lp, AcceleratorCount: 15}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v1 := &gceapi.Reservation{
				Name: "rsv",
				AggregateReservation: &gceapi.AllocationAggregateReservation{
					VmFamily:          ct5lpVM,
					WorkloadType:      "BATCH",
					ReservedResources: tc.reserved,
					InUseResources:    tc.inUse,
				},
			}
			want := &Reservation{
				Name: "rsv",
				AggregateReservation: &AggregateReservation{
					VMFamily:           ct5lpVM,
					WorkloadType:       "BATCH",
					AvailableResources: tc.wantAvailable,
				},
			}

			if diff := cmp.Diff(want, FromV1(v1)); diff != "" {
				t.Errorf("FromV1() diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestFromV1List(t *testing.T) {
	got := FromV1List([]*gceapi.Reservation{{Name: "a"}, nil, {Name: "b"}})
	want := []*Reservation{{Name: "a"}, {Name: "b"}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("FromV1List() diff (-want +got):\n%s", diff)
	}
	if got := FromV1List(nil); got != nil {
		t.Errorf("FromV1List(nil) = %v, want nil", got)
	}
}

func TestCloneV1Nil(t *testing.T) {
	if got := CloneV1(nil); got != nil {
		t.Errorf("CloneV1(nil) = %v, want nil", got)
	}
}

func TestCloneV1DoesNotShareUsage(t *testing.T) {
	specific := &gceapi.Reservation{
		Name:                "specific",
		SpecificReservation: &gceapi.AllocationSpecificSKUReservation{Count: 5, InUseCount: 1},
	}
	aggregate := &gceapi.Reservation{
		Name: "aggregate",
		AggregateReservation: &gceapi.AllocationAggregateReservation{
			ReservedResources: []*gceapi.AllocationAggregateReservationReservedResourceInfo{nil, v1Resource(ct5lp, 16)},
			InUseResources:    []*gceapi.AllocationAggregateReservationReservedResourceInfo{v1Resource(ct5lp, 4)},
		},
	}

	gotSpecific := CloneV1(specific)
	gotAggregate := CloneV1(aggregate)
	if diff := cmp.Diff(specific, gotSpecific); diff != "" {
		t.Errorf("CloneV1(specific) mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(aggregate, gotAggregate); diff != "" {
		t.Errorf("CloneV1(aggregate) mismatch (-want +got):\n%s", diff)
	}

	gotSpecific.SpecificReservation.InUseCount = 5
	gotAggregate.AggregateReservation.InUseResources[0].Accelerator.AcceleratorCount = 16
	if specific.SpecificReservation.InUseCount != 1 {
		t.Errorf("modifying the clone changed the original InUseCount to %d, want 1", specific.SpecificReservation.InUseCount)
	}
	if got := aggregate.AggregateReservation.InUseResources[0].Accelerator.AcceleratorCount; got != 4 {
		t.Errorf("modifying the clone changed the original in-use accelerators to %d, want 4", got)
	}
}
