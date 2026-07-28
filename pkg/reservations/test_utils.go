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
	"strings"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"

	gce_api "google.golang.org/api/compute/v1"
	gke_api "google.golang.org/api/container/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
)

func BuildReservationAccelerators(acceleratorType string, count machinetypes.PhysicalGpuCount) []*gce_api.AcceleratorConfig {
	return []*gce_api.AcceleratorConfig{
		{
			AcceleratorType:  acceleratorType,
			AcceleratorCount: int64(count),
		},
	}
}

func BuildReservationLocalSSDs(localSSDType string, size int) []*gce_api.AllocationSpecificSKUAllocationAllocatedInstancePropertiesReservedDisk {
	return []*gce_api.AllocationSpecificSKUAllocationAllocatedInstancePropertiesReservedDisk{
		{
			Interface:  localSSDType,
			DiskSizeGb: int64(size),
		},
	}
}

func BuildNodePoolSpecAccelerators(acceleratorType string, count machinetypes.PhysicalGpuCount) []*gke_api.AcceleratorConfig {
	return []*gke_api.AcceleratorConfig{
		{
			AcceleratorType:  acceleratorType,
			AcceleratorCount: int64(count),
		},
	}
}

func BuildNodePoolSpecLocalSSDs(localSSDType string, count int) *gkeclient.LocalSSDConfig {
	if localSSDType == ssdSCSI {
		return &gkeclient.LocalSSDConfig{
			LocalSsdCount: int64(count),
		}
	}
	return &gkeclient.LocalSSDConfig{
		LocalNvmeSsdBlockConfig: &gke_api.LocalNvmeSsdBlockConfig{
			LocalSsdCount: int64(count),
		},
	}
}

func BuildSingleMachineReservation(machineType string, zone string) *gce_api.Reservation {
	return BuildMultipleMachineReservation(machineType, zone, 0, 1)
}

func BuildReservationWithSpecificReservationRequired(name string, machineType string, zone string) *gce_api.Reservation {
	return &gce_api.Reservation{
		Name:                        name,
		Status:                      "READY",
		SpecificReservationRequired: true,
		Zone:                        zone,
		SpecificReservation: &gce_api.AllocationSpecificSKUReservation{
			InstanceProperties: &gce_api.AllocationSpecificSKUAllocationReservedInstanceProperties{
				MachineType: machineType,
			},
		},
		SelfLink: fmt.Sprintf("%s/%s", zone, name),
	}
}

func BuildMultipleMachineReservation(machineType, zone string, inUseCount, count int) *gce_api.Reservation {
	return &gce_api.Reservation{
		Status:                      "READY",
		SpecificReservationRequired: false,
		Zone:                        zone,
		SpecificReservation: &gce_api.AllocationSpecificSKUReservation{
			InstanceProperties: &gce_api.AllocationSpecificSKUAllocationReservedInstanceProperties{
				MachineType: machineType,
			},
			InUseCount: int64(inUseCount),
			Count:      int64(count),
		},
	}
}

func BuildMultipleMachineReservationWithId(id, inUseCount, count int, machineType, zone string) *gce_api.Reservation {
	rsv := BuildMultipleMachineReservation(machineType, zone, inUseCount, count)
	rsv.Id = uint64(id)
	return rsv
}

func BuildReservationWithId(id uint64, rsvStatus string, specificReservationRequired bool, zone string,
	machineType string, minCpuPlatform string, accelerators []*gce_api.AcceleratorConfig,
	localSSDs []*gce_api.AllocationSpecificSKUAllocationAllocatedInstancePropertiesReservedDisk, project string, name string) *gce_api.Reservation {
	rsv := BuildReservation(rsvStatus, specificReservationRequired, zone, machineType, minCpuPlatform, accelerators, localSSDs)
	rsv.SelfLink = fmt.Sprintf("projects/%s/reservations/%s", project, name)
	rsv.Name = name
	rsv.Id = id
	return rsv
}

func BuildReservationWithLink(zone string, machineType string, project string, name string) *gce_api.Reservation {
	rsv := BuildReservationWithSpecificReservationRequired(name, machineType, zone)
	rsv.SelfLink = fmt.Sprintf("projects/%s/reservations/%s", project, name)
	rsv.Name = name
	return rsv
}

func BuildReservation(rsvStatus string, specificReservationRequired bool, zone string,
	machineType string, minCpuPlatform string, accelerators []*gce_api.AcceleratorConfig,
	localSSDs []*gce_api.AllocationSpecificSKUAllocationAllocatedInstancePropertiesReservedDisk) *gce_api.Reservation {

	return &gce_api.Reservation{
		Status:                      rsvStatus,
		Zone:                        "/" + zone,
		SpecificReservationRequired: specificReservationRequired,
		SpecificReservation: &gce_api.AllocationSpecificSKUReservation{
			InstanceProperties: &gce_api.AllocationSpecificSKUAllocationReservedInstanceProperties{
				MachineType:       machineType,
				MinCpuPlatform:    minCpuPlatform,
				GuestAccelerators: accelerators,
				LocalSsds:         localSSDs,
			},
		},
	}
}

func BuildAggregateReservation(zone string) *gce_api.Reservation {
	return &gce_api.Reservation{
		Status: "READY",
		Zone:   zone,
		AggregateReservation: &gce_api.AllocationAggregateReservation{
			VmFamily:     "VM_FAMILY_CLOUD_TPU_POD_SLICE_CT4P",
			WorkloadType: "BATCH",
		},
	}
}

func BuildNodePoolSpecIfMigExists(
	machineType string,
	minCpuPlatform string,
	accelerators []*gke_api.AcceleratorConfig,
	localSSDConfig *gkeclient.LocalSSDConfig,
) gkeclient.NodePoolSpec {
	if localSSDConfig == nil {
		localSSDConfig = &gkeclient.LocalSSDConfig{
			LocalSsdCount: 0,
		}
	}
	return gkeclient.NodePoolSpec{
		MachineType:    machineType,
		MinCpuPlatform: minCpuPlatform,
		Accelerators:   accelerators,
		LocalSSDConfig: localSSDConfig,
	}
}

func BuildNodePoolSpecIfMigNotExist(
	machineType string,
	minCpuPlatform string,
	accelerators []*gke_api.AcceleratorConfig,
	localSSDConfig *gkeclient.LocalSSDConfig,
) gkeclient.NodePoolSpec {

	return gkeclient.NodePoolSpec{
		MachineType:    machineType,
		MinCpuPlatform: minCpuPlatform,
		LocalSSDConfig: localSSDConfig,
		Accelerators:   accelerators,
	}
}

func BuildNodePoolSpecWithReservationAffinity(reservationAffinityType string, reservationAffinityName string, machineType string) gkeclient.NodePoolSpec {
	return gkeclient.NodePoolSpec{
		MachineType: machineType,
		ReservationAffinity: &gke_api.ReservationAffinity{
			ConsumeReservationType: reservationAffinityType,
			Key:                    "",
			Values:                 []string{reservationAffinityName},
		},
	}
}

type FakeBlocksPullerProvider struct {
	reservationBlocks map[gceclient.ReservationRef][]*gceclient.GceReservationBlock
	errors            map[gceclient.ReservationRef]error
}

func NewFakeBlocksPullerProvider(rbs map[gceclient.ReservationRef][]*gceclient.GceReservationBlock,
	err map[gceclient.ReservationRef]error) *FakeBlocksPullerProvider {
	return &FakeBlocksPullerProvider{
		reservationBlocks: rbs,
		errors:            err,
	}
}

func (f *FakeBlocksPullerProvider) GetReservationBlocksInReservation(reservationRef gceclient.ReservationRef) ([]*gceclient.GceReservationBlock, error) {
	if err, ok := f.errors[reservationRef]; ok {
		return nil, err
	}
	if blockMap, ok := f.reservationBlocks[reservationRef]; ok {
		return blockMap, nil
	}
	return nil, nil
}

func (f *FakeBlocksPullerProvider) GetReservationSubBlocksInReservationBlock(reservationRef gceclient.ReservationRef) ([]*gceclient.GceReservationSubBlock, error) {
	return nil, nil
}

func NewTestingReservationsPuller(localProject string, sharedPrjs []string, rsv []*gce_api.Reservation) *gceclient.ReservationsPuller {
	mGceClient := gceclient.BuildAutoscalingInternalGceClientMock().
		WithFetchZones(func(region string) ([]string, error) { return []string{"us-central1-c"}, nil })
	puller, _ := gceclient.NewReservationsPuller(mGceClient, nil, nil, localProject, false, "us-central1")
	if len(sharedPrjs) != 0 {
		for _, project := range sharedPrjs {
			puller.AddProject(project)
		}
	}
	puller.SetReservations(rsv)
	return puller
}

func BuildSingleReservationBlock(name string, count int64, inUseCount int64, zone string) *gceclient.GceReservationBlock {
	return &gceclient.GceReservationBlock{
		Name:       name,
		Count:      count,
		InUseCount: inUseCount,
		Status:     "READY",
		Zone:       zone,
	}
}

func ParseReservationRef(key string) gceclient.ReservationRef {
	rk := gceclient.ReservationRef{}
	parts := strings.Split(key, "/")
	if len(parts) == 3 {
		rk.Project = parts[0]
		rk.Zone = parts[1]
		rk.Name = parts[2]
	}
	return rk
}

type fakeSubBlockProvider struct {
	subBlocks map[gceclient.ReservationRef]map[string][]*gceclient.GceReservationSubBlock
	errors    map[gceclient.ReservationRef]error
}

func (f *fakeSubBlockProvider) GetReservationBlocksInReservation(ref gceclient.ReservationRef) ([]*gceclient.GceReservationBlock, error) {
	return nil, nil
}

func (f *fakeSubBlockProvider) GetReservationSubBlocksInReservationBlock(ref gceclient.ReservationRef) ([]*gceclient.GceReservationSubBlock, error) {
	if err, ok := f.errors[ref]; ok {
		return nil, err
	}
	r := gceclient.ReservationRef{Project: ref.Project, Zone: ref.Zone, Name: ref.Name}
	if blocks, ok := f.subBlocks[r]; ok {
		if subBlocks, ok := blocks[ref.BlockName]; ok {
			return subBlocks, nil
		}
	}
	return nil, nil
}

const (
	DefaultProject         = "gke-test-project"
	DefaultReservationName = "res-group"
	DefaultMachineType     = "n2-standard-2"
)

var autoIncrementId uint64 = 1

func nextId() uint64 {
	id := autoIncrementId
	autoIncrementId++
	return id
}

type ReservationOption func(*gce_api.Reservation)

func WithMachine(machineType string) ReservationOption {
	return func(r *gce_api.Reservation) {
		if r.SpecificReservation != nil && r.SpecificReservation.InstanceProperties != nil {
			r.SpecificReservation.InstanceProperties.MachineType = machineType
		}
	}
}

func WithId(id uint64) ReservationOption {
	return func(r *gce_api.Reservation) {
		r.Id = id
	}
}

func WithSpecificReservationRequired(required bool) ReservationOption {
	return func(r *gce_api.Reservation) {
		r.SpecificReservationRequired = required
	}
}

func WithGuestAccelerators(accelerators []*gce_api.AcceleratorConfig) ReservationOption {
	return func(r *gce_api.Reservation) {
		if r.SpecificReservation != nil && r.SpecificReservation.InstanceProperties != nil {
			r.SpecificReservation.InstanceProperties.GuestAccelerators = accelerators
		}
	}
}

func WithAggregate(zone, workloadType string) ReservationOption {
	return func(r *gce_api.Reservation) {
		if workloadType == "" {
			workloadType = tpu.WorkloadTypeUnspecified
		}
		r.SpecificReservation = nil
		r.AggregateReservation = &gce_api.AllocationAggregateReservation{
			VmFamily:     "VM_FAMILY_CLOUD_TPU_LITE_POD_SLICE_CT6E",
			WorkloadType: workloadType,
			ReservedResources: []*gce_api.AllocationAggregateReservationReservedResourceInfo{
				{
					Accelerator: &gce_api.AllocationAggregateReservationReservedResourceInfoAccelerator{
						AcceleratorType:  fmt.Sprintf("projects/whatever/zones/%s/acceleratorTypes/ct6e", zone),
						AcceleratorCount: 8,
					},
				},
			},
		}
	}
}

func WithProject(project string) ReservationOption {
	return func(r *gce_api.Reservation) {
		parts := strings.Split(r.SelfLink, "/")
		name := parts[len(parts)-1]

		zoneParts := strings.Split(r.Zone, "/")
		zone := zoneParts[len(zoneParts)-1]

		r.Zone = fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s", project, zone)
		r.SelfLink = fmt.Sprintf("projects/%s/reservations/%s", project, name)
	}
}

func WithCounts(inUseCount, count int64) ReservationOption {
	return func(r *gce_api.Reservation) {
		r.SpecificReservation.InUseCount = inUseCount
		r.SpecificReservation.Count = count
	}
}

func New(name, zone string, opts ...ReservationOption) *gce_api.Reservation {
	zoneURL := fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s", DefaultProject, zone)
	r := &gce_api.Reservation{
		Name:                        name,
		Status:                      "READY",
		SpecificReservationRequired: true,
		Zone:                        zoneURL,
		SpecificReservation: &gce_api.AllocationSpecificSKUReservation{
			InstanceProperties: &gce_api.AllocationSpecificSKUAllocationReservedInstanceProperties{
				MachineType: DefaultMachineType,
			},
		},
		Id:       nextId(),
		SelfLink: fmt.Sprintf("projects/%s/reservations/%s", DefaultProject, name),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func NewAny(name, zone string, opts ...ReservationOption) *gce_api.Reservation {
	opts = append([]ReservationOption{WithSpecificReservationRequired(false)}, opts...)
	return New(name, zone, opts...)
}
