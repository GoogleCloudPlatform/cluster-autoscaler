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

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/reservation"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"

	gce_api "google.golang.org/api/compute/v1"
	gke_api "google.golang.org/api/container/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
)

func BuildReservationAccelerators(acceleratorType string, count machinetypes.PhysicalGpuCount) []reservation.AcceleratorResource {
	return []reservation.AcceleratorResource{{AcceleratorType: acceleratorType, AcceleratorCount: int64(count)}}
}

func BuildReservationLocalSSDs(localSSDType string, size int) []reservation.LocalSSD {
	return []reservation.LocalSSD{{Interface: localSSDType, DiskSizeGB: int64(size)}}
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

// BuildSingleMachineReservation returns a specific reservation with one available VM.
func BuildSingleMachineReservation(machineType string, zone string) *reservation.Reservation {
	return BuildAvailableReservation(machineType, zone, 1)
}

func BuildReservationWithSpecificReservationRequired(name string, machineType string, zone string) *reservation.Reservation {
	return &reservation.Reservation{
		Name:                        name,
		Status:                      "READY",
		SpecificReservationRequired: true,
		Zone:                        zone,
		SpecificReservation: &reservation.SpecificReservation{
			InstanceProperties: &reservation.InstanceProperties{MachineType: machineType},
		},
		SelfLink: fmt.Sprintf("%s/%s", zone, name),
	}
}

// BuildAvailableReservation returns a specific reservation, not requiring targeting by name, with
// availableCount VMs available.
func BuildAvailableReservation(machineType, zone string, availableCount int) *reservation.Reservation {
	return &reservation.Reservation{
		Status: "READY",
		Zone:   zone,
		SpecificReservation: &reservation.SpecificReservation{
			InstanceProperties: &reservation.InstanceProperties{MachineType: machineType},
			AvailableCount:     int64(availableCount),
		},
	}
}

// BuildAvailableReservationWithID is BuildAvailableReservation with the given ID.
func BuildAvailableReservationWithID(id uint64, machineType, zone string, availableCount int) *reservation.Reservation {
	rsv := BuildAvailableReservation(machineType, zone, availableCount)
	rsv.ID = id
	return rsv
}

func BuildReservationWithId(id uint64, rsvStatus string, specificReservationRequired bool, zone string,
	machineType string, minCpuPlatform string, accelerators []reservation.AcceleratorResource,
	localSSDs []reservation.LocalSSD, project string, name string) *reservation.Reservation {
	rsv := BuildReservation(rsvStatus, specificReservationRequired, zone, machineType, minCpuPlatform, accelerators, localSSDs)
	rsv.SelfLink = fmt.Sprintf("projects/%s/reservations/%s", project, name)
	rsv.Name = name
	rsv.ID = id
	return rsv
}

func BuildReservationWithLink(zone string, machineType string, project string, name string) *reservation.Reservation {
	rsv := BuildReservationWithSpecificReservationRequired(name, machineType, zone)
	rsv.SelfLink = fmt.Sprintf("projects/%s/reservations/%s", project, name)
	rsv.Name = name
	return rsv
}

func BuildReservation(rsvStatus string, specificReservationRequired bool, zone string,
	machineType string, minCpuPlatform string, accelerators []reservation.AcceleratorResource,
	localSSDs []reservation.LocalSSD) *reservation.Reservation {
	return &reservation.Reservation{
		Status:                      rsvStatus,
		Zone:                        "/" + zone,
		SpecificReservationRequired: specificReservationRequired,
		SpecificReservation: &reservation.SpecificReservation{
			InstanceProperties: &reservation.InstanceProperties{
				MachineType:       machineType,
				MinCPUPlatform:    minCpuPlatform,
				GuestAccelerators: accelerators,
				LocalSSDs:         localSSDs,
			},
		},
	}
}

func BuildAggregateReservation(zone string) *reservation.Reservation {
	return &reservation.Reservation{
		Status: "READY",
		Zone:   zone,
		AggregateReservation: &reservation.AggregateReservation{
			VMFamily:     "VM_FAMILY_CLOUD_TPU_POD_SLICE_CT4P",
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

func NewTestingReservationsPuller(localProject string, sharedPrjs []string, rsv []*reservation.Reservation) *gceclient.ReservationsPuller {
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

type ReservationOption func(*reservation.Reservation)

func WithMachine(machineType string) ReservationOption {
	return func(r *reservation.Reservation) {
		if r.SpecificReservation != nil && r.SpecificReservation.InstanceProperties != nil {
			r.SpecificReservation.InstanceProperties.MachineType = machineType
		}
	}
}

func WithId(id uint64) ReservationOption {
	return func(r *reservation.Reservation) {
		r.ID = id
	}
}

func WithSpecificReservationRequired(required bool) ReservationOption {
	return func(r *reservation.Reservation) {
		r.SpecificReservationRequired = required
	}
}

func WithGuestAccelerators(accelerators []reservation.AcceleratorResource) ReservationOption {
	return func(r *reservation.Reservation) {
		if r.SpecificReservation != nil && r.SpecificReservation.InstanceProperties != nil {
			r.SpecificReservation.InstanceProperties.GuestAccelerators = accelerators
		}
	}
}

// WithAggregate turns the reservation into an aggregate ct6e reservation with 8 available chips.
func WithAggregate(zone, workloadType string) ReservationOption {
	return func(r *reservation.Reservation) {
		if workloadType == "" {
			workloadType = tpu.WorkloadTypeUnspecified
		}
		r.SpecificReservation = nil
		r.AggregateReservation = &reservation.AggregateReservation{
			VMFamily:     "VM_FAMILY_CLOUD_TPU_LITE_POD_SLICE_CT6E",
			WorkloadType: workloadType,
			AvailableResources: []reservation.AcceleratorResource{
				{
					AcceleratorType:  fmt.Sprintf("projects/whatever/zones/%s/acceleratorTypes/ct6e", zone),
					AcceleratorCount: 8,
				},
			},
		}
	}
}

func WithProject(project string) ReservationOption {
	return func(r *reservation.Reservation) {
		parts := strings.Split(r.SelfLink, "/")
		name := parts[len(parts)-1]

		zoneParts := strings.Split(r.Zone, "/")
		zone := zoneParts[len(zoneParts)-1]

		r.Zone = fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s", project, zone)
		r.SelfLink = fmt.Sprintf("projects/%s/reservations/%s", project, name)
	}
}

// WithAvailableCount sets the number of VMs that can still be created using a specific reservation.
func WithAvailableCount(availableCount int64) ReservationOption {
	return func(r *reservation.Reservation) {
		r.SpecificReservation.AvailableCount = availableCount
	}
}

func New(name, zone string, opts ...ReservationOption) *reservation.Reservation {
	zoneURL := fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s", DefaultProject, zone)
	r := &reservation.Reservation{
		Name:                        name,
		Status:                      "READY",
		SpecificReservationRequired: true,
		Zone:                        zoneURL,
		SpecificReservation: &reservation.SpecificReservation{
			InstanceProperties: &reservation.InstanceProperties{
				MachineType: DefaultMachineType,
			},
		},
		ID:       nextId(),
		SelfLink: fmt.Sprintf("projects/%s/reservations/%s", DefaultProject, name),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func NewAny(name, zone string, opts ...ReservationOption) *reservation.Reservation {
	opts = append([]ReservationOption{WithSpecificReservationRequired(false)}, opts...)
	return New(name, zone, opts...)
}

// NewTestSubBlockMig builds a *gke.GkeMig with the given spec and optional workload policy
// topology mode (e.g. gceclient.AcceleratorTopologyModeProvisionOnly).
func NewTestSubBlockMig(spec *gkeclient.NodePoolSpec, topologyMode string) *gke.GkeMig {
	if spec != nil && topologyMode != "" {
		cp := *spec
		cp.PlacementGroup.ResourcePolicy = &gceclient.GceResourcePolicy{
			WorkloadPolicy: gceclient.WorkloadPolicy{
				AcceleratorTopologyMode: topologyMode,
			},
		}
		spec = &cp
	}
	return gke.NewTestGkeMigBuilder().SetSpec(spec).Build()
}

// SpecificSubBlockSpec returns a node pool spec with a SPECIFIC reservation affinity to the given values.
func SpecificSubBlockSpec(values ...string) *gkeclient.NodePoolSpec {
	return ReservationAffinitySpec(gkeclient.ReservationAffinitySpecific, values...)
}

// ReservationAffinitySpec returns a node pool spec with the given reservation affinity.
func ReservationAffinitySpec(consumeType string, values ...string) *gkeclient.NodePoolSpec {
	return &gkeclient.NodePoolSpec{
		ReservationAffinity: &gke_api.ReservationAffinity{
			ConsumeReservationType: consumeType,
			Values:                 values,
		},
	}
}

// NewV1 is New returning a GCE API reservation, e.g. for tests that set up the fake GCE client.
// Available resources are returned as reserved resources with nothing in use.
func NewV1(name, zone string, opts ...ReservationOption) *gce_api.Reservation {
	return toV1(New(name, zone, opts...))
}

// NewAnyV1 is NewAny returning a GCE API reservation, see NewV1.
func NewAnyV1(name, zone string, opts ...ReservationOption) *gce_api.Reservation {
	return toV1(NewAny(name, zone, opts...))
}

// toV1 converts a domain reservation to the v1 Reservations API representation for test fixtures.
//
// Available resources are returned as reserved resources with nothing in use, so
// reservation.FromV1(toV1(r)) equals r. Returns nil for nil input.
func toV1(r *reservation.Reservation) *gce_api.Reservation {
	if r == nil {
		return nil
	}
	res := &gce_api.Reservation{
		Id:                          r.ID,
		Name:                        r.Name,
		SelfLink:                    r.SelfLink,
		Zone:                        r.Zone,
		Status:                      r.Status,
		DeploymentType:              r.DeploymentType,
		SpecificReservationRequired: r.SpecificReservationRequired,
		ResourcePolicies:            r.ResourcePolicies,
	}
	if r.ResourceStatus != nil {
		res.ResourceStatus = &gce_api.AllocationResourceStatus{ReservationBlockCount: r.ResourceStatus.ReservationBlockCount}
	}
	if r.SpecificReservation != nil {
		res.SpecificReservation = &gce_api.AllocationSpecificSKUReservation{
			InstanceProperties: instancePropertiesToV1(r.SpecificReservation.InstanceProperties),
			Count:              r.SpecificReservation.AvailableCount,
		}
	}
	if r.AggregateReservation != nil {
		res.AggregateReservation = &gce_api.AllocationAggregateReservation{
			VmFamily:     r.AggregateReservation.VMFamily,
			WorkloadType: r.AggregateReservation.WorkloadType,
		}
		for _, a := range r.AggregateReservation.AvailableResources {
			res.AggregateReservation.ReservedResources = append(res.AggregateReservation.ReservedResources, &gce_api.AllocationAggregateReservationReservedResourceInfo{
				Accelerator: &gce_api.AllocationAggregateReservationReservedResourceInfoAccelerator{
					AcceleratorType:  a.AcceleratorType,
					AcceleratorCount: a.AcceleratorCount,
				},
			})
		}
	}
	return res
}

func instancePropertiesToV1(p *reservation.InstanceProperties) *gce_api.AllocationSpecificSKUAllocationReservedInstanceProperties {
	if p == nil {
		return nil
	}
	res := &gce_api.AllocationSpecificSKUAllocationReservedInstanceProperties{
		MachineType:    p.MachineType,
		MinCpuPlatform: p.MinCPUPlatform,
	}
	for _, a := range p.GuestAccelerators {
		res.GuestAccelerators = append(res.GuestAccelerators, &gce_api.AcceleratorConfig{
			AcceleratorType:  a.AcceleratorType,
			AcceleratorCount: a.AcceleratorCount,
		})
	}
	for _, d := range p.LocalSSDs {
		res.LocalSsds = append(res.LocalSsds, &gce_api.AllocationSpecificSKUAllocationAllocatedInstancePropertiesReservedDisk{
			DiskSizeGb: d.DiskSizeGB,
			Interface:  d.Interface,
		})
	}
	return res
}
