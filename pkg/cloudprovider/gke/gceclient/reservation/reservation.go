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

// Package reservation contains the cluster autoscaler domain object representing a GCE reservation.
//
// Reservations can be fetched from two GCE APIs which carry different data:
//   - the Reservations API (v1) returns reserved (total) and in-use resources,
//   - the ListConsumableReservations API (beta) returns only the available resources.
//
// Reservation normalizes both to available resources at construction time, so consumers don't
// need to know which API a reservation was fetched from.
package reservation

import (
	gceapi "google.golang.org/api/compute/v1"
)

// Reservation is a GKE cluster autoscaler domain object abstracting a GCE reservation.
type Reservation struct {
	ID       uint64
	Name     string
	SelfLink string
	// Zone is the URL or name of the zone the reservation is in.
	Zone   string
	Status string
	// DeploymentType is the GCE deployment type, e.g. "DENSE".
	DeploymentType              string
	SpecificReservationRequired bool
	// ResourcePolicies maps a policy name to the URL of a resource policy (e.g. a placement policy).
	ResourcePolicies map[string]string
	// ResourceStatus is nil if GCE didn't report the reservation's resource status.
	ResourceStatus *ResourceStatus

	// At most one of SpecificReservation and AggregateReservation is set.
	SpecificReservation  *SpecificReservation
	AggregateReservation *AggregateReservation
}

// ResourceStatus is the status of the resources of a reservation.
type ResourceStatus struct {
	ReservationBlockCount int64
}

// SpecificReservation is a domain object abstracting a GCE specific SKU reservation.
type SpecificReservation struct {
	InstanceProperties *InstanceProperties
	// AvailableCount is the number of VMs that can still be created using the reservation.
	AvailableCount int64
}

// InstanceProperties are the properties of the VMs reserved by a specific reservation.
type InstanceProperties struct {
	MachineType       string
	MinCPUPlatform    string
	GuestAccelerators []AcceleratorResource
	LocalSSDs         []LocalSSD
}

// LocalSSD is a local SSD attached to the reserved VMs.
type LocalSSD struct {
	DiskSizeGB int64
	// Interface is the disk interface, e.g. "NVME" or "SCSI".
	Interface string
}

// AggregateReservation is a domain object abstracting a GCE aggregate reservation.
type AggregateReservation struct {
	VMFamily     string
	WorkloadType string
	// AvailableResources are the resources that can still be used by new VMs.
	AvailableResources []AcceleratorResource
}

// AcceleratorResource is an amount of accelerators of a given type.
type AcceleratorResource struct {
	// AcceleratorType is the full or partial URL of the accelerator type, e.g. "projects/p/zones/z/acceleratorTypes/ct5lp".
	AcceleratorType string
	// AcceleratorCount is the number of accelerators.
	AcceleratorCount int64
}

// FromV1 converts a reservation returned by the v1 Reservations API.
//
// Available resources are computed as reserved resources minus in-use resources, floored at 0.
// Returns nil for nil input.
func FromV1(r *gceapi.Reservation) *Reservation {
	if r == nil {
		return nil
	}
	res := &Reservation{
		ID:                          r.Id,
		Name:                        r.Name,
		SelfLink:                    r.SelfLink,
		Zone:                        r.Zone,
		Status:                      r.Status,
		DeploymentType:              r.DeploymentType,
		SpecificReservationRequired: r.SpecificReservationRequired,
		ResourcePolicies:            r.ResourcePolicies,
	}
	if r.ResourceStatus != nil {
		res.ResourceStatus = &ResourceStatus{ReservationBlockCount: r.ResourceStatus.ReservationBlockCount}
	}
	if r.SpecificReservation != nil {
		res.SpecificReservation = &SpecificReservation{
			InstanceProperties: instancePropertiesFromV1(r.SpecificReservation.InstanceProperties),
			AvailableCount:     max(0, r.SpecificReservation.Count-r.SpecificReservation.InUseCount),
		}
	}
	if r.AggregateReservation != nil {
		res.AggregateReservation = &AggregateReservation{
			VMFamily:           r.AggregateReservation.VmFamily,
			WorkloadType:       r.AggregateReservation.WorkloadType,
			AvailableResources: subtractResources(r.AggregateReservation.ReservedResources, r.AggregateReservation.InUseResources),
		}
	}
	return res
}

// FromV1List converts reservations returned by the v1 Reservations API using FromV1. Nil entries
// are skipped.
func FromV1List(rs []*gceapi.Reservation) []*Reservation {
	if len(rs) == 0 {
		return nil
	}
	result := make([]*Reservation, 0, len(rs))
	for _, r := range rs {
		if r == nil {
			continue
		}
		result = append(result, FromV1(r))
	}
	return result
}

// CloneV1 returns a copy of r whose usage counters (SpecificReservation.InUseCount,
// AggregateReservation.InUseResources) can be changed without affecting r. Other
// nested fields are shared with r.
func CloneV1(r *gceapi.Reservation) *gceapi.Reservation {
	if r == nil {
		return nil
	}
	c := *r
	if r.SpecificReservation != nil {
		specific := *r.SpecificReservation
		c.SpecificReservation = &specific
	}
	if r.AggregateReservation != nil {
		aggregate := *r.AggregateReservation
		aggregate.ReservedResources = cloneResourceInfos(r.AggregateReservation.ReservedResources)
		aggregate.InUseResources = cloneResourceInfos(r.AggregateReservation.InUseResources)
		c.AggregateReservation = &aggregate
	}
	return &c
}

func cloneResourceInfos(infos []*gceapi.AllocationAggregateReservationReservedResourceInfo) []*gceapi.AllocationAggregateReservationReservedResourceInfo {
	if infos == nil {
		return nil
	}
	results := make([]*gceapi.AllocationAggregateReservationReservedResourceInfo, 0, len(infos))
	for _, info := range infos {
		if info == nil {
			results = append(results, nil)
			continue
		}
		c := *info
		if info.Accelerator != nil {
			accelerator := *info.Accelerator
			c.Accelerator = &accelerator
		}
		results = append(results, &c)
	}
	return results
}

// subtractResources returns reserved minus inUse accelerators, floored at 0. Returns nil if no
// accelerators are reserved.
//
// Each resource entry holds exactly one of vCPU, memory or accelerator, and all accelerators of an
// aggregate reservation are of the single type matching its VM family. Non-accelerator entries are
// skipped.
func subtractResources(reserved, inUse []*gceapi.AllocationAggregateReservationReservedResourceInfo) []AcceleratorResource {
	acceleratorType, reservedCount, found := sumAccelerators(reserved)
	if !found {
		return nil
	}
	_, inUseCount, _ := sumAccelerators(inUse)
	return []AcceleratorResource{{
		AcceleratorType:  acceleratorType,
		AcceleratorCount: max(0, reservedCount-inUseCount),
	}}
}

// sumAccelerators returns the accelerator type and the total number of accelerators in resources.
// found is false if resources contain no accelerator entries.
func sumAccelerators(resources []*gceapi.AllocationAggregateReservationReservedResourceInfo) (acceleratorType string, count int64, found bool) {
	for _, r := range resources {
		if r == nil || r.Accelerator == nil {
			continue
		}
		if !found {
			acceleratorType = r.Accelerator.AcceleratorType
			found = true
		}
		count += r.Accelerator.AcceleratorCount
	}
	return acceleratorType, count, found
}

func instancePropertiesFromV1(p *gceapi.AllocationSpecificSKUAllocationReservedInstanceProperties) *InstanceProperties {
	if p == nil {
		return nil
	}
	props := &InstanceProperties{
		MachineType:    p.MachineType,
		MinCPUPlatform: p.MinCpuPlatform,
	}
	for _, a := range p.GuestAccelerators {
		if a != nil {
			props.GuestAccelerators = append(props.GuestAccelerators, AcceleratorResource{AcceleratorType: a.AcceleratorType, AcceleratorCount: a.AcceleratorCount})
		}
	}
	for _, d := range p.LocalSsds {
		if d != nil {
			props.LocalSSDs = append(props.LocalSSDs, LocalSSD{DiskSizeGB: d.DiskSizeGb, Interface: d.Interface})
		}
	}
	return props
}
