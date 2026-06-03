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

	gce_api "google.golang.org/api/compute/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/klog/v2"
)

const (
	ssdNVME = "NVME"
	ssdSCSI = "SCSI"
)

const (
	ZoneNotMatching                          = "zones mismatch, requested %s, reservation at %s"
	MachineTypeNotMatching                   = "machine type mismatch, requested %s, reservation has %s"
	MinCpuPlatformNotMatching                = "min cpu platform mismatch, requested %s, reservation has %s"
	AcceleratorNotMatching                   = "accelerator mismatch, requested %d chips of %s, reservation has %d chips of %s"
	AcceleratorMissing                       = "accelerator mismatch, requested %d chips of %s, but it's missing from reservation"
	AcceleratorNotRequested                  = "accelerator mismatch, reservation has %d chips of %s, but it's not requested"
	LocalSsdNotMatching                      = "local SSD mismatch, requested %d GB %s SSD, reservation has %d GB %s SSD"
	LocalSsdMissing                          = "local SSD mismatch, requested %d GB %s SSD, but it's missing from reservation"
	LocalSsdTypeNotSupported                 = "reservation local SSD interface is not supported: %s"
	TpuRequestNotSupported                   = "requested TPU is not supported: %s"
	MalformedTpuTopology                     = "requested TPU topology is malformed: %s"
	MultiHostTPURequestedSingleHostAvailable = "requested multihost TPU, reservation has singlehost"
	SingleHostTPURequestedMultihostAvailable = "requested singlehost TPU, reservation has multihost"
	TpuAcceleratorMissing                    = "requested TPU accelerator %s, but it's missing from reservation"
)

// MatchingUnusedReservations sums up all the unused reservations that match the given node group
func MatchingUnusedReservations(provider machineConfigProvider, nodegroup cloudprovider.NodeGroup, reservations []*gce_api.Reservation, localSSDDiskSizeProvider localssdsize.LocalSSDSizeProvider) int {
	availableCount := 0
	for _, rsv := range reservations {
		if reservationMatch(provider, nodegroup, rsv, localSSDDiskSizeProvider) {
			availableCount += int(rsv.SpecificReservation.Count - rsv.SpecificReservation.InUseCount)
		}
	}

	if availableCount > 0 {
		klog.V(4).Infof("MatchingUnusedReservations found: nodegroup=%s, availableCount=%d", nodegroup.Id(), availableCount)
	}

	return availableCount
}

// reservationMatch checks if the nodegroup matches reservation.
// https://cloud.google.com/compute/docs/instances/reservations-overview#vm-properties
func reservationMatch(provider machineConfigProvider, nodegroup cloudprovider.NodeGroup, rsv *gce_api.Reservation, localSSDDiskSizeProvider localssdsize.LocalSSDSizeProvider) bool {
	mig, ok := nodegroup.(*gke.GkeMig)
	if !ok {
		klog.Errorf("Nodegroup - %v cannot be converted to MIG", nodegroup.Debug())
		return false
	}

	// Check if the reservation can be used by the mig.
	if !gceclient.IsReservationUsable(rsv, false) || !mig.IsReservationCompatible(rsv) {
		return false
	}

	nodeShape := NodeShape{
		MachineType:    mig.Spec().MachineType,
		MinCpuPlatform: mig.Spec().MinCpuPlatform,
		Accelerators:   map[string]machinetypes.PhysicalGpuCount{},
		LocalSSDSizes:  map[string]int64{},
		Zone:           mig.GceRef().Zone,
	}

	for _, a := range mig.Spec().Accelerators {
		nodeShape.Accelerators[a.AcceleratorType] = machinetypes.PhysicalGpuCount(a.AcceleratorCount)
	}

	localSSDSizeInGiB := int64(localSSDDiskSizeProvider.SSDSizeInGiB(nodeShape.MachineType))
	nodeShape.LocalSSDSizes[ssdSCSI] = int64(mig.GetSCSILLocalSSDCount()) * localSSDSizeInGiB
	nodeShape.LocalSSDSizes[ssdNVME] = int64(mig.GetNVMELocalSSDCount()) * localSSDSizeInGiB

	return MatchSpecificReservationShape(provider, rsv, nodeShape, true)
}

type NodeShape struct {
	MachineType    string
	MinCpuPlatform string
	LocalSSDSizes  map[string]int64
	Accelerators   map[string]machinetypes.PhysicalGpuCount
	Zone           string
}

// MatchSpecificReservationShape determines whether reservation matches specific machine shape including accelerator and local SSD.
func MatchSpecificReservationShape(provider machineConfigProvider, rsv *gce_api.Reservation, nodeShape NodeShape, acceleratorStrictRequests bool) bool {
	noMatchReasons := MatchSpecificReservationShapeWithReasons(provider, rsv, nodeShape, acceleratorStrictRequests)
	klog.V(5).Infof("Not matching node shape %+v against %v reservation: %q", nodeShape, rsv.Name, noMatchReasons)
	return len(noMatchReasons) == 0
}

// MatchSpecificReservationShape determines whether reservation matches specific machine shape including accelerator and local SSD including reasoning when they don't match.
func MatchSpecificReservationShapeWithReasons(provider machineConfigProvider, rsv *gce_api.Reservation, nodeShape NodeShape, acceleratorStrictRequests bool) []string {
	ip := rsv.SpecificReservation.InstanceProperties

	var noMatchReasons []string

	// Compare Zone.
	if rsvZone := gceclient.GetReservationZone(rsv); nodeShape.Zone != rsvZone {
		noMatchReasons = append(noMatchReasons, fmt.Sprintf(ZoneNotMatching, nodeShape.Zone, rsvZone))
	}

	// Compare Machine Type.
	if nodeShape.MachineType != ip.MachineType {
		noMatchReasons = append(noMatchReasons, fmt.Sprintf(MachineTypeNotMatching, nodeShape.MachineType, ip.MachineType))
	}

	// Compare Minimum CPU platform.
	instanceMinCpuPlatform := provider.MachineConfigProvider().GetOrDefaultMinCPUPlatform(nodeShape.MachineType, nodeShape.MinCpuPlatform)
	rsvMinCpuPlatform := provider.MachineConfigProvider().GetOrDefaultMinCPUPlatform(nodeShape.MachineType, ip.MinCpuPlatform)
	if instanceMinCpuPlatform != rsvMinCpuPlatform {
		noMatchReasons = append(noMatchReasons, fmt.Sprintf(MinCpuPlatformNotMatching, instanceMinCpuPlatform, rsvMinCpuPlatform))
	}

	// Compare GPU type and count.
	rsvAccelerators := map[string]machinetypes.PhysicalGpuCount{}
	for _, ga := range ip.GuestAccelerators {
		rsvAccelerators[ga.AcceleratorType] = machinetypes.PhysicalGpuCount(ga.AcceleratorCount)
	}

	for acceleratorType, acceleratorCount := range nodeShape.Accelerators {
		rsvAcceleratorCount, acceleratorAvailable := rsvAccelerators[acceleratorType]
		if !acceleratorAvailable {
			noMatchReasons = append(noMatchReasons, fmt.Sprintf(AcceleratorMissing, acceleratorCount, acceleratorType))
			continue
		}

		if rsvAcceleratorCount != acceleratorCount {
			noMatchReasons = append(noMatchReasons, fmt.Sprintf(AcceleratorNotMatching, acceleratorCount, acceleratorType, rsvAcceleratorCount, acceleratorType))
		}
	}

	// If strict matching enabled - verify that all the resources present in reservation were requested by the node shape
	if acceleratorStrictRequests {
		for acceleratorType, rsvAcceleratorCount := range rsvAccelerators {
			_, acceleratorRequested := nodeShape.Accelerators[acceleratorType]
			if !acceleratorRequested {
				noMatchReasons = append(noMatchReasons, fmt.Sprintf(AcceleratorNotRequested, rsvAcceleratorCount, acceleratorType))
			}
		}
	}

	// Compare Local SSD type and count.
	for localSSDInterface, localSSDSize := range nodeShape.LocalSSDSizes {
		// Size of zero indicates that there's no local SSD
		if localSSDSize == 0 {
			continue
		}

		rsvLocalSSDSize := getTotalLocalSSDSize(ip, localSSDInterface)
		if rsvLocalSSDSize == 0 {
			noMatchReasons = append(noMatchReasons, fmt.Sprintf(LocalSsdMissing, localSSDSize, localSSDInterface))
			continue
		}

		if localSSDSize != rsvLocalSSDSize {
			noMatchReasons = append(noMatchReasons, fmt.Sprintf(LocalSsdNotMatching, localSSDSize, localSSDInterface, rsvLocalSSDSize, localSSDInterface))
		}
	}

	// Prevent usage of unsupported local SSD interfaces
	for _, ssd := range ip.LocalSsds {
		if ssd.Interface != ssdNVME && ssd.Interface != ssdSCSI {
			noMatchReasons = append(noMatchReasons, fmt.Sprintf(LocalSsdTypeNotSupported, ssd.Interface))
		}
	}

	return noMatchReasons
}

type machineConfigProvider interface {
	MachineConfigProvider() *machinetypes.MachineConfigProvider
}

// MatchAggregateReservationShapeWithReasons determines whether aggregate reservation is matching
// a given TPU shape while including reasoning when they don't match.
func MatchAggregateReservationShapeWithReasons(provider machineConfigProvider, rsv *gce_api.Reservation, tpuType string, tpuTopology string, tpuCount int64) []string {
	tpuRequest := fmt.Sprintf("{%s, %s, %d}", tpuType, tpuTopology, tpuCount)
	if !IsAggregateReservation(rsv) {
		return []string{fmt.Sprintf(TpuAcceleratorMissing, tpuRequest)}
	}

	agg := rsv.AggregateReservation
	if agg.WorkloadType == tpu.WorkloadTypeBatch || agg.WorkloadType == tpu.WorkloadTypeServing {
		isMultihost, err := provider.MachineConfigProvider().IsMultiHostTpuPodslice(tpuType, tpuTopology, tpuCount)
		if err != nil {
			klog.V(5).Infof("Failed to determine TPU topology: %v", err)
			return []string{fmt.Sprintf(TpuRequestNotSupported, tpuRequest)}
		}

		isBatchRsv := rsv.AggregateReservation.WorkloadType == tpu.WorkloadTypeBatch
		if isMultihost && !isBatchRsv {
			return []string{MultiHostTPURequestedSingleHostAvailable}
		}

		if !isMultihost && isBatchRsv {
			return []string{SingleHostTPURequestedMultihostAvailable}
		}
	}

	chipsRequired, err := provider.MachineConfigProvider().NumChipsFromTopology(tpuTopology)
	if err != nil {
		klog.V(5).Infof("Failed to determine amount of chips required for TPU topology (%s): %v", tpuTopology, err)
		return []string{fmt.Sprintf(MalformedTpuTopology, tpuTopology)}
	}

	for _, resource := range agg.ReservedResources {
		rsvMachineFamily, ok := tpu.GetTpuMachineFamilyFromUrl(resource.Accelerator.AcceleratorType)
		if !ok {
			klog.V(5).Infof("Unable to get TPU machine family for accelerator URL: %s", resource.Accelerator.AcceleratorType)
			continue
		}

		// If machine family isn't found, we'll still get a struct with empty name field.
		machineFamily, _ := provider.MachineConfigProvider().MachineFamilyForTpuType(tpuType)
		if rsvMachineFamily != machineFamily.Name() {
			continue
		}

		if chipsRequired > resource.Accelerator.AcceleratorCount {
			continue
		}

		return nil
	}

	return []string{fmt.Sprintf(TpuAcceleratorMissing, tpuRequest)}
}
