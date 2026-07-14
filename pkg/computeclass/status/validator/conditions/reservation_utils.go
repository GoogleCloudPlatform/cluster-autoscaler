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

package conditions

import (
	"errors"
	"fmt"
	"strings"

	gceapiv1 "google.golang.org/api/compute/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"k8s.io/klog/v2"
)

type ValidatorReservationsPuller interface {
	GetLocalReservations() []*gceapiv1.Reservation
	GetReservationsInProject(project string) []*gceapiv1.Reservation
	AddProject(projectID string)
}

// ReservationsCache is a cache intended to store results acquired from reservations puller
// in the form better suitable for validator usage patterns and without sync locks.
type ReservationsCache struct {
	puller ValidatorReservationsPuller
	cache  map[string]map[string]*gceapiv1.Reservation
}

func NewReservationsCache(puller ValidatorReservationsPuller) *ReservationsCache {
	return &ReservationsCache{
		puller: puller,
		cache:  map[string]map[string]*gceapiv1.Reservation{},
	}
}

// PopulateForCrds fetches reservations affected by node config
// rules in the CRDs provided and populates a cache from it.
func (c *ReservationsCache) PopulateForCrds(crds []crd.CRD) {
	c.cache = map[string]map[string]*gceapiv1.Reservation{}
	for _, crd := range crds {
		for _, rule := range crd.Rules() {
			for _, reservation := range rule.Reservations() {
				c.AddCacheForProject(reservation.Project())
			}
		}
	}
}

// AddCacheForProject pulls reservations from specified project into a cache.
// Considers empty project name to be local project.
func (c *ReservationsCache) AddCacheForProject(project string) {
	var reservationsInProject []*gceapiv1.Reservation
	if project != "" {
		c.puller.AddProject(project)
		reservationsInProject = c.puller.GetReservationsInProject(project)
	} else {
		reservationsInProject = c.puller.GetLocalReservations()
	}

	if _, exists := c.cache[project]; !exists {
		c.cache[project] = map[string]*gceapiv1.Reservation{}
	}

	for _, reservation := range reservationsInProject {
		c.cache[project][reservation.Name] = reservation
	}
}

// GetReservation tries to acquire reservation with a given name in a specified project
// in case not found in cache - returns nil.
func (c *ReservationsCache) GetReservation(name, project string) *gceapiv1.Reservation {
	reservationsInProject, exists := c.cache[project]
	if !exists {
		return nil
	}

	reservation, exists := reservationsInProject[name]
	if !exists {
		return nil
	}

	return reservation
}

// matchSpecificReservationOrError matches specific non-aggregate reservation against
// properties specified in a node config rule.
func matchSpecificReservationOrError(
	provider CloudProvider,
	gceReservation *gceapiv1.Reservation,
	rule rules.Rule,
	localSSDSizeProvider localssdsize.LocalSSDSizeProvider,
) error {
	// Not trying to match any reservation, only specific are allowed
	if !gceReservation.SpecificReservationRequired {
		return errors.New("any affinity reservation cannot be consumed")
	}

	if !reservations.IsSpecificReservation(gceReservation) {
		return errors.New("any affinity reservation cannot be consumed")
	}

	if gceReservation.SpecificReservation.InstanceProperties == nil {
		return errors.New("unable to target non-specific reservation")
	}

	// Not able to use reservation unless specifying machine type or machine family
	if rule.MachineFamily() == "" && rule.MachineType() == "" {
		return errors.New("missing machineType and machineFamily, unable to define reservation compatibility")
	}

	rsvProperties := gceReservation.SpecificReservation.InstanceProperties
	var machineType string
	if rule.MachineType() != "" {
		machineType = rule.MachineType()
	} else if rule.MachineFamily() != "" {
		rsvMachineFamily, err := provider.MachineConfigProvider().GetMachineFamilyFromMachineName(rsvProperties.MachineType)
		if err != nil {
			klog.V(4).Infof("Unable get info about reservation machine type %q: %v", rsvProperties.MachineType, err)
			return fmt.Errorf("unsupported machine type: %s", rsvProperties.MachineType)
		}

		if rsvMachineFamily.Name() != strings.ToLower(rule.MachineFamily()) {
			return fmt.Errorf("machine family mismatch: requested %s, reservation has %s", rule.MachineFamily(), rsvMachineFamily.Name())
		}

		// If priority rule has machine family matching - assume reservation machine type is usable
		machineType = rsvProperties.MachineType
	} else {
		// TODO(b/365964197): Remove once steering would support machine families as well
		return fmt.Errorf("missing machineType and machineFamily, unable to define reservation compatibility")
	}

	// Setup local SSDs mapping based on priority rule
	localSSDSizes := map[string]int64{}
	if rule.TotalLSSDCount() != 0 {
		// Only NVME local ssds are supported to be configured for CCCs
		localSSDSizes["NVME"] = int64(localSSDSizeProvider.SSDSizeInGiB(machineType)) * rule.TotalLSSDCount()
	}

	// Setup accelerators mapping based on priority rule
	accelerators := map[string]machinetypes.PhysicalGpuCount{}
	if rule.GpuRequest().PhysicalGPUCount != 0 {
		accelerators[rule.GpuRequest().Config.GpuType] = rule.GpuRequest().PhysicalGPUCount
	}

	// These options are ignored as they are not configured as part of CCC
	minCpuPlatform := rsvProperties.MinCpuPlatform
	// TODO(b/517097938): add zone validation once location support becomes part of CCC
	zone := gceclient.GetReservationZone(gceReservation)

	nodeShape := reservations.NodeShape{
		MachineType:    machineType,
		MinCpuPlatform: minCpuPlatform,
		Accelerators:   accelerators,
		LocalSSDSizes:  localSSDSizes,
		Zone:           zone,
	}

	reasons := reservations.MatchSpecificReservationShapeWithReasons(provider, gceReservation, nodeShape, false)
	if len(reasons) == 0 {
		return nil
	}

	return errors.New(strings.Join(reasons, "; "))
}

// matchAggregateReservationOrError matches aggregate reservation against properties specified in a node config rule.
func matchAggregateReservationOrError(
	provider CloudProvider,
	gceReservation *gceapiv1.Reservation,
	rule rules.Rule,
) error {
	// Not trying to match any reservation, only specific are allowed
	if !gceReservation.SpecificReservationRequired {
		return errors.New("any affinity reservation cannot be consumed")
	}

	if !reservations.IsAggregateReservation(gceReservation) {
		return errors.New("unable to match non-aggregate reservation")
	}

	if gceReservation.AggregateReservation == nil {
		return errors.New("unable to match non-aggregate reservation")
	}

	if !rule.HasTpu() {
		return errors.New("no TPU request while targeting aggregate reservation")
	}

	reasons := reservations.MatchAggregateReservationShapeWithReasons(provider, gceReservation, rule.TpuType(), rule.TpuTopology(), rule.TpuCount())
	if len(reasons) == 0 {
		return nil
	}

	return errors.New(strings.Join(reasons, "; "))
}

func matchReservationBlock(blockName string, reservationBlocks []*gceclient.GceReservationBlock) bool {
	for _, reservationBlock := range reservationBlocks {
		if blockName == reservationBlock.Name {
			return true
		}
	}
	return false
}
