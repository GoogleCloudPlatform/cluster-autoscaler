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

package flexadvisor

import (
	"maps"
	"slices"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"k8s.io/klog/v2"
)

type instanceAvailabilityThreshold struct {
	provider                 instanceavailability.Provider
	reservationPuller        *gceclient.ReservationsPuller
	localSSDDiskSizeProvider localssdsize.LocalSSDSizeProvider
	cccLister                lister.Lister
	cloudProvider            InstanceAvailabilityCloudProvider
	experimentsManager       experiments.Manager
}

type InstanceAvailabilityCloudProvider interface {
	MachineConfigProvider() *machinetypes.MachineConfigProvider
}

// NewInstanceAvailabilityThreshold returns an instance of instanceAvailabilityThreshold.
func NewInstanceAvailabilityThreshold(provider instanceavailability.Provider, puller *gceclient.ReservationsPuller, localSSDDiskSizeProvider localssdsize.LocalSSDSizeProvider, cccLister lister.Lister, cloudProvider InstanceAvailabilityCloudProvider, experimentsManager experiments.Manager) *instanceAvailabilityThreshold {
	return &instanceAvailabilityThreshold{
		provider:                 provider,
		reservationPuller:        puller,
		localSSDDiskSizeProvider: localSSDDiskSizeProvider,
		cccLister:                cccLister,
		cloudProvider:            cloudProvider,
		experimentsManager:       experimentsManager,
	}
}

// NodeLimit return max node limit based on Flex Advisor guidance and matching unused reservations.
// In case of matching reservations, the max value from reservations and Flex Advisor guidance is used.
// Effect of negative capacity from Flex Advisor is ignored if there are matching unused reservations.
// max node limit is the sum of max node limits for nodegroup and all similar node groups.
// In case of error, 0 is returned. Thresholds with 0 limits will be ignored in favor of thresholds with positive or negative limits.
// -1 is returned when max node limit is zero, to disallow new nodes.
func (t *instanceAvailabilityThreshold) NodeLimit(nodeGroup cloudprovider.NodeGroup, estimationContext estimator.EstimationContext) int {
	maxNodeLimit := 0
	totalReservationCount := 0

	guidanceIdsUsed := make(map[string]bool)
	instanceReferencesProcessed := make(map[string]bool)

	for _, ng := range allUniqueNodeGroups(append(estimationContext.SimilarNodeGroups(), nodeGroup)) {
		instanceRef, err := constructInstanceReference(ng, t.cccLister, t.experimentsManager)
		if err != nil {
			return 0
		}

		snapshot := t.provider.GetInstanceAvailability(instanceRef.flexibilityScopeKey, instanceRef.instanceConfigKey)
		if snapshot == nil {
			return 0
		}
		reservationCount := t.allUnusedReservations(ng)
		totalReservationCount += reservationCount
		maxInstancesFromFA, ok := snapshot.MaxAvailableInstances(instanceRef.zone)
		if !ok {
			// if we didn't receive available instances from GCE FlexAdvisor for at least one zone, we don't apply node limit to the node group at all
			klog.Warningf("FlexAdvisor: NodeLimit not applied to nodeGroup %s due to unknown availability in the zone, zone=%v, flexibilityScopeKey=%v, guidanceId=%v", nodeGroup.Id(), instanceRef.zone, instanceRef.instanceConfigKey, snapshot.GuidanceId())
			return 0
		}

		if reservationCount > 0 {
			maxNodeLimit = maxNodeLimit + maxInstancesFromFA + reservationCount
		} else {
			maxNodeLimit = maxNodeLimit + maxInstancesFromFA
		}
		guidanceIdsUsed[snapshot.GuidanceId()] = true
		instanceReferencesProcessed[instanceRef.String()] = true
	}

	// if there is any reservation, we favour them against negative capacities from other node groups.
	if totalReservationCount > 0 {
		maxNodeLimit = max(maxNodeLimit, totalReservationCount)
	}
	if maxNodeLimit <= 0 {
		klog.Infof("FlexAdvisor: removing %s from bin packing due to no capacity, instanceReferencesProcessed=%v, guidancesUsed=%v", nodeGroup.Id(), slices.Collect(maps.Keys(instanceReferencesProcessed)), slices.Collect(maps.Keys(guidanceIdsUsed)))
		return -1
	}
	klog.Infof("FlexAdvisor: setting %s bin packing maxNodeLimit to %d based on instanceReferencesProcessed=%v, guidancesUsed=%v", nodeGroup.Id(), maxNodeLimit, slices.Collect(maps.Keys(instanceReferencesProcessed)), slices.Collect(maps.Keys(guidanceIdsUsed)))
	return maxNodeLimit
}

// DurationLimit always returns 0. No time based limit is set.
func (t *instanceAvailabilityThreshold) DurationLimit(_ cloudprovider.NodeGroup, _ estimator.EstimationContext) time.Duration {
	return 0
}

func allUniqueNodeGroups(nodeGroups []cloudprovider.NodeGroup) []cloudprovider.NodeGroup {
	var uniqueNodeGroups []cloudprovider.NodeGroup
	processedGroups := make(map[string]bool)
	for _, ng := range nodeGroups {
		if found := processedGroups[ng.Id()]; found {
			continue
		}
		uniqueNodeGroups = append(uniqueNodeGroups, ng)
		processedGroups[ng.Id()] = true
	}
	return uniqueNodeGroups
}

func (t *instanceAvailabilityThreshold) allUnusedReservations(ng cloudprovider.NodeGroup) int {
	if t.localSSDDiskSizeProvider == nil {
		return 0
	}
	if t.reservationPuller == nil {
		return 0
	}

	allReservations := t.reservationPuller.GetReservations()
	return reservations.MatchingUnusedReservations(t.cloudProvider, ng, allReservations, t.localSSDDiskSizeProvider)
}
