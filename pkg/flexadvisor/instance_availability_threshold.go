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
	"context"
	"maps"
	"slices"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
)

type instanceAvailabilityThreshold struct {
	provider                 instanceavailability.Provider
	reservationPuller        *gceclient.ReservationsPuller
	localSSDDiskSizeProvider localssdsize.LocalSSDSizeProvider
	cccLister                lister.Lister
	cloudProvider            InstanceAvailabilityCloudProvider
	experimentsManager       experiments.Manager
	limiterTracker           ScaleUpLimiterTracker
	// balanceSimilarNodeGroups mirrors the BalanceSimilarNodeGroups autoscaling option. Capacity of an
	// uncreated NAP candidate is summed across its planned zones only when it is set, as only then is the
	// scale-up spread across the zonal MIGs after the node pool is created.
	balanceSimilarNodeGroups bool
}

type InstanceAvailabilityCloudProvider interface {
	MachineConfigProvider() *machinetypes.MachineConfigProvider
	gke.PlannedLocationsProvider
}

// NewInstanceAvailabilityThreshold returns an instance of instanceAvailabilityThreshold.
func NewInstanceAvailabilityThreshold(provider instanceavailability.Provider, puller *gceclient.ReservationsPuller, localSSDDiskSizeProvider localssdsize.LocalSSDSizeProvider, cccLister lister.Lister, cloudProvider InstanceAvailabilityCloudProvider, experimentsManager experiments.Manager, limiterTracker ScaleUpLimiterTracker, balanceSimilarNodeGroups bool) *instanceAvailabilityThreshold {
	return &instanceAvailabilityThreshold{
		provider:                 provider,
		reservationPuller:        puller,
		localSSDDiskSizeProvider: localSSDDiskSizeProvider,
		cccLister:                cccLister,
		cloudProvider:            cloudProvider,
		experimentsManager:       experimentsManager,
		limiterTracker:           limiterTracker,
		balanceSimilarNodeGroups: balanceSimilarNodeGroups,
	}
}

// NodeLimit return maxNodeLimit as a sum of FlexAdvisor recommendations + matching reservations across all similar node groups (so for a pool it should match all MIGs for each of its zones).
// For example for pool-1 with zone-a, zone-b and zone-c MIGs, we may receive zone-a as main nodeGroup. We will find zone-b and zone-c through SimilarNodeGroups() and sum all their matching FA recommendations to return total NodeLimit just for zone-a - ie 3000 (1000+1000+1000)
// Similar node groups are matched in such way (and their limits summed) to duct-tape drift between how bin packing and balancers operate - limiters make single zone represent capacity for all similar node groups to allow given MIG through, knowing that
// balancers expand selected best option in same way
func (t *instanceAvailabilityThreshold) NodeLimit(ctx context.Context, nodeGroup cloudprovider.NodeGroup, estimationContext estimator.EstimationContext) estimator.NodeLimitResult {
	if !IsFlexAdvisorProcessingEnabled(t.experimentsManager) {
		klog.Info("FlexAdvisor: bin packer processing is disabled by FlexAdvisorProcessing experiment, skipping applying FlexAdvisor limits in bin packer")
		return estimator.NodeLimitResult{Limit: 0}
	}
	maxNodeLimit := 0

	guidanceIdsUsed := make(map[string]bool)
	instanceReferencesProcessed := make(map[string]bool)
	flexibilityScopes := make(map[string]bool)

	for _, ng := range t.nodeGroupsToEvaluate(ctx, nodeGroup, estimationContext.SimilarNodeGroups()) {
		if !isFlexAdvisorReservationSpecificMigsProcessingEnabled(t.experimentsManager) && hasReservationAffinitySpecific(ng) {
			klog.V(4).Infof("FlexAdvisor: NodeLimit not applied to nodeGroup %s because nodeGroup %s targets a specific reservation", nodeGroup.Id(), ng.Id())
			return estimator.NodeLimitResult{Limit: 0}
		}
		instanceRef, err := ConstructInstanceReference(ng, t.cccLister, t.experimentsManager)
		if err != nil {
			return estimator.NodeLimitResult{Limit: 0}
		}
		flexibilityScopes[instanceRef.FlexibilityScopeKey] = true

		snapshot := t.provider.GetInstanceAvailability(instanceRef.FlexibilityScopeKey, instanceRef.InstanceConfigKey)
		if snapshot == nil {
			return estimator.NodeLimitResult{Limit: 0}
		}
		reservationCount := t.allUnusedReservations(ng)
		maxInstancesFromFA, ok := snapshot.MaxAvailableInstances(instanceRef.Zone)
		if !ok {
			// if we didn't receive available instances from GCE FlexAdvisor for at least one zone, we don't apply node limit to the node group at all
			klog.Warningf("FlexAdvisor: NodeLimit not applied to nodeGroup %s due to unknown availability in the zone, zone=%v, flexibilityScopeKey=%v, guidanceId=%v", nodeGroup.Id(), instanceRef.Zone, instanceRef.InstanceConfigKey, snapshot.GuidanceId())
			return estimator.NodeLimitResult{Limit: 0}
		}

		// Negative capacity from FlexAdvisor or negative unused reservation count in one zone must not deduct capacity from another zone.
		// Capping each zone's contribution at 0 also ensures maxNodeLimit is monotonically non-decreasing across the loop.
		maxNodeLimit += max(0, maxInstancesFromFA) + max(0, reservationCount)

		guidanceIdsUsed[snapshot.GuidanceId()] = true
		instanceReferencesProcessed[instanceRef.String()] = true
	}
	if maxNodeLimit <= 0 {
		klog.Infof("FlexAdvisor: removing %s from bin packing due to no capacity, instanceReferencesProcessed=%v, guidancesUsed=%v", nodeGroup.Id(), slices.Collect(maps.Keys(instanceReferencesProcessed)), slices.Collect(maps.Keys(guidanceIdsUsed)))
		if t.limiterTracker != nil {
			for scope := range flexibilityScopes {
				t.limiterTracker.MarkScaleUpOptionRemovedByFlexAdvisor(nodeGroup.Id(), scope)
			}
		}
		return estimator.NodeLimitResult{Limit: -1}
	}
	klog.Infof("FlexAdvisor: setting %s bin packing maxNodeLimit to %d based on instanceReferencesProcessed=%v, guidancesUsed=%v", nodeGroup.Id(), maxNodeLimit, slices.Collect(maps.Keys(instanceReferencesProcessed)), slices.Collect(maps.Keys(guidanceIdsUsed)))
	return estimator.NodeLimitResult{Limit: maxNodeLimit}
}

// DurationLimit always returns 0. No time based limit is set.
func (t *instanceAvailabilityThreshold) DurationLimit(_ cloudprovider.NodeGroup, _ estimator.EstimationContext) estimator.DurationLimitResult {
	return estimator.DurationLimitResult{Duration: 0}
}

// nodeGroupsToEvaluate returns the node groups whose capacity contributes to the node limit of nodeGroup.
// For an uncreated NAP candidate (which is not expected to have similar node groups), these are per-zone
// MIGs across all zones the node pool would span after creation.
//
// Summing across planned zones is only done when BalanceSimilarNodeGroups is enabled, as only then is the
// scale-up spread across the zonal MIGs after the node pool is created; otherwise the whole scale-up would
// target the representative MIG, whose zone may not have the summed capacity.
//
// Known limitations (see section 9 of the design doc,
// https://docs.google.com/document/d/1UTnpC2r5FDfgYFi6wR8FS52DPIueE5yf09X9DF2msdI):
//   - Planned zonal MIGs are not filtered by scale-up backoff; FA guidance is relied upon instead.
//   - Unknown FA availability in any planned zone disables the limit, as for existing regional pools.
func (t *instanceAvailabilityThreshold) nodeGroupsToEvaluate(ctx context.Context, nodeGroup cloudprovider.NodeGroup, similarNodeGroups []cloudprovider.NodeGroup) []cloudprovider.NodeGroup {
	if !IsFlexAdvisorNapZoneSetExpansionEnabled(t.experimentsManager) || !t.balanceSimilarNodeGroups {
		return allUniqueNodeGroups(append(similarNodeGroups, nodeGroup))
	}
	return NewNodeGroupSet(ctx, nodeGroup, similarNodeGroups, t.cloudProvider).NodeGroups()
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
