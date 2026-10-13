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
	"errors"
	"fmt"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	base_backoff "sigs.k8s.io/cluster-autoscaler/pkg/utils/backoff"
)

// NodeGroupSet represents a node group together with its per-zone counterparts:
//   - For an existing or upcoming node pool, these are the representative nodeGroup and similarNodeGroups.
//   - For an uncreated NAP candidate without similarNodeGroups, these are per-zone MIGs across the
//     locations that the created node pool would span: the candidate itself for its own zone and
//     virtual copies for the other planned zones.
//
// The virtual copies are not registered in the node pool and must not be scaled; they are used to evaluate
// capacity, reservations, and preference scores across all planned zones of an uncreated candidate.
type NodeGroupSet struct {
	representative    cloudprovider.NodeGroup
	similarNodeGroups []cloudprovider.NodeGroup

	// plannedZonalMigs is populated only for an uncreated NAP candidate without similarNodeGroups whose
	// planned locations could be determined. It holds exactly one MIG per planned zone, in planned
	// location order. When planned locations are empty or no provider is available, it holds only the
	// candidate itself.
	plannedZonalMigs []*gke.GkeMig
	// plannedLocationsErr is set if the representative is an uncreated NAP candidate whose planned
	// locations couldn't be computed. plannedZonalMigs is empty in that case.
	plannedLocationsErr error
}

// NewNodeGroupSet creates a NodeGroupSet for the given representative nodeGroup and similarNodeGroups.
// For uncreated NAP candidates, planned node pool locations are computed using provider (if non-nil).
func NewNodeGroupSet(
	ctx context.Context,
	nodeGroup cloudprovider.NodeGroup,
	similarNodeGroups []cloudprovider.NodeGroup,
	provider gke.PlannedLocationsProvider,
) NodeGroupSet {
	ngs := NodeGroupSet{
		representative:    nodeGroup,
		similarNodeGroups: similarNodeGroups,
	}
	if len(similarNodeGroups) > 0 {
		return ngs
	}
	gkeNg, ok := nodeGroup.(gke.NodeGroup)
	if !ok || nodeGroup.Exist(ctx) || gkeNg.IsUpcoming() || !nodeGroup.Autoprovisioned(ctx) {
		return ngs
	}
	mig := gkeNg.GetMig()
	if mig == nil {
		return ngs
	}
	if provider == nil {
		ngs.plannedZonalMigs = []*gke.GkeMig{mig}
		return ngs
	}
	locations, err := provider.PlannedNodePoolLocations(mig)
	if err != nil {
		klog.Warningf("Couldn't determine planned node pool locations for uncreated node group %s: %v", mig.Id(), err)
		ngs.plannedLocationsErr = fmt.Errorf("couldn't determine planned node pool locations for %s: %w", mig.Id(), err)
		return ngs
	}
	ngs.plannedZonalMigs = zonalMigsForLocations(mig, sanitizeZones(locations))
	return ngs
}

// Representative returns the primary node group for which this NodeGroupSet was created.
func (ngs NodeGroupSet) Representative() cloudprovider.NodeGroup {
	return ngs.representative
}

// NodeGroups returns the unique per-zone node groups to evaluate (e.g. for capacity and unused reservations):
// one MIG per planned zone for uncreated NAP candidates, and representative + similarNodeGroups otherwise
// (including when planned locations cannot be computed).
// The caller's similarNodeGroups slice is never modified.
func (ngs NodeGroupSet) NodeGroups() []cloudprovider.NodeGroup {
	if len(ngs.plannedZonalMigs) > 0 {
		result := make([]cloudprovider.NodeGroup, len(ngs.plannedZonalMigs))
		for i, m := range ngs.plannedZonalMigs {
			result[i] = m
		}
		return result
	}
	return uniqueNodeGroups(ngs.representative, ngs.similarNodeGroups)
}

// TargetZonesForScoring returns the non-backed-off zones in which the node group set can be scored:
//   - For an existing or upcoming node group, the zones of the representative and similar node groups.
//   - For a !mig.exists NAP candidate, the planned node pool locations (or an error if they could not be determined).
//
// For !mig.exists node groups, backoff calculations are applied here; for existing node groups, backoffs are applied at ComputeSimilarNodeGroups.
//
// Note: Because backed-off zones are excluded for scoring, the returned list may be a subset of the zones where the node pool exists, will be created, or will have scale-up balanced across.
//
// An empty slice means all target zones are backed off
func (ngs NodeGroupSet) TargetZonesForScoring(backoff base_backoff.Backoff, nodeInfos map[string]*framework.NodeInfo) ([]string, error) {
	if ngs.representative == nil {
		return nil, errors.New("nil node group")
	}
	gkeNg, ok := ngs.representative.(gke.NodeGroup)
	if !ok {
		return nil, fmt.Errorf("node group %s is not a GKE node group", ngs.representative.Id())
	}
	if ngs.plannedLocationsErr != nil {
		return nil, ngs.plannedLocationsErr
	}

	var targetZones []string
	var targetNodeGroups []gke.NodeGroup
	if len(ngs.plannedZonalMigs) == 0 {
		// Existing or upcoming node group: the representative and its similarNodeGroups.
		targetNodeGroups = make([]gke.NodeGroup, 0, 1+len(ngs.similarNodeGroups))
		targetNodeGroups = append(targetNodeGroups, gkeNg)
		for _, ng := range ngs.similarNodeGroups {
			similarGkeNg, ok := ng.(gke.NodeGroup)
			if !ok {
				klog.Warningf("NodeGroupSet: Could not cast similar node group of type %T to gke.NodeGroup", ng)
				continue
			}
			targetNodeGroups = append(targetNodeGroups, similarGkeNg)
		}
	} else {
		// Uncreated NAP candidate node group:
		// Use the locations that GKE will create the node pool in, computed by the same code that builds
		// the node pool spec on creation. Zones that are merely not covered by existing node pools are
		// intentionally kept.
		targetNodeGroups = make([]gke.NodeGroup, 0, len(ngs.plannedZonalMigs))
		for _, m := range ngs.plannedZonalMigs {
			targetNodeGroups = append(targetNodeGroups, m)
		}
	}
	if !gkeNg.Exist(context.TODO()) {
		// !exists && !upcoming && len(similarNodeGroups)==0 pools have their zones set expanded by us, we need to replicate logic from NAP and remove backed off migs so these pools are homogenous with existing pools.
		// !exists && !upcoming && len(similarNodeGroups)>0 pools don't have their zones set expanded, so we don't need to apply backoffs again - to simplify logic we calculate it again (all zones should actually pass)
		// !exists && upcoming && len(similarNodeGroups)==0 pools don't have zones set expanded, but due to CA bug they don't have backoffs applied, we apply it here to make these pools homogenous with existing ones
		// !exists && upcoming && len(similarNodeGroups)>0 - same as above, need to apply backoffs
		targetZones = ngs.limitByBackoffs(backoff, nodeInfos, targetNodeGroups)
	} else {
		targetZones = make([]string, 0, len(targetNodeGroups))
		for _, ng := range targetNodeGroups {
			targetZones = append(targetZones, ng.GceRef().Zone)
		}
	}
	return sanitizeZones(targetZones), nil
}

// limitByBackoffs returns the zones of targetNodeGroups, excluding the zones of non-existing node groups
// that are backed off.
//
// Existing node groups are not checked, as ComputeSimilarNodeGroups and filterValidScaleUpNodeGroups
// already exclude backed-off ones. Uncreated NAP candidates and upcoming node groups have Exist()==false,
// so they are skipped by those checks. Their backed-off (e.g. stocked-out) zones are excluded here, so that
// they do not create phantom scores. Upcoming node groups and uncreated candidates of the same shape share
// the same spec-and-zone based singleMigBackoff key, so they are scored on the same set of zones.
func (ngs NodeGroupSet) limitByBackoffs(backoff base_backoff.Backoff, nodeInfos map[string]*framework.NodeInfo, targetNodeGroups []gke.NodeGroup) []string {
	var availableZones, backedOffZones []string
	now := time.Now()
	candidateNodeInfo := nodeInfos[ngs.representative.Id()]
	for _, gkeNg := range targetNodeGroups {
		zone := gkeNg.GceRef().Zone
		nodeInfo := nodeInfos[gkeNg.Id()]
		if nodeInfo == nil {
			nodeInfo = nodeInfoInZone(candidateNodeInfo, zone)
		}
		if backoff != nil && backoff.BackoffStatus(gkeNg, nodeInfo, now).IsBackedOff {
			backedOffZones = append(backedOffZones, zone)
		} else {
			availableZones = append(availableZones, zone)
		}
	}
	if len(backedOffZones) > 0 {
		klog.V(5).Infof("Excluding backed-off zones %v of node group %s, remaining target zones: %v", backedOffZones, ngs.representative.Id(), availableZones)
	}
	return availableZones
}

// zonalMigsForLocations returns one MIG per planned location of an uncreated candidate, in location order:
// the candidate itself for its own zone and a virtual shallow copy for every other zone. If locations is
// empty, only the candidate is returned. Locations are expected to be sanitized.
func zonalMigsForLocations(mig *gke.GkeMig, locations []string) []*gke.GkeMig {
	if len(locations) == 0 {
		return []*gke.GkeMig{mig}
	}
	result := make([]*gke.GkeMig, 0, len(locations))
	for _, zone := range locations {
		if zone == mig.GceRef().Zone {
			result = append(result, mig)
		} else {
			result = append(result, mig.ShallowCopyInZone(zone))
		}
	}
	return result
}

func uniqueNodeGroups(representative cloudprovider.NodeGroup, similar []cloudprovider.NodeGroup) []cloudprovider.NodeGroup {
	result := make([]cloudprovider.NodeGroup, 0, 1+len(similar))
	seen := make(map[string]bool, 1+len(similar))
	add := func(ng cloudprovider.NodeGroup) {
		if ng == nil || seen[ng.Id()] {
			return
		}
		seen[ng.Id()] = true
		result = append(result, ng)
	}
	add(representative)
	for _, ng := range similar {
		add(ng)
	}
	return result
}

// sanitizeZones returns a new slice with duplicate and empty zone names removed
// while preserving the original order.
func sanitizeZones(raw []string) []string {
	if len(raw) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(raw))
	res := make([]string, 0, len(raw))
	for _, z := range raw {
		if z == "" || seen[z] {
			continue
		}
		seen[z] = true
		res = append(res, z)
	}
	return res
}

// nodeInfoInZone returns a copy of nodeInfo whose node is labeled with the given zone. Pods and resource
// slices are not copied, as they are not needed for backoff checks. Returns nil for a nil nodeInfo.
func nodeInfoInZone(nodeInfo *framework.NodeInfo, zone string) *framework.NodeInfo {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return nil
	}
	node := nodeInfo.Node().DeepCopy()
	if node.Labels == nil {
		node.Labels = make(map[string]string)
	}
	node.Labels[apiv1.LabelTopologyZone] = zone
	return framework.NewNodeInfo(node, nil)
}
