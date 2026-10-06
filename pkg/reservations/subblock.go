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
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
)

// TargetedSubBlocks returns the canonicalized reservation paths of all reservation sub-blocks that
// ng specifically targets. Only SPECIFIC reservation affinities pin a node group to concrete
// sub-blocks; ANY/NONE affinities let GCE place the nodes anywhere and return nil. Paths are
// canonicalized against clusterProject so that long-form and short-form paths within it match.
func TargetedSubBlocks(ng cloudprovider.NodeGroup, clusterProject string) []string {
	gkeNg, ok := ng.(gke.NodeGroup)
	if !ok {
		return nil
	}
	spec := gkeNg.Spec()
	if spec == nil || spec.ReservationAffinity == nil {
		return nil
	}
	affinity := spec.ReservationAffinity
	if affinity.ConsumeReservationType != gkeclient.ReservationAffinitySpecific {
		return nil
	}
	var subBlocks []string
	for _, val := range affinity.Values {
		if gceclient.TargetsReservationSubBlock(val) {
			subBlocks = append(subBlocks, gceclient.CanonicalizeReservationPath(val, clusterProject))
		}
	}
	return subBlocks
}

// TargetedSubBlock returns the canonicalized reservation path of the single sub-block that ng
// specifically targets, or "" if ng does not target exactly one reservation sub-block.
func TargetedSubBlock(ng cloudprovider.NodeGroup, clusterProject string) string {
	subBlocks := TargetedSubBlocks(ng, clusterProject)
	if len(subBlocks) != 1 {
		return ""
	}
	return subBlocks[0]
}

// RequiresWholeSubBlock reports whether ng is a dynamic slicing node group (under a PROVISION_ONLY
// workload policy). GCE requires a PROVISION_ONLY MIG to span a full reservation sub-block (1:1
// mapping, e.g. 4x4x4 on tpu7x), so it can never share one with any other node group. Static slices
// (AUTO_CONNECT or no workload policy) may legitimately occupy part of a sub-block or span several,
// so they are not subject to that constraint.
func RequiresWholeSubBlock(ng cloudprovider.NodeGroup) bool {
	gkeNg, ok := ng.(gke.NodeGroup)
	return ok && gkeNg.GetMig() != nil && gkeNg.GetMig().IsProvisionOnly()
}

// ClaimedSubBlocks returns the canonicalized paths of all reservation sub-blocks targeted by any
// of nodeGroups, regardless of their kind: a dynamic slicing node group needs the whole sub-block,
// so anything already sitting on a given sub-block is enough to rule that sub-block out for a
// dynamic slicing node group.
func ClaimedSubBlocks(nodeGroups []cloudprovider.NodeGroup, clusterProject string) sets.Set[string] {
	claimed := sets.New[string]()
	for _, ng := range nodeGroups {
		claimed.Insert(TargetedSubBlocks(ng, clusterProject)...)
	}
	return claimed
}
