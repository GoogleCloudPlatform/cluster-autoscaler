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

package gkeprice

import (
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/provider"
)

const (
	// Penalty given to node groups that are yet to be created.
	notExistCoefficient = 1.5
	// Penalty given to node groups with GPUs that are yet to be created.
	// It is smaller as GPU node pools are dedicated so additional resources are wasted.
	notExistGpuCoefficient = 1.1
	// Number of MIGs per pool for pool count penalty calculations
	// Usually there are 3 MIGs per pool in multizonal clusters, but using 3 would make
	// our node pool creation behaviour vastly different for single and multizonal cluster.
	// Number below should serve as a compromise to not penalize heavily either type of clusters.
	// Approximate geometric mean:
	migsPerPool = 1.75
)

// GroupCountReducer returns preferred node based on the cluster size.
type GroupCountReducer interface {
	// Penalty given to node groups that are yet to be created.
	GroupCreationPenalty(hasGpu bool) float64
	// Penalty given to node groups that are yet to be created without coefficient multiplication at the end.
	BaseGroupCreationPenalty() float64
}

type nodePoolNameGetter interface {
	NodePoolName() string
}

type progressiveGroupCountReducer struct {
	provider provider.GkeExpanderCloudProvider
}

// NewProgressiveGroupCountReducer returns GroupCountReducer that increase a penalty
// along with number of existing node pool and underlying MIGs.
func NewProgressiveGroupCountReducer(provider provider.GkeExpanderCloudProvider) GroupCountReducer {
	return &progressiveGroupCountReducer{
		provider: provider,
	}
}

// GroupCreationPenalty returns penalty for creation of a new node group.
func (pcr *progressiveGroupCountReducer) GroupCreationPenalty(hasGpu bool) float64 {
	penalty := pcr.BaseGroupCreationPenalty()
	if hasGpu {
		penalty *= notExistGpuCoefficient
	} else {
		penalty *= notExistCoefficient
	}
	return penalty
}

func (pcr *progressiveGroupCountReducer) BaseGroupCreationPenalty() float64 {
	// Make multizonal cluster behave similarly to single zonal
	var nodeGroups []cloudprovider.NodeGroup

	for _, ng := range pcr.provider.NodeGroups() {
		// we want to skip non-autoprovisioned nodepools for autopilot cluster
		// This will essentially exclude snowflake and quickstart nodepools primarily.
		// context: b/291550261#comment6
		if pcr.provider.IsAutopilotEnabled() && !ng.Autoprovisioned() {
			continue
		}
		nodeGroups = append(nodeGroups, ng)
	}

	nodePools := make(map[string]struct{})
	for _, nodeGroup := range nodeGroups {
		mig, ok := nodeGroup.(nodePoolNameGetter)
		if ok {
			nodePools[mig.NodePoolName()] = struct{}{}
		} else {
			nodePools[nodeGroup.Id()] = struct{}{}
		}
	}
	migCount := len(nodeGroups)
	// Bumping up the number for single-zone clusters
	poolBasedCount := migsPerPool * float64(len(nodePools))
	// Trying to find a balance between single-zone and multizonal MIG counts
	groupCount := max(int64(migCount), int64(poolBasedCount))
	return groupCountPenalty(groupCount)
}

// groupCountPenalty computes penalty based on number of node groups
// Results:
// 1.0004 for 0 groups
// 1.5 for 35 groups
// 2 for 50 groups
// 3 for 70 groups
// 5 for 100 groups
func groupCountPenalty(count int64) float64 {
	return 1 + 0.0004*float64(1+count*count)
}
