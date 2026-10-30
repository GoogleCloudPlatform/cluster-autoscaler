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

package computeclass

import (
	"strconv"

	apiv1 "k8s.io/api/core/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
)

// CountsTowardMinCapacityScaleDown reports whether the node counts toward the
// min capacity of its ComputeClass when deciding which nodes may be removed:
// like CanHoldMinCapacityFakePod, minus defrag candidates.
func CountsTowardMinCapacityScaleDown(node *apiv1.Node, nodeFilter resourcequotas.NodeFilter) bool {
	// Don't count a defrag candidate until its replacement is registered.
	if taints.HasTaint(node, defrag.HardTaint) {
		return false
	}
	return CanHoldMinCapacityFakePod(node, nodeFilter)
}

// CanHoldMinCapacityFakePod reports whether a min capacity fake pod can be
// placed on the node: like CountsTowardMinCapacityScaleUp, minus upcoming
// placeholders.
func CanHoldMinCapacityFakePod(node *apiv1.Node, nodeFilter resourcequotas.NodeFilter) bool {
	if upcoming, _ := strconv.ParseBool(node.Annotations[annotations.NodeUpcomingAnnotation]); upcoming {
		return false
	}
	return CountsTowardMinCapacityScaleUp(node, nodeFilter)
}

// CountsTowardMinCapacityScaleUp reports whether the node counts toward the min
// capacity of its ComputeClass when computing the scale-up shortfall: every
// node except the ones being deleted and the ones nodeFilter excludes.
func CountsTowardMinCapacityScaleUp(node *apiv1.Node, nodeFilter resourcequotas.NodeFilter) bool {
	if node.DeletionTimestamp != nil || taints.HasToBeDeletedTaint(node) {
		return false
	}
	return nodeFilter == nil || !nodeFilter.ExcludeFromTracking(node)
}
