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

package processors

import (
	"maps"

	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

var ignoredLabelsAcrossNodePools = func() map[string]bool {
	baseLabels := maps.Clone(nodegroupset.BasicIgnoredLabels)
	baseLabels[gkelabels.GkeNodePoolLabel] = true            // Groups can be similar even if they belong to different node pools.
	baseLabels[gkelabels.ProvisioningRequestLabelKey] = true // Groups can be similar even if they contain nodes provisioned for different provisioning requests.
	return baseLabels
}()

// IsGkeNodeInfoSimilar compares if two nodes should be considered part of the
// same NodeGroupSet. This is true if they either belong to the same GKE nodepool.
// After a node is created there's a short time when it doesn't have a nodepool label yet.
// In this case we fall back to the OSS comparator, which in this case should return true
// only if the nodes are in the same GKE nodepool.
func IsGkeNodeInfoSimilar(n1, n2 *framework.NodeInfo) bool {
	n1GkeNodePool := GkeNodePoolLabel(n1)
	n2GkeNodePool := GkeNodePoolLabel(n2)
	if n1GkeNodePool == "" || n2GkeNodePool == "" {
		// Nodepool label hasn't been applied yet to a new node.
		// Fall back to the OSS logic.
		return nodegroupset.IsCloudProviderNodeInfoSimilar(n1, n2, nodegroupset.BasicIgnoredLabels, config.NewDefaultNodeGroupDifferenceRatios())
	}
	return n1GkeNodePool == n2GkeNodePool
}

// MIGs from other node pools are also considered similar.
func IsGkeNodeInfoSimilarAcrossNodePools(n1, n2 *framework.NodeInfo) bool {
	n1GkeNodePool := GkeNodePoolLabel(n1)
	n2GkeNodePool := GkeNodePoolLabel(n2)
	if n1GkeNodePool != "" && n1GkeNodePool == n2GkeNodePool {
		return true
	}

	return nodegroupset.IsCloudProviderNodeInfoSimilar(n1, n2, ignoredLabelsAcrossNodePools, config.NewDefaultNodeGroupDifferenceRatios())

}

func GkeNodePoolLabel(n *framework.NodeInfo) string {
	return n.Node().Labels[gkelabels.GkeNodePoolLabel]
}
