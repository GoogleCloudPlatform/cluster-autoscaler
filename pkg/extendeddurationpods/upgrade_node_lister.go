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

package extendeddurationpods

import (
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils"
	"k8s.io/klog/v2"
)

// UpgradeEligibleEdpNodes returns a list of real EDP nodes which are not yet upgraded to the new master version
func UpgradeEligibleEdpNodes(ctx *context.AutoscalingContext) []*framework.NodeInfo {
	var nodeList []*framework.NodeInfo
	allNodes, err := ctx.ClusterSnapshot.ListNodeInfos()
	if err != nil {
		return nodeList
	}
	provider, ok := ctx.CloudProvider.(CloudProvider)
	if !ok {
		return nodeList
	}
	for _, node := range allNodes {
		if IsNodeEligibleForUpgrade(node, provider.GetClusterVersion()) {
			nodeList = append(nodeList, node)
		}
	}
	return nodeList
}

// IsNodeEligibleForUpgrade returns the eligibility of a node for an EDP upgrade criteria
func IsNodeEligibleForUpgrade(node *framework.NodeInfo, clusterVersion string) bool {
	if node.Node() == nil {
		return false
	}
	if _, f := node.Node().Labels[labels.ExtendedDurationPodsLabel]; !f {
		return false
	}
	if utils.IsNodeInfoUpcoming(node) {
		return false
	}
	cVersion, err := version.FromString(clusterVersion)
	if err != nil {
		klog.Warningf("Unable to parse cluster version: %s, %q", clusterVersion, err)
		return false
	}
	nodeVersion, err := version.FromString(node.Node().Status.NodeInfo.KubeletVersion)
	if err != nil {
		klog.Warningf("Unable to parse node version: %s, %q", node.Node().Status.NodeInfo.KubeletVersion, err)
		return false
	}
	return nodeVersion.LessThan(cVersion)
}
