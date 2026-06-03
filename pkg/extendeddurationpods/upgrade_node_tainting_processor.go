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
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	ca_taints "k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/util/taints"
)

var (
	notTargetGkeVersionTaint = &v1.Taint{
		Key:    labels.NotTargetGkeVersionLabel,
		Value:  labels.NotTargetGkeVersionValue,
		Effect: v1.TaintEffectNoSchedule,
	}
)

// UpgradeNodeTaintingProcessor is an autoscaling processor which runs at the end of runOnce (even when runOnce fails)
// along with status processors ensure higher availability to run
type UpgradeNodeTaintingProcessor struct {
	// nodesProcessingPerLoop limits the number of nodes that'll be processed per ca loop
	nodesProcessingPerLoop int
}

// NewUpgradeNodeTaintingProcessor creates a new instance of UpgradeNodeTaintingProcessor
func NewUpgradeNodeTaintingProcessor(nodeProcessingLimitPerLoop int) *UpgradeNodeTaintingProcessor {
	return &UpgradeNodeTaintingProcessor{
		nodesProcessingPerLoop: nodeProcessingLimitPerLoop,
	}
}

// Process adds taints to EDP nodes which are on a lower node version than the cluster version. This is block new pods to schedule on these nodes
// as they're due to undergo the cluster upgrade process
func (u *UpgradeNodeTaintingProcessor) Process(ctx *context.AutoscalingContext, _ *clusterstate.ClusterStateRegistry, _ time.Time) error {
	eligibleNodes := UpgradeEligibleEdpNodes(ctx)

	filteredNodes := u.getFilteredNodes(eligibleNodes)
	klog.V(3).Infof("total node count: %d, for processing of edp upgrade taints", len(filteredNodes))

	// We need to notTargetGkeVersionTaint ~thousands of nodes in <24h.
	// Tainting ~10s per loop sequentially, we can easily satisfy that.
	// Tainting ~10s per loop doesn't increase the loop time significantly
	// (a single notTargetGkeVersionTaint call is cheap, and we do a lot of them during scale-down anyway).
	// It's safer than a background async loop because it doesn't interfere with other regular loop calls.
	for _, node := range filteredNodes {
		_, err := ca_taints.AddTaints(node.Node(), ctx.ClientSet, []v1.Taint{*notTargetGkeVersionTaint}, false)
		if err != nil {
			klog.Warningf("Error while tainting EDP node, %s, for upgrade: %q", node.Node().Name, err)
		}
	}
	return nil
}

func (u *UpgradeNodeTaintingProcessor) getFilteredNodes(nodes []*framework.NodeInfo) []*framework.NodeInfo {
	toBeProcessed := 0
	var filteredNodes []*framework.NodeInfo
	for _, node := range nodes {
		if toBeProcessed >= u.nodesProcessingPerLoop {
			break
		}
		if node.Node() == nil {
			continue
		}
		// if notTargetGkeVersionTaint already exists on the node. skip the node from processing
		if taints.TaintExists(node.Node().Spec.Taints, notTargetGkeVersionTaint) {
			continue
		}
		filteredNodes = append(filteredNodes, node)
		klog.V(4).Infof("added node: %s, for processing of edp upgrade taints", node.Node().Name)
		toBeProcessed++
	}
	return filteredNodes
}

func (u *UpgradeNodeTaintingProcessor) CleanUp() {
}
