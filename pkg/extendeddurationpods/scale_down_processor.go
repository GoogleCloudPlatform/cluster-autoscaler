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
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
)

// ScaleDownProcessor is used to process EDP nodes passed to scale-down logic.
type ScaleDownProcessor struct {
}

// GetPodDestinationCandidates filters out all nodes containing labels.ExtendedDurationPodsLabel label
// and Kubernetes version of the node less than Kubernetes version of the master node
func (d *ScaleDownProcessor) GetPodDestinationCandidates(context *context.AutoscalingContext, nodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	filteredNodes := []*apiv1.Node{}
	nodesToFilterOut := UpgradeEligibleEdpNodes(context)
	nodesToFilterOutSet := make(map[string]bool)
	for _, node := range nodesToFilterOut {
		nodesToFilterOutSet[node.Node().Name] = true
	}
	for _, node := range nodes {
		if !nodesToFilterOutSet[node.Name] {
			filteredNodes = append(filteredNodes, node)
		}
	}
	return filteredNodes, nil
}

// GetScaleDownCandidates filters out nodes where pods with the EDP node selector are scheduled. These could either be pods with safe-to-evict=false,
// or pods with terminationGracePeriodSeconds>600 - we want to block scale-down for both kinds. We'd get the blocking for free with safe-to-evict=false,
// but it's easiest to just look at the node selector and cut both kinds from the candidates here. More details: go/extended-duration-pod-design.
func (d *ScaleDownProcessor) GetScaleDownCandidates(ctx *context.AutoscalingContext, nodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	var possibleCandidates = make([]*apiv1.Node, 0, len(nodes))

	for _, node := range nodes {
		if nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(node.Name); err == nil && nodeInfo != nil {
			if !hasEdpScheduled(nodeInfo) {
				possibleCandidates = append(possibleCandidates, node)
			}
		}
	}

	return possibleCandidates, nil
}

// hasEdpScheduled determines if there is a pod with the EDP node selector/affinity scheduled on the node.
func hasEdpScheduled(nodeInfo *framework.NodeInfo) bool {
	for _, podInfo := range nodeInfo.Pods() {
		if edpValue := EdpSelector(podInfo.Pod); edpValue != "" {
			return true
		}
	}
	return false
}

// CleanUp - No-op required
func (d *ScaleDownProcessor) CleanUp() {
}

// NewScaleDownProcessor instantiates a new object of the processor
func NewScaleDownProcessor() *ScaleDownProcessor {
	return &ScaleDownProcessor{}
}
