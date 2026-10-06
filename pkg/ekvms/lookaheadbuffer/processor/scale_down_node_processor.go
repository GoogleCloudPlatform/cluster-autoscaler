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

package processor

import (
	"context"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/processor"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/utils"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
)

type ScaleDownNodeProcessor struct {
	experimentsManager experiments.Manager
	mcp                *machinetypes.MachineConfigProvider
}

func NewScaleDownNodeProcessor(mcp *machinetypes.MachineConfigProvider, experimentsManager experiments.Manager) *ScaleDownNodeProcessor {
	return &ScaleDownNodeProcessor{
		experimentsManager: experimentsManager,
		mcp:                mcp,
	}
}

// GetPodDestinationCandidates filters out nodes which contain lookahead pods.
func (p *ScaleDownNodeProcessor) GetPodDestinationCandidates(ctx *ca_context.AutoscalingContext, nodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	var candidates []*apiv1.Node

	for _, node := range nodes {
		if p.isPodDestinationCandidate(ctx, node) {
			candidates = append(candidates, node)
		}
	}

	return candidates, nil
}

func (p *ScaleDownNodeProcessor) isPodDestinationCandidate(ctx *ca_context.AutoscalingContext, node *apiv1.Node) bool {
	// Filter out nil pointers.
	if node == nil {
		return false
	}

	// Lookahead pods can only be scheduled on resizable machines.
	isResizable, err := utils.IsResizableNode(node, p.mcp)
	if err != nil || !isResizable {
		return true
	}

	if !processor.PreventScheduleOnLookaheadNode(p.experimentsManager, node) {
		return true
	}

	info, err := ctx.ClusterSnapshot.GetNodeInfo(node.Name)
	// Let's consider nodes for which we fail to obtain info
	// as potential pod destinations.
	if err != nil {
		return true
	}

	if processor.HasLookaheadPods(info) {
		return false
	}
	return true
}

// GetScaleDownCandidates should be a no-op.
func (p *ScaleDownNodeProcessor) GetScaleDownCandidates(ctx context.Context, _ *ca_context.AutoscalingContext, nodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	return nodes, nil
}

// CleanUp should be a no-op.
func (p *ScaleDownNodeProcessor) CleanUp() {
}
