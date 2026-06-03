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
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	ek "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/processor"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/utils"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

type ScaleDownNodeProcessor struct {
	experimentsManager experiments.Manager
}

func NewScaleDownNodeProcessor(experimentsManager experiments.Manager) *ScaleDownNodeProcessor {
	return &ScaleDownNodeProcessor{
		experimentsManager: experimentsManager,
	}
}

// GetPodDestinationCandidates filters out nodes which contain lookahead pods.
func (p *ScaleDownNodeProcessor) GetPodDestinationCandidates(ctx *context.AutoscalingContext, nodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	if !p.experimentsManager.DirectLaunchBoolFlag(experiments.EkPreventScheduleOnLookaheadNodes) {
		return nodes, nil
	}

	var candidates []*apiv1.Node

	for _, node := range nodes {
		if isPodDestinationCandidate(ctx, node) {
			candidates = append(candidates, node)
		}
	}

	return candidates, nil
}

func isPodDestinationCandidate(ctx *context.AutoscalingContext, node *apiv1.Node) bool {
	// Filter out nil pointers.
	if node == nil {
		return false
	}
	// Lookahead pods can only be scheduled on EK machines.
	if isEk, err := utils.IsEkMachine(node); !isEk || err != nil {
		return true
	}
	info, err := ctx.ClusterSnapshot.GetNodeInfo(node.Name)
	// Let's consider nodes for which we fail to obtain info
	// as potential pod destinations.
	if err != nil {
		return true
	}

	if ek.HasLookaheadPods(info) {
		return false
	}
	return true
}

// GetScaleDownCandidates should be a no-op.
func (p *ScaleDownNodeProcessor) GetScaleDownCandidates(_ *context.AutoscalingContext, nodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	return nodes, nil
}

// CleanUp should be a no-op.
func (p *ScaleDownNodeProcessor) CleanUp() {
}
