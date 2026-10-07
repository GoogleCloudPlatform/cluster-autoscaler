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

package churntracker

import (
	"context"

	apiv1 "k8s.io/api/core/v1"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/status"
)

// ChurnScaleDownStatusProcessor passes scaled-down nodes to the ChurnTracker so it can record lookahead churn events.
type ChurnScaleDownStatusProcessor struct {
	churnTracker *ChurnTracker
}

// NewChurnScaleDownStatusProcessor returns a new ChurnScaleDownStatusProcessor.
func NewChurnScaleDownStatusProcessor(churnTracker *ChurnTracker) *ChurnScaleDownStatusProcessor {
	return &ChurnScaleDownStatusProcessor{churnTracker: churnTracker}
}

// Process records lookahead churn events for the nodes scaled down in this loop.
func (p *ChurnScaleDownStatusProcessor) Process(_ context.Context, _ *ca_context.AutoscalingContext, scaleDownStatus *status.ScaleDownStatus) {
	if p.churnTracker == nil || scaleDownStatus == nil || len(scaleDownStatus.ScaledDownNodes) == 0 {
		return
	}
	nodes := make([]*apiv1.Node, 0, len(scaleDownStatus.ScaledDownNodes))
	for _, scaledDownNode := range scaleDownStatus.ScaledDownNodes {
		if scaledDownNode != nil {
			nodes = append(nodes, scaledDownNode.Node)
		}
	}
	p.churnTracker.RecordScaledDownNodes(nodes)
}

// CleanUp cleans up the processor's internal structures.
func (p *ChurnScaleDownStatusProcessor) CleanUp() {}
