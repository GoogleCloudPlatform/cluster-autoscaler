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

package binpacking

import (
	"context"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/expander"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/binpacking"
)

type binpackingMetrics interface {
	ObserveBinpackingNodeGroupTotal(int)
	ObserveBinpackingNodeGroupProcessed(int)
	ObserveBinpackingNodeGroupSkipped(int)
}

// binpackingMetricsProcessor aggregates number of node groups injected and
// processed by binpacking algorithm. It implements BinpackingLimiter.
type binpackingMetricsProcessor struct {
	binpackingLimiter binpacking.BinpackingLimiter
	metrics           binpackingMetrics

	totalNodeGroupsCount     int
	processedNodeGroupsCount int
}

// NewBinpackingMetricsProcessor returns a new binpackingMetricsProcessor
func NewBinpackingMetricsProcessor(binpackingLimiter binpacking.BinpackingLimiter) *binpackingMetricsProcessor {
	return &binpackingMetricsProcessor{
		binpackingLimiter: binpackingLimiter,
		metrics:           metrics.Metrics,
	}
}

// InitBinpacking initializes the binpacking algorithm.
func (p *binpackingMetricsProcessor) InitBinpacking(context *ca_context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup) {
	p.totalNodeGroupsCount = len(nodeGroups)
	p.processedNodeGroupsCount = 0

	p.binpackingLimiter.InitBinpacking(context, nodeGroups)
}

// MarkProcessed is called both after a node group is skipped and after the node
// group is processed.
func (p *binpackingMetricsProcessor) MarkProcessed(context *ca_context.AutoscalingContext, nodegroupId string) {
	p.binpackingLimiter.MarkProcessed(context, nodegroupId)
}

// StopBinpacking is called only after the node group is processed.
func (p *binpackingMetricsProcessor) StopBinpacking(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, evaluatedOptions []expander.Option) bool {
	p.processedNodeGroupsCount += 1
	return p.binpackingLimiter.StopBinpacking(ctx, autoscalingCtx, evaluatedOptions)
}

// FinalizeBinpacking finalizes the binpacking algorithm.
func (p *binpackingMetricsProcessor) FinalizeBinpacking(context *ca_context.AutoscalingContext, finalOptions []expander.Option) {
	p.metrics.ObserveBinpackingNodeGroupTotal(p.totalNodeGroupsCount)
	p.metrics.ObserveBinpackingNodeGroupProcessed(p.processedNodeGroupsCount)
	p.metrics.ObserveBinpackingNodeGroupSkipped(p.totalNodeGroupsCount - p.processedNodeGroupsCount)

	p.binpackingLimiter.FinalizeBinpacking(context, finalOptions)
}
