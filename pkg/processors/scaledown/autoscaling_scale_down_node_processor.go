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

package scaledown

import (
	"context"
	"reflect"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
)

// GkeInternalAutoscalingScaleDownNodeProcessor is an AutoscalingScaleDownNodeProcessor used in gke internal CA.
type GkeInternalAutoscalingScaleDownNodeProcessor struct {
	processors []nodes.ScaleDownNodeProcessor
}

// GetPodDestinationCandidates calls various GKE AutoscalingScaleDownNodeProcessors
func (p *GkeInternalAutoscalingScaleDownNodeProcessor) GetPodDestinationCandidates(ctx *ca_context.AutoscalingContext, nodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	var err errors.AutoscalerError
	for _, processor := range p.processors {
		nodes, err = processor.GetPodDestinationCandidates(ctx, nodes)
		if err != nil {
			klog.Errorf("Processor %v: GetPodDestinationCandidates failed with error: %v", reflect.TypeOf(processor), err)
			break
		}
	}
	return nodes, err
}

// GetScaleDownCandidates calls various GKE AutoscalingScaleDownNodeProcessors
func (p *GkeInternalAutoscalingScaleDownNodeProcessor) GetScaleDownCandidates(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, nodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	var err errors.AutoscalerError
	for _, processor := range p.processors {
		nodes, err = processor.GetScaleDownCandidates(ctx, autoscalingCtx, nodes)
		if err != nil {
			klog.Errorf("Processor %v: GetScaleDownCandidates failed with error: %v", reflect.TypeOf(processor), err)
			break
		}
	}
	return nodes, err
}

// CleanUp calls various GKE AutoscalingScaleDownNodeProcessors
func (p *GkeInternalAutoscalingScaleDownNodeProcessor) CleanUp() {
	for _, processor := range p.processors {
		processor.CleanUp()
	}
}

// NewGkeInternalAutoscalingScaleDownNodeProcessor creates GkeInternalAutoscalingScaleDownNodeProcessor
func NewGkeInternalAutoscalingScaleDownNodeProcessor(processors []nodes.ScaleDownNodeProcessor) *GkeInternalAutoscalingScaleDownNodeProcessor {
	return &GkeInternalAutoscalingScaleDownNodeProcessor{processors: processors}
}

// DefragScaleDownNodeProcessorProvider is implemented by ScaleDownNodeProcessors whose regular
// behavior is not suitable for defrag, and which offer a defrag-specific variant instead.
type DefragScaleDownNodeProcessorProvider interface {
	DefragScaleDownNodeProcessor() nodes.ScaleDownNodeProcessor
}

// NewDefragScaleDownNodeProcessor builds the defrag chain out of the regular scale-down chain,
// swapping every processor implementing DefragScaleDownNodeProcessorProvider for its defrag
// variant. Deriving both chains from a single slice keeps them from drifting apart.
func NewDefragScaleDownNodeProcessor(processors []nodes.ScaleDownNodeProcessor) *GkeInternalAutoscalingScaleDownNodeProcessor {
	defragProcessors := make([]nodes.ScaleDownNodeProcessor, 0, len(processors))
	for _, processor := range processors {
		if provider, ok := processor.(DefragScaleDownNodeProcessorProvider); ok {
			processor = provider.DefragScaleDownNodeProcessor()
		}
		defragProcessors = append(defragProcessors, processor)
	}
	return NewGkeInternalAutoscalingScaleDownNodeProcessor(defragProcessors)
}
