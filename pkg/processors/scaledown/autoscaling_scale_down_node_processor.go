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
	"reflect"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodes"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/klog/v2"
)

// GkeInternalAutoscalingScaleDownNodeProcessor is an AutoscalingScaleDownNodeProcessor used in gke internal CA.
type GkeInternalAutoscalingScaleDownNodeProcessor struct {
	processors []nodes.ScaleDownNodeProcessor
}

// GetPodDestinationCandidates calls various GKE AutoscalingScaleDownNodeProcessors
func (p *GkeInternalAutoscalingScaleDownNodeProcessor) GetPodDestinationCandidates(ctx *context.AutoscalingContext, nodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
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
func (p *GkeInternalAutoscalingScaleDownNodeProcessor) GetScaleDownCandidates(ctx *context.AutoscalingContext, nodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	var err errors.AutoscalerError
	for _, processor := range p.processors {
		nodes, err = processor.GetScaleDownCandidates(ctx, nodes)
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
