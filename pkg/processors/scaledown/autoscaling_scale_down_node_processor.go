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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/observability"
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

// ScaleDownCandidateStages attributes each node removed from the scale-down
// candidate set to the processor in the chain that removed it.
//
// The chain as a whole only returns the surviving nodes, which makes it
// impossible to tell why any particular node was excluded. This type recovers
// that information for observability purposes.
type ScaleDownCandidateStages struct {
	// ProcessorNames holds the fully qualified type names of the processors in
	// the chain, in the order they ran, for example
	// "sigs.k8s.io/cluster-autoscaler/pkg/processors/nodes.PreFilteringScaleDownNodeProcessor".
	ProcessorNames []string
	// ExclusionReasons holds, at the same index as ProcessorNames, the reason
	// declared by that processor through ExclusionReasonProvider. Processors
	// that do not implement that interface leave an empty reason here, and the
	// consumer decides how to report them.
	ExclusionReasons []observability.BlockReason
	// DroppedBy maps a node name to the index in ProcessorNames of the
	// processor that removed it. Nodes that survived the whole chain are absent.
	DroppedBy map[string]int
}

// ProcessorFor returns the name of the processor that removed the given node.
func (s *ScaleDownCandidateStages) ProcessorFor(nodeName string) (string, bool) {
	idx, ok := s.indexFor(nodeName)
	if !ok {
		return "", false
	}
	return s.ProcessorNames[idx], true
}

// ReasonFor returns the exclusion reason declared by the processor that removed
// the given node.
//
// The second result is false when the node was not removed, or when the
// processor that removed it does not declare a reason. Callers must supply
// their own fallback in that case so that no excluded node goes unaccounted for.
func (s *ScaleDownCandidateStages) ReasonFor(nodeName string) (observability.BlockReason, bool) {
	idx, ok := s.indexFor(nodeName)
	if !ok || idx >= len(s.ExclusionReasons) {
		return "", false
	}
	reason := s.ExclusionReasons[idx]
	if reason == "" {
		return "", false
	}
	return reason, true
}

// indexFor returns the validated index of the processor that removed the node.
func (s *ScaleDownCandidateStages) indexFor(nodeName string) (int, bool) {
	if s == nil {
		return 0, false
	}
	idx, ok := s.DroppedBy[nodeName]
	if !ok || idx < 0 || idx >= len(s.ProcessorNames) {
		return 0, false
	}
	return idx, true
}

// ProcessorName returns a stable, fully qualified name for a processor.
//
// reflect.Type.String() is deliberately not used here: it renders only the
// short package name, and several distinct packages in this binary export a
// type called ScaleDownNodeProcessor. Callers that map processors to
// user-visible reasons would not be able to tell those apart.
//
// It is exported so that callers building such a mapping can key it on the
// processor type rather than on a copy of the name, which would silently stop
// matching if the type were renamed or moved.
func ProcessorName(processor nodes.ScaleDownNodeProcessor) string {
	t := reflect.TypeOf(processor)
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == nil {
		return "unknown"
	}
	if pkgPath := t.PkgPath(); pkgPath != "" {
		return pkgPath + "." + t.Name()
	}
	return t.String()
}

// GetScaleDownCandidatesWithStages behaves exactly like GetScaleDownCandidates,
// but additionally reports which processor removed each excluded node.
//
// It is intended for callers that need to explain exclusions to users, such as
// defrag migration reporting. It costs one extra map of node names plus one
// pass over the surviving nodes per processor, so callers that do not need the
// attribution should use GetScaleDownCandidates instead.
func (p *GkeInternalAutoscalingScaleDownNodeProcessor) GetScaleDownCandidatesWithStages(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, nodes []*apiv1.Node) ([]*apiv1.Node, *ScaleDownCandidateStages, errors.AutoscalerError) {
	stages := &ScaleDownCandidateStages{
		ProcessorNames:   make([]string, 0, len(p.processors)),
		ExclusionReasons: make([]observability.BlockReason, 0, len(p.processors)),
		DroppedBy:        make(map[string]int, len(nodes)),
	}

	// lastSurvived records the index of the last processor a node made it past,
	// so that a single map serves the whole chain instead of one set per stage.
	lastSurvived := make(map[string]int, len(nodes))
	for _, node := range nodes {
		lastSurvived[node.Name] = -1
	}

	var err errors.AutoscalerError
	for i, processor := range p.processors {
		stages.ProcessorNames = append(stages.ProcessorNames, ProcessorName(processor))
		stages.ExclusionReasons = append(stages.ExclusionReasons, exclusionReason(processor))
		nodes, err = processor.GetScaleDownCandidates(ctx, autoscalingCtx, nodes)
		if err != nil {
			klog.Errorf("Processor %v: GetScaleDownCandidates failed with error: %v", reflect.TypeOf(processor), err)
			break
		}
		for _, node := range nodes {
			lastSurvived[node.Name] = i
		}
	}

	// A node was dropped by the processor immediately after the last one it
	// survived. Nodes that survived every processor are left out.
	lastRun := len(stages.ProcessorNames) - 1
	if err != nil {
		// A processor that failed dropped nothing, so the nodes that survived
		// everything before it are still in the running rather than excluded,
		// and blaming them on it would invent a reason. What the processors
		// that did complete established stays valid.
		lastRun--
	}
	for name, survived := range lastSurvived {
		if survived < lastRun {
			stages.DroppedBy[name] = survived + 1
		}
	}
	return nodes, stages, err
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
