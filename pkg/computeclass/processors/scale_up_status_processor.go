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

package processors

import (
	"context"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/klog/v2"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
)

type scaleUpStatusMetrics interface {
	IncreaseScaledUpNodesPerRule(string, int, int)
}

type crdScaleUpStatusProvider interface {
	IsAutopilotEnabled() bool
}

// CrdScaleUpStatusProcessor is responsible for emitting relevant events for crd
// depending on the post scale-up status.
type CrdScaleUpStatusProcessor struct {
	lister  lister.Lister
	matcher computeclass.Matcher
	metrics scaleUpStatusMetrics
}

// NewCrdScaleUpStatusProcessor return crdScaleUpStatusProcessor.
func NewCrdScaleUpStatusProcessor(lister lister.Lister, provider crdScaleUpStatusProvider, metrics scaleUpStatusMetrics) *CrdScaleUpStatusProcessor {
	return &CrdScaleUpStatusProcessor{
		lister:  lister,
		matcher: computeclass.NewMatcher(lister, provider),
		metrics: metrics,
	}
}

// Process processes the state of the cluster after a scale-up
func (p *CrdScaleUpStatusProcessor) Process(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, scaleUpStatus *status.ScaleUpStatus) {
	if scaleUpStatus == nil || scaleUpStatus.Result != status.ScaleUpSuccessful {
		return
	}
	for _, scaleUpInfo := range scaleUpStatus.ScaleUpInfos {
		nodesScaledUp := scaleUpInfo.NewSize - scaleUpInfo.CurrentSize
		ruleIndex, crd, err := getRuleIndexForMetrics(scaleUpInfo.Group, p.lister, p.matcher)
		if err != nil {
			klog.Errorf("scale up status for crd error: %v", err)
			continue
		}

		if crd == nil {
			// crd is nil, no sense in metrics
			continue
		}

		p.metrics.IncreaseScaledUpNodesPerRule(crd.CrdType(), ruleIndex, nodesScaledUp)
	}
}

// CleanUp cleans up the processor's internal structures.
func (p *CrdScaleUpStatusProcessor) CleanUp() {
}

// MinCapacityFilteringScaleUpStatusProcessor is a ScaleUpStatusProcessor used for filtering out
// ComputeClass minimum capacity fake pods from scale-up status so that K8s events and visibility
// logs are not emitted for synthetic pods.
type MinCapacityFilteringScaleUpStatusProcessor struct{}

// NewMinCapacityFilteringScaleUpStatusProcessor returns a new MinCapacityFilteringScaleUpStatusProcessor.
func NewMinCapacityFilteringScaleUpStatusProcessor() *MinCapacityFilteringScaleUpStatusProcessor {
	return &MinCapacityFilteringScaleUpStatusProcessor{}
}

// Process updates scaleUpStatus to remove all min capacity fake pods from
// PodsRemainUnschedulable, PodsAwaitEvaluation, and PodsTriggeredScaleUp.
func (p *MinCapacityFilteringScaleUpStatusProcessor) Process(ctx context.Context, _ *ca_context.AutoscalingContext, scaleUpStatus *status.ScaleUpStatus) {
	if scaleUpStatus == nil {
		return
	}
	scaleUpStatus.PodsRemainUnschedulable = filterMinCapacityFakePods(scaleUpStatus.PodsRemainUnschedulable, func(noScaleUpInfo status.NoScaleUpInfo) *apiv1.Pod { return noScaleUpInfo.Pod })
	scaleUpStatus.PodsAwaitEvaluation = filterMinCapacityFakePods(scaleUpStatus.PodsAwaitEvaluation, func(pod *apiv1.Pod) *apiv1.Pod { return pod })
	scaleUpStatus.PodsTriggeredScaleUp = filterMinCapacityFakePods(scaleUpStatus.PodsTriggeredScaleUp, func(pod *apiv1.Pod) *apiv1.Pod { return pod })
}

// CleanUp cleans up the processor's internal structures.
func (p *MinCapacityFilteringScaleUpStatusProcessor) CleanUp() {}

func filterMinCapacityFakePods[T any](items []T, getPod func(T) *apiv1.Pod) []T {
	if len(items) == 0 {
		return items
	}
	filtered := make([]T, 0, len(items))
	for _, item := range items {
		if !IsMinCapacityFakePod(getPod(item)) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}
