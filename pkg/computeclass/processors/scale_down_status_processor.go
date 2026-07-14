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
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/klog/v2"
)

type scaleDownStatusMetrics interface {
	IncreaseScaledDownNodesPerRule(string, int)
}

type crdScaleDownStatusProvider interface {
	IsAutopilotEnabled() bool
}

// crdScaleDownStatusProcessor is responsible for emitting relevant events for crd
// depending on the post scale-down status.
type crdScaleDownStatusProcessor struct {
	lister  lister.Lister
	matcher computeclass.Matcher
	metrics scaleDownStatusMetrics
}

// NewCrdScaleDownStatusProcessor return crdScaleDownStatusProcessor
func NewCrdScaleDownStatusProcessor(lister lister.Lister, provider crdScaleDownStatusProvider, metrics scaleDownStatusMetrics) *crdScaleDownStatusProcessor {
	return &crdScaleDownStatusProcessor{
		lister:  lister,
		matcher: computeclass.NewMatcher(lister, provider),
		metrics: metrics,
	}
}

// Process processes the state of the cluster after a scale-down
func (p *crdScaleDownStatusProcessor) Process(_ *context.AutoscalingContext, scaleDownStatus *status.ScaleDownStatus) {
	if scaleDownStatus == nil || scaleDownStatus.Result != status.ScaleDownNodeDeleteStarted {
		return
	}
	for _, scaledDownNode := range scaleDownStatus.ScaledDownNodes {
		ruleIndex, crd, err := getRuleIndexForMetrics(scaledDownNode.NodeGroup, p.lister, p.matcher)
		if err != nil {
			klog.Errorf("scale down status for crd error: %v", err)
			continue
		}

		if crd == nil {
			// crd is nil, no sense in metrics
			continue
		}

		p.metrics.IncreaseScaledDownNodesPerRule(crd.CrdType(), ruleIndex)
	}
}

// CleanUp cleans up the processor's internal structures.
func (p *crdScaleDownStatusProcessor) CleanUp() {
}
