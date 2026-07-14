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
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/klog/v2"
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
func (p *CrdScaleUpStatusProcessor) Process(context *context.AutoscalingContext, scaleUpStatus *status.ScaleUpStatus) {
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
