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
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	crd_status "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status/history"
	edps "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/extendeddurationpods"
	metrics_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/nodequota"
	viz_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/processors"
	klog "k8s.io/klog/v2"
)

// GkeInternalAutoscalingStatusProcessor is an AutoscalingStatusProcessor used in gke internal CA.
type GkeInternalAutoscalingStatusProcessor struct {
	quotaProcessor                  *nodequota.NodeQuotaProcessor
	vizProcessor                    *viz_processors.AutoscalingStatusVisibilityProcessor
	metricsFilterProcessor          *metrics_processors.MetricsFilterScaleUpProcessor
	edpUpgradeNodeTaintingProcessor *edps.UpgradeNodeTaintingProcessor
	edpMetrics                      *edps.Metrics
	observabilityProcessor          *crd_status.CrdResourceReportingProcessor
	autoscalingHistoryProcessor     *history.AutoscalingStatusHistoryProcessor
}

// Process calls various GKE AutoscalingStatusProcessors
func (p *GkeInternalAutoscalingStatusProcessor) Process(context *context.AutoscalingContext, csr *clusterstate.ClusterStateRegistry, now time.Time) error {
	var err error
	if p.quotaProcessor != nil {
		err = p.quotaProcessor.Process(context, csr, now)
		if err != nil {
			klog.Errorf("Node quota processor failed with error: %v", err)
		}
	}
	if p.vizProcessor != nil {
		vizErr := p.vizProcessor.Process(context, csr, now)
		if vizErr != nil {
			klog.Errorf("Visibility status processor failed with error: %v", vizErr)
			// if at least one error is not nil return error;
			// we can't really do much about those errors on top of logging them
			// and it doesn't feel worth it trying to try and aggregate them
			err = vizErr
		}
	}
	if p.metricsFilterProcessor != nil {
		metricsErr := p.metricsFilterProcessor.Process(context, csr, now)
		if metricsErr != nil {
			klog.Errorf("Metrics status processor failed with error: %v", metricsErr)
			// same as above. log the error and override the last error.
			err = metricsErr
		}
	}
	if p.edpUpgradeNodeTaintingProcessor != nil {
		edpErr := p.edpUpgradeNodeTaintingProcessor.Process(context, csr, now)
		if edpErr != nil {
			klog.Errorf("Edp Upgrade node taint processor failed with error: %v", edpErr)
			// same as above. log the error and override the last error.
			err = edpErr
		}
	}
	if p.edpMetrics != nil {
		edpErr := p.edpMetrics.Process(context, csr, now)
		if edpErr != nil {
			klog.Errorf("Edp Metrics failed with error: %v", edpErr)
			// same as above. log the error and override the last error.
			err = edpErr
		}
	}
	if p.observabilityProcessor != nil {
		processorErr := p.observabilityProcessor.Process(context, csr, now)
		if processorErr != nil {
			klog.Errorf("Observability processor failed: %v", processorErr)
			err = processorErr
		}
	}
	if p.autoscalingHistoryProcessor != nil {
		historyErr := p.autoscalingHistoryProcessor.Process(context, csr, now)
		if historyErr != nil {
			klog.Errorf("Autoscaling history processor failed with error: %v", historyErr)
			err = historyErr
		}
	}
	return err
}

// CleanUp calls various GKE AutoscalingStatusProcessors
func (p *GkeInternalAutoscalingStatusProcessor) CleanUp() {
	if p.quotaProcessor != nil {
		p.quotaProcessor.CleanUp()
	}
	if p.vizProcessor != nil {
		p.vizProcessor.CleanUp()
	}
	if p.metricsFilterProcessor != nil {
		p.metricsFilterProcessor.CleanUp()
	}
	if p.edpUpgradeNodeTaintingProcessor != nil {
		p.edpUpgradeNodeTaintingProcessor.CleanUp()
	}
	if p.edpMetrics != nil {
		p.edpMetrics.CleanUp()
	}
	if p.autoscalingHistoryProcessor != nil {
		p.autoscalingHistoryProcessor.CleanUp()
	}
}

// NewGkeInternalAutoscalingStatusProcessor creates GkeInternalAutoscalingStatusProcessor
func NewGkeInternalAutoscalingStatusProcessor(
	quotaProcessor *nodequota.NodeQuotaProcessor,
	vizProcessor *viz_processors.AutoscalingStatusVisibilityProcessor,
	metricsProcessor *metrics_processors.MetricsFilterScaleUpProcessor,
	edpUpgradeNodeTaintingProcessor *edps.UpgradeNodeTaintingProcessor,
	edpMetrics *edps.Metrics,
	observabilityProcessor *status.CrdResourceReportingProcessor,
	autoscalingHistoryProcessor *history.AutoscalingStatusHistoryProcessor) *GkeInternalAutoscalingStatusProcessor {
	return &GkeInternalAutoscalingStatusProcessor{
		quotaProcessor:                  quotaProcessor,
		vizProcessor:                    vizProcessor,
		metricsFilterProcessor:          metricsProcessor,
		edpUpgradeNodeTaintingProcessor: edpUpgradeNodeTaintingProcessor,
		edpMetrics:                      edpMetrics,
		observabilityProcessor:          observabilityProcessor,
		autoscalingHistoryProcessor:     autoscalingHistoryProcessor,
	}
}
