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

package metrics_processors

import (
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/filter"
)

// MetricsFilterScaleUpProcessor returns info on stockouts and quota issues to
// the MetricsFilter.
type MetricsFilterScaleUpProcessor struct {
	metricsFilter filter.MetricsFilter
}

// Process processes scale up failures to inform the MetricsFilter.
func (m *MetricsFilterScaleUpProcessor) Process(_ *context.AutoscalingContext, csr *clusterstate.ClusterStateRegistry, _ time.Time) error {
	for nodeGroupId, failures := range csr.GetScaleUpFailures() {
		for _, failure := range failures {
			reason := metrics.FailedScaleUpReason(failure.ErrorInfo.ErrorCode)
			switch reason {
			case gce.ErrorCodeResourcePoolExhausted:
				m.metricsFilter.ObserveNodeGroupStockOut(nodeGroupId)
			case gce.ErrorCodeQuotaExceeded, gce.ErrorIPSpaceExhausted, gkeclient.ServiceAccountDeleted:
				m.metricsFilter.ObserveNodeGroupFilterableIssue(nodeGroupId)
			}
		}
	}
	return nil
}

// CleanUp cleans up the processor
func (m *MetricsFilterScaleUpProcessor) CleanUp() {}

// NewMetricsFilterScaleUpProcessor returns a MetricsFilterScaleUpProcessor
func NewMetricsFilterScaleUpProcessor(filter filter.MetricsFilter) *MetricsFilterScaleUpProcessor {
	return &MetricsFilterScaleUpProcessor{
		metricsFilter: filter,
	}
}
