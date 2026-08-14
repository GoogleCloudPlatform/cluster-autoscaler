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

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/filter"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/gce"
)

// MetricsFilterScaleUpProcessor returns info on stockouts and quota issues to
// the MetricsFilter.
type MetricsFilterScaleUpProcessor struct {
	metricsFilter filter.MetricsFilter
}

// NewMetricsFilterScaleUpProcessor returns a MetricsFilterScaleUpProcessor
func NewMetricsFilterScaleUpProcessor(filter filter.MetricsFilter) *MetricsFilterScaleUpProcessor {
	return &MetricsFilterScaleUpProcessor{
		metricsFilter: filter,
	}
}

// RegisterFailedScaleUp records failed scale-up for a nodegroup to inform the MetricsFilter.
func (m *MetricsFilterScaleUpProcessor) RegisterFailedScaleUp(nodeGroup cloudprovider.NodeGroup, _ int, errorInfo cloudprovider.InstanceErrorInfo, _ time.Time) {
	if nodeGroup == nil || m.metricsFilter == nil {
		return
	}
	switch errorInfo.ErrorCode {
	case gce.ErrorCodeResourcePoolExhausted:
		m.metricsFilter.ObserveNodeGroupStockOut(nodeGroup.Id())
	case gce.ErrorCodeQuotaExceeded, gce.ErrorIPSpaceExhausted, gkeclient.ServiceAccountDeleted:
		m.metricsFilter.ObserveNodeGroupFilterableIssue(nodeGroup.Id())
	}
}

// RegisterScaleUp records when scale up happened for a nodegroup.
func (m *MetricsFilterScaleUpProcessor) RegisterScaleUp(_ cloudprovider.NodeGroup, _ int, _ time.Time) {
}

// RegisterScaleDown records when scale down happened for a nodegroup.
func (m *MetricsFilterScaleUpProcessor) RegisterScaleDown(_ cloudprovider.NodeGroup, _ string, _ time.Time, _ time.Time) {
}

// RegisterFailedScaleDown records failed scale-down for a nodegroup.
func (m *MetricsFilterScaleUpProcessor) RegisterFailedScaleDown(_ cloudprovider.NodeGroup, _ string, _ time.Time) {
}
