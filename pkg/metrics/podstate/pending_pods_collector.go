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

package podstate

import (
	"strconv"

	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/podstate/types"
)

const (
	metricFQName = "cluster_autoscaler_pending_pods"
)

var (
	//Descriptor of pending_pods metric used by Prometheus
	pendingPodsMetricDesc = prometheus.NewDesc(
		metricFQName,
		"Number of pending pods of various kinds in the cluster.",
		[]string{"kind", "system_pod"},
		nil,
	)
)

// PendingPodsCalculationFunc is a function that calculates and returns a slice of pending pod metrics.
type PendingPodsCalculationFunc func() []types.PendingPodsMetric

// pendingPodsCollector is a prometheus.Collector that collects pending pod metrics
// by invoking a provided PendingPodsCalculationFunc.
type pendingPodsCollector struct {
	metricsCalculationFunc PendingPodsCalculationFunc
}

// Describe implements the prometheus.Collector interface.
func (c *pendingPodsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- pendingPodsMetricDesc
}

// Collect implements the prometheus.Collector interface.
func (c *pendingPodsCollector) Collect(ch chan<- prometheus.Metric) {
	metrics := c.metricsCalculationFunc()
	for _, m := range metrics {
		ch <- prometheus.MustNewConstMetric(
			pendingPodsMetricDesc,
			prometheus.GaugeValue,
			float64(m.Count),
			string(m.Kind),
			strconv.FormatBool(m.SystemPod),
		)
	}
}

// Create implements the k8smetrics.Registerable interface.
func (c *pendingPodsCollector) Create(version *semver.Version) bool {
	return true
}

// ClearState implements the k8smetrics.Registerable interface.
func (c *pendingPodsCollector) ClearState() {
	// No-op for stateless collector
}

// FQName implements the k8smetrics.Registerable interface.
func (c *pendingPodsCollector) FQName() string {
	return metricFQName
}

// RegisterPendingPodsCollector registers a collector for the pending_pods_stateless metric.
func RegisterPendingPodsCollector(metricsCalculationFunc PendingPodsCalculationFunc) {
	legacyregistry.MustRegister(&pendingPodsCollector{
		metricsCalculationFunc: metricsCalculationFunc,
	})
}
