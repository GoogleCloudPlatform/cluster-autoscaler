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

package ccc

import (
	"strconv"

	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/podstate/types"
)

const (
	metricFQName        = "cluster_autoscaler_pending_pods_per_ccc"
	clusterMetricFQName = "cluster_autoscaler_cluster_pending_pods_per_ccc"
)

var (
	pendingPodsMetricDesc = prometheus.NewDesc(
		metricFQName,
		"Number of pending pods of various kinds in the cluster.",
		[]string{entityTypeLabel, entityNameLabel, "kind", "system_pod"},
		nil,
	)
	clusterPendingPodsMetricDesc = prometheus.NewDesc(
		clusterMetricFQName,
		"Number of pending pods in the cluster, broken down by reason, type and Custom Compute Class entity.",
		[]string{entityTypeLabel, entityNameLabel, "reason", "type"},
		nil,
	)
)

// PendingPodsPerCccCalculationFunc is a function that calculates and returns a slice of pending pod metrics.
type PendingPodsPerCccCalculationFunc func() []types.PendingPodsPerCccMetric

// PendingPodsPerCccCollector implements prometheus.Collector.
type pendingPodsPerCccCollector struct {
	metricsCalculationFunc PendingPodsPerCccCalculationFunc
}

// NewPendingPodsPerCccCollector creates a new collector instance.
func NewPendingPodsPerCccCollector(calcFunc PendingPodsPerCccCalculationFunc) k8smetrics.Registerable {
	return &pendingPodsPerCccCollector{
		metricsCalculationFunc: calcFunc,
	}
}

// Describe implements the prometheus.Collector interface.
func (c *pendingPodsPerCccCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- pendingPodsMetricDesc
	ch <- clusterPendingPodsMetricDesc
}

// Collect implements the prometheus.Collector interface.
func (c *pendingPodsPerCccCollector) Collect(ch chan<- prometheus.Metric) {
	metrics := c.metricsCalculationFunc()

	for _, m := range metrics {
		labels := []string{
			cccEntityType,
			m.CccName,
			string(m.Kind),
			strconv.FormatBool(m.SystemPod),
		}

		ch <- prometheus.MustNewConstMetric(
			pendingPodsMetricDesc,
			prometheus.GaugeValue,
			float64(m.Count),
			labels...,
		)

		var podType string
		if m.SystemPod {
			podType = "system_pod"
		} else {
			podType = "user_pod"
		}

		clusterLabels := []string{
			computeClassEntityType,
			m.CccName,
			string(m.Kind),
			podType,
		}

		ch <- prometheus.MustNewConstMetric(
			clusterPendingPodsMetricDesc,
			prometheus.GaugeValue,
			float64(m.Count),
			clusterLabels...,
		)
	}
}

// Create implements the k8smetrics.Registerable interface.
func (c *pendingPodsPerCccCollector) Create(version *semver.Version) bool {
	return true
}

// ClearState implements the k8smetrics.Registerable interface.
func (c *pendingPodsPerCccCollector) ClearState() {
	// No-op for stateless collector
}

// FQName implements the k8smetrics.Registerable interface.
func (c *pendingPodsPerCccCollector) FQName() string {
	return metricFQName
}

func RegisterPendingPodsPerCccCollector(metricsCalculationFunc PendingPodsPerCccCalculationFunc) {
	collector := NewPendingPodsPerCccCollector(metricsCalculationFunc)
	registry.MustRegister(collector)
}
