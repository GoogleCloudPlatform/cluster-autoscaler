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
	"fmt"
	"net/http"
	"time"

	k8smetrics "k8s.io/component-base/metrics"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
)

const (
	cccEntityType          = "ccc"
	computeClassEntityType = "ComputeClass"

	entityTypeLabel = "entity_type"
	entityNameLabel = "entity_name"

	caNamespace = "cluster_autoscaler"
)

var (
	podSchedulingDuration = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Namespace: caNamespace,
			Name:      cccMetricName("pod_scheduling_duration_seconds"),
			Help:      "How long it takes for an initially unschedulable pod to actually be scheduled per CCC.",
			Buckets:   metrics.DurationBuckets1sTo24h,
		},
		[]string{entityTypeLabel, entityNameLabel, "out_of_resource"},
	)

	scaleUpAttempts = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      cccMetricName("cluster_node_provisioning_attempts_count"),
			Help:      "Number of node provisioning attempts per CCC.",
		},
		[]string{entityTypeLabel, entityNameLabel},
	)

	failedScaleUpAttempts = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      cccMetricName("cluster_node_provisioning_failures_count"),
			Help:      "Number of node provisioning failures per CCC.",
		},
		[]string{entityTypeLabel, entityNameLabel, "reason"},
	)
)
var (
	// A custom prometheus registry to export metrics per ccc on
	// a separate endpoint so that prom-to-sd can process entity_type and entity_name
	// labels correctly to solve for cardinality of pod ccc.
	registry = k8smetrics.NewKubeRegistry()
)

func init() {
	registry.MustRegister(podSchedulingDuration)
	registry.MustRegister(scaleUpAttempts)
	registry.MustRegister(failedScaleUpAttempts)
}

func MetricsRegistryHandler() http.Handler {
	return promhttp.HandlerFor(registry.Gatherer(), promhttp.HandlerOpts{})
}

// prometheusMetrics implements the interface of prometheus metrics
type prometheusMetrics struct{}

// Metrics surfaces the default prod metrics implementation
var Metrics = &prometheusMetrics{}

func (m *prometheusMetrics) ObservePodSchedulingDuration(duration time.Duration, stockout, cccName string) {
	podSchedulingDuration.WithLabelValues(cccEntityType, cccName, stockout).Observe(duration.Seconds())
}

func (m *prometheusMetrics) RegisterScaleUp(cccName string, delta int) {
	scaleUpAttempts.WithLabelValues(cccEntityType, cccName).Add(float64(delta))
}

func (m *prometheusMetrics) RegisterFailedScaleUp(cccName string, reason string) {
	failedScaleUpAttempts.WithLabelValues(cccEntityType, cccName, reason).Inc()
}

// cccMetricName adds the _per_ccc suffix to the given metric name.
func cccMetricName(baseName string) string {
	return fmt.Sprintf("%s_per_ccc", baseName)
}
