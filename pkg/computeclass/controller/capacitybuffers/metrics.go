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

package capacitybuffers

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	cbv1beta1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	metricsNamespace = "cluster_autoscaler"

	// BufferStatus constants identify the categorical status buckets for capacity buffers.
	bufferStatusAccepted     = "accepted"
	bufferStatusProvisioning = "provisioning"
	bufferStatusFailed       = "failed"
	bufferStatusUnprocessed  = "unprocessed"
)

var (
	knownBufferMetricStrategies = []string{activeCapacityStrategy, standbyCapacityStrategy, "custom"}
	knownBufferMetricStatuses   = []string{bufferStatusAccepted, bufferStatusProvisioning, bufferStatusFailed, bufferStatusUnprocessed}

	reconcileDuration = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "compute_class_capacity_buffers_reconcile_duration_seconds",
			Help:      "Time taken to reconcile capacity buffers for a single ComputeClass.",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0},
		},
		[]string{"status"},
	)

	managedBuffersDesc = prometheus.NewDesc(
		"cluster_autoscaler_compute_class_capacity_buffers",
		"Number of capacity buffers currently managed across the cluster, partitioned by status buckets (accepted, provisioning, failed, unprocessed).",
		[]string{"provisioning_strategy", "status"},
		nil,
	)

	collectorOnce sync.Once
)

func init() {
	legacyregistry.MustRegister(reconcileDuration)
}

type bufferMetricKey struct {
	strategy string
	status   string
}

// capacityBuffersCollector implements prometheus.Collector to emit active buffer counts on scrape.
type capacityBuffersCollector struct {
	client client.Reader
}

func registerCapacityBuffersCollector(c client.Reader) {
	collectorOnce.Do(func() {
		legacyregistry.RawMustRegister(&capacityBuffersCollector{client: c})
	})
}

// Describe sends the super-set of all possible descriptors of metrics collected by this Collector.
func (c *capacityBuffersCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- managedBuffersDesc
}

func clampProvisioningStrategyForMetric(strategy string) string {
	if isSupportedProvisioningStrategy(strategy) {
		return strategy
	}
	return "custom"
}

// Collect is called by Prometheus at scrape time to query current capacity buffers from the cache and emit metrics.
func (c *capacityBuffersCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var cbList cbv1beta1.CapacityBufferList
	if err := c.client.List(ctx, &cbList, client.InNamespace(NamespaceGkeManagedCCC), client.MatchingLabels{labelManagedBy: managedByController}); err != nil {
		klog.Errorf("%s Failed to list CapacityBuffers for metrics collection: %v", logPrefix, err)
		return
	}

	counts := make(map[bufferMetricKey]int, len(knownBufferMetricStrategies)*len(knownBufferMetricStatuses))

	for i := range cbList.Items {
		cb := &cbList.Items[i]
		if cb.GetDeletionTimestamp() != nil {
			continue
		}
		strategy := clampProvisioningStrategyForMetric(getCapacityBufferStrategy(cb))
		status := getBufferStatus(cb)
		counts[bufferMetricKey{strategy: strategy, status: status}]++
	}

	// Emit every strategy/status pair so that empty buckets are exported as 0 instead of being omitted.
	for _, s := range knownBufferMetricStrategies {
		for _, stat := range knownBufferMetricStatuses {
			ch <- prometheus.MustNewConstMetric(
				managedBuffersDesc,
				prometheus.GaugeValue,
				float64(counts[bufferMetricKey{strategy: s, status: stat}]),
				s,
				stat,
			)
		}
	}
}

// getBufferStatus derives the status label for a CapacityBuffer from its conditions: "unprocessed"
// until ReadyForProvisioning is set, "accepted" once it is True but CA hasn't set Provisioning yet,
// "provisioning" while Provisioning is True, and "failed" if either condition is set but not True.
// Buffers with an unsupported provisioning strategy are always "unprocessed".
func getBufferStatus(cb *cbv1beta1.CapacityBuffer) string {
	if cb == nil {
		return bufferStatusUnprocessed
	}
	strategy := getCapacityBufferStrategy(cb)
	if !isSupportedProvisioningStrategy(strategy) {
		return bufferStatusUnprocessed
	}
	cond := meta.FindStatusCondition(cb.Status.Conditions, capacitybuffer.ReadyForProvisioningCondition)
	if cond == nil {
		return bufferStatusUnprocessed
	}
	if cond.Status != metav1.ConditionTrue {
		return bufferStatusFailed
	}
	provCond := meta.FindStatusCondition(cb.Status.Conditions, capacitybuffer.ProvisioningCondition)
	if provCond == nil {
		return bufferStatusAccepted
	}
	if provCond.Status != metav1.ConditionTrue {
		return bufferStatusFailed
	}
	return bufferStatusProvisioning
}

// recordReconcile records the result status ("success" or "error") and duration of a reconciliation pass.
func recordReconcile(status string, duration time.Duration) {
	reconcileDuration.WithLabelValues(status).Observe(duration.Seconds())
}
