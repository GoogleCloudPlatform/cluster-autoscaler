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
	"strconv"
	"time"

	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	cbclient "sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/client"
	filters "sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/filters"
	cbmetrics "sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/metrics"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/klogx"
)

const (
	processingIntervalMetricFQName = "capacity_buffer_processing_interval_seconds"
	loggingQuota                   = 5
)

var (
	processingIntervalMetricDesc = prometheus.NewDesc(
		prometheus.BuildFQName("cluster_autoscaler", "", processingIntervalMetricFQName),
		"How long ago each capacity buffer was last processed by the buffer controller (maximum value in seconds).",
		[]string{"is_new_buffer"}, // TODO(b/556141185): add provisioning_strategy
		nil,
	)
)

type capacityBufferClient interface {
	ListCapacityBuffers(namespace string) ([]*v1beta1.CapacityBuffer, error)
}

// processingIntervalCollector is a prometheus.Collector that collects capacity buffer processing interval metrics.
type processingIntervalCollector struct {
	client                 capacityBufferClient
	reconciledBuffers      *cbmetrics.ReconciliationCache
	supportedBuffersFilter filters.Filter
	clock                  clock.Clock
}

// NewProcessingIntervalCollector creates a new collector instance.
func NewProcessingIntervalCollector(client capacityBufferClient, strategies []string, reconciledBuffers *cbmetrics.ReconciliationCache, clock clock.Clock) *processingIntervalCollector {
	return &processingIntervalCollector{
		client:                 client,
		reconciledBuffers:      reconciledBuffers,
		supportedBuffersFilter: filters.NewStrategyFilter(strategies),
		clock:                  clock,
	}
}

// RegisterProcessingIntervalCollector registers the processing interval collector with Prometheus.
func RegisterProcessingIntervalCollector(client *cbclient.CapacityBufferClient, strategies []string, reconciledBuffers *cbmetrics.ReconciliationCache, clock clock.Clock) {
	collector := NewProcessingIntervalCollector(client, strategies, reconciledBuffers, clock)
	legacyregistry.MustRegister(collector)
}

// Describe implements the prometheus.Collector interface.
func (c *processingIntervalCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- processingIntervalMetricDesc
}

// Collect implements the prometheus.Collector interface.
func (c *processingIntervalCollector) Collect(ch chan<- prometheus.Metric) {
	// List all capacity buffers
	buffers, err := c.client.ListCapacityBuffers("")
	if err != nil {
		klog.Errorf("Failed to list capacity buffers with error: %v", err.Error())
		return
	}

	// Delete buffers that no longer exist from cache
	c.reconciledBuffers.Prune(buffers)

	supportedBuffers, _ := c.supportedBuffersFilter.Filter(context.TODO(), buffers)
	reconciledBuffersSnapshot := c.reconciledBuffers.Snapshot()

	now := c.clock.Now()
	maxNewBufferDelay := 0.0
	maxExistingBufferDelay := 0.0

	loggingQuotaTracker := klogx.NewLoggingQuota(loggingQuota)
	for _, buffer := range supportedBuffers {
		lastReconciliationTime, isNewBuffer, skipped := calculateBufferLastReconciliationTime(buffer, reconciledBuffersSnapshot)
		if skipped {
			klogx.V(2).UpTo(loggingQuotaTracker).Infof("Skipping capacity buffer %s/%s from %s metric: "+
				"buffer is not registered in reconciled buffers cache while having conditions, expected when CA restarts.", buffer.Namespace, buffer.Name, processingIntervalMetricFQName)
			continue
		}

		delay := max(0.0, now.Sub(lastReconciliationTime).Seconds()) // if edge case exists with negative time difference, it will be 0

		if isNewBuffer {
			maxNewBufferDelay = max(maxNewBufferDelay, delay)
		} else {
			maxExistingBufferDelay = max(maxExistingBufferDelay, delay)
		}
	}
	klogx.V(2).Over(loggingQuotaTracker).Infof("Skipped %d other capacity buffers from %s metric "+
		"for not being registered in reconciled buffers cache while having conditions, expected when CA restarts.", -loggingQuotaTracker.Left(), processingIntervalMetricFQName)

	c.reportMetric(ch, maxNewBufferDelay, true)
	c.reportMetric(ch, maxExistingBufferDelay, false)
}

func (c *processingIntervalCollector) reportMetric(ch chan<- prometheus.Metric, delay float64, isNew bool) {
	ch <- prometheus.MustNewConstMetric(
		processingIntervalMetricDesc,
		prometheus.GaugeValue,
		delay,
		strconv.FormatBool(isNew),
	)
}

// Create implements the k8smetrics.Registerable interface.
func (c *processingIntervalCollector) Create(version *semver.Version) bool {
	return true
}

// ClearState implements the k8smetrics.Registerable interface.
func (c *processingIntervalCollector) ClearState() {
	// No-op for stateless collector
}

// FQName implements the k8smetrics.Registerable interface.
func (c *processingIntervalCollector) FQName() string {
	return processingIntervalMetricFQName
}

// calculateBufferLastReconciliationTime returns (last reconciliation time, isNewBuffer flag, skipped flag)
func calculateBufferLastReconciliationTime(buffer *v1beta1.CapacityBuffer, reconciledBuffersSnapshot map[types.UID]time.Time) (time.Time, bool, bool) {
	lastReconciliationTime, exists := reconciledBuffersSnapshot[buffer.UID]
	if exists {
		return lastReconciliationTime, false, false
	}

	if len(buffer.Status.Conditions) == 0 {
		// non-cached buffers with no conditions are considered new buffers
		return buffer.CreationTimestamp.Time, true, false
	}

	// non-cached buffers with conditions are skipped (CA restart)
	return time.Time{}, false, true
}
