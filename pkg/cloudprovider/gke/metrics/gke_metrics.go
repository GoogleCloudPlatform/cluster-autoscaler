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

package metrics

import (
	"time"

	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/klog/v2"
)

type NodePoolOperationType string
type NodePoolOperationResult string

const (
	caNamespace = "cluster_autoscaler"

	CreateNodePool  NodePoolOperationType = "create"
	DeleteNodePool  NodePoolOperationType = "delete"
	CleanupNodePool NodePoolOperationType = "cleanup"

	Success NodePoolOperationResult = "success"
	Failure NodePoolOperationResult = "failure"
)

var (
	/**** Metrics related to GKE API usage ****/
	requestCounter = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "gke_request_count",
			Help:      "Counter of GKE API requests for each verb and API resource.",
		}, []string{"resource", "verb"},
	)

	requestLatencies = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Namespace: caNamespace,
			Name:      "request_latencies",
			Help:      "Latencies of requests made by Cluster Autoscaler, measured in seconds.",
			Buckets: []float64{
				0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 3, 4, 5, 6, 7, 8, 16, 32, 64, 128,
				256, 512, 1024, 2048, 4096,
			},
		}, []string{"service", "resource", "verb", "status"},
	)

	invalidAtomicScaleDowns = k8smetrics.NewCounter(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "invalid_atomic_scale_downs",
			Help:      "Number of attempted partial scale-downs that should have been performed for all nodes from the pool.",
		},
	)

	unexpectedNodePools = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: caNamespace,
			Name:      "unexpected_node_pools",
			Help:      "Number of node pools detected by Cluster Autoscaler with unexpected configuration.",
		}, []string{"reason"},
	)

	nodePoolOperationDuration = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Namespace: caNamespace,
			Name:      "node_pool_operation_duration",
			Help:      "The time node pool operation takes to complete, measured in seconds",
			Buckets:   k8smetrics.ExponentialBuckets(0.01, 1.5, 30), // 0.01, 0.015, 0.0225, ..., 852.2269299239293, 1278.3403948858938
		},
		[]string{"type", "result"},
	)

	nodePoolOngoingOperations = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "node_pool_ongoing_operations",
			Help:      "The number of node pool operations by operation type and status",
		},
		[]string{"status", "type"},
	)

	nodePoolOngoingOperationDuration = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Namespace: caNamespace,
			Name:      "node_pool_operation_status_duration",
			Help:      "The time spend by node pool operation in particular status, measured in seconds",
			Buckets:   k8smetrics.ExponentialBuckets(0.01, 1.5, 30), // 0.01, 0.015, 0.0225, ..., 852.2269299239293, 1278.3403948858938
		},
		[]string{"status", "type"},
	)
)

// RegisterMetrics registerMetrics registers all GKE metrics.
func RegisterMetrics() {
	legacyregistry.MustRegister(requestCounter)
	legacyregistry.MustRegister(requestLatencies)
	legacyregistry.MustRegister(invalidAtomicScaleDowns)
	legacyregistry.MustRegister(unexpectedNodePools)
	legacyregistry.MustRegister(nodePoolOperationDuration)
	legacyregistry.MustRegister(nodePoolOngoingOperations)
	legacyregistry.MustRegister(nodePoolOngoingOperationDuration)
}

// emitRequest registerRequest emits request to GKE API.
func emitRequest(resource string, verb string) {
	requestCounter.WithLabelValues(resource, verb).Add(1.0)
}

// emitRequestLatency emits latency of the call to the GKE and GCE APIs.
func emitRequestLatency(service, resource, verb, status string, duration time.Duration) {
	requestLatencies.WithLabelValues(service, resource, verb, status).Observe(duration.Seconds())
}

// Emits both request latency and request counter metrics for a given event.
// Expects the `response` argument to be either nil, a struct or a pointer to a struct
// that contains a field named `ServerResponse` of `googleapi.ServerResponse` type.
// The argument `verb` should be a description of the domain-level action performed on the resource, unless for
// the sake of compatibility it needs to be the http method of the underyling call.
// TODO(b/432195815): Deprecate the RegisterRequest call when dashboards are migrated to use RequestLatency instead.
func emitRequestLatencyMetric(service, resource, verb string, response any, err error, start time.Time, withReason bool) {
	duration := time.Since(start)
	serverResponse := serverResponseFromAny(response)
	if response != nil && serverResponse == nil {
		klog.Warningf("failed to retrieve ServerResponse object from response for a call with label values { service: %s, resource: %s, verb: %s }", service, resource, verb)
	}
	status := determineResponseStatusForLatencyMetric(serverResponse, err, withReason)
	emitRequestLatency(service, resource, verb, status, duration)
	emitRequest(resource, verb)
}

// Emits both request latency and request counter metrics for a given event.
// Expects the `response` argument to be a struct or a pointer to a struct
// that contains a field named `ServerResponse` of `googleapi.ServerResponse` type.
func EmitServiceControlLatency(resource, verb string, response any, err error, start time.Time) {
	emitRequestLatencyMetric("service_control", resource, verb, response, err, start, false)
}

// Emits both request latency and request counter metrics for a given event.
// Expects the `response` argument to be either nil, a struct or a pointer to a struct
// that contains a field named `ServerResponse` of `googleapi.ServerResponse` type.
func EmitGceLatency(resource, verb string, response any, err error, start time.Time) {
	emitRequestLatencyMetric("gce", resource, verb, response, err, start, false)
}

// Emits both request latency and request counter metrics for a given event.
// Expects the `response` argument to be either nil, a struct or a pointer to a struct
// that contains a field named `ServerResponse` of `googleapi.ServerResponse` type.
func EmitGceLatencyWithReason(resource, verb string, response any, err error, start time.Time) {
	emitRequestLatencyMetric("gce", resource, verb, response, err, start, true)
}

// Emits both request latency and request counter metrics for a given event.
// Expects the `response` argument to be either nil, a struct or a pointer to a struct
// that contains a field named `ServerResponse` of `googleapi.ServerResponse` type.
func EmitGkeLatency(resource, verb string, response any, err error, start time.Time) {
	emitRequestLatencyMetric("gke", resource, verb, response, err, start, false)
}

func EmitInvalidAtomicScaleDown() {
	invalidAtomicScaleDowns.Inc()
}

func EmitUnexpectedNodePool(reason string) {
	unexpectedNodePools.WithLabelValues(reason).Inc()
}

// ObserveNodePoolOperationDuration registers a measurement of node pool operation duration.
func ObserveNodePoolOperationDuration(operationType NodePoolOperationType, result NodePoolOperationResult, duration time.Duration) {
	nodePoolOperationDuration.WithLabelValues(string(operationType), string(result)).Observe(duration.Seconds())
}

// UpdateNodePoolOngoingOperations updates the count of ongoing node pool operations.
func UpdateNodePoolOngoingOperations(operationType NodePoolOperationType, status string, count int) {
	nodePoolOngoingOperations.WithLabelValues(status, string(operationType)).Set(float64(count))
}

// ObserveNodePoolOperationStatusDuration registers a measurement of ongoing node pool operation spent in particular status.
func ObserveNodePoolOperationStatusDuration(operationType NodePoolOperationType, status string, duration time.Duration) {
	nodePoolOngoingOperationDuration.WithLabelValues(status, string(operationType)).Observe(duration.Seconds())
}
