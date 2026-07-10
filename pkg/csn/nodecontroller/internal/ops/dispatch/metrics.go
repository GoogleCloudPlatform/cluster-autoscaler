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

package dispatch

import (
	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const namespace = "cluster_autoscaler"

var (
	opResultsTotal = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: namespace,
			Name:      "csn_processed_operations",
			Help:      "Total number of nodes processed by operations dispatcher.",
		},
		[]string{"op_type", "status"},
	)
	opLatencySeconds = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Namespace: namespace,
			Name:      "csn_operation_latency_seconds",
			Help:      "Latency of processing a CSN operation in seconds.",
			Buckets: []float64{
				0.5, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512,
				1024, 2048, 4096,
			},
		},
		[]string{"op_type", "node_count_bucket"}, // number of node can grow quickly, so we use log2
	)
)

const (
	// `status` label - describes the result of processing an operation
	opSuccess      = "success"
	opRetryFailure = "retryable_failure"
	opFailure      = "failure"
)

func init() {
	legacyregistry.MustRegister(opResultsTotal)
	legacyregistry.MustRegister(opLatencySeconds)
}
