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

package reconciliation

import (
	"strconv"

	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const namespace = "cluster_autoscaler"

var (
	deviatingNodes = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: namespace,
			Name:      "csn_reconciler_deviating_nodes",
			Help:      "Number of CSN nodes whose observed state deviates from expected GCE instance status.",
		},
		[]string{"state", "gce_status", "invalid_count"},
	)
	reconcileRequestsTotal = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: namespace,
			Name:      "csn_reconciler_requests_total",
			Help:      "Total number of reconciliation operations enqueued by the CSN Node Controller.",
		},
		[]string{"from_state", "to_state"},
	)

	reconcileDurationSeconds = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Namespace: namespace,
			Name:      "csn_reconciler_duration_seconds",
			Help:      "Duration of reconciliation loop passes in seconds.",
			Buckets: []float64{
				0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512,
				1024, 2048, 4096,
			},
		},
		[]string{},
	)
)

type deviatingNodeKey struct {
	state        string
	gceStatus    string
	invalidCount int
}

func updateDeviatingNodes(counts map[deviatingNodeKey]int) {
	deviatingNodes.Reset()
	for key, count := range counts {
		deviatingNodes.WithLabelValues(key.state, key.gceStatus, strconv.Itoa(key.invalidCount)).Set(float64(count))
	}
}

func init() {
	legacyregistry.MustRegister(deviatingNodes)
	legacyregistry.MustRegister(reconcileRequestsTotal)
	legacyregistry.MustRegister(reconcileDurationSeconds)
}
