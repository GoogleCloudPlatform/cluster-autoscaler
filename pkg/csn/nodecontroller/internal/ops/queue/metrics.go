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

package queue

import (
	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const namespace = "cluster_autoscaler"

var (
	opQueueLength = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: namespace,
			Name:      "csn_operation_queue_length",
			Help:      "Current number of operations in the queue.",
		},
		[]string{"op_type"},
	)
	opQueueAddsTotal = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace: namespace,
			Name:      "csn_operation_queue_adds_total",
			Help:      "Total number of operations attempted to be added to the queue.",
		},
		[]string{"status", "batch", "op_type"},
	)
	opQueueNodesTotal = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: namespace,
			Name:      "csn_operation_queue_nodes_total",
			Help:      "Current number of nodes in the queue.",
		},
		[]string{"op_type"},
	)
)

const (
	// `status` label for metrics
	// They are used to describe the completion of enqueueing an operation.
	queueAddFailure = "failure"
	queueAddSuccess = "success"
)

const (
	// 'batch' label
	// Enqueueing operation should have `existing_batch` if the operation
	// was merged with one already existing in the queue. `new_batch` should be
	// present otherwise.
	existingBatch = "existing_batch"
	newBatch      = "new_batch"
)

func init() {
	legacyregistry.MustRegister(opQueueLength)
	legacyregistry.MustRegister(opQueueAddsTotal)
	legacyregistry.MustRegister(opQueueNodesTotal)
}
