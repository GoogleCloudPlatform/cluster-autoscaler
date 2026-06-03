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

package handler

import (
	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const namespace = "cluster_autoscaler"

var (
	opGceBatchSize = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Namespace: namespace,
			Name:      "csn_operation_gce_batch_size",
			Help:      "Count of nodes per GCE call.",
			Buckets:   []float64{1, 2, 5, 10, 20, 50, 100, 200, 400, 800, 1600},
		},
		[]string{"call_type", "status"},
	)
)

const (
	// `status` label
	gceSuccess = "success"
	gceFailure = "failure"
)

const (
	// `call_type` label
	resumeCall  = "resume"
	suspendCall = "suspend"
)

func init() {
	legacyregistry.MustRegister(
		opGceBatchSize,
	)
}
