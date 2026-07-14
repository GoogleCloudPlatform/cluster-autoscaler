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

package daemonsetmutation

import (
	"context"
	"errors"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	cametrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
)

var (
	// DryrunResolutionsTotal tracks dry-run webhook resolution outcomes.
	DryrunResolutionsTotal = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Name: "daemonset_mutation_resolutions_total",
			Help: "Counter of background dry-run pod creation calls tagged by outcome status.",
		},
		[]string{"status"},
	)

	// ResolutionDuration tracks duration of dry-run webhook resolutions.
	ResolutionDuration = k8smetrics.NewHistogram(
		&k8smetrics.HistogramOpts{
			Name:    "daemonset_mutation_resolution_duration_seconds",
			Help:    "Duration of dry-run webhook resolutions.",
			Buckets: cametrics.DurationBuckets1sTo24h,
		},
	)
)

func init() {
	legacyregistry.MustRegister(DryrunResolutionsTotal)
	legacyregistry.MustRegister(ResolutionDuration)
}

func observeDryRunResolution(err error, duration time.Duration) {
	if errors.Is(err, context.Canceled) {
		return
	}
	DryrunResolutionsTotal.WithLabelValues(classifyError(err)).Inc()
	ResolutionDuration.Observe(duration.Seconds())
}

func classifyError(err error) string {
	if err == nil {
		return "success"
	}
	if errors.Is(err, context.DeadlineExceeded) || apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) {
		return "error_timeout"
	}
	if apierrors.IsForbidden(err) {
		return "error_forbidden"
	}
	if apierrors.IsInvalid(err) {
		return "error_invalid"
	}
	if apierrors.IsNotFound(err) {
		return "error_not_found"
	}
	if apierrors.IsTooManyRequests(err) {
		return "error_rate_limited"
	}
	if apierrors.IsInternalError(err) || apierrors.IsServiceUnavailable(err) {
		return "error_internal"
	}
	return "error_other"
}
