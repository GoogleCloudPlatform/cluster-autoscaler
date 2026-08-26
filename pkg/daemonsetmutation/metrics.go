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
	cametrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
)

func observeDryRunResolution(mutated bool, err error, duration time.Duration) {
	if errors.Is(err, context.Canceled) {
		return
	}
	status, reason := classifyOutcome(mutated, err)
	cametrics.Metrics.ObserveDaemonSetMutationResolution(status, reason, duration)
}

func classifyOutcome(mutated bool, err error) (string, string) {
	if err == nil {
		if mutated {
			return "success", "mutated"
		}
		return "success", "unmutated"
	}
	if errors.Is(err, context.DeadlineExceeded) || apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) {
		return "error", "timeout"
	}
	if apierrors.IsForbidden(err) {
		return "error", "forbidden"
	}
	if apierrors.IsInvalid(err) {
		return "error", "invalid"
	}
	if apierrors.IsNotFound(err) {
		return "error", "not_found"
	}
	if apierrors.IsTooManyRequests(err) {
		return "error", "rate_limited"
	}
	if apierrors.IsInternalError(err) || apierrors.IsServiceUnavailable(err) {
		return "error", "internal"
	}
	return "error", "other"
}
