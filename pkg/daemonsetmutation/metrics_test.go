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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/component-base/metrics/testutil"
)

func TestObserveDryRunResolution(t *testing.T) {
	getVal := func(status string) float64 {
		val, _ := testutil.GetCounterMetricValue(DryrunResolutionsTotal.WithLabelValues(status))
		return val
	}

	initSuccess := getVal("success")
	initTimeout := getVal("error_timeout")
	initForbidden := getVal("error_forbidden")
	initInvalid := getVal("error_invalid")
	initNotFound := getVal("error_not_found")
	initRateLimited := getVal("error_rate_limited")
	initInternal := getVal("error_internal")
	initDurationVal, _ := testutil.GetHistogramMetricValue(ResolutionDuration.ObserverMetric)

	// Success case
	observeDryRunResolution(nil, 5*time.Second)
	assert.Equal(t, initSuccess+1, getVal("success"))
	durationVal, _ := testutil.GetHistogramMetricValue(ResolutionDuration.ObserverMetric)
	assert.Equal(t, initDurationVal+5.0, durationVal)

	// Context timeout case
	observeDryRunResolution(context.DeadlineExceeded, 1*time.Second)
	assert.Equal(t, initTimeout+1, getVal("error_timeout"))

	// API Forbidden case
	forbiddenErr := apierrors.NewForbidden(schema.GroupResource{Group: "", Resource: "pods"}, "fake-pod", errors.New("policy block"))
	observeDryRunResolution(forbiddenErr, 1*time.Second)
	assert.Equal(t, initForbidden+1, getVal("error_forbidden"))

	// API Invalid case
	invalidErr := apierrors.NewInvalid(schema.GroupKind{Group: "", Kind: "Pod"}, "fake-pod", field.ErrorList{})
	observeDryRunResolution(invalidErr, 1*time.Second)
	assert.Equal(t, initInvalid+1, getVal("error_invalid"))

	// API NotFound case
	notFoundErr := apierrors.NewNotFound(schema.GroupResource{Group: "", Resource: "namespaces"}, "fake-ns")
	observeDryRunResolution(notFoundErr, 1*time.Second)
	assert.Equal(t, initNotFound+1, getVal("error_not_found"))

	// API TooManyRequests case
	rateLimitErr := apierrors.NewTooManyRequests("throttled", 10)
	observeDryRunResolution(rateLimitErr, 1*time.Second)
	assert.Equal(t, initRateLimited+1, getVal("error_rate_limited"))

	// API Internal case
	internalErr := apierrors.NewInternalError(errors.New("db error"))
	observeDryRunResolution(internalErr, 1*time.Second)
	assert.Equal(t, initInternal+1, getVal("error_internal"))

	// API ServiceUnavailable case
	unavailableErr := apierrors.NewServiceUnavailable("down")
	observeDryRunResolution(unavailableErr, 1*time.Second)
	assert.Equal(t, initInternal+2, getVal("error_internal")) // Increments initInternal by 2 total

	// Context Canceled case (should be ignored completely)
	currOther := getVal("error_other")
	currDurationVal, _ := testutil.GetHistogramMetricValue(ResolutionDuration.ObserverMetric)
	observeDryRunResolution(context.Canceled, 1*time.Second)
	assert.Equal(t, currOther, getVal("error_other"))
	durationVal, _ = testutil.GetHistogramMetricValue(ResolutionDuration.ObserverMetric)
	assert.Equal(t, currDurationVal, durationVal)

	// Generic error case
	observeDryRunResolution(errors.New("dryrun failed"), 10*time.Second)
	assert.Equal(t, currOther+1, getVal("error_other"))
}
