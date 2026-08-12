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
	cametrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
)

func TestObserveDryRunResolution(t *testing.T) {
	cametrics.RegisterAll()
	cametrics.ResetAllForTest()

	getVal := func(status, reason string) float64 {
		val, _ := cametrics.GetDaemonSetMutationResolutionsCountForTest(status, reason)
		return val
	}

	initMutated := getVal("success", "mutated")
	initUnmutated := getVal("success", "unmutated")
	initTimeout := getVal("error", "timeout")
	initForbidden := getVal("error", "forbidden")
	initInvalid := getVal("error", "invalid")
	initNotFound := getVal("error", "not_found")
	initRateLimited := getVal("error", "rate_limited")
	initInternal := getVal("error", "internal")
	initDurationVal, _ := cametrics.GetDaemonSetMutationResolutionDurationSumForTest()

	// Success mutated case
	observeDryRunResolution(true, nil, 5*time.Second)
	assert.Equal(t, initMutated+1, getVal("success", "mutated"))
	durationVal, _ := cametrics.GetDaemonSetMutationResolutionDurationSumForTest()
	assert.Equal(t, initDurationVal+5.0, durationVal)

	// Success unmutated case
	observeDryRunResolution(false, nil, 3*time.Second)
	assert.Equal(t, initUnmutated+1, getVal("success", "unmutated"))
	durationVal, _ = cametrics.GetDaemonSetMutationResolutionDurationSumForTest()
	assert.Equal(t, initDurationVal+8.0, durationVal)

	// Context timeout case
	observeDryRunResolution(false, context.DeadlineExceeded, 1*time.Second)
	assert.Equal(t, initTimeout+1, getVal("error", "timeout"))

	// API Forbidden case
	forbiddenErr := apierrors.NewForbidden(schema.GroupResource{Group: "", Resource: "pods"}, "fake-pod", errors.New("policy block"))
	observeDryRunResolution(false, forbiddenErr, 1*time.Second)
	assert.Equal(t, initForbidden+1, getVal("error", "forbidden"))

	// API Invalid case
	invalidErr := apierrors.NewInvalid(schema.GroupKind{Group: "", Kind: "Pod"}, "fake-pod", field.ErrorList{})
	observeDryRunResolution(false, invalidErr, 1*time.Second)
	assert.Equal(t, initInvalid+1, getVal("error", "invalid"))

	// API NotFound case
	notFoundErr := apierrors.NewNotFound(schema.GroupResource{Group: "", Resource: "namespaces"}, "fake-ns")
	observeDryRunResolution(false, notFoundErr, 1*time.Second)
	assert.Equal(t, initNotFound+1, getVal("error", "not_found"))

	// API TooManyRequests case
	rateLimitErr := apierrors.NewTooManyRequests("throttled", 10)
	observeDryRunResolution(false, rateLimitErr, 1*time.Second)
	assert.Equal(t, initRateLimited+1, getVal("error", "rate_limited"))

	// API Internal case
	internalErr := apierrors.NewInternalError(errors.New("db error"))
	observeDryRunResolution(false, internalErr, 1*time.Second)
	assert.Equal(t, initInternal+1, getVal("error", "internal"))

	// API ServiceUnavailable case
	unavailableErr := apierrors.NewServiceUnavailable("down")
	observeDryRunResolution(false, unavailableErr, 1*time.Second)
	assert.Equal(t, initInternal+2, getVal("error", "internal")) // Increments initInternal by 2 total

	// Context Canceled case (should be ignored completely)
	currOther := getVal("error", "other")
	currDurationVal, _ := cametrics.GetDaemonSetMutationResolutionDurationSumForTest()
	observeDryRunResolution(false, context.Canceled, 1*time.Second)
	assert.Equal(t, currOther, getVal("error", "other"))
	durationVal, _ = cametrics.GetDaemonSetMutationResolutionDurationSumForTest()
	assert.Equal(t, currDurationVal, durationVal)

	// Generic error case
	observeDryRunResolution(false, errors.New("dryrun failed"), 10*time.Second)
	assert.Equal(t, currOther+1, getVal("error", "other"))
}
