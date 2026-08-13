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

func TestClassifyOutcome(t *testing.T) {
	testCases := []struct {
		name           string
		changed        bool
		err            error
		expectedStatus string
		expectedReason string
	}{
		{
			name:           "success with changed resources",
			changed:        true,
			err:            nil,
			expectedStatus: "success",
			expectedReason: "mutated",
		},
		{
			name:           "success with unchanged resources",
			changed:        false,
			err:            nil,
			expectedStatus: "success",
			expectedReason: "unmutated",
		},
		{
			name:           "context deadline exceeded",
			changed:        false,
			err:            context.DeadlineExceeded,
			expectedStatus: "error",
			expectedReason: "timeout",
		},
		{
			name:           "api timeout error",
			changed:        false,
			err:            apierrors.NewTimeoutError("timeout", 1),
			expectedStatus: "error",
			expectedReason: "timeout",
		},
		{
			name:           "api server timeout error",
			changed:        false,
			err:            apierrors.NewServerTimeout(schema.GroupResource{Resource: "pods"}, "create", 1),
			expectedStatus: "error",
			expectedReason: "timeout",
		},
		{
			name:           "api forbidden error",
			changed:        false,
			err:            apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "fake-pod", errors.New("policy block")),
			expectedStatus: "error",
			expectedReason: "forbidden",
		},
		{
			name:           "api invalid error",
			changed:        false,
			err:            apierrors.NewInvalid(schema.GroupKind{Kind: "Pod"}, "fake-pod", field.ErrorList{}),
			expectedStatus: "error",
			expectedReason: "invalid",
		},
		{
			name:           "api not found error",
			changed:        false,
			err:            apierrors.NewNotFound(schema.GroupResource{Resource: "namespaces"}, "fake-ns"),
			expectedStatus: "error",
			expectedReason: "not_found",
		},
		{
			name:           "api too many requests (rate limited)",
			changed:        false,
			err:            apierrors.NewTooManyRequests("throttled", 10),
			expectedStatus: "error",
			expectedReason: "rate_limited",
		},
		{
			name:           "api internal error",
			changed:        false,
			err:            apierrors.NewInternalError(errors.New("db error")),
			expectedStatus: "error",
			expectedReason: "internal",
		},
		{
			name:           "api service unavailable error",
			changed:        false,
			err:            apierrors.NewServiceUnavailable("down"),
			expectedStatus: "error",
			expectedReason: "internal",
		},
		{
			name:           "generic other error",
			changed:        false,
			err:            errors.New("dryrun failed"),
			expectedStatus: "error",
			expectedReason: "other",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotStatus, gotReason := classifyOutcome(tc.changed, tc.err)
			assert.Equal(t, tc.expectedStatus, gotStatus)
			assert.Equal(t, tc.expectedReason, gotReason)
		})
	}
}

func TestObserveDryRunResolution(t *testing.T) {
	cametrics.RegisterAll()

	testCases := []struct {
		name           string
		changed        bool
		err            error
		duration       time.Duration
		expectedStatus string
		expectedReason string
		expectRecorded bool
	}{
		{
			name:           "records success changed outcome and duration",
			changed:        true,
			err:            nil,
			duration:       5 * time.Second,
			expectedStatus: "success",
			expectedReason: "mutated",
			expectRecorded: true,
		},
		{
			name:           "records success unchanged outcome and duration",
			changed:        false,
			err:            nil,
			duration:       3 * time.Second,
			expectedStatus: "success",
			expectedReason: "unmutated",
			expectRecorded: true,
		},
		{
			name:           "records error outcome and duration",
			changed:        false,
			err:            errors.New("dryrun failed"),
			duration:       10 * time.Second,
			expectedStatus: "error",
			expectedReason: "other",
			expectRecorded: true,
		},
		{
			name:           "ignores context canceled completely",
			changed:        false,
			err:            context.Canceled,
			duration:       1 * time.Second,
			expectedStatus: "error",
			expectedReason: "other",
			expectRecorded: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cametrics.ResetAllForTest()

			observeDryRunResolution(tc.changed, tc.err, tc.duration)

			count, _ := cametrics.GetDaemonSetMutationResolutionsCountForTest(tc.expectedStatus, tc.expectedReason)
			durationSum, _ := cametrics.GetDaemonSetMutationResolutionDurationSumForTest(tc.expectedStatus, tc.expectedReason)

			if !tc.expectRecorded {
				assert.Equal(t, 0.0, count)
				assert.Equal(t, 0.0, durationSum)
			} else {
				assert.Equal(t, 1.0, count)
				assert.Equal(t, tc.duration.Seconds(), durationSum)
			}
		})
	}
}
