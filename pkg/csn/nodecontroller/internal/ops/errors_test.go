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

package ops

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
)

func TestErrorWithCode_Unwrap(t *testing.T) {
	baseErr := errors.New("underlying error")
	codedErr := NewErrorWithCode(gce.ErrorCodeQuotaExceeded, baseErr)

	assert.Equal(t, baseErr, errors.Unwrap(codedErr))
	assert.True(t, errors.Is(codedErr, baseErr))
}

func TestNewErrorWithCode(t *testing.T) {
	baseErr := errors.New("operation failed")

	tests := []struct {
		name     string
		code     string
		err      error
		expected string
	}{
		{
			name:     "nil error returns nil",
			code:     gce.ErrorCodeResourcePoolExhausted,
			err:      nil,
			expected: "",
		},
		{
			name:     "GKE internal error returns prefixed error",
			code:     ErrorCodeGKEInternal,
			err:      baseErr,
			expected: "[GKE_INTERNAL_ERROR] operation failed",
		},
		{
			name:     "empty code defaults to GKE internal error",
			code:     "",
			err:      baseErr,
			expected: "[GKE_INTERNAL_ERROR] operation failed",
		},
		{
			name:     "stockout code returns prefixed error",
			code:     gce.ErrorCodeResourcePoolExhausted,
			err:      baseErr,
			expected: "[RESOURCE_POOL_EXHAUSTED] operation failed",
		},
		{
			name:     "quota exceeded code returns prefixed error",
			code:     gce.ErrorCodeQuotaExceeded,
			err:      baseErr,
			expected: "[QUOTA_EXCEEDED] operation failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := NewErrorWithCode(tc.code, tc.err)
			if tc.err == nil {
				assert.Nil(t, got)
				return
			}
			assert.Equal(t, tc.expected, got.Error())
		})
	}
}

func TestExtractErrorCode(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "nil error returns ErrorCodeNone",
			err:      nil,
			expected: ErrorCodeNone,
		},
		{
			name:     "plain error returns ErrorCodeGKEInternal",
			err:      errors.New("something went wrong"),
			expected: ErrorCodeGKEInternal,
		},
		{
			name:     "ErrorWithCode stockout returns RESOURCE_POOL_EXHAUSTED",
			err:      NewErrorWithCode(gce.ErrorCodeResourcePoolExhausted, errors.New("timeout waiting for instances")),
			expected: gce.ErrorCodeResourcePoolExhausted,
		},
		{
			name:     "wrapped ErrorWithCode returns code via errors.As",
			err:      fmt.Errorf("outer wrapper: %w", NewErrorWithCode(gce.ErrorCodeQuotaExceeded, errors.New("quota exceeded"))),
			expected: gce.ErrorCodeQuotaExceeded,
		},
		{
			name:     "GKE internal error returns ErrorCodeGKEInternal",
			err:      NewErrorWithCode(ErrorCodeGKEInternal, errors.New("something went wrong")),
			expected: ErrorCodeGKEInternal,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, ExtractErrorCode(tc.err))
		})
	}
}

func TestErrorCodeFromGCEError(t *testing.T) {
	tests := []struct {
		name           string
		code           string
		msg            string
		instanceStatus string
		expected       string
	}{
		{
			name:     "stockout - zone resource pool exhausted",
			code:     "ZONE_RESOURCE_POOL_EXHAUSTED",
			expected: gce.ErrorCodeResourcePoolExhausted,
		},
		{
			name:     "stockout - insufficient capacity",
			code:     "INSUFFICIENT_CAPACITY",
			expected: gce.ErrorCodeInsufficientCapacity,
		},
		{
			name:     "quota exceeded",
			code:     "QUOTA_EXCEEDED",
			expected: gce.ErrorCodeQuotaExceeded,
		},
		{
			name:     "permissions error",
			code:     "PERMISSIONS_ERROR",
			expected: gce.ErrorCodePermissions,
		},
		{
			name:     "IP space exhausted",
			code:     gce.ErrorIPSpaceExhausted,
			expected: gce.ErrorIPSpaceExhausted,
		},
		{
			name:     "unsupported TPU configuration",
			code:     "CONDITION_NOT_MET",
			msg:      "Unsupported TPU configuration for node",
			expected: gce.ErrorUnsupportedTpuConfiguration,
		},
		{
			name:     "reservation not found",
			msg:      "Specified reservation res-1 does not exist",
			expected: gce.ErrorReservationNotFound,
		},
		{
			name:           "unrecognized GCE error code with provisioning status returns raw errorCode",
			code:           "SOME_UNKNOWN_GCE_ERROR",
			instanceStatus: "PROVISIONING",
			expected:       "SOME_UNKNOWN_GCE_ERROR",
		},
		{
			name:           "unrecognized GCE error code with suspended status returns raw errorCode",
			code:           "SOME_UNKNOWN_GCE_ERROR",
			instanceStatus: "SUSPENDED",
			expected:       "SOME_UNKNOWN_GCE_ERROR",
		},
		{
			name:           "empty error code defaults to GKE internal error",
			code:           "",
			instanceStatus: "SUSPENDED",
			expected:       ErrorCodeGKEInternal,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, ErrorCodeFromGCEError(tc.code, tc.msg, tc.instanceStatus))
		})
	}
}
