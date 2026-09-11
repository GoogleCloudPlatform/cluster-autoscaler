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

func TestNewCategorizedError(t *testing.T) {
	baseErr := errors.New("operation failed")

	tests := []struct {
		name     string
		category string
		err      error
		expected string
	}{
		{
			name:     "nil error returns nil",
			category: CategoryStockout,
			err:      nil,
			expected: "",
		},
		{
			name:     "actionable returns unwrapped error",
			category: CategoryActionable,
			err:      baseErr,
			expected: "operation failed",
		},
		{
			name:     "stockout returns prefixed error",
			category: CategoryStockout,
			err:      baseErr,
			expected: "[stockout] operation failed",
		},
		{
			name:     "quota exceeded returns prefixed error",
			category: CategoryQuotaExceeded,
			err:      baseErr,
			expected: "[quota_exceeded] operation failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := NewCategorizedError(tc.category, tc.err)
			if tc.err == nil {
				assert.Nil(t, got)
				return
			}
			assert.Equal(t, tc.expected, got.Error())
		})
	}
}

func TestCategorizeError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "nil error returns CategoryNone",
			err:      nil,
			expected: CategoryNone,
		},

		{
			name:     "plain error returns CategoryActionable",
			err:      errors.New("something went wrong"),
			expected: CategoryActionable,
		},
		{
			name:     "prefixed stockout error returns CategoryStockout",
			err:      fmt.Errorf("[%s] timeout waiting for instances", CategoryStockout),
			expected: CategoryStockout,
		},
		{
			name:     "prefixed quota exceeded error returns CategoryQuotaExceeded",
			err:      fmt.Errorf("[%s] quota exceeded", CategoryQuotaExceeded),
			expected: CategoryQuotaExceeded,
		},
		{
			name:     "prefixed invalid config error returns CategoryInvalidConfig",
			err:      fmt.Errorf("[%s] invalid config", CategoryInvalidConfig),
			expected: CategoryInvalidConfig,
		},
		{
			name:     "prefixed actionable error returns CategoryActionable",
			err:      fmt.Errorf("[%s] internal error", CategoryActionable),
			expected: CategoryActionable,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, CategorizeError(tc.err))
		})
	}
}

func TestErrorCategoryFromGCEError(t *testing.T) {
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
			expected: CategoryStockout,
		},
		{
			name:           "unrecognized error with suspended status produces nil errInfo and defaults to actionable",
			code:           "SOME_UNKNOWN_GCE_ERROR",
			instanceStatus: "SUSPENDED",
			expected:       CategoryActionable,
		},
		{
			name:     "stockout - insufficient capacity",
			code:     "INSUFFICIENT_CAPACITY",
			expected: CategoryStockout,
		},
		{
			name:     "quota exceeded",
			code:     "QUOTA_EXCEEDED",
			expected: CategoryQuotaExceeded,
		},
		{
			name:     "permissions error",
			code:     "PERMISSIONS_ERROR",
			expected: CategoryInvalidConfig,
		},
		{
			name:     "IP space exhausted",
			code:     gce.ErrorIPSpaceExhausted,
			expected: CategoryInvalidConfig,
		},
		{
			name:     "unsupported TPU configuration",
			code:     "CONDITION_NOT_MET",
			msg:      "Unsupported TPU configuration for node",
			expected: CategoryInvalidConfig,
		},
		{
			name:     "reservation not found",
			msg:      "Specified reservation res-1 does not exist",
			expected: CategoryInvalidConfig,
		},
		{
			name:     "unknown error code defaults to actionable",
			code:     "SOME_UNKNOWN_GCE_ERROR",
			expected: CategoryActionable,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, ErrorCategoryFromGCEError(tc.code, tc.msg, tc.instanceStatus))
		})
	}
}
