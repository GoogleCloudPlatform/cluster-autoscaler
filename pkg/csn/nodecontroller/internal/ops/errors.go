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
	"fmt"
	"strings"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
)

// Error categories for csn_processed_operations metric.
const (
	// CategoryNone represents no error (e.g. for successful operations).
	CategoryNone = ""
	// CategoryActionable represents GKE-actionable internal errors.
	CategoryActionable = "actionable"
	// CategoryStockout represents GCE compute stockouts / capacity exhaustion.
	CategoryStockout = "stockout"
	// CategoryQuotaExceeded represents customer GCE quota exhaustion.
	CategoryQuotaExceeded = "quota_exceeded"
	// CategoryInvalidConfig represents customer configuration issues (permissions, invalid reservation, unsupported TPU, IP space).
	CategoryInvalidConfig = "invalid_config"
)

// NewCategorizedError wraps an error with a category tag.
// If category is empty or CategoryActionable, it returns the original error unwrapped.
func NewCategorizedError(category string, err error) error {
	if err == nil {
		return nil
	}
	if category == "" || category == CategoryActionable {
		return err
	}
	return fmt.Errorf("[%s] %w", category, err)
}

// CategorizeError returns the error category from an error string.
// If err is nil, it returns CategoryNone ("").
// Otherwise, it checks for the category prefix "[<category>]" or defaults to CategoryActionable.
func CategorizeError(err error) string {
	if err == nil {
		return CategoryNone
	}
	msg := err.Error()
	switch {
	case strings.HasPrefix(msg, "["+CategoryStockout+"]"):
		return CategoryStockout
	case strings.HasPrefix(msg, "["+CategoryQuotaExceeded+"]"):
		return CategoryQuotaExceeded
	case strings.HasPrefix(msg, "["+CategoryInvalidConfig+"]"):
		return CategoryInvalidConfig
	default:
		return CategoryActionable
	}
}

// ErrorCategoryFromGCEError maps GCE error details to a category.
func ErrorCategoryFromGCEError(errorCode, errorMessage, instanceStatus string) string {
	errInfo := gce.GetErrorInfo(errorCode, errorMessage, instanceStatus, nil)
	if errInfo == nil {
		return CategoryActionable
	}
	switch errInfo.ErrorCode {
	case gce.ErrorCodeResourcePoolExhausted, gce.ErrorCodeInsufficientCapacity:
		return CategoryStockout
	case gce.ErrorCodeQuotaExceeded:
		return CategoryQuotaExceeded
	case gce.ErrorCodePermissions,
		gce.ErrorCodeVmExternalIpAccessPolicyConstraint,
		gce.ErrorInvalidReservation,
		gce.ErrorReservationNotFound,
		gce.ErrorReservationNotReady,
		gce.ErrorReservationCapacityExceeded,
		gce.ErrorReservationIncompatible,
		gce.ErrorAutomaticReservationsNotAvailable,
		gce.ErrorAutomaticReservationsNoCapacity,
		gce.ErrorUnsupportedTpuConfiguration,
		gce.ErrorIPSpaceExhausted:
		return CategoryInvalidConfig
	default:
		return CategoryActionable
	}
}
