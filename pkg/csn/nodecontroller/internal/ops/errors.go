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

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
)

// Error codes for csn_processed_operations metric.
const (
	// ErrorCodeNone represents no error (e.g. for successful operations).
	ErrorCodeNone = ""
	// ErrorCodeGKEInternal represents GKE-actionable internal errors or unrecognized errors.
	ErrorCodeGKEInternal = "GKE_INTERNAL_ERROR"
	// ErrorCodeGCESourcePrefix is a prefix for errors that were originally GCE errors but are not recognized by GetErrorInfo.
	ErrorCodeGCESourcePrefix = "GCE_SOURCE_"
)

// ErrorWithCode wraps an error with a canonical error code for metrics reporting.
type ErrorWithCode struct {
	Code string
	Err  error
}

func (e *ErrorWithCode) Error() string {
	return fmt.Sprintf("[%s] %v", e.Code, e.Err)
}

func (e *ErrorWithCode) Unwrap() error {
	return e.Err
}

// NewErrorWithCode wraps an error with an error code.
func NewErrorWithCode(code string, err error) error {
	if err == nil {
		return nil
	}
	if code == "" {
		code = ErrorCodeGKEInternal
	}
	return &ErrorWithCode{
		Code: code,
		Err:  err,
	}
}

// ExtractErrorCode returns the error code from an error using errors.As.
// If err is nil, it returns ErrorCodeNone ("").
// If an ErrorWithCode is found in the error chain, it returns its Code; otherwise it defaults to ErrorCodeGKEInternal.
func ExtractErrorCode(err error) string {
	if err == nil {
		return ErrorCodeNone
	}
	var codedErr *ErrorWithCode
	if errors.As(err, &codedErr) {
		return codedErr.Code
	}
	return ErrorCodeGKEInternal
}

// ErrorCodeFromGCEError maps GCE error details to a canonical GCE ErrorCode, raw GCE error code, or ErrorCodeGKEInternal.
func ErrorCodeFromGCEError(errorCode, errorMessage, instanceStatus string) string {
	errInfo := gce.GetErrorInfo(errorCode, errorMessage, instanceStatus, nil)
	if errInfo != nil && errInfo.ErrorCode != gce.ErrorCodeOther {
		return errInfo.ErrorCode
	}
	if errorCode != "" {
		return ErrorCodeGCESourcePrefix + errorCode
	}
	return ErrorCodeGKEInternal
}
