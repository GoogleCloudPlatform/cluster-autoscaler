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

package utils

import (
	"fmt"
	"strings"
)

// MultiError represent a list of errors.
type MultiError struct {
	quota int
	total int
	errs  []error
}

// NewMultiErr creates new multi error with specified quota.
func NewMultiErr(q int) *MultiError {
	return &MultiError{
		quota: q,
	}
}

// Append return true if the quota hasn't been reached and the new error was appended.
func (err *MultiError) Append(newErr error) {
	if len(err.errs) < err.quota {
		err.errs = append(err.errs, newErr)
	}
	err.total += 1
}

// ErrorOrNil return nil if there are no errors.
func (err *MultiError) ErrorOrNil() error {
	if err.total == 0 {
		return nil
	}
	return err
}

// Error return the error message and how many error in the list.
func (err *MultiError) Error() string {
	if err.total == 0 {
		return ""
	}
	errStr := make([]string, len(err.errs))
	for i, er := range err.errs {
		errStr[i] = er.Error()
	}
	return fmt.Sprintf("%d error(s) in total: %s", err.total, strings.Join(errStr, "\n"))
}
