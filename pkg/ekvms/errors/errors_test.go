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

package errors

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResizeErrorAs(t *testing.T) {
	// Useful when we only care if the error contains a resize error.
	err1 := NewGuestAgentFailedToResizeError("test-family", errors.New("A resize error"))
	assert.ErrorAs(t, err1, &ResizeErr)

	// Works even if resize error is wrapped using the %w directive.
	err2 := fmt.Errorf("Wrapped resize error: %w", err1)
	assert.ErrorAs(t, err2, &ResizeErr)

	// Using %v directive prevents correct unwrapping so the type information is lost.
	err3 := fmt.Errorf("Wrapped resize error: %v", err1)
	assert.False(t, errors.As(err3, &ResizeErr))

	// Can unwrap the error to access information stored in the custom error struct.
	err4 := fmt.Errorf("Wrapped resize error: %w", err1)
	var resizeErr *ResizeError
	assert.ErrorAs(t, err4, &resizeErr)
	assert.Equal(t, GuestAgentFailedToResizeError, resizeErr.ErrType)

	// As expected, non-resize errors aren't of type ResizeError
	err5 := errors.New("Not a resize error")
	assert.False(t, errors.As(err5, &ResizeErr))
}

func TestResizeErrorIs(t *testing.T) {
	// Useful when checking if error is or contains an exact error variable.
	sentinelError := NewGuestAgentFailedToResizeError("test-family", errors.New("A resize error"))
	err1 := fmt.Errorf("Error wrapping sentinel error: %w", sentinelError)
	assert.ErrorIs(t, err1, sentinelError)
}

func TestToResizeError(t *testing.T) {
	// Fails for errors that don't contain a resize error.
	err1 := errors.New("A resize error")
	_, ok := ToResizeError(err1)
	assert.False(t, ok)

	// Works for dircet resize errors.
	err2 := NewHttp5xxError("test-family", errors.New("HTTP 5xx error"))
	resizeErr, ok := ToResizeError(err2)
	assert.True(t, ok)
	assert.Equal(t, Http5xxError, resizeErr.ErrType)

	// Works for wrapped errors using the %w directive.
	err3 := fmt.Errorf("Wrapped resize error: %w", err2)
	resizeErr, ok = ToResizeError(err3)
	assert.True(t, ok)
	assert.Equal(t, Http5xxError, resizeErr.ErrType)

	// Fails for wrapped errors using the %v directive.
	err4 := fmt.Errorf("Wrapped resize error: %v", err2)
	_, ok = ToResizeError(err4)
	assert.False(t, ok)
}
