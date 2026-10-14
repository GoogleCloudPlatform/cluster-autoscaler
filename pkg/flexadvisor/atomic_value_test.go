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

package flexadvisor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAtomicValue(t *testing.T) {
	testCases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "uninitialized Load and Swap - return zero value",
			run: func(t *testing.T) {
				var v atomicValue[PollingMode]
				assert.Equal(t, PollingModeNormal, v.Load())
				assert.Equal(t, PollingModeNormal, v.Swap(PollingModeSlow))
				assert.Equal(t, PollingModeSlow, v.Load())
			},
		},
		{
			name: "Store followed by Swap - returns previous value and updates stored value",
			run: func(t *testing.T) {
				var v atomicValue[PollingMode]
				v.Store(PollingModeSlow)
				assert.Equal(t, PollingModeSlow, v.Load())
				assert.Equal(t, PollingModeSlow, v.Swap(PollingModeNormal))
				assert.Equal(t, PollingModeNormal, v.Load())
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.run)
	}
}
