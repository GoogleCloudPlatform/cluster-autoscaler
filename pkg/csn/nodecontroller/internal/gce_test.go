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

package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Source:
// https://docs.cloud.google.com/compute/docs/instances/instance-lifecycle
// Date of access: 2026.03.23
func TestInstanceStatus(t *testing.T) {
	testCases := []struct {
		status            string
		expectedSuspended bool
		expectedStopped   bool
	}{
		{status: "PENDING"},
		{status: "PROVISIONING"},
		{status: "STAGING"},
		{status: "REPAIRING"},
		{status: "RUNNING"},
		{
			status:            "SUSPENDING",
			expectedSuspended: true,
		},
		{
			status:            "SUSPENDED",
			expectedSuspended: true,
		},
		{
			status:          "TERMINATED",
			expectedStopped: true,
		},
		{
			status:          "STOPPING",
			expectedStopped: true,
		},
		{
			status:          "PENDING_STOP",
			expectedStopped: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.status, func(t *testing.T) {
			assert.Equal(t, tc.expectedStopped, IsStopped(tc.status))
			assert.Equal(t, tc.expectedSuspended, IsSuspended(tc.status))
		})
	}
}
