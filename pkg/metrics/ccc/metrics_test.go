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

package ccc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCccMetricName(t *testing.T) {
	testCases := []struct {
		name     string
		baseName string
		expected string
	}{
		{
			name:     "simple base name",
			baseName: "node_provisioning_attempts_count",
			expected: "node_provisioning_attempts_count_per_ccc",
		},
		{
			name:     "empty base name",
			baseName: "",
			expected: "_per_ccc",
		},
		{
			name:     "base name with underscores",
			baseName: "some_other_metric",
			expected: "some_other_metric_per_ccc",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, cccMetricName(tc.baseName))
		})
	}
}
