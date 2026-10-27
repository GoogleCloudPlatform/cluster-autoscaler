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

package rules

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMinEstimatedUptimeRule_Matches(t *testing.T) {
	rule := &minEstimatedUptimeRule{}
	assert.True(t, rule.Matches(&mockNodeGroup{}))
}

func TestMinEstimatedUptimeRule_MinEstimatedUptimeMinutes(t *testing.T) {
	val := 60
	testCases := []struct {
		name     string
		rule     *minEstimatedUptimeRule
		expected *int
	}{
		{
			name:     "set value",
			rule:     &minEstimatedUptimeRule{minEstimatedUptimeMinutes: &val},
			expected: &val,
		},
		{
			name:     "nil rule",
			rule:     nil,
			expected: nil,
		},
		{
			name:     "nil value",
			rule:     &minEstimatedUptimeRule{},
			expected: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.rule.MinEstimatedUptimeMinutes())
		})
	}
}

func TestWithMinEstimatedUptimeRule(t *testing.T) {
	val := 60
	r := &rule{}
	opt := WithMinEstimatedUptimeRule(&val)
	opt(r)

	assert.NotNil(t, r.minEstimatedUptimeRule.MinEstimatedUptimeMinutes())
	assert.Equal(t, 60, *r.minEstimatedUptimeRule.MinEstimatedUptimeMinutes())
}
