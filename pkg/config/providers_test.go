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

package config

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"

	"k8s.io/apimachinery/pkg/util/sets"
)

func TestSplitOrEmpty(t *testing.T) {
	assert.Equal(t, 0, len(splitOrEmpty("")))
	assert.Equal(t, 1, len(splitOrEmpty("ab")))
	assert.Equal(t, 2, len(splitOrEmpty("a,b")))
}

func TestSimpleStringSetProvider(t *testing.T) {
	tests := []struct {
		name             string
		values           []string
		expectedProvided sets.Set[string]
	}{
		{
			name:             "nil values",
			values:           nil,
			expectedProvided: sets.New[string](),
		},
		{
			name:             "empty slice values",
			values:           []string{},
			expectedProvided: sets.New[string](),
		},
		{
			name:             "single value",
			values:           []string{"ab"},
			expectedProvided: sets.New[string]("ab"),
		},
		{
			name:             "two values",
			values:           []string{"a", "b"},
			expectedProvided: sets.New[string]("a", "b"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider := NewSimpleStringSetProvider(tc.values)
			provided := provider.Provide()
			if diff := cmp.Diff(tc.expectedProvided, provided); diff != "" {
				t.Errorf("Unexpected result provided, diff %s", diff)
			}
		})
	}
}

func TestCommaSeparatedStringSetProvider(t *testing.T) {
	tests := []struct {
		name             string
		initialValues    string
		expectedProvided sets.Set[string]
	}{
		{
			name:             "empty string",
			initialValues:    "",
			expectedProvided: sets.New[string](),
		},
		{
			name:             "single value",
			initialValues:    "ab",
			expectedProvided: sets.New[string]("ab"),
		},
		{
			name:             "two values",
			initialValues:    "a,b",
			expectedProvided: sets.New[string]("a", "b"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider := NewCommaSeparatedStringSetProvider(tc.initialValues)
			provided := provider.Provide()
			if diff := cmp.Diff(tc.expectedProvided, provided); diff != "" {
				t.Errorf("Unexpected result provided, diff %s", diff)
			}
		})
	}
}

func TestExperimentStringSetProvider(t *testing.T) {
	tests := []struct {
		name             string
		mockedFlagValue  string
		expectedProvided sets.Set[string]
	}{
		{
			name:             "experiment provider returns empty string",
			mockedFlagValue:  "",
			expectedProvided: sets.New[string]("fallback"),
		},
		{
			name:             "experiment provider returns a single value",
			mockedFlagValue:  "value1",
			expectedProvided: sets.New[string]("value1"),
		},
		{
			name:             "experiment provider returns a comma-separated string with two values",
			mockedFlagValue:  "value1,value2",
			expectedProvided: sets.New[string]("value1", "value2"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockEvaluator := mockStringFlagEvaluator{value: tc.mockedFlagValue}
			provider := NewExperimentStringSetProvider(mockEvaluator, "flag-name", NewSimpleStringSetProvider([]string{"fallback"}))
			provided := provider.Provide()
			if diff := cmp.Diff(tc.expectedProvided, provided); diff != "" {
				t.Errorf("Unexpected result provided, diff %s", diff)
			}
		})
	}
}

// Mock implementation of StringFlagEvaluator for testing.
type mockStringFlagEvaluator struct {
	value string
}

func (m mockStringFlagEvaluator) EvaluateStringFlagOrFailsafe(_, _ string) string {
	return m.value
}
