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

package labels

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMatcher(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
		input    string
		want     bool
	}{
		{
			name:     "match single pattern",
			patterns: []string{"^abc$"},
			input:    "abc",
			want:     true,
		},
		{
			name:     "no match single pattern",
			patterns: []string{"^abc$"},
			input:    "abcd",
			want:     false,
		},
		{
			name:     "match multiple patterns - first",
			patterns: []string{"^abc$", "^def$"},
			input:    "abc",
			want:     true,
		},
		{
			name:     "match multiple patterns - second",
			patterns: []string{"^abc$", "^def$"},
			input:    "def",
			want:     true,
		},
		{
			name:     "no match multiple patterns",
			patterns: []string{"^abc$", "^def$"},
			input:    "ghi",
			want:     false,
		},
		{
			name:     "empty patterns",
			patterns: []string{},
			input:    "abc",
			want:     false,
		},
		{
			name:     "complex patterns",
			patterns: []string{"cloud\\.google\\.com/.*", "k8s\\.io/.*"},
			input:    "cloud.google.com/gke-nodepool",
			want:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, err := NewMatcher(tc.patterns)
			assert.NoError(t, err)
			assert.Equal(t, tc.want, m.Match(tc.input))
		})
	}
}

func TestNewMatcherError(t *testing.T) {
	_, err := NewMatcher([]string{"["})
	assert.Error(t, err)
}
