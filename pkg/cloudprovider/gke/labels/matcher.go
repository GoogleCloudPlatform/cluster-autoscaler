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
	"bytes"
	"fmt"
	"regexp"
)

// Matcher matches labels based on given patterns.
type Matcher regexp.Regexp

// NewMatcher returns a new Matcher instance for a given list of regex patterns.
func NewMatcher(patterns []string) (*Matcher, error) {
	if len(patterns) == 0 {
		return nil, nil
	}
	var buffer bytes.Buffer
	for i, pattern := range patterns {
		if i > 0 {
			buffer.WriteString("|")
		}
		buffer.WriteString("(?:")
		buffer.WriteString(pattern)
		buffer.WriteString(")")
	}
	combinedPattern := buffer.String()
	compiled, err := regexp.Compile(combinedPattern)
	if err != nil {
		return nil, fmt.Errorf("failed to compile label pattern %q: %v", combinedPattern, err)
	}
	return (*Matcher)(compiled), nil
}

// Match returns true if any of the Matcher's patterns matches a given string.
func (m *Matcher) Match(s string) bool {
	if m == nil {
		return false
	}
	return (*regexp.Regexp)(m).MatchString(s)
}
