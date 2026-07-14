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

package reasons

import (
	"slices"
	"strings"
)

// SortGroupedEventsMessages makes sure every event with a combined message has the sub-messages sorted for determinism in comparison
func SortGroupedEventsMessages(events []string) []string {
	result := []string{}
	for _, event := range events {
		if strings.Contains(event, groupedMsgsPrefix) {
			msgsStartIndex := strings.Index(event, groupedMsgsPrefix) + len(groupedMsgsPrefix)
			gotMessages := strings.Split(event[msgsStartIndex:], groupByReasonDelimiter)
			slices.Sort(gotMessages)
			event = event[:msgsStartIndex] + strings.Join(gotMessages, groupByReasonDelimiter)
		}
		result = append(result, event)
	}
	return result
}

func MultipleErrorsMessage(messages []string) string {
	return groupedMsgsPrefix + strings.Join(messages, groupByReasonDelimiter)
}
