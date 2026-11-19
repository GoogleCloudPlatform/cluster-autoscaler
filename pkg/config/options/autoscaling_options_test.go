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

package options

import "testing"

func TestHandlesUnannotated(t *testing.T) {
	testCases := []struct {
		name            string
		autoscalerNames []string
		want            bool
	}{
		{
			name: "single CA",
			want: true,
		},
		{
			name:            "standard instance",
			autoscalerNames: []string{UnannotatedAutoscalerName, StandardAutoscalerName},
			want:            true,
		},
		{
			name:            "solver instance",
			autoscalerNames: []string{SolverAutoscalerName},
			want:            false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := HandlesUnannotated(tc.autoscalerNames); got != tc.want {
				t.Errorf("HandlesUnannotated(%v) = %v, want %v", tc.autoscalerNames, got, tc.want)
			}
		})
	}
}
