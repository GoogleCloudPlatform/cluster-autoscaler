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

package provider

import (
	"testing"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestEnabled(t *testing.T) {
	for _, tc := range []struct {
		name      string
		autopilot bool
		manager   experiments.Manager
		want      bool
	}{
		{
			name:      "enabled in autopilot",
			autopilot: true,
			manager:   nil,
			want:      true,
		},
		{
			name:      "disabled in standard by default",
			autopilot: false,
			manager:   experiments.NewMockManager(),
			want:      false,
		},
		{
			name:      "enabled in standard via experiment",
			autopilot: false,
			manager:   experiments.NewMockManager(experiments.RelaxedNodeGroupCreationPenalty),
			want:      true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checker := NewRelaxedNodeGroupPenaltyChecker(tc.manager, tc.autopilot)
			got := checker.Enabled()
			if got != tc.want {
				t.Errorf("Enabled() got %v, want %v", got, tc.want)
			}
		})
	}
}
