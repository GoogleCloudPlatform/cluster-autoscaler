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

package reconciler

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	apiv1 "k8s.io/api/core/v1"
)

func TestDedupeTaints(t *testing.T) {
	tpuPresent := apiv1.Taint{Key: "google.com/tpu", Value: "present", Effect: apiv1.TaintEffectNoSchedule}
	tpuOther := apiv1.Taint{Key: "google.com/tpu", Value: "other", Effect: apiv1.TaintEffectNoSchedule}
	tpuNoExecute := apiv1.Taint{Key: "google.com/tpu", Value: "present", Effect: apiv1.TaintEffectNoExecute}
	gpu := apiv1.Taint{Key: "nvidia.com/gpu", Value: "present", Effect: apiv1.TaintEffectNoSchedule}

	tests := []struct {
		name   string
		taints []apiv1.Taint
		want   []apiv1.Taint
	}{
		{name: "empty", taints: nil, want: []apiv1.Taint{}},
		{name: "no duplicates", taints: []apiv1.Taint{tpuPresent, gpu}, want: []apiv1.Taint{tpuPresent, gpu}},
		{name: "exact duplicate", taints: []apiv1.Taint{tpuPresent, gpu, tpuPresent}, want: []apiv1.Taint{tpuPresent, gpu}},
		{name: "same key and effect keeps first value", taints: []apiv1.Taint{tpuPresent, tpuOther}, want: []apiv1.Taint{tpuPresent}},
		{name: "same key different effect kept", taints: []apiv1.Taint{tpuPresent, tpuNoExecute}, want: []apiv1.Taint{tpuPresent, tpuNoExecute}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if diff := cmp.Diff(tc.want, dedupeTaints(tc.taints)); diff != "" {
				t.Errorf("dedupeTaints() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
