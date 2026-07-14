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

package gkeclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
	gkeapibeta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
)

func TestNodeTaintsConversion(t *testing.T) {
	tcs := []struct {
		desc              string
		gkeNodeTaints     []*gkeapibeta.NodeTaint
		autoscalingTaints []apiv1.Taint
	}{
		{
			desc: "no gpu taints",
			gkeNodeTaints: []*gkeapibeta.NodeTaint{
				{
					Key:    "f",
					Value:  "1",
					Effect: "NO_SCHEDULE",
				},
				{
					Key:    "g",
					Value:  "2",
					Effect: "NO_EXECUTE",
				},
			},
			autoscalingTaints: []apiv1.Taint{
				{
					Key:    "f",
					Value:  "1",
					Effect: apiv1.TaintEffectNoSchedule,
				},
				{
					Key:    "g",
					Value:  "2",
					Effect: apiv1.TaintEffectNoExecute,
				},
			},
		},
		{
			desc: "no effect",
			gkeNodeTaints: []*gkeapibeta.NodeTaint{
				{
					Key:    "f",
					Value:  "1",
					Effect: "NO_SCHEDULE",
				},
				{
					Key:    "g",
					Value:  "2",
					Effect: "EFFECT_UNSPECIFIED",
				},
			},
			autoscalingTaints: []apiv1.Taint{
				{
					Key:    "f",
					Value:  "1",
					Effect: apiv1.TaintEffectNoSchedule,
				},
				{
					Key:    "g",
					Value:  "2",
					Effect: "ff",
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.desc, func(t *testing.T) {
			gotGkeLabels := v1beta1NodeTaints(tc.autoscalingTaints)
			assert.Equal(t, gotGkeLabels, tc.gkeNodeTaints)
		})
	}
}
