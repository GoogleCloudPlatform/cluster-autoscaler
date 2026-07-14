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

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
)

func TestTpuRuleMatchesNodeGroup(t *testing.T) {
	testCases := []struct {
		name      string
		nodegroup cloudprovider.NodeGroup
		rule      TpuRule
		expected  bool
	}{
		{
			name:      "rule with tpu, node group with same tpu config, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{TpuType: "tpu-v5-lite-podslice", TpuTopology: "2x4", MachineType: "ct5lp-hightpu-4t"}).Build(),
			rule:      NewRule(WithTpuRule("tpu-v5-lite-podslice", 4, "2x4")),
			expected:  true,
		},
		{
			name:      "rule with tpu, node group with different tpu topology, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{TpuType: "tpu-v5p-slice", TpuTopology: "4x4x1", MachineType: "ct5p-hightpu-4t"}).Build(),
			rule:      NewRule(WithTpuRule("tpu-v5p-slice", 4, "2x2x1")),
			expected:  false,
		},
		{
			name:      "rule with tpu, node group with different tpu type, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{TpuType: "tpu-v4-lite-device", TpuTopology: "2x2x1", MachineType: "ct4p-hightpu-4t"}).Build(),
			rule:      NewRule(WithTpuRule("tpu-v5p-slice", 4, "2x2x1")),
			expected:  false,
		},
		{
			name:      "rule with tpu, node group with different tpu count, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{TpuType: "tpu-v5-lite-podslice", TpuTopology: "2x4", MachineType: "ct5lp-hightpu-8t"}).Build(),
			rule:      NewRule(WithTpuRule("tpu-v5-lite-podslice", 4, "2x4")),
			expected:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.rule.Matches(tc.nodegroup)
			if actual != tc.expected {
				t.Errorf("Test: \"%v\" failed, expected matching: %v got: %v", tc.name, tc.expected, actual)
			}
		})
	}
}
