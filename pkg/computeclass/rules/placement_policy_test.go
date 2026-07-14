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
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"testing"
)

func TestPlacementPolicyRuleMatchesNodeGroup(t *testing.T) {
	machineType := "n2-standard-8"
	testCases := []struct {
		name      string
		nodegroup cloudprovider.NodeGroup
		rule      StorageRule
		expected  bool
	}{
		{
			name:      "rule with placement policy, node group without placement policy - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{}).Build(),
			rule: NewRule(
				WithPlacementPolicyRule("policy"),
			),
			expected: false,
		},
		{
			name:      "rule with placement policy, node group with different placement policy - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{PlacementGroup: placement.Spec{Policy: "different_policy"}}).Build(),
			rule: NewRule(
				WithPlacementPolicyRule("policy"),
			),
			expected: false,
		},
		{
			name:      "rule without placement policy, node group with placement policy - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{PlacementGroup: placement.Spec{Policy: "policy"}, MachineType: machineType}).Build(),
			rule:      NewRule(WithMachineTypeRule(&machineType)),
			expected:  true,
		},
		{
			name:      "rule and node group with same placement policy - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{PlacementGroup: placement.Spec{Policy: "policy"}, MachineType: machineType}).Build(),
			rule: NewRule(
				WithPlacementPolicyRule("policy"),
				WithMachineTypeRule(&machineType),
			),
			expected: true,
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
