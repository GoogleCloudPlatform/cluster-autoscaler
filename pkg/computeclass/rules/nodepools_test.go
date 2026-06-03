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
)

func TestNodepoolsRuleMatchesNodeGroup(t *testing.T) {
	defaultNodepoolName := "default-nodepool"

	testCases := []struct {
		name      string
		nodegroup cloudprovider.NodeGroup
		rule      NodePoolsRule
		expected  bool
	}{
		{
			name:      "nodepool is the only one listed - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetNodePoolName(defaultNodepoolName).Build(),
			rule:      NewRule(WithNodePoolsRule([]string{defaultNodepoolName})),
			expected:  true,
		},
		{
			name:      "nodepool is one of the listed - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetNodePoolName(defaultNodepoolName).Build(),
			rule:      NewRule(WithNodePoolsRule([]string{"other-np", defaultNodepoolName, "yet-another-np"})),
			expected:  true,
		},
		{
			name:      "nodepool is not listed - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetNodePoolName(defaultNodepoolName).Build(),
			rule:      NewRule(WithNodePoolsRule([]string{"other-np", "some-other-np", "yet-another-np"})),
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
