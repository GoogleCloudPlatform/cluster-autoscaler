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
	"fmt"
	"testing"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
)

func TestMaxPodsPerNodeRuleMatchesNodeGroup(t *testing.T) {
	nonDefaultMachineFamilyName := machinetypes.N2.Name()
	nonDefaultMachineType := fmt.Sprintf("%s-standard-8", nonDefaultMachineFamilyName)

	defaultMaxPodsPerNode := 120
	nonDefaultMaxPodsPerNode := 10

	testCases := []struct {
		name      string
		nodegroup cloudprovider.NodeGroup
		rule      MaxPodsPerNodeRule
		expected  bool
	}{
		{
			name:      "rule with max pods per node, node group without max pods per node - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, MaxPodsPerNode: int64(defaultMaxPodsPerNode)}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithMaxPodsPerNodeRule(&defaultMaxPodsPerNode),
			),
			expected: true,
		},
		{
			name:      "rule with max pods per node, node group without max pods per node - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithMaxPodsPerNodeRule(&defaultMaxPodsPerNode),
			),
			expected: false,
		},
		{
			name:      "rule with max pods per node, node group with different max pods per node - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, MaxPodsPerNode: int64(nonDefaultMaxPodsPerNode)}).Build(),
			rule: NewRule(
				WithMachineFamilyRule(&nonDefaultMachineFamilyName),
				WithMaxPodsPerNodeRule(&defaultMaxPodsPerNode),
			),
			expected: false,
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
