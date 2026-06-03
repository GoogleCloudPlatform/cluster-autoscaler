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

func TestSelfServiceRuleMatchesNodeGroup(t *testing.T) {
	defaultMachineFamily := machinetypes.E2
	defaultMachineType := fmt.Sprintf("%s-standard-4", defaultMachineFamily.Name())

	testCases := []struct {
		name      string
		nodegroup cloudprovider.NodeGroup
		rule      SelfServiceRule
		expected  bool
	}{
		{
			name: "rule with self service labels, node group without these labels",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: defaultMachineType,
				Labels: map[string]string{
					"l":  "v",
					"f1": "v1",
				},
				SelfServiceMetadata: map[string]string{
					"l":  "v",
					"f1": "v1",
				},
			}).Build(),
			rule: NewRule(
				WithSelfServiceRule(map[string]string{
					"f1": "v1",
					"f2": "v2",
				}),
			),
			expected: false,
		},
		{
			name: "rule with self service labels, node group with these labels",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: defaultMachineType,
				Labels: map[string]string{
					"l":  "v",
					"f1": "v1",
					"f2": "v2",
				},
				SelfServiceMetadata: map[string]string{
					"l":  "v",
					"f1": "v1",
					"f2": "v2",
				},
			}).Build(),
			rule: NewRule(
				WithSelfServiceRule(map[string]string{
					"f1": "v1",
					"f2": "v2",
				}),
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
