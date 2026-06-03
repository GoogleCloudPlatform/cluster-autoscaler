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

func TestMachineSpecRuleMatchesNodeGroup(t *testing.T) {
	defaultMachineFamily := machinetypes.E2
	defaultMachineType := fmt.Sprintf("%s-standard-4", defaultMachineFamily.Name())

	nonDefaultMachineFamilyName := machinetypes.N2.Name()
	machineType := "n2-standard-8"
	nonDefaultMachineType := fmt.Sprintf("%s-standard-8", nonDefaultMachineFamilyName)
	higherMinCores := 8
	higherMemoryGb := 32
	nonDefaultSpot := true

	testCases := []struct {
		name      string
		nodegroup cloudprovider.NodeGroup
		rule      MachineSpecRule
		expected  bool
	}{
		{
			name:      "rule with no specified values, node group with defaults - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: defaultMachineType}).Build(),
			rule:      NewMachineSpecRule(nil, nil, nil, nil),
			expected:  true,
		},
		{
			name:      "rule with no specified, node group with different machine family - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: fmt.Sprintf("%v-standard-4", nonDefaultMachineFamilyName)}).Build(),
			rule:      NewMachineSpecRule(nil, nil, nil, nil),
			expected:  true,
		},
		{
			name:      "rule with no specified values, node group with different spot - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: defaultMachineType, Spot: nonDefaultSpot}).Build(),
			rule:      NewMachineSpecRule(nil, nil, nil, nil),
			expected:  true,
		},
		{
			name:      "rule with non-default machine type, node group with default machine type - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: defaultMachineType}).Build(),
			rule:      NewMachineSpecRule(&nonDefaultMachineFamilyName, nil, nil, nil),
			expected:  false,
		},
		{
			name:      "rule with non-default machine type, node group invalid machine type - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: "invalid-type"}).Build(),
			rule:      NewMachineSpecRule(&nonDefaultMachineFamilyName, nil, nil, nil),
			expected:  false,
		},
		{
			name:      "rule with non-default machine type, node group with non-default machine type - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType}).Build(),
			rule:      NewMachineSpecRule(&nonDefaultMachineFamilyName, nil, nil, nil),
			expected:  true,
		},
		{
			name:      "rule with higher min cores, node group with default min cores - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: defaultMachineType}).Build(),
			rule:      NewMachineSpecRule(nil, nil, &higherMinCores, nil),
			expected:  false,
		},
		{
			name:      "rule with higher min cores, node group with higher min cores - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: fmt.Sprintf("%s-standard-8", defaultMachineFamily.Name())}).Build(),
			rule:      NewMachineSpecRule(nil, nil, &higherMinCores, nil),
			expected:  true,
		},
		{
			name:      "rule with higher min memory, node group with default min memory - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: defaultMachineType}).Build(),
			rule:      NewMachineSpecRule(nil, nil, nil, &higherMemoryGb),
			expected:  false,
		},
		{
			name:      "rule with higher min memory, node group with higher min memory - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: fmt.Sprintf("%s-standard-8", defaultMachineFamily.Name())}).Build(),
			rule:      NewMachineSpecRule(nil, nil, nil, &higherMemoryGb),
			expected:  true,
		},
		{
			name:      "rule with non-default spot, node group with default spot - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: defaultMachineType}).Build(),
			rule:      NewMachineSpecRule(nil, &nonDefaultSpot, nil, nil),
			expected:  false,
		},
		{
			name:      "rule with non-default spot, node group with non-default spot - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: defaultMachineType, Spot: nonDefaultSpot}).Build(),
			rule:      NewMachineSpecRule(nil, &nonDefaultSpot, nil, nil),
			expected:  true,
		},
		{
			name:      "rule with all non-default values, node group with all default values - no matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: defaultMachineType}).Build(),
			rule:      NewMachineSpecRule(&nonDefaultMachineFamilyName, &nonDefaultSpot, &higherMinCores, &higherMemoryGb),
			expected:  false,
		},
		{
			name:      "rule with all non-default values, node group with all non-default values - matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: nonDefaultMachineType, Spot: nonDefaultSpot}).Build(),
			rule:      NewMachineSpecRule(&nonDefaultMachineFamilyName, &nonDefaultSpot, &higherMinCores, &higherMemoryGb),
			expected:  true,
		},
		{
			name:      "rule with instance type, matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: machineType}).Build(),
			rule:      NewRule(WithMachineTypeRule(&machineType)),
			expected:  true,
		},
		{
			name:      "rule with instance type, node group with different instance type, not matching",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: "e2-standard-8"}).Build(),
			rule:      NewRule(WithMachineTypeRule(&machineType)),
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
