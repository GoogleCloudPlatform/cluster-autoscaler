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

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
)

func TestMinCpuPlatformRuleMatchesNodeGroup(t *testing.T) {
	// intelSandyBridgeName < intelEmeraldRapids < intelGraniteRapids
	intelSandyBridgeName := "Intel Sandy Bridge"
	intelEmeraldRapidsName := "Intel Emerald Rapids"
	intelGraniteRapidsName := "Intel Granite Rapids"
	invalidCpuPlatformName := "Invalid CPU Platform"
	amdMilanName := "AMD Milan"

	machineType := "n2-standard-8"

	testCases := []struct {
		name          string
		nodegroup     cloudprovider.NodeGroup
		rule          MinCpuPlatformRule
		expectedMatch bool
	}{
		{
			name:          "rule with min cpu platform rule set, node group with the higher min cpu platform, matching",
			nodegroup:     gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MinCpuPlatform: intelEmeraldRapidsName, MachineType: machineType}).Build(),
			rule:          NewRule(WithMinCpuPlatformRule(&intelSandyBridgeName), WithMachineTypeRule(&machineType)),
			expectedMatch: true,
		},
		{
			name:          "rule with min cpu platform rule set, node group with the lower min cpu platform, not matching",
			nodegroup:     gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MinCpuPlatform: intelEmeraldRapidsName, MachineType: machineType}).Build(),
			rule:          NewRule(WithMinCpuPlatformRule(&intelGraniteRapidsName), WithMachineTypeRule(&machineType)),
			expectedMatch: false,
		},
		{
			name:          "rule with min cpu platform rule set, node group with the exact min cpu platform, matching",
			nodegroup:     gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MinCpuPlatform: intelEmeraldRapidsName, MachineType: machineType}).Build(),
			rule:          NewRule(WithMinCpuPlatformRule(&intelEmeraldRapidsName), WithMachineTypeRule(&machineType)),
			expectedMatch: true,
		},
		{
			name:          "rule with min cpu platform rule set, node group with invalid min cpu platform, not matching",
			nodegroup:     gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MinCpuPlatform: invalidCpuPlatformName, MachineType: machineType}).Build(),
			rule:          NewRule(WithMinCpuPlatformRule(&intelGraniteRapidsName), WithMachineTypeRule(&machineType)),
			expectedMatch: false,
		},
		{
			name:          "rule with min cpu platform rule set, node group with no min cpu platform set, not matching",
			nodegroup:     gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: machineType}).Build(),
			rule:          NewRule(WithMinCpuPlatformRule(&intelEmeraldRapidsName), WithMachineTypeRule(&machineType)),
			expectedMatch: false,
		},
		{
			name:          "rule with min cpu platform rule set, node group with the min cpu platform from other order, not matching",
			nodegroup:     gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MinCpuPlatform: amdMilanName, MachineType: machineType}).Build(),
			rule:          NewRule(WithMinCpuPlatformRule(&intelEmeraldRapidsName), WithMachineTypeRule(&machineType)),
			expectedMatch: false,
		},
		{
			name:          "rule with any min cpu platform, node group with the min cpu platform set, matching",
			nodegroup:     gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MinCpuPlatform: intelEmeraldRapidsName, MachineType: machineType}).Build(),
			rule:          NewRule(WithMinCpuPlatformRule(nil), WithMachineTypeRule(&machineType)),
			expectedMatch: true,
		},
		{
			name:          "rule with any min cpu platform, node group with no min cpu platform set, matching",
			nodegroup:     gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: machineType}).Build(),
			rule:          NewRule(WithMinCpuPlatformRule(nil), WithMachineTypeRule(&machineType)),
			expectedMatch: true,
		},
		{
			name:          "rule with invalid min cpu platform, node group with no min cpu platform set, no matching",
			nodegroup:     gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: machineType}).Build(),
			rule:          NewRule(WithMinCpuPlatformRule(&invalidCpuPlatformName), WithMachineTypeRule(&machineType)),
			expectedMatch: false,
		},
		{
			name:          "rule with invalid min cpu platform, node group with min cpu platform set, no matching",
			nodegroup:     gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MinCpuPlatform: amdMilanName, MachineType: machineType}).Build(),
			rule:          NewRule(WithMinCpuPlatformRule(&invalidCpuPlatformName), WithMachineTypeRule(&machineType)),
			expectedMatch: false,
		},

		// Without specifying min cpu platform it is by default set to AnyPlatform.
		// Such rule will match any node group regardless of their minCpuPlatform values.
		{
			name:          "rule without min cpu platform set defaults to any, node group with no min cpu platform set, matching",
			nodegroup:     gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MachineType: machineType}).Build(),
			rule:          NewRule(WithMachineTypeRule(&machineType)),
			expectedMatch: true,
		},
		{
			name:          "rule without min cpu platform set defaults to any, node group with the min cpu platform set, matching",
			nodegroup:     gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MinCpuPlatform: intelEmeraldRapidsName, MachineType: machineType}).Build(),
			rule:          NewRule(WithMachineTypeRule(&machineType)),
			expectedMatch: true,
		},
		{
			name:          "rule without min cpu platform set defaults to any, node group with invalid min cpu platform, matching",
			nodegroup:     gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{MinCpuPlatform: invalidCpuPlatformName, MachineType: machineType}).Build(),
			rule:          NewRule(WithMachineTypeRule(&machineType)),
			expectedMatch: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.rule.Matches(tc.nodegroup)
			if actual != tc.expectedMatch {
				t.Errorf("Test: \"%v\" failed, expected matching: %v got: %v", tc.name, tc.expectedMatch, actual)
			}
		})
	}
}

func TestMinCpuPlatform(t *testing.T) {
	intelEmeraldRapidsName := "Intel Emerald Rapids"
	amdMilanName := "AMD Milan"
	genericName := "name"
	emptyName := ""
	testCases := []struct {
		name             string
		rule             MinCpuPlatformRule
		expectedName     string
		expectedPlatform machinetypes.CpuPlatform
	}{
		{
			name:             "rule with generic not known name set",
			rule:             NewRule(WithMinCpuPlatformRule(&genericName)),
			expectedName:     genericName,
			expectedPlatform: machinetypes.UnknownPlatform,
		},
		{
			name:             "rule with name not set",
			rule:             NewRule(),
			expectedName:     emptyName,
			expectedPlatform: machinetypes.AnyPlatform,
		},
		{
			name:             "rule with empty name set",
			rule:             NewRule(WithMinCpuPlatformRule(&emptyName)),
			expectedName:     emptyName,
			expectedPlatform: machinetypes.UnknownPlatform,
		},
		{
			name:             "rule with Intel Emerald Rapids name set",
			rule:             NewRule(WithMinCpuPlatformRule(&intelEmeraldRapidsName)),
			expectedName:     intelEmeraldRapidsName,
			expectedPlatform: machinetypes.IntelEmeraldRapids,
		},
		{
			name:             "rule with AMD Milan name set",
			rule:             NewRule(WithMinCpuPlatformRule(&amdMilanName)),
			expectedName:     amdMilanName,
			expectedPlatform: machinetypes.AmdMilan,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedName, tc.rule.MinCpuPlatformString())
			actualPlatform, _ := tc.rule.MinCpuPlatform()
			assert.Equal(t, tc.expectedPlatform, actualPlatform)
		})
	}
}
