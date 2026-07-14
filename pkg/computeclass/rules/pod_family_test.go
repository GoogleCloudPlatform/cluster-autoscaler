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
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
)

func TestPodFamilyRuleMatchesNodeGroup(t *testing.T) {
	generalPurposePodFamily := "general-purpose"
	unknownPodFamily := "unknown"

	testCases := []struct {
		name      string
		nodegroup cloudprovider.NodeGroup
		rule      MachineSpecRule
		expected  bool
	}{
		{
			name: "podFamily, not managed by Autopilot (should not happen), matches",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: "n2d-standard-4",
			}).Build(),
			rule:     NewRule(WithPodFamilyRule(&generalPurposePodFamily)),
			expected: true,
		},
		{
			name: "podFamily, managed by autopilot, matching machine family, matches",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: "e2-standard-4",
			}).Build(),
			rule:     NewRule(WithPodFamilyRule(&generalPurposePodFamily), WithAutopilotModeRule()),
			expected: true,
		},
		{
			name: "podFamily, managed by autopilot, non-matching machine family, does not match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: "c2-standard-4",
			}).Build(),
			rule:     NewRule(WithPodFamilyRule(&generalPurposePodFamily), WithAutopilotModeRule()),
			expected: false,
		},
		{
			name: "podFamily, managed by autopilot, matching machine family, BPSoHW, matches",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: "e2-standard-4",
				Labels:      map[string]string{gkelabels.PodsPerNodeKey: gkelabels.BinpackedSliceOfHardwareValue},
			}).Build(),
			rule:     NewRule(WithPodFamilyRule(&generalPurposePodFamily), WithAutopilotModeRule()),
			expected: false,
		},
		{
			name: "podFamily, managed by autopilot, matchine machine family, EK, matches",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: "ek-standard-32",
			}).Build(),
			rule:     NewRule(WithPodFamilyRule(&generalPurposePodFamily), WithAutopilotModeRule()),
			expected: true,
		},
		{
			name: "no podFamily, managed by autopilot, BPSoHW, matches",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: "n2d-standard-4",
				Labels:      map[string]string{gkelabels.PodsPerNodeKey: gkelabels.BinpackedSliceOfHardwareValue},
			}).Build(),
			rule:     NewRule(WithAutopilotModeRule()),
			expected: true,
		},
		{
			name: "no podFamily, managed by autopilot, no BPSoHW, does not match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: "n2d-standard-4",
			}).Build(),
			rule:     NewRule(WithAutopilotModeRule()),
			expected: false,
		},
		{
			name: "unknown podFamily, managed by autopilot, does not match",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: "e2-standard-4",
			}).Build(),
			rule:     NewRule(WithPodFamilyRule(&unknownPodFamily), WithAutopilotModeRule()),
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

func TestPodFamilyMachineFamilies(t *testing.T) {
	generalPurposePodFamily := "general-purpose"
	unknownPodFamily := "unknown"
	emptyPodFamily := ""

	testCases := []struct {
		name                    string
		rule                    Rule
		expectedMachineFamilies []machinetypes.MachineFamily
		expectedError           bool
	}{
		{
			name:          "empty podFamily, expect error",
			rule:          NewRule(WithPodFamilyRule(&emptyPodFamily)),
			expectedError: true,
		},
		{
			name:          "unknown podFamily, expect error",
			rule:          NewRule(WithPodFamilyRule(&unknownPodFamily)),
			expectedError: true,
		},
		{
			name:                    "general purpose podFamily, expect E2 and EK",
			rule:                    NewRule(WithPodFamilyRule(&generalPurposePodFamily)),
			expectedMachineFamilies: podFamilyMachineFamilies[generalPurposePodFamily],
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			machineFamilies, err := tc.rule.PodFamilyMachineFamilies()
			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.ElementsMatch(t, tc.expectedMachineFamilies, machineFamilies)
			}
		})
	}
}
