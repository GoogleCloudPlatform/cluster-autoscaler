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
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
)

func TestLocationRuleMatchesNodeGroup(t *testing.T) {
	machineType := "n2-standard-8"
	testCases := []struct {
		name      string
		nodegroup cloudprovider.NodeGroup
		rule      NodePoolsRule
		expected  bool
	}{
		{
			name: "node group in the only preferred zone - match",
			nodegroup: gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{
				Zone: "zone1",
			}).SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"zone1"},
			}).SetExist(false).Build(),
			rule:     NewRule(WithLocationRule([]string{"zone1"})),
			expected: true,
		},
		{
			name: "existing node group matches zonal preferences",
			nodegroup: gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{
				Zone: "zone1",
			}).SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"zone1", "zone2"},
			}).SetExist(true).Build(),
			rule:     NewRule(WithLocationRule([]string{"zone1", "zone2"})),
			expected: true,
		},
		{
			name: "existing node group matches rule with no zonal preferences",
			nodegroup: gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{
				Zone: "zone1",
			}).SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"zone1", "zone2"},
			}).SetExist(true).Build(),
			rule:     NewRule(WithMachineTypeRule(&machineType)),
			expected: true,
		},
		{
			name: "NAP node group matches no zonal preferences",
			nodegroup: gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{
				Zone: "zone1",
			}).SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
			}).SetExist(false).Build(),
			rule:     NewRule(WithMachineTypeRule(&machineType)),
			expected: true,
		},
		{
			name: "NAP node group with zonal preferences does not match other rules",
			nodegroup: gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{
				Zone: "zone1",
			}).SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"zone1"},
			}).SetExist(false).Build(),
			rule:     NewRule(WithMachineTypeRule(&machineType)),
			expected: true,
		},
		{
			name: "existing node group matches empty zonal preferences",
			nodegroup: gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{
				Zone: "zone1",
			}).SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"zone1"},
			}).SetExist(true).Build(),
			rule:     NewRule(WithLocationRule([]string{})),
			expected: true,
		},
		{
			name: "existing node group doesn't match if zonal preferences is a superset of node pool zones",
			nodegroup: gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{
				Zone: "zone1",
			}).SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"zone1", "zone2"},
			}).SetExist(true).Build(),
			rule:     NewRule(WithLocationRule([]string{"zone1", "zone2", "zone3"})),
			expected: false,
		},
		{
			name: "existing node group doesn't match if zonal preferences is only a subset of node pool zones",
			nodegroup: gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{
				Zone: "zone1",
			}).SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
				Locations:   []string{"zone1", "zone2"},
			}).SetExist(true).Build(),
			rule:     NewRule(WithLocationRule([]string{"zone1"})),
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
