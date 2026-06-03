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

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

func TestFlexStartRuleMatchesNodeGroup(t *testing.T) {
	leadTimeSeconds := 3600
	nodeRecyclingConfig := &v1.NodeRecyclingConfig{
		LeadTimeSeconds: &leadTimeSeconds,
	}
	machineType := "n1-standard-2"

	testCases := []struct {
		name      string
		nodegroup cloudprovider.NodeGroup
		rule      FlexStartRule
		expected  bool
	}{
		{
			name: "ruleNoFlexStart_ngNoFlexStart_ok",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				MachineType: machineType,
			}).Build(),
			rule: NewRule(
				WithMachineTypeRule(&machineType)),
			expected: true,
		},
		{
			name: "ruleFlexStart_ngFlexStart_ok",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				FlexStart:   true,
				MachineType: machineType,
			}).Build(),
			rule: NewRule(
				WithFlexStartRule(true, nil),
				WithMachineTypeRule(&machineType)),
			expected: true,
		},
		{
			name: "ruleNoFlexStart_ngFlexStart_ok",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				FlexStart:   true,
				MachineType: machineType,
			}).Build(),
			rule: NewRule(
				WithFlexStartRule(false, nil),
				WithMachineTypeRule(&machineType)),
			expected: true,
		},
		{
			name: "ruleFlexStart_ngNoFlexStart_noMatch",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				FlexStart:   false,
				MachineType: machineType,
			}).Build(),
			rule: NewRule(
				WithFlexStartRule(true, nil),
				WithMachineTypeRule(&machineType)),
			expected: false,
		},
		{
			name: "ruleFlexStartAndRecycling_ngFlexStartAndRecycling_sameLeadTimeSeconds_ok",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				FlexStart: true,
				Labels: map[string]string{
					labels.NodeRecycleLeadTimeSecondsLabelKey: fmt.Sprintf("%d", *nodeRecyclingConfig.LeadTimeSeconds),
				},
				MachineType: machineType,
			}).Build(),
			rule: NewRule(
				WithFlexStartRule(true, nodeRecyclingConfig),
				WithMachineTypeRule(&machineType)),
			expected: true,
		},
		{
			name: "ruleFlexStartAndRecycling_ngFlexStartAndRecycling_differentLeadTimeSeconds_noMatch",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				FlexStart: true,
				Labels: map[string]string{
					labels.NodeRecycleLeadTimeSecondsLabelKey: "127",
				},
				MachineType: machineType,
			}).Build(),
			rule: NewRule(
				WithFlexStartRule(true, nodeRecyclingConfig),
				WithMachineTypeRule(&machineType)),
			expected: false,
		},
		{
			name: "ruleFlexStartAndRecycling_ngFlexStart_recyclingMismatch_noMatch",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				FlexStart:   true,
				MachineType: machineType,
			}).Build(),
			rule: NewRule(
				WithFlexStartRule(true, nodeRecyclingConfig),
				WithMachineTypeRule(&machineType)),
			expected: false,
		},
		{
			name: "ruleFlexStart_ngFlexStartAndRecycling_recyclingMismatch_noMatch",
			nodegroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				FlexStart: true,
				Labels: map[string]string{
					labels.NodeRecycleLeadTimeSecondsLabelKey: "127",
				},
				MachineType: machineType,
			}).Build(),
			rule: NewRule(
				WithFlexStartRule(true, nil),
				WithMachineTypeRule(&machineType)),
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
