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

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
)

func TestTaintsRuleMatchesNodeGroup(t *testing.T) {
	defaultMachineFamily := machinetypes.E2
	defaultMachineType := fmt.Sprintf("%s-standard-4", defaultMachineFamily.Name())

	testCases := []struct {
		name      string
		nodegroup cloudprovider.NodeGroup
		rule      TaintsRule
		expected  bool
	}{
		{
			name: "taints do match node group taints",
			nodegroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType: defaultMachineType,
					Taints: []apiv1.Taint{
						{
							Key:    "user-label-1",
							Value:  "user-value-1",
							Effect: apiv1.TaintEffectNoSchedule,
						},
						{
							Key:    "user-label-2",
							Value:  "user-value-2",
							Effect: apiv1.TaintEffectNoExecute,
						},
					},
				}).
				Build(),
			rule: NewRule(
				WithTaintsRule([]apiv1.Taint{
					{
						Key:    "user-label-1",
						Value:  "user-value-1",
						Effect: apiv1.TaintEffectNoSchedule,
					},
					{
						Key:    "user-label-2",
						Value:  "user-value-2",
						Effect: apiv1.TaintEffectNoExecute,
					},
				}),
			),
			expected: true,
		},
		{
			name: "taints do not match node group taints by keys",
			nodegroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType: defaultMachineType,
					Taints: []apiv1.Taint{
						{
							Key:    "user-label-1",
							Value:  "user-value-1",
							Effect: apiv1.TaintEffectNoSchedule,
						},
					},
				}).
				Build(),
			rule: NewRule(
				WithTaintsRule([]apiv1.Taint{
					{
						Key:    "user-label-1",
						Value:  "user-value-1",
						Effect: apiv1.TaintEffectNoSchedule,
					},
					{
						Key:    "user-label-2",
						Value:  "user-value-2",
						Effect: apiv1.TaintEffectNoExecute,
					},
				}),
			),
			expected: false,
		},
		{
			name: "taints do not match node group taints by values",
			nodegroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType: defaultMachineType,
					Taints: []apiv1.Taint{
						{
							Key:    "user-label-1",
							Value:  "user-value-1",
							Effect: apiv1.TaintEffectNoSchedule,
						},
						{
							Key:    "user-label-2",
							Value:  "user-value-2",
							Effect: apiv1.TaintEffectNoExecute,
						},
					},
				}).
				Build(),
			rule: NewRule(
				WithTaintsRule([]apiv1.Taint{
					{
						Key:    "user-label-1",
						Value:  "user-value-1",
						Effect: apiv1.TaintEffectNoSchedule,
					},
					{
						Key:    "user-label-2",
						Value:  "user-value-different",
						Effect: apiv1.TaintEffectNoExecute,
					},
				}),
			),
			expected: false,
		},
		{
			name: "taints do not match node group taints by effects",
			nodegroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType: defaultMachineType,
					Taints: []apiv1.Taint{
						{
							Key:    "user-label-1",
							Value:  "user-value-1",
							Effect: apiv1.TaintEffectNoSchedule,
						},
						{
							Key:    "user-label-2",
							Value:  "user-value-2",
							Effect: apiv1.TaintEffectNoExecute,
						},
					},
				}).
				Build(),
			rule: NewRule(
				WithTaintsRule([]apiv1.Taint{
					{
						Key:    "user-label-1",
						Value:  "user-value-1",
						Effect: apiv1.TaintEffectNoSchedule,
					},
					{
						Key:    "user-label-2",
						Value:  "user-value-2",
						Effect: apiv1.TaintEffectPreferNoSchedule,
					},
				}),
			),
			expected: false,
		},
		{
			name: "taints are a subset of node group taints",
			nodegroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType: defaultMachineType,
					Taints: []apiv1.Taint{
						{
							Key:    "user-label-1",
							Value:  "user-value-1",
							Effect: apiv1.TaintEffectNoSchedule,
						},
						{
							Key:    "user-label-2",
							Value:  "user-value-2",
							Effect: apiv1.TaintEffectNoExecute,
						},
						{
							Key:    "extra-label-1",
							Value:  "extra-value-1",
							Effect: apiv1.TaintEffectPreferNoSchedule,
						},
					},
				}).
				Build(),
			rule: NewRule(
				WithTaintsRule([]apiv1.Taint{
					{
						Key:    "user-label-1",
						Value:  "user-value-1",
						Effect: apiv1.TaintEffectNoSchedule,
					},
					{
						Key:    "user-label-2",
						Value:  "user-value-2",
						Effect: apiv1.TaintEffectNoExecute,
					},
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
