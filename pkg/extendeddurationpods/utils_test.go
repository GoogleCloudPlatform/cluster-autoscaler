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

package extendeddurationpods

import (
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	ekvmtypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
)

func TestEdpSelector(t *testing.T) {
	podWithNodeSelector := func(value string) *apiv1.Pod {
		pod := test.BuildTestPod("test-pod", 1000, 0)
		pod.Spec.NodeSelector = map[string]string{
			gkelabels.ExtendedDurationPodsLabel: value,
		}
		return pod
	}

	podWithNodeAffinity := func(values []string) *apiv1.Pod {
		pod := test.BuildTestPod("test-pod", 1000, 0)
		pod.Spec.Affinity = &apiv1.Affinity{
			NodeAffinity: &apiv1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
					NodeSelectorTerms: []apiv1.NodeSelectorTerm{
						{
							MatchExpressions: []apiv1.NodeSelectorRequirement{
								{
									Key:      gkelabels.ExtendedDurationPodsLabel,
									Operator: apiv1.NodeSelectorOpIn,
									Values:   values,
								},
							},
						},
					},
				},
			},
		}
		return pod
	}

	podWithBoth := func(nsValue string, affinityValues []string) *apiv1.Pod {
		pod := podWithNodeAffinity(affinityValues)
		pod.Spec.NodeSelector = map[string]string{
			gkelabels.ExtendedDurationPodsLabel: nsValue,
		}
		return pod
	}

	for tn, tc := range map[string]struct {
		pod             *apiv1.Pod
		wantEdpSelector string
	}{
		"Label presents in nodeSelector": {
			pod:             podWithNodeSelector("1000m"),
			wantEdpSelector: "1000m",
		},
		"Label presents in nodeAffinity": {
			pod:             podWithNodeAffinity([]string{"500m"}),
			wantEdpSelector: "500m",
		},
		"Label presents in both": {
			pod:             podWithBoth("500m", []string{"1000m"}),
			wantEdpSelector: "1000m",
		},
		"No selector": {
			pod:             test.BuildTestPod("test-pod", 1000, 0),
			wantEdpSelector: "",
		},
		"Label 'X' presents in nodeSelector": {
			pod:             podWithNodeSelector(ekvmtypes.ExtendedDurationLabelX),
			wantEdpSelector: ekvmtypes.ExtendedDurationLabelX,
		},
		"Label 'X' presents in nodeAffinity": {
			pod:             podWithNodeAffinity([]string{ekvmtypes.ExtendedDurationLabelX}),
			wantEdpSelector: ekvmtypes.ExtendedDurationLabelX,
		},
		"Label 'X' presents in both": {
			pod:             podWithBoth(ekvmtypes.ExtendedDurationLabelX, []string{ekvmtypes.ExtendedDurationLabelX}),
			wantEdpSelector: ekvmtypes.ExtendedDurationLabelX,
		},
		"Label numeric and 'X' present (prioritizes numeric)": {
			pod:             podWithNodeAffinity([]string{ekvmtypes.ExtendedDurationLabelX, "500m"}),
			wantEdpSelector: "500m",
		},
		"Label present but with empty values": {
			pod:             podWithNodeAffinity([]string{}),
			wantEdpSelector: "",
		},
	} {
		t.Run(tn, func(t *testing.T) {
			assert.Equal(t, EdpSelector(tc.pod), tc.wantEdpSelector)
		})
	}
}

func TestEdpOnePodPerNode(t *testing.T) {
	for tn, tc := range map[string]struct {
		node                     *apiv1.Node
		expectedEdpOnePodPerNode bool
	}{
		"No EDP label - return false": {
			node:                     buildNodeWithLabels("node", 100, 500, map[string]string{}),
			expectedEdpOnePodPerNode: false,
		},
		"Empty EDP label - return false": {
			node: buildNodeWithLabels("node", 100, 500, map[string]string{
				gkelabels.ExtendedDurationPodsLabel: "",
			}),
			expectedEdpOnePodPerNode: false,
		},
		"Packed EDP label value - return false": {
			node: buildNodeWithLabels("node", 100, 500, map[string]string{
				gkelabels.ExtendedDurationPodsLabel: gkelabels.ExtendedDurationPackedPodsValue,
			}),
			expectedEdpOnePodPerNode: false,
		},
		"Non-packed EDP label, EK machine family - return false": {
			node: buildNodeWithLabels("node", 100, 500, map[string]string{
				gkelabels.ExtendedDurationPodsLabel: "100m",
				gkelabels.MachineFamilyLabel:        "ek",
			}),
			expectedEdpOnePodPerNode: false,
		},
		"Non-packed EDP label, unknown machine family - return true": {
			node: buildNodeWithLabels("node", 100, 500, map[string]string{
				gkelabels.ExtendedDurationPodsLabel: "100m",
			}),
			expectedEdpOnePodPerNode: true,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			assert.Equal(t, tc.expectedEdpOnePodPerNode, EdpOnePodPerNode(tc.node))
		})
	}
}

func buildNodeWithLabels(name string, millicpuCapacity int64, memCapacity int64, labels map[string]string) *apiv1.Node {
	node := test.BuildTestNode(name, millicpuCapacity, memCapacity)
	node.Labels = labels
	return node
}
