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

package csn

import (
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"
)

func TestPodRequestsLocalSSD(t *testing.T) {
	localSSDRequirement := apiv1.NodeSelectorRequirement{
		Key:      labels.EphemeralLocalSsdLabel,
		Operator: apiv1.NodeSelectorOpIn,
		Values:   []string{labels.EphemeralLocalSsdEnabledValue},
	}
	zoneRequirement := apiv1.NodeSelectorRequirement{
		Key:      "topology.kubernetes.io/zone",
		Operator: apiv1.NodeSelectorOpIn,
		Values:   []string{"us-central1-a"},
	}

	tests := []struct {
		name string
		pod  *apiv1.Pod
		want bool
	}{
		{
			name: "no local SSD selector or affinity",
			pod:  test.BuildTestPod("pod", 1000, units.GiB),
			want: false,
		},
		{
			name: "nodeSelector requests local SSD",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{
						labels.EphemeralLocalSsdLabel: labels.EphemeralLocalSsdEnabledValue,
					},
				},
			},
			want: true,
		},
		{
			name: "nodeSelector disables local SSD",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{
						labels.EphemeralLocalSsdLabel: "false",
					},
				},
			},
			want: false,
		},
		{
			name: "nodeAffinity with empty nodeSelectorTerms",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{},
							},
						},
					},
				},
			},
			want: false,
		},
		{
			name: "nodeAffinity requires local SSD in single term",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{MatchExpressions: []apiv1.NodeSelectorRequirement{localSSDRequirement}},
								},
							},
						},
					},
				},
			},
			want: true,
		},
		{
			name: "nodeAffinity requires local SSD in all terms",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{MatchExpressions: []apiv1.NodeSelectorRequirement{localSSDRequirement, zoneRequirement}},
									{MatchExpressions: []apiv1.NodeSelectorRequirement{localSSDRequirement}},
								},
							},
						},
					},
				},
			},
			want: true,
		},
		{
			name: "nodeAffinity requires local SSD in only one of two terms",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					Affinity: &apiv1.Affinity{
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{MatchExpressions: []apiv1.NodeSelectorRequirement{localSSDRequirement}},
									{MatchExpressions: []apiv1.NodeSelectorRequirement{zoneRequirement}},
								},
							},
						},
					},
				},
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, podRequestsLocalSSD(tc.pod))
			MakePodCSN(tc.pod, "ns/buffer")
			assert.Equal(t, tc.want, podRequestsLocalSSD(tc.pod))
		})
	}
}

func TestPodRequestsLocalSSDNilPod(t *testing.T) {
	assert.False(t, podRequestsLocalSSD(nil))
}
