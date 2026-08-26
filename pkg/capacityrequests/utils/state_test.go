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

package utils

import (
	"testing"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	client_testing "k8s.io/client-go/testing"
	cr_types "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/apis/internal.autoscaling.gke.io/v1"
	cr_fake "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/client/clientset/versioned/fake"

	"github.com/stretchr/testify/assert"
)

func TestSetResources(t *testing.T) {
	testCases := []struct {
		caseName          string
		capacityRequest   *cr_types.CapacityRequest
		expectedCondition cr_types.CapacityRequestConditionType
		expectedAction    bool
	}{
		{
			caseName:          "ResourcesAvailable. Sets condition.",
			capacityRequest:   BuildTestCr("cr", "600m", "0", []cr_types.CapacityRequestConditionType{}),
			expectedCondition: cr_types.ResourcesAvailable,
			expectedAction:    true,
		}, {
			caseName:          "ResourcesAvailable. Unsets other conditions.",
			capacityRequest:   BuildTestCr("cr", "100m", "0", []cr_types.CapacityRequestConditionType{cr_types.ResourcesInProvisioning}),
			expectedCondition: cr_types.ResourcesAvailable,
			expectedAction:    true,
		}, {
			caseName:          "ResourcesAvailable. No action needed if status set before.",
			capacityRequest:   BuildTestCr("cr", "10m", "0", []cr_types.CapacityRequestConditionType{cr_types.ResourcesAvailable}),
			expectedCondition: cr_types.ResourcesAvailable,
			expectedAction:    false,
		}, {
			caseName:          "ResourcesInProvisioning. Sets condition.",
			capacityRequest:   BuildTestCr("cr", "600m", "0", []cr_types.CapacityRequestConditionType{}),
			expectedCondition: cr_types.ResourcesInProvisioning,
			expectedAction:    true,
		}, {
			caseName:          "ResourcesInProvisioning. Sets other conditions to false.",
			capacityRequest:   BuildTestCr("cr", "100m", "0", []cr_types.CapacityRequestConditionType{cr_types.ResourcesUnattainable}),
			expectedCondition: cr_types.ResourcesInProvisioning,
			expectedAction:    true,
		}, {
			caseName:          "ResourcesInProvisioning. No action needed if status set before.",
			capacityRequest:   BuildTestCr("cr", "10m", "0", []cr_types.CapacityRequestConditionType{cr_types.ResourcesInProvisioning}),
			expectedCondition: cr_types.ResourcesInProvisioning,
			expectedAction:    false,
		}, {
			caseName:          "ResourcesUnattainable. Sets condition.",
			capacityRequest:   BuildTestCr("cr", "600m", "0", []cr_types.CapacityRequestConditionType{}),
			expectedCondition: cr_types.ResourcesUnattainable,
			expectedAction:    true,
		}, {
			caseName:          "ResourcesUnattainable. Sets other conditions to false.",
			capacityRequest:   BuildTestCr("cr", "100m", "0", []cr_types.CapacityRequestConditionType{cr_types.ResourcesInProvisioning}),
			expectedCondition: cr_types.ResourcesUnattainable,
			expectedAction:    true,
		}, {
			caseName:          "ResourcesUnattainable. No action needed if status set before.",
			capacityRequest:   BuildTestCr("cr", "10m", "0", []cr_types.CapacityRequestConditionType{cr_types.ResourcesUnattainable}),
			expectedCondition: cr_types.ResourcesUnattainable,
			expectedAction:    false,
		}, {
			caseName:          "ResourcesUnknown. Sets all conditions to false.",
			capacityRequest:   BuildTestCr("cr", "600m", "0", []cr_types.CapacityRequestConditionType{cr_types.ResourcesUnattainable}),
			expectedCondition: "",
			expectedAction:    true,
		}, {
			caseName:          "ResourcesUnknown. No action needed if status not changed.",
			capacityRequest:   BuildTestCr("cr", "10m", "0", []cr_types.CapacityRequestConditionType{}),
			expectedCondition: "",
			expectedAction:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			fakeClient := cr_fake.NewSimpleClientset(tc.capacityRequest)
			crState := NewCapacityRequestState(fakeClient)
			switch tc.expectedCondition {
			case cr_types.ResourcesAvailable:
				err := crState.SetResourcesAvailable(tc.capacityRequest)
				assert.NoError(t, err)
			case cr_types.ResourcesInProvisioning:
				err := crState.SetResourcesInProvisioning(tc.capacityRequest)
				assert.NoError(t, err)
			case cr_types.ResourcesUnattainable:
				err := crState.SetResourcesUnattainable(tc.capacityRequest)
				assert.NoError(t, err)
			case "":
				err := crState.SetResourcesUnknown(tc.capacityRequest)
				assert.NoError(t, err)
			default:
				t.Errorf("Unknown condition expected: %v", tc.expectedCondition)
			}

			actions := fakeClient.Actions()
			if tc.expectedAction {
				assert.Equal(t, 1, len(actions), "Test case '%v' failed. Exactly one action was expected.", tc.caseName)
				if len(actions) > 0 {
					a := actions[0]
					assert.Equal(t, "update", a.GetVerb(), "Test case '%v' failed. Unexpected action: %v", tc.caseName, a)
					ua := a.(client_testing.UpdateAction)
					obj := ua.GetObject()
					cr, ok := obj.(*cr_types.CapacityRequest)
					assert.True(t, ok, "Test case '%v' failed. Failed to cast object to Capacity Request: %v", tc.caseName, obj)
					found := false
					for _, cond := range cr.Status.Conditions {
						if cond.Type == tc.expectedCondition {
							assert.Equal(t, apiv1.ConditionTrue, cond.Status, "Test case '%v' failed. Missing %v condition on CapacityRequest %v", tc.caseName, cond.Type, cr)
							found = true
						} else {
							assert.NotEqual(t, apiv1.ConditionTrue, cond.Status, "Test case '%v' failed. Unexpected %v condition on CapacityRequest %v", tc.caseName, cond.Type, cr)
						}
					}
					if tc.expectedCondition != "" {
						assert.True(t, found, "Test case '%v' failed. Missing %v condition on CapacityRequest %v", tc.caseName, tc.expectedCondition, cr)
					}
				}
			} else {
				assert.Equal(t, 0, len(actions), "Test case '%v' failed. Unexpected actions: %v", tc.caseName, actions)
				if len(actions) > 0 {
					a := actions[0]
					ua := a.(client_testing.UpdateAction)
					obj := ua.GetObject()
					cr, _ := obj.(*cr_types.CapacityRequest)
					t.Errorf("Unexpected action object: %+v", cr)
				}
			}
		})
	}
}

func TestSanitizeNodeAffinity(t *testing.T) {
	testCases := []struct {
		name     string
		input    *apiv1.PodSpec
		expected *apiv1.PodSpec
	}{
		{
			name:     "nil spec",
			input:    nil,
			expected: nil,
		},
		{
			name:     "spec with nil affinity",
			input:    &apiv1.PodSpec{},
			expected: &apiv1.PodSpec{},
		},
		{
			name: "spec with nil node affinity",
			input: &apiv1.PodSpec{
				Affinity: &apiv1.Affinity{
					PodAntiAffinity: &apiv1.PodAntiAffinity{},
				},
			},
			expected: &apiv1.PodSpec{
				Affinity: &apiv1.Affinity{
					PodAntiAffinity: &apiv1.PodAntiAffinity{},
				},
			},
		},
		{
			name: "spec with single-node hostname affinity only",
			input: &apiv1.PodSpec{
				Affinity: &apiv1.Affinity{
					NodeAffinity: &apiv1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
							NodeSelectorTerms: []apiv1.NodeSelectorTerm{
								{
									MatchFields: []apiv1.NodeSelectorRequirement{
										{
											Key:      metav1.ObjectNameField,
											Operator: apiv1.NodeSelectorOpIn,
											Values:   []string{"gke-cluster-node-1234"},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: &apiv1.PodSpec{
				Affinity: nil,
			},
		},
		{
			name: "spec with single-node hostname affinity and pod affinity",
			input: &apiv1.PodSpec{
				Affinity: &apiv1.Affinity{
					NodeAffinity: &apiv1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
							NodeSelectorTerms: []apiv1.NodeSelectorTerm{
								{
									MatchFields: []apiv1.NodeSelectorRequirement{
										{
											Key:      metav1.ObjectNameField,
											Operator: apiv1.NodeSelectorOpIn,
											Values:   []string{"node-1"},
										},
									},
								},
							},
						},
					},
					PodAffinity: &apiv1.PodAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: []apiv1.PodAffinityTerm{
							{TopologyKey: "kubernetes.io/hostname"},
						},
					},
				},
			},
			expected: &apiv1.PodSpec{
				Affinity: &apiv1.Affinity{
					PodAffinity: &apiv1.PodAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: []apiv1.PodAffinityTerm{
							{TopologyKey: "kubernetes.io/hostname"},
						},
					},
				},
			},
		},
		{
			name: "spec with single-node hostname affinity and preferred node affinity",
			input: &apiv1.PodSpec{
				Affinity: &apiv1.Affinity{
					NodeAffinity: &apiv1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
							NodeSelectorTerms: []apiv1.NodeSelectorTerm{
								{
									MatchFields: []apiv1.NodeSelectorRequirement{
										{
											Key:      metav1.ObjectNameField,
											Operator: apiv1.NodeSelectorOpIn,
											Values:   []string{"node-1"},
										},
									},
								},
							},
						},
						PreferredDuringSchedulingIgnoredDuringExecution: []apiv1.PreferredSchedulingTerm{
							{
								Weight: 10,
								Preference: apiv1.NodeSelectorTerm{
									MatchExpressions: []apiv1.NodeSelectorRequirement{
										{
											Key:      "topology.kubernetes.io/zone",
											Operator: apiv1.NodeSelectorOpIn,
											Values:   []string{"us-central1-a"},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: &apiv1.PodSpec{
				Affinity: &apiv1.Affinity{
					NodeAffinity: &apiv1.NodeAffinity{
						PreferredDuringSchedulingIgnoredDuringExecution: []apiv1.PreferredSchedulingTerm{
							{
								Weight: 10,
								Preference: apiv1.NodeSelectorTerm{
									MatchExpressions: []apiv1.NodeSelectorRequirement{
										{
											Key:      "topology.kubernetes.io/zone",
											Operator: apiv1.NodeSelectorOpIn,
											Values:   []string{"us-central1-a"},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "mixed term with hostname matchField and zone matchExpression",
			input: &apiv1.PodSpec{
				Affinity: &apiv1.Affinity{
					NodeAffinity: &apiv1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
							NodeSelectorTerms: []apiv1.NodeSelectorTerm{
								{
									MatchExpressions: []apiv1.NodeSelectorRequirement{
										{
											Key:      "topology.kubernetes.io/zone",
											Operator: apiv1.NodeSelectorOpIn,
											Values:   []string{"us-central1-a"},
										},
									},
									MatchFields: []apiv1.NodeSelectorRequirement{
										{
											Key:      metav1.ObjectNameField,
											Operator: apiv1.NodeSelectorOpIn,
											Values:   []string{"node-1"},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: &apiv1.PodSpec{
				Affinity: &apiv1.Affinity{
					NodeAffinity: &apiv1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
							NodeSelectorTerms: []apiv1.NodeSelectorTerm{
								{
									MatchExpressions: []apiv1.NodeSelectorRequirement{
										{
											Key:      "topology.kubernetes.io/zone",
											Operator: apiv1.NodeSelectorOpIn,
											Values:   []string{"us-central1-a"},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "multi-term selector: one hostname term and one label expression term",
			input: &apiv1.PodSpec{
				Affinity: &apiv1.Affinity{
					NodeAffinity: &apiv1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
							NodeSelectorTerms: []apiv1.NodeSelectorTerm{
								{
									MatchFields: []apiv1.NodeSelectorRequirement{
										{
											Key:      metav1.ObjectNameField,
											Operator: apiv1.NodeSelectorOpIn,
											Values:   []string{"node-1"},
										},
									},
								},
								{
									MatchExpressions: []apiv1.NodeSelectorRequirement{
										{
											Key:      "cloud.google.com/gke-nodepool",
											Operator: apiv1.NodeSelectorOpIn,
											Values:   []string{"pool-1"},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: &apiv1.PodSpec{
				Affinity: &apiv1.Affinity{
					NodeAffinity: &apiv1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
							NodeSelectorTerms: []apiv1.NodeSelectorTerm{
								{
									MatchExpressions: []apiv1.NodeSelectorRequirement{
										{
											Key:      "cloud.google.com/gke-nodepool",
											Operator: apiv1.NodeSelectorOpIn,
											Values:   []string{"pool-1"},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			SanitizeNodeAffinity(tc.input)
			assert.Equal(t, tc.expected, tc.input)
		})
	}
}

func TestCapacityRequestState_Update_SanitizesNodeAffinity(t *testing.T) {
	cr := BuildTestCr("cr-daemonset", "500m", "1Gi", nil)
	cr.Spec.Capacity.NodeName = "node-1"
	cr.Spec.Capacity.Affinity = &apiv1.Affinity{
		NodeAffinity: &apiv1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
				NodeSelectorTerms: []apiv1.NodeSelectorTerm{
					{
						MatchFields: []apiv1.NodeSelectorRequirement{
							{
								Key:      metav1.ObjectNameField,
								Operator: apiv1.NodeSelectorOpIn,
								Values:   []string{"node-1"},
							},
						},
					},
				},
			},
		},
	}

	fakeClient := cr_fake.NewSimpleClientset(cr)
	crState := NewCapacityRequestState(fakeClient)
	crState.Update([]*cr_types.CapacityRequest{cr})

	pod, found := crState.CapacityRequestToPod(cr)
	assert.True(t, found, "Expected to find synthesized pod for CR")
	assert.Empty(t, pod.Spec.NodeName, "Expected NodeName to be cleared")
	assert.Nil(t, pod.Spec.Affinity, "Expected single-node hostname affinity to be sanitized")
}
