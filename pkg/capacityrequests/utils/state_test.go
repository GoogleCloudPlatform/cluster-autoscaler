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
