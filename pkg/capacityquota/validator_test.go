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

package capacityquota

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	cqv1beta1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacityquota/autoscaling.x-k8s.io/v1beta1"
)

func TestNoBlocklistedLabelsValidator(t *testing.T) {
	testCases := []struct {
		name     string
		selector *metav1.LabelSelector
		wantErr  bool
	}{
		{
			name:     "no_selector_valid",
			selector: nil,
			wantErr:  false,
		},
		{
			name:     "empty_selector_valid",
			selector: &metav1.LabelSelector{},
			wantErr:  false,
		},
		{
			name: "valid_selector",
			selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"valid-label": "true",
				},
			},
			wantErr: false,
		},
		{
			name: "match_labels_blocklisted_label",
			selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"valid-label":                      "true",
					"node.kubernetes.io/instance-type": "e2-standard-32",
				},
			},
			wantErr: true,
		},
		{
			name: "match_expressions_blocklisted_label",
			selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"valid-label": "true",
				},
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "node.kubernetes.io/instance-type",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{"e2-standard-32", "e2-standard-64"},
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			v := NewDefaultBlocklistedLabelsValidator()
			cq := &cqv1beta1.CapacityQuota{
				Spec: cqv1beta1.CapacityQuotaSpec{
					Selector: tc.selector,
				},
			}

			err := v.Validate(nil, cq)

			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Errorf("Validate() gotErr = %t, wantErr %t", gotErr, tc.wantErr)
			}
		})
	}
}
