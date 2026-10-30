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

package conditions

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
)

func TestInvalidNameCheck(t *testing.T) {
	invalidNameCondition := &metav1.Condition{
		Type:    CrdMisconfiguredCondition,
		Status:  metav1.ConditionTrue,
		Reason:  InvalidComputeClassNameReason,
		Message: "ComputeClass name must be a valid label value of at most 63 characters because it is used as the value of the crd-1 label. Pods and nodes cannot use this ComputeClass. Recreate it with a shorter name.",
	}
	testCases := []struct {
		name    string
		crdName string
		want    *metav1.Condition
	}{
		{
			name:    "short name",
			crdName: "crd-object-1",
		},
		{
			name:    "63-character name",
			crdName: "cc-" + strings.Repeat("a", 60),
		},
		{
			name:    "64-character name",
			crdName: "cc-" + strings.Repeat("a", 61),
			want:    invalidNameCondition,
		},
		{
			name:    "253-character name",
			crdName: "cc-" + strings.Repeat("a", 250),
			want:    invalidNameCondition,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := crd.NewTestCrd(crd.WithLabel("crd-1"), crd.WithName(tc.crdName))
			checker := &invalidNameCheck{}

			got := checker.checkCrd(c, nil)

			if diff := cmp.Diff(tc.want, got, cmpopts.IgnoreFields(metav1.Condition{}, "LastTransitionTime")); diff != "" {
				t.Errorf("checkCrd() for name %q mismatch (-want +got):\n%s", tc.crdName, diff)
			}
		})
	}
}
