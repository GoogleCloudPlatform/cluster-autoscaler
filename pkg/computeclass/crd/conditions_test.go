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

package crd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestAnyConditionsChanged(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-10 * time.Minute)
	maxAge := 5 * time.Minute

	testCases := []struct {
		name                  string
		existingConditions    []metav1.Condition
		newConditions         []metav1.Condition
		maxAge                *time.Duration
		wantConditionsChanged bool
	}{
		{
			name: "different lengths",
			existingConditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Foo", Message: "Bar"},
			},
			newConditions:         []metav1.Condition{},
			maxAge:                nil,
			wantConditionsChanged: true,
		},
		{
			name: "identical conditions with nil maxAge",
			existingConditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Foo", Message: "Bar", LastTransitionTime: metav1.NewTime(oldTime)},
			},
			newConditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Foo", Message: "Bar"},
			},
			maxAge:                nil,
			wantConditionsChanged: false,
		},
		{
			name: "identical conditions but older than maxAge",
			existingConditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Foo", Message: "Bar", LastTransitionTime: metav1.NewTime(oldTime)},
			},
			newConditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Foo", Message: "Bar"},
			},
			maxAge:                &maxAge,
			wantConditionsChanged: true,
		},
		{
			name: "identical conditions including last transition time and older than maxAge",
			existingConditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Foo", Message: "Bar", LastTransitionTime: metav1.NewTime(oldTime)},
			},
			newConditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Foo", Message: "Bar", LastTransitionTime: metav1.NewTime(oldTime)},
			},
			maxAge:                &maxAge,
			wantConditionsChanged: false,
		},
		{
			name: "identical conditions not older than maxAge",
			existingConditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Foo", Message: "Bar", LastTransitionTime: metav1.NewTime(now.Add(-2 * time.Minute))},
			},
			newConditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Foo", Message: "Bar"},
			},
			maxAge:                &maxAge,
			wantConditionsChanged: false,
		},
		{
			name: "status changed",
			existingConditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Foo", Message: "Bar", LastTransitionTime: metav1.NewTime(now)},
			},
			newConditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionFalse, Reason: "Foo", Message: "Bar"},
			},
			maxAge:                &maxAge,
			wantConditionsChanged: true,
		},
		{
			name: "zero LastTransitionTime with non-nil maxAge triggers change",
			existingConditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Foo", Message: "Bar"},
			},
			newConditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Foo", Message: "Bar", LastTransitionTime: metav1.NewTime(now)},
			},
			maxAge:                &maxAge,
			wantConditionsChanged: true,
		},
		{
			name: "zero LastTransitionTime with nil maxAge does not trigger change",
			existingConditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Foo", Message: "Bar"},
			},
			newConditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Foo", Message: "Bar", LastTransitionTime: metav1.NewTime(now)},
			},
			maxAge:                nil,
			wantConditionsChanged: false,
		},
		{
			name: "multiple conditions in different order",
			existingConditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Foo", Message: "Bar", LastTransitionTime: metav1.NewTime(now)},
				{Type: "Valid", Status: metav1.ConditionTrue, Reason: "Foo2", Message: "Bar2", LastTransitionTime: metav1.NewTime(now)},
			},
			newConditions: []metav1.Condition{
				{Type: "Valid", Status: metav1.ConditionTrue, Reason: "Foo2", Message: "Bar2"},
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Foo", Message: "Bar"},
			},
			maxAge:                &maxAge,
			wantConditionsChanged: false,
		},
		{
			name: "multiple conditions one changed",
			existingConditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Foo", Message: "Bar", LastTransitionTime: metav1.NewTime(now)},
				{Type: "Valid", Status: metav1.ConditionTrue, Reason: "Foo2", Message: "Bar2", LastTransitionTime: metav1.NewTime(now)},
			},
			newConditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Foo", Message: "Bar"},
				{Type: "Valid", Status: metav1.ConditionFalse, Reason: "Foo2", Message: "Bar2"},
			},
			maxAge:                &maxAge,
			wantConditionsChanged: true,
		},
		{
			name: "multiple conditions one older than maxAge",
			existingConditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Foo", Message: "Bar", LastTransitionTime: metav1.NewTime(now)},
				{Type: "Valid", Status: metav1.ConditionTrue, Reason: "Foo2", Message: "Bar2", LastTransitionTime: metav1.NewTime(oldTime)},
			},
			newConditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Foo", Message: "Bar"},
				{Type: "Valid", Status: metav1.ConditionTrue, Reason: "Foo2", Message: "Bar2"},
			},
			maxAge:                &maxAge,
			wantConditionsChanged: true,
		},
		{
			name: "observed generation and last transition time changes do not trigger update",
			existingConditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Foo", Message: "Bar", ObservedGeneration: 1, LastTransitionTime: metav1.NewTime(now)},
			},
			newConditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Foo", Message: "Bar", ObservedGeneration: 2, LastTransitionTime: metav1.NewTime(now.Add(time.Minute))},
			},
			maxAge:                &maxAge,
			wantConditionsChanged: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := AnyConditionsChanged(tc.existingConditions, tc.newConditions, tc.maxAge)
			assert.Equal(t, tc.wantConditionsChanged, got)
		})
	}
}
