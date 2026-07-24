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
	"time"

	k8sapimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AnyConditionsChanged returns true if the newConditions slice differs from existingConditions
// or if any existing condition is older than maxConditionAge (when maxConditionAge is non-nil).
func AnyConditionsChanged(existingConditions, newConditions []metav1.Condition, maxConditionAge *time.Duration) bool {
	if len(existingConditions) != len(newConditions) {
		return true
	}
	for _, c := range newConditions {
		existing := k8sapimeta.FindStatusCondition(existingConditions, c.Type)
		if existing == nil || existing.Status != c.Status || existing.Reason != c.Reason || existing.Message != c.Message {
			return true
		}
		if maxConditionAge != nil && existing.LastTransitionTime != c.LastTransitionTime && time.Since(existing.LastTransitionTime.Time) >= *maxConditionAge {
			return true
		}
	}
	return false
}
