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
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
)

const AdditionalSoftTaintKeyPrefix = "buffer.gke.io/standby-capacity-soft-taint-"

// GetSoftTaintCount returns the number of additional soft taints on the node.
func GetSoftTaintCount(node *v1.Node) int {
	if node == nil {
		return 0
	}
	count := 0
	for _, taint := range node.Spec.Taints {
		if strings.HasPrefix(taint.Key, AdditionalSoftTaintKeyPrefix) {
			count++
		}
	}
	return count
}

// ApplySoftTaints returns a copy of the node with the specified number of soft taints.
func ApplySoftTaints(node *v1.Node, count int) error {
	if node == nil {
		return fmt.Errorf("cannot apply soft taints to nil node")
	}
	if count < 0 {
		return fmt.Errorf("soft taint count should be at least 0, got %d", count)
	}
	if count == 0 && len(node.Spec.Taints) == 0 {
		// soft taints cannot exist when there are no taints
		return nil
	}
	// Start with enough capacity for existing taints + new soft taints.
	newTaints := make([]v1.Taint, 0, len(node.Spec.Taints)+count)
	for _, taint := range node.Spec.Taints {
		if !strings.HasPrefix(taint.Key, AdditionalSoftTaintKeyPrefix) {
			newTaints = append(newTaints, taint)
		}
	}
	for i := 1; i <= count; i++ {
		newTaints = append(newTaints, v1.Taint{
			Key:    fmt.Sprintf("%s%d", AdditionalSoftTaintKeyPrefix, i),
			Effect: v1.TaintEffectPreferNoSchedule,
		})
	}
	node.Spec.Taints = newTaints
	return nil
}
