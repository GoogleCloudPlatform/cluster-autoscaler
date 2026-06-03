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
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/util/taints"
)

func TestGetSoftTaintCount(t *testing.T) {
	tests := []struct {
		name     string
		node     *v1.Node
		expected int
	}{
		{
			name:     "nil_node",
			node:     nil,
			expected: 0,
		},
		{
			name: "no_taints",
			node: &v1.Node{
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{},
				},
			},
			expected: 0,
		},
		{
			name: "unrelated_taints",
			node: &v1.Node{
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{Key: "other-taint", Effect: v1.TaintEffectNoSchedule},
					},
				},
			},
			expected: 0,
		},
		{
			name: "single_soft_taint",
			node: &v1.Node{
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						softTaint(1),
					},
				},
			},
			expected: 1,
		},
		{
			name: "multiple_soft_taints",
			node: &v1.Node{
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						softTaint(1),
						softTaint(2),
						{Key: "other-taint", Effect: v1.TaintEffectNoSchedule},
					},
				},
			},
			expected: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, GetSoftTaintCount(tc.node))
		})
	}
}

func TestApplySoftTaints(t *testing.T) {
	otherTaint := v1.Taint{Key: "other-taint", Effect: v1.TaintEffectNoSchedule}
	tests := []struct {
		name                    string
		node                    *v1.Node
		count                   int
		expectedCount           int
		expectError             bool
		expectedUnrelatedTaints []v1.Taint
	}{
		{
			name: "apply_zero_taints_when_some_exist",
			node: &v1.Node{
				Spec: v1.NodeSpec{Taints: []v1.Taint{
					softTaint(1),
					softTaint(2),
					softTaint(3),
					softTaint(4),
					softTaint(5),
					otherTaint,
				}},
			},
			count:         0,
			expectedCount: 0,
			expectedUnrelatedTaints: []v1.Taint{
				otherTaint,
			},
		},
		{
			name:          "apply_zero_taints_when_there_is_node",
			node:          &v1.Node{},
			count:         0,
			expectedCount: 0,
			expectError:   false,
		},
		{
			name: "apply_negative_taints",
			node: &v1.Node{
				Spec: v1.NodeSpec{Taints: []v1.Taint{}},
			},
			count:         -1,
			expectedCount: 0,
			expectError:   true,
		},
		{
			name:          "error_when_nil_node",
			node:          nil,
			count:         5,
			expectedCount: 0,
			expectError:   true,
		},
		{
			name: "apply_one_taint_to_empty",
			node: &v1.Node{
				Spec: v1.NodeSpec{Taints: []v1.Taint{}},
			},
			count:         1,
			expectedCount: 1,
		},
		{
			name: "apply_multiple_taints_to_empty",
			node: &v1.Node{
				Spec: v1.NodeSpec{Taints: []v1.Taint{}},
			},
			count:         3,
			expectedCount: 3,
		},
		{
			name: "apply_taints_idempotent",
			node: &v1.Node{
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						softTaint(1),
					},
				},
			},
			count:         1,
			expectedCount: 1, // Should not duplicate
		},
		{
			name: "apply_more_taints_to_existing",
			node: &v1.Node{
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						softTaint(1),
					},
				},
			},
			count:         2,
			expectedCount: 2, // Should add taint-2
		},
		{
			name: "preserves_existing_unrelated_taints",
			node: &v1.Node{
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						otherTaint,
					},
				},
			},
			count:         1,
			expectedCount: 1, // 1 soft taint (plus 1 other)
			expectedUnrelatedTaints: []v1.Taint{
				otherTaint,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ApplySoftTaints(tc.node, tc.count)

			if tc.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedCount, GetSoftTaintCount(tc.node))

			// Verify specific keys exist
			for i := 1; i <= tc.count; i++ {
				expected := softTaint(i)
				assert.True(t, taints.TaintExists(tc.node.Spec.Taints, &expected))
			}

			for _, taint := range tc.expectedUnrelatedTaints {
				assert.True(t, taints.TaintExists(tc.node.Spec.Taints, &taint))
			}
			assert.Len(t, tc.node.Spec.Taints, len(tc.expectedUnrelatedTaints)+tc.expectedCount)
		})
	}
}

func softTaint(n int) v1.Taint {
	return v1.Taint{Key: fmt.Sprintf("%s%d", AdditionalSoftTaintKeyPrefix, n), Effect: v1.TaintEffectPreferNoSchedule}
}
