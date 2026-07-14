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
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSetNodeAs(t *testing.T) {
	unrelatedTaint := apiv1.Taint{
		Key:    "unrelated-taint",
		Value:  "true",
		Effect: apiv1.TaintEffectNoSchedule,
	}

	conditionsSuspended := []apiv1.NodeCondition{
		{
			Type:    NodeConditionSuspended,
			Status:  apiv1.ConditionTrue,
			Message: NodeSuspendedMessage,
			Reason:  NodeConditionReason,
		},
	}

	conditionsConsumed := []apiv1.NodeCondition{
		{
			Type:    NodeConditionSuspended,
			Status:  apiv1.ConditionFalse,
			Message: NodeConsumedMessage,
			Reason:  NodeConditionReason,
		},
	}

	conditionsChillingFromSuspended := []apiv1.NodeCondition{
		{
			Type:    NodeConditionSuspended,
			Status:  apiv1.ConditionFalse,
			Message: NodeResumedMessage,
			Reason:  NodeConditionReason,
		},
	}

	testCases := []struct {
		description  string
		desiredState NodeState
		initialNode  *apiv1.Node
		expectedNode *apiv1.Node
		expectError  bool
	}{
		//
		// Desired State: Chilling
		//
		{
			description:  "Chilling from empty node",
			desiredState: NodeStateChilling,
			initialNode:  &apiv1.Node{},
			expectedNode: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						SoftWorkloadSeparationKey: SoftWorkloadSeparationValue,
					},
				},
			},
		},
		{
			description:  "Chilling from cordoned (not CSN hard-tainted) node",
			desiredState: NodeStateChilling,
			initialNode: &apiv1.Node{
				Spec: apiv1.NodeSpec{
					Unschedulable: true,
				},
			},
			expectedNode: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						SoftWorkloadSeparationKey: SoftWorkloadSeparationValue,
					},
				},
				Spec: apiv1.NodeSpec{
					Unschedulable: true,
				},
			},
		},
		{
			description:  "Chilling from a node with existing different CSN taint value",
			desiredState: NodeStateChilling,
			initialNode: &apiv1.Node{
				Spec: apiv1.NodeSpec{
					Taints: []apiv1.Taint{
						{Key: SuspendedTaintKey, Value: "other", Effect: apiv1.TaintEffectNoSchedule},
						unrelatedTaint,
					},
				},
			},
			expectedNode: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						SoftWorkloadSeparationKey: SoftWorkloadSeparationValue,
					},
				},
				Spec: apiv1.NodeSpec{
					Taints: []apiv1.Taint{unrelatedTaint},
				},
			},
		},
		{
			description:  "Chilling from Suspended",
			desiredState: NodeStateChilling,
			initialNode: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						SoftWorkloadSeparationKey: SoftWorkloadSeparationValue,
					},
				},
				Spec: apiv1.NodeSpec{
					Unschedulable: true,
					Taints:        []apiv1.Taint{SuspendedTaint},
				},
				Status: apiv1.NodeStatus{
					Conditions: conditionsSuspended,
				},
			},
			expectedNode: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						SoftWorkloadSeparationKey: SoftWorkloadSeparationValue,
					},
				},
				Spec: apiv1.NodeSpec{
					Unschedulable: false,
					Taints:        []apiv1.Taint{},
				},
				Status: apiv1.NodeStatus{
					Conditions: conditionsChillingFromSuspended,
				},
			},
		},
		//
		// Desired State: Suspended
		//
		{
			description:  "Suspended from an empty node",
			desiredState: NodeStateSuspended,
			initialNode:  &apiv1.Node{},
			expectedNode: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						SoftWorkloadSeparationKey: SoftWorkloadSeparationValue,
					},
				},
				Spec: apiv1.NodeSpec{
					Unschedulable: true,
					Taints:        []apiv1.Taint{SuspendedTaint},
				},
				Status: apiv1.NodeStatus{
					Conditions: conditionsSuspended,
				},
			},
		},
		{
			description:  "Suspended from a node with unrelated taints",
			desiredState: NodeStateSuspended,
			initialNode: &apiv1.Node{
				Spec: apiv1.NodeSpec{
					Taints: []apiv1.Taint{unrelatedTaint},
				},
			},
			expectedNode: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						SoftWorkloadSeparationKey: SoftWorkloadSeparationValue,
					},
				},
				Spec: apiv1.NodeSpec{
					Unschedulable: true,
					Taints:        []apiv1.Taint{unrelatedTaint, SuspendedTaint},
				},
				Status: apiv1.NodeStatus{
					Conditions: conditionsSuspended,
				},
			},
		},
		{
			description:  "Suspended from Suspended",
			desiredState: NodeStateSuspended,
			initialNode: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						SoftWorkloadSeparationKey: SoftWorkloadSeparationValue,
					},
				},
				Spec: apiv1.NodeSpec{
					Unschedulable: true,
					Taints:        []apiv1.Taint{SuspendedTaint},
				},
				Status: apiv1.NodeStatus{
					Conditions: conditionsSuspended,
				},
			},
			expectedNode: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						SoftWorkloadSeparationKey: SoftWorkloadSeparationValue,
					},
				},
				Spec: apiv1.NodeSpec{
					Unschedulable: true,
					Taints:        []apiv1.Taint{SuspendedTaint},
				},
				Status: apiv1.NodeStatus{
					Conditions: conditionsSuspended,
				},
			},
		},
		//
		// Desired State: Consumed
		//
		{
			description:  "Consumed from empty node",
			desiredState: NodeStateConsumed,
			initialNode:  &apiv1.Node{},
			expectedNode: &apiv1.Node{},
		},
		{
			description:  "Consumed from CSN tainted node",
			desiredState: NodeStateConsumed,
			initialNode: &apiv1.Node{
				Spec: apiv1.NodeSpec{
					Taints: []apiv1.Taint{SuspendedTaint, SoftWorkloadSeparationTaint},
				},
				Status: apiv1.NodeStatus{
					Conditions: conditionsSuspended,
				},
			},
			expectedNode: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{},
				Spec: apiv1.NodeSpec{
					Taints: []apiv1.Taint{},
				},
				Status: apiv1.NodeStatus{
					Conditions: conditionsConsumed,
				},
			},
		},
		{
			description:  "Consumed from cordoned (not CSN hard-tainted) node",
			desiredState: NodeStateConsumed,
			initialNode: &apiv1.Node{
				Spec: apiv1.NodeSpec{
					Unschedulable: true,
				},
			},
			expectedNode: &apiv1.Node{
				Spec: apiv1.NodeSpec{
					Unschedulable: true,
				},
			},
		},
		{
			description:  "Consumed from Chilling with unrelated taints",
			desiredState: NodeStateConsumed,
			initialNode: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						SoftWorkloadSeparationKey: SoftWorkloadSeparationValue,
					},
				},
				Spec: apiv1.NodeSpec{
					Taints: []apiv1.Taint{unrelatedTaint},
				},
			},
			expectedNode: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
				Spec: apiv1.NodeSpec{
					Taints: []apiv1.Taint{unrelatedTaint},
				},
			},
		},
		{
			description:  "Consumed from Suspended",
			desiredState: NodeStateConsumed,
			initialNode: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						SoftWorkloadSeparationKey: SoftWorkloadSeparationValue,
					},
				},
				Spec: apiv1.NodeSpec{
					Unschedulable: true,
					Taints:        []apiv1.Taint{SuspendedTaint},
				},
				Status: apiv1.NodeStatus{
					Conditions: conditionsSuspended,
				},
			},
			expectedNode: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
				Spec: apiv1.NodeSpec{
					Unschedulable: false,
					Taints:        []apiv1.Taint{},
				},
				Status: apiv1.NodeStatus{
					Conditions: conditionsConsumed,
				},
			},
		},
		//
		// Desired State: Unknown
		//
		{
			description:  "Unknown desired state",
			desiredState: NodeStateUnknown,
			initialNode:  &apiv1.Node{},
			expectedNode: &apiv1.Node{},
			expectError:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// Deep copy the node to avoid modifications between test cases
			initialNodeCopy := tc.initialNode.DeepCopy()
			resultNode, err := SetNodeAs(initialNodeCopy, tc.desiredState)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				now := metav1.Now()
				// LastTransition time on condition may be different slightly, rectify that for equals test
				setCommonTime(true, now, tc.expectedNode)
				setCommonTime(false, now, resultNode)
				assert.Equal(t, tc.expectedNode, resultNode)
			}
		})
	}
}

func TestCordonNode(t *testing.T) {
	testCases := []struct {
		description string
		node        *apiv1.Node
	}{
		{
			description: "node is initially not cordoned",
			node:        &apiv1.Node{},
		},
		{
			description: "node is initially cordoned",
			node: &apiv1.Node{
				Spec: apiv1.NodeSpec{
					Unschedulable: true,
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			cordonNode(tc.node)
			assert.Equal(t, true, tc.node.Spec.Unschedulable)
		})
	}
}

func TestUncordonNode(t *testing.T) {
	testCases := []struct {
		description string
		node        *apiv1.Node
	}{
		{
			description: "node is initially not cordoned",
			node:        &apiv1.Node{},
		},
		{
			description: "node is initially cordoned",
			node: &apiv1.Node{
				Spec: apiv1.NodeSpec{
					Unschedulable: true,
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			uncordonNode(tc.node)
			assert.Equal(t, false, tc.node.Spec.Unschedulable)
		})
	}
}

func TestIsCSNNode(t *testing.T) {
	testCases := []struct {
		description string
		node        *apiv1.Node
		expected    bool
	}{
		{
			description: "Node with CSN label",
			node: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						SoftWorkloadSeparationKey: SoftWorkloadSeparationValue,
					},
				},
			},
			expected: true,
		},
		{
			description: "Node without CSN label",
			node: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"some-other-label": "true",
					},
				},
			},
			expected: false,
		},
		{
			description: "Node with no labels",
			node:        &apiv1.Node{},
			expected:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			assert.Equal(t, tc.expected, IsCSNNode(tc.node))
		})
	}
}

func TestClassifyNode(t *testing.T) {
	testCases := []struct {
		description string
		node        *apiv1.Node
		expected    NodeState
	}{
		{
			description: "Consumed node (no CSN label)",
			node:        &apiv1.Node{},
			expected:    NodeStateConsumed,
		},
		{
			description: "Suspended node (CSN label and taint)",
			node: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						SoftWorkloadSeparationKey: SoftWorkloadSeparationValue,
					},
				},
				Spec: apiv1.NodeSpec{
					Taints: []apiv1.Taint{SuspendedTaint},
				},
			},
			expected: NodeStateSuspended,
		},
		{
			description: "Chilling node (CSN label, no taint)",
			node: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						SoftWorkloadSeparationKey: SoftWorkloadSeparationValue,
					},
				},
			},
			expected: NodeStateChilling,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			assert.Equal(t, tc.expected, ClassifyNode(tc.node))
		})
	}
}

func TestConsumptionRemovesCSNFields(t *testing.T) {
	tests := []struct {
		name           string
		initialNode    *apiv1.Node
		withAddedField func(*testing.T, *apiv1.Node) *apiv1.Node
		expectError    bool
	}{
		{
			name:        "buffer_assignment_removed",
			initialNode: &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{}}},
			withAddedField: func(t *testing.T, node *apiv1.Node) *apiv1.Node {
				assignedNode, err := AssignNodeToBufferId(node, "default/buffer1")
				assert.NoError(t, err)
				return assignedNode
			},
		},
		{
			name:        "soft_taints_removed",
			initialNode: &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{}}},
			withAddedField: func(t *testing.T, node *apiv1.Node) *apiv1.Node {
				c := node.DeepCopy()
				err := ApplySoftTaints(c, 5)
				assert.NoError(t, err)
				return c
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			suspendedNode, err := SetNodeAs(tc.initialNode, NodeStateSuspended)
			assert.NoError(t, err)
			assert.Equal(t, NodeStateSuspended, ClassifyNode(suspendedNode))

			changedNode := tc.withAddedField(t, suspendedNode)
			assert.Equal(t, NodeStateSuspended, ClassifyNode(changedNode))

			// Changed and suspended node should have values different from a node that
			// is only suspended.
			assert.NotEqual(t, changedNode, suspendedNode)

			consumedUnchangedNode, err := SetNodeAs(suspendedNode.DeepCopy(), NodeStateConsumed)
			assert.NoError(t, err)
			assert.Equal(t, NodeStateConsumed, ClassifyNode(consumedUnchangedNode))

			consumedChangedNode, err := SetNodeAs(changedNode.DeepCopy(), NodeStateConsumed)
			assert.NoError(t, err)
			assert.Equal(t, NodeStateConsumed, ClassifyNode(consumedChangedNode))

			// Consumption should get rid of any differences between changed
			// and unchanged nodes, apart from the last transition time on Suspended condition.
			setCommonTime(true, metav1.Now(), consumedUnchangedNode, consumedChangedNode)
			assert.Equal(t, consumedUnchangedNode, consumedChangedNode)
		})
	}
}

func TestAssignNodeToBuffer(t *testing.T) {
	testCases := []struct {
		description         string
		node                *apiv1.Node
		buffer              string
		expectError         bool
		expectedAnnotations map[string]string
	}{
		{
			description: "Nil node",
			node:        nil,
			buffer:      "some-buffer",
			expectError: true,
		},
		{
			description: "Nil buffer",
			node:        &apiv1.Node{},
			buffer:      "",
			expectError: true,
		},
		{
			description: "Valid assignment",
			node:        &apiv1.Node{},
			buffer:      "default/buffer1",
			expectError: false,
			expectedAnnotations: map[string]string{
				BufferAssignmentKey: "default/buffer1",
			},
		},
		{
			description: "Update existing assignment",
			node: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						BufferAssignmentKey: "old/value",
					},
				},
			},
			buffer:      "ns2/buffer2",
			expectError: false,
			expectedAnnotations: map[string]string{
				BufferAssignmentKey: "ns2/buffer2",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			gotNode, err := AssignNodeToBufferId(tc.node, tc.buffer)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if gotNode == nil {
				return
			}

			for annKey, annValue := range tc.expectedAnnotations {
				val, ok := gotNode.Annotations[annKey]
				assert.True(t, ok)
				assert.Equal(t, annValue, val)
			}
			assert.Equal(t, len(tc.expectedAnnotations), len(gotNode.Annotations))
			assert.Equal(t, tc.buffer, GetBufferIdFromNode(gotNode))
		})
	}
}

func TestIsAssignedNodeToBuffer(t *testing.T) {
	testCases := []struct {
		desc           string
		node           *apiv1.Node
		bufferId       string
		assignBufferId string
		expected       bool
	}{
		{
			desc:     "Nil node",
			node:     nil,
			bufferId: "ns/buffer",
			expected: false,
		},
		{
			desc:     "Node without annotations",
			node:     &apiv1.Node{},
			bufferId: "ns/buffer",
			expected: false,
		},
		{
			desc:           "Node assigned to given buffer",
			node:           &apiv1.Node{},
			assignBufferId: "ns/buffer",
			bufferId:       "ns/buffer",
			expected:       true,
		},
		{
			desc:           "Node assigned to different buffer",
			node:           &apiv1.Node{},
			assignBufferId: "ns/different-buffer",
			bufferId:       "ns/buffer",
			expected:       false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			if tc.assignBufferId != "" {
				var err error
				tc.node, err = AssignNodeToBufferId(tc.node, tc.assignBufferId)
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.expected, IsAssignedToBuffer(tc.node, tc.bufferId))
		})
	}
}

func TestRemoveBufferAssignment(t *testing.T) {
	testCases := []struct {
		description         string
		node                *apiv1.Node
		expectedAnnotations map[string]string
	}{
		{
			description: "Nil node",
			node:        nil,
		},
		{
			description: "Node with assignment",
			node: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						BufferAssignmentKey: "ns/buffer",
						"other-label":       "value",
					},
				},
			},
			expectedAnnotations: map[string]string{
				"other-label": "value",
			},
		},
		{
			description: "Node without assignment",
			node: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"other-label": "value",
					},
				},
			},
			expectedAnnotations: map[string]string{
				"other-label": "value",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			RemoveBufferAssignment(tc.node)
			if tc.node == nil {
				return
			}

			for annKey, annValue := range tc.expectedAnnotations {
				val, ok := tc.node.Annotations[annKey]
				assert.True(t, ok)
				assert.Equal(t, annValue, val)
			}
			assert.Equal(t, len(tc.expectedAnnotations), len(tc.node.Annotations))
		})
	}
}

func TestGetBufferIdFromNode(t *testing.T) {
	testCases := []struct {
		description string
		node        *apiv1.Node
		expected    string
	}{
		{
			description: "Nil node",
			node:        nil,
			expected:    "",
		},
		{
			description: "Node with no annotations",
			node:        &apiv1.Node{},
			expected:    "",
		},
		{
			description: "Node with unrelated annotations",
			node: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"other-annotation": "value",
					},
				},
			},
			expected: "",
		},
		{
			description: "Node with buffer assignment annotation",
			node: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						BufferAssignmentKey: "ns/buffer",
					},
				},
			},
			expected: "ns/buffer",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			assert.Equal(t, tc.expected, GetBufferIdFromNode(tc.node))
		})
	}
}

func setCommonTime(force bool, now metav1.Time, nodes ...*apiv1.Node) {
	setSuspendedTime := func(node *apiv1.Node, now metav1.Time) {
		unsetTime := metav1.Time{}
		for i, cond := range node.Status.Conditions {
			if cond.Type != NodeConditionSuspended {
				continue
			}
			if force || cond.LastTransitionTime != unsetTime {
				node.Status.Conditions[i].LastTransitionTime = now
			}
			if force || cond.LastHeartbeatTime != unsetTime {
				node.Status.Conditions[i].LastHeartbeatTime = now
				break
			}
		}
		for i := range node.Spec.Taints {
			if node.Spec.Taints[i].Key == SuspendedTaintKey {
				node.Spec.Taints[i].TimeAdded = &now
			}
		}
	}
	for _, node := range nodes {
		setSuspendedTime(node, now)
	}
}

func TestSetNodeAs_SetsTimeAddedOnSuspendedTaint(t *testing.T) {
	// In synctest, the clock starts at 2000-01-01 00:00:00 UTC.
	synctestStartTime := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	past := metav1.NewTime(time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC))

	testCases := []struct {
		description  string
		initialNode  *apiv1.Node
		delay        time.Duration
		expectedTime metav1.Time
	}{
		{
			description:  "Empty node transition to Suspended",
			initialNode:  &apiv1.Node{},
			delay:        0,
			expectedTime: metav1.NewTime(synctestStartTime),
		},
		{
			description:  "Chilling node transition to Suspended after delay",
			initialNode:  &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{SoftWorkloadSeparationKey: SoftWorkloadSeparationValue}}},
			delay:        10 * time.Minute,
			expectedTime: metav1.NewTime(synctestStartTime.Add(10 * time.Minute)),
		},
		{
			description:  "Node with unrelated taint transition to Suspended after delay",
			initialNode:  &apiv1.Node{Spec: apiv1.NodeSpec{Taints: []apiv1.Taint{{Key: "unrelated", Value: "foo", Effect: apiv1.TaintEffectNoSchedule}}}},
			delay:        20 * time.Minute,
			expectedTime: metav1.NewTime(synctestStartTime.Add(20 * time.Minute)),
		},
		{
			description: "Node already has SuspendedTaint with TimeAdded, should be preserved",
			initialNode: &apiv1.Node{
				Spec: apiv1.NodeSpec{
					Taints: []apiv1.Taint{
						{
							Key:       SuspendedTaintKey,
							Value:     SuspendedTaintValue,
							Effect:    apiv1.TaintEffectNoSchedule,
							TimeAdded: &past,
						},
					},
				},
			},
			delay:        30 * time.Minute,
			expectedTime: past,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				if tc.delay > 0 {
					time.Sleep(tc.delay)
				}

				resultNode, err := SetNodeAs(tc.initialNode.DeepCopy(), NodeStateSuspended)
				assert.NoError(t, err)

				var suspendedTaint *apiv1.Taint
				for i := range resultNode.Spec.Taints {
					if resultNode.Spec.Taints[i].Key == SuspendedTaintKey {
						suspendedTaint = &resultNode.Spec.Taints[i]
						break
					}
				}

				assert.NotNil(t, suspendedTaint, "Suspended taint should be present")
				assert.NotNil(t, suspendedTaint.TimeAdded, "TimeAdded should be set on the suspended taint")
				assert.Equal(t, tc.expectedTime.UTC(), suspendedTaint.TimeAdded.UTC(), "TimeAdded should match expected virtual time")
			})
		})
	}
}

func TestIsSuspendedNode(t *testing.T) {
	testCases := []struct {
		description string
		node        *apiv1.Node
		expected    bool
	}{
		{
			description: "Node with no conditions",
			node:        &apiv1.Node{},
			expected:    false,
		},
		{
			description: "Node with unrelated conditions",
			node: &apiv1.Node{
				Status: apiv1.NodeStatus{
					Conditions: []apiv1.NodeCondition{
						{
							Type:   apiv1.NodeReady,
							Status: apiv1.ConditionTrue,
						},
					},
				},
			},
			expected: false,
		},
		{
			description: "Node with Suspended condition True",
			node: &apiv1.Node{
				Status: apiv1.NodeStatus{
					Conditions: []apiv1.NodeCondition{
						{
							Type:   NodeConditionSuspended,
							Status: apiv1.ConditionTrue,
						},
					},
				},
			},
			expected: true,
		},
		{
			description: "Node with Suspended condition False",
			node: &apiv1.Node{
				Status: apiv1.NodeStatus{
					Conditions: []apiv1.NodeCondition{
						{
							Type:   NodeConditionSuspended,
							Status: apiv1.ConditionFalse,
						},
					},
				},
			},
			expected: false,
		},
		{
			description: "Node with Suspended condition Unknown",
			node: &apiv1.Node{
				Status: apiv1.NodeStatus{
					Conditions: []apiv1.NodeCondition{
						{
							Type:   NodeConditionSuspended,
							Status: apiv1.ConditionUnknown,
						},
					},
				},
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			assert.Equal(t, tc.expected, IsSuspendedNode(tc.node))
		})
	}
}
