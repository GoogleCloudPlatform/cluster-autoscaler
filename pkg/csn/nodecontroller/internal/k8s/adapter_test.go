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

package k8s

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/test"
)

func TestNewClientAdapter(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	adapter := NewClientAdapter(fakeClient)
	assert.NotNil(t, adapter)
	assert.Equal(t, fakeClient, adapter.clientSet)
}

func TestIsSuspensionBlocked(t *testing.T) {
	nodeName := "test-node"

	daemonSetPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ds-pod",
			OwnerReferences: []metav1.OwnerReference{
				{
					Kind:       "DaemonSet",
					Controller: new(bool),
				},
			},
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}
	*daemonSetPod.ObjectMeta.OwnerReferences[0].Controller = true

	normalPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "normal-pod",
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}

	succeededPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "succeeded-pod",
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
		},
		Status: v1.PodStatus{
			Phase: v1.PodSucceeded,
		},
	}

	tests := []struct {
		name           string
		pods           []*v1.Pod
		clientError    error
		expectedResult bool
		expectedError  bool
	}{
		{
			name:           "no_pods",
			pods:           []*v1.Pod{},
			expectedResult: false,
		},
		{
			name:           "only_daemonset_pods",
			pods:           []*v1.Pod{daemonSetPod},
			expectedResult: false,
		},
		{
			name:           "only_succeeded_pods",
			pods:           []*v1.Pod{succeededPod},
			expectedResult: false,
		},
		{
			name:           "normal_pod_present",
			pods:           []*v1.Pod{normalPod},
			expectedResult: true,
		},
		{
			name:           "mixed_pods",
			pods:           []*v1.Pod{daemonSetPod, succeededPod, normalPod},
			expectedResult: true,
		},
		{
			name:          "list_error",
			clientError:   fmt.Errorf("list error"),
			expectedError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			objects := make([]runtime.Object, 0, len(tc.pods))
			for _, p := range tc.pods {
				objects = append(objects, p)
			}
			fakeClient := fake.NewSimpleClientset(objects...)

			if tc.clientError != nil {
				fakeClient.PrependReactor("list", "pods", func(action core.Action) (handled bool, ret runtime.Object, err error) {
					return true, nil, tc.clientError
				})
			}

			adapter := NewClientAdapter(fakeClient)
			res, err := adapter.IsSuspensionBlocked(context.Background(), nodeName)

			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResult, res)
			}
		})
	}
}

func TestApplyNodePatch(t *testing.T) {
	nodeName := "test-node"

	tests := []struct {
		name          string
		initialNode   *v1.Node
		desiredState  csn.NodeState
		clientError   error
		expectPatch   bool
		expectedError bool
	}{
		{
			name:         "consumed_when_no_change_needed",
			initialNode:  test.CreateNode(nodeName),
			desiredState: csn.NodeStateConsumed,
			expectPatch:  false,
		},
		{
			name:         "change_to_suspended",
			initialNode:  test.CreateNode(nodeName),
			desiredState: csn.NodeStateSuspended,
			expectPatch:  true,
		},
		{
			name:         "change_to_consumed",
			initialNode:  test.CreateNode(nodeName, test.StateOpt(csn.NodeStateSuspended)),
			desiredState: csn.NodeStateConsumed,
			expectPatch:  true,
		},
		{
			name:          "patch_error",
			initialNode:   test.CreateNode(nodeName),
			desiredState:  csn.NodeStateSuspended,
			clientError:   fmt.Errorf("patch error"),
			expectPatch:   true, // Attempted
			expectedError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeClient := fake.NewSimpleClientset(tc.initialNode.DeepCopy())

			if tc.clientError != nil {
				fakeClient.PrependReactor("patch", "nodes", func(action core.Action) (handled bool, ret runtime.Object, err error) {
					return true, nil, tc.clientError
				})
			}

			adapter := NewClientAdapter(fakeClient)
			err := adapter.ApplyNodePatch(context.Background(), tc.initialNode.DeepCopy(), tc.desiredState)

			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			// Verify if Patch was called
			actions := fakeClient.Actions()
			patchCalled := false
			for _, action := range actions {
				if action.GetVerb() == "patch" && action.GetResource().Resource == "nodes" {
					patchCalled = true
					break
				}
			}
			assert.Equal(t, tc.expectPatch, patchCalled)

			n, err := fakeClient.CoreV1().Nodes().Get(t.Context(), tc.initialNode.Name, metav1.GetOptions{})
			assert.NoError(t, err)
			if tc.expectPatch && !tc.expectedError {
				assert.Equal(t, tc.desiredState, csn.ClassifyNode(n))
			}
			if tc.expectPatch && tc.expectedError {
				assert.Equal(t, csn.ClassifyNode(tc.initialNode), csn.ClassifyNode(n))
			}
		})
	}
}

func TestApplyNodeToBufferAssignmentPatch(t *testing.T) {
	nodeName := "test-node"
	buffer := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-buffer",
			Namespace: "test-namespace",
		},
	}

	tests := []struct {
		name                string
		initialNode         *v1.Node
		clientError         error
		expectedError       bool
		expectedPatchCalled bool
	}{
		{
			name:                "successful_assignment",
			initialNode:         test.CreateNode(nodeName),
			expectedPatchCalled: true,
		},
		{
			// patch shouldn't be called if there is no difference in node
			name: "already_assigned",
			initialNode: test.CreateNode(nodeName, func(n *v1.Node) {
				updated, err := csn.AssignNodeToBufferId(n, "test-namespace/test-buffer")
				assert.NoError(t, err)
				*n = *updated
			}),
		},
		{
			name:                "patch_error",
			initialNode:         test.CreateNode(nodeName),
			clientError:         fmt.Errorf("patch error"),
			expectedError:       true,
			expectedPatchCalled: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeClient := fake.NewSimpleClientset(tc.initialNode.DeepCopy())

			var patchCalled atomic.Bool

			fakeClient.PrependReactor("patch", "nodes", func(action core.Action) (handled bool, ret runtime.Object, err error) {
				patchCalled.Store(true)
				return tc.clientError != nil, nil, tc.clientError
			})

			adapter := NewClientAdapter(fakeClient)
			err := adapter.ApplyNodeToBufferAssignmentPatch(context.Background(), tc.initialNode.DeepCopy(), buffer)

			if tc.expectedError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedPatchCalled, patchCalled.Load())

			gotNode, err := fakeClient.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
			assert.NoError(t, err)

			expectedNode, err := csn.AssignNodeToBufferId(gotNode.DeepCopy(), "test-namespace/test-buffer")
			assert.NoError(t, err)

			// gotNode should have the buffer fully assigned
			assert.Equal(t, expectedNode, gotNode)
		})
	}
}

func TestApplyAdditionalSoftTaintsPatch(t *testing.T) {
	nodeName := "test-node"

	tests := []struct {
		name                string
		initialNode         *v1.Node
		taintCount          int
		clientError         error
		expectedError       bool
		expectedPatchCalled bool
		expectedTaintCount  int
	}{
		{
			name:                "successful_application_of_1_taint",
			initialNode:         test.CreateNode(nodeName),
			taintCount:          1,
			expectedPatchCalled: true,
			expectedTaintCount:  1,
		},
		{
			name:                "successful_application_of_2_taints",
			initialNode:         test.CreateNode(nodeName),
			taintCount:          2,
			expectedPatchCalled: true,
			expectedTaintCount:  2,
		},
		{
			name: "taints_already_exist",
			initialNode: test.CreateNode(nodeName, func(n *v1.Node) {
				err := csn.ApplySoftTaints(n, 1)
				assert.NoError(t, err)
			}),
			taintCount:          1,
			expectedPatchCalled: false,
			expectedTaintCount:  1,
		},
		{
			name: "taints_removed_when_0_applied",
			initialNode: test.CreateNode(nodeName, func(n *v1.Node) {
				err := csn.ApplySoftTaints(n, 5)
				assert.NoError(t, err)
			}),
			taintCount:          0,
			expectedPatchCalled: true,
			expectedTaintCount:  0,
		},
		{
			name:                "taints_count_0_no_op",
			initialNode:         test.CreateNode(nodeName),
			taintCount:          0,
			expectedPatchCalled: false,
			expectedTaintCount:  0,
		},
		{
			name:                "taints_count_negative_error",
			initialNode:         test.CreateNode(nodeName),
			taintCount:          -1,
			expectedError:       true,
			expectedPatchCalled: false,
			expectedTaintCount:  0,
		},
		{
			name:                "capped_taints_count",
			initialNode:         test.CreateNode(nodeName),
			taintCount:          maxTaintCount,
			expectedPatchCalled: true,
			expectedTaintCount:  16,
		},
		{
			name:                "nil_node_error",
			initialNode:         nil,
			taintCount:          5,
			expectedError:       true,
			expectedPatchCalled: false,
			expectedTaintCount:  0,
		},
		{
			name:                "patch_error",
			initialNode:         test.CreateNode(nodeName),
			taintCount:          1,
			clientError:         fmt.Errorf("patch error"),
			expectedError:       true,
			expectedPatchCalled: true,
			expectedTaintCount:  0, // Not updated in store
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeClient := fake.NewSimpleClientset()
			if tc.initialNode != nil {
				_, err := fakeClient.CoreV1().Nodes().Create(t.Context(), tc.initialNode, metav1.CreateOptions{})
				assert.NoError(t, err)
			}

			var patchCalled atomic.Bool

			fakeClient.PrependReactor("patch", "nodes", func(action core.Action) (handled bool, ret runtime.Object, err error) {
				patchCalled.Store(true)
				return tc.clientError != nil, nil, tc.clientError
			})

			adapter := NewClientAdapter(fakeClient)
			err := adapter.ApplyAdditionalSoftTaintsPatch(t.Context(), tc.initialNode.DeepCopy(), tc.taintCount)

			assert.Equal(t, tc.expectedPatchCalled, patchCalled.Load())
			if tc.expectedError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			gotNode, err := fakeClient.CoreV1().Nodes().Get(t.Context(), nodeName, metav1.GetOptions{})
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedTaintCount, csn.GetSoftTaintCount(gotNode))
		})
	}
}
