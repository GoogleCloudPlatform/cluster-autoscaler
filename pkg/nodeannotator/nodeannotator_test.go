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

package nodeannotator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestProcessNodeWithFakeClient(t *testing.T) {
	testNodeName := "test-node-1"
	defaultCtx := context.Background()
	timeoutDuration := 50 * time.Millisecond
	timeoutCtx, cancelTimeout := context.WithTimeout(defaultCtx, timeoutDuration)
	defer cancelTimeout()

	conflictErr := k8serrors.NewConflict(schema.GroupResource{Group: "", Resource: "nodes"}, testNodeName, errors.New("object modified"))
	genericErr := errors.New("some generic API error")
	apiTimeoutErr := k8serrors.NewTimeoutError(fmt.Sprintf("request timed out after %s", timeoutDuration), 0)

	testCases := []struct {
		name                     string
		initialObjects           []runtime.Object
		plugins                  []Plugin
		reactorGenerator         func(t *testing.T, tracker k8stesting.ObjectTracker) ReactionFunc
		expectErr                bool
		expectedErrMsg           string
		testCtx                  context.Context
		verifyActionsFunc        func(t *testing.T, actions []k8stesting.Action)
		expectedFinalAnnotations map[string]string
	}{
		{
			name:           "No plugins, no annotations",
			initialObjects: []runtime.Object{createNode(testNodeName, nil)},
			plugins:        []Plugin{},
			reactorGenerator: func(t *testing.T, tracker k8stesting.ObjectTracker) ReactionFunc {
				return noPatchReactor(t)
			},
			expectErr: false,
			testCtx:   defaultCtx,
			verifyActionsFunc: func(t *testing.T, actions []k8stesting.Action) {
				assert.Empty(t, actions, "Expected no client actions")
			},
			expectedFinalAnnotations: nil,
		},
		{
			name:           "Plugin adds one new annotation, patch succeeds",
			initialObjects: []runtime.Object{createNode(testNodeName, nil)},
			plugins: []Plugin{&mockPlugin{nameFunc: func() string { return "P1" }, getAnnotationFunc: func(node *apiv1.Node) (map[string]string, error) {
				return map[string]string{"new.key/a": "valueA"}, nil
			}}},
			reactorGenerator: func(t *testing.T, tracker k8stesting.ObjectTracker) ReactionFunc {
				return successfulPatchReactor(t, testNodeName, map[string]string{"new.key/a": "valueA"})
			},
			expectErr: false,
			testCtx:   defaultCtx,
			verifyActionsFunc: func(t *testing.T, actions []k8stesting.Action) {
				assert.Len(t, actions, 1)
				assert.Equal(t, "patch", actions[0].GetVerb())
			},
			expectedFinalAnnotations: map[string]string{"new.key/a": "valueA"},
		},
		{
			name:           "Plugin suggests annotation that already exists with same value",
			initialObjects: []runtime.Object{createNode(testNodeName, map[string]string{"existing/a": "valueA"})},
			plugins: []Plugin{&mockPlugin{nameFunc: func() string { return "P1" }, getAnnotationFunc: func(node *apiv1.Node) (map[string]string, error) {
				return map[string]string{"existing/a": "valueA"}, nil
			}}},
			reactorGenerator: func(t *testing.T, tracker k8stesting.ObjectTracker) ReactionFunc {
				return noPatchReactor(t)
			},
			expectErr:                false,
			testCtx:                  defaultCtx,
			verifyActionsFunc:        func(t *testing.T, actions []k8stesting.Action) { assert.Empty(t, actions) },
			expectedFinalAnnotations: map[string]string{"existing/a": "valueA"},
		},
		{
			name:           "Plugin updates existing annotation value",
			initialObjects: []runtime.Object{createNode(testNodeName, map[string]string{"existing.key/a": "oldValueA", "other.key/b": "valueB"})},
			plugins: []Plugin{&mockPlugin{nameFunc: func() string { return "P_Update" }, getAnnotationFunc: func(node *apiv1.Node) (map[string]string, error) {
				return map[string]string{"existing.key/a": "newValueA"}, nil
			}}},
			reactorGenerator: func(t *testing.T, tracker k8stesting.ObjectTracker) ReactionFunc {
				return successfulPatchReactor(t, testNodeName, map[string]string{"existing.key/a": "newValueA"})
			},
			expectErr: false,
			testCtx:   defaultCtx,
			verifyActionsFunc: func(t *testing.T, actions []k8stesting.Action) {
				assert.Len(t, actions, 1)
				assert.Equal(t, "patch", actions[0].GetVerb())
			},
			expectedFinalAnnotations: map[string]string{"existing.key/a": "newValueA", "other.key/b": "valueB"},
		},
		{
			name:           "Multiple plugins, one new, one existing",
			initialObjects: []runtime.Object{createNode(testNodeName, map[string]string{"key/b": "valB"})},
			plugins: []Plugin{
				&mockPlugin{nameFunc: func() string { return "P_New" }, getAnnotationFunc: func(node *apiv1.Node) (map[string]string, error) { return map[string]string{"key/a": "valA"}, nil }},
				&mockPlugin{nameFunc: func() string { return "P_Existing" }, getAnnotationFunc: func(node *apiv1.Node) (map[string]string, error) { return map[string]string{"key/b": "valB"}, nil }},
			},
			reactorGenerator: func(t *testing.T, tracker k8stesting.ObjectTracker) ReactionFunc {
				// Expect patch containing only the new annotation
				return successfulPatchReactor(t, testNodeName, map[string]string{"key/a": "valA"})
			},
			expectErr: false,
			testCtx:   defaultCtx,
			verifyActionsFunc: func(t *testing.T, actions []k8stesting.Action) {
				assert.Len(t, actions, 1)
				assert.Equal(t, "patch", actions[0].GetVerb())
			},
			expectedFinalAnnotations: map[string]string{"key/a": "valA", "key/b": "valB"},
		},
		{
			name:           "One plugin fails, one succeeds, patch occurs",
			initialObjects: []runtime.Object{createNode(testNodeName, nil)},
			plugins: []Plugin{
				&mockPlugin{nameFunc: func() string { return "P_Fail" }, getAnnotationFunc: func(node *apiv1.Node) (map[string]string, error) { return nil, errors.New("plugin failed") }},
				&mockPlugin{nameFunc: func() string { return "P_OK" }, getAnnotationFunc: func(node *apiv1.Node) (map[string]string, error) { return map[string]string{"key/ok": "valOK"}, nil }},
			},
			reactorGenerator: func(t *testing.T, tracker k8stesting.ObjectTracker) ReactionFunc {
				// Expect patch containing only the successful plugin's annotation
				return successfulPatchReactor(t, testNodeName, map[string]string{"key/ok": "valOK"})
			},
			expectErr: false,
			testCtx:   defaultCtx,
			verifyActionsFunc: func(t *testing.T, actions []k8stesting.Action) {
				assert.Len(t, actions, 1)
				assert.Equal(t, "patch", actions[0].GetVerb())
			},
			expectedFinalAnnotations: map[string]string{"key/ok": "valOK"},
		},
		{
			name:           "Patch fails with Conflict error",
			initialObjects: []runtime.Object{createNode(testNodeName, nil)},
			plugins: []Plugin{&mockPlugin{nameFunc: func() string { return "P1" }, getAnnotationFunc: func(node *apiv1.Node) (map[string]string, error) {
				return map[string]string{"new.key/a": "valueA"}, nil
			}}},
			reactorGenerator: func(t *testing.T, tracker k8stesting.ObjectTracker) ReactionFunc {
				return failingPatchReactor(t, testNodeName, conflictErr)
			},
			expectErr: false,
			testCtx:   defaultCtx,
			verifyActionsFunc: func(t *testing.T, actions []k8stesting.Action) {
				assert.Len(t, actions, 1)
				assert.Equal(t, "patch", actions[0].GetVerb())
			},
			expectedFinalAnnotations: nil,
		},
		{
			name:           "Patch fails with generic error",
			initialObjects: []runtime.Object{createNode(testNodeName, nil)},
			plugins: []Plugin{&mockPlugin{nameFunc: func() string { return "P1" }, getAnnotationFunc: func(node *apiv1.Node) (map[string]string, error) {
				return map[string]string{"new.key/a": "valueA"}, nil
			}}},
			reactorGenerator: func(t *testing.T, tracker k8stesting.ObjectTracker) ReactionFunc {
				// Expect patch failure with genericErr
				return failingPatchReactor(t, testNodeName, genericErr)
			},
			expectErr:      true,
			expectedErrMsg: "some generic API error",
			testCtx:        defaultCtx,
			verifyActionsFunc: func(t *testing.T, actions []k8stesting.Action) {
				assert.Len(t, actions, 1)
				assert.Equal(t, "patch", actions[0].GetVerb())
			},
			expectedFinalAnnotations: nil,
		},
		{
			name:           "Patch fails with timeout error",
			initialObjects: []runtime.Object{createNode(testNodeName, nil)},
			plugins: []Plugin{&mockPlugin{nameFunc: func() string { return "P1" }, getAnnotationFunc: func(node *apiv1.Node) (map[string]string, error) {
				return map[string]string{"new.key/a": "valueA"}, nil
			}}},
			reactorGenerator: func(t *testing.T, tracker k8stesting.ObjectTracker) ReactionFunc {
				return func(action k8stesting.Action, tracker k8stesting.ObjectTracker) (handled bool, ret runtime.Object, err error) {
					patchAction, ok := action.(k8stesting.PatchAction)
					if !ok || patchAction.GetResource().Resource != "nodes" {
						return false, nil, nil
					}
					select {
					case <-timeoutCtx.Done():
						if errors.Is(timeoutCtx.Err(), context.DeadlineExceeded) {
							return true, nil, apiTimeoutErr
						}
						return true, nil, fmt.Errorf("reactor context cancelled unexpectedly: %w", timeoutCtx.Err())
					case <-time.After(timeoutDuration + 20*time.Millisecond):
						assert.Fail(t, "Reactor context did not time out")
						return true, nil, errors.New("reactor timeout check failed")
					}
				}
			},
			expectErr:      true,
			expectedErrMsg: "Timeout: request timed out after",
			testCtx:        timeoutCtx,
			verifyActionsFunc: func(t *testing.T, actions []k8stesting.Action) {
				assert.Len(t, actions, 1)
				assert.Equal(t, "patch", actions[0].GetVerb())
			},
			expectedFinalAnnotations: nil,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			fakeClient := fake.NewSimpleClientset(tc.initialObjects...)
			tracker := fakeClient.Tracker()

			if tc.reactorGenerator != nil {
				reactorFunc := tc.reactorGenerator(t, tracker)
				fakeClient.PrependReactor("patch", "nodes", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
					return reactorFunc(action, tracker)
				})
			}

			na := &NodeAnnotator{kubeClient: fakeClient, plugins: tc.plugins, config: Config{}, nodeLister: nil}
			var initialNode *apiv1.Node
			for _, obj := range tc.initialObjects {
				if n, ok := obj.(*apiv1.Node); ok && n.Name == testNodeName {
					initialNode = n
					break
				}
			}
			if initialNode == nil {
				t.Fatalf("Test setup error: Initial node %q not found", testNodeName)
			}

			err := na.processNode(tc.testCtx, initialNode.DeepCopy())

			if tc.expectErr {
				assert.Error(t, err)
				if tc.expectedErrMsg != "" {
					assert.Contains(t, err.Error(), tc.expectedErrMsg)
					if strings.Contains(tc.name, "timeout") {
						assert.True(t, k8serrors.IsTimeout(err))
					}
				}
			} else {
				assert.NoError(t, err)
			}
			if tc.verifyActionsFunc != nil {
				tc.verifyActionsFunc(t, fakeClient.Actions())
			}
			finalNode, getErr := fakeClient.CoreV1().Nodes().Get(context.Background(), testNodeName, metav1.GetOptions{})
			assert.NoError(t, getErr)

			if getErr == nil {
				if len(tc.expectedFinalAnnotations) == 0 {
					assert.Empty(t, finalNode.Annotations)
				} else {
					assert.Equal(t, tc.expectedFinalAnnotations, finalNode.Annotations, "Final node annotations mismatch")
				}
			}
		})
	}
}

func TestRegisterPlugin(t *testing.T) {
	na := &NodeAnnotator{plugins: []Plugin{}}
	na.RegisterPlugin(nil)
	assert.Empty(t, na.plugins)
	p1 := &mockPlugin{nameFunc: func() string { return "P1" }}
	p2 := &mockPlugin{nameFunc: func() string { return "P2" }}
	na.RegisterPlugin(p1)
	na.RegisterPlugin(p2)
	assert.Len(t, na.plugins, 2)
	assert.Equal(t, p1, na.plugins[0])
	assert.Equal(t, p2, na.plugins[1])
}
