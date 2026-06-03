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

package operationtracker

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/client-go/kubernetes/fake"
	client_testing "k8s.io/client-go/testing"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	ekvmtypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
	"k8s.io/kubernetes/pkg/util/taints"
)

var (
	timeAdded               = time.Date(2024, time.December, 25, 8, 0, 0, 0, time.UTC)
	testNode                = test.BuildTestNode("node-1", 4000, 400*size.MiB)
	testNodeWithTaint, _, _ = addOrUpdateTaintWithTimeAdded(testNode, ekvmtypes.BPResizeTaint, timeAdded)
)

func TestResizeBalloonPod(t *testing.T) {
	var (
		bpCreationErr = fmt.Errorf("BP creation error")
		bpDeletionErr = fmt.Errorf("BP deletion error")
	)

	testCases := []struct {
		desc                  string
		node                  *v1.Node
		desiredSize           size.Allocatable
		deleteBalloonPodErr   error
		createBalloonPodErr   error
		expectedBalloonPodCpu resource.Quantity
		expectedBalloonPodMem resource.Quantity
		expectedErr           error
	}{
		{
			desc: "success",
			node: testNode,
			desiredSize: size.Allocatable{
				MilliCpus: 2000,
				KBytes:    200 * 1024,
			},
			expectedBalloonPodCpu: *resource.NewMilliQuantity(2000, resource.DecimalSI),
			expectedBalloonPodMem: *resource.NewQuantity(200*size.MiB, resource.DecimalSI),
			expectedErr:           nil,
		},
		{
			desc: "delete balloon pod err",
			node: testNode,
			desiredSize: size.Allocatable{
				MilliCpus: 2000,
				KBytes:    200 * 1024,
			},
			deleteBalloonPodErr: bpDeletionErr,
			expectedErr:         bpDeletionErr,
		},
		{
			desc: "create balloon pod err",
			node: testNode,
			desiredSize: size.Allocatable{
				MilliCpus: 2000,
				KBytes:    200 * 1024,
			},
			expectedBalloonPodCpu: *resource.NewMilliQuantity(2000, resource.DecimalSI),
			expectedBalloonPodMem: *resource.NewQuantity(200*size.MiB, resource.DecimalSI),
			createBalloonPodErr:   bpCreationErr,
			expectedErr:           bpCreationErr,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			fakeClient := &fake.Clientset{}
			mockBPController := &mockBalloonPodController{}

			mockBPController.On("DeleteAllBalloonPods", mock.Anything).Return(tc.deleteBalloonPodErr).Once()
			if tc.deleteBalloonPodErr == nil {
				mockBPController.On("CreateBalloonPod", mock.Anything, tc.expectedBalloonPodCpu, tc.expectedBalloonPodMem).Return(tc.createBalloonPodErr).Once()
			}

			balloonPodResizer := &defaultBalloonPodResizer{
				bPController: mockBPController,
				clientSet:    fakeClient,
			}

			err := balloonPodResizer.resizeBalloonPod(tc.node, tc.desiredSize)
			if tc.expectedErr == nil {
				assert.NoError(t, err)
			} else {
				// Error can be wrapped, thus check if final error contains a substring.
				assert.True(t, strings.Contains(err.Error(), tc.expectedErr.Error()))
			}
			mockBPController.AssertExpectations(t)
		})
	}
}

func TestAddTaint(t *testing.T) {
	var (
		testNode                = test.BuildTestNode("node-1", 4000, 400*size.MiB)
		testNodeWithTaint, _, _ = addOrUpdateTaintWithTimeAdded(testNode, ekvmtypes.BPResizeTaint, timeAdded)
		patch                   = "{\"spec\":{\"taints\":[{\"effect\":\"NoSchedule\",\"key\":\"node.gke.io/balloon-pod-resize\",\"timeAdded\":\"2024-12-25T08:00:00Z\",\"value\":\"true\"}]}}"
		patchErr                = fmt.Errorf("patch error")
	)

	testCases := []struct {
		desc                 string
		node                 *v1.Node
		patchErr             error
		expectedNode         *v1.Node
		expectedPatchesCount int
		expectedErr          error
	}{
		{
			desc:                 "success - taint added",
			node:                 testNode,
			expectedNode:         testNodeWithTaint,
			expectedPatchesCount: 1,
		},
		{
			desc:                 "success - taint exists",
			node:                 testNodeWithTaint,
			expectedNode:         testNodeWithTaint,
			expectedPatchesCount: 0,
		},
		{
			desc:                 "failure",
			node:                 testNode,
			patchErr:             patchErr,
			expectedPatchesCount: 1,
			expectedErr:          patchErr,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			fakeClient := fake.NewSimpleClientset(tc.node.DeepCopy())
			if tc.patchErr != nil {
				fakeClient.PrependReactor("patch", "nodes", func(action client_testing.Action) (bool, runtime.Object, error) {
					return true, nil, tc.patchErr
				})
			}

			balloonPodResizer := &defaultBalloonPodResizer{
				clientSet: fakeClient,
			}

			taintedNode, err := balloonPodResizer.addTaint(tc.node, timeAdded)
			if tc.expectedErr == nil {
				assert.NoError(t, err)
				if taintedNode == nil {
					t.Errorf("taintedNode is nil")
				} else if diff := cmp.Diff(tc.expectedNode.Spec.Taints, taintedNode.Spec.Taints); diff != "" { // assert.Equal func isn't suitable here, as it fails to compare time properly (e.g. UTC in test vs actual result in local time)
					t.Errorf("taints mismatch (-want +got):\n%s", diff)
				}
			} else {
				// Error can be wrapped, thus check if final error contains a substring.
				assert.True(t, strings.Contains(err.Error(), tc.expectedErr.Error()))
			}

			actions := fakeClient.Actions()
			patchesCount := 0
			for _, a := range actions {
				assert.Contains(t, []string{"patch", "get"}, a.GetVerb())
				pa, isPatch := a.(client_testing.PatchAction)
				if isPatch {
					p := pa.GetPatch()
					assert.Equal(t, patch, string(p))
					patchesCount++
				}
			}
			assert.Equal(t, patchesCount, tc.expectedPatchesCount)
		})
	}
}

func TestRemoveTaint(t *testing.T) {
	var (
		patch    = "{\"spec\":{\"taints\":null}}"
		patchErr = fmt.Errorf("patch error")
	)

	testCases := []struct {
		desc                 string
		node                 *v1.Node
		patchErr             error
		expectedNode         *v1.Node
		expectedPatchesCount int
		expectedErr          error
	}{
		{
			desc:                 "success - taint removed",
			node:                 testNodeWithTaint,
			expectedNode:         testNode,
			expectedPatchesCount: 1,
		},
		{
			desc:                 "success - taint doensn't exists",
			node:                 testNode,
			expectedNode:         testNode,
			expectedPatchesCount: 0,
		},
		{
			desc:                 "failure",
			node:                 testNodeWithTaint,
			patchErr:             patchErr,
			expectedPatchesCount: 1,
			expectedErr:          patchErr,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			fakeClient := fake.NewSimpleClientset(tc.node.DeepCopy())
			if tc.patchErr != nil {
				fakeClient.PrependReactor("patch", "nodes", func(action client_testing.Action) (bool, runtime.Object, error) {
					return true, nil, tc.patchErr
				})
			}

			balloonPodResizer := &defaultBalloonPodResizer{
				clientSet: fakeClient,
			}

			untaintedNode, err := balloonPodResizer.removeTaint(tc.node)
			if tc.expectedErr == nil {
				assert.NoError(t, err)
				if untaintedNode == nil {
					t.Errorf("untaintedNode is nil")
				} else if diff := cmp.Diff(tc.expectedNode.Spec.Taints, untaintedNode.Spec.Taints); diff != "" { // assert.Equal func isn't suitable here, as it fails on null vs empty.s
					t.Errorf("taints mismatch (-want +got):\n%s", diff)
				}
			} else {
				// Error can be wrapped, thus check if final error contains a substring.
				assert.True(t, strings.Contains(err.Error(), tc.expectedErr.Error()))
			}

			actions := fakeClient.Actions()
			patchesCount := 0
			for _, a := range actions {
				assert.Contains(t, []string{"patch", "get"}, a.GetVerb())
				pa, isPatch := a.(client_testing.PatchAction)
				if isPatch {
					p := pa.GetPatch()
					assert.Equal(t, patch, string(p))
					patchesCount++
				}
			}
			assert.Equal(t, patchesCount, tc.expectedPatchesCount)
		})
	}
}

func TestHasTaint(t *testing.T) {
	testCases := []struct {
		desc     string
		node     *v1.Node
		expected bool
	}{
		{
			desc:     "node without taint",
			node:     testNode,
			expected: false,
		},
		{
			desc:     "node with taint",
			node:     testNodeWithTaint,
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			balloonPodResizer := &defaultBalloonPodResizer{}
			got := balloonPodResizer.hasTaint(tc.node)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func addOrUpdateTaintWithTimeAdded(node *v1.Node, taint *v1.Taint, timeAdded time.Time) (*v1.Node, bool, error) {
	taintWithTimeAdded := taint.DeepCopy()
	taintWithTimeAdded.TimeAdded = &metav1.Time{Time: timeAdded}
	return taints.AddOrUpdateTaint(node, taintWithTimeAdded)
}

func TestAddTaintWithConcurrentUpdate(t *testing.T) {
	staleNode := test.BuildTestNode("node", 4000, 400*size.MiB)

	freshNode := staleNode.DeepCopy()
	concurrentTaint := v1.Taint{Key: "concurrent-update", Value: "true", Effect: v1.TaintEffectNoSchedule}
	freshNode.Spec.Taints = append(freshNode.Spec.Taints, concurrentTaint)

	fakeClient := fake.NewSimpleClientset(freshNode.DeepCopy())
	balloonPodResizer := &defaultBalloonPodResizer{clientSet: fakeClient}

	updatedNode, err := balloonPodResizer.addTaint(staleNode, timeAdded)

	assert.NoError(t, err)
	assert.True(t, balloonPodResizer.hasTaint(updatedNode), "Expected BPResize taint to be added")
	assert.Contains(t, updatedNode.Spec.Taints, concurrentTaint, "Expected concurrent taint to be preserved")
}
func TestRemoveTaintWithConcurrentUpdate(t *testing.T) {
	staleNode := test.BuildTestNode("node", 4000, 400*size.MiB)
	staleNode.Spec.Taints = append(staleNode.Spec.Taints, *ekvmtypes.BPResizeTaint)

	freshNode := staleNode.DeepCopy()
	concurrentTaint := v1.Taint{Key: "concurrent-update", Value: "true", Effect: v1.TaintEffectNoSchedule}
	freshNode.Spec.Taints = append(freshNode.Spec.Taints, concurrentTaint)

	fakeClient := fake.NewSimpleClientset(freshNode.DeepCopy())
	balloonPodResizer := &defaultBalloonPodResizer{clientSet: fakeClient}

	updatedNode, err := balloonPodResizer.removeTaint(staleNode)

	assert.NoError(t, err)
	assert.False(t, balloonPodResizer.hasTaint(updatedNode), "Expected BPResize taint to be removed")
	assert.Contains(t, updatedNode.Spec.Taints, concurrentTaint, "Expected concurrent taint to be preserved")
}
