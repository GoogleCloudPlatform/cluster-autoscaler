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
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	client_testing "k8s.io/client-go/testing"
	ek_errors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	ekvmtypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
	consistencyutil "k8s.io/kubernetes/pkg/controller/util/consistency"
	"k8s.io/kubernetes/pkg/util/taints"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
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

			balloonPodResizer := &defaultBalloonPodResizer{consistencyStore: consistencyutil.NewNoopConsistencyStore(),
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

func TestResizeBalloonPodInPlace(t *testing.T) {
	t.Parallel()

	desiredCpu := resource.MustParse("2000m")
	desiredMem := *resource.NewQuantity(200*size.MiB, resource.DecimalSI)
	baseNode := test.BuildTestNode("test-node", 4000, 400*size.MiB)
	desiredSize := size.Allocatable{MilliCpus: 2000, KBytes: 200 * 1024}

	errConflict := errors.New("apiserver conflict")
	testCases := []struct {
		desc       string
		node       *v1.Node
		setupMock  func(t *testing.T, m *mockBalloonPodController)
		wantErr    error
		wantErrMsg string
	}{
		{
			desc:       "node has no allocatable set",
			node:       &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-no-alloc"}},
			setupMock:  func(t *testing.T, m *mockBalloonPodController) {},
			wantErrMsg: fmt.Sprintf("cannot resize balloon pod for node %q with no allocatable set", "node-no-alloc"),
		},
		{
			desc: "successful in-place resize via controller",
			node: baseNode,
			setupMock: func(t *testing.T, m *mockBalloonPodController) {
				m.On("ResizeBalloonPodInPlace", baseNode, desiredCpu, desiredMem).Return(nil)
			},
			wantErr: nil,
		},
		{
			desc: "controller returns error during resize",
			node: baseNode,
			setupMock: func(t *testing.T, m *mockBalloonPodController) {
				m.On("ResizeBalloonPodInPlace", baseNode, desiredCpu, desiredMem).
					Return(errConflict)
			},
			wantErr: errConflict,
		},
		{
			desc: "controller returns NoActiveBalloonPodError",
			node: baseNode,
			setupMock: func(t *testing.T, m *mockBalloonPodController) {
				m.On("ResizeBalloonPodInPlace", baseNode, desiredCpu, desiredMem).
					Return(ek_errors.NoActiveBalloonPodError)
			},
			wantErr: ek_errors.NoActiveBalloonPodError,
		},
		{
			desc: "controller returns IncompatibleQoSError",
			node: baseNode,
			setupMock: func(t *testing.T, m *mockBalloonPodController) {
				m.On("ResizeBalloonPodInPlace", baseNode, desiredCpu, desiredMem).
					Return(ek_errors.IncompatibleQoSError)
			},
			wantErr: ek_errors.IncompatibleQoSError,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			m := &mockBalloonPodController{}
			tc.setupMock(t, m)

			resizer := &defaultBalloonPodResizer{bPController: m}
			err := resizer.resizeBalloonPodInPlace(tc.node, desiredSize)

			if tc.wantErrMsg != "" {
				assert.EqualError(t, err, tc.wantErrMsg)
			} else if tc.wantErr != nil {
				assert.ErrorIs(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}

			m.AssertExpectations(t)
		})
	}
}

func TestAddTaint(t *testing.T) {
	var (
		patch    = fmt.Sprintf("{\"spec\":{\"taints\":[{\"effect\":\"NoSchedule\",\"key\":\"%s\",\"timeAdded\":\"2024-12-25T08:00:00Z\",\"value\":\"true\"}]}}", ekvmtypes.BPResizeTaint.Key)
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

			balloonPodResizer := &defaultBalloonPodResizer{consistencyStore: consistencyutil.NewNoopConsistencyStore(),
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

			balloonPodResizer := &defaultBalloonPodResizer{consistencyStore: consistencyutil.NewNoopConsistencyStore(),
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
			balloonPodResizer := &defaultBalloonPodResizer{consistencyStore: consistencyutil.NewNoopConsistencyStore()}
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
	balloonPodResizer := &defaultBalloonPodResizer{consistencyStore: consistencyutil.NewNoopConsistencyStore(), clientSet: fakeClient}

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
	balloonPodResizer := &defaultBalloonPodResizer{consistencyStore: consistencyutil.NewNoopConsistencyStore(), clientSet: fakeClient}

	updatedNode, err := balloonPodResizer.removeTaint(staleNode)

	assert.NoError(t, err)
	assert.False(t, balloonPodResizer.hasTaint(updatedNode), "Expected BPResize taint to be removed")
	assert.Contains(t, updatedNode.Spec.Taints, concurrentTaint, "Expected concurrent taint to be preserved")
}

// fakeRVGetter implements LastSyncRVGetter for testing ConsistencyStore.
type fakeRVGetter struct {
	mu sync.Mutex
	rv string
}

func (f *fakeRVGetter) setRV(rv string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rv = rv
}

func (f *fakeRVGetter) LastStoreSyncResourceVersion() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rv
}

func TestBalloonPodResizerConsistencyStore(t *testing.T) {
	node := testNode.DeepCopy()
	node.ResourceVersion = "1"

	fakeClient := fake.NewSimpleClientset(node)
	rvGetter := &fakeRVGetter{rv: "1"}

	consistencyStore := consistencyutil.NewConsistencyStore(map[schema.GroupResource]consistencyutil.LastSyncRVGetter{
		{Resource: "nodes"}: rvGetter,
	})

	balloonPodResizer := &defaultBalloonPodResizer{consistencyStore: consistencyStore,
		clientSet: fakeClient,
	}

	// Fake patch returning a different ResourceVersion simulating APIServer write.
	fakeClient.PrependReactor("patch", "nodes", func(action client_testing.Action) (bool, runtime.Object, error) {
		updatedNode := testNode.DeepCopy()
		updatedNode.ResourceVersion = "2"
		updatedNode.Spec.Taints = []v1.Taint{*ekvmtypes.BPResizeTaint.DeepCopy()}
		return true, updatedNode, nil
	})

	if err := consistencyStore.EnsureReady(types.NamespacedName{Name: node.Name}); err != nil {
		t.Fatalf("ConsistencyStore should be ready initially, got: %v", err)
	}

	if _, err := balloonPodResizer.addTaint(node, time.Now()); err != nil {
		t.Fatalf("Unexpected addTaint error: %v", err)
	}

	if err := consistencyStore.EnsureReady(types.NamespacedName{Name: node.Name}); err == nil {
		t.Fatal("ConsistencyStore should NOT be ready after write because RV is stale (1 < 2)")
	}

	rvGetter.rv = "2"

	if err := consistencyStore.EnsureReady(types.NamespacedName{Name: node.Name}); err != nil {
		t.Fatalf("ConsistencyStore should be ready after RV caught up, got: %v", err)
	}
}
