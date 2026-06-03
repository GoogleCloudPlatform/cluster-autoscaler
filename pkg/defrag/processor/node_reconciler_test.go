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

package processor

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	cacontext "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	csisnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/csi/snapshot"
	drasnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/dynamicresources/snapshot"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
)

func TestReconcileCandidates(t *testing.T) {

	testCases := []struct {
		name              string
		candidates        []*defrag.Candidate
		nodes             []*apiv1.Node
		deletedNodes      []string
		patchErrs         map[string]error
		wantTaintedNodes  []string
		wantErrSubstrings []string
	}{
		{
			name:             "no nodes, no candidates",
			wantTaintedNodes: nil,
		},
		{
			name: "some nodes, no candidates",
			nodes: []*apiv1.Node{
				buildReadyNode("n1", 1000, 1),
				buildReadyNode("n2", 1000, 1),
				buildReadyNode("n3", 1000, 1),
			},
			wantTaintedNodes: nil,
		},
		{
			name: "some tainted nodes, no candidates",
			nodes: []*apiv1.Node{
				setDefragTaints(buildReadyNode("n1", 1000, 1)),
				setDefragTaints(buildReadyNode("n2", 1000, 1)),
				buildReadyNode("n3", 1000, 1),
			},
			wantTaintedNodes: nil,
		},
		{
			name: "single candidate without nodes (should never happen)",
			candidates: []*defrag.Candidate{
				{Nodes: []string{}},
			},
			nodes: []*apiv1.Node{
				setDefragTaints(buildReadyNode("n1", 1000, 1)),
				setDefragTaints(buildReadyNode("n2", 1000, 1)),
				buildReadyNode("n3", 1000, 1),
			},
			wantTaintedNodes: nil,
		},
		{
			name: "single candidate with nodes",
			nodes: []*apiv1.Node{
				setDefragTaints(buildReadyNode("n1", 1000, 1)),
				setDefragTaints(buildReadyNode("n2", 1000, 1)),
				buildReadyNode("n3", 1000, 1),
			},
			candidates: []*defrag.Candidate{
				{Nodes: []string{"n1", "n3"}},
			},
			wantTaintedNodes: []string{"n1", "n3"},
		},
		{
			name: "multiple candidate with nodes",
			nodes: []*apiv1.Node{
				setDefragTaints(buildReadyNode("n1", 1000, 1)),
				setDefragTaints(buildReadyNode("n2", 1000, 1)),
				buildReadyNode("n3", 1000, 1),
			},
			candidates: []*defrag.Candidate{
				{Nodes: []string{"n1"}},
				{Nodes: []string{"n3"}},
			},
			wantTaintedNodes: []string{"n1", "n3"},
		},
		{
			name: "upcoming tainted node",
			nodes: []*apiv1.Node{
				setDefragTaints(buildUpcomingNode("n", 1000, 1)),
			},
			candidates:       []*defrag.Candidate{},
			wantTaintedNodes: []string{"n"},
		},
		{
			name: "one node already deleted, second still gets untainted",
			nodes: []*apiv1.Node{
				setDefragTaints(buildReadyNode("n1", 1000, 1)),
				setDefragTaints(buildReadyNode("n2", 1000, 1)),
			},
			deletedNodes:     []string{"n1"},
			wantTaintedNodes: nil,
		},
		{
			name: "some nodes fail to reconcile",
			nodes: []*apiv1.Node{
				buildReadyNode("n1", 1000, 1),
				buildReadyNode("n2", 1000, 1),
				buildReadyNode("n3", 1000, 1),
			},
			candidates: []*defrag.Candidate{
				{Nodes: []string{"n1", "n2"}},
				{Nodes: []string{"n3"}},
			},
			patchErrs: map[string]error{
				"n1": fmt.Errorf("err1"),
				"n3": fmt.Errorf("err2"),
			},
			wantTaintedNodes:  []string{"n2"},
			wantErrSubstrings: []string{"err1", "err2"},
		},
		{
			name: "some nodes fail to clean up",
			nodes: []*apiv1.Node{
				setDefragTaints(buildReadyNode("n1", 1000, 1)),
				setDefragTaints(buildReadyNode("n2", 1000, 1)),
				setDefragTaints(buildReadyNode("n3", 1000, 1)),
			},
			patchErrs: map[string]error{
				"n1": fmt.Errorf("err1"),
				"n3": fmt.Errorf("err2"),
			},
			wantTaintedNodes:  []string{"n1", "n3"},
			wantErrSubstrings: []string{"err1", "err2"},
		},
		{
			name: "skip untainting nodes to be scaled down",
			nodes: []*apiv1.Node{
				setDefragTaints(buildReadyNode("n1", 1000, 1)),
				func() *apiv1.Node {
					node := setDefragTaints(buildReadyNode("n2", 1000, 1))
					node.Spec.Taints = append(node.Spec.Taints, apiv1.Taint{
						Key:    taints.ToBeDeletedTaint,
						Value:  fmt.Sprint(time.Now().Unix()),
						Effect: apiv1.TaintEffectNoSchedule,
					})
					return node
				}(),
			},
			wantTaintedNodes: []string{"n2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var candidateInfos []*candidateInfo
			for _, candidate := range tc.candidates {
				candidateInfos = append(candidateInfos, &candidateInfo{candidate: candidate})
			}
			client := fake.NewSimpleClientset()
			ctx := &cacontext.AutoscalingContext{
				ClusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t),
				AutoscalingKubeClients: cacontext.AutoscalingKubeClients{
					ClientSet: client,
				},
			}
			reconciler := newNodeReconciler(nodeReconcilerOptions{})
			assert.NoError(t, ctx.ClusterSnapshot.SetClusterState(tc.nodes, nil, drasnapshot.NewEmptySnapshot(), csisnapshot.NewEmptySnapshot()))

			client.PrependReactor("patch", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
				patchAction := action.(ktesting.PatchAction)
				nodeName := patchAction.GetName()
				if err := tc.patchErrs[nodeName]; err != nil {
					return true, nil, err
				}
				return false, nil, nil
			})
			for _, node := range tc.nodes {
				if !slices.Contains(tc.deletedNodes, node.Name) {
					_, err := client.CoreV1().Nodes().Create(context.TODO(), node, metav1.CreateOptions{})
					assert.NoError(t, err)
				}
			}

			err := reconciler.Reconcile(ctx, candidateInfos)
			if len(tc.wantErrSubstrings) > 0 {
				assert.Error(t, err)
				for _, wantSubstr := range tc.wantErrSubstrings {
					assert.ErrorContains(t, err, wantSubstr)
				}
			}
			wantTaintedNode := make(map[string]bool)
			for _, node := range tc.wantTaintedNodes {
				wantTaintedNode[node] = true
			}

			nodes, err := client.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
			assert.NoError(t, err)
			for _, node := range nodes.Items {
				assert.Equal(t, wantTaintedNode[node.Name], taints.HasTaint(&node, defrag.HardTaint))
				assert.Equal(t, wantTaintedNode[node.Name], taints.HasTaint(&node, defrag.SoftTaint))
			}
		})
	}
}

func TestReconcileCandidate(t *testing.T) {
	timeNow := time.Now()
	plugin := &fakePlugin{}
	testCases := []struct {
		name             string
		nodes            []*apiv1.Node
		candidate        *defrag.Candidate
		wantTainted      []string
		wantUpdatedNodes []string
	}{
		{
			name: "taint candidate",
			nodes: []*apiv1.Node{
				buildReadyNode("n1", 1000, 1),
			},
			candidate: &defrag.Candidate{
				Nodes:  []string{"n1"},
				Plugin: plugin,
			},
			wantTainted:      []string{"n1"},
			wantUpdatedNodes: []string{"n1"},
		},
		{
			name: "taint candidate with taints already set",
			nodes: []*apiv1.Node{
				setDefragTaints(buildReadyNode("n1", 1000, 1)),
			},
			candidate: &defrag.Candidate{
				Nodes:  []string{"n1"},
				Plugin: plugin,
			},
			wantTainted: []string{"n1"},
		},
		{
			name: "multiple nodes",
			nodes: []*apiv1.Node{
				buildReadyNode("n1", 1000, 1),
				buildReadyNode("n2", 1000, 1),
				buildReadyNode("n3", 1000, 1),
			},
			candidate: &defrag.Candidate{
				Nodes:  []string{"n1", "n2"},
				Plugin: plugin,
			},
			wantTainted:      []string{"n1", "n2"},
			wantUpdatedNodes: []string{"n1", "n2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			var updatedNodes []string
			updatedNodesMap := make(map[string]bool)
			client.PrependReactor("patch", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
				patchAction := action.(ktesting.PatchAction)
				nodeName := patchAction.GetName()
				if !updatedNodesMap[nodeName] {
					updatedNodes = append(updatedNodes, patchAction.GetName())
					updatedNodesMap[nodeName] = true
				}
				return false, nil, nil
			})
			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			for _, node := range tc.nodes {
				_, err := client.CoreV1().Nodes().Create(context.TODO(), node, metav1.CreateOptions{})
				assert.NoError(t, err)
			}
			err := snapshot.SetClusterState(tc.nodes, nil, drasnapshot.NewEmptySnapshot(), csisnapshot.NewEmptySnapshot())
			assert.NoError(t, err)
			info := &candidateInfo{candidate: tc.candidate, creationTime: timeNow}
			ctx := &cacontext.AutoscalingContext{
				ClusterSnapshot: snapshot,
				AutoscalingKubeClients: cacontext.AutoscalingKubeClients{
					ClientSet: client,
				},
			}
			reconciler := newNodeReconciler(nodeReconcilerOptions{})

			err = reconciler.ReconcileCandidate(ctx, info)
			assert.NoError(t, err)

			for _, n := range tc.wantTainted {
				nodeInfo, err := snapshot.GetNodeInfo(n)
				assert.NoError(t, err)
				node := nodeInfo.Node()
				apiNode, err := client.CoreV1().Nodes().Get(context.TODO(), node.Name, metav1.GetOptions{})
				assert.NoError(t, err)
				for _, nodeObj := range []*apiv1.Node{node, apiNode} {
					assert.True(t, taints.HasTaint(nodeObj, defrag.HardTaint))
					assert.True(t, taints.HasTaint(nodeObj, defrag.SoftTaint))
				}
			}
			assert.ElementsMatch(t, tc.wantUpdatedNodes, updatedNodes)
		})
	}
}

func TestApplyOperation_Patch(t *testing.T) {
	taintValue := "defrag-test"

	testCases := []struct {
		name                string
		op                  *applyOperation
		initialNode         *apiv1.Node
		expectedUpdated     bool
		expectedTaintKeys   []string
		expectedAnnotations map[string]string
	}{
		{
			name:                "add only taints (no force-annotation)",
			op:                  &applyOperation{taintValue: taintValue},
			initialNode:         newNode("taints-only-node"),
			expectedUpdated:     true,
			expectedTaintKeys:   []string{defrag.HardTaint, defrag.SoftTaint},
			expectedAnnotations: map[string]string{},
		},
		{
			name: "node has other annotation, so keep it",
			op:   &applyOperation{taintValue: taintValue},
			initialNode: func() *apiv1.Node {
				node := newNode("annotated-node")
				node.Annotations["other-key"] = "other-value"
				return node
			}(),
			expectedUpdated:     true,
			expectedTaintKeys:   []string{defrag.HardTaint, defrag.SoftTaint},
			expectedAnnotations: map[string]string{"other-key": "other-value"},
		},
		{
			name:                "no changes needed, node is already correct",
			op:                  &applyOperation{taintValue: taintValue},
			initialNode:         setDefragTaints(newNode("correct-node")),
			expectedUpdated:     false,
			expectedTaintKeys:   []string{defrag.HardTaint, defrag.SoftTaint},
			expectedAnnotations: map[string]string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nodeToPatch := tc.initialNode.DeepCopy()

			updated, err := tc.op.Patch(nodeToPatch)
			if err != nil {
				t.Fatalf("Patch() returned an unexpected error: %v", err)
			}

			if updated != tc.expectedUpdated {
				t.Errorf("expected updated flag to be %v, but got %v", tc.expectedUpdated, updated)
			}

			actualTaintKeys := make([]string, len(nodeToPatch.Spec.Taints))
			for i, taint := range nodeToPatch.Spec.Taints {
				actualTaintKeys[i] = taint.Key
			}
			if diff := cmp.Diff(actualTaintKeys, tc.expectedTaintKeys); diff != "" {
				t.Errorf("taints mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(nodeToPatch.Annotations, tc.expectedAnnotations); diff != "" {
				t.Errorf("annotations mismatch: (-want +got):\n%s", diff)
			}
		})
	}
}

func TestApplyOperation_ProcessError(t *testing.T) {
	op := &applyOperation{}
	someError := errors.New("some transient error")

	// Should return the same error back
	if err := op.ProcessError(someError); !errors.Is(err, someError) {
		t.Errorf("ProcessError should have returned the original error, but it didn't")
	}

	// Should handle nil error correctly
	if err := op.ProcessError(nil); err != nil {
		t.Errorf("ProcessError should have returned nil when given nil, but got: %v", err)
	}
}

func TestCleanupOperation_Patch(t *testing.T) {
	testCases := []struct {
		name                string
		op                  *cleanupOperation
		initialNode         *apiv1.Node
		expectedUpdated     bool
		expectedTaints      []apiv1.Taint
		expectedAnnotations map[string]string
	}{
		{
			name: "remove taints from a node",
			op:   &cleanupOperation{},
			initialNode: func() *apiv1.Node {
				node := setDefragTaints(newNode("full-cleanup-node"))
				node.Spec.Taints = append(node.Spec.Taints, apiv1.Taint{
					Key: "keep-this-taint", Effect: apiv1.TaintEffectNoSchedule,
				})
				node.Annotations["keep-this-annotation"] = "value"
				return node
			}(),
			expectedUpdated: true,
			expectedTaints: []apiv1.Taint{
				{Key: "keep-this-taint", Effect: apiv1.TaintEffectNoSchedule},
			},
			expectedAnnotations: map[string]string{"keep-this-annotation": "value"},
		},
		{
			name:                "remove taints (no annotation present)",
			op:                  &cleanupOperation{},
			initialNode:         setDefragTaints(newNode("taints-only-node")),
			expectedUpdated:     true,
			expectedTaints:      nil,
			expectedAnnotations: map[string]string{},
		},
		{
			name: "node with only annotation does not get updated",
			op:   &cleanupOperation{},
			initialNode: func() *apiv1.Node {
				node := newNode("annotation-only-node")
				node.Annotations["keep-this-annotation"] = "value"
				return node
			}(),
			expectedUpdated:     false,
			expectedTaints:      nil,
			expectedAnnotations: map[string]string{"keep-this-annotation": "value"},
		},
		{
			name: "no cleanup needed (no relevant taints or annotations)",
			op:   &cleanupOperation{},
			initialNode: func() *apiv1.Node {
				node := newNode("clean-node")
				node.Spec.Taints = []apiv1.Taint{{Key: "other-taint"}}
				node.Annotations["other-annotation"] = "value"
				return node
			}(),
			expectedUpdated:     false,
			expectedTaints:      []apiv1.Taint{{Key: "other-taint"}},
			expectedAnnotations: map[string]string{"other-annotation": "value"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nodeToPatch := tc.initialNode.DeepCopy()

			updated, err := tc.op.Patch(nodeToPatch)
			if err != nil {
				t.Fatalf("Patch() returned an unexpected error: %v", err)
			}

			if updated != tc.expectedUpdated {
				t.Errorf("expected updated flag to be %v, but got %v", tc.expectedUpdated, updated)
			}

			opts := []cmp.Option{cmpopts.EquateEmpty()}

			if diff := cmp.Diff(nodeToPatch.Spec.Taints, tc.expectedTaints, opts...); diff != "" {
				t.Errorf("taints mismatch: (-wantErr +got):\n%s", diff)
			}

			if diff := cmp.Diff(nodeToPatch.Annotations, tc.expectedAnnotations, opts...); diff != "" {
				t.Errorf("annotations mismatch: (-wantErr +got):\n%s", diff)
			}
		})
	}
}

func TestCleanupOperation_ProcessError(t *testing.T) {
	op := &cleanupOperation{}
	testError := errors.New("some error")

	testCases := []struct {
		name     string
		inputErr error
		wantErr  error
	}{
		{
			name:     "IsNotFound error should be ignored",
			inputErr: apierrors.NewNotFound(schema.GroupResource{Group: "v1", Resource: "nodes"}, "missing-node"),
			wantErr:  nil,
		},
		{
			name:     "other error should be passed as is",
			inputErr: testError,
			wantErr:  testError,
		},
		{
			name:     "nil error should be passed as is",
			inputErr: nil,
			wantErr:  nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := op.ProcessError(tc.inputErr); !errors.Is(err, tc.wantErr) {
				t.Errorf("ProcessError(%v) wantErr %v, got %v", tc.inputErr, tc.wantErr, err)
			}
		})
	}
}

func newNode(nodeName string) *apiv1.Node {
	node := buildReadyNode(nodeName, 1000, 1)
	node.Annotations = make(map[string]string)
	return node
}
