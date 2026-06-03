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

package utils

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPatchNodeAndStatus(t *testing.T) {
	testCases := []struct {
		name    string
		node    *v1.Node
		patcher func(n *v1.Node) (bool, error)
	}{
		{
			name: "no update",
			node: test.BuildTestNode("n1", 1000, 1000),
			patcher: func(n *v1.Node) (bool, error) {
				return false, nil
			},
		},
		{
			name: "update only status",
			node: test.BuildTestNode("n2", 1000, 1000),
			patcher: func(n *v1.Node) (bool, error) {
				newConditions := []v1.NodeCondition{
					{
						Type:   v1.NodeReady,
						Status: v1.ConditionFalse,
						Reason: "KubeletNotReady",
					},
				}
				// BuildTestNode initializes the labels field to empty map,
				// regardless of whether its needed
				n.Labels = nil
				n.Status.Conditions = newConditions
				return true, nil
			},
		},
		{
			name: "update only non-status field (label)",
			node: test.BuildTestNode("n3", 1000, 1000),
			patcher: func(n *v1.Node) (bool, error) {
				if n.Labels == nil {
					n.Labels = make(map[string]string)
				}
				if _, exists := n.Labels["new-label"]; exists {
					return false, nil
				}
				n.Labels["new-label"] = "value"
				return true, nil
			},
		},
		{
			name: "update both status and label",
			node: test.BuildTestNode("n4", 1000, 1000),
			patcher: func(n *v1.Node) (bool, error) {
				newConditions := []v1.NodeCondition{
					{
						Type:   v1.NodeDiskPressure,
						Status: v1.ConditionTrue,
					},
				}

				n.Status.Conditions = newConditions

				if n.Labels == nil {
					n.Labels = make(map[string]string)
				}
				if _, exists := n.Labels["another-label"]; !exists {
					n.Labels["another-label"] = "another-value"
				}
				return true, nil
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fakeClient := fake.NewSimpleClientset(tc.node)
			patchedLocally := tc.node.DeepCopy()
			_, err := tc.patcher(patchedLocally)
			if err != nil {
				t.Fatalf("failed to apply patch to node: %v", err)
			}

			_, err = PatchNode(ctx, fakeClient, tc.node, tc.patcher, true)
			if err != nil {
				t.Fatalf("failed to patch node: %v", err)
			}

			actualNode, err := fakeClient.CoreV1().Nodes().Get(context.TODO(), tc.node.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("failed to get node: %v", err)
			}
			if diff := cmp.Diff(patchedLocally, actualNode); diff != "" {
				t.Errorf("node mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAnnotateNode(t *testing.T) {
	testCases := []struct {
		name                string
		existingAnnotations map[string]string
		newAnnotations      map[string]string
		wantAnnotations     map[string]string
	}{
		{
			name: "empty annotations",
			newAnnotations: map[string]string{
				"a": "b",
				"c": "d",
			},
			wantAnnotations: map[string]string{
				"a": "b",
				"c": "d",
			},
		},
		{
			name: "no conflicting annotations",
			existingAnnotations: map[string]string{
				"a": "b",
			},
			newAnnotations: map[string]string{
				"c": "d",
			},
			wantAnnotations: map[string]string{
				"a": "b",
				"c": "d",
			},
		},
		{
			name: "conflicting annotations",
			existingAnnotations: map[string]string{
				"a": "b",
				"c": "e",
			},
			newAnnotations: map[string]string{
				"c": "d",
			},
			wantAnnotations: map[string]string{
				"a": "b",
				"c": "d",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			n := test.BuildTestNode("n1", 1000, 1000)
			n.Annotations = tc.existingAnnotations
			fakeClient := fake.NewSimpleClientset(n)
			err := AnnotateNode(ctx, fakeClient, n, tc.newAnnotations)
			if err != nil {
				t.Fatalf("got error: %v", err)
			}
			newNode, err := fakeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("failed to get node: %v", err)
			}
			if diff := cmp.Diff(tc.wantAnnotations, newNode.Annotations); diff != "" {
				t.Errorf("annotations mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRemoveAnnotations(t *testing.T) {
	testCases := []struct {
		name                string
		existingAnnotations map[string]string
		annotationKeys      []string
		wantAnnotations     map[string]string
	}{
		{
			name:            "empty annotations",
			annotationKeys:  []string{"a"},
			wantAnnotations: nil,
		},
		{
			name: "annotation removed",
			existingAnnotations: map[string]string{
				"a": "b",
				"c": "d",
			},
			annotationKeys: []string{"a"},
			wantAnnotations: map[string]string{
				"c": "d",
			},
		},
		{
			name: "all annotations removed",
			existingAnnotations: map[string]string{
				"a": "b",
				"c": "d",
			},
			annotationKeys:  []string{"a", "c"},
			wantAnnotations: map[string]string{},
		},
		{
			name: "annotation not present",
			existingAnnotations: map[string]string{
				"a": "b",
				"c": "d",
			},
			annotationKeys: []string{"e"},
			wantAnnotations: map[string]string{
				"a": "b",
				"c": "d",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			n := test.BuildTestNode("n1", 1000, 1000)
			n.Annotations = tc.existingAnnotations
			fakeClient := fake.NewSimpleClientset(n)
			err := RemoveAnnotations(ctx, fakeClient, n, tc.annotationKeys)
			if err != nil {
				t.Fatalf("got error: %v", err)
			}
			newNode, err := fakeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("failed to get node: %v", err)
			}
			if diff := cmp.Diff(tc.wantAnnotations, newNode.Annotations, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("annotations mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
