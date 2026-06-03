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

package customresources

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
)

func TestFilterOutNodesByAnnotationPresence(t *testing.T) {
	processor := LabelsProcessor{}

	unreadyNode := kubernetes.GetUnreadyNodeCopy(
		buildTestNodeWithLabels("n1", map[string]string{}),
		kubernetes.ResourceUnready,
	)
	nodeWithAnnotation := buildTestNode("n2", true)
	nodeWithoutAnnotation := buildTestNode("n3", false)
	for desc, tc := range map[string]struct {
		initialAllNodes   []*v1.Node
		initialReadyNodes []*v1.Node
		wantAllNodes      []*v1.Node
		wantReadyNodes    []*v1.Node
	}{
		"only unready node": {
			initialAllNodes:   []*v1.Node{unreadyNode},
			initialReadyNodes: []*v1.Node{},
			wantAllNodes:      []*v1.Node{unreadyNode},
			wantReadyNodes:    []*v1.Node{},
		},
		"node with annotation": {
			initialAllNodes:   []*v1.Node{nodeWithAnnotation},
			initialReadyNodes: []*v1.Node{nodeWithAnnotation},
			wantAllNodes:      []*v1.Node{nodeWithAnnotation},
			wantReadyNodes:    []*v1.Node{nodeWithAnnotation},
		},
		"node without annotation": {
			initialAllNodes:   []*v1.Node{nodeWithoutAnnotation},
			initialReadyNodes: []*v1.Node{nodeWithoutAnnotation},
			wantAllNodes:      []*v1.Node{kubernetes.GetUnreadyNodeCopy(nodeWithoutAnnotation, kubernetes.ResourceUnready)},
			wantReadyNodes:    []*v1.Node{},
		},
		"all nodes": {
			initialAllNodes:   []*v1.Node{nodeWithAnnotation, nodeWithoutAnnotation},
			initialReadyNodes: []*v1.Node{nodeWithAnnotation, nodeWithoutAnnotation},
			wantAllNodes:      []*v1.Node{nodeWithAnnotation, kubernetes.GetUnreadyNodeCopy(nodeWithoutAnnotation, kubernetes.ResourceUnready)},
			wantReadyNodes:    []*v1.Node{nodeWithAnnotation},
		},
	} {
		t.Run(desc, func(t *testing.T) {
			gotAllNodes, gotReadyNodes := processor.FilterOutNodesWithMissingLabels(tc.initialAllNodes, tc.initialReadyNodes)
			assert.Equal(t, gotAllNodes, tc.wantAllNodes)
			assert.Equal(t, gotReadyNodes, tc.wantReadyNodes)
		})
	}
}

func buildTestNodeWithLabels(name string, labels map[string]string) *v1.Node {
	node := test.BuildTestNode(name, 1000, 1000)
	node.Labels = labels
	return node
}

func buildTestNode(name string, hasAnnotation bool) *v1.Node {
	node := test.BuildTestNode(name, 1000, 1000)
	if hasAnnotation {
		node.Annotations = map[string]string{
			lastAppliedLabelsKey: "cloud.google.com/gke-nodepool=np-1,cloud.google.com/compute-class=ccc",
		}
	}
	return node
}
