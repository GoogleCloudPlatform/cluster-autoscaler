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
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestGetPodDestinationCandidates(t *testing.T) {
	ekNode := createNode("ek-node", "ek-standard-2")
	ekNodeWithLookahead := createNode("ek-node-with-lookahead", "ek-standard-2")
	nonEkNode := createNode("non-ek-node", "n1-standard-2")
	unknownNode := createNode("unknown-node", "") // No instance type label
	missingFromSnapshotNode := createNode("missing-node", "ek-standard-2")

	lookaheadPod := lookaheadbuffer.BuildTestLookaheadPod("some-workload", 100, 100)
	normalPod := test.BuildTestPod("normal-pod", 100, 100)

	tests := []struct {
		name              string
		experimentEnabled bool
		nodes             []*apiv1.Node
		pods              map[string][]*apiv1.Pod
		wantCandidates    []string
	}{
		{
			name:              "Experiment disabled, return all nodes",
			experimentEnabled: false,
			nodes:             []*apiv1.Node{ekNode, ekNodeWithLookahead, nonEkNode},
			pods: map[string][]*apiv1.Pod{
				ekNode.Name:              {normalPod},
				ekNodeWithLookahead.Name: {lookaheadPod},
				nonEkNode.Name:           {normalPod},
			},
			wantCandidates: []string{ekNode.Name, ekNodeWithLookahead.Name, nonEkNode.Name},
		},
		{
			name:              "Experiment enabled, filter logic",
			experimentEnabled: true,
			nodes:             []*apiv1.Node{ekNode, ekNodeWithLookahead, nonEkNode, unknownNode, missingFromSnapshotNode, nil},
			pods: map[string][]*apiv1.Pod{
				ekNode.Name:              {normalPod},
				ekNodeWithLookahead.Name: {lookaheadPod},
				nonEkNode.Name:           {normalPod},
				unknownNode.Name:         {},
			},
			// Logic:
			// nil -> dropped
			// ekNode -> !HasLookahead -> kept
			// ekNodeWithLookahead -> HasLookahead -> dropped
			// nonEkNode -> kept (not EK)
			// unknownNode -> kept (IsEkMachine fails)
			// missingFromSnapshotNode -> kept (GetNodeInfo fails)
			wantCandidates: []string{ekNode.Name, nonEkNode.Name, unknownNode.Name, missingFromSnapshotNode.Name},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			for nodeName, pods := range tc.pods {
				var node *apiv1.Node
				// Find node in tc.nodes
				for _, n := range tc.nodes {
					if n != nil && n.Name == nodeName {
						node = n
						break
					}
				}
				if node != nil {
					nodeInfo := framework.NewTestNodeInfo(node, pods...)
					err := snapshot.AddNodeInfo(nodeInfo)
					assert.NoError(t, err)
				}
			}

			ctx := &context.AutoscalingContext{
				ClusterSnapshot: snapshot,
			}

			p := NewScaleDownNodeProcessor(createExpManager(tc.experimentEnabled))

			got, err := p.GetPodDestinationCandidates(ctx, tc.nodes)
			assert.NoError(t, err)

			var gotNames []string
			for _, n := range got {
				if n == nil {
					gotNames = append(gotNames, "nil")
				} else {
					gotNames = append(gotNames, n.Name)
				}
			}

			assert.ElementsMatch(t, tc.wantCandidates, gotNames)
		})
	}
}

func TestGetScaleDownCandidates(t *testing.T) {
	t.Parallel()
	p := NewScaleDownNodeProcessor(createExpManager(false))
	nodes := []*apiv1.Node{
		createNode("node1", "ek-standard-32"),
		createNode("node2", "n2-standard-4"),
		createNode("node3", "g2-standard-8"),
	}
	pods := []*apiv1.Pod{
		lookaheadbuffer.BuildTestLookaheadPod("some-workload", 100, 100),
		test.BuildTestPod("normal-pod", 100, 100),
	}
	snapshot := testsnapshot.NewTestSnapshotOrDie(t)
	for _, node := range nodes {
		err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(node, pods...))
		assert.NoError(t, err)
	}
	got, err := p.GetScaleDownCandidates(&context.AutoscalingContext{
		ClusterSnapshot: snapshot,
	}, nodes)
	assert.NoError(t, err)
	assert.Equal(t, nodes, got)
}

func createNode(name string, machineType string) *apiv1.Node {
	n := test.BuildTestNode(name, 1000, 1000)
	if machineType != "" {
		n.Labels[apiv1.LabelInstanceTypeStable] = machineType
	}
	return n
}

func createExpManager(expEnabled bool) experiments.Manager {
	if !expEnabled {
		return experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{experiments.EkPreventScheduleOnLookaheadNodes: expEnabled}, nil)
	}
	return experiments.NewMockManager()
}
