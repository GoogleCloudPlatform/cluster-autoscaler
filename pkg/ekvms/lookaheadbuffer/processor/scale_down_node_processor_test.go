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
	"testing"

	"github.com/stretchr/testify/assert"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestGetPodDestinationCandidates(t *testing.T) {
	resizableNode := createNode("resizable-node", "ek-standard-2")
	ekNodeWithLookahead := createNode("ek-node-with-lookahead", "ek-standard-2")
	e4aNodeWithLookahead := createNode("e4a-node-with-lookahead", "e4a-standard-2")
	e4NodeWithLookahead := createNode("e4-node-with-lookahead", "e4-standard-2")
	nonResizableNode := createNode("non-resizable-node", "n1-standard-2")
	unknownNode := createNode("unknown-node", "") // No instance type label
	missingFromSnapshotNode := createNode("missing-node", "ek-standard-2")

	lookaheadPod := lookaheadbuffer.BuildTestLookaheadPod("some-workload", 100, 100, machinetypes.EK.Name())
	normalPod := test.BuildTestPod("normal-pod", 100, 100)

	tests := []struct {
		name               string
		experimentsEnabled map[string]bool
		nodes              []*apiv1.Node
		pods               map[string][]*apiv1.Pod
		wantCandidates     []string
	}{
		{
			name: "Experiment disabled, return all nodes",
			experimentsEnabled: map[string]bool{
				experiments.EkPreventScheduleOnLookaheadNodesFlag:  false,
				experiments.E4aPreventScheduleOnLookaheadNodesFlag: false,
				experiments.E4PreventScheduleOnLookaheadNodesFlag:  false,
			},
			nodes: []*apiv1.Node{resizableNode, e4NodeWithLookahead, nonResizableNode},
			pods: map[string][]*apiv1.Pod{
				resizableNode.Name:       {normalPod},
				e4NodeWithLookahead.Name: {lookaheadPod},
				nonResizableNode.Name:    {normalPod},
			},
			wantCandidates: []string{resizableNode.Name, e4NodeWithLookahead.Name, nonResizableNode.Name},
		},
		{
			name: "Experiments partially enabled: EK enabled, E4 and E4A disabled",
			experimentsEnabled: map[string]bool{
				experiments.EkPreventScheduleOnLookaheadNodesFlag:  true,
				experiments.E4aPreventScheduleOnLookaheadNodesFlag: false,
				experiments.E4PreventScheduleOnLookaheadNodesFlag:  false,
			},
			nodes: []*apiv1.Node{resizableNode, ekNodeWithLookahead, e4aNodeWithLookahead, e4NodeWithLookahead, nonResizableNode},
			pods: map[string][]*apiv1.Pod{
				resizableNode.Name:        {normalPod},
				ekNodeWithLookahead.Name:  {lookaheadPod},
				e4aNodeWithLookahead.Name: {lookaheadPod},
				e4NodeWithLookahead.Name:  {lookaheadPod},
				nonResizableNode.Name:     {normalPod},
			},
			// Logic:
			// resizableNode -> !HasLookahead -> kept
			// ekNodeWithLookahead -> EK experiment enabled && HasLookahead -> dropped
			// e4aNodeWithLookahead -> E4a experiment disabled -> kept
			// e4NodeWithLookahead -> E4 experiment disabled -> kept
			// nonResizableNode -> kept
			wantCandidates: []string{resizableNode.Name, e4aNodeWithLookahead.Name, e4NodeWithLookahead.Name, nonResizableNode.Name},
		},
		{
			name: "Experiments partially enabled: E4 enabled, EK and E4A disabled",
			experimentsEnabled: map[string]bool{
				experiments.EkPreventScheduleOnLookaheadNodesFlag:  false,
				experiments.E4aPreventScheduleOnLookaheadNodesFlag: false,
				experiments.E4PreventScheduleOnLookaheadNodesFlag:  true,
			},
			nodes: []*apiv1.Node{resizableNode, ekNodeWithLookahead, e4aNodeWithLookahead, e4NodeWithLookahead, nonResizableNode},
			pods: map[string][]*apiv1.Pod{
				resizableNode.Name:        {normalPod},
				ekNodeWithLookahead.Name:  {lookaheadPod},
				e4aNodeWithLookahead.Name: {lookaheadPod},
				e4NodeWithLookahead.Name:  {lookaheadPod},
				nonResizableNode.Name:     {normalPod},
			},
			// Logic:
			// resizableNode -> !HasLookahead -> kept
			// ekNodeWithLookahead -> EK experiment disabled -> kept
			// e4NodeWithLookahead -> E4 experiment enabled && HasLookahead -> dropped
			// e4aNodeWithLookahead -> E4a experiment disabled -> kept
			// nonResizableNode -> kept
			wantCandidates: []string{resizableNode.Name, ekNodeWithLookahead.Name, e4aNodeWithLookahead.Name, nonResizableNode.Name},
		},
		{
			name: "Experiments enabled, filter logic",
			experimentsEnabled: map[string]bool{
				experiments.EkPreventScheduleOnLookaheadNodesFlag:  true,
				experiments.E4aPreventScheduleOnLookaheadNodesFlag: true,
				experiments.E4PreventScheduleOnLookaheadNodesFlag:  true,
			},
			nodes: []*apiv1.Node{resizableNode, e4NodeWithLookahead, nonResizableNode, unknownNode, missingFromSnapshotNode, nil},
			pods: map[string][]*apiv1.Pod{
				resizableNode.Name:       {normalPod},
				e4NodeWithLookahead.Name: {lookaheadPod},
				nonResizableNode.Name:    {normalPod},
				unknownNode.Name:         {},
			},
			// Logic:
			// nil -> dropped
			// resizableNode -> !HasLookahead -> kept
			// e4NodeWithLookahead -> HasLookahead -> dropped
			// nonResizableNode -> kept
			// unknownNode -> kept (IsResizableNode fails)
			// missingFromSnapshotNode -> kept (GetNodeInfo fails)
			wantCandidates: []string{resizableNode.Name, nonResizableNode.Name, unknownNode.Name, missingFromSnapshotNode.Name},
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

			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, tc.experimentsEnabled, nil)
			p := NewScaleDownNodeProcessor(machinetypes.NewMachineConfigProvider(nil), experimentsManager)

			ctx := &ca_context.AutoscalingContext{
				ClusterSnapshot: snapshot,
			}
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
	experimentsManager := experiments.NewMockManager()
	p := NewScaleDownNodeProcessor(machinetypes.NewMachineConfigProvider(nil), experimentsManager)
	nodes := []*apiv1.Node{
		createNode("node1", "ek-standard-32"),
		createNode("node2", "n2-standard-4"),
		createNode("node3", "g2-standard-8"),
	}
	pods := []*apiv1.Pod{
		lookaheadbuffer.BuildTestLookaheadPod("some-workload", 100, 100, machinetypes.EK.Name()),
		test.BuildTestPod("normal-pod", 100, 100),
	}
	snapshot := testsnapshot.NewTestSnapshotOrDie(t)
	for _, node := range nodes {
		err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(node, pods...))
		assert.NoError(t, err)
	}
	got, err := p.GetScaleDownCandidates(context.TODO(), &ca_context.AutoscalingContext{
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
