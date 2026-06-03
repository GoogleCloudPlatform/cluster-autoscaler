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

package nodepooldrain

import (
	"testing"

	"github.com/stretchr/testify/assert"

	apiv1 "k8s.io/api/core/v1"
	testCloudProvider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	csisnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/csi/snapshot"
	drasnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/dynamicresources/snapshot"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/config"
)

type NodeGroup struct {
	id      string
	maxSize int
	nodes   []*apiv1.Node
}

func TestNodePoolDrainNewCandidate(t *testing.T) {
	testCases := []struct {
		name                      string
		nodeNames                 []string
		nodeGroups                []NodeGroup
		maxCandidateNodesCount    int
		wantCandidateNodeNames    []string
		wantLatestUnfitNodesCount int
	}{
		{
			name:      "No potential candidate nodes exists",
			nodeNames: []string{"n1", "n2", "n3"},
			nodeGroups: []NodeGroup{
				{id: "group1", maxSize: nodeDrainThreshold + 5, nodes: []*apiv1.Node{
					test.BuildTestNode("n1", 1000, 10),
				}},
				{id: "group2", maxSize: nodeDrainThreshold + 10, nodes: []*apiv1.Node{
					test.BuildTestNode("n2", 1000, 10),
					test.BuildTestNode("n3", 1000, 10),
				}},
			},
			wantCandidateNodeNames: []string{},
		},
		{
			name:      "Only one potential candidate node exists",
			nodeNames: []string{"n1", "n2", "n3"},
			nodeGroups: []NodeGroup{
				{id: "group1", maxSize: nodeDrainThreshold, nodes: []*apiv1.Node{
					test.BuildTestNode("n1", 1000, 10),
				}},
				{id: "group2", maxSize: nodeDrainThreshold + 1, nodes: []*apiv1.Node{
					test.BuildTestNode("n2", 1000, 10),
					test.BuildTestNode("n3", 1000, 10),
				}},
			},
			wantCandidateNodeNames:    []string{"n1"},
			wantLatestUnfitNodesCount: 1,
		},
		{
			name:      "Multiple potential candidates node exist",
			nodeNames: []string{"n1", "n2", "n3"},
			nodeGroups: []NodeGroup{
				{id: "group1", maxSize: nodeDrainThreshold, nodes: []*apiv1.Node{
					test.BuildTestNode("n1", 1000, 10),
					test.BuildTestNode("n2", 1000, 10),
				}},
				{id: "group2", maxSize: nodeDrainThreshold + 1, nodes: []*apiv1.Node{
					test.BuildTestNode("n3", 1000, 10),
				}},
			},
			wantCandidateNodeNames:    []string{"n1", "n2"},
			wantLatestUnfitNodesCount: 2,
		},
		{
			name:      "Multiple potential candidates node exist, max candidate nodes count exceeded",
			nodeNames: []string{"n1", "n2", "n3"},
			nodeGroups: []NodeGroup{
				{id: "group1", maxSize: nodeDrainThreshold, nodes: []*apiv1.Node{
					test.BuildTestNode("n1", 1000, 10),
					test.BuildTestNode("n2", 1000, 10),
				}},
				{id: "group2", maxSize: nodeDrainThreshold + 1, nodes: []*apiv1.Node{
					test.BuildTestNode("n3", 1000, 10),
				}},
			},
			maxCandidateNodesCount:    1,
			wantCandidateNodeNames:    []string{"n1"},
			wantLatestUnfitNodesCount: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cp := testCloudProvider.NewTestCloudProviderBuilder().Build()
			var allNodes []*apiv1.Node
			for _, ng := range tc.nodeGroups {
				cp.AddNodeGroup(ng.id, 0, ng.maxSize, 0)
				for _, node := range ng.nodes {
					cp.AddNode(ng.id, node)
					allNodes = append(allNodes, node)
				}
			}
			cs := testsnapshot.NewTestSnapshotOrDie(t)
			assert.NoError(t, cs.SetClusterState(allNodes, nil, drasnapshot.NewEmptySnapshot(), csisnapshot.NewEmptySnapshot()))

			ctx := &context.AutoscalingContext{
				ClusterSnapshot: cs,
				CloudProvider:   cp,
			}

			plugin := NewPlugin(config.PluginsConfig{
				MaxCandidateNodeCount: tc.maxCandidateNodesCount,
			})
			candidate := plugin.NewCandidate(ctx, tc.nodeNames)

			if len(tc.wantCandidateNodeNames) == 0 {
				assert.Nil(t, candidate)
			} else {
				assert.NotNil(t, candidate)
				assert.Equal(t, tc.wantCandidateNodeNames, candidate.Nodes)
				assert.Equal(t, defrag.Partial, candidate.Mode)
			}

			latestUnfitNodesCount := plugin.LatestUnfitNodesCount()
			if latestUnfitNodesCount != tc.wantLatestUnfitNodesCount {
				t.Errorf("plugin.LatestUnfitNodesCount() got latest unfit node count: %d, want latest unfit node count: %d", latestUnfitNodesCount, tc.wantLatestUnfitNodesCount)
			}
		})
	}
}

func TestNodePoolDrainValidCandidateNodes(t *testing.T) {
	testCases := []struct {
		name                    string
		candidateNodes          []string
		nodeGroups              []NodeGroup
		wantValidCandidateNodes []string
	}{
		{
			name:           "Candidate is valid",
			candidateNodes: []string{"n1"},
			nodeGroups: []NodeGroup{
				{id: "group1", maxSize: nodeDrainThreshold, nodes: []*apiv1.Node{
					test.BuildTestNode("n1", 1000, 10),
					test.BuildTestNode("n2", 1000, 10),
					test.BuildTestNode("n3", 1000, 10),
				}},
			},
			wantValidCandidateNodes: []string{"n1"},
		},
		{
			name:           "Candidate is invalid - max size > nodeDrainThreshold",
			candidateNodes: []string{"n1", "n2", "n3"},
			nodeGroups: []NodeGroup{
				{id: "group1", maxSize: nodeDrainThreshold + 5, nodes: []*apiv1.Node{
					test.BuildTestNode("n1", 1000, 10),
					test.BuildTestNode("n2", 1000, 10),
					test.BuildTestNode("n3", 1000, 10),
				}},
			},
			wantValidCandidateNodes: nil,
		},
		{
			name:           "Candidate is invalid - nodeInfo does not exist",
			candidateNodes: []string{"n6"},
			nodeGroups: []NodeGroup{
				{id: "group1", maxSize: nodeDrainThreshold + 5, nodes: []*apiv1.Node{
					test.BuildTestNode("n1", 1000, 10),
					test.BuildTestNode("n2", 1000, 10),
					test.BuildTestNode("n3", 1000, 10),
				}},
			},
			wantValidCandidateNodes: nil,
		},
		{
			name: "some nodes are invalid",
			nodeGroups: []NodeGroup{
				{
					id:      "group1",
					maxSize: nodeDrainThreshold + 5,
					nodes: []*apiv1.Node{
						test.BuildTestNode("n1", 1000, 10),
					},
				},
				{
					id:      "group2",
					maxSize: nodeDrainThreshold,
					nodes: []*apiv1.Node{
						test.BuildTestNode("n2", 1000, 10),
					},
				},
			},
			candidateNodes:          []string{"n1", "n2"},
			wantValidCandidateNodes: []string{"n2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cp := testCloudProvider.NewTestCloudProviderBuilder().Build()
			var allNodes []*apiv1.Node
			for _, ng := range tc.nodeGroups {
				cp.AddNodeGroup(ng.id, 0, ng.maxSize, 0)
				for _, node := range ng.nodes {
					cp.AddNode(ng.id, node)
					allNodes = append(allNodes, node)
				}
			}

			cs := testsnapshot.NewTestSnapshotOrDie(t)
			assert.NoError(t, cs.SetClusterState(allNodes, nil, drasnapshot.NewEmptySnapshot(), csisnapshot.NewEmptySnapshot()))

			ctx := &context.AutoscalingContext{
				ClusterSnapshot: cs,
				CloudProvider:   cp,
			}

			plugin := NewPlugin(config.PluginsConfig{})
			assert.Equal(t, tc.wantValidCandidateNodes, plugin.ValidCandidateNodes(ctx, tc.candidateNodes))
		})
	}
}
