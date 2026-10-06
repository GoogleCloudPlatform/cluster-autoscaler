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

package resizablevmconsolidation

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"

	v1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/config"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/operationtracker"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/processor"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	ekvms_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const (
	giBToKiB   = 1024 * 1024
	giBToBytes = 1024 * 1024 * 1024
)

func TestNewCandidate(t *testing.T) {
	testCases := []struct {
		name                      string
		maxCandidateNodeCount     int
		resizableNodesSnapshot    operationtracker.ResizableNodesSnapshot
		nodeNames                 []string
		nodes                     []*v1.Node
		podsPerNode               map[string][]*v1.Pod
		wantCandidateNodeNames    []string
		wantLatestUnfitNodesCount int
		experimentFlags           map[string]bool
	}{
		{
			name:                  "Resizing is disabled - no candidates",
			maxCandidateNodeCount: 1,
			nodes: []*v1.Node{
				node8(machinetypes.EK.Name(), "ek-node", 8000, 32),
			},
			podsPerNode: map[string][]*v1.Pod{
				"ek-node": {
					buildTestPod("ek-node-pod-1", "ek-node", 500, 1),
					buildTestPod("ek-node-pod-2", "ek-node", 500, 1),
				},
			},
			nodeNames: []string{"ek-node"},
		},
		{
			name:                  "No resizable nodes - no candidates",
			maxCandidateNodeCount: 1,
			resizableNodesSnapshot: operationtracker.ResizableNodesSnapshot{
				"ek-node-1": buildTestResizableNodeObject(6500, 29, 8000, 32),
				"ek-node-2": buildTestResizableNodeObject(6500, 29, 8000, 32),
				"ek-node-3": buildTestResizableNodeObject(6500, 29, 8000, 32),
			},
			nodes: []*v1.Node{
				buildTestNode("ek-node-1", 8000, 32),
				buildTestNode("ek-node-2", 8000, 32),
				buildTestNode("ek-node-3", 8000, 32),
				buildTestNode("e2-node", 8000, 32),
			},
			podsPerNode: map[string][]*v1.Pod{
				"e2-node": {
					buildTestPod("e2-node-pod-1", "e2-node", 500, 10),
					buildTestPod("e2-node-pod-2", "e2-node", 500, 10),
				},
			},
			nodeNames: []string{"e2-node"},
		},
		{
			name:                  "Several resizable families: Pod can be rescheduled on an E4A node with lookahead pods when E4A experiment disabled even if EK experiment enabled",
			maxCandidateNodeCount: 5,
			resizableNodesSnapshot: operationtracker.ResizableNodesSnapshot{
				"ek-node-1":  buildTestResizableNodeObject(1500, 5, 7500, 30),
				"e4a-node-2": buildTestResizableNodeObject(1500, 5, 7500, 30),
			},
			nodes: []*v1.Node{
				node8(machinetypes.EK.Name(), "ek-node-1", 8000, 32),
				node8(machinetypes.E4A.Name(), "e4a-node-2", 8000, 32),
			},
			podsPerNode: map[string][]*v1.Pod{
				"ek-node-1": {
					buildTestPod("ek-node-1-pod", "ek-node-1", 1500, 5),
					buildTestBalloonPod("ek-node-1-balloon-pod", "ek-node-1", 6000, 25),
				},
				"e4a-node-2": {
					buildTestPod("e4a-node-2-pod", "e4a-node-2", 1000, 4),
					lookaheadbuffer.BuildTestLookaheadPod("", 500, 1, machinetypes.E4A.Name(), lookaheadbuffer.WithNode("e4a-node-2")),
					buildTestBalloonPod("e4a-node-2-balloon-pod", "e4a-node-2", 6000, 25),
				},
			},
			experimentFlags: map[string]bool{
				experiments.EkPreventScheduleOnLookaheadNodesFlag:  true,
				experiments.E4aPreventScheduleOnLookaheadNodesFlag: false,
			},
			nodeNames:                 []string{"ek-node-1"},
			wantCandidateNodeNames:    []string{"ek-node-1"},
			wantLatestUnfitNodesCount: 1,
		},
	}

	for _, family := range []string{machinetypes.EK.Name(), machinetypes.E4A.Name(), machinetypes.E4.Name()} {
		testCases = append(testCases, []struct {
			name                      string
			maxCandidateNodeCount     int
			resizableNodesSnapshot    operationtracker.ResizableNodesSnapshot
			nodeNames                 []string
			nodes                     []*v1.Node
			podsPerNode               map[string][]*v1.Pod
			wantCandidateNodeNames    []string
			wantLatestUnfitNodesCount int
			experimentFlags           map[string]bool
		}{
			{
				name:                  fmt.Sprintf("%s: Nodes are big enough - no candidates", family),
				maxCandidateNodeCount: 1,
				resizableNodesSnapshot: operationtracker.ResizableNodesSnapshot{
					"node-1": buildTestResizableNodeObject(2000, 20, 8000, 32),
					"node-2": buildTestResizableNodeObject(6500, 20, 8000, 32),
					"node-3": buildTestResizableNodeObject(6500, 20, 8000, 32),
				},
				nodes: []*v1.Node{
					node8(family, "node-1", 8000, 32),
					node8(family, "node-2", 8000, 32),
					node8(family, "node-3", 8000, 32),
				},
				podsPerNode: map[string][]*v1.Pod{
					"node-1": {
						buildTestPod("node-1-pod", "node-1", 2000, 20),
					},
					"node-2": {
						buildTestPod("node-2-pod", "node-2", 5000, 20),
					},
					"node-3": {
						buildTestPod("node-3-pod", "node-3", 5000, 20),
					},
				},
				nodeNames: []string{"node-1", "node-2", "node-3"},
			},
			{
				name:                  fmt.Sprintf("%s: Not all pods are schedulable - no candidates", family),
				maxCandidateNodeCount: 1,
				resizableNodesSnapshot: operationtracker.ResizableNodesSnapshot{
					"node-1": buildTestResizableNodeObject(1500, 5, 7500, 30),
					"node-2": buildTestResizableNodeObject(7000, 10, 7500, 30),
					"node-3": buildTestResizableNodeObject(7000, 10, 7500, 30),
				},
				nodes: []*v1.Node{
					node8(family, "node-1", 8000, 32),
					node8(family, "node-2", 8000, 32),
					node8(family, "node-3", 8000, 32),
				},
				podsPerNode: map[string][]*v1.Pod{
					"node-1": {
						buildTestPod("node-1-pod", "node-1", 500, 2),
						buildTestPod("node-2-pod", "node-1", 1000, 3),
						buildTestBalloonPod("node-1-balloon-pod", "node-1", 6000, 20),
					},
					"node-2": {
						buildTestPod("node-2-pod", "node-2", 7000, 10),
						buildTestBalloonPod("node-2-balloon-pod", "node-2", 500, 20),
					},
					"node-3": {
						buildTestPod("node-3-pod", "node-3", 7000, 10),
						buildTestBalloonPod("node-3-balloon-pod", "node-3", 500, 20),
					},
				},
				nodeNames:                 []string{"node-1"},
				wantLatestUnfitNodesCount: 1,
			},
			{
				name:                  fmt.Sprintf("%s: Node has a lookahead pod - not a candidate", family),
				maxCandidateNodeCount: 1,
				resizableNodesSnapshot: operationtracker.ResizableNodesSnapshot{
					"node-1": buildTestResizableNodeObject(1500, 5, 7500, 30),
					"node-2": buildTestResizableNodeObject(2500, 10, 7500, 30),
					"node-3": buildTestResizableNodeObject(24000, 10, 30000, 120),
				},
				nodes: []*v1.Node{
					node8(family, "node-1", 8000, 32),
					node8(family, "node-2", 8000, 32),
					node32(family, "node-3", 32000, 128),
				},
				podsPerNode: map[string][]*v1.Pod{
					"node-1": {
						lookaheadbuffer.BuildTestLookaheadPod("", 1500, 5, family, lookaheadbuffer.WithNode("node-1")),
						buildTestBalloonPod("node-1-balloon-pod", "node-1", 6000, 25),
					},
					"node-2": {
						buildTestPod("node-2-pod", "node-2", 2500, 5),
						buildTestBalloonPod("node-2-balloon-pod", "node-2", 4000, 25),
					},
					"node-3": {
						buildTestPod("node-3-pod", "node-3", 24000, 10),
						buildTestBalloonPod("node-3-balloon-pod", "node-3", 6000, 20),
					},
				},
				nodeNames: []string{"node-1"},
			},
			{
				name:                  fmt.Sprintf("%s: Pods from all nodes can be rescheduled, 1 node per candidate - only first node is candidate", family),
				maxCandidateNodeCount: 1,
				resizableNodesSnapshot: operationtracker.ResizableNodesSnapshot{
					"node-1": buildTestResizableNodeObject(1500, 5, 7500, 30),
					"node-2": buildTestResizableNodeObject(1500, 5, 7500, 30),
					"node-3": buildTestResizableNodeObject(24000, 10, 30000, 120),
				},
				nodes: []*v1.Node{
					node8(family, "node-1", 8000, 32),
					node8(family, "node-2", 8000, 32),
					node32(family, "node-3", 32000, 128),
				},
				podsPerNode: map[string][]*v1.Pod{
					"node-1": {
						buildTestPod("node-1-pod", "node-1", 1500, 5),
						buildTestBalloonPod("node-1-balloon-pod", "node-1", 6000, 25),
					},
					"node-2": {
						buildTestPod("node-2-pod", "node-2", 1500, 5),
						buildTestBalloonPod("node-2-balloon-pod", "node-2", 6000, 25),
					},
					"node-3": {
						buildTestPod("node-3-pod", "node-3", 24000, 10),
						buildTestBalloonPod("node-3-balloon-pod", "node-3", 6000, 20),
					},
				},
				nodeNames:                 []string{"node-1", "node-2"},
				wantCandidateNodeNames:    []string{"node-1"},
				wantLatestUnfitNodesCount: 2,
			},
			{
				name:                  fmt.Sprintf("%s: Pods from all nodes can be rescheduled, many nodes per candidate - nodes 1 and 2 are candidates", family),
				maxCandidateNodeCount: 5,
				resizableNodesSnapshot: operationtracker.ResizableNodesSnapshot{
					"node-1": buildTestResizableNodeObject(1500, 5, 7500, 30),
					"node-2": buildTestResizableNodeObject(1500, 5, 7500, 30),
					"node-3": buildTestResizableNodeObject(24000, 10, 30000, 120),
				},
				nodes: []*v1.Node{
					node8(family, "node-1", 8000, 32),
					node8(family, "node-2", 8000, 32),
					node32(family, "node-3", 32000, 128),
				},
				podsPerNode: map[string][]*v1.Pod{
					"node-1": {
						buildTestPod("node-1-pod", "node-1", 1500, 5),
						buildTestBalloonPod("node-1-balloon-pod", "node-1", 6000, 25),
					},
					"node-2": {
						buildTestPod("node-2-pod", "node-2", 1500, 5),
						buildTestBalloonPod("node-2-balloon-pod", "node-2", 6000, 25),
					},
					"node-3": {
						buildTestPod("node-3-pod", "node-3", 24000, 10),
						buildTestBalloonPod("node-3-balloon-pod", "node-3", 6000, 20),
					},
				},
				nodeNames:                 []string{"node-1", "node-2"},
				wantCandidateNodeNames:    []string{"node-1", "node-2"},
				wantLatestUnfitNodesCount: 2,
			},
			{
				name:                  fmt.Sprintf("%s: Nodes 1 and 2 are candidates, node 3 used the desired cpu over the upsizable max cpu", family),
				maxCandidateNodeCount: 5,
				resizableNodesSnapshot: operationtracker.ResizableNodesSnapshot{
					"node-1": buildTestResizableNodeObject(1500, 5, 7500, 30),
					"node-2": buildTestResizableNodeObject(1500, 5, 7500, 30),
					"node-3": buildTestResizableNodeObject(30000, 10, 1000, 120),
				},
				nodes: []*v1.Node{
					node8(family, "node-1", 8000, 32),
					node8(family, "node-2", 8000, 32),
					node32(family, "node-3", 32000, 128),
				},
				podsPerNode: map[string][]*v1.Pod{
					"node-1": {
						buildTestPod("node-1-pod", "node-1", 1500, 5),
						buildTestBalloonPod("node-1-balloon-pod", "node-1", 6000, 25),
					},
					"node-2": {
						buildTestPod("node-2-pod", "node-2", 1500, 5),
						buildTestBalloonPod("node-2-balloon-pod", "node-2", 6000, 25),
					},
					"node-3": {
						buildTestPod("node-3-pod", "node-3", 24000, 10),
						buildTestBalloonPod("node-3-balloon-pod", "node-3", 6000, 20),
					},
				},
				nodeNames:                 []string{"node-1", "node-2"},
				wantCandidateNodeNames:    []string{"node-1", "node-2"},
				wantLatestUnfitNodesCount: 2,
			},
			{
				name:                  fmt.Sprintf("%s: Pod cannot be rescheduled on a node with lookahead pods", family),
				maxCandidateNodeCount: 5,
				resizableNodesSnapshot: operationtracker.ResizableNodesSnapshot{
					"node-1": buildTestResizableNodeObject(1500, 5, 7500, 30),
					"node-2": buildTestResizableNodeObject(1500, 5, 7500, 30),
				},
				nodes: []*v1.Node{
					node8(family, "node-1", 8000, 32),
					node8(family, "node-2", 8000, 32),
				},
				podsPerNode: map[string][]*v1.Pod{
					"node-1": {
						buildTestPod("node-1-pod", "node-1", 1500, 5),
						buildTestBalloonPod("node-1-balloon-pod", "node-1", 6000, 25),
					},
					"node-2": {
						buildTestPod("node-2-pod", "node-2", 1000, 4),
						lookaheadbuffer.BuildTestLookaheadPod("", 500, 1, family, lookaheadbuffer.WithNode("node-2")),
						buildTestBalloonPod("node-2-balloon-pod", "node-2", 6000, 25),
					},
				},
				nodeNames:                 []string{"node-1"},
				wantCandidateNodeNames:    nil,
				wantLatestUnfitNodesCount: 1,
			},
			{
				name:                  fmt.Sprintf("%s: Pods can be rescheduled on nodes with lookahead pods when experiment disabled", family),
				maxCandidateNodeCount: 5,
				resizableNodesSnapshot: operationtracker.ResizableNodesSnapshot{
					"node-1": buildTestResizableNodeObject(1500, 5, 7500, 30),
					"node-2": buildTestResizableNodeObject(1500, 5, 7500, 30),
				},
				nodes: []*v1.Node{
					node8(family, "node-1", 8000, 32),
					node8(family, "node-2", 8000, 32),
				},
				podsPerNode: map[string][]*v1.Pod{
					"node-1": {
						buildTestPod("node-1-pod", "node-1", 1500, 5),
						buildTestBalloonPod("node-1-balloon-pod", "node-1", 6000, 25),
					},
					"node-2": {
						buildTestPod("node-2-pod", "node-2", 1000, 4),
						lookaheadbuffer.BuildTestLookaheadPod("", 500, 1, family, lookaheadbuffer.WithNode("node-2")),
						buildTestBalloonPod("node-2-balloon-pod", "node-2", 6000, 25),
					},
				},
				experimentFlags: map[string]bool{
					processor.PreventScheduleOnLookaheadExperimentFlags[family]: false,
				},
				nodeNames:                 []string{"node-1"},
				wantCandidateNodeNames:    []string{"node-1"},
				wantLatestUnfitNodesCount: 1,
			},
		}...)
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cs := testsnapshot.NewTestSnapshotOrDie(t)
			for _, node := range tc.nodes {
				assert.NoError(t, cs.AddNodeInfo(framework.NewTestNodeInfo(node, tc.podsPerNode[node.Name]...)))
			}
			ctx := &context.AutoscalingContext{ClusterSnapshot: cs}

			resizableVmManager := &mockResizableVmManager{}
			resizableVmManager.On("FilteredNodesSnapshot", false, operationtracker.ResizableOnly).Return(tc.resizableNodesSnapshot)

			plugin := NewPlugin(config.PluginsConfig{
				ResizableVmManager:    resizableVmManager,
				ExperimentsManager:    experiments.NewMockManagerWithOptions(version.Version{}, tc.experimentFlags, nil),
				MaxCandidateNodeCount: tc.maxCandidateNodeCount,
			})
			candidate := plugin.NewCandidate(ctx, tc.nodeNames)

			if len(tc.wantCandidateNodeNames) == 0 {
				assert.Nil(t, candidate)
			} else {
				assert.NotNil(t, candidate)
				assert.Equal(t, tc.wantCandidateNodeNames, candidate.Nodes)
				assert.Equal(t, defrag.CreateBeforeDelete, candidate.Mode)
				assert.False(t, candidate.IsAtomic)
			}

			latestUnfitNodesCount := plugin.LatestUnfitNodesCount()
			if latestUnfitNodesCount != tc.wantLatestUnfitNodesCount {
				t.Errorf("plugin.LatestUnfitNodesCount() got latest unfit node count: %d, want latest unfit node count: %d", latestUnfitNodesCount, tc.wantLatestUnfitNodesCount)
			}
		})
	}
}

func TestValidCandidateNodes(t *testing.T) {
	testCases := []struct {
		name                    string
		maxCandidateNodeCount   int
		resizableNodesSnapshot  operationtracker.ResizableNodesSnapshot
		nodeNames               []string
		nodes                   []*v1.Node
		podsPerNode             map[string][]*v1.Pod
		wantValidCandidateNodes []string
		experimentFlags         map[string]bool
	}{
		{
			name:                  "Resizing is disabled",
			maxCandidateNodeCount: 1,
			nodes: []*v1.Node{
				node8(machinetypes.EK.Name(), "ek-node", 8000, 32),
			},
			podsPerNode: map[string][]*v1.Pod{
				"ek-node": {
					buildTestPod("ek-node-pod-1", "ek-node", 500, 1),
					buildTestPod("ek-node-pod-2", "ek-node", 500, 1),
				},
			},
			nodeNames:               []string{"ek-node"},
			wantValidCandidateNodes: nil,
		},
		{
			name:                  "Pods from EKs and E4As are schedulable",
			maxCandidateNodeCount: 2,
			resizableNodesSnapshot: operationtracker.ResizableNodesSnapshot{
				"ek-node-1":  buildTestResizableNodeObject(2500, 10, 7500, 30),
				"e4a-node-2": buildTestResizableNodeObject(2500, 10, 7500, 30),
				"ek-node-3":  buildTestResizableNodeObject(24000, 10, 30000, 120),
			},
			nodes: []*v1.Node{
				node8(machinetypes.EK.Name(), "ek-node-1", 8000, 32),
				node8(machinetypes.E4A.Name(), "e4a-node-2", 8000, 32),
				node32(machinetypes.EK.Name(), "ek-node-3", 32000, 128),
			},
			podsPerNode: map[string][]*v1.Pod{
				"ek-node-1": {
					buildTestPod("ek-node-1-pod", "ek-node-1", 2500, 5),
					buildTestBalloonPod("ek-node-1-balloon-pod", "ek-node-1", 4000, 25),
				},
				"e4a-node-2": {
					buildTestPod("e4a-node-2-pod", "e4a-node-2", 2500, 5),
					buildTestBalloonPod("e4a-node-2-balloon-pod", "e4a-node-2", 4000, 25),
				},
				"ek-node-3": {
					buildTestPod("ek-node-3-pod", "ek-node-3", 24000, 10),
					buildTestBalloonPod("ek-node-3-balloon-pod", "ek-node-3", 6000, 20),
				},
			},
			nodeNames:               []string{"ek-node-1", "e4a-node-2"},
			wantValidCandidateNodes: []string{"ek-node-1", "e4a-node-2"},
		},
		{
			name:                  "Several resizable families: experiments partially enabled: pods are schedulable on E4A node with lookahead pods when E4A experiment disabled even if EK experiment enabled",
			maxCandidateNodeCount: 1,
			resizableNodesSnapshot: operationtracker.ResizableNodesSnapshot{
				"ek-node-1":  buildTestResizableNodeObject(2500, 10, 7500, 30),
				"e4a-node-2": buildTestResizableNodeObject(2500, 10, 7500, 30),
			},
			nodes: []*v1.Node{
				node8(machinetypes.EK.Name(), "ek-node-1", 8000, 32),
				node8(machinetypes.E4A.Name(), "e4a-node-2", 8000, 32),
			},
			podsPerNode: map[string][]*v1.Pod{
				"ek-node-1": {
					buildTestPod("ek-node-1-pod", "ek-node-1", 2500, 5),
					buildTestBalloonPod("ek-node-1-balloon-pod", "ek-node-1", 4000, 25),
				},
				"e4a-node-2": {
					buildTestPod("e4a-node-2-pod", "e4a-node-2", 2000, 4),
					lookaheadbuffer.BuildTestLookaheadPod("", 500, 1, machinetypes.E4A.Name(), lookaheadbuffer.WithNode("e4a-node-2")),
					buildTestBalloonPod("e4a-node-2-balloon-pod", "e4a-node-2", 4000, 25),
				},
			},
			nodeNames:               []string{"ek-node-1"},
			wantValidCandidateNodes: []string{"ek-node-1"},
			experimentFlags: map[string]bool{
				experiments.EkPreventScheduleOnLookaheadNodesFlag:  true,
				experiments.E4aPreventScheduleOnLookaheadNodesFlag: false,
			},
		},
		{
			name:                  "Several resizable families: experiments partially enabled: pods are not schedulable on E4A node with lookahead pods when E4A experiment enabled even if EK experiment disabled",
			maxCandidateNodeCount: 1,
			resizableNodesSnapshot: operationtracker.ResizableNodesSnapshot{
				"ek-node-1":  buildTestResizableNodeObject(2500, 10, 7500, 30),
				"e4a-node-2": buildTestResizableNodeObject(2500, 10, 7500, 30),
			},
			nodes: []*v1.Node{
				node8(machinetypes.EK.Name(), "ek-node-1", 8000, 32),
				node8(machinetypes.E4A.Name(), "e4a-node-2", 8000, 32),
			},
			podsPerNode: map[string][]*v1.Pod{
				"ek-node-1": {
					buildTestPod("ek-node-1-pod", "ek-node-1", 2500, 5),
					buildTestBalloonPod("ek-node-1-balloon-pod", "ek-node-1", 4000, 25),
				},
				"e4a-node-2": {
					buildTestPod("e4a-node-2-pod", "e4a-node-2", 2000, 4),
					lookaheadbuffer.BuildTestLookaheadPod("", 500, 1, machinetypes.E4A.Name(), lookaheadbuffer.WithNode("e4a-node-2")),
					buildTestBalloonPod("e4a-node-2-balloon-pod", "e4a-node-2", 4000, 25),
				},
			},
			nodeNames:               []string{"ek-node-1"},
			wantValidCandidateNodes: nil,
			experimentFlags: map[string]bool{
				experiments.EkPreventScheduleOnLookaheadNodesFlag:  false,
				experiments.E4aPreventScheduleOnLookaheadNodesFlag: true,
			},
		},
		{
			name:                  "Several resizable families: destination selection: pod is scheduled on E4A node with lookahead when E4A flag disabled, while EK node with lookahead is skipped due to enabled EK flag",
			maxCandidateNodeCount: 1,
			resizableNodesSnapshot: operationtracker.ResizableNodesSnapshot{
				"ek-node-1":  buildTestResizableNodeObject(2500, 10, 7500, 30),
				"ek-node-2":  buildTestResizableNodeObject(2500, 10, 7500, 30),
				"e4a-node-3": buildTestResizableNodeObject(2500, 10, 7500, 30),
			},
			nodes: []*v1.Node{
				node8(machinetypes.EK.Name(), "ek-node-1", 8000, 32),
				node8(machinetypes.EK.Name(), "ek-node-2", 8000, 32),
				node8(machinetypes.E4A.Name(), "e4a-node-3", 8000, 32),
			},
			podsPerNode: map[string][]*v1.Pod{
				"ek-node-1": {
					buildTestPod("ek-node-1-pod", "ek-node-1", 2500, 5),
					buildTestBalloonPod("ek-node-1-balloon-pod", "ek-node-1", 4000, 25),
				},
				"ek-node-2": {
					buildTestPod("ek-node-2-pod", "ek-node-2", 2000, 4),
					lookaheadbuffer.BuildTestLookaheadPod("", 500, 1, machinetypes.EK.Name(), lookaheadbuffer.WithNode("ek-node-2")),
					buildTestBalloonPod("ek-node-2-balloon-pod", "ek-node-2", 4000, 25),
				},
				"e4a-node-3": {
					buildTestPod("e4a-node-3-pod", "e4a-node-3", 2000, 4),
					lookaheadbuffer.BuildTestLookaheadPod("", 500, 1, machinetypes.E4A.Name(), lookaheadbuffer.WithNode("e4a-node-3")),
					buildTestBalloonPod("e4a-node-3-balloon-pod", "e4a-node-3", 4000, 25),
				},
			},
			nodeNames:               []string{"ek-node-1"},
			wantValidCandidateNodes: []string{"ek-node-1"},
			experimentFlags: map[string]bool{
				experiments.EkPreventScheduleOnLookaheadNodesFlag:  true,
				experiments.E4aPreventScheduleOnLookaheadNodesFlag: false,
			},
		},
	}

	for _, family := range []string{machinetypes.EK.Name(), machinetypes.E4A.Name(), machinetypes.E4.Name()} {
		testCases = append(testCases, []struct {
			name                    string
			maxCandidateNodeCount   int
			resizableNodesSnapshot  operationtracker.ResizableNodesSnapshot
			nodeNames               []string
			nodes                   []*v1.Node
			podsPerNode             map[string][]*v1.Pod
			wantValidCandidateNodes []string
			experimentFlags         map[string]bool
		}{
			{
				name:                  fmt.Sprintf("%s: Not all pods are schedulable", family),
				maxCandidateNodeCount: 1,
				resizableNodesSnapshot: operationtracker.ResizableNodesSnapshot{
					"node-1": buildTestResizableNodeObject(2500, 10, 7500, 30),
					"node-2": buildTestResizableNodeObject(6000, 10, 7500, 30),
					"node-3": buildTestResizableNodeObject(6000, 10, 7500, 30),
				},
				nodes: []*v1.Node{
					node8(family, "node-1", 8000, 32),
					node8(family, "node-2", 8000, 32),
					node8(family, "node-3", 8000, 32),
				},
				podsPerNode: map[string][]*v1.Pod{
					"node-1": {
						buildTestPod("node-1-pod", "node-1", 500, 5),
						buildTestPod("node-2-pod", "node-1", 2000, 5),
						buildTestBalloonPod("node-1-balloon-pod", "node-1", 5000, 20),
					},
					"node-2": {
						buildTestPod("node-2-pod", "node-2", 6000, 10),
						buildTestBalloonPod("node-2-balloon-pod", "node-2", 1500, 20),
					},
					"node-3": {
						buildTestPod("node-3-pod", "node-3", 6000, 10),
						buildTestBalloonPod("node-3-balloon-pod", "node-3", 1500, 20),
					},
				},
				nodeNames:               []string{"node-1"},
				wantValidCandidateNodes: nil,
			},
			{
				name:                  fmt.Sprintf("%s: Pods from some candidates are unschedulable", family),
				maxCandidateNodeCount: 2,
				resizableNodesSnapshot: operationtracker.ResizableNodesSnapshot{
					"node-1": buildTestResizableNodeObject(2500, 10, 7500, 30),
					"node-2": buildTestResizableNodeObject(2500, 10, 7500, 30),
					"node-3": buildTestResizableNodeObject(28000, 10, 32000, 120),
				},
				nodes: []*v1.Node{
					node8(family, "node-1", 8000, 32),
					node8(family, "node-2", 8000, 32),
					node32(family, "node-3", 32000, 128),
				},
				podsPerNode: map[string][]*v1.Pod{
					"node-1": {
						buildTestPod("node-1-pod", "node-1", 2500, 5),
						buildTestBalloonPod("node-1-balloon-pod", "node-1", 4000, 25),
					},
					"node-2": {
						buildTestPod("node-2-pod", "node-2", 2500, 5),
						buildTestBalloonPod("node-2-balloon-pod", "node-2", 4000, 25),
					},
					"node-3": {
						buildTestPod("node-3-pod", "node-3", 28000, 10),
						buildTestBalloonPod("node-3-balloon-pod", "node-3", 4000, 20),
					},
				},
				nodeNames:               []string{"node-1", "node-2"},
				wantValidCandidateNodes: []string{"node-1"},
			},
			{
				name:                  fmt.Sprintf("%s: Lookahead Pod in a candidate makes the candidate node invalid", family),
				maxCandidateNodeCount: 2,
				resizableNodesSnapshot: operationtracker.ResizableNodesSnapshot{
					"node-1": buildTestResizableNodeObject(2500, 10, 7500, 30),
					"node-2": buildTestResizableNodeObject(2500, 10, 7500, 30),
					"node-3": buildTestResizableNodeObject(24000, 10, 30000, 120),
				},
				nodes: []*v1.Node{
					node8(family, "node-1", 8000, 32),
					node8(family, "node-2", 8000, 32),
					node32(family, "node-3", 32000, 128),
				},
				podsPerNode: map[string][]*v1.Pod{
					"node-1": {
						lookaheadbuffer.BuildTestLookaheadPod("", 2500, 5, family, lookaheadbuffer.WithNode("node-1")),
						buildTestBalloonPod("node-1-balloon-pod", "node-1", 4000, 25),
					},
					"node-2": {
						buildTestPod("node-2-pod", "node-2", 2500, 5),
						buildTestBalloonPod("node-2-balloon-pod", "node-2", 4000, 25),
					},
					"node-3": {
						buildTestPod("node-3-pod", "node-3", 24000, 10),
						buildTestBalloonPod("node-3-balloon-pod", "node-3", 6000, 20),
					},
				},
				nodeNames:               []string{"node-1", "node-2"},
				wantValidCandidateNodes: []string{"node-2"},
			},
			{
				name:                  fmt.Sprintf("%s: Pods from all candidates are schedulable", family),
				maxCandidateNodeCount: 2,
				resizableNodesSnapshot: operationtracker.ResizableNodesSnapshot{
					"node-1": buildTestResizableNodeObject(2500, 10, 7500, 30),
					"node-2": buildTestResizableNodeObject(2500, 10, 7500, 30),
					"node-3": buildTestResizableNodeObject(24000, 10, 30000, 120),
				},
				nodes: []*v1.Node{
					node8(family, "node-1", 8000, 32),
					node8(family, "node-2", 8000, 32),
					node32(family, "node-3", 32000, 128),
				},
				podsPerNode: map[string][]*v1.Pod{
					"node-1": {
						buildTestPod("node-1-pod", "node-1", 2500, 5),
						buildTestBalloonPod("node-1-balloon-pod", "node-1", 4000, 25),
					},
					"node-2": {
						buildTestPod("node-2-pod", "node-2", 2500, 5),
						buildTestBalloonPod("node-2-balloon-pod", "node-2", 4000, 25),
					},
					"node-3": {
						buildTestPod("node-3-pod", "node-3", 24000, 10),
						buildTestBalloonPod("node-3-balloon-pod", "node-3", 6000, 20),
					},
				},
				nodeNames:               []string{"node-1", "node-2"},
				wantValidCandidateNodes: []string{"node-1", "node-2"},
			},
			{
				name:                  fmt.Sprintf("%s: Pods are not schedulable on nodes with lookahead pods", family),
				maxCandidateNodeCount: 1,
				resizableNodesSnapshot: operationtracker.ResizableNodesSnapshot{
					"node-1": buildTestResizableNodeObject(2500, 10, 7500, 30),
					"node-2": buildTestResizableNodeObject(2500, 10, 7500, 30),
				},
				nodes: []*v1.Node{
					node8(family, "node-1", 8000, 32),
					node8(family, "node-2", 8000, 32),
				},
				podsPerNode: map[string][]*v1.Pod{
					"node-1": {
						buildTestPod("node-1-pod", "node-1", 2500, 5),
						buildTestBalloonPod("node-1-balloon-pod", "node-1", 4000, 25),
					},
					"node-2": {
						buildTestPod("node-2-pod", "node-2", 2000, 4),
						lookaheadbuffer.BuildTestLookaheadPod("", 500, 1, family, lookaheadbuffer.WithNode("node-2")),
						buildTestBalloonPod("node-2-balloon-pod", "node-2", 4000, 25),
					},
				},
				nodeNames:               []string{"node-1"},
				wantValidCandidateNodes: nil,
			},
			{
				name:                  fmt.Sprintf("%s: Pods are schedulable on nodes with lookahead pods when experiment is disabled", family),
				maxCandidateNodeCount: 1,
				resizableNodesSnapshot: operationtracker.ResizableNodesSnapshot{
					"node-1": buildTestResizableNodeObject(2500, 10, 7500, 30),
					"node-2": buildTestResizableNodeObject(2500, 10, 7500, 30),
				},
				nodes: []*v1.Node{
					node8(family, "node-1", 8000, 32),
					node8(family, "node-2", 8000, 32),
				},
				podsPerNode: map[string][]*v1.Pod{
					"node-1": {
						buildTestPod("node-1-pod", "node-1", 2500, 5),
						buildTestBalloonPod("node-1-balloon-pod", "node-1", 4000, 25),
					},
					"node-2": {
						buildTestPod("node-2-pod", "node-2", 2000, 4),
						lookaheadbuffer.BuildTestLookaheadPod("", 500, 1, family, lookaheadbuffer.WithNode("node-2")),
						buildTestBalloonPod("node-2-balloon-pod", "node-2", 4000, 25),
					},
				},
				nodeNames:               []string{"node-1"},
				wantValidCandidateNodes: []string{"node-1"},
				experimentFlags: map[string]bool{
					processor.PreventScheduleOnLookaheadExperimentFlags[family]: false,
				},
			},
		}...)
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cs := testsnapshot.NewTestSnapshotOrDie(t)
			for _, node := range tc.nodes {
				assert.NoError(t, cs.AddNodeInfo(framework.NewTestNodeInfo(node, tc.podsPerNode[node.Name]...)))
			}
			ctx := &context.AutoscalingContext{ClusterSnapshot: cs}

			resizableVmManager := &mockResizableVmManager{}
			resizableVmManager.On("FilteredNodesSnapshot", false, operationtracker.ResizableOnly).Return(tc.resizableNodesSnapshot)

			plugin := NewPlugin(config.PluginsConfig{
				ResizableVmManager:    resizableVmManager,
				ExperimentsManager:    experiments.NewMockManagerWithOptions(version.Version{}, tc.experimentFlags, nil),
				MaxCandidateNodeCount: tc.maxCandidateNodeCount,
			})

			assert.Equal(t, tc.wantValidCandidateNodes, plugin.ValidCandidateNodes(ctx, tc.nodeNames))
		})
	}
}

func buildTestNode(name string, cpu, memory int64) *v1.Node {
	return test.BuildTestNode(name, cpu, memory*giBToBytes)
}

func node8(family, name string, cpu, memory int64) *v1.Node {
	switch family {
	case machinetypes.EK.Name():
		return ekvms_test.EkNode8(name, cpu, memory*giBToBytes)
	case machinetypes.E4A.Name():
		return ekvms_test.E4aNode8(name, cpu, memory*giBToBytes)
	case machinetypes.E4.Name():
		return ekvms_test.E4Node8(name, cpu, memory*giBToBytes)
	default:
		panic("unknown machine family: " + family)
	}
}

func node32(family, name string, cpu, memory int64) *v1.Node {
	switch family {
	case machinetypes.EK.Name():
		return ekvms_test.EkNode32(name, cpu, memory*giBToBytes)
	case machinetypes.E4A.Name():
		return ekvms_test.E4aNode32(name, cpu, memory*giBToBytes)
	case machinetypes.E4.Name():
		return ekvms_test.E4Node32(name, cpu, memory*giBToBytes)
	default:
		panic("unknown machine family: " + family)
	}
}

func buildTestResizableNodeObject(desiredCPU, desiredMem, upsizableMaxCPU, upsizableMaxMem int64) operationtracker.ResizableNode {
	return operationtracker.ResizableNode{
		DesiredSize: size.Allocatable{
			MilliCpus: desiredCPU,
			KBytes:    desiredMem * giBToKiB,
		},
		UpsizableMaxSize: size.Allocatable{
			MilliCpus: upsizableMaxCPU,
			KBytes:    upsizableMaxMem * giBToKiB,
		},
		PhysicalMaxSize: size.Allocatable{
			MilliCpus: upsizableMaxCPU,
			KBytes:    upsizableMaxMem * giBToKiB,
		},
	}
}

func buildTestPod(name, nodeName string, cpu, mem int64) *v1.Pod {
	return test.BuildTestPod(name, cpu, mem*giBToBytes, func(pod *v1.Pod) {
		pod.Spec.NodeName = nodeName
	})
}

func buildTestBalloonPod(name, nodeName string, cpu, mem int64) *v1.Pod {
	return test.BuildTestPod(name, cpu, mem*giBToBytes, func(pod *v1.Pod) {
		pod.ObjectMeta.Name = fmt.Sprintf("gke-system-balloon-pod-%s", name)
		pod.ObjectMeta.Namespace = "kube-system"
		pod.Spec.NodeName = nodeName
	})
}

type mockResizableVmManager struct {
	mock.Mock
	operationtracker.Manager
}

func (m *mockResizableVmManager) FilteredNodesSnapshot(forceRefresh bool, mode operationtracker.SnapshotFilterMode) operationtracker.ResizableNodesSnapshot {
	args := m.Called(forceRefresh, mode)
	return args.Get(0).(operationtracker.ResizableNodesSnapshot)
}
