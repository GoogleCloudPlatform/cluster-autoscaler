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
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	testprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/utils/annotations"
	"k8s.io/autoscaler/cluster-autoscaler/utils/drain"

	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/eligibility"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/pdb"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability/rules"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/options"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
)

func TestNewValidCandidateNodes(t *testing.T) {
	onlyNXNodesProcessor := &mockScaleDownNodeProcessor{
		candidatesFilter: func(nodes []*apiv1.Node) []*apiv1.Node {
			var result []*apiv1.Node
			for _, n := range nodes {
				if strings.HasPrefix(n.Name, "n") {
					result = append(result, n)
				}
			}
			return result
		},
	}

	pdbs := []*v1.PodDisruptionBudget{
		buildPdb("pdb-remaining", 1),
		buildPdb("pdb-missing", 0),
	}
	pdbTracker := pdb.NewBasicRemainingPdbTracker()
	err := pdbTracker.SetPdbs(pdbs)
	assert.NoError(t, err)

	testCases := []struct {
		name              string
		nodesWithPods     map[*apiv1.Node][]*apiv1.Pod
		allCandidateNodes []string
		wantNodes         []string
	}{
		{
			name: "no nodes",
		},
		{
			name: "some valid nodes",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 1): {},
				buildReadyNode("n2", 1000, 1): {
					test.SetRSPodSpec(test.BuildScheduledTestPod("p1", 100, 1, "n2"), "rs"),
				},
				buildReadyNode("n3", 1000, 1): {
					setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p2", 100, 1, "n2"), "rs"), "pdb-remaining"),
				},
			},
			wantNodes: []string{"n1", "n2", "n3"},
		},
		{
			name: "some valid candidate nodes",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 1): {},
				buildReadyNode("n2", 1000, 1): {
					test.SetRSPodSpec(test.BuildScheduledTestPod("p1", 100, 1, "n2"), "rs"),
				},
				buildReadyNode("n3", 1000, 1): {
					setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p2", 100, 1, "n2"), "rs"), "pdb-remaining"),
				},
			},
			allCandidateNodes: []string{"n1"},
			wantNodes:         []string{"n2", "n3"},
		},
		{
			name: "various nodes",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 1): {},
				buildReadyNode("n2", 1000, 1): {
					test.SetRSPodSpec(test.BuildScheduledTestPod("p1", 100, 1, "n2"), "rs"),
				},
				buildReadyNode("n3", 1000, 1): {
					setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p2", 100, 1, "n3"), "rs"), "pdb-remaining"),
				},
				test.BuildTestNode("n4", 1000, 1):      {},
				buildUpcomingNode("n5", 1000, 1):       {},
				buildDuringDeletionNode("n6", 1000, 1): {},
				buildReadyNode("n7", 1000, 1): {
					test.BuildScheduledTestPod("p3", 100, 1, "n7"),
				},
				buildReadyNode("n8", 1000, 1): {
					setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p4", 100, 1, "n2"), "rs"), "pdb-missing"),
				},
				buildReadyNode("m1", 1000, 1): {},
			},
			allCandidateNodes: []string{"n3"},
			wantNodes:         []string{"n1", "n2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &context.AutoscalingContext{
				ClusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t),
				CloudProvider:   testprovider.NewTestCloudProviderBuilder().Build(),
			}
			for node, pods := range tc.nodesWithPods {
				assert.NoError(t, ctx.ClusterSnapshot.AddNodeInfo(framework.NewTestNodeInfo(node, pods...)))
			}

			allCandidateNodes := make(map[string]bool)
			for _, node := range tc.allCandidateNodes {
				allCandidateNodes[node] = true
			}

			factory := newDefragNodeFilterFactory(onlyNXNodesProcessor, options.NodeDeleteOptions{}, rules.Default(options.NodeDeleteOptions{}))
			nodeFilter, err := factory.NewDefragNodeFilter(ctx)
			assert.NoError(t, err)

			gotNodes, err := nodeFilter.newValidCandidateNodes(ctx, pdbTracker, allCandidateNodes)
			assert.NoError(t, err)
			assert.ElementsMatch(t, tc.wantNodes, gotNodes)
		})
	}
}

func TestFilterInvalidCandidateNodes(t *testing.T) {
	onlyNXNodesProcessor := &mockScaleDownNodeProcessor{
		candidatesFilter: func(nodes []*apiv1.Node) []*apiv1.Node {
			var result []*apiv1.Node
			for _, n := range nodes {
				if strings.HasPrefix(n.Name, "n") {
					result = append(result, n)
				}
			}
			return result
		},
	}

	pdbs := []*v1.PodDisruptionBudget{
		buildPdb("pdb-remaining", 1),
		buildPdb("pdb-missing", 0),
	}
	pdbTracker := pdb.NewBasicRemainingPdbTracker()
	err := pdbTracker.SetPdbs(pdbs)
	assert.NoError(t, err)

	testCases := []struct {
		name          string
		nodesWithPods map[*apiv1.Node][]*apiv1.Pod
		candidate     *defrag.Candidate
		wantNodes     []string
	}{
		{
			name:      "candidate without nodes",
			candidate: &defrag.Candidate{},
		},
		{
			name: "candidate with valid nodes",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 1): {},
				buildReadyNode("n2", 1000, 1): {
					test.SetRSPodSpec(test.BuildScheduledTestPod("p1", 100, 1, "n2"), "rs"),
				},
				buildReadyNode("n3", 1000, 1): {
					setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p2", 100, 1, "n2"), "rs"), "pdb-remaining"),
				},
			},
			candidate: &defrag.Candidate{Nodes: []string{"n1", "n2", "n3"}},
			wantNodes: []string{"n1", "n2", "n3"},
		},
		{
			name: "candidate with invalid and deleted nodes",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 1000, 1):      {},
				buildUpcomingNode("n2", 1000, 1):       {},
				buildDuringDeletionNode("n3", 1000, 1): {},
				buildReadyNode("n4", 1000, 1): {
					test.BuildScheduledTestPod("p1", 100, 1, "n4"),
				},
				buildReadyNode("n5", 1000, 1): {
					setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p2", 100, 1, "n2"), "rs"), "pdb-missing"),
				},
				buildReadyNode("m1", 1000, 1): {},
			},
			candidate: &defrag.Candidate{Nodes: []string{"n1", "n2", "n3", "n4", "n5", "m1"}},
		},
		{
			name: "candidate with various nodes",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 1): {},
				buildReadyNode("n2", 1000, 1): {
					test.SetRSPodSpec(test.BuildScheduledTestPod("p1", 100, 1, "n2"), "rs"),
				},
				buildReadyNode("n3", 1000, 1): {
					setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p2", 100, 1, "n2"), "rs"), "pdb-remaining"),
				},
				test.BuildTestNode("n4", 1000, 1):      {},
				buildUpcomingNode("n5", 1000, 1):       {},
				buildDuringDeletionNode("n6", 1000, 1): {},
				buildReadyNode("n7", 1000, 1): {
					test.BuildScheduledTestPod("p3", 100, 1, "n6"),
				},
				buildReadyNode("n8", 1000, 1): {
					setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p4", 100, 1, "n2"), "rs"), "pdb-missing"),
				},
				buildReadyNode("m1", 1000, 1): {},
			},
			candidate: &defrag.Candidate{Nodes: []string{"n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "m1"}},
			wantNodes: []string{"n1", "n2", "n3"},
		},
		{
			name: "candidate with invalid nodes according to plugin",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 1): {},
				buildReadyNode("n2", 1000, 1): {
					test.SetRSPodSpec(test.BuildScheduledTestPod("p1", 100, 1, "n2"), "rs"),
				},
				buildReadyNode("n3", 1000, 1): {
					setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p2", 100, 1, "n2"), "rs"), "pdb-remaining"),
				},
			},
			candidate: &defrag.Candidate{
				Nodes: []string{"n1", "n2", "n3"},
				Plugin: &fakePlugin{
					validFilter: func(nodeName string) bool {
						return nodeName != "n2"
					},
				},
			},
			wantNodes: []string{"n1", "n3"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &context.AutoscalingContext{
				ClusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t),
				CloudProvider:   testprovider.NewTestCloudProviderBuilder().Build(),
			}
			for node, pods := range tc.nodesWithPods {
				assert.NoError(t, ctx.ClusterSnapshot.AddNodeInfo(framework.NewTestNodeInfo(node, pods...)))
			}

			if tc.candidate.Plugin == nil {
				tc.candidate.Plugin = &fakePlugin{}
			}

			deleteOpts := options.NodeDeleteOptions{}
			factory := newDefragNodeFilterFactory(onlyNXNodesProcessor, deleteOpts, rules.Default(deleteOpts))
			nodeFilter, err := factory.NewDefragNodeFilter(ctx)
			assert.NoError(t, err)
			nodeFilter.filterInvalidCandidateNodes(ctx, pdbTracker, tc.candidate)
			assert.Equal(t, tc.wantNodes, tc.candidate.Nodes)
		})
	}
}

func TestIsCandidateNodeValid(t *testing.T) {
	allNodesProcessor := &mockScaleDownNodeProcessor{
		candidatesFilter: func(nodes []*apiv1.Node) []*apiv1.Node {
			assert.Equal(t, 1, len(nodes))
			return nodes
		},
	}

	noNodesProcessor := &mockScaleDownNodeProcessor{
		candidatesFilter: func(nodes []*apiv1.Node) []*apiv1.Node {
			assert.Equal(t, 1, len(nodes))
			return nil
		},
	}

	testUid := "1000"
	testLabelKey1 := "testKey1"
	testLabels := map[string]string{testLabelKey1: "true"}

	testCases := []struct {
		name          string
		processor     *mockScaleDownNodeProcessor
		node          *apiv1.Node
		pods          []*apiv1.Pod
		defragEnabled bool
		want          bool
	}{
		{
			name:      "node during deletion",
			processor: allNodesProcessor,
			node:      buildDuringDeletionNode("n", 1000, 1),
			want:      false,
		},
		{
			name:      "unready node",
			processor: allNodesProcessor,
			node:      test.BuildTestNode("n", 1000, 1),
			want:      false,
		},
		{
			name:      "upcoming node",
			processor: allNodesProcessor,
			node:      buildUpcomingNode("n", 1000, 1),
			want:      false,
		},
		{
			name:      "no scale down node",
			processor: allNodesProcessor,
			node:      buildNoScaleDownNode("n", 1000, 1),
			want:      false,
		},
		{
			name:      "node filtered by ScaleDownNodeProcessor",
			processor: noNodesProcessor,
			node:      buildReadyNode("n", 1000, 1),
			want:      false,
		},
		{
			name:      "valid node",
			processor: allNodesProcessor,
			node:      buildReadyNode("n", 1000, 1),
			want:      true,
		},
		{
			name:      "kube-system pods with defrag enabled",
			processor: allNodesProcessor,
			node:      buildReadyNode("n", 1000, 1),
			pods: []*apiv1.Pod{
				createTestPod("p1", testUid, "kube-system", "n", map[string]string{}, testLabels, time.Now().Add(-2*time.Hour)),
			},
			defragEnabled: true,
			want:          true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &context.AutoscalingContext{
				AutoscalingKubeClients: context.AutoscalingKubeClients{
					ListerRegistry: kubernetes.NewListerRegistry(nil, nil, nil, kubernetes.NewTestPodDisruptionBudgetLister(nil), nil, nil, nil, nil, nil),
				},
				ClusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t),
				CloudProvider:   testprovider.NewTestCloudProviderBuilder().Build(),
			}
			assert.NoError(t, ctx.ClusterSnapshot.AddNodeInfo(framework.NewTestNodeInfo(tc.node, tc.pods...)))

			deleteOpts := options.NodeDeleteOptions{}
			factory := newDefragNodeFilterFactory(tc.processor, deleteOpts, rules.Default(deleteOpts))
			nodeFilter, err := factory.NewDefragNodeFilter(ctx)
			assert.NoError(t, err)

			nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(tc.node.Name)
			assert.NoError(t, err)
			assert.Equal(t, tc.want, nodeFilter.isCandidateNodeValid(ctx, nodeInfo))
		})
	}
}

func TestHasBlockingPods(t *testing.T) {
	allNodesProcessor := &mockScaleDownNodeProcessor{
		candidatesFilter: func(nodes []*apiv1.Node) []*apiv1.Node {
			assert.Equal(t, 1, len(nodes))
			return nodes
		},
	}

	testCases := []struct {
		name      string
		processor *mockScaleDownNodeProcessor
		node      *apiv1.Node
		pods      []*apiv1.Pod
		pdbs      []*v1.PodDisruptionBudget
		want      bool
	}{
		{
			name:      "replicated pod",
			processor: allNodesProcessor,
			node:      buildReadyNode("n", 1000, 1),
			pods: []*apiv1.Pod{
				test.SetRSPodSpec(test.BuildScheduledTestPod("p", 100, 1, "n"), "rs"),
			},
			want: false,
		},
		{
			name:      "blocking non-replicated pods",
			processor: allNodesProcessor,
			node:      buildReadyNode("n", 1000, 1),
			pods: []*apiv1.Pod{
				test.BuildScheduledTestPod("p", 100, 1, "n"),
			},
			want: true,
		},
		{
			name:      "pdb with remaining disruptions",
			processor: allNodesProcessor,
			node:      buildReadyNode("n", 1000, 1),
			pods: []*apiv1.Pod{
				setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p1", 100, 1, "n"), "rs"), "label"),
				setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p2", 100, 1, "n"), "rs"), "label"),
			},
			pdbs: []*v1.PodDisruptionBudget{
				buildPdb("label", 1),
			},
			want: false,
		},
		{
			name:      "pdb without remaining disruptions",
			processor: allNodesProcessor,
			node:      buildReadyNode("n", 1000, 1),
			pods: []*apiv1.Pod{
				setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p1", 100, 1, "n"), "rs"), "label"),
				setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p2", 100, 1, "n"), "rs"), "label"),
			},
			pdbs: []*v1.PodDisruptionBudget{
				buildPdb("label", 0),
			},
			want: true,
		},
		{
			name:      "pdb with various remaining disruptions",
			processor: allNodesProcessor,
			node:      buildReadyNode("n", 1000, 1),
			pods: []*apiv1.Pod{
				setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p1", 100, 1, "n"), "rs"), "label"),
				setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p2", 100, 1, "n"), "rs"), "label"),
			},
			pdbs: []*v1.PodDisruptionBudget{
				buildPdb("label", 1),
				buildPdb("label", 0),
			},
			want: true,
		},
		{
			name:      "on-completion pod",
			processor: allNodesProcessor,
			node:      buildReadyNode("n", 1000, 1),
			pods: []*apiv1.Pod{
				buildOnCompletionPod("p", "n"),
			},
			want: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &context.AutoscalingContext{
				ClusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t),
				CloudProvider:   testprovider.NewTestCloudProviderBuilder().Build(),
			}
			assert.NoError(t, ctx.ClusterSnapshot.AddNodeInfo(framework.NewTestNodeInfo(tc.node, tc.pods...)))

			deleteOpts := options.NodeDeleteOptions{}
			factory := newDefragNodeFilterFactory(tc.processor, deleteOpts, rules.Default(deleteOpts))
			nodeFilter, err := factory.NewDefragNodeFilter(ctx)
			assert.NoError(t, err)

			pdbTracker := pdb.NewBasicRemainingPdbTracker()
			err = pdbTracker.SetPdbs(tc.pdbs)
			assert.NoError(t, err)

			nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(tc.node.Name)
			assert.NoError(t, err)
			assert.Equal(t, tc.want, nodeFilter.hasBlockingPods(nodeInfo, ctx, pdbTracker))
		})
	}
}

func TestFilterNodesViolatingMinSize(t *testing.T) {

	ng_nonexistent := test.BuildTestNode("ng-nonexistent", 1000, 1000)

	provider := testprovider.NewTestCloudProviderBuilder().Build()
	ng1_nodes := buildNodeGroup(provider, "ng1", 3, 5)
	ng2_nodes := buildNodeGroup(provider, "ng2", 2, 3)
	ng3_nodes := buildNodeGroup(provider, "ng3", 0, 3)
	ng4_nodes := buildNodeGroup(provider, "ng4", 1, 1)

	allNodes := slices.Concat(ng1_nodes, ng2_nodes, ng3_nodes, ng4_nodes)

	testCases := []struct {
		name               string
		candidateInfos     []*candidateInfo
		actuationStatus    fakeActuationStatus
		wantCandidateInfos []*candidateInfo
	}{
		{
			name: "one group cumulative skip ng1_n5",
			// ng1 size: 5, min: 3
			candidateInfos: []*candidateInfo{
				{
					candidate: &defrag.Candidate{
						Nodes: []string{ng1_nodes[2].Name, ng1_nodes[3].Name, ng1_nodes[4].Name},
					},
				},
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate: &defrag.Candidate{
						Nodes: []string{ng1_nodes[2].Name, ng1_nodes[3].Name},
					},
				},
			},
		},
		{
			name: "mixed groups cumulative skip, ng2_n3 and ng1_n5",
			// ng1 size: 5, min: 3 | ng2 size: 3, min: 2
			candidateInfos: []*candidateInfo{
				{
					candidate: &defrag.Candidate{
						Nodes: []string{ng1_nodes[2].Name, ng2_nodes[1].Name, ng1_nodes[3].Name, ng2_nodes[2].Name, ng1_nodes[4].Name},
					},
				},
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate: &defrag.Candidate{
						Nodes: []string{ng1_nodes[2].Name, ng2_nodes[1].Name, ng1_nodes[3].Name},
					},
				},
			},
		},
		{
			name: "skip nodes across multiple candidates, skip ng2_n3, ng1_n5 and ng4_n1",
			// ng1 size: 5, min: 3 | ng2 size: 3, min: 2 | ng3 size: 3, min: 0
			candidateInfos: []*candidateInfo{
				{
					candidate: &defrag.Candidate{
						Nodes: []string{ng1_nodes[2].Name, ng2_nodes[1].Name, ng1_nodes[3].Name},
					},
				},
				{
					candidate: &defrag.Candidate{
						Nodes: []string{ng2_nodes[2].Name, ng3_nodes[1].Name},
					},
				},
				{
					candidate: &defrag.Candidate{
						Nodes: []string{ng1_nodes[4].Name, ng3_nodes[2].Name},
					},
				},
				{
					candidate: &defrag.Candidate{
						Nodes: []string{ng4_nodes[0].Name},
					},
				},
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate: &defrag.Candidate{
						Nodes: []string{ng1_nodes[2].Name, ng2_nodes[1].Name, ng1_nodes[3].Name},
					},
				},
				{
					candidate: &defrag.Candidate{
						Nodes: []string{ng3_nodes[1].Name},
					},
				},
				{
					candidate: &defrag.Candidate{
						Nodes: []string{ng3_nodes[2].Name},
					},
				},
				{
					candidate: &defrag.Candidate{},
				},
			},
		},
		{
			name: "ng2 has 1 node in deletion, skip ng2_n2",
			// ng1 size: 5, min: 3 | ng2 size: 3, min: 2
			candidateInfos: []*candidateInfo{
				{
					candidate: &defrag.Candidate{
						Nodes: []string{ng1_nodes[2].Name, ng2_nodes[1].Name, ng1_nodes[3].Name},
					},
				},
			},
			actuationStatus: fakeActuationStatus{
				deletionsCountsByGroup: map[string]int{
					"ng2": 1,
				},
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate: &defrag.Candidate{
						Nodes: []string{ng1_nodes[2].Name, ng1_nodes[3].Name},
					},
				},
			},
		},
		{
			name: "node group not found",
			candidateInfos: []*candidateInfo{
				{
					candidate: &defrag.Candidate{
						Nodes: []string{ng1_nodes[2].Name, ng_nonexistent.Name, ng1_nodes[3].Name},
					},
				},
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate: &defrag.Candidate{
						Nodes: []string{ng1_nodes[2].Name, ng1_nodes[3].Name},
					},
				},
			},
		},
		{
			name:           "empty list",
			candidateInfos: []*candidateInfo{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scaleDownActuator := &mockScaleDownActuator{}
			scaleDownActuator.On("CheckStatus").Return(&tc.actuationStatus)

			ctx := &context.AutoscalingContext{
				CloudProvider:     provider,
				ClusterSnapshot:   testsnapshot.NewTestSnapshotOrDie(t),
				ScaleDownActuator: scaleDownActuator,
			}

			for _, node := range allNodes {
				node.Spec.ProviderID = fmt.Sprintf("test://%s", node.Name)
				assert.NoError(t, ctx.ClusterSnapshot.AddNodeInfo(framework.NewTestNodeInfo(node)))
			}

			deleteOpts := options.NodeDeleteOptions{}
			processor := &mockScaleDownNodeProcessor{
				candidatesFilter: func(nodes []*apiv1.Node) []*apiv1.Node {
					assert.Equal(t, 12, len(nodes))
					return nodes
				},
			}
			factory := newDefragNodeFilterFactory(processor, deleteOpts, rules.Default(deleteOpts))
			nodeFilter, err := factory.NewDefragNodeFilter(ctx)
			assert.NoError(t, err)

			for idx, candidateInfo := range tc.candidateInfos {
				resultNames := nodeFilter.filterNodesViolatingMinSize(ctx, candidateInfo.candidate.Nodes)
				assert.Equal(t, tc.wantCandidateInfos[idx].candidate.Nodes, resultNames, "Result nodes for removal do not match expected list")
			}
		})
	}
}

func createTestPod(name, uid, namespace, nodeName string, annotations, labels map[string]string, creationTimestamp time.Time) *apiv1.Pod {
	isController := true
	pod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:               types.UID(uid),
			Namespace:         namespace,
			Name:              name,
			Annotations:       annotations,
			CreationTimestamp: metav1.NewTime(creationTimestamp),
			Labels:            labels,
			OwnerReferences:   []metav1.OwnerReference{{UID: types.UID(uid), Kind: "ReplicaSet", Controller: &isController}},
		},
		Spec: apiv1.PodSpec{
			NodeName: nodeName,
			Containers: []apiv1.Container{
				{
					Resources: apiv1.ResourceRequirements{
						Requests: apiv1.ResourceList{},
					},
				},
			},
		},
	}
	return pod
}

func buildReadyNode(name string, cpu, mem int64) *apiv1.Node {
	node := test.BuildTestNode(name, cpu, mem)
	node.Status.Conditions = append(node.Status.Conditions, apiv1.NodeCondition{Type: apiv1.NodeReady})
	return node
}

func buildUpcomingNode(name string, cpu, mem int64) *apiv1.Node {
	node := buildReadyNode(name, cpu, mem)
	if node.Annotations == nil {
		node.Annotations = make(map[string]string)
	}
	node.Annotations[annotations.NodeUpcomingAnnotation] = "true"
	return node
}

func buildDuringDeletionNode(name string, cpu, mem int64) *apiv1.Node {
	node := buildReadyNode(name, cpu, mem)
	node.Spec.Taints = append(node.Spec.Taints, apiv1.Taint{
		Key:    taints.ToBeDeletedTaint,
		Value:  fmt.Sprint(time.Now().Unix()),
		Effect: apiv1.TaintEffectNoSchedule,
	})
	return node
}

func buildNoScaleDownNode(name string, cpu, mem int64) *apiv1.Node {
	node := buildReadyNode(name, cpu, mem)
	if node.Annotations == nil {
		node.Annotations = make(map[string]string)
	}
	node.Annotations[eligibility.ScaleDownDisabledKey] = "true"
	return node
}

func buildNodeGroup(provider *testprovider.TestCloudProvider, groupName string, minSize, numNodes int) (nodes []*apiv1.Node) {
	maxSize := 10
	nodes = make([]*apiv1.Node, numNodes)
	provider.AddNodeGroup(groupName, minSize, maxSize, numNodes)
	for i := range numNodes {
		newNode := test.BuildTestNode(fmt.Sprintf("%s-%d", groupName, i), 1000, 1000)
		nodes[i] = newNode
		provider.AddNode(groupName, newNode)
	}
	return nodes
}

func setPodLabel(pod *apiv1.Pod, label string) *apiv1.Pod {
	if pod.ObjectMeta.Labels == nil {
		pod.ObjectMeta.Labels = make(map[string]string)
	}
	pod.ObjectMeta.Labels[label] = "true"
	return pod
}

func buildOnCompletionPod(name, node string) *apiv1.Pod {
	pod := test.BuildScheduledTestPod(name, 100, 1, node)
	pod.Annotations[drain.PodSafeToEvictKey] = drain.PodSafeToEvictOnCompletionValue
	return pod
}

func buildPdb(label string, remaining int32) *v1.PodDisruptionBudget {
	zero := intstr.FromInt(0)
	return &v1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pdb",
			Namespace: "default",
		},
		Spec: v1.PodDisruptionBudgetSpec{
			MinAvailable: &zero,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					label: "true",
				},
			},
		},
		Status: v1.PodDisruptionBudgetStatus{
			DisruptionsAllowed: remaining,
		},
	}
}

type mockScaleDownNodeProcessor struct {
	candidatesFilter func([]*apiv1.Node) []*apiv1.Node
}

func (p *mockScaleDownNodeProcessor) GetPodDestinationCandidates(*context.AutoscalingContext, []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	return nil, nil
}

func (p *mockScaleDownNodeProcessor) GetScaleDownCandidates(_ *context.AutoscalingContext, nodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	return p.candidatesFilter(nodes), nil
}

func (p *mockScaleDownNodeProcessor) CleanUp() {}
