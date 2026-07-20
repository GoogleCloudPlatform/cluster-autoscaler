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

package daemonset

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	testprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/config"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/testutil"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/controller/daemon"
	clocktesting "k8s.io/utils/clock/testing"
)

func TestDaemonSetPluginNewCandidate(t *testing.T) {
	ds := newDaemonSet("ds", 1000, 1, nil)
	crdLabel := "test-crd"
	gkeManager := &gke.GkeManagerMock{}
	gkeManager.On("GetMigTemplateNodeInfo", mock.Anything).Return(framework.NewTestNodeInfo(&apiv1.Node{}), nil)

	testCases := []struct {
		name                      string
		pods                      []*apiv1.Pod
		listerErr                 error
		nodesWithPods             map[*apiv1.Node][]*apiv1.Pod
		nodeGroups                []testutil.ExtendedNodeGroup
		autopilotEnabled          bool
		crds                      []crd.CRD
		nodeNames                 []string
		maxCandidateNodesCount    int
		wantNodes                 []string
		wantLatestUnfitNodesCount int
	}{
		{
			name: "no daemon set expected, no candidate",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 1000, 10): {},
				test.BuildTestNode("n2", 1000, 10): {},
			},
			autopilotEnabled: true,
			nodeNames:        []string{"n1", "n2"},
		},
		{
			name: "all nodes cannot schedule DS - exceeds max candidate nodes count",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				withNonEKLabel(test.BuildTestNode("n1", 500, 10)): {},
				withNonEKLabel(test.BuildTestNode("n2", 500, 10)): {},
			},
			pods: []*apiv1.Pod{
				newDSPod(ds, "n1", test.MarkUnschedulable()),
				newDSPod(ds, "n2", test.MarkUnschedulable()),
			},
			autopilotEnabled:          true,
			nodeNames:                 []string{"n1", "n2"},
			maxCandidateNodesCount:    1,
			wantNodes:                 []string{"n1"},
			wantLatestUnfitNodesCount: 2,
		},
		{
			name: "all nodes cannot schedule DS",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 500, 10): {},
				test.BuildTestNode("n2", 500, 10): {},
			},
			pods: []*apiv1.Pod{
				newDSPod(ds, "n1", test.MarkUnschedulable()),
				newDSPod(ds, "n2", test.MarkUnschedulable()),
			},
			autopilotEnabled:          true,
			nodeNames:                 []string{"n1", "n2"},
			wantNodes:                 []string{"n1", "n2"},
			wantLatestUnfitNodesCount: 2,
		},
		{
			name: "all EK nodes cannot schedule DS, enter grace period",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				withEKLabel(test.BuildTestNode("n1", 500, 10)): {},
				withEKLabel(test.BuildTestNode("n2", 500, 10)): {},
			},
			pods: []*apiv1.Pod{
				newDSPod(ds, "n1", test.MarkUnschedulable()),
				newDSPod(ds, "n2", test.MarkUnschedulable()),
			},
			autopilotEnabled:          true,
			nodeNames:                 []string{"n1", "n2"},
			wantNodes:                 nil, // Does not become a candidate immediately
			wantLatestUnfitNodesCount: 0,
		},
		{
			name: "Non-EK node cannot schedule DS, becomes candidate immediately",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				withNonEKLabel(test.BuildTestNode("n1", 500, 10)): {},
				withNonEKLabel(test.BuildTestNode("n2", 500, 10)): {},
			},
			pods: []*apiv1.Pod{
				newDSPod(ds, "n1", test.MarkUnschedulable()),
				newDSPod(ds, "n2", test.MarkUnschedulable()),
			},
			autopilotEnabled:          true,
			nodeNames:                 []string{"n1", "n2"},
			wantNodes:                 []string{"n1", "n2"}, // Becomes candidate immediately
			wantLatestUnfitNodesCount: 2,
		},
		{
			name: "Missing label node cannot schedule DS, becomes candidate immediately",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 500, 10): {},
			},
			pods: []*apiv1.Pod{
				newDSPod(ds, "n1", test.MarkUnschedulable()),
			},
			autopilotEnabled:          true,
			nodeNames:                 []string{"n1"},
			wantNodes:                 []string{"n1"}, // Becomes candidate immediately
			wantLatestUnfitNodesCount: 1,
		},
		{
			name: "single daemon set expected, some nodes cannot scheduled DS",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				withNonEKLabel(test.BuildTestNode("n1", 1000, 10)): {},
				withNonEKLabel(test.BuildTestNode("n2", 500, 10)):  {},
			},
			pods: []*apiv1.Pod{
				newDSPod(ds, "n1", test.WithNodeName("n1")),
				newDSPod(ds, "n2", test.MarkUnschedulable()),
			},
			autopilotEnabled:          true,
			nodeNames:                 []string{"n1", "n2"},
			wantNodes:                 []string{"n2"},
			wantLatestUnfitNodesCount: 1,
		},
		{
			name: "single daemon set expected, all nodes can schedule DS",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 1000, 10): {},
				test.BuildTestNode("n2", 1000, 10): {},
			},
			pods: []*apiv1.Pod{
				newDSPod(ds, "n1", test.WithNodeName("n1")),
				newDSPod(ds, "n2", test.WithNodeName("n2")),
			},
			autopilotEnabled: true,
			nodeNames:        []string{"n1", "n2"},
		},
		{
			name: "pods lister error",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 500, 10): {},
				test.BuildTestNode("n2", 500, 10): {},
			},
			listerErr:        errors.New("error"),
			autopilotEnabled: true,
			nodeNames:        []string{"n1", "n2"},
		},
		{
			name: "no possible candidates",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 500, 10): {},
				test.BuildTestNode("n2", 500, 10): {},
			},
			pods: []*apiv1.Pod{
				newDSPod(ds, "n1", test.MarkUnschedulable()),
				newDSPod(ds, "n2", test.MarkUnschedulable()),
			},
			autopilotEnabled: true,
			nodeNames:        []string{},
		},
		{
			name: "autopilot disabled, no possible candidates",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 500, 10): {},
				test.BuildTestNode("n2", 500, 10): {},
			},
			pods: []*apiv1.Pod{
				newDSPod(ds, "n1", test.MarkUnschedulable()),
				newDSPod(ds, "n2", test.MarkUnschedulable()),
			},
			autopilotEnabled:          false,
			nodeNames:                 []string{"n1", "n2"},
			wantNodes:                 []string{},
			wantLatestUnfitNodesCount: 0,
		},
		{
			name: "configured via ccc, no node can schedule DS, defrag runs on CCC with proper config",
			nodeGroups: []testutil.ExtendedNodeGroup{
				{
					Name: "ng-1",
					Nodes: []*apiv1.Node{
						withNonEKLabel(test.BuildTestNode("n1", 500, 10)),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							crdLabel: "ccc",
						},
					},
				},
				{
					Name: "ng-2",
					Nodes: []*apiv1.Node{
						withNonEKLabel(test.BuildTestNode("n2", 500, 10)),
					},
					Spec: &gkeclient.NodePoolSpec{
						Labels: map[string]string{
							crdLabel: "ccc-2",
						},
					},
				},
				{
					Name: "ng-3",
					Nodes: []*apiv1.Node{
						withNonEKLabel(test.BuildTestNode("n3", 500, 10)),
					},
					Spec: &gkeclient.NodePoolSpec{},
				},
			},
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("ccc"),
					crd.WithLabel(crdLabel),
					crd.WithEnsureAllDaemonSetPodsRunning(),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"ng-1"})),
					}),
				),
				crd.NewTestCrd(
					crd.WithName("ccc-2"),
					crd.WithLabel(crdLabel),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"ng-2"})),
					}),
				),
			},
			pods: []*apiv1.Pod{
				newDSPod(ds, "n1", test.MarkUnschedulable()),
				newDSPod(ds, "n2", test.MarkUnschedulable()),
				newDSPod(ds, "n3", test.MarkUnschedulable()),
			},
			autopilotEnabled:          false,
			nodeNames:                 []string{"n1", "n2", "n3"},
			wantNodes:                 []string{"n1"},
			wantLatestUnfitNodesCount: 1,
		},
		{
			name: "non-DS unschedulable pods",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 500, 10): {},
			},
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1000, 10, test.MarkUnschedulable()),
			},
			autopilotEnabled: true,
			nodeNames:        []string{"n1"},
			wantNodes:        []string{},
		},
		{
			name: "DS unschedulable pod with no affinity",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 500, 10): {},
			},
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1000, 10, test.MarkUnschedulable(), test.WithControllerOwnerRef(ds.Name, "DaemonSet", ds.UID)),
			},
			autopilotEnabled: true,
			nodeNames:        []string{"n1"},
			wantNodes:        []string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			crdLister := lister.NewMockCrdListerWithLabel(tc.crds, crdLabel)
			var podLister kubernetes.PodLister
			if tc.listerErr != nil {
				podLister = &errorPodLister{tc.listerErr}
			} else {
				podLister = kubernetes.NewTestPodLister(tc.pods)
			}
			cp := testprovider.NewTestCloudProviderBuilder().Build()
			ctx := &context.AutoscalingContext{
				ClusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t),
				CloudProvider:   cp,
				AutoscalingKubeClients: context.AutoscalingKubeClients{
					ListerRegistry: kubernetes.NewListerRegistry(nil, nil, podLister, nil, nil, nil, nil, nil, nil),
				},
			}

			for _, ng := range tc.nodeGroups {
				mig := testutil.CreateMig(ng, gkeManager)
				cp.InsertNodeGroup(mig)
				for _, node := range ng.Nodes {
					cp.AddNode(mig.Id(), node)
					assert.NoError(t, ctx.ClusterSnapshot.AddNodeInfo(framework.NewTestNodeInfo(node)))
				}
			}

			for node, pods := range tc.nodesWithPods {
				assert.NoError(t, ctx.ClusterSnapshot.AddNodeInfo(framework.NewTestNodeInfo(node, pods...)))
			}
			pluginsConfig := config.PluginsConfig{
				Autopilot:             tc.autopilotEnabled,
				NPCLister:             crdLister,
				MaxCandidateNodeCount: tc.maxCandidateNodesCount,
			}
			plugin := NewPlugin(pluginsConfig)
			candidate := plugin.NewCandidate(ctx, tc.nodeNames)
			if len(tc.wantNodes) > 0 {
				if assert.NotNil(t, candidate) {
					assert.Equal(t, tc.wantNodes, candidate.Nodes)
					assert.Equal(t, defrag.Partial, candidate.Mode)
				}
			} else {
				assert.Nil(t, candidate)
			}

			latestUnfitNodesCount := plugin.LatestUnfitNodesCount()
			if latestUnfitNodesCount != tc.wantLatestUnfitNodesCount {
				t.Errorf("plugin.LatestUnfitNodesCount() got latest unfit node count: %d, want latest unfit node count: %d", latestUnfitNodesCount, tc.wantLatestUnfitNodesCount)
			}
		})
	}
}

func TestDaemonSetPluginValidCandidateNodes(t *testing.T) {
	ds := newDaemonSet("ds", 1000, 1, nil)

	testCases := []struct {
		name                    string
		pods                    []*apiv1.Pod
		listerErr               error
		nodesWithPods           map[*apiv1.Node][]*apiv1.Pod
		candidate               *defrag.Candidate
		wantValidCandidateNodes []string
	}{
		{
			name:                    "candidate without nodes, no valid nodes expected",
			candidate:               &defrag.Candidate{},
			wantValidCandidateNodes: nil,
		},
		{
			name: "candidate with single node, no DS expected, invalid",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 1000, 10): {},
			},
			candidate:               &defrag.Candidate{Nodes: []string{"n1"}},
			wantValidCandidateNodes: nil,
		},
		{
			name: "candidate with single node, DS expected and not schedulable, valid",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 500, 10): {},
			},
			pods:                    []*apiv1.Pod{newDSPod(ds, "n1", test.MarkUnschedulable())},
			candidate:               &defrag.Candidate{Nodes: []string{"n1"}},
			wantValidCandidateNodes: []string{"n1"},
		},
		{
			name: "candidate with single node, DS expected and scheduled, invalid",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 1000, 10): {
					newDaemonSetPod(ds, "n1"),
				},
			},
			pods:                    []*apiv1.Pod{newDSPod(ds, "n1")},
			candidate:               &defrag.Candidate{Nodes: []string{"n1"}},
			wantValidCandidateNodes: nil,
		},
		{
			name: "candidate with multiple nodes, all with DS scheduled",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 1000, 10): {
					newDaemonSetPod(ds, "n1"),
				},
				test.BuildTestNode("n2", 1000, 10): {
					newDaemonSetPod(ds, "n2"),
				},
			},
			pods:                    []*apiv1.Pod{newDSPod(ds, "n1"), newDSPod(ds, "n2")},
			candidate:               &defrag.Candidate{Nodes: []string{"n1", "n2"}},
			wantValidCandidateNodes: nil,
		},
		{
			name: "candidate with multiple nodes, some invalid, returns only valid",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 500, 10):  {},
				test.BuildTestNode("n2", 1000, 10): {},
			},
			pods:                    []*apiv1.Pod{newDSPod(ds, "n1", test.MarkUnschedulable())},
			candidate:               &defrag.Candidate{Nodes: []string{"n1", "n2"}},
			wantValidCandidateNodes: []string{"n1"},
		},
		{
			name: "candidate with multiple nodes, all valid, valid",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 500, 10): {},
				test.BuildTestNode("n2", 500, 10): {},
			},
			pods: []*apiv1.Pod{
				newDSPod(ds, "n1", test.MarkUnschedulable()),
				newDSPod(ds, "n2", test.MarkUnschedulable()),
			},
			candidate:               &defrag.Candidate{Nodes: []string{"n1", "n2"}},
			wantValidCandidateNodes: []string{"n1", "n2"},
		},
		{
			name: "candidate with Defrag taints are valid",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				setTaint(test.BuildTestNode("n1", 500, 10), defrag.HardTaint): {},
			},
			pods:                    []*apiv1.Pod{newDSPod(ds, "n1", test.MarkUnschedulable())},
			candidate:               &defrag.Candidate{Nodes: []string{"n1"}},
			wantValidCandidateNodes: []string{"n1"},
		},
		{
			name: "DS lister error, invalid",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 500, 10): {},
				test.BuildTestNode("n2", 500, 10): {},
			},
			listerErr:               errors.New("error"),
			candidate:               &defrag.Candidate{Nodes: []string{"n1", "n2"}},
			wantValidCandidateNodes: nil,
		},
		{
			name: "Non-existing node (should never happen), only existing valid",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 500, 10): {},
			},
			pods:                    []*apiv1.Pod{newDSPod(ds, "n1", test.MarkUnschedulable())},
			candidate:               &defrag.Candidate{Nodes: []string{"n1", "n2"}},
			wantValidCandidateNodes: []string{"n1"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var podLister kubernetes.PodLister
			if tc.listerErr != nil {
				podLister = &errorPodLister{tc.listerErr}
			} else {
				podLister = kubernetes.NewTestPodLister(tc.pods)
			}
			ctx := &context.AutoscalingContext{
				ClusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t),
				AutoscalingKubeClients: context.AutoscalingKubeClients{
					ListerRegistry: kubernetes.NewListerRegistry(nil, nil, podLister, nil, nil, nil, nil, nil, nil),
				},
			}
			for node, pods := range tc.nodesWithPods {
				assert.NoError(t, ctx.ClusterSnapshot.AddNodeInfo(framework.NewTestNodeInfo(node, pods...)))
			}

			plugin := NewPlugin(config.PluginsConfig{})
			assert.Equal(t, tc.wantValidCandidateNodes, plugin.ValidCandidateNodes(ctx, tc.candidate.Nodes))
		})
	}
}

func setTaint(node *apiv1.Node, key string) *apiv1.Node {
	node.Spec.Taints = append(node.Spec.Taints, apiv1.Taint{
		Key:    key,
		Effect: apiv1.TaintEffectNoSchedule,
	})
	return node
}

func TestDaemonSetPluginNewCandidate_GracePeriod(t *testing.T) {
	ds := newDaemonSet("ds", 1000, 1, nil)
	const testGracePeriod = 10 * time.Second
	type step struct {
		timeAdvance        time.Duration
		updateNodeFunc     func(n *apiv1.Node)
		wantCandidateNodes []string
		wantPendingCount   int
		wantUnfitCount     int
		markPodSchedulable bool
	}
	testCases := []struct {
		name  string
		node  *apiv1.Node
		steps []step
	}{
		{
			name: "EK node unfit, expires grace period",
			node: withEKLabel(test.BuildTestNode("n1", 500, 10)),
			steps: []step{
				{
					// Node is found unfit, enters pending.
					timeAdvance:        0,
					wantCandidateNodes: nil,
					wantPendingCount:   1,
					wantUnfitCount:     0,
				},
				{
					// Still within grace period, remains pending.
					timeAdvance:        5 * time.Second,
					wantCandidateNodes: nil,
					wantPendingCount:   1,
					wantUnfitCount:     0,
				},
				{
					// Grace period expired, becomes a candidate.
					timeAdvance:        6 * time.Second,
					wantCandidateNodes: []string{"n1"},
					wantPendingCount:   0,
					wantUnfitCount:     1,
				},
			},
		},
		{
			name: "EK node unfit, fixed during grace period",
			node: withEKLabel(test.BuildTestNode("n1", 500, 10)),
			steps: []step{
				{
					// Node is found unfit, enters pending.
					timeAdvance:        0,
					wantCandidateNodes: nil,
					wantPendingCount:   1,
					wantUnfitCount:     0,
				},
				{
					// Node is fixed (e.g. EK upsize), removed from pending.
					timeAdvance: 5 * time.Second,
					updateNodeFunc: func(n *apiv1.Node) {
						n.Status.Capacity[apiv1.ResourceCPU] = *resource.NewMilliQuantity(2000, resource.DecimalSI)
						n.Status.Allocatable[apiv1.ResourceCPU] = *resource.NewMilliQuantity(2000, resource.DecimalSI)
					},
					markPodSchedulable: true,
					wantCandidateNodes: nil,
					wantPendingCount:   0,
					wantUnfitCount:     0,
				},
			},
		},
		{
			name: "Non-EK node unfit, becomes candidate immediately",
			node: withNonEKLabel(test.BuildTestNode("n1", 500, 10)),
			steps: []step{
				{
					// Node is found unfit, becomes candidate immediately.
					timeAdvance:        0,
					wantCandidateNodes: []string{"n1"},
					wantPendingCount:   0,
					wantUnfitCount:     1,
				},
			},
		},
		{
			name: "Missing label node unfit, becomes candidate immediately",
			node: test.BuildTestNode("n1", 500, 10),
			steps: []step{
				{
					// Node is found unfit, becomes candidate immediately.
					timeAdvance:        0,
					wantCandidateNodes: []string{"n1"},
					wantPendingCount:   0,
					wantUnfitCount:     1,
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pod := newDSPod(ds, tc.node.Name, test.MarkUnschedulable())
			podLister := kubernetes.NewTestPodLister([]*apiv1.Pod{pod})
			ctx := &context.AutoscalingContext{
				ClusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t),
				AutoscalingKubeClients: context.AutoscalingKubeClients{
					ListerRegistry: kubernetes.NewListerRegistry(nil, nil, podLister, nil, nil, nil, nil, nil, nil),
				},
			}
			assert.NoError(t, ctx.ClusterSnapshot.AddNodeInfo(framework.NewTestNodeInfo(tc.node)))
			nodeNames := []string{tc.node.Name}
			plugin, fakeClock := newTestPluginWithClock(config.PluginsConfig{Autopilot: true})
			plugin.gracePeriod = testGracePeriod
			for i, step := range tc.steps {
				if step.markPodSchedulable {
					scheduled := &apiv1.PodCondition{
						Type:   apiv1.PodScheduled,
						Status: apiv1.ConditionTrue,
					}
					podutil.UpdatePodCondition(&pod.Status, scheduled)
				}
				fakeClock.Step(step.timeAdvance)
				if step.updateNodeFunc != nil {
					nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(tc.node.Name)
					assert.NoError(t, err, "Step %d: failed to get node info", i+1)
					newNode := nodeInfo.Node().DeepCopy()
					step.updateNodeFunc(newNode)
					nodeInfo.SetNode(newNode)
				}
				candidate := plugin.NewCandidate(ctx, nodeNames)
				assert.Equal(t, step.wantPendingCount, len(plugin.pendingUnfitNodes), "Step %d: wrong pending count", i+1)
				assert.Equal(t, step.wantUnfitCount, plugin.LatestUnfitNodesCount(), "Step %d: wrong unfit count", i+1)
				if step.wantCandidateNodes == nil {
					assert.Nil(t, candidate, "Step %d: expected nil candidate", i+1)
				} else {
					if assert.NotNil(t, candidate, "Step %d: expected non-nil candidate", i+1) {
						assert.Equal(t, step.wantCandidateNodes, candidate.Nodes, "Step %d: wrong candidate nodes", i+1)
					}
				}
			}
		})
	}
}

func setLabel(node *apiv1.Node, key, value string) *apiv1.Node {
	node.Labels[key] = value
	return node
}

func newDaemonSetPod(ds *appsv1.DaemonSet, nodeName string) *apiv1.Pod {
	pod := daemon.NewPod(ds, nodeName)
	ptrVal := true
	pod.ObjectMeta.OwnerReferences = []metav1.OwnerReference{
		{Kind: "DaemonSet", UID: ds.UID, Controller: &ptrVal},
	}
	return pod
}

func newDaemonSet(name string, cpu, memory int64, selector map[string]string) *appsv1.DaemonSet {
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
			UID:       types.UID(name),
		},
		Spec: appsv1.DaemonSetSpec{
			Template: apiv1.PodTemplateSpec{
				Spec: apiv1.PodSpec{
					NodeSelector: selector,
					Containers: []apiv1.Container{
						{
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{
									apiv1.ResourceCPU:    *resource.NewMilliQuantity(cpu, resource.DecimalSI),
									apiv1.ResourceMemory: *resource.NewMilliQuantity(memory, resource.DecimalSI),
								},
							},
						},
					},
				},
			},
		},
	}
}

// newTestPluginWithClock is a helper for testing the stateful grace period logic
func newTestPluginWithClock(config config.PluginsConfig) (*plugin, *clocktesting.FakeClock) {
	fakeClock := clocktesting.NewFakeClock(time.Now())
	plugin := &plugin{
		config:            config,
		pendingUnfitNodes: make(map[string]time.Time),
		clock:             fakeClock,
		gracePeriod:       defaultGracePeriod,
	}
	return plugin, fakeClock
}

func withEKLabel(node *apiv1.Node) *apiv1.Node {
	if node.Labels == nil {
		node.Labels = make(map[string]string)
	}
	node.Labels[gkelabels.MachineFamilyLabel] = machinetypes.EK.Name()
	return node
}

// Helper to add a non-EK machine family label to a node
func withNonEKLabel(node *apiv1.Node) *apiv1.Node {
	if node.Labels == nil {
		node.Labels = make(map[string]string)
	}
	node.Labels[gkelabels.MachineFamilyLabel] = machinetypes.E2.Name()
	return node
}

type errorPodLister struct {
	err error
}

func (e *errorPodLister) List() ([]*apiv1.Pod, error) {
	return nil, e.err
}

func newDSPod(ds *appsv1.DaemonSet, nodeName string, extraOpts ...func(pod *apiv1.Pod)) *apiv1.Pod {
	pod := &apiv1.Pod{
		ObjectMeta: ds.Spec.Template.ObjectMeta,
		Spec:       ds.Spec.Template.Spec,
	}
	opts := []func(pod *apiv1.Pod){
		test.WithControllerOwnerRef(ds.Name, "DaemonSet", ds.UID),
		test.WithNodeNamesAffinity(nodeName),
	}
	opts = append(opts, extraOpts...)
	for _, opt := range opts {
		opt(pod)
	}
	return pod
}
