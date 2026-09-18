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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crdtest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	listertest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/drainability/rules"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/options"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
)

func ptrInt32(i int32) *int32 {
	return &i
}

type testMaxDisruptionLister struct {
	listertest.Lister
	crdName string
	crd     crdtest.CRD
}

func (m *testMaxDisruptionLister) NodeGroupCrd(nodeGroup cloudprovider.NodeGroup) (crdtest.CRD, string, error) {
	return m.crd, m.crdName, nil
}

func (m *testMaxDisruptionLister) NodeCrd(node *apiv1.Node) (crdtest.CRD, string, error) {
	return m.crd, m.crdName, nil
}

func TestFilterNodesViolatingMaxDisruption(t *testing.T) {
	provider := testprovider.NewTestCloudProviderBuilder().Build()
	ng1Nodes := buildNodeGroup(provider, "ng1", 0, 3)

	crd1 := crdtest.NewTestCrd(
		crdtest.WithName("test-crd1"),
		crdtest.WithLabel(ccc.CrdType),
		crdtest.WithMaxNodeDisruption(ptrInt32(1)),
	)
	crd2 := crdtest.NewTestCrd(
		crdtest.WithName("test-crd2"),
		crdtest.WithLabel(ccc.CrdType),
		crdtest.WithMaxNodeDisruption(ptrInt32(0)),
	)

	testLister := listertest.NewMockCrdListerWithLabel([]crdtest.CRD{crd1, crd2}, ccc.CrdType)

	for _, node := range ng1Nodes {
		node.Spec.ProviderID = fmt.Sprintf("test://%s", node.Name)
		node.Labels = map[string]string{ccc.CrdType: "test-crd1"}
	}

	testCases := []struct {
		name            string
		candidate       *defrag.Candidate
		crdName         string
		actuationStatus fakeActuationStatus
		wantNodes       []string
	}{
		{
			name: "partial candidate, budget 1, one node requested",
			candidate: &defrag.Candidate{
				IsAtomic: false,
				Mode:     defrag.CreateBeforeDelete,
				Nodes:    []string{ng1Nodes[0].Name},
			},
			crdName:   "test-crd1",
			wantNodes: []string{ng1Nodes[0].Name},
		},
		{
			name: "partial candidate, budget 1, two nodes requested",
			candidate: &defrag.Candidate{
				IsAtomic: false,
				Mode:     defrag.CreateBeforeDelete,
				Nodes:    []string{ng1Nodes[0].Name, ng1Nodes[1].Name},
			},
			crdName:   "test-crd1",
			wantNodes: []string{ng1Nodes[0].Name}, // only the first one allowed
		},
		{
			name: "atomic candidate, budget 1, one node requested",
			candidate: &defrag.Candidate{
				IsAtomic: true,
				Mode:     defrag.CreateBeforeDelete,
				Nodes:    []string{ng1Nodes[0].Name},
			},
			crdName:   "test-crd1",
			wantNodes: []string{ng1Nodes[0].Name},
		},
		{
			name: "atomic candidate, budget 1, two nodes requested",
			candidate: &defrag.Candidate{
				IsAtomic: true,
				Mode:     defrag.CreateBeforeDelete,
				Nodes:    []string{ng1Nodes[0].Name, ng1Nodes[1].Name},
			},
			crdName:   "test-crd1",
			wantNodes: []string{ng1Nodes[0].Name}, // atomicity check moved to processor.go
		},
		{
			name: "partial candidate, budget 0 (unlimited)",
			candidate: &defrag.Candidate{
				IsAtomic: false,
				Mode:     defrag.CreateBeforeDelete,
				Nodes:    []string{ng1Nodes[0].Name},
			},
			crdName:   "test-crd2",
			wantNodes: []string{ng1Nodes[0].Name},
		},
		{
			name: "atomic candidate, budget 0 (unlimited)",
			candidate: &defrag.Candidate{
				IsAtomic: true,
				Mode:     defrag.CreateBeforeDelete,
				Nodes:    []string{ng1Nodes[0].Name},
			},
			crdName:   "test-crd2",
			wantNodes: []string{ng1Nodes[0].Name},
		},
		{
			name: "partial candidate with unknown node skipped gracefully",
			candidate: &defrag.Candidate{
				IsAtomic: false,
				Mode:     defrag.CreateBeforeDelete,
				Nodes:    []string{"unknown-node", ng1Nodes[0].Name},
			},
			crdName:   "test-crd1",
			wantNodes: []string{ng1Nodes[0].Name},
		},
		{
			name: "atomic candidate with unknown node rejected completely for safety",
			candidate: &defrag.Candidate{
				IsAtomic: true,
				Mode:     defrag.CreateBeforeDelete,
				Nodes:    []string{"unknown-node", ng1Nodes[0].Name},
			},
			crdName:   "test-crd1",
			wantNodes: []string{ng1Nodes[0].Name},
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

			for _, node := range ng1Nodes {
				node.Labels[ccc.CrdType] = tc.crdName
				assert.NoError(t, ctx.ClusterSnapshot.AddNodeInfo(framework.NewTestNodeInfo(node)))
			}

			deleteOpts := options.NodeDeleteOptions{}
			processor := &mockScaleDownNodeProcessor{
				candidatesFilter: func(nodes []*apiv1.Node) []*apiv1.Node { return nodes },
			}
			trackerFactory := newTestTrackerFactory(nil)

			crdMap := map[string]crdtest.CRD{
				"test-crd1": crd1,
				"test-crd2": crd2,
			}
			customLister := &testMaxDisruptionLister{
				Lister:  testLister,
				crdName: tc.crdName,
				crd:     crdMap[tc.crdName],
			}

			factory := newDefragNodeFilterFactory(processor, deleteOpts, rules.Default(deleteOpts), trackerFactory, customLister)
			nodeFilter, err := factory.NewDefragNodeFilter(ctx)
			assert.NoError(t, err)

			tc.candidate.Nodes = nodeFilter.filterNodesViolatingMaxDisruption(ctx, tc.candidate.Nodes)
			assert.Equal(t, tc.wantNodes, tc.candidate.Nodes)
		})
	}
}

func TestBuildDisruptionTracker(t *testing.T) {
	provider := testprovider.NewTestCloudProviderBuilder().Build()
	ng1Nodes := buildNodeGroup(provider, "ng1", 0, 3)

	crd1 := crdtest.NewTestCrd(
		crdtest.WithName("test-crd1"),
		crdtest.WithLabel(ccc.CrdType),
		crdtest.WithMaxNodeDisruption(ptrInt32(2)),
	)
	testLister := listertest.NewMockCrdListerWithLabel([]crdtest.CRD{crd1}, ccc.CrdType)
	customLister := &testMaxDisruptionLister{
		Lister:  testLister,
		crdName: "test-crd1",
		crd:     crd1,
	}

	for _, node := range ng1Nodes {
		node.Spec.ProviderID = fmt.Sprintf("test://%s", node.Name)
		node.Labels = map[string]string{ccc.CrdType: "test-crd1"}
	}

	testCases := []struct {
		name            string
		actuationStatus fakeActuationStatus
		setupNodes      func(nodes []*apiv1.Node) []*apiv1.Node
		wantBudget      int
	}{
		{
			name: "no deletions, full budget available",
			setupNodes: func(nodes []*apiv1.Node) []*apiv1.Node {
				return nodes
			},
			wantBudget: 2,
		},
		{
			name: "ScaleDownActuator has 1 deletion in progress, budget reduced by 1",
			actuationStatus: fakeActuationStatus{
				deletionsCountsByGroup: map[string]int{"ng1": 1},
			},
			setupNodes: func(nodes []*apiv1.Node) []*apiv1.Node {
				return nodes
			},
			wantBudget: 1,
		},
		{
			name: "node has ToBeDeletedTaint even though ScaleDownActuator counter is 0 (cloud provider delete finished, VM terminating)",
			actuationStatus: fakeActuationStatus{
				deletionsCountsByGroup: map[string]int{"ng1": 0},
			},
			setupNodes: func(nodes []*apiv1.Node) []*apiv1.Node {
				nCopy := nodes[0].DeepCopy()
				nCopy.Spec.Taints = append(nCopy.Spec.Taints, apiv1.Taint{
					Key:    taints.ToBeDeletedTaint,
					Value:  fmt.Sprint(time.Now().Unix()),
					Effect: apiv1.TaintEffectNoSchedule,
				})
				return []*apiv1.Node{nCopy, nodes[1], nodes[2]}
			},
			wantBudget: 1,
		},
		{
			name: "node has ToBeDeletedTaint AND ScaleDownActuator counter is 1 (no double counting)",
			actuationStatus: fakeActuationStatus{
				deletionsCountsByGroup: map[string]int{"ng1": 1},
			},
			setupNodes: func(nodes []*apiv1.Node) []*apiv1.Node {
				nCopy := nodes[0].DeepCopy()
				nCopy.Spec.Taints = append(nCopy.Spec.Taints, apiv1.Taint{
					Key:    taints.ToBeDeletedTaint,
					Value:  fmt.Sprint(time.Now().Unix()),
					Effect: apiv1.TaintEffectNoSchedule,
				})
				return []*apiv1.Node{nCopy, nodes[1], nodes[2]}
			},
			wantBudget: 1, // max(1, 1) = 1, budget remaining = 2 - 1 = 1
		},
		{
			name: "node has DeletionTimestamp set",
			setupNodes: func(nodes []*apiv1.Node) []*apiv1.Node {
				nCopy := nodes[0].DeepCopy()
				now := metav1.Now()
				nCopy.DeletionTimestamp = &now
				return []*apiv1.Node{nCopy, nodes[1], nodes[2]}
			},
			wantBudget: 1,
		},
		{
			name: "two nodes terminating with ToBeDeletedTaint, budget completely exhausted",
			setupNodes: func(nodes []*apiv1.Node) []*apiv1.Node {
				n0 := nodes[0].DeepCopy()
				n0.Spec.Taints = append(n0.Spec.Taints, apiv1.Taint{
					Key:    taints.ToBeDeletedTaint,
					Value:  fmt.Sprint(time.Now().Unix()),
					Effect: apiv1.TaintEffectNoSchedule,
				})
				n1 := nodes[1].DeepCopy()
				n1.Spec.Taints = append(n1.Spec.Taints, apiv1.Taint{
					Key:    taints.ToBeDeletedTaint,
					Value:  fmt.Sprint(time.Now().Unix()),
					Effect: apiv1.TaintEffectNoSchedule,
				})
				return []*apiv1.Node{n0, n1, nodes[2]}
			},
			wantBudget: 0,
		},
		{
			name: "node has ToBeDeletedTaint and is unknown to CloudProvider (VM already terminated), budget still consumed via NodeCrd",
			actuationStatus: fakeActuationStatus{
				deletionsCountsByGroup: map[string]int{"ng1": 0},
			},
			setupNodes: func(nodes []*apiv1.Node) []*apiv1.Node {
				unmappedNode := &apiv1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "deleted-vm-node",
						Labels: map[string]string{ccc.CrdType: "test-crd1"},
					},
					Spec: apiv1.NodeSpec{
						Taints: []apiv1.Taint{
							{
								Key:    taints.ToBeDeletedTaint,
								Value:  fmt.Sprint(time.Now().Unix()),
								Effect: apiv1.TaintEffectNoSchedule,
							},
						},
					},
				}
				return []*apiv1.Node{unmappedNode, nodes[1], nodes[2]}
			},
			wantBudget: 1, // 2 - 1 = 1
		},
		{
			name: "ScaleDownActuator has distinct node in DeletionsInProgress and snapshot has distinct terminating node (both counted via set union)",
			actuationStatus: fakeActuationStatus{
				drainedNodesList: []string{ng1Nodes[1].Name},
			},
			setupNodes: func(nodes []*apiv1.Node) []*apiv1.Node {
				nCopy := nodes[0].DeepCopy()
				nCopy.Spec.Taints = append(nCopy.Spec.Taints, apiv1.Taint{
					Key:    taints.ToBeDeletedTaint,
					Value:  fmt.Sprint(time.Now().Unix()),
					Effect: apiv1.TaintEffectNoSchedule,
				})
				return []*apiv1.Node{nCopy, nodes[1], nodes[2]}
			},
			wantBudget: 0, // 2 - (1 snapshot + 1 actuator) = 0
		},
		{
			name: "same node in DeletionsInProgress and with ToBeDeletedTaint in snapshot (no double counting via set union)",
			actuationStatus: fakeActuationStatus{
				drainedNodesList: []string{ng1Nodes[0].Name},
			},
			setupNodes: func(nodes []*apiv1.Node) []*apiv1.Node {
				nCopy := nodes[0].DeepCopy()
				nCopy.Spec.Taints = append(nCopy.Spec.Taints, apiv1.Taint{
					Key:    taints.ToBeDeletedTaint,
					Value:  fmt.Sprint(time.Now().Unix()),
					Effect: apiv1.TaintEffectNoSchedule,
				})
				return []*apiv1.Node{nCopy, nodes[1], nodes[2]}
			},
			wantBudget: 1, // 2 - 1 = 1
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scaleDownActuator := &mockScaleDownActuator{}
			scaleDownActuator.On("CheckStatus").Return(&tc.actuationStatus)

			ctx := &context.AutoscalingContext{
				CloudProvider:     provider,
				ScaleDownActuator: scaleDownActuator,
			}

			nodes := tc.setupNodes(ng1Nodes)
			tracker := NewMaxNodeDisruptionTracker(ctx, customLister, nodes)

			assert.Equal(t, tc.wantBudget, tracker.remainingDisruptionBudget["test-crd1"])
		})
	}
}

func TestBuildDisruptionTracker_ZeroAndListCrdsDiscovery(t *testing.T) {
	crdPositive := crdtest.NewTestCrd(
		crdtest.WithName("crd-positive"),
		crdtest.WithLabel(ccc.CrdType),
		crdtest.WithMaxNodeDisruption(ptrInt32(3)),
	)
	crdZero := crdtest.NewTestCrd(
		crdtest.WithName("crd-zero"),
		crdtest.WithLabel(ccc.CrdType),
		crdtest.WithMaxNodeDisruption(ptrInt32(0)),
	)
	testLister := listertest.NewMockCrdListerWithLabel([]crdtest.CRD{crdPositive, crdZero}, ccc.CrdType)
	provider := testprovider.NewTestCloudProviderBuilder().Build()
	ctx := &context.AutoscalingContext{
		CloudProvider: provider,
	}

	tracker := NewMaxNodeDisruptionTracker(ctx, testLister, nil)

	// crd-positive discovered from ListCrds even without any nodes
	assert.Equal(t, 3, tracker.remainingDisruptionBudget["crd-positive"])
	// crd-zero has MaxNodeDisruption 0 (unlimited), so not tracked
	_, exists := tracker.remainingDisruptionBudget["crd-zero"]
	assert.False(t, exists, "CRD with MaxNodeDisruption 0 should not be tracked (unlimited)")
}

func TestReserveMaxDisruptionBudget(t *testing.T) {
	provider := testprovider.NewTestCloudProviderBuilder().Build()
	ng1Nodes := buildNodeGroup(provider, "ng1", 0, 3)

	crd1 := crdtest.NewTestCrd(
		crdtest.WithName("test-crd1"),
		crdtest.WithLabel(ccc.CrdType),
		crdtest.WithMaxNodeDisruption(ptrInt32(2)),
	)
	testLister := listertest.NewMockCrdListerWithLabel([]crdtest.CRD{crd1}, ccc.CrdType)
	customLister := &testMaxDisruptionLister{
		Lister:  testLister,
		crdName: "test-crd1",
		crd:     crd1,
	}

	for _, node := range ng1Nodes {
		node.Spec.ProviderID = fmt.Sprintf("test://%s", node.Name)
		node.Labels = map[string]string{ccc.CrdType: "test-crd1"}
	}

	ctx := &context.AutoscalingContext{
		CloudProvider:   provider,
		ClusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t),
	}
	for _, node := range ng1Nodes {
		assert.NoError(t, ctx.ClusterSnapshot.AddNodeInfo(framework.NewTestNodeInfo(node)))
	}

	testCases := []struct {
		name                   string
		initialBudget          int
		candidate              *defrag.Candidate
		isNodeScaleDownStarted func(string) bool
		wantBudget             int
		wantCandidateNodes     []string
	}{
		{
			name:          "budget decremented from 2 to 1, candidate nodes unmodified",
			initialBudget: 2,
			candidate: &defrag.Candidate{
				IsAtomic: false,
				Mode:     defrag.CreateBeforeDelete,
				Nodes:    []string{ng1Nodes[0].Name},
			},
			wantBudget:         1,
			wantCandidateNodes: []string{ng1Nodes[0].Name},
		},
		{
			name:          "node already scale-down started is skipped without decrementing budget",
			initialBudget: 1,
			candidate: &defrag.Candidate{
				IsAtomic: false,
				Mode:     defrag.CreateBeforeDelete,
				Nodes:    []string{ng1Nodes[1].Name},
			},
			isNodeScaleDownStarted: func(name string) bool {
				return name == ng1Nodes[1].Name
			},
			wantBudget:         1,
			wantCandidateNodes: []string{ng1Nodes[1].Name},
		},
		{
			name:          "clamping at 0: reserving when budget is 0 does not make it negative",
			initialBudget: 0,
			candidate: &defrag.Candidate{
				IsAtomic: false,
				Mode:     defrag.CreateBeforeDelete,
				Nodes:    []string{ng1Nodes[0].Name},
			},
			wantBudget:         0,
			wantCandidateNodes: []string{ng1Nodes[0].Name},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filter := &defragNodeFilter{
				disruptionTracker: NewMaxNodeDisruptionTrackerWithBudgets(customLister, map[string]int{
					"test-crd1": tc.initialBudget,
				}),
			}

			filter.reserveMaxDisruptionBudget(ctx, tc.candidate, tc.isNodeScaleDownStarted)

			assert.Equal(t, tc.wantBudget, filter.disruptionTracker.remainingDisruptionBudget["test-crd1"])
			assert.Equal(t, tc.wantCandidateNodes, tc.candidate.Nodes)
		})
	}
}

func TestMaxNodeDisruptionTracker_DoubleCountingPrevention(t *testing.T) {
	provider := testprovider.NewTestCloudProviderBuilder().Build()
	ng1Nodes := buildNodeGroup(provider, "ng1", 0, 3)

	crd1 := crdtest.NewTestCrd(
		crdtest.WithName("test-crd1"),
		crdtest.WithLabel(ccc.CrdType),
		crdtest.WithMaxNodeDisruption(ptrInt32(2)),
	)
	testLister := listertest.NewMockCrdListerWithLabel([]crdtest.CRD{crd1}, ccc.CrdType)
	customLister := &testMaxDisruptionLister{
		Lister:  testLister,
		crdName: "test-crd1",
		crd:     crd1,
	}

	for _, node := range ng1Nodes {
		node.Spec.ProviderID = fmt.Sprintf("test://%s", node.Name)
		node.Labels = map[string]string{ccc.CrdType: "test-crd1"}
	}

	ctx := &context.AutoscalingContext{
		CloudProvider:   provider,
		ClusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t),
	}
	for _, node := range ng1Nodes {
		assert.NoError(t, ctx.ClusterSnapshot.AddNodeInfo(framework.NewTestNodeInfo(node)))
	}

	testCases := []struct {
		name                string
		initialBudget       int
		reservedNodes       []string
		candidatesToReserve []*defrag.Candidate
		nodesToFilter       []string
		wantBudget          int
		wantReservedNodes   []string
		wantFilteredNodes   []string
	}{
		{
			name:              "duplicate nodes within candidate with sufficient budget all pass",
			initialBudget:     2,
			nodesToFilter:     []string{ng1Nodes[0].Name, ng1Nodes[0].Name, ng1Nodes[1].Name},
			wantFilteredNodes: []string{ng1Nodes[0].Name, ng1Nodes[0].Name, ng1Nodes[1].Name},
		},
		{
			name:              "duplicate nodes within candidate with budget 1 allows duplicates of first node and drops second node",
			initialBudget:     1,
			nodesToFilter:     []string{ng1Nodes[0].Name, ng1Nodes[0].Name, ng1Nodes[1].Name},
			wantFilteredNodes: []string{ng1Nodes[0].Name, ng1Nodes[0].Name},
		},
		{
			name:          "reserving node decrements budget and marks node as reserved",
			initialBudget: 2,
			candidatesToReserve: []*defrag.Candidate{
				{Nodes: []string{ng1Nodes[0].Name}},
			},
			wantBudget:        1,
			wantReservedNodes: []string{ng1Nodes[0].Name},
		},
		{
			name:          "reserving already reserved node across new candidate does not double-charge budget",
			initialBudget: 2,
			candidatesToReserve: []*defrag.Candidate{
				{Nodes: []string{ng1Nodes[0].Name}},
				{Nodes: []string{ng1Nodes[0].Name, ng1Nodes[1].Name}},
			},
			wantBudget:        0,
			wantReservedNodes: []string{ng1Nodes[0].Name, ng1Nodes[1].Name},
		},
		{
			name:          "reserving duplicate nodes within the same candidate does not double-charge budget",
			initialBudget: 2,
			candidatesToReserve: []*defrag.Candidate{
				{Nodes: []string{ng1Nodes[0].Name, ng1Nodes[0].Name}},
			},
			wantBudget:        1,
			wantReservedNodes: []string{ng1Nodes[0].Name},
		},
		{
			name:              "filtering ignores already reserved nodes even with 0 remaining budget",
			initialBudget:     0,
			reservedNodes:     []string{ng1Nodes[0].Name, ng1Nodes[1].Name},
			nodesToFilter:     []string{ng1Nodes[0].Name, ng1Nodes[1].Name, ng1Nodes[2].Name},
			wantFilteredNodes: []string{ng1Nodes[0].Name, ng1Nodes[1].Name},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tracker := NewMaxNodeDisruptionTrackerWithBudgets(customLister, map[string]int{
				"test-crd1": tc.initialBudget,
			})
			for _, r := range tc.reservedNodes {
				tracker.reservedNodes[r] = true
			}

			for _, candidate := range tc.candidatesToReserve {
				tracker.ReserveMaxDisruptionBudget(ctx, candidate, nil)
			}
			if len(tc.candidatesToReserve) > 0 {
				assert.Equal(t, tc.wantBudget, tracker.remainingDisruptionBudget["test-crd1"])
				for _, nodeName := range tc.wantReservedNodes {
					assert.True(t, tracker.reservedNodes[nodeName])
				}
			}

			if tc.nodesToFilter != nil {
				filtered := tracker.FilterNodesViolatingMaxDisruption(ctx, tc.nodesToFilter)
				assert.Equal(t, tc.wantFilteredNodes, filtered)
			}
		})
	}
}
