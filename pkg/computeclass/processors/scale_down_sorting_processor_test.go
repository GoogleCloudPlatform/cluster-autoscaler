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

package processors

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testCloudProvider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/scaledowncandidates"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

var (
	n1           = buildNode("node-1")
	n2           = buildNode("node-2")
	n3           = buildNode("node-3")
	n2Family     = "n2"
	e2Family     = "e2"
	c3Family     = "c3"
	spot         = true
	minResources = 1
	icCrd        = crd.NewTestCrd(crd.WithLabel("crd-1"),
		crd.WithName("crd-object-1"),
		crd.WithRules([]rules.Rule{
			rules.NewMachineSpecRule(&c3Family, &spot, &minResources, &minResources),
			rules.NewMachineSpecRule(&e2Family, &spot, &minResources, &minResources),
			rules.NewMachineSpecRule(&n2Family, &spot, &minResources, &minResources),
			rules.NewMachineSpecRule(&n2Family, nil, &minResources, &minResources),
		}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled())
)

// Holds node & extra data about the related node group to construct
type extendedNode struct {
	nodeGroupName string
	node          *apiv1.Node
	crd           crd.CRD
	machineType   string
	spot          bool
	skipNodeGroup bool
}

func initTestCase(t *testing.T, extendedNodes []extendedNode) ([]*apiv1.Node, *context.AutoscalingContext, *crdScaleDownSortingProcessor) {
	cp := NewMockCloudProvider()
	crds := make([]crd.CRD, 0)
	allNodes := make([]*apiv1.Node, 0)
	snapshot := testsnapshot.NewTestSnapshotOrDie(t)
	gkeManager := &gke.GkeManagerMock{}
	gkeManager.On("GetNumberOfSurgeNodesInMig", mock.AnythingOfType("*gke.GkeMig")).Return(0)
	for _, extendedNode := range extendedNodes {
		var mig *gke.GkeMig
		if extendedNode.crd != nil {
			mig = gke.NewTestGkeMigBuilder().
				SetNodePoolName(extendedNode.nodeGroupName).
				SetGceRefName(extendedNode.nodeGroupName).
				SetSpec(&gkeclient.NodePoolSpec{
					Labels:      map[string]string{icCrd.Label(): extendedNode.crd.Name()},
					MachineType: extendedNode.machineType,
					Spot:        extendedNode.spot}).
				SetGkeManager(gkeManager).
				SetMinSize(-1). // A hack to allow scale down with mock GkeManager
				Build()
			crds = append(crds, extendedNode.crd)
		} else {
			mig = gke.NewTestGkeMigBuilder().
				SetNodePoolName(extendedNode.nodeGroupName).
				SetGceRefName(extendedNode.nodeGroupName).
				SetSpec(&gkeclient.NodePoolSpec{
					Labels:      map[string]string{icCrd.Label(): "default"},
					MachineType: extendedNode.machineType,
					Spot:        extendedNode.spot}).
				SetGkeManager(gkeManager).
				SetMinSize(-1). // A hack to allow scale down with mock GkeManager
				Build()
		}
		cp.On("GetAutoprovisioningDefaultFamily").Return(machinetypes.N1)
		cp.On("IsAutopilotEnabled").Return(false)
		if !extendedNode.skipNodeGroup {
			cp.InsertNodeGroup(mig)
			cp.AddNode(mig.Id(), extendedNode.node)
		}
		err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(extendedNode.node))
		assert.NoError(t, err)
		allNodes = append(allNodes, extendedNode.node)
	}

	crdLister := lister.NewMockCrdLister(crds)
	crdLister.SetCrdLabel("crd-1")
	processor := NewCrdScaleDownSortingProcessor(crdLister, cp)
	ctx := &context.AutoscalingContext{
		CloudProvider:   cp,
		ClusterSnapshot: snapshot,
	}
	return allNodes, ctx, processor
}

func TestScaleDownEarlierThan(t *testing.T) {
	testCases := []struct {
		name       string
		node1      extendedNode
		node2      extendedNode
		wantResult bool
	}{
		{
			name: "No Crds",
			node1: extendedNode{
				nodeGroupName: "nodepool-1",
				machineType:   "n1-standard-2",
				spot:          false,
				node:          n1,
			},
			node2: extendedNode{
				nodeGroupName: "nodepool-2",
				machineType:   "e2-standard-4",
				spot:          false,
				node:          n2,
			},
			wantResult: false,
		},
		{
			name: "Node(1) has crd, Node(2) doesn't - Node(2) should be scaled down earlier",
			node1: extendedNode{
				nodeGroupName: "nodepool-1",
				machineType:   "n2-standard-4",
				spot:          false,
				node:          n1,
				crd:           icCrd,
			},
			node2: extendedNode{
				nodeGroupName: "nodepool-2",
				machineType:   "e2-standard-4",
				spot:          false,
				node:          n2,
			},
			wantResult: false,
		},
		{
			name: "Node(2) has crd, Node(1) doesn't - Node(1) should be scaled down earlier",
			node1: extendedNode{
				nodeGroupName: "nodepool-1",
				machineType:   "n1-standard-2",
				spot:          false,
				node:          n1,
			},
			node2: extendedNode{
				nodeGroupName: "nodepool-2",
				machineType:   "n2-standard-4",
				spot:          false,
				node:          n2,
				crd:           icCrd,
			},
			wantResult: true,
		},
		{
			name: "Node(1) has higher priority",
			node1: extendedNode{
				nodeGroupName: "nodepool-1",
				machineType:   "c3-standard-4",
				spot:          true,
				node:          n1,
				crd:           icCrd,
			},
			node2: extendedNode{
				nodeGroupName: "nodepool-2",
				machineType:   "e2-standard-4",
				spot:          true,
				node:          n2,
				crd:           icCrd,
			},
			wantResult: false,
		},
		{
			name: "Node(1) has lower priority",
			node1: extendedNode{
				nodeGroupName: "nodepool-1",
				machineType:   "n2-standard-2",
				spot:          true,
				node:          n1,
				crd:           icCrd,
			},
			node2: extendedNode{
				nodeGroupName: "nodepool-2",
				machineType:   "e2-standard-4",
				spot:          true,
				node:          n2,
				crd:           icCrd,
			},
			wantResult: true,
		},
		{
			name: "Node(1) has same priority as Node(2)",
			node1: extendedNode{
				nodeGroupName: "nodepool-1",
				machineType:   "c3-standard-4",
				spot:          true,
				node:          n1,
				crd:           icCrd,
			},
			node2: extendedNode{
				nodeGroupName: "nodepool-2",
				machineType:   "c3-standard-4",
				spot:          true,
				node:          n2,
				crd:           icCrd,
			},
			wantResult: false,
		},
		{
			name: "Node(1) has no node group",
			node1: extendedNode{
				node:          n1,
				skipNodeGroup: true,
			},
			node2: extendedNode{
				nodeGroupName: "nodepool-2",
				machineType:   "n2-standard-4",
				spot:          false,
				node:          n2,
				crd:           icCrd,
			},
			wantResult: true,
		},
		{
			name: "Both nodes have no node group",
			node1: extendedNode{
				node:          n1,
				skipNodeGroup: true,
			},
			node2: extendedNode{
				node:          n2,
				skipNodeGroup: true,
			},
			wantResult: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, processor := initTestCase(t, []extendedNode{tc.node1, tc.node2})
			result := processor.ScaleDownEarlierThan(tc.node1.node, tc.node2.node)
			assert.Equal(t, tc.wantResult, result)
		})
	}
}

func TestScaleDownSortingProcessorIntegration(t *testing.T) {
	testCases := []struct {
		name      string
		nodes     []extendedNode
		wantOrder []*apiv1.Node
		wantErr   bool
	}{
		{
			name: "No crds",
			nodes: []extendedNode{
				{
					nodeGroupName: "nodepool-1",
					machineType:   "n1-standard-2",
					spot:          false,
					node:          n1,
				},
				{
					nodeGroupName: "nodepool-2",
					machineType:   "e2-standard-4",
					spot:          false,
					node:          n2,
				},
				{
					nodeGroupName: "nodepool-3",
					machineType:   "c3-standard-2",
					spot:          false,
					node:          n3,
				},
			},
			wantOrder: []*apiv1.Node{n1, n2, n3},
		},
		{
			name: "Nodes without crd are scaled down first",
			nodes: []extendedNode{
				{
					nodeGroupName: "nodepool-1",
					machineType:   "n2-standard-4",
					spot:          false,
					node:          n1,
					crd:           icCrd,
				},
				{
					nodeGroupName: "nodepool-2",
					machineType:   "e2-standard-4",
					spot:          false,
					node:          n2,
				},
				{
					nodeGroupName: "nodepool-3",
					machineType:   "c3-standard-2",
					spot:          false,
					node:          n3,
				},
			},
			wantOrder: []*apiv1.Node{n2, n3, n1},
		},
		{
			name: "Nodes with lower priority index are scaled down last",
			nodes: []extendedNode{
				{
					nodeGroupName: "nodepool-1",
					machineType:   "c3-standard-4",
					spot:          true,
					node:          n1,
					crd:           icCrd,
				},
				{
					nodeGroupName: "nodepool-2",
					machineType:   "e2-standard-4",
					spot:          true,
					node:          n2,
					crd:           icCrd,
				},
				{
					nodeGroupName: "nodepool-3",
					machineType:   "n2-standard-4",
					spot:          true,
					node:          n3,
					crd:           icCrd,
				},
			},
			wantOrder: []*apiv1.Node{n3, n2, n1},
		},
		{
			name: "Order of nodes with same priority index is unchanged",
			nodes: []extendedNode{
				{
					nodeGroupName: "nodepool-1",
					machineType:   "c3-standard-4",
					spot:          true,
					node:          n1,
					crd:           icCrd,
				},
				{
					nodeGroupName: "nodepool-2",
					machineType:   "c3-standard-4",
					spot:          true,
					node:          n2,
					crd:           icCrd,
				},
				{
					nodeGroupName: "nodepool-3",
					machineType:   "c3-standard-4",
					spot:          true,
					node:          n3,
					crd:           icCrd,
				},
			},
			wantOrder: []*apiv1.Node{n1, n2, n3},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nodes, ctx, comparer := initTestCase(t, tc.nodes)
			comparers := []scaledowncandidates.CandidatesComparer{comparer}
			sortingProcessor := scaledowncandidates.NewScaleDownCandidatesSortingProcessor(comparers)
			sortedNodes, err := sortingProcessor.GetScaleDownCandidates(ctx, nodes)
			assert.Equal(t, tc.wantOrder, sortedNodes)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func buildNode(name string) *apiv1.Node {
	return &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}
}

type mockCloudProvider struct {
	mock.Mock
	*testCloudProvider.TestCloudProvider
}

// NewMockCloudProvider extends TestCloudProvider by adding GetAutoprovisioningDefaultFamily method.
func NewMockCloudProvider() *mockCloudProvider {
	cp := testCloudProvider.NewTestCloudProviderBuilder().Build()
	machineProviderCp := &mockCloudProvider{
		TestCloudProvider: cp,
	}
	return machineProviderCp
}

func (m *mockCloudProvider) GetAutoprovisioningDefaultFamily() machinetypes.MachineFamily {
	args := m.Called()
	return args.Get(0).(machinetypes.MachineFamily)
}

func (m *mockCloudProvider) IsAutopilotEnabled() bool {
	args := m.Called()
	return args.Get(0).(bool)
}
