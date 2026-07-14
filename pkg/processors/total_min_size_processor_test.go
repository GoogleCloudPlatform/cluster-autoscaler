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
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodes"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
)

func TestCompositeScaleDownSetProcessorGetNodesToRemove(t *testing.T) {
	zones := []string{"us-test1-a", "us-test1-b", "us-test1-c"}
	nodePoolNames := []string{"testNodePoolName", "other-node-pool", "yet-another-pool"}
	var allCandidates []simulator.NodeToBeRemoved
	var allNodeInfos []*framework.NodeInfo
	for _, nodePoolName := range nodePoolNames {
		for _, zone := range zones {
			for i := 0; i < 2; i++ {
				node := test.BuildTestNode(fmt.Sprintf("node%d-%s-%s", i, zone, nodePoolName), 1000, 1000)
				allCandidates = append(allCandidates, simulator.NodeToBeRemoved{
					Node: node,
				})
				allNodeInfos = append(allNodeInfos, framework.NewNodeInfo(node, nil))
			}
		}
	}
	defaultMigIDToCandidatesMap := map[int][]simulator.NodeToBeRemoved{
		0: {allCandidates[0], allCandidates[1]},
		1: {allCandidates[2]},
		2: {allCandidates[4]},
		3: {allCandidates[6]},
		4: {allCandidates[8]},
		5: {allCandidates[10]},
	}

	type args struct {
		migIDToCandidatesMap   map[int][]simulator.NodeToBeRemoved
		unknownCandidates      []simulator.NodeToBeRemoved
		nodePoolTemplates      [][]migTemplate
		getMigsTargetSizeError error
	}
	tests := []struct {
		name      string
		args      args
		wantNodes []simulator.NodeToBeRemoved
	}{
		{
			name: "total size nodepool and zonal size limited nodepool, no nodepool is hitting min size",
			args: args{
				nodePoolTemplates: [][]migTemplate{
					{
						newMigTemplate("test-proj", zones[0], "testMigName1", nodePoolNames[0], migSizes{currentSize: 2, totalMinSize: 2, totalMaxSize: 10}),
						newMigTemplate("test-proj", zones[1], "testMigName1", nodePoolNames[0], migSizes{currentSize: 2, totalMinSize: 2, totalMaxSize: 10}),
						newMigTemplate("test-proj", zones[2], "testMigName1", nodePoolNames[0], migSizes{currentSize: 2, totalMinSize: 2, totalMaxSize: 10}),
					},
					{
						newMigTemplate("test-proj", zones[0], "testMigName2", nodePoolNames[1], migSizes{currentSize: 3, minSize: 2, maxSize: 10}),
						newMigTemplate("test-proj", zones[1], "testMigName2", nodePoolNames[1], migSizes{currentSize: 3, minSize: 2, maxSize: 10}),
						newMigTemplate("test-proj", zones[2], "testMigName2", nodePoolNames[1], migSizes{currentSize: 3, minSize: 2, maxSize: 10}),
					},
				},
			},
			wantNodes: []simulator.NodeToBeRemoved{
				allCandidates[0],
				allCandidates[1],
				allCandidates[2],
				allCandidates[4],
				allCandidates[6],
				allCandidates[8],
				allCandidates[10],
			},
		},
		{
			name: "total size nodepool and zonal size limited nodepool, first one hitting total min size",
			args: args{
				nodePoolTemplates: [][]migTemplate{
					{
						newMigTemplate("test-proj", zones[0], "testMigName1", nodePoolNames[0], migSizes{currentSize: 2, totalMinSize: 2, totalMaxSize: 10}),
						newMigTemplate("test-proj", zones[1], "testMigName1", nodePoolNames[0], migSizes{currentSize: 1, totalMinSize: 2, totalMaxSize: 10}),
						newMigTemplate("test-proj", zones[2], "testMigName1", nodePoolNames[0], migSizes{currentSize: 1, totalMinSize: 2, totalMaxSize: 10}),
					},
					{
						newMigTemplate("test-proj", zones[0], "testMigName2", nodePoolNames[1], migSizes{currentSize: 3, minSize: 2, maxSize: 10}),
						newMigTemplate("test-proj", zones[1], "testMigName2", nodePoolNames[1], migSizes{currentSize: 3, minSize: 2, maxSize: 10}),
						newMigTemplate("test-proj", zones[2], "testMigName2", nodePoolNames[1], migSizes{currentSize: 3, minSize: 2, maxSize: 10}),
					},
				},
			},
			wantNodes: []simulator.NodeToBeRemoved{
				allCandidates[0],
				allCandidates[1],
				allCandidates[6],
				allCandidates[8],
				allCandidates[10],
			},
		},
		{
			name: "two total size limited nodepools, both hitting min size",
			args: args{
				nodePoolTemplates: [][]migTemplate{
					{
						newMigTemplate("test-proj", zones[0], "testMigName1", nodePoolNames[0], migSizes{currentSize: 2, totalMinSize: 2, totalMaxSize: 10}),
						newMigTemplate("test-proj", zones[1], "testMigName1", nodePoolNames[0], migSizes{currentSize: 1, totalMinSize: 2, totalMaxSize: 10}),
						newMigTemplate("test-proj", zones[2], "testMigName1", nodePoolNames[0], migSizes{currentSize: 1, totalMinSize: 2, totalMaxSize: 10}),
					},
					{
						newMigTemplate("test-proj", zones[0], "testMigName2", nodePoolNames[1], migSizes{currentSize: 3, totalMinSize: 6, totalMaxSize: 10}),
						newMigTemplate("test-proj", zones[1], "testMigName2", nodePoolNames[1], migSizes{currentSize: 3, totalMinSize: 6, totalMaxSize: 10}),
						newMigTemplate("test-proj", zones[2], "testMigName2", nodePoolNames[1], migSizes{currentSize: 2, totalMinSize: 6, totalMaxSize: 10}),
					},
				},
			},
			wantNodes: []simulator.NodeToBeRemoved{
				allCandidates[0],
				allCandidates[1],
				allCandidates[6],
				allCandidates[8],
			},
		},
		{
			name: "two total size limited nodepools, where first is in blue green upgrade, both hitting min size",
			args: args{
				migIDToCandidatesMap: map[int][]simulator.NodeToBeRemoved{
					// Nodes from green migs are picked from blue/green nodepool.
					3: {allCandidates[0], allCandidates[1]},
					4: {allCandidates[2]},
					5: {allCandidates[4]},
					6: {allCandidates[6]},
					7: {allCandidates[8]},
					8: {allCandidates[10]},
				},
				nodePoolTemplates: [][]migTemplate{
					{
						newMigTemplate("test-proj", zones[0], "testMigName1-blue", nodePoolNames[0], migSizes{currentSize: 2, totalMinSize: 2, totalMaxSize: 10}, withBlueGreenInfo(&gke.MigBlueGreenInfo{Color: gke.BlueMig, Phase: gkeclient.PhaseUnspecified})),
						newMigTemplate("test-proj", zones[1], "testMigName1-blue", nodePoolNames[0], migSizes{currentSize: 2, totalMinSize: 2, totalMaxSize: 10}, withBlueGreenInfo(&gke.MigBlueGreenInfo{Color: gke.BlueMig, Phase: gkeclient.PhaseUnspecified})),
						newMigTemplate("test-proj", zones[2], "testMigName1-blue", nodePoolNames[0], migSizes{currentSize: 2, totalMinSize: 2, totalMaxSize: 10}, withBlueGreenInfo(&gke.MigBlueGreenInfo{Color: gke.BlueMig, Phase: gkeclient.PhaseUnspecified})),
						newMigTemplate("test-proj", zones[0], "testMigName1-green", nodePoolNames[0], migSizes{currentSize: 2, totalMinSize: 2, totalMaxSize: 10}, withBlueGreenInfo(&gke.MigBlueGreenInfo{Color: gke.GreenMig, Phase: gkeclient.PhaseUnspecified})),
						newMigTemplate("test-proj", zones[1], "testMigName1-green", nodePoolNames[0], migSizes{currentSize: 1, totalMinSize: 2, totalMaxSize: 10}, withBlueGreenInfo(&gke.MigBlueGreenInfo{Color: gke.GreenMig, Phase: gkeclient.PhaseUnspecified})),
						newMigTemplate("test-proj", zones[2], "testMigName1-green", nodePoolNames[0], migSizes{currentSize: 1, totalMinSize: 2, totalMaxSize: 10}, withBlueGreenInfo(&gke.MigBlueGreenInfo{Color: gke.GreenMig, Phase: gkeclient.PhaseUnspecified})),
					},
					{
						newMigTemplate("test-proj", zones[0], "testMigName2", nodePoolNames[1], migSizes{currentSize: 3, totalMinSize: 6, totalMaxSize: 10}),
						newMigTemplate("test-proj", zones[1], "testMigName2", nodePoolNames[1], migSizes{currentSize: 3, totalMinSize: 6, totalMaxSize: 10}),
						newMigTemplate("test-proj", zones[2], "testMigName2", nodePoolNames[1], migSizes{currentSize: 2, totalMinSize: 6, totalMaxSize: 10}),
					},
				},
			},
			wantNodes: []simulator.NodeToBeRemoved{
				allCandidates[0],
				allCandidates[1],
				allCandidates[6],
				allCandidates[8],
			},
		},
		{
			name: "two total size limited nodepools, where first is in blue green upgrade, second one hitting min size",
			args: args{
				migIDToCandidatesMap: map[int][]simulator.NodeToBeRemoved{
					// Nodes from blue migs are picked from blue/green nodepool.
					0: {allCandidates[0], allCandidates[1]},
					1: {allCandidates[2]},
					2: {allCandidates[4]},
					6: {allCandidates[6]},
					7: {allCandidates[8]},
					8: {allCandidates[10]},
				},
				nodePoolTemplates: [][]migTemplate{
					{
						newMigTemplate("test-proj", zones[0], "testMigName1-blue", nodePoolNames[0], migSizes{currentSize: 2, totalMinSize: 2, totalMaxSize: 10}, withBlueGreenInfo(&gke.MigBlueGreenInfo{Color: gke.BlueMig, Phase: gkeclient.PhaseUnspecified})),
						newMigTemplate("test-proj", zones[1], "testMigName1-blue", nodePoolNames[0], migSizes{currentSize: 2, totalMinSize: 2, totalMaxSize: 10}, withBlueGreenInfo(&gke.MigBlueGreenInfo{Color: gke.BlueMig, Phase: gkeclient.PhaseUnspecified})),
						newMigTemplate("test-proj", zones[2], "testMigName1-blue", nodePoolNames[0], migSizes{currentSize: 2, totalMinSize: 2, totalMaxSize: 10}, withBlueGreenInfo(&gke.MigBlueGreenInfo{Color: gke.BlueMig, Phase: gkeclient.PhaseUnspecified})),
						newMigTemplate("test-proj", zones[0], "testMigName1-green", nodePoolNames[0], migSizes{currentSize: 2, totalMinSize: 2, totalMaxSize: 10}, withBlueGreenInfo(&gke.MigBlueGreenInfo{Color: gke.GreenMig, Phase: gkeclient.PhaseUnspecified})),
						newMigTemplate("test-proj", zones[1], "testMigName1-green", nodePoolNames[0], migSizes{currentSize: 1, totalMinSize: 2, totalMaxSize: 10}, withBlueGreenInfo(&gke.MigBlueGreenInfo{Color: gke.GreenMig, Phase: gkeclient.PhaseUnspecified})),
						newMigTemplate("test-proj", zones[2], "testMigName1-green", nodePoolNames[0], migSizes{currentSize: 1, totalMinSize: 2, totalMaxSize: 10}, withBlueGreenInfo(&gke.MigBlueGreenInfo{Color: gke.GreenMig, Phase: gkeclient.PhaseUnspecified})),
					},
					{
						newMigTemplate("test-proj", zones[0], "testMigName2", nodePoolNames[1], migSizes{currentSize: 3, totalMinSize: 6, totalMaxSize: 10}),
						newMigTemplate("test-proj", zones[1], "testMigName2", nodePoolNames[1], migSizes{currentSize: 3, totalMinSize: 6, totalMaxSize: 10}),
						newMigTemplate("test-proj", zones[2], "testMigName2", nodePoolNames[1], migSizes{currentSize: 2, totalMinSize: 6, totalMaxSize: 10}),
					},
				},
			},
			wantNodes: []simulator.NodeToBeRemoved{
				allCandidates[0],
				allCandidates[1],
				allCandidates[2],
				allCandidates[4],
				allCandidates[6],
				allCandidates[8],
			},
		},
		{
			name: "nodepool size cannot be accessed",
			args: args{
				nodePoolTemplates: [][]migTemplate{
					{
						newMigTemplate("test-proj", zones[0], "testMigName1", nodePoolNames[0], migSizes{currentSize: 2, totalMinSize: 2, totalMaxSize: 10}),
						newMigTemplate("test-proj", zones[1], "testMigName1", nodePoolNames[0], migSizes{currentSize: 2, totalMinSize: 2, totalMaxSize: 10}),
						newMigTemplate("test-proj", zones[2], "testMigName1", nodePoolNames[0], migSizes{currentSize: 2, totalMinSize: 2, totalMaxSize: 10}),
					},
					{
						newMigTemplate("test-proj", zones[0], "testMigName2", nodePoolNames[1], migSizes{currentSize: 3, minSize: 2, maxSize: 10}),
						newMigTemplate("test-proj", zones[1], "testMigName2", nodePoolNames[1], migSizes{currentSize: 3, minSize: 2, maxSize: 10}),
						newMigTemplate("test-proj", zones[2], "testMigName2", nodePoolNames[1], migSizes{currentSize: 3, minSize: 2, maxSize: 10}),
					},
				},
				getMigsTargetSizeError: errors.New("test error1"),
			},
			wantNodes: []simulator.NodeToBeRemoved{
				allCandidates[6],
				allCandidates[8],
				allCandidates[10],
			},
		},
		{
			name: "no corresponding migs",
			args: args{
				migIDToCandidatesMap: map[int][]simulator.NodeToBeRemoved{},
				unknownCandidates:    []simulator.NodeToBeRemoved{allCandidates[6]},
				nodePoolTemplates: [][]migTemplate{
					{
						newMigTemplate("test-proj", zones[0], "testMigName1", nodePoolNames[0], migSizes{currentSize: 2, totalMinSize: 2, totalMaxSize: 10}),
						newMigTemplate("test-proj", zones[1], "testMigName1", nodePoolNames[0], migSizes{currentSize: 2, totalMinSize: 2, totalMaxSize: 10}),
						newMigTemplate("test-proj", zones[2], "testMigName1", nodePoolNames[0], migSizes{currentSize: 2, totalMinSize: 2, totalMaxSize: 10}),
					},
					{
						newMigTemplate("test-proj", zones[0], "testMigName2", nodePoolNames[1], migSizes{currentSize: 3, minSize: 2, maxSize: 10}),
						newMigTemplate("test-proj", zones[1], "testMigName2", nodePoolNames[1], migSizes{currentSize: 3, minSize: 2, maxSize: 10}),
						newMigTemplate("test-proj", zones[2], "testMigName2", nodePoolNames[1], migSizes{currentSize: 3, minSize: 2, maxSize: 10}),
					},
				},
			},
			wantNodes: []simulator.NodeToBeRemoved{},
		},
		{
			name: "partial atomic node pools are filtered out",
			args: args{
				migIDToCandidatesMap: map[int][]simulator.NodeToBeRemoved{
					0: {allCandidates[0], allCandidates[1], allCandidates[2]},
					1: {allCandidates[3], allCandidates[4], allCandidates[5]},
				},
				nodePoolTemplates: [][]migTemplate{
					{
						newMigTemplate("test-proj", zones[0], "testMigName1", nodePoolNames[0], migSizes{currentSize: 3, minSize: 0, maxSize: 3}, withTpuMultiHost()),
					},
					{
						newMigTemplate("test-proj", zones[0], "testMigName2", nodePoolNames[1], migSizes{currentSize: 4, minSize: 0, maxSize: 4}, withTpuMultiHost()),
					},
				},
				getMigsTargetSizeError: errors.New("test error1"),
			},
			wantNodes: []simulator.NodeToBeRemoved{
				allCandidates[0],
				allCandidates[1],
				allCandidates[2],
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cloudProvider := &gke.GkeCloudProviderMock{}

			gkeManager := &gke.GkeManagerMock{}
			var allMigs []*gke.GkeMig
			for _, nodepoolTemplate := range tt.args.nodePoolTemplates {
				_, nodePoolGroups := fillMigTemplates(gkeManager, nodepoolTemplate, tt.args.getMigsTargetSizeError)
				allMigs = append(allMigs, nodePoolGroups...)
			}
			gkeManager.On("GetGkeMigs").Return(allMigs)
			gkeManager.On("ScaleDownUnreadyTimeOverride", mock.AnythingOfType("*gke.GkeMig")).Return(time.Duration(0), false).Maybe()

			migIDToCandidatesMap := tt.args.migIDToCandidatesMap
			if migIDToCandidatesMap == nil {
				migIDToCandidatesMap = defaultMigIDToCandidatesMap
			}

			allCandidates := []simulator.NodeToBeRemoved{}
			migIDs := getSortedmigIDs(migIDToCandidatesMap)
			for _, migID := range migIDs {
				candidates := migIDToCandidatesMap[migID]
				for _, candidate := range candidates {
					cloudProvider.On("GkeMigForNode", candidate.Node).Return(allMigs[migID], nil).Once()
					cloudProvider.On("NodeGroupForNode", candidate.Node).Return(allMigs[migID], nil)
				}
				allCandidates = append(allCandidates, candidates...)
			}
			for _, candidate := range tt.args.unknownCandidates {
				var nilMig *gke.GkeMig = nil
				cloudProvider.On("GkeMigForNode", candidate.Node).Return(nilMig, nil).Once()
			}
			allCandidates = append(allCandidates, tt.args.unknownCandidates...)

			s := nodes.NewCompositeScaleDownSetProcessor(
				[]nodes.ScaleDownSetProcessor{
					NewMinSizeProcessor(cloudProvider),
					nodes.NewAtomicResizeFilteringProcessor(),
				},
			)
			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			for _, nodeInfo := range allNodeInfos {
				err := snapshot.AddNodeInfo(nodeInfo)
				assert.NoError(t, err)
			}
			gkeManager.On("GetMigNodes", mock.AnythingOfType("*gke.GkeMig")).Return([]gce.GceInstance{}, nil)
			ctx := context.AutoscalingContext{
				CloudProvider:   cloudProvider,
				ClusterSnapshot: snapshot,
			}
			got, _ := s.FilterUnremovableNodes(&ctx, nodes.NewDefaultScaleDownContext(), allCandidates)
			if !reflect.DeepEqual(got, tt.wantNodes) {
				t.Errorf("TotalMinSizeProcessor.GetNodesToRemove() got:\n%+v\n, want:\n%+v", got, tt.wantNodes)
			}
		})
	}
}

func getSortedmigIDs(m map[int][]simulator.NodeToBeRemoved) []int {
	migIDs := []int{}
	for k := range m {
		migIDs = append(migIDs, k)
	}
	sort.Ints(migIDs)
	return migIDs
}

func TestGetNodesBeingDeletedInNodeGroup(t *testing.T) {
	node := test.BuildTestNode("node1", 1000, 1000)
	tests := []struct {
		name            string
		nodeGroup       any
		nodeGroupError  error
		actuationStatus *fakeActuationStatus
		want            int
	}{
		{
			name:            "Node group nil",
			nodeGroup:       nil,
			nodeGroupError:  nil,
			actuationStatus: &fakeActuationStatus{},
			want:            0,
		},
		{
			name:            "Node group error",
			nodeGroup:       nil,
			nodeGroupError:  errors.New("error"),
			actuationStatus: &fakeActuationStatus{},
			want:            0,
		},
		{
			name:            "Actuation status nil",
			nodeGroup:       &gke.GkeMig{},
			nodeGroupError:  nil,
			actuationStatus: nil,
			want:            0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cloudProvider := &gke.GkeCloudProviderMock{}
			cloudProvider.On("NodeGroupForNode", node).Return(tt.nodeGroup, tt.nodeGroupError)

			ctx := &context.AutoscalingContext{
				CloudProvider: cloudProvider,
			}
			scaleDownCtx := &nodes.ScaleDownContext{}
			if tt.actuationStatus != nil {
				scaleDownCtx.ActuationStatus = tt.actuationStatus
			}

			got := getNodesBeingDeletedInNodeGroup(ctx, scaleDownCtx, node)
			assert.Equal(t, tt.want, got)
		})
	}
}

type fakeActuationStatus struct {
	emptyNodesList         []string
	drainedNodesList       []string
	deletionsCountsByGroup map[string]int
	evictedPods            []*apiv1.Pod
}

func (f *fakeActuationStatus) DeletionsInProgress() (empty, drained []string) {
	return f.emptyNodesList, f.drainedNodesList
}

func (f *fakeActuationStatus) DeletionsCount(nodeGroupId string) int {
	return f.deletionsCountsByGroup[nodeGroupId]
}

func (f *fakeActuationStatus) RecentEvictions() (pods []*apiv1.Pod) {
	return f.evictedPods
}
