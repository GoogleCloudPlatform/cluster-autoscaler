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
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/operationtracker"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	calculator_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator/test"
	ekvms_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/kubernetes"
	update_infos_mock "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/client/listers/nodemanagement.gke.io/v1alpha1/mock"
	clock "k8s.io/utils/clock/testing"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/store"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

func TestResizableVmDefragScaleDownNodeProcessorGetScaleDownCandidates(t *testing.T) {
	resizingNode := ekvms_test.EkNode32("resizing-node", 8000, 32*size.GiB)
	idleNode := ekvms_test.EkNode32("idle-node", 8000, 32*size.GiB)
	nonResizableNode := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "non-resizable-node"}}
	allNodes := []*v1.Node{resizingNode, idleNode, nonResizableNode}

	testCases := []struct {
		name            string
		resizingEnabled bool
		allNodes        []*v1.Node
		resizingNodes   map[string]bool
		wantCandidates  []*v1.Node
		wantChecked     []string
	}{
		{
			name:            "rejects only resizable nodes with an ongoing or pending resize",
			resizingEnabled: true,
			allNodes:        allNodes,
			resizingNodes:   map[string]bool{"resizing-node": true},
			wantCandidates:  []*v1.Node{idleNode, nonResizableNode},
			wantChecked:     []string{"resizing-node", "idle-node"},
		},
		{
			name:            "keeps all nodes when none is resizing",
			resizingEnabled: true,
			allNodes:        allNodes,
			wantCandidates:  allNodes,
			wantChecked:     []string{"resizing-node", "idle-node"},
		},
		{
			name:            "keeps all nodes when no machine family is resizable",
			resizingEnabled: false,
			allNodes:        allNodes,
			resizingNodes:   map[string]bool{"resizing-node": true},
			wantCandidates:  allNodes,
		},
		{
			name:            "handles an empty node list",
			resizingEnabled: true,
			allNodes:        nil,
			wantCandidates:  []*v1.Node{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockMgr := newManagerMock()
			mockMgr.On("IsResizingEnabled", mock.Anything).Return(tc.resizingEnabled)
			for _, node := range tc.allNodes {
				mockMgr.On("IsNodeResizingOrPending", node.Name).Return(tc.resizingNodes[node.Name]).Maybe()
			}

			p := NewResizableVmDefragScaleDownNodeProcessor(machinetypes.NewMachineConfigProvider(nil), mockMgr)
			candidates, err := p.GetScaleDownCandidates(t.Context(), nil, tc.allNodes)

			assert.NoError(t, err)
			assert.Equal(t, tc.wantCandidates, candidates)

			for _, name := range tc.wantChecked {
				mockMgr.AssertCalled(t, "IsNodeResizingOrPending", name)
			}
		})
	}
}

func TestResizableVmDefragScaleDownNodeProcessorGetPodDestinationCandidates(t *testing.T) {
	allNodes := []*v1.Node{
		ekvms_test.EkNode32("resizable-node", 8000, 32*size.GiB),
		{ObjectMeta: metav1.ObjectMeta{Name: "non-resizable-node"}},
	}

	p := NewResizableVmDefragScaleDownNodeProcessor(machinetypes.NewMachineConfigProvider(nil), newManagerMock())
	destinations, err := p.GetPodDestinationCandidates(nil, allNodes)

	assert.NoError(t, err)
	assert.Equal(t, allNodes, destinations)
}

// TestScaleDownNodeProcessorVsDefragVariant verifies that utilized resizable nodes rejected
// by ScaleDownNodeProcessor (due to downsize) remain eligible candidates in defrag.
func TestScaleDownNodeProcessorVsDefragVariant(t *testing.T) {
	ekNode := ekvms_test.EkNode32("node-32", 8000, 32*size.GiB)
	pods := []*v1.Pod{
		userPod("pod1", 1000, 6*size.GiB),
		systemPod("pod2", 2000, 7*size.GiB),
		daemonsetPod("pod3", 3000, 8*size.GiB),
	}

	testCases := []struct {
		name               string
		nodeResizing       bool
		wantScaleDownNodes []*v1.Node
		wantDefragNodes    []*v1.Node
	}{
		{
			name:               "utilized node is downsized instead of scaled down, but stays a defrag candidate",
			nodeResizing:       false,
			wantScaleDownNodes: []*v1.Node{},
			wantDefragNodes:    []*v1.Node{ekNode},
		},
		{
			name:               "node with an in-flight resize is rejected by both",
			nodeResizing:       true,
			wantScaleDownNodes: []*v1.Node{},
			wantDefragNodes:    []*v1.Node{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sdProcessor, ctx := setupTestScaleDownProcessor(t, ekNode, pods, tc.nodeResizing)

			sdCandidates, err := sdProcessor.GetScaleDownCandidates(t.Context(), ctx, []*v1.Node{ekNode})
			assert.NoError(t, err)
			assert.Equal(t, tc.wantScaleDownNodes, sdCandidates, "scale down candidates")

			defragProcessor := sdProcessor.DefragScaleDownNodeProcessor()
			defragCandidates, err := defragProcessor.GetScaleDownCandidates(t.Context(), ctx, []*v1.Node{ekNode})
			assert.NoError(t, err)
			assert.Equal(t, tc.wantDefragNodes, defragCandidates, "defrag candidates")
		})
	}
}

func setupTestScaleDownProcessor(t *testing.T, node *v1.Node, pods []*v1.Pod, isResizing bool) (*ScaleDownNodeProcessor, *ca_context.AutoscalingContext) {
	snapshot := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
	assert.NoError(t, snapshot.AddNodeInfo(framework.NewTestNodeInfo(node, pods...)))

	cloudProvider := &gke.GkeCloudProviderMock{}
	cloudProvider.On("NodeGroupForNode", mock.AnythingOfType("*v1.Node")).Return(gke.NewTestGkeMigBuilder().Build(), nil)

	ctx := &ca_context.AutoscalingContext{
		ClusterSnapshot: snapshot,
		CloudProvider:   cloudProvider,
	}

	mockMgr := newManagerMock()
	mockMgr.On("IsResizingEnabled", mock.Anything).Return(true)
	mockMgr.On("GetNodesScaleDownAllowedFromCache", []string{node.Name}).Return(map[string]bool{})
	mockMgr.On("FilteredNodesSnapshot", false, operationtracker.ResizableOnly).Return(operationtracker.ResizableNodesSnapshot{
		node.Name: {
			MachineFamily:     machinetypes.EK.Name(),
			DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
			PhysicalMaxSize:   resizable32MaxSize,
			UpsizableMaxSize:  resizable32MaxSize,
			LastOperationTime: testStartTime.Add(-2 * time.Hour),
		},
	})
	mockMgr.On("IsNodeResizingOrPending", node.Name).Return(isResizing)
	mockMgr.On("Downsize", mock.Anything, mock.Anything).Return(nil)
	mockMgr.On("UpdateNodesScaleDownAllowedCache", mock.Anything)

	mockUpdateInfoLister := update_infos_mock.NewMockUpdateInfoLister(gomock.NewController(t))
	mockUpdateInfoLister.EXPECT().List(labels.Everything()).Return(nil, nil).AnyTimes()

	testClock := clock.NewFakeClock(testStartTime)
	fetcher := kubernetes.NewUpdateInfoFetcher(mockUpdateInfoLister, testClock)
	assert.NoError(t, fetcher.Refresh())

	sdProcessor := NewScaleDownNodeProcessor(
		machinetypes.NewMachineConfigProvider(nil),
		mockMgr,
		experiments.NewMockManager(),
		fetcher,
		testDownsizeConfigProvider,
		calculator_test.New(),
		&mockScaleDownMetrics{},
		testClock,
	)
	sdProcessor.downsizePossibleSince = map[string]time.Time{
		node.Name: testStartTime.Add(-1 * time.Hour),
	}

	return sdProcessor, ctx
}
