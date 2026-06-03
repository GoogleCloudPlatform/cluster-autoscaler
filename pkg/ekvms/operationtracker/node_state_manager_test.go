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

package operationtracker

import (
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	v1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	resize_errors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/nodesizerecommender"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	ekvms_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	clock "k8s.io/utils/clock/testing"
)

type mockResizingProvider struct {
	resizingEnabled func(string) bool
	mcp             *machinetypes.MachineConfigProvider
}

func (p mockResizingProvider) ResizingEnabled(machineFamily string) bool {
	return p.resizingEnabled(machineFamily)
}

func (p mockResizingProvider) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return p.mcp
}

func newMockResizingProvider(resizingEnabled func(string) bool) mockResizingProvider {
	return mockResizingProvider{
		resizingEnabled: resizingEnabled,
		mcp:             machinetypes.NewMachineConfigProvider(nil),
	}
}

func TestSnapshotCreatesCopy(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			node := ekvms_test.NewResizableNodeBuilder("node", 32, 128).Build()
			nsm := &nodeStateManagerImpl{provider: newMockResizingProvider(func(string) bool { return true }),
				clock: clock.NewFakeClock(testStartTime),
				nodes: ResizableNodesSnapshot{
					node.Name: ResizableNode{
						DesiredSize:       size.Allocatable{},
						UpsizableMaxSize:  size.Allocatable{},
						PhysicalMaxSize:   size.Allocatable{},
						LastOperationTime: testStartTime,
						Node:              node,
						MachineFamily:     family,
					},
				},
				nodeCountByFamily: map[string]int{family: 1},
			}

			// Make a copy before updating underlying map.
			otherSnapshot := nsm.snapshot(true)
			assert.Equal(t, nsm.nodes[node.Name], otherSnapshot[node.Name])

			// Update of underlying map does not affect its copy.
			copyNode, exists := nsm.getNode(node.Name)
			assert.True(t, exists)
			copyNode.DesiredSize.MilliCpus = 16000
			copyNode.UpsizableMaxSize.MilliCpus = 32000
			nsm.setNode(node.Name, copyNode)
			assert.NotEqual(t, nsm.nodes[node.Name], otherSnapshot[node.Name])
		})
	}
}

func TestSnapshotCache(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			node := ekvms_test.NewResizableNodeBuilder("node", 32, 128).Build()
			testClock := clock.NewFakeClock(testStartTime)

			fullAllocatable := size.Allocatable{MilliCpus: 32000, KBytes: 128 * 1024 * 1024}
			halfAllocatable := size.Allocatable{MilliCpus: 16000, KBytes: 64 * 1024 * 1024}

			tests := []struct {
				name                   string
				forceRefresh           bool
				timeAdvance            time.Duration
				recommendationSequence []*nodesizerecommender.MaxSizeRecommendation
				expectedMaxSize        size.Allocatable
			}{
				{
					name:         "returns cached snapshot when fresh",
					forceRefresh: false,
					timeAdvance:  0,
					recommendationSequence: []*nodesizerecommender.MaxSizeRecommendation{
						{VmSize: size.VmSize(fullAllocatable), CreationTime: testStartTime},
					},
					expectedMaxSize: fullAllocatable,
				},
				{
					name:         "refreshes snapshot after TTL expires",
					forceRefresh: false,
					timeAdvance:  snapshotTTL,
					recommendationSequence: []*nodesizerecommender.MaxSizeRecommendation{
						{VmSize: size.VmSize(fullAllocatable), CreationTime: testStartTime},
						{VmSize: size.VmSize(halfAllocatable), CreationTime: testStartTime.Add(snapshotTTL)},
					},
					expectedMaxSize: halfAllocatable,
				},
				{
					name:         "refreshes snapshot when forced",
					forceRefresh: true,
					timeAdvance:  0,
					recommendationSequence: []*nodesizerecommender.MaxSizeRecommendation{
						{VmSize: size.VmSize(fullAllocatable), CreationTime: testStartTime},
						{VmSize: size.VmSize(halfAllocatable), CreationTime: testStartTime},
					},
					expectedMaxSize: halfAllocatable,
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					mockNodeSizeRecommender := &mockNodeSizeRecommender{}
					for _, rec := range tt.recommendationSequence {
						mockNodeSizeRecommender.On("MaxSize", node).Once().Return(rec)
					}

					nsm := &nodeStateManagerImpl{provider: newMockResizingProvider(func(string) bool { return true }),
						clock:               testClock,
						nodeSizeRecommender: mockNodeSizeRecommender,
						sizeCalculator:      &identitySizeCalculator{},
						nodes: ResizableNodesSnapshot{
							node.Name: ResizableNode{
								DesiredSize:       size.Allocatable{},
								UpsizableMaxSize:  size.Allocatable{},
								PhysicalMaxSize:   fullAllocatable,
								LastOperationTime: testStartTime,
								Node:              node,
								MachineFamily:     family,
							},
						},
						nodeCountByFamily: map[string]int{family: 1},
					}

					// Prime initial snapshot
					nsm.snapshot(false)

					// Advance time if necessary
					testClock.Sleep(tt.timeAdvance)

					// Second snapshot call
					snapshot := nsm.snapshot(tt.forceRefresh)

					// Validate UpsizableMaxSize after snapshot
					gotMaxSize := snapshot[node.Name].UpsizableMaxSize
					assert.Equal(t, tt.expectedMaxSize, gotMaxSize)
					mockNodeSizeRecommender.AssertExpectations(t)
				})
			}
		})
	}
}

func TestUpdateSnapshot(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			node := ekvms_test.NewResizableNodeBuilder("node", 32, 128).Build()
			nsm := &nodeStateManagerImpl{provider: newMockResizingProvider(func(string) bool { return true }),
				nodes: ResizableNodesSnapshot{
					node.Name: ResizableNode{
						DesiredSize:       size.Allocatable{},
						UpsizableMaxSize:  size.Allocatable{},
						PhysicalMaxSize:   size.Allocatable{},
						LastOperationTime: testStartTime,
						Node:              node,
						MachineFamily:     family,
					},
				},
				nodeCountByFamily: map[string]int{family: 1},
				clock:             clock.NewFakeClock(testStartTime),
			}

			testingSize := size.Allocatable{MilliCpus: 5, KBytes: 5}
			gotNode, exists := nsm.getNode(node.Name)
			assert.True(t, exists)
			assert.NotEqual(t, testingSize, gotNode.DesiredSize)
			nsm.setNodeSize(node, testingSize)

			gotUpdatedNode, exists := nsm.getNode(node.Name)
			assert.True(t, exists)
			assert.Equal(t, testingSize, gotUpdatedNode.DesiredSize)
		})
	}
}

func TestFilteredNodesSnapshot(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			fullAllocatable32 := size.Allocatable{MilliCpus: 32000, KBytes: 128 * 1024 * 1024}
			halfAllocatable32 := size.Allocatable{MilliCpus: 16000, KBytes: 64 * 1024 * 1024}
			downsizedAllocatable32 := size.Allocatable{MilliCpus: 1, KBytes: 1}
			nonResizableAllocatable := size.Allocatable{MilliCpus: 0, KBytes: 0}

			node := ekvms_test.NewResizableNodeBuilder("node", 32, 128).Build()
			node2 := ekvms_test.NewResizableNodeBuilder("node-2", 32, 128).Build()

			tests := []struct {
				name string

				startingSnapshot           ResizableNodesSnapshot
				maxAllocatable             size.Allocatable
				useNodeSizeRecommender     bool
				maxSizeRecommendations     map[string]*nodesizerecommender.MaxSizeRecommendation
				nodeIsBackedOff            bool
				nodeIsInUnknownResizeState bool
				nodeIsInFailedState        bool
				snapshotFilterMode         SnapshotFilterMode

				wantFilteredSnapshot ResizableNodesSnapshot
			}{
				{
					name: "recommendation higher than possible for machine type - trimmed",
					startingSnapshot: ResizableNodesSnapshot{
						node.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  halfAllocatable32,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node,
							MachineFamily:     family,
						},
					},
					useNodeSizeRecommender: true,
					maxSizeRecommendations: map[string]*nodesizerecommender.MaxSizeRecommendation{
						node.Name: {VmSize: size.VmSize{MilliCpus: 64000, KBytes: 256 * 1024 * 1024}, CreationTime: testStartTime},
					},
					snapshotFilterMode: ResizableOnly,
					wantFilteredSnapshot: ResizableNodesSnapshot{
						node.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  fullAllocatable32,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node,
							MachineFamily:     family,
						},
					},
				},
				{
					name: "recommendation lower than current UpsizableMaxSize",
					startingSnapshot: ResizableNodesSnapshot{
						node.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  fullAllocatable32,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node,
							MachineFamily:     family,
						},
					},
					useNodeSizeRecommender: true,
					maxSizeRecommendations: map[string]*nodesizerecommender.MaxSizeRecommendation{
						node.Name: {VmSize: size.VmSize(halfAllocatable32), CreationTime: testStartTime},
					},
					snapshotFilterMode: ResizableOnly,
					wantFilteredSnapshot: ResizableNodesSnapshot{
						node.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  halfAllocatable32,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node,
							MachineFamily:     family,
						},
					},
				},
				{
					name: "recommendation indicates non-resizable node",
					startingSnapshot: ResizableNodesSnapshot{
						node.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  fullAllocatable32,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node,
							MachineFamily:     family,
						},
					},
					useNodeSizeRecommender: true,
					maxSizeRecommendations: map[string]*nodesizerecommender.MaxSizeRecommendation{
						node.Name: {VmSize: size.VmSize(nonResizableAllocatable), CreationTime: testStartTime},
					},
					snapshotFilterMode:   ResizableOnly,
					wantFilteredSnapshot: ResizableNodesSnapshot{},
				},
				{
					name: "non-resizable node becomes resizable",
					startingSnapshot: ResizableNodesSnapshot{
						node.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  nonResizableAllocatable,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node,
							MachineFamily:     family,
						},
					},
					useNodeSizeRecommender: true,
					maxSizeRecommendations: map[string]*nodesizerecommender.MaxSizeRecommendation{
						node.Name: {VmSize: size.VmSize(halfAllocatable32), CreationTime: testStartTime},
					},
					snapshotFilterMode: ResizableOnly,
					wantFilteredSnapshot: ResizableNodesSnapshot{
						node.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  halfAllocatable32,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node,
							MachineFamily:     family,
						},
					},
				},
				{
					name: "missing recommendation",
					startingSnapshot: ResizableNodesSnapshot{
						node.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  fullAllocatable32,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node,
							MachineFamily:     family,
						},
					},
					useNodeSizeRecommender: true,
					maxSizeRecommendations: map[string]*nodesizerecommender.MaxSizeRecommendation{node.Name: nil},
					snapshotFilterMode:     ResizableOnly,
					wantFilteredSnapshot:   ResizableNodesSnapshot{},
				},
				{
					name: "no recommender",
					startingSnapshot: ResizableNodesSnapshot{
						node.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  fullAllocatable32,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node,
							MachineFamily:     family,
						},
					},
					useNodeSizeRecommender: false,
					snapshotFilterMode:     ResizableOnly,
					wantFilteredSnapshot:   ResizableNodesSnapshot{},
				},
				{
					name: "node is backed off",
					startingSnapshot: ResizableNodesSnapshot{
						node.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  fullAllocatable32,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node,
							MachineFamily:     family,
						},
					},
					useNodeSizeRecommender: true,
					maxSizeRecommendations: map[string]*nodesizerecommender.MaxSizeRecommendation{
						node.Name: {VmSize: size.VmSize(halfAllocatable32), CreationTime: testStartTime},
					},
					nodeIsBackedOff:      true,
					snapshotFilterMode:   ResizableOnly,
					wantFilteredSnapshot: ResizableNodesSnapshot{},
				},
				{
					name: "node is backed off, mode is DownsizableOnly",
					startingSnapshot: ResizableNodesSnapshot{
						node.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  fullAllocatable32,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node,
							MachineFamily:     family,
						},
					},
					useNodeSizeRecommender: true,
					maxSizeRecommendations: map[string]*nodesizerecommender.MaxSizeRecommendation{
						node.Name: {VmSize: size.VmSize(halfAllocatable32), CreationTime: testStartTime},
					},
					nodeIsBackedOff:      true,
					snapshotFilterMode:   DownsizableOnly,
					wantFilteredSnapshot: ResizableNodesSnapshot{},
				},
				{
					name: "node is backed off, mode is AllNodes",
					startingSnapshot: ResizableNodesSnapshot{
						node.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  fullAllocatable32,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node,
							MachineFamily:     family,
						},
					},
					useNodeSizeRecommender: true,
					maxSizeRecommendations: map[string]*nodesizerecommender.MaxSizeRecommendation{
						node.Name: {VmSize: size.VmSize(halfAllocatable32), CreationTime: testStartTime},
					},
					nodeIsBackedOff:    true,
					snapshotFilterMode: AllNodes,
					wantFilteredSnapshot: ResizableNodesSnapshot{
						node.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  halfAllocatable32,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node,
							MachineFamily:     family,
						},
					},
				},
				{
					name: "node is in unknown resize state",
					startingSnapshot: ResizableNodesSnapshot{
						node.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  fullAllocatable32,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node,
							MachineFamily:     family,
						},
					},
					useNodeSizeRecommender: true,
					maxSizeRecommendations: map[string]*nodesizerecommender.MaxSizeRecommendation{
						node.Name: {VmSize: size.VmSize(halfAllocatable32), CreationTime: testStartTime},
					},
					nodeIsInUnknownResizeState: true,
					snapshotFilterMode:         ResizableOnly,
					wantFilteredSnapshot:       ResizableNodesSnapshot{},
				},
				{
					name: "node is in failed state",
					startingSnapshot: ResizableNodesSnapshot{
						node.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  fullAllocatable32,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node,
							MachineFamily:     family,
						},
					},
					useNodeSizeRecommender: true,
					maxSizeRecommendations: map[string]*nodesizerecommender.MaxSizeRecommendation{
						node.Name: {VmSize: size.VmSize(halfAllocatable32), CreationTime: testStartTime},
					},
					nodeIsInFailedState:  true,
					snapshotFilterMode:   ResizableOnly,
					wantFilteredSnapshot: ResizableNodesSnapshot{},
				},
				{
					name: "node is in failed state, mode is DownsizableOnly",
					startingSnapshot: ResizableNodesSnapshot{
						node.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  fullAllocatable32,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node,
							MachineFamily:     family,
						},
					},
					useNodeSizeRecommender: true,
					maxSizeRecommendations: map[string]*nodesizerecommender.MaxSizeRecommendation{
						node.Name: {VmSize: size.VmSize(halfAllocatable32), CreationTime: testStartTime},
					},
					nodeIsInFailedState:  true,
					snapshotFilterMode:   DownsizableOnly,
					wantFilteredSnapshot: ResizableNodesSnapshot{},
				},
				{
					name: "node is in failed state, mode is AllNodes",
					startingSnapshot: ResizableNodesSnapshot{
						node.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  fullAllocatable32,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node,
							MachineFamily:     family,
						},
					},
					useNodeSizeRecommender: true,
					maxSizeRecommendations: map[string]*nodesizerecommender.MaxSizeRecommendation{
						node.Name: {VmSize: size.VmSize(halfAllocatable32), CreationTime: testStartTime},
					},
					nodeIsInFailedState: true,
					snapshotFilterMode:  AllNodes,
					wantFilteredSnapshot: ResizableNodesSnapshot{
						node.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  halfAllocatable32,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node,
							MachineFamily:     family,
						},
					},
				},
				{
					name: "get all nodes, resizable and non-resizable",
					startingSnapshot: ResizableNodesSnapshot{
						node.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  nonResizableAllocatable,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node,
							MachineFamily:     family,
						},
						node2.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  fullAllocatable32,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node2,
							MachineFamily:     family,
						},
					},
					useNodeSizeRecommender: true,
					maxSizeRecommendations: map[string]*nodesizerecommender.MaxSizeRecommendation{
						node.Name:  {VmSize: size.VmSize(nonResizableAllocatable), CreationTime: testStartTime},
						node2.Name: {VmSize: size.VmSize(halfAllocatable32), CreationTime: testStartTime},
					},
					snapshotFilterMode: AllNodes,
					wantFilteredSnapshot: ResizableNodesSnapshot{
						node.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  nonResizableAllocatable,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node,
							MachineFamily:     family,
						},
						node2.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  halfAllocatable32,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node2,
							MachineFamily:     family,
						},
					},
				},
				{
					name: "get all DownsizableOnly nodes",
					startingSnapshot: ResizableNodesSnapshot{
						node.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  nonResizableAllocatable,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node,
							MachineFamily:     family,
						},
						node2.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  fullAllocatable32,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node2,
							MachineFamily:     family,
						},
					},
					useNodeSizeRecommender: true,
					maxSizeRecommendations: map[string]*nodesizerecommender.MaxSizeRecommendation{
						node.Name:  {VmSize: size.VmSize(nonResizableAllocatable), CreationTime: testStartTime},
						node2.Name: {VmSize: size.VmSize(halfAllocatable32), CreationTime: testStartTime},
					},
					nodeIsInFailedState: true,
					snapshotFilterMode:  DownsizableOnly,
					wantFilteredSnapshot: ResizableNodesSnapshot{
						node2.Name: {
							DesiredSize:       downsizedAllocatable32,
							UpsizableMaxSize:  halfAllocatable32,
							PhysicalMaxSize:   fullAllocatable32,
							LastOperationTime: testStartTime,
							Node:              node2,
							MachineFamily:     family,
						},
					},
				},
			}
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					mockBackoff := &mockBackoff{}
					mockBackoff.On("IsBackedOff", family, node.Name).Once().Return(tc.nodeIsBackedOff)
					mockBackoff.On("IsBackedOff", family, node2.Name).Once().Return(tc.nodeIsBackedOff)

					nsm := &nodeStateManagerImpl{provider: newMockResizingProvider(func(string) bool { return true }),
						backoffManager:    mockBackoff,
						clock:             clock.NewFakeClock(testStartTime),
						sizeCalculator:    &identitySizeCalculator{},
						nodes:             tc.startingSnapshot,
						unhealthyNodes:    map[string]UnhealthyResizableNodeStatus{},
						nodesWithNoRecom:  map[string]time.Time{},
						nodeCountByFamily: map[string]int{family: len(tc.startingSnapshot)},
					}

					if tc.useNodeSizeRecommender {
						nodeSizeRecommender := &mockNodeSizeRecommender{}
						for nodeName, rec := range tc.maxSizeRecommendations {
							ekNode := tc.startingSnapshot[nodeName]
							nodeSizeRecommender.On("MaxSize", ekNode.Node).Once().Return(rec)
						}
						nsm.nodeSizeRecommender = nodeSizeRecommender
					}
					if tc.nodeIsInUnknownResizeState {
						nsm.setNodeAsUnhealthy(node.Name, UnknownResizeStatus)
					}
					if tc.nodeIsInFailedState {
						nsm.setNodeAsUnhealthy(node.Name, FailedResizeStatus)
					}

					gotSnapshot := nsm.filteredNodesSnapshot(true, tc.snapshotFilterMode)
					assert.Equal(t, tc.wantFilteredSnapshot, gotSnapshot)
				})
			}
		})
	}
}

func TestNodesCount(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			tests := []struct {
				description       string
				snapshot          ResizableNodesSnapshot
				nodeCountByFamily map[string]int
				machineFamily     string
				wantNodeCount     int
			}{
				{
					description:       "no nodes",
					snapshot:          ResizableNodesSnapshot{},
					nodeCountByFamily: map[string]int{},
					machineFamily:     family,
					wantNodeCount:     0,
				},
				{
					description: "one node",
					snapshot: ResizableNodesSnapshot{
						"node-1": {
							DesiredSize:   size.Allocatable{},
							Node:          nil,
							MachineFamily: family,
						},
					},
					nodeCountByFamily: map[string]int{family: 1},
					machineFamily:     family,
					wantNodeCount:     1,
				},
				{
					description: "multiple nodes",
					snapshot: ResizableNodesSnapshot{
						"node-1": {
							DesiredSize:   size.Allocatable{},
							Node:          nil,
							MachineFamily: family,
						},
						"node-2": {
							DesiredSize:   size.Allocatable{},
							Node:          nil,
							MachineFamily: family,
						},
						"node-3": {
							DesiredSize:   size.Allocatable{},
							Node:          nil,
							MachineFamily: family,
						},
					},
					nodeCountByFamily: map[string]int{family: 3},
					machineFamily:     family,
					wantNodeCount:     3,
				},
			}
			for _, tt := range tests {
				t.Run(tt.description, func(t *testing.T) {
					nsm := &nodeStateManagerImpl{provider: newMockResizingProvider(func(string) bool { return true }),
						nodes:             tt.snapshot,
						nodeCountByFamily: tt.nodeCountByFamily,
					}

					assert.Equal(t, tt.wantNodeCount, nsm.nodesCount(tt.machineFamily))
				})
			}
		})
	}
}

func TestSetGetNode(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			fullAllocatable32 := size.Allocatable{MilliCpus: 32000, KBytes: 128 * 1024 * 1024}
			halfAllocatable32 := size.Allocatable{MilliCpus: 16000, KBytes: 64 * 1024 * 1024}
			node := ekvms_test.NewResizableNodeBuilder("node", 32, 128).Build()
			mockNodeSizeRecommender := &mockNodeSizeRecommender{}
			mockNodeSizeRecommender.On("MaxSize", node).Once().Return(&nodesizerecommender.MaxSizeRecommendation{VmSize: size.VmSize(halfAllocatable32), CreationTime: testStartTime})
			nsm := &nodeStateManagerImpl{provider: newMockResizingProvider(func(string) bool { return true }),
				nodeSizeRecommender: mockNodeSizeRecommender,
				sizeCalculator:      &identitySizeCalculator{},
				nodes:               make(ResizableNodesSnapshot),
				nodeCountByFamily:   make(map[string]int),
			}

			_, exists := nsm.getNode(node.Name)
			assert.False(t, exists)
			assert.Len(t, nsm.nodes, 0)

			nsm.setNode(node.Name, ResizableNode{
				DesiredSize:       fullAllocatable32,
				UpsizableMaxSize:  fullAllocatable32,
				PhysicalMaxSize:   fullAllocatable32,
				LastOperationTime: testStartTime,
				Node:              node,
				MachineFamily:     family,
			})

			// Access directly.
			gotNode, exists := nsm.nodes[node.Name]
			assert.True(t, exists)
			assert.Equal(t, fullAllocatable32, gotNode.DesiredSize)
			assert.Equal(t, fullAllocatable32, gotNode.UpsizableMaxSize)
			assert.Equal(t, fullAllocatable32, gotNode.PhysicalMaxSize)

			// Access through getNode, this always returns the most recent UAS recommendation for upsizibility.
			gotNode, exists = nsm.getNode(node.Name)
			assert.True(t, exists)
			assert.Equal(t, halfAllocatable32, gotNode.UpsizableMaxSize)
			assert.Equal(t, fullAllocatable32, gotNode.DesiredSize)
			assert.Equal(t, fullAllocatable32, gotNode.PhysicalMaxSize)
			assert.Equal(t, 1, nsm.nodesCount(family))
			mockNodeSizeRecommender.AssertExpectations(t)
		})
	}
}

func TestSetNodeAsHealthyAndUnhealthy(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			nsm := &nodeStateManagerImpl{provider: newMockResizingProvider(func(string) bool { return true }),
				backoffManager: &mockBackoff{},
				nodes: createTestSnapshotByNames([]string{
					"node-1",
					"node-2",
					"node-3",
				}, family),
				unhealthyNodes: map[string]UnhealthyResizableNodeStatus{},
				clock:          clock.NewFakeClock(testStartTime),
			}

			assert.Equal(t, nsm.unhealthyNodes, map[string]UnhealthyResizableNodeStatus{})

			nsm.setNodeAsUnhealthy("node-1", UnknownResizeStatus)
			nsm.setNodeAsUnhealthy("node-2", FailedResizeStatus)
			assert.Equal(t, nsm.unhealthyNodes, map[string]UnhealthyResizableNodeStatus{
				"node-1": UnknownResizeStatus,
				"node-2": FailedResizeStatus,
			})

			// Nothing changed as we are marking a node as healthy that was already healthy.
			nsm.setNodeAsHealthy("node-3")
			assert.Equal(t, nsm.unhealthyNodes, map[string]UnhealthyResizableNodeStatus{
				"node-1": UnknownResizeStatus,
				"node-2": FailedResizeStatus,
			})

			nsm.setNodeAsHealthy("node-1")
			nsm.setNodeAsHealthy("node-2")
			assert.Equal(t, nsm.unhealthyNodes, map[string]UnhealthyResizableNodeStatus{})
		})
	}
}

func TestGetUnhealthyNodesWithStatus(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			tests := []struct {
				name           string
				nodes          ResizableNodesSnapshot
				unhealthyNodes map[string]UnhealthyResizableNodeStatus
				status         UnhealthyResizableNodeStatus
				wantNodes      []string
			}{
				{
					name: "unknown status: no unhealthy nodes",
					nodes: createTestSnapshotByNames([]string{
						"node-1",
						"node-2",
					}, family),
					unhealthyNodes: map[string]UnhealthyResizableNodeStatus{},
					status:         UnknownResizeStatus,
					wantNodes:      []string{},
				},
				{
					name:  "unknown status: unhealthy nodes exist, but none are in unknownResizeState",
					nodes: createTestSnapshotByNames([]string{"node-1", "node-2"}, family),
					unhealthyNodes: map[string]UnhealthyResizableNodeStatus{
						"node-1": FailedResizeStatus,
						"node-2": FailedResizeStatus,
					},
					status:    UnknownResizeStatus,
					wantNodes: []string{},
				},
				{
					name: "unknown status: unhealthy nodes exist, some are in unknownResizeState, and all exist in nodes",
					nodes: createTestSnapshotByNames([]string{
						"node-1",
						"node-2",
						"node-3",
					}, family),
					unhealthyNodes: map[string]UnhealthyResizableNodeStatus{
						"node-1": UnknownResizeStatus, // Match
						"node-2": FailedResizeStatus,
						"node-3": UnknownResizeStatus, // Match
					},
					status:    UnknownResizeStatus,
					wantNodes: []string{"node-1", "node-3"},
				},
				{
					name:  "unknown status: unhealthy nodes exist, some are in unknownResizeState, but some do not exist in nodes",
					nodes: createTestSnapshotByNames([]string{"node-1", "node-3"}, family),
					unhealthyNodes: map[string]UnhealthyResizableNodeStatus{
						"node-1": UnknownResizeStatus, // Match
						"node-2": UnknownResizeStatus, // This node is not in nodes
						"node-3": FailedResizeStatus,
					},
					status:    UnknownResizeStatus,
					wantNodes: []string{"node-1"},
				},
				{
					name: "failed status: no unhealthy nodes",
					nodes: createTestSnapshotByNames([]string{
						"node-1",
						"node-2",
					}, family),
					unhealthyNodes: map[string]UnhealthyResizableNodeStatus{},
					status:         FailedResizeStatus,
					wantNodes:      []string{},
				},
				{
					name:  "failed status: unhealthy nodes exist, but none are in failedState",
					nodes: createTestSnapshotByNames([]string{"node-1", "node-2"}, family),
					unhealthyNodes: map[string]UnhealthyResizableNodeStatus{
						"node-1": UnknownResizeStatus,
						"node-2": UnknownResizeStatus,
					},
					status:    FailedResizeStatus,
					wantNodes: []string{},
				},
				{
					name: "failed status: unhealthy nodes exist, some are in failedState, and all exist in nodes",
					nodes: createTestSnapshotByNames([]string{
						"node-1",
						"node-2",
						"node-3",
					}, family),
					unhealthyNodes: map[string]UnhealthyResizableNodeStatus{
						"node-1": FailedResizeStatus, // Match
						"node-2": UnknownResizeStatus,
						"node-3": FailedResizeStatus, // Match
					},
					status:    FailedResizeStatus,
					wantNodes: []string{"node-1", "node-3"},
				},
				{
					name: "failed status: unhealthy nodes exist, some are in failedState, but some do not exist in nodes",
					nodes: createTestSnapshotByNames([]string{
						"node-1",
						"node-3",
					}, family),
					unhealthyNodes: map[string]UnhealthyResizableNodeStatus{
						"node-1": FailedResizeStatus, // Match
						"node-2": FailedResizeStatus, // This node is not in nodes
						"node-3": UnknownResizeStatus,
					},
					status:    FailedResizeStatus,
					wantNodes: []string{"node-1"},
				},
				{
					name: "multiple machine families: returns all matching nodes",
					nodes: ResizableNodesSnapshot{
						"node-1": {MachineFamily: family, Node: &v1.Node{}},
						"other":  {MachineFamily: "other", Node: &v1.Node{}},
					},
					unhealthyNodes: map[string]UnhealthyResizableNodeStatus{
						"node-1": FailedResizeStatus,
						"other":  FailedResizeStatus,
					},
					status:    FailedResizeStatus,
					wantNodes: []string{"node-1", "other"},
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					nsm := &nodeStateManagerImpl{provider: newMockResizingProvider(func(string) bool { return true }),
						nodes:          tt.nodes,
						unhealthyNodes: tt.unhealthyNodes,
						clock:          clock.NewFakeClock(testStartTime),
					}
					gotNodes := nsm.getUnhealthyNodesWithStatus(tt.status)
					assert.ElementsMatch(t, tt.wantNodes, gotNodes)
				})
			}
		})
	}
}

func TestDeleteNode(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			tc := []struct {
				description    string
				nodes          []string
				unhealthyNodes map[string]UnhealthyResizableNodeStatus
				nodesToDelete  []string
				wantNodes      []string
			}{
				{
					description: "single node deletion",
					nodes: []string{
						"node-1",
						"node-2",
						"node-3",
						"node-4-unknown",
						"node-5-failed",
					},
					unhealthyNodes: map[string]UnhealthyResizableNodeStatus{
						"node-4-unknown": UnknownResizeStatus,
						"node-5-failed":  FailedResizeStatus,
					},
					nodesToDelete: []string{"node-1"},
					wantNodes: []string{
						"node-2",
						"node-3",
						"node-4-unknown",
						"node-5-failed",
					},
				},
				{
					description: "multiple node deletions",
					nodes: []string{
						"node-1",
						"node-2",
						"node-3",
						"node-4-unknown",
						"node-5-failed",
					},
					unhealthyNodes: map[string]UnhealthyResizableNodeStatus{
						"node-4-unknown": UnknownResizeStatus,
						"node-5-failed":  FailedResizeStatus,
					},
					nodesToDelete: []string{"node-1", "node-2", "node-4-unknown", "node-5-failed"},
					wantNodes: []string{
						"node-3",
					},
				},
			}

			for _, tc := range tc {
				t.Run(tc.description, func(t *testing.T) {
					mockBackoff := &mockBackoff{}
					for _, nodeName := range tc.nodesToDelete {
						mockBackoff.On("DeleteNode", family, nodeName).Once()
					}
					nsm := &nodeStateManagerImpl{provider: newMockResizingProvider(func(string) bool { return true }),
						backoffManager:    mockBackoff,
						nodes:             createTestSnapshotByNames(tc.nodes, family),
						unhealthyNodes:    tc.unhealthyNodes,
						nodeCountByFamily: map[string]int{family: len(tc.nodes)},
					}

					for _, nodeName := range tc.nodesToDelete {
						nsm.deleteNode(nodeName)
					}

					assert.ElementsMatch(t, tc.wantNodes, mapKeys(nsm.nodes))
					for _, nodeName := range tc.nodesToDelete {
						assert.NotContains(t, nsm.unhealthyNodes, nodeName)
					}
					mockBackoff.AssertExpectations(t)
				})
			}
		})
	}
}

func TestEkNodeMissingRecommendationLogging(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			tests := []struct {
				name                     string
				maxSizeRecommendations   map[string]*nodesizerecommender.MaxSizeRecommendation
				timePassed               time.Duration
				startingNodesWithNoRecom map[string]time.Time
				nodesToDelete            []string
				wantNodesWithNoRecom     map[string]time.Time
			}{
				{
					name:                     "Node w/o recommendations does not have recommendatons again within maxSizeRecomLogInterval",
					startingNodesWithNoRecom: map[string]time.Time{"node1": testStartTime},
					maxSizeRecommendations:   map[string]*nodesizerecommender.MaxSizeRecommendation{"node1": nil},
					timePassed:               10 * time.Second,
					wantNodesWithNoRecom: map[string]time.Time{
						"node1": testStartTime,
					},
				},
				{
					name:                     "Node w/o recommendations does not have recommendations again after maxSizeRecomLogInterval",
					startingNodesWithNoRecom: map[string]time.Time{"node1": testStartTime},
					maxSizeRecommendations:   map[string]*nodesizerecommender.MaxSizeRecommendation{"node1": nil},
					timePassed:               20 * time.Second,
					wantNodesWithNoRecom: map[string]time.Time{
						"node1": testStartTime.Add(20 * time.Second),
					},
				},
				{
					name:                     "Node w/o recommendations have the recommendations now",
					startingNodesWithNoRecom: map[string]time.Time{"node1": testStartTime},
					maxSizeRecommendations: map[string]*nodesizerecommender.MaxSizeRecommendation{
						"node1": {VmSize: size.VmSize{MilliCpus: 64000, KBytes: 256 * 1024 * 1024}, CreationTime: testStartTime},
					},
					timePassed:           10 * time.Second,
					wantNodesWithNoRecom: map[string]time.Time{},
				},
				{
					name:                     "Node with recommendation is getting deleted",
					startingNodesWithNoRecom: map[string]time.Time{"node1": testStartTime},
					maxSizeRecommendations: map[string]*nodesizerecommender.MaxSizeRecommendation{
						"node1": {VmSize: size.VmSize{MilliCpus: 64000, KBytes: 256 * 1024 * 1024}, CreationTime: testStartTime},
					},
					timePassed:           10 * time.Second,
					nodesToDelete:        []string{"node1"},
					wantNodesWithNoRecom: map[string]time.Time{},
				},
			}
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					startingSnapshot := createTestSnapshotByNames([]string{"node1"}, family)
					mockBackoff := &mockBackoff{}
					testClock := clock.NewFakeClock(testStartTime)
					nsm := &nodeStateManagerImpl{provider: newMockResizingProvider(func(string) bool { return true }),
						backoffManager:    mockBackoff,
						clock:             testClock,
						sizeCalculator:    &identitySizeCalculator{},
						nodes:             startingSnapshot,
						unhealthyNodes:    map[string]UnhealthyResizableNodeStatus{},
						nodesWithNoRecom:  tc.startingNodesWithNoRecom,
						nodeCountByFamily: map[string]int{family: 1},
					}
					nodeSizeRecommender := &mockNodeSizeRecommender{}
					for nodeName, rec := range tc.maxSizeRecommendations {
						node := startingSnapshot[nodeName]
						nodeSizeRecommender.On("MaxSize", node.Node).Return(rec)
					}
					nsm.nodeSizeRecommender = nodeSizeRecommender
					// Call maxRecommendedNodeSize "timePassed" seconds after the startTime and check nodesWithNoRecom.
					testClock.Step(tc.timePassed)
					for _, node := range nsm.nodes {
						nsm.maxRecommendedNodeSize(node)
					}
					for _, nodeName := range tc.nodesToDelete {
						mockBackoff.On("DeleteNode", family, nodeName).Once()
						nsm.deleteNode(nodeName)
					}
					assert.Equal(t, tc.wantNodesWithNoRecom, nsm.nodesWithNoRecom)
				})
			}
		})
	}
}

func TestMultiFamilyNodesCount(t *testing.T) {
	testClock := clock.NewFakeClock(testStartTime)
	mockBackoff := &mockBackoff{}
	mockBackoff.On("DeleteNode", mock.Anything, mock.Anything).Return()
	nsm := NewNodeStateManager(newMockResizingProvider(func(string) bool { return true }), nil, mockBackoff, &identitySizeCalculator{}, testClock)

	nodeEK1 := ekvms_test.NewResizableNodeBuilder("ek-node-1", 32, 128).Build()
	nodeEK2 := ekvms_test.NewResizableNodeBuilder("ek-node-2", 32, 128).Build()
	nodeE4A1 := ekvms_test.NewResizableNodeBuilder("e4a-node-1", 32, 128).Build()

	nsm.setNode(nodeEK1.Name, ResizableNode{Node: nodeEK1, MachineFamily: "ek"})
	nsm.setNode(nodeEK2.Name, ResizableNode{Node: nodeEK2, MachineFamily: "ek"})
	nsm.setNode(nodeE4A1.Name, ResizableNode{Node: nodeE4A1, MachineFamily: "e4a"})

	assert.Equal(t, 2, nsm.nodesCount("ek"))
	assert.Equal(t, 1, nsm.nodesCount("e4a"))
	assert.Equal(t, 0, nsm.nodesCount("other"))

	nsm.deleteNode(nodeEK1.Name)
	assert.Equal(t, 1, nsm.nodesCount("ek"))
	assert.Equal(t, 1, nsm.nodesCount("e4a"))

	nsm.deleteNode(nodeE4A1.Name)
	assert.Equal(t, 1, nsm.nodesCount("ek"))
	assert.Equal(t, 0, nsm.nodesCount("e4a"))
}

func createEkSnapshot(nodeNames []string) ResizableNodesSnapshot {
	return createTestSnapshotByNames(nodeNames, "ek")
}

func createTestSnapshotByNames(nodeNames []string, family string) ResizableNodesSnapshot {
	snapshot := ResizableNodesSnapshot{}
	for _, nodeName := range nodeNames {
		snapshot[nodeName] = ResizableNode{
			Node:              ekvms_test.NewResizableNodeBuilder(nodeName, 32, 128).Build(),
			MachineFamily:     family,
			DesiredSize:       size.Allocatable{},
			UpsizableMaxSize:  size.Allocatable{},
			PhysicalMaxSize:   size.Allocatable{},
			LastOperationTime: testStartTime,
		}
	}
	return snapshot
}

func mapKeys[K comparable, V any](mp map[K]V) []K {
	return slices.Collect(maps.Keys(mp))
}

type mockBackoff struct {
	mock.Mock
}

func (m *mockBackoff) Backoff(node *v1.Node, resizeError resize_errors.ResizeError) {
	m.Called(node, resizeError)
}

func (m *mockBackoff) IsBackedOff(machineFamily string, nodeName string) bool {
	args := m.Called(machineFamily, nodeName)
	return args.Get(0).(bool)
}

func (m *mockBackoff) DeleteNode(machineFamily string, nodeName string) {
	m.Called(machineFamily, nodeName)
}

func (m *mockBackoff) Run(stopCh <-chan struct{}) {
	m.Called(stopCh)
}

func (m *mockBackoff) RefreshCustomThresholds(em experiments.Manager) {
	m.Called(em)
}
