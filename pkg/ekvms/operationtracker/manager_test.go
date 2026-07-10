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
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	v1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/nodesizerecommender"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/test"
	clock "k8s.io/utils/clock/testing"
)

func TestUpsize(t *testing.T) {
	fullAllocatable32 := size.Allocatable{MilliCpus: 32000, KBytes: 128 * 1024 * 1024}
	halfAllocatable32 := size.Allocatable{MilliCpus: 16000, KBytes: 64 * 1024 * 1024}
	downsizedAllocatableEk32 := size.Allocatable{MilliCpus: 1, KBytes: 1}
	upsizeTime := testStartTime.Add(5 * time.Second)
	node := test.NewResizableNodeBuilder(testResizableNodeName, 8000, 32).Build()

	for _, family := range []string{"ek", "e4a"} {
		mockBackoff := &mockBackoff{}
		mockBackoff.On("IsBackedOff", family, mock.Anything).Return(false)

		tests := []struct {
			name string

			node                  *v1.Node
			snapshot              ResizableNodesSnapshot
			proposedSize          size.Allocatable
			wantUpsize            bool
			wantErr               string
			wantLastOperationTime time.Time
			maxSizeRecommendation *nodesizerecommender.MaxSizeRecommendation
		}{
			{
				name: "successful upsize for valid node",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:     downsizedAllocatableEk32,
						PhysicalMaxSize: fullAllocatable32,
						Node:            node,
						MachineFamily:   family,
					},
				},
				maxSizeRecommendation: &nodesizerecommender.MaxSizeRecommendation{VmSize: size.VmSize(fullAllocatable32), CreationTime: testStartTime},
				proposedSize:          fullAllocatable32,
				wantUpsize:            true,
				wantErr:               "",
				wantLastOperationTime: upsizeTime,
			},
			{
				name: "returns error for invalid node (not present in resizable node snapshot)",
				node: node,
				snapshot: ResizableNodesSnapshot{
					"different-node": {
						DesiredSize:     downsizedAllocatableEk32,
						PhysicalMaxSize: fullAllocatable32,
						Node:            node,
						MachineFamily:   family,
					},
				},
				proposedSize: fullAllocatable32,
				wantUpsize:   false,
				wantErr:      "node .*? is not present in resizable node snapshot",
			},
			{
				name: "returns error for node with missing max size recommendations",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:     downsizedAllocatableEk32,
						PhysicalMaxSize: fullAllocatable32,
						Node:            node,
						MachineFamily:   family,
					},
				},
				maxSizeRecommendation: nil,
				proposedSize:          fullAllocatable32,
				wantUpsize:            false,
				wantErr:               "node .*? is non-resizable",
			},
			{
				name: "returns error for downsize request in both dimensions",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:     fullAllocatable32,
						PhysicalMaxSize: fullAllocatable32,
						Node:            node,
						MachineFamily:   family,
					},
				},
				maxSizeRecommendation: &nodesizerecommender.MaxSizeRecommendation{VmSize: size.VmSize(fullAllocatable32), CreationTime: testStartTime},
				proposedSize:          downsizedAllocatableEk32,
				wantUpsize:            false,
				wantErr:               "not an upsize",
			},
			{
				name: "returns error for downsize request by cpu",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:     fullAllocatable32,
						PhysicalMaxSize: fullAllocatable32,
						Node:            node,
						MachineFamily:   family,
					},
				},
				maxSizeRecommendation: &nodesizerecommender.MaxSizeRecommendation{VmSize: size.VmSize(fullAllocatable32), CreationTime: testStartTime},
				proposedSize:          size.Allocatable{MilliCpus: downsizedAllocatableEk32.MilliCpus, KBytes: fullAllocatable32.KBytes},
				wantUpsize:            false,
				wantErr:               "not an upsize",
			},
			{
				name: "returns error for downsize request by memory",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:     fullAllocatable32,
						PhysicalMaxSize: fullAllocatable32,
						Node:            node,
						MachineFamily:   family,
					},
				},
				maxSizeRecommendation: &nodesizerecommender.MaxSizeRecommendation{VmSize: size.VmSize(fullAllocatable32), CreationTime: testStartTime},
				proposedSize:          size.Allocatable{MilliCpus: fullAllocatable32.MilliCpus, KBytes: downsizedAllocatableEk32.KBytes},
				wantUpsize:            false,
				wantErr:               "not an upsize",
			},
			{
				name: "returns error if upsize exceeds max size by cpu",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:     downsizedAllocatableEk32,
						PhysicalMaxSize: fullAllocatable32,
						Node:            node,
						MachineFamily:   family,
					},
				},
				maxSizeRecommendation: &nodesizerecommender.MaxSizeRecommendation{VmSize: size.VmSize(fullAllocatable32), CreationTime: testStartTime},
				proposedSize:          size.Add(fullAllocatable32, size.Allocatable{MilliCpus: 100, KBytes: 0}),
				wantUpsize:            false,
				wantErr:               "node .*? proposed size .*? exceeds maximum possible size",
			},
			{
				name: "returns error if upsize exceeds max size by memory",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:     downsizedAllocatableEk32,
						PhysicalMaxSize: fullAllocatable32,
						Node:            node,
						MachineFamily:   family,
					},
				},
				maxSizeRecommendation: &nodesizerecommender.MaxSizeRecommendation{VmSize: size.VmSize(fullAllocatable32), CreationTime: testStartTime},
				proposedSize:          size.Add(fullAllocatable32, size.Allocatable{MilliCpus: 0, KBytes: 100}),
				wantUpsize:            false,
				wantErr:               "node .*? proposed size .*? exceeds maximum possible size",
			},
			{
				name: "returns error if upsize exceeds max size in both dimensions",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:     downsizedAllocatableEk32,
						PhysicalMaxSize: fullAllocatable32,
						Node:            node,
						MachineFamily:   family,
					},
				},
				maxSizeRecommendation: &nodesizerecommender.MaxSizeRecommendation{VmSize: size.VmSize(fullAllocatable32), CreationTime: testStartTime},
				proposedSize:          size.Add(fullAllocatable32, size.Allocatable{MilliCpus: 100, KBytes: 100}),
				wantUpsize:            false,
				wantErr:               "node .*? proposed size .*? exceeds maximum possible size",
			},
			{
				name: "returns error when node size is already maxed out in both dimensions",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:     fullAllocatable32,
						PhysicalMaxSize: fullAllocatable32,
						Node:            node,
						MachineFamily:   family,
					},
				},
				maxSizeRecommendation: &nodesizerecommender.MaxSizeRecommendation{VmSize: size.VmSize(fullAllocatable32), CreationTime: testStartTime},
				proposedSize:          fullAllocatable32,
				wantUpsize:            false,
				wantErr:               "not an upsize",
			},
			{
				name: "returns error when node size is already at proposed size",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:     halfAllocatable32,
						PhysicalMaxSize: fullAllocatable32,
						Node:            node,
						MachineFamily:   family,
					},
				},
				maxSizeRecommendation: &nodesizerecommender.MaxSizeRecommendation{VmSize: size.VmSize(fullAllocatable32), CreationTime: testStartTime},
				proposedSize:          halfAllocatable32,
				wantUpsize:            false,
				wantErr:               "not an upsize",
			},
			{
				name: "starts upsize for valid node and size recommendation above maxSize",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:     halfAllocatable32,
						PhysicalMaxSize: fullAllocatable32,
						Node:            node,
						MachineFamily:   family,
					},
				},
				proposedSize:          fullAllocatable32,
				wantUpsize:            true,
				wantErr:               "",
				wantLastOperationTime: upsizeTime,
				maxSizeRecommendation: &nodesizerecommender.MaxSizeRecommendation{VmSize: size.VmSize(size.Add(fullAllocatable32, halfAllocatable32)), CreationTime: testStartTime},
			},
			{
				name: "starts upsize for valid node and size recommendation above proposedSize and below maxSize",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:     downsizedAllocatableEk32,
						PhysicalMaxSize: fullAllocatable32,
						Node:            node,
						MachineFamily:   family,
					},
				},
				proposedSize:          halfAllocatable32,
				wantUpsize:            true,
				wantErr:               "",
				wantLastOperationTime: upsizeTime,
				maxSizeRecommendation: &nodesizerecommender.MaxSizeRecommendation{VmSize: size.VmSize(fullAllocatable32), CreationTime: testStartTime},
			},
			{
				name: "returns error if proposed size exceeds recommended size",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:     downsizedAllocatableEk32,
						PhysicalMaxSize: fullAllocatable32,
						Node:            node,
						MachineFamily:   family,
					},
				},
				proposedSize:          halfAllocatable32,
				wantUpsize:            false,
				wantErr:               "node .*? proposed size .*? exceeds maximum possible size",
				maxSizeRecommendation: &nodesizerecommender.MaxSizeRecommendation{VmSize: size.VmSize(downsizedAllocatableEk32), CreationTime: testStartTime},
			},
		}
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%s-%s", family, tt.name), func(t *testing.T) {
				sizeCalculator := &identitySizeCalculator{}
				testClock := clock.NewFakeClock(testStartTime)
				nodeSizeRecommender := &mockNodeSizeRecommender{}
				nodeSizeRecommender.On("MaxSize", tt.node).Maybe().Return(tt.maxSizeRecommendation)
				mockTracker := &mockOperationTracker{}
				mockTracker.On("Resize", mock.Anything).Return()
				nodeStateManager := NewNodeStateManager(newMockResizingProvider(func(string) bool { return true }), nodeSizeRecommender, mockBackoff, sizeCalculator, testClock)
				nodeStateManager.nodes = tt.snapshot

				m := &ManagerImpl{
					tracker:          mockTracker,
					sizeCalculator:   sizeCalculator,
					clock:            testClock,
					nodeStateManager: nodeStateManager,
				}
				originalSnapshot := m.FilteredNodesSnapshot(true, AllNodes)
				testClock.SetTime(upsizeTime)
				err := m.Upsize(tt.node, tt.proposedSize)

				if tt.wantErr == "" {
					assert.NoError(t, err)
				} else if assert.Error(t, err) {
					match, matchErr := regexp.MatchString(tt.wantErr, err.Error())
					assert.NoError(t, matchErr)
					assert.True(t, match, "Expected error [%s], but got [%s]", tt.wantErr, err.Error())
				}

				if tt.wantUpsize {
					mockTracker.AssertNumberOfCalls(t, "Resize", 1)
					mockTracker.AssertCalled(
						t,
						"Resize",
						mock.MatchedBy(func(op ResizeOperation) bool {
							desiredSizeCheck := assert.Equal(t, size.VmSize(tt.proposedSize), op.DesiredSize, "DesiredSize check error")
							startingSizeCheck := assert.Equal(t, size.VmSize(originalSnapshot[testResizableNodeName].DesiredSize), op.StartingSize, "StartingSize check error")
							return desiredSizeCheck && startingSizeCheck
						}),
					)
					assert.Equal(t, tt.wantLastOperationTime, m.FilteredNodesSnapshot(true, AllNodes)[testResizableNodeName].LastOperationTime)
				} else {
					// Snapshot was not modified.
					mockTracker.AssertNumberOfCalls(t, "Resize", 0)
					assert.Equal(t, originalSnapshot, m.FilteredNodesSnapshot(true, AllNodes))
				}
				nodeSizeRecommender.AssertExpectations(t)
			})
		}
	}
}

func TestDownsize(t *testing.T) {
	sizeCalc := &identitySizeCalculator{}
	node := test.NewResizableNodeBuilder(testResizableNodeName, 8000, 32).Build()
	fullAllocatable32 := size.Allocatable{MilliCpus: 32000, KBytes: 128 * 1024 * 1024}
	halfAllocatable32 := size.Allocatable{MilliCpus: 16000, KBytes: 64 * 1024 * 1024}
	nonReziableSize := size.Allocatable{MilliCpus: 0, KBytes: 0}
	downsizedAllocatableEk32 := size.Allocatable{MilliCpus: 1, KBytes: 1}
	downsizeTime := testStartTime.Add(5 * time.Second)
	minAllocatable, err := sizeCalc.MinAllocatable(node)
	assert.NoError(t, err)
	maxSizeRecommendation := &nodesizerecommender.MaxSizeRecommendation{VmSize: size.VmSize(fullAllocatable32), CreationTime: testStartTime}
	maxSizeRecommendationNonResizable := &nodesizerecommender.MaxSizeRecommendation{VmSize: size.VmSize(nonReziableSize), CreationTime: testStartTime}

	for _, family := range []string{"ek", "e4a"} {
		mockBackoff := &mockBackoff{}
		mockBackoff.On("IsBackedOff", family, mock.Anything).Return(false)

		tests := []struct {
			name string

			node                  *v1.Node
			snapshot              ResizableNodesSnapshot
			maxSizeRecommendation *nodesizerecommender.MaxSizeRecommendation
			proposedSize          size.Allocatable
			wantDownsize          bool
			wantErr               string
			wantLastOperationTime time.Time
		}{
			{
				name: "starts downsize for valid node",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:     fullAllocatable32,
						PhysicalMaxSize: fullAllocatable32,
						Node:            node,
						MachineFamily:   family,
					},
				},
				maxSizeRecommendation: maxSizeRecommendation,
				proposedSize:          downsizedAllocatableEk32,
				wantDownsize:          true,
				wantErr:               "",
				wantLastOperationTime: downsizeTime,
			},
			{
				name: "starts downsize without max size recommendations - assuming non-resizable",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:     fullAllocatable32,
						PhysicalMaxSize: fullAllocatable32,
						Node:            node,
						MachineFamily:   family,
					},
				},
				maxSizeRecommendation: nil,
				proposedSize:          downsizedAllocatableEk32,
				wantDownsize:          true,
				wantErr:               "",
				wantLastOperationTime: downsizeTime,
			},
			{
				name: "starts downsize for non-resizable node",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:     fullAllocatable32,
						PhysicalMaxSize: fullAllocatable32,
						Node:            node,
						MachineFamily:   family,
					},
				},
				maxSizeRecommendation: maxSizeRecommendationNonResizable,
				proposedSize:          downsizedAllocatableEk32,
				wantDownsize:          true,
				wantErr:               "",
				wantLastOperationTime: downsizeTime,
			},
			{
				name: "returns error for invalid node (not present in resizable node snapshot)",
				node: node,
				snapshot: ResizableNodesSnapshot{
					"different-node": {
						DesiredSize:     fullAllocatable32,
						PhysicalMaxSize: fullAllocatable32,
						Node:            node,
						MachineFamily:   family,
					},
				},
				maxSizeRecommendation: maxSizeRecommendation,
				proposedSize:          downsizedAllocatableEk32,
				wantDownsize:          false,
				wantErr:               "node .*? is not present in resizable node snapshot",
			},
			{
				name: "returns error for upsize request",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:     downsizedAllocatableEk32,
						PhysicalMaxSize: fullAllocatable32,
						Node:            node,
						MachineFamily:   family,
					},
				},
				maxSizeRecommendation: maxSizeRecommendation,
				proposedSize:          fullAllocatable32,
				wantDownsize:          false,
				wantErr:               "not a downsize",
			},
			{
				name: "returns error when node size is already at proposed size",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:     halfAllocatable32,
						PhysicalMaxSize: fullAllocatable32,
						Node:            node,
						MachineFamily:   family,
					},
				},
				maxSizeRecommendation: maxSizeRecommendation,
				proposedSize:          halfAllocatable32,
				wantDownsize:          false,
				wantErr:               "not a downsize",
			},
			{
				name: "starts downsize if request matches target size only by memory",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:     fullAllocatable32,
						PhysicalMaxSize: fullAllocatable32,
						Node:            node,
						MachineFamily:   family,
					},
				},
				maxSizeRecommendation: maxSizeRecommendation,
				proposedSize:          size.Allocatable{MilliCpus: fullAllocatable32.MilliCpus, KBytes: downsizedAllocatableEk32.KBytes},
				wantDownsize:          true,
				wantErr:               "",
				wantLastOperationTime: downsizeTime,
			},
			{
				name: "starts downsize if request matches target size only by cpu",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:     fullAllocatable32,
						PhysicalMaxSize: fullAllocatable32,
						Node:            node,
						MachineFamily:   family,
					},
				},
				maxSizeRecommendation: maxSizeRecommendation,
				proposedSize:          size.Allocatable{MilliCpus: downsizedAllocatableEk32.MilliCpus, KBytes: fullAllocatable32.KBytes},
				wantDownsize:          true,
				wantErr:               "",
				wantLastOperationTime: downsizeTime,
			},
			// // For this no-op to happen the node needs to be at minimum size, and
			// // we need to request downsize to minimum
			{
				name: "skips downsize if node is already at minimum size in both dimensions",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:      minAllocatable,
						PhysicalMaxSize:  fullAllocatable32,
						UpsizableMaxSize: fullAllocatable32,
						Node:             node,
						MachineFamily:    family,
					},
				},
				maxSizeRecommendation: maxSizeRecommendation,
				proposedSize:          minAllocatable,
				wantDownsize:          false,
				wantErr:               "not a downsize",
			},
			{
				name: "starts downsize if node is at minimum size only by cpu",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:      size.Allocatable{MilliCpus: minAllocatable.MilliCpus, KBytes: fullAllocatable32.KBytes},
						PhysicalMaxSize:  fullAllocatable32,
						UpsizableMaxSize: fullAllocatable32,
						Node:             node,
						MachineFamily:    family,
					},
				},
				maxSizeRecommendation: maxSizeRecommendation,
				proposedSize:          minAllocatable,
				wantDownsize:          true,
				wantErr:               "",
				wantLastOperationTime: downsizeTime,
			},
			{
				name: "starts downsize if node is at minimum size only by memory",
				node: node,
				snapshot: ResizableNodesSnapshot{
					testResizableNodeName: {
						DesiredSize:      size.Allocatable{MilliCpus: fullAllocatable32.MilliCpus, KBytes: downsizedAllocatableEk32.KBytes},
						PhysicalMaxSize:  fullAllocatable32,
						UpsizableMaxSize: fullAllocatable32,
						Node:             node,
						MachineFamily:    family,
					},
				},
				maxSizeRecommendation: maxSizeRecommendation,
				proposedSize:          minAllocatable,
				wantDownsize:          true,
				wantErr:               "",
				wantLastOperationTime: downsizeTime,
			},
		}
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%s-%s", family, tt.name), func(t *testing.T) {
				sizeCalculator := &identitySizeCalculator{}
				testClock := clock.NewFakeClock(testStartTime)
				nodeSizeRecommender := &mockNodeSizeRecommender{}
				nodeSizeRecommender.On("MaxSize", tt.node).Return(tt.maxSizeRecommendation)
				mockTracker := &mockOperationTracker{}
				mockTracker.On("Resize", mock.Anything).Return()
				nodeStateManager := NewNodeStateManager(newMockResizingProvider(func(string) bool { return true }), nodeSizeRecommender, mockBackoff, sizeCalculator, testClock)
				nodeStateManager.nodes = tt.snapshot

				m := &ManagerImpl{
					tracker:          mockTracker,
					sizeCalculator:   sizeCalculator,
					clock:            testClock,
					nodeStateManager: nodeStateManager,
				}
				originalSnapshot := m.FilteredNodesSnapshot(true, AllNodes)
				testClock.SetTime(downsizeTime)

				testClock.SetTime(downsizeTime)
				err := m.Downsize(tt.node, tt.proposedSize)

				if len(tt.wantErr) == 0 {
					assert.NoError(t, err)
				} else if assert.Error(t, err) {
					match, matchErr := regexp.MatchString(tt.wantErr, err.Error())
					assert.NoError(t, matchErr)
					assert.True(t, match, "Expected error [%s], but got [%s]", tt.wantErr, err.Error())
				}

				if tt.wantDownsize {
					mockTracker.AssertNumberOfCalls(t, "Resize", 1)
					mockTracker.AssertCalled(
						t,
						"Resize",
						mock.MatchedBy(func(op ResizeOperation) bool {
							desiredSizeCheck := assert.Equal(t, size.VmSize(tt.proposedSize), op.DesiredSize, "DesiredSize check error")
							startingSizeCheck := assert.Equal(t, size.VmSize(originalSnapshot[testResizableNodeName].DesiredSize), op.StartingSize, "StartingSize check error")
							return desiredSizeCheck && startingSizeCheck
						}),
					)
					assert.Equal(t, tt.wantLastOperationTime, m.FilteredNodesSnapshot(true, AllNodes)[testResizableNodeName].LastOperationTime)
				} else {
					// Snapshot was not modified.
					mockTracker.AssertNumberOfCalls(t, "Resize", 0)
					assert.Equal(t, originalSnapshot, m.FilteredNodesSnapshot(true, AllNodes))
				}
			})
		}
	}
}

type mockOperationTracker struct {
	mock.Mock
}

func (m *mockOperationTracker) Run(_ context.Context) {
}

func (m *mockOperationTracker) Resize(resizeOperation ResizeOperation) {
	m.MethodCalled("Resize", resizeOperation)
}

func (m *mockOperationTracker) Fix(machineFamily, nodeName string) {
	m.MethodCalled("Fix", machineFamily, nodeName)
}

func (m *mockOperationTracker) IsNodeInProcess(nodeName string) bool {
	return false
}

func (m *mockOperationTracker) IsNodeResizingOrPending(nodeName string) bool {
	return false
}

type mockNodeSizeRecommender struct {
	mock.Mock
}

func (m *mockNodeSizeRecommender) MaxSize(node *v1.Node) *nodesizerecommender.MaxSizeRecommendation {
	args := m.Called(node)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*nodesizerecommender.MaxSizeRecommendation)
}

func (m *mockNodeSizeRecommender) SetLocations(locations []string) {
	m.Called(locations)
}

type mockManagerMetrics struct {
	mock.Mock
	mockPeriodicMetrics
}

// identitySizeCalculator assumes that size.VmSize and size.Allocatable are identical. This is handy for some testing.
type identitySizeCalculator struct{}

func (c *identitySizeCalculator) ToAllocatable(_ *v1.Node, vmSize size.VmSize) size.Allocatable {
	return size.Allocatable(vmSize)
}

func (c *identitySizeCalculator) ToVmSize(_ *v1.Node, allocatable size.Allocatable) (size.VmSize, error) {
	return size.VmSize(allocatable), nil
}

func (c *identitySizeCalculator) MinAllocatable(_ *v1.Node) (size.Allocatable, error) {
	return size.Allocatable{}, nil
}

func (c *identitySizeCalculator) RoundUp(_ *v1.Node, allocatable size.Allocatable) (size.Allocatable, error) {
	return allocatable, nil
}

func (c *identitySizeCalculator) MakeVmSizeValid(_ *v1.Node, vmSize size.VmSize) (size.VmSize, error) {
	return vmSize, nil
}

func (c *identitySizeCalculator) GetMaxResizableVmSizeByMachineType(_ string) (size.VmSize, error) {
	return size.VmSize{}, fmt.Errorf("GetMaxResizableVmSizeByMachineType not implemented in identitySizeCalculator")
}
