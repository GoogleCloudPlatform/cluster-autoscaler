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

package binpacking

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
)

func TestBinpackingMetricsProcessor(t *testing.T) {
	testCases := []struct {
		name       string
		operations []binpackingOperation
	}{
		{
			name: "no node groups",
			operations: []binpackingOperation{
				{opType: initBinpacking, opGroupCount: 0},
				{opType: finalizeBinpacking, opStats: binpackingStats{total: 0, processed: 0, skipped: 0}},
			},
		},
		{
			name: "single processed node group",
			operations: []binpackingOperation{
				{opType: initBinpacking, opGroupCount: 1},
				{opType: markProcessed},
				{opType: stopBinpacking, opResult: true},
				{opType: finalizeBinpacking, opStats: binpackingStats{total: 1, processed: 1, skipped: 0}},
			},
		},
		{
			name: "single skipped node group",
			operations: []binpackingOperation{
				{opType: initBinpacking, opGroupCount: 1},
				{opType: markProcessed},
				{opType: finalizeBinpacking, opStats: binpackingStats{total: 1, processed: 0, skipped: 1}},
			},
		},
		{
			name: "multiple node groups, binpacking not stopped",
			operations: []binpackingOperation{
				{opType: initBinpacking, opGroupCount: 5},
				{opType: markProcessed},
				{opType: markProcessed},
				{opType: markProcessed},
				{opType: markProcessed},
				{opType: stopBinpacking},
				{opType: markProcessed},
				{opType: stopBinpacking},
				{opType: finalizeBinpacking, opStats: binpackingStats{total: 5, processed: 2, skipped: 3}},
			},
		},
		{
			name: "multiple node groups, binpacking stopped",
			operations: []binpackingOperation{
				{opType: initBinpacking, opGroupCount: 5},
				{opType: markProcessed},
				{opType: markProcessed},
				{opType: markProcessed},
				{opType: markProcessed},
				{opType: stopBinpacking, opResult: true},
				{opType: finalizeBinpacking, opStats: binpackingStats{total: 5, processed: 1, skipped: 4}},
			},
		},
		{
			name: "multiple binpacking loops",
			operations: []binpackingOperation{
				{opType: initBinpacking, opGroupCount: 5},
				{opType: markProcessed},
				{opType: stopBinpacking, opResult: true},
				{opType: finalizeBinpacking, opStats: binpackingStats{total: 5, processed: 1, skipped: 4}},
				{opType: initBinpacking, opGroupCount: 3},
				{opType: markProcessed},
				{opType: markProcessed},
				{opType: stopBinpacking},
				{opType: markProcessed},
				{opType: stopBinpacking},
				{opType: finalizeBinpacking, opStats: binpackingStats{total: 3, processed: 2, skipped: 1}},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockLimiter := &mockBinpackingLimiter{}
			mockMetrics := &mockBinpackingMetrics{}
			processor := NewBinpackingMetricsProcessor(mockLimiter)
			processor.metrics = mockMetrics

			for i, op := range tc.operations {
				ctx := &context.AutoscalingContext{}
				switch op.opType {
				case initBinpacking:
					var nodeGroups []cloudprovider.NodeGroup
					for j := 0; j < op.opGroupCount; j++ {
						nodeGroups = append(nodeGroups, test.NewTestNodeGroup(fmt.Sprintf("ng-%d-%d", i, j), 0, 0, 0, false, false, "", nil, nil))
					}
					mockLimiter.On("InitBinpacking", ctx, nodeGroups).Once()
					processor.InitBinpacking(ctx, nodeGroups)
				case markProcessed:
					nodeGroupName := fmt.Sprintf("ng-%d", i)
					mockLimiter.On("MarkProcessed", ctx, nodeGroupName).Once()
					processor.MarkProcessed(ctx, nodeGroupName)
				case stopBinpacking:
					options := []expander.Option{{Debug: fmt.Sprintf("options-%d", i)}}
					mockLimiter.On("StopBinpacking", ctx, options).Return(op.opResult).Once()
					assert.Equal(t, op.opResult, processor.StopBinpacking(ctx, options))
				case finalizeBinpacking:
					mockMetrics.On("ObserveBinpackingNodeGroupTotal", op.opStats.total).Once()
					mockMetrics.On("ObserveBinpackingNodeGroupProcessed", op.opStats.processed).Once()
					mockMetrics.On("ObserveBinpackingNodeGroupSkipped", op.opStats.skipped).Once()

					options := []expander.Option{{Debug: fmt.Sprintf("options-%d", i)}}
					mockLimiter.On("FinalizeBinpacking", ctx, options).Once()
					processor.FinalizeBinpacking(ctx, options)

					mockMetrics.AssertExpectations(t)
				}

				mockLimiter.AssertExpectations(t)
			}
		})
	}
}

type binpackingStats struct {
	total, processed, skipped int
}

type binpackingOperationType int

const (
	initBinpacking binpackingOperationType = iota
	markProcessed
	stopBinpacking
	finalizeBinpacking
)

type binpackingOperation struct {
	opType       binpackingOperationType
	opStats      binpackingStats
	opGroupCount int
	opResult     bool
}

type mockBinpackingLimiter struct {
	mock.Mock
}

func (m *mockBinpackingLimiter) InitBinpacking(context *context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup) {
	m.Called(context, nodeGroups)
}

func (m *mockBinpackingLimiter) MarkProcessed(context *context.AutoscalingContext, nodegroupId string) {
	m.Called(context, nodegroupId)
}

func (m *mockBinpackingLimiter) StopBinpacking(context *context.AutoscalingContext, evaluatedOptions []expander.Option) bool {
	return m.Called(context, evaluatedOptions).Bool(0)
}

func (m *mockBinpackingLimiter) FinalizeBinpacking(context *context.AutoscalingContext, finalOptions []expander.Option) {
	m.Called(context, finalOptions)
}

type mockBinpackingMetrics struct {
	mock.Mock
}

func (m *mockBinpackingMetrics) ObserveBinpackingNodeGroupTotal(count int) {
	m.Called(count)
}

func (m *mockBinpackingMetrics) ObserveBinpackingNodeGroupProcessed(count int) {
	m.Called(count)
}

func (m *mockBinpackingMetrics) ObserveBinpackingNodeGroupSkipped(count int) {
	m.Called(count)
}
