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

package failednodes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/config"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/operationtracker"
)

func TestNewCandidate(t *testing.T) {
	testCases := []struct {
		name                      string
		isResizingEnabled         bool
		maxCandidateNodeCount     int
		failedNodes               []string
		nodeNames                 []string
		wantCandidateNodeNames    []string
		wantLatestUnfitNodesCount int
	}{
		{
			name:                      "a failed node - a candidate",
			isResizingEnabled:         true,
			maxCandidateNodeCount:     1,
			failedNodes:               []string{"ek-node"},
			nodeNames:                 []string{"ek-node"},
			wantCandidateNodeNames:    []string{"ek-node"},
			wantLatestUnfitNodesCount: 1,
		},
		{
			name:                      "resizing is disabled - no candidates",
			isResizingEnabled:         false,
			maxCandidateNodeCount:     1,
			failedNodes:               []string{"ek-node"},
			nodeNames:                 []string{"ek-node"},
			wantCandidateNodeNames:    []string{"ek-node"},
			wantLatestUnfitNodesCount: 1,
		},
		{
			name:                  "no failed nodes - no candidates",
			isResizingEnabled:     true,
			maxCandidateNodeCount: 1,
			failedNodes:           []string{},
			nodeNames:             []string{"ek-node"},
		},
		{
			name:                      "multiple failed nodes - maxCandidateNodeCount - candidate with single node",
			isResizingEnabled:         true,
			maxCandidateNodeCount:     1,
			failedNodes:               []string{"ek-node-1", "ek-node-2", "ek-node-4"},
			nodeNames:                 []string{"ek-node-1", "ek-node-3", "ek-node-4"},
			wantCandidateNodeNames:    []string{"ek-node-1"},
			wantLatestUnfitNodesCount: 2,
		},
		{
			name:                      "multiple failed nodes - only nodes passed by defrag are candidates",
			isResizingEnabled:         true,
			maxCandidateNodeCount:     2,
			failedNodes:               []string{"ek-node-1", "ek-node-2", "ek-node-4"},
			nodeNames:                 []string{"ek-node-1", "ek-node-3", "ek-node-4"},
			wantCandidateNodeNames:    []string{"ek-node-1", "ek-node-4"},
			wantLatestUnfitNodesCount: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &context.AutoscalingContext{}

			resizableVmManager := &mockResizableVmManager{}
			resizableVmManager.On("IsResizingEnabled", machinetypes.EK.Name()).Return(
				tc.isResizingEnabled)
			resizableVmManager.On("UnhealthyNodesWithStatus", mock.Anything).Return(tc.failedNodes)

			plugin := NewPlugin(config.PluginsConfig{
				ResizableVmManager:    resizableVmManager,
				MaxCandidateNodeCount: tc.maxCandidateNodeCount,
			})
			candidate := plugin.NewCandidate(ctx, tc.nodeNames)

			if len(tc.wantCandidateNodeNames) == 0 {
				assert.Nil(t, candidate)
			} else {
				assert.NotNil(t, candidate)
				assert.Equal(t, tc.wantCandidateNodeNames, candidate.Nodes)
				assert.Equal(t, defrag.Partial, candidate.Mode)
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
		name                  string
		isResizingEnabled     bool
		maxCandidateNodeCount int
		failedNodes           []string
		nodeNames             []string
		want                  []string
	}{
		{
			name:                  "candidate is still valid",
			isResizingEnabled:     true,
			maxCandidateNodeCount: 1,
			failedNodes:           []string{"ek-node"},
			nodeNames:             []string{"ek-node"},
			want:                  []string{"ek-node"},
		},
		{
			name:                  "resizing is disabled",
			isResizingEnabled:     false,
			maxCandidateNodeCount: 1,
			failedNodes:           []string{"ek-node"},
			nodeNames:             []string{"ek-node"},
			want:                  []string{"ek-node"},
		},
		{
			name:                  "multi-node candidate - all nodes valid",
			isResizingEnabled:     true,
			maxCandidateNodeCount: 1,
			failedNodes:           []string{"ek-node-1", "ek-node-2", "ek-node-3"},
			nodeNames:             []string{"ek-node-1", "ek-node-2"},
			want:                  []string{"ek-node-1", "ek-node-2"},
		},
		{
			name:                  "multi-node candidate - some nodes are no longer failed",
			isResizingEnabled:     true,
			maxCandidateNodeCount: 1,
			failedNodes:           []string{"ek-node-1", "ek-node-3"},
			nodeNames:             []string{"ek-node-1", "ek-node-2"},
			want:                  []string{"ek-node-1"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &context.AutoscalingContext{}

			resizableVmManager := &mockResizableVmManager{}
			resizableVmManager.On("IsResizingEnabled", machinetypes.EK.Name()).Return(
				tc.isResizingEnabled)
			resizableVmManager.On("UnhealthyNodesWithStatus", mock.Anything).Return(tc.failedNodes)

			plugin := NewPlugin(config.PluginsConfig{
				ResizableVmManager:    resizableVmManager,
				MaxCandidateNodeCount: tc.maxCandidateNodeCount,
			})

			assert.Equal(t, tc.want, plugin.ValidCandidateNodes(ctx, tc.nodeNames))
		})
	}
}

type mockResizableVmManager struct {
	mock.Mock
	operationtracker.Manager
}

func (m *mockResizableVmManager) IsResizingEnabled(machineFamily string) bool {
	args := m.Called(machineFamily)
	return args.Get(0).(bool)
}

func (m *mockResizableVmManager) UnhealthyNodesWithStatus(status operationtracker.UnhealthyResizableNodeStatus) []string {
	args := m.Called(status)
	return args.Get(0).([]string)
}
