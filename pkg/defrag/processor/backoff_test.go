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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/observability"
	autoscaling_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
)

func TestBackoff(t *testing.T) {
	pluginA := &mockPlugin{}
	pluginA.On("BackoffDuration", mock.Anything, mock.Anything).Return(time.Minute)

	pluginB := &mockPlugin{}
	pluginB.On("BackoffDuration", mock.Anything, mock.Anything).Return(time.Minute)

	pluginC := &mockPlugin{}
	pluginC.On("BackoffDuration", mock.Anything, mock.Anything).Return(0 * time.Minute)

	allPlugins := []defrag.Plugin{pluginA, pluginB, pluginC}

	testCases := []struct {
		name               string
		candidates         []*defrag.Candidate
		allNodes           []string
		wantNodes          map[defrag.Plugin][]string
		wantBackoffedNodes map[defrag.Plugin][]string
	}{
		{
			name: "no nodes, empty candidates",
			candidates: []*defrag.Candidate{
				{Plugin: pluginA},
				{Plugin: pluginB},
				{Plugin: pluginC},
			},
			wantNodes: map[defrag.Plugin][]string{
				pluginA: {},
				pluginB: {},
				pluginC: {},
			},
			wantBackoffedNodes: map[defrag.Plugin][]string{
				pluginA: {},
				pluginB: {},
				pluginC: {},
			},
		},
		{
			name: "some nodes, empty candidates",
			candidates: []*defrag.Candidate{
				{Plugin: pluginA},
				{Plugin: pluginB},
				{Plugin: pluginC},
			},
			allNodes: []string{"n1", "n2", "n3"},
			wantNodes: map[defrag.Plugin][]string{
				pluginA: {"n1", "n2", "n3"},
				pluginB: {"n1", "n2", "n3"},
				pluginC: {"n1", "n2", "n3"},
			},
			wantBackoffedNodes: map[defrag.Plugin][]string{
				pluginA: {},
				pluginB: {},
				pluginC: {},
			},
		},
		{
			name: "some nodes, candidates with some nodes",
			candidates: []*defrag.Candidate{
				{Plugin: pluginA, Nodes: []string{"n1", "n2"}},
				{Plugin: pluginB, Nodes: []string{"n2", "n3"}},
				{Plugin: pluginC, Nodes: []string{"n1", "n3"}},
			},
			allNodes: []string{"n1", "n2", "n3"},
			wantNodes: map[defrag.Plugin][]string{
				pluginA: {"n3"},
				pluginB: {"n1"},
				pluginC: {"n1", "n2", "n3"},
			},
			wantBackoffedNodes: map[defrag.Plugin][]string{
				pluginA: {"n1", "n2"},
				pluginB: {"n2", "n3"},
				pluginC: {},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			backoff := newDefragBackoff()

			for _, candidate := range tc.candidates {
				backoff.backoff(&autoscaling_context.AutoscalingContext{}, candidate, observability.RecentMigrationFailure)
			}
			for _, plugin := range allPlugins {
				availableNodes, backedOffNodes := backoff.splitNodesBasedOnBackoff(plugin, tc.allNodes)
				assert.ElementsMatch(t, tc.wantNodes[plugin], availableNodes)
				assert.ElementsMatch(t, tc.wantBackoffedNodes[plugin], backedOffNodes)
			}
		})
	}
}

// TestBackoffCause covers the reason surviving alongside the backoff, which is
// what stops a candidate abandoned for lack of capacity from being reported as
// a generic recent failure on every pass after the first.
func TestBackoffCause(t *testing.T) {
	plugin := &mockPlugin{}
	plugin.On("BackoffDuration", mock.Anything, mock.Anything).Return(time.Hour)
	otherPlugin := &mockPlugin{}
	otherPlugin.On("BackoffDuration", mock.Anything, mock.Anything).Return(time.Hour)

	backoff := newDefragBackoff()
	backoff.backoff(&autoscaling_context.AutoscalingContext{}, &defrag.Candidate{
		Plugin: plugin,
		Nodes:  []string{"n1"},
	}, observability.ReplacementUnavailable)

	t.Run("recorded cause is returned", func(t *testing.T) {
		assert.Equal(t, observability.ReplacementUnavailable, backoff.cause(plugin, "n1"))
	})
	t.Run("unknown node falls back", func(t *testing.T) {
		// Never an empty reason: an unattributed node would silently drop out
		// of the reported totals.
		assert.Equal(t, observability.RecentMigrationFailure, backoff.cause(plugin, "unknown"))
	})
	t.Run("cause is per plugin", func(t *testing.T) {
		assert.Equal(t, observability.RecentMigrationFailure, backoff.cause(otherPlugin, "n1"))
	})
	t.Run("cause survives cleanup while the backoff is live", func(t *testing.T) {
		backoff.cleanBackoffInfo()
		assert.Equal(t, observability.ReplacementUnavailable, backoff.cause(plugin, "n1"))
	})
}
