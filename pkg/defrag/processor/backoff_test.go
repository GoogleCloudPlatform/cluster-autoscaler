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
	autoscaling_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
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
				backoff.backoff(&autoscaling_context.AutoscalingContext{}, candidate)
			}
			for _, plugin := range allPlugins {
				availableNodes, backedOffNodes := backoff.splitNodesBasedOnBackoff(plugin, tc.allNodes)
				assert.ElementsMatch(t, tc.wantNodes[plugin], availableNodes)
				assert.ElementsMatch(t, tc.wantBackoffedNodes[plugin], backedOffNodes)
			}
		})
	}
}
