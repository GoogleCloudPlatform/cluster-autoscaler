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

package recycling

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	csisnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/csi/snapshot"
	drasnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/dynamicresources/snapshot"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/config"
	clockutils "k8s.io/utils/clock/testing"
)

var (
	timeNow = time.Now()
)

func TestNewCandidate(t *testing.T) {
	for desc, tc := range map[string]struct {
		nodes                     []*apiv1.Node
		maxCandidateNodesCount    int
		wantNodes                 []string
		wantLatestUnfitNodesCount int
	}{
		"no valid nodes": {
			nodes: []*apiv1.Node{
				test.BuildTestNode("gke-no-recycling", 1000, 10),
				test.BuildTestNode("gke-not-relevant", 1000, 10),
			},
		},
		"only one of the labels exists": {
			nodes: []*apiv1.Node{
				withLabel(test.BuildTestNode("gke-no-term-time", 1000, 10), gkelabels.NodeRecycleLeadTimeSecondsLabelKey, "1000.0"),
				withAnnotation(test.BuildTestNode("gke-no-recycling", 1000, 10), gkelabels.InstanceTerminationAnnotationKey, timeNow.Format(time.RFC3339)),
			},
		},
		"invalid label values": {
			nodes: []*apiv1.Node{
				withAnnotation(buildNodeWithTermTimeAndRecycleLeadTimeSec("gke-invalid1", timeNow, 100),
					gkelabels.InstanceTerminationAnnotationKey, "Not a valid timestamp"),
				withLabel(buildNodeWithTermTimeAndRecycleLeadTimeSec("gke-invalid2", timeNow.Add(100*time.Second), 10),
					gkelabels.NodeRecycleLeadTimeSecondsLabelKey, "Not a valid lead time"),
			},
		},
		"one valid node": {
			nodes: []*apiv1.Node{
				test.BuildTestNode("gke-no-recycling", 1000, 10),
				buildNodeWithTermTimeAndRecycleLeadTimeSec("gke-valid-node", timeNow.Add(50*time.Second), 100),
			},
			wantNodes:                 []string{"gke-valid-node"},
			wantLatestUnfitNodesCount: 1,
		},
		"only take nodes with ttl under lead time": {
			nodes: []*apiv1.Node{
				buildNodeWithTermTimeAndRecycleLeadTimeSec("gke-ttl-under-lead-time", timeNow.Add(50*time.Second), 100),
				buildNodeWithTermTimeAndRecycleLeadTimeSec("gke-ttl-over-lead-time1", timeNow.Add(200*time.Second), 100),
				buildNodeWithTermTimeAndRecycleLeadTimeSec("gke-ttl-over-lead-time2", timeNow.Add(50*time.Second), 25),
			},
			wantNodes:                 []string{"gke-ttl-under-lead-time"},
			wantLatestUnfitNodesCount: 1,
		},
		"Also recycle if we're past the termination time (somehow)": {
			nodes: []*apiv1.Node{
				buildNodeWithTermTimeAndRecycleLeadTimeSec("gke-past-term", timeNow.Add(-50*time.Second), 100),
			},
			wantNodes:                 []string{"gke-past-term"},
			wantLatestUnfitNodesCount: 1,
		},
		"multiple valid nodes - ordered by TTL": {
			nodes: []*apiv1.Node{
				buildNodeWithTermTimeAndRecycleLeadTimeSec("gke-valid1", timeNow.Add(51*time.Second), 100),
				test.BuildTestNode("gke-no-recycling", 1000, 10),
				buildNodeWithTermTimeAndRecycleLeadTimeSec("gke-valid2", timeNow.Add(200*time.Second), 300),
				buildNodeWithTermTimeAndRecycleLeadTimeSec("gke-valid3", timeNow.Add(50*time.Second), 55),
			},
			wantNodes:                 []string{"gke-valid3", "gke-valid1", "gke-valid2"},
			wantLatestUnfitNodesCount: 3,
		},
		"multiple valid nodes, max nodes count exceeded": {
			nodes: []*apiv1.Node{
				buildNodeWithTermTimeAndRecycleLeadTimeSec("gke-valid1", timeNow.Add(51*time.Second), 100),
				test.BuildTestNode("gke-no-recycling", 1000, 10),
				buildNodeWithTermTimeAndRecycleLeadTimeSec("gke-valid2", timeNow.Add(200*time.Second), 300),
				buildNodeWithTermTimeAndRecycleLeadTimeSec("gke-valid3", timeNow.Add(50*time.Second), 55),
			},
			maxCandidateNodesCount:    2,
			wantNodes:                 []string{"gke-valid3", "gke-valid1"},
			wantLatestUnfitNodesCount: 3,
		},
	} {
		t.Run(desc, func(t *testing.T) {
			ctx := &context.AutoscalingContext{
				ClusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t),
			}
			assert.NoError(t, ctx.ClusterSnapshot.SetClusterState(tc.nodes, nil, drasnapshot.NewEmptySnapshot(), csisnapshot.NewEmptySnapshot()))

			var nodeNames []string
			for _, node := range tc.nodes {
				nodeNames = append(nodeNames, node.Name)
			}

			plugin := &plugin{
				config: config.PluginsConfig{
					MaxCandidateNodeCount: tc.maxCandidateNodesCount,
				},
				clock: clockutils.NewFakePassiveClock(timeNow),
			}
			candidate := plugin.NewCandidate(ctx, nodeNames)

			if len(tc.wantNodes) > 0 {
				assert.NotNil(t, candidate)
				assert.Equal(t, tc.wantNodes, candidate.Nodes)
				assert.Equal(t, defrag.Partial, candidate.Mode)
			} else {
				assert.Nil(t, candidate)
			}

			latestUnfitNodesCount := plugin.LatestUnfitNodesCount()
			if latestUnfitNodesCount != tc.wantLatestUnfitNodesCount {
				t.Errorf("plugin.LatestUnfitNodesCount() got latest unfit node count: %d, want latest unfit node count: %d", latestUnfitNodesCount, tc.wantLatestUnfitNodesCount)
			}
		})
	}
}

func TestValidCandidateNodes(t *testing.T) {
	for desc, tc := range map[string]struct {
		nodes                   []*apiv1.Node
		candidate               *defrag.Candidate
		wantValidCandidateNodes []string
	}{
		"valid single-node candidate": {
			nodes: []*apiv1.Node{
				buildNodeWithTermTimeAndRecycleLeadTimeSec("gke-valid", timeNow.Add(50*time.Second), 100),
			},
			candidate:               &defrag.Candidate{Nodes: []string{"gke-valid"}},
			wantValidCandidateNodes: []string{"gke-valid"},
		},
		"invalid single-node candidate": {
			nodes: []*apiv1.Node{
				test.BuildTestNode("gke-invalid", 1000, 10),
			},
			candidate:               &defrag.Candidate{Nodes: []string{"gke-invalid"}},
			wantValidCandidateNodes: nil,
		},
		"invalid timestamp": {
			nodes: []*apiv1.Node{
				withAnnotation(buildNodeWithTermTimeAndRecycleLeadTimeSec("gke-invalid", timeNow, 100),
					gkelabels.InstanceTerminationAnnotationKey, "Not a valid timestamp"),
			},
			candidate:               &defrag.Candidate{Nodes: []string{"gke-invalid"}},
			wantValidCandidateNodes: nil,
		},
		"invalid lead time": {
			nodes: []*apiv1.Node{
				withLabel(buildNodeWithTermTimeAndRecycleLeadTimeSec("gke-invalid", timeNow.Add(100*time.Second), 10),
					gkelabels.NodeRecycleLeadTimeSecondsLabelKey, "Not a valid lead time"),
			},
			candidate:               &defrag.Candidate{Nodes: []string{"gke-invalid"}},
			wantValidCandidateNodes: nil,
		},
		"ttl over lead time": {
			nodes: []*apiv1.Node{
				buildNodeWithTermTimeAndRecycleLeadTimeSec("gke-invalid", timeNow.Add(100*time.Second), 50),
			},
			candidate:               &defrag.Candidate{Nodes: []string{"gke-invalid"}},
			wantValidCandidateNodes: nil,
		},
		"valid multi-node candidate": {
			nodes: []*apiv1.Node{
				buildNodeWithTermTimeAndRecycleLeadTimeSec("gke-valid1", timeNow.Add(50*time.Second), 100),
				buildNodeWithTermTimeAndRecycleLeadTimeSec("gke-valid2", timeNow.Add(150*time.Second), 200),
			},
			candidate:               &defrag.Candidate{Nodes: []string{"gke-valid1", "gke-valid2"}},
			wantValidCandidateNodes: []string{"gke-valid1", "gke-valid2"},
		},
		"one node valid, one node invalid and filtered out": {
			nodes: []*apiv1.Node{
				buildNodeWithTermTimeAndRecycleLeadTimeSec("gke-valid", timeNow.Add(100*time.Second), 200),
				buildNodeWithTermTimeAndRecycleLeadTimeSec("gke-invalid1", timeNow.Add(100*time.Second), 50),
				test.BuildTestNode("gke-invalid2", 1000, 10),
			},
			candidate:               &defrag.Candidate{Nodes: []string{"gke-valid", "gke-invalid"}},
			wantValidCandidateNodes: []string{"gke-valid"},
		},
	} {
		t.Run(desc, func(t *testing.T) {
			ctx := &context.AutoscalingContext{
				ClusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t),
			}
			assert.NoError(t, ctx.ClusterSnapshot.SetClusterState(tc.nodes, nil, drasnapshot.NewEmptySnapshot(), csisnapshot.NewEmptySnapshot()))

			plugin := newPluginWithFakeClock(timeNow)
			assert.Equal(t, tc.wantValidCandidateNodes, plugin.ValidCandidateNodes(ctx, tc.candidate.Nodes))
		})
	}
}

func withLabel(node *apiv1.Node, key, value string) *apiv1.Node {
	if node.Labels == nil {
		node.Labels = make(map[string]string)
	}
	node.Labels[key] = value
	return node
}

func withAnnotation(node *apiv1.Node, key, value string) *apiv1.Node {
	if node.Annotations == nil {
		node.Annotations = make(map[string]string)
	}
	node.Annotations[key] = value
	return node
}

func buildNodeWithTermTimeAndRecycleLeadTimeSec(name string, terminationTime time.Time, recycleLeadTimeSec int) *apiv1.Node {
	node := test.BuildTestNode(name, 1000, 10)
	node = withAnnotation(node, gkelabels.InstanceTerminationAnnotationKey, terminationTime.Format(time.RFC3339))
	node = withLabel(node, gkelabels.NodeRecycleLeadTimeSecondsLabelKey, fmt.Sprintf("%d", recycleLeadTimeSec))
	return node
}

func newPluginWithFakeClock(timeNow time.Time) defrag.Plugin {
	return &plugin{
		config: config.PluginsConfig{},
		clock:  clockutils.NewFakePassiveClock(timeNow),
	}
}
