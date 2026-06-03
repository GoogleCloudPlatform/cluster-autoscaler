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
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	cacontext "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/pdb"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/status"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability/rules"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/options"
	caerrors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/fairness"
	. "k8s.io/utils/clock/testing"
)

func TestCleanUpCandidates(t *testing.T) {
	backoffDuration := 12 * time.Minute
	onlyNXNodesProcessor := &mockScaleDownNodeProcessor{
		candidatesFilter: func(nodes []*apiv1.Node) []*apiv1.Node {
			var result []*apiv1.Node
			for _, n := range nodes {
				if strings.HasPrefix(n.Name, "n") {
					result = append(result, n)
				}
			}
			return result
		},
	}

	alwaysValidPlugin := &fakePlugin{
		backoffDuration: backoffDuration,
	}

	neverValidPlugin := &fakePlugin{
		validFilter: func(nodeName string) bool {
			return false
		},
		backoffDuration: backoffDuration,
	}

	allPlugins := []defrag.Plugin{alwaysValidPlugin, neverValidPlugin}

	timeNow := time.Now()
	overScaleUpTimeout := time.Now().Add(-10 * time.Minute)
	overScaleDownTimeout := time.Now().Add(-5 * time.Minute)

	testCases := []struct {
		name                     string
		pdbs                     []*v1.PodDisruptionBudget
		nodesWithPods            map[*apiv1.Node][]*apiv1.Pod
		candidateInfos           []*candidateInfo
		scaleDownTimes           []*time.Time
		minNodeGroupSize         int
		wantPdbs                 []*v1.PodDisruptionBudget
		wantCandidateInfos       []*candidateInfo
		wantBackoffFilteredNodes map[defrag.Plugin][]string
		wantBackoffedNodes       map[defrag.Plugin][]string
	}{
		{
			name: "no candidates",
			wantBackoffFilteredNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {},
				neverValidPlugin:  {},
			},
			wantBackoffedNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {},
				neverValidPlugin:  {},
			},
		},
		{
			name: "candidate without nodes is removed",
			candidateInfos: []*candidateInfo{
				{
					candidate:    &defrag.Candidate{Nodes: []string{}, Plugin: alwaysValidPlugin},
					creationTime: timeNow,
				},
			},
			scaleDownTimes: []*time.Time{nil},
			wantBackoffFilteredNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {},
				neverValidPlugin:  {},
			},
			wantBackoffedNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {},
				neverValidPlugin:  {},
			},
		},
		{
			name: "invalid and non-existing nodes removed from candidate",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 1): {},
				buildReadyNode("n2", 1000, 1): {
					test.SetRSPodSpec(test.BuildScheduledTestPod("p1", 100, 1, "n2"), "rs"),
				},
				test.BuildTestNode("n3", 1000, 1):      {},
				buildUpcomingNode("n4", 1000, 1):       {},
				buildDuringDeletionNode("n5", 1000, 1): {},
				buildReadyNode("n6", 1000, 1): {
					test.BuildScheduledTestPod("p2", 100, 1, "n6"),
				},
				buildReadyNode("m1", 1000, 1): {},
			},
			candidateInfos: []*candidateInfo{
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n0", "n1", "n2", "n3", "n4", "n5", "n6", "m1"}, Plugin: alwaysValidPlugin},
					creationTime: timeNow,
				},
			},
			scaleDownTimes: []*time.Time{nil},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n1", "n2"}, Plugin: alwaysValidPlugin},
					creationTime: timeNow,
				},
			},
			wantBackoffFilteredNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {"n1", "n2", "n3", "n4", "n5", "n6", "m1"},
				neverValidPlugin:  {"n1", "n2", "n3", "n4", "n5", "n6", "m1"},
			},
			wantBackoffedNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {},
				neverValidPlugin:  {},
			},
		},
		{
			name: "remove nodes from candidate to avoid going under the min node group size",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 1): {},
				buildReadyNode("n2", 1000, 1): {
					test.SetRSPodSpec(test.BuildScheduledTestPod("p1", 100, 1, "n2"), "rs"),
				},
				buildReadyNode("n3", 1000, 1): {
					test.SetRSPodSpec(test.BuildScheduledTestPod("p1", 100, 1, "n2"), "rs"),
				},
			},
			candidateInfos: []*candidateInfo{
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n1", "n2", "n3"}, Plugin: alwaysValidPlugin},
					creationTime: timeNow,
				},
			},
			scaleDownTimes:   []*time.Time{nil},
			minNodeGroupSize: 2,
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n1"}, Plugin: alwaysValidPlugin},
					creationTime: timeNow,
				},
			},
			wantBackoffFilteredNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {"n1", "n2", "n3"},
				neverValidPlugin:  {"n1", "n2", "n3"},
			},
			wantBackoffedNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {},
				neverValidPlugin:  {},
			},
		},
		{
			name: "candidate removed if all nodes are invalid/non-existing",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n3", 1000, 1):      {},
				buildUpcomingNode("n4", 1000, 1):       {},
				buildDuringDeletionNode("n5", 1000, 1): {},
				buildReadyNode("n6", 1000, 1): {
					test.BuildScheduledTestPod("p2", 100, 1, "n6"),
				},
				buildReadyNode("m1", 1000, 1): {},
			},
			candidateInfos: []*candidateInfo{
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n0", "n3", "n4", "n5", "n6", "m1"}, Plugin: alwaysValidPlugin},
					creationTime: timeNow,
				},
			},
			scaleDownTimes: []*time.Time{nil},
			wantBackoffFilteredNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {"n3", "n4", "n5", "n6", "m1"},
				neverValidPlugin:  {"n3", "n4", "n5", "n6", "m1"},
			},
			wantBackoffedNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {},
				neverValidPlugin:  {},
			},
		},
		{
			name: "keep candidate undergoing scale-down within scale-down timeout",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 1): {},
			},
			candidateInfos: []*candidateInfo{
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n1"}, Plugin: alwaysValidPlugin},
					creationTime: timeNow,
				},
			},
			scaleDownTimes: []*time.Time{&timeNow},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n1"}, Plugin: alwaysValidPlugin},
					creationTime: timeNow,
				},
			},
			wantBackoffFilteredNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {"n1"},
				neverValidPlugin:  {"n1"},
			},
			wantBackoffedNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {},
				neverValidPlugin:  {},
			},
		},
		{
			name: "remove candidate undergoing scale-down over scale-down timeout",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 1): {},
			},
			candidateInfos: []*candidateInfo{
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n1"}, Plugin: alwaysValidPlugin},
					creationTime: timeNow,
				},
			},
			scaleDownTimes: []*time.Time{&overScaleDownTimeout},
			wantBackoffFilteredNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {},
				neverValidPlugin:  {"n1"},
			},
			wantBackoffedNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {"n1"},
				neverValidPlugin:  {},
			},
		},
		{
			name: "remove candidate over scale-up timeout",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 1): {},
			},
			candidateInfos: []*candidateInfo{
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n1"}, Plugin: alwaysValidPlugin},
					creationTime: overScaleUpTimeout,
				},
			},
			scaleDownTimes: []*time.Time{nil},
			wantBackoffFilteredNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {},
				neverValidPlugin:  {"n1"},
			},
			wantBackoffedNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {"n1"},
				neverValidPlugin:  {},
			},
		},
		{
			name: "keep candidate over scale-up timeout if it is scaled-down within limit",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 1): {},
			},
			candidateInfos: []*candidateInfo{
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n1"}, Plugin: alwaysValidPlugin},
					creationTime: overScaleUpTimeout,
				},
			},
			scaleDownTimes: []*time.Time{&timeNow},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n1"}, Plugin: alwaysValidPlugin},
					creationTime: overScaleUpTimeout,
				},
			},
			wantBackoffFilteredNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {"n1"},
				neverValidPlugin:  {"n1"},
			},
			wantBackoffedNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {},
				neverValidPlugin:  {},
			},
		},
		{
			name: "remove candidate over scale-up timeout if it is scaled-down over limit",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 1): {},
			},
			candidateInfos: []*candidateInfo{
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n1"}, Plugin: alwaysValidPlugin},
					creationTime: overScaleUpTimeout,
				},
			},
			scaleDownTimes: []*time.Time{&overScaleDownTimeout},
			wantBackoffFilteredNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {},
				neverValidPlugin:  {"n1"},
			},
			wantBackoffedNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {"n1"},
				neverValidPlugin:  {},
			},
		},
		{
			name: "remove candidate without scale up options",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 1): {},
			},
			candidateInfos: []*candidateInfo{
				{
					candidate:        &defrag.Candidate{Nodes: []string{"n1"}, Plugin: alwaysValidPlugin},
					scaleUpNoOptions: true,
				},
			},
			scaleDownTimes: []*time.Time{nil},
			wantBackoffFilteredNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {},
				neverValidPlugin:  {"n1"},
			},
			wantBackoffedNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {"n1"},
				neverValidPlugin:  {},
			},
		},
		{
			name: "candidate with invalid nodes according to plugin",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 1): {},
			},
			candidateInfos: []*candidateInfo{
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n1"}, Plugin: neverValidPlugin},
					creationTime: timeNow,
				},
			},
			scaleDownTimes: []*time.Time{nil},
			wantBackoffFilteredNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {"n1"},
				neverValidPlugin:  {"n1"},
			},
			wantBackoffedNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {},
				neverValidPlugin:  {},
			},
		},
		{
			name: "track remaining PDB across cleaned candidates",
			pdbs: []*v1.PodDisruptionBudget{
				buildPdb("big-pdb", 2),
				buildPdb("small-pdb", 0),
			},
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 1): {
					setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p1", 100, 1, "n1"), "rs"), "big-pdb"),
				},
				buildReadyNode("n2", 1000, 1): {
					setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p2", 100, 1, "n2"), "rs"), "small-pdb"),
				},
				buildReadyNode("n3", 1000, 1): {
					setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p3", 100, 1, "n3"), "rs"), "big-pdb"),
					setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p4", 100, 1, "n3"), "rs"), "small-pdb"),
				},
				buildReadyNode("n4", 1000, 1): {
					setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p5", 100, 1, "n4"), "rs"), "big-pdb"),
					setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p6", 100, 1, "n4"), "rs"), "big-pdb"),
				},
				buildReadyNode("n5", 1000, 1): {
					setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p7", 100, 1, "n5"), "rs"), "big-pdb"),
				},
				buildReadyNode("n6", 1000, 1): {
					setPodLabel(test.SetRSPodSpec(test.BuildScheduledTestPod("p8", 100, 1, "n6"), "rs"), "big-pdb"),
				},
			},
			candidateInfos: []*candidateInfo{
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n1"}, Plugin: alwaysValidPlugin}, // Disruptions for big-pdb within limit
					creationTime: timeNow,
				},
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n2"}, Plugin: alwaysValidPlugin}, // Disruptions for small-pdb exceeded
					creationTime: timeNow,
				},
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n3", "n4", "n5"}, Plugin: alwaysValidPlugin}, // n3 exceeds disruptions for small-pdb, n4 and n5 with >0 remaining disruptions for big-pdb
					creationTime: timeNow,
				},
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n6"}, Plugin: alwaysValidPlugin}, // Remaining disruptions for big-pdb exceeded
					creationTime: timeNow,
				},
			},
			scaleDownTimes: []*time.Time{nil, nil, nil, nil},
			wantPdbs: []*v1.PodDisruptionBudget{
				buildPdb("big-pdb", -2),
				buildPdb("small-pdb", 0),
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n1"}, Plugin: alwaysValidPlugin},
					creationTime: timeNow,
				},
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n4", "n5"}, Plugin: alwaysValidPlugin},
					creationTime: timeNow,
				},
			},
			wantBackoffFilteredNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {"n1", "n2", "n3", "n4", "n5", "n6"},
				neverValidPlugin:  {"n1", "n2", "n3", "n4", "n5", "n6"},
			},
			wantBackoffedNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {},
				neverValidPlugin:  {},
			},
		},
		{
			name: "partial candidate nodes are reorganized",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 1): {},
				buildReadyNode("n2", 1000, 1): {},
				buildReadyNode("n3", 1000, 1): {},
				buildReadyNode("n4", 1000, 1): {},
			},
			candidateInfos: []*candidateInfo{
				{
					candidate: &defrag.Candidate{
						Nodes:  []string{"n1", "n2", "n3", "n4"},
						Mode:   defrag.Partial,
						Plugin: alwaysValidPlugin,
					},
					creationTime: timeNow,
					defragPossibleMap: map[string]time.Time{
						"n2": timeNow,
						"n3": timeNow,
					},
				},
			},
			scaleDownTimes: []*time.Time{nil},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate: &defrag.Candidate{
						Nodes:  []string{"n2", "n3", "n1", "n4"},
						Mode:   defrag.Partial,
						Plugin: alwaysValidPlugin,
					},
					creationTime: timeNow,
					defragPossibleMap: map[string]time.Time{
						"n2": timeNow,
						"n3": timeNow,
					},
				},
			},
			wantBackoffFilteredNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {"n1", "n2", "n3", "n4"},
				neverValidPlugin:  {"n1", "n2", "n3", "n4"},
			},
			wantBackoffedNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {},
				neverValidPlugin:  {},
			},
		},
		{
			name: "everything together",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 1): {
					test.SetRSPodSpec(test.BuildScheduledTestPod("p1", 100, 1, "n1"), "rs"),
				},
				test.BuildTestNode("n2", 1000, 1):      {},
				buildUpcomingNode("n3", 1000, 1):       {},
				buildDuringDeletionNode("n4", 1000, 1): {},
				buildReadyNode("n5", 1000, 1): {
					test.BuildScheduledTestPod("p2", 100, 1, "n9"),
				},
				buildReadyNode("m1", 1000, 1):  {},
				buildReadyNode("n6", 1000, 1):  {},
				buildReadyNode("n7", 1000, 1):  {},
				buildReadyNode("n8", 1000, 1):  {},
				buildReadyNode("n9", 1000, 1):  {},
				buildReadyNode("n10", 1000, 1): {},
				buildReadyNode("n11", 1000, 1): {},
				buildReadyNode("n12", 1000, 1): {},
			},
			candidateInfos: []*candidateInfo{
				{
					candidate:    &defrag.Candidate{Nodes: []string{}, Plugin: alwaysValidPlugin},
					creationTime: timeNow,
				},
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n0", "n1", "n2", "n3", "n4", "n5", "m1"}, Plugin: alwaysValidPlugin},
					creationTime: timeNow,
				},
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n0", "n2", "n3", "n4", "n5", "m1"}, Plugin: alwaysValidPlugin},
					creationTime: timeNow,
				},
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n6"}, Plugin: alwaysValidPlugin},
					creationTime: timeNow,
				},
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n7"}, Plugin: alwaysValidPlugin},
					creationTime: timeNow,
				},
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n8"}, Plugin: alwaysValidPlugin},
					creationTime: overScaleUpTimeout,
				},
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n9"}, Plugin: alwaysValidPlugin},
					creationTime: overScaleUpTimeout,
				},
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n10"}, Plugin: alwaysValidPlugin},
					creationTime: overScaleUpTimeout,
				},
				{
					candidate:        &defrag.Candidate{Nodes: []string{"n11"}, Plugin: alwaysValidPlugin},
					scaleUpNoOptions: true,
				},
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n12"}, Plugin: neverValidPlugin},
					creationTime: timeNow,
				},
			},
			scaleDownTimes: []*time.Time{
				nil,
				nil,
				nil,
				&timeNow,
				&overScaleDownTimeout,
				nil,
				&timeNow,
				&overScaleDownTimeout,
				nil,
				nil,
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n1"}, Plugin: alwaysValidPlugin},
					creationTime: timeNow,
				},
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n6"}, Plugin: alwaysValidPlugin},
					creationTime: timeNow,
				},
				{
					candidate:    &defrag.Candidate{Nodes: []string{"n9"}, Plugin: alwaysValidPlugin},
					creationTime: overScaleUpTimeout,
				},
			},
			wantBackoffFilteredNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {"n1", "n2", "n3", "n4", "n5", "n6", "n9", "n12", "m1"},
				neverValidPlugin:  {"n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9", "n10", "n11", "n12", "m1"},
			},
			wantBackoffedNodes: map[defrag.Plugin][]string{
				alwaysValidPlugin: {"n7", "n8", "n10", "n11"},
				neverValidPlugin:  {},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := testprovider.NewTestCloudProviderBuilder().Build()
			provider.AddNodeGroup("ng1", tc.minNodeGroupSize, 1000, len(tc.nodesWithPods))

			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			var nodes []string
			for node, pods := range tc.nodesWithPods {
				provider.AddNode("ng1", node)
				nodes = append(nodes, node.Name)
				assert.NoError(t, snapshot.AddNodeInfo(framework.NewTestNodeInfo(node, pods...)))
			}

			pdbTracker := pdb.NewBasicRemainingPdbTracker()
			assert.NoError(t, pdbTracker.SetPdbs(tc.pdbs))

			config := Config{ScaleUpTimeout: 10 * time.Minute, ScaleDownTimeout: 5 * time.Minute}
			deleteOpts := options.NodeDeleteOptions{}
			processor := NewProcessor(Options{
				ScaleDownNodeProcessor: onlyNXNodesProcessor,
				DeleteOptions:          deleteOpts,
				DrainabilityRules:      rules.Default(deleteOpts),
				Config:                 config,
			})

			scaleDownActuator := &mockScaleDownActuator{}
			scaleDownActuator.On("CheckStatus").Return(&fakeActuationStatus{})

			processor.candidateInfos = tc.candidateInfos
			processor.ctx = &cacontext.AutoscalingContext{
				ClusterSnapshot:     snapshot,
				RemainingPdbTracker: pdbTracker,
				CloudProvider:       provider,
				ScaleDownActuator:   scaleDownActuator,
			}
			nodeFilter, err := processor.nodeFilterFactory.NewDefragNodeFilter(processor.ctx)
			assert.NoError(t, err)

			for idx := range tc.candidateInfos {
				if tc.scaleDownTimes[idx] != nil {
					for _, nodeName := range tc.candidateInfos[idx].candidate.Nodes {
						processor.actuator.scaledDownNodes[nodeName] = *tc.scaleDownTimes[idx]
					}
				}
			}

			assert.NoError(t, processor.cleanUpCandidates(nodeFilter))
			assert.ElementsMatch(t, tc.wantPdbs, processor.pdbTracker.GetPdbs())
			assert.Equal(t, tc.wantCandidateInfos, processor.candidateInfos)
			for _, plugin := range allPlugins {
				availableNodes, backedOffNodes := processor.backoff.splitNodesBasedOnBackoff(plugin, nodes)
				assert.ElementsMatch(t, tc.wantBackoffFilteredNodes[plugin], availableNodes)
				assert.ElementsMatch(t, tc.wantBackoffedNodes[plugin], backedOffNodes)
			}
		})
	}
}

func TestShouldRemoveCandidate(t *testing.T) {
	alwaysValidPlugin := &mockPlugin{}
	alwaysValidPlugin.On("IsCandidateValid", mock.Anything, mock.Anything).Return(true)
	alwaysValidPlugin.On("Type").Return(defrag.StandardPluginType)

	neverValidPlugin := &mockPlugin{}
	neverValidPlugin.On("IsCandidateValid", mock.Anything, mock.Anything).Return(false)
	neverValidPlugin.On("Type").Return(defrag.StandardPluginType)

	resizesOnlyPlugin := &mockPlugin{}
	resizesOnlyPlugin.On("IsCandidateValid", mock.Anything, mock.Anything).Return(true)
	resizesOnlyPlugin.On("Type").Return(defrag.ResizesOnlyPluginType)

	timeNow := time.Now()
	overScaleUpTimeout := time.Now().Add(-10 * time.Minute)
	overScaleDownTimeout := time.Now().Add(-5 * time.Minute)

	testCases := []struct {
		name             string
		candidateInfo    *candidateInfo
		scaleDownTime    *time.Time
		wantShouldRemove bool
		wantReason       removeCandidateReason
	}{
		{
			name: "candidate with some nodes is not removed",
			candidateInfo: &candidateInfo{
				candidate:    &defrag.Candidate{Nodes: []string{"n1", "n2"}, Plugin: alwaysValidPlugin},
				creationTime: timeNow,
			},
			wantShouldRemove: false,
		},
		{
			name: "candidate without nodes is removed",
			candidateInfo: &candidateInfo{
				candidate:    &defrag.Candidate{Nodes: []string{}, Plugin: alwaysValidPlugin},
				creationTime: timeNow,
			},
			wantShouldRemove: true,
			wantReason:       noValidNodes,
		},
		{
			name: "candidate over scale up timeout is removed",
			candidateInfo: &candidateInfo{
				candidate:    &defrag.Candidate{Nodes: []string{"n1", "n2"}, Plugin: alwaysValidPlugin},
				creationTime: overScaleUpTimeout,
			},
			wantShouldRemove: true,
			wantReason:       scaleUpTimeoutExceeded,
		},
		{
			name: "candidate without scale up options is removed",
			candidateInfo: &candidateInfo{
				candidate:        &defrag.Candidate{Nodes: []string{"n1", "n2"}, Plugin: alwaysValidPlugin},
				creationTime:     timeNow,
				scaleUpNoOptions: true,
			},
			wantShouldRemove: true,
			wantReason:       noScaleUpOptions,
		},
		{
			name: "candidate without scale up options is not removed if it is waiting for upcoming nodes",
			candidateInfo: &candidateInfo{
				candidate:         &defrag.Candidate{Nodes: []string{"n1", "n2"}, Plugin: alwaysValidPlugin},
				creationTime:      timeNow,
				scaleUpNoOptions:  true,
				waitingForScaleUp: true,
			},
			wantShouldRemove: false,
		},
		{
			name: "candidate for resizes without scale up options not removed",
			candidateInfo: &candidateInfo{
				candidate:        &defrag.Candidate{Nodes: []string{"n1", "n2"}, Plugin: resizesOnlyPlugin},
				creationTime:     timeNow,
				scaleUpNoOptions: true,
			},
			wantShouldRemove: false,
		},
		{
			name: "no scale up options, waiting for scale down delay, not removed",
			candidateInfo: &candidateInfo{
				candidate:        &defrag.Candidate{Nodes: []string{"n1", "n2"}, Plugin: alwaysValidPlugin},
				creationTime:     timeNow,
				scaleUpNoOptions: true,
				defragPossibleMap: map[string]time.Time{
					"n1": timeNow,
				},
			},
			wantShouldRemove: false,
		},
		{
			name: "no scale up options, node with scale down delay no longer in the candidate, removed",
			candidateInfo: &candidateInfo{
				candidate:        &defrag.Candidate{Nodes: []string{"n1", "n2"}, Plugin: alwaysValidPlugin},
				creationTime:     timeNow,
				scaleUpNoOptions: true,
				defragPossibleMap: map[string]time.Time{
					"n3": timeNow,
				},
			},
			wantShouldRemove: true,
			wantReason:       noScaleUpOptions,
		},
		{
			name: "scaled down candidate with some nodes is not removed",
			candidateInfo: &candidateInfo{
				candidate:    &defrag.Candidate{Nodes: []string{"n1", "n2"}, Plugin: alwaysValidPlugin},
				creationTime: timeNow,
			},
			scaleDownTime:    &timeNow,
			wantShouldRemove: false,
		},
		{
			name: "candidate over scale down timeout is removed",
			candidateInfo: &candidateInfo{
				candidate:    &defrag.Candidate{Nodes: []string{"n1", "n2"}, Plugin: alwaysValidPlugin},
				creationTime: timeNow,
			},
			scaleDownTime:    &overScaleDownTimeout,
			wantShouldRemove: true,
			wantReason:       scaleDownTimeoutExceeded,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := Config{ScaleUpTimeout: 10 * time.Minute, ScaleDownTimeout: 5 * time.Minute}
			deleteOpts := options.NodeDeleteOptions{}
			processor := NewProcessor(Options{
				DeleteOptions:     deleteOpts,
				DrainabilityRules: rules.Default(deleteOpts),
				Config:            config,
			})
			if tc.scaleDownTime != nil {
				for _, nodeName := range tc.candidateInfo.candidate.Nodes {
					processor.actuator.scaledDownNodes[nodeName] = *tc.scaleDownTime
				}
			}

			shouldRemove, reason := processor.shouldRemoveCandidate(tc.candidateInfo)
			assert.Equal(t, tc.wantShouldRemove, shouldRemove)
			assert.Equal(t, tc.wantReason, reason)
		})
	}
}

func TestProcessCandidates(t *testing.T) {
	timeNow := time.Now()

	allNodesProcessor := &mockScaleDownNodeProcessor{
		candidatesFilter: func(nodes []*apiv1.Node) []*apiv1.Node {
			return nodes
		},
	}

	pod1 := test.SetRSPodSpec(test.BuildTestPod("p1", 400, 1), "rs")
	pod2 := test.SetRSPodSpec(test.BuildTestPod("p2", 500, 1), "rs")
	pod3 := test.SetRSPodSpec(test.BuildTestPod("p3", 600, 1), "rs")

	pdbPod1 := setPodLabel(test.SetRSPodSpec(test.BuildTestPod("p1-pdb", 400, 1), "rs"), "pdb")
	pdbPod2 := setPodLabel(test.SetRSPodSpec(test.BuildTestPod("p2-pdb", 500, 1), "rs"), "pdb")
	pdbPod3 := setPodLabel(test.SetRSPodSpec(test.BuildTestPod("p3-pdb", 600, 1), "rs"), "pdb")

	specialPlugin := mockPluginBuilder{
		targetNodeName: "special",
		mode:           defrag.CreateBeforeDelete,
	}.build()
	otherPlugin := mockPluginBuilder{
		targetNodeName: "other",
		mode:           defrag.CreateBeforeDelete,
	}.build()
	deletePlugin := mockPluginBuilder{
		targetNodeName: "delete",
		mode:           defrag.DeleteBeforeCreate,
	}.build()

	testCases := []struct {
		name           string
		pdbs           []*v1.PodDisruptionBudget
		nodesWithPods  map[*apiv1.Node][]*apiv1.Pod
		plugins        []defrag.Plugin
		candidateInfos []*candidateInfo

		wantStartDeletionCallsNodes  [][]*apiv1.Node
		wantStartDeletionCallsErrors []error

		wantPdbs                []*v1.PodDisruptionBudget
		wantNodesWithPods       map[*apiv1.Node][]*apiv1.Pod
		wantCandidateInfos      []*candidateInfo
		wantPickedCandidateInfo *candidateInfo
		wantCandidatePods       []*apiv1.Pod
		wantErr                 bool
	}{
		{
			name:    "no candidates, no new candidates",
			plugins: []defrag.Plugin{specialPlugin, otherPlugin},
		},
		{
			name: "no candidates, single new candidate with unschedulable pods",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 900, 10): {pod1, pod2},
			},
			plugins: []defrag.Plugin{specialPlugin, otherPlugin},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 900, 10): {pod1, pod2},
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:        &defrag.Candidate{Nodes: []string{"special"}, Plugin: specialPlugin},
					creationTime:     timeNow,
					scaleUpNoOptions: true,
				},
			},
			wantPickedCandidateInfo: &candidateInfo{
				candidate:        &defrag.Candidate{Nodes: []string{"special"}, Plugin: specialPlugin},
				creationTime:     timeNow,
				scaleUpNoOptions: true,
			},
			wantCandidatePods: []*apiv1.Pod{pod1, pod2},
		},
		{
			name: "no candidates, single new candidate with pods partially schedulable on upcoming",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 900, 10):     {pod1, pod2},
				buildUpcomingNode("upcoming", 400, 10): {},
			},
			plugins: []defrag.Plugin{specialPlugin, otherPlugin},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 900, 10):     {pod1, pod2},
				buildUpcomingNode("upcoming", 400, 10): {pod1},
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:        &defrag.Candidate{Nodes: []string{"special"}, Plugin: specialPlugin},
					creationTime:     timeNow,
					scaleUpNoOptions: true,
				},
			},
			wantPickedCandidateInfo: &candidateInfo{
				candidate:        &defrag.Candidate{Nodes: []string{"special"}, Plugin: specialPlugin},
				creationTime:     timeNow,
				scaleUpNoOptions: true,
			},
			wantCandidatePods: []*apiv1.Pod{pod2},
		},
		{
			name: "no candidates, single new candidate with pods fully schedulable on upcoming",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 900, 10):     {pod1, pod2},
				buildUpcomingNode("upcoming", 900, 10): {},
			},
			plugins: []defrag.Plugin{specialPlugin, otherPlugin},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 900, 10):     {pod1, pod2},
				buildUpcomingNode("upcoming", 900, 10): {pod1, pod2},
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:    &defrag.Candidate{Nodes: []string{"special"}, Plugin: specialPlugin},
					creationTime: timeNow,
				},
			},
			wantPickedCandidateInfo: nil,
			wantCandidatePods:       nil,
		},
		{
			name: "no candidates, single new candidate with pods partially schedulable on existing",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 900, 10): {pod1, pod2},
				buildReadyNode("ready", 400, 10):   {},
			},
			plugins: []defrag.Plugin{specialPlugin, otherPlugin},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 900, 10): {pod1, pod2},
				buildReadyNode("ready", 400, 10):   {pod1},
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:        &defrag.Candidate{Nodes: []string{"special"}, Plugin: specialPlugin},
					creationTime:     timeNow,
					scaleUpNoOptions: true,
				},
			},
			wantPickedCandidateInfo: &candidateInfo{
				candidate:        &defrag.Candidate{Nodes: []string{"special"}, Plugin: specialPlugin},
				creationTime:     timeNow,
				scaleUpNoOptions: true,
			},
			wantCandidatePods: []*apiv1.Pod{pod2},
		},
		{
			name: "no candidates, single new candidate with pods fully schedulable on existing",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 900, 10): {pod1, pod2},
				buildReadyNode("ready", 900, 10):   {},
			},
			plugins: []defrag.Plugin{specialPlugin, otherPlugin},
			wantStartDeletionCallsNodes: [][]*apiv1.Node{
				{
					buildReadyNode("special", 900, 10),
				},
			},
			wantStartDeletionCallsErrors: []error{nil},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 900, 10): {pod1, pod2},
				buildReadyNode("ready", 900, 10):   {pod1, pod2},
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:          &defrag.Candidate{Nodes: []string{"special"}, Plugin: specialPlugin},
					creationTime:       timeNow,
					defragPossible:     true,
					defragPossibleTime: timeNow,
				},
			},
			wantPickedCandidateInfo: nil,
			wantCandidatePods:       nil,
		},
		{
			name: "no candidates, single potential candidate without enough remaining PDB",
			pdbs: []*v1.PodDisruptionBudget{buildPdb("pdb", 0)},
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 900, 10): {pdbPod1, pdbPod2},
				buildReadyNode("ready", 900, 10):   {},
			},
			plugins:  []defrag.Plugin{specialPlugin, otherPlugin},
			wantPdbs: []*v1.PodDisruptionBudget{buildPdb("pdb", 0)},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 900, 10): {pdbPod1, pdbPod2},
				buildReadyNode("ready", 900, 10):   {},
			},
			wantPickedCandidateInfo: nil,
			wantCandidatePods:       nil,
		},
		{
			name: "no candidates, multiple potential candidates, but the first one is not fully reschedulable",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 900, 10): {pod1, pod2},
				buildReadyNode("other", 600, 10):   {pod3},
				buildReadyNode("ready", 400, 10):   {},
			},
			plugins: []defrag.Plugin{specialPlugin, otherPlugin},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 900, 10): {pod1, pod2},
				buildReadyNode("other", 600, 10):   {pod3},
				buildReadyNode("ready", 400, 10):   {pod1},
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:        &defrag.Candidate{Nodes: []string{"special"}, Plugin: specialPlugin},
					creationTime:     timeNow,
					scaleUpNoOptions: true,
				},
			},
			wantPickedCandidateInfo: &candidateInfo{
				candidate:        &defrag.Candidate{Nodes: []string{"special"}, Plugin: specialPlugin},
				creationTime:     timeNow,
				scaleUpNoOptions: true,
			},
			wantCandidatePods: []*apiv1.Pod{pod2},
		},
		{
			name: "no candidates, multiple potential candidates, only first is fully reschedulable",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 900, 10): {pod1, pod2},
				buildReadyNode("other", 600, 10):   {pod3},
				buildReadyNode("ready", 900, 10):   {},
			},
			plugins: []defrag.Plugin{specialPlugin, otherPlugin},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 900, 10): {pod1, pod2},
				buildReadyNode("other", 600, 10):   {pod3},
				buildReadyNode("ready", 900, 10):   {pod1, pod2},
			},
			wantStartDeletionCallsNodes: [][]*apiv1.Node{
				{
					buildReadyNode("special", 900, 10),
				},
			},
			wantStartDeletionCallsErrors: []error{nil},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:          &defrag.Candidate{Nodes: []string{"special"}, Plugin: specialPlugin},
					creationTime:       timeNow,
					defragPossible:     true,
					defragPossibleTime: timeNow,
				},
				{
					candidate:        &defrag.Candidate{Nodes: []string{"other"}, Plugin: otherPlugin},
					creationTime:     timeNow,
					scaleUpNoOptions: true,
				},
			},
			wantPickedCandidateInfo: &candidateInfo{
				candidate:        &defrag.Candidate{Nodes: []string{"other"}, Plugin: otherPlugin},
				creationTime:     timeNow,
				scaleUpNoOptions: true,
			},
			wantCandidatePods: []*apiv1.Pod{pod3},
		},
		{
			name: "no candidates, multiple potential candidates, both are fully reschedulable",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 900, 10): {pod1, pod2},
				buildReadyNode("other", 600, 10):   {pod3},
				buildReadyNode("ready", 1500, 10):  {},
			},
			plugins:                      []defrag.Plugin{specialPlugin, otherPlugin},
			wantStartDeletionCallsErrors: []error{nil, nil},
			wantStartDeletionCallsNodes: [][]*apiv1.Node{
				{
					buildReadyNode("special", 900, 10),
				},
				{
					buildReadyNode("other", 600, 10),
				},
			},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 900, 10): {pod1, pod2},
				buildReadyNode("other", 600, 10):   {pod3},
				buildReadyNode("ready", 1500, 10):  {pod1, pod2, pod3},
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:          &defrag.Candidate{Nodes: []string{"special"}, Plugin: specialPlugin},
					creationTime:       timeNow,
					defragPossible:     true,
					defragPossibleTime: timeNow,
				},
				{
					candidate:          &defrag.Candidate{Nodes: []string{"other"}, Plugin: otherPlugin},
					creationTime:       timeNow,
					defragPossible:     true,
					defragPossibleTime: timeNow,
				},
			},
			wantPickedCandidateInfo: nil,
			wantCandidatePods:       nil,
		},
		{
			name: "no candidates, multiple potential candidates, only one within remaining pdb",
			pdbs: []*v1.PodDisruptionBudget{buildPdb("pdb", 1)},
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 900, 10): {pdbPod1, pdbPod2},
				buildReadyNode("other", 600, 10):   {pdbPod3},
				buildReadyNode("ready", 1500, 10):  {},
			},
			plugins: []defrag.Plugin{specialPlugin, otherPlugin},
			wantStartDeletionCallsNodes: [][]*apiv1.Node{
				{
					buildReadyNode("special", 900, 10),
				},
			},
			wantStartDeletionCallsErrors: []error{nil},
			wantPdbs:                     []*v1.PodDisruptionBudget{buildPdb("pdb", -1)},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 900, 10): {pdbPod1, pdbPod2},
				buildReadyNode("other", 600, 10):   {pdbPod3},
				buildReadyNode("ready", 1500, 10):  {pdbPod1, pdbPod2},
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:          &defrag.Candidate{Nodes: []string{"special"}, Plugin: specialPlugin},
					creationTime:       timeNow,
					defragPossible:     true,
					defragPossibleTime: timeNow,
				},
			},
			wantPickedCandidateInfo: nil,
			wantCandidatePods:       nil,
		},
		{
			name: "single non-reschedulable candidate, no new candidates",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("old", 400, 10):     {pod1},
				buildReadyNode("special", 500, 10): {pod2},
				buildReadyNode("other", 600, 10):   {pod3},
				buildReadyNode("ready", 200, 10):   {},
			},
			plugins: []defrag.Plugin{specialPlugin, otherPlugin},
			candidateInfos: []*candidateInfo{
				{
					candidate:    &defrag.Candidate{Nodes: []string{"old"}},
					creationTime: timeNow.Add(-10 * time.Minute),
				},
			},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("old", 400, 10):     {pod1},
				buildReadyNode("special", 500, 10): {pod2},
				buildReadyNode("other", 600, 10):   {pod3},
				buildReadyNode("ready", 200, 10):   {},
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:        &defrag.Candidate{Nodes: []string{"old"}},
					creationTime:     timeNow.Add(-10 * time.Minute),
					scaleUpNoOptions: true,
				},
			},
			wantPickedCandidateInfo: &candidateInfo{
				candidate:        &defrag.Candidate{Nodes: []string{"old"}},
				creationTime:     timeNow.Add(-10 * time.Minute),
				scaleUpNoOptions: true,
			},
			wantCandidatePods: []*apiv1.Pod{pod1},
		},
		{
			name: "single reschedulable candidate, single new non-reschedulable candidate",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("old", 400, 10):     {pod1},
				buildReadyNode("special", 500, 10): {pod2},
				buildReadyNode("other", 600, 10):   {pod3},
				buildReadyNode("ready", 400, 10):   {},
			},
			plugins: []defrag.Plugin{specialPlugin, otherPlugin},
			candidateInfos: []*candidateInfo{
				{
					candidate:          &defrag.Candidate{Nodes: []string{"old"}, Plugin: otherPlugin},
					creationTime:       timeNow.Add(-10 * time.Minute),
					defragPossible:     true,
					defragPossibleTime: timeNow.Add(-2 * time.Minute),
				},
			},
			wantStartDeletionCallsNodes: [][]*apiv1.Node{
				{buildReadyNode("old", 400, 10)},
			},
			wantStartDeletionCallsErrors: []error{nil},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("old", 400, 10):     {pod1},
				buildReadyNode("special", 500, 10): {pod2},
				buildReadyNode("other", 600, 10):   {pod3},
				buildReadyNode("ready", 400, 10):   {pod1},
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:          &defrag.Candidate{Nodes: []string{"old"}, Plugin: otherPlugin},
					creationTime:       timeNow.Add(-10 * time.Minute),
					defragPossible:     true,
					defragPossibleTime: timeNow.Add(-2 * time.Minute),
				},
				{
					candidate:        &defrag.Candidate{Nodes: []string{"special"}, Plugin: specialPlugin},
					creationTime:     timeNow,
					scaleUpNoOptions: true,
				},
			},
			wantPickedCandidateInfo: &candidateInfo{
				candidate:        &defrag.Candidate{Nodes: []string{"special"}, Plugin: specialPlugin},
				creationTime:     timeNow,
				scaleUpNoOptions: true,
			},
			wantCandidatePods: []*apiv1.Pod{pod2},
		},
		{
			name: "single reschedulable candidate, two new reschedulable & non-reschedulable candidates",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("old", 400, 10):     {pod1},
				buildReadyNode("special", 500, 10): {pod2},
				buildReadyNode("other", 600, 10):   {pod3},
				buildReadyNode("ready", 900, 10):   {},
			},
			plugins: []defrag.Plugin{specialPlugin, otherPlugin},
			candidateInfos: []*candidateInfo{
				{
					candidate:          &defrag.Candidate{Nodes: []string{"old"}, Plugin: otherPlugin},
					creationTime:       timeNow.Add(-10 * time.Minute),
					defragPossible:     true,
					defragPossibleTime: timeNow.Add(-2 * time.Minute),
				},
			},
			wantStartDeletionCallsNodes: [][]*apiv1.Node{
				{buildReadyNode("old", 400, 10)},
				{buildReadyNode("special", 500, 10)},
			},
			wantStartDeletionCallsErrors: []error{nil, nil},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("old", 400, 10):     {pod1},
				buildReadyNode("special", 500, 10): {pod2},
				buildReadyNode("other", 600, 10):   {pod3},
				buildReadyNode("ready", 900, 10):   {pod1, pod2},
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:          &defrag.Candidate{Nodes: []string{"old"}, Plugin: otherPlugin},
					creationTime:       timeNow.Add(-10 * time.Minute),
					defragPossible:     true,
					defragPossibleTime: timeNow.Add(-2 * time.Minute),
				},
				{
					candidate:          &defrag.Candidate{Nodes: []string{"special"}, Plugin: specialPlugin},
					creationTime:       timeNow,
					defragPossible:     true,
					defragPossibleTime: timeNow,
				},
				{
					candidate:        &defrag.Candidate{Nodes: []string{"other"}, Plugin: otherPlugin},
					creationTime:     timeNow,
					scaleUpNoOptions: true,
				},
			},
			wantPickedCandidateInfo: &candidateInfo{
				candidate:        &defrag.Candidate{Nodes: []string{"other"}, Plugin: otherPlugin},
				creationTime:     timeNow,
				scaleUpNoOptions: true,
			},
			wantCandidatePods: []*apiv1.Pod{pod3},
		},
		{
			name: "single reschedulable candidate, two new reschedulable candidates",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("old", 400, 10):     {pod1},
				buildReadyNode("special", 500, 10): {pod2},
				buildReadyNode("other", 600, 10):   {pod3},
				buildReadyNode("ready", 1500, 10):  {},
			},
			plugins: []defrag.Plugin{specialPlugin, otherPlugin},
			candidateInfos: []*candidateInfo{
				{
					candidate:          &defrag.Candidate{Nodes: []string{"old"}, Plugin: otherPlugin},
					creationTime:       timeNow.Add(-10 * time.Minute),
					defragPossible:     true,
					defragPossibleTime: timeNow.Add(-2 * time.Minute),
				},
			},
			wantStartDeletionCallsNodes: [][]*apiv1.Node{
				{buildReadyNode("old", 400, 10)},
				{buildReadyNode("special", 500, 10)},
				{buildReadyNode("other", 600, 10)},
			},
			wantStartDeletionCallsErrors: []error{nil, nil, nil},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("old", 400, 10):     {pod1},
				buildReadyNode("special", 500, 10): {pod2},
				buildReadyNode("other", 600, 10):   {pod3},
				buildReadyNode("ready", 1500, 10):  {pod1, pod2, pod3},
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:          &defrag.Candidate{Nodes: []string{"old"}, Plugin: otherPlugin},
					creationTime:       timeNow.Add(-10 * time.Minute),
					defragPossible:     true,
					defragPossibleTime: timeNow.Add(-2 * time.Minute),
				},
				{
					candidate:          &defrag.Candidate{Nodes: []string{"special"}, Plugin: specialPlugin},
					creationTime:       timeNow,
					defragPossible:     true,
					defragPossibleTime: timeNow,
				},
				{
					candidate:          &defrag.Candidate{Nodes: []string{"other"}, Plugin: otherPlugin},
					creationTime:       timeNow,
					defragPossible:     true,
					defragPossibleTime: timeNow,
				},
			},
			wantPickedCandidateInfo: nil,
			wantCandidatePods:       nil,
		},
		{
			name: "first candidate schedulable on existing, second on upcoming",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 400, 10):     {pod1},
				buildReadyNode("other", 500, 10):       {pod2},
				buildReadyNode("ready", 400, 10):       {},
				buildUpcomingNode("upcoming", 500, 10): {},
			},
			plugins: []defrag.Plugin{specialPlugin, otherPlugin},
			candidateInfos: []*candidateInfo{
				{
					candidate:          &defrag.Candidate{Nodes: []string{"special"}, Plugin: specialPlugin},
					creationTime:       timeNow.Add(-10 * time.Minute),
					defragPossible:     true,
					defragPossibleTime: timeNow.Add(-2 * time.Minute),
				},
				{
					candidate:          &defrag.Candidate{Nodes: []string{"other"}, Plugin: otherPlugin},
					creationTime:       timeNow.Add(-20 * time.Minute),
					defragPossible:     true,
					defragPossibleTime: timeNow.Add(-2 * time.Minute),
				},
			},
			wantStartDeletionCallsNodes: [][]*apiv1.Node{
				{buildReadyNode("special", 400, 10)},
			},
			wantStartDeletionCallsErrors: []error{nil},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 400, 10):     {pod1},
				buildReadyNode("other", 500, 10):       {pod2},
				buildReadyNode("ready", 400, 10):       {pod1},
				buildUpcomingNode("upcoming", 500, 10): {pod2},
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:          &defrag.Candidate{Nodes: []string{"special"}, Plugin: specialPlugin},
					creationTime:       timeNow.Add(-10 * time.Minute),
					defragPossible:     true,
					defragPossibleTime: timeNow.Add(-2 * time.Minute),
				},
				{
					candidate:          &defrag.Candidate{Nodes: []string{"other"}, Plugin: otherPlugin},
					creationTime:       timeNow.Add(-20 * time.Minute),
					defragPossible:     false,
					defragPossibleTime: timeNow.Add(-2 * time.Minute),
				},
			},
			wantPickedCandidateInfo: nil,
			wantCandidatePods:       nil,
		},
		{
			name: "first candidate schedulable on the second",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 400, 10):     {pod1},
				buildReadyNode("other", 900, 10):       {pod2},
				buildUpcomingNode("upcoming", 400, 10): {},
			},
			plugins: []defrag.Plugin{specialPlugin, otherPlugin},
			wantStartDeletionCallsNodes: [][]*apiv1.Node{
				{buildReadyNode("special", 400, 10)},
			},
			wantStartDeletionCallsErrors: []error{nil},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 400, 10):     {pod1},
				buildReadyNode("other", 900, 10):       {pod1, pod2},
				buildUpcomingNode("upcoming", 400, 10): {pod1},
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:          &defrag.Candidate{Nodes: []string{"special"}, Plugin: specialPlugin},
					creationTime:       timeNow,
					defragPossible:     true,
					defragPossibleTime: timeNow,
				},
				{
					candidate:        &defrag.Candidate{Nodes: []string{"other"}, Plugin: otherPlugin},
					creationTime:     timeNow,
					scaleUpNoOptions: true,
				},
			},
			wantPickedCandidateInfo: &candidateInfo{
				candidate:        &defrag.Candidate{Nodes: []string{"other"}, Plugin: otherPlugin},
				creationTime:     timeNow,
				scaleUpNoOptions: true,
			},
			wantCandidatePods: []*apiv1.Pod{pod2},
		},
		{
			name: "candidate is delete before create",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("delete", 900, 10): {pod1, pod2},
				buildReadyNode("other", 600, 10):  {pod3},
				buildReadyNode("ready", 400, 10):  {},
			},
			plugins: []defrag.Plugin{deletePlugin, otherPlugin},
			wantStartDeletionCallsNodes: [][]*apiv1.Node{
				{buildReadyNode("delete", 900, 10)},
			},
			wantStartDeletionCallsErrors: []error{nil},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("delete", 900, 10): {pod1, pod2},
				buildReadyNode("other", 600, 10):  {pod3},
				buildReadyNode("ready", 400, 10):  {pod1},
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:        &defrag.Candidate{Nodes: []string{"delete"}, Mode: defrag.DeleteBeforeCreate, Plugin: deletePlugin},
					creationTime:     timeNow,
					scaleUpNoOptions: true,
				},
			},
			wantPickedCandidateInfo: &candidateInfo{
				candidate:        &defrag.Candidate{Nodes: []string{"delete"}, Mode: defrag.DeleteBeforeCreate, Plugin: deletePlugin},
				creationTime:     timeNow,
				scaleUpNoOptions: true,
			},
			wantCandidatePods: []*apiv1.Pod{pod2},
		},
		{
			name: "candidate limit reached, no new candidates",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 400, 10):      {pod1},
				buildReadyNode("n2", 400, 10):      {pod1},
				buildReadyNode("n3", 400, 10):      {pod1},
				buildReadyNode("special", 400, 10): {pod1},
				buildReadyNode("big", 10000, 10):   {},
			},
			plugins: []defrag.Plugin{specialPlugin, otherPlugin},
			candidateInfos: []*candidateInfo{
				{
					candidate:          &defrag.Candidate{Nodes: []string{"n1"}, Plugin: otherPlugin},
					creationTime:       timeNow.Add(-10 * time.Minute),
					defragPossible:     true,
					defragPossibleTime: timeNow.Add(-2 * time.Minute),
				},
				{
					candidate:          &defrag.Candidate{Nodes: []string{"n2"}, Plugin: otherPlugin},
					creationTime:       timeNow.Add(-20 * time.Minute),
					defragPossible:     true,
					defragPossibleTime: timeNow.Add(-2 * time.Minute),
				},
				{
					candidate:          &defrag.Candidate{Nodes: []string{"n3"}, Plugin: otherPlugin},
					creationTime:       timeNow.Add(-30 * time.Minute),
					defragPossible:     true,
					defragPossibleTime: timeNow.Add(-2 * time.Minute),
				},
			},
			wantStartDeletionCallsNodes: [][]*apiv1.Node{
				{buildReadyNode("n1", 400, 10)},
				{buildReadyNode("n2", 400, 10)},
				{buildReadyNode("n3", 400, 10)},
			},
			wantStartDeletionCallsErrors: []error{nil, nil, nil},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 400, 10):      {pod1},
				buildReadyNode("n2", 400, 10):      {pod1},
				buildReadyNode("n3", 400, 10):      {pod1},
				buildReadyNode("special", 400, 10): {pod1},
				buildReadyNode("big", 10000, 10):   {pod1, pod1, pod1},
			},
			wantCandidateInfos: []*candidateInfo{
				{
					candidate:          &defrag.Candidate{Nodes: []string{"n1"}, Plugin: otherPlugin},
					creationTime:       timeNow.Add(-10 * time.Minute),
					defragPossible:     true,
					defragPossibleTime: timeNow.Add(-2 * time.Minute),
				},
				{
					candidate:          &defrag.Candidate{Nodes: []string{"n2"}, Plugin: otherPlugin},
					creationTime:       timeNow.Add(-20 * time.Minute),
					defragPossible:     true,
					defragPossibleTime: timeNow.Add(-2 * time.Minute),
				},
				{
					candidate:          &defrag.Candidate{Nodes: []string{"n3"}, Plugin: otherPlugin},
					creationTime:       timeNow.Add(-30 * time.Minute),
					defragPossible:     true,
					defragPossibleTime: timeNow.Add(-2 * time.Minute),
				},
			},
			wantPickedCandidateInfo: nil,
			wantCandidatePods:       nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := testprovider.NewTestCloudProviderBuilder().Build()
			provider.AddNodeGroup("ng1", 0, 1000, len(tc.nodesWithPods))

			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			client := fake.NewClientset()
			for node, pods := range tc.nodesWithPods {
				provider.AddNode("ng1", node)
				assert.NoError(t, snapshot.AddNodeInfo(framework.NewTestNodeInfo(node, pods...)))
				_, err := client.CoreV1().Nodes().Create(context.TODO(), node, metav1.CreateOptions{})
				assert.NoError(t, err)
			}

			fakeClock := &FakePassiveClock{}
			fakeClock.SetTime(timeNow)

			defragTaints := []apiv1.Taint{
				{
					Key:    defrag.HardTaint,
					Value:  fmt.Sprint(fakeClock.Now().Unix()),
					Effect: apiv1.TaintEffectNoSchedule,
				},
				{
					Key:    defrag.SoftTaint,
					Value:  fmt.Sprint(fakeClock.Now().Unix()),
					Effect: apiv1.TaintEffectPreferNoSchedule,
				},
			}
			for _, info := range tc.candidateInfos {
				for _, nodeName := range info.candidate.Nodes {
					nodeInfo, err := snapshot.GetNodeInfo(nodeName)
					assert.NoError(t, err)
					node := nodeInfo.Node()
					node.Spec.Taints = append(node.Spec.Taints, defragTaints...)
				}
			}

			scaleDownActuator := &mockScaleDownActuator{}
			scaleDownActuator.On("CheckStatus").Return(&fakeActuationStatus{})
			for i := range tc.wantStartDeletionCallsErrors {
				for _, n := range tc.wantStartDeletionCallsNodes[i] {
					n.Spec.Taints = append(n.Spec.Taints, defragTaints...)
				}
				scaleDownActuator.On("StartDeletion", []*apiv1.Node(nil), mock.MatchedBy(func(nodes []*apiv1.Node) bool {
					for j, node := range nodes {
						if node.Name != tc.wantStartDeletionCallsNodes[i][j].Name {
							return false
						}
					}
					return true
				})).Return(status.ScaleDownNoNodeDeleted, []*status.ScaleDownNode{}, tc.wantStartDeletionCallsErrors[i]).Once()
			}

			deleteOpts := options.NodeDeleteOptions{}
			processor := NewProcessor(Options{
				ScaleDownNodeProcessor: allNodesProcessor,
				DeleteOptions:          deleteOpts,
				DrainabilityRules:      rules.Default(deleteOpts),
				Plugins:                tc.plugins,
				Config:                 Config{CandidateLimit: 3},
				Clock:                  fakeClock,
			})
			processor.candidateInfos = tc.candidateInfos
			processor.ctx = &cacontext.AutoscalingContext{
				ClusterSnapshot:     snapshot,
				CloudProvider:       provider,
				ScaleDownActuator:   scaleDownActuator,
				RemainingPdbTracker: pdb.NewBasicRemainingPdbTracker(),
				AutoscalingKubeClients: cacontext.AutoscalingKubeClients{
					ClientSet: client,
				},
			}
			nodeFilter, err := processor.nodeFilterFactory.NewDefragNodeFilter(processor.ctx)
			assert.NoError(t, err)

			assert.NoError(t, processor.ctx.RemainingPdbTracker.SetPdbs(tc.pdbs))
			assert.NoError(t, processor.pdbTracker.SetPdbs(tc.pdbs))

			pods, err := processor.processCandidates(nodeFilter)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.ElementsMatch(t, tc.wantCandidatePods, pods)
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.wantPdbs, processor.pdbTracker.GetPdbs())
			assert.Equal(t, tc.wantCandidateInfos, processor.candidateInfos)
			assert.Equal(t, tc.wantPickedCandidateInfo, processor.pickedCandidateInfo)

			candidateNodes := make(map[string]bool)
			for _, info := range processor.candidateInfos {
				for _, nodeName := range info.candidate.Nodes {
					candidateNodes[nodeName] = true
				}
			}

			scaleDownActuator.AssertNumberOfCalls(t, "StartDeletion", len(tc.wantStartDeletionCallsNodes))
			for node, wantPods := range tc.wantNodesWithPods {
				if _, ok := candidateNodes[node.Name]; ok {
					node.Spec.Taints = append(node.Spec.Taints, defragTaints...)
				}
				nodeInfo, err := snapshot.GetNodeInfo(node.Name)
				assert.NoError(t, err)
				diff := cmp.Diff(node, nodeInfo.Node(),
					cmp.FilterPath(func(path cmp.Path) bool {
						return path.String() == "ObjectMeta.Labels"
					}, cmpopts.EquateEmpty()),
					cmpopts.IgnoreTypes(metav1.TypeMeta{}),                     // Ignore Kind and APIVersion
					cmpopts.IgnoreFields(metav1.ObjectMeta{}, "ManagedFields"), // Ignore server-set ManagedFields
				)
				if diff != "" {
					t.Errorf("nodes diff (-want +got):\n%s", diff)
				}
				var pods []*apiv1.Pod
				for _, podInfo := range nodeInfo.Pods() {
					pods = append(pods, podInfo.Pod)
				}
				assert.ElementsMatch(t, wantPods, pods)
			}
		})
	}
}

func TestProcessCandidate(t *testing.T) {
	timeNow := time.Now()

	pod1 := test.SetRSPodSpec(test.BuildTestPod("p1", 400, 1), "rs1")
	pod2 := test.SetRSPodSpec(test.BuildTestPod("p2", 500, 1), "rs2")
	pod3 := test.SetRSPodSpec(test.BuildTestPod("p3", 600, 1), "rs3")
	pod4 := test.SetRSPodSpec(test.BuildTestPod("p4", 500, 1), "rs3")
	pod5 := test.SetRSPodSpec(test.BuildTestPod("p5", 500, 1), "rs3")

	testCases := []struct {
		name              string
		nodesWithPods     map[*apiv1.Node][]*apiv1.Pod
		allCandidateNodes []string
		isScaledDown      bool
		candidateInfo     *candidateInfo

		wantStartDeletionCall  bool
		wantStartDeletionNodes []*apiv1.Node
		wantStartDeletionError error

		wantCandidatePods []*apiv1.Pod
		wantCandidateInfo *candidateInfo
		wantNodesWithPods map[*apiv1.Node][]*apiv1.Pod
		wantErr           bool
	}{
		{
			name: "candidate with non-reschedulable pods",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1500, 10): {pod1, pod2, pod3},
			},
			allCandidateNodes: []string{"n1"},
			candidateInfo: &candidateInfo{
				candidate:    &defrag.Candidate{Nodes: []string{"n1"}},
				creationTime: timeNow,
			},
			wantCandidatePods: []*apiv1.Pod{pod1, pod2, pod3},
			wantCandidateInfo: &candidateInfo{
				candidate:    &defrag.Candidate{Nodes: []string{"n1"}},
				creationTime: timeNow,
			},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1500, 10): {pod1, pod2, pod3},
			},
		},
		{
			name: "candidate with all pods reschedulable on upcoming",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1500, 10):    {pod1, pod2, pod3},
				buildUpcomingNode("n2", 1500, 10): nil,
			},
			allCandidateNodes: []string{"n1"},
			candidateInfo: &candidateInfo{
				candidate:    &defrag.Candidate{Nodes: []string{"n1"}},
				creationTime: timeNow,
			},
			wantCandidatePods: []*apiv1.Pod{},
			wantCandidateInfo: &candidateInfo{
				candidate:    &defrag.Candidate{Nodes: []string{"n1"}},
				creationTime: timeNow,
			},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1500, 10):    {pod1, pod2, pod3},
				buildUpcomingNode("n2", 1500, 10): {pod1, pod2, pod3},
			},
		},
		{
			name: "candidate with some pods reschedulable on upcoming",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1500, 10):   {pod1, pod2, pod3},
				buildUpcomingNode("n2", 400, 10): nil,
			},
			allCandidateNodes: []string{"n1"},
			candidateInfo: &candidateInfo{
				candidate:    &defrag.Candidate{Nodes: []string{"n1"}},
				creationTime: timeNow,
			},
			wantCandidatePods: []*apiv1.Pod{pod2, pod3},
			wantCandidateInfo: &candidateInfo{
				candidate:    &defrag.Candidate{Nodes: []string{"n1"}},
				creationTime: timeNow,
			},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1500, 10):   {pod1, pod2, pod3},
				buildUpcomingNode("n2", 400, 10): {pod1},
			},
		},
		{
			name: "candidate with all pods reschedulable on ready just now",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1500, 10): {pod1, pod2, pod3},
				buildReadyNode("n2", 1500, 10): nil,
			},
			allCandidateNodes: []string{"n1"},
			candidateInfo: &candidateInfo{
				candidate:    &defrag.Candidate{Nodes: []string{"n1"}},
				creationTime: timeNow,
			},
			wantCandidatePods: []*apiv1.Pod{},
			wantCandidateInfo: &candidateInfo{
				candidate:          &defrag.Candidate{Nodes: []string{"n1"}},
				creationTime:       timeNow,
				defragPossible:     true,
				defragPossibleTime: timeNow,
			},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1500, 10): {pod1, pod2, pod3},
				buildReadyNode("n2", 1500, 10): {pod1, pod2, pod3},
			},
		},
		{
			name: "candidate with all pods reschedulable on ready for some time",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1500, 10): {pod1, pod2, pod3},
				buildReadyNode("n2", 1500, 10): nil,
			},
			allCandidateNodes: []string{"n1"},
			candidateInfo: &candidateInfo{
				candidate:          &defrag.Candidate{Nodes: []string{"n1"}},
				creationTime:       timeNow,
				defragPossible:     true,
				defragPossibleTime: timeNow.Add(-1 * time.Minute),
			},
			wantStartDeletionCall: true,
			wantStartDeletionNodes: []*apiv1.Node{
				buildReadyNode("n1", 1500, 10),
			},
			wantCandidatePods: []*apiv1.Pod{},
			wantCandidateInfo: &candidateInfo{
				candidate:          &defrag.Candidate{Nodes: []string{"n1"}},
				creationTime:       timeNow,
				defragPossible:     true,
				defragPossibleTime: timeNow.Add(-1 * time.Minute),
			},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1500, 10): {pod1, pod2, pod3},
				buildReadyNode("n2", 1500, 10): {pod1, pod2, pod3},
			},
		},
		{
			name: "candidate with no longer reschedulable pods",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1500, 10): {pod1, pod2, pod3},
			},
			allCandidateNodes: []string{"n1"},
			candidateInfo: &candidateInfo{
				candidate:          &defrag.Candidate{Nodes: []string{"n1"}},
				creationTime:       timeNow,
				defragPossible:     true,
				defragPossibleTime: timeNow.Add(-1 * time.Minute),
			},
			wantCandidatePods: []*apiv1.Pod{pod1, pod2, pod3},
			wantCandidateInfo: &candidateInfo{
				candidate:          &defrag.Candidate{Nodes: []string{"n1"}},
				creationTime:       timeNow,
				defragPossibleTime: timeNow.Add(-1 * time.Minute),
			},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1500, 10): {pod1, pod2, pod3},
			},
		},
		{
			name: "candidate with some pods reschedulable on ready",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1500, 10): {pod1, pod2, pod3},
				buildReadyNode("n2", 400, 10):  nil,
			},
			allCandidateNodes: []string{"n1"},
			candidateInfo: &candidateInfo{
				candidate:    &defrag.Candidate{Nodes: []string{"n1"}},
				creationTime: timeNow,
			},
			wantCandidatePods: []*apiv1.Pod{pod2, pod3},
			wantCandidateInfo: &candidateInfo{
				candidate:    &defrag.Candidate{Nodes: []string{"n1"}},
				creationTime: timeNow,
			},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1500, 10): {pod1, pod2, pod3},
				buildReadyNode("n2", 400, 10):  {pod1},
			},
		},
		{
			name: "candidate with some pods reschedulable on ready, some on upcoming",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1500, 10):    {pod1, pod2, pod3},
				buildReadyNode("n2", 400, 10):     nil,
				buildUpcomingNode("n3", 1500, 10): nil,
			},
			allCandidateNodes: []string{"n1"},
			candidateInfo: &candidateInfo{
				candidate:    &defrag.Candidate{Nodes: []string{"n1"}},
				creationTime: timeNow,
			},
			wantCandidatePods: []*apiv1.Pod{},
			wantCandidateInfo: &candidateInfo{
				candidate:    &defrag.Candidate{Nodes: []string{"n1"}},
				creationTime: timeNow,
			},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1500, 10):    {pod1, pod2, pod3},
				buildReadyNode("n2", 400, 10):     {pod1},
				buildUpcomingNode("n3", 1500, 10): {pod2, pod3},
			},
		},
		{
			name: "candidate with non-reschedulable pods, but it is delete-before-create",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1500, 10): {pod1, pod2, pod3},
			},
			allCandidateNodes: []string{"n1"},
			candidateInfo: &candidateInfo{
				candidate:    &defrag.Candidate{Nodes: []string{"n1"}, Mode: defrag.DeleteBeforeCreate},
				creationTime: timeNow,
			},
			wantStartDeletionCall: true,
			wantStartDeletionNodes: []*apiv1.Node{
				buildReadyNode("n1", 1500, 10),
			},
			wantCandidatePods: []*apiv1.Pod{pod1, pod2, pod3},
			wantCandidateInfo: &candidateInfo{
				candidate:    &defrag.Candidate{Nodes: []string{"n1"}, Mode: defrag.DeleteBeforeCreate},
				creationTime: timeNow,
			},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1500, 10): {pod1, pod2, pod3},
			},
		},
		{
			name: "candidate already initiated scaled-down",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1500, 10): {pod1, pod2, pod3},
				buildReadyNode("n2", 1500, 10): nil,
			},
			allCandidateNodes: []string{"n1"},
			isScaledDown:      true,
			candidateInfo: &candidateInfo{
				candidate:    &defrag.Candidate{Nodes: []string{"n1"}},
				creationTime: timeNow,
			},
			wantCandidatePods: []*apiv1.Pod{},
			wantCandidateInfo: &candidateInfo{
				candidate:    &defrag.Candidate{Nodes: []string{"n1"}},
				creationTime: timeNow,
			},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1500, 10): {pod1, pod2, pod3},
				buildReadyNode("n2", 1500, 10): {pod1, pod2, pod3},
			},
		},
		{
			name: "error during node scale-down",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1500, 10): {pod1, pod2, pod3},
				buildReadyNode("n2", 1500, 10): nil,
			},
			allCandidateNodes: []string{"n1"},
			candidateInfo: &candidateInfo{
				candidate:          &defrag.Candidate{Nodes: []string{"n1"}},
				creationTime:       timeNow,
				defragPossible:     true,
				defragPossibleTime: timeNow.Add(-1 * time.Minute),
			},
			wantStartDeletionCall: true,
			wantStartDeletionNodes: []*apiv1.Node{
				buildReadyNode("n1", 1500, 10),
			},
			wantStartDeletionError: caerrors.NewAutoscalerError("type", "msg"),
			wantCandidateInfo: &candidateInfo{
				candidate:          &defrag.Candidate{Nodes: []string{"n1"}},
				creationTime:       timeNow,
				defragPossible:     true,
				defragPossibleTime: timeNow.Add(-1 * time.Minute),
			},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1500, 10): {pod1, pod2, pod3},
				buildReadyNode("n2", 1500, 10): {pod1, pod2, pod3},
			},
			wantErr: true,
		},
		{
			name: "partial defrag, some nodes can be scaled down",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 10): {pod1, pod2},
				buildReadyNode("n2", 1000, 10): {pod3},
				buildReadyNode("n3", 1000, 10): {},
			},
			allCandidateNodes: []string{"n1", "n2"},
			candidateInfo: &candidateInfo{
				candidate: &defrag.Candidate{
					Nodes: []string{"n1", "n2"},
					Mode:  defrag.Partial,
				},
				creationTime: timeNow,
				defragPossibleMap: map[string]time.Time{
					"n1": timeNow.Add(-1 * time.Minute),
				},
			},
			wantStartDeletionCall: true,
			wantStartDeletionNodes: []*apiv1.Node{
				buildReadyNode("n1", 1000, 10),
			},
			wantCandidateInfo: &candidateInfo{
				candidate: &defrag.Candidate{
					Nodes: []string{"n1", "n2"},
					Mode:  defrag.Partial,
				},
				creationTime: timeNow,
				defragPossibleMap: map[string]time.Time{
					"n1": timeNow.Add(-1 * time.Minute),
				},
			},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n2", 1000, 10): {pod3},
				buildReadyNode("n3", 1000, 10): {pod1, pod2},
			},
			wantCandidatePods: []*apiv1.Pod{pod3},
		},
		{
			name: "partial defrag, pods fit on upcoming node, but destination nodes must be ready",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 10):    {pod1, pod2},
				buildReadyNode("n2", 1000, 10):    {pod3},
				buildUpcomingNode("n3", 1000, 10): {},
			},
			allCandidateNodes: []string{"n1", "n2"},
			candidateInfo: &candidateInfo{
				candidate: &defrag.Candidate{
					Nodes: []string{"n1", "n2"},
					Mode:  defrag.Partial,
				},
				creationTime: timeNow,
			},
			wantStartDeletionCall: false,
			wantCandidateInfo: &candidateInfo{
				candidate: &defrag.Candidate{
					Nodes: []string{"n1", "n2"},
					Mode:  defrag.Partial,
				},
				creationTime:      timeNow,
				waitingForScaleUp: true,
				defragPossibleMap: make(map[string]time.Time),
			},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 10):    {pod1, pod2},
				buildReadyNode("n2", 1000, 10):    {pod3},
				buildUpcomingNode("n3", 1000, 10): {pod1, pod2},
			},
			wantCandidatePods: []*apiv1.Pod{pod3},
		},
		{
			name: "partial defrag, node removals take precedence over regular simulations",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 10): {pod1, pod3},
				buildReadyNode("n2", 1000, 10): {pod2},
				buildReadyNode("n3", 900, 10):  {},
			},
			allCandidateNodes: []string{"n1", "n2"},
			candidateInfo: &candidateInfo{
				candidate: &defrag.Candidate{
					Nodes: []string{"n1", "n2"},
					Mode:  defrag.Partial,
				},
				creationTime: timeNow,
				defragPossibleMap: map[string]time.Time{
					"n2": timeNow.Add(-1 * time.Minute),
				},
			},
			wantStartDeletionCall: true,
			wantStartDeletionNodes: []*apiv1.Node{
				buildReadyNode("n2", 1000, 10),
			},
			wantCandidateInfo: &candidateInfo{
				candidate: &defrag.Candidate{
					Nodes: []string{"n1", "n2"},
					Mode:  defrag.Partial,
				},
				creationTime: timeNow,
				defragPossibleMap: map[string]time.Time{
					"n2": timeNow.Add(-1 * time.Minute),
				},
			},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 10): {pod1, pod3},
				buildReadyNode("n3", 900, 10):  {pod2, pod1},
			},
			wantCandidatePods: []*apiv1.Pod{pod3},
		},
		{
			name: "partial defrag, multiple deletions",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 10): {pod1, pod2},
				buildReadyNode("n2", 1000, 10): {pod3},
				buildReadyNode("n3", 1000, 10): {pod4, pod5},
				buildReadyNode("n4", 2000, 10): {},
			},
			allCandidateNodes: []string{"n1", "n2", "n3"},
			candidateInfo: &candidateInfo{
				candidate: &defrag.Candidate{
					Nodes: []string{"n1", "n2", "n3"},
					Mode:  defrag.Partial,
				},
				creationTime: timeNow,
				defragPossibleMap: map[string]time.Time{
					"n1": timeNow.Add(-1 * time.Minute),
					"n2": timeNow.Add(-1 * time.Minute),
				},
			},
			wantStartDeletionCall: true,
			wantStartDeletionNodes: []*apiv1.Node{
				buildReadyNode("n1", 1000, 10),
				buildReadyNode("n2", 1000, 10),
			},
			wantCandidateInfo: &candidateInfo{
				candidate: &defrag.Candidate{
					Nodes: []string{"n1", "n2", "n3"},
					Mode:  defrag.Partial,
				},
				creationTime: timeNow,
				defragPossibleMap: map[string]time.Time{
					"n1": timeNow.Add(-1 * time.Minute),
					"n2": timeNow.Add(-1 * time.Minute),
				},
			},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n3", 1000, 10): {pod4, pod5},
				buildReadyNode("n4", 2000, 10): {pod1, pod2, pod3, pod4},
			},
			wantCandidatePods: []*apiv1.Pod{pod5},
		},
		{
			name: "partial defrag, node removable, but waiting for scale down delay",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 10): {pod1, pod2},
				buildReadyNode("n2", 1000, 10): {pod3},
				buildReadyNode("n3", 1000, 10): {},
			},
			allCandidateNodes: []string{"n1", "n2"},
			candidateInfo: &candidateInfo{
				candidate: &defrag.Candidate{
					Nodes: []string{"n1", "n2"},
					Mode:  defrag.Partial,
				},
				creationTime: timeNow,
			},
			wantCandidateInfo: &candidateInfo{
				candidate: &defrag.Candidate{
					Nodes: []string{"n1", "n2"},
					Mode:  defrag.Partial,
				},
				creationTime: timeNow,
				defragPossibleMap: map[string]time.Time{
					"n1": timeNow,
				},
			},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n2", 1000, 10): {pod3},
				buildReadyNode("n3", 1000, 10): {pod1, pod2},
			},
			wantCandidatePods: []*apiv1.Pod{pod3},
		},
		{
			name: "partial defrag, node was removable in previous iteration, but is no longer removable",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 10): {pod1, pod2},
				buildReadyNode("n2", 1000, 10): {pod3},
				buildReadyNode("n3", 500, 10):  {},
			},
			allCandidateNodes: []string{"n1", "n2"},
			candidateInfo: &candidateInfo{
				candidate: &defrag.Candidate{
					Nodes: []string{"n1", "n2"},
					Mode:  defrag.Partial,
				},
				creationTime: timeNow,
				defragPossibleMap: map[string]time.Time{
					"n1": timeNow,
				},
			},
			wantCandidateInfo: &candidateInfo{
				candidate: &defrag.Candidate{
					Nodes: []string{"n1", "n2"},
					Mode:  defrag.Partial,
				},
				creationTime:      timeNow,
				defragPossibleMap: make(map[string]time.Time),
			},
			wantNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("n1", 1000, 10): {pod1, pod2},
				buildReadyNode("n2", 1000, 10): {pod3},
				buildReadyNode("n3", 500, 10):  {pod1},
			},
			wantCandidatePods: []*apiv1.Pod{pod2, pod3},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			for node, pods := range tc.nodesWithPods {
				assert.NoError(t, snapshot.AddNodeInfo(framework.NewTestNodeInfo(node, pods...)))
			}

			scaleDownActuator := &mockScaleDownActuator{}
			scaleDownActuator.On("CheckStatus").Return(&fakeActuationStatus{})
			if tc.wantStartDeletionCall {
				scaledDownNodes := make([]*status.ScaleDownNode, len(tc.wantStartDeletionNodes))
				for i, node := range tc.wantStartDeletionNodes {
					scaledDownNodes[i] = &status.ScaleDownNode{Node: node}
				}
				scaleDownActuator.
					On("StartDeletion", []*apiv1.Node(nil), tc.wantStartDeletionNodes).
					Return(status.ScaleDownNodeDeleteStarted, scaledDownNodes, tc.wantStartDeletionError).
					Once()
				plugin := mockPluginBuilder{}.build()
				tc.candidateInfo.candidate.Plugin = plugin
				tc.wantCandidateInfo.candidate.Plugin = plugin
			}

			deleteOpts := options.NodeDeleteOptions{}
			fakeClock := &FakePassiveClock{}
			fakeClock.SetTime(timeNow)
			processor := NewProcessor(Options{
				DeleteOptions:     deleteOpts,
				DrainabilityRules: rules.Default(deleteOpts),
				Config:            Config{ScaleDownDelay: time.Minute},
				Clock:             fakeClock,
			})
			processor.candidateInfos = []*candidateInfo{tc.candidateInfo}
			processor.ctx = &cacontext.AutoscalingContext{
				ClusterSnapshot:     snapshot,
				ScaleDownActuator:   scaleDownActuator,
				RemainingPdbTracker: pdb.NewBasicRemainingPdbTracker(),
			}
			if tc.isScaledDown {
				for _, nodeName := range tc.candidateInfo.candidate.Nodes {
					processor.actuator.scaledDownNodes[nodeName] = timeNow
				}
			}

			allCandidateNodes := make(map[string]bool)
			for _, node := range tc.allCandidateNodes {
				allCandidateNodes[node] = true
			}
			pods, err := processor.processCandidate(tc.candidateInfo, allCandidateNodes)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.ElementsMatch(t, tc.wantCandidatePods, pods)
				assert.NoError(t, err)
			}
			assert.Equal(t, []*candidateInfo{tc.wantCandidateInfo}, processor.candidateInfos)
			if tc.wantStartDeletionCall {
				scaleDownActuator.AssertNumberOfCalls(t, "StartDeletion", 1)
			} else {
				scaleDownActuator.AssertNumberOfCalls(t, "StartDeletion", 0)
			}
			for node, wantPods := range tc.wantNodesWithPods {
				nodeInfo, err := snapshot.GetNodeInfo(node.Name)
				assert.NoError(t, err)
				assert.Equal(t, node, nodeInfo.Node())
				var pods []*apiv1.Pod
				for _, podInfo := range nodeInfo.Pods() {
					pods = append(pods, podInfo.Pod)
				}
				assert.ElementsMatch(t, wantPods, pods)
			}
		})
	}
}

func TestNewCandidate(t *testing.T) {
	allNodesProcessor := &mockScaleDownNodeProcessor{
		candidatesFilter: func(nodes []*apiv1.Node) []*apiv1.Node {
			return nodes
		},
	}

	noCandidatePlugin := &mockPlugin{}
	noCandidatePlugin.On("NewCandidate", mock.Anything, mock.Anything).Return(nil)
	noCandidatePlugin.On("LatestUnfitNodesCount").Return(0)

	specialPlugin := mockPluginBuilder{
		targetNodeName: "special",
		mode:           defrag.CreateBeforeDelete,
	}.build()
	otherPlugin := mockPluginBuilder{
		targetNodeName: "other",
		mode:           defrag.CreateBeforeDelete,
	}.build()

	timeNow := time.Now()

	testCases := []struct {
		name              string
		plugins           []defrag.Plugin
		allNodesWithPods  map[*apiv1.Node][]*apiv1.Pod
		allCandidateNodes []string
		partialEnabled    bool
		minNodeGroupSize  int
		wantCandidateInfo *candidateInfo
	}{
		{
			name:    "no nodes",
			plugins: []defrag.Plugin{noCandidatePlugin, specialPlugin, otherPlugin},
		},
		{
			name: "no plugins",
			allNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 1000, 1): {},
				buildReadyNode("other", 1000, 1):   {},
			},
		},
		{
			name:    "plugin does not produce candidate",
			plugins: []defrag.Plugin{noCandidatePlugin},
			allNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 1000, 1): {},
				buildReadyNode("other", 1000, 1):   {},
			},
		},
		{
			name:    "plugin produces a candidate",
			plugins: []defrag.Plugin{specialPlugin},
			allNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 1000, 1): {},
				buildReadyNode("other", 1000, 1):   {},
			},
			wantCandidateInfo: &candidateInfo{
				candidate: &defrag.Candidate{
					Nodes:  []string{"special"},
					Plugin: specialPlugin,
				},
				creationTime: timeNow,
			},
		},
		{
			name: "skip valid node to maintain the node group size",
			plugins: []defrag.Plugin{
				&fakePlugin{
					targetNodes: []string{"node-1", "node-2"},
					mode:        defrag.CreateBeforeDelete,
				},
			},
			allNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("node-1", 1000, 1): {},
				buildReadyNode("node-2", 1000, 1): {},
			},
			minNodeGroupSize: 1,
			wantCandidateInfo: &candidateInfo{
				candidate: &defrag.Candidate{
					Nodes: []string{"node-1"},
					Plugin: &fakePlugin{
						targetNodes: []string{"node-1", "node-2"},
						mode:        defrag.CreateBeforeDelete,
					},
				},
				creationTime: timeNow,
			},
		},
		{
			name:    "one plugin does not produce, another produces a candidate",
			plugins: []defrag.Plugin{noCandidatePlugin, specialPlugin},
			allNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 1000, 1): {},
				buildReadyNode("other", 1000, 1):   {},
			},
			wantCandidateInfo: &candidateInfo{
				candidate: &defrag.Candidate{
					Nodes:  []string{"special"},
					Plugin: specialPlugin,
				},
				creationTime: timeNow,
			},
		},
		{
			name:    "multiple plugins produce a candidate",
			plugins: []defrag.Plugin{otherPlugin, specialPlugin},
			allNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 1000, 1): {},
				buildReadyNode("other", 1000, 1):   {},
			},
			wantCandidateInfo: &candidateInfo{
				candidate: &defrag.Candidate{
					Nodes:  []string{"other"},
					Plugin: otherPlugin,
				},
				creationTime: timeNow,
			},
		},
		{
			name:    "regular plugin produces no candidate with blocking pods",
			plugins: []defrag.Plugin{specialPlugin},
			allNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("special", 1000, 1): {
					test.BuildScheduledTestPod("p", 100, 1, "n"),
				},
				buildReadyNode("other", 1000, 1): {},
			},
		},
		{
			name: "partial plugin gets reverted to CreateBeforeDelete and yields only one candidate node if partial is not enabled",
			plugins: []defrag.Plugin{
				&fakePlugin{
					targetNodes: []string{"partial-1", "partial-2"},
					mode:        defrag.Partial,
				},
			},
			allNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("other", 1000, 1):     {},
				buildReadyNode("partial-1", 1000, 1): {},
				buildReadyNode("partial-2", 1000, 1): {},
			},
			wantCandidateInfo: &candidateInfo{
				candidate: &defrag.Candidate{
					Nodes: []string{"partial-1"},
					Plugin: &fakePlugin{
						targetNodes: []string{"partial-1", "partial-2"},
						mode:        defrag.Partial,
					},
					Mode: defrag.CreateBeforeDelete,
				},
				creationTime: timeNow,
			},
		},
		{
			name: "partial plugin produces partial candidate if partial is enabled",
			plugins: []defrag.Plugin{
				&fakePlugin{
					targetNodes: []string{"partial"},
					mode:        defrag.Partial,
				},
			},
			allNodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				buildReadyNode("partial", 1000, 1): {},
			},
			partialEnabled: true,
			wantCandidateInfo: &candidateInfo{
				candidate: &defrag.Candidate{
					Nodes: []string{"partial"},
					Plugin: &fakePlugin{
						targetNodes: []string{"partial"},
						mode:        defrag.Partial,
					},
					Mode: defrag.Partial,
				},
				creationTime: timeNow,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := testprovider.NewTestCloudProviderBuilder().Build()
			provider.AddNodeGroup("ng1", tc.minNodeGroupSize, 1000, len(tc.allNodesWithPods))

			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			client := fake.NewClientset()
			for node, pods := range tc.allNodesWithPods {
				provider.AddNode("ng1", node)
				assert.NoError(t, snapshot.AddNodeInfo(framework.NewTestNodeInfo(node, pods...)))
				_, err := client.CoreV1().Nodes().Create(context.TODO(), node, metav1.CreateOptions{})
				assert.NoError(t, err)
			}

			deleteOpts := options.NodeDeleteOptions{}
			fakeClock := &FakePassiveClock{}
			fakeClock.SetTime(timeNow)
			var experimentsManager experiments.Manager
			if tc.partialEnabled {
				experimentsManager = experiments.NewMockManager(experiments.EnablePartialDefragFlag)
			}
			processor := NewProcessor(Options{
				ScaleDownNodeProcessor: allNodesProcessor,
				DeleteOptions:          deleteOpts,
				DrainabilityRules:      rules.Default(deleteOpts),
				Plugins:                tc.plugins,
				Clock:                  fakeClock,
				ExperimentsManager:     experimentsManager,
			})
			scaleDownActuator := &mockScaleDownActuator{}
			scaleDownActuator.On("CheckStatus").Return(&fakeActuationStatus{})
			processor.ctx = &cacontext.AutoscalingContext{
				ClusterSnapshot:     snapshot,
				CloudProvider:       provider,
				ScaleDownActuator:   scaleDownActuator,
				RemainingPdbTracker: pdb.NewBasicRemainingPdbTracker(),
				AutoscalingKubeClients: cacontext.AutoscalingKubeClients{
					ClientSet: client,
				},
			}
			nodeFilter, err := processor.nodeFilterFactory.NewDefragNodeFilter(processor.ctx)
			assert.NoError(t, err)

			allCandidateNodes := make(map[string]bool)
			for _, node := range tc.allCandidateNodes {
				allCandidateNodes[node] = true
			}
			candidateInfo, err := processor.newCandidate(nodeFilter, allCandidateNodes)
			assert.Equal(t, tc.wantCandidateInfo, candidateInfo)
			assert.NoError(t, err)

			if tc.wantCandidateInfo != nil {
				for _, nodeName := range candidateInfo.candidate.Nodes {
					info, err := snapshot.GetNodeInfo(nodeName)
					assert.NoError(t, err)
					node := info.Node()
					assert.True(t, taints.HasTaint(node, defrag.HardTaint))
					assert.True(t, taints.HasTaint(node, defrag.SoftTaint))
				}
			}
		})
	}
}

type mockPluginBuilder struct {
	targetNodeName string
	mode           defrag.Mode
}

func (b mockPluginBuilder) build() *mockPlugin {
	containsNode := func(nodeNames []string) bool {
		for _, nodeName := range nodeNames {
			if nodeName == b.targetNodeName {
				return true
			}
		}
		return false
	}
	notContainsNode := func(nodeNames []string) bool { return !containsNode(nodeNames) }

	plugin := &mockPlugin{}
	plugin.On("NewCandidate", mock.Anything, mock.MatchedBy(containsNode)).
		Return(&defrag.Candidate{Nodes: []string{b.targetNodeName}, Mode: b.mode})
	plugin.On("NewCandidate", mock.Anything, mock.MatchedBy(notContainsNode)).
		Return(nil)
	plugin.On("LatestUnfitNodesCount").Return(0)

	return plugin
}

func setDefragTaints(node *apiv1.Node) *apiv1.Node {
	node.Spec.Taints = append(node.Spec.Taints, apiv1.Taint{
		Key:    defrag.HardTaint,
		Effect: apiv1.TaintEffectNoSchedule,
	}, apiv1.Taint{
		Key:    defrag.SoftTaint,
		Effect: apiv1.TaintEffectPreferNoSchedule,
	})
	return node
}

func TestFilterExpansionOptions(t *testing.T) {
	cheapOption := expander.Option{Debug: "cheap"}
	expensiveOption := expander.Option{Debug: "expensive"}

	timeNow := time.Now()

	testCases := []struct {
		name              string
		initPlugin        func() *mockPlugin
		options           []expander.Option
		wantOptions       []expander.Option
		wantCandidateInfo *candidateInfo
	}{
		{
			name: "No options",
			initPlugin: func() *mockPlugin {
				return &mockPlugin{}
			},
			wantCandidateInfo: &candidateInfo{creationTime: timeNow, scaleUpNoOptions: true},
		},
		{
			name: "Plugin likes all options",
			initPlugin: func() *mockPlugin {
				plugin := &mockPlugin{}
				plugin.On("IsExpansionOptionValid", mock.Anything, mock.Anything, cheapOption).Return(true).Once()
				plugin.On("IsExpansionOptionValid", mock.Anything, mock.Anything, expensiveOption).Return(true).Once()
				return plugin
			},
			options:           []expander.Option{cheapOption, expensiveOption},
			wantOptions:       []expander.Option{cheapOption, expensiveOption},
			wantCandidateInfo: &candidateInfo{creationTime: timeNow},
		},
		{
			name: "Plugin does not like expensive option",
			initPlugin: func() *mockPlugin {
				plugin := &mockPlugin{}
				plugin.On("IsExpansionOptionValid", mock.Anything, mock.Anything, cheapOption).Return(true).Once()
				plugin.On("IsExpansionOptionValid", mock.Anything, mock.Anything, expensiveOption).Return(false).Once()
				return plugin
			},
			options:           []expander.Option{cheapOption, expensiveOption},
			wantOptions:       []expander.Option{cheapOption},
			wantCandidateInfo: &candidateInfo{creationTime: timeNow},
		},
		{
			name: "Plugin does not like any option",
			initPlugin: func() *mockPlugin {
				plugin := &mockPlugin{}
				plugin.On("IsExpansionOptionValid", mock.Anything, mock.Anything, cheapOption).Return(false).Once()
				plugin.On("IsExpansionOptionValid", mock.Anything, mock.Anything, expensiveOption).Return(false).Once()
				return plugin
			},
			options:           []expander.Option{cheapOption, expensiveOption},
			wantCandidateInfo: &candidateInfo{creationTime: timeNow, scaleUpNoOptions: true},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			plugin := tc.initPlugin()
			deleteOpts := options.NodeDeleteOptions{}
			processor := NewProcessor(Options{
				DeleteOptions:     deleteOpts,
				DrainabilityRules: rules.Default(deleteOpts),
				Plugins:           []defrag.Plugin{plugin},
			})
			tc.wantCandidateInfo.candidate = &defrag.Candidate{Plugin: plugin}
			processor.pickedCandidateInfo = tc.wantCandidateInfo
			assert.ElementsMatch(t, tc.wantOptions, processor.BestOptions(tc.options, nil))
			assert.Equal(t, tc.wantCandidateInfo, processor.pickedCandidateInfo)
		})
	}
}

type mockPlugin struct {
	mock.Mock
}

func (m *mockPlugin) LatestUnfitNodesCount() int {
	args := m.Called()
	return args.Get(0).(int)
}

func (m *mockPlugin) NewCandidate(ctx *cacontext.AutoscalingContext, nodeNames []string) *defrag.Candidate {
	args := m.Called(ctx, nodeNames)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*defrag.Candidate)
}

func (m *mockPlugin) ValidCandidateNodes(ctx *cacontext.AutoscalingContext, nodeNames []string) []string {
	args := m.Called(ctx, nodeNames)
	return args.Get(0).([]string)
}

func (m *mockPlugin) IsExpansionOptionValid(ctx *cacontext.AutoscalingContext, candidate *defrag.Candidate, option expander.Option) bool {
	args := m.Called(ctx, candidate, option)
	return args.Get(0).(bool)
}

func (m *mockPlugin) BackoffDuration(ctx *cacontext.AutoscalingContext, candidate *defrag.Candidate) time.Duration {
	args := m.Called(ctx, candidate)
	return args.Get(0).(time.Duration)
}

func (m *mockPlugin) Type() defrag.PluginType {
	args := m.Called()
	return args.Get(0).(defrag.PluginType)
}

type fakePlugin struct {
	mode            defrag.Mode
	targetNodes     []string
	validFilter     func(string) bool
	backoffDuration time.Duration
}

func (f fakePlugin) String() string {
	return "fake"
}

func (f fakePlugin) Type() defrag.PluginType {
	return defrag.StandardPluginType
}

func (f fakePlugin) NewCandidate(ctx *cacontext.AutoscalingContext, nodeNames []string) *defrag.Candidate {
	var candidateNodes []string
	// keep order of nodes in targetNodes to avoid flaky tests, as nodes in the snapshot
	// are stored in a map.
	for _, node := range f.targetNodes {
		if slices.Contains(nodeNames, node) {
			candidateNodes = append(candidateNodes, node)
		}
	}
	return &defrag.Candidate{
		Plugin: f,
		Nodes:  candidateNodes,
		Mode:   f.mode,
	}
}

func (f fakePlugin) ValidCandidateNodes(ctx *cacontext.AutoscalingContext, nodeNames []string) []string {
	var candidateNodes []string
	for _, nodeName := range nodeNames {
		if f.validFilter == nil || f.validFilter(nodeName) {
			candidateNodes = append(candidateNodes, nodeName)
		}
	}
	return candidateNodes
}

func (f fakePlugin) IsExpansionOptionValid(ctx *cacontext.AutoscalingContext, candidate *defrag.Candidate, option expander.Option) bool {
	return true
}

func (f fakePlugin) BackoffDuration(ctx *cacontext.AutoscalingContext, candidate *defrag.Candidate) time.Duration {
	return f.backoffDuration
}

func (f fakePlugin) LatestUnfitNodesCount() int {
	return 0
}

func TestCleanPickedCandidate(t *testing.T) {
	testCases := []struct {
		name  string
		admit bool
	}{
		{
			name:  "defrag admitted",
			admit: true,
		},
		{
			name:  "defrag not admitted",
			admit: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			deleteOpts := options.NodeDeleteOptions{}
			config := Config{MaxDelay: time.Hour}
			allNodesProcessor := &mockScaleDownNodeProcessor{
				candidatesFilter: func(nodes []*apiv1.Node) []*apiv1.Node {
					return nodes
				},
			}
			provider := testprovider.NewTestCloudProviderBuilder().Build()
			processor := NewProcessor(Options{
				ScaleDownNodeProcessor: allNodesProcessor,
				DeleteOptions:          deleteOpts,
				DrainabilityRules:      rules.Default(deleteOpts),
				Config:                 config,
			})
			processor.pickedCandidateInfo = &candidateInfo{}
			maxLoopsBeforeAdmission := 5
			sharedFairnessManager := fairness.NewSharedEnforcerManager(maxLoopsBeforeAdmission)
			processor.fairnessEnforcer = sharedFairnessManager.CreateEnforcer(DefragProcessorName)
			processor.fairnessEnforcer.Admit(nil) // sets previously admitted.

			// Use empty/non-empty unschedulablePods to force admission
			var unschedulablePods []*apiv1.Pod
			if !tc.admit {
				unschedulablePods = append(unschedulablePods, &apiv1.Pod{})
			}
			assert.Equal(t, tc.admit, processor.fairnessEnforcer.Admit(unschedulablePods))

			ctx := &cacontext.AutoscalingContext{
				ClusterSnapshot:     testsnapshot.NewTestSnapshotOrDie(t),
				RemainingPdbTracker: pdb.NewBasicRemainingPdbTracker(),
				CloudProvider:       provider,
			}

			pods, err := processor.Process(ctx, unschedulablePods)
			assert.Equal(t, pods, unschedulablePods)
			assert.NoError(t, err)

			assert.Nil(t, processor.pickedCandidateInfo)
		})
	}
}

func TestPartialEnabled(t *testing.T) {

	tests := []struct {
		name               string
		experimentsManager experiments.Manager
		want               bool
	}{
		{
			name:               "experimentsManager is nil",
			experimentsManager: nil,
			want:               false,
		},
		{
			name:               "flag disabled",
			experimentsManager: experiments.NewMockManager(),
			want:               false,
		},
		{
			name:               "flag enabled",
			experimentsManager: experiments.NewMockManager(experiments.EnablePartialDefragFlag),
			want:               true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &Processor{
				experimentsManager: tc.experimentsManager,
			}
			got := p.partialEnabled()
			if got != tc.want {
				t.Errorf("partialAllowed() = %v, want %v", got, tc.want)
			}
		})
	}
}
