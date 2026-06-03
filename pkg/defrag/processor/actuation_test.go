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

	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/policy/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/pdb"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/status"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	caerrors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	. "k8s.io/utils/clock/testing"
)

func TestStartScaleDown(t *testing.T) {
	timeNow := time.Now()

	pdbPod1 := setPodLabel(test.BuildTestPod("p1", 100, 1), "pdb-1")
	pdbPod2 := setPodLabel(test.BuildTestPod("p2", 100, 1), "pdb-2")
	pdbPod3 := setPodLabel(test.BuildTestPod("p3", 100, 1), "pdb-3")

	testCases := []struct {
		name            string
		pdbs            []*v1.PodDisruptionBudget
		nodesWithPods   map[*apiv1.Node][]*apiv1.Pod
		candidate       *defrag.Candidate
		scaledDownNodes []string

		isScaleDownStatusProcessorSet bool

		wantStartDeletionCall   bool
		wantStartDeletionNodes  []*apiv1.Node
		wantStartDeletionStatus *status.ScaleDownStatus
		wantStartDeletionError  error

		wantPdbs                        []*v1.PodDisruptionBudget
		wantScaledDownNodes             []string
		wantCurrentCycleScaledDownNodes []string
		wantError                       bool
	}{
		{
			name: "Empty candidate, no nodes in cluster",
			pdbs: []*v1.PodDisruptionBudget{
				buildPdb("pdb-1", 2),
				buildPdb("pdb-2", 2),
			},
			candidate:                     &defrag.Candidate{},
			isScaleDownStatusProcessorSet: true,
			wantStartDeletionCall:         true,
			wantStartDeletionStatus:       &status.ScaleDownStatus{},
			wantPdbs: []*v1.PodDisruptionBudget{
				buildPdb("pdb-1", 2),
				buildPdb("pdb-2", 2),
			},
		},
		{
			name: "Empty candidate, some nodes in cluster",
			pdbs: []*v1.PodDisruptionBudget{
				buildPdb("pdb-1", 2),
				buildPdb("pdb-2", 2),
			},
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 100, 1): {pdbPod1, pdbPod2},
				test.BuildTestNode("n2", 100, 1): {pdbPod1, pdbPod3},
				test.BuildTestNode("n3", 100, 1): {pdbPod2, pdbPod3},
			},
			candidate:                     &defrag.Candidate{},
			isScaleDownStatusProcessorSet: true,
			wantStartDeletionCall:         true,
			wantStartDeletionStatus:       &status.ScaleDownStatus{},
			wantPdbs: []*v1.PodDisruptionBudget{
				buildPdb("pdb-1", 2),
				buildPdb("pdb-2", 2),
			},
		},
		{
			name: "Candidate with a single node",
			pdbs: []*v1.PodDisruptionBudget{
				buildPdb("pdb-1", 2),
				buildPdb("pdb-2", 2),
			},
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 100, 1): {pdbPod1, pdbPod2},
				test.BuildTestNode("n2", 100, 1): {pdbPod1, pdbPod3},
				test.BuildTestNode("n3", 100, 1): {pdbPod2, pdbPod3},
			},
			candidate:                     &defrag.Candidate{Nodes: []string{"n1"}},
			isScaleDownStatusProcessorSet: true,
			wantStartDeletionCall:         true,
			wantStartDeletionNodes: []*apiv1.Node{
				test.BuildTestNode("n1", 100, 1),
			},

			wantStartDeletionStatus: &status.ScaleDownStatus{
				Result: status.ScaleDownNodeDeleteStarted,
				ScaledDownNodes: []*status.ScaleDownNode{
					{Node: test.BuildTestNode("n1", 100, 1)},
				}},
			wantPdbs: []*v1.PodDisruptionBudget{
				buildPdb("pdb-1", 1),
				buildPdb("pdb-2", 1),
			},
			wantScaledDownNodes:             []string{"n1"},
			wantCurrentCycleScaledDownNodes: []string{"n1"},
		},
		{
			name: "Candidate with multiple nodes, all nodes scale-down",
			pdbs: []*v1.PodDisruptionBudget{
				buildPdb("pdb-1", 2),
				buildPdb("pdb-2", 2),
			},
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 100, 1): {pdbPod1, pdbPod2},
				test.BuildTestNode("n2", 100, 1): {pdbPod1, pdbPod3},
				test.BuildTestNode("n3", 100, 1): {pdbPod2, pdbPod3},
			},
			candidate:                     &defrag.Candidate{Nodes: []string{"n1", "n2", "n3"}},
			isScaleDownStatusProcessorSet: true,
			wantStartDeletionCall:         true,
			wantStartDeletionNodes: []*apiv1.Node{
				test.BuildTestNode("n1", 100, 1),
				test.BuildTestNode("n2", 100, 1),
				test.BuildTestNode("n3", 100, 1),
			},
			wantStartDeletionStatus: &status.ScaleDownStatus{ScaledDownNodes: []*status.ScaleDownNode{
				{Node: test.BuildTestNode("n1", 100, 1)},
				{Node: test.BuildTestNode("n2", 100, 1)},
				{Node: test.BuildTestNode("n3", 100, 1)},
			}},
			wantPdbs: []*v1.PodDisruptionBudget{
				buildPdb("pdb-1", 0),
				buildPdb("pdb-2", 0),
			},
			wantScaledDownNodes:             []string{"n1", "n2", "n3"},
			wantCurrentCycleScaledDownNodes: []string{"n1", "n2", "n3"},
		},
		{
			name: "Candidate with multiple nodes, some nodes scale-down",
			pdbs: []*v1.PodDisruptionBudget{
				buildPdb("pdb-1", 2),
				buildPdb("pdb-2", 2),
			},
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 100, 1): {pdbPod1, pdbPod2},
				test.BuildTestNode("n2", 100, 1): {pdbPod1, pdbPod3},
				test.BuildTestNode("n3", 100, 1): {pdbPod2, pdbPod3},
			},
			candidate:                     &defrag.Candidate{Nodes: []string{"n1", "n2", "n3"}},
			isScaleDownStatusProcessorSet: true,
			wantStartDeletionCall:         true,
			wantStartDeletionNodes: []*apiv1.Node{
				test.BuildTestNode("n1", 100, 1),
				test.BuildTestNode("n2", 100, 1),
				test.BuildTestNode("n3", 100, 1),
			},
			wantStartDeletionStatus: &status.ScaleDownStatus{ScaledDownNodes: []*status.ScaleDownNode{
				{Node: test.BuildTestNode("n1", 100, 1)},
				{Node: test.BuildTestNode("n2", 100, 1)},
			}},
			wantPdbs: []*v1.PodDisruptionBudget{
				buildPdb("pdb-1", 0),
				buildPdb("pdb-2", 1),
			},
			wantScaledDownNodes:             []string{"n1", "n2"},
			wantCurrentCycleScaledDownNodes: []string{"n1", "n2"},
		},
		{
			name: "Candidate with multiple nodes, no nodes scale-down",
			pdbs: []*v1.PodDisruptionBudget{
				buildPdb("pdb-1", 2),
				buildPdb("pdb-2", 2),
			},
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 100, 1): {pdbPod1, pdbPod2},
				test.BuildTestNode("n2", 100, 1): {pdbPod1, pdbPod3},
				test.BuildTestNode("n3", 100, 1): {pdbPod2, pdbPod3},
			},
			candidate:                     &defrag.Candidate{Nodes: []string{"n1", "n2", "n3"}},
			isScaleDownStatusProcessorSet: true,
			wantStartDeletionCall:         true,
			wantStartDeletionNodes: []*apiv1.Node{
				test.BuildTestNode("n1", 100, 1),
				test.BuildTestNode("n2", 100, 1),
				test.BuildTestNode("n3", 100, 1),
			},
			wantStartDeletionStatus: &status.ScaleDownStatus{ScaledDownNodes: []*status.ScaleDownNode{}},
			wantPdbs: []*v1.PodDisruptionBudget{
				buildPdb("pdb-1", 2),
				buildPdb("pdb-2", 2),
			},
		},
		{
			name: "Candidate with multiple nodes, some nodes already scaled-down",
			pdbs: []*v1.PodDisruptionBudget{
				buildPdb("pdb-1", 1),
				buildPdb("pdb-2", 1),
			},
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 100, 1): {pdbPod1, pdbPod2},
				test.BuildTestNode("n2", 100, 1): {pdbPod1, pdbPod3},
				test.BuildTestNode("n3", 100, 1): {pdbPod2, pdbPod3},
			},
			candidate:                     &defrag.Candidate{Nodes: []string{"n1", "n2", "n3"}},
			scaledDownNodes:               []string{"n1"},
			isScaleDownStatusProcessorSet: true,
			wantStartDeletionCall:         true,
			wantStartDeletionNodes: []*apiv1.Node{
				test.BuildTestNode("n2", 100, 1),
				test.BuildTestNode("n3", 100, 1),
			},
			wantStartDeletionStatus: &status.ScaleDownStatus{ScaledDownNodes: []*status.ScaleDownNode{
				{Node: test.BuildTestNode("n2", 100, 1)},
			}},
			wantPdbs: []*v1.PodDisruptionBudget{
				buildPdb("pdb-1", 0),
				buildPdb("pdb-2", 1),
			},
			wantScaledDownNodes:             []string{"n1", "n2"},
			wantCurrentCycleScaledDownNodes: []string{"n2"},
		},
		{
			name: "ScaleDownStatusProcessor not set",
			pdbs: []*v1.PodDisruptionBudget{
				buildPdb("pdb-1", 1),
				buildPdb("pdb-2", 1),
			},
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 100, 1): {pdbPod1, pdbPod2},
				test.BuildTestNode("n2", 100, 1): {pdbPod1, pdbPod3},
				test.BuildTestNode("n3", 100, 1): {pdbPod2, pdbPod3},
			},
			candidate:                     &defrag.Candidate{Nodes: []string{"n1", "n2", "n3"}},
			scaledDownNodes:               []string{"n1"},
			isScaleDownStatusProcessorSet: false,
			wantStartDeletionCall:         true,
			wantStartDeletionNodes: []*apiv1.Node{
				test.BuildTestNode("n2", 100, 1),
				test.BuildTestNode("n3", 100, 1),
			},
			wantStartDeletionStatus: &status.ScaleDownStatus{ScaledDownNodes: []*status.ScaleDownNode{
				{Node: test.BuildTestNode("n2", 100, 1)},
			}},
			wantPdbs: []*v1.PodDisruptionBudget{
				buildPdb("pdb-1", 0),
				buildPdb("pdb-2", 1),
			},
			wantScaledDownNodes:             []string{"n1", "n2"},
			wantCurrentCycleScaledDownNodes: []string{"n2"},
		},
		{
			name: "Candidate with non existing node (should never happen)",
			pdbs: []*v1.PodDisruptionBudget{
				buildPdb("pdb-1", 2),
				buildPdb("pdb-2", 2),
			},
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 100, 1): {pdbPod1, pdbPod2},
				test.BuildTestNode("n2", 100, 1): {pdbPod1, pdbPod3},
				test.BuildTestNode("n3", 100, 1): {pdbPod2, pdbPod3},
			},
			candidate:                     &defrag.Candidate{Nodes: []string{"n4"}},
			wantStartDeletionStatus:       &status.ScaleDownStatus{},
			isScaleDownStatusProcessorSet: true,
			wantPdbs: []*v1.PodDisruptionBudget{
				buildPdb("pdb-1", 2),
				buildPdb("pdb-2", 2),
			},
			wantError: true,
		},
		{
			name: "StartDeletion error",
			pdbs: []*v1.PodDisruptionBudget{
				buildPdb("pdb-1", 2),
				buildPdb("pdb-2", 2),
			},
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 100, 1): {pdbPod1, pdbPod2},
				test.BuildTestNode("n2", 100, 1): {pdbPod1, pdbPod3},
				test.BuildTestNode("n3", 100, 1): {pdbPod2, pdbPod3},
			},
			candidate:                     &defrag.Candidate{Nodes: []string{"n1", "n2", "n3"}},
			wantStartDeletionStatus:       &status.ScaleDownStatus{},
			isScaleDownStatusProcessorSet: true,
			wantStartDeletionCall:         true,
			wantStartDeletionNodes: []*apiv1.Node{
				test.BuildTestNode("n1", 100, 1),
				test.BuildTestNode("n2", 100, 1),
				test.BuildTestNode("n3", 100, 1),
			},
			wantStartDeletionError: caerrors.NewAutoscalerError("type", "msg"),
			wantPdbs: []*v1.PodDisruptionBudget{
				buildPdb("pdb-1", 2),
				buildPdb("pdb-2", 2),
			},
			wantError: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scaleDownActuator := &mockScaleDownActuator{}
			var scaleDownStatusProcessor *mockScaleDownStatusProcessor
			if tc.isScaleDownStatusProcessorSet {
				scaleDownStatusProcessor = &mockScaleDownStatusProcessor{}
			}
			if tc.wantStartDeletionCall {
				scaleDownActuator.On("StartDeletion", []*apiv1.Node(nil), tc.wantStartDeletionNodes).Return(
					tc.wantStartDeletionStatus.Result,
					tc.wantStartDeletionStatus.ScaledDownNodes,
					tc.wantStartDeletionError,
				).Once()
				if scaleDownStatusProcessor != nil && tc.wantStartDeletionStatus != nil && tc.wantStartDeletionStatus.Result == status.ScaleDownNodeDeleteStarted {
					scaleDownStatusProcessor.On("Process", mock.Anything, tc.wantStartDeletionStatus).Once()
				}
			}
			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			for node, pods := range tc.nodesWithPods {
				assert.NoError(t, snapshot.AddNodeInfo(framework.NewTestNodeInfo(node, pods...)))
			}
			pdbTracker := pdb.NewBasicRemainingPdbTracker()
			assert.NoError(t, pdbTracker.SetPdbs(tc.pdbs))
			ctx := &context.AutoscalingContext{
				ClusterSnapshot:     snapshot,
				ScaleDownActuator:   scaleDownActuator,
				RemainingPdbTracker: pdbTracker,
			}
			fakeClock := &FakePassiveClock{}
			fakeClock.SetTime(timeNow)
			defragActuator := newDefragActuator(actuatorOptions{
				ScaleDownStatusProcessor: scaleDownStatusProcessor,
				Clock:                    fakeClock,
			})
			for _, nodeName := range tc.scaledDownNodes {
				defragActuator.scaledDownNodes[nodeName] = timeNow
			}
			tc.candidate.Plugin = mockPluginBuilder{}.build()
			currentCycleScaledDownNodes, err := defragActuator.startScaleDown(ctx, tc.candidate, timeNow)
			if tc.wantStartDeletionCall {
				scaleDownActuator.AssertNumberOfCalls(t, "StartDeletion", 1)
				if scaleDownStatusProcessor != nil && tc.wantStartDeletionStatus != nil && tc.wantStartDeletionStatus.Result == status.ScaleDownNodeDeleteStarted {
					scaleDownStatusProcessor.AssertNumberOfCalls(t, "Process", 1)
				}
			}
			if tc.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.ElementsMatch(t, tc.wantPdbs, ctx.RemainingPdbTracker.GetPdbs())
			var scaledDownNodes []string
			for nodeName := range defragActuator.scaledDownNodes {
				scaledDownNodes = append(scaledDownNodes, nodeName)
			}
			assert.ElementsMatch(t, scaledDownNodes, tc.wantScaledDownNodes)
			assert.ElementsMatch(t, currentCycleScaledDownNodes, tc.wantCurrentCycleScaledDownNodes)
		})
	}
}

func TestIsScaleDownStarted(t *testing.T) {
	testCases := []struct {
		name            string
		scaledDownNodes map[string]time.Time
		candidate       *defrag.Candidate
		wantStarted     bool
	}{
		{
			name:            "candidate with no nodes",
			scaledDownNodes: map[string]time.Time{},
			candidate:       &defrag.Candidate{},
			wantStarted:     true,
		},
		{
			name:            "candidate with a single non-scaled down node",
			scaledDownNodes: map[string]time.Time{},
			candidate:       &defrag.Candidate{Nodes: []string{"n"}},
			wantStarted:     false,
		},
		{
			name: "candidate with a single scaled down node",
			scaledDownNodes: map[string]time.Time{
				"n": time.Now(),
			},
			candidate:   &defrag.Candidate{Nodes: []string{"n"}},
			wantStarted: true,
		},
		{
			name: "candidate multiple nodes, all scaled down",
			scaledDownNodes: map[string]time.Time{
				"n1": time.Now(),
				"n2": time.Now(),
				"n3": time.Now(),
			},
			candidate:   &defrag.Candidate{Nodes: []string{"n1", "n2", "n3"}},
			wantStarted: true,
		},
		{
			name: "candidate multiple nodes, some scaled down",
			scaledDownNodes: map[string]time.Time{
				"n1": time.Now(),
				"n3": time.Now(),
			},
			candidate:   &defrag.Candidate{Nodes: []string{"n1", "n2", "n3"}},
			wantStarted: false,
		},
		{
			name:            "candidate multiple nodes, none scaled down",
			scaledDownNodes: map[string]time.Time{},
			candidate:       &defrag.Candidate{Nodes: []string{"n1", "n2", "n3"}},
			wantStarted:     false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defragActuator := newDefragActuator(actuatorOptions{})
			defragActuator.scaledDownNodes = tc.scaledDownNodes
			assert.Equal(t, tc.wantStarted, defragActuator.isScaleDownFullyStarted(tc.candidate))
		})
	}
}

func TestIsScaleDownTimedOut(t *testing.T) {
	timedOut := time.Now().Add(-5 * time.Minute)

	testCases := []struct {
		name            string
		scaledDownNodes map[string]time.Time
		candidate       *defrag.Candidate
		wantTimedOut    bool
	}{
		{
			name:            "candidate with no nodes",
			scaledDownNodes: map[string]time.Time{},
			candidate:       &defrag.Candidate{},
			wantTimedOut:    false,
		},
		{
			name:            "candidate with a single non-scaled down node",
			scaledDownNodes: map[string]time.Time{},
			candidate:       &defrag.Candidate{Nodes: []string{"n"}},
			wantTimedOut:    false,
		},
		{
			name: "candidate with a single scaled down node within timeout",
			scaledDownNodes: map[string]time.Time{
				"n": time.Now(),
			},
			candidate:    &defrag.Candidate{Nodes: []string{"n"}},
			wantTimedOut: false,
		},
		{
			name: "candidate with a single scaled down node over timeout",
			scaledDownNodes: map[string]time.Time{
				"n": timedOut,
			},
			candidate:    &defrag.Candidate{Nodes: []string{"n"}},
			wantTimedOut: true,
		},
		{
			name: "candidate multiple nodes, all scaled down within time out",
			scaledDownNodes: map[string]time.Time{
				"n1": time.Now(),
				"n2": time.Now(),
				"n3": time.Now(),
			},
			candidate:    &defrag.Candidate{Nodes: []string{"n1", "n2", "n3"}},
			wantTimedOut: false,
		},
		{
			name: "candidate multiple nodes, some scaled down within timeout",
			scaledDownNodes: map[string]time.Time{
				"n1": time.Now(),
				"n2": timedOut,
				"n3": time.Now(),
			},
			candidate:    &defrag.Candidate{Nodes: []string{"n1", "n2", "n3"}},
			wantTimedOut: true,
		},
		{
			name: "candidate multiple nodes, all scaled down over timeout",
			scaledDownNodes: map[string]time.Time{
				"n1": timedOut,
				"n2": timedOut,
				"n3": timedOut,
			},
			candidate:    &defrag.Candidate{Nodes: []string{"n1", "n2", "n3"}},
			wantTimedOut: true,
		},
		{
			name: "candidate multiple nodes, some over time out some not scaled down",
			scaledDownNodes: map[string]time.Time{
				"n1": timedOut,
				"n2": timedOut,
			},
			candidate:    &defrag.Candidate{Nodes: []string{"n1", "n2", "n3"}},
			wantTimedOut: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defragActuator := newDefragActuator(actuatorOptions{})
			defragActuator.scaledDownNodes = tc.scaledDownNodes
			assert.Equal(t, tc.wantTimedOut, defragActuator.isScaleDownTimedOut(tc.candidate, 5*time.Minute))
		})
	}
}

func TestCleanScaleDownInfo(t *testing.T) {
	c1 := &defrag.Candidate{Nodes: []string{"n1"}}
	c2 := &defrag.Candidate{Nodes: []string{"n2", "n3"}}

	testCases := []struct {
		name                string
		candidates          []*defrag.Candidate
		scaledDownNodes     map[string]time.Time
		wantScaledDownNodes map[string]time.Time
	}{
		{
			name:                "no candidates, no scaled-down candidates",
			scaledDownNodes:     make(map[string]time.Time),
			wantScaledDownNodes: make(map[string]time.Time),
		},
		{
			name:                "some candidates, no scaled-down candidates",
			candidates:          []*defrag.Candidate{c1, c2},
			scaledDownNodes:     make(map[string]time.Time),
			wantScaledDownNodes: make(map[string]time.Time),
		},
		{
			name:       "scaled-down candidates are still candidates",
			candidates: []*defrag.Candidate{c1, c2},
			scaledDownNodes: map[string]time.Time{
				"n1": time.UnixMilli(1),
				"n2": time.UnixMilli(2),
			},
			wantScaledDownNodes: map[string]time.Time{
				"n1": time.UnixMilli(1),
				"n2": time.UnixMilli(2),
			},
		},
		{
			name:       "some scaled-down candidates are still candidates, some not",
			candidates: []*defrag.Candidate{c1, c2},
			scaledDownNodes: map[string]time.Time{
				"n2": time.UnixMilli(2),
				"n3": time.UnixMilli(3),
				"n4": time.UnixMilli(4),
				"n5": time.UnixMilli(5),
			},
			wantScaledDownNodes: map[string]time.Time{
				"n2": time.UnixMilli(2),
				"n3": time.UnixMilli(3),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defragActuator := newDefragActuator(actuatorOptions{})
			defragActuator.scaledDownNodes = tc.scaledDownNodes

			defragActuator.cleanScaleDownInfo(tc.candidates)
			assert.Equal(t, tc.wantScaledDownNodes, defragActuator.scaledDownNodes)
		})
	}
}

type mockScaleDownActuator struct {
	mock.Mock
}

func (m *mockScaleDownActuator) StartDeletion(empty, drain []*apiv1.Node) (status.ScaleDownResult, []*status.ScaleDownNode, errors.AutoscalerError) {
	args := m.Called(empty, drain)

	result := args.Get(0).(status.ScaleDownResult)
	nodes := args.Get(1).([]*status.ScaleDownNode)
	if args.Get(2) == nil {
		return result, nodes, nil
	}
	return result, nodes, args.Get(2).(caerrors.AutoscalerError)
}

func (m *mockScaleDownActuator) StartForceDeletion(empty, drain []*apiv1.Node) (status.ScaleDownResult, []*status.ScaleDownNode, errors.AutoscalerError) {
	args := m.Called(empty, drain)

	result := args.Get(0).(status.ScaleDownResult)
	nodes := args.Get(1).([]*status.ScaleDownNode)
	if args.Get(2) == nil {
		return result, nodes, nil
	}
	return result, nodes, args.Get(2).(caerrors.AutoscalerError)
}

func (m *mockScaleDownActuator) CheckStatus() scaledown.ActuationStatus {
	args := m.Called()
	return args.Get(0).(scaledown.ActuationStatus)
}

func (m *mockScaleDownActuator) ClearResultsNotNewerThan(timestamp time.Time) {
	m.Called(timestamp)
}

func (m *mockScaleDownActuator) DeletionResults() (map[string]status.NodeDeleteResult, time.Time) {
	return map[string]status.NodeDeleteResult{}, time.Now()
}

type mockScaleDownStatusProcessor struct {
	mock.Mock
}

func (m *mockScaleDownStatusProcessor) Process(ctx *context.AutoscalingContext, status *status.ScaleDownStatus) {
	m.Called(ctx, status)
}

func (m *mockScaleDownStatusProcessor) CleanUp() {
	m.Called()
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
