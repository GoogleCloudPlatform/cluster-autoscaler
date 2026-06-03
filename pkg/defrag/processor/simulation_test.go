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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	cacontext "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/pdb"

	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/predicate"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/store"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	csisnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/csi/snapshot"
	drasnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/dynamicresources/snapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	schedulerframework "k8s.io/kube-scheduler/framework"
	schedulermetrics "k8s.io/kubernetes/pkg/scheduler/metrics"
)

func TestSimulatePodsScheduling(t *testing.T) {
	schedulermetrics.Register()

	pod1 := test.BuildTestPod("p1", 400, 1)
	pod2 := test.BuildTestPod("p2", 500, 1)
	pod3 := test.BuildTestPod("p3", 600, 1)

	testCases := []struct {
		name          string
		nodesWithPods map[*apiv1.Node][]*apiv1.Pod

		setupMockSnapshotStore func() *mockSnapshotStore
		checkMockSnapshotStore func(*testing.T, *mockSnapshotStore)

		candidate         *defrag.Candidate
		allCandidateNodes map[string]bool

		wantCandidatePods *candidatePods
		wantErr           bool
	}{
		{
			name: "candidate with recreatable unschedulable pods",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 1500, 10): {pod1, pod2, pod3},
			},
			setupMockSnapshotStore: func() *mockSnapshotStore {
				snapshot := newMockSnapshotStore()
				snapshot.m.On("Fork").Return().Once()
				snapshot.m.On("Commit").Return(nil).Once()
				return snapshot
			},
			checkMockSnapshotStore: func(t *testing.T, snapshot *mockSnapshotStore) {
				snapshot.m.AssertNumberOfCalls(t, "Fork", 1)
				snapshot.m.AssertNumberOfCalls(t, "Revert", 0)
				snapshot.m.AssertNumberOfCalls(t, "Commit", 1)
				snapshot.m.AssertNumberOfCalls(t, "StorePodInfo", 0)
			},
			candidate:         &defrag.Candidate{Nodes: []string{"n1"}},
			allCandidateNodes: map[string]bool{"n1": true},
			wantCandidatePods: &candidatePods{
				unschedulable: []*apiv1.Pod{pod1, pod2, pod3},
			},
		},
		{
			name: "candidate with recreatable pods schedulable on existing nodes",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 1500, 10): {pod1, pod2, pod3},
				test.BuildTestNode("n2", 1500, 10): {},
			},
			setupMockSnapshotStore: func() *mockSnapshotStore {
				snapshot := newMockSnapshotStore()
				snapshot.m.On("Fork").Return().Once()
				snapshot.m.On("StorePodInfo", pod1, "n2").Return(nil).Once()
				snapshot.m.On("StorePodInfo", pod2, "n2").Return(nil).Once()
				snapshot.m.On("StorePodInfo", pod3, "n2").Return(nil).Once()
				snapshot.m.On("Commit").Return(nil).Once()
				return snapshot
			},
			checkMockSnapshotStore: func(t *testing.T, snapshot *mockSnapshotStore) {
				snapshot.m.AssertNumberOfCalls(t, "Fork", 1)
				snapshot.m.AssertNumberOfCalls(t, "Revert", 0)
				snapshot.m.AssertNumberOfCalls(t, "Commit", 1)
				snapshot.m.AssertNumberOfCalls(t, "StorePodInfo", 3)
			},
			candidate:         &defrag.Candidate{Nodes: []string{"n1"}},
			allCandidateNodes: map[string]bool{"n1": true},
			wantCandidatePods: &candidatePods{
				schedulableOnExisting: []*apiv1.Pod{pod1, pod2, pod3},
			},
		},
		{
			name: "candidate with recreatable pods not schedulable on other candidate node",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 1500, 10): {pod1, pod2, pod3},
				test.BuildTestNode("n2", 1500, 10): {},
			},
			setupMockSnapshotStore: func() *mockSnapshotStore {
				snapshot := newMockSnapshotStore()
				snapshot.m.On("Fork").Return().Once()
				snapshot.m.On("Commit").Return(nil).Once()
				return snapshot
			},
			checkMockSnapshotStore: func(t *testing.T, snapshot *mockSnapshotStore) {
				snapshot.m.AssertNumberOfCalls(t, "Fork", 1)
				snapshot.m.AssertNumberOfCalls(t, "Revert", 0)
				snapshot.m.AssertNumberOfCalls(t, "Commit", 1)
				snapshot.m.AssertNumberOfCalls(t, "StorePodInfo", 0)
			},
			candidate:         &defrag.Candidate{Nodes: []string{"n1"}},
			allCandidateNodes: map[string]bool{"n1": true, "n2": true},
			wantCandidatePods: &candidatePods{
				unschedulable: []*apiv1.Pod{pod1, pod2, pod3},
			},
		},
		{
			name: "candidate with recreatable pods schedulable on upcoming node",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 1500, 10): {pod1, pod2, pod3},
				buildUpcomingNode("n2", 1500, 10):  {},
			},
			setupMockSnapshotStore: func() *mockSnapshotStore {
				snapshot := newMockSnapshotStore()
				snapshot.m.On("Fork").Return().Once()
				snapshot.m.On("StorePodInfo", pod1, "n2").Return(nil).Once()
				snapshot.m.On("StorePodInfo", pod2, "n2").Return(nil).Once()
				snapshot.m.On("StorePodInfo", pod3, "n2").Return(nil).Once()
				snapshot.m.On("Commit").Return(nil).Once()
				return snapshot
			},
			checkMockSnapshotStore: func(t *testing.T, snapshot *mockSnapshotStore) {
				snapshot.m.AssertNumberOfCalls(t, "Fork", 1)
				snapshot.m.AssertNumberOfCalls(t, "Revert", 0)
				snapshot.m.AssertNumberOfCalls(t, "Commit", 1)
				snapshot.m.AssertNumberOfCalls(t, "StorePodInfo", 3)
			},
			candidate:         &defrag.Candidate{Nodes: []string{"n1"}},
			allCandidateNodes: map[string]bool{"n1": true},
			wantCandidatePods: &candidatePods{
				schedulableOnUpcoming: []*apiv1.Pod{pod1, pod2, pod3},
			},
		},
		{
			name: "candidate with recreatable pods with various scheduling",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 1500, 10): {pod1, pod2, pod3},
				test.BuildTestNode("n2", 400, 10):  {},
				buildUpcomingNode("n3", 500, 10):   {},
				test.BuildTestNode("n4", 600, 10):  {},
			},
			setupMockSnapshotStore: func() *mockSnapshotStore {
				snapshot := newMockSnapshotStore()
				snapshot.m.On("Fork").Return().Once()
				snapshot.m.On("StorePodInfo", pod1, "n2").Return(nil).Once()
				snapshot.m.On("StorePodInfo", pod2, "n3").Return(nil).Once()
				snapshot.m.On("Commit").Return(nil).Once()
				return snapshot
			},
			checkMockSnapshotStore: func(t *testing.T, snapshot *mockSnapshotStore) {
				snapshot.m.AssertNumberOfCalls(t, "Fork", 1)
				snapshot.m.AssertNumberOfCalls(t, "Revert", 0)
				snapshot.m.AssertNumberOfCalls(t, "Commit", 1)
				snapshot.m.AssertNumberOfCalls(t, "StorePodInfo", 2)
			},
			candidate:         &defrag.Candidate{Nodes: []string{"n1"}},
			allCandidateNodes: map[string]bool{"n1": true, "n4": true},
			wantCandidatePods: &candidatePods{
				schedulableOnExisting: []*apiv1.Pod{pod1},
				schedulableOnUpcoming: []*apiv1.Pod{pod2},
				unschedulable:         []*apiv1.Pod{pod3},
			},
		},
		{
			name: "candidate with many nodes",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 1500, 10): {pod1},
				test.BuildTestNode("n2", 1500, 10): {pod2},
				test.BuildTestNode("n3", 1500, 10): {pod3},
				test.BuildTestNode("n4", 400, 10):  {},
				buildUpcomingNode("n5", 500, 10):   {},
				test.BuildTestNode("n6", 600, 10):  {},
			},
			setupMockSnapshotStore: func() *mockSnapshotStore {
				snapshot := newMockSnapshotStore()
				snapshot.m.On("Fork").Return().Once()
				snapshot.m.On("StorePodInfo", pod1, "n4").Return(nil).Once()
				snapshot.m.On("StorePodInfo", pod2, "n5").Return(nil).Once()
				snapshot.m.On("Commit").Return(nil).Once()
				return snapshot
			},
			checkMockSnapshotStore: func(t *testing.T, snapshot *mockSnapshotStore) {
				snapshot.m.AssertNumberOfCalls(t, "Fork", 1)
				snapshot.m.AssertNumberOfCalls(t, "Revert", 0)
				snapshot.m.AssertNumberOfCalls(t, "Commit", 1)
				snapshot.m.AssertNumberOfCalls(t, "StorePodInfo", 2)
			},
			candidate:         &defrag.Candidate{Nodes: []string{"n1", "n2", "n3"}},
			allCandidateNodes: map[string]bool{"n1": true, "n2": true, "n3": true, "n6": true},
			wantCandidatePods: &candidatePods{
				schedulableOnExisting: []*apiv1.Pod{pod1},
				schedulableOnUpcoming: []*apiv1.Pod{pod2},
				unschedulable:         []*apiv1.Pod{pod3},
			},
		},
		{
			name: "error during scheduling on existing",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 1500, 10): {pod1, pod2, pod3},
				test.BuildTestNode("n2", 400, 10):  {},
				buildUpcomingNode("n3", 500, 10):   {},
			},
			setupMockSnapshotStore: func() *mockSnapshotStore {
				snapshot := newMockSnapshotStore()
				snapshot.m.On("Fork").Return().Once()
				snapshot.m.On("StorePodInfo", pod1, "n2").Return(errors.New("error")).Once()
				snapshot.m.On("Revert").Return().Once()
				return snapshot
			},
			checkMockSnapshotStore: func(t *testing.T, snapshot *mockSnapshotStore) {
				snapshot.m.AssertNumberOfCalls(t, "Fork", 1)
				snapshot.m.AssertNumberOfCalls(t, "Revert", 1)
				snapshot.m.AssertNumberOfCalls(t, "Commit", 0)
				snapshot.m.AssertNumberOfCalls(t, "StorePodInfo", 1)
			},
			candidate:         &defrag.Candidate{Nodes: []string{"n1"}},
			allCandidateNodes: map[string]bool{"n1": true},
			wantErr:           true,
		},
		{
			name: "error during scheduling on upcoming",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 1500, 10): {pod1, pod2, pod3},
				test.BuildTestNode("n2", 400, 10):  {},
				buildUpcomingNode("n3", 500, 10):   {},
			},
			setupMockSnapshotStore: func() *mockSnapshotStore {
				snapshot := newMockSnapshotStore()
				snapshot.m.On("Fork").Return().Once()
				snapshot.m.On("StorePodInfo", pod1, "n2").Return(nil).Once()
				snapshot.m.On("StorePodInfo", pod2, "n3").Return(errors.New("error")).Once()
				snapshot.m.On("Revert").Return().Once()
				return snapshot
			},
			checkMockSnapshotStore: func(t *testing.T, snapshot *mockSnapshotStore) {
				snapshot.m.AssertNumberOfCalls(t, "Fork", 1)
				snapshot.m.AssertNumberOfCalls(t, "Revert", 1)
				snapshot.m.AssertNumberOfCalls(t, "Commit", 0)
				snapshot.m.AssertNumberOfCalls(t, "StorePodInfo", 2)
			},
			candidate:         &defrag.Candidate{Nodes: []string{"n1"}},
			allCandidateNodes: map[string]bool{"n1": true},
			wantErr:           true,
		},
		{
			name: "error during commit",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 1500, 10): {pod1, pod2, pod3},
				test.BuildTestNode("n2", 400, 10):  {},
				buildUpcomingNode("n3", 500, 10):   {},
			},
			setupMockSnapshotStore: func() *mockSnapshotStore {
				snapshot := newMockSnapshotStore()
				snapshot.m.On("Fork").Return().Once()
				snapshot.m.On("StorePodInfo", pod1, "n2").Return(nil).Once()
				snapshot.m.On("StorePodInfo", pod2, "n3").Return(nil).Once()
				snapshot.m.On("Commit").Return(errors.New("error")).Once()
				return snapshot
			},
			checkMockSnapshotStore: func(t *testing.T, snapshot *mockSnapshotStore) {
				snapshot.m.AssertNumberOfCalls(t, "Fork", 1)
				snapshot.m.AssertNumberOfCalls(t, "Revert", 0)
				snapshot.m.AssertNumberOfCalls(t, "Commit", 1)
				snapshot.m.AssertNumberOfCalls(t, "StorePodInfo", 2)
			},
			candidate:         &defrag.Candidate{Nodes: []string{"n1"}},
			allCandidateNodes: map[string]bool{"n1": true},
			wantErr:           true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			snapshotStore := tc.setupMockSnapshotStore()
			snapshot := testsnapshot.NewCustomTestSnapshotOrDie(t, snapshotStore)
			for node, pods := range tc.nodesWithPods {
				assert.NoError(t, snapshot.AddNodeInfo(framework.NewTestNodeInfo(node, pods...)))
			}

			recreatablePods, err := recreatablePods(snapshot, tc.candidate.Nodes)
			assert.NoError(t, err)

			simulator := newSimulator(simulatorOptions{})
			candidatePods, err := simulator.simulatePodsScheduling(snapshot, tc.candidate.Nodes, tc.allCandidateNodes)
			tc.checkMockSnapshotStore(t, snapshotStore)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.Equal(t, tc.wantCandidatePods, candidatePods)
				assert.NoError(t, err)

				simulatedPods := candidatePods.schedulableOnExisting
				simulatedPods = append(simulatedPods, candidatePods.schedulableOnUpcoming...)
				simulatedPods = append(simulatedPods, candidatePods.unschedulable...)
				assert.ElementsMatch(t, simulatedPods, recreatablePods)
			}
		})
	}
}

func TestSchedulePods(t *testing.T) {
	testCases := []struct {
		name  string
		nodes []*apiv1.Node

		pendingPods       []*apiv1.Pod
		allCandidateNodes map[string]bool
		upcomingNodes     bool

		wantSchedulablePods   []*apiv1.Pod
		wantUnschedulablePods []*apiv1.Pod
	}{
		{
			name: "no pending pods",
			nodes: []*apiv1.Node{
				test.BuildTestNode("n1", 1000, 40),
				test.BuildTestNode("n2", 2000, 30),
				buildUpcomingNode("n3", 3000, 20),
				buildUpcomingNode("n4", 4000, 10),
			},
			upcomingNodes: true,
		},
		{
			name: "multiple pending pod, non existing nodes",
			nodes: []*apiv1.Node{
				test.BuildTestNode("n1", 1000, 40),
				test.BuildTestNode("n2", 2000, 30),
				buildUpcomingNode("n3", 3000, 20),
				buildUpcomingNode("n4", 4000, 10),
			},
			pendingPods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1000, 40),
				test.BuildTestPod("p2", 2000, 30),
				test.BuildTestPod("p3", 3000, 20),
				test.BuildTestPod("p4", 4000, 10),
			},
			upcomingNodes: true,
			wantSchedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p3", 3000, 20),
				test.BuildTestPod("p4", 4000, 10),
			},
			wantUnschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1000, 40),
				test.BuildTestPod("p2", 2000, 30),
			},
		},
		{
			name: "multiple pending pod, existing nodes",
			nodes: []*apiv1.Node{
				test.BuildTestNode("n1", 1000, 40),
				test.BuildTestNode("n2", 2000, 30),
				buildUpcomingNode("n3", 3000, 20),
				buildUpcomingNode("n4", 4000, 10),
			},
			pendingPods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1000, 40),
				test.BuildTestPod("p2", 2000, 30),
				test.BuildTestPod("p3", 3000, 20),
				test.BuildTestPod("p4", 4000, 10),
			},
			upcomingNodes: false,
			wantSchedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1000, 40),
				test.BuildTestPod("p2", 2000, 30),
			},
			wantUnschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p3", 3000, 20),
				test.BuildTestPod("p4", 4000, 10),
			},
		},
		{
			name: "candidate nodes not available for scheduling, non-existing nodes",
			nodes: []*apiv1.Node{
				test.BuildTestNode("n1", 1000, 40),
				test.BuildTestNode("n2", 2000, 30),
				buildUpcomingNode("n3", 3000, 20),
				buildUpcomingNode("n4", 4000, 10),
			},
			pendingPods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1000, 40),
				test.BuildTestPod("p2", 2000, 30),
				test.BuildTestPod("p3", 3000, 20),
				test.BuildTestPod("p4", 4000, 10),
			},
			allCandidateNodes: map[string]bool{"n2": true, "n4": true},
			upcomingNodes:     true,
			wantSchedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p3", 3000, 20),
			},
			wantUnschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1000, 40),
				test.BuildTestPod("p2", 2000, 30),
				test.BuildTestPod("p4", 4000, 10),
			},
		},
		{
			name: "candidate nodes not available for scheduling, existing nodes",
			nodes: []*apiv1.Node{
				test.BuildTestNode("n1", 1000, 40),
				test.BuildTestNode("n2", 2000, 30),
				buildUpcomingNode("n3", 3000, 20),
				buildUpcomingNode("n4", 4000, 10),
			},
			pendingPods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1000, 40),
				test.BuildTestPod("p2", 2000, 30),
				test.BuildTestPod("p3", 3000, 20),
				test.BuildTestPod("p4", 4000, 10),
			},
			allCandidateNodes: map[string]bool{"n2": true, "n4": true},
			upcomingNodes:     false,
			wantSchedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p1", 1000, 40),
			},
			wantUnschedulablePods: []*apiv1.Pod{
				test.BuildTestPod("p2", 2000, 30),
				test.BuildTestPod("p3", 3000, 20),
				test.BuildTestPod("p4", 4000, 10),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			assert.NoError(t, snapshot.SetClusterState(tc.nodes, nil, drasnapshot.NewEmptySnapshot(), csisnapshot.NewEmptySnapshot()))

			simulator := newSimulator(simulatorOptions{})
			schedulablePods, unschedulablePods, err := simulator.schedulePods(snapshot, tc.pendingPods, tc.allCandidateNodes, tc.upcomingNodes)
			assert.ElementsMatch(t, tc.wantSchedulablePods, schedulablePods)
			assert.ElementsMatch(t, tc.wantUnschedulablePods, unschedulablePods)
			assert.NoError(t, err)
		})
	}
}

func TestRecreatablePods(t *testing.T) {
	now := time.Now()

	testCases := []struct {
		name          string
		nodesWithPods map[*apiv1.Node][]*apiv1.Pod
		nodeNames     []string
		matchOrder    bool
		wantPods      []*apiv1.Pod
		wantErr       bool
	}{
		{
			name: "single empty node",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n", 100, 1): {},
			},
			nodeNames: []string{"n"},
		},
		{
			name: "single node with recreatable pods",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n", 100, 1): {
					test.BuildTestPod("p1", 10, 1),
					test.SetRSPodSpec(test.BuildTestPod("p2", 10, 1), "rs"),
				},
			},
			nodeNames: []string{"n"},
			wantPods: []*apiv1.Pod{
				test.BuildTestPod("p1", 10, 1),
				test.SetRSPodSpec(test.BuildTestPod("p2", 10, 1), "rs"),
			},
		},
		{
			name: "single node with non-recreatable pods",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n", 100, 1): {
					test.SetDSPodSpec(test.BuildTestPod("p1", 10, 1)),
					test.SetMirrorPodSpec(test.BuildTestPod("p2", 10, 1)),
					test.SetStaticPodSpec(test.BuildTestPod("p3", 10, 1)),
				},
			},
			nodeNames: []string{"n"},
		},
		{
			name: "single node with both recreatable and non-recreatable pods",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n", 100, 1): {
					test.SetDSPodSpec(test.BuildTestPod("p1", 10, 1)),
					test.BuildTestPod("p2", 10, 1),
					test.SetMirrorPodSpec(test.BuildTestPod("p3", 10, 1)),
					test.SetRSPodSpec(test.BuildTestPod("p4", 10, 1), "rs"),
					test.SetStaticPodSpec(test.BuildTestPod("p5", 10, 1)),
				},
			},
			nodeNames: []string{"n"},
			wantPods: []*apiv1.Pod{
				test.BuildTestPod("p2", 10, 1),
				test.SetRSPodSpec(test.BuildTestPod("p4", 10, 1), "rs"),
			},
		},
		{
			name: "multiple nodes",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 100, 1): {
					test.SetDSPodSpec(test.BuildTestPod("p1", 10, 1)),
					test.BuildTestPod("p2", 10, 1),
				},
				test.BuildTestNode("n2", 100, 1): {
					test.SetMirrorPodSpec(test.BuildTestPod("p3", 10, 1)),
					test.SetRSPodSpec(test.BuildTestPod("p4", 10, 1), "rs"),
				},
				test.BuildTestNode("n3", 100, 1): {
					test.SetStaticPodSpec(test.BuildTestPod("p5", 10, 1)),
					test.SetRSPodSpec(test.BuildTestPod("p6", 10, 1), "rs"),
				},
			},
			nodeNames: []string{"n1", "n2"},
			wantPods: []*apiv1.Pod{
				test.BuildTestPod("p2", 10, 1),
				test.SetRSPodSpec(test.BuildTestPod("p4", 10, 1), "rs"),
			},
		},
		{
			name: "pods are ordered according to creation timestamp",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n1", 100, 1): {
					test.BuildTestPod("p1", 10, 1, withCreationTime(now.Add(-5*time.Minute))),
					test.BuildTestPod("p2", 10, 1, withCreationTime(now.Add(-15*time.Minute))),
				},
				test.BuildTestNode("n2", 100, 1): {
					test.BuildTestPod("p3", 10, 1, withCreationTime(now.Add(-10*time.Minute))),
				},
				test.BuildTestNode("n3", 100, 1): {
					test.BuildTestPod("p4", 10, 1, withCreationTime(now.Add(-20*time.Minute))),
				},
			},
			nodeNames:  []string{"n1", "n2", "n3"},
			matchOrder: true,
			wantPods: []*apiv1.Pod{
				test.BuildTestPod("p4", 10, 1, withCreationTime(now.Add(-20*time.Minute))),
				test.BuildTestPod("p2", 10, 1, withCreationTime(now.Add(-15*time.Minute))),
				test.BuildTestPod("p3", 10, 1, withCreationTime(now.Add(-10*time.Minute))),
				test.BuildTestPod("p1", 10, 1, withCreationTime(now.Add(-5*time.Minute))),
			},
		},
		{
			name: "no nodes (should not happen)",
			nodesWithPods: map[*apiv1.Node][]*apiv1.Pod{
				test.BuildTestNode("n", 100, 1): {
					test.SetDSPodSpec(test.BuildTestPod("p1", 10, 1)),
					test.BuildTestPod("p2", 10, 1),
					test.SetMirrorPodSpec(test.BuildTestPod("p3", 10, 1)),
					test.SetRSPodSpec(test.BuildTestPod("p4", 10, 1), "rs"),
					test.SetStaticPodSpec(test.BuildTestPod("p5", 10, 1)),
				},
			},
			nodeNames: []string{},
		},
		{
			name:      "non-existing nodes (should not happen)",
			nodeNames: []string{"n"},
			wantErr:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			for node, pods := range tc.nodesWithPods {
				assert.NoError(t, snapshot.AddNodeInfo(framework.NewTestNodeInfo(node, pods...)))
			}

			pods, err := recreatablePods(snapshot, tc.nodeNames)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				if tc.matchOrder {
					assert.EqualValues(t, pods, tc.wantPods)
				} else {
					assert.ElementsMatch(t, pods, tc.wantPods)
				}
				assert.NoError(t, err)
			}
		})
	}
}

func TestSimulateNodeRemovals(t *testing.T) {
	testCases := []struct {
		name              string
		nodes             []*apiv1.Node
		pods              []*apiv1.Pod
		candidateNodes    []string
		allCandidateNodes []string
		nodesWithPods     map[string][]string
		wantNodesWithPods map[string][]string
		wantNodesToRemove []string
		wantUnreadyNodes  []string
	}{
		{
			name: "some nodes to be removed",
			nodes: []*apiv1.Node{
				buildReadyNode("n1", 1000, 10),
				buildReadyNode("n2", 1000, 10),
				buildReadyNode("n3", 1000, 10),
			},
			pods: []*apiv1.Pod{
				buildReplicatedPod("p1", 400, 1),
				buildReplicatedPod("p2", 500, 1),
				buildReplicatedPod("p3", 600, 1),
			},
			nodesWithPods: map[string][]string{
				"n1": {"p1", "p2"},
				"n2": {"p3"},
				"n3": {},
			},
			candidateNodes:    []string{"n1", "n2"},
			allCandidateNodes: []string{"n1", "n2"},
			wantNodesWithPods: map[string][]string{
				"n2": {"p3"},
				"n3": {"p1", "p2"},
			},
			wantNodesToRemove: []string{"n1"},
			wantUnreadyNodes:  []string{"n2"},
		},
		{
			name: "no node can be removed",
			nodes: []*apiv1.Node{
				buildReadyNode("n1", 1000, 10),
				buildReadyNode("n2", 400, 10),
			},
			pods: []*apiv1.Pod{
				buildReplicatedPod("p1", 500, 1),
			},
			nodesWithPods: map[string][]string{
				"n1": {"p1"},
				"n2": {},
			},
			candidateNodes:    []string{"n1"},
			allCandidateNodes: []string{"n1"},
			wantNodesWithPods: map[string][]string{
				"n1": {"p1"},
				"n2": {},
			},
			wantNodesToRemove: nil,
			wantUnreadyNodes:  []string{"n1"},
		},
		{
			name: "all nodes can be removed",
			nodes: []*apiv1.Node{
				buildReadyNode("n1", 1000, 10),
				buildReadyNode("n2", 1000, 10),
				buildReadyNode("n3", 1000, 10),
				buildReadyNode("n4", 2000, 10),
			},
			pods: []*apiv1.Pod{
				buildReplicatedPod("p1", 400, 1),
				buildReplicatedPod("p2", 600, 1),
				buildReplicatedPod("p3", 400, 1),
				buildReplicatedPod("p4", 600, 1),
			},
			nodesWithPods: map[string][]string{
				"n1": {"p1", "p2"},
				"n2": {"p3", "p4"},
				"n3": {},
				"n4": {},
			},
			candidateNodes:    []string{"n1", "n2"},
			allCandidateNodes: []string{"n1", "n2", "n3"},
			wantNodesWithPods: map[string][]string{
				"n3": {},
				"n4": {"p1", "p2", "p3", "p4"},
			},
			wantNodesToRemove: []string{"n1", "n2"},
			wantUnreadyNodes:  nil,
		},
		{
			name: "cannot move pods to an upcoming node",
			nodes: []*apiv1.Node{
				buildReadyNode("n1", 1000, 10),
				buildUpcomingNode("n2", 1000, 10),
			},
			pods: []*apiv1.Pod{
				buildReplicatedPod("p1", 400, 1),
			},
			nodesWithPods: map[string][]string{
				"n1": {"p1"},
			},
			candidateNodes:    []string{"n1"},
			allCandidateNodes: []string{"n1"},
			wantNodesWithPods: map[string][]string{
				"n1": {"p1"},
				"n2": {},
			},
			wantNodesToRemove: nil,
			wantUnreadyNodes:  []string{"n1"},
		},
		{
			name: "cannot move pods to other candidate nodes",
			nodes: []*apiv1.Node{
				buildReadyNode("n1", 1000, 10),
				buildReadyNode("n2", 1000, 10),
			},
			pods: []*apiv1.Pod{
				buildReplicatedPod("p1", 400, 1),
			},
			nodesWithPods: map[string][]string{
				"n1": {"p1"},
			},
			candidateNodes:    []string{"n1"},
			allCandidateNodes: []string{"n1", "n2"},
			wantNodesWithPods: map[string][]string{
				"n1": {"p1"},
				"n2": {},
			},
			wantNodesToRemove: nil,
			wantUnreadyNodes:  []string{"n1"},
		},
		{
			name: "candidate node has blocking pods",
			nodes: []*apiv1.Node{
				buildReadyNode("n1", 1000, 10),
				buildReadyNode("n2", 1000, 10),
				buildReadyNode("n3", 2000, 10),
			},
			pods: []*apiv1.Pod{
				buildReplicatedPod("p1", 400, 1),
				buildReplicatedPod("p2", 600, 1),
				test.BuildTestPod("p3", 400, 1),
				test.BuildTestPod("p4", 600, 1),
			},
			nodesWithPods: map[string][]string{
				"n1": {"p1", "p2"},
				"n2": {"p3", "p4"},
				"n3": {},
			},
			candidateNodes:    []string{"n1", "n2"},
			allCandidateNodes: []string{"n1", "n2"},
			wantNodesWithPods: map[string][]string{
				"n2": {"p3", "p4"},
				"n3": {"p1", "p2"},
			},
			wantNodesToRemove: []string{"n1"},
			wantUnreadyNodes:  nil,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nodeMap := make(map[string]*apiv1.Node)
			for _, n := range tc.nodes {
				nodeMap[n.Name] = n
			}
			podMap := make(map[string]*apiv1.Pod)
			for _, p := range tc.pods {
				podMap[p.Name] = p
			}
			for nodeName, pods := range tc.nodesWithPods {
				for _, podName := range pods {
					p := podMap[podName]
					p.Spec.NodeName = nodeName
				}
			}
			allCandidateNodes := make(map[string]bool)
			for _, n := range tc.allCandidateNodes {
				allCandidateNodes[n] = true
			}
			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			if err := snapshot.SetClusterState(tc.nodes, tc.pods, drasnapshot.NewEmptySnapshot(), csisnapshot.NewEmptySnapshot()); err != nil {
				t.Fatalf("failed to set cluster state: %v", err)
			}
			ctx := &cacontext.AutoscalingContext{
				ClusterSnapshot:     snapshot,
				RemainingPdbTracker: pdb.NewBasicRemainingPdbTracker(),
			}
			simulator := newSimulator(simulatorOptions{})
			nodesToRemove, unreadyNodes, err := simulator.simulateNodeRemovals(ctx, tc.candidateNodes, allCandidateNodes)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if diff := cmp.Diff(tc.wantNodesToRemove, nodesToRemove); diff != "" {
				t.Errorf("nodesToRemove mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantUnreadyNodes, unreadyNodes); diff != "" {
				t.Errorf("unreadyNodes mismatch (-want +got):\n%s", diff)
			}
			for nodeName, wantPods := range tc.wantNodesWithPods {
				nodeInfo, err := snapshot.GetNodeInfo(nodeName)
				if err != nil {
					t.Fatalf("failed to get node info for node %s: %v", nodeName, err)
				}
				gotPods := make([]string, 0)
				for _, pod := range nodeInfo.Pods() {
					gotPods = append(gotPods, pod.Pod.Name)
				}
				if diff := cmp.Diff(wantPods, gotPods); diff != "" {
					t.Errorf("pods mismatch on node %s (-want +got):\n%s", nodeName, diff)
				}
			}
		})
	}
}

type mockSnapshotStore struct {
	s *predicate.PredicateSnapshot
	m mock.Mock
}

func newMockSnapshotStore() *mockSnapshotStore {
	store := store.NewBasicSnapshotStore()
	predicateSnapshot := predicate.NewPredicateSnapshot(store, nil, false, 1, false)
	return &mockSnapshotStore{s: predicateSnapshot, m: mock.Mock{}}
}

func (m *mockSnapshotStore) NodeInfos() schedulerframework.NodeInfoLister {
	return m.s.NodeInfos()
}

func (m *mockSnapshotStore) PodGroupStates() schedulerframework.PodGroupStateLister {
	return m.s.PodGroupStates()
}

func (m *mockSnapshotStore) StorageInfos() schedulerframework.StorageInfoLister {
	return m.s.StorageInfos()
}

func (m *mockSnapshotStore) ResourceClaims() schedulerframework.ResourceClaimTracker {
	return m.s.ResourceClaims()
}

func (m *mockSnapshotStore) ResourceSlices() schedulerframework.ResourceSliceLister {
	return m.s.ResourceSlices()
}

func (m *mockSnapshotStore) DeviceClasses() schedulerframework.DeviceClassLister {
	return m.s.DeviceClasses()
}

func (m *mockSnapshotStore) DeviceClassResolver() schedulerframework.DeviceClassResolver {
	return m.s.DeviceClassResolver()
}

func (m *mockSnapshotStore) SetClusterState(nodes []*apiv1.Node, scheduledPods []*apiv1.Pod, draSnapshot *drasnapshot.Snapshot, csiSnapshot *csisnapshot.Snapshot) error {
	return m.s.SetClusterState(nodes, scheduledPods, draSnapshot, csiSnapshot)
}

func (m *mockSnapshotStore) DraSnapshot() *drasnapshot.Snapshot {
	return m.s.DraSnapshot()
}

func (m *mockSnapshotStore) CSINodes() schedulerframework.CSINodeLister {
	return m.s.CSINodes()
}

func (m *mockSnapshotStore) CsiSnapshot() *csisnapshot.Snapshot {
	return m.s.CsiSnapshot()
}

func (m *mockSnapshotStore) StoreNodeInfo(nodeInfo *framework.NodeInfo) error {
	return m.s.StoreNodeInfo(nodeInfo)
}

func (m *mockSnapshotStore) RemoveNodeInfo(nodeName string) error {
	return m.s.RemoveNodeInfo(nodeName)
}

func (m *mockSnapshotStore) StorePodInfo(podInfo *framework.PodInfo, nodeName string) error {
	expectedErr := m.m.Called(podInfo.Pod, nodeName).Error(0)
	if expectedErr == nil {
		m.s.StorePodInfo(podInfo, nodeName)
	}
	return expectedErr
}

func (m *mockSnapshotStore) RemovePodInfo(namespace, podName, nodeName string) error {
	m.m.Called(namespace, podName, nodeName)
	return m.s.RemovePodInfo(namespace, podName, nodeName)
}

func (m *mockSnapshotStore) Fork() {
	m.m.Called()
}

func (m *mockSnapshotStore) Revert() {
	m.m.Called()
}

func (m *mockSnapshotStore) Commit() error {
	return m.m.Called().Error(0)
}

func (m *mockSnapshotStore) Clear() {
	m.m.Called()
}

func withCreationTime(t time.Time) func(*apiv1.Pod) {
	return func(p *apiv1.Pod) {
		p.CreationTimestamp = v1.NewTime(t)
	}
}

func buildReplicatedPod(name string, cpu, mem int64) *apiv1.Pod {
	return test.BuildTestPod(name, cpu, mem, func(pod *apiv1.Pod) {
		test.SetRSPodSpec(pod, fmt.Sprintf("rs-%s", pod.Name))
	})
}
