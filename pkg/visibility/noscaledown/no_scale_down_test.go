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

package noscaledown

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/status"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/utilization"
	"k8s.io/autoscaler/cluster-autoscaler/utils/drain"
	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
)

func TestGetNewReasons(t *testing.T) {
	now := time.Now()
	nsd := NewNoScaleDown(time.Minute)

	node1 := &vistypes.Node{Name: "node1", Mig: &vistypes.GkeMig{Name: "mig1"}, UtilInfo: &utilization.Info{CpuUtil: 0.12345, MemUtil: 0.66666}}
	node2 := &vistypes.Node{Name: "node2", Mig: &vistypes.GkeMig{Name: "mig1"}, UtilInfo: &utilization.Info{CpuUtil: 0.12345, MemUtil: 0.33333}}
	scaleDownStatus := &vistypes.ScaleDownStatus{
		Result: status.ScaleDownInCooldown,
		UnremovableNodes: []*vistypes.UnremovableNode{
			{Node: node1, Reason: simulator.NoPlaceToMovePods},
			{Node: node2, Reason: simulator.NotUnderutilized},
		},
	}

	// Check that reasons are returned when there are meaningful unremovable nodes.
	reasons := nsd.GetNewReasons(scaleDownStatus, now)
	assert.Equal(t, &Reasons{
		TopLevel: vistypes.NewNoScaleDownInBackoffMsg(),
		UnremovableNodes: []*vistypes.NodeExplanation{
			{Node: node1, Reason: vistypes.NewNoScaleDownNodeNoPlaceToMovePodsMsg()},
		},
	}, reasons)

	// Check that if a top-level reason changes, its returned even though unremovable nodes are throttled (but
	// are still present).
	nsd.MarkReasonsReported(reasons, now.Add(time.Second))
	scaleDownStatus.Result = status.ScaleDownInProgress
	reasons = nsd.GetNewReasons(scaleDownStatus, now.Add(10*time.Second))
	assert.Equal(t, &Reasons{
		TopLevel: vistypes.NewNoScaleDownInProgressMsg(),
	}, reasons)

	// Check that no reasons are reported when there are no meaningful unremovable nodes.
	scaleDownStatus.UnremovableNodes = []*vistypes.UnremovableNode{
		{Node: node1, Reason: simulator.RecentlyUnremovable},
		{Node: node2, Reason: simulator.NotUnderutilized},
	}
	reasons = nsd.GetNewReasons(scaleDownStatus, now.Add(time.Hour))
	assert.True(t, reasons.IsEmpty())

	// Check that no reasons are reported when there are no unremovable nodes at all.
	scaleDownStatus.UnremovableNodes = []*vistypes.UnremovableNode{}
	reasons = nsd.GetNewReasons(scaleDownStatus, now.Add(time.Hour))
	assert.True(t, reasons.IsEmpty())
}

func TestTopLevelReason(t *testing.T) {
	for _, testCase := range []struct {
		scaleDownResult status.ScaleDownResult
		expectedReason  *vistypes.Message
	}{
		{status.ScaleDownError, vistypes.NewNoScaleDownUnexpectedErrorMsg()},
		{status.ScaleDownInProgress, vistypes.NewNoScaleDownInProgressMsg()},
		{status.ScaleDownInCooldown, vistypes.NewNoScaleDownInBackoffMsg()},
		{status.ScaleDownNotTried, vistypes.NewNoScaleDownNotTriedMsg()},
		{status.ScaleDownNoNodeDeleted, nil},
		{status.ScaleDownNodeDeleteStarted, nil},
	} {
		noScaleDown := throttledNoScaleDown{}
		assert.Equal(t, testCase.expectedReason, noScaleDown.computeTopLevel(testCase.scaleDownResult))
	}
}

func TestUnremovableNodeReasons(t *testing.T) {
	for _, testCase := range []struct {
		unremovableReason simulator.UnremovableReason
		blockingPod       *vistypes.BlockingPod
		expectedReason    *vistypes.Message
	}{
		{unremovableReason: simulator.NoReason, expectedReason: nil},
		{unremovableReason: simulator.NotAutoscaled, expectedReason: nil},
		{unremovableReason: simulator.NotUnneededLongEnough, expectedReason: nil},
		{unremovableReason: simulator.NotUnreadyLongEnough, expectedReason: nil},
		{unremovableReason: simulator.CurrentlyBeingDeleted, expectedReason: nil},
		{unremovableReason: simulator.NotUnderutilized, expectedReason: nil},
		{unremovableReason: simulator.NotUnneededOtherReason, expectedReason: nil},
		{unremovableReason: simulator.RecentlyUnremovable, expectedReason: nil},
		{
			unremovableReason: simulator.MinimalResourceLimitExceeded,
			expectedReason:    vistypes.NewNoScaleDownNodeMinimalResourceLimitsExceededMsg(),
		},
		{
			unremovableReason: simulator.UnexpectedError,
			expectedReason:    vistypes.NewNoScaleDownNodeUnexpectedErrorMsg(),
		},
		{
			unremovableReason: simulator.NodeGroupMinSizeReached,
			expectedReason:    vistypes.NewNoScaleDownNodeNodeGroupMinSizeReachedMsg(),
		},
		{
			unremovableReason: simulator.ScaleDownDisabledAnnotation,
			expectedReason:    vistypes.NewNoScaleDownNodeScaleDownDisabledAnnotationMsg(),
		},
		{
			unremovableReason: simulator.NoPlaceToMovePods,
			expectedReason:    vistypes.NewNoScaleDownNodeNoPlaceToMovePodsMsg(),
		},
		{
			unremovableReason: simulator.BlockedByPod,
			blockingPod:       &vistypes.BlockingPod{Pod: &vistypes.Pod{Name: "test-pod"}, Reason: drain.ControllerNotFound},
			expectedReason:    vistypes.NewNoScaleDownNodePodControllerNotFoundMsg("test-pod"),
		},
		{
			unremovableReason: simulator.BlockedByPod,
			blockingPod:       &vistypes.BlockingPod{Pod: &vistypes.Pod{Name: "test-pod"}, Reason: drain.MinReplicasReached},
			expectedReason:    vistypes.NewNoScaleDownNodePodMinReplicasReachedMsg("test-pod"),
		},
		{
			unremovableReason: simulator.BlockedByPod,
			blockingPod:       &vistypes.BlockingPod{Pod: &vistypes.Pod{Name: "test-pod"}, Reason: drain.NotReplicated},
			expectedReason:    vistypes.NewNoScaleDownNodePodNotBackedByControllerMsg("test-pod"),
		},
		{
			unremovableReason: simulator.BlockedByPod,
			blockingPod:       &vistypes.BlockingPod{Pod: &vistypes.Pod{Name: "test-pod"}, Reason: drain.LocalStorageRequested},
			expectedReason:    vistypes.NewNoScaleDownNodePodHasLocalStorageMsg("test-pod"),
		},
		{
			unremovableReason: simulator.BlockedByPod,
			blockingPod:       &vistypes.BlockingPod{Pod: &vistypes.Pod{Name: "test-pod"}, Reason: drain.NotSafeToEvictAnnotation},
			expectedReason:    vistypes.NewNoScaleDownNodePodNotSafeToEvictAnnotationMsg("test-pod"),
		},
		{
			unremovableReason: simulator.BlockedByPod,
			blockingPod:       &vistypes.BlockingPod{Pod: &vistypes.Pod{Name: "test-pod"}, Reason: drain.UnmovableKubeSystemPod},
			expectedReason:    vistypes.NewNoScaleDownNodePodKubeSystemUnmovableMsg("test-pod"),
		},
		{
			unremovableReason: simulator.BlockedByPod,
			blockingPod:       &vistypes.BlockingPod{Pod: &vistypes.Pod{Name: "test-pod"}, Reason: drain.NotEnoughPdb},
			expectedReason:    vistypes.NewNoScaleDownNodePodNotEnoughPdbMsg("test-pod"),
		},
		{
			unremovableReason: simulator.BlockedByPod,
			blockingPod:       &vistypes.BlockingPod{Pod: &vistypes.Pod{Name: "test-pod"}, Reason: drain.UnexpectedError},
			expectedReason:    vistypes.NewNoScaleDownNodePodUnexpectedErrorMsg("test-pod"),
		},
		{
			unremovableReason: simulator.BlockedByPod,
			blockingPod:       &vistypes.BlockingPod{Pod: &vistypes.Pod{Name: "test-pod"}, Reason: drain.NoReason},
			expectedReason:    vistypes.NewNoScaleDownNodePodUnexpectedErrorMsg("test-pod"),
		},
	} {
		noScaleDown := throttledNoScaleDown{}
		node := &vistypes.Node{Name: "test-node"}
		scaleDownStatus := &vistypes.ScaleDownStatus{
			UnremovableNodes: []*vistypes.UnremovableNode{
				{
					Node:        node,
					Reason:      testCase.unremovableReason,
					BlockingPod: testCase.blockingPod,
				},
			},
		}
		unremovableNodes := noScaleDown.computeUnremovableNodes(scaleDownStatus)

		if testCase.expectedReason == nil {
			assert.Empty(t, unremovableNodes)
		} else {
			expectedUnremovableNode := &vistypes.NodeExplanation{
				Node:   node,
				Reason: testCase.expectedReason,
			}
			assert.ElementsMatch(t, []*vistypes.NodeExplanation{expectedUnremovableNode}, unremovableNodes)
		}
	}
}

func TestMultipleUnremovableNodeReasons(t *testing.T) {
	noScaleDown := throttledNoScaleDown{}
	node1 := &vistypes.Node{Name: "test-node-1"}
	node2 := &vistypes.Node{Name: "test-node-2"}
	node3 := &vistypes.Node{Name: "test-node-3"}
	scaleDownStatus := &vistypes.ScaleDownStatus{
		UnremovableNodes: []*vistypes.UnremovableNode{
			{Node: node1, Reason: simulator.NoPlaceToMovePods},
			{Node: node2, Reason: simulator.NotUnderutilized},
			{Node: node3, Reason: simulator.BlockedByPod, BlockingPod: &vistypes.BlockingPod{Pod: &vistypes.Pod{Name: "test-pod"}, Reason: drain.UnmovableKubeSystemPod}},
		},
	}
	expectedUnremovableNodes := []*vistypes.NodeExplanation{
		{Node: node1, Reason: vistypes.NewNoScaleDownNodeNoPlaceToMovePodsMsg()},
		{Node: node3, Reason: vistypes.NewNoScaleDownNodePodKubeSystemUnmovableMsg("test-pod")},
	}
	unremovableNodes := noScaleDown.computeUnremovableNodes(scaleDownStatus)
	assert.ElementsMatch(t, expectedUnremovableNodes, unremovableNodes)
}
