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

package processors

import (
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/testing/protocmp"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/utilization"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/noscaledown"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/status"
	vispb "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/proto"
	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
)

func nodeExplanationSortFunc(a, b *vispb.NodeExplanation) bool {
	return a.Node.Name < b.Node.Name
}

func assertScaleDownEventsEqual(t *testing.T, expectedEvent, event *vispb.AutoscalerEvent) {
	expectedNodes := expectedEvent.GetDecision().GetScaleDown().NodesToBeRemoved
	nodes := event.GetDecision().GetScaleDown().NodesToBeRemoved
	assert.Equal(t, len(expectedNodes), len(nodes))
	for i, expectedNode := range expectedNodes {
		node := nodes[i]
		assert.ElementsMatch(t, expectedNode.EvictedPods, node.EvictedPods)
		expectedNode.EvictedPods = nil
		node.EvictedPods = nil
	}
	assert.ElementsMatch(t, expectedNodes, nodes)
	expectedEvent.GetDecision().GetScaleDown().NodesToBeRemoved = nil
	event.GetDecision().GetScaleDown().NodesToBeRemoved = nil
	assert.Equal(t, expectedEvent, event)
}

func assertNodePoolDeletedEventsEqual(t *testing.T, expectedEvent, event *vispb.AutoscalerEvent) {
	assert.ElementsMatch(t, expectedEvent.GetDecision().GetNodePoolDeleted().GetNodePoolNames(), event.GetDecision().GetNodePoolDeleted().GetNodePoolNames())
}

func TestProcessScaleDownEvent(t *testing.T) {
	processor := ScaleDownStatusVisibilityProcessor{
		data: NewSharedData(), idGen: new(visibility.MockEventIDGenerator), nodeDeletionInfos: make(map[string]nodeDeletionInfo),
		nodeGroupScaleDownInfos: make(map[string]*nodeGroupScaleDownInfo),
	}

	now := time.Date(2000, 1, 1, 10, 10, 10, 0, time.UTC)
	pod1 := &vistypes.Pod{Name: "pod1", Controller: &vistypes.PodController{Name: "c3", Kind: "ck3", ApiVersion: "v1"}}
	pod2 := &vistypes.Pod{Name: "pod2"}
	mig1 := &vistypes.GkeMig{Id: "mid1", Name: "mig1", Zone: "z1", NodePoolName: "np1"}
	mig2 := &vistypes.GkeMig{Id: "mid2", Name: "mig2", Zone: "z2", NodePoolName: "np2"}

	// There are 2 empty nodes that were decided to be scaled down. Info about both of them should be appropriately transformed into
	// an event. No event results should be emitted.
	input := &vistypes.ScaleDownStatus{
		Result: status.ScaleDownNodeDeleteStarted,
		ScaledDownNodes: []*vistypes.ScaleDownNode{
			{Node: &vistypes.Node{Name: "node1", Mig: mig1, UtilInfo: &utilization.Info{MemUtil: 0.1234567, CpuUtil: 0.999}}, EvictedPods: []*vistypes.Pod{}},
			{Node: &vistypes.Node{Name: "node2", Mig: mig2, UtilInfo: &utilization.Info{MemUtil: 0.1234567, CpuUtil: 0.999}}, EvictedPods: []*vistypes.Pod{}},
		},
		NodeDeleteResults: make(map[string]status.NodeDeleteResult),
	}
	expectedEvent := &vispb.AutoscalerEvent{
		EventOneof: &vispb.AutoscalerEvent_Decision{
			Decision: &vispb.DecisionEvent{
				DecideTime: now.Unix(),
				EventId:    "event_0",
				DecisionOneof: &vispb.DecisionEvent_ScaleDown{
					ScaleDown: &vispb.ScaleDownData{
						NodesToBeRemoved: []*vispb.ScaleDownNode{
							{
								Node: &vispb.Node{
									Name: "node1",
									Mig: &vispb.Mig{
										Name:     "mig1",
										Nodepool: "np1",
										Zone:     "z1",
									},
									CpuRatio: 99,
									MemRatio: 12,
								},
								EvictedPods:           nil,
								EvictedPodsTotalCount: 0,
							},
							{
								Node: &vispb.Node{
									Name: "node2",
									Mig: &vispb.Mig{
										Name:     "mig2",
										Nodepool: "np2",
										Zone:     "z2",
									},
									CpuRatio: 99,
									MemRatio: 12,
								},
								EvictedPods:           nil,
								EvictedPodsTotalCount: 0,
							},
						},
					},
				},
			},
		},
	}
	event := processor.processScaleDownEvent(input, now)
	assertScaleDownEventsEqual(t, expectedEvent, event)
	assert.Empty(t, processor.data.GetNextResults())

	// There is a node with 2 pods that was decided to be drained and scaled down. Info about the 3 entities should be
	// appropriately transformed into an event. No event results should be emitted.
	input = &vistypes.ScaleDownStatus{
		Result: status.ScaleDownNodeDeleteStarted,
		ScaledDownNodes: []*vistypes.ScaleDownNode{
			{Node: &vistypes.Node{Name: "node3", Mig: mig1, UtilInfo: &utilization.Info{MemUtil: 0.1234567, CpuUtil: 0.999}}, EvictedPods: []*vistypes.Pod{pod1, pod2}},
		},
		NodeDeleteResults: make(map[string]status.NodeDeleteResult),
	}
	expectedEvent = &vispb.AutoscalerEvent{
		EventOneof: &vispb.AutoscalerEvent_Decision{
			Decision: &vispb.DecisionEvent{
				DecideTime: now.Unix(),
				EventId:    "event_1",
				DecisionOneof: &vispb.DecisionEvent_ScaleDown{
					ScaleDown: &vispb.ScaleDownData{
						NodesToBeRemoved: []*vispb.ScaleDownNode{{
							Node: &vispb.Node{
								Name: "node3",
								Mig: &vispb.Mig{
									Name:     "mig1",
									Nodepool: "np1",
									Zone:     "z1",
								},
								CpuRatio: 99,
								MemRatio: 12,
							},
							EvictedPods: []*vispb.Pod{
								{Name: "pod1", Controller: &vispb.PodController{Name: "c3", Kind: "ck3", ApiVersion: "v1"}},
								{Name: "pod2"},
							},
							EvictedPodsTotalCount: 2,
						}},
					},
				},
			},
		},
	}
	event = processor.processScaleDownEvent(input, now)
	assertScaleDownEventsEqual(t, expectedEvent, event)
	assert.Empty(t, processor.data.GetNextResults())
}

func createSimpleScaleDownStatus(nodesByMig map[string][]string) *vistypes.ScaleDownStatus {
	var scaledDownNodes []*vistypes.ScaleDownNode

	for migId, nodes := range nodesByMig {
		for _, nodeName := range nodes {
			scaledDownNodes = append(scaledDownNodes, &vistypes.ScaleDownNode{
				Node: &vistypes.Node{
					Name: nodeName,
					Mig:  &vistypes.GkeMig{Id: migId, Name: migId, NodePoolName: "np", Zone: "z"},
				},
				EvictedPods: make([]*vistypes.Pod, 0),
			})
		}
	}

	return &vistypes.ScaleDownStatus{
		Result:            status.ScaleDownNodeDeleteStarted,
		NodeDeleteResults: make(map[string]status.NodeDeleteResult),
		ScaledDownNodes:   scaledDownNodes,
	}
}

func TestProcessNodeDeleteErrors(t *testing.T) {
	now := time.Date(2000, 1, 1, 10, 10, 10, 0, time.UTC)

	processor := ScaleDownStatusVisibilityProcessor{
		data: NewSharedData(), idGen: new(visibility.MockEventIDGenerator), nodeDeletionInfos: make(map[string]nodeDeletionInfo),
		nodeGroupScaleDownInfos: make(map[string]*nodeGroupScaleDownInfo),
	}
	// event_0
	_ = processor.processScaleDownEvent(createSimpleScaleDownStatus(map[string][]string{
		"ng1": {"node1_1", "node1_2"}, // Will fail.
	}), now)
	// event_1
	_ = processor.processScaleDownEvent(createSimpleScaleDownStatus(map[string][]string{
		"ng2": {"node2_1", "node2_2"}, // Will succeed.
	}), now)
	// event_2
	_ = processor.processScaleDownEvent(createSimpleScaleDownStatus(map[string][]string{
		"ng3": {"node3_1", "node3_2"}, // Will fail.
		"ng4": {"node4_1", "node4_2"}, // Will fail.
		"ng7": {"node7_1", "node7_2"}, // Will fail.
	}), now)
	// event_3
	_ = processor.processScaleDownEvent(createSimpleScaleDownStatus(map[string][]string{
		"ng5": {"node5_1", "node5_2"}, // Will fail.
		"ng6": {"node6_1", "node6_2"}, // Will succeed.
	}), now)
	// event_4
	_ = processor.processScaleDownEvent(createSimpleScaleDownStatus(map[string][]string{
		"ng8": {"node8"}, // Will succeed.
		"ng9": {"node9"}, // Will succeed.
	}), now)

	// Simulate finishing groups because actual size == target size, because the target hasn't
	// dropped yet.
	for ngName := range processor.data.GetUnfinishedNodeGroupIds() {
		processor.data.FinishNodeGroup(ngName)
	}
	assert.Empty(t, processor.data.GetNextResults())
	processor.data.PeriodicCleanup()

	// Processor gets first node deletion results.
	input := &vistypes.ScaleDownStatus{
		NodeDeleteResults: map[string]status.NodeDeleteResult{
			// ng1: both nodes failed.
			"node1_1": {ResultType: status.NodeDeleteErrorFailedToMarkToBeDeleted, Err: errors.New("error1")},
			"node1_2": {ResultType: status.NodeDeleteErrorFailedToDelete, Err: errors.New("error2")},

			// ng2: both nodes succeeded.
			"node2_1": {ResultType: status.NodeDeleteOk},
			"node2_2": {ResultType: status.NodeDeleteOk},

			// ng3: one of the nodes failed.
			"node3_1": {ResultType: status.NodeDeleteErrorFailedToDelete, Err: gke.MinSizeReachedError{}},
			"node3_2": {ResultType: status.NodeDeleteOk},

			// ng4: one node failed, no info about the other.
			"node4_1": {ResultType: status.NodeDeleteErrorFailedToDelete, Err: errors.New("error3")},

			// ng5: one node failed, no info about the other.
			"node5_1": {ResultType: status.NodeDeleteErrorFailedToEvictPods, Err: errors.New("error4"), PodEvictionResults: map[string]status.PodEvictionResult{
				"pod1": {Pod: &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1"}}, Err: errors.New("pod_error1")},
				"pod2": {Pod: &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod2"}}, Err: errors.New("pod_error2")},
				"pod3": {Pod: &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod3"}}, TimedOut: true},
				"pod4": {Pod: &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod3"}}},
				"pod5": {Pod: &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod3"}}},
			}},

			// ng6: one node succeeded, no info about the other.
			"node6_1": {ResultType: status.NodeDeleteOk},

			// ng7: one node succeeded, no info about the other.
			"node7_1": {ResultType: status.NodeDeleteOk},

			// ng8: only node succeeded.
			"node8": {ResultType: status.NodeDeleteOk},
		},
	}
	processor.processNodeDeleteResults(input)
	for ngName := range processor.data.GetUnfinishedNodeGroupIds() {
		processor.data.FinishNodeGroup(ngName)
	}
	assert.ElementsMatch(t, []*vispb.EventResult{
		{EventId: "event_0", ErrorMsg: vistypes.NewScaleDownErrorFailedToMarkToBeDeletedMsg("node1_1").Proto()},
		{EventId: "event_0", ErrorMsg: vistypes.NewScaleDownErrorFailedToDeleteNodeOtherMsg("node1_2").Proto()},
		{EventId: "event_1"},
		{EventId: "event_2", ErrorMsg: vistypes.NewScaleDownErrorFailedToDeleteNodeMinSizeReachedMsg("node3_1").Proto()},
	}, processor.data.GetNextResults())
	processor.data.PeriodicCleanup()

	// Processor gets info about the remaining nodes.
	input = &vistypes.ScaleDownStatus{
		NodeDeleteResults: map[string]status.NodeDeleteResult{
			// event4: the other node failed too.
			"node4_2": {ResultType: status.NodeDeleteErrorFailedToDelete, Err: errors.New("error5")},

			// event5: the other node succeeded.
			"node5_2": {ResultType: status.NodeDeleteOk},

			// event6: the other node succeeded too.
			"node6_2": {ResultType: status.NodeDeleteOk},

			// event7: the other node failed.
			"node7_2": {ResultType: status.NodeDeleteErrorFailedToDelete, Err: errors.New("error6")},

			// ng9: only node succeeded.
			"node9": {ResultType: status.NodeDeleteOk},
		},
	}
	processor.processNodeDeleteResults(input)
	for ngName := range processor.data.GetUnfinishedNodeGroupIds() {
		processor.data.FinishNodeGroup(ngName)
	}

	nextResults := processor.data.GetNextResults()
	processor.data.PeriodicCleanup()
	// Sort the pods in event results for determinism.
	for _, result := range nextResults {
		errMsg := result.GetErrorMsg()
		if errMsg != nil && errMsg.GetMessageId() == vistypes.MessageIdToStringMap[vistypes.ScaleDownErrorFailedToEvictPods] {
			sort.Strings(errMsg.GetParameters())
		}
	}

	assert.ElementsMatch(t, []*vispb.EventResult{
		{EventId: "event_2", ErrorMsg: vistypes.NewScaleDownErrorFailedToDeleteNodeOtherMsg("node4_1").Proto()},
		{EventId: "event_2", ErrorMsg: vistypes.NewScaleDownErrorFailedToDeleteNodeOtherMsg("node4_2").Proto()},
		{EventId: "event_2", ErrorMsg: vistypes.NewScaleDownErrorFailedToDeleteNodeOtherMsg("node7_2").Proto()},
		{EventId: "event_3", ErrorMsg: vistypes.NewScaleDownErrorFailedToEvictPodsMsg("node5_1", []string{"pod1", "pod2", "pod3"}).Proto()},
		{EventId: "event_4"},
	}, nextResults)
}

func TestNoScaleDownNegativeEventsFlag(t *testing.T) {
	loggerMock := new(visibility.MockEventLogger)
	loggerMock.On("LogEvent", mock.Anything).Return(nil).Once()

	processor := ScaleDownStatusVisibilityProcessor{
		logger:                  loggerMock,
		opts:                    visibility.VisibilityOptions{EmitNoScaleDownEvents: false, IncludePerMigStatuses: false},
		data:                    NewSharedData(),
		idGen:                   new(visibility.MockEventIDGenerator),
		nodeDeletionInfos:       make(map[string]nodeDeletionInfo),
		nodeGroupScaleDownInfos: make(map[string]*nodeGroupScaleDownInfo),
		noScaleDown:             noscaledown.NewNoScaleDown(time.Minute),
	}

	unremovableNodes := []*status.UnremovableNode{
		{Node: &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}}, Reason: simulator.NoPlaceToMovePods},
	}

	// Check that negative event is not logged with the flag turned off.
	processor.Process(&context.AutoscalingContext{}, &status.ScaleDownStatus{
		Result:           status.ScaleDownInCooldown,
		UnremovableNodes: unremovableNodes,
	})
	loggerMock.AssertNumberOfCalls(t, "LogEvent", 0)

	// Turn the flag on and check that the negative event is correctly logged.
	processor.opts.EmitNoScaleDownEvents = true
	processor.Process(&context.AutoscalingContext{}, &status.ScaleDownStatus{
		Result:           status.ScaleDownInCooldown,
		UnremovableNodes: unremovableNodes,
	})

	loggerMock.AssertExpectations(t)
	loggedEvent := loggerMock.Calls[0].Arguments.Get(0).(*vispb.AutoscalerEvent)
	noDecisionStatus := loggedEvent.GetNoDecisionStatus()
	assert.NotNil(t, noDecisionStatus)
	noScaleDown := noDecisionStatus.GetNoScaleDown()
	assert.NotNil(t, noScaleDown)

	if diff := cmp.Diff(vistypes.NewNoScaleDownInBackoffMsg().Proto(), noScaleDown.Reason, protocmp.Transform()); diff != "" {
		t.Errorf("NoScaleDown Reason diff (-want +got):\n%s", diff)
	}
}

func createNodePoolDeletedEvent(now time.Time, id string, nodePoolNames []string) *vispb.AutoscalerEvent {
	return &vispb.AutoscalerEvent{
		EventOneof: &vispb.AutoscalerEvent_Decision{
			Decision: &vispb.DecisionEvent{
				DecideTime: now.Unix(),
				EventId:    id,
				DecisionOneof: &vispb.DecisionEvent_NodePoolDeleted{
					NodePoolDeleted: &vispb.NodePoolDeletedData{
						NodePoolNames: nodePoolNames,
					},
				},
			},
		},
	}
}

func TestNodePoolDeletedEvent(t *testing.T) {
	now := time.Date(2000, 1, 1, 10, 10, 10, 0, time.UTC)
	mig1 := &vistypes.GkeMig{Id: "mid1", Name: "m1", Zone: "z1", NodePoolName: "np1"}
	mig2 := &vistypes.GkeMig{Id: "mid2", Name: "m2", Zone: "z2", NodePoolName: "np1"}
	mig3 := &vistypes.GkeMig{Id: "mid3", Name: "m3", Zone: "z1", NodePoolName: "np2"}

	for _, testCase := range []struct {
		name          string
		removedMigs   []*vistypes.GkeMig
		expectedEvent *vispb.AutoscalerEvent
	}{
		{
			name:          "No removed node groups (nil slice)",
			removedMigs:   nil,
			expectedEvent: nil,
		},
		{
			name:          "No removed node groups (empty slice)",
			removedMigs:   []*vistypes.GkeMig{},
			expectedEvent: nil,
		},
		{
			name:          "Single removed node group",
			removedMigs:   []*vistypes.GkeMig{mig1},
			expectedEvent: createNodePoolDeletedEvent(now, "event_0", []string{"np1"}),
		},
		{
			name:          "Multiple removed node groups from the same node pool",
			removedMigs:   []*vistypes.GkeMig{mig1, mig2},
			expectedEvent: createNodePoolDeletedEvent(now, "event_0", []string{"np1"}),
		},
		{
			name:          "Multiple removed node groups from different node pools",
			removedMigs:   []*vistypes.GkeMig{mig1, mig2, mig3},
			expectedEvent: createNodePoolDeletedEvent(now, "event_0", []string{"np1", "np2"}),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			processor := ScaleDownStatusVisibilityProcessor{data: NewSharedData(), idGen: new(visibility.MockEventIDGenerator)}
			event := processor.processNodePoolDeletedEvent(&vistypes.ScaleDownStatus{RemovedMigs: testCase.removedMigs}, now)
			assertNodePoolDeletedEventsEqual(t, testCase.expectedEvent, event)
		})
	}
}

func TestProcessNoScaleDownEvent(t *testing.T) {
	ctx := &context.AutoscalingContext{}
	scaleDownStatus := &status.ScaleDownStatus{Result: 1337}
	vizScaleDownStatus := &vistypes.ScaleDownStatus{Result: 1337}

	noScaleDownMock := noscaledown.NewNoScaleDownMock()
	loggerMock := new(visibility.MockEventLogger)
	processor := ScaleDownStatusVisibilityProcessor{
		logger:                  loggerMock,
		opts:                    visibility.VisibilityOptions{EmitNoScaleDownEvents: true},
		data:                    NewSharedData(),
		idGen:                   new(visibility.MockEventIDGenerator),
		noScaleDown:             noScaleDownMock,
		nodeGroupScaleDownInfos: make(map[string]*nodeGroupScaleDownInfo),
		nodeDeletionInfos:       make(map[string]nodeDeletionInfo),
	}

	// Assert that nothing is logged if NoScaleDown doesn't return any reasons.
	noScaleDownMock.On("GetNewReasons", vizScaleDownStatus, mock.Anything).Return(&noscaledown.Reasons{}).Once()
	processor.Process(ctx, scaleDownStatus)

	// Assert that the correct event is logged if NoScaleDown does return reasons.
	topLevel := &vistypes.Message{Id: 1, Params: []string{"a", "b", "c"}}
	unremovableNode1 := &vistypes.NodeExplanation{
		Node:   &vistypes.Node{Name: "node-1"},
		Reason: vistypes.NewNoScaleDownNodeNoPlaceToMovePodsMsg(),
	}
	unremovableNode2 := &vistypes.NodeExplanation{
		Node: &vistypes.Node{
			Name:     "node-2",
			Mig:      &vistypes.GkeMig{Name: "mig-1", NodePoolName: "np-1", Zone: "z-1"},
			UtilInfo: &utilization.Info{CpuUtil: 0.123, MemUtil: 0.456},
		},
		Reason: vistypes.NewNoScaleDownNodePodNotBackedByControllerMsg("pod-1"),
	}
	returnedReasons := &noscaledown.Reasons{
		TopLevel:         topLevel,
		UnremovableNodes: []*vistypes.NodeExplanation{unremovableNode1, unremovableNode2},
	}
	noScaleDownMock.On("GetNewReasons", vizScaleDownStatus, mock.Anything).Return(returnedReasons).Once()
	loggerMock.On("LogEvent", mock.Anything).Return(nil).Once()
	noScaleDownMock.On("MarkReasonsReported", returnedReasons, mock.Anything).Return().Once()
	processor.Process(ctx, scaleDownStatus)

	loggedNoScaleDownData := loggerMock.Calls[0].Arguments.Get(0).(*vispb.AutoscalerEvent).GetNoDecisionStatus().GetNoScaleDown()
	if diff := cmp.Diff(topLevel.Proto(), loggedNoScaleDownData.GetReason(), protocmp.Transform()); diff != "" {
		t.Errorf("GetReason() diff (-want +got):\n%s", diff)
	}

	if diff := cmp.Diff([]*vispb.NodeExplanation{unremovableNode1.Proto(), unremovableNode2.Proto()}, loggedNoScaleDownData.GetNodes(), cmpopts.SortSlices(nodeExplanationSortFunc), protocmp.Transform()); diff != "" {
		t.Errorf("NodeExplanation() diff (-want +got):\n%s", diff)
	}

	// Assert that if the logger fails to log, the reasons are not marked as reported.
	noScaleDownMock.On("GetNewReasons", vizScaleDownStatus, mock.Anything).Return(returnedReasons).Once()
	loggerMock.On("LogEvent", mock.Anything).Return(fmt.Errorf("Some error")).Once()
	processor.Process(ctx, scaleDownStatus)

	loggerMock.AssertExpectations(t)
	noScaleDownMock.AssertExpectations(t)
}
