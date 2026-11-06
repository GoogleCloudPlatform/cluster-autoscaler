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
	"context"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube_record "k8s.io/client-go/tools/record"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/events"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/noscaleup"
	vispb "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/proto"
	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/callbacks"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
)

func migExplanationSortFunc(a, b *vispb.MigExplanation) bool {
	return a.Mig.Name < b.Mig.Name
}
func parameterizedMessageSortFunc(a, b *vispb.ParametrizedMessage) bool {
	return a.MessageId < b.MessageId
}

func assertScaleUpEventsEqual(t *testing.T, expectedEvent, event *vispb.AutoscalerEvent) {
	assert.ElementsMatch(t, expectedEvent.GetDecision().GetScaleUp().IncreasedMigs, event.GetDecision().GetScaleUp().IncreasedMigs)
	assert.ElementsMatch(t, expectedEvent.GetDecision().GetScaleUp().TriggeringPods, event.GetDecision().GetScaleUp().TriggeringPods)
	expectedEvent.GetDecision().GetScaleUp().IncreasedMigs = nil
	expectedEvent.GetDecision().GetScaleUp().TriggeringPods = nil
	event.GetDecision().GetScaleUp().IncreasedMigs = nil
	event.GetDecision().GetScaleUp().TriggeringPods = nil
	assert.Equal(t, expectedEvent, event)
}

func assertNodePoolCreatedEventsEqual(t *testing.T, expectedEvent, event *vispb.AutoscalerEvent) {
	npcEvent := event.GetDecision().GetNodePoolCreated()
	expectedNpcEvent := expectedEvent.GetDecision().GetNodePoolCreated()
	assert.Equal(t, expectedNpcEvent.GetTriggeringScaleUpId(), npcEvent.GetTriggeringScaleUpId())
	assert.Equal(t, len(expectedNpcEvent.GetNodePools()), len(npcEvent.GetNodePools()))

	expectedNodePools := make(map[string]*vispb.NodePool)
	for _, nodePool := range expectedNpcEvent.GetNodePools() {
		expectedNodePools[nodePool.GetName()] = nodePool
	}

	for _, nodePool := range npcEvent.GetNodePools() {
		expectedNodePool, found := expectedNodePools[nodePool.GetName()]
		assert.True(t, found)
		assert.Equal(t, expectedNodePool.GetName(), nodePool.GetName())
		assert.ElementsMatch(t, expectedNodePool.GetMigs(), nodePool.GetMigs())
	}
}

func TestProcessScaleUpEvent(t *testing.T) {
	processor := ScaleUpStatusVisibilityProcessor{logger: nil, data: NewSharedData(), idGen: new(visibility.MockEventIDGenerator)}

	now := time.Date(2000, 1, 1, 10, 10, 10, 0, time.UTC)
	mig1 := &vistypes.GkeMig{Id: "mid1", Name: "m1", Zone: "z1", NodePoolName: "np1"}
	mig2 := &vistypes.GkeMig{Id: "mid2", Name: "m2", Zone: "z2", NodePoolName: "np2"}

	// There are 2 pods that triggered the scale up of 2 MIGs. Info about all 4 should be appropriately transformed into
	// an event and shared data should denote that the 2 MIGs now have the new event as pending and that 2 MIGs are waiting
	// for that event.
	input := &vistypes.ScaleUpStatus{
		Result: status.ScaleUpSuccessful,
		ScaleUpMigs: []*vistypes.ScaleUpMig{
			{Mig: mig1, CurrentSize: 13, NewSize: 37},
			{Mig: mig2, CurrentSize: 3, NewSize: 14},
		},
		PodsTriggeredScaleUp: []*vistypes.Pod{
			{Name: "p1", Controller: &vistypes.PodController{Name: "c3", Kind: "ck3", ApiVersion: "v1"}},
			{Name: "p2"},
		},
	}
	expectedEvent := &vispb.AutoscalerEvent{
		EventOneof: &vispb.AutoscalerEvent_Decision{
			Decision: &vispb.DecisionEvent{
				DecideTime: now.Unix(),
				EventId:    "event_0",
				DecisionOneof: &vispb.DecisionEvent_ScaleUp{
					ScaleUp: &vispb.ScaleUpData{
						IncreasedMigs: []*vispb.ScaleUpMig{
							{
								Mig: &vispb.Mig{
									Name:     "m1",
									Nodepool: "np1",
									Zone:     "z1",
								},
								RequestedNodes: 24,
							},
							{
								Mig: &vispb.Mig{
									Name:     "m2",
									Nodepool: "np2",
									Zone:     "z2",
								},
								RequestedNodes: 11,
							},
						},
						TriggeringPods: []*vispb.Pod{
							{Name: "p1", Controller: &vispb.PodController{Name: "c3", Kind: "ck3", ApiVersion: "v1"}},
							{Name: "p2"},
						},
						TriggeringPodsTotalCount: 2,
					},
				},
			},
		},
	}
	event := processor.processScaleUpEvent(input, now)
	assertScaleUpEventsEqual(t, expectedEvent, event)

	// Another pod triggered the scale up of the first MIG, before the previous one finished. Shared data should denote that
	// there are now 2 pending events on this MIG, 1 on the other and that event_0 still waits for 2 MIGs and event_1 waits
	// for 1 MIG.
	input = &vistypes.ScaleUpStatus{
		Result:               status.ScaleUpSuccessful,
		ScaleUpMigs:          []*vistypes.ScaleUpMig{{Mig: mig1, CurrentSize: 37, NewSize: 137}},
		PodsTriggeredScaleUp: []*vistypes.Pod{{Name: "p2"}},
	}
	expectedEvent = &vispb.AutoscalerEvent{
		EventOneof: &vispb.AutoscalerEvent_Decision{
			Decision: &vispb.DecisionEvent{
				DecideTime: now.Add(time.Minute).Unix(),
				EventId:    "event_1",
				DecisionOneof: &vispb.DecisionEvent_ScaleUp{
					ScaleUp: &vispb.ScaleUpData{
						IncreasedMigs: []*vispb.ScaleUpMig{
							{
								Mig: &vispb.Mig{
									Name:     "m1",
									Nodepool: "np1",
									Zone:     "z1",
								},
								RequestedNodes: 100,
							},
						},
						TriggeringPods:           []*vispb.Pod{{Name: "p2"}},
						TriggeringPodsTotalCount: 1,
					},
				},
			},
		},
	}
	event = processor.processScaleUpEvent(input, now.Add(time.Minute))
	assertScaleUpEventsEqual(t, expectedEvent, event)
}

func TestProcessNoScaleUpEvent(t *testing.T) {
	mig1 := &vistypes.GkeMig{Id: "mig1", Name: "mig1", NodePoolName: "np1", Zone: "z1"}
	mig2 := &vistypes.GkeMig{Id: "mig2", Name: "mig2", NodePoolName: "np2", Zone: "z1"}
	mig3 := &vistypes.GkeMig{Id: "mig3", Name: "mig3", NodePoolName: "np3", Zone: "z1"}
	mig4 := &vistypes.GkeMig{Id: "mig4", Name: "mig4", NodePoolName: "np4", Zone: "z1"}
	mig5 := &vistypes.GkeMig{Id: "mig5", Name: "mig5", NodePoolName: "np5", Zone: "z1"}

	pod1 := &vistypes.Pod{Name: "pod1"}
	pod2 := &vistypes.Pod{Name: "pod2"}

	m1 := &vistypes.Message{Id: 1, Params: []string{"p11", "p21"}}
	m2 := &vistypes.Message{Id: 2, Params: []string{"p21", "p22"}}
	m3 := &vistypes.Message{Id: 13, Params: []string{"p131"}}
	m4 := &vistypes.Message{Id: 15, Params: []string{"p151"}}
	m5 := &vistypes.Message{Id: 23, Params: []string{"p231"}}
	m6 := &vistypes.Message{Id: 25, Params: []string{"p251"}}
	m7 := &vistypes.Message{Id: 100, Params: []string{"p101", "p102"}}
	m8 := &vistypes.Message{Id: 101, Params: []string{"p201", "p202"}}
	m9 := &vistypes.Message{Id: 230}
	m10 := &vistypes.Message{Id: 1000, Params: []string{"p1010"}}

	migReason1 := &vistypes.MigExplanation{Mig: mig1, Reason: m3}
	migReason2 := &vistypes.MigExplanation{Mig: mig2, Reason: m4}
	migReason3 := &vistypes.MigExplanation{Mig: mig3, Reason: m5}
	migReason4 := &vistypes.MigExplanation{Mig: mig4, Reason: m6}
	migReason5 := &vistypes.MigExplanation{Mig: mig5, Reason: m9}

	for _, tc := range []struct {
		name              string
		simulationEnabled bool
		globalSkippedMigs []*vistypes.MigExplanation
		podGroup1         *vistypes.PodGroupExplanation
		podGroup2         *vistypes.PodGroupExplanation
	}{
		{
			name:              "SkippedMigsSimulationDisabled",
			simulationEnabled: false,
			globalSkippedMigs: []*vistypes.MigExplanation{migReason1, migReason2},
			podGroup1: &vistypes.PodGroupExplanation{
				SamplePod: pod1,
				PodCount:  1337,
				MigReasons: map[string]*vistypes.MigExplanation{
					"mig3": migReason3,
					"mig4": migReason4,
				},
				NapReasons: []*vistypes.Message{m7, m8},
			},
			podGroup2: &vistypes.PodGroupExplanation{
				SamplePod:  pod2,
				PodCount:   2550,
				MigReasons: map[string]*vistypes.MigExplanation{"mig5": migReason5},
				NapReasons: []*vistypes.Message{m10},
			},
		},
		{
			name:              "SkippedMigsSimulationEnabled",
			simulationEnabled: true,
			globalSkippedMigs: nil,
			podGroup1: &vistypes.PodGroupExplanation{
				SamplePod: pod1,
				PodCount:  1337,
				MigReasons: map[string]*vistypes.MigExplanation{
					"mig3": migReason3,
					"mig4": migReason4,
				},
				NapReasons: []*vistypes.Message{m7, m8},
				SkippedMigReasons: map[string]*vistypes.MigExplanation{
					"mig1": migReason1,
				},
			},
			podGroup2: &vistypes.PodGroupExplanation{
				SamplePod:         pod2,
				PodCount:          2550,
				MigReasons:        map[string]*vistypes.MigExplanation{"mig5": migReason5},
				NapReasons:        []*vistypes.Message{m10},
				SkippedMigReasons: map[string]*vistypes.MigExplanation{"mig2": migReason2},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			napStatus := autoprovisioning.NewProcessingStatus()
			napStatus.SetResult(autoprovisioning.ProcessingResult(1337))
			vizNapStatus := &vistypes.NapStatus{
				Result:          1337,
				DisregardedMigs: map[autoprovisioning.NodeGroupOptions]autoprovisioning.NodeGroupDisregardedReason{},
				PodStatuses:     map[string]autoprovisioning.PodProcessingStatus{},
			}
			vizNapStatus.Result = autoprovisioning.ProcessingResult(1337)

			scaleUpStatus := &status.ScaleUpStatus{
				Result: 1337,
				PodsRemainUnschedulable: []status.NoScaleUpInfo{
					{Pod: &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1"}}},
				},
			}
			vizScaleUpStatus, err := vistypes.ConvertScaleUpStatus(scaleUpStatus)
			assert.NoError(t, err)

			ctx := &ca_context.AutoscalingContext{
				ProcessorCallbacks: callbacks.NewTestProcessorCallbacks(),
			}
			ctx.ScaleUpSimulationForSkippedNodeGroupsEnabled = tc.simulationEnabled
			ctx.ProcessorCallbacks.SetExtraValue(autoprovisioning.ProcessingStatusContextKey, napStatus)

			noScaleUpMock := noscaleup.NewNoScaleUpMock()
			loggerMock := new(visibility.MockEventLogger)
			processor := ScaleUpStatusVisibilityProcessor{
				logger:    loggerMock,
				opts:      visibility.VisibilityOptions{EmitNoScaleUpEvents: true},
				data:      NewSharedData(),
				idGen:     new(visibility.MockEventIDGenerator),
				noScaleUp: noScaleUpMock,
			}

			// Assert that nothing is logged if NoScaleUp doesn't return any reasons.
			noScaleUpMock.On("GetNewReasons", vizScaleUpStatus, vizNapStatus, mock.Anything).Return(&noscaleup.Reasons{}).Once()
			processor.Process(context.TODO(), ctx, scaleUpStatus)

			// Assert that the correct event is logged if NoScaleUp does return reasons.
			returnedReasons := &noscaleup.Reasons{
				TopLevel:    m1,
				TopLevelNap: m2,
				SkippedMigs: tc.globalSkippedMigs,
				PodGroups:   []*vistypes.PodGroupExplanation{tc.podGroup1, tc.podGroup2},
			}
			noScaleUpMock.On("GetNewReasons", vizScaleUpStatus, vizNapStatus, mock.Anything).Return(returnedReasons).Once()
			loggerMock.On("LogEvent", mock.Anything).Return(nil).Once()
			noScaleUpMock.On("MarkReasonsReported", returnedReasons, mock.Anything).Return().Once()
			processor.Process(context.TODO(), ctx, scaleUpStatus)

			loggedNoScaleUpData := loggerMock.Calls[0].Arguments.Get(0).(*vispb.AutoscalerEvent).GetNoDecisionStatus().GetNoScaleUp()

			if diff := cmp.Diff(m1.Proto(), loggedNoScaleUpData.GetReason(), protocmp.Transform()); diff != "" {
				t.Errorf("GetReason() diff (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(m2.Proto(), loggedNoScaleUpData.GetNapFailureReason(), protocmp.Transform()); diff != "" {
				t.Errorf("GetNapFailureReason() diff (-want +got):\n%s", diff)
			}

			var expectedGlobalSkippedMigsProto []*vispb.MigExplanation
			for _, m := range tc.globalSkippedMigs {
				expectedGlobalSkippedMigsProto = append(expectedGlobalSkippedMigsProto, m.Proto())
			}
			if diff := cmp.Diff(expectedGlobalSkippedMigsProto, loggedNoScaleUpData.GetSkippedMigs(), cmpopts.SortSlices(migExplanationSortFunc), protocmp.Transform()); diff != "" {
				t.Errorf("GetSkippedMigs() diff (-want +got):\n%s", diff)
			}

			assert.Equal(t, int32(2), loggedNoScaleUpData.GetUnhandledPodGroupsTotalCount())
			for _, loggedPodGroupProto := range loggedNoScaleUpData.GetUnhandledPodGroups() {
				var expectedPodGroup *vistypes.PodGroupExplanation
				if loggedPodGroupProto.GetPodGroup().GetSamplePod().GetName() == "pod1" {
					expectedPodGroup = tc.podGroup1
				} else if loggedPodGroupProto.GetPodGroup().GetSamplePod().GetName() == "pod2" {
					expectedPodGroup = tc.podGroup2
				}
				assert.NotNil(t, expectedPodGroup)
				expectedPodGroupProto := expectedPodGroup.Proto()

				if diff := cmp.Diff(expectedPodGroupProto.GetPodGroup(), loggedPodGroupProto.GetPodGroup(), protocmp.Transform()); diff != "" {
					t.Errorf("GetPodGroup() diff (-want +got):\n%s", diff)
				}
				if diff := cmp.Diff(expectedPodGroupProto.GetNapFailureReasons(), loggedPodGroupProto.GetNapFailureReasons(), cmpopts.SortSlices(parameterizedMessageSortFunc), protocmp.Transform()); diff != "" {
					t.Errorf("GetNapFailureReasons() diff (-want +got):\n%s", diff)
				}
				if diff := cmp.Diff(expectedPodGroupProto.GetRejectedMigs(), loggedPodGroupProto.GetRejectedMigs(), cmpopts.SortSlices(migExplanationSortFunc), protocmp.Transform()); diff != "" {
					t.Errorf("GetRejectedMigs() diff (-want +got):\n%s", diff)
				}
				if diff := cmp.Diff(expectedPodGroupProto.GetSkippedMigs(), loggedPodGroupProto.GetSkippedMigs(), cmpopts.SortSlices(migExplanationSortFunc), protocmp.Transform()); diff != "" {
					t.Errorf("GetSkippedMigs() diff (-want +got):\n%s", diff)
				}
			}

			// Assert that if the logger fails to log, the reasons are not marked as reported.
			noScaleUpMock.On("GetNewReasons", vizScaleUpStatus, vizNapStatus, mock.Anything).Return(returnedReasons).Once()
			loggerMock.On("LogEvent", mock.Anything).Return(fmt.Errorf("Some error")).Once()
			processor.Process(context.TODO(), ctx, scaleUpStatus)

			loggerMock.AssertExpectations(t)
			noScaleUpMock.AssertExpectations(t)
		})
	}
}

func TestEmitNoScaleUpEvent_SkipsWhenNoUnschedulablePods(t *testing.T) {
	napStatus := autoprovisioning.NewProcessingStatus()
	napStatus.SetResult(autoprovisioning.ProcessingResult(1337))

	ctx := &ca_context.AutoscalingContext{
		ProcessorCallbacks: callbacks.NewTestProcessorCallbacks(),
	}
	ctx.ProcessorCallbacks.SetExtraValue(autoprovisioning.ProcessingStatusContextKey, napStatus)

	noScaleUpMock := noscaleup.NewNoScaleUpMock()
	loggerMock := new(visibility.MockEventLogger)
	processor := ScaleUpStatusVisibilityProcessor{
		logger:    loggerMock,
		opts:      visibility.VisibilityOptions{EmitNoScaleUpEvents: true},
		data:      NewSharedData(),
		idGen:     new(visibility.MockEventIDGenerator),
		noScaleUp: noScaleUpMock,
	}

	// When there are no unschedulable pods (e.g., successful scale-up or async NAP
	// InitializeNodeGroup callback), emitNoScaleUpEvent returns early without
	// reading ProcessorCallbacks or invoking NoScaleUp.GetNewReasons.
	scaleUpStatus := &status.ScaleUpStatus{
		Result:                  status.ScaleUpSuccessful,
		PodsRemainUnschedulable: []status.NoScaleUpInfo{},
	}
	processor.Process(context.TODO(), ctx, scaleUpStatus)

	noScaleUpMock.AssertNotCalled(t, "GetNewReasons", mock.Anything, mock.Anything, mock.Anything)
	loggerMock.AssertNotCalled(t, "LogEvent", mock.Anything)
}

func TestNoScaleUpNegativeEventsFlag(t *testing.T) {
	loggerMock := new(visibility.MockEventLogger)
	loggerMock.On("LogEvent", mock.Anything).Return(nil).Once()

	ctx := &ca_context.AutoscalingContext{ProcessorCallbacks: callbacks.NewTestProcessorCallbacks()}
	processor := ScaleUpStatusVisibilityProcessor{
		logger:    loggerMock,
		opts:      visibility.VisibilityOptions{EmitNoScaleUpEvents: false, IncludePerMigStatuses: false},
		data:      NewSharedData(),
		idGen:     new(visibility.MockEventIDGenerator),
		noScaleUp: noscaleup.NewNoScaleUp(time.Minute, false, nil),
	}

	unschedulablePods := []status.NoScaleUpInfo{
		{Pod: &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1"}}},
	}

	// Check that negative event is not logged with the flag turned off.
	processor.Process(context.TODO(), ctx, &status.ScaleUpStatus{Result: status.ScaleUpNotTried, PodsRemainUnschedulable: unschedulablePods})
	loggerMock.AssertNumberOfCalls(t, "LogEvent", 0)

	// Turn the flag on and check that the negative event is correctly logged.
	processor.opts.EmitNoScaleUpEvents = true
	processor.Process(context.TODO(), ctx, &status.ScaleUpStatus{Result: status.ScaleUpNotTried, PodsRemainUnschedulable: unschedulablePods})

	loggerMock.AssertExpectations(t)
	loggedEvent := loggerMock.Calls[0].Arguments.Get(0).(*vispb.AutoscalerEvent)
	noDecisionStatus := loggedEvent.GetNoDecisionStatus()
	assert.NotNil(t, noDecisionStatus)
	noScaleUpDecision := noDecisionStatus.GetNoScaleUp()
	assert.NotNil(t, noScaleUpDecision)

	if diff := cmp.Diff(vistypes.NewNoScaleUpNotTriedMsg().Proto(), noScaleUpDecision.Reason, protocmp.Transform()); diff != "" {
		t.Errorf("NoScaleUpNotTriedMsg diff (-want +got):\n%s", diff)
	}
}

func createNodePoolCreatedEvent(now time.Time, id string, data *vispb.NodePoolCreatedData) *vispb.AutoscalerEvent {
	return &vispb.AutoscalerEvent{
		EventOneof: &vispb.AutoscalerEvent_Decision{
			Decision: &vispb.DecisionEvent{
				DecideTime: now.Unix(),
				EventId:    id,
				DecisionOneof: &vispb.DecisionEvent_NodePoolCreated{
					NodePoolCreated: data,
				},
			},
		},
	}
}

func TestNodePoolCreatedEvent(t *testing.T) {
	now := time.Date(2000, 1, 1, 10, 10, 10, 0, time.UTC)
	mig1 := &vistypes.GkeMig{Id: "mid1", Name: "m1", Zone: "z1", NodePoolName: "np1"}
	mig2 := &vistypes.GkeMig{Id: "mid2", Name: "m2", Zone: "z2", NodePoolName: "np1"}
	mig3 := &vistypes.GkeMig{Id: "mid3", Name: "m3", Zone: "z3", NodePoolName: "np1"}
	mig4 := &vistypes.GkeMig{Id: "mid4", Name: "m4", Zone: "z1", NodePoolName: "np2"}

	for _, testCase := range []struct {
		name                string
		triggeringScaleUpId string
		createdNodePools    [][]*vistypes.GkeMig
		expectedEvent       *vispb.AutoscalerEvent
	}{
		{
			name:                "No create node group results (nil slice)",
			triggeringScaleUpId: "scale-up-id-1",
			createdNodePools:    nil,
			expectedEvent:       nil,
		},
		{
			name:                "No create node group results (empty slice)",
			triggeringScaleUpId: "scale-up-id-1",
			createdNodePools:    [][]*vistypes.GkeMig{},
			expectedEvent:       nil,
		},
		{
			name:                "Only empty create node group results",
			triggeringScaleUpId: "scale-up-id-1",
			createdNodePools:    [][]*vistypes.GkeMig{{}, {}},
			expectedEvent:       nil,
		},
		{
			name:                "Single create node group result with single MIG",
			triggeringScaleUpId: "scale-up-id-1",
			createdNodePools:    [][]*vistypes.GkeMig{{mig1}},
			expectedEvent: createNodePoolCreatedEvent(now, "event_0", &vispb.NodePoolCreatedData{
				TriggeringScaleUpId: "scale-up-id-1",
				NodePools: []*vispb.NodePool{
					{
						Name: "np1",
						Migs: []*vispb.Mig{
							{Name: "m1", Nodepool: "np1", Zone: "z1"},
						},
					},
				},
			}),
		},
		{
			name:                "Single create node group result with multiple MIGs",
			triggeringScaleUpId: "scale-up-id-1",
			createdNodePools: [][]*vistypes.GkeMig{
				{mig1, mig2, mig3},
			},
			expectedEvent: createNodePoolCreatedEvent(now, "event_0", &vispb.NodePoolCreatedData{
				TriggeringScaleUpId: "scale-up-id-1",
				NodePools: []*vispb.NodePool{
					{
						Name: "np1",
						Migs: []*vispb.Mig{
							{Name: "m1", Nodepool: "np1", Zone: "z1"},
							{Name: "m2", Nodepool: "np1", Zone: "z2"},
							{Name: "m3", Nodepool: "np1", Zone: "z3"},
						},
					},
				},
			}),
		},
		{
			name:                "Both empty and non-empty create node group results",
			triggeringScaleUpId: "scale-up-id-1",
			createdNodePools: [][]*vistypes.GkeMig{
				{mig1, mig2, mig3},
				{},
			},
			expectedEvent: createNodePoolCreatedEvent(now, "event_0", &vispb.NodePoolCreatedData{
				TriggeringScaleUpId: "scale-up-id-1",
				NodePools: []*vispb.NodePool{
					{
						Name: "np1",
						Migs: []*vispb.Mig{
							{Name: "m1", Nodepool: "np1", Zone: "z1"},
							{Name: "m2", Nodepool: "np1", Zone: "z2"},
							{Name: "m3", Nodepool: "np1", Zone: "z3"},
						},
					},
				},
			}),
		},
		{
			name:                "Multiple create node group results",
			triggeringScaleUpId: "scale-up-id-1",
			createdNodePools: [][]*vistypes.GkeMig{
				{mig1, mig2, mig3},
				{mig4},
			},
			expectedEvent: createNodePoolCreatedEvent(now, "event_0", &vispb.NodePoolCreatedData{
				TriggeringScaleUpId: "scale-up-id-1",
				NodePools: []*vispb.NodePool{
					{
						Name: "np1",
						Migs: []*vispb.Mig{
							{Name: "m1", Nodepool: "np1", Zone: "z1"},
							{Name: "m2", Nodepool: "np1", Zone: "z2"},
							{Name: "m3", Nodepool: "np1", Zone: "z3"},
						},
					},
					{
						Name: "np2",
						Migs: []*vispb.Mig{
							{Name: "m4", Nodepool: "np2", Zone: "z1"},
						},
					},
				},
			}),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			processor := ScaleUpStatusVisibilityProcessor{data: NewSharedData(), idGen: new(visibility.MockEventIDGenerator)}
			event := processor.processNodePoolCreatedEvent(&vistypes.ScaleUpStatus{CreatedNodePools: testCase.createdNodePools}, testCase.triggeringScaleUpId, now)
			assertNodePoolCreatedEventsEqual(t, testCase.expectedEvent, event)
		})
	}
}

func TestSpreadingEvents(t *testing.T) {
	loggerMock := new(visibility.MockEventLogger)
	processor := ScaleUpStatusVisibilityProcessor{
		logger:    loggerMock,
		opts:      visibility.VisibilityOptions{EmitNoScaleUpEvents: true},
		data:      NewSharedData(),
		idGen:     new(visibility.MockEventIDGenerator),
		noScaleUp: noscaleup.NewNoScaleUp(10*time.Minute, false, nil),
	}
	ctx := &ca_context.AutoscalingContext{ProcessorCallbacks: callbacks.NewTestProcessorCallbacks()}
	podGroupsLen := 80
	consideredMigsLen := 80
	noScaleUpInfos, consideredMigs := createNoScaleUpInfos(podGroupsLen, consideredMigsLen, false, false)
	s := vistypes.ScaleUpStatus{
		ConsideredMigs: consideredMigs,
		NoScaleUpInfos: noScaleUpInfos,
	}
	now := time.Now()
	coveredPodsxMig := make(map[string]bool)
	for i := 0; i < 7; i++ { //all of the information should be emitted in at most 7 batches.
		ev, callback := processor.processNoScaleUpEvent(ctx, &s, now)
		if callback != nil {
			callback(now)
		}
		if ev != nil {
			noDecisionStatus, ok := ev.EventOneof.(*vispb.AutoscalerEvent_NoDecisionStatus)
			if !ok {
				t.Fatalf("Got unexpected type of event: %T, expected AutoscalerEvent_NoDecisionStatus", ev.EventOneof)
			}
			noScaleUpEv, ok := noDecisionStatus.NoDecisionStatus.KindOneof.(*vispb.NoDecisionStatus_NoScaleUp)
			if !ok {
				t.Fatalf("Got unexpected type of event: %T, expected NoDecisionStatus_NoScaleUp", noDecisionStatus.NoDecisionStatus.KindOneof)
			}
			if pgLen := len(noScaleUpEv.NoScaleUp.UnhandledPodGroups); pgLen > 50 {
				t.Errorf("Unexpected length of unhandled pod groups in no scale up event, got: %d, expected <=50", pgLen)
			}
			for _, pg := range noScaleUpEv.NoScaleUp.UnhandledPodGroups {
				if rmLen := len(pg.RejectedMigs); rmLen > 30 {
					t.Errorf("Unexpected length of rejected migs in no scale up event, got: %d, expected <=30", rmLen)
				}
				for _, mig := range pg.RejectedMigs {
					id := fmt.Sprintf("%v/%v", pg.PodGroup.SamplePod.GetName(), mig.Mig.Name)
					if found := coveredPodsxMig[id]; found {
						t.Errorf("One of expected mig - pod configurations emitted more than once: %v", id)
					}
					coveredPodsxMig[id] = true
				}
			}
		}
	}
	if len(coveredPodsxMig) != consideredMigsLen*podGroupsLen {
		t.Errorf("Incorrect number of mig explanations per pod groups; got: %d, expected: %d", len(coveredPodsxMig), consideredMigsLen*podGroupsLen)
	}
	for i := 0; i < podGroupsLen; i++ {
		for j := 0; j < consideredMigsLen; j++ {
			podName := fmt.Sprintf("uid-%d", i)
			migName := fmt.Sprintf("mig-%d", j)
			id := fmt.Sprintf("%v/%v", podName, migName)
			if !coveredPodsxMig[id] {
				t.Errorf("One of expected mig - pod configurations not emitted in no scale up info: %v", id)
			}
		}
	}
}

func TestMaxLengthOfNoScaleUpEvent(t *testing.T) {
	loggerMock := new(visibility.MockEventLogger)
	processor := ScaleUpStatusVisibilityProcessor{
		logger:    loggerMock,
		opts:      visibility.VisibilityOptions{EmitNoScaleUpEvents: true},
		data:      NewSharedData(),
		idGen:     new(visibility.MockEventIDGenerator),
		noScaleUp: noscaleup.NewNoScaleUp(10*time.Minute, false, nil),
	}
	ctx := &ca_context.AutoscalingContext{ProcessorCallbacks: callbacks.NewTestProcessorCallbacks()}
	podGroupsLen := 80
	consideredMigsLen := 80
	noScaleUpInfos, consideredMigs := createNoScaleUpInfos(podGroupsLen, consideredMigsLen, true, true)
	s := vistypes.ScaleUpStatus{
		ConsideredMigs: consideredMigs,
		NoScaleUpInfos: noScaleUpInfos,
	}
	now := time.Now()
	ev, _ := processor.processNoScaleUpEvent(ctx, &s, now)

	eventRaw, err := protojson.Marshal(ev)
	if err != nil {
		t.Errorf("Error when Marshal vispb.AutoscalerEvent in JSON format: %s", err)
	}
	eventJSON := googleapi.RawMessage(eventRaw)

	// Golang byte is guaranteed to be one byte: https://golang.org/ref/spec#Size_and_alignment_guarantees.
	if bytesLen := len(eventJSON); bytesLen > 512000 {
		t.Errorf("No scale up event is too large; actual length: %d, expected <%d", bytesLen, 512000)
	}
}

func createNoScaleUpInfos(podsLen, migMapsLen int, createNapMigs, createSkippedMigs bool) ([]*vistypes.NoScaleUpInfo, []*vistypes.GkeMig) {
	rejectedMigs, consideredMigs := createRejectedMIGsMap(migMapsLen, true, "mig")
	if createNapMigs {
		rejectedNapMigs, consideredNapMigs := createRejectedMIGsMap(40, false, "nap-mig")
		consideredMigs = append(consideredMigs, consideredNapMigs...)
		for val, key := range rejectedNapMigs {
			rejectedMigs[val] = key
		}
	}
	noScaleUpInfos := make([]*vistypes.NoScaleUpInfo, 0, podsLen)
	skippedMigs := make(map[string]status.Reasons)
	if createSkippedMigs {
		var consideredNapMigs []*vistypes.GkeMig
		skippedMigs, consideredNapMigs = createRejectedMIGsMap(migMapsLen, true, "skipped-mig")
		consideredMigs = append(consideredMigs, consideredNapMigs...)
	}
	for i := 0; i < podsLen; i++ {
		uid := fmt.Sprintf("uid-%d", i)
		noScaleUpInfos = append(noScaleUpInfos, &vistypes.NoScaleUpInfo{
			Pod: &vistypes.Pod{
				Name:       uid,
				Namespace:  "ns1",
				Controller: &vistypes.PodController{Uid: uid},
				Uid:        uid,
			},
			RejectedNodeGroups: rejectedMigs,
			SkippedNodeGroups:  skippedMigs,
		})
	}
	return noScaleUpInfos, consideredMigs
}

func createRejectedMIGsMap(mapLen int, exists bool, namePrefix string) (map[string]status.Reasons, []*vistypes.GkeMig) {
	rejectedMigs := make(map[string]status.Reasons)
	var consideredMigs []*vistypes.GkeMig
	for i := 0; i < mapLen; i++ {
		migName := fmt.Sprintf("%v-%d", namePrefix, i)
		pod := &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "test-namespace", Name: "test-name"}}
		rejectedMigs[migName] = clustersnapshot.NewFailingPredicateError(pod, "not-schedulable", []string{"Very long error message", "Very long error message", "Very long error message", "Very long error message"}, "", "")
		consideredMigs = append(consideredMigs, &vistypes.GkeMig{
			Id:           migName,
			Name:         migName,
			NodePoolName: migName,
			Exists:       exists,
		})
	}
	return rejectedMigs, consideredMigs
}

func TestFailedScaleUpEventEmitted(t *testing.T) {
	err := errors.NewAutoscalerError(gkeclient.ServiceAccountDeleted, "")
	tcs := map[string]struct {
		status         status.ScaleUpStatus
		expectedEvents []string
	}{
		"NAP failed scale ups are correctly mapped to per-pod events": {
			status: status.ScaleUpStatus{
				Result:                   status.ScaleUpError,
				ScaleUpError:             &err,
				PodsTriggeredScaleUp:     []*apiv1.Pod{makeV1Pod("p1", "ns1")},
				FailedCreationNodeGroups: []cloudprovider.NodeGroup{createTestMigWithZone("mig1", "us-central1-a")},
			},
			expectedEvents: []string{"Warning FailedScaleUp Node scale up in zones us-central1-a associated with this pod failed: Service Account was deleted. Pod is at risk of not being scheduled."},
		},
		"Failed scale ups affecting multiple pods are correctly mapped to per-pod events": {
			status: status.ScaleUpStatus{
				Result:                   status.ScaleUpError,
				ScaleUpError:             &err,
				PodsTriggeredScaleUp:     []*apiv1.Pod{makeV1Pod("p1", "ns1"), makeV1Pod("p2", "ns1")},
				FailedCreationNodeGroups: []cloudprovider.NodeGroup{createTestMigWithZone("mig1", "us-central1-a")},
			},
			expectedEvents: []string{
				"Warning FailedScaleUp Node scale up in zones us-central1-a associated with this pod failed: Service Account was deleted. Pod is at risk of not being scheduled.",
				"Warning FailedScaleUp Node scale up in zones us-central1-a associated with this pod failed: Service Account was deleted. Pod is at risk of not being scheduled.",
			},
		},
		"Failed scale ups affecting multiple node groups are correctly mapped to per-pod events": {
			status: status.ScaleUpStatus{
				Result:                 status.ScaleUpError,
				ScaleUpError:           &err,
				PodsTriggeredScaleUp:   []*apiv1.Pod{makeV1Pod("p1", "ns1")},
				FailedResizeNodeGroups: []cloudprovider.NodeGroup{createTestMigWithZone("mig2", "us-central1-c"), createTestMigWithZone("mig1", "us-central1-a")},
			},
			expectedEvents: []string{
				"Warning FailedScaleUp Node scale up in zones us-central1-c, us-central1-a associated with this pod failed: Service Account was deleted. Pod is at risk of not being scheduled.",
			},
		},
		"No trigger pods": {
			status: status.ScaleUpStatus{
				Result:                 status.ScaleUpError,
				ScaleUpError:           &err,
				FailedResizeNodeGroups: []cloudprovider.NodeGroup{createTestMigWithZone("mig2", "us-central1-c"), createTestMigWithZone("mig1", "us-central1-a")},
			},
			expectedEvents: []string{},
		},
	}
	for desc, tc := range tcs {
		t.Run(desc, func(t *testing.T) {
			fakeRecorder := kube_record.NewFakeRecorder(5)
			ctx := &ca_context.AutoscalingContext{
				AutoscalingKubeClients: ca_context.AutoscalingKubeClients{
					Recorder: fakeRecorder,
				},
			}
			processor := ScaleUpStatusVisibilityProcessor{data: NewSharedData(), idGen: new(visibility.MockEventIDGenerator), failedScaleUpEventLogger: events.NewFailedScaleUpEventLogger()}
			processor.Process(context.TODO(), ctx, &tc.status)
			close(fakeRecorder.Events)
			i := 0
			for event := range fakeRecorder.Events {
				if i < len(tc.expectedEvents) && event != tc.expectedEvents[i] {
					t.Errorf("Incorrect event: expected: %v, got: %v.", tc.expectedEvents[i], event)
				}
				i++
			}
			if i != len(tc.expectedEvents) {
				t.Errorf("Incorrect number of received events: expected: %d, got: %d.", len(tc.expectedEvents), i)
			}
		})
	}
}

func TestTrimMigReasons(t *testing.T) {
	testCases := []struct {
		name           string
		migReasons     map[string]*vistypes.MigExplanation
		maxMigs        int
		expectedMigIds []string
	}{
		{
			name:           "fewer MIGs than the limit are returned unchanged",
			migReasons:     buildMigReasons(5),
			maxMigs:        30,
			expectedMigIds: buildMigIds(0, 5),
		},
		{
			name:           "NodeResourcesFit MIG is kept even if it sorts last",
			migReasons:     buildMigReasons(40, "mig-39"),
			maxMigs:        30,
			expectedMigIds: append(buildMigIds(0, 29), "mig-39"),
		},
		{
			name:           "keeps all NodeResourcesFit MIGs",
			migReasons:     buildMigReasons(40, "mig-35", "mig-36", "mig-37"),
			maxMigs:        30,
			expectedMigIds: append(buildMigIds(0, 27), "mig-35", "mig-36", "mig-37"),
		},
		{
			name:           "keeps lowest IDs when all MIGs are NodeResourcesFit",
			migReasons:     buildMigReasons(40, buildMigIds(0, 40)...),
			maxMigs:        30,
			expectedMigIds: buildMigIds(0, 30),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			trimmed := trimMigReasons(tc.migReasons, tc.maxMigs)
			assert.Equal(t, tc.expectedMigIds, slices.Sorted(maps.Keys(trimmed)))
		})
	}
}

func TestNoScaleUpEventKeepsNodeResourcesFitMigs(t *testing.T) {
	loggerMock := new(visibility.MockEventLogger)
	processor := ScaleUpStatusVisibilityProcessor{
		logger:    loggerMock,
		opts:      visibility.VisibilityOptions{EmitNoScaleUpEvents: true},
		data:      NewSharedData(),
		idGen:     new(visibility.MockEventIDGenerator),
		noScaleUp: noscaleup.NewNoScaleUp(10*time.Minute, false, nil),
	}
	ctx := &ca_context.AutoscalingContext{ProcessorCallbacks: callbacks.NewTestProcessorCallbacks()}
	noScaleUpInfos, consideredMigs := createNoScaleUpInfos(3, 40, false, true)
	// All pods share the same RejectedNodeGroups map, so overriding it once applies to every pod group.
	noScaleUpInfos[0].RejectedNodeGroups["mig-39"] = clustersnapshot.NewFailingPredicateError(&apiv1.Pod{}, "NodeResourcesFit", []string{"Insufficient cpu"}, "", "")
	s := vistypes.ScaleUpStatus{
		ConsideredMigs: consideredMigs,
		NoScaleUpInfos: noScaleUpInfos,
	}

	ev, _ := processor.processNoScaleUpEvent(ctx, &s, time.Now())

	noScaleUp := ev.GetNoDecisionStatus().GetNoScaleUp()
	require.NotEmpty(t, noScaleUp.GetUnhandledPodGroups())
	for _, podGroup := range noScaleUp.GetUnhandledPodGroups() {
		require.Len(t, podGroup.GetRejectedMigs(), visibility.MaxMIGsInPodGroup)
		assert.Equal(t, "mig-39", podGroup.GetRejectedMigs()[0].GetMig().GetName())
	}
}

func TestNoScaleUpEventListsNodeResourcesFitMigsFirstWithoutTrimming(t *testing.T) {
	processor := ScaleUpStatusVisibilityProcessor{
		logger:    new(visibility.MockEventLogger),
		opts:      visibility.VisibilityOptions{EmitNoScaleUpEvents: true},
		data:      NewSharedData(),
		idGen:     new(visibility.MockEventIDGenerator),
		noScaleUp: noscaleup.NewNoScaleUp(10*time.Minute, false, nil),
	}
	ctx := &ca_context.AutoscalingContext{ProcessorCallbacks: callbacks.NewTestProcessorCallbacks()}
	noScaleUpInfos, consideredMigs := createNoScaleUpInfos(1, 5, false, false)
	noScaleUpInfos[0].RejectedNodeGroups["mig-4"] = clustersnapshot.NewFailingPredicateError(&apiv1.Pod{}, "NodeResourcesFit", []string{"Insufficient cpu"}, "", "")
	s := vistypes.ScaleUpStatus{
		ConsideredMigs: consideredMigs,
		NoScaleUpInfos: noScaleUpInfos,
	}

	ev, _ := processor.processNoScaleUpEvent(ctx, &s, time.Now())

	podGroups := ev.GetNoDecisionStatus().GetNoScaleUp().GetUnhandledPodGroups()
	require.Len(t, podGroups, 1)
	require.Len(t, podGroups[0].GetRejectedMigs(), 5)
	assert.Equal(t, "mig-4", podGroups[0].GetRejectedMigs()[0].GetMig().GetName())
}

func TestNoScaleUpEventCapsSkippedMigs(t *testing.T) {
	processor := ScaleUpStatusVisibilityProcessor{
		logger:    new(visibility.MockEventLogger),
		opts:      visibility.VisibilityOptions{EmitNoScaleUpEvents: true},
		data:      NewSharedData(),
		idGen:     new(visibility.MockEventIDGenerator),
		noScaleUp: noscaleup.NewNoScaleUp(10*time.Minute, false, nil),
	}
	ctx := &ca_context.AutoscalingContext{ProcessorCallbacks: callbacks.NewTestProcessorCallbacks()}
	noScaleUpInfos, consideredMigs := createNoScaleUpInfos(1, visibility.MaxSkippedMIGsInEvent+10, false, true)
	s := vistypes.ScaleUpStatus{
		ConsideredMigs: consideredMigs,
		NoScaleUpInfos: noScaleUpInfos,
	}

	ev, _ := processor.processNoScaleUpEvent(ctx, &s, time.Now())

	assert.Len(t, ev.GetNoDecisionStatus().GetNoScaleUp().GetSkippedMigs(), visibility.MaxSkippedMIGsInEvent)
}

// buildMigReasons builds count rejected MIG explanations named mig-00, mig-01, ... MIGs listed in
// nodeResourcesFitMigIds fail the NodeResourcesFit predicate, the rest fail NodeAffinity.
func buildMigReasons(count int, nodeResourcesFitMigIds ...string) map[string]*vistypes.MigExplanation {
	nodeResourcesFit := make(map[string]bool, len(nodeResourcesFitMigIds))
	for _, migId := range nodeResourcesFitMigIds {
		nodeResourcesFit[migId] = true
	}
	migReasons := make(map[string]*vistypes.MigExplanation, count)
	for _, migId := range buildMigIds(0, count) {
		reason := vistypes.NewNoScaleUpMigFailingPredicateMsg("NodeAffinity", []string{"node(s) didn't match Pod's node affinity/selector"})
		if nodeResourcesFit[migId] {
			reason = vistypes.NewNoScaleUpMigFailingPredicateMsg("NodeResourcesFit", []string{"Insufficient cpu"})
		}
		migReasons[migId] = &vistypes.MigExplanation{
			Mig:    &vistypes.GkeMig{Id: migId, Name: migId, NodePoolName: migId, Exists: true},
			Reason: reason,
		}
	}
	return migReasons
}

// buildMigIds returns MIG IDs mig-<from> ... mig-<to-1>, zero-padded so that they sort lexicographically.
func buildMigIds(from, to int) []string {
	migIds := make([]string, 0, to-from)
	for i := from; i < to; i++ {
		migIds = append(migIds, fmt.Sprintf("mig-%02d", i))
	}
	return migIds
}
