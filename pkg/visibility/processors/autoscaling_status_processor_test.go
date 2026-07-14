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
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	testprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate/scaleupfailures"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/metrics"
	nodegroupchange "k8s.io/autoscaler/cluster-autoscaler/observers/nodegroupchange"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupconfig"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups/asyncnodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	kube_record "k8s.io/client-go/tools/record"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
	internal_customresources "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/customresources"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/events"
	vispb "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/proto"
	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
)

type mockVisibilityClusterStateRegistry struct {
	mock.Mock
	scalingNodeGroupIds map[string]bool
	scaleUpFailures     map[string][]scaleupfailures.Record
}

func (csr *mockVisibilityClusterStateRegistry) GetAutoscaledNodesCount() (int, int) {
	return 13, 37
}

func (csr *mockVisibilityClusterStateRegistry) IsNodeGroupAtTargetSize(nodeGroupName string) bool {
	_, ok := csr.scalingNodeGroupIds[nodeGroupName]
	return !ok
}

func (csr *mockVisibilityClusterStateRegistry) IsNodeGroupRegistered(nodeGroupName string) bool {
	_, found := csr.scalingNodeGroupIds[nodeGroupName]
	return found
}

func (csr *mockVisibilityClusterStateRegistry) GetScaleUpFailures() map[string][]scaleupfailures.Record {
	return csr.scaleUpFailures
}

func newMockVisibilityClusterStateRegistry() *mockVisibilityClusterStateRegistry {
	return &mockVisibilityClusterStateRegistry{scalingNodeGroupIds: make(map[string]bool),
		scaleUpFailures: make(map[string][]scaleupfailures.Record)}
}

func assertResultsEventsEqual(t *testing.T, expectedEvent, event *vispb.AutoscalerEvent) {
	if expectedEvent == nil {
		assert.Nil(t, event)
	} else {
		assert.NotNil(t, expectedEvent.GetResultInfo())
		assert.NotNil(t, event.GetResultInfo())
	}
	assert.ElementsMatch(t, expectedEvent.GetResultInfo().GetResults(), event.GetResultInfo().GetResults())
	assert.Equal(t, expectedEvent.GetResultInfo().GetMeasureTime(), event.GetResultInfo().GetMeasureTime())
}

func TestProcessClusterStatusEvent(t *testing.T) {
	now := time.Date(2000, 1, 1, 10, 10, 10, 0, time.UTC)
	ctx := &context.AutoscalingContext{
		CloudProvider: testprovider.NewTestCloudProviderBuilder().Build(),
	}

	csr := newMockVisibilityClusterStateRegistry()
	csr.scalingNodeGroupIds["ng1"] = true
	csr.scalingNodeGroupIds["ng2"] = true
	csr.scalingNodeGroupIds["ng3"] = true
	csr.scalingNodeGroupIds["ng4"] = true
	csr.scalingNodeGroupIds["ng5"] = true

	data := NewSharedData()
	data.StartEvent("event1", map[string]bool{"ng1": true, "ng2": true}, nil)
	data.StartEvent("event2", map[string]bool{"ng1": true}, nil)
	data.StartEvent("event3", map[string]bool{"ng2": true}, nil)
	data.StartEvent("failEvent1", map[string]bool{"ng3": true, "ng1": true}, nil)
	data.StartEvent("failEvent2", map[string]bool{"ng3": true, "ng5": true}, nil)
	data.StartEvent("failEvent3", map[string]bool{"ng4": true}, nil)

	processor := AutoscalingStatusVisibilityProcessor{data: data}

	// There are 2 pending node groups, both are still scaling up - no finished events should be emitted.
	expectedStatusEvent := &vispb.AutoscalerEvent{
		EventOneof: &vispb.AutoscalerEvent_Status{
			Status: &vispb.ClusterStatus{
				MeasureTime:           now.Unix(),
				AutoscaledNodesCount:  13,
				AutoscaledNodesTarget: 37,
			},
		},
	}
	var expectedResultsEvent *vispb.AutoscalerEvent
	statusEvent := processor.processClusterStatusEvent(csr, now)
	resultsEvent := processor.processResultsEvent(ctx, csr, now)
	assert.Equal(t, expectedStatusEvent, statusEvent)
	assert.Equal(t, expectedResultsEvent, resultsEvent)

	// One of the nodegroups (ng3) failed - errors should be emitted.
	expectedStatusEvent = &vispb.AutoscalerEvent{
		EventOneof: &vispb.AutoscalerEvent_Status{
			Status: &vispb.ClusterStatus{
				MeasureTime:           now.Unix(),
				AutoscaledNodesCount:  13,
				AutoscaledNodesTarget: 37,
			},
		},
	}
	expectedResultsEvent = &vispb.AutoscalerEvent{
		EventOneof: &vispb.AutoscalerEvent_ResultInfo{
			ResultInfo: &vispb.ResultInfo{
				MeasureTime: now.Unix(),
				Results: []*vispb.EventResult{
					{EventId: "failEvent1", ErrorMsg: &vispb.ParametrizedMessage{MessageId: vistypes.MessageId(0).String(), Parameters: []string{"p1", "p2"}}},
					{EventId: "failEvent1", ErrorMsg: &vispb.ParametrizedMessage{MessageId: vistypes.MessageId(0).String(), Parameters: []string{"p3", "p4"}}},
					{EventId: "failEvent2", ErrorMsg: &vispb.ParametrizedMessage{MessageId: vistypes.MessageId(0).String(), Parameters: []string{"p1", "p2"}}},
					{EventId: "failEvent2", ErrorMsg: &vispb.ParametrizedMessage{MessageId: vistypes.MessageId(0).String(), Parameters: []string{"p3", "p4"}}},
				},
			},
		},
	}
	data.FailNodeGroup("ng3", []*vistypes.Message{
		{Id: 0, Params: []string{"p1", "p2"}},
		{Id: 0, Params: []string{"p3", "p4"}},
	})
	statusEvent = processor.processClusterStatusEvent(csr, now)
	resultsEvent = processor.processResultsEvent(ctx, csr, now)
	assert.Equal(t, expectedStatusEvent, statusEvent)
	assertResultsEventsEqual(t, expectedResultsEvent, resultsEvent)

	// One of the pending NGs has finished scaling. Event2 was waiting only for that group, so it should
	// be emitted in finished events. Event1 is still waiting for the other NG. ng4 failed, so it should be emitted as well.
	expectedStatusEvent = &vispb.AutoscalerEvent{
		EventOneof: &vispb.AutoscalerEvent_Status{
			Status: &vispb.ClusterStatus{
				MeasureTime:           now.Unix(),
				AutoscaledNodesCount:  13,
				AutoscaledNodesTarget: 37,
			},
		},
	}
	expectedResultsEvent = &vispb.AutoscalerEvent{
		EventOneof: &vispb.AutoscalerEvent_ResultInfo{
			ResultInfo: &vispb.ResultInfo{
				MeasureTime: now.Unix(),
				Results: []*vispb.EventResult{
					{EventId: "event2"},
					{EventId: "failEvent3", ErrorMsg: &vispb.ParametrizedMessage{MessageId: vistypes.MessageId(0).String(), Parameters: []string{"p5", "p6"}}},
				},
			},
		},
	}
	delete(csr.scalingNodeGroupIds, "ng1")
	data.FailNodeGroup("ng4", []*vistypes.Message{
		{Id: 0, Params: []string{"p5", "p6"}},
	})
	statusEvent = processor.processClusterStatusEvent(csr, now)
	resultsEvent = processor.processResultsEvent(ctx, csr, now)
	assert.Equal(t, expectedStatusEvent, statusEvent)
	assertResultsEventsEqual(t, expectedResultsEvent, resultsEvent)

	// The other NG has finished scaling. Both events that had been waiting for that have no other groups
	// to wait for, so they both should be emitted in finished events.
	expectedStatusEvent = &vispb.AutoscalerEvent{
		EventOneof: &vispb.AutoscalerEvent_Status{
			Status: &vispb.ClusterStatus{
				MeasureTime:           now.Unix(),
				AutoscaledNodesCount:  13,
				AutoscaledNodesTarget: 37,
			},
		},
	}
	expectedResultsEvent = &vispb.AutoscalerEvent{
		EventOneof: &vispb.AutoscalerEvent_ResultInfo{
			ResultInfo: &vispb.ResultInfo{
				MeasureTime: now.Unix(),
				Results: []*vispb.EventResult{
					{EventId: "event1"},
					{EventId: "event3"},
				},
			},
		},
	}
	delete(csr.scalingNodeGroupIds, "ng2")
	statusEvent = processor.processClusterStatusEvent(csr, now)
	resultsEvent = processor.processResultsEvent(ctx, csr, now)
	assert.Equal(t, expectedStatusEvent, statusEvent)
	assertResultsEventsEqual(t, expectedResultsEvent, resultsEvent)

	// The second node group failed for failEvent2 - an error should be emitted.
	expectedStatusEvent = &vispb.AutoscalerEvent{
		EventOneof: &vispb.AutoscalerEvent_Status{
			Status: &vispb.ClusterStatus{
				MeasureTime:           now.Unix(),
				AutoscaledNodesCount:  13,
				AutoscaledNodesTarget: 37,
			},
		},
	}
	expectedResultsEvent = &vispb.AutoscalerEvent{
		EventOneof: &vispb.AutoscalerEvent_ResultInfo{
			ResultInfo: &vispb.ResultInfo{
				MeasureTime: now.Unix(),
				Results: []*vispb.EventResult{
					{EventId: "failEvent2", ErrorMsg: &vispb.ParametrizedMessage{MessageId: vistypes.MessageId(0).String(), Parameters: []string{"p7", "p8"}}},
				},
			},
		},
	}
	data.FailNodeGroup("ng5", []*vistypes.Message{
		{Id: 0, Params: []string{"p7", "p8"}},
	})
	statusEvent = processor.processClusterStatusEvent(csr, now)
	resultsEvent = processor.processResultsEvent(ctx, csr, now)
	assert.Equal(t, expectedStatusEvent, statusEvent)
	assertResultsEventsEqual(t, expectedResultsEvent, resultsEvent)

	// There shouldn't be any more pending groups, so finished_events should be empty.
	expectedStatusEvent = &vispb.AutoscalerEvent{
		EventOneof: &vispb.AutoscalerEvent_Status{
			Status: &vispb.ClusterStatus{
				MeasureTime:           now.Unix(),
				AutoscaledNodesCount:  13,
				AutoscaledNodesTarget: 37,
			},
		},
	}
	expectedResultsEvent = nil
	statusEvent = processor.processClusterStatusEvent(csr, now)
	resultsEvent = processor.processResultsEvent(ctx, csr, now)
	assert.Equal(t, expectedStatusEvent, statusEvent)
	assertResultsEventsEqual(t, expectedResultsEvent, resultsEvent)
}

func createSimpleTestMig(name string) *gke.GkeMig {
	return createTestMigWithZone(name, "")
}

func createTestMigWithZone(name, zone string) *gke.GkeMig {
	return gke.NewTestGkeMigBuilder().
		SetId(name).
		SetGceRefName(name).
		SetGceRefZone(zone).
		SetExist(true).
		Build()
}

func TestProcessScaleUpFailures(t *testing.T) {
	now := time.Date(2000, 1, 1, 10, 10, 10, 0, time.UTC)

	csr := newMockVisibilityClusterStateRegistry()
	ctx := &context.AutoscalingContext{
		CloudProvider: gke.NewTestAutoprovisioningCloudProviderBuilder().Build(),
	}
	csr.scaleUpFailures = map[string][]scaleupfailures.Record{
		"ng1": {
			{ErrorInfo: cloudprovider.InstanceErrorInfo{ErrorCode: string(metrics.Timeout)}, Time: now.Add(-1 * time.Minute)},
			{ErrorInfo: cloudprovider.InstanceErrorInfo{ErrorCode: gce.ErrorCodeResourcePoolExhausted}, Time: now.Add(-2 * time.Minute)},
		},
		"ng2": {
			{ErrorInfo: cloudprovider.InstanceErrorInfo{ErrorCode: string(metrics.Timeout)}, Time: now.Add(-1 * time.Minute)},
		},
		"ng3": {
			{ErrorInfo: cloudprovider.InstanceErrorInfo{ErrorCode: gce.ErrorCodeQuotaExceeded}, Time: now.Add(-1 * time.Minute)},
		},
		"ng4": {
			{ErrorInfo: cloudprovider.InstanceErrorInfo{ErrorCode: gce.ErrorCodeOther}, Time: now.Add(-1 * time.Minute)},
		},
		"ng5": {
			{ErrorInfo: cloudprovider.InstanceErrorInfo{ErrorCode: gce.ErrorIPSpaceExhausted}, Time: now.Add(-1 * time.Minute)},
		},
		"ng6": {
			{ErrorInfo: cloudprovider.InstanceErrorInfo{ErrorCode: gkeclient.ServiceAccountDeleted}, Time: now.Add(-1 * time.Minute)},
		},
	}

	data := NewSharedData()
	data.StartEvent("event1", map[string]bool{"ng1": true, "ng2": true}, nil)
	data.StartEvent("event2", map[string]bool{"ng1": true}, nil)
	data.StartEvent("event3", map[string]bool{"ng2": true}, nil)
	data.StartEvent("event4", map[string]bool{"ng3": true}, nil)
	data.StartEvent("event5", map[string]bool{"ng4": true}, nil)
	data.StartEvent("event6", map[string]bool{"ng5": true}, nil)
	data.StartEvent("event7", map[string]bool{"ng6": true}, nil)

	processor := AutoscalingStatusVisibilityProcessor{data: data}
	processor.processScaleUpFailures(ctx, csr)
	results := processor.data.GetNextResults()

	assert.ElementsMatch(t, []*vispb.EventResult{
		{EventId: "event1", ErrorMsg: vistypes.NewScaleUpErrorWaitingForInstancesTimeoutMsg("ng1").Proto()},
		{EventId: "event1", ErrorMsg: vistypes.NewScaleUpErrorWaitingForInstancesTimeoutMsg("ng2").Proto()},
		{EventId: "event1", ErrorMsg: vistypes.NewScaleUpErrorOutOfResourcesMsg("ng1").Proto()},
		{EventId: "event2", ErrorMsg: vistypes.NewScaleUpErrorWaitingForInstancesTimeoutMsg("ng1").Proto()},
		{EventId: "event2", ErrorMsg: vistypes.NewScaleUpErrorOutOfResourcesMsg("ng1").Proto()},
		{EventId: "event3", ErrorMsg: vistypes.NewScaleUpErrorWaitingForInstancesTimeoutMsg("ng2").Proto()},
		{EventId: "event4", ErrorMsg: vistypes.NewScaleUpErrorQuotaExceededMsg("ng3").Proto()},
		{EventId: "event5", ErrorMsg: vistypes.NewScaleUpErrorOtherMsg("ng4").Proto()},
		{EventId: "event6", ErrorMsg: vistypes.NewScaleUpErrorIPSpaceExhaustedMsg("ng5").Proto()},
		{EventId: "event7", ErrorMsg: vistypes.NewScaleUpErrorServiceAccountDeletedMsg("ng6").Proto()},
	}, results)
}

func TestStatusEventThrottling(t *testing.T) {
	now := time.Date(2000, 1, 1, 10, 10, 10, 0, time.UTC)

	provider := testprovider.NewTestCloudProviderBuilder().Build()
	ctx := &context.AutoscalingContext{
		CloudProvider: provider,
	}
	logger := new(visibility.MockEventLogger)
	processor := NewAutoscalingStatusVisibilityProcessor(logger, visibility.VisibilityOptions{}, NewSharedData(), nil)

	// Setup CSR so that it works. This is really ugly, but since *clusterstate.ClusterStateRegistry is explicitly needed as a parameter
	// to Process(), it can't be mocked.
	node := test.BuildTestNode("node", 1000, 1000)
	test.SetNodeReadyState(node, true, now.Add(-time.Minute))
	provider.AddNodeGroup("ng", 1, 10, 5)
	provider.AddNode("ng", node)
	customResourcesProcessor := internal_customresources.NewProcessor(nodetemplate.NewCache())
	customResourcesProcessor.SetContext(&context.AutoscalingContext{CloudProvider: provider})
	csr := clusterstate.NewClusterStateRegistry(provider, nil, backoff.NewGkeBackoff(backoff.Config{CustomResourceProcessor: customResourcesProcessor}), nodegroupconfig.NewDefaultNodeGroupConfigProcessor(config.NodeGroupAutoscalingOptions{}), nil, clusterstate.WithAsyncNodeGroupStateChecker(asyncnodegroups.NewDefaultAsyncNodeGroupStateChecker()), clusterstate.WithScaleStateNotifier(nodegroupchange.NewNodeGroupChangeObserversList()))
	err := csr.UpdateNodes([]*apiv1.Node{node}, now)
	assert.NoError(t, err)

	// Target size is 5, actual size is 1, this is the first call - event should be logged.
	logger.On("LogEventWithDefaults", mock.Anything).Return(nil).Once()
	err = processor.Process(ctx, csr, now)
	assert.NoError(t, err)

	// The sizes didn't change, this a second call but below the staleness threshold - event shouldn't be logged.
	now = now.Add(visibility.StatusEventsStalenessThreshold - time.Second)
	err = processor.Process(ctx, csr, now)
	assert.NoError(t, err)

	// The sizes didn't change, but the call is after the staleness threshold - event should be logged.
	logger.On("LogEventWithDefaults", mock.Anything).Return(nil).Once()
	now = now.Add(2 * time.Second)
	err = processor.Process(ctx, csr, now)
	assert.NoError(t, err)

	// The call is before the staleness threshold, but the actual size changed from 1 to 2 - event should be logged.
	logger.On("LogEventWithDefaults", mock.Anything).Return(nil).Once()
	now = now.Add(visibility.StatusEventsStalenessThreshold - 2*time.Second)
	err = csr.UpdateNodes([]*apiv1.Node{node, node}, now)
	assert.NoError(t, err)
	err = processor.Process(ctx, csr, now)
	assert.NoError(t, err)

	// The call is before the staleness threshold, but the target size changed from 5 to 7 - event should be logged.
	logger.On("LogEventWithDefaults", mock.Anything).Return(nil).Once()
	provider.AddNodeGroup("ng2", 1, 10, 2)
	err = csr.UpdateNodes([]*apiv1.Node{node, node}, now)
	assert.NoError(t, err)
	now = now.Add(visibility.StatusEventsStalenessThreshold - time.Second)
	err = processor.Process(ctx, csr, now)
	assert.NoError(t, err)

	// The first call is after the staleness threshold, but the call to the logger failed - so the next call should log the event even
	// if it's before the staleness threshold.
	logger.On("LogEventWithDefaults", mock.Anything).Return(fmt.Errorf("this is expected")).Once()
	now = now.Add(visibility.StatusEventsStalenessThreshold + time.Second)
	err = processor.Process(ctx, csr, now)
	assert.NoError(t, err)

	logger.On("LogEventWithDefaults", mock.Anything).Return(nil).Once()
	now = now.Add(visibility.StatusEventsStalenessThreshold - time.Second)
	err = processor.Process(ctx, csr, now)
	assert.NoError(t, err)

	logger.AssertExpectations(t)
}

func TestFailedScaleUpPodEvents(t *testing.T) {
	now := time.Date(2000, 1, 1, 10, 10, 10, 0, time.UTC)
	type eventInfo struct {
		eventId          string
		targetNodeGroups map[string]bool
		pods             []*vistypes.Pod
	}
	tcs := map[string]struct {
		groups          []cloudprovider.NodeGroup
		scaleUpFailures map[string][]scaleupfailures.Record
		expectedEvents  []*regexp.Regexp
		eventInfos      []eventInfo
	}{
		"Failed scale up in two node groups emits correct event.": {
			groups: []cloudprovider.NodeGroup{
				createTestMigWithZone("ng2", "us-central1-c"),
				createTestMigWithZone("ng1", "us-central1-a"),
			},
			eventInfos: []eventInfo{
				{
					eventId: "event1",
					targetNodeGroups: map[string]bool{
						"ng1": true,
						"ng2": true,
					},
					pods: []*vistypes.Pod{makePod("p1", "ns1")},
				},
			},
			scaleUpFailures: map[string][]scaleupfailures.Record{
				"ng2": {
					{ErrorInfo: cloudprovider.InstanceErrorInfo{ErrorCode: gce.ErrorCodeResourcePoolExhausted}, Time: now.Add(-2 * time.Minute)},
				},
				"ng1": {
					{ErrorInfo: cloudprovider.InstanceErrorInfo{ErrorCode: string(metrics.Timeout)}, Time: now.Add(-1 * time.Minute)},
				},
			},
			expectedEvents: []*regexp.Regexp{
				regexp.MustCompile(`Warning FailedScaleUp Node scale up in zones (us-central1-c, us-central1-a|us-central1-a, us-central1-c) associated with this pod failed: (GCE out of resources, Some nodes in node group \["ng1"] failed to appear in time|Some nodes in node group \["ng1"] failed to appear in time, GCE out of resources)\. Pod is at risk of not being scheduled\.`),
			},
		}, "Failed scale up in one node group emits correct event.": {
			groups: []cloudprovider.NodeGroup{
				createTestMigWithZone("ng1", "us-central1-a"),
			},
			eventInfos: []eventInfo{
				{
					eventId: "event1",
					targetNodeGroups: map[string]bool{
						"ng1": true,
						"ng2": true,
					},
					pods: []*vistypes.Pod{makePod("p1", "ns1")},
				},
			},
			scaleUpFailures: map[string][]scaleupfailures.Record{
				"ng1": {
					{ErrorInfo: cloudprovider.InstanceErrorInfo{ErrorCode: string(metrics.Timeout)}, Time: now.Add(-1 * time.Minute)},
				},
			},
			expectedEvents: []*regexp.Regexp{
				regexp.MustCompile(`Warning FailedScaleUp Node scale up in zones us-central1-a associated with this pod failed: Some nodes in node group \["ng1"] failed to appear in time\. Pod is at risk of not being scheduled\.`),
			},
		}, "Multiple failed scale ups for one node group emit correct event.": {
			groups: []cloudprovider.NodeGroup{
				createTestMigWithZone("ng1", "us-central1-a"),
			},
			eventInfos: []eventInfo{
				{
					eventId:          "event1",
					targetNodeGroups: map[string]bool{"ng1": true},
					pods:             []*vistypes.Pod{makePod("p1", "ns1")},
				},
			},
			scaleUpFailures: map[string][]scaleupfailures.Record{
				"ng1": {
					{ErrorInfo: cloudprovider.InstanceErrorInfo{ErrorCode: string(metrics.Timeout)}, Time: now.Add(-1 * time.Minute)},
					{ErrorInfo: cloudprovider.InstanceErrorInfo{ErrorCode: gce.ErrorCodeResourcePoolExhausted}, Time: now.Add(-1 * time.Minute)},
				},
			},
			expectedEvents: []*regexp.Regexp{
				regexp.MustCompile(`Warning FailedScaleUp Node scale up in zones us-central1-a associated with this pod failed: Some nodes in node group \["ng1"] failed to appear in time, GCE out of resources\. Pod is at risk of not being scheduled\.`),
			},
		}, "Failure affecting multiple pods emits event for each of them.": {
			groups: []cloudprovider.NodeGroup{
				createTestMigWithZone("ng1", "us-central1-a"),
			},
			eventInfos: []eventInfo{
				{
					eventId: "event1",
					targetNodeGroups: map[string]bool{
						"ng1": true,
						"ng2": true,
					},
					pods: []*vistypes.Pod{makePod("p1", "ns1"), makePod("p2", "ns1")},
				},
			},
			scaleUpFailures: map[string][]scaleupfailures.Record{
				"ng1": {
					{ErrorInfo: cloudprovider.InstanceErrorInfo{ErrorCode: string(metrics.Timeout)}, Time: now.Add(-1 * time.Minute)},
					{ErrorInfo: cloudprovider.InstanceErrorInfo{ErrorCode: gce.ErrorCodeResourcePoolExhausted}, Time: now.Add(-1 * time.Minute)},
				},
			},
			expectedEvents: []*regexp.Regexp{
				regexp.MustCompile(`Warning FailedScaleUp Node scale up in zones us-central1-a associated with this pod failed: Some nodes in node group \["ng1"] failed to appear in time, GCE out of resources\. Pod is at risk of not being scheduled\.`),
				regexp.MustCompile(`Warning FailedScaleUp Node scale up in zones us-central1-a associated with this pod failed: Some nodes in node group \["ng1"] failed to appear in time, GCE out of resources\. Pod is at risk of not being scheduled\.`),
			},
		}, "Failed node group present in multiple scale up events should emit events properly.": {
			groups: []cloudprovider.NodeGroup{
				createTestMigWithZone("ng1", "us-central1-a"),
			},
			eventInfos: []eventInfo{
				{
					eventId: "event1",
					targetNodeGroups: map[string]bool{
						"ng1": true,
						"ng2": true,
					},
					pods: []*vistypes.Pod{makePod("p1", "ns1")},
				},
				{
					eventId: "event2",
					targetNodeGroups: map[string]bool{
						"ng1": true,
						"ng2": true,
					},
					pods: []*vistypes.Pod{makePod("p2", "ns1")},
				},
			},
			scaleUpFailures: map[string][]scaleupfailures.Record{
				"ng1": {
					{ErrorInfo: cloudprovider.InstanceErrorInfo{ErrorCode: string(metrics.Timeout)}, Time: now.Add(-1 * time.Minute)},
					{ErrorInfo: cloudprovider.InstanceErrorInfo{ErrorCode: gce.ErrorCodeResourcePoolExhausted}, Time: now.Add(-1 * time.Minute)},
				},
			},
			expectedEvents: []*regexp.Regexp{
				regexp.MustCompile(`Warning FailedScaleUp Node scale up in zones us-central1-a associated with this pod failed: Some nodes in node group \["ng1"] failed to appear in time, GCE out of resources\. Pod is at risk of not being scheduled\.`),
				regexp.MustCompile(`Warning FailedScaleUp Node scale up in zones us-central1-a associated with this pod failed: Some nodes in node group \["ng1"] failed to appear in time, GCE out of resources\. Pod is at risk of not being scheduled\.`),
			},
		},
	}
	for desc, tc := range tcs {
		t.Run(desc, func(t *testing.T) {

			data := NewSharedData()
			for _, eInfo := range tc.eventInfos {
				data.StartEvent(eInfo.eventId, eInfo.targetNodeGroups, eInfo.pods)
			}
			fakeRecorder := kube_record.NewFakeRecorder(5)
			fakeProvider := testprovider.NewTestCloudProviderBuilder().Build()
			for _, g := range tc.groups {
				fakeProvider.InsertNodeGroup(g)
				fmt.Printf("Adding ng %v\n", g.Id())
			}
			ctx := &context.AutoscalingContext{
				AutoscalingKubeClients: context.AutoscalingKubeClients{
					Recorder: fakeRecorder,
				},
				CloudProvider: fakeProvider,
			}

			logger := new(visibility.MockEventLogger)
			csr := newMockVisibilityClusterStateRegistry()
			csr.scaleUpFailures = tc.scaleUpFailures
			processor := NewAutoscalingStatusVisibilityProcessor(logger, visibility.VisibilityOptions{}, data, events.NewFailedScaleUpEventLogger())
			processor.processScaleUpFailures(ctx, csr)
			close(fakeRecorder.Events)
			i := 0
			for event := range fakeRecorder.Events {
				if i < len(tc.expectedEvents) && !tc.expectedEvents[i].MatchString(event) {
					t.Errorf("Incorrect event:\n expected: %v,\n got: %v.", tc.expectedEvents[i], event)
				}
				i++
			}
			if i != len(tc.expectedEvents) {
				t.Errorf("Incorrect number of received events: expected: %d, got: %d.", len(tc.expectedEvents), i)
			}
		})
	}
}

func makePod(name, namespace string) *vistypes.Pod {
	return &vistypes.Pod{
		Name:      name,
		Namespace: namespace,
	}
}

func makeV1Pod(name, namespace string) *apiv1.Pod {
	return &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
}

type mockMachineFamilyProvider struct {
	mock.Mock
}

func (m *mockMachineFamilyProvider) GetAutoprovisioningDefaultFamily() machinetypes.MachineFamily {
	args := m.Called()
	return args.Get(0).(machinetypes.MachineFamily)
}
