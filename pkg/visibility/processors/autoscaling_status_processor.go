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
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate/scaleupfailures"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/metrics"
	internalmetrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/events"
	vispb "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/proto"
	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
	klog "k8s.io/klog/v2"
)

// AutoscalingStatusVisibilityProcessor produces and logs visibility events at the end of each autoscaling iteration.
type AutoscalingStatusVisibilityProcessor struct {
	logger                    visibility.EventLogger // optional
	failedScaleUpLogger       events.FailedScaleUpEventLogger
	opts                      visibility.VisibilityOptions
	data                      *SharedData
	lastStatusEventReportTime time.Time
	lastReportedCurrentSize   int
	lastReportedTargetSize    int
}

type visibilityClusterStateRegistry interface {
	GetAutoscaledNodesCount() (int, int)
	IsNodeGroupRegistered(nodeGroupName string) bool
	IsNodeGroupAtTargetSize(nodeGroupName string) bool
	GetScaleUpFailures() map[string][]scaleupfailures.Record
}

// NewAutoscalingStatusVisibilityProcessor creates and returns a default instance of AutoscalingStatusVisibilityProcessor.
func NewAutoscalingStatusVisibilityProcessor(logger visibility.EventLogger, opts visibility.VisibilityOptions, data *SharedData, failedEventLogger events.FailedScaleUpEventLogger) *AutoscalingStatusVisibilityProcessor {
	return &AutoscalingStatusVisibilityProcessor{logger: logger, opts: opts, data: data, lastReportedTargetSize: -1, lastReportedCurrentSize: -1, failedScaleUpLogger: failedEventLogger}
}

// Process processes the cluster state and logs appropriate events.
func (p *AutoscalingStatusVisibilityProcessor) Process(context *context.AutoscalingContext, csr *clusterstate.ClusterStateRegistry, now time.Time) error {
	startTime := time.Now()
	defer metrics.UpdateDurationFromStart(internalmetrics.CaVizStatus, startTime)

	p.data.PeriodicCleanup()

	statusEvent := p.processClusterStatusEvent(csr, now)
	if statusEvent != nil {
		err := p.tryLogEventWithDefaults(statusEvent)
		if err != nil {
			klog.Errorf("Error logging visibility event for status: %v", err)
		} else {
			p.confirmStatusEventReported(statusEvent, now)
		}
	}

	resultsEvent := p.processResultsEvent(context, csr, now)
	if resultsEvent == nil {
		return nil
	}
	err := p.tryLogEvent(resultsEvent)
	if err != nil {
		klog.Errorf("Error logging visibility event for results: %v", err)
		return err
	}

	return nil
}

// CleanUp closes the internal logger of the processor.
func (p *AutoscalingStatusVisibilityProcessor) CleanUp() {
	if IsNil(p.logger) {
		klog.Info("Visibility events disabled; nothing to Cleanup")
		return
	}

	err := p.logger.Close()
	if err != nil {
		klog.Errorf("Error while closing the Stackdriver client: %v", err)
	}
}

func (p *AutoscalingStatusVisibilityProcessor) confirmStatusEventReported(statusEvent *vispb.AutoscalerEvent, reportTime time.Time) {
	p.lastStatusEventReportTime = reportTime
	p.lastReportedCurrentSize = int(statusEvent.GetStatus().GetAutoscaledNodesCount())
	p.lastReportedTargetSize = int(statusEvent.GetStatus().GetAutoscaledNodesTarget())
}

func (p *AutoscalingStatusVisibilityProcessor) processScaleUpFailures(ctx *context.AutoscalingContext, csr visibilityClusterStateRegistry) {
	failures := csr.GetScaleUpFailures()
	if len(failures) == 0 {
		return
	}
	failuresAssociatedWithEvent := make(map[string][]*vistypes.Message)
	nodeGroupsAssociatedWithEvent := make(map[string][]cloudprovider.NodeGroup)
	nodeGroupsMap := cloudprovider.NodeGroupListToMapById(ctx.CloudProvider.NodeGroups())
	for nodeGroupId, failures := range failures {
		errMsgs := make([]*vistypes.Message, 0)
		for _, failure := range failures {
			reason := failure.ErrorInfo.ErrorCode
			failureVisMessage := vistypes.ScaleUpFailureToVisMessage(string(reason), nodeGroupId, nil)
			if failureVisMessage != nil {
				errMsgs = append(errMsgs, failureVisMessage)
			}
		}
		eventIds := p.data.FailNodeGroup(nodeGroupId, errMsgs)
		// TODO(b/519143061): Node group is no longer linked in scaleupfailures.Record
		nodeGroup := nodeGroupsMap[nodeGroupId]
		if nodeGroup == nil {
			klog.Warningf("AutoscalingStatusVisibilityProcessor: nodegroup %v not found in cloudprovider.NodeGroups()", nodeGroupId)
		}
		for _, eventId := range eventIds {
			failuresAssociatedWithEvent[eventId] = append(failuresAssociatedWithEvent[eventId], errMsgs...)
			nodeGroupsAssociatedWithEvent[eventId] = append(nodeGroupsAssociatedWithEvent[eventId], nodeGroup)
		}
	}
	p.emitFailedScaleUpEvents(ctx, failuresAssociatedWithEvent, nodeGroupsAssociatedWithEvent)
}

func (p *AutoscalingStatusVisibilityProcessor) emitFailedScaleUpEvents(ctx *context.AutoscalingContext, failuresAssociatedWithEvent map[string][]*vistypes.Message, nodeGroupsAssociatedWithEvent map[string][]cloudprovider.NodeGroup) {
	if p.failedScaleUpLogger == nil {
		return
	}
	for id, failures := range failuresAssociatedWithEvent {
		eInfo := p.data.GetEvent(id)
		if len(eInfo.podsTriggeringScaleUp) > 0 {
			p.failedScaleUpLogger.EmitEventsFromFailure(ctx, eInfo.podsTriggeringScaleUp, nodeGroupsAssociatedWithEvent[id], failures)
		}
	}
}

func (p *AutoscalingStatusVisibilityProcessor) processUnfinishedNodeGroups(csr visibilityClusterStateRegistry) {
	for nodeGroupId := range p.data.GetUnfinishedNodeGroupIds() {
		if !csr.IsNodeGroupRegistered(nodeGroupId) || csr.IsNodeGroupAtTargetSize(nodeGroupId) {
			p.data.FinishNodeGroup(nodeGroupId)
		}
	}
}

func (p *AutoscalingStatusVisibilityProcessor) processClusterStatusEvent(csr visibilityClusterStateRegistry, now time.Time) *vispb.AutoscalerEvent {
	currentSize, targetSize := csr.GetAutoscaledNodesCount()

	// Throttle the status events - if none of the sizes change, only emit the event once every X minutes.
	if currentSize == p.lastReportedCurrentSize && targetSize == p.lastReportedTargetSize && p.lastStatusEventReportTime.Add(visibility.StatusEventsStalenessThreshold).After(now) {
		return nil
	}

	s := &vispb.ClusterStatus{
		MeasureTime:           now.Unix(),
		AutoscaledNodesCount:  int32(currentSize),
		AutoscaledNodesTarget: int32(targetSize),
	}

	if p.opts.IncludePerMigStatuses {
		// TODO(b/517098459): Implement per-MIG statuses.
	}

	return &vispb.AutoscalerEvent{
		EventOneof: &vispb.AutoscalerEvent_Status{Status: s},
	}
}

func (p *AutoscalingStatusVisibilityProcessor) processResultsEvent(ctx *context.AutoscalingContext, csr visibilityClusterStateRegistry, now time.Time) *vispb.AutoscalerEvent {
	p.processScaleUpFailures(ctx, csr)
	p.processUnfinishedNodeGroups(csr)

	eventResults := p.data.GetNextResults()
	if len(eventResults) == 0 {
		return nil
	}

	return &vispb.AutoscalerEvent{
		EventOneof: &vispb.AutoscalerEvent_ResultInfo{ResultInfo: &vispb.ResultInfo{
			MeasureTime: now.Unix(),
			Results:     eventResults,
		}},
	}
}

// log visibility event only when logger is initiallized (if AutoscalerVisibility is enabled)
func (p *AutoscalingStatusVisibilityProcessor) tryLogEvent(event *vispb.AutoscalerEvent) error {
	if IsNil(p.logger) {
		return nil
	}
	return p.logger.LogEvent(event)
}

// log visibility event only when logger is initiallized (if AutoscalerVisibility is enabled)
func (p *AutoscalingStatusVisibilityProcessor) tryLogEventWithDefaults(event *vispb.AutoscalerEvent) error {
	if IsNil(p.logger) {
		return nil
	}
	return p.logger.LogEventWithDefaults(event)
}
