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
	"strings"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/metrics"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	internalmetrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/events"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/noscaleup"
	vispb "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/proto"
	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
	klog "k8s.io/klog/v2"
)

// ScaleUpStatusVisibilityProcessor analyses the scale up status and emits appropriate visibility events.
type ScaleUpStatusVisibilityProcessor struct {
	logger                   visibility.EventLogger // optional
	opts                     visibility.VisibilityOptions
	data                     *SharedData
	idGen                    visibility.EventIDGenerator
	noScaleUp                noscaleup.NoScaleUp
	failedScaleUpEventLogger events.FailedScaleUpEventLogger
}

// NewScaleUpStatusVisibilityProcessor creates and returns a default instance of ScaleUpStatusVisibilityProcessor.
func NewScaleUpStatusVisibilityProcessor(logger visibility.EventLogger, opts visibility.VisibilityOptions, data *SharedData, eventLogger events.FailedScaleUpEventLogger) status.ScaleUpStatusProcessor {
	return &ScaleUpStatusVisibilityProcessor{
		logger:                   logger,
		opts:                     opts,
		data:                     data,
		idGen:                    new(visibility.UuidEventIDGenerator),
		noScaleUp:                noscaleup.NewNoScaleUp(visibility.NegativeEventsStalenessThreshold),
		failedScaleUpEventLogger: eventLogger,
	}
}

// Process analyses the scale up status and emits appropriate visibility events.
func (p *ScaleUpStatusVisibilityProcessor) Process(context *context.AutoscalingContext, originalScaleUpStatus *status.ScaleUpStatus) {
	startTime := time.Now()
	defer metrics.UpdateDurationFromStart(internalmetrics.CaVizScaleUp, startTime)

	scaleUpStatus, err := vistypes.ConvertScaleUpStatus(originalScaleUpStatus)
	if err != nil {
		klog.Error(err.Error())
		return
	}
	p.emitScaleUpEvent(context, scaleUpStatus, originalScaleUpStatus)
	p.emitNoScaleUpEvent(context, scaleUpStatus)
}

// CleanUp closes the internal logger.
func (p *ScaleUpStatusVisibilityProcessor) CleanUp() {
	if IsNil(p.logger) {
		klog.Info("Visibility events disabled; nothing to Cleanup")
		return
	}

	err := p.logger.Close()
	if err != nil {
		klog.Errorf("Error while closing the Stackdriver client: %v", err)
	}
}

func (p *ScaleUpStatusVisibilityProcessor) emitNoScaleUpEvent(context *context.AutoscalingContext, scaleUpStatus *vistypes.ScaleUpStatus) {
	if !p.opts.EmitNoScaleUpEvents {
		return
	}

	noScaleUpEvent, eventSentCallback := p.processNoScaleUpEvent(context, scaleUpStatus, time.Now())
	if noScaleUpEvent == nil {
		return
	}
	if err := p.tryLogEvent(noScaleUpEvent); err != nil {
		klog.Errorf("Error logging visibility event for no scale-up: %v", err)
		return
	}
	eventSentCallback(time.Now())
}

func (p *ScaleUpStatusVisibilityProcessor) processNoScaleUpEvent(context *context.AutoscalingContext, scaleUpStatus *vistypes.ScaleUpStatus, now time.Time) (event *vispb.AutoscalerEvent, eventSentCallback func(time.Time)) {
	napStatus := vistypes.GetNapStatus(context)

	unreportedReasons := p.noScaleUp.GetNewReasons(scaleUpStatus, napStatus, now)
	if unreportedReasons.IsEmpty() {
		// There are no new reasons to report.
		return nil, nil
	}
	data := new(vispb.NoScaleUpData)
	if unreportedReasons.TopLevel != nil {
		data.Reason = unreportedReasons.TopLevel.Proto()
	}
	if unreportedReasons.TopLevelNap != nil {
		data.NapFailureReason = unreportedReasons.TopLevelNap.Proto()
	}
	if len(unreportedReasons.SkippedMigs) > 0 {
		data.SkippedMigs = make([]*vispb.MigExplanation, 0, len(unreportedReasons.SkippedMigs))
		for i, skippedMig := range unreportedReasons.SkippedMigs {
			if i > visibility.MaxSkippedMIGsInEvent {
				break
			}
			data.SkippedMigs = append(data.SkippedMigs, skippedMig.Proto())
		}
		if len(unreportedReasons.SkippedMigs) > visibility.MaxSkippedMIGsInEvent {
			unreportedReasons.SkippedMigs = unreportedReasons.SkippedMigs[:visibility.MaxSkippedMIGsInEvent]
		}
	}

	if len(unreportedReasons.PodGroups) > 0 {
		data.UnhandledPodGroups = make([]*vispb.PodGroupExplanation, 0, len(unreportedReasons.PodGroups))
		for i, podGroup := range unreportedReasons.PodGroups {
			if i >= visibility.MaxPodsGroupsInEvent {
				break
			}
			if len(podGroup.MigReasons) > visibility.MaxMIGsInPodGroup {
				trimmedMigReasons := make(map[string]*vistypes.MigExplanation)
				for key, migReason := range podGroup.MigReasons {
					trimmedMigReasons[key] = migReason
					if len(trimmedMigReasons) >= visibility.MaxMIGsInPodGroup {
						break
					}
				}
				podGroup.MigReasons = trimmedMigReasons
			}
			data.UnhandledPodGroups = append(data.UnhandledPodGroups, podGroup.Proto())
		}
		data.UnhandledPodGroupsTotalCount = int32(len(unreportedReasons.PodGroups))

		if len(unreportedReasons.PodGroups) > visibility.MaxPodsGroupsInEvent {
			// Make sure to crop the unreported pod groups to those that we'll actually be reporting.
			unreportedReasons.PodGroups = unreportedReasons.PodGroups[:visibility.MaxPodsGroupsInEvent]
		}
	}

	eventSentCallback = func(sendingTime time.Time) {
		p.noScaleUp.MarkReasonsReported(unreportedReasons, sendingTime)
	}

	return &vispb.AutoscalerEvent{EventOneof: &vispb.AutoscalerEvent_NoDecisionStatus{
		NoDecisionStatus: &vispb.NoDecisionStatus{
			MeasureTime: now.Unix(),
			KindOneof:   &vispb.NoDecisionStatus_NoScaleUp{NoScaleUp: data},
		},
	}}, eventSentCallback
}

func (p *ScaleUpStatusVisibilityProcessor) emitScaleUpEvent(context *context.AutoscalingContext, scaleUpStatus *vistypes.ScaleUpStatus, originalScaleUpStatus *status.ScaleUpStatus) {
	if len(scaleUpStatus.PodsTriggeredScaleUp) == 0 {
		// We are doing silent proactive scale-ups.
		// The implementation shouldn't be visible to customers, thus we remove fake pods and events that were triggered only by them.
		ids := getScaleUpMigIds(scaleUpStatus)
		if len(ids) > 0 {
			klog.V(4).Infof("Scale-up status with no triggering pods silenced for following migs: %v", ids)
		}
		return
	}

	var scaleUpEventId string

	if scaleUpEvent := p.processScaleUpEvent(scaleUpStatus, time.Now()); scaleUpEvent != nil {
		scaleUpEventId = scaleUpEvent.GetDecision().GetEventId()

		if err := p.tryLogEvent(scaleUpEvent); err != nil {
			klog.Errorf("Error logging visibility event for scale-up: %v", err)
		}
	}
	if originalScaleUpStatus.Result == status.ScaleUpError && originalScaleUpStatus.ScaleUpError != nil {
		p.failedScaleUpEventLogger.EmitEventsFromFailure(
			context,
			scaleUpStatus.PodsTriggeredScaleUp,
			nodeGroupsFromScaleUpStatus(originalScaleUpStatus),
			[]*vistypes.Message{vistypes.ScaleUpFailureToVisMessage(string((*originalScaleUpStatus.ScaleUpError).Type()), "" /* NodeGroupId is not important in case of sync errors*/, *originalScaleUpStatus.ScaleUpError)})
	}

	if p.opts.EmitNapInfo {
		if nodePoolCreatedEvent := p.processNodePoolCreatedEvent(scaleUpStatus, scaleUpEventId, time.Now()); nodePoolCreatedEvent != nil {
			if err := p.tryLogEvent(nodePoolCreatedEvent); err != nil {
				klog.Errorf("Error logging visibility event for node pool creation: %v", err)
			}
		}
	}
}
func (p *ScaleUpStatusVisibilityProcessor) processScaleUpEvent(scaleUpStatus *vistypes.ScaleUpStatus, now time.Time) *vispb.AutoscalerEvent {
	if scaleUpStatus.Result != status.ScaleUpSuccessful {
		return nil
	}

	eventId := p.idGen.GenerateID()

	targetNodeGroupIds := make(map[string]bool)
	for _, scaleUpMig := range scaleUpStatus.ScaleUpMigs {
		targetNodeGroupIds[scaleUpMig.Mig.Id] = true
	}
	p.data.StartEvent(eventId, targetNodeGroupIds, scaleUpStatus.PodsTriggeredScaleUp)

	decision := &vispb.DecisionEvent{
		DecideTime: now.Unix(),
		EventId:    eventId,
		DecisionOneof: &vispb.DecisionEvent_ScaleUp{
			ScaleUp: scaleUpStatus.ScaleUpDataProto(),
		},
	}
	return &vispb.AutoscalerEvent{
		EventOneof: &vispb.AutoscalerEvent_Decision{Decision: decision},
	}
}

func (p *ScaleUpStatusVisibilityProcessor) processNodePoolCreatedEvent(scaleUpStatus *vistypes.ScaleUpStatus, scaleUpEventId string, now time.Time) *vispb.AutoscalerEvent {
	data := scaleUpStatus.NodePoolCreatedDataProto()
	if len(data.NodePools) == 0 {
		return nil
	}

	if len(scaleUpEventId) > 0 {
		data.TriggeringScaleUpId = scaleUpEventId
	}

	decision := &vispb.DecisionEvent{
		DecideTime: now.Unix(),
		EventId:    p.idGen.GenerateID(),
		DecisionOneof: &vispb.DecisionEvent_NodePoolCreated{
			NodePoolCreated: data,
		},
	}
	return &vispb.AutoscalerEvent{
		EventOneof: &vispb.AutoscalerEvent_Decision{Decision: decision},
	}
}

// log visibility event only when logger is initiallized (if AutoscalerVisibility is enabled)
func (p *ScaleUpStatusVisibilityProcessor) tryLogEvent(event *vispb.AutoscalerEvent) error {
	if IsNil(p.logger) {
		return nil
	}
	return p.logger.LogEvent(event)
}

func nodeGroupsFromScaleUpStatus(scaleUpStatus *status.ScaleUpStatus) []cloudprovider.NodeGroup {
	if scaleUpStatus.FailedResizeNodeGroups != nil {
		return scaleUpStatus.FailedResizeNodeGroups
	}
	return scaleUpStatus.FailedCreationNodeGroups
}

func getScaleUpMigIds(scaleUpStatus *vistypes.ScaleUpStatus) string {
	ids := make([]string, 0, len(scaleUpStatus.ScaleUpMigs))
	for _, mig := range scaleUpStatus.ScaleUpMigs {
		ids = append(ids, mig.Mig.Id)
	}
	return strings.Join(ids, ",")
}
