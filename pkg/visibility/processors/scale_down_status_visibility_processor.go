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

	"k8s.io/autoscaler/cluster-autoscaler/context"
	scaledownstatus "k8s.io/autoscaler/cluster-autoscaler/core/scaledown/status"
	"k8s.io/autoscaler/cluster-autoscaler/metrics"
	statusprocessors "k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	internalmetrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/noscaledown"
	vispb "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/proto"
	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
	klog "k8s.io/klog/v2"
)

type nodeGroupScaleDownInfo struct {
	pendingDeletions int
	errMsgs          []*vistypes.Message
}

type nodeDeletionInfo struct {
	nodeGroupId string
	eventId     string
}

// ScaleDownStatusVisibilityProcessor analyses the scale down status and emits appropriate visibility events.
type ScaleDownStatusVisibilityProcessor struct {
	logger      visibility.EventLogger // optional
	opts        visibility.VisibilityOptions
	data        *SharedData
	idGen       visibility.EventIDGenerator
	noScaleDown noscaledown.NoScaleDown
	// nodeGroupScaleDownInfos contains scale down information for a given node group name.
	nodeGroupScaleDownInfos map[string]*nodeGroupScaleDownInfo
	// nodeDeletionInfos contains information about node deletion for a given node name.
	nodeDeletionInfos map[string]nodeDeletionInfo
}

// NewScaleDownStatusVisibilityProcessor creates and returns a default instance of ScaleDownStatusVisibilityProcessor.
func NewScaleDownStatusVisibilityProcessor(logger visibility.EventLogger, opts visibility.VisibilityOptions, data *SharedData) statusprocessors.ScaleDownStatusProcessor {
	return &ScaleDownStatusVisibilityProcessor{
		logger:                  logger,
		opts:                    opts,
		data:                    data,
		idGen:                   new(visibility.UuidEventIDGenerator),
		noScaleDown:             noscaledown.NewNoScaleDown(visibility.NegativeEventsStalenessThreshold),
		nodeGroupScaleDownInfos: make(map[string]*nodeGroupScaleDownInfo),
		nodeDeletionInfos:       make(map[string]nodeDeletionInfo),
	}
}

// Process analyses the scale down status and emits appropriate visibility events.
func (p *ScaleDownStatusVisibilityProcessor) Process(context *context.AutoscalingContext, originalScaleDownStatus *scaledownstatus.ScaleDownStatus) {
	startTime := time.Now()
	defer metrics.UpdateDurationFromStart(internalmetrics.CaVizScaleDown, startTime)

	scaleDownStatus, err := vistypes.ConvertScaleDownStatus(originalScaleDownStatus)
	if err != nil {
		klog.Error(err.Error())
		return
	}

	p.processNodeDeleteResults(scaleDownStatus)

	if scaleDownEvent := p.processScaleDownEvent(scaleDownStatus, time.Now()); scaleDownEvent != nil {
		err = p.tryLogEvent(scaleDownEvent)
		if err != nil {
			klog.Errorf("Error logging visibility event for scale-down: %v", err)
		}
	}

	if p.opts.EmitNapInfo {
		if nodePoolDeletedEvent := p.processNodePoolDeletedEvent(scaleDownStatus, time.Now()); nodePoolDeletedEvent != nil {
			err = p.tryLogEvent(nodePoolDeletedEvent)
			if err != nil {
				klog.Errorf("Error logging visibility event for node pool deletion: %v", err)
			}
		}
	}

	if !p.opts.EmitNoScaleDownEvents {
		return
	}

	if noScaleDownEvent, eventSentCallback := p.processNoScaleDownEvent(scaleDownStatus, time.Now()); noScaleDownEvent != nil {
		err = p.tryLogEvent(noScaleDownEvent)
		if err != nil {
			klog.Errorf("Error logging visibility event for no scale-down: %v", err)
		} else {
			eventSentCallback(time.Now())
		}
	}
}

// CleanUp closes the internal logger.
func (p *ScaleDownStatusVisibilityProcessor) CleanUp() {
	if IsNil(p.logger) {
		klog.Info("Visibility events disabled; nothing to Cleanup")
		return
	}

	err := p.logger.Close()
	if err != nil {
		klog.Errorf("Error while closing the Stackdriver client: %v", err)
	}
}

func (p *ScaleDownStatusVisibilityProcessor) processNodeDeleteResults(scaleDownStatus *vistypes.ScaleDownStatus) {
	for nodeName, nodeDeletionResult := range scaleDownStatus.NodeDeleteResults {
		ndInfo, found := p.nodeDeletionInfos[nodeName]
		if !found {
			klog.Errorf("Couldn't find node deletion info for a node delete nodeDeletionResult (node id: %v).", nodeName)
			continue
		}
		ngScaleDownInfo, found := p.nodeGroupScaleDownInfos[ndInfo.nodeGroupId]
		if !found {
			klog.Errorf("Couldn't find node group scale down info for a node group (node group id: %v).", ndInfo.nodeGroupId)
			continue
		}

		switch nodeDeletionResult.ResultType {
		case scaledownstatus.NodeDeleteErrorFailedToMarkToBeDeleted:
			ngScaleDownInfo.errMsgs = append(ngScaleDownInfo.errMsgs, vistypes.NewScaleDownErrorFailedToMarkToBeDeletedMsg(nodeName))
		case scaledownstatus.NodeDeleteErrorFailedToDelete:
			if _, ok := nodeDeletionResult.Err.(gke.MinSizeReachedError); ok {
				ngScaleDownInfo.errMsgs = append(ngScaleDownInfo.errMsgs, vistypes.NewScaleDownErrorFailedToDeleteNodeMinSizeReachedMsg(nodeName))
			} else {
				ngScaleDownInfo.errMsgs = append(ngScaleDownInfo.errMsgs, vistypes.NewScaleDownErrorFailedToDeleteNodeOtherMsg(nodeName))
			}
		case scaledownstatus.NodeDeleteErrorFailedToEvictPods:
			failingPodNames := make([]string, 0)
			for _, podEvictionResult := range nodeDeletionResult.PodEvictionResults {
				if !podEvictionResult.WasEvictionSuccessful() {
					failingPodNames = append(failingPodNames, podEvictionResult.Pod.Name)
					if len(failingPodNames) >= visibility.MaxPodsInEvent {
						break
					}
				}
			}
			ngScaleDownInfo.errMsgs = append(ngScaleDownInfo.errMsgs, vistypes.NewScaleDownErrorFailedToEvictPodsMsg(nodeName, failingPodNames))
		}

		ngScaleDownInfo.pendingDeletions -= 1

		if ngScaleDownInfo.pendingDeletions == 0 {
			delete(p.nodeGroupScaleDownInfos, ndInfo.nodeGroupId)
			p.data.MarkNodeGroupTargetSizeStable(ndInfo.nodeGroupId)
			if len(ngScaleDownInfo.errMsgs) > 0 {
				p.data.FailNodeGroupForEvent(ndInfo.eventId, ndInfo.nodeGroupId, ngScaleDownInfo.errMsgs)
			}
		}

		delete(p.nodeDeletionInfos, nodeName)
	}
}

func (p *ScaleDownStatusVisibilityProcessor) processNoScaleDownEvent(scaleDownStatus *vistypes.ScaleDownStatus, now time.Time) (event *vispb.AutoscalerEvent, eventSentCallback func(time.Time)) {
	unreportedReasons := p.noScaleDown.GetNewReasons(scaleDownStatus, now)
	if unreportedReasons.IsEmpty() {
		return nil, nil
	}

	data := new(vispb.NoScaleDownData)
	if unreportedReasons.TopLevel != nil {
		data.Reason = unreportedReasons.TopLevel.Proto()
	}
	if len(unreportedReasons.UnremovableNodes) > 0 {
		data.Nodes = make([]*vispb.NodeExplanation, 0, len(unreportedReasons.UnremovableNodes))
		data.NodesTotalCount = int32(len(unreportedReasons.UnremovableNodes))

		if len(unreportedReasons.UnremovableNodes) > visibility.MaxNodesInEvent {
			unreportedReasons.UnremovableNodes = unreportedReasons.UnremovableNodes[:visibility.MaxNodesInEvent]
		}

		for _, nodeReason := range unreportedReasons.UnremovableNodes {
			data.Nodes = append(data.Nodes, nodeReason.Proto())
		}
	}

	eventSentCallback = func(sendingTime time.Time) {
		p.noScaleDown.MarkReasonsReported(unreportedReasons, sendingTime)
	}

	return &vispb.AutoscalerEvent{EventOneof: &vispb.AutoscalerEvent_NoDecisionStatus{
		NoDecisionStatus: &vispb.NoDecisionStatus{
			MeasureTime: now.Unix(),
			KindOneof:   &vispb.NoDecisionStatus_NoScaleDown{NoScaleDown: data},
		},
	}}, eventSentCallback
}

func (p *ScaleDownStatusVisibilityProcessor) processScaleDownEvent(scaleDownStatus *vistypes.ScaleDownStatus, now time.Time) *vispb.AutoscalerEvent {
	if scaleDownStatus.Result != scaledownstatus.ScaleDownNodeDeleteStarted {
		return nil
	}

	eventId := p.idGen.GenerateID()
	targetNodeGroupIds := make(map[string]bool)

	for _, scaleDownNode := range scaleDownStatus.ScaledDownNodes {
		if scaleDownStatus.Result == scaledownstatus.ScaleDownNodeDeleteStarted {
			p.data.MarkNodeGroupTargetSizeUnstable(scaleDownNode.Node.Mig.Id)
			if _, found := p.nodeGroupScaleDownInfos[scaleDownNode.Node.Mig.Id]; !found {
				p.nodeGroupScaleDownInfos[scaleDownNode.Node.Mig.Id] = &nodeGroupScaleDownInfo{pendingDeletions: 0, errMsgs: make([]*vistypes.Message, 0)}
			}
			p.nodeGroupScaleDownInfos[scaleDownNode.Node.Mig.Id].pendingDeletions += 1
			p.nodeDeletionInfos[scaleDownNode.Node.Name] = nodeDeletionInfo{nodeGroupId: scaleDownNode.Node.Mig.Id, eventId: eventId}
		}

		targetNodeGroupIds[scaleDownNode.Node.Mig.Id] = true
	}

	p.data.StartEvent(eventId, targetNodeGroupIds, nil /* we don't store data about podsTriggeringScaleUp in case of scale down */)

	decision := &vispb.DecisionEvent{
		DecideTime: now.Unix(),
		EventId:    eventId,
		DecisionOneof: &vispb.DecisionEvent_ScaleDown{
			ScaleDown: scaleDownStatus.ScaleDownDataProto(),
		},
	}
	return &vispb.AutoscalerEvent{
		EventOneof: &vispb.AutoscalerEvent_Decision{Decision: decision},
	}
}

func (p *ScaleDownStatusVisibilityProcessor) processNodePoolDeletedEvent(scaleDownStatus *vistypes.ScaleDownStatus, now time.Time) *vispb.AutoscalerEvent {
	if len(scaleDownStatus.RemovedMigs) == 0 {
		return nil
	}

	decision := &vispb.DecisionEvent{
		DecideTime: now.Unix(),
		EventId:    p.idGen.GenerateID(),
		DecisionOneof: &vispb.DecisionEvent_NodePoolDeleted{
			NodePoolDeleted: scaleDownStatus.NodePoolDeletedDataProto(),
		},
	}
	return &vispb.AutoscalerEvent{
		EventOneof: &vispb.AutoscalerEvent_Decision{Decision: decision},
	}
}

// log visibility event only when logger is initiallized (if AutoscalerVisibility is enabled)
func (p *ScaleDownStatusVisibilityProcessor) tryLogEvent(event *vispb.AutoscalerEvent) error {
	if IsNil(p.logger) {
		return nil
	}
	return p.logger.LogEvent(event)
}
