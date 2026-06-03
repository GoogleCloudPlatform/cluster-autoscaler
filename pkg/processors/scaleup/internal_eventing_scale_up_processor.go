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
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/utils"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
)

const (
	crType  = "CapacityRequest"
	prType  = "ProvisioningRequest"
	podType = "Pod"
)

// InternalEventingScaleUpStatusProcessor processes the state of the cluster after
// a scale-up by emitting relevant events for pods, capacity requests, and provisioning requests
// depending on their post scale-up status.
type InternalEventingScaleUpStatusProcessor struct {
	crState            *utils.CapacityRequestState
	processCapacityReq bool
	processProvReq     bool
}

// NewInternalEventingScaleUpStatusProcessor creates a new processor for emitting
// relevant events for pods, capacity requests, and provisioning requests depending on their post
// scale-up status.
func NewInternalEventingScaleUpStatusProcessor() *InternalEventingScaleUpStatusProcessor {
	return &InternalEventingScaleUpStatusProcessor{}
}

func (p *InternalEventingScaleUpStatusProcessor) EnableCapacityReqProcessing(crState *utils.CapacityRequestState) {
	p.crState = crState
	p.processCapacityReq = true
}

func (p *InternalEventingScaleUpStatusProcessor) EnableProvReqProcessing() {
	p.processProvReq = true
}

// Process processes the state of the cluster after a scale-up by emitting
// relevant events for pods, capacity requests, and provisioning requests depending on their post
// scale-up status.
func (p *InternalEventingScaleUpStatusProcessor) Process(context *context.AutoscalingContext, scaleUpStatus *status.ScaleUpStatus) {
	consideredNodeGroupsMap := nodeGroupListToMapById(scaleUpStatus.ConsideredNodeGroups)
	if scaleUpStatus.Result != status.ScaleUpSuccessful {
		processedPRs := sets.New[types.UID]()
		for _, noScaleUpInfo := range scaleUpStatus.PodsRemainUnschedulable {

			if p.processProvReq {
				if prRef, found := pods.InjectedPodProvReqRef(noScaleUpInfo.Pod); found {
					if processedPRs.Has(prRef.UID) {
						continue
					}
					recordNotTriggeredScaleUpEvent(prRef, prType, context, status.ReasonsMessage(scaleUpStatus.Result, noScaleUpInfo, consideredNodeGroupsMap))
					processedPRs.Insert(prRef.UID)
					continue
				}
			}

			if p.processCapacityReq {
				if cr, found := p.crState.PodToCapacityRequest(noScaleUpInfo.Pod); found {
					recordNotTriggeredScaleUpEvent(cr, crType, context, status.ReasonsMessage(scaleUpStatus.Result, noScaleUpInfo, consideredNodeGroupsMap))
					continue
				}
			}

			recordNotTriggeredScaleUpEvent(noScaleUpInfo.Pod, podType, context, status.ReasonsMessage(scaleUpStatus.Result, noScaleUpInfo, consideredNodeGroupsMap))
		}
	}

	if len(scaleUpStatus.ScaleUpInfos) > 0 {
		for _, pod := range scaleUpStatus.PodsTriggeredScaleUp {

			if p.processCapacityReq {
				if cr, found := p.crState.PodToCapacityRequest(pod); found {
					recordTriggeredScaleUpEvent(cr, crType, context, scaleUpStatus)
					continue
				}
			}

			recordTriggeredScaleUpEvent(pod, podType, context, scaleUpStatus)
		}
	}
}

// CleanUp cleans up the processor's internal structures.
func (p *InternalEventingScaleUpStatusProcessor) CleanUp() {
}

func recordNotTriggeredScaleUpEvent(obj runtime.Object, objectType string, context *context.AutoscalingContext, reasons string) {
	context.Recorder.Eventf(obj, apiv1.EventTypeNormal, "NotTriggerScaleUp",
		"%v didn't trigger scale-up: %s", objectType, reasons)
}

func recordTriggeredScaleUpEvent(obj runtime.Object, objectType string, context *context.AutoscalingContext, scaleUpStatus *status.ScaleUpStatus) {
	context.Recorder.Eventf(obj, apiv1.EventTypeNormal, "TriggeredScaleUp",
		"%v triggered scale-up: %v", objectType, scaleUpStatus.ScaleUpInfos)
}

func nodeGroupListToMapById(nodeGroups []cloudprovider.NodeGroup) map[string]cloudprovider.NodeGroup {
	result := make(map[string]cloudprovider.NodeGroup)
	for _, nodeGroup := range nodeGroups {
		result[nodeGroup.Id()] = nodeGroup
	}
	return result
}
