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
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/utils"
	klog "k8s.io/klog/v2"
)

// CapacityRequestScaleUpProcessor sets appropriate status on Capacity Requests
// looking at status from scale up.
type CapacityRequestScaleUpProcessor struct {
	crStatus *utils.CapacityRequestState
}

// NewCapacityRequestScaleUpProcessor creates a scale up processor for setting
// status in Capacity Requests.
func NewCapacityRequestScaleUpProcessor(crStatus *utils.CapacityRequestState) *CapacityRequestScaleUpProcessor {
	return &CapacityRequestScaleUpProcessor{crStatus: crStatus}

}

// Process processes scale up status by setting appropriate status on Capacity Requests.
func (p *CapacityRequestScaleUpProcessor) Process(context *context.AutoscalingContext, status *status.ScaleUpStatus) {
	for _, noScaleUpInfo := range status.PodsRemainUnschedulable {
		if cr, found := p.crStatus.PodToCapacityRequest(noScaleUpInfo.Pod); found {
			if err := p.crStatus.SetResourcesUnattainable(cr); err != nil {
				klog.Errorf("Failed to set status for Capacity Request %v/%v: %v", cr.Namespace, cr.Name, err)
			}
		}
	}
	for _, pod := range status.PodsTriggeredScaleUp {
		if cr, found := p.crStatus.PodToCapacityRequest(pod); found {
			if err := p.crStatus.SetResourcesInProvisioning(cr); err != nil {
				klog.Errorf("Failed to set status for Capacity Request %v/%v: %v", cr.Namespace, cr.Name, err)
			}
		}
	}
	for _, pod := range status.PodsAwaitEvaluation {
		if cr, found := p.crStatus.PodToCapacityRequest(pod); found {
			if err := p.crStatus.SetResourcesUnknown(cr); err != nil {
				klog.Errorf("Failed to set status for Capacity Request %v/%v: %v", cr.Namespace, cr.Name, err)
			}
		}
	}
}

// CleanUp cleans up the processor's internal structures.
func (p *CapacityRequestScaleUpProcessor) CleanUp() {
}
