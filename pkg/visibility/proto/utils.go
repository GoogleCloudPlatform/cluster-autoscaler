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

package proto

import klog "k8s.io/klog/v2"

// EventType represents the type of a CA Viz event.
type EventType string

const (
	statusEvent          EventType = "status"
	scaleUpEvent         EventType = "scale_up"
	scaleDownEvent       EventType = "scale_down"
	noScaleUpEvent       EventType = "no_scale_up"
	noScaleDownEvent     EventType = "no_scale_down"
	nodePoolCreatedEvent EventType = "node_pool_created"
	nodePoolDeletedEvent EventType = "node_pool_deleted"
	resultInfoEvent      EventType = "result_info"
	unknownEvent         EventType = "UNKNOWN"
)

// GetType returns the type of the event as a string.
func (e *AutoscalerEvent) GetType() EventType {
	if e.GetStatus() != nil {
		return statusEvent
	}
	if e.GetResultInfo() != nil {
		return resultInfoEvent
	}

	noDecisionStatus := e.GetNoDecisionStatus()
	if noDecisionStatus != nil && noDecisionStatus.GetNoScaleUp() != nil {
		return noScaleUpEvent
	}
	if noDecisionStatus != nil && noDecisionStatus.GetNoScaleDown() != nil {
		return noScaleDownEvent
	}

	decision := e.GetDecision()
	if decision == nil {
		klog.Errorf("CA Viz event: status, results, no decision status and decision fields all empty, this should never happen")
		return unknownEvent
	}
	if decision.GetScaleUp() != nil {
		return scaleUpEvent
	}
	if decision.GetScaleDown() != nil {
		return scaleDownEvent
	}
	if decision.GetNodePoolCreated() != nil {
		return nodePoolCreatedEvent
	}
	if decision.GetNodePoolDeleted() != nil {
		return nodePoolDeletedEvent
	}
	klog.Errorf("CA Viz event: all possible decision event fields empty, this should never happen")
	return unknownEvent
}
