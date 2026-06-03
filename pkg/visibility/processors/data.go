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
	"sync"
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility"
	vispb "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/proto"
	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
)

type nodeGroupStatus struct {
	reachedTargetSize bool
	failed            bool
}

func (ngs *nodeGroupStatus) isFinished() bool {
	return ngs.reachedTargetSize || ngs.failed
}

type eventInfo struct {
	id                    string
	targetNodeGroups      map[string]*nodeGroupStatus
	expirationTime        time.Time
	podsTriggeringScaleUp []*vistypes.Pod
}

func (ei *eventInfo) isFinished() bool {
	for _, ngStatus := range ei.targetNodeGroups {
		if !ngStatus.isFinished() {
			return false
		}
	}
	return true
}

func (ei *eventInfo) isSuccessful() bool {
	for _, ngStatus := range ei.targetNodeGroups {
		if !ngStatus.reachedTargetSize {
			return false
		}
	}
	return true
}

func (ei *eventInfo) timedOut() bool {
	return time.Now().After(ei.expirationTime)
}

// SharedData holds the data needed for generating the visibility events that needs to be shared between processors.
type SharedData struct {
	lock               sync.Mutex
	currentEvents      map[string]*eventInfo
	eventResultsBuffer []*vispb.EventResult

	// nodeGroupsWithUnstableTargetSize contains node groups with unstable target sizes. The values are expiration
	// times, after which the node group is considered as having stable target size again.
	nodeGroupsWithUnstableTargetSize map[string]time.Time
}

// NewSharedData returns an empty initialized instance of SharedData, ready for use.
func NewSharedData() *SharedData {
	return &SharedData{currentEvents: make(map[string]*eventInfo), nodeGroupsWithUnstableTargetSize: make(map[string]time.Time),
		eventResultsBuffer: make([]*vispb.EventResult, 0)}
}

// StartEvent adds a new event to the shared data.
func (sd *SharedData) StartEvent(eventId string, targetNodeGroupIds map[string]bool, pods []*vistypes.Pod) {
	sd.lock.Lock()
	defer sd.lock.Unlock()

	targetNodeGroups := make(map[string]*nodeGroupStatus)
	for nodeGroupId := range targetNodeGroupIds {
		targetNodeGroups[nodeGroupId] = &nodeGroupStatus{reachedTargetSize: false, failed: false}
	}
	sd.currentEvents[eventId] = &eventInfo{id: eventId, targetNodeGroups: targetNodeGroups, expirationTime: time.Now().Add(visibility.EventExpirationTimeout), podsTriggeringScaleUp: pods}
}

// PeriodicCleanup cleans up no longer needed data from the structure.
func (sd *SharedData) PeriodicCleanup() {
	sd.lock.Lock()
	defer sd.lock.Unlock()

	// Clean up stale event data.
	for eventId, event := range sd.currentEvents {
		if event.isFinished() || event.timedOut() {
			delete(sd.currentEvents, eventId)
		}
	}

	// Clean up stale node group target size unstability data.
	for nodeGroupId, expirationTime := range sd.nodeGroupsWithUnstableTargetSize {
		if time.Now().After(expirationTime) {
			delete(sd.nodeGroupsWithUnstableTargetSize, nodeGroupId)
		}
	}
}

// GetNextResults returns those results from all events that haven't been emitted before.
func (sd *SharedData) GetNextResults() []*vispb.EventResult {
	sd.lock.Lock()
	defer sd.lock.Unlock()

	nextResults := sd.eventResultsBuffer
	sd.eventResultsBuffer = make([]*vispb.EventResult, 0)
	return nextResults
}

// GetUnfinishedNodeGroupIds returns a set of ids of the node groups that still need to reach
// their target size for some event to be completed.
func (sd *SharedData) GetUnfinishedNodeGroupIds() map[string]bool {
	sd.lock.Lock()
	defer sd.lock.Unlock()

	result := make(map[string]bool)
	for _, event := range sd.currentEvents {
		for nodeGroupId, ngStatus := range event.targetNodeGroups {
			if !ngStatus.isFinished() {
				result[nodeGroupId] = true
			}
		}
	}
	return result
}

// FinishNodeGroup should be called when the node group actual size meets its target size.
func (sd *SharedData) FinishNodeGroup(nodeGroupId string) {
	sd.lock.Lock()
	defer sd.lock.Unlock()

	if _, found := sd.nodeGroupsWithUnstableTargetSize[nodeGroupId]; found {
		return
	}

	for _, event := range sd.currentEvents {
		if ngStatus, found := event.targetNodeGroups[nodeGroupId]; found && !ngStatus.isFinished() {
			ngStatus.reachedTargetSize = true
			if event.isSuccessful() {
				sd.eventResultsBuffer = append(sd.eventResultsBuffer, &vispb.EventResult{EventId: event.id})
			}
		}
	}
}

// GetEvent returns event associated with given eventId.
func (sd *SharedData) GetEvent(eventId string) *eventInfo {
	sd.lock.Lock()
	defer sd.lock.Unlock()
	return sd.currentEvents[eventId]
}

// FailNodeGroup should be called when there are node group errors encountered that relate to the whole node group. Returns event ids of events associated with given nodeGroupId.
func (sd *SharedData) FailNodeGroup(nodeGroupId string, errMsgs []*vistypes.Message) []string {
	sd.lock.Lock()
	defer sd.lock.Unlock()
	var eventIds []string

	for _, event := range sd.currentEvents {
		if ngStatus, found := event.targetNodeGroups[nodeGroupId]; found && !ngStatus.isFinished() {
			ngStatus.failed = true
			for _, errMsg := range errMsgs {
				sd.eventResultsBuffer = append(sd.eventResultsBuffer, &vispb.EventResult{EventId: event.id, ErrorMsg: errMsg.Proto()})
			}
			eventIds = append(eventIds, event.id)
		}
	}
	return eventIds
}

// FailNodeGroupForEvent should be called when there are node group errors encountered that relate to one event only.
func (sd *SharedData) FailNodeGroupForEvent(eventId, nodeGroupId string, errMsgs []*vistypes.Message) {
	sd.lock.Lock()
	defer sd.lock.Unlock()

	if event, found := sd.currentEvents[eventId]; found {
		if ngStatus, found := event.targetNodeGroups[nodeGroupId]; found && !ngStatus.isFinished() {
			ngStatus.failed = true
			for _, errMsg := range errMsgs {
				sd.eventResultsBuffer = append(sd.eventResultsBuffer, &vispb.EventResult{EventId: event.id, ErrorMsg: errMsg.Proto()})
			}
		}
	}
}

// MarkNodeGroupTargetSizeUnstable should be called when a node group is in the process of changing
// its target size - e.g. a drain is happening and target size is being changed asynchronously.
func (sd *SharedData) MarkNodeGroupTargetSizeUnstable(nodeGroupId string) {
	sd.lock.Lock()
	defer sd.lock.Unlock()
	sd.nodeGroupsWithUnstableTargetSize[nodeGroupId] = time.Now().Add(visibility.NodeGroupTargetSizeUnstabilityTimeout)
}

// MarkNodeGroupTargetSizeStable should be called when a node group is no longer in the process of
// changing its target size.
func (sd *SharedData) MarkNodeGroupTargetSizeStable(nodeGroupId string) {
	sd.lock.Lock()
	defer sd.lock.Unlock()
	delete(sd.nodeGroupsWithUnstableTargetSize, nodeGroupId)
}
