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

package capacitybuffer

import (
	"time"
)

// bufferState tracks the capacity buffer fake pods, so that we're able to report their first reaction time.
//
// As fake pods don't have identity, we only track the number of pods in different states and the time unreported pods appeared for the first time.
// We try to replicate behaviour of baloon pods.
//
// We assume the following lifecycle of a fake pod: unevaluated <-> remain unschedulable -> trigger scale up -> schedulable
// Rules:
//   - Fake pods can start from any of these statuses and skip them during their lifetime.
//   - Fake pod that moves forward in this lifecycle is considered the same pod.
//   - Fake pod that moves backward in this lifecycle is considered a new pod and reported separately.
//   - Fake pod that remain unschedulable can be reported as "unevaluated" in a consecutive reconciliation and they would be still considered the same pod.
//     We assume CA in this particular loop didn't manage to look at those pods.
//   - Fake pods cannot "stay" in "trigger scale up" status; it is assumed that on next check they will be schedulable.
//     If there is a pod that triggered scale up in two consecutive reconciliation, it is considered a new pod.
//   - Fake pod creation date is the time when the pod is first injected.
//
// Callers are expected to call in each CA loop:
// - ProcessInjectedPods to record newly injected pods and their creation time. Should be called exactly once.
// - ProcessScaleUpStatus to update the state of buffer pods after scale-up decision is made and get metrics to report. Can be called any number of times or not called at all.
type bufferState struct {
	// Queue of "creation" times of fake pods that are not yet reported. Items must be added in ascending order.
	unreportedQueue   timeQueue
	pendingReported   int // number of pods considered pending (unschedulable or timed out) with metric already reported
	completed         int // number of pods considered complete (schedulable or to be schedulable after scale-up)
	lastInjectionTime time.Time
}

// reactionsToReport has lists of creation times of fake pods that should be reported with different reaction types.
type reactionsToReport struct {
	timeouts       []time.Time
	noActionNeeded []time.Time
	scaleUp        []time.Time
	unhelpable     []time.Time
}

func NewBufferState() *bufferState {
	return &bufferState{}
}

func (bs *bufferState) ProcessInjectedPods(totalInjected int, now time.Time, timeout time.Duration) reactionsToReport {
	bs.lastInjectionTime = now

	podsWithDecision := bs.completed + bs.pendingReported + bs.unreportedQueue.Len()

	// add new pods if the number increased; decreased number of pods will be handled in ProcessScaleUpStatus
	if podsWithDecision < totalInjected {
		bs.unreportedQueue.EnqueueMany(bs.lastInjectionTime, totalInjected-podsWithDecision)
	}

	return reactionsToReport{
		timeouts: bs.processTimeouts(now.Add(-timeout)),
	}
}

func (bs *bufferState) ProcessScaleUpStatus(
	schedulablePods, podsTriggeringScaleUp, podsRemainUnschedulable, podsAwaitEvaluation int,
	now time.Time,
) reactionsToReport {
	return reactionsToReport{
		noActionNeeded: bs.processCompletedPods(schedulablePods, now),
		scaleUp:        bs.processPodsTriggeringScaleUp(podsTriggeringScaleUp, now),
		unhelpable:     bs.processPendingPods(podsRemainUnschedulable, podsAwaitEvaluation, now),
	}
}

func (bs *bufferState) processTimeouts(minCreationTime time.Time) []time.Time {
	timeouts := []time.Time{}

	item, ok := bs.unreportedQueue.Peek()
	for ok && !item.After(minCreationTime) {
		timeouts = append(timeouts, item)
		bs.unreportedQueue.Dequeue()
		item, ok = bs.unreportedQueue.Peek()
	}

	// mark timeouts as reported
	bs.pendingReported += len(timeouts)

	return timeouts
}

func (bs *bufferState) processCompletedPods(numberOfPods int, now time.Time) []time.Time {
	podsToPromote := max(0, numberOfPods-bs.completed) // excess pods are forgotten
	bs.completed = numberOfPods

	return bs.promotePendingReportedPods(podsToPromote, now)
}

func (bs *bufferState) processPodsTriggeringScaleUp(numberOfPods int, now time.Time) []time.Time {
	bs.completed += numberOfPods

	return bs.promotePendingReportedPods(numberOfPods, now)
}

func (bs *bufferState) processPendingPods(podsRemainUnschedulable int, podsAwaitEvaluation int, now time.Time) []time.Time {
	newTotalPending := podsRemainUnschedulable + podsAwaitEvaluation
	// we should not decrease the number of pending reported pods unless it's above total pending pods
	newPendingReported := min(newTotalPending, max(podsRemainUnschedulable, bs.pendingReported))
	newUnreported := newTotalPending - newPendingReported

	// process reported
	podsToPromote := max(0, newPendingReported-bs.pendingReported)
	toReport := bs.promoteUnreportedPods(podsToPromote, now)
	bs.pendingReported = newPendingReported

	// process unreported
	diff := newUnreported - bs.unreportedQueue.Len()
	if diff > 0 {
		bs.unreportedQueue.EnqueueMany(bs.getCreationTime(now), diff)
	} else if diff < 0 {
		for i := 0; i < -diff; i++ {
			bs.unreportedQueue.Dequeue()
		}
	}

	return toReport
}

func (bs *bufferState) promotePendingReportedPods(numberOfPods int, now time.Time) []time.Time {
	toRemove := min(bs.pendingReported, numberOfPods)
	bs.pendingReported -= toRemove

	// "promote" remaining pods (if any) from unreported
	return bs.promoteUnreportedPods(numberOfPods-toRemove, now)
}

func (bs *bufferState) promoteUnreportedPods(numberOfPods int, now time.Time) []time.Time {
	result := make([]time.Time, numberOfPods)

	createdTime := bs.getCreationTime(now)

	for i := 0; i < numberOfPods; i++ {
		if item, ok := bs.unreportedQueue.Dequeue(); ok {
			// promote existing item
			result[i] = item
		} else {
			// create new item - normally shouldn't happen (new pods should be added in ProcessInjectedPods)
			result[i] = createdTime
		}
	}

	return result
}

func (bs *bufferState) getCreationTime(now time.Time) time.Time {
	if bs.lastInjectionTime.IsZero() {
		return now
	}
	return bs.lastInjectionTime
}
