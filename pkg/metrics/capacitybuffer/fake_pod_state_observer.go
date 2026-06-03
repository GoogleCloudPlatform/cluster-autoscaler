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
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer"
	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/fakepods"
	ca_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/logging"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/podstate"
	podutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/pod"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/systempods"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

const logLimit = 100
const logWindow = 1 * time.Minute
const overdueReactionThreshold = 1 * time.Minute

// FakePodStateObserver is responsible for reporting fake capacity buffer pods first reaction times.
// In each CA loop the following functions should be called:
// - Reset at the beginning of the CA loop
// - ObserveInjectedPods (can be called multiple times, exactly once per CapacityBuffer)
// - ObserveSchedulablePods (can be called multiple times, at most once per pod)
// - Process each time scale up happen (part of scale up status processor chain)
type FakePodStateObserver struct {
	lock                 sync.Mutex
	systemPodsClassifier systempods.Classifier
	observer             FakePodReactionTimeObserver
	bufferPodsRegistry   *fakepods.Registry // It gets cleared at the beginning of the CA loop.
	clock                clock.Clock
	logLimiter           *logging.LogRateLimiter
	stateByBuffer        map[types.UID]*bufferState // holds the reporting state of each buffer
	// recreated on each CA loop:
	podsSnapshotByBuffer map[types.UID]*bufferPodsSnapshot // pod scheduling status in current CA loop
}

// FakePodReactionTimeObserver is an interface for reporting fake pod reaction times metric.
type FakePodReactionTimeObserver interface {
	ObserveCapacityBufferFakePodReactionTime(duration time.Duration, systemPod bool, hasPVC bool, hasCSI bool, reactionType metrics.ReactionType, provisioningType, allocationMode string)
}

// snapshot of a capacity buffers pods scheduling state in the current CA loop
type bufferPodsSnapshot struct {
	buffer    *v1beta1.CapacityBuffer
	samplePod *v1.Pod
	pods      map[types.UID]podSchedulingState
}

type podSchedulingState int

const (
	unevaluated podSchedulingState = iota
	awaitEvaluation
	remainUnschedulable
	triggeredScaleUp
	schedulable
)

// summary of a capacity buffers pods scheduling states.
type bufferPodsReport struct {
	buffer                  *v1beta1.CapacityBuffer
	samplePod               *v1.Pod
	injectedPods            int
	schedulablePods         int
	podsTriggeredScaleUp    int
	podsRemainUnschedulable int
	podsAwaitEvaluation     int
}

// pod classification used to build metrics labels.
type podClassification struct {
	systemPod            bool
	hasPVC               bool
	hasCSI               bool
	provisioningStrategy string
}

func NewFakePodStateObserver(systemPodsClassifier systempods.Classifier, observer FakePodReactionTimeObserver, bufferPodsRegistry *fakepods.Registry, clock clock.Clock, enableLogRateLimiter bool) *FakePodStateObserver {
	var logLimiter *logging.LogRateLimiter
	if enableLogRateLimiter {
		logLimiter = logging.NewLogRateLimiter(logLimit, logWindow)
	}

	return &FakePodStateObserver{
		systemPodsClassifier: systemPodsClassifier,
		observer:             observer,
		bufferPodsRegistry:   bufferPodsRegistry,
		clock:                clock,
		stateByBuffer:        make(map[types.UID]*bufferState),
		logLimiter:           logLimiter,
		podsSnapshotByBuffer: make(map[types.UID]*bufferPodsSnapshot),
	}
}

// Reset clears the loop-specific state. It should be called at the beginning of each CA loop.
func (f *FakePodStateObserver) Reset() {
	f.lock.Lock()
	defer f.lock.Unlock()
	clear(f.podsSnapshotByBuffer)
}

// ObserveInjectedPods is called after CapacityBuffer fake pods are injected.
// It can be called multiple times during CA loop.
// It is assumed that for each CapacityBuffer it is called exactly once per CA loop.
// It is safe to be called outside CA loop (and from other goroutines).
func (f *FakePodStateObserver) ObserveInjectedPods(allPods []*v1.Pod) {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.updateSnapshots(allPods)
	f.processInjectedPods()
	f.removeDeletedBuffers()
}

// ObserveSchedulablePods is called each time pods are marked as schedulable.
// It can be called multiple times during CA loop, but it should not report the same pod multiple times
// (in an edge case this can cause the same buffer to be reported multiple times).
// It is safe to be called outside CA loop (and from other goroutines).
// This function can report the metric if all buffer's pods have been processed within the CA loop.
func (f *FakePodStateObserver) ObserveSchedulablePods(pods []*v1.Pod) {
	f.lock.Lock()
	defer f.lock.Unlock()

	now := f.clock.Now()

	reportedSnapshots := make(map[types.UID]*bufferPodsSnapshot)
	for _, pod := range pods {
		snapshot := f.getBufferPodsSnapshotForPod(pod.UID)
		if snapshot != nil {
			snapshot.pods[pod.UID] = schedulable
			reportedSnapshots[snapshot.buffer.UID] = snapshot
		}
	}

	for _, snapshot := range reportedSnapshots {
		podsReport := getReportFromSnapshot(snapshot)
		f.processAndReportBuffer(now, podsReport)
	}
}

// Process processes the scale-up status (implementation of ScaleUpStatusProcessor)
// It can be called multiple times during CA loop. It is safe to be called outside CA loop (and from other goroutines).
// This function can report the metric if all buffer's pods have been processed within the CA loop.
func (f *FakePodStateObserver) Process(context *ca_context.AutoscalingContext, scaleUpStatus *status.ScaleUpStatus) {
	f.lock.Lock()
	defer f.lock.Unlock()

	now := f.clock.Now()

	reportedSnapshots := make(map[types.UID]*bufferPodsSnapshot)

	for _, pod := range scaleUpStatus.PodsTriggeredScaleUp {
		snapshot := f.getBufferPodsSnapshotForPod(pod.UID)
		if snapshot != nil {
			snapshot.pods[pod.UID] = triggeredScaleUp
			reportedSnapshots[snapshot.buffer.UID] = snapshot
		}
	}

	for _, noScaleUpInfo := range scaleUpStatus.PodsRemainUnschedulable {
		snapshot := f.getBufferPodsSnapshotForPod(noScaleUpInfo.Pod.UID)
		if snapshot != nil {
			snapshot.pods[noScaleUpInfo.Pod.UID] = remainUnschedulable
			reportedSnapshots[snapshot.buffer.UID] = snapshot
		}
	}

	for _, pod := range scaleUpStatus.PodsAwaitEvaluation {
		snapshot := f.getBufferPodsSnapshotForPod(pod.UID)
		if snapshot != nil {
			snapshot.pods[pod.UID] = awaitEvaluation
			reportedSnapshots[snapshot.buffer.UID] = snapshot
		}
	}

	for _, snapshot := range reportedSnapshots {
		podsReport := getReportFromSnapshot(snapshot)
		f.processAndReportBuffer(now, podsReport)
	}
}

// CleanUp is called when CA is shutting down (implementation of ScaleUpStatusProcessor).
func (f *FakePodStateObserver) CleanUp() {}

func (f *FakePodStateObserver) updateSnapshots(allPods []*v1.Pod) {
	// populate podsSnapshotByBuffer for all capacity buffer fake pods
	for _, pod := range allPods {
		buffer := f.bufferPodsRegistry.GetCapacityBuffer(pod.UID)
		if buffer == nil {
			continue // not capacity buffer fake pod
		}

		snapshot, ok := f.podsSnapshotByBuffer[buffer.UID]
		if !ok {
			snapshot = &bufferPodsSnapshot{
				buffer:    buffer,
				samplePod: pod,
				pods:      make(map[types.UID]podSchedulingState),
			}
			f.podsSnapshotByBuffer[buffer.UID] = snapshot
		}
		if _, ok := snapshot.pods[pod.UID]; !ok {
			snapshot.pods[pod.UID] = unevaluated
		}
	}
}

func (f *FakePodStateObserver) processInjectedPods() {
	now := f.clock.Now()

	for _, snapshot := range f.podsSnapshotByBuffer {
		state := f.getOrCreateBufferState(snapshot.buffer.UID)
		report := state.ProcessInjectedPods(len(snapshot.pods), now, metrics.MaxReactionTime)
		f.observeReactions(report, now, snapshot.buffer, snapshot.samplePod)
	}
}

func (f *FakePodStateObserver) getOrCreateBufferState(bufferUID types.UID) *bufferState {

	state, ok := f.stateByBuffer[bufferUID]
	if !ok {
		state = NewBufferState()
		f.stateByBuffer[bufferUID] = state
		f.logWithRateLimit(klog.Level(5), "CapacityBufferFakePodStateObserver: Starting to observe buffer (UID=%v)", bufferUID)
	}

	return state
}

func (f *FakePodStateObserver) getBufferPodsSnapshotForPod(podUID types.UID) *bufferPodsSnapshot {
	buffer := f.bufferPodsRegistry.GetCapacityBuffer(podUID)
	if buffer == nil {
		// not a capacity buffer fake pod
		return nil
	}

	return f.podsSnapshotByBuffer[buffer.UID]
}

func getReportFromSnapshot(snapshot *bufferPodsSnapshot) *bufferPodsReport {
	report := &bufferPodsReport{
		buffer:       snapshot.buffer,
		samplePod:    snapshot.samplePod,
		injectedPods: len(snapshot.pods),
	}
	for _, state := range snapshot.pods {
		switch state {
		case schedulable:
			report.schedulablePods++
		case triggeredScaleUp:
			report.podsTriggeredScaleUp++
		case remainUnschedulable:
			report.podsRemainUnschedulable++
		case awaitEvaluation:
			report.podsAwaitEvaluation++
		}
	}
	return report
}

func (f *FakePodStateObserver) processAndReportBuffer(now time.Time, podsReport *bufferPodsReport) {
	state := f.getOrCreateBufferState(podsReport.buffer.UID)

	reportedPods := podsReport.schedulablePods + podsReport.podsTriggeredScaleUp + podsReport.podsRemainUnschedulable + podsReport.podsAwaitEvaluation

	// do not process and report when not all injected pods have been processed by CA
	if reportedPods != podsReport.injectedPods {
		return
	}

	// process buffer scale up status and get metrics to report
	report := state.ProcessScaleUpStatus(
		podsReport.schedulablePods,
		podsReport.podsTriggeredScaleUp,
		podsReport.podsRemainUnschedulable,
		podsReport.podsAwaitEvaluation,
		now,
	)

	f.observeReactions(report, now, podsReport.buffer, podsReport.samplePod)
}

func (f *FakePodStateObserver) observeReactions(report reactionsToReport, now time.Time, buffer *v1beta1.CapacityBuffer, samplePod *v1.Pod) {
	if len(report.timeouts) == 0 && len(report.noActionNeeded) == 0 && len(report.scaleUp) == 0 && len(report.unhelpable) == 0 {
		return
	}

	classification := f.classifyBuffer(samplePod, buffer)
	allocationMode := podutils.DeviceAllocationModeForPod(samplePod).String()

	ObserveCapacityBufferFakePodReactionTime := func(creationTimes []time.Time, reactionType metrics.ReactionType) {
		if len(creationTimes) == 0 {
			return
		}

		for _, created := range creationTimes {
			duration := now.Sub(created)
			f.observer.ObserveCapacityBufferFakePodReactionTime(duration, classification.systemPod, classification.hasPVC, classification.hasCSI, reactionType, classification.provisioningStrategy, allocationMode)
			isReactionOverdue := duration >= overdueReactionThreshold
			if isReactionOverdue {
				f.logWithRateLimit(klog.Level(1), "CapacityBufferFakePodStateObserver: Buffer (UID=%v) pod %v after %v (overdue)", buffer.UID, reactionType, duration)
			} else {
				f.logWithRateLimit(klog.Level(5), "CapacityBufferFakePodStateObserver: Buffer (UID=%v) pod %v after %v", buffer.UID, reactionType, duration)
			}
		}
	}

	// report metrics
	ObserveCapacityBufferFakePodReactionTime(report.timeouts, metrics.Timeout)
	ObserveCapacityBufferFakePodReactionTime(report.noActionNeeded, metrics.NoActionNeeded)
	ObserveCapacityBufferFakePodReactionTime(report.scaleUp, metrics.ScaleUp)
	ObserveCapacityBufferFakePodReactionTime(report.unhelpable, metrics.Unhelpable)
}

// removeDeletedBuffers removes buffers from stateByBufferUID that are not injecting fake pods any more.
// This can happen when the buffer is deleted, the buffer is not ready for provisioning or scaled to zero.
// In such cases, we don't report fake pods reaction time. We technically could report "deleted" reaction type,
// but we don't have sample pod to build all labels (system_pod, has_pvc, etc.).
func (f *FakePodStateObserver) removeDeletedBuffers() {
	buffersToRemove := make([]types.UID, 0)
	for bufferUID := range f.stateByBuffer {
		if _, ok := f.podsSnapshotByBuffer[bufferUID]; !ok {
			buffersToRemove = append(buffersToRemove, bufferUID)
		}
	}

	for _, bufferUID := range buffersToRemove {
		f.logWithRateLimit(klog.Level(5), "CapacityBufferFakePodStateObserver: Stopping observation of buffer (UID=%v)", bufferUID)
		delete(f.stateByBuffer, bufferUID)
	}
}

func (f *FakePodStateObserver) classifyBuffer(pod *v1.Pod, buffer *v1beta1.CapacityBuffer) podClassification {
	provisioningStrategy := capacitybuffer.ActiveProvisioningStrategy
	if buffer != nil && buffer.Status.ProvisioningStrategy != nil {
		provisioningStrategy = *buffer.Status.ProvisioningStrategy
	}

	return podClassification{
		systemPod:            f.systemPodsClassifier.IsSystemPod(pod),
		hasPVC:               podstate.HasPVC(pod),
		hasCSI:               podstate.HasCSI(pod),
		provisioningStrategy: provisioningStrategy,
	}
}

func (f *FakePodStateObserver) logWithRateLimit(level klog.Level, format string, args ...any) {
	if f.logLimiter != nil {
		f.logLimiter.Logf(level, format, args...)
	} else {
		klog.V(level).Infof(format, args...)
	}
}
