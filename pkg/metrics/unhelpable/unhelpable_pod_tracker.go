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

package unhelpable

import (
	"context"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
)

const (
	noLongerUnhelpableThreshold = 15 * time.Minute
)

type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

// UnhelpablePodTracker is a ScaleUpStatusProcessor that tracks in memory the
// pods that are considered unhelpable by CA.
type UnhelpablePodTracker struct {
	mu                          sync.RWMutex
	unhelpablePods              map[types.UID]time.Time
	noLongerUnhelpableThreshold time.Duration
	clock                       Clock
}

func NewUnhelpablePodTracker() *UnhelpablePodTracker {
	return &UnhelpablePodTracker{
		unhelpablePods:              make(map[types.UID]time.Time),
		noLongerUnhelpableThreshold: noLongerUnhelpableThreshold,
		clock:                       realClock{},
	}
}

type podKey struct {
	namespace string
	name      string
}

func getRealPods(ctx *ca_context.AutoscalingContext, scaleUpStatus *status.ScaleUpStatus) map[podKey]*corev1.Pod {
	if len(scaleUpStatus.PodsRemainUnschedulable) == 0 {
		return nil
	}
	if ctx == nil || ctx.ListerRegistry == nil || ctx.AllPodLister() == nil {
		klog.V(4).Infof("Real pod state not available for pod tracker")
		return nil
	}

	podList, err := ctx.AllPodLister().List()
	if err != nil {
		klog.Warningf("Failed to list pods for tracker: %v", err)
		return nil
	}

	targetKeys := make(map[podKey]bool, len(scaleUpStatus.PodsRemainUnschedulable))
	for _, p := range scaleUpStatus.PodsRemainUnschedulable {
		targetKeys[podKey{namespace: p.Pod.Namespace, name: p.Pod.Name}] = true
	}

	realPods := make(map[podKey]*corev1.Pod, len(targetKeys))
	for _, p := range podList {
		key := podKey{namespace: p.Namespace, name: p.Name}
		if targetKeys[key] {
			realPods[key] = p
			if len(realPods) == len(targetKeys) {
				break
			}
		}
	}
	return realPods
}

func filterOutScheduledPods(unschedulablePods []status.NoScaleUpInfo, realPods map[podKey]*corev1.Pod) []status.NoScaleUpInfo {
	if len(realPods) == 0 {
		return unschedulablePods
	}
	var filtered []status.NoScaleUpInfo
	for _, noScaleUp := range unschedulablePods {
		pod := noScaleUp.Pod
		if realPod, exists := realPods[podKey{namespace: pod.Namespace, name: pod.Name}]; exists {
			// Skip pods where UID doesn't match because the original pod might have been deleted and recreated with the same name.
			// Skip pods with NodeName != "" because they are actually scheduled and their presence in PodsRemainUnschedulable is an artifact of defragmentation simulation.
			if realPod.UID != pod.UID || realPod.Spec.NodeName != "" {
				continue
			}
		}
		filtered = append(filtered, noScaleUp)
	}
	return filtered
}

// Process tracks unhelpable pods in memory based on scaleUpStatus.
func (a *UnhelpablePodTracker) Process(_ context.Context, ctx *ca_context.AutoscalingContext, scaleUpStatus *status.ScaleUpStatus) {
	if scaleUpStatus == nil {
		return
	}
	now := a.clock.Now()

	realPods := getRealPods(ctx, scaleUpStatus)
	unschedulablePods := filterOutScheduledPods(scaleUpStatus.PodsRemainUnschedulable, realPods)

	a.mu.Lock()
	defer a.mu.Unlock()

	for _, noScaleUp := range unschedulablePods {
		pod := noScaleUp.Pod
		a.unhelpablePods[pod.UID] = now
	}
	a.clear(now)
}

func (*UnhelpablePodTracker) CleanUp() {}

// clear removes pods that are no longer considered unhelpable from internal structure.
func (a *UnhelpablePodTracker) clear(now time.Time) {
	for podID, lastSeen := range a.unhelpablePods {
		if now.Sub(lastSeen) > a.noLongerUnhelpableThreshold {
			delete(a.unhelpablePods, podID)
		}
	}
}

// IsPodUnhelpable returns true if the pod is currently tracked as unhelpable.
func (a *UnhelpablePodTracker) IsPodUnhelpable(pod *corev1.Pod) bool {
	if a == nil || pod == nil {
		return false
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	_, found := a.unhelpablePods[pod.UID]
	return found
}
