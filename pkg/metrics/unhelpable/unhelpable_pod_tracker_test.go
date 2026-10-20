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
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
)

func TestUnhelpablePodTracker(t *testing.T) {
	pod1 := createPod("pod1", "ns1")
	pod2 := createPod("pod2", "ns2")
	tst1Time := time.Date(2023, 1, 18, 14, 18, 34, 0, time.UTC)
	tst2Time := tst1Time.Add(3 * time.Minute)
	tst3Time := tst2Time.Add(3 * time.Minute)

	type scaleUpStatusWithTime struct {
		status *status.ScaleUpStatus
		time   time.Time
	}
	tcs := map[string]struct {
		scaleUpStatuses             []scaleUpStatusWithTime
		expectedUnhelpable          map[*corev1.Pod]bool
		noLongerUnhelpableThreshold time.Duration
	}{
		"pod correctly tracked as unhelpable": {
			scaleUpStatuses: []scaleUpStatusWithTime{{
				status: &status.ScaleUpStatus{
					PodsRemainUnschedulable: []status.NoScaleUpInfo{
						{
							Pod: pod1,
						},
					},
				},
				time: tst1Time,
			}},
			expectedUnhelpable: map[*corev1.Pod]bool{
				pod1: true,
			},
			noLongerUnhelpableThreshold: noLongerUnhelpableThreshold,
		},
		"pod correctly marked as no longer unhelpable": {
			scaleUpStatuses: []scaleUpStatusWithTime{
				{
					status: &status.ScaleUpStatus{
						PodsRemainUnschedulable: []status.NoScaleUpInfo{
							{
								Pod: pod1,
							},
						},
					},
					time: tst1Time,
				},
				{
					status: &status.ScaleUpStatus{},
					time:   tst2Time,
				},
			},
			noLongerUnhelpableThreshold: 100 * time.Millisecond,
			expectedUnhelpable: map[*corev1.Pod]bool{
				pod1: false,
			},
		},
		"pod re-marked as unhelpable after becoming no longer unhelpable": {
			scaleUpStatuses: []scaleUpStatusWithTime{
				{
					status: &status.ScaleUpStatus{
						PodsRemainUnschedulable: []status.NoScaleUpInfo{
							{
								Pod: pod1,
							},
						},
					},
					time: tst1Time,
				},
				{
					status: &status.ScaleUpStatus{},
					time:   tst2Time,
				},
				{
					status: &status.ScaleUpStatus{
						PodsRemainUnschedulable: []status.NoScaleUpInfo{
							{
								Pod: pod1,
							},
						},
					},
					time: tst3Time,
				},
			},
			noLongerUnhelpableThreshold: 100 * time.Millisecond,
			expectedUnhelpable: map[*corev1.Pod]bool{
				pod1: true,
			},
		},
		"two pods tracked across transitions": {
			scaleUpStatuses: []scaleUpStatusWithTime{
				{
					status: &status.ScaleUpStatus{
						PodsRemainUnschedulable: []status.NoScaleUpInfo{
							{
								Pod: pod1,
							},
							{
								Pod: pod2,
							},
						},
					},
					time: tst1Time,
				},
				{
					status: &status.ScaleUpStatus{
						PodsRemainUnschedulable: []status.NoScaleUpInfo{
							{
								Pod: pod2,
							},
						},
					},
					time: tst2Time,
				},
				{
					status: &status.ScaleUpStatus{},
					time:   tst3Time,
				},
			},
			noLongerUnhelpableThreshold: 100 * time.Millisecond,
			expectedUnhelpable: map[*corev1.Pod]bool{
				pod1: false,
				pod2: false,
			},
		},
		"pod remains unhelpable before threshold": {
			scaleUpStatuses: []scaleUpStatusWithTime{
				{
					status: &status.ScaleUpStatus{
						PodsRemainUnschedulable: []status.NoScaleUpInfo{
							{
								Pod: pod1,
							},
						},
					},
					time: tst1Time,
				},
				{
					status: &status.ScaleUpStatus{},
					time:   tst2Time,
				},
			},
			noLongerUnhelpableThreshold: noLongerUnhelpableThreshold,
			expectedUnhelpable: map[*corev1.Pod]bool{
				pod1: true,
			},
		},
		"seeing pod again refreshes lastSeen and extends expiry": {
			scaleUpStatuses: []scaleUpStatusWithTime{
				{
					status: &status.ScaleUpStatus{
						PodsRemainUnschedulable: []status.NoScaleUpInfo{
							{
								Pod: pod1,
							},
						},
					},
					time: tst1Time,
				},
				{
					status: &status.ScaleUpStatus{
						PodsRemainUnschedulable: []status.NoScaleUpInfo{
							{
								Pod: pod1,
							},
						},
					},
					time: tst1Time.Add(10 * time.Minute),
				},
				{
					status: &status.ScaleUpStatus{},
					time:   tst1Time.Add(20 * time.Minute),
				},
			},
			noLongerUnhelpableThreshold: noLongerUnhelpableThreshold,
			expectedUnhelpable: map[*corev1.Pod]bool{
				pod1: true,
			},
		},
		"pod remains unhelpable at exact threshold boundary": {
			scaleUpStatuses: []scaleUpStatusWithTime{
				{
					status: &status.ScaleUpStatus{
						PodsRemainUnschedulable: []status.NoScaleUpInfo{
							{
								Pod: pod1,
							},
						},
					},
					time: tst1Time,
				},
				{
					status: &status.ScaleUpStatus{},
					time:   tst1Time.Add(noLongerUnhelpableThreshold),
				},
			},
			noLongerUnhelpableThreshold: noLongerUnhelpableThreshold,
			expectedUnhelpable: map[*corev1.Pod]bool{
				pod1: true,
			},
		},
		"pod expires 1ns past threshold boundary": {
			scaleUpStatuses: []scaleUpStatusWithTime{
				{
					status: &status.ScaleUpStatus{
						PodsRemainUnschedulable: []status.NoScaleUpInfo{
							{
								Pod: pod1,
							},
						},
					},
					time: tst1Time,
				},
				{
					status: &status.ScaleUpStatus{},
					time:   tst1Time.Add(noLongerUnhelpableThreshold + time.Nanosecond),
				},
			},
			noLongerUnhelpableThreshold: noLongerUnhelpableThreshold,
			expectedUnhelpable: map[*corev1.Pod]bool{
				pod1: false,
			},
		},
		"nil scaleUpStatus is a no-op": {
			scaleUpStatuses: []scaleUpStatusWithTime{
				{
					status: &status.ScaleUpStatus{
						PodsRemainUnschedulable: []status.NoScaleUpInfo{
							{
								Pod: pod1,
							},
						},
					},
					time: tst1Time,
				},
				{
					status: nil,
					time:   tst2Time,
				},
			},
			noLongerUnhelpableThreshold: 100 * time.Millisecond,
			expectedUnhelpable: map[*corev1.Pod]bool{
				pod1: true,
			},
		},
	}
	for desc, tc := range tcs {
		tc := tc
		t.Run(desc, func(t *testing.T) {
			t.Parallel()
			clock := &fakeClock{}
			tracker := setUpTracker(tc.noLongerUnhelpableThreshold, clock)

			for _, suStatus := range tc.scaleUpStatuses {
				clock.now = suStatus.time
				tracker.Process(context.TODO(), &ca_context.AutoscalingContext{}, suStatus.status)
			}

			for pod, wantUnhelpable := range tc.expectedUnhelpable {
				if got := tracker.IsPodUnhelpable(pod); got != wantUnhelpable {
					t.Errorf("IsPodUnhelpable(%s/%s) = %v, want %v", pod.Namespace, pod.Name, got, wantUnhelpable)
				}
			}
		})
	}
}

func TestPodsCleared(t *testing.T) {
	pods := []*corev1.Pod{createPod("pod1", "ns1"), createPod("pod2", "ns2")}
	threshold := noLongerUnhelpableThreshold
	tst1 := time.Now()
	tst2 := tst1.Add(threshold + time.Minute)
	clock := fakeClock{now: tst1}
	tracker := setUpTracker(threshold, &clock)
	tracker.Process(context.TODO(), nil, &status.ScaleUpStatus{PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pods[0]}, {Pod: pods[1]}}})
	if l := len(tracker.unhelpablePods); l != 2 {
		t.Errorf("Incorrect number of elements in unhelpablepods map; got: %d, want: %d.", l, 2)
	}
	clock.now = tst2
	tracker.Process(context.TODO(), nil, &status.ScaleUpStatus{PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pods[0]}}})
	if l := len(tracker.unhelpablePods); l != 1 {
		t.Errorf("Incorrect number of elements in unhelpablepods map; got: %d, want: %d.", l, 1)
	}
	if !tracker.IsPodUnhelpable(pods[0]) {
		t.Errorf("Expected pod1 to remain unhelpable")
	}
	if tracker.IsPodUnhelpable(pods[1]) {
		t.Errorf("Expected pod2 to be cleared and not unhelpable")
	}
}

func setUpTracker(noLongerUnhelpableThreshold time.Duration, clock *fakeClock) *UnhelpablePodTracker {
	tracker := NewUnhelpablePodTracker()
	tracker.noLongerUnhelpableThreshold = noLongerUnhelpableThreshold
	tracker.clock = clock
	return tracker
}

func createPod(name, namespace string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(fmt.Sprintf("%s/%s", namespace, name)),
		},
	}
}

type fakeClock struct {
	now time.Time
}

func (f *fakeClock) Now() time.Time {
	return f.now
}

func TestIsPodUnhelpable(t *testing.T) {
	pod1 := createPod("pod1", "ns1")
	pod2 := createPod("pod2", "ns2")
	pod3 := createPod("pod3", "ns3")

	clock := &fakeClock{now: time.Now()}
	tracker := setUpTracker(100*time.Millisecond, clock)

	// Mark pod1 and pod2 as unhelpable.
	tracker.Process(context.TODO(), nil, &status.ScaleUpStatus{
		PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pod1}, {Pod: pod2}},
	})

	// Advance time past threshold and keep only pod1 unhelpable; pod2 becomes no longer unhelpable.
	clock.now = clock.now.Add(200 * time.Millisecond)
	tracker.Process(context.TODO(), nil, &status.ScaleUpStatus{
		PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pod1}},
	})

	testCases := []struct {
		name     string
		tracker  *UnhelpablePodTracker
		pod      *corev1.Pod
		expected bool
	}{
		{
			name:     "Nil tracker",
			tracker:  nil,
			pod:      pod1,
			expected: false,
		},
		{
			name:     "Nil pod",
			tracker:  tracker,
			pod:      nil,
			expected: false,
		},
		{
			name:     "Pod currently tracked as unhelpable",
			tracker:  tracker,
			pod:      pod1,
			expected: true,
		},
		{
			name:     "Pod marked as no longer unhelpable",
			tracker:  tracker,
			pod:      pod2,
			expected: false,
		},
		{
			name:     "Untracked pod",
			tracker:  tracker,
			pod:      pod3,
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.tracker.IsPodUnhelpable(tc.pod)
			if actual != tc.expected {
				t.Errorf("IsPodUnhelpable() = %v, expected %v", actual, tc.expected)
			}
		})
	}
}

// fakePodLister implements PodLister interface for testing.
type fakePodLister struct {
	kube_util.PodLister
	pods []*corev1.Pod
}

func (f *fakePodLister) List() ([]*corev1.Pod, error) {
	return f.pods, nil
}

// fakeListerRegistry implements ListerRegistry for testing.
type fakeListerRegistry struct {
	kube_util.ListerRegistry
	podLister *fakePodLister
}

func (f *fakeListerRegistry) AllPodLister() kube_util.PodLister {
	return f.podLister
}

func TestUnhelpablePodTracker_RealStateVerification(t *testing.T) {
	podName := "pod1"
	podNamespace := "ns1"

	// Simulated unschedulable pod (what defrag would inject)
	simulatedPod := createPod(podName, podNamespace)
	simulatedPod.UID = "uid-1"

	tcs := map[string]struct {
		realPodInCache *corev1.Pod
		wantUnhelpable bool
	}{
		"pod not in cache (fallback to unhelpable)": {
			realPodInCache: nil,
			wantUnhelpable: true,
		},
		"pod in cache but recreated (different UID)": {
			realPodInCache: func() *corev1.Pod {
				p := createPod(podName, podNamespace)
				p.UID = "uid-2" // Different UID
				return p
			}(),
			wantUnhelpable: false,
		},
		"pod in cache and already scheduled": {
			realPodInCache: func() *corev1.Pod {
				p := createPod(podName, podNamespace)
				p.UID = "uid-1"               // Same UID
				p.Spec.NodeName = "some-node" // Scheduled
				return p
			}(),
			wantUnhelpable: false,
		},
		"pod in cache, same UID, not scheduled": {
			realPodInCache: func() *corev1.Pod {
				p := createPod(podName, podNamespace)
				p.UID = "uid-1"      // Same UID
				p.Spec.NodeName = "" // Not scheduled
				return p
			}(),
			wantUnhelpable: true,
		},
	}

	for desc, tc := range tcs {
		t.Run(desc, func(t *testing.T) {
			clock := &fakeClock{now: time.Now()}
			tracker := setUpTracker(noLongerUnhelpableThreshold, clock)

			// Setup fake lister registry
			var cachedPods []*corev1.Pod
			if tc.realPodInCache != nil {
				cachedPods = append(cachedPods, tc.realPodInCache)
			}
			ctx := &ca_context.AutoscalingContext{
				AutoscalingKubeClients: ca_context.AutoscalingKubeClients{
					ListerRegistry: &fakeListerRegistry{
						podLister: &fakePodLister{pods: cachedPods},
					},
				},
			}

			suStatus := &status.ScaleUpStatus{
				PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: simulatedPod}},
			}

			tracker.Process(t.Context(), ctx, suStatus)

			if got := tracker.IsPodUnhelpable(simulatedPod); got != tc.wantUnhelpable {
				t.Errorf("Expected IsPodUnhelpable: %v, got: %v", tc.wantUnhelpable, got)
			}
		})
	}
}
