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

package annotator

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ca_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	metrics_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/metrics"
)

func TestPodAnnotations(t *testing.T) {
	pod1 := createPod("pod1", "ns1")
	tst1 := "2023-01-18T14:18:34-0700"
	tst2 := "2023-01-18T17:18:34-0700"
	tst3 := "2023-01-18T18:18:34-0700"
	tst1Time, _ := time.Parse(timeFormat, tst1)
	tst2Time, _ := time.Parse(timeFormat, tst2)
	tst3Time, _ := time.Parse(timeFormat, tst3)
	alreadyAnnotated := createPodWithAnnotations("pod2", "ns2", map[string]string{
		unhelpableSinceAnnotation: tst1,
		UnhelpableUntilAnnotation: UnhelpableForever,
	})
	type expectedPod struct {
		name, namespace, expectedUntil, expectedSince string
	}
	type scaleUpStatusWithTime struct {
		status *status.ScaleUpStatus
		time   time.Time
	}
	tcs := map[string]struct {
		allPods                  []*corev1.Pod
		scaleUpStatuses          []scaleUpStatusWithTime
		unschedulablePods        []*corev1.Pod
		expectedPodAnnotations   []expectedPod
		unhelpableUntilThreshold time.Duration
	}{
		"pod correctly annotated as unhelpable": {
			allPods: []*corev1.Pod{pod1},
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
			unschedulablePods: []*corev1.Pod{pod1},
			expectedPodAnnotations: []expectedPod{
				{
					name:          pod1.Name,
					namespace:     pod1.Namespace,
					expectedUntil: UnhelpableForever,
					expectedSince: tst1,
				},
			},
			unhelpableUntilThreshold: noLongerUnhelpableThreshold,
		},
		"pod correctly annotated as no longer unhelpable": {
			allPods: []*corev1.Pod{pod1},
			scaleUpStatuses: []scaleUpStatusWithTime{{
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
			unschedulablePods:        []*corev1.Pod{pod1},
			unhelpableUntilThreshold: 100 * time.Millisecond,
			expectedPodAnnotations: []expectedPod{
				{
					name:          pod1.Name,
					namespace:     pod1.Namespace,
					expectedUntil: tst1,
					expectedSince: tst1,
				},
			},
		},
		"two pods annotated correctly": {
			allPods: []*corev1.Pod{pod1, alreadyAnnotated},
			scaleUpStatuses: []scaleUpStatusWithTime{{
				status: &status.ScaleUpStatus{
					PodsRemainUnschedulable: []status.NoScaleUpInfo{
						{
							Pod: pod1,
						},
						{
							Pod: alreadyAnnotated,
						},
					},
				},
				time: tst1Time,
			},
				{
					status: &status.ScaleUpStatus{
						PodsRemainUnschedulable: []status.NoScaleUpInfo{
							{
								Pod: alreadyAnnotated,
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
			unschedulablePods:        []*corev1.Pod{pod1, alreadyAnnotated},
			unhelpableUntilThreshold: 100 * time.Millisecond,
			expectedPodAnnotations: []expectedPod{
				{
					name:          pod1.Name,
					namespace:     pod1.Namespace,
					expectedUntil: tst1,
					expectedSince: tst1,
				},
				{
					name:          alreadyAnnotated.Name,
					namespace:     alreadyAnnotated.Namespace,
					expectedUntil: tst2,
					expectedSince: tst1,
				},
			},
		},
		"pod correctly not overridden as unhelpable since": {
			allPods: []*corev1.Pod{alreadyAnnotated},
			scaleUpStatuses: []scaleUpStatusWithTime{{
				status: &status.ScaleUpStatus{
					PodsRemainUnschedulable: []status.NoScaleUpInfo{
						{
							Pod: alreadyAnnotated,
						},
					},
				},
				time: tst2Time,
			}},
			unschedulablePods:        []*corev1.Pod{alreadyAnnotated},
			unhelpableUntilThreshold: defaultPodTTL,
			expectedPodAnnotations: []expectedPod{
				{
					name:          alreadyAnnotated.Name,
					namespace:     alreadyAnnotated.Namespace,
					expectedUntil: UnhelpableForever,
					expectedSince: tst1,
				},
			},
		},
	}
	for desc, tc := range tcs {
		tc := tc
		t.Run(desc, func(t *testing.T) {
			t.Parallel()
			clock := &fakeClock{}
			annotator, kubeClient := setUpAnnotator(tc.allPods, tc.unschedulablePods, tc.unhelpableUntilThreshold, clock)

			for _, suStatus := range tc.scaleUpStatuses {
				clock.now = suStatus.time
				annotator.Process(&ca_context.AutoscalingContext{}, suStatus.status)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer cancel()
				annotator.annotatorLoop(ctx)
			}

			for _, pod := range tc.expectedPodAnnotations {
				validatePodAnnotations(t, pod.name, pod.namespace, pod.expectedSince, pod.expectedUntil, kubeClient)
			}
		})
	}
}

func validatePodAnnotations(t *testing.T, podName, podNamespace string, expectedSince, expectedUntil string, kubeClient kubernetes.Interface) {
	pod, _ := kubeClient.CoreV1().Pods(podNamespace).Get(context.Background(), podName, metav1.GetOptions{})
	if since := pod.Annotations[unhelpableSinceAnnotation]; expectedSince != since {
		t.Errorf("Unexpected unhelpable_since annotation, got: %s, want: %s", since, expectedSince)
	}

	if until := pod.Annotations[UnhelpableUntilAnnotation]; expectedUntil != until {
		t.Errorf("Unexpected unhelpable_until annotation, got: %s, want: %s", until, expectedUntil)
	}
}

func TestPodsCleared(t *testing.T) {
	pods := []*corev1.Pod{createPod("pod1", "ns1"), createPod("pod2", "ns2")}
	ttl := defaultPodTTL
	tst1 := time.Now()
	tst2 := tst1.Add(31 * time.Minute)
	clock := fakeClock{now: tst1}
	annotator, _ := setUpAnnotator(pods, pods, ttl, &clock)
	annotator.Process(nil, &status.ScaleUpStatus{PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pods[0]}, {Pod: pods[1]}}})
	if l := len(annotator.unhelpablePods); l != 2 {
		t.Errorf("Incorrect number of elements in unhelpablepods map; got: %d, want: %d.", l, 2)
	}
	clock.now = tst2
	annotator.Process(nil, &status.ScaleUpStatus{PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pods[0]}}})
	if l := len(annotator.unhelpablePods); l != 1 {
		t.Errorf("Incorrect number of elements in unhelpablepods map; got: %d, want: %d.", l, 1)
	}
}

func TestDuplicatesAreNotEmitted(t *testing.T) {
	pod := createPod("pod1", "ns1")
	pods := []*corev1.Pod{pod}
	tst1 := time.Now()
	tst2 := tst1.Add(40 * time.Minute)
	clock := fakeClock{now: tst1}
	annotator, _ := setUpAnnotator(pods, pods, defaultPodTTL, &clock)
	annotator.Process(nil, &status.ScaleUpStatus{PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pod}}})
	if l := len(annotator.podsToAnnotate); l != 1 {
		t.Errorf("Incorrect number of elements in pods to annotate queue; got: %d, want: %d.", l, 1)
	}
	annotator.Process(nil, &status.ScaleUpStatus{PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pod}}})
	// no element was removed, so we don't expect any new elements in the queue
	if l := len(annotator.podsToAnnotate); l != 1 {
		t.Errorf("Incorrect number of elements in pods to annotate queue; got: %d, want: %d.", l, 1)
	}
	clock.now = tst2
	annotator.Process(nil, &status.ScaleUpStatus{PodsRemainUnschedulable: []status.NoScaleUpInfo{}})
	// pod has new unhelpable until - should be added to queue
	if l := len(annotator.podsToAnnotate); l != 2 {
		t.Errorf("Incorrect number of elements in pods to annotate queue; got: %d, want: %d.", l, 2)
	}
}

func setUpAnnotator(pods []*corev1.Pod, unschedulablePods []*corev1.Pod,
	noLongerUnhelpableThreshold time.Duration, clock *fakeClock) (annotator *PodAnnotator,
	kubeClient kubernetes.Interface) {
	var objects []runtime.Object
	for _, pod := range pods {
		objects = append(objects, pod)
	}
	kubeClient = fake.NewSimpleClientset(objects...)
	annotator = NewPodAnnotator(kubeClient, &metrics_processors.PodStatusAggregator{Unschedulable: unschedulablePods})
	annotator.noLongerUnhelpableThreshold = noLongerUnhelpableThreshold
	annotator.clock = clock
	return
}

func createPodWithAnnotations(name, namespace string, annotations map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			UID:         types.UID(fmt.Sprintf("%s/%s", namespace, name)),
			Annotations: annotations,
		},
	}
}

func createPod(name, namespace string) *corev1.Pod {
	return createPodWithAnnotations(name, namespace, map[string]string{})
}

type fakeClock struct {
	now time.Time
}

func (f *fakeClock) Now() time.Time {
	return f.now
}

func TestIsUnhelpablePod(t *testing.T) {
	testCases := []struct {
		name     string
		pod      *corev1.Pod
		expected bool
	}{
		{
			name:     "Pod with no annotations",
			pod:      createPod("pod1", "ns1"),
			expected: false,
		},
		{
			name: "Pod with UnhelpableForever annotation",
			pod: createPodWithAnnotations("pod2", "ns2", map[string]string{
				UnhelpableUntilAnnotation: UnhelpableForever,
			}),
			expected: true,
		},
		{
			name: "Pod with unhelpable annotation set to a timestamp",
			pod: createPodWithAnnotations("pod3", "ns3", map[string]string{
				UnhelpableUntilAnnotation: "2023-01-18T14:18:34-0700",
			}),
			expected: false,
		},
		{
			name: "Pod with unhelpable annotation set to a different value",
			pod: createPodWithAnnotations("pod4", "ns4", map[string]string{
				UnhelpableUntilAnnotation: "SomethingElse",
			}),
			expected: false,
		},
		{
			name: "Pod with different annotation",
			pod: createPodWithAnnotations("pod5", "ns5", map[string]string{
				unhelpableSinceAnnotation: UnhelpableForever,
			}),
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := IsUnhelpablePod(tc.pod)
			if actual != tc.expected {
				t.Errorf("IsUnhelpablePod() = %v, expected %v", actual, tc.expected)
			}
		})
	}
}
