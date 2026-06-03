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
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	ca_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/client-go/kubernetes"
	metrics_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/metrics"
	"k8s.io/klog/v2"
)

const (
	unhelpableSinceAnnotation = "cloud.google.com/cluster_autoscaler_unhelpable_since"
	// UnhelpableUntilAnnotation is the annotation indicating at which time given pod stopped being unhelpable.
	UnhelpableUntilAnnotation = "cloud.google.com/cluster_autoscaler_unhelpable_until"
	// UnhelpableForever indicates that a pod never stopped being unhelpable.
	UnhelpableForever           = "Inf"
	defaultPodTTL               = 30 * time.Minute
	noLongerUnhelpableThreshold = 15 * time.Minute
	timeFormat                  = "2006-01-02T15:04:05-0700"
	// GKE clusters have limit of 150k pods. We want to support 15k
	// unhelpable pods (10% of max pods) before dropping annotations.
	podChannelSize = 15000
)

type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

// PodAnnotator is a ScaleUpStatusProcessor used for annotating pods that are
// considered as unhelpable by CA.
type PodAnnotator struct {
	kubeClient                  kubernetes.Interface
	aggregator                  *metrics_processors.PodStatusAggregator
	podsToAnnotate              chan unhelpablePod
	unhelpablePods              map[types.UID]*unhelpablePod
	podTTL                      time.Duration
	noLongerUnhelpableThreshold time.Duration
	clock                       Clock
	podsInQueue                 sync.Map
}

type unhelpablePod struct {
	name, namespace                  string
	uid                              types.UID
	unhelpableSince, unhelpableUntil string
	lastSeenAsUnhelpable             time.Time
}

func NewPodAnnotator(kubeClient kubernetes.Interface, aggregator *metrics_processors.PodStatusAggregator) *PodAnnotator {
	return &PodAnnotator{
		kubeClient:                  kubeClient,
		podsToAnnotate:              make(chan unhelpablePod, podChannelSize),
		unhelpablePods:              make(map[types.UID]*unhelpablePod),
		podTTL:                      defaultPodTTL,
		aggregator:                  aggregator,
		noLongerUnhelpableThreshold: noLongerUnhelpableThreshold,
		clock:                       realClock{},
		podsInQueue:                 sync.Map{},
	}
}

// Process annotates unhelpable pods based on scaleUpStatus.
func (a *PodAnnotator) Process(_ *ca_context.AutoscalingContext, scaleUpStatus *status.ScaleUpStatus) {
	now := a.clock.Now()
	for _, noScaleUp := range scaleUpStatus.PodsRemainUnschedulable {
		pod := noScaleUp.Pod
		if _, found := a.unhelpablePods[pod.UID]; !found {
			a.unhelpablePods[pod.UID] = &unhelpablePod{
				name:            pod.Name,
				namespace:       pod.Namespace,
				uid:             pod.UID,
				unhelpableSince: now.Format(timeFormat),
				unhelpableUntil: UnhelpableForever,
			}
			if pod.Annotations != nil {
				unhelpableSince, unhelpableSinceFound := pod.Annotations[unhelpableSinceAnnotation]
				if unhelpableSinceFound {
					a.unhelpablePods[pod.UID].unhelpableSince = unhelpableSince
				}
			}
		}
		a.unhelpablePods[pod.UID].lastSeenAsUnhelpable = now
		if a.shouldAnnotateUnhelpablePod(pod) {
			a.annotatePod(a.unhelpablePods[pod.UID])
		}
	}
	a.annotateNoLongerUnhelpable(a.aggregator.Unschedulable)
	a.clear()
}

func (_ *PodAnnotator) CleanUp() {}

// Start starts numWorkers goroutines that send pod annotation updates.
func (a *PodAnnotator) Start(ctx context.Context, numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		go a.annotatorLoop(ctx)
	}
}

func (a *PodAnnotator) annotatorLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case pod := <-a.podsToAnnotate:
			err := a.patchPod(ctx, pod)
			if err != nil {
				klog.Warningf("Failed to update pod annotation: %v", err)
			}
			a.podsInQueue.Delete(unhelpablePodID(pod))
		}
	}
}

func (a *PodAnnotator) annotatePod(pod *unhelpablePod) {
	select {
	case a.podsToAnnotate <- *pod:
		a.podsInQueue.Store(unhelpablePodID(*pod), true)
	default:
		klog.Warningf("Cannot update pod %s/%s annotations, channel full.", pod.namespace, pod.name)
	}
}

// clear removes old pods from internal structure.
func (a *PodAnnotator) clear() {
	now := a.clock.Now()
	filteredOutPods := make(map[types.UID]*unhelpablePod)
	for podId, pod := range a.unhelpablePods {
		if now.Sub(pod.lastSeenAsUnhelpable) < a.podTTL {
			filteredOutPods[podId] = pod
		}
	}
	a.unhelpablePods = filteredOutPods
}

// annotateNoLongerUnhelpable checks whether there are any pods that
// are considered no longer unhelpable and annotates them if needed.
func (a *PodAnnotator) annotateNoLongerUnhelpable(unschedulablePods []*corev1.Pod) {
	now := a.clock.Now()
	for _, p := range unschedulablePods {
		if pod, found := a.unhelpablePods[p.UID]; found {
			if now.Sub(pod.lastSeenAsUnhelpable) > a.noLongerUnhelpableThreshold {
				pod.unhelpableUntil = pod.lastSeenAsUnhelpable.Format(timeFormat)
				if a.shouldAnnotateNoLongerUnhelpablePod(p) {
					a.annotatePod(pod)
				}
			}
		}
	}
}

func (a *PodAnnotator) patchPod(ctx context.Context, updatedPod unhelpablePod) error {
	patchRaw := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": podAnnotations(updatedPod),
		},
	}
	patchJson, err := json.Marshal(patchRaw)
	if err != nil {
		return err
	}
	_, err = a.kubeClient.CoreV1().Pods(updatedPod.namespace).Patch(ctx, updatedPod.name, types.MergePatchType, patchJson, metav1.PatchOptions{})
	return err
}

// shouldAnnotateUnhelpablePod returns whether given pod should be annotated as unhelpable.
// It shouldn't if it was already annotated and unhelpableUntil annotation wasn't overridden later.
func (a *PodAnnotator) shouldAnnotateUnhelpablePod(apiPod *corev1.Pod) bool {
	pod := a.unhelpablePods[apiPod.UID]
	if _, found := a.podsInQueue.Load(unhelpablePodID(*pod)); found {
		return false
	}
	if apiPod.Annotations == nil {
		return true
	}
	_, unhelpableSinceFound := apiPod.Annotations[unhelpableSinceAnnotation]
	if !unhelpableSinceFound {
		return true
	}
	unhelpableUntil := apiPod.Annotations[UnhelpableUntilAnnotation]
	return unhelpableUntil != UnhelpableForever
}

// shouldAnnotateNoLongerUnhelpablePod returns whether given pod should be annotated as no longer
// unhelpable. It shouldn't if it was already annotated and unhelpableUntil annotation is up to date.
func (a *PodAnnotator) shouldAnnotateNoLongerUnhelpablePod(apiPod *corev1.Pod) bool {
	pod := a.unhelpablePods[apiPod.UID]
	if _, found := a.podsInQueue.Load(unhelpablePodID(*pod)); found {
		return false
	}
	unhelpableUntil := apiPod.Annotations[UnhelpableUntilAnnotation]
	return unhelpableUntil != pod.unhelpableUntil
}

func IsUnhelpablePod(pod *corev1.Pod) bool {
	return pod.Annotations[UnhelpableUntilAnnotation] == UnhelpableForever
}

func podAnnotations(pod unhelpablePod) map[string]string {
	return map[string]string{
		unhelpableSinceAnnotation: pod.unhelpableSince,
		UnhelpableUntilAnnotation: pod.unhelpableUntil,
	}
}

func unhelpablePodID(pod unhelpablePod) string {
	return fmt.Sprintf("%s/%s/%s", pod.uid, pod.unhelpableSince, pod.unhelpableUntil)
}
