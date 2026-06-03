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

package kubernetes

import (
	ctx "context"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	client "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	podv1 "k8s.io/kubernetes/pkg/api/v1/pod"

	klog "k8s.io/klog/v2"
)

const maxPodChangeAge = 10 * time.Second

// NewUnschedulablePodNotificationsChannel returns a channel and creates an informer
// and watches for newly added or updated pods. Each time a new unschedulable
// pod appears or a change to a pod happens and the pod is unschedulable after the change,
// a message is sent to the channel returned by NewUnschedulablePodNotificationsChannel.
func NewUnschedulablePodNotificationsChannel(ctx ctx.Context, kubeClient client.Interface, namespace string) (<-chan any, error) {
	output := make(chan any, 1)
	selector := fields.ParseSelectorOrDie("spec.nodeName==" + "" + ",status.phase!=" +
		string(apiv1.PodSucceeded) + ",status.phase!=" + string(apiv1.PodFailed))
	listWatch := cache.NewListWatchFromClient(kubeClient.CoreV1().RESTClient(), "pods", namespace, selector)
	informer := cache.NewSharedInformer(listWatch, &apiv1.Pod{}, time.Hour)
	addEventHandlerFunc := func(obj any) {
		if isRecentUnschedulablePod(obj) {
			klog.V(5).Infof(" filterPodChanUntilClose emits signal")
			select {
			case output <- struct{}{}:
			default:
			}
		}
	}
	updateEventHandlerFunc := func(old any, newOjb any) { addEventHandlerFunc(newOjb) }
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    addEventHandlerFunc,
		UpdateFunc: updateEventHandlerFunc,
	})
	if err != nil {
		return nil, err
	}
	go informer.Run(ctx.Done())
	return output, nil
}

// isRecentUnschedulablePod check if the object is an unschedulable pod observed recently.
func isRecentUnschedulablePod(obj any) bool {
	pod, ok := obj.(*apiv1.Pod)
	if !ok {
		return false
	}
	if pod.Status.Phase == apiv1.PodSucceeded || pod.Status.Phase == apiv1.PodFailed {
		return false
	}
	if pod.Spec.NodeName != "" {
		return false
	}
	_, scheduledCondition := podv1.GetPodCondition(&pod.Status, apiv1.PodScheduled)
	if scheduledCondition == nil {
		return false
	}
	if scheduledCondition.Status != apiv1.ConditionFalse || scheduledCondition.Reason != "Unschedulable" {
		return false
	}
	if scheduledCondition.LastTransitionTime.Time.Add(maxPodChangeAge).Before(time.Now()) {
		return false
	}
	return true
}
