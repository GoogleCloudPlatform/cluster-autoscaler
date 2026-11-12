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

package reactors

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/operationtracker"
)

// BalloonPodReactor simulates kube-apiserver DryRunAll admission and Kubelet
// startup and in-place pod vertical scaling (IPPR) for balloon and workload
// pods.
type BalloonPodReactor struct {
	client *fake.Clientset

	uidSeq        atomic.Int64
	dryRunCreates atomic.Int64
	creates       atomic.Int64
	resizePatches atomic.Int64

	mu                        sync.Mutex
	dryRunCreateErr           error
	createErr                 error
	resizePatchErr            error
	resizeRejectionCondCopy   *apiv1.PodCondition
	freezePodLevelMemOnResize bool
}

// InstallBalloonPodReactor prepends create and resize-patch reactors to client.
func InstallBalloonPodReactor(client *fake.Clientset) *BalloonPodReactor {
	r := &BalloonPodReactor{client: client}
	client.PrependReactor("create", "pods", r.handleCreate)
	client.PrependReactor("patch", "pods", r.handlePatch)
	return r
}

// SetDryRunCreateError injects an error for DryRunAll balloon pod creates
// (used by balloonPodChecker).
func (r *BalloonPodReactor) SetDryRunCreateError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dryRunCreateErr = err
}

// SetCreateError injects an error for normal balloon pod creates.
func (r *BalloonPodReactor) SetCreateError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.createErr = err
}

// SetResizePatchError injects an API error on balloon pod resize subresource
// patches.
func (r *BalloonPodReactor) SetResizePatchError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resizePatchErr = err
}

// SetResizeRejectionCondition configures Kubelet IPPR rejection or deferral on
// balloon pod resize patches. Pass nil to restore normal immediate IPPR
// actuation.
func (r *BalloonPodReactor) SetResizeRejectionCondition(cond *apiv1.PodCondition) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cond == nil {
		r.resizeRejectionCondCopy = nil
		return
	}
	c := *cond
	r.resizeRejectionCondCopy = &c
}

// SetFreezePodLevelMemoryOnResize configures whether balloon pod resize patches
// leave pod.Status.Resources.Requests[memory] at its initial PodRunning value,
// simulating the 1.36 Kubelet bug (b/583763422).
func (r *BalloonPodReactor) SetFreezePodLevelMemoryOnResize(freeze bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.freezePodLevelMemOnResize = freeze
}

// Counts returns the number of dry-run creates, normal creates, and balloon
// resize patches observed.
func (r *BalloonPodReactor) Counts() (dryRunCreates, creates, resizePatches int64) {
	return r.dryRunCreates.Load(), r.creates.Load(), r.resizePatches.Load()
}

func (r *BalloonPodReactor) handleCreate(action clienttesting.Action) (bool, runtime.Object, error) {
	if action.GetSubresource() != "" {
		return false, nil, nil
	}
	createAction, ok := action.(clienttesting.CreateAction)
	if !ok {
		return false, nil, nil
	}
	pod, ok := createAction.GetObject().(*apiv1.Pod)
	if !ok || !operationtracker.IsBalloonPod(pod) {
		return false, nil, nil
	}

	var dryRun []string
	if impl, ok := action.(clienttesting.CreateActionImpl); ok {
		dryRun = impl.CreateOptions.DryRun
	}
	if slices.Contains(dryRun, metav1.DryRunAll) {
		r.dryRunCreates.Add(1)
		r.mu.Lock()
		err := r.dryRunCreateErr
		r.mu.Unlock()
		if err != nil {
			return true, nil, err
		}
		return true, pod.DeepCopy(), nil
	}

	r.creates.Add(1)
	r.mu.Lock()
	err := r.createErr
	r.mu.Unlock()
	if err != nil {
		return true, nil, err
	}

	seq := r.uidSeq.Add(1)
	if pod.UID == "" {
		pod.UID = types.UID(fmt.Sprintf("balloon-uid-%d", seq))
	}
	if pod.Name == "" {
		pod.Name = fmt.Sprintf("%s%d", pod.GenerateName, seq)
	}
	if pod.Spec.NodeName != "" {
		pod.Status.Phase = apiv1.PodRunning
		syncContainerAllocatedResources(pod, false)
	}
	return false, nil, nil
}

func (r *BalloonPodReactor) handlePatch(action clienttesting.Action) (bool, runtime.Object, error) {
	if action.GetSubresource() != "resize" {
		return false, nil, nil
	}
	patchAction, ok := action.(clienttesting.PatchAction)
	if !ok {
		return false, nil, nil
	}

	ns := patchAction.GetNamespace()
	name := patchAction.GetName()
	obj, err := r.client.Tracker().Get(podsResource, ns, name)
	if err != nil {
		return true, nil, err
	}
	storedPod, ok := obj.(*apiv1.Pod)
	if !ok {
		return false, nil, nil
	}

	balloon := operationtracker.IsBalloonPod(storedPod)
	if balloon {
		r.resizePatches.Add(1)
		r.mu.Lock()
		patchErr := r.resizePatchErr
		freezePodLevelMem := r.freezePodLevelMemOnResize
		var rejectCond *apiv1.PodCondition
		if r.resizeRejectionCondCopy != nil {
			c := *r.resizeRejectionCondCopy
			rejectCond = &c
		}
		r.mu.Unlock()

		if patchErr != nil {
			return true, nil, patchErr
		}
		updated := storedPod.DeepCopy()
		if err := applyContainerResourcePatch(updated, patchAction.GetPatch()); err != nil {
			return true, nil, err
		}
		if rejectCond != nil {
			setPodResizeCondition(updated, *rejectCond)
			if err := r.client.Tracker().Update(podsResource, updated, ns); err != nil {
				return true, nil, err
			}
			return true, updated.DeepCopy(), nil
		}
		clearPodResizeConditions(updated)
		syncContainerAllocatedResources(updated, freezePodLevelMem)
		if err := r.client.Tracker().Update(podsResource, updated, ns); err != nil {
			return true, nil, err
		}
		return true, updated.DeepCopy(), nil
	}

	updated := storedPod.DeepCopy()
	if err := applyContainerResourcePatch(updated, patchAction.GetPatch()); err != nil {
		return true, nil, err
	}
	clearPodResizeConditions(updated)
	syncContainerAllocatedResources(updated, false)
	if err := r.client.Tracker().Update(podsResource, updated, ns); err != nil {
		return true, nil, err
	}
	return true, updated.DeepCopy(), nil
}

func applyContainerResourcePatch(pod *apiv1.Pod, rawPatch []byte) error {
	var patchPod apiv1.Pod
	if err := json.Unmarshal(rawPatch, &patchPod); err != nil {
		return err
	}
	for _, pc := range patchPod.Spec.Containers {
		for i := range pod.Spec.Containers {
			if pod.Spec.Containers[i].Name != pc.Name {
				continue
			}
			if len(pc.Resources.Requests) > 0 {
				if pod.Spec.Containers[i].Resources.Requests == nil {
					pod.Spec.Containers[i].Resources.Requests = make(apiv1.ResourceList)
				}
				maps.Copy(pod.Spec.Containers[i].Resources.Requests, pc.Resources.Requests)
			}
			if len(pc.Resources.Limits) > 0 {
				if pod.Spec.Containers[i].Resources.Limits == nil {
					pod.Spec.Containers[i].Resources.Limits = make(apiv1.ResourceList)
				}
				maps.Copy(pod.Spec.Containers[i].Resources.Limits, pc.Resources.Limits)
			}
		}
	}
	return nil
}

func syncContainerAllocatedResources(pod *apiv1.Pod, freezePodLevelMem bool) {
	podReqs := make(apiv1.ResourceList)
	for _, c := range pod.Spec.Containers {
		idx := -1
		for i := range pod.Status.ContainerStatuses {
			if pod.Status.ContainerStatuses[i].Name == c.Name {
				idx = i
				break
			}
		}
		if idx < 0 {
			pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, apiv1.ContainerStatus{
				Name:  c.Name,
				Ready: true,
				State: apiv1.ContainerState{Running: &apiv1.ContainerStateRunning{}},
			})
			idx = len(pod.Status.ContainerStatuses) - 1
		}
		pod.Status.ContainerStatuses[idx].AllocatedResources = maps.Clone(c.Resources.Requests)
		pod.Status.ContainerStatuses[idx].Resources = &apiv1.ResourceRequirements{
			Requests: maps.Clone(c.Resources.Requests),
			Limits:   maps.Clone(c.Resources.Limits),
		}
		for resName, qty := range c.Resources.Requests {
			total := podReqs[resName]
			total.Add(qty)
			podReqs[resName] = total
		}
	}
	if len(podReqs) == 0 {
		return
	}
	pod.Status.AllocatedResources = maps.Clone(podReqs)
	statusReqs := maps.Clone(podReqs)
	if freezePodLevelMem && pod.Status.Resources != nil {
		if prevMem, ok := pod.Status.Resources.Requests[apiv1.ResourceMemory]; ok {
			statusReqs[apiv1.ResourceMemory] = prevMem
		}
	}
	pod.Status.Resources = &apiv1.ResourceRequirements{
		Requests: statusReqs,
	}
}

func setPodResizeCondition(pod *apiv1.Pod, cond apiv1.PodCondition) {
	clearPodResizeConditions(pod)
	pod.Status.Conditions = append(pod.Status.Conditions, cond)
}

func clearPodResizeConditions(pod *apiv1.Pod) {
	filtered := pod.Status.Conditions[:0]
	for _, c := range pod.Status.Conditions {
		if c.Type == apiv1.PodResizePending || c.Type == apiv1.PodResizeInProgress {
			continue
		}
		filtered = append(filtered, c)
	}
	pod.Status.Conditions = filtered
}
