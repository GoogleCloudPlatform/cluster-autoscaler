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

package daemonsetmutation

import (
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	podutils "k8s.io/autoscaler/cluster-autoscaler/utils/pod"
)

// Injector handles injecting mutated admission webhook changes
// into simulated DaemonSet pods inside node templates.
type Injector struct {
	cache      *MutationCache
	controller *Controller
}

// NewInjector returns a new instance of Injector.
func NewInjector(cache *MutationCache, ctrl *Controller) *Injector {
	return &Injector{
		cache:      cache,
		controller: ctrl,
	}
}

// CleanUp shuts down the underlying mutation controller.
func (inj *Injector) CleanUp() {
	if inj.controller != nil {
		inj.controller.CleanUp()
	}
}

// InjectDaemonSetMutations scans template NodeInfos for simulated DaemonSet pods,
// applies cached overhead mutations if available, and updates templates.
func (inj *Injector) InjectDaemonSetMutations(nodeInfos map[string]*framework.NodeInfo, daemonsets []*appsv1.DaemonSet) map[string]*framework.NodeInfo {
	if len(nodeInfos) == 0 || len(daemonsets) == 0 {
		return nodeInfos
	}

	if inj.cache == nil || inj.controller == nil {
		return nodeInfos
	}

	dsMap := prepareDaemonSetMap(daemonsets)
	newNodeInfos := make(map[string]*framework.NodeInfo, len(nodeInfos))

	for nodeGroupId, nodeInfo := range nodeInfos {
		if nodeInfo == nil {
			newNodeInfos[nodeGroupId] = nil
			continue
		}

		newNodeInfos[nodeGroupId] = inj.injectMutationsToNodeInfo(nodeInfo, dsMap)
	}

	return newNodeInfos
}

func (inj *Injector) injectMutationsToNodeInfo(nodeInfo *framework.NodeInfo, dsMap map[types.UID]*appsv1.DaemonSet) *framework.NodeInfo {
	podsUpdated := false
	newPods := make([]*framework.PodInfo, 0, len(nodeInfo.Pods()))

	for _, podInfo := range nodeInfo.Pods() {
		if podInfo == nil || podInfo.Pod == nil {
			newPods = append(newPods, podInfo)
			continue
		}

		ds := getDaemonSetForPod(dsMap, podInfo.Pod)
		if ds == nil {
			newPods = append(newPods, podInfo)
			continue
		}

		cachedPod, stale := inj.cache.Get(ds.UID, ds.Generation)
		if stale {
			inj.controller.Enqueue(ds)
		}
		if cachedPod == nil {
			newPods = append(newPods, podInfo)
			continue
		}

		mutatedPod := applyCacheMutationToPod(podInfo.Pod, cachedPod)
		newPods = append(newPods, framework.NewPodInfo(mutatedPod, podInfo.NeededResourceClaims))
		podsUpdated = true
	}

	if !podsUpdated {
		return nodeInfo
	}

	updatedNodeInfo := framework.NewNodeInfo(nodeInfo.Node(), nodeInfo.LocalResourceSlices, newPods...)
	if nodeInfo.CSINode != nil {
		updatedNodeInfo.SetCSINode(nodeInfo.CSINode)
	}
	return updatedNodeInfo
}

func getDaemonSetForPod(dsMap map[types.UID]*appsv1.DaemonSet, pod *apiv1.Pod) *appsv1.DaemonSet {
	if !podutils.IsDaemonSetPod(pod) {
		return nil
	}
	controllerRef := metav1.GetControllerOf(pod)
	if controllerRef == nil {
		return nil
	}
	return dsMap[controllerRef.UID]
}

func prepareDaemonSetMap(daemonsets []*appsv1.DaemonSet) map[types.UID]*appsv1.DaemonSet {
	dsMap := make(map[types.UID]*appsv1.DaemonSet, len(daemonsets))
	for _, ds := range daemonsets {
		if ds != nil {
			dsMap[ds.UID] = ds
		}
	}
	return dsMap
}

// applyCacheMutationToPod merges dry-run webhook mutations from cachedPod onto simulatedPod
// while strictly preserving pod identity (Name, Namespace, UID, OwnerReferences) and simulation state.
func applyCacheMutationToPod(simulatedPod, cachedPod *apiv1.Pod) *apiv1.Pod {
	mutatedPod := simulatedPod.DeepCopy()
	mutatedPod.Spec = *cachedPod.Spec.DeepCopy()
	mutatedPod.Spec.NodeName = simulatedPod.Spec.NodeName

	// Merge any annotations or labels injected by mutating admission webhooks during dry-run
	if len(cachedPod.Annotations) > 0 {
		if mutatedPod.Annotations == nil {
			mutatedPod.Annotations = make(map[string]string, len(cachedPod.Annotations))
		}
		for k, v := range cachedPod.Annotations {
			mutatedPod.Annotations[k] = v
		}
	}
	if len(cachedPod.Labels) > 0 {
		if mutatedPod.Labels == nil {
			mutatedPod.Labels = make(map[string]string, len(cachedPod.Labels))
		}
		for k, v := range cachedPod.Labels {
			mutatedPod.Labels[k] = v
		}
	}
	return mutatedPod
}
