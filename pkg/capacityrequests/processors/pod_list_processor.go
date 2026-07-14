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
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/annotations"
	cr_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/client/listers/internal.autoscaling.gke.io/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/utils"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/gkedebuggingsnapshot"

	"k8s.io/apimachinery/pkg/labels"

	"k8s.io/klog/v2"
)

// CapacityRequestPodListProcessor processes the pod lists before scale up
// modifying them according to Capacity Requests currently present in the cluster.
type CapacityRequestPodListProcessor struct {
	crState  *utils.CapacityRequestState
	crLister cr_lister.CapacityRequestLister
}

// NewCapacityRequestPodListProcessor creates a new CapacityRequestPodListProcessor.
func NewCapacityRequestPodListProcessor(crState *utils.CapacityRequestState, crLister cr_lister.CapacityRequestLister) *CapacityRequestPodListProcessor {
	return &CapacityRequestPodListProcessor{crState: crState, crLister: crLister}
}

type simplePodRef struct {
	name      string
	namespace string
}

// Process processes lists of unschedulable and sheduled pods before scaling of the cluster.
// Checks the list of Capacity Requests that are currently active and modifies the
// pod lists accordingly; adds pods representing capacity requests and removes
// pods that are to be replaced.
func (p *CapacityRequestPodListProcessor) Process(context *context.AutoscalingContext,
	unschedulablePods []*apiv1.Pod) ([]*apiv1.Pod, error) {

	crs, err := p.crLister.List(labels.Everything())
	if err != nil {
		return unschedulablePods, err
	}
	gkeDebuggingSnapshot, ok := context.DebuggingSnapshotter.(*gkedebuggingsnapshot.GkeDebuggingSnapshotter)
	if ok {
		gkeDebuggingSnapshot.SetCapacityRequest(crs)
	}
	p.crState.Update(crs)
	if len(crs) == 0 {
		klog.V(3).Infof("No Capacity Requests to process.")
		return unschedulablePods, nil
	}
	klog.V(2).Infof("%v Capacity Requests in the cluster.", len(crs))
	podsToFilter := map[simplePodRef]bool{}
	for _, cr := range crs {
		pod, found := p.crState.CapacityRequestToPod(cr)
		if !found {
			klog.Errorf("Can't find pod for Capacity Request: %v/%v", cr.Namespace, cr.Name)
			continue
		}
		unschedulablePods = append(unschedulablePods, pod)
		for _, toReplace := range cr.Spec.ProvisionPolicy.PodsToReplace {
			podsToFilter[simplePodRef{name: toReplace.Name, namespace: cr.Namespace}] = true
		}
	}

	nodeInfos, err := context.ClusterSnapshot.ListNodeInfos()
	if err != nil {
		return nil, err
	}
	var originallyScheduledPods []*apiv1.Pod
	for _, nodeInfo := range nodeInfos {
		for _, podInfo := range nodeInfo.Pods() {
			originallyScheduledPods = append(originallyScheduledPods, podInfo.Pod)
		}
	}

	if len(podsToFilter) > 0 {
		unschedulablePods = filterPods(unschedulablePods, podsToFilter)
		for _, pod := range originallyScheduledPods {
			if podsToFilter[simplePodRef{name: pod.Name, namespace: pod.Namespace}] {
				nodeName := pod.Spec.NodeName
				if nodeName != "" {
					if err := context.ClusterSnapshot.ForceRemovePod(pod.Namespace, pod.Name, nodeName); err != nil {
						return nil, err
					}
				} else {
					// This can theoretically happen if podToReplace is "scheduled" earlier in the loop by some
					// other CA heuristic. In practice it should never happen, because:
					// 1. crProcessor is the first PLP called.
					// 2. VPA should only put running pods in PodsToReplace.
					klog.Errorf("Can't remove pod %s/%s from simulation, because it doesn't specify NodeName", pod.Namespace, pod.Name)
				}
			}
		}
	}
	unschedulablePods = p.handleSchedulableCapacityRequests(context, unschedulablePods)
	return unschedulablePods, nil
}

func filterPods(podList []*apiv1.Pod, podsToFilter map[simplePodRef]bool) []*apiv1.Pod {
	retList := []*apiv1.Pod{}

	for _, pod := range podList {
		if found := podsToFilter[simplePodRef{name: pod.Name, namespace: pod.Namespace}]; !found {
			retList = append(retList, pod)
		}
	}
	return retList
}

// handleSchedulableCapacityRequests checks if capacity requests can be
// scheduled in the cluster and marks them as scheduled in the cluster if
// possible.
func (p *CapacityRequestPodListProcessor) handleSchedulableCapacityRequests(
	context *context.AutoscalingContext,
	unschedulablePods []*apiv1.Pod) []*apiv1.Pod {
	unschedulablePods = p.markScheduledStillSchedulableCRs(context, unschedulablePods)
	return p.markScheduledSchedulableCRs(context, unschedulablePods)
}

// markScheduledStillSchedulableCRs checks if the capacity request that were
// assigned to a node before can still be scheduled on same nodes and if so,
// marks them as scheduled on the same node.
func (p *CapacityRequestPodListProcessor) markScheduledStillSchedulableCRs(
	context *context.AutoscalingContext,
	unschedulablePods []*apiv1.Pod) []*apiv1.Pod {
	newUnschedulable := []*apiv1.Pod{}
	for _, pod := range unschedulablePods {
		isUnschedulable := true
		if cr, found := p.crState.PodToCapacityRequest(pod); found {
			nodeName := pod.Spec.NodeName
			if nodeName != "" {
				pod.Spec.NodeName = ""
				if err := context.ClusterSnapshot.SchedulePod(pod, nodeName); err != nil && err.Type() == clustersnapshot.SchedulingInternalError {
					// Unexpected error.
					klog.Errorf("Could not add Capacity Request pod %s/%s to same node %q: %v", pod.Namespace, pod.Name, nodeName, err)
				} else if err == nil {
					// The pod was scheduled on nodeName.
					pod.Spec.NodeName = nodeName
					// CR still fits on the same node.
					if crErr := p.crState.SetResourcesAvailable(cr); crErr != nil {
						klog.Errorf("Failed to set status for Capacity Request %v/%v: %v", cr.Namespace, cr.Name, crErr)
					}
					isUnschedulable = false
				}
				// The pod can't be scheduled on nodeName because of scheduling predicates.
			}
		}
		if isUnschedulable {
			newUnschedulable = append(newUnschedulable, pod)
		}
	}
	return newUnschedulable
}

// markScheduledSchedulableCRs checks if there are schedulable Capacity Requests
// in the unschedulable pods list and if so marks them as scheduled on a node
// that they fit on.
func (p *CapacityRequestPodListProcessor) markScheduledSchedulableCRs(
	context *context.AutoscalingContext,
	unschedulablePods []*apiv1.Pod) []*apiv1.Pod {
	newUnschedulable := []*apiv1.Pod{}
	for _, pod := range unschedulablePods {
		isUnschedulable := true
		if cr, found := p.crState.PodToCapacityRequest(pod); found {
			nodeName, err := context.ClusterSnapshot.SchedulePodOnAnyNodeMatching(pod, clustersnapshot.SchedulingOptions{
				IsNodeAcceptable: func(nodeInfo *framework.NodeInfo) bool {
					_, isUpcoming := nodeInfo.Node().Annotations[annotations.NodeUpcomingAnnotation]
					return !isUpcoming
				},
			})
			if err != nil || nodeName == "" {
				klog.Errorf("Could not add Capacity Request pod %s/%s to new node %q: %v", pod.Namespace, pod.Name, nodeName, err)
			} else {
				pod.Spec.NodeName = nodeName
				if err := p.crState.SetResourcesAvailable(cr); err != nil {
					klog.Errorf("Failed to set status for Capacity Request %v/%v: %v", cr.Namespace, cr.Name, err)
				}
				isUnschedulable = false
			}
		}
		if isUnschedulable {
			newUnschedulable = append(newUnschedulable, pod)
		}
	}
	return newUnschedulable
}

// CleanUp cleans up the processor's internal structures.
func (p *CapacityRequestPodListProcessor) CleanUp() {
}
