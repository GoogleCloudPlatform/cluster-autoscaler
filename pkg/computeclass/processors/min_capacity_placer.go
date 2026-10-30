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
	"fmt"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
)

// PlaceInSnapshot adds a min capacity fake pod to each node holding the min
// capacity of a ComputeClass, for a defrag pass or the cluster scale-to-zero
// check. It does nothing if min capacity is disabled or Process places the pods
// for the whole loop.
//
// It returns a function that removes the pods it placed from the snapshot,
// wherever a simulation moved or copied them since. Call it even when an error
// is returned: some pods may already be placed.
func (p *MinCapacityPodListProcessor) PlaceInSnapshot(autoscalingCtx *ca_context.AutoscalingContext) (remove func(), err error) {
	if !computeclass.IsComputeClassMinCapacityEnabled(p.experimentsManager) || !computeclass.IsComputeClassMinCapacityDefragOnlyFakePodsEnabled(p.experimentsManager) {
		return func() {}, nil
	}
	crds, err := p.ccLister.ListCrds()
	if err != nil {
		return func() {}, fmt.Errorf("failed to list ComputeClasses: %w", err)
	}
	placed, err := p.placeMinCapacityPods(autoscalingCtx, crds)
	return func() { removeMinCapacityPods(autoscalingCtx.ClusterSnapshot, placed) }, err
}

// removeMinCapacityPods removes the min capacity fake pods named like the given
// pods from every node of the snapshot, wherever a simulation moved or copied
// them.
//
// Removal errors are logged and skipped.
func removeMinCapacityPods(snapshot clustersnapshot.ClusterSnapshot, pods []*apiv1.Pod) {
	if len(pods) == 0 {
		return
	}
	names := make(map[string]bool, len(pods))
	for _, pod := range pods {
		names[pod.Name] = true
	}
	nodeInfos, err := snapshot.ListNodeInfos()
	if err != nil {
		klog.Errorf("Failed to list node infos to remove min capacity pods: %v", err)
		return
	}
	for _, ni := range nodeInfos {
		nodeName := ni.Node().Name
		// Collect first: unscheduling mutates the node info's pod list.
		var toRemove []*apiv1.Pod
		for _, podInfo := range ni.Pods() {
			if crd.IsMinCapacityFakePod(podInfo.Pod) && names[podInfo.Pod.Name] {
				toRemove = append(toRemove, podInfo.Pod)
			}
		}
		for _, pod := range toRemove {
			if err := snapshot.UnschedulePod(pod.Namespace, pod.Name, nodeName); err != nil {
				klog.Errorf("Failed to remove min capacity pod %s from node %s: %v", pod.Name, nodeName, err)
			}
		}
	}
}
