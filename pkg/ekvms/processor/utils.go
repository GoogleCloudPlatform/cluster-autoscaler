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

package processor

import (
	"fmt"
	"math"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	podutils "k8s.io/autoscaler/cluster-autoscaler/utils/pod"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/operationtracker"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/utils"
)

const (
	miBToKiB = size.MiB / size.KiB
	giBToKiB = size.GiB / size.KiB
)

// getRequestedResources returns the requested resources of the node as Allocatable.
func getRequestedResources(nodeInfo *framework.NodeInfo) size.Allocatable {
	return size.Allocatable{
		MilliCpus: nodeInfo.GetRequested().GetMilliCPU(),
		KBytes:    int64(math.Ceil(float64(nodeInfo.GetRequested().GetMemory()) / float64(size.KiB))),
	}
}

// AdjustBalloonPodsSize sets the balloon pods size in the cluster snapshot to the difference of the
// node.Status.Allocatable and the allocatable sizes provided via sizeMap.
func AdjustBalloonPodsSize(snapshot clustersnapshot.ClusterSnapshot, sizeMap map[string]size.Allocatable, calculator calculator.Calculator) error {
	for nodeName, allocatable := range sizeMap {
		nodeInfo, err := snapshot.GetNodeInfo(nodeName)
		if err != nil {
			// Node being in sizeMap but not snapshot can potentially happen,
			// but should reconcile within a 1 CA loop.
			continue
		}
		node := nodeInfo.Node()
		if nodeInfo, err = removeBalloonPod(snapshot, nodeInfo); err != nil {
			return err
		}

		bPodCpu := *resource.NewMilliQuantity(max(nodeInfo.Node().Status.Allocatable.Cpu().MilliValue()-allocatable.MilliCpus, operationtracker.MinBalloonPodCpu), resource.DecimalSI)
		bPodMem := *resource.NewQuantity(max(nodeInfo.Node().Status.Allocatable.Memory().Value()-allocatable.KBytes*size.KiB, operationtracker.MinBalloonPodMem), resource.DecimalSI)
		bPod, err := operationtracker.GenerateBalloonPod(node, bPodCpu, bPodMem, true)
		if err != nil {
			return err
		}
		// TODO(b/517096952): Figure out if/how to use the predicate-checking SchedulePod() here instead - otherwise this doesn't work with DRA pods.
		if err := snapshot.ForceAddPod(bPod, node.Name); err != nil {
			return err
		}
	}
	return nil
}

// removeBalloonPod removes the balloon pod (if there is one) from the given node and the cluster snapshot.
func removeBalloonPod(snapshot clustersnapshot.ClusterSnapshot, nodeInfo *framework.NodeInfo) (*framework.NodeInfo, error) {
	nodeName := nodeInfo.Node().Name
	for _, podInfo := range nodeInfo.Pods() {
		if operationtracker.IsBalloonPod(podInfo.Pod) {
			if err := snapshot.ForceRemovePod(podInfo.Pod.Namespace, podInfo.Pod.Name, nodeName); err != nil {
				return nodeInfo, err
			}
			break
		}
	}
	// Return a new node, since it is possible that the current nodeInfo reference is obsolete.
	newNodeInfo, err := snapshot.GetNodeInfo(nodeName)
	if err != nil {
		return nodeInfo, fmt.Errorf("getting node info for node %q failed: %v", nodeName, err)
	}
	return newNodeInfo, nil
}

// isNodeEmpty returns true if a node only has system and/or daemon set pods.
func isNodeEmpty(nodeInfo *framework.NodeInfo) bool {
	for _, podInfo := range nodeInfo.Pods() {
		if !isSystemPod(podInfo.Pod) && !podutils.IsDaemonSetPod(podInfo.Pod) {
			return false
		}
	}
	return true
}

func isResizableNode(node *apiv1.Node, mcp *machinetypes.MachineConfigProvider) bool {
	isResizable, err := utils.IsResizableNode(node, mcp)
	return err == nil && isResizable
}

func isAnyResizingEnabled(manager operationtracker.Manager, families []machinetypes.MachineFamily) bool {
	for _, family := range families {
		if manager.IsResizingEnabled(family.Name()) {
			return true
		}
	}
	return false
}

func isSystemPod(pod *apiv1.Pod) bool {
	return pod.Namespace == metav1.NamespaceSystem
}

// allLookaheadPodsRequests return the sum of requests of all lookahead pods in a given node.
func allLookaheadPodsRequests(nodeInfo *framework.NodeInfo) size.Allocatable {
	return matchingPodRequests(nodeInfo, func(pod *apiv1.Pod) bool {
		return lookaheadbuffer.IsLookaheadPod(pod)
	})
}

// allBalloonPodsRequests return the sum of requests of all balloon pods in a given node.
func allBalloonPodsRequests(nodeInfo *framework.NodeInfo) size.Allocatable {
	return matchingPodRequests(nodeInfo, func(pod *apiv1.Pod) bool {
		return operationtracker.IsBalloonPod(pod)
	})
}

// matchingPodRequests returns the sum of resource requests of pods that satisfy the predicate
func matchingPodRequests(nodeInfo *framework.NodeInfo, isPodAcceptable func(*apiv1.Pod) bool) size.Allocatable {
	var requests size.Allocatable
	for _, podInfo := range nodeInfo.Pods() {
		if !isPodAcceptable(podInfo.Pod) {
			continue
		}
		podSize := utils.PodRequestsAsSize(podInfo.Pod)
		requests.Add(podSize)
	}
	return requests
}

/** HasLookaheadPods returns true if the node has any lookahead pods */
func HasLookaheadPods(nodeInfo *framework.NodeInfo) bool {
	for _, podInfo := range nodeInfo.Pods() {
		if lookaheadbuffer.IsLookaheadPod(podInfo.Pod) {
			return true
		}
	}
	return false
}

// IsUserWorkloadPod returns true if the pod is non-static non-daemonSet non-kubesystem pod.
func IsUserWorkloadPod(pod *apiv1.Pod) bool {
	return !isSystemPod(pod) && !podutils.IsDaemonSetPod(pod) && !podutils.IsMirrorPod(pod) && !podutils.IsStaticPod(pod)
}
