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

package operationtracker

import (
	"context"
	"fmt"
	"math"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	podutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/pod"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/v1/pod"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

func calculateRequestedResources(clientSet clientset.Interface, node *v1.Node) (size.Allocatable, error) {
	podList, err := clientSet.CoreV1().Pods(allPodsNamespace).List(context.TODO(), metav1.ListOptions{FieldSelector: "spec.nodeName=" + node.Name})
	if err != nil {
		return size.Allocatable{}, err
	}
	var pods []*v1.Pod
	for i := range podList.Items {
		// Balloon Pod guards resources that are actually not on the Node.
		// Thus, it should be omitted for real resource request calculation.
		if IsBalloonPod(&podList.Items[i]) {
			continue
		}
		// Terminated Pods (Succeeded, Failed) do not use Node resources.
		// Thus, they should be omitted for real resource request calculation.
		if pod.IsPodTerminal(&podList.Items[i]) {
			continue
		}
		pods = append(pods, &podList.Items[i])
	}
	info := framework.NewTestNodeInfo(nil, pods...)
	return size.Allocatable{
		MilliCpus: info.GetRequested().GetMilliCPU(),
		KBytes:    int64(math.Ceil(float64(info.GetRequested().GetMemory()) / float64(size.KiB))),
	}, nil
}

func balloonPodRequests(bPod *v1.Pod) (*resource.Quantity, *resource.Quantity) {
	podRequests := podutils.PodRequests(bPod)
	cpu := podRequests[v1.ResourceCPU]
	memory := podRequests[v1.ResourceMemory]
	return &cpu, &memory
}

func formatResourcePair(rl v1.ResourceList) (cpu, mem string) {
	cpu, mem = "none", "none"
	if q, ok := rl[v1.ResourceCPU]; ok {
		cpu = q.String()
	}
	if q, ok := rl[v1.ResourceMemory]; ok {
		mem = q.String()
	}
	return cpu, mem
}

func getBalloonPodsLog(bpStatus BalloonPodStatus, balloonPods []*v1.Pod, node *v1.Node, desiredAllocatable size.Allocatable) string {
	desiredCpu, desiredMem := getBalloonPodSize(node, desiredAllocatable)
	switch bpStatus {
	case BalloonPodWrongCount:
		var bPodsLogs []string
		for _, bPod := range balloonPods {
			bPodCpu, bPodMem := balloonPodRequests(bPod)
			bPodsLogs = append(bPodsLogs, fmt.Sprintf("balloon pod: {name: %q, cpu: %q, memory: %q}.", bPod.Name, bPodCpu, bPodMem))
		}
		return fmt.Sprintf("Want single balloon pod: {cpu: %q, memory: %q}, got %d: [%s]", desiredCpu.String(), desiredMem.String(), len(balloonPods), strings.Join(bPodsLogs, ", "))
	case BalloonPodIncorrectSize:
		bPod := balloonPods[0]
		bPodCpu, bPodMem := balloonPodRequests(bPod)
		var allocReqs, statusReqs v1.ResourceList
		if cStatus := getBalloonContainerStatus(bPod); cStatus != nil {
			allocReqs = cStatus.AllocatedResources
		}
		if bPod.Status.Resources != nil {
			statusReqs = bPod.Status.Resources.Requests
		}
		allocCpuStr, allocMemStr := formatResourcePair(allocReqs)
		statusCpuStr, statusMemStr := formatResourcePair(statusReqs)

		resizeState := string(getResizeState(bPod))
		if resizeState == "" {
			resizeState = "none"
		}

		return fmt.Sprintf("Want balloon pod %q size: {cpu: %q, memory: %q}, got requests: {cpu: %q, memory: %q}, got allocated: {cpu: %q, memory: %q}, got podStatusRequests: {cpu: %q, memory: %q}, resizeState: %q.",
			bPod.Name, desiredCpu.String(), desiredMem.String(), bPodCpu.String(), bPodMem.String(), allocCpuStr, allocMemStr, statusCpuStr, statusMemStr, resizeState)
	case BalloonPodNotRunning:
		bPod := balloonPods[0]
		return fmt.Sprintf("Want balloon pod %q status: %q, got: %q.", bPod.Name, v1.PodRunning, bPod.Status.Phase)
	default:
		return "Unknown balloon pod problem."
	}
}

func isNodeReady(node *v1.Node) bool {
	for _, c := range node.Status.Conditions {
		if c.Type == v1.NodeReady {
			return c.Status == v1.ConditionTrue
		}
	}
	return false
}

// nodeSupportsIppr reports whether the node's Kubelet version can safely resize balloon pods in place.
// Worker nodes are not upgraded together with KCP and can remain on 1.36 after KCP moves to 1.37.
// Kubelet 1.36 enables InPlacePodLevelResourcesVerticalScaling by default while freezing pod-level
// status.resources.requests.memory at its initial PodRunning value, causing 1.37 kube-scheduler to
// over-count shrunk balloon pods (b/583763422). Nodes on < 1.37 (or with unparseable versions)
// therefore use pod recreation without recording IPPR fallback metrics.
func nodeSupportsIppr(node *v1.Node) bool {
	if node == nil {
		return false
	}
	kubeletVerStr := node.Status.NodeInfo.KubeletVersion
	kubeletVer, err := version.FromString(kubeletVerStr)
	if err != nil {
		klog.Warningf("Skipping in-place balloon pod resize for node %q: unable to parse kubelet version %q: %v", node.Name, kubeletVerStr, err)
		return false
	}
	if kubeletVer.MinorVersion().LessThan(version.MinorVersion{1, minIpprKubeletMinorVersion}) {
		klog.V(4).Infof("Skipping in-place balloon pod resize for node %q: kubelet version %q is older than 1.%d", node.Name, kubeletVerStr, minIpprKubeletMinorVersion)
		return false
	}
	return true
}
