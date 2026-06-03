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
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	podutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/pod"
	"k8s.io/kubernetes/pkg/api/v1/pod"
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
		return fmt.Sprintf("Want balloon pod %q size: {cpu: %q, memory: %q}, got: {cpu: %q, memory: %q}.", bPod.Name, desiredCpu.String(), desiredMem.String(), bPodCpu, bPodMem)
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
