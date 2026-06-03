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
	"fmt"
	"strings"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/utils"
	podutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/pod"
	"k8s.io/klog/v2"
	"sigs.k8s.io/yaml"
)

const (
	balloonPodPrefix = "gke-system-balloon-pod-"
	MinBalloonPodCpu = 0
	MinBalloonPodMem = 50 * size.MiB

	componentLabel                = "component"
	balloonPodComponentLabelValue = "gke-system-balloon-pod"
)

type BalloonPodState int

const (
	// BalloonPodTemplate means that Balloon Pod was just created.
	BalloonPodTemplate BalloonPodState = iota
	// BalloonPodWaiting means that Balloon Pod was spotted by kubelet and should be running soon.
	BalloonPodWaiting
	// BalloonPodRunning means that Balloon Pod is running.
	BalloonPodRunning
	// BalloonPodRunning means that Balloon Pod stopped.
	BalloonPodTerminated
)

var balloonPodStateMap map[BalloonPodState]string = map[BalloonPodState]string{
	BalloonPodTemplate:   "Template",
	BalloonPodWaiting:    "Waiting",
	BalloonPodRunning:    "Running",
	BalloonPodTerminated: "Terminated",
}

func (s BalloonPodState) String() string {
	state, found := balloonPodStateMap[s]
	if !found {
		klog.Warningf("Unknown balloon pod state %d", s)
		return "Unknown"
	}
	return state
}

type BalloonPodStatus string

const (
	BalloonPodOk            BalloonPodStatus = "BALLOON_POD_OK"
	BalloonPodWrongCount    BalloonPodStatus = "BALLOON_POD_WRONG_COUNT"
	BalloonPodIncorrectSize BalloonPodStatus = "BALLOON_POD_INCORRECT_SIZE"
	BalloonPodNotRunning    BalloonPodStatus = "BALLOON_POD_NOT_RUNNING"
)

const balloonPodTemplate = `
apiVersion: v1
kind: Pod
metadata:
  annotations:
    cluster-autoscaler.kubernetes.io/daemonset-pod: true
  labels:
    component: gke-system-balloon-pod
  generateName: gke-system-balloon-pod-
  namespace: kube-system
spec:
  containers:
  - command:
    - /pause
    image: gke.gcr.io/pause:3.6
    imagePullPolicy: IfNotPresent
    name: balloon
    securityContext:
      allowPrivilegeEscalation: false
      capabilities:
        drop:
        - ALL
  preemptionPolicy: PreemptLowerPriority
  priorityClassName: system-node-critical
  restartPolicy: Always
  schedulerName: default-scheduler
  hostNetwork: true
  dnsPolicy: ClusterFirstWithHostNet
  terminationGracePeriodSeconds: 1
  automountServiceAccountToken: false
  serviceAccountName: gke-system-balloon-pod
  securityContext:
    runAsNonRoot: true
    runAsGroup: 1000
    runAsUser: 1000
  tolerations:
  - effect: NoExecute
    operator: Exists
  - effect: NoSchedule
    operator: Exists
  - effect: NoSchedule
    key: cloud.google.com/autopilot-managed-node
    value: true
    operator: Equal
`

// GenerateBalloonPod generates a new Ballon Pod object.
func GenerateBalloonPod(node *apiv1.Node, cpu, memory resource.Quantity, generatePodUid bool) (*apiv1.Pod, error) {
	if node == nil {
		return nil, fmt.Errorf("node not provided")
	}

	pod := &apiv1.Pod{}
	if err := yaml.Unmarshal([]byte(balloonPodTemplate), pod); err != nil {
		return nil, fmt.Errorf("can't generate balloon pod: %v", err)
	}
	if generatePodUid {
		pod.UID = uuid.NewUUID()
		podName := fmt.Sprintf("%s%s", pod.GenerateName, pod.UID)
		if len(podName) >= 63 {
			podName = podName[:63]
		}
		pod.Name = podName
	}

	pod.Spec.NodeName = node.Name
	pod.Spec.Containers[0].Resources.Requests = apiv1.ResourceList{
		apiv1.ResourceCPU:    cpu,
		apiv1.ResourceMemory: memory,
	}
	pod.Spec.Containers[0].Resources.Limits = apiv1.ResourceList{
		apiv1.ResourceCPU:    cpu,
		apiv1.ResourceMemory: memory,
	}

	return pod, nil
}

// IsBalloonPod returns true if a given Pod is Balloon Pod.
func IsBalloonPod(pod *apiv1.Pod) bool {
	if pod == nil {
		return false
	}
	return pod.Namespace == "kube-system" && (strings.HasPrefix(pod.Name, balloonPodPrefix) || pod.GenerateName == balloonPodPrefix)
}

// InjectDefaultBalloonPod injects a balloon pod based on node's machine type. If a balloon pod exists, it's replaced with a new one.
func InjectDefaultBalloonPod(nodeInfo *framework.NodeInfo, calculator calculator.Calculator) error {
	if err := removeBalloonPodIfExists(nodeInfo); err != nil {
		return err
	}
	maxSize, err := utils.GetMaxResizableVmSize(calculator, nodeInfo.Node())
	if err != nil {
		return err
	}
	desiredAllocatable := calculator.ToAllocatable(nodeInfo.Node(), maxSize)
	bPodCpu, bPodMem := getBalloonPodSize(nodeInfo.Node(), desiredAllocatable)
	bPod, err := GenerateBalloonPod(nodeInfo.Node(), bPodCpu, bPodMem, true)
	if err != nil {
		return err
	}
	klog.V(4).Infof("Injecting default balloon pod to node %q with size: cpu %v, mem %v", nodeInfo.Node().Name, &bPodCpu, &bPodMem)
	nodeInfo.AddPod(framework.NewPodInfo(bPod, nil))
	nodeInfo.SetNode(nodeInfo.Node())
	return nil
}

// removeBalloonPodIfExists removes all balloon pods from the node info if any exist, otherwise does nothing.
func removeBalloonPodIfExists(nodeInfo *framework.NodeInfo) error {
	for _, podInfo := range nodeInfo.Pods() {
		if IsBalloonPod(podInfo.Pod) {
			if err := nodeInfo.RemovePod(klog.Background(), podInfo.Pod); err != nil {
				return err
			}
		}
	}
	return nil
}

func getBalloonPodSize(node *apiv1.Node, overrideSize size.Allocatable) (resource.Quantity, resource.Quantity) {
	nodeAllocatable := node.Status.Allocatable
	bPodCpu := nodeAllocatable.Cpu().MilliValue() - overrideSize.MilliCpus
	if bPodCpu < MinBalloonPodCpu {
		klog.Warningf("Balloon Pod cpu (%d) is less than 0 for node %s, setting to 0.", bPodCpu, node.Name)
		bPodCpu = MinBalloonPodCpu
	}
	bPodMem := nodeAllocatable.Memory().Value() - overrideSize.KBytes*size.KiB
	if bPodMem < MinBalloonPodMem {
		klog.Warningf("Balloon Pod memory (%d) is less than 50 MB for node %s, setting to 50 MB.", bPodMem, node.Name)
		bPodMem = MinBalloonPodMem
	}
	return *resource.NewMilliQuantity(bPodCpu, resource.DecimalSI), *resource.NewQuantity(bPodMem, resource.DecimalSI)
}

func balloonPodHasCorrectSize(node *apiv1.Node, desiredAllocatable size.Allocatable, bPod *apiv1.Pod) bool {
	desiredCpu, desiredMem := getBalloonPodSize(node, desiredAllocatable)

	currentBpRequests := podutils.PodRequests(bPod)
	currentCpu := currentBpRequests[apiv1.ResourceCPU]
	currentMem := currentBpRequests[apiv1.ResourceMemory]

	return desiredCpu.Equal(currentCpu) && desiredMem.Equal(currentMem)
}

func balloonPodHasCorrectState(bPod *apiv1.Pod) bool {
	state := getBalloonPodState(bPod)
	return state == BalloonPodWaiting || state == BalloonPodRunning
}

func balloonPodIsCorrect(node *apiv1.Node, desiredAllocatable size.Allocatable, bPods []*apiv1.Pod) (bool, BalloonPodStatus) {
	if len(bPods) != 1 {
		return false, BalloonPodWrongCount
	}
	bPod := bPods[0]
	if !balloonPodHasCorrectSize(node, desiredAllocatable, bPod) {
		return false, BalloonPodIncorrectSize
	}
	if !balloonPodHasCorrectState(bPod) {
		return false, BalloonPodNotRunning
	}
	return true, BalloonPodOk
}

func getBalloonPodState(pod *apiv1.Pod) BalloonPodState {
	if pod.Status.Phase == apiv1.PodRunning {
		return BalloonPodRunning
	}
	if pod.Status.Phase == apiv1.PodSucceeded || pod.Status.Phase == apiv1.PodFailed {
		return BalloonPodTerminated
	}
	if len(pod.Status.ContainerStatuses) < 1 {
		return BalloonPodTemplate
	}
	if pod.Status.ContainerStatuses[0].State.Waiting != nil {
		return BalloonPodWaiting
	}
	if pod.Status.ContainerStatuses[0].State.Running != nil {
		return BalloonPodRunning
	}
	if pod.Status.ContainerStatuses[0].State.Terminated != nil {
		return BalloonPodTerminated
	}

	return BalloonPodTemplate
}
