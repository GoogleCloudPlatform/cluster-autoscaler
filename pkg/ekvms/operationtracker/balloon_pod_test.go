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
	"testing"

	"github.com/stretchr/testify/assert"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	podutils "k8s.io/autoscaler/cluster-autoscaler/utils/pod"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	test_utils "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	calculator_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator/test"
	ekvms_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/test"
)

func TestGenerateBalloonPod(t *testing.T) {
	testCases := []struct {
		desc        string
		node        *apiv1.Node
		cpu         resource.Quantity
		memory      resource.Quantity
		generateUID bool
		wantErr     bool
	}{
		{
			desc:    "success",
			node:    test.BuildTestNode("node1", 1000, 1024*1024),
			cpu:     *resource.NewMilliQuantity(1000, resource.DecimalSI),
			memory:  *resource.NewQuantity(1024*1024, resource.DecimalSI),
			wantErr: false,
		},
		{
			desc:    "no node provided error",
			node:    nil,
			cpu:     *resource.NewMilliQuantity(1000, resource.DecimalSI),
			memory:  *resource.NewQuantity(1024*1024, resource.DecimalSI),
			wantErr: true,
		},
		{
			desc:        "uid and name generated",
			node:        test.BuildTestNode("node1", 1000, 1024*1024),
			cpu:         *resource.NewMilliQuantity(1000, resource.DecimalSI),
			memory:      *resource.NewQuantity(1024*1024, resource.DecimalSI),
			generateUID: true,
			wantErr:     false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {

			pod, err := GenerateBalloonPod(tc.node, tc.cpu, tc.memory, tc.generateUID)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}

			assert.True(t, IsBalloonPod(pod))
			assert.True(t, podutils.IsDaemonSetPod(pod))
			assert.Equal(t, tc.node.Name, pod.Spec.NodeName)
			resourceList := apiv1.ResourceList{
				apiv1.ResourceCPU:    tc.cpu,
				apiv1.ResourceMemory: tc.memory,
			}

			assert.Equal(t, resourceList, pod.Spec.Containers[0].Resources.Requests)
			assert.Equal(t, resourceList, pod.Spec.Containers[0].Resources.Limits)
			if tc.generateUID {
				assert.NotEmpty(t, pod.UID)
				assert.NotEmpty(t, pod.Name)
			}
		})
	}
}

func TestIsBalloonPod(t *testing.T) {
	setNamespace := func(ns string) func(*apiv1.Pod) {
		return func(pod *apiv1.Pod) {
			pod.Namespace = ns
		}
	}

	testCases := []struct {
		desc string
		pod  *apiv1.Pod
		want bool
	}{
		{
			desc: "Balloon Pod",
			pod:  test.BuildTestPod("gke-system-balloon-pod-1234", 0, 0, setNamespace("kube-system")),
			want: true,
		},
		{
			desc: "Wrong name",
			pod:  test.BuildTestPod("random", 0, 0, setNamespace("kube-system")),
			want: false,
		},
		{
			desc: "Wrong namespace",
			pod:  test.BuildTestPod("gke-system-balloon-pod-1234", 0, 0, setNamespace("default")),
			want: false,
		},
		{
			desc: "nil pointer",
			pod:  nil,
			want: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			assert.Equal(t, tc.want, IsBalloonPod(tc.pod))
		})
	}
}

func TestInjectDefaultBalloonPod(t *testing.T) {
	machineType := "ek-standard-32"
	nodeName := "node-1"
	resizableNode := ekvms_test.NewResizableNodeBuilder(nodeName, 32000, 128).WithSupportedMachineType(machineType).WithReadyStatus().Build()
	nonResizableNode := test_utils.BuildTestNode("node-2", 32000, 128)

	bPod, _ := GenerateBalloonPod(
		resizableNode,
		*resource.NewMilliQuantity(6000, resource.DecimalSI),
		*resource.NewQuantity(12*1024*1024, resource.DecimalSI),
		true) // Need UID or removePod doesn't work.

	testCases := []struct {
		desc              string
		node              *apiv1.Node
		existingPods      []*apiv1.Pod
		expectedErr       bool
		expectedResources apiv1.ResourceList
	}{
		{
			desc: "default balloon pod set",
			node: resizableNode,
			expectedResources: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(8000, resource.DecimalSI),   // Determined by calculator.
				apiv1.ResourceMemory: *resource.NewQuantity(32*size.GiB, resource.DecimalSI), // Determined by calculator.
			},
		},
		{
			desc:        "non resizable node",
			node:        nonResizableNode,
			expectedErr: true,
		},
		{
			desc:         "existing balloon pod replaced with default one",
			node:         resizableNode,
			existingPods: []*apiv1.Pod{bPod},
			expectedResources: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(8000, resource.DecimalSI),   // Determined by calculator.
				apiv1.ResourceMemory: *resource.NewQuantity(32*size.GiB, resource.DecimalSI), // Determined by calculator.
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(tc.node, tc.existingPods...))
			assert.NoError(t, err)
			nodeInfo, err := snapshot.GetNodeInfo(tc.node.Name)
			assert.Len(t, tc.existingPods, len(nodeInfo.Pods()))
			assert.NoError(t, err)
			err = InjectDefaultBalloonPod(nodeInfo, calculator_test.NewWithProvider(machinetypes.NewMachineConfigProvider(nil)))
			if tc.expectedErr {
				assert.Error(t, err)
				return
			} else {
				assert.NoError(t, err)
			}

			assert.Len(t, nodeInfo.Pods(), 1)
			assert.Equal(t, tc.expectedResources, nodeInfo.Pods()[0].Pod.Spec.Containers[0].Resources.Requests)
		})
	}
}

func TestBalloonPodHasCorrectSize(t *testing.T) {
	nodeMilliCpu := int64(10 * 1000)
	nodeMem := int64(10 * size.GiB)
	node := test.BuildTestNode("node1", nodeMilliCpu, nodeMem)
	testCases := []struct {
		desc               string
		desiredAllocatable size.Allocatable
		bPodCpu            resource.Quantity
		bPodMem            resource.Quantity
		expected           bool
	}{
		{
			desc:               "with correct cpu & memory",
			desiredAllocatable: size.Allocatable{MilliCpus: nodeMilliCpu / 4, KBytes: nodeMem / 4 / size.KiB},
			bPodCpu:            *resource.NewMilliQuantity(nodeMilliCpu*3/4, resource.BinarySI),
			bPodMem:            *resource.NewQuantity(nodeMem*3/4, resource.BinarySI),
			expected:           true,
		},
		{
			desc:               "with incorrect cpu",
			desiredAllocatable: size.Allocatable{MilliCpus: nodeMilliCpu / 4, KBytes: nodeMem / 4 / size.KiB},
			bPodCpu:            *resource.NewMilliQuantity(nodeMilliCpu/4, resource.BinarySI),
			bPodMem:            *resource.NewQuantity(nodeMem*3/4, resource.BinarySI),
			expected:           false,
		},
		{
			desc:               "with incorrect memory",
			desiredAllocatable: size.Allocatable{MilliCpus: nodeMilliCpu / 4, KBytes: nodeMem / 4 / size.KiB},
			bPodCpu:            *resource.NewMilliQuantity(nodeMilliCpu*3/4, resource.BinarySI),
			bPodMem:            *resource.NewQuantity(nodeMem/4, resource.BinarySI),
			expected:           false,
		},
		{
			desc:               "with incorrect cpu & memory",
			desiredAllocatable: size.Allocatable{MilliCpus: nodeMilliCpu / 4, KBytes: nodeMem / 4 / size.KiB},
			bPodCpu:            *resource.NewMilliQuantity(nodeMilliCpu/4, resource.BinarySI),
			bPodMem:            *resource.NewQuantity(nodeMem/4, resource.BinarySI),
			expected:           false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			bPod, err := GenerateBalloonPod(
				node,
				tc.bPodCpu,
				tc.bPodMem,
				false)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, balloonPodHasCorrectSize(node, tc.desiredAllocatable, bPod))
		})
	}
}

func TestBalloonPodIsCorrect(t *testing.T) {
	nodeMilliCpu := int64(10 * 1000)
	nodeMem := int64(10 * size.GiB)
	node := test.BuildTestNode("node", nodeMilliCpu, nodeMem)
	desiredAllocatable := size.Allocatable{MilliCpus: nodeMilliCpu / 2, KBytes: nodeMem / 2 / size.KiB}

	testCases := []struct {
		desc       string
		bPodSizes  [][]resource.Quantity
		bPodStatus apiv1.PodStatus
		wantResult bool
		wantStatus BalloonPodStatus
	}{
		{
			desc:       "no ballooon pods",
			wantResult: false,
			wantStatus: BalloonPodWrongCount,
		},
		{
			desc: "more than one balloon pod",
			bPodSizes: [][]resource.Quantity{
				{*resource.NewMilliQuantity(nodeMilliCpu/2, resource.BinarySI), *resource.NewQuantity(nodeMem/2, resource.BinarySI)},
				{*resource.NewMilliQuantity(nodeMilliCpu/2, resource.BinarySI), *resource.NewQuantity(nodeMem/2, resource.BinarySI)},
			},
			bPodStatus: apiv1.PodStatus{
				Phase: apiv1.PodRunning,
			},
			wantResult: false,
			wantStatus: BalloonPodWrongCount,
		},
		{
			desc: "incorrect size",
			bPodSizes: [][]resource.Quantity{
				{*resource.NewMilliQuantity(nodeMilliCpu/4, resource.BinarySI), *resource.NewQuantity(nodeMem/4, resource.BinarySI)},
			},
			bPodStatus: apiv1.PodStatus{
				Phase: apiv1.PodRunning,
			},
			wantResult: false,
			wantStatus: BalloonPodIncorrectSize,
		},
		{
			desc: "balloon pod not running",
			bPodSizes: [][]resource.Quantity{
				{*resource.NewMilliQuantity(nodeMilliCpu/2, resource.BinarySI), *resource.NewQuantity(nodeMem/2, resource.BinarySI)},
			},
			bPodStatus: apiv1.PodStatus{
				Phase: apiv1.PodFailed,
			},
			wantResult: false,
			wantStatus: BalloonPodNotRunning,
		},
		{
			desc: "correct balloon pod - running",
			bPodSizes: [][]resource.Quantity{
				{*resource.NewMilliQuantity(nodeMilliCpu/2, resource.BinarySI), *resource.NewQuantity(nodeMem/2, resource.BinarySI)},
			},
			bPodStatus: apiv1.PodStatus{
				Phase: apiv1.PodRunning,
			},
			wantResult: true,
			wantStatus: BalloonPodOk,
		},
		{
			desc: "correct balloon pod - waiting",
			bPodSizes: [][]resource.Quantity{
				{*resource.NewMilliQuantity(nodeMilliCpu/2, resource.BinarySI), *resource.NewQuantity(nodeMem/2, resource.BinarySI)},
			},
			bPodStatus: apiv1.PodStatus{
				Phase: apiv1.PodPending,
				ContainerStatuses: []apiv1.ContainerStatus{
					{
						State: apiv1.ContainerState{
							Waiting: &apiv1.ContainerStateWaiting{},
						},
					},
				},
			},
			wantResult: true,
			wantStatus: BalloonPodOk,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			var bPods []*apiv1.Pod
			for _, bPodSize := range tc.bPodSizes {
				bPodCpu, bPodMem := bPodSize[0], bPodSize[1]
				bPod, err := GenerateBalloonPod(node, bPodCpu, bPodMem, false)
				assert.NoError(t, err)
				bPod.Status = tc.bPodStatus
				bPods = append(bPods, bPod)
			}

			bPodIsCorrect, bPodStatus := balloonPodIsCorrect(node, desiredAllocatable, bPods)
			assert.Equal(t, tc.wantResult, bPodIsCorrect)
			assert.Equal(t, tc.wantStatus, bPodStatus)
		})
	}
}

func TestSecurityPolicy(t *testing.T) {
	node := test.BuildTestNode("node", 1000, 1024)

	bPod, err := GenerateBalloonPod(node, *resource.NewQuantity(1, resource.BinarySI), *resource.NewQuantity(1, resource.BinarySI), false)
	assert.NoError(t, err)

	// Required by security policy to be set on global level.
	assert.True(t, *bPod.Spec.SecurityContext.RunAsNonRoot)

	for _, c := range bPod.Spec.Containers {
		assert.False(t, *c.SecurityContext.AllowPrivilegeEscalation)
		assert.True(t, c.SecurityContext.RunAsNonRoot == nil || *c.SecurityContext.RunAsNonRoot)
	}
}
