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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	calculator_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator/test"
	ekvms_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	podutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/pod"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	test_utils "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
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
			assert.Empty(t, pod.Spec.Containers[0].Resources.Limits)
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
	resizableNode := ekvms_test.NewNodeBuilder(nodeName, 32000, 128).WithSupportedMachineType(machineType).WithReadyStatus().Build()
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

func TestBalloonPodIsCorrect_IpprEnabled(t *testing.T) {
	t.Parallel()

	nodeMilliCPU := int64(10 * 1000)
	nodeMem := int64(10 * size.GiB)
	node := test.WithAllocatable(test.BuildTestNode("node", nodeMilliCPU, nodeMem), nodeMilliCPU, nodeMem)

	desiredAllocatable := size.Allocatable{
		MilliCpus: nodeMilliCPU / 2,
		KBytes:    (nodeMem / 2) / size.KiB,
	}

	correctCPU := *resource.NewMilliQuantity(nodeMilliCPU/2, resource.DecimalSI)
	correctMem := *resource.NewQuantity(nodeMem/2, resource.BinarySI)

	wrongCPU := *resource.NewMilliQuantity(nodeMilliCPU/4, resource.DecimalSI)
	wrongMem := *resource.NewQuantity(nodeMem/4, resource.BinarySI)

	matchedAlloc := apiv1.ResourceList{
		apiv1.ResourceCPU:    correctCPU,
		apiv1.ResourceMemory: correctMem,
	}

	newStatus := func(containerName string, allocated apiv1.ResourceList) apiv1.PodStatus {
		status := apiv1.PodStatus{Phase: apiv1.PodRunning}
		if containerName != "" {
			status.ContainerStatuses = []apiv1.ContainerStatus{{
				Name:               containerName,
				AllocatedResources: allocated,
			}}
		}
		return status
	}

	// withConditions returns a status whose resize state is reported through pod conditions, which is the only source
	// getResizeState reads.
	withConditions := func(status apiv1.PodStatus, conditions ...apiv1.PodCondition) apiv1.PodStatus {
		status.Conditions = conditions
		return status
	}
	resizeCondition := func(condType apiv1.PodConditionType, reason string) apiv1.PodCondition {
		return apiv1.PodCondition{Type: condType, Status: apiv1.ConditionTrue, Reason: reason}
	}

	testCases := []struct {
		desc       string
		pods       func(t *testing.T) []*apiv1.Pod
		wantResult bool
		wantStatus BalloonPodStatus
	}{
		{
			desc: "success - allocated resources match desired size",
			pods: func(t *testing.T) []*apiv1.Pod {
				bPod, err := GenerateBalloonPod(node, correctCPU, correctMem, false)
				assert.NoError(t, err)
				bPod.Status = newStatus(balloonContainerName, matchedAlloc)
				return []*apiv1.Pod{bPod}
			},
			wantResult: true,
			wantStatus: BalloonPodOk,
		},
		{
			desc: "incorrect size - spec requests mismatch even if allocation matches",
			pods: func(t *testing.T) []*apiv1.Pod {
				bPod, err := GenerateBalloonPod(node, wrongCPU, correctMem, false)
				assert.NoError(t, err)
				bPod.Status = newStatus(balloonContainerName, matchedAlloc)
				return []*apiv1.Pod{bPod}
			},
			wantResult: false,
			wantStatus: BalloonPodIncorrectSize,
		},
		{
			desc: "incorrect size - allocated resources do not match",
			pods: func(t *testing.T) []*apiv1.Pod {
				bPod, err := GenerateBalloonPod(node, correctCPU, correctMem, false)
				assert.NoError(t, err)
				bPod.Status = newStatus(balloonContainerName, apiv1.ResourceList{
					apiv1.ResourceCPU:    wrongCPU,
					apiv1.ResourceMemory: wrongMem,
				})
				return []*apiv1.Pod{bPod}
			},
			wantResult: false,
			wantStatus: BalloonPodIncorrectSize,
		},
		{
			desc: "incorrect size - allocated resources missing memory key",
			pods: func(t *testing.T) []*apiv1.Pod {
				bPod, err := GenerateBalloonPod(node, correctCPU, correctMem, false)
				assert.NoError(t, err)
				bPod.Status = newStatus(balloonContainerName, apiv1.ResourceList{
					apiv1.ResourceCPU: correctCPU,
				})
				return []*apiv1.Pod{bPod}
			},
			wantResult: false,
			wantStatus: BalloonPodIncorrectSize,
		},
		{
			desc: "success - empty container statuses list (relies on requests)",
			pods: func(t *testing.T) []*apiv1.Pod {
				bPod, err := GenerateBalloonPod(node, correctCPU, correctMem, false)
				assert.NoError(t, err)
				bPod.Status = newStatus("", nil)
				return []*apiv1.Pod{bPod}
			},
			wantResult: true,
			wantStatus: BalloonPodOk,
		},
		{
			desc: "success - container status present but AllocatedResources is nil (relies on requests)",
			pods: func(t *testing.T) []*apiv1.Pod {
				bPod, err := GenerateBalloonPod(node, correctCPU, correctMem, false)
				assert.NoError(t, err)
				bPod.Status = newStatus(balloonContainerName, nil)
				return []*apiv1.Pod{bPod}
			},
			wantResult: true,
			wantStatus: BalloonPodOk,
		},
		{
			desc: "success - container status present but AllocatedResources is empty map (relies on requests)",
			pods: func(t *testing.T) []*apiv1.Pod {
				bPod, err := GenerateBalloonPod(node, correctCPU, correctMem, false)
				assert.NoError(t, err)
				bPod.Status = newStatus(balloonContainerName, apiv1.ResourceList{})
				return []*apiv1.Pod{bPod}
			},
			wantResult: true,
			wantStatus: BalloonPodOk,
		},
		{
			desc: "success - container status has other container, skips to requests check",
			pods: func(t *testing.T) []*apiv1.Pod {
				bPod, err := GenerateBalloonPod(node, correctCPU, correctMem, false)
				assert.NoError(t, err)
				bPod.Status = newStatus("other-sidecar-container", matchedAlloc)
				return []*apiv1.Pod{bPod}
			},
			wantResult: true,
			wantStatus: BalloonPodOk,
		},
		{
			desc: "correct size - deprecated Pod.Status.Resize is ignored",
			pods: func(t *testing.T) []*apiv1.Pod {
				bPod, err := GenerateBalloonPod(node, correctCPU, correctMem, false)
				assert.NoError(t, err)
				bPod.Status = newStatus(balloonContainerName, matchedAlloc)
				// A node that only sets the deprecated field reports no resize at all, so a pod whose spec and
				// allocation already match is correct. The resize conditions below are the only source consulted.
				bPod.Status.Resize = apiv1.PodResizeStatusInfeasible
				return []*apiv1.Pod{bPod}
			},
			wantResult: true,
			wantStatus: BalloonPodOk,
		},
		{
			desc: "incorrect size - PodResizeInProgress condition",
			pods: func(t *testing.T) []*apiv1.Pod {
				bPod, err := GenerateBalloonPod(node, correctCPU, correctMem, false)
				assert.NoError(t, err)
				bPod.Status = withConditions(newStatus(balloonContainerName, matchedAlloc),
					resizeCondition(apiv1.PodResizeInProgress, ""))
				return []*apiv1.Pod{bPod}
			},
			wantResult: false,
			wantStatus: BalloonPodIncorrectSize,
		},
		{
			desc: "incorrect size - PodResizeInProgress condition with Error reason",
			pods: func(t *testing.T) []*apiv1.Pod {
				bPod, err := GenerateBalloonPod(node, correctCPU, correctMem, false)
				assert.NoError(t, err)
				bPod.Status = withConditions(newStatus(balloonContainerName, matchedAlloc),
					resizeCondition(apiv1.PodResizeInProgress, apiv1.PodReasonError))
				return []*apiv1.Pod{bPod}
			},
			wantResult: false,
			wantStatus: BalloonPodIncorrectSize,
		},
		{
			desc: "incorrect size - PodResizePending condition with Infeasible reason",
			pods: func(t *testing.T) []*apiv1.Pod {
				bPod, err := GenerateBalloonPod(node, correctCPU, correctMem, false)
				assert.NoError(t, err)
				bPod.Status = withConditions(newStatus(balloonContainerName, matchedAlloc),
					resizeCondition(apiv1.PodResizePending, apiv1.PodReasonInfeasible))
				return []*apiv1.Pod{bPod}
			},
			wantResult: false,
			wantStatus: BalloonPodIncorrectSize,
		},
		{
			desc: "incorrect size - PodResizePending condition with Deferred reason",
			pods: func(t *testing.T) []*apiv1.Pod {
				bPod, err := GenerateBalloonPod(node, correctCPU, correctMem, false)
				assert.NoError(t, err)
				bPod.Status = withConditions(newStatus(balloonContainerName, matchedAlloc),
					resizeCondition(apiv1.PodResizePending, apiv1.PodReasonDeferred))
				return []*apiv1.Pod{bPod}
			},
			wantResult: false,
			wantStatus: BalloonPodIncorrectSize,
		},
		{
			desc: "no pods passed - returns not found",
			pods: func(t *testing.T) []*apiv1.Pod {
				return []*apiv1.Pod{}
			},
			wantResult: false,
			wantStatus: BalloonPodWrongCount,
		},
		{
			desc: "multiple pods passed - returns too many",
			pods: func(t *testing.T) []*apiv1.Pod {
				p1, err := GenerateBalloonPod(node, correctCPU, correctMem, false)
				assert.NoError(t, err)
				p1.Name = "balloon-pod-1"
				p1.Status = newStatus(balloonContainerName, matchedAlloc)

				p2, err := GenerateBalloonPod(node, correctCPU, correctMem, false)
				assert.NoError(t, err)
				p2.Name = "balloon-pod-2"
				p2.Status = newStatus(balloonContainerName, matchedAlloc)

				return []*apiv1.Pod{p1, p2}
			},
			wantResult: false,
			wantStatus: BalloonPodWrongCount,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			pods := tc.pods(t)
			bPodIsCorrect, bPodStatus := balloonPodIsCorrect(node, desiredAllocatable, pods)

			assert.Equal(t, tc.wantResult, bPodIsCorrect)
			assert.Equal(t, tc.wantStatus, bPodStatus)
		})
	}
}

func TestBalloonContainerHelpers(t *testing.T) {
	t.Parallel()

	desiredCPU := *resource.NewMilliQuantity(2000, resource.DecimalSI)
	desiredMem := *resource.NewQuantity(200*size.MiB, resource.DecimalSI)
	otherCPU := *resource.NewMilliQuantity(1000, resource.DecimalSI)
	otherMem := *resource.NewQuantity(100*size.MiB, resource.DecimalSI)

	podWithRequests := func(cName string, res apiv1.ResourceList) *apiv1.Pod {
		return &apiv1.Pod{
			Spec: apiv1.PodSpec{
				Containers: []apiv1.Container{
					{Name: cName, Resources: apiv1.ResourceRequirements{Requests: res}},
				},
			},
		}
	}

	podWithAllocated := func(cName string, res apiv1.ResourceList) *apiv1.Pod {
		return &apiv1.Pod{
			Status: apiv1.PodStatus{
				ContainerStatuses: []apiv1.ContainerStatus{
					{Name: cName, AllocatedResources: res},
				},
			},
		}
	}

	t.Run("getBalloonContainerSpec", func(t *testing.T) {
		testCases := []struct {
			desc      string
			pod       *apiv1.Pod
			wantFound bool
		}{
			{desc: "nil pod", pod: nil, wantFound: false},
			{desc: "empty containers list", pod: &apiv1.Pod{}, wantFound: false},
			{desc: "single container matching", pod: podWithRequests(balloonContainerName, nil), wantFound: true},
			{desc: "single container different name", pod: podWithRequests("custom-pause", nil), wantFound: false},
			{
				desc: "multiple containers with balloon container present",
				pod: &apiv1.Pod{
					Spec: apiv1.PodSpec{
						Containers: []apiv1.Container{
							{Name: "sidecar-logger"},
							{Name: balloonContainerName},
						},
					},
				},
				wantFound: true,
			},
			{
				desc: "multiple containers without balloon container",
				pod: &apiv1.Pod{
					Spec: apiv1.PodSpec{
						Containers: []apiv1.Container{
							{Name: "sidecar-1"},
							{Name: "sidecar-2"},
						},
					},
				},
				wantFound: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.desc, func(t *testing.T) {
				t.Parallel()
				res := getBalloonContainerSpec(tc.pod)
				if !tc.wantFound {
					assert.Nil(t, res)
					return
				}
				if assert.NotNil(t, res) {
					assert.Equal(t, balloonContainerName, res.Name)
				}
			})
		}
	})

	t.Run("getBalloonContainerStatus", func(t *testing.T) {
		testCases := []struct {
			desc      string
			pod       *apiv1.Pod
			wantFound bool
		}{
			{desc: "nil pod", pod: nil, wantFound: false},
			{desc: "empty container statuses list", pod: &apiv1.Pod{}, wantFound: false},
			{desc: "single container matching", pod: podWithAllocated(balloonContainerName, nil), wantFound: true},
			{desc: "single container different name", pod: podWithAllocated("custom-pause", nil), wantFound: false},
			{
				desc: "multiple statuses with balloon present",
				pod: &apiv1.Pod{
					Status: apiv1.PodStatus{
						ContainerStatuses: []apiv1.ContainerStatus{
							{Name: "sidecar-logger"},
							{Name: balloonContainerName},
						},
					},
				},
				wantFound: true,
			},
			{
				desc: "multiple statuses without balloon",
				pod: &apiv1.Pod{
					Status: apiv1.PodStatus{
						ContainerStatuses: []apiv1.ContainerStatus{
							{Name: "sidecar-1"},
							{Name: "sidecar-2"},
						},
					},
				},
				wantFound: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.desc, func(t *testing.T) {
				t.Parallel()
				res := getBalloonContainerStatus(tc.pod)
				if !tc.wantFound {
					assert.Nil(t, res)
					return
				}
				if assert.NotNil(t, res) {
					assert.Equal(t, balloonContainerName, res.Name)
				}
			})
		}
	})

	t.Run("hasMatchingSpec", func(t *testing.T) {
		testCases := []struct {
			desc      string
			pod       *apiv1.Pod
			targetCPU resource.Quantity
			targetMem resource.Quantity
			wantMatch bool
		}{
			{desc: "nil pod", pod: nil, targetCPU: desiredCPU, targetMem: desiredMem, wantMatch: false},
			{desc: "pod with no containers", pod: &apiv1.Pod{}, targetCPU: desiredCPU, targetMem: desiredMem, wantMatch: false},
			{
				desc:      "pod without balloon container",
				pod:       podWithRequests("not-balloon", apiv1.ResourceList{apiv1.ResourceCPU: desiredCPU, apiv1.ResourceMemory: desiredMem}),
				targetCPU: desiredCPU,
				targetMem: desiredMem,
				wantMatch: false,
			},
			{
				desc:      "matching spec requests",
				pod:       podWithRequests(balloonContainerName, apiv1.ResourceList{apiv1.ResourceCPU: desiredCPU, apiv1.ResourceMemory: desiredMem}),
				targetCPU: desiredCPU,
				targetMem: desiredMem,
				wantMatch: true,
			},
			{
				desc:      "matching spec equivalent quantity units (2000m vs 2)",
				pod:       podWithRequests(balloonContainerName, apiv1.ResourceList{apiv1.ResourceCPU: *resource.NewQuantity(2, resource.DecimalSI), apiv1.ResourceMemory: desiredMem}),
				targetCPU: desiredCPU,
				targetMem: desiredMem,
				wantMatch: true,
			},
			{
				desc:      "missing CPU key in requests map",
				pod:       podWithRequests(balloonContainerName, apiv1.ResourceList{apiv1.ResourceMemory: desiredMem}),
				targetCPU: desiredCPU,
				targetMem: desiredMem,
				wantMatch: false,
			},
			{
				desc:      "missing memory key in requests map",
				pod:       podWithRequests(balloonContainerName, apiv1.ResourceList{apiv1.ResourceCPU: desiredCPU}),
				targetCPU: desiredCPU,
				targetMem: desiredMem,
				wantMatch: false,
			},
			{
				desc:      "mismatched CPU request",
				pod:       podWithRequests(balloonContainerName, apiv1.ResourceList{apiv1.ResourceCPU: otherCPU, apiv1.ResourceMemory: desiredMem}),
				targetCPU: desiredCPU,
				targetMem: desiredMem,
				wantMatch: false,
			},
			{
				desc:      "mismatched memory request",
				pod:       podWithRequests(balloonContainerName, apiv1.ResourceList{apiv1.ResourceCPU: desiredCPU, apiv1.ResourceMemory: otherMem}),
				targetCPU: desiredCPU,
				targetMem: desiredMem,
				wantMatch: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.desc, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, tc.wantMatch, hasMatchingSpec(tc.pod, tc.targetCPU, tc.targetMem))
			})
		}
	})

	t.Run("hasMatchingAlloc", func(t *testing.T) {
		testCases := []struct {
			desc      string
			pod       *apiv1.Pod
			targetCPU resource.Quantity
			targetMem resource.Quantity
			wantMatch bool
		}{
			{desc: "nil pod", pod: nil, targetCPU: desiredCPU, targetMem: desiredMem, wantMatch: false},
			{desc: "pod with no container statuses", pod: &apiv1.Pod{}, targetCPU: desiredCPU, targetMem: desiredMem, wantMatch: false},
			{
				desc:      "pod without balloon container status",
				pod:       podWithAllocated("not-balloon", apiv1.ResourceList{apiv1.ResourceCPU: desiredCPU, apiv1.ResourceMemory: desiredMem}),
				targetCPU: desiredCPU,
				targetMem: desiredMem,
				wantMatch: false,
			},
			{
				desc:      "container status present but AllocatedResources is empty",
				pod:       podWithAllocated(balloonContainerName, nil),
				targetCPU: desiredCPU,
				targetMem: desiredMem,
				wantMatch: false,
			},
			{
				desc:      "matching allocated resources",
				pod:       podWithAllocated(balloonContainerName, apiv1.ResourceList{apiv1.ResourceCPU: desiredCPU, apiv1.ResourceMemory: desiredMem}),
				targetCPU: desiredCPU,
				targetMem: desiredMem,
				wantMatch: true,
			},
			{
				desc:      "missing CPU key in AllocatedResources map",
				pod:       podWithAllocated(balloonContainerName, apiv1.ResourceList{apiv1.ResourceMemory: desiredMem}),
				targetCPU: desiredCPU,
				targetMem: desiredMem,
				wantMatch: false,
			},
			{
				desc:      "missing memory key in AllocatedResources map",
				pod:       podWithAllocated(balloonContainerName, apiv1.ResourceList{apiv1.ResourceCPU: desiredCPU}),
				targetCPU: desiredCPU,
				targetMem: desiredMem,
				wantMatch: false,
			},
			{
				desc:      "mismatched allocated CPU",
				pod:       podWithAllocated(balloonContainerName, apiv1.ResourceList{apiv1.ResourceCPU: otherCPU, apiv1.ResourceMemory: desiredMem}),
				targetCPU: desiredCPU,
				targetMem: desiredMem,
				wantMatch: false,
			},
			{
				desc:      "mismatched allocated memory",
				pod:       podWithAllocated(balloonContainerName, apiv1.ResourceList{apiv1.ResourceCPU: desiredCPU, apiv1.ResourceMemory: otherMem}),
				targetCPU: desiredCPU,
				targetMem: desiredMem,
				wantMatch: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.desc, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, tc.wantMatch, hasMatchingAlloc(tc.pod, tc.targetCPU, tc.targetMem))
			})
		}
	})
}

func TestGetResizeState(t *testing.T) {
	t.Parallel()

	condition := func(condType apiv1.PodConditionType, reason string) apiv1.PodCondition {
		return apiv1.PodCondition{Type: condType, Status: apiv1.ConditionTrue, Reason: reason}
	}

	testCases := []struct {
		desc      string
		pod       *apiv1.Pod
		wantState balloonPodResizeState
	}{
		{
			desc:      "nil pod",
			pod:       nil,
			wantState: resizeStateNone,
		},
		{
			desc:      "no resize reported",
			pod:       &apiv1.Pod{},
			wantState: resizeStateNone,
		},
		{
			desc: "PodResizePending with Infeasible reason",
			pod: &apiv1.Pod{Status: apiv1.PodStatus{
				Conditions: []apiv1.PodCondition{condition(apiv1.PodResizePending, apiv1.PodReasonInfeasible)},
			}},
			wantState: resizeStateInfeasible,
		},
		{
			desc: "PodResizePending with Deferred reason",
			pod: &apiv1.Pod{Status: apiv1.PodStatus{
				Conditions: []apiv1.PodCondition{condition(apiv1.PodResizePending, apiv1.PodReasonDeferred)},
			}},
			wantState: resizeStateDeferred,
		},
		{
			desc: "PodResizePending with an unknown reason falls back to deferred",
			pod: &apiv1.Pod{Status: apiv1.PodStatus{
				Conditions: []apiv1.PodCondition{condition(apiv1.PodResizePending, "SomethingNew")},
			}},
			wantState: resizeStateDeferred,
		},
		{
			desc: "PodResizeInProgress without a reason",
			pod: &apiv1.Pod{Status: apiv1.PodStatus{
				Conditions: []apiv1.PodCondition{condition(apiv1.PodResizeInProgress, "")},
			}},
			wantState: resizeStateInProgress,
		},
		{
			desc: "PodResizeInProgress with Error reason",
			pod: &apiv1.Pod{Status: apiv1.PodStatus{
				Conditions: []apiv1.PodCondition{condition(apiv1.PodResizeInProgress, apiv1.PodReasonError)},
			}},
			wantState: resizeStateError,
		},
		{
			desc: "both conditions set - pending describes the newest request and wins",
			pod: &apiv1.Pod{Status: apiv1.PodStatus{
				Conditions: []apiv1.PodCondition{
					condition(apiv1.PodResizeInProgress, ""),
					condition(apiv1.PodResizePending, apiv1.PodReasonDeferred),
				},
			}},
			wantState: resizeStateDeferred,
		},
		{
			desc: "deprecated Pod.Status.Resize is ignored",
			pod: &apiv1.Pod{Status: apiv1.PodStatus{
				Resize: apiv1.PodResizeStatusInProgress,
			}},
			wantState: resizeStateNone,
		},
		{
			desc: "deprecated Pod.Status.Resize does not override the conditions",
			pod: &apiv1.Pod{Status: apiv1.PodStatus{
				Resize:     apiv1.PodResizeStatusInProgress,
				Conditions: []apiv1.PodCondition{condition(apiv1.PodResizePending, apiv1.PodReasonInfeasible)},
			}},
			wantState: resizeStateInfeasible,
		},
		{
			desc: "unrelated conditions are ignored",
			pod: &apiv1.Pod{Status: apiv1.PodStatus{
				Conditions: []apiv1.PodCondition{condition(apiv1.PodReady, "")},
			}},
			wantState: resizeStateNone,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			got := getResizeState(tc.pod)
			assert.Equal(t, tc.wantState, got)
		})
	}
}

func TestBalloonPodResizeStateShouldRecreate(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc  string
		state balloonPodResizeState
		want  bool
	}{
		{desc: "none", state: resizeStateNone, want: false},
		{desc: "in progress", state: resizeStateInProgress, want: false},
		{desc: "deferred", state: resizeStateDeferred, want: true},
		{desc: "infeasible", state: resizeStateInfeasible, want: true},
		{desc: "error", state: resizeStateError, want: true},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, tc.state.shouldRecreate())
		})
	}
}
