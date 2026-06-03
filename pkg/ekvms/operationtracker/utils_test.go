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
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/client-go/kubernetes/fake"
	client_testing "k8s.io/client-go/testing"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
)

func TestCalculateRequestedResources(t *testing.T) {
	node := test.BuildTestNode("node1", 1000, 1*giBToBytes)
	node.Spec.ProviderID = "gce://project1/us-central1-b/node1"

	bPod, err := GenerateBalloonPod(node,
		*resource.NewMilliQuantity(1000, resource.DecimalSI),
		*resource.NewQuantity(1024*1024, resource.DecimalSI),
		true)
	assert.NoError(t, err)

	succeededPod := test.BuildTestPod("podT", 500, 1*units.GiB)
	succeededPod.Spec.RestartPolicy = v1.RestartPolicyAlways
	succeededPod.Status = v1.PodStatus{
		Phase: v1.PodSucceeded,
	}

	failedPod := test.BuildTestPod("podF", 500, 1*units.GiB)
	failedPod.Spec.RestartPolicy = v1.RestartPolicyAlways
	failedPod.Status = v1.PodStatus{
		Phase: v1.PodFailed,
	}

	testCases := map[string]struct {
		podList           *v1.PodList
		expectedResources size.Allocatable
	}{
		"Empty pod list": {
			podList:           &v1.PodList{Items: []v1.Pod{}},
			expectedResources: size.Allocatable{},
		},
		"Sum of multiple pods": {
			podList: &v1.PodList{Items: []v1.Pod{
				*test.BuildTestPod("pod1", 500, 1*units.GiB),
				*test.BuildTestPod("pod2", 1000, 3*units.GiB),
				*test.BuildTestPod("pod3", 2250, 5*units.GiB),
			}},
			expectedResources: size.Allocatable{
				MilliCpus: 3750,
				KBytes:    9 * giBToKiB,
			},
		},
		"Sum of multiple pods - balloon pod excluded": {
			podList: &v1.PodList{Items: []v1.Pod{
				*test.BuildTestPod("pod1", 500, 1*units.GiB),
				*test.BuildTestPod("pod2", 1000, 3*units.GiB),
				*test.BuildTestPod("pod3", 2250, 5*units.GiB),
				*bPod,
			}},
			expectedResources: size.Allocatable{
				MilliCpus: 3750,
				KBytes:    9 * giBToKiB,
			},
		},
		"Sum of multiple pods - terminated pods excluded": {
			podList: &v1.PodList{Items: []v1.Pod{
				*test.BuildTestPod("pod1", 500, 1*units.GiB),
				*test.BuildTestPod("pod2", 1000, 3*units.GiB),
				*test.BuildTestPod("pod3", 2250, 5*units.GiB),
				*succeededPod,
				*failedPod,
			}},
			expectedResources: size.Allocatable{
				MilliCpus: 3750,
				KBytes:    9 * giBToKiB,
			},
		},
		"Memory is rounded up to next KByte": {
			podList: &v1.PodList{Items: []v1.Pod{
				*test.BuildTestPod("pod1", 50, 1*size.KiB),
				*test.BuildTestPod("pod2", 50, 1),
			}},
			expectedResources: size.Allocatable{
				MilliCpus: 100,
				KBytes:    2,
			},
		},
	}
	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			fakeClient := &fake.Clientset{}
			fakeClient.AddReactor("*", "pods", func(action client_testing.Action) (bool, runtime.Object, error) { return true, tc.podList, nil })
			allocatable, err := calculateRequestedResources(fakeClient, node)
			assert.NoError(t, err)
			assert.Equal(t, allocatable, tc.expectedResources)
		})
	}
}

func TestGetBalloonPodLogs(t *testing.T) {
	nodeMilliCpu := int64(10 * 1000)
	nodeMem := int64(10 * size.GiB)
	node := test.BuildTestNode("node", nodeMilliCpu, nodeMem)
	desiredAllocatable := size.Allocatable{MilliCpus: nodeMilliCpu / 2, KBytes: nodeMem / 2 / size.KiB}

	testCases := []struct {
		desc       string
		bPodStatus BalloonPodStatus
		bPodNames  []string
		bPodSizes  [][]resource.Quantity
		bPodPhase  v1.PodPhase
		want       string
	}{
		{
			desc:       "wrong number of balloon pods",
			bPodStatus: BalloonPodWrongCount,
			bPodNames:  []string{"bp1", "bp2"},
			bPodSizes: [][]resource.Quantity{
				{*resource.NewMilliQuantity(nodeMilliCpu/2, resource.BinarySI), *resource.NewQuantity(nodeMem/2, resource.BinarySI)},
				{*resource.NewMilliQuantity(nodeMilliCpu/2, resource.BinarySI), *resource.NewQuantity(nodeMem/2, resource.BinarySI)},
			},
			want: "Want single balloon pod: {cpu: \"5\", memory: \"5368709120\"}, got 2: [balloon pod: {name: \"bp1\", cpu: \"5\", memory: \"5Gi\"}., balloon pod: {name: \"bp2\", cpu: \"5\", memory: \"5Gi\"}.]",
		},
		{
			desc:       "incorrect size",
			bPodStatus: BalloonPodIncorrectSize,
			bPodNames:  []string{"bp1"},
			bPodSizes: [][]resource.Quantity{
				{*resource.NewMilliQuantity(nodeMilliCpu/4, resource.BinarySI), *resource.NewQuantity(nodeMem/4, resource.BinarySI)},
			},
			want: "Want balloon pod \"bp1\" size: {cpu: \"5\", memory: \"5368709120\"}, got: {cpu: \"2500m\", memory: \"2560Mi\"}.",
		},
		{
			desc:       "balloon pod not running",
			bPodStatus: BalloonPodNotRunning,
			bPodNames:  []string{"bp1"},
			bPodSizes: [][]resource.Quantity{
				{*resource.NewMilliQuantity(nodeMilliCpu/2, resource.BinarySI), *resource.NewQuantity(nodeMem/2, resource.BinarySI)},
			},
			bPodPhase: v1.PodFailed,
			want:      "Want balloon pod \"bp1\" status: \"Running\", got: \"Failed\".",
		},
		{
			desc:       "wrong status",
			bPodStatus: "",
			bPodNames:  []string{"bp1"},
			bPodSizes: [][]resource.Quantity{
				{*resource.NewMilliQuantity(nodeMilliCpu/2, resource.BinarySI), *resource.NewQuantity(nodeMem/2, resource.BinarySI)},
			},
			bPodPhase: v1.PodUnknown,
			want:      "Unknown balloon pod problem.",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			var bPods []*v1.Pod
			for i, bPodSize := range tc.bPodSizes {
				bPodCpu, bPodMem := bPodSize[0], bPodSize[1]
				bPod, err := GenerateBalloonPod(node, bPodCpu, bPodMem, true)
				assert.NoError(t, err)
				bPod.Name = tc.bPodNames[i]
				bPod.Status.Phase = tc.bPodPhase
				bPods = append(bPods, bPod)
			}

			logs := getBalloonPodsLog(tc.bPodStatus, bPods, node, desiredAllocatable)
			assert.Equal(t, tc.want, logs)
		})
	}
}

func TestIsNodeReady(t *testing.T) {
	testCases := []struct {
		name     string
		node     *v1.Node
		expected bool
	}{
		{
			name:     "Node has no conditions",
			node:     test.BuildTestNode("node1", 1000, 1000),
			expected: false,
		},
		{
			name: "Node is not ready",
			node: func() *v1.Node {
				node := test.BuildTestNode("node2", 1000, 1000)
				node.Status.Conditions = append(node.Status.Conditions, v1.NodeCondition{
					Type:   v1.NodeReady,
					Status: v1.ConditionFalse,
				})
				return node
			}(),
			expected: false,
		},
		{
			name: "Node has ready condition with unknown status",
			node: func() *v1.Node {
				node := test.BuildTestNode("node4", 1000, 1000)
				node.Status.Conditions = append(node.Status.Conditions, v1.NodeCondition{
					Type:   v1.NodeReady,
					Status: v1.ConditionUnknown,
				})
				return node
			}(),
			expected: false,
		},
		{
			name: "Node has ready condition with ready status",
			node: func() *v1.Node {
				node := test.BuildTestNode("node4", 1000, 1000)
				node.Status.Conditions = append(node.Status.Conditions, v1.NodeCondition{
					Type:   v1.NodeReady,
					Status: v1.ConditionTrue,
				})
				return node
			}(),
			expected: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := isNodeReady(tc.node)
			assert.Equal(t, tc.expected, result)
		})
	}
}
