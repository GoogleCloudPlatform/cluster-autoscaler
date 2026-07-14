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
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/store"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	calculator_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator/test"
	ekvms_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/test"
)

func TestRemoveBalloonPod(t *testing.T) {
	testCases := []struct {
		desc         string
		node         *v1.Node
		podsOnNode   []*v1.Pod
		expectedPods []*v1.Pod
	}{
		{
			desc: "Remove balloon pod",
			node: ekvms_test.EkNode32("ek-node", 4000, 400*size.MiB),
			podsOnNode: []*v1.Pod{
				userPod("pod1", 1000, 100*size.MiB),
				balloonPod(t, "ek-node", 3000, 300*size.MiB),
			},
			expectedPods: []*v1.Pod{
				userPod("pod1", 1000, 100*size.MiB),
			},
		},
		{
			desc: "There is no balloon pod",
			node: ekvms_test.EkNode32("ek-node", 4000, 400*size.MiB),
			podsOnNode: []*v1.Pod{
				userPod("pod1", 1000, 100*size.MiB),
			},
			expectedPods: []*v1.Pod{
				userPod("pod1", 1000, 100*size.MiB),
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			snapshot := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
			err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(tc.node, tc.podsOnNode...))
			assert.NoError(t, err)
			nodeInfos, err := snapshot.ListNodeInfos()
			assert.NoError(t, err)
			assert.Len(t, nodeInfos, 1)
			originalNodeInfo := nodeInfos[0]

			gotNodeInfo, err := removeBalloonPod(snapshot, originalNodeInfo)
			assert.NoError(t, err)
			assert.Equal(t, originalNodeInfo.Node(), gotNodeInfo.Node())
			assert.Len(t, gotNodeInfo.Pods(), len(tc.expectedPods))
			for _, podInfo := range gotNodeInfo.Pods() {
				assert.Contains(t, tc.expectedPods, podInfo.Pod)
			}
		})
	}
}

func TestRemoveBalloonPodWithError(t *testing.T) {
	snapshot := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
	nodeInfo := framework.NewTestNodeInfo(ekvms_test.EkNode32("ek-node", 4000, 400*size.MiB))
	_, err := removeBalloonPod(snapshot, nodeInfo)
	assert.Error(t, err)
}

func TestAdjustBalloonPodSize(t *testing.T) {
	machineType := "ek-standard-32"
	testEkNodeName := "ek-node"
	testCases := []struct {
		desc         string
		node         *v1.Node
		podsOnNode   []*v1.Pod
		sizeMap      map[string]size.Allocatable
		expectedPods []*v1.Pod
	}{
		{
			desc: "Balloon pod grows",
			node: ekvms_test.NewResizableNodeBuilder(testEkNodeName, 32000, 128).WithSupportedMachineType(machineType).WithReadyStatus().Build(),
			podsOnNode: []*v1.Pod{
				userPod("pod1", 1000, 1*size.GiB),
				balloonPod(t, "ek-node", 2000, 2*size.GiB),
			},
			sizeMap: map[string]size.Allocatable{
				"ek-node": {
					MilliCpus: 1000,
					KBytes:    1 * giBToKiB,
				},
			},
			expectedPods: []*v1.Pod{
				userPod("pod1", 1000, 1*size.GiB),
				balloonPod(t, "ek-node", 31000, 127*size.GiB),
			},
		},
		{
			desc: "Balloon pod shrinks",
			node: ekvms_test.NewResizableNodeBuilder(testEkNodeName, 32000, 128).WithSupportedMachineType(machineType).WithReadyStatus().Build(),
			podsOnNode: []*v1.Pod{
				userPod("pod1", 1000, 1*size.GiB),
				balloonPod(t, "ek-node", 31000, 127*size.GiB),
			},
			sizeMap: map[string]size.Allocatable{
				"ek-node": {
					MilliCpus: 4000,
					KBytes:    4 * giBToKiB,
				},
			},
			expectedPods: []*v1.Pod{
				userPod("pod1", 1000, 1*size.GiB),
				balloonPod(t, "ek-node", 28000, 124*size.GiB),
			},
		},
		{
			desc: "Balloon pod doesn't shrink below 50MiB memory",
			node: ekvms_test.NewResizableNodeBuilder(testEkNodeName, 32000, 128).WithSupportedMachineType(machineType).WithReadyStatus().Build(),
			podsOnNode: []*v1.Pod{
				userPod("pod1", 1000, 1*size.GiB),
				balloonPod(t, "ek-node", 31000, 127*size.GiB),
			},
			sizeMap: map[string]size.Allocatable{
				"ek-node": {
					MilliCpus: 32000,
					KBytes:    127*giBToKiB + 999*miBToKiB,
				},
			},
			expectedPods: []*v1.Pod{
				userPod("pod1", 1000, 1*size.GiB),
				balloonPod(t, "ek-node", 0, 50*size.MiB),
			},
		},
		{
			desc: "Balloon pod is added when missing",
			node: ekvms_test.NewResizableNodeBuilder(testEkNodeName, 32000, 128).WithSupportedMachineType(machineType).WithReadyStatus().Build(),
			podsOnNode: []*v1.Pod{
				userPod("pod1", 1000, 1*size.GiB),
			},
			sizeMap: map[string]size.Allocatable{
				"ek-node": {
					MilliCpus: 1000,
					KBytes:    1 * giBToKiB,
				},
			},
			expectedPods: []*v1.Pod{
				userPod("pod1", 1000, 1*size.GiB),
				balloonPod(t, "ek-node", 31000, 127*size.GiB),
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			snapshot := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
			err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(tc.node, tc.podsOnNode...))
			assert.NoError(t, err)
			ctx := &context.AutoscalingContext{
				ClusterSnapshot: snapshot,
			}

			err = AdjustBalloonPodsSize(ctx.ClusterSnapshot, tc.sizeMap, calculator_test.New())
			assert.NoError(t, err)

			nodeInfos, err := snapshot.ListNodeInfos()
			assert.NoError(t, err)
			assert.Len(t, nodeInfos, 1)

			assert.Len(t, nodeInfos[0].Pods(), len(tc.expectedPods))
			expectedPodSpecs := []v1.PodSpec{}
			for _, pod := range tc.expectedPods {
				expectedPodSpecs = append(expectedPodSpecs, pod.Spec)
			}
			for _, podInfo := range nodeInfos[0].Pods() {
				assert.Contains(t, expectedPodSpecs, podInfo.Pod.Spec)
			}
		})
	}
}

func TestAllLookaheadPodsRequests(t *testing.T) {
	testCases := []struct {
		desc              string
		pods              []*v1.Pod
		expectedResources size.Allocatable
	}{
		{
			desc:              "No pods",
			pods:              []*v1.Pod{},
			expectedResources: size.Allocatable{MilliCpus: 0, KBytes: 0 * giBToKiB},
		},
		{
			desc: "No lookahead pods",
			pods: []*v1.Pod{
				userPod("pod-1", 100, 100*size.MiB),
			},
			expectedResources: size.Allocatable{MilliCpus: 0, KBytes: 0 * giBToKiB},
		},
		{
			desc: "One lookahead pod",
			pods: []*v1.Pod{
				userPod("pod-1", 100, 100*size.MiB),
				lookaheadPod("lookahead-pod-1", 200, 500*size.MiB),
			},
			expectedResources: size.Allocatable{MilliCpus: 200, KBytes: 500 * miBToKiB},
		},
		{
			desc: "Multiple lookahead pods",
			pods: []*v1.Pod{
				userPod("pod-1", 100, 100*size.MiB),
				lookaheadPod("lookahead-pod-1", 200, 500*size.MiB),
				lookaheadPod("lookahead-pod-2", 100, 250*size.MiB),
			},
			expectedResources: size.Allocatable{MilliCpus: 300, KBytes: 750 * miBToKiB},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			node := ekvms_test.EkNode32("ek-node", 4000, 400*size.MiB)
			nodeInfo := framework.NewTestNodeInfo(node, tc.pods...)
			gotResources := allLookaheadPodsRequests(nodeInfo)
			assert.Equal(t, tc.expectedResources, gotResources)

		})
	}

}

func TestAllBalloonPodsRequests(t *testing.T) {
	testCases := []struct {
		desc              string
		pods              []*v1.Pod
		expectedResources size.Allocatable
	}{
		{
			desc:              "No pods",
			pods:              []*v1.Pod{},
			expectedResources: size.Allocatable{MilliCpus: 0, KBytes: 0 * giBToKiB},
		},
		{
			desc: "No balloon pods",
			pods: []*v1.Pod{
				userPod("pod-1", 100, 100*size.MiB),
			},
			expectedResources: size.Allocatable{MilliCpus: 0, KBytes: 0 * giBToKiB},
		},
		{
			desc: "One balloon pod",
			pods: []*v1.Pod{
				userPod("pod-1", 100, 100*size.MiB),
				balloonPod(t, "balloon-pod-1", 200, 500*size.MiB),
			},
			expectedResources: size.Allocatable{MilliCpus: 200, KBytes: 500 * miBToKiB},
		},
		{
			desc: "Multiple balloon pods",
			pods: []*v1.Pod{
				userPod("pod-1", 100, 100*size.MiB),
				balloonPod(t, "balloon-pod-1", 200, 500*size.MiB),
				balloonPod(t, "balloon-pod-2", 100, 250*size.MiB),
			},
			expectedResources: size.Allocatable{MilliCpus: 300, KBytes: 750 * miBToKiB},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			node := ekvms_test.EkNode32("ek-node", 4000, 400*size.MiB)
			nodeInfo := framework.NewTestNodeInfo(node, tc.pods...)
			gotResources := allBalloonPodsRequests(nodeInfo)
			assert.Equal(t, tc.expectedResources, gotResources)

		})
	}

}

func TestMatchingPodRequests(t *testing.T) {
	testCases := []struct {
		desc              string
		pods              []*v1.Pod
		isPodAcceptable   func(*v1.Pod) bool
		expectedResources size.Allocatable
	}{
		{
			desc:              "No pods",
			pods:              []*v1.Pod{},
			isPodAcceptable:   func(pod *v1.Pod) bool { return true },
			expectedResources: size.Allocatable{MilliCpus: 0, KBytes: 0},
		},
		{
			desc: "No acceptable pods",
			pods: []*v1.Pod{
				userPod("pod-1", 100, 100*size.MiB),
			},
			isPodAcceptable:   func(pod *v1.Pod) bool { return false },
			expectedResources: size.Allocatable{MilliCpus: 0, KBytes: 0},
		},
		{
			desc: "One acceptable pod",
			pods: []*v1.Pod{
				userPod("pod-1", 100, 100*size.MiB),
				userPod("pod-2", 200, 500*size.MiB),
			},
			isPodAcceptable:   func(pod *v1.Pod) bool { return pod.Name == "pod-2" },
			expectedResources: size.Allocatable{MilliCpus: 200, KBytes: 500 * miBToKiB},
		},
		{
			desc: "Multiple acceptable pods",
			pods: []*v1.Pod{
				userPod("pod-1", 100, 100*size.MiB),
				userPod("pod-2", 200, 500*size.MiB),
				userPod("pod-3", 300, 750*size.MiB),
			},
			isPodAcceptable:   func(pod *v1.Pod) bool { return pod.Name == "pod-2" || pod.Name == "pod-3" },
			expectedResources: size.Allocatable{MilliCpus: 500, KBytes: 1250 * miBToKiB},
		},
		{
			desc: "Pod with multiple containers",
			pods: []*v1.Pod{
				userPodWithMultipleContainers("pod-1", 100, 100*size.MiB, 10, 10*size.MiB),
			},
			isPodAcceptable:   func(pod *v1.Pod) bool { return true },
			expectedResources: size.Allocatable{MilliCpus: 110, KBytes: 110 * miBToKiB},
		},
		{
			desc: "Pod with init container - take max(containers, init-containers) from each pod",
			pods: []*v1.Pod{
				userPodWithInitContainer("pod-1", 100, 100*size.MiB, 10, 10*size.MiB),
				userPodWithInitContainer("pod-2", 20, 20*size.MiB, 222, 222*size.MiB),
			},
			isPodAcceptable:   func(pod *v1.Pod) bool { return true },
			expectedResources: size.Allocatable{MilliCpus: 322, KBytes: 322 * miBToKiB},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			node := ekvms_test.EkNode32("ek-node", 4000, 400*size.MiB)
			nodeInfo := framework.NewTestNodeInfo(node, tc.pods...)
			gotResources := matchingPodRequests(nodeInfo, tc.isPodAcceptable)
			assert.Equal(t, tc.expectedResources, gotResources)
		})
	}
}

func TestHasLookaheadPods(t *testing.T) {
	testCases := []struct {
		name     string
		pods     []*v1.Pod
		expected bool
	}{
		{
			name:     "no pods",
			pods:     []*v1.Pod{},
			expected: false,
		},
		{
			name: "regular pod",
			pods: []*v1.Pod{
				userPod("pod-1", 100, 100),
			},
			expected: false,
		},
		{
			name: "lookahead pod",
			pods: []*v1.Pod{
				lookaheadPod("la-pod-1", 100, 100),
			},
			expected: true,
		},

		{
			name: "mixed pods",
			pods: []*v1.Pod{
				userPod("pod-1", 100, 100),
				lookaheadPod("la-pod-1", 100, 100),
				userPod("pod-2", 100, 100),
			},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node := test.BuildTestNode("node-1", 1000, 1000)
			nodeInfo := framework.NewTestNodeInfo(node, tc.pods...)

			actual := HasLookaheadPods(nodeInfo)
			assert.Equal(t, tc.expected, actual)

		})
	}

}

func userPodWithInitContainer(name string, milliCPU, memoryBytes int64, initMilliCpu, initMemoryBytes int64) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
			UID:       types.UID(name),
		},
		Spec: v1.PodSpec{
			InitContainers: []v1.Container{
				{
					Name: "init-container",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    *resource.NewMilliQuantity(initMilliCpu, resource.DecimalSI),
							v1.ResourceMemory: *resource.NewQuantity(initMemoryBytes, resource.BinarySI),
						},
					},
				},
			},
			Containers: []v1.Container{
				{
					Name: "container",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    *resource.NewMilliQuantity(milliCPU, resource.DecimalSI),
							v1.ResourceMemory: *resource.NewQuantity(memoryBytes, resource.BinarySI),
						},
					},
				},
			},
		},
	}
}

func userPodWithMultipleContainers(name string, milliCPU, memoryBytes int64, extraMilliCpu, extraMemoryBytes int64) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
			UID:       types.UID(name),
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "container-1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    *resource.NewMilliQuantity(milliCPU, resource.DecimalSI),
							v1.ResourceMemory: *resource.NewQuantity(memoryBytes, resource.BinarySI),
						},
					},
				},
				{
					Name: "container-2",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    *resource.NewMilliQuantity(extraMilliCpu, resource.DecimalSI),
							v1.ResourceMemory: *resource.NewQuantity(extraMemoryBytes, resource.BinarySI),
						},
					},
				},
			},
		},
	}
}

func TestIsUserWorkloadPod(t *testing.T) {
	testCases := []struct {
		name     string
		pod      *v1.Pod
		expected bool
	}{
		{
			name: "user pod",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "user-pod",
					Namespace: "default",
				},
				Spec: v1.PodSpec{},
			},
			expected: true,
		},
		{
			name: "kube-system pod",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kube-system-pod",
					Namespace: metav1.NamespaceSystem,
				},
				Spec: v1.PodSpec{},
			},
			expected: false,
		},
		{
			name: "daemonset pod",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "daemonset-pod",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							Kind:       "DaemonSet",
							Controller: proto.Bool(true),
						},
					},
				},
				Spec: v1.PodSpec{},
			},
			expected: false,
		},
		{
			name: "pod with daemon-set annotation set to true",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "daemonset-annotation-pod",
					Namespace: "default",
					Annotations: map[string]string{
						"cluster-autoscaler.kubernetes.io/daemonset-pod": "true",
					},
				},
				Spec: v1.PodSpec{},
			},
			expected: false,
		},
		{
			name: "pod with daemon-set annotation set to false",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "daemonset-annotation-pod",
					Namespace: "default",
					Annotations: map[string]string{
						"cluster-autoscaler.kubernetes.io/daemonset-pod": "false",
					},
				},
				Spec: v1.PodSpec{},
			},
			expected: true,
		},
		{
			name: "static pod",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "static-pod",
					Namespace: "default",
					Annotations: map[string]string{
						"kubernetes.io/config.source": "file",
					},
				},
				Spec: v1.PodSpec{},
			},
			expected: false,
		},
		{
			name: "mirror pod",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-pod",
					Namespace: "default",
					Annotations: map[string]string{
						"kubernetes.io/config.mirror": "mirror",
					},
				},
				Spec: v1.PodSpec{},
			},
			expected: false,
		},
		{
			name: "mirror pod with static pod annotation",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-pod",
					Namespace: "default",
					Annotations: map[string]string{
						"kubernetes.io/config.mirror": "mirror",
						"kubernetes.io/config.source": "file",
					},
				},
				Spec: v1.PodSpec{},
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := IsUserWorkloadPod(tc.pod)
			if result != tc.expected {
				t.Errorf("IsUserWorkloadPod(%v) = %v, expected %v", tc.pod.Name, result, tc.expected)
			}
		})
	}
}
