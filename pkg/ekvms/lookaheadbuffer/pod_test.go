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

package lookaheadbuffer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/fake"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/utils/ptr"
)

func TestGenerateLookaheadPods(t *testing.T) {
	testCases := []struct {
		name         string
		number       int
		cpu          resource.Quantity
		memory       resource.Quantity
		workloadID   string
		expectedPods []apiv1.Pod
	}{
		{
			name:   "multiple_pods",
			number: 3,
			cpu:    resource.MustParse("100m"),
			memory: resource.MustParse("128Mi"),
			expectedPods: []apiv1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "lookahead-virtual-pod-default-0",
						UID:       types.UID("lookahead-virtual-pod-default-0"),
						Namespace: "kube-system",
						Labels: map[string]string{
							lookaheadPodLabel: "true",
						},
						Annotations: map[string]string{
							fake.FakePodAnnotationKey: fake.FakePodAnnotationValue,
						},
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "v1",
								Kind:       "LookaheadBuffer",
								Name:       "lookahead-buffer-default",
								UID:        types.UID("lookahead-vuid-default"),
								Controller: ptr.To(true),
							},
						},
					},

					Spec: apiv1.PodSpec{
						Containers: []apiv1.Container{
							{
								Resources: apiv1.ResourceRequirements{
									Requests: apiv1.ResourceList{
										apiv1.ResourceCPU:    resource.MustParse("100m"),
										apiv1.ResourceMemory: resource.MustParse("128Mi"),
									},
								},
							},
						},
						NodeSelector: map[string]string{
							labels.MachineFamilyLabel: "ek",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "lookahead-virtual-pod-default-1",
						UID:       types.UID("lookahead-virtual-pod-default-1"),
						Namespace: "kube-system",
						Labels: map[string]string{
							lookaheadPodLabel: "true",
						},
						Annotations: map[string]string{
							fake.FakePodAnnotationKey: fake.FakePodAnnotationValue,
						},
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "v1",
								Kind:       "LookaheadBuffer",
								Name:       "lookahead-buffer-default",
								UID:        types.UID("lookahead-vuid-default"),
								Controller: ptr.To(true),
							},
						},
					},
					Spec: apiv1.PodSpec{
						Containers: []apiv1.Container{
							{
								Resources: apiv1.ResourceRequirements{
									Requests: apiv1.ResourceList{
										apiv1.ResourceCPU:    resource.MustParse("100m"),
										apiv1.ResourceMemory: resource.MustParse("128Mi"),
									},
								},
							},
						},
						NodeSelector: map[string]string{
							labels.MachineFamilyLabel: "ek",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "lookahead-virtual-pod-default-2",
						UID:       types.UID("lookahead-virtual-pod-default-2"),
						Namespace: "kube-system",
						Labels: map[string]string{
							lookaheadPodLabel: "true",
						},
						Annotations: map[string]string{
							fake.FakePodAnnotationKey: fake.FakePodAnnotationValue,
						},
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "v1",
								Kind:       "LookaheadBuffer",
								Name:       "lookahead-buffer-default",
								UID:        types.UID("lookahead-vuid-default"),
								Controller: ptr.To(true),
							},
						},
					},
					Spec: apiv1.PodSpec{
						Containers: []apiv1.Container{
							{
								Resources: apiv1.ResourceRequirements{
									Requests: apiv1.ResourceList{
										apiv1.ResourceCPU:    resource.MustParse("100m"),
										apiv1.ResourceMemory: resource.MustParse("128Mi"),
									},
								},
							},
						},
						NodeSelector: map[string]string{
							labels.MachineFamilyLabel: "ek",
						},
					},
				},
			},
		},
		{
			name:       "pod belongs to autopilot compute class",
			number:     1,
			cpu:        resource.MustParse("100m"),
			memory:     resource.MustParse("128Mi"),
			workloadID: "NoSchedule:cloud.google.com/compute-class:autopilot",
			expectedPods: []apiv1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "lookahead-virtual-pod-77bb52f2-0",
						UID:       types.UID("lookahead-virtual-pod-77bb52f2-0"),
						Namespace: "kube-system",
						Labels: map[string]string{
							lookaheadPodLabel: "true",
						},
						Annotations: map[string]string{
							fake.FakePodAnnotationKey: fake.FakePodAnnotationValue,
						},
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "v1",
								Kind:       "LookaheadBuffer",
								Name:       "lookahead-buffer-77bb52f2",
								UID:        types.UID("lookahead-vuid-77bb52f2"),
								Controller: ptr.To(true),
							},
						},
					},
					Spec: apiv1.PodSpec{
						Containers: []apiv1.Container{
							{
								Resources: apiv1.ResourceRequirements{
									Requests: apiv1.ResourceList{
										apiv1.ResourceCPU:    resource.MustParse("100m"),
										apiv1.ResourceMemory: resource.MustParse("128Mi"),
									},
								},
							},
						},
						NodeSelector: map[string]string{
							labels.MachineFamilyLabel: "ek",
							labels.ComputeClassLabel:  "autopilot",
						},
						Tolerations: []apiv1.Toleration{
							{
								Key:      labels.ComputeClassLabel,
								Operator: apiv1.TolerationOpEqual,
								Value:    "autopilot",
								Effect:   apiv1.TaintEffectNoSchedule,
							},
						},
					},
				},
			},
		},
		{
			name:         "zero_pods",
			number:       0,
			cpu:          resource.MustParse("100m"),
			memory:       resource.MustParse("128Mi"),
			expectedPods: []apiv1.Pod{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pods := GenerateLookaheadPods(tc.number, tc.cpu, tc.memory, tc.workloadID)

			actualPods := make([]apiv1.Pod, len(pods))
			for i, pod := range pods {
				actualPods[i] = *pod
			}

			assert.Equal(t, tc.expectedPods, actualPods)
		})
	}
}

func TestIsLookaheadPod(t *testing.T) {
	testCases := []struct {
		name     string
		pod      *apiv1.Pod
		expected bool
	}{
		{
			name: "lookahead_pod",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						lookaheadPodLabel: "true",
					},
				},
			},
			expected: true,
		},
		{
			name: "non_lookahead_pod_missing_label",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{},
			},
			expected: false,
		},
		{
			name: "non_lookahead_pod_wrong_label_value",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						lookaheadPodLabel: "false",
					},
				},
			},
			expected: false,
		},
		{
			name:     "nil_pod",
			pod:      nil,
			expected: false,
		},
		{
			name: "non_lookahead_pod_nil_labels",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: nil,
				},
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := IsLookaheadPod(tc.pod)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestAllLookaheadPodsRequests(t *testing.T) {
	testCases := []struct {
		name                 string
		pods                 []*apiv1.Pod
		expectedResourceList apiv1.ResourceList
	}{
		{
			name:                 "no_pods_in_list",
			pods:                 []*apiv1.Pod{},
			expectedResourceList: apiv1.ResourceList{},
		},
		{
			name: "no_lookahead_pods_in_list",
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 200, 2, func(p *apiv1.Pod) { p.Namespace = "kube-system" }),
				test.BuildTestPod("p2", 1000, 4),
			},
			expectedResourceList: apiv1.ResourceList{},
		},
		{
			name: "lookahead_pods_in_list",
			pods: []*apiv1.Pod{
				test.BuildTestPod("p1", 200, 2, func(p *apiv1.Pod) { p.Namespace = "kube-system" }),
				BuildTestLookaheadPod("", 6000, 24, WithPosition(0)),
				test.BuildTestPod("p2", 1000, 4),
				BuildTestLookaheadPod("", 5400, 23, WithPosition(1)),
			},
			expectedResourceList: apiv1.ResourceList{
				apiv1.ResourceCPU:    resource.MustParse("11400m"),
				apiv1.ResourceMemory: resource.MustParse("47"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nodeInfo := framework.NewTestNodeInfo(&apiv1.Node{})
			for _, pod := range tc.pods {
				nodeInfo.AddPod(framework.NewPodInfo(pod, nil))
			}
			actualResourceList := AllLookaheadPodsRequests(nodeInfo)
			assert.Equal(t, tc.expectedResourceList.Cpu().MilliValue(), actualResourceList.Cpu().MilliValue())
			assert.Equal(t, tc.expectedResourceList.Memory().Value(), actualResourceList.Memory().Value())
		})
	}
}

func TestHashWorkloadID(t *testing.T) {
	tests := []struct {
		name string
		id   string
		want string
	}{
		{
			name: "default workload ID",
			id:   "",
			want: "default",
		},
		{
			name: "workload separation",
			id:   "NoSchedule:key:value",
			want: fnvHash32("NoSchedule:key:value"),
		},
		{
			name: "Compute class based workload separation",
			id:   "NoSchedule:cloud.google.com/compute-class:autopilot",
			want: fnvHash32("NoSchedule:cloud.google.com/compute-class:autopilot"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hashWorkloadID(tt.id); got != tt.want {
				t.Errorf("hashWorkloadID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLookaheadPodIsFake(t *testing.T) {
	pods := GenerateLookaheadPods(1, resource.MustParse("100m"), resource.MustParse("128Mi"), "")
	for _, pod := range pods {
		assert.True(t, fake.IsFake(pod))
	}
}

func TestWorkloadSeparationInfo(t *testing.T) {
	testCases := []struct {
		name     string
		pod      *apiv1.Pod
		expected []string
	}{
		{
			name:     "no tolerations",
			pod:      test.BuildTestPod("p1", 100, 100),
			expected: []string{},
		},
		{
			name: "toleration but no selector",
			pod: test.BuildTestPod("p1", 100, 100, func(p *apiv1.Pod) {
				p.Spec.Tolerations = []apiv1.Toleration{
					{Key: "key1", Operator: apiv1.TolerationOpExists},
				}
			}),
			expected: []string{},
		},
		{
			name: "selector but no toleration",
			pod: test.BuildTestPod("p1", 100, 100, func(p *apiv1.Pod) {
				p.Spec.NodeSelector = map[string]string{"key1": "val1"}
			}),
			expected: []string{},
		},
		{
			name: "matching toleration and selector",
			pod: test.BuildTestPod("p1", 100, 100, func(p *apiv1.Pod) {
				p.Spec.Tolerations = []apiv1.Toleration{
					{Key: "key1", Operator: apiv1.TolerationOpEqual, Value: "val1"},
				}
				p.Spec.NodeSelector = map[string]string{"key1": "val1"}
			}),
			expected: []string{"key1=val1"},
		},
		{
			name: "matching toleration (Exists) and selector",
			pod: test.BuildTestPod("p1", 100, 100, func(p *apiv1.Pod) {
				p.Spec.Tolerations = []apiv1.Toleration{
					{Key: "key1", Operator: apiv1.TolerationOpExists},
				}
				p.Spec.NodeSelector = map[string]string{"key1": "any-value"}
			}),
			expected: []string{"key1="},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := LookaheadWorkloadSeparationInfo(tc.pod)
			assert.ElementsMatch(t, tc.expected, actual)
		})
	}
}
