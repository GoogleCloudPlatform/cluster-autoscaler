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

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestIsSystemPod(t *testing.T) {
	for _, tc := range []struct {
		name      string
		namespace string
		want      bool
	}{
		{name: "kube-system pod", namespace: metav1.NamespaceSystem, want: true},
		{name: "default namespace pod", namespace: metav1.NamespaceDefault, want: false},
		{name: "custom namespace pod", namespace: "custom", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: tc.namespace}}
			if got := IsSystemPod(pod); got != tc.want {
				t.Errorf("IsSystemPod() = %v, want %v", got, tc.want)
			}
		})
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

func TestHasRunningCustomerPods(t *testing.T) {
	const nodeName = "node-1"
	boundTo := func(pod *v1.Pod, nodeName string) *v1.Pod {
		pod.Spec.NodeName = nodeName
		return pod
	}
	userPod := func(name string) *v1.Pod {
		return test.BuildTestPod(name, 100, 100)
	}
	systemPod := func(name string) *v1.Pod {
		return test.BuildTestPod(name, 100, 100, func(p *v1.Pod) { p.Namespace = metav1.NamespaceSystem })
	}
	lookaheadPod := func(name string) *v1.Pod {
		return lookaheadbuffer.BuildTestLookaheadPod("", 100, 100, machinetypes.EK.Name(), lookaheadbuffer.WithName(name))
	}
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
			name: "lookahead pod only",
			pods: []*v1.Pod{
				lookaheadPod("la-pod-1"),
			},
			expected: false,
		},
		{
			name: "lookahead pod bound to the node",
			pods: []*v1.Pod{
				boundTo(lookaheadPod("la-pod-1"), nodeName),
			},
			expected: false,
		},
		{
			name: "kube-system pod bound to the node",
			pods: []*v1.Pod{
				boundTo(systemPod("sys-pod-1"), nodeName),
			},
			expected: false,
		},
		{
			name: "user pod bound to the node",
			pods: []*v1.Pod{
				boundTo(userPod("pod-1"), nodeName),
			},
			expected: true,
		},
		{
			name: "user pod placed only by simulation",
			pods: []*v1.Pod{
				userPod("pod-1"),
			},
			expected: false,
		},
		{
			name: "user pod bound to a different node",
			pods: []*v1.Pod{
				boundTo(userPod("pod-1"), "other-node"),
			},
			expected: false,
		},
		{
			name: "lookahead pod and simulated user pod",
			pods: []*v1.Pod{
				lookaheadPod("la-pod-1"),
				userPod("pod-1"),
			},
			expected: false,
		},
		{
			name: "lookahead pod and bound user pod",
			pods: []*v1.Pod{
				lookaheadPod("la-pod-1"),
				boundTo(userPod("pod-1"), nodeName),
			},
			expected: true,
		},
		{
			name: "simulated and bound user pods",
			pods: []*v1.Pod{
				userPod("pod-1"),
				boundTo(userPod("pod-2"), nodeName),
			},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node := test.BuildTestNode(nodeName, 1000, 1000)
			nodeInfo := framework.NewTestNodeInfo(node, tc.pods...)

			actual := HasRunningCustomerPods(nodeInfo)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestHasRunningCustomerPodsWithoutNode(t *testing.T) {
	assert.False(t, HasRunningCustomerPods(nil), "nil node info")
	assert.False(t, HasRunningCustomerPods(framework.NewNodeInfo(nil, nil)), "node info without node")
}
