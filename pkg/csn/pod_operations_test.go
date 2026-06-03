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

package csn

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	capacitybufferpodlister "k8s.io/autoscaler/cluster-autoscaler/processors/capacitybuffer"
	"k8s.io/autoscaler/cluster-autoscaler/utils/pod"

	"github.com/stretchr/testify/assert"
)

func TestMakePodCSN(t *testing.T) {
	tests := []struct {
		name     string
		pod      *apiv1.Pod
		bufferId string
	}{
		{
			name:     "nil annotations and node selector",
			pod:      &apiv1.Pod{},
			bufferId: "ns/buffer",
		},
		{
			name: "existing annotations and node selector",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"existing-annotation": "value",
					},
				},
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{
						"existing-node-selector": "value",
					},
					Tolerations: []apiv1.Toleration{
						{
							Key:    "existing-toleration-key",
							Value:  "existing-toleration-value",
							Effect: apiv1.TaintEffectNoSchedule,
						},
					},
				},
			},
			bufferId: "ns/buffer-2",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			MakePodCSN(test.pod, test.bufferId)

			expectedBufferId := strings.ReplaceAll(test.bufferId, "/", "_")

			assert.NotNil(t, test.pod.Spec.NodeSelector)
			assert.Equal(t, SoftWorkloadSeparationValue, test.pod.Spec.NodeSelector[SoftWorkloadSeparationKey])
			assert.Equal(t, expectedBufferId, test.pod.Spec.NodeSelector[BufferAssignmentKey])

			assert.NotNil(t, test.pod.Annotations)
			assert.Equal(t, capacitybufferpodlister.CapacityBufferFakePodAnnotationValue, test.pod.Annotations[capacitybufferpodlister.CapacityBufferFakePodAnnotationKey])
			assert.Equal(t, CSNPodAnnotationValue, test.pod.Annotations[CSNPodAnnotationKey])
			assert.Equal(t, expectedBufferId, test.pod.Annotations[BufferAssignmentKey])

			assert.Contains(t, test.pod.Spec.Tolerations, apiv1.Toleration{
				Key:    SoftWorkloadSeparationKey,
				Value:  SoftWorkloadSeparationValue,
				Effect: apiv1.TaintEffectPreferNoSchedule,
			})
			assert.Contains(t, test.pod.Spec.Tolerations, apiv1.Toleration{
				Key:    BufferAssignmentKey,
				Value:  expectedBufferId,
				Effect: apiv1.TaintEffectNoSchedule,
			})
			assert.Contains(t, test.pod.Spec.Tolerations, apiv1.Toleration{
				Key:    SuspendedTaintKey,
				Value:  SuspendedTaintValue,
				Effect: apiv1.TaintEffectNoSchedule,
			})
			assert.Equal(t, test.pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms, []apiv1.NodeSelectorTerm{
				{
					MatchExpressions: []apiv1.NodeSelectorRequirement{
						{
							Key:      memoryScalingLevelLabel,
							Operator: apiv1.NodeSelectorOpLt,
							Values:   []string{"209"},
						},
					},
				},
				{
					MatchExpressions: []apiv1.NodeSelectorRequirement{
						{
							Key:      memoryScalingLevelLabel,
							Operator: apiv1.NodeSelectorOpDoesNotExist,
						},
					},
				},
			})
			assert.True(t, IsCSNPod(test.pod))
		})
	}
}

func TestIsCSNPod(t *testing.T) {
	tests := []struct {
		name string
		pod  *apiv1.Pod
		want bool
	}{
		{
			name: "CSN pod - csn annotation",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						CSNPodAnnotationKey: CSNPodAnnotationValue,
					},
				},
			},
			want: true,
		},
		{
			name: "CSN pod - csn annotation and podType annotation",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						CSNPodAnnotationKey: CSNPodAnnotationValue,
						capacitybufferpodlister.CapacityBufferFakePodAnnotationKey: capacitybufferpodlister.CapacityBufferFakePodAnnotationValue,
					},
				},
			},
			want: true,
		},
		{
			name: "not a CSN pod - podType annotation only",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						capacitybufferpodlister.CapacityBufferFakePodAnnotationKey: capacitybufferpodlister.CapacityBufferFakePodAnnotationValue,
					},
				},
			},
			want: false,
		},
		{
			name: "not a CSN pod - wrong annotation value",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						CSNPodAnnotationKey: "not-a-csn-pod",
					},
				},
			},
			want: false,
		},
		{
			name: "not a CSN pod - different annotation key",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"some-other-annotation": CSNPodAnnotationValue,
					},
				},
			},
			want: false,
		},
		{
			name: "not a CSN pod - no annotations",
			pod:  &apiv1.Pod{},
			want: false,
		},
		{
			name: "nil pod",
			pod:  nil,
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsCSNPod(tt.pod))
		})
	}
}

func TestGetBufferIdFromPod(t *testing.T) {
	tests := []struct {
		name string
		pod  *apiv1.Pod
		want string
	}{
		{
			name: "pod with buffer assignment",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						BufferAssignmentKey: "ns_buffer",
					},
				},
			},
			want: "ns_buffer",
		},
		{
			name: "pod without buffer assignment",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"other-key": "other-value",
					},
				},
			},
			want: "",
		},
		{
			name: "pod with nil node annotations",
			pod:  &apiv1.Pod{},
			want: "",
		},
		{
			name: "nil pod",
			pod:  nil,
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, GetBufferIdFromPod(tt.pod))
		})
	}
}

func TestIsPodBlockingSuspension(t *testing.T) {
	tests := []struct {
		name string
		pod  *apiv1.Pod
		want bool
	}{
		{
			name: "daemon set pod",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{
						{
							Kind:       "DaemonSet",
							Controller: proto.Bool(true),
						},
					},
				},
			},
			want: false,
		},
		{
			name: "CA-marked daemon set pod",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						pod.DaemonSetPodAnnotationKey: "true",
					},
				},
			},
			want: false,
		},
		{
			name: "mirror pod",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"kubernetes.io/config.mirror": "true",
					},
				},
			},
			want: false,
		},
		{
			name: "static pod",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"kubernetes.io/config.source": "file",
					},
				},
			},
			want: false,
		},
		{
			name: "pod with terminal state succeeded",
			pod: &apiv1.Pod{
				Status: apiv1.PodStatus{
					Phase: apiv1.PodSucceeded,
				},
			},
			want: false,
		},
		{
			name: "pod with terminal state failed",
			pod: &apiv1.Pod{
				Status: apiv1.PodStatus{
					Phase: apiv1.PodFailed,
				},
			},
			want: false,
		},
		{
			name: "regular pod",
			pod:  &apiv1.Pod{},
			want: true,
		},
		{
			name: "pod with state running",
			pod: &apiv1.Pod{
				Status: apiv1.PodStatus{
					Phase: apiv1.PodRunning,
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPodBlockingSuspension(tt.pod); got != tt.want {
				t.Errorf("IsPodBlockingSuspension() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRemoveBufferAssignmentWorkloadSeparation(t *testing.T) {
	tests := []struct {
		name                 string
		pod                  *apiv1.Pod
		expectedTolerations  []apiv1.Toleration
		expectedNodeSelector map[string]string
		expectedAnnotations  map[string]string
	}{
		{
			name: "nil pod",
			pod:  nil,
		},
		{
			name: "pod with only buffer assignment",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						BufferAssignmentKey: "ns/buffer",
					},
				},
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{
						BufferAssignmentKey: "ns/buffer",
					},
					Tolerations: []apiv1.Toleration{
						{
							Key:    BufferAssignmentKey,
							Value:  "ns/buffer",
							Effect: apiv1.TaintEffectNoSchedule,
						},
					},
				},
			},
			expectedTolerations:  nil,
			expectedNodeSelector: map[string]string{},
			expectedAnnotations: map[string]string{
				BufferAssignmentKey: "ns/buffer",
			},
		},
		{
			name: "pod without buffer assignment",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"other-annotation": "value",
					},
				},
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{
						"other-selector": "value",
					},
					Tolerations: []apiv1.Toleration{
						{
							Key:    "other-toleration",
							Value:  "value",
							Effect: apiv1.TaintEffectNoSchedule,
						},
					},
				},
			},
			expectedTolerations: []apiv1.Toleration{
				{
					Key:    "other-toleration",
					Value:  "value",
					Effect: apiv1.TaintEffectNoSchedule,
				},
			},
			expectedNodeSelector: map[string]string{
				"other-selector": "value",
			},
			expectedAnnotations: map[string]string{
				"other-annotation": "value",
			},
		},
		{
			name: "pod with buffer assignment and unrelated fields",
			pod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						BufferAssignmentKey: "ns/buffer",
						"other-annotation":  "value",
					},
				},
				Spec: apiv1.PodSpec{
					NodeSelector: map[string]string{
						BufferAssignmentKey: "ns/buffer",
						"other-selector":    "value",
					},
					Tolerations: []apiv1.Toleration{
						{
							Key:    BufferAssignmentKey,
							Value:  "ns/buffer",
							Effect: apiv1.TaintEffectNoSchedule,
						},
						{
							Key:    "other-toleration",
							Value:  "value",
							Effect: apiv1.TaintEffectNoSchedule,
						},
					},
				},
			},
			expectedTolerations: []apiv1.Toleration{
				{
					Key:    "other-toleration",
					Value:  "value",
					Effect: apiv1.TaintEffectNoSchedule,
				},
			},
			expectedNodeSelector: map[string]string{
				"other-selector": "value",
			},
			expectedAnnotations: map[string]string{
				"other-annotation":  "value",
				BufferAssignmentKey: "ns/buffer",
			},
		},
		{
			name: "pod with nil fields",
			pod: &apiv1.Pod{
				Spec: apiv1.PodSpec{
					NodeSelector: nil,
					Tolerations:  nil,
				},
			},
			expectedTolerations:  nil,
			expectedNodeSelector: nil,
			expectedAnnotations:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			RemoveBufferAssignmentWorkloadSeparation(test.pod)

			if test.pod == nil {
				return
			}

			assert.Equal(t, test.expectedNodeSelector, test.pod.Spec.NodeSelector)
			assert.Equal(t, test.expectedTolerations, test.pod.Spec.Tolerations)
			assert.Equal(t, test.expectedAnnotations, test.pod.Annotations)
		})
	}
}
