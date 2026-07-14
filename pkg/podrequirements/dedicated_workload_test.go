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

package podrequirements

import (
	"testing"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"

	"github.com/stretchr/testify/assert"
)

func TestExtractWorkloadID(t *testing.T) {
	testCases := []struct {
		name      string
		taints    []apiv1.Taint
		labels    map[string]string
		resources map[apiv1.ResourceName]resource.Quantity
		expected  string
	}{
		{
			name:     "no taints or labels",
			expected: "",
		},
		{
			name: "no labels",
			taints: []apiv1.Taint{
				{Key: "workload", Value: "workload1", Effect: apiv1.TaintEffectNoSchedule},
			},
			expected: "",
		},
		{
			name:     "no taints",
			labels:   map[string]string{"key": "value"},
			expected: "",
		},
		{
			name: "label matching NoSchedule taint",
			taints: []apiv1.Taint{
				{Key: "key", Value: "value", Effect: apiv1.TaintEffectNoSchedule},
			},
			labels:   map[string]string{"key": "value"},
			expected: "NoSchedule:key:value",
		},
		{
			name: "label matching NoExecute taint",
			taints: []apiv1.Taint{
				{Key: "key", Value: "value", Effect: apiv1.TaintEffectNoExecute},
			},
			labels:   map[string]string{"key": "value"},
			expected: "NoExecute:key:value",
		},
		{
			name: "label not matching taint value",
			taints: []apiv1.Taint{
				{Key: "key", Value: "value", Effect: apiv1.TaintEffectNoSchedule},
			},
			labels:   map[string]string{"key": "value2"},
			expected: "",
		},
		{
			name: "label not matching taint key",
			taints: []apiv1.Taint{
				{Key: "key", Value: "value", Effect: apiv1.TaintEffectNoSchedule},
			},
			labels:   map[string]string{"key2": "value"},
			expected: "",
		},
		{
			name: "one labels and two taints",
			taints: []apiv1.Taint{
				{Key: "key1", Value: "value1", Effect: apiv1.TaintEffectNoSchedule},
				{Key: "key2", Value: "value2", Effect: apiv1.TaintEffectNoSchedule},
			},
			labels:   map[string]string{"key1": "value1"},
			expected: "NoSchedule:key1:value1",
		},
		{
			name: "two labels and two taints",
			taints: []apiv1.Taint{
				{Key: "key1", Value: "value1", Effect: apiv1.TaintEffectNoSchedule},
				{Key: "key2", Value: "value2", Effect: apiv1.TaintEffectNoSchedule},
			},
			labels:   map[string]string{"key1": "value1", "key2": "value2"},
			expected: "NoSchedule:key1:value1,NoSchedule:key2:value2",
		},
		{
			name: "two labels and two taints reversed",
			taints: []apiv1.Taint{
				{Key: "key2", Value: "value2", Effect: apiv1.TaintEffectNoSchedule},
				{Key: "key1", Value: "value1", Effect: apiv1.TaintEffectNoSchedule},
			},
			labels:   map[string]string{"key1": "value1", "key2": "value2"},
			expected: "NoSchedule:key1:value1,NoSchedule:key2:value2",
		},
		{
			name: "disregarded system label",
			taints: []apiv1.Taint{
				{Key: "cloud.google.com/disregard-label", Value: "value1", Effect: apiv1.TaintEffectNoSchedule},
			},
			labels:   map[string]string{"cloud.google.com/disregard-label": "value1"},
			expected: "",
		},
		{
			name: "gpu taint and resource",
			taints: []apiv1.Taint{
				{Key: gpu.ResourceNvidiaGPU, Value: "present", Effect: apiv1.TaintEffectNoSchedule},
			},
			resources: map[apiv1.ResourceName]resource.Quantity{gpu.ResourceNvidiaGPU: *resource.NewQuantity(1, resource.DecimalSI)},
			expected:  "NoSchedule:nvidia.com/gpu:present",
		},
		{
			name: "gpu taint but not resource",
			taints: []apiv1.Taint{
				{Key: gpu.ResourceNvidiaGPU, Value: "present", Effect: apiv1.TaintEffectNoSchedule},
			},
			expected: "",
		},
		{
			name:      "gpu resource but not taint",
			resources: map[apiv1.ResourceName]resource.Quantity{gpu.ResourceNvidiaGPU: *resource.NewQuantity(1, resource.DecimalSI)},
			expected:  "",
		},
		{
			name: "compute class is a type of workload separation",
			taints: []apiv1.Taint{
				{Key: gkelabels.ComputeClassLabel, Value: "my-compute-class", Effect: apiv1.TaintEffectNoSchedule},
			},
			labels:   map[string]string{gkelabels.ComputeClassLabel: "my-compute-class"},
			expected: "NoSchedule:cloud.google.com/compute-class:my-compute-class",
		},
		{
			name: "autopilot_managed_node",
			taints: []apiv1.Taint{
				{Key: gkelabels.ManagedNodeLabel, Value: "true", Effect: apiv1.TaintEffectNoSchedule},
			},
			labels:   map[string]string{gkelabels.ManagedNodeLabel: "true"},
			expected: "NoSchedule:cloud.google.com/autopilot-managed-node:true",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node := BuildTestNode("test", 1000, 1000)
			node.Spec.Taints = tc.taints
			node.Labels = tc.labels
			for key, value := range tc.resources {
				node.Status.Capacity[key] = value
				node.Status.Allocatable[key] = value
			}
			assert.Equal(t, tc.expected, ExtractWorkloadID(node))
		})
	}
}

func TestWorkloadIDToTolerations(t *testing.T) {
	testCases := []struct {
		name    string
		id      string
		want    []apiv1.Toleration
		wantErr bool
	}{
		{
			name: "empty string",
			id:   "",
			want: []apiv1.Toleration{},
		},
		{
			name: "invalid string",
			id:   "invalid",
			want: []apiv1.Toleration{},
		},
		{
			name: "valid workload id",
			id:   "NoSchedule:key2:value2",
			want: []apiv1.Toleration{
				{
					Key:      "key2",
					Operator: apiv1.TolerationOpEqual,
					Value:    "value2",
					Effect:   apiv1.TaintEffectNoSchedule,
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := WorkloadIDToTolerations(tc.id)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestTaintsToWorkloadID(t *testing.T) {
	testCases := []struct {
		name   string
		taints []apiv1.Taint
		want   string
	}{
		{
			name:   "empty taints",
			taints: []apiv1.Taint{},
			want:   "",
		},
		{
			name: "single taint",
			taints: []apiv1.Taint{
				{
					Key:    "key",
					Value:  "value",
					Effect: apiv1.TaintEffectNoSchedule,
				},
			},
			want: "NoSchedule:key:value",
		},
		{
			name: "multiple taints",
			taints: []apiv1.Taint{
				{
					Key: "key", Value: "value", Effect: apiv1.TaintEffectNoSchedule,
				},
				{
					Key: gkelabels.ComputeClassLabel, Value: "compute-class", Effect: apiv1.TaintEffectNoSchedule,
				},
			},
			want: "NoSchedule:cloud.google.com/compute-class:compute-class,NoSchedule:key:value",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := taintsToWorkloadID(tc.taints); got != tc.want {
				t.Errorf("TaintsToWorkloadID() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestExtractWorkloadIDTaints(t *testing.T) {
	tests := []struct {
		name   string
		taints []apiv1.Taint
		labels map[string]string
		want   []apiv1.Taint
	}{
		{
			name: "empty node",
			want: []apiv1.Taint{},
		},
		{
			name: "node with taints",
			taints: []apiv1.Taint{
				{
					Key:    "key1",
					Value:  "value1",
					Effect: apiv1.TaintEffectNoSchedule,
				},
			},
			want: []apiv1.Taint{},
		},
		{
			name: "node with workload separation",
			labels: map[string]string{
				"key1": "value1",
			},
			taints: []apiv1.Taint{
				{
					Key:    "key1",
					Value:  "value1",
					Effect: apiv1.TaintEffectNoSchedule,
				},
			},
			want: []apiv1.Taint{
				{
					Key:    "key1",
					Value:  "value1",
					Effect: apiv1.TaintEffectNoSchedule,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := BuildTestNode("test", 1000, 1000)
			node.Spec.Taints = tt.taints
			node.Labels = tt.labels

			assert.Equal(t, tt.want, ExtractWorkloadIDTaints(node))
		})
	}
}
