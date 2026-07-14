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

package extendeddurationpods

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	testCloudProvider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/annotations"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

func TestMetricsGeneration(t *testing.T) {

	testCases := map[string]struct {
		nodeInfo       func() *framework.NodeInfo
		expectedTuples []metricTuple
	}{
		"utilization of cpu and memory is 47%": {
			nodeInfo: func() *framework.NodeInfo {
				n1 := test.BuildTestNode("node1", 1000, 1000)
				n1.Labels = map[string]string{
					labels.ExtendedDurationPodsLabel: "1",
					v1.LabelInstanceTypeStable:       "machine-1",
				}
				pod := test.BuildTestPod("p1", 470, 470)
				nn := framework.NewTestNodeInfo(n1, pod)
				return nn
			},
			expectedTuples: []metricTuple{
				{
					nodeType:     edp,
					resourceName: v1.ResourceCPU,
					machineType:  "machine-1",
					utilBucket:   utilization45To50,
				},
				{
					nodeType:     edp,
					resourceName: v1.ResourceMemory,
					machineType:  "machine-1",
					utilBucket:   utilization45To50,
				},
			},
		},
		"utilization of cpu and memory is 47%, but node is not edp": {
			nodeInfo: func() *framework.NodeInfo {
				n1 := test.BuildTestNode("node1", 1000, 1000)
				n1.Labels = map[string]string{
					v1.LabelInstanceTypeStable: "machine-1",
				}
				pod := test.BuildTestPod("p1", 470, 470)
				nn := framework.NewTestNodeInfo(n1, pod)
				return nn
			},
			expectedTuples: []metricTuple{},
		},
		"utilization of cpu and memory is 47%, but node is not real": {
			nodeInfo: func() *framework.NodeInfo {
				n1 := test.BuildTestNode("node1", 1000, 1000)
				n1.Labels = map[string]string{
					labels.ExtendedDurationPodsLabel: "1",
					v1.LabelInstanceTypeStable:       "machine-1",
				}
				n1.Annotations = map[string]string{
					annotations.NodeUpcomingAnnotation: "",
				}
				pod := test.BuildTestPod("p1", 470, 470)
				nn := framework.NewTestNodeInfo(n1, pod)
				return nn
			},
			expectedTuples: []metricTuple{},
		},
		"utilization of cpu and memory is 47%, but machine is unknown": {
			nodeInfo: func() *framework.NodeInfo {
				n1 := test.BuildTestNode("node1", 1000, 1000)
				n1.Labels = map[string]string{
					labels.ExtendedDurationPodsLabel: "1",
				}
				pod := test.BuildTestPod("p1", 470, 470)
				nn := framework.NewTestNodeInfo(n1, pod)
				return nn
			},
			expectedTuples: []metricTuple{
				{
					nodeType:     edp,
					resourceName: v1.ResourceCPU,
					utilBucket:   utilization45To50,
				},
				{
					nodeType:     edp,
					resourceName: v1.ResourceMemory,
					utilBucket:   utilization45To50,
				},
			},
		},
		"utilization of cpu is 47%, but no allocatable memory": {
			nodeInfo: func() *framework.NodeInfo {
				n1 := test.BuildTestNode("node1", 1000, -1)
				n1.Labels = map[string]string{
					labels.ExtendedDurationPodsLabel: "1",
					v1.LabelInstanceTypeStable:       "machine-1",
				}
				pod := test.BuildTestPod("p1", 470, -1)
				nn := framework.NewTestNodeInfo(n1, pod)
				return nn
			},
			expectedTuples: []metricTuple{
				{
					nodeType:     edp,
					resourceName: v1.ResourceCPU,
					machineType:  "machine-1",
					utilBucket:   utilization45To50,
				},
			},
		},
		"utilization of cpu is 80%, but memory is 30%": {
			nodeInfo: func() *framework.NodeInfo {
				n1 := test.BuildTestNode("node1", 1000, 1000)
				n1.Labels = map[string]string{
					labels.ExtendedDurationPodsLabel: "1",
					v1.LabelInstanceTypeStable:       "machine-1",
				}
				pod := test.BuildTestPod("p1", 800, 300)
				nn := framework.NewTestNodeInfo(n1, pod)
				return nn
			},
			expectedTuples: []metricTuple{
				{
					nodeType:     edp,
					resourceName: v1.ResourceCPU,
					machineType:  "machine-1",
					utilBucket:   utilization70To100,
				},
				{
					nodeType:     edp,
					resourceName: v1.ResourceMemory,
					machineType:  "machine-1",
					utilBucket:   utilization0To40,
				},
			},
		},
		"gpu node, no allocatable gpu, with utilization of cpu and memory is 80%": {
			nodeInfo: func() *framework.NodeInfo {
				n1 := test.BuildTestNode("node1", 1000, 1000)
				n1.Labels = map[string]string{
					labels.ExtendedDurationPodsLabel: "1",
					v1.LabelInstanceTypeStable:       "machine-1",
					"TestGPULabel/accelerator":       "1",
				}
				pod := test.BuildTestPod("p1", 800, 800)
				nn := framework.NewTestNodeInfo(n1, pod)
				return nn
			},
			expectedTuples: []metricTuple{
				{
					nodeType:     edpGpu,
					resourceName: v1.ResourceCPU,
					machineType:  "machine-1",
					utilBucket:   utilization70To100,
				},
				{
					nodeType:     edpGpu,
					resourceName: v1.ResourceMemory,
					machineType:  "machine-1",
					utilBucket:   utilization70To100,
				},
			},
		},
		"gpu node with utilization of cpu and memory is 50%, and gpu is 100%": {
			nodeInfo: func() *framework.NodeInfo {
				n1 := test.BuildTestNode("node1", 1000, 1000)
				n1.Labels = map[string]string{
					labels.ExtendedDurationPodsLabel: "1",
					v1.LabelInstanceTypeStable:       "machine-1",
					"TestGPULabel/accelerator":       "nvidia.com/gpu",
				}
				pod := test.BuildTestPod("p1", 500, 500)
				test.AddGpusToNode(n1, 1)
				test.RequestGpuForPod(pod, 1)
				nn := framework.NewTestNodeInfo(n1, pod)
				return nn
			},
			expectedTuples: []metricTuple{
				{
					nodeType:     edpGpu,
					resourceName: v1.ResourceCPU,
					machineType:  "machine-1",
					utilBucket:   utilization50To55,
				},
				{
					nodeType:     edpGpu,
					resourceName: v1.ResourceMemory,
					machineType:  "machine-1",
					utilBucket:   utilization50To55,
				},
				{
					nodeType:     edpGpu,
					resourceName: gpu.ResourceNvidiaGPU,
					machineType:  "machine-1",
					utilBucket:   utilization70To100,
				},
			},
		},
		"packed node, utilization of cpu and memory is 47%": {
			nodeInfo: func() *framework.NodeInfo {
				n1 := test.BuildTestNode("node1", 1000, 1000)
				n1.Labels = map[string]string{
					labels.ExtendedDurationPodsLabel: labels.ExtendedDurationPackedPodsValue,
					v1.LabelInstanceTypeStable:       "machine-1",
				}
				pod := test.BuildTestPod("p1", 470, 470)
				nn := framework.NewTestNodeInfo(n1, pod)
				return nn
			},
			expectedTuples: []metricTuple{
				{
					nodeType:     edpPacked,
					resourceName: v1.ResourceCPU,
					machineType:  "machine-1",
					utilBucket:   utilization45To50,
				},
				{
					nodeType:     edpPacked,
					resourceName: v1.ResourceMemory,
					machineType:  "machine-1",
					utilBucket:   utilization45To50,
				},
			},
		},
		"packed gpu node with utilization of cpu and memory is 50%, and gpu is 100%": {
			nodeInfo: func() *framework.NodeInfo {
				n1 := test.BuildTestNode("node1", 1000, 1000)
				n1.Labels = map[string]string{
					labels.ExtendedDurationPodsLabel: labels.ExtendedDurationPackedPodsValue,
					v1.LabelInstanceTypeStable:       "machine-1",
					"TestGPULabel/accelerator":       "nvidia.com/gpu",
				}
				pod := test.BuildTestPod("p1", 500, 500)
				test.AddGpusToNode(n1, 1)
				test.RequestGpuForPod(pod, 1)
				nn := framework.NewTestNodeInfo(n1, pod)
				return nn
			},
			expectedTuples: []metricTuple{
				{
					nodeType:     edpGpuPacked,
					resourceName: v1.ResourceCPU,
					machineType:  "machine-1",
					utilBucket:   utilization50To55,
				},
				{
					nodeType:     edpGpuPacked,
					resourceName: v1.ResourceMemory,
					machineType:  "machine-1",
					utilBucket:   utilization50To55,
				},
				{
					nodeType:     edpGpuPacked,
					resourceName: gpu.ResourceNvidiaGPU,
					machineType:  "machine-1",
					utilBucket:   utilization70To100,
				},
			},
		},
		"Nodes undergoing scale down": {
			nodeInfo: func() *framework.NodeInfo {
				n1 := test.BuildTestNode("node1", 1000, 1000)
				n1.Spec.Taints = append(n1.Spec.Taints, v1.Taint{Key: taints.ToBeDeletedTaint, Effect: v1.TaintEffectNoSchedule})
				nn := framework.NewTestNodeInfo(n1)
				return nn
			},
			expectedTuples: []metricTuple{},
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			ctx := &context.AutoscalingContext{
				CloudProvider: testCloudProvider.NewTestCloudProviderBuilder().Build(),
			}
			actual := getContributingTuples(ctx, tc.nodeInfo())
			assert.Equal(t, len(tc.expectedTuples), len(actual))
			for _, tr := range tc.expectedTuples {
				assert.Contains(t, actual, tr)
			}
		})
	}
}

func TestMetrics_UtilizationBuckets(t *testing.T) {

	testCases := map[string]struct {
		val      float64
		expected UtilizationBucket
	}{
		"less than 40": {
			val:      0.1,
			expected: utilization0To40,
		},
		"equals 40, which should be 40-45": {
			val:      0.4,
			expected: utilization40To45,
		},
		"higher than 70": {
			val:      0.85,
			expected: utilization70To100,
		},
		"equal to 70": {
			val:      0.7,
			expected: utilization70To100,
		},
		"equals 54.9999999999, which is less than [55-60)": {
			val:      0.549999999999,
			expected: utilization50To55,
		},
		"negative, which should be unsupported": {
			val:      -0.112,
			expected: utilizationUnsupported,
		},
		"greater than 1, which should be unsupported": {
			val:      2.112,
			expected: utilizationUnsupported,
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			actual := getUtilizationBucket(tc.val)
			assert.IsType(t, tc.expected, actual)
			assert.Equal(t, tc.expected, actual)
		})
	}
}
