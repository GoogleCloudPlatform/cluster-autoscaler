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

package gkeprice

import (
	"fmt"
	"math"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	expanderutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/systempods"
)

type testNodeLister struct {
	nodes []*apiv1.Node
}

func (l *testNodeLister) List() ([]*apiv1.Node, error) {
	return l.nodes, nil
}

func (l *testNodeLister) Get(name string) (*apiv1.Node, error) {
	for _, node := range l.nodes {
		if node.Name == name {
			return node, nil
		}
	}
	return nil, fmt.Errorf("node not found: %v", name)
}

// newTestNodeLister returns a lister that returns provided nodes
func newTestNodeLister(nodes []*apiv1.Node) *testNodeLister {
	return &testNodeLister{nodes: nodes}
}

func TestGroupingClusterAnalyzerWorkloadSeparation(t *testing.T) {
	testCases := []struct {
		workloads             []string
		expectedWorkloadCount int
		name                  string
	}{
		{[]string{"", ""}, 1, "two regular pods"},
		{[]string{"", "w1"}, 2, "one regular one dedicated pod"},
		{[]string{"w1", "w1"}, 1, "two pods from the same dedicated workload"},
		{[]string{"w1", "w2"}, 2, "two pods from different dedicated workloads"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nodes := []*apiv1.Node{}
			pods := []*apiv1.Pod{}
			provider := testprovider.NewTestCloudProviderBuilder().Build()
			nodeInfos := make(map[string]*framework.NodeInfo)
			for idx, workload := range tc.workloads {
				nodeGroupName := fmt.Sprintf("ng%d", idx)
				nodeName := fmt.Sprintf("n%d", idx)
				podName := fmt.Sprintf("p%d", idx)
				node := BuildTestNode(nodeName, 5000, 7*units.GiB)
				pod := BuildTestPod(podName, 2000, 3*units.GiB)
				if workload != "" {
					expanderutils.DedicateNodeForWorkload(node, workload)
					expanderutils.DedicatePodForWorkload(pod, workload)
				}
				provider.AddNodeGroup(nodeGroupName, 1, 10, 1)
				provider.AddNode(nodeGroupName, node)
				pod.Spec.NodeName = nodeName
				nodes = append(nodes, node)
				pods = append(pods, pod)
				nodeInfo := framework.NewTestNodeInfo(node)
				nodeInfos[nodeGroupName] = nodeInfo
			}
			analyzer := NewGroupingClusterAnalyzer(provider, newTestNodeLister(nodes), kube_util.NewTestPodLister(pods), nil)
			analysis, err := analyzer.Analyze(nodeInfos)
			assert.NoError(t, err)
			groupingAnalysis, ok := analysis.(*groupingClusterAnalysis)
			assert.True(t, ok)
			if assert.Equal(t, tc.expectedWorkloadCount, len(groupingAnalysis.workloadCapacity)) {
				assert.Equal(t, tc.expectedWorkloadCount, len(groupingAnalysis.workloadUse))
			}
		})
	}
}

func TestGroupingClusterAnalyzerGpuSeparation(t *testing.T) {
	testCases := []struct {
		gpus                  []string
		expectedWorkloadCount int
		name                  string
	}{
		{[]string{"", ""}, 1, "two regular pods"},
		{[]string{"", "k80"}, 2, "one regular pod one with gpu"},
		{[]string{"k80", "k80"}, 1, "two pods same gpu types"},
		{[]string{"k80", "p100"}, 1, "two pods different gpu types"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nodes := []*apiv1.Node{}
			pods := []*apiv1.Pod{}
			provider := testprovider.NewTestCloudProviderBuilder().Build()
			nodeInfos := make(map[string]*framework.NodeInfo)
			for idx, gpuType := range tc.gpus {
				nodeGroupName := fmt.Sprintf("ng%d", idx)
				nodeName := fmt.Sprintf("n%d", idx)
				podName := fmt.Sprintf("p%d", idx)
				node := BuildTestNode(nodeName, 5000, 7*units.GiB)
				pod := BuildTestPod(podName, 2000, 3*units.GiB)
				if gpuType != "" {
					node.Spec.Taints = []apiv1.Taint{{
						Key:    gpu.ResourceNvidiaGPU,
						Value:  "present",
						Effect: "NoSchedule",
					}}
					node.Labels[labels.GPULabel] = gpuType
					node.Labels[gpu.ResourceNvidiaGPU] = gpuType
					node.Status.Capacity[gpu.ResourceNvidiaGPU] = *resource.NewQuantity(1, resource.DecimalSI)
					pod.Spec.Containers[0].Resources.Requests[gpu.ResourceNvidiaGPU] = *resource.NewQuantity(1, resource.DecimalSI)
					expanderutils.AddGpuTolerationToPod(pod)
					pod.Spec.NodeSelector = map[string]string{labels.GPULabel: gpuType}
				}
				provider.AddNodeGroup(nodeGroupName, 1, 10, 1)
				provider.AddNode(nodeGroupName, node)
				pod.Spec.NodeName = nodeName
				nodes = append(nodes, node)
				pods = append(pods, pod)
				nodeInfo := framework.NewTestNodeInfo(node)
				nodeInfos[nodeGroupName] = nodeInfo
			}
			analyzer := NewGroupingClusterAnalyzer(provider, newTestNodeLister(nodes), kube_util.NewTestPodLister(pods), nil)
			analysis, err := analyzer.Analyze(nodeInfos)
			assert.NoError(t, err)
			groupingAnalysis, ok := analysis.(*groupingClusterAnalysis)
			assert.True(t, ok)
			if assert.Equal(t, tc.expectedWorkloadCount, len(groupingAnalysis.workloadCapacity)) {
				assert.Equal(t, tc.expectedWorkloadCount, len(groupingAnalysis.workloadUse))
			}
		})
	}
}

func TestAnalyzeExisting(t *testing.T) {
	testCases := []struct {
		nodes    int
		milliCpu int64
		memory   int64
	}{
		{2, 1000, 2 * units.GiB},
		{2, 2000, 5 * units.GiB},
		{1, 4000, 5 * units.GiB},
		{2, 4000, 10 * units.GiB},
		{4, 4000, 10 * units.GiB},
		{8, 4000, 10 * units.GiB},
		{4, 16000, 1 * units.GiB},
		{32, 16000, 1 * units.GiB},
		{100, 32000, 1 * units.GiB},
		{1, 2000, 1 * units.GiB},
	}
	for _, tc := range testCases {
		nodes := []*apiv1.Node{}
		pods := []*apiv1.Pod{}
		provider := testprovider.NewTestCloudProviderBuilder().Build()
		nodeGroupName := "ng"
		provider.AddNodeGroup(nodeGroupName, 1, tc.nodes, tc.nodes)
		nodeInfos := make(map[string]*framework.NodeInfo)
		nodeInfo := framework.NewTestNodeInfo(nil)
		nodeInfos[nodeGroupName] = nodeInfo
		for i := 0; i < tc.nodes; i++ {
			nodeName := fmt.Sprintf("n%d", i)
			node := BuildTestNode(nodeName, tc.milliCpu, tc.memory)
			provider.AddNode(nodeGroupName, node)
			nodes = append(nodes, node)
			// Add a pod to the first node
			if nodeInfo.Node() == nil {
				podName := fmt.Sprintf("p%d", i)
				pod := BuildTestPod(podName, tc.milliCpu, tc.memory)
				pod.Spec.NodeName = nodeName
				pods = append(pods, pod)
				nodeInfo.SetNode(node)
			}
		}
		analyzer := NewGroupingClusterAnalyzer(provider, newTestNodeLister(nodes), kube_util.NewTestPodLister(pods), nil)
		analysis, err := analyzer.Analyze(nodeInfos)
		assert.NoError(t, err)
		groupingAnalysis, ok := analysis.(*groupingClusterAnalysis)
		assert.True(t, ok)
		assert.Equal(t, int64(tc.nodes)*tc.milliCpu, groupingAnalysis.workloadCapacity[""].MilliCPU)
		assert.Equal(t, int64(tc.nodes)*tc.memory, groupingAnalysis.workloadCapacity[""].Memory)
	}
}

func TestGetPreferredCpuCountExisting(t *testing.T) {
	testCases := []struct {
		nodes       int
		cpusPerNode int64
		expected    int64
	}{
		{1, 2, 1},
		{2, 1, 1},
		{2, 2, 2},
		{1, 4, 2},
		{2, 4, 2},
		{4, 4, 4},
		{8, 4, 4},
		{4, 16, 8},
		{32, 16, 16},
		{100, 32, 32},
	}
	for _, tc := range testCases {
		nodes := []*apiv1.Node{}
		pods := []*apiv1.Pod{}
		provider := testprovider.NewTestCloudProviderBuilder().Build()
		nodeGroupName := "ng"
		provider.AddNodeGroup(nodeGroupName, 1, tc.nodes, tc.nodes)
		nodeInfos := make(map[string]*framework.NodeInfo)
		nodeInfo := framework.NewTestNodeInfo(nil)
		nodeInfos[nodeGroupName] = nodeInfo
		for i := 0; i < tc.nodes; i++ {
			nodeName := fmt.Sprintf("n%d", i)
			podName := fmt.Sprintf("p%d", i)
			node := BuildTestNode(nodeName, tc.cpusPerNode*1000, 7*units.GiB)
			pod := BuildTestPod(podName, tc.cpusPerNode*1000, 3*units.GiB)
			provider.AddNode(nodeGroupName, node)
			pod.Spec.NodeName = nodeName
			nodes = append(nodes, node)
			pods = append(pods, pod)
			if nodeInfo.Node() == nil {
				nodeInfo.SetNode(node)
			}
		}
		analyzer := NewGroupingClusterAnalyzer(provider, newTestNodeLister(nodes), kube_util.NewTestPodLister(pods), nil)
		analysis, err := analyzer.Analyze(nodeInfos)
		assert.NoError(t, err)
		nodeGroup, err := provider.NodeGroupForNode(nodes[0])
		assert.NoError(t, err)
		option := expander.Option{NodeGroup: nodeGroup}
		result, err := analysis.GetPreferredCpuCount(option, nodeInfo)
		assert.NoError(t, err)
		assert.Equal(t, tc.expected, result, "nodes %v x %v cpus", tc.nodes, tc.cpusPerNode)
	}
}

func TestGetPreferredCpuCountNew(t *testing.T) {
	testCases := []struct {
		podCount   int
		cpusPerPod int64
		expected   int64
	}{
		{1, 2, 1},
		{2, 1, 1},
		{2, 2, 2},
		{1, 4, 2},
		{2, 4, 2},
		{4, 4, 4},
		{8, 4, 4},
		{4, 16, 8},
		{32, 16, 16},
		{100, 32, 32},
	}
	for _, tc := range testCases {
		pods := []*apiv1.Pod{}
		provider := testprovider.NewTestCloudProviderBuilder().Build()
		nodeGroupName := "ng"
		provider.AddNodeGroup(nodeGroupName, 0, 0, 0)
		nodeGroups := provider.NodeGroups()
		assert.Equal(t, 1, len(nodeGroups))
		nodeGroup := nodeGroups[0]
		nodeInfos := make(map[string]*framework.NodeInfo)
		nodeInfo := framework.NewTestNodeInfo(nil)
		nodeInfos[nodeGroupName] = nodeInfo
		node := BuildTestNode("n1", tc.cpusPerPod*1000, 7*units.GiB)
		nodeInfo.SetNode(node)
		for i := 0; i < tc.podCount; i++ {
			podName := fmt.Sprintf("p%d", i)
			pod := BuildTestPod(podName, tc.cpusPerPod*1000, 3*units.GiB)
			pods = append(pods, pod)
		}
		analyzer := NewGroupingClusterAnalyzer(provider, newTestNodeLister(nil), kube_util.NewTestPodLister(nil), nil)
		analysis, err := analyzer.Analyze(nodeInfos)
		assert.NoError(t, err)
		optionNoPods := expander.Option{NodeGroup: nodeGroup, NodeCount: tc.podCount}
		result, err := analysis.GetPreferredCpuCount(optionNoPods, nodeInfo)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), result, "nodes %v x %v cpus, no pods", tc.podCount, tc.cpusPerPod)
		optionBigPods := expander.Option{NodeGroup: nodeGroup, NodeCount: tc.podCount, Pods: pods}
		result, err = analysis.GetPreferredCpuCount(optionBigPods, nodeInfo)
		assert.NoError(t, err)
		assert.Equal(t, tc.expected, result, "pods %v x %v cpus", tc.podCount, tc.cpusPerPod)
	}
}

func TestPreferredCpuCount(t *testing.T) {
	testCases := []struct {
		cpus     int64
		expected int64
	}{
		{-1000, 1},
		{0, 1},
		{1, 1},
		{2, 1},
		{3, 1},
		{4, 2},
		{8, 2},
		{12, 2},
		{13, 4},
		{30, 4},
		{60, 4},
		{61, 8},
		{200, 8},
		{380, 8},
		{381, 16},
		{1000, 16},
		{2300, 16},
		{2301, 32},
		{10000, 32},
		{18300, 32},
		{18301, 64},
		{100000, 64},
		{107900, 64},
		{107901, 96},
		{1000000, 96},
	}
	for _, tc := range testCases {
		assert.Equal(t, tc.expected, preferredCpuCount(tc.cpus))
	}
}

func TestCalculateCpuMilliEquivalent(t *testing.T) {
	testCases := []struct {
		memory   int64
		expected int64
	}{
		{-units.GiB, -154},
		{0, 0},
		{units.GiB, 154},
		{13 * units.GiB, 2000},
		{130 * units.GiB, 20000},
	}
	for _, tc := range testCases {
		assert.InDelta(t, tc.expected, calculateCpuMilliEquivalent(tc.memory), math.Abs(float64(tc.expected)/100))
	}
}

func TestCropToRatio(t *testing.T) {
	testCases := []struct {
		name            string
		target          float64
		inputResource1  int64
		inputResource2  int64
		outputResource1 int64
		outputResource2 int64
	}{
		{
			name:            "Target = 1",
			target:          1,
			inputResource1:  10,
			inputResource2:  20,
			outputResource1: 10,
			outputResource2: 10,
		},
		{
			name:            "Target < 1",
			target:          0.5,
			inputResource1:  20,
			inputResource2:  20,
			outputResource1: 10,
			outputResource2: 20,
		},
		{
			name:            "Target > 1",
			target:          1.5,
			inputResource1:  30,
			inputResource2:  10,
			outputResource1: 15,
			outputResource2: 10,
		},
	}
	for _, test := range testCases {
		resource1, resource2 := cropToRatio(test.target, test.inputResource1, test.inputResource2)
		if resource1 != test.outputResource1 || resource2 != test.outputResource2 {
			t.Errorf("%s: cropToRatio(%v, %v, %v) returned %v, %v; expected %v, %v", test.name, test.target, test.inputResource1, test.inputResource2, resource1, resource2, test.outputResource1, test.outputResource2)
		}
	}
}

func TestCalculateReusable(t *testing.T) {
	testCases := []struct {
		name                 string
		wastedMemory         int64
		wastedCpu            int64
		wastedEph            int64
		memToCpu             float64
		ephToCpu             float64
		outputReusableMemory int64
		outputReusableCpu    int64
		outputReusableEph    int64
	}{
		{
			name:                 "base",
			wastedMemory:         1000,
			wastedCpu:            1000,
			wastedEph:            1000,
			memToCpu:             2,
			ephToCpu:             2,
			outputReusableMemory: 500,
			outputReusableCpu:    250,
			outputReusableEph:    500,
		},
		{
			name:                 "output cpu is a min",
			wastedMemory:         200,
			wastedCpu:            99,
			wastedEph:            300,
			memToCpu:             1,
			ephToCpu:             2,
			outputReusableMemory: 50,
			outputReusableCpu:    50,
			outputReusableEph:    100,
		},
		{
			name:                 "output memory is a min",
			wastedMemory:         100,
			wastedCpu:            299,
			wastedEph:            300,
			memToCpu:             0.5,
			ephToCpu:             2,
			outputReusableMemory: 37,
			outputReusableCpu:    75,
			outputReusableEph:    150,
		},
		{
			name:                 "output ephemeral storage is a min",
			wastedMemory:         200,
			wastedCpu:            99,
			wastedEph:            300,
			memToCpu:             1,
			ephToCpu:             0.5,
			outputReusableMemory: 50,
			outputReusableCpu:    50,
			outputReusableEph:    25,
		},
		{
			name:                 "resources limited by input cpu",
			wastedMemory:         200,
			wastedCpu:            99,
			wastedEph:            300,
			memToCpu:             1,
			ephToCpu:             1,
			outputReusableMemory: 50,
			outputReusableCpu:    50,
			outputReusableEph:    50,
		},
		{
			name:                 "resources limited by input memory",
			wastedMemory:         100,
			wastedCpu:            299,
			wastedEph:            300,
			memToCpu:             1,
			ephToCpu:             1,
			outputReusableMemory: 50,
			outputReusableCpu:    50,
			outputReusableEph:    50,
		},
		{
			name:                 "resources limited by input ephemeral storage",
			wastedMemory:         200,
			wastedCpu:            99,
			wastedEph:            50,
			memToCpu:             1,
			ephToCpu:             1,
			outputReusableMemory: 25,
			outputReusableCpu:    25,
			outputReusableEph:    25,
		},
	}
	for _, test := range testCases {
		wastedResource := Resource{
			MilliCPU:         test.wastedCpu,
			Memory:           test.wastedMemory,
			EphemeralStorage: test.wastedEph,
		}
		outputReusableResource := Resource{
			MilliCPU:         test.outputReusableCpu,
			Memory:           test.outputReusableMemory,
			EphemeralStorage: test.outputReusableEph,
		}
		a := groupingClusterAnalysis{}
		ratio := resourceRatio{memToCpu: test.memToCpu, ephToCpu: test.ephToCpu}
		got := a.calculateReusable(wastedResource, ratio)
		if diff := cmp.Diff(outputReusableResource, got); diff != "" {
			t.Errorf("%s: calculateReusable(%+v, %+v) mismatch diff (-want +got):\n%s", test.name, wastedResource, ratio, diff)
		}
	}
}

func TestCalculateLimit(t *testing.T) {
	testCases := []struct {
		name            string
		resource1       int64
		resource2       int64
		nodeCount       int64
		targetRatio     float64
		outputResource1 int64
		outputResource2 int64
	}{
		{
			name:            "NodeCount = 2, initial resources have target ratio",
			resource1:       100,
			resource2:       200,
			nodeCount:       2,
			targetRatio:     0.5,
			outputResource1: 100,
			outputResource2: 200,
		},
		{
			name:            "NodeCount = 2, targetRatio < 1, initial resources have smaller ratio then target one",
			resource1:       100,
			resource2:       400,
			nodeCount:       2,
			targetRatio:     0.5,
			outputResource1: 200,
			outputResource2: 400,
		},
		{
			name:            "NodeCount = 2, targetRatio < 1, initial resources have bigger ratio then target one",
			resource1:       100,
			resource2:       100,
			nodeCount:       2,
			targetRatio:     0.5,
			outputResource1: 100,
			outputResource2: 200,
		},
		{
			name:            "NodeCount = 2, targetRatio > 1, initial resources have smaller ratio then target one",
			resource1:       100,
			resource2:       100,
			nodeCount:       2,
			targetRatio:     2,
			outputResource1: 200,
			outputResource2: 100,
		},
		{
			name:            "NodeCount > 2",
			resource1:       300,
			resource2:       130,
			nodeCount:       4,
			targetRatio:     2,
			outputResource1: 200,
			outputResource2: 100,
		},
	}
	for _, test := range testCases {
		if test.nodeCount <= 1 {
			return
		}
		resource1, resource2 := applyLimit(test.resource1, test.resource2, test.nodeCount, test.targetRatio)
		if resource1 != test.outputResource1 || resource2 != test.outputResource2 {
			t.Errorf("%s: calculateLimit(%v, %v, %v, %v) returned %v, %v; expected %v, %v", test.name, test.resource1, test.resource2, test.nodeCount, test.targetRatio, resource1, resource2, test.outputResource1, test.outputResource2)
		}
	}
}

func TestPodApproximateResources(t *testing.T) {
	namespaces := []string{"kube-system"}
	testCases := []struct {
		name                        string
		existingNodes               int
		newNodes                    int
		podsPerExistingNodes        int
		podsPerNewNodes             int
		existingNodeTaint           *apiv1.Taint
		newNodeTaint                *apiv1.Taint
		existingUserPodsResource    *Resource
		existingKubeSystemResources *Resource
		existingDSPodResource       *Resource
		newUserPodResource          *Resource
		newDSPodResource            *Resource
		newKubeSystemResource       *Resource
		expectedResources           *Resource
		expectedWorkloadCount       map[string]int
	}{
		{
			name:          "same requests for new and existing pods",
			existingNodes: 3,
			newNodes:      5,
			existingUserPodsResource: &Resource{
				MilliCPU:         1 * 1000,
				Memory:           1 * units.GiB,
				EphemeralStorage: 1,
			},
			podsPerExistingNodes: 1,
			podsPerNewNodes:      1,
			newUserPodResource: &Resource{
				MilliCPU:         1 * 1000,
				Memory:           1 * units.GiB,
				EphemeralStorage: 1,
			},
			expectedResources: &Resource{
				MilliCPU:         1 * 1000,
				EphemeralStorage: 1,
				Memory:           1 * units.GiB,
			},
			expectedWorkloadCount: map[string]int{
				"": 3,
			},
		},
		{
			name:                 "double requests for new pods",
			existingNodes:        1,
			newNodes:             1,
			podsPerExistingNodes: 1,
			podsPerNewNodes:      1,
			existingUserPodsResource: &Resource{
				MilliCPU:         1 * 1000,
				Memory:           1 * units.GiB,
				EphemeralStorage: 1,
			},
			newUserPodResource: &Resource{
				MilliCPU:         2 * 1000,
				Memory:           2 * units.GiB,
				EphemeralStorage: 2,
			},
			expectedResources: &Resource{
				MilliCPU:         1666,
				Memory:           (5 * units.GiB) / 3,
				EphemeralStorage: 1,
			},
			expectedWorkloadCount: map[string]int{
				"": 1,
			},
		},
		{
			name:                 "double requests for new pods, double number of existing nodes",
			existingNodes:        4,
			newNodes:             2,
			podsPerExistingNodes: 1,
			podsPerNewNodes:      1,
			existingUserPodsResource: &Resource{
				MilliCPU:         1 * 1000,
				Memory:           1 * units.GiB,
				EphemeralStorage: 10,
			},
			newUserPodResource: &Resource{
				MilliCPU:         2 * 1000,
				Memory:           2 * units.GiB,
				EphemeralStorage: 20,
			},
			expectedResources: &Resource{
				MilliCPU:         1500,
				Memory:           (3 * units.GiB) / 2,
				EphemeralStorage: 15,
			},
			expectedWorkloadCount: map[string]int{
				"": 4,
			},
		},
		{
			name:                 "double requests for new pods, double number of new pods",
			existingNodes:        1,
			newNodes:             1,
			podsPerExistingNodes: 1,
			podsPerNewNodes:      2,
			existingUserPodsResource: &Resource{
				MilliCPU:         1 * 1000,
				Memory:           1 * units.GiB,
				EphemeralStorage: 10,
			},
			newUserPodResource: &Resource{
				MilliCPU:         2 * 1000,
				Memory:           2 * units.GiB,
				EphemeralStorage: 20,
			},
			expectedResources: &Resource{
				MilliCPU:         1800,
				Memory:           (9 * units.GiB) / 5,
				EphemeralStorage: 18,
			},
			expectedWorkloadCount: map[string]int{
				"": 1,
			},
		},
		{
			name:                 "double requests for new pods, double number of existing pods",
			existingNodes:        1,
			newNodes:             1,
			podsPerExistingNodes: 2,
			podsPerNewNodes:      1,
			existingUserPodsResource: &Resource{
				MilliCPU:         1 * 1000,
				Memory:           1 * units.GiB,
				EphemeralStorage: 10,
			},
			newUserPodResource: &Resource{
				MilliCPU:         2 * 1000,
				Memory:           2 * units.GiB,
				EphemeralStorage: 20,
			},
			expectedResources: &Resource{
				MilliCPU:         1500,
				Memory:           (3 * units.GiB) / 2,
				EphemeralStorage: 15,
			},
			expectedWorkloadCount: map[string]int{
				"": 2,
			},
		},
		{
			name:                 "new nodes in a different workload separation group",
			existingNodes:        1,
			newNodes:             1,
			podsPerExistingNodes: 1,
			podsPerNewNodes:      1,
			existingNodeTaint: &apiv1.Taint{
				Key:    "key1",
				Value:  "val1",
				Effect: apiv1.TaintEffectNoSchedule,
			},
			newNodeTaint: &apiv1.Taint{
				Key:    "key2",
				Value:  "val2",
				Effect: apiv1.TaintEffectNoSchedule,
			},
			existingUserPodsResource: &Resource{
				MilliCPU:         1 * 1000,
				Memory:           1 * units.GiB,
				EphemeralStorage: 10,
			},
			newUserPodResource: &Resource{
				MilliCPU:         2 * 1000,
				Memory:           2 * units.GiB,
				EphemeralStorage: 20,
			},
			expectedResources: &Resource{
				MilliCPU:         2000,
				Memory:           2 * units.GiB,
				EphemeralStorage: 20,
			},
			expectedWorkloadCount: map[string]int{
				"NoSchedule:key1:val1": 1,
			},
		},
		{
			name:                 "no existing pods",
			existingNodes:        0,
			newNodes:             5,
			podsPerExistingNodes: 1,
			podsPerNewNodes:      1,
			newUserPodResource: &Resource{
				MilliCPU:         1 * 1000,
				Memory:           1 * units.GiB,
				EphemeralStorage: 1,
			},
			expectedResources: &Resource{
				MilliCPU:         1000,
				Memory:           1 * units.GiB,
				EphemeralStorage: 1,
			},
			expectedWorkloadCount: map[string]int{},
		},
		{
			name:                 "no new nodes",
			existingNodes:        3,
			newNodes:             0,
			podsPerExistingNodes: 1,
			existingUserPodsResource: &Resource{
				MilliCPU:         1 * 1000,
				Memory:           1 * units.GiB,
				EphemeralStorage: 10,
			},
			expectedResources: &Resource{
				MilliCPU:         0,
				Memory:           0,
				EphemeralStorage: 0,
			},
			expectedWorkloadCount: map[string]int{
				"": 3,
			},
		},
		{
			name:                 "existing pods from kube-system",
			existingNodes:        3,
			newNodes:             5,
			podsPerExistingNodes: 1,
			podsPerNewNodes:      1,
			existingKubeSystemResources: &Resource{
				MilliCPU:         2 * 1000,
				Memory:           2 * units.GiB,
				EphemeralStorage: 2,
			},
			newUserPodResource: &Resource{
				MilliCPU:         1 * 1000,
				Memory:           1 * units.GiB,
				EphemeralStorage: 1,
			},
			expectedResources: &Resource{
				MilliCPU:         1 * 1000,
				EphemeralStorage: 1,
				Memory:           1 * units.GiB,
			},
			expectedWorkloadCount: map[string]int{},
		},
		{
			name:                 "new pods from kube-system",
			existingNodes:        3,
			newNodes:             5,
			podsPerExistingNodes: 1,
			podsPerNewNodes:      1,
			existingUserPodsResource: &Resource{
				MilliCPU:         2 * 1000,
				Memory:           2 * units.GiB,
				EphemeralStorage: 2,
			},
			newKubeSystemResource: &Resource{
				MilliCPU:         1 * 1000,
				Memory:           1 * units.GiB,
				EphemeralStorage: 1,
			},
			expectedResources: &Resource{
				MilliCPU:         2 * 1000,
				EphemeralStorage: 2,
				Memory:           2 * units.GiB,
			},
			expectedWorkloadCount: map[string]int{
				"": 3,
			},
		},
		{
			name:                 "existing pods are owned by DS",
			existingNodes:        3,
			newNodes:             5,
			podsPerExistingNodes: 1,
			podsPerNewNodes:      1,
			existingDSPodResource: &Resource{
				MilliCPU:         2 * 1000,
				Memory:           2 * units.GiB,
				EphemeralStorage: 2,
			},
			newUserPodResource: &Resource{
				MilliCPU:         1 * 1000,
				Memory:           1 * units.GiB,
				EphemeralStorage: 1,
			},
			expectedResources: &Resource{
				MilliCPU:         1 * 1000,
				EphemeralStorage: 1,
				Memory:           1 * units.GiB,
			},
			expectedWorkloadCount: map[string]int{},
		},
		{
			name:                 "new pods are owned by DS",
			existingNodes:        3,
			newNodes:             5,
			podsPerExistingNodes: 1,
			podsPerNewNodes:      1,
			existingUserPodsResource: &Resource{
				MilliCPU:         2 * 1000,
				Memory:           2 * units.GiB,
				EphemeralStorage: 2,
			},
			newDSPodResource: &Resource{
				MilliCPU:         1 * 1000,
				Memory:           1 * units.GiB,
				EphemeralStorage: 1,
			},
			expectedResources: &Resource{
				MilliCPU:         2 * 1000,
				EphemeralStorage: 2,
				Memory:           2 * units.GiB,
			},
			expectedWorkloadCount: map[string]int{
				"": 3,
			},
		},
		{
			name:                 "all pod configurations in new and existing nodes",
			existingNodes:        1,
			newNodes:             1,
			podsPerExistingNodes: 1,
			podsPerNewNodes:      1,
			existingUserPodsResource: &Resource{
				MilliCPU:         1 * 1000,
				Memory:           1 * units.GiB,
				EphemeralStorage: 1,
			},
			existingDSPodResource: &Resource{
				MilliCPU:         10 * 1000,
				Memory:           10 * units.GiB,
				EphemeralStorage: 10,
			},
			existingKubeSystemResources: &Resource{
				MilliCPU:         10 * 1000,
				Memory:           10 * units.GiB,
				EphemeralStorage: 10,
			},
			newUserPodResource: &Resource{
				MilliCPU:         2 * 1000,
				Memory:           2 * units.GiB,
				EphemeralStorage: 2,
			},
			newDSPodResource: &Resource{
				MilliCPU:         10 * 1000,
				Memory:           10 * units.GiB,
				EphemeralStorage: 10,
			},
			newKubeSystemResource: &Resource{
				MilliCPU:         10 * 1000,
				Memory:           10 * units.GiB,
				EphemeralStorage: 10,
			},
			expectedResources: &Resource{
				MilliCPU:         1666,
				Memory:           (5 * units.GiB) / 3,
				EphemeralStorage: 1,
			},
			expectedWorkloadCount: map[string]int{
				"": 1,
			},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var nodes []*apiv1.Node
			var pods []*apiv1.Pod
			provider := testprovider.NewTestCloudProviderBuilder().Build()
			nodeGroupName := "ng"
			if tc.existingNodes > 0 {
				provider.AddNodeGroup(nodeGroupName, 1, tc.existingNodes, tc.existingNodes)
			}
			for i := 0; i < tc.existingNodes; i++ {
				nodeName := fmt.Sprintf("n%d", i)
				podName := fmt.Sprintf("p%d", i)
				node := BuildTestNode(nodeName, 5*1000, 7*units.GiB)
				if tc.existingNodeTaint != nil {
					node.Spec.Taints = []apiv1.Taint{*tc.existingNodeTaint}
					node.Labels[tc.existingNodeTaint.Key] = tc.existingNodeTaint.Value
				}
				if tc.existingUserPodsResource != nil {
					provider.AddNode(nodeGroupName, node)
					for j := 0; j < tc.podsPerExistingNodes; j++ {
						pod := BuildTestPodWithEphemeralStorage(podName, tc.existingUserPodsResource.MilliCPU, tc.existingUserPodsResource.Memory, tc.existingUserPodsResource.EphemeralStorage)
						pod.Spec.NodeName = nodeName
						pods = append(pods, pod)
					}
				}
				if tc.existingKubeSystemResources != nil {
					pod := BuildTestPodWithEphemeralStorage(podName, tc.existingKubeSystemResources.MilliCPU, tc.existingKubeSystemResources.Memory, tc.existingKubeSystemResources.EphemeralStorage)
					provider.AddNode(nodeGroupName, node)
					pod.Spec.NodeName = nodeName
					pod.Namespace = "kube-system"
					pods = append(pods, pod)
				}
				if tc.existingDSPodResource != nil {
					pod := BuildTestPodWithEphemeralStorage(podName, tc.existingDSPodResource.MilliCPU, tc.existingDSPodResource.Memory, tc.existingDSPodResource.EphemeralStorage)
					provider.AddNode(nodeGroupName, node)
					pod.Spec.NodeName = nodeName
					pod.OwnerReferences = []metav1.OwnerReference{{Kind: "DaemonSet"}}
					pods = append(pods, pod)
				}
				nodes = append(nodes, node)
			}
			var newNodeInfos []framework.NodeInfo
			for i := 0; i < tc.newNodes; i++ {
				nodeName := fmt.Sprintf("n_n%d", i)
				podName := fmt.Sprintf("p_n%d", i)
				node := BuildTestNode(nodeName, 5*1000, 7*units.GiB)
				if tc.newNodeTaint != nil {
					node.Spec.Taints = []apiv1.Taint{*tc.newNodeTaint}
					node.Labels[tc.newNodeTaint.Key] = tc.newNodeTaint.Value
				}
				var newPods []*apiv1.Pod
				if tc.newUserPodResource != nil {
					for j := 0; j < tc.podsPerNewNodes; j++ {
						pod := BuildTestPodWithEphemeralStorage(podName, tc.newUserPodResource.MilliCPU, tc.newUserPodResource.Memory, tc.newUserPodResource.EphemeralStorage)
						provider.AddNode(nodeGroupName, node)
						newPods = append(newPods, pod)
					}
				}
				if tc.newKubeSystemResource != nil {
					pod := BuildTestPodWithEphemeralStorage(podName, tc.newKubeSystemResource.MilliCPU, tc.newKubeSystemResource.Memory, tc.newKubeSystemResource.EphemeralStorage)
					provider.AddNode(nodeGroupName, node)
					pod.Namespace = "kube-system"
					newPods = append(newPods, pod)
				}
				if tc.newDSPodResource != nil {
					pod := BuildTestPodWithEphemeralStorage(podName, tc.newDSPodResource.MilliCPU, tc.newDSPodResource.Memory, tc.newDSPodResource.EphemeralStorage)
					provider.AddNode(nodeGroupName, node)
					pod.OwnerReferences = []metav1.OwnerReference{{Kind: "DaemonSet"}}
					newPods = append(newPods, pod)
				}
				info := framework.NewTestNodeInfo(node, newPods...)
				newNodeInfos = append(newNodeInfos, *info)
			}
			analyzer := NewGroupingClusterAnalyzer(provider, newTestNodeLister(nodes), kube_util.NewTestPodLister(pods), systempods.NewClassifier(namespaces))
			analysis, err := analyzer.AnalyzeUserWorkloadUse()

			assert.NoError(t, err)
			cAnalysis := analysis.(*groupingClusterAnalysis)
			assert.Equal(t, cAnalysis.userWorkloadCount, tc.expectedWorkloadCount)
			res, err := analysis.GetPodResourceRequestApproximation(newNodeInfos)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedResources.MilliCPU, res.MilliCPU)
			assert.Equal(t, tc.expectedResources.Memory, res.Memory)
			assert.Equal(t, tc.expectedResources.EphemeralStorage, res.EphemeralStorage)
		})
	}
}

func TestLimitReusableResources(t *testing.T) {
	testCases := []struct {
		name          string
		nodeCount     int
		memToCpu      float64
		ephToCpu      float64
		reusableMem   int64
		reusableCpu   int64
		reusableEph   int64
		newRequestMem int64
		newRequestCpu int64
		newRequestEph int64
		outputMem     int64
		outputCpu     int64
		outputEph     int64
	}{
		{
			name:          "NodeCount = 1 — limitReusableResources() doesn't change resources",
			nodeCount:     1,
			memToCpu:      1,
			ephToCpu:      1,
			reusableMem:   100,
			reusableCpu:   100,
			reusableEph:   100,
			newRequestMem: 1,
			newRequestCpu: 1,
			newRequestEph: 1,
			outputMem:     100,
			outputCpu:     100,
			outputEph:     100,
		},
		{
			name:          "NodeCount > 2",
			nodeCount:     11,
			memToCpu:      2,
			ephToCpu:      2,
			reusableMem:   100,
			reusableCpu:   50,
			reusableEph:   100,
			newRequestMem: 10,
			newRequestCpu: 10,
			newRequestEph: 10,
			outputMem:     5,
			outputCpu:     6,
			outputEph:     5,
		},
	}
	for _, test := range testCases {
		a := groupingClusterAnalysis{}
		ratio := resourceRatio{memToCpu: test.memToCpu, ephToCpu: test.ephToCpu}
		reusableResources := Resource{Memory: test.reusableMem, MilliCPU: test.reusableCpu, EphemeralStorage: test.reusableEph}
		newPodsRequests := Resource{Memory: test.newRequestMem, MilliCPU: test.newRequestCpu, EphemeralStorage: test.newRequestEph}
		want := Resource{Memory: test.outputMem, MilliCPU: test.outputCpu, EphemeralStorage: test.outputEph}
		got := a.limitReusableResources(test.nodeCount, reusableResources, newPodsRequests, ratio)
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("%s: limitReusableResources(%d, %+v, %+v, %+v) mismatch diff (-want +got):\n%s", test.name, test.nodeCount, reusableResources, newPodsRequests, ratio, diff)
		}
	}
}
