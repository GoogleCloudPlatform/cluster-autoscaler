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
	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	test_utils "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/operationtracker"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	calculator_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator/test"
)

func TestNodeGroupListProcessor(t *testing.T) {
	nodeName := "node-1"

	e2Standard32Ng := gke.NewTestGkeMigBuilder().
		SetNodePoolName("nodepool-e2").
		SetGceRefName("nodepool-e2").
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: "e2-standard-32",
			Spot:        false}).
		Build()

	ekStandard16Ng := gke.NewTestGkeMigBuilder().
		SetNodePoolName("nodepool-ek-16").
		SetGceRefName("nodepool-ek-16").
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: "ek-standard-16",
			Spot:        false}).
		Build()

	ekStandard32Ng := gke.NewTestGkeMigBuilder().
		SetNodePoolName("nodepool-ek-32").
		SetGceRefName("nodepool-ek-32").
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: "ek-standard-32",
			Spot:        false}).
		Build()

	e4aStandard16Ng := gke.NewTestGkeMigBuilder().
		SetNodePoolName("nodepool-e4a-16").
		SetGceRefName("nodepool-e4a-16").
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: "e4a-standard-16",
			Spot:        false}).
		Build()

	bPod, _ := operationtracker.GenerateBalloonPod(
		test.BuildTestNode(nodeName, 32000, 128*1024*1024),
		*resource.NewMilliQuantity(16000, resource.DecimalSI),
		*resource.NewQuantity(64*1024*1024, resource.DecimalSI),
		true) // Need UID or removePod doesn't work.

	smallPod := test_utils.BuildTestPod("pod-1", 1000, 1*1024*1024)

	allResizableMachineTypes := []string{
		"ek-standard-8",
		"ek-standard-16",
		"ek-standard-32",
		"e4a-standard-8",
		"e4a-standard-16",
		"e4a-standard-32",
	}

	testCases := []struct {
		name                    string
		nodegroups              []cloudprovider.NodeGroup
		existingPods            map[string][]*apiv1.Pod
		ekMachineTypesProvider  config.Provider[sets.Set[string]]
		resizingEnabledFamilies []string
		wantNonBpPods           map[string][]*apiv1.Pod
		wantNodegroups          []cloudprovider.NodeGroup
		wantBalloonPodResources map[string]map[v1.ResourceName]resource.Quantity
	}{
		{
			name:       "no_nodegroups",
			nodegroups: []cloudprovider.NodeGroup{},
		},
		{
			name: "no_ek_nodegroups",
			nodegroups: []cloudprovider.NodeGroup{
				e2Standard32Ng,
			},
			ekMachineTypesProvider:  config.NewSimpleStringSetProvider(allResizableMachineTypes),
			resizingEnabledFamilies: []string{machinetypes.EK.Name()},
			wantNodegroups: []cloudprovider.NodeGroup{
				e2Standard32Ng,
			},
		},
		{
			name: "ek_nodegroups",
			nodegroups: []cloudprovider.NodeGroup{
				ekStandard16Ng,
				ekStandard32Ng,
			},
			ekMachineTypesProvider:  config.NewSimpleStringSetProvider(allResizableMachineTypes),
			resizingEnabledFamilies: []string{machinetypes.EK.Name()},
			wantNodegroups: []cloudprovider.NodeGroup{
				ekStandard16Ng,
				ekStandard32Ng,
			},
			wantBalloonPodResources: map[string]map[v1.ResourceName]resource.Quantity{
				ekStandard16Ng.Id(): {
					v1.ResourceMemory: *resource.NewQuantity(16*size.GiB, resource.DecimalSI),
					v1.ResourceCPU:    *resource.NewMilliQuantity(4000, resource.DecimalSI),
				},
				ekStandard32Ng.Id(): {
					v1.ResourceMemory: *resource.NewQuantity(32*size.GiB, resource.DecimalSI),
					v1.ResourceCPU:    *resource.NewMilliQuantity(8000, resource.DecimalSI),
				},
			},
		},
		// TODO(b/464239880): add tests for resizable E4As
		{
			name: "e4a_nodegroups_resizing_disabled",
			nodegroups: []cloudprovider.NodeGroup{
				e4aStandard16Ng,
			},
			ekMachineTypesProvider:  config.NewSimpleStringSetProvider(allResizableMachineTypes),
			resizingEnabledFamilies: []string{machinetypes.EK.Name()},
			wantNodegroups: []cloudprovider.NodeGroup{
				e4aStandard16Ng,
			},
			wantBalloonPodResources: map[string]map[v1.ResourceName]resource.Quantity{
				e4aStandard16Ng.Id(): {},
			},
		},
		{
			name: "mixed_resizable_nodegroups_only_ek_resizing_enabled",
			nodegroups: []cloudprovider.NodeGroup{
				ekStandard16Ng,
				e4aStandard16Ng,
			},
			ekMachineTypesProvider:  config.NewSimpleStringSetProvider(allResizableMachineTypes),
			resizingEnabledFamilies: []string{machinetypes.EK.Name()},
			wantNodegroups: []cloudprovider.NodeGroup{
				ekStandard16Ng,
				e4aStandard16Ng,
			},
			wantBalloonPodResources: map[string]map[v1.ResourceName]resource.Quantity{
				ekStandard16Ng.Id(): {
					v1.ResourceMemory: *resource.NewQuantity(16*size.GiB, resource.DecimalSI),
					v1.ResourceCPU:    *resource.NewMilliQuantity(4000, resource.DecimalSI),
				},
				e4aStandard16Ng.Id(): {},
			},
		},
		{
			name: "ek_nodegroups_but_config_provider_is_nil",
			nodegroups: []cloudprovider.NodeGroup{
				ekStandard16Ng,
				ekStandard32Ng,
				e2Standard32Ng,
			},
			ekMachineTypesProvider:  nil,
			resizingEnabledFamilies: []string{machinetypes.EK.Name()},
			wantNodegroups: []cloudprovider.NodeGroup{
				e2Standard32Ng,
			},
			wantBalloonPodResources: map[string]map[v1.ResourceName]resource.Quantity{},
		},
		{
			name: "ek_nodegroups_but_only_ek_16_supported",
			nodegroups: []cloudprovider.NodeGroup{
				ekStandard16Ng,
				ekStandard32Ng,
			},
			ekMachineTypesProvider:  config.NewSimpleStringSetProvider([]string{"ek-standard-16"}),
			resizingEnabledFamilies: []string{machinetypes.EK.Name()},
			wantNodegroups: []cloudprovider.NodeGroup{
				ekStandard16Ng,
			},
			wantBalloonPodResources: map[string]map[v1.ResourceName]resource.Quantity{
				ekStandard16Ng.Id(): {
					v1.ResourceMemory: *resource.NewQuantity(16*size.GiB, resource.DecimalSI),
					v1.ResourceCPU:    *resource.NewMilliQuantity(4000, resource.DecimalSI),
				},
			},
		},
		{
			name: "ek_nodegroups_but_resizing_disabled",
			nodegroups: []cloudprovider.NodeGroup{
				ekStandard16Ng,
				ekStandard32Ng,
			},
			ekMachineTypesProvider:  config.NewSimpleStringSetProvider(allResizableMachineTypes),
			resizingEnabledFamilies: []string{},
			wantNodegroups: []cloudprovider.NodeGroup{
				ekStandard16Ng,
				ekStandard32Ng,
			},
			wantBalloonPodResources: map[string]map[v1.ResourceName]resource.Quantity{},
		},
		{
			name: "mixed_nodegroups",
			nodegroups: []cloudprovider.NodeGroup{
				ekStandard32Ng, e2Standard32Ng,
			},
			ekMachineTypesProvider:  config.NewSimpleStringSetProvider(allResizableMachineTypes),
			resizingEnabledFamilies: []string{machinetypes.EK.Name()},
			wantNodegroups: []cloudprovider.NodeGroup{
				ekStandard32Ng, e2Standard32Ng,
			},
			wantBalloonPodResources: map[string]map[v1.ResourceName]resource.Quantity{
				ekStandard32Ng.Id(): {
					v1.ResourceMemory: *resource.NewQuantity(32*size.GiB, resource.DecimalSI),
					v1.ResourceCPU:    *resource.NewMilliQuantity(8000, resource.DecimalSI),
				},
			},
		},
		{
			name: "balloon_pod_updated_if_exists_already",
			nodegroups: []cloudprovider.NodeGroup{
				ekStandard32Ng,
			},
			existingPods: map[string][]*apiv1.Pod{
				ekStandard32Ng.Id(): {bPod},
			},
			ekMachineTypesProvider:  config.NewSimpleStringSetProvider(allResizableMachineTypes),
			resizingEnabledFamilies: []string{machinetypes.EK.Name()},
			wantNodegroups: []cloudprovider.NodeGroup{
				ekStandard32Ng,
			},
			wantBalloonPodResources: map[string]map[v1.ResourceName]resource.Quantity{
				ekStandard32Ng.Id(): {
					v1.ResourceMemory: *resource.NewQuantity(32*size.GiB, resource.DecimalSI),
					v1.ResourceCPU:    *resource.NewMilliQuantity(8000, resource.DecimalSI),
				},
			},
		},
		{
			name: "other_pods_unaffected",
			nodegroups: []cloudprovider.NodeGroup{
				ekStandard32Ng,
			},
			existingPods: map[string][]*apiv1.Pod{
				ekStandard32Ng.Id(): {smallPod},
			},
			ekMachineTypesProvider:  config.NewSimpleStringSetProvider(allResizableMachineTypes),
			resizingEnabledFamilies: []string{machinetypes.EK.Name()},
			wantNodegroups: []cloudprovider.NodeGroup{
				ekStandard32Ng,
			},
			wantBalloonPodResources: map[string]map[v1.ResourceName]resource.Quantity{
				ekStandard32Ng.Id(): {
					v1.ResourceMemory: *resource.NewQuantity(32*size.GiB, resource.DecimalSI),
					v1.ResourceCPU:    *resource.NewMilliQuantity(8000, resource.DecimalSI),
				},
			},
			wantNonBpPods: map[string][]*apiv1.Pod{
				ekStandard32Ng.Id(): {smallPod},
			},
		},
		{
			name: "mixed_with_pods",
			nodegroups: []cloudprovider.NodeGroup{
				ekStandard32Ng,
				e2Standard32Ng,
			},
			existingPods: map[string][]*apiv1.Pod{
				ekStandard32Ng.Id(): {smallPod, bPod},
				e2Standard32Ng.Id(): {smallPod},
			},
			ekMachineTypesProvider:  config.NewSimpleStringSetProvider(allResizableMachineTypes),
			resizingEnabledFamilies: []string{machinetypes.EK.Name()},
			wantNodegroups: []cloudprovider.NodeGroup{
				ekStandard32Ng,
				e2Standard32Ng,
			},
			wantBalloonPodResources: map[string]map[v1.ResourceName]resource.Quantity{
				ekStandard32Ng.Id(): {
					v1.ResourceMemory: *resource.NewQuantity(32*size.GiB, resource.DecimalSI),
					v1.ResourceCPU:    *resource.NewMilliQuantity(8000, resource.DecimalSI),
				},
			},
			wantNonBpPods: map[string][]*apiv1.Pod{
				ekStandard32Ng.Id(): {smallPod},
				e2Standard32Ng.Id(): {smallPod},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nodegroupListProcessor := newMockNodeGroupListProcessor()
			mcp := machinetypes.NewMachineConfigProvider(nil)
			mockCalculator := calculator_test.NewWithProvider(mcp)
			mockResizingProvider := &mockResizingEnabledProvider{enabledFamilies: sets.New(tc.resizingEnabledFamilies...)}
			processor := NewNodeGroupListProcessor(nodegroupListProcessor, mockCalculator, tc.ekMachineTypesProvider, mockResizingProvider, mcp)

			// Prepare node infos.
			nodeInfos := map[string]*framework.NodeInfo{}
			for _, ng := range tc.nodegroups {
				mig, ok := ng.(*gke.GkeMig)
				assert.True(t, ok)

				machineType := mig.Spec().MachineType
				machineTypeInfo, err := mcp.ToMachineType(machineType)
				assert.NoError(t, err)

				node := test_utils.BuildTestNode("node-1", machineTypeInfo.CPU*1000, machineTypeInfo.Memory)
				node.Labels[apiv1.LabelInstanceTypeStable] = machineType
				nodeInfos[ng.Id()] = framework.NewTestNodeInfo(node, tc.existingPods[ng.Id()]...)
			}

			actual, _, _ := processor.Process(nil, tc.nodegroups, nodeInfos, nil)

			// Extract balloon pods from node infos.
			balloonPods := map[string]*v1.Pod{}
			for _, ng := range actual {
				balloonPods[ng.Id()] = getBalloonPod(nodeInfos[ng.Id()])
			}

			// Check if balloon pods were injected with correct sizes.
			for ngId, expectedResources := range tc.wantBalloonPodResources {
				if expectedResources == nil {
					continue
				}
				balloonPod := balloonPods[ngId]
				if len(expectedResources) == 0 {
					assert.Nil(t, balloonPod)
					continue
				}
				assert.NotNil(t, balloonPod)
				for resourceName, expectedResource := range expectedResources {
					assert.Equal(t, expectedResource, balloonPod.Spec.Containers[0].Resources.Requests[resourceName], "Error for resource %q", resourceName)
				}
			}

			// Check if nodeinfo non-bp pods stayed unchanged.
			for ngId, nonBpPods := range tc.wantNonBpPods {
				nodeInfoPods := make([]*apiv1.Pod, 0, len(nodeInfos[ngId].Pods()))
				for _, podInfo := range nodeInfos[ngId].Pods() {
					nodeInfoPods = append(nodeInfoPods, podInfo.Pod)
				}
				for _, pod := range nonBpPods {
					assert.Contains(t, nodeInfoPods, pod)
				}
			}

			assert.ElementsMatch(t, tc.wantNodegroups, actual)
		})
	}
}

type mockNodeGroupListProcessor struct {
}

func newMockNodeGroupListProcessor() *mockNodeGroupListProcessor {
	return &mockNodeGroupListProcessor{}
}

func (p *mockNodeGroupListProcessor) Process(ctx *context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup, nodeInfos map[string]*framework.NodeInfo, unschedulablePods []*apiv1.Pod) ([]cloudprovider.NodeGroup, map[string]*framework.NodeInfo, error) {
	return nodeGroups, nodeInfos, nil
}

func (m *mockNodeGroupListProcessor) CleanUp() {
}

type mockResizingEnabledProvider struct {
	enabledFamilies sets.Set[string]
}

func (p *mockResizingEnabledProvider) ResizingEnabled(machineFamily string) bool {
	return p.enabledFamilies.Has(machineFamily)
}

func getBalloonPod(nodeInfo *framework.NodeInfo) *v1.Pod {
	for _, podInfo := range nodeInfo.Pods() {
		if operationtracker.IsBalloonPod(podInfo.Pod) {
			return podInfo.Pod
		}
	}
	return nil
}
