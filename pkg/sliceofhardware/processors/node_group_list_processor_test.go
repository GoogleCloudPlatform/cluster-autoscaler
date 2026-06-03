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

package processors

import (
	"testing"

	"github.com/stretchr/testify/assert"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/callbacks"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/gkedebuggingsnapshot"
)

func TestProcessorE2E(t *testing.T) {
	// noOpProcessor early processor in "chain" which will just return input
	// nodeGroups and nodeInfos.
	noOpProcessor := &nodegroups.NoOpNodeGroupListProcessor{}
	sohwNodeGroupListProcessor := NewNodeGroupListProcessor(noOpProcessor)

	debuggingSnapshotter, _ := gkedebuggingsnapshot.NewGkeDebuggingSnapshotter(true)

	// Slice of hardware labels and extraResources used by slice of hardware nodes.
	sohwLabels := map[string]string{
		gkelabels.PodPerVMSizeLabel: "2",
		gkelabels.PodCapacityLabel:  "1",
	}
	sohwExtraResources := map[string]resource.Quantity{
		gkelabels.PodCapacityLabel: *resource.NewQuantity(1, resource.DecimalSI),
	}

	nodeInfos := map[string]*framework.NodeInfo{}
	// Node groups of different machine types.
	nonSohwC3Standard4 := buildMig("non-sohw1", "c3-standard-4", 4000, 16000, nil, nil)
	nodeInfos[nonSohwC3Standard4.Id()], _ = nonSohwC3Standard4.TemplateNodeInfo()
	nonSohwC3Standard8 := buildMig("non-sohw2", "c3-standard-8", 8000, 32000, nil, nil)
	nodeInfos[nonSohwC3Standard8.Id()], _ = nonSohwC3Standard8.TemplateNodeInfo()
	nonSohwC3Standard22 := buildMig("non-sohw2", "c3-standard-22", 22000, 64000, nil, nil)
	nodeInfos[nonSohwC3Standard22.Id()], _ = nonSohwC3Standard22.TemplateNodeInfo()
	// Slice of hardware nodeGroups
	sohwC3Standard4 := buildMig("sohw-1", "c3-standard-4", 4000, 16000, sohwLabels, sohwExtraResources)
	nodeInfos[sohwC3Standard4.Id()], _ = sohwC3Standard4.TemplateNodeInfo()
	sohwC3Standard8 := buildMig("sohw-2", "c3-standard-8", 8000, 32000, sohwLabels, sohwExtraResources)
	nodeInfos[sohwC3Standard8.Id()], _ = sohwC3Standard8.TemplateNodeInfo()
	sohwC3Standard22 := buildMig("sohw-3", "c3-standard-22", 22000, 64000, sohwLabels, sohwExtraResources)
	nodeInfos[sohwC3Standard22.Id()], _ = sohwC3Standard22.TemplateNodeInfo()
	sohwC3Standard44a := buildMig("sohw-4a", "c3-standard-44", 44000, 128000, sohwLabels, sohwExtraResources)
	nodeInfos[sohwC3Standard44a.Id()], _ = sohwC3Standard44a.TemplateNodeInfo()
	sohwC3Standard44b := buildMig("sohw-4b", "c3-standard-44", 44000, 128000, sohwLabels, sohwExtraResources)
	nodeInfos[sohwC3Standard44b.Id()], _ = sohwC3Standard44b.TemplateNodeInfo()

	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithMachineTypes("c3-standard-4", "c3-standard-8", "c3-standard-22", "c3-standard-44", "e2-standard-4", "e2-standard-8").
		WithMachineTemplates(nodeInfos).
		Build()

	// Pods used in tests
	normalPod := test.BuildTestPod("p1", 400, 100)
	// Slice of hardware pods
	sohwPod1 := test.BuildTestPod("sohw-pod1", 8500, 10000)
	sohwPod1.Spec.Containers[0].Resources.Requests[gkelabels.PodCapacityLabel] = *resource.NewQuantity(1, resource.DecimalSI)
	sohwPod2 := test.BuildTestPod("sohw-pod2", 3000, 5000)
	sohwPod2.Spec.Containers[0].Resources.Requests[gkelabels.PodCapacityLabel] = *resource.NewQuantity(1, resource.DecimalSI)
	sohwPod3 := test.BuildTestPod("sohw-pod3", 2000, 4000)
	sohwPod3.Spec.Containers[0].Resources.Requests[gkelabels.PodCapacityLabel] = *resource.NewQuantity(1, resource.DecimalSI)
	sohwPod4 := test.BuildTestPod("sohw-pod4", 25000, 100000)
	sohwPod4.Spec.Containers[0].Resources.Requests[gkelabels.PodCapacityLabel] = *resource.NewQuantity(1, resource.DecimalSI)

	testCases := []struct {
		name              string
		nodeGroups        []cloudprovider.NodeGroup
		unschedulablePods []*apiv1.Pod
		wantNodegroups    []cloudprovider.NodeGroup
	}{
		{
			name:              "No nodegroups, no nodeGroups returned",
			nodeGroups:        []cloudprovider.NodeGroup{},
			unschedulablePods: []*apiv1.Pod{},
			wantNodegroups:    []cloudprovider.NodeGroup{},
		},
		{
			name: "All non-slice-of-hardware nodeGroups returned",
			nodeGroups: []cloudprovider.NodeGroup{
				nonSohwC3Standard4,
				nonSohwC3Standard8,
				nonSohwC3Standard22,
			},
			unschedulablePods: []*apiv1.Pod{},
			wantNodegroups: []cloudprovider.NodeGroup{
				nonSohwC3Standard4,
				nonSohwC3Standard8,
				nonSohwC3Standard22,
			},
		},
		{
			name: "slice of hardware nodeGroups with slice of hardware pod 1",
			nodeGroups: []cloudprovider.NodeGroup{
				nonSohwC3Standard4,
				nonSohwC3Standard8,
				sohwC3Standard4,
				sohwC3Standard8,
				sohwC3Standard22,
			},
			unschedulablePods: []*apiv1.Pod{normalPod, sohwPod1}, // sohw pod can only fit on c3-standard-22
			wantNodegroups: []cloudprovider.NodeGroup{
				nonSohwC3Standard4,
				nonSohwC3Standard8,
				sohwC3Standard22,
			},
		},
		{
			name: "slice of hardware nodeGroups with slice of hardware pod 2",
			nodeGroups: []cloudprovider.NodeGroup{
				nonSohwC3Standard4,
				nonSohwC3Standard8,
				sohwC3Standard4,
				sohwC3Standard8,
				sohwC3Standard22,
			},
			unschedulablePods: []*apiv1.Pod{normalPod, sohwPod2}, // sohw pod can fit on c3-standard-4
			wantNodegroups: []cloudprovider.NodeGroup{
				nonSohwC3Standard4,
				nonSohwC3Standard8,
				sohwC3Standard4,
			},
		},
		{
			name: "slice of hardware pods can fit on different smallest node groups",
			nodeGroups: []cloudprovider.NodeGroup{
				nonSohwC3Standard4,
				nonSohwC3Standard8,
				sohwC3Standard4,
				sohwC3Standard8,
				sohwC3Standard22,
			},
			unschedulablePods: []*apiv1.Pod{sohwPod1, sohwPod2},
			wantNodegroups: []cloudprovider.NodeGroup{
				nonSohwC3Standard4,
				nonSohwC3Standard8,
				sohwC3Standard4,
				sohwC3Standard22,
			},
		},
		{
			name: "similar slice of hardware pods can fit on same smallest node groups",
			nodeGroups: []cloudprovider.NodeGroup{
				nonSohwC3Standard4,
				nonSohwC3Standard8,
				sohwC3Standard4,
				sohwC3Standard8,
				sohwC3Standard22,
			},
			unschedulablePods: []*apiv1.Pod{sohwPod2, sohwPod3},
			wantNodegroups: []cloudprovider.NodeGroup{
				nonSohwC3Standard4,
				nonSohwC3Standard8,
				sohwC3Standard4,
			},
		},
		{
			name: "slice of hardware pod can fit on same machine type node groups",
			nodeGroups: []cloudprovider.NodeGroup{
				nonSohwC3Standard4,
				nonSohwC3Standard8,
				sohwC3Standard4,
				sohwC3Standard8,
				sohwC3Standard22,
				sohwC3Standard44a,
				sohwC3Standard44b,
			},
			unschedulablePods: []*apiv1.Pod{sohwPod4},
			wantNodegroups: []cloudprovider.NodeGroup{
				nonSohwC3Standard4,
				nonSohwC3Standard8,
				sohwC3Standard44a,
				sohwC3Standard44b,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &context.AutoscalingContext{
				CloudProvider:        provider,
				ProcessorCallbacks:   callbacks.NewTestProcessorCallbacks(),
				ClusterSnapshot:      testsnapshot.NewTestSnapshotOrDie(t),
				DebuggingSnapshotter: debuggingSnapshotter,
			}

			gotNodeGroups, _, gotError := sohwNodeGroupListProcessor.Process(ctx, tc.nodeGroups, nodeInfos, tc.unschedulablePods)
			assert.NoError(t, gotError)
			assert.Equal(t, len(tc.wantNodegroups), len(gotNodeGroups))
			// Processor should return the correct node groups.
			assert.ElementsMatch(t, tc.wantNodegroups, gotNodeGroups)
		})
	}
}

func buildMig(nodePoolName, machineType string, mCpu int64, mem int64, labels map[string]string, extraResources map[string]resource.Quantity) *gke.GkeMig {
	mockGkeManager := &gke.GkeManagerMock{}
	mig := gke.NewTestGkeMigBuilder().
		SetSpec(&gkeclient.NodePoolSpec{MachineType: machineType, Labels: labels}).
		SetGkeManager(mockGkeManager).
		SetExtraResources(extraResources).
		SetNodePoolName(nodePoolName).
		SetGceRefName(nodePoolName).
		Build()

	node := test.BuildTestNode(nodePoolName+"-xxx", mCpu, mem)
	node.Labels = labels
	for resourceName, resourceValue := range extraResources {
		node.Status.Allocatable[apiv1.ResourceName(resourceName)] = resourceValue
	}
	mockGkeManager.On("GetMigTemplateNodeInfo", mig).Return(framework.NewTestNodeInfo(node), nil)
	return mig
}
