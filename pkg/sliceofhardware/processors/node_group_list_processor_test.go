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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"

	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/gkedebuggingsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/callbacks"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodegroups"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
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
	nodeInfos[nonSohwC3Standard4.Id()], _ = nonSohwC3Standard4.TemplateNodeInfo(context.TODO())
	nonSohwC3Standard8 := buildMig("non-sohw2", "c3-standard-8", 8000, 32000, nil, nil)
	nodeInfos[nonSohwC3Standard8.Id()], _ = nonSohwC3Standard8.TemplateNodeInfo(context.TODO())
	nonSohwC3Standard22 := buildMig("non-sohw2", "c3-standard-22", 22000, 64000, nil, nil)
	nodeInfos[nonSohwC3Standard22.Id()], _ = nonSohwC3Standard22.TemplateNodeInfo(context.TODO())
	// Slice of hardware nodeGroups
	sohwC3Standard4 := buildMig("sohw-1", "c3-standard-4", 4000, 16000, sohwLabels, sohwExtraResources)
	nodeInfos[sohwC3Standard4.Id()], _ = sohwC3Standard4.TemplateNodeInfo(context.TODO())
	sohwC3Standard8 := buildMig("sohw-2", "c3-standard-8", 8000, 32000, sohwLabels, sohwExtraResources)
	nodeInfos[sohwC3Standard8.Id()], _ = sohwC3Standard8.TemplateNodeInfo(context.TODO())
	sohwC3Standard22 := buildMig("sohw-3", "c3-standard-22", 22000, 64000, sohwLabels, sohwExtraResources)
	nodeInfos[sohwC3Standard22.Id()], _ = sohwC3Standard22.TemplateNodeInfo(context.TODO())
	sohwC3Standard44a := buildMig("sohw-4a", "c3-standard-44", 44000, 128000, sohwLabels, sohwExtraResources)
	nodeInfos[sohwC3Standard44a.Id()], _ = sohwC3Standard44a.TemplateNodeInfo(context.TODO())
	sohwC3Standard44b := buildMig("sohw-4b", "c3-standard-44", 44000, 128000, sohwLabels, sohwExtraResources)
	nodeInfos[sohwC3Standard44b.Id()], _ = sohwC3Standard44b.TemplateNodeInfo(context.TODO())

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
			ctx := &ca_context.AutoscalingContext{
				CloudProvider:        provider,
				ProcessorCallbacks:   callbacks.NewTestProcessorCallbacks(),
				ClusterSnapshot:      testsnapshot.NewTestSnapshotOrDie(t),
				DebuggingSnapshotter: debuggingSnapshotter,
			}

			gotNodeGroups, _, gotError := sohwNodeGroupListProcessor.Process(context.TODO(), ctx, tc.nodeGroups, nodeInfos, tc.unschedulablePods)
			assert.NoError(t, gotError)
			assert.Equal(t, len(tc.wantNodegroups), len(gotNodeGroups))
			// Processor should return the correct node groups.
			assert.ElementsMatch(t, tc.wantNodegroups, gotNodeGroups)
		})
	}
}

func TestProcess_FittingNodeGroupNotFilteredOutByTopologySpread(t *testing.T) {
	noOpProcessor := &nodegroups.NoOpNodeGroupListProcessor{}
	sohwNodeGroupListProcessor := NewNodeGroupListProcessor(noOpProcessor)
	debuggingSnapshotter, _ := gkedebuggingsnapshot.NewGkeDebuggingSnapshotter(true)

	sohwExtraResources := map[string]resource.Quantity{
		gkelabels.PodCapacityLabel: *resource.NewQuantity(1, resource.DecimalSI),
	}
	zoneALabels := map[string]string{
		gkelabels.PodPerVMSizeLabel: "2",
		gkelabels.PodCapacityLabel:  "1",
		apiv1.LabelTopologyZone:     "zone-a",
	}
	zoneBLabels := map[string]string{
		gkelabels.PodPerVMSizeLabel: "2",
		gkelabels.PodCapacityLabel:  "1",
		apiv1.LabelTopologyZone:     "zone-b",
	}

	nodeInfos := map[string]*framework.NodeInfo{}
	// sohwZoneBSmall is in zone-b with only 4000m CPU, so pending-sohw-pod (8500m CPU)
	// cannot fit on it due to insufficient CPU.
	sohwZoneBSmall := buildMig("sohw-zone-b-small", "c3-standard-4", 4000, 16000, zoneBLabels, sohwExtraResources)
	nodeInfos[sohwZoneBSmall.Id()], _ = sohwZoneBSmall.TemplateNodeInfo(context.TODO())
	// sohwZoneALarge is in zone-a with 22000m CPU, which is large enough to fit pending-sohw-pod.
	sohwZoneALarge := buildMig("sohw-zone-a-large", "c3-standard-22", 22000, 64000, zoneALabels, sohwExtraResources)
	nodeInfos[sohwZoneALarge.Id()], _ = sohwZoneALarge.TemplateNodeInfo(context.TODO())

	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithMachineTypes("c3-standard-4", "c3-standard-22").
		WithMachineTemplates(nodeInfos).
		Build()

	// The cluster currently has one node in zone-a running one matching pod (app=sohw),
	// and zero nodes in zone-b.
	snapshot := testsnapshot.NewTestSnapshotOrDie(t)
	existingNode := test.BuildTestNode("existing-zone-a", 22000, 64000)
	existingNode.Labels = map[string]string{apiv1.LabelTopologyZone: "zone-a"}
	existingPod := test.BuildTestPod("existing-sohw-pod", 8500, 10000)
	existingPod.Labels = map[string]string{"app": "sohw"}
	existingPod.Spec.NodeName = existingNode.Name
	assert.NoError(t, snapshot.AddNodeInfo(framework.NewTestNodeInfo(existingNode, existingPod)))

	// pending-sohw-pod requires 8500m CPU and has a DoNotSchedule TopologySpreadConstraint
	// with MaxSkew=1 across zones.
	// - When only zone-a nodes exist in ClusterSnapshot, scheduling pending-sohw-pod on
	//   sohwZoneALarge (zone-a) satisfies PodTopologySpread (zone-a is the only zone domain).
	// - However, if sohwZoneBSmall's template node (in zone-b, with 0 matching pods) is left
	//   in ClusterSnapshot after failing the CPU check, zone-b becomes a second topology domain
	//   with 0 matching pods. Scheduling pending-sohw-pod in zone-a would then result in
	//   2 pods in zone-a vs 0 pods in zone-b (skew = 2 > MaxSkew = 1), causing PodTopologySpread
	//   to reject sohwZoneALarge and filtering it out completely.
	unschedulablePod := test.BuildTestPod("pending-sohw-pod", 8500, 10000)
	unschedulablePod.Labels = map[string]string{"app": "sohw"}
	unschedulablePod.Spec.Containers[0].Resources.Requests[gkelabels.PodCapacityLabel] = *resource.NewQuantity(1, resource.DecimalSI)
	unschedulablePod.Spec.TopologySpreadConstraints = []apiv1.TopologySpreadConstraint{
		{
			MaxSkew:           1,
			TopologyKey:       apiv1.LabelTopologyZone,
			WhenUnsatisfiable: apiv1.DoNotSchedule,
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "sohw"},
			},
		},
	}

	autoscalingCtx := &ca_context.AutoscalingContext{
		CloudProvider:        provider,
		ProcessorCallbacks:   callbacks.NewTestProcessorCallbacks(),
		ClusterSnapshot:      snapshot,
		DebuggingSnapshotter: debuggingSnapshotter,
	}

	// 1. Baseline: when sohwZoneALarge is evaluated without an earlier zone-b candidate,
	// it passes PodTopologySpread and is returned as a fitting Slice-of-Hardware node group.
	gotAlone, _, err := sohwNodeGroupListProcessor.Process(
		context.TODO(),
		autoscalingCtx,
		[]cloudprovider.NodeGroup{sohwZoneALarge},
		nodeInfos,
		[]*apiv1.Pod{unschedulablePod},
	)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []cloudprovider.NodeGroup{sohwZoneALarge}, gotAlone)

	// 2. Bug reproduction: when undersized sohwZoneBSmall (zone-b) is evaluated before
	// fitting sohwZoneALarge (zone-a), sohwZoneBSmall's template node must not remain in
	// ClusterSnapshot and cause sohwZoneALarge to fail PodTopologySpread and be filtered out.
	gotAfterZoneBCandidate, _, err := sohwNodeGroupListProcessor.Process(
		context.TODO(),
		autoscalingCtx,
		[]cloudprovider.NodeGroup{sohwZoneBSmall, sohwZoneALarge},
		nodeInfos,
		[]*apiv1.Pod{unschedulablePod},
	)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []cloudprovider.NodeGroup{sohwZoneALarge}, gotAfterZoneBCandidate)
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
