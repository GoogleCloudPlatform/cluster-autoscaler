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

package edp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
)

func TestEdpExpander(t *testing.T) {

	n1 := BuildTestNode("n1", 4000, 1000)
	n2 := BuildTestNode("n2", 4000, 1000)
	n3 := BuildTestNode("n3", 4000, 1000)
	n4 := BuildTestNode("n4", 4000, 1000)
	np1 := BuildTestNode("np1", 4000, 1000)
	np2 := BuildTestNode("np2", 4000, 1000)

	n2.Labels = map[string]string{
		gkelabels.ExtendedDurationPodsLabel: "1000m",
	}
	n3.Labels = map[string]string{
		gkelabels.ExtendedDurationPodsLabel: "1000m",
	}
	n4.Labels = map[string]string{
		gkelabels.ExtendedDurationPodsLabel: "500m",
	}
	np1.Labels = map[string]string{
		gkelabels.ExtendedDurationPodsLabel: gkelabels.ExtendedDurationPackedPodsValue,
	}
	np2.Labels = map[string]string{
		gkelabels.ExtendedDurationPodsLabel: gkelabels.ExtendedDurationPackedPodsValue,
	}

	regularPod1 := BuildTestPod("regularPod1", 1000, 0)
	regularPod2 := BuildTestPod("regularPod2", 500, 0)
	edpPod1000_1 := BuildTestPod("edpPod1000_1", 1000, 0)
	edpPod1000_2 := BuildTestPod("edpPod1000_2", 1000, 0)
	edpPod500_1 := BuildTestPod("edpPod500_1", 500, 0)
	edpPod500_2 := BuildTestPod("edpPod500_2", 500, 0)
	edpPodPacked_1 := BuildTestPod("edpPodPacked_1", 500, 0)
	edpPodPacked_2 := BuildTestPod("edpPodPacked_2", 1500, 0)

	edpPod1000_1.Spec.NodeSelector = map[string]string{
		gkelabels.ExtendedDurationPodsLabel: "1000m",
	}
	edpPod1000_2.Spec.NodeSelector = map[string]string{
		gkelabels.ExtendedDurationPodsLabel: "1000m",
	}
	edpPod500_1.Spec.NodeSelector = map[string]string{
		gkelabels.ExtendedDurationPodsLabel: "500m",
	}
	edpPod500_2.Spec.NodeSelector = map[string]string{
		gkelabels.ExtendedDurationPodsLabel: "500m",
	}
	edpPodPacked_1.Spec.NodeSelector = map[string]string{
		gkelabels.ExtendedDurationPodsLabel: gkelabels.ExtendedDurationPackedPodsValue,
	}
	edpPodPacked_2.Spec.NodeSelector = map[string]string{
		gkelabels.ExtendedDurationPodsLabel: gkelabels.ExtendedDurationPackedPodsValue,
	}

	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		Build()
	testMig1 := gke.NewTestGkeMigBuilder().SetGceRefName("ng1").SetSpec(&gkeclient.NodePoolSpec{MachineType: "n1-standard-4"}).Build()
	testMig2 := gke.NewTestGkeMigBuilder().SetGceRefName("ng2").SetSpec(&gkeclient.NodePoolSpec{MachineType: "e2-medium"}).Build()
	testMig3 := gke.NewTestGkeMigBuilder().SetGceRefName("ng3").SetSpec(&gkeclient.NodePoolSpec{MachineType: "e2-highcpu-2"}).Build()
	testMig4 := gke.NewTestGkeMigBuilder().SetGceRefName("ng4").SetSpec(&gkeclient.NodePoolSpec{MachineType: "n1-standard-4"}).Build()
	testMigP1 := gke.NewTestGkeMigBuilder().SetGceRefName("ngp1").SetSpec(&gkeclient.NodePoolSpec{MachineType: "n1-standard-8"}).Build()
	testMigP2 := gke.NewTestGkeMigBuilder().SetGceRefName("ngp2").SetSpec(&gkeclient.NodePoolSpec{MachineType: "n1-standard-32"}).Build()
	provider.InsertNodeGroup(testMig1)
	provider.InsertNodeGroup(testMig2)
	provider.InsertNodeGroup(testMig3)
	provider.InsertNodeGroup(testMig4)
	provider.InsertNodeGroup(testMigP1)
	provider.InsertNodeGroup(testMigP2)
	provider.AddNode(testMig1.Id(), n1)
	provider.AddNode(testMig2.Id(), n2)
	provider.AddNode(testMig3.Id(), n3)
	provider.AddNode(testMig4.Id(), n4)
	provider.AddNode(testMigP1.Id(), np1)
	provider.AddNode(testMigP2.Id(), np2)

	ng1, _ := provider.NodeGroupForNode(n1)
	ng2, _ := provider.NodeGroupForNode(n2)
	ng3, _ := provider.NodeGroupForNode(n3)
	ng4, _ := provider.NodeGroupForNode(n4)
	ngp1, _ := provider.NodeGroupForNode(np1)
	ngp2, _ := provider.NodeGroupForNode(np2)

	ni1 := framework.NewTestNodeInfo(n1)
	ni2 := framework.NewTestNodeInfo(n2)
	ni3 := framework.NewTestNodeInfo(n3)
	ni4 := framework.NewTestNodeInfo(n4)
	nip1 := framework.NewTestNodeInfo(np1)
	nip2 := framework.NewTestNodeInfo(np2)
	nodeInfosForGroups := map[string]*framework.NodeInfo{
		testMig1.Id():  ni1,
		testMig2.Id():  ni2,
		testMig3.Id():  ni3,
		testMig4.Id():  ni4,
		testMigP1.Id(): nip1,
		testMigP2.Id(): nip2,
	}

	testCases := map[string]struct {
		inputOptions []expander.Option
		wantIndexes  []int
	}{
		"All options have non-edp pods": {
			inputOptions: []expander.Option{
				{
					NodeGroup: ng1,
					Pods:      []*apiv1.Pod{regularPod1, regularPod2},
				},
				{
					NodeGroup: ng1,
					Pods:      []*apiv1.Pod{regularPod1, regularPod2},
				},
			},
			wantIndexes: []int{0, 1},
		},
		"Only one option has edp pods and epd nodes": {
			inputOptions: []expander.Option{
				{
					NodeGroup: ng1,
					Pods:      []*apiv1.Pod{regularPod1, regularPod2},
				},
				{
					NodeGroup: ng2,
					Pods:      []*apiv1.Pod{regularPod1, regularPod2},
				},
				{
					NodeGroup: ng2,
					Pods:      []*apiv1.Pod{edpPod1000_1, edpPod1000_2},
				},
			},
			wantIndexes: []int{0, 2},
		},
		"Few edp pod options belongs to same machine type and same node selector value": {
			inputOptions: []expander.Option{
				{
					NodeGroup: ng1,
					Pods:      []*apiv1.Pod{regularPod1, regularPod2},
				},
				{
					NodeGroup: ng2,
					Pods:      []*apiv1.Pod{regularPod1, regularPod2},
				},
				{
					NodeGroup: ng2,
					Pods:      []*apiv1.Pod{edpPod1000_1, edpPod1000_2},
				},
				{
					NodeGroup: ng2,
					Pods:      []*apiv1.Pod{regularPod1, edpPod1000_2},
				},
				{
					NodeGroup: ng2,
					Pods:      []*apiv1.Pod{regularPod2, edpPod1000_1},
				},
			},
			wantIndexes: []int{0, 2, 3, 4},
		},
		"Few edp pod options belongs to different machine types and same node selector value": {
			inputOptions: []expander.Option{
				{
					NodeGroup: ng1,
					Pods:      []*apiv1.Pod{regularPod1, regularPod2},
				},
				{
					NodeGroup: ng2,
					Pods:      []*apiv1.Pod{regularPod1, regularPod2},
				},
				{
					NodeGroup: ng2,
					Pods:      []*apiv1.Pod{edpPod1000_1, edpPod1000_2},
				},
				{
					NodeGroup: ng3,
					Pods:      []*apiv1.Pod{edpPod1000_1, edpPod1000_2},
				},
			},
			wantIndexes: []int{0, 2},
		},
		"Few edp pod options belongs to different machine types and different node selector values": {
			inputOptions: []expander.Option{
				{
					NodeGroup: ng1,
					Pods:      []*apiv1.Pod{regularPod1, regularPod2},
				},
				{
					NodeGroup: ng2,
					Pods:      []*apiv1.Pod{regularPod1, regularPod2},
				},
				{
					NodeGroup: ng2,
					Pods:      []*apiv1.Pod{edpPod1000_1, edpPod1000_2},
				},
				{
					NodeGroup: ng3,
					Pods:      []*apiv1.Pod{edpPod1000_1, edpPod1000_2},
				},
				{
					NodeGroup: ng4,
					Pods:      []*apiv1.Pod{edpPod500_1, edpPod500_2},
				},
				{
					NodeGroup: ng4,
					Pods:      []*apiv1.Pod{regularPod1, edpPod500_2},
				},
			},
			wantIndexes: []int{0, 2, 4, 5},
		},
		"Packed EDPs": {
			inputOptions: []expander.Option{
				{
					NodeGroup: ng1,
					Pods:      []*apiv1.Pod{regularPod1, regularPod2},
				},
				{
					NodeGroup: ngp1,
					Pods:      []*apiv1.Pod{regularPod1, regularPod2},
				},
				{
					NodeGroup: ngp1,
					Pods:      []*apiv1.Pod{edpPodPacked_1, edpPodPacked_2},
				},
				{
					NodeGroup: ngp1,
					Pods:      []*apiv1.Pod{edpPodPacked_1, regularPod1},
				},
				{
					NodeGroup: ngp2,
					Pods:      []*apiv1.Pod{edpPodPacked_1, edpPodPacked_2},
				},
			},
			wantIndexes: []int{0, 2, 3, 4},
		},
		"Packed EDPs mixed with normal EDPs": {
			inputOptions: []expander.Option{
				{
					NodeGroup: ng1,
					Pods:      []*apiv1.Pod{regularPod1, regularPod2},
				},
				{
					NodeGroup: ng2,
					Pods:      []*apiv1.Pod{edpPod1000_1, edpPod1000_2},
				},
				{
					NodeGroup: ng3,
					Pods:      []*apiv1.Pod{edpPod1000_1, edpPod1000_2},
				},
				{
					NodeGroup: ngp1,
					Pods:      []*apiv1.Pod{regularPod1, regularPod2},
				},
				{
					NodeGroup: ngp1,
					Pods:      []*apiv1.Pod{edpPodPacked_1, edpPodPacked_2},
				},
				{
					NodeGroup: ngp1,
					Pods:      []*apiv1.Pod{edpPodPacked_1, regularPod1},
				},
				{
					NodeGroup: ngp2,
					Pods:      []*apiv1.Pod{edpPodPacked_1, edpPodPacked_2},
				},
			},
			wantIndexes: []int{0, 1, 4, 5, 6},
		},
	}
	// EDP filter expander enabled
	for tn, tc := range testCases {
		wantOptions := []expander.Option{}
		for _, wantIndex := range tc.wantIndexes {
			wantOptions = append(wantOptions, tc.inputOptions[wantIndex])
		}
		t.Run(tn, func(t *testing.T) {
			assert.ElementsMatch(t, NewEdpFilter(provider).BestOptions(tc.inputOptions, nodeInfosForGroups), wantOptions)
		})
	}
}
