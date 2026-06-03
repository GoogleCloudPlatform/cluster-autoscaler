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

package mppn

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	gke_api_beta "google.golang.org/api/container/v1beta1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	test_util "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/gkeprice"
)

func TestMppnFilter_BestOptions(t *testing.T) {
	nodeInfos := make(map[string]*framework.NodeInfo)
	newNg32Mppn := createGkeMig("newNg32Mppn", &gkeclient.NodePoolSpec{MaxPodsPerNode: 32}, false, nodeInfos)
	existingNg64Mppn := createGkeMig("existingNg64Mppn", &gkeclient.NodePoolSpec{MaxPodsPerNode: 64}, true, nodeInfos)
	newAutopilotNg32Mppn := createGkeMig("newNg32Mppn", &gkeclient.NodePoolSpec{MaxPodsPerNode: 32, AutopilotManaged: true}, false, nodeInfos)
	existingAutopilotNg64Mppn := createGkeMig("existingNg64Mppn", &gkeclient.NodePoolSpec{MaxPodsPerNode: 64, AutopilotManaged: true}, true, nodeInfos)
	acceleratorA1Ng := createGkeMig("a1", &gkeclient.NodePoolSpec{
		Accelerators: []*gke_api_beta.AcceleratorConfig{
			{
				AcceleratorCount: 2,
				AcceleratorType:  "a1",
				GpuPartitionSize: "p1",
			},
		},
		MaxPodsPerNode: 32}, true, nodeInfos)
	localSSDNg := createGkeMig("localssd", &gkeclient.NodePoolSpec{LocalSSDConfig: &gkeclient.LocalSSDConfig{
		LocalSsdCount: 1,
	}}, false, nodeInfos)

	for desc, tc := range map[string]struct {
		inOptions          []expander.Option
		wantOptions        []expander.Option
		poolCount          int
		locations          int
		isAutopilotCluster bool
	}{
		"different node count in option, none filtered out": {
			poolCount:          1,
			locations:          1,
			isAutopilotCluster: true,
			inOptions: []expander.Option{
				{
					NodeGroup: existingNg64Mppn,
					Pods:      createPods(60),
					NodeCount: 20,
				},
				{
					NodeGroup: newNg32Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
			},
			wantOptions: []expander.Option{
				{
					NodeGroup: existingNg64Mppn,
					Pods:      createPods(60),
					NodeCount: 20,
				},
				{
					NodeGroup: newNg32Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
			},
		},
		"different node group in options, none filtered out": {
			poolCount:          1,
			locations:          1,
			isAutopilotCluster: true,
			inOptions: []expander.Option{
				{
					NodeGroup: localSSDNg,
					Pods:      createPods(60),
					NodeCount: 20,
				},
				{
					NodeGroup: newNg32Mppn,
					Pods:      createPods(60),
					NodeCount: 20,
				},
				{
					NodeGroup: acceleratorA1Ng,
					Pods:      createPods(60),
					NodeCount: 20,
				},
			},
			wantOptions: []expander.Option{
				{
					NodeGroup: localSSDNg,
					Pods:      createPods(60),
					NodeCount: 20,
				},
				{
					NodeGroup: newNg32Mppn,
					Pods:      createPods(60),
					NodeCount: 20,
				},
				{
					NodeGroup: acceleratorA1Ng,
					Pods:      createPods(60),
					NodeCount: 20,
				},
			},
		},
		"similar options with different mppn, 2 existing nodes, regional cluster, standard node group, none filtered out": {
			poolCount:          1,
			locations:          2,
			isAutopilotCluster: false,
			inOptions: []expander.Option{
				{
					NodeGroup: existingNg64Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
				{
					NodeGroup: newNg32Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
			},
			wantOptions: []expander.Option{
				{
					NodeGroup: existingNg64Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
				{
					NodeGroup: newNg32Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
			},
		},
		"similar options with different mppn, 2 existing nodes, regional cluster, autopilot managed node group, one filtered out": {
			poolCount:          1,
			locations:          2,
			isAutopilotCluster: false,
			inOptions: []expander.Option{
				{
					NodeGroup: existingAutopilotNg64Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
				{
					NodeGroup: newAutopilotNg32Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
			},
			wantOptions: []expander.Option{
				{
					NodeGroup: newAutopilotNg32Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
			},
		},
		"similar options with different mppn, 2 existing nodes, regional cluster, autopilot managed and standard node groups mixed, only ap filtered out": {
			poolCount:          1,
			locations:          2,
			isAutopilotCluster: false,
			inOptions: []expander.Option{
				{
					NodeGroup: newNg32Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
				{
					NodeGroup: newAutopilotNg32Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
				{
					NodeGroup: existingNg64Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
				{
					NodeGroup: existingAutopilotNg64Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
			},
			wantOptions: []expander.Option{
				{
					NodeGroup: newNg32Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
				{
					NodeGroup: newAutopilotNg32Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
				{
					NodeGroup: existingNg64Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
			},
		},
		"similar options with different mppn, 2 existing nodes, regional cluster": {
			poolCount:          1,
			locations:          2,
			isAutopilotCluster: true,
			inOptions: []expander.Option{
				{
					NodeGroup: existingNg64Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
				{
					NodeGroup: newNg32Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
			},
			wantOptions: []expander.Option{
				{
					NodeGroup: newNg32Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
			},
		},
		"similar options with different mppn, 2 existing nodes, zonal cluster": {
			poolCount:          2,
			locations:          1,
			isAutopilotCluster: true,
			inOptions: []expander.Option{
				{
					NodeGroup: existingNg64Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
				{
					NodeGroup: newNg32Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
			},
			wantOptions: []expander.Option{
				{
					NodeGroup: newNg32Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
			},
		},
		"similar options with different mppn, 10 existing nodes, regional cluster": {
			poolCount:          5,
			locations:          2,
			isAutopilotCluster: true,
			inOptions: []expander.Option{
				{
					NodeGroup: existingNg64Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
				{
					NodeGroup: newNg32Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
			},
			wantOptions: []expander.Option{
				{
					NodeGroup: newNg32Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
			},
		},
		"similar options with different mppn, 10 existing nodes, zonal cluster": {
			poolCount:          10,
			locations:          1,
			isAutopilotCluster: true,
			inOptions: []expander.Option{
				{
					NodeGroup: existingNg64Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
				{
					NodeGroup: newNg32Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
			},
			wantOptions: []expander.Option{
				{
					NodeGroup: existingNg64Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
			},
		},
		"similar options with different mppn, 30 existing nodes, regional cluster": {
			poolCount:          15,
			locations:          2,
			isAutopilotCluster: true,
			inOptions: []expander.Option{
				{
					NodeGroup: existingNg64Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
				{
					NodeGroup: newNg32Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
			},
			wantOptions: []expander.Option{
				{
					NodeGroup: existingNg64Mppn,
					Pods:      createPods(60),
					NodeCount: 2,
				},
			},
		},
		"500 pending pods, 2 existing nodes, regional cluster": {
			poolCount:          1,
			locations:          2,
			isAutopilotCluster: true,
			inOptions: []expander.Option{
				{
					NodeGroup: existingNg64Mppn,
					Pods:      createPods(500),
					NodeCount: 20,
				},
				{
					NodeGroup: newNg32Mppn,
					Pods:      createPods(500),
					NodeCount: 20,
				},
			},
			wantOptions: []expander.Option{
				{
					NodeGroup: newNg32Mppn,
					Pods:      createPods(500),
					NodeCount: 20,
				},
			},
		},
		"500 pending pods, 30 existing nodes, regional cluster": {
			poolCount:          15,
			locations:          2,
			isAutopilotCluster: true,
			inOptions: []expander.Option{
				{
					NodeGroup: existingNg64Mppn,
					Pods:      createPods(500),
					NodeCount: 20,
				},
				{
					NodeGroup: newNg32Mppn,
					Pods:      createPods(500),
					NodeCount: 20,
				},
			},
			wantOptions: []expander.Option{
				{
					NodeGroup: newNg32Mppn,
					Pods:      createPods(500),
					NodeCount: 20,
				},
			},
		},
		"500 pending pods, 30 existing nodes, zonal cluster": {
			poolCount:          30,
			locations:          1,
			isAutopilotCluster: true,
			inOptions: []expander.Option{
				{
					NodeGroup: existingNg64Mppn,
					Pods:      createPods(500),
					NodeCount: 20,
				},
				{
					NodeGroup: newNg32Mppn,
					Pods:      createPods(500),
					NodeCount: 20,
				},
			},
			wantOptions: []expander.Option{
				{
					NodeGroup: existingNg64Mppn,
					Pods:      createPods(500),
					NodeCount: 20,
				},
			},
		},
		"500 pending pods, 70 existing nodes, regional cluster": {
			poolCount:          35,
			locations:          2,
			isAutopilotCluster: true,
			inOptions: []expander.Option{
				{
					NodeGroup: existingNg64Mppn,
					Pods:      createPods(500),
					NodeCount: 20,
				},
				{
					NodeGroup: newNg32Mppn,
					Pods:      createPods(500),
					NodeCount: 20,
				},
			},
			wantOptions: []expander.Option{
				{
					NodeGroup: existingNg64Mppn,
					Pods:      createPods(500),
					NodeCount: 20,
				},
			},
		},
	} {
		tc := tc
		t.Run(desc, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().Build()
			for i := 0; i < tc.poolCount; i++ {
				machineType := fmt.Sprintf("n1-standard-%d", i+1)
				for j := 0; j < tc.locations; j++ {
					groupId := fmt.Sprintf("%s-zone%d", machineType, j)
					provider.AddAutoprovisionedGkeNodeGroup(machineType, groupId, 0, true, false, machineType, false, true)
				}
			}
			reducer := gkeprice.NewProgressiveGroupCountReducer(provider)
			mf := NewFilter(reducer, tc.isAutopilotCluster)
			gotOptions := mf.BestOptions(tc.inOptions, nodeInfos)
			if diff := cmp.Diff(tc.wantOptions, gotOptions, cmpopts.IgnoreUnexported(gke.FakeGkeManager{}), cmp.AllowUnexported(gke.GkeMig{}), cmp.AllowUnexported(gke.GkeNodePool{}), cmpopts.SortSlices(optionsSortFunc)); diff != "" {
				t.Errorf("Unexpected diff: %v", diff)
			}
		})
	}
}

func createGkeMig(name string, spec *gkeclient.NodePoolSpec, exists bool, nodeInfos map[string]*framework.NodeInfo) *gke.GkeMig {
	mig := gke.NewTestGkeMigBuilder().SetSpec(spec).SetExist(exists).SetGceRef(gce.GceRef{Name: name}).Build()
	nodeInfos[mig.Id()] = framework.NewTestNodeInfo(&v1.Node{})
	return mig
}

func createPods(numPods int) []*v1.Pod {
	var pods []*v1.Pod
	for i := 0; i < numPods; i++ {
		pod := test_util.BuildTestPod(fmt.Sprintf("pod-%d", i), 1, 1)
		pods = append(pods, pod)
	}
	return pods
}
