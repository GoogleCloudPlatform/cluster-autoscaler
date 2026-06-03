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

package customresources

import (
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	csisnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/csi/snapshot"
	drasnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/dynamicresources/snapshot"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	testutils "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

func buildNode(name string, withGpu, gpuResourceAvailable, isDra bool) *apiv1.Node {
	node := testutils.BuildTestNode(name, 1000, 1000)
	if withGpu {
		node.Labels[gkelabels.GPULabel] = "nvidia-tesla-k80"
		if gpuResourceAvailable {
			node.Status.Allocatable[gpu.ResourceNvidiaGPU] = *resource.NewQuantity(1, resource.DecimalSI)
		}
	}
	if isDra {
		node.Labels[gkelabels.DraGpuNodeLabel] = "true"
	}
	return node
}

func TestGpuPartitioningCustomResourcesProcessor_FilterOutNodesWithUnreadyResources(t *testing.T) {
	// non-DRA nodes
	readyGpuNode := buildNode("ready-gpu", true, true, false)
	unreadyGpuNode := buildNode("unready-gpu", true, false, false)
	noGpuNode := buildNode("no-gpu", false, false, false)
	unreadyNode := kubernetes.GetUnreadyNodeCopy(buildNode("unready", false, false, false), kubernetes.ResourceUnready)
	// DRA nodes
	gpuDraNodeWithoutResource := buildNode("gpu-dra-no-resource", true, false, true)

	processor := NewGpuPartitioningCustomResourcesProcessor()
	provider := &gke.GkeCloudProviderMock{}
	var emptyGpuConfig *cloudprovider.GpuConfig
	provider.On("GetNodeGpuConfig", readyGpuNode).Return(&cloudprovider.GpuConfig{Label: "cloud.google.com/gke-accelerator", Type: "nvidia-tesla-k80", ExtendedResourceName: "nvidia.com/gpu"})
	provider.On("GetNodeGpuConfig", unreadyGpuNode).Return(&cloudprovider.GpuConfig{Label: "cloud.google.com/gke-accelerator", Type: "nvidia-tesla-k80", ExtendedResourceName: "nvidia.com/gpu"})
	provider.On("GetNodeGpuConfig", noGpuNode).Return(emptyGpuConfig)
	provider.On("GetNodeGpuConfig", unreadyNode).Return(emptyGpuConfig)
	provider.On("GetNodeGpuConfig", gpuDraNodeWithoutResource).Return(&cloudprovider.GpuConfig{Label: "cloud.google.com/gke-accelerator", Type: "nvidia-tesla-k80", DraDriverName: "gpu.nvidia.com"})
	provider.On("GPULabel").Return("cloud.google.com/gke-accelerator")
	ctx := &context.AutoscalingContext{CloudProvider: provider}

	testCases := map[string]struct {
		allNodes   []*apiv1.Node
		readyNodes []*apiv1.Node
		wantAll    []*apiv1.Node
		wantReady  []*apiv1.Node
	}{
		"EmptyNodes": {
			allNodes:   []*apiv1.Node{},
			readyNodes: []*apiv1.Node{},
			wantAll:    []*apiv1.Node{},
			wantReady:  []*apiv1.Node{},
		},
		"NonDraNodes": {
			allNodes:   []*apiv1.Node{readyGpuNode, unreadyGpuNode, noGpuNode, unreadyNode},
			readyNodes: []*apiv1.Node{readyGpuNode, unreadyGpuNode, noGpuNode},
			wantAll: []*apiv1.Node{
				readyGpuNode,
				kubernetes.GetUnreadyNodeCopy(unreadyGpuNode, kubernetes.ResourceUnready),
				noGpuNode,
				unreadyNode,
			},
			wantReady: []*apiv1.Node{readyGpuNode, noGpuNode},
		},
		"DraNodes": {
			allNodes:   []*apiv1.Node{gpuDraNodeWithoutResource},
			readyNodes: []*apiv1.Node{gpuDraNodeWithoutResource},
			wantAll:    []*apiv1.Node{gpuDraNodeWithoutResource},
			wantReady:  []*apiv1.Node{gpuDraNodeWithoutResource},
		},
		"MixOfDraAndNonDraNodes": {
			allNodes:   []*apiv1.Node{readyGpuNode, unreadyGpuNode, gpuDraNodeWithoutResource},
			readyNodes: []*apiv1.Node{readyGpuNode, unreadyGpuNode, gpuDraNodeWithoutResource},
			wantAll: []*apiv1.Node{
				kubernetes.GetUnreadyNodeCopy(unreadyGpuNode, kubernetes.ResourceUnready),
				readyGpuNode,
				gpuDraNodeWithoutResource,
			},
			wantReady: []*apiv1.Node{readyGpuNode, gpuDraNodeWithoutResource},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			gotAll, gotReady := processor.FilterOutNodesWithUnreadyResources(ctx, tc.allNodes, tc.readyNodes, drasnapshot.NewEmptySnapshot(), csisnapshot.NewEmptySnapshot())
			assert.ElementsMatch(t, tc.wantAll, gotAll)
			assert.ElementsMatch(t, tc.wantReady, gotReady)
		})
	}
}
