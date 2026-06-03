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
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/customresources"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
)

func TestDraCrpInternalOverride_GetNodeResourceTargets(t *testing.T) {
	standardNode := BuildTestNode("node-1", 1000, 1000)

	draGpuNode := BuildTestNode("node-gpu", 1000, 1000)
	draGpuNode.Labels[gkelabels.DraGpuNodeLabel] = "true"
	draGpuNode.Labels[gkelabels.GPULabel] = "nvidia-tesla-t4"

	draGpuNodeMissingLabel := BuildTestNode("node-gpu-missing-label", 1000, 1000)
	draGpuNodeMissingLabel.Labels[gkelabels.DraGpuNodeLabel] = "true"

	draTpuNode := BuildTestNode("node-tpu", 1000, 1000)
	draTpuNode.Labels[gkelabels.DraTpuNodeLabel] = "true"
	draTpuNode.Labels[gkelabels.TPULabel] = "v3-8"

	draTpuNodeMissingLabel := BuildTestNode("node-tpu-missing-label", 1000, 1000)
	draTpuNodeMissingLabel.Labels[gkelabels.DraTpuNodeLabel] = "true"

	tests := []struct {
		name          string
		node          *apiv1.Node
		nodeGroup     cloudprovider.NodeGroup
		cloudProvider cloudProvider
		nodePoolSpec  *gkeclient.NodePoolSpec
		wantTargets   []customresources.CustomResourceTarget
		wantErr       bool
	}{
		{
			name: "NonDraNode_ShouldReturnNil",
			node: standardNode,
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{MachineType: "n1-standard-1"}).
				Build(),
			wantTargets: nil,
		},
		{
			name: "DraGpuEnabledNode_WithAccelerators_ButMissingGpuLabel",
			node: draGpuNodeMissingLabel,
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType: "n1-standard-1",
					Accelerators: []*gke_api_beta.AcceleratorConfig{
						{
							AcceleratorType:  "nvidia-tesla-t4",
							AcceleratorCount: 1,
						},
					},
				}).
				Build(),
			wantTargets: nil,
		},
		{
			name: "DraGpuEnabledNode_WithValidAccelerator",
			node: draGpuNode,
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType: "n1-standard-1",
					Accelerators: []*gke_api_beta.AcceleratorConfig{
						{
							AcceleratorType:  "nvidia-tesla-t4",
							AcceleratorCount: 2,
						},
					},
				}).
				Build(),
			wantTargets: []customresources.CustomResourceTarget{
				{
					ResourceType:  "nvidia-tesla-t4",
					ResourceCount: 2,
				},
			},
		},
		{
			name: "DraGpuEnabledNode_ButNoAcceleratorsInSpec",
			node: draGpuNodeMissingLabel,
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType: "n1-standard-1",
				}).
				Build(),
			wantTargets: nil,
		},
		{
			name: "DraTpuEnabledNode_WithValidMachineType",
			node: draTpuNode,
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType: "ct5lp-hightpu-4t",
				}).
				Build(),
			wantTargets: []customresources.CustomResourceTarget{
				{
					ResourceType:  "v3-8",
					ResourceCount: 4,
				},
			},
		},
		{
			name: "DraTpuEnabledNode_ButMissingTpuLabelOnNode",
			node: draTpuNodeMissingLabel,
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType: "ct5lp-hightpu-4t",
				}).
				Build(),
			wantTargets: nil,
		},
		{
			name:        "NonGkeNodeGroup",
			node:        standardNode,
			nodeGroup:   nil, // nil here is treated as the implementation of the NodeGroup interface
			wantTargets: nil,
		},
		{
			name: "DraNonAutoscaledNodeGroup",
			node: draGpuNode,
			// for non-autoscaled nodes, nodeGroup is nil, unable to extract node pool spec
			// as cloud provider is not initialized
			nodeGroup:   nil,
			wantTargets: nil,
		},
		{
			name:      "DraGpuEnabledNode_WithValidAccelerator_NonAutoscaled",
			node:      draGpuNode,
			nodeGroup: nil,
			nodePoolSpec: &gkeclient.NodePoolSpec{
				MachineType: "n1-standard-1",
				Accelerators: []*gke_api_beta.AcceleratorConfig{
					{
						AcceleratorType:  "nvidia-tesla-t4",
						AcceleratorCount: 2,
					},
				},
			},
			wantTargets: []customresources.CustomResourceTarget{
				{
					ResourceType:  "nvidia-tesla-t4",
					ResourceCount: 2,
				},
			},
		},
		{
			name:      "DraGpuEnabledNode_NoAccelerator_NonAutoscaled",
			node:      draGpuNode,
			nodeGroup: nil,
			nodePoolSpec: &gkeclient.NodePoolSpec{
				MachineType: "n1-standard-1",
			},
			wantTargets: nil,
		},
		{
			name: "DraTpuEnabledNode_WithValidMachineType_NonAutoscaled",
			node: draTpuNode,
			nodePoolSpec: &gkeclient.NodePoolSpec{
				MachineType: "ct5lp-hightpu-4t",
			},
			nodeGroup: nil,
			wantTargets: []customresources.CustomResourceTarget{
				{
					ResourceType:  "v3-8",
					ResourceCount: 4,
				},
			},
		},
		{
			name:      "DraTpuEnabledNode_ButMissingTpuLabelOnNode_NonAutoscaled",
			node:      draTpuNodeMissingLabel,
			nodeGroup: nil,
			nodePoolSpec: &gkeclient.NodePoolSpec{
				MachineType: "ct5lp-hightpu-4t",
			},
			wantTargets: nil,
		},
		{
			name:      "DraTpuEnabledNode_NoAccelerator_NonAutoscaled",
			node:      draTpuNode,
			nodeGroup: nil,
			nodePoolSpec: &gkeclient.NodePoolSpec{
				MachineType: "n1-standard-1",
			},
			wantTargets: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &context.AutoscalingContext{}
			processor := NewDraCrpInternalOverride()
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithNodePoolSpec(tc.nodePoolSpec).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			processor.SetCloudProvider(provider)
			targets, err := processor.GetNodeResourceTargets(ctx, tc.node, tc.nodeGroup)

			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tc.wantTargets, targets)
		})
	}
}
