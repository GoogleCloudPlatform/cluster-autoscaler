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
	"time"

	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/customresources"
	drasnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/dynamicresources/snapshot"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
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

func TestDraCrpInternalOverride_FilterOutNodesWithUnreadyResources(t *testing.T) {
	now := metav1.Now()
	draGpuNode := BuildTestNode("node-gpu", 1000, 1000)
	draGpuNode.Labels[gkelabels.DraGpuNodeLabel] = "true"
	draGpuNode.Labels[gkelabels.GPULabel] = "nvidia-tesla-t4"
	draGpuNode.CreationTimestamp = now

	oldDraGpuNode := BuildTestNode("node-gpu-old", 1000, 1000)
	oldDraGpuNode.Labels[gkelabels.DraGpuNodeLabel] = "true"
	oldDraGpuNode.Labels[gkelabels.GPULabel] = "nvidia-tesla-t4"
	oldDraGpuNode.CreationTimestamp = metav1.NewTime(now.Add(-10 * time.Minute))

	standardNode := BuildTestNode("node-standard", 1000, 1000)
	standardNode.CreationTimestamp = now

	tests := []struct {
		name         string
		node         *apiv1.Node
		nodePoolSpec *gkeclient.NodePoolSpec
		wantUnready  bool
	}{
		{
			name: "DraGpuEnabledNode_NodePoolSpecAvailable_ShouldNotMarkAsUnreadyHere",
			node: draGpuNode,
			nodePoolSpec: &gkeclient.NodePoolSpec{
				MachineType: "n1-standard-1",
				Accelerators: []*gke_api_beta.AcceleratorConfig{
					{
						AcceleratorType:  "nvidia-tesla-t4",
						AcceleratorCount: 1,
					},
				},
			},
			wantUnready: false, // wrapped logic might mark it as unready later, but here we should skip it
		},
		{
			name:         "DraGpuEnabledNode_NodePoolSpecMissing_ShouldMarkAsUnready",
			node:         draGpuNode,
			nodePoolSpec: nil,
			wantUnready:  true,
		},
		{
			name:         "OldDraGpuEnabledNode_NodePoolSpecMissing_ShouldNotMarkAsUnready",
			node:         oldDraGpuNode,
			nodePoolSpec: nil,
			wantUnready:  false,
		},
		{
			name:         "StandardNode_NodePoolSpecMissing_ShouldNotMarkAsUnready",
			node:         standardNode,
			nodePoolSpec: nil,
			wantUnready:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			processor := NewDraCrpInternalOverride()
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithNodePoolSpec(tc.nodePoolSpec).
				Build()
			processor.SetCloudProvider(provider)

			allNodes := []*apiv1.Node{tc.node}
			readyNodes := []*apiv1.Node{tc.node}

			// We need a DRA snapshot to avoid nil panics in OSS wrap
			draSnapshot := drasnapshot.NewSnapshot(nil, nil, nil, nil)

			gotAllNodes, gotReadyNodes := processor.FilterOutNodesWithUnreadyResources(&context.AutoscalingContext{CloudProvider: provider}, allNodes, readyNodes, draSnapshot, nil)

			if tc.wantUnready {
				assert.Len(t, gotReadyNodes, 0)
				assert.Len(t, gotAllNodes, 1)
				ready, _, _ := kube_util.GetReadinessState(gotAllNodes[0])
				assert.False(t, ready)
			} else {
				// In the case where nodePoolSpec is provided, we don't mark it as unready.
				// The OSS wrap might still mark it as unready if it sees no slices in draSnapshot,
				// but for the "StandardNode" case it definitely shouldn't be marked as unready.
				if tc.node == standardNode {
					assert.Len(t, gotReadyNodes, 1)
					assert.Equal(t, tc.node, gotReadyNodes[0])
				}
			}
		})
	}
}
