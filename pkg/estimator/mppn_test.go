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

package estimator

import (
	"testing"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/gkeprice"
)

// phisical: machinetypes.DefaultDiskSizeGBForAutopilot = 250 --> allocatable ~= 210G
var allocatableEphemeralStorage = resource.MustParse("210G")

func TestPostBinpackingAnalyzer_MppnAnalysisFuncAutopilotCluster(t *testing.T) {
	type nodeWithPods struct {
		node *apiv1.Node
		pods []*apiv1.Pod
	}
	tcs := map[string]struct {
		nodes            []nodeWithPods
		nodeGroup        *gke.GkeMig
		newNodesWithPods map[string]bool
		resourceApprox   gkeprice.Resource
		expectedMppn     int64
	}{
		"node pool with GPU - all GPU taken, no more future pods": {
			nodes: []nodeWithPods{
				{
					node: &apiv1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "n1",
							Labels: map[string]string{
								labels.GPULabel: "accelerator",
								labels.NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey: "true",
							},
						},
						Status: apiv1.NodeStatus{
							Allocatable: apiv1.ResourceList{
								apiv1.ResourceCPU:              *resource.NewQuantity(16, resource.DecimalSI),
								apiv1.ResourceMemory:           *resource.NewQuantity(32*units.GB, resource.DecimalSI),
								apiv1.ResourceEphemeralStorage: allocatableEphemeralStorage,
								gpu.ResourceNvidiaGPU:          *resource.NewQuantity(2, resource.DecimalSI),
							},
						},
					},
					pods: append(createPods(15, 250, 500, 100), createPodsWithGPU(1, 250, 500, 100, 2)...),
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetAutoprovisioned(true).SetExist(false).SetSpec(&gkeclient.NodePoolSpec{MaxPodsPerNode: 256, Labels: map[string]string{labels.NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey: "true"}}).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 100,
			},
			expectedMppn: 32,
		},
		"node pool with GPU - some GPU still allocatable, some future pods": {
			nodes: []nodeWithPods{
				{
					node: &apiv1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "n1",
							Labels: map[string]string{
								labels.GPULabel: "accelerator",
								labels.NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey: "true",
							},
						},
						Status: apiv1.NodeStatus{
							Allocatable: apiv1.ResourceList{
								apiv1.ResourceCPU:              *resource.NewQuantity(16, resource.DecimalSI),
								apiv1.ResourceMemory:           *resource.NewQuantity(32*units.GB, resource.DecimalSI),
								apiv1.ResourceEphemeralStorage: allocatableEphemeralStorage,
								gpu.ResourceNvidiaGPU:          *resource.NewQuantity(4, resource.DecimalSI),
							},
						},
					},
					pods: append(createPods(15, 250, 500, 100), createPodsWithGPU(1, 250, 500, 100, 2)...),
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetAutoprovisioned(true).SetExist(false).SetSpec(&gkeclient.NodePoolSpec{MaxPodsPerNode: 256, Labels: map[string]string{labels.NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey: "true"}}).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 100,
			},
			expectedMppn: 32,
		},
		"node pool with configured MaxPodsPerNode": {
			nodes: []nodeWithPods{
				{
					node: &apiv1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "n1",
							Labels: map[string]string{
								labels.NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey: "true",
							},
						},
						Status: apiv1.NodeStatus{
							Allocatable: apiv1.ResourceList{
								apiv1.ResourceCPU:              *resource.NewQuantity(16, resource.DecimalSI),
								apiv1.ResourceMemory:           *resource.NewQuantity(32*units.GB, resource.DecimalSI),
								apiv1.ResourceEphemeralStorage: allocatableEphemeralStorage,
							},
						},
					},
					pods: append(createPods(15, 250, 500, 100), createPodsWithGPU(1, 250, 500, 100, 2)...),
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetAutoprovisioned(true).SetExist(false).SetSpec(&gkeclient.NodePoolSpec{MaxPodsPerNode: 16, Labels: map[string]string{
				labels.MaxPodsPerNodeLabel: "16",
			}}).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 100,
			},
			expectedMppn: 16,
		},
		"node pool with TPU - no future pods calculation": {
			nodes: []nodeWithPods{
				{
					node: &apiv1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "n1",
							Labels: map[string]string{
								labels.TPULabel: "tpu",
								labels.NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey: "true",
							},
						},
						Status: apiv1.NodeStatus{
							Allocatable: apiv1.ResourceList{
								apiv1.ResourceCPU:              *resource.NewQuantity(16, resource.DecimalSI),
								apiv1.ResourceMemory:           *resource.NewQuantity(32*units.GB, resource.DecimalSI),
								apiv1.ResourceEphemeralStorage: allocatableEphemeralStorage,
								tpu.ResourceGoogleTPU:          *resource.NewQuantity(2, resource.DecimalSI),
							},
						},
					},
					pods: createPods(15, 250, 500, 100),
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetAutoprovisioned(true).
				SetExist(false).
				SetSpec(&gkeclient.NodePoolSpec{MaxPodsPerNode: 256, Labels: map[string]string{labels.NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey: "true"}}).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 100,
			},
			expectedMppn: 32,
		},
		"EDP node pool - no future pods calculation": {
			nodes: []nodeWithPods{
				{
					node: &apiv1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "n1",
							Labels: map[string]string{
								labels.ExtendedDurationPodsLabel:                     "100m",
								labels.NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey: "true",
							},
						},
						Status: apiv1.NodeStatus{
							Allocatable: apiv1.ResourceList{
								apiv1.ResourceCPU:              *resource.NewQuantity(16, resource.DecimalSI),
								apiv1.ResourceMemory:           *resource.NewQuantity(32*units.GB, resource.DecimalSI),
								apiv1.ResourceEphemeralStorage: allocatableEphemeralStorage,
							},
						},
					},
					pods: createPods(15, 250, 500, 100),
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetAutoprovisioned(true).
				SetExist(false).
				SetSpec(&gkeclient.NodePoolSpec{MaxPodsPerNode: 256, Labels: map[string]string{labels.NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey: "true"}}).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 100,
			},
			expectedMppn: 32,
		},
		"EDP EK node pool with 10 pods per node, 50 future pods": {
			nodes: []nodeWithPods{
				{
					node: &apiv1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "ek",
							Labels: map[string]string{
								labels.ExtendedDurationPodsLabel:                     "100m",
								labels.MachineFamilyLabel:                            "ek",
								labels.NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey: "true",
							},
						},
						Status: apiv1.NodeStatus{
							Allocatable: apiv1.ResourceList{
								apiv1.ResourceCPU:              *resource.NewQuantity(16, resource.DecimalSI),
								apiv1.ResourceMemory:           *resource.NewQuantity(32*units.GB, resource.DecimalSI),
								apiv1.ResourceEphemeralStorage: allocatableEphemeralStorage,
							},
						},
					},
					pods: createPods(10, 250, 500, 100),
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetAutoprovisioned(true).
				SetExist(false).
				SetSpec(&gkeclient.NodePoolSpec{MaxPodsPerNode: 256, Labels: map[string]string{labels.NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey: "true"}}).Build(),
			newNodesWithPods: map[string]bool{
				"ek": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 100,
			},
			expectedMppn: 64,
		},
		"already existing node pool": {
			nodes: []nodeWithPods{
				{
					node: &apiv1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "n1",
						},
						Status: apiv1.NodeStatus{
							Allocatable: apiv1.ResourceList{
								apiv1.ResourceCPU:              *resource.NewQuantity(16, resource.DecimalSI),
								apiv1.ResourceMemory:           *resource.NewQuantity(32*units.GB, resource.DecimalSI),
								apiv1.ResourceEphemeralStorage: allocatableEphemeralStorage,
							},
						},
					},
					pods: createPods(50, 250, 500, 100),
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetAutoprovisioned(true).SetExist(true).SetSpec(&gkeclient.NodePoolSpec{MaxPodsPerNode: 123}).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 100,
			},
			expectedMppn: 123,
		},
		"non autoprovisioned node pool": {
			nodes: []nodeWithPods{
				{
					node: &apiv1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "n1",
						},
						Status: apiv1.NodeStatus{
							Allocatable: apiv1.ResourceList{
								apiv1.ResourceCPU:              *resource.NewQuantity(16, resource.DecimalSI),
								apiv1.ResourceMemory:           *resource.NewQuantity(32*units.GB, resource.DecimalSI),
								apiv1.ResourceEphemeralStorage: allocatableEphemeralStorage,
							},
						},
					},
					pods: createPods(50, 250, 500, 100),
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetAutoprovisioned(false).SetExist(false).SetSpec(&gkeclient.NodePoolSpec{MaxPodsPerNode: 123}).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 100,
			},
			expectedMppn: 123,
		},
		"50 pods per node, 10 future pods": {
			nodes: []nodeWithPods{
				{
					node: &apiv1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "n1",
						},
						Status: apiv1.NodeStatus{
							Allocatable: apiv1.ResourceList{
								apiv1.ResourceCPU:              *resource.NewQuantity(16, resource.DecimalSI),
								apiv1.ResourceMemory:           *resource.NewQuantity(32*units.GB, resource.DecimalSI),
								apiv1.ResourceEphemeralStorage: allocatableEphemeralStorage,
							},
						},
					},
					pods: createPods(50, 250, 500, 100),
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetAutoprovisioned(true).SetExist(false).
				SetSpec(&gkeclient.NodePoolSpec{MaxPodsPerNode: 256, Labels: map[string]string{labels.NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey: "true"}}).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 100,
			},
			expectedMppn: 64,
		},
		"1 node with 50 pods per node, second node with 10 pods per node": {
			nodes: []nodeWithPods{
				{
					node: &apiv1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "n1",
						},
						Status: apiv1.NodeStatus{
							Allocatable: apiv1.ResourceList{
								apiv1.ResourceCPU:              *resource.NewQuantity(16, resource.DecimalSI),
								apiv1.ResourceMemory:           *resource.NewQuantity(32*units.GB, resource.DecimalSI),
								apiv1.ResourceEphemeralStorage: allocatableEphemeralStorage,
							},
						},
					},
					pods: createPods(50, 250, 500, 100),
				}, {
					node: &apiv1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "n2",
						},
						Status: apiv1.NodeStatus{
							Allocatable: apiv1.ResourceList{
								apiv1.ResourceCPU:              *resource.NewQuantity(16, resource.DecimalSI),
								apiv1.ResourceMemory:           *resource.NewQuantity(32*units.GB, resource.DecimalSI),
								apiv1.ResourceEphemeralStorage: allocatableEphemeralStorage,
							},
						},
					},
					pods: createPods(10, 1600, 500, 100),
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetAutoprovisioned(true).SetExist(false).
				SetSpec(&gkeclient.NodePoolSpec{MaxPodsPerNode: 256, Labels: map[string]string{labels.NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey: "true"}}).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
				"n2": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 100,
			},
			expectedMppn: 64,
		},
		"1 node with 128+ pods per node, second node with 10 pods per node": {
			nodes: []nodeWithPods{
				{
					node: &apiv1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "n1",
						},
						Status: apiv1.NodeStatus{
							Allocatable: apiv1.ResourceList{
								apiv1.ResourceCPU:              *resource.NewQuantity(64, resource.DecimalSI),
								apiv1.ResourceMemory:           *resource.NewQuantity(128*units.GB, resource.DecimalSI),
								apiv1.ResourceEphemeralStorage: *resource.NewQuantity(128*units.GB, resource.DecimalSI),
							},
						},
					},
					pods: createPods(110, 450, 500, 100),
				}, {
					node: &apiv1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "n2",
						},
						Status: apiv1.NodeStatus{
							Allocatable: apiv1.ResourceList{
								apiv1.ResourceCPU:              *resource.NewQuantity(64, resource.DecimalSI),
								apiv1.ResourceMemory:           *resource.NewQuantity(128*units.GB, resource.DecimalSI),
								apiv1.ResourceEphemeralStorage: *resource.NewQuantity(128*units.GB, resource.DecimalSI),
							},
						},
					},
					pods: createPods(10, 640, 500, 100),
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetAutoprovisioned(true).SetExist(false).
				SetSpec(&gkeclient.NodePoolSpec{MaxPodsPerNode: 256, Labels: map[string]string{labels.NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey: "true"}}).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
				"n2": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 100,
			},
			expectedMppn: 256,
		},
		"1 node with 128+ pods per node, second node with 10 pods per node, current max pods per node equal to 128": {
			nodes: []nodeWithPods{
				{
					node: &apiv1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "n1",
						},
						Status: apiv1.NodeStatus{
							Allocatable: apiv1.ResourceList{
								apiv1.ResourceCPU:              *resource.NewQuantity(64, resource.DecimalSI),
								apiv1.ResourceMemory:           *resource.NewQuantity(128*units.GB, resource.DecimalSI),
								apiv1.ResourceEphemeralStorage: *resource.NewQuantity(128*units.GB, resource.DecimalSI),
							},
						},
					},
					pods: createPods(110, 450, 500, 100),
				}, {
					node: &apiv1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "n2",
						},
						Status: apiv1.NodeStatus{
							Allocatable: apiv1.ResourceList{
								apiv1.ResourceCPU:              *resource.NewQuantity(64, resource.DecimalSI),
								apiv1.ResourceMemory:           *resource.NewQuantity(128*units.GB, resource.DecimalSI),
								apiv1.ResourceEphemeralStorage: *resource.NewQuantity(128*units.GB, resource.DecimalSI),
							},
						},
					},
					pods: createPods(10, 640, 500, 100),
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetAutoprovisioned(true).SetExist(false).SetSpec(&gkeclient.NodePoolSpec{MaxPodsPerNode: 110, Labels: map[string]string{labels.NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey: "true"}}).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
				"n2": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 100,
			},
			expectedMppn: 110,
		},
	}
	for desc, tc := range tcs {
		tc := tc
		t.Run(desc, func(t *testing.T) {
			a := staticClusterAnalyzer{&staticClusterAnalysis{resourceApprox: tc.resourceApprox}}
			storageCalc := staticStorageCalculator{}
			nrt := NewNapResourceTrimmer(&a, &storageCalc, true /* autopilotEnabled */)
			clusterSnapshot := testsnapshot.NewTestSnapshotOrDie(t)
			for _, node := range tc.nodes {
				err := clusterSnapshot.AddNodeInfo(framework.NewTestNodeInfo(node.node, node.pods...))
				if err != nil {
					t.Fatalf("Failed to add nodes: %v", err)
				}
			}
			nrt.analysisChainFunc(clusterSnapshot, tc.nodeGroup, tc.newNodesWithPods)
			if gotMppn := tc.nodeGroup.Spec().MaxPodsPerNode; gotMppn != tc.expectedMppn {
				t.Errorf("Unexpected max pods per node, got: %d, want: %d", gotMppn, tc.expectedMppn)
			}
		})
	}
}

func TestPostBinpackingAnalyzer_MppnAnalysisFuncStandardCluster(t *testing.T) {
	type nodeWithPods struct {
		node *apiv1.Node
		pods []*apiv1.Pod
	}
	tcs := map[string]struct {
		nodes            []nodeWithPods
		nodeGroup        *gke.GkeMig
		newNodesWithPods map[string]bool
		resourceApprox   gkeprice.Resource
		expectedMppn     int64
	}{
		"standard node group - no calculation": {
			nodes: []nodeWithPods{
				{
					node: &apiv1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "n1",
						},
						Status: apiv1.NodeStatus{
							Allocatable: apiv1.ResourceList{
								apiv1.ResourceCPU:              *resource.NewQuantity(16, resource.DecimalSI),
								apiv1.ResourceMemory:           *resource.NewQuantity(32*units.GB, resource.DecimalSI),
								apiv1.ResourceEphemeralStorage: allocatableEphemeralStorage,
							},
						},
					},
					pods: createPods(50, 250, 500, 100),
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetAutoprovisioned(true).SetExist(false).SetSpec(&gkeclient.NodePoolSpec{MaxPodsPerNode: 128}).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 100,
			},
			expectedMppn: 128, // in case of calculations performed would be 64
		},
		"managed node group with max pods per node set to true - calculation performed": {
			nodes: []nodeWithPods{
				{
					node: &apiv1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "n1",
						},
						Status: apiv1.NodeStatus{
							Allocatable: apiv1.ResourceList{
								apiv1.ResourceCPU:              *resource.NewQuantity(16, resource.DecimalSI),
								apiv1.ResourceMemory:           *resource.NewQuantity(32*units.GB, resource.DecimalSI),
								apiv1.ResourceEphemeralStorage: allocatableEphemeralStorage,
							},
						},
					},
					pods: createPods(50, 250, 500, 100),
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetAutoprovisioned(true).SetExist(false).SetSpec(&gkeclient.NodePoolSpec{
				MaxPodsPerNode:   32,
				Labels:           map[string]string{gkelabels.NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey: "true"},
				AutopilotManaged: true,
			}).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 100,
			},
			expectedMppn: 64,
		},
		"managed node group with max pods per node flag set to false (absence of MaxPodsPerNodeEnabled label) - no calculation": {
			nodes: []nodeWithPods{
				{
					node: &apiv1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "n1",
						},
						Status: apiv1.NodeStatus{
							Allocatable: apiv1.ResourceList{
								apiv1.ResourceCPU:              *resource.NewQuantity(16, resource.DecimalSI),
								apiv1.ResourceMemory:           *resource.NewQuantity(32*units.GB, resource.DecimalSI),
								apiv1.ResourceEphemeralStorage: allocatableEphemeralStorage,
							},
						},
					},
					pods: createPods(50, 250, 500, 100),
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetAutoprovisioned(true).SetExist(false).SetSpec(&gkeclient.NodePoolSpec{
				MaxPodsPerNode:   56,
				AutopilotManaged: true,
			}).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 100,
			},
			expectedMppn: 56, // in case of calculations performed would be 64
		},
		"managed node group , but max pods per node is set by node selectors - no calculation": {
			nodes: []nodeWithPods{
				{
					node: &apiv1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "n1",
						},
						Status: apiv1.NodeStatus{
							Allocatable: apiv1.ResourceList{
								apiv1.ResourceCPU:              *resource.NewQuantity(16, resource.DecimalSI),
								apiv1.ResourceMemory:           *resource.NewQuantity(32*units.GB, resource.DecimalSI),
								apiv1.ResourceEphemeralStorage: allocatableEphemeralStorage,
							},
						},
					},
					pods: append(createPods(15, 250, 500, 100), createPodsWithGPU(1, 250, 500, 100, 2)...),
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetAutoprovisioned(true).SetExist(false).SetSpec(&gkeclient.NodePoolSpec{
				MaxPodsPerNode: 72,
				Labels: map[string]string{
					gkelabels.ManagedNodeLabel: "true",
					labels.MaxPodsPerNodeLabel: "72",
				},
			}).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 100,
			},
			expectedMppn: 72, // in case of calculations performed would be 64
		},
	}
	for desc, tc := range tcs {
		tc := tc
		t.Run(desc, func(t *testing.T) {
			a := staticClusterAnalyzer{&staticClusterAnalysis{resourceApprox: tc.resourceApprox}}
			storageCalc := staticStorageCalculator{}
			nrt := NewNapResourceTrimmer(&a, &storageCalc, false /* autopilotEnabled */)
			clusterSnapshot := testsnapshot.NewTestSnapshotOrDie(t)
			for _, node := range tc.nodes {
				err := clusterSnapshot.AddNodeInfo(framework.NewTestNodeInfo(node.node, node.pods...))
				if err != nil {
					t.Fatalf("Failed to add nodes: %v", err)
				}
			}
			nrt.analysisChainFunc(clusterSnapshot, tc.nodeGroup, tc.newNodesWithPods)
			if gotMppn := tc.nodeGroup.Spec().MaxPodsPerNode; gotMppn != tc.expectedMppn {
				t.Errorf("Unexpected max pods per node, got: %d, want: %d", gotMppn, tc.expectedMppn)
			}
		})
	}
}
