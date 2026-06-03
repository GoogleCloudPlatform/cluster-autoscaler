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
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	volume "k8s.io/cloud-provider/volume/helpers"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/gkeprice"
)

const (
	maxSupportedDiskSize = machinetypes.MaxBootDiskSizeNonSharedCoreMachinesGb
)

var (
	// based on phisical: maxSupportedDiskSize=64Tb --> allocatable ~= 62T
	maxAllocatableEphemeralStorage = resource.MustParse("62T")

	arch = gce.DefaultArch
)

func TestPostBinpackingAnalyzer_DiskSizeAnalysisFuncAutopilotCluster(t *testing.T) {
	gkeManager := &gke.GkeManagerMock{}
	gkeManager.On("GetClusterVersion").Return("1.2.3-gke.100")
	gkeManager.On("AreConfidentialNodesEnabled").Return(false)

	type nodeWithPods struct {
		node *apiv1.Node
		pods []*apiv1.Pod
	}
	tcs := map[string]struct {
		nodes            []nodeWithPods
		nodeGroup        *gke.GkeMig
		newNodesWithPods map[string]bool
		resourceApprox   gkeprice.Resource
		expectedDiskSize int64
	}{
		"node pool with GPU - all GPU fully allocated, no future pods calculation": {
			nodes: []nodeWithPods{
				{
					node: &apiv1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "n1",
							Labels: map[string]string{
								labels.GPULabel: "accelerator",
							},
						},
						Status: apiv1.NodeStatus{
							Allocatable: apiv1.ResourceList{
								apiv1.ResourceCPU:              *resource.NewQuantity(16, resource.DecimalSI),
								apiv1.ResourceMemory:           *resource.NewQuantity(32*units.GB, resource.DecimalSI),
								apiv1.ResourceEphemeralStorage: maxAllocatableEphemeralStorage,
								gpu.ResourceNvidiaGPU:          *resource.NewQuantity(2, resource.DecimalSI),
							},
						},
					},
					pods: append(createPods(14, 250, 500, 10*volume.GiB), createPodsWithGPU(1, 250, 500, 10*volume.GiB, 2)...), // 15 pods, 10Gb each <- 150Gb
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetGkeManager(gkeManager).SetAutoprovisioned(true).SetExist(false).SetSpec(
				&gkeclient.NodePoolSpec{SystemArchitecture: &arch, DiskSize: maxSupportedDiskSize},
			).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 15 * volume.GiB,
			},
			expectedDiskSize: 261, // requestedPodsSize=150Gb => physicalSize:249Gb (+5% buffer)
		},
		"node pool with GPU - some GPU still allocatable, 2 future pods": {
			nodes: []nodeWithPods{
				{
					node: &apiv1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "n1",
							Labels: map[string]string{
								labels.GPULabel: "accelerator",
							},
						},
						Status: apiv1.NodeStatus{
							Allocatable: apiv1.ResourceList{
								apiv1.ResourceCPU:              *resource.NewQuantity(16, resource.DecimalSI),
								apiv1.ResourceMemory:           *resource.NewQuantity(32*units.GB, resource.DecimalSI),
								apiv1.ResourceEphemeralStorage: maxAllocatableEphemeralStorage,
								gpu.ResourceNvidiaGPU:          *resource.NewQuantity(2, resource.DecimalSI),
							},
						},
					},
					pods: createPods(14, 250, 500, 10*volume.GiB), // 14 pods, 10Gb each <- 140Gb
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetGkeManager(gkeManager).SetAutoprovisioned(true).SetExist(false).SetSpec(
				&gkeclient.NodePoolSpec{SystemArchitecture: &arch, DiskSize: maxSupportedDiskSize},
			).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 15 * volume.GiB,
			},
			expectedDiskSize: 296,
		},
		"low requirements - set min disk size": {
			nodes: []nodeWithPods{
				{
					node: &apiv1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "n1",
							Labels: map[string]string{
								labels.GPULabel: "accelerator",
							},
						},
						Status: apiv1.NodeStatus{
							Allocatable: apiv1.ResourceList{
								apiv1.ResourceCPU:              *resource.NewQuantity(16, resource.DecimalSI),
								apiv1.ResourceMemory:           *resource.NewQuantity(32*units.GB, resource.DecimalSI),
								apiv1.ResourceEphemeralStorage: maxAllocatableEphemeralStorage,
								gpu.ResourceNvidiaGPU:          *resource.NewQuantity(2, resource.DecimalSI),
							},
						},
					},
					pods: createPods(1, 250, 500, 200*volume.MiB), // 1 pods, 200MiB each
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetGkeManager(gkeManager).SetAutoprovisioned(true).SetExist(false).SetSpec(
				&gkeclient.NodePoolSpec{SystemArchitecture: &arch, DiskSize: maxSupportedDiskSize},
			).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
			},
			resourceApprox:   gkeprice.Resource{},
			expectedDiskSize: 100, // MinBootDiskSizeGBForNAP
		},
		"5 pods with 20Gb storage, 10 future pods": {
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
								apiv1.ResourceEphemeralStorage: maxAllocatableEphemeralStorage,
							},
						},
					},
					pods: createPods(5, 250, 500, 20*volume.GiB),
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetGkeManager(gkeManager).SetAutoprovisioned(true).SetExist(false).SetSpec(
				&gkeclient.NodePoolSpec{SystemArchitecture: &arch, DiskSize: maxSupportedDiskSize},
			).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 15 * volume.GiB,
			},
			expectedDiskSize: 1716,
		},
		"boot disk set by node selectors - no future pods calculation": {
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
								apiv1.ResourceEphemeralStorage: maxAllocatableEphemeralStorage,
							},
						},
					},
					pods: createPods(1, 250, 500, 20*volume.GiB),
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetGkeManager(gkeManager).SetAutoprovisioned(true).SetExist(false).SetSpec(
				&gkeclient.NodePoolSpec{DiskSize: 250, Labels: map[string]string{gkelabels.BootDiskSizeLabelKey: "250"}},
			).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 15 * volume.GiB,
			},
			expectedDiskSize: 250,
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
			if gotDiskSize := tc.nodeGroup.Spec().DiskSize; gotDiskSize != tc.expectedDiskSize {
				t.Errorf("Unexpected disk size, got: %d, want: %d", gotDiskSize, tc.expectedDiskSize)
			}
		})
	}
}

func TestPostBinpackingAnalyzer_DiskSizeAnalysisFuncStandardCluster(t *testing.T) {
	gkeManager := &gke.GkeManagerMock{}
	gkeManager.On("GetClusterVersion").Return("1.2.3-gke.100")
	gkeManager.On("AreConfidentialNodesEnabled").Return(false)

	type nodeWithPods struct {
		node *apiv1.Node
		pods []*apiv1.Pod
	}
	tcs := map[string]struct {
		nodes            []nodeWithPods
		nodeGroup        *gke.GkeMig
		newNodesWithPods map[string]bool
		resourceApprox   gkeprice.Resource
		expectedDiskSize int64
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
								apiv1.ResourceEphemeralStorage: maxAllocatableEphemeralStorage,
							},
						},
					},
					pods: createPods(1, 250, 500, 20*volume.GiB),
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetGkeManager(gkeManager).SetAutoprovisioned(true).SetExist(false).SetSpec(
				&gkeclient.NodePoolSpec{DiskSize: maxSupportedDiskSize},
			).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 15 * volume.GiB,
			},
			expectedDiskSize: maxSupportedDiskSize,
		},
		"managed node group with dynamic disk size set to true - calculation performed": {
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
								apiv1.ResourceEphemeralStorage: maxAllocatableEphemeralStorage,
							},
						},
					},
					pods: createPods(1, 250, 500, 20*volume.GiB),
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetGkeManager(gkeManager).SetAutoprovisioned(true).SetExist(false).SetSpec(
				&gkeclient.NodePoolSpec{DiskSize: maxSupportedDiskSize,
					Labels:           map[string]string{gkelabels.NodeGroupDynamicBootDiskSizeEnabledLabelKey: "true"},
					AutopilotManaged: true},
			).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 15 * volume.GiB,
			},
			expectedDiskSize: 1681,
		},
		"managed node group with dynamic disk size set to false (absence of DynamicBootDiskSizeEnabled label) - no calculation": {
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
								apiv1.ResourceEphemeralStorage: maxAllocatableEphemeralStorage,
							},
						},
					},
					pods: createPods(1, 250, 500, 20*volume.GiB),
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetGkeManager(gkeManager).SetAutoprovisioned(true).SetExist(false).SetSpec(
				&gkeclient.NodePoolSpec{DiskSize: maxSupportedDiskSize,
					AutopilotManaged: true},
			).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 15 * volume.GiB,
			},
			expectedDiskSize: maxSupportedDiskSize,
		},
		"managed node group with dynamic disk size set to true, but boot disk size is set by node selectors - no calculation": {
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
								apiv1.ResourceEphemeralStorage: maxAllocatableEphemeralStorage,
							},
						},
					},
					pods: createPods(1, 250, 500, 20*volume.GiB),
				},
			},
			nodeGroup: gke.NewTestGkeMigBuilder().SetGkeManager(gkeManager).SetAutoprovisioned(true).SetExist(false).SetSpec(
				&gkeclient.NodePoolSpec{DiskSize: 500,
					Labels: map[string]string{
						gkelabels.NodeGroupDynamicBootDiskSizeEnabledLabelKey: "true",
						gkelabels.BootDiskSizeLabelKey:                        "500",
					},
					AutopilotManaged: true},
			).Build(),
			newNodesWithPods: map[string]bool{
				"n1": true,
			},
			resourceApprox: gkeprice.Resource{
				MilliCPU:         250,
				Memory:           700,
				EphemeralStorage: 15 * volume.GiB,
			},
			expectedDiskSize: 500,
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
			if gotDiskSize := tc.nodeGroup.Spec().DiskSize; gotDiskSize != tc.expectedDiskSize {
				t.Errorf("Unexpected disk size, got: %d, want: %d", gotDiskSize, tc.expectedDiskSize)
			}
		})
	}
}
