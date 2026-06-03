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

package gkeprice

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	gce_api "google.golang.org/api/compute/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups/asyncnodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
)

type testPricingModel struct {
	nodePrice map[string]float64
	podPrice  map[string]float64
}

func (tpm *testPricingModel) NodePrice(node *apiv1.Node, startTime time.Time, endTime time.Time) (float64, error) {
	if price, found := tpm.nodePrice[node.Name]; found {
		return price, nil
	}
	return 0.0, fmt.Errorf("price for node %v not found", node.Name)
}

func (tpm *testPricingModel) PodPrice(node *apiv1.Pod, startTime time.Time, endTime time.Time) (float64, error) {
	if price, found := tpm.podPrice[node.Name]; found {
		return price, nil
	}
	return 0.0, fmt.Errorf("price for pod %v not found", node.Name)
}

func TestPriceExpander(t *testing.T) {
	n1 := BuildTestNode("n1", 1000, 1000)
	n2 := BuildTestNode("n2", 4000, 1000)
	n3 := BuildTestNode("n3", 4000, 1000)

	p1 := BuildTestPod("p1", 1000, 0)
	p2 := BuildTestPod("p2", 500, 0)

	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		Build()
	provider.AddNodeGroup("ng1", 1, 10, 1)
	provider.AddNodeGroup("ng2", 1, 10, 1)
	provider.AddNode("ng1", n1)
	provider.AddNode("ng2", n2)
	ng1, _ := provider.NodeGroupForNode(n1)
	ng2, _ := provider.NodeGroupForNode(n2)
	ng3, _ := provider.NewNodeGroup("MT1", nil, nil, nil, nil)

	ni1 := framework.NewTestNodeInfo(n1)
	ni2 := framework.NewTestNodeInfo(n2)
	ni3 := framework.NewTestNodeInfo(n3)
	nodeInfosForGroups := map[string]*framework.NodeInfo{
		"ng1": ni1, "ng2": ni2,
	}

	// All node groups accept the same set of pods.
	options := []expander.Option{
		{
			NodeGroup: ng1,
			NodeCount: 2,
			Pods:      []*apiv1.Pod{p1, p2},
			Debug:     "ng1",
		},
		{
			NodeGroup: ng2,
			NodeCount: 1,
			Pods:      []*apiv1.Pod{p1, p2},
			Debug:     "ng2",
		},
	}

	// First node group is cheaper.
	pricingModel := &testPricingModel{
		podPrice: map[string]float64{
			"p1":        20.0,
			"p2":        10.0,
			"stabilize": 10,
		},
		nodePrice: map[string]float64{
			"n1": 20.0,
			"n2": 200.0,
		},
	}
	assert.Contains(t, NewTestStrategy(provider, pricingModel, 2, WithPvmUnfitnessPenalty()).BestOption(options, nodeInfosForGroups).Debug, "ng1")

	// First node group is cheaper, however, the second one is preferred.
	pricingModel = &testPricingModel{
		podPrice: map[string]float64{
			"p1":        20.0,
			"p2":        10.0,
			"stabilize": 10,
		},
		nodePrice: map[string]float64{
			"n1": 50.0,
			"n2": 200.0,
		},
	}
	assert.Contains(t, NewTestStrategy(provider, pricingModel, 4, WithPvmUnfitnessPenalty()).BestOption(options, nodeInfosForGroups).Debug, "ng2")

	// All node groups accept the same set of pods. Lots of nodes.
	options1b := []expander.Option{
		{
			NodeGroup: ng1,
			NodeCount: 80,
			Pods:      []*apiv1.Pod{p1, p2},
			Debug:     "ng1",
		},
		{
			NodeGroup: ng2,
			NodeCount: 40,
			Pods:      []*apiv1.Pod{p1, p2},
			Debug:     "ng2",
		},
	}
	// First node group is cheaper, the second is preferred
	// but there is lots of nodes to be created.
	pricingModel = &testPricingModel{
		podPrice: map[string]float64{
			"p1":        20.0,
			"p2":        10.0,
			"stabilize": 10,
		},
		nodePrice: map[string]float64{
			"n1": 20.0,
			"n2": 200.0,
		},
	}
	assert.Contains(t, NewTestStrategy(provider, pricingModel, 4, WithPvmUnfitnessPenalty()).BestOption(options1b, nodeInfosForGroups).Debug, "ng1")

	// Second node group is cheaper
	pricingModel = &testPricingModel{
		podPrice: map[string]float64{
			"p1":        20.0,
			"p2":        10.0,
			"stabilize": 10,
		},
		nodePrice: map[string]float64{
			"n1": 200.0,
			"n2": 100.0,
		},
	}
	assert.Contains(t, NewTestStrategy(provider, pricingModel, 2, WithPvmUnfitnessPenalty()).BestOption(options, nodeInfosForGroups).Debug, "ng2")

	// First group accept 1 pod and second accepts 2.
	options2 := []expander.Option{
		{
			NodeGroup: ng1,
			NodeCount: 2,
			Pods:      []*apiv1.Pod{p1},
			Debug:     "ng1",
		},
		{
			NodeGroup: ng2,
			NodeCount: 1,
			Pods:      []*apiv1.Pod{p1, p2},
			Debug:     "ng2",
		},
	}
	pricingModel = &testPricingModel{
		podPrice: map[string]float64{
			"p1":        20.0,
			"p2":        10.0,
			"stabilize": 10,
		},
		nodePrice: map[string]float64{
			"n1": 200.0,
			"n2": 200.0,
		},
	}
	// Both node groups are equally expensive. However 2
	// accept two pods.
	assert.Contains(t, NewTestStrategy(provider, pricingModel, 2, WithPvmUnfitnessPenalty()).BestOption(options2, nodeInfosForGroups).Debug, "ng2")

	// Errors are expected
	pricingModel = &testPricingModel{
		podPrice:  map[string]float64{},
		nodePrice: map[string]float64{},
	}
	assert.Nil(t, NewTestStrategy(provider, pricingModel, 2, WithPvmUnfitnessPenalty()).BestOption(options2, nodeInfosForGroups))

	// Add node info for autoprovisioned group.
	nodeInfosForGroups["autoprovisioned-MT1"] = ni3
	// First group accept 1 pod, second accepts 2 and third accepts 2 (non-existent autoprovisioned)
	options3 := []expander.Option{
		{
			NodeGroup: ng1,
			NodeCount: 2,
			Pods:      []*apiv1.Pod{p1},
			Debug:     "ng1",
		},
		{
			NodeGroup: ng2,
			NodeCount: 1,
			Pods:      []*apiv1.Pod{p1, p2},
			Debug:     "ng2",
		},
		{
			NodeGroup: ng3,
			NodeCount: 1,
			Pods:      []*apiv1.Pod{p1, p2},
			Debug:     "ng3",
		},
	}
	// Choose existing group when non-existing has the same price.
	pricingModel = &testPricingModel{
		podPrice: map[string]float64{
			"p1":        20.0,
			"p2":        10.0,
			"stabilize": 10,
		},
		nodePrice: map[string]float64{
			"n1": 200.0,
			"n2": 200.0,
			"n3": 200.0,
		},
	}
	assert.Contains(t, NewTestStrategy(provider, pricingModel, 2, WithPvmUnfitnessPenalty()).BestOption(options3, nodeInfosForGroups).Debug, "ng2")

	// Choose non-existing group when non-existing is cheaper.
	pricingModel = &testPricingModel{
		podPrice: map[string]float64{
			"p1":        20.0,
			"p2":        10.0,
			"stabilize": 10,
		},
		nodePrice: map[string]float64{
			"n1": 200.0,
			"n2": 200.0,
			"n3": 90.0,
		},
	}
	assert.Contains(t, NewTestStrategy(provider, pricingModel, 2, WithPvmUnfitnessPenalty()).BestOption(options3, nodeInfosForGroups).Debug, "ng3")
}

func TestBestOption_HighCPUPreemptionVmOverHighMemPreemptionVm(t *testing.T) {
	for _, preemptionLabel := range []string{labels.PreemptibleLabel, labels.SpotLabel} {
		t.Run(preemptionLabel, func(t *testing.T) {
			pricingModel := gce.NewGcePriceModel(gke.NewGkePriceInfo(machinetypes.NewMachineConfigProvider(nil), false), localssdsize.NewSimpleLocalSSDProvider())

			preemptionHicpu := BuildTestNode("n1", 32000, 28*units.GiB)
			preemptionHicpu.Labels[apiv1.LabelInstanceType] = "n1-highcpu-32"
			preemptionHicpu.Labels[preemptionLabel] = labels.PreemptionValue

			preemptionHimem := BuildTestNode("n2", 16000, 104*units.GiB)
			preemptionHimem.Labels[apiv1.LabelInstanceType] = "n1-highmem-16"
			preemptionHimem.Labels[preemptionLabel] = labels.PreemptionValue

			himem := BuildTestNode("n3", 16000, 104*units.GiB)
			himem.Labels[apiv1.LabelInstanceType] = "n1-highmem-16"

			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			provider.AddNodeGroup("prem-n1-highcpu-32", 1, 1000, 1000)
			provider.AddNodeGroup("prem-n1-highmem-16", 1, 1000, 1000)
			provider.AddNodeGroup("n1-highmem-16", 1, 1000, 1000)
			provider.AddNode("prem-n1-highcpu-32", preemptionHicpu)
			provider.AddNode("prem-n1-highmem-16", preemptionHimem)
			provider.AddNode("n1-highmem-16", himem)
			ng1, _ := provider.NodeGroupForNode(preemptionHicpu)
			ng2, _ := provider.NodeGroupForNode(preemptionHimem)
			ng3, _ := provider.NodeGroupForNode(himem)

			ni1 := framework.NewTestNodeInfo(preemptionHicpu)
			ni2 := framework.NewTestNodeInfo(preemptionHimem)
			ni3 := framework.NewTestNodeInfo(himem)

			nodeInfosForGroups := map[string]*framework.NodeInfo{
				"prem-n1-highcpu-32": ni1, "prem-n1-highmem-16": ni2, "n1-highmem-16": ni3,
			}

			p1 := BuildTestPod("p1", 15, 28*units.GiB)

			options := []expander.Option{
				{
					NodeGroup: ng1,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{p1},
					Debug:     "ng1",
				},
				{
					NodeGroup: ng2,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{p1},
					Debug:     "ng2",
				},
				{
					NodeGroup: ng3,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{p1},
					Debug:     "ng3",
				},
			}

			assert.Contains(t, NewTestStrategy(provider, pricingModel, 2, WithPvmUnfitnessPenalty()).BestOption(options, nodeInfosForGroups).Debug, "ng1")

			assert.Contains(t, NewTestStrategy(provider, pricingModel, 2).BestOption(options, nodeInfosForGroups).Debug, "ng2")
		})
	}
}

func TestBestOption_UnfitPreemptionVmOverNonPreemptibleOfSameSize(t *testing.T) {
	for _, preemptionLabel := range []string{labels.PreemptibleLabel, labels.SpotLabel} {
		t.Run(preemptionLabel, func(t *testing.T) {

			pricingModel := gce.NewGcePriceModel(gce.NewGcePriceInfo(), localssdsize.NewSimpleLocalSSDProvider())

			preemptionHimem := BuildTestNode("n2", 16000, 104*units.GiB)
			preemptionHimem.Labels[apiv1.LabelInstanceType] = "n1-highmem-16"
			preemptionHimem.Labels[preemptionLabel] = labels.PreemptionValue

			himem := BuildTestNode("n3", 16000, 104*units.GiB)
			himem.Labels[apiv1.LabelInstanceType] = "n1-highmem-16"

			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			provider.AddNodeGroup("prem-n1-highmem-16", 1, 1000, 1000)
			provider.AddNodeGroup("n1-highmem-16", 1, 1000, 1000)
			provider.AddNode("prem-n1-highmem-16", preemptionHimem)
			provider.AddNode("n1-highmem-16", himem)
			ng1, _ := provider.NodeGroupForNode(preemptionHimem)
			ng2, _ := provider.NodeGroupForNode(himem)

			ni1 := framework.NewTestNodeInfo(preemptionHimem)
			ni2 := framework.NewTestNodeInfo(himem)

			nodeInfosForGroups := map[string]*framework.NodeInfo{
				"prem-n1-highmem-16": ni1, "n1-highmem-16": ni2,
			}

			p1 := BuildTestPod("p1", 15, 28*units.GiB)

			options := []expander.Option{
				{
					NodeGroup: ng1,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{p1},
					Debug:     "ng1",
				},
				{
					NodeGroup: ng2,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{p1},
					Debug:     "ng2",
				},
			}

			assert.Contains(t, NewTestStrategy(provider, pricingModel, 2, WithPvmUnfitnessPenalty()).BestOption(options, nodeInfosForGroups).Debug, "ng1")
		})
	}
}

// RequestCustomGpuForPod modifies pod's resource requests by adding a number of GPUs of specified type to them.
func RequestCustomGpuForPod(pod *apiv1.Pod, gpusCount int64, gpuType string) {
	RequestGpuForPod(pod, gpusCount)
	pod.Spec.NodeSelector = map[string]string{gce.GPULabel: gpuType}
}

// AddCustomGpusToNode adds GPU capacity of the specified GPU type to given node.
func AddCustomGpusToNode(node *apiv1.Node, gpusCount int64, gpuType string) {
	AddGpusToNode(node, gpusCount)
	// Override the default GPU type with custom type
	node.Labels[gce.GPULabel] = gpuType
}

func TestBestOption_CustomGPU(t *testing.T) {
	NvidiaTeslaA100 := "nvidia-tesla-a100"

	pricingModel := gce.NewGcePriceModel(gke.NewGkePriceInfo(machinetypes.NewMachineConfigProvider(nil), false), localssdsize.NewSimpleLocalSSDProvider())

	a2VM1Gpu := BuildTestNode("a2-1g", 12000, 85*units.GiB)
	a2VM1Gpu.Labels[apiv1.LabelInstanceType] = "a2-highgpu-1g"
	AddCustomGpusToNode(a2VM1Gpu, 1, NvidiaTeslaA100)

	a2VM2Gpus := BuildTestNode("a2-2g", 24000, 170*units.GiB)
	a2VM2Gpus.Labels[apiv1.LabelInstanceType] = "a2-highgpu-2g"
	AddCustomGpusToNode(a2VM2Gpus, 2, NvidiaTeslaA100)

	a2VM4Gpus := BuildTestNode("a2-4g", 48000, 340*units.GiB)
	a2VM4Gpus.Labels[apiv1.LabelInstanceType] = "a2-highgpu-4g"
	AddCustomGpusToNode(a2VM4Gpus, 4, NvidiaTeslaA100)

	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		Build()

	provider.AddNodeGroup("node-group-a2-highgpu-1g", 1, 1000, 1000)
	provider.AddNodeGroup("node-group-a2-highgpu-2g", 1, 1000, 1000)
	provider.AddNodeGroup("node-group-a2-highgpu-4g", 1, 1000, 1000)
	provider.AddNode("node-group-a2-highgpu-1g", a2VM1Gpu)
	provider.AddNode("node-group-a2-highgpu-2g", a2VM2Gpus)
	provider.AddNode("node-group-a2-highgpu-4g", a2VM4Gpus)

	ng1, _ := provider.NodeGroupForNode(a2VM1Gpu)
	ng2, _ := provider.NodeGroupForNode(a2VM2Gpus)
	ng4, _ := provider.NodeGroupForNode(a2VM4Gpus)

	ni1 := framework.NewTestNodeInfo(a2VM1Gpu)
	ni2 := framework.NewTestNodeInfo(a2VM2Gpus)
	ni4 := framework.NewTestNodeInfo(a2VM4Gpus)

	nodeInfosForGroups := map[string]*framework.NodeInfo{
		"node-group-a2-highgpu-1g": ni1, "node-group-a2-highgpu-2g": ni2, "node-group-a2-highgpu-4g": ni4,
	}

	pHiMem := BuildTestPod("pHiMem", 1000, 96*units.GiB)
	RequestCustomGpuForPod(pHiMem, 1, NvidiaTeslaA100)
	pLoMem := BuildTestPod("pLoMem", 1000, 1*units.GiB)
	RequestCustomGpuForPod(pLoMem, 1, NvidiaTeslaA100)

	tests := []struct {
		name       string
		options    []expander.Option
		bestOption string
	}{
		{
			name: "We prefer the cheapest machine with least GPUs",
			options: []expander.Option{
				{
					NodeGroup: ng1,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{pLoMem},
					Debug:     "ng1",
				},
				{
					NodeGroup: ng2,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{pLoMem},
					Debug:     "ng2",
				},
				{
					NodeGroup: ng4,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{pLoMem},
					Debug:     "ng4",
				},
			},
			bestOption: "ng1",
		},
		{
			name: "We prefer 2 separate smaller machines than a single one fitting both pods",
			options: []expander.Option{
				{
					NodeGroup: ng1,
					NodeCount: 2,
					Pods:      []*apiv1.Pod{pLoMem, pLoMem},
					Debug:     "ng1",
				},
				{
					NodeGroup: ng2,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{pLoMem, pLoMem},
					Debug:     "ng2",
				},
				{
					NodeGroup: ng4,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{pLoMem, pLoMem},
					Debug:     "ng4",
				},
			},
			bestOption: "ng1",
		},
		{
			name: "We prefer multiple separate smaller machines than bigger ones fitting multiple pods per node",
			options: []expander.Option{
				{
					NodeGroup: ng1,
					NodeCount: 4,
					Pods:      []*apiv1.Pod{pLoMem, pLoMem, pLoMem, pLoMem},
					Debug:     "ng1",
				},
				{
					NodeGroup: ng2,
					NodeCount: 2,
					Pods:      []*apiv1.Pod{pLoMem, pLoMem, pLoMem, pLoMem},
					Debug:     "ng2",
				},
				{
					NodeGroup: ng4,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{pLoMem, pLoMem, pLoMem, pLoMem},
					Debug:     "ng4",
				},
			},
			bestOption: "ng1",
		},
		{
			name: "We prefer the cheaper machine fitting only 1 pod (especially cause otherwise 2 GPU pods would run on the same node)",
			options: []expander.Option{
				{
					NodeGroup: ng1,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{pLoMem},
					Debug:     "ng1",
				},
				{
					NodeGroup: ng2,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{pHiMem, pLoMem},
					Debug:     "ng2",
				},
				{
					NodeGroup: ng4,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{pHiMem, pLoMem},
					Debug:     "ng4",
				},
			},
			bestOption: "ng1",
		},
		{
			name: "We prefer the cheapest machine fitting the pod",
			options: []expander.Option{
				{
					NodeGroup: ng2,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{pHiMem},
					Debug:     "ng2",
				},
				{
					NodeGroup: ng4,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{pHiMem},
					Debug:     "ng4",
				},
			},
			bestOption: "ng2",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Contains(t, NewTestStrategy(provider, pricingModel, 1, WithPvmUnfitnessPenalty()).BestOption(tc.options, nodeInfosForGroups).Debug, tc.bestOption)
		})
	}
}

func TestBestOption_VariousGPUs(t *testing.T) {
	NvidiaTeslaV100 := "nvidia-tesla-v100"
	NvidiaTeslaK80 := "nvidia-tesla-k80"

	pricingModel := gce.NewGcePriceModel(gke.NewGkePriceInfo(machinetypes.NewMachineConfigProvider(nil), false), localssdsize.NewSimpleLocalSSDProvider())

	n1VM := BuildTestNode("n1", 8000, 30*units.GiB)
	n1VM.Labels[apiv1.LabelInstanceType] = "n1-standard-8"

	n1VM1GpuV100 := BuildTestNode("n1-v100", 8000, 30*units.GiB)
	n1VM1GpuV100.Labels[apiv1.LabelInstanceType] = "n1-standard-8"
	AddCustomGpusToNode(n1VM1GpuV100, 1, NvidiaTeslaV100)

	n1VM1GpuK80 := BuildTestNode("n1-k80", 8000, 30*units.GiB)
	n1VM1GpuK80.Labels[apiv1.LabelInstanceType] = "n1-standard-8"
	AddCustomGpusToNode(n1VM1GpuK80, 1, NvidiaTeslaK80)

	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		Build()

	provider.AddNodeGroup("node-group-n1", 1, 1000, 1000)
	provider.AddNodeGroup("node-group-n1-v100", 1, 1000, 1000)
	provider.AddNodeGroup("node-group-n1-k80", 1, 1000, 1000)
	provider.AddNode("node-group-n1", n1VM)
	provider.AddNode("node-group-n1-v100", n1VM1GpuV100)
	provider.AddNode("node-group-n1-k80", n1VM1GpuK80)

	ng, _ := provider.NodeGroupForNode(n1VM)
	ngV100, _ := provider.NodeGroupForNode(n1VM1GpuV100)
	ngK80, _ := provider.NodeGroupForNode(n1VM1GpuK80)

	ni := framework.NewTestNodeInfo(n1VM)
	niV100 := framework.NewTestNodeInfo(n1VM1GpuV100)
	niK80 := framework.NewTestNodeInfo(n1VM1GpuK80)

	nodeInfosForGroups := map[string]*framework.NodeInfo{
		"node-group-n1": ni, "node-group-n1-v100": niV100, "node-group-n1-k80": niK80,
	}

	pV100 := BuildTestPod("pV100", 4000, 16*units.GiB)
	RequestCustomGpuForPod(pV100, 1, NvidiaTeslaV100)
	pK80 := BuildTestPod("pK80", 4000, 16*units.GiB)
	RequestCustomGpuForPod(pK80, 1, NvidiaTeslaK80)
	pNoGpus := BuildTestPod("pNoGpus", 4000, 16*units.GiB)
	pAnyGpu := BuildTestPod("pNoGpus", 4000, 16*units.GiB)
	RequestGpuForPod(pAnyGpu, 1)

	tests := []struct {
		name       string
		options    []expander.Option
		bestOption string
	}{
		{
			name: "We prefer the cheapest machine first (the one with no GPUs)",
			options: []expander.Option{
				{
					NodeGroup: ng,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{pNoGpus},
					Debug:     "ng",
				},
				{
					NodeGroup: ngV100,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{pV100},
					Debug:     "ngV100",
				},
				{
					NodeGroup: ngK80,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{pK80},
					Debug:     "ngK80",
				},
			},
			bestOption: "ng",
		},
		{
			name: "We prefer the cheapest machine for a pod requiring any GPU",
			options: []expander.Option{
				{
					NodeGroup: ngV100,
					NodeCount: 2,
					Pods:      []*apiv1.Pod{pAnyGpu, pV100},
					Debug:     "ngV100",
				},
				{
					NodeGroup: ngK80,
					NodeCount: 2,
					Pods:      []*apiv1.Pod{pAnyGpu, pK80},
					Debug:     "ngK80",
				},
			},
			bestOption: "ngK80",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Contains(t, NewTestStrategy(provider, pricingModel, 1, WithPvmUnfitnessPenalty()).BestOption(tc.options, nodeInfosForGroups).Debug, tc.bestOption)
		})
	}
}

func TestBestOption_ReservationsIncludedInTotalNodePrice(t *testing.T) {
	tests := []struct {
		name         string
		reservations []*gce_api.Reservation
		machineType  string
		zone         string
		nodeCount    int
		nodePrice    float64

		wantTotalReservations int64
		wantTotalNodePrice    float64
	}{
		{
			name:        "no reservations",
			machineType: "some-machine",
			zone:        "zone-A",
			nodeCount:   10,
			nodePrice:   100.0,

			wantTotalReservations: 0,
			wantTotalNodePrice:    1000.0,
		},
		{
			name: "not matching machine type",
			reservations: []*gce_api.Reservation{
				reservations.BuildMultipleMachineReservation("other-machine", "zone-A", 0, 5),
			},
			machineType: "some-machine",
			zone:        "zone-A",
			nodeCount:   10,
			nodePrice:   100.0,

			wantTotalReservations: 0,
			wantTotalNodePrice:    1000.0,
		},
		{
			name: "not matching zone",
			reservations: []*gce_api.Reservation{
				reservations.BuildMultipleMachineReservation("some-machine", "zone-B", 0, 5),
			},
			machineType: "some-machine",
			zone:        "zone-A",
			nodeCount:   10,
			nodePrice:   100.0,

			wantTotalReservations: 0,
			wantTotalNodePrice:    1000.0,
		},
		{
			name: "half of the nodes are reserved",
			reservations: []*gce_api.Reservation{
				reservations.BuildMultipleMachineReservation("some-machine", "zone-A", 0, 5),
			},
			machineType: "some-machine",
			zone:        "zone-A",
			nodeCount:   10,
			nodePrice:   100.0,

			wantTotalReservations: 5,
			wantTotalNodePrice:    510.0,
		},
		{
			name: "all of the nodes are reserved",
			reservations: []*gce_api.Reservation{
				reservations.BuildMultipleMachineReservation("some-machine", "zone-A", 0, 10),
			},
			machineType: "some-machine",
			zone:        "zone-A",
			nodeCount:   10,
			nodePrice:   100.0,

			wantTotalReservations: 10,
			wantTotalNodePrice:    20.0,
		},
		{
			name: "even more nodes are reserved",
			reservations: []*gce_api.Reservation{
				reservations.BuildMultipleMachineReservation("some-machine", "zone-A", 0, 20),
			},
			machineType: "some-machine",
			zone:        "zone-A",
			nodeCount:   10,
			nodePrice:   100.0,

			wantTotalReservations: 10,
			wantTotalNodePrice:    20.0,
		},
		{
			name: "all of the nodes are reserved by multiple reservations",
			reservations: []*gce_api.Reservation{
				reservations.BuildMultipleMachineReservationWithId(0, 0, 5, "some-machine", "zone-A"),
				reservations.BuildMultipleMachineReservationWithId(1, 0, 5, "some-machine", "zone-A"),
			},
			machineType: "some-machine",
			zone:        "zone-A",
			nodeCount:   10,
			nodePrice:   100.0,

			wantTotalReservations: 10,
			wantTotalNodePrice:    20.0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := BuildTestNode("n", 1000, 1000)
			node.Labels[apiv1.LabelInstanceType] = tc.machineType

			nodeGroup := gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{MachineType: tc.machineType}).
				SetGceRef(gce.GceRef{Name: "ng", Zone: tc.zone}).Build()

			nodeInfo := framework.NewTestNodeInfo(node)

			nodeInfos := map[string]*framework.NodeInfo{
				nodeGroup.Id(): nodeInfo,
			}

			options := []expander.Option{
				{
					NodeGroup: nodeGroup,
					NodeCount: tc.nodeCount,
					Pods:      nil,
					Debug:     "ng",
				},
			}

			pricingModel := &testPricingModel{
				nodePrice: map[string]float64{
					"n": tc.nodePrice,
				},
			}
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()

			gotDebug := NewTestStrategy(provider, pricingModel, 2, WithReservations(tc.reservations)).BestOption(options, nodeInfos).Debug
			assert.Contains(t, gotDebug, fmt.Sprintf("total_reservations=%d", tc.wantTotalReservations))
			assert.Contains(t, gotDebug, fmt.Sprintf("all_nodes_price=%f", tc.wantTotalNodePrice))
		})
	}
}

func TestBestOption_ReservationDiscount(t *testing.T) {
	zone := "some-zone"
	node2 := BuildTestNode("n2", 2000, 2000)
	node2.Labels[apiv1.LabelInstanceType] = "standard-2"
	node16 := BuildTestNode("n16", 16000, 16000)
	node16.Labels[apiv1.LabelInstanceType] = "standard-16"
	node32 := BuildTestNode("n32", 32000, 32000)
	node32.Labels[apiv1.LabelInstanceType] = "standard-32"

	nodeInfo2 := framework.NewTestNodeInfo(node2)
	nodeInfo16 := framework.NewTestNodeInfo(node16)
	nodeInfo32 := framework.NewTestNodeInfo(node32)

	pod := BuildTestPod("p", 1000, 1000)

	pricingModel := &testPricingModel{
		nodePrice: map[string]float64{
			"n2":  2,
			"n16": 16,
			"n32": 32,
		},
		podPrice: map[string]float64{
			"p": 1,
		},
	}

	tests := []struct {
		name         string
		reservations []*gce_api.Reservation

		cheaperNodeInfo        *framework.NodeInfo
		cheaperNodeGroupExists bool
		otherNodeInfo          *framework.NodeInfo
		otherNodeGroupExists   bool
	}{
		{
			name:            "no reservations, 2 < 16",
			cheaperNodeInfo: nodeInfo2,
			otherNodeInfo:   nodeInfo16,
		},
		{
			name:            "no reservations, 2 < 32",
			cheaperNodeInfo: nodeInfo2,
			otherNodeInfo:   nodeInfo16,
		},
		{
			name: "16 core reservation, non-existing 16 < existing 2",
			reservations: []*gce_api.Reservation{
				reservations.BuildMultipleMachineReservation("standard-16", zone, 0, 1),
			},
			cheaperNodeInfo:        nodeInfo16,
			cheaperNodeGroupExists: false,
			otherNodeInfo:          nodeInfo2,
			otherNodeGroupExists:   true,
		},
		{
			name: "32 core reservation, existing 32 < existing 2",
			reservations: []*gce_api.Reservation{
				reservations.BuildMultipleMachineReservation("standard-32", zone, 0, 1),
			},
			cheaperNodeInfo:        nodeInfo32,
			cheaperNodeGroupExists: true,
			otherNodeInfo:          nodeInfo2,
			otherNodeGroupExists:   true,
		},
		{
			name: "32 core reservation, existing 2 < non-existing 32",
			reservations: []*gce_api.Reservation{
				reservations.BuildMultipleMachineReservation("standard-32", zone, 0, 1),
			},
			cheaperNodeInfo:        nodeInfo2,
			cheaperNodeGroupExists: true,
			otherNodeInfo:          nodeInfo32,
			otherNodeGroupExists:   false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cheaperSpec := &gkeclient.NodePoolSpec{
				MachineType:    tc.cheaperNodeInfo.Node().Labels[apiv1.LabelInstanceType],
				LocalSSDConfig: &gkeclient.LocalSSDConfig{LocalSsdCount: 0},
			}
			otherSpec := &gkeclient.NodePoolSpec{
				MachineType:    tc.otherNodeInfo.Node().Labels[apiv1.LabelInstanceType],
				LocalSSDConfig: &gkeclient.LocalSSDConfig{LocalSsdCount: 0},
			}

			cheaperNodeGroup := gke.NewTestGkeMigBuilder().SetSpec(cheaperSpec).SetGceRef(gce.GceRef{Name: "cheaper", Zone: zone}).SetExist(tc.cheaperNodeGroupExists).Build()
			otherNodeGroup := gke.NewTestGkeMigBuilder().SetSpec(otherSpec).SetGceRef(gce.GceRef{Name: "other", Zone: zone}).SetExist(tc.otherNodeGroupExists).Build()

			nodeInfos := map[string]*framework.NodeInfo{
				cheaperNodeGroup.Id(): tc.cheaperNodeInfo,
				otherNodeGroup.Id():   tc.otherNodeInfo,
			}

			options := []expander.Option{
				{
					NodeGroup: cheaperNodeGroup,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{pod},
					Debug:     "cheaper",
				},
				{
					NodeGroup: otherNodeGroup,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{pod},
					Debug:     "other",
				},
			}
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()

			bestOption := NewTestStrategy(provider, pricingModel, 4, WithReservations(tc.reservations)).BestOption(options, nodeInfos)
			assert.Contains(t, bestOption.Debug, "cheaper")
		})
	}
}

func TestBestOption_NewNodeGroup(t *testing.T) {
	zone := "some-zone"
	node10 := BuildTestNode("n10", 10000, 10000)
	node11 := BuildTestNode("n11", 11000, 11000)

	nodeInfo10 := framework.NewTestNodeInfo(node10)
	nodeInfo11 := framework.NewTestNodeInfo(node11)

	pod := BuildTestPod("p", 1000, 1000)

	pricingModel := &testPricingModel{
		nodePrice: map[string]float64{
			"n10": 1,
			"n11": 1.1,
		},
		podPrice: map[string]float64{
			"p": 1,
		},
	}
	tests := []struct {
		name                   string
		relaxedGroupPenalty    bool
		cheaperNodeInfo        *framework.NodeInfo
		cheaperNodeGroupExists bool
		otherNodeInfo          *framework.NodeInfo
		otherNodeGroupExists   bool
		wantExpensive          bool
	}{
		{
			name:                   "cheaper preferred when relaxed group penalty is disabled and both NGs exist",
			relaxedGroupPenalty:    false,
			cheaperNodeInfo:        nodeInfo10,
			cheaperNodeGroupExists: true,
			otherNodeInfo:          nodeInfo11,
			otherNodeGroupExists:   true,
		},
		{
			name:                   "cheaper preferred when relaxed group penalty is enabled and both NGs exist",
			relaxedGroupPenalty:    true,
			cheaperNodeInfo:        nodeInfo10,
			cheaperNodeGroupExists: true,
			otherNodeInfo:          nodeInfo11,
			otherNodeGroupExists:   true,
		},
		{
			name:                   "cheaper preferred when relaxed group penalty is disabled and neither NG exists",
			relaxedGroupPenalty:    false,
			cheaperNodeInfo:        nodeInfo10,
			cheaperNodeGroupExists: false,
			otherNodeInfo:          nodeInfo11,
			otherNodeGroupExists:   false,
		},
		{
			name:                   "expensive preferred when relaxed group penalty is disabled and cheaper NG doesn't exist",
			relaxedGroupPenalty:    false,
			cheaperNodeInfo:        nodeInfo10,
			cheaperNodeGroupExists: false,
			otherNodeInfo:          nodeInfo11,
			otherNodeGroupExists:   true,
			wantExpensive:          true,
		},
		{
			name:                   "non-existing cheaper preferred over when relaxed group penalty is enabled and only expensive NG exists",
			relaxedGroupPenalty:    true,
			cheaperNodeInfo:        nodeInfo10,
			cheaperNodeGroupExists: false,
			otherNodeInfo:          nodeInfo11,
			otherNodeGroupExists:   true,
			wantExpensive:          false,
		},
		{
			name:                   "cheaper preferred when relaxed group penalty is enabled and only expensive NG exists",
			relaxedGroupPenalty:    true,
			cheaperNodeInfo:        nodeInfo10,
			cheaperNodeGroupExists: false,
			otherNodeInfo:          nodeInfo11,
			otherNodeGroupExists:   true,
		},
		{
			name:                   "existing identical preferred when relaxed group penalty is enabled over non-existing",
			relaxedGroupPenalty:    true,
			cheaperNodeInfo:        nodeInfo10,
			cheaperNodeGroupExists: true,
			otherNodeInfo:          nodeInfo10,
			otherNodeGroupExists:   false,
		},
		{
			name:                   "existing identical preferred when relaxed group penalty is disabled over non-existing",
			relaxedGroupPenalty:    false,
			cheaperNodeInfo:        nodeInfo10,
			cheaperNodeGroupExists: true,
			otherNodeInfo:          nodeInfo10,
			otherNodeGroupExists:   false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cheaperSpec := &gkeclient.NodePoolSpec{
				MachineType:    tc.cheaperNodeInfo.Node().Labels[apiv1.LabelInstanceType],
				LocalSSDConfig: &gkeclient.LocalSSDConfig{LocalSsdCount: 0},
			}
			otherSpec := &gkeclient.NodePoolSpec{
				MachineType:    tc.otherNodeInfo.Node().Labels[apiv1.LabelInstanceType],
				LocalSSDConfig: &gkeclient.LocalSSDConfig{LocalSsdCount: 0},
			}

			cheaperNodeGroup := gke.NewTestGkeMigBuilder().SetSpec(cheaperSpec).SetGceRef(gce.GceRef{Name: "cheaper", Zone: zone}).SetExist(tc.cheaperNodeGroupExists).Build()
			otherNodeGroup := gke.NewTestGkeMigBuilder().SetSpec(otherSpec).SetGceRef(gce.GceRef{Name: "other", Zone: zone}).SetExist(tc.otherNodeGroupExists).Build()

			nodeInfos := map[string]*framework.NodeInfo{
				cheaperNodeGroup.Id(): tc.cheaperNodeInfo,
				otherNodeGroup.Id():   tc.otherNodeInfo,
			}

			options := []expander.Option{
				{
					NodeGroup: cheaperNodeGroup,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{pod},
					Debug:     "cheaper",
				},
				{
					NodeGroup: otherNodeGroup,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{pod},
					Debug:     "expensive",
				},
			}
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()

			bestOption := NewTestStrategy(provider, pricingModel, 1, WithRelaxedGroupPenaltyChecker(NewStaticRelaxedGroupPenaltyChecker(tc.relaxedGroupPenalty))).BestOption(options, nodeInfos)
			var want string
			if tc.wantExpensive {
				want = "expensive"
			} else {
				want = "cheaper"
			}
			assert.Contains(t, bestOption.Debug, want)
		})
	}
}

func TestUpcomingNodeGroups(t *testing.T) {
	upcomingNode := BuildTestNode("upcoming", 1000, 1000)
	injectedNode := BuildTestNode("injected", 1000, 1000)
	existingNode := BuildTestNode("existing", 1000, 1000)

	spec := &gkeclient.NodePoolSpec{
		MachineType:    upcomingNode.Labels[apiv1.LabelInstanceType],
		LocalSSDConfig: &gkeclient.LocalSSDConfig{LocalSsdCount: 0},
	}
	upcoming := gke.NewTestGkeMigBuilder().SetSpec(spec).SetGceRef(gce.GceRef{Name: "upcoming", Zone: "us-central1-c"}).SetExist(false).Build()
	injected := gke.NewTestGkeMigBuilder().SetSpec(spec).SetGceRef(gce.GceRef{Name: "injected", Zone: "us-central1-c"}).SetExist(false).Build()
	existing := gke.NewTestGkeMigBuilder().SetSpec(spec).SetGceRef(gce.GceRef{Name: "existing", Zone: "us-central1-c"}).SetExist(true).Build()
	nodeInfosForGroups := map[string]*framework.NodeInfo{
		upcoming.Id(): framework.NewTestNodeInfo(upcomingNode),
		injected.Id(): framework.NewTestNodeInfo(injectedNode),
		existing.Id(): framework.NewTestNodeInfo(existingNode),
	}

	p := BuildTestPod("p", 1000, 0)

	for _, tc := range []struct {
		desc          string
		regularPrice  float64
		injectedPrice float64
		upcomingPrice float64
		wantNg        string
	}{
		{
			desc:          "NAP has penalty",
			upcomingPrice: 1000.0,
			injectedPrice: 99.99,
			regularPrice:  100.0,
			wantNg:        existing.Id(),
		},
		{
			desc:          "Upcoming doesn't have penalty",
			upcomingPrice: 99.99,
			injectedPrice: 1000.0,
			regularPrice:  100.0,
			wantNg:        upcoming.Id(),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			pricingModel := &testPricingModel{
				podPrice: map[string]float64{
					"p": 1.0,
				},
				nodePrice: map[string]float64{
					upcomingNode.Name: tc.upcomingPrice,
					injectedNode.Name: tc.injectedPrice,
					existingNode.Name: tc.regularPrice,
				},
			}
			options := []expander.Option{
				{
					NodeGroup: upcoming,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{p},
					Debug:     "upcoming",
				},
				{
					NodeGroup: injected,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{p},
					Debug:     "injected",
				},
				{
					NodeGroup: existing,
					NodeCount: 1,
					Pods:      []*apiv1.Pod{p},
					Debug:     "existing",
				},
			}
			uc := &asyncnodegroups.MockAsyncNodeGroupStateChecker{IsUpcomingNodeGroup: map[string]bool{
				upcoming.Id(): true,
			}}
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			strategy := NewTestStrategy(provider, pricingModel, 2, WithUpcomingChecker(uc))
			gotOption := strategy.BestOption(options, nodeInfosForGroups)
			if gotOption == nil {
				t.Errorf("strategy returned nil option")
			} else if gotOption.NodeGroup.Id() != tc.wantNg {
				t.Errorf("Incorrect node group selected, want %s, got %s", tc.wantNg, gotOption.NodeGroup.Id())
			}
		})
	}
}
