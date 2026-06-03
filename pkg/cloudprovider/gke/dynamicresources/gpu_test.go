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

package dynamicresources

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	mt "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/utils/ptr"
)

func TestGpuResourceSlicesForNode(t *testing.T) {
	gpuTemplateNode := test.BuildTestNode("node1", 1, 1)
	gpuTemplateNode.Labels[labels.GPULabel] = "exists"
	noAcceleratorTemplateNode := test.BuildTestNode("node1", 1, 1)

	tests := map[string]struct {
		withAcceleratorLabel   bool
		acceleratorConfig      []GpuConfig
		expectedResourceSlices []*resourceapi.ResourceSlice
	}{
		"EmptyAcceleratorConfig": {
			withAcceleratorLabel:   true,
			acceleratorConfig:      []GpuConfig{},
			expectedResourceSlices: []*resourceapi.ResourceSlice{},
		},
		"SingleGPUTeslaT4": {
			withAcceleratorLabel: true,
			acceleratorConfig: []GpuConfig{
				{
					Count: int64(1),
					Type:  "nvidia-tesla-t4",
				},
			},
			expectedResourceSlices: []*resourceapi.ResourceSlice{
				buildResourceSlice("node1-gpu.nvidia.com-0-slice-0", "node1", gpuSlicePoolName("node1", 0), "gpu.nvidia.com", 1, []resourceapi.Device{
					buildGpuDevice("gpu-0", "Turing", "Tesla T4", 0, 15),
				}),
			},
		},
		"EmptyAcceleratorConfig_NoAcceleratorLabel": {
			withAcceleratorLabel:   false,
			acceleratorConfig:      []GpuConfig{},
			expectedResourceSlices: []*resourceapi.ResourceSlice{},
		},
		"SingleGPUTeslaT4_NoAcceleratorLabel": {
			withAcceleratorLabel: false,
			acceleratorConfig: []GpuConfig{
				{
					Count: int64(1),
					Type:  "nvidia-tesla-t4",
				},
			},
			expectedResourceSlices: []*resourceapi.ResourceSlice{},
		},
		"SingleGPUTeslaT4_TwoDevices": {
			withAcceleratorLabel: true,
			acceleratorConfig: []GpuConfig{
				{
					Count: int64(2),
					Type:  "nvidia-tesla-t4",
				},
			},
			expectedResourceSlices: []*resourceapi.ResourceSlice{
				buildResourceSlice("node1-gpu.nvidia.com-0-slice-0", "node1", gpuSlicePoolName("node1", 0), "gpu.nvidia.com", 1, []resourceapi.Device{
					buildGpuDevice("gpu-0", "Turing", "Tesla T4", 0, 15),
					buildGpuDevice("gpu-1", "Turing", "Tesla T4", 1, 15),
				}),
			},
		},
		"SingleGPUTeslaA100_80gb": {
			withAcceleratorLabel: true,
			acceleratorConfig: []GpuConfig{
				{
					Count: int64(1),
					Type:  "nvidia-a100-80gb",
				},
			},
			expectedResourceSlices: []*resourceapi.ResourceSlice{
				buildResourceSlice("node1-gpu.nvidia.com-0-slice-0", "node1", gpuSlicePoolName("node1", 0), "gpu.nvidia.com", 1, []resourceapi.Device{
					buildGpuDevice("gpu-0", "Ampere", "A100 80GB", 0, 75),
				}),
			},
		},
		"MultipleDifferentGPUs": {
			withAcceleratorLabel: true,
			acceleratorConfig: []GpuConfig{
				{
					Count: int64(2),
					Type:  "nvidia-tesla-t4",
				},
				{
					Count: int64(4),
					Type:  "nvidia-a100-80gb",
				},
			},
			expectedResourceSlices: []*resourceapi.ResourceSlice{
				buildResourceSlice("node1-gpu.nvidia.com-0-slice-0", "node1", gpuSlicePoolName("node1", 0), "gpu.nvidia.com", 1, []resourceapi.Device{
					buildGpuDevice("gpu-0", "Turing", "Tesla T4", 0, 15),
					buildGpuDevice("gpu-1", "Turing", "Tesla T4", 1, 15),
				}),
				buildResourceSlice("node1-gpu.nvidia.com-1-slice-0", "node1", gpuSlicePoolName("node1", 1), "gpu.nvidia.com", 1, []resourceapi.Device{
					buildGpuDevice("gpu-0", "Ampere", "A100 80GB", 0, 75),
					buildGpuDevice("gpu-1", "Ampere", "A100 80GB", 1, 75),
					buildGpuDevice("gpu-2", "Ampere", "A100 80GB", 2, 75),
					buildGpuDevice("gpu-3", "Ampere", "A100 80GB", 3, 75),
				}),
			},
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			templateNode := gpuTemplateNode
			if !test.withAcceleratorLabel {
				templateNode = noAcceleratorTemplateNode
			}
			hwConfig := &NodeHardwareConfig{
				Accelerators: test.acceleratorConfig,
			}
			resourceSlices, err := gpuResourceSlicesForNode(mt.NewMachineConfigProvider(nil), hwConfig, templateNode)
			assert.NoError(t, err)
			removeSliceUIDsForAssertion(resourceSlices)
			assert.ElementsMatch(t, test.expectedResourceSlices, resourceSlices)
		})
	}
}

func TestGPUResourceSlicesDeviceSplit(t *testing.T) {
	countOfAccelerators := 1111
	gpuTemplateNode := test.BuildTestNode("node1", 1, 1)
	gpuTemplateNode.Labels[labels.GPULabel] = "exists"
	hwConfig := &NodeHardwareConfig{Accelerators: []GpuConfig{
		{
			Count: int64(countOfAccelerators),
			Type:  "nvidia-tesla-t4",
		},
	}}
	resourceSlices, err := gpuResourceSlicesForNode(mt.NewMachineConfigProvider(nil), hwConfig, gpuTemplateNode)
	assert.NoError(t, err)
	wantCountOfSlices := int(math.Ceil(float64(countOfAccelerators) / float64(resourceapi.ResourceSliceMaxDevices)))
	assert.Equal(t, wantCountOfSlices, len(resourceSlices))
}

func TestInvalidGPUTypeProvided(t *testing.T) {
	gpuTemplateNode := test.BuildTestNode("node1", 1, 1)
	gpuTemplateNode.Labels[labels.GPULabel] = "exists"
	hwConfig := &NodeHardwareConfig{Accelerators: []GpuConfig{
		{
			Count: 1,
			Type:  "not.gpu",
		},
	}}
	_, err := gpuResourceSlicesForNode(mt.NewMachineConfigProvider(nil), hwConfig, gpuTemplateNode)
	assert.ErrorContains(t, err, "unregistered GPU type: not.gpu")
}

func buildGpuDevice(deviceName, architecture, productName string, index, capacity int64) resourceapi.Device {
	return resourceapi.Device{
		Name: deviceName,
		Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
			"type": {
				StringValue: ptr.To("gpu"),
			},
			"architecture": {
				StringValue: ptr.To(architecture),
			},
			"brand": {
				StringValue: ptr.To("Nvidia"),
			},
			"productName": {
				StringValue: ptr.To(productName),
			},
			"uuid": {
				StringValue: ptr.To(""),
			},
			"index": {
				IntValue: ptr.To(index),
			},
			"minor": {
				IntValue: ptr.To(index),
			},
		},
		Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
			"memory": {
				Value: *resource.NewQuantity(capacity*2^30, resource.BinarySI),
			},
		},
	}
}
