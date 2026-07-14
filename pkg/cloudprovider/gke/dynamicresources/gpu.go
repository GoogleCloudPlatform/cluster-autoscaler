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
	"fmt"

	"github.com/google/uuid"
	apiv1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	mt "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/klog/v2"
)

const (
	GpuDeviceType = "gpu"
	GpuDriver     = "gpu.nvidia.com"
)

// GpuDraDriverEnabled checks whether GPU driver is enabled on the node
func GpuDraDriverEnabled(node *apiv1.Node) bool {
	return draDriverEnabled(node, gkelabels.DraGpuNodeLabel)
}

func gpuResourceSlicesForNode(mcp *mt.MachineConfigProvider, hwConfig *NodeHardwareConfig, templateNode *apiv1.Node) ([]*resourceapi.ResourceSlice, error) {
	if _, exists := templateNode.Labels[labels.GPULabel]; !exists {
		klog.V(4).Infof("Node without GPU label enables DRA GPU driver, not attaching resource slices")
		return nil, nil
	}

	var slices []*resourceapi.ResourceSlice
	for acceleratorIndex, accelerator := range hwConfig.Accelerators {
		var acceleratorDevices []resourceapi.Device
		numberOfDevices := accelerator.Count
		for deviceIndex := range numberOfDevices {
			deviceName := fmt.Sprintf("%s-%v", GpuDeviceType, deviceIndex)
			deviceUUID, err := uuid.NewRandom()
			if err != nil {
				return nil, fmt.Errorf("unable to generate device UUID: %w", err)
			}

			device, err := createDraGpuDevice(mcp, deviceName, GpuDeviceType, accelerator.Type, deviceUUID.String(), deviceIndex)
			if err != nil {
				return nil, err
			}
			acceleratorDevices = append(acceleratorDevices, device)
		}
		resourcePoolName := gpuSlicePoolName(templateNode.Name, acceleratorIndex)
		acceleratorSlices := assignDevicesIntoResourceSlices(templateNode.Name, GpuDriver, resourcePoolName, acceleratorDevices)
		slices = append(slices, acceleratorSlices...)
	}

	return slices, nil
}

func createDraGpuDevice(mcp *mt.MachineConfigProvider, name, deviceType, acceleratorType, uuid string, index int64) (resourceapi.Device, error) {
	acceleratorInfo, known := mcp.GetDraAcceleratorInfo(acceleratorType)
	if !known {
		return resourceapi.Device{}, fmt.Errorf("unable to build DRA device: unregistered GPU type: %s", acceleratorType)
	}

	device := resourceapi.Device{
		Name: name,
		Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
			"type":         stringAttrValue(deviceType),
			"architecture": stringAttrValue(acceleratorInfo.Architecture),
			"brand":        stringAttrValue(acceleratorInfo.Brand),
			"productName":  stringAttrValue(acceleratorInfo.Model),
			"uuid":         stringAttrValue(uuid),
			"index":        intAttrValue(index),
			"minor":        intAttrValue(index),
		},
		Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
			"memory": {
				Value: *memoryGbToQuantity(acceleratorInfo.CapacityGB),
			},
		},
	}

	return device, nil
}

func memoryGbToQuantity(memoryGb int64) *resource.Quantity {
	return resource.NewQuantity(int64(memoryGb)*2^30, resource.BinarySI)
}

func gpuSlicePoolName(nodeName string, acceleratorIndex int) string {
	return fmt.Sprintf("%v-%v-%v", nodeName, GpuDriver, acceleratorIndex)
}
