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
	"regexp"
	"strings"

	"github.com/google/uuid"
	apiv1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	mt "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/klog/v2"
)

const (
	TpuDeviceType = "tpu"
	TpuDriver     = "tpu.google.com"
)

// TpuDraDriverEnabled checks whether TPU driver is enabled on the node
func TpuDraDriverEnabled(node *apiv1.Node) bool {
	return draDriverEnabled(node, gkelabels.DraTpuNodeLabel)
}

func tpuResourceSlicesForNode(mcp *mt.MachineConfigProvider, hwConfig *NodeHardwareConfig, templateNode *apiv1.Node) ([]*resourceapi.ResourceSlice, error) {
	if _, exists := templateNode.Labels[labels.TPULabel]; !exists {
		klog.V(4).Infof("Node without TPU label enables DRA TPU driver, not attaching resource slices")
		return nil, nil
	}

	tpuCount, err := mcp.GetTpuCountForMachineType(hwConfig.MachineType)
	if err != nil {
		return nil, fmt.Errorf("unable to build DRA devices for TPU: %w", err)
	}

	tpuDevices := make([]resourceapi.Device, tpuCount)
	for deviceIndex := range tpuCount {
		deviceName := fmt.Sprintf("%s-%v", TpuDeviceType, deviceIndex)
		deviceUUID, err := uuid.NewRandom()
		if err != nil {
			return nil, fmt.Errorf("unable to generate device UUID: %w", err)
		}

		device, err := createDraTpuDevice(deviceName, hwConfig.TpuType, deviceUUID.String(), deviceIndex)
		if err != nil {
			return nil, err
		}

		tpuDevices[deviceIndex] = device
	}

	resourcePoolName := tpuSlicePoolName(templateNode.Name, 0)
	resourceSlices := assignDevicesIntoResourceSlices(templateNode.Name, TpuDriver, resourcePoolName, tpuDevices)

	return resourceSlices, nil
}

func createDraTpuDevice(name, tpuType, uuid string, index int64) (resourceapi.Device, error) {
	tpuGen, err := acceleratorGen(tpuType)
	if err != nil {
		return resourceapi.Device{}, fmt.Errorf("unable to build DRA device: %w", err)
	}

	device := resourceapi.Device{
		Name: name,
		Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
			"uuid":   stringAttrValue(uuid),
			"index":  intAttrValue(index),
			"tpuGen": stringAttrValue(tpuGen),
		},
	}

	return device, nil
}

func tpuSlicePoolName(nodeName string, acceleratorIndex int) string {
	return fmt.Sprintf("%v-%v-%v", nodeName, TpuDriver, acceleratorIndex)
}

var (
	acceleratorRegex     = regexp.MustCompile(`^tpu\d+[a-z]?$`)
	pastAcceleratorRegex = regexp.MustCompile(`^tpu-v\d+([ep]?-slice)?((?:-lite)?-(device|podslice))?$`)

	//The tpugen's value should follow the format of CloudTPU "TPU_ACCELERATOR_TYPE" : <tpugen>-<# of cores>
	validTPUGenerations = map[string]bool{
		"v3":        true,
		"v4":        true,
		"v4lite":    true,
		"v5lite":    true,
		"v5litepod": true,
		"v5p":       true,
		"v6e":       true,
		"tpu7x":     true,
	}
)

// AcceleratorGen obtains the generation (v3, v4, v4lite, etc.)
// from the accelerator type label.
// accelerator: tpu-v3-device or tpu-v3-slice; return: v3
// accelerator: tpu-v4-podslice; return: v4
// accelerator: tpu-v4-lite-device; return: v4lite
// accelerator: tpu-v5-lite-device; return: v5lite
// accelerator: tpu-v5-lite-podslice; return: v5litepod
// accelerator: tpu-v5p-slice; return: v5p
// accelerator: tpu-v6e-slice; return: v6e
// accelerator: tpu7x; return: tpu7x
func acceleratorGen(accelerator string) (string, error) {
	// For > TPU7x, the new accelerator label will be the generation
	if acceleratorRegex.MatchString(accelerator) {
		return accelerator, nil
	}
	if !pastAcceleratorRegex.MatchString(accelerator) {
		return "", fmt.Errorf("invalid accelerator type: %v", accelerator)
	}

	// Edge cases that match regex but are not valid accelerator types.
	if accelerator == "tpu-v4-device" || accelerator == "tpu-v4-lite-podslice" {
		return "", fmt.Errorf("no such accelerator type: %s", accelerator)
	}

	// v = v2, v3, v4, v5, v5p, v6e
	v := strings.Split(accelerator, "-")[1]

	// append 'lite' to lite device and lite podslice
	if strings.Contains(accelerator, "lite") {
		v = fmt.Sprintf("%slite", v)
	}

	// append 'pod' to v5 lite podslices
	if strings.HasPrefix(v, "v5") && strings.Contains(accelerator, "podslice") {
		v = fmt.Sprintf("%spod", v)
	}
	if _, exists := validTPUGenerations[v]; !exists {
		return "", fmt.Errorf("invalid TPU generation: %s", v)
	}
	return v, nil
}
