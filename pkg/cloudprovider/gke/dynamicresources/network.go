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
	"crypto/rand"
	"fmt"
	"net"

	apiv1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/dynamicresources/dranet"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/klog/v2"
)

const (
	NetworkDriver = "dra.net"
)

// DRADriverEnabled checks whether networking DRA driver is enabled on the node
func DRANETDriverEnabled(node *apiv1.Node) bool {
	value, foundLabel := node.Labels[labels.DraNetNodeLabel]
	return foundLabel && value == "true"
}

func dranetResourceSlicesForNode(provider machinetypes.DranetConfigProvider, hwConfig *NodeHardwareConfig, templateNode *apiv1.Node) ([]*resourceapi.ResourceSlice, error) {
	_, gpuExists := templateNode.Labels[labels.GPULabel]
	_, tpuExists := templateNode.Labels[labels.TPULabel]
	if !gpuExists && !tpuExists {
		klog.V(4).Infof("Node without GPU or TPU label enables DRANET driver, not attaching network resource slices")
		return nil, nil
	}

	config, found := provider.ConfigForMachineType(hwConfig.MachineType)
	if !found {
		// Dranet config is not defined for this machine type, not attaching network resource slices.
		return nil, nil
	}

	nicDevices := createDRANETDevices(config)
	resourcePoolName := fmt.Sprintf("%v-%v", templateNode.Name, NetworkDriver)
	resourceSlices := assignDevicesIntoResourceSlices(templateNode.Name, NetworkDriver, resourcePoolName, nicDevices)

	return resourceSlices, nil
}

func createDRANETDevices(config machinetypes.MultiNicConfig) []resourceapi.Device {
	var nicDevices []resourceapi.Device
	var nicNum int64 = 0
	for _, nicGroup := range config.NicGroups {
		for nicIndex := range nicGroup.NicCount {
			sharedAttributes := config.SharedAttributes
			specAttributes := nicGroup // specAttributes is the attributes defined specifically in nicGroup

			device := resourceapi.Device{
				Name:       fmt.Sprintf("%v-device-%v", NetworkDriver, nicNum),
				Attributes: createDRANETAttributesPerDevice(nicIndex, sharedAttributes, specAttributes),
			}
			nicDevices = append(nicDevices, device)
			nicNum++
		}
	}
	return nicDevices
}

func createDRANETAttributesPerDevice(nicIndex int64, sharedAttributes map[resourceapi.QualifiedName]resourceapi.DeviceAttribute, specAttributes machinetypes.NicGroupConfig) map[resourceapi.QualifiedName]resourceapi.DeviceAttribute {

	// initialize general attributes with placeholder values
	attributes := map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
		dranet.AttrMac:           stringAttrValue(generateRandomMAC()),
		dranet.AttrEncapsulation: stringAttrValue("ether"),
		dranet.AttrAlias:         stringAttrValue(""),
		dranet.AttrState:         stringAttrValue("up"),
		dranet.AttrType:          stringAttrValue("device"),
		dranet.AttrIPv4:          stringAttrValue(generateRandomIPv4()),
		dranet.AttrIPv6:          stringAttrValue(generateRandomIPv6()),
		dranet.AttrSRIOV:         boolAttrValue(false),
		dranet.AttrVirtual:       boolAttrValue(false),
		dranet.AttrPCIAddress:    stringAttrValue(generateRandomPCI()),
		dranet.AttrPCIDevice:     stringAttrValue(""),
		dranet.AttrPCISubsystem:  stringAttrValue(""),
		dranet.AttrNUMANode:      intAttrValue(int64(0)),

		dranet.GceAttrBlock:                stringAttrValue(""),
		dranet.GceAttrSubBlock:             stringAttrValue(""),
		dranet.GceAttrHost:                 stringAttrValue(""),
		dranet.GceAttrNetworkProjectNumber: stringAttrValue(""),
		dranet.K8sAttrPcieRoot:             stringAttrValue(""),
	}

	for sharedKey, sharedValue := range sharedAttributes {
		attributes[sharedKey] = sharedValue
	}

	for specFixedKey, specFixedValue := range specAttributes.StaticAttributes {
		attributes[specFixedKey] = specFixedValue
	}

	for specParamKey, specParamValue := range specAttributes.DynamicAttributes {
		attributes[specParamKey] = specParamValue(int(nicIndex))
	}

	return attributes
}

func generateRandomMAC() string {
	buf := make([]byte, 6)
	_, _ = rand.Read(buf)
	return net.HardwareAddr(buf).String()
}

func generateRandomIPv4() string {
	buf := make([]byte, 4)
	_, _ = rand.Read(buf)
	return net.IP(buf).String()
}

func generateRandomIPv6() string {
	buf := make([]byte, 16)
	_, _ = rand.Read(buf)
	return net.IP(buf).String()
}

func generateRandomPCI() string {
	// PCI address format: 0000:BB:DD.F
	buf := make([]byte, 2)
	_, _ = rand.Read(buf)

	// 8 bits for bus number
	bus := buf[0]
	// 5 bits for device number
	device := buf[1] & 0x1f
	// 3 bits for function number
	function := (buf[1] >> 5) & 0x07
	return fmt.Sprintf("0000:%02x:%02x.%x", bus, device, function)
}
