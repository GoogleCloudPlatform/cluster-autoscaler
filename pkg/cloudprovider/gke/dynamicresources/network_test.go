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
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/dynamicresources/dranet"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	mt "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/utils/ptr"
)

func TestNetworkResourceSlicesForNode(t *testing.T) {
	gpuTemplateNode := test.BuildTestNode("node-gpu", 1, 1)
	gpuTemplateNode.Labels[labels.GPULabel] = "exists"
	tpuTemplateNode := test.BuildTestNode("node-tpu", 1, 1)
	tpuTemplateNode.Labels[labels.TPULabel] = "exists"
	noAcceleratorTemplateNode := test.BuildTestNode("node-no-accelerator", 1, 1)

	tests := map[string]struct {
		templateNode           *apiv1.Node
		machineType            string
		expectedResourceSlices []*resourceapi.ResourceSlice
		expectErr              bool
		expectedErrorMsg       string
	}{
		"a3-highgpu-8g": {
			templateNode: gpuTemplateNode,
			machineType:  "a3-highgpu-8g",
			expectedResourceSlices: []*resourceapi.ResourceSlice{
				buildResourceSlice("node-gpu-dra.net-slice-0", "node-gpu", "node-gpu-dra.net", "dra.net", 1, []resourceapi.Device{
					buildDRANETDevice("dra.net-device-0", "eth1", "additional-network-default-0", "Google, Inc.", 8244, true, false),
					buildDRANETDevice("dra.net-device-1", "eth2", "additional-network-default-1", "Google, Inc.", 8244, true, false),
					buildDRANETDevice("dra.net-device-2", "eth3", "additional-network-default-2", "Google, Inc.", 8244, true, false),
					buildDRANETDevice("dra.net-device-3", "eth4", "additional-network-default-3", "Google, Inc.", 8244, true, false),
				}),
			},
		},
		"a3-megagpu-8g": {
			templateNode: gpuTemplateNode,
			machineType:  "a3-megagpu-8g",
			expectedResourceSlices: []*resourceapi.ResourceSlice{
				buildResourceSlice("node-gpu-dra.net-slice-0", "node-gpu", "node-gpu-dra.net", "dra.net", 1, []resourceapi.Device{
					buildDRANETDevice("dra.net-device-0", "eth1", "additional-network-default-0", "Google, Inc.", 8244, true, false),
					buildDRANETDevice("dra.net-device-1", "eth2", "additional-network-default-1", "Google, Inc.", 8244, true, false),
					buildDRANETDevice("dra.net-device-2", "eth3", "additional-network-default-2", "Google, Inc.", 8244, true, false),
					buildDRANETDevice("dra.net-device-3", "eth4", "additional-network-default-3", "Google, Inc.", 8244, true, false),
					buildDRANETDevice("dra.net-device-4", "eth5", "additional-network-default-4", "Google, Inc.", 8244, true, false),
					buildDRANETDevice("dra.net-device-5", "eth6", "additional-network-default-5", "Google, Inc.", 8244, true, false),
					buildDRANETDevice("dra.net-device-6", "eth7", "additional-network-default-6", "Google, Inc.", 8244, true, false),
					buildDRANETDevice("dra.net-device-7", "eth8", "additional-network-default-7", "Google, Inc.", 8244, true, false),
				}),
			},
		},
		"a3-ultragpu-8g": {
			templateNode: gpuTemplateNode,
			machineType:  "a3-ultragpu-8g",
			expectedResourceSlices: []*resourceapi.ResourceSlice{
				buildResourceSlice("node-gpu-dra.net-slice-0", "node-gpu", "node-gpu-dra.net", "dra.net", 1, []resourceapi.Device{
					buildDRANETDevice("dra.net-device-0", "eth1", "additional-network-default-0", "Google, Inc.", 8896, true, false),
					buildDRANETDevice("dra.net-device-1", "gpu0rdma0", "additional-network-rdma-0", "Mellanox Technologies", 8896, false, true),
					buildDRANETDevice("dra.net-device-2", "gpu1rdma0", "additional-network-rdma-1", "Mellanox Technologies", 8896, false, true),
					buildDRANETDevice("dra.net-device-3", "gpu2rdma0", "additional-network-rdma-2", "Mellanox Technologies", 8896, false, true),
					buildDRANETDevice("dra.net-device-4", "gpu3rdma0", "additional-network-rdma-3", "Mellanox Technologies", 8896, false, true),
					buildDRANETDevice("dra.net-device-5", "gpu4rdma0", "additional-network-rdma-4", "Mellanox Technologies", 8896, false, true),
					buildDRANETDevice("dra.net-device-6", "gpu5rdma0", "additional-network-rdma-5", "Mellanox Technologies", 8896, false, true),
					buildDRANETDevice("dra.net-device-7", "gpu6rdma0", "additional-network-rdma-6", "Mellanox Technologies", 8896, false, true),
					buildDRANETDevice("dra.net-device-8", "gpu7rdma0", "additional-network-rdma-7", "Mellanox Technologies", 8896, false, true),
				}),
			},
		},
		"a4-highgpu-8g": {
			templateNode: gpuTemplateNode,
			machineType:  "a4-highgpu-8g",
			expectedResourceSlices: []*resourceapi.ResourceSlice{
				buildResourceSlice("node-gpu-dra.net-slice-0", "node-gpu", "node-gpu-dra.net", "dra.net", 1, []resourceapi.Device{
					buildDRANETDevice("dra.net-device-0", "eth1", "additional-network-default-0", "Google, Inc.", 8896, true, false),
					buildDRANETDevice("dra.net-device-1", "gpu0rdma0", "additional-network-rdma-0", "Mellanox Technologies", 8896, false, true),
					buildDRANETDevice("dra.net-device-2", "gpu1rdma0", "additional-network-rdma-1", "Mellanox Technologies", 8896, false, true),
					buildDRANETDevice("dra.net-device-3", "gpu2rdma0", "additional-network-rdma-2", "Mellanox Technologies", 8896, false, true),
					buildDRANETDevice("dra.net-device-4", "gpu3rdma0", "additional-network-rdma-3", "Mellanox Technologies", 8896, false, true),
					buildDRANETDevice("dra.net-device-5", "gpu4rdma0", "additional-network-rdma-4", "Mellanox Technologies", 8896, false, true),
					buildDRANETDevice("dra.net-device-6", "gpu5rdma0", "additional-network-rdma-5", "Mellanox Technologies", 8896, false, true),
					buildDRANETDevice("dra.net-device-7", "gpu6rdma0", "additional-network-rdma-6", "Mellanox Technologies", 8896, false, true),
					buildDRANETDevice("dra.net-device-8", "gpu7rdma0", "additional-network-rdma-7", "Mellanox Technologies", 8896, false, true),
				}),
			},
		},
		"a4x-highgpu-4g": {
			templateNode: gpuTemplateNode,
			machineType:  "a4x-highgpu-4g",
			expectedResourceSlices: []*resourceapi.ResourceSlice{
				buildResourceSlice("node-gpu-dra.net-slice-0", "node-gpu", "node-gpu-dra.net", "dra.net", 1, []resourceapi.Device{
					buildDRANETDevice("dra.net-device-0", "eth1", "additional-network-default-0", "Google, Inc.", 8896, true, false),
					buildDRANETDevice("dra.net-device-1", "gpu0rdma0", "additional-network-rdma-0", "Mellanox Technologies", 8896, false, true),
					buildDRANETDevice("dra.net-device-2", "gpu1rdma0", "additional-network-rdma-1", "Mellanox Technologies", 8896, false, true),
					buildDRANETDevice("dra.net-device-3", "gpu2rdma0", "additional-network-rdma-2", "Mellanox Technologies", 8896, false, true),
					buildDRANETDevice("dra.net-device-4", "gpu3rdma0", "additional-network-rdma-3", "Mellanox Technologies", 8896, false, true),
				}),
			},
		},
		"ct6e-standard-4t": {
			templateNode: tpuTemplateNode,
			machineType:  "ct6e-standard-4t",
			expectedResourceSlices: []*resourceapi.ResourceSlice{
				buildResourceSlice("node-tpu-dra.net-slice-0", "node-tpu", "node-tpu-dra.net", "dra.net", 1, []resourceapi.Device{
					buildDRANETDevice("dra.net-device-0", "eth1", "additional-network-default-0", "Google, Inc.", 8896, true, false),
					buildDRANETDevice("dra.net-device-1", "eth2", "additional-network-default-1", "Google, Inc.", 8896, true, false),
				}),
			},
		},
		"a3-highgpu-8g-no-accelerator-label": {
			templateNode:           noAcceleratorTemplateNode,
			machineType:            "a3-highgpu-8g",
			expectedResourceSlices: nil,
		},
		"no-dra-info": {
			templateNode:           gpuTemplateNode,
			machineType:            "e2-standard-2",
			expectErr:              false,
			expectedResourceSlices: []*resourceapi.ResourceSlice{},
		},
	}

	provider := mt.NewDranetConfigProvider()
	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			hwConfig := &NodeHardwareConfig{MachineType: test.machineType}
			resourceSlices, err := dranetResourceSlicesForNode(provider, hwConfig, test.templateNode)

			if test.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), test.expectedErrorMsg)
				return
			}

			assert.NoError(t, err)
			removeRandomNetFields(resourceSlices)
			assert.ElementsMatch(t, test.expectedResourceSlices, resourceSlices)
		})
	}
}

func removeRandomNetFields(resourceSlices []*resourceapi.ResourceSlice) {
	for _, resourceSlice := range resourceSlices {
		resourceSlice.UID = types.UID("")
		for i := range resourceSlice.Spec.Devices {
			device := &resourceSlice.Spec.Devices[i]
			attrs := device.Attributes
			attrs[dranet.AttrMac] = resourceapi.DeviceAttribute{StringValue: ptr.To("")}
			attrs[dranet.AttrIPv4] = resourceapi.DeviceAttribute{StringValue: ptr.To("")}
			attrs[dranet.AttrIPv6] = resourceapi.DeviceAttribute{StringValue: ptr.To("")}
			attrs[dranet.AttrPCIAddress] = resourceapi.DeviceAttribute{StringValue: ptr.To("")}
		}
	}
}

func buildDRANETDevice(deviceName, ifName, networkName, pciVendor string, mtu int64, hasEbpf, isRdma bool) resourceapi.Device {
	return resourceapi.Device{
		Name:       deviceName,
		Attributes: buildDRANETAttributes(ifName, networkName, pciVendor, mtu, hasEbpf, isRdma),
	}
}

func buildDRANETAttributes(ifName, networkName, pciVendor string, mtu int64, hasEbpf, isRdma bool) map[resourceapi.QualifiedName]resourceapi.DeviceAttribute {
	attributes := map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
		dranet.AttrInterfaceName: {StringValue: ptr.To(ifName)},
		dranet.AttrMac:           {StringValue: ptr.To("")},
		dranet.AttrMTU:           {IntValue: ptr.To(mtu)},
		dranet.AttrEncapsulation: {StringValue: ptr.To("ether")},
		dranet.AttrAlias:         {StringValue: ptr.To("")},
		dranet.AttrState:         {StringValue: ptr.To("up")},
		dranet.AttrType:          {StringValue: ptr.To("device")},
		dranet.AttrIPv4:          {StringValue: ptr.To("")},
		dranet.AttrIPv6:          {StringValue: ptr.To("")},
		dranet.AttrSRIOV:         {BoolValue: ptr.To(false)},
		dranet.AttrVirtual:       {BoolValue: ptr.To(false)},
		dranet.AttrPCIAddress:    {StringValue: ptr.To("")},
		dranet.AttrPCIDevice:     {StringValue: ptr.To("")},
		dranet.AttrPCISubsystem:  {StringValue: ptr.To("")},
		dranet.AttrPCIVendor:     {StringValue: ptr.To(pciVendor)},
		dranet.AttrNUMANode:      {IntValue: ptr.To(int64(0))},
		dranet.AttrEBPF:          {BoolValue: ptr.To(hasEbpf)},
		dranet.AttrRDMA:          {BoolValue: ptr.To(isRdma)},

		dranet.GceAttrBlock:                {StringValue: ptr.To("")},
		dranet.GceAttrSubBlock:             {StringValue: ptr.To("")},
		dranet.GceAttrHost:                 {StringValue: ptr.To("")},
		dranet.GceAttrNetworkName:          {StringValue: ptr.To(networkName)},
		dranet.GceAttrNetworkProjectNumber: {StringValue: ptr.To("")},
		dranet.K8sAttrPcieRoot:             {StringValue: ptr.To("")},
	}
	if hasEbpf {
		attributes[dranet.AttrTCFilterNames] = resourceapi.DeviceAttribute{StringValue: ptr.To("")}
		attributes[dranet.AttrTCXProgramNames] = resourceapi.DeviceAttribute{StringValue: ptr.To("")}
	}

	return attributes
}
