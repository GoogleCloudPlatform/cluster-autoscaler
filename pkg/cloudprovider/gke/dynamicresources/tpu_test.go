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

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/utils/ptr"
)

func TestTpuResourceSlicesForNode(t *testing.T) {
	tpuTemplateNode := test.BuildTestNode("node1", 1, 1)
	tpuTemplateNode.Labels[labels.TPULabel] = "exists"
	noAcceleratorTemplateNode := test.BuildTestNode("node1", 1, 1)

	tests := map[string]struct {
		withAcceleratorLabel   bool
		machineType            string
		tpuType                string
		expectedResourceSlices []*resourceapi.ResourceSlice
	}{
		"TPUAttached": {
			withAcceleratorLabel: true,
			machineType:          "ct5lp-hightpu-8t",
			tpuType:              "tpu-v5-lite-podslice",
			expectedResourceSlices: []*resourceapi.ResourceSlice{
				buildResourceSlice("node1-tpu.google.com-0-slice-0", "node1", tpuSlicePoolName("node1", 0), "tpu.google.com", 1, []resourceapi.Device{
					buildTpuDevice("tpu-0", "v5litepod", 0),
					buildTpuDevice("tpu-1", "v5litepod", 1),
					buildTpuDevice("tpu-2", "v5litepod", 2),
					buildTpuDevice("tpu-3", "v5litepod", 3),
					buildTpuDevice("tpu-4", "v5litepod", 4),
					buildTpuDevice("tpu-5", "v5litepod", 5),
					buildTpuDevice("tpu-6", "v5litepod", 6),
					buildTpuDevice("tpu-7", "v5litepod", 7),
				}),
			},
		},
		"SingleTPU_NoTPULabel": {
			withAcceleratorLabel:   false,
			machineType:            "ct5lp-hightpu-1t",
			tpuType:                "tpu-v5-lite-podslice",
			expectedResourceSlices: []*resourceapi.ResourceSlice{},
		},
		"NoTpu": {
			withAcceleratorLabel:   false,
			expectedResourceSlices: []*resourceapi.ResourceSlice{},
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			templateNode := tpuTemplateNode
			if !test.withAcceleratorLabel {
				templateNode = noAcceleratorTemplateNode
			}
			hwConfig := &NodeHardwareConfig{MachineType: test.machineType, TpuType: test.tpuType}
			mcp := machinetypes.NewMachineConfigProvider(nil)
			resourceSlices, err := tpuResourceSlicesForNode(mcp, hwConfig, templateNode)
			assert.NoError(t, err)
			removeSliceUIDsForAssertion(resourceSlices)
			assert.ElementsMatch(t, test.expectedResourceSlices, resourceSlices)
		})
	}
}

func TestInvalidTPUTypeProvided(t *testing.T) {
	tpuTemplateNode := test.BuildTestNode("node1", 1, 1)
	tpuTemplateNode.Labels[labels.TPULabel] = "exists"
	hwConfig := &NodeHardwareConfig{
		MachineType: "ct5lp-hightpu-1t",
		TpuType:     "not.tpu",
	}

	mcp := machinetypes.NewMachineConfigProvider(nil)
	_, err := tpuResourceSlicesForNode(mcp, hwConfig, tpuTemplateNode)
	assert.ErrorContains(t, err, "invalid accelerator type: not.tpu")
}

func buildTpuDevice(deviceName, tpuGen string, index int64) resourceapi.Device {
	return resourceapi.Device{
		Name: deviceName,
		Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
			"uuid": {
				StringValue: ptr.To(""),
			},
			"index": {
				IntValue: ptr.To(index),
			},
			"tpuGen": {
				StringValue: ptr.To(tpuGen),
			},
		},
	}
}
