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
	"k8s.io/utils/ptr"
)

func TestComputeDomainResourceSlicesForNode(t *testing.T) {
	gpuTemplateNode := test.BuildTestNode("node1", 1, 1)
	gpuTemplateNode.Labels[labels.GPULabel] = "exists"
	noAcceleratorTemplateNode := test.BuildTestNode("node1", 1, 1)

	gpuComputeDomainDevices := []resourceapi.Device{
		{
			Name: "channel-0",
			Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
				"type": {
					StringValue: ptr.To("channel"),
				},
				"id": {
					IntValue: ptr.To(int64(0)),
				},
			},
		},
		{
			Name: "daemon-0",
			Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
				"type": {
					StringValue: ptr.To("daemon"),
				},
				"id": {
					IntValue: ptr.To(int64(0)),
				},
			},
		},
	}

	tests := map[string]struct {
		withAcceleratorLabel   bool
		expectedResourceSlices []*resourceapi.ResourceSlice
	}{
		"NoGPU": {
			withAcceleratorLabel:   false,
			expectedResourceSlices: []*resourceapi.ResourceSlice{},
		},
		"WithGPU": {
			withAcceleratorLabel: true,
			expectedResourceSlices: []*resourceapi.ResourceSlice{
				buildResourceSlice("node1-compute-domain.nvidia.com-slice-0", "node1", computeDomainSlicePoolName("node1"), "compute-domain.nvidia.com", 1, gpuComputeDomainDevices),
			},
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			templateNode := gpuTemplateNode
			if !test.withAcceleratorLabel {
				templateNode = noAcceleratorTemplateNode
			}

			resourceSlices, err := computeDomainResourceSlicesForNode(templateNode)
			assert.NoError(t, err)
			removeSliceUIDsForAssertion(resourceSlices)
			assert.ElementsMatch(t, test.expectedResourceSlices, resourceSlices)
		})
	}
}
