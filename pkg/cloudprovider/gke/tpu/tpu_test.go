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

package tpu

import (
	"testing"

	"github.com/stretchr/testify/assert"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
)

func TestGetNodeTpu(t *testing.T) {
	tpuType := "some-tpu-type"
	for tn, tc := range map[string]struct {
		resourceName     apiv1.ResourceName
		acceleratorLabel string
		acceleratorType  string
		expectedConfig   *cloudprovider.GpuConfig
	}{
		"Node with no accelerator": {
			resourceName:     "",
			acceleratorLabel: "",
			acceleratorType:  "",
			expectedConfig:   nil,
		},
		"Node with GPU accelerator": {
			resourceName:     gpu.ResourceNvidiaGPU,
			acceleratorLabel: gkelabels.GPULabel,
			acceleratorType:  machinetypes.NvidiaTeslaK80.Name(),
			expectedConfig:   nil,
		},
		"Node with TPU accelerator": {
			resourceName:     ResourceGoogleTPU,
			acceleratorLabel: gkelabels.TPULabel,
			acceleratorType:  tpuType,
			expectedConfig:   &cloudprovider.GpuConfig{Label: gkelabels.TPULabel, Type: tpuType, ExtendedResourceName: ResourceGoogleTPU},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			node := BuildTestNode("testNode", 1000, 1000)
			if tc.resourceName != "" {
				node.Status.Capacity[tc.resourceName] = *resource.NewQuantity(1, resource.DecimalSI)
				node.Status.Allocatable[tc.resourceName] = *resource.NewQuantity(1, resource.DecimalSI)
			}
			if tc.acceleratorLabel != "" {
				node.Labels[tc.acceleratorLabel] = tc.acceleratorType
			}
			gotConfig := GetNodeTpu(node)
			assert.Equal(t, tc.expectedConfig, gotConfig)
		})
	}
}

func TestIsMultiHostTpuPodslice(t *testing.T) {
	for tn, tc := range map[string]struct {
		tpuType       string
		tpuTopology   string
		tpuRequest    int64
		wantMultiHost bool
		wantErr       bool
	}{
		"empty tpu type": {
			tpuType:     "",
			tpuTopology: "4x4",
			tpuRequest:  4,
		},
		"single-host 2d topology": {
			tpuType:     "my-tpu",
			tpuTopology: "2x2",
			tpuRequest:  4,
		},
		"single-host small 2d topology": {
			tpuType:     "my-tpu",
			tpuTopology: "1x1",
			tpuRequest:  1,
		},
		"single-host 3d topology": {
			tpuType:     "my-tpu",
			tpuTopology: "2x2x1",
			tpuRequest:  4,
		},
		"single-host 10d topology": {
			tpuType:     "my-tpu",
			tpuTopology: "2x2x1x1x1x1x1x1x1x1",
			tpuRequest:  4,
		},
		"invalid topology": {
			tpuType:     "my-tpu",
			tpuTopology: "invalid-topology",
			tpuRequest:  4,
			wantErr:     true,
		},
		"multi-host 2d topology": {
			tpuType:       "my-tpu",
			tpuTopology:   "2x2",
			tpuRequest:    2,
			wantMultiHost: true,
		},
		"multi-host 3d topology": {
			tpuType:       "my-tpu",
			tpuTopology:   "2x2x2",
			tpuRequest:    4,
			wantMultiHost: true,
		},
		"multi-host 10d topology": {
			tpuType:       "my-tpu",
			tpuTopology:   "1x2x1x2x1x1x2x1x1x1",
			tpuRequest:    4,
			wantMultiHost: true,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			mcp := machinetypes.NewMachineConfigProvider(nil)
			isMultiHost, err := mcp.IsMultiHostTpuPodslice(tc.tpuType, tc.tpuTopology, tc.tpuRequest)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantMultiHost, isMultiHost)
			}
		})
	}
}
