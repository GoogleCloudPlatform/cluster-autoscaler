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

package machinetypes

import (
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

var (
	supportedTpuTypes = []string{
		labels.TpuV3DeviceValue,
		labels.TpuV3SliceValue,
		labels.TpuV4LiteDeviceValue,
		labels.TpuV4PodsliceValue,
		labels.TpuV5LiteDeviceValue,
		labels.TpuV5LitePodsliceValue,
		labels.TpuV5PSliceValue,
		labels.TpuV6ESliceValue,
		labels.Tpu7xValue,
		labels.Tpu7Value,
	}
	napSupportedTpuTypes = map[string]struct{}{
		labels.TpuV3DeviceValue:       {},
		labels.TpuV3SliceValue:        {},
		labels.TpuV4LiteDeviceValue:   {},
		labels.TpuV4PodsliceValue:     {},
		labels.TpuV5LiteDeviceValue:   {},
		labels.TpuV5LitePodsliceValue: {},
		labels.TpuV5PSliceValue:       {},
		labels.TpuV6ESliceValue:       {},
		labels.Tpu7xValue:             {},
		labels.Tpu7Value:              {},
	}
	fixedTpuCount = map[string]int64{
		"ct3-hightpu-4t":    4,
		"ct3p-hightpu-4t":   4,
		"ct4l-hightpu-4t":   4,
		"ct4p-hightpu-4t":   4,
		"ct5l-hightpu-1t":   1,
		"ct5l-hightpu-4t":   4,
		"ct5l-hightpu-8t":   8,
		"ct5lp-hightpu-1t":  1,
		"ct5lp-hightpu-4t":  4,
		"ct5lp-hightpu-8t":  8,
		"ct5p-hightpu-4t":   4,
		"ct6e-standard-1t":  1,
		"ct6e-standard-4t":  4,
		"ct6e-standard-8t":  8,
		"tpu7x-standard-1t": 1,
		"tpu7x-standard-4t": 4,
		"tpu7x-ultranet-4t": 4,
		"tpu7-standard-1t":  1,
		"tpu7-standard-4t":  4,
	}
	singleHostTopologyMap = map[string]string{
		"ct3-hightpu-4t": "2x2",
		// ct3p-hightpu-4t doesn't support single host topology
		"ct4l-hightpu-4t":   "2x2",
		"ct4p-hightpu-4t":   "2x2x1",
		"ct5l-hightpu-1t":   "1x1",
		"ct5l-hightpu-4t":   "2x2",
		"ct5l-hightpu-8t":   "2x4",
		"ct5lp-hightpu-1t":  "1x1",
		"ct5lp-hightpu-4t":  "2x2",
		"ct5lp-hightpu-8t":  "2x4",
		"ct5p-hightpu-4t":   "2x2x1",
		"ct6e-standard-1t":  "1x1",
		"ct6e-standard-4t":  "2x2",
		"ct6e-standard-8t":  "2x4",
		"tpu7x-standard-1t": "1x1x1",
		"tpu7x-standard-4t": "2x2x1",
		"tpu7x-ultranet-4t": "2x2x1",
		"tpu7-standard-1t":  "1x1x1",
		"tpu7-standard-4t":  "2x2x1",
	}
	maxTpuCount = map[string]int64{
		labels.TpuV3DeviceValue:       4,
		labels.TpuV3SliceValue:        4,
		labels.TpuV4LiteDeviceValue:   4,
		labels.TpuV4PodsliceValue:     4,
		labels.TpuV5LiteDeviceValue:   8,
		labels.TpuV5LitePodsliceValue: 8,
		labels.TpuV5PSliceValue:       4,
		labels.TpuV6ESliceValue:       8,
		labels.Tpu7xValue:             4,
		labels.Tpu7Value:              4,
	}
	machineFamilyForTpuType = map[string]MachineFamily{
		labels.TpuV3DeviceValue:       CT3,
		labels.TpuV3SliceValue:        CT3P,
		labels.TpuV4LiteDeviceValue:   CT4L,
		labels.TpuV4PodsliceValue:     CT4P,
		labels.TpuV5LiteDeviceValue:   CT5L,
		labels.TpuV5LitePodsliceValue: CT5LP,
		labels.TpuV5PSliceValue:       CT5P,
		labels.TpuV6ESliceValue:       CT6E,
		labels.Tpu7xValue:             TPU7X,
		labels.Tpu7Value:              TPU7,
	}
)
