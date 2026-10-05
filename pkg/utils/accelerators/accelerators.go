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

package accelerators

import (
	"slices"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	gpuutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/gpu"
	tpuutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/tpu"
)

// IsGPU returns true if the given resource name is a known GPU extended resource.
func IsGPU(name v1.ResourceName) bool {
	return slices.Contains(gpuutils.GPUVendorResourceNames, name)
}

// IsTPU returns true if the given resource name is a known TPU extended resource.
func IsTPU(name v1.ResourceName) bool {
	return strings.HasPrefix(string(name), tpuutils.ResourceTPUPrefix) || name == tpu.ResourceGoogleTPU
}

// IsAccelerator returns true if the given resource name is a known GPU or TPU extended resource.
func IsAccelerator(name v1.ResourceName) bool {
	return IsGPU(name) || IsTPU(name)
}
