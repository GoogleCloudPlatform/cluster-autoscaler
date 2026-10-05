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
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/gpu"
	tpuutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/tpu"
)

func TestIsAccelerator(t *testing.T) {
	testCases := []struct {
		name            corev1.ResourceName
		wantGPU         bool
		wantTPU         bool
		wantAccelerator bool
	}{
		{name: gpu.ResourceNvidiaGPU, wantGPU: true, wantAccelerator: true},
		{name: gpu.ResourceAMDGPU, wantGPU: true, wantAccelerator: true},
		{name: gpu.ResourceIntelGaudi, wantGPU: true, wantAccelerator: true},
		{name: gpu.ResourceIntelGPU, wantGPU: true, wantAccelerator: true},
		{name: gpu.ResourceDirectX, wantGPU: true, wantAccelerator: true},
		{name: tpu.ResourceGoogleTPU, wantTPU: true, wantAccelerator: true},
		{name: tpuutils.ResourceTPUPrefix + "v3-8", wantTPU: true, wantAccelerator: true},
		{name: corev1.ResourceCPU, wantAccelerator: false},
		{name: corev1.ResourceMemory, wantAccelerator: false},
		{name: "example.com/dongle", wantAccelerator: false},
	}

	for _, tc := range testCases {
		t.Run(string(tc.name), func(t *testing.T) {
			assert.Equal(t, tc.wantGPU, IsGPU(tc.name))
			assert.Equal(t, tc.wantTPU, IsTPU(tc.name))
			assert.Equal(t, tc.wantAccelerator, IsAccelerator(tc.name))
		})
	}
}
