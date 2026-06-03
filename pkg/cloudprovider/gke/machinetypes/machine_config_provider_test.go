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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLocalSSDDiskSizes(t *testing.T) {
	cache := newMachineConfigurationCache()

	// Override the disk size for a machine family that doesn't have
	// a hard-coded override to validate the new path.
	a3 := cache.config.families["a3"]
	for mtName, mt := range a3.autoprovisionedMachineTypes {
		if mt.ephemeralLocalSsdCfg != nil {
			mt.ephemeralLocalSsdCfg.diskSize = 1000
			a3.autoprovisionedMachineTypes[mtName] = mt
		}
	}
	cache.config.families["a3"] = a3

	mcp := &MachineConfigProvider{
		cache: cache,
	}

	for tn, tc := range map[string]struct {
		machineType  string
		wantDiskSize uint64
	}{
		"unset value": {
			machineType: "n1-standard-1",
		},
		"hard-coded override": {
			machineType:  "z3",
			wantDiskSize: 3000,
		},
		"machine type object override (MachineConfig CRD path)": {
			machineType:  "a3-ultragpu-8g",
			wantDiskSize: 1000,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			got := mcp.LocalSSDDiskSizes()[tc.machineType]
			assert.Equalf(t, tc.wantDiskSize, got, "Unexpected disk size for %s, got: %v, want: %v", tc.machineType, got, tc.wantDiskSize)
		})
	}
}

func TestNumNodesFromTopology_doesntThrowForTPUMachines(t *testing.T) {
	mcp := NewMachineConfigProvider(nil)
	for _, mf := range mcp.AllMachineFamilies() {
		if !mf.IsTpuSupported() {
			continue
		}
		// Verify all machines have corresponding `fixedTPUCount` entry
		for _, mt := range mf.AllMachineTypes(NoConstraints) {
			_, err := mcp.NumNodesFromTopology(mt.Name, "100x100x100")
			assert.NoError(t, err)
		}
	}
}

func TestNumNodesFromTopology_throwsForAnyNonGPUNonTPUMachine(t *testing.T) {
	mcp := NewMachineConfigProvider(nil)
	for _, mf := range mcp.AllMachineFamilies() {
		if mf.IsTpuSupported() || mf.SupportedGpuTypes() != nil {
			continue
		}
		// Should throw for any machines with neither TPU nor GPU
		for _, mt := range mf.AllMachineTypes(NoConstraints) {
			_, err := mcp.NumNodesFromTopology(mt.Name, "100x100x100")
			assert.Error(t, err)
		}
	}
}

func TestNumNodesFromTopology(t *testing.T) {
	tpu4chipMachine := "ct4p-hightpu-4t"
	tpu8chipMachine := "ct5lp-hightpu-8t"
	for tn, tc := range map[string]struct {
		machineType  string
		topology     string
		wantNumNodes int64
		wantErr      bool
	}{
		"empty topology": {
			machineType: tpu4chipMachine,
			topology:    "",
			wantErr:     true,
		},
		"malformed topology 'NxxNxN'": {
			machineType: tpu4chipMachine,
			topology:    "2xx2x1",
			wantErr:     true,
		},
		"malformed topology 'xNxN'": {
			machineType: tpu4chipMachine,
			topology:    "x2x1",
			wantErr:     true,
		},
		"malformed topology 'NxNxNx'": {
			machineType: tpu4chipMachine,
			topology:    "2x2x1x",
			wantErr:     true,
		},
		"malformed topology 'xxx'": {
			machineType: tpu4chipMachine,
			topology:    "xxx",
			wantErr:     true,
		},
		"Valid 2d topology '2x4'": {
			machineType:  tpu4chipMachine,
			topology:     "2x4",
			wantNumNodes: 2,
			wantErr:      false,
		},
		"Valid 2d topology '4x4'": {
			machineType:  tpu4chipMachine,
			topology:     "4x4",
			wantNumNodes: 4,
			wantErr:      false,
		},
		"Valid 3d topology '2x2x1'": {
			machineType:  tpu4chipMachine,
			topology:     "2x2x1",
			wantNumNodes: 1,
		},
		"Valid 3d topology '2x2x2'": {
			machineType:  tpu4chipMachine,
			topology:     "2x2x2",
			wantNumNodes: 2,
			wantErr:      false,
		},
		"Valid 3d topology '8x8x16'": {
			machineType:  tpu4chipMachine,
			topology:     "8x8x16",
			wantNumNodes: 256,
			wantErr:      false,
		},
		"Number of chips is not divisible by 'chips per node'": {
			machineType: tpu4chipMachine,
			topology:    "2x3x5",
			wantErr:     true,
		},
		"Number of chips larger than topology": {
			machineType: tpu8chipMachine,
			topology:    "2x2",
			wantErr:     true,
		},
		"gpu_not_found_for_cpu_machine_type": {
			machineType: "n1-standard-1",
			topology:    "1x1",
			wantErr:     true,
		},
		"topology_error_a2_doesnt_support_topology": {
			machineType: "a2-highgpu-1g",
			topology:    "invalid",
			wantErr:     true,
		},
		"valid_1gpu_1x1": {
			machineType:  "a2-highgpu-1g",
			topology:     "1x1",
			wantNumNodes: 1,
		},
		"valid_2gpu_1x2": {
			machineType:  "a2-highgpu-2g",
			topology:     "1x2",
			wantNumNodes: 1,
		},
		"valid_2gpu_1x4": {
			machineType:  "a2-highgpu-2g",
			topology:     "1x4",
			wantNumNodes: 2,
		},
		"indivisible": {
			machineType: "a2-highgpu-2g",
			topology:    "1x1",
			wantErr:     true,
		},
		"valid_2gpu_2x36": {
			machineType:  "a2-highgpu-2g",
			topology:     "2x36",
			wantNumNodes: 36,
		},
		"valid_4gpu_2x36": {
			machineType:  "a2-highgpu-4g",
			topology:     "2x36",
			wantNumNodes: 18,
		},
		"non_tpu_non_gpu_machine_throws": {
			machineType: "e2-standard-1",
			topology:    "2x36",
			wantErr:     true,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			mcp := NewMachineConfigProvider(nil)
			nodeCount, err := mcp.NumNodesFromTopology(tc.machineType, tc.topology)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantNumNodes, nodeCount)
			}
		})
	}
}
