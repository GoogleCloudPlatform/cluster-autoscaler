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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
)

const (
	machineTypeN1     = "n1-standard-1"
	machineTypeA2     = "a2-highgpu-1g"
	machineTypeA2Plus = "a2-ultragpu-1g"
	machineTypeG2     = "g2-standard-4"
	machineTypeA3     = "a3-highgpu-8g"
	machineTypeA3Plus = "a3-megagpu-8g"
	machineTypeA4     = "a4-highgpu-8g"
	machineTypeA4X    = "a4x-highgpu-4g"
	machineTypeA4XMax = "a4x-maxgpu-4g-metal"
	machineTypeG4     = "g4-standard-48"
)

func TestGetNormalizedGpuCount(t *testing.T) {
	testCases := []struct {
		name                   string
		gpu                    Gpu
		gpuPartitionSize       string
		gpuMaxSharedClients    string
		initialGpuCount        AllocatableGpuCount
		cpuCount               int64
		memCount               int64
		wantNormalizedGpuCount AllocatableGpuCount
		wantErr                bool
	}{
		{
			name: "exact match",
			gpu: Gpu{
				maxCpuCount: map[PhysicalGpuCount]int{
					2: 20,
					4: 40,
				},
			},
			initialGpuCount:        2,
			cpuCount:               20,
			wantNormalizedGpuCount: 2,
			wantErr:                false,
		},
		{
			name: "requested gpu count that have to be rounded up",
			gpu: Gpu{
				maxCpuCount: map[PhysicalGpuCount]int{
					2: 20,
					4: 40,
				},
			},
			initialGpuCount:        3,
			cpuCount:               30,
			wantNormalizedGpuCount: 4,
			wantErr:                false,
		},
		{
			name: "requested less gpu than minimum GPU count, should return minimum GPU count",
			gpu: Gpu{
				maxCpuCount: map[PhysicalGpuCount]int{
					2: 20,
					4: 40,
				},
			},
			initialGpuCount:        1,
			cpuCount:               20,
			wantNormalizedGpuCount: 2,
			wantErr:                false,
		},
		{
			name: "requested more gpu than available, should return error",
			gpu: Gpu{
				maxCpuCount: map[PhysicalGpuCount]int{
					2: 20,
					4: 40,
				},
			},
			initialGpuCount:        8,
			cpuCount:               20,
			wantNormalizedGpuCount: 0,
			wantErr:                true,
		},
		{
			name: "requested less CPU than available for the GPU count, should still pick the same gpu count",
			gpu: Gpu{
				maxCpuCount: map[PhysicalGpuCount]int{
					2: 20,
					4: 40,
				},
			},
			initialGpuCount:        2,
			cpuCount:               10,
			wantNormalizedGpuCount: 2,
			wantErr:                false,
		},
		{
			name: "requested more CPU than available for the GPU count, should pick next gpu count",
			gpu: Gpu{
				maxCpuCount: map[PhysicalGpuCount]int{
					2: 20,
					4: 40,
				},
			},
			initialGpuCount:        2,
			cpuCount:               30,
			wantNormalizedGpuCount: 4,
			wantErr:                false,
		},
		{
			name: "CPU request too big, should return error",
			gpu: Gpu{
				maxCpuCount: map[PhysicalGpuCount]int{
					2: 20,
					4: 40,
				},
			},
			initialGpuCount:        2,
			cpuCount:               50,
			wantNormalizedGpuCount: 0,
			wantErr:                true,
		},
		{
			name: "With partitions: exact match",
			gpu: Gpu{
				maxCpuCount: map[PhysicalGpuCount]int{
					2: 20,
					4: 40,
				},
				partitionSizes: map[string]int64{
					"p1": 7,
				},
			},
			gpuPartitionSize:       "p1",
			initialGpuCount:        14,
			cpuCount:               20,
			wantNormalizedGpuCount: 14,
			wantErr:                false,
		},
		{
			name: "With partitions: requested gpu count that have to be rounded up",
			gpu: Gpu{
				maxCpuCount: map[PhysicalGpuCount]int{
					2: 20,
					4: 40,
				},
				partitionSizes: map[string]int64{
					"p1": 7,
				},
			},
			gpuPartitionSize:       "p1",
			initialGpuCount:        10,
			cpuCount:               30,
			wantNormalizedGpuCount: 28,
			wantErr:                false,
		},
		{
			name: "With partitions and max shared clients: exact match",
			gpu: Gpu{
				maxCpuCount: map[PhysicalGpuCount]int{
					2: 20,
					4: 40,
				},
				partitionSizes: map[string]int64{
					"p1": 7,
				},
			},
			gpuPartitionSize:       "p1",
			gpuMaxSharedClients:    "5",
			initialGpuCount:        70,
			cpuCount:               20,
			wantNormalizedGpuCount: 70,
			wantErr:                false,
		},
		{
			name: "With partitions and max shared clients: gpu count that has to be rounded up",
			gpu: Gpu{
				maxCpuCount: map[PhysicalGpuCount]int{
					2: 20,
					4: 40,
				},
				partitionSizes: map[string]int64{
					"p1": 7,
				},
			},
			gpuPartitionSize:       "p1",
			gpuMaxSharedClients:    "5",
			initialGpuCount:        70,
			cpuCount:               30,
			wantNormalizedGpuCount: 140,
			wantErr:                false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.gpu.GetNormalizedGpuCount(
				tc.gpuPartitionSize,
				tc.gpuMaxSharedClients,
				tc.initialGpuCount,
				tc.cpuCount,
				tc.memCount)

			if (err != nil) != tc.wantErr {
				t.Errorf("GetNormalizedGpuCount() error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			if got != tc.wantNormalizedGpuCount {
				t.Errorf("GetNormalizedGpuCount() got = %v, want %v", got, tc.wantNormalizedGpuCount)
			}
		})
	}
}

func TestValidateGpuForMachineType(t *testing.T) {
	tests := []struct {
		gpuType             string
		gpuPartitionSize    string
		gpuMaxSharedClients string
		machineType         string
		gpuCount            AllocatableGpuCount
		cpuCount            int64
		memCount            int64
		expectedErr         bool
	}{
		// valid configs
		{
			gpuType:     NvidiaTeslaK80.Name(),
			machineType: machineTypeN1,
			gpuCount:    1,
			cpuCount:    1,
			memCount:    1,
		},
		{
			gpuType:     NvidiaTeslaP100.Name(),
			machineType: machineTypeN1,
			gpuCount:    1,
			cpuCount:    1,
			memCount:    1,
		},
		{
			gpuType:     NvidiaTeslaK80.Name(),
			machineType: machineTypeN1,
			gpuCount:    8,
			cpuCount:    1,
			memCount:    1,
		},
		{
			gpuType:             NvidiaTeslaK80.Name(),
			machineType:         machineTypeN1,
			gpuMaxSharedClients: "2",
			gpuCount:            8,
			cpuCount:            1,
			memCount:            1,
		},
		// invalid max shared clients
		{
			gpuType:             NvidiaTeslaK80.Name(),
			machineType:         machineTypeN1,
			gpuMaxSharedClients: "300",
			gpuCount:            8,
			cpuCount:            1,
			memCount:            1,
			expectedErr:         true,
		},
		// invalid gpu
		{
			gpuType:     "duke-igthorn",
			machineType: machineTypeN1,
			gpuCount:    1,
			cpuCount:    1,
			memCount:    1,
			expectedErr: true,
		},
		// too many gpusHelperValidateGpuConfig
		{
			gpuType:     NvidiaTeslaP4.Name(),
			machineType: machineTypeN1,
			gpuCount:    8,
			cpuCount:    1,
			memCount:    1,
			expectedErr: true,
		},
		// 1 gpu with large machine
		{
			gpuType:     NvidiaTeslaK80.Name(),
			machineType: machineTypeN1,
			gpuCount:    1,
			cpuCount:    32,
			memCount:    1,
			expectedErr: true,
		},
		// correct A100 gpu count
		{
			gpuType:     NvidiaTeslaA100.Name(),
			machineType: machineTypeA2,
			gpuCount:    1,
			cpuCount:    12,
			memCount:    40,
		},
		{
			gpuType:          NvidiaTeslaA100.Name(),
			gpuPartitionSize: "1g.5gb",
			machineType:      machineTypeA2,
			gpuCount:         7,
			cpuCount:         12,
			memCount:         40,
		},
		{
			gpuType:             NvidiaTeslaA100.Name(),
			gpuPartitionSize:    "1g.5gb",
			machineType:         machineTypeA2,
			gpuMaxSharedClients: "3",
			gpuCount:            21,
			cpuCount:            12,
			memCount:            40,
		},
		// incorrect A100 gpu count
		{
			gpuType:     NvidiaTeslaA100.Name(),
			machineType: machineTypeA2,
			gpuCount:    2,
			cpuCount:    12,
			memCount:    40,
			expectedErr: true,
		},
		{
			gpuType:          NvidiaTeslaA100.Name(),
			gpuPartitionSize: "1g.5gb",
			machineType:      machineTypeA2,
			gpuCount:         14,
			cpuCount:         12,
			memCount:         40,
			expectedErr:      true,
		},
		// nvidia-a100-80gb
		{
			gpuType:     NvidiaA100_80gb.Name(),
			machineType: machineTypeA2Plus,
			gpuCount:    1,
			cpuCount:    12,
			memCount:    80,
		},
		{
			gpuType:          NvidiaA100_80gb.Name(),
			gpuPartitionSize: "1g.10gb",
			machineType:      machineTypeA2Plus,
			gpuCount:         7,
			cpuCount:         12,
			memCount:         80,
		},
		{
			gpuType:             NvidiaA100_80gb.Name(),
			gpuPartitionSize:    "1g.10gb",
			gpuMaxSharedClients: "3",
			machineType:         machineTypeA2Plus,
			gpuCount:            21,
			cpuCount:            12,
			memCount:            80,
		},
		{
			// Incompatible with families other than A2.
			gpuType:     NvidiaTeslaA100.Name(),
			machineType: machineTypeN1,
			gpuCount:    1,
			cpuCount:    12,
			memCount:    80,
			expectedErr: true,
		},
		{
			// Incompatible with non-A2+ machine types from the A2 family.
			gpuType:     NvidiaA100_80gb.Name(),
			machineType: machineTypeA2,
			gpuCount:    1,
			cpuCount:    12,
			memCount:    80,
			expectedErr: true,
		},
		{
			// Incorrect partition size.
			gpuType:          NvidiaA100_80gb.Name(),
			machineType:      machineTypeA2Plus,
			gpuPartitionSize: "1g.5gb",
			gpuCount:         7,
			cpuCount:         12,
			memCount:         80,
			expectedErr:      true,
		},
		{
			// Too much GPU.
			gpuType:     NvidiaA100_80gb.Name(),
			machineType: machineTypeA2Plus,
			gpuCount:    2,
			cpuCount:    12,
			memCount:    80,
			expectedErr: true,
		},
		{
			// Too much GPU with partitioning and time-sharing.
			gpuType:             NvidiaA100_80gb.Name(),
			gpuPartitionSize:    "1g.10gb",
			gpuMaxSharedClients: "3",
			machineType:         machineTypeA2Plus,
			gpuCount:            22,
			cpuCount:            12,
			memCount:            80,
			expectedErr:         true,
		},
		// correct L4 gpu count
		{
			gpuType:     NvidiaL4.Name(),
			machineType: machineTypeG2,
			gpuCount:    1,
			cpuCount:    4,
			memCount:    16,
		},
		// incorrect L4 gpu count
		{
			gpuType:     NvidiaL4.Name(),
			machineType: machineTypeG2,
			gpuCount:    2,
			cpuCount:    4,
			memCount:    16,
			expectedErr: true,
		},
		{
			// Incompatible with families other than G2.
			gpuType:     NvidiaL4.Name(),
			machineType: machineTypeN1,
			gpuCount:    1,
			cpuCount:    4,
			memCount:    16,
			expectedErr: true,
		},
		// correct nvidia-h100-80gb count.
		{
			gpuType:     NvidiaH100_80gb.Name(),
			machineType: machineTypeA3,
			gpuCount:    8,
			cpuCount:    208,
			memCount:    1872,
		},
		// incompatible with families other than A3.
		{
			gpuType:     NvidiaH100_80gb.Name(),
			machineType: machineTypeN1,
			gpuCount:    1,
			cpuCount:    26,
			memCount:    234,
			expectedErr: true,
		},
		// incorrect H100 gpu count.
		{
			gpuType:     NvidiaH100_80gb.Name(),
			machineType: machineTypeA3,
			gpuCount:    2,
			cpuCount:    208,
			memCount:    1872,
			expectedErr: true,
		},
		// correct nvidia-h100-mega-80gb count.
		{
			gpuType:     NvidiaH100Mega_80gb.Name(),
			machineType: machineTypeA3Plus,
			gpuCount:    8,
			cpuCount:    208,
			memCount:    1872,
		},
		// incompatible with families other than A3.
		{
			gpuType:     NvidiaH100Mega_80gb.Name(),
			machineType: machineTypeN1,
			gpuCount:    1,
			cpuCount:    26,
			memCount:    234,
			expectedErr: true,
		},
		// incorrectnvidia-h100-mega-80gb count.
		{
			gpuType:     NvidiaH100Mega_80gb.Name(),
			machineType: machineTypeA3,
			gpuCount:    2,
			cpuCount:    208,
			memCount:    1872,
			expectedErr: true,
		},
		// correct nvidia-b200 count.
		{
			gpuType:     NvidiaB200.Name(),
			machineType: machineTypeA4,
			gpuCount:    8,
			cpuCount:    224,
			memCount:    3968,
		},
		// incompatible with families other than A4.
		{
			gpuType:     NvidiaB200.Name(),
			machineType: machineTypeN1,
			gpuCount:    1,
			cpuCount:    26,
			memCount:    234,
			expectedErr: true,
		},
		// incorrect nvidia-b200 count.
		{
			gpuType:     NvidiaB200.Name(),
			machineType: machineTypeA4,
			gpuCount:    2,
			cpuCount:    224,
			memCount:    3968,
			expectedErr: true,
		},
		// correct nvidia-gb200 count.
		{
			gpuType:     NvidiaGB200.Name(),
			machineType: machineTypeA4X,
			gpuCount:    4,
			cpuCount:    140,
			memCount:    884,
		},
		// correct nvidia-gb300 count
		{
			gpuType:     NvidiaGB300.Name(),
			machineType: machineTypeA4XMax,
			gpuCount:    4,
			cpuCount:    144,
			memCount:    960,
		},
		// incompatible with families other than A4.
		{
			gpuType:     NvidiaGB200.Name(),
			machineType: machineTypeN1,
			gpuCount:    1,
			cpuCount:    26,
			memCount:    234,
			expectedErr: true,
		},
		// incorrect nvidia-b200 count.
		{
			gpuType:     NvidiaGB200.Name(),
			machineType: machineTypeA4,
			gpuCount:    20,
			cpuCount:    200,
			memCount:    2000,
			expectedErr: true,
		},
		// correct nvidia-rtx-pro-6000 count.
		{
			gpuType:     NvidiaRTXPro6000.Name(),
			machineType: machineTypeG4,
			gpuCount:    1,
			cpuCount:    48,
			memCount:    180,
		},
		// incompatible with families other than G4.
		{
			gpuType:     NvidiaRTXPro6000.Name(),
			machineType: machineTypeN1,
			gpuCount:    1,
			cpuCount:    26,
			memCount:    234,
			expectedErr: true,
		},
		// incorrect nvidia-rtx-pro-6000 count.
		{
			gpuType:     NvidiaRTXPro6000.Name(),
			machineType: machineTypeG4,
			gpuCount:    3,
			cpuCount:    48,
			memCount:    180,
			expectedErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%v-%v-%v-%v", tc.gpuType, tc.machineType, tc.gpuCount, tc.cpuCount), func(t *testing.T) {
			t.Parallel()
			mcp := NewMachineConfigProvider(nil)
			err := mcp.ValidateGpuForMachineType(tc.gpuType, tc.gpuPartitionSize, tc.gpuMaxSharedClients, tc.machineType, tc.gpuCount, tc.cpuCount, tc.memCount)
			if tc.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
func TestValidateGpuTypeForMachineType_allMachineTypesWithGpuOverrideAreValid(t *testing.T) {
	testFunc := func(mtName string, mt MachineType) {
		override, found := mt.gpuOverride()
		if !found {
			return
		}
		t.Run(fmt.Sprintf("%v-%v-%v-%v", override.gpu.Name(), mtName, override.fixedGpuCount, mt.CPU), func(t *testing.T) {
			t.Parallel()
			mcp := NewMachineConfigProvider(nil)
			err := mcp.ValidateGpuForMachineType(override.gpu.Name(), "", "", mtName, AllocatableGpuCount(override.fixedGpuCount), mt.CPU, mt.Memory)
			assert.NoError(t, err)
		})
	}
	for _, mf := range NewMachineConfigProvider(nil).AllMachineFamilies() {
		for mtName, mt := range mf.autoprovisionedMachineTypes {
			testFunc(mtName, mt)
		}
		for mtName, mt := range mf.otherMachineTypes {
			testFunc(mtName, mt)
		}
	}
}

func TestGetAvailableGpuTypes(t *testing.T) {
	tests := []struct {
		name          string
		minLimits     map[string]int64
		maxLimits     map[string]int64
		expectedTypes []string
	}{
		{
			name:          "no limits",
			minLimits:     map[string]int64{},
			maxLimits:     map[string]int64{},
			expectedTypes: []string{},
		},
		{
			name:          "no max limits",
			minLimits:     map[string]int64{NvidiaTeslaK80.Name(): 1, NvidiaTeslaP100.Name(): 2},
			maxLimits:     map[string]int64{},
			expectedTypes: []string{},
		},
		{
			name:          "min and max limits",
			minLimits:     map[string]int64{NvidiaTeslaK80.Name(): 1, NvidiaTeslaP100.Name(): 2},
			maxLimits:     map[string]int64{NvidiaTeslaK80.Name(): 100, NvidiaTeslaP100.Name(): 200},
			expectedTypes: []string{NvidiaTeslaK80.Name(), NvidiaTeslaP100.Name()},
		},
		{
			name:          "just max limits",
			minLimits:     map[string]int64{},
			maxLimits:     map[string]int64{NvidiaTeslaK80.Name(): 100, NvidiaTeslaP100.Name(): 200},
			expectedTypes: []string{NvidiaTeslaK80.Name(), NvidiaTeslaP100.Name()},
		},
	}

	for _, tc := range tests {
		limiter := cloudprovider.NewResourceLimiter(tc.minLimits, tc.maxLimits)
		mcp := NewMachineConfigProvider(nil)
		actualTypes := mcp.GetAvailableGpuTypes(limiter)
		assert.ElementsMatch(t, actualTypes, tc.expectedTypes, fmt.Sprintf("failed for %s", tc.name))
	}
}

func TestValidateMaxGpuCountExists(t *testing.T) {
	mcp := NewMachineConfigProvider(nil)
	for _, gpu := range mcp.GetAllGpuTypes() {
		assert.NotNil(t, gpu.maxCpuCount)
		assert.NotEmpty(t, gpu.maxCpuCount)
	}
}

func TestGetMaxGpuCount(t *testing.T) {
	tests := []struct {
		gpuType             string
		gpuPartitionSize    string
		gpuMaxSharedClients string
		maxGpuCount         AllocatableGpuCount
		expectedErr         bool
	}{
		{
			gpuType:     "nvidia-tesla-k80",
			maxGpuCount: 8,
		},
		{
			gpuType:     "nvidia-tesla-p100",
			maxGpuCount: 4,
		},
		{
			gpuType:     "nvidia-tesla-a100",
			maxGpuCount: 16,
		},
		{
			gpuType:          "nvidia-tesla-a100",
			gpuPartitionSize: "1g.5gb",
			maxGpuCount:      16 * 7,
		},
		{
			gpuType:     "nvidia-tesla-blah",
			expectedErr: true,
		},
		{
			gpuType:             "nvidia-tesla-k80",
			gpuMaxSharedClients: "6",
			maxGpuCount:         48,
		},
		{
			gpuType:             "nvidia-tesla-a100",
			gpuPartitionSize:    "1g.5gb",
			gpuMaxSharedClients: "3",
			maxGpuCount:         16 * 7 * 3,
		},
		{
			gpuType:             "nvidia-tesla-k80",
			gpuMaxSharedClients: "100",
			expectedErr:         true,
		},
		{
			gpuType:     "nvidia-a100-80gb",
			maxGpuCount: 8,
		},
		{
			gpuType:     "nvidia-h100-80gb",
			maxGpuCount: 8,
		},
		{
			gpuType:          "nvidia-a100-80gb",
			gpuPartitionSize: "1g.10gb",
			maxGpuCount:      8 * 7,
		},
		{
			gpuType:             "nvidia-a100-80gb",
			gpuPartitionSize:    "1g.10gb",
			gpuMaxSharedClients: "3",
			maxGpuCount:         8 * 7 * 3,
		},
		{
			gpuType:          "nvidia-a100-80gb",
			gpuPartitionSize: "1g.5gb",
			expectedErr:      true,
		},
		{
			gpuType:     "nvidia-l4",
			maxGpuCount: 8,
		},
		{
			gpuType:     "nvidia-b200",
			maxGpuCount: 8,
		},
		{
			gpuType:     "nvidia-gb200",
			maxGpuCount: 4,
		},
		{
			gpuType:     "nvidia-gb300",
			maxGpuCount: 4,
		},
	}

	for _, tc := range tests {
		mcp := NewMachineConfigProvider(nil)
		count, err := mcp.GetMaxAllocatableGpuCount(tc.gpuType, tc.gpuPartitionSize, tc.gpuMaxSharedClients)
		if tc.expectedErr {
			assert.Error(t, err)
		} else {
			assert.Equal(t, tc.maxGpuCount, count)
			assert.NoError(t, err)
		}
	}
}

func TestGetGpuCpuConfigsGreaterOrEqual(t *testing.T) {
	tests := []struct {
		gpuType                       string
		gpuPartitionSize              string
		gpuMaxSharedClients           string
		minNumberOfGpus               AllocatableGpuCount
		configsWithGreaterOrEqualGpus []AllocatableGpuCount
	}{
		{
			gpuType:                       NvidiaTeslaK80.Name(),
			minNumberOfGpus:               1,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{1, 2, 4, 8},
		},
		{
			gpuType:                       NvidiaTeslaK80.Name(),
			minNumberOfGpus:               2,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{2, 4, 8},
		},
		{
			gpuType:         NvidiaTeslaK80.Name(),
			minNumberOfGpus: 12,
		},
		{
			gpuType:                       NvidiaTeslaP100.Name(),
			minNumberOfGpus:               1,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{1, 2, 4},
		},
		{
			gpuType:                       NvidiaTeslaP100.Name(),
			minNumberOfGpus:               4,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{4},
		},
		{
			gpuType:         NvidiaTeslaP100.Name(),
			minNumberOfGpus: 8,
		},
		{
			gpuType:                       NvidiaTeslaV100.Name(),
			minNumberOfGpus:               1,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{1, 2, 4, 8},
		},
		{
			gpuType:                       NvidiaTeslaV100.Name(),
			minNumberOfGpus:               2,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{2, 4, 8},
		},
		{
			gpuType:         NvidiaTeslaV100.Name(),
			minNumberOfGpus: 16,
		},
		{
			gpuType:                       NvidiaTeslaP4.Name(),
			minNumberOfGpus:               1,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{1, 2, 4},
		},
		{
			gpuType:         NvidiaTeslaP4.Name(),
			minNumberOfGpus: 8,
		},
		{
			gpuType:                       NvidiaTeslaT4.Name(),
			minNumberOfGpus:               2,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{2, 4},
		},
		{
			gpuType:                       NvidiaTeslaT4.Name(),
			minNumberOfGpus:               4,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{4},
		},
		{
			gpuType:                       NvidiaTeslaA100.Name(),
			minNumberOfGpus:               1,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{1, 2, 4, 8, 16},
		},
		{
			gpuType:                       NvidiaTeslaA100.Name(),
			minNumberOfGpus:               8,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{8, 16},
		},
		{
			gpuType:         NvidiaTeslaA100.Name(),
			minNumberOfGpus: 20,
		},
		{
			gpuType:                       NvidiaA100_80gb.Name(),
			minNumberOfGpus:               2,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{2, 4, 8},
		},
		{
			gpuType:                       NvidiaA100_80gb.Name(),
			minNumberOfGpus:               8,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{8},
		},
		{
			gpuType:                       NvidiaTeslaA100.Name(),
			gpuPartitionSize:              "1g.5gb",
			minNumberOfGpus:               4,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{7, 14, 28, 56, 112},
		},
		{
			gpuType:                       NvidiaTeslaA100.Name(),
			gpuPartitionSize:              "7g.40gb",
			minNumberOfGpus:               4,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{4, 8, 16},
		},
		{
			gpuType:                       NvidiaA100_80gb.Name(),
			gpuPartitionSize:              "2g.20gb",
			minNumberOfGpus:               1,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{3, 6, 12, 24},
		},
		{
			gpuType:                       NvidiaTeslaV100.Name(),
			gpuMaxSharedClients:           "3",
			minNumberOfGpus:               3,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{3, 6, 12, 24},
		},
		{
			gpuType:                       NvidiaTeslaA100.Name(),
			gpuMaxSharedClients:           "3",
			minNumberOfGpus:               6,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{6, 12, 24, 48},
		},
		{
			gpuType:                       NvidiaTeslaA100.Name(),
			gpuMaxSharedClients:           "4",
			minNumberOfGpus:               2,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{4, 8, 16, 32, 64},
		},
		{
			gpuType:                       NvidiaTeslaA100.Name(),
			gpuPartitionSize:              "2g.10gb",
			gpuMaxSharedClients:           "4",
			minNumberOfGpus:               2,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{12, 24, 48, 96, 192},
		},
		{
			gpuType:                       NvidiaTeslaA100.Name(),
			gpuPartitionSize:              "7g.40gb",
			gpuMaxSharedClients:           "3",
			minNumberOfGpus:               2,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{3, 6, 12, 24, 48},
		},
		{
			gpuType:                       NvidiaL4.Name(),
			minNumberOfGpus:               1,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{1, 2, 4, 8},
		},
		{
			gpuType:                       NvidiaL4.Name(),
			minNumberOfGpus:               4,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{4, 8},
		},
		{
			gpuType:                       NvidiaH100_80gb.Name(),
			minNumberOfGpus:               8,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{8},
		},
		{
			gpuType:                       NvidiaH100_80gb.Name(),
			minNumberOfGpus:               1,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{1, 2, 4, 8},
		},
		{
			gpuType:                       NvidiaH200Ultra_141gb.Name(),
			minNumberOfGpus:               8,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{8},
		},
		{
			gpuType:                       NvidiaB200.Name(),
			minNumberOfGpus:               8,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{8},
		},
		{
			gpuType:                       NvidiaGB200.Name(),
			minNumberOfGpus:               4,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{4},
		},
		{
			gpuType:                       NvidiaGB300.Name(),
			minNumberOfGpus:               4,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{4},
		},
		{
			gpuType:                       NvidiaRTXPro6000.Name(),
			minNumberOfGpus:               1,
			configsWithGreaterOrEqualGpus: []AllocatableGpuCount{1, 2, 4, 8},
		},
	}

	for _, tc := range tests {
		mcp := NewMachineConfigProvider(nil)
		configs, _, _, _ := mcp.GetGpuConfigsGreaterOrEqual(tc.gpuPartitionSize, tc.gpuMaxSharedClients, tc.gpuType, tc.minNumberOfGpus)
		assert.Equal(t, tc.configsWithGreaterOrEqualGpus, configs)
	}
}

func TestAllGPUsHaveMachineFamilySpecified(t *testing.T) {
	for g := range allGpusByName {
		t.Run(g, func(t *testing.T) {
			m, ok := machineFamilyForGPU[g]
			if !ok {
				t.Fatalf("GPU named %q does not have a machine family specified in the \"machineFamilyForGPU\" map.", g)
			}

			_, gpuSupported := m.supportedGpuTypes[g]
			if !gpuSupported {
				for _, machine := range m.AllMachineTypes(NoConstraints) {
					override, hasOverride := machine.gpuOverride()
					if hasOverride && override.gpu.Name() == g {
						gpuSupported = true
						break
					}
				}
			}
			if !gpuSupported {
				t.Fatalf("GPU named %q specifies machine family %q in the \"machineFamilyForGPU\" map, but is not supported by that family.", g, m.name)
			}
		})
	}
}
