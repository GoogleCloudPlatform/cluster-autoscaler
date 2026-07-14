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

// Tests in this package tests only hard-coded configuration's integrity,
// not TPU logic. It's useful mostly for validating the configuration for the new TPU types.
//
// TODO(b/394262022): delete it once simship automation is GAed and we stop updating hard-coded configuration.

func TestSingleHostTopologyMap(t *testing.T) {
	for mt, topo := range singleHostTopologyMap {
		t.Run(mt, func(t *testing.T) {
			mcp := NewMachineConfigProvider(nil)
			mf, err := mcp.GetMachineFamilyFromMachineName(mt)
			if err != nil {
				t.Fatalf("Failed to get machine family for %s: %s", mt, err)
			}
			tpuType, err := mcp.TpuTypeForMachineFamily(mf.Name())
			if err != nil {
				t.Fatalf("Failed to get tpu type for %s: %s", mf.Name(), err)
			}
			tpuCount, err := mcp.GetTpuCountForMachineType(mt)
			if err != nil {
				t.Fatalf("Failed to get tpu count for %s: %s", mt, err)
			}
			isMultiHost, err := mcp.IsMultiHostTpuPodslice(tpuType, topo, tpuCount)
			if err != nil {
				t.Fatalf("Failed to check if %s with topology %s is multi-host: %s", mt, topo, err)
			}
			if isMultiHost {
				t.Fatalf("Topology %s for machine type %s is identified inconsistently as multi-host and single-host at the same time", topo, mt)
			}
		})
	}
}

func TestGetMaxTpuCount_supported(t *testing.T) {
	mcp := NewMachineConfigProvider(nil)
	for _, tt := range supportedTpuTypes {
		_, err := mcp.GetMaxTpuCount(tt)
		assert.NoError(t, err)
	}
}

func TestGetMaxTpuCount_unsupported(t *testing.T) {
	mcp := NewMachineConfigProvider(nil)
	_, err := mcp.GetMaxTpuCount("unsupported")
	assert.Error(t, err)
}

func TestIsTPUCountSupported_supported(t *testing.T) {
	mcp := NewMachineConfigProvider(nil)
	for _, mf := range mcp.AllMachineFamilies() {
		if !mf.IsTpuSupported() {
			continue
		}
		for _, mt := range mf.AllMachineTypes(NoConstraints) {
			cnt, err := mcp.GetTpuCountForMachineType(mt.Name)
			assert.NoError(t, err)
			tpu, err := mcp.TpuTypeForMachineFamily(mf.Name())
			assert.NoError(t, err)
			assert.Equal(t, true, mcp.IsTPUCountSupported(tpu, cnt))
			assert.Equal(t, false, mcp.IsTPUCountSupported(tpu, cnt+1))
		}
	}
}

func TestIsTPUCountSupported_unsupportedTpu(t *testing.T) {
	mcp := NewMachineConfigProvider(nil)
	for _, c := range []int64{1, 4, 8} { // most common tpu counts
		assert.Equal(t, false, mcp.IsTPUCountSupported("unsupported-tpu", c))
	}
}

func TestGetTpuCountForMachineType_supported(t *testing.T) {
	mcp := NewMachineConfigProvider(nil)
	for _, mf := range mcp.AllMachineFamilies() {
		if !mf.IsTpuSupported() {
			continue
		}
		for _, mt := range mf.AllMachineTypes(NoConstraints) {
			_, err := mcp.GetTpuCountForMachineType(mt.Name)
			assert.NoError(t, err)
		}
	}
}

func TestGetTpuCountForMachineType_unsupported(t *testing.T) {
	mcp := NewMachineConfigProvider(nil)
	for _, mf := range mcp.AllMachineFamilies() {
		if mf.IsTpuSupported() {
			continue
		}
		for _, mt := range mf.AllMachineTypes(NoConstraints) {
			_, err := mcp.GetTpuCountForMachineType(mt.Name)
			assert.Error(t, err)
		}
	}
}
