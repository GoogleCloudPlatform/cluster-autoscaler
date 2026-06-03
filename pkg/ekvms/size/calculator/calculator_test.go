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

package calculator

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	vmreservation_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/vmreservation/test"
	ekvms_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/test"
)

func TestConvertBetweenVmSizeAndAllocatable(t *testing.T) {
	mockReservation := new(vmreservation_test.MockVmReservation)
	mockReservation.On("CalculateKernelReserved", mock.Anything, mock.Anything).Return(func(physicalMemory int64) int64 {
		return physicalMemory / 16
	})
	mockReservation.On("PredictKubeReservedMemory", mock.Anything, mock.Anything).Return(func(physicalMemory int64) int64 {
		// We shouldn't attempt calling this function with size not divisible by MiB as there is rounding that breaks monotonicity of the binary search.
		if physicalMemory%size.MiB != 0 {
			assert.Fail(t, "physicalMemory argument in PredictKubeReservedMemory should be multiple of MiB")
			return -physicalMemory
		}
		return physicalMemory / 11
	})
	mockReservation.On("PredictKubeReservedCpuMillicores", mock.Anything, mock.Anything, mock.Anything).Return(func(physicalCpuMillicores int64) int64 {
		return physicalCpuMillicores / 11
	})

	vmSize := size.VmSize{MilliCpus: 10 * 1000, KBytes: 11 * 1024 * 1024}
	evictionThresholdKb := int64(100 * 1024)
	kubeSystemReservationKb := vmSize.KBytes / 11
	kubeSystemReservationMilliCpu := vmSize.MilliCpus / 11
	kernelReservationKbForEkStandard32 := int64(128 * 1024 * 1024 / 16)
	kernelReservationKbForEkStandard16 := int64(64 * 1024 * 1024 / 16)
	kubeProxyMemoryBytesOverheadPerCPU := int64(8192000)
	hugepages1gKb := int64(1 * 1024 * 1024)

	minVmSize := apiv1.ResourceList{
		"cpu":    resource.MustParse("2"),
		"memory": resource.MustParse("4Gi"),
	}
	incrementStep := apiv1.ResourceList{
		"cpu":    resource.MustParse("2"),
		"memory": resource.MustParse("1Mi"),
	}
	safetyBuffer := apiv1.ResourceList{
		"cpu":    resource.MustParse("0.2"),
		"memory": resource.MustParse("500Mi"),
	}

	tests := []struct {
		name               string
		safetyBuffer       apiv1.ResourceList
		isClusterUsingDPV1 bool
		node               *apiv1.Node
		wantAllocatable    size.Allocatable
		expectError        bool
		expectErrorMessage string
	}{
		{
			name:            "Converts both ways correctly - ek-standard-32",
			node:            createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "ek-standard-32"}),
			wantAllocatable: size.Allocatable{MilliCpus: vmSize.MilliCpus - kubeSystemReservationMilliCpu, KBytes: vmSize.KBytes - kubeSystemReservationKb - evictionThresholdKb - kernelReservationKbForEkStandard32},
		},
		{
			name:         "Converts both ways correctly with safety buffer - ek-standard-32",
			safetyBuffer: safetyBuffer,
			node:         createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "ek-standard-32"}),
			wantAllocatable: size.Allocatable{
				MilliCpus: vmSize.MilliCpus - kubeSystemReservationMilliCpu - safetyBuffer.Cpu().MilliValue(),
				KBytes:    vmSize.KBytes - kubeSystemReservationKb - evictionThresholdKb - kernelReservationKbForEkStandard32 - (safetyBuffer.Memory().Value() / size.KiB)},
		},
		{
			name:               "Converts both ways correctly with safety buffer for DPV1 cluster - ek-standard-32",
			safetyBuffer:       safetyBuffer,
			isClusterUsingDPV1: true,
			node:               createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "ek-standard-32"}),
			wantAllocatable: size.Allocatable{
				MilliCpus: vmSize.MilliCpus - kubeSystemReservationMilliCpu - safetyBuffer.Cpu().MilliValue(),
				KBytes:    vmSize.KBytes - kubeSystemReservationKb - evictionThresholdKb - kernelReservationKbForEkStandard32 - (safetyBuffer.Memory().Value() / size.KiB) - (kubeProxyMemoryBytesOverheadPerCPU*32)/size.KiB},
		},
		{
			name:            "Converts both ways correctly - ek-standard-16",
			node:            createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "ek-standard-16"}),
			wantAllocatable: size.Allocatable{MilliCpus: vmSize.MilliCpus - kubeSystemReservationMilliCpu, KBytes: vmSize.KBytes - kubeSystemReservationKb - evictionThresholdKb - kernelReservationKbForEkStandard16},
		},
		{
			name:               "Converts both ways correctly for DPV1 cluster - ek-standard-16",
			isClusterUsingDPV1: true,
			node:               createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "ek-standard-16"}),
			wantAllocatable:    size.Allocatable{MilliCpus: vmSize.MilliCpus - kubeSystemReservationMilliCpu, KBytes: vmSize.KBytes - kubeSystemReservationKb - evictionThresholdKb - kernelReservationKbForEkStandard16 - (kubeProxyMemoryBytesOverheadPerCPU*16)/size.KiB},
		},
		{
			name:            "Converts both ways correctrly with hugepages - ek-standard-32",
			node:            createEkNodeWithHugepages("node-1", resource.MustParse("1Gi")),
			wantAllocatable: size.Allocatable{MilliCpus: vmSize.MilliCpus - kubeSystemReservationMilliCpu, KBytes: vmSize.KBytes - kubeSystemReservationKb - evictionThresholdKb - kernelReservationKbForEkStandard32 - hugepages1gKb},
		},
		{
			name:               "returns error on invalid machine type",
			node:               createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "invalid-machine-type"}),
			expectError:        true,
			expectErrorMessage: "failed to get machine family for machine type invalid-machine-type in node \"node-1\": unsupported machine family \"invalid\"",
		}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			limitConfig := LimitConfig{MinVmSize: minVmSize, IncrementStep: incrementStep, SafetyBuffer: tc.safetyBuffer}
			limitProvider := NewResizeLimitProvider(&mockCloudProvider{})
			limitProvider.RegisterConfig(machinetypes.EK.Name(), limitConfig)
			sizeCalc := New(mockReservation, &mockCloudProvider{}, tc.isClusterUsingDPV1, limitProvider)
			actualVmSize, err := sizeCalc.ToVmSize(tc.node, tc.wantAllocatable)
			if tc.expectError {
				assert.EqualError(t, err, tc.expectErrorMessage)
			} else {
				assert.NoError(t, err)
				assert.Equalf(t, vmSize, actualVmSize, "ToVmSize(%v)", vmSize)
				assert.Equalf(t, tc.wantAllocatable, sizeCalc.ToAllocatable(tc.node, vmSize), "ToAllocatable(%v)", vmSize)
			}
		})
	}
}

func TestGetMachineType(t *testing.T) {
	tests := []struct {
		name                string
		node                *apiv1.Node
		expectedMachineType string
		expectError         bool
	}{
		{
			name:                "with machine type in labels",
			node:                createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "ek-standard-32-test"}),
			expectedMachineType: "ek-standard-32-test",
			expectError:         false,
		},
		{
			name: "without machine type in labels",
			node: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-1",
				},
			},
			expectedMachineType: "", // Expected machine type is irrelevant if an error is expected.
			expectError:         true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			machineType, err := getMachineType(tc.node)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedMachineType, machineType)
			}
		})
	}
}

func TestGetGkeMigOsInfo(t *testing.T) {
	tests := []struct {
		name           string
		node           *apiv1.Node
		expectedOsInfo *gke.GkeMigOsInfo
	}{
		{
			name: "Using defaults",
			node: createEkNodeWithLabelsAndVersion("node-1", "", nil),
			expectedOsInfo: gke.NewGkeMigOsInfo(gce.NewMigOsInfo(
				gce.OperatingSystemDefault,
				gce.OperatingSystemDistributionDefault,
				gce.DefaultArch,
			), "1.30.3-gke.1225000", false),
		},
		{
			name: "With version and all labels set",
			node: createEkNodeWithLabelsAndVersion("node-1", "1.28.1", map[string]string{
				apiv1.LabelOSStable:              string(gce.OperatingSystemLinux),
				gkelabels.GkeOsDistributionLabel: string(gce.OperatingSystemDistributionUbuntu),
				apiv1.LabelArchStable:            gce.Arm64.Name(),
				gkelabels.GkeConfidentialNodes:   "true",
			}),
			expectedOsInfo: gke.NewGkeMigOsInfo(gce.NewMigOsInfo(
				gce.OperatingSystemLinux,
				gce.OperatingSystemDistributionUbuntu,
				gce.Arm64,
			), "1.28.1", true),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedOsInfo, getGkeMigOsInfo(&mockCloudProvider{}, tc.node))
		})
	}
}

func TestBinarySearchFirstTrue(t *testing.T) {
	tests := []struct {
		name        string
		low         int64
		high        int64
		f           func(int64) bool
		expectedVal int64
	}{
		{
			name:        "returns correct value",
			low:         55,
			high:        75,
			f:           func(val int64) bool { return val >= 71 },
			expectedVal: 71,
		},
		{
			name:        "returns correct value with low being large negative",
			low:         -987654321098765432,
			high:        987654321098765432,
			f:           func(val int64) bool { return val >= -987654321098765 },
			expectedVal: -987654321098765,
		},
		{
			name:        "returns correct value with low and high being negative",
			low:         -987654321098765432,
			high:        -987654321,
			f:           func(val int64) bool { return val >= -987654321098765 },
			expectedVal: -987654321098765,
		},
		{
			name:        "returns correct value with low and high being the same value",
			low:         987654321098765432,
			high:        987654321098765432,
			f:           func(val int64) bool { return val >= 987654321098765432 },
			expectedVal: 987654321098765432,
		},
		{
			name:        "returns correct value with low and high being have difference of 1 value",
			low:         -1,
			high:        0,
			f:           func(val int64) bool { return val >= 0 },
			expectedVal: 0,
		},
		{
			name:        "returns correct value without overflowing",
			low:         math.MaxInt64,
			high:        math.MaxInt64,
			f:           func(val int64) bool { return val >= math.MaxInt64 },
			expectedVal: math.MaxInt64,
		},
		{
			name:        "returns math.MinInt64 when value doesn't exist",
			low:         -987654321098765432,
			high:        987654321098765432,
			f:           func(val int64) bool { return val >= 987654321098765433 },
			expectedVal: math.MinInt64,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedVal, binarySearchFirstTrue(tc.low, tc.high, tc.f))
		})
	}
}

func TestMinMemKiBLimit(t *testing.T) {
	for name, tc := range map[string]struct {
		node                *apiv1.Node
		calculatorMinMemory int64
		desiredMemory       int64
		expectedMinMemory   int64
		expectError         bool
		expectErrorMessage  string
	}{
		"apply_min_mem_from_desired": {
			node:                createEkNode32(),
			calculatorMinMemory: 4 * 1024 * 1024,
			desiredMemory:       8 * 1024 * 1024,
			expectedMinMemory:   8 * 1024 * 1024,
		},
		"apply_min_mem_from_calculator": {
			node:                createEkNode32(),
			calculatorMinMemory: 4 * 1024 * 1024,
			desiredMemory:       2 * 1024 * 1024,
			expectedMinMemory:   4 * 1024 * 1024,
		},
		"apply_min_mem_from_ek_8": {
			node:                createEkNode8(),
			calculatorMinMemory: 1 * 1024 * 1024,
			desiredMemory:       1 * 1024 * 1024,
			expectedMinMemory:   2 * 1024 * 1024,
		},
		"apply_min_mem_from_ek_32": {
			node:                createEkNode32(),
			calculatorMinMemory: 1 * 1024 * 1024,
			desiredMemory:       1 * 1024 * 1024,
			expectedMinMemory:   3 * 1024 * 1024,
		},
		"error_on_invalid_machine_type": {
			node:               createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "invalid-machine-type"}),
			expectError:        true,
			expectErrorMessage: "failed to find info for machine type invalid-machine-type in node \"node-1\": unsupported machine family \"invalid\"",
		}} {
		t.Run(name, func(t *testing.T) {
			minVmSize := size.VmSize{MilliCpus: 2000, KBytes: tc.calculatorMinMemory}
			limits := limits{
				minVmSize: minVmSize,
			}
			sizeCalc := &calculator{
				provider: &mockCloudProvider{},
			}
			desiredSize := size.VmSize{
				MilliCpus: 2000,
				KBytes:    tc.desiredMemory,
			}
			actualMinMemory, err := sizeCalc.minMemKiBLimit(tc.node, desiredSize, limits)
			if tc.expectError {
				assert.EqualError(t, err, tc.expectErrorMessage)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedMinMemory, actualMinMemory)
			}
		})
	}
}

func TestMakeSizeValid(t *testing.T) {
	const (
		minVmSizeMilliCpu   = 2000
		minVmSizeKiB        = 4 * 1024 * 1024 //4Gi
		ekIncrementMilliCpu = 2000
		ekIncrementKiB      = 1024
	)
	for name, tc := range map[string]struct {
		in                 size.VmSize
		expect             size.VmSize
		node               *apiv1.Node
		expectError        bool
		expectErrorMessage string
	}{
		"apply_min_mem": {
			in: size.VmSize{
				MilliCpus: minVmSizeMilliCpu,
				KBytes:    minVmSizeKiB / 2,
			},
			expect: size.VmSize{
				MilliCpus: minVmSizeMilliCpu,
				KBytes:    minVmSizeKiB,
			},
		},
		"apply_min_cpu": {
			in: size.VmSize{
				MilliCpus: minVmSizeMilliCpu / 2,
				KBytes:    minVmSizeKiB,
			},
			expect: size.VmSize{
				MilliCpus: minVmSizeMilliCpu,
				KBytes:    minVmSizeKiB,
			},
		},
		"round_up_cpu": {
			in: size.VmSize{
				MilliCpus: minVmSizeMilliCpu + 1,
				KBytes:    minVmSizeKiB,
			},
			expect: size.VmSize{
				MilliCpus: minVmSizeMilliCpu + ekIncrementMilliCpu,
				KBytes:    minVmSizeKiB,
			},
		},
		"round_up_mem": {
			in: size.VmSize{
				MilliCpus: minVmSizeMilliCpu,
				KBytes:    minVmSizeKiB + 1,
			},
			expect: size.VmSize{
				MilliCpus: minVmSizeMilliCpu,
				KBytes:    minVmSizeKiB + ekIncrementKiB,
			},
		},
		"increase_mem_to_match_cpu": {
			in: size.VmSize{
				MilliCpus: 8000,
				KBytes:    1024 * 1024,
			},
			expect: size.VmSize{
				MilliCpus: 8000,
				KBytes:    4 * 1024 * 1024, // We need at least 512 MiB mem per CPU, we want 8 CPUs so at least 4 GiB mem
			},
		},
		"increase_cpu_to_match_mem": {
			in: size.VmSize{
				MilliCpus: 250,
				KBytes:    64 * 1024 * 1024,
			},
			expect: size.VmSize{
				MilliCpus: 8000, // We can have at most 8 GiB mem per CPU; We want 64 GiB, so we need 8 CPU
				KBytes:    64 * 1024 * 1024,
			},
		},
		"error_on_invalid_machine_type": {
			node:               createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "invalid-machine-type"}),
			expectError:        true,
			expectErrorMessage: "failed to get machine family for machine type invalid-machine-type in node \"node-1\": unsupported machine family \"invalid\"",
		},
	} {
		t.Run(name, func(t *testing.T) {
			opts := LimitConfig{
				MinVmSize:     apiv1.ResourceList{"cpu": resource.MustParse(fmt.Sprintf("%dm", minVmSizeMilliCpu)), "memory": resource.MustParse(fmt.Sprintf("%dKi", minVmSizeKiB))},
				IncrementStep: apiv1.ResourceList{"cpu": resource.MustParse(fmt.Sprintf("%dm", ekIncrementMilliCpu)), "memory": resource.MustParse(fmt.Sprintf("%dKi", ekIncrementKiB))},
			}
			isClusterUsingDPV1 := true
			limitProvider := NewResizeLimitProvider(&mockCloudProvider{})
			limitProvider.RegisterConfig(machinetypes.EK.Name(), opts)
			sizeCalc := New(nil, &mockCloudProvider{}, isClusterUsingDPV1, limitProvider)
			node := tc.node
			if node == nil {
				node = createEkNode32()
			}
			validSize, err := sizeCalc.MakeVmSizeValid(node, tc.in)
			if tc.expectError {
				assert.EqualError(t, err, tc.expectErrorMessage)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expect, validSize)
			}
		})
	}
}

type mockClusterVersionProvider struct {
	mock.Mock
}

func (m *mockClusterVersionProvider) GetClusterVersion() string {
	return "1.30.3-gke.1225000"
}

func TestMinAllocatable(t *testing.T) {
	mockReservation := new(vmreservation_test.MockVmReservation)
	mockReservation.On("CalculateKernelReserved", mock.Anything, mock.Anything).Return(int64(0))
	mockReservation.On("PredictKubeReservedMemory", mock.Anything, mock.Anything).Return(int64(0))
	mockReservation.On("PredictKubeReservedCpuMillicores", mock.Anything, mock.Anything, mock.Anything).Return(int64(0))

	minVmSize := apiv1.ResourceList{
		"cpu":    resource.MustParse("250m"),
		"memory": resource.MustParse("2Gi"),
	}
	incrementStep := apiv1.ResourceList{
		"cpu":    resource.MustParse("50m"),
		"memory": resource.MustParse("1Mi"),
	}
	limitConfig := LimitConfig{MinVmSize: minVmSize, IncrementStep: incrementStep}
	limitProvider := NewResizeLimitProvider(&mockCloudProvider{})
	limitProvider.RegisterConfig(machinetypes.EK.Name(), limitConfig)
	sizeCalc := New(mockReservation, &mockCloudProvider{}, false, limitProvider)

	t.Run("success", func(t *testing.T) {
		node := createEkNode32()
		// For an ek-standard-32, the minimum memory is 3Gi.
		// This requires ~375m cpu, which rounds up to 400m.
		// With mocks returning 0 for reservations, the allocatable should match this.
		expectedMinAllocatable := size.Allocatable{
			MilliCpus: 400,
			KBytes:    3043328,
		}
		minAlloc, err := sizeCalc.MinAllocatable(node)
		assert.NoError(t, err)
		assert.Equal(t, expectedMinAllocatable, minAlloc)
	})

	t.Run("error on invalid machine type", func(t *testing.T) {
		nodeInvalid := createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "invalid-machine-type"})
		_, err := sizeCalc.MinAllocatable(nodeInvalid)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get machine family for machine type")
	})
}

func TestRoundUp(t *testing.T) {
	mockReservation := new(vmreservation_test.MockVmReservation)
	mockReservation.On("CalculateKernelReserved", mock.Anything, mock.Anything).Return(int64(0))
	mockReservation.On("PredictKubeReservedMemory", mock.Anything, mock.Anything).Return(int64(0))
	mockReservation.On("PredictKubeReservedCpuMillicores", mock.Anything, mock.Anything, mock.Anything).Return(int64(0))

	minVmSize := apiv1.ResourceList{
		"cpu":    resource.MustParse("250m"),
		"memory": resource.MustParse("2Gi"),
	}
	incrementStep := apiv1.ResourceList{
		"cpu":    resource.MustParse("50m"),
		"memory": resource.MustParse("1Mi"),
	}
	limitConfig := LimitConfig{MinVmSize: minVmSize, IncrementStep: incrementStep}
	limitProvider := NewResizeLimitProvider(&mockCloudProvider{})
	limitProvider.RegisterConfig(machinetypes.EK.Name(), limitConfig)
	sizeCalc := New(mockReservation, &mockCloudProvider{}, false, limitProvider)

	t.Run("success", func(t *testing.T) {
		node := createEkNode8()
		// Request an allocatable that isn't a multiple of the increment step.
		alloc := size.Allocatable{
			MilliCpus: 251,
			KBytes:    (2 * 1024 * 1024) + 1,
		}
		// Expect it to be rounded up to the next valid size.
		// Min memory for ek-8 is 2Gi.
		// 2Gi + 1KiB allocatable -> 2Gi + 100Mi + 1KiB capacity -> rounds to 2149 MiB.
		// 2149 MiB capacity -> 2049 MiB allocatable.
		// CPU: 251m -> 300m.
		expectedRoundedAllocatable := size.Allocatable{
			MilliCpus: 300,
			KBytes:    2098176,
		}

		rounded, err := sizeCalc.RoundUp(node, alloc)
		assert.NoError(t, err)
		assert.Equal(t, expectedRoundedAllocatable, rounded)
	})

	t.Run("error on invalid machine type", func(t *testing.T) {
		nodeInvalid := createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "invalid-machine-type"})
		alloc := size.Allocatable{MilliCpus: 1000, KBytes: 1000}
		_, err := sizeCalc.RoundUp(nodeInvalid, alloc)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get machine family for machine type")
	})
}

func createEkNodeWithHugepages(name string, hugepage1gSize resource.Quantity) *apiv1.Node {
	node := createEkNode(name, 0, 0)
	node.Status.Capacity[gke.HugepageSize1gResourceName] = hugepage1gSize
	return node
}

func createEkNodeWithLabelsAndVersion(name, version string, labels map[string]string) *apiv1.Node {
	node := createEkNode(name, 0, 0)
	for k, v := range labels {
		node.Labels[k] = v
	}
	node.Status.NodeInfo.KubeletVersion = version
	return node
}

func createEkNode(name string, cpu, mem int64) *apiv1.Node {
	node := ekvms_test.EkNode32(name, cpu, mem)
	return node
}

func createEkNode8() *apiv1.Node {
	return ekvms_test.EkNode8("ek-8", 8000, 32*1024*1024)
}

func createEkNode32() *apiv1.Node {
	return ekvms_test.EkNode32("ek-32", 32000, 128*1024*1024)
}

type mockCloudProvider struct {
	mock.Mock
}

func (m *mockCloudProvider) GetClusterVersion() string {
	return "1.30.3-gke.1225000"
}

func (m *mockCloudProvider) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return machinetypes.NewMachineConfigProvider(nil)
}
