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
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
)

var (
	minVmSize = apiv1.ResourceList{
		"cpu":    resource.MustParse("2"),
		"memory": resource.MustParse("4Gi"),
	}
	incrementStep = apiv1.ResourceList{
		"cpu":    resource.MustParse("1"),
		"memory": resource.MustParse("1Mi"),
	}
	safetyBuffer = apiv1.ResourceList{
		"cpu":    resource.MustParse("0.2"),
		"memory": resource.MustParse("500Mi"),
	}
)

func TestGetLimits(t *testing.T) {
	tests := []struct {
		name          string
		machineFamily string
		node          *apiv1.Node
		limitConfig   *LimitConfig
		wantLimits    limits
	}{
		{
			name:          "Registered config - valid",
			machineFamily: machinetypes.EK.Name(),
			node:          createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "ek-standard-32"}),
			limitConfig: &LimitConfig{
				MinVmSize:     minVmSize,
				IncrementStep: incrementStep,
				SafetyBuffer:  safetyBuffer,
			},
			wantLimits: limits{
				minVmSize:       size.VmSize{MilliCpus: minVmSize.Cpu().MilliValue(), KBytes: minVmSize.Memory().Value() / size.KiB},
				incrementStep:   size.VmSize{MilliCpus: incrementStep.Cpu().MilliValue(), KBytes: incrementStep.Memory().Value() / size.KiB},
				safetyBuffer:    safetyBuffer,
				resizableConfig: machinetypes.EK.ResizableConfig(),
			},
		},
		{
			name: "Fallback to default config - valid",
			node: createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "ek-standard-32"}),
			wantLimits: limits{
				minVmSize: size.VmSize{
					MilliCpus: machinetypes.EK.ResizableConfig().MinVmSizeDefaultMilliCPU(),
					KBytes:    machinetypes.EK.ResizableConfig().MinVmSizeDefaultKiB(),
				},
				incrementStep: size.VmSize{
					MilliCpus: machinetypes.EK.ResizableConfig().IncrementStepDefaultMilliCPU(),
					KBytes:    machinetypes.EK.ResizableConfig().IncrementStepDefaultKiB(),
				},
				safetyBuffer:    machinetypes.EK.ResizableConfig().AllocationSafetyDefault,
				resizableConfig: machinetypes.EK.ResizableConfig(),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider := NewResizeLimitProvider(&mockCloudProvider{})
			if tc.limitConfig != nil {
				provider.RegisterConfig(tc.machineFamily, *tc.limitConfig)
			}
			lmts, err := provider.GetLimits(tc.node)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantLimits, lmts)
		})
	}
}

func TestGetLimits_Errors(t *testing.T) {
	tests := []struct {
		name          string
		machineFamily string
		node          *apiv1.Node
		limitConfig   *LimitConfig
		errorContains string
	}{
		{
			name:          "Invalid machine type",
			node:          createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "invalid-machine-type"}),
			errorContains: "unsupported machine family",
		},
		{
			name:          "Not resizable machine type",
			node:          createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "n1-standard-1"}),
			errorContains: "not resizable",
		},
		{
			name:          "Invalid config - MinVmSize below min (CPU)",
			machineFamily: machinetypes.EK.Name(),
			node:          createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "ek-standard-32"}),
			limitConfig: &LimitConfig{
				MinVmSize: apiv1.ResourceList{
					"cpu":    resource.MustParse("100m"), // 100m < 250m (EK min)
					"memory": resource.MustParse("4Gi"),
				},
				IncrementStep: incrementStep,
				SafetyBuffer:  safetyBuffer,
			},
			errorContains: "Min VmSize MilliCPUs has unsupported value",
		},
		{
			name:          "Invalid config - MinVmSize below min (Memory)",
			machineFamily: machinetypes.EK.Name(),
			node:          createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "ek-standard-32"}),
			limitConfig: &LimitConfig{
				MinVmSize: apiv1.ResourceList{
					"cpu":    resource.MustParse("2"),
					"memory": resource.MustParse("1Gi"), // 1Gi < 2Gi (EK min)
				},
				IncrementStep: incrementStep,
				SafetyBuffer:  safetyBuffer,
			},
			errorContains: "Min VmSize KBytes has unsupported value",
		},
		{
			name:          "Invalid config - IncrementStep below min (CPU)",
			machineFamily: machinetypes.EK.Name(),
			node:          createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "ek-standard-32"}),
			limitConfig: &LimitConfig{
				MinVmSize: minVmSize,
				IncrementStep: apiv1.ResourceList{
					"cpu":    resource.MustParse("0"), // 0 < 50m (EK min increment)
					"memory": resource.MustParse("1Mi"),
				},
				SafetyBuffer: safetyBuffer,
			},
			errorContains: "Increment Step MilliCPUs has unsupported value",
		},
		{
			name:          "Invalid config - IncrementStep below min (Memory)",
			machineFamily: machinetypes.EK.Name(),
			node:          createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "ek-standard-32"}),
			limitConfig: &LimitConfig{
				MinVmSize: minVmSize,
				IncrementStep: apiv1.ResourceList{
					"cpu":    resource.MustParse("1"),
					"memory": resource.MustParse("0Mi"), // 0 < 1Mi (EK min increment)
				},
				SafetyBuffer: safetyBuffer,
			},
			errorContains: "Increment Step KBytes has unsupported value",
		},
		{
			name:          "Invalid config - MinVmSize not divisible (CPU)",
			machineFamily: machinetypes.EK.Name(),
			node:          createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "ek-standard-32"}),
			limitConfig: &LimitConfig{
				MinVmSize: apiv1.ResourceList{
					"cpu":    resource.MustParse("2001m"), // not divisible by 50m
					"memory": resource.MustParse("4Gi"),
				},
				IncrementStep: incrementStep,
				SafetyBuffer:  safetyBuffer,
			},
			errorContains: "Min VmSize MilliCPUs has unsupported value",
		},
		{
			name:          "Invalid config - MinVmSize not divisible (Memory)",
			machineFamily: machinetypes.EK.Name(),
			node:          createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "ek-standard-32"}),
			limitConfig: &LimitConfig{
				MinVmSize: apiv1.ResourceList{
					"cpu":    resource.MustParse("2"),
					"memory": resource.MustParse("4194305Ki"), // 4Gi + 1Ki, not divisible by 1Mi
				},
				IncrementStep: incrementStep,
				SafetyBuffer:  safetyBuffer,
			},
			errorContains: "Min VmSize KBytes has unsupported value",
		},
		{
			name:          "Invalid config - IncrementStep not divisible (CPU)",
			machineFamily: machinetypes.EK.Name(),
			node:          createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "ek-standard-32"}),
			limitConfig: &LimitConfig{
				MinVmSize: minVmSize,
				IncrementStep: apiv1.ResourceList{
					"cpu":    resource.MustParse("1001m"), // not divisible by 50m
					"memory": resource.MustParse("1Mi"),
				},
				SafetyBuffer: safetyBuffer,
			},
			errorContains: "Increment Step MilliCPUs has unsupported value",
		},
		{
			name:          "Invalid config - IncrementStep not divisible (Memory)",
			machineFamily: machinetypes.EK.Name(),
			node:          createEkNodeWithLabelsAndVersion("node-1", "", map[string]string{apiv1.LabelInstanceTypeStable: "ek-standard-32"}),
			limitConfig: &LimitConfig{
				MinVmSize: minVmSize,
				IncrementStep: apiv1.ResourceList{
					"cpu":    resource.MustParse("1"),
					"memory": resource.MustParse("1025Ki"), // 1Mi + 1Ki, not divisible by 1Mi
				},
				SafetyBuffer: safetyBuffer,
			},
			errorContains: "Increment Step KBytes has unsupported value",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider := NewResizeLimitProvider(&mockCloudProvider{})
			if tc.limitConfig != nil {
				provider.RegisterConfig(tc.machineFamily, *tc.limitConfig)
			}

			_, err := provider.GetLimits(tc.node)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.errorContains)
		})
	}
}

func TestValidateValue(t *testing.T) {
	tests := []struct {
		name      string
		value     int64
		minValue  int64
		increment int64
		expectErr bool
	}{
		{
			name:      "valid",
			value:     2000,
			minValue:  1000,
			increment: 100,
			expectErr: false,
		},
		{
			name:      "value less than min",
			value:     900,
			minValue:  1000,
			increment: 100,
			expectErr: true,
		},
		{
			name:      "not divisible",
			value:     2050,
			minValue:  1000,
			increment: 100,
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateValue(tc.value, tc.minValue, tc.increment)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
