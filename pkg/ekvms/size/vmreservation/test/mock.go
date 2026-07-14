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

package test

import (
	"fmt"

	"github.com/stretchr/testify/mock"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
)

// MockVmReservation returns a mock for VmReservation.
//
// On.Return method in the mock accepts either int64 value (which will be returned in the VmReservation methods)
// or a function of type func(physicalMemory/physicalCpuMillicores int64) int64 (where the value of its execution will be returned in the VmReservation methods).
type MockVmReservation struct {
	mock.Mock
}

func (m *MockVmReservation) CalculateKernelReserved(osInfo gce.MigOsInfo, physicalMemory int64) int64 {
	args := m.MethodCalled("CalculateKernelReserved", osInfo, physicalMemory)
	switch v := args.Get(0).(type) {
	case int64:
		return v
	case func(args int64) int64:
		return v(physicalMemory)
	default:
		panic(fmt.Sprintf("Return value is of wrong type (%T)", v))
	}
}

func (m *MockVmReservation) PredictKubeReservedMemory(physicalMemory int64, gcfsEnabled bool) int64 {
	args := m.MethodCalled("PredictKubeReservedMemory", physicalMemory, gcfsEnabled)
	switch v := args.Get(0).(type) {
	case int64:
		return v
	case func(args int64) int64:
		return v(physicalMemory)
	default:
		panic(fmt.Sprintf("Return value is of wrong type (%T)", v))
	}
}

func (m *MockVmReservation) PredictKubeReservedCpuMillicores(physicalCpuMillicores int64, machineType string, maxPodsPerNode int64) int64 {
	args := m.MethodCalled("PredictKubeReservedCpuMillicores", physicalCpuMillicores, machineType)
	switch v := args.Get(0).(type) {
	case int64:
		return v
	case func(args int64) int64:
		return v(physicalCpuMillicores)
	default:
		panic(fmt.Sprintf("Return value is of wrong type (%T)", v))
	}
}
