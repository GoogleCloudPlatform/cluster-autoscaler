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

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator"
)

// fakeCalculator is a simple Calculator that is useful for testing due to its predictability.
type fakeCalculator struct {
	machineConfigProvider *machinetypes.MachineConfigProvider
}

// New returns a new instance of fakeCalculator.
func New() calculator.Calculator {
	return &fakeCalculator{}
}

// NewWithProvider returns a new instance of fakeCalculator with machine config provider.
func NewWithProvider(p *machinetypes.MachineConfigProvider) calculator.Calculator {
	return &fakeCalculator{
		machineConfigProvider: p,
	}
}

func (*fakeCalculator) ToAllocatable(_ *apiv1.Node, vmSize size.VmSize) size.Allocatable {
	return size.Allocatable{
		MilliCpus: vmSize.MilliCpus * 3 / 4,
		KBytes:    vmSize.KBytes * 3 / 4,
	}
}

func (*fakeCalculator) ToVmSize(_ *apiv1.Node, allocatable size.Allocatable) (size.VmSize, error) {
	return size.VmSize{
		MilliCpus: allocatable.MilliCpus * 4 / 3,
		KBytes:    allocatable.KBytes * 4 / 3,
	}, nil
}

func (c *fakeCalculator) MinAllocatable(_ *apiv1.Node) (size.Allocatable, error) {
	return size.Allocatable{}, nil
}

func (c *fakeCalculator) RoundUp(_ *apiv1.Node, allocatable size.Allocatable) (size.Allocatable, error) {
	return allocatable, nil
}

func (*fakeCalculator) MakeVmSizeValid(_ *apiv1.Node, vmSize size.VmSize) (size.VmSize, error) {
	return vmSize, nil
}

func (c *fakeCalculator) GetMaxResizableVmSizeByMachineType(machineType string) (size.VmSize, error) {
	if c.machineConfigProvider != nil {
		return c.machineConfigProvider.GetMaxResizableVmSizeByMachineType(machineType)
	}
	return size.VmSize{}, fmt.Errorf("GetMaxResizableVmSizeByMachineType not implemented in fake calculator")
}
