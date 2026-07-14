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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator"
)

// roundingUpCalculator is a simple Calculator for testing.
// It rounds up milliCpus to the closest multiple of roundingFactor.
type roundingUpCalculator struct {
	roundingFactor int64
}

// NewRoundingCalculator returns a new instance of roundingUpCalculator.
func NewRoundingCalculator(roundingFactor int64) calculator.Calculator {
	return &roundingUpCalculator{roundingFactor: roundingFactor}
}

func (*roundingUpCalculator) ToAllocatable(_ *apiv1.Node, vmSize size.VmSize) size.Allocatable {
	return size.Allocatable(vmSize)
}

func (*roundingUpCalculator) ToVmSize(_ *apiv1.Node, allocatable size.Allocatable) (size.VmSize, error) {
	return size.VmSize(allocatable), nil
}

func (*roundingUpCalculator) MinAllocatable(_ *apiv1.Node) (size.Allocatable, error) {
	return size.Allocatable{}, nil
}

func (rc *roundingUpCalculator) RoundUp(_ *apiv1.Node, allocatable size.Allocatable) (size.Allocatable, error) {
	return size.Allocatable{
		MilliCpus: (allocatable.MilliCpus + (rc.roundingFactor - 1)) / rc.roundingFactor * rc.roundingFactor,
		KBytes:    allocatable.KBytes,
	}, nil
}

func (*roundingUpCalculator) MakeVmSizeValid(_ *apiv1.Node, vmSize size.VmSize) (size.VmSize, error) {
	return vmSize, nil
}

func (*roundingUpCalculator) GetMaxResizableVmSizeByMachineType(_ string) (size.VmSize, error) {
	return size.VmSize{}, fmt.Errorf("GetMaxResizableVmSizeByMachineType not implemented in rounding calculator")
}
