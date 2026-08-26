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

package utils

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
)

func UpdateAllocatable(node *v1.Node, size size.Allocatable) {
	allocatableCpu := *resource.NewMilliQuantity(size.MilliCpus, resource.DecimalSI)
	allocatableMemory := *resource.NewQuantity(size.KBytes*1024, resource.DecimalSI)

	if node.Status.Allocatable == nil {
		node.Status.Allocatable = map[v1.ResourceName]resource.Quantity{}
	}
	node.Status.Allocatable[v1.ResourceMemory] = allocatableMemory
	node.Status.Allocatable[v1.ResourceCPU] = allocatableCpu
}

// IsEkMachine returns true if the given node is a supported EK machine, based on instance label.
// It will return an error if the instance label is not set.
func IsEkMachine(node *v1.Node) (bool, error) {
	// We expect GKE instance type to be GCE machine type
	machineType, exists := GetMachineTypeFromLabels(node.Labels)
	if !exists {
		return false, fmt.Errorf("node %q does not have instance type label set", node.Name)
	}

	return IsEkMachineType(machineType)
}

// IsEkMachineType returns true if the given machineType is from EK machine family.
func IsEkMachineType(machineType string) (bool, error) {
	family, err := gce.GetMachineFamily(machineType)
	if err != nil {
		return false, err
	}

	if family != machinetypes.EK.Name() {
		return false, nil
	}
	return true, nil
}

// IsResizableMachineType returns true if the given machineType is from a resizable machine family.
func IsResizableMachineType(provider *machinetypes.MachineConfigProvider, machineType string) (bool, error) {
	familyName, err := gce.GetMachineFamily(machineType)
	if err != nil {
		return false, err
	}
	family, err := provider.ToMachineFamily(familyName)
	if err != nil {
		return false, err
	}
	return family.IsResizable(), nil
}

// IsResizableNode returns true if the given node is a supported resizable machine, based on instance label.
// It will return an error if the instance label is not set.
func IsResizableNode(node *v1.Node, provider *machinetypes.MachineConfigProvider) (bool, error) {
	// We expect GKE instance type to be GCE machine type
	machineType, exists := GetMachineTypeFromLabels(node.Labels)
	if !exists {
		return false, fmt.Errorf("node %q does not have instance type label set", node.Name)
	}

	return IsResizableMachineType(provider, machineType)
}

type resizableVmSizeProvider interface {
	GetMaxResizableVmSizeByMachineType(string) (size.VmSize, error)
}

// GetMaxResizableVmSize returns VM size for known resizable machine types or an error in case of unknown machine type
func GetMaxResizableVmSize(provider resizableVmSizeProvider, node *v1.Node) (size.VmSize, error) {
	// We expect GKE instance type to be GCE machine type
	machineType, exists := GetMachineTypeFromLabels(node.Labels)
	if !exists {
		return size.VmSize{}, fmt.Errorf("node %q does not have instance type label set", node.Name)
	}
	return provider.GetMaxResizableVmSizeByMachineType(machineType)
}

// GetMachineFamilyName returns the machine family name for the given node.
func GetMachineFamilyName(node *v1.Node) (string, error) {
	machineType, exists := GetMachineTypeFromLabels(node.Labels)
	if !exists {
		return "", fmt.Errorf("node %q does not have instance type label set", node.Name)
	}
	return gce.GetMachineFamily(machineType)
}

func GetMachineTypeFromLabels(labels map[string]string) (string, bool) {
	machineType, found := labels[v1.LabelInstanceTypeStable]
	if !found {
		machineType, found = labels[v1.LabelInstanceType]
	}
	return machineType, found
}

func IsPreemptible(node *v1.Node) bool {
	val, ok := node.Labels[gkelabels.SpotLabel]
	if ok && val == gkelabels.PreemptionValue {
		return true
	}
	val, ok = node.Labels[gkelabels.PreemptibleLabel]
	return ok && val == gkelabels.PreemptionValue
}
