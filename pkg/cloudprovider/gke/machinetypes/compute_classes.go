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
)

// PredefinedComputeClass contains information about a compute class.
type PredefinedComputeClass struct {
	name string
	// machineFamilies contains the machine families present in this class.
	// the expectation is that a machineFamily will only belong to one compute class
	machineFamilies []MachineFamily
	// machineFamilyBalancingEnabled denotes whether actions should be taken to
	// balance preferences between machine families
	machineFamilyBalancingEnabled bool
	// sliceOfHardware dictates if the workload expects a whole node to be provisioned
	sliceOfHardware bool
	// acceleratorClass dictates if specifying accelerator is required for this compute class.
	acceleratorClass bool
	// napLargerBootDisk determines whether NAP configures a larger boot disk for the node pools with this CC.
	napLargerBootDisk bool
}

// Name returns a name of compute class
func (c PredefinedComputeClass) Name() string {
	return c.name
}

// MachineFamilies returns machine families in this compute class
func (c PredefinedComputeClass) MachineFamilies() []MachineFamily {
	return c.machineFamilies
}

// IsFamilyBalancingEnabled returns the flag for machine family balancing
func (c PredefinedComputeClass) IsFamilyBalancingEnabled() bool {
	return c.machineFamilyBalancingEnabled
}

// IsSliceOfHardware returns the flag for slice of hardware compute class.
func (c PredefinedComputeClass) IsSliceOfHardware() bool {
	return c.sliceOfHardware
}

// IsAcceleratorClass returns the flag for accelerator compute class.
func (c PredefinedComputeClass) IsAcceleratorClass() bool {
	return c.acceleratorClass
}

// CanonicalFamily return a single canonical / equivalent family that should be
// used for compute class. This is used for cases where all machine families in
// the compute class should be treated with same machine family properties
func (c PredefinedComputeClass) CanonicalFamily() MachineFamily {
	return c.machineFamilies[0]
}

// NapLargerBootDisk returns whether NAP should configure a larger boot disk for the node pools with this CC.
func (c PredefinedComputeClass) NapLargerBootDisk() bool {
	return c.napLargerBootDisk
}

var (
	// ScaleOutClass represents a compute class containing TX machines
	ScaleOutClass = RegisterComputeClass(
		PredefinedComputeClass{
			name:                          "Scale-Out",
			machineFamilies:               []MachineFamily{T2A, T2D},
			machineFamilyBalancingEnabled: false,
		})
	// BalancedClass represents a compute class containing NX machines
	BalancedClass = RegisterComputeClass(
		PredefinedComputeClass{
			name:                          "Balanced",
			machineFamilies:               []MachineFamily{N2, N2D},
			machineFamilyBalancingEnabled: true,
		})

	// PerformanceClass represents a compute class for slice of hardware provisioning
	PerformanceClass = RegisterComputeClass(
		PredefinedComputeClass{
			name:                          "Performance",
			machineFamilies:               []MachineFamily{C4, C4A, C4D, C3, C3D, C2, C2D, H3, H4D, E2, M4, N1, N2, N2D, N4, N4A, N4D, T2D, T2A, Z3, Z4D},
			machineFamilyBalancingEnabled: false,
			sliceOfHardware:               true,
			napLargerBootDisk:             true,
		})

	// AcceleratorClass represents a compute class for gpus, is a noop for GPUs.
	AcceleratorClass = RegisterComputeClass(
		PredefinedComputeClass{
			name:                          "Accelerator",
			machineFamilies:               []MachineFamily{N1, A2, G2, A3, CT4L, CT4P, CT5L, CT5LP, A4, A4X, G4},
			machineFamilyBalancingEnabled: false,
			acceleratorClass:              true,
			napLargerBootDisk:             true,
		})
)

// computeClassesByName is auto-populated by NewComputeClass.
var computeClassesByName = map[string]PredefinedComputeClass{}

// RegisterComputeClass creates a ComputeClass object and registers it.
func RegisterComputeClass(class PredefinedComputeClass) PredefinedComputeClass {
	computeClassesByName[class.Name()] = class
	return class
}

// AllComputeClasses returns all compute classes.
func AllComputeClasses() []PredefinedComputeClass {
	var classes []PredefinedComputeClass
	for _, computeClass := range computeClassesByName {
		classes = append(classes, computeClass)
	}
	return classes
}

// ToPredefinedComputeClass converts a compute class string into a ComputeClass object.
func ToPredefinedComputeClass(computeClassName string) (PredefinedComputeClass, error) {
	computeClass, found := computeClassesByName[computeClassName]
	if !found {
		return PredefinedComputeClass{}, fmt.Errorf("unsupported compute class %q", computeClassName)
	}
	return computeClass, nil
}

// IsPredefinedComputeClass determines whether compute class with such name is a system default.
func IsPredefinedComputeClass(computeClassName string) bool {
	_, found := computeClassesByName[computeClassName]
	return found
}

// IsCustomComputeClass determines whether compute class is user defined.
func IsCustomComputeClass(computeClassName string) bool {
	return !IsPredefinedComputeClass(computeClassName)
}

// ComputeClassForMachineFamily return compute class for a machine family if it exists
func ComputeClassForMachineFamily(family MachineFamily) (PredefinedComputeClass, bool) {
	// This function is used by GKE pricing info to balance scale-ups between
	// all machine families within a compute class.
	// We can exclude slice of hardware compute-classes from this because
	// slice of hardware workloads must specify machine family and no balancing
	// is required to be done.
	// It is ok if the pricing information is different for machine family which
	// appears in both normal compute class and slice of hardware compute class
	// because of slice of hardware workloads must specify machine family.
	for _, class := range AllComputeClasses() {
		if class.sliceOfHardware {
			continue
		}
		if family.In(class.MachineFamilies()...) {
			return class, true
		}
	}
	return PredefinedComputeClass{}, false
}
