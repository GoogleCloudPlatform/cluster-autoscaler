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
	"errors"
	"fmt"
)

// machineFamiliesByName is auto-populated by RegisterMachineFamily.
var machineFamiliesByName map[string]MachineFamily

// allGpusByName - auto-populated - list of all gpus by name set by RegisterGpus
var allGpusByName map[string]Gpu

// RegisterMachineFamily registers a MachineFamily object as supported by NAP.
func RegisterMachineFamily(family MachineFamily) MachineFamily {
	if machineFamiliesByName == nil {
		machineFamiliesByName = map[string]MachineFamily{}
	}
	// This is an ugly hack to allow MachineType's methods to access default values used by the family.
	family = backfillMachineFamilyInMachineTypes(family)
	family.precomputeAllMachineTypes()
	machineFamiliesByName[family.name] = family
	return family
}

func backfillMachineFamilyInMachineTypes(family MachineFamily) MachineFamily {
	for name, mt := range family.autoprovisionedMachineTypes {
		mt.family = &family
		family.autoprovisionedMachineTypes[name] = mt
	}
	for name, mt := range family.otherMachineTypes {
		mt.family = &family
		family.otherMachineTypes[name] = mt
	}
	return family
}

// RegisterGpu registers a Gpu object
func RegisterGpu(gpu Gpu) Gpu {
	if allGpusByName == nil {
		allGpusByName = map[string]Gpu{}
	}
	allGpusByName[gpu.name] = gpu
	return gpu
}

// ApplyMaxCompactPlacementNodesUpdates sets whether a given machine family supports Compact Placement
// along with its maximum nodes capacity value, based on an input map.
func ApplyMaxCompactPlacementNodesUpdates(maxNodeMap map[string]int64) error {
	for name, mf := range machineFamiliesByName {
		maxNodes, ok := maxNodeMap[name]
		if ok {
			mf.supportCompactPlacement = true
			mf.maxCompactPlacementNodes = maxNodes
		} else {
			mf.supportCompactPlacement = false
		}
		machineFamiliesByName[name] = mf
	}
	return validateMachineNamesCompactPlacementValues(maxNodeMap)
}

func validateMachineNamesCompactPlacementValues(maxNodeMap map[string]int64) error {
	var errs []error
	for name := range maxNodeMap {
		if _, ok := machineFamiliesByName[name]; !ok {
			errs = append(errs, fmt.Errorf("no such machine family exists, ignoring: %v", name))
		}
	}
	return errors.Join(errs...)
}
