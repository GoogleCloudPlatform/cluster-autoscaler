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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// *DO NOT RUN THESE TEST IN PARALLEL*
// Since we use package level variable computeClassesByName

func TestValidateComputeClassConfig(t *testing.T) {
	computeClasses := AllComputeClasses()
	assert.NotEmpty(t, computeClasses)

	for _, computeClass := range computeClasses {
		t.Run(computeClass.name, func(t *testing.T) {
			// Check if the class has at least one machine family.
			assert.NotEmpty(t, computeClass.machineFamilies)
			// Check if all the machine families are registered.
			mcp := NewMachineConfigProvider(nil)
			for _, machineFamily := range computeClass.machineFamilies {
				_, err := mcp.ToMachineFamily(machineFamily.name)
				assert.NoError(t, err)
			}
			// Check if the compute class name can be used to convert back to the object.
			_, err := ToPredefinedComputeClass(computeClass.name)
			assert.NoError(t, err)
			// Different case family names should not work.
			_, err = ToPredefinedComputeClass(strings.ToUpper(computeClass.name))
			assert.Error(t, err)
		})
	}
}

func TestValidateMachineFamilyExistsInSingleComputeClass(t *testing.T) {

	// assert the configuration is set up that each machine family is only set in a single compute class
	for _, c := range AllComputeClasses() {
		if c.sliceOfHardware {
			// machine family can also be in slice of hardware compute class.
			continue
		}
		for _, family := range c.MachineFamilies() {
			var classCount = 0
			for _, cx := range AllComputeClasses() {
				if cx.sliceOfHardware {
					// machine family can also be in slice of hardware compute class.
					continue
				}
				if family.In(cx.MachineFamilies()...) {
					classCount++
				}
			}
			assert.Equal(t, classCount, 1)
		}
	}
}

func TestValidateCanonicalFamilyWithComputeClassFetched(t *testing.T) {

	// assert the configuration of canonical family being same for each class
	// when class is fetched by machine family
	for _, c := range AllComputeClasses() {
		if c.sliceOfHardware {
			continue
		}
		canonicalFamily := c.CanonicalFamily()
		for _, family := range c.MachineFamilies() {
			class, found := ComputeClassForMachineFamily(family)
			assert.True(t, found)
			assert.Equal(t, class.CanonicalFamily(), canonicalFamily)
		}
	}
}
