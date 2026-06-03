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
	"regexp"
	"strconv"
	"strings"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/klog/v2"
)

var machineGenerationRegex = regexp.MustCompile("^[a-zA-Z]+([0-9]+)[a-zA-Z]*$")

// GetMachineGeneration extracts the generation number from a machine type string.
// For example, "c3-standard-4" returns 3, "n2-standard-4" returns 2.
func GetMachineGeneration(machineType string) int {
	// extracting machine family from full name, e.g., c3 from c3-standard-4
	machineFamily, _, _ := strings.Cut(machineType, "-")
	match := machineGenerationRegex.FindStringSubmatch(machineFamily)
	if len(match) < 2 {
		klog.Errorf("unable to extract generation from machine type '%v', got matches:%v", machineFamily, match)
		return 0
	}
	gen, err := strconv.Atoi(match[1])
	if err != nil {
		klog.Errorf("received error when converting machine type substring '%v' to machine generation: %v", match[1], err)
		return 0
	}
	return gen
}

// IsBareMetal returns whether given machine type is bare metal
func IsBareMetal(machineType string) bool {
	return strings.HasSuffix(machineType, "-metal")
}

// ToCustomMachineType attempts to construct a custom machine type from name.
func ToCustomMachineType(machineTypeName string) (MachineType, error) {
	if gce.IsCustomMachine(machineTypeName) {
		machineType, err := gce.NewCustomMachineType(machineTypeName)
		return MachineType{MachineType: machineType}, err
	}
	return MachineType{}, fmt.Errorf("unsupported machine type %q", machineTypeName)
}
