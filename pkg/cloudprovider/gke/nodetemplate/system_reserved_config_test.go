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

package nodetemplate

import (
	"strings"
	"testing"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	node_version "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
)

var (
	configForMemoryTest = &fileConfig{
		Memory: []MemConfig{
			{
				OsDistribution: "cos",
				Architecture:   "amd64",
				Confidential:   false,
				PhysicalBytes:  1536 * 1024 * 1024, // 1,5GB
				ReservedBytes:  2,
				NodeVersions:   []string{"1.2.3-gke.4", "2.3.4-gke.5"},
			},
			{
				OsDistribution: "cos",
				Architecture:   "amd64",
				Confidential:   false,
				PhysicalBytes:  3584 * 1024 * 1024, // 3,5GB
				ReservedBytes:  7,
				NodeVersions:   []string{"1.2.3-gke.4", "2.3.4-gke.5"},
			},
			{
				OsDistribution: "cos",
				Architecture:   "amd64",
				Confidential:   false,
				PhysicalBytes:  7 * bytesInGb,
				ReservedBytes:  10,
				NodeVersions:   []string{"1.2.3-gke.4", "2.3.4-gke.5"},
			},
			{
				OsDistribution: "cos",
				Architecture:   "amd64",
				Confidential:   false,
				PhysicalBytes:  1000 * bytesInGb,
				ReservedBytes:  100,
				NodeVersions:   []string{"1.2.3-gke.4", "2.3.4-gke.5"},
			},
			{
				OsDistribution: "ubuntu",
				Architecture:   "amd64",
				Confidential:   false,
				PhysicalBytes:  1000 * bytesInGb,
				ReservedBytes:  9 * bytesInGb,
				NodeVersions:   []string{"2.3.4-gke.5"},
			},
			{
				OsDistribution: "ubuntu",
				Architecture:   "amd64",
				Confidential:   false,
				PhysicalBytes:  1000 * bytesInGb,
				ReservedBytes:  10 * bytesInGb,
				NodeVersions:   []string{"1.2.3-gke.4"},
			},
			{
				OsDistribution: "ubuntu",
				Architecture:   "amd64",
				Confidential:   false,
				PhysicalBytes:  1100 * bytesInGb,
				ReservedBytes:  20 * bytesInGb,
				NodeVersions:   []string{"1.2.3-gke.4"},
			},
			{
				OsDistribution: "ubuntu",
				Architecture:   "amd64",
				Confidential:   false,
				PhysicalBytes:  1200 * bytesInGb,
				ReservedBytes:  40 * bytesInGb,
				NodeVersions:   []string{"1.2.3-gke.4"},
			},
			{
				OsDistribution: "cos",
				Architecture:   "arm64",
				Confidential:   false,
				PhysicalBytes:  1000 * bytesInGb,
				ReservedBytes:  100,
				NodeVersions:   []string{"1.2.3-gke.5"},
			},
			{
				OsDistribution: "cos",
				Architecture:   "amd64",
				Confidential:   true,
				PhysicalBytes:  1000 * bytesInGb,
				ReservedBytes:  100,
				NodeVersions:   []string{"1.2.3-gke.4", "2.3.4-gke.5"},
			},
		},
		Disk: []DiskConfig{
			{
				OsDistribution: "cos",
				Architecture:   "amd64",
				PhysicalBytes:  1000 * bytesInGb,
				ReservedBytes:  4 * bytesInGb,
				NodeVersions:   []string{"1.2.3-gke.4", "2.3.4-gke.5"},
			},
			{
				OsDistribution: "cos",
				Architecture:   "arm64",
				PhysicalBytes:  1000 * bytesInGb,
				ReservedBytes:  1 * bytesInGb,
				NodeVersions:   []string{"1.2.3-gke.5"},
			},
			{
				OsDistribution: "ubuntu",
				Architecture:   "amd64",
				PhysicalBytes:  1000 * bytesInGb,
				ReservedBytes:  4 * bytesInGb,
				NodeVersions:   []string{"1.2.3-gke.4", "2.3.4-gke.5"},
			},
		},
	}

	configForDiskTest = &fileConfig{
		Memory: []MemConfig{
			{
				OsDistribution: "cos",
				Architecture:   "amd64",
				PhysicalBytes:  1000 * bytesInGb,
				ReservedBytes:  10,
				NodeVersions:   []string{"1.2.3-gke.4", "2.3.4-gke.5"},
			},
			{
				OsDistribution: "ubuntu",
				Architecture:   "amd64",
				PhysicalBytes:  1000 * bytesInGb,
				ReservedBytes:  9 * bytesInGb,
				NodeVersions:   []string{"2.3.4-gke.5"},
			},
			{
				OsDistribution: "ubuntu",
				Architecture:   "amd64",
				PhysicalBytes:  1000 * bytesInGb,
				ReservedBytes:  10 * bytesInGb,
				NodeVersions:   []string{"1.2.3-gke.4"},
			},
			{
				OsDistribution: "cos",
				Architecture:   "arm64",
				PhysicalBytes:  1000 * bytesInGb,
				ReservedBytes:  10 * bytesInGb,
				NodeVersions:   []string{"1.2.3-gke.5"},
			},
		},
		Disk: []DiskConfig{
			{
				OsDistribution: "cos",
				Architecture:   "amd64",
				PhysicalBytes:  1000 * bytesInGb,
				ReservedBytes:  1,
				NodeVersions:   []string{"1.2.3-gke.4", "2.3.4-gke.5"},
			},
			{
				OsDistribution: "ubuntu",
				Architecture:   "amd64",
				PhysicalBytes:  1000 * bytesInGb,
				ReservedBytes:  3 * bytesInGb,
				NodeVersions:   []string{"2.3.4-gke.5"},
			},
			{
				OsDistribution: "ubuntu",
				Architecture:   "amd64",
				PhysicalBytes:  1000 * bytesInGb,
				ReservedBytes:  1 * bytesInGb,
				NodeVersions:   []string{"1.2.3-gke.4"},
			},
			{
				OsDistribution: "ubuntu",
				Architecture:   "amd64",
				PhysicalBytes:  1100 * bytesInGb,
				ReservedBytes:  2 * bytesInGb,
				NodeVersions:   []string{"1.2.3-gke.4"},
			},
			{
				OsDistribution: "ubuntu",
				Architecture:   "amd64",
				PhysicalBytes:  1200 * bytesInGb,
				ReservedBytes:  4 * bytesInGb,
				NodeVersions:   []string{"1.2.3-gke.4"},
			},
			{
				OsDistribution: "cos",
				Architecture:   "arm64",
				PhysicalBytes:  1000 * bytesInGb,
				ReservedBytes:  1,
				NodeVersions:   []string{"1.2.3-gke.5"},
			},
		},
	}
)

func TestGetOsReservedMemory(t *testing.T) {
	s, err := parseSystemReservedConfigFile(configForMemoryTest)
	if err != nil {
		t.Errorf("parseSystemReservedConfigFile return err: %v, want nil", err)
		return
	}
	tests := []struct {
		name           string
		nodeVersion    string
		osDistribution string
		architecture   gce.SystemArchitecture
		confidential   bool
		physicalBytes  int
		memReserved    int
		err            string
	}{
		{
			name:           "Test interpolation when one of memory option is fraction of GB",
			nodeVersion:    "1.2.3-gke.4",
			osDistribution: "cos",
			architecture:   gce.Amd64,
			physicalBytes:  5 * bytesInGb,
			memReserved:    8,
		},
		{
			name:           "Test interpolation when initial memory is fraction of GB",
			nodeVersion:    "1.2.3-gke.4",
			osDistribution: "cos",
			architecture:   gce.Amd64,
			physicalBytes:  5632 * 1024 * 1024, //5,5GB
			memReserved:    8,
		},
		{
			name:           "Test interpolation when both of memory options are fraction of GB",
			nodeVersion:    "1.2.3-gke.4",
			osDistribution: "cos",
			architecture:   gce.Amd64,
			physicalBytes:  2 * bytesInGb,
			memReserved:    3,
		},
		{
			name:           "Interpolation, node version in config",
			nodeVersion:    "1.2.3-gke.4",
			osDistribution: "ubuntu",
			architecture:   gce.Amd64,
			physicalBytes:  1050 * bytesInGb,
			memReserved:    15 * bytesInGb,
		},
		{
			name:           "Interpolation, node version isn't in config",
			nodeVersion:    "1.2.3-gke.5",
			osDistribution: "ubuntu",
			architecture:   gce.Amd64,
			physicalBytes:  1150 * bytesInGb,
			memReserved:    30 * bytesInGb,
		},
		{
			name:           "Value from config",
			nodeVersion:    "1.2.3-gke.4",
			osDistribution: "cos",
			architecture:   gce.Amd64,
			physicalBytes:  1000 * bytesInGb,
			memReserved:    100,
		},
		{
			name:           "Extrapolation, node version in config",
			nodeVersion:    "1.2.3-gke.4",
			osDistribution: "cos",
			architecture:   gce.Amd64,
			physicalBytes:  2000 * bytesInGb,
			memReserved:    190,
		},
		{
			name:           "Memory option in config",
			nodeVersion:    "1.2.3-gke.10",
			osDistribution: "cos",
			architecture:   gce.Amd64,
			physicalBytes:  1000 * bytesInGb,
			memReserved:    100,
		},
		{
			name:           "Extrapolation, node version and memory aren't in config",
			nodeVersion:    "1.2.3-gke.10",
			osDistribution: "cos",
			architecture:   gce.Amd64,
			physicalBytes:  2000 * bytesInGb,
			memReserved:    190,
		},
		{
			name:           "Value from config for arm",
			nodeVersion:    "1.2.3-gke.5",
			osDistribution: "cos",
			architecture:   gce.Arm64,
			physicalBytes:  1000 * bytesInGb,
			memReserved:    100,
		},
		{
			name:           "Value from config for confidential node",
			nodeVersion:    "1.2.3-gke.5",
			osDistribution: "cos",
			architecture:   gce.Amd64,
			confidential:   true,
			physicalBytes:  1000 * bytesInGb,
			memReserved:    100,
		},
	}
	for _, test := range tests {
		reserved, err := s.GetOsReservedValue(MemoryResource, test.nodeVersion, gce.OperatingSystemDistribution(test.osDistribution), test.architecture, test.confidential, int64(test.physicalBytes))
		if test.err != "" {
			if ok := strings.Contains(err.Error(), test.err); !ok {
				t.Errorf("%s: GetOsReservedValue for memory resource return error %s; want:%s", test.name, err.Error(), test.err)
			}
		}
		if err != nil {
			t.Errorf("%s: GetOsReservedValue for memory resource return err: %v, want nil", test.name, err)
		}
		if int64(test.memReserved) != reserved {
			t.Errorf("%s: GetOsReservedValue for memory resource return: %d, want: %d", test.name, reserved, test.memReserved)
		}
	}
}

func TestGetOsReservedDisk(t *testing.T) {
	s, err := parseSystemReservedConfigFile(configForDiskTest)
	if err != nil {
		t.Errorf("parseSystemReservedConfigFile return err: %v, want nil", err)
		return
	}
	tests := []struct {
		name           string
		nodeVersion    string
		osDistribution string
		architecture   gce.SystemArchitecture
		physicalBytes  int
		diskReserved   int
		err            string
	}{
		{
			name:           "Interpolation, node version in config",
			nodeVersion:    "1.2.3-gke.4",
			osDistribution: "ubuntu",
			architecture:   gce.Amd64,
			physicalBytes:  1050 * bytesInGb,
			diskReserved:   1536 * 1024 * 1024, //1,5GB
		},
		{
			name:           "Interpolation, node version isn't in config",
			nodeVersion:    "1.2.3-gke.5",
			osDistribution: "ubuntu",
			architecture:   gce.Amd64,
			physicalBytes:  1150 * bytesInGb,
			diskReserved:   3 * bytesInGb,
		},
		{
			name:           "Value from config",
			nodeVersion:    "1.2.3-gke.4",
			osDistribution: "cos",
			architecture:   gce.Amd64,
			physicalBytes:  1000 * bytesInGb,
			diskReserved:   1,
		},
		{
			name:           "Extrapolation, node version in config",
			nodeVersion:    "1.2.3-gke.4",
			osDistribution: "cos",
			architecture:   gce.Amd64,
			physicalBytes:  2000 * bytesInGb,
			diskReserved:   2,
		},
		{
			name:           "Disk option in config",
			nodeVersion:    "1.2.3-gke.10",
			osDistribution: "cos",
			architecture:   gce.Amd64,
			physicalBytes:  1000 * bytesInGb,
			diskReserved:   1,
		},
		{
			name:           "Extrapolation, node version and memory aren't in config",
			nodeVersion:    "1.2.3-gke.10",
			osDistribution: "cos",
			architecture:   gce.Amd64,
			physicalBytes:  2000 * bytesInGb,
			diskReserved:   2,
		},
		{
			name:           "Value from config for arm",
			nodeVersion:    "1.2.3-gke.5",
			osDistribution: "cos",
			architecture:   gce.Arm64,
			physicalBytes:  1000 * bytesInGb,
			diskReserved:   1,
		},
	}
	for _, test := range tests {
		reserved, err := s.GetOsReservedValue(EphemeralStorageResource, test.nodeVersion, gce.OperatingSystemDistribution(test.osDistribution), test.architecture, false, int64(test.physicalBytes))
		if test.err != "" {
			if ok := strings.Contains(err.Error(), test.err); !ok {
				t.Errorf("%s: GetOsReservedValue for ephemeral storage resource return error %s; want:%s", test.name, err.Error(), test.err)
			}
		}
		if err != nil {
			t.Errorf("%s: GetOsReservedValue for ephemeral storage resource return err: %v, want nil", test.name, err)
		}
		if int64(test.diskReserved) != reserved {
			t.Errorf("%s: GetOsReservedValue for ephemeral storage resource return: %d, want: %d", test.name, reserved, test.diskReserved)
		}
	}
}

func TestStringToNodeVersion(t *testing.T) {
	tests := []struct {
		nodeVersion string
		result      node_version.Version
	}{
		{
			nodeVersion: "1.2.3-gke.4",
			result:      node_version.Version{1, 2, 3, 4},
		},
		{
			nodeVersion: "1.2.3.5-gke.4",
			result:      node_version.Version{},
		},
		{
			nodeVersion: "1.2.3",
			result:      node_version.Version{1, 2, 3, 0},
		},
	}
	for _, test := range tests {
		result, _ := node_version.FromString(test.nodeVersion)
		if result != test.result {
			t.Errorf("TestStringToNodeVersion return: %d, want: %d", result, test.result)
		}
	}
}

func TestNoArchitectureSet(t *testing.T) {
	config := &fileConfig{
		Memory: []MemConfig{
			{
				OsDistribution: "cos",
				PhysicalBytes:  1000 * bytesInGb,
				ReservedBytes:  100,
				NodeVersions:   []string{"1.2.3-gke.5"},
			},
		},
		Disk: []DiskConfig{
			{
				OsDistribution: "cos",
				PhysicalBytes:  1000 * bytesInGb,
				ReservedBytes:  1 * bytesInGb,
				NodeVersions:   []string{"1.2.3-gke.5"},
			},
		},
	}
	expectedMemoryReserved := 100
	s, err := parseSystemReservedConfigFile(config)
	if err != nil {
		t.Errorf("parseSystemReservedConfigFile return err: %v, want nil", err)
		return
	}
	reserved, err := s.GetOsReservedValue(MemoryResource, "1.2.3-gke.5", gce.OperatingSystemDistributionCOS, gce.Amd64, false, 1000*bytesInGb)
	if err != nil {
		t.Errorf("testNoArchitectureSet: GetOsReservedValue for memory resource return err: %v, want nil", err)
	}
	if int64(expectedMemoryReserved) != reserved {
		t.Errorf("TestNoArchitectureSet: GetOsReservedValue for memory resource return: %d, want: %d", reserved, expectedMemoryReserved)
	}
}

func TestInvalidConfig(t *testing.T) {
	empty := &fileConfig{
		Memory: []MemConfig{},
		Disk:   []DiskConfig{},
	}
	missingNodeVersions1 := &fileConfig{
		Memory: []MemConfig{
			{
				OsDistribution: "cos",
				PhysicalBytes:  1000 * bytesInGb,
				ReservedBytes:  10,
				NodeVersions:   []string{"1.2.3-gke.4", "2.3.4-gke.5"},
			},
		},
		Disk: []DiskConfig{
			{
				OsDistribution: "ubuntu",
				PhysicalBytes:  1000 * bytesInGb,
				ReservedBytes:  1,
				NodeVersions:   []string{"2.3.4-gke.5"},
			},
		},
	}
	missingNodeVersions2 := &fileConfig{
		Memory: []MemConfig{
			{
				OsDistribution: "cos",
				PhysicalBytes:  1000 * bytesInGb,
				ReservedBytes:  10,
				NodeVersions:   []string{"2.3.4-gke.5"},
			},
		},
		Disk: []DiskConfig{
			{
				OsDistribution: "ubuntu",
				PhysicalBytes:  1000 * bytesInGb,
				ReservedBytes:  1,
				NodeVersions:   []string{"1.2.3-gke.4", "2.3.4-gke.5"},
			},
		},
	}
	for _, config := range []*fileConfig{empty, missingNodeVersions1, missingNodeVersions2} {
		_, err := parseSystemReservedConfigFile(config)
		if err == nil {
			t.Error("parseSystemReservedConfigFile return nil err")
		}
	}
}
