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
	"fmt"
	"sort"

	"gopkg.in/yaml.v2"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	node_version "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
)

// ResourceType defines the resources supported by OS reserved
type ResourceType string

const (
	// MemoryResource denotes memory resource.
	MemoryResource ResourceType = "memory"
	// EphemeralStorageResource denotes ephemeral storage resource.
	EphemeralStorageResource ResourceType = "ephemeral-storage"
)

const bytesInGb = 1024 * 1024 * 1024

type bytesPair struct {
	physicalBytes int64
	reservedBytes int64
}

// SystemReserved is stores the reserved values map for all configurations
type SystemReserved struct {
	Reservation map[reservationCriteria]*systemReservedConfig
}

// NewSystemReserved returns a new instance of SystemReserved
func NewSystemReserved() *SystemReserved {
	return &SystemReserved{
		Reservation: make(map[reservationCriteria]*systemReservedConfig),
	}
}

// reservationCriteria identifies the unique set for reservation configurations
type reservationCriteria struct {
	OsDistribution     gce.OperatingSystemDistribution
	SystemArchitecture gce.SystemArchitecture
	Confidential       bool
}

// newReservationCriteria returns a new instance of reservationCriteria
func newReservationCriteria(osDistribution gce.OperatingSystemDistribution, architecture gce.SystemArchitecture, confidential bool) reservationCriteria {
	return reservationCriteria{
		OsDistribution:     osDistribution,
		SystemArchitecture: architecture,
		Confidential:       confidential,
	}
}

// systemReservedConfig stores the value reserved by OS for a single reservationCriteria.
type systemReservedConfig struct {
	nodeVersions []node_version.Version
	// NodeVersion: [(physical memory size: reserved size)]
	memoryReserved map[node_version.Version][]bytesPair
	diskReserved   map[node_version.Version][]bytesPair
}

// newSystemReservedConfig returns empty systemReservedConfig.
func newSystemReservedConfig() *systemReservedConfig {
	memoryData := make(map[node_version.Version][]bytesPair)
	diskData := make(map[node_version.Version][]bytesPair)
	return &systemReservedConfig{
		nodeVersions:   []node_version.Version{},
		memoryReserved: memoryData,
		diskReserved:   diskData,
	}
}

// GetOsReservedValue returns a reserved value for an OS resource
func (s *SystemReserved) GetOsReservedValue(resource ResourceType, nodeVersion string, osDistribution gce.OperatingSystemDistribution, architecture gce.SystemArchitecture, confidential bool, value int64) (int64, error) {
	key := newReservationCriteria(osDistribution, architecture, confidential)
	reserved, ok := s.Reservation[key]
	if !ok {
		return 0, fmt.Errorf("don't have data in os reserved for key: %+v", key)
	}

	return reserved.getOsReservedValue(resource, nodeVersion, value)
}

func (t *systemReservedConfig) extractReservedValues(resource ResourceType) (map[node_version.Version][]bytesPair, error) {
	if resource == MemoryResource {
		return t.memoryReserved, nil
	} else if resource == EphemeralStorageResource {
		return t.diskReserved, nil
	}

	return nil, fmt.Errorf("unknown resource: %s", resource)

}

// getOsReservedValue return the value reserved by OS.
func (t *systemReservedConfig) getOsReservedValue(resource ResourceType, nodeVersion string, physicalSize int64) (int64, error) {
	val, err := t.extractReservedValues(resource)
	if err != nil {
		return int64(0), fmt.Errorf("couldn't extract systemReservedConfig data structure, err: %v", err)
	}

	nv, err := node_version.FromString(nodeVersion)
	if err != nil {
		return int64(0), fmt.Errorf("couldn't parse node version %s, err: %v", nodeVersion, err)
	}
	version := t.nodeVersions[t.findNodeVersionIdx(nv)]
	options, err := findOptions(val[version], physicalSize)
	if err != nil {
		return 0, fmt.Errorf("failed to find memory option, err: %v", err)
	}
	if len(options) == 1 {
		return options[0].reservedBytes, nil
	}
	if len(options) > 2 {
		return int64(0), fmt.Errorf("more than 2 values passed to interpolation function: %+v for node version: %s", options, nodeVersion)
	}
	return interpolate(physicalSize, options), nil
}

// Return a node version that will be used in calculation for os reserved.
// Use older verions if possible and the current is not present in the file.
func (t *systemReservedConfig) findNodeVersionIdx(nv node_version.Version) int {
	nidx := sort.Search(len(t.nodeVersions), func(i int) bool { return nv.LessThan(t.nodeVersions[i]) })
	if nidx == len(t.nodeVersions) {
		return nidx - 1
	}
	if t.nodeVersions[nidx] == nv || nidx == 0 {
		return nidx
	}
	return nidx - 1
}

func findOptions(pairs []bytesPair, value int64) ([]bytesPair, error) {
	midx := sort.Search(len(pairs), func(i int) bool { return pairs[i].physicalBytes >= value })
	if midx == len(pairs) {
		return []bytesPair{pairs[midx-2], pairs[midx-1]}, nil
	}
	if pairs[midx].physicalBytes == value && midx > 0 {
		return []bytesPair{pairs[midx]}, nil
	}
	if midx == 0 {
		return []bytesPair{}, fmt.Errorf("can't find options for value %d", value)
	}
	return []bytesPair{pairs[midx-1], pairs[midx]}, nil
}

func (t *systemReservedConfig) parseMemory(mConfig MemConfig) error {
	memValue := bytesPair{physicalBytes: mConfig.PhysicalBytes, reservedBytes: mConfig.ReservedBytes}

	for _, version := range mConfig.NodeVersions {
		v, err := node_version.FromString(version)
		if err != nil {
			return err
		}

		t.memoryReserved[v] = append(t.memoryReserved[v], memValue)
	}

	return nil
}

func (t *systemReservedConfig) parseDisk(dConfig DiskConfig) error {
	diskValue := bytesPair{physicalBytes: dConfig.PhysicalBytes, reservedBytes: dConfig.ReservedBytes}

	for _, version := range dConfig.NodeVersions {
		v, err := node_version.FromString(version)
		if err != nil {
			return err
		}
		t.diskReserved[v] = append(t.diskReserved[v], diskValue)
	}

	return nil
}

func (t *systemReservedConfig) initializeAndVerify() error {
	memoryNodeVersions := make(map[node_version.Version]bool)

	// Add bytesPair (0,0) for all nodeVersions and sort the memory pairs
	for version := range t.memoryReserved {
		t.memoryReserved[version] = append(t.memoryReserved[version], bytesPair{physicalBytes: int64(0), reservedBytes: int64(0)})
		memValues := t.memoryReserved[version]
		memoryNodeVersions[version] = true
		sort.Slice(memValues, func(x, y int) bool { return memValues[x].physicalBytes < memValues[y].physicalBytes })
	}

	// For confidential nodes there is no diskReserved value.
	if len(t.diskReserved) == 0 {
		for v := range memoryNodeVersions {
			t.nodeVersions = append(t.nodeVersions, v)
		}
		return nil
	}

	diskNodeVersions := make(map[node_version.Version]bool)
	// Add bytesPair (0,0) for all nodeVersions and sort the memory pairs
	for version := range t.diskReserved {
		t.diskReserved[version] = append(t.diskReserved[version], bytesPair{physicalBytes: int64(0), reservedBytes: int64(0)})
		diskValues := t.diskReserved[version]
		diskNodeVersions[version] = true
		sort.Slice(diskValues, func(x, y int) bool { return diskValues[x].physicalBytes < diskValues[y].physicalBytes })
	}

	nodeVersionsDiff := map[string][]string{
		"memory": {},
		"disk":   {},
	}
	for v := range memoryNodeVersions {
		if _, found := t.diskReserved[v]; found {
			t.nodeVersions = append(t.nodeVersions, v)
			delete(diskNodeVersions, v)
		} else {
			nodeVersionsDiff["memory"] = append(nodeVersionsDiff["memory"], v.String())
		}
	}
	for nv := range diskNodeVersions {
		nodeVersionsDiff["disk"] = append(nodeVersionsDiff["disk"], nv.String())
	}
	if len(nodeVersionsDiff["memory"])+len(nodeVersionsDiff["disk"]) > 0 {
		return fmt.Errorf("versions for memory and disk configs are not consistent, diff: %v", nodeVersionsDiff)
	}
	sort.Slice(t.nodeVersions, func(x, y int) bool { return t.nodeVersions[x].LessThan(t.nodeVersions[y]) })

	return nil
}

func getArch(arch string) gce.SystemArchitecture {
	if arch == "" {
		return gce.DefaultArch
	}
	return gce.ToSystemArchitecture(arch)
}

func interpolate(mem int64, memOptions []bytesPair) int64 {
	// Since 1GB=2^30bytes, using numbers in bytes for multiplication results in int64 overflow for some cases
	// (i.e mem=60GB, memOption=(32GB,64GB)). In majority cases the memory value in GB is integer,
	// but custom machine types may use fraction of GB (i.e. 2,5 GB).
	// To deal with int64 overflow issue the physicalBytes value is converted to float value to use in calculation.

	// Alternatives considered: Convert physicalBytes value to value in MB. This approach was rejected because this function is used
	// for both memory reservation and disk reservation. Currently possible max disk size is 65536GB which may lead to int64 overflow
	// if this value is used in MB for interpolation, and also if new machine types with bigger machine size is introduced it's possible
	// to hit int64 overflow if using value in MB.
	x1, x2 := memOptions[0].physicalBytes, memOptions[1].physicalBytes
	y1, y2 := memOptions[0].reservedBytes, memOptions[1].reservedBytes
	return y1 + int64((float64(mem-x1)/float64(x2-x1))*float64(y2-y1))
}

// BuildSystemReservedConfig parses the os reserved config into a structure.
func BuildSystemReservedConfig(in []byte) (*SystemReserved, error) {
	file := &fileConfig{}
	err := yaml.Unmarshal(in, file)
	if err != nil {
		return nil, err
	}
	return parseSystemReservedConfigFile(file)
}

func parseSystemReservedConfigFile(file *fileConfig) (*SystemReserved, error) {
	systemReserved := NewSystemReserved()

	// initialise system reserved with all possible keys
	for _, f := range file.Memory {
		key := newReservationCriteria(gce.OperatingSystemDistribution(f.OsDistribution), getArch(f.Architecture), f.Confidential)
		if _, found := systemReserved.Reservation[key]; !found {
			systemReserved.Reservation[key] = newSystemReservedConfig()
		}
		err := systemReserved.Reservation[key].parseMemory(f)
		if err != nil {
			return nil, fmt.Errorf("memory parsing failed for key: %+v - %v", key, err)
		}
	}
	for _, f := range file.Disk {
		key := newReservationCriteria(gce.OperatingSystemDistribution(f.OsDistribution), getArch(f.Architecture), false)
		if _, found := systemReserved.Reservation[key]; !found {
			systemReserved.Reservation[key] = newSystemReservedConfig()
		}
		err := systemReserved.Reservation[key].parseDisk(f)
		if err != nil {
			return nil, fmt.Errorf("disk parsing failed for key: %+v - %v", key, err)
		}
	}

	if len(systemReserved.Reservation) == 0 {
		return nil, fmt.Errorf("no os reserved entries were parsed from os reserved config")
	}

	for k, config := range systemReserved.Reservation {
		err := config.initializeAndVerify()
		if err != nil {
			return nil, fmt.Errorf("verification failed for systemConfig, arch: %q, osDistribution: %q, confidential: %v : %v", k.SystemArchitecture, k.OsDistribution, k.Confidential, err)
		}
	}

	return systemReserved, nil
}

// fileConfig represents the config layout that is going to be used for storing/loading from file.
type fileConfig struct {
	Memory []MemConfig  `yaml:"memory"`
	Disk   []DiskConfig `yaml:"disk"`
}

// DiskConfig represents all the dimensions that impact OS reserved value for disk.
// This struct is only public because it couldn't be marshalled to yaml otherwise.
type DiskConfig struct {
	OsDistribution string   `yaml:"osDistribution"`
	Architecture   string   `yaml:"architecture"`
	PhysicalBytes  int64    `yaml:"physicalBytes"`
	ReservedBytes  int64    `yaml:"reservedBytes"`
	NodeVersions   []string `yaml:"nodeVersions"`
}

// MemConfig represents all the dimensions that impact OS reserved value for memory.
// This struct is only public because it couldn't be marshalled to yaml otherwise.
type MemConfig struct {
	OsDistribution string   `yaml:"osDistribution"`
	Architecture   string   `yaml:"architecture"`
	Confidential   bool     `yaml:"confidential"`
	PhysicalBytes  int64    `yaml:"physicalBytes"`
	ReservedBytes  int64    `yaml:"reservedBytes"`
	NodeVersions   []string `yaml:"nodeVersions"`
}
