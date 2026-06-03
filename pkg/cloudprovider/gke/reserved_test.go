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

package gke

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
)

func PredictKubeReservedMemoryHelper(physicalCpuMillicores int64, _ string, gcfsEnabled bool, _ int64) int64 {
	return PredictKubeReservedMemory(physicalCpuMillicores, gcfsEnabled)
}

func PredictKubeReservedCpuMillicoresHelper(physicalCpuMillicores int64, machineType string, _ bool, maxPodsPerNode int64) int64 {
	return PredictKubeReservedCpuMillicores(physicalCpuMillicores, machineType, maxPodsPerNode)
}

func TestPredictKubeReserved(t *testing.T) {
	type testCase struct {
		name             string
		function         func(capacity int64, machineType string, gcfsEnabled bool, maxPodsPerNode int64) int64
		capacity         int64
		machineType      string
		gcfsEnabled      bool
		maxPodsPerNode   int64
		expectedReserved int64
	}
	testCases := []testCase{
		{
			name:             "zero memory capacity",
			function:         PredictKubeReservedMemoryHelper,
			capacity:         0,
			expectedReserved: 0,
		},
		{
			name:             "f1-micro",
			function:         PredictKubeReservedMemoryHelper,
			capacity:         600 * MiB,
			expectedReserved: 255 * MiB,
		},
		{
			name:             "f1-micro with GCFS enabled",
			function:         PredictKubeReservedMemoryHelper,
			capacity:         600 * MiB,
			gcfsEnabled:      true,
			expectedReserved: 255 * MiB,
		},
		{
			name:             "between memory thresholds",
			function:         PredictKubeReservedMemoryHelper,
			capacity:         2000 * MiB,
			expectedReserved: 500 * MiB,
		},
		{
			name:             "between memory thresholds with GCFS enabled",
			function:         PredictKubeReservedMemoryHelper,
			capacity:         2000 * MiB,
			gcfsEnabled:      true,
			expectedReserved: 520 * MiB,
		},
		{
			name:             "at a memory threshold boundary",
			function:         PredictKubeReservedMemoryHelper,
			capacity:         8000 * MiB,
			expectedReserved: 1800 * MiB,
		},
		{
			name:             "at a memory threshold boundary with GCFS enabled",
			function:         PredictKubeReservedMemoryHelper,
			capacity:         8000 * MiB,
			gcfsEnabled:      true,
			expectedReserved: 1872 * MiB,
		},
		{
			name:             "exceeds highest memory threshold",
			function:         PredictKubeReservedMemoryHelper,
			capacity:         200 * 1000 * MiB,
			expectedReserved: 10760 * MiB,
		},
		{
			name:             "exceeds highest memory threshold with GCFS enabled",
			function:         PredictKubeReservedMemoryHelper,
			capacity:         200 * 1000 * MiB,
			gcfsEnabled:      true,
			expectedReserved: 11190 * MiB,
		},
		{
			name:             "cpu sanity check",
			function:         PredictKubeReservedCpuMillicoresHelper, /* change */
			capacity:         4000,
			expectedReserved: 80,
		},
		{
			name:             "cpu with higher max pods per node",
			function:         PredictKubeReservedCpuMillicoresHelper, /* change */
			capacity:         8000,
			maxPodsPerNode:   256,
			expectedReserved: 490,
		},
		{
			name:             "cpu with lower max pods per node",
			function:         PredictKubeReservedCpuMillicoresHelper, /* change */
			capacity:         8000,
			maxPodsPerNode:   32,
			expectedReserved: 90,
		},
	}
	for _, tc := range testCases {
		if actualReserved := tc.function(tc.capacity, tc.machineType, tc.gcfsEnabled, tc.maxPodsPerNode); actualReserved != tc.expectedReserved {
			t.Errorf("Test case: %s, Got f(%d) = %d.  Want %d", tc.name, tc.capacity, actualReserved, tc.expectedReserved)
		}
	}
}

func memoryReservedMiBHelper(memoryCapacityMiB int64, _ string, gcfsEnabled bool, maxPodsPerNode int64) int64 {
	return memoryReservedMiB(memoryCapacityMiB, gcfsEnabled)
}

func cpuReservedMillicoresHelper(memoryCapacityMiB int64, machineType string, _ bool, maxPodsPerNode int64) int64 {
	return cpuReservedMillicores(memoryCapacityMiB, machineType, maxPodsPerNode)
}

func TestCalculateReserved(t *testing.T) {
	type testCase struct {
		name             string
		function         func(capacity int64, machineType string, gcfsEnabled bool, maxPodsPerNode int64) int64
		capacity         int64
		machineType      string
		gcfsEnabled      bool
		expectedReserved int64
		maxPodsPerNode   int64
	}
	testCases := []testCase{
		{
			name:             "zero memory capacity",
			function:         memoryReservedMiBHelper,
			capacity:         0,
			expectedReserved: 0,
		},
		{
			name:             "f1-micro",
			function:         memoryReservedMiBHelper,
			capacity:         600,
			expectedReserved: 255,
		},
		{
			name:             "f1-micro with GCFS enabled",
			function:         memoryReservedMiBHelper,
			capacity:         600,
			gcfsEnabled:      true,
			expectedReserved: 255,
		},
		{
			name:             "between memory thresholds",
			function:         memoryReservedMiBHelper,
			capacity:         2000,
			expectedReserved: 500,
		},
		{
			name:             "between memory thresholds with GCFS enabled",
			function:         memoryReservedMiBHelper,
			capacity:         2000,
			gcfsEnabled:      true,
			expectedReserved: 520,
		},
		{
			name:             "at a memory threshold boundary",
			function:         memoryReservedMiBHelper,
			capacity:         8000,
			expectedReserved: 1800,
		},
		{
			name:             "at a memory threshold boundary with GCFS enabled",
			function:         memoryReservedMiBHelper,
			capacity:         8000,
			gcfsEnabled:      true,
			expectedReserved: 1872,
		},
		{
			name:             "exceeds highest memory threshold",
			function:         memoryReservedMiBHelper,
			capacity:         200 * 1000,
			expectedReserved: 10760,
		},
		{
			name:             "exceeds highest memory threshold with GCFS enabled",
			function:         memoryReservedMiBHelper,
			capacity:         200 * 1000,
			gcfsEnabled:      true,
			expectedReserved: 11190,
		},
		{
			name:             "cpu sanity check",
			function:         cpuReservedMillicoresHelper,
			capacity:         4 * millicoresPerCore,
			expectedReserved: 80,
		},
		{
			name:             "cpu with high max pods per node",
			function:         cpuReservedMillicoresHelper,
			capacity:         4 * millicoresPerCore,
			expectedReserved: 480,
			maxPodsPerNode:   256,
		},
		{
			name:             "cpu with high max pods per node",
			function:         cpuReservedMillicoresHelper,
			capacity:         4 * millicoresPerCore,
			expectedReserved: 80,
			maxPodsPerNode:   32,
		},
		{
			name:             "linux: e2-micro",
			function:         cpuReservedMillicoresHelper,
			capacity:         2 * millicoresPerCore,
			machineType:      "e2-micro",
			expectedReserved: 1060, // 1000 + .06 * 1000
		},
		{
			name:             "linux: e2-small",
			function:         cpuReservedMillicoresHelper,
			capacity:         2 * millicoresPerCore,
			machineType:      "e2-small",
			expectedReserved: 1060, // 1000 + .06 * 1000
		},
		{
			name:             "linux: e2-medium",
			function:         cpuReservedMillicoresHelper,
			capacity:         2 * millicoresPerCore,
			machineType:      "e2-medium",
			expectedReserved: 1060, // 1000 + .06 * 1000
		},
	}
	for _, tc := range testCases {
		if actualReserved := tc.function(tc.capacity, tc.machineType, tc.gcfsEnabled, tc.maxPodsPerNode); actualReserved != tc.expectedReserved {
			t.Errorf("Test case: %s, Got f(%d Mb) = %d.  Want %d", tc.name, tc.capacity, actualReserved, tc.expectedReserved)
		}
	}
}

func TestGceReservedOSPartitionPrediction(t *testing.T) {
	r := &GkeReserved{}
	type testCase struct {
		name           string
		function       func(gce.MigOsInfo, int64) int64
		osDistribution gce.OperatingSystemDistribution
		diskSize       int64
		expectedSize   int64
	}
	testCases := []testCase{
		{
			name:           "cos: 15Gi",
			function:       r.CalculateOSReservedEphemeralStorage,
			osDistribution: gce.OperatingSystemDistributionCOS,
			diskSize:       15 * gce.GiB,
			expectedSize:   4721806516, // ~4.7Gi, matches TestCalculatePhysicalEphemeralStorage values
		},
		{
			name:           "cos: 90Gi",
			function:       r.CalculateOSReservedEphemeralStorage,
			osDistribution: gce.OperatingSystemDistributionCOS,
			diskSize:       90 * gce.GiB,
			expectedSize:   6061433659,
		},
		{
			name:           "cos: 200Gi",
			function:       r.CalculateOSReservedEphemeralStorage,
			osDistribution: gce.OperatingSystemDistributionCOS,
			diskSize:       200 * gce.GiB,
			expectedSize:   7916329370,
		},
		{
			name:           "ubuntu: 90Gi",
			function:       r.CalculateOSReservedEphemeralStorage,
			osDistribution: gce.OperatingSystemDistributionUbuntu,
			diskSize:       90 * gce.GiB,
			expectedSize:   3259558057,
		},
		{
			name:           "ubuntu: 200Gi",
			function:       r.CalculateOSReservedEphemeralStorage,
			osDistribution: gce.OperatingSystemDistributionUbuntu,
			diskSize:       200 * gce.GiB,
			expectedSize:   6909159539,
		},
		{
			name:           "windows-ltsc: 100Gi",
			function:       r.CalculateOSReservedEphemeralStorage,
			osDistribution: gce.OperatingSystemDistributionWindowsLTSC,
			diskSize:       100 * gce.GiB,
			expectedSize:   132392368,
		},
		{
			name:           "windows-sac: 100Gi",
			function:       r.CalculateOSReservedEphemeralStorage,
			osDistribution: gce.OperatingSystemDistributionWindowsSAC,
			diskSize:       100 * gce.GiB,
			expectedSize:   132392368,
		},
		{
			name:           "unknown: 100Gi",
			function:       r.CalculateOSReservedEphemeralStorage,
			osDistribution: gce.OperatingSystemDistributionUnknown,
			diskSize:       100 * gce.GiB,
			expectedSize:   0,
		},
	}
	for _, tc := range testCases {
		gkeMigOsInfo := NewGkeMigOsInfo(gce.NewMigOsInfo(gce.OperatingSystemDefault, tc.osDistribution, ""), "", false)
		if actualReserved := tc.function(gkeMigOsInfo, tc.diskSize); actualReserved != tc.expectedSize {
			t.Errorf("Test case: %s, Got f(%d b, %s ) = %d.  Want %d", tc.name, tc.diskSize, tc.osDistribution, actualReserved, tc.expectedSize)
		}
	}
}

func TestCalculatePhysicalEphemeralStorageGiB(t *testing.T) {
	gkeMigOsInfo := NewGkeMigOsInfo(gce.NewMigOsInfo(gce.OperatingSystemDefault, gce.OperatingSystemDistributionCOS, ""), "", false)
	r := &GkeReserved{}

	type testCase struct {
		name             string
		allocatableBytes int64
		expectedSize     int64
	}
	testCases := []testCase{
		{
			name:             "allocatable:84GiB ==> physical: 174GiB (osReserved=6.9GiB, kubeReserved=66GiB, evictionThreshold=16.6GiB)",
			allocatableBytes: 84 * GiB,
			expectedSize:     174,
		},
		{
			name:             "Above Autopilot default: allocatable:400GiB ==> physical: 569GiB (osReserved=13GiB, kubeReserved=100GiB, evictionThreshold=55.6GiB)",
			allocatableBytes: 400 * GiB,
			expectedSize:     569,
		},
		{
			name:             "MAX: allocatable:63Ti ==> physical: 64Ti",
			allocatableBytes: 63 * 1024 * GiB,
			expectedSize:     64 * 1024,
		},
		{
			name:             "MIN: allocatable:50MiB ==> physical: 10GiB",
			allocatableBytes: 50 * MiB,
			expectedSize:     10,
		},
	}

	for _, tc := range testCases {
		if physical := r.CalculatePhysicalEphemeralStorageGiB(gkeMigOsInfo, tc.allocatableBytes); physical != tc.expectedSize {
			t.Errorf("Test case: %s, Got f(%d b) = %d.  Want %d", tc.name, tc.allocatableBytes, physical, tc.expectedSize)
		}
	}
}

func TestReverseCalculationBasedOnBuildNodeAllocatable(t *testing.T) {
	var cpu int64 = 4
	var memory int64 = 800000000
	osDistribution := gce.OperatingSystemDistributionCOS
	systemArchitecture := gce.Amd64

	// bootDiskGiB only
	testCases := []int64{
		20, // min
		25,
		98,
		99,
		100,
		101,
		500,
		5000,
		50000,
		64 * 1024, // max: 64Ti
	}
	for _, expectedBootDiskGiB := range testCases {
		scenario := fmt.Sprintf("bootDiskGiB: %d", expectedBootDiskGiB)
		t.Run(scenario, func(t *testing.T) {
			tb := &GkeTemplateBuilder{}
			reserved := &GkeReserved{}
			ssdDiskSizeProvider := localssdsize.NewSimpleLocalSSDProvider()

			g := newTestGkeManager(t, "", napEnabled, false, false, nil, false, nil)
			mig := &GkeMig{
				gceRef: gce.GceRef{
					Project: projectId,
					Zone:    zoneB,
					Name:    "nodeautoprovisioning-323233232",
				},
				gkeManager:      g,
				minSize:         0,
				maxSize:         10000,
				autoprovisioned: true,
				exist:           true,
				spec: &gkeclient.NodePoolSpec{
					MachineType:        "n1-standard-1",
					DiskSize:           expectedBootDiskGiB,
					SystemArchitecture: &systemArchitecture,
					LocalSSDConfig:     &gkeclient.LocalSSDConfig{EphemeralStorageConfig: nil},
				},
			}
			AddMigsToNodePool("nodeautoprovisioning-323233232", mig)
			gkeMigOsInfo := NewGkeMigOsInfo(gce.NewMigOsInfo(gce.OperatingSystemLinux, osDistribution, systemArchitecture), mig.Version(), mig.IsConfidentialNode())

			node, _ := tb.BuildNodeFromMigSpec(mig, gkeMigOsInfo, cpu, memory, nil, &DaemonSetConditions{}, false, reserved, ssdDiskSizeProvider, mig.spec.MaxPodsPerNode)
			q := node.Status.Allocatable[apiv1.ResourceEphemeralStorage]
			allocatableBytes := q.Value()

			actualRecalculatedPhysical := reserved.CalculatePhysicalEphemeralStorageGiB(gkeMigOsInfo, allocatableBytes)

			// Why 2GiB error margin?
			// The Node Allocatable doesn't linearly grow with it's bootDiskSize,
			// e.g. node with bootDisk=100GiB has less Allocatable than node with bootDisk=99GiB
			//
			// This is due to the "kube-reserved" formula and it's internal GiB rounding
			// ref: pkg/cloudprovider/gke/reserved.go:PredictKubeReservedEphemeralStorage
			if math.Abs(float64(expectedBootDiskGiB-actualRecalculatedPhysical)) > 2 {
				t.Errorf("Invalid PhysicalEphemeralStorage recalculation: expected=%d, actual=%d", expectedBootDiskGiB, actualRecalculatedPhysical)
			}
		})
	}
}

const osReservedTestContent = `
memory:
- osDistribution: cos
  architecture: amd64
  confidential: false
  physicalBytes: 1073741824
  reservedBytes: 49881088
  nodeVersions:
  - 1.25.0-gke.1100
disk:
- osDistribution: cos
  architecture: amd64
  physicalBytes: 10737418240
  reservedBytes: 4642525184
  nodeVersions:
  - 1.25.0-gke.1100
`

func TestNewGkeReservedFromBytesContent(t *testing.T) {
	testCases := []struct {
		descr   string
		content string
		wantErr error
	}{
		{
			descr:   "Without error",
			content: osReservedTestContent,
		},
		{
			descr:   "With error",
			content: "invalid",
			wantErr: fmt.Errorf("cannot unmarshal !!str `invalid` into nodetemplate.fileConfig"),
		},
		{
			descr:   "Empty content",
			content: "",
			wantErr: fmt.Errorf("haven't loaded os-reserved file: no os reserved entries were parsed from os reserved config"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.descr, func(t *testing.T) {
			gkeReserved, err := NewGkeReserved([]byte(tc.content))
			if tc.wantErr != nil {
				assert.Contains(t, err.Error(), tc.wantErr.Error())
			} else {
				assert.Nil(t, err)
				assert.NotNil(t, gkeReserved)
			}
		})
	}
}
