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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	mcv1 "k8s.io/gke-autoscaling/cluster-autoscaler/apis/machineconfig/cloud.google.com/v1"
	"k8s.io/utils/ptr"
)

// Define "raw" type to omit the (f MachineFamily) Equal() function
type rawMF MachineFamily
type rawMT MachineType

var (
	testAmdTurinPlatform     = newCpuPlatformInfo(AmdTurin, amd, []CpuPlatform{"Amd Turin", "amd-turin"})
	testAmdGenoaPlatform     = newCpuPlatformInfo(AmdGenoa, amd, []CpuPlatform{"Amd Genoa", "amd-genoa"})
	testIntelSkylakePlatform = newCpuPlatformInfo(IntelSkylake, intel, []CpuPlatform{"skylake", "intel-skylake"})

	cpSource = newCpuPlatformsSource().
			register(testAmdTurinPlatform).
			register(testAmdGenoaPlatform).
			register(testIntelSkylakePlatform)

	crdAmdTurinPlatform = mcv1.CPUPlatform{
		Name:        AmdTurin,
		Aliases:     []string{"Amd Turin", "amd-turin"},
		Vendor:      ptr.To(amd),
		VendorOrder: ptr.To(int64(1)),
	}

	crdAmdGenoaPlatform = mcv1.CPUPlatform{
		Name:        AmdGenoa,
		Aliases:     []string{"Amd Genoa", "amd-genoa"},
		Vendor:      ptr.To(amd),
		VendorOrder: ptr.To(int64(2)),
	}

	crdIntelSkylakePlatform = mcv1.CPUPlatform{
		Name:        IntelSkylake,
		Aliases:     []string{"skylake", "intel-skylake"},
		Vendor:      ptr.To(intel),
		VendorOrder: ptr.To(int64(1)),
	}
)

func newTestMachineTypeInfo(name string, cpus int64, mem int64) MachineType {
	return MachineType{
		MachineType: gce.MachineType{
			Name:   name,
			CPU:    cpus,
			Memory: mem * bytesPerMiB,
		},
	}

}

func TestToMachineFamilyObject_NilInput(t *testing.T) {
	want := MachineFamily{}
	got, gradedErr := ToMachineFamilyObject(nil, newCpuPlatformsSource(), false)

	if gradedErr.Warning != nil {
		t.Errorf("ToMachineFamilyObject() unexpected warning = %v", gradedErr.Warning)
	}
	if gradedErr.Err != nil {
		t.Errorf("ToMachineFamilyObject() unexpected error = %v", gradedErr.Err)
	}
	if diff := cmp.Diff(rawMF(want), rawMF(got), cmp.AllowUnexported(rawMF{}, CpuPlatformRequirements{}), cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("ToMachineFamilyObject() mismatch (-want +got):\n%s", diff)
	}
}

func TestToMachineFamilyObject_AllProperties(t *testing.T) {
	input := &mcv1.MachineFamily{
		Name: "e2",
		DefaultProperties: mcv1.MachineProperties{
			SystemArchitecture: ptr.To("arm64"),
			ThreadsPerCore:     ptr.To(int64(16)),
			BootDiskConfig: &mcv1.BootDiskConfig{
				DefaultType: "pd-standard",
				Types:       []string{"pd-standard", "pd-ssd"},
			},
			CompactPlacementConfig: &mcv1.CompactPlacementConfig{
				Supported: true,
				MaxCount:  ptr.To(int64(125)),
			},
			CPUPlatforms: []mcv1.CPUPlatform{
				crdAmdTurinPlatform,
				crdAmdGenoaPlatform,
			},
		},
		Weights: &mcv1.MachineFamilyWeights{
			Predefined: mcv1.ResourceWeights{
				CPU:      "0.123",
				Memory:   "0.456",
				LocalSSD: "0.78",
			},
		},
	}
	want := MachineFamily{
		name:                     "e2",
		systemArchitecture:       gce.Arm64,
		nonDefaultThreadsPerCore: ptr.To(int64(16)),
		supportedBootDiskTypes: map[string]bool{
			"pd-standard": true,
			"pd-ssd":      true,
		},
		defaultDiskType:          "pd-standard",
		supportCompactPlacement:  true,
		maxCompactPlacementNodes: 125,
		pricingInfo: MachineFamilyPricingInfo{
			CpuPricePerHour:           0.123,
			MemoryPricePerHourPerGb:   0.456,
			LocalSsdPricePerHourPerGb: 0.78,
		},
		supportedCpuPlatforms: CpuPlatformRequirements{
			lowerBound: AmdTurin,
			upperBound: AmdGenoa,
		},
	}

	got, gradedErr := ToMachineFamilyObject(input, cpSource, false)

	if gradedErr.Warning != nil {
		t.Errorf("ToMachineFamilyObject() unexpected warning = %v", gradedErr.Warning)
	}
	if gradedErr.Err != nil {
		t.Errorf("ToMachineFamilyObject() unexpected error = %v", gradedErr.Err)
	}

	if diff := cmp.Diff(rawMF(want), rawMF(got), cmp.AllowUnexported(rawMF{}, CpuPlatformRequirements{}), cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("ToMachineFamilyObject() mismatch (-want +got):\n%s", diff)
	}
}

func TestToMachineFamilyObject_Name(t *testing.T) {
	input := &mcv1.MachineFamily{
		Name: "e2",
	}
	want := MachineFamily{
		name:                  "e2",
		supportedCpuPlatforms: noPlatformSupported,
	}

	got, gradedErr := ToMachineFamilyObject(input, cpSource, false)

	if gradedErr.Warning != nil {
		t.Errorf("ToMachineFamilyObject() unexpected warning = %v", gradedErr.Warning)
	}
	if gradedErr.Err != nil {
		t.Errorf("ToMachineFamilyObject() unexpected error = %v", gradedErr.Err)
	}

	if diff := cmp.Diff(rawMF(want), rawMF(got), cmp.AllowUnexported(rawMF{}, CpuPlatformRequirements{}), cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("ToMachineFamilyObject() mismatch (-want +got):\n%s", diff)
	}
}

func TestToMachineFamilyObject_SystemArchitecture(t *testing.T) {
	testCases := []struct {
		name         string
		arch         *string
		expectedArch gce.SystemArchitecture
	}{
		{
			name:         "arm64",
			arch:         ptr.To("arm64"),
			expectedArch: gce.Arm64,
		},
		{
			name:         "unknown",
			arch:         ptr.To("noexist"),
			expectedArch: gce.UnknownArch,
		},
		{
			name:         "nil",
			arch:         nil,
			expectedArch: gce.UnknownArch,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			input := &mcv1.MachineFamily{
				Name: "test-family",
				DefaultProperties: mcv1.MachineProperties{
					SystemArchitecture: tc.arch,
				},
			}

			want := MachineFamily{
				name:                  "test-family",
				systemArchitecture:    tc.expectedArch,
				supportedCpuPlatforms: noPlatformSupported,
			}
			got, gradedErr := ToMachineFamilyObject(input, cpSource, false)

			if gradedErr.Warning != nil {
				t.Errorf("ToMachineFamilyObject() unexpected warning = %v", gradedErr.Warning)
			}
			if gradedErr.Err != nil {
				t.Errorf("ToMachineFamilyObject() unexpected error = %v", gradedErr.Err)
			}
			if diff := cmp.Diff(rawMF(want), rawMF(got), cmp.AllowUnexported(rawMF{}, CpuPlatformRequirements{}), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("ToMachineFamilyObject() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestToMachineFamilyObject_ThreadsPerCore(t *testing.T) {
	input := &mcv1.MachineFamily{
		Name: "e2",
		DefaultProperties: mcv1.MachineProperties{
			ThreadsPerCore: ptr.To(int64(16)),
		},
	}
	want := MachineFamily{
		name:                     "e2",
		nonDefaultThreadsPerCore: ptr.To(int64(16)),
		supportedCpuPlatforms:    noPlatformSupported,
	}

	got, gradedErr := ToMachineFamilyObject(input, cpSource, false)

	if gradedErr.Warning != nil {
		t.Errorf("ToMachineFamilyObject() unexpected warning = %v", gradedErr.Warning)
	}
	if gradedErr.Err != nil {
		t.Errorf("ToMachineFamilyObject() unexpected error = %v", gradedErr.Err)
	}

	if diff := cmp.Diff(rawMF(want), rawMF(got), cmp.AllowUnexported(rawMF{}, CpuPlatformRequirements{}), cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("ToMachineFamilyObject() mismatch (-want +got):\n%s", diff)
	}
}

func TestToMachineFamilyObject_BootDiskConfig(t *testing.T) {
	testCases := []struct {
		name           string
		bootDiskConfig *mcv1.BootDiskConfig
		wantErr        bool
		want           MachineFamily
	}{
		{
			name: "valid config",
			bootDiskConfig: &mcv1.BootDiskConfig{
				DefaultType: "pd-standard",
				Types:       []string{"pd-standard", "pd-ssd"},
			},
			wantErr: false,
			want: MachineFamily{
				name: "e2",
				supportedBootDiskTypes: map[string]bool{
					"pd-standard": true,
					"pd-ssd":      true,
				},
				defaultDiskType:       "pd-standard",
				supportedCpuPlatforms: noPlatformSupported,
			},
		},
		{
			name: "default disk not in supported list",
			bootDiskConfig: &mcv1.BootDiskConfig{
				DefaultType: "pd-standard",
				Types:       []string{"pd-ssd"},
			},
			wantErr: true,
		},
		{
			name: "only supported disk types with no default type",
			bootDiskConfig: &mcv1.BootDiskConfig{
				Types: []string{"pd-ssd"},
			},
			wantErr: false,
			want: MachineFamily{
				name: "e2",
				supportedBootDiskTypes: map[string]bool{
					"pd-ssd": true,
				},
				supportedCpuPlatforms: noPlatformSupported,
			},
		},
		{
			name:           "nil boot disk config",
			bootDiskConfig: nil,
			wantErr:        false,
			want: MachineFamily{
				name:                  "e2",
				supportedCpuPlatforms: noPlatformSupported,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			input := &mcv1.MachineFamily{
				Name: "e2",
				DefaultProperties: mcv1.MachineProperties{
					BootDiskConfig: tc.bootDiskConfig,
				},
			}
			got, gradedErr := ToMachineFamilyObject(input, cpSource, false)

			if gradedErr.Warning != nil {
				t.Errorf("ToMachineFamilyObject() unexpected warning = %v", gradedErr.Warning)
			}
			if (gradedErr.Err != nil) != tc.wantErr {
				t.Errorf("ToMachineFamilyObject() error = %v, wantErr %v", gradedErr.Err, tc.wantErr)
				return
			}
			if tc.wantErr {
				return
			}

			if diff := cmp.Diff(rawMF(tc.want), rawMF(got), cmp.AllowUnexported(rawMF{}, CpuPlatformRequirements{}), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("ToMachineFamilyObject() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestToMachineFamilyObject_CompactPlacementConfig(t *testing.T) {
	testCases := []struct {
		name                   string
		compactPlacementConfig *mcv1.CompactPlacementConfig
		wantErr                bool
		want                   MachineFamily
	}{
		{
			name: "valid config",
			compactPlacementConfig: &mcv1.CompactPlacementConfig{
				Supported: true,
				MaxCount:  ptr.To(int64(125)),
			},
			want: MachineFamily{
				name:                     "e2",
				supportCompactPlacement:  true,
				maxCompactPlacementNodes: 125,
				supportedCpuPlatforms:    noPlatformSupported,
			},
		},
		{
			name: "Compact placement unsupported explicitly for machine family",
			compactPlacementConfig: &mcv1.CompactPlacementConfig{
				Supported: false,
			},
			want: MachineFamily{
				name:                     "e2",
				supportCompactPlacement:  false,
				maxCompactPlacementNodes: 0,
				supportedCpuPlatforms:    noPlatformSupported,
			},
		},
		{
			name: "Compact placement valid config - disabled with max count set",
			compactPlacementConfig: &mcv1.CompactPlacementConfig{
				Supported: false,
				MaxCount:  ptr.To(int64(125)),
			},
			want: MachineFamily{
				name:                     "e2",
				supportCompactPlacement:  false,
				maxCompactPlacementNodes: 125,
				supportedCpuPlatforms:    noPlatformSupported,
			},
		},
		{
			name: "Compact placement wrong config - enabled with negative max count",
			compactPlacementConfig: &mcv1.CompactPlacementConfig{
				Supported: true,
				MaxCount:  ptr.To(int64(-5)),
			},
			wantErr: true,
		},
		{
			name:                   "nil compact placement config",
			compactPlacementConfig: nil,
			wantErr:                false,
			want: MachineFamily{
				name:                  "e2",
				supportedCpuPlatforms: noPlatformSupported,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			input := &mcv1.MachineFamily{
				Name: "e2",
				DefaultProperties: mcv1.MachineProperties{
					CompactPlacementConfig: tc.compactPlacementConfig,
				},
			}
			got, gradedErr := ToMachineFamilyObject(input, cpSource, false)

			if gradedErr.Warning != nil {
				t.Errorf("ToMachineFamilyObject() unexpected warning = %v", gradedErr.Warning)
			}
			if (gradedErr.Err != nil) != tc.wantErr {
				t.Errorf("ToMachineFamilyObject() error = %v, wantErr %v", gradedErr.Err, tc.wantErr)
				return
			}
			if tc.wantErr {
				return
			}

			if diff := cmp.Diff(rawMF(tc.want), rawMF(got), cmp.AllowUnexported(rawMF{}, CpuPlatformRequirements{}), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("ToMachineFamilyObject() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestToMachineFamilyObject_Weights(t *testing.T) {
	testCases := []struct {
		name    string
		weights *mcv1.MachineFamilyWeights
		wantErr bool
		want    MachineFamily
	}{
		// Test cases for weights values being missing.
		{
			name:    "nil weights",
			weights: nil,
			want: MachineFamily{
				name:                  "e2",
				supportedCpuPlatforms: noPlatformSupported,
			},
		},
		{
			name: "missing weights",
			weights: &mcv1.MachineFamilyWeights{
				Predefined: mcv1.ResourceWeights{},
			},
			wantErr: false,
			want: MachineFamily{
				name: "e2",
				pricingInfo: MachineFamilyPricingInfo{
					CpuPricePerHour:         0.0,
					MemoryPricePerHourPerGb: 0.0,
					PreemptibleDiscount:     0.0,
				},
				supportedCpuPlatforms: noPlatformSupported,
			},
		},
		{
			name: "empty string as a CPU weight in Predefined field",
			weights: &mcv1.MachineFamilyWeights{
				Predefined: mcv1.ResourceWeights{
					CPU:      "",
					Memory:   "0.456",
					LocalSSD: "0.78",
				},
			},
			want: MachineFamily{
				name: "e2",
				pricingInfo: MachineFamilyPricingInfo{
					CpuPricePerHour:           0.0,
					MemoryPricePerHourPerGb:   0.456,
					LocalSsdPricePerHourPerGb: 0.78,
				},
				supportedCpuPlatforms: noPlatformSupported,
			},
		},
		{
			name: "empty string as a CPU weight in Custom field",
			weights: &mcv1.MachineFamilyWeights{
				Custom: &mcv1.ResourceWeights{
					CPU:      "",
					Memory:   "0.456",
					LocalSSD: "0.78",
				},
			},
			want: MachineFamily{
				name: "e2",
				customPricingInfo: &MachineFamilyPricingInfo{
					CpuPricePerHour:           0.0,
					MemoryPricePerHourPerGb:   0.456,
					LocalSsdPricePerHourPerGb: 0.78,
				},
				supportedCpuPlatforms: noPlatformSupported,
			},
		},
		{
			name: "missing preemptible discount",
			weights: &mcv1.MachineFamilyWeights{
				Predefined: mcv1.ResourceWeights{
					CPU: "0.04",
				},
				Custom: &mcv1.ResourceWeights{
					CPU: "0.05",
				},
			},
			want: MachineFamily{
				name: "e2",
				pricingInfo: MachineFamilyPricingInfo{
					CpuPricePerHour: 0.04,
				},
				customPricingInfo: &MachineFamilyPricingInfo{
					CpuPricePerHour: 0.05,
				},
				supportedCpuPlatforms: noPlatformSupported,
			},
		},
		{
			name: "preemptible discount zero if predefined CPU is zero",
			weights: &mcv1.MachineFamilyWeights{
				Predefined: mcv1.ResourceWeights{
					CPU: "0",
				},
				Preemptible: &mcv1.ResourceWeights{
					CPU: "0.01",
				},
				Custom: &mcv1.ResourceWeights{
					CPU: "0.05",
				},
			},
			want: MachineFamily{
				name: "e2",
				pricingInfo: MachineFamilyPricingInfo{
					CpuPricePerHour:     0.0,
					PreemptibleDiscount: 0.0,
				},
				customPricingInfo: &MachineFamilyPricingInfo{
					CpuPricePerHour:     0.05,
					PreemptibleDiscount: 0.0,
				},
				supportedCpuPlatforms: noPlatformSupported,
			},
		},
		// Test cases for weights values that cannot be parsed.
		{
			name: "invalid CPU price format in Predefined field",
			weights: &mcv1.MachineFamilyWeights{
				Predefined: mcv1.ResourceWeights{
					CPU:      "INVALID",
					Memory:   "0.456",
					LocalSSD: "0.78",
				},
			},
			wantErr: true,
		},
		{
			name: "invalid Memory price format in Predefined field",
			weights: &mcv1.MachineFamilyWeights{
				Predefined: mcv1.ResourceWeights{
					CPU:      "0.123",
					Memory:   "INVALID",
					LocalSSD: "0.78",
				},
			},
			wantErr: true,
		},
		{
			name: "invalid LocalSSD price format in Predefined field",
			weights: &mcv1.MachineFamilyWeights{
				Predefined: mcv1.ResourceWeights{
					CPU:      "0.123",
					Memory:   "0.456",
					LocalSSD: "INVALID",
				},
			},
			wantErr: true,
		},
		{
			name: "invalid CPU price format in Preemptible field",
			weights: &mcv1.MachineFamilyWeights{
				Preemptible: &mcv1.ResourceWeights{
					CPU:      "INVALID",
					Memory:   "0.456",
					LocalSSD: "0.78",
				},
			},
			wantErr: true,
		},
		{
			name: "invalid CPU price format in Custom field",
			weights: &mcv1.MachineFamilyWeights{
				Custom: &mcv1.ResourceWeights{
					CPU:      "INVALID",
					Memory:   "0.456",
					LocalSSD: "0.78",
				},
			},
			wantErr: true,
		},
		// --- Parsing / Converting ---
		{
			name: "valid weights for all fields",
			weights: &mcv1.MachineFamilyWeights{
				Predefined: mcv1.ResourceWeights{
					CPU:      "0.1",
					Memory:   "0.2",
					LocalSSD: "0.3",
				},
				Custom: &mcv1.ResourceWeights{
					CPU:      "0.4",
					Memory:   "0.5",
					LocalSSD: "0.6",
				},
				Preemptible: &mcv1.ResourceWeights{
					CPU:      "0.05",
					Memory:   "0.06",
					LocalSSD: "0.07",
				},
			},
			want: MachineFamily{
				name: "e2",
				pricingInfo: MachineFamilyPricingInfo{
					CpuPricePerHour:           0.1,
					MemoryPricePerHourPerGb:   0.2,
					LocalSsdPricePerHourPerGb: 0.3,
					PreemptibleDiscount:       0.05 / 0.1,
				},
				customPricingInfo: &MachineFamilyPricingInfo{
					CpuPricePerHour:           0.4,
					MemoryPricePerHourPerGb:   0.5,
					LocalSsdPricePerHourPerGb: 0.6,
					PreemptibleDiscount:       0.05 / 0.1,
				},
				supportedCpuPlatforms: noPlatformSupported,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			input := &mcv1.MachineFamily{
				Name:    "e2",
				Weights: tc.weights,
			}
			got, gradedErr := ToMachineFamilyObject(input, cpSource, false)

			if gradedErr.Warning != nil {
				t.Errorf("ToMachineFamilyObject() unexpected warning = %v", gradedErr.Warning)
			}
			if (gradedErr.Err != nil) != tc.wantErr {
				t.Errorf("ToMachineFamilyObject() error = %v, wantErr %v", gradedErr.Err, tc.wantErr)
				return
			}
			if tc.wantErr {
				return
			}

			if diff := cmp.Diff(rawMF(tc.want), rawMF(got), cmp.AllowUnexported(rawMF{}, CpuPlatformRequirements{}), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("ToMachineFamilyObject() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestToMachineFamilyObject_NAPDisabled(t *testing.T) {
	testCases := []struct {
		name              string
		familyNAPDisabled *bool
		types             []mcv1.MachineType
		wantAuto          []string
		wantOther         []string
	}{
		{
			name:              "Family disabled by default",
			familyNAPDisabled: ptr.To(true),
			types: []mcv1.MachineType{
				{Name: "t1", Resources: mcv1.MachineResources{CPUs: 1, Memory: 1024}},
				{Name: "t2", Resources: mcv1.MachineResources{CPUs: 1, Memory: 1024}, Properties: &mcv1.MachineProperties{NAPDisabled: ptr.To(false)}},
			},
			wantAuto:  []string{"t2"},
			wantOther: []string{"t1"},
		},
		{
			name:              "Family disabled by default, non-override set in machine types",
			familyNAPDisabled: ptr.To(true),
			types: []mcv1.MachineType{
				{Name: "t1", Resources: mcv1.MachineResources{CPUs: 1, Memory: 1024}, Properties: &mcv1.MachineProperties{NAPDisabled: ptr.To(false)}},
				{Name: "t2", Resources: mcv1.MachineResources{CPUs: 1, Memory: 1024}, Properties: &mcv1.MachineProperties{NAPDisabled: ptr.To(true)}},
			},
			wantAuto:  []string{"t1"},
			wantOther: []string{"t2"},
		},
		{
			name:              "Family enabled by default",
			familyNAPDisabled: ptr.To(false),
			types: []mcv1.MachineType{
				{Name: "t1", Resources: mcv1.MachineResources{CPUs: 1, Memory: 1024}},
				{Name: "t2", Resources: mcv1.MachineResources{CPUs: 1, Memory: 1024}, Properties: &mcv1.MachineProperties{NAPDisabled: ptr.To(true)}},
			},
			wantAuto:  []string{"t1"},
			wantOther: []string{"t2"},
		},
		{
			name:              "Family enabled by default, non-override set in machine types",
			familyNAPDisabled: ptr.To(false),
			types: []mcv1.MachineType{
				{Name: "t1", Resources: mcv1.MachineResources{CPUs: 1, Memory: 1024}, Properties: &mcv1.MachineProperties{NAPDisabled: ptr.To(false)}},
				{Name: "t2", Resources: mcv1.MachineResources{CPUs: 1, Memory: 1024}, Properties: &mcv1.MachineProperties{NAPDisabled: ptr.To(true)}},
			},
			wantAuto:  []string{"t1"},
			wantOther: []string{"t2"},
		},
		{
			name:              "Family nil (defaults to enabled)",
			familyNAPDisabled: nil,
			types: []mcv1.MachineType{
				{Name: "t1", Resources: mcv1.MachineResources{CPUs: 1, Memory: 1024}},
				{Name: "t2", Resources: mcv1.MachineResources{CPUs: 1, Memory: 1024}, Properties: &mcv1.MachineProperties{NAPDisabled: ptr.To(true)}},
			},
			wantAuto:  []string{"t1"},
			wantOther: []string{"t2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			input := &mcv1.MachineFamily{
				Name: "test-family",
				DefaultProperties: mcv1.MachineProperties{
					NAPDisabled: tc.familyNAPDisabled,
				},
				MachineTypes: tc.types,
			}

			got, gradedErr := ToMachineFamilyObject(input, cpSource, false)

			if gradedErr.Err != nil {
				t.Errorf("ToMachineFamilyObject() unexpected error = %v", gradedErr.Err)
			}

			// Check autoprovisioned
			for _, name := range tc.wantAuto {
				if _, ok := got.autoprovisionedMachineTypes[name]; !ok {
					t.Errorf("Expected %q to be in autoprovisionedMachineTypes", name)
				}
			}
			if len(got.autoprovisionedMachineTypes) != len(tc.wantAuto) {
				t.Errorf("autoprovisionedMachineTypes length mismatch: got %d, want %d", len(got.autoprovisionedMachineTypes), len(tc.wantAuto))
			}

			// Check other
			for _, name := range tc.wantOther {
				if _, ok := got.otherMachineTypes[name]; !ok {
					t.Errorf("Expected %q to be in otherMachineTypes", name)
				}
			}
			if len(got.otherMachineTypes) != len(tc.wantOther) {
				t.Errorf("otherMachineTypes length mismatch: got %d, want %d", len(got.otherMachineTypes), len(tc.wantOther))
			}
		})
	}
}

func TestToMachineFamilyObject_MachineTypeWeights(t *testing.T) {
	testCases := []struct {
		name    string
		weights *mcv1.MachineTypeWeights
		wantErr bool
		want    *MachinePriceInfo
	}{
		{
			name:    "nil weights",
			weights: nil,
			want:    nil,
		},
		{
			name: "valid weights",
			weights: &mcv1.MachineTypeWeights{
				InstanceWeight:            ptr.To("0.0475"),
				PreemptibleInstanceWeight: ptr.To("0.0100"),
			},
			want: &MachinePriceInfo{
				instancePrice:           ptr.To(0.0475),
				preemtibleInstancePrice: ptr.To(0.0100),
			},
		},
		{
			name: "missing data - both fields nil",
			weights: &mcv1.MachineTypeWeights{
				InstanceWeight:            nil,
				PreemptibleInstanceWeight: nil,
			},
			want: nil,
		},
		{
			name: "missing data - instance weight nil",
			weights: &mcv1.MachineTypeWeights{
				InstanceWeight:            nil,
				PreemptibleInstanceWeight: ptr.To("0.0100"),
			},
			want: &MachinePriceInfo{
				preemtibleInstancePrice: ptr.To(0.0100),
			},
		},
		{
			name: "missing data - preemptible weight nil",
			weights: &mcv1.MachineTypeWeights{
				InstanceWeight:            ptr.To("0.0475"),
				PreemptibleInstanceWeight: nil,
			},
			want: &MachinePriceInfo{
				instancePrice: ptr.To(0.0475),
			},
		},
		{
			name: "invalid InstanceWeight",
			weights: &mcv1.MachineTypeWeights{
				InstanceWeight: ptr.To("INVALID"),
			},
			wantErr: true,
		},
		{
			name: "invalid PreemptibleInstanceWeight",
			weights: &mcv1.MachineTypeWeights{
				PreemptibleInstanceWeight: ptr.To("INVALID"),
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			input := &mcv1.MachineFamily{
				Name: "e2",
				MachineTypes: []mcv1.MachineType{
					{
						Name: "e2-standard-4",
						Resources: mcv1.MachineResources{
							CPUs:   4,
							Memory: 16384,
						},
						Weights: tc.weights,
					},
				},
			}

			got, gradedErr := ToMachineFamilyObject(input, cpSource, false)

			t.Logf("warn: %v", gradedErr.Warning)

			if (gradedErr.Err != nil) != tc.wantErr {
				t.Errorf("ToMachineFamilyObject() error = %v, wantErr %v", gradedErr.Err, tc.wantErr)
				return
			}
			if tc.wantErr {
				return
			}

			mt, ok := got.autoprovisionedMachineTypes["e2-standard-4"]
			if !ok {
				t.Fatalf("Expected e2-standard-4 to be in autoprovisionedMachineTypes")
			}

			if diff := cmp.Diff(tc.want, mt.priceInfo, cmp.AllowUnexported(MachinePriceInfo{})); diff != "" {
				t.Errorf("MachineType.priceInfo mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestToMachineFamilyObject_CPUPlatforms(t *testing.T) {
	type testCase struct {
		name     string
		input    *mcv1.MachineFamily
		want     MachineFamily
		wantErr  bool
		wantWarn bool
	}

	tests := []testCase{
		{
			name: "Empty CPU platform list returns noPlatformSupported platforms and no error",
			input: &mcv1.MachineFamily{
				Name: "e2",
				DefaultProperties: mcv1.MachineProperties{
					CPUPlatforms: []mcv1.CPUPlatform{},
				},
			},
			want: MachineFamily{
				name:                  "e2",
				supportedCpuPlatforms: noPlatformSupported,
			},
		},
		{
			name: "Singleton CPU platform list returns same lower and upper bounds",
			input: &mcv1.MachineFamily{
				Name: "e2",
				DefaultProperties: mcv1.MachineProperties{
					CPUPlatforms: []mcv1.CPUPlatform{
						crdAmdTurinPlatform,
					},
				},
			},
			want: MachineFamily{
				name: "e2",
				supportedCpuPlatforms: CpuPlatformRequirements{
					lowerBound: AmdTurin,
					upperBound: AmdTurin,
				},
			},
		},
		{
			name: "Valid input with 2 CPU platforms",
			input: &mcv1.MachineFamily{
				Name: "e2",
				DefaultProperties: mcv1.MachineProperties{
					CPUPlatforms: []mcv1.CPUPlatform{
						crdAmdGenoaPlatform,
						crdAmdTurinPlatform,
					},
				},
			},
			want: MachineFamily{
				name: "e2",
				supportedCpuPlatforms: CpuPlatformRequirements{
					lowerBound: AmdTurin,
					upperBound: AmdGenoa,
				},
			},
		},
		{
			name: "Invalid input: 1 existing and 2 unexistent CPU platforms, warning expected",
			input: &mcv1.MachineFamily{
				Name: "bad-family",
				DefaultProperties: mcv1.MachineProperties{
					CPUPlatforms: []mcv1.CPUPlatform{
						crdAmdTurinPlatform,
						{Name: "A"},
						{Name: "B"},
					},
				},
			},
			want: MachineFamily{
				name: "bad-family",
				supportedCpuPlatforms: CpuPlatformRequirements{
					lowerBound: AmdTurin,
					upperBound: AmdTurin,
				},
			},
			wantWarn: true,
		},
		{
			name: "Invalid input: 3 unexistent CPU platforms, warning expected",
			input: &mcv1.MachineFamily{
				Name: "bad-family",
				DefaultProperties: mcv1.MachineProperties{
					CPUPlatforms: []mcv1.CPUPlatform{
						{Name: "A"},
						{Name: "B"},
						{Name: "C"},
					},
				},
			},
			want: MachineFamily{
				name: "bad-family",
				supportedCpuPlatforms: CpuPlatformRequirements{
					lowerBound: UnknownPlatform,
					upperBound: UnknownPlatform,
				},
			},
			wantWarn: true,
		},
		{
			name: "Invalid input: 3 CPU platforms that belong to different orders, warning expected",
			input: &mcv1.MachineFamily{
				Name: "multiorder-family",
				DefaultProperties: mcv1.MachineProperties{
					CPUPlatforms: []mcv1.CPUPlatform{
						crdAmdTurinPlatform,
						crdAmdGenoaPlatform,
						{Name: IntelSkylake, Vendor: ptr.To(intel), VendorOrder: ptr.To(int64(1))},
					},
				},
			},
			want: MachineFamily{
				name: "multiorder-family",
				supportedCpuPlatforms: CpuPlatformRequirements{
					lowerBound: UnknownPlatform,
					upperBound: UnknownPlatform,
				},
			},
			wantWarn: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, gradedErr := ToMachineFamilyObject(tc.input, cpSource, false)

			if (gradedErr.Warning != nil) != tc.wantWarn {
				t.Errorf("ToMachineFamilyObject() warning = %v, wantWarn %v", gradedErr.Warning, tc.wantWarn)
				return
			}
			if (gradedErr.Err != nil) != tc.wantErr {
				t.Errorf("ToMachineFamilyObject() error = %v, wantErr %v", gradedErr.Err, tc.wantErr)
				return
			}

			if diff := cmp.Diff(rawMF(tc.want), rawMF(got), cmp.AllowUnexported(rawMF{}, CpuPlatformRequirements{}), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("ToMachineFamilyObject() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestToMachineFamilyObject_MachineTypes(t *testing.T) {
	cpuReqsAmdTurin := CpuPlatformRequirements{lowerBound: AmdTurin, upperBound: AmdTurin}

	type testCase struct {
		name     string
		input    *mcv1.MachineFamily
		want     MachineFamily
		wantErr  bool
		wantWarn bool
	}

	tests := []testCase{
		{
			name: "Machine types extracted without properties",
			input: &mcv1.MachineFamily{
				Name: "e2",
				MachineTypes: []mcv1.MachineType{
					{
						Name: "e2-standard-4",
						Resources: mcv1.MachineResources{
							CPUs:   4,
							Memory: 16,
						},
					},
				},
			},
			want: MachineFamily{
				name: "e2",
				autoprovisionedMachineTypes: map[string]MachineType{
					"e2-standard-4": newTestMachineTypeInfo("e2-standard-4", 4, 16),
				},
				supportedCpuPlatforms: noPlatformSupported,
			},
		},
		{
			name: "Machine types with properties assigned to other types due to NAP disabled",
			input: &mcv1.MachineFamily{
				Name: "c3",
				DefaultProperties: mcv1.MachineProperties{
					CPUPlatforms: []mcv1.CPUPlatform{
						crdAmdGenoaPlatform,
					},
				},
				MachineTypes: []mcv1.MachineType{
					{
						Name:      "c3-standard-4",
						Resources: mcv1.MachineResources{CPUs: 4, Memory: 16},
						Properties: &mcv1.MachineProperties{
							NAPDisabled: ptr.To(true),
							CPUPlatforms: []mcv1.CPUPlatform{
								crdAmdTurinPlatform,
							},
						},
					},
				},
			},
			want: MachineFamily{
				name: "c3",
				otherMachineTypes: map[string]MachineType{
					"c3-standard-4": newTestMachineTypeInfo("c3-standard-4", 4, 16).withExplicitReqOnly().withCpuPlatformRequirements(cpuReqsAmdTurin),
				},
				supportedCpuPlatforms: CpuPlatformRequirements{
					lowerBound: AmdGenoa,
					upperBound: AmdGenoa,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, warnsAndErrs := ToMachineFamilyObject(tc.input, cpSource, false)

			if (warnsAndErrs.Warning != nil) != tc.wantWarn {
				t.Errorf("ToMachineFamilyObject() warning = %v, wantWarn %v", warnsAndErrs.Warning, tc.wantWarn)
				return
			}
			if (warnsAndErrs.Err != nil) != tc.wantErr {
				t.Errorf("ToMachineFamilyObject() error = %v, wantErr %v", warnsAndErrs.Err, tc.wantErr)
				return
			}

			tc.want.precomputeAllMachineTypes()
			// Use cmp.AllowUnexported to compare private fields in the structs.
			if diff := cmp.Diff(rawMF(tc.want), rawMF(got), cmp.AllowUnexported(rawMF{}, CpuPlatformRequirements{}, MachineType{}), cmpopts.IgnoreFields(MachineType{}, "family")); diff != "" {
				t.Errorf("ToMachineFamilyObject() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestToMachineTypeObject(t *testing.T) {
	defaultCpuReqs := CpuPlatformRequirements{lowerBound: AmdTurin, upperBound: AmdTurin}

	testCases := []struct {
		name    string
		input   *mcv1.MachineType
		want    MachineType
		wantErr bool
	}{
		{
			name:  "nil input",
			input: nil,
			want:  MachineType{},
		},
		{
			name: "Machine type with no properties",
			input: &mcv1.MachineType{
				Name: "mt-1",
				Resources: mcv1.MachineResources{
					CPUs:   4,
					Memory: 16,
				},
			},
			want: newTestMachineTypeInfo("mt-1", 4, 16),
		},
		{
			name: "Machine type with all properties",
			input: &mcv1.MachineType{
				Name: "mt-1",
				Resources: mcv1.MachineResources{
					CPUs:   8,
					Memory: 32,
				},
				Properties: &mcv1.MachineProperties{
					CPUPlatforms: []mcv1.CPUPlatform{
						crdAmdGenoaPlatform,
					},
					ThreadsPerCore:     ptr.To(int64(1)),
					NAPDisabled:        ptr.To(true),
					SystemArchitecture: ptr.To("arm64"), // will be neglected
					BootDiskConfig: &mcv1.BootDiskConfig{ // will be neglected
						DefaultType: "pd-standard",
						Types:       []string{"pd-standard", "pd-ssd"},
					},
					CompactPlacementConfig: &mcv1.CompactPlacementConfig{ // will be neglected
						Supported: true,
						MaxCount:  ptr.To(int64(125)),
					},
				},
			},
			want: newTestMachineTypeInfo("mt-1", 8, 32).
				withCpuPlatformRequirements(NewCpuPlatformRequirements(AmdGenoa, AmdGenoa)).
				withThreadsPerCoreOverride(1).
				withExplicitReqOnly(),
		},
		{
			name: "Machine type with the same CPU platforms as the one in the Machine Family DefaultProperties",
			input: &mcv1.MachineType{
				Name: "mt-1",
				Resources: mcv1.MachineResources{
					CPUs:   8,
					Memory: 32,
				},
				Properties: &mcv1.MachineProperties{
					CPUPlatforms: []mcv1.CPUPlatform{
						crdAmdTurinPlatform,
					},
				},
			},
			want: newTestMachineTypeInfo("mt-1", 8, 32),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ToMachineTypeObject(nil, tc.input, cpSource, defaultCpuReqs, false)
			if (err != nil) != tc.wantErr {
				t.Errorf("ToMachineTypeObject() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if diff := cmp.Diff(tc.want, got, cmp.AllowUnexported(CpuPlatformRequirements{}, MachineType{})); diff != "" {
				t.Errorf("ToMachineTypeObject() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestToMachineTypeObject_ConfidentialNodeConfigs(t *testing.T) {
	mf := &mcv1.MachineFamily{Name: "n2d"}
	defaultCpuReqs := CpuPlatformRequirements{lowerBound: AmdTurin, upperBound: AmdTurin}

	testCases := []struct {
		name         string
		enableCvmSot bool
		input        *mcv1.MachineType
		wantSupport  map[string]bool
	}{
		{
			name:         "dynamic config explicitly disables cvm",
			enableCvmSot: true,
			input: &mcv1.MachineType{
				Name:      "n2d-standard-2",
				Resources: mcv1.MachineResources{CPUs: 2, Memory: 8192},
				Properties: &mcv1.MachineProperties{
					SupportsConfidentialNodes: ptr.To(true), // legacy true
					ConfidentialNodeConfig: &mcv1.ConfidentialNodeConfig{
						Supported: ptr.To(false), // dynamic explicitly false
					},
				},
			},
			wantSupport: nil,
		},
		{
			name:         "dynamic config overrides to true with types",
			enableCvmSot: true,
			input: &mcv1.MachineType{
				Name:      "n2d-standard-2",
				Resources: mcv1.MachineResources{CPUs: 2, Memory: 8192},
				Properties: &mcv1.MachineProperties{
					ConfidentialNodeConfig: &mcv1.ConfidentialNodeConfig{
						Supported: ptr.To(true),
						Types: []mcv1.ConfidentialNodeType{
							{Type: "SEV"},
						},
					},
				},
			},
			wantSupport: map[string]bool{"SEV": true},
		},
		{
			name:         "fallback to hardcoded list when crd is empty",
			enableCvmSot: false,
			input: &mcv1.MachineType{
				Name:      "n2d-standard-2",
				Resources: mcv1.MachineResources{CPUs: 2, Memory: 8192},
				Properties: &mcv1.MachineProperties{
					SupportsConfidentialNodes: ptr.To(true),
				},
			},
			wantSupport: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ToMachineTypeObject(mf, tc.input, cpSource, defaultCpuReqs, tc.enableCvmSot)
			if err != nil {
				t.Fatalf("ToMachineTypeObject() unexpected error = %v", err)
			}

			var gotSupport map[string]bool
			if got.confidentialNodeCfg != nil {
				gotSupport = got.confidentialNodeCfg.supportConfidentialNodeTypes
			}
			if diff := cmp.Diff(tc.wantSupport, gotSupport); diff != "" {
				t.Errorf("confidentialNodeCfg.supportConfidentialNodeTypes mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestToCpuPlatformInfoObject(t *testing.T) {
	testCases := []struct {
		name    string
		input   mcv1.CPUPlatform
		want    cpuPlatformInfo
		wantErr bool
	}{
		{
			name: "valid CPU Platform with aliases",
			input: mcv1.CPUPlatform{
				Name:        "Intel Ice Lake",
				Aliases:     []string{"icelake", "intel-icelake"},
				Vendor:      ptr.To("Intel"),
				VendorOrder: ptr.To(int64(6)),
			},
			want: cpuPlatformInfo{
				name:    "Intel Ice Lake",
				aliases: []CpuPlatform{"icelake", "intel-icelake"},
				vendor:  "Intel",
				order:   6,
			},
			wantErr: false,
		},
		{
			name: "CPU Platform with missing vendor info",
			input: mcv1.CPUPlatform{
				Name: "AMD Rome",
			},
			want: cpuPlatformInfo{
				name:    "AMD Rome",
				aliases: nil,
				vendor:  "",
				order:   0,
			},
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ToCpuPlatformInfoObject(tc.input)
			if (err != nil) != tc.wantErr {
				t.Errorf("ToCpuPlatformInfoObject() unexpected error = %v, wantErr = %v", err, tc.wantErr)
				return
			}
			if tc.wantErr {
				return
			}
			if diff := cmp.Diff(tc.want, got, cmp.AllowUnexported(cpuPlatformInfo{})); diff != "" {
				t.Errorf("ToCpuPlatformInfoObject() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCollectCPUPlatforms(t *testing.T) {
	testCases := []struct {
		name  string
		input *mcv1.MachineFamily
		want  map[string]mcv1.CPUPlatform
	}{
		{
			name:  "nil input",
			input: nil,
			want:  nil,
		},
		{
			name:  "empty machine family",
			input: &mcv1.MachineFamily{},
			want:  map[string]mcv1.CPUPlatform{},
		},
		{
			name: "only default properties CPU platforms",
			input: &mcv1.MachineFamily{
				DefaultProperties: mcv1.MachineProperties{
					CPUPlatforms: []mcv1.CPUPlatform{
						{Name: "Intel Ice Lake"},
						{Name: "Intel Sapphire Rapids"},
					},
				},
			},
			want: map[string]mcv1.CPUPlatform{
				"Intel Ice Lake":        {Name: "Intel Ice Lake"},
				"Intel Sapphire Rapids": {Name: "Intel Sapphire Rapids"},
			},
		},
		{
			name: "default and machine type CPU platforms",
			input: &mcv1.MachineFamily{
				DefaultProperties: mcv1.MachineProperties{
					CPUPlatforms: []mcv1.CPUPlatform{
						{Name: "Intel Ice Lake"},
					},
				},
				MachineTypes: []mcv1.MachineType{
					{
						Name: "n2-standard-4",
						Properties: &mcv1.MachineProperties{
							CPUPlatforms: []mcv1.CPUPlatform{
								{Name: "Intel Cascade Lake"},
							},
						},
					},
					{
						Name: "n2-standard-8",
						Properties: &mcv1.MachineProperties{
							CPUPlatforms: []mcv1.CPUPlatform{
								{Name: "Intel Ice Lake"}, // duplicate
							},
						},
					},
					{
						Name:       "n2-standard-16",
						Properties: nil,
					},
				},
			},
			want: map[string]mcv1.CPUPlatform{
				"Intel Ice Lake":     {Name: "Intel Ice Lake"},
				"Intel Cascade Lake": {Name: "Intel Cascade Lake"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := CollectCPUPlatforms(tc.input)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("CollectCPUPlatforms() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestToMachineFamilyObject_ConfidentialVMConfigs(t *testing.T) {
	testCases := []struct {
		name                  string
		enableCvmSot          bool
		familyConfigs         *mcv1.ConfidentialNodeConfig
		typeConfigs           *mcv1.ConfidentialNodeConfig
		expectedFamilySupport map[string]bool
		expectedTypeSupport   map[string]bool
	}{
		{
			name:         "family level configs",
			enableCvmSot: true,
			familyConfigs: &mcv1.ConfidentialNodeConfig{
				Supported: ptr.To(true),
				Types: []mcv1.ConfidentialNodeType{
					{Type: "SEV"},
					{Type: "TDX"},
				},
			},
			expectedFamilySupport: map[string]bool{"SEV": true, "TDX": true},
		},
		{
			name:         "type level overrides",
			enableCvmSot: true,
			familyConfigs: &mcv1.ConfidentialNodeConfig{
				Supported: ptr.To(true),
				Types: []mcv1.ConfidentialNodeType{
					{Type: "SEV"},
				},
			},
			typeConfigs: &mcv1.ConfidentialNodeConfig{
				Supported: ptr.To(true),
				Types: []mcv1.ConfidentialNodeType{
					{Type: "TDX"},
				},
			},
			expectedFamilySupport: map[string]bool{"SEV": true},
			expectedTypeSupport:   map[string]bool{"TDX": true},
		},
		{
			name:                  "fallback to hardcoded list when crd is empty but sot is disabled",
			enableCvmSot:          false,
			familyConfigs:         nil,
			typeConfigs:           nil,
			expectedFamilySupport: map[string]bool{"SEV": true, "SEV_SNP": true},
			expectedTypeSupport:   nil,
		},
		{
			name:                  "fallback to hardcoded list when crd is empty and sot is enabled",
			enableCvmSot:          true,
			familyConfigs:         nil,
			typeConfigs:           nil,
			expectedFamilySupport: map[string]bool{"SEV": true, "SEV_SNP": true},
			expectedTypeSupport:   nil,
		},
		{
			name:         "dynamic config explicitly unsupported overrides legacy field",
			enableCvmSot: true,
			familyConfigs: &mcv1.ConfidentialNodeConfig{
				Supported: ptr.To(false),
			},
			expectedFamilySupport: nil,
			expectedTypeSupport:   nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			input := &mcv1.MachineFamily{
				Name: "n2d",
				DefaultProperties: mcv1.MachineProperties{
					SupportsConfidentialNodes: ptr.To(true),
					ConfidentialNodeConfig:    tc.familyConfigs,
				},
				MachineTypes: []mcv1.MachineType{
					{
						Name: "n2d-standard-2",
						Resources: mcv1.MachineResources{
							CPUs:   2,
							Memory: 8192,
						},
						Properties: &mcv1.MachineProperties{
							SupportsConfidentialNodes: ptr.To(true),
							ConfidentialNodeConfig:    tc.typeConfigs,
						},
					},
				},
			}

			got, gradedErr := ToMachineFamilyObject(input, newCpuPlatformsSource(), tc.enableCvmSot)
			if gradedErr.Err != nil {
				t.Fatalf("ToMachineFamilyObject() unexpected error = %v", gradedErr.Err)
			}

			if diff := cmp.Diff(tc.expectedFamilySupport, got.supportConfidentialNodeTypes); diff != "" {
				t.Errorf("MachineFamily.supportConfidentialNodeTypes mismatch (-want +got):\n%s", diff)
			}

			mt, ok := got.autoprovisionedMachineTypes["n2d-standard-2"]
			if !ok {
				t.Fatalf("Expected n2d-standard-2 to be in autoprovisionedMachineTypes")
			}

			var typeSupport map[string]bool
			if mt.confidentialNodeCfg != nil {
				typeSupport = mt.confidentialNodeCfg.supportConfidentialNodeTypes
			}
			if diff := cmp.Diff(tc.expectedTypeSupport, typeSupport); diff != "" {
				t.Errorf("MachineType.supportConfidentialNodeTypes mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
