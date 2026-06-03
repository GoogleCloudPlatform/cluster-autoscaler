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
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
)

func TestValidatePlatformConfig(t *testing.T) {
	orderValidation := map[string]map[int]bool{}
	for platformName, platform := range cpuPlatforms.platformByName {
		t.Run(string(platformName), func(t *testing.T) {
			// Check if the name with underscores instead of spaces can be used to convert back to the object.
			platformFromName, err := ToCpuPlatform(strings.ReplaceAll(string(platformName), " ", "_"))
			assert.Nil(t, err)
			assert.Equal(t, platform.name, platformFromName)
			// Check if the correct name is returned for the platform.
			assert.Equal(t, string(platformName), CanonicalCpuPlatformName(platformFromName, false))
			// Check if the platform has a vendor assigned.
			assert.True(t, platform.vendor != "")
			// Check if the platform has a unique order assigned.
			assert.False(t, orderValidation[platform.vendor][platform.order])
			// Check if the platform doesn't pass validation for noPlatformSupported.
			assert.False(t, noPlatformSupported.validate(platform.name))

			// Test accounting.
			if _, found := orderValidation[platform.vendor]; !found {
				orderValidation[platform.vendor] = map[int]bool{}
			}
			orderValidation[platform.vendor][platform.order] = true
		})
	}
}

func TestToCpuPlatform(t *testing.T) {
	for tn, tc := range map[string]struct {
		platformName     string
		expectedPlatform CpuPlatform
		shouldFail       bool
	}{
		"supported platform": {
			platformName:     "Intel Ice Lake",
			expectedPlatform: IntelIceLake,
			shouldFail:       false,
		},
		"underscores instead of spaces still supported": {
			platformName:     "Intel_Ice_Lake",
			expectedPlatform: IntelIceLake,
			shouldFail:       false,
		},
		"unknown platform": {
			platformName:     "not known",
			expectedPlatform: UnknownPlatform,
			shouldFail:       true,
		},
		"case matters": {
			platformName:     "intel ice lake",
			expectedPlatform: UnknownPlatform,
			shouldFail:       true,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			platform, err := ToCpuPlatform(tc.platformName)
			assert.Equal(t, tc.expectedPlatform, platform)
			if tc.shouldFail {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCanonicalCpuPlatformName(t *testing.T) {
	for tn, tc := range map[string]struct {
		platform     CpuPlatform
		underscores  bool
		expectedName string
	}{
		"supported platform": {
			platform:     IntelIceLake,
			underscores:  false,
			expectedName: "Intel Ice Lake",
		},
		"supported platform with underscores": {
			platform:     IntelIceLake,
			underscores:  true,
			expectedName: "Intel_Ice_Lake",
		},
		"AnyPlatform": {
			platform:     AnyPlatform,
			expectedName: "",
		},
		"UnknownPlatform": {
			platform:     UnknownPlatform,
			expectedName: "",
		},
		"unexpected platform": {
			platform:     "unexpected platform",
			expectedName: "",
		},
	} {
		t.Run(tn, func(t *testing.T) {
			assert.Equal(t, tc.expectedName, CanonicalCpuPlatformName(tc.platform, tc.underscores))
		})
	}
}

func TestCpuPlatformDebugName(t *testing.T) {
	for tn, tc := range map[string]struct {
		platform     CpuPlatform
		expectedName string
	}{
		"supported platform": {
			platform:     IntelIceLake,
			expectedName: "Intel Ice Lake",
		},
		"AnyPlatform": {
			platform:     AnyPlatform,
			expectedName: string(AnyPlatform),
		},
		"UnknownPlatform": {
			platform:     UnknownPlatform,
			expectedName: string(UnknownPlatform),
		},
		"unexpected platform": {
			platform:     "unexpected platform",
			expectedName: "",
		},
	} {
		t.Run(tn, func(t *testing.T) {
			assert.Equal(t, tc.expectedName, CpuPlatformDebugName(tc.platform))
		})
	}
}

func TestValidate(t *testing.T) {
	for tn, tc := range map[string]struct {
		platform     CpuPlatform
		requirements CpuPlatformRequirements
		expected     bool
	}{
		"UnknownPlatform is never valid": {
			platform:     UnknownPlatform,
			requirements: noPlatformSupported,
			expected:     false,
		},
		"AnyPlatform is always valid": {
			platform:     AnyPlatform,
			requirements: noPlatformSupported,
			expected:     true,
		},
		"valid platform between bounds": {
			platform:     IntelSkylake,
			requirements: CpuPlatformRequirements{lowerBound: IntelBroadwell, upperBound: IntelCascadeLake},
			expected:     true,
		},
		"valid platform at the edge of bounds": {
			platform:     IntelSkylake,
			requirements: CpuPlatformRequirements{lowerBound: IntelSkylake, upperBound: IntelCascadeLake},
			expected:     true,
		},
		"valid platform singleton": {
			platform:     IntelSkylake,
			requirements: CpuPlatformRequirements{lowerBound: IntelSkylake, upperBound: IntelSkylake},
			expected:     true,
		},
		"invalid platform": {
			platform:     IntelSkylake,
			requirements: CpuPlatformRequirements{lowerBound: IntelCascadeLake, upperBound: IntelIceLake},
			expected:     false,
		},
		"invalid platform across manufacturers": {
			platform:     IntelSkylake,
			requirements: CpuPlatformRequirements{lowerBound: AmdRome, upperBound: AmdMilan},
			expected:     false,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			got := tc.requirements.validate(tc.platform)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestIsPlatformAtLeast(t *testing.T) {
	for tn, tc := range map[string]struct {
		platform    CpuPlatform
		minPlatform CpuPlatform
		want        bool
	}{
		"UnknownPlatform toKnow -> false": {
			platform:    UnknownPlatform,
			minPlatform: IntelSkylake,
			want:        false,
		},
		"UnknownPlatform base -> false": {
			platform:    IntelSkylake,
			minPlatform: UnknownPlatform,
			want:        false,
		},
		"AnyPlatform toKnow -> false": {
			platform:    AnyPlatform,
			minPlatform: IntelSkylake,
			want:        false,
		},
		"AnyPlatform base -> false": {
			platform:    IntelSkylake,
			minPlatform: AnyPlatform,
			want:        false,
		},
		"invalid enum toKnow -> false": {
			platform:    "unexpected platform",
			minPlatform: IntelSkylake,
			want:        false,
		},
		"invalid enum base -> false": {
			platform:    IntelSkylake,
			minPlatform: "unexpected platform",
			want:        false,
		},
		"platforms from different orders -> false": {
			platform:    AmdMilan,
			minPlatform: IntelSkylake,
			want:        false,
		},
		"platforms same order, toKnown > base -> true": {
			platform:    AmdMilan,
			minPlatform: AmdRome,
			want:        true,
		},
		"platforms same order, toKnown == base  -> true": {
			platform:    AmdMilan,
			minPlatform: AmdMilan,
			want:        true,
		},
		"platforms same order, toKnown < base -> false": {
			platform:    AmdRome,
			minPlatform: AmdMilan,
			want:        false,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			got := cpuPlatforms.isAtLeast(tc.platform, tc.minPlatform)
			if got != tc.want {
				t.Errorf("IsPlatformAtLeast diff: want %v, got %v", tc.want, got)
			}
		})
	}
}

func TestPlatformsLowerOrEqualTo(t *testing.T) {
	for tn, tc := range map[string]struct {
		platform CpuPlatform
		want     []CpuPlatform
	}{
		"lowest platform": {
			platform: AmdRome,
			want:     []CpuPlatform{AmdRome},
		},
		"highest platform": {
			platform: IntelGraniteRapids,
			want:     []CpuPlatform{IntelSandyBridge, IntelIvyBridge, IntelHaswell, IntelBroadwell, IntelSkylake, IntelCascadeLake, IntelIceLake, IntelSapphireRapids, IntelEmeraldRapids, IntelGraniteRapids},
		},
		"middle platform": {
			platform: IntelHaswell,
			want:     []CpuPlatform{IntelSandyBridge, IntelIvyBridge, IntelHaswell},
		},
		"only platform": {
			platform: AmpereAltra,
			want:     []CpuPlatform{AmpereAltra},
		},
		"platform without an order defined (shouldn't happen, sanity check)": {
			platform: "unexpected platform",
			want:     []CpuPlatform{"unexpected platform"},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			got := PlatformsLowerOrEqualTo(tc.platform)
			if diff := cmp.Diff(tc.want, got, cmpopts.SortSlices(func(a, b CpuPlatform) bool { return a < b })); diff != "" {
				t.Errorf("PlatformsLowerOrEqualTo(%v) diff (-want +got):\n%s", tc.platform, diff)
			}
		})
	}
}

func TestGetOrDefaultMinCPUPlatform(t *testing.T) {
	testCases := []struct {
		name               string
		machineType        string
		minCPuPlatform     string
		wantMinCpuPlatform CpuPlatform
	}{
		{
			name:               "unknown machine type, unknown min cpu platform",
			machineType:        "some-machine-type",
			minCPuPlatform:     "some-min-cpu-platform",
			wantMinCpuPlatform: AnyPlatform,
		},
		{
			name:               "unknown machine type, canonical min cpu platform name",
			machineType:        "some-machine-type",
			minCPuPlatform:     "Intel Ice Lake",
			wantMinCpuPlatform: IntelIceLake,
		},
		{
			name:               "unknown machine type, non-canonical min cpu platform name",
			machineType:        "some-machine-type",
			minCPuPlatform:     "icelake",
			wantMinCpuPlatform: IntelIceLake,
		},
		{
			name:               "e2-standard-4, unknown min cpu platform name",
			machineType:        "e2-standard-4",
			minCPuPlatform:     "some-min-cpu-platform",
			wantMinCpuPlatform: AnyPlatform,
		},
		{
			name:               "e2-standard-4, known min cpu platform name",
			machineType:        "e2-standard-4",
			minCPuPlatform:     "Intel Ice Lake",
			wantMinCpuPlatform: IntelIceLake,
		},
		{
			name:               "unknown e2 machine type, unknown min cpu platform name",
			machineType:        "e2-something",
			minCPuPlatform:     "some-min-cpu-platform",
			wantMinCpuPlatform: AnyPlatform,
		},
		{
			name:               "unknown e2 machine type, known min cpu platform name",
			machineType:        "e2-something",
			minCPuPlatform:     "Intel Ice Lake",
			wantMinCpuPlatform: IntelIceLake,
		},
		{
			name:               "n2-standard-4, unknown min cpu platform name",
			machineType:        "n2-standard-4",
			minCPuPlatform:     "some-min-cpu-platform",
			wantMinCpuPlatform: IntelCascadeLake,
		},
		{
			name:               "n2-standard-4, known min cpu platform name",
			machineType:        "n2-standard-4",
			minCPuPlatform:     "Intel Ice Lake",
			wantMinCpuPlatform: IntelIceLake,
		},
		{
			name:               "n2-standard-4, known impossible min cpu platform name (we don't validate)",
			machineType:        "n2-standard-4",
			minCPuPlatform:     "AMD Rome",
			wantMinCpuPlatform: AmdRome,
		},
		{
			name:               "n2-standard-128, unknown min cpu platform name",
			machineType:        "n2-standard-128",
			minCPuPlatform:     "some-min-cpu-platform",
			wantMinCpuPlatform: IntelIceLake,
		},
		{
			name:               "unknown n2 machine type, unknown min cpu platform name",
			machineType:        "n2-something",
			minCPuPlatform:     "some-min-cpu-platform",
			wantMinCpuPlatform: IntelCascadeLake,
		},
		{
			name:               "unknown n2 machine type, known min cpu platform name",
			machineType:        "n2-something",
			minCPuPlatform:     "Intel Ice Lake",
			wantMinCpuPlatform: IntelIceLake,
		},
		{
			name:               "n2d-standard-4, unknown min cpu platform name",
			machineType:        "n2d-standard-4",
			minCPuPlatform:     "some-min-cpu-platform",
			wantMinCpuPlatform: AmdRome,
		},
		{
			name:               "n2d-standard-4, known min cpu platform name",
			machineType:        "n2d-standard-4",
			minCPuPlatform:     "Amd Milan",
			wantMinCpuPlatform: AmdMilan,
		},
		{
			name:               "unknown n2d machine type, unknown min cpu platform name",
			machineType:        "n2d-something",
			minCPuPlatform:     "some-min-cpu-platform",
			wantMinCpuPlatform: AmdRome,
		},
		{
			name:               "unknown n2d machine type, known min cpu platform name",
			machineType:        "n2d-something",
			minCPuPlatform:     "Amd Milan",
			wantMinCpuPlatform: AmdMilan,
		},
		{
			name:               "t2a-standard-4, unknown min cpu platform name",
			machineType:        "t2a-standard-4",
			minCPuPlatform:     "some-min-cpu-platform",
			wantMinCpuPlatform: AmpereAltra,
		},
		{
			name:               "t2a-standard-4, known min cpu platform name",
			machineType:        "t2a-standard-4",
			minCPuPlatform:     "Ampere Altra",
			wantMinCpuPlatform: AmpereAltra,
		},
		{
			name:               "unknown t2a machine type, unknown min cpu platform name",
			machineType:        "t2a-something",
			minCPuPlatform:     "some-min-cpu-platform",
			wantMinCpuPlatform: AmpereAltra,
		},
		{
			name:               "unknown t2a machine type, known min cpu platform name",
			machineType:        "t2a-something",
			minCPuPlatform:     "Ampere Altra",
			wantMinCpuPlatform: AmpereAltra,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mcp := NewMachineConfigProvider(nil)
			assert.Equal(t, tc.wantMinCpuPlatform, mcp.GetOrDefaultMinCPUPlatform(tc.machineType, tc.minCPuPlatform))
		})
	}
}

func TestCpuPlatformsSourceDynamicRegistration(t *testing.T) {
	src := newCpuPlatformsSource()

	// Initially empty
	_, found := src.get("Intel Ice Lake")
	assert.False(t, found)

	p1 := cpuPlatformInfo{
		name:    "Dynamic Intel",
		aliases: []CpuPlatform{"dyn-intel", "dynamic_intel"},
		vendor:  "Intel",
		order:   1,
	}
	p2 := cpuPlatformInfo{
		name:    "Dynamic AMD",
		aliases: []CpuPlatform{"dyn-amd"},
		vendor:  "AMD",
		order:   2,
	}

	src.registerDynamic(p1)
	src.registerDynamic(p2)

	// Verify lookup by name
	gotP1, found := src.get("Dynamic Intel")
	assert.True(t, found)
	assert.Equal(t, p1, gotP1)

	// Verify lookup by alias
	gotP1Alias, found := src.get("dyn-intel")
	assert.True(t, found)
	assert.Equal(t, p1, gotP1Alias)

	gotP1Alias2, found := src.get("dynamic_intel")
	assert.True(t, found)
	assert.Equal(t, p1, gotP1Alias2)

	// Verify lookup for p2
	gotP2, found := src.get("dyn-amd")
	assert.True(t, found)
	assert.Equal(t, p2, gotP2)
}

func TestCpuPlatformsSourceConcurrent(t *testing.T) {
	src := newCpuPlatformsSource()
	var waitGroup sync.WaitGroup

	numWorkers := 20
	numOps := 100

	for i := range numWorkers {
		waitGroup.Add(1)
		go func(workerID int) {
			defer waitGroup.Done()
			for j := range numOps {
				name := CpuPlatform(fmt.Sprintf("platform-%d-%d", workerID, j))
				platform := cpuPlatformInfo{
					name:    name,
					aliases: []CpuPlatform{CpuPlatform(fmt.Sprintf("alias-%d-%d", workerID, j))},
					vendor:  "Intel",
					order:   workerID*numOps + j,
				}
				// Mix of register and get
				src.registerDynamic(platform)
				src.get(name)
				src.isAtLeast(name, name)
			}
		}(i)
	}

	waitGroup.Wait()
}
