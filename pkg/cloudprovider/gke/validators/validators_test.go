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

package validators

import (
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"

	gceapi "google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"

	"k8s.io/utils/ptr"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
)

const (
	machineTypeN1             = "n1-standard-1"
	machineTypeA2             = "a2-highgpu-1g"
	predefinedCustomType      = "custom-48-184320"
	customType1               = "n2-custom-80-532480"
	customType2               = "custom-16-32768"
	customMachineTypeGeneric1 = "XYZ-custom-96-1840400"
	customMachineTypeN1       = "custom-24-65536"
	customMachineTypeN2       = "n2-custom-8-65536"
	customMachineTypeN2D      = "n2d-custom-8-65536"

	genericZone = "generic-zone"
)

var (
	testKnownCustomTypes = map[string]bool{customType1: true, customType2: true}

	testZoneMachineTypes = map[string]map[string]bool{
		"europe-west1-b": {
			machineTypeN1: true,
			customType1:   true,
		},
		"us-central1-a": {
			machineTypeN1:       true,
			customMachineTypeN1: true,
		},
		"us-central1-b": {
			machineTypeN1:       true,
			customMachineTypeN1: true,
			customMachineTypeN2: true,
		},
		"us-central1-c": {
			machineTypeN1: true,
			machineTypeA2: true,
			customType1:   true,
			customType2:   true,
		},
		"us-central1-f": {
			machineTypeN1:        true,
			predefinedCustomType: true,
			customType2:          true,
			customMachineTypeN1:  true,
			customMachineTypeN2D: true,
		},
		"us-west1-b": {
			machineTypeN1: true,
			machineTypeA2: true,
		},
		"mini-zone": {
			machineTypeN1: true,
		},
		"castle-drekmore": {},
		genericZone: {
			customMachineTypeGeneric1: true,
		},
	}

	testZoneGpuCounts = map[string]map[string]int64{
		"europe-west1-b": {
			machinetypes.NvidiaTeslaK80.Name():  8,
			machinetypes.NvidiaTeslaP100.Name(): 4,
		},
		"us-central1-a": {
			machinetypes.NvidiaTeslaK80.Name():  8,
			machinetypes.NvidiaTeslaV100.Name(): 8,
			machinetypes.NvidiaTeslaP4.Name():   4,
			machinetypes.NvidiaTeslaT4.Name():   4,
		},
		"us-central1-b": {
			machinetypes.NvidiaTeslaK80.Name(): 8,
		},
		"us-central1-c": {
			machinetypes.NvidiaTeslaK80.Name():  8,
			machinetypes.NvidiaTeslaA100.Name(): 8,
		},
		"us-central1-f": {
			machinetypes.NvidiaTeslaK80.Name(): 8,
		},
		"us-west1-b": {
			machinetypes.NvidiaTeslaK80.Name():  8,
			machinetypes.NvidiaTeslaA100.Name(): 1,
		},
		"mini-zone": {
			machinetypes.NvidiaTeslaK80.Name(): 1,
		},
		"castle-drekmore": {},
	}
)

type fakeMachineTypeFetcher struct {
	fetchFun         func() error
	zoneMachineTypes *map[string]map[string]bool
}

func (f *fakeMachineTypeFetcher) FetchMachineType(zone, machineType string) (*gceapi.MachineType, error) {
	if err := f.fetchFun(); err != nil {
		return nil, err
	}
	if f.zoneMachineTypes == nil {
		return nil, fmt.Errorf("no zones available")
	}
	zoneTypes := (*f.zoneMachineTypes)[zone]
	typeFound := zoneTypes[machineType]
	if typeFound {
		return &gceapi.MachineType{Name: machineType}, nil
	}
	return nil, &googleapi.Error{Code: http.StatusBadRequest}
}

func (f *fakeMachineTypeFetcher) FetchMachineTypes(zone string) ([]*gceapi.MachineType, error) {
	if err := f.fetchFun(); err != nil {
		return nil, err
	}
	if f.zoneMachineTypes == nil {
		return nil, fmt.Errorf("no zones available")
	}
	zoneTypes := (*f.zoneMachineTypes)[zone]
	var result []*gceapi.MachineType
	for typeName := range zoneTypes {
		if !gce.IsCustomMachine(typeName) {
			result = append(result, &gceapi.MachineType{Name: typeName})
		}
	}
	return result, nil
}

// fakeAdvancedMachineTypeFetcher gives more control over fetch functions.
// Respective parameters are passed to the fetch functions.
type fakeAdvancedMachineTypeFetcher struct {
	zoneMachineTypes          *map[string]map[string]bool
	fetchMachineTypeFunction  func(zone, machineType string) error
	fetchZoneMachinesFunction func(zone string) error
}

func (f *fakeAdvancedMachineTypeFetcher) FetchMachineType(zone, machineType string) (*gceapi.MachineType, error) {
	if err := f.fetchMachineTypeFunction(zone, machineType); err != nil {
		return nil, err
	}
	if f.zoneMachineTypes == nil {
		return nil, fmt.Errorf("no zones available")
	}
	zoneTypes := (*f.zoneMachineTypes)[zone]
	typeFound := zoneTypes[machineType]
	if typeFound {
		return &gceapi.MachineType{Name: machineType}, nil
	}
	return nil, &googleapi.Error{Code: http.StatusBadRequest}
}

func (f *fakeAdvancedMachineTypeFetcher) FetchMachineTypes(zone string) ([]*gceapi.MachineType, error) {
	if err := f.fetchZoneMachinesFunction(zone); err != nil {
		return nil, err
	}
	if f.zoneMachineTypes == nil {
		return nil, fmt.Errorf("no zones available")
	}
	zoneTypes := (*f.zoneMachineTypes)[zone]
	var result []*gceapi.MachineType
	for typeName := range zoneTypes {
		if !gce.IsCustomMachine(typeName) {
			result = append(result, &gceapi.MachineType{Name: typeName})
		}
	}
	return result, nil
}

func buildCachedMachineConfigValidator(
	fetch func(),
	zoneMachineTypes *map[string]map[string]bool,
	zoneGpuCounts *map[string]map[string]int64,
	customTypes map[string]bool,
	httpTimeout time.Duration,
	expectedError error,
) *cachedMachineConfigValidator {
	gceInternalService := gceclient.BuildAutoscalingInternalGceClientMock().
		WithGetZoneGpuCounts(func() (map[string]map[string]int64, error) { fetch(); return *zoneGpuCounts, expectedError }).
		WithHttpTimeout(httpTimeout)
	fakeMachineFetcher := &fakeMachineTypeFetcher{fetchFun: func() error { fetch(); return expectedError }, zoneMachineTypes: zoneMachineTypes}
	validator := NewCachedMachineConfigValidator(gceInternalService, fakeMachineFetcher, machinetypes.NewMachineConfigProvider(nil))
	// Override cache state.
	validator.customMachineTypes = customTypes
	return validator
}

// Result validator uses fakeAdvancedMachineTypeFetcher.
// All three fetch functions are directly passed.
func buildAdvancedCachedMachineConfigValidator(
	getZoneGpuCounts func() (map[string]map[string]int64, error),
	fetchMachineTypeFunction func(zone, machineType string) error,
	fetchZoneMachinesFunction func(zone string) error,
	zoneMachineTypes *map[string]map[string]bool,
	customTypes map[string]bool,
	httpTimeout time.Duration,
) *cachedMachineConfigValidator {
	gceInternalService := gceclient.BuildAutoscalingInternalGceClientMock().
		WithGetZoneGpuCounts(getZoneGpuCounts).
		WithHttpTimeout(httpTimeout)
	fakeMachineFetcher := &fakeAdvancedMachineTypeFetcher{
		fetchMachineTypeFunction:  fetchMachineTypeFunction,
		fetchZoneMachinesFunction: fetchZoneMachinesFunction,
		zoneMachineTypes:          zoneMachineTypes,
	}
	return NewCachedMachineConfigValidator(gceInternalService, fakeMachineFetcher, machinetypes.NewMachineConfigProvider(nil))
}

func fillValidatorCache(validator *cachedMachineConfigValidator) {
	for zone := range testZoneMachineTypes {
		machineTypes := testZoneMachineTypes[zone]
		gpuCounts := testZoneGpuCounts[zone]
		validator.configCaches[zone] = machineConfigCache{
			expires:      time.Now().Add(maxCacheValidity),
			machineTypes: machineTypes,
			gpuCounts:    gpuCounts,
		}
	}
}

func TestCachedGpuValidator(t *testing.T) {
	filledValidator := buildCachedMachineConfigValidator(func() { t.Fatal("The fetch should not be run") }, &testZoneMachineTypes, &testZoneGpuCounts, testKnownCustomTypes, time.Second, nil)
	fillValidatorCache(filledValidator)
	emptyValidator := buildCachedMachineConfigValidator(func() {}, &testZoneMachineTypes, &testZoneGpuCounts, testKnownCustomTypes, time.Second, nil)

	tests := []struct {
		gpuType             string
		gpuPartitionSize    string
		gpuMaxSharedClients string
		gpuSharingStrategy  string
		machineType         string
		gpuCount            int64
		zone                string
		cpuCount            int64
		memCount            int64
		expectedErr         bool
	}{
		// valid configs
		{
			gpuType:     machinetypes.NvidiaTeslaK80.Name(),
			machineType: machineTypeN1,
			gpuCount:    1,
			zone:        "europe-west1-b",
			cpuCount:    1,
			memCount:    1,
		},
		{
			gpuType:     machinetypes.NvidiaTeslaK80.Name(),
			machineType: customMachineTypeN1,
			gpuCount:    4,
			zone:        "europe-west1-b",
			cpuCount:    24,
			memCount:    32,
		},
		{
			gpuType:     machinetypes.NvidiaTeslaP100.Name(),
			machineType: machineTypeN1,
			gpuCount:    1,
			zone:        "europe-west1-b",
			cpuCount:    1,
			memCount:    1,
		},
		{
			gpuType:     machinetypes.NvidiaTeslaK80.Name(),
			machineType: machineTypeN1,
			gpuCount:    8,
			zone:        "europe-west1-b",
			cpuCount:    32,
			memCount:    1,
		},
		{
			gpuType:     machinetypes.NvidiaTeslaA100.Name(),
			machineType: machineTypeA2,
			gpuCount:    1,
			zone:        "us-central1-c",
			cpuCount:    1,
			memCount:    1,
		},
		{
			gpuType:          machinetypes.NvidiaTeslaA100.Name(),
			gpuPartitionSize: "1g.5gb",
			machineType:      machineTypeA2,
			gpuCount:         7,
			zone:             "us-central1-c",
			cpuCount:         1,
			memCount:         1,
		},
		// invalid gpu
		{
			gpuType:     "duke-igthorn",
			machineType: machineTypeN1,
			gpuCount:    1,
			zone:        "europe-west1-b",
			cpuCount:    1,
			memCount:    1,
			expectedErr: true,
		},
		// invalid gpu for specific zone
		{
			gpuType:     machinetypes.NvidiaTeslaP100.Name(),
			machineType: machineTypeN1,
			gpuCount:    1,
			zone:        "us-central1-a",
			cpuCount:    1,
			memCount:    1,
			expectedErr: true,
		},
		// invalid zone
		{
			gpuType:     machinetypes.NvidiaTeslaK80.Name(),
			machineType: machineTypeN1,
			gpuCount:    1,
			zone:        "castle-drekmore",
			cpuCount:    1,
			memCount:    1,
			expectedErr: true,
		},
		// too many gpus
		{
			gpuType:     machinetypes.NvidiaTeslaP4.Name(),
			machineType: machineTypeN1,
			gpuCount:    8,
			zone:        "europe-west1-b",
			cpuCount:    1,
			memCount:    1,
			expectedErr: true,
		},
		// 1 gpu with large machine
		{
			gpuType:     machinetypes.NvidiaTeslaK80.Name(),
			machineType: machineTypeN1,
			gpuCount:    1,
			zone:        "europe-west1-b",
			cpuCount:    32,
			memCount:    1,
			expectedErr: true,
		},
		// A2 machine but non A100 gpu
		{
			gpuType:     machinetypes.NvidiaTeslaK80.Name(),
			machineType: machineTypeA2,
			gpuCount:    8,
			zone:        "us-central1-c",
			cpuCount:    1,
			memCount:    1,
			expectedErr: true,
		},
		// Non A2 machine but A100 gpu
		{
			gpuType:     machinetypes.NvidiaTeslaA100.Name(),
			machineType: machineTypeN1,
			gpuCount:    1,
			zone:        "us-central1-c",
			cpuCount:    1,
			memCount:    1,
			expectedErr: true,
		},
		// Incorrect A100 gpu partitioning size
		{
			gpuType:          machinetypes.NvidiaTeslaA100.Name(),
			gpuPartitionSize: "gummy-juice-flasks",
			machineType:      machineTypeA2,
			gpuCount:         1,
			zone:             "us-central1-c",
			cpuCount:         1,
			memCount:         1,
			expectedErr:      true,
		},
		// Incorrect A100 gpu count for given partitioning size
		{
			gpuType:          machinetypes.NvidiaTeslaA100.Name(),
			gpuPartitionSize: "1g.5gb",
			machineType:      machineTypeA2,
			gpuCount:         8,
			zone:             "us-central1-c",
			cpuCount:         1,
			memCount:         1,
			expectedErr:      true,
		},
		// gpu count in the limit
		{
			gpuType:     machinetypes.NvidiaTeslaK80.Name(),
			machineType: machineTypeN1,
			gpuCount:    1,
			zone:        "mini-zone",
			cpuCount:    1,
			memCount:    1,
			expectedErr: false,
		},
		// gpu count over the limit
		{
			gpuType:     machinetypes.NvidiaTeslaK80.Name(),
			machineType: machineTypeN1,
			gpuCount:    8,
			zone:        "mini-zone",
			cpuCount:    1,
			memCount:    1,
			expectedErr: true,
		},
		// correct gpu time-sharing set up.
		{
			gpuType:             machinetypes.NvidiaTeslaK80.Name(),
			machineType:         machineTypeN1,
			gpuCount:            2,
			zone:                "mini-zone",
			cpuCount:            1,
			memCount:            1,
			gpuMaxSharedClients: "2",
			gpuSharingStrategy:  "time-sharing",
			expectedErr:         false,
		},
		// Incorrect gpu sharing strategy.
		{
			gpuType:             machinetypes.NvidiaTeslaK80.Name(),
			machineType:         machineTypeN1,
			gpuCount:            2,
			zone:                "mini-zone",
			cpuCount:            1,
			memCount:            1,
			gpuMaxSharedClients: "2",
			gpuSharingStrategy:  "time-sharing2",
			expectedErr:         true,
		},
		// Incorrect gpu max shared clients value (too high value).
		{
			gpuType:             machinetypes.NvidiaTeslaK80.Name(),
			machineType:         machineTypeN1,
			gpuCount:            2,
			zone:                "mini-zone",
			cpuCount:            1,
			memCount:            1,
			gpuMaxSharedClients: "49",
			gpuSharingStrategy:  "time-sharing",
			expectedErr:         true,
		},
		// Incorrect gpu max shared clients value (non numeric value).
		{
			gpuType:             machinetypes.NvidiaTeslaK80.Name(),
			machineType:         machineTypeN1,
			gpuCount:            2,
			zone:                "mini-zone",
			cpuCount:            1,
			memCount:            1,
			gpuMaxSharedClients: "abc",
			gpuSharingStrategy:  "time-sharing",
			expectedErr:         true,
		},
	}

	for _, tc := range tests {
		filledErr := filledValidator.ValidateGpuConfig(tc.gpuType, tc.gpuPartitionSize, tc.gpuMaxSharedClients, tc.gpuSharingStrategy, tc.machineType, tc.gpuCount, tc.zone, tc.cpuCount, tc.memCount)
		emptyErr := emptyValidator.ValidateGpuConfig(tc.gpuType, tc.gpuPartitionSize, tc.gpuMaxSharedClients, tc.gpuSharingStrategy, tc.machineType, tc.gpuCount, tc.zone, tc.cpuCount, tc.memCount)
		if tc.expectedErr {
			assert.Error(t, filledErr)
			assert.Error(t, emptyErr)
		} else {
			assert.NoError(t, filledErr)
			assert.NoError(t, emptyErr)
		}
	}
}

func TestCachedMachineTypeValidator(t *testing.T) {
	filledValidator := buildCachedMachineConfigValidator(func() { t.Fatal("The fetch should not be run") }, &testZoneMachineTypes, &testZoneGpuCounts, testKnownCustomTypes, time.Second, nil)
	fillValidatorCache(filledValidator)
	emptyValidator := buildCachedMachineConfigValidator(func() {}, &testZoneMachineTypes, &testZoneGpuCounts, testKnownCustomTypes, time.Second, nil)

	tests := []struct {
		machineType string
		zone        string
		expectedErr bool
	}{
		// valid configs
		{
			machineType: machineTypeN1,
			zone:        "us-central1-c",
			expectedErr: false,
		},
		{
			machineType: machineTypeA2,
			zone:        "us-central1-c",
			expectedErr: false,
		},
		{
			machineType: machineTypeN1,
			zone:        "us-central1-f",
			expectedErr: false,
		},
		{
			machineType: customType1,
			zone:        "us-central1-c",
			expectedErr: false,
		},
		{
			machineType: customType2,
			zone:        "us-central1-c",
			expectedErr: false,
		},
		{
			machineType: customType2,
			zone:        "us-central1-f",
			expectedErr: false,
		},
		// not existing machine type
		{
			machineType: "gummy-bear-i-guess?",
			zone:        "us-central1-c",
			expectedErr: true,
		},
		// not existing zone
		{
			machineType: machineTypeN1,
			zone:        "castle-drekmore",
			expectedErr: true,
		},
		// not available in zone
		{
			machineType: machineTypeA2,
			zone:        "us-central1-f",
			expectedErr: true,
		},
		{
			machineType: customType1,
			zone:        "us-central1-f",
			expectedErr: true,
		},
		{
			machineType: customType2,
			zone:        "us-central1-b",
			expectedErr: true,
		},
	}

	for _, tc := range tests {
		filledErr := filledValidator.ValidateMachineTypeConfig(tc.machineType, tc.zone)
		emptyErr := emptyValidator.ValidateMachineTypeConfig(tc.machineType, tc.zone)
		if tc.expectedErr {
			assert.Error(t, filledErr)
			assert.Error(t, emptyErr)
		} else {
			assert.NoError(t, filledErr)
			assert.NoError(t, emptyErr)
		}
	}
}

func TestCustomMachineTypeValidation(t *testing.T) {
	tests := map[string]struct {
		machineType        string
		zone               string
		machineTypeError   error
		wantError          bool
		wantErrorSubstring *string
	}{
		"valid generic unknown machine type in a generic zone, no fetching error": {
			machineType:      customMachineTypeGeneric1,
			zone:             genericZone,
			machineTypeError: nil,
		},
		"invalid unknown machine type, fetching error: bad status request": {
			machineType:        customMachineTypeGeneric1,
			zone:               genericZone,
			machineTypeError:   &googleapi.Error{Code: http.StatusBadRequest},
			wantError:          true,
			wantErrorSubstring: ptr.To(fmt.Sprintf("Machine type %s not available in %s zone", customMachineTypeGeneric1, genericZone)),
		},
		"unknown machine type fetching error: status bad gateway": {
			machineType:        customMachineTypeGeneric1,
			zone:               genericZone,
			machineTypeError:   &googleapi.Error{Code: http.StatusBadGateway},
			wantError:          true,
			wantErrorSubstring: ptr.To(fmt.Sprintf("Unexpected error while fetching custom machine type %q in zone %q", customMachineTypeGeneric1, genericZone)),
		},
		"unknown machine type generic fetching error": {
			machineType:        customMachineTypeGeneric1,
			zone:               genericZone,
			machineTypeError:   &googleapi.Error{Code: 123456789},
			wantError:          true,
			wantErrorSubstring: ptr.To(fmt.Sprintf("Unexpected error while fetching custom machine type %q in zone %q", customMachineTypeGeneric1, genericZone)),
		},
	}

	for testName, testCase := range tests {
		testCustomTypesCopy := maps.Clone(testKnownCustomTypes)
		t.Run(testName, func(t *testing.T) {
			validator := buildAdvancedCachedMachineConfigValidator(
				func() (map[string]map[string]int64, error) {
					return nil, nil
				},
				func(zone, machineType string) error {
					if testCase.machineType == machineType && testCase.zone == zone {
						return testCase.machineTypeError
					}
					return nil
				},
				func(zone string) error {
					return nil
				},
				&testZoneMachineTypes,
				testCustomTypesCopy,
				time.Second,
			)

			validationError := validator.ValidateMachineTypeConfig(testCase.machineType, testCase.zone)

			if testCase.wantError {
				assert.Error(t, validationError)

				if testCase.wantErrorSubstring == nil {
					return
				}

				if !strings.Contains(validationError.Error(), *testCase.wantErrorSubstring) {
					t.Errorf("Expected error to contain substring %q, but got %q", *testCase.wantErrorSubstring, validationError.Error())
				}
				if gErr, ok := testCase.machineTypeError.(*googleapi.Error); ok && gErr.Code == http.StatusBadRequest {
					_, found := validator.customMachineTypes[testCase.machineType]
					assert.True(t, found, "Expected custom machine type %q to be in the validator customMachineTypes map", testCase.machineType)
					_, found = validator.configCaches[testCase.zone]
					assert.True(t, found, "Expected validator config cache to be present for the zone %s", testCase.zone)
					_, found = validator.configCaches[testCase.zone].machineTypes[testCase.machineType]
					assert.False(t, found, "Expected custom machine type %q not to be present in the validator config cache for zone %s", testCase.machineType, testCase.zone)
				} else {
					_, found := validator.customMachineTypes[testCase.machineType]
					assert.False(t, found, "Expected custom machine type %q not to be in the validator customMachineTypes map", testCase.machineType)
					_, found = validator.configCaches[testCase.zone]
					assert.False(t, found, "Expected validator config cache not to be present for the zone %s", testCase.zone)
				}
				return
			}
			assert.NoError(t, validationError)
			_, found := validator.customMachineTypes[testCase.machineType]
			assert.True(t, found, "Expected custom machine type %q to be in the validator customMachineTypes map", testCase.machineType)
			_, found = validator.configCaches[testCase.zone]
			assert.True(t, found, "Expected validator config cache to be present for the zone %s", testCase.zone)
			_, found = validator.configCaches[testCase.zone].machineTypes[testCase.machineType]
			assert.True(t, found, "Expected custom machine type %q to be present in the validator config cache for zone %s", testCase.machineType, testCase.zone)
		})
	}
}

func TestValidatePredefinedAndCustomMachineTypesSequence(t *testing.T) {
	testCustomTypesCopy := maps.Clone(testKnownCustomTypes)
	machineTypeFetches := make(chan bool, 10)
	machineTypesFetches := make(chan bool, 10)
	zoneA := "us-central1-a"
	zoneB := "us-central1-b"
	zoneF := "us-central1-f"
	fetchingErrorCustomMachineType := "e2-custom-16-131072"
	fetchingErrorZone := zoneB

	fetchMachineTypeFunction := func(zone, machineType string) error {
		machineTypeFetches <- true
		if zone == fetchingErrorZone && machineType == fetchingErrorCustomMachineType {
			return &googleapi.Error{Code: http.StatusServiceUnavailable}
		}
		return nil
	}
	fetchZoneMachinesFunction := func(zone string) error {
		machineTypesFetches <- true
		return nil
	}
	getZoneGpuCountFunction := func() (map[string]map[string]int64, error) {
		return nil, nil
	}

	validator := buildAdvancedCachedMachineConfigValidator(
		getZoneGpuCountFunction,
		fetchMachineTypeFunction,
		fetchZoneMachinesFunction,
		&testZoneMachineTypes,
		testCustomTypesCopy,
		time.Second,
	)

	expectedCacheMachineTypes := map[string]bool{
		machineTypeN1: true,
	}
	// Predefined machine type validation in zone A.
	err := validator.ValidateMachineTypeConfig(machineTypeN1, zoneA)
	assert.Equal(t, 4, countChan(machineTypeFetches))  // 4 calls for predefined custom machine types
	assert.Equal(t, 1, countChan(machineTypesFetches)) // 1 call for predefined machine types in zoneA
	assert.NoError(t, err)
	configCache, found := validator.configCaches[zoneA]
	assert.True(t, found)
	assert.Equal(t, expectedCacheMachineTypes, configCache.machineTypes)

	// Custom yet not known valid machine type (custom-24-65536) validation in zone B.
	expectedCacheMachineTypes = map[string]bool{
		machineTypeN1:       true,
		customMachineTypeN1: true,
	}
	err = validator.ValidateMachineTypeConfig(customMachineTypeN1, zoneB)
	// 1 call for unknown in zoneA
	// + 1 for unknown in zoneB
	// + 4 for predefined custom machine types for zoneB when updating zonal cache for zoneB
	// + 1 for newly added now known custom machine type when updating zonal cache for zoneB
	assert.Equal(t, 7, countChan(machineTypeFetches))
	assert.Equal(t, 1, countChan(machineTypesFetches)) // 1 call for the zone with predefined machine types
	assert.NoError(t, err)
	configCache, found = validator.configCaches[zoneB]
	assert.True(t, found) // Cache for zone B should be there
	assert.Equal(t, expectedCacheMachineTypes, configCache.machineTypes)
	configCache, found = validator.configCaches[zoneA]
	assert.True(t, found) // Cache for zone A should have also been updated (customMachineTypeN1 is valid in zone A as well)
	assert.Equal(t, expectedCacheMachineTypes, configCache.machineTypes)

	// Custom yet not known valid machine type (n2-custom-8-65536) validation in zone B.
	expectedCacheMachineTypes = map[string]bool{
		machineTypeN1:       true,
		customMachineTypeN1: true,
		customMachineTypeN2: true,
	}
	err = validator.ValidateMachineTypeConfig(customMachineTypeN2, zoneB)
	// 1 call for unknown in zoneA
	// + 1 for unknown in zoneB
	assert.Equal(t, 2, countChan(machineTypeFetches))
	assert.Equal(t, 0, countChan(machineTypesFetches)) // 0 calls for the zone with predefined machine types as cache is up to date
	assert.NoError(t, err)
	configCache, found = validator.configCaches[zoneB]
	assert.True(t, found) // Cache for zone B should be there
	assert.Equal(t, expectedCacheMachineTypes, configCache.machineTypes)

	// Custom yet not known valid machine type (n2d-custom-8-65536) validation in zone F.
	expectedCacheMachineTypes = map[string]bool{
		machineTypeN1:        true, // predefined machine type
		predefinedCustomType: true, // known from the beginning custom machine type
		customMachineTypeN1:  true, // known after second validation call
		customMachineTypeN2D: true, // not known yet custom machine type
	}
	err = validator.ValidateMachineTypeConfig(customMachineTypeN2D, zoneF)
	// 1 call for unknown in zoneA
	// + 1 for unknown in zoneB
	// + 1 for unknown in zoneF
	// + 4 for predefined custom machine types when updating zonal cache for zoneF
	// + 2 for originally not known but later added custom machine types (custom-24-65536 and n2-custom-8-65536) for zoneF when updating zonal cache for zoneF
	// + 1 for newly added now known custom machine type when updating zonal cache for zoneF
	assert.Equal(t, 10, countChan(machineTypeFetches))
	assert.Equal(t, 1, countChan(machineTypesFetches)) // 1 call for the zone with predefined machine types
	assert.NoError(t, err)
	configCache, found = validator.configCaches[zoneF]
	assert.True(t, found) // Cache for zone F should be there
	assert.Equal(t, expectedCacheMachineTypes, configCache.machineTypes)

	// Cache expiration is expected to be reset here because of the fetching error.
	// So new cache update for zone B should be triggered during the next call for zone B.
	err = validator.ValidateMachineTypeConfig(fetchingErrorCustomMachineType, zoneB)
	// 1 unsuccessful call for unknown in zoneB
	assert.Equal(t, 1, countChan(machineTypeFetches))
	assert.Equal(t, 0, countChan(machineTypesFetches))
	assert.Error(t, err)

	// Asynchronous cache update is expected.
	err = validator.ValidateMachineTypeConfig(customMachineTypeN1, zoneB)
	// 4 for predefined custom machine types when updating zonal cache for zoneB
	// + 3 for originally not known but later added custom machine types when updating zonal cache for zoneB
	for i := 0; i < 7; i++ {
		waitErr := waitForChan(machineTypeFetches)
		assert.NoError(t, waitErr)
	}
	assert.Equal(t, 0, countChan(machineTypeFetches))
	for i := 0; i < 1; i++ { // 1 call for the zone with predefined machine types
		waitErr := waitForChan(machineTypesFetches)
		assert.NoError(t, waitErr)
	}
	assert.Equal(t, 0, countChan(machineTypesFetches))
	assert.NoError(t, err)
}

func waitForChan(ch chan bool) error {
	select {
	case <-ch:
		return nil
	case <-time.After(500 * time.Millisecond):
		return errors.New("channel empty")
	}
}

func countChan(ch chan bool) int {
	var count int
	for len(ch) > 0 {
		<-ch
		count++
	}
	return count
}

func TestGetZoneCaching(t *testing.T) {
	zoneMachineTypes := testZoneMachineTypes
	zoneGpuCounts := testZoneGpuCounts

	fetches := make(chan bool, 10)
	validator := buildCachedMachineConfigValidator(func() { fetches <- true }, &zoneMachineTypes, &zoneGpuCounts, testKnownCustomTypes, time.Second, nil)

	now := time.Now()
	expired := now.Add(maxCacheValidity).Add(time.Hour)

	// Initial get, synchronous fetch is performed.
	machineTypes, gpuCounts, err := validator.getZone("us-central1-c", now)
	assert.NoError(t, err)
	assert.Len(t, machineTypes, 4)
	assert.Len(t, gpuCounts, 2)

	// Exactly 4 fetches per zone update are expected - 1x GPU, 1x predefined machine types, 2x custom machine type.
	assert.Equal(t, 8, countChan(fetches))

	// Clearing zonal data, now fetching will return empty result.
	zoneMachineTypes = map[string]map[string]bool{}
	zoneGpuCounts = map[string]map[string]int64{}

	// Result from cache, no fetch performed.
	machineTypes, gpuCounts, err = validator.getZone("us-central1-c", now)
	assert.NoError(t, err)
	assert.Len(t, machineTypes, 4)
	assert.Len(t, gpuCounts, 2)

	// Verify that fetch was not performed.
	assert.Equal(t, 0, countChan(fetches))

	// Result from cache with triggered cache invalidation.
	machineTypes, gpuCounts, err = validator.getZone("us-central1-c", expired)
	assert.NoError(t, err)
	assert.Len(t, machineTypes, 4)
	assert.Len(t, gpuCounts, 2)

	// Wait for zone update to complete (4 additional fetches).
	for i := 0; i < 4; i++ {
		err = waitForChan(fetches)
		assert.NoError(t, err)
	}

	// The test is set up so that the channel receives an element on fetch. In this case the fetches are asynchronous, so they
	// are not executed under a lock. The lock is only acquired after all fetches finish, and there's some processing in between.
	// It can rarely happen that the getZone call below is executed in this between time - after the channel receives all fetch
	// elements, but before the cache is actually updated. To reduce flakiness, try to assert a couple of times before failing.
	cacheCleared := false
	for i := 0; i < 5; i++ {
		machineTypes, gpuCounts, err = validator.getZone("us-central1-c", expired)
		assert.NoError(t, err)
		if len(machineTypes) == 0 && len(gpuCounts) == 0 {
			cacheCleared = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	assert.True(t, cacheCleared)
}

func TestGetZoneBackoff(t *testing.T) {
	fetches := make(chan bool, 10)
	initialBackoff := 10 * time.Second
	validator := buildCachedMachineConfigValidator(func() { fetches <- true }, &testZoneMachineTypes, &testZoneGpuCounts, testKnownCustomTypes, initialBackoff, errors.New("always broken"))

	// initial
	now := time.Now()
	_, _, err := validator.getZone("mini-zone", now)
	assert.Error(t, err)
	assert.Equal(t, 1, countChan(fetches))

	// testing backoff
	_, _, err = validator.getZone("mini-zone", now)
	assert.Error(t, err)
	assert.Equal(t, 0, countChan(fetches))

	// testing retry after initial backoff
	now = now.Add(2 * initialBackoff)
	_, _, err = validator.getZone("mini-zone", now)
	assert.Error(t, err)
	assert.Len(t, fetches, 1)
	_ = countChan(fetches)
	// second attempt
	_, _, err = validator.getZone("mini-zone", now)
	assert.Error(t, err)
	assert.Len(t, fetches, 0)

	// testing if backoff is exponential
	now = now.Add(2 * initialBackoff)
	_, _, err = validator.getZone("mini-zone", now)
	assert.Error(t, err)
	assert.Len(t, fetches, 0)
	// after two times the initial backoff
	now = now.Add(2 * initialBackoff)
	_, _, err = validator.getZone("mini-zone", now)
	assert.Error(t, err)
	assert.Len(t, fetches, 1)
	_ = countChan(fetches)
	// second attempt
	_, _, err = validator.getZone("mini-zone", now)
	assert.Error(t, err)
	assert.Len(t, fetches, 0)

	// ensure no other fetches
	err = waitForChan(fetches)
	assert.Error(t, err)
}

func TestGetZoneConcurrentUpdate(t *testing.T) {
	fetches := make(chan bool, 10)
	validator := buildCachedMachineConfigValidator(func() { time.Sleep(5 * time.Millisecond); fetches <- true }, &testZoneMachineTypes, &testZoneGpuCounts, testKnownCustomTypes, time.Second, nil)

	now := time.Now()
	expired := now.Add(maxCacheValidity).Add(time.Hour)

	// Trigger initial zone update.
	machineTypes, gpuCounts, err := validator.getZone("us-central1-c", now)
	assert.NoError(t, err)
	assert.Len(t, machineTypes, 4)
	assert.Len(t, gpuCounts, 2)
	// Exactly 8 fetches per zone update are expected - 1x GPU, 1x predefined machine type, 2x custom machine type, 4x predefined custom machine type.
	assert.Equal(t, 8, countChan(fetches))

	finished := make(chan bool, 10)
	getZone := func() {
		_, _, err := validator.getZone("us-central1-c", expired)
		assert.NoError(t, err)
		finished <- true
	}

	// results from cache with triggered cache update in the background
	getZone()
	// subsequent requests should not trigger fetch
	getZone()
	go getZone()

	// wait for all goroutines to finish
	for i := 0; i < 3; i++ {
		err = waitForChan(finished)
		assert.NoError(t, err)
	}
	// Ensure that zone was only updated 1 time after the initial update (4 additional fetch calls).
	for i := 0; i < 8; i++ {
		err = waitForChan(fetches)
		assert.NoError(t, err)
	}
	// ensure no other fetches
	err = waitForChan(fetches)
	assert.Error(t, err)
}

func TestGetZoneConcurrentUpdateMultiThreaded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fetches := make(chan bool, 10)
		validator := buildCachedMachineConfigValidator(func() { fetches <- true }, &testZoneMachineTypes, &testZoneGpuCounts, testKnownCustomTypes, time.Second, nil)
		now := time.Now()
		finished := make(chan bool, 10)

		getZone := func() {
			_, _, err := validator.getZone("us-central1-c", now)
			assert.NoError(t, err)
			finished <- true
		}
		// Results are fetched synchronously.
		go getZone()
		// Subsequent requests should not fail and not trigger additional fetch.
		for i := 0; i < 4; i++ {
			go getZone()
		}

		// Wait for all goroutines to finish.
		synctest.Wait()

		// Exactly 8 fetches per zone update are expected - 1x GPU, 1x predefined machine type, 2x custom machine type, 4x predefined custom machine type.
		// Ensure that zone was only updated 1 time.
		assert.Equal(t, 8, countChan(fetches))
		assert.Equal(t, 5, countChan(finished))
	})
}

func TestComputeBackoff(t *testing.T) {
	type testCase struct {
		errorCount  uint64
		httpTimeout time.Duration
		expectAbove time.Duration
		expectBelow time.Duration
	}
	halfMaxCacheValidity := time.Duration(0.5 * float64(maxCacheValidity))
	testCases := []testCase{
		{0, time.Second, time.Second, 2 * time.Second},
		{0, time.Minute, time.Minute, 2 * time.Minute},
		{1, time.Second, 2 * time.Second, 4 * time.Second},
		{2, time.Minute, 4 * time.Minute, 8 * time.Minute},
		{3, time.Minute, 8 * time.Minute, 16 * time.Minute},
		{16, time.Second, halfMaxCacheValidity, maxCacheValidity},
		{100, time.Minute, halfMaxCacheValidity, maxCacheValidity},
	}
	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("errorCount=%d, httpTimeout=%v", tc.errorCount, tc.httpTimeout), func(t *testing.T) {
			backoff := computeBackoff(tc.errorCount, tc.httpTimeout)
			assert.True(t, float64(backoff) > float64(tc.expectAbove),
				"Test %v: %v > %v", idx, backoff, tc.expectAbove)
			assert.True(t, float64(backoff) < float64(tc.expectBelow),
				"Test %v: %v < %v", idx, backoff, tc.expectBelow)
		})
	}
}
