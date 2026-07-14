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

package fake

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	flexadvisorapi "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/utils/set"
)

func TestFakeFlexAdvisorClientPreciseMocking(t *testing.T) {
	machineType1 := "n1-standard-4"
	machineType2 := "n2-standard-8"
	zoneA := "us-central1-a"
	zoneB := "us-central1-b"

	testCases := []struct {
		name              string
		capacityGuidances []CapacityGuidance
		reqKey            string
		config            *flexadvisorapi.InstanceConfig
		expectedCapacity  map[string]int
		expectedScore     map[string]float64
	}{
		{
			name: "machine type with specific zone and fallback to wildcard zone mock",
			capacityGuidances: []CapacityGuidance{
				{
					MachineType:        &machineType1,
					Zone:               &zoneB,
					InstanceCount:      5,
					GcePreferenceScore: 0.9,
				},
				{
					MachineType:        &machineType1,
					InstanceCount:      100,
					GcePreferenceScore: 0.5,
				},
			},
			reqKey: "req-1",
			config: flexadvisorapi.NewInstanceConfigWithZones(
				machineType1, "", 0, 0, instanceavailability.Standard, "",
				set.New(zoneA, zoneB),
			),
			expectedCapacity: map[string]int{
				zoneA: 100,
				zoneB: 5,
			},
			expectedScore: map[string]float64{
				zoneA: 0.5,
				zoneB: 0.9,
			},
		},
		{
			name: "machine type with wildcard zone mock",
			capacityGuidances: []CapacityGuidance{
				{
					MachineType:        &machineType2,
					InstanceCount:      25,
					GcePreferenceScore: 0.6,
				},
			},
			reqKey: "req-2",
			config: flexadvisorapi.NewInstanceConfigWithZones(
				machineType2, "", 0, 0, instanceavailability.Standard, "",
				set.New(zoneA, zoneB),
			),
			expectedCapacity: map[string]int{
				zoneA: 25,
				zoneB: 25,
			},
			expectedScore: map[string]float64{
				zoneA: 0.6,
				zoneB: 0.6,
			},
		},
		{
			name:              "unknown machine fallback to overall default",
			capacityGuidances: []CapacityGuidance{},
			reqKey:            "req-3",
			config: flexadvisorapi.NewInstanceConfigWithZones(
				"unknown-machine", "", 0, 0, instanceavailability.Standard, "",
				set.New(zoneA),
			),
			expectedCapacity: map[string]int{
				zoneA: DefaultZonalCapacity,
			},
			expectedScore: map[string]float64{
				zoneA: DefaultZonalScore,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := &FakeFlexAdvisorClient{}
			client.AddCapacityGuidances(tc.capacityGuidances...)

			ctx := context.Background()
			reqConfigs := map[string]*flexadvisorapi.InstanceConfig{
				tc.reqKey: tc.config,
			}

			results, err := client.FetchCapacityGuidance(ctx, "scope-1", reqConfigs)
			assert.NoError(t, err)

			avail := results[tc.reqKey]
			assert.NotNil(t, avail)
			snapshot := avail.NewSnapshot()

			for zone, wantCap := range tc.expectedCapacity {
				gotCap, found := snapshot.MaxAvailableInstances(zone)
				assert.True(t, found)
				assert.Equal(t, wantCap, gotCap)
			}

			for zone, wantScore := range tc.expectedScore {
				gotScore, found := snapshot.GcePreferenceScore(zone)
				assert.True(t, found)
				assert.Equal(t, wantScore, gotScore)
			}
		})
	}
}

func TestFakeFlexAdvisorClientFilters(t *testing.T) {
	spotMode := instanceavailability.Spot
	gpuType := "nvidia-tesla-t4"
	gpuCount := 2
	rank := 3
	duration := "3600"
	policies := flexadvisorapi.WorkloadPolicies{AcceleratorTopology: "topology-1"}
	machineType := "n1-standard-4"
	zoneA := "zone-a"

	mismatchGuidance := []CapacityGuidance{
		{
			MachineType:             &machineType,
			ProvisioningMode:        &spotMode,
			GpuType:                 &gpuType,
			GpuCount:                &gpuCount,
			Rank:                    &rank,
			MaxRunDurationInSeconds: &duration,
			WorkloadPolicies:        &policies,
			Zone:                    &zoneA,
			InstanceCount:           999,
		},
	}

	testCases := []struct {
		name              string
		capacityGuidances []CapacityGuidance
		config            *flexadvisorapi.InstanceConfig
		expectedCapacity  int
		expectedScore     float64
	}{
		// Matches tests
		{
			name: "match on machine type",
			capacityGuidances: []CapacityGuidance{
				{
					MachineType:        &machineType,
					InstanceCount:      60,
					GcePreferenceScore: 0.3,
				},
			},
			config: flexadvisorapi.NewInstanceConfigWithZones(
				machineType, "", 0, 0, instanceavailability.Standard, "", set.New(zoneA),
			),
			expectedCapacity: 60,
			expectedScore:    0.3,
		},
		{
			name: "match on zone",
			capacityGuidances: []CapacityGuidance{
				{
					Zone:               &zoneA,
					InstanceCount:      70,
					GcePreferenceScore: 0.2,
				},
			},
			config: flexadvisorapi.NewInstanceConfigWithZones(
				machineType, "", 0, 0, instanceavailability.Standard, "", set.New(zoneA),
			),
			expectedCapacity: 70,
			expectedScore:    0.2,
		},
		{
			name: "match on provisioning mode",
			capacityGuidances: []CapacityGuidance{
				{
					ProvisioningMode:   &spotMode,
					InstanceCount:      10,
					GcePreferenceScore: 0.8,
				},
			},
			config: flexadvisorapi.NewInstanceConfigWithZones(
				machineType, "", 0, 0, instanceavailability.Spot, "", set.New(zoneA),
			),
			expectedCapacity: 10,
			expectedScore:    0.8,
		},
		{
			name: "match on GPU type and count",
			capacityGuidances: []CapacityGuidance{
				{
					GpuType:            &gpuType,
					GpuCount:           &gpuCount,
					InstanceCount:      20,
					GcePreferenceScore: 0.7,
				},
			},
			config: flexadvisorapi.NewInstanceConfigWithZones(
				machineType, gpuType, gpuCount, 0, instanceavailability.Standard, "", set.New(zoneA),
			),
			expectedCapacity: 20,
			expectedScore:    0.7,
		},
		{
			name: "match on rank",
			capacityGuidances: []CapacityGuidance{
				{
					Rank:               &rank,
					InstanceCount:      30,
					GcePreferenceScore: 0.6,
				},
			},
			config: flexadvisorapi.NewInstanceConfigWithZones(
				machineType, "", 0, rank, instanceavailability.Standard, "", set.New(zoneA),
			),
			expectedCapacity: 30,
			expectedScore:    0.6,
		},
		{
			name: "match on max run duration",
			capacityGuidances: []CapacityGuidance{
				{
					MaxRunDurationInSeconds: &duration,
					InstanceCount:           40,
					GcePreferenceScore:      0.5,
				},
			},
			config: flexadvisorapi.NewInstanceConfigWithZones(
				machineType, "", 0, 0, instanceavailability.Standard, duration, set.New(zoneA),
			),
			expectedCapacity: 40,
			expectedScore:    0.5,
		},
		{
			name: "match on workload policies",
			capacityGuidances: []CapacityGuidance{
				{
					WorkloadPolicies:   &policies,
					InstanceCount:      50,
					GcePreferenceScore: 0.4,
				},
			},
			config: flexadvisorapi.NewInstanceConfigWithZones(
				machineType, "", 0, 0, instanceavailability.Standard, "", set.New(zoneA),
				flexadvisorapi.WithWorkloadPolicies(policies),
			),
			expectedCapacity: 50,
			expectedScore:    0.4,
		},
		// Mismatches tests
		{
			name:              "mismatch on machine type",
			capacityGuidances: mismatchGuidance,
			config: flexadvisorapi.NewInstanceConfigWithZones(
				"mismatch", "", 0, 0, instanceavailability.Standard, "", set.New(zoneA),
			),
			expectedCapacity: DefaultZonalCapacity,
			expectedScore:    DefaultZonalScore,
		},
		{
			name:              "mismatch on provisioning mode",
			capacityGuidances: mismatchGuidance,
			config: flexadvisorapi.NewInstanceConfigWithZones(
				machineType, "", 0, 0, instanceavailability.Standard, "", set.New(zoneA),
			),
			expectedCapacity: DefaultZonalCapacity,
			expectedScore:    DefaultZonalScore,
		},
		{
			name:              "mismatch on GPU type",
			capacityGuidances: mismatchGuidance,
			config: flexadvisorapi.NewInstanceConfigWithZones(
				machineType, "mismatch", gpuCount, 0, spotMode, "", set.New(zoneA),
			),
			expectedCapacity: DefaultZonalCapacity,
			expectedScore:    DefaultZonalScore,
		},
		{
			name:              "mismatch on GPU count",
			capacityGuidances: mismatchGuidance,
			config: flexadvisorapi.NewInstanceConfigWithZones(
				machineType, gpuType, 999, 0, spotMode, "", set.New(zoneA),
			),
			expectedCapacity: DefaultZonalCapacity,
			expectedScore:    DefaultZonalScore,
		},
		{
			name:              "mismatch on rank",
			capacityGuidances: mismatchGuidance,
			config: flexadvisorapi.NewInstanceConfigWithZones(
				machineType, gpuType, gpuCount, 999, spotMode, "", set.New(zoneA),
			),
			expectedCapacity: DefaultZonalCapacity,
			expectedScore:    DefaultZonalScore,
		},
		{
			name:              "mismatch on max run duration",
			capacityGuidances: mismatchGuidance,
			config: flexadvisorapi.NewInstanceConfigWithZones(
				machineType, gpuType, gpuCount, rank, spotMode, "mismatch", set.New(zoneA),
			),
			expectedCapacity: DefaultZonalCapacity,
			expectedScore:    DefaultZonalScore,
		},
		{
			name:              "mismatch on workload policies",
			capacityGuidances: mismatchGuidance,
			config: flexadvisorapi.NewInstanceConfigWithZones(
				machineType, gpuType, gpuCount, rank, spotMode, duration, set.New(zoneA),
				flexadvisorapi.WithWorkloadPolicies(flexadvisorapi.WorkloadPolicies{AcceleratorTopology: "mismatch"}),
			),
			expectedCapacity: DefaultZonalCapacity,
			expectedScore:    DefaultZonalScore,
		},
		{
			name:              "mismatch on zone",
			capacityGuidances: mismatchGuidance,
			config: flexadvisorapi.NewInstanceConfigWithZones(
				machineType, gpuType, gpuCount, rank, spotMode, duration, set.New("zone-b"),
				flexadvisorapi.WithWorkloadPolicies(policies),
			),
			expectedCapacity: DefaultZonalCapacity,
			expectedScore:    DefaultZonalScore,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := &FakeFlexAdvisorClient{}
			client.AddCapacityGuidances(tc.capacityGuidances...)

			ctx := context.Background()
			reqConfigs := map[string]*flexadvisorapi.InstanceConfig{
				"req": tc.config,
			}

			res, err := client.FetchCapacityGuidance(ctx, "scope", reqConfigs)
			assert.NoError(t, err)

			snap := res["req"].NewSnapshot()

			// Find the zone to check from config's zone list
			var targetZone string
			for _, z := range tc.config.Zones().UnsortedList() {
				targetZone = z
				break
			}

			gotCap, _ := snap.MaxAvailableInstances(targetZone)
			gotScore, found := snap.GcePreferenceScore(targetZone)
			assert.True(t, found)
			assert.Equal(t, tc.expectedCapacity, gotCap)
			assert.Equal(t, tc.expectedScore, gotScore)
		})
	}
}

func TestFakeFlexAdvisorClientGetFetchCapacityCalls(t *testing.T) {
	client := &FakeFlexAdvisorClient{}
	assert.Equal(t, 0, client.GetFetchCapacityCalls())

	machineType := "n1-standard-4"
	client.AddCapacityGuidances(CapacityGuidance{
		MachineType:   &machineType,
		InstanceCount: 15,
	})

	ctx := context.Background()
	reqConfigs := map[string]*flexadvisorapi.InstanceConfig{
		"req": flexadvisorapi.NewInstanceConfigWithZones(machineType, "", 0, 0, instanceavailability.Standard, "", set.New("zone-a")),
	}

	_, err := client.FetchCapacityGuidance(ctx, "scope", reqConfigs)
	assert.NoError(t, err)
	assert.Equal(t, 1, client.GetFetchCapacityCalls())

	_, err = client.FetchCapacityGuidance(ctx, "scope", reqConfigs)
	assert.NoError(t, err)
	assert.Equal(t, 2, client.GetFetchCapacityCalls())
}

func TestFakeFlexAdvisorClientOmit(t *testing.T) {
	client := &FakeFlexAdvisorClient{}
	machineType := "n1-standard-4"
	client.AddCapacityGuidances(CapacityGuidance{
		MachineType: &machineType,
		Omit:        true,
	})

	ctx := context.Background()
	reqConfigs := map[string]*flexadvisorapi.InstanceConfig{
		"req": flexadvisorapi.NewInstanceConfigWithZones(machineType, "", 0, 0, instanceavailability.Standard, "", set.New("zone-a")),
	}

	results, err := client.FetchCapacityGuidance(ctx, "scope", reqConfigs)
	assert.NoError(t, err)

	// Omitted config should not be present in results
	_, found := results["req"]
	assert.False(t, found)
}

func TestFakeFlexAdvisorClientDelay(t *testing.T) {
	client := &FakeFlexAdvisorClient{}
	client.SetDelay(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := client.FetchCapacityGuidance(ctx, "scope", nil)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestFakeFlexAdvisorClientClearCapacityGuidances(t *testing.T) {
	client := &FakeFlexAdvisorClient{}
	machineType := "n1-standard-4"
	client.AddCapacityGuidances(CapacityGuidance{
		MachineType:   &machineType,
		InstanceCount: 15,
	})

	ctx := context.Background()
	reqConfigs := map[string]*flexadvisorapi.InstanceConfig{
		"req": flexadvisorapi.NewInstanceConfigWithZones(machineType, "", 0, 0, instanceavailability.Standard, "", set.New("zone-a")),
	}

	client.ClearCapacityGuidances()

	results, err := client.FetchCapacityGuidance(ctx, "scope", reqConfigs)
	assert.NoError(t, err)

	snap := results["req"].NewSnapshot()
	capVal, _ := snap.MaxAvailableInstances("zone-a")
	assert.Equal(t, DefaultZonalCapacity, capVal)
}

func TestFakeFlexAdvisorClientError(t *testing.T) {
	client := &FakeFlexAdvisorClient{}

	machineType := "n1-standard-4"
	mockErr := errors.New("mock flexadvisor client error")

	client.AddCapacityGuidances(CapacityGuidance{
		MachineType: &machineType,
		Error:       mockErr,
	})

	ctx := context.Background()
	reqConfigs := map[string]*flexadvisorapi.InstanceConfig{
		"req": flexadvisorapi.NewInstanceConfigWithZones(machineType, "", 0, 0, instanceavailability.Standard, "", set.New("zone-a")),
	}

	results, err := client.FetchCapacityGuidance(ctx, "scope", reqConfigs)
	assert.Error(t, err)
	assert.Nil(t, results)
	assert.Equal(t, mockErr, err)
}

func TestFakeFlexAdvisorClientDefensiveNilHandling(t *testing.T) {
	client := &FakeFlexAdvisorClient{}

	ctx := context.Background()
	reqConfigs := map[string]*flexadvisorapi.InstanceConfig{
		"nil-config": nil,
		"nil-zones":  flexadvisorapi.NewInstanceConfigWithZones("n1-standard-4", "", 0, 0, instanceavailability.Standard, "", nil),
	}

	results, err := client.FetchCapacityGuidance(ctx, "scope", reqConfigs)
	assert.NoError(t, err)
	assert.NotNil(t, results)

	// The nil-config should be skipped and not exist in results
	assert.NotContains(t, results, "nil-config")

	// The nil-zones config results in 0 capacity entries, so it is omitted
	_, found := results["nil-zones"]
	assert.False(t, found)
}
