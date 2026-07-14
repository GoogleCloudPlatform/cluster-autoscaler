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

package conditions

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	gceapiv1 "google.golang.org/api/compute/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
)

func TestReservationsCacheAddProject(t *testing.T) {
	localProject := ""
	clusterProject := "cluster-project"
	sharedProject1 := "shared-project1"
	sharedProject2 := "shared-project2"
	rsv1 := &gceapiv1.Reservation{Name: "reservation-1", SelfLink: fmt.Sprintf("projects/%s/reservations/reservation-1", clusterProject)}
	rsv2 := &gceapiv1.Reservation{Name: "reservation-2", SelfLink: fmt.Sprintf("projects/%s/reservations/reservation-2", sharedProject1)}
	rsv3 := &gceapiv1.Reservation{Name: "reservation-3", SelfLink: fmt.Sprintf("projects/%s/reservations/reservation-3", sharedProject2)}

	tests := map[string]struct {
		reservations  []*gceapiv1.Reservation
		projectsToAdd []string
		wantCache     map[string]map[string]*gceapiv1.Reservation
	}{
		"NoReservations": {
			reservations:  []*gceapiv1.Reservation{},
			projectsToAdd: []string{"project-1"},
			wantCache: map[string]map[string]*gceapiv1.Reservation{
				"project-1": {},
			},
		},
		"LocalReservations": {
			reservations:  []*gceapiv1.Reservation{rsv1},
			projectsToAdd: []string{localProject},
			wantCache: map[string]map[string]*gceapiv1.Reservation{
				localProject: {
					rsv1.Name: rsv1,
				},
			},
		},
		"ReservationsPerProject": {
			reservations:  []*gceapiv1.Reservation{rsv2, rsv3},
			projectsToAdd: []string{sharedProject1, sharedProject2},
			wantCache: map[string]map[string]*gceapiv1.Reservation{
				sharedProject1: {
					rsv2.Name: rsv2,
				},
				sharedProject2: {
					rsv3.Name: rsv3,
				},
			},
		},
		"CombinedReservations": {
			reservations:  []*gceapiv1.Reservation{rsv1, rsv2, rsv3},
			projectsToAdd: []string{localProject, sharedProject1, sharedProject2},
			wantCache: map[string]map[string]*gceapiv1.Reservation{
				localProject: {
					rsv1.Name: rsv1,
				},
				sharedProject1: {
					rsv2.Name: rsv2,
				},
				sharedProject2: {
					rsv3.Name: rsv3,
				},
			},
		},
		"WithNotAddedProjects": {
			reservations:  []*gceapiv1.Reservation{rsv1, rsv2, rsv3},
			projectsToAdd: []string{},
			wantCache:     map[string]map[string]*gceapiv1.Reservation{},
		},
	}

	for testName, test := range tests {
		test := test

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			mGceClient := gceclient.BuildAutoscalingInternalGceClientMock().
				WithFetchZones(func(region string) ([]string, error) { return []string{"us-central1-a"}, nil })
			puller, err := gceclient.NewReservationsPuller(mGceClient, nil, nil, clusterProject, false, "us-central1")
			assert.NoError(t, err)
			puller.AddProject(sharedProject1)
			puller.AddProject(sharedProject2)
			puller.SetReservations(test.reservations)
			rsvCache := NewReservationsCache(puller)

			for _, project := range test.projectsToAdd {
				rsvCache.AddCacheForProject(project)
			}

			assert.Equal(t, test.wantCache, rsvCache.cache)
		})
	}
}

func TestReservationsCacheGetReservation(t *testing.T) {
	rsv := &gceapiv1.Reservation{
		Name: "reservation-1",
	}

	tests := map[string]struct {
		cache           map[string]map[string]*gceapiv1.Reservation
		name            string
		project         string
		wantReservation *gceapiv1.Reservation
	}{
		"ReservationInCache": {
			cache: map[string]map[string]*gceapiv1.Reservation{
				"project-1": {
					"reservation-1": rsv,
				},
			},
			name:            "reservation-1",
			project:         "project-1",
			wantReservation: rsv,
		},
		"ReservationNotInCache": {
			cache: map[string]map[string]*gceapiv1.Reservation{
				"project-1": {
					"reservation-1": rsv,
				},
			},
			name:            "reservation-2",
			project:         "project-1",
			wantReservation: nil,
		},
		"ProjectNotInCache": {
			cache: map[string]map[string]*gceapiv1.Reservation{
				"project-1": {
					"reservation-1": rsv,
				},
			},
			name:            "reservation-1",
			project:         "project-2",
			wantReservation: nil,
		},
	}

	for testName, test := range tests {
		test := test

		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			rsvCache := ReservationsCache{cache: test.cache}
			reservation := rsvCache.GetReservation(test.name, test.project)
			assert.Equal(t, test.wantReservation, reservation)
		})
	}
}

func TestMatchSpecificReservation(t *testing.T) {
	ssdProvider := localssdsize.NewSimpleLocalSSDProvider()
	defaultZone := "reservationZone"
	defaultMinCpuPlatform := "reservationMinCpuPlatform"
	defaultMachineFamily := "e2"
	defaultMachineType := "e2-standard-2"
	defaultAcceleratorType := "acc1"
	defaultAcceleratorCount := machinetypes.PhysicalGpuCount(4)
	defaultLocalSsdType := "NVME"
	defaultLocalSsdSize := 375
	defaultLocalSsdCount := 1
	nonDefaultMachineType := "n2-standard-2"
	nonDefaultMachineFamily := "n2"

	tests := map[string]struct {
		reservation *gceapiv1.Reservation
		rule        rules.Rule

		wantError error
	}{
		"AllMatch": {
			reservation: reservations.BuildReservation(
				"READY",
				/*specificReservationRequired=*/ true,
				defaultZone,
				defaultMachineType,
				defaultMinCpuPlatform,
				reservations.BuildReservationAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				reservations.BuildReservationLocalSSDs(defaultLocalSsdType, defaultLocalSsdSize),
			),
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&defaultMachineType),
				rules.WithGpuRule(&machinetypes.GpuRequest{Count: machinetypes.AllocatableGpuCount(defaultAcceleratorCount), PhysicalGPUCount: defaultAcceleratorCount, Config: machinetypes.GpuConfig{
					GpuType: defaultAcceleratorType,
				}}),
				rules.WithStorageRule(nil, nil, nil, &defaultLocalSsdCount),
			),

			wantError: nil,
		},
		"AllMatchWithMachineFamily": {
			reservation: reservations.BuildReservation(
				"READY",
				/*specificReservationRequired=*/ true,
				defaultZone,
				defaultMachineType,
				defaultMinCpuPlatform,
				reservations.BuildReservationAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				reservations.BuildReservationLocalSSDs(defaultLocalSsdType, defaultLocalSsdSize),
			),
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&defaultMachineFamily),
				rules.WithGpuRule(&machinetypes.GpuRequest{Count: machinetypes.AllocatableGpuCount(defaultAcceleratorCount), PhysicalGPUCount: defaultAcceleratorCount, Config: machinetypes.GpuConfig{
					GpuType: defaultAcceleratorType,
				}}),
				rules.WithStorageRule(nil, nil, nil, &defaultLocalSsdCount),
			),

			wantError: nil,
		},
		"WithCorrectMachineFamily_Match": {
			reservation: reservations.BuildReservation(
				"READY",
				/*specificReservationRequired=*/ true,
				defaultZone,
				defaultMachineType,
				defaultMinCpuPlatform,
				reservations.BuildReservationAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				reservations.BuildReservationLocalSSDs(defaultLocalSsdType, defaultLocalSsdSize),
			),
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&defaultMachineFamily),
				rules.WithGpuRule(&machinetypes.GpuRequest{Count: machinetypes.AllocatableGpuCount(defaultAcceleratorCount), PhysicalGPUCount: defaultAcceleratorCount, Config: machinetypes.GpuConfig{
					GpuType: defaultAcceleratorType,
				}}),
				rules.WithStorageRule(nil, nil, nil, &defaultLocalSsdCount),
			),

			wantError: nil,
		},
		"SameMachineType_Match": {
			reservation: reservations.BuildReservation(
				"READY",
				/*specificReservationRequired=*/ true,
				defaultZone,
				defaultMachineType,
				defaultMinCpuPlatform,
				/*accelerators=*/ nil,
				/*localSsds=*/ nil,
			),
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&defaultMachineType),
			),

			wantError: nil,
		},
		"SameMachineFamily_Match": {
			reservation: reservations.BuildReservation(
				"READY",
				/*specificReservationRequired=*/ true,
				defaultZone,
				defaultMachineType,
				defaultMinCpuPlatform,
				/*accelerators=*/ nil,
				/*localSsds=*/ nil,
			),
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&defaultMachineFamily),
			),

			wantError: nil,
		},
		"AnyReservation_NoMatch": {
			reservation: reservations.BuildReservation(
				"READY",
				/*specificReservationRequired=*/ false,
				defaultZone,
				defaultMachineType,
				defaultMinCpuPlatform,
				/*accelerators=*/ nil,
				/*localSsds=*/ nil,
			),
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&defaultMachineType),
			),

			wantError: errors.New("any affinity reservation cannot be consumed"),
		},
		"AggregateReservation_NoMatch": {
			reservation: reservations.BuildAggregateReservation(defaultZone),
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&defaultMachineType),
			),

			wantError: errors.New("any affinity reservation cannot be consumed"),
		},
		"NoTypeOrFamilySet_NoMatch": {
			reservation: reservations.BuildReservation(
				"READY",
				/*specificReservationRequired=*/ true,
				defaultZone,
				defaultMachineType,
				defaultMinCpuPlatform,
				/*accelerators=*/ nil,
				/*localSsds=*/ nil,
			),
			rule: rules.NewRule(),

			wantError: errors.New("missing machineType and machineFamily, unable to define reservation compatibility"),
		},
		"DifferentMachineType_NoMatch": {
			reservation: reservations.BuildReservation(
				"READY",
				/*specificReservationRequired=*/ true,
				defaultZone,
				defaultMachineType,
				defaultMinCpuPlatform,
				/*accelerators=*/ nil,
				/*localSsds=*/ nil,
			),
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&nonDefaultMachineType),
			),

			wantError: errors.New("machine type mismatch, requested n2-standard-2, reservation has e2-standard-2"),
		},
		"DifferentMachineFamily_NoMatch": {
			reservation: reservations.BuildReservation(
				"READY",
				/*specificReservationRequired=*/ true,
				defaultZone,
				defaultMachineType,
				defaultMinCpuPlatform,
				/*accelerators=*/ nil,
				/*localSsds=*/ nil,
			),
			rule: rules.NewRule(
				rules.WithMachineFamilyRule(&nonDefaultMachineFamily),
			),

			wantError: errors.New("machine family mismatch: requested n2, reservation has e2"),
		},
		"DifferentLocalSsdSize_NoMatch": {
			reservation: reservations.BuildReservation(
				"READY",
				/*specificReservationRequired=*/ true,
				defaultZone,
				defaultMachineType,
				defaultMinCpuPlatform,
				/*accelerators=*/ nil,
				reservations.BuildReservationLocalSSDs(defaultLocalSsdType, 5000000),
			),
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&defaultMachineType),
				rules.WithStorageRule(nil, nil, nil, &defaultLocalSsdCount),
			),

			wantError: errors.New("local SSD mismatch, requested 375 GB NVME SSD, reservation has 5000000 GB NVME SSD"),
		},
		"DifferentLocalSsdType_NoMatch": {
			reservation: reservations.BuildReservation(
				"READY",
				/*specificReservationRequired=*/ true,
				defaultZone,
				defaultMachineType,
				defaultMinCpuPlatform,
				/*accelerators=*/ nil,
				reservations.BuildReservationLocalSSDs("SCSI", defaultLocalSsdSize),
			),
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&defaultMachineType),
				rules.WithStorageRule(nil, nil, nil, &defaultLocalSsdCount),
			),

			wantError: errors.New("local SSD mismatch, requested 375 GB NVME SSD, but it's missing from reservation"),
		},
		"DifferentAcceleratorType_NoMatch": {
			reservation: reservations.BuildReservation(
				"READY",
				/*specificReservationRequired=*/ true,
				defaultZone,
				defaultMachineType,
				defaultMinCpuPlatform,
				reservations.BuildReservationAccelerators("weird-accelerator", defaultAcceleratorCount),
				/*localSsds=*/ nil,
			),
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&defaultMachineType),
				rules.WithGpuRule(&machinetypes.GpuRequest{Count: machinetypes.AllocatableGpuCount(defaultAcceleratorCount), PhysicalGPUCount: defaultAcceleratorCount, Config: machinetypes.GpuConfig{
					GpuType: defaultAcceleratorType,
				}}),
			),

			wantError: errors.New("accelerator mismatch, requested 4 chips of acc1, but it's missing from reservation"),
		},
		"DifferentAcceleratorCount_NoMatch": {
			reservation: reservations.BuildReservation(
				"READY",
				/*specificReservationRequired=*/ true,
				defaultZone,
				defaultMachineType,
				defaultMinCpuPlatform,
				reservations.BuildReservationAccelerators(defaultAcceleratorType, 50000),
				/*localSsds=*/ nil,
			),
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&defaultMachineType),
				rules.WithGpuRule(&machinetypes.GpuRequest{Count: machinetypes.AllocatableGpuCount(defaultAcceleratorCount), PhysicalGPUCount: defaultAcceleratorCount, Config: machinetypes.GpuConfig{
					GpuType: defaultAcceleratorType,
				}}),
			),

			wantError: errors.New("accelerator mismatch, requested 4 chips of acc1, reservation has 50000 chips of acc1"),
		},
		"UnsupportedReservatioLocalSSDType_NoMatch": {
			reservation: reservations.BuildReservation(
				"READY",
				/*specificReservationRequired=*/ true,
				defaultZone,
				defaultMachineType,
				defaultMinCpuPlatform,
				/*accelerators=*/ nil,
				reservations.BuildReservationLocalSSDs("UNSUPPORTED", defaultLocalSsdSize),
			),
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&defaultMachineType),
				rules.WithStorageRule(nil, nil, nil, &defaultLocalSsdCount),
			),

			wantError: errors.New("local SSD mismatch, requested 375 GB NVME SSD, but it's missing from reservation; reservation local SSD interface is not supported: UNSUPPORTED"),
		},
		"ArbitraryZone_Match": {
			reservation: reservations.BuildReservation(
				"READY",
				/*specificReservationRequired=*/ true,
				"something",
				defaultMachineType,
				defaultMinCpuPlatform,
				/*accelerators=*/ nil,
				/*localSsds=*/ nil,
			),
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&defaultMachineType),
			),

			wantError: nil,
		},
		"ArbitraryMinCpuPlatform_Match": {
			reservation: reservations.BuildReservation(
				"READY",
				/*specificReservationRequired=*/ true,
				defaultZone,
				defaultMachineType,
				"something",
				/*accelerators=*/ nil,
				/*localSsds=*/ nil,
			),
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&defaultMachineType),
			),

			wantError: nil,
		},
		"AcceleratorAvailableButNotClaimed_Match": {
			reservation: reservations.BuildReservation(
				"READY",
				/*specificReservationRequired=*/ true,
				defaultZone,
				defaultMachineType,
				"something",
				reservations.BuildReservationAccelerators(defaultAcceleratorType, 50000),
				/*localSsds=*/ nil,
			),
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&defaultMachineType),
			),

			wantError: nil,
		},
		"LocalSSDAvailableButNotClaimed_Match": {
			reservation: reservations.BuildReservation(
				"READY",
				/*specificReservationRequired=*/ true,
				defaultZone,
				defaultMachineType,
				"something",
				nil,
				reservations.BuildReservationLocalSSDs("SCSI", defaultLocalSsdSize),
			),
			rule: rules.NewRule(
				rules.WithMachineTypeRule(&defaultMachineType),
			),

			wantError: nil,
		},
	}

	for testName, test := range tests {
		test := test

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			gotError := matchSpecificReservationOrError(provider, test.reservation, test.rule, ssdProvider)
			assert.Equal(t, test.wantError, gotError)
		})
	}
}

func TestMatchAggregateReservation(t *testing.T) {
	tests := map[string]struct {
		reservation *gceapiv1.Reservation
		rule        rules.Rule

		wantError error
	}{
		"MatchTypeAndCount": {
			reservation: &gceapiv1.Reservation{
				Status:                      "READY",
				SpecificReservationRequired: true,
				AggregateReservation: &gceapiv1.AllocationAggregateReservation{
					ReservedResources: []*gceapiv1.AllocationAggregateReservationReservedResourceInfo{
						{
							Accelerator: &gceapiv1.AllocationAggregateReservationReservedResourceInfoAccelerator{
								AcceleratorType:  "projects/whatever/zones/us-central2-b/acceleratorTypes/ct4p",
								AcceleratorCount: 4,
							},
						},
					},
					WorkloadType: tpu.WorkloadTypeUnspecified,
				},
			},
			rule: rules.NewRule(
				rules.WithTpuRule(labels.TpuV4PodsliceValue, 4, "2x2"),
			),

			wantError: nil,
		},
		"MultipleResourcesAvailable": {
			reservation: &gceapiv1.Reservation{
				Status:                      "READY",
				SpecificReservationRequired: true,
				AggregateReservation: &gceapiv1.AllocationAggregateReservation{
					ReservedResources: []*gceapiv1.AllocationAggregateReservationReservedResourceInfo{
						{
							Accelerator: &gceapiv1.AllocationAggregateReservationReservedResourceInfoAccelerator{
								AcceleratorType:  "projects/whatever/zones/us-central2-b/acceleratorTypes/ct5p",
								AcceleratorCount: 4,
							},
						},
						{
							Accelerator: &gceapiv1.AllocationAggregateReservationReservedResourceInfoAccelerator{
								AcceleratorType:  "projects/whatever/zones/us-central2-b/acceleratorTypes/ct4p",
								AcceleratorCount: 8,
							},
						},
					},
					WorkloadType: tpu.WorkloadTypeUnspecified,
				},
			},
			rule: rules.NewRule(
				rules.WithTpuRule(labels.TpuV4PodsliceValue, 4, "2x2x2"),
			),

			wantError: nil,
		},
		"TypeMismatch": {
			reservation: &gceapiv1.Reservation{
				Status:                      "READY",
				SpecificReservationRequired: true,
				AggregateReservation: &gceapiv1.AllocationAggregateReservation{
					ReservedResources: []*gceapiv1.AllocationAggregateReservationReservedResourceInfo{
						{
							Accelerator: &gceapiv1.AllocationAggregateReservationReservedResourceInfoAccelerator{
								AcceleratorType:  "projects/whatever/zones/us-central2-b/acceleratorTypes/ct4p",
								AcceleratorCount: 4,
							},
						},
					},
					WorkloadType: tpu.WorkloadTypeUnspecified,
				},
			},
			rule: rules.NewRule(
				rules.WithTpuRule(labels.TpuV5LitePodsliceValue, 4, "2x2"),
			),

			wantError: errors.New("requested TPU accelerator {tpu-v5-lite-podslice, 2x2, 4}, but it's missing from reservation"),
		},
		"CountMismatch": {
			reservation: &gceapiv1.Reservation{
				Status:                      "READY",
				SpecificReservationRequired: true,
				AggregateReservation: &gceapiv1.AllocationAggregateReservation{
					ReservedResources: []*gceapiv1.AllocationAggregateReservationReservedResourceInfo{
						{
							Accelerator: &gceapiv1.AllocationAggregateReservationReservedResourceInfoAccelerator{
								AcceleratorType:  "projects/whatever/zones/us-central2-b/acceleratorTypes/ct4p",
								AcceleratorCount: 4,
							},
						},
					},
					WorkloadType: tpu.WorkloadTypeUnspecified,
				},
			},
			rule: rules.NewRule(
				rules.WithTpuRule(labels.TpuV4PodsliceValue, 8, "2x2x2"),
			),

			wantError: errors.New("requested TPU accelerator {tpu-v4-podslice, 2x2x2, 8}, but it's missing from reservation"),
		},
		"AnyReservation_NoMatch": {
			reservation: &gceapiv1.Reservation{
				Status: "READY",
				AggregateReservation: &gceapiv1.AllocationAggregateReservation{
					ReservedResources: []*gceapiv1.AllocationAggregateReservationReservedResourceInfo{
						{
							Accelerator: &gceapiv1.AllocationAggregateReservationReservedResourceInfoAccelerator{
								AcceleratorType:  "projects/whatever/zones/us-central2-b/acceleratorTypes/ct4p",
								AcceleratorCount: 4,
							},
						},
					},
					WorkloadType: tpu.WorkloadTypeUnspecified,
				},
			},
			rule: rules.NewRule(
				rules.WithTpuRule(labels.TpuV4PodsliceValue, 4, "2x2"),
			),

			wantError: errors.New("any affinity reservation cannot be consumed"),
		},
		"NotAggregateReservation": {
			reservation: &gceapiv1.Reservation{
				Status:                      "READY",
				SpecificReservationRequired: true,
				SpecificReservation:         &gceapiv1.AllocationSpecificSKUReservation{},
			},
			rule: rules.NewRule(
				rules.WithTpuRule(labels.TpuV4PodsliceValue, 4, "2x2"),
			),

			wantError: errors.New("unable to match non-aggregate reservation"),
		},
		"MultihostRequiredSinglehostAvailable": {
			reservation: &gceapiv1.Reservation{
				Status:                      "READY",
				SpecificReservationRequired: true,
				AggregateReservation: &gceapiv1.AllocationAggregateReservation{
					ReservedResources: []*gceapiv1.AllocationAggregateReservationReservedResourceInfo{
						{
							Accelerator: &gceapiv1.AllocationAggregateReservationReservedResourceInfoAccelerator{
								AcceleratorType:  "projects/whatever/zones/us-central2-b/acceleratorTypes/ct4p",
								AcceleratorCount: 8,
							},
						},
					},
					WorkloadType: tpu.WorkloadTypeServing,
				},
			},
			rule: rules.NewRule(
				rules.WithTpuRule(labels.TpuV4PodsliceValue, 8, "2x2"),
			),

			wantError: errors.New("requested multihost TPU, reservation has singlehost"),
		},
		"SingleHostRequiredMultihostAvailable": {
			reservation: &gceapiv1.Reservation{
				Status:                      "READY",
				SpecificReservationRequired: true,
				AggregateReservation: &gceapiv1.AllocationAggregateReservation{
					ReservedResources: []*gceapiv1.AllocationAggregateReservationReservedResourceInfo{
						{
							Accelerator: &gceapiv1.AllocationAggregateReservationReservedResourceInfoAccelerator{
								AcceleratorType:  "projects/whatever/zones/us-central2-b/acceleratorTypes/ct4p",
								AcceleratorCount: 8,
							},
						},
					},
					WorkloadType: tpu.WorkloadTypeBatch,
				},
			},
			rule: rules.NewRule(
				rules.WithTpuRule(labels.TpuV4PodsliceValue, 8, "2x2x2"),
			),

			wantError: errors.New("requested singlehost TPU, reservation has multihost"),
		},
		"WorkloadTypeMatchingMultihost": {
			reservation: &gceapiv1.Reservation{
				Status:                      "READY",
				SpecificReservationRequired: true,
				AggregateReservation: &gceapiv1.AllocationAggregateReservation{
					ReservedResources: []*gceapiv1.AllocationAggregateReservationReservedResourceInfo{
						{
							Accelerator: &gceapiv1.AllocationAggregateReservationReservedResourceInfoAccelerator{
								AcceleratorType:  "projects/whatever/zones/us-central2-b/acceleratorTypes/ct4p",
								AcceleratorCount: 8,
							},
						},
					},
					WorkloadType: tpu.WorkloadTypeBatch,
				},
			},
			rule: rules.NewRule(
				rules.WithTpuRule(labels.TpuV4PodsliceValue, 2, "2x2"),
			),

			wantError: nil,
		},
		"WorkloadTypeMatchingSinglehost": {
			reservation: &gceapiv1.Reservation{
				Status:                      "READY",
				SpecificReservationRequired: true,
				AggregateReservation: &gceapiv1.AllocationAggregateReservation{
					ReservedResources: []*gceapiv1.AllocationAggregateReservationReservedResourceInfo{
						{
							Accelerator: &gceapiv1.AllocationAggregateReservationReservedResourceInfoAccelerator{
								AcceleratorType:  "projects/whatever/zones/us-central2-b/acceleratorTypes/ct4p",
								AcceleratorCount: 8,
							},
						},
					},
					WorkloadType: tpu.WorkloadTypeServing,
				},
			},
			rule: rules.NewRule(
				rules.WithTpuRule(labels.TpuV4PodsliceValue, 8, "2x2x2"),
			),

			wantError: nil,
		},
	}

	for testName, test := range tests {
		test := test

		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			gotError := matchAggregateReservationOrError(provider, test.reservation, test.rule)
			assert.Equal(t, test.wantError, gotError)
		})
	}
}

func TestMatchReservationBlock(t *testing.T) {
	testCases := []struct {
		name      string
		blockName string
		blocks    []*gceclient.GceReservationBlock
		wantMatch bool
	}{
		{
			name:      "Matching reservation block",
			blockName: "rb1",
			blocks: []*gceclient.GceReservationBlock{
				{
					Name: "rb1",
				},
				{
					Name: "rb2",
				},
			},
			wantMatch: true,
		},
		{
			name:      "No matching reservation block",
			blockName: "rb0",
			blocks: []*gceclient.GceReservationBlock{
				{
					Name: "rb1",
				},
				{
					Name: "rb2",
				},
			},
			wantMatch: false,
		},
		{
			name:      "Empty blocks",
			blockName: "rb1",
			blocks:    nil,
			wantMatch: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotMatch := matchReservationBlock(tc.blockName, tc.blocks)
			assert.Equal(t, tc.wantMatch, gotMatch)
		})
	}

}
