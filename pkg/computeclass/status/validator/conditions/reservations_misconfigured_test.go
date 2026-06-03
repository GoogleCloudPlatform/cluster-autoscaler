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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	gceapiv1 "google.golang.org/api/compute/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
)

func TestValidateReservationsConfig(t *testing.T) {
	existingMachineFamily := "N2"
	existingMachineType := "n2-standard-4"
	existingGpuType1 := "nvidia-a100-80gb"
	existingGpuCount := machinetypes.PhysicalGpuCount(2)
	machineTypeWithGpu := "a2-ultragpu-2g"
	machineFamilyWithGpu := "a2"
	localReservationProject := "cluster-project"
	sharedReservationProject := "shared-project"
	ssdProvider := localssdsize.NewSimpleLocalSSDProvider()
	existingLocalSSDType := "NVME"
	existingLocalSSDCount := 2
	existingLocalSSDSize := int(ssdProvider.SSDSizeInGiB(existingLocalSSDType)) * existingLocalSSDCount

	customMachineType1 := "custom-4-32768"
	customMachineType2 := "n2d-custom-8-65536"

	testCases := []struct {
		name              string
		crd               crd.CRD
		reservations      []*gceapiv1.Reservation
		reservationBlocks map[gceclient.ReservationRef][]*gceclient.GceReservationBlock

		wantCondition bool
		wantReason    string
		wantMessage   string
	}{
		{
			name: "No reservation, not found condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithMachineTypeRule(&machineTypeWithGpu),
						rules.WithGpuRule(&machinetypes.GpuRequest{Count: machinetypes.AllocatableGpuCount(existingGpuCount), PhysicalGPUCount: existingGpuCount, Config: machinetypes.GpuConfig{
							GpuType: existingGpuType1,
						}}),
						rules.WithStorageRule(nil, nil, nil, &existingLocalSSDCount),
					),
				}),
			),
			reservations:  []*gceapiv1.Reservation{},
			wantCondition: true,
			wantReason:    ReservationNotFoundReason,
			wantMessage:   fmt.Sprintf(ReservationNotFoundMessage, "reservation1"),
		},
		{
			name: "Shared project reservation, local required, not found condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithMachineTypeRule(&machineTypeWithGpu),
						rules.WithGpuRule(&machinetypes.GpuRequest{Count: machinetypes.AllocatableGpuCount(existingGpuCount), PhysicalGPUCount: existingGpuCount, Config: machinetypes.GpuConfig{
							GpuType: existingGpuType1,
						}}),
						rules.WithStorageRule(nil, nil, nil, &existingLocalSSDCount),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					machineTypeWithGpu,
					"minCpuPlatform",
					reservations.BuildReservationAccelerators(existingGpuType1, existingGpuCount),
					reservations.BuildReservationLocalSSDs(existingLocalSSDType, existingLocalSSDSize),
					sharedReservationProject,
					"reservation1",
				),
			},
			wantCondition: true,
			wantReason:    ReservationNotFoundReason,
			wantMessage:   fmt.Sprintf(ReservationNotFoundMessage, "reservation1"),
		},
		{
			name: "Local project reservation, shared required, not found condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity).WithReservationProject(sharedReservationProject)),
						rules.WithMachineTypeRule(&machineTypeWithGpu),
						rules.WithGpuRule(&machinetypes.GpuRequest{Count: machinetypes.AllocatableGpuCount(existingGpuCount), PhysicalGPUCount: existingGpuCount, Config: machinetypes.GpuConfig{
							GpuType: existingGpuType1,
						}}),
						rules.WithStorageRule(nil, nil, nil, &existingLocalSSDCount),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					machineTypeWithGpu,
					"minCpuPlatform",
					reservations.BuildReservationAccelerators(existingGpuType1, existingGpuCount),
					reservations.BuildReservationLocalSSDs(existingLocalSSDType, existingLocalSSDSize),
					localReservationProject,
					"reservation1",
				),
			},
			wantCondition: true,
			wantReason:    ReservationNotFoundReason,
			wantMessage:   fmt.Sprintf(ReservationNotFoundMessage, formatReservationName("reservation1", sharedReservationProject)),
		},
		{
			name: "Shared project reservation, other shared project required, not found condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity).WithReservationProject("different")),
						rules.WithMachineTypeRule(&machineTypeWithGpu),
						rules.WithGpuRule(&machinetypes.GpuRequest{Count: machinetypes.AllocatableGpuCount(existingGpuCount), PhysicalGPUCount: existingGpuCount, Config: machinetypes.GpuConfig{
							GpuType: existingGpuType1,
						}}),
						rules.WithStorageRule(nil, nil, nil, &existingLocalSSDCount),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					machineTypeWithGpu,
					"minCpuPlatform",
					reservations.BuildReservationAccelerators(existingGpuType1, existingGpuCount),
					reservations.BuildReservationLocalSSDs(existingLocalSSDType, existingLocalSSDSize),
					localReservationProject,
					"reservation1",
				),
			},
			wantCondition: true,
			wantReason:    ReservationNotFoundReason,
			wantMessage:   fmt.Sprintf(ReservationNotFoundMessage, "different/reservation1"),
		},
		{
			name: "Reservation found, only any affinity, unusable condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithMachineTypeRule(&machineTypeWithGpu),
						rules.WithGpuRule(&machinetypes.GpuRequest{Count: machinetypes.AllocatableGpuCount(existingGpuCount), PhysicalGPUCount: existingGpuCount, Config: machinetypes.GpuConfig{
							GpuType: existingGpuType1,
						}}),
						rules.WithStorageRule(nil, nil, nil, &existingLocalSSDCount),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ false,
					"zone",
					machineTypeWithGpu,
					"minCpuPlatform",
					reservations.BuildReservationAccelerators(existingGpuType1, existingGpuCount),
					reservations.BuildReservationLocalSSDs(existingLocalSSDType, existingLocalSSDSize),
					localReservationProject,
					"reservation1",
				),
			},

			wantCondition: true,
			wantReason:    ReservationUnusableReason,
			wantMessage:   fmt.Sprintf(ReservationUnusableMessageWithReason, "reservation1", "any affinity reservation cannot be consumed"),
		},
		{
			name: "Reservation found, reservation has different SSD type, unusable",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithMachineTypeRule(&machineTypeWithGpu),
						rules.WithGpuRule(&machinetypes.GpuRequest{Count: machinetypes.AllocatableGpuCount(existingGpuCount), PhysicalGPUCount: existingGpuCount, Config: machinetypes.GpuConfig{
							GpuType: existingGpuType1,
						}}),
						rules.WithStorageRule(nil, nil, nil, &existingLocalSSDCount),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					machineTypeWithGpu,
					"minCpuPlatform",
					reservations.BuildReservationAccelerators(existingGpuType1, existingGpuCount),
					reservations.BuildReservationLocalSSDs("SCSI", existingLocalSSDSize),
					localReservationProject,
					"reservation1",
				),
			},

			wantCondition: true,
			wantReason:    ReservationUnusableReason,
			wantMessage:   fmt.Sprintf(ReservationUnusableMessageWithReason, "reservation1", "local SSD mismatch, requested 750 GB NVME SSD, but it's missing from reservation"),
		},
		{
			name: "Reservation found, reservation has different SSD size, unusable",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithMachineTypeRule(&machineTypeWithGpu),
						rules.WithGpuRule(&machinetypes.GpuRequest{Count: machinetypes.AllocatableGpuCount(existingGpuCount), PhysicalGPUCount: existingGpuCount, Config: machinetypes.GpuConfig{
							GpuType: existingGpuType1,
						}}),
						rules.WithStorageRule(nil, nil, nil, &existingLocalSSDCount),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					machineTypeWithGpu,
					"minCpuPlatform",
					reservations.BuildReservationAccelerators(existingGpuType1, existingGpuCount),
					reservations.BuildReservationLocalSSDs(existingLocalSSDType, 9999999),
					localReservationProject,
					"reservation1",
				),
			},

			wantCondition: true,
			wantReason:    ReservationUnusableReason,
			wantMessage:   fmt.Sprintf(ReservationUnusableMessageWithReason, "reservation1", "local SSD mismatch, requested 750 GB NVME SSD, reservation has 9999999 GB NVME SSD"),
		},
		{
			name: "Reservation found, reservation has different accelerator type, unusable",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithMachineTypeRule(&machineTypeWithGpu),
						rules.WithGpuRule(&machinetypes.GpuRequest{Count: machinetypes.AllocatableGpuCount(existingGpuCount), PhysicalGPUCount: existingGpuCount, Config: machinetypes.GpuConfig{
							GpuType: existingGpuType1,
						}}),
						rules.WithStorageRule(nil, nil, nil, &existingLocalSSDCount),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					machineTypeWithGpu,
					"minCpuPlatform",
					reservations.BuildReservationAccelerators("foobar", existingGpuCount),
					reservations.BuildReservationLocalSSDs(existingLocalSSDType, existingLocalSSDSize),
					localReservationProject,
					"reservation1",
				),
			},

			wantCondition: true,
			wantReason:    ReservationUnusableReason,
			wantMessage:   fmt.Sprintf(ReservationUnusableMessageWithReason, "reservation1", "accelerator mismatch, requested 2 chips of nvidia-a100-80gb, but it's missing from reservation"),
		},
		{
			name: "Reservation found, reservation has different accelerator count, unusable",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithMachineTypeRule(&machineTypeWithGpu),
						rules.WithGpuRule(&machinetypes.GpuRequest{Count: machinetypes.AllocatableGpuCount(existingGpuCount), PhysicalGPUCount: existingGpuCount, Config: machinetypes.GpuConfig{
							GpuType: existingGpuType1,
						}}),
						rules.WithStorageRule(nil, nil, nil, &existingLocalSSDCount),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					machineTypeWithGpu,
					"minCpuPlatform",
					reservations.BuildReservationAccelerators(existingGpuType1, 9999999),
					reservations.BuildReservationLocalSSDs(existingLocalSSDType, existingLocalSSDSize),
					localReservationProject,
					"reservation1",
				),
			},

			wantCondition: true,
			wantReason:    ReservationUnusableReason,
			wantMessage:   fmt.Sprintf(ReservationUnusableMessageWithReason, "reservation1", "accelerator mismatch, requested 2 chips of nvidia-a100-80gb, reservation has 9999999 chips of nvidia-a100-80gb"),
		},
		{
			name: "Reservation found, different machine type, unusable condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithMachineTypeRule(&existingMachineType),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					machineTypeWithGpu,
					"minCpuPlatform",
					nil,
					nil,
					localReservationProject,
					"reservation1",
				),
			},

			wantCondition: true,
			wantReason:    ReservationUnusableReason,
			wantMessage:   fmt.Sprintf(ReservationUnusableMessageWithReason, "reservation1", fmt.Sprintf("machine type mismatch, requested %v, reservation has %v", existingMachineType, machineTypeWithGpu)),
		},
		{
			name: "Reservation found, different custom machine type, unusable condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithMachineTypeRule(&customMachineType2),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					customMachineType1,
					"minCpuPlatform",
					nil,
					nil,
					localReservationProject,
					"reservation1",
				),
			},

			wantCondition: true,
			wantReason:    ReservationUnusableReason,
			wantMessage:   fmt.Sprintf(ReservationUnusableMessageWithReason, "reservation1", fmt.Sprintf("machine type mismatch, requested %v, reservation has %v", customMachineType2, customMachineType1)),
		},
		{
			name: "Reservation found, different machine family, unusable condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithMachineFamilyRule(&existingMachineFamily),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					machineTypeWithGpu,
					"minCpuPlatform",
					nil,
					nil,
					localReservationProject,
					"reservation1",
				),
			},

			wantCondition: true,
			wantReason:    ReservationUnusableReason,
			wantMessage:   fmt.Sprintf(ReservationUnusableMessageWithReason, "reservation1", "machine family mismatch: requested N2, reservation has a2"),
		},
		{
			name: "Reservation with custom machine type found, different machine family, unusable condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithMachineFamilyRule(&existingMachineFamily),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					customMachineType2,
					"minCpuPlatform",
					nil,
					nil,
					localReservationProject,
					"reservation1",
				),
			},

			wantCondition: true,
			wantReason:    ReservationUnusableReason,
			wantMessage:   fmt.Sprintf(ReservationUnusableMessageWithReason, "reservation1", "machine family mismatch: requested N2, reservation has n2d"),
		},
		{
			name: "Multiple reservations, one is missing",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationAffinity(reservations.AnyAffinity)),
						rules.WithMachineFamilyRule(&existingMachineFamily),
					),
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithMachineFamilyRule(&existingMachineFamily),
					),
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity).WithReservationProject(sharedReservationProject)),
						rules.WithMachineTypeRule(&machineTypeWithGpu),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					existingMachineType,
					"minCpuPlatform",
					nil,
					nil,
					localReservationProject,
					"reservation1",
				),
			},

			wantCondition: true,
			wantReason:    ReservationNotFoundReason,
			wantMessage:   fmt.Sprintf(ReservationNotFoundMessage, formatReservationName("reservation1", sharedReservationProject)),
		},
		{
			name: "Any reservation required, no condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationAffinity(reservations.AnyAffinity)),
						rules.WithMachineFamilyRule(&existingMachineFamily),
					),
				}),
			),
			reservations:  []*gceapiv1.Reservation{},
			wantCondition: false,
		},
		{
			name: "Reservation found, full match, no condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithMachineTypeRule(&machineTypeWithGpu),
						rules.WithGpuRule(&machinetypes.GpuRequest{Count: machinetypes.AllocatableGpuCount(existingGpuCount), PhysicalGPUCount: existingGpuCount, Config: machinetypes.GpuConfig{
							GpuType: existingGpuType1,
						}}),
						rules.WithStorageRule(nil, nil, nil, &existingLocalSSDCount),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					machineTypeWithGpu,
					"minCpuPlatform",
					reservations.BuildReservationAccelerators(existingGpuType1, existingGpuCount),
					reservations.BuildReservationLocalSSDs(existingLocalSSDType, existingLocalSSDSize),
					localReservationProject,
					"reservation1",
				),
			},
			wantCondition: false,
		},
		{
			name: "Reservation with custom machine type found, full match, no condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithMachineTypeRule(&customMachineType1),
						rules.WithGpuRule(&machinetypes.GpuRequest{Count: machinetypes.AllocatableGpuCount(existingGpuCount), PhysicalGPUCount: existingGpuCount, Config: machinetypes.GpuConfig{
							GpuType: machinetypes.NvidiaTeslaP100.Name(),
						}}),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					customMachineType1,
					"minCpuPlatform",
					reservations.BuildReservationAccelerators(machinetypes.NvidiaTeslaP100.Name(), existingGpuCount),
					nil,
					localReservationProject,
					"reservation1",
				),
			},
			wantCondition: false,
		},
		{
			name: "Reservation found, machine family matches, no condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithMachineFamilyRule(&machineFamilyWithGpu),
						rules.WithGpuRule(&machinetypes.GpuRequest{Count: machinetypes.AllocatableGpuCount(existingGpuCount), PhysicalGPUCount: existingGpuCount, Config: machinetypes.GpuConfig{
							GpuType: existingGpuType1,
						}}),
						rules.WithStorageRule(nil, nil, nil, &existingLocalSSDCount),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					machineTypeWithGpu,
					"minCpuPlatform",
					reservations.BuildReservationAccelerators(existingGpuType1, existingGpuCount),
					reservations.BuildReservationLocalSSDs(existingLocalSSDType, existingLocalSSDSize),
					localReservationProject,
					"reservation1",
				),
			},
			wantCondition: false,
		},
		{
			name: "Shared reservation found, full match, no condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity).WithReservationProject(sharedReservationProject)),
						rules.WithMachineTypeRule(&machineTypeWithGpu),
						rules.WithGpuRule(&machinetypes.GpuRequest{Count: machinetypes.AllocatableGpuCount(existingGpuCount), PhysicalGPUCount: existingGpuCount, Config: machinetypes.GpuConfig{
							GpuType: existingGpuType1,
						}}),
						rules.WithStorageRule(nil, nil, nil, &existingLocalSSDCount),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					machineTypeWithGpu,
					"minCpuPlatform",
					reservations.BuildReservationAccelerators(existingGpuType1, existingGpuCount),
					reservations.BuildReservationLocalSSDs(existingLocalSSDType, existingLocalSSDSize),
					sharedReservationProject,
					"reservation1",
				),
			},
			wantCondition: false,
		},
		{
			name: "Reservation found, full match, no accelerator, no condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithMachineTypeRule(&machineTypeWithGpu),
						rules.WithStorageRule(nil, nil, nil, &existingLocalSSDCount),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					machineTypeWithGpu,
					"minCpuPlatform",
					nil,
					reservations.BuildReservationLocalSSDs(existingLocalSSDType, existingLocalSSDSize),
					localReservationProject,
					"reservation1",
				),
			},
			wantCondition: false,
		},
		{
			name: "Reservation found, full match, no local SSD, no condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithMachineTypeRule(&machineTypeWithGpu),
						rules.WithGpuRule(&machinetypes.GpuRequest{Count: machinetypes.AllocatableGpuCount(existingGpuCount), PhysicalGPUCount: existingGpuCount, Config: machinetypes.GpuConfig{
							GpuType: existingGpuType1,
						}}),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					machineTypeWithGpu,
					"minCpuPlatform",
					reservations.BuildReservationAccelerators(existingGpuType1, existingGpuCount),
					nil,
					localReservationProject,
					"reservation1",
				),
			},
			wantCondition: false,
		},
		{
			name: "Reservation found, full match, only machine type, no condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithMachineTypeRule(&existingMachineType),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					existingMachineType,
					"minCpuPlatform",
					nil,
					nil,
					localReservationProject,
					"reservation1",
				),
			},
			wantCondition: false,
		},
		{
			name: "Reservation found, full match, only matching machine family, no condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithMachineFamilyRule(&existingMachineFamily),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					existingMachineType,
					"minCpuPlatform",
					nil,
					nil,
					localReservationProject,
					"reservation1",
				),
			},
			wantCondition: false,
		},
		{
			name: "Reservation block found, full match, no condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity).WithReservationBlock("rb1")),
						rules.WithMachineFamilyRule(&existingMachineFamily),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					true,
					"zone",
					existingMachineType,
					"minCpuPlatform",
					nil,
					nil,
					localReservationProject,
					"reservation1",
				),
			},
			reservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				{Project: localReservationProject, Zone: "zone", Name: "reservation1"}: {reservations.BuildSingleReservationBlock("rb1", 1, 0, "zone")},
			},
			wantCondition: false,
		},
		{
			name: "Reservation block found, full match, project specified in rules, no condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity).WithReservationProject(localReservationProject).WithReservationBlock("rb1")),
						rules.WithMachineFamilyRule(&existingMachineFamily),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					true,
					"zone",
					existingMachineType,
					"minCpuPlatform",
					nil,
					nil,
					localReservationProject,
					"reservation1",
				),
			},
			reservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				{Project: localReservationProject, Zone: "zone", Name: "reservation1"}: {reservations.BuildSingleReservationBlock("rb1", 1, 0, "zone")},
			},
			wantCondition: false,
		},
		{
			name: "Block in shared reservation found, full match, no condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity).WithReservationProject(sharedReservationProject)),
						rules.WithMachineTypeRule(&machineTypeWithGpu),
						rules.WithGpuRule(&machinetypes.GpuRequest{Count: machinetypes.AllocatableGpuCount(existingGpuCount), PhysicalGPUCount: existingGpuCount, Config: machinetypes.GpuConfig{
							GpuType: existingGpuType1,
						}}),
						rules.WithStorageRule(nil, nil, nil, &existingLocalSSDCount),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					true,
					"zone",
					machineTypeWithGpu,
					"minCpuPlatform",
					reservations.BuildReservationAccelerators(existingGpuType1, existingGpuCount),
					reservations.BuildReservationLocalSSDs(existingLocalSSDType, existingLocalSSDSize),
					sharedReservationProject,
					"reservation1",
				),
			},
			reservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				{Project: localReservationProject, Zone: "zone", Name: "reservation1"}: {reservations.BuildSingleReservationBlock("rb1", 1, 0, "zone")},
			},
			wantCondition: false,
		},
		{
			name: "Reservation block not found in local project, unusable condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity).WithReservationBlock("rb0")),
						rules.WithMachineFamilyRule(&existingMachineFamily),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					true,
					"zone",
					existingMachineType,
					"minCpuPlatform",
					nil,
					nil,
					localReservationProject,
					"reservation1",
				),
			},
			reservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				{Project: localReservationProject, Zone: "zone", Name: "reservation1"}: {reservations.BuildSingleReservationBlock("rb1", 1, 0, "zone")},
			},
			wantCondition: true,
			wantReason:    ReservationBlockUnusableReason,
			wantMessage:   fmt.Sprintf(ReservationBlockUnusableMessage, "rb0", formatReservationName("reservation1", localReservationProject)),
		},
		{
			name: "Reservation block not found in shared project, unusable condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity).WithReservationProject(sharedReservationProject).WithReservationBlock("rb0")),
						rules.WithMachineFamilyRule(&existingMachineFamily),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					true,
					"zone",
					existingMachineType,
					"minCpuPlatform",
					nil,
					nil,
					sharedReservationProject,
					"reservation1",
				),
			},
			reservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				{Project: localReservationProject, Zone: "zone", Name: "reservation1"}: {reservations.BuildSingleReservationBlock("rb1", 1, 0, "zone")},
			},
			wantCondition: true,
			wantReason:    ReservationBlockUnusableReason,
			wantMessage:   fmt.Sprintf(ReservationBlockUnusableMessage, "rb0", formatReservationName("reservation1", sharedReservationProject)),
		},
		{
			name: "Multiple reservations, no condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationAffinity(reservations.AnyAffinity)),
						rules.WithMachineFamilyRule(&existingMachineFamily),
					),
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithMachineFamilyRule(&existingMachineFamily),
					),
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation1").WithReservationAffinity(reservations.SpecificAffinity).WithReservationProject(sharedReservationProject)),
						rules.WithMachineTypeRule(&machineTypeWithGpu),
					),
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("aggregate").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithTpuRule(labels.TpuV4PodsliceValue, 8, "2x2x2"),
						rules.WithMachineTypeRule(&existingMachineType),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					machineTypeWithGpu,
					"minCpuPlatform",
					nil,
					nil,
					sharedReservationProject,
					"reservation1",
				),
				reservations.BuildReservationWithId(
					1,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					existingMachineType,
					"minCpuPlatform",
					nil,
					nil,
					localReservationProject,
					"reservation1",
				),
				reservations.BuildReservationWithId(
					2,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					existingMachineType,
					"minCpuPlatform",
					nil,
					nil,
					localReservationProject,
					"reservation1",
				),
				{
					Name:                        "aggregate",
					Id:                          3,
					SelfLink:                    fmt.Sprintf("projects/%s/reservations/%s", localReservationProject, "aggregate"),
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
			},
			wantCondition: false,
		},
		{
			name: "Aggregate reservation, no condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("aggregate").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithTpuRule(labels.TpuV4PodsliceValue, 8, "2x2x2"),
						rules.WithMachineTypeRule(&existingMachineType),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				{
					Name:                        "aggregate",
					SelfLink:                    fmt.Sprintf("projects/%s/reservations/%s", localReservationProject, "aggregate"),
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
			},

			wantCondition: false,
		},
		{
			name: "Aggregate reservation, type mismatch, unusable condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("aggregate").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithTpuRule(labels.TpuV4PodsliceValue, 4, "2x2x4"),
						rules.WithMachineTypeRule(&existingMachineType),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				{
					Name:                        "aggregate",
					SelfLink:                    fmt.Sprintf("projects/%s/reservations/%s", localReservationProject, "aggregate"),
					SpecificReservationRequired: true,
					AggregateReservation: &gceapiv1.AllocationAggregateReservation{
						ReservedResources: []*gceapiv1.AllocationAggregateReservationReservedResourceInfo{
							{
								Accelerator: &gceapiv1.AllocationAggregateReservationReservedResourceInfoAccelerator{
									AcceleratorType:  "projects/whatever/zones/us-central2-b/acceleratorTypes/ct5p",
									AcceleratorCount: 4,
								},
							},
						},
					},
				},
			},

			wantCondition: true,
			wantReason:    ReservationUnusableReason,
			wantMessage:   fmt.Sprintf(ReservationUnusableMessageWithReason, formatReservationName("aggregate", ""), "requested TPU accelerator {tpu-v4-podslice, 2x2x4, 4}, but it's missing from reservation"),
		},
		{
			name: "Aggregate reservation, count mismatch, unusable condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("aggregate").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithTpuRule(labels.TpuV4PodsliceValue, 4, "2x2x4"),
						rules.WithMachineTypeRule(&existingMachineType),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				{
					Name:                        "aggregate",
					SelfLink:                    fmt.Sprintf("projects/%s/reservations/%s", localReservationProject, "aggregate"),
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
					},
				},
			},

			wantCondition: true,
			wantReason:    ReservationUnusableReason,
			wantMessage:   fmt.Sprintf(ReservationUnusableMessageWithReason, formatReservationName("aggregate", ""), "requested TPU accelerator {tpu-v4-podslice, 2x2x4, 4}, but it's missing from reservation"),
		},
		{
			name: "Aggregate reservation required, specific is available, unusable condition",
			crd: crd.NewTestCrd(crd.WithLabel("crd-1"),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithReservationsRule(rules.NewReservation().WithReservationName("aggregate").WithReservationAffinity(reservations.SpecificAffinity)),
						rules.WithTpuRule(labels.TpuV4PodsliceValue, 4, "2x2x4"),
						rules.WithMachineTypeRule(&existingMachineType),
					),
				}),
			),
			reservations: []*gceapiv1.Reservation{
				// Specific reservation claiming to be aggregate
				reservations.BuildReservationWithId(
					0,
					"READY",
					/*specificReservationRequired=*/ true,
					"zone",
					existingMachineType,
					"minCpuPlatform",
					nil,
					nil,
					localReservationProject,
					"aggregate",
				),
			},

			wantCondition: true,
			wantReason:    ReservationUnusableReason,
			wantMessage:   fmt.Sprintf(ReservationUnusableMessageWithReason, formatReservationName("aggregate", ""), "tpu requested for non aggregate reservation"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			puller := newMockReservationsPuller(localReservationProject, []string{sharedReservationProject}, tc.reservations)
			blocksPuller := reservations.NewBlocksPuller(reservations.NewFakeBlocksPullerProvider(tc.reservationBlocks, nil), puller)
			blocksPuller.Loop()

			rsvCache := NewReservationsCache(puller)
			rsvCache.PopulateForCrds([]crd.CRD{tc.crd})

			cloudProvider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			ch := reservationConfigChecker{
				reservationBlocksPuller: blocksPuller,
				rsvCache:                rsvCache,
				localSsdProvider:        ssdProvider,
				cloudProvider:           cloudProvider,
			}

			var condition *metav1.Condition
			for _, rule := range tc.crd.Rules() {
				if c := ch.checkRule(rule); c != nil {
					condition = c
					break
				}
			}

			if tc.wantReason != "" {
				if assert.NotNil(t, condition) {
					assert.Equal(t, RuleMisconfiguredCondition, condition.Type)
					assert.Equal(t, tc.wantReason, condition.Reason)
					assert.Equal(t, tc.wantMessage, condition.Message)
				}
			} else {
				assert.Nil(t, condition)
			}
		})
	}
}
