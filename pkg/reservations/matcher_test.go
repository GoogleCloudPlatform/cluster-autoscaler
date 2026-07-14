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

package reservations

import (
	"testing"

	"github.com/stretchr/testify/assert"
	gce_api "google.golang.org/api/compute/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
)

func TestMatchReservationShapeMigNotExist(t *testing.T) {
	defaultMachineType := "machine-type-1"
	defaultZone := "zone-1"
	defaultRsvStatus := "READY"
	defaultspecificReservationRequired := false
	defaultMinCPUPlatform := "Automatic"
	defaultAcceleratorType := "acc-1"
	defaultAcceleratorCount := machinetypes.PhysicalGpuCount(2)

	testCases := []struct {
		name         string
		reservation  *gce_api.Reservation
		nodePoolSpec gkeclient.NodePoolSpec
		expected     bool
	}{
		{
			name: "Reservation not ready - No matching",
			reservation: BuildReservation( /*rsvStatus=*/ "UNREADY",
				defaultspecificReservationRequired,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				/*accelerators=*/ nil,
				/*localSSDs=*/ nil),
			nodePoolSpec: BuildNodePoolSpecIfMigNotExist(defaultMachineType,
				defaultMinCPUPlatform,
				/*accelerators=*/ nil,
				/*localSSDConfig=*/ nil),
			expected: false,
		},
		{
			name: "Reservation has different zone - No matching",
			reservation: BuildReservation(defaultRsvStatus,
				defaultspecificReservationRequired,
				"different-zone",
				defaultMachineType,
				defaultMinCPUPlatform,
				/*accelerators=*/ nil,
				/*localSSDs=*/ nil),
			nodePoolSpec: BuildNodePoolSpecIfMigNotExist(defaultMachineType,
				defaultMinCPUPlatform,
				/*accelerators=*/ nil,
				/*localSSDConfig=*/ nil),
			expected: false,
		},
		{
			name: "Reservation has different machine type - No matching",
			reservation: BuildReservation(defaultRsvStatus,
				defaultspecificReservationRequired,
				defaultZone,
				"machine-type-2",
				defaultMinCPUPlatform,
				/*accelerators=*/ nil,
				/*localSSDs=*/ nil),
			nodePoolSpec: BuildNodePoolSpecIfMigNotExist(defaultMachineType,
				defaultMinCPUPlatform,
				/*accelerators=*/ nil,
				/*localSSDConfig=*/ nil),
			expected: false,
		},
		{
			name: "Different Minimum CPU platform - No matching",
			reservation: BuildReservation(defaultRsvStatus,
				defaultspecificReservationRequired,
				defaultZone,
				defaultMachineType,
				/*minCpuPlatform=*/ "intel-broadwell",
				/*accelerators=*/ nil,
				/*localSSDs=*/ nil),
			nodePoolSpec: BuildNodePoolSpecIfMigNotExist(defaultMachineType,
				/*minCpuPlatform=*/ "intel-skylake",
				/*accelerators=*/ nil,
				/*localSSDConfig=*/ nil),
			expected: false,
		},
		{
			name: "Reservation has different Accelerator type - No matching",
			reservation: BuildReservation(defaultRsvStatus,
				defaultspecificReservationRequired,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				BuildReservationAccelerators("different-acc-type", defaultAcceleratorCount),
				/*localSSDs=*/ nil),
			nodePoolSpec: BuildNodePoolSpecIfMigNotExist(defaultMachineType,
				defaultMinCPUPlatform,
				BuildNodePoolSpecAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				/*localSSDConfig=*/ nil),
			expected: false,
		},
		{
			name: "Reservation has different Accelerator count - No matching",
			reservation: BuildReservation(defaultRsvStatus,
				defaultspecificReservationRequired,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				BuildReservationAccelerators(defaultAcceleratorType /*count=*/, 5),
				/*localSSDs=*/ nil),
			nodePoolSpec: BuildNodePoolSpecIfMigNotExist(defaultMachineType,
				defaultMinCPUPlatform,
				BuildNodePoolSpecAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				/*localSSDConfig=*/ nil),
			expected: false,
		},
		{
			name: "Reservation matching",
			reservation: BuildReservation(defaultRsvStatus,
				defaultspecificReservationRequired,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				BuildReservationAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				/*localSSDs=*/ nil),
			nodePoolSpec: BuildNodePoolSpecIfMigNotExist(defaultMachineType,
				defaultMinCPUPlatform,
				BuildNodePoolSpecAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				/*localSSDConfig=*/ nil),
			expected: true,
		},
		{
			// `a2-ultragpu-4g` has 4 SSD with SCSI interface
			name: "Machine type with automatic ephemeral local ssd - Reservation matching",
			reservation: BuildReservation(defaultRsvStatus,
				defaultspecificReservationRequired,
				defaultZone,
				/*machineType=*/ "a2-ultragpu-4g",
				defaultMinCPUPlatform,
				BuildReservationAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				BuildReservationLocalSSDs( /*localSSDType=*/ ssdNVME /*size=*/, 1500)),
			nodePoolSpec: BuildNodePoolSpecIfMigNotExist( /*machineType=*/ "a2-ultragpu-4g",
				defaultMinCPUPlatform,
				BuildNodePoolSpecAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				&gkeclient.LocalSSDConfig{EphemeralStorageConfig: &gke_api_beta.EphemeralStorageConfig{LocalSsdCount: 4}}),
			expected: true,
		},
		{
			// default min cpu platform for the machine n2-standard-4 is Intel Cascadelake.
			name: "Machine type: n2-standard-4, min cpu platform in reservation is intel-cascadelake and in node group is Automatic - Reservation matching",
			reservation: BuildReservation(defaultRsvStatus,
				defaultspecificReservationRequired,
				defaultZone,
				/*machineType=*/ "n2-standard-4",
				/*minCpuPlatform=*/ "intel-cascadelake",
				/*accelerators=*/ nil,
				/*localSSDs=*/ nil),
			nodePoolSpec: BuildNodePoolSpecIfMigNotExist( /*machineType=*/ "n2-standard-4",
				defaultMinCPUPlatform,
				/*accelerators=*/ nil,
				/*localSSDConfig=*/ nil),
			expected: true,
		},
		{
			// default min cpu platform for the machine n2-standard-4 is Intel Cascadelake.
			name: "Machine type: n2-standard-4, min cpu platform in reservation is Automatic and in node group is intel-cascadelake - Reservation matching",
			reservation: BuildReservation(defaultRsvStatus,
				defaultspecificReservationRequired,
				defaultZone,
				/*machineType=*/ "n2-standard-4",
				defaultMinCPUPlatform,
				/*accelerators=*/ nil,
				/*localSSDs=*/ nil),
			nodePoolSpec: BuildNodePoolSpecIfMigNotExist /*machineType=*/ ( /*machineType=*/ "n2-standard-4",
				/*minCpuPlatform=*/ "intel-cascadelake",
				/*accelerators=*/ nil,
				/*localSSDConfig=*/ nil),
			expected: true,
		},
		{
			// default min cpu platform for the machine n2-standard-4 is Intel Cascadelake.
			name: "Machine type: n2-standard-4, min cpu platform in reservation in node group is intel-cascadelake - Reservation matching",
			reservation: BuildReservation(defaultRsvStatus,
				defaultspecificReservationRequired,
				defaultZone,
				/*machineType=*/ "n2-standard-4",
				/*minCpuPlatform=*/ "intel-cascadelake",
				/*accelerators=*/ nil,
				/*localSSDs=*/ nil),
			nodePoolSpec: BuildNodePoolSpecIfMigNotExist( /*machineType=*/ "n2-standard-4",
				/*minCpuPlatform=*/ "intel-cascadelake",
				/*accelerators=*/ nil,
				/*localSSDConfig=*/ nil),
			expected: true,
		},
		{
			// default min cpu platform for the machine n2-standard-4 is Intel Cascadelake.
			name: "Machine type: n2-standard-4, min cpu platform in reservation and in node group is Automatic - Reservation matching",
			reservation: BuildReservation(defaultRsvStatus,
				defaultspecificReservationRequired,
				defaultZone,
				/*machineType=*/ "n2-standard-4",
				defaultMinCPUPlatform,
				/*accelerators=*/ nil,
				/*localSSDs=*/ nil),
			nodePoolSpec: BuildNodePoolSpecIfMigNotExist( /*machineType=*/ "n2-standard-4",
				defaultMinCPUPlatform,
				/*accelerators=*/ nil,
				/*localSSDConfig=*/ nil),
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			mig := gke.NewTestGkeMigBuilder().SetSpec(&tc.nodePoolSpec).SetGceRefZone(defaultZone).Build()
			actual := reservationMatch(provider, mig, tc.reservation, localssdsize.NewSimpleLocalSSDProvider())
			if actual != tc.expected {
				t.Errorf("Test: \"%v\" failed, expected matching: %v got: %v", tc.name, tc.expected, actual)
			}
		})
	}
}

func TestMatchReservationShapeMigExist(t *testing.T) {
	defaultMachineType := "machine-type-1"
	defaultZone := "zone-1"
	defaultRsvStatus := "READY"
	defaultspecificReservationRequired := false
	defaultMinCPUPlatform := "Automatic"
	defaultAcceleratorType := "acc-1"
	defaultAcceleratorCount := machinetypes.PhysicalGpuCount(2)
	defaultSSDType := ssdSCSI
	defaultSSDSizeInGB := 750
	defaultSSDCount := 2 // Each SSD is 375 GB.

	testCases := []struct {
		name         string
		reservation  *gce_api.Reservation
		nodePoolSpec gkeclient.NodePoolSpec
		expected     bool
	}{
		{
			name: "Reservation has different Accelerator type - No matching",
			reservation: BuildReservation(defaultRsvStatus,
				defaultspecificReservationRequired,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				BuildReservationAccelerators("different-acc-type", defaultAcceleratorCount),
				/*localSSDs=*/ nil),
			nodePoolSpec: BuildNodePoolSpecIfMigExists(defaultMachineType,
				defaultMinCPUPlatform,
				BuildNodePoolSpecAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				/*localSSDConfig=*/ nil),
			expected: false,
		},
		{
			name: "Reservation has different Accelerator count - No matching",
			reservation: BuildReservation(defaultRsvStatus,
				defaultspecificReservationRequired,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				BuildReservationAccelerators(defaultAcceleratorType /*count=*/, 5),
				/*localSSDs=*/ nil),
			nodePoolSpec: BuildNodePoolSpecIfMigExists(defaultMachineType,
				defaultMinCPUPlatform,
				BuildNodePoolSpecAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				/*localSSDConfig=*/ nil),
			expected: false,
		},
		{
			name: "Only Accelerator no Local SSD - Reservation Matching",
			reservation: BuildReservation(defaultRsvStatus,
				defaultspecificReservationRequired,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				BuildReservationAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				/*localSSDs=*/ nil),
			nodePoolSpec: BuildNodePoolSpecIfMigExists(defaultMachineType,
				defaultMinCPUPlatform,
				BuildNodePoolSpecAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				/*localSSDConfig=*/ nil),
			expected: true,
		},
		{
			name: "Reservation has different SSD type - No matching",
			reservation: BuildReservation(defaultRsvStatus,
				defaultspecificReservationRequired,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				/*accelerators=*/ nil,
				BuildReservationLocalSSDs( /*localSSDType=*/ ssdNVME, defaultSSDSizeInGB)),
			nodePoolSpec: BuildNodePoolSpecIfMigExists(defaultMachineType,
				defaultMinCPUPlatform,
				/*accelerators=*/ nil,
				BuildNodePoolSpecLocalSSDs(defaultSSDType, defaultSSDCount)),
			expected: false,
		},
		{
			name: "Reservation has different SSD size - No matching",
			reservation: BuildReservation(defaultRsvStatus,
				defaultspecificReservationRequired,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				/*accelerators=*/ nil,
				BuildReservationLocalSSDs(defaultSSDType /*size=*/, 1500)),
			nodePoolSpec: BuildNodePoolSpecIfMigExists(defaultMachineType,
				defaultMinCPUPlatform,
				/*accelerators=*/ nil,
				BuildNodePoolSpecLocalSSDs(defaultSSDType, defaultSSDCount)),
			expected: false,
		},
		{
			name: "Reservation without Accelerator and Local SSDs - matching",
			reservation: BuildReservation(defaultRsvStatus,
				defaultspecificReservationRequired,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				/*accelerators=*/ nil,
				/*localSSDs=*/ nil),
			nodePoolSpec: BuildNodePoolSpecIfMigExists(defaultMachineType,
				defaultMinCPUPlatform,
				/*accelerators=*/ nil,
				/*localSSDConfig=*/ nil),
			expected: true,
		},
		{
			name: "Reservation with Accelerator and Local SSDs - matching",
			reservation: BuildReservation(defaultRsvStatus,
				defaultspecificReservationRequired,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				BuildReservationAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				BuildReservationLocalSSDs(defaultSSDType, defaultSSDSizeInGB)),
			nodePoolSpec: BuildNodePoolSpecIfMigExists(defaultMachineType,
				defaultMinCPUPlatform,
				BuildNodePoolSpecAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				BuildNodePoolSpecLocalSSDs(defaultSSDType, defaultSSDCount)),
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			mig := gke.NewTestGkeMigBuilder().SetSpec(&tc.nodePoolSpec).SetGceRefZone(defaultZone).SetExist(true).Build()
			actual := reservationMatch(provider, mig, tc.reservation, localssdsize.NewSimpleLocalSSDProvider())
			if actual != tc.expected {
				t.Errorf("Test: \"%v\" failed, expected matching: %v got: %v", tc.name, tc.expected, actual)
			}
		})
	}
}

func TestMatchReservationShapeWithReservationAffinity(t *testing.T) {
	defaultMachineType := "machine-type-1"
	defaultZone := "zone-1"
	defaultMinCPUPlatform := "Automatic"
	defaultReservationName := "default-reservation"
	defaultReservationType := gkeclient.ReservationAffinityAny

	testCases := []struct {
		name         string
		reservation  *gce_api.Reservation
		nodePoolSpec gkeclient.NodePoolSpec
		expected     bool
	}{
		{
			name: "Reservation and nodepool with Specific Reservation required - Matching",
			reservation: BuildReservationWithSpecificReservationRequired(defaultReservationName,
				defaultMachineType, defaultZone),
			nodePoolSpec: BuildNodePoolSpecWithReservationAffinity(gkeclient.ReservationAffinitySpecific,
				defaultReservationName, defaultMachineType),
			expected: true,
		},
		{
			name: "Reservation and nodepool with Specific Reservation required but with different name - No matching",
			reservation: BuildReservationWithSpecificReservationRequired(defaultReservationName,
				defaultMachineType, defaultZone),
			nodePoolSpec: BuildNodePoolSpecWithReservationAffinity(gkeclient.ReservationAffinitySpecific,
				/*reservationAffinityName=*/ "abc-reservation", defaultMachineType),
			expected: false,
		},
		{
			name: "Reservation with Specific Reservation required and nodepool without reservation affinity - No matching",
			reservation: BuildReservationWithSpecificReservationRequired(defaultReservationName,
				defaultMachineType, defaultZone),
			nodePoolSpec: BuildNodePoolSpecIfMigExists(defaultMachineType,
				defaultMinCPUPlatform,
				/*accelerators=*/ nil,
				/*localSSDConfig=*/ nil),
			expected: false,
		},
		{
			name: "Reservation with Specific Reservation required and nodepool with no reservation required - No matching",
			reservation: BuildReservationWithSpecificReservationRequired(defaultReservationName,
				defaultMachineType, defaultZone),
			nodePoolSpec: BuildNodePoolSpecWithReservationAffinity(gkeclient.ReservationAffinityNone,
				defaultReservationName, defaultMachineType),
			expected: false,
		},
		{
			name: "Reservation with Specific Reservation required and nodepool with Any Reservation required - No matching",
			reservation: BuildReservationWithSpecificReservationRequired(defaultReservationName,
				defaultMachineType, defaultZone),
			nodePoolSpec: BuildNodePoolSpecWithReservationAffinity(defaultReservationType,
				defaultReservationName, defaultMachineType),
			expected: false,
		},
		{
			name:        "Reservation without Specific Reservation required and nodepool with Specific Reservation required - No matching",
			reservation: BuildSingleMachineReservation(defaultMachineType, defaultZone),
			nodePoolSpec: BuildNodePoolSpecWithReservationAffinity(gkeclient.ReservationAffinityNone,
				defaultReservationName, defaultMachineType),
			expected: false,
		},
		{
			name:        "Reservation without Specific Reservation required and nodepool with no Reservation required - No matching",
			reservation: BuildSingleMachineReservation(defaultMachineType, defaultZone),
			nodePoolSpec: BuildNodePoolSpecWithReservationAffinity(gkeclient.ReservationAffinityNone,
				defaultReservationName, defaultMachineType),
			expected: false,
		},
		{
			name:        "Reservation without Specific Reservation required and nodepool with Any Reservation required - Matching",
			reservation: BuildSingleMachineReservation(defaultMachineType, defaultZone),
			nodePoolSpec: BuildNodePoolSpecWithReservationAffinity(defaultReservationType,
				defaultReservationName, defaultMachineType),
			expected: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			mig := gke.NewTestGkeMigBuilder().SetSpec(&tc.nodePoolSpec).SetGceRefZone(defaultZone).Build()
			actual := reservationMatch(provider, mig, tc.reservation, localssdsize.NewSimpleLocalSSDProvider())
			if actual != tc.expected {
				t.Errorf("Test: \"%v\" failed, expected matching: %v got: %v", tc.name, tc.expected, actual)
			}
		})
	}
}
func TestMatchingUnusedReservations(t *testing.T) {
	defaultMachineType := "machine-type-1"
	defaultZone := "zone-A"
	defaultNodeGroup := gke.NewTestGkeMigBuilder().SetGceRefZone("zone-A").SetSpec(&gkeclient.NodePoolSpec{MachineType: defaultMachineType}).Build()

	testCases := []struct {
		name              string
		reservations      []*gce_api.Reservation
		nodeGroup         cloudprovider.NodeGroup
		wantMatchingCount int
	}{
		{
			name:              "No reservations - No matching",
			reservations:      nil,
			nodeGroup:         defaultNodeGroup,
			wantMatchingCount: 0,
		},
		{
			name: "Reservation already in use - No matching",
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservation(defaultMachineType,
					defaultZone,
					/*inUseCount=*/ 1,
					/*count=*/ 1),
			},
			nodeGroup:         defaultNodeGroup,
			wantMatchingCount: 0,
		},
		{
			name: "6 similar reservations with 3 in use - 3 matches",
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservation(defaultMachineType,
					defaultZone,
					/*inUseCount=*/ 3,
					/*count=*/ 6),
			},
			nodeGroup:         defaultNodeGroup,
			wantMatchingCount: 3,
		},
		{
			name: "2 different reservations - 1 match",
			reservations: []*gce_api.Reservation{
				BuildSingleMachineReservation(defaultMachineType, defaultZone),
				BuildSingleMachineReservation( /*machineType=*/ "different", defaultZone),
			},
			nodeGroup:         defaultNodeGroup,
			wantMatchingCount: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			gotMatchingCount := MatchingUnusedReservations(provider, tc.nodeGroup, tc.reservations, localssdsize.NewSimpleLocalSSDProvider())

			if tc.wantMatchingCount != gotMatchingCount {
				t.Errorf("Test: \"%v\" failed, matching reservations count, want: %v got: %v", tc.name, tc.wantMatchingCount, gotMatchingCount)
			}
		})
	}
}

func TestMatchSpecificReservationShape(t *testing.T) {
	defaultMachineType := "n2-standard-4"
	defaultZone := "zone-1"
	defaultMinCPUPlatform := "Automatic"
	defaultAcceleratorType := "nvidia-tesla-t4"
	defaultAcceleratorCount := machinetypes.PhysicalGpuCount(2)
	defaultLocalSSDType := ssdNVME
	defaultLocalSSDSize := int64(375)

	testCases := []struct {
		name                      string
		reservation               *gce_api.Reservation
		machineType               string
		zone                      string
		localSSDs                 map[string]int64
		accelerators              map[string]machinetypes.PhysicalGpuCount
		minCpuPlatform            string
		acceleratorStrictRequests bool

		wantReservationMatch          bool
		wantReservationNoMatchReasons []string
	}{
		{
			name: "MatchingSpec",
			reservation: BuildReservation("READY",
				false,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				BuildReservationAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				BuildReservationLocalSSDs(defaultLocalSSDType, int(defaultLocalSSDSize))),
			machineType:                   defaultMachineType,
			zone:                          defaultZone,
			localSSDs:                     map[string]int64{defaultLocalSSDType: defaultLocalSSDSize},
			accelerators:                  map[string]machinetypes.PhysicalGpuCount{defaultAcceleratorType: defaultAcceleratorCount},
			minCpuPlatform:                defaultMinCPUPlatform,
			wantReservationMatch:          true,
			wantReservationNoMatchReasons: nil,
		},
		{
			name: "DifferentZone_NotMatching",
			reservation: BuildReservation("READY",
				false,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				BuildReservationAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				BuildReservationLocalSSDs(defaultLocalSSDType, int(defaultLocalSSDSize))),
			machineType:                   defaultMachineType,
			zone:                          "different-zone",
			localSSDs:                     map[string]int64{defaultLocalSSDType: defaultLocalSSDSize},
			accelerators:                  map[string]machinetypes.PhysicalGpuCount{defaultAcceleratorType: defaultAcceleratorCount},
			minCpuPlatform:                defaultMinCPUPlatform,
			wantReservationMatch:          false,
			wantReservationNoMatchReasons: []string{"zones mismatch, requested different-zone, reservation at zone-1"},
		},
		{
			name: "DifferentMachineType_NotMatching",
			reservation: BuildReservation("READY",
				false,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				BuildReservationAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				BuildReservationLocalSSDs(defaultLocalSSDType, int(defaultLocalSSDSize))),
			machineType:                   "n1-standard-1",
			zone:                          defaultZone,
			localSSDs:                     map[string]int64{defaultLocalSSDType: defaultLocalSSDSize},
			accelerators:                  map[string]machinetypes.PhysicalGpuCount{defaultAcceleratorType: defaultAcceleratorCount},
			minCpuPlatform:                defaultMinCPUPlatform,
			wantReservationMatch:          false,
			wantReservationNoMatchReasons: []string{"machine type mismatch, requested n1-standard-1, reservation has n2-standard-4"},
		},
		{
			name: "DifferentMinCpuPlatform_NotMatching",
			reservation: BuildReservation("READY",
				false,
				defaultZone,
				defaultMachineType,
				/*minCpuPlatform=*/ "intel-broadwell",
				BuildReservationAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				BuildReservationLocalSSDs(defaultLocalSSDType, int(defaultLocalSSDSize))),
			machineType:                   defaultMachineType,
			zone:                          defaultZone,
			localSSDs:                     map[string]int64{defaultLocalSSDType: defaultLocalSSDSize},
			accelerators:                  map[string]machinetypes.PhysicalGpuCount{defaultAcceleratorType: defaultAcceleratorCount},
			minCpuPlatform:                "intel-skylake",
			wantReservationMatch:          false,
			wantReservationNoMatchReasons: []string{"min cpu platform mismatch, requested Intel Skylake, reservation has Intel Broadwell"},
		},
		{
			name: "DifferentAcceleratorType_NotMatching",
			reservation: BuildReservation("READY",
				false,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				BuildReservationAccelerators("different-acc-type", defaultAcceleratorCount),
				BuildReservationLocalSSDs(defaultLocalSSDType, int(defaultLocalSSDSize))),
			machineType:                   defaultMachineType,
			zone:                          defaultZone,
			localSSDs:                     map[string]int64{defaultLocalSSDType: defaultLocalSSDSize},
			accelerators:                  map[string]machinetypes.PhysicalGpuCount{defaultAcceleratorType: defaultAcceleratorCount},
			minCpuPlatform:                defaultMinCPUPlatform,
			wantReservationMatch:          false,
			wantReservationNoMatchReasons: []string{"accelerator mismatch, requested 2 chips of nvidia-tesla-t4, but it's missing from reservation"},
		},
		{
			name: "DifferentAcceleratorCount_NotMatching",
			reservation: BuildReservation("READY",
				false,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				BuildReservationAccelerators(defaultAcceleratorType, defaultAcceleratorCount+1),
				BuildReservationLocalSSDs(defaultLocalSSDType, int(defaultLocalSSDSize))),
			machineType:                   defaultMachineType,
			zone:                          defaultZone,
			localSSDs:                     map[string]int64{defaultLocalSSDType: defaultLocalSSDSize},
			accelerators:                  map[string]machinetypes.PhysicalGpuCount{defaultAcceleratorType: defaultAcceleratorCount},
			minCpuPlatform:                defaultMinCPUPlatform,
			wantReservationMatch:          false,
			wantReservationNoMatchReasons: []string{"accelerator mismatch, requested 2 chips of nvidia-tesla-t4, reservation has 3 chips of nvidia-tesla-t4"},
		},
		{
			name: "DifferentLocalSsdSize_NotMatching",
			reservation: BuildReservation("READY",
				false,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				BuildReservationAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				BuildReservationLocalSSDs(defaultLocalSSDType, int(defaultLocalSSDSize*2))),
			machineType:                   defaultMachineType,
			zone:                          defaultZone,
			localSSDs:                     map[string]int64{defaultLocalSSDType: defaultLocalSSDSize},
			accelerators:                  map[string]machinetypes.PhysicalGpuCount{defaultAcceleratorType: defaultAcceleratorCount},
			minCpuPlatform:                defaultMinCPUPlatform,
			wantReservationMatch:          false,
			wantReservationNoMatchReasons: []string{"local SSD mismatch, requested 375 GB NVME SSD, reservation has 750 GB NVME SSD"},
		},
		{
			name: "DifferentLocalSsdType_NotMatching",
			reservation: BuildReservation("READY",
				false,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				BuildReservationAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				BuildReservationLocalSSDs(ssdSCSI, int(defaultLocalSSDSize))),
			machineType:                   defaultMachineType,
			zone:                          defaultZone,
			localSSDs:                     map[string]int64{defaultLocalSSDType: defaultLocalSSDSize},
			accelerators:                  map[string]machinetypes.PhysicalGpuCount{defaultAcceleratorType: defaultAcceleratorCount},
			minCpuPlatform:                defaultMinCPUPlatform,
			wantReservationMatch:          false,
			wantReservationNoMatchReasons: []string{"local SSD mismatch, requested 375 GB NVME SSD, but it's missing from reservation"},
		},
		{
			name: "WrongReservationLocalSsdType_NotMatching",
			reservation: BuildReservation("READY",
				false,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				BuildReservationAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				BuildReservationLocalSSDs("Test", int(defaultLocalSSDSize))),
			machineType:                   defaultMachineType,
			zone:                          defaultZone,
			localSSDs:                     map[string]int64{defaultLocalSSDType: defaultLocalSSDSize},
			accelerators:                  map[string]machinetypes.PhysicalGpuCount{defaultAcceleratorType: defaultAcceleratorCount},
			minCpuPlatform:                defaultMinCPUPlatform,
			wantReservationMatch:          false,
			wantReservationNoMatchReasons: []string{"local SSD mismatch, requested 375 GB NVME SSD, but it's missing from reservation", "reservation local SSD interface is not supported: Test"},
		},
		{
			name: "ExtraLocalSsdType_NotMatching",
			reservation: BuildReservation("READY",
				false,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				BuildReservationAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				BuildReservationLocalSSDs(defaultLocalSSDType, int(defaultLocalSSDSize))),
			machineType:                   defaultMachineType,
			zone:                          defaultZone,
			localSSDs:                     map[string]int64{defaultLocalSSDType: defaultLocalSSDSize, "test": int64(1)},
			accelerators:                  map[string]machinetypes.PhysicalGpuCount{defaultAcceleratorType: defaultAcceleratorCount},
			minCpuPlatform:                defaultMinCPUPlatform,
			wantReservationMatch:          false,
			wantReservationNoMatchReasons: []string{"local SSD mismatch, requested 1 GB test SSD, but it's missing from reservation"},
		},
		{
			name: "MultipleMismatches_NotMatching",
			reservation: BuildReservation("READY",
				false,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				BuildReservationAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				BuildReservationLocalSSDs(defaultLocalSSDType, int(defaultLocalSSDSize))),
			machineType:          "n1-standard-1",
			zone:                 "different-zone",
			minCpuPlatform:       "intel-skylake",
			localSSDs:            map[string]int64{defaultLocalSSDType: defaultLocalSSDSize, "test": int64(1)},
			accelerators:         map[string]machinetypes.PhysicalGpuCount{defaultAcceleratorType: defaultAcceleratorCount, ssdSCSI: machinetypes.PhysicalGpuCount(1)},
			wantReservationMatch: false,
			wantReservationNoMatchReasons: []string{
				"zones mismatch, requested different-zone, reservation at zone-1",
				"machine type mismatch, requested n1-standard-1, reservation has n2-standard-4",
				"min cpu platform mismatch, requested Intel Skylake, reservation has Intel Sandy Bridge",
				"accelerator mismatch, requested 1 chips of SCSI, but it's missing from reservation",
				"local SSD mismatch, requested 1 GB test SSD, but it's missing from reservation",
			},
		},
		{
			name: "AcceleratorNotRequested_Strict_NotMatching",
			reservation: BuildReservation("READY",
				false,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				BuildReservationAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				nil,
			),
			machineType:                   defaultMachineType,
			zone:                          defaultZone,
			localSSDs:                     map[string]int64{},
			accelerators:                  map[string]machinetypes.PhysicalGpuCount{},
			minCpuPlatform:                defaultMinCPUPlatform,
			acceleratorStrictRequests:     true,
			wantReservationMatch:          false,
			wantReservationNoMatchReasons: []string{"accelerator mismatch, reservation has 2 chips of nvidia-tesla-t4, but it's not requested"},
		},
		{
			name: "AcceleratorNotRequested_NotStrict_NotMatching",
			reservation: BuildReservation("READY",
				false,
				defaultZone,
				defaultMachineType,
				defaultMinCPUPlatform,
				BuildReservationAccelerators(defaultAcceleratorType, defaultAcceleratorCount),
				nil,
			),
			machineType:                   defaultMachineType,
			zone:                          defaultZone,
			localSSDs:                     map[string]int64{},
			accelerators:                  map[string]machinetypes.PhysicalGpuCount{},
			minCpuPlatform:                defaultMinCPUPlatform,
			acceleratorStrictRequests:     false,
			wantReservationMatch:          true,
			wantReservationNoMatchReasons: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			nodeShape := NodeShape{MachineType: tc.machineType, Zone: tc.zone, MinCpuPlatform: tc.minCpuPlatform, LocalSSDSizes: tc.localSSDs, Accelerators: tc.accelerators}
			gotReservationMatch := MatchSpecificReservationShape(provider, tc.reservation, nodeShape, tc.acceleratorStrictRequests)
			gotReservationNoMatchReasons := MatchSpecificReservationShapeWithReasons(provider, tc.reservation, nodeShape, tc.acceleratorStrictRequests)
			assert.Equal(t, tc.wantReservationMatch, gotReservationMatch)
			assert.Equal(t, tc.wantReservationNoMatchReasons, gotReservationNoMatchReasons)
		})
	}
}

func TestMatchAggregateReservation(t *testing.T) {
	tests := map[string]struct {
		tpuType     string
		tpuTopology string
		tpuCount    int64
		reservation *gce_api.Reservation

		wantMatch          bool
		wantNoMatchReasons []string
	}{
		"FullMatchServing": {
			tpuType:     labels.TpuV4PodsliceValue,
			tpuTopology: "2x2x2",
			tpuCount:    8,
			reservation: &gce_api.Reservation{
				Name:                        "aggregate",
				Status:                      "READY",
				SpecificReservationRequired: true,
				AggregateReservation: &gce_api.AllocationAggregateReservation{
					ReservedResources: []*gce_api.AllocationAggregateReservationReservedResourceInfo{
						{
							Accelerator: &gce_api.AllocationAggregateReservationReservedResourceInfoAccelerator{
								AcceleratorType:  "projects/whatever/zones/us-central2-b/acceleratorTypes/ct4p",
								AcceleratorCount: 8,
							},
						},
					},
					WorkloadType: tpu.WorkloadTypeServing,
				},
			},

			wantMatch: true,
		},
		"FullMatchBatch": {
			tpuType:     labels.TpuV4PodsliceValue,
			tpuTopology: "2x2x2",
			tpuCount:    4,
			reservation: &gce_api.Reservation{
				Name:                        "aggregate",
				Status:                      "READY",
				SpecificReservationRequired: true,
				AggregateReservation: &gce_api.AllocationAggregateReservation{
					ReservedResources: []*gce_api.AllocationAggregateReservationReservedResourceInfo{
						{
							Accelerator: &gce_api.AllocationAggregateReservationReservedResourceInfoAccelerator{
								AcceleratorType:  "projects/whatever/zones/us-central2-b/acceleratorTypes/ct4p",
								AcceleratorCount: 8,
							},
						},
					},
					WorkloadType: tpu.WorkloadTypeBatch,
				},
			},

			wantMatch:          true,
			wantNoMatchReasons: nil,
		},
		"DifferentWorkloadType_Batch_NoMatch": {
			tpuType:     labels.TpuV4PodsliceValue,
			tpuTopology: "2x2x2",
			tpuCount:    4,
			reservation: &gce_api.Reservation{
				Name:                        "aggregate",
				Status:                      "READY",
				SpecificReservationRequired: true,
				AggregateReservation: &gce_api.AllocationAggregateReservation{
					ReservedResources: []*gce_api.AllocationAggregateReservationReservedResourceInfo{
						{
							Accelerator: &gce_api.AllocationAggregateReservationReservedResourceInfoAccelerator{
								AcceleratorType:  "projects/whatever/zones/us-central2-b/acceleratorTypes/ct4p",
								AcceleratorCount: 8,
							},
						},
					},
					WorkloadType: tpu.WorkloadTypeServing,
				},
			},

			wantMatch:          false,
			wantNoMatchReasons: []string{"requested multihost TPU, reservation has singlehost"},
		},
		"DifferentWorkloadType_Serving_NoMatch": {
			tpuType:     labels.TpuV4PodsliceValue,
			tpuTopology: "2x2x2",
			tpuCount:    8,
			reservation: &gce_api.Reservation{
				Name:                        "aggregate",
				Status:                      "READY",
				SpecificReservationRequired: true,
				AggregateReservation: &gce_api.AllocationAggregateReservation{
					ReservedResources: []*gce_api.AllocationAggregateReservationReservedResourceInfo{
						{
							Accelerator: &gce_api.AllocationAggregateReservationReservedResourceInfoAccelerator{
								AcceleratorType:  "projects/whatever/zones/us-central2-b/acceleratorTypes/ct4p",
								AcceleratorCount: 8,
							},
						},
					},
					WorkloadType: tpu.WorkloadTypeBatch,
				},
			},

			wantMatch:          false,
			wantNoMatchReasons: []string{"requested singlehost TPU, reservation has multihost"},
		},
		"DifferentType_NoMatch": {
			tpuType:     labels.TpuV5LitePodsliceValue,
			tpuTopology: "2x2x2",
			tpuCount:    8,
			reservation: &gce_api.Reservation{
				Name:                        "aggregate",
				SpecificReservationRequired: true,
				AggregateReservation: &gce_api.AllocationAggregateReservation{
					ReservedResources: []*gce_api.AllocationAggregateReservationReservedResourceInfo{
						{
							Accelerator: &gce_api.AllocationAggregateReservationReservedResourceInfoAccelerator{
								AcceleratorType:  "projects/whatever/zones/us-central2-b/acceleratorTypes/ct5p",
								AcceleratorCount: 8,
							},
						},
					},
					WorkloadType: tpu.WorkloadTypeUnspecified,
				},
			},

			wantMatch:          false,
			wantNoMatchReasons: []string{"requested TPU accelerator {tpu-v5-lite-podslice, 2x2x2, 8}, but it's missing from reservation"},
		},
		"DifferentChipsCount_NoMatch": {
			tpuType:     labels.TpuV4PodsliceValue,
			tpuTopology: "2x2x4",
			tpuCount:    8,
			reservation: &gce_api.Reservation{
				Name:                        "aggregate",
				Status:                      "READY",
				SpecificReservationRequired: true,
				AggregateReservation: &gce_api.AllocationAggregateReservation{
					ReservedResources: []*gce_api.AllocationAggregateReservationReservedResourceInfo{
						{
							Accelerator: &gce_api.AllocationAggregateReservationReservedResourceInfoAccelerator{
								AcceleratorType:  "projects/whatever/zones/us-central2-b/acceleratorTypes/ct4p",
								AcceleratorCount: 8,
							},
						},
					},
					WorkloadType: tpu.WorkloadTypeUnspecified,
				},
			},

			wantMatch:          false,
			wantNoMatchReasons: []string{"requested TPU accelerator {tpu-v4-podslice, 2x2x4, 8}, but it's missing from reservation"},
		},
		"SpecificReservation_NoMatch": {
			tpuType:     labels.TpuV4PodsliceValue,
			tpuTopology: "2x2x2",
			tpuCount:    8,
			reservation: BuildReservation(
				"READY",
				/*specificReservationRequired=*/ true,
				"zone",
				"e2-standard-2",
				"minCpuPlatform",
				nil,
				nil,
			),

			wantMatch:          false,
			wantNoMatchReasons: []string{"requested TPU accelerator {tpu-v4-podslice, 2x2x2, 8}, but it's missing from reservation"},
		},
		"InvalidReservation_NoMatch": {
			tpuType:     labels.TpuV4PodsliceValue,
			tpuTopology: "2x2x2",
			tpuCount:    8,
			reservation: &gce_api.Reservation{},

			wantMatch:          false,
			wantNoMatchReasons: []string{"requested TPU accelerator {tpu-v4-podslice, 2x2x2, 8}, but it's missing from reservation"},
		},
	}

	for testName, test := range tests {
		test := test

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			gotMatchReasons := MatchAggregateReservationShapeWithReasons(provider, test.reservation, test.tpuType, test.tpuTopology, test.tpuCount)
			assert.Equal(t, test.wantMatch, len(gotMatchReasons) == 0)
			assert.Equal(t, test.wantNoMatchReasons, gotMatchReasons)
		})
	}
}
