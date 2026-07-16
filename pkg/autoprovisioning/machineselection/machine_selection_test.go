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

package machineselection

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"

	"k8s.io/utils/ptr"
)

func TestSelectMachineSpec(t *testing.T) {
	machinetypes.RegisterComputeClass(machinetypes.NewTestPredefinedComputeClass(
		"autopilot",
		[]machinetypes.MachineFamily{machinetypes.E2, machinetypes.EK, machinetypes.E4},
		true,  // balancingEnabled
		false, // sliceOfHardware
	))
	defaultCloudProviderFamily := gke.NewTestAutoprovisioningCloudProviderBuilder().Build().GetAutoprovisioningDefaultFamily()
	generalPurposePodFamily := "general-purpose"
	generalPurposeArmPodFamily := "general-purpose-arm"
	unknownPodFamily := "unknown"
	knownMachineType := "n2d-standard-4"
	unknownMachineType := "unknown"
	knownMachineFamily := "n2"
	unknownMachineFamily := "unknown"
	customMachineTypeN1 := "custom-4-32768"
	customMachineTypeN2 := "n2-custom-4-32768"
	customMachineTypeN4 := "n4-custom-4-16384"
	customMachineTypeN2D := "n2d-custom-8-65536"
	customMachineTypeE2 := "e2-custom-16-131072"
	customMachineTypeE4 := "e4-custom-16-131072"
	customMachineTypeG2 := "g2-custom-4-32768"

	for tn, tc := range map[string]struct {
		machineFamily                     string
		podClass                          string
		specifiedGpu                      string
		specifiedTpu                      string
		specifiedBootDiskType             string
		specifiedReservationMachineType   string
		resizableVmInAutopilotEnabled     map[string]bool
		resizableVmWithinPodFamilyEnabled map[string]bool
		isEkSpotEnabled                   bool
		isArmMachineFallbacksEnabled      bool
		confidentialNodes                 bool
		confidentialInstanceType          string
		autopilotEnabled                  bool
		autopilotManaged                  bool
		wantsSpot                         bool
		isE2lessRegion                    bool
		isE4Enabled                       bool
		boolFlags                         map[string]bool
		stringFlags                       map[string]string
		componentVersion                  string
		architectures                     map[gce.SystemArchitecture]bool
		rule                              rules.Rule
		isStateless                       bool
		isExtendedFallbacksEnabled        bool
		expectedMinCpuPlatform            *machinetypes.CpuPlatform
		expectedFamilies                  []machinetypes.MachineFamily
		expectedComputeClassName          string
		expectedMachineType               []string
		expectedErr                       error
	}{
		"default CloudProvider family used by default": {
			expectedFamilies: []machinetypes.MachineFamily{defaultCloudProviderFamily},
		},
		"N2D used if confidential nodes enabled and instance type is empty": {
			confidentialNodes:        true,
			confidentialInstanceType: "",
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.N2D},
		},
		"N2D used if confidential nodes enabled and instance type is SEV": {
			confidentialNodes:        true,
			confidentialInstanceType: gkelabels.SEVConfidentialNodeTypeValue,
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.N2D},
		},
		"G4 used if confidential nodes enabled, instance type is SEV, and RTX PRO 6000 specified": {
			confidentialNodes:        true,
			confidentialInstanceType: gkelabels.SEVConfidentialNodeTypeValue,
			specifiedGpu:             machinetypes.NvidiaRTXPro6000.Name(),
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.G4},
		},
		"N2D used if confidential nodes enabled and instance type is SEV_SNP": {
			confidentialNodes:        true,
			confidentialInstanceType: gkelabels.SEVSNPConfidentialNodeTypeValue,
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.N2D},
		},
		"C3 used if confidential nodes enabled and instance type is TDX": {
			confidentialNodes:        true,
			confidentialInstanceType: gkelabels.TDXConfidentialNodeTypeValue,
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.C3},
		},
		"C3 used if confidential nodes instance type is specified as TDX": {
			confidentialInstanceType: gkelabels.TDXConfidentialNodeTypeValue,
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.C3},
		},
		"A2 used if A100 specified": {
			specifiedGpu:     machinetypes.NvidiaTeslaA100.Name(),
			expectedFamilies: []machinetypes.MachineFamily{machinetypes.A2},
		},
		"A2 used if A100-80gb specified": {
			specifiedGpu:     machinetypes.NvidiaA100_80gb.Name(),
			expectedFamilies: []machinetypes.MachineFamily{machinetypes.A2},
		},
		"N1 used if GPU other than A100/A100-80gb specified": {
			specifiedGpu:     machinetypes.NvidiaTeslaK80.Name(),
			expectedFamilies: []machinetypes.MachineFamily{machinetypes.N1},
		},
		"C4A used if only ARM specified and E4A experiment disabled": {
			architectures:                 map[gce.SystemArchitecture]bool{gce.Arm64: true},
			expectedFamilies:              []machinetypes.MachineFamily{machinetypes.C4A},
			resizableVmInAutopilotEnabled: map[string]bool{machinetypes.E4A.Name(): false},
		},
		"E4A used if only ARM specified, is Autopilot and E4A experiment enabled": {
			architectures:                 map[gce.SystemArchitecture]bool{gce.Arm64: true},
			expectedFamilies:              []machinetypes.MachineFamily{machinetypes.E4A},
			autopilotEnabled:              true,
			resizableVmInAutopilotEnabled: map[string]bool{machinetypes.E4A.Name(): true},
		},
		"C4A used if only ARM specified and IsResizableVmWithinPodFamilyEnabled is true (Standard)": {
			architectures:                     map[gce.SystemArchitecture]bool{gce.Arm64: true},
			expectedFamilies:                  []machinetypes.MachineFamily{machinetypes.C4A},
			resizableVmWithinPodFamilyEnabled: map[string]bool{machinetypes.E4A.Name(): true},
		},
		"E4A used if only ARM specified and IsResizableVmWithinPodFamilyEnabled is true (Autopilot)": {
			architectures:                     map[gce.SystemArchitecture]bool{gce.Arm64: true},
			expectedFamilies:                  []machinetypes.MachineFamily{machinetypes.E4A},
			resizableVmWithinPodFamilyEnabled: map[string]bool{machinetypes.E4A.Name(): true},
			autopilotEnabled:                  true,
			autopilotManaged:                  true,
		},
		"C4A used if only ARM specified, IsResizableVmWithinPodFamilyEnabled is true, but not Autopilot managed": {
			architectures:                     map[gce.SystemArchitecture]bool{gce.Arm64: true},
			expectedFamilies:                  []machinetypes.MachineFamily{machinetypes.C4A},
			resizableVmWithinPodFamilyEnabled: map[string]bool{machinetypes.E4A.Name(): true},
			autopilotEnabled:                  true,
			autopilotManaged:                  false,
		},
		"E4A used if only ARM specified, IsResizableVmWithinPodFamilyEnabled is true, and is Autopilot managed (non-Autopilot cluster)": {
			architectures:                     map[gce.SystemArchitecture]bool{gce.Arm64: true},
			expectedFamilies:                  []machinetypes.MachineFamily{machinetypes.E4A},
			resizableVmWithinPodFamilyEnabled: map[string]bool{machinetypes.E4A.Name(): true},
			autopilotEnabled:                  false,
			autopilotManaged:                  true,
		},
		"E4A used if only ARM specified, IsResizableVmEnabledInAutopilot is true, and is Autopilot cluster": {
			architectures:                 map[gce.SystemArchitecture]bool{gce.Arm64: true},
			expectedFamilies:              []machinetypes.MachineFamily{machinetypes.E4A},
			resizableVmInAutopilotEnabled: map[string]bool{machinetypes.E4A.Name(): true},
			autopilotEnabled:              true,
			autopilotManaged:              false,
		},
		"C4A used if only ARM specified, IsResizableVmEnabledInAutopilot is true, but not Autopilot cluster": {
			architectures:                 map[gce.SystemArchitecture]bool{gce.Arm64: true},
			expectedFamilies:              []machinetypes.MachineFamily{machinetypes.C4A},
			resizableVmInAutopilotEnabled: map[string]bool{machinetypes.E4A.Name(): true},
			autopilotEnabled:              false,
			autopilotManaged:              true,
		},
		"CT4L used if tpu-v4-lite-device specified": {
			specifiedTpu:     gkelabels.TpuV4LiteDeviceValue,
			expectedFamilies: []machinetypes.MachineFamily{machinetypes.CT4L},
		}, "CT4P used if tpu-v4-podslice specified": {
			specifiedTpu:     gkelabels.TpuV4PodsliceValue,
			expectedFamilies: []machinetypes.MachineFamily{machinetypes.CT4P},
		},
		"existing boot disk type specified, machine family is default": {
			specifiedBootDiskType: machinetypes.DiskTypeSSD,
			expectedFamilies:      []machinetypes.MachineFamily{defaultCloudProviderFamily},
		},
		"generic boot disk type specified, machine family is default": {
			specifiedBootDiskType: "generic-boot-disk-type",
			expectedFamilies:      []machinetypes.MachineFamily{defaultCloudProviderFamily},
		},
		"boot disk type specified both in labels and CCC, CCC rule does not effect boot disk type in the spec": {
			rule:                  rules.NewRule(rules.WithStorageRule(ptr.To(machinetypes.DiskTypeBalanced), ptr.To(5), nil, ptr.To(1))),
			specifiedBootDiskType: machinetypes.DiskTypeSSD,
			expectedFamilies:      []machinetypes.MachineFamily{defaultCloudProviderFamily},
		},
		"default family used if only AMD specified": {
			architectures:    map[gce.SystemArchitecture]bool{gce.Amd64: true},
			expectedFamilies: []machinetypes.MachineFamily{defaultCloudProviderFamily},
		},
		"default family used if both AMD and ARM specified": {
			architectures:    map[gce.SystemArchitecture]bool{gce.Arm64: true, gce.Amd64: true},
			expectedFamilies: []machinetypes.MachineFamily{defaultCloudProviderFamily},
		},
		"machine family overrides default family": {
			machineFamily:    "t2d",
			expectedFamilies: []machinetypes.MachineFamily{machinetypes.T2D},
		},
		"machine family overrides default family even if incompatible with cluster settings": {
			machineFamily:            "t2d",
			confidentialNodes:        true,
			confidentialInstanceType: gkelabels.SEVConfidentialNodeTypeValue,
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.T2D},
		},
		"machine family overrides default family for c4n": {
			machineFamily:    "c4n",
			expectedFamilies: []machinetypes.MachineFamily{machinetypes.C4N},
		},
		"machine family overrides default family for z4d": {
			machineFamily:    "z4d",
			expectedFamilies: []machinetypes.MachineFamily{machinetypes.Z4D},
		},
		"unknown machine family results in an explicit error": {
			machineFamily: "not-supported",
			expectedErr:   NewMachineFamilyUnknownError("not-supported"),
		},
		"compute class overrides default family": {
			podClass:                 machinetypes.ScaleOutClass.Name(),
			autopilotEnabled:         true,
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.T2A, machinetypes.T2D},
			expectedComputeClassName: machinetypes.ScaleOutClass.Name(),
		},
		"compute class overrides default family even if it is incompatible with cluster settings": {
			podClass:                 machinetypes.ScaleOutClass.Name(),
			specifiedGpu:             machinetypes.NvidiaTeslaA100.Name(),
			autopilotEnabled:         true,
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.T2A, machinetypes.T2D},
			expectedComputeClassName: machinetypes.ScaleOutClass.Name(),
		},
		"compute class cannot be specified with machine family": {
			podClass:         machinetypes.ScaleOutClass.Name(),
			machineFamily:    "n2",
			autopilotEnabled: true,
			expectedErr:      NewComputeClassWithMachineFamilyError(machinetypes.ScaleOutClass.Name()),
		},
		"unknown compute class results in a default behavior": {
			podClass:         "not-supported",
			expectedFamilies: []machinetypes.MachineFamily{defaultCloudProviderFamily},
			autopilotEnabled: true,
		},
		"unknown compute class results results in a default behavior, non-autopilot": {
			podClass:         "not-supported",
			expectedFamilies: []machinetypes.MachineFamily{defaultCloudProviderFamily},
			autopilotEnabled: false,
		},
		"compute class in non-Autopilot cluster results in an explicit error": {
			podClass:         machinetypes.ScaleOutClass.Name(),
			autopilotEnabled: false,
			expectedErr:      NewComputeClassNonAutopilotError(machinetypes.ScaleOutClass.Name()),
		},
		"slice of hardware compute class unspecified machine family": {
			podClass:         machinetypes.PerformanceClass.Name(),
			autopilotEnabled: true,
			expectedErr:      NewComputeClassWithoutMachineFamilyError(machinetypes.PerformanceClass.Name()),
		},
		"slice of hardware compute class specifies invalid machine family": {
			podClass:         machinetypes.PerformanceClass.Name(),
			machineFamily:    "m2",
			autopilotEnabled: true,
			expectedErr:      NewComputeClassWithInvalidMachineFamilyError(machinetypes.PerformanceClass.Name(), "m2"),
		},
		"slice of hardware compute class specifies valid c3 machine family": {
			podClass:                 machinetypes.PerformanceClass.Name(),
			machineFamily:            "c3",
			autopilotEnabled:         true,
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.C3},
			expectedComputeClassName: machinetypes.PerformanceClass.Name(),
		},
		"slice of hardware compute class specifies valid h3 machine family": {
			podClass:                 machinetypes.PerformanceClass.Name(),
			machineFamily:            "h3",
			autopilotEnabled:         true,
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.H3},
			expectedComputeClassName: machinetypes.PerformanceClass.Name(),
		},
		"slice of hardware compute class specifies valid z4d machine family": {
			podClass:                 machinetypes.PerformanceClass.Name(),
			machineFamily:            "z4d",
			autopilotEnabled:         true,
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.Z4D},
			expectedComputeClassName: machinetypes.PerformanceClass.Name(),
		},
		"slice of hardware compute class with Arm specified": {
			podClass:                 machinetypes.PerformanceClass.Name(),
			architectures:            map[gce.SystemArchitecture]bool{gce.Arm64: true},
			autopilotEnabled:         true,
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.C4A},
			expectedComputeClassName: machinetypes.PerformanceClass.Name(),
		},
		"slice of hardware compute class with Arm specified, is Autopilot and E4A experiment enabled": {
			podClass:                      machinetypes.PerformanceClass.Name(),
			architectures:                 map[gce.SystemArchitecture]bool{gce.Arm64: true},
			autopilotEnabled:              true,
			expectedFamilies:              []machinetypes.MachineFamily{machinetypes.C4A},
			expectedComputeClassName:      machinetypes.PerformanceClass.Name(),
			resizableVmInAutopilotEnabled: map[string]bool{machinetypes.E4A.Name(): true},
		},
		"Accelerator compute class in non-Autopilot cluster results in an explicit error": {
			podClass:         machinetypes.AcceleratorClass.Name(),
			autopilotEnabled: false,
			expectedErr:      NewComputeClassNonAutopilotError(machinetypes.AcceleratorClass.Name()),
		},
		"Accelerator compute class unspecified gpu and tpu results in an explicit error": {
			podClass:         machinetypes.AcceleratorClass.Name(),
			autopilotEnabled: true,
			expectedErr:      NewComputeClassWithoutAcceleratorError(machinetypes.AcceleratorClass.Name()),
		},
		"Accelerator compute class cannot be specified with machine family": {
			podClass:         machinetypes.AcceleratorClass.Name(),
			machineFamily:    "n2",
			autopilotEnabled: true,
			expectedErr:      NewComputeClassWithMachineFamilyError(machinetypes.AcceleratorClass.Name()),
		},
		"Accelerator compute class A2 used if A100 specified": {
			podClass:                 machinetypes.AcceleratorClass.Name(),
			autopilotEnabled:         true,
			specifiedGpu:             machinetypes.NvidiaTeslaA100.Name(),
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.A2},
			expectedComputeClassName: machinetypes.AcceleratorClass.Name(),
		},
		"Accelerator compute class A2 used if A100-80gb specified": {
			podClass:                 machinetypes.AcceleratorClass.Name(),
			autopilotEnabled:         true,
			specifiedGpu:             machinetypes.NvidiaA100_80gb.Name(),
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.A2},
			expectedComputeClassName: machinetypes.AcceleratorClass.Name(),
		},
		"Accelerator compute class A3 used if H100-80gb specified": {
			podClass:                 machinetypes.AcceleratorClass.Name(),
			autopilotEnabled:         true,
			specifiedGpu:             machinetypes.NvidiaH100_80gb.Name(),
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.A3},
			expectedComputeClassName: machinetypes.AcceleratorClass.Name(),
		},
		"Accelerator compute class A4 used if B200 specified": {
			podClass:                 machinetypes.AcceleratorClass.Name(),
			autopilotEnabled:         true,
			specifiedGpu:             machinetypes.NvidiaB200.Name(),
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.A4},
			expectedComputeClassName: machinetypes.AcceleratorClass.Name(),
		},
		"Accelerator compute class A4X used if GB200 specified": {
			podClass:                 machinetypes.AcceleratorClass.Name(),
			autopilotEnabled:         true,
			specifiedGpu:             machinetypes.NvidiaGB200.Name(),
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.A4X},
			expectedComputeClassName: machinetypes.AcceleratorClass.Name(),
		},
		"Accelerator_compute_class_A4X_used_if_GB300_specified": {
			podClass:                 machinetypes.AcceleratorClass.Name(),
			autopilotEnabled:         true,
			specifiedGpu:             machinetypes.NvidiaGB300.Name(),
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.A4X},
			expectedComputeClassName: machinetypes.AcceleratorClass.Name(),
		},
		"Accelerator compute class g2 used if L4 specified": {
			podClass:                 machinetypes.AcceleratorClass.Name(),
			autopilotEnabled:         true,
			specifiedGpu:             machinetypes.NvidiaL4.Name(),
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.G2},
			expectedComputeClassName: machinetypes.AcceleratorClass.Name(),
		},
		"Accelerator compute class G4 used if RTX PRO 6000 specified": {
			podClass:                 machinetypes.AcceleratorClass.Name(),
			autopilotEnabled:         true,
			specifiedGpu:             machinetypes.NvidiaRTXPro6000.Name(),
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.G4},
			expectedComputeClassName: machinetypes.AcceleratorClass.Name(),
		},
		"Accelerator compute class N1 used if GPU other than A100/A100-80gb/H100/L4 specified": {
			podClass:                 machinetypes.AcceleratorClass.Name(),
			autopilotEnabled:         true,
			specifiedGpu:             machinetypes.NvidiaTeslaK80.Name(),
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.N1},
			expectedComputeClassName: machinetypes.AcceleratorClass.Name(),
		},
		"Accelerator compute class CT4L used if tpu-v4-lite-device specified": {
			podClass:                 machinetypes.AcceleratorClass.Name(),
			autopilotEnabled:         true,
			specifiedTpu:             gkelabels.TpuV4LiteDeviceValue,
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.CT4L},
			expectedComputeClassName: machinetypes.AcceleratorClass.Name(),
		},
		"Accelerator compute class CT4P used if tpu-v4-podslice specified": {
			podClass:                 machinetypes.AcceleratorClass.Name(),
			autopilotEnabled:         true,
			specifiedTpu:             gkelabels.TpuV4PodsliceValue,
			expectedFamilies:         []machinetypes.MachineFamily{machinetypes.CT4P},
			expectedComputeClassName: machinetypes.AcceleratorClass.Name(),
		},
		"EK: EK and E2 machine types are used for non-Spot pod non-compute class pods": {
			resizableVmInAutopilotEnabled: map[string]bool{machinetypes.EK.Name(): true},
			autopilotEnabled:              true,
			wantsSpot:                     false,
			expectedFamilies:              []machinetypes.MachineFamily{machinetypes.EK, defaultCloudProviderFamily},
		},
		"EK: EKs require per-pod billing and CCC requires per-node billing, fallback to E2": {
			resizableVmInAutopilotEnabled: map[string]bool{machinetypes.EK.Name(): true},
			autopilotEnabled:              true,
			podClass:                      "custom-compute-class",
			expectedFamilies:              []machinetypes.MachineFamily{defaultCloudProviderFamily},
		},
		"EK: EK spots disabled, fallback to E2": {
			resizableVmInAutopilotEnabled: map[string]bool{machinetypes.EK.Name(): true},
			autopilotEnabled:              true,
			wantsSpot:                     true,
			expectedFamilies:              []machinetypes.MachineFamily{defaultCloudProviderFamily},
		},
		"EK: EK spots enabled": {
			resizableVmInAutopilotEnabled: map[string]bool{machinetypes.EK.Name(): true},
			autopilotEnabled:              true,
			wantsSpot:                     true,
			isEkSpotEnabled:               true,
			expectedFamilies:              []machinetypes.MachineFamily{machinetypes.EK, defaultCloudProviderFamily},
		},
		"MN: pod family specified in CCC rule": {
			rule:                              rules.NewRule(rules.WithPodFamilyRule(&generalPurposePodFamily)),
			resizableVmWithinPodFamilyEnabled: map[string]bool{machinetypes.EK.Name(): true},
			expectedFamilies:                  []machinetypes.MachineFamily{machinetypes.E2, machinetypes.EK},
		},
		"MN: EK spots disabled, fallback to E2": {
			rule:             rules.NewRule(rules.WithPodFamilyRule(&generalPurposePodFamily)),
			wantsSpot:        true,
			expectedFamilies: []machinetypes.MachineFamily{machinetypes.E2},
		},
		"MN: EK spots enabled": {
			rule:             rules.NewRule(rules.WithPodFamilyRule(&generalPurposePodFamily)),
			wantsSpot:        true,
			isEkSpotEnabled:  true,
			expectedFamilies: []machinetypes.MachineFamily{machinetypes.E2, machinetypes.EK},
		},
		"MN: EK within pod family disabled, fallback to E2": {
			rule:                              rules.NewRule(rules.WithPodFamilyRule(&generalPurposePodFamily)),
			resizableVmWithinPodFamilyEnabled: map[string]bool{machinetypes.EK.Name(): false},
			expectedFamilies:                  []machinetypes.MachineFamily{machinetypes.E2},
		},
		"general purpose arm pod family in CCC rule, E4A and fallbacks experiment enabled": {
			rule:                          rules.NewRule(rules.WithPodFamilyRule(&generalPurposeArmPodFamily)),
			resizableVmInAutopilotEnabled: map[string]bool{machinetypes.E4A.Name(): true},
			isArmMachineFallbacksEnabled:  true,
			autopilotEnabled:              true,
			expectedFamilies:              []machinetypes.MachineFamily{machinetypes.E4A, machinetypes.N4A, machinetypes.C4A},
		},
		"general purpose arm pod family in CCC rule, fallbacks experiment disabled": {
			rule:                          rules.NewRule(rules.WithPodFamilyRule(&generalPurposeArmPodFamily)),
			resizableVmInAutopilotEnabled: map[string]bool{machinetypes.E4A.Name(): true},
			isArmMachineFallbacksEnabled:  false,
			autopilotEnabled:              true,
			expectedFamilies:              []machinetypes.MachineFamily{machinetypes.E4A},
		},
		"general purpose arm pod family in CCC rule, E4A experiment disabled": {
			rule:                          rules.NewRule(rules.WithPodFamilyRule(&generalPurposeArmPodFamily)),
			resizableVmInAutopilotEnabled: map[string]bool{machinetypes.E4A.Name(): false},
			autopilotEnabled:              true,
			expectedErr:                   NewPodFamilyUnknownError(generalPurposeArmPodFamily),
		},
		"Error if GeneralPurposeArmPodFamily rule specified and IsResizableVmWithinPodFamilyEnabled is true (Standard)": {
			rule:                              rules.NewRule(rules.WithPodFamilyRule(&generalPurposeArmPodFamily)),
			resizableVmWithinPodFamilyEnabled: map[string]bool{machinetypes.E4A.Name(): true},
			expectedErr:                       NewPodFamilyUnknownError(generalPurposeArmPodFamily),
		},
		"E4A, N4A, C4A used if GeneralPurposeArmPodFamily rule specified and IsResizableVmWithinPodFamilyEnabled is true (Autopilot)": {
			rule:                              rules.NewRule(rules.WithPodFamilyRule(&generalPurposeArmPodFamily)),
			resizableVmWithinPodFamilyEnabled: map[string]bool{machinetypes.E4A.Name(): true},
			isArmMachineFallbacksEnabled:      true,
			expectedFamilies:                  []machinetypes.MachineFamily{machinetypes.E4A, machinetypes.N4A, machinetypes.C4A},
			autopilotEnabled:                  true,
			autopilotManaged:                  true,
		},
		"E4A used if GeneralPurposeArmPodFamily rule specified, IsResizableVmWithinPodFamilyEnabled is true, but fallbacks disabled (Autopilot)": {
			rule:                              rules.NewRule(rules.WithPodFamilyRule(&generalPurposeArmPodFamily)),
			resizableVmWithinPodFamilyEnabled: map[string]bool{machinetypes.E4A.Name(): true},
			isArmMachineFallbacksEnabled:      false,
			expectedFamilies:                  []machinetypes.MachineFamily{machinetypes.E4A},
			autopilotEnabled:                  true,
			autopilotManaged:                  true,
		},
		"Error if GeneralPurposeArmPodFamily rule specified and all E4A flags are false": {
			rule:                              rules.NewRule(rules.WithPodFamilyRule(&generalPurposeArmPodFamily)),
			resizableVmWithinPodFamilyEnabled: map[string]bool{machinetypes.E4A.Name(): false},
			resizableVmInAutopilotEnabled:     map[string]bool{machinetypes.E4A.Name(): false},
			expectedErr:                       NewPodFamilyUnknownError(generalPurposeArmPodFamily),
		},
		"unknown pod family specidied in CCC rule": {
			rule:        rules.NewRule(rules.WithPodFamilyRule(&unknownPodFamily)),
			expectedErr: NewPodFamilyUnknownError(unknownPodFamily),
		},
		"known machine type specified in CCC rule": {
			rule:             rules.NewRule(rules.WithMachineTypeRule(&knownMachineType)),
			expectedFamilies: []machinetypes.MachineFamily{machinetypes.N2D},
		},
		"known n1 family custom machine type specified in CCC rule": {
			rule:             rules.NewRule(rules.WithMachineTypeRule(&customMachineTypeN1)),
			expectedFamilies: []machinetypes.MachineFamily{machinetypes.N1},
		},
		"known n2 family custom machine type specified in CCC rule": {
			rule:             rules.NewRule(rules.WithMachineTypeRule(&customMachineTypeN2)),
			expectedFamilies: []machinetypes.MachineFamily{machinetypes.N2},
		},
		"known n4 family custom machine type specified in CCC rule": {
			rule:             rules.NewRule(rules.WithMachineTypeRule(&customMachineTypeN4)),
			expectedFamilies: []machinetypes.MachineFamily{machinetypes.N4},
		},
		"known n2d family custom machine type specified in CCC rule": {
			rule:             rules.NewRule(rules.WithMachineTypeRule(&customMachineTypeN2D)),
			expectedFamilies: []machinetypes.MachineFamily{machinetypes.N2D},
		},
		"known e2 family custom machine type specified in CCC rule": {
			rule:             rules.NewRule(rules.WithMachineTypeRule(&customMachineTypeE2)),
			expectedFamilies: []machinetypes.MachineFamily{machinetypes.E2},
		},
		"known e4 family custom machine type specified in CCC rule": {
			rule:             rules.NewRule(rules.WithMachineTypeRule(&customMachineTypeE4)),
			expectedFamilies: []machinetypes.MachineFamily{machinetypes.E4},
		},
		"known g2 family custom machine type specified in CCC rule": {
			rule:             rules.NewRule(rules.WithMachineTypeRule(&customMachineTypeG2)),
			expectedFamilies: []machinetypes.MachineFamily{machinetypes.G2},
		},
		"unknown machine type specified in CCC rule": {
			rule:        rules.NewRule(rules.WithMachineTypeRule(&unknownMachineType)),
			expectedErr: NewMachineTypeNotSupportedError(unknownMachineType),
		},
		"gpu request specified in CCC rule": {
			rule:             rules.NewRule(rules.WithMachineFamilyRule(&knownMachineFamily), rules.WithGpuRule(&machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: gkelabels.NvidiaTeslaA100}})),
			expectedFamilies: []machinetypes.MachineFamily{defaultCloudProviderFamily},
		},
		"known machine family specified in CCC rule": {
			rule:             rules.NewRule(rules.WithMachineFamilyRule(&knownMachineFamily)),
			expectedFamilies: []machinetypes.MachineFamily{machinetypes.N2},
		},
		"unknown machine family specified in CCC rule": {
			rule:        rules.NewRule(rules.WithMachineFamilyRule(&unknownMachineFamily)),
			expectedErr: NewMachineFamilyUnknownError(unknownMachineFamily),
		},
		"min cpu platform specified in CCC rule": {
			rule:                   rules.NewRule(rules.WithMinCpuPlatformRule(ptr.To("Intel Broadwell"))),
			expectedFamilies:       []machinetypes.MachineFamily{defaultCloudProviderFamily},
			expectedMinCpuPlatform: ptr.To(machinetypes.CpuPlatform(machinetypes.IntelBroadwell)),
		},
		"reservation machine type specified": {
			specifiedReservationMachineType: "n2-standard-2",
			expectedFamilies:                []machinetypes.MachineFamily{machinetypes.N2},
			expectedMachineType:             []string{"n2-standard-2"},
		},
		"invalid min cpu platform specified in CCC rule": {
			rule:        rules.NewRule(rules.WithMinCpuPlatformRule(ptr.To("invalid"))),
			expectedErr: NewMinCpuPlatformUnknownError("invalid"),
		},
		"E4 used if GeneralPurposePodFamily rule specified and is E2-less region": {
			rule:                              rules.NewRule(rules.WithPodFamilyRule(&generalPurposePodFamily)),
			isE2lessRegion:                    true,
			isE4Enabled:                       true,
			autopilotManaged:                  true,
			resizableVmWithinPodFamilyEnabled: map[string]bool{machinetypes.EK.Name(): true},
			expectedFamilies:                  []machinetypes.MachineFamily{machinetypes.E4},
		},
		"E2/EK used if GeneralPurposePodFamily rule specified and is NOT E2-less region": {
			rule:                              rules.NewRule(rules.WithPodFamilyRule(&generalPurposePodFamily)),
			isE2lessRegion:                    false,
			autopilotManaged:                  true,
			resizableVmWithinPodFamilyEnabled: map[string]bool{machinetypes.EK.Name(): true},
			expectedFamilies:                  []machinetypes.MachineFamily{machinetypes.E2, machinetypes.EK},
		},
		"Default Autopilot pods use E4 in E2-less region": {
			autopilotEnabled: true,
			isE2lessRegion:   true,
			isE4Enabled:      true,
			expectedFamilies: []machinetypes.MachineFamily{machinetypes.E4},
		},
		"Default Autopilot pods use EK and default family in NOT E2-less region": {
			autopilotEnabled:              true,
			isE2lessRegion:                false,
			resizableVmInAutopilotEnabled: map[string]bool{machinetypes.EK.Name(): true},
			expectedFamilies:              []machinetypes.MachineFamily{machinetypes.EK, defaultCloudProviderFamily},
		},
		"E4 used in mixed region if pod is stateless and experiment enabled": {
			rule:                              rules.NewRule(rules.WithPodFamilyRule(&generalPurposePodFamily)),
			isE2lessRegion:                    false,
			isE4Enabled:                       true,
			autopilotManaged:                  true,
			resizableVmWithinPodFamilyEnabled: map[string]bool{machinetypes.EK.Name(): true},
			isStateless:                       true,
			expectedFamilies:                  []machinetypes.MachineFamily{machinetypes.E2, machinetypes.EK, machinetypes.E4},
		},
		"E2/EK used in mixed region if pod is stateful with PVC and experiment enabled": {
			rule:                              rules.NewRule(rules.WithPodFamilyRule(&generalPurposePodFamily)),
			isE2lessRegion:                    false,
			isE4Enabled:                       true,
			autopilotManaged:                  true,
			resizableVmWithinPodFamilyEnabled: map[string]bool{machinetypes.EK.Name(): true},
			isStateless:                       false,
			expectedFamilies:                  []machinetypes.MachineFamily{machinetypes.E2, machinetypes.EK},
		},
		"E2/EK used in mixed region if pod is stateful with Ephemeral and experiment enabled": {
			rule:                              rules.NewRule(rules.WithPodFamilyRule(&generalPurposePodFamily)),
			isE2lessRegion:                    false,
			isE4Enabled:                       true,
			autopilotManaged:                  true,
			resizableVmWithinPodFamilyEnabled: map[string]bool{machinetypes.EK.Name(): true},
			isStateless:                       false,
			expectedFamilies:                  []machinetypes.MachineFamily{machinetypes.E2, machinetypes.EK},
		},
		"E4 still used in E2-less region even if pod is stateful and experiment enabled": {
			rule:                              rules.NewRule(rules.WithPodFamilyRule(&generalPurposePodFamily)),
			isE2lessRegion:                    true,
			isE4Enabled:                       true,
			autopilotManaged:                  true,
			resizableVmWithinPodFamilyEnabled: map[string]bool{machinetypes.EK.Name(): true},
			isStateless:                       false,
			expectedFamilies:                  []machinetypes.MachineFamily{machinetypes.E4},
		},
		"autopilot compute class filters out E4 if isE4Enabled is false": {
			podClass:                      "autopilot",
			autopilotEnabled:              true,
			isE4Enabled:                   false,
			resizableVmInAutopilotEnabled: map[string]bool{machinetypes.EK.Name(): true},
			expectedFamilies:              []machinetypes.MachineFamily{machinetypes.E2, machinetypes.EK},
			expectedComputeClassName:      "autopilot",
		},
		"autopilot compute class keeps E4 if isE4Enabled is true": {
			podClass:                      "autopilot",
			autopilotEnabled:              true,
			isE4Enabled:                   true,
			isStateless:                   true,
			resizableVmInAutopilotEnabled: map[string]bool{machinetypes.EK.Name(): true},
			expectedFamilies:              []machinetypes.MachineFamily{machinetypes.E2, machinetypes.EK, machinetypes.E4},
			expectedComputeClassName:      "autopilot",
		},
		"autopilot compute class filters out E2/EK in E2-less region if isE4Enabled is true": {
			podClass:                      "autopilot",
			autopilotEnabled:              true,
			isE4Enabled:                   true,
			isStateless:                   true,
			isE2lessRegion:                true,
			resizableVmInAutopilotEnabled: map[string]bool{machinetypes.EK.Name(): true},
			expectedFamilies:              []machinetypes.MachineFamily{machinetypes.E4},
			expectedComputeClassName:      "autopilot",
		},
		"E2-less region strips E2 and EK and forces E4 for general purpose": {
			rule:                              rules.NewRule(rules.WithPodFamilyRule(&generalPurposePodFamily)),
			isE2lessRegion:                    true,
			isE4Enabled:                       true,
			autopilotManaged:                  true,
			resizableVmWithinPodFamilyEnabled: map[string]bool{machinetypes.E2.Name(): true, machinetypes.EK.Name(): true},
			expectedFamilies:                  []machinetypes.MachineFamily{machinetypes.E4},
		},
		"extended fallbacks disabled -> returns E2, EK, E4": {
			rule:                              rules.NewRule(rules.WithPodFamilyRule(&generalPurposePodFamily)),
			isE2lessRegion:                    false,
			isE4Enabled:                       true,
			autopilotManaged:                  true,
			resizableVmWithinPodFamilyEnabled: map[string]bool{machinetypes.EK.Name(): true},
			isStateless:                       true,
			isExtendedFallbacksEnabled:        false,
			expectedFamilies:                  []machinetypes.MachineFamily{machinetypes.E2, machinetypes.EK, machinetypes.E4},
		},
		"extended fallbacks enabled -> returns E2, EK, E4 and fallback families": {
			rule:                              rules.NewRule(rules.WithPodFamilyRule(&generalPurposePodFamily)),
			isE2lessRegion:                    false,
			isE4Enabled:                       true,
			autopilotManaged:                  true,
			resizableVmWithinPodFamilyEnabled: map[string]bool{machinetypes.EK.Name(): true},
			isStateless:                       true,
			isExtendedFallbacksEnabled:        true,
			expectedFamilies: []machinetypes.MachineFamily{
				machinetypes.E2, machinetypes.EK, machinetypes.E4,
				machinetypes.N4, machinetypes.N4D,
				machinetypes.N2, machinetypes.N2D,
				machinetypes.N1,
				machinetypes.C4, machinetypes.C4D,
			},
		},
		"extended fallbacks enabled but isStateless=false -> returns E2, EK (E4 filtered)": {
			rule:                              rules.NewRule(rules.WithPodFamilyRule(&generalPurposePodFamily)),
			isE2lessRegion:                    false,
			isE4Enabled:                       true,
			autopilotEnabled:                  true,
			autopilotManaged:                  true,
			resizableVmWithinPodFamilyEnabled: map[string]bool{machinetypes.EK.Name(): true},
			isStateless:                       false,
			isExtendedFallbacksEnabled:        true,
			expectedFamilies:                  []machinetypes.MachineFamily{machinetypes.E2, machinetypes.EK},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			req := map[string]podrequirements.Values{}
			if tc.machineFamily != "" {
				req[gkelabels.MachineFamilyLabel] = podrequirements.NewValues(tc.machineFamily)
			}
			if tc.podClass != "" {
				req[gkelabels.ComputeClassLabel] = podrequirements.NewValues(tc.podClass)
			}
			if tc.wantsSpot {
				req[gkelabels.SpotLabel] = podrequirements.NewValues("true")
			}
			builder := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithConfidentialNodesEnabled(tc.confidentialNodes).
				WithConfidentialInstanceType(tc.confidentialInstanceType).
				WithAutopilotEnabled(tc.autopilotEnabled).
				WithEkSpotEnabled(tc.isEkSpotEnabled).
				WithArmMachineFallbacksEnabled(tc.isArmMachineFallbacksEnabled).
				WithExtendedFallbacksEnabled(tc.isExtendedFallbacksEnabled).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil))
			for family, enabled := range tc.resizableVmInAutopilotEnabled {
				builder = builder.WithResizableVmInAutopilotEnabled(family, enabled)
			}
			for family, enabled := range tc.resizableVmWithinPodFamilyEnabled {
				builder = builder.WithResizableVmWithinPodFamilyEnabled(family, enabled)
			}
			if tc.isE4Enabled {
				builder = builder.WithResizableVmInAutopilotEnabled(machinetypes.E4.Name(), true)
				builder = builder.WithResizableVmWithinPodFamilyEnabled(machinetypes.E4.Name(), true)
			}
			if tc.isE2lessRegion {
				builder = builder.WithAutoprovisioningDefaultFamily(machinetypes.E4)
			}
			provider := builder.Build()
			boolFlags := map[string]bool{}
			for k, v := range tc.boolFlags {
				boolFlags[k] = v
			}

			stringFlags := map[string]string{}
			for k, v := range tc.stringFlags {
				stringFlags[k] = v
			}

			var compVersion version.Version
			if tc.componentVersion != "" {
				compVersion, _ = version.FromString(tc.componentVersion)
			}

			selector := Selector{
				CloudProvider:      provider,
				ExperimentsManager: experiments.NewMockManagerWithOptions(compVersion, boolFlags, stringFlags),
			}
			gotSpec, _, gotErr := selector.selectMachineSpec(podrequirements.NewLabelRequirements(req),
				tc.specifiedGpu, tc.specifiedTpu, tc.specifiedBootDiskType,
				tc.architectures, tc.rule, tc.wantsSpot, tc.specifiedReservationMachineType, tc.autopilotEnabled, tc.autopilotManaged, tc.isStateless)
			expectedPlatform := machinetypes.AnyPlatform
			if tc.expectedErr != nil {
				// Check that selectMachineSpec returns an empty spec if there's an error.
				expectedPlatform = ""
			}
			if tc.expectedMinCpuPlatform != nil {
				expectedPlatform = *tc.expectedMinCpuPlatform
			}
			var expectedConfidentialNodesEnabled bool
			var expectedConfidentialNodeType string
			if tc.expectedErr == nil {
				expectedConfidentialNodesEnabled = tc.confidentialNodes || tc.confidentialInstanceType != ""
				expectedConfidentialNodeType = tc.confidentialInstanceType
			}
			expectedSpec := machinetypes.MachineSpec{
				Families:                 tc.expectedFamilies,
				ComputeClassName:         tc.expectedComputeClassName,
				GpuType:                  tc.specifiedGpu,
				TpuType:                  tc.specifiedTpu,
				BootDiskType:             tc.specifiedBootDiskType,
				MinCpuPlatform:           expectedPlatform,
				ExplicitMachineTypes:     tc.expectedMachineType,
				ConfidentialNodesEnabled: expectedConfidentialNodesEnabled,
				ConfidentialNodeType:     expectedConfidentialNodeType,
			}
			assert.Equal(t, expectedSpec, gotSpec)
			assert.Equal(t, tc.expectedErr, gotErr)
		})
	}
}

func TestSelectMinCpuPlatform(t *testing.T) {
	for tn, tc := range map[string]struct {
		families                  []machinetypes.MachineFamily
		podPlatformFromRequested  string
		podPlatformsFromSupported []string
		clusterWidePlatform       string
		rule                      rules.Rule
		expectedPlatform          machinetypes.CpuPlatform
		expectedErr               error
	}{
		"AnyPlatform returned if nothing specified": {
			expectedPlatform: machinetypes.AnyPlatform,
		},
		"cluster-wide platform returned if specified and compatible with the only family": {
			families:            []machinetypes.MachineFamily{machinetypes.N1},
			clusterWidePlatform: "Intel Broadwell",
			expectedPlatform:    machinetypes.IntelBroadwell,
		},
		"cluster-wide platform returned if specified and compatible with all the families": {
			families:            []machinetypes.MachineFamily{machinetypes.N1, machinetypes.M1},
			clusterWidePlatform: "Intel Broadwell",
			expectedPlatform:    machinetypes.IntelBroadwell,
		},
		"cluster-wide platform returned if specified and compatible with some of the families": {
			families:            []machinetypes.MachineFamily{machinetypes.N1, machinetypes.E2},
			clusterWidePlatform: "Intel Broadwell",
			expectedPlatform:    machinetypes.IntelBroadwell,
		},
		"fall back to AnyPlatform if cluster-wide platform specified, but not compatible with the only family": {
			families:            []machinetypes.MachineFamily{machinetypes.E2},
			clusterWidePlatform: "Intel Broadwell",
			expectedPlatform:    machinetypes.AnyPlatform,
		},
		"fall back to AnyPlatform if cluster-wide platform specified, but not compatible with any of the families": {
			families:            []machinetypes.MachineFamily{machinetypes.N2D, machinetypes.E2},
			clusterWidePlatform: "Intel Broadwell",
			expectedPlatform:    machinetypes.AnyPlatform,
		},
		"fall back to AnyPlatform if cluster-wide platform specified, but unknown": {
			families:            []machinetypes.MachineFamily{machinetypes.N1},
			clusterWidePlatform: "not-supported",
			expectedPlatform:    machinetypes.AnyPlatform,
		},
		"pod platform specified via cloud.google.com/requested-min-cpu-platform is always returned": {
			families:                 []machinetypes.MachineFamily{machinetypes.N1},
			podPlatformFromRequested: "Intel Broadwell",
			expectedPlatform:         machinetypes.IntelBroadwell,
		},
		"pod platform specified via supported-cpu-platform.cloud.google.com/ is always returned": {
			families:                  []machinetypes.MachineFamily{machinetypes.N1},
			podPlatformsFromSupported: []string{"Intel Broadwell"},
			expectedPlatform:          machinetypes.IntelBroadwell,
		},
		"specified pod platform is always returned, even if incompatible with chosen family": {
			families:                 []machinetypes.MachineFamily{machinetypes.E2},
			podPlatformFromRequested: "Intel Broadwell",
			expectedPlatform:         machinetypes.IntelBroadwell,
		},
		"specified pod platform overrides cluster-wide": {
			families:                 []machinetypes.MachineFamily{machinetypes.N1},
			podPlatformFromRequested: "Intel Broadwell",
			clusterWidePlatform:      "Intel Haswell",
			expectedPlatform:         machinetypes.IntelBroadwell,
		},
		"specified pod platform overrides cluster-wide, even if incompatible with chosen family": {
			families:                 []machinetypes.MachineFamily{machinetypes.N1},
			podPlatformFromRequested: "AMD Rome",
			clusterWidePlatform:      "Intel Haswell",
			expectedPlatform:         machinetypes.AmdRome,
		},
		"specified pod platform overrides cluster-wide, even if cluster-wide is incompatible": {
			families:                 []machinetypes.MachineFamily{machinetypes.N1},
			podPlatformFromRequested: "Intel Broadwell",
			clusterWidePlatform:      "AMD Rome",
			expectedPlatform:         machinetypes.IntelBroadwell,
		},
		"specified pod platform overrides cluster-wide, even if cluster-wide is unknown": {
			families:                 []machinetypes.MachineFamily{machinetypes.N1},
			podPlatformFromRequested: "Intel Broadwell",
			clusterWidePlatform:      "not-supported",
			expectedPlatform:         machinetypes.IntelBroadwell,
		},
		"unknown pod platform results in an explicit error": {
			families:                 []machinetypes.MachineFamily{machinetypes.N1},
			podPlatformFromRequested: "not-supported",
			expectedPlatform:         machinetypes.UnknownPlatform,
			expectedErr:              NewMinCpuPlatformUnknownError("not-supported"),
		},
		"unknown pod platform results in an explicit error, even if cluster-wide is specified": {
			families:                 []machinetypes.MachineFamily{machinetypes.N1},
			podPlatformFromRequested: "not-supported",
			clusterWidePlatform:      "Intel Broadwell",
			expectedPlatform:         machinetypes.UnknownPlatform,
			expectedErr:              NewMinCpuPlatformUnknownError("not-supported"),
		},
		"pod specifying a platform both via cloud.google.com/requested-min-cpu-platform and supported-cpu-platform.cloud.google.com/ is an explicit error": {
			families:                  []machinetypes.MachineFamily{machinetypes.N1},
			podPlatformFromRequested:  "Intel Broadwell",
			podPlatformsFromSupported: []string{"Intel Haswell"},
			expectedPlatform:          machinetypes.UnknownPlatform,
			expectedErr:               NewMultipleMinCpuPlatformsError(),
		},
		"pod specifying multiple platform via supported-cpu-platform.cloud.google.com/ is an explicit error": {
			families:                  []machinetypes.MachineFamily{machinetypes.N1},
			podPlatformsFromSupported: []string{"Intel Haswell", "Intel Broadwell"},
			expectedPlatform:          machinetypes.UnknownPlatform,
			expectedErr:               NewMultipleMinCpuPlatformsError(),
		},
		"AnyPlatform returned if rule has nothing specified": {
			rule:             rules.NewRule(),
			expectedPlatform: machinetypes.AnyPlatform,
		},
		"specified rule platform returned if rule has it specified": {
			rule:             rules.NewRule(rules.WithMinCpuPlatformRule(ptr.To("AMD Genoa"))),
			expectedPlatform: machinetypes.AmdGenoa,
		},
		"unknown rule platform results in an explicit error": {
			rule:             rules.NewRule(rules.WithMinCpuPlatformRule(ptr.To("not-supported"))),
			expectedPlatform: machinetypes.UnknownPlatform,
			expectedErr:      NewMinCpuPlatformUnknownError("not-supported"),
		},
		"specified rule platform overrides pod platform and cluster-wide": {
			families:                 []machinetypes.MachineFamily{machinetypes.N1},
			rule:                     rules.NewRule(rules.WithMinCpuPlatformRule(ptr.To("AMD Genoa"))),
			podPlatformFromRequested: "Intel Broadwell",
			clusterWidePlatform:      "Intel Haswell",
			expectedPlatform:         machinetypes.AmdGenoa,
		},
		"unknown rule platform results in an explicit error even if pod platform and cluster-wide are correct": {
			families:                 []machinetypes.MachineFamily{machinetypes.N1},
			rule:                     rules.NewRule(rules.WithMinCpuPlatformRule(ptr.To("not-supported"))),
			podPlatformFromRequested: "Intel Broadwell",
			clusterWidePlatform:      "Intel Haswell",
			expectedPlatform:         machinetypes.UnknownPlatform,
			expectedErr:              NewMinCpuPlatformUnknownError("not-supported"),
		},
		"AnyPlatform rule platform does override pod platform": {
			families:                 []machinetypes.MachineFamily{machinetypes.N1},
			rule:                     rules.NewRule(),
			podPlatformFromRequested: "Intel Broadwell",
			expectedPlatform:         machinetypes.AnyPlatform,
		},
		"AnyPlatform rule platform does override cluster-wide": {
			families:            []machinetypes.MachineFamily{machinetypes.N1},
			rule:                rules.NewRule(),
			clusterWidePlatform: "Intel Haswell",
			expectedPlatform:    machinetypes.AnyPlatform,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			req := map[string]podrequirements.Values{}
			if tc.podPlatformFromRequested != "" {
				req[gkelabels.RequestedMinCpuPlatformLabel] = podrequirements.NewValues(tc.podPlatformFromRequested)
			}
			for _, platform := range tc.podPlatformsFromSupported {
				req[gkelabels.SupportedCpuPlatformKey(platform)] = podrequirements.NewValues(gkelabels.SupportedCpuPlatformValue)
			}
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithDefaultNodePoolMinCpuPlatform(tc.clusterWidePlatform).Build()
			selector := Selector{CloudProvider: provider}
			gotPlatform, gotErr := selector.selectMinCpuPlatform(tc.families, podrequirements.NewLabelRequirements(req), tc.rule)
			assert.Equal(t, tc.expectedPlatform, gotPlatform)
			assert.Equal(t, tc.expectedErr, gotErr)
		})
	}
}

func TestLimitArchitectures(t *testing.T) {
	for tn, tc := range map[string]struct {
		spec                  machinetypes.MachineSpec
		architectures         map[gce.SystemArchitecture]bool
		autopilotEnabled      bool
		expectedArchitectures map[gce.SystemArchitecture]bool
		expectedErr           error
	}{
		"no architectures specified remains no architectures specified": {
			spec:                  machinetypes.NewMachineSpecSingleFamily(machinetypes.N2, machinetypes.AnyPlatform, "", ""),
			architectures:         nil,
			expectedArchitectures: nil,
		},
		"amd64 remains amd64": {
			spec:                  machinetypes.NewMachineSpecSingleFamily(machinetypes.N2, machinetypes.AnyPlatform, "", ""),
			architectures:         map[gce.SystemArchitecture]bool{gce.Amd64: true},
			expectedArchitectures: map[gce.SystemArchitecture]bool{gce.Amd64: true},
		},
		"arm64 remains arm64": {
			spec:                  machinetypes.NewMachineSpecSingleFamily(machinetypes.N2, machinetypes.AnyPlatform, "", ""),
			architectures:         map[gce.SystemArchitecture]bool{gce.Arm64: true},
			expectedArchitectures: map[gce.SystemArchitecture]bool{gce.Arm64: true},
		},
		"arm64+amd64 remains arm64+amd64": {
			spec:                  machinetypes.NewMachineSpecSingleFamily(machinetypes.N2, machinetypes.AnyPlatform, "", ""),
			architectures:         map[gce.SystemArchitecture]bool{gce.Arm64: true, gce.Amd64: true},
			expectedArchitectures: map[gce.SystemArchitecture]bool{gce.Arm64: true, gce.Amd64: true},
		},
		"Scale-Out compute class without arch specified defaults to amd64": {
			autopilotEnabled: true,
			spec: machinetypes.MachineSpec{
				Families:         machinetypes.ScaleOutClass.MachineFamilies(),
				MinCpuPlatform:   machinetypes.AnyPlatform,
				ComputeClassName: machinetypes.ScaleOutClass.Name(),
			},
			architectures:         nil,
			expectedArchitectures: map[gce.SystemArchitecture]bool{gce.Amd64: true},
		},
		"Scale-Out compute class with arch specified remains this arch": {
			autopilotEnabled: true,
			spec: machinetypes.MachineSpec{
				Families:         machinetypes.ScaleOutClass.MachineFamilies(),
				MinCpuPlatform:   machinetypes.AnyPlatform,
				ComputeClassName: machinetypes.ScaleOutClass.Name(),
			},
			architectures:         map[gce.SystemArchitecture]bool{gce.Arm64: true},
			expectedArchitectures: map[gce.SystemArchitecture]bool{gce.Arm64: true},
		},
		"Scale-Out compute class with multiple architectures specified remains these architectures": {
			autopilotEnabled: true,
			spec: machinetypes.MachineSpec{
				Families:         machinetypes.ScaleOutClass.MachineFamilies(),
				MinCpuPlatform:   machinetypes.AnyPlatform,
				ComputeClassName: machinetypes.ScaleOutClass.Name(),
			},
			architectures:         map[gce.SystemArchitecture]bool{gce.Arm64: true, gce.Amd64: true},
			expectedArchitectures: map[gce.SystemArchitecture]bool{gce.Arm64: true, gce.Amd64: true},
		},
		"Autopilot, no architectures specified -> no change": {
			autopilotEnabled:      true,
			spec:                  machinetypes.NewMachineSpecSingleFamily(machinetypes.T2A, machinetypes.AnyPlatform, "", ""),
			architectures:         nil,
			expectedArchitectures: nil,
		},
		"Autopilot, no compute class, only ARM specified -> no change": {
			autopilotEnabled:      true,
			spec:                  machinetypes.NewMachineSpecSingleFamily(machinetypes.T2A, machinetypes.AnyPlatform, "", ""),
			architectures:         map[gce.SystemArchitecture]bool{gce.Arm64: true},
			expectedArchitectures: map[gce.SystemArchitecture]bool{gce.Arm64: true},
		},
		"Autopilot, no compute class, only AMD specified -> no change": {
			autopilotEnabled:      true,
			spec:                  machinetypes.NewMachineSpecSingleFamily(machinetypes.T2A, machinetypes.AnyPlatform, "", ""),
			architectures:         map[gce.SystemArchitecture]bool{gce.Amd64: true},
			expectedArchitectures: map[gce.SystemArchitecture]bool{gce.Amd64: true},
		},
		"Autopilot, no compute class, both AMD and ARM specified -> no change": {
			autopilotEnabled:      true,
			spec:                  machinetypes.NewMachineSpecSingleFamily(machinetypes.T2A, machinetypes.AnyPlatform, "", ""),
			architectures:         map[gce.SystemArchitecture]bool{gce.Amd64: true, gce.Arm64: true},
			expectedArchitectures: map[gce.SystemArchitecture]bool{gce.Amd64: true, gce.Arm64: true},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithAutopilotEnabled(tc.autopilotEnabled).Build()
			selector := Selector{CloudProvider: provider}
			gotArchitectures, gotErr := selector.limitArchitectures(tc.spec, tc.architectures)
			assert.Equal(t, tc.expectedArchitectures, gotArchitectures)
			assert.Equal(t, tc.expectedErr, gotErr)
		})
	}
}

func TestLimitMachineSpec(t *testing.T) {
	// This is a family where the "gpu-type-1" + IntelCascadeLake config is invalid because all machine types only support parts of it
	// (machine-type-1 supports "gpu-type-1" but not IntelCascadeLake, machine-type-2 supports IntelCascadeLake but not "gpu-type-1").
	intelIceLake := machinetypes.NewCpuPlatformRequirements(machinetypes.IntelIceLake, machinetypes.IntelIceLake)
	cascadeLake := machinetypes.NewCpuPlatformRequirements(machinetypes.IntelCascadeLake, machinetypes.IntelCascadeLake)
	gpuType1 := machinetypes.NewTestGpu("gpu-type-1", true, nil, nil)
	gpuType2 := machinetypes.NewTestGpu("gpu-type-2", true, nil, nil)

	constraintsTestFamily := machinetypes.NewTestMachineFamily("test-family",
		[]machinetypes.MachineType{machinetypes.NewTestMachineTypeInfo(gce.MachineType{Name: "machine-type-1"}, machinetypes.NewTestMachineGpuSpec(gpuType1, 0), &intelIceLake, nil),
			machinetypes.NewTestMachineTypeInfo(gce.MachineType{Name: "machine-type-2"}, machinetypes.NewTestMachineGpuSpec(gpuType2, 0), &cascadeLake, nil)},
		machinetypes.UnknownPlatform, machinetypes.UnknownPlatform, nil, nil)

	for tn, tc := range map[string]struct {
		spec                     machinetypes.MachineSpec
		architectures            map[gce.SystemArchitecture]bool
		confidentialNodes        bool
		confidentialInstanceType string
		expectedSpec             machinetypes.MachineSpec
		expectedErr              error
	}{
		"M2 is supported in NAP": {
			spec:         machinetypes.NewMachineSpecSingleFamily(machinetypes.M2, machinetypes.AnyPlatform, "", ""),
			expectedSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.M2, machinetypes.AnyPlatform, "", ""),
		},
		"M3 is supported in NAP": {
			spec:         machinetypes.NewMachineSpecSingleFamily(machinetypes.M3, machinetypes.AnyPlatform, "", ""),
			expectedSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.M3, machinetypes.AnyPlatform, "", ""),
		},
		"M4 is supported in NAP": {
			spec:         machinetypes.NewMachineSpecSingleFamily(machinetypes.M4, machinetypes.AnyPlatform, "", ""),
			expectedSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.M4, machinetypes.AnyPlatform, "", ""),
		},
		"N4A is supported in NAP": {
			spec:         machinetypes.NewMachineSpecSingleFamily(machinetypes.N4A, machinetypes.AnyPlatform, "", ""),
			expectedSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.N4A, machinetypes.AnyPlatform, "", ""),
		},
		"N4D is supported in NAP": {
			spec:         machinetypes.NewMachineSpecSingleFamily(machinetypes.N4D, machinetypes.AnyPlatform, "", ""),
			expectedSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.N4D, machinetypes.AnyPlatform, "", ""),
		},
		"E4 is supported in NAP": {
			spec:         machinetypes.NewMachineSpecSingleFamily(machinetypes.E4, machinetypes.AnyPlatform, "", ""),
			expectedSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.E4, machinetypes.AnyPlatform, "", ""),
		},
		"N2D works with Confidential Nodes": {
			spec:              machinetypes.NewMachineSpecSingleFamily(machinetypes.N2D, machinetypes.AnyPlatform, "", ""),
			confidentialNodes: true,
			expectedSpec:      machinetypes.NewMachineSpecSingleFamily(machinetypes.N2D, machinetypes.AnyPlatform, "", ""),
		},
		"C2D works with Confidential Nodes": {
			spec:                     machinetypes.NewMachineSpecSingleFamily(machinetypes.C2D, machinetypes.AnyPlatform, "", ""),
			confidentialNodes:        true,
			confidentialInstanceType: gkelabels.SEVConfidentialNodeTypeValue,
			expectedSpec:             machinetypes.NewMachineSpecSingleFamily(machinetypes.C2D, machinetypes.AnyPlatform, "", ""),
		},
		"C3 does not work with Confidential Nodes if confidential instance type is not TDX": {
			spec:              machinetypes.NewMachineSpecSingleFamily(machinetypes.C3, machinetypes.AnyPlatform, "", ""),
			confidentialNodes: true,
			expectedErr:       NewConfidentialNodesIncompatibleError(`machine family "c3"`),
		},
		"C3 works with Confidential Nodes if confidential instance type is TDX": {
			spec:                     machinetypes.NewMachineSpecSingleFamily(machinetypes.C3, machinetypes.AnyPlatform, "", ""),
			confidentialNodes:        true,
			confidentialInstanceType: gkelabels.TDXConfidentialNodeTypeValue,
			expectedSpec:             machinetypes.NewMachineSpecSingleFamily(machinetypes.C3, machinetypes.AnyPlatform, "", ""),
		},
		"C3 works with Confidential Nodes if confidential instance type is TDX and confidentialNodes is unset": {
			spec:                     machinetypes.NewMachineSpecSingleFamily(machinetypes.C3, machinetypes.AnyPlatform, "", ""),
			confidentialInstanceType: gkelabels.TDXConfidentialNodeTypeValue,
			expectedSpec:             machinetypes.NewMachineSpecSingleFamily(machinetypes.C3, machinetypes.AnyPlatform, "", ""),
		},
		"C4 works with Confidential Nodes if confidential instance type is TDX": {
			spec:                     machinetypes.NewMachineSpecSingleFamily(machinetypes.C4, machinetypes.AnyPlatform, "", ""),
			confidentialNodes:        true,
			confidentialInstanceType: gkelabels.TDXConfidentialNodeTypeValue,
			expectedSpec:             machinetypes.NewMachineSpecSingleFamily(machinetypes.C4, machinetypes.AnyPlatform, "", ""),
		},
		"C4 does not work with Confidential Nodes if confidential instance type is not TDX": {
			spec:                     machinetypes.NewMachineSpecSingleFamily(machinetypes.C4, machinetypes.AnyPlatform, "", ""),
			confidentialNodes:        true,
			confidentialInstanceType: gkelabels.SEVConfidentialNodeTypeValue,
			expectedErr:              NewConfidentialNodesIncompatibleError(`machine family "c4"`),
		},
		"C4 works with Confidential Nodes if confidential instance type is TDX and confidentialNodes is unset": {
			spec:                     machinetypes.NewMachineSpecSingleFamily(machinetypes.C4, machinetypes.AnyPlatform, "", ""),
			confidentialInstanceType: gkelabels.TDXConfidentialNodeTypeValue,
			expectedSpec:             machinetypes.NewMachineSpecSingleFamily(machinetypes.C4, machinetypes.AnyPlatform, "", ""),
		},
		"N2D works with Confidential Nodes if confidential instance type is SEV and confidentialNodes is unset": {
			spec:                     machinetypes.NewMachineSpecSingleFamily(machinetypes.N2D, machinetypes.AnyPlatform, "", ""),
			confidentialInstanceType: gkelabels.SEVConfidentialNodeTypeValue,
			expectedSpec:             machinetypes.NewMachineSpecSingleFamily(machinetypes.N2D, machinetypes.AnyPlatform, "", ""),
		},
		"G4 works with Confidential Nodes if confidential instance type is SEV and RTX PRO 6000 is specified": {
			spec:                     machinetypes.NewMachineSpecSingleFamily(machinetypes.G4, machinetypes.AnyPlatform, machinetypes.NvidiaRTXPro6000.Name(), ""),
			confidentialNodes:        true,
			confidentialInstanceType: gkelabels.SEVConfidentialNodeTypeValue,
			expectedSpec:             machinetypes.NewMachineSpecSingleFamily(machinetypes.G4, machinetypes.AnyPlatform, machinetypes.NvidiaRTXPro6000.Name(), ""),
		},
		"N2D works with Confidential Nodes if confidential instance type is SEV_SNP and confidentialNodes is unset": {
			spec:                     machinetypes.NewMachineSpecSingleFamily(machinetypes.N2D, machinetypes.AnyPlatform, "", ""),
			confidentialInstanceType: gkelabels.SEVSNPConfidentialNodeTypeValue,
			expectedSpec:             machinetypes.NewMachineSpecSingleFamily(machinetypes.N2D, machinetypes.AnyPlatform, "", ""),
		},
		"A3 does not work with Confidential Nodes if confidential instance type is not TDX": {
			spec:              machinetypes.NewMachineSpecSingleFamily(machinetypes.A3, machinetypes.AnyPlatform, "", ""),
			confidentialNodes: true,
			expectedErr:       NewConfidentialNodesIncompatibleError(`machine family "a3"`),
		},
		"A3 works with Confidential Nodes if confidential instance type is TDX": {
			spec:                     machinetypes.NewMachineSpecSingleFamily(machinetypes.A3, machinetypes.AnyPlatform, "", ""),
			confidentialNodes:        true,
			confidentialInstanceType: gkelabels.TDXConfidentialNodeTypeValue,
			expectedSpec:             machinetypes.NewMachineSpecSingleFamily(machinetypes.A3, machinetypes.AnyPlatform, "", ""),
		},
		"ok if one of the families is incompatible with Confidential Nodes": {
			spec:                     machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.N2D, machinetypes.N1}, machinetypes.AnyPlatform, "", ""),
			confidentialNodes:        true,
			confidentialInstanceType: gkelabels.SEVConfidentialNodeTypeValue,
			expectedSpec:             machinetypes.NewMachineSpecSingleFamily(machinetypes.N2D, machinetypes.AnyPlatform, "", ""),
		},
		"error if family is incompatible with Confidential Nodes": {
			spec:                     machinetypes.NewMachineSpecSingleFamily(machinetypes.N1, machinetypes.AnyPlatform, "", ""),
			confidentialNodes:        true,
			confidentialInstanceType: gkelabels.SEVConfidentialNodeTypeValue,
			expectedErr:              NewConfidentialNodesIncompatibleError(`machine family "n1"`),
		},
		"error if all of the families are incompatible with Confidential Nodes": {
			spec:              machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.N2, machinetypes.N1}, machinetypes.AnyPlatform, "", ""),
			confidentialNodes: true,
			expectedErr:       NewConfidentialNodesIncompatibleError(`machine family "n2"`),
		},
		"error if compute class is incompatible with Confidential Nodes": {
			spec: machinetypes.MachineSpec{
				Families:         []machinetypes.MachineFamily{machinetypes.N2, machinetypes.N1},
				MinCpuPlatform:   machinetypes.AnyPlatform,
				ComputeClassName: "test-class",
			},
			confidentialNodes: true,
			expectedErr:       NewConfidentialNodesIncompatibleError(`compute class "test-class"`),
		},
		"A2 works with A100": {
			spec:         machinetypes.NewMachineSpecSingleFamily(machinetypes.A2, machinetypes.AnyPlatform, machinetypes.NvidiaTeslaA100.Name(), ""),
			expectedSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.A2, machinetypes.AnyPlatform, machinetypes.NvidiaTeslaA100.Name(), ""),
		},
		"A2 works with A100-80gb": {
			spec:         machinetypes.NewMachineSpecSingleFamily(machinetypes.A2, machinetypes.AnyPlatform, machinetypes.NvidiaA100_80gb.Name(), ""),
			expectedSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.A2, machinetypes.AnyPlatform, machinetypes.NvidiaA100_80gb.Name(), ""),
		},
		"ok if one of the families is incompatible with A100": {
			spec:         machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.A2, machinetypes.N1}, machinetypes.AnyPlatform, machinetypes.NvidiaTeslaA100.Name(), ""),
			expectedSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.A2, machinetypes.AnyPlatform, machinetypes.NvidiaTeslaA100.Name(), ""),
		},
		"ok if one of the families is incompatible with A100-80gb": {
			spec:         machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.A2, machinetypes.N1}, machinetypes.AnyPlatform, machinetypes.NvidiaA100_80gb.Name(), ""),
			expectedSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.A2, machinetypes.AnyPlatform, machinetypes.NvidiaA100_80gb.Name(), ""),
		},
		"error if family is incompatible with A100": {
			spec:        machinetypes.NewMachineSpecSingleFamily(machinetypes.N2, machinetypes.AnyPlatform, machinetypes.NvidiaTeslaA100.Name(), ""),
			expectedErr: NewGpuIncompatibleError(`machine family "n2"`, machinetypes.NvidiaTeslaA100.Name()),
		},
		"error if family is incompatible with A100-80gb": {
			spec:        machinetypes.NewMachineSpecSingleFamily(machinetypes.N2, machinetypes.AnyPlatform, machinetypes.NvidiaA100_80gb.Name(), ""),
			expectedErr: NewGpuIncompatibleError(`machine family "n2"`, machinetypes.NvidiaA100_80gb.Name()),
		},
		"error if all of the families are incompatible with A100": {
			spec:        machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.N2, machinetypes.N1}, machinetypes.AnyPlatform, machinetypes.NvidiaTeslaA100.Name(), ""),
			expectedErr: NewGpuIncompatibleError(`machine family "n2"`, machinetypes.NvidiaTeslaA100.Name()),
		},
		"error if all of the families are incompatible with A100-80gb": {
			spec:        machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.N2, machinetypes.N1}, machinetypes.AnyPlatform, machinetypes.NvidiaA100_80gb.Name(), ""),
			expectedErr: NewGpuIncompatibleError(`machine family "n2"`, machinetypes.NvidiaA100_80gb.Name()),
		},
		"error if compute class is incompatible with A100": {
			spec: machinetypes.MachineSpec{
				Families:         []machinetypes.MachineFamily{machinetypes.N2, machinetypes.N1},
				MinCpuPlatform:   machinetypes.AnyPlatform,
				GpuType:          machinetypes.NvidiaTeslaA100.Name(),
				ComputeClassName: "test-class",
			},
			expectedErr: NewGpuIncompatibleError(`compute class "test-class"`, machinetypes.NvidiaTeslaA100.Name()),
		},
		"error if compute class is incompatible with A100-80gb": {
			spec: machinetypes.MachineSpec{
				Families:         []machinetypes.MachineFamily{machinetypes.N2, machinetypes.N1},
				MinCpuPlatform:   machinetypes.AnyPlatform,
				GpuType:          machinetypes.NvidiaA100_80gb.Name(),
				ComputeClassName: "test-class",
			},
			expectedErr: NewGpuIncompatibleError(`compute class "test-class"`, machinetypes.NvidiaA100_80gb.Name()),
		},
		"N1 works with other GPUs": {
			spec:         machinetypes.NewMachineSpecSingleFamily(machinetypes.N1, machinetypes.AnyPlatform, machinetypes.NvidiaTeslaK80.Name(), ""),
			expectedSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.N1, machinetypes.AnyPlatform, machinetypes.NvidiaTeslaK80.Name(), ""),
		},
		"ok if one of the families is incompatible with other GPUs": {
			spec:         machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.A2, machinetypes.N1}, machinetypes.AnyPlatform, machinetypes.NvidiaTeslaK80.Name(), ""),
			expectedSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.N1, machinetypes.AnyPlatform, machinetypes.NvidiaTeslaK80.Name(), ""),
		},
		"error if family is incompatible with other GPUs": {
			spec:        machinetypes.NewMachineSpecSingleFamily(machinetypes.A2, machinetypes.AnyPlatform, machinetypes.NvidiaTeslaK80.Name(), ""),
			expectedErr: NewGpuIncompatibleError(`machine family "a2"`, machinetypes.NvidiaTeslaK80.Name()),
		},
		"error if all of the families are incompatible with other GPUs": {
			spec:        machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.N2, machinetypes.N2D}, machinetypes.AnyPlatform, machinetypes.NvidiaTeslaK80.Name(), ""),
			expectedErr: NewGpuIncompatibleError(`machine family "n2"`, machinetypes.NvidiaTeslaK80.Name()),
		},
		"error if compute class is incompatible with other GPUs": {
			spec: machinetypes.MachineSpec{
				Families:         []machinetypes.MachineFamily{machinetypes.N2, machinetypes.N2D},
				MinCpuPlatform:   machinetypes.AnyPlatform,
				GpuType:          machinetypes.NvidiaTeslaK80.Name(),
				ComputeClassName: "test-class",
			},
			expectedErr: NewGpuIncompatibleError(`compute class "test-class"`, machinetypes.NvidiaTeslaK80.Name()),
		},
		"K80 works with platform lower than Intel Skylake": {
			spec:         machinetypes.NewMachineSpecSingleFamily(machinetypes.N1, machinetypes.IntelBroadwell, machinetypes.NvidiaTeslaK80.Name(), ""),
			expectedSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.N1, machinetypes.IntelBroadwell, machinetypes.NvidiaTeslaK80.Name(), ""),
		},
		"error if platform is incompatible with K80": {
			spec:        machinetypes.NewMachineSpecSingleFamily(machinetypes.N1, machinetypes.IntelSkylake, machinetypes.NvidiaTeslaK80.Name(), ""),
			expectedErr: NewGpuMinCpuPlatformIncompatibleError(machinetypes.CpuPlatformDebugName(machinetypes.IntelSkylake), machinetypes.NvidiaTeslaK80.Name()),
		},
		"compatible families and platforms pass validation": {
			spec:         machinetypes.NewMachineSpecSingleFamily(machinetypes.N1, machinetypes.IntelBroadwell, "", ""),
			expectedSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.N1, machinetypes.IntelBroadwell, "", ""),
		},
		"okay if one of the families is compatible with min_cpu_platform": {
			spec:         machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.N1, machinetypes.N2D}, machinetypes.IntelBroadwell, "", ""),
			expectedSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.N1, machinetypes.IntelBroadwell, "", ""),
		},
		"error if family is incompatible with min_cpu_platform": {
			spec:        machinetypes.NewMachineSpecSingleFamily(machinetypes.N1, machinetypes.IntelIceLake, "", ""),
			expectedErr: NewMinCpuPlatformInvalidError(`machine family "n1"`, "Intel Ice Lake"),
		},
		"error if all of the families are incompatible with min_cpu_platform": {
			spec:        machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.N2D, machinetypes.N1}, machinetypes.IntelIceLake, "", ""),
			expectedErr: NewMinCpuPlatformInvalidError(`machine family "n2d"`, "Intel Ice Lake"),
		},
		"error if compute class is incompatible with min_cpu_platform": {
			spec: machinetypes.MachineSpec{
				Families:         []machinetypes.MachineFamily{machinetypes.N2D, machinetypes.N1},
				MinCpuPlatform:   machinetypes.IntelIceLake,
				ComputeClassName: "test-class",
			},
			expectedErr: NewMinCpuPlatformInvalidError(`compute class "test-class"`, "Intel Ice Lake"),
		},
		"compatible architecture and machine pass validation": {
			spec:          machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.T2A, machinetypes.C4A}, machinetypes.AnyPlatform, "", ""),
			architectures: map[gce.SystemArchitecture]bool{gce.Arm64: true},
			expectedSpec:  machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.T2A, machinetypes.C4A}, machinetypes.AnyPlatform, "", ""),
		},
		"machines T2A compatible with any of the architectures pass validation": {
			spec:          machinetypes.NewMachineSpecSingleFamily(machinetypes.T2A, machinetypes.AnyPlatform, "", ""),
			architectures: map[gce.SystemArchitecture]bool{gce.Arm64: true, gce.Amd64: true},
			expectedSpec:  machinetypes.NewMachineSpecSingleFamily(machinetypes.T2A, machinetypes.AnyPlatform, "", ""),
		},
		"machines C4A compatible with any of the architectures pass validation": {
			spec:          machinetypes.NewMachineSpecSingleFamily(machinetypes.C4A, machinetypes.AnyPlatform, "", ""),
			architectures: map[gce.SystemArchitecture]bool{gce.Arm64: true, gce.Amd64: true},
			expectedSpec:  machinetypes.NewMachineSpecSingleFamily(machinetypes.C4A, machinetypes.AnyPlatform, "", ""),
		},
		"different families can be compatible with different architectures": {
			spec:          machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.T2A, machinetypes.C4A, machinetypes.N1}, machinetypes.AnyPlatform, "", ""),
			architectures: map[gce.SystemArchitecture]bool{gce.Arm64: true, gce.Amd64: true},
			expectedSpec:  machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.T2A, machinetypes.C4A, machinetypes.N1}, machinetypes.AnyPlatform, "", ""),
		},
		"okay if one of the families is incompatible with architecture": {
			spec:          machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.C4A, machinetypes.N1}, machinetypes.AnyPlatform, "", ""),
			architectures: map[gce.SystemArchitecture]bool{gce.Arm64: true},
			expectedSpec:  machinetypes.NewMachineSpecSingleFamily(machinetypes.C4A, machinetypes.AnyPlatform, "", ""),
		},
		"error if family is incompatible with arch": {
			spec:          machinetypes.NewMachineSpecSingleFamily(machinetypes.N1, machinetypes.AnyPlatform, "", ""),
			architectures: map[gce.SystemArchitecture]bool{gce.Arm64: true},
			expectedErr:   NewSystemArchitectureIncompatibleError(`machine family "n1"`, "arm64"),
		},
		"error if family is incompatible with all architectures": {
			spec:          machinetypes.NewMachineSpecSingleFamily(machinetypes.N1, machinetypes.AnyPlatform, "", ""),
			architectures: map[gce.SystemArchitecture]bool{gce.Arm64: true, gce.SystemArchitecture("some-arch"): true},
			expectedErr:   NewSystemArchitectureIncompatibleError(`machine family "n1"`, "arm64,some-arch"),
		},
		"error if all of the families are incompatible with architecture": {
			spec:          machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.M1, machinetypes.N1}, machinetypes.AnyPlatform, "", ""),
			architectures: map[gce.SystemArchitecture]bool{gce.Arm64: true},
			expectedErr:   NewSystemArchitectureIncompatibleError(`machine family "m1"`, "arm64"),
		},
		"error if compute class is incompatible with architecture": {
			spec: machinetypes.MachineSpec{
				Families:         []machinetypes.MachineFamily{machinetypes.M1, machinetypes.N1},
				MinCpuPlatform:   machinetypes.AnyPlatform,
				ComputeClassName: "test-class",
			},
			architectures: map[gce.SystemArchitecture]bool{gce.Arm64: true},
			expectedErr:   NewSystemArchitectureIncompatibleError(`compute class "test-class"`, "arm64"),
		},
		"error if machine family is incompatible with reservation machine type": {
			spec: machinetypes.MachineSpec{
				Families:             []machinetypes.MachineFamily{machinetypes.M1},
				MinCpuPlatform:       machinetypes.AnyPlatform,
				ExplicitMachineTypes: []string{"n2-standard-2"},
			},
			expectedErr: NewMachineTypesUnsupportedByFamilyError([]string{"n2-standard-2"}, `machine family "m1"`),
		},
		"error if machine family constraints result in no compatible machine types": {
			spec: machinetypes.MachineSpec{
				Families:       []machinetypes.MachineFamily{constraintsTestFamily},
				MinCpuPlatform: machinetypes.IntelCascadeLake,
				GpuType:        "gpu-type-1",
			},
			expectedErr: NewMachineConfigInvalidError(machinetypes.Constraints{GpuType: "gpu-type-1", CpuPlatform: machinetypes.IntelCascadeLake}.String(), "no machine types supporting all parts of the config found"),
		},
	} {
		t.Run(tn, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithConfidentialNodesEnabled(tc.confidentialNodes).WithConfidentialInstanceType(tc.confidentialInstanceType).Build()
			selector := Selector{CloudProvider: provider}
			gotSpec, gotErr := selector.limitMachineSpec(tc.spec, tc.architectures)
			assert.Equal(t, tc.expectedSpec, gotSpec)
			assert.Equal(t, tc.expectedErr, gotErr)
		})
	}
}
