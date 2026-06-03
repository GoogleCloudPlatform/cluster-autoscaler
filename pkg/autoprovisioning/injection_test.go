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

package autoprovisioning

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	netapi "github.com/GoogleCloudPlatform/gke-networking-api/apis/network/v1"
	"github.com/gogo/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	gce_api "google.golang.org/api/compute/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	base_backoff "k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	caerrors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/utils/ptr"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/machineselection"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/preemption"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/sandbox"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	computeclass "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	computeclass_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
)

func TestNodeGroupParameters(t *testing.T) {
	spotTrue := true
	computeClassLabel := "cc-label"
	computeClassName := "cc-name"
	validLocalSSDcount := 1
	invalidLocalSSDCount := 4
	ccMPPN := 10
	rule1 := rules.NewRule(rules.WithMachineFamilyRule(ptr.To("n2")))
	rule2 := rules.NewRule(rules.WithMachineFamilyRule(ptr.To("e2")))

	for tn, tc := range map[string]struct {
		nodeAutoprovisioningEnabled    bool
		requirements                   nodeGroupRequirements
		options                        NodeGroupOptions
		provisioningLabelEnabled       bool
		tpuAutoprovisioningEnabled     bool
		expectedParams                 nodeGroupParameters
		expectErr                      error
		disableComputeClassMinCapacity bool
	}{
		"zone and machine type are passed correctly": {
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain: "ss-moon-1",
				},
			},
		},
		"preemption is passed correctly": {
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
				Preemption:  preemption.Spot,
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain: "ss-moon-1",
					gkelabels.SpotLabel:          gkelabels.PreemptionValue,
				},
				taints: []apiv1.Taint{{Key: gkelabels.SpotLabel, Value: gkelabels.PreemptionValue, Effect: apiv1.TaintEffectNoSchedule}},
			},
		},
		"workload separation is passed correctly": {
			requirements: nodeGroupRequirements{
				workloadSeparationLabels: map[string]string{"k1": "v1", "k2": "v2"},
				workloadSeparationTaints: []apiv1.Taint{
					{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
					{Key: "k2", Value: "v2", Effect: apiv1.TaintEffectNoSchedule},
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain: "ss-moon-1",
				},
				labels: map[string]string{"k1": "v1", "k2": "v2"},
				taints: []apiv1.Taint{
					{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
					{Key: "k2", Value: "v2", Effect: apiv1.TaintEffectNoSchedule},
				},
			},
		},
		"workload separation empty taints are filled up correctly": {
			requirements: nodeGroupRequirements{
				workloadSeparationLabels: map[string]string{"k1": "v1", "k2": "v2"},
				workloadSeparationTaints: []apiv1.Taint{
					{Key: "k1", Value: "v1"},
					{Key: "k2", Value: "v2"},
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain: "ss-moon-1",
				},
				labels: map[string]string{"k1": "v1", "k2": "v2"},
				taints: []apiv1.Taint{
					{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
					{Key: "k2", Value: "v2", Effect: apiv1.TaintEffectNoSchedule},
				},
			},
		},
		"GPU request is passed correctly - standard": {
			requirements: nodeGroupRequirements{
				gpuRequest: machinetypes.GpuRequest{
					Config:           machinetypes.GpuConfig{GpuType: "nvidia-crypto-abc"},
					Count:            13,
					PhysicalGPUCount: 13,
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:       "ss-moon-1",
					gkelabels.GPULabel:                 "nvidia-crypto-abc",
					gkelabels.GPUPartitionSizeLabel:    "",
					gkelabels.GPUSharingStrategyLabel:  "",
					gkelabels.GPUMaxSharedClientsLabel: "",
					gkelabels.GPUDriverVersionLabel:    "",
				},
				extraResources: map[string]resource.Quantity{
					gpu.ResourceNvidiaGPU: *resource.NewQuantity(13, resource.DecimalSI),
				},
			},
		},
		"GPU request with partitioning is passed correctly - standard": {
			requirements: nodeGroupRequirements{
				gpuRequest: machinetypes.GpuRequest{
					Config:           machinetypes.GpuConfig{GpuType: "nvidia-crypto-abc", PartitionSize: "part-size"},
					Count:            13,
					PhysicalGPUCount: 13,
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:       "ss-moon-1",
					gkelabels.GPULabel:                 "nvidia-crypto-abc",
					gkelabels.GPUPartitionSizeLabel:    "part-size",
					gkelabels.GPUSharingStrategyLabel:  "",
					gkelabels.GPUDriverVersionLabel:    "",
					gkelabels.GPUMaxSharedClientsLabel: "",
				},
				extraResources: map[string]resource.Quantity{
					gpu.ResourceNvidiaGPU: *resource.NewQuantity(13, resource.DecimalSI),
				},
			},
		},
		"GPU request with driver version is passed correctly - standard": {
			requirements: nodeGroupRequirements{
				gpuRequest: machinetypes.GpuRequest{
					Config:           machinetypes.GpuConfig{GpuType: "nvidia-crypto-abc", DriverVersion: "version-x"},
					Count:            13,
					PhysicalGPUCount: 13,
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:       "ss-moon-1",
					gkelabels.GPULabel:                 "nvidia-crypto-abc",
					gkelabels.GPUDriverVersionLabel:    "version-x",
					gkelabels.GPUPartitionSizeLabel:    "",
					gkelabels.GPUSharingStrategyLabel:  "",
					gkelabels.GPUMaxSharedClientsLabel: "",
				},
				extraResources: map[string]resource.Quantity{
					gpu.ResourceNvidiaGPU: *resource.NewQuantity(13, resource.DecimalSI),
				},
			},
		},
		"GPU request with driver version is passed correctly - autopilot": {
			nodeAutoprovisioningEnabled: true,
			requirements: nodeGroupRequirements{
				gpuRequest: machinetypes.GpuRequest{
					Config:           machinetypes.GpuConfig{GpuType: "nvidia-crypto-abc", DriverVersion: "version-x"},
					Count:            13,
					PhysicalGPUCount: 13,
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:       "ss-moon-1",
					gkelabels.GPULabel:                 "nvidia-crypto-abc",
					gkelabels.GPUDriverVersionLabel:    "version-x",
					gkelabels.GPUPartitionSizeLabel:    "",
					gkelabels.GPUSharingStrategyLabel:  "",
					gkelabels.GPUMaxSharedClientsLabel: "",
				},
				extraResources: map[string]resource.Quantity{
					gpu.ResourceNvidiaGPU: *resource.NewQuantity(13, resource.DecimalSI),
				},
			},
		},
		"GPU request without driver version defaults to 'default' driver  - autopilot": {
			nodeAutoprovisioningEnabled: true,
			requirements: nodeGroupRequirements{
				gpuRequest: machinetypes.GpuRequest{
					Config:           machinetypes.GpuConfig{GpuType: "nvidia-crypto-abc"},
					Count:            13,
					PhysicalGPUCount: 13,
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:       "ss-moon-1",
					gkelabels.GPULabel:                 "nvidia-crypto-abc",
					gkelabels.GPUDriverVersionLabel:    gkelabels.DefaultGPUDriverVersionValue,
					gkelabels.GPUPartitionSizeLabel:    "",
					gkelabels.GPUSharingStrategyLabel:  "",
					gkelabels.GPUMaxSharedClientsLabel: "",
				},
				extraResources: map[string]resource.Quantity{
					gpu.ResourceNvidiaGPU: *resource.NewQuantity(13, resource.DecimalSI),
				},
			},
		},
		"TPU Device request is passed correctly": {
			tpuAutoprovisioningEnabled: true,
			requirements: nodeGroupRequirements{
				tpuRequest: TpuRequest{
					TpuType:      gkelabels.TpuV4LiteDeviceValue,
					ChipsPerNode: 4,
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "ct4l-hightpu-4t",
			},
			expectedParams: nodeGroupParameters{
				machineType: "ct4l-hightpu-4t",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:    "ss-moon-1",
					gkelabels.TPULabel:              gkelabels.TpuV4LiteDeviceValue,
					gkelabels.AcceleratorCountLabel: "4",
				},
				extraResources: map[string]resource.Quantity{
					tpu.ResourceGoogleTPU: *resource.NewQuantity(4, resource.DecimalSI),
				},
			},
		},
		"TPU Podslice request is passed correctly": {
			tpuAutoprovisioningEnabled: true,
			requirements: nodeGroupRequirements{
				tpuRequest: TpuRequest{
					TpuType:      gkelabels.TpuV4PodsliceValue,
					ChipsPerNode: 4,
					Topology:     "2x2x2",
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "ct4p-hightpu-4t",
			},
			expectedParams: nodeGroupParameters{
				machineType: "ct4p-hightpu-4t",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:    "ss-moon-1",
					gkelabels.TPULabel:              gkelabels.TpuV4PodsliceValue,
					gkelabels.TPUTopologyLabel:      "2x2x2",
					gkelabels.AcceleratorCountLabel: "4",
				},
				extraResources: map[string]resource.Quantity{
					tpu.ResourceGoogleTPU: *resource.NewQuantity(4, resource.DecimalSI),
				},
			},
		},
		"AnyPlatform min_cpu_platform is not passed": {
			requirements: nodeGroupRequirements{
				machineSpec: machinetypes.MachineSpec{MinCpuPlatform: machinetypes.AnyPlatform},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain: "ss-moon-1",
				},
			},
		},
		"concrete min_cpu_platform is passed correctly - underscores instead of spaces": {
			requirements: nodeGroupRequirements{
				machineSpec: machinetypes.MachineSpec{MinCpuPlatform: machinetypes.AmdRome},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:           "ss-moon-1",
					gkelabels.RequestedMinCpuPlatformLabel: "AMD_Rome",
				},
			},
		},
		"compute class is passed correctly": {
			requirements: nodeGroupRequirements{
				machineSpec: machinetypes.MachineSpec{
					ComputeClassName: "test-class",
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain: "ss-moon-1",
					gkelabels.ComputeClassLabel:  "test-class",
				},
			},
		},
		"compact placement is passed correctly": {
			requirements: nodeGroupRequirements{
				placementGroup: placement.Spec{GroupId: "placement-id"},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:  "ss-moon-1",
					gkelabels.PlacementGroupLabel: "placement-id",
				},
			},
		},
		"compact placement is passed correctly with policy label": {
			requirements: nodeGroupRequirements{
				placementGroup: placement.Spec{GroupId: "placement-id", Policy: "policy-name"},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:  "ss-moon-1",
					gkelabels.PlacementGroupLabel: "placement-id",
					gkelabels.PolicyLabel:         "policy-name",
				},
			},
		},
		"CC placement policy is passed correctly with policy rule": {
			requirements: nodeGroupRequirements{
				computeClassRule: rules.NewRule(
					rules.WithPlacementPolicyRule("policy-name"),
				),
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain: "ss-moon-1",
				},
			},
		},
		"Standard provisioning is passed correctly": {
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			provisioningLabelEnabled: true,
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain: "ss-moon-1",
					gkelabels.ProvisioningLabel:  gkelabels.StandardProvisioningValue,
				},
			},
		},
		"Spot provisioning is passed correctly": {
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
				Preemption:  preemption.Spot,
			},
			provisioningLabelEnabled: true,
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain: "ss-moon-1",
					gkelabels.SpotLabel:          gkelabels.PreemptionValue,
					gkelabels.ProvisioningLabel:  gkelabels.SpotProvisioningValue,
				},
				taints: []apiv1.Taint{{Key: gkelabels.SpotLabel, Value: gkelabels.PreemptionValue, Effect: apiv1.TaintEffectNoSchedule}},
			},
		},
		"Preemptible provisioning is passed correctly": {
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
				Preemption:  preemption.LegacyPreemptible,
			},
			provisioningLabelEnabled: true,
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain: "ss-moon-1",
					gkelabels.PreemptibleLabel:   gkelabels.PreemptionValue,
					gkelabels.ProvisioningLabel:  gkelabels.PreemptibleProvisioningValue,
				},
				taints: []apiv1.Taint{{Key: gkelabels.PreemptibleLabel, Value: gkelabels.PreemptionValue, Effect: apiv1.TaintEffectNoSchedule}},
			},
		},
		"Spot provisioning from compute class rule is passed correctly": {
			requirements: nodeGroupRequirements{
				computeClassRule: rules.NewMachineSpecRule(nil, &spotTrue, nil, nil),
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
				Preemption:  preemption.Spot,
			},
			provisioningLabelEnabled: true,
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain: "ss-moon-1",
					gkelabels.SpotLabel:          gkelabels.PreemptionValue,
					gkelabels.ProvisioningLabel:  gkelabels.SpotProvisioningValue,
				},
				taints: []apiv1.Taint{},
			},
		},
		"Max pods per node from compute class rule is passed correctly": {
			requirements: nodeGroupRequirements{
				maxPodsPerNode: ccMPPN,
			},
			options: NodeGroupOptions{
				Zone:           "ss-moon-1",
				MachineType:    "n2-standard-4",
				MaxPodsPerNode: ccMPPN,
			},
			provisioningLabelEnabled: true,
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:  "ss-moon-1",
					gkelabels.ProvisioningLabel:   gkelabels.StandardProvisioningValue,
					gkelabels.MaxPodsPerNodeLabel: strconv.Itoa(ccMPPN),
				},
				taints: []apiv1.Taint{},
			},
		},
		"gvisor sandbox type is passed correctly": {
			provisioningLabelEnabled: true,
			requirements: nodeGroupRequirements{
				sandboxType: sandbox.GVisor,
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain: "ss-moon-1",
					sandbox.GVisorLabelKey:       sandbox.GVisorLabelValue,
					gkelabels.ProvisioningLabel:  gkelabels.StandardProvisioningValue,
				},
			},
		},
		"Reservation specified in requirements": {
			requirements: nodeGroupRequirements{
				reservation: reservationRequirements{
					exists:   true,
					name:     "res-name",
					project:  "res-proj",
					affinity: "specific",
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:       "ss-moon-1",
					gkelabels.ReservationNameLabel:     "res-name",
					gkelabels.ReservationProjectLabel:  "res-proj",
					gkelabels.ReservationAffinityLabel: "specific",
				},
			},
		},
		"Compute class name is passed correctly": {
			options: NodeGroupOptions{
				MachineType: "e2-medium-2",
				Zone:        "ss-moon-1",
			},
			requirements: nodeGroupRequirements{computeClass: computeclass.NewTestCrd(computeclass.WithName(computeClassName), computeclass.WithScaleUpAnyway(), computeclass.WithAutoprovisioningEnabled(), computeclass.WithLabel(computeClassLabel))},
			expectedParams: nodeGroupParameters{
				machineType: "e2-medium-2",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:           "ss-moon-1",
					computeClassLabel:                      computeClassName,
					labelComputeClassRequired:              "true",
					gkelabels.ComputeClassPriorityIdxLabel: "-1",
				},
			},
		},
		"Reservation Exists so parameters are filled in": {
			requirements: nodeGroupRequirements{
				reservation: reservationRequirements{
					exists:  true,
					name:    "res-name",
					project: "res-proj",
					block:   "res-block",
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:          "ss-moon-1",
					gkelabels.ReservationNameLabel:        "res-name",
					gkelabels.ReservationProjectLabel:     "res-proj",
					gkelabels.ReservationBlocksLabel:      "res-block",
					gkelabels.ReservationBlocksCountLabel: "0",
				},
			},
		},
		"Reservation Missing so parameters not filled in": {
			requirements: nodeGroupRequirements{
				reservation: reservationRequirements{
					exists:   false,
					name:     "res-name",
					project:  "res-proj",
					block:    "res-block",
					subBlock: "res-subblock",
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain: "ss-moon-1",
				},
			},
		},
		"Reservation exists with sub-block so parameters are filled in": {
			requirements: nodeGroupRequirements{
				reservation: reservationRequirements{
					exists:        true,
					name:          "res-name",
					project:       "res-proj",
					block:         "res-block",
					subBlock:      "res-subblock",
					subBlockCount: 5,
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:             "ss-moon-1",
					gkelabels.ReservationNameLabel:           "res-name",
					gkelabels.ReservationProjectLabel:        "res-proj",
					gkelabels.ReservationBlocksLabel:         "res-block",
					gkelabels.ReservationSubBlocksLabel:      "res-subblock",
					gkelabels.ReservationBlocksCountLabel:    "0",
					gkelabels.ReservationSubBlocksCountLabel: "5",
				},
			},
		},
		"System labels filled in": {
			requirements: nodeGroupRequirements{
				systemLabels: map[string]string{
					"cloud.google.com/my-feature": "my-value",
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				labels: map[string]string{
					"cloud.google.com/my-feature": "my-value",
				},
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain: "ss-moon-1",
				},
			},
		},
		"localSSD count is passed correctly for automatically attached disk machine type": {
			requirements: nodeGroupRequirements{
				explicitlyRequiresLocalSSD: false,
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "a2-ultragpu-2g",
			},
			expectedParams: nodeGroupParameters{
				machineType: "a2-ultragpu-2g",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:     "ss-moon-1",
					labelEphemeralLocalSsdDisksCount: "2",
				},
			},
		},
		"localSSD count is passed correctly for supported machine type": {
			requirements: nodeGroupRequirements{
				explicitlyRequiresLocalSSD: true,
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "g2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "g2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:     "ss-moon-1",
					gkelabels.EphemeralLocalSsdLabel: gkelabels.EphemeralLocalSsdEnabledValue,
					labelEphemeralLocalSsdDisksCount: "1",
				},
			},
		},
		"localSSD count is not passed for unsupported machine type": {
			requirements: nodeGroupRequirements{
				explicitlyRequiresLocalSSD: true,
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "c3-standard-4",
			},
			expectErr: NewLocalSSDNotSupportedForMachineTypeError("c3-standard-4"),
		},
		"localSSD count is not passed for unsupported custom machine type": {
			requirements: nodeGroupRequirements{
				explicitlyRequiresLocalSSD: true,
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2d-custom-4-32768",
			},
			expectErr: NewLocalSSDNotSupportedForMachineTypeError("n2d-custom-4-32768"),
		},
		"valid local ssd count for supported machine type with Compute Class enabled": {
			requirements: nodeGroupRequirements{
				ephemeralStorageLocalSSDCount: validLocalSSDcount,
				totalLSSDCount:                validLocalSSDcount,
			},
			options: NodeGroupOptions{
				MachineType: "g2-standard-4",
				Zone:        "ss-moon-1",
			},
			expectedParams: nodeGroupParameters{
				machineType: "g2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:     "ss-moon-1",
					labelEphemeralLocalSsdDisksCount: fmt.Sprintf("%d", validLocalSSDcount),
				},
			},
		},
		"invalid local ssd count for supported machine type with Compute Class enabled": {
			requirements: nodeGroupRequirements{
				ephemeralStorageLocalSSDCount: invalidLocalSSDCount,
				totalLSSDCount:                invalidLocalSSDCount,
			},
			options: NodeGroupOptions{
				MachineType: "g2-standard-4",
				Zone:        "ss-moon-1",
			},
			expectErr: NewInvalidLocalSSDCountForMachineTypeError("g2-standard-4", invalidLocalSSDCount, []int{1}),
		},
		"localSSD count is not passed for unsupported machine type with Compute Class enabled": {
			requirements: nodeGroupRequirements{
				totalLSSDCount: invalidLocalSSDCount,
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "c3-standard-4",
			},
			expectErr: NewLocalSSDNotSupportedForMachineTypeError("c3-standard-4"),
		},
		"Boot disk type and size passed correctly - Compute Class enabled": {
			requirements: nodeGroupRequirements{
				bootDiskType: "hello-world",
				bootDiskSize: 100,
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:   "ss-moon-1",
					gkelabels.BootDiskTypeLabelKey: "hello-world",
					gkelabels.BootDiskSizeLabelKey: "100",
				},
			},
		},
		"everything is passed correctly together": {
			provisioningLabelEnabled: true,
			requirements: nodeGroupRequirements{
				computeClass: computeclass.NewTestCrd(computeclass.WithName(computeClassName), computeclass.WithScaleUpAnyway(), computeclass.WithAutoprovisioningEnabled(), computeclass.WithLabel(computeClassLabel)),
				systemLabels: map[string]string{
					"cloud.google.com/my-feature": "my-value",
				},
				machineSpec: machinetypes.MachineSpec{
					Families:         []machinetypes.MachineFamily{machinetypes.N2, machinetypes.N2D},
					MinCpuPlatform:   machinetypes.AmdRome,
					ComputeClassName: "test-class",
				},
				gpuRequest: machinetypes.GpuRequest{
					Config:           machinetypes.GpuConfig{GpuType: "nvidia-crypto-abc", PartitionSize: "part-size", SharingStrategy: "time-sharing", MaxSharedClients: "10", DriverVersion: "version-x"},
					Count:            13,
					PhysicalGPUCount: 13,
				},
				workloadSeparationLabels: map[string]string{"k1": "v1", "k2": "v2"},
				workloadSeparationTaints: []apiv1.Taint{
					{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
					{Key: "k2", Value: "v2", Effect: apiv1.TaintEffectNoSchedule},
				},
				placementGroup: placement.Spec{GroupId: "placement-id"},
				sandboxType:    sandbox.GVisor,
				reservation: reservationRequirements{
					exists:   true,
					name:     "res-name",
					project:  "res-proj",
					affinity: "specific",
					block:    "res-block",
					subBlock: "res-subblock",
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
				Preemption:  preemption.Spot,
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				labels:      map[string]string{"k1": "v1", "k2": "v2", "cloud.google.com/my-feature": "my-value"},
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:             "ss-moon-1",
					gkelabels.RequestedMinCpuPlatformLabel:   "AMD_Rome",
					gkelabels.SpotLabel:                      gkelabels.PreemptionValue,
					gkelabels.GPULabel:                       "nvidia-crypto-abc",
					gkelabels.GPUPartitionSizeLabel:          "part-size",
					gkelabels.GPUSharingStrategyLabel:        "time-sharing",
					gkelabels.GPUMaxSharedClientsLabel:       "10",
					gkelabels.GPUDriverVersionLabel:          "version-x",
					gkelabels.ComputeClassLabel:              "test-class",
					gkelabels.PlacementGroupLabel:            "placement-id",
					gkelabels.ReservationNameLabel:           "res-name",
					gkelabels.ReservationProjectLabel:        "res-proj",
					gkelabels.ReservationAffinityLabel:       "specific",
					gkelabels.ReservationBlocksLabel:         "res-block",
					gkelabels.ReservationSubBlocksLabel:      "res-subblock",
					gkelabels.ReservationBlocksCountLabel:    "0",
					gkelabels.ReservationSubBlocksCountLabel: "0",
					sandbox.GVisorLabelKey:                   sandbox.GVisorLabelValue,
					gkelabels.ProvisioningLabel:              gkelabels.SpotProvisioningValue,
					computeClassLabel:                        computeClassName,
					labelComputeClassRequired:                "true",
					gkelabels.ComputeClassPriorityIdxLabel:   "-1",
				},
				extraResources: map[string]resource.Quantity{
					gpu.ResourceNvidiaGPU: *resource.NewQuantity(13, resource.DecimalSI),
				},
				taints: []apiv1.Taint{
					{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
					{Key: "k2", Value: "v2", Effect: apiv1.TaintEffectNoSchedule},
					{Key: gkelabels.SpotLabel, Value: gkelabels.PreemptionValue, Effect: apiv1.TaintEffectNoSchedule},
				},
			},
		},
		"everything is passed correctly together with cc": {
			provisioningLabelEnabled: true,
			requirements: nodeGroupRequirements{
				maxPodsPerNode: 32,
				computeClass: computeclass.NewTestCrd(
					computeclass.WithName("cc"),
					computeclass.WithScaleUpAnyway(),
					computeclass.WithAutoprovisioningEnabled(),
					computeclass.WithLabel(gkelabels.ComputeClassLabel),
					computeclass.WithUserDefinedTaints([]apiv1.Taint{
						{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
					}),
				),
				systemLabels: map[string]string{
					"cloud.google.com/my-feature": "my-value",
				},
				machineSpec: machinetypes.MachineSpec{
					Families:         []machinetypes.MachineFamily{machinetypes.N2, machinetypes.N2D},
					MinCpuPlatform:   machinetypes.AmdRome,
					ComputeClassName: "cc",
				},
				gpuRequest: machinetypes.GpuRequest{
					Config:           machinetypes.GpuConfig{GpuType: "nvidia-crypto-abc", PartitionSize: "part-size", SharingStrategy: "time-sharing", MaxSharedClients: "10", DriverVersion: "version-x"},
					Count:            13,
					PhysicalGPUCount: 13,
				},
				workloadSeparationLabels: map[string]string{"k1": "v1", "k2": "v2"},
				workloadSeparationTaints: []apiv1.Taint{
					{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
					{Key: "k2", Value: "v2", Effect: apiv1.TaintEffectNoSchedule},
				},
				placementGroup: placement.Spec{GroupId: "placement-id"},
				sandboxType:    sandbox.GVisor,
				reservation: reservationRequirements{
					exists:   true,
					name:     "res-name",
					project:  "res-proj",
					affinity: "specific",
				},
			},
			options: NodeGroupOptions{
				Zone:           "ss-moon-1",
				MachineType:    "n2-standard-4",
				Preemption:     preemption.Spot,
				MaxPodsPerNode: 32,
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				labels:      map[string]string{"k1": "v1", "k2": "v2", "cloud.google.com/my-feature": "my-value"},
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:           "ss-moon-1",
					gkelabels.RequestedMinCpuPlatformLabel: "AMD_Rome",
					gkelabels.SpotLabel:                    gkelabels.PreemptionValue,
					gkelabels.GPULabel:                     "nvidia-crypto-abc",
					gkelabels.GPUPartitionSizeLabel:        "part-size",
					gkelabels.GPUSharingStrategyLabel:      "time-sharing",
					gkelabels.GPUMaxSharedClientsLabel:     "10",
					gkelabels.GPUDriverVersionLabel:        "version-x",
					gkelabels.PlacementGroupLabel:          "placement-id",
					gkelabels.ReservationNameLabel:         "res-name",
					gkelabels.ReservationProjectLabel:      "res-proj",
					gkelabels.ReservationAffinityLabel:     "specific",
					sandbox.GVisorLabelKey:                 sandbox.GVisorLabelValue,
					gkelabels.ProvisioningLabel:            gkelabels.SpotProvisioningValue,
					gkelabels.ComputeClassLabel:            "cc",
					gkelabels.MaxPodsPerNodeLabel:          "32",
					labelComputeClassRequired:              "true",
					gkelabels.ComputeClassPriorityIdxLabel: "-1",
				},
				extraResources: map[string]resource.Quantity{
					gpu.ResourceNvidiaGPU: *resource.NewQuantity(13, resource.DecimalSI),
				},
				taints: []apiv1.Taint{
					{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
					{Key: "k2", Value: "v2", Effect: apiv1.TaintEffectNoSchedule},
					{Key: gkelabels.SpotLabel, Value: gkelabels.PreemptionValue, Effect: apiv1.TaintEffectNoSchedule},
				},
			},
		},
		"service account is put to the parameters": {
			requirements: nodeGroupRequirements{
				computeClass: computeclass.NewTestCrd(
					computeclass.WithName(computeClassName),
					computeclass.WithScaleUpAnyway(),
					computeclass.WithAutoprovisioningEnabled(),
					computeclass.WithLabel(computeClassLabel),
					computeclass.WithServiceAccount("test@12345.iam.gserviceaccount.com")),
				machineSpec: machinetypes.MachineSpec{
					Families:         []machinetypes.MachineFamily{machinetypes.N2, machinetypes.N2D},
					ComputeClassName: "cc",
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					gkelabels.ComputeClassLabel:            "cc",
					apiv1.LabelZoneFailureDomain:           "ss-moon-1",
					gkelabels.ServiceAccountLabelKey:       "test@12345.iam.gserviceaccount.com",
					labelComputeClassRequired:              "true",
					computeClassLabel:                      computeClassName,
					gkelabels.ComputeClassPriorityIdxLabel: "-1",
				},
			},
		},
		"image type is passed correctly to the parameters": {
			requirements: nodeGroupRequirements{
				computeClass: computeclass.NewTestCrd(
					computeclass.WithName(computeClassName),
					computeclass.WithLabel(computeClassLabel),
					computeclass.WithScaleUpAnyway(),
					computeclass.WithAutoprovisioningEnabled(),
					computeclass.WithImageType("ubuntu_containerd")),
				machineSpec: machinetypes.MachineSpec{
					Families:         []machinetypes.MachineFamily{machinetypes.N2, machinetypes.N2D},
					ComputeClassName: "cc",
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					gkelabels.ComputeClassLabel:            "cc",
					apiv1.LabelZoneFailureDomain:           "ss-moon-1",
					gkelabels.ImageTypeLabelKey:            "ubuntu_containerd",
					labelComputeClassRequired:              "true",
					computeClassLabel:                      computeClassName,
					gkelabels.ComputeClassPriorityIdxLabel: "-1",
				},
			},
		},
		"node version is passed correctly to the parameters": {
			requirements: nodeGroupRequirements{
				nodeVersion: "1.32.9-gke.1726000",
				computeClass: computeclass.NewTestCrd(
					computeclass.WithName(computeClassName),
					computeclass.WithLabel(computeClassLabel),
					computeclass.WithScaleUpAnyway(),
					computeclass.WithAutoprovisioningEnabled(),
					computeclass.WithNodeVersion("1.32.9-gke.1726000")),
				machineSpec: machinetypes.MachineSpec{
					Families:         []machinetypes.MachineFamily{machinetypes.N2, machinetypes.N2D},
					ComputeClassName: "cc",
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					gkelabels.ComputeClassLabel:            "cc",
					apiv1.LabelZoneFailureDomain:           "ss-moon-1",
					gkelabels.NodeVersionLabelKey:          "1.32.9-gke.1726000",
					labelComputeClassRequired:              "true",
					computeClassLabel:                      computeClassName,
					gkelabels.ComputeClassPriorityIdxLabel: "-1",
				},
			},
		},
		"dynamic pods per node through CC is passed correctly to the parameters": {
			requirements: nodeGroupRequirements{
				computeClass: computeclass.NewTestCrd(
					computeclass.WithName(computeClassName),
					computeclass.WithLabel(computeClassLabel),
					computeclass.WithScaleUpAnyway(),
					computeclass.WithAutoprovisioningEnabled(),
					computeclass.WithAutopilotManaged(),
					computeclass.WithDynamicMaxPodsPerNodeEnabled()),
				machineSpec: machinetypes.MachineSpec{
					Families:         []machinetypes.MachineFamily{machinetypes.N2, machinetypes.N2D},
					ComputeClassName: "cc",
				},
			},
			options: NodeGroupOptions{
				Zone:                  "ss-moon-1",
				MachineType:           "n2-standard-4",
				DynamicMaxPodsPerNode: true,
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					gkelabels.ComputeClassLabel:                             "cc",
					apiv1.LabelZoneFailureDomain:                            "ss-moon-1",
					gkelabels.ManagedNodeLabel:                              "true",
					gkelabels.NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey: "true",
					gkelabels.PodsPerNodeKey:                                "any",
					labelComputeClassRequired:                               "true",
					computeClassLabel:                                       computeClassName,
					gkelabels.ComputeClassPriorityIdxLabel:                  "-1",
				},
			},
		},
		"dynamic pods per node through CC is not passed if rule specifies static value": {
			requirements: nodeGroupRequirements{
				computeClass: computeclass.NewTestCrd(
					computeclass.WithName(computeClassName),
					computeclass.WithLabel(computeClassLabel),
					computeclass.WithScaleUpAnyway(),
					computeclass.WithAutoprovisioningEnabled(),
					computeclass.WithAutopilotManaged(),
					computeclass.WithDynamicMaxPodsPerNodeEnabled()),
				machineSpec: machinetypes.MachineSpec{
					Families:         []machinetypes.MachineFamily{machinetypes.N2, machinetypes.N2D},
					ComputeClassName: "cc",
				},
			},
			options: NodeGroupOptions{
				Zone:           "ss-moon-1",
				MachineType:    "n2-standard-4",
				MaxPodsPerNode: 32,
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					gkelabels.ComputeClassLabel:            "cc",
					apiv1.LabelZoneFailureDomain:           "ss-moon-1",
					gkelabels.ManagedNodeLabel:             "true",
					gkelabels.PodsPerNodeKey:               "any",
					gkelabels.MaxPodsPerNodeLabel:          "32",
					labelComputeClassRequired:              "true",
					computeClassLabel:                      computeClassName,
					gkelabels.ComputeClassPriorityIdxLabel: "-1",
				},
			},
		},
		"CRD user defined taints are passed correctly": {
			requirements: nodeGroupRequirements{
				computeClass: computeclass.NewTestCrd(
					computeclass.WithName(computeClassName),
					computeclass.WithLabel(computeClassLabel),
					computeclass.WithScaleUpAnyway(),
					computeclass.WithAutoprovisioningEnabled(),
					computeclass.WithUserDefinedTaints(
						[]apiv1.Taint{
							{Key: "k1", Value: "v1", Effect: "NoSchedule"},
							{Key: "k2", Value: "v2"},
							{Key: "k3"},
						},
					),
				),
				machineSpec: machinetypes.MachineSpec{
					Families:         []machinetypes.MachineFamily{machinetypes.N2, machinetypes.N2D},
					ComputeClassName: "cc",
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					gkelabels.ComputeClassLabel:            "cc",
					apiv1.LabelZoneFailureDomain:           "ss-moon-1",
					labelComputeClassRequired:              "true",
					computeClassLabel:                      computeClassName,
					gkelabels.ComputeClassPriorityIdxLabel: "-1",
				},
				taints: []apiv1.Taint{
					{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
					{Key: "k2", Value: "v2", Effect: apiv1.TaintEffectNoSchedule},
					{Key: "k3", Effect: apiv1.TaintEffectNoSchedule},
				},
			},
		},

		"Swap enabled on dedicated local ssd profile with ephemeral local ssd on gen1 LSSD machines": {
			requirements: nodeGroupRequirements{
				ephemeralStorageLocalSSDCount: 1,
				totalLSSDCount:                2,
				linuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						DedicatedLocalSsdProfile: &gkeclient.DedicatedLocalSsdProfile{
							DiskCount: 1,
						},
					},
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n1-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n1-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:     "ss-moon-1",
					labelLinuxNodeConfig:             `{"swapConfig":{"dedicatedLocalSsdProfile":{"diskCount":"1"},"enabled":true}}`,
					labelEphemeralLocalSsdDisksCount: "1",
				},
			},
		},
		"Swap enabled on dedicated local ssd profile without ephemeral local ssd on gen1 LSSD machines": {
			requirements: nodeGroupRequirements{
				totalLSSDCount: 1,
				linuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						DedicatedLocalSsdProfile: &gkeclient.DedicatedLocalSsdProfile{
							DiskCount: 1,
						},
					},
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n1-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n1-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain: "ss-moon-1",
					labelLinuxNodeConfig:         `{"swapConfig":{"dedicatedLocalSsdProfile":{"diskCount":"1"},"enabled":true}}`,
				},
			},
		},
		"invalid swap dedicated local ssd count": {
			requirements: nodeGroupRequirements{
				totalLSSDCount: invalidLocalSSDCount,
				linuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						DedicatedLocalSsdProfile: &gkeclient.DedicatedLocalSsdProfile{
							DiskCount: int64(invalidLocalSSDCount),
						},
					},
				},
			},
			options: NodeGroupOptions{
				MachineType: "g2-standard-4",
				Zone:        "ss-moon-1",
			},
			expectErr: NewInvalidLocalSSDCountForMachineTypeError("g2-standard-4", invalidLocalSSDCount, []int{1}),
		},
		"Swap enabled on dedicated local ssd profile with explicit local ssd": {
			requirements: nodeGroupRequirements{
				explicitlyRequiresLocalSSD: true,
				linuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						DedicatedLocalSsdProfile: &gkeclient.DedicatedLocalSsdProfile{
							DiskCount: 1,
						},
					},
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "g2-standard-48",
			},
			expectedParams: nodeGroupParameters{
				machineType: "g2-standard-48",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:     "ss-moon-1",
					labelLinuxNodeConfig:             `{"swapConfig":{"dedicatedLocalSsdProfile":{"diskCount":"1"},"enabled":true}}`,
					labelEphemeralLocalSsdDisksCount: "3",
					gkelabels.EphemeralLocalSsdLabel: "true",
				},
			},
		},
		"invalid swap dedicated local ssd profile with explicit local ssd": {
			requirements: nodeGroupRequirements{
				explicitlyRequiresLocalSSD: true,
				totalLSSDCount:             1,
				linuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						DedicatedLocalSsdProfile: &gkeclient.DedicatedLocalSsdProfile{
							DiskCount: int64(1),
						},
					},
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "g2-standard-4",
			},
			expectErr: NewInvalidLocalSSDCountForMachineTypeError("g2-standard-4", 2, []int{1}),
		},
		"Swap enabled on dedicated local ssd profile with ephemeral local ssd on gen1 LSSD reservations": {
			requirements: nodeGroupRequirements{
				totalLSSDCount: 1,
				linuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						DedicatedLocalSsdProfile: &gkeclient.DedicatedLocalSsdProfile{
							DiskCount: 1,
						},
					},
				},
				reservation: reservationRequirements{
					totalLSSDCount: 2,
					machineType:    "n1-standard-4",
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n1-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n1-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:     "ss-moon-1",
					labelLinuxNodeConfig:             `{"swapConfig":{"dedicatedLocalSsdProfile":{"diskCount":"1"},"enabled":true}}`,
					labelEphemeralLocalSsdDisksCount: "1",
				},
			},
		},
		"Swap enabled on dedicated local ssd profile without ephemeral local ssd on gen1 LSSD reservations": {
			requirements: nodeGroupRequirements{
				totalLSSDCount: 1,
				linuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						DedicatedLocalSsdProfile: &gkeclient.DedicatedLocalSsdProfile{
							DiskCount: 1,
						},
					},
				},
				reservation: reservationRequirements{
					totalLSSDCount: 1,
					machineType:    "n1-standard-4",
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n1-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n1-standard-4",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain: "ss-moon-1",
					labelLinuxNodeConfig:         `{"swapConfig":{"dedicatedLocalSsdProfile":{"diskCount":"1"},"enabled":true}}`,
				},
			},
		},
		"invalid swap dedicated local ssd profile on gen1 LSSD reservations": {
			requirements: nodeGroupRequirements{
				totalLSSDCount: invalidLocalSSDCount,
				linuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						DedicatedLocalSsdProfile: &gkeclient.DedicatedLocalSsdProfile{
							DiskCount: int64(invalidLocalSSDCount),
						},
					},
				},
				reservation: reservationRequirements{
					name:           "reservation-1",
					totalLSSDCount: 1,
					machineType:    "n1-standard-4",
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n1-standard-4",
			},
			expectErr: NewInvalidLocalSSDCountForReservationError("reservation-1", 1, invalidLocalSSDCount),
		},
		"Swap enabled on dedicated local ssd with ephemeral local ssd on gen4 LSSD machines": {
			requirements: nodeGroupRequirements{
				explicitlyRequiresLocalSSD: false,
				linuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						DedicatedLocalSsdProfile: &gkeclient.DedicatedLocalSsdProfile{
							DiskCount: 1,
						},
					},
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "c4-standard-16-lssd",
			},
			expectedParams: nodeGroupParameters{
				machineType: "c4-standard-16-lssd",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:     "ss-moon-1",
					labelLinuxNodeConfig:             `{"swapConfig":{"dedicatedLocalSsdProfile":{"diskCount":"1"},"enabled":true}}`,
					labelEphemeralLocalSsdDisksCount: "1",
				},
			},
		},
		"Swap enabled on dedicated local ssd without ephemeral local ssd on gen4 LSSD machines": {
			requirements: nodeGroupRequirements{
				explicitlyRequiresLocalSSD: false,
				linuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						DedicatedLocalSsdProfile: &gkeclient.DedicatedLocalSsdProfile{
							DiskCount: 2,
						},
					},
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "c4-standard-16-lssd",
			},
			expectedParams: nodeGroupParameters{
				machineType: "c4-standard-16-lssd",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain: "ss-moon-1",
					labelLinuxNodeConfig:         `{"swapConfig":{"dedicatedLocalSsdProfile":{"diskCount":"2"},"enabled":true}}`,
				},
			},
		},
		"invalid swap dedicated local ssd count on gen4": {
			requirements: nodeGroupRequirements{
				totalLSSDCount: invalidLocalSSDCount,
				linuxNodeConfig: &gkeclient.LinuxNodeConfig{
					SwapConfig: &gkeclient.SwapConfig{
						Enabled: true,
						DedicatedLocalSsdProfile: &gkeclient.DedicatedLocalSsdProfile{
							DiskCount: int64(invalidLocalSSDCount),
						},
					},
				},
			},
			options: NodeGroupOptions{
				MachineType: "c4-standard-4-lssd",
				Zone:        "ss-moon-1",
			},
			expectErr: NewInvalidLocalSSDCountForMachineTypeError("c4-standard-4-lssd", invalidLocalSSDCount, []int{1}),
		},
		"Priority idx label is set correctly": {
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "e2-medium-2",
			},
			requirements: nodeGroupRequirements{
				computeClass: computeclass.NewTestCrd(
					computeclass.WithName(computeClassName),
					computeclass.WithScaleUpAnyway(),
					computeclass.WithAutoprovisioningEnabled(),
					computeclass.WithLabel(computeClassLabel),
					computeclass.WithRules([]rules.Rule{rule1, rule2}),
				),
				computeClassRule: rule2,
			},
			expectedParams: nodeGroupParameters{
				machineType: "e2-medium-2",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain:           "ss-moon-1",
					computeClassLabel:                      computeClassName,
					labelComputeClassRequired:              "true",
					gkelabels.ComputeClassPriorityIdxLabel: "1",
				},
			},
		},
		"Priority idx label is not set with flag disabled": {
			disableComputeClassMinCapacity: true,
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "e2-medium-2",
			},
			requirements: nodeGroupRequirements{
				computeClass: computeclass.NewTestCrd(
					computeclass.WithName(computeClassName),
					computeclass.WithScaleUpAnyway(),
					computeclass.WithAutoprovisioningEnabled(),
					computeclass.WithLabel(computeClassLabel),
					computeclass.WithRules([]rules.Rule{rule1, rule2}),
				),
				computeClassRule: rule2,
			},
			expectedParams: nodeGroupParameters{
				machineType: "e2-medium-2",
				systemLabels: map[string]string{
					apiv1.LabelZoneFailureDomain: "ss-moon-1",
					computeClassLabel:            computeClassName,
					labelComputeClassRequired:    "true",
				},
			},
		},
		"Architecture taint behavior from CCC is passed to the parameters": {
			requirements: nodeGroupRequirements{
				computeClass: computeclass.NewTestCrd(
					computeclass.WithName(computeClassName),
					computeclass.WithLabel(computeClassLabel),
					computeclass.WithScaleUpAnyway(),
					computeclass.WithAutoprovisioningEnabled(),
					computeclass.WithArchitectureTaintBehavior("NONE")),
				machineSpec: machinetypes.MachineSpec{
					Families:         []machinetypes.MachineFamily{machinetypes.N2, machinetypes.N2D},
					ComputeClassName: "cc",
				},
			},
			options: NodeGroupOptions{
				Zone:        "ss-moon-1",
				MachineType: "n2-standard-4",
			},
			expectedParams: nodeGroupParameters{
				machineType: "n2-standard-4",
				systemLabels: map[string]string{
					gkelabels.ComputeClassLabel:            "cc",
					apiv1.LabelZoneFailureDomain:           "ss-moon-1",
					labelArchitectureTaintBehavior:         "NONE",
					labelComputeClassRequired:              "true",
					computeClassLabel:                      computeClassName,
					gkelabels.ComputeClassPriorityIdxLabel: "-1",
				},
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineTypes("test-machine-type").
				WithAutoprovisioningEnabled(tc.nodeAutoprovisioningEnabled).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			em := experiments.NewMockManager(experiments.ReservationSubblocksTargetingEnabledFlag)
			opts := AutoprovisioningNodeGroupManagerOptions{
				CloudProvider:                  provider,
				AllowlistedSystemLabelsMatcher: buildTestMatcher(t, "cloud.google.com/my-feature"),
				Flags: AutoprovisioningNodeGroupManagerFlags{
					ProvisioningLabelEnabled:   tc.provisioningLabelEnabled,
					TpuAutoprovisioningEnabled: tc.tpuAutoprovisioningEnabled,
					ReservationFlags: ReservationFlags{
						SpecificTypeReservationMatchEnabled: true,
					},
					EnableComputeClassMinCapacity: !tc.disableComputeClassMinCapacity,
				},
				ExperimentsManager: em,
				OptionsTracker:     tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
			}
			m := NewAutoprovisioningNodeGroupManager(opts)
			gotParams, err := m.getNodeGroupParameters(tc.requirements, tc.options)
			if diff := cmp.Diff(tc.expectErr, err); diff != "" {
				t.Errorf("getNodeGroupParameters error diff: %s", diff)
			}
			compareAllUnexportedOpt := cmp.Exporter(func(t reflect.Type) bool { return true })
			if diff := cmp.Diff(tc.expectedParams, gotParams, compareAllUnexportedOpt, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("getNodeGroupParameters params diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSecondaryBootDisksSignature(t *testing.T) {
	for tn, tc := range map[string]struct {
		groupRequirements nodeGroupRequirements
		expected          string
	}{
		"empty group requirements": {
			groupRequirements: nodeGroupRequirements{},
			expected:          ``,
		},
		"1 secondary boot disk": {
			groupRequirements: nodeGroupRequirements{
				secondaryBootDisks: []*gke_api_beta.SecondaryBootDisk{
					{
						DiskImage: "image1",
						Mode:      "mode1",
					},
				},
			},
			expected: `image1`,
		},
		"2 secondary boot disks": {
			groupRequirements: nodeGroupRequirements{
				secondaryBootDisks: []*gke_api_beta.SecondaryBootDisk{
					{
						DiskImage: "image1",
						Mode:      "mode1",
					},
					{
						DiskImage: "image2",
						Mode:      "mode2",
					},
				},
			},
			expected: `image1; image2`,
		},
		"3 secondary boot disks": {
			groupRequirements: nodeGroupRequirements{
				secondaryBootDisks: []*gke_api_beta.SecondaryBootDisk{
					{
						DiskImage: "image1",
						Mode:      "mode1",
					},
					{
						DiskImage: "image2",
						Mode:      "mode2",
					},
					{
						DiskImage: "image3",
						Mode:      "mode3",
					},
				},
			},
			expected: `image1; image2; image3`,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.groupRequirements.secondaryBootDisksSignature())
		})
	}
}

func TestNonGpuPodsRequirements(t *testing.T) {
	nonGpu1 := test.BuildTestPod("non-gpu-1", 1, 128)
	nonGpu2 := test.BuildTestPod("non-gpu-2", 1, 128)
	nonGpu3 := test.BuildTestPod("non-gpu-3", 1, 128)
	k80Gpu1 := buildPodWithGpuCpusAndMem("k80-gpu-1", machinetypes.NvidiaTeslaK80.Name(), 1, 1, 128)
	k80Gpu2 := buildPodWithGpuCpusAndMem("k80-gpu-2", machinetypes.NvidiaTeslaK80.Name(), 1, 1, 128)
	t4Gpu := buildPodWithGpuCpusAndMem("t4-gpu", machinetypes.NvidiaTeslaT4.Name(), 1, 1, 128)
	tpuDevice := buildTpuPod("tpu-device", gkelabels.TpuV4LiteDeviceValue, 4, "")
	tpuSliceSingle := buildTpuPod("tpu-slice-single", gkelabels.TpuV4PodsliceValue, 4, "2x2x1")
	tpuSliceMulti := buildTpuPod("tpu-slice-multi", gkelabels.TpuV4PodsliceValue, 4, "2x2x4")

	draTpuCC := computeclass.NewTestCrd(
		computeclass.WithLabel(gkelabels.ComputeClassLabel),
		computeclass.WithName("dra-tpu-cc"),
		computeclass.WithTpuDriverMode(computeclass.TpuDriverModeDynamicResourceAllocation),
	)

	draTpuPod := buildTpuPod("dra-tpu", "", 0, "")
	draTpuPod = addSeparation(draTpuPod, gkelabels.ComputeClassLabel, draTpuCC.Name(), true)

	unknownFamily := addMachineFamily(testPod("unknown-family"), "unknown")

	for tn, tc := range map[string]struct {
		pods             []*apiv1.Pod
		wantRequirements []nodeGroupRequirements
		wantPodStatuses  map[types.UID]PodProcessingStatus
		ccs              []computeclass.CRD
	}{
		"no pods produce empty requirements": {
			pods:             []*apiv1.Pod{},
			wantRequirements: []nodeGroupRequirements{},
		},
		"all non-GPU pods are included in the requirements": {
			pods: []*apiv1.Pod{nonGpu1, nonGpu2, nonGpu3},
			wantRequirements: []nodeGroupRequirements{{
				pods: []*apiv1.Pod{nonGpu1, nonGpu2, nonGpu3},
			}},
		},
		"only GPU pods produce empty requirements": {
			pods:             []*apiv1.Pod{k80Gpu1, k80Gpu2, t4Gpu},
			wantRequirements: []nodeGroupRequirements{},
		},
		"only TPU pods produce empty requirements": {
			pods:             []*apiv1.Pod{tpuDevice, tpuSliceSingle, tpuSliceMulti},
			wantRequirements: []nodeGroupRequirements{},
		},
		"GPU pods are correctly filtered out": {
			pods: []*apiv1.Pod{nonGpu1, nonGpu2, nonGpu3, k80Gpu1, k80Gpu2, t4Gpu},
			wantRequirements: []nodeGroupRequirements{{
				pods: []*apiv1.Pod{nonGpu1, nonGpu2, nonGpu3},
			}},
		},
		"TPU pods are correctly filtered out": {
			pods: []*apiv1.Pod{nonGpu1, nonGpu2, nonGpu3, tpuDevice, tpuSliceMulti, tpuSliceSingle},
			wantRequirements: []nodeGroupRequirements{{
				pods: []*apiv1.Pod{nonGpu1, nonGpu2, nonGpu3},
			}},
		},
		"DRA TPU pods are correctly filtered out": {
			pods: []*apiv1.Pod{nonGpu1, draTpuPod},
			wantRequirements: []nodeGroupRequirements{{
				pods: []*apiv1.Pod{nonGpu1},
			}},
			ccs: []computeclass.CRD{draTpuCC},
		},
		"pods with requirement errors are correctly filtered out and reported": {
			pods: []*apiv1.Pod{nonGpu1, unknownFamily, nonGpu2, nonGpu3},
			wantRequirements: []nodeGroupRequirements{{
				pods: []*apiv1.Pod{nonGpu1, nonGpu2, nonGpu3},
			}},
			wantPodStatuses: map[types.UID]PodProcessingStatus{
				"unknown-family": {
					Err: machineselection.NewMachineFamilyUnknownError("unknown"),
				},
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithAutoprovisioningEnabled(true).
				WithMachineTypes("test-machine-type").
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			lister := computeclass_lister.NewMockCrdListerWithLabel(tc.ccs, gkelabels.ComputeClassLabel)
			em := experiments.NewMockManager()
			manager := NewAutoprovisioningNodeGroupManager(AutoprovisioningNodeGroupManagerOptions{
				CloudProvider:        provider,
				Lister:               lister,
				ExperimentsManager:   em,
				OptionsTracker:       tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
				ResourcePolicyPuller: &placement.FakeResourcePolicyPullerProvider{},
			})
			ctx := &injectionContext{status: NewProcessingStatus()}
			requirements := manager.nonGpuPodsRequirements(ctx, tc.pods)
			if diff := cmp.Diff(tc.wantRequirements, requirements, requirementsSliceIgnoreOrderOpt, requirementsCompareOnlyPodsAndGpuOpt, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("expected requirements differ (only pods and gpuRequests are compared), diff (-want +got):\n%s", diff)
			}
			cmpErrorsByValue := cmp.Comparer(func(e1, e2 error) bool {
				return e1 == e2 || cmp.Equal(e1, e2)
			})
			if diff := cmp.Diff(tc.wantPodStatuses, ctx.status.PodStatuses, cmpErrorsByValue, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("expected pod statuses differ, diff (-want + got):\n%s", diff)
			}
		})
	}
}

func testPod(name string) *apiv1.Pod {
	pod := test.BuildTestPod(name, 1, 1024)
	pod.UID = types.UID(name)
	return pod
}

func testPodCPUMem(name string, cpu int64, mem int64) *apiv1.Pod {
	pod := test.BuildTestPod(name, cpu, mem)
	pod.UID = types.UID(name)
	return pod
}

func TestComputePossibleRequirements(t *testing.T) {
	diskTypeSSD := machinetypes.DiskTypeSSD
	testMachineFamilyName := "testfamily"
	defaultFamily := machinetypes.NewTestMachineFamily(
		testMachineFamilyName,
		[]machinetypes.MachineType{machinetypes.NewMachineTypeInfo("test-machine-type", 1, 2)},
		machinetypes.IntelSandyBridge, machinetypes.IntelIceLake,
		nil,
		[]string{diskTypeSSD},
	)

	plain1 := testPod("plain-1")
	plain2 := testPod("plain-2")

	separationA1 := addSeparation(testPod("separationA-1"), "separation", "A", true)
	separationA2 := addSeparation(testPod("separationA-2"), "separation", "A", true)
	separationB := addSeparation(testPod("separationB"), "separation", "B", true)
	separationMultiple := addSeparation(addSeparation(testPod("separationMultiple"), "sep1", "X", true), "sep2", "Y", true)
	separationIncorrect1 := addSeparation(testPod("separationIncorrect-1"), "separation", "X", false)
	separationIncorrect2 := addSeparation(testPod("separationIncorrect-2"), "separation", "Z", false)
	separationErr := podrequirements.NewInvalidWorkloadSeparationError("separation")

	unknownFamily := addMachineFamily(testPod("unknownFamily"), "unknown-family")
	unknownComputeClass := addComputeClass(testPod("unknownClass"), "unknown-class")

	n2Rome := addMinCpuPlatform(addMachineFamily(testPod("n2Rome"), "n2"), "AMD_Rome")
	n2IceLake1 := addMinCpuPlatform(addMachineFamily(testPod("n2IceLake-1"), "n2"), "Intel_Ice_Lake")
	n2IceLake2 := addMinCpuPlatform(addMachineFamily(testPod("n2IceLake-2"), "n2"), "Intel_Ice_Lake")
	n2IceLakeSeparationA1 := addSeparation(addMinCpuPlatform(addMachineFamily(testPod("n2IceLakeSeparationA-1"), "n2"), "Intel_Ice_Lake"), "separation", "A", true)
	n2IceLakeSeparationA2 := addSeparation(addMinCpuPlatform(addMachineFamily(testPod("n2IceLakeSeparationA-2"), "n2"), "Intel_Ice_Lake"), "separation", "A", true)
	n2NoPlatform := addMachineFamily(testPod("n2NoPlatform"), "n2")
	n2CascadeLake := addMinCpuPlatform(addMachineFamily(testPod("n2CascadeLake"), "n2"), "Intel_Cascade_Lake")
	c2CascadeLake := addMinCpuPlatform(addMachineFamily(testPod("c2CascadeLake"), "c2"), "Intel_Cascade_Lake")

	scaleOutClass := addComputeClass(testPod("scaleOutClass"), "Scale-Out")
	scaleOutClassAmd := addArch(addComputeClass(testPod("scaleOutClassAmd"), "Scale-Out"), "amd64")
	scaleOutClassArm := addArch(addComputeClass(testPod("scaleOutClassArm"), "Scale-Out"), "arm64")
	scaleOutClassMilan := addMinCpuPlatform(addComputeClass(testPod("scaleOutClassMilan"), "Scale-Out"), "AMD_Milan")
	scaleOutClassAltra := addMinCpuPlatform(addComputeClass(testPod("scaleOutClassAltra"), "Scale-Out"), "Ampere_Altra")
	scaleOutClassIceLake := addMinCpuPlatform(addComputeClass(testPod("scaleOutClassIceLake"), "Scale-Out"), "Intel_Ice_Lake")

	gVisorSandbox := addGVisorLabel(testPod("gvisor"))

	compactPlacement := addCompactPlacementGroupLabel(addMachineFamily(testPod("compact"), "n2"), "n2")

	extendedDurationPodCPUReq100m := "100m"
	extendedDurationPod100m := addExtendedDurationPodLabel(testPod("extended100m"), extendedDurationPodCPUReq100m)
	extendedDurationPod100m2 := addExtendedDurationPodLabel(testPod("extended100m2"), extendedDurationPodCPUReq100m)
	extendedDurationPodCPUReq200m := "200m"
	extendedDurationPod200m := addExtendedDurationPodLabel(testPod("extended200m"), extendedDurationPodCPUReq200m)

	incorrectExtendedDurationPodCPUReq := "ab"
	incorrectExtendedDurationPod := addExtendedDurationPodLabel(testPod("incorrect-extended"), incorrectExtendedDurationPodCPUReq)

	sohwPerformancePod1 := addSliceOfHardwarePod(testPodCPUMem("p1", 2000, 4096), "2")
	sohwPerformancePod2 := addSliceOfHardwarePod(testPodCPUMem("p2", 2500, 4096), "2500m")
	sohwPerformancePod3 := addSliceOfHardwarePod(testPodCPUMem("p3", 2000, 4096), "2")

	sohwPerformancePodWithEDP := addExtendedDurationPodLabel(addSliceOfHardwarePod(testPodCPUMem("p4", 2000, 4096), "2"), "2")

	// unsupported machine-family slice of hardware pod.
	sohwPerformancePodUnsupportedMachineFamily := addMachineFamily(addSliceOfHardwarePod(testPodCPUMem("soh-invalid-machine-family", 2000, 4096), "2"), "m2")
	// slice of hardware with n2d
	sohwPerformancePodN2D := addMachineFamily(addSliceOfHardwarePod(testPodCPUMem("soh-n2d", 2000, 4096), "2"), "n2d")
	sohwPerformancePodC2D := addMachineFamily(addSliceOfHardwarePod(testPodCPUMem("soh-n2d", 2000, 4096), "2"), "c2d")
	sohwPerformancePodH3 := addMachineFamily(addSliceOfHardwarePod(testPodCPUMem("soh-n2d", 2000, 4096), "2"), "h3")

	// slice of hardware with localssd selection
	lssdPod1 := addSeparation(testPod("lssd-pod-1"), gkelabels.EphemeralLocalSsdLabel, "true", true)
	lssdPod2 := addSeparation(testPod("lssd-pod-2"), gkelabels.EphemeralLocalSsdLabel, "true", true)
	sohwPerformancePodLocalSSD := addSeparation(addSliceOfHardwarePod(testPodCPUMem("soh-c3-lssd", 2000, 4096), "2"), gkelabels.EphemeralLocalSsdLabel, "true", true)

	// Boot disk config
	diskTypeHyperdiskBalanced := machinetypes.DiskTypeHyperdiskBalanced
	bootDiskConfigPod1 := addBootDiskTypeLabel(testPod("bdc-pod-1"), diskTypeSSD)
	bootDiskConfigPod1 = addBootDiskSizeLabel(bootDiskConfigPod1, "250")
	bootDiskConfigPod1 = addBootDiskEncryptionLabel(bootDiskConfigPod1, "key")
	bootDiskConfigPod2 := addBootDiskSizeLabel(testPod("bdc-pod-2"), "200")
	bootDiskConfigPod3 := addBootDiskSizeLabel(testPod("bdc-pod-3"), "200")
	bootDiskConfigPod4 := addBootDiskEncryptionLabel(testPod("bdc-pod-4"), "key2")
	bootDiskConfigPod5 := addBootDiskTypeLabel(testPod("bdc-pod-5"), diskTypeHyperdiskBalanced)
	bootDiskConfigPod5 = addBootDiskSizeLabel(bootDiskConfigPod5, "500")
	bootDiskConfigPod5 = addBootDiskEncryptionLabel(bootDiskConfigPod5, "key")

	// ComputeClass config
	n2MachineFamily := "n2"
	diskTypeStandard := machinetypes.DiskTypeStandard
	bootDiskSize := 100
	localSSDCount := 4
	bootDiskEncryptionKey := "encryption-key"

	n2NodeConfigRule := rules.NewMachineSpecRule(&n2MachineFamily, nil, nil, nil)
	n2NodeConfigRuleStorage := rules.NewRule(
		rules.WithMachineFamilyRule(&n2MachineFamily),
		rules.WithStorageRule(&diskTypeStandard, &bootDiskSize, &bootDiskEncryptionKey, &localSSDCount),
	)
	t2ANodeConfigRule := rules.NewMachineSpecRule(proto.String("t2a"), nil, nil, nil)
	n1ReservationConfigRule := rules.NewRule(
		rules.WithMachineFamilyRule(proto.String("n1")),
		rules.WithReservationsRule(rules.NewReservation().
			WithReservationName("reservation").
			WithReservationAffinity(reservations.SpecificAffinity).
			WithReservationPath("reservation")),
		rules.WithReservationsRule(rules.NewReservation().
			WithReservationName("reservation-block").
			WithReservationAffinity(reservations.SpecificAffinity).
			WithReservationProject("other").
			WithReservationPath("projects/other-project/reservations/with-block/reservationBlocks/res-block").
			WithReservationBlock("res-block")),
		rules.WithReservationsRule(rules.NewReservation().
			WithReservationName("reservation-sub-block").
			WithReservationAffinity(reservations.SpecificAffinity).
			WithReservationProject("other").
			WithReservationPath("projects/other-project/reservations/with-sub-block/reservationBlocks/res-block/reservationSubBlocks/res-sub-block").
			WithReservationBlock("res-block").
			WithReservationSubBlock("res-sub-block")),
	)
	n2AnyReservationConfigRule := rules.NewRule(
		rules.WithMachineFamilyRule(proto.String("n2")),
		rules.WithReservationsRule(rules.NewReservation().WithReservationAffinity(reservations.AnyAffinity)),
	)
	gpuConfigRule := rules.NewRule(
		rules.WithGpuRule(&machinetypes.GpuRequest{
			Config: machinetypes.GpuConfig{
				DriverVersion: "version-x",
				GpuType:       machinetypes.NvidiaTeslaT4.Name(),
			},
			Count:            1,
			PhysicalGPUCount: 1,
		}),
	)
	mppn := 32
	mppnConfigRule := rules.NewRule(
		rules.WithMaxPodsPerNodeRule(&mppn))
	placementPolicyRule := rules.NewRule(
		rules.WithMachineFamilyRule(&n2MachineFamily),
		rules.WithPlacementPolicyRule("policy"),
	)
	ccWithMultipleRules := computeclass.NewTestCrd(
		computeclass.WithName("custom-compute-class"),
		computeclass.WithRules([]rules.Rule{n2NodeConfigRule, t2ANodeConfigRule}),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithScaleUpAnyway(),
		computeclass.WithLabel(gkelabels.ComputeClassLabel),
	)
	ccWithStorage := computeclass.NewTestCrd(
		computeclass.WithName("custom-compute-class-storage"),
		computeclass.WithRules([]rules.Rule{n2NodeConfigRuleStorage}),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithLabel(gkelabels.ComputeClassLabel),
	)
	ccWithReservations := computeclass.NewTestCrd(
		computeclass.WithName("custom-compute-class-reservations"),
		computeclass.WithRules([]rules.Rule{n1ReservationConfigRule, n2AnyReservationConfigRule}),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithLabel(gkelabels.ComputeClassLabel),
	)
	ccWithGPU := computeclass.NewTestCrd(
		computeclass.WithName("custom-compute-class-gpu"),
		computeclass.WithRules([]rules.Rule{gpuConfigRule}),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithLabel(gkelabels.ComputeClassLabel),
	)
	ccWithMppn := computeclass.NewTestCrd(
		computeclass.WithName("custom-compute-class-mppn"),
		computeclass.WithRules([]rules.Rule{mppnConfigRule}),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithLabel(gkelabels.ComputeClassLabel),
	)
	ccWithPlacementPolicy := computeclass.NewTestCrd(
		computeclass.WithName("custom-compute-class-placement-policy"),
		computeclass.WithRules([]rules.Rule{placementPolicyRule}),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithLabel(gkelabels.ComputeClassLabel),
	)
	ccLister := computeclass_lister.NewMockCrdLister([]computeclass.CRD{ccWithMultipleRules, ccWithStorage, ccWithReservations, ccWithGPU, ccWithMppn, ccWithPlacementPolicy})
	ccLister.SetCrdLabel(gkelabels.ComputeClassLabel)

	customComputeClassPod := addComputeClass(testPod("custom-compute-class-pod"), ccWithMultipleRules.Name())
	customComputeClassPodWithStorage := addComputeClass(testPod("custom-compute-class-pod-with-storage"), ccWithStorage.Name())
	customComputeClassPodWithReservations := addComputeClass(testPod("test-compute-class-pod-with-reservation"), ccWithReservations.Name())
	customComputeClassPodWithGPU := addComputeClass(testPod("test-compute-class-pod-with-gpu"), ccWithGPU.Name())
	ccPodWithMppn := addComputeClass(testPod("test-compute-class-pod-with-mppn"), ccWithMppn.Name())
	ccPodWithPlacementPolicy := addComputeClass(testPod("test-compute-class-pod-with-pp"), ccWithPlacementPolicy.Name())

	// Pod requirements blocked by autoprovisioning enablement
	noAutoprovisioning := addSeparation(testPod("no-autoprovisioning"), "no-autoprovisioning", "true", true)
	autoprovisioningEligibility := &gke.MockAutoprovisioningEligibility{}
	autoprovisioningEligibility.On("UseAutoprovisioningFeaturesForPodRequirements", podrequirements.GetRequirements(noAutoprovisioning)).Return(false)
	autoprovisioningEligibility.On("UseAutoprovisioningFeaturesForPodRequirements", mock.Anything).Return(true)

	for tn, tc := range map[string]struct {
		pods                 []*apiv1.Pod
		autopilotEnabled     bool
		expectedRequirements [][]nodeGroupRequirements
		expectedErrors       map[types.UID]errors.AutoscalerError
	}{
		"no pods": {
			pods:                 []*apiv1.Pod{},
			expectedRequirements: [][]nodeGroupRequirements{},
		},
		"pod with autoprovisioning disabled by autoprovisioning eligibility": {
			pods:                 []*apiv1.Pod{noAutoprovisioning},
			expectedRequirements: [][]nodeGroupRequirements{},
		},
		"pods without any requirements end up in the same requirements": {
			pods: []*apiv1.Pod{plain1, plain2},
			expectedRequirements: [][]nodeGroupRequirements{
				{{
					pods:                 []*apiv1.Pod{plain1, plain2},
					machineSpec:          machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType: machinetypes.SelectionTypeDefault,
				}},
			},
		},
		"one of the pods with autoprovisioning disabled by autoprovisioning eligibility": {
			pods: []*apiv1.Pod{noAutoprovisioning, plain1, plain2},
			expectedRequirements: [][]nodeGroupRequirements{
				{{
					pods:                 []*apiv1.Pod{plain1, plain2},
					machineSpec:          machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType: machinetypes.SelectionTypeDefault,
				}},
			},
		},
		"pods with the same workload separation end up in the same requirements": {
			pods: []*apiv1.Pod{separationA1, separationA2},
			expectedRequirements: [][]nodeGroupRequirements{
				{{
					pods:                     []*apiv1.Pod{separationA1, separationA2},
					machineSpec:              machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType:     machinetypes.SelectionTypeDefault,
					workloadSeparationTaints: []apiv1.Taint{{Key: "separation", Value: "A", Effect: apiv1.TaintEffectNoSchedule}},
					workloadSeparationLabels: map[string]string{"separation": "A"},
				}},
			},
		},
		"pods with the same machine requirements end up in the same requirements": {
			pods: []*apiv1.Pod{n2IceLake1, n2IceLake2},
			expectedRequirements: [][]nodeGroupRequirements{
				{{
					pods:                 []*apiv1.Pod{n2IceLake1, n2IceLake2},
					machineSpec:          machinetypes.NewMachineSpecSingleFamily(machinetypes.N2, machinetypes.IntelIceLake, "", ""),
					machineSelectionType: machinetypes.SelectionTypeSpecified,
				}},
			},
		},
		"pods with the same workload separation and machine requirements end up in the same requirements": {
			pods: []*apiv1.Pod{n2IceLakeSeparationA1, n2IceLakeSeparationA2},
			expectedRequirements: [][]nodeGroupRequirements{
				{{
					pods:                     []*apiv1.Pod{n2IceLakeSeparationA1, n2IceLakeSeparationA2},
					machineSpec:              machinetypes.NewMachineSpecSingleFamily(machinetypes.N2, machinetypes.IntelIceLake, "", ""),
					machineSelectionType:     machinetypes.SelectionTypeSpecified,
					workloadSeparationTaints: []apiv1.Taint{{Key: "separation", Value: "A", Effect: apiv1.TaintEffectNoSchedule}},
					workloadSeparationLabels: map[string]string{"separation": "A"},
				}},
			},
		},
		"pods with incorrect workload separation config don't end up in any requirements": {
			pods:                 []*apiv1.Pod{separationIncorrect1, separationIncorrect2},
			expectedRequirements: [][]nodeGroupRequirements{},
			expectedErrors: map[types.UID]errors.AutoscalerError{
				"separationIncorrect-1": separationErr,
				"separationIncorrect-2": separationErr,
			},
		},
		"pods with incorrect machine requirements don't end up in any requirements": {
			pods:                 []*apiv1.Pod{unknownFamily, unknownComputeClass, n2Rome},
			expectedRequirements: [][]nodeGroupRequirements{},
			expectedErrors: map[types.UID]errors.AutoscalerError{
				"unknownFamily": machineselection.NewMachineFamilyUnknownError("unknown-family"),
				"n2Rome":        machineselection.NewMinCpuPlatformInvalidError(`machine family "n2"`, "AMD Rome"),
				"unknownClass":  NewComputeClassNotFoundError("unknown-class", "CCC", nil),
			},
		},
		"pods with different workload separation configs end up in different requirementss": {
			pods: []*apiv1.Pod{separationA1, separationB, separationMultiple},
			expectedRequirements: [][]nodeGroupRequirements{
				{{
					pods:                     []*apiv1.Pod{separationA1},
					machineSpec:              machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType:     machinetypes.SelectionTypeDefault,
					workloadSeparationTaints: []apiv1.Taint{{Key: "separation", Value: "A", Effect: apiv1.TaintEffectNoSchedule}},
					workloadSeparationLabels: map[string]string{"separation": "A"},
				}},
				{{
					pods:                     []*apiv1.Pod{separationB},
					machineSpec:              machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType:     machinetypes.SelectionTypeDefault,
					workloadSeparationTaints: []apiv1.Taint{{Key: "separation", Value: "B", Effect: apiv1.TaintEffectNoSchedule}},
					workloadSeparationLabels: map[string]string{"separation": "B"},
				}},
				{{
					pods:                 []*apiv1.Pod{separationMultiple},
					machineSpec:          machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType: machinetypes.SelectionTypeDefault,
					workloadSeparationTaints: []apiv1.Taint{
						{Key: "sep1", Value: "X", Effect: apiv1.TaintEffectNoSchedule},
						{Key: "sep2", Value: "Y", Effect: apiv1.TaintEffectNoSchedule},
					},
					workloadSeparationLabels: map[string]string{"sep1": "X", "sep2": "Y"},
				}},
			},
		},
		"pods with compute classes do not work outside autopilot": {
			pods: []*apiv1.Pod{scaleOutClass},
			expectedErrors: map[types.UID]errors.AutoscalerError{
				"scaleOutClass": machineselection.NewComputeClassNonAutopilotError(`Scale-Out`),
			},
		},
		"pods with compute classes work correctly in autopilot": {
			pods:             []*apiv1.Pod{scaleOutClass, scaleOutClassAmd, scaleOutClassArm, scaleOutClassMilan, scaleOutClassAltra, scaleOutClassIceLake},
			autopilotEnabled: true,
			expectedRequirements: [][]nodeGroupRequirements{
				{{
					pods:                 []*apiv1.Pod{scaleOutClass, scaleOutClassAmd},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.T2D}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Scale-Out"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
				}},
				{{
					pods:                 []*apiv1.Pod{scaleOutClassArm},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.T2A}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Scale-Out"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
				}},
				{{
					pods:                 []*apiv1.Pod{scaleOutClassMilan},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.T2D}, MinCpuPlatform: machinetypes.AmdMilan, ComputeClassName: "Scale-Out"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
				}},
				{{
					pods:                 []*apiv1.Pod{scaleOutClassAltra},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.T2A}, MinCpuPlatform: machinetypes.AmpereAltra, ComputeClassName: "Scale-Out"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
				}},
			},
			expectedErrors: map[types.UID]errors.AutoscalerError{
				"scaleOutClassIceLake": machineselection.NewMinCpuPlatformInvalidError(`compute class "Scale-Out"`, "Intel Ice Lake"),
			},
		},
		"pods with different machine requirements end up in different requirements": {
			pods: []*apiv1.Pod{n2IceLake1, n2CascadeLake, n2NoPlatform, c2CascadeLake},
			expectedRequirements: [][]nodeGroupRequirements{
				{{
					pods:                 []*apiv1.Pod{n2IceLake1},
					machineSpec:          machinetypes.NewMachineSpecSingleFamily(machinetypes.N2, machinetypes.IntelIceLake, "", ""),
					machineSelectionType: machinetypes.SelectionTypeSpecified,
				}},
				{{
					pods:                 []*apiv1.Pod{n2CascadeLake},
					machineSpec:          machinetypes.NewMachineSpecSingleFamily(machinetypes.N2, machinetypes.IntelCascadeLake, "", ""),
					machineSelectionType: machinetypes.SelectionTypeSpecified,
				}},
				{{
					pods:                 []*apiv1.Pod{n2NoPlatform},
					machineSpec:          machinetypes.NewMachineSpecSingleFamily(machinetypes.N2, machinetypes.AnyPlatform, "", ""),
					machineSelectionType: machinetypes.SelectionTypeSpecified,
				}},
				{{
					pods:                 []*apiv1.Pod{c2CascadeLake},
					machineSpec:          machinetypes.NewMachineSpecSingleFamily(machinetypes.C2, machinetypes.IntelCascadeLake, "", ""),
					machineSelectionType: machinetypes.SelectionTypeSpecified,
				}},
			},
		},
		"pods with different sandbox requirements end up in different requirements": {
			pods: []*apiv1.Pod{plain1, gVisorSandbox},
			expectedRequirements: [][]nodeGroupRequirements{
				{{
					pods:                 []*apiv1.Pod{plain1},
					machineSpec:          machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType: machinetypes.SelectionTypeDefault,
				}},
				{{
					pods:                 []*apiv1.Pod{gVisorSandbox},
					machineSpec:          machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType: machinetypes.SelectionTypeDefault,
					sandboxType:          sandbox.GVisor,
				}},
			},
		},
		"pods with different compact placement end up in different requirements": {
			pods: []*apiv1.Pod{plain1, compactPlacement},
			expectedRequirements: [][]nodeGroupRequirements{
				{{
					pods:                 []*apiv1.Pod{plain1},
					machineSpec:          machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType: machinetypes.SelectionTypeDefault,
				}},
				{{
					pods:                 []*apiv1.Pod{compactPlacement},
					machineSpec:          machinetypes.NewMachineSpecSingleFamily(machinetypes.N2, machinetypes.AnyPlatform, "", ""),
					machineSelectionType: machinetypes.SelectionTypeSpecified,
					placementGroup:       placement.Spec{GroupId: "n2"},
				}},
			},
		},
		"extended duration pods with different cpu requests end up in different requirements": {
			pods:             []*apiv1.Pod{extendedDurationPod100m, extendedDurationPod200m},
			autopilotEnabled: true,
			expectedRequirements: [][]nodeGroupRequirements{
				{{
					pods:                      []*apiv1.Pod{extendedDurationPod100m},
					machineSpec:               machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType:      machinetypes.SelectionTypeDefault,
					extendedDurationPodCPUReq: extendedDurationPodCPUReq100m,
				}},
				{{
					pods:                      []*apiv1.Pod{extendedDurationPod200m},
					machineSpec:               machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType:      machinetypes.SelectionTypeDefault,
					extendedDurationPodCPUReq: extendedDurationPodCPUReq200m,
				}},
			},
		},
		"extended duration pods with same cpu requests end up in the same requirements": {
			pods:             []*apiv1.Pod{extendedDurationPod100m, extendedDurationPod100m2},
			autopilotEnabled: true,
			expectedRequirements: [][]nodeGroupRequirements{
				{{
					pods:                      []*apiv1.Pod{extendedDurationPod100m, extendedDurationPod100m2},
					machineSpec:               machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType:      machinetypes.SelectionTypeDefault,
					extendedDurationPodCPUReq: extendedDurationPodCPUReq100m,
				}},
			},
		},
		"pods with slice of hardware compute classes do not work outside autopilot": {
			pods: []*apiv1.Pod{sohwPerformancePod1, sohwPerformancePod2},
			expectedErrors: map[types.UID]errors.AutoscalerError{
				"p1": machineselection.NewComputeClassNonAutopilotError(`Performance`),
				"p2": machineselection.NewComputeClassNonAutopilotError(`Performance`),
			},
		},
		"pods with slice of hardware compute classes work correctly in autopilot": {
			pods:             []*apiv1.Pod{sohwPerformancePod1, sohwPerformancePod2},
			autopilotEnabled: true,
			expectedRequirements: [][]nodeGroupRequirements{
				{{
					pods:                 []*apiv1.Pod{sohwPerformancePod1},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.C3}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Performance"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
					podCapacity:          1,
					podIsolationCPUReq:   "2",
				}},
				{{
					pods:                 []*apiv1.Pod{sohwPerformancePod2},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.C3}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Performance"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
					podCapacity:          1,
					podIsolationCPUReq:   "2500m",
				}},
			},
			expectedErrors: map[types.UID]errors.AutoscalerError{},
		},
		"pods with slice of hardware compute classes work correctly in autopilot with edp": {
			pods:             []*apiv1.Pod{sohwPerformancePod1, sohwPerformancePod2, sohwPerformancePod3, sohwPerformancePodWithEDP},
			autopilotEnabled: true,
			expectedRequirements: [][]nodeGroupRequirements{
				{{
					pods:                 []*apiv1.Pod{sohwPerformancePod1, sohwPerformancePod3},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.C3}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Performance"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
					podCapacity:          1,
					podIsolationCPUReq:   "2",
				}},
				{{
					pods:                 []*apiv1.Pod{sohwPerformancePod2},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.C3}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Performance"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
					podCapacity:          1,
					podIsolationCPUReq:   "2500m",
				}},
				{{
					pods:                      []*apiv1.Pod{sohwPerformancePodWithEDP},
					machineSpec:               machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.C3}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Performance"},
					machineSelectionType:      machinetypes.SelectionTypeSpecified,
					podCapacity:               1,
					podIsolationCPUReq:        "2",
					extendedDurationPodCPUReq: "2",
				}},
			},
			expectedErrors: map[types.UID]errors.AutoscalerError{},
		},
		"pods with slice of hardware compute classes one with invalid machine family": {
			pods:             []*apiv1.Pod{sohwPerformancePod1, sohwPerformancePodUnsupportedMachineFamily},
			autopilotEnabled: true,
			expectedRequirements: [][]nodeGroupRequirements{
				{{
					pods:                 []*apiv1.Pod{sohwPerformancePod1},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.C3}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Performance"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
					podCapacity:          1,
					podIsolationCPUReq:   "2",
				}},
			},
			expectedErrors: map[types.UID]errors.AutoscalerError{
				"soh-invalid-machine-family": machineselection.NewComputeClassWithInvalidMachineFamilyError("Performance", "m2"),
			},
		},
		"pods with slice of hardware compute classes with different machine-families have different requirements": {
			pods:             []*apiv1.Pod{sohwPerformancePod1, sohwPerformancePodN2D, sohwPerformancePodC2D, sohwPerformancePodH3},
			autopilotEnabled: true,
			expectedRequirements: [][]nodeGroupRequirements{
				{{
					pods:                 []*apiv1.Pod{sohwPerformancePod1},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.C3}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Performance"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
					podCapacity:          1,
					podIsolationCPUReq:   "2",
				}},
				{{
					pods:                 []*apiv1.Pod{sohwPerformancePodN2D},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.N2D}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Performance"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
					podCapacity:          1,
					podIsolationCPUReq:   "2",
				}},
				{{
					pods:                 []*apiv1.Pod{sohwPerformancePodC2D},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.C2D}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Performance"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
					podCapacity:          1,
					podIsolationCPUReq:   "2",
				}},
				{{
					pods:                 []*apiv1.Pod{sohwPerformancePodH3},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.H3}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Performance"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
					podCapacity:          1,
					podIsolationCPUReq:   "2",
				}},
			},
			expectedErrors: map[types.UID]errors.AutoscalerError{},
		},
		"pods with slice of hardware compute classes with local ssd have different requirements": {
			pods:             []*apiv1.Pod{sohwPerformancePod1, sohwPerformancePodLocalSSD},
			autopilotEnabled: true,
			expectedRequirements: [][]nodeGroupRequirements{
				{{
					pods:                 []*apiv1.Pod{sohwPerformancePod1},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.C3}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Performance"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
					podCapacity:          1,
					podIsolationCPUReq:   "2",
				}},
				{{
					pods:                       []*apiv1.Pod{sohwPerformancePodLocalSSD},
					machineSpec:                machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.C3}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Performance"},
					machineSelectionType:       machinetypes.SelectionTypeSpecified,
					podCapacity:                1,
					podIsolationCPUReq:         "2",
					explicitlyRequiresLocalSSD: true,
				}},
			},
			expectedErrors: map[types.UID]errors.AutoscalerError{},
		},
		"similar pods with local ssd have same requirements": {
			pods:             []*apiv1.Pod{lssdPod1, lssdPod2},
			autopilotEnabled: true,
			expectedRequirements: [][]nodeGroupRequirements{
				{{
					pods:                       []*apiv1.Pod{lssdPod1, lssdPod2},
					machineSpec:                machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType:       machinetypes.SelectionTypeDefault,
					explicitlyRequiresLocalSSD: true,
				}},
			},
			expectedErrors: map[types.UID]errors.AutoscalerError{},
		},
		"pods with local ssd without autopilot do not require localSSD taint": {
			pods:             []*apiv1.Pod{lssdPod1, lssdPod2},
			autopilotEnabled: false,
			expectedRequirements: [][]nodeGroupRequirements{
				{{
					pods:                       []*apiv1.Pod{lssdPod1, lssdPod2},
					machineSpec:                machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType:       machinetypes.SelectionTypeDefault,
					explicitlyRequiresLocalSSD: true,
				}},
			},
			expectedErrors: map[types.UID]errors.AutoscalerError{},
		},
		"pods with boot disk config have different requirements": {
			pods:             []*apiv1.Pod{bootDiskConfigPod1, bootDiskConfigPod2, bootDiskConfigPod4, bootDiskConfigPod5},
			autopilotEnabled: true,
			expectedRequirements: [][]nodeGroupRequirements{
				{{
					pods: []*apiv1.Pod{bootDiskConfigPod1},
					machineSpec: machinetypes.MachineSpec{
						Families:       []machinetypes.MachineFamily{defaultFamily},
						MinCpuPlatform: machinetypes.AnyPlatform,
						BootDiskType:   diskTypeSSD,
					},
					machineSelectionType:         machinetypes.SelectionTypeDefault,
					bootDiskType:                 "pd-ssd",
					bootDiskSize:                 250,
					bootDiskEncryptionKey:        "encryption-key-annotation",
					bootDiskEncryptionAnnotation: "key",
				}},
				{{
					pods:                 []*apiv1.Pod{bootDiskConfigPod2},
					machineSpec:          machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType: machinetypes.SelectionTypeDefault,
					bootDiskSize:         200,
				}},
				{{
					pods:                         []*apiv1.Pod{bootDiskConfigPod4},
					machineSpec:                  machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType:         machinetypes.SelectionTypeDefault,
					bootDiskEncryptionKey:        "encryption-key-annotation",
					bootDiskEncryptionAnnotation: "key2",
				}},
			},
			expectedErrors: map[types.UID]errors.AutoscalerError{
				"bdc-pod-5": machineselection.NewBootDiskTypeIncompatibleError(fmt.Sprintf("machine family %q", testMachineFamilyName), diskTypeHyperdiskBalanced),
			},
		},
		"similar pods with boot disk config have same requirements": {
			pods:             []*apiv1.Pod{bootDiskConfigPod2, bootDiskConfigPod3},
			autopilotEnabled: true,
			expectedRequirements: [][]nodeGroupRequirements{
				{{
					pods:                 []*apiv1.Pod{bootDiskConfigPod2, bootDiskConfigPod3},
					machineSpec:          machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType: machinetypes.SelectionTypeDefault,
					bootDiskSize:         200,
				}},
			},
			expectedErrors: map[types.UID]errors.AutoscalerError{},
		},
		"pod with custom compute class specified": {
			pods:             []*apiv1.Pod{customComputeClassPod},
			autopilotEnabled: true,
			expectedRequirements: [][]nodeGroupRequirements{
				{
					requirementsForCC([]*apiv1.Pod{customComputeClassPod}, machinetypes.N2, machinetypes.SelectionTypeSpecified, ccWithMultipleRules, n2NodeConfigRule),
					requirementsForCC([]*apiv1.Pod{customComputeClassPod}, machinetypes.T2A, machinetypes.SelectionTypeSpecified, ccWithMultipleRules, t2ANodeConfigRule),
					requirementsForCC([]*apiv1.Pod{customComputeClassPod}, defaultFamily, machinetypes.SelectionTypeDefault, ccWithMultipleRules, nil),
				},
			},
		},
		"pod with custom compute class specified, with storage": {
			pods: []*apiv1.Pod{customComputeClassPodWithStorage},
			expectedRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{customComputeClassPodWithStorage}),
						withMachineFamily(machinetypes.N2),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(ccWithStorage),
						withNodeConfigRule(n2NodeConfigRuleStorage),
						withBootDiskSize(bootDiskSize),
						withBootDiskType(diskTypeStandard),
						withEphemeralStorageLSSDCount(localSSDCount),
						withTotalLSSDCount(localSSDCount),
						withBootDiskEncryptionKey(bootDiskEncryptionKey)),
				},
			},
		},
		"pod with custom compute class specified, non autopilot": {
			pods:             []*apiv1.Pod{customComputeClassPod},
			autopilotEnabled: false,
			expectedRequirements: [][]nodeGroupRequirements{
				{
					requirementsForCC([]*apiv1.Pod{customComputeClassPod}, machinetypes.N2, machinetypes.SelectionTypeSpecified, ccWithMultipleRules, n2NodeConfigRule),
					requirementsForCC([]*apiv1.Pod{customComputeClassPod}, machinetypes.T2A, machinetypes.SelectionTypeSpecified, ccWithMultipleRules, t2ANodeConfigRule),
					requirementsForCC([]*apiv1.Pod{customComputeClassPod}, defaultFamily, machinetypes.SelectionTypeDefault, ccWithMultipleRules, nil),
				},
			},
		},
		"pod with custom compute class specified, with reservations": {
			pods: []*apiv1.Pod{customComputeClassPodWithReservations},
			expectedRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(
						withPods([]*apiv1.Pod{customComputeClassPodWithReservations}),
						withMachineFamily(machinetypes.N1),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(ccWithReservations),
						withNodeConfigRule(n1ReservationConfigRule),
						withReservationName("reservation"),
						withReservationProject("12345"),
						withReservationExists(),
						withReservationAffinity(reservations.SpecificAffinity),
					),
					newTestNodeGroupRequirements(
						withPods([]*apiv1.Pod{customComputeClassPodWithReservations}),
						withMachineFamily(machinetypes.N1),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(ccWithReservations),
						withNodeConfigRule(n1ReservationConfigRule),
						withReservationName("reservation-block"),
						withReservationExists(),
						withReservationAffinity(reservations.SpecificAffinity),
						withReservationProject("other"),
						withReservationBlock("res-block"),
					),
					newTestNodeGroupRequirements(
						withPods([]*apiv1.Pod{customComputeClassPodWithReservations}),
						withMachineFamily(machinetypes.N1),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(ccWithReservations),
						withNodeConfigRule(n1ReservationConfigRule),
						withReservationName("reservation-sub-block"),
						withReservationExists(),
						withReservationAffinity(reservations.SpecificAffinity),
						withReservationProject("other"),
						withReservationBlock("res-block"),
						withReservationSubBlock("res-sub-block"),
					),
					newTestNodeGroupRequirements(
						withPods([]*apiv1.Pod{customComputeClassPodWithReservations}),
						withMachineFamily(machinetypes.N2),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(ccWithReservations),
						withNodeConfigRule(n2AnyReservationConfigRule),
						withReservationAffinity(reservations.AnyAffinity),
						withReservationExists(),
					),
				},
			},
		},
		"pod with custom compute class specified, with gpu": {
			pods: []*apiv1.Pod{customComputeClassPodWithGPU},
			expectedRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(
						withPods([]*apiv1.Pod{customComputeClassPodWithGPU}),
						withMachineFamily(machinetypes.N1),
						withMachineSelectionType(machinetypes.SelectionTypeImplied),
						withComputeClass(ccWithGPU),
						withGPURequestWithDriverVersion(machinetypes.NvidiaTeslaT4.Name(), 1, "version-x"),
						withNodeConfigRule(gpuConfigRule),
					),
				},
			},
		},
		"pod with custom compute class specified, with mppn": {
			pods: []*apiv1.Pod{ccPodWithMppn},
			expectedRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(
						withPods([]*apiv1.Pod{ccPodWithMppn}),
						withComputeClass(ccWithMppn),
						withNodeConfigRule(mppnConfigRule),
						withMachineFamily(defaultFamily),
						withMachineSelectionType(machinetypes.SelectionTypeDefault),
						withMaxPodsPerNode(mppn),
					),
				},
			},
		},
		"pod with custom compute class specified, with placement policy": {
			pods: []*apiv1.Pod{ccPodWithPlacementPolicy},
			expectedRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(
						withPods([]*apiv1.Pod{ccPodWithPlacementPolicy}),
						withPlacementPolicy("policy"),
						withPlacementPolicyRule(placementPolicyRule),
						withMachineFamily(machinetypes.N2),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(ccWithPlacementPolicy),
					),
				},
			},
		},
		"everything works correctly together": {
			pods: []*apiv1.Pod{
				plain1, plain2, separationA1, separationA2, separationB, separationMultiple, separationIncorrect1, separationIncorrect2, unknownFamily, unknownComputeClass,
				n2Rome, n2IceLake1, n2IceLake2, n2IceLakeSeparationA1, n2IceLakeSeparationA2, n2NoPlatform, n2CascadeLake, c2CascadeLake,
				scaleOutClass, scaleOutClassAmd, scaleOutClassArm, scaleOutClassMilan, scaleOutClassAltra, scaleOutClassIceLake,
				extendedDurationPod100m, extendedDurationPod200m, incorrectExtendedDurationPod, sohwPerformancePod1, sohwPerformancePod2, sohwPerformancePod3, sohwPerformancePodWithEDP,
				sohwPerformancePodUnsupportedMachineFamily, sohwPerformancePodN2D, sohwPerformancePodC2D, sohwPerformancePodH3, sohwPerformancePodLocalSSD, lssdPod1, lssdPod2,
				bootDiskConfigPod1, bootDiskConfigPod2, bootDiskConfigPod4, bootDiskConfigPod5, customComputeClassPod, customComputeClassPodWithStorage,
			},
			autopilotEnabled: true,
			expectedRequirements: [][]nodeGroupRequirements{
				{{
					pods:                 []*apiv1.Pod{plain1, plain2},
					machineSpec:          machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType: machinetypes.SelectionTypeDefault,
				}},
				{{
					pods:                     []*apiv1.Pod{separationA1, separationA2},
					machineSpec:              machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType:     machinetypes.SelectionTypeDefault,
					workloadSeparationTaints: []apiv1.Taint{{Key: "separation", Value: "A", Effect: apiv1.TaintEffectNoSchedule}},
					workloadSeparationLabels: map[string]string{"separation": "A"},
				}},
				{{
					pods:                 []*apiv1.Pod{n2IceLake1, n2IceLake2},
					machineSpec:          machinetypes.NewMachineSpecSingleFamily(machinetypes.N2, machinetypes.IntelIceLake, "", ""),
					machineSelectionType: machinetypes.SelectionTypeSpecified,
				}},
				{{
					pods:                     []*apiv1.Pod{n2IceLakeSeparationA1, n2IceLakeSeparationA2},
					machineSpec:              machinetypes.NewMachineSpecSingleFamily(machinetypes.N2, machinetypes.IntelIceLake, "", ""),
					machineSelectionType:     machinetypes.SelectionTypeSpecified,
					workloadSeparationTaints: []apiv1.Taint{{Key: "separation", Value: "A", Effect: apiv1.TaintEffectNoSchedule}},
					workloadSeparationLabels: map[string]string{"separation": "A"},
				}},
				{{
					pods:                     []*apiv1.Pod{separationB},
					machineSpec:              machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType:     machinetypes.SelectionTypeDefault,
					workloadSeparationTaints: []apiv1.Taint{{Key: "separation", Value: "B", Effect: apiv1.TaintEffectNoSchedule}},
					workloadSeparationLabels: map[string]string{"separation": "B"},
				}},
				{{
					pods:                 []*apiv1.Pod{separationMultiple},
					machineSpec:          machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType: machinetypes.SelectionTypeDefault,
					workloadSeparationTaints: []apiv1.Taint{
						{Key: "sep1", Value: "X", Effect: apiv1.TaintEffectNoSchedule},
						{Key: "sep2", Value: "Y", Effect: apiv1.TaintEffectNoSchedule},
					},
					workloadSeparationLabels: map[string]string{"sep1": "X", "sep2": "Y"},
				}},
				{{
					pods:                 []*apiv1.Pod{n2CascadeLake},
					machineSpec:          machinetypes.NewMachineSpecSingleFamily(machinetypes.N2, machinetypes.IntelCascadeLake, "", ""),
					machineSelectionType: machinetypes.SelectionTypeSpecified,
				}},
				{{
					pods:                 []*apiv1.Pod{n2NoPlatform},
					machineSpec:          machinetypes.NewMachineSpecSingleFamily(machinetypes.N2, machinetypes.AnyPlatform, "", ""),
					machineSelectionType: machinetypes.SelectionTypeSpecified,
				}},
				{{
					pods:                 []*apiv1.Pod{c2CascadeLake},
					machineSpec:          machinetypes.NewMachineSpecSingleFamily(machinetypes.C2, machinetypes.IntelCascadeLake, "", ""),
					machineSelectionType: machinetypes.SelectionTypeSpecified,
				}},
				{{
					pods:                 []*apiv1.Pod{scaleOutClass, scaleOutClassAmd},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.T2D}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Scale-Out"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
				}},
				{{
					pods:                 []*apiv1.Pod{scaleOutClassArm},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.T2A}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Scale-Out"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
				}},
				{{
					pods:                 []*apiv1.Pod{scaleOutClassMilan},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.T2D}, MinCpuPlatform: machinetypes.AmdMilan, ComputeClassName: "Scale-Out"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
				}},
				{{
					pods:                 []*apiv1.Pod{scaleOutClassAltra},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.T2A}, MinCpuPlatform: machinetypes.AmpereAltra, ComputeClassName: "Scale-Out"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
				}},
				{{
					pods:                      []*apiv1.Pod{extendedDurationPod100m},
					machineSpec:               machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType:      machinetypes.SelectionTypeDefault,
					extendedDurationPodCPUReq: extendedDurationPodCPUReq100m,
				}},
				{{
					pods:                      []*apiv1.Pod{extendedDurationPod200m},
					machineSpec:               machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType:      machinetypes.SelectionTypeDefault,
					extendedDurationPodCPUReq: extendedDurationPodCPUReq200m,
				}},
				{{
					pods:                 []*apiv1.Pod{sohwPerformancePod1, sohwPerformancePod3},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.C3}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Performance"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
					podCapacity:          1,
					podIsolationCPUReq:   "2",
				}},
				{{
					pods:                 []*apiv1.Pod{sohwPerformancePod2},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.C3}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Performance"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
					podCapacity:          1,
					podIsolationCPUReq:   "2500m",
				}},
				{{
					pods:                      []*apiv1.Pod{sohwPerformancePodWithEDP},
					machineSpec:               machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.C3}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Performance"},
					machineSelectionType:      machinetypes.SelectionTypeSpecified,
					podCapacity:               1,
					podIsolationCPUReq:        "2",
					extendedDurationPodCPUReq: "2",
				}},
				{{
					pods:                 []*apiv1.Pod{sohwPerformancePodN2D},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.N2D}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Performance"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
					podCapacity:          1,
					podIsolationCPUReq:   "2",
				}},
				{{
					pods:                 []*apiv1.Pod{sohwPerformancePodC2D},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.C2D}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Performance"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
					podCapacity:          1,
					podIsolationCPUReq:   "2",
				}},
				{{
					pods:                 []*apiv1.Pod{sohwPerformancePodH3},
					machineSpec:          machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.H3}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Performance"},
					machineSelectionType: machinetypes.SelectionTypeSpecified,
					podCapacity:          1,
					podIsolationCPUReq:   "2",
				}},
				{{
					pods:                       []*apiv1.Pod{sohwPerformancePodLocalSSD},
					machineSpec:                machinetypes.MachineSpec{Families: []machinetypes.MachineFamily{machinetypes.C3}, MinCpuPlatform: machinetypes.AnyPlatform, ComputeClassName: "Performance"},
					machineSelectionType:       machinetypes.SelectionTypeSpecified,
					podCapacity:                1,
					podIsolationCPUReq:         "2",
					explicitlyRequiresLocalSSD: true,
				}},
				{{
					pods:                       []*apiv1.Pod{lssdPod1, lssdPod2},
					machineSpec:                machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType:       machinetypes.SelectionTypeDefault,
					explicitlyRequiresLocalSSD: true,
				}},
				{{
					pods: []*apiv1.Pod{bootDiskConfigPod1},
					machineSpec: machinetypes.MachineSpec{
						Families:       []machinetypes.MachineFamily{defaultFamily},
						MinCpuPlatform: machinetypes.AnyPlatform,
						BootDiskType:   diskTypeSSD,
					},
					machineSelectionType:         machinetypes.SelectionTypeDefault,
					bootDiskType:                 "pd-ssd",
					bootDiskSize:                 250,
					bootDiskEncryptionKey:        "encryption-key-annotation",
					bootDiskEncryptionAnnotation: "key",
				}},
				{{
					pods:                 []*apiv1.Pod{bootDiskConfigPod2},
					machineSpec:          machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType: machinetypes.SelectionTypeDefault,
					bootDiskSize:         200,
				}},
				{{
					pods:                         []*apiv1.Pod{bootDiskConfigPod4},
					machineSpec:                  machinetypes.NewMachineSpecSingleFamily(defaultFamily, machinetypes.AnyPlatform, "", ""),
					machineSelectionType:         machinetypes.SelectionTypeDefault,
					bootDiskEncryptionKey:        "encryption-key-annotation",
					bootDiskEncryptionAnnotation: "key2",
				}},
				{
					requirementsForCC([]*apiv1.Pod{customComputeClassPod}, machinetypes.N2, machinetypes.SelectionTypeSpecified, ccWithMultipleRules, n2NodeConfigRule),
					requirementsForCC([]*apiv1.Pod{customComputeClassPod}, machinetypes.T2A, machinetypes.SelectionTypeSpecified, ccWithMultipleRules, t2ANodeConfigRule),
					requirementsForCC([]*apiv1.Pod{customComputeClassPod}, defaultFamily, machinetypes.SelectionTypeDefault, ccWithMultipleRules, nil),
				},
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{customComputeClassPodWithStorage}),
						withMachineFamily(machinetypes.N2),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(ccWithStorage),
						withNodeConfigRule(n2NodeConfigRuleStorage),
						withBootDiskSize(bootDiskSize),
						withBootDiskType(diskTypeStandard),
						withEphemeralStorageLSSDCount(localSSDCount),
						withTotalLSSDCount(localSSDCount),
						withBootDiskEncryptionKey(bootDiskEncryptionKey)),
				},
			},
			expectedErrors: map[types.UID]errors.AutoscalerError{
				"separationIncorrect-1":      separationErr,
				"separationIncorrect-2":      separationErr,
				"unknownFamily":              machineselection.NewMachineFamilyUnknownError("unknown-family"),
				"n2Rome":                     machineselection.NewMinCpuPlatformInvalidError(`machine family "n2"`, "AMD Rome"),
				"scaleOutClassIceLake":       machineselection.NewMinCpuPlatformInvalidError(`compute class "Scale-Out"`, "Intel Ice Lake"),
				"incorrect-extended":         NewInvalidExtendedDurationPodCPUReq("ab"),
				"soh-invalid-machine-family": machineselection.NewComputeClassWithInvalidMachineFamilyError("Performance", "m2"),
				"unknownClass":               NewComputeClassNotFoundError("unknown-class", "CCC", nil),
				"bdc-pod-5":                  machineselection.NewBootDiskTypeIncompatibleError(fmt.Sprintf("machine family %q", testMachineFamilyName), diskTypeHyperdiskBalanced),
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineTypes("test-machine-type").
				WithUsingPSCInfrastructure(true).
				WithAutoprovisioningEligibility(autoprovisioningEligibility).
				WithAutopilotEnabled(tc.autopilotEnabled).
				WithAutoprovisioningDefaultFamily(defaultFamily).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()

			em := experiments.NewMockManager()
			manager := NewAutoprovisioningNodeGroupManager(AutoprovisioningNodeGroupManagerOptions{
				CloudProvider: provider,
				Flags: AutoprovisioningNodeGroupManagerFlags{
					BootDiskConfigEnabled: true,
					ReservationFlags: ReservationFlags{
						SpecificTypeReservationsEnabled: true,
					},
				},
				ExperimentsManager:   em,
				OptionsTracker:       tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
				Lister:               ccLister,
				ReservationsPuller:   reservations.NewTestingReservationsPuller("12345", nil, nil),
				ResourcePolicyPuller: &placement.FakeResourcePolicyPullerProvider{},
			})
			requirements, podErrors := manager.computePossibleRequirements(tc.pods, machinetypes.GpuRequest{}, TpuRequest{})
			if diff := cmp.Diff(tc.expectedRequirements, requirements, nestedRequirementsIgnoreOrderOpt, compareAllUnexportedOpt, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("computePossibleRequirements expected requirements diff (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.expectedErrors, podErrors, compareErrorsByValueOpt, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("computePossibleRequirements expected pod errors diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTpuRequestGenerator_UpdateNodePoolSpec(t *testing.T) {
	zone := "us-west1-b"
	for name, tc := range map[string]struct {
		machineType          string
		systemLabels         map[string]string
		extraResources       map[string]resource.Quantity
		expectedSpecLabels   map[string]string
		expectedTpuType      string
		expectedTpuTopology  string
		expectedTpuMultihost bool
	}{
		"TPU v3 Device; SingleHost 2x2": {
			machineType: "ct3-hightpu-4t",
			systemLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV3DeviceValue,
				gkelabels.TPUTopologyLabel:      "2x2",
				gkelabels.AcceleratorCountLabel: "4",
			},
			extraResources: map[string]resource.Quantity{
				tpu.ResourceGoogleTPU: resource.MustParse("4"),
			},
			expectedSpecLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV3DeviceValue,
				gkelabels.TPUTopologyLabel:      "2x2",
				gkelabels.AcceleratorCountLabel: "4",
			},
			expectedTpuType:      gkelabels.TpuV3DeviceValue,
			expectedTpuTopology:  "2x2",
			expectedTpuMultihost: false,
		},
		"TPU v3 Podslice; MultiHost 4x4": {
			machineType: "ct3-hightpu-4t",
			systemLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV3SliceValue,
				gkelabels.TPUTopologyLabel:      "4x4",
				gkelabels.AcceleratorCountLabel: "4",
			},
			extraResources: map[string]resource.Quantity{
				tpu.ResourceGoogleTPU: resource.MustParse("4"),
			},
			expectedSpecLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV3SliceValue,
				gkelabels.TPUTopologyLabel:      "4x4",
				gkelabels.AcceleratorCountLabel: "4",
			},
			expectedTpuType:      gkelabels.TpuV3SliceValue,
			expectedTpuTopology:  "4x4",
			expectedTpuMultihost: true,
		},
		"TPU v3 Podslice; MultiHost 16x32": {
			machineType: "ct3-hightpu-4t",
			systemLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV3SliceValue,
				gkelabels.TPUTopologyLabel:      "16x32",
				gkelabels.AcceleratorCountLabel: "4",
			},
			extraResources: map[string]resource.Quantity{
				tpu.ResourceGoogleTPU: resource.MustParse("4"),
			},
			expectedSpecLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV3SliceValue,
				gkelabels.TPUTopologyLabel:      "16x32",
				gkelabels.AcceleratorCountLabel: "4",
			},
			expectedTpuType:      gkelabels.TpuV3SliceValue,
			expectedTpuTopology:  "16x32",
			expectedTpuMultihost: true,
		},
		"TPU Device; no topology": {
			machineType: "ct4l-hightpu-4t",
			systemLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV4LiteDeviceValue,
				gkelabels.TPUTopologyLabel:      "2x2",
				gkelabels.AcceleratorCountLabel: "4",
			},
			extraResources: map[string]resource.Quantity{
				tpu.ResourceGoogleTPU: resource.MustParse("4"),
			},
			expectedSpecLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV4LiteDeviceValue,
				gkelabels.TPUTopologyLabel:      "2x2",
				gkelabels.AcceleratorCountLabel: "4",
			},
			expectedTpuType:      gkelabels.TpuV4LiteDeviceValue,
			expectedTpuTopology:  "2x2",
			expectedTpuMultihost: false,
		},
		"TPU Podslice; with topology": {
			machineType: "ct4p-hightpu-4t",
			systemLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV4PodsliceValue,
				gkelabels.TPUTopologyLabel:      "2x2x4",
				gkelabels.AcceleratorCountLabel: "4",
			},
			extraResources: map[string]resource.Quantity{
				tpu.ResourceGoogleTPU: resource.MustParse("4"),
			},
			expectedSpecLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV4PodsliceValue,
				gkelabels.TPUTopologyLabel:      "2x2x4",
				gkelabels.AcceleratorCountLabel: "4",
			},
			expectedTpuType:      gkelabels.TpuV4PodsliceValue,
			expectedTpuTopology:  "2x2x4",
			expectedTpuMultihost: true,
		},
		"TPU Podslice single host; with topology": {
			machineType: "ct4p-hightpu-4t",
			systemLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV4PodsliceValue,
				gkelabels.TPUTopologyLabel:      "2x2x1",
				gkelabels.AcceleratorCountLabel: "4",
			},
			extraResources: map[string]resource.Quantity{
				tpu.ResourceGoogleTPU: resource.MustParse("4"),
			},
			expectedSpecLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV4PodsliceValue,
				gkelabels.TPUTopologyLabel:      "2x2x1",
				gkelabels.AcceleratorCountLabel: "4",
			},
			expectedTpuType:      gkelabels.TpuV4PodsliceValue,
			expectedTpuTopology:  "2x2x1",
			expectedTpuMultihost: false,
		},
		"TPU v5 Lite Device; SingleHost '1x1'": {
			machineType: "ct5l-hightpu-1t",
			systemLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV5LiteDeviceValue,
				gkelabels.TPUTopologyLabel:      "1x1",
				gkelabels.AcceleratorCountLabel: "1",
			},
			extraResources: map[string]resource.Quantity{
				tpu.ResourceGoogleTPU: resource.MustParse("1"),
			},
			expectedSpecLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV5LiteDeviceValue,
				gkelabels.TPUTopologyLabel:      "1x1",
				gkelabels.AcceleratorCountLabel: "1",
			},
			expectedTpuType:      gkelabels.TpuV5LiteDeviceValue,
			expectedTpuTopology:  "1x1",
			expectedTpuMultihost: false,
		},
		"TPU v5 Lite Device; SingleHost '2x2'": {
			machineType: "ct5l-hightpu-4t",
			systemLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV5LiteDeviceValue,
				gkelabels.TPUTopologyLabel:      "2x2",
				gkelabels.AcceleratorCountLabel: "4",
			},
			extraResources: map[string]resource.Quantity{
				tpu.ResourceGoogleTPU: resource.MustParse("4"),
			},
			expectedSpecLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV5LiteDeviceValue,
				gkelabels.TPUTopologyLabel:      "2x2",
				gkelabels.AcceleratorCountLabel: "4",
			},
			expectedTpuType:      gkelabels.TpuV5LiteDeviceValue,
			expectedTpuTopology:  "2x2",
			expectedTpuMultihost: false,
		},
		"TPU v5 Lite Device; SingleHost '2x4'": {
			machineType: "ct5l-hightpu-8t",
			systemLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV5LiteDeviceValue,
				gkelabels.TPUTopologyLabel:      "2x4",
				gkelabels.AcceleratorCountLabel: "8",
			},
			extraResources: map[string]resource.Quantity{
				tpu.ResourceGoogleTPU: resource.MustParse("8"),
			},
			expectedSpecLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV5LiteDeviceValue,
				gkelabels.TPUTopologyLabel:      "2x4",
				gkelabels.AcceleratorCountLabel: "8",
			},
			expectedTpuType:      gkelabels.TpuV5LiteDeviceValue,
			expectedTpuTopology:  "2x4",
			expectedTpuMultihost: false,
		},
		"TPU v5 Lite Device; SingleHost '2x4' without full pod request": {
			machineType: "ct5l-hightpu-8t",
			systemLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV5LiteDeviceValue,
				gkelabels.TPUTopologyLabel:      "2x4",
				gkelabels.AcceleratorCountLabel: "8",
			},
			extraResources: map[string]resource.Quantity{
				tpu.ResourceGoogleTPU: resource.MustParse("4"),
			},
			expectedSpecLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV5LiteDeviceValue,
				gkelabels.TPUTopologyLabel:      "2x4",
				gkelabels.AcceleratorCountLabel: "8",
			},
			expectedTpuType:      gkelabels.TpuV5LiteDeviceValue,
			expectedTpuTopology:  "2x4",
			expectedTpuMultihost: false,
		},
		"TPU v5 Lite Podslice; SingleHost '1x1'": {
			machineType: "ct5lp-hightpu-1t",
			systemLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV5LitePodsliceValue,
				gkelabels.TPUTopologyLabel:      "1x1",
				gkelabels.AcceleratorCountLabel: "1",
			},
			extraResources: map[string]resource.Quantity{
				tpu.ResourceGoogleTPU: resource.MustParse("1"),
			},
			expectedSpecLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV5LitePodsliceValue,
				gkelabels.TPUTopologyLabel:      "1x1",
				gkelabels.AcceleratorCountLabel: "1",
			},
			expectedTpuType:      gkelabels.TpuV5LitePodsliceValue,
			expectedTpuTopology:  "1x1",
			expectedTpuMultihost: false,
		},
		"TPU v5 Lite Podslice; SingleHost '2x2'": {
			machineType: "ct5lp-hightpu-4t",
			systemLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV5LitePodsliceValue,
				gkelabels.TPUTopologyLabel:      "2x2",
				gkelabels.AcceleratorCountLabel: "4",
			},
			extraResources: map[string]resource.Quantity{
				tpu.ResourceGoogleTPU: resource.MustParse("4"),
			},
			expectedSpecLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV5LitePodsliceValue,
				gkelabels.TPUTopologyLabel:      "2x2",
				gkelabels.AcceleratorCountLabel: "4",
			},
			expectedTpuType:      gkelabels.TpuV5LitePodsliceValue,
			expectedTpuTopology:  "2x2",
			expectedTpuMultihost: false,
		},
		"TPU v5 Lite Podslice; SingleHost '2x4'": {
			machineType: "ct5lp-hightpu-8t",
			systemLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV5LitePodsliceValue,
				gkelabels.TPUTopologyLabel:      "2x4",
				gkelabels.AcceleratorCountLabel: "8",
			},
			extraResources: map[string]resource.Quantity{
				tpu.ResourceGoogleTPU: resource.MustParse("8"),
			},
			expectedSpecLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV5LitePodsliceValue,
				gkelabels.TPUTopologyLabel:      "2x4",
				gkelabels.AcceleratorCountLabel: "8",
			},
			expectedTpuType:      gkelabels.TpuV5LitePodsliceValue,
			expectedTpuTopology:  "2x4",
			expectedTpuMultihost: false,
		},
		"TPU v5 Lite Podslice; MultiHost '2x4'": {
			machineType: "ct5lp-hightpu-4t",
			systemLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV5LitePodsliceValue,
				gkelabels.TPUTopologyLabel:      "2x4",
				gkelabels.AcceleratorCountLabel: "4",
			},
			extraResources: map[string]resource.Quantity{
				tpu.ResourceGoogleTPU: resource.MustParse("4"),
			},
			expectedSpecLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV5LitePodsliceValue,
				gkelabels.TPUTopologyLabel:      "2x4",
				gkelabels.AcceleratorCountLabel: "4",
			},
			expectedTpuType:      gkelabels.TpuV5LitePodsliceValue,
			expectedTpuTopology:  "2x4",
			expectedTpuMultihost: true,
		},
		"TPU v5 Lite Podslice; MultiHost": {
			machineType: "ct5lp-hightpu-4t",
			systemLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV5LitePodsliceValue,
				gkelabels.TPUTopologyLabel:      "4x4",
				gkelabels.AcceleratorCountLabel: "4",
			},
			extraResources: map[string]resource.Quantity{
				tpu.ResourceGoogleTPU: resource.MustParse("4"),
			},
			expectedSpecLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV5LitePodsliceValue,
				gkelabels.TPUTopologyLabel:      "4x4",
				gkelabels.AcceleratorCountLabel: "4",
			},
			expectedTpuType:      gkelabels.TpuV5LitePodsliceValue,
			expectedTpuTopology:  "4x4",
			expectedTpuMultihost: true,
		},
		"TPU v5 Lite Podslice; no accelerator count specified": {
			machineType: "ct5lp-hightpu-4t",
			systemLabels: map[string]string{
				gkelabels.TPULabel:         gkelabels.TpuV5LitePodsliceValue,
				gkelabels.TPUTopologyLabel: "4x4",
			},
			extraResources: map[string]resource.Quantity{
				tpu.ResourceGoogleTPU: resource.MustParse("4"),
			},
			expectedSpecLabels: map[string]string{
				gkelabels.TPULabel:              gkelabels.TpuV5LitePodsliceValue,
				gkelabels.TPUTopologyLabel:      "4x4",
				gkelabels.AcceleratorCountLabel: "4",
			},
			expectedTpuType:      gkelabels.TpuV5LitePodsliceValue,
			expectedTpuTopology:  "4x4",
			expectedTpuMultihost: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			// Attach additional system gkelabels.
			tc.systemLabels[apiv1.LabelZoneFailureDomain] = zone

			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			trg := NewTpuRequestGenerator(provider)
			spec := &gkeclient.NodePoolSpec{
				MachineType: tc.machineType,
				Labels:      map[string]string{},
			}
			err := trg.UpdateNodePoolSpec(spec, tc.systemLabels, tc.extraResources)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedSpecLabels, spec.Labels)
			assert.Equal(t, tc.expectedTpuType, spec.TpuType)
			assert.Equal(t, tc.expectedTpuTopology, spec.TpuTopology)
			assert.Equal(t, tc.expectedTpuMultihost, spec.TpuMultiHost)
		})
	}
}

func TestGpuRequestGenerator_UpdateNodePoolSpec(t *testing.T) {
	zone := "us-west1-b"
	machineType := "n1-standard-1"

	for name, tc := range map[string]struct {
		autopilotEnabled  bool
		systemLabels      map[string]string
		wantSpecLabels    map[string]string
		wantDriverVersion string
	}{
		"autopilot enabled; labels not empty": {
			autopilotEnabled: true,
			systemLabels: map[string]string{
				gkelabels.GPULabel:                 machinetypes.NvidiaA100_80gb.Name(),
				gkelabels.GPUDriverVersionLabel:    "latest",
				gkelabels.GPUPartitionSizeLabel:    "1g.10gb",
				gkelabels.GPUMaxSharedClientsLabel: "8",
				gkelabels.GPUSharingStrategyLabel:  gkelabels.GPUTimeSharingStrategy,
			},
			wantSpecLabels: map[string]string{
				gkelabels.GPULabel:                 machinetypes.NvidiaA100_80gb.Name(),
				gkelabels.GPUPartitionSizeLabel:    "1g.10gb",
				gkelabels.GPUMaxSharedClientsLabel: "8",
				gkelabels.GPUDriverVersionLabel:    "latest",
				gkelabels.GPUSharingStrategyLabel:  gkelabels.GPUTimeSharingStrategy,
				gkelabels.AcceleratorCountLabel:    "1",
			},
		},
		"managed nodes; labels not empty, should fill accelerator count label": {
			autopilotEnabled: false,
			systemLabels: map[string]string{
				gkelabels.ManagedNodeLabel:         "true",
				gkelabels.GPULabel:                 machinetypes.NvidiaA100_80gb.Name(),
				gkelabels.GPUDriverVersionLabel:    "latest",
				gkelabels.GPUPartitionSizeLabel:    "1g.10gb",
				gkelabels.GPUMaxSharedClientsLabel: "8",
				gkelabels.GPUSharingStrategyLabel:  gkelabels.GPUTimeSharingStrategy,
			},
			wantSpecLabels: map[string]string{
				gkelabels.GPULabel:                 machinetypes.NvidiaA100_80gb.Name(),
				gkelabels.GPUDriverVersionLabel:    "latest",
				gkelabels.GPUPartitionSizeLabel:    "1g.10gb",
				gkelabels.GPUMaxSharedClientsLabel: "8",
				gkelabels.GPUSharingStrategyLabel:  gkelabels.GPUTimeSharingStrategy,
				gkelabels.AcceleratorCountLabel:    "1",
			},
		},
		"autopilot disabled; labels not empty": {
			autopilotEnabled: false,
			systemLabels: map[string]string{
				gkelabels.GPULabel:                 machinetypes.NvidiaA100_80gb.Name(),
				gkelabels.GPUPartitionSizeLabel:    "1g.10gb",
				gkelabels.GPUMaxSharedClientsLabel: "8",
				gkelabels.GPUDriverVersionLabel:    "latest",
				gkelabels.GPUSharingStrategyLabel:  gkelabels.GPUTimeSharingStrategy,
			},
			wantSpecLabels: map[string]string{
				gkelabels.GPULabel:                 machinetypes.NvidiaA100_80gb.Name(),
				gkelabels.GPUPartitionSizeLabel:    "1g.10gb",
				gkelabels.GPUMaxSharedClientsLabel: "8",
				gkelabels.GPUDriverVersionLabel:    "latest",
				gkelabels.GPUSharingStrategyLabel:  gkelabels.GPUTimeSharingStrategy,
			},
		},
		"autopilot enabled; labels empty": {
			autopilotEnabled: true,
			systemLabels: map[string]string{
				gkelabels.GPULabel:                 machinetypes.NvidiaA100_80gb.Name(),
				gkelabels.GPUPartitionSizeLabel:    "",
				gkelabels.GPUMaxSharedClientsLabel: "",
				gkelabels.GPUDriverVersionLabel:    "",
				gkelabels.GPUSharingStrategyLabel:  "",
			},
			wantSpecLabels: map[string]string{
				gkelabels.GPULabel:              machinetypes.NvidiaA100_80gb.Name(),
				gkelabels.AcceleratorCountLabel: "56",
			},
		},
		"autoinstall-disabled": {
			autopilotEnabled: true,
			systemLabels: map[string]string{
				gkelabels.GPULabel:              machinetypes.NvidiaA100_80gb.Name(),
				gkelabels.GPUDriverVersionLabel: gkelabels.DisabledGPUDriverVersionValue,
			},
			wantSpecLabels: map[string]string{
				gkelabels.GPULabel:              machinetypes.NvidiaA100_80gb.Name(),
				gkelabels.GPUDriverVersionLabel: "disabled",
				gkelabels.AcceleratorCountLabel: "56",
			},
			wantDriverVersion: "INSTALLATION_DISABLED",
		},
	} {
		t.Run(name, func(t *testing.T) {
			// Attach additional system gkelabels.
			tc.systemLabels[apiv1.LabelZoneFailureDomain] = zone

			extraResources := map[string]resource.Quantity{
				gpu.ResourceNvidiaGPU: resource.MustParse("56"),
			}

			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithAutopilotEnabled(tc.autopilotEnabled).
				WithMachineTypesPerZone(map[string][]string{zone: {machineType}}).
				Build()
			grg := NewGpuRequestGenerator(provider)
			spec := &gkeclient.NodePoolSpec{
				MachineType: machineType,
				Labels:      map[string]string{},
			}
			err := grg.UpdateNodePoolSpec(spec, tc.systemLabels, extraResources)
			assert.NoError(t, err)
			assert.Equal(t, spec.Taints, []apiv1.Taint{
				{
					Key:    gpu.ResourceNvidiaGPU,
					Value:  "present",
					Effect: apiv1.TaintEffectNoSchedule,
				},
			})
			assert.Equal(t, tc.wantSpecLabels, spec.Labels)
			assert.Len(t, spec.Accelerators, 1)
			if tc.wantDriverVersion != "" {
				assert.NotNil(t, spec.Accelerators[0].GpuDriverInstallationConfig)
				assert.Equal(t, tc.wantDriverVersion, spec.Accelerators[0].GpuDriverInstallationConfig.GpuDriverVersion)
			} else {
				assert.Nil(t, spec.Accelerators[0].GpuDriverInstallationConfig)
			}
		})
	}
}

func TestGpuRequestGenerator_UpdateNodePoolSpec_UnavailableGpu(t *testing.T) {
	zone := "us-west1-a"
	machineType := "n1-standard-1"

	// Test GPU validation
	systemLabels := map[string]string{
		gkelabels.GPULabel:           machinetypes.NvidiaTeslaT4.Name(),
		apiv1.LabelZoneFailureDomain: zone,
	}
	extraResources := map[string]resource.Quantity{
		gpu.ResourceNvidiaGPU: resource.MustParse("1"),
	}
	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithAutopilotEnabled(true).
		WithMachineTypesPerZone(map[string][]string{zone: {machineType}}).
		WithValidateGpuConfigErrors(map[string]error{machinetypes.NvidiaTeslaT4.Name(): cloudprovider.ErrIllegalConfiguration}).
		Build()
	grg := NewGpuRequestGenerator(provider)
	err := grg.UpdateNodePoolSpec(&gkeclient.NodePoolSpec{
		MachineType: machineType,
		Labels:      map[string]string{},
	}, systemLabels, extraResources)
	assert.Error(t, err)
}

// TODO(b/266688134): Merge TestMachineSelectionGenerator_UpdateNodePoolSpec.* tests into one.
func TestMachineSelectionGenerator_UpdateNodePoolSpec_Autopilot(t *testing.T) {
	for tn, tc := range map[string]struct {
		autopilotEnabled         bool
		isEkInAutopilotEnabled   bool
		confidentialNodesEnabled bool
		machineType              string
		computeClassName         string
		gpuType                  string
		tpuPresent               bool
		expectedClassName        string
		expectedCpuScaling       string
		expectedMemoryScaling    string
		expectedTaints           []apiv1.Taint
		expectedError            error
	}{
		"non-default family, autopilot disabled": {
			autopilotEnabled:      false,
			machineType:           "n1-standard-1",
			expectedCpuScaling:    "1",
			expectedMemoryScaling: "4",
			expectedTaints:        nil,
		},
		"non-default family, autopilot enabled": {
			autopilotEnabled:      true,
			machineType:           "n1-standard-1",
			expectedCpuScaling:    "1",
			expectedMemoryScaling: "4",
			expectedTaints:        []apiv1.Taint{{Key: gkelabels.MachineFamilyLabel, Value: "n1", Effect: apiv1.TaintEffectNoSchedule}},
		},
		"non-default family, GPU, taint injected": {
			machineType:           "a2-highgpu-1g",
			gpuType:               "nvidia-tesla-a100",
			expectedCpuScaling:    "12",
			expectedMemoryScaling: "91",
			expectedTaints: []apiv1.Taint{
				{
					Key:    gpu.ResourceNvidiaGPU,
					Value:  "present",
					Effect: apiv1.TaintEffectNoSchedule,
				},
			},
		},
		"non-default family, autopilot enabled, GPU, taint injected": {
			autopilotEnabled:      true,
			machineType:           "a2-highgpu-1g",
			gpuType:               "nvidia-tesla-a100",
			expectedCpuScaling:    "12",
			expectedMemoryScaling: "91",
			expectedTaints: []apiv1.Taint{
				{
					Key:    gpu.ResourceNvidiaGPU,
					Value:  "present",
					Effect: apiv1.TaintEffectNoSchedule,
				},
			},
		},
		"non-default family, no GPU type, taint injected": {
			machineType:           "a2-highgpu-1g",
			expectedCpuScaling:    "12",
			expectedMemoryScaling: "91",
			expectedTaints: []apiv1.Taint{
				{
					Key:    gpu.ResourceNvidiaGPU,
					Value:  "present",
					Effect: apiv1.TaintEffectNoSchedule,
				},
			},
		},
		"non-default family, autopilot enabled, TPU": {
			autopilotEnabled:      true,
			machineType:           "ct4l-hightpu-4t",
			tpuPresent:            true,
			expectedCpuScaling:    "48",
			expectedMemoryScaling: "365",
			expectedTaints:        []apiv1.Taint{},
		},
		"non-default family, autopilot enabled, Confidential Nodes": {
			autopilotEnabled:         true,
			confidentialNodesEnabled: true,
			machineType:              "n2d-standard-2",
			expectedCpuScaling:       "2",
			expectedMemoryScaling:    "8",
			expectedTaints:           []apiv1.Taint{},
		},
		"compute class, autopilot disabled (should not happen, sanity check)": {
			autopilotEnabled:      false,
			machineType:           "n1-standard-1",
			computeClassName:      "test-class",
			expectedClassName:     "",
			expectedCpuScaling:    "1",
			expectedMemoryScaling: "4",
			expectedTaints:        nil,
		},
		"compute class, autopilot enabled": {
			autopilotEnabled:      true,
			machineType:           "n1-standard-1",
			computeClassName:      "Balanced",
			expectedClassName:     "Balanced",
			expectedCpuScaling:    "1",
			expectedMemoryScaling: "4",
			expectedTaints:        []apiv1.Taint{{Key: gkelabels.ComputeClassLabel, Value: "Balanced", Effect: apiv1.TaintEffectNoSchedule}},
		},
		"Scale-Out compute class, autopilot enabled, amd64 architecture": {
			autopilotEnabled:      true,
			machineType:           "n1-standard-1",
			computeClassName:      "Scale-Out",
			expectedClassName:     "Scale-Out",
			expectedCpuScaling:    "1",
			expectedMemoryScaling: "4",
			expectedTaints: []apiv1.Taint{
				{Key: gkelabels.ComputeClassLabel, Value: "Scale-Out", Effect: apiv1.TaintEffectNoSchedule},
				{Key: apiv1.LabelArchStable, Value: "amd64", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
		"Scale-Out compute class, autopilot enabled, arm64 architecture": {
			autopilotEnabled:      true,
			machineType:           "t2a-standard-1",
			computeClassName:      "Scale-Out",
			expectedClassName:     "Scale-Out",
			expectedCpuScaling:    "1",
			expectedMemoryScaling: "4",
			expectedTaints: []apiv1.Taint{
				{Key: gkelabels.ComputeClassLabel, Value: "Scale-Out", Effect: apiv1.TaintEffectNoSchedule},
				{Key: apiv1.LabelArchStable, Value: "arm64", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
		"custom compute class, autopilot enabled": {
			autopilotEnabled:      true,
			machineType:           "n1-standard-1",
			computeClassName:      "custom-compute-class",
			expectedClassName:     "",
			expectedCpuScaling:    "1",
			expectedMemoryScaling: "4",
			expectedTaints:        []apiv1.Taint{},
		},
		"no compute class, autopilot enabled, amd64 architecture": {
			autopilotEnabled:      true,
			machineType:           "e2-standard-4",
			expectedCpuScaling:    "4",
			expectedMemoryScaling: "17",
			expectedTaints:        nil,
		},
		"no compute class, autopilot enabled, arm64 architecture": {
			autopilotEnabled:      true,
			machineType:           "t2a-standard-4",
			expectedCpuScaling:    "4",
			expectedMemoryScaling: "17",
			expectedTaints: []apiv1.Taint{
				{Key: apiv1.LabelArchStable, Value: "arm64", Effect: apiv1.TaintEffectNoSchedule},
				{Key: gkelabels.MachineFamilyLabel, Value: "t2a", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
		"default family, autopilot disabled": {
			autopilotEnabled:      false,
			machineType:           "e2-standard-4",
			expectedCpuScaling:    "4",
			expectedMemoryScaling: "17",
			expectedTaints:        nil,
		},
		"default family, autopilot enabled": {
			autopilotEnabled:      true,
			machineType:           "e2-standard-4",
			expectedCpuScaling:    "4",
			expectedMemoryScaling: "17",
			expectedTaints:        nil,
		},
		"wrong machine type specified (shouldn't happen, just a sanity check)": {
			autopilotEnabled: true,
			machineType:      "notavalidmachinetype",
			expectedError:    fmt.Errorf(`unknown machine type "notavalidmachinetype", unable to parse machine type "notavalidmachinetype"`),
		},
		"machine type from unknown family specified (shouldn't happen, just a sanity check)": {
			autopilotEnabled: true,
			machineType:      "x7-abc-1",
			expectedError:    fmt.Errorf(`unknown machine type "x7-abc-1", unsupported machine family "x7"`),
		},
		"EKs enabled, autopilot enabled, no taint": {
			autopilotEnabled:       true,
			isEkInAutopilotEnabled: true,
			machineType:            "ek-standard-32",
			expectedCpuScaling:     "32",
			expectedMemoryScaling:  "137",
			expectedTaints:         []apiv1.Taint{},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			extraResources := map[string]resource.Quantity{}
			systemLabels := map[string]string{}

			if tc.gpuType != "" {
				extraResources[gpu.ResourceNvidiaGPU] = resource.MustParse("1")
				systemLabels[gkelabels.GPULabel] = tc.gpuType
			}
			if tc.tpuPresent {
				extraResources[tpu.ResourceGoogleTPU] = resource.MustParse("4")
			}
			if tc.computeClassName != "" {
				systemLabels[gkelabels.ComputeClassLabel] = tc.computeClassName
			}
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithConfidentialNodesEnabled(tc.confidentialNodesEnabled).
				WithAutopilotEnabled(tc.autopilotEnabled).
				WithResizableVmInAutopilotEnabled(machinetypes.EK.Name(), tc.isEkInAutopilotEnabled).
				WithAutoprovisioningDefaultFamily(machinetypes.E2).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			msg := NewMachineSelectionGenerator(provider, machineselection.Selector{CloudProvider: provider}, nil)
			spec := &gkeclient.NodePoolSpec{
				MachineType: tc.machineType,
				Labels:      map[string]string{},
			}
			err := msg.UpdateNodePoolSpec(spec, systemLabels, extraResources)
			if tc.expectedError != nil {
				assert.Equal(t, tc.expectedError, err)
				return
			}
			assert.NoError(t, err)
			assert.ElementsMatch(t, tc.expectedTaints, spec.Taints)
			assert.Equal(t, tc.expectedClassName, spec.Labels[gkelabels.ComputeClassLabel])
			assert.Equal(t, tc.expectedClassName, spec.ComputeClass)
			assert.Equal(t, tc.expectedCpuScaling, spec.Labels[gkelabels.CpuScalingLevelLabel])
			assert.Equal(t, tc.expectedMemoryScaling, spec.Labels[gkelabels.MemoryScalingLevelLabel])
			assert.Equal(t, tc.machineType, spec.MachineType)
			assert.Nil(t, spec.Metadata)
		})
	}
}

func TestMachineSelectionGenerator_UpdateNodePoolSpec_Arm(t *testing.T) {
	for tn, tc := range map[string]struct {
		machineType             string
		expectedArchTargetLabel string
		expectedArch            gce.SystemArchitecture
		expectedTaint           *apiv1.Taint
		expectedError           error
	}{
		"valid arm arch present in systemLabels": {
			machineType:             "t2a-standard-1",
			expectedArchTargetLabel: "arm64",
			expectedArch:            gce.Arm64,
			expectedTaint: &apiv1.Taint{
				Key:    apiv1.LabelArchStable,
				Value:  gce.Arm64.Name(),
				Effect: apiv1.TaintEffectNoSchedule,
			},
		},
		"unknown machine family": {
			machineType:             "abc-standard-2",
			expectedArchTargetLabel: "",
			expectedError:           fmt.Errorf("unknown machine type \"abc-standard-2\", unsupported machine family \"abc\""),
		},
		"valid amd64 arch present in systemLabels": {
			machineType:             "n1-standard-1",
			expectedArchTargetLabel: "amd64",
			expectedArch:            gce.Amd64,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithAutopilotEnabled(true).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			msg := NewMachineSelectionGenerator(provider, machineselection.Selector{CloudProvider: provider}, nil)
			spec := &gkeclient.NodePoolSpec{
				MachineType: tc.machineType,
				Labels:      map[string]string{},
			}
			err := msg.UpdateNodePoolSpec(spec, map[string]string{}, map[string]resource.Quantity{})
			if tc.expectedError != nil {
				assert.Equal(t, tc.expectedError, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedArch, *spec.SystemArchitecture)
			if tc.expectedArchTargetLabel == "" {
				assert.NotContains(t, spec.Labels, apiv1.LabelArchStable)
			} else {
				assert.Equal(t, tc.expectedArchTargetLabel, spec.Labels[apiv1.LabelArchStable])
			}
			if tc.expectedTaint != nil {
				found := false
				for _, taint := range spec.Taints {
					if taint == *tc.expectedTaint {
						found = true
					}
				}
				assert.True(t, found)
			}
		})
	}
}

func TestMachineSelectionGenerator_UpdateNodePoolSpec_MinCpuPlatform(t *testing.T) {
	for tn, tc := range map[string]struct {
		platformSystemLabel  string
		expectedTargetLabels map[string]string
		expectedPlatform     string
		expectedError        error
	}{
		"valid min_cpu_platform present in systemLabels": {
			platformSystemLabel: "Intel_Haswell",
			expectedTargetLabels: map[string]string{
				"cloud.google.com/requested-min-cpu-platform":                "Intel_Haswell",
				"supported-cpu-platform.cloud.google.com/Intel_Haswell":      "true",
				"supported-cpu-platform.cloud.google.com/Intel_Ivy_Bridge":   "true",
				"supported-cpu-platform.cloud.google.com/Intel_Sandy_Bridge": "true",
			},
			expectedPlatform: "Intel Haswell",
		},
		"invalid min_cpu_platform present in systemLabels": {
			platformSystemLabel: "Unknown_Platform",
			expectedError:       fmt.Errorf("unknown min_cpu_platform \"Unknown_Platform\" in systemLabels - this should never happen"),
		},
		"min_cpu_platform not present in systemLabels": {
			platformSystemLabel:  "",
			expectedTargetLabels: nil,
			expectedPlatform:     "",
		},
	} {
		t.Run(tn, func(t *testing.T) {
			systemLabels := map[string]string{}
			if tc.platformSystemLabel != "" {
				systemLabels[gkelabels.RequestedMinCpuPlatformLabel] = tc.platformSystemLabel
			}
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithAutopilotEnabled(true).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			msg := NewMachineSelectionGenerator(provider, machineselection.Selector{CloudProvider: provider}, nil)
			spec := &gkeclient.NodePoolSpec{
				MachineType: "n1-standard-8",
				Labels:      map[string]string{},
			}
			err := msg.UpdateNodePoolSpec(spec, systemLabels, map[string]resource.Quantity{})
			if tc.expectedError != nil {
				assert.Equal(t, tc.expectedError, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedPlatform, spec.MinCpuPlatform)
			if len(tc.expectedTargetLabels) == 0 {
				assert.NotContains(t, spec.Labels, gkelabels.RequestedMinCpuPlatformLabel)
				for label := range spec.Labels {
					assert.False(t, strings.HasPrefix(label, gkelabels.SupportedCpuPlatformKeyPrefix))
				}
			} else {
				for key, val := range tc.expectedTargetLabels {
					assert.Equal(t, spec.Labels[key], val)
				}
			}
		})
	}
}

func TestMachineSelectionGenerator_GenerateNodeGroupOptionsForRequirements(t *testing.T) {
	familyM1Name := machinetypes.M1.Name()
	minCores := 90
	minMemoryGb := 2000
	hugepageSize1g := int64(2)

	allMachineTypesInTest := []string{
		"m1-ultramem-40", "m1-ultramem-80", "m1-ultramem-160", "m1-megamem-96",
		"a2-highgpu-1g", "a2-highgpu-2g", "a2-highgpu-4g", "a2-highgpu-8g", "a2-megagpu-16g",
		"a2-ultragpu-1g", "a2-ultragpu-2g", "a2-ultragpu-4g", "a2-ultragpu-8g",
		"ct4l-hightpu-4t", "a2-ultragpu-1g-nolssd",
		"m3-ultramem-32", "m3-ultramem-64", "m3-ultramem-128", "m3-megamem-64", "m3-megamem-128",
		"n1-custom-4-32768",
	}
	allMachineTypesPerZone := map[string][]string{
		"zone-1": allMachineTypesInTest,
		"zone-2": allMachineTypesInTest,
		"zone-3": allMachineTypesInTest,
	}

	for tn, tc := range map[string]struct {
		options                       []NodeGroupOptions
		requirements                  nodeGroupRequirements
		resizableMachineTypesProvider config.Provider[sets.Set[string]]
		machineTypesPerZone           map[string][]string
		wantOptions                   []NodeGroupOptions
	}{
		"no options passed": {
			requirements: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpec(machinetypes.NewMachineConfigProvider(nil).AllMachineFamilies(), machinetypes.AnyPlatform, "", ""),
			},
			machineTypesPerZone: allMachineTypesPerZone,
		},
		"single machine family": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
			},
			requirements: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.M1}, machinetypes.AnyPlatform, "", ""),
			},
			machineTypesPerZone: allMachineTypesPerZone,
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-1", MachineType: "m1-ultramem-40"},
				{Zone: "zone-1", MachineType: "m1-ultramem-80"},
				{Zone: "zone-1", MachineType: "m1-ultramem-160"},
				{Zone: "zone-1", MachineType: "m1-megamem-96"},
			},
		},
		"multiple machine families": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
			},
			requirements: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.A2, machinetypes.CT4L, machinetypes.M1}, machinetypes.AnyPlatform, "", ""),
			},
			machineTypesPerZone: allMachineTypesPerZone,
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-1", MachineType: "a2-highgpu-1g"},
				{Zone: "zone-1", MachineType: "a2-highgpu-2g"},
				{Zone: "zone-1", MachineType: "a2-highgpu-4g"},
				{Zone: "zone-1", MachineType: "a2-highgpu-8g"},
				{Zone: "zone-1", MachineType: "a2-megagpu-16g"},
				{Zone: "zone-1", MachineType: "a2-ultragpu-1g"},
				{Zone: "zone-1", MachineType: "a2-ultragpu-2g"},
				{Zone: "zone-1", MachineType: "a2-ultragpu-4g"},
				{Zone: "zone-1", MachineType: "a2-ultragpu-8g"},
				{Zone: "zone-1", MachineType: "ct4l-hightpu-4t"},
				{Zone: "zone-1", MachineType: "m1-ultramem-40"},
				{Zone: "zone-1", MachineType: "m1-ultramem-80"},
				{Zone: "zone-1", MachineType: "m1-ultramem-160"},
				{Zone: "zone-1", MachineType: "m1-megamem-96"},
			},
		},
		"multiple machine families with minCpuPlatform constraint": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
			},
			requirements: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.A2, machinetypes.CT4L, machinetypes.M1}, machinetypes.IntelSkylake, "", ""),
			},
			machineTypesPerZone: allMachineTypesPerZone,
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-1", MachineType: "m1-megamem-96"},
			},
		},
		"multiple machine families with gpu requirements": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
			},
			requirements: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.A2, machinetypes.CT4L, machinetypes.M1}, machinetypes.AnyPlatform, gkelabels.NvidiaTeslaA100, ""),
			},
			machineTypesPerZone: allMachineTypesPerZone,
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-1", MachineType: "a2-highgpu-1g"},
				{Zone: "zone-1", MachineType: "a2-highgpu-2g"},
				{Zone: "zone-1", MachineType: "a2-highgpu-4g"},
				{Zone: "zone-1", MachineType: "a2-highgpu-8g"},
				{Zone: "zone-1", MachineType: "a2-megagpu-16g"},
			},
		},
		"multiple machine families with tpu requirements": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
			},
			requirements: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.A2, machinetypes.CT4L, machinetypes.M1}, machinetypes.AnyPlatform, "", gkelabels.TpuV4LiteDeviceValue),
			},
			machineTypesPerZone: allMachineTypesPerZone,
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-1", MachineType: "ct4l-hightpu-4t"},
			},
		},
		"multiple options with different zones": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
				{Zone: "zone-2"},
				{Zone: "zone-3"},
			},
			requirements: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.M1}, machinetypes.AnyPlatform, "", ""),
			},
			machineTypesPerZone: allMachineTypesPerZone,
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-1", MachineType: "m1-ultramem-40"},
				{Zone: "zone-1", MachineType: "m1-ultramem-80"},
				{Zone: "zone-1", MachineType: "m1-ultramem-160"},
				{Zone: "zone-1", MachineType: "m1-megamem-96"},
				{Zone: "zone-2", MachineType: "m1-ultramem-40"},
				{Zone: "zone-2", MachineType: "m1-ultramem-80"},
				{Zone: "zone-2", MachineType: "m1-ultramem-160"},
				{Zone: "zone-2", MachineType: "m1-megamem-96"},
				{Zone: "zone-3", MachineType: "m1-ultramem-40"},
				{Zone: "zone-3", MachineType: "m1-ultramem-80"},
				{Zone: "zone-3", MachineType: "m1-ultramem-160"},
				{Zone: "zone-3", MachineType: "m1-megamem-96"},
			},
		},
		"compute class instance characteristics rule without constraints": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
			},
			requirements: nodeGroupRequirements{
				machineSpec:      machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.M1}, machinetypes.AnyPlatform, "", ""),
				computeClassRule: rules.NewMachineSpecRule(&familyM1Name, nil, nil, nil),
			},
			machineTypesPerZone: allMachineTypesPerZone,
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-1", MachineType: "m1-ultramem-40"},
				{Zone: "zone-1", MachineType: "m1-ultramem-80"},
				{Zone: "zone-1", MachineType: "m1-ultramem-160"},
				{Zone: "zone-1", MachineType: "m1-megamem-96"},
			},
		},
		"compute class instance characteristics rule with min cores constraints": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
			},
			requirements: nodeGroupRequirements{
				machineSpec:      machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.M1}, machinetypes.AnyPlatform, "", ""),
				computeClassRule: rules.NewMachineSpecRule(&familyM1Name, nil, &minCores, nil),
			},
			machineTypesPerZone: allMachineTypesPerZone,
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-1", MachineType: "m1-ultramem-160"},
				{Zone: "zone-1", MachineType: "m1-megamem-96"},
			},
		},
		"compute class instance characteristics rule with min memory constraints": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
			},
			requirements: nodeGroupRequirements{
				machineSpec:      machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.M1}, machinetypes.AnyPlatform, "", ""),
				computeClassRule: rules.NewMachineSpecRule(&familyM1Name, nil, nil, &minMemoryGb),
			},
			machineTypesPerZone: allMachineTypesPerZone,
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-1", MachineType: "m1-ultramem-160"},
			},
		},
		"explicit-req-only machine types filtered out": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
			},
			requirements: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.A2}, machinetypes.AnyPlatform, "", ""),
			},
			machineTypesPerZone: allMachineTypesPerZone,
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-1", MachineType: "a2-highgpu-1g"},
				{Zone: "zone-1", MachineType: "a2-highgpu-2g"},
				{Zone: "zone-1", MachineType: "a2-highgpu-4g"},
				{Zone: "zone-1", MachineType: "a2-highgpu-8g"},
				{Zone: "zone-1", MachineType: "a2-megagpu-16g"},
				{Zone: "zone-1", MachineType: "a2-ultragpu-1g"},
				{Zone: "zone-1", MachineType: "a2-ultragpu-2g"},
				{Zone: "zone-1", MachineType: "a2-ultragpu-4g"},
				{Zone: "zone-1", MachineType: "a2-ultragpu-8g"},
			},
		},
		"explicit-req-only machine types generated when requested": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
			},
			requirements: nodeGroupRequirements{
				machineSpec: machinetypes.NewExplicitMachineSpec([]machinetypes.MachineFamily{machinetypes.A2}, machinetypes.AnyPlatform, "", "", []string{"a2-ultragpu-1g-nolssd"}),
			},
			machineTypesPerZone: allMachineTypesPerZone,
			wantOptions:         []NodeGroupOptions{{Zone: "zone-1", MachineType: "a2-ultragpu-1g-nolssd"}},
		},
		"cc with 1-gigabyte-sized hugepages constraints - not supported in M1 family": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
			},
			requirements: nodeGroupRequirements{
				machineSpec:      machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.M1, machinetypes.M3}, machinetypes.AnyPlatform, "", ""),
				computeClassRule: rules.NewRule(rules.WithHugepageSize1gRule(hugepageSize1g)),
			},
			machineTypesPerZone: allMachineTypesPerZone,
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-1", MachineType: "m3-ultramem-32"},
				{Zone: "zone-1", MachineType: "m3-ultramem-64"},
				{Zone: "zone-1", MachineType: "m3-ultramem-128"},
				{Zone: "zone-1", MachineType: "m3-megamem-64"},
				{Zone: "zone-1", MachineType: "m3-megamem-128"},
			},
		},
		"cc with hugepages 2m constraint - exceedig hugepage memory limit on some M1 anmd M3 machine types": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
			},
			requirements: nodeGroupRequirements{
				machineSpec:      machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.M1, machinetypes.M3}, machinetypes.AnyPlatform, "", ""),
				computeClassRule: rules.NewRule(rules.WithHugepageSize2mRule(400000)),
			},
			machineTypesPerZone: allMachineTypesPerZone,
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-1", MachineType: "m1-ultramem-160"},
				{Zone: "zone-1", MachineType: "m1-megamem-96"},
				{Zone: "zone-1", MachineType: "m1-ultramem-80"},
				{Zone: "zone-1", MachineType: "m3-ultramem-64"},
				{Zone: "zone-1", MachineType: "m3-ultramem-128"},
				{Zone: "zone-1", MachineType: "m3-megamem-128"},
			},
		},
		"cc with hugepages 1g constraint - exceedig hugepage memory limit on some M3 machine types": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
			},
			requirements: nodeGroupRequirements{
				machineSpec:      machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.M1, machinetypes.M3}, machinetypes.AnyPlatform, "", ""),
				computeClassRule: rules.NewRule(rules.WithHugepageSize1gRule(800)),
			},
			machineTypesPerZone: allMachineTypesPerZone,
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-1", MachineType: "m3-ultramem-64"},
				{Zone: "zone-1", MachineType: "m3-ultramem-128"},
				{Zone: "zone-1", MachineType: "m3-megamem-128"},
			},
		},
		"cc with hugepages both 1g and 2m constraint - exceedig hugepage memory limit on some M3 machine types": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
			},
			requirements: nodeGroupRequirements{
				machineSpec:      machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.M1, machinetypes.M3}, machinetypes.AnyPlatform, "", ""),
				computeClassRule: rules.NewRule(rules.WithHugepageSize1gRule(600), rules.WithHugepageSize2mRule(310000)),
			},
			machineTypesPerZone: allMachineTypesPerZone,
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-1", MachineType: "m3-ultramem-128"},
				{Zone: "zone-1", MachineType: "m3-ultramem-64"},
				{Zone: "zone-1", MachineType: "m3-megamem-128"},
			},
		},
		"unsupported EM machine types filtered out": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
			},
			requirements: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.EK}, machinetypes.AnyPlatform, "", ""),
			},
			resizableMachineTypesProvider: config.NewCommaSeparatedStringSetProvider("ek-standard-2,ek-standard-32"),
			machineTypesPerZone:           allMachineTypesPerZone,
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-1", MachineType: "ek-standard-2"},
				{Zone: "zone-1", MachineType: "ek-standard-32"},
			},
		},
		"EM machine types filtered out if selection not enabled": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
			},
			requirements: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.EK}, machinetypes.AnyPlatform, "", ""),
			},
			machineTypesPerZone: allMachineTypesPerZone,
			wantOptions:         []NodeGroupOptions{},
		},
		"machine family with explicit custom machine type": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
			},
			requirements: nodeGroupRequirements{
				machineSpec: machinetypes.NewExplicitMachineSpec(
					[]machinetypes.MachineFamily{machinetypes.N1},
					machinetypes.AnyPlatform,
					"",
					"",
					[]string{"n1-custom-4-32768"}),
			},
			machineTypesPerZone: allMachineTypesPerZone,
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-1", MachineType: "n1-custom-4-32768"},
			},
		},
		"only some machine types are available per zone": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
				{Zone: "zone-2"},
				{Zone: "zone-3"},
			},
			requirements: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.A2}, machinetypes.AnyPlatform, "", ""),
			},
			machineTypesPerZone: map[string][]string{
				"zone-1": {"a2-highgpu-1g", "a2-highgpu-2g"},
				"zone-2": {"a2-highgpu-2g", "a2-highgpu-4g", "a2-megagpu-16g"},
				"zone-3": {"a2-highgpu-2g", "a2-megagpu-16g", "a2-ultragpu-4g"},
			},
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-1", MachineType: "a2-highgpu-1g"},
				{Zone: "zone-1", MachineType: "a2-highgpu-2g"},
				{Zone: "zone-2", MachineType: "a2-highgpu-2g"},
				{Zone: "zone-2", MachineType: "a2-highgpu-4g"},
				{Zone: "zone-2", MachineType: "a2-megagpu-16g"},
				{Zone: "zone-3", MachineType: "a2-highgpu-2g"},
				{Zone: "zone-3", MachineType: "a2-megagpu-16g"},
				{Zone: "zone-3", MachineType: "a2-ultragpu-4g"},
			},
		},
		"eviction memory available constraint": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
			},
			requirements: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.E2}, machinetypes.AnyPlatform, "", ""),
				kubeletConfig: &gke_api_beta.NodeKubeletConfig{
					EvictionSoft: &gke_api_beta.EvictionSignals{
						MemoryAvailable: "2Gi",
					},
				},
			},
			machineTypesPerZone: map[string][]string{
				"zone-1": {"e2-medium", "e2-standard-2", "e2-standard-4"},
			},
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-1", MachineType: "e2-standard-2"},
				{Zone: "zone-1", MachineType: "e2-standard-4"},
			},
		},
		"sysctl vm.overcommit_memory constraint": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
			},
			requirements: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.E2}, machinetypes.AnyPlatform, "", ""),
				computeClassRule: rules.NewRule(rules.WithSysctlsRule(map[string]string{
					"vm.overcommit_memory": "2",
				})),
			},
			machineTypesPerZone: map[string][]string{
				"zone-1": {"e2-medium", "e2-standard-2", "e2-standard-4"},
			},
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-1", MachineType: "e2-standard-4"},
			},
		},
		"resizable family machine types are filtered by resizable provider": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
			},
			requirements: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.E4A, machinetypes.EK}, machinetypes.AnyPlatform, "", ""),
			},
			resizableMachineTypesProvider: config.NewCommaSeparatedStringSetProvider("e4a-standard-8,e4a-standard-32,ek-standard-2,ek-standard-8"),
			machineTypesPerZone: map[string][]string{
				"zone-1": {"e4a-standard-8", "e4a-standard-16", "e4a-standard-32", "ek-standard-2", "ek-standard-4", "ek-standard-8"},
			},
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-1", MachineType: "e4a-standard-8"},
				{Zone: "zone-1", MachineType: "e4a-standard-32"},
				{Zone: "zone-1", MachineType: "ek-standard-2"},
				{Zone: "zone-1", MachineType: "ek-standard-8"},
			},
		},
		"memory manager static policy filters out unsupported machine types": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
			},
			requirements: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.A2, machinetypes.M1}, machinetypes.AnyPlatform, "", ""),
				kubeletConfig: &gke_api_beta.NodeKubeletConfig{
					MemoryManager: &gke_api_beta.MemoryManager{
						Policy: "Static",
					},
				},
			},
			machineTypesPerZone: allMachineTypesPerZone,
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-1", MachineType: "a2-highgpu-1g"},
				{Zone: "zone-1", MachineType: "a2-highgpu-2g"},
				{Zone: "zone-1", MachineType: "a2-highgpu-4g"},
				{Zone: "zone-1", MachineType: "a2-highgpu-8g"},
				{Zone: "zone-1", MachineType: "a2-megagpu-16g"},
				{Zone: "zone-1", MachineType: "a2-ultragpu-1g"},
				{Zone: "zone-1", MachineType: "a2-ultragpu-2g"},
				{Zone: "zone-1", MachineType: "a2-ultragpu-4g"},
				{Zone: "zone-1", MachineType: "a2-ultragpu-8g"},
			},
		},
		"topology manager policy filters out unsupported machine types": {
			options: []NodeGroupOptions{
				{Zone: "zone-1"},
			},
			requirements: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.A2, machinetypes.M1}, machinetypes.AnyPlatform, "", ""),
				kubeletConfig: &gke_api_beta.NodeKubeletConfig{
					TopologyManager: &gke_api_beta.TopologyManager{
						Policy: "best-effort",
					},
				},
			},
			machineTypesPerZone: allMachineTypesPerZone,
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-1", MachineType: "a2-highgpu-1g"},
				{Zone: "zone-1", MachineType: "a2-highgpu-2g"},
				{Zone: "zone-1", MachineType: "a2-highgpu-4g"},
				{Zone: "zone-1", MachineType: "a2-highgpu-8g"},
				{Zone: "zone-1", MachineType: "a2-megagpu-16g"},
				{Zone: "zone-1", MachineType: "a2-ultragpu-1g"},
				{Zone: "zone-1", MachineType: "a2-ultragpu-2g"},
				{Zone: "zone-1", MachineType: "a2-ultragpu-4g"},
				{Zone: "zone-1", MachineType: "a2-ultragpu-8g"},
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineTypesPerZone(tc.machineTypesPerZone).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			msg := NewMachineSelectionGenerator(provider, machineselection.Selector{CloudProvider: provider}, tc.resizableMachineTypesProvider)
			assert.ElementsMatch(t, tc.wantOptions, msg.GenerateNodeGroupOptionsForRequirements(tc.options, tc.requirements))
		})
	}
}

func TestPlacementGroupGenerator_UpdateNodePoolSpec(t *testing.T) {
	const (
		policyName                        = "policy0"
		tpuTopology                       = "15x15"
		tpuMachineWithSliceSupport        = "tpu7x-standard-1t"
		gpuMachineWithSliceSupport        = "a4x-highgpu-4g"
		tpuMachineTypeWithoutSliceSupport = "ct5l-hightpu-1t"
		cpuMachineWithoutSliceSupport     = "e2-standard-2"
	)

	resourcePolicyNoTopology := &gceclient.GceResourcePolicy{
		Name: policyName,
	}

	resourcePolicyWithTopology := &gceclient.GceResourcePolicy{
		Name:           policyName,
		WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: tpuTopology},
	}

	for name, tc := range map[string]struct {
		compactPlacementEnabled bool
		systemLabels            map[string]string
		inputSpec               *gkeclient.NodePoolSpec
		pullerPolicies          []*gceclient.GceResourcePolicy
		expectLabels            map[string]string
		expectPlacementGroup    placement.Spec
		expectErr               bool
	}{
		"happy_path_compact_placement_with_topology": {
			compactPlacementEnabled: true,
			systemLabels: map[string]string{
				gkelabels.PlacementGroupLabel: "group0",
				gkelabels.PolicyLabel:         policyName,
			},
			inputSpec: &gkeclient.NodePoolSpec{
				MachineType: tpuMachineTypeWithoutSliceSupport,
				Labels:      map[string]string{},
			},
			pullerPolicies: []*gceclient.GceResourcePolicy{resourcePolicyWithTopology},
			expectLabels: map[string]string{
				gkelabels.PlacementGroupLabel: "group0",
				gkelabels.PolicyLabel:         policyName,
			},
			expectPlacementGroup: placement.Spec{
				GroupId:        "group0",
				Policy:         policyName,
				ResourcePolicy: resourcePolicyWithTopology,
			},
		},
		"happy_path_compact_placement_no_topology": {
			compactPlacementEnabled: true,
			systemLabels: map[string]string{
				gkelabels.PlacementGroupLabel: "group0",
				gkelabels.PolicyLabel:         policyName,
			},
			inputSpec: &gkeclient.NodePoolSpec{
				MachineType: tpuMachineTypeWithoutSliceSupport,
				Labels:      map[string]string{},
			},
			pullerPolicies: []*gceclient.GceResourcePolicy{resourcePolicyNoTopology},
			expectLabels: map[string]string{
				gkelabels.PlacementGroupLabel: "group0",
				gkelabels.PolicyLabel:         policyName,
			},
			expectPlacementGroup: placement.Spec{
				GroupId:        "group0",
				Policy:         policyName,
				ResourcePolicy: resourcePolicyNoTopology,
			},
		},
		"feature_disabled_systemLabels_not_updated": {
			compactPlacementEnabled: false,
			systemLabels: map[string]string{
				gkelabels.PlacementGroupLabel: "group0",
				gkelabels.PolicyLabel:         policyName,
			},
			inputSpec: &gkeclient.NodePoolSpec{
				MachineType: tpuMachineTypeWithoutSliceSupport,
				Labels:      map[string]string{},
			},
			pullerPolicies: []*gceclient.GceResourcePolicy{resourcePolicyWithTopology},
			expectLabels:   map[string]string{}, // No spec labels updated
			expectPlacementGroup: placement.Spec{
				GroupId:        "group0",
				Policy:         policyName,
				ResourcePolicy: resourcePolicyWithTopology,
			},
		},
		"no_placement_labels_does_nothing": {
			compactPlacementEnabled: true,
			systemLabels:            map[string]string{},
			inputSpec: &gkeclient.NodePoolSpec{
				MachineType: tpuMachineTypeWithoutSliceSupport,
				Labels:      map[string]string{},
			},
			expectLabels:         map[string]string{},
			expectPlacementGroup: placement.Spec{},
		},
		"missing_resource_policy_no_backfill_cpu_machine": {
			compactPlacementEnabled: true,
			systemLabels: map[string]string{
				gkelabels.PolicyLabel: policyName,
			},
			inputSpec: &gkeclient.NodePoolSpec{
				MachineType: cpuMachineWithoutSliceSupport,
				Labels:      map[string]string{},
			},
			pullerPolicies: nil, // Policy missing
			expectLabels: map[string]string{
				gkelabels.PolicyLabel: policyName,
			},
			expectPlacementGroup: placement.Spec{
				Policy: policyName,
			},
		},
		"gpu_missing_policy_returns_error": {
			compactPlacementEnabled: true,
			systemLabels: map[string]string{
				gkelabels.PolicyLabel: policyName,
			},
			inputSpec: &gkeclient.NodePoolSpec{
				MachineType: gpuMachineWithSliceSupport,
				Labels:      map[string]string{},
			},
			pullerPolicies: nil, // Policy missing
			expectErr:      true,
		},
		"tpu_missing_policy_with_topology_label_backfills_policy": {
			compactPlacementEnabled: true,
			systemLabels: map[string]string{
				gkelabels.PolicyLabel: policyName,
			},
			inputSpec: &gkeclient.NodePoolSpec{
				MachineType: tpuMachineWithSliceSupport,
				Labels: map[string]string{
					gkelabels.TPUTopologyLabel: tpuTopology,
				},
			},
			pullerPolicies: nil, // Policy missing
			expectLabels: map[string]string{
				gkelabels.PolicyLabel:      policyName,
				gkelabels.TPUTopologyLabel: tpuTopology,
			},
			expectPlacementGroup: placement.Spec{
				Policy: policyName,
				// policy created from label
				ResourcePolicy: &gceclient.GceResourcePolicy{
					Name:           policyName,
					WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: tpuTopology},
				},
			},
		},
		"happy_path_compact_placement_with_topology_label_and_existing_compct_policy_with_no_toplogy": {
			compactPlacementEnabled: true,
			systemLabels: map[string]string{
				gkelabels.PolicyLabel: policyName,
			},
			inputSpec: &gkeclient.NodePoolSpec{
				MachineType: tpuMachineTypeWithoutSliceSupport,
				Labels: map[string]string{
					gkelabels.TPUTopologyLabel: tpuTopology,
				},
			},
			pullerPolicies: []*gceclient.GceResourcePolicy{resourcePolicyNoTopology},
			expectLabels: map[string]string{
				gkelabels.PolicyLabel:      policyName,
				gkelabels.TPUTopologyLabel: tpuTopology,
			},
			// Expect the existing policy from puller, NOT a backfilled one
			expectPlacementGroup: placement.Spec{
				Policy:         policyName,
				ResourcePolicy: resourcePolicyNoTopology,
			},
		},
		"tpu_missing_policy_no_topology_multi_host_error": {
			compactPlacementEnabled: true,
			systemLabels: map[string]string{
				gkelabels.PolicyLabel: policyName,
			},
			inputSpec: &gkeclient.NodePoolSpec{
				TpuMultiHost: true,
				MachineType:  tpuMachineWithSliceSupport,
				Labels:       map[string]string{}, // No topology label
			},
			pullerPolicies: nil, // Policy missing
			expectErr:      true,
		},
		"tpu_missing_policy_no_topology_single_host": {
			compactPlacementEnabled: true,
			systemLabels: map[string]string{
				gkelabels.PolicyLabel: policyName,
			},
			inputSpec: &gkeclient.NodePoolSpec{
				MachineType: tpuMachineWithSliceSupport,
				Labels:      map[string]string{}, // No topology label
			},
			pullerPolicies: nil, // Policy missing
			expectLabels: map[string]string{
				gkelabels.PolicyLabel: policyName,
			},
			expectPlacementGroup: placement.Spec{
				Policy: policyName,
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithAutopilotEnabled(true).
				WithCompactPlacementEnabled(tc.compactPlacementEnabled).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()

			pgg := NewPlacementGroupGenerator(provider, placement.NewFakeResourcePolicyPullerProvider(tc.pullerPolicies, nil))

			err := pgg.UpdateNodePoolSpec(tc.inputSpec, tc.systemLabels, nil)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectLabels, tc.inputSpec.Labels)
				assert.Equal(t, tc.expectPlacementGroup, tc.inputSpec.PlacementGroup)
			}
		})
	}
}

func TestPlacementGroupGenerator_ValidateRequirements(t *testing.T) {
	const (
		policyName = "policy-1"
	)
	var (
		mfWithoutCompactPlacement     = machinetypes.N1
		mfWithCompactPlacement        = machinetypes.G2
		mfWithBYOResourcePolicyReqGPU = machinetypes.A4X
		mfWithBYOResourcePolicyReqTPU = machinetypes.TPU7X
		resourcePolicyWithTopology    = &gceclient.GceResourcePolicy{
			Name: policyName,
			WorkloadPolicy: gceclient.WorkloadPolicy{
				AcceleratorTopology: "2x2",
			},
		}
		resourcePolicyWithoutTopology = &gceclient.GceResourcePolicy{
			Name: policyName,
		}
	)

	for tn, tc := range map[string]struct {
		machineFamily        machinetypes.MachineFamily
		machineSelectionType machinetypes.SelectionType
		placementGroup       placement.Spec
		resourcePolicies     []*gceclient.GceResourcePolicy
		wantErr              bool
	}{
		"no_placement_used": {
			machineFamily:  mfWithoutCompactPlacement,
			placementGroup: placement.Spec{},
		},
		"default_selection_type_with_placement": {
			machineFamily:        mfWithCompactPlacement,
			machineSelectionType: machinetypes.SelectionTypeDefault,
			placementGroup:       placement.Spec{GroupId: "group"},
			wantErr:              true,
		},
		"unsupported_machine_family": {
			machineFamily:        mfWithoutCompactPlacement,
			machineSelectionType: machinetypes.SelectionTypeSpecified,
			placementGroup:       placement.Spec{GroupId: "group"},
			wantErr:              true,
		},
		"supported_machine_family": {
			machineFamily:        mfWithCompactPlacement,
			machineSelectionType: machinetypes.SelectionTypeSpecified,
			placementGroup:       placement.Spec{Policy: policyName},
		},
		"tpu_slice_missing_resource_policy_error": {
			machineFamily:        mfWithBYOResourcePolicyReqTPU,
			machineSelectionType: machinetypes.SelectionTypeSpecified,
			placementGroup:       placement.Spec{Policy: policyName},
			resourcePolicies:     nil,
			wantErr:              true,
		},
		"gpu_slice_missing_resource_policy": {
			machineFamily:        machinetypes.A4X,
			machineSelectionType: machinetypes.SelectionTypeSpecified,
			placementGroup:       placement.Spec{Policy: policyName},
			resourcePolicies:     nil,
			wantErr:              true,
		},
		"gpu_slice_with_resource_policy_ok": {
			machineFamily:        mfWithBYOResourcePolicyReqGPU,
			machineSelectionType: machinetypes.SelectionTypeSpecified,
			placementGroup:       placement.Spec{Policy: policyName},
			resourcePolicies:     []*gceclient.GceResourcePolicy{resourcePolicyWithTopology},
		},
		"gpu_slice_with_resource_policy_without_topology": {
			machineFamily:        mfWithBYOResourcePolicyReqGPU,
			machineSelectionType: machinetypes.SelectionTypeSpecified,
			placementGroup:       placement.Spec{Policy: policyName},
			resourcePolicies:     []*gceclient.GceResourcePolicy{resourcePolicyWithoutTopology},
			wantErr:              true,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().Build()
			pgg := NewPlacementGroupGenerator(provider, placement.NewFakeResourcePolicyPullerProvider(tc.resourcePolicies, nil))

			ngReq := &nodeGroupRequirements{
				machineSpec:          machinetypes.NewMachineSpecSingleFamily(tc.machineFamily, machinetypes.AnyPlatform, "", ""),
				machineSelectionType: tc.machineSelectionType,
				placementGroup:       tc.placementGroup,
			}

			err := pgg.ValidateRequirements(ngReq)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestComputeClassGenerator_UpdateParameters(t *testing.T) {
	t.Parallel()
	ccLabel := "cc-label"
	ccName := "cc-name"
	machineFamilyName := "machine-family"
	podFamilyName := "pod-family"

	for name, tc := range map[string]struct {
		cc                             computeclass.CRD
		ruleIndex                      *int
		existingTaints                 []apiv1.Taint
		autopilotEnabled               bool
		disableComputeClassMinCapacity bool
		expectedLabels                 map[string]string
		expectedTaints                 []apiv1.Taint
	}{
		"GKE Standard, empty CRD without rule": {
			cc: computeclass.NewTestCrd(
				computeclass.WithName(ccName),
				computeclass.WithLabel(ccLabel),
			),
			expectedLabels: map[string]string{
				ccLabel:                                ccName,
				labelComputeClassRequired:              "true",
				gkelabels.ComputeClassPriorityIdxLabel: "-1",
			},
		},
		"GKE Autopilot, empty CRD without rule": {
			cc: computeclass.NewTestCrd(
				computeclass.WithName(ccName),
				computeclass.WithLabel(ccLabel),
			),
			autopilotEnabled: true,
			expectedLabels: map[string]string{
				ccLabel:                                ccName,
				labelComputeClassRequired:              "true",
				gkelabels.PodsPerNodeKey:               gkelabels.BinpackedSliceOfHardwareValue,
				gkelabels.ComputeClassPriorityIdxLabel: "-1",
			},
		},
		"GKE Autopilot, CRD with machine family rule": {
			cc: computeclass.NewTestCrd(
				computeclass.WithName(ccName),
				computeclass.WithLabel(ccLabel),
				computeclass.WithRules([]rules.Rule{
					rules.NewRule(rules.WithMachineFamilyRule(&machineFamilyName)),
				}),
			),
			ruleIndex:        ptr.To(0),
			autopilotEnabled: true,
			expectedLabels: map[string]string{
				ccLabel:                                ccName,
				labelComputeClassRequired:              "true",
				gkelabels.PodsPerNodeKey:               gkelabels.BinpackedSliceOfHardwareValue,
				gkelabels.ComputeClassPriorityIdxLabel: "0",
			},
		},
		"GKE Autopilot, CRD with pod family rule": {
			cc: computeclass.NewTestCrd(
				computeclass.WithName(ccName),
				computeclass.WithLabel(ccLabel),
				computeclass.WithRules([]rules.Rule{
					rules.NewRule(rules.WithPodFamilyRule(&podFamilyName)),
				}),
			),
			ruleIndex:        ptr.To(0),
			autopilotEnabled: true,
			expectedLabels: map[string]string{
				ccLabel:                                ccName,
				labelComputeClassRequired:              "true",
				gkelabels.ComputeClassPriorityIdxLabel: "0",
			},
		},
		"GKE Standard, empty Autopilot-managed CRD without rule": {
			cc: computeclass.NewTestCrd(
				computeclass.WithName(ccName),
				computeclass.WithLabel(ccLabel),
				computeclass.WithAutopilotManaged(),
			),
			expectedLabels: map[string]string{
				ccLabel:                                ccName,
				labelComputeClassRequired:              "true",
				gkelabels.ManagedNodeLabel:             "true",
				gkelabels.PodsPerNodeKey:               gkelabels.BinpackedSliceOfHardwareValue,
				gkelabels.ComputeClassPriorityIdxLabel: "-1",
			},
		},
		"GKE Standard, empty Autopilot-managed CRD without rule, with default Autopilot-managed settings": {
			cc: computeclass.NewTestCrd(
				computeclass.WithName(ccName),
				computeclass.WithLabel(ccLabel),
				computeclass.WithAutopilotManaged(),
				computeclass.WithDynamicBootDiskSizeEnabled(),   // This setting is enabled for CC by default if autopilot flag is true
				computeclass.WithDynamicMaxPodsPerNodeEnabled(), // This setting is enabled for CC by default if autopilot flag is true
			),
			expectedLabels: map[string]string{
				ccLabel:                    ccName,
				labelComputeClassRequired:  "true",
				gkelabels.ManagedNodeLabel: "true",
				gkelabels.PodsPerNodeKey:   gkelabels.BinpackedSliceOfHardwareValue,
				gkelabels.NodeGroupDynamicBootDiskSizeEnabledLabelKey: "true",
				gkelabels.ComputeClassPriorityIdxLabel:                "-1",
			},
		},
		"GKE Standard, Autopilot-managed CRD with machine family rule": {
			cc: computeclass.NewTestCrd(
				computeclass.WithName(ccName),
				computeclass.WithLabel(ccLabel),
				computeclass.WithAutopilotManaged(),
				computeclass.WithRules([]rules.Rule{
					rules.NewRule(rules.WithMachineFamilyRule(&machineFamilyName)),
				}),
			),
			ruleIndex: ptr.To(0),
			expectedLabels: map[string]string{
				ccLabel:                                ccName,
				labelComputeClassRequired:              "true",
				gkelabels.ManagedNodeLabel:             "true",
				gkelabels.PodsPerNodeKey:               gkelabels.BinpackedSliceOfHardwareValue,
				gkelabels.ComputeClassPriorityIdxLabel: "0",
			},
		},
		"GKE Standard, Autopilot-managed CRD with pod family rule": {
			cc: computeclass.NewTestCrd(
				computeclass.WithName(ccName),
				computeclass.WithLabel(ccLabel),
				computeclass.WithAutopilotManaged(),
				computeclass.WithRules([]rules.Rule{
					rules.NewRule(rules.WithPodFamilyRule(&podFamilyName)),
				}),
			),
			ruleIndex: ptr.To(0),
			expectedLabels: map[string]string{
				ccLabel:                                ccName,
				labelComputeClassRequired:              "true",
				gkelabels.ManagedNodeLabel:             "true",
				gkelabels.ComputeClassPriorityIdxLabel: "0",
			},
		},
		"Service account is specified": {
			cc: computeclass.NewTestCrd(
				computeclass.WithName(ccName),
				computeclass.WithLabel(ccLabel),
				computeclass.WithServiceAccount("some-service-account"),
			),
			expectedLabels: map[string]string{
				ccLabel:                                ccName,
				labelComputeClassRequired:              "true",
				gkelabels.ServiceAccountLabelKey:       "some-service-account",
				gkelabels.ComputeClassPriorityIdxLabel: "-1",
			},
		},
		"DRA TPU label is specified": {
			cc: computeclass.NewTestCrd(
				computeclass.WithName(ccName),
				computeclass.WithLabel(ccLabel),
				computeclass.WithTpuDriverMode(computeclass.TpuDriverModeDynamicResourceAllocation),
			),
			expectedLabels: map[string]string{
				ccLabel:                                ccName,
				labelComputeClassRequired:              "true",
				gkelabels.DraTpuNodeLabel:              "true",
				gkelabels.ComputeClassPriorityIdxLabel: "-1",
			},
		},
		"Image type is specified": {
			cc: computeclass.NewTestCrd(
				computeclass.WithName(ccName),
				computeclass.WithLabel(ccLabel),
				computeclass.WithImageType("some-image-type"),
			),
			expectedLabels: map[string]string{
				ccLabel:                                ccName,
				labelComputeClassRequired:              "true",
				gkelabels.ImageTypeLabelKey:            "some-image-type",
				gkelabels.ComputeClassPriorityIdxLabel: "-1",
			},
		},
		"Spec taints are specified": {
			cc: computeclass.NewTestCrd(
				computeclass.WithName(ccName),
				computeclass.WithLabel(ccLabel),
				computeclass.WithUserDefinedTaints([]apiv1.Taint{
					{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
				}),
			),
			expectedLabels: map[string]string{
				ccLabel:                                ccName,
				labelComputeClassRequired:              "true",
				gkelabels.ComputeClassPriorityIdxLabel: "-1",
			},
			expectedTaints: []apiv1.Taint{
				{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
		"Rule taints are specified": {
			cc: computeclass.NewTestCrd(
				computeclass.WithName(ccName),
				computeclass.WithLabel(ccLabel),
				computeclass.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithTaintsRule([]apiv1.Taint{
							{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
						}),
					),
				}),
			),
			ruleIndex: ptr.To(0),
			expectedLabels: map[string]string{
				ccLabel:                                ccName,
				labelComputeClassRequired:              "true",
				gkelabels.ComputeClassPriorityIdxLabel: "0",
			},
			expectedTaints: []apiv1.Taint{
				{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
		"Spec and rule taints are specified, not conflicting": {
			cc: computeclass.NewTestCrd(
				computeclass.WithName(ccName),
				computeclass.WithLabel(ccLabel),
				computeclass.WithUserDefinedTaints([]apiv1.Taint{
					{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
				}),
				computeclass.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithTaintsRule([]apiv1.Taint{
							{Key: "k2", Value: "v2", Effect: apiv1.TaintEffectNoSchedule},
						}),
					),
				}),
			),
			ruleIndex: ptr.To(0),
			expectedLabels: map[string]string{
				ccLabel:                                ccName,
				labelComputeClassRequired:              "true",
				gkelabels.ComputeClassPriorityIdxLabel: "0",
			},
			expectedTaints: []apiv1.Taint{
				{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
				{Key: "k2", Value: "v2", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
		"Spec and rule taints are specified, same value": {
			cc: computeclass.NewTestCrd(
				computeclass.WithName(ccName),
				computeclass.WithLabel(ccLabel),
				computeclass.WithUserDefinedTaints([]apiv1.Taint{
					{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
				}),
				computeclass.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithTaintsRule([]apiv1.Taint{
							{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
						}),
					),
				}),
			),
			ruleIndex: ptr.To(0),
			expectedLabels: map[string]string{
				ccLabel:                                ccName,
				labelComputeClassRequired:              "true",
				gkelabels.ComputeClassPriorityIdxLabel: "0",
			},
			expectedTaints: []apiv1.Taint{
				{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
		"Spec and rule taints are specified, conflicting": {
			cc: computeclass.NewTestCrd(
				computeclass.WithName(ccName),
				computeclass.WithLabel(ccLabel),
				computeclass.WithUserDefinedTaints([]apiv1.Taint{
					{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
				}),
				computeclass.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithTaintsRule([]apiv1.Taint{
							{Key: "k1", Value: "different", Effect: apiv1.TaintEffectNoSchedule},
						}),
					),
				}),
			),
			ruleIndex: ptr.To(0),
			expectedLabels: map[string]string{
				ccLabel:                                ccName,
				labelComputeClassRequired:              "true",
				gkelabels.ComputeClassPriorityIdxLabel: "0",
			},
			expectedTaints: []apiv1.Taint{
				{Key: "k1", Value: "different", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
		"Spec taints are specified, override existing taints": {
			cc: computeclass.NewTestCrd(
				computeclass.WithName(ccName),
				computeclass.WithLabel(ccLabel),
				computeclass.WithUserDefinedTaints([]apiv1.Taint{
					{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
				}),
			),
			existingTaints: []apiv1.Taint{
				{Key: "k1", Value: "old", Effect: apiv1.TaintEffectNoSchedule},
			},
			expectedLabels: map[string]string{
				ccLabel:                                ccName,
				labelComputeClassRequired:              "true",
				gkelabels.ComputeClassPriorityIdxLabel: "-1",
			},
			expectedTaints: []apiv1.Taint{
				{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
		"Spec taints are specified with different effect, not conflicting": {
			cc: computeclass.NewTestCrd(
				computeclass.WithName(ccName),
				computeclass.WithLabel(ccLabel),
				computeclass.WithUserDefinedTaints([]apiv1.Taint{
					{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
				}),
			),
			existingTaints: []apiv1.Taint{
				{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectPreferNoSchedule},
			},
			expectedLabels: map[string]string{
				ccLabel:                                ccName,
				labelComputeClassRequired:              "true",
				gkelabels.ComputeClassPriorityIdxLabel: "-1",
			},
			expectedTaints: []apiv1.Taint{
				{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectPreferNoSchedule},
				{Key: "k1", Value: "v1", Effect: apiv1.TaintEffectNoSchedule},
			},
		},
		"Label compute-class-priority-idx is set correctly": {
			cc: computeclass.NewTestCrd(
				computeclass.WithName(ccName),
				computeclass.WithLabel(ccLabel),
				computeclass.WithRules([]rules.Rule{
					rules.NewRule(rules.WithMachineFamilyRule(ptr.To("first"))),
					rules.NewRule(rules.WithMachineFamilyRule(ptr.To("second"))),
					rules.NewRule(rules.WithMachineFamilyRule(ptr.To("third"))),
				}),
			),
			ruleIndex:        ptr.To(1),
			autopilotEnabled: true,
			expectedLabels: map[string]string{
				ccLabel:                                ccName,
				labelComputeClassRequired:              "true",
				gkelabels.PodsPerNodeKey:               gkelabels.BinpackedSliceOfHardwareValue,
				gkelabels.ComputeClassPriorityIdxLabel: "1",
			},
		},
		"Label compute-class-priority-idx is not set when flag is disabled": {
			cc: computeclass.NewTestCrd(
				computeclass.WithName(ccName),
				computeclass.WithLabel(ccLabel),
				computeclass.WithRules([]rules.Rule{
					rules.NewRule(rules.WithMachineFamilyRule(ptr.To("first"))),
					rules.NewRule(rules.WithMachineFamilyRule(ptr.To("second"))),
					rules.NewRule(rules.WithMachineFamilyRule(ptr.To("third"))),
				}),
			),
			ruleIndex:                      ptr.To(1),
			autopilotEnabled:               true,
			disableComputeClassMinCapacity: true,
			expectedLabels: map[string]string{
				ccLabel:                   ccName,
				labelComputeClassRequired: "true",
				gkelabels.PodsPerNodeKey:  gkelabels.BinpackedSliceOfHardwareValue,
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithMachineTypes("test-machine-type").WithAutopilotEnabled(tc.autopilotEnabled).Build()
			lister := computeclass_lister.NewMockCrdLister(nil)
			generator := NewComputeClassGenerator(provider, lister, !tc.disableComputeClassMinCapacity)
			params := &nodeGroupParameters{
				systemLabels: make(map[string]string),
				taints:       tc.existingTaints,
			}
			var ruleToUse rules.Rule
			if tc.ruleIndex != nil && len(tc.cc.Rules()) > *tc.ruleIndex {
				ruleToUse = tc.cc.Rules()[*tc.ruleIndex]
			}
			ngReq := nodeGroupRequirements{
				computeClass:     tc.cc,
				computeClassRule: ruleToUse,
			}
			err := generator.UpdateParameters(params, ngReq, NodeGroupOptions{})
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedLabels, params.systemLabels)
			assert.Equal(t, tc.expectedTaints, params.taints)
		})
	}
}

func TestComputeClassGenerator_UpdateNodePoolSpec(t *testing.T) {
	defaultCCLabel := "cc-label"
	defaultCCName := "default-cc"
	nonDefaultCCName := "non-default-cc"

	for tName, tc := range map[string]struct {
		systemLabels     map[string]string
		ccName           string
		ccLabel          string
		defaultCCExists  bool
		autopilotEnabled bool
		expectedSpec     *gkeclient.NodePoolSpec
	}{
		"standard, default cc - taint not present": {
			systemLabels:    map[string]string{defaultCCLabel: defaultCCName},
			defaultCCExists: true,
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{
					defaultCCLabel: defaultCCName,
				},
			},
		},
		"standard, non default cc - taint present": {
			systemLabels:    map[string]string{defaultCCLabel: nonDefaultCCName},
			defaultCCExists: true,
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{
					defaultCCLabel: nonDefaultCCName,
				},
				Taints: []apiv1.Taint{
					{Key: defaultCCLabel, Value: nonDefaultCCName, Effect: apiv1.TaintEffectNoSchedule},
				},
			},
		},
		"standard, default cc doesn't exist - taint present": {
			systemLabels: map[string]string{defaultCCLabel: defaultCCName},
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{
					defaultCCLabel: defaultCCName,
				},
				Taints: []apiv1.Taint{
					{Key: defaultCCLabel, Value: defaultCCName, Effect: apiv1.TaintEffectNoSchedule},
				},
			},
		},
		"autopilot, default cc - taint not present": {
			systemLabels:     map[string]string{defaultCCLabel: defaultCCName},
			defaultCCExists:  true,
			autopilotEnabled: true,
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{
					defaultCCLabel: defaultCCName,
				},
			},
		},
		"autopilot, non default cc - taint present": {
			systemLabels:     map[string]string{defaultCCLabel: nonDefaultCCName},
			defaultCCExists:  true,
			autopilotEnabled: true,
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{
					defaultCCLabel: nonDefaultCCName,
				},
				Taints: []apiv1.Taint{
					{Key: defaultCCLabel, Value: nonDefaultCCName, Effect: apiv1.TaintEffectNoSchedule},
				},
			},
		},
		"autopilot, default cc doesn't exist - taint present": {
			systemLabels:     map[string]string{defaultCCLabel: defaultCCName},
			autopilotEnabled: true,
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{
					defaultCCLabel: defaultCCName,
				},
				Taints: []apiv1.Taint{
					{Key: defaultCCLabel, Value: defaultCCName, Effect: apiv1.TaintEffectNoSchedule},
				},
			},
		},
		"standard, nothing injected for predefined compute class in CC mode": {
			systemLabels: map[string]string{defaultCCLabel: machinetypes.AllComputeClasses()[0].Name()},
			ccLabel:      gkelabels.ComputeClassLabel,
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{},
			},
		},
		"autopilot, nothing injected for predefined compute class in CC mode": {
			systemLabels: map[string]string{defaultCCLabel: machinetypes.AllComputeClasses()[0].Name()},
			ccLabel:      gkelabels.ComputeClassLabel,
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{},
			},
		},
		"standard, autopilot managed - label, taint, autopilot managed set": {
			systemLabels: map[string]string{
				defaultCCLabel:             nonDefaultCCName,
				gkelabels.ManagedNodeLabel: "true",
			},
			defaultCCExists: true,
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{
					defaultCCLabel:             nonDefaultCCName,
					gkelabels.ManagedNodeLabel: "true",
				},
				Taints: []apiv1.Taint{
					{Key: gkelabels.ManagedNodeLabel, Value: "true", Effect: apiv1.TaintEffectNoSchedule},
					{Key: defaultCCLabel, Value: nonDefaultCCName, Effect: apiv1.TaintEffectNoSchedule},
				},
				AutopilotManaged: true,
			},
		},
		"autopilot, autopilot managed - label, taint, autopilot managed not set (noop)": {
			autopilotEnabled: true,
			systemLabels: map[string]string{
				defaultCCLabel:             nonDefaultCCName,
				gkelabels.ManagedNodeLabel: "true",
			},
			defaultCCExists: true,
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{
					defaultCCLabel: nonDefaultCCName,
				},
				Taints: []apiv1.Taint{
					{Key: defaultCCLabel, Value: nonDefaultCCName, Effect: apiv1.TaintEffectNoSchedule},
				},
			},
		},
		"BPSoHW injected": {
			systemLabels: map[string]string{
				defaultCCLabel:           nonDefaultCCName,
				gkelabels.PodsPerNodeKey: gkelabels.BinpackedSliceOfHardwareValue,
			},
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{
					defaultCCLabel:           nonDefaultCCName,
					gkelabels.PodsPerNodeKey: gkelabels.BinpackedSliceOfHardwareValue,
				},
				Taints: []apiv1.Taint{
					{Key: defaultCCLabel, Value: nonDefaultCCName, Effect: apiv1.TaintEffectNoSchedule},
				},
			},
		},
		"service account injected": {
			systemLabels: map[string]string{
				gkelabels.ServiceAccountLabelKey: "test@12345.iam.gserviceaccount.com",
			},
			expectedSpec: &gkeclient.NodePoolSpec{
				ServiceAccount: "test@12345.iam.gserviceaccount.com",
				Labels:         map[string]string{},
			},
		},
		"image type injected into node pool spec": {
			systemLabels: map[string]string{
				gkelabels.ImageTypeLabelKey: "ubuntu_containerd",
			},
			expectedSpec: &gkeclient.NodePoolSpec{
				ImageType: "ubuntu_containerd",
				Labels:    map[string]string{},
			},
		},
		"DRA TPU label injected into node pool spec": {
			systemLabels: map[string]string{
				gkelabels.DraTpuNodeLabel: "true",
			},
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{
					gkelabels.DraTpuNodeLabel: "true",
				},
			},
		},
		"DRA TPU label invalid value not passed": {
			systemLabels: map[string]string{
				gkelabels.DraTpuNodeLabel: "false",
			},
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{},
			},
		},
		"node group dynamic max pods per node enabled": {
			ccLabel: gkelabels.ComputeClassLabel,
			systemLabels: map[string]string{
				gkelabels.ComputeClassLabel: defaultCCName,
			},
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{
					gkelabels.ComputeClassLabel: defaultCCName,
				},
				Taints: []apiv1.Taint{
					{Key: gkelabels.ComputeClassLabel, Value: defaultCCName, Effect: apiv1.TaintEffectNoSchedule},
				},
			},
		},
		"node group dynamic boot disk size enabled": {
			ccLabel: gkelabels.ComputeClassLabel,
			systemLabels: map[string]string{
				gkelabels.ComputeClassLabel:                           defaultCCName,
				gkelabels.NodeGroupDynamicBootDiskSizeEnabledLabelKey: "true",
			},
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{
					gkelabels.ComputeClassLabel:                           defaultCCName,
					gkelabels.NodeGroupDynamicBootDiskSizeEnabledLabelKey: "true",
				},
				Taints: []apiv1.Taint{
					{Key: gkelabels.ComputeClassLabel, Value: defaultCCName, Effect: apiv1.TaintEffectNoSchedule},
				},
			},
		},
		"Priority idx label is set correctly": {
			ccLabel: gkelabels.ComputeClassLabel,
			systemLabels: map[string]string{
				gkelabels.ComputeClassLabel:            defaultCCName,
				gkelabels.ComputeClassPriorityIdxLabel: "2",
			},
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{
					gkelabels.ComputeClassLabel:            defaultCCName,
					gkelabels.ComputeClassPriorityIdxLabel: "2",
				},
				Taints: []apiv1.Taint{
					{Key: gkelabels.ComputeClassLabel, Value: defaultCCName, Effect: apiv1.TaintEffectNoSchedule},
				},
			},
		},
	} {
		t.Run(tName, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithMachineTypes("test-machine-type").WithAutopilotEnabled(tc.autopilotEnabled).Build()
			lister := computeclass_lister.NewMockCrdLister(nil)

			if tc.ccLabel == "" {
				tc.ccLabel = defaultCCLabel
			}
			lister.SetCrdLabel(tc.ccLabel)

			if tc.defaultCCExists {
				lister.SetDefaultCrdName(defaultCCName)
			}
			pgg := NewComputeClassGenerator(provider, lister, true)
			spec := &gkeclient.NodePoolSpec{
				Labels: map[string]string{},
			}
			err := pgg.UpdateNodePoolSpec(spec, tc.systemLabels, nil)
			assert.NoError(t, err)

			assert.Equal(t, tc.expectedSpec, spec)
		})
	}
}

func TestProvisioningRequestGenerator_UpdateNodePoolSpec(t *testing.T) {
	for tName, tc := range map[string]struct {
		enabledFeatures        []string
		bulkSpec               bool
		systemLabels           map[string]string
		wantLabels             map[string]string
		wantQueuedProvisioning bool
	}{
		"queued provisioning label present - spec queued provisioning is true": {
			systemLabels:           map[string]string{gkelabels.ProvisioningRequestLabelKey: "rr0"},
			wantQueuedProvisioning: true,
		},
		"queued provisioning label missing - spec queued provisioning is false": {
			systemLabels: map[string]string{},
		},
		"bulkSpec_rrSystemLabelMissing_dontSetLabel": {
			enabledFeatures:        []string{experiments.ProvisioningRequestBulkMigsFlag},
			bulkSpec:               true,
			systemLabels:           map[string]string{},
			wantQueuedProvisioning: false,
			wantLabels:             nil,
		},
		"bulkSpec_expDisabled_dontSetLabel": {
			bulkSpec:               true,
			systemLabels:           map[string]string{gkelabels.ProvisioningRequestLabelKey: "rr0"},
			wantQueuedProvisioning: true,
			wantLabels:             nil,
		},
		"bulkSpec_setLabel": {
			enabledFeatures:        []string{experiments.ProvisioningRequestBulkMigsFlag},
			bulkSpec:               true,
			systemLabels:           map[string]string{gkelabels.ProvisioningRequestLabelKey: "rr0"},
			wantQueuedProvisioning: true,
			wantLabels:             map[string]string{gkelabels.ProvisioningRequestLabelKey: "rr0"},
		},
	} {
		t.Run(tName, func(t *testing.T) {
			spec := &gkeclient.NodePoolSpec{}
			if tc.bulkSpec {
				spec.FlexStart = true
				spec.MachineType = "a4x-highgpu-4g"
				spec.PlacementGroup.Policy = "a4x-policy"
			}
			assert.Equal(t, tc.bulkSpec, spec.UsesBulkProvisioning(machinetypes.NewMachineConfigProvider(nil)))

			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).Build()
			generator := NewProvisioningRequestGenerator(experiments.NewMockManager(tc.enabledFeatures...), provider)
			err := generator.UpdateNodePoolSpec(spec, tc.systemLabels, nil)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantQueuedProvisioning, spec.QueuedProvisioning)
			assert.Equal(t, tc.wantLabels, spec.Labels)
			if tc.wantQueuedProvisioning {
				assert.Equal(t, ptr.To(gke_api_beta.UpgradeSettings{Strategy: "SHORT_LIVED"}), spec.UpgradeSettings)
			} else {
				assert.Nil(t, spec.UpgradeSettings)
			}
		})
	}
}

func TestMaxRunDurationGenerator_UpdateNodePoolSpec(t *testing.T) {
	for tName, tc := range map[string]struct {
		systemLabels map[string]string
		wantMrd      bool
	}{
		"MRD label missing - MRD and np label is not set": {
			systemLabels: map[string]string{},
		},
		"MRD label empty - MRD and np label is not set": {
			systemLabels: map[string]string{gkelabels.MaxRunDurationLabelKey: ""},
		},
		"MRD label present - MRD and np Label is set": {
			systemLabels: map[string]string{gkelabels.MaxRunDurationLabelKey: "3600"},
			wantMrd:      true,
		},
	} {
		t.Run(tName, func(t *testing.T) {
			generator := NewMaxRunDurationGenerator(nil, nil)
			spec := &gkeclient.NodePoolSpec{}
			err := generator.UpdateNodePoolSpec(spec, tc.systemLabels, nil)
			assert.NoError(t, err)
			if tc.wantMrd {
				wantMrdValue := tc.systemLabels[gkelabels.MaxRunDurationLabelKey]
				assert.Equal(t, wantMrdValue, spec.MaxRunDurationInSeconds)
				assert.Equal(t, wantMrdValue, spec.Labels[gkelabels.MaxRunDurationLabelKey])
			} else {
				assert.Empty(t, spec.MaxRunDurationInSeconds)
				assert.Empty(t, spec.Labels[gkelabels.MaxRunDurationLabelKey])
			}
		})
	}
}

func TestFlexStartGenerator_UpdateNodePoolSpec(t *testing.T) {
	for tName, tc := range map[string]struct {
		systemLabels  map[string]string
		wantFlexStart bool
		wantLabels    map[string]string
	}{
		"no_FlexStartLabel_FlexStart_notSet": {
			systemLabels: map[string]string{},
		},
		"FlexStartLabel_FlexStart_set": {
			systemLabels:  map[string]string{gkelabels.FlexStartLabel: ""},
			wantFlexStart: true,
			wantLabels: map[string]string{
				gkelabels.FlexStartLabel:    gkelabels.FlexStartValue,
				gkelabels.ProvisioningLabel: gkelabels.FlexStartProvisioningValue,
			},
		},
	} {
		t.Run(tName, func(t *testing.T) {
			generator := NewFlexStartGenerator(nil)
			spec := &gkeclient.NodePoolSpec{}
			err := generator.UpdateNodePoolSpec(spec, tc.systemLabels, nil)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantFlexStart, spec.FlexStart)
			assert.Equal(t, len(tc.wantLabels), len(spec.Labels))
			for key, val := range tc.wantLabels {
				assert.Equal(t, spec.Labels[key], val)
			}
			if tc.wantFlexStart {
				assert.Equal(t, ptr.To(gke_api_beta.UpgradeSettings{Strategy: "SHORT_LIVED"}), spec.UpgradeSettings)
			} else {
				assert.Nil(t, spec.UpgradeSettings)
			}
		})
	}
}

func TestSandboxTypeGenerator_UpdateNodePoolSpec(t *testing.T) {
	for name, tc := range map[string]struct {
		systemLabels         map[string]string
		expectedSanboxType   sandbox.Type
		expectLabelAndTaints bool
	}{
		"valid gVisor label": {
			systemLabels: map[string]string{
				sandbox.GVisorLabelKey: sandbox.GVisorLabelValue,
			},
			expectedSanboxType:   sandbox.GVisor,
			expectLabelAndTaints: true,
		},
		"invalid gVisor label": {
			systemLabels: map[string]string{
				sandbox.GVisorLabelKey: "not-a-proper-label",
			},
			expectedSanboxType:   sandbox.None,
			expectLabelAndTaints: false,
		},
		"no gVisor label": {
			systemLabels:         map[string]string{},
			expectedSanboxType:   sandbox.None,
			expectLabelAndTaints: false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			stg := NewSandboxTypeGenerator()
			spec := &gkeclient.NodePoolSpec{
				Labels: map[string]string{},
			}
			err := stg.UpdateNodePoolSpec(spec, tc.systemLabels, nil)
			assert.NoError(t, err)
			if tc.expectLabelAndTaints {
				assert.Contains(t, spec.Labels, sandbox.GVisorLabelKey)
				assert.Contains(t, spec.Taints, apiv1.Taint{Key: sandbox.GVisorTaintKey, Value: sandbox.GVisorTaintValue, Effect: apiv1.TaintEffectNoSchedule})
			} else {
				assert.NotContains(t, spec.Labels, sandbox.GVisorLabelKey)
				assert.NotContains(t, spec.Taints, apiv1.Taint{Key: sandbox.GVisorTaintKey, Value: sandbox.GVisorTaintValue, Effect: apiv1.TaintEffectNoSchedule})
			}
			assert.Equal(t, spec.SandboxType, tc.expectedSanboxType)
		})
	}
}

func TestPreemeptionOptionGenerator_UpdateNodePoolSpec(t *testing.T) {
	systemLabels := map[string]string{
		gkelabels.ProvisioningLabel: gkelabels.StandardProvisioningValue,
	}
	stg := NewPreemeptionOptionGenerator(true)
	spec := &gkeclient.NodePoolSpec{
		Labels: map[string]string{},
	}
	err := stg.UpdateNodePoolSpec(spec, systemLabels, nil)
	assert.NoError(t, err)
	assert.Equal(t, gkelabels.StandardProvisioningValue, spec.Labels[gkelabels.ProvisioningLabel])
}

func TestConsolidationDelayGenerator_UpdateRequirements(t *testing.T) {
	for desc, tc := range map[string]struct {
		podReq                             *podrequirements.Requirements
		wantNgReq                          *nodeGroupRequirements
		computeClass                       computeclass.CRD
		wantErr                            errors.AutoscalerError
		nodePoolConsolidationDelayDisabled bool
	}{
		"noLabel_doesNotSetConsolidationDelay": {
			podReq:    &podrequirements.Requirements{},
			wantNgReq: &nodeGroupRequirements{},
		},
		"hasBothLabelAndCC_throws": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.ConsolidationDelayLabelKey: podrequirements.NewValues("600"),
				}),
			},
			computeClass: computeclass.NewTestCrd(
				computeclass.WithName("test-cc"),
				computeclass.WithConsolidationDelay(5*time.Minute),
				computeclass.WithCrdType("CC"),
			),
			wantNgReq: &nodeGroupRequirements{
				computeClass: computeclass.NewTestCrd(
					computeclass.WithName("test-cc"),
					computeclass.WithConsolidationDelay(5*time.Minute),
					computeclass.WithCrdType("CC"),
				),
			},
			wantErr: NewComputeClassPodIncompatibleError("test-cc", "CC"),
		},
		"hasLabel_setsConsolidationDelay": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.ConsolidationDelayLabelKey: podrequirements.NewValues("600-may-be-incorrectly-formatted-just-copies"),
				}),
			},
			wantNgReq: &nodeGroupRequirements{
				consolidationDelayInSeconds: "600-may-be-incorrectly-formatted-just-copies",
			},
		},
		"noExperiment_hasLabel_doesNotSetConsolidationDelay": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.ConsolidationDelayLabelKey: podrequirements.NewValues("600-may-be-incorrectly-formatted-just-copies"),
				}),
			},
			wantNgReq:                          &nodeGroupRequirements{},
			nodePoolConsolidationDelayDisabled: true,
		},
	} {
		t.Run(desc, func(t *testing.T) {
			exps := []string{}
			if !tc.nodePoolConsolidationDelayDisabled {
				exps = append(exps, experiments.NodePoolConsolidationDelayMinCAVersionFlag)
			}
			generator := NewConsolidationDelayGenerator(experiments.NewMockManager(exps...))
			ngReq := &nodeGroupRequirements{}
			if tc.computeClass != nil {
				ngReq.computeClass = tc.computeClass
			}
			err := generator.UpdateRequirements(ngReq, tc.podReq, machinetypes.GpuRequest{}, TpuRequest{})
			if tc.wantErr != nil {
				assert.Equal(t, tc.wantErr, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.wantNgReq, ngReq)
		})
	}
}

func TestConsolidationDelayGenerator_UpdateNodePoolSpec(t *testing.T) {
	for tName, tc := range map[string]struct {
		systemLabels                       map[string]string
		wantErr                            error
		wantConsolidationDelayInSeconds    bool
		nodePoolConsolidationDelayDisabled bool
	}{
		"noLabel_doesNotSetConsolidationDelay": {
			systemLabels: map[string]string{},
		},
		"emptyLabel_doesNotSetConsolidationDelay": {
			systemLabels: map[string]string{gkelabels.ConsolidationDelayLabelKey: ""},
		},
		"hasLabel_setsConsolidationDelay": {
			systemLabels:                    map[string]string{gkelabels.ConsolidationDelayLabelKey: "600-may-be-incorrectly-formatted-just-copies"},
			wantConsolidationDelayInSeconds: true,
		},
		"noExperiment_hasLabel_doesNotSetConsolidationDelay": {
			systemLabels:                       map[string]string{gkelabels.ConsolidationDelayLabelKey: "600-may-be-incorrectly-formatted-just-copies"},
			wantConsolidationDelayInSeconds:    false,
			nodePoolConsolidationDelayDisabled: true,
		},
	} {
		t.Run(tName, func(t *testing.T) {
			exps := []string{}
			if !tc.nodePoolConsolidationDelayDisabled {
				exps = append(exps, experiments.NodePoolConsolidationDelayMinCAVersionFlag)
			}
			generator := NewConsolidationDelayGenerator(experiments.NewMockManager(exps...))
			spec := &gkeclient.NodePoolSpec{}
			err := generator.UpdateNodePoolSpec(spec, tc.systemLabels, nil)
			if tc.wantErr != nil {
				assert.Equal(t, tc.wantErr, err)
			} else {
				assert.NoError(t, err)
			}
			if tc.wantConsolidationDelayInSeconds {
				assert.Equal(t, tc.systemLabels[gkelabels.ConsolidationDelayLabelKey], spec.ConsolidationDelayInSeconds)
				assert.Equal(t, tc.systemLabels[gkelabels.ConsolidationDelayLabelKey], spec.Labels[gkelabels.ConsolidationDelayLabelKey])
			} else {
				assert.Empty(t, spec.ConsolidationDelayInSeconds)
				assert.Empty(t, spec.Labels[gkelabels.ConsolidationDelayLabelKey])
			}
		})
	}
}

func TestPreemeptionOptionGenerator_GenerateNodeGroupOptionsForRequirements(t *testing.T) {
	falseSpot := false
	trueSpot := true

	pod := &apiv1.Pod{
		Spec: apiv1.PodSpec{
			Tolerations: nil,
		},
	}
	preemptiblePod := &apiv1.Pod{
		Spec: apiv1.PodSpec{
			Tolerations: []apiv1.Toleration{
				{
					Key:      gkelabels.PreemptibleLabel,
					Operator: "Exists",
					Effect:   apiv1.TaintEffectNoSchedule,
				},
			},
		},
	}
	spotToleration := apiv1.Toleration{
		Key:      gkelabels.SpotLabel,
		Operator: "Exists",
		Effect:   apiv1.TaintEffectNoSchedule,
	}
	spotPod := &apiv1.Pod{
		Spec: apiv1.PodSpec{
			Tolerations: []apiv1.Toleration{spotToleration},
		},
	}

	flexStartPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				gkelabels.FlexStartLabel: gkelabels.FlexStartValue,
			},
		},
		Spec: apiv1.PodSpec{
			Tolerations: []apiv1.Toleration{},
		},
	}
	flexStartToleratingSpotPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				gkelabels.ProvisioningLabel: gkelabels.FlexStartProvisioningValue,
			},
		},
		Spec: apiv1.PodSpec{
			Tolerations: []apiv1.Toleration{spotToleration},
		},
	}

	for tn, tc := range map[string]struct {
		options      []NodeGroupOptions
		requirements nodeGroupRequirements
		wantOptions  []NodeGroupOptions
	}{
		"no options passed": {
			requirements: nodeGroupRequirements{
				pods: []*apiv1.Pod{pod, preemptiblePod, spotPod},
			},
		},
		"pod without tolerations": {
			options: []NodeGroupOptions{
				{MachineType: "machine-1"},
				{MachineType: "machine-2"},
			},
			requirements: nodeGroupRequirements{
				pods: []*apiv1.Pod{pod},
			},
			wantOptions: []NodeGroupOptions{
				{MachineType: "machine-1", Preemption: preemption.NoPreemption},
				{MachineType: "machine-2", Preemption: preemption.NoPreemption},
			},
		},
		"preemptible pod": {
			options: []NodeGroupOptions{
				{MachineType: "machine-1"},
				{MachineType: "machine-2"},
			},
			requirements: nodeGroupRequirements{
				pods: []*apiv1.Pod{preemptiblePod},
			},
			wantOptions: []NodeGroupOptions{
				{MachineType: "machine-1", Preemption: preemption.NoPreemption},
				{MachineType: "machine-2", Preemption: preemption.NoPreemption},
				{MachineType: "machine-1", Preemption: preemption.LegacyPreemptible},
				{MachineType: "machine-2", Preemption: preemption.LegacyPreemptible},
			},
		},
		"spot pod": {
			options: []NodeGroupOptions{
				{MachineType: "machine-1"},
				{MachineType: "machine-2"},
			},
			requirements: nodeGroupRequirements{
				pods: []*apiv1.Pod{spotPod},
			},
			wantOptions: []NodeGroupOptions{
				{MachineType: "machine-1", Preemption: preemption.NoPreemption},
				{MachineType: "machine-2", Preemption: preemption.NoPreemption},
				{MachineType: "machine-1", Preemption: preemption.Spot},
				{MachineType: "machine-2", Preemption: preemption.Spot},
			},
		},
		"all pods": {
			options: []NodeGroupOptions{
				{MachineType: "machine-1"},
				{MachineType: "machine-2"},
			},
			requirements: nodeGroupRequirements{
				pods: []*apiv1.Pod{pod, preemptiblePod, spotPod},
			},
			wantOptions: []NodeGroupOptions{
				{MachineType: "machine-1", Preemption: preemption.NoPreemption},
				{MachineType: "machine-2", Preemption: preemption.NoPreemption},
				{MachineType: "machine-1", Preemption: preemption.Spot},
				{MachineType: "machine-2", Preemption: preemption.Spot},
			},
		},
		"compute class instance characteristic rule with default non-spot constraint": {
			options: []NodeGroupOptions{
				{MachineType: "machine-1"},
				{MachineType: "machine-2"},
			},
			requirements: nodeGroupRequirements{
				pods:             []*apiv1.Pod{pod, preemptiblePod, spotPod},
				computeClassRule: rules.NewMachineSpecRule(nil, nil, nil, nil),
			},
			wantOptions: []NodeGroupOptions{
				{MachineType: "machine-1", Preemption: preemption.NoPreemption},
				{MachineType: "machine-2", Preemption: preemption.NoPreemption},
			},
		},
		"compute class instance characteristic rule with specified non-spot constraint": {
			options: []NodeGroupOptions{
				{MachineType: "machine-1"},
				{MachineType: "machine-2"},
			},
			requirements: nodeGroupRequirements{
				pods:             []*apiv1.Pod{pod, preemptiblePod, spotPod},
				computeClassRule: rules.NewMachineSpecRule(nil, &falseSpot, nil, nil),
			},
			wantOptions: []NodeGroupOptions{
				{MachineType: "machine-1", Preemption: preemption.NoPreemption},
				{MachineType: "machine-2", Preemption: preemption.NoPreemption},
			},
		},
		"compute class instance characteristic rule with specified spot constraint": {
			options: []NodeGroupOptions{
				{MachineType: "machine-1"},
				{MachineType: "machine-2"},
			},
			requirements: nodeGroupRequirements{
				pods:             []*apiv1.Pod{pod, preemptiblePod, spotPod},
				computeClassRule: rules.NewMachineSpecRule(nil, &trueSpot, nil, nil),
			},
			wantOptions: []NodeGroupOptions{
				{MachineType: "machine-1", Preemption: preemption.Spot},
				{MachineType: "machine-2", Preemption: preemption.Spot},
			},
		},
		"flex start pods": {
			options: []NodeGroupOptions{
				{MachineType: "machine-1"},
				{MachineType: "machine-2"},
			},
			requirements: nodeGroupRequirements{
				pods:         []*apiv1.Pod{flexStartPod, flexStartToleratingSpotPod},
				flexStartReq: flexStartRequirements{enabled: true},
			},
			wantOptions: []NodeGroupOptions{
				{MachineType: "machine-1", Preemption: preemption.NoPreemption},
				{MachineType: "machine-2", Preemption: preemption.NoPreemption},
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			pog := NewPreemeptionOptionGenerator(false)
			assert.ElementsMatch(t, tc.wantOptions, pog.GenerateNodeGroupOptionsForRequirements(tc.options, tc.requirements))
		})
	}
}

func TestReservationGenerator_UpdateNodePoolSpec(t *testing.T) {
	for name, tc := range map[string]struct {
		enableReservationAffinity bool
		projectId                 string
		systemLabels              map[string]string
		expectLabels              map[string]string
		expectRes                 *gke_api_beta.ReservationAffinity
	}{
		"only name - non-path must be used as res value to support TPUs": {
			enableReservationAffinity: true,
			projectId:                 "res-proj",
			systemLabels: map[string]string{
				gkelabels.ReservationNameLabel:     "res-name",
				gkelabels.ReservationAffinityLabel: "specific",
			},
			expectLabels: map[string]string{
				gkelabels.ReservationNameLabel:     "res-name",
				gkelabels.ReservationAffinityLabel: "specific",
				gkelabels.ReservationProjectLabel:  "res-proj",
			},
			expectRes: &gke_api_beta.ReservationAffinity{
				ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
				Key:                    gkeclient.ReservationNameKey,
				Values:                 []string{"res-name"},
			},
		},
		"name and project": {
			enableReservationAffinity: true,
			systemLabels: map[string]string{
				gkelabels.ReservationNameLabel:     "res-name",
				gkelabels.ReservationProjectLabel:  "res-proj",
				gkelabels.ReservationAffinityLabel: "specific",
			},
			expectLabels: map[string]string{
				gkelabels.ReservationNameLabel:     "res-name",
				gkelabels.ReservationProjectLabel:  "res-proj",
				gkelabels.ReservationAffinityLabel: "specific",
			},
			expectRes: &gke_api_beta.ReservationAffinity{
				ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
				Key:                    gkeclient.ReservationNameKey,
				Values:                 []string{"projects/res-proj/reservations/res-name"},
			},
		},
		"name and block - reservationBlocks should be included without project": {
			enableReservationAffinity: true,
			systemLabels: map[string]string{
				gkelabels.ReservationNameLabel:     "res-name",
				gkelabels.ReservationAffinityLabel: "specific",
				gkelabels.ReservationBlocksLabel:   "res-block",
			},
			expectLabels: map[string]string{
				gkelabels.ReservationNameLabel:     "res-name",
				gkelabels.ReservationAffinityLabel: "specific",
				gkelabels.ReservationBlocksLabel:   "res-block",
				gkelabels.ReservationProjectLabel:  "",
			},
			expectRes: &gke_api_beta.ReservationAffinity{
				ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
				Key:                    gkeclient.ReservationNameKey,
				Values:                 []string{"res-name/reservationBlocks/res-block"},
			},
		},
		"project, name and block - reservationBlocks should be included with the project": {
			enableReservationAffinity: true,
			systemLabels: map[string]string{
				gkelabels.ReservationNameLabel:     "res-name",
				gkelabels.ReservationProjectLabel:  "res-proj",
				gkelabels.ReservationAffinityLabel: "specific",
				gkelabels.ReservationBlocksLabel:   "res-block",
			},
			expectLabels: map[string]string{
				gkelabels.ReservationNameLabel:     "res-name",
				gkelabels.ReservationProjectLabel:  "res-proj",
				gkelabels.ReservationAffinityLabel: "specific",
				gkelabels.ReservationBlocksLabel:   "res-block",
			},
			expectRes: &gke_api_beta.ReservationAffinity{
				ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
				Key:                    gkeclient.ReservationNameKey,
				Values:                 []string{"projects/res-proj/reservations/res-name/reservationBlocks/res-block"},
			},
		},
		"name and default project - non-path must be used as res value to support TPUs": {
			enableReservationAffinity: true,
			projectId:                 "res-proj",
			systemLabels: map[string]string{
				gkelabels.ReservationNameLabel:     "res-name",
				gkelabels.ReservationProjectLabel:  "res-proj",
				gkelabels.ReservationAffinityLabel: "specific",
			},
			expectLabels: map[string]string{
				gkelabels.ReservationNameLabel:     "res-name",
				gkelabels.ReservationProjectLabel:  "res-proj",
				gkelabels.ReservationAffinityLabel: "specific",
			},
			expectRes: &gke_api_beta.ReservationAffinity{
				ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
				Key:                    gkeclient.ReservationNameKey,
				Values:                 []string{"res-name"},
			},
		},
		"any reservation": {
			enableReservationAffinity: true,
			projectId:                 "res-proj",
			systemLabels: map[string]string{
				gkelabels.ReservationAffinityLabel: "any",
			},
			expectLabels: map[string]string{
				gkelabels.ReservationAffinityLabel: "any",
			},
			expectRes: &gke_api_beta.ReservationAffinity{
				ConsumeReservationType: gkeclient.ReservationAffinityAny,
			},
		},
		"nothing": {
			enableReservationAffinity: true,
			systemLabels:              map[string]string{},
			expectLabels:              map[string]string{},
		},
		"not enabled - only project": {
			enableReservationAffinity: true,
			systemLabels: map[string]string{
				gkelabels.ReservationProjectLabel: "res-proj",
			},
			expectLabels: map[string]string{},
		},
		"queued provisioning label present - reservation affinity gets set to none": {
			enableReservationAffinity: true,
			systemLabels:              map[string]string{gkelabels.ProvisioningRequestLabelKey: "prov-req-1"},
			expectLabels:              map[string]string{},
			expectRes:                 &gke_api_beta.ReservationAffinity{ConsumeReservationType: gkeclient.ReservationAffinityNone},
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := ReservationFlags{
				SpecificTypeReservationMatchEnabled: true,
			}
			rg := NewReservationGenerator(nil, f, tc.projectId, experiments.NewMockManager(), nil)
			spec := &gkeclient.NodePoolSpec{
				Labels: map[string]string{},
			}
			err := rg.UpdateNodePoolSpec(spec, tc.systemLabels, nil)
			assert.NoError(t, err)

			assert.Equal(t, tc.expectLabels, spec.Labels)
			assert.Equal(t, tc.expectRes, spec.ReservationAffinity)
		})
	}
}

func TestExtendedDurationPodGenerator(t *testing.T) {
	for name, tc := range map[string]struct {
		autopilotEnabled         bool
		ekEdpEnabled             bool
		machineType              string
		cpuReq                   []string
		expectNoNodeGroupOptions bool
		expectedErr              error
		expectedSpec             *gkeclient.NodePoolSpec
	}{
		"correct cpu req": {
			autopilotEnabled: true,
			cpuReq:           []string{"100m"},
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{
					gkelabels.ExtendedDurationPodsLabel: "100m",
				},
				ExtendedDurationPods: "100m",
			},
		},
		"incorrect cpu req": {
			autopilotEnabled: true,
			cpuReq:           []string{"ab"},
			expectedErr:      NewInvalidExtendedDurationPodCPUReq("ab"),
		},
		"autopilot disabled": {
			cpuReq:      []string{"100m"},
			expectedErr: NewExtendedDurationPodNonAutopilotError(),
		},
		"ek edp enabled with ek machine type": {
			autopilotEnabled: true,
			ekEdpEnabled:     true,
			machineType:      "ek-standard-2",
			cpuReq:           []string{"100m"},
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{
					gkelabels.ExtendedDurationPodsLabel: "X",
				},
				ExtendedDurationPods: "X",
				MachineType:          "ek-standard-2",
			},
		},
		"ek edp enabled with non-ek machine type": {
			autopilotEnabled: true,
			ekEdpEnabled:     true,
			machineType:      "c2-standard-4",
			cpuReq:           []string{"100m"},
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{
					gkelabels.ExtendedDurationPodsLabel: "100m",
				},
				ExtendedDurationPods: "100m",
				MachineType:          "c2-standard-4",
			},
		},
		"numeric and X pod requirements - X is used for EK machine type": {
			autopilotEnabled: true,
			ekEdpEnabled:     true,
			machineType:      "ek-standard-4",
			cpuReq:           []string{"X", "200m"},
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels:               map[string]string{gkelabels.ExtendedDurationPodsLabel: "X"},
				ExtendedDurationPods: "X",
				MachineType:          "ek-standard-4",
			},
		},
		"numeric and X pod requirement pod requirements - numeric is used for non-EK machine type": {
			autopilotEnabled: true,
			ekEdpEnabled:     true,
			machineType:      "cs-standard-4",
			cpuReq:           []string{"X", "200m"},
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels:               map[string]string{gkelabels.ExtendedDurationPodsLabel: "200m"},
				ExtendedDurationPods: "200m",
				MachineType:          "cs-standard-4",
			},
		},
		"only X pod requirement - X is used for EK machine type": {
			autopilotEnabled: true,
			ekEdpEnabled:     true,
			machineType:      "ek-standard-4",
			cpuReq:           []string{"X"},
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels:               map[string]string{gkelabels.ExtendedDurationPodsLabel: "X"},
				ExtendedDurationPods: "X",
				MachineType:          "ek-standard-4",
			},
		},
		"only X pod requirement - no options for non-EK machine type": {
			autopilotEnabled:         true,
			ekEdpEnabled:             true,
			machineType:              "c2-standard-2",
			cpuReq:                   []string{"X"},
			expectNoNodeGroupOptions: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithAutopilotEnabled(tc.autopilotEnabled).
				WithEkEdpEnabled(tc.ekEdpEnabled).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			edpg := NewExtendedDurationPodGenerator(provider)
			podReq := &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.ExtendedDurationPodsLabel: podrequirements.NewValues(tc.cpuReq...),
				}),
			}
			ngReq := nodeGroupRequirements{}
			err := edpg.UpdateRequirements(&ngReq, podReq, machinetypes.GpuRequest{}, TpuRequest{})
			if tc.expectedErr != nil {
				assert.Equal(t, tc.expectedErr, err)
				return
			}
			assert.NoError(t, err)
			params := &nodeGroupParameters{
				systemLabels: make(map[string]string),
			}
			opts := make([]NodeGroupOptions, 1)
			opts[0].MachineType = tc.machineType
			opts = edpg.GenerateNodeGroupOptionsForRequirements(opts, ngReq)
			if tc.expectNoNodeGroupOptions {
				assert.Empty(t, opts)
				return
			}
			err2 := edpg.UpdateParameters(params, ngReq, opts[1])
			assert.NoError(t, err2)
			spec := &gkeclient.NodePoolSpec{
				Labels:      map[string]string{},
				MachineType: tc.machineType,
			}
			err2 = edpg.UpdateNodePoolSpec(spec, params.systemLabels, nil)
			assert.NoError(t, err2)
			assert.Equal(t, tc.expectedSpec, spec)
		})
	}
}

func TestPodIsolationLabelGenerator(t *testing.T) {
	for name, tc := range map[string]struct {
		cpuReq           string
		expectedErr      error
		expectedSpec     *gkeclient.NodePoolSpec
		autopilotEnabled bool
	}{
		"correct cpu req": {
			cpuReq: "100m",
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{
					gkelabels.PodPerVMSizeLabel: "100m",
				},
			},
			autopilotEnabled: true,
		},
		"incorrect cpu req": {
			cpuReq:           "ab",
			expectedErr:      NewInvalidIsolatedPodCPUReq("ab"),
			autopilotEnabled: true,
		},
		"autopilot disabled": {
			expectedErr: NewIsolatedPodNonAutopilotError(),
		},
	} {
		t.Run(name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithAutopilotEnabled(tc.autopilotEnabled).Build()
			gen := NewPodIsolationLabelGenerator(provider)
			podReq := &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.PodPerVMSizeLabel: podrequirements.NewValues(tc.cpuReq),
				}),
			}
			ngReq := nodeGroupRequirements{}
			err := gen.UpdateRequirements(&ngReq, podReq, machinetypes.GpuRequest{}, TpuRequest{})
			if tc.expectedErr != nil {
				assert.Equal(t, err, tc.expectedErr)
				return
			}
			assert.NoError(t, err)
			params := &nodeGroupParameters{
				systemLabels: make(map[string]string),
			}
			opts := []NodeGroupOptions{
				{},
			}
			opts = gen.GenerateNodeGroupOptionsForRequirements(opts, ngReq)
			err2 := gen.UpdateParameters(params, ngReq, opts[0])
			assert.NoError(t, err2)
			spec := &gkeclient.NodePoolSpec{
				Labels: map[string]string{},
			}
			err2 = gen.UpdateNodePoolSpec(spec, params.systemLabels, nil)
			assert.NoError(t, err2)
			assert.Equal(t, spec, tc.expectedSpec)
		})
	}
}

func TestMultiNetworkingGenerator(t *testing.T) {
	networkAnnotation := "interface-config"
	network1Name := "networking.gke.io.networks/blue-net.IP"
	network1 := gkeclient.AdditionalNetworkConfig{
		VPCNetName:    network1Name,
		VPCSubnetName: "subnet",
	}
	network2Name := "networking.gke.io.networks/dpdk-net"
	network2 := gkeclient.AdditionalNetworkConfig{
		VPCNetName:     network2Name,
		VPCSubnetName:  "subnet",
		SubRange:       "range",
		MaxPodsPerNode: 10,
	}
	networks := map[string]gkeclient.AdditionalNetworkConfig{
		network1Name: network1,
		network2Name: network2,
	}
	matcher := mockMatcher{
		networkConfigs: networks,
	}
	g := NewMultiNetworkingGenerator(&matcher)
	for name, tc := range map[string]struct {
		networks         []string
		matcherErr       error
		wantNgReq        nodeGroupRequirements
		wantParameters   nodeGroupParameters
		wantNodePoolSpec gkeclient.NodePoolSpec
		wantErr          error
	}{
		"no networking resources": {
			wantNgReq: nodeGroupRequirements{
				networkAnnotation: networkAnnotation,
			},
			wantParameters: nodeGroupParameters{
				systemLabels: map[string]string{},
			},
		},
		"one networking resource": {
			networks: []string{network1Name},
			wantNgReq: nodeGroupRequirements{
				networkReq: podrequirements.NetworkingRequirements{
					AdditionalNetworkResources: []string{network1Name},
				},
				networkAnnotation: networkAnnotation,
			},
			wantParameters: nodeGroupParameters{
				extraResources: map[string]resource.Quantity{
					network1Name: *resource.NewQuantity(4, resource.DecimalSI),
				},
				systemLabels: map[string]string{
					netapi.InterfaceAnnotationKey: "interface-config",
				},
			},
			wantNodePoolSpec: gkeclient.NodePoolSpec{
				NetworkConfigs: []gkeclient.AdditionalNetworkConfig{network1},
			},
		},
		"two networking resources": {
			networks: []string{network1Name, network2Name},
			wantNgReq: nodeGroupRequirements{
				networkReq: podrequirements.NetworkingRequirements{
					AdditionalNetworkResources: []string{network1Name, network2Name},
				},
				networkAnnotation: networkAnnotation,
			},
			wantParameters: nodeGroupParameters{
				extraResources: map[string]resource.Quantity{
					network1Name: *resource.NewQuantity(4, resource.DecimalSI),
					network2Name: *resource.NewQuantity(1, resource.DecimalSI),
				},
				systemLabels: map[string]string{
					gkelabels.HighPerformanceNetworkLabel: "true",
					netapi.InterfaceAnnotationKey:         networkAnnotation,
				},
			},
			wantNodePoolSpec: gkeclient.NodePoolSpec{
				NetworkConfigs: []gkeclient.AdditionalNetworkConfig{network1, network2},
				Labels: map[string]string{
					gkelabels.HighPerformanceNetworkLabel: "true",
				},
			},
		},
		"error from matcher": {
			networks:   []string{network1Name, network2Name},
			matcherErr: fmt.Errorf("matcher error"),
			wantNgReq: nodeGroupRequirements{
				networkReq: podrequirements.NetworkingRequirements{
					AdditionalNetworkResources: []string{network1Name, network2Name},
				},
				networkAnnotation: networkAnnotation,
			},
			wantParameters: nodeGroupParameters{
				extraResources: map[string]resource.Quantity{
					network1Name: *resource.NewQuantity(4, resource.DecimalSI),
					network2Name: *resource.NewQuantity(1, resource.DecimalSI),
				},
				systemLabels: map[string]string{
					gkelabels.HighPerformanceNetworkLabel: "true",
					netapi.InterfaceAnnotationKey:         networkAnnotation,
				},
			},
			wantErr: fmt.Errorf("matcher error"),
		},
	} {
		t.Run(name, func(t *testing.T) {
			ngReq := nodeGroupRequirements{}
			matcher.err = tc.matcherErr
			var err error
			podReq := &podrequirements.Requirements{
				NetworkingReq: podrequirements.NetworkingRequirements{
					AdditionalNetworkResources: tc.networks,
				},
				NetworkingAnnotation: networkAnnotation,
			}
			err = g.UpdateRequirements(&ngReq, podReq, machinetypes.GpuRequest{}, TpuRequest{})
			assert.Nil(t, err)
			assert.Equal(t, ngReq, tc.wantNgReq)
			params := nodeGroupParameters{}
			params.systemLabels = make(map[string]string)
			err = g.UpdateParameters(&params, ngReq, NodeGroupOptions{})
			assert.Nil(t, err)
			assert.Equal(t, tc.wantParameters, params)
			spec := gkeclient.NodePoolSpec{}
			err = g.UpdateNodePoolSpec(&spec, params.systemLabels, tc.wantParameters.extraResources)
			assert.Equal(t, err, tc.wantErr)
			if diff := cmp.Diff(tc.wantNodePoolSpec.NetworkConfigs, spec.NetworkConfigs); diff != "" {
				t.Errorf("Unexpected NetworkConfigs in Node pool spec: %s", diff)
			}
			if diff := cmp.Diff(tc.wantNodePoolSpec.Labels, spec.Labels); diff != "" {
				t.Errorf("Unexpected Labels in Node pool spec: %s", diff)
			}
		})
	}
}

type mockMatcher struct {
	networkConfigs map[string]gkeclient.AdditionalNetworkConfig
	err            error
}

func (m *mockMatcher) GetNetworkingResourcesFromNetworkConfig(_ []gkeclient.AdditionalNetworkConfig) (map[string]resource.Quantity, error) {
	return nil, nil
}

func (m *mockMatcher) GetNetworkConfigFromResources(resources map[string]resource.Quantity, _ string) ([]gkeclient.AdditionalNetworkConfig, error) {
	if m.err != nil {
		return nil, m.err
	}
	var result []gkeclient.AdditionalNetworkConfig
	for res := range resources {
		if network, found := m.networkConfigs[res]; found {
			result = append(result, network)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].VPCNetName < result[j].VPCNetName
	})
	return result, nil
}

func TestPodCapacityLabelGenerator(t *testing.T) {
	for name, tc := range map[string]struct {
		podCapReq        string
		expectedErr      error
		expectedSpec     *gkeclient.NodePoolSpec
		autopilotEnabled bool
	}{
		"no pod capacity pod requirement autopilot": {
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{},
			},
			autopilotEnabled: true,
		},
		"pod capacity pod requirement autopilot": {
			podCapReq: "1",
			expectedSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{
					gkelabels.PodCapacityLabel: "1",
				},
			},
			autopilotEnabled: true,
		},
		"pod capacity pod requirement invalid autopilot": {
			podCapReq:        "ab",
			expectedErr:      NewIsolatedPodCapacityError("ab"),
			autopilotEnabled: true,
		},
		"pod capacity pod requirement not autopilot": {
			podCapReq:        "ab",
			expectedErr:      NewIsolatedPodNonAutopilotError(),
			autopilotEnabled: false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithAutopilotEnabled(tc.autopilotEnabled).Build()
			gen := NewPodCapacityLabelGenerator(provider)
			podReq := &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{}),
			}
			podReq.PodCapacity = tc.podCapReq

			ngReq := nodeGroupRequirements{}
			err := gen.UpdateRequirements(&ngReq, podReq, machinetypes.GpuRequest{}, TpuRequest{})
			if tc.expectedErr != nil {
				assert.Equal(t, err, tc.expectedErr)
				return
			}
			assert.NoError(t, err)
			params := &nodeGroupParameters{
				systemLabels: make(map[string]string),
			}
			opts := []NodeGroupOptions{
				{},
			}
			opts = gen.GenerateNodeGroupOptionsForRequirements(opts, ngReq)
			err2 := gen.UpdateParameters(params, ngReq, opts[0])
			assert.NoError(t, err2)
			spec := &gkeclient.NodePoolSpec{
				Labels: map[string]string{},
			}
			err2 = gen.UpdateNodePoolSpec(spec, params.systemLabels, nil)
			assert.NoError(t, err2)
			assert.Equal(t, spec, tc.expectedSpec)
		})
	}
}

func TestGenerateNodeGroupOptionsWithAdditionalConfig(t *testing.T) {
	for name, tc := range map[string]struct {
		req                         nodeGroupRequirements
		zones                       []string
		autoprovisioningLocations   []string
		extendedDuration            bool
		spotToleration              bool
		ekEdpEnabled                bool
		expectedCount               int
		expectedSpecificZone        string
		expectedSkippedZones        []string
		expectedSpecificMachineType string
	}{
		"no options generated due to no zone being set": {
			req:           nodeGroupRequirements{},
			zones:         []string{},
			expectedCount: 0,
		},
		"generated options for single zones": {
			req: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.E2, machinetypes.AnyPlatform, "", ""),
			},
			zones:         []string{"us-central1-a"},
			expectedCount: len(machinetypes.E2.AutoprovisionedMachineTypes(machinetypes.NoConstraints)),
		},
		"generation options for multiple zone": {
			req: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.E2, machinetypes.AnyPlatform, "", ""),
			},
			zones:         []string{"us-central1-a", "us-central1-c"},
			expectedCount: 2 * len(machinetypes.E2.AutoprovisionedMachineTypes(machinetypes.NoConstraints)),
		},
		"generate options with spot preemption": {
			req: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.E2, machinetypes.AnyPlatform, "", ""),
				pods:        []*apiv1.Pod{addSpotToleration(testPod("p1"))},
			},
			zones:          []string{"us-central1-a", "us-central1-c"},
			spotToleration: true,
			expectedCount:  4 * len(machinetypes.E2.AutoprovisionedMachineTypes(machinetypes.NoConstraints)),
		},
		"generate options with extended duration pods": {
			req: nodeGroupRequirements{
				machineSpec:               machinetypes.NewMachineSpecSingleFamily(machinetypes.E2, machinetypes.AnyPlatform, "", ""),
				extendedDurationPodCPUReq: "100m",
			},
			zones:            []string{"us-central1-a", "us-central1-c"},
			extendedDuration: true,
			expectedCount:    4 * len(machinetypes.E2.AutoprovisionedMachineTypes(machinetypes.NoConstraints)),
		},
		"generate options with missing reservation": {
			req: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.E2, machinetypes.AnyPlatform, "", ""),
				reservation: reservationRequirements{
					exists:  false,
					name:    "res1",
					project: "res-proj",
				},
			},
			zones:         []string{"us-central1-a", "us-central1-c"},
			expectedCount: 0,
		},
		"generate options with usable local reservation no specification": {
			req: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.E2, machinetypes.AnyPlatform, "", ""),
				reservation: reservationRequirements{
					exists: true,
					name:   "res1",
				},
			},
			zones:         []string{"us-central1-a", "us-central1-c"},
			expectedCount: 2 * len(machinetypes.E2.AutoprovisionedMachineTypes(machinetypes.NoConstraints)),
		},
		"generate options with usable local reservation": {
			req: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.E2, machinetypes.AnyPlatform, "", ""),
				reservation: reservationRequirements{
					exists:      true,
					name:        "res1",
					zone:        "us-central1-a",
					machineType: "e2-standard-2",
				},
			},
			zones:                       []string{"us-central1-a", "us-central1-c"},
			expectedCount:               1,
			expectedSpecificZone:        "us-central1-a",
			expectedSpecificMachineType: "e2-standard-2",
		},

		// This case won't work properly as MachineSelectionGenerator.GenerateNodeGroupOptionsForRequirements
		// will only generate options for known family types (in this case N2). Custom ones are not present in the option list
		// if not explicitly specified.
		// ReservationGenerator.GenerateNodeGroupOptionsForRequirements will compare machine types, notice that
		// there no matches and skip all options.

		// "generate options with usable local reservation, reservation uses custom machine type": {
		// 	req: nodeGroupRequirements{
		// 		machineSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.N2, machinetypes.AnyPlatform, "", ""),
		// 		reservation: reservationRequirements{
		// 			exists:      true,
		// 			name:        "res1",
		// 			zone:        "us-central1-a",
		// 			machineType: "n2-custom-32-131072",
		// 		},
		// 	},
		// 	zones:                       []string{"us-central1-a", "us-central1-c"},
		// 	expectedSpecificZone:        "us-central1-a",
		// 	expectedSpecificMachineType: "n2-custom-32-131072",
		// 	expectedCount:               1,
		// },
		"generate options with usable local reservation with custom machine type explicitly specified, reservation type and machineSpec type match": {
			req: nodeGroupRequirements{
				machineSpec: machinetypes.NewExplicitMachineSpec([]machinetypes.MachineFamily{machinetypes.E2}, machinetypes.AnyPlatform, "", "", []string{"e2-custom-16-131072"}),
				reservation: reservationRequirements{
					exists:      true,
					name:        "res1",
					zone:        "us-central1-a",
					machineType: "e2-custom-16-131072",
				},
			},
			zones:                       []string{"us-central1-a", "us-central1-c"},
			expectedCount:               1,
			expectedSpecificZone:        "us-central1-a",
			expectedSpecificMachineType: "e2-custom-16-131072",
		},
		"generate options with usable local reservation with custom machine type explicitly specified, reservation type and machineSpec type do not match": {
			req: nodeGroupRequirements{
				machineSpec: machinetypes.NewExplicitMachineSpec([]machinetypes.MachineFamily{machinetypes.E2}, machinetypes.AnyPlatform, "", "", []string{"e2-custom-20-131072"}),
				reservation: reservationRequirements{
					exists:      true,
					name:        "res1",
					zone:        "us-central1-a",
					machineType: "e2-custom-16-131072",
				},
			},
			zones:         []string{"us-central1-a", "us-central1-c"},
			expectedCount: 0,
		},
		"generate options with usable local reservation specifying block": {
			req: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.E2, machinetypes.AnyPlatform, "", ""),
				reservation: reservationRequirements{
					exists:      true,
					name:        "res1",
					zone:        "us-central1-a",
					block:       "res-block",
					machineType: "e2-standard-2",
				},
			},
			zones:                       []string{"us-central1-a", "us-central1-c"},
			expectedCount:               1,
			expectedSpecificZone:        "us-central1-a",
			expectedSpecificMachineType: "e2-standard-2",
		},
		"generate options with all requirements": { // This should not be happening in practice, since edps are not compatible with spot/preemptible.
			req: nodeGroupRequirements{
				pods:                      []*apiv1.Pod{addSpotToleration(testPod("p1"))},
				machineSpec:               machinetypes.NewMachineSpecSingleFamily(machinetypes.E2, machinetypes.AnyPlatform, "", ""),
				extendedDurationPodCPUReq: "100m",
			},
			zones:            []string{"us-central1-a", "us-central1-c"},
			extendedDuration: true,
			spotToleration:   true,
			expectedCount:    8 * len(machinetypes.E2.AutoprovisionedMachineTypes(machinetypes.NoConstraints)),
		},
		"filter options with zones not matching specified zones": {
			req: nodeGroupRequirements{
				machineSpec:    machinetypes.NewMachineSpecSingleFamily(machinetypes.E2, machinetypes.AnyPlatform, "", ""),
				specifiedZones: []string{"us-central1-a"},
			},
			zones:                []string{"us-central1-a", "us-central1-b"},
			expectedCount:        len(machinetypes.E2.AutoprovisionedMachineTypes(machinetypes.NoConstraints)),
			expectedSkippedZones: []string{"us-central1-b"},
		},
		"filter options with zones not matching specified zones, doesn't break other generators": {
			req: nodeGroupRequirements{
				machineSpec:    machinetypes.NewMachineSpecSingleFamily(machinetypes.E2, machinetypes.AnyPlatform, "", ""),
				specifiedZones: []string{"us-central1-a"},
				pods:           []*apiv1.Pod{addSpotToleration(testPod("p1"))},
			},
			zones:                []string{"us-central1-a", "us-central1-b"},
			spotToleration:       true,
			expectedCount:        2 * len(machinetypes.E2.AutoprovisionedMachineTypes(machinetypes.NoConstraints)),
			expectedSkippedZones: []string{"us-central1-b"},
		},
		"no specified zones - fallback to autoprovisioning locations": {
			req: nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.E2, machinetypes.AnyPlatform, "", ""),
			},
			zones:                     []string{"us-central1-a", "us-central1-c"},
			autoprovisioningLocations: []string{"us-central1-a"},
			expectedCount:             len(machinetypes.E2.AutoprovisionedMachineTypes(machinetypes.NoConstraints)),
			expectedSkippedZones:      []string{"us-central1-c"},
		},
		"specified zones (available in cluster region) outside of autoprovisioningLocations - option found": {
			req: nodeGroupRequirements{
				machineSpec:    machinetypes.NewMachineSpecSingleFamily(machinetypes.E2, machinetypes.AnyPlatform, "", ""),
				specifiedZones: []string{"us-central1-a"},
				pods:           []*apiv1.Pod{addSpotToleration(testPod("p1"))},
			},
			zones:                     []string{"us-central1-a", "us-central1-b", "us-central1-c"},
			autoprovisioningLocations: []string{"us-central1-b"},
			spotToleration:            true,
			expectedCount:             2 * len(machinetypes.E2.AutoprovisionedMachineTypes(machinetypes.NoConstraints)),
			expectedSkippedZones:      []string{"us-central1-b", "us-central1-c"},
		},
		"specified zones (not available in cluster region) outside of autoprovisioningLocations - no option found": {
			req: nodeGroupRequirements{
				machineSpec:    machinetypes.NewMachineSpecSingleFamily(machinetypes.E2, machinetypes.AnyPlatform, "", ""),
				specifiedZones: []string{"us-central1-a"},
				pods:           []*apiv1.Pod{addSpotToleration(testPod("p1"))},
			},
			zones:                     []string{"us-central1-b"},
			autoprovisioningLocations: []string{"us-central1-b"},
			spotToleration:            true,
			expectedCount:             0,
			expectedSkippedZones:      []string{"us-central1-b"},
		},
		"generate options with all requirements on EKs with X extendedDurationPodCPUReq": { // This should not be happening in practice, since edps are not compatible with spot/preemptible.
			req: nodeGroupRequirements{
				pods:                      []*apiv1.Pod{addSpotToleration(testPod("p1"))},
				machineSpec:               machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.EK}, machinetypes.AnyPlatform, "", ""),
				extendedDurationPodCPUReq: "X",
			},
			zones:            []string{"us-central1-a", "us-central1-c"},
			extendedDuration: true,
			spotToleration:   true,
			ekEdpEnabled:     true,
			expectedCount:    8 * len(machinetypes.EK.AutoprovisionedMachineTypes(machinetypes.NoConstraints)),
		},
		"generate options with extended duration pods on EKs": {
			req: nodeGroupRequirements{
				machineSpec:               machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.EK}, machinetypes.AnyPlatform, "", ""),
				extendedDurationPodCPUReq: "100m",
			},
			zones:            []string{"us-central1-a", "us-central1-c"},
			extendedDuration: true,
			ekEdpEnabled:     true,
			expectedCount:    4 * len(machinetypes.EK.AutoprovisionedMachineTypes(machinetypes.NoConstraints)),
		},
		"generate options with all requirements on EKs": { // This should not be happening in practice, since edps are not compatible with spot/preemptible.
			req: nodeGroupRequirements{
				pods:                      []*apiv1.Pod{addSpotToleration(testPod("p1"))},
				machineSpec:               machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.EK}, machinetypes.AnyPlatform, "", ""),
				extendedDurationPodCPUReq: "100m",
			},
			zones:            []string{"us-central1-a", "us-central1-c"},
			extendedDuration: true,
			spotToleration:   true,
			expectedCount:    8 * len(machinetypes.EK.AutoprovisionedMachineTypes(machinetypes.NoConstraints)),
		},
		"generate options with extended duration pods on EKs with X extendedDurationPodCPUReq": {
			req: nodeGroupRequirements{
				machineSpec:               machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.EK}, machinetypes.AnyPlatform, "", ""),
				extendedDurationPodCPUReq: "X",
			},
			zones:            []string{"us-central1-a", "us-central1-c"},
			extendedDuration: true,
			ekEdpEnabled:     true,
			expectedCount:    4 * len(machinetypes.EK.AutoprovisionedMachineTypes(machinetypes.NoConstraints)),
		},
		"generate options with extended duration pods on E2s with X extendedDurationPodCPUReq": {
			req: nodeGroupRequirements{
				machineSpec:               machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.E2}, machinetypes.AnyPlatform, "", ""),
				extendedDurationPodCPUReq: "X",
			},
			zones:            []string{"us-central1-a", "us-central1-c"},
			extendedDuration: true,
			ekEdpEnabled:     true,
			expectedCount:    0,
		},
		"generate options with extended duration pods on E2s and EKs with X extendedDurationPodCPUReq": {
			req: nodeGroupRequirements{
				machineSpec:               machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.E2, machinetypes.EK}, machinetypes.AnyPlatform, "", ""),
				extendedDurationPodCPUReq: "X",
			},
			zones:            []string{"us-central1-a", "us-central1-c"},
			extendedDuration: true,
			ekEdpEnabled:     true,
			expectedCount:    4 * len(machinetypes.EK.AutoprovisionedMachineTypes(machinetypes.NoConstraints)),
		},
	} {
		t.Run(name, func(t *testing.T) {
			if tc.autoprovisioningLocations == nil {
				tc.autoprovisioningLocations = tc.zones
			}
			allMachineTypes := []string{}
			for _, family := range tc.req.machineSpec.Families {
				for machineType := range family.AllMachineTypes(machinetypes.NoConstraints) {
					allMachineTypes = append(allMachineTypes, machineType)
				}
			}
			allMachineTypes = append(allMachineTypes, tc.req.machineSpec.ExplicitMachineTypes...)
			machineTypesPerZone := map[string][]string{}
			for _, zone := range tc.zones {
				machineTypesPerZone[zone] = allMachineTypes
			}
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithAutopilotEnabled(true).
				WithEkEdpEnabled(tc.ekEdpEnabled).
				WithAllZones(tc.zones...).
				WithAutoprovisioningLocations(tc.autoprovisioningLocations...).
				WithMachineTypesPerZone(machineTypesPerZone).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			m := NewAutoprovisioningNodeGroupManager(AutoprovisioningNodeGroupManagerOptions{
				CloudProvider: provider,
				Flags: AutoprovisioningNodeGroupManagerFlags{
					ReservationFlags: ReservationFlags{
						SpecificTypeReservationMatchEnabled: true,
					},
					EnableUserAnyZoneSelection: true,
				},
				OptionsTracker:                tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, experiments.NewMockManager()),
				PodLister:                     kube_util.NewTestPodLister(tc.req.pods),
				ResizableMachineTypesProvider: config.NewSimpleStringSetProvider(allMachineTypes),
				ResourcePolicyPuller:          &placement.FakeResourcePolicyPullerProvider{},
				ExperimentsManager:            experiments.NewMockManager(),
			})
			ctx := &injectionContext{
				zones: tc.zones,
			}
			options := m.generateNodeGroupOptions(ctx, tc.req)
			assert.Equal(t, tc.expectedCount, len(options))

			// validate half of all the options are extended duration
			if tc.extendedDuration {
				count := 0
				for _, option := range options {
					if option.ExtendedDurationPodCPUReq != "" {
						count++
					} else {
						count--
					}
				}
				assert.Zero(t, count)
			}

			// validate half of the options are spot
			if tc.spotToleration {
				count := 0
				for _, option := range options {
					if option.Preemption.IsSpot() {
						count++
					} else {
						count--
					}
				}
				assert.Zero(t, count)
			}

			if tc.expectedSpecificMachineType != "" {
				for _, option := range options {
					assert.Equal(t, tc.expectedSpecificMachineType, option.MachineType)
				}
			}

			if tc.expectedSpecificZone != "" {
				for _, option := range options {
					assert.Equal(t, tc.expectedSpecificZone, option.Zone)
				}
			}
			expectedEmptyZones := make(map[string]bool)
			if len(tc.expectedSkippedZones) != 0 {
				for _, zone := range tc.expectedSkippedZones {
					expectedEmptyZones[zone] = true
				}
				for _, option := range options {
					_, fail := expectedEmptyZones[option.Zone]
					assert.False(t, fail)
				}
			}
			if len(options) > 0 && tc.expectedSpecificZone == "" {
				// validate zone split
				zonalMap := map[string]int{}
				allZones := []string{}
				for _, option := range options {
					if _, f := zonalMap[option.Zone]; !f {
						zonalMap[option.Zone] = 0
						allZones = append(allZones, option.Zone)
					}
					zonalMap[option.Zone]++
				}
				var expectedZones []string
				for _, zone := range tc.zones {
					if _, skip := expectedEmptyZones[zone]; !skip {
						expectedZones = append(expectedZones, zone)
					}
				}
				assert.Equal(t, expectedZones, allZones)
				if len(tc.zones) != 0 {
					expectedZoneCount := tc.expectedCount / len(expectedZones)
					for _, v := range zonalMap {
						assert.Equal(t, expectedZoneCount, v)
					}
				}
			}
		})
	}
}

func TestApplyReducedZoneSetOptimisation(t *testing.T) {
	for tn, tc := range map[string]struct {
		options     []NodeGroupOptions
		wantOptions []NodeGroupOptions
	}{
		"no options": {
			options:     []NodeGroupOptions{},
			wantOptions: []NodeGroupOptions{},
		},
		"one option": {
			options: []NodeGroupOptions{
				{Zone: "zone-a", MachineType: "n1-standard-1"},
			},
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-a", MachineType: "n1-standard-1"},
			},
		},
		"multiple options, one group": {
			options: []NodeGroupOptions{
				{Zone: "zone-a", MachineType: "n1-standard-1"},
				{Zone: "zone-b", MachineType: "n1-standard-1"},
				{Zone: "zone-c", MachineType: "n1-standard-1"},
			},
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-a", MachineType: "n1-standard-1"},
			},
		},
		"multiple groups of options": {
			options: []NodeGroupOptions{
				{Zone: "zone-a", MachineType: "n1-standard-1"},
				{Zone: "zone-b", MachineType: "n1-standard-1"},
				{Zone: "zone-a", MachineType: "n1-standard-2"},
				{Zone: "zone-b", MachineType: "n1-standard-2"},
				{Zone: "zone-c", MachineType: "n1-standard-2"},
			},
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-a", MachineType: "n1-standard-1"},
				{Zone: "zone-a", MachineType: "n1-standard-2"},
			},
		},
		"multiple groups with different preemption": {
			options: []NodeGroupOptions{
				{Zone: "zone-a", MachineType: "n1-standard-1", Preemption: preemption.Spot},
				{Zone: "zone-b", MachineType: "n1-standard-1", Preemption: preemption.Spot},
				{Zone: "zone-a", MachineType: "n1-standard-1", Preemption: preemption.NoPreemption},
			},
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-a", MachineType: "n1-standard-1", Preemption: preemption.Spot},
				{Zone: "zone-a", MachineType: "n1-standard-1", Preemption: preemption.NoPreemption},
			},
		},
		"multiple groups with different extended duration pod cpu req": {
			options: []NodeGroupOptions{
				{Zone: "zone-a", MachineType: "n1-standard-1", ExtendedDurationPodCPUReq: "100m"},
				{Zone: "zone-b", MachineType: "n1-standard-1", ExtendedDurationPodCPUReq: "100m"},
				{Zone: "zone-a", MachineType: "n1-standard-1", ExtendedDurationPodCPUReq: "200m"},
			},
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-a", MachineType: "n1-standard-1", ExtendedDurationPodCPUReq: "100m"},
				{Zone: "zone-a", MachineType: "n1-standard-1", ExtendedDurationPodCPUReq: "200m"},
			},
		},
		"multiple groups with all fields": {
			options: []NodeGroupOptions{
				{Zone: "zone-a", MachineType: "n1-standard-1", Preemption: preemption.Spot, ExtendedDurationPodCPUReq: "100m", PodIsolationCPUReq: "100m", PodCapacity: 1, MaxPodsPerNode: 110, DynamicMaxPodsPerNode: true},
				{Zone: "zone-b", MachineType: "n1-standard-1", Preemption: preemption.Spot, ExtendedDurationPodCPUReq: "100m", PodIsolationCPUReq: "100m", PodCapacity: 1, MaxPodsPerNode: 110, DynamicMaxPodsPerNode: true},
				{Zone: "zone-a", MachineType: "n1-standard-1", Preemption: preemption.NoPreemption, ExtendedDurationPodCPUReq: "200m", PodIsolationCPUReq: "200m", PodCapacity: 2, MaxPodsPerNode: 110, DynamicMaxPodsPerNode: false},
			},
			wantOptions: []NodeGroupOptions{
				{Zone: "zone-a", MachineType: "n1-standard-1", Preemption: preemption.Spot, ExtendedDurationPodCPUReq: "100m", PodIsolationCPUReq: "100m", PodCapacity: 1, MaxPodsPerNode: 110, DynamicMaxPodsPerNode: true},
				{Zone: "zone-a", MachineType: "n1-standard-1", Preemption: preemption.NoPreemption, ExtendedDurationPodCPUReq: "200m", PodIsolationCPUReq: "200m", PodCapacity: 2, MaxPodsPerNode: 110, DynamicMaxPodsPerNode: false},
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			m := &AutoprovisioningNodeGroupManager{
				randInt: func(max int) int { return 0 },
			}
			gotOptions := m.applyReducedZoneSetOptimisation(tc.options)
			assert.ElementsMatch(t, tc.wantOptions, gotOptions)
		})
	}
}

func TestValidateRequirements(t *testing.T) {
	e2NodeGroupName := "machine family \"e2\""
	nonBulkPod := test.BuildTestPod("non-bulk-pod", 1, 1)
	bulkPod := buildBulkProvisioningPod("bulk-pod")

	for name, tc := range map[string]struct {
		ngReq                               *nodeGroupRequirements
		err                                 errors.AutoscalerError
		nodePoolConsolidationDelayDisabled  bool
		provisioningRequestBulkMigsDisabled bool
	}{
		"incorrect machine for cpu platform": {
			ngReq: &nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.E2, machinetypes.IntelHaswell, "", ""),
			},
			err: machineselection.NewMinCpuPlatformInvalidError(e2NodeGroupName, "Intel Haswell"),
		},
		"incorrect machine for gpu": {
			ngReq: &nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.E2, machinetypes.AnyPlatform, "nvidia-tesla-a100", ""),
			},
			err: machineselection.NewGpuIncompatibleError(e2NodeGroupName, "nvidia-tesla-a100"),
		},
		"incorrect machine for tpu": {
			ngReq: &nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.E2, machinetypes.AnyPlatform, "", "tpu-v4-podslice"),
			},
			err: machineselection.NewTpuIncompatibleError(e2NodeGroupName, "tpu-v4-podslice"),
		},
		"incorrect compact placement": {
			ngReq: &nodeGroupRequirements{
				machineSpec:    machinetypes.NewMachineSpecSingleFamily(machinetypes.E2, machinetypes.AnyPlatform, "", ""),
				placementGroup: placement.FromLabels(map[string]string{gkelabels.PlacementGroupLabel: "placement-id"}),
			},
			err: placement.NewInvalidMachineFamilyError("Group: 'placement-id'", "specified machine families [\"e2\"] don't support compact placement"),
		},
		"incorrect machine for DWS and tpu": {
			ngReq: &nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.CT3P, machinetypes.AnyPlatform, "", gkelabels.TpuV3DeviceValue),
				tpuRequest: TpuRequest{
					TpuType:      gkelabels.TpuV3DeviceValue,
					ChipsPerNode: 4,
					Topology:     "4x4",
				},
				flexStartReq:            flexStartRequirements{enabled: true},
				maxRunDurationInSeconds: "3600",
			},
			err: NewInvalidDwsMachineFamilyError([]string{machinetypes.CT3P.Name()}),
		},
		"correct set up for min cpu platform": {
			ngReq: &nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.N1, machinetypes.IntelSkylake, "", ""),
			},
		},
		"correct set up for gpu": {
			ngReq: &nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.A2, machinetypes.AnyPlatform, "nvidia-tesla-a100", ""),
			},
		},
		"correct set up for tpu": {
			ngReq: &nodeGroupRequirements{
				machineSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.CT5L, machinetypes.AnyPlatform, "", "tpu-v5-lite-device"),
			},
		},
		"correct set up for compact placement": {
			ngReq: &nodeGroupRequirements{
				machineSpec:    machinetypes.NewMachineSpecSingleFamily(machinetypes.C2, machinetypes.AnyPlatform, "", ""),
				placementGroup: placement.FromLabels(map[string]string{gkelabels.PlacementGroupLabel: "placement-id"}),
			},
		},
		"correct set up for no reservation": {
			ngReq: &nodeGroupRequirements{},
		},
		"incorrect set up for affinity - nonesense affinity": {
			ngReq: &nodeGroupRequirements{
				reservation: reservationRequirements{
					name:        "res1",
					affinity:    "foobar",
					machineType: "ssv-normandy-sr1",
				},
			},
			err: reservations.NewUnsupportedReservationAffinityError("foobar", "must be one of the supported values of [any any-reservation-then-fail none specific] or not set"),
		},
		"incorrect set up for specific affinity reservation - no reservation": {
			ngReq: &nodeGroupRequirements{
				reservation: reservationRequirements{
					affinity: "specific",
				},
			},
			err: reservations.NewUnsupportedReservationAffinityError("specific", "unsupported to both specify no reservation and specific reservation affinity"),
		},
		"incorrect set up for any affinity reservation - has reservation": {
			ngReq: &nodeGroupRequirements{
				reservation: reservationRequirements{
					name:     "res1",
					affinity: "any",
				},
			},
			err: reservations.NewUnsupportedReservationAffinityError("any", "unsupported to specify a reservation and not specify specific reservation affinity"),
		},
		"incorrect set up for any affinity reservation - has tpu request": {
			ngReq: &nodeGroupRequirements{
				machineSpec: machinetypes.MachineSpec{
					TpuType: "tpu-v6e-slice",
				},
				tpuRequest: TpuRequest{
					TpuType:      "tpu-v6e-slice",
					ChipsPerNode: 4,
					Topology:     "4x4",
				},
				reservation: reservationRequirements{
					affinity: "any",
				},
			},
			err: reservations.NewUnsupportedReservationAffinityError("any", "reservation affinity any is not supported with TPUs"),
		},
		"incorrect set up for specific affinity reservation - reservation block with no reservation name": {
			ngReq: &nodeGroupRequirements{
				reservation: reservationRequirements{
					affinity: "specific",
					name:     "",
					project:  "res-project",
					block:    "res-block",
				},
			},
			err: reservations.NewErrUnusableReservation(
				gceclient.ReservationRef{Project: "res-project", BlockName: "res-block"},
				"Specifying reservation block without reservation name"),
		},
		"incorrect set up for specific affinity reservation - reservation subblock with no reservation block": {
			ngReq: &nodeGroupRequirements{
				reservation: reservationRequirements{
					affinity: "specific",
					name:     "name",
					project:  "res-project",
					subBlock: "res-subblock",
				},
			},
			err: reservations.NewErrUnusableReservation(
				gceclient.ReservationRef{Name: "name", Project: "res-project", SubBlockName: "res-subblock"},
				"Specifying reservation subblock without reservation block"),
		},
		"queued provisioning enabled - incorrect afinity reservation: specific": {
			ngReq: &nodeGroupRequirements{
				queuedProvisioningReq: podrequirements.QueuedProvisioningRequirements{Enabled: true},
				reservation:           reservationRequirements{affinity: "specific"},
			},
			err: reservations.NewUnsupportedReservationAffinityError("specific", "Provisioning Requests don't support reservations"),
		},
		"queued provisioning enabled - correct afinity reservation: not set": {
			ngReq: &nodeGroupRequirements{
				queuedProvisioningReq: podrequirements.QueuedProvisioningRequirements{Enabled: true},
				reservation:           reservationRequirements{},
			},
		},
		"queued provisioning enabled - correct afinity reservation: none": {
			ngReq: &nodeGroupRequirements{
				queuedProvisioningReq: podrequirements.QueuedProvisioningRequirements{Enabled: true},
				reservation:           reservationRequirements{affinity: "none"},
			},
		},
		"queued provisioning enabled - correct afinity reservation: empty": {
			ngReq: &nodeGroupRequirements{
				queuedProvisioningReq: podrequirements.QueuedProvisioningRequirements{Enabled: true},
				reservation:           reservationRequirements{affinity: ""},
			},
		},
		"queued provisioning enabled, no pods, MRD is set - throws": {
			ngReq: &nodeGroupRequirements{
				queuedProvisioningReq:   podrequirements.QueuedProvisioningRequirements{Enabled: true},
				maxRunDurationInSeconds: "3600",
			},
			err: errors.NewAutoscalerError(errors.ConfigurationError, "MaxRunDuration cannot be set on non-bulk QueuedProvisioning pools."),
		},
		"queued provisioning enabled, pods with BulkProvisioning=false, MRD is set - throws": {
			ngReq: &nodeGroupRequirements{
				queuedProvisioningReq:   podrequirements.QueuedProvisioningRequirements{Enabled: true},
				maxRunDurationInSeconds: "3600",
				pods: []*apiv1.Pod{
					nonBulkPod,
				},
			},
			err: errors.NewAutoscalerError(errors.ConfigurationError, "MaxRunDuration cannot be set on non-bulk QueuedProvisioning pools."),
		},
		"queued provisioning enabled, experiment disabled, has pod with BulkProvisioning=true, MRD is set - throws": {
			ngReq: &nodeGroupRequirements{
				queuedProvisioningReq:   podrequirements.QueuedProvisioningRequirements{Enabled: true},
				maxRunDurationInSeconds: "3600",
				pods: []*apiv1.Pod{
					bulkPod,
				},
			},
			provisioningRequestBulkMigsDisabled: true,
			err:                                 errors.NewAutoscalerError(errors.ConfigurationError, "MaxRunDuration cannot be set on non-bulk QueuedProvisioning pools."),
		},
		"queued provisioning enabled, experiment enabled, has pod with BulkProvisioning=true, MRD is set - ok": {
			ngReq: &nodeGroupRequirements{
				queuedProvisioningReq:   podrequirements.QueuedProvisioningRequirements{Enabled: true},
				maxRunDurationInSeconds: "3600",
				pods: []*apiv1.Pod{
					bulkPod,
				},
			},
		},
		"MRD is below the minimum allowed - throws": {
			ngReq: &nodeGroupRequirements{
				maxRunDurationInSeconds: strconv.FormatInt(minMRDInSeconds-1, 10),
			},
			err: errors.NewAutoscalerError(errors.ConfigurationError, "MaxRunDuration is not within the allowed range (30 seconds - 120 days). Got 29 seconds."),
		},
		"MRD is above the maximum allowed - throws": {
			ngReq: &nodeGroupRequirements{
				maxRunDurationInSeconds: strconv.FormatInt(maxMRDInSeconds+1, 10),
			},
			err: errors.NewAutoscalerError(errors.ConfigurationError, "MaxRunDuration is not within the allowed range (30 seconds - 120 days). Got 10368001 seconds."),
		},
		"MRD is not a number - throws": {
			ngReq: &nodeGroupRequirements{
				maxRunDurationInSeconds: "12h",
			},
			err: errors.NewAutoscalerError(errors.ConfigurationError, "MaxRunDuration is not a valid int64."),
		},
		"MRD is valid": {
			ngReq: &nodeGroupRequirements{
				maxRunDurationInSeconds: "3600",
			},
		},
		"MRD is valid with FlexStart": {
			ngReq: &nodeGroupRequirements{
				flexStartReq:            flexStartRequirements{enabled: true},
				maxRunDurationInSeconds: "3600",
			},
		},
		"MRD more than allowed with FlexStart": {
			ngReq: &nodeGroupRequirements{
				flexStartReq:            flexStartRequirements{enabled: true},
				maxRunDurationInSeconds: "1209600", // 14 days
			},
			err: caerrors.NewAutoscalerErrorf(caerrors.ConfigurationError, "MaxRunDuration is not within the allowed range (10 minutes - 7 days). Got 1209600 seconds."),
		},
		"MRD less than allowed with FlexStart": {
			ngReq: &nodeGroupRequirements{
				flexStartReq:            flexStartRequirements{enabled: true},
				maxRunDurationInSeconds: "300", // 5 minutes
			},
			err: caerrors.NewAutoscalerErrorf(caerrors.ConfigurationError, "MaxRunDuration is not within the allowed range (10 minutes - 7 days). Got 300 seconds."),
		},
		"ConsolidationDelay is not a number": {
			ngReq: &nodeGroupRequirements{
				consolidationDelayInSeconds: "12h",
			},
			err: errors.NewAutoscalerError(errors.ConfigurationError, "ConsolidationDelay is not a valid int64."),
		},
		"ConsolidationDelay is below the minimum allowed": {
			ngReq: &nodeGroupRequirements{
				consolidationDelayInSeconds: "50",
			},
			err: errors.NewAutoscalerError(errors.ConfigurationError, "ConsolidationDelay is not within the allowed range (1 minute - 1 day). Got 50 seconds."),
		},
		"ConsolidationDelay is above the maximum allowed": {
			ngReq: &nodeGroupRequirements{
				consolidationDelayInSeconds: strconv.FormatInt(24*60*60+1, 10),
			},
			err: errors.NewAutoscalerError(errors.ConfigurationError, "ConsolidationDelay is not within the allowed range (1 minute - 1 day). Got 86401 seconds."),
		},
		"experiment disabled, ConsolidationDelay is above the maximum allowed - doesn't validate": {
			ngReq: &nodeGroupRequirements{
				consolidationDelayInSeconds: strconv.FormatInt(24*60*60+1, 10),
			},
			nodePoolConsolidationDelayDisabled: true,
		},
		"ConsolidationDelay is valid": {
			ngReq: &nodeGroupRequirements{
				consolidationDelayInSeconds: strconv.FormatInt(24*60*60, 10), // 24 hours
			},
		},
		"correct set up for specific affinity reservation - no affinity, assume specific": {
			ngReq: &nodeGroupRequirements{
				reservation: reservationRequirements{
					name: "res1",
				},
			},
		},
		"correct set up for reservation": {
			ngReq: &nodeGroupRequirements{
				reservation: reservationRequirements{
					name:        "res1",
					affinity:    "specific",
					machineType: "ssv-normandy-sr1",
				},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			exps := []string{}
			if !tc.nodePoolConsolidationDelayDisabled {
				exps = append(exps, experiments.NodePoolConsolidationDelayMinCAVersionFlag)
			}
			if !tc.provisioningRequestBulkMigsDisabled {
				exps = append(exps, experiments.ProvisioningRequestBulkMigsFlag)
			}
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithAutopilotEnabled(true).Build()
			em := experiments.NewMockManager(exps...)
			m := NewAutoprovisioningNodeGroupManager(AutoprovisioningNodeGroupManagerOptions{
				CloudProvider: provider,
				Flags: AutoprovisioningNodeGroupManagerFlags{
					ProvisioningLabelEnabled:   true,
					TpuAutoprovisioningEnabled: true,
					ReservationFlags: ReservationFlags{
						SpecificTypeReservationsEnabled: true,
					},
				},
				ExperimentsManager:   em,
				OptionsTracker:       tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
				ResourcePolicyPuller: &placement.FakeResourcePolicyPullerProvider{},
			})
			var err error
			for _, gen := range m.specGenerators {
				err = gen.ValidateRequirements(tc.ngReq)
				if err != nil {
					break
				}
			}
			assert.Equal(t, tc.err, err)
		})
	}
}

func TestReservations_ExtractRequirements(t *testing.T) {
	// provider's hardcoded projectID for local project is 12345
	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithAutoprovisioningEnabled(true).
		WithMachineTypes("test-machine-type", "n2").
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		Build()

	res1 := reservations.BuildSingleMachineReservation("n2-standard-2", "https://www.googleapis.com/compute/v1/projects/12345/zones/us-central1-a")
	res1.Id = 1
	res1.Name = "res-name"
	res1.SelfLink = "https://www.googleapis.com/compute/v1/projects/12345/zones/us-central1-a/reservations/res-name"
	res1.SpecificReservationRequired = true

	res2 := reservations.BuildSingleMachineReservation("e2-standard-2", "https://www.googleapis.com/compute/v1/projects/12345/zones/us-central1-b")
	res2.Id = 2
	res2.Name = "res-name"
	res2.SelfLink = "https://www.googleapis.com/compute/v1/projects/12345/zones/us-central1-b/reservations/res-name"
	res2.SpecificReservationRequired = true

	res1Any := reservations.BuildSingleMachineReservation("e2-standard-2", "https://www.googleapis.com/compute/v1/projects/12345/zones/us-central1-a")
	res1Any.Name = res1.Name
	res1Any.SelfLink = res1.SelfLink
	res1Any.SpecificReservationRequired = false

	resShared := reservations.BuildSingleMachineReservation("n2-standard-2", "https://www.googleapis.com/compute/v1/projects/res-shared-proj1/zones/us-central1-a")
	resShared.Name = "res-name"
	resShared.SelfLink = "https://www.googleapis.com/compute/v1/projects/res-shared-proj/zones/us-central1-a/reservations/res-name"
	resShared.SpecificReservationRequired = true

	resTpu := reservations.BuildAggregateReservation("https://www.googleapis.com/compute/v1/projects/12345/zones/us-central1-a")
	resTpu.Name = "res-name"
	resTpu.SelfLink = "https://www.googleapis.com/compute/v1/projects/12345/zones/us-central1-a/reservations/res-name"
	resTpu.SpecificReservationRequired = true

	resPlacementPolicy1 := reservations.BuildSingleMachineReservation("n2-standard-2", "https://www.googleapis.com/compute/v1/projects/12345/zones/us-central1-a")
	resPlacementPolicy1.Name = res1.Name
	resPlacementPolicy1.SelfLink = res1.SelfLink
	resPlacementPolicy1.SpecificReservationRequired = true
	resPlacementPolicy1.ResourcePolicies = make(map[string]string, 1)
	resPlacementPolicy1.ResourcePolicies["policy"] = "projects/12345/regions/us-central1-a/resourcePolicies/test-policy"

	resPlacementPolicy2 := reservations.BuildSingleMachineReservation("n2-standard-2", "https://www.googleapis.com/compute/v1/projects/12345/zones/us-central1-a")
	resPlacementPolicy2.Name = res1.Name
	resPlacementPolicy2.SelfLink = res1.SelfLink
	resPlacementPolicy2.SpecificReservationRequired = true
	resPlacementPolicy2.ResourcePolicies = make(map[string]string, 1)
	resPlacementPolicy2.ResourcePolicies["placement"] = "projects/12345/regions/us-central1-a/resourcePolicies/test-policy"

	resInvalidPlacementPolicy := reservations.BuildSingleMachineReservation("n2-standard-2", "https://www.googleapis.com/compute/v1/projects/12345/zones/us-central1-a")
	resInvalidPlacementPolicy.Name = res1.Name
	resInvalidPlacementPolicy.SelfLink = res1.SelfLink
	resInvalidPlacementPolicy.SpecificReservationRequired = true
	resInvalidPlacementPolicy.ResourcePolicies = make(map[string]string, 1)
	resInvalidPlacementPolicy.ResourcePolicies["placement"] = "invalid-test-policy"

	resPlacementPolicyShared := reservations.BuildSingleMachineReservation("e2-standard-2", "https://www.googleapis.com/compute/v1/projects/other-project/zones/us-central1-a")
	resPlacementPolicyShared.Name = res1.Name
	resPlacementPolicyShared.SelfLink = "https://www.googleapis.com/compute/v1/projects/other-project/zones/us-central1-a/reservations/res-name"
	resPlacementPolicyShared.SpecificReservationRequired = true
	resPlacementPolicyShared.ResourcePolicies = make(map[string]string, 1)
	resPlacementPolicyShared.ResourcePolicies["placement"] = "projects/other-project/regions/us-central1-a/resourcePolicies/test-policy"

	ccName := "reservation-cc"
	ccLabel := gkelabels.ComputeClassLabel
	resCCRule := rules.NewRule(
		rules.WithReservationsRule(rules.NewReservation().WithReservationName("res-name").WithReservationProject("12345").WithReservationPath("res-name")),
	)
	resCC := computeclass.NewTestCrd(
		computeclass.WithName(ccName),
		computeclass.WithRules([]rules.Rule{resCCRule}),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithLabel(ccLabel),
	)
	resCCWithZonesRule := rules.NewRule(
		rules.WithReservationsRule(rules.NewReservation().WithReservationName("res-name").WithReservationProject("12345").WithReservationZones([]string{"us-central1-a"}).WithReservationPath("res-name")),
	)
	resCCWithZones := computeclass.NewTestCrd(
		computeclass.WithName(ccName),
		computeclass.WithRules([]rules.Rule{resCCWithZonesRule}),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithLabel(ccLabel),
	)
	resCCWithOtherZonesRule := rules.NewRule(
		rules.WithReservationsRule(rules.NewReservation().WithReservationName("res-name").WithReservationProject("12345").WithReservationZones([]string{"us-central1-c"}).WithReservationPath("res-name")),
	)
	resCCWithOtherZones := computeclass.NewTestCrd(
		computeclass.WithName(ccName),
		computeclass.WithRules([]rules.Rule{resCCWithOtherZonesRule}),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithLabel(ccLabel),
	)
	resAnyCCRule := rules.NewRule(rules.WithReservationsRule(rules.NewReservation().WithReservationAffinity(reservations.AnyAffinity)))
	resNoneCCRule := rules.NewRule(rules.WithReservationsRule(rules.NewReservation().WithReservationAffinity(reservations.NoneAffinity)))
	resAnyCC := computeclass.NewTestCrd(
		computeclass.WithName(ccName),
		computeclass.WithRules([]rules.Rule{resAnyCCRule}),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithLabel(ccLabel),
	)
	resNoneCC := computeclass.NewTestCrd(
		computeclass.WithName(ccName),
		computeclass.WithRules([]rules.Rule{resNoneCCRule}),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithLabel(ccLabel),
	)
	resBadAffinityCCRule := rules.NewRule(
		rules.WithReservationsRule(rules.NewReservation().WithReservationName("res-name").WithReservationAffinity("foobar").WithReservationProject("12345").WithReservationPath("12345")),
	)
	resBadAffinityCC := computeclass.NewTestCrd(
		computeclass.WithName(ccName),
		computeclass.WithRules([]rules.Rule{resBadAffinityCCRule}),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithLabel(ccLabel),
	)
	resSharedCCRule := rules.NewRule(
		rules.WithReservationsRule(rules.NewReservation().WithReservationName("res-name").WithReservationProject("res-shared-proj").WithReservationPath("projects/res-shared-proj/reservations/res-name")),
	)
	resSharedCC := computeclass.NewTestCrd(
		computeclass.WithName(ccName),
		computeclass.WithRules([]rules.Rule{resSharedCCRule}),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithLabel(ccLabel),
	)

	localSsds := reservations.BuildReservationLocalSSDs("NVME", 2)
	resLocalSsd := reservations.BuildReservation(
		"READY",
		true,
		"https://www.googleapis.com/compute/v1/projects/12345/zones/us-central1-a",
		"n2-standard-2",
		"",
		nil,
		localSsds,
	)
	resLocalSsd.Name = "res-localssd"
	resLocalSsd.SelfLink = "https://www.googleapis.com/compute/v1/projects/12345/zones/us-central1-a/reservations/res-localssd"

	defaultPod := testPodCPUMem("respod-1", 1, 128)
	defaultPod.ObjectMeta.UID = "uuid"
	resPod := addReservationLabels(defaultPod.DeepCopy(), "res-name", "12345")
	resAnyPod := addReservationAffinity(defaultPod.DeepCopy(), "any")
	resPodBadAffinity := addReservationAffinity(resPod.DeepCopy(), "foobar")
	resNonePod := addReservationAffinity(defaultPod.DeepCopy(), "none")
	resPodShared := addReservationProjectLabel(resPod.DeepCopy(), "res-shared-proj")
	resCCPod := addComputeClass(defaultPod.DeepCopy(), ccName)
	resPodLocalSsd := addReservationLabels(defaultPod.DeepCopy(), "res-localssd", "12345")
	resPodWithAcceleratorLabels := addAcceleratorLabels(addReservationLabels(defaultPod.DeepCopy(), "res-name", "12345"), "4", "tpu-v4-podslice")
	resPodSpecificAffinity := addReservationAffinity(addMachineFamily(resPod.DeepCopy(), "n2"), "specific")
	resPodSpecificAffinityOtherProject := addReservationLabels(resPodSpecificAffinity.DeepCopy(), "res-name", "other-project")
	resPodSpecificAffintyCompactPlacementGroup := addCompactPlacementGroupLabel(resPodSpecificAffinity.DeepCopy(), "placement-group-id")
	resPodPlacementPolicyFromLabels := addCompactPlacementPolicyLabel(
		addCompactPlacementGroupLabel(resPodSpecificAffinity.DeepCopy(), "placement-group-id"), "test-policy")

	defaultReq := nodeGroupRequirements{
		pods:                     []*apiv1.Pod{defaultPod},
		machineSpec:              machinetypes.NewMachineSpec([]machinetypes.MachineFamily{provider.GetAutoprovisioningDefaultFamily()}, machinetypes.AnyPlatform, "", ""),
		machineSelectionType:     machinetypes.SelectionTypeDefault,
		workloadSeparationTaints: []apiv1.Taint{},
		workloadSeparationLabels: map[string]string{},
		systemLabels:             map[string]string{},
	}

	resReqWithoutMatch := nodeGroupRequirements{
		pods:                     []*apiv1.Pod{resPod},
		machineSpec:              machinetypes.NewMachineSpec([]machinetypes.MachineFamily{provider.GetAutoprovisioningDefaultFamily()}, machinetypes.AnyPlatform, "", ""),
		machineSelectionType:     machinetypes.SelectionTypeDefault,
		workloadSeparationTaints: []apiv1.Taint{},
		workloadSeparationLabels: map[string]string{},
		reservation: reservationRequirements{
			exists:  true,
			name:    "res-name",
			project: "12345",
		},
		systemLabels: map[string]string{},
	}

	sharedResReqWithoutMatch := nodeGroupRequirements{
		pods:                     []*apiv1.Pod{resPodShared},
		machineSpec:              machinetypes.NewMachineSpec([]machinetypes.MachineFamily{provider.GetAutoprovisioningDefaultFamily()}, machinetypes.AnyPlatform, "", ""),
		machineSelectionType:     machinetypes.SelectionTypeDefault,
		workloadSeparationTaints: []apiv1.Taint{},
		workloadSeparationLabels: map[string]string{},
		reservation: reservationRequirements{
			exists:  true,
			name:    "res-name",
			project: "res-shared-proj",
		},
		systemLabels: map[string]string{},
	}

	resReqWithCompactPlacement := nodeGroupRequirements{
		pods:                     []*apiv1.Pod{resPodSpecificAffinity},
		machineSpec:              machinetypes.NewMachineSpecSingleFamily(machinetypes.N2, machinetypes.AnyPlatform, "", ""),
		machineSelectionType:     machinetypes.SelectionTypeSpecified,
		workloadSeparationTaints: []apiv1.Taint{},
		workloadSeparationLabels: map[string]string{},
		placementGroup: placement.Spec{
			Policy: "test-policy",
		},
		reservation: reservationRequirements{
			exists:      true,
			name:        "res-name",
			project:     "12345",
			machineType: "n2-standard-2",
			zone:        "us-central1-a",
		},
		systemLabels: map[string]string{},
	}

	resReq := resReqWithoutMatch
	resReq.reservation.machineType = "n2-standard-2"
	resReq.reservation.zone = "us-central1-a"
	resReq.machineSpec = machinetypes.MachineSpec{
		Families:             []machinetypes.MachineFamily{machinetypes.N2},
		MinCpuPlatform:       machinetypes.AnyPlatform,
		ExplicitMachineTypes: []string{"n2-standard-2"},
	}
	resReq.machineSelectionType = machinetypes.SelectionTypeImplied

	resReqWithoutAffinity := resReq
	resReqWithoutAffinity.reservation.affinity = ""

	// add test where machine spec familiy doesn't support compact placement
	resReqSpecificAffinity := resReqWithCompactPlacement
	resReqSpecificAffinity.reservation.affinity = "specific"
	resReqSpecificAffinity.machineSpec.ExplicitMachineTypes = []string{"n2-standard-2"}

	resReqSpecificAffinityPlacementGroup := resReqSpecificAffinity
	resReqSpecificAffinityPlacementGroup.placementGroup = placement.Spec{
		Policy:  "test-policy",
		GroupId: "placement-group-id",
	}

	resReqAny := resReqWithoutMatch
	resReqAny.pods = []*apiv1.Pod{resAnyPod}
	resReqAny.reservation = reservationRequirements{
		exists:   true,
		affinity: "any",
	}

	resReqNone := resReqWithoutMatch
	resReqNone.pods = []*apiv1.Pod{resNonePod}
	resReqNone.reservation = reservationRequirements{
		exists:   true,
		affinity: "none",
	}

	tpuReq := TpuRequest{TpuType: "tpu-v4-podslice", ChipsPerNode: 4, Topology: "2x2x1"}

	resReqTpu := resReqWithoutMatch
	resReqTpu.machineSpec = machinetypes.NewMachineSpec([]machinetypes.MachineFamily{machinetypes.CT4P}, machinetypes.AnyPlatform, "", gkelabels.TpuV4PodsliceValue)
	resReqTpu.machineSelectionType = machinetypes.SelectionTypeImplied
	resReqTpu.tpuRequest = tpuReq

	resReqShared := resReq
	resReqShared.reservation.project = "res-shared-proj"
	resReqShared.pods = []*apiv1.Pod{resPodShared}

	resReqLocalSsd := resReq
	resReqLocalSsd.reservation.name = "res-localssd"
	resReqLocalSsd.reservation.totalLSSDCount = 1
	resReqLocalSsd.ephemeralStorageLocalSSDCount = 1
	resReqLocalSsd.totalLSSDCount = 1
	resReqLocalSsd.pods = []*apiv1.Pod{resPodLocalSsd}

	for name, tc := range map[string]struct {
		disableSpecificTypeReservations bool
		enableReservationMatch          bool
		pods                            []*apiv1.Pod
		ccs                             []computeclass.CRD
		reservations                    []*gce_api.Reservation
		tpuRequest                      TpuRequest
		enabledFeatures                 []string
		wantRequirements                []nodeGroupRequirements
		wantErrs                        map[types.UID]errors.AutoscalerError
	}{
		"[Node Selectors] Non Reservation Pod": {
			pods:                   []*apiv1.Pod{defaultPod},
			enableReservationMatch: true,
			wantRequirements:       []nodeGroupRequirements{defaultReq},
		},
		"[Node Selectors] Reservation Pod With Matching Reservation": {
			pods:                   []*apiv1.Pod{resPod},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{res1},
			wantRequirements:       []nodeGroupRequirements{resReq},
		},
		"[Node Selectors] Reservation Pod With Matching Reservation and Local SSD Steering, flag1 enabled": {
			pods:                   []*apiv1.Pod{resPodLocalSsd},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{resLocalSsd},
			enabledFeatures:        []string{experiments.SliceOfHardwareReservationSteerLocalSSDFlag},
			wantRequirements:       []nodeGroupRequirements{resReqLocalSsd},
		},
		"[Node Selectors] Reservation Pod With Matching Reservation and Local SSD Steering, flag2 enabled": {
			pods:                   []*apiv1.Pod{resPodLocalSsd},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{resLocalSsd},
			enabledFeatures:        []string{experiments.SliceOfHardwareReservationSteerLocalSSD2Flag},
			wantRequirements:       []nodeGroupRequirements{resReqLocalSsd},
		},
		"[Node Selectors] Specific Reservation Pod with Non-Specific Reservation": {
			pods:                   []*apiv1.Pod{resPod},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{res1Any},
			wantErrs: map[types.UID]errors.AutoscalerError{"uuid": reservations.NewErrUnusableReservation(
				gceclient.ReservationRef{Name: "res-name", Project: "12345"},
				"SpecificReservationRequired is not enabled so reservation cannot be specifically targeted for consumption")},
		},
		"[Node Selectors] Any Reservation Pod No Matching Needed": {
			pods:                   []*apiv1.Pod{resAnyPod},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{res1Any},
			wantRequirements:       []nodeGroupRequirements{resReqAny},
		},
		"[Node Selectors] Local Reservation Pod When ReservationMatch Disabled, reservation exists": {
			pods:                   []*apiv1.Pod{resPod},
			enableReservationMatch: false,
			reservations:           []*gce_api.Reservation{},
			wantRequirements:       []nodeGroupRequirements{resReqWithoutMatch},
		},
		"[Node Selectors] Shared Reservation Pod When ReservationMatch Disabled": {
			pods:                   []*apiv1.Pod{resPodShared},
			enableReservationMatch: false,
			reservations:           []*gce_api.Reservation{},
			wantRequirements:       []nodeGroupRequirements{sharedResReqWithoutMatch},
		},
		"[Node Selectors] Reservation Pod without TPU Specifying an Aggregrate Reservation": {
			pods:                   []*apiv1.Pod{resPod},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{resTpu},
			wantErrs: map[types.UID]errors.AutoscalerError{"uuid": reservations.NewErrUnusableReservation(
				gceclient.ReservationRef{Name: "res-name", Project: "12345"},
				"Unable to consume aggregate reservation for non-TPU workloads")},
		},
		"[Node Selectors] Reservation Pod without TPU request but with accelerator labels": {
			pods:                   []*apiv1.Pod{resPodWithAcceleratorLabels},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{resTpu},
			wantRequirements: []nodeGroupRequirements{
				{
					pods:                     []*apiv1.Pod{resPodWithAcceleratorLabels},
					machineSpec:              machinetypes.NewMachineSpec([]machinetypes.MachineFamily{provider.GetAutoprovisioningDefaultFamily()}, machinetypes.AnyPlatform, "", ""),
					machineSelectionType:     machinetypes.SelectionTypeDefault,
					workloadSeparationTaints: []apiv1.Taint{},
					workloadSeparationLabels: map[string]string{},
					reservation: reservationRequirements{
						exists:  true,
						name:    "res-name",
						project: "12345",
						zone:    "us-central1-a",
					},
					systemLabels: map[string]string{},
				},
			},
		},
		"[Node Selectors] Reservation Pod without TPU When ReservationMatch Disabled": {
			pods:                            []*apiv1.Pod{resPod},
			enableReservationMatch:          false,
			disableSpecificTypeReservations: true,
			reservations:                    []*gce_api.Reservation{},
			wantErrs: map[types.UID]errors.AutoscalerError{"uuid": reservations.NewErrUnusableReservation(
				gceclient.ReservationRef{Name: "res-name", Project: "12345"},
				"Specifying reservations without TPUs are not supported")},
		},
		"[Node Selectors] Reservation Pod with TPU When ReservationMatch Disabled": {
			pods:                            []*apiv1.Pod{resPod},
			enableReservationMatch:          false,
			disableSpecificTypeReservations: true,
			reservations:                    []*gce_api.Reservation{},
			tpuRequest:                      TpuRequest{TpuType: "tpu-v4-podslice", ChipsPerNode: 4, Topology: "2x2x1"},
			wantRequirements:                []nodeGroupRequirements{resReqTpu},
		},
		"[Node Selectors/Vertex] Reservation Pod With No Matching Reservation When ReservationMatch & ReservationMatchRequired Disabled": {
			pods:                   []*apiv1.Pod{resPod},
			enableReservationMatch: false,
			reservations:           []*gce_api.Reservation{},
			wantRequirements:       []nodeGroupRequirements{resReqWithoutMatch},
		},
		"[Node Selectors/Vertex] Reservation Pod with TPU With No Matching Reservation When When ReservationMatch & ReservationMatchRequired Disabled": {
			pods:                   []*apiv1.Pod{resPod},
			enableReservationMatch: false,
			reservations:           []*gce_api.Reservation{},
			tpuRequest:             TpuRequest{TpuType: "tpu-v4-podslice", ChipsPerNode: 4, Topology: "2x2x1"},
			wantRequirements:       []nodeGroupRequirements{resReqTpu},
		},
		"[Node Selectors] Reservation With Bad Affinity": {
			pods:                   []*apiv1.Pod{resPodBadAffinity},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{res1},
			wantErrs:               map[types.UID]errors.AutoscalerError{"uuid": reservations.NewUnsupportedReservationAffinityError("foobar", "must be one of the supported values of [any any-reservation-then-fail none specific] or not set")},
		},
		"[Node Selectors] Shared Reservation Pod": {
			pods:                   []*apiv1.Pod{resPodShared},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{resShared},
			wantRequirements:       []nodeGroupRequirements{resReqShared},
		},
		"[Node Selectors] None affinity reservation": {
			pods:                   []*apiv1.Pod{resNonePod},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{},
			wantRequirements:       []nodeGroupRequirements{resReqNone},
		},
		"[CC] Reservation Pod With Matching Reservation": {
			pods:                   []*apiv1.Pod{resCCPod},
			ccs:                    []computeclass.CRD{resCC},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{res1},
			wantRequirements: []nodeGroupRequirements{
				newTestNodeGroupRequirements(
					copyReqsFrom(resReq),
					withPods([]*apiv1.Pod{resCCPod}),
					withComputeClass(resCC),
					withNodeConfigRule(resCCRule),
				),
			},
		},
		"[CC] Reservation Pod With Matching Reservation and CC Reservation Zones correctly chooses the reservation": {
			pods:                   []*apiv1.Pod{resCCPod},
			ccs:                    []computeclass.CRD{resCCWithZones},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{res1, res2},
			wantRequirements: []nodeGroupRequirements{
				newTestNodeGroupRequirements(
					copyReqsFrom(resReq),
					withPods([]*apiv1.Pod{resCCPod}),
					withComputeClass(resCCWithZones),
					withNodeConfigRule(resCCWithZonesRule),
					withSpecifiedZones([]string{"us-central1-a"}),
				),
			},
		},
		"[CC] Reservation Pod With Matching Reservation and CC Reservation Zones, reservations outside the list are not used": {
			pods:                   []*apiv1.Pod{resCCPod},
			ccs:                    []computeclass.CRD{resCCWithOtherZones},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{res1, res2},
			wantErrs: map[types.UID]errors.AutoscalerError{"uuid": reservations.NewErrUnusableReservation(
				gceclient.ReservationRef{Name: "res-name", Project: "12345"},
				"Specified reservation either does not exist or has no ready capacity to consume")},
		},
		"[CC] Specific Reservation Pod with Non-Specific Reservation": {
			pods:                   []*apiv1.Pod{resCCPod},
			ccs:                    []computeclass.CRD{resCC},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{res1Any},
			wantErrs: map[types.UID]errors.AutoscalerError{"uuid": reservations.NewErrUnusableReservation(
				gceclient.ReservationRef{Name: "res-name", Project: "12345"},
				"SpecificReservationRequired is not enabled so reservation cannot be specifically targeted for consumption")},
		},
		"[CC] Any Reservation Pod No Matching Needed": {
			pods:                   []*apiv1.Pod{resCCPod},
			ccs:                    []computeclass.CRD{resAnyCC},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{res1Any},
			wantRequirements: []nodeGroupRequirements{
				newTestNodeGroupRequirements(
					copyReqsFrom(resReqAny),
					withPods([]*apiv1.Pod{resCCPod}),
					withComputeClass(resAnyCC),
					withNodeConfigRule(resAnyCCRule),
				),
			},
		},
		"[CC] Reservation Pod without TPU Specifying an Aggregrate Reservation": {
			pods:                   []*apiv1.Pod{resCCPod},
			ccs:                    []computeclass.CRD{resCC},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{resTpu},
			wantErrs: map[types.UID]errors.AutoscalerError{"uuid": reservations.NewErrUnusableReservation(
				gceclient.ReservationRef{Name: "res-name", Project: "12345"},
				"Unable to consume aggregate reservation for non-TPU workloads")},
		},
		"[CC] Reservation Pod without TPU When ReservationMatch Disabled": {
			pods:                            []*apiv1.Pod{resCCPod},
			ccs:                             []computeclass.CRD{resCC},
			enableReservationMatch:          false,
			disableSpecificTypeReservations: true,
			reservations:                    []*gce_api.Reservation{},
			wantErrs: map[types.UID]errors.AutoscalerError{"uuid": reservations.NewErrUnusableReservation(
				gceclient.ReservationRef{Name: "res-name", Project: "12345"},
				"Specifying reservations without TPUs are not supported")},
		},
		"[CC] Reservation Pod with TPU When ReservationMatch Disabled": {
			pods:                            []*apiv1.Pod{resCCPod},
			ccs:                             []computeclass.CRD{resCC},
			enableReservationMatch:          false,
			disableSpecificTypeReservations: true,
			reservations:                    []*gce_api.Reservation{},
			tpuRequest:                      TpuRequest{TpuType: "tpu-v4-podslice", ChipsPerNode: 4, Topology: "2x2x1"},
			wantRequirements: []nodeGroupRequirements{
				newTestNodeGroupRequirements(
					copyReqsFrom(resReqTpu),
					withPods([]*apiv1.Pod{resCCPod}),
					withComputeClass(resCC),
					withNodeConfigRule(resCCRule),
				),
			},
		},
		"[CC/Vertex] Reservation Pod With No Matching Reservation When ReservationMatch & ReservationMatchRequired Disabled": {
			pods:                   []*apiv1.Pod{resCCPod},
			ccs:                    []computeclass.CRD{resCC},
			enableReservationMatch: false,
			reservations:           []*gce_api.Reservation{},
			wantRequirements: []nodeGroupRequirements{
				newTestNodeGroupRequirements(
					copyReqsFrom(resReqWithoutMatch),
					withPods([]*apiv1.Pod{resCCPod}),
					withComputeClass(resCC),
					withNodeConfigRule(resCCRule),
				),
			},
		},
		"[CC/Vertex] Reservation Pod with TPU With No Matching Reservation When When ReservationMatch & ReservationMatchRequired Disabled": {
			pods:                   []*apiv1.Pod{resCCPod},
			ccs:                    []computeclass.CRD{resCC},
			enableReservationMatch: false,
			reservations:           []*gce_api.Reservation{},
			tpuRequest:             TpuRequest{TpuType: "tpu-v4-podslice", ChipsPerNode: 4, Topology: "2x2x1"},
			wantRequirements: []nodeGroupRequirements{
				newTestNodeGroupRequirements(
					copyReqsFrom(resReqTpu),
					withPods([]*apiv1.Pod{resCCPod}),
					withComputeClass(resCC),
					withNodeConfigRule(resCCRule),
				),
			},
		},
		"[CC] Reservation With Bad Affinity": {
			pods:                   []*apiv1.Pod{resCCPod},
			ccs:                    []computeclass.CRD{resBadAffinityCC},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{res1},
			wantErrs:               map[types.UID]errors.AutoscalerError{"uuid": reservations.NewUnsupportedReservationAffinityError("foobar", "must be one of the supported values of [any any-reservation-then-fail none specific] or not set")},
		},
		"[CC] Shared Reservation Pod": {
			pods:                   []*apiv1.Pod{resCCPod},
			ccs:                    []computeclass.CRD{resSharedCC},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{resShared},
			wantRequirements: []nodeGroupRequirements{
				newTestNodeGroupRequirements(
					copyReqsFrom(resReqShared),
					withPods([]*apiv1.Pod{resCCPod}),
					withComputeClass(resSharedCC),
					withNodeConfigRule(resSharedCCRule),
				),
			},
		},
		"[CC] None affinity reservation": {
			pods:                   []*apiv1.Pod{resCCPod},
			ccs:                    []computeclass.CRD{resNoneCC},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{},
			wantRequirements: []nodeGroupRequirements{
				newTestNodeGroupRequirements(
					copyReqsFrom(resReqNone),
					withPods([]*apiv1.Pod{resCCPod}),
					withComputeClass(resNoneCC),
					withNodeConfigRule(resNoneCCRule),
				),
			},
		},
		"[Placement policy inferred from reservation] invalid placement policy": {
			pods:                   []*apiv1.Pod{resPodSpecificAffinity},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{resInvalidPlacementPolicy},
			wantErrs: map[types.UID]errors.AutoscalerError{
				"uuid": reservations.NewErrUnusableReservation(
					gceclient.ReservationRef{Name: "res-name", Project: "12345"},
					"Reservation res-name has invalid placement policy invalid-test-policy, it should follow ^projects/([a-z0-9\\-]+)/regions/([a-z0-9\\-]+)/resourcePolicies/([a-z0-9\\-]+)$ regexp"),
			},
		},
		"[Placement policy inferred from reservation] reservation with valid placement policy under policy label": {
			pods:                   []*apiv1.Pod{resPodSpecificAffinity},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{resPlacementPolicy1},
			wantRequirements: []nodeGroupRequirements{
				newTestNodeGroupRequirements(
					copyReqsFrom(resReqSpecificAffinity),
					withPods([]*apiv1.Pod{resPodSpecificAffinity}),
				),
			},
		},
		"[Placement policy inferred from reservation] reservation with valid placement policy under placement label": {
			pods:                   []*apiv1.Pod{resPodSpecificAffinity},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{resPlacementPolicy2},
			wantRequirements: []nodeGroupRequirements{
				newTestNodeGroupRequirements(
					copyReqsFrom(resReqSpecificAffinity),
					withPods([]*apiv1.Pod{resPodSpecificAffinity}),
				),
			},
		},
		"[Placement policy inferred from reservation] reservation with valid placement policy while conflicting compact placement police label is provided": {
			pods:                   []*apiv1.Pod{addCompactPlacementPolicyLabel(resPodSpecificAffinity.DeepCopy(), "other-placement-policy")},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{resPlacementPolicy1},
			wantErrs: map[types.UID]errors.AutoscalerError{"uuid": reservations.NewErrUnusableReservation(
				gceclient.ReservationRef{Name: "res-name", Project: "12345"},
				"Unable to consume specific reservation with placement policy when conflicting placement policy (other-placement-policy) is provided via node selectors."),
			},
		},
		"[Placement policy inferred from reservation] reservation with valid placement policy while compact placement group label is provided": {
			pods:                   []*apiv1.Pod{resPodSpecificAffintyCompactPlacementGroup},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{resPlacementPolicy1},
			wantRequirements: []nodeGroupRequirements{
				newTestNodeGroupRequirements(
					copyReqsFrom(resReqSpecificAffinityPlacementGroup),
					withPods([]*apiv1.Pod{resPodSpecificAffintyCompactPlacementGroup}),
				),
			},
		},
		"[Placement policy inferred from reservation] reservation with valid placement policy is in other project": {
			pods:                   []*apiv1.Pod{resPodSpecificAffinityOtherProject},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{resPlacementPolicyShared},
			wantErrs: map[types.UID]errors.AutoscalerError{"uuid": reservations.NewErrUnusableReservation(
				gceclient.ReservationRef{Name: "res-name", Project: "other-project"},
				"Shared reservations with placement policy are not supported")},
		},
		"[Placement policy inferred from reservation] placement policy missing from reservation, policy provided via node selector labels": {
			pods:                   []*apiv1.Pod{resPodPlacementPolicyFromLabels},
			enableReservationMatch: true,
			reservations:           []*gce_api.Reservation{res1},
			wantRequirements: []nodeGroupRequirements{
				newTestNodeGroupRequirements(
					copyReqsFrom(resReqSpecificAffinityPlacementGroup),
					withPods([]*apiv1.Pod{resPodPlacementPolicyFromLabels})),
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			reservationsPuller := reservations.NewTestingReservationsPuller("12345", []string{"other-project", "res-shared-proj", "res-shared-proj1"}, tc.reservations)

			em := experiments.NewMockManager(tc.enabledFeatures...)
			manager := NewAutoprovisioningNodeGroupManager(AutoprovisioningNodeGroupManagerOptions{
				CloudProvider:      provider,
				ReservationsPuller: reservationsPuller,
				ExperimentsManager: em,
				OptionsTracker:     tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
				Flags: AutoprovisioningNodeGroupManagerFlags{
					TpuAutoprovisioningEnabled: true,
					ReservationFlags: ReservationFlags{
						SpecificTypeReservationMatchEnabled: tc.enableReservationMatch,
						SpecificTypeReservationsEnabled:     !tc.disableSpecificTypeReservations,
					},
				},
				Lister:               computeclass_lister.NewMockCrdListerWithLabel(tc.ccs, ccLabel),
				ResourcePolicyPuller: &placement.FakeResourcePolicyPullerProvider{},
			})

			requirements, err := manager.extractRequirements(tc.pods, machinetypes.GpuRequest{}, tc.tpuRequest)
			if tc.wantErrs != nil {
				assert.Equal(t, tc.wantErrs, err)
			} else {
				assert.Empty(t, err)
				compareAllUnexportedOpt := cmp.Exporter(func(t reflect.Type) bool { return true })
				if diff := cmp.Diff(tc.wantRequirements, requirements, compareAllUnexportedOpt); diff != "" {
					t.Errorf("expected requirements differ (only pods and gpuRequests are compared), diff (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func buildTestMatcher(t *testing.T, patterns ...string) *gkelabels.Matcher {
	m, err := gkelabels.NewMatcher(patterns)
	assert.NoError(t, err)
	return m
}

func TestWorkloadSeparationGenerator_UpdateRequirements(t *testing.T) {
	userLabel := "k"
	systemLabel := "cloud.google.com/my-feature"
	systemLabel2 := "cloud.google.com/my-feature2"
	systemLabel3 := "cloud.google.com/my-feature3"
	systemLabelPattern := `cloud.google.com/my-feature\d`
	userToleration := apiv1.Toleration{Key: userLabel, Value: "v", Effect: apiv1.TaintEffectNoSchedule}
	systemToleration1 := apiv1.Toleration{Key: systemLabel, Value: "v1", Effect: apiv1.TaintEffectNoSchedule}
	systemToleration2 := apiv1.Toleration{Key: systemLabel2, Value: "v2", Effect: apiv1.TaintEffectNoSchedule}
	systemToleration3 := apiv1.Toleration{Key: systemLabel3, Value: "v3", Effect: apiv1.TaintEffectNoSchedule}

	tests := map[string]struct {
		podReq                         *podrequirements.Requirements
		ngReq                          *nodeGroupRequirements
		allowlistedSystemLabelsMatcher *gkelabels.Matcher
		wantLabels                     map[string]string
		wantTaints                     []apiv1.Taint
		wantErr                        bool
	}{
		"workload separation no system labels": {
			podReq: &podrequirements.Requirements{
				Tolerations: []apiv1.Toleration{userToleration},
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					userLabel: podrequirements.NewValues("v"),
				}),
			},
			ngReq: &nodeGroupRequirements{},
			wantLabels: map[string]string{
				userLabel: "v",
			},
			wantTaints: []apiv1.Taint{{Key: userLabel, Value: "v", Effect: apiv1.TaintEffectNoSchedule}},
		},
		"pod requirement label without toleration": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					userLabel: podrequirements.NewValues("v"),
				}),
			},
			ngReq:   &nodeGroupRequirements{},
			wantErr: true,
		},
		"pod requirement label without toleration but label is allowlisted": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					userLabel: podrequirements.NewValues("v"),
				}),
			},
			ngReq: &nodeGroupRequirements{pods: []*apiv1.Pod{
				test.BuildTestPod("pod-1", 1, 1, withAnnotations(map[string]string{gkelabels.PTSDomainKeyAnnotation: userLabel})),
			}},
			wantLabels: map[string]string{},
			wantTaints: []apiv1.Taint{},
		},
		"workload separation non allow listed system label": {
			podReq: &podrequirements.Requirements{
				Tolerations: []apiv1.Toleration{systemToleration1},
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					systemLabel: podrequirements.NewValues("v1"),
				}),
			},
			ngReq:      &nodeGroupRequirements{},
			wantLabels: map[string]string{},
			wantTaints: []apiv1.Taint{},
		},
		"workload separation allow listed system label": {
			podReq: &podrequirements.Requirements{
				Tolerations: []apiv1.Toleration{systemToleration2},
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					systemLabel2: podrequirements.NewValues("v2"),
				}),
			},
			ngReq:                          &nodeGroupRequirements{},
			allowlistedSystemLabelsMatcher: buildTestMatcher(t, systemLabel2),
			wantLabels: map[string]string{
				systemLabel2: "v2",
			},
			wantTaints: []apiv1.Taint{{Key: systemLabel2, Value: "v2", Effect: apiv1.TaintEffectNoSchedule}},
		},
		"workload separation allow listed and non allow listed system label": {
			podReq: &podrequirements.Requirements{
				Tolerations: []apiv1.Toleration{systemToleration2, systemToleration1},
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					systemLabel2: podrequirements.NewValues("v2"),
					systemLabel:  podrequirements.NewValues("v1"),
				}),
			},
			ngReq:                          &nodeGroupRequirements{},
			allowlistedSystemLabelsMatcher: buildTestMatcher(t, systemLabel2),
			wantLabels: map[string]string{
				systemLabel2: "v2",
			},
			wantTaints: []apiv1.Taint{{Key: systemLabel2, Value: "v2", Effect: apiv1.TaintEffectNoSchedule}},
		},
		"workload separation allow listed pattern and non allow listed system label": {
			podReq: &podrequirements.Requirements{
				Tolerations: []apiv1.Toleration{systemToleration2, systemToleration1},
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					systemLabel2: podrequirements.NewValues("v2"),
					systemLabel:  podrequirements.NewValues("v1"),
				}),
			},
			ngReq:                          &nodeGroupRequirements{},
			allowlistedSystemLabelsMatcher: buildTestMatcher(t, systemLabelPattern),
			wantLabels: map[string]string{
				systemLabel2: "v2",
			},
			wantTaints: []apiv1.Taint{{Key: systemLabel2, Value: "v2", Effect: apiv1.TaintEffectNoSchedule}},
		},
		"workload separation allow listed pattern 2 system labels that are fit same pattern": {
			podReq: &podrequirements.Requirements{
				Tolerations: []apiv1.Toleration{systemToleration2, systemToleration1, systemToleration3},
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					systemLabel:  podrequirements.NewValues("v1"),
					systemLabel2: podrequirements.NewValues("v2"),
					systemLabel3: podrequirements.NewValues("v3"),
				}),
			},
			ngReq:                          &nodeGroupRequirements{},
			allowlistedSystemLabelsMatcher: buildTestMatcher(t, systemLabelPattern),
			wantLabels: map[string]string{
				systemLabel2: "v2",
				systemLabel3: "v3",
			},
			wantTaints: []apiv1.Taint{{Key: systemLabel2, Value: "v2", Effect: apiv1.TaintEffectNoSchedule}, {Key: systemLabel3, Value: "v3", Effect: apiv1.TaintEffectNoSchedule}},
		},
	}
	for desc, tc := range tests {
		t.Run(desc, func(t *testing.T) {
			generator := NewWorkloadSeparationGenerator(tc.allowlistedSystemLabelsMatcher)
			err := generator.UpdateRequirements(tc.ngReq, tc.podReq, machinetypes.GpuRequest{}, TpuRequest{})
			if !tc.wantErr {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantTaints, tc.ngReq.workloadSeparationTaints)
				assert.Equal(t, tc.wantLabels, tc.ngReq.workloadSeparationLabels)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestCSNGenerator(t *testing.T) {
	for desc, tc := range map[string]struct {
		podReq     *podrequirements.Requirements
		wantNgReq  *nodeGroupRequirements
		wantParams *nodeGroupParameters
		csnEnabled bool
	}{
		"pod has a CSN label requirement": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					csn.SoftWorkloadSeparationKey: podrequirements.NewValues(csn.SoftWorkloadSeparationValue),
				}),
			},
			wantNgReq: &nodeGroupRequirements{
				systemLabels: map[string]string{
					csn.SoftWorkloadSeparationKey: csn.SoftWorkloadSeparationValue,
				},
			},
			wantParams: &nodeGroupParameters{
				systemLabels: map[string]string{
					csn.SoftWorkloadSeparationKey: csn.SoftWorkloadSeparationValue,
				},
				taints: []apiv1.Taint{csn.SoftWorkloadSeparationTaint},
			},
			csnEnabled: true,
		},
		"pod doesn't have a CSN label requirement": {
			podReq:     &podrequirements.Requirements{},
			wantNgReq:  &nodeGroupRequirements{},
			wantParams: &nodeGroupParameters{},
			csnEnabled: true,
		},
	} {
		t.Run(desc, func(t *testing.T) {
			generator := NewCSNGenerator(tc.csnEnabled)
			ngReq := &nodeGroupRequirements{}
			aerr := generator.UpdateRequirements(ngReq, tc.podReq, machinetypes.GpuRequest{}, TpuRequest{})
			assert.NoError(t, aerr)
			assert.Equal(t, tc.wantNgReq, ngReq)

			params := &nodeGroupParameters{}
			err := generator.UpdateParameters(params, *ngReq, NodeGroupOptions{})
			assert.NoError(t, err)
			assert.Equal(t, tc.wantParams, params)
		})
	}
}

func TestProvisioningRequestGenerator_UpdateRequirements(t *testing.T) {
	for desc, tc := range map[string]struct {
		podReq    *podrequirements.Requirements
		wantNgReq *nodeGroupRequirements
	}{
		"pod has a queued provisioning requirement - update node group requirement": {
			podReq: &podrequirements.Requirements{
				QueuedProvisioningReq: podrequirements.QueuedProvisioningRequirements{Enabled: true, ResizeRequestName: "rr0"},
			},
			wantNgReq: &nodeGroupRequirements{
				queuedProvisioningReq: podrequirements.QueuedProvisioningRequirements{Enabled: true, ResizeRequestName: "rr0"},
			},
		},
	} {
		t.Run(desc, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).Build()
			generator := NewProvisioningRequestGenerator(experiments.NewMockManager(), provider)
			ngReq := &nodeGroupRequirements{}
			err := generator.UpdateRequirements(ngReq, tc.podReq, machinetypes.GpuRequest{}, TpuRequest{})
			assert.NoError(t, err)
			assert.Equal(t, tc.wantNgReq, ngReq)
		})
	}
}

func TestMaxRunDurationGenerator_UpdateRequirements(t *testing.T) {
	for desc, tc := range map[string]struct {
		podReq    *podrequirements.Requirements
		wantNgReq *nodeGroupRequirements
	}{
		"pod has a max run duration requirement - update node group requirement": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.MaxRunDurationLabelKey: podrequirements.NewValues("3600"),
				}),
			},
			wantNgReq: &nodeGroupRequirements{
				maxRunDurationInSeconds: "3600",
			},
		},
		"pod doesn't have a max run duration requirement - update node group requirement with an empty string": {
			podReq: &podrequirements.Requirements{},
			wantNgReq: &nodeGroupRequirements{
				maxRunDurationInSeconds: "",
			},
		},
	} {
		t.Run(desc, func(t *testing.T) {
			generator := NewMaxRunDurationGenerator(nil, nil)
			ngReq := &nodeGroupRequirements{}
			err := generator.UpdateRequirements(ngReq, tc.podReq, machinetypes.GpuRequest{}, TpuRequest{})
			assert.NoError(t, err)
			assert.Equal(t, tc.wantNgReq, ngReq)
		})
	}
}

func TestFlexStartGenerator_UpdateRequirements(t *testing.T) {
	computeClassName := "compute-class-name"
	ccType := "CC"
	leadTimeSeconds := 3600
	nodeRecyclingConfig := &v1.NodeRecyclingConfig{
		LeadTimeSeconds: &leadTimeSeconds,
	}
	tpuReq := TpuRequest{
		TpuType:      gkelabels.TpuV4PodsliceValue,
		Topology:     "2x2x2",
		ChipsPerNode: 4,
	}

	for desc, tc := range map[string]struct {
		flexStartExpDisabled    bool
		flexStartNAPExpDisabled bool
		podReq                  *podrequirements.Requirements
		computeClassRule        rules.Rule
		wantNgReq               *nodeGroupRequirements
		expectedErr             error
		tpuReq                  TpuRequest
	}{
		// Basic NAP cases
		"podHasFlexStartLabel_napExpDisabled_noReq": {
			flexStartNAPExpDisabled: true,
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.FlexStartLabel: podrequirements.NewValues(gkelabels.FlexStartValue),
				}),
			},
			wantNgReq: &nodeGroupRequirements{},
		},
		"podHasFlexStartLabel_expDisabled_noReq": {
			flexStartExpDisabled: true,
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.FlexStartLabel: podrequirements.NewValues(gkelabels.FlexStartValue),
				}),
			},
			wantNgReq: &nodeGroupRequirements{},
		},
		"podHasFlexStartLabel_ok": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.FlexStartLabel: podrequirements.NewValues(gkelabels.FlexStartValue),
				}),
			},
			wantNgReq: &nodeGroupRequirements{flexStartReq: flexStartRequirements{enabled: true}},
		},
		"podHasFlexStartLabel_invalidValue_err": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.FlexStartLabel: podrequirements.NewValues("invalid-value"),
				}),
			},
			expectedErr: NewFlexStartMisconfiguredError("Flex Start invalid \"cloud.google.com/gke-flex-start\" node selector value \"invalid-value\""),
		},
		"podHasFlexStartLabel_andInvalidProvisioningLabel_err": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.FlexStartLabel:    podrequirements.NewValues(gkelabels.FlexStartValue),
					gkelabels.ProvisioningLabel: podrequirements.NewValues(gkelabels.SpotProvisioningValue),
				}),
			},
			expectedErr: NewFlexStartMisconfiguredError("Flex Start incompatible \"cloud.google.com/gke-provisioning\" node selector value \"spot\""),
		},
		"podHasProvisioningLabel_FlexStartValue_ok": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.ProvisioningLabel: podrequirements.NewValues(gkelabels.FlexStartProvisioningValue),
				}),
			},
			wantNgReq: &nodeGroupRequirements{flexStartReq: flexStartRequirements{enabled: true}},
		},
		"podHasBothFlexStartLabels_ok": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.FlexStartLabel:    podrequirements.NewValues(gkelabels.FlexStartValue),
					gkelabels.ProvisioningLabel: podrequirements.NewValues(gkelabels.FlexStartProvisioningValue),
				}),
			},
			wantNgReq: &nodeGroupRequirements{flexStartReq: flexStartRequirements{enabled: true}},
		},
		"podHasOnlyFlexStartToleration_NoLabel_err": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{}),
				Tolerations: []apiv1.Toleration{
					{
						Key:    gkelabels.FlexStartLabel,
						Value:  gkelabels.FlexStartValue,
						Effect: apiv1.TaintEffectNoSchedule,
					},
				},
			},
			expectedErr: NewFlexStartMisconfiguredError("pod cannot have only Flex Start toleration \"cloud.google.com/gke-flex-start\" to trigger node auto-provisioning, please also specify the node selector"),
		},
		"podHasFlexStartSelectorAndToleration_okTainted": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.FlexStartLabel: podrequirements.NewValues(gkelabels.FlexStartValue),
				}),
				Tolerations: []apiv1.Toleration{
					{
						Key:    gkelabels.FlexStartLabel,
						Value:  gkelabels.FlexStartValue,
						Effect: apiv1.TaintEffectNoSchedule,
					},
				},
			},
			wantNgReq: &nodeGroupRequirements{flexStartReq: flexStartRequirements{enabled: true, tainted: true}},
		},
		"podHasFlexStartLabel_AndIncompatibleSpotLabel_err": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.FlexStartLabel: podrequirements.NewValues(gkelabels.FlexStartValue),
					gkelabels.SpotLabel:      podrequirements.NewValues(gkelabels.PreemptionValue),
				}),
			},
			expectedErr: NewFlexStartMisconfiguredError("Flex Start pod has incompatible node selectors: [cloud.google.com/gke-spot]"),
		},
		// CC cases
		"podHasFlexStartLabel_computeClassRuleNoFlexStart_ok": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.FlexStartLabel: podrequirements.NewValues(gkelabels.FlexStartValue),
				}),
			},
			computeClassRule: rules.NewRule(
				rules.WithFlexStartRule(false, nil)),
			wantNgReq: &nodeGroupRequirements{flexStartReq: flexStartRequirements{enabled: true}},
		},
		"podHasFlexStartLabelWithRecycling_computeClassRuleNoFlexStart_ok": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.FlexStartLabel:                     podrequirements.NewValues(gkelabels.FlexStartValue),
					gkelabels.NodeRecycleLeadTimeSecondsLabelKey: podrequirements.NewValues(fmt.Sprintf("%d", *nodeRecyclingConfig.LeadTimeSeconds)),
				}),
			},
			computeClassRule: rules.NewRule(
				rules.WithFlexStartRule(false, nil)),
			wantNgReq: &nodeGroupRequirements{flexStartReq: flexStartRequirements{enabled: true}},
		},
		"podHasFlexStartLabel_computeClassRuleFlexStart_ok": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.FlexStartLabel: podrequirements.NewValues(gkelabels.FlexStartValue),
				}),
			},
			computeClassRule: rules.NewRule(
				rules.WithFlexStartRule(true, nil)),
			wantNgReq: &nodeGroupRequirements{flexStartReq: flexStartRequirements{enabled: true}},
		},
		"podNoLabel_computeClassRuleFlexStart_ok": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{}),
			},
			computeClassRule: rules.NewRule(
				rules.WithFlexStartRule(true, nil)),
			wantNgReq: &nodeGroupRequirements{flexStartReq: flexStartRequirements{enabled: true}},
		},
		"podHasFlexStartLabel_computeClassRuleFlexStartWithRecycling_ok": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.FlexStartLabel: podrequirements.NewValues(gkelabels.FlexStartValue),
				}),
			},
			computeClassRule: rules.NewRule(
				rules.WithFlexStartRule(true, nodeRecyclingConfig)),
			wantNgReq: &nodeGroupRequirements{flexStartReq: flexStartRequirements{enabled: true}},
		},
		"podNoLabel_computeClassRuleFlexStartWithRecycling_ok": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{}),
			},
			computeClassRule: rules.NewRule(
				rules.WithFlexStartRule(true, nodeRecyclingConfig)),
			wantNgReq: &nodeGroupRequirements{flexStartReq: flexStartRequirements{enabled: true}},
		},
		"podHasFlexStartLabelWithRecycling_computeClassRuleFlexStartWithRecycling_matchingRecycling_ok": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.FlexStartLabel:                     podrequirements.NewValues(gkelabels.FlexStartValue),
					gkelabels.NodeRecycleLeadTimeSecondsLabelKey: podrequirements.NewValues(fmt.Sprintf("%d", *nodeRecyclingConfig.LeadTimeSeconds)),
				}),
			},
			computeClassRule: rules.NewRule(
				rules.WithFlexStartRule(true, nodeRecyclingConfig)),
			wantNgReq: &nodeGroupRequirements{flexStartReq: flexStartRequirements{enabled: true}},
		},
		"podHasFlexStartLabelWithRecycling_computeClassRuleFlexStartNoRecycling_err": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.FlexStartLabel:                     podrequirements.NewValues(gkelabels.FlexStartValue),
					gkelabels.NodeRecycleLeadTimeSecondsLabelKey: podrequirements.NewValues("12345"),
				}),
			},
			computeClassRule: rules.NewRule(
				rules.WithFlexStartRule(true, nil)),
			expectedErr: NewComputeClassPodIncompatibleError(computeClassName, ccType),
		},
		"podHasFlexStartLabelWithRecycling_computeClassRuleFlexStartRecycling_differentLeadTimeSeconds_err": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.FlexStartLabel:                     podrequirements.NewValues(gkelabels.FlexStartValue),
					gkelabels.NodeRecycleLeadTimeSecondsLabelKey: podrequirements.NewValues("12345"),
				}),
			},
			computeClassRule: rules.NewRule(
				rules.WithFlexStartRule(true, nodeRecyclingConfig)),
			expectedErr: NewComputeClassPodIncompatibleError(computeClassName, ccType),
		},
		"podHasRecycling_computeClassRuleFlexStartRecycling_differentLeadTimeSeconds_err": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.NodeRecycleLeadTimeSecondsLabelKey: podrequirements.NewValues("12345"),
				}),
			},
			computeClassRule: rules.NewRule(
				rules.WithFlexStartRule(true, nodeRecyclingConfig)),
			expectedErr: NewComputeClassPodIncompatibleError(computeClassName, ccType),
		},
		"podHasFlexStartLabel_andTpuRequest_ok": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.FlexStartLabel:   podrequirements.NewValues(gkelabels.FlexStartValue),
					gkelabels.TPULabel:         podrequirements.NewValues(gkelabels.TpuV4PodsliceValue),
					gkelabels.TPUTopologyLabel: podrequirements.NewValues("2x2x2"),
				}),
			},
			tpuReq:    tpuReq,
			wantNgReq: &nodeGroupRequirements{flexStartReq: flexStartRequirements{enabled: true}}},
	} {
		t.Run(desc, func(t *testing.T) {
			var exps []string
			if !tc.flexStartNAPExpDisabled {
				exps = append(exps, experiments.FlexStartNonQueuedNAPEnabledFlag)
			}
			if !tc.flexStartExpDisabled {
				exps = append(exps, experiments.FlexStartNonQueuedEnabledFlag)
			}
			generator := NewFlexStartGenerator(experiments.NewMockManager(exps...))

			ngReq := &nodeGroupRequirements{
				computeClassRule: tc.computeClassRule,
				computeClass:     computeclass.NewTestCrd(computeclass.WithName(computeClassName), computeclass.WithCrdType(ccType)),
			}
			if tc.wantNgReq != nil {
				tc.wantNgReq.computeClassRule = ngReq.computeClassRule
				tc.wantNgReq.computeClass = ngReq.computeClass
			}
			err := generator.UpdateRequirements(ngReq, tc.podReq, machinetypes.GpuRequest{}, tc.tpuReq)
			if tc.expectedErr != nil {
				assert.Equal(t, err, tc.expectedErr)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.wantNgReq, ngReq)
		})
	}
}

func TestSystemLabelsGenerator_UpdateRequirements(t *testing.T) {
	userLabel := "k"
	systemLabel := "cloud.google.com/my-feature"
	systemLabel2 := "cloud.google.com/my-feature2"
	systemLabel3 := "cloud.google.com/my-feature3"
	systemLabelPattern := `cloud.google.com/my-feature\d`

	for desc, tc := range map[string]struct {
		cc                             computeclass.CRD
		rule                           rules.Rule
		podReq                         *podrequirements.Requirements
		allowlistedSystemLabelsMatcher *gkelabels.Matcher
		wantLabels                     map[string]string
	}{
		"no system labels": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					userLabel: podrequirements.NewValues("v"),
				}),
			},
			wantLabels: map[string]string{},
		},
		"non allow listed system label": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					systemLabel: podrequirements.NewValues("v"),
				}),
			},
			wantLabels: map[string]string{},
		},
		"allow listed system label": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					systemLabel2: podrequirements.NewValues("v"),
				}),
			},
			allowlistedSystemLabelsMatcher: buildTestMatcher(t, systemLabel2),
			wantLabels: map[string]string{
				systemLabel2: "v",
			},
		},
		"allow listed and non allow listed system label": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					systemLabel2: podrequirements.NewValues("v"),
					systemLabel:  podrequirements.NewValues("v"),
				}),
			},
			allowlistedSystemLabelsMatcher: buildTestMatcher(t, systemLabel2),
			wantLabels: map[string]string{
				systemLabel2: "v",
			},
		},
		"allow listed pattern and non allow listed system label": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					systemLabel2: podrequirements.NewValues("v"),
					systemLabel:  podrequirements.NewValues("v"),
				}),
			},
			allowlistedSystemLabelsMatcher: buildTestMatcher(t, systemLabelPattern),
			wantLabels: map[string]string{
				systemLabel2: "v",
			},
		},
		"allow listed pattern with 2 labels and non allow listed system label": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					systemLabel2: podrequirements.NewValues("v2"),
					systemLabel3: podrequirements.NewValues("v3"),
					systemLabel:  podrequirements.NewValues("v1"),
				}),
			},
			allowlistedSystemLabelsMatcher: buildTestMatcher(t, systemLabelPattern),
			wantLabels: map[string]string{
				systemLabel2: "v2",
				systemLabel3: "v3",
			},
		},
		"CRD user-defined labels are passed": {
			cc: computeclass.NewTestCrd(
				computeclass.WithUserDefinedLabels(map[string]string{
					"label-1": "value-1",
					"label-2": "value-2",
				}),
			),
			wantLabels: map[string]string{
				"label-1": "value-1",
				"label-2": "value-2",
			},
		},
		"CRD rule user-defined labels are passed": {
			rule: rules.NewRule(
				rules.WithLabelsRule(map[string]string{
					"label-1": "value-1",
					"label-2": "value-2",
				}),
			),
			wantLabels: map[string]string{
				"label-1": "value-1",
				"label-2": "value-2",
			},
		},
		"CRD & CRD rule user-defined labels are passed": {
			cc: computeclass.NewTestCrd(
				computeclass.WithUserDefinedLabels(map[string]string{
					"label-1": "value-1",
					"label-2": "value-2",
				}),
			),
			rule: rules.NewRule(
				rules.WithLabelsRule(map[string]string{
					"label-3": "value-3",
					"label-4": "value-4",
				}),
			),
			wantLabels: map[string]string{
				"label-1": "value-1",
				"label-2": "value-2",
				"label-3": "value-3",
				"label-4": "value-4",
			},
		},
		"multiple allow listed patterns": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					systemLabel:  podrequirements.NewValues("v1"),
					systemLabel2: podrequirements.NewValues("v2"),
					userLabel:    podrequirements.NewValues("v3"),
				}),
			},
			allowlistedSystemLabelsMatcher: buildTestMatcher(t, systemLabel, systemLabel2),
			wantLabels: map[string]string{
				systemLabel:  "v1",
				systemLabel2: "v2",
			},
		},
	} {
		t.Run(desc, func(t *testing.T) {
			generator := NewSystemLabelsGenerator(tc.allowlistedSystemLabelsMatcher)
			ngReq := &nodeGroupRequirements{
				computeClass:     tc.cc,
				computeClassRule: tc.rule,
			}
			podReq := tc.podReq
			if podReq == nil {
				podReq = &podrequirements.Requirements{}
			}
			err := generator.UpdateRequirements(ngReq, podReq, machinetypes.GpuRequest{}, TpuRequest{})
			assert.NoError(t, err)
			assert.Equal(t, tc.wantLabels, ngReq.systemLabels)
		})
	}
}

func TestSelfServiceGenerator(t *testing.T) {
	testCases := []struct {
		name             string
		podReqMetadata   map[string]string
		computeClass     computeclass.CRD
		computeClassRule rules.Rule
		wantMetadata     map[string]string
	}{
		{
			name:           "Empty pod requirements metadata",
			podReqMetadata: make(map[string]string),
		},
		{
			name: "Preset pod requirements metadata",
			podReqMetadata: map[string]string{
				"key-1": "value-1",
				"key-2": "value-2",
			},
			wantMetadata: map[string]string{
				"key-1": "value-1",
				"key-2": "value-2",
			},
		},
		{
			name:         "CRD without self-service metadata",
			computeClass: computeclass.NewTestCrd(),
		},
		{
			name: "CRD with self-service metadata",
			computeClass: computeclass.NewTestCrd(
				computeclass.WithSelfServiceMetadata(map[string]string{
					"key-1": "value-1",
					"key-2": "value-2",
				})),
			wantMetadata: map[string]string{
				"key-1": "value-1",
				"key-2": "value-2",
			},
		},
		{
			name:             "Rule without self-service metadata",
			computeClassRule: rules.NewRule(),
		},
		{
			name: "Rule with self-service metadata",
			computeClassRule: rules.NewRule(
				rules.WithSelfServiceRule(map[string]string{
					"key-1": "value-1",
					"key-2": "value-2",
				})),
			wantMetadata: map[string]string{
				"key-1": "value-1",
				"key-2": "value-2",
			},
		},
		{
			name: "All self-service metadata",
			podReqMetadata: map[string]string{
				"key-1": "value-1",
				"key-2": "value-2",
			},
			computeClass: computeclass.NewTestCrd(
				computeclass.WithSelfServiceMetadata(map[string]string{
					"key-3": "value-3",
					"key-4": "value-4",
				})),
			computeClassRule: rules.NewRule(
				rules.WithSelfServiceRule(map[string]string{
					"key-5": "value-5",
					"key-6": "value-6",
				})),
			wantMetadata: map[string]string{
				"key-1": "value-1",
				"key-2": "value-2",
				"key-3": "value-3",
				"key-4": "value-4",
				"key-5": "value-5",
				"key-6": "value-6",
			},
		},
		{
			name: "CRD metadata overrides pod requirements metadata",
			podReqMetadata: map[string]string{
				"key-1": "pod-value-1",
				"key-2": "pod-value-2",
			},
			computeClass: computeclass.NewTestCrd(
				computeclass.WithSelfServiceMetadata(map[string]string{
					"key-1": "cc-value-1",
					"key-2": "cc-value-2",
				})),
			wantMetadata: map[string]string{
				"key-1": "cc-value-1",
				"key-2": "cc-value-2",
			},
		},
		{
			name: "Rule metadata overrides CRD metadata",
			computeClass: computeclass.NewTestCrd(
				computeclass.WithSelfServiceMetadata(map[string]string{
					"key-1": "cc-value-1",
					"key-2": "cc-value-2",
				})),
			computeClassRule: rules.NewRule(
				rules.WithSelfServiceRule(map[string]string{
					"key-1": "rule-value-1",
					"key-2": "rule-value-2",
				})),
			wantMetadata: map[string]string{
				"key-1": "rule-value-1",
				"key-2": "rule-value-2",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			generator := NewSelfServiceGenerator()
			params := &nodeGroupParameters{
				systemLabels: make(map[string]string),
			}
			ngReq := nodeGroupRequirements{
				computeClass:        tc.computeClass,
				computeClassRule:    tc.computeClassRule,
				selfServiceMetadata: tc.podReqMetadata,
			}
			spec := &gkeclient.NodePoolSpec{}

			assert.NoError(t, generator.UpdateParameters(params, ngReq, NodeGroupOptions{}))
			assert.NoError(t, generator.UpdateNodePoolSpec(spec, params.systemLabels, nil))
			assert.Equal(t, tc.wantMetadata, spec.SelfServiceMetadata)
		})
	}
}

func TestLinuxNodeConfigGenerator_UpdateParameters(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		linuxNodeConfig *gkeclient.LinuxNodeConfig
		expectedLabels  map[string]string
	}{
		"nil-config": {
			linuxNodeConfig: nil,
			expectedLabels:  map[string]string{},
		},
		"empty-config": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{},
			expectedLabels:  map[string]string{labelLinuxNodeConfig: "{}"},
		},
		"sysctls-config": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				Sysctls: map[string]string{
					"net.core.somaxconn": "65535",
				},
			},
			expectedLabels: map[string]string{labelLinuxNodeConfig: `{"sysctls":{"net.core.somaxconn":"65535"}}`},
		},
		"hugepages": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				Hugepages: &gkeclient.HugepagesConfig{
					HugepageSize1g: 3,
					HugepageSize2m: 1024,
				},
			},
			expectedLabels: map[string]string{labelLinuxNodeConfig: `{"hugepages":{"hugepageSize1g":3,"hugepageSize2m":1024}}`},
		},
		"sysctl and hugepages": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				Sysctls: map[string]string{
					"net.core.somaxconn": "65535",
				},
				Hugepages: &gkeclient.HugepagesConfig{
					HugepageSize1g: 3,
					HugepageSize2m: 1024,
				},
			},
			expectedLabels: map[string]string{labelLinuxNodeConfig: `{"hugepages":{"hugepageSize1g":3,"hugepageSize2m":1024},"sysctls":{"net.core.somaxconn":"65535"}}`},
		},
		"swap enabled on boot disk encrypted": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				SwapConfig: &gkeclient.SwapConfig{
					Enabled: true,
					BootDiskProfile: &gkeclient.BootDiskProfile{
						SwapSizeGib: int64(10),
					},
				},
			},
			expectedLabels: map[string]string{labelLinuxNodeConfig: `{"swapConfig":{"bootDiskProfile":{"swapSizeGib":"10"},"enabled":true}}`},
		},
		"swap enabled on boot disk unencrypted": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				SwapConfig: &gkeclient.SwapConfig{
					Enabled:          true,
					EncryptionConfig: &gkeclient.EncryptionConfig{Disabled: true},
					BootDiskProfile: &gkeclient.BootDiskProfile{
						SwapSizeGib: int64(10),
					},
				},
			},
			expectedLabels: map[string]string{labelLinuxNodeConfig: `{"swapConfig":{"bootDiskProfile":{"swapSizeGib":"10"},"enabled":true,"encryptionConfig":{"disabled":true}}}`},
		},
		"swap enabled on ephemeral local ssd profile": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				SwapConfig: &gkeclient.SwapConfig{
					Enabled: true,
					EphemeralLocalSsdProfile: &gkeclient.EphemeralLocalSsdProfile{
						SwapSizeGib: int64(10),
					},
				},
			},
			expectedLabels: map[string]string{labelLinuxNodeConfig: `{"swapConfig":{"enabled":true,"ephemeralLocalSsdProfile":{"swapSizeGib":"10"}}}`},
		},
		"swap enabled on dedicated local ssd profile": {
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				SwapConfig: &gkeclient.SwapConfig{
					Enabled:                  true,
					DedicatedLocalSsdProfile: &gkeclient.DedicatedLocalSsdProfile{DiskCount: 2},
				},
			},
			expectedLabels: map[string]string{labelLinuxNodeConfig: `{"swapConfig":{"dedicatedLocalSsdProfile":{"diskCount":"2"},"enabled":true}}`},
		},
	} {
		t.Run(name, func(t *testing.T) {
			generator := NewLinuxNodeConfigGenerator()
			params := &nodeGroupParameters{
				systemLabels: make(map[string]string),
			}
			ngReq := nodeGroupRequirements{
				linuxNodeConfig: tc.linuxNodeConfig,
			}
			err := generator.UpdateParameters(params, ngReq, NodeGroupOptions{})
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedLabels, params.systemLabels)
		})
	}
}

func TestReservationGenerator_UpdateRequirements(t *testing.T) {
	projectId := "gke-staging-reserved-tpu-1"
	sharedProject := "gke-staging-reserved-tpu-2"
	rsv1 := reservations.BuildAggregateReservationWithSpecificRequired(projectId, "cloudtpu-a", "us-central2-a", "")
	rsv2 := reservations.BuildAggregateReservationWithSpecificRequired(projectId, "cloudtpu-b", "us-central2-b", "")
	rsv2.Id = 1
	rsv3 := reservations.BuildAggregateReservationWithSpecificRequired(sharedProject, "cloudtpu-c", "us-central2-b", "")

	rsv1Key := gceclient.GetReservationRefFromReservation(*rsv1)
	rsv2Key := gceclient.GetReservationRefFromReservation(*rsv2)

	rbs := map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
		rsv1Key: {reservations.BuildSingleReservationBlock("block-a", 1, 0, "us-central2-a")},
		rsv2Key: {reservations.BuildSingleReservationBlock("block-b", 1, 0, "us-central2-b")},
	}

	for desc, tc := range map[string]struct {
		ngReq                  *nodeGroupRequirements
		podReq                 *podrequirements.Requirements
		reservations           []*gce_api.Reservation
		reservationBlocks      []*gceclient.GceReservationBlock
		enableReservationMatch bool
		expectedErr            error
	}{
		"rsv generator requesting tpu with tpu requirements": {
			ngReq: &nodeGroupRequirements{
				tpuRequest: TpuRequest{
					TpuType:      "tpu-v6e-slice",
					ChipsPerNode: 4,
				},
			},
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.ReservationNameLabel:    podrequirements.NewValues("cloudtpu-a"),
					gkelabels.ReservationProjectLabel: podrequirements.NewValues(projectId),
				}),
			},
			reservations: []*gce_api.Reservation{
				rsv1, rsv2,
			},
			enableReservationMatch: true,
		},
		"rsv generator requesting tpu without tpu requirements, want error": {
			ngReq: &nodeGroupRequirements{
				tpuRequest: TpuRequest{},
			},
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.ReservationNameLabel:    podrequirements.NewValues("cloudtpu-a"),
					gkelabels.ReservationProjectLabel: podrequirements.NewValues(projectId),
				}),
			},
			reservations: []*gce_api.Reservation{
				rsv1, rsv2,
			},
			enableReservationMatch: true,
			expectedErr: reservations.NewErrUnusableReservation(
				gceclient.ReservationRef{Name: "cloudtpu-a", Project: projectId},
				"Unable to consume aggregate reservation for non-TPU workloads"),
		},
		"rsv generator, requested reservation is not available, assume ones matches": {
			ngReq: &nodeGroupRequirements{
				tpuRequest: TpuRequest{},
			},
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.ReservationNameLabel:    podrequirements.NewValues("cloudtpu-c"),
					gkelabels.ReservationProjectLabel: podrequirements.NewValues(projectId),
				}),
			},
			reservations: []*gce_api.Reservation{
				rsv1, rsv2,
			},
		},
		"rsv generator requesting tpu with tpu requirements, shared reservation": {
			ngReq: &nodeGroupRequirements{
				tpuRequest: TpuRequest{
					TpuType:  "tpu-v6e-slice",
					Topology: "2x2",
				},
			},
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.ReservationNameLabel:    podrequirements.NewValues("cloudtpu-c"),
					gkelabels.ReservationProjectLabel: podrequirements.NewValues(sharedProject),
				}),
			},
			reservations: []*gce_api.Reservation{
				rsv1, rsv2, rsv3,
			},
		},
		"rsv generator requesting tpu with reservation blocks": {
			ngReq: &nodeGroupRequirements{
				tpuRequest: TpuRequest{
					TpuType:      "tpu7x",
					Topology:     "2x2x4",
					ChipsPerNode: 4,
				},
			},
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.ReservationNameLabel:   podrequirements.NewValues("cloudtpu-a"),
					gkelabels.ReservationBlocksLabel: podrequirements.NewValues("block-a"),
				}),
			},
			reservations: []*gce_api.Reservation{
				rsv1, rsv2,
			},
			reservationBlocks: []*gceclient.GceReservationBlock{
				reservations.BuildSingleReservationBlock("block-a", 1, 0, "zone-A"),
			},
			enableReservationMatch: true,
		},
	} {
		t.Run(desc, func(t *testing.T) {
			reservationsPuller := reservations.NewTestingReservationsPuller(projectId, []string{sharedProject}, tc.reservations)
			blocksPuller := reservations.NewBlocksPuller(reservations.NewFakeBlocksPullerProvider(rbs, nil), reservationsPuller)
			blocksPuller.Loop()
			generator := NewReservationGenerator(
				reservationsPuller,
				ReservationFlags{
					SpecificTypeReservationMatchEnabled: tc.enableReservationMatch,
					SpecificTypeReservationsEnabled:     true,
				},
				projectId,
				experiments.NewMockManager(),
				blocksPuller)
			err := generator.UpdateRequirements(tc.ngReq, tc.podReq, machinetypes.GpuRequest{}, TpuRequest{})
			if tc.expectedErr != nil {
				assert.Error(t, err)
				assert.Equal(t, err, tc.expectedErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLinuxNodeConfigGenerator_UpdateNodePoolSpec(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		systemLabels         map[string]string
		expectedNodePoolSpec *gkeclient.NodePoolSpec
	}{
		"nil-config": {
			systemLabels: map[string]string{},
			expectedNodePoolSpec: &gkeclient.NodePoolSpec{
				LinuxNodeConfig: nil,
			},
		},
		"empty-config": {
			systemLabels: map[string]string{
				labelLinuxNodeConfig: "{}",
			},
			expectedNodePoolSpec: &gkeclient.NodePoolSpec{
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{},
			},
		},
		"sysctls-config": {
			systemLabels: map[string]string{
				labelLinuxNodeConfig: `{"sysctls":{"net.core.somaxconn":"65535"}}`,
			},
			expectedNodePoolSpec: &gkeclient.NodePoolSpec{
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					Sysctls: map[string]string{
						"net.core.somaxconn": "65535",
					},
				},
			},
		},
		"hugepages": {
			systemLabels: map[string]string{labelLinuxNodeConfig: `{"hugepages":{"hugepageSize1g":3,"hugepageSize2m":1024}}`},
			expectedNodePoolSpec: &gkeclient.NodePoolSpec{
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					Hugepages: &gkeclient.HugepagesConfig{
						HugepageSize1g: 3,
						HugepageSize2m: 1024,
					},
				},
			},
		},
		"sysctl and hugepages": {
			systemLabels: map[string]string{labelLinuxNodeConfig: `{"hugepages":{"hugepageSize1g":3,"hugepageSize2m":1024},"sysctls":{"net.core.somaxconn":"65535"}}`},
			expectedNodePoolSpec: &gkeclient.NodePoolSpec{
				LinuxNodeConfig: &gkeclient.LinuxNodeConfig{
					Sysctls: map[string]string{
						"net.core.somaxconn": "65535",
					},
					Hugepages: &gkeclient.HugepagesConfig{
						HugepageSize1g: 3,
						HugepageSize2m: 1024,
					},
				},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			generator := NewLinuxNodeConfigGenerator()
			spec := &gkeclient.NodePoolSpec{}
			err := generator.UpdateNodePoolSpec(spec, tc.systemLabels, nil)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedNodePoolSpec, spec)
		})
	}
}

func TestKubeletConfigGenerator_UpdateParameters(t *testing.T) {
	for tn, tc := range map[string]struct {
		kubeletConfig  *gke_api_beta.NodeKubeletConfig
		expectedLabels map[string]string
	}{
		"nil config": {
			kubeletConfig:  nil,
			expectedLabels: map[string]string{},
		},
		"empty config": {
			kubeletConfig:  &gke_api_beta.NodeKubeletConfig{},
			expectedLabels: map[string]string{labelKubeletConfig: "{}"},
		},
		"config with fields": {
			kubeletConfig: &gke_api_beta.NodeKubeletConfig{
				CpuCfsQuota:       true,
				CpuCfsQuotaPeriod: "100ms",
				CpuManagerPolicy:  "static",
				PodPidsLimit:      10000,
			},
			expectedLabels: map[string]string{
				labelKubeletConfig: `{"cpuCfsQuota":true,"cpuCfsQuotaPeriod":"100ms","cpuManagerPolicy":"static","podPidsLimit":"10000"}`,
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			generator := NewKubeletConfigGenerator()
			params := &nodeGroupParameters{
				systemLabels: make(map[string]string),
			}
			ngReq := nodeGroupRequirements{
				kubeletConfig: tc.kubeletConfig,
			}
			err := generator.UpdateParameters(params, ngReq, NodeGroupOptions{})
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedLabels, params.systemLabels)
		})
	}
}

func TestKubeletConfigGenerator_UpdateNodePoolSpec(t *testing.T) {
	for tn, tc := range map[string]struct {
		systemLabels         map[string]string
		expectedNodePoolSpec *gkeclient.NodePoolSpec
	}{
		"nil config": {
			systemLabels: map[string]string{},
			expectedNodePoolSpec: &gkeclient.NodePoolSpec{
				KubeletConfig: nil,
			},
		},
		"empty config": {
			systemLabels: map[string]string{
				labelKubeletConfig: "{}",
			},
			expectedNodePoolSpec: &gkeclient.NodePoolSpec{
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{},
			},
		},
		"config with fields": {
			systemLabels: map[string]string{
				labelKubeletConfig: `{"cpuCfsQuotaPeriod":"100ms","podPidsLimit":"10000"}`,
			},
			expectedNodePoolSpec: &gkeclient.NodePoolSpec{
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{
					CpuCfsQuotaPeriod: "100ms",
					PodPidsLimit:      10000,
				},
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			generator := NewKubeletConfigGenerator()
			spec := &gkeclient.NodePoolSpec{}
			err := generator.UpdateNodePoolSpec(spec, tc.systemLabels, nil)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedNodePoolSpec, spec)
		})
	}
}

func TestSpecifiedZonesGenerator_UpdateNodePoolSpecWithRequirements(t *testing.T) {
	tests := map[string]struct {
		requirementsZones []string
		spec              *gkeclient.NodePoolSpec
		selectedZone      string
		expectedLocations []string
		trimmedLocations  []string
		zoneTypes         bool
		systemLabels      map[string]string
		wantErr           bool
	}{
		"empty requirements": {},
		"no specified zones": {
			requirementsZones: []string{},
		},
		"one specified zone": {
			requirementsZones: []string{
				"zone-1",
			},
			expectedLocations: []string{
				"zone-1",
			},
		},
		"multiple specified zones": {
			requirementsZones: []string{
				"zone-1",
				"zone-2",
			},
			expectedLocations: []string{
				"zone-1",
				"zone-2",
			},
		},
		"duplicated specified zones": {
			requirementsZones: []string{
				"zone-1",
				"zone-1",
			},
			expectedLocations: []string{
				"zone-1",
			},
		},
		"zone_types_without_tpu_not_reduced": {
			requirementsZones: []string{
				"zone-1",
				"zone-2",
			},
			selectedZone:     "zone-1",
			zoneTypes:        true,
			trimmedLocations: []string{"zone-1", "zone-2"},
			expectedLocations: []string{
				"zone-1",
				"zone-2",
			},
		},
		"zone_types_reduced_multi_host_tpu": {
			requirementsZones: []string{
				"zone-1",
				"zone-2",
			},
			selectedZone:     "zone-2",
			zoneTypes:        true,
			trimmedLocations: []string{"zone-1", "zone-2"},
			expectedLocations: []string{
				"zone-2",
			},
			spec: &gkeclient.NodePoolSpec{
				TpuType:      "tpu",
				TpuMultiHost: true,
				Labels:       map[string]string{},
			},
		},
		"zone_types_reduced_compact_placement": {
			requirementsZones: []string{
				"zone-1",
				"zone-2",
			},
			selectedZone:     "zone-2",
			zoneTypes:        true,
			trimmedLocations: []string{"zone-1", "zone-2"},
			expectedLocations: []string{
				"zone-2",
			},
			spec: &gkeclient.NodePoolSpec{
				PlacementGroup: placement.Spec{Policy: "placement-policy"},
			},
		},
		"zone_types_no_locations_available": {
			requirementsZones: []string{
				"zone-1",
			},
			selectedZone:     "zone-1",
			zoneTypes:        true,
			trimmedLocations: []string{},
			wantErr:          true,
		},
		"zone_types_picked_zone_not_available": {
			requirementsZones: []string{
				"zone-1",
				"zone-2",
			},
			selectedZone:     "zone-2",
			zoneTypes:        true,
			trimmedLocations: []string{"zone-1"},
			wantErr:          true,
		},
	}
	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithAutopilotEnabled(true).
				WithTrimmedLocations(tc.trimmedLocations).
				Build()

			if tc.spec == nil {
				tc.spec = &gkeclient.NodePoolSpec{Labels: map[string]string{}}
			}
			if tc.systemLabels == nil {
				tc.systemLabels = map[string]string{}
			}
			params := &nodeGroupParameters{
				systemLabels: tc.systemLabels,
			}
			ngReq := nodeGroupRequirements{
				specifiedZones: tc.requirementsZones,
				systemLabels:   tc.systemLabels,
			}
			var optionsTracker *optstracking.OptionsTracker
			if tc.zoneTypes {
				optionsTracker = testOptionsTracker(func(io *internalopts.InternalOptions) { io.ZoneTypesEnabled = true })
				ngReq.usesZoneTypes = true
			} else {
				optionsTracker = testOptionsTracker(nil)
			}
			generator := NewSpecifiedZonesGenerator(provider, true, optionsTracker)
			opts := NodeGroupOptions{
				Zone: tc.selectedZone,
			}

			err := generator.UpdateParameters(params, ngReq, opts)
			assert.NoError(t, err)

			err2 := generator.UpdateNodePoolSpec(tc.spec, params.systemLabels, nil)
			if tc.wantErr {
				assert.Error(t, err2)
				return
			}
			assert.NoError(t, err2)

			difference := cmp.Diff(tc.spec.Locations, tc.expectedLocations)
			if difference != "" {
				t.Errorf("Specified zones differ: %s", difference)
			}
		})
	}
}

func TestSpecifiedZonesGenerator_UpdateRequirements(t *testing.T) {
	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithAutopilotEnabled(true).
		WithAutoprovisioningLocations("us-central1-a", "us-central1-b").
		WithAllZones("us-central1-a", "us-central1-b", "us-central1-ai1a", "us-central1-ai1b").
		WithStandardZones([]string{"us-central1-a", "us-central1-b"}).
		WithAiZones([]string{"us-central1-ai1a", "us-central1-ai1b"}).
		Build()
	emptyPodReq := &podrequirements.Requirements{}

	tests := map[string]struct {
		podReq             *podrequirements.Requirements
		computeClassRule   rules.Rule
		optsModifier       func(*internalopts.InternalOptions)
		wantNgReqZones     []string
		wantNgReqZoneTypes bool
	}{
		"no_compute_class_rule_no_pod_labels": {
			podReq:           emptyPodReq,
			computeClassRule: nil,
			wantNgReqZones:   nil,
		},
		"compute_class_rule_with_zones": {
			podReq: emptyPodReq,
			computeClassRule: rules.NewRule(
				rules.WithLocationRule([]string{"us-central1-a", "us-central1-b"}),
			),
			wantNgReqZones: []string{"us-central1-a", "us-central1-b"},
		},
		"compute_class_rule_with_empty_zones": {
			podReq:           emptyPodReq,
			computeClassRule: rules.NewRule(rules.WithLocationRule([]string{})),
			wantNgReqZones:   nil,
		},
		"pod_labels_zones_inside_AP_locations": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					apiv1.LabelTopologyZone: podrequirements.NewValues("us-central1-a", "us-central1-b"),
				}),
			},
			computeClassRule: nil,
			wantNgReqZones:   nil, // zones part of AP locations are ignored by UpdateRequirements.
		},
		"pod_labels_zone_outside_of_AP_location": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					apiv1.LabelTopologyZone: podrequirements.NewValues("us-central1-ai1a"),
				}),
			},
			computeClassRule: nil,
			wantNgReqZones:   []string{"us-central1-ai1a"},
		},
		"pod_labels_multiple_zones_outside_of_AP_location": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					apiv1.LabelTopologyZone: podrequirements.NewValues("us-central1-ai1a", "us-central1-ai1b"),
				}),
			},
			computeClassRule: nil,
			wantNgReqZones:   []string{"us-central1-ai1a", "us-central1-ai1b"},
		},
		"pod_labels_zones_from_AP_locations_and_outside_it": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					apiv1.LabelTopologyZone: podrequirements.NewValues("us-central1-ai1a", "us-central1-a", "us-central1-ai1b"),
				}),
			},
			computeClassRule: nil,
			wantNgReqZones:   []string{"us-central1-ai1a", "us-central1-a", "us-central1-ai1b"}, // All are included if at least one is outside of AP locations.
		},
		"compute_class_rule_takes_precedence_over_pod_labels_with_ai_zone": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					apiv1.LabelTopologyZone: podrequirements.NewValues("us-central1-ai1a"),
				}),
			},
			computeClassRule: rules.NewRule(
				rules.WithLocationRule([]string{"us-central1-a"}),
			),
			wantNgReqZones: []string{"us-central1-a"},
		},
		"pod_labels_zones_outside_of_cluster_region": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					apiv1.LabelTopologyZone: podrequirements.NewValues("us-central1-ai2a"),
				}),
			},
			computeClassRule: nil,
			wantNgReqZones:   nil, // zone outside of cluster region, no requirement (bad user intent).
		},
		"pod_labels_some_zones_outside_of_cluster_region": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					apiv1.LabelTopologyZone: podrequirements.NewValues("us-central1-ai2a", "us-central1-ai1a"),
				}),
			},
			computeClassRule: nil,
			wantNgReqZones:   []string{"us-central1-ai1a"},
		},
		"zone_types_disabled": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					apiv1.LabelTopologyZone: podrequirements.NewValues("us-central1-ai2a", "us-central1-ai1a"),
				}),
			},
			computeClassRule: rules.NewRule(
				rules.WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD"}, provider),
			),
			wantNgReqZones:     []string{"us-central1-ai1a"},
			wantNgReqZoneTypes: false,
		},
		"zone_types_standard_zones": {
			computeClassRule: rules.NewRule(
				rules.WithLocationZoneTypesRule([]v1.ZoneType{"STANDARD"}, provider),
			),
			optsModifier:       func(io *internalopts.InternalOptions) { io.ZoneTypesEnabled = true },
			wantNgReqZones:     []string{"us-central1-a", "us-central1-b"},
			wantNgReqZoneTypes: true,
		},
		"zone_types_ai_zones": {
			computeClassRule: rules.NewRule(
				rules.WithLocationZoneTypesRule([]v1.ZoneType{"AI"}, provider),
			),
			optsModifier:       func(io *internalopts.InternalOptions) { io.ZoneTypesEnabled = true },
			wantNgReqZones:     []string{"us-central1-ai1a", "us-central1-ai1b"},
			wantNgReqZoneTypes: true,
		},
		"zone_types_cluster_default_zones": {
			computeClassRule: rules.NewRule(
				rules.WithLocationZoneTypesRule([]v1.ZoneType{"CLUSTER_DEFAULT"}, provider),
			),
			optsModifier:       func(io *internalopts.InternalOptions) { io.ZoneTypesEnabled = true },
			wantNgReqZones:     []string{"us-central1-a", "us-central1-b"},
			wantNgReqZoneTypes: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ngReq := &nodeGroupRequirements{
				computeClassRule: tc.computeClassRule,
			}

			optionsTracker := testOptionsTracker(tc.optsModifier)
			generator := NewSpecifiedZonesGenerator(provider, true, optionsTracker)
			err := generator.UpdateRequirements(ngReq, tc.podReq, machinetypes.GpuRequest{}, TpuRequest{})
			assert.NoError(t, err)
			assert.ElementsMatch(t, tc.wantNgReqZones, ngReq.specifiedZones)
			assert.Equal(t, tc.wantNgReqZoneTypes, ngReq.usesZoneTypes)
		})
	}
}

func TestMaxPodsPerNodeGenerator_GenerateNodeGroupOptionsForRequirements(t *testing.T) {
	staticMppn := 108
	mppnRule := rules.NewRule(
		rules.WithMaxPodsPerNodeRule(&staticMppn))
	for name, tc := range map[string]struct {
		unschedulablePodsNum      int
		runningPodsNum            int
		unschedulablePodsMilliCPU int64
		unschedulablePodsMemory   int64
		isClusterAutopilot        bool
		ngReq                     nodeGroupRequirements
		options                   []NodeGroupOptions
		expectedOptions           []NodeGroupOptions
	}{
		"cluster autopilot enabled": {
			isClusterAutopilot: true,
			options: []NodeGroupOptions{{
				MachineType: "e2-standard-32",
			}},
			expectedOptions: []NodeGroupOptions{
				{
					MachineType:           "e2-standard-32",
					MaxPodsPerNode:        110,
					DynamicMaxPodsPerNode: true,
				},
			},
		},
		"cluster autopilot enabled, static max pods per node": {
			isClusterAutopilot: true,
			options: []NodeGroupOptions{{
				MachineType: "e2-standard-32",
			}},
			ngReq: nodeGroupRequirements{
				maxPodsPerNode: 64,
			},
			expectedOptions: []NodeGroupOptions{
				{
					MachineType:    "e2-standard-32",
					MaxPodsPerNode: 64,
				},
			},
		},
		"cluster autopilot enabled, 1000 pods in cluster": {
			isClusterAutopilot:   true,
			unschedulablePodsNum: 100,
			runningPodsNum:       900,
			options: []NodeGroupOptions{{
				MachineType: "e2-standard-32",
			}},
			expectedOptions: []NodeGroupOptions{
				{
					MachineType:           "e2-standard-32",
					MaxPodsPerNode:        110,
					DynamicMaxPodsPerNode: true,
				},
			},
		},
		"cluster autopilot enabled, cc defined, 1000 pods in cluster": {
			isClusterAutopilot:   true,
			unschedulablePodsNum: 100,
			runningPodsNum:       900,
			ngReq: nodeGroupRequirements{
				computeClass: computeclass.NewTestCrd(
					computeclass.WithName("test-cc"),
				),
			},
			options: []NodeGroupOptions{{
				MachineType: "e2-standard-32",
			}},
			expectedOptions: []NodeGroupOptions{
				{
					MachineType:           "e2-standard-32",
					MaxPodsPerNode:        110,
					DynamicMaxPodsPerNode: true,
				},
			},
		},
		"cluster autopilot enabled, cc defined, static mppn": {
			isClusterAutopilot:   true,
			unschedulablePodsNum: 100,
			runningPodsNum:       900,
			ngReq: nodeGroupRequirements{
				maxPodsPerNode: staticMppn,
				computeClass: computeclass.NewTestCrd(
					computeclass.WithName("test-cc"),
					computeclass.WithRules([]rules.Rule{mppnRule}),
				),
			},
			options: []NodeGroupOptions{{
				MachineType: "e2-standard-32",
			}},
			expectedOptions: []NodeGroupOptions{
				{
					MachineType:           "e2-standard-32",
					MaxPodsPerNode:        staticMppn,
					DynamicMaxPodsPerNode: false,
				},
			},
		},
		"cluster autopilot enabled, 3000 pods in cluster": {
			isClusterAutopilot:        true,
			unschedulablePodsNum:      2000,
			runningPodsNum:            1000,
			unschedulablePodsMilliCPU: 100,
			options: []NodeGroupOptions{{
				MachineType: "e2-standard-32",
			}},
			expectedOptions: []NodeGroupOptions{
				{
					MachineType:           "e2-standard-32",
					MaxPodsPerNode:        110,
					DynamicMaxPodsPerNode: true,
				},
				{
					MachineType:           "e2-standard-32",
					MaxPodsPerNode:        256,
					DynamicMaxPodsPerNode: true,
				},
			},
		},
		"cluster autopilot enabled, 3000 pods in cluster with large CPU requests": {
			isClusterAutopilot:        true,
			unschedulablePodsNum:      2000,
			runningPodsNum:            1000,
			unschedulablePodsMilliCPU: 1000,
			options: []NodeGroupOptions{{
				MachineType: "e2-standard-32",
			}},
			expectedOptions: []NodeGroupOptions{
				{
					MachineType:           "e2-standard-32",
					MaxPodsPerNode:        110,
					DynamicMaxPodsPerNode: true,
				},
			},
		},
		"cluster autopilot enabled, 3000 pods in cluster with large memory requests": {
			isClusterAutopilot:        true,
			unschedulablePodsNum:      2000,
			runningPodsNum:            1000,
			unschedulablePodsMilliCPU: 100,
			unschedulablePodsMemory:   100 * units.GiB,
			options: []NodeGroupOptions{{
				MachineType: "e2-standard-32",
			}},
			expectedOptions: []NodeGroupOptions{
				{
					MachineType:           "e2-standard-32",
					MaxPodsPerNode:        110,
					DynamicMaxPodsPerNode: true,
				},
			},
		},
		// Standard cluster with managed node group and dynamic max pods per node enabled cases
		"standard cluster, managed node group, dynamic max pods per node enabled": {
			isClusterAutopilot: false,
			ngReq: nodeGroupRequirements{
				computeClass: computeclass.NewTestCrd(computeclass.WithAutopilotManaged(), computeclass.WithDynamicMaxPodsPerNodeEnabled()),
			},
			options: []NodeGroupOptions{{
				MachineType: "e2-standard-32",
			}},
			expectedOptions: []NodeGroupOptions{
				{
					MachineType:           "e2-standard-32",
					MaxPodsPerNode:        110,
					DynamicMaxPodsPerNode: true,
				},
			},
		},
		"standard cluster, managed node group, dynamic max pods per node enabled, 1000 pods in cluster": {
			isClusterAutopilot:   false,
			unschedulablePodsNum: 100,
			runningPodsNum:       900,
			ngReq: nodeGroupRequirements{
				computeClass: computeclass.NewTestCrd(computeclass.WithAutopilotManaged(), computeclass.WithDynamicMaxPodsPerNodeEnabled()),
			},
			options: []NodeGroupOptions{{
				MachineType: "e2-standard-32",
			}},
			expectedOptions: []NodeGroupOptions{
				{
					MaxPodsPerNode:        110,
					MachineType:           "e2-standard-32",
					DynamicMaxPodsPerNode: true,
				},
			},
		},
		"standard cluster, managed node group, dynamic max pods per node enabled, 3000 pods in cluster": {
			isClusterAutopilot:        false,
			unschedulablePodsNum:      2000,
			runningPodsNum:            1000,
			unschedulablePodsMilliCPU: 100,
			unschedulablePodsMemory:   1 * units.GiB,
			ngReq: nodeGroupRequirements{
				computeClass: computeclass.NewTestCrd(computeclass.WithAutopilotManaged(), computeclass.WithDynamicMaxPodsPerNodeEnabled()),
			},
			options: []NodeGroupOptions{{
				MachineType: "e2-standard-32",
			}},
			expectedOptions: []NodeGroupOptions{
				{
					MaxPodsPerNode:        110,
					MachineType:           "e2-standard-32",
					DynamicMaxPodsPerNode: true,
				},
				{
					MaxPodsPerNode:        256,
					DynamicMaxPodsPerNode: true,
					MachineType:           "e2-standard-32",
				},
			},
		},
		// Standard cluster with either usual node group or dynamic max pods per node disabled cases
		"standard cluster, unmanaged node group": {
			isClusterAutopilot:   false,
			unschedulablePodsNum: 2000,
			runningPodsNum:       1000,
			options: []NodeGroupOptions{{
				MachineType: "e2-standard-32",
			}},
			expectedOptions: []NodeGroupOptions{
				{
					MachineType:    "e2-standard-32",
					MaxPodsPerNode: 0,
				},
			},
		},
		"standard cluster, managed node group, but dynamic max pods per node disabled": {
			isClusterAutopilot:   false,
			unschedulablePodsNum: 300,
			runningPodsNum:       4000,
			ngReq: nodeGroupRequirements{
				computeClass: computeclass.NewTestCrd(computeclass.WithAutopilotManaged()),
			},
			options: []NodeGroupOptions{{
				MachineType: "e2-standard-32",
			}},
			expectedOptions: []NodeGroupOptions{
				{
					MachineType:    "e2-standard-32",
					MaxPodsPerNode: 0,
				},
			},
		},
		"standard cluster, managed node group, static max pods per node specified": {
			isClusterAutopilot:   false,
			unschedulablePodsNum: 300,
			ngReq: nodeGroupRequirements{
				computeClass:   computeclass.NewTestCrd(computeclass.WithAutopilotManaged(), computeclass.WithDynamicMaxPodsPerNodeEnabled()),
				maxPodsPerNode: 50,
			},
			options: []NodeGroupOptions{{
				MachineType: "e2-standard-32",
			}},
			expectedOptions: []NodeGroupOptions{
				{
					MachineType:    "e2-standard-32",
					MaxPodsPerNode: 50,
				},
			},
		},
		"standard cluster, static max pods per node specified": {
			isClusterAutopilot:   false,
			unschedulablePodsNum: 300,
			ngReq: nodeGroupRequirements{
				maxPodsPerNode: 50,
			},
			options: []NodeGroupOptions{{
				MachineType: "e2-standard-32",
			}},
			expectedOptions: []NodeGroupOptions{
				{
					MachineType:    "e2-standard-32",
					MaxPodsPerNode: 50,
				},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			var unschedulablePods []*apiv1.Pod
			var scheduledPods []*apiv1.Pod
			for i := 0; i < tc.unschedulablePodsNum; i++ {
				unschedulablePods = append(unschedulablePods, test.BuildTestPod(fmt.Sprintf("pod-%d", i), tc.unschedulablePodsMilliCPU, tc.unschedulablePodsMemory, test.MarkUnschedulable()))
			}
			for i := 0; i < tc.runningPodsNum; i++ {
				scheduledPods = append(scheduledPods, test.BuildScheduledTestPod(fmt.Sprintf("pod-%d", i), 0, 0, "test-node"))
			}
			lister := kube_util.NewTestPodLister(append(unschedulablePods, scheduledPods...))
			machinetypes.RegisterMachineFamily(machinetypes.E2)
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineTypes("e2-standard-32").
				WithAutopilotEnabled(tc.isClusterAutopilot).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			generator := NewMaxPodsPerNodeGenerator(provider, lister)
			tc.ngReq.pods = unschedulablePods
			gotOptions := generator.GenerateNodeGroupOptionsForRequirements(tc.options, tc.ngReq)
			assert.Equal(t, tc.expectedOptions, gotOptions)
		})
	}
}

func TestMaxPodsPerNodeGenerator_UpdateParameters(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		mppn           int
		expectedLabels map[string]string
	}{
		"MPPN is set to a positive number": {
			mppn:           10,
			expectedLabels: map[string]string{gkelabels.MaxPodsPerNodeLabel: "10"},
		},
		"MPPN is set to 0": {
			mppn:           0,
			expectedLabels: map[string]string{},
		},
		"MPPN is set to a negative number": {
			mppn:           -5,
			expectedLabels: map[string]string{},
		},
	} {
		t.Run(name, func(t *testing.T) {
			lister := kube_util.NewTestPodLister([]*apiv1.Pod{})
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithMachineTypes("g2-standard-2").WithAutopilotEnabled(true).Build()
			generator := NewMaxPodsPerNodeGenerator(provider, lister)
			params := &nodeGroupParameters{
				systemLabels: make(map[string]string),
			}
			ngOpt := NodeGroupOptions{
				MaxPodsPerNode: tc.mppn,
			}
			err := generator.UpdateParameters(params, nodeGroupRequirements{}, ngOpt)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedLabels, params.systemLabels)
		})
	}
}

func TestMaxPodsPerNodeGenerator_UpdateNodePoolSpec(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		systemLabels         map[string]string
		isClusterAutopilot   bool
		expectedNodePoolSpec *gkeclient.NodePoolSpec
		expectedErr          bool
	}{
		// MPPN set in system labels cases
		"MPPN is set to a negative number": {
			systemLabels: map[string]string{
				gkelabels.MaxPodsPerNodeLabel: "-5",
			},
			expectedNodePoolSpec: &gkeclient.NodePoolSpec{},
			expectedErr:          true,
		},
		"MPPN is set to a positive number": {
			systemLabels: map[string]string{
				gkelabels.MaxPodsPerNodeLabel: "10",
			},
			expectedNodePoolSpec: &gkeclient.NodePoolSpec{
				MaxPodsPerNode: 10,
				Labels:         map[string]string{},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			lister := kube_util.NewTestPodLister([]*apiv1.Pod{})
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithMachineTypes("g2-standard-2").WithAutopilotEnabled(tc.isClusterAutopilot).Build()
			generator := NewMaxPodsPerNodeGenerator(provider, lister)
			spec := &gkeclient.NodePoolSpec{}
			err := generator.UpdateNodePoolSpec(spec, tc.systemLabels, nil)
			if tc.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.expectedNodePoolSpec, spec)
		})
	}
}

func TestComputePossibleRequirementsWithSecondaryBootDisksCC(t *testing.T) {
	ccName := "storage_cc"
	ccLabel := "autoscaling.gke.io/cc-label"

	ccPod := test.BuildTestPod("cc-pod", 1, 128)
	ccPod = addSeparation(ccPod, ccLabel, ccName, true)

	disk1 := "disk1"
	disk2 := "disk2"
	disk3 := "disk3"
	project1 := "project1"
	project2 := "project2"
	project3 := "project3"
	mode := "CONTAINER_IMAGE_CACHE"

	bootDiskType := machinetypes.DiskTypeStandard
	bootDiskSize := 100
	localSSDCount := 4
	bootDiskEncryptionKey := "encryption-key"
	systemLabels := map[string]string{}

	secondaryBootDiskRule := rules.NewRule(
		rules.WithMachineFamilyRule(proto.String("n1")),
		rules.WithSecondaryBootDiskRule(disk1, project1, mode),
	)
	manySecondaryBootDisksRule := rules.NewRule(
		rules.WithMachineFamilyRule(proto.String("n1")),
		rules.WithSecondaryBootDiskRule(disk1, project1, mode),
		rules.WithSecondaryBootDiskRule(disk2, project2, mode),
		rules.WithSecondaryBootDiskRule(disk3, project3, mode),
	)
	manySecondaryBootDisksWithStorageRule := rules.NewRule(
		rules.WithMachineFamilyRule(proto.String("n1")),
		rules.WithSecondaryBootDiskRule(disk1, project1, mode),
		rules.WithStorageRule(&bootDiskType, &bootDiskSize, &bootDiskEncryptionKey, &localSSDCount),
		rules.WithSecondaryBootDiskRule(disk3, project3, mode),
	)
	ccWithSecondaryBootDisk := computeclass.NewTestCrd(
		computeclass.WithName(ccName),
		computeclass.WithRules([]rules.Rule{secondaryBootDiskRule}),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithLabel(ccLabel),
	)

	ccWithManySecondaryBootDisks := computeclass.NewTestCrd(
		computeclass.WithName(ccName),
		computeclass.WithRules([]rules.Rule{manySecondaryBootDisksRule}),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithLabel(ccLabel),
	)

	ccWithManySecondaryBootDisksAndStorage := computeclass.NewTestCrd(
		computeclass.WithName(ccName),
		computeclass.WithRules([]rules.Rule{manySecondaryBootDisksWithStorageRule}),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithLabel(ccLabel),
	)

	ccWithOverlappingSecondaryBootDisksRules := computeclass.NewTestCrd(
		computeclass.WithName(ccName),
		computeclass.WithRules([]rules.Rule{secondaryBootDiskRule, manySecondaryBootDisksRule}),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithLabel(ccLabel),
	)

	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithAutoprovisioningEnabled(true).
		WithMachineTypes("test-machine-type").
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		Build()

	for name, tc := range map[string]struct {
		pods             []*apiv1.Pod
		ccs              []computeclass.CRD
		gpuRequest       machinetypes.GpuRequest
		wantRequirements [][]nodeGroupRequirements
		wantErr          map[types.UID]errors.AutoscalerError
	}{
		"cc with single secondary boot disk": {
			pods: []*apiv1.Pod{ccPod},
			ccs:  []computeclass.CRD{ccWithSecondaryBootDisk},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{ccPod}),
						withMachineFamily(machinetypes.N1),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(ccWithSecondaryBootDisk),
						withNodeConfigRule(secondaryBootDiskRule),
						withSecondaryBootDisk(disk1, project1, mode),
						withSystemLabels(systemLabels),
					),
				},
			},
		},
		"cc with many secondary boot disks": {
			pods: []*apiv1.Pod{ccPod},
			ccs:  []computeclass.CRD{ccWithManySecondaryBootDisks},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{ccPod}),
						withMachineFamily(machinetypes.N1),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(ccWithManySecondaryBootDisks),
						withNodeConfigRule(manySecondaryBootDisksRule),
						withSecondaryBootDisk(disk1, project1, mode),
						withSecondaryBootDisk(disk2, project2, mode),
						withSecondaryBootDisk(disk3, project3, mode),
						withSystemLabels(systemLabels),
					),
				},
			},
		},
		"cc with secondary boot disks and other storage data combined": {
			pods: []*apiv1.Pod{ccPod},
			ccs:  []computeclass.CRD{ccWithManySecondaryBootDisksAndStorage},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{ccPod}),
						withMachineFamily(machinetypes.N1),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(ccWithManySecondaryBootDisksAndStorage),
						withNodeConfigRule(manySecondaryBootDisksWithStorageRule),
						withSecondaryBootDisk(disk1, project1, mode),
						withSecondaryBootDisk(disk3, project3, mode),
						withBootDiskSize(bootDiskSize),
						withBootDiskType(bootDiskType),
						withEphemeralStorageLSSDCount(localSSDCount),
						withTotalLSSDCount(localSSDCount),
						withBootDiskEncryptionKey(bootDiskEncryptionKey),
						withSystemLabels(systemLabels),
					),
				},
			},
		},
		"cc with overlapping secondary boot disks rules": {
			pods: []*apiv1.Pod{ccPod},
			ccs:  []computeclass.CRD{ccWithOverlappingSecondaryBootDisksRules},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{ccPod}),
						withMachineFamily(machinetypes.N1),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(ccWithOverlappingSecondaryBootDisksRules),
						withNodeConfigRule(secondaryBootDiskRule),
						withSecondaryBootDisk(disk1, project1, mode),
						withSystemLabels(systemLabels),
					),
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{ccPod}),
						withMachineFamily(machinetypes.N1),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(ccWithOverlappingSecondaryBootDisksRules),
						withNodeConfigRule(manySecondaryBootDisksRule),
						withSecondaryBootDisk(disk1, project1, mode),
						withSecondaryBootDisk(disk2, project2, mode),
						withSecondaryBootDisk(disk3, project3, mode),
						withSystemLabels(systemLabels),
					),
				},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			lister := computeclass_lister.NewMockCrdLister(tc.ccs)
			lister.SetCrdLabel(ccLabel)
			em := experiments.NewMockManager()
			manager := NewAutoprovisioningNodeGroupManager(AutoprovisioningNodeGroupManagerOptions{
				CloudProvider: provider,
				Lister:        lister,
				Flags: AutoprovisioningNodeGroupManagerFlags{
					TpuAutoprovisioningEnabled: true,
				},
				ExperimentsManager:   em,
				OptionsTracker:       tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
				ResourcePolicyPuller: &placement.FakeResourcePolicyPullerProvider{},
			})

			requirements, err := manager.computePossibleRequirements(tc.pods, tc.gpuRequest, TpuRequest{})
			if diff := cmp.Diff(tc.wantRequirements, requirements, compareAllUnexportedOpt, requirementsSliceIgnoreOrderOpt); diff != "" {
				t.Error(diff)
			}
			if tc.wantErr == nil {
				assert.Empty(t, err)
			} else {
				assert.Equal(t, tc.wantErr, err)
			}
		})
	}
}

func TestComputePossibleRequirementsWithComputeClass(t *testing.T) {
	ccName := "cc"
	// ccLabel starts with "autoscaling.gke.io" so that it is treated as system label.
	ccLabel := "autoscaling.gke.io/cc-label"
	ccType := "CC"
	defaultCCName := "default-cc"
	otherCCName := "other-cc"
	gpuCCName := "gpu-cc"

	n2FamilyName := "n2"
	t2aFamilyName := "t2a"
	invalidFamilyName := "v10101"

	bootDiskType := "pd-standard"
	bootDiskSize := 100
	localSSDCount := 4
	bootDiskEncryptionKey := "encryption-key"

	pod := test.BuildTestPod("pod", 1, 128)
	defaultPod := test.BuildTestPod("default-pod", 1, 128)
	defaultPod = addSeparation(defaultPod, ccLabel, defaultCCName, true)
	ccPod := test.BuildTestPod("cc-pod", 1, 128)
	ccPod = addSeparation(ccPod, ccLabel, ccName, true)
	gpuCCPod := buildGpuPod("cc-gpu-pod", machinetypes.AnyGPU, 2)
	gpuCCPod = addSeparation(gpuCCPod, ccLabel, gpuCCName, true)
	otherPod := test.BuildTestPod("other-cc-pod", 1, 128)
	otherPod = addSeparation(otherPod, ccLabel, otherCCName, true)
	separationA1Pod := addSeparation(testPod("separationA-1"), "separation", "A", true)
	separationA2Pod := addSeparation(testPod("separationA-2"), "separation", "A", true)
	n2IceLake1Pod := addMinCpuPlatform(addMachineFamily(testPod("n2IceLake-1"), "n2"), "Intel_Ice_Lake")

	a100GpuRequest := machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: gkelabels.NvidiaTeslaA100}, Count: 2, PhysicalGPUCount: 2}
	h100GpuRequest := machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: gkelabels.NvidiaH100Mega_80gb}, Count: 8, PhysicalGPUCount: 8}
	gpuV100 := machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: gkelabels.NvidiaTeslaV100}, Count: 2, PhysicalGPUCount: 2}
	b200GpuRequest := machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: gkelabels.NvidiaB200}, Count: 8, PhysicalGPUCount: 8}
	gb200GpuRequest := machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: gkelabels.NvidiaGB200}, Count: 4, PhysicalGPUCount: 4}
	rtxPro6000GpuRequest := machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: gkelabels.NvidiaRTXPro6000}, Count: 2, PhysicalGPUCount: 2}
	e2Standard4 := "e2-standard-4"

	n1Standard8 := "n1-standard-8"
	machineTypeRule := rules.NewRule(rules.WithMachineTypeRule(&n1Standard8))
	systemLabels := map[string]string{}

	n2Rule := rules.NewMachineSpecRule(&n2FamilyName, nil, nil, nil)
	n2RuleStorage := rules.NewRule(
		rules.WithMachineFamilyRule(&n2FamilyName),
		rules.WithStorageRule(&bootDiskType, &bootDiskSize, &bootDiskEncryptionKey, &localSSDCount),
	)
	t2aRule := rules.NewMachineSpecRule(&t2aFamilyName, nil, nil, nil)
	incorrectRule := rules.NewMachineSpecRule(&invalidFamilyName, nil, nil, nil)
	gpuRule := rules.NewRule(rules.WithGpuRule(&a100GpuRequest))
	h100GpuRule := rules.NewRule(rules.WithGpuRule(&h100GpuRequest))
	b200GpuRule := rules.NewRule(rules.WithGpuRule(&b200GpuRequest))
	gb200GpuRule := rules.NewRule(rules.WithGpuRule(&gb200GpuRequest))
	rtxPro6000GpuRule := rules.NewRule(rules.WithGpuRule(&rtxPro6000GpuRequest))
	gpuAndInstanceTypeRule := rules.NewRule(rules.WithGpuRule(&gpuV100), rules.WithMachineTypeRule(&n1Standard8))
	npRule := rules.NewRule(rules.WithNodePoolsRule([]string{"some-nodepool"}))

	tpuCCName := "tpu-cc"
	tpuV4Rule := rules.NewRule(rules.WithTpuRule(gkelabels.TpuV4PodsliceValue, 4, "2x2x2"))
	tpuV5eRule := rules.NewRule(rules.WithTpuRule(gkelabels.TpuV5LitePodsliceValue, 4, "2x2"))
	incorrectTpuRule := rules.NewRule(rules.WithTpuRule("incorrect-tpu", 10, "6x6"))
	tpuPod := buildTpuPod("cc-tpu", "", 4, "")
	tpuPod = addSeparation(tpuPod, ccLabel, tpuCCName, true)

	gpuAndIncorrectInstanceTypeRule := rules.NewRule(rules.WithGpuRule(&gpuV100), rules.WithMachineTypeRule(&e2Standard4))

	n2LocalReservationRule := rules.NewRule(
		rules.WithMachineFamilyRule(proto.String("n2")),
		rules.WithReservationsRule(rules.NewReservation().WithReservationName("test-reservation").WithReservationAffinity(reservations.SpecificAffinity).WithReservationPath("test-reservation")),
	)
	n1ReservationsRule := rules.NewRule(
		rules.WithMachineFamilyRule(proto.String("n1")),
		rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation").WithReservationAffinity(reservations.SpecificAffinity).WithReservationPath("reservation")),
		rules.WithReservationsRule(rules.NewReservation().
			WithReservationName("reservation").
			WithReservationProject("other").
			WithReservationAffinity(reservations.SpecificAffinity).
			WithReservationPath("projects/other/reservations/reservation/reservationBlocks/res-block").
			WithReservationBlock("res-block")),
	)
	n2AnyReservationRule := rules.NewRule(
		rules.WithMachineFamilyRule(proto.String("n2")),
		rules.WithReservationsRule(rules.NewReservation().WithReservationAffinity(reservations.AnyAffinity)),
	)

	n1ReservationsWithZonesRule := rules.NewRule(
		rules.WithMachineFamilyRule(proto.String("n1")),
		rules.WithReservationsRule(rules.NewReservation().WithReservationName("reservation").WithReservationAffinity(reservations.SpecificAffinity).WithReservationZones([]string{"zone-1"}).WithReservationPath("reservation")),
		rules.WithReservationsRule(rules.NewReservation().
			WithReservationName("reservation").
			WithReservationProject("other").
			WithReservationZones([]string{"zone-1", "zone-2"}).
			WithReservationAffinity(reservations.SpecificAffinity).
			WithReservationPath("projects/other/reservations/reservation/reservationBlocks/res-block").
			WithReservationBlock("res-block")),
	)
	specifiedZonesRule := rules.NewRule(
		rules.WithMachineFamilyRule(proto.String("n1")),
		rules.WithLocationRule([]string{"zone-1", "zone-2"}),
	)

	mrd := 3600
	mrdRule := rules.NewRule(rules.WithMaxRunDurationRule(&mrd))
	mrdCCName := "mrd-cc"

	sysctls := map[string]string{"net.core.netdev_max_backlog": "1234"}
	hugepageSize1g := int64(3)
	hugepageSize2m := int64(1024)
	linuxNodeConfigRule := rules.NewRule(
		rules.WithMachineFamilyRule(proto.String("n2")),
		rules.WithSysctlsRule(sysctls),
		rules.WithHugepageSize1gRule(hugepageSize1g),
		rules.WithHugepageSize2mRule(hugepageSize2m),
	)
	cpuCfsQuota := true
	cpuCfsQuotaPeriod := "100ms"
	cpuManagerPolicy := "static"
	podPidsLimit := int64(10000)
	kubeletConfigRule := rules.NewRule(
		rules.WithMachineFamilyRule(proto.String("n2")),
		rules.WithCpuCfsQuotaRule(cpuCfsQuota),
		rules.WithCpuCfsQuotaPeriodRule(cpuCfsQuotaPeriod),
		rules.WithCpuManagerPolicyRule(cpuManagerPolicy),
		rules.WithPodPidsLimitRule(podPidsLimit),
	)

	defaultCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{n2Rule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(defaultCCName),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithCrdType(ccType),
	)
	nodeVersionCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{n2Rule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(ccName),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithNodeVersion("1.32.9-gke.1726000"),
	)
	singleICRuleCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{n2Rule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(ccName),
		computeclass.WithAutoprovisioningEnabled(),
	)
	manyICRulesCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{n2Rule, t2aRule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(ccName),
		computeclass.WithAutoprovisioningEnabled(),
	)
	scaleUpAnywayCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{n2Rule, t2aRule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(ccName),
		computeclass.WithScaleUpAnyway(),
		computeclass.WithAutoprovisioningEnabled(),
	)
	noAutoprovisioningCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{n2Rule, t2aRule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(ccName),
		computeclass.WithCrdType(ccType),
	)
	noRulesCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(ccName),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithCrdType(ccType),
	)
	singleNPRuleCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{npRule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(ccName),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithCrdType(ccType),
	)
	singleIncorrectRuleCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{incorrectRule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(ccName),
		computeclass.WithAutoprovisioningEnabled(),
	)
	machineTypeCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{machineTypeRule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(ccName),
		computeclass.WithAutoprovisioningEnabled(),
	)
	n2RuleStorageCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{n2RuleStorage}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(ccName),
		computeclass.WithAutoprovisioningEnabled(),
	)
	singleGpuCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{gpuRule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(gpuCCName),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithCrdType(ccType),
	)
	multipleGPUCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{gpuRule, h100GpuRule, b200GpuRule, gb200GpuRule, rtxPro6000GpuRule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(gpuCCName),
		computeclass.WithAutoprovisioningEnabled(),
	)
	scaleUpAnywayGPUCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{gpuRule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(gpuCCName),
		computeclass.WithScaleUpAnyway(),
		computeclass.WithAutoprovisioningEnabled(),
	)
	instanceTypeWithGpuCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{gpuAndInstanceTypeRule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(gpuCCName),
		computeclass.WithAutoprovisioningEnabled(),
	)
	instanceTypeWithIncorrectGpuCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{gpuAndIncorrectInstanceTypeRule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(gpuCCName),
		computeclass.WithAutoprovisioningEnabled(),
	)
	multipleReservationsCC := computeclass.NewTestCrd(
		computeclass.WithName(ccName),
		computeclass.WithLabel(ccLabel),
		computeclass.WithRules([]rules.Rule{n1ReservationsRule, n2LocalReservationRule, n2AnyReservationRule}),
		computeclass.WithAutoprovisioningEnabled(),
	)
	singleLocalReservationCC := computeclass.NewTestCrd(
		computeclass.WithName(ccName),
		computeclass.WithLabel(ccLabel),
		computeclass.WithRules([]rules.Rule{n2LocalReservationRule}),
		computeclass.WithAutoprovisioningEnabled(),
	)
	singleAnyReservationCC := computeclass.NewTestCrd(
		computeclass.WithName(ccName),
		computeclass.WithLabel(ccLabel),
		computeclass.WithRules([]rules.Rule{n2AnyReservationRule}),
		computeclass.WithAutoprovisioningEnabled(),
	)
	everythingCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{n2Rule, t2aRule, incorrectRule, npRule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(ccName),
		computeclass.WithScaleUpAnyway(),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithCrdType(ccType),
	)
	singleTpuCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{tpuV4Rule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(tpuCCName),
		computeclass.WithAutoprovisioningEnabled(),
	)
	multipleTpuCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{tpuV4Rule, tpuV5eRule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(tpuCCName),
		computeclass.WithAutoprovisioningEnabled(),
	)
	incorrectTPUCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{incorrectTpuRule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(tpuCCName),
		computeclass.WithAutoprovisioningEnabled(),
	)
	mrdCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{mrdRule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(mrdCCName),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithCrdType(ccType),
	)
	ccWithLinuxNodeConfig := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{linuxNodeConfigRule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(ccName),
		computeclass.WithAutoprovisioningEnabled(),
	)
	ccWithKubeletConfig := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{kubeletConfigRule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(ccName),
		computeclass.WithAutoprovisioningEnabled(),
	)
	ccWithSpecificReservationZones := computeclass.NewTestCrd(
		computeclass.WithName(ccName),
		computeclass.WithLabel(ccLabel),
		computeclass.WithRules([]rules.Rule{n1ReservationsWithZonesRule}),
		computeclass.WithAutoprovisioningEnabled(),
	)
	ccWithSpecifiedZones := computeclass.NewTestCrd(
		computeclass.WithName(ccName),
		computeclass.WithRules([]rules.Rule{specifiedZonesRule}),
		computeclass.WithAutoprovisioningEnabled(),
		computeclass.WithLabel(ccLabel),
	)
	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithAutoprovisioningEnabled(true).
		WithMachineTypes("test-machine-type").
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		Build()
	for name, tc := range map[string]struct {
		pods                      []*apiv1.Pod
		ccs                       []computeclass.CRD
		defaultCC                 bool
		useComputeClassDefaulting bool
		gpuRequest                machinetypes.GpuRequest
		wantRequirements          [][]nodeGroupRequirements
		wantErr                   map[types.UID]errors.AutoscalerError
	}{
		"pod without cc selector, cc default disabled": {
			pods:                      []*apiv1.Pod{pod},
			defaultCC:                 false,
			useComputeClassDefaulting: true,
			wantRequirements: [][]nodeGroupRequirements{
				{
					requirementsForCC([]*apiv1.Pod{pod}, provider.GetAutoprovisioningDefaultFamily(), machinetypes.SelectionTypeDefault, nil, nil),
				},
			},
		},
		"pod without cc selector, machine spec is preserved if cc default disabled": {
			pods:                      []*apiv1.Pod{n2IceLake1Pod},
			defaultCC:                 false,
			useComputeClassDefaulting: true,
			wantRequirements: [][]nodeGroupRequirements{
				{{
					pods:                     []*apiv1.Pod{n2IceLake1Pod},
					machineSpec:              machinetypes.NewMachineSpecSingleFamily(machinetypes.N2, machinetypes.IntelIceLake, "", ""),
					machineSelectionType:     machinetypes.SelectionTypeSpecified,
					workloadSeparationTaints: []apiv1.Taint{},
					workloadSeparationLabels: map[string]string{},
					systemLabels:             map[string]string{},
				}},
			},
		},
		"pods without cc selector, workload separation is preserved if cc default disabled": {
			pods:                      []*apiv1.Pod{separationA1Pod, separationA2Pod},
			defaultCC:                 false,
			useComputeClassDefaulting: true,
			wantRequirements: [][]nodeGroupRequirements{
				{{
					pods:                     []*apiv1.Pod{separationA1Pod, separationA2Pod},
					machineSpec:              machinetypes.NewMachineSpec([]machinetypes.MachineFamily{provider.GetAutoprovisioningDefaultFamily()}, machinetypes.AnyPlatform, "", ""),
					machineSelectionType:     machinetypes.SelectionTypeDefault,
					workloadSeparationTaints: []apiv1.Taint{{Key: "separation", Value: "A", Effect: apiv1.TaintEffectNoSchedule}},
					workloadSeparationLabels: map[string]string{"separation": "A"},
					systemLabels:             map[string]string{},
				}},
			},
		},
		"pod with default cc selector, cc default disabled": {
			pods:                      []*apiv1.Pod{defaultPod},
			defaultCC:                 false,
			useComputeClassDefaulting: true,
			wantErr: map[types.UID]errors.AutoscalerError{
				defaultPod.GetUID(): NewComputeClassNotFoundError(defaultCCName, ccType, nil),
			},
		},
		"pod with non-default cc selector, non-default cc not found, cc default disabled": {
			pods:                      []*apiv1.Pod{ccPod},
			defaultCC:                 false,
			useComputeClassDefaulting: true,
			wantErr: map[types.UID]errors.AutoscalerError{
				ccPod.GetUID(): NewComputeClassNotFoundError(ccName, ccType, nil),
			},
		},
		"pod with non-default cc selector, cc with IC rule, cc default disabled": {
			pods:                      []*apiv1.Pod{ccPod},
			ccs:                       []computeclass.CRD{singleICRuleCC},
			defaultCC:                 false,
			useComputeClassDefaulting: true,
			wantRequirements: [][]nodeGroupRequirements{
				{
					requirementsForCC([]*apiv1.Pod{ccPod}, machinetypes.N2, machinetypes.SelectionTypeSpecified, singleICRuleCC, rules.NewMachineSpecRule(&n2FamilyName, nil, nil, nil)),
				},
			},
		},
		"pod without cc selector, cc default enabled but not defined": {
			pods:                      []*apiv1.Pod{pod},
			defaultCC:                 true,
			useComputeClassDefaulting: true,
			wantRequirements: [][]nodeGroupRequirements{
				{
					requirementsForCC([]*apiv1.Pod{pod}, provider.GetAutoprovisioningDefaultFamily(), machinetypes.SelectionTypeDefault, nil, nil),
				},
			},
		},
		"pods without cc selector, workload separation is preserved if cc default enabled but not defined": {
			pods:                      []*apiv1.Pod{separationA1Pod, separationA2Pod},
			defaultCC:                 true,
			useComputeClassDefaulting: true,
			wantRequirements: [][]nodeGroupRequirements{
				{{
					pods:                     []*apiv1.Pod{separationA1Pod, separationA2Pod},
					machineSpec:              machinetypes.NewMachineSpec([]machinetypes.MachineFamily{provider.GetAutoprovisioningDefaultFamily()}, machinetypes.AnyPlatform, "", ""),
					machineSelectionType:     machinetypes.SelectionTypeDefault,
					workloadSeparationTaints: []apiv1.Taint{{Key: "separation", Value: "A", Effect: apiv1.TaintEffectNoSchedule}},
					workloadSeparationLabels: map[string]string{"separation": "A"},
					systemLabels:             map[string]string{},
				}},
			},
		},
		"pod without cc selector, machine spec is preserved if cc default enabled but not defined": {
			pods:                      []*apiv1.Pod{n2IceLake1Pod},
			defaultCC:                 true,
			useComputeClassDefaulting: true,
			wantRequirements: [][]nodeGroupRequirements{
				{{
					pods:                     []*apiv1.Pod{n2IceLake1Pod},
					machineSpec:              machinetypes.NewMachineSpecSingleFamily(machinetypes.N2, machinetypes.IntelIceLake, "", ""),
					machineSelectionType:     machinetypes.SelectionTypeSpecified,
					workloadSeparationTaints: []apiv1.Taint{},
					workloadSeparationLabels: map[string]string{},
					systemLabels:             map[string]string{},
				}},
			},
		},
		"pod with default cc selector, cc default enabled but not defined": {
			pods:                      []*apiv1.Pod{defaultPod},
			defaultCC:                 true,
			useComputeClassDefaulting: true,
			wantErr: map[types.UID]errors.AutoscalerError{
				defaultPod.GetUID(): NewComputeClassNotFoundError(defaultCCName, ccType, nil),
			},
		},
		"pod with non-default cc selector, non-default cc not found, cc default enabled but not defined": {
			pods:                      []*apiv1.Pod{ccPod},
			defaultCC:                 true,
			useComputeClassDefaulting: true,
			wantErr: map[types.UID]errors.AutoscalerError{
				ccPod.GetUID(): NewComputeClassNotFoundError(ccName, ccType, nil),
			},
		},
		"pod without cc selector, cc default exists": {
			pods:                      []*apiv1.Pod{pod},
			ccs:                       []computeclass.CRD{defaultCC},
			defaultCC:                 true,
			useComputeClassDefaulting: true,
			wantRequirements: [][]nodeGroupRequirements{
				{
					requirementsForCC([]*apiv1.Pod{pod}, machinetypes.N2, machinetypes.SelectionTypeSpecified, defaultCC, rules.NewMachineSpecRule(&n2FamilyName, nil, nil, nil)),
				},
			},
		},
		"pod with default cc selector, cc default exists": {
			pods:                      []*apiv1.Pod{defaultPod},
			ccs:                       []computeclass.CRD{defaultCC},
			defaultCC:                 true,
			useComputeClassDefaulting: true,
			wantRequirements: [][]nodeGroupRequirements{
				{
					requirementsForCC([]*apiv1.Pod{defaultPod}, machinetypes.N2, machinetypes.SelectionTypeSpecified, defaultCC, rules.NewMachineSpecRule(&n2FamilyName, nil, nil, nil)),
				},
			},
		},
		"pod with non-default cc selector, non-default cc not found, cc default exists": {
			pods:                      []*apiv1.Pod{ccPod},
			ccs:                       []computeclass.CRD{defaultCC},
			defaultCC:                 true,
			useComputeClassDefaulting: true,
			wantErr: map[types.UID]errors.AutoscalerError{
				ccPod.GetUID(): NewComputeClassNotFoundError(ccName, ccType, nil),
			},
		},
		"pod with non-default cc selector, cc with IC rule, cc default exists": {
			pods:                      []*apiv1.Pod{ccPod},
			ccs:                       []computeclass.CRD{defaultCC, singleICRuleCC},
			defaultCC:                 true,
			useComputeClassDefaulting: true,
			wantRequirements: [][]nodeGroupRequirements{
				{
					requirementsForCC([]*apiv1.Pod{ccPod}, machinetypes.N2, machinetypes.SelectionTypeSpecified, singleICRuleCC, rules.NewMachineSpecRule(&n2FamilyName, nil, nil, nil)),
				},
			},
		},
		"pod without cc selector, default cc not set": {
			pods: []*apiv1.Pod{pod},
			wantRequirements: [][]nodeGroupRequirements{
				{
					requirementsForCC([]*apiv1.Pod{pod}, provider.GetAutoprovisioningDefaultFamily(), machinetypes.SelectionTypeDefault, nil, nil),
				},
			},
		},
		"pod without cc selector, default cc not found": {
			pods:      []*apiv1.Pod{pod},
			defaultCC: true,
			wantErr: map[types.UID]errors.AutoscalerError{
				pod.GetUID(): NewComputeClassNotFoundError(defaultCCName, ccType, nil),
			},
		},
		"pod without cc selector, default cc exists": {
			pods:      []*apiv1.Pod{pod},
			ccs:       []computeclass.CRD{defaultCC},
			defaultCC: true,
			wantRequirements: [][]nodeGroupRequirements{
				{
					requirementsForCC([]*apiv1.Pod{pod}, machinetypes.N2, machinetypes.SelectionTypeSpecified, defaultCC, rules.NewMachineSpecRule(&n2FamilyName, nil, nil, nil)),
				},
			},
		},
		"pod with default cc selector, default cc not found": {
			pods:      []*apiv1.Pod{defaultPod},
			defaultCC: true,
			wantErr: map[types.UID]errors.AutoscalerError{
				defaultPod.GetUID(): NewComputeClassNotFoundError(defaultCCName, ccType, nil),
			},
		},
		"pod with default cc selector, default cc exists": {
			pods:      []*apiv1.Pod{defaultPod},
			ccs:       []computeclass.CRD{defaultCC},
			defaultCC: true,
			wantRequirements: [][]nodeGroupRequirements{
				{
					requirementsForCC([]*apiv1.Pod{defaultPod}, machinetypes.N2, machinetypes.SelectionTypeSpecified, defaultCC, rules.NewMachineSpecRule(&n2FamilyName, nil, nil, nil)),
				},
			},
		},
		"pod with non-default cc selector, non-default cc not found": {
			pods: []*apiv1.Pod{ccPod},
			wantErr: map[types.UID]errors.AutoscalerError{
				ccPod.GetUID(): NewComputeClassNotFoundError(ccName, ccType, nil),
			},
		},
		"pod with non-default cc selector, cc with a single IC rule": {
			pods: []*apiv1.Pod{ccPod},
			ccs:  []computeclass.CRD{singleICRuleCC},
			wantRequirements: [][]nodeGroupRequirements{
				{
					requirementsForCC([]*apiv1.Pod{ccPod}, machinetypes.N2, machinetypes.SelectionTypeSpecified, singleICRuleCC, rules.NewMachineSpecRule(&n2FamilyName, nil, nil, nil)),
				},
			},
		},
		"pod with non-default cc selector, cc with many IC rules": {
			pods: []*apiv1.Pod{ccPod},
			ccs:  []computeclass.CRD{manyICRulesCC},
			wantRequirements: [][]nodeGroupRequirements{
				{
					requirementsForCC([]*apiv1.Pod{ccPod}, machinetypes.N2, machinetypes.SelectionTypeSpecified, manyICRulesCC, rules.NewMachineSpecRule(&n2FamilyName, nil, nil, nil)),
					requirementsForCC([]*apiv1.Pod{ccPod}, machinetypes.T2A, machinetypes.SelectionTypeSpecified, manyICRulesCC, rules.NewMachineSpecRule(&t2aFamilyName, nil, nil, nil)),
				},
			},
		},
		"pod with non-default cc selector, cc with ScaleUpAnyway": {
			pods: []*apiv1.Pod{ccPod},
			ccs:  []computeclass.CRD{scaleUpAnywayCC},
			wantRequirements: [][]nodeGroupRequirements{
				{
					requirementsForCC([]*apiv1.Pod{ccPod}, machinetypes.N2, machinetypes.SelectionTypeSpecified, scaleUpAnywayCC, rules.NewMachineSpecRule(&n2FamilyName, nil, nil, nil)),
					requirementsForCC([]*apiv1.Pod{ccPod}, machinetypes.T2A, machinetypes.SelectionTypeSpecified, scaleUpAnywayCC, rules.NewMachineSpecRule(&t2aFamilyName, nil, nil, nil)),
					requirementsForCC([]*apiv1.Pod{ccPod}, provider.GetAutoprovisioningDefaultFamily(), machinetypes.SelectionTypeDefault, scaleUpAnywayCC, nil),
				},
			},
		},
		"pod with non-default cc selector, cc without NAP": {
			pods: []*apiv1.Pod{ccPod},
			ccs:  []computeclass.CRD{noAutoprovisioningCC},
			wantErr: map[types.UID]errors.AutoscalerError{
				ccPod.GetUID(): NewComputeClassAutoprovisioningDisabled(ccName, ccType),
			},
		},
		"pod with non-default cc selector, cc with no rules": {
			pods: []*apiv1.Pod{ccPod},
			ccs:  []computeclass.CRD{noRulesCC},
			wantErr: map[types.UID]errors.AutoscalerError{
				ccPod.GetUID(): NewComputeClassPodIncompatibleError(ccName, ccType),
			},
		},
		"pod with non-default cc selector, cc with nodepool rule": {
			pods: []*apiv1.Pod{ccPod},
			ccs:  []computeclass.CRD{singleNPRuleCC},
			wantErr: map[types.UID]errors.AutoscalerError{
				ccPod.GetUID(): NewComputeClassPodIncompatibleError(ccName, ccType),
			},
		},
		"pod with non-default cc selector, cc with incorrect rule": {
			pods: []*apiv1.Pod{ccPod},
			ccs:  []computeclass.CRD{singleIncorrectRuleCC},
			wantErr: map[types.UID]errors.AutoscalerError{
				ccPod.GetUID(): machineselection.NewMachineFamilyUnknownError("v10101"),
			},
		},
		"pod with non-default cc selector, CRD with machine type": {
			pods: []*apiv1.Pod{ccPod},
			ccs:  []computeclass.CRD{machineTypeCC},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{ccPod}),
						withMachineFamily(machinetypes.N1),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(machineTypeCC),
						withNodeConfigRule(machineTypeRule),
						withMachineType("n1-standard-8"),
						withSystemLabels(systemLabels)),
				},
			},
		},
		"pod with non-default cc selector, CRD with node version": {
			pods: []*apiv1.Pod{ccPod},
			ccs:  []computeclass.CRD{nodeVersionCC},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{ccPod}),
						withMachineFamily(machinetypes.N2),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(nodeVersionCC),
						withNodeConfigRule(n2Rule),
						withNodeVersion("1.32.9-gke.1726000"),
						withSystemLabels(systemLabels)),
				},
			},
		},
		"pod with non-default cc selector, cc with everything": {
			pods: []*apiv1.Pod{ccPod},
			ccs:  []computeclass.CRD{everythingCC},
			wantRequirements: [][]nodeGroupRequirements{
				{
					requirementsForCC([]*apiv1.Pod{ccPod}, machinetypes.N2, machinetypes.SelectionTypeSpecified, everythingCC, rules.NewMachineSpecRule(&n2FamilyName, nil, nil, nil)),
					requirementsForCC([]*apiv1.Pod{ccPod}, machinetypes.T2A, machinetypes.SelectionTypeSpecified, everythingCC, rules.NewMachineSpecRule(&t2aFamilyName, nil, nil, nil)),
					requirementsForCC([]*apiv1.Pod{ccPod}, provider.GetAutoprovisioningDefaultFamily(), machinetypes.SelectionTypeDefault, everythingCC, nil),
				},
			},
		},
		"pod with non-default cc selector, CRD with gpu a100 requirements": {
			pods: []*apiv1.Pod{gpuCCPod},
			ccs:  []computeclass.CRD{singleGpuCC},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{gpuCCPod}),
						withMachineFamily(machinetypes.A2),
						withMachineSelectionType(machinetypes.SelectionTypeImplied),
						withComputeClass(singleGpuCC),
						withNodeConfigRule(gpuRule),
						withGPURequest(gkelabels.NvidiaTeslaA100, 2),
						withSystemLabels(systemLabels)),
				},
			},
		},
		"pod with non-default cc selector, CRD with multiple gpu requirements": {
			pods:       []*apiv1.Pod{gpuCCPod},
			ccs:        []computeclass.CRD{multipleGPUCC},
			gpuRequest: machinetypes.GpuRequest{},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{gpuCCPod}),
						withMachineFamily(machinetypes.A2),
						withMachineSelectionType(machinetypes.SelectionTypeImplied),
						withComputeClass(multipleGPUCC),
						withNodeConfigRule(gpuRule),
						withSystemLabels(systemLabels),
						withGPURequest(gkelabels.NvidiaTeslaA100, 2)),
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{gpuCCPod}),
						withMachineFamily(machinetypes.A3),
						withMachineSelectionType(machinetypes.SelectionTypeImplied),
						withComputeClass(multipleGPUCC),
						withNodeConfigRule(h100GpuRule),
						withSystemLabels(systemLabels),
						withGPURequest(gkelabels.NvidiaH100Mega_80gb, 8)),
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{gpuCCPod}),
						withMachineFamily(machinetypes.A4),
						withMachineSelectionType(machinetypes.SelectionTypeImplied),
						withComputeClass(multipleGPUCC),
						withNodeConfigRule(b200GpuRule),
						withSystemLabels(systemLabels),
						withGPURequest(gkelabels.NvidiaB200, 8)),
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{gpuCCPod}),
						withMachineFamily(machinetypes.A4X),
						withMachineSelectionType(machinetypes.SelectionTypeImplied),
						withComputeClass(multipleGPUCC),
						withNodeConfigRule(gb200GpuRule),
						withSystemLabels(systemLabels),
						withGPURequest(gkelabels.NvidiaGB200, 4)),
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{gpuCCPod}),
						withMachineFamily(machinetypes.G4),
						withMachineSelectionType(machinetypes.SelectionTypeImplied),
						withComputeClass(multipleGPUCC),
						withNodeConfigRule(rtxPro6000GpuRule),
						withSystemLabels(systemLabels),
						withGPURequest(gkelabels.NvidiaRTXPro6000, 2)),
				},
			},
		},
		"pod with non-default cc selector, CRD with instance type and gpu requirements": {
			pods:       []*apiv1.Pod{gpuCCPod},
			ccs:        []computeclass.CRD{instanceTypeWithGpuCC},
			gpuRequest: machinetypes.GpuRequest{},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{gpuCCPod}),
						withMachineFamily(machinetypes.N1),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(instanceTypeWithGpuCC),
						withNodeConfigRule(gpuAndInstanceTypeRule),
						withMachineType("n1-standard-8"),
						withSystemLabels(systemLabels),
						withGPURequest(gkelabels.NvidiaTeslaV100, 2)),
				},
			},
		},
		"pod with non-default cc selector, CRD with incompatible instance type and gpu requirements": {
			pods:       []*apiv1.Pod{gpuCCPod},
			ccs:        []computeclass.CRD{instanceTypeWithIncorrectGpuCC},
			gpuRequest: machinetypes.GpuRequest{},
			wantErr: map[types.UID]errors.AutoscalerError{
				gpuCCPod.GetUID(): machineselection.NewGpuIncompatibleError(fmt.Sprintf("machine family %q", "e2"), "nvidia-tesla-v100"),
			},
		},
		"pod with non-default cc selector, CRD with scale up anyway, non empty gpu requirements": {
			pods: []*apiv1.Pod{gpuCCPod},
			ccs:  []computeclass.CRD{scaleUpAnywayGPUCC},
			gpuRequest: machinetypes.GpuRequest{
				Config: machinetypes.GpuConfig{
					GpuType: gkelabels.NvidiaH100Mega_80gb,
				},
				Count:            8,
				PhysicalGPUCount: 8,
			},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{gpuCCPod}),
						withMachineFamily(machinetypes.A3),
						withMachineSelectionType(machinetypes.SelectionTypeImplied),
						withComputeClass(scaleUpAnywayGPUCC),
						withSystemLabels(systemLabels),
						withGPURequest(gkelabels.NvidiaH100Mega_80gb, 8)),
				},
			},
		},
		"pod with non-default cc selector, CRD with scale up anyway, non empty gpu B200 requirements": {
			pods: []*apiv1.Pod{gpuCCPod},
			ccs:  []computeclass.CRD{scaleUpAnywayGPUCC},
			gpuRequest: machinetypes.GpuRequest{
				Config: machinetypes.GpuConfig{
					GpuType: gkelabels.NvidiaB200,
				},
				Count:            8,
				PhysicalGPUCount: 8,
			},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{gpuCCPod}),
						withMachineFamily(machinetypes.A4),
						withMachineSelectionType(machinetypes.SelectionTypeImplied),
						withComputeClass(scaleUpAnywayGPUCC),
						withSystemLabels(systemLabels),
						withGPURequest(gkelabels.NvidiaB200, 8)),
				},
			},
		},
		"pod with non-default cc selector, CRD with scale up anyway, non empty gpu GB200 requirements": {
			pods: []*apiv1.Pod{gpuCCPod},
			ccs:  []computeclass.CRD{scaleUpAnywayGPUCC},
			gpuRequest: machinetypes.GpuRequest{
				Config: machinetypes.GpuConfig{
					GpuType: gkelabels.NvidiaGB200,
				},
				Count:            4,
				PhysicalGPUCount: 4,
			},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{gpuCCPod}),
						withMachineFamily(machinetypes.A4X),
						withMachineSelectionType(machinetypes.SelectionTypeImplied),
						withComputeClass(scaleUpAnywayGPUCC),
						withSystemLabels(systemLabels),
						withGPURequest(gkelabels.NvidiaGB200, 4)),
				},
			},
		},
		"pod with non-default cc selector, CRD with scale up anyway, non empty gpu RTX PRO 6000 requirements": {
			pods: []*apiv1.Pod{gpuCCPod},
			ccs:  []computeclass.CRD{scaleUpAnywayGPUCC},
			gpuRequest: machinetypes.GpuRequest{
				Config: machinetypes.GpuConfig{
					GpuType: gkelabels.NvidiaRTXPro6000,
				},
				Count:            2,
				PhysicalGPUCount: 2,
			},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{gpuCCPod}),
						withMachineFamily(machinetypes.G4),
						withMachineSelectionType(machinetypes.SelectionTypeImplied),
						withComputeClass(scaleUpAnywayGPUCC),
						withSystemLabels(systemLabels),
						withGPURequest(gkelabels.NvidiaRTXPro6000, 2)),
				},
			},
		},
		"pod with non-default cc selector, CRD without scale up anyway, non empty gpu requirements": {
			pods: []*apiv1.Pod{gpuCCPod},
			ccs:  []computeclass.CRD{singleGpuCC},
			gpuRequest: machinetypes.GpuRequest{
				Config: machinetypes.GpuConfig{
					GpuType: gkelabels.NvidiaH100Mega_80gb,
				},
				Count:            8,
				PhysicalGPUCount: 8,
			},
			wantErr: map[types.UID]errors.AutoscalerError{
				gpuCCPod.GetUID(): NewComputeClassPodIncompatibleError(gpuCCName, ccType),
			},
		},
		"pod with mrd cc selector; pod requesting 3600 MRD; CRD with 3600 MRD requirement - get 3600": {
			pods: []*apiv1.Pod{
				test.BuildTestPod("mrd-pod", 0, 1000, func(pod *apiv1.Pod) {
					addSeparation(pod, gkelabels.MaxRunDurationLabelKey, "3600", true)
					addSeparation(pod, ccLabel, mrdCCName, true)
				}),
			},
			ccs: []computeclass.CRD{mrdCC},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(
						withPods([]*apiv1.Pod{
							test.BuildTestPod("mrd-pod", 0, 1000, func(pod *apiv1.Pod) {
								addSeparation(pod, gkelabels.MaxRunDurationLabelKey, "3600", true)
								addSeparation(pod, ccLabel, mrdCCName, true)
							}),
						}),
						withMachineFamily(provider.GetAutoprovisioningDefaultFamily()),
						withMachineSelectionType(machinetypes.SelectionTypeDefault),
						withComputeClass(mrdCC),
						withNodeConfigRule(mrdRule),
						withSystemLabels(systemLabels),
						WithMaxRunDurationSeconds(3600),
					),
				},
			},
		},
		"pod with mrd cc selector; pod requesting 7200 MRD; CRD with 3600 MRD requirement - get error": {
			pods: []*apiv1.Pod{
				test.BuildTestPod("mrd-pod", 0, 1000, func(pod *apiv1.Pod) {
					addSeparation(pod, gkelabels.MaxRunDurationLabelKey, "7200", true)
					addSeparation(pod, ccLabel, mrdCCName, true)
				}),
			},
			ccs: []computeclass.CRD{mrdCC},
			wantErr: map[types.UID]errors.AutoscalerError{
				types.UID("mrd-pod"): NewComputeClassPodIncompatibleError(mrdCCName, ccType),
			},
		},
		"pod with mrd cc selector; pod not requesting MRD; CRD with 3600 MRD requirement - get 3600": {
			pods: []*apiv1.Pod{
				test.BuildTestPod("mrd-pod", 0, 1000, func(pod *apiv1.Pod) {
					addSeparation(pod, ccLabel, mrdCCName, true)
				}),
			},
			ccs: []computeclass.CRD{mrdCC},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(
						withPods([]*apiv1.Pod{
							test.BuildTestPod("mrd-pod", 0, 1000, func(pod *apiv1.Pod) {
								addSeparation(pod, ccLabel, mrdCCName, true)
							}),
						}),
						withMachineFamily(provider.GetAutoprovisioningDefaultFamily()),
						withMachineSelectionType(machinetypes.SelectionTypeDefault),
						withComputeClass(mrdCC),
						withNodeConfigRule(mrdRule),
						withSystemLabels(systemLabels),
						WithMaxRunDurationSeconds(3600),
					),
				},
			},
		},
		"pod with default cc selector; pod requesting 3600 MRD; CRD without MRD requirement - get 3600": {
			pods: []*apiv1.Pod{
				test.BuildTestPod("mrd-pod", 0, 1000, func(pod *apiv1.Pod) {
					addSeparation(pod, gkelabels.MaxRunDurationLabelKey, "3600", true)
					addSeparation(pod, ccLabel, defaultCCName, true)
				}),
			},
			ccs: []computeclass.CRD{defaultCC},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(
						withPods([]*apiv1.Pod{
							test.BuildTestPod("mrd-pod", 0, 1000, func(pod *apiv1.Pod) {
								addSeparation(pod, gkelabels.MaxRunDurationLabelKey, "3600", true)
								addSeparation(pod, ccLabel, defaultCCName, true)
							}),
						}),
						withMachineFamily(machinetypes.N2),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(defaultCC),
						withNodeConfigRule(n2Rule),
						withSystemLabels(systemLabels),
						WithMaxRunDurationSeconds(3600),
					),
				},
			},
		},
		"pod with default cc selector; pod not requesting MRD; CRD without MRD requirement - get none": {
			pods: []*apiv1.Pod{
				test.BuildTestPod("mrd-pod", 0, 1000, func(pod *apiv1.Pod) {
					addSeparation(pod, ccLabel, defaultCCName, true)
				}),
			},
			ccs: []computeclass.CRD{defaultCC},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(
						withPods([]*apiv1.Pod{
							test.BuildTestPod("mrd-pod", 0, 1000, func(pod *apiv1.Pod) {
								addSeparation(pod, ccLabel, defaultCCName, true)
							}),
						}),
						withMachineFamily(machinetypes.N2),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(defaultCC),
						withSystemLabels(systemLabels),
						withNodeConfigRule(n2Rule),
					),
				},
			},
		},
		"pod with non-default cc selector, CRD with single TPU requirements": {
			pods: []*apiv1.Pod{tpuPod},
			ccs:  []computeclass.CRD{singleTpuCC},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{tpuPod}),
						withMachineFamily(machinetypes.CT4P),
						withMachineSelectionType(machinetypes.SelectionTypeImplied),
						withComputeClass(singleTpuCC),
						withNodeConfigRule(tpuV4Rule),
						withSystemLabels(systemLabels),
						withTPURequest(gkelabels.TpuV4PodsliceValue, "2x2x2", 4)),
				},
			},
		},
		"pod with non-default cc selector, CRD with multiple TPU requirements": {
			pods: []*apiv1.Pod{tpuPod},
			ccs:  []computeclass.CRD{multipleTpuCC},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{tpuPod}),
						withMachineFamily(machinetypes.CT4P),
						withMachineSelectionType(machinetypes.SelectionTypeImplied),
						withComputeClass(multipleTpuCC),
						withNodeConfigRule(tpuV4Rule),
						withSystemLabels(systemLabels),
						withTPURequest(gkelabels.TpuV4PodsliceValue, "2x2x2", 4)),
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{tpuPod}),
						withMachineFamily(machinetypes.CT5LP),
						withMachineSelectionType(machinetypes.SelectionTypeImplied),
						withComputeClass(multipleTpuCC),
						withNodeConfigRule(tpuV5eRule),
						withSystemLabels(systemLabels),
						withTPURequest(gkelabels.TpuV5LitePodsliceValue, "2x2", 4)),
				},
			},
		},
		"pod with non-default cc selector, CRD with incorrect TPU requirements": {
			pods: []*apiv1.Pod{tpuPod},
			ccs:  []computeclass.CRD{incorrectTPUCC},
			wantErr: map[types.UID]errors.AutoscalerError{
				tpuPod.GetUID(): NewTpuTypeNotSupportedError("incorrect-tpu"),
			},
		},
		"pod with non-default cc selector, cc with storage": {
			pods: []*apiv1.Pod{ccPod},
			ccs:  []computeclass.CRD{n2RuleStorageCC},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{ccPod}),
						withMachineFamily(machinetypes.N2),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(n2RuleStorageCC),
						withNodeConfigRule(n2RuleStorage),
						withBootDiskSize(bootDiskSize),
						withBootDiskType(bootDiskType),
						withEphemeralStorageLSSDCount(localSSDCount),
						withTotalLSSDCount(localSSDCount),
						withSystemLabels(systemLabels),
						withBootDiskEncryptionKey(bootDiskEncryptionKey)),
				},
			},
		},
		"multiple pods, default cc set": {
			pods:      []*apiv1.Pod{pod, defaultPod, ccPod, otherPod},
			ccs:       []computeclass.CRD{everythingCC, defaultCC},
			defaultCC: true,
			wantRequirements: [][]nodeGroupRequirements{
				{
					requirementsForCC([]*apiv1.Pod{pod, defaultPod}, machinetypes.N2, machinetypes.SelectionTypeSpecified, defaultCC, rules.NewMachineSpecRule(&n2FamilyName, nil, nil, nil)),
				},
				{
					requirementsForCC([]*apiv1.Pod{ccPod}, machinetypes.N2, machinetypes.SelectionTypeSpecified, everythingCC, rules.NewMachineSpecRule(&n2FamilyName, nil, nil, nil)),
					requirementsForCC([]*apiv1.Pod{ccPod}, machinetypes.T2A, machinetypes.SelectionTypeSpecified, everythingCC, rules.NewMachineSpecRule(&t2aFamilyName, nil, nil, nil)),
					requirementsForCC([]*apiv1.Pod{ccPod}, provider.GetAutoprovisioningDefaultFamily(), machinetypes.SelectionTypeDefault, everythingCC, nil),
				},
			},
			wantErr: map[types.UID]errors.AutoscalerError{
				otherPod.GetUID(): NewComputeClassNotFoundError(otherCCName, ccType, nil),
			},
		},
		"multiple pods, default cc not set": {
			pods: []*apiv1.Pod{pod, defaultPod, ccPod, otherPod},
			ccs:  []computeclass.CRD{everythingCC, defaultCC},
			wantRequirements: [][]nodeGroupRequirements{
				{
					requirementsForCC([]*apiv1.Pod{pod}, provider.GetAutoprovisioningDefaultFamily(), machinetypes.SelectionTypeDefault, nil, nil),
				},
				{
					requirementsForCC([]*apiv1.Pod{defaultPod}, machinetypes.N2, machinetypes.SelectionTypeSpecified, defaultCC, rules.NewMachineSpecRule(&n2FamilyName, nil, nil, nil)),
				},
				{
					requirementsForCC([]*apiv1.Pod{ccPod}, machinetypes.N2, machinetypes.SelectionTypeSpecified, everythingCC, rules.NewMachineSpecRule(&n2FamilyName, nil, nil, nil)),
					requirementsForCC([]*apiv1.Pod{ccPod}, machinetypes.T2A, machinetypes.SelectionTypeSpecified, everythingCC, rules.NewMachineSpecRule(&t2aFamilyName, nil, nil, nil)),
					requirementsForCC([]*apiv1.Pod{ccPod}, provider.GetAutoprovisioningDefaultFamily(), machinetypes.SelectionTypeDefault, everythingCC, nil),
				},
			},
			wantErr: map[types.UID]errors.AutoscalerError{
				otherPod.GetUID(): NewComputeClassNotFoundError(otherCCName, ccType, nil),
			},
		},
		"pod with non-default cc selector, cc with multiple reservations": {
			pods: []*apiv1.Pod{ccPod},
			ccs:  []computeclass.CRD{multipleReservationsCC},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{ccPod}),
						withMachineFamily(machinetypes.N1),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(multipleReservationsCC),
						withNodeConfigRule(n1ReservationsRule),
						withReservationName("reservation"),
						withReservationProject("12345"),
						withReservationExists(),
						withSystemLabels(systemLabels),
						withReservationAffinity(reservations.SpecificAffinity),
					),
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{ccPod}),
						withMachineFamily(machinetypes.N1),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(multipleReservationsCC),
						withNodeConfigRule(n1ReservationsRule),
						withReservationName("reservation"),
						withReservationExists(),
						withReservationAffinity(reservations.SpecificAffinity),
						withReservationProject("other"),
						withSystemLabels(systemLabels),
						withReservationBlock("res-block"),
					),
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{ccPod}),
						withMachineFamily(machinetypes.N2),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(multipleReservationsCC),
						withNodeConfigRule(n2LocalReservationRule),
						withReservationName("test-reservation"),
						withReservationProject("12345"),
						withReservationExists(),
						withSystemLabels(systemLabels),
						withReservationAffinity(reservations.SpecificAffinity),
					),
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{ccPod}),
						withMachineFamily(machinetypes.N2),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(multipleReservationsCC),
						withNodeConfigRule(n2AnyReservationRule),
						withReservationExists(),
						withSystemLabels(systemLabels),
						withReservationAffinity(reservations.AnyAffinity),
					),
				},
			},
		},
		"pod with non-default cc selector, cc with local specific reservation": {
			pods: []*apiv1.Pod{ccPod},
			ccs:  []computeclass.CRD{singleLocalReservationCC},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{ccPod}),
						withMachineFamily(machinetypes.N2),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(singleLocalReservationCC),
						withNodeConfigRule(n2LocalReservationRule),
						withReservationName("test-reservation"),
						withReservationProject("12345"),
						withReservationExists(),
						withSystemLabels(systemLabels),
						withReservationAffinity(reservations.SpecificAffinity),
					),
				},
			},
		},
		"pod with non-default cc selector, cc with any reservation": {
			pods: []*apiv1.Pod{ccPod},
			ccs:  []computeclass.CRD{singleAnyReservationCC},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{ccPod}),
						withMachineFamily(machinetypes.N2),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(singleAnyReservationCC),
						withNodeConfigRule(n2AnyReservationRule),
						withReservationAffinity(reservations.AnyAffinity),
						withSystemLabels(systemLabels),
						withReservationExists(),
					),
				},
			},
		},
		"linux node config is passed correctly": {
			pods: []*apiv1.Pod{ccPod},
			ccs:  []computeclass.CRD{ccWithLinuxNodeConfig},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{ccPod}),
						withMachineFamily(machinetypes.N2),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(ccWithLinuxNodeConfig),
						withNodeConfigRule(linuxNodeConfigRule),
						withSystemLabels(systemLabels),
						withLinuxNodeConfig(&gkeclient.LinuxNodeConfig{
							Sysctls: sysctls,
							Hugepages: &gkeclient.HugepagesConfig{
								HugepageSize1g: hugepageSize1g,
								HugepageSize2m: hugepageSize2m,
							},
						}),
					),
				},
			},
		},
		"kubelet config is passed correctly": {
			pods: []*apiv1.Pod{ccPod},
			ccs:  []computeclass.CRD{ccWithKubeletConfig},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{ccPod}),
						withMachineFamily(machinetypes.N2),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(ccWithKubeletConfig),
						withNodeConfigRule(kubeletConfigRule),
						withSystemLabels(systemLabels),
						withKubeletConfig(&gke_api_beta.NodeKubeletConfig{
							CpuCfsQuota:       cpuCfsQuota,
							CpuCfsQuotaPeriod: cpuCfsQuotaPeriod,
							CpuManagerPolicy:  cpuManagerPolicy,
							PodPidsLimit:      podPidsLimit,
						}),
					),
				},
			},
		},
		"cc with specific reservations with zones": {
			pods: []*apiv1.Pod{ccPod},
			ccs:  []computeclass.CRD{ccWithSpecificReservationZones},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{ccPod}),
						withMachineFamily(machinetypes.N1),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(ccWithSpecificReservationZones),
						withNodeConfigRule(n1ReservationsWithZonesRule),
						withReservationName("reservation"),
						withReservationProject("12345"),
						withReservationExists(),
						withSystemLabels(systemLabels),
						withReservationAffinity(reservations.SpecificAffinity),
						withSpecifiedZones([]string{"zone-1"}),
					),
					newTestNodeGroupRequirements(withPods([]*apiv1.Pod{ccPod}),
						withMachineFamily(machinetypes.N1),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(ccWithSpecificReservationZones),
						withNodeConfigRule(n1ReservationsWithZonesRule),
						withReservationName("reservation"),
						withReservationExists(),
						withReservationAffinity(reservations.SpecificAffinity),
						withReservationProject("other"),
						withSystemLabels(systemLabels),
						withReservationBlock("res-block"),
						withSpecifiedZones([]string{"zone-1", "zone-2"}),
					),
				},
			},
		},
		"cc with specified zones": {
			pods: []*apiv1.Pod{ccPod},
			ccs:  []computeclass.CRD{ccWithSpecifiedZones},
			wantRequirements: [][]nodeGroupRequirements{
				{
					newTestNodeGroupRequirements(
						withPods([]*apiv1.Pod{ccPod}),
						withMachineFamily(machinetypes.N1),
						withMachineSelectionType(machinetypes.SelectionTypeSpecified),
						withComputeClass(ccWithSpecifiedZones),
						withNodeConfigRule(specifiedZonesRule),
						withSystemLabels(systemLabels),
						withSpecifiedZones([]string{"zone-1", "zone-2"}),
					),
				},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			lister := computeclass_lister.NewMockCrdListerWithCCCDefaulting(tc.ccs, tc.useComputeClassDefaulting)
			lister.SetCrdLabel(ccLabel)
			if tc.defaultCC {
				lister.SetDefaultCrdName(defaultCCName)
			}
			em := experiments.NewMockManager()
			manager := NewAutoprovisioningNodeGroupManager(AutoprovisioningNodeGroupManagerOptions{
				CloudProvider: provider,
				Lister:        lister,
				Flags: AutoprovisioningNodeGroupManagerFlags{
					TpuAutoprovisioningEnabled: true,
					ReservationFlags: ReservationFlags{
						SpecificTypeReservationsEnabled: true,
					},
				},
				ExperimentsManager:   em,
				OptionsTracker:       tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
				ReservationsPuller:   reservations.NewTestingReservationsPuller("12345", nil, nil),
				ResourcePolicyPuller: &placement.FakeResourcePolicyPullerProvider{},
			})

			requirements, err := manager.computePossibleRequirements(tc.pods, tc.gpuRequest, TpuRequest{})
			if diff := cmp.Diff(tc.wantRequirements, requirements, compareAllUnexportedOpt, requirementsSliceIgnoreOrderOpt); diff != "" {
				t.Error(diff)
			}
			if tc.wantErr == nil {
				assert.Empty(t, err)
			} else {
				assert.Equal(t, tc.wantErr, err)
			}
		})
	}
}

func TestResourceLabelsGeneratorFromPodToSpec(t *testing.T) {
	tests := map[string]struct {
		podRequirements     *podrequirements.Requirements
		expectedReqsError   errors.AutoscalerError
		expectedParamsError errors.AutoscalerError
		expectedSpecError   errors.AutoscalerError
		wantNodePoolSpec    *gkeclient.NodePoolSpec
	}{
		"extracts pod resource labels": {
			podRequirements: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					"cloud.google.com/resourcelabel_annotation": podrequirements.NewValues("resourcekey:resourcevalue"),
				}),
			},
			wantNodePoolSpec: &gkeclient.NodePoolSpec{
				ResourceLabels: map[string]string{"resourcekey": "resourcevalue"},
				Labels:         map[string]string{"cloud.google.com/resourcelabel_annotation": ""},
			},
		},
		"malformed resource label values object": {
			podRequirements: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					"cloud.google.com/resourcelabel_annotation": podrequirements.NewValues("one", "two"),
				}),
			},
			expectedReqsError: errors.NewAutoscalerError(errors.ConfigurationError, "malformed resource label 'cloud.google.com/resourcelabel_annotation': too many values provided while 1 expected"),
		},
		"malformed resource label value format": {
			podRequirements: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					"cloud.google.com/resourcelabel_annotation": podrequirements.NewValues("label"),
				}),
			},
			expectedSpecError: errors.NewAutoscalerError(errors.ConfigurationError, "malformed resource label 'cloud.google.com/resourcelabel_annotation': invalid annotation value format, should be <key:value>, while 'label' found"),
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			generator := NewResourceLabelsGenerator()

			gotRequirements := nodeGroupRequirements{}
			typedErr := generator.UpdateRequirements(&gotRequirements, test.podRequirements, machinetypes.GpuRequest{}, TpuRequest{})
			if test.expectedReqsError != nil {
				assert.ErrorIs(t, typedErr, test.expectedReqsError)
				return
			} else {
				assert.NoError(t, typedErr)
			}

			gotParams := &nodeGroupParameters{systemLabels: make(map[string]string)}
			err := generator.UpdateParameters(gotParams, gotRequirements, NodeGroupOptions{})
			if test.expectedParamsError != nil {
				assert.ErrorIs(t, err, test.expectedParamsError)
				return
			} else {
				assert.NoError(t, err)
			}

			gotNodePoolSpec := &gkeclient.NodePoolSpec{Labels: make(map[string]string), ResourceLabels: map[string]string{}}
			err = generator.UpdateNodePoolSpec(gotNodePoolSpec, gotParams.systemLabels, gotParams.extraResources)
			if test.expectedSpecError != nil {
				assert.ErrorIs(t, err, test.expectedSpecError)
				return
			} else {
				assert.NoError(t, err)
			}
			if diff := cmp.Diff(gotNodePoolSpec, test.wantNodePoolSpec); diff != "" {
				t.Errorf("Node pool spec differ: %s", diff)
			}
		})
	}
}

func TestNewLocalSSDCountGenerator_UpdateNodePoolSpec(t *testing.T) {
	type testcase struct {
		name         string
		labels       map[string]string
		expectError  error
		expectConfig *gkeclient.LocalSSDConfig
		expectTaints []apiv1.Taint
		expectLabels map[string]string
	}
	tcs := []testcase{
		{
			name:         "Explicitly requested LocalSSD",
			labels:       map[string]string{labelEphemeralLocalSsdDisksCount: "2", gkelabels.EphemeralLocalSsdLabel: gkelabels.EphemeralLocalSsdEnabledValue},
			expectError:  nil,
			expectConfig: &gkeclient.LocalSSDConfig{EphemeralStorageConfig: &gke_api_beta.EphemeralStorageConfig{LocalSsdCount: 2}},
			expectTaints: []apiv1.Taint{{Effect: apiv1.TaintEffectNoSchedule, Key: gkelabels.EphemeralLocalSsdLabel, Value: gkelabels.EphemeralLocalSsdEnabledValue}},
			expectLabels: map[string]string{gkelabels.EphemeralLocalSsdLabel: gkelabels.EphemeralLocalSsdEnabledValue},
		},
		{
			name:         "Automatically assigned LocalSSD",
			labels:       map[string]string{labelEphemeralLocalSsdDisksCount: "999"},
			expectError:  nil,
			expectConfig: &gkeclient.LocalSSDConfig{EphemeralStorageConfig: &gke_api_beta.EphemeralStorageConfig{LocalSsdCount: 999}},
			expectTaints: nil,
			expectLabels: map[string]string{gkelabels.EphemeralLocalSsdLabel: gkelabels.EphemeralLocalSsdEnabledValue},
		},
		{
			name:         "Invalid local ssd count set internally",
			labels:       map[string]string{labelEphemeralLocalSsdDisksCount: "foobar", gkelabels.EphemeralLocalSsdLabel: gkelabels.EphemeralLocalSsdEnabledValue},
			expectError:  errors.NewAutoscalerError(errors.InternalError, "invalid Local SSD count passed: foobar"),
			expectConfig: nil,
			expectTaints: nil,
		},
		{
			name:         "No taints and labels added for local ssd count when Compute Class is enabled",
			labels:       map[string]string{labelEphemeralLocalSsdDisksCount: "2", labelComputeClassRequired: "true"},
			expectError:  nil,
			expectConfig: &gkeclient.LocalSSDConfig{EphemeralStorageConfig: &gke_api_beta.EphemeralStorageConfig{LocalSsdCount: 2}},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithMachineTypes("g2-standard-2").WithAutopilotEnabled(true).Build()
			generator := NewLocalSSDConfigGenerator(provider)
			spec := &gkeclient.NodePoolSpec{}

			err := generator.UpdateNodePoolSpec(spec, tc.labels, nil)
			if err != tc.expectError {
				t.Errorf("retrieved error differs from expected, want %v, got %v", tc.expectError, err)
			}
			if tc.expectError == nil {
				if got, want := spec.LocalSSDConfig, tc.expectConfig; cmp.Diff(got, want) != "" {
					t.Errorf("localSSDConfig differs: %s", cmp.Diff(got, want))
				}
				if got, want := spec.Taints, tc.expectTaints; cmp.Diff(got, want) != "" {
					t.Errorf("taints differ: %s", cmp.Diff(got, want))
				}
				if got, want := spec.Labels, tc.expectLabels; cmp.Diff(got, want) != "" {
					t.Errorf("labels differ: %s", cmp.Diff(got, want))
				}
			}
		})
	}
}

func TestBootDiskConfigGenerator_UpdateNodePoolSpec(t *testing.T) {
	type testcase struct {
		name                    string
		labels                  map[string]string
		expectError             error
		expectDiskSize          int64
		expectDiskType          string
		expectDiskEncryptionKey string
		expectLabels            map[string]string
	}
	tcs := []testcase{
		{
			name: "No boot disk type or size",
		},
		{
			name:        "LabelZoneFailureDomain label not exists",
			labels:      map[string]string{gkelabels.BootDiskTypeLabelKey: "hello-world"},
			expectError: fmt.Errorf("LabelZoneFailureDomain label not found"),
		},
		{
			name:           "Only boot disk type exists",
			labels:         map[string]string{apiv1.LabelZoneFailureDomain: "my-zone", gkelabels.BootDiskTypeLabelKey: "hello-world"},
			expectError:    nil,
			expectDiskType: "hello-world",
			expectLabels:   map[string]string{gkelabels.BootDiskTypeLabelKey: "hello-world"},
		},
		{
			name:           "Only boot disk size exists",
			labels:         map[string]string{apiv1.LabelZoneFailureDomain: "my-zone", gkelabels.BootDiskSizeLabelKey: "100"},
			expectError:    nil,
			expectDiskSize: 100,
			expectLabels:   map[string]string{gkelabels.BootDiskSizeLabelKey: "100"},
		},
		{
			name:                    "Boot disk encryption with annotation exists",
			labels:                  map[string]string{apiv1.LabelZoneFailureDomain: "my-zone", gkelabels.BootDiskEncryptionLabelKey: "100", gkelabels.BootDiskEncryptionAnnotationKey: "encryption-key"},
			expectError:             nil,
			expectDiskEncryptionKey: "encryption-key",
			expectLabels:            map[string]string{gkelabels.BootDiskEncryptionLabelKey: "100"},
		},
		{
			name:                    "Only boot disk encryption exists",
			labels:                  map[string]string{apiv1.LabelZoneFailureDomain: "my-zone", gkelabels.BootDiskEncryptionLabelKey: "encryption-key"},
			expectError:             nil,
			expectDiskEncryptionKey: "encryption-key",
		},
		{
			name: "Both boot disk type, size and encryption key exist",
			labels: map[string]string{
				apiv1.LabelZoneFailureDomain:              "my-zone",
				gkelabels.BootDiskTypeLabelKey:            "hello-world",
				gkelabels.BootDiskSizeLabelKey:            "100",
				gkelabels.BootDiskEncryptionLabelKey:      "annotation-key",
				gkelabels.BootDiskEncryptionAnnotationKey: "encryption-key",
			},
			expectError:             nil,
			expectDiskSize:          100,
			expectDiskType:          "hello-world",
			expectDiskEncryptionKey: "encryption-key",
			expectLabels:            map[string]string{gkelabels.BootDiskTypeLabelKey: "hello-world", gkelabels.BootDiskSizeLabelKey: "100", gkelabels.BootDiskEncryptionLabelKey: "annotation-key"},
		},
		{
			name: "Both boot disk type and size exist, inferred from nodeconfig rule",
			labels: map[string]string{
				apiv1.LabelZoneFailureDomain:         "my-zone",
				labelComputeClassRequired:            "true",
				gkelabels.BootDiskTypeLabelKey:       "hello-world",
				gkelabels.BootDiskSizeLabelKey:       "100",
				gkelabels.BootDiskEncryptionLabelKey: "encryption-key",
			},
			expectError:             nil,
			expectDiskSize:          100,
			expectDiskType:          "hello-world",
			expectDiskEncryptionKey: "encryption-key",
			expectLabels:            map[string]string{gkelabels.BootDiskTypeLabelKey: "hello-world", gkelabels.BootDiskSizeLabelKey: "100"},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithMachineTypes("g2-standard-2").WithAutopilotEnabled(true).Build()
			generator := NewBootDiskConfigGenerator(provider)
			spec := &gkeclient.NodePoolSpec{}

			err := generator.UpdateNodePoolSpec(spec, tc.labels, nil)
			if err != nil {
				assert.EqualError(t, err, tc.expectError.Error())
			}
			if tc.expectError == nil {
				if got, want := spec.DiskSize, tc.expectDiskSize; got != want {
					t.Errorf("disk size differs, want %v, got %v", want, got)
				}
				if got, want := spec.DiskType, tc.expectDiskType; cmp.Diff(got, want) != "" {
					t.Errorf("disk type differs: %s", cmp.Diff(want, got))
				}
				if got, want := spec.Labels, tc.expectLabels; cmp.Diff(got, want) != "" {
					t.Errorf("labels differ: %s", cmp.Diff(want, got))
				}
			}
		})
	}
}

func TestBootDiskEncryptionKeyIntegration_UpdateNodePoolSpec(t *testing.T) {
	ccEncryptionKey := "cc-key"
	ccRules := []rules.Rule{rules.NewRule(rules.WithStorageRule(nil, nil, &ccEncryptionKey, nil))}
	ccObj := computeclass.NewTestCrd(computeclass.WithName("cc"), computeclass.WithLabel("cc-label"), computeclass.WithRules(ccRules))
	ccTaint := apiv1.Taint{Key: ccObj.Label(), Value: ccObj.Name(), Effect: "NoSchedule"}
	lister := computeclass_lister.NewMockCrdLister([]computeclass.CRD{ccObj})
	lister.SetCrdLabel(ccObj.Label())

	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithMachineTypes("e2-standard-2").Build()
	bootDiskGenerator := NewBootDiskConfigGenerator(provider)
	computeClassGenerator := NewComputeClassGenerator(provider, lister, true)

	selectorEncryptionKey := "nodeselector-key"
	selectorValue := "annotationKey"

	tests := map[string]struct {
		systemLabels map[string]string
		wantSpec     *gkeclient.NodePoolSpec
	}{
		"EncryptionKeyFromCrd": {
			systemLabels: map[string]string{
				ccObj.Label():                        ccObj.Name(),
				gkelabels.BootDiskEncryptionLabelKey: ccEncryptionKey,
			},
			wantSpec: &gkeclient.NodePoolSpec{
				DiskEncryptionKey: ccEncryptionKey,
				Taints:            []apiv1.Taint{ccTaint},
				Labels: map[string]string{
					ccObj.Label(): ccObj.Name(),
				},
			},
		},
		"EncryptionKeyFromNodeSelector": {
			systemLabels: map[string]string{
				gkelabels.BootDiskEncryptionLabelKey:      selectorValue,
				gkelabels.BootDiskEncryptionAnnotationKey: selectorEncryptionKey,
			},
			wantSpec: &gkeclient.NodePoolSpec{
				DiskEncryptionKey: selectorEncryptionKey,
				Labels: map[string]string{
					gkelabels.BootDiskEncryptionLabelKey: selectorValue,
				},
			},
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			spec := &gkeclient.NodePoolSpec{Labels: map[string]string{}}
			assert.NoError(t, bootDiskGenerator.UpdateNodePoolSpec(spec, test.systemLabels, nil))
			assert.NoError(t, computeClassGenerator.UpdateNodePoolSpec(spec, test.systemLabels, nil))
			assert.Equal(t, test.wantSpec, spec)

			spec = &gkeclient.NodePoolSpec{Labels: map[string]string{}}
			assert.NoError(t, computeClassGenerator.UpdateNodePoolSpec(spec, test.systemLabels, nil))
			assert.NoError(t, bootDiskGenerator.UpdateNodePoolSpec(spec, test.systemLabels, nil))
			assert.Equal(t, test.wantSpec, spec)
		})
	}
}

func TestSecondaryBootDisksIntegration_UpdateNodePoolSpecWithRequirements(t *testing.T) {
	secondaryBootDisk1 := gke_api_beta.SecondaryBootDisk{
		DiskImage: "image1",
		Mode:      "CONTAINER_IMAGE_CACHE",
	}
	secondaryBootDisk2 := gke_api_beta.SecondaryBootDisk{
		DiskImage: "image2",
		Mode:      "CONTAINER_IMAGE_CACHE",
	}
	secondaryBootDisk3 := gke_api_beta.SecondaryBootDisk{
		DiskImage: "image3",
		Mode:      "MODE_UNSPECIFIED",
	}
	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithMachineTypes("e2-standard-2").Build()
	generator := NewBootDiskConfigGenerator(provider)
	tests := map[string]struct {
		requirementsDisks []*gke_api_beta.SecondaryBootDisk
	}{
		"Empty requirements": {},
		"One secondary boot disk": {
			requirementsDisks: []*gke_api_beta.SecondaryBootDisk{
				&secondaryBootDisk1,
			},
		},
		"Two secondary boot disks": {
			requirementsDisks: []*gke_api_beta.SecondaryBootDisk{
				&secondaryBootDisk1,
				&secondaryBootDisk2,
			},
		},
		"Three secondary boot disks": {
			requirementsDisks: []*gke_api_beta.SecondaryBootDisk{
				&secondaryBootDisk1,
				&secondaryBootDisk2,
				&secondaryBootDisk3,
			},
		},
	}
	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			params := &nodeGroupParameters{
				systemLabels: make(map[string]string),
			}
			ngReq := nodeGroupRequirements{
				secondaryBootDisks: test.requirementsDisks,
			}
			err := generator.UpdateParameters(params, ngReq, NodeGroupOptions{})
			assert.NoError(t, err)

			spec := &gkeclient.NodePoolSpec{
				Labels: map[string]string{},
			}
			err2 := generator.UpdateNodePoolSpec(spec, params.systemLabels, nil)
			assert.NoError(t, err2)

			difference := cmp.Diff(spec.SecondaryBootDisks, test.requirementsDisks)
			if difference != "" {
				t.Errorf("Secondary boot disks differ: %s", difference)
			}
		})
	}
}

// TestReservationGenerator_matchReservationBlock test reservation block validation
func TestReservationGenerator_matchReservationBlock(t *testing.T) {
	projectID := "project"
	rsv1 := reservations.BuildReservationWithLink("zone-A", "fake-machine-type", projectID, "rsv1")
	rsv1Key := gceclient.GetReservationRefFromReservation(*rsv1)
	testCases := []struct {
		name              string
		reservations      []*gce_api.Reservation
		reservationBlocks map[gceclient.ReservationRef][]*gceclient.GceReservationBlock
		req               *reservationRequirements
		wantError         bool
		expectedError     error
	}{
		{
			name: "reservation name not specified",
			reservations: []*gce_api.Reservation{
				rsv1,
			},
			reservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				rsv1Key: {reservations.BuildSingleReservationBlock("rb1", 3, 1, "zone-A")},
			},
			req: &reservationRequirements{
				name:    "",
				project: projectID,
				zone:    "zone-A",
				block:   "rb1",
			},
			wantError: true,
			expectedError: reservations.NewErrUnusableReservation(
				gceclient.ReservationRef{Name: "", Project: projectID, BlockName: "rb1"},
				"Specifying reservation block without reservation name"),
		},
		{
			name: "block exists",
			reservations: []*gce_api.Reservation{
				rsv1,
			},
			reservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				rsv1Key: {reservations.BuildSingleReservationBlock("rb1", 3, 1, "zone-A")},
			},
			req: &reservationRequirements{
				name:    "rsv1",
				project: projectID,
				zone:    "zone-A",
				block:   "rb1",
			},
			wantError: false,
		},
		{
			name: "block does not exist",
			reservations: []*gce_api.Reservation{
				rsv1,
			},
			reservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				rsv1Key: {reservations.BuildSingleReservationBlock("rb2", 3, 1, "zone-A")},
			},
			req: &reservationRequirements{
				name:    "rsv1",
				project: projectID,
				zone:    "zone-A",
				block:   "rb1",
			},
			wantError: true,
			expectedError: reservations.NewErrUnusableReservation(
				gceclient.ReservationRef{Name: "rsv1", Project: projectID, BlockName: "rb1"},
				"Reservation block 'rb1' not found"),
		},
		{
			name: "sub-block exists",
			reservations: []*gce_api.Reservation{
				rsv1,
			},
			reservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				rsv1Key: {
					{
						Name:       "rb1",
						Count:      3,
						InUseCount: 1,
						Status:     "READY",
						Zone:       "zone-A",
						SubBlocks: []*gceclient.GceReservationSubBlock{
							{Name: "rsb1", Count: 2, Status: "READY"},
						},
					},
				},
			},
			req: &reservationRequirements{
				name:          "rsv1",
				project:       projectID,
				zone:          "zone-A",
				block:         "rb1",
				subBlock:      "rsb1",
				subBlockCount: 2,
			},
			wantError: false,
		},
		{
			name: "sub-block does not exist",
			reservations: []*gce_api.Reservation{
				rsv1,
			},
			reservationBlocks: map[gceclient.ReservationRef][]*gceclient.GceReservationBlock{
				rsv1Key: {
					{
						Name:       "rb1",
						Count:      3,
						InUseCount: 1,
						Status:     "READY",
						Zone:       "zone-A",
						SubBlocks: []*gceclient.GceReservationSubBlock{
							{Name: "rsb1", Count: 2, Status: "READY"},
						},
					},
				},
			},
			req: &reservationRequirements{
				name:     "rsv1",
				project:  projectID,
				zone:     "zone-A",
				block:    "rb1",
				subBlock: "rsb2",
			},
			wantError: true,
			expectedError: reservations.NewErrUnusableReservation(
				gceclient.ReservationRef{Name: "rsv1", Project: projectID, BlockName: "rb1", SubBlockName: "rsb2"},
				"reservation sub-block 'rsb2' not found"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reservationsPuller := reservations.NewTestingReservationsPuller(projectID, nil, tc.reservations)
			blocksPuller := reservations.NewBlocksPuller(reservations.NewFakeBlocksPullerProvider(tc.reservationBlocks, nil), reservationsPuller)
			blocksPuller.Loop()

			rg := ReservationGenerator{
				reservationsPuller:      reservationsPuller,
				reservationBlocksPuller: blocksPuller,
				experimentsManager:      experiments.NewMockManager(experiments.ReservationSubblocksTargetingEnabledFlag),
			}
			err := rg.matchReservationBlock(tc.req)

			if tc.wantError {
				assert.Error(t, err)
				assert.Equal(t, tc.expectedError, err)
			} else {
				assert.NoError(t, err)
				// length of reservation blocks will be 0 or 1
				for key := range tc.reservationBlocks {
					expectedBlockCount := tc.reservationBlocks[key][0].Count
					assert.Equal(t, expectedBlockCount, tc.req.blockCount)
				}
			}
		})
	}
}

func TestUpdateNodePoolSpecWithReservation(t *testing.T) {
	testCases := []struct {
		name                                  string
		systemLabels                          map[string]string
		expectedSpec                          *gkeclient.NodePoolSpec
		subBlocksEnabled                      bool
		reservationsAnyLocationPolicyOverride bool
	}{
		{
			name: "reservation block specified",
			systemLabels: map[string]string{
				gkelabels.ReservationNameLabel:        "rsv1",
				gkelabels.ReservationProjectLabel:     "prj",
				gkelabels.ReservationAffinityLabel:    "specific",
				gkelabels.ReservationBlocksLabel:      "rb1",
				gkelabels.ReservationBlocksCountLabel: "2",
			},
			expectedSpec: &gkeclient.NodePoolSpec{
				ReservationAffinity: &gke_api_beta.ReservationAffinity{
					ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
					Key:                    gkeclient.ReservationNameKey,
					Values:                 []string{"projects/prj/reservations/rsv1/reservationBlocks/rb1"},
				},
				Labels: map[string]string{
					gkelabels.ReservationNameLabel:     "rsv1",
					gkelabels.ReservationProjectLabel:  "prj",
					gkelabels.ReservationAffinityLabel: "specific",
					gkelabels.ReservationBlocksLabel:   "rb1",
				},
				ReservationBlockCount: 2,
			},
		},
		{
			name: "reservation block unspecified",
			systemLabels: map[string]string{
				gkelabels.ReservationNameLabel:     "rsv1",
				gkelabels.ReservationProjectLabel:  "prj",
				gkelabels.ReservationAffinityLabel: "specific",
			},
			expectedSpec: &gkeclient.NodePoolSpec{
				ReservationAffinity: &gke_api_beta.ReservationAffinity{
					ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
					Key:                    gkeclient.ReservationNameKey,
					Values:                 []string{"projects/prj/reservations/rsv1"},
				},
				Labels: map[string]string{
					gkelabels.ReservationNameLabel:     "rsv1",
					gkelabels.ReservationProjectLabel:  "prj",
					gkelabels.ReservationAffinityLabel: "specific",
				},
				ReservationBlockCount: 0,
			},
		},
		{
			name: "queued provisioned",
			systemLabels: map[string]string{
				gkelabels.ProvisioningRequestLabelKey: "provisioned",
			},
			expectedSpec: &gkeclient.NodePoolSpec{
				ReservationAffinity: &gke_api_beta.ReservationAffinity{
					ConsumeReservationType: gkeclient.ReservationAffinityNone,
				},
				Labels: map[string]string{},
			},
		},
		{
			name: "reservation affinity any with obsolete reservation specific labels",
			systemLabels: map[string]string{
				gkelabels.ReservationNameLabel:     "rsv1",
				gkelabels.ReservationProjectLabel:  "prj",
				gkelabels.ReservationAffinityLabel: "any",
			},
			expectedSpec: &gkeclient.NodePoolSpec{
				ReservationAffinity: &gke_api_beta.ReservationAffinity{
					ConsumeReservationType: gkeclient.ReservationAffinityAny,
				},
				Labels: map[string]string{
					gkelabels.ReservationAffinityLabel: "any",
				},
			},
		},
		{
			name: "reservation affinity any",
			systemLabels: map[string]string{
				gkelabels.ReservationAffinityLabel: "any",
			},
			expectedSpec: &gkeclient.NodePoolSpec{
				ReservationAffinity: &gke_api_beta.ReservationAffinity{
					ConsumeReservationType: gkeclient.ReservationAffinityAny,
				},
				Labels: map[string]string{
					gkelabels.ReservationAffinityLabel: "any",
				},
			},
		},
		{
			name: "reservation affinity any with ANY location policy",
			systemLabels: map[string]string{
				gkelabels.ReservationAffinityLabel: "any",
			},
			expectedSpec: &gkeclient.NodePoolSpec{
				ReservationAffinity: &gke_api_beta.ReservationAffinity{
					ConsumeReservationType: gkeclient.ReservationAffinityAny,
				},
				Labels: map[string]string{
					gkelabels.ReservationAffinityLabel: "any",
				},
				LocationPolicy: "ANY",
			},
			reservationsAnyLocationPolicyOverride: true,
		},
		{
			name: "reservation affinity specific",
			systemLabels: map[string]string{
				gkelabels.ReservationNameLabel:     "rsv1",
				gkelabels.ReservationProjectLabel:  "prj",
				gkelabels.ReservationAffinityLabel: "specific",
			},
			expectedSpec: &gkeclient.NodePoolSpec{
				ReservationAffinity: &gke_api_beta.ReservationAffinity{
					ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
					Key:                    gkeclient.ReservationNameKey,
					Values:                 []string{"projects/prj/reservations/rsv1"},
				},
				Labels: map[string]string{
					gkelabels.ReservationNameLabel:     "rsv1",
					gkelabels.ReservationProjectLabel:  "prj",
					gkelabels.ReservationAffinityLabel: "specific",
				},
			},
		},
		{
			name: "reservation affinity specific with ANY location policy",
			systemLabels: map[string]string{
				gkelabels.ReservationNameLabel:     "rsv1",
				gkelabels.ReservationProjectLabel:  "prj",
				gkelabels.ReservationAffinityLabel: "specific",
			},
			expectedSpec: &gkeclient.NodePoolSpec{
				ReservationAffinity: &gke_api_beta.ReservationAffinity{
					ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
					Key:                    gkeclient.ReservationNameKey,
					Values:                 []string{"projects/prj/reservations/rsv1"},
				},
				Labels: map[string]string{
					gkelabels.ReservationNameLabel:     "rsv1",
					gkelabels.ReservationProjectLabel:  "prj",
					gkelabels.ReservationAffinityLabel: "specific",
				},
				LocationPolicy: "ANY",
			},
			reservationsAnyLocationPolicyOverride: true,
		},
		{
			name: "reservation block specified, error during string conv",
			systemLabels: map[string]string{
				gkelabels.ReservationNameLabel:        "rsv1",
				gkelabels.ReservationProjectLabel:     "prj",
				gkelabels.ReservationAffinityLabel:    "specific",
				gkelabels.ReservationBlocksLabel:      "rb1",
				gkelabels.ReservationBlocksCountLabel: "/2",
			},
			expectedSpec: &gkeclient.NodePoolSpec{
				ReservationAffinity: &gke_api_beta.ReservationAffinity{
					ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
					Key:                    gkeclient.ReservationNameKey,
					Values:                 []string{"projects/prj/reservations/rsv1/reservationBlocks/rb1"},
				},
				Labels: map[string]string{
					gkelabels.ReservationNameLabel:     "rsv1",
					gkelabels.ReservationProjectLabel:  "prj",
					gkelabels.ReservationAffinityLabel: "specific",
					gkelabels.ReservationBlocksLabel:   "rb1",
				},
				ReservationBlockCount: 0,
			},
		},
		{
			name: "reservation subBlock specified, subBlocks disabled",
			systemLabels: map[string]string{
				gkelabels.ReservationNameLabel:      "rsv1",
				gkelabels.ReservationProjectLabel:   "prj",
				gkelabels.ReservationAffinityLabel:  "specific",
				gkelabels.ReservationBlocksLabel:    "rb1",
				gkelabels.ReservationSubBlocksLabel: "rsb1",
			},
			expectedSpec: &gkeclient.NodePoolSpec{
				ReservationAffinity: &gke_api_beta.ReservationAffinity{
					ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
					Key:                    gkeclient.ReservationNameKey,
					Values:                 []string{"projects/prj/reservations/rsv1/reservationBlocks/rb1"},
				},
				Labels: map[string]string{
					gkelabels.ReservationNameLabel:     "rsv1",
					gkelabels.ReservationProjectLabel:  "prj",
					gkelabels.ReservationAffinityLabel: "specific",
					gkelabels.ReservationBlocksLabel:   "rb1",
				},
				ReservationBlockCount: 0,
			},
		},
		{
			name:             "reservation subBlock specified, subBlocks enabled",
			subBlocksEnabled: true,
			systemLabels: map[string]string{
				gkelabels.ReservationNameLabel:           "rsv1",
				gkelabels.ReservationProjectLabel:        "prj",
				gkelabels.ReservationAffinityLabel:       "specific",
				gkelabels.ReservationBlocksLabel:         "rb1",
				gkelabels.ReservationSubBlocksLabel:      "rsb1",
				gkelabels.ReservationSubBlocksCountLabel: "3",
			},
			expectedSpec: &gkeclient.NodePoolSpec{
				ReservationAffinity: &gke_api_beta.ReservationAffinity{
					ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
					Key:                    gkeclient.ReservationNameKey,
					Values:                 []string{"projects/prj/reservations/rsv1/reservationBlocks/rb1/reservationSubBlocks/rsb1"},
				},
				Labels: map[string]string{
					gkelabels.ReservationNameLabel:      "rsv1",
					gkelabels.ReservationProjectLabel:   "prj",
					gkelabels.ReservationAffinityLabel:  "specific",
					gkelabels.ReservationBlocksLabel:    "rb1",
					gkelabels.ReservationSubBlocksLabel: "rsb1",
				},
				ReservationBlockCount:    0,
				ReservationSubBlockCount: 3,
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			spec := &gkeclient.NodePoolSpec{Labels: make(map[string]string)}
			var em experiments.Manager
			if tc.subBlocksEnabled {
				em = experiments.NewMockManager(experiments.ReservationSubblocksTargetingEnabledFlag)
			} else {
				em = experiments.NewMockManager()
			}
			rg := &ReservationGenerator{
				experimentsManager:                    em,
				reservationsAnyLocationPolicyOverride: tc.reservationsAnyLocationPolicyOverride,
			}
			err := rg.UpdateNodePoolSpec(spec, tc.systemLabels, nil)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedSpec, spec)
		})
	}
}

func TestAcceleratorSliceGenerator_UpdateNodePoolSpec(t *testing.T) {
	existingSettings := &gke_api_beta.UpgradeSettings{
		Strategy:       "BLUE_GREEN",
		MaxSurge:       5,
		MaxUnavailable: 5,
	}

	tests := []struct {
		name             string
		spec             *gkeclient.NodePoolSpec
		wantSpec         *gkeclient.NodePoolSpec
		wantErr          bool
		expectedErrorMsg string
	}{
		{
			name: "machine_type_not_found",
			spec: &gkeclient.NodePoolSpec{
				MachineType: "invalid-machine-type",
			},
			wantErr:          true,
			expectedErrorMsg: "failed to get machine family from machine type invalid-machine-type, this should never happen at this point",
			wantSpec: &gkeclient.NodePoolSpec{
				MachineType: "invalid-machine-type",
			},
		},
		{
			name: "no_support_no_changes",
			spec: &gkeclient.NodePoolSpec{
				MachineType: "e2-standard-2",
			},
			wantSpec: &gkeclient.NodePoolSpec{
				MachineType: "e2-standard-2",
			},
		},
		{
			name: "no_suppport_upgrade_settings_not_applied",
			spec: &gkeclient.NodePoolSpec{
				MachineType: "e2-standard-2",
				PlacementGroup: placement.Spec{
					Policy:         "some-policy",
					ResourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "1x72"}},
				},
			},
			wantSpec: &gkeclient.NodePoolSpec{
				MachineType: "e2-standard-2",
				PlacementGroup: placement.Spec{
					Policy:         "some-policy",
					ResourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "1x72"}},
				},
				UpgradeSettings: nil,
			},
		},
		{
			name: "flex_start_upgrade_settings_not_applied",
			spec: &gkeclient.NodePoolSpec{
				MachineType: "a4x-highgpu-4g",
				FlexStart:   true,
				PlacementGroup: placement.Spec{
					Policy:         "some-policy",
					ResourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "1x72"}},
				},
			},
			wantSpec: &gkeclient.NodePoolSpec{
				MachineType: "a4x-highgpu-4g",
				FlexStart:   true,
				PlacementGroup: placement.Spec{
					Policy:         "some-policy",
					ResourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "1x72"}},
				},
				UpgradeSettings: nil,
			},
		},
		{
			name: "queued_provisioining_upgrade_settings_not_applied",
			spec: &gkeclient.NodePoolSpec{
				MachineType:        "a4x-highgpu-4g",
				QueuedProvisioning: true,
				PlacementGroup: placement.Spec{
					Policy:         "some-policy",
					ResourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "1x72"}},
				},
			},
			wantSpec: &gkeclient.NodePoolSpec{
				MachineType:        "a4x-highgpu-4g",
				QueuedProvisioning: true,
				PlacementGroup: placement.Spec{
					Policy:         "some-policy",
					ResourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "1x72"}},
				},
				UpgradeSettings: nil,
			},
		},
		{
			name: "unsupported_compact_placement_upgrade_settings_not_applied",
			spec: &gkeclient.NodePoolSpec{
				MachineType: "a4x-highgpu-4g",
				PlacementGroup: placement.Spec{
					Policy: "some-policy",
					// ResourcePolicy missing topology
					ResourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{}},
				},
			},
			wantSpec: &gkeclient.NodePoolSpec{
				MachineType: "a4x-highgpu-4g",
				PlacementGroup: placement.Spec{
					Policy:         "some-policy",
					ResourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{}},
				},
				UpgradeSettings: nil,
			},
		},
		{
			name: "unsupported_group_placement_upgrade_settings_not_applied",
			spec: &gkeclient.NodePoolSpec{
				MachineType: "a4x-highgpu-4g",
				PlacementGroup: placement.Spec{
					GroupId: "compact-group", // SupportsMachineFamily returns false for A4X if only GroupId is set
				},
			},
			wantSpec: &gkeclient.NodePoolSpec{
				MachineType: "a4x-highgpu-4g",
				PlacementGroup: placement.Spec{
					GroupId: "compact-group",
				},
				UpgradeSettings: nil,
			},
		},
		{
			name: "slice_ok",
			spec: &gkeclient.NodePoolSpec{
				MachineType: "a4x-highgpu-4g",
				PlacementGroup: placement.Spec{
					Policy:         "a4x-policy",
					ResourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "1x72"}},
				},
			},
			wantSpec: &gkeclient.NodePoolSpec{
				MachineType: "a4x-highgpu-4g",
				PlacementGroup: placement.Spec{
					Policy:         "a4x-policy",
					ResourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "1x72"}},
				},
				UpgradeSettings: &gke_api_beta.UpgradeSettings{
					Strategy:       "SURGE",
					MaxSurge:       0,
					MaxUnavailable: 18,
				},
			},
		},
		{
			name: "slice_ok_settings_overwritten",
			spec: &gkeclient.NodePoolSpec{
				MachineType: "a4x-highgpu-4g",
				PlacementGroup: placement.Spec{
					Policy:         "a4x-policy",
					ResourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "2x32"}},
				},
				UpgradeSettings: existingSettings,
			},
			wantSpec: &gkeclient.NodePoolSpec{
				MachineType: "a4x-highgpu-4g",
				PlacementGroup: placement.Spec{
					Policy:         "a4x-policy",
					ResourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "2x32"}},
				},
				UpgradeSettings: &gke_api_beta.UpgradeSettings{
					Strategy:       "SURGE",
					MaxSurge:       0,
					MaxUnavailable: 16,
				},
			},
		},
		{
			name: "no_slice_no_chagnes",
			spec: &gkeclient.NodePoolSpec{
				MachineType: "e2-standard-2",
				UpgradeSettings: &gke_api_beta.UpgradeSettings{
					Strategy:       "SURGE",
					MaxSurge:       1234,
					MaxUnavailable: 5678,
				},
			},
			wantSpec: &gkeclient.NodePoolSpec{
				MachineType: "e2-standard-2",
				UpgradeSettings: &gke_api_beta.UpgradeSettings{
					Strategy:       "SURGE",
					MaxSurge:       1234,
					MaxUnavailable: 5678,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).Build()
			generator := NewAcceleratorSliceGenerator(provider)
			err := generator.UpdateNodePoolSpec(tc.spec, nil, nil)

			if tc.wantErr {
				assert.Error(t, err)
				if tc.expectedErrorMsg != "" {
					assert.Contains(t, err.Error(), tc.expectedErrorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.wantSpec, tc.spec)
		})
	}
}

func TestDWSSupportFilteringGenerator_GenerateNodeGroupOptionsForRequirements(t *testing.T) {
	for name, tc := range map[string]struct {
		options      []NodeGroupOptions
		requirements nodeGroupRequirements
		wantOptions  []NodeGroupOptions
	}{
		"No flex start or queued provisioning keeps all": {
			options: []NodeGroupOptions{
				{MachineType: "a3-edgegpu-8g"},
				{MachineType: "a3-highgpu-8g"},
			},
			requirements: nodeGroupRequirements{},
			wantOptions: []NodeGroupOptions{
				{MachineType: "a3-highgpu-8g"},
				{MachineType: "a3-edgegpu-8g"},
			},
		},
		"Flex start remove notInDWS": {
			options: []NodeGroupOptions{
				{MachineType: "a3-edgegpu-8g"},
				{MachineType: "a3-highgpu-8g"},
			},
			requirements: nodeGroupRequirements{
				flexStartReq: flexStartRequirements{enabled: true},
			},
			wantOptions: []NodeGroupOptions{
				{MachineType: "a3-highgpu-8g"},
			},
		},
		"Queued provisioning remove notInDWS": {
			options: []NodeGroupOptions{
				{MachineType: "a3-edgegpu-8g"},
				{MachineType: "a3-highgpu-8g"},
			},
			requirements: nodeGroupRequirements{
				queuedProvisioningReq: podrequirements.QueuedProvisioningRequirements{Enabled: true},
			},
			wantOptions: []NodeGroupOptions{
				{MachineType: "a3-highgpu-8g"},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			dsg := NewDWSSupportFilteringGenerator(machinetypes.NewMachineConfigProvider(nil))
			assert.ElementsMatch(t, tc.wantOptions, dsg.GenerateNodeGroupOptionsForRequirements(tc.options, tc.requirements))
		})
	}
}

func TestConfidentialNodeGenerator_UpdateRequirements(t *testing.T) {
	for desc, tc := range map[string]struct {
		podReq      *podrequirements.Requirements
		wantNgReq   *nodeGroupRequirements
		expectedErr error
	}{
		"confidential type SEV machine family n2d": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.GkeConfidentialNodeType: podrequirements.NewValues(gkelabels.SEVConfidentialNodeTypeValue),
					gkelabels.MachineFamilyLabel:      podrequirements.NewValues(machinetypes.N2D.Name()),
				}),
			},
			wantNgReq: &nodeGroupRequirements{confidentialNodeType: gkelabels.SEVConfidentialNodeTypeValue},
		},
		"confidential type SEV-SNP family n2d": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.GkeConfidentialNodeType: podrequirements.NewValues(gkelabels.SEVSNPConfidentialNodeTypeValue),
					gkelabels.MachineFamilyLabel:      podrequirements.NewValues(machinetypes.N2D.Name()),
				}),
			},
			wantNgReq: &nodeGroupRequirements{confidentialNodeType: gkelabels.SEVSNPConfidentialNodeTypeValue},
		},
		"confidential type SEV family g4": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.GkeConfidentialNodeType: podrequirements.NewValues(gkelabels.SEVConfidentialNodeTypeValue),
					gkelabels.MachineFamilyLabel:      podrequirements.NewValues(machinetypes.G4.Name()),
				}),
			},
			wantNgReq: &nodeGroupRequirements{confidentialNodeType: gkelabels.SEVConfidentialNodeTypeValue},
		},
		"confidential type TDX family C3": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.GkeConfidentialNodeType: podrequirements.NewValues(gkelabels.TDXConfidentialNodeTypeValue),
					gkelabels.MachineFamilyLabel:      podrequirements.NewValues(machinetypes.C3.Name()),
				}),
			},
			wantNgReq: &nodeGroupRequirements{confidentialNodeType: gkelabels.TDXConfidentialNodeTypeValue},
		},
		"confidential type TDX GPU nvidia-h100 (A3)": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.GkeConfidentialNodeType: podrequirements.NewValues(gkelabels.TDXConfidentialNodeTypeValue),
					gkelabels.GPULabel:                podrequirements.NewValues(machinetypes.NvidiaH100_80gb.Name()),
				}),
			},
			wantNgReq: &nodeGroupRequirements{confidentialNodeType: gkelabels.TDXConfidentialNodeTypeValue},
		},
		"confidential type SEV GPU nvidia-rtx-pro-6000 (G4)": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.GkeConfidentialNodeType: podrequirements.NewValues(gkelabels.SEVConfidentialNodeTypeValue),
					gkelabels.GPULabel:                podrequirements.NewValues(gkelabels.NvidiaRTXPro6000),
				}),
			},
			wantNgReq: &nodeGroupRequirements{confidentialNodeType: gkelabels.SEVConfidentialNodeTypeValue},
		},
		"confidential type TDX family C4": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.GkeConfidentialNodeType: podrequirements.NewValues(gkelabels.TDXConfidentialNodeTypeValue),
					gkelabels.MachineFamilyLabel:      podrequirements.NewValues(machinetypes.C4.Name()),
				}),
			},
			wantNgReq: &nodeGroupRequirements{confidentialNodeType: gkelabels.TDXConfidentialNodeTypeValue},
		},
		"Invalid confidential node type": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.GkeConfidentialNodeType: podrequirements.NewValues("invalid-type"),
					gkelabels.MachineFamilyLabel:      podrequirements.NewValues(machinetypes.N2D.Name()),
				}),
			},
			expectedErr: NewInvalidConfidentialNodeTypeError("invalid-type"),
		},
		"No machine family or GPU label": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.GkeConfidentialNodeType: podrequirements.NewValues(gkelabels.SEVConfidentialNodeTypeValue),
				}),
			},
			expectedErr: NewInvalidMachineFamilyForConfidentialNodeTypeError("Machine family or GPU must be explicitly specified to use confidential node types"),
		},
		"Non existent machine family": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.GkeConfidentialNodeType: podrequirements.NewValues(gkelabels.SEVConfidentialNodeTypeValue),
					gkelabels.MachineFamilyLabel:      podrequirements.NewValues("invalid-family"),
				}),
			},
			expectedErr: NewInvalidMachineFamilyForConfidentialNodeTypeError("Unknown machine family: invalid-family"),
		},
		"Unknown GPU type": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.GkeConfidentialNodeType: podrequirements.NewValues(gkelabels.SEVConfidentialNodeTypeValue),
					gkelabels.GPULabel:                podrequirements.NewValues("invalid-gpu"),
				}),
			},
			expectedErr: NewInvalidMachineFamilyForConfidentialNodeTypeError("No known machine family for gpu type: invalid-gpu"),
		},
		"Incompatible machine family": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.GkeConfidentialNodeType: podrequirements.NewValues(gkelabels.TDXConfidentialNodeTypeValue),
					gkelabels.MachineFamilyLabel:      podrequirements.NewValues(machinetypes.N1.Name()),
				}),
			},
			expectedErr: NewInvalidMachineFamilyForConfidentialNodeTypeError("Machine family: n1 does not support confidential node type: TDX"),
		},
		"Incompatible machine family for GPU type nvidia-l4 (G2)": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.GkeConfidentialNodeType: podrequirements.NewValues(gkelabels.TDXConfidentialNodeTypeValue),
					gkelabels.GPULabel:                podrequirements.NewValues(machinetypes.NvidiaL4.Name()),
				}),
			},
			expectedErr: NewInvalidMachineFamilyForConfidentialNodeTypeError("Machine family: g2 does not support confidential node type: TDX"),
		},
		"Incompatible machine family for GPU type nvidia-rtx-pro-6000 (G4) with TDX": {
			podReq: &podrequirements.Requirements{
				LabelReq: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
					gkelabels.GkeConfidentialNodeType: podrequirements.NewValues(gkelabels.TDXConfidentialNodeTypeValue),
					gkelabels.GPULabel:                podrequirements.NewValues(gkelabels.NvidiaRTXPro6000),
				}),
			},
			expectedErr: NewInvalidMachineFamilyForConfidentialNodeTypeError("Machine family: g4 does not support confidential node type: TDX"),
		},
	} {
		t.Run(desc, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).Build()
			generator := NewConfidentialNodeGenerator(provider)

			ngReq := &nodeGroupRequirements{}
			err := generator.UpdateRequirements(ngReq, tc.podReq, machinetypes.GpuRequest{}, TpuRequest{})
			if tc.expectedErr != nil {
				assert.Equal(t, err, tc.expectedErr)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.wantNgReq, ngReq)
		})
	}
}

func TestConfidentialNodeGenerator_UpdateParameters(t *testing.T) {
	for name, tc := range map[string]struct {
		confidentialNodeType string
		expectedLabels       map[string]string
		expectedTaints       []apiv1.Taint
	}{
		"No confidential node type": {
			expectedLabels: make(map[string]string),
		},
		"SEV confidential node type": {
			confidentialNodeType: gkelabels.SEVConfidentialNodeTypeValue,
			expectedLabels: map[string]string{
				gkelabels.GkeConfidentialNodeType: gkelabels.SEVConfidentialNodeTypeValue,
			},
			expectedTaints: []apiv1.Taint{{
				Effect: apiv1.TaintEffectNoSchedule,
				Key:    gkelabels.GkeConfidentialNodeType,
				Value:  gkelabels.SEVConfidentialNodeTypeValue,
			}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).Build()
			generator := NewConfidentialNodeGenerator(provider)
			params := &nodeGroupParameters{
				systemLabels: make(map[string]string),
			}
			ngReq := nodeGroupRequirements{
				confidentialNodeType: tc.confidentialNodeType,
			}
			err := generator.UpdateParameters(params, ngReq, NodeGroupOptions{})
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedLabels, params.systemLabels)
			assert.ElementsMatch(t, tc.expectedTaints, params.taints)
		})
	}
}

func TestConfidentialNodeGenerator_UpdateNodePoolSpec(t *testing.T) {
	for tName, tc := range map[string]struct {
		systemLabels             map[string]string
		wantConfidentialNodeType string
		wantLabels               map[string]string
	}{
		"no confidential node type": {
			systemLabels: map[string]string{},
		},
		"confidential node type set": {
			systemLabels:             map[string]string{gkelabels.GkeConfidentialNodeType: gkelabels.TDXConfidentialNodeTypeValue},
			wantConfidentialNodeType: gkelabels.TDXConfidentialNodeTypeValue,
			wantLabels: map[string]string{
				gkelabels.GkeConfidentialNodeType: gkelabels.TDXConfidentialNodeTypeValue,
			},
		},
	} {
		t.Run(tName, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).Build()
			generator := NewConfidentialNodeGenerator(provider)
			spec := &gkeclient.NodePoolSpec{}
			err := generator.UpdateNodePoolSpec(spec, tc.systemLabels, nil)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantConfidentialNodeType, spec.ConfidentialNodeType)
			assert.Equal(t, len(tc.wantLabels), len(spec.Labels))
			for key, val := range tc.wantLabels {
				assert.Equal(t, spec.Labels[key], val)
			}
		})
	}
}

func TestConfidentialNodeGenerator_GenerateNodeGroupOptionsForRequirements(t *testing.T) {
	for name, tc := range map[string]struct {
		options      []NodeGroupOptions
		requirements nodeGroupRequirements
		wantOptions  []NodeGroupOptions
	}{
		"No confidential node type keeps all": {
			options: []NodeGroupOptions{
				{MachineType: "n2d-standard-2"},
				{MachineType: "n1-standard-1"},
			},
			requirements: nodeGroupRequirements{},
			wantOptions: []NodeGroupOptions{
				{MachineType: "n2d-standard-2"},
				{MachineType: "n1-standard-1"},
			},
		},
		"SEV filters supported family": {
			options: []NodeGroupOptions{
				{MachineType: "n2d-standard-2"},
				{MachineType: "n1-standard-1"},
			},
			requirements: nodeGroupRequirements{
				confidentialNodeType: gkelabels.SEVConfidentialNodeTypeValue,
			},
			wantOptions: []NodeGroupOptions{
				{MachineType: "n2d-standard-2"},
			},
		},
		"SEV keeps supported g4 machine type and filters unsupported": {
			options: []NodeGroupOptions{
				{MachineType: "g4-standard-6"}, //vGPU machine type
				{MachineType: "g4-standard-48"},
				{MachineType: "g4-standard-96"}, //non-vGPU machine type
			},
			requirements: nodeGroupRequirements{
				confidentialNodeType: gkelabels.SEVConfidentialNodeTypeValue,
			},
			wantOptions: []NodeGroupOptions{
				{MachineType: "g4-standard-48"},
			},
		},
		"TDX chooses supported type out of family": {
			options: []NodeGroupOptions{
				{MachineType: "c3-standard-4"},
				{MachineType: "c3-highcpu-4"},
			},
			requirements: nodeGroupRequirements{
				confidentialNodeType: gkelabels.TDXConfidentialNodeTypeValue,
			},
			wantOptions: []NodeGroupOptions{
				{MachineType: "c3-standard-4"},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).Build()
			cng := NewConfidentialNodeGenerator(provider)
			assert.ElementsMatch(t, tc.wantOptions, cng.GenerateNodeGroupOptionsForRequirements(tc.options, tc.requirements))
		})
	}
}

func TestInjectNodeGroups(t *testing.T) {
	e2FamilyName := "e2"
	n2FamilyName := "n2"
	spotTrue := true
	minCpu4 := 4
	minCpu8 := 8

	ccLabel := "autoscaling.gke.io/cc-label"
	ccName := "cc"

	pod1 := test.BuildTestPod("pod1", 8, 128)
	pod2 := test.BuildTestPod("pod2", 16, 32)

	ccPod := test.BuildTestPod("cc-pod", 1, 128)
	ccPod = addSeparation(ccPod, ccLabel, ccName, true)

	testCases := []struct {
		name                       string
		ccs                        []computeclass.CRD
		pods                       []*apiv1.Pod
		wantInjected               int
		wantInjectedPerPriorityIdx map[string]int
	}{
		{
			name: "Duplicate rules in CRD",
			ccs: []computeclass.CRD{
				computeclass.NewTestCrd(
					computeclass.WithLabel(ccLabel),
					computeclass.WithName(ccName),
					computeclass.WithRules([]rules.Rule{
						rules.NewMachineSpecRule(&e2FamilyName, nil, nil, nil),
						rules.NewMachineSpecRule(&e2FamilyName, nil, nil, nil),
					}),
					computeclass.WithAutoprovisioningEnabled(),
				),
			},
			pods:         []*apiv1.Pod{ccPod},
			wantInjected: len(machinetypes.E2.AutoprovisionedMachineTypes(machinetypes.NoConstraints)),
			wantInjectedPerPriorityIdx: map[string]int{
				"0": len(machinetypes.E2.AutoprovisionedMachineTypes(machinetypes.NoConstraints)),
			},
		},
		{
			name: "No duplicate rules in CRD - Spot fallback to non-spot",
			ccs: []computeclass.CRD{
				computeclass.NewTestCrd(
					computeclass.WithLabel(ccLabel),
					computeclass.WithName(ccName),
					computeclass.WithRules([]rules.Rule{
						rules.NewMachineSpecRule(&e2FamilyName, &spotTrue, nil, nil),
						rules.NewMachineSpecRule(&e2FamilyName, nil, nil, nil),
					}),
					computeclass.WithAutoprovisioningEnabled(),
				),
			},
			pods:         []*apiv1.Pod{ccPod},
			wantInjected: 2 * len(machinetypes.E2.AutoprovisionedMachineTypes(machinetypes.NoConstraints)), // spot + non-spot machines
			wantInjectedPerPriorityIdx: map[string]int{
				"0": len(machinetypes.E2.AutoprovisionedMachineTypes(machinetypes.NoConstraints)),
				"1": len(machinetypes.E2.AutoprovisionedMachineTypes(machinetypes.NoConstraints)),
			},
		},
		{
			name: "Overlapping rules in CRD",
			ccs: []computeclass.CRD{
				computeclass.NewTestCrd(
					computeclass.WithLabel(ccLabel),
					computeclass.WithName(ccName),
					computeclass.WithRules([]rules.Rule{
						rules.NewMachineSpecRule(&n2FamilyName, nil, &minCpu8, nil),
						rules.NewMachineSpecRule(&n2FamilyName, nil, &minCpu4, nil),
					}),
					computeclass.WithAutoprovisioningEnabled(),
				),
			},
			pods:         []*apiv1.Pod{ccPod},
			wantInjected: len(machinetypes.N2.AutoprovisionedMachineTypes(machinetypes.NoConstraints)) - 3, // all machine types except n2-standard-2, n2-highcpu-2 and n2-highmem-2 are injected
			wantInjectedPerPriorityIdx: map[string]int{
				// all except: n2-standard-2, n2-standard-4, n2-highcpu-2, n2-highcpu-4, n2-highmem-2 and n2-highmem-4
				"0": len(machinetypes.N2.AutoprovisionedMachineTypes(machinetypes.NoConstraints)) - 6,
				// n2-standard-4, n2-highcpu-4, n2-highmem-4
				"1": 3,
			},
		},
		{
			name:                       "Pods without CRD",
			pods:                       []*apiv1.Pod{pod1, pod2},
			wantInjected:               len(machinetypes.E2.AutoprovisionedMachineTypes(machinetypes.NoConstraints)),
			wantInjectedPerPriorityIdx: map[string]int{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			allFamilies := machinetypes.NewMachineConfigProvider(nil).AllMachineFamilies()
			allMachineTypes := []string{}
			for _, family := range allFamilies {
				for machineType := range family.AllMachineTypes(machinetypes.NoConstraints) {
					allMachineTypes = append(allMachineTypes, machineType)
				}
			}
			machineTypesPerZone := map[string][]string{}
			machineTypesPerZone["us-central1-a"] = allMachineTypes
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithAutoprovisioningDefaultFamily(machinetypes.E2).
				WithMachineTypesPerZone(machineTypesPerZone).
				WithAutoprovisioningLocations("us-central1-a").
				WithAutoprovisioningEnabled(true).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()

			computeClassLister := computeclass_lister.NewMockCrdLister(tc.ccs)
			computeClassLister.SetCrdLabel(ccLabel)
			em := experiments.NewMockManager()
			opts := AutoprovisioningNodeGroupManagerOptions{
				CloudProvider:        provider,
				Lister:               computeClassLister,
				ExperimentsManager:   em,
				OptionsTracker:       tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
				Backoff:              &MockCompositeBackoff{},
				ResourcePolicyPuller: &placement.FakeResourcePolicyPullerProvider{},
				Flags: AutoprovisioningNodeGroupManagerFlags{
					EnableComputeClassMinCapacity: true,
				},
			}
			manager := NewAutoprovisioningNodeGroupManager(opts)

			ctx := &injectionContext{
				zones:                       []string{"us-central1-a"},
				status:                      NewProcessingStatus(),
				nodeInfos:                   make(map[string]*framework.NodeInfo),
				injectedNodeGroupSignatures: sets.New[string](),
			}

			reqs := manager.nonGpuPodsRequirements(ctx, tc.pods)

			totalInjected := 0
			for _, req := range reqs {
				totalInjected += manager.injectNodeGroups(ctx, req)
			}

			assert.Equal(t, tc.wantInjected, totalInjected)

			if tc.wantInjectedPerPriorityIdx != nil {
				gotInjectedPerPriorityIdx := map[string]int{}
				for _, ng := range ctx.injectedNodeGroups {
					if nodeInfo, err := ng.TemplateNodeInfo(); err == nil {
						priorityIdx := nodeInfo.Node().Labels[gkelabels.ComputeClassPriorityIdxLabel]
						if priorityIdx != "" {
							gotInjectedPerPriorityIdx[priorityIdx]++
						}
					}
				}
				assert.Equal(t, tc.wantInjectedPerPriorityIdx, gotInjectedPerPriorityIdx)
			}
		})
	}
}

func requirementsForCC(pods []*apiv1.Pod, family machinetypes.MachineFamily, selectionType machinetypes.SelectionType, cc computeclass.CRD, rule rules.Rule) nodeGroupRequirements {
	return nodeGroupRequirements{
		pods:                     pods,
		computeClass:             cc,
		computeClassRule:         rule,
		machineSpec:              machinetypes.NewMachineSpec([]machinetypes.MachineFamily{family}, machinetypes.AnyPlatform, "", ""),
		machineSelectionType:     selectionType,
		workloadSeparationTaints: []apiv1.Taint{},
		workloadSeparationLabels: map[string]string{},
		systemLabels:             map[string]string{},
	}
}

type testNodeGroupRequirementsOpt func(ngreq *nodeGroupRequirements)

func copyReqsFrom(copyreq nodeGroupRequirements) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		*ngreq = copyreq
	}
}

func withPods(pods []*apiv1.Pod) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.pods = pods
	}
}

func withTPURequest(tpuType string, tpuTopology string, tpuCount int64) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.tpuRequest = TpuRequest{
			TpuType:      tpuType,
			Topology:     tpuTopology,
			ChipsPerNode: tpuCount,
		}
		ngreq.machineSpec.TpuType = tpuType
	}
}

func withComputeClass(cc computeclass.CRD) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.computeClass = cc
	}
}

func withNodeConfigRule(rule rules.Rule) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.computeClassRule = rule
	}
}

func withMaxPodsPerNode(mppn int) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.maxPodsPerNode = mppn
	}
}

func withMachineFamily(family machinetypes.MachineFamily) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.machineSpec = machinetypes.NewMachineSpec([]machinetypes.MachineFamily{family}, machinetypes.AnyPlatform, "", "")
	}
}

func WithMaxRunDurationSeconds(maxRunDurationSeconds int) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.maxRunDurationInSeconds = fmt.Sprintf("%d", maxRunDurationSeconds)
	}
}

func withMachineSelectionType(selectionType machinetypes.SelectionType) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.machineSelectionType = selectionType
	}
}

func withBootDiskType(bootDiskType string) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.machineSpec.BootDiskType = bootDiskType
		ngreq.bootDiskType = bootDiskType
	}
}

func withBootDiskEncryptionKey(key string) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.bootDiskEncryptionKey = key
	}
}

func withBootDiskSize(bootDiskSize int) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.bootDiskSize = bootDiskSize
	}
}

func withEphemeralStorageLSSDCount(count int) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.ephemeralStorageLocalSSDCount = count
	}
}

func withTotalLSSDCount(localSSDCount int) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.totalLSSDCount = localSSDCount
	}
}

func withMachineType(machineType string) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.machineSpec.ExplicitMachineTypes = []string{machineType}
	}
}

func withGPURequest(gpuType string, gpuCount machinetypes.PhysicalGpuCount) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.gpuRequest = machinetypes.GpuRequest{
			Config: machinetypes.GpuConfig{
				GpuType: gpuType,
			},
			Count:            machinetypes.AllocatableGpuCount(gpuCount),
			PhysicalGPUCount: gpuCount,
		}
		ngreq.machineSpec.GpuType = gpuType
	}
}
func withGPURequestWithDriverVersion(gpuType string, gpuCount machinetypes.PhysicalGpuCount, driverVersion string) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.gpuRequest = machinetypes.GpuRequest{
			Config: machinetypes.GpuConfig{
				GpuType:       gpuType,
				DriverVersion: driverVersion,
			},
			Count:            machinetypes.AllocatableGpuCount(gpuCount),
			PhysicalGPUCount: gpuCount,
		}
		ngreq.machineSpec.GpuType = gpuType
	}
}

func withReservationAffinity(affinity string) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.reservation.affinity = affinity
	}
}

func withReservationProject(project string) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.reservation.project = project
	}
}

func withReservationName(name string) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.reservation.name = name
	}
}

func withReservationExists() testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.reservation.exists = true
	}
}

func withReservationBlock(blockName string) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.reservation.block = blockName
	}
}

func withReservationSubBlock(subBlockName string) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.reservation.subBlock = subBlockName
	}
}

func withSecondaryBootDisk(diskImageName string, project string, mode string) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		gkeApisecondaryBootDisk := rules.GenerateGkeApiSecondaryBootDisk(diskImageName, project, mode)
		ngreq.secondaryBootDisks = append(ngreq.secondaryBootDisks, gkeApisecondaryBootDisk)
	}
}

func withLinuxNodeConfig(linuxNodeConfig *gkeclient.LinuxNodeConfig) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.linuxNodeConfig = linuxNodeConfig
	}
}

func withKubeletConfig(kubeletConfig *gke_api_beta.NodeKubeletConfig) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.kubeletConfig = kubeletConfig
	}
}

func withSpecifiedZones(specifiedZones []string) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.specifiedZones = specifiedZones
	}
}

func withPlacementPolicy(policy string) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.placementGroup.Policy = policy
	}
}

func withPlacementPolicyRule(placementPolicyRule rules.Rule) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.computeClassRule = placementPolicyRule
	}
}

func withSystemLabels(systemLabels map[string]string) testNodeGroupRequirementsOpt {
	return func(ngreq *nodeGroupRequirements) {
		ngreq.systemLabels = systemLabels
	}
}

func withNodeVersion(nodeVersion string) testNodeGroupRequirementsOpt {
	return func(r *nodeGroupRequirements) {
		r.nodeVersion = nodeVersion
	}
}

func newTestNodeGroupRequirements(options ...testNodeGroupRequirementsOpt) nodeGroupRequirements {
	ngreq := &nodeGroupRequirements{}
	for _, opt := range options {
		opt(ngreq)
	}
	// This method is only being used by ComputeClass tests. Feel free to remove it if taints / labels are needed.
	ngreq.workloadSeparationTaints = []apiv1.Taint{}
	ngreq.workloadSeparationLabels = map[string]string{}
	return *ngreq
}

func withAnnotations(annotations map[string]string) func(*apiv1.Pod) {
	return func(pod *apiv1.Pod) {
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		for k, v := range annotations {
			pod.Annotations[k] = v
		}
	}
}

type MockCompositeBackoff struct{}

func (m *MockCompositeBackoff) Backoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, errorInfo cloudprovider.InstanceErrorInfo, currentTime time.Time) time.Time {
	return time.Time{}
}

func (m *MockCompositeBackoff) BackoffStatus(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, currentTime time.Time) base_backoff.Status {
	return base_backoff.Status{IsBackedOff: false}
}

func (m *MockCompositeBackoff) BackoffInAllZones(nodeGroup cloudprovider.NodeGroup, zones []string, nodeInfo *framework.NodeInfo, errorInfo cloudprovider.InstanceErrorInfo, currentTime time.Time) {
}

func (m *MockCompositeBackoff) RemoveBackoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo) {
}

func (m *MockCompositeBackoff) RemoveStaleBackoffData(currentTime time.Time) {}

func (m *MockCompositeBackoff) GetBackoffs() []base_backoff.Backoff {
	return nil
}

func TestNodeVersionGenerator_UpdateRequirements(t *testing.T) {
	nodeVersion := "1.32.9-gke.1726000"
	for tName, tc := range map[string]struct {
		computeClass    computeclass.CRD
		wantNodeVersion string
	}{
		"computeClass with node version": {
			computeClass: computeclass.NewTestCrd(
				computeclass.WithName("cc"),
				computeclass.WithScaleUpAnyway(),
				computeclass.WithAutoprovisioningEnabled(),
				computeclass.WithNodeVersion(nodeVersion)),
			wantNodeVersion: nodeVersion,
		},
		"computeClass without node version": {
			computeClass: computeclass.NewTestCrd(
				computeclass.WithName("cc"),
				computeclass.WithScaleUpAnyway(),
				computeclass.WithAutoprovisioningEnabled()),
			wantNodeVersion: "",
		},
		"no computeClass": {
			computeClass:    nil,
			wantNodeVersion: "",
		},
	} {
		t.Run(tName, func(t *testing.T) {
			generator := NewNodeVersionGenerator()
			ngReq := &nodeGroupRequirements{computeClass: tc.computeClass}
			err := generator.UpdateRequirements(ngReq, nil, machinetypes.GpuRequest{}, TpuRequest{})
			assert.NoError(t, err)
			assert.Equal(t, tc.wantNodeVersion, ngReq.nodeVersion)
		})
	}
}

func TestNodeVersionGenerator_UpdateNodePoolSpec(t *testing.T) {
	nodeVersion := "1.32.9-gke.1726000"
	for tName, tc := range map[string]struct {
		systemLabels    map[string]string
		wantNodeVersion string
	}{
		"node version label present": {
			systemLabels:    map[string]string{gkelabels.NodeVersionLabelKey: nodeVersion},
			wantNodeVersion: nodeVersion,
		},
		"node version label missing": {
			systemLabels:    map[string]string{},
			wantNodeVersion: "",
		},
	} {
		t.Run(tName, func(t *testing.T) {
			spec := &gkeclient.NodePoolSpec{}
			generator := NewNodeVersionGenerator()
			err := generator.UpdateNodePoolSpec(spec, tc.systemLabels, nil)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantNodeVersion, spec.NodeVersion)
		})
	}
}

func TestNodeVersionGenerator_UpdateParameters(t *testing.T) {
	nodeVersion := "1.32.9-gke.1726000"
	for tName, tc := range map[string]struct {
		req              nodeGroupRequirements
		wantSystemLabels map[string]string
	}{
		"node version present in requirements": {
			req:              nodeGroupRequirements{nodeVersion: nodeVersion},
			wantSystemLabels: map[string]string{gkelabels.NodeVersionLabelKey: nodeVersion},
		},
		"node version missing in requirements": {
			req:              nodeGroupRequirements{},
			wantSystemLabels: map[string]string{},
		},
	} {
		t.Run(tName, func(t *testing.T) {
			params := &nodeGroupParameters{systemLabels: map[string]string{}}
			generator := NewNodeVersionGenerator()
			err := generator.UpdateParameters(params, tc.req, NodeGroupOptions{})
			assert.NoError(t, err)
			assert.Equal(t, tc.wantSystemLabels, params.systemLabels)
		})
	}
}

func testOptionsTracker(modifier func(opts *internalopts.InternalOptions)) *optstracking.OptionsTracker {
	opts := internalopts.InternalOptions{}
	if modifier != nil {
		modifier(&opts)
	}

	return optstracking.FakeOptionsTracker(internalopts.AutoscalingOptions{
		InternalOptions: opts,
	}, gkeclient.Cluster{}, experiments.NewMockManager())
}
