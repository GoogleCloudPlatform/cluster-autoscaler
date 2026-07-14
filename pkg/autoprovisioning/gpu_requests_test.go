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
	"testing"

	"k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	gce_api "google.golang.org/api/compute/v1"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/machineselection"
	gke_backoff "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	computeclass "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	computeclass_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/gkedebuggingsnapshot"
	internal_customresources "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/customresources"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaleblocking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
)

func TestGpuPodsRequirements(t *testing.T) {
	ANY_GPU_SPECIAL_VAL := "ANY_GPU_SPECIAL_VAL"
	noGpuPod1 := test.BuildTestPod("noGpuPod1", 0, 1000)
	noGpuPod2 := test.BuildTestPod("noGpuPod2", 0, 1000)
	oneK80Pod := buildGpuPod("OneK80Pod1", machinetypes.NvidiaTeslaK80.Name(), 1)
	twoK80Pod := buildGpuPod("TwoK80Pod1", machinetypes.NvidiaTeslaK80.Name(), 2)
	fiveK80Pod := buildGpuPod("FiveK80Pod1", machinetypes.NvidiaTeslaK80.Name(), 5)
	twoP100Pod := buildGpuPod("TwoP100Pod1", machinetypes.NvidiaTeslaP100.Name(), 2)
	oneAnyGpuPod := buildGpuPod("OneAnyPod1", machinetypes.AnyGPU, 1)
	eightAnyGpuPod := buildGpuPod("EightAnyPod1", machinetypes.AnyGPU, 8)
	fiveAnyGpuPod := buildGpuPod("FiveAnyPod1", machinetypes.AnyGPU, 5)
	tooBigGpu := buildGpuPod("PodWithTooMuchGpuRequest", machinetypes.NvidiaTeslaP100.Name(), 20)
	limitNotDefined := buildGpuPod("LimitNotDefined", machinetypes.NvidiaTeslaP4.Name(), 1)
	unsupportedGpu := buildGpuPod("UnsupportedGpu", "unsupported-gpu", 1)
	tooBigCpu := buildPodWithGpuCpusAndMem("PodWithTooMuchCpuRequest", machinetypes.NvidiaTeslaK80.Name(), 1, 128000, 1000)
	valid96CpuRequestPod := buildPodWithGpuCpusAndMem("PodWithTooValid96CpuRequest", machinetypes.NvidiaTeslaT4.Name(), 1, 96000, 1000)
	oneA100PodVeryHighMemory := buildPodWithGpuCpusAndMem("oneA100PodVeryHighMemory", machinetypes.NvidiaTeslaA100.Name(), 1, 1000, 96*units.GiB)
	oneV100PodVeryHighMemory := buildPodWithGpuCpusAndMem("oneV100PodVeryHighMemory", machinetypes.NvidiaTeslaV100.Name(), 1, 1000, 60*units.GiB)
	unknownPlatformAnyGpu := addMinCpuPlatform(buildGpuPod("unknownPlatformAnyGpu", machinetypes.AnyGPU, 1), "unknown")
	unknownPlatformK80 := addMinCpuPlatform(buildGpuPod("unknownPlatformK80", machinetypes.NvidiaTeslaK80.Name(), 1), "unknown")
	a2AnyGpu := addMachineFamily(buildGpuPod("a2AnyGpu", machinetypes.AnyGPU, 1), "a2")
	partitioning := addGpuPartitioning(buildGpuPod("partitioning", machinetypes.NvidiaTeslaA100.Name(), 8), "1g.5gb")
	partitioningVeryHighMem := addGpuPartitioning(buildPodWithGpuCpusAndMem("partitioningVeryHighMem", machinetypes.NvidiaTeslaA100.Name(), 1, 1000, 96*units.GiB), "1g.5gb")
	timeSharing := addGpuTimeSharing(buildGpuPod("timeSharing", machinetypes.NvidiaTeslaA100.Name(), 7), "3", labels.GPUTimeSharingStrategy)
	timeSharingVeryHighMem := addGpuTimeSharing(buildPodWithGpuCpusAndMem("timeSharingVeryHighMem", machinetypes.NvidiaTeslaV100.Name(), 1, 1000, 60*units.GiB), "3", labels.GPUTimeSharingStrategy)
	partitioningTimeSharing := addGpuTimeSharing(addGpuPartitioning(buildGpuPod("partitioningTimeSharing", machinetypes.NvidiaTeslaA100.Name(), 22), "1g.5gb"), "3", labels.GPUTimeSharingStrategy)
	partitioningTimeSharingVeryHighMem := addGpuTimeSharing(addGpuPartitioning(buildPodWithGpuCpusAndMem("partitioningVeryHighMem", machinetypes.NvidiaTeslaA100.Name(), 1, 1000, 96*units.GiB), "1g.5gb"), "3", labels.GPUTimeSharingStrategy)
	invalidPartitioning := addGpuPartitioning(buildGpuPod("invalidPartitioning", machinetypes.NvidiaTeslaA100.Name(), 8), "100g.5000gb")
	invalidMaxSharedClients := addGpuTimeSharing(buildGpuPod("invalidMaxSharedClients", machinetypes.NvidiaTeslaA100.Name(), 7), "300", labels.GPUTimeSharingStrategy)
	oneK80PredicatesFailing := addRequirement(buildGpuPod("OneK80PredicatesFailing", machinetypes.NvidiaTeslaK80.Name(), 1), "cloud.google.com/not-a-valid-label", "some-value")
	oneK80PredicatesFailingCentral1aOnly := addRequirement(buildGpuPod("OneK80PredicatesFailingOneZoneOnly", machinetypes.NvidiaTeslaK80.Name(), 1), apiv1.LabelTopologyZone, "us-central1-a")
	oneNvidiaH200PodWithLatestVersion := addGpuDriverVersion(buildPodWithGpuCpusAndMem("oneNvidiaH200PodWithLatestVersion", machinetypes.NvidiaH200Ultra_141gb.Name(), 8, 1000, 1*units.GiB), "latest")
	oneNvidiaH200PodWithoutVersion := buildPodWithGpuCpusAndMem("oneNvidiaH200PodWithoutVersion", machinetypes.NvidiaH200Ultra_141gb.Name(), 8, 1000, 1*units.GiB)

	a100GPU := machinetypes.NvidiaTeslaA100.Name()
	gpuCount := machinetypes.PhysicalGpuCount(2)
	gpuCCName := "gpu-cc"
	ccLabel := labels.ComputeClassLabel
	a100GpuRequest := machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: a100GPU}, Count: machinetypes.AllocatableGpuCount(gpuCount), PhysicalGPUCount: gpuCount}
	gpuRule := rules.NewRule(rules.WithGpuRule(&a100GpuRequest), rules.WithMachineFamilyRule(nil))
	h100GPU := machinetypes.NvidiaH100Mega_80gb.Name()
	h100GpuCount := machinetypes.PhysicalGpuCount(8)
	h100GpuRequest := machinetypes.GpuRequest{Config: machinetypes.GpuConfig{GpuType: h100GPU}, Count: machinetypes.AllocatableGpuCount(h100GpuCount), PhysicalGPUCount: h100GpuCount}
	h100GpuRule := rules.NewRule(rules.WithGpuRule(&h100GpuRequest), rules.WithMachineFamilyRule(nil))
	singleGpuCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{gpuRule}),
		computeclass.WithLabel(ccLabel),
		computeclass.WithName(gpuCCName),
		computeclass.WithAutoprovisioningEnabled(),
	)
	multipleGPUCC := computeclass.NewTestCrd(
		computeclass.WithRules([]rules.Rule{gpuRule, h100GpuRule}),
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
	gpuComputeClassPod := buildGpuPod("gpu-cc-pod", machinetypes.AnyGPU, 2)
	gpuComputeClassPod = addSeparation(gpuComputeClassPod, labels.ComputeClassLabel, gpuCCName, true)
	defaultLimits := map[string]int64{
		machinetypes.NvidiaTeslaK80.Name():  15,
		machinetypes.NvidiaTeslaT4.Name():   15,
		machinetypes.NvidiaTeslaP100.Name(): 15,
		machinetypes.NvidiaTeslaA100.Name(): 15,
		machinetypes.NvidiaTeslaV100.Name(): 15,
	}

	tests := []struct {
		name                 string
		pods                 []*apiv1.Pod
		overrideLimits       map[string]int64
		overrideAPLocations  []string
		expectedRequirements []nodeGroupRequirements // Only pods and GpuRequest are checked.
		expectedPodStatuses  map[types.UID]PodProcessingStatus
		computeClasses       []computeclass.CRD
	}{
		{
			name:                 "no gpu pods",
			pods:                 []*apiv1.Pod{noGpuPod1, noGpuPod2},
			expectedRequirements: []nodeGroupRequirements{},
		},
		{
			name: "podAnyGPU_limitsWithDefault_error",
			pods: []*apiv1.Pod{oneAnyGpuPod},
			overrideLimits: map[string]int64{
				machinetypes.DeprecatedDefaultGPU:   999,
				machinetypes.NvidiaTeslaP100.Name(): 999,
				machinetypes.NvidiaTeslaT4.Name():   999,
			},
			expectedPodStatuses: map[types.UID]PodProcessingStatus{
				oneAnyGpuPod.UID: {Err: NewGpuRequestInvalidError("GPU type is not specified")},
			},
		},
		{
			name: "podAnyGPU_limitsWithoutDefault_ok",
			pods: []*apiv1.Pod{oneAnyGpuPod},
			overrideLimits: map[string]int64{
				machinetypes.NvidiaTeslaP100.Name(): 999,
				machinetypes.NvidiaTeslaT4.Name():   999,
			},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config:           machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaP100.Name()},
						Count:            1,
						PhysicalGPUCount: 1,
					},
					pods: []*apiv1.Pod{oneAnyGpuPod},
				},
				{
					gpuRequest: machinetypes.GpuRequest{
						Config:           machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaT4.Name()},
						Count:            1,
						PhysicalGPUCount: 1,
					},
					pods: []*apiv1.Pod{oneAnyGpuPod},
				},
			},
		},
		{
			name: "any-gpu pod, limit for default GPU defined",
			pods: []*apiv1.Pod{oneAnyGpuPod},
			overrideLimits: map[string]int64{
				machinetypes.DeprecatedDefaultGPU: 15,
			},
			expectedPodStatuses: map[types.UID]PodProcessingStatus{
				oneAnyGpuPod.UID: {Err: NewGpuRequestInvalidError("GPU type is not specified")},
			},
		},
		{
			name: "any-gpu pod, limit for default GPU not defined",
			pods: []*apiv1.Pod{oneAnyGpuPod},
			overrideLimits: map[string]int64{
				machinetypes.NvidiaTeslaT4.Name():   15,
				machinetypes.NvidiaTeslaP100.Name(): 15,
			},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config:           machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaT4.Name()},
						Count:            1,
						PhysicalGPUCount: 1,
					},
					pods: []*apiv1.Pod{oneAnyGpuPod},
				},
				{
					gpuRequest: machinetypes.GpuRequest{
						Config:           machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaP100.Name()},
						Count:            1,
						PhysicalGPUCount: 1,
					},
					pods: []*apiv1.Pod{oneAnyGpuPod},
				},
			},
		},
		{
			name: "any-gpu pod, limit for default GPU not defined, limits for unsupported GPUs are filtered out",
			pods: []*apiv1.Pod{oneAnyGpuPod},
			overrideLimits: map[string]int64{
				machinetypes.NvidiaTeslaT4.Name():   15,
				machinetypes.NvidiaTeslaP100.Name(): 15,
				"not-supported-gpu":                 15,
			},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config:           machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaT4.Name()},
						Count:            1,
						PhysicalGPUCount: 1,
					},
					pods: []*apiv1.Pod{oneAnyGpuPod},
				},
				{
					gpuRequest: machinetypes.GpuRequest{
						Config:           machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaP100.Name()},
						Count:            1,
						PhysicalGPUCount: 1,
					},
					pods: []*apiv1.Pod{oneAnyGpuPod},
				},
			},
		},
		{
			name: "no-gpu pods and k80 pods (1,2)",
			pods: []*apiv1.Pod{noGpuPod1, noGpuPod2, oneK80Pod, twoK80Pod},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config:           machinetypes.GpuConfig{GpuType: "nvidia-tesla-k80"},
						Count:            1,
						PhysicalGPUCount: 1,
					},
					pods: []*apiv1.Pod{oneK80Pod},
				},
			},
		},
		{
			name: "no-gpu pods and k80 pods (1,5)",
			pods: []*apiv1.Pod{noGpuPod1, noGpuPod2, oneK80Pod, fiveK80Pod},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config:           machinetypes.GpuConfig{GpuType: "nvidia-tesla-k80"},
						Count:            1,
						PhysicalGPUCount: 1,
					},
					pods: []*apiv1.Pod{oneK80Pod},
				},
			},
		},
		{
			name: "k80 pods (1,2), and any-gpu pods (1,5)",
			pods: []*apiv1.Pod{oneK80Pod, twoK80Pod, oneAnyGpuPod, fiveAnyGpuPod},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config:           machinetypes.GpuConfig{GpuType: "nvidia-tesla-k80"},
						Count:            1,
						PhysicalGPUCount: 1,
					},
					pods: []*apiv1.Pod{oneK80Pod},
				},
			},
			expectedPodStatuses: map[types.UID]PodProcessingStatus{
				fiveAnyGpuPod.UID: {Err: NewGpuRequestInvalidError("GPU type is not specified")},
				oneAnyGpuPod.UID:  {Err: NewGpuRequestInvalidError("GPU type is not specified")},
			},
		},
		{
			name: "k80 pods (1,2), and any-gpu pod (8)",
			pods: []*apiv1.Pod{oneK80Pod, twoK80Pod, eightAnyGpuPod},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config:           machinetypes.GpuConfig{GpuType: "nvidia-tesla-k80"},
						Count:            1,
						PhysicalGPUCount: 1,
					},
					pods: []*apiv1.Pod{oneK80Pod},
				},
			},
			expectedPodStatuses: map[types.UID]PodProcessingStatus{
				eightAnyGpuPod.UID: {Err: NewGpuRequestInvalidError("GPU type is not specified")},
			},
		},
		{
			name: "k80 pods (2), and any-gpu pod (1)",
			pods: []*apiv1.Pod{twoK80Pod, oneAnyGpuPod},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config:           machinetypes.GpuConfig{GpuType: "nvidia-tesla-k80"},
						Count:            2,
						PhysicalGPUCount: 2,
					},
					pods: []*apiv1.Pod{twoK80Pod},
				},
			},
			expectedPodStatuses: map[types.UID]PodProcessingStatus{
				oneAnyGpuPod.UID: {Err: NewGpuRequestInvalidError("GPU type is not specified")},
			},
		},
		{
			name:                 "k80 pod (20)",
			pods:                 []*apiv1.Pod{tooBigGpu},
			expectedRequirements: []nodeGroupRequirements{},
			expectedPodStatuses: map[types.UID]PodProcessingStatus{
				// The actual reason isn't checked for equality because it's not stable, just the error type.
				"PodWithTooMuchGpuRequest": {Err: NewGpuRequestInvalidError("")},
			},
		},
		{
			name:                 "k80 pod (1) with too much CPU request",
			pods:                 []*apiv1.Pod{tooBigCpu},
			expectedRequirements: []nodeGroupRequirements{},
			expectedPodStatuses: map[types.UID]PodProcessingStatus{
				// The actual reason isn't checked for equality because it's not stable, just the error type.
				"PodWithTooMuchCpuRequest": {Err: NewGpuRequestInvalidError("")},
			},
		},
		{
			name: "TeslaT4 pod (1) with valid CPU request",
			pods: []*apiv1.Pod{valid96CpuRequestPod},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config:           machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaT4.Name()},
						Count:            4,
						PhysicalGPUCount: 4,
					},
					pods: []*apiv1.Pod{valid96CpuRequestPod},
				},
			},
		},
		{
			name: "TeslaA100 pod requesting 1 GPU and high memory",
			pods: []*apiv1.Pod{oneA100PodVeryHighMemory},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config:           machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaA100.Name()},
						Count:            2,
						PhysicalGPUCount: 2,
					},
					pods: []*apiv1.Pod{oneA100PodVeryHighMemory},
				},
			},
		},
		{
			name: "TeslaV100 pod requesting 1 GPU and high memory",
			pods: []*apiv1.Pod{oneV100PodVeryHighMemory},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config:           machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaV100.Name()},
						Count:            2,
						PhysicalGPUCount: 2,
					},
					pods: []*apiv1.Pod{oneV100PodVeryHighMemory},
				},
			},
		},
		{
			name: "NvidiaH200 pod requesting 8 GPU, driver version latest",
			pods: []*apiv1.Pod{oneNvidiaH200PodWithLatestVersion},
			overrideLimits: map[string]int64{
				machinetypes.NvidiaH200Ultra_141gb.Name(): 224,
			},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config: machinetypes.GpuConfig{
							GpuType:       machinetypes.NvidiaH200Ultra_141gb.Name(),
							DriverVersion: "latest"},
						Count:            8,
						PhysicalGPUCount: 8,
					},
					pods: []*apiv1.Pod{oneNvidiaH200PodWithLatestVersion},
				},
			},
		},
		{
			name: "NvidiaH200 pod requesting 8 GPU, missing driver version",
			pods: []*apiv1.Pod{oneNvidiaH200PodWithoutVersion},
			overrideLimits: map[string]int64{
				machinetypes.NvidiaH200Ultra_141gb.Name(): 224,
			},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config: machinetypes.GpuConfig{
							GpuType:       machinetypes.NvidiaH200Ultra_141gb.Name(),
							DriverVersion: ""},
						Count:            8,
						PhysicalGPUCount: 8,
					},
					pods: []*apiv1.Pod{oneNvidiaH200PodWithoutVersion},
				},
			},
		},
		{
			name: "pod requesting an unsupported GPU",
			pods: []*apiv1.Pod{unsupportedGpu},
			expectedPodStatuses: map[types.UID]PodProcessingStatus{
				"UnsupportedGpu": {Err: NewGpuTypeNotSupportedError("unsupported-gpu")},
			},
		},
		{
			name: "pod requesting a GPU without a limit defined",
			pods: []*apiv1.Pod{limitNotDefined},
			expectedPodStatuses: map[types.UID]PodProcessingStatus{
				"LimitNotDefined": {Err: NewGpuTypeNoLimitDefinedError(machinetypes.NvidiaTeslaP4.Name())},
			},
		},
		{
			name:           "any GPU pod and no limits",
			pods:           []*apiv1.Pod{oneAnyGpuPod},
			overrideLimits: map[string]int64{},
			expectedPodStatuses: map[types.UID]PodProcessingStatus{
				"OneAnyPod1": {Err: NewGpuTypeNoLimitDefinedError("any GPU")},
			},
		},
		{
			name: "any-gpu pod and limits only for unsupported GPUs",
			pods: []*apiv1.Pod{oneAnyGpuPod},
			overrideLimits: map[string]int64{
				"not-supported-gpu-1": 15,
				"not-supported-gpu-2": 15,
			},
			expectedPodStatuses: map[types.UID]PodProcessingStatus{
				"OneAnyPod1": {Err: NewGpuTypeNoLimitDefinedError("any GPU")},
			},
		},
		{
			name: "pod with invalid machine selection config requesting a specific GPU",
			pods: []*apiv1.Pod{unknownPlatformK80},
			expectedPodStatuses: map[types.UID]PodProcessingStatus{
				"unknownPlatformK80": {Err: machineselection.NewMinCpuPlatformUnknownError("unknown")},
			},
		},
		{
			name: "pod with invalid machine selection config requesting any GPU, limit for the default GPU not defined",
			pods: []*apiv1.Pod{unknownPlatformAnyGpu},
			overrideLimits: map[string]int64{
				machinetypes.NvidiaTeslaT4.Name():   5,
				machinetypes.NvidiaTeslaV100.Name(): 5,
				machinetypes.NvidiaTeslaP100.Name(): 5,
			},
			expectedPodStatuses: map[types.UID]PodProcessingStatus{
				"unknownPlatformAnyGpu": {Err: machineselection.NewMinCpuPlatformUnknownError("unknown")},
			},
		},
		{
			name: "pod requesting A2 and any GPU, limit for the default GPU and A100 not defined",
			pods: []*apiv1.Pod{a2AnyGpu},
			overrideLimits: map[string]int64{
				machinetypes.NvidiaTeslaT4.Name():   5,
				machinetypes.NvidiaTeslaP100.Name(): 5,
				machinetypes.NvidiaTeslaV100.Name(): 5,
			},
			expectedPodStatuses: map[types.UID]PodProcessingStatus{
				// GpuName should contain one of the names from the limits, doesn't matter which one.
				// ANY_GPU_SPECIAL_VAL matches any value here.
				"a2AnyGpu": {Err: machineselection.NewGpuIncompatibleError(`machine family "a2"`, ANY_GPU_SPECIAL_VAL)},
			},
		},
		{
			name: "pod requesting A2 and any GPU, limit for default GPU not defined, but A100 defined",
			pods: []*apiv1.Pod{a2AnyGpu},
			overrideLimits: map[string]int64{
				machinetypes.NvidiaTeslaT4.Name():   5,
				machinetypes.NvidiaTeslaP100.Name(): 5,
				machinetypes.NvidiaTeslaA100.Name(): 5,
			},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config:           machinetypes.GpuConfig{GpuType: "nvidia-tesla-a100"},
						Count:            1,
						PhysicalGPUCount: 1,
					},
					pods: []*apiv1.Pod{a2AnyGpu},
				},
			},
		},
		{
			name: "pods requesting GPU partitioning",
			pods: []*apiv1.Pod{partitioning},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config: machinetypes.GpuConfig{
							GpuType:       "nvidia-tesla-a100",
							PartitionSize: "1g.5gb",
						},
						Count:            14,
						PhysicalGPUCount: 2,
					},
					pods: []*apiv1.Pod{partitioning},
				},
			},
		},
		{
			name: "pods requesting GPU partitioning - request is only 1 partition, but additionally very high memory",
			pods: []*apiv1.Pod{partitioningVeryHighMem},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config: machinetypes.GpuConfig{
							GpuType:       "nvidia-tesla-a100",
							PartitionSize: "1g.5gb",
						},
						Count:            14,
						PhysicalGPUCount: 2,
					},
					pods: []*apiv1.Pod{partitioningVeryHighMem},
				},
			},
		},
		{
			name: "invalid GPU partitioning",
			pods: []*apiv1.Pod{invalidPartitioning},
			expectedPodStatuses: map[types.UID]PodProcessingStatus{
				// The actual reason isn't checked for equality because it's not stable, just the error type.
				"invalidPartitioning": {Err: NewGpuRequestInvalidError("")},
			},
		},
		{
			name: "pods requesting GPU time-sharing",
			pods: []*apiv1.Pod{timeSharing},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config: machinetypes.GpuConfig{
							GpuType:          "nvidia-tesla-a100",
							MaxSharedClients: "3",
							SharingStrategy:  labels.GPUTimeSharingStrategy,
						},
						Count:            12,
						PhysicalGPUCount: 4,
					},
					pods: []*apiv1.Pod{timeSharing},
				},
			},
		},
		{
			name: "pods requesting GPU time-sharing - only 1 GPU, but very high memory",
			pods: []*apiv1.Pod{timeSharingVeryHighMem},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config: machinetypes.GpuConfig{
							GpuType:          "nvidia-tesla-v100",
							MaxSharedClients: "3",
							SharingStrategy:  labels.GPUTimeSharingStrategy,
						},
						Count:            6,
						PhysicalGPUCount: 2,
					},
					pods: []*apiv1.Pod{timeSharingVeryHighMem},
				},
			},
		},
		{
			name: "invalid max shared clients",
			pods: []*apiv1.Pod{invalidMaxSharedClients},
			expectedPodStatuses: map[types.UID]PodProcessingStatus{
				// The actual reason isn't checked for equality because it's not stable, just the error type.
				"invalidMaxSharedClients": {Err: NewGpuRequestInvalidError("")},
			},
		},
		{
			name: "pods requesting GPU partitioning and time sharing",
			pods: []*apiv1.Pod{partitioningTimeSharing},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config: machinetypes.GpuConfig{
							GpuType:          "nvidia-tesla-a100",
							PartitionSize:    "1g.5gb",
							MaxSharedClients: "3",
							SharingStrategy:  labels.GPUTimeSharingStrategy,
						},
						Count:            42,
						PhysicalGPUCount: 2,
					},
					pods: []*apiv1.Pod{partitioningTimeSharing},
				},
			},
		},
		{
			name: "pods requesting GPU partitioning and time sharing and very high memory",
			pods: []*apiv1.Pod{partitioningTimeSharingVeryHighMem},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config: machinetypes.GpuConfig{
							GpuType:          "nvidia-tesla-a100",
							PartitionSize:    "1g.5gb",
							MaxSharedClients: "3",
							SharingStrategy:  labels.GPUTimeSharingStrategy,
						},
						Count:            42,
						PhysicalGPUCount: 2,
					},
					pods: []*apiv1.Pod{partitioningTimeSharingVeryHighMem},
				},
			},
		},
		{
			name: "pods with low GPU requests that don't pass scheduler predicates don't block valid pods with higher GPU requests (b/239066590)",
			pods: []*apiv1.Pod{oneK80PredicatesFailing, twoK80Pod},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config:           machinetypes.GpuConfig{GpuType: "nvidia-tesla-k80"},
						Count:            2,
						PhysicalGPUCount: 2,
					},
					pods: []*apiv1.Pod{twoK80Pod},
				},
			},
			expectedPodStatuses: map[types.UID]PodProcessingStatus{
				"OneK80PredicatesFailing": {Err: NewGpuRequestFailingPredicatesError([]string{"node(s) didn't match Pod's node affinity/selector"})},
			},
		},
		{
			name: "pods that don't pass scheduler predicates only in some zones are considered as normal - good zone first",
			pods: []*apiv1.Pod{oneK80PredicatesFailingCentral1aOnly, twoK80Pod},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config:           machinetypes.GpuConfig{GpuType: "nvidia-tesla-k80"},
						Count:            1,
						PhysicalGPUCount: 1,
					},
					pods: []*apiv1.Pod{oneK80PredicatesFailingCentral1aOnly},
				},
			},
			overrideAPLocations: []string{"us-central1-b", "us-central1-a"},
		},
		{
			name: "pods that don't pass scheduler predicates only in some zones are considered as normal - bad zone first",
			pods: []*apiv1.Pod{oneK80PredicatesFailingCentral1aOnly, twoK80Pod},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config:           machinetypes.GpuConfig{GpuType: "nvidia-tesla-k80"},
						Count:            1,
						PhysicalGPUCount: 1,
					},
					pods: []*apiv1.Pod{oneK80PredicatesFailingCentral1aOnly},
				},
			},
			overrideAPLocations: []string{"us-central1-a", "us-central1-b"},
		},
		{
			name:           "pods requesting GPU through ComputeClass with single GPU rule",
			pods:           []*apiv1.Pod{gpuComputeClassPod},
			computeClasses: []computeclass.CRD{singleGpuCC},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config: machinetypes.GpuConfig{
							GpuType: "nvidia-tesla-a100",
						},
						Count:            2,
						PhysicalGPUCount: 2,
					},
					pods: []*apiv1.Pod{gpuComputeClassPod},
				},
			},
		},
		{
			name:           "pods requesting GPU through ComputeClass with multiple GPU rules",
			pods:           []*apiv1.Pod{gpuComputeClassPod},
			computeClasses: []computeclass.CRD{multipleGPUCC},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config: machinetypes.GpuConfig{
							GpuType: "nvidia-tesla-a100",
						},
						Count:            2,
						PhysicalGPUCount: 2,
					},
					pods: []*apiv1.Pod{gpuComputeClassPod},
				},
				{
					gpuRequest: machinetypes.GpuRequest{
						Config: machinetypes.GpuConfig{
							GpuType: "nvidia-h100-mega-80gb",
						},
						Count:            8,
						PhysicalGPUCount: 8,
					},
					pods: []*apiv1.Pod{gpuComputeClassPod},
				},
			},
		},
		{
			name:           "pods requesting GPU through ComputeClass with ScaleUpAnyway",
			pods:           []*apiv1.Pod{gpuComputeClassPod},
			computeClasses: []computeclass.CRD{scaleUpAnywayGPUCC},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config: machinetypes.GpuConfig{
							GpuType: "nvidia-tesla-a100",
						},
						Count:            2,
						PhysicalGPUCount: 2,
					},
					pods: []*apiv1.Pod{gpuComputeClassPod},
				},
			},
		},
		{
			name: "all you can get",
			pods: []*apiv1.Pod{noGpuPod1, oneK80Pod, twoP100Pod, fiveAnyGpuPod, partitioningTimeSharing, unknownPlatformK80, limitNotDefined, unsupportedGpu, invalidPartitioning, oneK80PredicatesFailing, oneK80PredicatesFailingCentral1aOnly},
			expectedRequirements: []nodeGroupRequirements{
				{
					gpuRequest: machinetypes.GpuRequest{
						Config:           machinetypes.GpuConfig{GpuType: "nvidia-tesla-k80"},
						Count:            1,
						PhysicalGPUCount: 1,
					},
					pods: []*apiv1.Pod{oneK80Pod, oneK80PredicatesFailingCentral1aOnly},
				},
				{
					gpuRequest: machinetypes.GpuRequest{
						Config:           machinetypes.GpuConfig{GpuType: "nvidia-tesla-p100"},
						Count:            2,
						PhysicalGPUCount: 2,
					},
					pods: []*apiv1.Pod{twoP100Pod},
				},
				{
					gpuRequest: machinetypes.GpuRequest{
						Config: machinetypes.GpuConfig{
							GpuType:          "nvidia-tesla-a100",
							PartitionSize:    "1g.5gb",
							MaxSharedClients: "3",
							SharingStrategy:  labels.GPUTimeSharingStrategy,
						},
						Count:            42,
						PhysicalGPUCount: 2,
					},
					pods: []*apiv1.Pod{partitioningTimeSharing},
				},
			},
			expectedPodStatuses: map[types.UID]PodProcessingStatus{
				"unknownPlatformK80":      {Err: machineselection.NewMinCpuPlatformUnknownError("unknown")},
				"UnsupportedGpu":          {Err: NewGpuTypeNotSupportedError("unsupported-gpu")},
				"LimitNotDefined":         {Err: NewGpuTypeNoLimitDefinedError(machinetypes.NvidiaTeslaP4.Name())},
				"invalidPartitioning":     {Err: NewGpuRequestInvalidError("")},
				"OneK80PredicatesFailing": {Err: NewGpuRequestFailingPredicatesError([]string{"node(s) didn't match Pod's node affinity/selector"})},
				fiveAnyGpuPod.UID:         {Err: NewGpuRequestInvalidError("GPU type is not specified")},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			limits := defaultLimits
			if tc.overrideLimits != nil {
				limits = tc.overrideLimits
			}
			apLocations := []string{"us-central1-a", "us-central1-b"}
			if len(tc.overrideAPLocations) > 0 {
				apLocations = tc.overrideAPLocations
			}
			allZones := []string{"us-central1-a", "us-central1-b", "us-central1-c"}
			allMachineTypes := []string{}
			for machineType := range machinetypes.N1.AllMachineTypes(machinetypes.NoConstraints) {
				allMachineTypes = append(allMachineTypes, machineType)
			}
			for machineType := range machinetypes.A2.AllMachineTypes(machinetypes.NoConstraints) {
				allMachineTypes = append(allMachineTypes, machineType)
			}
			for machineType := range machinetypes.A3.AllMachineTypes(machinetypes.NoConstraints) {
				allMachineTypes = append(allMachineTypes, machineType)
			}
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithAutoprovisioningEnabled(true).
				WithAutoprovisioningLocations(apLocations...).
				WithAllZones(allZones...).
				WithMachineTypesPerZone(map[string][]string{
					"us-central1-a": allMachineTypes,
					"us-central1-b": allMachineTypes,
					"us-central1-c": allMachineTypes,
				}).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			lister := computeclass_lister.NewMockCrdLister(tc.computeClasses)
			lister.SetCrdLabel(labels.ComputeClassLabel)
			em := experiments.NewMockManager()
			opts := AutoprovisioningNodeGroupManagerOptions{CloudProvider: provider, Lister: lister, Flags: AutoprovisioningNodeGroupManagerFlags{}, ExperimentsManager: em, OptionsTracker: tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em), ResourcePolicyPuller: &placement.FakeResourcePolicyPullerProvider{}}
			manager := NewAutoprovisioningNodeGroupManager(opts)
			ctx := &injectionContext{
				status:          NewProcessingStatus(),
				zones:           allZones,
				resourceLimiter: cloudprovider.NewResourceLimiter(nil, limits),
				clusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t),
			}

			gotRequirements := manager.gpuPodsRequirements(ctx, tc.pods)
			if diff := cmp.Diff(tc.expectedRequirements, gotRequirements, requirementsIgnoreOrderOpt, requirementsCompareOnlyPodsAndGpuOpt, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("expected requirements differ (only pods and gpuRequests are compared), diff (-want +got):\n%s", diff)
			}
			selErrCmp := cmp.Comparer(func(e1, e2 *machineselection.Error) bool {
				if e1.Type() == machineselection.GpuIncompatibleError {
					gpuNamesMatch := e1.GpuName == e2.GpuName
					// ANY_GPU_SPECIAL_VAL matches anything.
					if e1.GpuName == ANY_GPU_SPECIAL_VAL || e2.GpuName == ANY_GPU_SPECIAL_VAL {
						gpuNamesMatch = true
					}
					return e1.MachineGroupName == e2.MachineGroupName && gpuNamesMatch
				}
				return e1 == e2 || cmp.Equal(e1, e2)
			})
			napErrCmp := cmp.Comparer(func(e1, e2 *Error) bool {
				if e1.Type() == GpuRequestInvalidError {
					// Disregard the reason while comparing GpuRequestInvalidError errors, since the exact wording is not likely to be stable.
					return e2.Type() == GpuRequestInvalidError
				}
				return e1 == e2 || cmp.Equal(e1, e2)
			})
			if diff := cmp.Diff(tc.expectedPodStatuses, ctx.status.PodStatuses, selErrCmp, napErrCmp, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("expected pod statuses differ, diff (-want + got):\n%s", diff)
			}
		})
	}
}

func TestGpuRequestSignature(t *testing.T) {
	for tn, tc := range map[string]struct {
		request  machinetypes.GpuRequest
		expected string
	}{
		"empty request": {
			request:  machinetypes.GpuRequest{},
			expected: `type: "", partition: "", count: 0, physicalCount: 0, maxSharedClients: "", sharingStrategy: "", driverVersion: ""`,
		},
		"no partition size specified": {
			request: machinetypes.GpuRequest{
				Config:           machinetypes.GpuConfig{GpuType: "gpu-type-1"},
				Count:            3,
				PhysicalGPUCount: 3,
			},
			expected: `type: "gpu-type-1", partition: "", count: 3, physicalCount: 3, maxSharedClients: "", sharingStrategy: "", driverVersion: ""`,
		},
		"everything specified": {
			request: machinetypes.GpuRequest{
				Config:           machinetypes.GpuConfig{GpuType: "gpu-type-1", PartitionSize: "part-size-1", MaxSharedClients: "5", SharingStrategy: "time-sharing", DriverVersion: "default"},
				Count:            3,
				PhysicalGPUCount: 3,
			},
			expected: `type: "gpu-type-1", partition: "part-size-1", count: 3, physicalCount: 3, maxSharedClients: "5", sharingStrategy: "time-sharing", driverVersion: "default"`,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.request.Signature())
		})
	}
}

func TestAutoprovisioningNodeGroupManager_maxCpuMachineForGpuRequest(t *testing.T) {
	for tn, tc := range map[string]struct {
		gpuRequest  machinetypes.GpuRequest
		machineSpec machinetypes.MachineSpec
		wantMachine string
	}{
		"A100 - only 1 machine type is ever compatible with a given GPU count": {
			gpuRequest:  machinetypes.GpuRequest{Count: 1, PhysicalGPUCount: 1, Config: machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaA100.Name()}},
			machineSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.A2, machinetypes.AnyPlatform, machinetypes.NvidiaTeslaA100.Name(), ""),
			wantMachine: "a2-highgpu-1g",
		},
		"A100 with partitioning and time-sharing": {
			gpuRequest:  machinetypes.GpuRequest{Count: 12, PhysicalGPUCount: 12, Config: machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaA100.Name(), PartitionSize: "2g.10gb", MaxSharedClients: "2"}},
			machineSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.A2, machinetypes.AnyPlatform, machinetypes.NvidiaTeslaA100.Name(), ""),
			wantMachine: "a2-highgpu-2g",
		},
		"8-GPU A100 - corner case with a2-highgpu-8g and a2-megagpu-16g having the same amount of CPU": {
			gpuRequest:  machinetypes.GpuRequest{Count: 8, PhysicalGPUCount: 8, Config: machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaA100.Name()}},
			machineSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.A2, machinetypes.AnyPlatform, machinetypes.NvidiaTeslaA100.Name(), ""),
			wantMachine: "a2-highgpu-8g",
		},
		"16-GPU A100 - corner case with a2-highgpu-8g and a2-megagpu-16g having the same amount of CPU": {
			gpuRequest:  machinetypes.GpuRequest{Count: 16, PhysicalGPUCount: 16, Config: machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaA100.Name()}},
			machineSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.A2, machinetypes.AnyPlatform, machinetypes.NvidiaTeslaA100.Name(), ""),
			wantMachine: "a2-megagpu-16g",
		},
		"K80 GPU - among the machines with maximum CPU highmem is chosen (as it has the highest memory)": {
			gpuRequest:  machinetypes.GpuRequest{Count: 2, PhysicalGPUCount: 2, Config: machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaK80.Name()}},
			machineSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.N1, machinetypes.AnyPlatform, machinetypes.NvidiaTeslaK80.Name(), ""),
			wantMachine: "n1-highmem-16",
		},
		"T4 GPU - custom machine can be chosen as the max one": {
			gpuRequest:  machinetypes.GpuRequest{Count: 2, PhysicalGPUCount: 2, Config: machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaT4.Name()}},
			machineSpec: machinetypes.NewMachineSpecSingleFamily(machinetypes.N1, machinetypes.AnyPlatform, machinetypes.NvidiaTeslaT4.Name(), ""),
			wantMachine: "custom-48-319488",
		},
	} {
		t.Run(tn, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			em := experiments.NewMockManager()
			opts := AutoprovisioningNodeGroupManagerOptions{
				CloudProvider:        provider,
				ResourcePolicyPuller: &placement.FakeResourcePolicyPullerProvider{},
				OptionsTracker:       tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
				ExperimentsManager:   em,
			}
			manager := NewAutoprovisioningNodeGroupManager(opts)
			gotMachine, _ := manager.maxCpuMachineForGpuRequest(tc.gpuRequest, tc.machineSpec)
			if diff := cmp.Diff(tc.wantMachine, gotMachine, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("maxCpuMachineForGpuRequest diff (-want +got):\n%s", diff)
			}
		})
	}
}

type mockCloudProvider struct {
	migsForNodes map[string]*gke.GkeMig
}

func (p *mockCloudProvider) GkeMigForNode(node *apiv1.Node) (*gke.GkeMig, error) {
	return p.migsForNodes[node.Name], nil
}

func TestAutoprovisioningNodeGroupManager_gpuRequestsForPod(t *testing.T) {
	defaultLimits := map[string]int64{
		machinetypes.NvidiaTeslaK80.Name():  16,
		machinetypes.NvidiaTeslaV100.Name(): 16,
		machinetypes.NvidiaTeslaA100.Name(): 16,
	}
	gpuSpecA100 := machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaA100.Name()}
	gpuSpecV100 := machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaV100.Name()}
	gpuSpecK80 := machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaK80.Name()}
	gpuSpecT4 := machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaT4.Name()}

	// Mock reservations
	res1 := reservations.BuildReservation("READY",
		true,
		"us-central1-c",
		"n1-standard-4",
		"Automatic",
		reservations.BuildReservationAccelerators(machinetypes.NvidiaTeslaK80.Name(), 1),
		nil)
	res1.Name = "res1"
	res1.SelfLink = "https://www.googleapis.com/compute/v1/projects/res-proj/zones/us-central1-c/reservations/res1"
	mGceClient := gceclient.BuildAutoscalingInternalGceClientMock().
		WithFetchZones(func(region string) ([]string, error) { return []string{"us-central1-c"}, nil })
	mockReservationPuller, err := gceclient.NewReservationsPuller(mGceClient, nil, nil, "res-proj", false, "us-central1")
	assert.NoError(t, err)

	tests := []struct {
		name               string
		pod                *apiv1.Pod
		consideredGpuTypes []string
		driverVersion      string
		partitionSize      string
		maxSharedClients   string
		sharingStrategy    string
		gpuCount           resource.Quantity
		cpuCount           resource.Quantity
		memCount           resource.Quantity
		expectedRequests   []machinetypes.GpuRequest
		extendedDuration   bool
		reservationEnabled bool
		reservations       []*gce_api.Reservation
	}{
		{
			name:               "1 a100 GPU request",
			pod:                buildPodWithGpuCpusAndMem("oneA100Pod", machinetypes.NvidiaTeslaA100.Name(), 1, 1000, 1*units.GiB),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaA100.Name()},
			gpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			memCount:           *resource.NewQuantity(1*units.GiB, resource.DecimalSI),
			expectedRequests: []machinetypes.GpuRequest{
				{Config: gpuSpecA100, Count: 1, PhysicalGPUCount: 1},
			},
		},
		{
			name:               "1 a100 GPU request with high memory (not fitting with 1 GPU, but OK - just needs 2 GPUs)",
			pod:                buildPodWithGpuCpusAndMem("oneA100PodVeryHighMemory", machinetypes.NvidiaTeslaA100.Name(), 1, 1000, 96*units.GiB),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaA100.Name()},
			gpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			memCount:           *resource.NewQuantity(96*units.GiB, resource.DecimalSI),
			expectedRequests: []machinetypes.GpuRequest{
				{Config: gpuSpecA100, Count: 2, PhysicalGPUCount: 2},
			},
		},
		{
			name:               "1 a100 GPU request with significant number of CPUs",
			pod:                buildPodWithGpuCpusAndMem("oneA100PodCpus", machinetypes.NvidiaTeslaA100.Name(), 1, 8000, 1*units.GiB),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaA100.Name()},
			gpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(8, resource.DecimalSI),
			memCount:           *resource.NewQuantity(1*units.GiB, resource.DecimalSI),
			expectedRequests: []machinetypes.GpuRequest{
				{Config: gpuSpecA100, Count: 1, PhysicalGPUCount: 1},
			},
		},
		{
			name:               "1 a100 GPU request with many CPUs",
			pod:                buildPodWithGpuCpusAndMem("oneA100PodManyCpus", machinetypes.NvidiaTeslaA100.Name(), 1, 16000, 1*units.GiB),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaA100.Name()},
			gpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(16, resource.DecimalSI),
			memCount:           *resource.NewQuantity(1*units.GiB, resource.DecimalSI),
			expectedRequests: []machinetypes.GpuRequest{
				{Config: gpuSpecA100, Count: 2, PhysicalGPUCount: 2},
			},
		},
		{
			name:               "1 a100 GPU request with many CPUs and high memory",
			pod:                buildPodWithGpuCpusAndMem("oneA100PodManyCpusHighMem", machinetypes.NvidiaTeslaA100.Name(), 1, 16000, 96*units.GiB),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaA100.Name()},
			gpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(16, resource.DecimalSI),
			memCount:           *resource.NewQuantity(96*units.GiB, resource.DecimalSI),
			expectedRequests: []machinetypes.GpuRequest{
				{Config: gpuSpecA100, Count: 2, PhysicalGPUCount: 2},
			},
		},
		{
			name:               "4 a100 GPU request with many CPUs and high memory",
			pod:                buildPodWithGpuCpusAndMem("oneA100Pod4GPUs", machinetypes.NvidiaTeslaA100.Name(), 4, 16000, 96*units.GiB),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaA100.Name()},
			gpuCount:           *resource.NewQuantity(4, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(16, resource.DecimalSI),
			memCount:           *resource.NewQuantity(96*units.GiB, resource.DecimalSI),
			expectedRequests: []machinetypes.GpuRequest{
				{Config: gpuSpecA100, Count: 4, PhysicalGPUCount: 4},
			},
		},
		{
			name:               "7 a100 GPU request with many CPUs",
			pod:                buildPodWithGpuCpusAndMem("oneA100Pod7GPUs", machinetypes.NvidiaTeslaA100.Name(), 7, 40000, 1*units.GiB),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaA100.Name()},
			gpuCount:           *resource.NewQuantity(7, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(40, resource.DecimalSI),
			memCount:           *resource.NewQuantity(1*units.GiB, resource.DecimalSI),
			expectedRequests: []machinetypes.GpuRequest{
				{Config: gpuSpecA100, Count: 8, PhysicalGPUCount: 8},
			},
		},
		{
			name:               "12 a100 GPU request with very high memory",
			pod:                buildPodWithGpuCpusAndMem("oneA100Pod12GPUs", machinetypes.NvidiaTeslaA100.Name(), 12, 8000, 600*units.GiB),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaA100.Name()},
			gpuCount:           *resource.NewQuantity(12, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(8, resource.DecimalSI),
			memCount:           *resource.NewQuantity(600*units.GiB, resource.DecimalSI),
			expectedRequests: []machinetypes.GpuRequest{
				{Config: gpuSpecA100, Count: 16, PhysicalGPUCount: 16},
			},
		},
		{
			name:               "1 v100 GPU request",
			pod:                buildPodWithGpuCpusAndMem("oneV100Pod", machinetypes.NvidiaTeslaV100.Name(), 1, 1000, 1*units.GiB),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaV100.Name()},
			gpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			memCount:           *resource.NewQuantity(1*units.GiB, resource.DecimalSI),
			expectedRequests: []machinetypes.GpuRequest{
				{Config: gpuSpecV100, Count: 1, PhysicalGPUCount: 1},
			},
		},
		{
			name:               "1 v100 GPU request with high memory (not fitting with 1 GPU, but OK - just needs 2 GPUs)",
			pod:                buildPodWithGpuCpusAndMem("oneV100PodVeryHighMemory", machinetypes.NvidiaTeslaV100.Name(), 1, 1000, 60*units.GiB),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaV100.Name()},
			gpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			memCount:           *resource.NewQuantity(60*units.GiB, resource.DecimalSI),
			expectedRequests: []machinetypes.GpuRequest{
				{Config: gpuSpecV100, Count: 2, PhysicalGPUCount: 2},
			},
		},
		{
			name:               "1 v100 GPU request with 7 CPUs",
			pod:                buildPodWithGpuCpusAndMem("oneV100Pod7CPUs", machinetypes.NvidiaTeslaV100.Name(), 1, 7000, 1*units.GiB),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaV100.Name()},
			gpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(7, resource.DecimalSI),
			memCount:           *resource.NewQuantity(1*units.GiB, resource.DecimalSI),
			expectedRequests: []machinetypes.GpuRequest{
				{Config: gpuSpecV100, Count: 1, PhysicalGPUCount: 1},
			},
		},
		{
			name:               "1 v100 GPU request with 12 CPUs",
			pod:                buildPodWithGpuCpusAndMem("oneV100Pod12CPUs", machinetypes.NvidiaTeslaV100.Name(), 1, 12000, 1*units.GiB),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaV100.Name()},
			gpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(12, resource.DecimalSI),
			memCount:           *resource.NewQuantity(1*units.GiB, resource.DecimalSI),
			expectedRequests: []machinetypes.GpuRequest{
				{Config: gpuSpecV100, Count: 2, PhysicalGPUCount: 2},
			},
		},
		{
			name:               "1 v100 GPU request with many CPUs and high memory",
			pod:                buildPodWithGpuCpusAndMem("oneV100Pod12CPUs60Mem", machinetypes.NvidiaTeslaV100.Name(), 1, 12000, 60*units.GiB),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaV100.Name()},
			gpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(12, resource.DecimalSI),
			memCount:           *resource.NewQuantity(60*units.GiB, resource.DecimalSI),
			expectedRequests: []machinetypes.GpuRequest{
				{Config: gpuSpecV100, Count: 2, PhysicalGPUCount: 2},
			},
		},
		{
			name:               "1 k80 GPU request",
			pod:                buildPodWithGpuCpusAndMem("oneK80Pod", machinetypes.NvidiaTeslaK80.Name(), 1, 1000, 1*units.GiB),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaK80.Name()},
			gpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			memCount:           *resource.NewQuantity(1*units.GiB, resource.DecimalSI),
			expectedRequests: []machinetypes.GpuRequest{
				{Config: gpuSpecK80, Count: 1, PhysicalGPUCount: 1},
			},
		},
		{
			name:               "1 k80 GPU request with many CPUs",
			pod:                buildPodWithGpuCpusAndMem("oneK80PodManyCPUs", machinetypes.NvidiaTeslaK80.Name(), 1, 12000, 1*units.GiB),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaK80.Name()},
			gpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(12, resource.DecimalSI),
			memCount:           *resource.NewQuantity(1*units.GiB, resource.DecimalSI),
			expectedRequests: []machinetypes.GpuRequest{
				{Config: gpuSpecK80, Count: 2, PhysicalGPUCount: 2},
			},
		},
		{
			name:               "1 k80 GPU request with high memory (not fitting with 1 GPU, but OK - just needs 2 GPUs)",
			pod:                buildPodWithGpuCpusAndMem("oneK80PodVeryHighMemory", machinetypes.NvidiaTeslaK80.Name(), 1, 1000, 54*units.GiB),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaK80.Name()},
			gpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			memCount:           *resource.NewQuantity(54*units.GiB, resource.DecimalSI),
			expectedRequests: []machinetypes.GpuRequest{
				{Config: gpuSpecK80, Count: 2, PhysicalGPUCount: 2},
			},
		},
		{
			name:               "1 k80 GPU request with high memory (fitting with 1 GPU)",
			pod:                buildPodWithGpuCpusAndMem("oneK80PodHighMemory", machinetypes.NvidiaTeslaK80.Name(), 1, 1000, 32*units.GiB),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaK80.Name()},
			gpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			memCount:           *resource.NewQuantity(32*units.GiB, resource.DecimalSI),
			expectedRequests: []machinetypes.GpuRequest{
				{Config: gpuSpecK80, Count: 1, PhysicalGPUCount: 1},
			},
		},
		{
			name:               "a100 GPU request with partitioning",
			pod:                addGpuPartitioning(buildGpuPod("oneA100PodPartitioning", machinetypes.NvidiaTeslaA100.Name(), 8), "1g.5gb"),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaA100.Name()},
			gpuCount:           *resource.NewQuantity(8, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(0, resource.DecimalSI),
			memCount:           *resource.NewQuantity(1000, resource.DecimalSI),
			partitionSize:      "1g.5gb",
			expectedRequests: []machinetypes.GpuRequest{
				{Config: machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaA100.Name(), PartitionSize: "1g.5gb"}, Count: 14, PhysicalGPUCount: 2},
			},
		},
		{
			name:               "a100 GPU request with partitioning and very high memory",
			pod:                addGpuPartitioning(buildPodWithGpuCpusAndMem("oneA100PodPartitioningAndVeryHighMemory", machinetypes.NvidiaTeslaA100.Name(), 1, 1000, 96*units.GiB), "1g.5gb"),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaA100.Name()},
			gpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			memCount:           *resource.NewQuantity(96*units.GiB, resource.DecimalSI),
			partitionSize:      "1g.5gb",
			expectedRequests: []machinetypes.GpuRequest{
				{Config: machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaA100.Name(), PartitionSize: "1g.5gb"}, Count: 14, PhysicalGPUCount: 2},
			},
		},
		{
			name:               "a100 GPU request with time sharing",
			pod:                addGpuTimeSharing(buildGpuPod("oneA100PodTimeSharing", machinetypes.NvidiaTeslaA100.Name(), 7), "3", labels.GPUTimeSharingStrategy),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaA100.Name()},
			gpuCount:           *resource.NewQuantity(7, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(0, resource.DecimalSI),
			memCount:           *resource.NewQuantity(1000, resource.DecimalSI),
			sharingStrategy:    labels.GPUTimeSharingStrategy,
			maxSharedClients:   "3",
			expectedRequests: []machinetypes.GpuRequest{
				{Config: machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaA100.Name(), SharingStrategy: labels.GPUTimeSharingStrategy, MaxSharedClients: "3"}, Count: 12, PhysicalGPUCount: 4},
			},
		},
		{
			name:               "v100 GPU request with time sharing",
			pod:                addGpuTimeSharing(buildGpuPod("oneV100PodTimeSharing", machinetypes.NvidiaTeslaV100.Name(), 11), "3", labels.GPUTimeSharingStrategy),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaV100.Name()},
			gpuCount:           *resource.NewQuantity(11, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(0, resource.DecimalSI),
			memCount:           *resource.NewQuantity(1000, resource.DecimalSI),
			sharingStrategy:    labels.GPUTimeSharingStrategy,
			maxSharedClients:   "3",
			expectedRequests: []machinetypes.GpuRequest{
				{Config: machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaV100.Name(), SharingStrategy: labels.GPUTimeSharingStrategy, MaxSharedClients: "3"}, Count: 12, PhysicalGPUCount: 4},
			},
		},
		{
			name:               "a100 GPU request with partitioning and time sharing",
			pod:                addGpuTimeSharing(addGpuPartitioning(buildGpuPod("oneA100PodPartitioningAndTimeSharing", machinetypes.NvidiaTeslaA100.Name(), 7), "1g.5gb"), "3", labels.GPUTimeSharingStrategy),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaA100.Name()},
			gpuCount:           *resource.NewQuantity(7, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(0, resource.DecimalSI),
			memCount:           *resource.NewQuantity(1000, resource.DecimalSI),
			partitionSize:      "1g.5gb",
			sharingStrategy:    labels.GPUTimeSharingStrategy,
			maxSharedClients:   "3",
			expectedRequests: []machinetypes.GpuRequest{
				{Config: machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaA100.Name(), PartitionSize: "1g.5gb", SharingStrategy: labels.GPUTimeSharingStrategy, MaxSharedClients: "3"}, Count: 7 * 3 * 1, PhysicalGPUCount: 1},
			},
		},
		{
			name:               "a100 GPU request with partitioning, time sharing and very high memory",
			pod:                addGpuTimeSharing(addGpuPartitioning(buildPodWithGpuCpusAndMem("oneA100PodPartitioningTimeSharingVeryHighMem", machinetypes.NvidiaTeslaA100.Name(), 1, 1000, 96*units.GiB), "1g.5gb"), "3", labels.GPUTimeSharingStrategy),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaA100.Name()},
			gpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			memCount:           *resource.NewQuantity(96*units.GiB, resource.DecimalSI),
			partitionSize:      "1g.5gb",
			sharingStrategy:    labels.GPUTimeSharingStrategy,
			maxSharedClients:   "3",
			expectedRequests: []machinetypes.GpuRequest{
				{Config: machinetypes.GpuConfig{GpuType: machinetypes.NvidiaTeslaA100.Name(), PartitionSize: "1g.5gb", SharingStrategy: labels.GPUTimeSharingStrategy, MaxSharedClients: "3"}, Count: 7 * 3 * 2, PhysicalGPUCount: 2},
			},
		},
		{
			name:               "1 k80 GPU request with preemption",
			pod:                addSpotToleration(buildPodWithGpuCpusAndMem("oneK80Pod", machinetypes.NvidiaTeslaK80.Name(), 1, 1000, 1*units.GiB)),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaK80.Name()},
			gpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			memCount:           *resource.NewQuantity(1*units.GiB, resource.DecimalSI),
			expectedRequests: []machinetypes.GpuRequest{
				{Config: gpuSpecK80, Count: 1, PhysicalGPUCount: 1},
			},
		},
		{
			name:               "1 k80 GPU request with extended duration pod",
			pod:                addExtendedDurationPodLabel(buildPodWithGpuCpusAndMem("oneK80Pod", machinetypes.NvidiaTeslaK80.Name(), 1, 1000, 1*units.GiB), "1000m"),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaK80.Name()},
			gpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			memCount:           *resource.NewQuantity(1*units.GiB, resource.DecimalSI),
			extendedDuration:   true,
			expectedRequests: []machinetypes.GpuRequest{
				{Config: gpuSpecK80, Count: 1, PhysicalGPUCount: 1},
			},
		},
		{
			name:               "1 k80 GPU request with reservation pod",
			pod:                addReservationLabels(buildPodWithGpuCpusAndMem("oneK80Pod", machinetypes.NvidiaTeslaK80.Name(), 1, 1000, 1*units.GiB), "res1", "res-proj"),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaK80.Name()},
			gpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			memCount:           *resource.NewQuantity(1*units.GiB, resource.DecimalSI),
			reservationEnabled: true,
			reservations:       []*gce_api.Reservation{res1},
			expectedRequests: []machinetypes.GpuRequest{
				{Config: gpuSpecK80, Count: 1, PhysicalGPUCount: 1},
			},
		},
		{
			name:               "1 t4 GPU request with instanceType node selector pod",
			pod:                addInstanceType(buildPodWithGpuCpusAndMem("oneT4", machinetypes.NvidiaTeslaT4.Name(), 1, 1000, 1*units.GiB), "n1-standard-4"),
			consideredGpuTypes: []string{machinetypes.NvidiaTeslaT4.Name()},
			gpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			memCount:           *resource.NewQuantity(1*units.GiB, resource.DecimalSI),
			expectedRequests: []machinetypes.GpuRequest{
				{Config: gpuSpecT4, Count: 1, PhysicalGPUCount: 1},
			},
		},
		{
			name:               "a3 ultra GPU request, latest driver version",
			pod:                buildPodWithGpuCpusAndMem("a3ultraPod", machinetypes.NvidiaH200Ultra_141gb.Name(), 8, 1000, 1*units.GiB),
			consideredGpuTypes: []string{machinetypes.NvidiaH200Ultra_141gb.Name()},
			driverVersion:      "latest",
			gpuCount:           *resource.NewQuantity(8, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			memCount:           *resource.NewQuantity(1*units.GiB, resource.DecimalSI),
			expectedRequests: []machinetypes.GpuRequest{
				{Config: machinetypes.GpuConfig{GpuType: machinetypes.NvidiaH200Ultra_141gb.Name(), DriverVersion: "latest"}, Count: 8, PhysicalGPUCount: 8},
			},
		},
		{
			name:               "a3 ultra GPU request, missing driver version",
			pod:                buildPodWithGpuCpusAndMem("a3ultraPod", machinetypes.NvidiaH200Ultra_141gb.Name(), 8, 1000, 1*units.GiB),
			consideredGpuTypes: []string{machinetypes.NvidiaH200Ultra_141gb.Name()},
			gpuCount:           *resource.NewQuantity(8, resource.DecimalSI),
			cpuCount:           *resource.NewQuantity(1, resource.DecimalSI),
			memCount:           *resource.NewQuantity(1*units.GiB, resource.DecimalSI),
			expectedRequests: []machinetypes.GpuRequest{
				{Config: machinetypes.GpuConfig{GpuType: machinetypes.NvidiaH200Ultra_141gb.Name()}, Count: 8, PhysicalGPUCount: 8},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			limits := defaultLimits
			apLocations := []string{"us-central1-c"}
			allZones := []string{"us-central1-a", "us-central1-b", "us-central1-c"}
			allMachineTypes := []string{}
			for machineType := range machinetypes.A2.AllMachineTypes(machinetypes.NoConstraints) {
				allMachineTypes = append(allMachineTypes, machineType)
			}
			for machineType := range machinetypes.A3.AllMachineTypes(machinetypes.NoConstraints) {
				allMachineTypes = append(allMachineTypes, machineType)
			}
			for machineType := range machinetypes.N1.AllMachineTypes(machinetypes.NoConstraints) {
				allMachineTypes = append(allMachineTypes, machineType)
			}
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithAutoprovisioningEnabled(true).
				WithAutopilotEnabled(tc.extendedDuration).
				WithAutoprovisioningLocations(apLocations...).
				WithAllZones(allZones...).
				WithMachineTypesPerZone(map[string][]string{
					"us-central1-a": allMachineTypes,
					"us-central1-b": allMachineTypes,
					"us-central1-c": allMachineTypes,
				}).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			debuggingSnapshotter, _ := gkedebuggingsnapshot.NewGkeDebuggingSnapshotter(true)
			processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
			processor.SetContext(&context.AutoscalingContext{CloudProvider: provider, DebuggingSnapshotter: debuggingSnapshotter})
			computeClassLister := computeclass_lister.NewMockCrdLister([]computeclass.CRD{})
			nodeGroupBackoff := gke_backoff.NewGkeBackoff(gke_backoff.Config{CustomResourceProcessor: processor, NpcLister: computeClassLister})

			mockProvider := &mockCloudProvider{
				migsForNodes: map[string]*gke.GkeMig{},
			}
			scaleBlockingProcessor := scaleblocking.NewProcessor(mockProvider, []scaleblocking.BlockedMigsSource{})
			em := experiments.NewMockManager()
			mockReservationPuller.SetReservations(tc.reservations)
			opts := AutoprovisioningNodeGroupManagerOptions{
				CloudProvider:          provider,
				Backoff:                nodeGroupBackoff,
				ScaleBlockingProcessor: scaleBlockingProcessor,
				ReservationsPuller:     mockReservationPuller,
				Flags: AutoprovisioningNodeGroupManagerFlags{
					ReservationFlags: ReservationFlags{
						SpecificTypeReservationMatchEnabled: tc.reservationEnabled,
						SpecificTypeReservationsEnabled:     tc.reservationEnabled,
					},
				},
				PodLister:            kubernetes.NewTestPodLister([]*apiv1.Pod{tc.pod}),
				Lister:               computeClassLister,
				ExperimentsManager:   em,
				OptionsTracker:       tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
				ResourcePolicyPuller: &placement.FakeResourcePolicyPullerProvider{},
			}
			m := NewAutoprovisioningNodeGroupManager(opts)
			ctx := &injectionContext{
				status:          NewProcessingStatus(),
				zones:           allZones,
				resourceLimiter: cloudprovider.NewResourceLimiter(nil, limits),
				clusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t),
			}
			gpuConfig := machinetypes.GpuConfig{
				PartitionSize:    tc.partitionSize,
				MaxSharedClients: tc.maxSharedClients,
				SharingStrategy:  tc.sharingStrategy,
				DriverVersion:    tc.driverVersion,
			}
			gpuRequests, err := m.gpuRequestsForPod(ctx, tc.pod, tc.consideredGpuTypes, gpuConfig, tc.gpuCount, tc.cpuCount, tc.memCount)

			if err != nil {
				t.Errorf("AutoprovisioningNodeGroupManager.gpuRequestsForPod() unexpected pod error:\n%s", err)
			}
			assert.ElementsMatch(t, tc.expectedRequests, gpuRequests)
		})
	}
}
