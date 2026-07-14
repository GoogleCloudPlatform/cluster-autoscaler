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

package gke

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	gce_api "google.golang.org/api/compute/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	quota "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	gpuUtils "k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	networkingutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/networking/util"
)

func TestBuildNodeFromTemplateSetsResources(t *testing.T) {
	var thirtyPodsPerNode int64 = 30
	type testCase struct {
		scenario         string
		kubeEnv          string
		accelerators     []*gce_api.AcceleratorConfig
		physicalCpu      int64
		physicalMemory   int64
		kubeReserved     bool
		reservedCpu      string
		reservedMemory   string
		pods             *int64
		expectedGpuCount int64
		expectedErr      bool
	}
	testCases := []testCase{
		{
			scenario: "kube-reserved present in kube-env",
			kubeEnv: "ENABLE_NODE_PROBLEM_DETECTOR: 'daemonset'\n" +
				"NODE_LABELS: a=b,c=d,cloud.google.com/gke-nodepool=pool-3,cloud.google.com/gke-preemptible=true\n" +
				"DNS_SERVER_IP: '10.0.0.10'\n" +
				fmt.Sprintf("KUBELET_TEST_ARGS: --experimental-allocatable-ignore-eviction --kube-reserved=cpu=1000m,memory=%v\n", 1*units.MiB) +
				"NODE_TAINTS: 'dedicated=ml:NoSchedule,test=dev:PreferNoSchedule,a=b:c'\n",
			accelerators: []*gce_api.AcceleratorConfig{
				{AcceleratorType: "nvidia-tesla-k80", AcceleratorCount: 3},
				{AcceleratorType: "nvidia-tesla-p100", AcceleratorCount: 8},
			},
			physicalCpu:      8,
			physicalMemory:   200 * units.MiB,
			kubeReserved:     true,
			reservedCpu:      "1000m",
			reservedMemory:   fmt.Sprintf("%v", 1*units.MiB),
			expectedGpuCount: 11,
		},
		{
			scenario: "no kube-reserved in kube-env",
			kubeEnv: "ENABLE_NODE_PROBLEM_DETECTOR: 'daemonset'\n" +
				"NODE_LABELS: a=b,c=d,cloud.google.com/gke-nodepool=pool-3,cloud.google.com/gke-preemptible=true\n" +
				"DNS_SERVER_IP: '10.0.0.10'\n" +
				"NODE_TAINTS: 'dedicated=ml:NoSchedule,test=dev:PreferNoSchedule,a=b:c'\n",
			physicalCpu:      8,
			physicalMemory:   200 * units.MiB,
			kubeReserved:     false,
			expectedGpuCount: 11,
		}, {
			scenario:    "totally messed up kube-env",
			kubeEnv:     "This kube-env is totally messed up",
			expectedErr: true,
		}, {
			scenario: "kube-reserved present in kube-env; thirtyMaxPods",
			kubeEnv: "ENABLE_NODE_PROBLEM_DETECTOR: 'daemonset'\n" +
				"NODE_LABELS: a=b,c=d,cloud.google.com/gke-nodepool=pool-3,cloud.google.com/gke-preemptible=true\n" +
				"DNS_SERVER_IP: '10.0.0.10'\n" +
				fmt.Sprintf("KUBELET_TEST_ARGS: --experimental-allocatable-ignore-eviction --kube-reserved=cpu=1000m,memory=%v\n", 1*units.MiB) +
				"NODE_TAINTS: 'dedicated=ml:NoSchedule,test=dev:PreferNoSchedule,a=b:c'\n",
			accelerators: []*gce_api.AcceleratorConfig{
				{AcceleratorType: "nvidia-tesla-k80", AcceleratorCount: 3},
				{AcceleratorType: "nvidia-tesla-p100", AcceleratorCount: 8},
			},
			physicalCpu:      8,
			physicalMemory:   200 * units.MiB,
			kubeReserved:     true,
			reservedCpu:      "1000m",
			reservedMemory:   fmt.Sprintf("%v", 1*units.MiB),
			pods:             &thirtyPodsPerNode,
			expectedGpuCount: 11,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.scenario, func(t *testing.T) {
			tb := &GkeTemplateBuilder{}
			mig := &GkeMig{
				gceRef: gce.GceRef{
					Name:    "some-name",
					Project: "some-proj",
					Zone:    "us-central1-b",
				},
			}
			template := &gce_api.InstanceTemplate{
				Name: "node-name",
				Properties: &gce_api.InstanceProperties{
					GuestAccelerators: tc.accelerators,
					Metadata: &gce_api.Metadata{
						Items: []*gce_api.MetadataItems{{Key: "kube-env", Value: &tc.kubeEnv}},
					},
					MachineType: "irrelevant-type",
					Disks: []*gce_api.AttachedDisk{
						{
							Boot: true,
							InitializeParams: &gce_api.AttachedDiskInitializeParams{
								DiskSizeGb: 0,
							},
						},
					},
				},
			}
			gkeMigOsInfo := NewGkeMigOsInfo(gce.NewMigOsInfo(gce.OperatingSystemLinux, gce.OperatingSystemDistributionCOS, ""), "", false)
			var node *apiv1.Node
			kubeEnv, err := gce.ExtractKubeEnv(template)
			if err == nil {
				node, err = tb.BuildNodeFromTemplate(mig, gkeMigOsInfo, template, kubeEnv, tc.physicalCpu, tc.physicalMemory, tc.pods, &gce.GceReserved{}, localssdsize.NewSimpleLocalSSDProvider())
			}
			evictionHard := gce.ParseEvictionHardOrGetDefault(nil)
			if tc.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				capacity, err := tb.BuildCapacity(gkeMigOsInfo, tc.physicalCpu, tc.physicalMemory, tc.accelerators, -1, -1, tc.pods, &gce.GceReserved{}, nil)
				assert.NoError(t, err)
				assertEqualResourceLists(t, "Capacity", capacity, node.Status.Capacity)
				if !tc.kubeReserved {
					assertEqualResourceLists(t, "Allocatable", capacity, node.Status.Allocatable)
				} else {
					reserved, err := makeResourceList(tc.reservedCpu, tc.reservedMemory, 0, "")
					assert.NoError(t, err)
					allocatable := tb.CalculateAllocatable(capacity, reserved, evictionHard)
					assertEqualResourceLists(t, "Allocatable", allocatable, node.Status.Allocatable)
				}
			}
		})
	}
}

func TestBuildLabelsForAutoprovisionedMigOK(t *testing.T) {
	arch := gce.DefaultArch
	labels, err := buildLabelsForAutoprovisionedMig(
		&GkeMig{
			gceRef: gce.GceRef{
				Name:    "kubernetes-minion-autoprovisioned-group",
				Project: "mwielgus-proj",
				Zone:    "us-central1-b",
			},
			autoprovisioned: true,
			spec: &gkeclient.NodePoolSpec{
				MachineType: "n1-standard-8",
				Labels: map[string]string{
					"A": "B",
				},
				SystemArchitecture: &arch,
			},
			gkeManager: &gkeManagerImpl{
				machineConfigProvider: machinetypes.NewMachineConfigProvider(nil),
			},
		},
		"sillyname",
		gce.OperatingSystemLinux,
		1,
		gkelabels.DefaultMaxPodsPerNode,
		false)

	assert.Nil(t, err)
	assert.Equal(t, "B", labels["A"])
	assert.Equal(t, "us-central1", labels[apiv1.LabelZoneRegion])
	assert.Equal(t, "us-central1-b", labels[apiv1.LabelZoneFailureDomain])
	assert.Equal(t, "sillyname", labels[apiv1.LabelHostname])
	assert.Equal(t, "n1-standard-8", labels[apiv1.LabelInstanceType])
	assert.Equal(t, cloudprovider.DefaultArch, labels[apiv1.LabelArchStable])
	assert.Equal(t, cloudprovider.DefaultOS, labels[apiv1.LabelOSStable])
	assert.Equal(t, "true", labels[gkelabels.EphemeralLocalSsdLabel])
	assert.Equal(t, "110", labels[gkelabels.MaxPodsPerNodeLabel])
}

func TestBuildLabelsForAutoprovisionedMigConflict(t *testing.T) {
	arch := gce.DefaultArch
	_, err := buildLabelsForAutoprovisionedMig(
		&GkeMig{
			gceRef: gce.GceRef{
				Name:    "kubernetes-minion-autoprovisioned-group",
				Project: "mwielgus-proj",
				Zone:    "us-central1-b",
			},
			autoprovisioned: true,
			spec: &gkeclient.NodePoolSpec{
				MachineType: "n1-standard-8",
				Labels: map[string]string{
					apiv1.LabelOSStable: "windows",
				},
				SystemArchitecture: &arch,
			},
			gkeManager: &gkeManagerImpl{
				machineConfigProvider: machinetypes.NewMachineConfigProvider(nil),
			},
		},
		"sillyname",
		gce.OperatingSystemLinux,
		0,
		gkelabels.DefaultMaxPodsPerNode,
		false)
	assert.Error(t, err)
}

func TestBuildAllocatableFromKubeEnv(t *testing.T) {
	type testCase struct {
		kubeEnvValue             string
		capacityCpu              string
		capacityMemory           string
		capacityEphemeralStorage string
		expectedCpu              string
		expectedMemory           string
		expectedEphemeralStorage string
		gpuCount                 int64
		expectedErr              bool
	}
	testCases := []testCase{{
		kubeEnvValue: "ENABLE_NODE_PROBLEM_DETECTOR: 'daemonset'\n" +
			"NODE_LABELS: a=b,c=d,cloud.google.com/gke-nodepool=pool-3,cloud.google.com/gke-preemptible=true\n" +
			"DNS_SERVER_IP: '10.0.0.10'\n" +
			"KUBELET_TEST_ARGS: --experimental-allocatable-ignore-eviction --kube-reserved=cpu=1000m,memory=300000Mi,ephemeral-storage=30Gi\n" +
			"NODE_TAINTS: 'dedicated=ml:NoSchedule,test=dev:PreferNoSchedule,a=b:c'\n",
		capacityCpu:              "4000m",
		capacityMemory:           "700000Mi",
		capacityEphemeralStorage: "100Gi",
		expectedCpu:              "3000m",
		expectedMemory:           "399900Mi", // capacityMemory-kube_reserved-DefaultKubeletEvictionHardMemory
		expectedEphemeralStorage: "60Gi",     // capacityEphemeralStorage-kube_reserved-DefaultKubeletEvictionHardMemory
		gpuCount:                 10,
		expectedErr:              false,
	}, {
		kubeEnvValue: "ENABLE_NODE_PROBLEM_DETECTOR: 'daemonset'\n" +
			"NODE_LABELS: a=b,c=d,cloud.google.com/gke-nodepool=pool-3,cloud.google.com/gke-preemptible=true\n" +
			"DNS_SERVER_IP: '10.0.0.10'\n" +
			"NODE_TAINTS: 'dedicated=ml:NoSchedule,test=dev:PreferNoSchedule,a=b:c'\n",
		capacityCpu:    "4000m",
		capacityMemory: "700000Mi",
		expectedErr:    true,
	}}
	for _, tc := range testCases {
		capacity, err := makeResourceList(tc.capacityCpu, tc.capacityMemory, tc.gpuCount, tc.capacityEphemeralStorage)
		assert.NoError(t, err)
		kubeEnv, err := gce.ParseKubeEnv("instance-template", tc.kubeEnvValue)
		assert.NoError(t, err)
		tb := GkeTemplateBuilder{}
		allocatable, err := tb.BuildAllocatableFromKubeEnv(capacity, kubeEnv, gce.ParseEvictionHardOrGetDefault(nil))
		if tc.expectedErr {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			expectedResources, err := makeResourceList(tc.expectedCpu, tc.expectedMemory, tc.gpuCount, tc.expectedEphemeralStorage)
			assert.NoError(t, err)
			for res, expectedQty := range expectedResources {
				qty, found := allocatable[res]
				assert.True(t, found)
				assert.Equal(t, qty.Value(), expectedQty.Value())
			}
		}
	}
}

func TestBuildKubeReserved(t *testing.T) {
	type testCase struct {
		physicalCpu            int64
		reservedCpu            string
		physicalMemory         int64
		reservedMemory         string
		physicalStorage        int64
		reservedStorage        string
		gcfsEnabled            bool
		ephemeralLocalSsdCount int64
		maxPodsPerNode         int64
	}
	testCases := []testCase{{
		physicalCpu: 16,
		reservedCpu: "110m",
		// Below threshold for reserving memory
		physicalMemory:  units.GB,
		reservedMemory:  fmt.Sprintf("%v", 255*units.MiB),
		physicalStorage: 350,
		reservedStorage: fmt.Sprintf("%v", 100*units.GiB),
	}, {
		physicalCpu: 16,
		reservedCpu: "110m",
		// Below threshold for reserving memory
		physicalMemory:  units.GB,
		reservedMemory:  fmt.Sprintf("%v", 255*units.MiB),
		physicalStorage: 350,
		reservedStorage: fmt.Sprintf("%v", 100*units.GiB),
		gcfsEnabled:     true,
	}, {
		physicalCpu: 1,
		reservedCpu: "60m",
		// 10760Mi = 0.25*4000Mi + 0.2*4000Mi + 0.1*8000Mi + 0.06*112000Mi + 0.02*72000Mi
		physicalMemory:  200 * 1000 * units.MiB,
		reservedMemory:  fmt.Sprintf("%v", 10760*units.MiB),
		physicalStorage: 100,
		reservedStorage: fmt.Sprintf("%v", 41*units.GiB),
	}, {
		physicalCpu: 1,
		reservedCpu: "60m",
		// 10760Mi = 0.25*4000Mi + 0.2*4000Mi + 0.1*8000Mi + 0.06*112000Mi + 0.02*72000Mi
		physicalMemory:  200 * 1000 * units.MiB,
		reservedMemory:  fmt.Sprintf("%v", 10760*units.MiB),
		physicalStorage: 35,
		reservedStorage: fmt.Sprintf("%v", 17*units.GiB),
	}, {
		physicalCpu: 1,
		reservedCpu: "60m",
		// 11190Mi = 0.26*4000Mi + 0.208*4000Mi + 0.104*8000Mi + 0.0624*112000Mi + 0.0208*72000Mi
		physicalMemory:  200 * 1000 * units.MiB,
		reservedMemory:  fmt.Sprintf("%v", 11190*units.MiB),
		physicalStorage: 35,
		reservedStorage: fmt.Sprintf("%v", 17*units.GiB),
		gcfsEnabled:     true,
	}, {
		physicalCpu: 16,
		reservedCpu: "110m",
		// Below threshold for reserving memory
		physicalMemory:         units.GB,
		reservedMemory:         fmt.Sprintf("%v", 255*units.MiB),
		physicalStorage:        750,
		reservedStorage:        fmt.Sprintf("%v", 75*units.GiB), // 2 local ssd cards reserves 50GiB
		ephemeralLocalSsdCount: 2,
	}, {
		physicalCpu: 16,
		reservedCpu: "510m",
		// Below threshold for reserving memory
		physicalMemory:         units.GB,
		reservedMemory:         fmt.Sprintf("%v", 255*units.MiB),
		physicalStorage:        750,
		reservedStorage:        fmt.Sprintf("%v", 75*units.GiB), // 2 local ssd cards reserves 50GiB
		ephemeralLocalSsdCount: 2,
		maxPodsPerNode:         256,
	}}
	for _, tc := range testCases {
		tb := GkeTemplateBuilder{}
		expectedReserved, err := makeResourceList(tc.reservedCpu, tc.reservedMemory, 0, tc.reservedStorage)
		assert.NoError(t, err)
		kubeReserved := tb.BuildKubeReserved(tc.physicalCpu, tc.physicalMemory, "n1-standard-1", tc.physicalStorage, tc.gcfsEnabled, tc.ephemeralLocalSsdCount, tc.maxPodsPerNode)
		assertEqualResourceLists(t, "Kube reserved", expectedReserved, kubeReserved)
	}
}

func TestParseEvictionHard(t *testing.T) {
	type testCase struct {
		memory                        string
		ephemeralStorage              string
		memoryExpected                int64 // bytes
		ephemeralStorageRatioExpected float64
	}
	testCases := []testCase{{
		memory:                        "200Mi",
		ephemeralStorage:              "15%",
		memoryExpected:                200 * 1024 * 1024,
		ephemeralStorageRatioExpected: 0.15,
	}, {
		memory:                        "2Gi",
		ephemeralStorage:              "11.5%",
		memoryExpected:                2 * 1024 * 1024 * 1024,
		ephemeralStorageRatioExpected: 0.115,
	}, {
		memory:                        "",
		ephemeralStorage:              "", // empty string, fallback to default
		memoryExpected:                100 * 1024 * 1024,
		ephemeralStorageRatioExpected: 0.1,
	}, {
		memory:                        "110292",
		ephemeralStorage:              "11", // percentage missing, should fallback to default
		memoryExpected:                110292,
		ephemeralStorageRatioExpected: 0.1,
	}, {
		memory:                        "abcb12", // unparsable, fallback to default
		ephemeralStorage:              "-11%",   // negative percentage, should fallback to default
		memoryExpected:                100 * 1024 * 1024,
		ephemeralStorageRatioExpected: 0.1,
	}}
	for _, tc := range testCases {
		test := map[string]string{
			gce.MemoryEvictionHardTag:           tc.memory,
			gce.EphemeralStorageEvictionHardTag: tc.ephemeralStorage,
		}
		actualOutput := gce.ParseEvictionHardOrGetDefault(test)
		assert.EqualValues(t, tc.memoryExpected, actualOutput.MemoryEvictionQuantity, "TestParseEviction Failed Memory. %v expected does not match %v actual.", tc.memoryExpected, actualOutput.MemoryEvictionQuantity)
		assert.EqualValues(t, tc.ephemeralStorageRatioExpected, actualOutput.EphemeralStorageEvictionRatio, "TestParseEviction Failed Ephemeral Storage. %v expected does not match %v actual.", tc.memoryExpected, actualOutput.EphemeralStorageEvictionRatio)
	}
}

func TestAllocatableWithDraEnabled(t *testing.T) {
	acceleratorCountQuantity := resource.NewQuantity(4, resource.DecimalSI)
	gkeManager := newTestGkeManager(t, "", napEnabled, false, false, nil, false, nil)
	tests := map[string]struct {
		nodePoolLabels map[string]string
		extraResources map[string]resource.Quantity

		wantAllocatable apiv1.ResourceList
		wantCapacity    apiv1.ResourceList
	}{
		"NoDriversEnabled_GPU": {
			nodePoolLabels:  map[string]string{labels.GPULabel: "exists"},
			extraResources:  map[string]resource.Quantity{gpu.ResourceNvidiaGPU: *acceleratorCountQuantity},
			wantAllocatable: apiv1.ResourceList{gpu.ResourceNvidiaGPU: *acceleratorCountQuantity},
			wantCapacity:    apiv1.ResourceList{gpu.ResourceNvidiaGPU: *acceleratorCountQuantity},
		},
		"NoDriversEnabled_TPU": {
			nodePoolLabels:  map[string]string{labels.TPULabel: "exists"},
			extraResources:  map[string]resource.Quantity{tpu.ResourceGoogleTPU: *acceleratorCountQuantity},
			wantAllocatable: apiv1.ResourceList{tpu.ResourceGoogleTPU: *acceleratorCountQuantity},
			wantCapacity:    apiv1.ResourceList{tpu.ResourceGoogleTPU: *acceleratorCountQuantity},
		},
		"GPUDriverDisabled": {
			nodePoolLabels:  map[string]string{labels.GPULabel: "exists", labels.DraGpuNodeLabel: "false"},
			extraResources:  map[string]resource.Quantity{gpu.ResourceNvidiaGPU: *acceleratorCountQuantity},
			wantAllocatable: apiv1.ResourceList{gpu.ResourceNvidiaGPU: *acceleratorCountQuantity},
			wantCapacity:    apiv1.ResourceList{gpu.ResourceNvidiaGPU: *acceleratorCountQuantity},
		},
		"TPUDriverDisabled": {
			nodePoolLabels:  map[string]string{labels.TPULabel: "exists", labels.DraGpuNodeLabel: "false"},
			extraResources:  map[string]resource.Quantity{tpu.ResourceGoogleTPU: *acceleratorCountQuantity},
			wantAllocatable: apiv1.ResourceList{tpu.ResourceGoogleTPU: *acceleratorCountQuantity},
			wantCapacity:    apiv1.ResourceList{tpu.ResourceGoogleTPU: *acceleratorCountQuantity},
		},
		"GPUDriverEnabled": {
			nodePoolLabels:  map[string]string{labels.GPULabel: "exists", labels.DraGpuNodeLabel: "true"},
			extraResources:  map[string]resource.Quantity{gpu.ResourceNvidiaGPU: *acceleratorCountQuantity},
			wantAllocatable: apiv1.ResourceList{},
			wantCapacity:    apiv1.ResourceList{},
		},
		"TPUDriverEnabled": {
			nodePoolLabels:  map[string]string{labels.GPULabel: "exists", labels.DraGpuNodeLabel: "true"},
			extraResources:  map[string]resource.Quantity{gpu.ResourceNvidiaGPU: *acceleratorCountQuantity},
			wantAllocatable: apiv1.ResourceList{},
			wantCapacity:    apiv1.ResourceList{},
		},
	}

	for testName, test := range tests {
		const cpu = 4
		const memory = 800000000
		t.Run(testName, func(t *testing.T) {
			arch := gce.Amd64
			mig := &GkeMig{
				gceRef: gce.GceRef{
					Project: projectId,
					Zone:    zoneB,
					Name:    "nodeautoprovisioning-323233232",
				},
				gkeManager:      gkeManager,
				minSize:         0,
				maxSize:         10000,
				autoprovisioned: true,
				exist:           true,
				spec: &gkeclient.NodePoolSpec{
					MachineType:        "n1-standard-1",
					SystemArchitecture: &arch,
					Labels:             test.nodePoolLabels,
				},
				extraResources: test.extraResources,
			}
			AddMigsToNodePool("nodeautoprovisioning-323233232", mig)

			tb := &GkeTemplateBuilder{}
			gceMigOsInfo := gce.NewMigOsInfo(gce.OperatingSystemLinux, gce.OperatingSystemDistributionUbuntu, arch)
			gkeMigOsInfo := NewGkeMigOsInfo(gceMigOsInfo, mig.Version(), mig.IsConfidentialNode())
			ssdDiskSizeProvider := localssdsize.NewSimpleLocalSSDProvider()
			node, err := tb.BuildNodeFromMigSpec(mig, gkeMigOsInfo, cpu, memory, nil, &DaemonSetConditions{}, false, &GkeReserved{}, ssdDiskSizeProvider, gkelabels.DefaultMaxPodsPerNode)

			// Clear pods, cpu and memory from the capacity and allocatable as
			// DRA does not influence how they are being populated
			delete(node.Status.Allocatable, "cpu")
			delete(node.Status.Capacity, "cpu")
			delete(node.Status.Allocatable, "memory")
			delete(node.Status.Capacity, "memory")
			delete(node.Status.Allocatable, "pods")
			delete(node.Status.Capacity, "pods")

			assert.NoError(t, err)
			assertEqualResourceLists(t, "Allocatable", test.wantAllocatable, node.Status.Allocatable)
			assertEqualResourceLists(t, "Capacity", test.wantCapacity, node.Status.Allocatable)
		})
	}
}

func TestAllocatableResourceForBuildNodeFromMigSpec(t *testing.T) {
	type testCase struct {
		scenario           string
		cpu                int64
		memory             int64
		bootDiskGiB        int64
		osDistribution     gce.OperatingSystemDistribution
		systemArchitecture gce.SystemArchitecture
		extraResources     map[string]resource.Quantity
		automaticLocalSsd  bool
		linuxNodeConfig    *gkeclient.LinuxNodeConfig
	}
	testCases := []testCase{
		{
			scenario:           "correct allocatable calculations for cos",
			cpu:                4,
			memory:             800000000,
			bootDiskGiB:        300,
			osDistribution:     gce.OperatingSystemDistributionCOS,
			systemArchitecture: gce.Amd64,
		},
		{
			scenario:           "correct allocatable calculations for arm cos",
			cpu:                4,
			memory:             800000000,
			bootDiskGiB:        300,
			osDistribution:     gce.OperatingSystemDistributionCOS,
			systemArchitecture: gce.Arm64,
		},
		{
			scenario:           "correct allocatable calculations for ubuntu_containerd",
			cpu:                4,
			memory:             800000000,
			bootDiskGiB:        300,
			osDistribution:     gce.OperatingSystemDistributionUbuntu,
			systemArchitecture: gce.Amd64,
		},
		{
			scenario:           "correct allocatable calculations for ubuntu_containerd with 0 ephemeralStorage",
			cpu:                4,
			memory:             800000000,
			bootDiskGiB:        100,
			osDistribution:     gce.OperatingSystemDistributionUbuntu,
			systemArchitecture: gce.Amd64,
		},
		{
			scenario:           "correct allocatable calculations for automatic local SSD",
			cpu:                4,
			memory:             800000000,
			bootDiskGiB:        10,
			osDistribution:     gce.OperatingSystemDistributionCOS,
			systemArchitecture: gce.Amd64,
			automaticLocalSsd:  true,
		},
		{
			scenario:           "correct allocatable calculations for cos with TPUs",
			cpu:                4,
			memory:             800000000,
			bootDiskGiB:        300,
			osDistribution:     gce.OperatingSystemDistributionCOS,
			systemArchitecture: gce.Amd64,
			extraResources: map[string]resource.Quantity{
				tpu.ResourceGoogleTPU: *resource.NewQuantity(4, resource.DecimalSI),
			},
		},
		{
			scenario:           "correct allocatable calculations for networking extra resources",
			cpu:                4,
			memory:             800000000,
			bootDiskGiB:        300,
			osDistribution:     gce.OperatingSystemDistributionCOS,
			systemArchitecture: gce.Amd64,
			extraResources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP":      *resource.NewQuantity(4, resource.DecimalSI),
				"dpdk-net.networking.gke.io.networks/device": *resource.NewQuantity(1, resource.DecimalSI),
			},
		},
		{
			scenario:           "correct allocatable calculations for 2m hugepages",
			cpu:                4,
			memory:             800000000,
			bootDiskGiB:        300,
			osDistribution:     gce.OperatingSystemDistributionCOS,
			systemArchitecture: gce.Amd64,
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				Hugepages: &gkeclient.HugepagesConfig{
					HugepageSize1g: 100,
				},
			},
		},
		{
			scenario:           "correct allocatable calculations for 1g hugepages",
			cpu:                4,
			memory:             800000000,
			bootDiskGiB:        300,
			osDistribution:     gce.OperatingSystemDistributionCOS,
			systemArchitecture: gce.Amd64,
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				Hugepages: &gkeclient.HugepagesConfig{
					HugepageSize2m: 222222,
				},
			},
		},
		{
			scenario:           "correct allocatable calculations for both 1g and 2m hugepages",
			cpu:                4,
			memory:             800000000,
			bootDiskGiB:        300,
			osDistribution:     gce.OperatingSystemDistributionCOS,
			systemArchitecture: gce.Amd64,
			linuxNodeConfig: &gkeclient.LinuxNodeConfig{
				Hugepages: &gkeclient.HugepagesConfig{
					HugepageSize1g: 1,
					HugepageSize2m: 2,
				},
			},
		},
	}
	g := newTestGkeManager(t, "", napEnabled, false, false, nil, false, nil)
	for _, tc := range testCases {
		t.Run(tc.scenario, func(t *testing.T) {
			machineType := "n1-standard-1"
			localSsdCount := int64(-1)
			ephemeralGiB := tc.bootDiskGiB
			if tc.automaticLocalSsd {
				machineType = "a2-ultragpu-2g"
				localSsdCount = 2
				ephemeralGiB = 2 * 375
			}
			mig := &GkeMig{
				gceRef: gce.GceRef{
					Project: projectId,
					Zone:    zoneB,
					Name:    "nodeautoprovisioning-323233232",
				},
				gkeManager:      g,
				minSize:         0,
				maxSize:         10000,
				autoprovisioned: true,
				exist:           true,
				spec: &gkeclient.NodePoolSpec{
					MachineType:        machineType,
					SystemArchitecture: &tc.systemArchitecture,
					DiskSize:           tc.bootDiskGiB,
					LocalSSDConfig:     &gkeclient.LocalSSDConfig{EphemeralStorageConfig: &gke_api_beta.EphemeralStorageConfig{LocalSsdCount: localSsdCount}},
					LinuxNodeConfig:    tc.linuxNodeConfig,
				},
				extraResources: tc.extraResources,
			}
			AddMigsToNodePool("nodeautoprovisioning-323233232", mig)

			tb := &GkeTemplateBuilder{}
			gkeMigOsInfo := NewGkeMigOsInfo(gce.NewMigOsInfo(gce.OperatingSystemLinux, tc.osDistribution, tc.systemArchitecture), mig.Version(), mig.IsConfidentialNode())
			ssdDiskSizeProvider := localssdsize.NewSimpleLocalSSDProvider()
			node, err := tb.BuildNodeFromMigSpec(mig, gkeMigOsInfo, tc.cpu, tc.memory, nil, &DaemonSetConditions{}, false, &GkeReserved{}, ssdDiskSizeProvider, gkelabels.DefaultMaxPodsPerNode)
			assert.NoError(t, err)
			val, ok := node.Annotations[gce.BootDiskSizeAnnotation]
			if !ok {
				t.Errorf("Node doesn't have boot disk size annotation")
			}
			assert.Equal(t, strconv.FormatInt(tc.bootDiskGiB, 10), val)

			if tc.automaticLocalSsd {
				val, ok = node.Annotations[gce.EphemeralStorageLocalSsdAnnotation]
				if !ok {
					t.Errorf("Node doesn't have expected ephmeral local SSD annotation")
				}
				assert.Equal(t, "true", val)
				val, ok = node.Annotations[gce.LocalSsdCountAnnotation]
				if !ok {
					t.Errorf("Node doesn't have expected local SSD count annotation")
				}
				assert.Equal(t, "2", val)
			}

			capacity, err := tb.BuildCapacity(gkeMigOsInfo, tc.cpu, tc.memory, nil, ephemeralGiB*GiB, localSsdCount, nil, &GkeReserved{}, nil)
			if tpuRequest, found := tc.extraResources[tpu.ResourceGoogleTPU]; found {
				capacity[tpu.ResourceGoogleTPU] = tpuRequest.DeepCopy()
			}

			for key, val := range mig.extraResources {
				if networkingutils.IsNetworkResource(key) {
					capacity[apiv1.ResourceName(key)] = val.DeepCopy()
				}
			}

			if tc.linuxNodeConfig != nil {
				if hugepage1g := mig.GetHugepageSize1gBytes(); hugepage1g > 0 {
					capacity[HugepageSize1gResourceName] = *resource.NewQuantity(hugepage1g, resource.DecimalSI)
				}
				if hugepage2m := mig.GetHugepageSize2mBytes(); hugepage2m > 0 {
					capacity[HugepageSize2mResourceName] = *resource.NewQuantity(hugepage2m, resource.DecimalSI)
				}
			}

			assert.NoError(t, err)
			expectedAllocatable := tb.CalculateAllocatable(capacity, tb.BuildKubeReserved(tc.cpu, tc.memory, mig.Spec().MachineType, ephemeralGiB, false, localSsdCount, mig.spec.MaxPodsPerNode), gce.ParseEvictionHardOrGetDefault(nil))
			assertEqualResourceLists(t, "Allocatable", expectedAllocatable, node.Status.Allocatable)
		})
	}
}

func TestNodeNameForBuildNodeFromMigSpec(t *testing.T) {
	g := newTestGkeManager(t, "", napEnabled, false, false, nil, false, nil)
	arch := gce.Amd64
	mig := &GkeMig{
		gceRef: gce.GceRef{
			Project: projectId,
			Zone:    zoneB,
			Name:    "nodeautoprovisioning-323233232",
		},
		gkeManager: g,
		spec: &gkeclient.NodePoolSpec{
			MachineType:        "n1-standard-1",
			SystemArchitecture: &arch,
		},
	}
	AddMigsToNodePool("nodeautoprovisioning-323233232", mig)
	tb := &GkeTemplateBuilder{}
	gkeMigOsInfo := NewGkeMigOsInfo(gce.NewMigOsInfo(gce.OperatingSystemLinux, gce.OperatingSystemDistributionCOS, arch), mig.Version(), mig.IsConfidentialNode())
	ssdDiskSizeProvider := localssdsize.NewSimpleLocalSSDProvider()
	node, err := tb.BuildNodeFromMigSpec(mig, gkeMigOsInfo, 1, 1, nil, &DaemonSetConditions{}, false, &GkeReserved{}, ssdDiskSizeProvider, gkelabels.DefaultMaxPodsPerNode)
	assert.NoError(t, err)
	assert.True(t, strings.HasPrefix(node.Name, mig.GceRef().Name), "node name must start with node group name - it's used in asyncGkeMAnager to match upcoming node groups.")
}

func TestConditionalLabels(t *testing.T) {
	type testCase struct {
		scenario                     string
		daemonSetConditions          *DaemonSetConditions
		containLabels                map[string]string
		notContainLabels             map[string]string
		machineType                  string
		operatingSystem              gce.OperatingSystem
		localSSDCount                int64
		defaultMaxPodsPerNode        int64
		migMaxPodsPerNode            int64
		isConfidentialNode           bool
		machineSerenityLabelsEnabled bool
	}
	testCases := []testCase{
		{
			scenario:                     "disk support labels not added when flag disabled",
			machineType:                  "a4-highgpu-8g",
			daemonSetConditions:          &DaemonSetConditions{},
			machineSerenityLabelsEnabled: false,
			containLabels:                map[string]string{},
			notContainLabels: map[string]string{
				"disk-type.gke.io/pd-balanced":           "true",
				"disk-type.gke.io/pd-standard":           "true",
				"disk-type.gke.io/pd-ssd":                "true",
				"disk-type.gke.io/hyperdisk-balanced-ha": "true",
				"disk-type.gke.io/hyperdisk-throughput":  "true",
				"disk-type.gke.io/pd-extreme":            "true",
				"disk-type.gke.io/hyperdisk-balanced":    "true",
				"disk-type.gke.io/hyperdisk-extreme":     "true",
				"disk-type.gke.io/hyperdisk-ml":          "true",
			},
		},
		{
			scenario:                     "disk support labels on non-confidential node",
			machineType:                  "a4-highgpu-8g",
			daemonSetConditions:          &DaemonSetConditions{},
			machineSerenityLabelsEnabled: true,
			containLabels: map[string]string{
				"disk-type.gke.io/hyperdisk-balanced": "true",
				"disk-type.gke.io/hyperdisk-extreme":  "true",
				"disk-type.gke.io/hyperdisk-ml":       "true",
			},
			notContainLabels: map[string]string{
				"disk-type.gke.io/pd-balanced":           "true",
				"disk-type.gke.io/pd-standard":           "true",
				"disk-type.gke.io/pd-ssd":                "true",
				"disk-type.gke.io/hyperdisk-balanced-ha": "true",
				"disk-type.gke.io/hyperdisk-throughput":  "true",
				"disk-type.gke.io/pd-extreme":            "true",
			},
		},
		{
			scenario:                     "disk support labels on confidential node",
			machineType:                  "n2-standard-96",
			isConfidentialNode:           true,
			daemonSetConditions:          &DaemonSetConditions{},
			machineSerenityLabelsEnabled: true,
			notContainLabels: map[string]string{
				"disk-type.gke.io/hyperdisk-throughput": "true",
				"disk-type.gke.io/hyperdisk-extreme":    "true",
			},
			containLabels: map[string]string{
				"disk-type.gke.io/pd-balanced":                          "true",
				"disk-type.gke.io/pd-standard":                          "true",
				"disk-type.gke.io/pd-ssd":                               "true",
				"disk-type.gke.io/hyperdisk-balanced":                   "true",
				"disk-type.gke.io/hyperdisk-balanced-high-availability": "true",
				"disk-type.gke.io/hyperdisk-ml":                         "true",
				"disk-type.gke.io/pd-extreme":                           "true",
			},
		},
		{
			scenario:            "all daemon set disabled",
			daemonSetConditions: &DaemonSetConditions{},
			notContainLabels: map[string]string{
				"iam.gke.io/gke-metadata-server-enabled":           "true",
				"addon.gke.io/node-local-dns-ds-ready":             "true",
				"cloud.google.com/gke-ephemeral-storage-local-ssd": "true",
				"cloud.google.com/gke-logging-variant":             "MAX_THROUGHPUT",
				gkelabels.NetdLabel:                                gkelabels.NetdValue,
				gkelabels.IpMasqAgentLabel:                         gkelabels.IpMasqAgentValue,
			},
		},
		{
			scenario: "all daemon set enabled",
			daemonSetConditions: &DaemonSetConditions{
				NodeLocalDNSEnabled:          true,
				MetadataServerEnabled:        true,
				HighThroughputLoggingEnabled: true,
				NetdEnabled:                  true,
				IpMasqAgentEnabled:           true,
			},
			defaultMaxPodsPerNode: 100,
			containLabels: map[string]string{
				"iam.gke.io/gke-metadata-server-enabled": "true",
				"addon.gke.io/node-local-dns-ds-ready":   "true",
				"cloud.google.com/gke-logging-variant":   "MAX_THROUGHPUT",
				gkelabels.NetdLabel:                      gkelabels.NetdValue,
				gkelabels.IpMasqAgentLabel:               gkelabels.IpMasqAgentValue,
				gkelabels.MaxPodsPerNodeLabel:            "100",
			},
			notContainLabels: map[string]string{
				"cloud.google.com/gke-ephemeral-storage-local-ssd": "true",
			},
		},
		{
			scenario: "node-local-dns-ds-ready enabled but windows node",
			daemonSetConditions: &DaemonSetConditions{
				NodeLocalDNSEnabled: true,
			},
			defaultMaxPodsPerNode: 100,
			operatingSystem:       gce.OperatingSystemWindows,
			notContainLabels: map[string]string{
				"addon.gke.io/node-local-dns-ds-ready": "true",
			},
		},
		{
			scenario: "all daemon set enabled, a2ultra machine",
			daemonSetConditions: &DaemonSetConditions{
				NodeLocalDNSEnabled:          true,
				MetadataServerEnabled:        true,
				HighThroughputLoggingEnabled: true,
				NetdEnabled:                  true,
				IpMasqAgentEnabled:           true,
			},
			containLabels: map[string]string{
				"iam.gke.io/gke-metadata-server-enabled":           "true",
				"addon.gke.io/node-local-dns-ds-ready":             "true",
				"cloud.google.com/gke-ephemeral-storage-local-ssd": "true",
				"cloud.google.com/gke-logging-variant":             "MAX_THROUGHPUT",
				gkelabels.NetdLabel:                                gkelabels.NetdValue,
				gkelabels.IpMasqAgentLabel:                         gkelabels.IpMasqAgentValue,
			},
			machineType:   "a2-ultragpu-4g",
			localSSDCount: 4,
		},
		{
			scenario: "all daemon set enabled, mig mppn set",
			daemonSetConditions: &DaemonSetConditions{
				NodeLocalDNSEnabled:          true,
				MetadataServerEnabled:        true,
				HighThroughputLoggingEnabled: true,
				NetdEnabled:                  true,
				IpMasqAgentEnabled:           true,
			},
			defaultMaxPodsPerNode: 110,
			migMaxPodsPerNode:     32,
			containLabels: map[string]string{
				"iam.gke.io/gke-metadata-server-enabled":           "true",
				"addon.gke.io/node-local-dns-ds-ready":             "true",
				"cloud.google.com/gke-ephemeral-storage-local-ssd": "true",
				"cloud.google.com/gke-logging-variant":             "MAX_THROUGHPUT",
				gkelabels.NetdLabel:                                gkelabels.NetdValue,
				gkelabels.IpMasqAgentLabel:                         gkelabels.IpMasqAgentValue,
				gkelabels.MaxPodsPerNodeLabel:                      "32",
			},
			machineType:   "a2-ultragpu-4g",
			localSSDCount: 4,
		},
	}
	g := newTestGkeManager(t, "", napEnabled, false, false, nil, false, nil)
	arch := gce.DefaultArch
	for _, testcase := range testCases {
		tc := testcase
		t.Run(tc.scenario, func(t *testing.T) {
			machineType := "n1-standard-1"
			if tc.machineType != "" {
				machineType = tc.machineType
			}
			mig := &GkeMig{
				gceRef: gce.GceRef{
					Project: projectId,
					Zone:    zoneB,
					Name:    "nodeautoprovisioning-323233232",
				},
				gkeManager:      g,
				minSize:         0,
				maxSize:         10000,
				autoprovisioned: true,
				exist:           true,
				spec: &gkeclient.NodePoolSpec{
					SystemArchitecture: &arch,
					DiskSize:           300,
					LocalSSDConfig:     &gkeclient.LocalSSDConfig{EphemeralStorageConfig: &gke_api_beta.EphemeralStorageConfig{LocalSsdCount: tc.localSSDCount}},
					MaxPodsPerNode:     testcase.migMaxPodsPerNode,
				},
			}
			AddMigsToNodePool("nodeautoprovisioning-323233232", mig)
			if tc.isConfidentialNode {
				mig.nodeConfig = &NodeConfig{
					IsConfidentialNode: true,
				}
			}
			mig.spec.MachineType = machineType
			tb := &GkeTemplateBuilder{machineSerenityLabelsEnabled: tc.machineSerenityLabelsEnabled}
			operatingSystem := gce.OperatingSystemLinux
			if tc.operatingSystem != "" {
				operatingSystem = tc.operatingSystem
			}
			gkeMigOsInfo := NewGkeMigOsInfo(gce.NewMigOsInfo(operatingSystem, gce.OperatingSystemDistributionDefault, ""), mig.Version(), mig.IsConfidentialNode())
			ssdDiskSizeProvider := localssdsize.NewSimpleLocalSSDProvider()
			node, err := tb.BuildNodeFromMigSpec(mig, gkeMigOsInfo, 4, 800000000, nil, tc.daemonSetConditions, false, &GkeReserved{}, ssdDiskSizeProvider, tc.defaultMaxPodsPerNode)
			assert.NoError(t, err)
			for kc, vc := range tc.containLabels {
				assert.Equal(t, vc, node.Labels[kc], "Expected value doesn't exist on node")
			}
			for kc, vc := range tc.notContainLabels {
				assert.NotEqual(t, vc, node.Labels[kc], "Un-expected value exists on node")
			}
		})
	}
}

func makeResourceList(cpu string, memory string, gpu int64, ephemeralStorage string) (apiv1.ResourceList, error) {
	result := apiv1.ResourceList{}
	resultCpu, err := resource.ParseQuantity(cpu)
	if err != nil {
		return nil, err
	}
	result[apiv1.ResourceCPU] = resultCpu
	resultMemory, err := resource.ParseQuantity(memory)
	if err != nil {
		return nil, err
	}
	result[apiv1.ResourceMemory] = resultMemory
	if gpu > 0 {
		resultGpu := *resource.NewQuantity(gpu, resource.DecimalSI)
		result[gpuUtils.ResourceNvidiaGPU] = resultGpu
	}
	if len(ephemeralStorage) != 0 {
		resultEphemeralStorage, err := resource.ParseQuantity(ephemeralStorage)
		if err != nil {
			return nil, err
		}
		result[apiv1.ResourceEphemeralStorage] = resultEphemeralStorage
	}
	return result, nil
}

func assertEqualResourceLists(t *testing.T, name string, expected, actual apiv1.ResourceList) {
	t.Helper()
	assert.True(t, quota.Equals(expected, actual),
		"%q unequal:\nExpected: %v\nActual:   %v", name, stringifyResourceList(expected), stringifyResourceList(actual))
}

func stringifyResourceList(resourceList apiv1.ResourceList) string {
	resourceNames := []apiv1.ResourceName{
		apiv1.ResourcePods, apiv1.ResourceCPU, gpuUtils.ResourceNvidiaGPU, apiv1.ResourceMemory, apiv1.ResourceEphemeralStorage, HugepageSize1gResourceName, HugepageSize2mResourceName}
	var results []string
	for _, name := range resourceNames {
		quantity, found := resourceList[name]
		if found {
			value := quantity.Value()
			if name == apiv1.ResourceCPU {
				value = quantity.MilliValue()
			}
			results = append(results, fmt.Sprintf("%v: %v", string(name), value))
		}
	}
	return strings.Join(results, ", ")
}

func TestBuildNodeFromMigSpec_NodeVersion(t *testing.T) {
	nodeVersion := "1.32.9-gke.1726000"
	arch := gce.DefaultArch
	mig := &GkeMig{
		gceRef: gce.GceRef{
			Project: "project1",
			Zone:    "us-central1-b",
			Name:    "nodeautoprovisioning-323233232",
		},
		gkeManager:      &GkeManagerMock{},
		minSize:         0,
		maxSize:         10000,
		autoprovisioned: true,
		exist:           true,
		spec: &gkeclient.NodePoolSpec{
			MachineType:        "n1-standard-1",
			NodeVersion:        nodeVersion,
			SystemArchitecture: &arch,
		},
	}
	tb := &GkeTemplateBuilder{}
	gkeMigOsInfo := NewGkeMigOsInfo(gce.NewMigOsInfo(gce.OperatingSystemLinux, gce.OperatingSystemDistributionCOS, gce.Amd64), "", false)
	ssdDiskSizeProvider := localssdsize.NewSimpleLocalSSDProvider()
	node, err := tb.BuildNodeFromMigSpec(mig, gkeMigOsInfo, 1, 1, nil, &DaemonSetConditions{}, false, &GkeReserved{}, ssdDiskSizeProvider, gkelabels.DefaultMaxPodsPerNode)
	assert.NoError(t, err)
	assert.Equal(t, nodeVersion, node.Status.NodeInfo.KubeletVersion)
}
