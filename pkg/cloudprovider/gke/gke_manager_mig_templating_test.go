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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	gke_api_beta "google.golang.org/api/container/v1beta1"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	test_utils "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/dynamicresources"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	kubeletapis "k8s.io/kubelet/pkg/apis"
)

// params: machineType, serialized guestAccelerators, serialized labels, additional properties field if needed
const instanceTemplateTemplate = `
{
  "kind": "compute#instanceTemplate",
  "id": "28701103232323232",
  "creationTimestamp": "2017-09-15T04:47:21.577-07:00",
  "name": "gke-cluster-1-default-pool",
  "description": "",
  "properties": {
    "tags": {
      "items": [
        "gke-cluster-1-fc0afeeb-node"
      ]
    },
    "machineType": "%s",
    "canIpForward": true,
    "networkInterfaces": [
      {
        "kind": "compute#networkInterface",
        "network": "https://www.googleapis.com/compute/v1/projects/project1/global/networks/default",
        "subnetwork": "https://www.googleapis.com/compute/v1/projects/project1/regions/us-central1/subnetworks/default",
        "accessConfigs": [
          {
            "kind": "compute#accessConfig",
            "type": "ONE_TO_ONE_NAT",
            "name": "external-nat"
          }
        ]
      }
    ],
    "disks": [
      {
        "kind": "compute#attachedDisk",
        "type": "PERSISTENT",
        "mode": "READ_WRITE",
        "boot": true,
        "autoDelete": true
      }
    ],
	"guestAccelerators": %s,
    "metadata": {
      "kind": "compute#metadata",
      "fingerprint": "F7n_RsHD3ng=",
      "items": [
        {
          "key": "kube-env",
          "value": "ALLOCATE_NODE_CIDRS: \"true\"\nAUTOSCALER_ENV_VARS: \"node_labels=%s\""
        },
        {
          "key": "cluster-name",
          "value": "cluster-1"
        }
      ]
    },
    %s
    "scheduling": {
      "onHostMaintenance": "MIGRATE",
      "automaticRestart": true,
      "preemptible": false
    }
  },
  "selfLink": "https://www.googleapis.com/compute/v1/projects/project1/global/instanceTemplates/gke-cluster-1-default-pool-f7607aac"
}`

type testAcceleratorConfig struct {
	gpu              bool
	tpu              bool
	draEnabled       bool
	acceleratorCount int64
	acceleratorType  string
}

func (c *testAcceleratorConfig) gkeAcceleratorConfig() []*gke_api_beta.AcceleratorConfig {
	if c == nil {
		return nil
	}
	return []*gke_api_beta.AcceleratorConfig{{
		AcceleratorCount: c.acceleratorCount,
		AcceleratorType:  c.acceleratorType,
	}}
}
func (c *testAcceleratorConfig) serializedGceAcceleratorConfig() string {
	if c == nil {
		return "[]"
	}
	template := `[{"acceleratorCount":%d,"acceleratorType":"%s"}]`
	return fmt.Sprintf(template, c.acceleratorCount, c.acceleratorType)
}

func (c *testAcceleratorConfig) labels() map[string]string {
	result := map[string]string{}

	if c.gpu {
		result[gkelabels.GPULabel] = c.acceleratorType
		if c.draEnabled {
			result[gkelabels.DraGpuNodeLabel] = "true"
		}
	}

	if c.tpu {
		result[gkelabels.TPULabel] = c.acceleratorType
		if c.draEnabled {
			result[gkelabels.DraTpuNodeLabel] = "true"
		}
	}

	return result
}

func (c *testAcceleratorConfig) extendedResourceName() string {
	if c.gpu {
		return gpu.ResourceNvidiaGPU
	}
	if c.tpu {
		return tpu.ResourceGoogleTPU
	}
	return ""
}

type testMigConfig struct {
	napCreated             bool // shouldn't be set directly in test cases, testCase.finalizeTestCases() sets it.
	machineType            string
	machineTypeCpuOverride int64
	machineTypeMemOverride int64
	mppn                   int64
	threadsPerCore         int64
	accelerators           *testAcceleratorConfig
}

func (c *testMigConfig) machineFamily(t *testing.T) machinetypes.MachineFamily {
	machineFamily, err := machinetypes.NewMachineConfigProvider(nil).GetMachineFamilyFromMachineName(c.machineType)
	assert.NoError(t, err)
	return machineFamily
}

func (c *testMigConfig) machineTypeApiResponse(t *testing.T) string {
	template := `{
		"name": "%s",
		"guestCpus": %d,
		"memoryMb": %d
	}`

	machineType, err := machinetypes.NewMachineConfigProvider(nil).ToMachineType(c.machineType)
	assert.NoError(t, err)

	cpu := machineType.CPU
	if c.machineTypeCpuOverride != 0 {
		cpu = c.machineTypeCpuOverride
	}

	mem := machineType.Memory
	if c.machineTypeMemOverride != 0 {
		mem = c.machineTypeMemOverride
	}

	return fmt.Sprintf(template, c.machineType, cpu, mem)
}

// createMig creates a GkeMig object for the purpose of testing GkeManager.GetMigTemplateNodeInfo() logic:
//   - Only fields relevant to the templating logic are populated.
//   - The populated GkeMig fields should match how they are set during normal Cluster Autoscaler execution. NAP-created
//     MIGs should only have a field set if NAP sets it in reality, and so on.
//   - If the templating logic for the created MIG needs to make GCE calls, they are recorded in the provided gceServerMock.
func (c *testMigConfig) createMig(t *testing.T, gkeManager *gkeManagerImpl, gceServerMock *test_utils.HttpServerMock) *GkeMig {
	// GetMigTemplateNodeInfo() retrieves info about the MIG's machine type from GCE, unless the machine type is custom (in which case it relies on parsing the type name instead).
	if !gce.IsCustomMachine(c.machineType) {
		machineTypeUrl := fmt.Sprintf("/projects/project1/zones/us-central1-b/machineTypes/%s", c.machineType)
		gceServerMock.On("handle", machineTypeUrl).Return(c.machineTypeApiResponse(t))
	}

	// GKE-specific labels are stored in different places depending on whether the MIG is real or NAP-created - set them later in the NAP/non-NAP specific paths.
	gkeLabels := map[string]string{
		gkelabels.MachineFamilyLabel: c.machineFamily(t).Name(), // NAP-created MIGs set this in NewNodeGroup(), real MIGs get it from the instance template. Used in some templating logic.
	}

	maxPodsPerNode := c.mppn
	if maxPodsPerNode == 0 {
		maxPodsPerNode = gkeManager.defaultMaxPodsPerNode.Load()
	}

	// Create the MIG with parts that are common for real and NAP-created MIGs.
	mig := &GkeMig{
		gceRef: gce.GceRef{
			Project: projectId,
			Zone:    zoneB,
		},
		gkeManager: gkeManager,
		minSize:    0,
		maxSize:    1000,
		spec: &gkeclient.NodePoolSpec{
			MachineType:    c.machineType,  // NAP-created MIGs set this in NewNodeGroup(), real MIGs set this in GetCluster(). Used in most templating logic.
			MaxPodsPerNode: maxPodsPerNode, // NAP-created MIGs set this in MaxPodsPerNodeGenerator and overwrite in NapResourceTrimmer, real MIGs set this in GetCluster(). Used in most templating logic.
			Labels:         map[string]string{},
		},
		nodeConfig:     &NodeConfig{},
		extraResources: map[string]resource.Quantity{},
	}
	if c.accelerators != nil {
		for key, val := range c.accelerators.labels() {
			gkeLabels[key] = val // GPU/TPU labels are the same for NAP-created and real MIGs, but they are written to different places. Used in GPU/TPU templating logic.
		}
		if c.accelerators.tpu {
			mig.spec.TpuType = c.accelerators.acceleratorType // NAP-created MIGs set this in TpuRequestGenerator, real MIGs set this in GetCluster(). Used in most TPU templating logic.
		}
		if c.accelerators.gpu {
			// NAP-created MIGs set this in GpuRequestGenerator, real MIGs set this in GetCluster(). Used in DRA-specific GPU templating logic.
			mig.spec.Accelerators = c.accelerators.gkeAcceleratorConfig()
		}
	}

	if c.napCreated {
		// Set the parts of the MIG that are specific to NAP.
		mig.gceRef.Name = "nap-pool"
		mig.autoprovisioned = true
		mig.exist = false

		// Both NAP-created and real MIGs have the arch set on spec (NAP-created in MachineSelectionGenerator, real in GetCluster()).
		// However, the field is only used in the templating logic for NAP-created MIGs, and the logic panics if it's not set.
		// For real MIGs the arch is read from KUBE_ENV in the instance template, but if it's not there the logic falls back to the default one.
		arch := gce.DefaultArch
		mig.spec.SystemArchitecture = &arch

		// NAP-created MIGs have GKE-specific labels simulated in spec.Labels (written all over injection.go and NewNodeGroup()).
		for key, val := range gkeLabels {
			mig.spec.Labels[key] = val
		}

		// NAP-created MIGs with accelerators have mig.extraResources populated in GpuRequestGenerator/TpuRequestGenerator, and
		// the templating logic for them depends on it in the non-DRA path.
		if c.accelerators != nil {
			mig.extraResources[c.accelerators.extendedResourceName()] = *resource.NewQuantity(c.accelerators.acceleratorCount, resource.DecimalSI)
		}

		return mig
	}

	// Set the parts of the MIG that are specific to real MIGs.
	mig.gceRef.Name = "default-pool"
	mig.autoprovisioned = false
	mig.exist = true

	// Real MIGs have GKE-specific labels in the instance template, spec.Labels only contains user-configured labels for them.
	var gkeLabelsList []string
	for key, val := range gkeLabels {
		gkeLabelsList = append(gkeLabelsList, fmt.Sprintf("%s=%s", key, val))
	}
	kubeEnvSerializedLabels := strings.Join(gkeLabelsList, ",")

	serializedThreadsPerCoreConfig := ""
	if c.threadsPerCore != 0 {
		// NAP-created MIGs never have ThreadsPerCore set on the spec, real MIGs have it set in GetCluster(). But it doesn't seem like the field is actually used anywhere
		// in the templating logic. The templating logic uses the ThreadsPerCore value from the instance template instead. Setting it in both places for completeness.
		mig.nodeConfig.ThreadsPerCore = c.threadsPerCore
		serializedThreadsPerCoreConfig = fmt.Sprintf(`"advancedMachineFeatures": {"threadsPerCore": %d},`, c.threadsPerCore)
	}

	// Non-DRA templating logic for real GPU MIGs uses the accelerator config from the instance template.
	serializedGceAcceleratorConfig := c.accelerators.serializedGceAcceleratorConfig()

	// Render the GCE instance template with the computed machineType, accelerator config, and labels.
	renderedInstanceTemplate := fmt.Sprintf(instanceTemplateTemplate, c.machineType, serializedGceAcceleratorConfig, kubeEnvSerializedLabels, serializedThreadsPerCoreConfig)

	// Templating logic calls this for real MIGs to get the instance template URL.
	gceServerMock.On("handle", "/projects/project1/zones/us-central1-b/instanceGroupManagers/default-pool").Return(getInstanceGroupManagerResponse).Once()
	// Templating logic calls this for real MIGs to get the GCE MIG instance template. The returned value contains the machine type, labels, and accelerator config computed above.
	gceServerMock.On("handle", "/projects/project1/global/instanceTemplates/gke-cluster-1-default-pool").Return(renderedInstanceTemplate).Once()

	return mig
}

type testCase struct {
	testName                 string
	specificToNapCreatedMigs bool
	specificToExistingMigs   bool

	dataPlaneV2Disabled bool
	migConfig           testMigConfig
	clusterMPPN         int64

	wantCpuCapacity       int64
	wantMppn              int64
	wantKubeProxyPod      bool
	wantResourceSlices    map[string]int
	wantExtendedResources map[string]int64
}

func (tc testCase) finalizeTestCases() []testCase {
	// Shallow copy by value.
	var tcNap = tc
	// Make the copy into a NAP-specific test case.
	tcNap.testName = "NAP_MIG/" + tcNap.testName
	tcNap.migConfig.napCreated = true

	// Shallow copy by value.
	var tcExisting = tc
	// Make the copy into an existing-MIG-specific test case.
	tcExisting.testName = "EXISTING_MIG/" + tc.testName
	tcExisting.migConfig.napCreated = false

	if tc.specificToNapCreatedMigs {
		return []testCase{tcNap}
	}
	if tc.specificToExistingMigs {
		return []testCase{tcExisting}
	}
	// If the test case is not specific to one kind of MIGs - return both.
	return []testCase{tcNap, tcExisting}
}

func TestGetMigTemplateNodeInfo(t *testing.T) {
	testCases := []testCase{
		// Most test cases are applicable to both NAP-created and existing MIGs - they are duplicated to test both paths.
		{
			testName: "predefined_machine_type",
			migConfig: testMigConfig{
				machineType: "n1-standard-2",
			},
			wantCpuCapacity: 2,
			wantMppn:        gkelabels.DefaultMaxPodsPerNode,
		},
		{
			testName: "custom_machine_type",
			migConfig: testMigConfig{
				machineType: "custom-3-2000",
			},
			wantCpuCapacity: 3,
		},
		{
			testName: "non_default_mppn",
			migConfig: testMigConfig{
				machineType: "n1-standard-2",
				mppn:        50,
			},
			wantCpuCapacity: 2,
			wantMppn:        50,
		},
		{
			testName: "cluster_default_mppn",
			migConfig: testMigConfig{
				machineType: "n1-standard-2",
			},
			clusterMPPN:     50,
			wantCpuCapacity: 2,
			wantMppn:        50,
		},
		{
			testName: "mig_mppn_overrides_cluster_default_mppn",
			migConfig: testMigConfig{
				machineType: "n1-standard-2",
				mppn:        50,
			},
			clusterMPPN:     100,
			wantCpuCapacity: 2,
			wantMppn:        50,
		},
		{
			testName:            "data_plane_v2_disabled",
			dataPlaneV2Disabled: true,
			migConfig: testMigConfig{
				machineType: "n1-standard-2",
			},
			wantCpuCapacity:  2,
			wantKubeProxyPod: true, // kube-proxy static pod should be added if DataPlaneV2 is disabled.
		},
		// TPU-specific test cases.
		{
			testName: "ct6e-standard-8t",
			migConfig: testMigConfig{
				machineType:            "ct6e-standard-8t",
				machineTypeCpuOverride: 360,
				machineTypeMemOverride: 516096,
			},
			wantCpuCapacity: 180, // ct6e-standard-8t has threadsPerCore=1 by default in GKE, so we should predict half as much CPU as the GCE API shows
		},
		{
			testName: "ct6e-standard-4t",
			migConfig: testMigConfig{
				machineType:            "ct6e-standard-4t",
				machineTypeCpuOverride: 280,
				machineTypeMemOverride: 516096,
			},
			wantCpuCapacity: 280, // ct6e-standard-4t has the standard threadsPerCore=2, so we should predict as much CPU as the GCE API shows
		},
		// DRA vs non-DRA (GPU/TPU) test cases
		{
			testName: "gpu_dra",
			migConfig: testMigConfig{
				napCreated:  true,
				machineType: "n1-standard-2",
				accelerators: &testAcceleratorConfig{
					gpu:              true,
					draEnabled:       true,
					acceleratorType:  "nvidia-tesla-t4",
					acceleratorCount: 2,
				},
			},
			wantResourceSlices:    map[string]int{dynamicresources.GpuDriver: 1},
			wantExtendedResources: map[string]int64{gpu.ResourceNvidiaGPU: 0},
		},
		{
			testName: "gpu_dra_multiple_slices",
			migConfig: testMigConfig{
				napCreated:  true,
				machineType: "n1-standard-2",
				accelerators: &testAcceleratorConfig{
					gpu:              true,
					draEnabled:       true,
					acceleratorType:  "nvidia-tesla-t4",
					acceleratorCount: 200,
				},
			},
			wantResourceSlices:    map[string]int{dynamicresources.GpuDriver: 2},
			wantExtendedResources: map[string]int64{gpu.ResourceNvidiaGPU: 0},
		},
		{
			testName: "gpu_no_dra",
			migConfig: testMigConfig{
				napCreated:  true,
				machineType: "n1-standard-2",
				accelerators: &testAcceleratorConfig{
					gpu:              true,
					acceleratorType:  "nvidia-tesla-t4",
					acceleratorCount: 2,
				},
			},
			wantResourceSlices:    nil,
			wantExtendedResources: map[string]int64{gpu.ResourceNvidiaGPU: 2},
		},
		{
			testName: "tpu_dra",
			migConfig: testMigConfig{
				napCreated:  true,
				machineType: "ct5lp-hightpu-1t",
				accelerators: &testAcceleratorConfig{
					tpu:              true,
					draEnabled:       true,
					acceleratorType:  gkelabels.TpuV5LitePodsliceValue,
					acceleratorCount: 1,
				},
			},
			wantResourceSlices:    map[string]int{dynamicresources.TpuDriver: 1},
			wantExtendedResources: map[string]int64{tpu.ResourceGoogleTPU: 0},
		},
		{
			testName: "tpu_no_dra",
			migConfig: testMigConfig{
				napCreated:  true,
				machineType: "ct5lp-hightpu-1t",
				accelerators: &testAcceleratorConfig{
					tpu:              true,
					draEnabled:       false,
					acceleratorType:  "tpu-v5-lite-podslice",
					acceleratorCount: 1,
				},
			},
			wantResourceSlices:    nil,
			wantExtendedResources: map[string]int64{tpu.ResourceGoogleTPU: 1},
		},
		// Test cases only applicable to existing MIGs.
		{
			testName:               "2_core_vm_threadsPerCore=1",
			specificToExistingMigs: true,
			migConfig: testMigConfig{
				machineType:    "n1-standard-2",
				threadsPerCore: 1,
			},
			wantCpuCapacity: 1,
		},
		{
			testName:               "2_core_vm_threadsPerCore=2",
			specificToExistingMigs: true,
			migConfig: testMigConfig{
				machineType:    "n1-standard-2",
				threadsPerCore: 2,
			},
			wantCpuCapacity: 2,
		},
		{
			testName:               "1_core_vm_threadsPerCore=1",
			specificToExistingMigs: true,
			migConfig: testMigConfig{
				machineType:    "n1-standard-1",
				threadsPerCore: 1,
			},
			wantCpuCapacity: 1,
		},
		{
			testName:               "1_core_vm_threadsPerCore=2",
			specificToExistingMigs: true,
			migConfig: testMigConfig{
				machineType:    "n1-standard-1",
				threadsPerCore: 2,
			},
			wantCpuCapacity: 1,
		},
	}

	// Duplicate the test cases for NAP-created and existing MIGs.
	var finalTestCases []testCase
	for _, tc := range testCases {
		finalTestCases = append(finalTestCases, tc.finalizeTestCases()...)
	}

	for _, tc := range finalTestCases {
		t.Run(tc.testName, func(t *testing.T) {
			// Set up the mock GCE server.
			server := test_utils.NewHttpServerMock()
			t.Cleanup(server.Close)

			// Set up the tested GKE manager. Most parameters don't matter for GetMigTemplateNodeInfo().
			gkeManager := newTestGkeManager(t, server.URL, napDisabled, false, false, nil, false, nil)
			gkeManager.dataplaneV2Enabled.Store(!tc.dataPlaneV2Disabled)
			gkeManager.defaultMaxPodsPerNode.Store(tc.clusterMPPN)
			// newTestGkeManager seeds the machine-types cache with 2 arbitrary machine types, which makes reasoning about GCE calls very difficult (there's no
			// GCE call for the 2 types in cache, but there is one for all other types). Clear the cache so that we can always expect a GCE call.
			gkeManager.cache.SetMachines(nil)

			// Create a test MIG based on the test-case config. This also registers all GCE calls needed by GetMigTemplateNodeInfo() for a given MIG in the
			// mock server.
			mig := tc.migConfig.createMig(t, gkeManager, server)

			nodeInfo, err := gkeManager.GetMigTemplateNodeInfo(mig)
			assert.NoError(t, err)

			// Assert that all GCE calls made by GetMigTemplateNodeInfo() are as expected.
			mock.AssertExpectationsForObjects(t, server)

			// Assert the returned Node is as expected.
			assert.NotNil(t, nodeInfo.Node())
			capacity := nodeInfo.Node().Status.Capacity
			if got := capacity[apiv1.ResourceCPU]; tc.wantCpuCapacity != 0 && got.CmpInt64(tc.wantCpuCapacity) != 0 {
				t.Errorf("incorrect machine CPUs: got: %v, want: %d", &got, tc.wantCpuCapacity)
			}
			if got := capacity[apiv1.ResourcePods]; tc.wantMppn != 0 && got.CmpInt64(tc.wantMppn) != 0 {
				t.Errorf("incorrect max pods per node: got: %v, want: %d", &got, tc.wantMppn)
			}
			for resourceName, quantity := range tc.wantExtendedResources {
				if quantity == 0 {
					assert.NotContains(t, nodeInfo.Node().Status.Capacity, resourceName)
					assert.NotContains(t, nodeInfo.Node().Status.Allocatable, resourceName)
				} else {
					assert.Equal(t, *resource.NewQuantity(quantity, resource.DecimalSI), nodeInfo.Node().Status.Capacity[apiv1.ResourceName(resourceName)])
					assert.Equal(t, *resource.NewQuantity(quantity, resource.DecimalSI), nodeInfo.Node().Status.Allocatable[apiv1.ResourceName(resourceName)])
				}
			}

			// Assert the returned Pods are as expected.
			foundKubeProxy := false
			for _, p := range nodeInfo.Pods() {
				if strings.Contains(p.Pod.Name, "kube-proxy") {
					foundKubeProxy = true
					break
				}
			}
			assert.Equal(t, tc.wantKubeProxyPod, foundKubeProxy)

			// Assert the returned DRA objects are as expected
			var gotResourceSlices map[string]int
			if len(nodeInfo.LocalResourceSlices) > 0 { // A little hack so that test cases don't have to specify tc.wantResourceSlices if there are 0 slices expected.
				gotResourceSlices = map[string]int{}
				for _, slice := range nodeInfo.LocalResourceSlices {
					gotResourceSlices[slice.Spec.Driver]++
				}
			}
			assert.Equal(t, tc.wantResourceSlices, gotResourceSlices)
		})
	}
}

func TestGetMigTemplateNodeInfo_DeprecatedLabelsAndCaching(t *testing.T) {
	server := test_utils.NewHttpServerMock()
	t.Cleanup(server.Close)

	gkeManager := newTestGkeManager(t, server.URL, false, false, false, nil, false, nil)
	gkeManager.cache.SetMachines(nil)

	migConfig := testMigConfig{
		machineType: "e2-standard-2",
		napCreated:  true,
	}
	mig := migConfig.createMig(t, gkeManager, server)
	mig.spec.Labels[apiv1.LabelArchStable] = "amd64"

	// populate cache
	nodeInfo, err := gkeManager.GetMigTemplateNodeInfo(mig)
	assert.NoError(t, err)
	assert.NotNil(t, nodeInfo.Node())
	assert.Equal(t, "amd64", nodeInfo.Node().Labels[kubeletapis.LabelArch])

	// read from cache
	nodeInfo, err = gkeManager.GetMigTemplateNodeInfo(mig)
	assert.NoError(t, err)
	assert.NotNil(t, nodeInfo.Node())
	assert.Equal(t, "amd64", nodeInfo.Node().Labels[kubeletapis.LabelArch])
}
