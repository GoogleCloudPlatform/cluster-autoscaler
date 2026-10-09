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

package inmemory

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	gcev1 "google.golang.org/api/compute/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	dra "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/dynamicresources/testing"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/core"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// TestDraUnhappyPaths verifies that Cluster Autoscaler with DynamicResourceAllocationEnabled
// handles unregistered/custom machine types lacking static DRANET/accelerator hardware tables,
// pods referencing missing ResourceClaims or ResourceClaimTemplates, and nodes with incomplete
// or unknown-driver ResourceSlices during scale-up and scale-down without panicking.
func TestDraUnhappyPaths(t *testing.T) {
	t.Run("UnregisteredMachineTypesWithDraNetAndFallbackPool", testDraUnregisteredMachineTypesWithFallback)
	t.Run("UnregisteredMachineTypesWithComputeClassNapFallback", testDraUnregisteredMachineTypesWithComputeClassNap)
	t.Run("PodReferencingMissingResourceClaimOrTemplate", testDraPodReferencingMissingClaimOrTemplate)
	t.Run("NodeWithIncompleteOrOrphanedResourceSlices", testDraNodeWithIncompleteOrOrphanedResourceSlices)
}

func testDraUnregisteredMachineTypesWithFallback(t *testing.T) {
	testConfig := draUnregisteredMachineTypesTestConfig()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		registerUnregisteredMachineTypesInGce(infra)
		addUnschedulablePodTargetingCustomDranetPool(infra)
		addUnschedulableDraGpuPod(ctx, t, infra, "fallback-gpu-pod", "fallback-gpu-claim", 2)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		assertNodeMachineTypes(ctx, t, infra, "mockdranet1-standard-4", "g2-standard-96")
	})
}

func testDraUnregisteredMachineTypesWithComputeClassNap(t *testing.T) {
	cc := ccc.NewComputeClassBuilder("dra-fallback-cc").
		WithNapEnabled().
		WithWhenUnsatisfiable("DoNotScaleUp").
		WithPriorities(
			v1.Priority{
				MachineType: ptr.To("unregistered-nap-dra-mt-16"),
			},
			v1.Priority{
				MachineType: ptr.To("n1-standard-4"),
			},
		).
		Build()

	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithDraEnabled(),
			integration.WithAutoProvisioningEnabled(),
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithAutoprovisioningLocations("us-central1-b"),
		).
		WithCccCrds(cc)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		pendingPod := tu.BuildTestPod(
			"ccc-dra-pod",
			1000,
			2000,
			tu.MarkUnschedulable(),
			tu.WithCreationTimestamp(time.Now().Add(-1*time.Minute)),
			pod.WithCCC("dra-fallback-cc"),
		)
		infra.Fakes.K8s.AddPod(pendingPod)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		if assert.Len(t, nodes.Items, 1, "Expected NAP to provision 1 node using the fallback ComputeClass priority") {
			assert.Equal(t, "n1-standard-4", nodes.Items[0].Labels[apiv1.LabelInstanceTypeStable])
		}
	})
}

func testDraPodReferencingMissingClaimOrTemplate(t *testing.T) {
	testConfig := draGpuNodePoolTestConfig(1)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		var healthyNodeName string
		for _, node := range infra.Fakes.K8s.Nodes().Items {
			healthyNodeName = node.Name
		}
		assert.NotEmpty(t, healthyNodeName)

		allocateDraGpuFillerPodOnNode(ctx, t, infra, healthyNodeName, "filler", "filler-claim", 7)
		addUnschedulablePodWithMissingResourceClaim(infra, "pod-missing-claim", "non-existent-resource-claim")
		addUnschedulablePodWithMissingClaimTemplate(infra, "pod-missing-template", "non-existent-claim-template")

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		assert.Equal(t, 1, len(infra.Fakes.K8s.Nodes().Items), "Should not scale up for pods with missing ResourceClaim or ResourceClaimTemplate")

		addUnschedulableDraGpuPod(ctx, t, infra, "valid-gpu-pod", "valid-gpu-claim", 4)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		assert.Equal(t, 2, len(infra.Fakes.K8s.Nodes().Items), "Valid DRA pod should still trigger scale-up despite other pods missing claims")
	})
}

func testDraNodeWithIncompleteOrOrphanedResourceSlices(t *testing.T) {
	testConfig := draGpuNodePoolTestConfig(2)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		nodesList := infra.Fakes.K8s.Nodes().Items
		assert.Len(t, nodesList, 2)
		nodeWithFiller := nodesList[0].Name
		nodeWithMalformedSlices := nodesList[1].Name

		allocateDraGpuFillerPodOnNode(ctx, t, infra, nodeWithFiller, "filler", "filler-claim", 7)

		savedNode2Slices := removeNodeResourceSlices(ctx, t, infra, nodeWithMalformedSlices)
		createUnknownDriverResourceSlice(ctx, t, infra, nodeWithMalformedSlices, "unknown.driver.example.com", "orphaned-device-0")
		incompletePoolSlice := createIncompletePoolGpuResourceSlice(ctx, t, infra, nodeWithMalformedSlices)

		verifyNodeWithIncompleteResourceSlicePoolIsNotScaledDown(ctx, t, infra, autoscaler)
		restoreNodeGpuSlicesAndVerifyScaleDownWithUnknownDriverSlice(ctx, t, infra, autoscaler, incompletePoolSlice.Name, savedNode2Slices)
		verifyScaleUpWithOrphanUnknownDriverSliceOnActiveNode(ctx, t, infra, autoscaler, nodeWithFiller)
	})
}

func draUnregisteredMachineTypesTestConfig() *integration.TestConfig {
	return integration.NewTestConfig().
		WithNodePools(
			&gke_api_beta.NodePool{
				Name: "custom-dranet-pool",
				Config: &gke_api_beta.NodeConfig{
					MachineType: "mockdranet1-standard-4",
					Labels: map[string]string{
						labels.DraNetNodeLabel: "true",
						"pool-role":            "custom-dranet",
					},
				},
				InitialNodeCount: 0,
				Autoscaling: &gke_api_beta.NodePoolAutoscaling{
					Enabled:      true,
					MinNodeCount: 0,
					MaxNodeCount: 5,
				},
				Locations: []string{"us-central1-b"},
			},
			&gke_api_beta.NodePool{
				Name: "unregistered-gpu-dra-pool",
				Config: &gke_api_beta.NodeConfig{
					MachineType: "mockgpu1-standard-8",
					Labels: map[string]string{
						labels.DraGpuNodeLabel: "true",
						labels.GPULabel:        "nvidia-future-gpu-x99",
					},
					Accelerators: []*gke_api_beta.AcceleratorConfig{
						{
							AcceleratorCount: 8,
							AcceleratorType:  "nvidia-future-gpu-x99",
							GpuDriverInstallationConfig: &gke_api_beta.GPUDriverInstallationConfig{
								GpuDriverVersion: "disabled",
							},
						},
					},
				},
				InitialNodeCount: 0,
				Autoscaling: &gke_api_beta.NodePoolAutoscaling{
					Enabled:      true,
					MinNodeCount: 0,
					MaxNodeCount: 5,
				},
				Locations: []string{"us-central1-b"},
			},
			&gke_api_beta.NodePool{
				Name: "unregistered-tpu-dra-pool",
				Config: &gke_api_beta.NodeConfig{
					MachineType: "mocktpu1-standard-16",
					Labels: map[string]string{
						labels.DraTpuNodeLabel: "true",
						labels.TPULabel:        "tpu-future-v99-slice",
					},
				},
				InitialNodeCount: 0,
				Autoscaling: &gke_api_beta.NodePoolAutoscaling{
					Enabled:      true,
					MinNodeCount: 0,
					MaxNodeCount: 5,
				},
				Locations: []string{"us-central1-b"},
			},
			&gke_api_beta.NodePool{
				Name: "fallback-known-dra-pool",
				Config: &gke_api_beta.NodeConfig{
					MachineType: "g2-standard-96",
					Labels: map[string]string{
						labels.DraGpuNodeLabel: "true",
					},
					Accelerators: []*gke_api_beta.AcceleratorConfig{
						{
							AcceleratorCount: 8,
							AcceleratorType:  labels.NvidiaL4,
							GpuDriverInstallationConfig: &gke_api_beta.GPUDriverInstallationConfig{
								GpuDriverVersion: "disabled",
							},
						},
					},
				},
				InitialNodeCount: 0,
				Autoscaling: &gke_api_beta.NodePoolAutoscaling{
					Enabled:      true,
					MinNodeCount: 0,
					MaxNodeCount: 5,
				},
				Locations: []string{"us-central1-b"},
			},
		).
		WithOverrides(
			integration.WithDraEnabled(),
			withSalvoMultiScaleUp(true),
		)
}

func draGpuNodePoolTestConfig(initialNodes int64) *integration.TestConfig {
	return integration.NewTestConfig().
		WithNodePools(&gke_api_beta.NodePool{
			Name: "dra-nodepool",
			Config: &gke_api_beta.NodeConfig{
				MachineType: "g2-standard-96",
				Labels: map[string]string{
					labels.DraGpuNodeLabel: "true",
				},
				Accelerators: []*gke_api_beta.AcceleratorConfig{
					{
						AcceleratorCount: 8,
						AcceleratorType:  labels.NvidiaL4,
						GpuDriverInstallationConfig: &gke_api_beta.GPUDriverInstallationConfig{
							GpuDriverVersion: "disabled",
						},
					},
				},
			},
			InitialNodeCount: initialNodes,
			Autoscaling: &gke_api_beta.NodePoolAutoscaling{
				Enabled:      true,
				MinNodeCount: 1,
				MaxNodeCount: 5,
			},
			Locations: []string{"us-central1-b"},
		}).
		WithOverrides(
			integration.WithDraEnabled(),
		)
}

func registerUnregisteredMachineTypesInGce(infra *integration.TestInfrastructure) {
	infra.Fakes.GceService.AddCustomMachineType(&gcev1.MachineType{
		Name:      "mockdranet1-standard-4",
		GuestCpus: 4,
		MemoryMb:  16384,
		Zone:      "us-central1-b",
	})
	infra.Fakes.GceService.AddCustomMachineType(&gcev1.MachineType{
		Name:      "mockgpu1-standard-8",
		GuestCpus: 8,
		MemoryMb:  32768,
		Zone:      "us-central1-b",
	})
	infra.Fakes.GceService.AddCustomMachineType(&gcev1.MachineType{
		Name:      "mocktpu1-standard-16",
		GuestCpus: 16,
		MemoryMb:  65536,
		Zone:      "us-central1-b",
	})
}

func addUnschedulablePodTargetingCustomDranetPool(infra *integration.TestInfrastructure) {
	customPod := tu.BuildTestPod(
		"custom-dranet-pod",
		1000,
		2000,
		tu.MarkUnschedulable(),
		tu.WithCreationTimestamp(time.Now().Add(-1*time.Minute)),
		pod.WithNodeSelector(map[string]string{
			"pool-role": "custom-dranet",
		}),
	)
	infra.Fakes.K8s.AddPod(customPod)
}

func addUnschedulableDraGpuPod(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, podName, claimName string, gpuCount int) {
	t.Helper()
	gpuClaim := dra.NewResourceClaim(claimName, dra.GpuDeviceExactReq(int64(gpuCount)))
	_, err := infra.Fakes.KubeClient.ResourceV1().ResourceClaims("default").Create(ctx, gpuClaim, metav1.CreateOptions{})
	assert.NoError(t, err)
	draPod := gpuPod(podName, claimName, tu.MarkUnschedulable(), tu.WithCreationTimestamp(time.Now().Add(-1*time.Minute)))
	infra.Fakes.K8s.AddPod(draPod)
}

func allocateDraGpuFillerPodOnNode(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, nodeName, podName, claimName string, gpuCount int) {
	t.Helper()
	slices, err := infra.Fakes.KubeClient.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{})
	assert.NoError(t, err)
	fillerClaim := dra.NewResourceClaim(claimName, dra.GpuDeviceExactReq(int64(gpuCount)))
	dra.MustAllocateClaim(t, ctx, slices.Items, fillerClaim, gpuCount, nodeName)
	_, err = infra.Fakes.KubeClient.ResourceV1().ResourceClaims("default").Create(ctx, fillerClaim, metav1.CreateOptions{})
	assert.NoError(t, err)
	infra.Fakes.K8s.AddPod(gpuPod(podName, claimName, tu.WithNodeName(nodeName), tu.WithCreationTimestamp(time.Now().Add(-1*time.Minute))))
}

func addUnschedulablePodWithMissingResourceClaim(infra *integration.TestInfrastructure, podName, missingClaimName string) {
	p := tu.BuildTestPod(
		podName,
		100,
		400,
		tu.MarkUnschedulable(),
		tu.WithCreationTimestamp(time.Now().Add(-1*time.Minute)),
		tu.WithResourceClaim("missing-claim-ref", missingClaimName, ""),
		pod.WithNodeSelector(map[string]string{
			labels.GPULabel:           labels.NvidiaL4,
			labels.MachineFamilyLabel: "g2",
			labels.DraGpuNodeLabel:    "true",
		}),
	)
	infra.Fakes.K8s.AddPod(p)
}

func addUnschedulablePodWithMissingClaimTemplate(infra *integration.TestInfrastructure, podName, missingTemplateName string) {
	p := tu.BuildTestPod(
		podName,
		100,
		400,
		tu.MarkUnschedulable(),
		tu.WithCreationTimestamp(time.Now().Add(-1*time.Minute)),
		tu.WithResourceClaim("missing-template-ref", "", missingTemplateName),
		pod.WithNodeSelector(map[string]string{
			labels.GPULabel:           labels.NvidiaL4,
			labels.MachineFamilyLabel: "g2",
			labels.DraGpuNodeLabel:    "true",
		}),
	)
	infra.Fakes.K8s.AddPod(p)
}

func assertNodeMachineTypes(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, wantMachineTypes ...string) {
	t.Helper()
	nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, nodes.Items, len(wantMachineTypes))

	machineTypesSeen := map[string]bool{}
	for _, node := range nodes.Items {
		machineTypesSeen[node.Labels[apiv1.LabelInstanceTypeStable]] = true
	}
	for _, wantType := range wantMachineTypes {
		assert.True(t, machineTypesSeen[wantType], "Expected %s node to be created", wantType)
	}
}

func removeNodeResourceSlices(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, nodeName string) []*resourceapi.ResourceSlice {
	t.Helper()
	slices, err := infra.Fakes.KubeClient.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{})
	assert.NoError(t, err)

	var savedSlices []*resourceapi.ResourceSlice
	for _, slice := range slices.Items {
		if slice.Spec.NodeName != nil && *slice.Spec.NodeName == nodeName {
			sliceCopy := slice.DeepCopy()
			sliceCopy.ResourceVersion = ""
			savedSlices = append(savedSlices, sliceCopy)
			err = infra.Fakes.KubeClient.ResourceV1().ResourceSlices().Delete(ctx, slice.Name, metav1.DeleteOptions{})
			assert.NoError(t, err)
		}
	}
	assert.NotEmpty(t, savedSlices)
	return savedSlices
}

func createUnknownDriverResourceSlice(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, nodeName, driverName, deviceName string) {
	t.Helper()
	slice := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName + "-" + deviceName + "-slice",
		},
		Spec: resourceapi.ResourceSliceSpec{
			Driver:   driverName,
			NodeName: ptr.To(nodeName),
			Pool: resourceapi.ResourcePool{
				Name:               nodeName + "-" + deviceName + "-pool",
				ResourceSliceCount: 1,
				Generation:         1,
			},
			Devices: []resourceapi.Device{
				{
					Name: deviceName,
				},
			},
		},
	}
	_, err := infra.Fakes.KubeClient.ResourceV1().ResourceSlices().Create(ctx, slice, metav1.CreateOptions{})
	assert.NoError(t, err)
}

func createIncompletePoolGpuResourceSlice(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, nodeName string) *resourceapi.ResourceSlice {
	t.Helper()
	incompletePoolSlice := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName + "-incomplete-pool-slice",
		},
		Spec: resourceapi.ResourceSliceSpec{
			Driver:   "gpu.nvidia.com",
			NodeName: ptr.To(nodeName),
			Pool: resourceapi.ResourcePool{
				Name:               nodeName + "-incomplete-pool",
				ResourceSliceCount: 2,
				Generation:         1,
			},
			Devices: []resourceapi.Device{
				{
					Name: "gpu-incomplete-0",
				},
			},
		},
	}
	_, err := infra.Fakes.KubeClient.ResourceV1().ResourceSlices().Create(ctx, incompletePoolSlice, metav1.CreateOptions{})
	assert.NoError(t, err)
	return incompletePoolSlice
}

func verifyNodeWithIncompleteResourceSlicePoolIsNotScaledDown(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, autoscaler core.Autoscaler) {
	t.Helper()
	integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
	integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*time.Second)

	nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, nodes.Items, 2, "Node with incomplete ResourceSlice pool should not panic or be prematurely scaled down")
}

func restoreNodeGpuSlicesAndVerifyScaleDownWithUnknownDriverSlice(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, autoscaler core.Autoscaler, incompleteSliceName string, savedSlices []*resourceapi.ResourceSlice) {
	t.Helper()
	err := infra.Fakes.KubeClient.ResourceV1().ResourceSlices().Delete(ctx, incompleteSliceName, metav1.DeleteOptions{})
	assert.NoError(t, err)
	for _, savedSlice := range savedSlices {
		_, err = infra.Fakes.KubeClient.ResourceV1().ResourceSlices().Create(ctx, savedSlice, metav1.CreateOptions{})
		assert.NoError(t, err)
	}

	integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
	integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*time.Second)

	remainingNodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, remainingNodes.Items, 1, "Expected unneeded node with unknown-driver ResourceSlice to be scaled down cleanly")
}

func verifyScaleUpWithOrphanUnknownDriverSliceOnActiveNode(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, autoscaler core.Autoscaler, activeNodeName string) {
	t.Helper()
	createUnknownDriverResourceSlice(ctx, t, infra, activeNodeName, "unregistered.hardware.example.com", "mystery-accelerator-0")
	addUnschedulableDraGpuPod(ctx, t, infra, "scaleup-gpu-pod", "scaleup-gpu-claim", 4)

	integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

	afterScaleUpNodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, afterScaleUpNodes.Items, 2, "Expected cluster to scale up from 1 to 2 nodes without panicking")
}
