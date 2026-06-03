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

package gkeclient

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	container "google.golang.org/api/container/v1beta1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	caerrors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	test_util "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/selfservice"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	gkeapi "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient/api"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/sandbox"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	"google.golang.org/api/googleapi"
)

const napMaxNodes = 1000

const operationRunningResponse = `{
  "name": "operation-1505728466148-d16f5197",
  "zone": "us-central1-a",
  "operationType": "CREATE_NODE_POOL",
  "status": "RUNNING",
  "selfLink": "https://container.googleapis.com/v1/projects/601024681890/locations/us-central1-a/operations/operation-1505728466148-d16f5197",
  "targetLink": "https://container.googleapis.com/v1/projects/601024681890/locations/us-central1-a/clusters/cluster-1/nodePools/nodeautoprovisioning-323233232",
  "startTime": "2017-09-18T09:54:26.148507311Z",
  "endTime": "2017-09-18T09:54:35.124878859Z"
}`

const operationDoneResponse = `{
  "name": "operation-1505728466148-d16f5197",
  "zone": "us-central1-a",
  "operationType": "CREATE_NODE_POOL",
  "status": "DONE",
  "selfLink": "https://container.googleapis.com/v1/projects/601024681890/locations/us-central1-a/operations/operation-1505728466148-d16f5197",
  "targetLink": "https://container.googleapis.com/v1/projects/601024681890/locations/us-central1-a/clusters/cluster-1/nodePools/nodeautoprovisioning-323233232",
  "startTime": "2017-09-18T09:54:26.148507311Z",
  "endTime": "2017-09-18T09:54:35.124878859Z"
}`

const oneGiBinBytes = 1024 * 1024 * 1024

const privateNodeFromLabel = "internal.private-node-from-label"

func newTestAutoscalingGkeClientV1beta1(t *testing.T, project, location, clusterName, url string) *autoscalingGkeClientV1beta1 {
	*GkeAPIEndpoint = url
	client := &http.Client{}
	machineConfigProvider := machinetypes.NewMachineConfigProvider(nil)

	// TODO(b/485133862): refactor this package to use fake service instead of mocks.
	service, err := gkeapi.NewClient(client, "", *GkeAPIEndpoint)
	if !assert.NoError(t, err) {
		t.Fatalf("fatal error: %v", err)
	}

	gkeClient, err := NewAutoscalingGkeClientV1beta1(service, nil, project, location, clusterName, machineConfigProvider, napMaxNodes)
	if !assert.NoError(t, err) {
		t.Fatalf("fatal error: %v", err)
	}
	return gkeClient
}

func TestCreateNodePoolRequest(t *testing.T) {
	projectName := "test-project"
	location := "test-location"
	clusterName := "test-cluster"
	serverUrl := "test-url"
	nodePoolName := "test-nodepool"

	arm64 := gce.Arm64

	testCases := []struct {
		name        string
		spec        *NodePoolSpec
		wantRequest gke_api_beta.CreateNodePoolRequest
		wantError   bool
	}{
		{
			name:      "nil NodePoolSpec results in error",
			wantError: true,
		},
		{
			name: "Minimal passing spec",
			spec: &NodePoolSpec{},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config:        &gke_api_beta.NodeConfig{},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Node version specified",
			spec: &NodePoolSpec{
				NodeVersion: "1.32.9-gke.1726000",
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config:        &gke_api_beta.NodeConfig{},
					Name:          nodePoolName,
					Version:       "1.32.9-gke.1726000",
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Basic NodeConfig fields",
			spec: &NodePoolSpec{
				Defaults: &gke_api_beta.AutoprovisioningNodePoolDefaults{
					BootDiskKmsKey: "default-key",
				},
				LocationPolicy: "BALANCED",
				MachineType:    "test-machine-type",
				MinCpuPlatform: "test-min-cpu-platform",
				Labels: map[string]string{
					"test-key": "test-value",
				},
				ResourceLabels: map[string]string{
					"test-resource-key": "test-resource-value",
				},
				Accelerators: []*gke_api_beta.AcceleratorConfig{
					{AcceleratorType: "test-gpu"},
				},
				Taints: []v1.Taint{
					{
						Key:   "taint-key",
						Value: "taint-value",
					},
				},
				DiskEncryptionKey: "test-key",
				DiskType:          "test-disk",
				DiskSize:          123,
				Preemptible:       true,
				Spot:              true,
				FlexStart:         true,
				ImageType:         "test-image-type",
				Metadata: map[string]string{
					"test-metadata-key": "test-metadata-value",
				},
				SecondaryBootDisks: []*gke_api_beta.SecondaryBootDisk{
					{
						DiskImage: "test-disk-image",
					},
				},
				ServiceAccount: "test-service-account",
				LinuxNodeConfig: &LinuxNodeConfig{
					Sysctls: map[string]string{
						"test-sysctl-key": "test-sysctl-value",
					},
				},
				KubeletConfig: &gke_api_beta.NodeKubeletConfig{
					CpuManagerPolicy: "test-manager-policy",
				},
				ReservationAffinity: &gke_api_beta.ReservationAffinity{
					Key: "reservation-key",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
						LocationPolicy:  "BALANCED",
					},
					Config: &gke_api_beta.NodeConfig{
						MachineType:    "test-machine-type",
						MinCpuPlatform: "test-min-cpu-platform",
						Labels: map[string]string{
							"test-key": "test-value",
						},
						ResourceLabels: map[string]string{
							"test-resource-key": "test-resource-value",
						},
						Accelerators: []*gke_api_beta.AcceleratorConfig{
							{AcceleratorType: "test-gpu"},
						},
						Taints: []*gke_api_beta.NodeTaint{
							{
								Effect: "EFFECT_UNSPECIFIED",
								Key:    "taint-key",
								Value:  "taint-value",
							},
						},
						BootDiskKmsKey: "test-key",
						DiskType:       "test-disk",
						DiskSizeGb:     123,
						Preemptible:    true,
						Spot:           true,
						FlexStart:      true,
						ImageType:      "test-image-type",
						Metadata: map[string]string{
							"test-metadata-key": "test-metadata-value",
						},
						SecondaryBootDisks: []*gke_api_beta.SecondaryBootDisk{
							{
								DiskImage: "test-disk-image",
							},
						},
						ServiceAccount: "test-service-account",
						LinuxNodeConfig: &gke_api_beta.LinuxNodeConfig{
							Sysctls: map[string]string{
								"test-sysctl-key": "test-sysctl-value",
							},
						},
						KubeletConfig: &gke_api_beta.NodeKubeletConfig{
							CpuManagerPolicy: "test-manager-policy",
						},
						ReservationAffinity: &gke_api_beta.ReservationAffinity{
							Key: "reservation-key",
						},
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "LinuxNodeConfig specified with all fields",
			spec: &NodePoolSpec{
				LinuxNodeConfig: &LinuxNodeConfig{
					AccurateTimeConfig: &AccurateTimeConfig{
						EnablePtpKvmTimeSync: true,
					},
					CgroupMode: "CGROUP_MODE_V2",
					Hugepages: &HugepagesConfig{
						HugepageSize1g: 12345,
						HugepageSize2m: 987654321,
					},
					NodeKernelModuleLoading: &NodeKernelModuleLoading{
						Policy: "ALLOWED",
					},
					SwapConfig: &SwapConfig{
						BootDiskProfile: &BootDiskProfile{
							SwapSizeGib:     5,
							SwapSizePercent: 10,
						},
						DedicatedLocalSsdProfile: &DedicatedLocalSsdProfile{
							DiskCount: 2,
						},
						Enabled: true,
						EncryptionConfig: &EncryptionConfig{
							Disabled: false,
						},
						EphemeralLocalSsdProfile: &EphemeralLocalSsdProfile{
							SwapSizeGib:     10,
							SwapSizePercent: 20,
						},
					},
					Sysctls: map[string]string{
						"net.core.somaxconn": "1024",
						"more-sysctl":        "1",
					},
					TransparentHugepageDefrag:  "madvise",
					TransparentHugepageEnabled: "always",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{
						LinuxNodeConfig: &gke_api_beta.LinuxNodeConfig{
							AccurateTimeConfig: &gke_api_beta.AccurateTimeConfig{
								EnablePtpKvmTimeSync: true,
							},
							CgroupMode: "CGROUP_MODE_V2",
							Hugepages: &gke_api_beta.HugepagesConfig{
								HugepageSize1g: 12345,
								HugepageSize2m: 987654321,
							},
							NodeKernelModuleLoading: &gke_api_beta.NodeKernelModuleLoading{
								Policy: "ALLOWED",
							},
							SwapConfig: &gke_api_beta.SwapConfig{
								BootDiskProfile: &gke_api_beta.BootDiskProfile{
									SwapSizeGib:     5,
									SwapSizePercent: 10,
								},
								DedicatedLocalSsdProfile: &gke_api_beta.DedicatedLocalSsdProfile{
									DiskCount: 2,
								},
								Enabled: true,
								EncryptionConfig: &gke_api_beta.EncryptionConfig{
									Disabled: false,
								},
								EphemeralLocalSsdProfile: &gke_api_beta.EphemeralLocalSsdProfile{
									SwapSizeGib:     10,
									SwapSizePercent: 20,
								},
							},
							Sysctls: map[string]string{
								"net.core.somaxconn": "1024",
								"more-sysctl":        "1",
							},
							TransparentHugepageDefrag:  "madvise",
							TransparentHugepageEnabled: "always",
						},
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "MaxRunDurationInSeconds specified",
			spec: &NodePoolSpec{
				MaxRunDurationInSeconds: "1234",
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{
						MaxRunDuration: "1234s",
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "ConsolidationDelay specified",
			spec: &NodePoolSpec{
				ConsolidationDelayInSeconds: "1234",
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{
						ConsolidationDelay: "1234s",
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "GVisor specified",
			spec: &NodePoolSpec{
				SandboxType: sandbox.GVisor,
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{
						SandboxConfig: &gke_api_beta.SandboxConfig{
							Type: sandbox.GVisor.String(),
						},
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "System architecture is Arm64",
			spec: &NodePoolSpec{
				SystemArchitecture: &arm64,
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{
						Gvnic: &gke_api_beta.VirtualNIC{Enabled: true},
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Local SSD is specified",
			spec: &NodePoolSpec{
				LocalSSDConfig: &LocalSSDConfig{
					EphemeralStorageConfig: &gke_api_beta.EphemeralStorageConfig{
						LocalSsdCount: 1234,
					},
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{
						EphemeralStorageConfig: &gke_api_beta.EphemeralStorageConfig{
							LocalSsdCount: 1234,
						},
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Placement policy with TPU Multihost specified",
			spec: &NodePoolSpec{
				TpuMultiHost: true,
				MachineType:  "ct4p-hightpu-4t",
				TpuTopology:  "2x2x2",
				PlacementGroup: placement.Spec{
					GroupId: "test-group-id",
					Policy:  "test-policy",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    2,
					},
					Config: &gke_api_beta.NodeConfig{
						MachineType: "ct4p-hightpu-4t",
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						PolicyName:  "test-policy",
						TpuTopology: "2x2x2",
						Type:        "COMPACT",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Placement policy with invalid TPU Multihost specified",
			spec: &NodePoolSpec{
				TpuMultiHost: true,
				MachineType:  "ct4p-hightpu-4t",
				TpuTopology:  "invalid",
				PlacementGroup: placement.Spec{
					GroupId: "test-group-id",
					Policy:  "test-policy",
				},
			},
			wantError: true,
		},
		{
			name: "Workload policy with TPU7x Multihost specified",
			spec: &NodePoolSpec{
				TpuMultiHost: true,
				MachineType:  "tpu7x-standard-4t",
				TpuTopology:  "2x2x2",
				PlacementGroup: placement.Spec{
					Policy: "tpu7x-wp",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    2,
					},
					Config: &gke_api_beta.NodeConfig{
						MachineType: "tpu7x-standard-4t",
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						PolicyName:  "tpu7x-wp",
						Type:        "TYPE_UNSPECIFIED",
						TpuTopology: "2x2x2",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Placement policy without TPU Multihost specified, machine type with compact placement supported",
			spec: &NodePoolSpec{
				MachineType: "c2-standard-4",
				PlacementGroup: placement.Spec{
					GroupId: "test-group-id",
					Policy:  "test-policy",
				},
				Defaults: &gke_api_beta.AutoprovisioningNodePoolDefaults{
					UpgradeSettings: &gke_api_beta.UpgradeSettings{
						MaxUnavailable: 1234,
					},
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    150,
					},
					Config: &gke_api_beta.NodeConfig{
						MachineType: "c2-standard-4",
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						PolicyName: "test-policy",
						Type:       "TYPE_UNSPECIFIED",
					},
					UpgradeSettings: &gke_api_beta.UpgradeSettings{
						MaxUnavailable: 1234,
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "a4x_flex_with_1x64_topology_-_calculates_maxSize_from_policy,_doesn't_substract_surge",
			spec: &NodePoolSpec{
				MachineType: "a4x-highgpu-4g",
				PlacementGroup: placement.Spec{
					Policy:         "test-policy",
					ResourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "1x64"}},
				},
				FlexStart: true,
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    16,
					},
					Config: &gke_api_beta.NodeConfig{
						MachineType: "a4x-highgpu-4g",
						FlexStart:   true,
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						PolicyName: "test-policy",
						Type:       "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "a4x_queued_with_1x72_topology_-_calculates_maxSize_from_policy,_doesn't_substract_surge",
			spec: &NodePoolSpec{
				MachineType: "a4x-highgpu-4g",
				PlacementGroup: placement.Spec{
					Policy:         "test-policy",
					ResourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "1x64"}},
				},
				FlexStart:          true,
				QueuedProvisioning: true,
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    16,
					},
					Config: &gke_api_beta.NodeConfig{
						MachineType: "a4x-highgpu-4g",
						FlexStart:   true,
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						PolicyName: "test-policy",
						Type:       "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
					QueuedProvisioning: &gke_api_beta.QueuedProvisioning{
						Enabled: true,
					},
				},
			},
		},
		{
			// GPU slices as A4X requires topology, without topology the call to MaxNodes will fail
			name: "a4x_non_flex_with_surge_and_no_topology_-_returnsErr",
			spec: &NodePoolSpec{
				MachineType: "a4x-highgpu-4g",
				PlacementGroup: placement.Spec{
					Policy: "test-policy",
				},
				Defaults: &gke_api_beta.AutoprovisioningNodePoolDefaults{
					UpgradeSettings: &gke_api_beta.UpgradeSettings{
						MaxUnavailable: 1234,
					},
				},
			},
			wantError: true,
		},
		{
			// N2d is a CPU machine that can use placement
			name: "n2d-_machine_type_with_compact_placement_and_SURGE_-_substracts_maxSurge_from_maxSize",
			spec: &NodePoolSpec{
				MachineType: "n2d-standard-4",
				PlacementGroup: placement.Spec{
					Policy: "test-policy",
				},
				Defaults: &gke_api_beta.AutoprovisioningNodePoolDefaults{
					UpgradeSettings: &gke_api_beta.UpgradeSettings{
						MaxSurge: 16,
					},
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						// 150-16= 134
						MaxNodeCount: 134,
					},
					Config: &gke_api_beta.NodeConfig{
						MachineType: "n2d-standard-4",
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						PolicyName: "test-policy",
						Type:       "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
					UpgradeSettings: &gke_api_beta.UpgradeSettings{
						MaxSurge: 16,
					},
				},
			},
		},
		{
			// N2d is a CPU machine that can use placement
			name: "n2d-_machine_type_with_compact_placement_and_no_SURGE_settings_-_substracts_defaultMaxSurge_from_maxSize",
			spec: &NodePoolSpec{
				MachineType: "n2d-standard-4",
				PlacementGroup: placement.Spec{
					Policy: "test-policy",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						// 150-1= 149
						MaxNodeCount: 149,
					},
					Config: &gke_api_beta.NodeConfig{
						MachineType: "n2d-standard-4",
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						PolicyName: "test-policy",
						Type:       "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Reservation block count specified",
			spec: &NodePoolSpec{
				ReservationBlockCount: 1234,
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    1234,
					},
					Config:        &gke_api_beta.NodeConfig{},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Reservation subblock count specified",
			spec: &NodePoolSpec{
				ReservationBlockCount:    1234,
				ReservationSubBlockCount: 1337,
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    1337,
					},
					Config:        &gke_api_beta.NodeConfig{},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Queued provisioning specified",
			spec: &NodePoolSpec{
				QueuedProvisioning: true,
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config:        &gke_api_beta.NodeConfig{},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					QueuedProvisioning: &gke_api_beta.QueuedProvisioning{Enabled: true},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Autopilot managed specified",
			spec: &NodePoolSpec{
				AutopilotManaged: true,
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{
						ShieldedInstanceConfig: &gke_api_beta.ShieldedInstanceConfig{
							EnableIntegrityMonitoring: true,
							EnableSecureBoot:          true,
						},
						ImageType: string(gce.OperatingSystemImageCOSContainerd),
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
					UpgradeSettings: &gke_api_beta.UpgradeSettings{
						Strategy: "SURGE",
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					AutopilotConfig: &gke_api_beta.AutopilotConfig{
						Enabled: true,
					},
				},
			},
		},
		{
			name: "Auto upgrade set to true",
			spec: &NodePoolSpec{
				SelfServiceMetadata: map[string]string{
					labels.AutoUpgradeLabelKey: "true",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config:        &gke_api_beta.NodeConfig{},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Auto upgrade set to false",
			spec: &NodePoolSpec{
				SelfServiceMetadata: map[string]string{
					labels.AutoUpgradeLabelKey: "false",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config:        &gke_api_beta.NodeConfig{},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: false,
					},
				},
			},
		},
		{
			name: "Auto upgrade set to false, but Autopilot managed specified",
			spec: &NodePoolSpec{
				AutopilotManaged: true,
				SelfServiceMetadata: map[string]string{
					labels.AutoUpgradeLabelKey: "false",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{
						ShieldedInstanceConfig: &gke_api_beta.ShieldedInstanceConfig{
							EnableIntegrityMonitoring: true,
							EnableSecureBoot:          true,
						},
						ImageType: string(gce.OperatingSystemImageCOSContainerd),
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
					UpgradeSettings: &gke_api_beta.UpgradeSettings{
						Strategy: "SURGE",
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					AutopilotConfig: &gke_api_beta.AutopilotConfig{
						Enabled: true,
					},
				},
			},
		},
		{
			name: "Auto repair set to true",
			spec: &NodePoolSpec{
				SelfServiceMetadata: map[string]string{
					labels.AutoRepairLabelKey: "true",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config:        &gke_api_beta.NodeConfig{},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Auto repair set to false",
			spec: &NodePoolSpec{
				SelfServiceMetadata: map[string]string{
					labels.AutoRepairLabelKey: "false",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config:        &gke_api_beta.NodeConfig{},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  false,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Auto repair set to false, but Autopilot managed specified",
			spec: &NodePoolSpec{
				AutopilotManaged: true,
				SelfServiceMetadata: map[string]string{
					labels.AutoRepairLabelKey: "false",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{
						ShieldedInstanceConfig: &gke_api_beta.ShieldedInstanceConfig{
							EnableIntegrityMonitoring: true,
							EnableSecureBoot:          true,
						},
						ImageType: string(gce.OperatingSystemImageCOSContainerd),
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
					UpgradeSettings: &gke_api_beta.UpgradeSettings{
						Strategy: "SURGE",
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					AutopilotConfig: &gke_api_beta.AutopilotConfig{
						Enabled: true,
					},
				},
			},
		},
		{
			name: "Default specified auto upgrade and repair < spec specified auto upgrade and repair",
			spec: &NodePoolSpec{
				Defaults: &gke_api_beta.AutoprovisioningNodePoolDefaults{
					Management: &gke_api_beta.NodeManagement{
						AutoUpgrade: false,
						AutoRepair:  false,
					},
				},
				SelfServiceMetadata: map[string]string{
					labels.AutoUpgradeLabelKey: "true",
					labels.AutoRepairLabelKey:  "true",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config:        &gke_api_beta.NodeConfig{},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Auto repair set to invalid value",
			spec: &NodePoolSpec{
				SelfServiceMetadata: map[string]string{
					labels.AutoRepairLabelKey: "invalid",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config:        &gke_api_beta.NodeConfig{},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Auto upgrade set to invalid value",
			spec: &NodePoolSpec{
				SelfServiceMetadata: map[string]string{
					labels.AutoUpgradeLabelKey: "invalid",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config:        &gke_api_beta.NodeConfig{},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Network config specified",
			spec: &NodePoolSpec{
				NetworkConfigs: []AdditionalNetworkConfig{
					TestAdditionalNetworkConfig("net1", "sub1", "range1", 5),
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{},
					Name:   nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{
						AdditionalNodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
							additionalNodeNetworkConfig("net1", "sub1"),
						},
						AdditionalPodNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
							additionalPodNetworkConfig("sub1", "range1", 5),
						},
					},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Confidential Node Type CCC specified",
			spec: &NodePoolSpec{
				SelfServiceMetadata: map[string]string{
					labels.GkeConfidentialNodeType: "test-value",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{
						ConfidentialNodes: &container.ConfidentialNodes{ConfidentialInstanceType: "test-value"},
						Labels: map[string]string{
							labels.GkeConfidentialNodeType: "test-value",
						},
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "CCC self service specified (using example features)",
			spec: &NodePoolSpec{
				SelfServiceMetadata: map[string]string{
					labels.NodePoolGroupNameLabel:  "test-value-1",
					labels.WorkloadTypeLabel:       "test-value-2",
					labels.GkeConfidentialNodeType: "test-value-3",
					labels.AutoRepairLabelKey:      "false",
					labels.AutoUpgradeLabelKey:     "false",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{
						ConfidentialNodes: &container.ConfidentialNodes{ConfidentialInstanceType: "test-value-3"},
						Labels: map[string]string{
							labels.NodePoolGroupNameLabel:  "test-value-1",
							labels.WorkloadTypeLabel:       "test-value-2",
							labels.GkeConfidentialNodeType: "test-value-3",
						},
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  false,
						AutoUpgrade: false,
					},
				},
			},
		},
		{
			name: "Image streaming is true",
			spec: &NodePoolSpec{
				SelfServiceMetadata: map[string]string{
					labels.ImageStreamingLabelKey: "true",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{
						GcfsConfig: &container.GcfsConfig{Enabled: true},
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Secure boot is true",
			spec: &NodePoolSpec{
				SelfServiceMetadata: map[string]string{
					"ShieldedInstanceConfigEnableSecureBoot":          "true",
					"ShieldedInstanceConfigEnableIntegrityMonitoring": "true",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    1000,
					},
					Config: &gke_api_beta.NodeConfig{
						ShieldedInstanceConfig: &container.ShieldedInstanceConfig{
							EnableSecureBoot:          true,
							EnableIntegrityMonitoring: true,
						},
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Image streaming is false",
			spec: &NodePoolSpec{
				SelfServiceMetadata: map[string]string{
					labels.ImageStreamingLabelKey: "false",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config:        &gke_api_beta.NodeConfig{},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Gvnic is true",
			spec: &NodePoolSpec{
				SelfServiceMetadata: map[string]string{
					labels.GvnicLabelKey: "true",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{
						Gvnic: &container.VirtualNIC{Enabled: true},
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Sandbox is set (e.g. gvisor)",
			spec: &NodePoolSpec{
				SelfServiceMetadata: map[string]string{
					labels.SandboxLabelKey: "gvisor",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    1000,
					},
					Config: &gke_api_beta.NodeConfig{
						SandboxConfig: &gke_api_beta.SandboxConfig{
							Type: "gvisor",
						},
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Gvnic is false",
			spec: &NodePoolSpec{
				SelfServiceMetadata: map[string]string{
					labels.GvnicLabelKey: "false",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{
						Gvnic: &container.VirtualNIC{Enabled: false},
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "CCC self service CapacityCheckWaitTimeSeconds specified",
			spec: &NodePoolSpec{
				SelfServiceMetadata: map[string]string{
					labels.CapacityCheckWaitTimeSecondsLabel: "900",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{
						Labels: map[string]string{
							labels.CapacityCheckWaitTimeSecondsLabel: "900",
						},
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "CCC self service LocationPolicy specified",
			spec: &NodePoolSpec{
				SelfServiceMetadata: map[string]string{
					labels.LocationPolicyLabelKey: "BALANCED",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
						LocationPolicy:  "BALANCED",
					},
					Config:        &gke_api_beta.NodeConfig{},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "CCC self service LoggingConfigVariant specified",
			spec: &NodePoolSpec{
				SelfServiceMetadata: map[string]string{
					labels.LoggingConfigVariant: "MAX_THROUGHPUT",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{
						LoggingConfig: &gke_api_beta.NodePoolLoggingConfig{
							VariantConfig: &gke_api_beta.LoggingVariantConfig{
								Variant: "MAX_THROUGHPUT",
							},
						},
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Defaults specified",
			spec: &NodePoolSpec{
				Defaults: &gke_api_beta.AutoprovisioningNodePoolDefaults{
					Management: &gke_api_beta.NodeManagement{
						AutoRepair: true,
					},
					UpgradeSettings: &gke_api_beta.UpgradeSettings{
						MaxUnavailable: 1234,
					},
					OauthScopes: []string{"test-oauth"},
					ShieldedInstanceConfig: &gke_api_beta.ShieldedInstanceConfig{
						EnableSecureBoot: true,
					},
					BootDiskKmsKey: "test-key",
					ServiceAccount: "test-sa",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{
						OauthScopes: []string{"test-oauth"},
						ShieldedInstanceConfig: &gke_api_beta.ShieldedInstanceConfig{
							EnableSecureBoot: true,
						},
						BootDiskKmsKey: "test-key",
						ServiceAccount: "test-sa",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: false,
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					UpgradeSettings: &gke_api_beta.UpgradeSettings{
						MaxUnavailable: 1234,
					},
				},
			},
		},
		{
			name: "Pod range specified",
			spec: &NodePoolSpec{
				PodRange: "test-pod-range",
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{},
					Name:   nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{
						PodRange: "test-pod-range",
					},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "CCC self service Private nodes specified",
			spec: &NodePoolSpec{
				SelfServiceMetadata: selfservice.Metadata{privateNodeFromLabel: "true"},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{},
					Name:   nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{
						EnablePrivateNodes: true,
						ForceSendFields:    []string{"EnablePrivateNodes"},
					},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "CCC self service Workload Metadata specified",
			spec: &NodePoolSpec{
				SelfServiceMetadata: selfservice.Metadata{labels.WorkloadMetadataLabelKey: "GKE_METADATA"},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    1000,
					},
					Config: &gke_api_beta.NodeConfig{
						WorkloadMetadataConfig: &gke_api_beta.WorkloadMetadataConfig{
							Mode: "GKE_METADATA",
						},
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Confidential node type specified",
			spec: &NodePoolSpec{
				ConfidentialNodeType: labels.TDXConfidentialNodeTypeValue,
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{
						ConfidentialNodes: &container.ConfidentialNodes{
							ConfidentialInstanceType: labels.TDXConfidentialNodeTypeValue,
						},
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Arch taint behavior specified",
			spec: &NodePoolSpec{
				ArchTaintBehavior: "NONE",
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{
						TaintConfig: &gke_api_beta.TaintConfig{
							ArchitectureTaintBehavior: "NONE",
						},
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
		{
			name: "Instance metadata and nodepool config metadata are merged correctly",
			spec: &NodePoolSpec{
				Metadata: map[string]string{
					"np-config-only-key": "from-nodepool-config",
					"overridden-key":     "from-nodepool-config",
				},
				SelfServiceMetadata: map[string]string{
					"instance-metadata.cloud.google.com/priority-only-key": "from-priority",
					"instance-metadata.cloud.google.com/overridden-key":    "from-priority-or-ccc",
				},
			},
			wantRequest: gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Autoscaling: &gke_api_beta.NodePoolAutoscaling{
						Autoprovisioned: true,
						Enabled:         true,
						MaxNodeCount:    napMaxNodes,
					},
					Config: &gke_api_beta.NodeConfig{
						Metadata: map[string]string{
							"np-config-only-key": "from-nodepool-config",
							"priority-only-key":  "from-priority",
							"overridden-key":     "from-priority-or-ccc",
						},
					},
					Name:          nodePoolName,
					NetworkConfig: &gke_api_beta.NodeNetworkConfig{},
					PlacementPolicy: &gke_api_beta.PlacementPolicy{
						Type: "TYPE_UNSPECIFIED",
					},
					Management: &gke_api_beta.NodeManagement{
						AutoRepair:  true,
						AutoUpgrade: true,
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := newTestAutoscalingGkeClientV1beta1(t, projectName, location, clusterName, serverUrl)
			request, err := g.createNodePoolRequest(nodePoolName, tc.spec)
			if tc.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantRequest, request)
			}
		})
	}
}

func TestCreateNodePoolRequest_MaxNodeCount(t *testing.T) {
	defaultMaxSurge := int64(1)

	testCases := []struct {
		name             string
		spec             *NodePoolSpec
		wantMaxNodeCount int64
		wantError        string
	}{
		{
			name:      "nil_NodePoolSpec_results_in_error",
			wantError: "NodePoolSpec is nil",
		},
		{
			name: "tpu_multi_host_with_wrong_topology_-_returns_error",
			spec: &NodePoolSpec{
				TpuMultiHost: true,
				MachineType:  "ct4p-hightpu-4t",
				TpuTopology:  "invalid",
				PlacementGroup: placement.Spec{
					GroupId: "test-group-id",
					Policy:  "test-policy",
				},
			},
			wantError: "invalid topology string: invalid (cannot convert invalid to integer)",
		},
		{
			name: "tpu_multi_host_-_maxSize_equals_tpuTopology_divided_by_chips_per_node,_doesn't_substract_surge",
			spec: &NodePoolSpec{
				TpuMultiHost: true,
				MachineType:  "tpu7x-standard-4t",
				TpuTopology:  "2x2x2",
				PlacementGroup: placement.Spec{
					Policy: "test-policy",
				},
			},
			wantMaxNodeCount: 2,
		},
		{
			name: "a4x_flex_with_1x64_topology_-_maxSize_equals_topology_divided_by_chips_per_node,_doesn't_substract_surge",
			spec: &NodePoolSpec{
				MachineType: "a4x-highgpu-4g",
				PlacementGroup: placement.Spec{
					Policy:         "test-policy",
					ResourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "1x64"}},
				},
				FlexStart: true,
			},
			wantMaxNodeCount: 16,
		},
		{
			name: "a4x_queued_with_1x72_topology_-_maxSize_equals_topology_divided_by_chips_per_node,_doesn't_substract_surge",
			spec: &NodePoolSpec{
				MachineType: "a4x-highgpu-4g",
				PlacementGroup: placement.Spec{
					Policy:         "test-policy",
					ResourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "1x64"}},
				},
				FlexStart:          true,
				QueuedProvisioning: true,
			},
			wantMaxNodeCount: 16,
		},
		{
			name: "a3_non_flex_with_no_topology_-_substracts_maxSurge_from_maxSize",
			spec: &NodePoolSpec{
				MachineType: "a3-highgpu-4g",
				PlacementGroup: placement.Spec{
					Policy: "test-policy",
				},
				UpgradeSettings: &gke_api_beta.UpgradeSettings{
					MaxUnavailable: 1234,
					MaxSurge:       5,
				},
			},
			// 96 - 5 = 91
			wantMaxNodeCount: maxCPNodesForMachineType(t, "a3-highgpu-4g") - 5,
		},
		{
			name: "a3_non_flex_with_no_topology_-_substracts_defaultMaxSurge_from_maxSize",
			spec: &NodePoolSpec{
				MachineType: "a3-highgpu-4g",
				PlacementGroup: placement.Spec{
					Policy: "test-policy",
				},
			},
			// 96 - 1 = 95
			wantMaxNodeCount: maxCPNodesForMachineType(t, "a3-highgpu-4g") - 1,
		},
		{
			// GPU slices as A4X requires topology, without topology the call to MaxNodes will fail
			name: "a4x_non_flex_with_surge_and_no_topology_-_returnsErr",
			spec: &NodePoolSpec{
				MachineType: "a4x-highgpu-4g",
				PlacementGroup: placement.Spec{
					Policy: "test-policy",
				},
			},
			wantError: "machine family \"a4x\" does not support compact placement",
		},
		{
			name: "a4x_flex_with_1x64_topology_-_maxSize_equals_topology_divided_by_chips_per_node,_doesn't_substract_surge",
			spec: &NodePoolSpec{
				MachineType: "a4x-highgpu-4g",
				PlacementGroup: placement.Spec{
					Policy:         "test-policy",
					ResourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "1x64"}},
				},
				FlexStart: true,
			},
			wantMaxNodeCount: 16,
		},
		{
			name: "machine_with_topology_that_does_not_support_placement_-_returns_error",
			spec: &NodePoolSpec{
				MachineType: "c4a-highcpu-72-lssd",
				PlacementGroup: placement.Spec{
					Policy:         "test-policy",
					ResourcePolicy: &gceclient.GceResourcePolicy{WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: "1x84"}},
				},
			},
			wantError: "machine family \"c4a\" does not support compact placement",
		},
		{
			// N2d is a CPU machine that can use placement
			name: "n2d-_machine_type_with_compact_placement_and_SURGE_-_substracts_maxSurge_from_maxSize",
			spec: &NodePoolSpec{
				MachineType: "n2d-standard-4",
				PlacementGroup: placement.Spec{
					Policy: "test-policy",
				},
				UpgradeSettings: &gke_api_beta.UpgradeSettings{
					MaxSurge: 16,
				},
			},
			// 150-16= 134
			wantMaxNodeCount: maxCPNodesForMachineType(t, "n2d-standard-4") - 16,
		},
		{
			// N2d is a CPU machine that can use placement
			name: "n2d-_machine_type_with_compact_placement_and_no_SURGE_settings_-_substracts_defaultMaxSurge_from_maxSize",
			spec: &NodePoolSpec{
				MachineType: "n2d-standard-4",
				PlacementGroup: placement.Spec{
					Policy: "test-policy",
				},
			},
			// 150-1= 149
			wantMaxNodeCount: maxCPNodesForMachineType(t, "n2d-standard-4") - defaultMaxSurge,
		},
		{
			name: "ct3_with_ReservationBlockCount_-_returns_ReservationBlockCount",
			spec: &NodePoolSpec{
				MachineType:           "ct3-hightpu-4t",
				ReservationBlockCount: 156,
			},
			wantMaxNodeCount: 156,
		},
		{
			name: "ct3_with_ReservationBlockCount_and_ReservationSubBlockCount_-_returns_ReservationSubBlockCount",
			spec: &NodePoolSpec{
				MachineType:              "ct3-hightpu-4t",
				ReservationBlockCount:    156,
				ReservationSubBlockCount: 120,
			},
			wantMaxNodeCount: 120,
		},
		{
			name: "ct3_with_no_ReservationBlockCount_and_no_ReservationSubBlockCount_-_returns_default_napMaxNodes",
			spec: &NodePoolSpec{
				MachineType: "ct3-hightpu-4t",
			},
			wantMaxNodeCount: napMaxNodes,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := newTestAutoscalingGkeClientV1beta1(t, "project-name", "us-central1", "cluster-1", "server-test")
			request, err := g.createNodePoolRequest("pool-1", tc.spec)
			if tc.wantError != "" {
				assert.Error(t, err)
				if err != nil && !strings.Contains(err.Error(), tc.wantError) {
					t.Errorf("Expected error to contain %q, but got %q", tc.wantError, err.Error())
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantMaxNodeCount, request.NodePool.Autoscaling.MaxNodeCount)
			}
		})
	}
}

func TestWaitForGkeOp(t *testing.T) {
	server := test_util.NewHttpServerMock()
	defer server.Close()
	g := newTestAutoscalingGkeClientV1beta1(t, "project1", "us-central1-b", "cluster-1", server.URL)

	g.operationPollInterval = 1 * time.Millisecond
	g.operationWaitTimeout = 500 * time.Millisecond

	server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/operations/operation-1505728466148-d16f5197").Return(operationRunningResponse).Once()
	server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/operations/operation-1505728466148-d16f5197").Return(operationDoneResponse).Once()

	operation := &gke_api_beta.Operation{Name: "operation-1505728466148-d16f5197"}

	errStatus := g.waitForGkeOp(operation, "test")
	if errStatus != nil {
		t.Errorf("Expected nil error, got: %+v", errStatus)
	}
	mock.AssertExpectationsForObjects(t, server)
}

func TestGetBlueGreenInfoV1Beta1(t *testing.T) {
	for tn, tc := range map[string]struct {
		apiPool  *gke_api_beta.NodePool
		wantInfo *BlueGreenInfo
		wantErr  error
	}{
		"nil pool": {
			apiPool:  nil,
			wantInfo: nil,
			wantErr:  nil,
		},
		"nil UpdateInfo": {
			apiPool:  &gke_api_beta.NodePool{UpdateInfo: nil},
			wantInfo: nil,
			wantErr:  nil,
		},
		"nil BlueGreenInfo": {
			apiPool:  &gke_api_beta.NodePool{UpdateInfo: &gke_api_beta.UpdateInfo{BlueGreenInfo: nil}},
			wantInfo: nil,
			wantErr:  nil,
		},
		"PHASE_UNSPECIFIED is an error": {
			apiPool: &gke_api_beta.NodePool{
				UpdateInfo: &gke_api_beta.UpdateInfo{
					BlueGreenInfo: &gke_api_beta.BlueGreenInfo{
						Phase: string(PhaseUnspecified),
					},
				},
			},
			wantErr: cmpopts.AnyError,
		},
		"unknown phase is an error": {
			apiPool: &gke_api_beta.NodePool{
				UpdateInfo: &gke_api_beta.UpdateInfo{
					BlueGreenInfo: &gke_api_beta.BlueGreenInfo{
						Phase: "NOT_A_VALID_PHASE",
					},
				},
			},
			wantErr: cmpopts.AnyError,
		},
		"valid BlueGreenInfo": {
			apiPool: &gke_api_beta.NodePool{
				UpdateInfo: &gke_api_beta.UpdateInfo{
					BlueGreenInfo: &gke_api_beta.BlueGreenInfo{
						BlueInstanceGroupUrls:  []string{"blue1", "blue2"},
						GreenInstanceGroupUrls: []string{"green1", "green2"},
						Phase:                  "DELETING_BLUE_POOL",
					},
				},
			},
			wantInfo: &BlueGreenInfo{
				BlueMigUrls:  []string{"blue1", "blue2"},
				GreenMigUrls: []string{"green1", "green2"},
				Phase:        PhaseDeletingBluePool,
			},
		},
		"valid AutoscaledBlueGreen": {
			apiPool: &gke_api_beta.NodePool{
				UpgradeSettings: &gke_api_beta.UpgradeSettings{
					BlueGreenSettings: &gke_api_beta.BlueGreenSettings{
						AutoscaledRolloutPolicy: &gke_api_beta.AutoscaledRolloutPolicy{},
					},
				},
				UpdateInfo: &gke_api_beta.UpdateInfo{
					BlueGreenInfo: &gke_api_beta.BlueGreenInfo{
						BlueInstanceGroupUrls:  []string{"blue1", "blue2"},
						GreenInstanceGroupUrls: []string{"green1", "green2"},
						Phase:                  "WAITING_TO_DRAIN_BLUE_POOL",
					},
				},
			},
			wantInfo: &BlueGreenInfo{
				BlueMigUrls:  []string{"blue1", "blue2"},
				GreenMigUrls: []string{"green1", "green2"},
				Phase:        PhaseWaitingToDrainBluePool,
				Autoscaled:   true,
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			gotInfo, gotErr := getBlueGreenInfoV1Beta1(tc.apiPool)
			if diff := cmp.Diff(tc.wantErr, gotErr, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("getBlueGreenInfoV1Beta1 error diff (-want +got): %s", diff)
			}
			if diff := cmp.Diff(tc.wantInfo, gotInfo, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("getBlueGreenInfoV1Beta1 diff (-want +got): %s", diff)
			}
		})
	}
}

func TestNodePoolFiltering(t *testing.T) {
	for tn, tc := range map[string]struct {
		apiClusterResponse string
		expectNodePool     bool
	}{
		"Keep node pool with RUNNING status": {
			apiClusterResponse: `{
			  "autopilot": {
				"enabled": true
			  },
			  "autoscaling": {
				"enabled": true
			  },
			  "createTime": "2017-10-24T12:20:00+00:00",
			  "nodePools": [
				{
				  "initialNodeCount": 4,
				  "name": "default-pool",
				  "config": {},
				  "autoscaling": {
					"enabled": true,
					"minNodeCount": 1,
					"maxNodeCount": 8,
					"autoprovisioned": false
				  },
				  "status": "RUNNING"
				}
			  ]
			}`,
			expectNodePool: true,
		},
		"Filter out node pool with STOPPING status": {
			apiClusterResponse: `{
			  "autopilot": {
				"enabled": true
			  },
			  "autoscaling": {
				"enabled": true
			  },
			  "createTime": "2017-10-24T12:20:00+00:00",
			  "nodePools": [
				{
				  "initialNodeCount": 4,
				  "name": "default-pool",
				  "config": {},
				  "autoscaling": {
					"enabled": true,
					"minNodeCount": 1,
					"maxNodeCount": 8,
					"autoprovisioned": false
				  },
				  "status": "STOPPING"
				}
			  ]
			}`,
			expectNodePool: false,
		},
		"Filter out node pool with PROVISIONING status": {
			apiClusterResponse: `{
			  "autopilot": {
				"enabled": true
			  },
			  "autoscaling": {
				"enabled": true
			  },
			  "createTime": "2017-10-24T12:20:00+00:00",
			  "nodePools": [
				{
				  "initialNodeCount": 4,
				  "name": "default-pool",
				  "config": {},
				  "autoscaling": {
					"enabled": true,
					"minNodeCount": 1,
					"maxNodeCount": 8,
					"autoprovisioned": false
				  },
				  "status": "PROVISIONING"
				}
			  ]
			}`,
			expectNodePool: false,
		},
		"Keep node pool with STATUS_UNSPECIFIED status": {
			apiClusterResponse: `{
			  "autopilot": {
				"enabled": true
			  },
			  "autoscaling": {
				"enabled": true
			  },
			  "createTime": "2017-10-24T12:20:00+00:00",
			  "nodePools": [
				{
				  "initialNodeCount": 4,
				  "name": "default-pool",
				  "config": {},
				  "autoscaling": {
					"enabled": true,
					"minNodeCount": 1,
					"maxNodeCount": 8,
					"autoprovisioned": false
				  },
				  "status": "STATUS_UNSPECIFIED"
				}
			  ]
			}`,
			expectNodePool: true,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1").Return(tc.apiClusterResponse).Once()
			client := newTestAutoscalingGkeClientV1beta1(t, "project1", "us-central1-b", "cluster-1", server.URL)
			cluster, err := client.GetCluster()
			if err != nil {
				t.Errorf("GetCluster returned %+v and error %v, want no error", cluster, err)
			}
			if tc.expectNodePool != (len(cluster.NodePools) > 0) {
				t.Errorf("Incorrect number of node pools")
			}
		})
	}
}

func TestResourceLimits(t *testing.T) {
	lowestDefaultMinLimits := map[string]int64{
		"cpu":                         0,
		"memory":                      0,
		"nvidia-a100-80gb":            0,
		"nvidia-tesla-a100":           0,
		"nvidia-tesla-k80":            0,
		"nvidia-tesla-p100":           0,
		"nvidia-tesla-p4":             0,
		"nvidia-tesla-t4":             0,
		"nvidia-tesla-v100":           0,
		"nvidia-l4":                   0,
		"nvidia-h100-80gb":            0,
		"nvidia-h100-mega-80gb":       0,
		"nvidia-h200-141gb":           0,
		"nvidia-b200":                 0,
		"nvidia-gb200":                0,
		"nvidia-gb300":                0,
		"nvidia-rtx-pro-6000":         0,
		labels.TpuV3DeviceValue:       0,
		labels.TpuV3SliceValue:        0,
		labels.TpuV4LiteDeviceValue:   0,
		labels.TpuV4PodsliceValue:     0,
		labels.TpuV5LitePodsliceValue: 0,
		labels.TpuV6ESliceValue:       0,
	}
	largestDefaultMaxLimits := map[string]int64{
		"cpu":    6240000,                        // 416 cpus * 15k nodes
		"memory": oneGiBinBytes * 11776 * 15_000, // 11776 GiB mem in bytes * 15k nodes
		// gpu limits are based on the maximum number of gpus on a
		// single machine (largest key in maxCpuCount attribute of Gpu struct) * 15k nodes
		// ref: pkg/cloudprovider/gke/machinetypes/gpu.go#Gpu.maxCpuCount
		"nvidia-a100-80gb":            120000,
		"nvidia-tesla-a100":           240000,
		"nvidia-tesla-k80":            120000,
		"nvidia-tesla-p100":           60000,
		"nvidia-tesla-p4":             60000,
		"nvidia-tesla-t4":             60000,
		"nvidia-tesla-v100":           120000,
		"nvidia-l4":                   120000,
		"nvidia-h100-80gb":            120000,
		"nvidia-h100-mega-80gb":       120000,
		"nvidia-h200-141gb":           120000,
		"nvidia-b200":                 120000,
		"nvidia-gb200":                60000,
		"nvidia-gb300":                60000,
		"nvidia-rtx-pro-6000":         120000,
		labels.TpuV3DeviceValue:       4 * 15000,
		labels.TpuV3SliceValue:        4 * 15000,
		labels.TpuV4LiteDeviceValue:   4 * 15000,
		labels.TpuV4PodsliceValue:     4 * 15000,
		labels.TpuV5LiteDeviceValue:   8 * 15000,
		labels.TpuV5LitePodsliceValue: 8 * 15000,
		labels.TpuV5PSliceValue:       4 * 15000,
		labels.TpuV6ESliceValue:       8 * 15000,
		labels.Tpu7xValue:             4 * 15000,
		labels.Tpu7Value:              4 * 15000,
	}

	for tn, tc := range map[string]struct {
		apiClusterResponse  string
		wantResourceLimiter *cloudprovider.ResourceLimiter
	}{
		"Autopilot mode -> use largest autopilot resource limits": {
			apiClusterResponse: `{
			  "autopilot": {
				"enabled": true
			  },
			  "autoscaling": {
				"enableNodeAutoprovisioning": true,
				"resourceLimits": [
				  {
				    "resourceType": "cpu",
				    "minimum": "4",
				    "maximum": "8"
				  }
				]
			  },
			  "createTime": "2017-10-24T12:20:00+00:00",
			  "nodePools": [
				{
				  "initialNodeCount": 4,
				  "name": "default-pool",
				  "config": {},
				  "autoscaling": {
					"enabled": true,
					"minNodeCount": 1,
					"maxNodeCount": 8,
					"autoprovisioned": false
				  }
				}
			  ]
			}`,
			wantResourceLimiter: cloudprovider.NewResourceLimiter(lowestDefaultMinLimits, largestDefaultMaxLimits),
		},
		"Standard mode, no NAP limits -> largest resource limits": {
			apiClusterResponse: `{
			  "createTime": "2017-10-24T12:20:00+00:00",
			  "nodePools": [
				{
				  "initialNodeCount": 4,
				  "name": "default-pool",
				  "config": {}
				}
			  ]
			}`,
			wantResourceLimiter: cloudprovider.NewResourceLimiter(lowestDefaultMinLimits, largestDefaultMaxLimits),
		},
		"Standard mode, NAP limits defined -> respect cluster resource limits": {
			apiClusterResponse: `{
		      "createTime": "2017-10-24T12:20:00+00:00",
		      "nodePools": [
		        {
		          "initialNodeCount": 4,
		          "name": "default-pool",
		          "config": {},
		          "autoscaling": {
		            "enabled": true,
		            "minNodeCount": 1,
		            "maxNodeCount": 8,
		            "autoprovisioned": false
		          }
		        }
		      ],
		      "autoscaling": {
		        "enableNodeAutoprovisioning": true,
		        "resourceLimits": [
		          {
		            "resourceType": "cpu",
		            "minimum": "4",
		            "maximum": "8"
		          },
		          {
		            "resourceType": "memory",
		            "minimum": "4",
		            "maximum": "16"
		          }
		        ],
		        "autoprovisioningNodePoolDefaults": {},
		        "autoprovisioningLocations": ["us-central1-a", "us-central1-c"]
		      }
		    }`,
			wantResourceLimiter: cloudprovider.NewResourceLimiter(map[string]int64{"cpu": 4, "memory": 4 * units.GiB}, map[string]int64{"cpu": 8, "memory": 16 * units.GiB}),
		},
		"Standard mode, overflowing memory defined -> sanitized cluster resource limits": {
			apiClusterResponse: `{
		      "createTime": "2017-10-24T12:20:00+00:00",
		      "nodePools": [
		        {
		          "initialNodeCount": 4,
		          "name": "default-pool",
		          "config": {},
		          "autoscaling": {
		            "enabled": true,
		            "minNodeCount": 1,
		            "maxNodeCount": 8,
		            "autoprovisioned": false
		          }
		        }
		      ],
		      "autoscaling": {
		        "enableNodeAutoprovisioning": true,
		        "resourceLimits": [
		          {
		            "resourceType": "cpu",
		            "minimum": "4",
		            "maximum": "8"
		          },
		          {
		            "resourceType": "memory",
		            "minimum": "1099511627776",
		            "maximum": "1099511627776"
		          }
		        ],
		        "autoprovisioningNodePoolDefaults": {},
		        "autoprovisioningLocations": ["us-central1-a", "us-central1-c"]
		      }
		    }`,
			wantResourceLimiter: cloudprovider.NewResourceLimiter(map[string]int64{"cpu": 4, "memory": 0}, map[string]int64{"cpu": 8, "memory": math.MaxInt64}),
		},
	} {
		t.Run(tn, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1").Return(tc.apiClusterResponse).Once()
			client := newTestAutoscalingGkeClientV1beta1(t, "project1", "us-central1-b", "cluster-1", server.URL)
			cluster, err := client.GetCluster()
			if err != nil {
				t.Errorf("GetCluster returned %+v and error %v, want no error", cluster, err)
			}
			if tc.wantResourceLimiter == nil && cluster.ResourceLimiter != nil {
				t.Errorf("Cluster ResourceLimiter should be nil")
			}
			if tc.wantResourceLimiter != nil && tc.wantResourceLimiter.String() != cluster.ResourceLimiter.String() {
				t.Errorf("Cluster ResourceLimiter diff (want: %v, got: %v)", tc.wantResourceLimiter, cluster.ResourceLimiter)
			}
		})
	}
}

func TestGetBlueGreenInfoSkippingNodePools(t *testing.T) {
	const (
		nodePoolTemplate = `{
		  "name": "%s",
		  "config": {},
		  "autoscaling": {
			"enabled": true
		  },
		  "updateInfo": {
			"blueGreenInfo": {
			  "phase": "%s",
			  "blueInstanceGroupUrls": [
				"blue-mig-url-1",
				"blue-mig-url-2"
			  ],
			  "greenInstanceGroupUrls": [
				"green-mig-url-1",
				"green-mig-url-2"
			  ]
			}
		  }
		}`
		getClusterResponseTemplate = `{
		  "createTime": "2017-10-24T12:20:00+00:00",
		  "nodePools": %s
		}`
	)
	for tn, tc := range map[string]struct {
		apiPoolBgPhases map[string]string
		wantPoolPhases  map[string]UpdatePhase
	}{
		"no errors -> no skipping": {
			apiPoolBgPhases: map[string]string{
				"pool1": "DELETING_BLUE_POOL",
				"pool2": "CREATING_GREEN_POOL",
			},
			wantPoolPhases: map[string]UpdatePhase{
				"pool1": PhaseDeletingBluePool,
				"pool2": PhaseCreatingGreenPool,
			},
		},
		"node pools with B/G related errors are skipped": {
			apiPoolBgPhases: map[string]string{
				"pool1": "DELETING_BLUE_POOL",
				"pool2": "NOT_A_VALID_PHASE",
				"pool3": string(PhaseUnspecified),
				"pool4": "CREATING_GREEN_POOL",
			},
			wantPoolPhases: map[string]UpdatePhase{
				"pool1": PhaseDeletingBluePool,
				"pool4": PhaseCreatingGreenPool,
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			var apiPools []string
			for poolName, bgPhase := range tc.apiPoolBgPhases {
				apiPools = append(apiPools, fmt.Sprintf(nodePoolTemplate, poolName, bgPhase))
			}
			apiPoolsRepr := fmt.Sprintf("[%s]", strings.Join(apiPools, ","))
			apiClusterResponse := fmt.Sprintf(getClusterResponseTemplate, apiPoolsRepr)
			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1").Return(apiClusterResponse).Once()
			client := newTestAutoscalingGkeClientV1beta1(t, "project1", "us-central1-b", "cluster-1", server.URL)
			cluster, err := client.GetCluster()
			if err != nil {
				t.Errorf("GetCluster returned %+v and error %v, want no error", cluster, err)
			}
			poolPhases := map[string]UpdatePhase{}
			for _, pool := range cluster.NodePools {
				if pool.BlueGreenInfo == nil {
					t.Errorf("Node pool %q: want non-nil BlueGreenInfo, got nil", pool.Name)
					continue
				}
				poolPhases[pool.Name] = pool.BlueGreenInfo.Phase
			}
			if diff := cmp.Diff(tc.wantPoolPhases, poolPhases, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Cluster node pool names diff (-want +got): %s", diff)
			}
		})
	}
}

func TestLocalSSDConfig(t *testing.T) {
	for _, testcase := range []struct {
		machineType string
		count       int64
		expectErr   bool
	}{
		{
			machineType: "a2-ultragpu-2g",
			count:       2,
		},
		{
			machineType: "g2-standard-4",
			count:       4,
		},
		{
			machineType: "weird-invalid-machine",
			count:       0,
		},
	} {
		t.Run(testcase.machineType, func(t *testing.T) {
			testcase := testcase
			spec := NodePoolSpec{
				MachineType:    testcase.machineType,
				LocalSSDConfig: &LocalSSDConfig{EphemeralStorageConfig: &gke_api_beta.EphemeralStorageConfig{LocalSsdCount: testcase.count}},
			}
			config := gke_api_beta.NodeConfig{}
			setLocalSSDConfig(&spec, &config)
			if testcase.count <= 0 {
				assert.Nil(t, config.EphemeralStorageConfig)
			} else {
				assert.NotNil(t, config.EphemeralStorageConfig)
				assert.Equal(t, config.EphemeralStorageConfig.LocalSsdCount, testcase.count)
			}
		})
	}
}

func TestMaxPodsPerNodeConstraintToMppn(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mpc      *gke_api_beta.MaxPodsConstraint
		wantMppn int64
	}{
		{
			name:     "nil max pods constraint",
			mpc:      nil,
			wantMppn: 0,
		}, {
			name: "32 max pods constraint",
			mpc: &gke_api_beta.MaxPodsConstraint{
				MaxPodsPerNode: 32,
			},
			wantMppn: 32,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotMppn := maxPodsPerNode(tc.mpc)
			assert.Equal(t, tc.wantMppn, gotMppn)
		})
	}
}

func TestMaxPodsPerNodeToMaxPodsConstraint(t *testing.T) {
	for _, tc := range []struct {
		name    string
		wantMpc *gke_api_beta.MaxPodsConstraint
		mppn    int64
	}{
		{
			name:    "nil max pods constraint",
			wantMpc: nil,
			mppn:    0,
		}, {
			name: "32 max pods constraint",
			wantMpc: &gke_api_beta.MaxPodsConstraint{
				MaxPodsPerNode: 32,
			},
			mppn: 32,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotMpc := maxPodsConstraint(&NodePoolSpec{MaxPodsPerNode: tc.mppn})
			assert.Equal(t, tc.wantMpc, gotMpc)
		})
	}
}

func TestToInternalNetworkConfig(t *testing.T) {
	for desc, tc := range map[string]struct {
		podNetworkConfigs         []*gke_api_beta.AdditionalPodNetworkConfig
		nodeNetworkConfigs        []*gke_api_beta.AdditionalNodeNetworkConfig
		npMaxPodsPerNode          int64
		clusterMaxPodsPerNode     int64
		clusterNetwork            string
		clusterSubnetwork         string
		wantInternalNetworkConfig []AdditionalNetworkConfig
	}{
		"no additional node network and pod network": {},
		"no additional node network": {
			podNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("sub1", "range", 10),
			},
		},
		"one matching pod network and node network": {
			podNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("sub1", "range", 10),
			},
			nodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
				additionalNodeNetworkConfig("net1", "sub1"),
			},
			wantInternalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfig("net1", "sub1", "range", 10),
			},
		},
		"node networks with and without pod networks": {
			podNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("sub1", "range", 10),
			},
			nodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
				additionalNodeNetworkConfig("net1", "sub1"),
				additionalNodeNetworkConfig("net2", "sub2"),
			},
			wantInternalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfig("net1", "sub1", "range", 10),
				TestAdditionalNetworkConfig("net2", "sub2", "", 0),
			},
		},
		"no pod network": {
			nodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
				additionalNodeNetworkConfig("net1", "sub1"),
			},
			wantInternalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfig("net1", "sub1", "", 0),
			},
		},
		"two matching pod and node networks": {
			podNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("sub1", "range", 10),
				additionalPodNetworkConfig("sub2", "range2", 12),
			},
			nodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
				additionalNodeNetworkConfig("net1", "sub1"),
				additionalNodeNetworkConfig("net2", "sub2"),
			},
			wantInternalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfig("net1", "sub1", "range", 10),
				TestAdditionalNetworkConfig("net2", "sub2", "range2", 12),
			},
		},
		"pod network without max pods per node, node pool and cluster mppn present": {
			podNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("sub1", "range", 0),
			},
			nodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
				additionalNodeNetworkConfig("net1", "sub1"),
			},
			clusterMaxPodsPerNode: 110,
			npMaxPodsPerNode:      32,
			wantInternalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfig("net1", "sub1", "range", 32),
			},
		},
		"pod network without max pods per node,cluster mppn present": {
			podNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("sub1", "range", 0),
			},
			nodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
				additionalNodeNetworkConfig("net1", "sub1"),
			},
			clusterMaxPodsPerNode: 110,
			npMaxPodsPerNode:      0,
			wantInternalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfig("net1", "sub1", "range", 110),
			},
		},
		"2 same subnets for a common network": {
			podNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("sub", "range1", 5),
				additionalPodNetworkConfig("sub", "range2", 5),
			},
			nodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
				additionalNodeNetworkConfig("net", "sub"),
			},
			wantInternalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfig("net", "sub", "range1", 5),
				TestAdditionalNetworkConfig("net", "sub", "range2", 5),
			},
		},
		"2 pairs of same subnets for 2 different networks": {
			podNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("sub1", "range1", 5),
				additionalPodNetworkConfig("sub1", "range2", 5),
				additionalPodNetworkConfig("sub2", "range3", 5),
				additionalPodNetworkConfig("sub2", "range4", 5),
			},
			nodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
				additionalNodeNetworkConfig("net1", "sub1"),
				additionalNodeNetworkConfig("net2", "sub2"),
			},
			wantInternalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfig("net1", "sub1", "range1", 5),
				TestAdditionalNetworkConfig("net1", "sub1", "range2", 5),
				TestAdditionalNetworkConfig("net2", "sub2", "range3", 5),
				TestAdditionalNetworkConfig("net2", "sub2", "range4", 5),
			},
		},
		"2 same subnets for a common network and one extra subnet": {
			podNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("sub1", "range1", 5),
				additionalPodNetworkConfig("sub1", "range2", 5),
				additionalPodNetworkConfig("sub2", "range3", 5),
			},
			nodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
				additionalNodeNetworkConfig("net1", "sub1"),
				additionalNodeNetworkConfig("net1", "sub2"),
			},
			wantInternalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfig("net1", "sub1", "range1", 5),
				TestAdditionalNetworkConfig("net1", "sub1", "range2", 5),
				TestAdditionalNetworkConfig("net1", "sub2", "range3", 5),
			},
		},
		"AdditionalPodNetwork within cluster network": {
			clusterSubnetwork: "my-cluster-subnetwork",
			clusterNetwork:    "my-cluster-network",
			podNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("my-cluster-subnetwork", "range1", 5),
			},
			wantInternalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfig("my-cluster-network", "my-cluster-subnetwork", "range1", 5),
			},
		},
		"a lot of different net combinations": {
			clusterSubnetwork: "my-cluster-subnetwork",
			clusterNetwork:    "my-cluster-network",
			podNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("sub1", "range1", 5),
				additionalPodNetworkConfig("sub1", "range2", 5),
				additionalPodNetworkConfig("sub2", "range3", 5),
				additionalPodNetworkConfig("sub5", "range5", 5),
				additionalPodNetworkConfig("sub6", "range6", 5),
				additionalPodNetworkConfig("sub7", "range7", 5),
				additionalPodNetworkConfig("sub8", "range8", 5),
				additionalPodNetworkConfig("my-cluster-subnetwork", "range1", 5),
			},
			nodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
				additionalNodeNetworkConfig("net1", "sub1"),
				additionalNodeNetworkConfig("net1", "sub2"),
				additionalNodeNetworkConfig("net5", "sub5"),
				additionalNodeNetworkConfig("net6", "sub6"),
				additionalNodeNetworkConfig("net6", "sub7"),
				additionalNodeNetworkConfig("net6", "sub8"),
			},
			wantInternalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfig("net1", "sub1", "range1", 5),
				TestAdditionalNetworkConfig("net1", "sub1", "range2", 5),
				TestAdditionalNetworkConfig("net1", "sub2", "range3", 5),
				TestAdditionalNetworkConfig("net5", "sub5", "range5", 5),
				TestAdditionalNetworkConfig("net6", "sub6", "range6", 5),
				TestAdditionalNetworkConfig("net6", "sub7", "range7", 5),
				TestAdditionalNetworkConfig("net6", "sub8", "range8", 5),
				TestAdditionalNetworkConfig("my-cluster-network", "my-cluster-subnetwork", "range1", 5),
			},
		},
		"node networks and pod networks, where one is with attachment": {
			podNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("sub1", "range", 10),
				additionalPodNetworkConfigWithAttachment(10, "attachment1"),
			},
			nodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
				additionalNodeNetworkConfig("net1", "sub1"),
				additionalNodeNetworkConfig("net2", "sub2"),
			},
			wantInternalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfigWithAttachment(10, "attachment1"),
				TestAdditionalNetworkConfig("net1", "sub1", "range", 10),
				TestAdditionalNetworkConfig("net2", "sub2", "", 0),
			},
		},
		"node networks and pod networks, where two are with attachment": {
			podNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("sub1", "range", 10),
				additionalPodNetworkConfigWithAttachment(15, "attachment1"),
				additionalPodNetworkConfigWithAttachment(30, "attachment2"),
			},
			nodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
				additionalNodeNetworkConfig("net1", "sub1"),
				additionalNodeNetworkConfig("net2", "sub2"),
			},
			wantInternalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfigWithAttachment(15, "attachment1"),
				TestAdditionalNetworkConfigWithAttachment(30, "attachment2"),
				TestAdditionalNetworkConfig("net1", "sub1", "range", 10),
				TestAdditionalNetworkConfig("net2", "sub2", "", 0),
			},
		},
	} {
		t.Run(desc, func(t *testing.T) {
			gotInternalNetworkConfig := toInternalNetworkConfig(
				tc.podNetworkConfigs, tc.nodeNetworkConfigs, tc.npMaxPodsPerNode,
				tc.clusterMaxPodsPerNode, tc.clusterSubnetwork, tc.clusterNetwork)
			if diff := cmp.Diff(gotInternalNetworkConfig, tc.wantInternalNetworkConfig); diff != "" {
				t.Errorf("Unexpected toInternalNetworkConfig: %v", diff)
			}
		})
	}
}

func TestFromInternalNetworkConfig(t *testing.T) {
	for desc, tc := range map[string]struct {
		internalNetworkConfig  []AdditionalNetworkConfig
		clusterSubnetwork      string
		wantPodNetworkConfigs  []*gke_api_beta.AdditionalPodNetworkConfig
		wantNodeNetworkConfigs []*gke_api_beta.AdditionalNodeNetworkConfig
	}{
		"single AdditionalNetworkConfig": {
			internalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfig("net", "sub", "range", 10),
			},
			wantPodNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("sub", "range", 10),
			},
			wantNodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
				additionalNodeNetworkConfig("net", "sub"),
			},
		},
		"double AdditionalNetworkConfig": {
			internalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfig("net", "sub", "range", 10),
				TestAdditionalNetworkConfig("net2", "sub2", "range2", 12),
			},
			wantPodNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("sub", "range", 10),
				additionalPodNetworkConfig("sub2", "range2", 12),
			},
			wantNodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
				additionalNodeNetworkConfig("net", "sub"),
				additionalNodeNetworkConfig("net2", "sub2"),
			},
		},
		"no AdditionalNetworkConfig": {},
		"no max pods per node & subrange": {
			internalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfig("net", "sub", "", 0),
			},
			wantNodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
				additionalNodeNetworkConfig("net", "sub"),
			},
		},
		"no max pods per node": {
			internalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfig("net", "sub", "range", 0),
			},
			wantNodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
				additionalNodeNetworkConfig("net", "sub"),
			},
			wantPodNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("sub", "range", 0),
			},
		},
		"2 same VPCSubnetNames but different SubRanges": {
			internalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfig("net", "sub", "range1", 5),
				TestAdditionalNetworkConfig("net", "sub", "range2", 5),
			},
			wantNodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
				additionalNodeNetworkConfig("net", "sub"),
			},
			wantPodNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("sub", "range1", 5),
				additionalPodNetworkConfig("sub", "range2", 5),
			},
		},
		"2 pairs of same VPCSubnetNames with different SubRanges each": {
			internalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfig("net1", "sub1", "range1", 5),
				TestAdditionalNetworkConfig("net1", "sub1", "range2", 5),
				TestAdditionalNetworkConfig("net2", "sub2", "range3", 5),
				TestAdditionalNetworkConfig("net2", "sub2", "range4", 5),
			},
			wantNodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
				additionalNodeNetworkConfig("net1", "sub1"),
				additionalNodeNetworkConfig("net2", "sub2"),
			},
			wantPodNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("sub1", "range1", 5),
				additionalPodNetworkConfig("sub1", "range2", 5),
				additionalPodNetworkConfig("sub2", "range3", 5),
				additionalPodNetworkConfig("sub2", "range4", 5),
			},
		},
		"2 same VPCSubnetNames but different SubRanges and one extra subnet": {
			internalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfig("net1", "sub1", "range1", 5),
				TestAdditionalNetworkConfig("net1", "sub1", "range2", 5),
				TestAdditionalNetworkConfig("net1", "sub2", "range3", 5),
			},
			wantNodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
				additionalNodeNetworkConfig("net1", "sub1"),
				additionalNodeNetworkConfig("net1", "sub2"),
			},
			wantPodNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("sub1", "range1", 5),
				additionalPodNetworkConfig("sub1", "range2", 5),
				additionalPodNetworkConfig("sub2", "range3", 5),
			},
		},
		"a lot of different net combinations": {
			clusterSubnetwork: "my-cluster-vpc-sub",
			internalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfig("net1", "sub1", "range1", 5),
				TestAdditionalNetworkConfig("net1", "sub1", "range2", 5),
				TestAdditionalNetworkConfig("net1", "sub2", "range3", 5),
				TestAdditionalNetworkConfig("net5", "sub5", "range5", 5),
				TestAdditionalNetworkConfig("net6", "sub6", "range6", 5),
				TestAdditionalNetworkConfig("net6", "sub7", "range7", 5),
				TestAdditionalNetworkConfig("net6", "sub8", "range8", 5),
				TestAdditionalNetworkConfig("my-cluster-vpc", "my-cluster-vpc-sub", "range8", 5),
			},
			wantNodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
				additionalNodeNetworkConfig("net1", "sub1"),
				additionalNodeNetworkConfig("net1", "sub2"),
				additionalNodeNetworkConfig("net5", "sub5"),
				additionalNodeNetworkConfig("net6", "sub6"),
				additionalNodeNetworkConfig("net6", "sub7"),
				additionalNodeNetworkConfig("net6", "sub8"),
			},
			wantPodNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("sub1", "range1", 5),
				additionalPodNetworkConfig("sub1", "range2", 5),
				additionalPodNetworkConfig("sub2", "range3", 5),
				additionalPodNetworkConfig("sub5", "range5", 5),
				additionalPodNetworkConfig("sub6", "range6", 5),
				additionalPodNetworkConfig("sub7", "range7", 5),
				additionalPodNetworkConfig("sub8", "range8", 5),
				additionalPodNetworkConfig("my-cluster-vpc-sub", "range8", 5),
			},
		},
		"AdditionalNetworkConfig with cluster subnetwork": {
			internalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfig("net", "my-vpc-subnet", "range", 10),
			},
			clusterSubnetwork: "my-vpc-subnet",
			wantPodNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("my-vpc-subnet", "range", 10),
			},
		},
		"2 VPCSubnetNames and 1 net with network attachment": {
			internalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfig("net1", "sub1", "range1", 5),
				TestAdditionalNetworkConfig("net2", "sub2", "range3", 5),
				TestAdditionalNetworkConfigWithAttachment(10, "attachment1"),
			},
			wantNodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
				additionalNodeNetworkConfig("net1", "sub1"),
				additionalNodeNetworkConfig("net2", "sub2"),
			},
			wantPodNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("sub1", "range1", 5),
				additionalPodNetworkConfig("sub2", "range3", 5),
				additionalPodNetworkConfigWithAttachment(10, "attachment1"),
			},
		},
		"2 VPCSubnetNames and 2 nets with network attachment": {
			internalNetworkConfig: []AdditionalNetworkConfig{
				TestAdditionalNetworkConfig("net1", "sub1", "range1", 5),
				TestAdditionalNetworkConfig("net2", "sub2", "range3", 5),
				TestAdditionalNetworkConfigWithAttachment(10, "attachment1"),
				TestAdditionalNetworkConfigWithAttachment(8, "attachment2"),
			},
			wantNodeNetworkConfigs: []*gke_api_beta.AdditionalNodeNetworkConfig{
				additionalNodeNetworkConfig("net1", "sub1"),
				additionalNodeNetworkConfig("net2", "sub2"),
			},
			wantPodNetworkConfigs: []*gke_api_beta.AdditionalPodNetworkConfig{
				additionalPodNetworkConfig("sub1", "range1", 5),
				additionalPodNetworkConfig("sub2", "range3", 5),
				additionalPodNetworkConfigWithAttachment(10, "attachment1"),
				additionalPodNetworkConfigWithAttachment(8, "attachment2"),
			},
		},
	} {
		t.Run(desc, func(t *testing.T) {
			gotPodNetworkConfig, gotNodeNetworkConfig := internalNetworkConfigToAdditionalPodAndNodeNetworkConfig(tc.internalNetworkConfig, tc.clusterSubnetwork)
			if diff := cmp.Diff(gotPodNetworkConfig, tc.wantPodNetworkConfigs); diff != "" {
				t.Errorf("Unexpected podNetworkConfig: %v", diff)
			}
			if diff := cmp.Diff(gotNodeNetworkConfig, tc.wantNodeNetworkConfigs); diff != "" {
				t.Errorf("Unexpected nodeNetworkConfig: %v", diff)
			}
		})
	}
}

func TestReleaseChannel(t *testing.T) {
	for _, tc := range []struct {
		name               string
		apiClusterResponse string
		wantReleaseChannel string
	}{
		{
			name: "Rapid channel",
			apiClusterResponse: `{
				"releaseChannel": {
				  "channel": "RAPID"
				},
				"createTime": "2017-10-24T12:20:00+00:00"
			  }`,
			wantReleaseChannel: "RAPID",
		},
		{
			name: "Regular channel",
			apiClusterResponse: `{
				"releaseChannel": {
				  "channel": "REGULAR"
				},
				"createTime": "2017-10-24T12:20:00+00:00"
			  }`,
			wantReleaseChannel: "REGULAR",
		},
		{
			name: "No channel",
			apiClusterResponse: `{
				"createTime": "2017-10-24T12:20:00+00:00"
			  }`,
			wantReleaseChannel: "",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1").Return(tc.apiClusterResponse).Once()
			client := newTestAutoscalingGkeClientV1beta1(t, "project1", "us-central1-b", "cluster-1", server.URL)
			cluster, err := client.GetCluster()
			if err != nil {
				t.Errorf("GetCluster unexpected error: %v, %v", cluster, err)
			}
			if tc.wantReleaseChannel != cluster.ReleaseChannel {
				t.Errorf("Incorrect release channel, want %q, got %q", tc.wantReleaseChannel, cluster.ReleaseChannel)
			}
		})
	}
}

func TestTPU(t *testing.T) {
	for _, tc := range []struct {
		name               string
		apiClusterResponse string
		wantType           string
		wantTopology       string
		wantMultiHost      bool
	}{
		{
			name: "multi-host v4",
			apiClusterResponse: `{
				"autoscaling": {
				  "enableNodeAutoprovisioning": true,
				  "resourceLimits": [
					{
					  "resourceType": "cpu",
					  "minimum": "4",
					  "maximum": "8"
					}
				  ]
				},
				"createTime": "2017-10-24T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "tpu-pool",
					"config": {
					  "machineType": "ct4p-hightpu-4t"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					},
					"placementPolicy": {
					  "type": "COMPACT",
					  "tpuTopology": "2x2x2"
					}
				  }
				]
			  }`,
			wantType:      "tpu-v4-podslice",
			wantTopology:  "2x2x2",
			wantMultiHost: true,
		},
		{
			name: "single-host v4",
			apiClusterResponse: `{
				"autoscaling": {
				  "enableNodeAutoprovisioning": true,
				  "resourceLimits": [
					{
					  "resourceType": "cpu",
					  "minimum": "4",
					  "maximum": "8"
					}
				  ]
				},
				"createTime": "2017-10-24T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "tpu-pool",
					"config": {
					  "machineType": "ct4p-hightpu-4t"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					}
				  }
				]
			  }`,
			wantType:      "tpu-v4-podslice",
			wantTopology:  "2x2x1",
			wantMultiHost: false,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1").Return(tc.apiClusterResponse).Once()
			client := newTestAutoscalingGkeClientV1beta1(t, "project1", "us-central1-b", "cluster-1", server.URL)
			cluster, err := client.GetCluster()
			if err != nil {
				t.Errorf("GetCluster returned %+v and error %v, want no error", cluster, err)
			}
			if len(cluster.NodePools) != 1 {
				t.Errorf("Incorrect number of node pools, want 1, got %v", len(cluster.NodePools))
			}
			np := cluster.NodePools[0]
			if np.Spec.TpuType != tc.wantType {
				t.Errorf("Incorrect tpu type, want %s, got %s", tc.wantType, np.Spec.TpuType)
			}
			if np.Spec.TpuTopology != tc.wantTopology {
				t.Errorf("Incorrect tpu topology, want %s, got %s", tc.wantTopology, np.Spec.TpuTopology)
			}
			if np.Spec.TpuMultiHost != tc.wantMultiHost {
				t.Errorf("Incorrect tpu multi-host value, want %v, got %v", tc.wantMultiHost, np.Spec.TpuMultiHost)
			}
		})
	}
}

func TestDefaultCCC(t *testing.T) {
	for tn, tc := range map[string]struct {
		apiClusterResponse string
		wantDefaultCCC     bool
	}{
		"default_ccc_config_missing": {
			apiClusterResponse: `{
				"autoscaling": {
				  "enableNodeAutoprovisioning": true
				},
				"createTime": "2017-10-24T12:20:00+00:00"
			  }`,
			wantDefaultCCC: false,
		},
		"default_ccc_config_disabled": {
			apiClusterResponse: `{
				"autoscaling": {
				  "enableNodeAutoprovisioning": true,
				  "defaultComputeClassConfig": {
					"enabled": false
				  }
				},
				"createTime": "2017-10-24T12:20:00+00:00"
			  }`,
			wantDefaultCCC: false,
		},
		"default_ccc_config_enabled": {
			apiClusterResponse: `{
				"autoscaling": {
				  "enableNodeAutoprovisioning": true,
				  "defaultComputeClassConfig": {
					"enabled": true
				  }
				},
				"createTime": "2017-10-24T12:20:00+00:00"
			  }`,
			wantDefaultCCC: true,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1").Return(tc.apiClusterResponse).Once()
			client := newTestAutoscalingGkeClientV1beta1(t, "project1", "us-central1-b", "cluster-1", server.URL)
			cluster, err := client.GetCluster()
			if err != nil {
				t.Errorf("GetCluster returned %+v and error %v, want no error", cluster, err)
			}
			if tc.wantDefaultCCC != cluster.DefaultCCCEnabled {
				t.Errorf("Cluster DefaultCCCEnabled diff (want: %v, got: %v)", tc.wantDefaultCCC, cluster.DefaultCCCEnabled)
			}
		})
	}
}

func TestMaxRunDuration(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		apiClusterResponse          string
		wantMaxRunDurationInSeconds string
	}{
		{
			name: "no MaxRunDuration, MaxRunDurationInSeconds field empty",
			apiClusterResponse: `{
				"autoscaling": {
				  "enableNodeAutoprovisioning": true,
				  "resourceLimits": [
					{
					  "resourceType": "cpu",
					  "minimum": "4",
					  "maximum": "8"
					}
				  ]
				},
				"createTime": "2017-10-24T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "mrd-pool",
					"config": {
					  "machineType": "n1-standard-2"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					}
				  }
				]
			  }`,
			wantMaxRunDurationInSeconds: "",
		},
		{
			name: "empty MaxRunDuration, MaxRunDurationInSeconds field empty",
			apiClusterResponse: `{
				"autoscaling": {
				  "enableNodeAutoprovisioning": true,
				  "resourceLimits": [
					{
					  "resourceType": "cpu",
					  "minimum": "4",
					  "maximum": "8"
					}
				  ]
				},
				"createTime": "2017-10-24T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "mrd-pool",
					"config": {
					  "machineType": "n1-standard-2",
					  "maxRunDuration": ""
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					}
				  }
				]
			  }`,
			wantMaxRunDurationInSeconds: "",
		},
		{
			name: "correct MaxRunDuration",
			apiClusterResponse: `{
				"autoscaling": {
				  "enableNodeAutoprovisioning": true,
				  "resourceLimits": [
					{
					  "resourceType": "cpu",
					  "minimum": "4",
					  "maximum": "8"
					}
				  ]
				},
				"createTime": "2017-10-24T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "mrd-pool",
					"config": {
					  "machineType": "n1-standard-2",
					  "maxRunDuration": "172800s"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					}
				  }
				]
			  }`,
			wantMaxRunDurationInSeconds: "172800",
		},
		{
			name: "correct MaxRunDuration, decimals ignored",
			apiClusterResponse: `{
				"autoscaling": {
				  "enableNodeAutoprovisioning": true,
				  "resourceLimits": [
					{
					  "resourceType": "cpu",
					  "minimum": "4",
					  "maximum": "8"
					}
				  ]
				},
				"createTime": "2017-10-24T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "mrd-pool",
					"config": {
					  "machineType": "n1-standard-2",
					  "maxRunDuration": "172800.987s"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					}
				  }
				]
			  }`,
			wantMaxRunDurationInSeconds: "172800",
		},
		{
			name: "unlikely incorrect format MaxRunDuration, MaxRunDurationInSeconds field empty",
			apiClusterResponse: `{
				"autoscaling": {
				  "enableNodeAutoprovisioning": true,
				  "resourceLimits": [
					{
					  "resourceType": "cpu",
					  "minimum": "4",
					  "maximum": "8"
					}
				  ]
				},
				"createTime": "2017-10-24T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "mrd-pool",
					"config": {
					  "machineType": "n1-standard-2",
					  "maxRunDuration": "24h"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					}
				  }
				]
			  }`,
			wantMaxRunDurationInSeconds: "",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1").Return(tc.apiClusterResponse).Once()
			client := newTestAutoscalingGkeClientV1beta1(t, "project1", "us-central1-b", "cluster-1", server.URL)
			cluster, err := client.GetCluster()
			if err != nil {
				t.Errorf("GetCluster returned %+v and error %v, want no error", cluster, err)
			}
			if len(cluster.NodePools) != 1 {
				t.Errorf("Incorrect number of node pools, want 1, got %v", len(cluster.NodePools))
			}
			np := cluster.NodePools[0]
			if np.Spec.MaxRunDurationInSeconds != tc.wantMaxRunDurationInSeconds {
				t.Errorf("Incorrect MaxRunDurationInSeconds, want %q, got %q", tc.wantMaxRunDurationInSeconds, np.Spec.MaxRunDurationInSeconds)
			}
		})
	}
}

func TestConsolidationDelay(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		apiClusterResponse              string
		wantConsolidationDelayInSeconds string
	}{
		{
			name: "apiResponseDoesNotHaveConsolidationDelay_notSetting",
			apiClusterResponse: `{
				"autoscaling": {
				  "enableNodeAutoprovisioning": true,
				  "resourceLimits": [
					{
					  "resourceType": "cpu",
					  "minimum": "4",
					  "maximum": "8"
					}
				  ]
				},
				"createTime": "2017-10-24T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "consopol",
					"config": {
					  "machineType": "n1-standard-2"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					}
				  }
				]
			  }`,
			wantConsolidationDelayInSeconds: "",
		},
		{
			name: "apiResponseHasEmptyConsolidationDelay_notSetting",
			apiClusterResponse: `{
				"autoscaling": {
				  "enableNodeAutoprovisioning": true,
				  "resourceLimits": [
					{
					  "resourceType": "cpu",
					  "minimum": "4",
					  "maximum": "8"
					}
				  ]
				},
				"createTime": "2017-10-24T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "consopol",
					"config": {
					  "machineType": "n1-standard-2",
					  "consolidationDelay": ""
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					}
				  }
				]
			  }`,
			wantConsolidationDelayInSeconds: "",
		},
		{
			name: "invalidFormat_doesNotSetConsolidationDelay",
			apiClusterResponse: `{
				"autoscaling": {
				  "enableNodeAutoprovisioning": true,
				  "resourceLimits": [
					{
					  "resourceType": "cpu",
					  "minimum": "4",
					  "maximum": "8"
					}
				  ]
				},
				"createTime": "2017-10-24T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "consopol",
					"config": {
					  "machineType": "n1-standard-2",
					  "consolidationDelay": "invalidstring"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					}
				  }
				]
			  }`,
			wantConsolidationDelayInSeconds: "",
		},
		{
			name: "correctFormat1_ignoresDecimals",
			apiClusterResponse: `{
				"autoscaling": {
				  "enableNodeAutoprovisioning": true,
				  "resourceLimits": [
					{
					  "resourceType": "cpu",
					  "minimum": "4",
					  "maximum": "8"
					}
				  ]
				},
				"createTime": "2017-10-24T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "consopol",
					"config": {
					  "machineType": "n1-standard-2",
					  "consolidationDelay": "600.567s"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					}
				  }
				]
			  }`,
			wantConsolidationDelayInSeconds: "600",
		},
		{
			name: "correctFormat2_setsConsolidationDelay",
			apiClusterResponse: `{
				"autoscaling": {
				  "enableNodeAutoprovisioning": true,
				  "resourceLimits": [
					{
					  "resourceType": "cpu",
					  "minimum": "4",
					  "maximum": "8"
					}
				  ]
				},
				"createTime": "2017-10-24T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "consopol",
					"config": {
					  "machineType": "n1-standard-2",
					  "consolidationDelay": "600s"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					}
				  }
				]
			  }`,
			wantConsolidationDelayInSeconds: "600",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1").Return(tc.apiClusterResponse).Once()
			client := newTestAutoscalingGkeClientV1beta1(t, "project1", "us-central1-b", "cluster-1", server.URL)
			cluster, err := client.GetCluster()
			if err != nil {
				t.Errorf("GetCluster returned %+v and error %v, want no error", cluster, err)
			}
			if len(cluster.NodePools) != 1 {
				t.Errorf("Incorrect number of node pools, want 1, got %v", len(cluster.NodePools))
			}
			np := cluster.NodePools[0]
			assert.Equal(t, tc.wantConsolidationDelayInSeconds, np.Spec.ConsolidationDelayInSeconds)
		})
	}
}

func TestPodIpv4CidrBlock(t *testing.T) {
	testCases := []struct {
		name               string
		apiClusterResponse string
		wantedCidrBlock    string
	}{
		{
			name: "1st present cidr block get test",
			apiClusterResponse: `{
				"createTime": "2024-04-25T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "tpu-pool",
					"config": {
					  "machineType": "ct4p-hightpu-4t"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					},
					"networkConfig": {
					  "podIpv4CidrBlock": "1.1.1.1/28"
					}
				  }
				]
			  }`,
			wantedCidrBlock: "1.1.1.1/28",
		},
		{
			name: "2nd present cidr block get test",
			apiClusterResponse: `{
				"createTime": "2024-04-25T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "tpu-pool",
					"config": {
					  "machineType": "ct4p-hightpu-4t"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					},
					"networkConfig": {
					  "podIpv4CidrBlock": "2.2.2.2/24"
					}
				  }
				]
			  }`,
			wantedCidrBlock: "2.2.2.2/24",
		},
		{
			name: "Absent cidr block get test",
			apiClusterResponse: `{
				"createTime": "2024-04-25T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "tpu-pool",
					"config": {
					  "machineType": "ct4p-hightpu-4t"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					},
					"networkConfig": {
					}
				  }
				]
			  }`,
			wantedCidrBlock: "",
		},
		{
			name: "Absent networkConfig get test",
			apiClusterResponse: `{
				"createTime": "2024-04-25T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "tpu-pool",
					"config": {
					  "machineType": "ct4p-hightpu-4t"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					}
				  }
				]
			  }`,
			wantedCidrBlock: "",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1").Return(testCase.apiClusterResponse).Once()
			client := newTestAutoscalingGkeClientV1beta1(t, "project1", "us-central1-b", "cluster-1", server.URL)
			cluster, err := client.GetCluster()
			if err != nil {
				t.Errorf("GetCluster returned %+v and error %v, want no error", cluster, err)
			}

			if len(cluster.NodePools) != 1 {
				t.Errorf("Incorrect number of node pools, want 1, got %v", len(cluster.NodePools))
			}
			nodePool := cluster.NodePools[0]
			if nodePool.Spec.PodIpv4CidrBlock != testCase.wantedCidrBlock {
				t.Errorf("Incorrect pod IP version 4 cidr block, wanted %s, got %s", testCase.wantedCidrBlock, nodePool.Spec.PodIpv4CidrBlock)
			}
		})
	}
}

func TestBootDiskKmsKey(t *testing.T) {
	testCases := []struct {
		name               string
		apiClusterResponse string
		wantKmsKey         string
	}{
		{
			name: "BootDiskKmsKey present",
			apiClusterResponse: `{
				"createTime": "2024-08-27T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 1,
					"name": "kms-pool",
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 3
					},
					"config": {
					  "machineType": "n1-standard-1",
					  "bootDiskKmsKey": "projects/p/locations/l/keyRings/k/cryptoKeys/c"
					}
				  }
				]
			  }`,
			wantKmsKey: "projects/p/locations/l/keyRings/k/cryptoKeys/c",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1").Return(tc.apiClusterResponse).Once()

			client := newTestAutoscalingGkeClientV1beta1(t, "project1", "us-central1-b", "cluster-1", server.URL)
			cluster, err := client.GetCluster()
			assert.NoError(t, err)
			assert.Len(t, cluster.NodePools, 1)

			np := cluster.NodePools[0]
			assert.NotNil(t, np.Spec)
			assert.Equal(t, tc.wantKmsKey, np.Spec.DiskEncryptionKey)
		})
	}
}

func TestSecondaryBootDisks(t *testing.T) {
	secondaryBootDisk1 := &gke_api_beta.SecondaryBootDisk{
		DiskImage: "image1",
		Mode:      "CONTAINER_IMAGE_CACHE",
	}

	secondaryBootDisk2 := &gke_api_beta.SecondaryBootDisk{
		DiskImage: "image2",
		Mode:      "MODE_UNSPECIFIED",
	}

	testCases := []struct {
		name                     string
		apiClusterResponse       string
		wantedSecondaryBootDisks []*gke_api_beta.SecondaryBootDisk
	}{
		{
			name: "Test 1st secondary boot disk presence",
			apiClusterResponse: `{
				"createTime": "2024-08-27T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "boot-disks-pool",
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					},
					"config": {
					  "secondaryBootDisks": [
					    {
					    	  "diskImage": "image1",
					    	  "mode": "CONTAINER_IMAGE_CACHE"
					    }
					  ]
					}
				  }
				]
			  }`,
			wantedSecondaryBootDisks: []*gke_api_beta.SecondaryBootDisk{
				secondaryBootDisk1,
			},
		},
		{
			name: "Test 2nd secondary boot disk presence",
			apiClusterResponse: `{
				"createTime": "2024-08-27T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "boot-disks-pool",
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					},
					"config": {
					  "secondaryBootDisks": [
					    {
					    	  "diskImage": "image2",
					    	  "mode": "MODE_UNSPECIFIED"
					    }
					  ]
					}
				  }
				]
			  }`,
			wantedSecondaryBootDisks: []*gke_api_beta.SecondaryBootDisk{
				secondaryBootDisk2,
			},
		},
		{
			name: "Test no secondary boot disks",
			apiClusterResponse: `{
				"createTime": "2024-08-27T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "boot-disks-pool",
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					},
					"config": {
					}
				  }
				]
			  }`,
		},
		{
			name: "Test both secondary boot disks presence",
			apiClusterResponse: `{
				"createTime": "2024-08-27T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "boot-disks-pool",
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					},
					"config": {
					  "secondaryBootDisks": [
					    {
					    	  "diskImage": "image2",
					    	  "mode": "MODE_UNSPECIFIED"
					    },
						{
					    	  "diskImage": "image1",
					    	  "mode": "CONTAINER_IMAGE_CACHE"
					    }
					  ]
					}
				  }
				]
			  }`,
			wantedSecondaryBootDisks: []*gke_api_beta.SecondaryBootDisk{
				secondaryBootDisk2,
				secondaryBootDisk1,
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1").Return(testCase.apiClusterResponse).Once()
			client := newTestAutoscalingGkeClientV1beta1(t, "project1", "us-central1-b", "cluster-1", server.URL)
			cluster, err := client.GetCluster()
			if err != nil {
				t.Errorf("GetCluster returned %+v and error %v, want no error", cluster, err)
			}

			if len(cluster.NodePools) != 1 {
				t.Errorf("Incorrect number of node pools, want 1, got %v", len(cluster.NodePools))
			}

			nodePool := cluster.NodePools[0]
			if diff := cmp.Diff(testCase.wantedSecondaryBootDisks, nodePool.Spec.SecondaryBootDisks); diff != "" {
				t.Errorf("Unexpected secondary boot disks: %v", diff)
			}
		})
	}
}

func TestLinuxNodeConfig(t *testing.T) {
	testCases := []struct {
		name               string
		apiClusterResponse string
		wantNodeConfig     *LinuxNodeConfig
	}{
		{
			name: "linux node config not present",
			apiClusterResponse: `{
				"createTime": "2024-08-27T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 2,
					"name": "foo-bar",
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					},
					"config": {
					}
				  }
				]
			  }`,
			wantNodeConfig: nil,
		},
		{
			name: "sysctls present",
			apiClusterResponse: `{
				"createTime": "2024-08-27T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 2,
					"name": "foo-bar",
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					},
					"config": {
					  "linuxNodeConfig": {
					  	"sysctls": {
					  		"net.core.somaxconn": "1024",
					  		"more-sysctl": "1"
					  	}
					  }
					}
				  }
				]
			  }`,
			wantNodeConfig: &LinuxNodeConfig{
				Sysctls: map[string]string{
					"net.core.somaxconn": "1024",
					"more-sysctl":        "1",
				},
			},
		},
		{
			name: "hugepages present",
			apiClusterResponse: `{
				"createTime": "2024-08-27T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 2,
					"name": "foo-bar",
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					},
					"config": {
					  "linuxNodeConfig": {
					  	"sysctls": {
					  		"net.core.somaxconn": "1024",
					  		"more-sysctl": "1"
					  	},
						"hugepages": {
							"hugepageSize1g": 12345,
							"hugepageSize2m": 987654321
						}
					  }
					}
				  }
				]
			  }`,
			wantNodeConfig: &LinuxNodeConfig{
				Sysctls: map[string]string{
					"net.core.somaxconn": "1024",
					"more-sysctl":        "1",
				},
				Hugepages: &HugepagesConfig{
					HugepageSize1g: 12345,
					HugepageSize2m: 987654321,
				},
			},
		},
		{
			name: "everything present",
			apiClusterResponse: `{
				"createTime": "2024-08-27T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 2,
					"name": "foo-bar",
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					},
					"config": {
					  "linuxNodeConfig": {
						"accurateTimeConfig": {
							"enablePtpKvmTimeSync": true
						},
						"cgroupMode": "CGROUP_MODE_V2",
						"hugepages": {
							"hugepageSize1g": 12345,
							"hugepageSize2m": 987654321
						},
						"nodeKernelModuleLoading": {
							"policy": "ALLOWED"
						},
						"swapConfig": {
							"bootDiskProfile": {
								"swapSizeGib": "5",
								"swapSizePercent": 10
							},
							"dedicatedLocalSsdProfile": {
								"diskCount": "2"
							},
							"enabled": true,
							"encryptionConfig": {
								"disabled": false
							},
							"ephemeralLocalSsdProfile": {
								"swapSizeGib": "10",
								"swapSizePercent": 20
							}
						},
						"sysctls": {
							"net.core.somaxconn": "1024",
							"more-sysctl": "1"
						},
						"transparentHugepageDefrag": "madvise",
						"transparentHugepageEnabled": "always"
					  }
					}
				  }
				]
			  }`,
			wantNodeConfig: &LinuxNodeConfig{
				AccurateTimeConfig: &AccurateTimeConfig{
					EnablePtpKvmTimeSync: true,
				},
				CgroupMode: "CGROUP_MODE_V2",
				Hugepages: &HugepagesConfig{
					HugepageSize1g: 12345,
					HugepageSize2m: 987654321,
				},
				NodeKernelModuleLoading: &NodeKernelModuleLoading{
					Policy: "ALLOWED",
				},
				SwapConfig: &SwapConfig{
					BootDiskProfile: &BootDiskProfile{
						SwapSizeGib:     5,
						SwapSizePercent: 10,
					},
					DedicatedLocalSsdProfile: &DedicatedLocalSsdProfile{
						DiskCount: 2,
					},
					Enabled: true,
					EncryptionConfig: &EncryptionConfig{
						Disabled: false,
					},
					EphemeralLocalSsdProfile: &EphemeralLocalSsdProfile{
						SwapSizeGib:     10,
						SwapSizePercent: 20,
					},
				},
				Sysctls: map[string]string{
					"net.core.somaxconn": "1024",
					"more-sysctl":        "1",
				},
				TransparentHugepageDefrag:  "madvise",
				TransparentHugepageEnabled: "always",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1").Return(tc.apiClusterResponse).Once()
			client := newTestAutoscalingGkeClientV1beta1(t, "project1", "us-central1-b", "cluster-1", server.URL)
			cluster, err := client.GetCluster()
			if err != nil {
				t.Errorf("GetCluster unexpected error: %v, %v", cluster, err)
			}

			if len(cluster.NodePools) != 1 {
				t.Errorf("Incorrect number of node pools, want 1, got %v", len(cluster.NodePools))
			}

			nodePool := cluster.NodePools[0]
			if diff := cmp.Diff(tc.wantNodeConfig, nodePool.Spec.LinuxNodeConfig); diff != "" {
				t.Errorf("Unexpected Linux Node Config: %v", diff)
			}
		})
	}
}

func TestNodeKubeletConfig(t *testing.T) {
	testCases := []struct {
		name               string
		apiClusterResponse string
		wantKubeletConfig  *gke_api_beta.NodeKubeletConfig
	}{
		{
			name: "kubelet config not present",
			apiClusterResponse: `{
				"createTime": "2024-08-27T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 2,
					"name": "foo-bar",
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					},
					"config": {
					}
				  }
				]
			  }`,
			wantKubeletConfig: nil,
		},
		{
			name: "kubelet config present",
			apiClusterResponse: `{
				"createTime": "2024-08-27T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 2,
					"name": "foo-bar",
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					},
					"config": {
					  "kubeletConfig": {
						"cpuCfsQuota": true,
						"cpuCfsQuotaPeriod": "300ms",
						"cpuManagerPolicy": "none"
					  }
					}
				  }
				]
			  }`,
			wantKubeletConfig: &gke_api_beta.NodeKubeletConfig{
				CpuCfsQuota:       true,
				CpuCfsQuotaPeriod: "300ms",
				CpuManagerPolicy:  "none",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1").Return(tc.apiClusterResponse).Once()
			client := newTestAutoscalingGkeClientV1beta1(t, "project1", "us-central1-b", "cluster-1", server.URL)
			cluster, err := client.GetCluster()
			if err != nil {
				t.Errorf("GetCluster unexpected error: %v, %v", cluster, err)
			}

			if len(cluster.NodePools) != 1 {
				t.Errorf("Incorrect number of node pools, want 1, got %v", len(cluster.NodePools))
			}

			nodePool := cluster.NodePools[0]
			if diff := cmp.Diff(tc.wantKubeletConfig, nodePool.Spec.KubeletConfig); diff != "" {
				t.Errorf("Unexpected Node Kubelet Config: %v", diff)
			}
		})
	}
}

func TestSelfServiceFromNodepool(t *testing.T) {
	testCases := []struct {
		name                    string
		apiClusterResponse      string
		wantSelfServiceMetadata map[string]string
	}{
		{
			name: "management field present, auto repair is true",
			apiClusterResponse: `{
				"createTime": "2024-04-25T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "tpu-pool",
					"config": {
					  "machineType": "ct4p-hightpu-4t"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					},
					"management": {
					  "autoRepair": true
					}
				  }
				]
			  }`,
			wantSelfServiceMetadata: map[string]string{
				labels.AutoRepairLabelKey:  "true",
				labels.AutoUpgradeLabelKey: "false",
				labels.GvnicLabelKey:       "true",
			},
		},
		{
			name: "management field present, sandbox is set",
			apiClusterResponse: `{
				"createTime": "2024-04-25T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "tpu-pool",
					"config": {
					  "machineType": "ct4p-hightpu-4t",
					  "sandboxConfig": {
						"type": "gvisor"
					  }
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					},
					"management": {
					  "autoRepair": true
					}
				  }
				]
			  }`,
			wantSelfServiceMetadata: map[string]string{
				labels.AutoRepairLabelKey:  "true",
				labels.AutoUpgradeLabelKey: "false",
				labels.GvnicLabelKey:       "true",
				labels.SandboxLabelKey:     "gvisor",
			},
		},
		{
			name: "management field present, auto repair is false",
			apiClusterResponse: `{
				"createTime": "2024-04-25T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "tpu-pool",
					"config": {
					  "machineType": "ct4p-hightpu-4t"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					},
					"management": {
					  "autoRepair": false
					}
				  }
				]
			  }`,
			wantSelfServiceMetadata: map[string]string{
				labels.AutoRepairLabelKey:  "false",
				labels.AutoUpgradeLabelKey: "false",
				labels.GvnicLabelKey:       "true",
			},
		},
		{
			name: "management field present, auto upgrade is true",
			apiClusterResponse: `{
				"createTime": "2024-04-25T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "tpu-pool",
					"config": {
					  "machineType": "ct4p-hightpu-4t"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					},
					"management": {
					  "autoUpgrade": true
					}
				  }
				]
			  }`,
			wantSelfServiceMetadata: map[string]string{
				labels.AutoRepairLabelKey:  "false",
				labels.AutoUpgradeLabelKey: "true",
				labels.GvnicLabelKey:       "true",
			},
		},
		{
			name: "management field present, auto upgrade is false",
			apiClusterResponse: `{
				"createTime": "2024-04-25T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "tpu-pool",
					"config": {
					  "machineType": "ct4p-hightpu-4t"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					},
					"management": {
					  "autoUpgrade": false
					}
				  }
				]
			  }`,
			wantSelfServiceMetadata: map[string]string{
				labels.AutoRepairLabelKey:  "false",
				labels.AutoUpgradeLabelKey: "false",
				labels.GvnicLabelKey:       "true",
			},
		},
		{
			name: "management field present, everything is set to true",
			apiClusterResponse: `{
				"createTime": "2024-04-25T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "tpu-pool",
					"config": {
					  "machineType": "ct4p-hightpu-4t"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					},
					"management": {
					  "autoUpgrade": true,
					  "autoRepair": true
					}
				  }
				]
			  }`,
			wantSelfServiceMetadata: map[string]string{
				labels.AutoRepairLabelKey:  "true",
				labels.AutoUpgradeLabelKey: "true",
				labels.GvnicLabelKey:       "true",
			},
		},
		{
			name: "management field absent",
			apiClusterResponse: `{
				"createTime": "2024-04-25T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "tpu-pool",
					"config": {
					  "machineType": "ct4p-hightpu-4t"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					}
				  }
				]
			  }`,
			wantSelfServiceMetadata: map[string]string{
				labels.GvnicLabelKey: "true",
			},
		},
		{
			name: "management field is present, nothing is explicitly set",
			apiClusterResponse: `{
				"createTime": "2024-04-25T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "tpu-pool",
					"config": {
					  "machineType": "ct4p-hightpu-4t"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					},
					"management": {}
				  }
				]
			  }`,
			wantSelfServiceMetadata: map[string]string{
				labels.AutoRepairLabelKey:  "false",
				labels.AutoUpgradeLabelKey: "false",
				labels.GvnicLabelKey:       "true",
			},
		},
		{
			name: "Image streaming is enabled",
			apiClusterResponse: `{
				"createTime": "2024-04-25T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "tpu-pool",
					"config": {
					  "machineType": "ct4p-hightpu-4t",
                      "gcfsConfig": {"enabled": true}
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					}
				  }
				]
			  }`,
			wantSelfServiceMetadata: map[string]string{
				labels.ImageStreamingLabelKey: "true",
				labels.GvnicLabelKey:          "true",
			},
		},
		{
			name: "Image streaming is explicitly disabled",
			apiClusterResponse: `{
				"createTime": "2024-04-25T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "tpu-pool",
					"config": {
					  "machineType": "ct4p-hightpu-4t",
                      "gcfsConfig": {"enabled": false}
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					}
				  }
				]
			  }`,
			wantSelfServiceMetadata: map[string]string{
				labels.GvnicLabelKey: "true",
			},
		},
		{
			name: "Image streaming setting is empty",
			apiClusterResponse: `{
				"createTime": "2024-04-25T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "tpu-pool",
					"config": {
					  "machineType": "ct4p-hightpu-4t",
                      "gcfsConfig": {}
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					}
				  }
				]
			  }`,
			wantSelfServiceMetadata: map[string]string{
				labels.GvnicLabelKey: "true",
			},
		},
		{
			name: "Location policy is BALANCED",
			apiClusterResponse: `{
				"createTime": "2024-04-25T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "tpu-pool",
					"config": {
					  "machineType": "ct4p-hightpu-4t"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false,
					  "locationPolicy": "BALANCED"
					}
				  }
				]
			  }`,
			wantSelfServiceMetadata: map[string]string{
				labels.LocationPolicyLabelKey: "BALANCED",
				labels.GvnicLabelKey:          "true",
			},
		},
		{
			name: "Location policy is empty results in empty metadata",
			apiClusterResponse: `{
				"createTime": "2024-04-25T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 4,
					"name": "tpu-pool",
					"config": {
					  "machineType": "ct4p-hightpu-4t"
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false,
					  "locationPolicy": ""
					}
				  }
				]
			  }`,
			wantSelfServiceMetadata: map[string]string{
				labels.GvnicLabelKey: "true",
			},
		},
		{
			name: "Accelerator Network Profile label is present",
			apiClusterResponse: `{
				"createTime": "2024-04-25T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 2,
					"name": "anp-pool",
					"config": {
					  "machineType": "ct4p-hightpu-4t",
					  "labels": {
						"gke.networks.io/accelerator-network-profile": "auto"
					  }
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					}
				  }
				]
			  }`,
			wantSelfServiceMetadata: map[string]string{
				labels.AcceleratorNetworkProfileLabel: "auto",
				labels.GvnicLabelKey:                  "true",
			},
		},
		{
			name: "GpuDirect strategy is RDMA",
			apiClusterResponse: `{
				"createTime": "2024-04-25T12:20:00+00:00",
				"nodePools": [
				  {
					"initialNodeCount": 2,
					"name": "rdma-pool",
					"config": {
					  "machineType": "a3-highgpu-8g",
					  "gpuDirectConfig": {
						"gpuDirectStrategy": "RDMA"
					  }
					},
					"autoscaling": {
					  "enabled": true,
					  "minNodeCount": 1,
					  "maxNodeCount": 8,
					  "autoprovisioned": false
					}
				  }
				]
			  }`,
			wantSelfServiceMetadata: map[string]string{
				labels.GpuDirectLabel: "rdma",
				labels.GvnicLabelKey:  "true",
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1").Return(testCase.apiClusterResponse).Once()
			client := newTestAutoscalingGkeClientV1beta1(t, "project1", "us-central1-b", "cluster-1", server.URL)
			cluster, err := client.GetCluster()
			if err != nil {
				t.Errorf("GetCluster returned %+v and error %v, want no error", cluster, err)
			}

			if len(cluster.NodePools) != 1 {
				t.Errorf("Incorrect number of node pools, want 1, got %v", len(cluster.NodePools))
			}
			nodePool := cluster.NodePools[0]

			assert.Equal(t, testCase.wantSelfServiceMetadata, nodePool.Spec.SelfServiceMetadata)
		})
	}
}

func TestIsHighThroughputLoggingEnabled(t *testing.T) {
	testCase := []struct {
		description     string
		clusterResponse *container.Cluster
		want            bool
	}{
		{
			description: "NodePoolDefaults is nil",
			clusterResponse: &container.Cluster{
				NodePoolDefaults: nil,
			},
			want: false,
		},
		{
			description: "NodePoolDefaults.NodeConfigDefaults is nil",
			clusterResponse: &container.Cluster{
				NodePoolDefaults: &container.NodePoolDefaults{},
			},
			want: false,
		},
		{
			description: "NodePoolDefaults.NodeConfigDefaults.LoggingConfig is nil",
			clusterResponse: &container.Cluster{
				NodePoolDefaults: &container.NodePoolDefaults{
					NodeConfigDefaults: &container.NodeConfigDefaults{},
				},
			},
			want: false,
		},
		{
			description: "NodePoolDefaults.NodeConfigDefaults.LoggingConfig.VariantConfig is nil",
			clusterResponse: &container.Cluster{
				NodePoolDefaults: &container.NodePoolDefaults{
					NodeConfigDefaults: &container.NodeConfigDefaults{
						LoggingConfig: &container.NodePoolLoggingConfig{},
					},
				},
			},
			want: false,
		},
		{
			description: "NodePoolDefaults.NodeConfigDefaults.LoggingConfig.VariantConfig.Variant is set to DEFAULT",
			clusterResponse: &container.Cluster{
				NodePoolDefaults: &container.NodePoolDefaults{
					NodeConfigDefaults: &container.NodeConfigDefaults{
						LoggingConfig: &container.NodePoolLoggingConfig{
							VariantConfig: &container.LoggingVariantConfig{
								Variant: "DEFAULT",
							},
						},
					},
				},
			},
			want: false,
		},
		{
			description: "NodePoolDefaults.NodeConfigDefaults.LoggingConfig.VariantConfig.Variant is set to MAX_THROUGHPUT",
			clusterResponse: &container.Cluster{
				NodePoolDefaults: &container.NodePoolDefaults{
					NodeConfigDefaults: &container.NodeConfigDefaults{
						LoggingConfig: &container.NodePoolLoggingConfig{
							VariantConfig: &container.LoggingVariantConfig{
								Variant: "MAX_THROUGHPUT",
							},
						},
					},
				},
			},
			want: true,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.description, func(t *testing.T) {
			assert.Equal(t, tc.want, isHighThroughputLoggingEnabled(tc.clusterResponse))
		})
	}
}

func TestV1Taint(t *testing.T) {
	testCases := []struct {
		description   string
		nodeTaint     *gke_api_beta.NodeTaint
		expectedTaint v1.Taint
	}{
		{
			description:   "nil taint",
			nodeTaint:     nil,
			expectedTaint: v1.Taint{},
		},
		{
			description: "NO_EXECUTE taint",
			nodeTaint: &gke_api_beta.NodeTaint{
				Key:    "g",
				Value:  "2",
				Effect: "NO_EXECUTE",
			},
			expectedTaint: v1.Taint{
				Key:    "g",
				Value:  "2",
				Effect: v1.TaintEffectNoExecute,
			},
		},
		{
			description: "NO_SCHEDULE taint",
			nodeTaint: &gke_api_beta.NodeTaint{
				Key:    "f",
				Value:  "3",
				Effect: "NO_SCHEDULE",
			},
			expectedTaint: v1.Taint{
				Key:    "f",
				Value:  "3",
				Effect: v1.TaintEffectNoSchedule,
			},
		},
		{
			description: "PREFER_NO_SCHEDULE taint",
			nodeTaint: &gke_api_beta.NodeTaint{
				Key:    "k",
				Value:  "4",
				Effect: "PREFER_NO_SCHEDULE",
			},
			expectedTaint: v1.Taint{
				Key:    "k",
				Value:  "4",
				Effect: v1.TaintEffectPreferNoSchedule,
			},
		},
		{
			description: "taint with empty effect",
			nodeTaint: &gke_api_beta.NodeTaint{
				Key:    "a",
				Value:  "5",
				Effect: "",
			},
			expectedTaint: v1.Taint{
				Key:    "a",
				Value:  "5",
				Effect: v1.TaintEffect(""),
			},
		},
		{
			description: "taint with unknown one word effect",
			nodeTaint: &gke_api_beta.NodeTaint{
				Key:    "bb",
				Value:  "k41",
				Effect: "UNKNOWN",
			},
			expectedTaint: v1.Taint{
				Key:    "bb",
				Value:  "k41",
				Effect: v1.TaintEffect("Unknown"),
			},
		},
		{
			description: "taint with unknown two words effect",
			nodeTaint: &gke_api_beta.NodeTaint{
				Key:    "bc",
				Value:  "k42",
				Effect: "UNKNOWN_EFFECT",
			},
			expectedTaint: v1.Taint{
				Key:    "bc",
				Value:  "k42",
				Effect: v1.TaintEffect("UnknownEffect"),
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			assert.Equal(t, tc.expectedTaint, v1Taint(tc.nodeTaint))
		})
	}
}

func additionalPodNetworkConfig(subnet, podRange string, mppn int64) *gke_api_beta.AdditionalPodNetworkConfig {
	var mpc *gke_api_beta.MaxPodsConstraint
	if mppn != 0 {
		mpc = &gke_api_beta.MaxPodsConstraint{MaxPodsPerNode: mppn}
	}
	return &gke_api_beta.AdditionalPodNetworkConfig{
		Subnetwork:        subnet,
		SecondaryPodRange: podRange,
		MaxPodsPerNode:    mpc,
	}
}

func additionalPodNetworkConfigWithAttachment(mppn int64, networkAttachment string) *gke_api_beta.AdditionalPodNetworkConfig {
	var mpc *gke_api_beta.MaxPodsConstraint
	if mppn != 0 {
		mpc = &gke_api_beta.MaxPodsConstraint{MaxPodsPerNode: mppn}
	}
	return &gke_api_beta.AdditionalPodNetworkConfig{
		MaxPodsPerNode:    mpc,
		NetworkAttachment: networkAttachment,
	}
}

func additionalNodeNetworkConfig(net, subnet string) *gke_api_beta.AdditionalNodeNetworkConfig {
	return &gke_api_beta.AdditionalNodeNetworkConfig{Network: net, Subnetwork: subnet}
}

func TestNodePoolApplyDefaults(t *testing.T) {
	spec := &NodePoolSpec{
		Defaults: &gke_api_beta.AutoprovisioningNodePoolDefaults{
			ServiceAccount: "default@12345.iam.gserviceaccount.com",
		},
	}
	testCases := []struct {
		description        string
		reqServiceAccount  string
		wantServiceAccount string
	}{
		{
			description:        "requested SA is used",
			reqServiceAccount:  "sa@12345.iam.gserviceaccount.com",
			wantServiceAccount: "sa@12345.iam.gserviceaccount.com",
		},
		{
			description:        "default SA is used if not set",
			reqServiceAccount:  "",
			wantServiceAccount: spec.Defaults.ServiceAccount,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			client := newTestAutoscalingGkeClientV1beta1(t, "12345", "us-central1-b", "test-cluster", server.URL)
			req := &gke_api_beta.CreateNodePoolRequest{
				NodePool: &gke_api_beta.NodePool{
					Config: &gke_api_beta.NodeConfig{
						ServiceAccount: tc.reqServiceAccount,
					},
				},
			}

			client.applyDefaults(req, spec.Defaults)

			if got := req.NodePool.Config.ServiceAccount; got != tc.wantServiceAccount {
				t.Errorf("invalid serviceAccount, want: %s, got: %s", tc.wantServiceAccount, got)
			}
		})
	}
}

func TestParseNodePoolCreationError(t *testing.T) {
	nodePoolSpec := &NodePoolSpec{}

	testCases := []struct {
		description             string
		creationError           error
		placementPolicy         *gke_api_beta.PlacementPolicy
		wantAutoscalerErrorType caerrors.AutoscalerErrorType
	}{
		{
			description:             "not a googleapi error -> cloudProviderError",
			creationError:           errors.New("not a googleapi error"),
			wantAutoscalerErrorType: caerrors.CloudProviderError,
		},
		{
			description:             "err -> gkeTooManyRequestsError",
			creationError:           &googleapi.Error{Code: http.StatusTooManyRequests},
			wantAutoscalerErrorType: GkeTooManyRequestsError,
		},
		{
			description:             "err -> gkePersistentOperationError",
			creationError:           &googleapi.Error{Code: http.StatusBadRequest, Message: "Cluster byte size limit reached"},
			wantAutoscalerErrorType: GkePersistentOperationError,
		},
		{
			description:             "err -> invalidTpuTopologyError",
			creationError:           &googleapi.Error{Code: http.StatusBadRequest},
			placementPolicy:         &gke_api_beta.PlacementPolicy{Type: placement.Compact, TpuTopology: "2x2"},
			wantAutoscalerErrorType: tpu.InvalidTpuTopologyError,
		},
		{
			description:             "err -> invalidPlacementGroupNameError",
			creationError:           &googleapi.Error{Code: http.StatusBadRequest},
			placementPolicy:         &gke_api_beta.PlacementPolicy{Type: placement.Compact, TpuTopology: ""},
			wantAutoscalerErrorType: placement.InvalidPlacementGroupNameError,
		},
		{
			description:             "err -> nodeGroupAlreadyExistsError",
			creationError:           &googleapi.Error{Code: http.StatusConflict},
			placementPolicy:         &gke_api_beta.PlacementPolicy{Type: placement.Compact},
			wantAutoscalerErrorType: placement.NodeGroupAlreadyExistsError,
		},
		{
			description:             "not specifically handled googleapi error -> cloudProviderError",
			creationError:           &googleapi.Error{Code: http.StatusTeapot},
			placementPolicy:         &gke_api_beta.PlacementPolicy{Type: placement.Compact},
			wantAutoscalerErrorType: caerrors.CloudProviderError,
		},
		{
			description:             "5xx error -> gkePersistentOperationError",
			creationError:           &googleapi.Error{Code: http.StatusInternalServerError},
			wantAutoscalerErrorType: GkePersistentOperationError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			gotAutoscalerError := parseNodePoolCreationError(tc.creationError, tc.placementPolicy, nodePoolSpec)
			if got, want := gotAutoscalerError.Type(), tc.wantAutoscalerErrorType; got != want {
				t.Errorf("invalid autoscalerError type, want: %v, got: %v", want, got)
			}
		})
	}
}

func TestPlacementGroup(t *testing.T) {
	testCases := []struct {
		name               string
		apiClusterResponse string
		wantPolicy         string
	}{
		{
			name: "placement policy name",
			apiClusterResponse: `{
				"createTime": "2024-04-25T12:20:00+00:00",
				"nodePools": [
				  {
					"config": {
					  "machineType": "ct4p-hightpu-4t"
					},
					"autoscaling": {
					  "enabled": true
					 },
					"placementPolicy": {
					  "policyName": "test-policy"
					}
				  }
				]
			  }`,
			wantPolicy: "test-policy",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			server.
				On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1").
				Return(testCase.apiClusterResponse).
				Once()
			client := newTestAutoscalingGkeClientV1beta1(t, "project1", "us-central1-b", "cluster-1", server.URL)
			cluster, err := client.GetCluster()
			if err != nil {
				t.Fatalf("GetCluster returned %+v and error %v, want no error", cluster, err)
			}

			if len(cluster.NodePools) != 1 {
				t.Fatalf("Incorrect number of node pools, want 1, got %v", len(cluster.NodePools))
			}
			nodePool := cluster.NodePools[0]

			assert.Equal(t, testCase.wantPolicy, nodePool.Spec.PlacementGroup.Policy)
		})
	}
}

func TestConfidentialNodeType(t *testing.T) {
	testCases := []struct {
		name                     string
		apiClusterResponse       string
		wantConfidentialNodeType string
	}{
		{
			name: "Confidential TDX",
			apiClusterResponse: `{
				"createTime": "2024-04-25T12:20:00+00:00",
				"nodePools": [
				  {
					"config": {
					  "machineType": "c3-standard-2",
					  "confidentialNodes": {
					    "confidentialInstanceType": "TDX"
					  }
					},
					"autoscaling": {
					  "enabled": true
					}
				  }
				]
			  }`,
			wantConfidentialNodeType: "TDX",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			server.
				On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1").
				Return(testCase.apiClusterResponse).
				Once()
			client := newTestAutoscalingGkeClientV1beta1(t, "project1", "us-central1-b", "cluster-1", server.URL)
			cluster, err := client.GetCluster()
			if err != nil {
				t.Fatalf("GetCluster returned %+v and error %v, want no error", cluster, err)
			}

			if len(cluster.NodePools) != 1 {
				t.Fatalf("Incorrect number of node pools, want 1, got %v", len(cluster.NodePools))
			}
			nodePool := cluster.NodePools[0]

			assert.Equal(t, testCase.wantConfidentialNodeType, nodePool.Spec.ConfidentialNodeType)
		})
	}
}

func TestEmulatedClusterVersionSetFromClusterProto(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		apiClusterResponse         string
		wantEmulatedClusterVersion string
	}{
		{
			name: "empty currentEmulatedVersion",
			apiClusterResponse: `{
				"currentEmulatedVersion": "",
				"createTime": "2017-10-24T12:20:00+00:00"
			}`,
			wantEmulatedClusterVersion: "",
		},
		{
			name: "non-empty currentEmulatedVersion",
			apiClusterResponse: `{
				"currentEmulatedVersion": "1.34",
				"createTime": "2017-10-24T12:20:00+00:00"
			}`,
			wantEmulatedClusterVersion: "1.34",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := test_util.NewHttpServerMock()
			defer server.Close()
			server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1").Return(tc.apiClusterResponse).Once()
			client := newTestAutoscalingGkeClientV1beta1(t, "project1", "us-central1-b", "cluster-1", server.URL)
			cluster, err := client.GetCluster()
			if err != nil {
				t.Errorf("GetCluster unexpected error: %v, %v", cluster, err)
			}
			if tc.wantEmulatedClusterVersion != cluster.EmulatedClusterVersion {
				t.Errorf("Incorrect EmulatedClusterVersion, want %q, got %q", tc.wantEmulatedClusterVersion, cluster.EmulatedClusterVersion)
			}
		})
	}
}

func TestGetCluster_NodeVersion(t *testing.T) {
	server := test_util.NewHttpServerMock()
	defer server.Close()

	apiClusterResponse := `{
	  "createTime": "2017-10-24T12:20:00+00:00",
	  "nodePools": [
		{
		  "name": "default-pool",
		  "config": {},
		  "version": "1.32.9-gke.1726000",
		  "status": "RUNNING"
		}
	  ]
	}`

	server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1").Return(apiClusterResponse).Once()
	client := newTestAutoscalingGkeClientV1beta1(t, "project1", "us-central1-b", "cluster-1", server.URL)

	cluster, err := client.GetCluster()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(cluster.NodePools))
	assert.Equal(t, "1.32.9-gke.1726000", cluster.NodePools[0].Spec.NodeVersion)
}

func maxCPNodesForMachineType(t *testing.T, machineType string) int64 {
	famiy, err := machinetypes.NewMachineConfigProvider(nil).GetMachineFamilyFromMachineName(machineType)
	if err != nil {
		t.Fatalf("error fetching machne family %v", err)
	}
	ret, err := famiy.MaxCompactPlacementNodes()
	if err != nil {
		t.Fatalf("error fetching max compact placement nodes %v", err)
	}
	return ret
}

func TestGetCluster_ArchTaintBehavior(t *testing.T) {
	server := test_util.NewHttpServerMock()
	defer server.Close()

	apiClusterResponse := `{
	  "createTime": "2017-10-24T12:20:00+00:00",
	  "nodePools": [
			{
				"name": "default-pool",
				"config": {
					"taintConfig": {
						"architectureTaintBehavior": "NONE"
					}
				},
				"status": "RUNNING"
			}
		]
	}`

	server.On("handle", "/v1beta1/projects/project1/locations/us-central1-b/clusters/cluster-1").Return(apiClusterResponse).Once()
	client := newTestAutoscalingGkeClientV1beta1(t, "project1", "us-central1-b", "cluster-1", server.URL)

	cluster, err := client.GetCluster()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(cluster.NodePools))
	assert.Equal(t, "NONE", cluster.NodePools[0].Spec.ArchTaintBehavior)
}
