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

package locationpolicy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	container "google.golang.org/api/container/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
)

var defaultNetworkInterface = gceclient.NetworkInterface{
	AccessConfig: []*gceclient.NetworkAccessConfig{},
}

func TestInstanceTemplateFilled(t *testing.T) {
}

func TestFillAccelerators(t *testing.T) {
	testCases := []struct {
		name  string
		spec  *gkeclient.NodePoolSpec
		props *gceclient.InstanceProperties
		want  *gceclient.InstanceProperties
	}{
		{
			name: "TwoAcceleratorsDefined",
			spec: &gkeclient.NodePoolSpec{
				Accelerators: []*container.AcceleratorConfig{
					{AcceleratorType: "Type1", AcceleratorCount: 1},
					{AcceleratorType: "Type2", AcceleratorCount: 2},
				},
			},
			props: &gceclient.InstanceProperties{},
			want: &gceclient.InstanceProperties{
				GuestAccelerators: []*gceclient.AcceleratorConfig{
					{
						AcceleratorType:  "Type1",
						AcceleratorCount: 1,
					},
					{
						AcceleratorType:  "Type2",
						AcceleratorCount: 2,
					},
				},
			},
		},
		{
			name:  "NoAccelerators",
			spec:  &gkeclient.NodePoolSpec{},
			props: &gceclient.InstanceProperties{},
			want:  &gceclient.InstanceProperties{},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mig := gke.NewTestGkeMigBuilder().SetSpec(tc.spec).Build()
			fillAccelerators(mig, tc.props)
			assert.Equal(t, tc.want, tc.props)
		})
	}
}

func TestFillNetworkInterfaces(t *testing.T) {
	testCases := []struct {
		name  string
		spec  *gkeclient.NodePoolSpec
		props *gceclient.InstanceProperties
		want  *gceclient.InstanceProperties
	}{
		{
			name:  "add default network interface",
			spec:  &gkeclient.NodePoolSpec{},
			props: &gceclient.InstanceProperties{},
			want: &gceclient.InstanceProperties{
				NetworkInterfaces: []*gceclient.NetworkInterface{&defaultNetworkInterface},
			},
		},
		{
			name: "with additional network configs",
			spec: &gkeclient.NodePoolSpec{
				NetworkConfigs: []gkeclient.AdditionalNetworkConfig{
					{VPCNetName: "vpcNet1", VPCSubnetName: "vpcSubnet1", NetworkAttachment: "networkAttachment1"},
				},
			},
			props: &gceclient.InstanceProperties{},
			want: &gceclient.InstanceProperties{
				NetworkInterfaces: []*gceclient.NetworkInterface{
					&defaultNetworkInterface,
					{
						Name:              "nic-1",
						Network:           "vpcNet1",
						Subnetwork:        "vpcSubnet1",
						NetworkAttachment: "networkAttachment1",
					},
				},
			},
		},
		{
			name: "with non-default cluster network",
			spec: &gkeclient.NodePoolSpec{
				ClusterNetworkPath:    "projects/test-project/global/networks/test-network",
				ClusterSubnetworkPath: "projects/test-project/regions/us-central1/subnetworks/test-subnet",
			},
			props: &gceclient.InstanceProperties{},
			want: &gceclient.InstanceProperties{
				NetworkInterfaces: []*gceclient.NetworkInterface{
					{
						Network:    "https://www.googleapis.com/compute/v1/projects/test-project/global/networks/test-network",
						Subnetwork: "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/subnetworks/test-subnet",
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mig := gke.NewTestGkeMigBuilder().SetSpec(tc.spec).Build()
			fillNetworkInterfaces(mig, tc.props)
			assert.Equal(t, tc.want, tc.props)
		})
	}
}

func TestFillDisks(t *testing.T) {
	testCases := []struct {
		name  string
		spec  *gkeclient.NodePoolSpec
		props *gceclient.InstanceProperties
		want  *gceclient.InstanceProperties
	}{
		{
			name: "only default disk",
			spec: &gkeclient.NodePoolSpec{
				DiskType: "boot-disk",
				DiskSize: 100,
			},
			props: &gceclient.InstanceProperties{},
			want: &gceclient.InstanceProperties{
				Disks: []*gceclient.AttachedDisk{
					{
						DiskSizeGb: 100,
						AutoDelete: true,
						Boot:       true,
						Type:       "PERSISTENT",
						InitializeParams: &gceclient.AttachedDiskInitializeParams{
							DiskSizeGb: 100,
							DiskType:   "boot-disk",
						},
					},
				},
			},
		},
		{
			name: "default disk and SCSI local SSD",
			spec: &gkeclient.NodePoolSpec{
				DiskType:       "boot-disk",
				DiskSize:       100,
				LocalSSDConfig: &gkeclient.LocalSSDConfig{LocalSsdCount: 2},
			},
			props: &gceclient.InstanceProperties{},
			want: &gceclient.InstanceProperties{
				Disks: []*gceclient.AttachedDisk{
					{
						DiskSizeGb: 100,
						AutoDelete: true,
						Boot:       true,
						Type:       "PERSISTENT",
						InitializeParams: &gceclient.AttachedDiskInitializeParams{
							DiskSizeGb: 100,
							DiskType:   "boot-disk",
						},
					},
					{
						AutoDelete: true,
						Boot:       false,
						Type:       "SCRATCH",
						Interface:  "SCSI",
						InitializeParams: &gceclient.AttachedDiskInitializeParams{
							DiskType: "local-ssd",
						},
					},
					{
						AutoDelete: true,
						Boot:       false,
						Type:       "SCRATCH",
						Interface:  "SCSI",
						InitializeParams: &gceclient.AttachedDiskInitializeParams{
							DiskType: "local-ssd",
						},
					},
				},
			},
		},
		{
			name: "default disk and NVME local SSD",
			spec: &gkeclient.NodePoolSpec{
				DiskType:       "boot-disk",
				DiskSize:       100,
				LocalSSDConfig: &gkeclient.LocalSSDConfig{EphemeralStorageConfig: &container.EphemeralStorageConfig{LocalSsdCount: 2}},
			},
			props: &gceclient.InstanceProperties{},
			want: &gceclient.InstanceProperties{
				Disks: []*gceclient.AttachedDisk{
					{
						DiskSizeGb: 100,
						AutoDelete: true,
						Boot:       true,
						Type:       "PERSISTENT",
						InitializeParams: &gceclient.AttachedDiskInitializeParams{
							DiskSizeGb: 100,
							DiskType:   "boot-disk",
						},
					},
					{
						AutoDelete: true,
						Boot:       false,
						Type:       "SCRATCH",
						Interface:  "NVME",
						InitializeParams: &gceclient.AttachedDiskInitializeParams{
							DiskType: "local-ssd",
						},
					},
					{
						AutoDelete: true,
						Boot:       false,
						Type:       "SCRATCH",
						Interface:  "NVME",
						InitializeParams: &gceclient.AttachedDiskInitializeParams{
							DiskType: "local-ssd",
						},
					},
				},
			},
		},
		{
			name: "default disk and SCSI local SSD and NVME local SSD",
			spec: &gkeclient.NodePoolSpec{
				DiskType: "boot-disk",
				DiskSize: 100,
				LocalSSDConfig: &gkeclient.LocalSSDConfig{
					LocalSsdCount:          1,
					EphemeralStorageConfig: &container.EphemeralStorageConfig{LocalSsdCount: 1},
				},
			},
			props: &gceclient.InstanceProperties{},
			want: &gceclient.InstanceProperties{
				Disks: []*gceclient.AttachedDisk{
					{
						DiskSizeGb: 100,
						AutoDelete: true,
						Boot:       true,
						Type:       "PERSISTENT",
						InitializeParams: &gceclient.AttachedDiskInitializeParams{
							DiskSizeGb: 100,
							DiskType:   "boot-disk",
						},
					},
					{
						AutoDelete: true,
						Boot:       false,
						Type:       "SCRATCH",
						Interface:  "SCSI",
						InitializeParams: &gceclient.AttachedDiskInitializeParams{
							DiskType: "local-ssd",
						},
					},
					{
						AutoDelete: true,
						Boot:       false,
						Type:       "SCRATCH",
						Interface:  "NVME",
						InitializeParams: &gceclient.AttachedDiskInitializeParams{
							DiskType: "local-ssd",
						},
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mig := gke.NewTestGkeMigBuilder().SetSpec(tc.spec).Build()
			fillDisks(mig, tc.props)
			assert.Equal(t, tc.want, tc.props)
		})
	}
}

func TestFillScheduling(t *testing.T) {
	testCases := []struct {
		name  string
		spec  *gkeclient.NodePoolSpec
		props *gceclient.InstanceProperties
		want  *gceclient.InstanceProperties
	}{
		{
			name: "not a spot node and maxRunDuration undefined",
			spec: &gkeclient.NodePoolSpec{
				Spot:                    false,
				Preemptible:             true,
				MaxRunDurationInSeconds: "",
			},
			props: &gceclient.InstanceProperties{},
			want: &gceclient.InstanceProperties{
				Scheduling: &gceclient.Scheduling{
					Preemptible:       true,
					ProvisioningModel: "",
				},
			},
		},
		{
			name: "spot node and maxRunDuration undefined",
			spec: &gkeclient.NodePoolSpec{
				Spot:                    true,
				Preemptible:             true,
				MaxRunDurationInSeconds: "",
			},
			props: &gceclient.InstanceProperties{},
			want: &gceclient.InstanceProperties{
				Scheduling: &gceclient.Scheduling{
					Preemptible:       true,
					ProvisioningModel: "Spot",
				},
			},
		},
		{
			name: "flex-start node",
			spec: &gkeclient.NodePoolSpec{
				FlexStart: true,
			},
			props: &gceclient.InstanceProperties{},
			want: &gceclient.InstanceProperties{
				Scheduling: &gceclient.Scheduling{
					ProvisioningModel:         "FLEX_START",
					OnHostMaintenance:         "TERMINATE",
					InstanceTerminationAction: "DELETE",
				},
			},
		},
		{
			name: "queued node",
			spec: &gkeclient.NodePoolSpec{
				QueuedProvisioning: true,
			},
			props: &gceclient.InstanceProperties{},
			want: &gceclient.InstanceProperties{
				Scheduling: &gceclient.Scheduling{
					InstanceTerminationAction: "DELETE",
				},
			},
		},
		{
			name: "flex-start queued node",
			spec: &gkeclient.NodePoolSpec{
				FlexStart:          true,
				QueuedProvisioning: true,
			},
			props: &gceclient.InstanceProperties{},
			want: &gceclient.InstanceProperties{
				Scheduling: &gceclient.Scheduling{
					ProvisioningModel:         "FLEX_START",
					OnHostMaintenance:         "TERMINATE",
					InstanceTerminationAction: "DELETE",
				},
			},
		},
		{
			name: "maxRunDuration set",
			spec: &gkeclient.NodePoolSpec{
				Spot:                    false,
				Preemptible:             true,
				MaxRunDurationInSeconds: "10",
			},
			props: &gceclient.InstanceProperties{},
			want: &gceclient.InstanceProperties{
				Scheduling: &gceclient.Scheduling{
					Preemptible:       true,
					ProvisioningModel: "",
					MaxRunDuration: &gceclient.Duration{
						Seconds: 10,
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mig := gke.NewTestGkeMigBuilder().SetSpec(tc.spec).Build()
			fillScheduling(mig, tc.props)
			assert.Equal(t, tc.want, tc.props)
		})
	}
}

func TestInstanceTemplatePropertiesFromMig(t *testing.T) {
	testCases := []struct {
		name  string
		exist bool
		spec  *gkeclient.NodePoolSpec
		want  *gceclient.InstanceProperties
	}{
		{
			name: "mig spec undefined",
			spec: nil,
			want: nil,
		},
		{
			name:  "valid mig",
			exist: true,
			spec: &gkeclient.NodePoolSpec{
				Accelerators: []*container.AcceleratorConfig{
					{AcceleratorType: "Type1", AcceleratorCount: 1},
					{AcceleratorType: "Type2", AcceleratorCount: 2},
				},
				DiskType:                "boot-disk",
				DiskSize:                100,
				LocalSSDConfig:          &gkeclient.LocalSSDConfig{LocalSsdCount: 1},
				Spot:                    false,
				Preemptible:             true,
				MaxRunDurationInSeconds: "10",
				MachineType:             "machine-type-1",
				MinCpuPlatform:          "intel",
				ConfidentialNodeType:    "TDX",
			},
			want: &gceclient.InstanceProperties{
				GuestAccelerators: []*gceclient.AcceleratorConfig{
					{
						AcceleratorType:  "Type1",
						AcceleratorCount: 1,
					},
					{
						AcceleratorType:  "Type2",
						AcceleratorCount: 2,
					},
				},
				NetworkInterfaces: []*gceclient.NetworkInterface{&defaultNetworkInterface},
				Disks: []*gceclient.AttachedDisk{
					{
						DiskSizeGb: 100,
						AutoDelete: true,
						Boot:       true,
						Type:       "PERSISTENT",
						InitializeParams: &gceclient.AttachedDiskInitializeParams{
							DiskSizeGb: 100,
							DiskType:   "boot-disk",
						},
					},
					{
						AutoDelete: true,
						Boot:       false,
						Type:       "SCRATCH",
						Interface:  "SCSI",
						InitializeParams: &gceclient.AttachedDiskInitializeParams{
							DiskType: "local-ssd",
						},
					},
				},
				Scheduling: &gceclient.Scheduling{
					Preemptible:       true,
					ProvisioningModel: "",
					MaxRunDuration: &gceclient.Duration{
						Seconds: 10,
					},
				},
				MachineType:    "machine-type-1",
				MinCpuPlatform: "intel",
				ConfidentialInstanceConfig: &gceclient.ConfidentialInstanceConfig{
					ConfidentialInstanceType: "TDX",
				},
			},
		},
		{
			name:  "resourcePolicies",
			exist: true,
			spec: &gkeclient.NodePoolSpec{
				MachineType: "machine-type-1",
				PlacementGroup: placement.Spec{
					Policy: "wp123",
				},
			},
			want: &gceclient.InstanceProperties{
				MachineType:      "machine-type-1",
				ResourcePolicies: []string{"wp123"},
				NetworkInterfaces: []*gceclient.NetworkInterface{
					{
						AccessConfig: []*gceclient.NetworkAccessConfig{},
					},
				},
				Disks: []*gceclient.AttachedDisk{
					{
						DiskSizeGb:       0,
						AutoDelete:       true,
						Boot:             true,
						Type:             "PERSISTENT",
						InitializeParams: &gceclient.AttachedDiskInitializeParams{},
					},
				},
				Scheduling: &gceclient.Scheduling{},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mig := gke.NewTestGkeMigBuilder().SetExist(tc.exist).SetSpec(tc.spec).Build()
			props := instanceTemplatePropertiesFromGkeNodeGroup(mig)
			assert.Equal(t, tc.want, props)
		})
	}
}
