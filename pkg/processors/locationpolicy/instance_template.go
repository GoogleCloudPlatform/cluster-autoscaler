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
	"fmt"
	"strconv"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/klog/v2"
)

// instanceTemplatePropertiesFromGkeNodeGroup creates InstanceProperties for the purpose of
// Recommend Location API. It fills in required fields that are documented at go/rla-for-ht-nap.
func instanceTemplatePropertiesFromGkeNodeGroup(gkeNodeGroup gke.NodeGroup) *gceclient.InstanceProperties {
	if gkeNodeGroup == nil || gkeNodeGroup.Spec() == nil {
		return nil
	}
	spec := gkeNodeGroup.Spec()
	props := gceclient.InstanceProperties{
		MachineType:    spec.MachineType,
		MinCpuPlatform: spec.MinCpuPlatform,
	}
	fillResourcePolicies(spec, &props)
	fillAccelerators(gkeNodeGroup, &props)
	fillNetworkInterfaces(gkeNodeGroup, &props)
	fillDisks(gkeNodeGroup, &props)
	fillScheduling(gkeNodeGroup, &props)
	fillReservationAffinity(gkeNodeGroup, &props)
	fillConfidentialInstance(gkeNodeGroup, &props)
	return &props
}

func fillResourcePolicies(spec *gkeclient.NodePoolSpec, props *gceclient.InstanceProperties) {
	if spec != nil && spec.PlacementGroup.Policy != "" {
		props.ResourcePolicies = []string{spec.PlacementGroup.Policy}
	}
}

func fillAccelerators(gkeNodeGroup gke.NodeGroup, props *gceclient.InstanceProperties) {
	for _, acc := range gkeNodeGroup.Spec().Accelerators {
		config := &gceclient.AcceleratorConfig{
			AcceleratorType:  acc.AcceleratorType,
			AcceleratorCount: acc.AcceleratorCount,
		}
		props.GuestAccelerators = append(props.GuestAccelerators, config)
	}
}

func fillNetworkInterfaces(gkeNodeGroup gke.NodeGroup, props *gceclient.InstanceProperties) {
	// RLA requires at least one, default network interface
	if gkeNodeGroup.Spec().ClusterSubnetworkPath != "" {
		defaultNetworkInterface := gceclient.NetworkInterface{
			Network:    "https://www.googleapis.com/compute/v1/" + gkeNodeGroup.Spec().ClusterNetworkPath,
			Subnetwork: "https://www.googleapis.com/compute/v1/" + gkeNodeGroup.Spec().ClusterSubnetworkPath,
		}
		props.NetworkInterfaces = append(props.NetworkInterfaces, &defaultNetworkInterface)
	} else {
		// empty network interface is translated by RLA to the default network
		// If "default" network is removed CA must specify the cluster network and subnetwork
		defaultNetworkInterface := gceclient.NetworkInterface{AccessConfig: []*gceclient.NetworkAccessConfig{}}
		props.NetworkInterfaces = append(props.NetworkInterfaces, &defaultNetworkInterface)
	}
	for i, nc := range gkeNodeGroup.Spec().NetworkConfigs {
		props.NetworkInterfaces = append(props.NetworkInterfaces, &gceclient.NetworkInterface{
			Name:              fmt.Sprintf("nic-%v", i+1),
			Network:           nc.VPCNetName,
			Subnetwork:        nc.VPCSubnetName,
			NetworkAttachment: nc.NetworkAttachment,
		})
	}
}

func fillDisks(gkeNodeGroup gke.NodeGroup, props *gceclient.InstanceProperties) {
	spec := gkeNodeGroup.Spec()
	initializeParams := gceclient.AttachedDiskInitializeParams{
		DiskSizeGb: spec.DiskSize,
		DiskType:   spec.DiskType,
	}
	defaultDisk := gceclient.AttachedDisk{
		DiskSizeGb:       spec.DiskSize,
		AutoDelete:       true,
		Boot:             true,
		Type:             "PERSISTENT",
		InitializeParams: &initializeParams,
	}
	props.Disks = append(props.Disks, &defaultDisk)
	for i := 0; i < gkeNodeGroup.GetSCSILLocalSSDCount(); i++ {
		props.Disks = append(props.Disks, &gceclient.AttachedDisk{
			AutoDelete: true,
			Boot:       false,
			Type:       "SCRATCH",
			Interface:  "SCSI",
			InitializeParams: &gceclient.AttachedDiskInitializeParams{
				DiskType: "local-ssd",
			},
		})
	}
	for i := 0; i < gkeNodeGroup.GetNVMELocalSSDCount(); i++ {
		props.Disks = append(props.Disks, &gceclient.AttachedDisk{
			AutoDelete: true,
			Boot:       false,
			Type:       "SCRATCH",
			Interface:  "NVME",
			InitializeParams: &gceclient.AttachedDiskInitializeParams{
				DiskType: "local-ssd",
			},
		})
	}
}

func fillScheduling(gkeNodeGroup gke.NodeGroup, props *gceclient.InstanceProperties) {
	spec := gkeNodeGroup.Spec()

	props.Scheduling = &gceclient.Scheduling{
		Preemptible: spec.Preemptible,
	}
	if spec.Spot {
		props.Scheduling.ProvisioningModel = "Spot"
	}
	if spec.FlexStart {
		props.Scheduling.ProvisioningModel = "FLEX_START"
		props.Scheduling.OnHostMaintenance = "TERMINATE"
		props.Scheduling.InstanceTerminationAction = "DELETE"
	}
	if spec.QueuedProvisioning {
		props.Scheduling.InstanceTerminationAction = "DELETE"
	}

	if spec.MaxRunDurationInSeconds != "" {
		mrd, err := strconv.Atoi(spec.MaxRunDurationInSeconds)
		if err != nil {
			klog.Warningf("Incorrect MRD for gkeNodeGroup spec from node pool %s: %s", gkeNodeGroup.NodePool().Name(), spec.MaxRunDurationInSeconds)
		} else {
			props.Scheduling.MaxRunDuration = &gceclient.Duration{
				Seconds: int64(mrd),
			}
		}
	}
}

func fillReservationAffinity(gkeNodeGroup gke.NodeGroup, props *gceclient.InstanceProperties) {
	affinity := gkeNodeGroup.Spec().ReservationAffinity
	if affinity != nil {
		props.ReservationAffinity = &gceclient.ReservationAffinity{
			ConsumeAllocationType: affinity.ConsumeReservationType,
			Key:                   affinity.Key,
			Values:                affinity.Values,
		}
	}
}

func fillConfidentialInstance(gkeNodeGroup gke.NodeGroup, props *gceclient.InstanceProperties) {
	spec := gkeNodeGroup.Spec()
	if spec.ConfidentialNodeType != "" {
		props.ConfidentialInstanceConfig = &gceclient.ConfidentialInstanceConfig{
			ConfidentialInstanceType: spec.ConfidentialNodeType,
		}
	}
}
