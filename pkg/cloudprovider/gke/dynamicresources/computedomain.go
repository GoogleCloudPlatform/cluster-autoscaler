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

package dynamicresources

import (
	"fmt"

	apiv1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	mt "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/klog/v2"
)

const (
	ComputeDomainDaemonDeviceType       = "daemon"
	ComputeDomainDaemonDeviceClassName  = "compute-domain-daemon.nvidia.com"
	ComputeDomainChannelDeviceType      = "channel"
	ComputeDomainChannelDeviceClassName = "compute-domain-default-channel.nvidia.com"
	ComputeDomainDriver                 = "compute-domain.nvidia.com"
)

// computeDomainDriverEnabled checks whether ComputeDomain driver is enabled on the node
func computeDomainDriverEnabled(mcp *mt.MachineConfigProvider, node *apiv1.Node, channelDeviceClassFound, daemonDeviceClassFound bool) bool {
	// Since the ComputeDomain driver is not managed by GKE yet, we don't have a label that we can rely on to tell if it's enabled like we do for the other drivers.
	// We can try detecting if the driver is enabled, but precise detection (e.g. checking the driver DaemonSet and its config against the Node) would be very
	// brittle. Less precise detection (e.g. checking if DeviceClasses associated with the driver are created) leaves a lot of room for false positives (e.g. the classes
	// are there but the driver DS is misconfigured).
	//
	// If we assume enabled and the DS is somehow broken, we keep doing scale-ups that don't work. If we assume disabled and the driver is actually healthy and running,
	// we don't scale up for Pods that reference the ComputeDomain claims. Since ComputeDomain is a new, DRA-specific feature it seems better to assume disabled and intentionally
	// not support scale-from-0 for Pods referencing ComputeDomain claims until the driver is managed by GKE.
	//
	// There is one exception where we can't just assume disabled - the A4X machine family. We launched support for A4X in Autopilot (go/gke-nap-a4x-dra) before we enabled DRA
	// integration in CA. The support relies on NAP being able to create A4X node pools for Pods referencing ComputeDomain claims. This is true with DRA integration disabled
	// because the claims are fully ignored during binpacking. With DRA integration enabled, we have to predict the ComputeDomain devices so that binpacking passes for the
	// injected A4X options. In order not to break existing Autopilot workloads targeting A4X, we have to lean towards assuming the driver is enabled and risk entering the
	// bad scale-up loop. So for A4X we try to detect if the driver is enabled based on whether well-known DeviceClasses associated with it are in the cluster. If the classes
	// can't be found, it should pretty safe to assume that the driver wasn't set up at all. Just finding the classes in the cluster doesn't really tell us anything about
	// the driver DaemonSet health, but trying to depend on anything more would be extremely brittle.
	//
	// Details: go/dra-nap-computedomains
	machineFamilyName := node.Labels[gkelabels.MachineFamilyLabel]
	machineFamily, err := mcp.ToMachineFamily(machineFamilyName)
	if err != nil {
		klog.Warningf("Unable to determine machine family for node (%s): %v", node.Name, err)
		return false
	}
	return machineFamily.DraComputeDomainAutoDetection() && channelDeviceClassFound && daemonDeviceClassFound
}

func computeDomainResourceSlicesForNode(templateNode *apiv1.Node) ([]*resourceapi.ResourceSlice, error) {
	if _, exists := templateNode.Labels[labels.GPULabel]; !exists {
		klog.V(4).Infof("Node without GPU label enables DRA GPU driver, not attaching resource slices")
		return nil, nil
	}

	channel := createComputeDomainDevice(ComputeDomainChannelDeviceType, 0)
	daemon := createComputeDomainDevice(ComputeDomainDaemonDeviceType, 0)
	devices := []resourceapi.Device{channel, daemon}
	poolName := computeDomainSlicePoolName(templateNode.Name)
	slices := assignDevicesIntoResourceSlices(templateNode.Name, ComputeDomainDriver, poolName, devices)

	return slices, nil
}

func createComputeDomainDevice(deviceType string, deviceId int64) resourceapi.Device {
	name := fmt.Sprintf("%s-%d", deviceType, deviceId)
	return resourceapi.Device{
		Name: name,
		Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
			"type": stringAttrValue(deviceType),
			"id":   intAttrValue(deviceId),
		},
	}
}

func computeDomainSlicePoolName(nodeName string) string {
	return fmt.Sprintf("%s-%s", nodeName, ComputeDomainDriver)
}
