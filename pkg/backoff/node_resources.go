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

package backoff

import (
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/processors/customresources"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/klog/v2"
)

// NodeResourceCostClass represents cost class of resource
type NodeResourceCostClass int

const (
	// StandardCostClass represent resources of standard cost (default handling logic)
	StandardCostClass NodeResourceCostClass = 1
	// ExpensiveCostClass represent expensive resources (logic limiting overprovisioning)
	ExpensiveCostClass NodeResourceCostClass = 2
)

type NodeResourceCapacityClass string

const (
	// StandardProvisioningClass represents Standard/Spot nodes
	StandardCapacityClass NodeResourceCapacityClass = "STANDARD"
	// FlexStartProvisioningClass represents DWS nodes, which have separate capacity
	FlexStartCapacityClass NodeResourceCapacityClass = "FLEX"
)

// NodeResource describe a single resource type consumed by cluster node
type NodeResource struct {
	// Location determines location of resource.
	// It represents compute zone/region; e.g. "us-central1-c", "us-central1"
	// or meta-location like "global"
	Location string

	// Type names type of a resource (cpu, memory, specific GPU chip, ...)
	Type string

	// Machine family of the resource.
	MachineFamily string

	// Value represents quantity of the resource
	Value int64

	// CostClass represents cost class of resource.
	CostClass NodeResourceCostClass

	CapacityClass NodeResourceCapacityClass
}

func (n *NodeResource) getKey() string {
	return n.MachineFamily + "_" + n.Type + "_" + string(n.CapacityClass)
}

// NodeResources groups all resource descriptions for cluster node
type NodeResources []NodeResource

// GetNodeResources gets resources used by cluster node from give NodeInfo object.
func GetNodeResources(processor customresources.CustomResourcesProcessor,
	nodeInfo *framework.NodeInfo, nodeGroup cloudprovider.NodeGroup, location string) (NodeResources, error) {
	if nodeInfo == nil {
		return nil, fmt.Errorf("Nil nodeInfo")
	}
	if location == "" {
		return nil, fmt.Errorf("Location unknown")
	}
	mig, ok := nodeGroup.(*gke.GkeMig)
	if !ok {
		return nil, fmt.Errorf("cannot get resources for non-MIG group: %s", getNodeGroupId(nodeGroup))
	}
	capacityClass := StandardCapacityClass
	if mig.Spec() != nil && mig.Spec().FlexStart {
		capacityClass = FlexStartCapacityClass
	}

	var resources NodeResources

	machineFamily, err := gke.GetMachineFamilyFromNodeGroup(nodeGroup)
	if err != nil {
		machineFamily = unknownMachineFamily
		klog.Warningf("Could not get machine family from node group %s", getNodeGroupId(nodeGroup))
	}

	cpu := getNodeResource(nodeInfo.Node(), v1.ResourceCPU)
	if cpu > 0 {
		resources = append(resources, NodeResource{
			Type:          "cpu",
			MachineFamily: machineFamily,
			Value:         cpu,
			Location:      location,
			CostClass:     StandardCostClass,
			CapacityClass: capacityClass,
		})
	}

	// TODO: check what unit is used for memory and flatten it appropriatelly to end up with GBs
	memory := getNodeResource(nodeInfo.Node(), v1.ResourceMemory)
	if memory > 0 {
		resources = append(resources, NodeResource{
			Type:          "memory",
			MachineFamily: machineFamily,
			Value:         int64(math.RoundToEven(float64(memory) / units.GiB)),
			Location:      location,
			CostClass:     StandardCostClass,
			CapacityClass: capacityClass,
		})
	}

	resourceTargets, err := processor.GetNodeResourceTargets(nil, nodeInfo.Node(), nodeGroup)
	for _, resourceTarget := range resourceTargets {
		if err == nil && resourceTarget.ResourceCount > 0 {
			resources = append(resources, NodeResource{
				Type:          resourceTarget.ResourceType,
				MachineFamily: machineFamily,
				Value:         resourceTarget.ResourceCount,
				Location:      location,
				CostClass:     ExpensiveCostClass,
				CapacityClass: capacityClass,
			})
		}
	}

	return resources, nil
}

func getNodeResource(node *v1.Node, resource v1.ResourceName) int64 {
	nodeCapacity, found := node.Status.Capacity[resource]
	if !found {
		return 0
	}

	nodeCapacityValue := nodeCapacity.Value()
	if nodeCapacityValue < 0 {
		nodeCapacityValue = 0
	}

	return nodeCapacityValue
}
