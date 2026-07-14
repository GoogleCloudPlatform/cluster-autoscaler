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

package gkeprice

import (
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/provider"
	"k8s.io/klog/v2"
)

// MachineTypeBalancer shows node preference based on machine family balancing strategy.
type MachineTypeBalancer interface {
	// MachineTypeBalancingFactor returns the balancing factor for the given machine family
	MachineTypeBalancingFactor(machineType string, nodeInfos map[string]*framework.NodeInfo) float64
}

type computeClassBalancer struct {
	provider              provider.GkeExpanderCloudProvider
	computeClassesEnabled []machinetypes.PredefinedComputeClass
}

// NewComputeClassBalancer returns MachineTypeBalancer that tries to balance
// configured compute classes node counts.
func NewComputeClassBalancer(provider provider.GkeExpanderCloudProvider) MachineTypeBalancer {
	var computeClasses []machinetypes.PredefinedComputeClass
	for _, c := range machinetypes.AllComputeClasses() {
		if c.IsFamilyBalancingEnabled() {
			computeClasses = append(computeClasses, c)
		}
	}
	return &computeClassBalancer{
		provider:              provider,
		computeClassesEnabled: computeClasses,
	}
}

// MachineTypeBalancingFactor returns the balancing factor based on the balanced compute classes
func (b *computeClassBalancer) MachineTypeBalancingFactor(machineType string, nodeInfos map[string]*framework.NodeInfo) float64 {
	machineFamily, err := b.provider.MachineConfigProvider().GetMachineFamilyFromMachineName(machineType)
	if err != nil {
		klog.Errorf("Failed to get %s machine family: %v", machineType, err)
		return 1
	}

	// this code assumes that a machine family will only be part of a single compute class
	for _, computeClass := range b.computeClassesEnabled {
		if machineFamily.In(computeClass.MachineFamilies()...) {
			return b.computeFactor(machineFamily, computeClass, nodeInfos)
		}
	}
	return 1
}

func (b *computeClassBalancer) computeFactor(machineFamily machinetypes.MachineFamily, computeClass machinetypes.PredefinedComputeClass, nodeInfos map[string]*framework.NodeInfo) float64 {
	cpuCount := make(map[string]int64)
	for _, nodeGroup := range b.provider.NodeGroups() {
		machineFamilyName, err := gke.GetMachineFamilyFromNodeGroup(nodeGroup)
		if err != nil {
			klog.Errorf("Failed to get machine family name for %v", nodeGroup.Id())
			return 1
		}
		targetSize, err := nodeGroup.TargetSize()
		if err != nil {
			klog.Errorf("Failed to get target size for %v", nodeGroup.Id())
			return 1
		}
		nodeInfo, found := nodeInfos[nodeGroup.Id()]
		if !found {
			klog.Errorf("Failed to get template node info for %v", nodeGroup.Id())
			return 1
		}
		cpuCount[machineFamilyName] += int64(targetSize) * nodeInfo.Node().Status.Capacity.Cpu().Value()
	}
	maxCpuCount := int64(0)
	for _, otherMachineFamily := range computeClass.MachineFamilies() {
		maxCpuCount = max(maxCpuCount, cpuCount[otherMachineFamily.Name()])
	}
	return float64(cpuCount[machineFamily.Name()]+1) / float64(maxCpuCount+1)
}
