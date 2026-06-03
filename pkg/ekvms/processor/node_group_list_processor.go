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

package processor

import (
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/operationtracker"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator"
	ekvms_utils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/utils"
	"k8s.io/klog/v2"
)

type resizingEnabledProvider interface {
	ResizingEnabled(machineFamily string) bool
}

type nodeGroupListProcessor struct {
	nodeGroupListProcessor        nodegroups.NodeGroupListProcessor
	resizeCalculator              calculator.Calculator
	resizableMachineTypesProvider config.Provider[sets.Set[string]]
	resizingEnabledProvider       resizingEnabledProvider
	machineConfigProvider         *machinetypes.MachineConfigProvider
}

// NewNodeGroupListProcessor create a processor for injecting balloon pods into resizable node infos for binpacking simulation.
func NewNodeGroupListProcessor(NodeGroupListProcessor nodegroups.NodeGroupListProcessor, resizeCalculator calculator.Calculator, resizableMachineTypesProvider config.Provider[sets.Set[string]], resizingEnabledProvider resizingEnabledProvider, machineConfigProvider *machinetypes.MachineConfigProvider) *nodeGroupListProcessor {
	return &nodeGroupListProcessor{
		nodeGroupListProcessor:        NodeGroupListProcessor,
		resizeCalculator:              resizeCalculator,
		resizableMachineTypesProvider: resizableMachineTypesProvider,
		resizingEnabledProvider:       resizingEnabledProvider,
		machineConfigProvider:         machineConfigProvider,
	}
}

// Process updates nodeInfos for resizable nodes to add balloon pods appropriate for scale-up.
func (p *nodeGroupListProcessor) Process(ctx *context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup, nodeInfos map[string]*framework.NodeInfo, unschedulablePods []*apiv1.Pod) ([]cloudprovider.NodeGroup, map[string]*framework.NodeInfo, error) {
	nodeGroups, nodeInfos, err := p.nodeGroupListProcessor.Process(ctx, nodeGroups, nodeInfos, unschedulablePods)
	if err != nil {
		return nodeGroups, nodeInfos, err
	}

	var supportedResizableMachineTypes sets.Set[string]
	if p.resizableMachineTypesProvider != nil {
		supportedResizableMachineTypes = p.resizableMachineTypesProvider.Provide()
	}

	processedNodeGroups := make([]cloudprovider.NodeGroup, 0, len(nodeGroups))
	for _, ng := range nodeGroups {
		nodeInfo, ok := nodeInfos[ng.Id()]
		if !ok {
			klog.Warningf("Failed to get node info for a node group: %q", ng.Id())
			continue
		}
		// We still add the node if it doesn't have machine family label or if it is not resizable, since this processor only handles resizable nodes.
		machineType, _ := ekvms_utils.GetMachineTypeFromLabels(nodeInfo.Node().Labels)
		if isResizable, err := ekvms_utils.IsResizableMachineType(p.machineConfigProvider, machineType); isResizable && err == nil {
			if !autoprovisioning.IsResizableMachineTypeSupported(machineType, supportedResizableMachineTypes) {
				klog.V(4).Infof("Skipping nodegroup %q since it has an unsupported machine type %q", ng.Id(), machineType)
				continue
			}

			machineFamily, err := ekvms_utils.GetMachineFamilyName(nodeInfo.Node())
			if err != nil {
				klog.Errorf("Couldn't get machine family name for node %q (err: %v)", nodeInfo.Node().Name, err)
				continue
			}

			if p.resizingEnabledProvider == nil || !p.resizingEnabledProvider.ResizingEnabled(machineFamily) {
				processedNodeGroups = append(processedNodeGroups, ng)
				continue
			}

			if err := operationtracker.InjectDefaultBalloonPod(nodeInfo, p.resizeCalculator); err != nil {
				klog.Errorf("Couldn't inject balloon pod into resizable nodeinfo for nodegroup %q (err: %v)", ng.Id(), err)
				continue
			}
		}
		processedNodeGroups = append(processedNodeGroups, ng)
	}
	return processedNodeGroups, nodeInfos, nil
}

// CleanUp cleans up the processor's internal structures. Just here to satisfy the NodeGroupListProcessor interface.
func (p *nodeGroupListProcessor) CleanUp() {
	p.nodeGroupListProcessor.CleanUp()
}
