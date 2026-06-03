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

package edp

import (
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/extendeddurationpods"
	"k8s.io/klog/v2"
)

type edpCloudProvider interface {
	MachineConfigProvider() *machinetypes.MachineConfigProvider
}

type EdpFilter struct {
	provider edpCloudProvider
}

func NewEdpFilter(provider edpCloudProvider) *EdpFilter {
	return &EdpFilter{
		provider: provider,
	}
}

func (edp *EdpFilter) BestOptions(options []expander.Option, nodeInfos map[string]*framework.NodeInfo) []expander.Option {
	edpOptions, regularOptions := splitEdpOptions(options, nodeInfos)
	edpOptionsWithEdps := filterOutOptsWithoutEdps(edpOptions)
	edpOptionsByEdpNodeSelector := groupOptsByLabelValue(edpOptionsWithEdps, nodeInfos, gkelabels.ExtendedDurationPodsLabel)
	smallestEdpOptions := []expander.Option{}
	for nodeSelector, opts := range edpOptionsByEdpNodeSelector {
		if nodeSelector == gkelabels.ExtendedDurationPackedPodsValue {
			// If packed, don't limit to the smallest machine type.
			smallestEdpOptions = append(smallestEdpOptions, opts...)
		} else {
			smallestEdpOptions = append(smallestEdpOptions, edp.smallestMachineTypeOptionsOnly(opts)...)
		}
	}
	return append(regularOptions, smallestEdpOptions...)
}

func splitEdpOptions(options []expander.Option, nodeInfos map[string]*framework.NodeInfo) (edpOptions []expander.Option, regularOptions []expander.Option) {
	for _, option := range options {
		nodeInfo, found := nodeInfos[option.NodeGroup.Id()]
		if !found {
			klog.Errorf("No node info for %s", option.NodeGroup.Id())
			continue
		}

		if _, isLabeled := nodeInfo.Node().Labels[gkelabels.ExtendedDurationPodsLabel]; isLabeled {
			edpOptions = append(edpOptions, option)
		} else {
			regularOptions = append(regularOptions, option)
		}
	}
	return edpOptions, regularOptions
}

func filterOutOptsWithoutEdps(options []expander.Option) []expander.Option {
	optionsWithEdps := []expander.Option{}
	for _, option := range options {
		for _, pod := range option.Pods {
			if edpSelector := extendeddurationpods.EdpSelector(pod); edpSelector != "" {
				optionsWithEdps = append(optionsWithEdps, option)
				break
			}
		}
	}
	return optionsWithEdps
}

func groupOptsByLabelValue(options []expander.Option, nodeInfos map[string]*framework.NodeInfo, labelKey string) map[string][]expander.Option {
	groupedOpts := map[string][]expander.Option{}
	for _, option := range options {
		nodeInfo, found := nodeInfos[option.NodeGroup.Id()]
		if !found {
			klog.Errorf("No node info for %s", option.NodeGroup.Id())
			continue
		}

		label, isLabeled := nodeInfo.Node().Labels[labelKey]
		if !isLabeled {
			continue
		}

		if _, found := groupedOpts[label]; !found {
			groupedOpts[label] = []expander.Option{}
		}

		groupedOpts[label] = append(groupedOpts[label], option)
	}
	return groupedOpts
}

func (edp *EdpFilter) smallestMachineTypeOptionsOnly(options []expander.Option) []expander.Option {
	smallestOptions := []expander.Option{}
	smallestMachineTypeInfo := machinetypes.MachineType{}

	for _, option := range options {
		mig, ok := option.NodeGroup.(*gke.GkeMig)
		if !ok {
			klog.Errorf("Couldn't cast to GkeMig: %v", option.NodeGroup)
			continue
		}
		machineTypeName := mig.Spec().MachineType
		currentMachineTypeInfo, err := edp.provider.MachineConfigProvider().ToMachineType(machineTypeName)
		if err != nil {
			klog.Errorf("Error getting machine type: %s", err)
			continue
		}

		if (machinetypes.MachineType{}) == smallestMachineTypeInfo || smallestMachineTypeInfo.Name == currentMachineTypeInfo.Name {
			smallestMachineTypeInfo = currentMachineTypeInfo
			smallestOptions = append(smallestOptions, option)
		} else if machinetypes.IsLargerThan(smallestMachineTypeInfo, currentMachineTypeInfo) {
			smallestMachineTypeInfo = currentMachineTypeInfo
			smallestOptions = []expander.Option{option}
		}
	}
	return smallestOptions
}
