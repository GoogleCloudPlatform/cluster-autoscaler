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

package autoprovisioning

import (
	"strconv"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"

	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/napcloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

// PodCapacityLabelGenerator uses pod-capacity resources to enforce pod-per-vm.
// Pods requesting pod-capacity require a node providing that pod-capacity.
// Details in go/gke-ap-sohw
type PodCapacityLabelGenerator struct {
	cloudProvider napcloudprovider.AutoprovisioningCloudProvider
}

func NewPodCapacityLabelGenerator(cloudProvider napcloudprovider.AutoprovisioningCloudProvider) *PodCapacityLabelGenerator {
	return &PodCapacityLabelGenerator{
		cloudProvider: cloudProvider,
	}
}

func (g PodCapacityLabelGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, pReq *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) errors.AutoscalerError {
	if pReq.PodCapacity == "" {
		return nil
	}
	// If pod requires PPVM get the capacity it requires.
	// PodCapacity is set in GetRequirements(pod).
	v := pReq.PodCapacity
	if !g.cloudProvider.IsAutopilotEnabled() {
		return NewIsolatedPodNonAutopilotError()
	}
	if i, err := strconv.Atoi(v); err != nil {
		return NewIsolatedPodCapacityError(v)
	} else {
		ngReq.podCapacity = i
	}
	return nil
}

func (g PodCapacityLabelGenerator) GenerateNodeGroupOptionsForRequirements(options []NodeGroupOptions, requirements nodeGroupRequirements) []NodeGroupOptions {
	if requirements.podCapacity <= 0 {
		return options
	}
	for i := range options {
		options[i].PodCapacity = requirements.podCapacity
	}
	return options
}

func (g PodCapacityLabelGenerator) UpdateParameters(params *nodeGroupParameters, _ nodeGroupRequirements, opt NodeGroupOptions) error {
	if opt.PodCapacity <= 0 {
		return nil
	}
	params.systemLabels[gkelabels.PodCapacityLabel] = strconv.Itoa(opt.PodCapacity)
	if params.extraResources == nil {
		params.extraResources = map[string]resource.Quantity{}
	}
	params.extraResources[gkelabels.PodCapacityLabel] = *resource.NewQuantity(int64(opt.PodCapacity), resource.DecimalSI)
	return nil
}

func (g PodCapacityLabelGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, extraResources map[string]resource.Quantity) error {
	if val, ok := systemLabels[gkelabels.PodCapacityLabel]; ok {
		spec.Labels[gkelabels.PodCapacityLabel] = val
	}
	return nil
}

func (g PodCapacityLabelGenerator) ValidateRequirements(_ *nodeGroupRequirements) errors.AutoscalerError {
	return nil
}

// PodIsolationLabelGenerator uses a label to isolate pods based on CPU requests.
// Details in go/gke-ap-sohw.
type PodIsolationLabelGenerator struct {
	cloudProvider napcloudprovider.AutoprovisioningCloudProvider
}

func NewPodIsolationLabelGenerator(cloudProvider napcloudprovider.AutoprovisioningCloudProvider) *PodIsolationLabelGenerator {
	return &PodIsolationLabelGenerator{
		cloudProvider: cloudProvider,
	}
}

func (g PodIsolationLabelGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, pReq *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) errors.AutoscalerError {
	if v, ok := pReq.LabelReq.GetSingleValue(gkelabels.PodPerVMSizeLabel); ok {
		if !g.cloudProvider.IsAutopilotEnabled() {
			return NewIsolatedPodNonAutopilotError()
		}
		if _, err := resource.ParseQuantity(v); err != nil {
			return NewInvalidIsolatedPodCPUReq(v)
		}
		ngReq.podIsolationCPUReq = v
	}
	return nil
}

func (g PodIsolationLabelGenerator) GenerateNodeGroupOptionsForRequirements(options []NodeGroupOptions, requirements nodeGroupRequirements) []NodeGroupOptions {
	if requirements.podIsolationCPUReq == "" {
		return options
	}
	for i := range options {
		options[i].PodIsolationCPUReq = requirements.podIsolationCPUReq
	}
	return options
}

func (g PodIsolationLabelGenerator) UpdateParameters(params *nodeGroupParameters, _ nodeGroupRequirements, opt NodeGroupOptions) error {
	if opt.PodIsolationCPUReq != "" {
		params.systemLabels[gkelabels.PodPerVMSizeLabel] = opt.PodIsolationCPUReq
	}
	return nil
}

func (g PodIsolationLabelGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, _ map[string]resource.Quantity) error {
	if val, ok := systemLabels[gkelabels.PodPerVMSizeLabel]; ok {
		spec.Labels[gkelabels.PodPerVMSizeLabel] = val
	}
	return nil
}

func (g PodIsolationLabelGenerator) ValidateRequirements(_ *nodeGroupRequirements) errors.AutoscalerError {
	return nil
}
