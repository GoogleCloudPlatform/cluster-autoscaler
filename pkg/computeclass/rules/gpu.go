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

package rules

import (
	"strconv"
	"strings"

	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/klog/v2"
)

// GpuRule is an interface for rules with GPU.
type GpuRule interface {
	BaseRule
	GpuRequest() machinetypes.GpuRequest
}

type gpuRule struct {
	gpuRequest *machinetypes.GpuRequest
}

// Matches returns true if the nodegroup is within one of the nodepools.
func (r *gpuRule) Matches(nodeGroup cloudprovider.NodeGroup) bool {
	mig, ok := nodeGroup.(gkeNodeGroup)
	if !ok {
		klog.Errorf("expected GkeMig; got %v", nodeGroup)
		return false
	}

	// If gpu is specified we want to validate that the config of nodeGroup is the same.
	// If it's not specified, we skip the check, as for example users may provide
	// config in which machine family implied GPU, but it's not explicitly set in
	// CCC, and we still want to honor that.
	if r.gpuRequest != nil {
		if len(mig.Spec().Accelerators) != 1 {
			return false
		}

		acceleratorConfig := mig.Spec().Accelerators[0]
		if machinetypes.PhysicalGpuCount(acceleratorConfig.AcceleratorCount) != r.gpuRequest.PhysicalGPUCount {
			return false
		}

		if acceleratorConfig.AcceleratorType != r.gpuRequest.Config.GpuType {
			return false
		}

		if !matchesGpuDriverVersion(mig, r, acceleratorConfig) {
			return false
		}

		if acceleratorConfig.GpuPartitionSize != r.gpuRequest.Config.PartitionSize {
			return false
		}

		maxClients, err := strconv.Atoi(r.gpuRequest.Config.MaxSharedClients)
		if r.gpuRequest.Config.MaxSharedClients != "" && err != nil {
			klog.Errorf("Received an error, when parsing maxSharedClients(%v): %v", r.gpuRequest.Config.MaxSharedClients, err)
			return false
		}
		if gsc := acceleratorConfig.GpuSharingConfig; gsc != nil {
			if labels.ConvertGpuSharingStrategyToLabelEnum(gsc.GpuSharingStrategy) != r.gpuRequest.Config.SharingStrategy {
				return false
			}
			if gsc.MaxSharedClientsPerGpu != int64(maxClients) {
				return false
			}
		} else {
			if r.gpuRequest.Config.SharingStrategy != "" || maxClients != 0 {
				return false
			}
		}
	}
	return true
}

// GpuRequest returns the GPURequest of rule.
func (r *gpuRule) GpuRequest() machinetypes.GpuRequest {
	if r.gpuRequest == nil {
		return machinetypes.GpuRequest{}
	}
	return *r.gpuRequest
}

// WithGpuRule returns RuleOption adding GpuRule.
func WithGpuRule(gpuRequest *machinetypes.GpuRequest) RuleOption {
	return func(r *rule) {
		r.gpuRule.gpuRequest = gpuRequest
	}
}

// matchesGpuDriverVersion checks if the GPU driver version matches. It uses the GPUDriverVersionLabel and then
// falling back to GpuDriverInstallationConfig if the label is missing.
// This handles cases where manually created node pool doesn't have the correct "GPUDriverVersionLabel" label.
// b/396625695
func matchesGpuDriverVersion(mig gkeNodeGroup, r *gpuRule, acceleratorConfig *gke_api_beta.AcceleratorConfig) bool {
	driverVersionLabel := strings.ToLower(mig.Spec().Labels[labels.GPUDriverVersionLabel])
	requestedDriverVersion := strings.ToLower(r.gpuRequest.Config.DriverVersion)
	if driverVersionLabel == requestedDriverVersion {
		return true
	}
	if requestedDriverVersion == labels.DisabledGPUDriverVersionValue && driverVersionLabel == "disabled" {
		return true
	}
	if acceleratorConfig.GpuDriverInstallationConfig != nil {
		nodePoolDriverVersion := strings.ToLower(acceleratorConfig.GpuDriverInstallationConfig.GpuDriverVersion)
		if nodePoolDriverVersion == requestedDriverVersion {
			return true
		}
		if requestedDriverVersion == labels.DisabledGPUDriverVersionValue &&
			(nodePoolDriverVersion == "installation_disabled" || nodePoolDriverVersion == "gpu_driver_version_unspecified") {
			return true
		}
	}
	return false
}
