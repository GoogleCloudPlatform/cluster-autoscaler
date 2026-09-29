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

package billing

import (
	apiv1 "k8s.io/api/core/v1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	kubeletapis "k8s.io/kubelet/pkg/apis"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/gpu"
)

// HasVmBasedBillingExclusions returns true if the workload specifies any VM-based billing labels,
// node selectors, affinities, or dedicated hardware requests that disqualify it from
// pay-per-pod classification.
func HasVmBasedBillingExclusions(pod *apiv1.Pod) bool {
	if pod == nil {
		return false
	}
	return HasVmBasedBillingExclusionsWithRequirements(pod, podrequirements.GetRequirements(pod))
}

// HasVmBasedBillingExclusionsWithRequirements returns true if the workload specifies any VM-based
// billing labels or hardware requests, using the pre-extracted pod requirements.
func HasVmBasedBillingExclusionsWithRequirements(pod *apiv1.Pod, req *podrequirements.Requirements) bool {
	if pod == nil {
		return false
	}
	if req == nil {
		req = podrequirements.GetRequirements(pod)
	}

	// 1. Check scheduling label requirements (node selectors and node affinity).
	if _, exists := req.LabelReq.GetValues(gkelabels.MachineFamilyLabel); exists {
		return true
	}
	if _, exists := req.LabelReq.GetValues(gkelabels.GPULabel); exists {
		return true
	}
	if _, exists := req.LabelReq.GetValues(gkelabels.TPULabel); exists {
		return true
	}
	if _, exists := req.LabelReq.GetValues(gkelabels.TPUTopologyLabel); exists {
		return true
	}
	if _, exists := req.LabelReq.GetValues(gkelabels.EphemeralLocalSsdLabel); exists {
		return true
	}
	if _, exists := req.LabelReq.GetValues(gkelabels.PodCapacityLabel); exists {
		return true
	}
	if _, exists := req.LabelReq.GetValues(gkelabels.PodPerVMSizeLabel); exists {
		return true
	}
	if req.PodCapacity != "" {
		return true
	}

	// 2. Check container resource requests and limits for accelerator hardware (GPU / TPU).
	if hasAcceleratorResource(pod.Spec.Containers) || hasAcceleratorResource(pod.Spec.InitContainers) {
		return true
	}

	return false
}

func hasNonZeroResource(resources apiv1.ResourceList, name apiv1.ResourceName) bool {
	qty, exists := resources[name]
	return exists && !qty.IsZero()
}

func hasAcceleratorResource(containers []apiv1.Container) bool {
	for i := range containers {
		c := &containers[i]
		if hasNonZeroResource(c.Resources.Requests, gpu.ResourceNvidiaGPU) ||
			hasNonZeroResource(c.Resources.Requests, tpu.ResourceGoogleTPU) ||
			hasNonZeroResource(c.Resources.Limits, gpu.ResourceNvidiaGPU) ||
			hasNonZeroResource(c.Resources.Limits, tpu.ResourceGoogleTPU) {
			return true
		}
	}
	return false
}

// GetPodFamilyForPayPerPodAutopilotWorkload returns the podFamily (general-purpose or general-purpose-arm)
// for standard pay-per-pod Autopilot workloads (unlabelled, non-SoHW, non-accelerator, non-custom CCC).
// If the pod does not qualify as a default pay-per-pod Autopilot workload, it returns ("", false).
func GetPodFamilyForPayPerPodAutopilotWorkload(pod *apiv1.Pod) (string, bool) {
	if pod == nil {
		return "", false
	}
	return GetPodFamilyForPayPerPodAutopilotWorkloadWithRequirements(pod, podrequirements.GetRequirements(pod))
}

// GetPodFamilyForPayPerPodAutopilotWorkloadWithRequirements returns the podFamily for standard
// pay-per-pod Autopilot workloads using the pre-extracted pod requirements.
func GetPodFamilyForPayPerPodAutopilotWorkloadWithRequirements(pod *apiv1.Pod, req *podrequirements.Requirements) (string, bool) {
	if pod == nil {
		return "", false
	}
	if req == nil {
		req = podrequirements.GetRequirements(pod)
	}

	// Must not specify a compute class (default Autopilot workload).
	if _, exists := req.LabelReq.GetValues(gkelabels.ComputeClassLabel); exists {
		return "", false
	}

	// Must not have VM-based billing exclusions.
	if HasVmBasedBillingExclusionsWithRequirements(pod, req) {
		return "", false
	}

	// Determine architecture (defaults to x86/general-purpose unless ARM requested).
	if IsArmAutopilotWorkloadWithRequirements(pod, req) {
		return rules.GeneralPurposeArmPodFamily, true
	}
	return rules.GeneralPurposePodFamily, true
}

// IsArmAutopilotWorkload returns true if the workload requests ARM architecture.
func IsArmAutopilotWorkload(pod *apiv1.Pod) bool {
	if pod == nil {
		return false
	}
	return IsArmAutopilotWorkloadWithRequirements(pod, podrequirements.GetRequirements(pod))
}

// IsArmAutopilotWorkloadWithRequirements returns true if the workload requests ARM architecture,
// using the pre-extracted pod requirements.
func IsArmAutopilotWorkloadWithRequirements(pod *apiv1.Pod, req *podrequirements.Requirements) bool {
	if pod == nil {
		return false
	}
	if req == nil {
		req = podrequirements.GetRequirements(pod)
	}
	if archVals, exists := req.LabelReq.GetValues(apiv1.LabelArchStable); exists {
		if archVals.Get()["arm64"] {
			return true
		}
	}
	if archVals, exists := req.LabelReq.GetValues(kubeletapis.LabelArch); exists {
		if archVals.Get()["arm64"] {
			return true
		}
	}
	return false
}

// BillingModel represents the billing model classification for a workload or compute class.
type BillingModel string

const (
	// PodBasedBilling indicates the workload exclusively uses pod-based (pay-per-pod) billing.
	PodBasedBilling BillingModel = "PodBased"
	// NodeBasedBilling indicates the workload exclusively uses node-based (VM / Slice of Hardware) billing.
	NodeBasedBilling BillingModel = "NodeBased"
	// MixedBilling indicates the workload uses a Custom Compute Class containing both
	// pod-based (podFamily) and node-based (e.g., machineFamily/machineType or ScaleUpAnyway) priority rules.
	MixedBilling BillingModel = "Mixed"
)

// GetBillingModel returns the BillingModel (PodBasedBilling, NodeBasedBilling, or MixedBilling)
// for a workload given its pod spec, resolved ComputeClass CRD, compute class name, and cluster mode.
//
// Assumptions:
//   - pod is required (must be non-nil).
//   - If ccCrd is provided, computeClassName is ignored and the billing model is determined from ccCrd.
//   - If ccCrd is not provided, computeClassName (if set) is treated as a predefined compute class.
func GetBillingModel(pod *apiv1.Pod, ccCrd crd.CRD, computeClassName string, isAutopilot bool) BillingModel {
	req := podrequirements.GetRequirements(pod)
	// Common exclusions: Any dedicated hardware or VM-based billing selectors disqualify the workload from pod-based billing.
	if HasVmBasedBillingExclusionsWithRequirements(pod, req) {
		return NodeBasedBilling
	}

	if ccCrd != nil {
		return GetBillingModelForCCC(ccCrd)
	}

	if !isAutopilot {
		return NodeBasedBilling
	}

	if computeClassName == "" {
		// Default Autopilot workload (no compute class) uses pod-based billing.
		return PodBasedBilling
	}

	if pcc, err := machinetypes.ToPredefinedComputeClass(computeClassName); err == nil {
		// Balanced and Scale-Out are pod-based; Performance and Accelerator are node-based.
		if !pcc.IsSliceOfHardware() && !pcc.IsAcceleratorClass() {
			return PodBasedBilling
		}
	}
	return NodeBasedBilling
}

// GetBillingModelForCCC classifies the billing model of a Custom Compute Class CRD across its priority rules
// and its whenUnsatisfiable policy. When ScaleUpAnyway is enabled, fallback scale-up uses node-based billing
// and is treated as an additional priority without pod-based billing.
func GetBillingModelForCCC(ccCrd crd.CRD) BillingModel {
	if ccCrd == nil || ccCrd.CrdType() != ccc.CrdType {
		return NodeBasedBilling
	}
	hasPodBased := false
	hasNodeBased := ccCrd.ScaleUpAnyway()
	for _, rule := range ccCrd.Rules() {
		if _, err := rule.PodFamilyMachineFamilies(); err == nil {
			hasPodBased = true
		} else {
			hasNodeBased = true
		}
	}
	if hasPodBased && hasNodeBased {
		return MixedBilling
	}
	if hasPodBased {
		return PodBasedBilling
	}
	return NodeBasedBilling
}
