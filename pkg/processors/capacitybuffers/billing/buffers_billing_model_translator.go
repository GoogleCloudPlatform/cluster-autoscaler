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
	"fmt"

	"k8s.io/klog/v2"

	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	cbclient "k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/client"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/common"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	pod "k8s.io/autoscaler/cluster-autoscaler/utils/pod"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

const (

	// Predefined compute classes use SoHW on autopilot clusters.
	performanceComputeClass = "Performance"
	acceleratorComputeClass = "Accelerator"
)

// BillingModelTranslator excludes buffers with pod templates that use pod based billing
// for both standard and autopilot clusters
type BillingModelTranslator struct {
	client      *cbclient.CapacityBufferClient
	crdLister   lister.Lister
	isAutopilot bool
}

// NewBillingModelTranslator creates an instance of BillingModelTranslator.
func NewBillingModelTranslator(client *cbclient.CapacityBufferClient, crdLister lister.Lister, isAutopilot bool) *BillingModelTranslator {
	if crdLister == nil {
		klog.Errorf("Capacity buffer billing model translator has no crd lister set.")
	}

	return &BillingModelTranslator{
		client:      client,
		crdLister:   crdLister,
		isAutopilot: isAutopilot,
	}
}

// Translate sets buffers with pod based billing as not ready for provisioning for autopilot and standard clusters.
func (t *BillingModelTranslator) Translate(buffers []*v1beta1.CapacityBuffer) []error {
	errors := []error{}

	for _, buffer := range buffers {
		if buffer.Status.PodTemplateRef == nil {
			continue
		}

		podTemplate, err := t.client.GetPodTemplate(buffer.Namespace, buffer.Status.PodTemplateRef.Name)
		if err != nil {
			errMessage := fmt.Sprintf("couldn't get pod template %s/%s: %v", buffer.Namespace, buffer.Status.PodTemplateRef.Name, err.Error())
			err := setBufferAsNotReadyForProvisioningWithError(buffer, errMessage)
			errors = append(errors, err)
			continue
		}

		pod := pod.GetPodFromTemplate(&podTemplate.Template)
		pod.Namespace = buffer.Namespace
		crd, computeClassName, err := t.crdLister.PodCrd(pod)
		if err != nil {
			errMessage := fmt.Sprintf("couldn't get crd for pod template %s/%s with error: %v", buffer.Namespace, buffer.Status.PodTemplateRef.Name, err.Error())
			err := setBufferAsNotReadyForProvisioningWithError(buffer, errMessage)
			errors = append(errors, err)
			continue
		}

		bufferUsesNodeBasedBilling := true
		if t.isAutopilot {
			// For autopilot if the workload requests hardware or uses crd for SoHW then the buffer results in node based billing
			bufferUsesNodeBasedBilling = podRequestsSliceOfHardware(pod) || t.isCrdNodeBasedBillingOnAutopilot(crd, computeClassName)
		} else {
			// For standard if the workload has CCC CRD with rules define pod family then the buffer results in pod based billing
			bufferUsesNodeBasedBilling = t.isCrdNodeBasedBillingOnStandard(crd)
		}

		if !bufferUsesNodeBasedBilling {
			err := setBufferAsNotReadyForProvisioningWithError(buffer, "can't create a buffer with pod based billing")
			errors = append(errors, err)
			continue
		}

	}
	return errors
}

// CleanUp cleans up the translator's internal structures.
func (t *BillingModelTranslator) CleanUp() {
}

// isCrdNodeBasedBillingOnStandard returns true if the passed Crd results in node-based billing model.
func (t *BillingModelTranslator) isCrdNodeBasedBillingOnStandard(crd crd.CRD) bool {
	if crd == nil || crd.CrdType() != ccc.CrdType {
		return true
	}
	return !crdDefinesPodFamily(crd)
}

// isCrdNodeBasedBillingOnAutopilot returns true if the passed Crd results in node-based billing model on autopilot.
func (t *BillingModelTranslator) isCrdNodeBasedBillingOnAutopilot(crd crd.CRD, computeClassName string) bool {
	isPredefinedCC := machinetypes.IsPredefinedComputeClass(computeClassName)
	if isPredefinedCC {
		// only Performance and Accelerator predefined compute classes use node-based billing model
		return computeClassName == performanceComputeClass || computeClassName == acceleratorComputeClass
	}
	// custom compute classes with no pod family defined use node-based billing model
	return crd != nil && crd.CrdType() == ccc.CrdType && !crdDefinesPodFamily(crd)
}

// crdDefinesPodFamily returns true if any rule in the passed crd defines pod family.
func crdDefinesPodFamily(crd crd.CRD) bool {
	for _, rule := range crd.Rules() {
		if _, err := rule.PodFamilyMachineFamilies(); err == nil {
			return true
		}
	}
	return false
}

// podRequestsSliceOfHardware returns true if the pod requests specific machine family, GPU, or TPU
// using node selectors or requests GPU or TPU using resource limits
func podRequestsSliceOfHardware(pod *apiv1.Pod) bool {
	// Check for direct hardware node selectors
	selectors := pod.Spec.NodeSelector
	if _, exists := selectors[gkelabels.MachineFamilyLabel]; exists {
		return true
	}
	if _, exists := selectors[gkelabels.GPULabel]; exists {
		return true
	}
	if _, exists := selectors[gkelabels.TPULabel]; exists {
		return true
	}

	// Check for GPU/TPU requests in container resources
	for _, container := range pod.Spec.Containers {
		if _, exists := container.Resources.Limits[gpu.ResourceNvidiaGPU]; exists {
			return true
		}
		if _, exists := container.Resources.Limits[tpu.ResourceGoogleTPU]; exists {
			return true
		}
	}
	return false
}

// setBufferAsNotReadyForProvisioningWithError sets the buffer status as not ready for provisioning with the passed error message
func setBufferAsNotReadyForProvisioningWithError(buffer *v1beta1.CapacityBuffer, errMessage string) error {
	err := fmt.Errorf("Failed to translate capacity buffer %s/%s: %s", buffer.Namespace, buffer.Name, errMessage)
	common.SetBufferAsNotReadyForProvisioning(buffer, nil, nil, nil, nil, err)
	return err
}
