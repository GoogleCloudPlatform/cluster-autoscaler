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

	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	gkebilling "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/billing"
	lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/klog/v2"
	cbclient "sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/client"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/common"
	pod "sigs.k8s.io/cluster-autoscaler/pkg/utils/pod"
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

		billingModel := gkebilling.GetBillingModel(pod, crd, computeClassName, t.isAutopilot)

		if billingModel != gkebilling.NodeBasedBilling {
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

// setBufferAsNotReadyForProvisioningWithError sets the buffer status as not ready for provisioning with the passed error message
func setBufferAsNotReadyForProvisioningWithError(buffer *v1beta1.CapacityBuffer, errMessage string) error {
	err := fmt.Errorf("Failed to translate capacity buffer %s/%s: %s", buffer.Namespace, buffer.Name, errMessage)
	common.SetBufferAsNotReadyForProvisioning(buffer, nil, nil, nil, nil, err)
	return err
}
