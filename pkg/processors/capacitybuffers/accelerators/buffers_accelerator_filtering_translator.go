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

package accelerators

import (
	"fmt"
	"slices"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	cbclient "k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/client"
	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/common"
	gpuutils "k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	podutils "k8s.io/autoscaler/cluster-autoscaler/utils/pod"
	tpuutils "k8s.io/autoscaler/cluster-autoscaler/utils/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
)

type AcceleratorsFilteringTranslator struct {
	client                             *cbclient.CapacityBufferClient
	noAcceleratorsProvisioningStrategy string
}

func NewAcceleratorsFilteringTranslator(client *cbclient.CapacityBufferClient, noAcceleratorsProvisioningStrategy string) *AcceleratorsFilteringTranslator {
	return &AcceleratorsFilteringTranslator{
		client:                             client,
		noAcceleratorsProvisioningStrategy: noAcceleratorsProvisioningStrategy,
	}
}

func (t *AcceleratorsFilteringTranslator) Translate(buffers []*v1beta1.CapacityBuffer) []error {
	errors := []error{}
	for _, buffer := range buffers {
		if buffer.Status.PodTemplateRef == nil {
			continue
		}
		if t.provisioningStrategyAllowsAccelerators(buffer) {
			continue
		}
		if err := t.processBuffer(buffer); err != nil {
			err := fmt.Errorf("failed to translate capacity buffer %s/%s: %w", buffer.Namespace, buffer.Name, err)
			common.SetBufferAsNotReadyForProvisioning(buffer, nil, nil, nil, &t.noAcceleratorsProvisioningStrategy, err)
			errors = append(errors, err)
		}
	}
	return errors
}

func (t *AcceleratorsFilteringTranslator) CleanUp() {
}

func (t *AcceleratorsFilteringTranslator) provisioningStrategyAllowsAccelerators(buffer *v1beta1.CapacityBuffer) bool {
	return buffer == nil ||
		buffer.Spec.ProvisioningStrategy == nil ||
		*buffer.Spec.ProvisioningStrategy != t.noAcceleratorsProvisioningStrategy
}

func (t *AcceleratorsFilteringTranslator) processBuffer(buffer *v1beta1.CapacityBuffer) error {
	requests, err := t.getResourceRequestsFromBuffer(buffer)
	if err != nil {
		return err
	}
	if present, resourceName, resourceType := hasAccelerators(requests); present {
		return fmt.Errorf(
			"accelerators are disabled in the provisioning strategies: [\"%s\"], but requested resource %s is a %s",
			t.noAcceleratorsProvisioningStrategy,
			resourceName,
			resourceType,
		)
	}
	return nil
}

func (t *AcceleratorsFilteringTranslator) getResourceRequestsFromBuffer(buffer *v1beta1.CapacityBuffer) (v1.ResourceList, error) {
	if buffer.Status.PodTemplateRef == nil {
		return nil, fmt.Errorf("there is no pod template reference in buffer status")
	}

	podTemplate, err := t.client.GetPodTemplate(buffer.Namespace, buffer.Status.PodTemplateRef.Name)
	if err != nil {
		return nil, fmt.Errorf("couldn't get pod template %s/%s: %w", buffer.Namespace, buffer.Status.PodTemplateRef.Name, err)
	}
	pod := podutils.GetPodFromTemplate(&podTemplate.Template)
	return podutils.PodRequests(pod), nil
}

func hasAccelerators(resources v1.ResourceList) (bool, string, string) {
	if resources == nil {
		return false, "", ""
	}
	for name := range resources {
		if slices.Contains(gpuutils.GPUVendorResourceNames, name) {
			return true, string(name), "GPU"
		}
		if strings.HasPrefix(string(name), tpuutils.ResourceTPUPrefix) || tpu.ResourceGoogleTPU == name {
			return true, string(name), "TPU"
		}
	}
	return false, "", ""
}
