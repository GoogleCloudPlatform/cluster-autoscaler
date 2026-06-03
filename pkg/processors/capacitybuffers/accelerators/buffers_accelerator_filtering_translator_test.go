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
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakeClient "k8s.io/client-go/kubernetes/fake"

	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	cbclient "k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/client"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
)

func TestAcceleratorFilteringTranslator(t *testing.T) {
	csnProvisioningStrategy := "buffer.gke.io/standby-capacity"

	cpuResources := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("10m"),
		corev1.ResourceMemory: resource.MustParse("512Mi"),
	}
	gpuResources := corev1.ResourceList{
		gpu.ResourceNvidiaGPU: *resource.NewQuantity(1, resource.DecimalSI),
	}
	tpuResources := corev1.ResourceList{
		tpu.ResourceGoogleTPU: *resource.NewQuantity(1, resource.DecimalSI),
	}

	podTempCpu := getPodTemplateWithResourceList("podTempCpu", cpuResources)
	podTempGpu := getPodTemplateWithResourceList("podTempGpu", gpuResources)
	podTempTpu := getPodTemplateWithResourceList("podTempTpu", tpuResources)
	deploymentCpu := getPodTemplateWithResourceList("deploymentCpu", cpuResources)
	deploymentGpu := getDeploymentWithResourceRequest("deploymentGpu", gpuResources)
	deploymentTpu := getDeploymentWithResourceRequest("deploymentTpu", tpuResources)

	fakeClient := fakeClient.NewClientset(podTempGpu, podTempTpu, deploymentGpu, deploymentTpu, podTempCpu, deploymentCpu)
	fakeCapacityBuffersClient, _ := cbclient.NewCapacityBufferClient(nil, fakeClient, nil, nil, nil, nil, nil, nil, nil, nil, nil)

	testCases := []struct {
		name               string
		buffersToTranslate []*v1beta1.CapacityBuffer
		expectingError     []bool
	}{
		{
			name:               "standard: no buffers",
			buffersToTranslate: []*v1beta1.CapacityBuffer{},
			expectingError:     []bool{},
		},
		{
			name: "standard: CSN buffer with nil PodTemplateRef",
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				func() *v1beta1.CapacityBuffer {
					b := getCSNBufferReadyForProvisioningFromPodTemplate(podTempCpu.Name, &csnProvisioningStrategy)
					b.Status.PodTemplateRef = nil
					return b
				}(),
			},
			expectingError: []bool{false}, // We shouldn't return error in this case, as otherwise it will override the previous errors.
		},
		{
			name: "standard: CSN buffer with pod template without accelerators",
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getCSNBufferReadyForProvisioningFromPodTemplate(podTempCpu.Name, &csnProvisioningStrategy),
			},
			expectingError: []bool{false},
		},
		{
			name: "CSN buffer with pod template with GPU requested",
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getCSNBufferReadyForProvisioningFromPodTemplate(podTempGpu.Name, &csnProvisioningStrategy),
			},
			expectingError: []bool{true},
		},
		{
			name: "CSN buffer with pod template with TPU requested",
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getCSNBufferReadyForProvisioningFromPodTemplate(podTempTpu.Name, &csnProvisioningStrategy),
			},
			expectingError: []bool{true},
		},
		{
			name: "standard: CSN buffer with deployment without accelerators",
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getCSNBufferReadyForProvisioningFromDeployment(deploymentCpu.Name, podTempCpu.Name, &csnProvisioningStrategy),
			},
			expectingError: []bool{false},
		},
		{
			name: "CSN buffer with deployment with GPU requested",
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getCSNBufferReadyForProvisioningFromDeployment(deploymentGpu.Name, podTempGpu.Name, &csnProvisioningStrategy),
			},
			expectingError: []bool{true},
		},
		{
			name: "CSN buffer with deployment with TPU requested",
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getCSNBufferReadyForProvisioningFromDeployment(deploymentTpu.Name, podTempTpu.Name, &csnProvisioningStrategy),
			},
			expectingError: []bool{true},
		},
		{
			name: "ASN buffer with podTemplate with TPU requested",
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getBufferReadyForProvisioningFromPodTemplate(podTempTpu.Name),
			},
			expectingError: []bool{false},
		},
		{
			name: "ASN buffer with podTemplate with GPU requested",
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getBufferReadyForProvisioningFromPodTemplate(podTempGpu.Name),
			},
			expectingError: []bool{false},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			translator := NewAcceleratorsFilteringTranslator(fakeCapacityBuffersClient, csnProvisioningStrategy)
			translator.Translate(tc.buffersToTranslate)
			for index, buffer := range tc.buffersToTranslate {
				assert.Equal(t, tc.expectingError[index], isBufferStatusNotReadyForProvisioning(buffer))
			}
		})
	}
}

func isBufferStatusNotReadyForProvisioning(buffer *v1beta1.CapacityBuffer) bool {
	return len(buffer.Status.Conditions) > 0 && buffer.Status.Conditions[0].Type == "ReadyForProvisioning" && buffer.Status.Conditions[0].Status == "False"
}

func getPodTemplateWithResourceList(name string, resources corev1.ResourceList) *corev1.PodTemplate {
	return &corev1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  "default",
			Generation: 1,
		},
		Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Resources: corev1.ResourceRequirements{
							Requests: resources,
						},
					},
				},
			},
		},
	}
}

func getDeploymentWithResourceRequest(name string, resources corev1.ResourceList) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Resources: corev1.ResourceRequirements{
								Requests: resources,
							},
						},
					},
				},
			},
		},
	}
}

func getCSNBufferReadyForProvisioningFromDeployment(deploymentName string, podTemplateName string, provisioningStrategy *string) *v1beta1.CapacityBuffer {
	buffer := getBufferReadyForProvisioningFromDeployment(deploymentName, podTemplateName)
	buffer.Spec.ProvisioningStrategy = provisioningStrategy
	return buffer
}

func getCSNBufferReadyForProvisioningFromPodTemplate(podTemplateName string, provisioningStrategy *string) *v1beta1.CapacityBuffer {
	buffer := getBufferReadyForProvisioningFromPodTemplate(podTemplateName)
	buffer.Spec.ProvisioningStrategy = provisioningStrategy
	return buffer
}

func getBufferReadyForProvisioningFromPodTemplate(podTemplateName string) *v1beta1.CapacityBuffer {
	meta := getObjectMeta(podTemplateName)
	status := getBufferStatusFromPodTemplate(podTemplateName)
	spec := v1beta1.CapacityBufferSpec{
		PodTemplateRef: &v1beta1.LocalObjectRef{Name: podTemplateName},
	}

	return getBuffer(meta, spec, status)
}

func getBufferReadyForProvisioningFromDeployment(deploymentName string, podTemplateName string) *v1beta1.CapacityBuffer {
	meta := getObjectMeta(deploymentName)
	status := getBufferStatusFromPodTemplate(podTemplateName)
	spec := v1beta1.CapacityBufferSpec{
		ScalableRef: &v1beta1.ScalableRef{Name: deploymentName},
	}

	return getBuffer(meta, spec, status)
}

func getBuffer(meta metav1.ObjectMeta, spec v1beta1.CapacityBufferSpec, status v1beta1.CapacityBufferStatus) *v1beta1.CapacityBuffer {
	return &v1beta1.CapacityBuffer{
		ObjectMeta: meta,
		Spec:       spec,
		Status:     status,
	}
}

func getObjectMeta(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:       "CB_" + name,
		Namespace:  "default",
		Generation: 1,
	}
}

func getBufferStatusFromPodTemplate(podTemplateName string) v1beta1.CapacityBufferStatus {
	return v1beta1.CapacityBufferStatus{
		PodTemplateRef: &v1beta1.LocalObjectRef{Name: podTemplateName},
		Conditions: []metav1.Condition{
			{
				Type:   "ReadyForProvisioning",
				Status: "True",
			},
		},
	}
}
