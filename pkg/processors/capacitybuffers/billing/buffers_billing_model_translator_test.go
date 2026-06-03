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
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	cbclient "k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/client"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	fakeClient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

func TestBillingModelTranslator(t *testing.T) {
	podFamily := "general-purpose"
	CCCWithPodFamily := crd.NewTestCrd(
		crd.WithName("CCCWithPodFamily"),
		crd.WithCrdType(ccc.CrdType),
		crd.WithAutoprovisioningEnabled(),
		crd.WithAutopilotManaged(),
		crd.WithRules([]rules.Rule{
			rules.NewRule(
				rules.WithAutopilotModeRule(),
				rules.WithPodFamilyRule(&podFamily),
			),
		}),
		crd.WithLabel(labels.ComputeClassLabel),
	)
	CCCWithoutPodFamily := crd.NewTestCrd(
		crd.WithName("CCCWithoutPodFamily"),
		crd.WithCrdType(ccc.CrdType),
		crd.WithAutoprovisioningEnabled(),
		crd.WithLabel(labels.ComputeClassLabel),
	)

	lister := lister.NewMockCrdListerWithLabel([]crd.CRD{CCCWithPodFamily, CCCWithoutPodFamily}, labels.ComputeClassLabel)

	podTempUsesCCCPodBasedBilling := getPodTemplateWithNodeSelectors("podTempUsesPodBasedBilling", map[string]string{labels.ComputeClassLabel: CCCWithPodFamily.Name()})
	podTempUsesCCCNodeBasedBilling := getPodTemplateWithNodeSelectors("podTempUsesNodeBasedBilling", map[string]string{labels.ComputeClassLabel: CCCWithoutPodFamily.Name()})
	podTempUsesPerformanceCC := getPodTemplateWithNodeSelectors("podTempUsesPerformanceCC", map[string]string{labels.ComputeClassLabel: "Performance"})
	podTempUsesScaleOutCC := getPodTemplateWithNodeSelectors("podTempUsesScaleOutCC", map[string]string{labels.ComputeClassLabel: "Scale-Out"})
	podTempWithNoCrd := getPodTemplateWithNodeSelectors("podTempWithNoCrd", map[string]string{})
	podTempUsesTPUNodeSel := getPodTemplateWithNodeSelectors("podTempUsesTPUNodeSel", map[string]string{gkelabels.TPULabel: ""})
	podTempUsesMachineFamilyForSoHW := getPodTemplateWithNodeSelectors("podTempUsesMachineFamilyForSoHW", map[string]string{labels.MachineFamilyLabel: "C3"})
	podTempUsesGPUResource := getPodTemplateWithResourceLimits("podTempUsesGPUResource", corev1.ResourceList{
		gpu.ResourceNvidiaGPU: *resource.NewQuantity(1, resource.DecimalSI),
	})
	podTempUsesAffinityForSoHW := getPodTemplateWithNodeAffinity("podTempUsesAffinityForSoHW", &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{
							Key:      gkelabels.ComputeClassLabel,
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"Performance"},
						},
					},
				},
			},
		},
	})

	fakeClient := fakeClient.NewClientset(podTempUsesCCCPodBasedBilling, podTempUsesCCCNodeBasedBilling,
		podTempUsesTPUNodeSel, podTempUsesGPUResource, podTempUsesPerformanceCC, podTempUsesScaleOutCC, podTempWithNoCrd,
		podTempUsesAffinityForSoHW, podTempUsesMachineFamilyForSoHW)
	fakeCapacityBuffersClient, _ := cbclient.NewCapacityBufferClient(nil, fakeClient, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	testCases := []struct {
		name               string
		isAutopilot        bool
		buffersToTranslate []*v1beta1.CapacityBuffer
		expectingError     []bool
	}{
		{
			name:               "standard: no buffers",
			isAutopilot:        false,
			buffersToTranslate: []*v1beta1.CapacityBuffer{},
			expectingError:     []bool{},
		},
		{
			name:        "standard: a buffer with template pod uses no CRD",
			isAutopilot: false,
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getBufferReadyForProvisioning(podTempWithNoCrd.Name),
			},
			expectingError: []bool{false},
		},
		{
			name:        "standard: a buffer with pod template uses CCC with pod based billing (pod family defined)",
			isAutopilot: false,
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getBufferReadyForProvisioning(podTempUsesCCCPodBasedBilling.Name),
			},
			expectingError: []bool{true},
		},
		{
			name:        "standard: a buffer with pod template uses predefined performance CC",
			isAutopilot: false,
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getBufferReadyForProvisioning(podTempUsesPerformanceCC.Name),
			},
			expectingError: []bool{false},
		},
		{
			name:        "standard: a buffer with pod template uses CCC with node based billing (no pod family defined)",
			isAutopilot: false,
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getBufferReadyForProvisioning(podTempUsesCCCNodeBasedBilling.Name),
			},
			expectingError: []bool{false},
		},
		{
			name:        "standard: all together",
			isAutopilot: false,
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getBufferReadyForProvisioning(podTempUsesCCCNodeBasedBilling.Name),
				getBufferReadyForProvisioning(podTempUsesCCCPodBasedBilling.Name),
				getBufferReadyForProvisioning(podTempUsesPerformanceCC.Name),
				getBufferReadyForProvisioning(podTempWithNoCrd.Name),
			},
			expectingError: []bool{false, true, false, false},
		},
		{
			name:               "autopilot: no buffers",
			isAutopilot:        true,
			buffersToTranslate: []*v1beta1.CapacityBuffer{},
			expectingError:     []bool{},
		},
		{
			name:        "autopilot: a buffer with template pod uses no CRD",
			isAutopilot: true,
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getBufferReadyForProvisioning(podTempWithNoCrd.Name),
			},
			expectingError: []bool{true},
		},
		{
			name:        "autopilot: a buffer with pod template uses CCC with pod based billing (pod family defined)",
			isAutopilot: true,
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getBufferReadyForProvisioning(podTempUsesCCCPodBasedBilling.Name),
			},
			expectingError: []bool{true},
		},
		{
			name:        "autopilot: a buffer with pod template uses predefined performance CC",
			isAutopilot: true,
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getBufferReadyForProvisioning(podTempUsesPerformanceCC.Name),
			},
			expectingError: []bool{false},
		},
		{
			name:        "autopilot: a buffer with pod template uses predefined scale-out CC",
			isAutopilot: true,
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getBufferReadyForProvisioning(podTempUsesScaleOutCC.Name),
			},
			expectingError: []bool{true},
		},
		{
			name:        "autopilot: a buffer with pod template uses TPU resource",
			isAutopilot: true,
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getBufferReadyForProvisioning(podTempUsesTPUNodeSel.Name),
			},
			expectingError: []bool{false},
		},
		{
			name:        "autopilot: a buffer with pod template uses GPU resource",
			isAutopilot: true,
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getBufferReadyForProvisioning(podTempUsesGPUResource.Name),
			},
			expectingError: []bool{false},
		},
		{
			name:        "autopilot: a buffer with pod template uses CCC with node based billing (no pod family defined)",
			isAutopilot: true,
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getBufferReadyForProvisioning(podTempUsesCCCNodeBasedBilling.Name),
			},
			expectingError: []bool{false},
		},
		{
			name:        "autopilot: a buffer with pod template uses machine family label for node based billing",
			isAutopilot: true,
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getBufferReadyForProvisioning(podTempUsesMachineFamilyForSoHW.Name),
			},
			expectingError: []bool{false},
		},
		{
			name:        "autopilot: a buffer with pod template uses pod template affinity for node based billing",
			isAutopilot: true,
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getBufferReadyForProvisioning(podTempUsesAffinityForSoHW.Name),
			},
			expectingError: []bool{false},
		},
		{
			name:        "autopilot: all together",
			isAutopilot: true,
			buffersToTranslate: []*v1beta1.CapacityBuffer{
				getBufferReadyForProvisioning(podTempUsesCCCNodeBasedBilling.Name),
				getBufferReadyForProvisioning(podTempUsesCCCPodBasedBilling.Name),
				getBufferReadyForProvisioning(podTempUsesPerformanceCC.Name),
				getBufferReadyForProvisioning(podTempWithNoCrd.Name),
			},
			expectingError: []bool{false, true, false, true},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			translator := NewBillingModelTranslator(fakeCapacityBuffersClient, lister, tc.isAutopilot)
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

func getPodTemplateWithNodeSelectors(name string, nodeSelector map[string]string) *corev1.PodTemplate {
	return &corev1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  "default",
			Generation: 1,
		},
		Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				NodeSelector: nodeSelector,
			},
		},
	}
}

func getPodTemplateWithResourceLimits(name string, Limits corev1.ResourceList) *corev1.PodTemplate {
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
							Limits: Limits,
						},
					},
				},
			},
		},
	}
}

func getPodTemplateWithNodeAffinity(name string, NodeAffinity *corev1.NodeAffinity) *corev1.PodTemplate {
	return &corev1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  "default",
			Generation: 1,
		},
		Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Affinity: &corev1.Affinity{
					NodeAffinity: NodeAffinity,
				},
			},
		},
	}
}

func getBufferReadyForProvisioning(podTemplateName string) *v1beta1.CapacityBuffer {
	return &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "CB_" + podTemplateName,
			Namespace:  "default",
			Generation: 1,
		},
		Status: v1beta1.CapacityBufferStatus{
			PodTemplateRef: &v1beta1.LocalObjectRef{Name: podTemplateName},
			Conditions: []metav1.Condition{
				{
					Type:   "ReadyForProvisioning",
					Status: "True",
				},
			},
		},
	}
}
