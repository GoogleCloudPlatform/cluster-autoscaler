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
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/gpu"
)

func makePodWithNodeSelector(selector map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			NodeSelector: selector,
		},
	}
}

func makePodWithNodeAffinity(nodeSelectorTerms []corev1.NodeSelectorTerm) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-affinity",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: nodeSelectorTerms,
					},
				},
			},
		},
	}
}

func makePodWithContainerResources(requests, limits corev1.ResourceList) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-res",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "main",
					Resources: corev1.ResourceRequirements{
						Requests: requests,
						Limits:   limits,
					},
				},
			},
		},
	}
}

func makePodWithInitContainerResources(requests, limits corev1.ResourceList) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-init-res",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{
				{
					Name: "init",
					Resources: corev1.ResourceRequirements{
						Requests: requests,
						Limits:   limits,
					},
				},
			},
		},
	}
}

func TestHasVmBasedBillingExclusions(t *testing.T) {
	testCases := []struct {
		name     string
		pod      *corev1.Pod
		expected bool
	}{
		{
			name:     "nil pod",
			pod:      nil,
			expected: false,
		},
		{
			name:     "plain pod without selectors or hardware requests",
			pod:      makePodWithNodeSelector(nil),
			expected: false,
		},
		{
			name: "pod with machine family selector",
			pod: makePodWithNodeSelector(map[string]string{
				gkelabels.MachineFamilyLabel: "n2",
			}),
			expected: true,
		},
		{
			name: "pod with machine family node affinity",
			pod: makePodWithNodeAffinity([]corev1.NodeSelectorTerm{
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{
							Key:      gkelabels.MachineFamilyLabel,
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"c3"},
						},
					},
				},
			}),
			expected: true,
		},
		{
			name: "pod with GPU label selector",
			pod: makePodWithNodeSelector(map[string]string{
				gkelabels.GPULabel: "nvidia-tesla-t4",
			}),
			expected: true,
		},
		{
			name: "pod with TPU label selector",
			pod: makePodWithNodeSelector(map[string]string{
				gkelabels.TPULabel: "tpu-v4-podslice",
			}),
			expected: true,
		},
		{
			name: "pod with TPU topology selector",
			pod: makePodWithNodeSelector(map[string]string{
				gkelabels.TPUTopologyLabel: "2x2x1",
			}),
			expected: true,
		},
		{
			name: "pod with Ephemeral Local SSD selector",
			pod: makePodWithNodeSelector(map[string]string{
				gkelabels.EphemeralLocalSsdLabel: "true",
			}),
			expected: true,
		},
		{
			name: "pod with Ephemeral Local SSD node affinity",
			pod: makePodWithNodeAffinity([]corev1.NodeSelectorTerm{
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{
							Key:      gkelabels.EphemeralLocalSsdLabel,
							Operator: corev1.NodeSelectorOpExists,
						},
					},
				},
			}),
			expected: true,
		},
		{
			name: "pod with pod-slots / PodCapacity selector",
			pod: makePodWithNodeSelector(map[string]string{
				gkelabels.PodCapacityLabel: "1",
			}),
			expected: true,
		},
		{
			name: "pod with pod-isolation / PodPerVMSize selector",
			pod: makePodWithNodeSelector(map[string]string{
				gkelabels.PodPerVMSizeLabel: "1",
			}),
			expected: true,
		},
		{
			name: "pod requesting pod-slots in container resources",
			pod: makePodWithContainerResources(corev1.ResourceList{
				corev1.ResourceName(gkelabels.PodCapacityLabel): *resource.NewQuantity(1, resource.DecimalSI),
			}, nil),
			expected: true,
		},
		{
			name: "pod with GPU in container Requests",
			pod: makePodWithContainerResources(corev1.ResourceList{
				gpu.ResourceNvidiaGPU: *resource.NewQuantity(1, resource.DecimalSI),
			}, nil),
			expected: true,
		},
		{
			name: "pod with GPU in container Limits",
			pod: makePodWithContainerResources(nil, corev1.ResourceList{
				gpu.ResourceNvidiaGPU: *resource.NewQuantity(1, resource.DecimalSI),
			}),
			expected: true,
		},
		{
			name: "pod with GPU in initContainer Requests",
			pod: makePodWithInitContainerResources(corev1.ResourceList{
				gpu.ResourceNvidiaGPU: *resource.NewQuantity(1, resource.DecimalSI),
			}, nil),
			expected: true,
		},
		{
			name: "pod with TPU in container Requests",
			pod: makePodWithContainerResources(corev1.ResourceList{
				tpu.ResourceGoogleTPU: *resource.NewQuantity(4, resource.DecimalSI),
			}, nil),
			expected: true,
		},
		{
			name: "pod with TPU in container Limits",
			pod: makePodWithContainerResources(nil, corev1.ResourceList{
				tpu.ResourceGoogleTPU: *resource.NewQuantity(4, resource.DecimalSI),
			}),
			expected: true,
		},
		{
			name: "pod with 0 GPU in container Requests and Limits",
			pod: makePodWithContainerResources(corev1.ResourceList{
				gpu.ResourceNvidiaGPU: *resource.NewQuantity(0, resource.DecimalSI),
			}, corev1.ResourceList{
				gpu.ResourceNvidiaGPU: *resource.NewQuantity(0, resource.DecimalSI),
			}),
			expected: false,
		},
		{
			name: "pod with 0 TPU in container Requests and Limits",
			pod: makePodWithContainerResources(corev1.ResourceList{
				tpu.ResourceGoogleTPU: *resource.NewQuantity(0, resource.DecimalSI),
			}, corev1.ResourceList{
				tpu.ResourceGoogleTPU: *resource.NewQuantity(0, resource.DecimalSI),
			}),
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, HasVmBasedBillingExclusions(tc.pod))
		})
	}
}

func TestGetPodFamilyForPayPerPodAutopilotWorkload(t *testing.T) {
	testCases := []struct {
		name           string
		pod            *corev1.Pod
		expectedFamily string
		expectedOk     bool
	}{
		{
			name:           "nil pod",
			pod:            nil,
			expectedFamily: "",
			expectedOk:     false,
		},
		{
			name:           "default x86 Autopilot workload",
			pod:            makePodWithNodeSelector(nil),
			expectedFamily: rules.GeneralPurposePodFamily,
			expectedOk:     true,
		},
		{
			name: "ARM Autopilot workload via LabelArchStable nodeSelector",
			pod: makePodWithNodeSelector(map[string]string{
				corev1.LabelArchStable: "arm64",
			}),
			expectedFamily: rules.GeneralPurposeArmPodFamily,
			expectedOk:     true,
		},
		{
			name: "ARM Autopilot workload via beta arch nodeSelector",
			pod: makePodWithNodeSelector(map[string]string{
				"beta.kubernetes.io/arch": "arm64",
			}),
			expectedFamily: rules.GeneralPurposeArmPodFamily,
			expectedOk:     true,
		},
		{
			name: "ARM Autopilot workload via nodeAffinity",
			pod: makePodWithNodeAffinity([]corev1.NodeSelectorTerm{
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{
							Key:      corev1.LabelArchStable,
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"arm64"},
						},
					},
				},
			}),
			expectedFamily: rules.GeneralPurposeArmPodFamily,
			expectedOk:     true,
		},
		{
			name: "disqualified by compute class label",
			pod: makePodWithNodeSelector(map[string]string{
				gkelabels.ComputeClassLabel: "Balanced",
			}),
			expectedFamily: "",
			expectedOk:     false,
		},
		{
			name: "disqualified by machine family selector",
			pod: makePodWithNodeSelector(map[string]string{
				gkelabels.MachineFamilyLabel: "c3",
			}),
			expectedFamily: "",
			expectedOk:     false,
		},
		{
			name: "disqualified by GPU request",
			pod: makePodWithContainerResources(corev1.ResourceList{
				gpu.ResourceNvidiaGPU: *resource.NewQuantity(1, resource.DecimalSI),
			}, nil),
			expectedFamily: "",
			expectedOk:     false,
		},
		{
			name: "disqualified by TPU selector",
			pod: makePodWithNodeSelector(map[string]string{
				gkelabels.TPULabel: "tpu-v4-lite-device",
			}),
			expectedFamily: "",
			expectedOk:     false,
		},
		{
			name: "disqualified by local SSD selector",
			pod: makePodWithNodeSelector(map[string]string{
				gkelabels.EphemeralLocalSsdLabel: "true",
			}),
			expectedFamily: "",
			expectedOk:     false,
		},
		{
			name: "disqualified by SoHW pod-slots",
			pod: makePodWithNodeSelector(map[string]string{
				gkelabels.PodCapacityLabel: "1",
			}),
			expectedFamily: "",
			expectedOk:     false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			family, ok := GetPodFamilyForPayPerPodAutopilotWorkload(tc.pod)
			assert.Equal(t, tc.expectedOk, ok)
			assert.Equal(t, tc.expectedFamily, family)
		})
	}
}

func TestGetBillingModelForCCC(t *testing.T) {
	gpFamily := rules.GeneralPurposePodFamily
	testCases := []struct {
		name          string
		ccCrd         crd.CRD
		expectedModel BillingModel
	}{
		{
			name:          "nil CRD",
			ccCrd:         nil,
			expectedModel: NodeBasedBilling,
		},
		{
			name: "CCC with only podFamily rule",
			ccCrd: crd.NewTestCrd(
				crd.WithName("ccc-gp"),
				crd.WithCrdType(ccc.CrdType),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithAutopilotModeRule(),
						rules.WithPodFamilyRule(&gpFamily),
					),
				}),
			),
			expectedModel: PodBasedBilling,
		},
		{
			name: "CCC without podFamily rule",
			ccCrd: crd.NewTestCrd(
				crd.WithName("ccc-no-family"),
				crd.WithCrdType(ccc.CrdType),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithAutopilotModeRule(),
					),
				}),
			),
			expectedModel: NodeBasedBilling,
		},
		{
			name: "CCC with both podFamily and non-podFamily rules",
			ccCrd: crd.NewTestCrd(
				crd.WithName("ccc-mixed"),
				crd.WithCrdType(ccc.CrdType),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithAutopilotModeRule(),
						rules.WithPodFamilyRule(&gpFamily),
					),
					rules.NewRule(
						rules.WithAutopilotModeRule(),
					),
				}),
			),
			expectedModel: MixedBilling,
		},
		{
			name: "CCC with podFamily rule and ScaleUpAnyway",
			ccCrd: crd.NewTestCrd(
				crd.WithName("ccc-gp-scaleup-anyway"),
				crd.WithCrdType(ccc.CrdType),
				crd.WithScaleUpAnyway(),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithAutopilotModeRule(),
						rules.WithPodFamilyRule(&gpFamily),
					),
				}),
			),
			expectedModel: MixedBilling,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedModel, GetBillingModelForCCC(tc.ccCrd))
		})
	}
}

func TestGetBillingModel(t *testing.T) {
	gpFamily := rules.GeneralPurposePodFamily
	cccWithPodFamily := crd.NewTestCrd(
		crd.WithName("ccc-gp"),
		crd.WithCrdType(ccc.CrdType),
		crd.WithRules([]rules.Rule{
			rules.NewRule(
				rules.WithAutopilotModeRule(),
				rules.WithPodFamilyRule(&gpFamily),
			),
		}),
	)

	cccWithoutPodFamily := crd.NewTestCrd(
		crd.WithName("ccc-no-family"),
		crd.WithCrdType(ccc.CrdType),
		crd.WithRules([]rules.Rule{
			rules.NewRule(
				rules.WithAutopilotModeRule(),
			),
		}),
	)

	cccMixed := crd.NewTestCrd(
		crd.WithName("ccc-mixed"),
		crd.WithCrdType(ccc.CrdType),
		crd.WithRules([]rules.Rule{
			rules.NewRule(
				rules.WithAutopilotModeRule(),
				rules.WithPodFamilyRule(&gpFamily),
			),
			rules.NewRule(
				rules.WithAutopilotModeRule(),
			),
		}),
	)

	cccWithPodFamilyAndScaleUpAnyway := crd.NewTestCrd(
		crd.WithName("ccc-gp-scaleup-anyway"),
		crd.WithCrdType(ccc.CrdType),
		crd.WithScaleUpAnyway(),
		crd.WithRules([]rules.Rule{
			rules.NewRule(
				rules.WithAutopilotModeRule(),
				rules.WithPodFamilyRule(&gpFamily),
			),
		}),
	)

	testCases := []struct {
		name             string
		pod              *corev1.Pod
		crd              crd.CRD
		computeClassName string
		isAutopilot      bool
		expectedModel    BillingModel
	}{
		// Autopilot cluster tests
		{
			name:             "autopilot: default pod with no compute class",
			pod:              makePodWithNodeSelector(nil),
			crd:              nil,
			computeClassName: "",
			isAutopilot:      true,
			expectedModel:    PodBasedBilling,
		},
		{
			name:             "autopilot: pod with Predefined Compute Class Balanced",
			pod:              makePodWithNodeSelector(map[string]string{gkelabels.ComputeClassLabel: "Balanced"}),
			crd:              nil,
			computeClassName: "Balanced",
			isAutopilot:      true,
			expectedModel:    PodBasedBilling,
		},
		{
			name:             "autopilot: pod with Predefined Compute Class Scale-Out",
			pod:              makePodWithNodeSelector(map[string]string{gkelabels.ComputeClassLabel: "Scale-Out"}),
			crd:              nil,
			computeClassName: "Scale-Out",
			isAutopilot:      true,
			expectedModel:    PodBasedBilling,
		},
		{
			name:             "autopilot: pod with Predefined Compute Class Performance",
			pod:              makePodWithNodeSelector(map[string]string{gkelabels.ComputeClassLabel: "Performance"}),
			crd:              nil,
			computeClassName: "Performance",
			isAutopilot:      true,
			expectedModel:    NodeBasedBilling,
		},
		{
			name:             "autopilot: pod with Predefined Compute Class Accelerator",
			pod:              makePodWithNodeSelector(map[string]string{gkelabels.ComputeClassLabel: "Accelerator"}),
			crd:              nil,
			computeClassName: "Accelerator",
			isAutopilot:      true,
			expectedModel:    NodeBasedBilling,
		},
		{
			name:             "autopilot: pod with CCC defining only pod family",
			pod:              makePodWithNodeSelector(map[string]string{gkelabels.ComputeClassLabel: "ccc-gp"}),
			crd:              cccWithPodFamily,
			computeClassName: "ccc-gp",
			isAutopilot:      true,
			expectedModel:    PodBasedBilling,
		},
		{
			name:             "autopilot: pod with CCC not defining pod family",
			pod:              makePodWithNodeSelector(map[string]string{gkelabels.ComputeClassLabel: "ccc-no-family"}),
			crd:              cccWithoutPodFamily,
			computeClassName: "ccc-no-family",
			isAutopilot:      true,
			expectedModel:    NodeBasedBilling,
		},
		{
			name:             "autopilot: pod with CCC defining both pod family and machine family (mixed)",
			pod:              makePodWithNodeSelector(map[string]string{gkelabels.ComputeClassLabel: "ccc-mixed"}),
			crd:              cccMixed,
			computeClassName: "ccc-mixed",
			isAutopilot:      true,
			expectedModel:    MixedBilling,
		},
		{
			name:             "autopilot: pod with CCC defining pod family and ScaleUpAnyway (mixed)",
			pod:              makePodWithNodeSelector(map[string]string{gkelabels.ComputeClassLabel: "ccc-gp-scaleup-anyway"}),
			crd:              cccWithPodFamilyAndScaleUpAnyway,
			computeClassName: "ccc-gp-scaleup-anyway",
			isAutopilot:      true,
			expectedModel:    MixedBilling,
		},
		{
			name: "autopilot: pod with Balanced CC but specifying GPU request is disqualified",
			pod: func() *corev1.Pod {
				p := makePodWithContainerResources(corev1.ResourceList{
					gpu.ResourceNvidiaGPU: *resource.NewQuantity(1, resource.DecimalSI),
				}, nil)
				p.Spec.NodeSelector = map[string]string{gkelabels.ComputeClassLabel: "Balanced"}
				return p
			}(),
			crd:              nil,
			computeClassName: "Balanced",
			isAutopilot:      true,
			expectedModel:    NodeBasedBilling,
		},
		{
			name: "autopilot: default pod specifying machine-family is disqualified",
			pod: makePodWithNodeSelector(map[string]string{
				gkelabels.MachineFamilyLabel: "c3",
			}),
			crd:              nil,
			computeClassName: "",
			isAutopilot:      true,
			expectedModel:    NodeBasedBilling,
		},
		// Standard cluster tests
		{
			name:             "standard: default pod with no compute class",
			pod:              makePodWithNodeSelector(nil),
			crd:              nil,
			computeClassName: "",
			isAutopilot:      false,
			expectedModel:    NodeBasedBilling,
		},
		{
			name:             "standard: pod with CCC defining only pod family",
			pod:              makePodWithNodeSelector(map[string]string{gkelabels.ComputeClassLabel: "ccc-gp"}),
			crd:              cccWithPodFamily,
			computeClassName: "ccc-gp",
			isAutopilot:      false,
			expectedModel:    PodBasedBilling,
		},
		{
			name:             "standard: pod with CCC not defining pod family",
			pod:              makePodWithNodeSelector(map[string]string{gkelabels.ComputeClassLabel: "ccc-no-family"}),
			crd:              cccWithoutPodFamily,
			computeClassName: "ccc-no-family",
			isAutopilot:      false,
			expectedModel:    NodeBasedBilling,
		},
		{
			name:             "standard: pod with CCC defining both pod family and machine family (mixed)",
			pod:              makePodWithNodeSelector(map[string]string{gkelabels.ComputeClassLabel: "ccc-mixed"}),
			crd:              cccMixed,
			computeClassName: "ccc-mixed",
			isAutopilot:      false,
			expectedModel:    MixedBilling,
		},
		{
			name:             "standard: pod with Predefined Compute Class Balanced",
			pod:              makePodWithNodeSelector(map[string]string{gkelabels.ComputeClassLabel: "Balanced"}),
			crd:              nil,
			computeClassName: "Balanced",
			isAutopilot:      false,
			expectedModel:    NodeBasedBilling,
		},
		{
			name: "standard: pod with CCC defining pod family but specifying machine family is disqualified",
			pod: makePodWithNodeSelector(map[string]string{
				gkelabels.ComputeClassLabel:  "ccc-gp",
				gkelabels.MachineFamilyLabel: "c3",
			}),
			crd:              cccWithPodFamily,
			computeClassName: "ccc-gp",
			isAutopilot:      false,
			expectedModel:    NodeBasedBilling,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			model := GetBillingModel(tc.pod, tc.crd, tc.computeClassName, tc.isAutopilot)
			assert.Equal(t, tc.expectedModel, model, "GetBillingModel mismatch")
		})
	}
}
