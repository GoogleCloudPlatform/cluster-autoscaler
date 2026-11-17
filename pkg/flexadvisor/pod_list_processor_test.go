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

package flexadvisor

import (
	"context"
	"fmt"
	"testing"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/gpu"
)

type mockAutopilotCloudProvider struct {
	cloudprovider.CloudProvider
	autopilotEnabled bool
}

func (m *mockAutopilotCloudProvider) IsAutopilotEnabled() bool {
	return m.autopilotEnabled
}

func TestProcess(t *testing.T) {
	pod1 := testPod("ccc-1")
	pod2 := testPod("ccc-1")
	pod3 := testPod("ccc-2")
	pod4 := &apiv1.Pod{}
	pod5 := &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"random-key": "random-value"}}}

	crd1 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ccc-1",
		},
	}, "", false, nil, nil)
	crd2 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ccc-2",
		},
	}, "", false, nil, nil)

	testCases := []struct {
		name              string
		initialSetup      func(provider *instanceavailability.MockProvider)
		unschedulablePods []*apiv1.Pod
		want              []*apiv1.Pod
	}{
		{
			name: "multiple pods with same ccc label",
			initialSetup: func(provider *instanceavailability.MockProvider) {
				provider.On("RegisterFlexibilityScope", "ccc-1").Return(nil).Once()
			},
			unschedulablePods: []*apiv1.Pod{pod1, pod2},
			want:              []*apiv1.Pod{pod1, pod2},
		},
		{
			name: "multiple pods with different ccc labels",
			initialSetup: func(provider *instanceavailability.MockProvider) {
				provider.On("RegisterFlexibilityScope", "ccc-1").Return(nil).Once()
				provider.On("RegisterFlexibilityScope", "ccc-2").Return(nil).Once()
			},
			unschedulablePods: []*apiv1.Pod{pod1, pod2, pod3},
			want:              []*apiv1.Pod{pod1, pod2, pod3},
		},
		{
			name:              "pod without ccc labels",
			initialSetup:      nil,
			unschedulablePods: []*apiv1.Pod{pod4, pod5},
			want:              []*apiv1.Pod{pod4, pod5},
		},
		{
			name:              "empty pod list",
			initialSetup:      nil,
			unschedulablePods: []*apiv1.Pod{},
			want:              []*apiv1.Pod{},
		},
		{
			name: "provider error does not break loop",
			initialSetup: func(provider *instanceavailability.MockProvider) {
				provider.On("RegisterFlexibilityScope", "ccc-1").Return(fmt.Errorf("some error")).Once()
			},
			unschedulablePods: []*apiv1.Pod{pod1},
			want:              []*apiv1.Pod{pod1},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := &instanceavailability.MockProvider{}
			if tc.initialSetup != nil {
				tc.initialSetup(provider)
			}
			mockLister := lister.NewMockCrdListerWithLabel([]crd.CRD{crd1, crd2}, labels.ComputeClassLabel)

			processor := NewPodListProcessor(provider, mockLister, experiments.NewMockManager())
			got, err := processor.Process(context.TODO(), nil, tc.unschedulablePods)

			assert.ElementsMatch(t, tc.want, got)
			assert.NoError(t, err)
			provider.AssertExpectations(t)
		})
	}
}

func TestProcess_PayPerPodAutopilotWorkloads(t *testing.T) {
	defaultX86Pod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "default-x86"},
	}
	defaultArmPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "default-arm"},
		Spec: apiv1.PodSpec{
			NodeSelector: map[string]string{
				apiv1.LabelArchStable: "arm64",
			},
		},
	}
	defaultMultiArchPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "default-multi-arch"},
		Spec: apiv1.PodSpec{
			Affinity: &apiv1.Affinity{
				NodeAffinity: &apiv1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
						NodeSelectorTerms: []apiv1.NodeSelectorTerm{
							{
								MatchExpressions: []apiv1.NodeSelectorRequirement{
									{
										Key:      apiv1.LabelArchStable,
										Operator: apiv1.NodeSelectorOpIn,
										Values:   []string{"arm64", "amd64"},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	machineFamilyExcludedPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "excluded-machine-family"},
		Spec: apiv1.PodSpec{
			NodeSelector: map[string]string{
				labels.MachineFamilyLabel: "n2",
			},
		},
	}
	gpuExcludedPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "excluded-gpu"},
		Spec: apiv1.PodSpec{
			Containers: []apiv1.Container{
				{
					Resources: apiv1.ResourceRequirements{
						Requests: apiv1.ResourceList{
							gpu.ResourceNvidiaGPU: resource.MustParse("1"),
						},
					},
				},
			},
		},
	}
	pccPod := testPod("Balanced")

	testCases := []struct {
		name              string
		autopilotEnabled  bool
		experimentEnabled bool
		unschedulablePods []*apiv1.Pod
		expectedScopes    []string
	}{
		{
			name:              "Autopilot with experiment enabled registers general-purpose for default x86 pod",
			autopilotEnabled:  true,
			experimentEnabled: true,
			unschedulablePods: []*apiv1.Pod{defaultX86Pod, defaultX86Pod},
			expectedScopes:    []string{ccc.AutopilotPodFamilyCrdName},
		},
		{
			name:              "Autopilot with experiment enabled registers unified general-purpose scope for default ARM pod",
			autopilotEnabled:  true,
			experimentEnabled: true,
			unschedulablePods: []*apiv1.Pod{defaultArmPod},
			expectedScopes:    []string{ccc.AutopilotPodFamilyCrdName},
		},
		{
			name:              "Autopilot with experiment enabled registers single unified scope for x86, ARM, and multi-arch pods and PCC",
			autopilotEnabled:  true,
			experimentEnabled: true,
			unschedulablePods: []*apiv1.Pod{defaultX86Pod, defaultArmPod, defaultMultiArchPod, pccPod},
			expectedScopes:    []string{ccc.AutopilotPodFamilyCrdName, "Balanced"},
		},
		{
			name:              "Autopilot with experiment enabled skips pods with VM-based billing exclusions",
			autopilotEnabled:  true,
			experimentEnabled: true,
			unschedulablePods: []*apiv1.Pod{machineFamilyExcludedPod, gpuExcludedPod},
			expectedScopes:    nil,
		},
		{
			name:              "Autopilot with experiment disabled does not register general-purpose scope",
			autopilotEnabled:  true,
			experimentEnabled: false,
			unschedulablePods: []*apiv1.Pod{defaultX86Pod, defaultArmPod},
			expectedScopes:    nil,
		},
		{
			name:              "Standard cluster with experiment enabled does not register general-purpose scope",
			autopilotEnabled:  false,
			experimentEnabled: true,
			unschedulablePods: []*apiv1.Pod{defaultX86Pod, defaultArmPod},
			expectedScopes:    nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := &instanceavailability.MockProvider{}
			for _, scope := range tc.expectedScopes {
				provider.On("RegisterFlexibilityScope", scope).Return(nil).Once()
			}
			mockLister := lister.NewMockCrdListerWithLabel(nil, labels.ComputeClassLabel)
			var em experiments.Manager
			if tc.experimentEnabled {
				em = experiments.NewMockManager(experiments.PayPerPodFleetEfficiencyMinCAVersionFlag)
			} else {
				em = experiments.NewMockManager()
			}
			autoscalingCtx := &ca_context.AutoscalingContext{
				CloudProvider: &mockAutopilotCloudProvider{autopilotEnabled: tc.autopilotEnabled},
			}
			processor := NewPodListProcessor(provider, mockLister, em)
			got, err := processor.Process(context.Background(), autoscalingCtx, tc.unschedulablePods)

			assert.ElementsMatch(t, tc.unschedulablePods, got)
			assert.NoError(t, err)
			provider.AssertExpectations(t)
		})
	}
}

func testPod(flexibilityScopeName string) *apiv1.Pod {
	return &apiv1.Pod{
		Spec: apiv1.PodSpec{
			NodeSelector: map[string]string{
				labels.ComputeClassLabel: flexibilityScopeName,
			},
		},
	}
}

func TestProcess_FlexAdvisorDisabled(t *testing.T) {
	pod1 := testPod("ccc-1")
	crd1 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ccc-1",
		},
	}, "", false, nil, nil)

	provider := &instanceavailability.MockProvider{}
	// RegisterFlexibilityScope should NOT be called
	provider.On("RegisterFlexibilityScope").Maybe().Panic("RegisterFlexibilityScope should not be called")

	mockLister := lister.NewMockCrdListerWithLabel([]crd.CRD{crd1}, labels.ComputeClassLabel)
	boolFlags := map[string]bool{
		experiments.FlexAdvisorProcessingEnabledFlag: false,
	}
	mockManager := experiments.NewMockManagerWithOptions(version.Version{}, boolFlags, map[string]string{})

	processor := NewPodListProcessor(provider, mockLister, mockManager)
	got, err := processor.Process(context.TODO(), nil, []*apiv1.Pod{pod1})

	assert.ElementsMatch(t, []*apiv1.Pod{pod1}, got)
	assert.NoError(t, err)
	provider.AssertExpectations(t)
}

type mockPodListProcessorMetrics struct {
	mock.Mock
}

func (m *mockPodListProcessorMetrics) UpdateFlexAdvisorRejectedScopes(rejected int) {
	m.Called(rejected)
}

func TestProcess_RegistersScopesAndEmitsMetrics(t *testing.T) {
	pods := make([]*apiv1.Pod, 0, 250)
	var crds []crd.CRD
	for i := 0; i < 250; i++ {
		scopeName := fmt.Sprintf("ccc-%d", i)
		pods = append(pods, testPod(scopeName))
		crdItem := ccc.NewCccCrd(&v1.ComputeClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: scopeName,
			},
		}, "", false, nil, nil)
		crds = append(crds, crdItem)
	}

	provider := &instanceavailability.MockProvider{}

	// Simulate 50 successful registrations and 200 failures
	provider.On("RegisterFlexibilityScope", mock.Anything).Return(nil).Times(50)
	provider.On("RegisterFlexibilityScope", mock.Anything).Return(fmt.Errorf("active scope limit of 50 reached")).Times(200)

	mockLister := lister.NewMockCrdListerWithLabel(crds, labels.ComputeClassLabel)
	mockManager := experiments.NewMockManager()

	mockMetrics := &mockPodListProcessorMetrics{}
	mockMetrics.On("UpdateFlexAdvisorRejectedScopes", 200).Return().Once()

	processor := NewPodListProcessor(provider, mockLister, mockManager, withPodListProcessorMetrics(mockMetrics))
	got, err := processor.Process(context.TODO(), nil, pods)

	assert.ElementsMatch(t, pods, got)
	assert.NoError(t, err)
	provider.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
}

func TestPodListProcessor_ResetRejectedScopes(t *testing.T) {
	testCases := []struct {
		name               string
		nilProcessor       bool
		nilMetrics         bool
		experimentsManager experiments.Manager
		wantReset          bool
	}{
		{
			name:               "processing enabled - resets rejected scopes gauge to 0",
			experimentsManager: experiments.NewMockManager(),
			wantReset:          true,
		},
		{
			name: "processing disabled - does not update rejected scopes gauge",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag: false,
			}, nil),
		},
		{
			name:               "nil receiver - does not panic",
			nilProcessor:       true,
			experimentsManager: experiments.NewMockManager(),
		},
		{
			name:               "nil metrics - does not panic",
			nilMetrics:         true,
			experimentsManager: experiments.NewMockManager(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockMetrics := &mockPodListProcessorMetrics{}
			if tc.wantReset {
				mockMetrics.On("UpdateFlexAdvisorRejectedScopes", 0).Return().Once()
			}

			var processor *PodListProcessor
			if !tc.nilProcessor {
				var metricsArg podListProcessorMetrics = mockMetrics
				if tc.nilMetrics {
					metricsArg = nil
				}
				processor = NewPodListProcessor(&instanceavailability.MockProvider{}, nil, tc.experimentsManager, withPodListProcessorMetrics(metricsArg))
			}

			processor.ResetRejectedScopes()

			mockMetrics.AssertExpectations(t)
		})
	}
}
