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
	"fmt"
	"testing"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
)

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
			got, err := processor.Process(nil, tc.unschedulablePods)

			assert.ElementsMatch(t, tc.want, got)
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
	got, err := processor.Process(nil, []*apiv1.Pod{pod1})

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
	got, err := processor.Process(nil, pods)

	assert.ElementsMatch(t, pods, got)
	assert.NoError(t, err)
	provider.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
}
