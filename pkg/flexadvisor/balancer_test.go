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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/testutil"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
)

type MigOption func(mig *gke.GkeMig)

// WithMaxRunDuration returns MigOption adding MaxRunDurationRule.
func WithMaxRunDuration(maxRunDurationSeconds string) MigOption {
	return func(mig *gke.GkeMig) {
		mig.Spec().MaxRunDurationInSeconds = maxRunDurationSeconds
	}
}

// WithDWS returns MigOption adding DWS
func WithDWS() MigOption {
	return func(mig *gke.GkeMig) {
		mig.Spec().FlexStart = true
	}
}

// WithTpuType returns MigOption adding tpuType.
func WithTpuType(tpuType string) MigOption {
	return func(mig *gke.GkeMig) {
		mig.Spec().TpuType = tpuType
	}
}

// WithTpuTopology returns MigOption adding tpuTopology.
func WithTpuTopology(tpuTopology string) MigOption {
	return func(mig *gke.GkeMig) {
		mig.Spec().TpuTopology = tpuTopology
	}
}

// WithTpuMultiHost returns MigOption adding tpuMultiHost.
func WithTpuMultiHost() MigOption {
	return func(mig *gke.GkeMig) {
		mig.Spec().TpuMultiHost = true
	}
}

func TestBalanceScaleUpBetweenGroups(t *testing.T) {

	gkeMockManager := &gke.GkeManagerMock{}
	gkeMockManager.On("IsResizeRequestErrorHandlingEnabled").Return(false)

	mig1 := createMockMigs(gkeMockManager, "e2-standard-4", "us-central1-a", "ccc-1", gke.LocationPolicyBalanced, 100, 200, nil)
	mig2 := createMockMigs(gkeMockManager, "e2-standard-4", "us-central1-b", "ccc-1", gke.LocationPolicyBalanced, 150, 200, nil)
	mig3 := createMockMigs(gkeMockManager, "e2-standard-4", "us-central1-c", "ccc-1", gke.LocationPolicyBalanced, 150, 200, nil)

	locationAnyMig1 := createMockMigs(gkeMockManager, "e2-standard-4", "us-central1-a", "ccc-1", gke.LocationPolicyAny, 0, 1000, nil)
	locationAnyMig2 := createMockMigs(gkeMockManager, "e2-standard-4", "us-central1-b", "ccc-1", gke.LocationPolicyAny, 0, 1000, nil)
	locationAnyMig3 := createMockMigs(gkeMockManager, "e2-standard-4", "us-central1-c", "ccc-1", gke.LocationPolicyAny, 0, 1000, nil)

	dwsMig := createMockMigs(gkeMockManager, "e2-standard-4", "us-central1-a", "ccc-1", gke.LocationPolicyBalanced, 0, 10, nil, WithDWS(), WithMaxRunDuration("3600"))
	tpuMig := createMockMigs(gkeMockManager, "ct5p-hightpu-4t", "us-central1-a", "ccc-1", gke.LocationPolicyBalanced, 0, 10, nil, WithTpuType("tpu-v5p-slice"), WithTpuTopology("2x2x1"))
	tpuMultiHostMig := createMockMigs(gkeMockManager, "ct5p-hightpu-4t", "us-central1-a", "ccc-1", gke.LocationPolicyBalanced, 0, 10, nil, WithTpuType("tpu-v5p-slice"), WithTpuTopology("2x2x5"), WithTpuMultiHost())

	dwsTpuMig := createMockMigs(gkeMockManager, "ct5p-hightpu-4t", "us-central1-a", "ccc-1", gke.LocationPolicyBalanced, 0, 10, nil, WithDWS(), WithMaxRunDuration("3600"), WithTpuType("tpu-v5p-slice"), WithTpuTopology("2x2x5"))

	snapshot1 := instanceavailability.NewSnapshot(nil, "ccc-1", "", "guidance-1", map[string]int{"us-central1-a": 100, "us-central1-b": 100, "us-central1-c": 100}, map[string]float64{"us-central1-a": 1.0, "us-central1-b": 0.8, "us-central1-c": 0.8})
	snapshot2 := instanceavailability.NewSnapshot(nil, "ccc-1", "", "guidance-2", map[string]int{"us-central1-a": 100, "us-central1-b": 100, "us-central1-c": 0}, map[string]float64{"us-central1-a": 1.0, "us-central1-b": 0.8, "us-central1-c": 0.8})

	crd1 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ccc-1",
		},
	}, "", false, nil, nil)

	testCases := []struct {
		name                  string
		groups                []cloudprovider.NodeGroup
		newNodes              int
		initialSetup          func(provider *instanceavailability.MockProvider)
		wantScaleUpInfos      []nodegroupset.ScaleUpInfo
		enabledFeatures       []string
		disabledFeatures      []string
		withFallbackBalancers func(provider *instanceavailability.MockProvider, experimentsManager experiments.Manager, lister lister.Lister, registerMock func(m *mock.Mock)) nodegroupset.NodeGroupSetProcessor
	}{
		{
			name:     "b/501046868 - locationPolicy == ANY - fallbacks to second FA balancer, propagates result without substracting capacity again",
			groups:   []cloudprovider.NodeGroup{locationAnyMig1, locationAnyMig2, locationAnyMig3},
			newNodes: 274,
			withFallbackBalancers: func(provider *instanceavailability.MockProvider, experimentsManager experiments.Manager, lister lister.Lister, registerMock func(m *mock.Mock)) nodegroupset.NodeGroupSetProcessor {
				// FA balancer with ignoreRLA==true installed as fallback to main FA
				return NewScaleUpBalancer(nil, provider, lister, experimentsManager, true)
			},
			initialSetup: func(provider *instanceavailability.MockProvider) {
				snapshot := instanceavailability.NewSnapshot(provider, "ccc-1", "", "guidance-1", map[string]int{
					"us-central1-a": 91,
					"us-central1-b": 91,
					"us-central1-c": 91,
				}, map[string]float64{"us-central1-a": 1.0, "us-central1-b": 1.0, "us-central1-c": 1.0})

				provider.On("AwaitInstanceAvailability", "ccc-1", "machineType: e2-standard-4, provisioningMode: STANDARD").Once().Return(snapshot, nil)
				provider.On("MarkUsed", "ccc-1", "", "guidance-1", mock.Anything, map[string]int{
					"us-central1-a": 91,
					"us-central1-b": 91,
					"us-central1-c": 91,
				}).Once().Return(nil)
			},
			wantScaleUpInfos: []nodegroupset.ScaleUpInfo{
				{
					Group:       locationAnyMig1,
					CurrentSize: 0,
					NewSize:     91,
					MaxSize:     1000,
				},
				{
					Group:       locationAnyMig2,
					CurrentSize: 0,
					NewSize:     91,
					MaxSize:     1000,
				},
				{
					Group:       locationAnyMig3,
					CurrentSize: 0,
					NewSize:     91,
					MaxSize:     1000,
				},
			},
		},
		{
			name:     "balance using flex advisor with plenty of capacity",
			groups:   []cloudprovider.NodeGroup{mig1, mig2, mig3},
			newNodes: 80,
			initialSetup: func(provider *instanceavailability.MockProvider) {
				snapshot1.SetProvider(provider)
				provider.On("AwaitInstanceAvailability", "ccc-1", "machineType: e2-standard-4, provisioningMode: STANDARD").Once().Return(snapshot1, nil)
				provider.On("MarkUsed", "ccc-1", "", "guidance-1", mock.Anything, map[string]int{"us-central1-a": 60, "us-central1-b": 10, "us-central1-c": 10}).Once().Return(nil)
			},
			wantScaleUpInfos: []nodegroupset.ScaleUpInfo{
				{
					Group:       mig1,
					CurrentSize: 100,
					NewSize:     160,
					MaxSize:     200,
				},
				{
					Group:       mig2,
					CurrentSize: 150,
					NewSize:     160,
					MaxSize:     200,
				},
				{
					Group:       mig3,
					CurrentSize: 150,
					NewSize:     160,
					MaxSize:     200,
				},
			},
		},
		{
			name:     "balance using flex advisor with limited capacity",
			groups:   []cloudprovider.NodeGroup{mig1, mig2, mig3},
			newNodes: 80,
			initialSetup: func(provider *instanceavailability.MockProvider) {
				snapshot2.SetProvider(provider)
				provider.On("AwaitInstanceAvailability", "ccc-1", "machineType: e2-standard-4, provisioningMode: STANDARD").Once().Return(snapshot2, nil)
				provider.On("MarkUsed", "ccc-1", "", "guidance-2", mock.Anything, map[string]int{"us-central1-a": 65, "us-central1-b": 15}).Once().Return(nil)
			},
			wantScaleUpInfos: []nodegroupset.ScaleUpInfo{
				{
					Group:       mig1,
					CurrentSize: 100,
					NewSize:     165,
					MaxSize:     200,
				},
				{
					Group:       mig2,
					CurrentSize: 150,
					NewSize:     165,
					MaxSize:     200,
				},
			},
		},
		{
			name:     "TPU - FlexAdvisorTPU disabled - uses fallback balancer, doesn't call Flex Advisor",
			groups:   []cloudprovider.NodeGroup{tpuMig},
			newNodes: 5,
			withFallbackBalancers: func(provider *instanceavailability.MockProvider, experimentsManager experiments.Manager, lister lister.Lister, registerMock func(m *mock.Mock)) nodegroupset.NodeGroupSetProcessor {
				mockBalancer := new(testutil.MockBalancer)
				provider.On("AwaitInstanceAvailability", "ccc-1", mock.Anything).Maybe().Panic("AwaitInstanceAvailability: should not be called")
				mockBalancer.On("BalanceScaleUpBetweenGroups", mock.Anything, []cloudprovider.NodeGroup{tpuMig}, 5).Return([]nodegroupset.ScaleUpInfo{
					{
						Group:       tpuMig,
						CurrentSize: 0,
						NewSize:     5,
						MaxSize:     10,
					},
				}, nil)
				registerMock(&mockBalancer.Mock)
				return mockBalancer
			},
			wantScaleUpInfos: []nodegroupset.ScaleUpInfo{
				{
					Group:       tpuMig,
					CurrentSize: 0,
					NewSize:     5,
					MaxSize:     10,
				},
			},
		},
		{
			name:            "TPU - FlexAdvisorTPU enabled but version flag off - uses fallback balancer",
			groups:          []cloudprovider.NodeGroup{tpuMig},
			newNodes:        5,
			enabledFeatures: []string{experiments.FlexAdvisorTPUEnabledFlag},
			withFallbackBalancers: func(provider *instanceavailability.MockProvider, experimentsManager experiments.Manager, lister lister.Lister, registerMock func(m *mock.Mock)) nodegroupset.NodeGroupSetProcessor {
				mockBalancer := new(testutil.MockBalancer)
				mockBalancer.On("BalanceScaleUpBetweenGroups", mock.Anything, []cloudprovider.NodeGroup{tpuMig}, 5).Return([]nodegroupset.ScaleUpInfo{
					{
						Group:       tpuMig,
						CurrentSize: 0,
						NewSize:     5,
						MaxSize:     10,
					},
				}, nil)
				registerMock(&mockBalancer.Mock)
				return mockBalancer
			},
			wantScaleUpInfos: []nodegroupset.ScaleUpInfo{
				{
					Group:       tpuMig,
					CurrentSize: 0,
					NewSize:     5,
					MaxSize:     10,
				},
			},
		},
		{
			name:            "TPU - FlexAdvisorTPU enabled - calls Flex Advisor",
			groups:          []cloudprovider.NodeGroup{tpuMig},
			newNodes:        5,
			enabledFeatures: []string{experiments.FlexAdvisorTPUEnabledFlag, experiments.FlexAdvisorTPUMinCAVersionFlag},
			initialSetup: func(provider *instanceavailability.MockProvider) {
				snapshot := instanceavailability.NewSnapshot(nil, "ccc-1", "", "guidance-flex", map[string]int{"us-central1-a": 10}, map[string]float64{"us-central1-a": 1.0})
				snapshot.SetProvider(provider)
				provider.On("AwaitInstanceAvailability", "ccc-1", "machineType: ct5p-hightpu-4t, provisioningMode: STANDARD, acceleratorTopology: 2x2x1").Return(snapshot, nil)
				provider.On("MarkUsed", "ccc-1", "", "guidance-flex", mock.Anything, map[string]int{"us-central1-a": 5}).Return(nil)
			},
			wantScaleUpInfos: []nodegroupset.ScaleUpInfo{
				{
					Group:       tpuMig,
					CurrentSize: 0,
					NewSize:     5,
					MaxSize:     10,
				},
			},
		},
		{
			name:            "DWS TPU - FlexAdvisorTPU & FlexAdvisorDWS enabled - calls Flex Advisor",
			groups:          []cloudprovider.NodeGroup{dwsTpuMig},
			newNodes:        5,
			enabledFeatures: []string{experiments.FlexAdvisorDWSEnabledFlag, experiments.FlexAdvisorDWSMinCAVersionFlag, experiments.FlexAdvisorTPUEnabledFlag, experiments.FlexAdvisorTPUMinCAVersionFlag},
			initialSetup: func(provider *instanceavailability.MockProvider) {
				snapshot := instanceavailability.NewSnapshot(nil, "ccc-1", "", "guidance-flex", map[string]int{"us-central1-a": 10}, map[string]float64{"us-central1-a": 1.0})
				snapshot.SetProvider(provider)
				provider.On("AwaitInstanceAvailability", "ccc-1", "machineType: ct5p-hightpu-4t, provisioningMode: FLEX_START, maxRunDuration: 3600, acceleratorTopology: 2x2x5").Return(snapshot, nil)
				provider.On("MarkUsed", "ccc-1", "", "guidance-flex", mock.Anything, map[string]int{"us-central1-a": 5}).Return(nil)
			},
			wantScaleUpInfos: []nodegroupset.ScaleUpInfo{
				{
					Group:       dwsTpuMig,
					CurrentSize: 0,
					NewSize:     5,
					MaxSize:     10,
				},
			},
		},
		{
			name:            "TPU Multihost - FlexAdvisorTPU - calls Flex Advisor",
			groups:          []cloudprovider.NodeGroup{tpuMultiHostMig},
			newNodes:        5,
			enabledFeatures: []string{experiments.FlexAdvisorTPUEnabledFlag, experiments.FlexAdvisorTPUMinCAVersionFlag},
			initialSetup: func(provider *instanceavailability.MockProvider) {
				snapshot := instanceavailability.NewSnapshot(nil, "ccc-1", "", "guidance-flex", map[string]int{"us-central1-a": 10}, map[string]float64{"us-central1-a": 1.0})
				snapshot.SetProvider(provider)
				provider.On("AwaitInstanceAvailability", "ccc-1", "machineType: ct5p-hightpu-4t, provisioningMode: STANDARD, acceleratorTopology: 2x2x5").Return(snapshot, nil)
				provider.On("MarkUsed", "ccc-1", "", "guidance-flex", mock.Anything, map[string]int{"us-central1-a": 5}).Return(nil)
			},
			wantScaleUpInfos: []nodegroupset.ScaleUpInfo{
				{
					Group:       tpuMultiHostMig,
					CurrentSize: 0,
					NewSize:     5,
					MaxSize:     10,
				},
			},
		},
		{
			name:     "DWS - FlexAdvisorDWS disabled - uses fallback balancer",
			groups:   []cloudprovider.NodeGroup{dwsMig},
			newNodes: 5,
			withFallbackBalancers: func(provider *instanceavailability.MockProvider, experimentsManager experiments.Manager, lister lister.Lister, registerMock func(m *mock.Mock)) nodegroupset.NodeGroupSetProcessor {
				mockBalancer := new(testutil.MockBalancer)
				mockBalancer.On("BalanceScaleUpBetweenGroups", mock.Anything, []cloudprovider.NodeGroup{dwsMig}, 5).Return([]nodegroupset.ScaleUpInfo{
					{
						Group:       dwsMig,
						CurrentSize: 0,
						NewSize:     5,
						MaxSize:     10,
					},
				}, nil)
				registerMock(&mockBalancer.Mock)
				return mockBalancer
			},
			wantScaleUpInfos: []nodegroupset.ScaleUpInfo{
				{
					Group:       dwsMig,
					CurrentSize: 0,
					NewSize:     5,
					MaxSize:     10,
				},
			},
		},
		{
			name:            "DWS - FlexAdvisorDWS enabled - calls Flex Advisor",
			groups:          []cloudprovider.NodeGroup{dwsMig},
			newNodes:        5,
			enabledFeatures: []string{experiments.FlexAdvisorDWSEnabledFlag, experiments.FlexAdvisorDWSMinCAVersionFlag},
			initialSetup: func(provider *instanceavailability.MockProvider) {
				snapshot := instanceavailability.NewSnapshot(nil, "ccc-1", "", "guidance-flex", map[string]int{"us-central1-a": 10}, map[string]float64{"us-central1-a": 1.0})
				snapshot.SetProvider(provider)
				provider.On("AwaitInstanceAvailability", "ccc-1", "machineType: e2-standard-4, provisioningMode: FLEX_START, maxRunDuration: 3600").Return(snapshot, nil)
				provider.On("MarkUsed", "ccc-1", "", "guidance-flex", mock.Anything, map[string]int{"us-central1-a": 5}).Return(nil)
			},
			wantScaleUpInfos: []nodegroupset.ScaleUpInfo{
				{
					Group:       dwsMig,
					CurrentSize: 0,
					NewSize:     5,
					MaxSize:     10,
				},
			},
		},
		{
			name:     "global processing disabled - fallbacks early",
			groups:   []cloudprovider.NodeGroup{mig1},
			newNodes: 80,
			initialSetup: func(provider *instanceavailability.MockProvider) {
				snapshot2.SetProvider(provider)
				provider.On("AwaitInstanceAvailability", "ccc-1", "machineType: e2-standard-4, provisioningMode: STANDARD").Maybe().Panic("AwaitInstanceAvailability: should not be called")
				provider.On("MarkUsed", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Maybe().Panic("AwaitInstanceAvailability: should not be called")
			},
			withFallbackBalancers: func(provider *instanceavailability.MockProvider, experimentsManager experiments.Manager, lister lister.Lister, registerMock func(m *mock.Mock)) nodegroupset.NodeGroupSetProcessor {
				mockBalancer := new(testutil.MockBalancer)
				mockBalancer.On("BalanceScaleUpBetweenGroups", mock.Anything, []cloudprovider.NodeGroup{mig1}, 80).Return([]nodegroupset.ScaleUpInfo{
					{
						Group:       mig1,
						CurrentSize: 100,
						NewSize:     180,
						MaxSize:     200,
					},
				}, nil)
				registerMock(&mockBalancer.Mock)
				return mockBalancer
			},
			disabledFeatures: []string{experiments.FlexAdvisorProcessingEnabledFlag},
			wantScaleUpInfos: []nodegroupset.ScaleUpInfo{
				{
					Group:       mig1,
					CurrentSize: 100,
					NewSize:     180,
					MaxSize:     200,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// store all mocks created throughout the test to verify their calls at the end
			mocks := []*mock.Mock{}
			registerMock := func(m *mock.Mock) {
				mocks = append(mocks, m)
			}

			mockProvider := new(instanceavailability.MockProvider)
			mockProvider.On("IncrementFlexAdvisorCacheQueryCount", mock.Anything, mock.Anything, mock.Anything).Maybe().Return()

			registerMock(&mockProvider.Mock)

			if tc.initialSetup != nil {
				tc.initialSetup(mockProvider)
			}
			mockLister := lister.NewMockCrdListerWithLabel([]crd.CRD{crd1}, labels.ComputeClassLabel)

			featuresMap := make(map[string]bool)
			for _, feature := range tc.enabledFeatures {
				featuresMap[feature] = true
			}
			for _, feature := range tc.disabledFeatures {
				featuresMap[feature] = false
			}
			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, featuresMap, nil)

			fallbackBalancers := (nodegroupset.NodeGroupSetProcessor)(nil)
			if tc.withFallbackBalancers != nil {
				fallbackBalancers = tc.withFallbackBalancers(mockProvider, experimentsManager, mockLister, registerMock)
			}

			balancer := NewScaleUpBalancer(fallbackBalancers, mockProvider, mockLister, experimentsManager, false)
			got, gotErr := balancer.BalanceScaleUpBetweenGroups(nil, tc.groups, tc.newNodes)

			assert.Equal(t, len(tc.wantScaleUpInfos), len(got))
			assert.ElementsMatch(t, got, tc.wantScaleUpInfos)
			assert.NoError(t, gotErr)

			for _, m := range mocks {
				m.AssertExpectations(t)
			}
		})
	}
}

func TestMaxScaleUpsByCloudProvider(t *testing.T) {
	mig1 := createMockMigs(&gke.GkeManagerMock{}, "", "us-central1-a", "", gke.LocationPolicyBalanced, 100, 200, nil)
	mig2 := createMockMigs(&gke.GkeManagerMock{}, "", "us-central1-a", "", gke.LocationPolicyBalanced, 200, 200, nil)
	mig3 := createMockMigs(&gke.GkeManagerMock{}, "", "us-central1-a", "", gke.LocationPolicyBalanced, 500, 2000, nil)
	errMig := createMockMigs(&gke.GkeManagerMock{}, "", "us-central1-a", "", gke.LocationPolicyBalanced, 100, 200, fmt.Errorf("size error"))
	testCases := []struct {
		name             string
		nodeGroups       []cloudprovider.NodeGroup
		wantScaleUpInfos []nodegroupset.ScaleUpInfo
		wantPossibleMax  int
		wantErr          error
	}{
		{
			name:       "single node group with capacity to scale up",
			nodeGroups: []cloudprovider.NodeGroup{mig1},
			wantScaleUpInfos: []nodegroupset.ScaleUpInfo{
				{
					Group:       mig1,
					CurrentSize: 100,
					NewSize:     100,
					MaxSize:     200,
				},
			},
			wantPossibleMax: 100,
		},
		{
			name:             "error when getting target size",
			nodeGroups:       []cloudprovider.NodeGroup{mig1, errMig},
			wantScaleUpInfos: []nodegroupset.ScaleUpInfo{},
			wantPossibleMax:  0,
			wantErr:          fmt.Errorf("size error"),
		},
		{
			name:       "multiple migs including a mig without spare capacity",
			nodeGroups: []cloudprovider.NodeGroup{mig1, mig2, mig3},
			wantScaleUpInfos: []nodegroupset.ScaleUpInfo{
				{
					Group:       mig1,
					CurrentSize: 100,
					NewSize:     100,
					MaxSize:     200,
				}, {
					Group:       mig3,
					CurrentSize: 500,
					NewSize:     500,
					MaxSize:     2000,
				},
			},
			wantPossibleMax: 1600,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotScaleUpInfos, gotPossibleMax, gotErr := maxScaleUpsByCloudProvider(tc.nodeGroups)
			assert.Equal(t, tc.wantScaleUpInfos, gotScaleUpInfos)
			assert.Equal(t, tc.wantPossibleMax, gotPossibleMax)
			assert.Equal(t, tc.wantErr, gotErr)
		})
	}
}

func TestBalancingInfoFromFlexAdvisor(t *testing.T) {
	mig1 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-a", "ccc-1", gke.LocationPolicyBalanced, 100, 200, nil)
	mig2 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-b", "ccc-1", gke.LocationPolicyBalanced, 150, 200, nil)
	mig3 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-c", "ccc-1", gke.LocationPolicyBalanced, 150, 200, nil)

	snapshot1 := instanceavailability.NewSnapshot(nil, "ccc-1", "", "guidance-1", map[string]int{"us-central1-a": 100, "us-central1-b": 20, "us-central1-c": 0}, map[string]float64{"us-central1-a": 1.0, "us-central1-b": 0.8, "us-central1-c": 0.8})
	snapshot2 := instanceavailability.NewSnapshot(nil, "ccc-1", "", "guidance-2", map[string]int{"us-central1-a": 1000, "us-central1-b": 5}, map[string]float64{"us-central1-a": 1.0, "us-central1-b": 0.8})

	scaleUpInfo1 := nodegroupset.ScaleUpInfo{
		Group:       mig1,
		CurrentSize: 100,
		NewSize:     100,
		MaxSize:     200,
	}
	scaleUpInfo2 := nodegroupset.ScaleUpInfo{
		Group:       mig2,
		CurrentSize: 150,
		NewSize:     150,
		MaxSize:     200,
	}
	scaleUpInfo3 := nodegroupset.ScaleUpInfo{
		Group:       mig3,
		CurrentSize: 150,
		NewSize:     150,
		MaxSize:     200,
	}

	testCases := []struct {
		name                       string
		initialSetup               func(*instanceavailability.MockProvider)
		scaleUpInfos               []nodegroupset.ScaleUpInfo
		wantBalancingInfos         []*balancingInfo
		wantPossibleMaxScaleUpSize int
		wantErr                    error
	}{
		{
			name: "balancing info for one mig",
			initialSetup: func(provider *instanceavailability.MockProvider) {
				provider.On("AwaitInstanceAvailability", "ccc-1", "machineType: e2-standard-4, provisioningMode: STANDARD").Return(snapshot1, nil)
			},
			scaleUpInfos: []nodegroupset.ScaleUpInfo{
				scaleUpInfo1,
			},
			wantBalancingInfos: []*balancingInfo{
				{
					scaleUpInfo:        scaleUpInfo1,
					snapshot:           snapshot1,
					capacityLimit:      100,
					gcePreferenceScore: 1.0,
				},
			},
			wantPossibleMaxScaleUpSize: 100,
		},
		{
			name: "balancing info multiple migs from same flexibility scope and instance configuration",
			initialSetup: func(provider *instanceavailability.MockProvider) {
				provider.On("AwaitInstanceAvailability", "ccc-1", "machineType: e2-standard-4, provisioningMode: STANDARD").Return(snapshot1, nil)
			},
			scaleUpInfos: []nodegroupset.ScaleUpInfo{
				scaleUpInfo1,
				scaleUpInfo2,
			},
			wantBalancingInfos: []*balancingInfo{
				{
					scaleUpInfo:        scaleUpInfo1,
					snapshot:           snapshot1,
					capacityLimit:      100,
					gcePreferenceScore: 1.0,
				},
				{
					scaleUpInfo:        scaleUpInfo2,
					snapshot:           snapshot1,
					capacityLimit:      20,
					gcePreferenceScore: 0.8,
				},
			},
			wantPossibleMaxScaleUpSize: 120,
		},
		{
			name: "doesnt filter out options without capacity",
			initialSetup: func(provider *instanceavailability.MockProvider) {
				provider.On("AwaitInstanceAvailability", "ccc-1", "machineType: e2-standard-4, provisioningMode: STANDARD").Return(snapshot1, nil)
			},
			scaleUpInfos: []nodegroupset.ScaleUpInfo{
				scaleUpInfo1,
				scaleUpInfo2,
				scaleUpInfo3,
			},
			wantBalancingInfos: []*balancingInfo{
				{
					scaleUpInfo:        scaleUpInfo1,
					snapshot:           snapshot1,
					capacityLimit:      100,
					gcePreferenceScore: 1.0,
				},
				{
					scaleUpInfo:        scaleUpInfo2,
					snapshot:           snapshot1,
					capacityLimit:      20,
					gcePreferenceScore: 0.8,
				},
				{
					scaleUpInfo:        scaleUpInfo3,
					snapshot:           snapshot1,
					capacityLimit:      0,
					gcePreferenceScore: 0.8,
				},
			},
			wantPossibleMaxScaleUpSize: 120,
		},
		{
			name: "DWS - FlexAdvisorDWS disabled - returns error",
			initialSetup: func(provider *instanceavailability.MockProvider) {
				provider.On("AwaitInstanceAvailability", "ccc-1", "machineType: e2-standard-4, provisioningMode: STANDARD").Return(nil, fmt.Errorf("not found"))
			},
			scaleUpInfos: []nodegroupset.ScaleUpInfo{
				scaleUpInfo1,
				scaleUpInfo2,
				scaleUpInfo3,
			},
			wantBalancingInfos:         []*balancingInfo{},
			wantPossibleMaxScaleUpSize: 0,
			wantErr:                    fmt.Errorf("not found"),
		},
		{
			name: "should cap capacityLimit either by (MaxSize - CurrentSize) or snapshot data",
			initialSetup: func(provider *instanceavailability.MockProvider) {
				provider.On("AwaitInstanceAvailability", "ccc-1", "machineType: e2-standard-4, provisioningMode: STANDARD").Return(snapshot2, nil)
			},
			scaleUpInfos: []nodegroupset.ScaleUpInfo{
				scaleUpInfo1,
				scaleUpInfo2,
			},
			wantBalancingInfos: []*balancingInfo{
				{
					scaleUpInfo:        scaleUpInfo1,
					snapshot:           snapshot2,
					capacityLimit:      100, // capped by MaxSize - CurrentSize
					gcePreferenceScore: 1.0,
				},
				{
					scaleUpInfo:        scaleUpInfo2,
					snapshot:           snapshot2,
					capacityLimit:      5, // capped by snapshot
					gcePreferenceScore: 0.8,
				},
			},
			wantPossibleMaxScaleUpSize: 105,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockProvider := &instanceavailability.MockProvider{}
			mockProvider.On("IncrementFlexAdvisorCacheQueryCount", mock.Anything, mock.Anything, mock.Anything).Return()
			if tc.initialSetup != nil {
				tc.initialSetup(mockProvider)
			}
			mockLister := lister.NewMockCrdListerWithLabel([]crd.CRD{}, labels.ComputeClassLabel)
			gotBalancingInfo, gotPossibleMaxScaleUpSize, gotErr := BalancingInfoFromFlexAdvisor(mockProvider, mockLister, tc.scaleUpInfos, experiments.NewMockManager(), 0)
			assert.Equal(t, len(tc.wantBalancingInfos), len(gotBalancingInfo))
			for _, info := range gotBalancingInfo {
				info.instanceRef = nil
			}
			assert.ElementsMatch(t, tc.wantBalancingInfos, gotBalancingInfo)
			assert.Equal(t, tc.wantPossibleMaxScaleUpSize, gotPossibleMaxScaleUpSize)
			assert.Equal(t, tc.wantErr, gotErr)
		})
	}
}

func TestDistributeNewNodes(t *testing.T) {
	mig1 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-a", "ccc-1", gke.LocationPolicyBalanced, 100, 200, nil)
	mig2 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-b", "ccc-1", gke.LocationPolicyBalanced, 150, 200, nil)
	mig3 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-c", "ccc-1", gke.LocationPolicyBalanced, 150, 200, nil)

	testCases := []struct {
		name               string
		balancingInfos     []*balancingInfo
		newNodes           int
		wantBalancingInfos []*balancingInfo
		want               map[cloudprovider.NodeGroup]int
	}{
		{
			name: "doesnt return noop items",
			balancingInfos: []*balancingInfo{
				{
					scaleUpInfo: nodegroupset.ScaleUpInfo{
						Group:       mig1,
						CurrentSize: 100,
						NewSize:     100,
						MaxSize:     200,
					},
					capacityLimit: 100,
				},
				{
					scaleUpInfo: nodegroupset.ScaleUpInfo{
						Group:       mig2,
						CurrentSize: 150,
						NewSize:     150,
						MaxSize:     200,
					},
					capacityLimit: 50,
				},
				{
					scaleUpInfo: nodegroupset.ScaleUpInfo{
						Group:       mig3,
						CurrentSize: 150,
						NewSize:     150,
						MaxSize:     200,
					},
					capacityLimit: 50,
				},
			},
			newNodes: 50,
			want: map[cloudprovider.NodeGroup]int{
				// only mig1 required increase, other migs did not change
				mig1: 150,
			},
		},
		{
			name: "plenty of capacity, equally balance the node groups",
			balancingInfos: []*balancingInfo{
				{
					scaleUpInfo: nodegroupset.ScaleUpInfo{
						Group:       mig1,
						CurrentSize: 100,
						NewSize:     100,
						MaxSize:     200,
					},
					capacityLimit: 100,
				},
				{
					scaleUpInfo: nodegroupset.ScaleUpInfo{
						Group:       mig2,
						CurrentSize: 150,
						NewSize:     150,
						MaxSize:     200,
					},
					capacityLimit: 50,
				},
				{
					scaleUpInfo: nodegroupset.ScaleUpInfo{
						Group:       mig3,
						CurrentSize: 150,
						NewSize:     150,
						MaxSize:     200,
					},
					capacityLimit: 50,
				},
			},
			newNodes: 80,
			want: map[cloudprovider.NodeGroup]int{
				mig1: 160,
				mig2: 160,
				mig3: 160,
			},
		},
		{
			name: "one node group reaches the full capacity",
			balancingInfos: []*balancingInfo{
				{
					scaleUpInfo: nodegroupset.ScaleUpInfo{
						Group:       mig1,
						CurrentSize: 100,
						NewSize:     100,
						MaxSize:     200,
					},
					capacityLimit: 10,
				},
				{
					scaleUpInfo: nodegroupset.ScaleUpInfo{
						Group:       mig2,
						CurrentSize: 150,
						NewSize:     150,
						MaxSize:     200,
					},
					capacityLimit: 50,
				},
				{
					scaleUpInfo: nodegroupset.ScaleUpInfo{
						Group:       mig3,
						CurrentSize: 150,
						NewSize:     150,
						MaxSize:     200,
					},
					capacityLimit: 50,
				},
			},
			newNodes: 80,
			want: map[cloudprovider.NodeGroup]int{
				mig1: 110,
				mig2: 185,
				mig3: 185,
			},
		},
		{
			name: "tie break by GCE preference score",
			balancingInfos: []*balancingInfo{
				{
					scaleUpInfo: nodegroupset.ScaleUpInfo{
						Group:       mig2,
						CurrentSize: 150,
						NewSize:     150,
						MaxSize:     200,
					},
					capacityLimit:      50,
					gcePreferenceScore: 1.0,
				},
				{
					scaleUpInfo: nodegroupset.ScaleUpInfo{
						Group:       mig3,
						CurrentSize: 150,
						NewSize:     150,
						MaxSize:     200,
					},
					capacityLimit:      50,
					gcePreferenceScore: 0.8,
				},
			},
			newNodes: 11,
			want: map[cloudprovider.NodeGroup]int{
				mig2: 156,
				mig3: 155,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := distributeNewNodes(tc.balancingInfos, tc.newNodes)
			assert.Equal(t, len(tc.want), len(got))
			for _, info := range got {
				wantSize, found := tc.want[info.scaleUpInfo.Group]
				assert.True(t, found)
				assert.Equal(t, wantSize, info.scaleUpInfo.NewSize, "For mig: %v got: %d, want: %d", info.scaleUpInfo.Group.Id(), info.scaleUpInfo.NewSize, wantSize)
			}
		})
	}
}

func createMockMigs(gkeMock *gke.GkeManagerMock, machineType, zone, cccName string, locationPolicy gke.LocationPolicyEnum, migSize, maxSize int, migSizeErr error, opts ...MigOption) *gke.GkeMig {
	builder := gke.NewTestGkeMigBuilder().
		SetGceRefZone(zone).
		SetGkeManager(gkeMock).
		SetMaxSize(maxSize).
		SetLocationPolicy(locationPolicy).
		SetExist(true).
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: machineType,
			Labels:      map[string]string{labels.ComputeClassLabel: cccName},
		})
	mig := builder.Build()

	if migSizeErr != nil {
		gkeMock.On("GetMigSize", mig).Return(int64(0), migSizeErr)
	} else {
		gkeMock.On("GetMigSize", mig).Return(int64(migSize), nil)
	}
	gkeMock.On("GetGkeMigs").Return([]*gke.GkeMig{mig})
	gkeMock.On("GetMigTemplateNodeInfo", mig).Return(framework.NewTestNodeInfo(buildNodeWithLabels(map[string]string{
		labels.ComputeClassLabel: cccName,
	})), nil)

	for _, opt := range opts {
		opt(mig)
	}
	return mig
}
